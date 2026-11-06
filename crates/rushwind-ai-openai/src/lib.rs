//! OpenAI-compatible engine for the RushWind [`ai`] contract — one
//! reqwest client speaking the chat-completions wire format against
//! cloud providers (OpenAI, Qwen/DashScope, any compatible endpoint)
//! and local runtimes (Ollama's OpenAI-compatible `/v1`).
//!
//! [`ai`]: rushwind_ai
//!
//! # Configuration resolutions
//!
//! The client is built from the config — cloud means a
//! base URL, an api key and an optional organization; local means
//! `http://host:port/v1` with a placeholder bearer. Endpoint
//! resolutions: Ollama gets the
//! `none` bearer (the runtime ignores it), default timeout 30 s — and
//! [`Config::model_name`] serves as
//! the per-request model fallback when a request does not name one.
//! Orchestration is
//! the caller's business.
//!
//! # Beyond the sync surface
//!
//! [`ChatModel::chat`] covers the contract. The engine also streams
//! server-sent events ([`OpenAiClient::chat_stream`]: content deltas
//! plus the finish reason; tool-call delta assembly is not provided)
//! and embeds text ([`OpenAiClient::embed`]), both over the same
//! endpoint resolution.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::BoxStream;
use rushwind_ai::{
    AiError, BoxFuture, ChatModel, ChatRequest, ChatResponse, Config, ModelType, Usage,
};

/// The provider default when [`rushwind_ai::CloudConfig::base_url`]
/// is empty.
const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

/// The request timeout when [`Config::timeout_seconds`] is 0.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct Inner {
    http: reqwest::Client,
    /// Endpoint root without a trailing slash; paths join onto it.
    base_url: String,
    /// Bearer token; empty drops the `Authorization` header.
    api_key: String,
    organization: Option<String>,
    /// The [`Config::model_name`] fallback.
    default_model: String,
}

/// An OpenAI-compatible chat/embedding client.
#[derive(Debug)]
pub struct OpenAiClient {
    inner: Arc<Inner>,
}

/// One increment of a streamed completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatStreamEvent {
    /// The next piece of assistant content.
    Delta(String),
    /// The stream ended, with the provider's finish reason
    /// (`stop`, `tool_calls`, `length`, ...).
    Finish(String),
}

impl OpenAiClient {
    /// Builds an engine from the connection settings, branching on
    /// [`Config::model_type`].
    pub fn new(config: Config) -> Result<Self, AiError> {
        let (base_url, api_key, organization) = resolve_endpoint(&config)?;
        let timeout = if config.timeout_seconds == 0 {
            DEFAULT_TIMEOUT
        } else {
            Duration::from_secs(u64::from(config.timeout_seconds))
        };
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| AiError::Config(format!("http client: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                http,
                base_url,
                api_key,
                organization,
                default_model: config.model_name,
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape
    /// — the [`Config`] fields, snake_case keys.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, AiError> {
        let config: Config = serde_json::from_value(settings)
            .map_err(|e| AiError::Config(format!("settings parse: {e}")))?;
        Self::new(config)
    }

    /// The model requests fall back to — the [`Config::model_name`].
    pub fn model(&self) -> &str {
        &self.inner.default_model
    }

    /// Streams a completion as server-sent events. The request is
    /// sent (and its status checked) before the call resolves; the
    /// returned stream then yields content deltas and the finish
    /// reason, ending after [`ChatStreamEvent::Finish`] or an error.
    pub async fn chat_stream(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<ChatStreamEvent, AiError>>, AiError> {
        let body = self.completion_body(&request, true)?;
        let response = self
            .start_post("chat/completions", &body)?
            .send()
            .await
            .map_err(|e| AiError::Request(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|e| format!("<unreadable body: {e}>"));
            return Err(AiError::Status {
                code: status.as_u16(),
                body,
            });
        }
        let state = SseState {
            response,
            buffer: Vec::new(),
            pending: VecDeque::new(),
            done: false,
        };
        Ok(Box::pin(futures::stream::unfold(
            state,
            |mut state| async move {
                match state.advance().await {
                    Ok(Some(event)) => Some((event, state)),
                    Ok(None) => None,
                    Err(e) => {
                        state.done = true;
                        Some((Err(e), state))
                    }
                }
            },
        )))
    }

    /// Embeds texts with the [`Config::model_name`] model — the
    /// langchaingo `EmbedDocuments` convenience.
    pub async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AiError> {
        self.embed_with_model(&self.inner.default_model, texts)
            .await
    }

    /// Embeds a single text — the langchaingo `EmbedQuery`
    /// convenience.
    pub async fn embed_query(&self, text: &str) -> Result<Vec<f32>, AiError> {
        Ok(self.embed(&[text]).await?.remove(0))
    }

    /// Embeds texts with an explicit model name (embedding models
    /// usually differ from the chat model).
    pub async fn embed_with_model(
        &self,
        model: &str,
        texts: &[&str],
    ) -> Result<Vec<Vec<f32>>, AiError> {
        if model.is_empty() {
            return Err(AiError::EmptyModel);
        }
        let response: EmbeddingsWire = self
            .post_json(
                "embeddings",
                &serde_json::json!({ "model": model, "input": texts }),
            )
            .await?;
        let mut vectors: Vec<(usize, Vec<f32>)> = response
            .data
            .into_iter()
            .map(|item| (item.index, item.embedding))
            .collect();
        vectors.sort_by_key(|(index, _)| *index);
        Ok(vectors
            .into_iter()
            .map(|(_, embedding)| embedding)
            .collect())
    }

    /// Resolves the request model (override, else the config
    /// fallback) and serializes the completion body with the stream
    /// flag stamped.
    fn completion_body(
        &self,
        request: &ChatRequest,
        stream: bool,
    ) -> Result<serde_json::Value, AiError> {
        let model = request
            .model
            .as_deref()
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .or_else(|| {
                let name = self.inner.default_model.clone();
                (!name.is_empty()).then_some(name)
            })
            .ok_or(AiError::EmptyModel)?;
        let mut body = serde_json::to_value(request)
            .map_err(|e| AiError::Config(format!("request encode: {e}")))?;
        body["model"] = serde_json::Value::String(model);
        body["stream"] = serde_json::Value::Bool(stream);
        Ok(body)
    }

    /// Posts a JSON body and decodes the JSON response, mapping
    /// non-success statuses to [`AiError::Status`].
    async fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<T, AiError> {
        let payload = self.post_bytes(path, body).await?;
        serde_json::from_slice(&payload).map_err(|e| AiError::Decode(format!("{path}: {e}")))
    }

    /// Posts a JSON body, returning the response bytes of a success
    /// status.
    async fn post_bytes(&self, path: &str, body: &serde_json::Value) -> Result<Vec<u8>, AiError> {
        let response = self
            .start_post(path, body)?
            .send()
            .await
            .map_err(|e| AiError::Request(e.to_string()))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| AiError::Request(format!("{path}: {e}")))?;
        if !status.is_success() {
            return Err(AiError::Status {
                code: status.as_u16(),
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        Ok(bytes.to_vec())
    }

    /// Builds the POST request: JSON content type, bearer token and
    /// organization when configured.
    fn start_post(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<reqwest::RequestBuilder, AiError> {
        let payload = serde_json::to_vec(body)
            .map_err(|e| AiError::Config(format!("request encode: {e}")))?;
        let mut request = self
            .inner
            .http
            .post(format!("{}/{path}", self.inner.base_url))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload);
        if !self.inner.api_key.is_empty() {
            request = request.bearer_auth(&self.inner.api_key);
        }
        if let Some(organization) = &self.inner.organization {
            request = request.header("OpenAI-Organization", organization);
        }
        Ok(request)
    }
}

impl ChatModel for OpenAiClient {
    fn chat<'a>(&'a self, request: ChatRequest) -> BoxFuture<'a, Result<ChatResponse, AiError>> {
        Box::pin(async move {
            let body = self.completion_body(&request, false)?;
            let wire: CompletionWire = self.post_json("chat/completions", &body).await?;
            wire.into_response()
        })
    }
}

/// The endpoint/bearer/organization triple for a [`Config`] — pure
/// so the normalizing fallbacks (empty host, zero port, default
/// base URL) unit-test without a client.
fn resolve_endpoint(config: &Config) -> Result<(String, String, Option<String>), AiError> {
    match config.model_type {
        ModelType::Cloud => {
            let cloud = config.cloud.as_ref().ok_or(AiError::MissingCloudConfig)?;
            let base_url = if cloud.base_url.trim().is_empty() {
                DEFAULT_BASE_URL.to_string()
            } else {
                cloud.base_url.trim_end_matches('/').to_string()
            };
            let organization = if cloud.organization.is_empty() {
                None
            } else {
                Some(cloud.organization.clone())
            };
            Ok((base_url, cloud.api_key.clone(), organization))
        }
        ModelType::Local => {
            let local = config.local.as_ref().ok_or(AiError::MissingLocalConfig)?;
            let host = if local.host.trim().is_empty() {
                "localhost"
            } else {
                local.host.trim()
            };
            let port = if local.port == 0 { 11434 } else { local.port };
            Ok((format!("http://{host}:{port}/v1"), "none".to_string(), None))
        }
    }
}

/// The chat-completions response wire shape, narrowed to the
/// contract fields.
#[derive(serde::Deserialize)]
struct CompletionWire {
    #[serde(default)]
    choices: Vec<ChoiceWire>,
    #[serde(default)]
    usage: Option<Usage>,
}

#[derive(serde::Deserialize)]
struct ChoiceWire {
    #[serde(default)]
    message: Option<rushwind_ai::Message>,
    #[serde(default)]
    finish_reason: Option<String>,
}

impl CompletionWire {
    fn into_response(self) -> Result<ChatResponse, AiError> {
        let choice = self.choices.into_iter().next().ok_or_else(|| {
            AiError::Decode("chat/completions: response has no choices".to_string())
        })?;
        Ok(ChatResponse {
            message: choice.message.unwrap_or_default(),
            finish_reason: choice.finish_reason,
            usage: self.usage,
        })
    }
}

/// The embeddings response wire shape.
#[derive(serde::Deserialize)]
struct EmbeddingsWire {
    #[serde(default)]
    data: Vec<EmbeddingWire>,
}

#[derive(serde::Deserialize)]
struct EmbeddingWire {
    #[serde(default)]
    index: usize,
    embedding: Vec<f32>,
}

/// One streamed chat-completions chunk — the `delta` object plus
/// the terminal finish reason.
#[derive(serde::Deserialize)]
struct StreamChunkWire {
    #[serde(default)]
    choices: Vec<StreamChoiceWire>,
}

#[derive(serde::Deserialize)]
struct StreamChoiceWire {
    #[serde(default)]
    delta: DeltaWire,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(serde::Deserialize, Default)]
struct DeltaWire {
    #[serde(default)]
    content: Option<String>,
}

/// The incremental state of one SSE stream: the live response, the
/// byte buffer between newlines, and events parsed ahead of demand.
struct SseState {
    response: reqwest::Response,
    buffer: Vec<u8>,
    pending: VecDeque<Result<ChatStreamEvent, AiError>>,
    done: bool,
}

impl SseState {
    /// Advances one step: drains parsed events first, then pulls
    /// more bytes and splits SSE lines. The outer error is a
    /// terminal transport failure; per-event decode failures travel
    /// through the stream as yielded items.
    async fn advance(&mut self) -> Result<Option<Result<ChatStreamEvent, AiError>>, AiError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
            if self.done {
                return Ok(None);
            }
            match self.response.chunk().await {
                Ok(Some(bytes)) => {
                    self.buffer.extend_from_slice(&bytes);
                    self.drain_complete_lines();
                }
                Ok(None) => {
                    self.flush_partial_line();
                    self.done = true;
                }
                Err(e) => return Err(AiError::Request(format!("stream: {e}"))),
            }
        }
    }

    /// Splits complete `data:` lines out of the buffer into
    /// `pending`. A line split across chunk boundaries stays buffered
    /// until its `\n` arrives — flushing the partial bytes per chunk
    /// would parse half a JSON payload as a whole line (a fabricated
    /// decode error in the stream, and a lost `[DONE]` terminator when
    /// that line is the split one).
    fn drain_complete_lines(&mut self) {
        while let Some(position) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=position).collect();
            self.parse_line(&line[..line.len() - 1]);
        }
    }

    /// The end-of-stream flush: after EOF the remaining unterminated
    /// bytes are the final line (a peer that closes without a trailing
    /// newline).
    fn flush_partial_line(&mut self) {
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.parse_line(&line);
        }
    }

    /// Decodes one SSE line: `data: [DONE]` closes the stream,
    /// other `data:` lines carry a chunk JSON.
    fn parse_line(&mut self, line: &[u8]) {
        let line = String::from_utf8_lossy(line);
        let Some(payload) = line.trim_end_matches('\r').strip_prefix("data:") else {
            return;
        };
        let payload = payload.trim();
        if payload == "[DONE]" {
            self.done = true;
            return;
        }
        if payload.is_empty() {
            return;
        }
        let chunk: StreamChunkWire = match serde_json::from_str(payload) {
            Ok(chunk) => chunk,
            Err(e) => {
                self.pending
                    .push_back(Err(AiError::Decode(format!("stream chunk: {e}"))));
                return;
            }
        };
        for choice in chunk.choices {
            if let Some(content) = choice.delta.content {
                self.pending.push_back(Ok(ChatStreamEvent::Delta(content)));
            }
            if let Some(reason) = choice.finish_reason {
                self.pending.push_back(Ok(ChatStreamEvent::Finish(reason)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cloud_config(base_url: &str) -> Config {
        Config {
            model_type: ModelType::Cloud,
            model_name: "gpt-4o".to_string(),
            timeout_seconds: 0,
            cloud: Some(rushwind_ai::CloudConfig {
                api_key: "sk".to_string(),
                base_url: base_url.to_string(),
                organization: String::new(),
            }),
            local: None,
        }
    }

    #[test]
    fn cloud_empty_base_url_falls_back_to_openai() {
        let (base, key, org) = resolve_endpoint(&cloud_config("")).unwrap();
        assert_eq!(base, "https://api.openai.com/v1");
        assert_eq!(key, "sk");
        assert_eq!(org, None);
    }

    #[test]
    fn cloud_custom_base_url_loses_trailing_slash() {
        let (base, _, _) = resolve_endpoint(&cloud_config(
            "https://dashscope.aliyuncs.com/compatible-mode/v1/",
        ))
        .unwrap();
        assert_eq!(base, "https://dashscope.aliyuncs.com/compatible-mode/v1");
    }

    #[test]
    fn cloud_organization_carries_when_present() {
        let mut config = cloud_config("https://x.example");
        config.cloud.as_mut().unwrap().organization = "org-7".to_string();
        let (_, _, org) = resolve_endpoint(&config).unwrap();
        assert_eq!(org.as_deref(), Some("org-7"));
    }

    #[test]
    fn cloud_without_cloud_config_is_typed_error() {
        let mut config = cloud_config("");
        config.cloud = None;
        assert!(matches!(
            resolve_endpoint(&config),
            Err(AiError::MissingCloudConfig)
        ));
    }

    #[test]
    fn local_defaults_normalize_like_go() {
        let config = Config {
            model_type: ModelType::Local,
            model_name: "llama3".to_string(),
            timeout_seconds: 0,
            cloud: None,
            local: Some(rushwind_ai::LocalConfig {
                host: String::new(),
                port: 0,
            }),
        };
        let (base, key, org) = resolve_endpoint(&config).unwrap();
        assert_eq!(base, "http://localhost:11434/v1");
        assert_eq!(key, "none");
        assert_eq!(org, None);
    }

    #[test]
    fn local_custom_host_and_port_join_v1() {
        let config = Config {
            model_type: ModelType::Local,
            model_name: String::new(),
            timeout_seconds: 0,
            cloud: None,
            local: Some(rushwind_ai::LocalConfig {
                host: "127.0.0.1".to_string(),
                port: 41234,
            }),
        };
        let (base, _, _) = resolve_endpoint(&config).unwrap();
        assert_eq!(base, "http://127.0.0.1:41234/v1");
    }

    #[test]
    fn local_without_local_config_is_typed_error() {
        let config = Config {
            model_type: ModelType::Local,
            model_name: String::new(),
            timeout_seconds: 0,
            cloud: Some(rushwind_ai::CloudConfig::default()),
            local: None,
        };
        assert!(matches!(
            resolve_endpoint(&config),
            Err(AiError::MissingLocalConfig)
        ));
    }
}
