//! AI model contract for RushWind: a configuration taxonomy pointing at an LLM —
//! cloud ([`ModelType::Cloud`]: OpenAI, Qwen, any OpenAI-compatible
//! API) or local ([`ModelType::Local`]: Ollama) — plus the message,
//! tool, request and response shapes of the chat-completions wire,
//! and the [`ChatModel`] trait engines implement.
//!
//! # The core shapes
//!
//! The contract is one `Config → client` factory over two deployment
//! modes, plus three traits engines implement: [`ChatModel`] (one
//! assistant turn), [`StreamingChatModel`] (the turn as a stream of
//! [`ChatStreamEvent`]s — content deltas, assembled tool calls, the
//! finish reason) and [`EmbeddingModel`] (texts to vectors). There is
//! a single engine today (`rushwind-ai-openai`): one OpenAI-compatible
//! client story, and orchestration frameworks (chains, agents,
//! compose graphs) stay the caller's business.
//!
//! Missing configuration surfaces as typed errors: a cloud [`Config`]
//! without [`CloudConfig`] is [`AiError::MissingCloudConfig`], a
//! local one without [`LocalConfig`] is
//! [`AiError::MissingLocalConfig`]. An unrecognized model type cannot
//! occur: [`ModelType`]
//! is an enum, so the compiler holds the check. Wire settings are
//! plain snake_case keys, read through [`serde`].

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;

use futures_core::Stream;

/// Future type used across the ai contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors surfaced by ai engines.
#[derive(Debug)]
#[non_exhaustive]
pub enum AiError {
    /// Cloud model selected but no [`CloudConfig`] supplied.
    MissingCloudConfig,
    /// Local model selected but no [`LocalConfig`] supplied.
    MissingLocalConfig,
    /// Neither the request nor the [`Config`] names a model.
    EmptyModel,
    /// Malformed settings — the bootstrap `from_settings` wire shape.
    Config(String),
    /// The HTTP exchange itself failed (connect, timeout, reset).
    Request(String),
    /// A non-success HTTP status, with the response body.
    Status {
        /// The HTTP status code.
        code: u16,
        /// The response body, best-effort decoded.
        body: String,
    },
    /// The response body did not decode as expected.
    Decode(String),
}

impl std::fmt::Display for AiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCloudConfig => write!(f, "ai: cloud config is missing"),
            Self::MissingLocalConfig => write!(f, "ai: local config is missing"),
            Self::EmptyModel => write!(f, "ai: model name is empty"),
            Self::Config(msg) => write!(f, "ai: invalid config: {msg}"),
            Self::Request(msg) => write!(f, "ai: request failed: {msg}"),
            Self::Status { code, body } => write!(f, "ai: HTTP {code}: {body}"),
            Self::Decode(msg) => write!(f, "ai: decode response: {msg}"),
        }
    }
}

impl std::error::Error for AiError {}

/// Whether the model runs locally or in the cloud. On the
/// wire the lowercase names are spoken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelType {
    /// A locally-hosted runtime (e.g. Ollama).
    Local,
    /// A cloud provider speaking the OpenAI-compatible API.
    Cloud,
}

/// Settings for cloud-based LLM providers.
///
/// `base_url` empty (or absent on the wire) selects the provider
/// default (`https://api.openai.com/v1`); `organization` empty
/// omits the `OpenAI-Organization` header.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(default)]
pub struct CloudConfig {
    /// The bearer token for the `Authorization` header.
    pub api_key: String,
    /// The OpenAI-compatible base URL, e.g. a Qwen/DashScope
    /// endpoint; empty selects `https://api.openai.com/v1`.
    pub base_url: String,
    /// The OpenAI organization, when the account has several.
    pub organization: String,
}

/// Settings for locally-hosted models.
/// Zero values resolve to sensible defaults: empty host → `localhost`, port 0 →
/// `11434` (Ollama's default).
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default)]
pub struct LocalConfig {
    /// The runtime host.
    pub host: String,
    /// The runtime port.
    pub port: u16,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 11434,
        }
    }
}

/// The configuration engines build a client from.
/// Exactly the branch named by [`Config::model_type`] must be
/// populated; the other is ignored. `model_type` is required on the
/// wire, everything else
/// defaults.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct Config {
    /// Which deployment mode the client targets.
    pub model_type: ModelType,
    /// The model to use when a request does not name one
    /// (`gpt-4o`, `qwen-max`, `llama3`, ...).
    #[serde(default)]
    pub model_name: String,
    /// The request timeout in seconds; 0 uses the engine default
    /// (30 s).
    #[serde(default)]
    pub timeout_seconds: u32,
    /// Cloud settings, required when `model_type` is
    /// [`ModelType::Cloud`].
    #[serde(default)]
    pub cloud: Option<CloudConfig>,
    /// Local settings, required when `model_type` is
    /// [`ModelType::Local`].
    #[serde(default)]
    pub local: Option<LocalConfig>,
}

/// A speaker in a conversation — the OpenAI wire roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// The behavior instruction.
    System,
    /// The human turn.
    User,
    /// The model turn.
    Assistant,
    /// A tool result fed back to the model.
    Tool,
}

/// One function invocation the assistant asked for — the OpenAI
/// `tool_calls` entry. `ToolCallFunction::arguments` is a
/// JSON-encoded object *string* on the wire, not a nested object.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolCall {
    /// The invocation id, to echo back in the tool result message.
    pub id: String,
    /// Always `function` on the wire — the only kind chat
    /// completions knows. Emitted on serialize, defaulted on read.
    #[serde(rename = "type")]
    pub kind: ToolKind,
    /// The invoked function.
    pub function: ToolCallFunction,
}

/// The tool-kind marker (`"function"`) on tool calls and tool
/// definitions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolKind {
    /// The only kind: a named function with JSON-schema parameters.
    #[default]
    Function,
}

/// The function half of a [`ToolCall`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolCallFunction {
    /// The invoked function name.
    pub name: String,
    /// The arguments as a JSON-encoded object string (`"{\"city\":\"...\"}"`).
    pub arguments: String,
}

/// One message in a conversation — the OpenAI wire shape. Fields
/// absent from a given role are omitted from serialization
/// (a user message is just `{role, content}` on the wire).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Message {
    /// Who speaks.
    pub role: Role,
    /// The message text; `None` on assistant turns that only call
    /// tools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Tool invocations, on assistant turns that call tools.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    /// The [`ToolCall::id`] this result answers — on `Role::Tool`
    /// turns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl Default for Message {
    fn default() -> Self {
        Self {
            role: Role::User,
            content: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }
}

impl Message {
    /// A `Role::System` message.
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: Some(content.into()),
            ..Self::default()
        }
    }

    /// A `Role::User` message.
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: Some(content.into()),
            ..Self::default()
        }
    }

    /// A `Role::Assistant` message.
    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: Some(content.into()),
            ..Self::default()
        }
    }

    /// A `Role::Tool` message answering `tool_call_id`.
    pub fn tool(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: Role::Tool,
            content: Some(content.into()),
            tool_call_id: Some(tool_call_id.into()),
            ..Self::default()
        }
    }
}

/// A callable the model may invoke — the OpenAI tool definition.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Tool {
    /// Always [`ToolKind::Function`].
    #[serde(rename = "type")]
    pub kind: ToolKind,
    /// The function the model sees.
    pub function: FunctionDefinition,
}

impl Tool {
    /// A function tool: name, description, and the JSON-schema
    /// object describing the parameters.
    pub fn function(
        name: impl Into<String>,
        description: impl Into<String>,
        parameters: serde_json::Value,
    ) -> Self {
        Self {
            kind: ToolKind::Function,
            function: FunctionDefinition {
                name: name.into(),
                description: Some(description.into()),
                parameters: Some(parameters),
            },
        }
    }
}

/// The function description inside a [`Tool`].
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct FunctionDefinition {
    /// The function name; unique across the request's tools.
    pub name: String,
    /// When to use the function — what the model reasons over.
    pub description: Option<String>,
    /// A JSON-schema object describing the parameters; `None`/absent
    /// for a no-argument function.
    pub parameters: Option<serde_json::Value>,
}

/// One chat completion request — the OpenAI wire shape. The engine
/// stamps `model` (request override, else [`Config::model_name`])
/// and the stream flag; everything else passes through as set.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChatRequest {
    /// Overrides [`Config::model_name`] for this request.
    pub model: Option<String>,
    /// The conversation so far.
    pub messages: Vec<Message>,
    /// The tools the model may call.
    pub tools: Vec<Tool>,
    /// Sampling temperature.
    pub temperature: Option<f32>,
    /// Nucleus sampling cutoff.
    pub top_p: Option<f32>,
    /// Generation length cap.
    pub max_tokens: Option<u32>,
    /// Stop sequences.
    pub stop: Vec<String>,
}

/// Token accounting for a completion — the OpenAI `usage` object.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Usage {
    /// Tokens consumed by the prompt.
    pub prompt_tokens: u32,
    /// Tokens generated.
    pub completion_tokens: u32,
    /// The sum.
    pub total_tokens: u32,
}

/// One chat completion result — the assistant's turn.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChatResponse {
    /// The assistant's reply (content and/or tool calls).
    pub message: Message,
    /// Why generation stopped: `stop`, `tool_calls`, `length`, ...
    pub finish_reason: Option<String>,
    /// Token accounting, when the provider reports it.
    pub usage: Option<Usage>,
}

/// The chat contract: generate one assistant turn from a conversation.
/// Engines must
/// be callable through shared references (`&self`).
pub trait ChatModel: Send + Sync {
    /// Completes the conversation — the OpenAI chat-completions
    /// call.
    fn chat<'a>(&'a self, request: ChatRequest) -> BoxFuture<'a, Result<ChatResponse, AiError>>;
}

/// One increment of a streamed completion — the streaming half of the
/// contract. Tool calls arrive **assembled**: engines merge the
/// provider's streamed fragments (id/name on the first, argument
/// fragments concatenated) and emit each invocation once, complete —
/// callers handle one shape, the same [`ToolCall`] the non-streaming
/// [`ChatResponse`] carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatStreamEvent {
    /// The next piece of assistant content.
    Content(String),
    /// A complete tool invocation, assembled from the streamed
    /// fragments.
    ToolCall(ToolCall),
    /// The stream ended, with the provider's finish reason
    /// (`stop`, `tool_calls`, `length`, ...).
    Finish {
        /// The finish reason, verbatim from the provider.
        reason: String,
    },
}

/// The streamed chat stream — item type
/// `Result<ChatStreamEvent, AiError>`; a yielded `Err` is terminal
/// (the stream ends after it).
pub type ChatStream = Pin<Box<dyn Stream<Item = Result<ChatStreamEvent, AiError>> + Send>>;

/// The streaming chat contract: generate one assistant turn as a
/// stream. Engines must be callable through shared references
/// (`&self`). The request is sent (and its status checked) before the
/// call resolves; the returned stream yields
/// [`ChatStreamEvent`]s, ending after [`ChatStreamEvent::Finish`] or
/// a terminal `Err`.
pub trait StreamingChatModel: Send + Sync {
    /// Streams the completion.
    fn chat_stream<'a>(
        &'a self,
        request: ChatRequest,
    ) -> BoxFuture<'a, Result<ChatStream, AiError>>;
}

/// The embedding contract: texts to vectors, in input order.
/// Engines must be callable through shared references (`&self`).
pub trait EmbeddingModel: Send + Sync {
    /// Embeds the texts. `model` names the embedding model —
    /// embedding models usually differ from the chat model — and
    /// `None` falls back to the engine's configured default.
    fn embed<'a>(
        &'a self,
        model: Option<&'a str>,
        texts: &'a [&'a str],
    ) -> BoxFuture<'a, Result<Vec<Vec<f32>>, AiError>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_message_serializes_bare() {
        let value = serde_json::to_value(Message::user("hi")).unwrap();
        assert_eq!(value, serde_json::json!({"role": "user", "content": "hi"}));
    }

    #[test]
    fn tool_message_carries_the_call_id() {
        let value = serde_json::to_value(Message::tool("call_1", "21℃")).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"role": "tool", "content": "21℃", "tool_call_id": "call_1"})
        );
    }

    #[test]
    fn assistant_tool_calls_round_trip_the_wire() {
        let message = Message {
            role: Role::Assistant,
            content: None,
            tool_calls: vec![ToolCall {
                id: "call_1".to_string(),
                kind: ToolKind::Function,
                function: ToolCallFunction {
                    name: "get_weather".to_string(),
                    arguments: "{\"city\":\"Hangzhou\"}".to_string(),
                },
            }],
            tool_call_id: None,
        };
        let wire = serde_json::to_value(&message).unwrap();
        assert_eq!(wire["tool_calls"][0]["type"], "function");
        assert_eq!(wire["tool_calls"][0]["function"]["name"], "get_weather");
        assert_eq!(wire["tool_call_id"], serde_json::Value::Null);
        let back: Message = serde_json::from_value(wire).unwrap();
        assert_eq!(back, message);
    }

    #[test]
    fn tool_definitions_serialize_the_openai_envelope() {
        let tool = Tool::function(
            "get_weather",
            "Look up weather",
            serde_json::json!({"type": "object"}),
        );
        let value = serde_json::to_value(&tool).unwrap();
        assert_eq!(value["type"], "function");
        assert_eq!(value["function"]["name"], "get_weather");
        assert_eq!(value["function"]["description"], "Look up weather");
    }

    #[test]
    fn config_parses_snake_case_and_requires_the_type() {
        let config: Config = serde_json::from_str(
            r#"{"model_type": "cloud", "model_name": "gpt-4o", "cloud": {"api_key": "sk"}}"#,
        )
        .unwrap();
        assert_eq!(config.model_type, ModelType::Cloud);
        assert_eq!(config.model_name, "gpt-4o");
        assert_eq!(config.timeout_seconds, 0);

        assert!(serde_json::from_str::<Config>(r#"{"model_name": "x"}"#).is_err());
        assert!(serde_json::from_str::<Config>(r#"{"model_type": 2}"#).is_err());
    }

    #[test]
    fn local_config_wire_defaults_match_go_zero_values() {
        let local: LocalConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(local.host, "localhost");
        assert_eq!(local.port, 11434);
    }

    #[test]
    fn error_displays_readably() {
        assert_eq!(
            AiError::MissingCloudConfig.to_string(),
            "ai: cloud config is missing"
        );
        assert_eq!(
            AiError::Status {
                code: 429,
                body: "rate limited".to_string()
            }
            .to_string(),
            "ai: HTTP 429: rate limited"
        );
    }

    /// The three faces are object safe: heterogenous engine
    /// collections (`Arc<dyn ...>`) are the deployment shape.
    #[test]
    fn the_three_faces_are_object_safe() {
        use std::sync::Arc;

        fn takes_chat(_: Arc<dyn ChatModel>) {}
        fn takes_streaming(_: Arc<dyn StreamingChatModel>) {}
        fn takes_embedding(_: Arc<dyn EmbeddingModel>) {}
        takes_chat(Arc::new(NoopModel));
        takes_streaming(Arc::new(NoopModel));
        takes_embedding(Arc::new(NoopModel));
    }

    struct NoopModel;

    impl ChatModel for NoopModel {
        fn chat<'a>(
            &'a self,
            _request: ChatRequest,
        ) -> BoxFuture<'a, Result<ChatResponse, AiError>> {
            Box::pin(async { Err(AiError::EmptyModel) })
        }
    }

    impl StreamingChatModel for NoopModel {
        fn chat_stream<'a>(
            &'a self,
            _request: ChatRequest,
        ) -> BoxFuture<'a, Result<ChatStream, AiError>> {
            Box::pin(async { Err(AiError::EmptyModel) })
        }
    }

    impl EmbeddingModel for NoopModel {
        fn embed<'a>(
            &'a self,
            _model: Option<&'a str>,
            _texts: &'a [&'a str],
        ) -> BoxFuture<'a, Result<Vec<Vec<f32>>, AiError>> {
            Box::pin(async { Err(AiError::EmptyModel) })
        }
    }
}
