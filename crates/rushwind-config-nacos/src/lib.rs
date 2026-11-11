//! Nacos config source for the RushWind configuration contract.
//!
//! The wire behavior is hand-written over reqwest — no Nacos SDK
//! dependency; the engine speaks the one REST face the config contract
//! needs, Nacos's v1 config API:
//!
//! - **Read** — `GET {server}/nacos/v1/cs/configs?dataId={dataId}&group={group}`
//!   (plus `tenant={namespace}` when a namespace is configured, and
//!   `accessToken={token}` when one is set): 200 carries the config's
//!   raw content — the stored bytes verbatim, the same release always
//!   the same bytes; 404 is the contract's `Ok(None)` "absent" answer;
//!   anything else is an error.
//!
//! - **Watch** — poll-based, the config-http shape: the read re-issued
//!   on a cadence, values pushed when the bytes changed. The v1
//!   contentMD5 long-poll would cut push latency to the server's hold
//!   window but drags an md5 dependency in for it; the poll cadence is
//!   the honest default for a config source (the sdk's own v2 face is
//!   gRPC, a different engine entirely).
//!
//! The key a [`Source::load`] receives is the dataId (`app.properties`,
//! `app.yml`, …); an empty key uses the configured default dataId.
//! Group and namespace (tenant) come from the options.
//!
//! # Engine notes
//!
//! - Auth is an access token, passed through as `accessToken` — the
//!   deployment obtains it (Nacos's login endpoint issues one); the
//!   engine does not run the login flow.
//! - A send failure (the stream's receiver dropped) ends the poll
//!   loop, the config-http stance: no polling a channel nobody reads.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::time::Duration;

use rushwind_config::{BoxFuture, ConfigError, Source, ValueStream};
use tokio::sync::mpsc;

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct NacosOptions {
    /// The Nacos server base URL (`http://nacos:8848`).
    pub server: String,
    /// The dataId a load with an empty key reads.
    pub data_id: String,
    /// The Nacos group. Default `DEFAULT_GROUP`.
    pub group: String,
    /// The Nacos namespace (tenant). Empty reads the public namespace.
    pub namespace: String,
    /// The access token, when the server requires auth.
    pub access_token: Option<String>,
    /// The poll cadence for the watch. Default 30 s.
    pub poll_interval: Duration,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for NacosOptions {
    fn default() -> Self {
        Self {
            server: String::new(),
            data_id: String::new(),
            group: "DEFAULT_GROUP".to_string(),
            namespace: String::new(),
            access_token: None,
            poll_interval: Duration::from_secs(30),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// The Nacos configuration source.
pub struct NacosSource {
    options: NacosOptions,
    http: reqwest::Client,
}

impl NacosSource {
    /// Builds the engine; fails when the server or the default dataId
    /// is unset — loads would have to guess, a construction error.
    pub fn new(options: NacosOptions) -> Result<Self, ConfigError> {
        if options.server.is_empty() {
            return Err(ConfigError::Failed("nacos: server unset".to_string()));
        }
        if options.data_id.is_empty() {
            return Err(ConfigError::Failed("nacos: data_id unset".to_string()));
        }
        Ok(Self {
            options,
            http: reqwest::Client::new(),
        })
    }

    /// The dataId a load's key resolves to: the key itself, or the
    /// configured default for an empty key.
    fn resolve_data_id<'a>(&'a self, key: &'a str) -> Result<&'a str, ConfigError> {
        if key.is_empty() && self.options.data_id.is_empty() {
            return Err(ConfigError::Failed(
                "nacos: no dataId specified".to_string(),
            ));
        }
        Ok(if key.is_empty() {
            &self.options.data_id
        } else {
            key
        })
    }

    /// One config read: the stored content bytes, or `None` when
    /// Nacos answers 404 (config absent).
    async fn read_config(&self, data_id: &str) -> Result<Option<Vec<u8>>, ConfigError> {
        let url = format!("{}/nacos/v1/cs/configs", self.options.server);
        let mut query: Vec<(&str, &str)> =
            vec![("dataId", data_id), ("group", self.options.group.as_str())];
        if !self.options.namespace.is_empty() {
            query.push(("tenant", self.options.namespace.as_str()));
        }
        if let Some(token) = &self.options.access_token {
            query.push(("accessToken", token));
        }
        let response = self
            .http
            .get(&url)
            .query(&query)
            .timeout(self.options.request_timeout)
            .send()
            .await
            .map_err(|e| ConfigError::Failed(format!("nacos read {data_id}: {e}")))?;
        let status = response.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ConfigError::Failed(format!(
                "nacos read {data_id}: HTTP {status}"
            )));
        }
        read_bounded(response, data_id).await.map(Some)
    }
}

/// The largest accepted response body — the config-http stance: a
/// config endpoint gone wrong must not turn a read (or a poll loop)
/// into an unbounded buffer.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Reads a successful response under [`MAX_BODY_BYTES`].
async fn read_bounded(mut response: reqwest::Response, what: &str) -> Result<Vec<u8>, ConfigError> {
    if let Some(length) = response.content_length() {
        if length as usize > MAX_BODY_BYTES {
            return Err(ConfigError::Failed(format!(
                "nacos {what}: body of {length} bytes exceeds the {MAX_BODY_BYTES} limit"
            )));
        }
    }
    let mut body = Vec::new();
    loop {
        let chunk = response
            .chunk()
            .await
            .map_err(|e| ConfigError::Failed(format!("nacos {what}: {e}")))?;
        let Some(chunk) = chunk else {
            break;
        };
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(ConfigError::Failed(format!(
                "nacos {what}: body exceeds the {MAX_BODY_BYTES} limit"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

impl Source for NacosSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
        Box::pin(async move {
            let data_id = self.resolve_data_id(key)?;
            self.read_config(data_id).await
        })
    }

    /// Poll-based watch: the read re-issued on the cadence, values
    /// pushed when the bytes changed (the first read pushes).
    fn watch_value<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
        Box::pin(async move {
            let data_id = self.resolve_data_id(key)?.to_string();
            let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let http = self.http.clone();
            let options = self.options.clone();
            tokio::spawn(async move {
                let source = NacosSource { options, http };
                let mut last: Option<Vec<u8>> = None;
                loop {
                    if let Ok(Some(body)) = source.read_config(&data_id).await {
                        if last.as_deref() != Some(body.as_slice()) {
                            last = Some(body.clone());
                            // A send failure means the stream (and its
                            // receiver) is gone: end the task instead
                            // of polling a channel nobody reads.
                            if tx.send(body).is_err() {
                                break;
                            }
                        }
                    }
                    tokio::time::sleep(source.options.poll_interval).await;
                }
            });
            Ok(Box::new(NacosValueStream { rx }) as Box<dyn ValueStream>)
        })
    }
}

/// The poll-based [`ValueStream`]: pushed values from the background
/// loop. Dropping the stream ends it.
struct NacosValueStream {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl ValueStream for NacosValueStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.rx.recv().await })
    }
}
