//! Kubernetes ConfigMap source for the RushWind configuration contract.
//!
//! The wire behavior is hand-written over reqwest — no Kubernetes
//! client dependency; the engine speaks the one REST face the config
//! contract needs, the core v1 API:
//!
//! - **Read** — `GET {server}/api/v1/namespaces/{namespace}/configmaps/{name}`
//!   (plus `Authorization: Bearer {token}` when a token is set — the
//!   in-cluster service-account shape): 200 carries the ConfigMap's
//!   `data` map, served as the canonical JSON encoding of that map
//!   (key-sorted — a BTreeMap — so the same ConfigMap version always
//!   yields the same bytes, and a watch tick means the content truly
//!   changed); 404 is the contract's `Ok(None)` "absent" answer;
//!   anything else is an error.
//!
//! - **Watch** — poll-based, the config-nacos shape: the read re-issued
//!   on a cadence, values pushed when the canonical bytes changed. The
//!   real watch API is a chunked JSON stream (per-event `{"type":…,
//!   "object":…}` lines); the poll cadence is the honest default for a
//!   config source — the stream face is its own engine when a
//!   deployment needs the latency.
//!
//! The key a [`Source::load`] receives is the ConfigMap name
//! (`app-config`, …); an empty key uses the configured default name.
//! Namespace comes from the options.
//!
//! # Engine notes
//!
//! - A ConfigMap's `data` values are UTF-8 strings; the binary `binaryData`
//!   field is deliberately unread — base64 payloads belong to whoever
//!   mounted them, and mixing the two encodings in one canonical map
//!   would make every read a decode policy.
//! - A send failure (the stream's receiver dropped) ends the poll
//!   loop, the config-http stance: no polling a channel nobody reads.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::time::Duration;

use rushwind_config::{BoxFuture, ConfigError, Source, ValueStream};
use serde::Deserialize;
use tokio::sync::mpsc;

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct KubernetesOptions {
    /// The Kubernetes API server base URL
    /// (`https://kubernetes.default.svc`).
    pub server: String,
    /// The namespace the ConfigMaps live in. Default `default`.
    pub namespace: String,
    /// The ConfigMap a load with an empty key reads.
    pub configmap: String,
    /// The bearer token for the `Authorization` header — the
    /// in-cluster service-account token, or a deployment-issued one.
    pub token: Option<String>,
    /// The poll cadence for the watch. Default 30 s.
    pub poll_interval: Duration,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for KubernetesOptions {
    fn default() -> Self {
        Self {
            server: String::new(),
            namespace: "default".to_string(),
            configmap: String::new(),
            token: None,
            poll_interval: Duration::from_secs(30),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// The ConfigMap wire shape, narrowed to the `data` map.
#[derive(Debug, Deserialize)]
struct ConfigMapWire {
    #[serde(default)]
    data: BTreeMap<String, String>,
}

/// The Kubernetes ConfigMap configuration source.
pub struct KubernetesSource {
    options: KubernetesOptions,
    http: reqwest::Client,
}

impl KubernetesSource {
    /// Builds the engine; fails when the server or the default
    /// ConfigMap name is unset — loads would have to guess, a
    /// construction error.
    pub fn new(options: KubernetesOptions) -> Result<Self, ConfigError> {
        if options.server.is_empty() {
            return Err(ConfigError::Failed("kubernetes: server unset".to_string()));
        }
        if options.configmap.is_empty() {
            return Err(ConfigError::Failed(
                "kubernetes: configmap unset".to_string(),
            ));
        }
        Ok(Self {
            options,
            http: reqwest::Client::new(),
        })
    }

    /// The ConfigMap name a load's key resolves to: the key itself, or
    /// the configured default for an empty key.
    fn resolve_name<'a>(&'a self, key: &'a str) -> Result<&'a str, ConfigError> {
        if key.is_empty() && self.options.configmap.is_empty() {
            return Err(ConfigError::Failed(
                "kubernetes: no configmap specified".to_string(),
            ));
        }
        Ok(if key.is_empty() {
            &self.options.configmap
        } else {
            key
        })
    }

    /// One config read: the ConfigMap's canonical key-sorted `data`
    /// JSON, or `None` when the API server answers 404 (ConfigMap
    /// absent).
    async fn read_configmap(&self, name: &str) -> Result<Option<Vec<u8>>, ConfigError> {
        let url = format!(
            "{}/api/v1/namespaces/{}/configmaps/{name}",
            self.options.server.trim_end_matches('/'),
            self.options.namespace
        );
        let mut request = self.http.get(&url).timeout(self.options.request_timeout);
        if let Some(token) = &self.options.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| ConfigError::Failed(format!("kubernetes read {name}: {e}")))?;
        let status = response.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ConfigError::Failed(format!(
                "kubernetes read {name}: HTTP {status}"
            )));
        }
        let body = read_bounded(response, name).await?;
        let parsed: ConfigMapWire = serde_json::from_slice(&body)
            .map_err(|e| ConfigError::Failed(format!("kubernetes read {name}: {e}")))?;
        // Canonical, key-sorted JSON: identical ConfigMap versions
        // yield identical bytes, so watch ticks mean real change.
        serde_json::to_vec(&parsed.data)
            .map(Some)
            .map_err(|e| ConfigError::Failed(format!("kubernetes read {name}: {e}")))
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
                "kubernetes {what}: body of {length} bytes exceeds the {MAX_BODY_BYTES} limit"
            )));
        }
    }
    let mut body = Vec::new();
    loop {
        let chunk = response
            .chunk()
            .await
            .map_err(|e| ConfigError::Failed(format!("kubernetes {what}: {e}")))?;
        let Some(chunk) = chunk else {
            break;
        };
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(ConfigError::Failed(format!(
                "kubernetes {what}: body exceeds the {MAX_BODY_BYTES} limit"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

impl Source for KubernetesSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
        Box::pin(async move {
            let name = self.resolve_name(key)?;
            self.read_configmap(name).await
        })
    }

    /// Poll-based watch: the read re-issued on the cadence, values
    /// pushed when the canonical bytes changed (the first read pushes).
    fn watch_value<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
        Box::pin(async move {
            let name = self.resolve_name(key)?.to_string();
            let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let http = self.http.clone();
            let options = self.options.clone();
            tokio::spawn(async move {
                let source = KubernetesSource { options, http };
                let mut last: Option<Vec<u8>> = None;
                loop {
                    if let Ok(Some(value)) = source.read_configmap(&name).await {
                        if last.as_deref() != Some(value.as_slice()) {
                            last = Some(value.clone());
                            // A send failure means the stream (and its
                            // receiver) is gone: end the task instead
                            // of polling a channel nobody reads.
                            if tx.send(value).is_err() {
                                break;
                            }
                        }
                    }
                    tokio::time::sleep(source.options.poll_interval).await;
                }
            });
            Ok(Box::new(KubernetesValueStream { rx }) as Box<dyn ValueStream>)
        })
    }
}

/// The poll-based [`ValueStream`]: pushed values from the background
/// loop. Dropping the stream ends it.
struct KubernetesValueStream {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl ValueStream for KubernetesValueStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.rx.recv().await })
    }
}
