//! Apollo config source for the RushWind configuration contract.
//!
//! The wire behavior is hand-written over reqwest — no Apollo SDK
//! dependency; the two REST faces the config contract needs are small:
//!
//! - **Read** — `GET {server}/configs/{appId}/{cluster}/{namespace}`:
//!   200 carries `{"configurations": {…}}`, served as the canonical
//!   JSON encoding of that map (key-sorted — a BTreeMap — so the same
//!   release always yields the same bytes, and a watch tick means the
//!   content truly changed); 404 is the contract's `Ok(None)`
//!   "absent" answer; anything else is an error. A `.yml`/`.properties`
//!   namespace arrives as a one-entry `content` map — Apollo's own
//!   shape, passed through as-is.
//! - **Watch** — `GET {server}/notifications/v2` long-polling with the
//!   namespace's `notificationId`: a 200 with a grown id re-reads the
//!   namespace and pushes the value when its bytes changed; a 304 (the
//!   server's "nothing changed in the hold window") loops. The
//!   long-poll request's timeout is the hold window plus the read
//!   timeout, so the server's 60-second hold does not read as client
//!   failure.
//!
//! The key a [`Source::load`] receives is the namespace name
//! (`application`, `application.yml`, …); an empty key uses the
//! configured default namespace.
//!
//! # Engine notes
//!
//! - The watch loop sleeps a small gap between long-poll cycles — a
//!   misbehaving server that answers instantly instead of holding
//!   would otherwise turn the loop into a hot spin.
//! - A send failure (the stream's receiver dropped) ends the loop, the
//!   config-http stance: no polling a channel nobody reads.
//! - No `from_settings` face yet — the bootstrap assembly has not
//!   adopted this engine; it grows the face when it does.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::time::Duration;

use rushwind_config::{BoxFuture, ConfigError, Source, ValueStream};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct ApolloOptions {
    /// The Apollo config-service base URL
    /// (`http://apollo-config:8080`).
    pub server: String,
    /// The application id Apollo knows the namespaces under.
    pub app_id: String,
    /// The cluster name. Default `default`.
    pub cluster: String,
    /// The namespace a load with an empty key reads. Default
    /// `application`.
    pub namespace: String,
    /// The client ip Apollo's dashboard attributes reads to.
    pub ip: Option<String>,
    /// The long-poll hold the server applies (its own default is 60 s);
    /// the watch request timeout is this plus [`Self::request_timeout`].
    /// Default 60 s.
    pub poll_timeout: Duration,
    /// The per-request timeout for config reads. Default 10 s.
    pub request_timeout: Duration,
    /// The gap between long-poll cycles. Default 1 s — real servers
    /// hold for the poll window, so the gap only matters against
    /// instant-answer ones.
    pub poll_gap: Duration,
}

impl Default for ApolloOptions {
    fn default() -> Self {
        Self {
            server: String::new(),
            app_id: String::new(),
            cluster: "default".to_string(),
            namespace: "application".to_string(),
            ip: None,
            poll_timeout: Duration::from_secs(60),
            request_timeout: Duration::from_secs(10),
            poll_gap: Duration::from_secs(1),
        }
    }
}

/// The `configurations` body of a config read.
#[derive(Debug, Deserialize)]
struct ConfigurationsResponse {
    #[serde(default)]
    configurations: BTreeMap<String, String>,
}

/// One namespace entry on the notifications wire.
#[derive(Debug, Serialize, Deserialize)]
struct Notification {
    #[serde(rename = "namespaceName")]
    namespace_name: String,
    #[serde(rename = "notificationId")]
    notification_id: u64,
}

/// The Apollo configuration source.
pub struct ApolloSource {
    options: ApolloOptions,
    http: reqwest::Client,
}

impl ApolloSource {
    /// Builds the engine; fails when the server or app id is unset —
    /// loads would have to guess, a construction error.
    pub fn new(options: ApolloOptions) -> Result<Self, ConfigError> {
        if options.server.is_empty() {
            return Err(ConfigError::Failed("apollo: server unset".to_string()));
        }
        if options.app_id.is_empty() {
            return Err(ConfigError::Failed("apollo: app_id unset".to_string()));
        }
        Ok(Self {
            options,
            http: reqwest::Client::new(),
        })
    }

    /// The namespace a load's key resolves to: the key itself, or the
    /// configured default for an empty key.
    fn resolve_namespace<'a>(&'a self, key: &'a str) -> Result<&'a str, ConfigError> {
        if key.is_empty() && self.options.namespace.is_empty() {
            return Err(ConfigError::Failed(
                "apollo: no namespace specified".to_string(),
            ));
        }
        Ok(if key.is_empty() {
            &self.options.namespace
        } else {
            key
        })
    }

    /// One config read: the namespace's canonical JSON bytes, or
    /// `None` when Apollo answers 404 (namespace absent).
    async fn read_namespace(&self, namespace: &str) -> Result<Option<Vec<u8>>, ConfigError> {
        let url = format!(
            "{}/configs/{}/{}/{}",
            self.options.server, self.options.app_id, self.options.cluster, namespace
        );
        let mut request = self.http.get(&url).timeout(self.options.request_timeout);
        if let Some(ip) = &self.options.ip {
            request = request.query(&[("ip", ip)]);
        }
        let response = request
            .send()
            .await
            .map_err(|e| ConfigError::Failed(format!("apollo read {namespace}: {e}")))?;
        let status = response.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ConfigError::Failed(format!(
                "apollo read {namespace}: HTTP {status}"
            )));
        }
        let body = read_bounded(response, namespace).await?;
        let parsed: ConfigurationsResponse = serde_json::from_slice(&body)
            .map_err(|e| ConfigError::Failed(format!("apollo read {namespace}: {e}")))?;
        // Canonical, key-sorted JSON: identical releases yield identical
        // bytes, so watch ticks mean real change.
        serde_json::to_vec(&parsed.configurations)
            .map(Some)
            .map_err(|e| ConfigError::Failed(format!("apollo read {namespace}: {e}")))
    }
}

/// The largest accepted response body — the config-http stance: a
/// config endpoint gone wrong must not turn a read (or a long-poll
/// loop) into an unbounded buffer.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Reads a successful response under [`MAX_BODY_BYTES`].
async fn read_bounded(mut response: reqwest::Response, what: &str) -> Result<Vec<u8>, ConfigError> {
    if let Some(length) = response.content_length() {
        if length as usize > MAX_BODY_BYTES {
            return Err(ConfigError::Failed(format!(
                "apollo {what}: body of {length} bytes exceeds the {MAX_BODY_BYTES} limit"
            )));
        }
    }
    let mut body = Vec::new();
    loop {
        let chunk = response
            .chunk()
            .await
            .map_err(|e| ConfigError::Failed(format!("apollo {what}: {e}")))?;
        let Some(chunk) = chunk else {
            break;
        };
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(ConfigError::Failed(format!(
                "apollo {what}: body exceeds the {MAX_BODY_BYTES} limit"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

impl Source for ApolloSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
        Box::pin(async move {
            let namespace = self.resolve_namespace(key)?;
            self.read_namespace(namespace).await
        })
    }

    /// Long-poll watch: the notifications/v2 loop pushes the
    /// namespace's value whenever its canonical bytes changed.
    fn watch_value<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
        Box::pin(async move {
            let namespace = self.resolve_namespace(key)?.to_string();
            let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let http = self.http.clone();
            let options = self.options.clone();
            tokio::spawn(async move {
                let mut last: Option<Vec<u8>> = None;
                let mut notification_id: u64 = 0;
                let url = format!("{}/notifications/v2", options.server);
                loop {
                    let probe = serde_json::to_string(&[Notification {
                        namespace_name: namespace.clone(),
                        notification_id,
                    }])
                    .unwrap_or_default();
                    let response = http
                        .get(&url)
                        .query(&[
                            ("appId", options.app_id.as_str()),
                            ("cluster", options.cluster.as_str()),
                            ("notifications", probe.as_str()),
                        ])
                        // The server holds for the poll window; give it
                        // the window plus one read timeout so a hold is
                        // never read as client failure.
                        .timeout(options.poll_timeout + options.request_timeout)
                        .send()
                        .await;
                    // The grown id the response carried, when it carried
                    // one; adopted only on a "changed" verdict.
                    let mut new_id = notification_id;
                    let changed = match response {
                        Ok(response) if response.status().as_u16() == 304 => false,
                        Ok(response) if response.status().is_success() => {
                            // An unparseable notifications body reads as
                            // unchanged — never as a crash.
                            read_bounded(response, "watch")
                                .await
                                .ok()
                                .and_then(|body| {
                                    serde_json::from_slice::<Vec<Notification>>(&body).ok()
                                })
                                .and_then(|entries| entries.into_iter().next())
                                .is_some_and(|entry| {
                                    let grew = entry.notification_id > notification_id;
                                    if grew {
                                        new_id = entry.notification_id;
                                    }
                                    grew
                                })
                        }
                        _ => false,
                    };
                    if changed {
                        notification_id = new_id;
                        // The id grew: re-read and push only when the
                        // canonical bytes truly changed.
                        if let Ok(Some(value)) =
                            read_namespace_with(&http, &options, &namespace).await
                        {
                            if last.as_deref() != Some(value.as_slice()) {
                                last = Some(value.clone());
                                // A send failure means the stream (and its
                                // receiver) is gone: end the loop.
                                if tx.send(value).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    tokio::time::sleep(options.poll_gap).await;
                }
            });
            Ok(Box::new(ApolloValueStream { rx }) as Box<dyn ValueStream>)
        })
    }
}

/// The namespace read the watch loop runs — [`ApolloSource`]'s method
/// needs `&self`, the spawned loop owns only the parts.
async fn read_namespace_with(
    http: &reqwest::Client,
    options: &ApolloOptions,
    namespace: &str,
) -> Result<Option<Vec<u8>>, ConfigError> {
    let source = ApolloSource {
        options: options.clone(),
        http: http.clone(),
    };
    source.read_namespace(namespace).await
}

/// The long-poll [`ValueStream`]: pushed values from the background
/// loop. Dropping the stream ends it.
struct ApolloValueStream {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl ValueStream for ApolloValueStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.rx.recv().await })
    }
}
