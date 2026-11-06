//! HTTP config source for the RushWind configuration contract, over reqwest.
//!
//! A load is a GET of the key treated as a **full URL** (the caller's
//! key wins, the configured default URL
//! covers an empty key). 2xx responses carry the body as the value;
//! 404 is the contract's `Ok(None)` "absent" answer; anything else is
//! an error.
//!
//! Watch is poll-based, with a 30-second default cadence:
//! a background task re-fetches the URL and emits only when the body
//! changed. [`ConfigOptions::poll_interval`] scales it.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::time::Duration;

use rushwind_config::{BoxFuture, ConfigError, Source, ValueStream};
use tokio::sync::mpsc;

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct HttpOptions {
    /// The default URL used when a load passes an empty key. A load
    /// with a non-empty key uses that key as the URL instead.
    pub url: String,
    /// The request method. Default GET.
    pub method: String,
    /// The poll cadence for the watch. Default 30 s.
    pub poll_interval: Duration,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            method: "GET".to_string(),
            url: String::new(),
            poll_interval: Duration::from_secs(30),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// The HTTP configuration source.
pub struct HttpSource {
    options: HttpOptions,
    http: reqwest::Client,
}

impl HttpSource {
    /// Builds the engine; fails when no default URL is configured and
    /// loads would have to guess — an empty default URL is a
    /// construction error.
    pub fn new(options: HttpOptions) -> Result<Self, ConfigError> {
        if options.url.is_empty() {
            return Err(ConfigError::Failed("http: url invalid".to_string()));
        }
        Ok(Self {
            options,
            http: reqwest::Client::new(),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, ConfigError> {
        #[derive(serde::Deserialize)]
        struct HttpSettings {
            url: Option<String>,
            poll_interval_ms: Option<u64>,
            timeout_ms: Option<u64>,
        }
        let settings: HttpSettings = serde_json::from_value(settings)
            .map_err(|e| ConfigError::Failed(format!("settings parse: {e}")))?;
        let mut options = HttpOptions {
            url: settings.url.unwrap_or_default(),
            ..HttpOptions::default()
        };
        if let Some(ms) = settings.poll_interval_ms {
            options.poll_interval = Duration::from_millis(ms);
        }
        if let Some(ms) = settings.timeout_ms {
            options.request_timeout = Duration::from_millis(ms);
        }
        Self::new(options)
    }

    fn resolve_url<'a>(&'a self, key: &'a str) -> &'a str {
        if key.is_empty() {
            &self.options.url
        } else {
            key
        }
    }

    async fn fetch(&self, url: &str) -> Result<Option<Vec<u8>>, ConfigError> {
        let method = reqwest::Method::from_bytes(self.options.method.as_bytes())
            .map_err(|e| ConfigError::Failed(format!("http: method parse: {e}")))?;
        let response = self
            .http
            .request(method, url)
            .timeout(self.options.request_timeout)
            .send()
            .await
            .map_err(|e| ConfigError::Failed(format!("http get {url}: {e}")))?;
        let status = response.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ConfigError::Failed(format!(
                "http get {url}: HTTP {status}"
            )));
        }
        read_bounded(response, url).await
    }
}

/// The largest accepted response body: a config endpoint gone wrong
/// must not turn a load — or a 30-second poll loop — into an
/// unbounded buffer.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Reads a successful response under [`MAX_BODY_BYTES`], rejecting the
/// body (by declared `Content-Length` first, then by accumulated
/// bytes) instead of buffering whatever the endpoint sends.
async fn read_bounded(
    response: reqwest::Response,
    url: &str,
) -> Result<Option<Vec<u8>>, ConfigError> {
    if let Some(length) = response.content_length() {
        if length as usize > MAX_BODY_BYTES {
            return Err(ConfigError::Failed(format!(
                "http get {url}: body of {length} bytes exceeds the {MAX_BODY_BYTES} limit"
            )));
        }
    }
    let mut body = Vec::new();
    let mut response = response;
    loop {
        let chunk = response
            .chunk()
            .await
            .map_err(|e| ConfigError::Failed(format!("http get {url}: {e}")))?;
        let Some(chunk) = chunk else {
            break;
        };
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(ConfigError::Failed(format!(
                "http get {url}: body exceeds the {MAX_BODY_BYTES} limit"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Some(body))
}

impl Source for HttpSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
        Box::pin(async move {
            let url = self.resolve_url(key);
            self.fetch(url).await
        })
    }

    /// Poll-mode watch: a background task re-fetches the default URL
    /// every cadence tick and pushes the body when it changed.
    fn watch_value<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
        Box::pin(async move {
            let url = self.resolve_url(key).to_string();
            let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let http = self.http.clone();
            let poll_interval = self.options.poll_interval;
            let request_timeout = self.options.request_timeout;
            let method = self.options.method.clone();
            tokio::spawn(async move {
                let mut last: Option<Vec<u8>> = None;
                loop {
                    let request = http
                        .request(
                            reqwest::Method::from_bytes(method.as_bytes())
                                .unwrap_or(reqwest::Method::GET),
                            &url,
                        )
                        .timeout(request_timeout);
                    if let Ok(response) = request.send().await {
                        if response.status().is_success() {
                            if let Ok(Some(body)) = read_bounded(response, &url).await {
                                if last.as_deref() != Some(body.as_slice()) {
                                    last = Some(body.clone());
                                    // A send failure means the stream
                                    // (and its receiver) is gone: end
                                    // the task instead of polling a
                                    // channel nobody reads forever.
                                    if tx.send(body).is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    tokio::time::sleep(poll_interval).await;
                }
            });
            Ok(Box::new(PollValueStream { rx }) as Box<dyn ValueStream>)
        })
    }
}

/// The poll-based [`ValueStream`]: pushed bodies from the background
/// fetch loop. The loop runs for the process lifetime; dropping the
/// stream detaches the caller.
struct PollValueStream {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl ValueStream for PollValueStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.rx.recv().await })
    }
}
