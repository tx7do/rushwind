//! Argo Workflows engine for the RushWind workflow contract — the
//! Argo Server REST API over hand-written reqwest, no SDK (the
//! house's k8s-configmap/nacos precedent).
//!
//! # The mapping
//!
//! - **Submit** — `POST /api/v1/workflows/{namespace}` with the spec's
//!   manifest as the body; the created Workflow's
//!   `metadata.name` comes back as the handle. A spec without a name
//!   rides the manifest's `generateName` (Argo mints one); a nameless
//!   manifest without `generateName` refuses as
//!   [`WorkflowError::InvalidQuery`] — the engine does not invent
//!   names it cannot honor.
//! - **Status** — `GET /api/v1/workflows/{namespace}/{name}`; the
//!   `status.phase` string maps onto [`Phase`] (`Running`,
//!   `Succeeded`, `Failed`, `Cancelled`; anything else reads
//!   `Unknown`). 404 is [`WorkflowError::NotFound`].
//! - **Cancel** — `POST /api/v1/workflows/{namespace}/{name}/terminate`
//!   (Argo's terminate face); 404 is [`WorkflowError::NotFound`].
//! - **Auth** — a bearer token the deployment supplies (the
//!   service-account or gateway JWT; the lifecycle is the deployer's,
//!   the OIDC engine's stance).
//!
//! The worker polls status on an interval until a terminal phase,
//! honoring its stop switch; `is_running` reflects live poll loops.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_workflow::{
    BoxFuture, Phase, WorkflowClient, WorkflowError, WorkflowHandle, WorkflowSpec, WorkflowStatus,
    WorkflowWorker,
};
use serde_json::json;
use serde_json::Value;

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct ArgoOptions {
    /// The Argo Server base URL (`https://argo.example:2746`).
    pub server: String,
    /// The namespace workflows live in. Default `default`.
    pub namespace: String,
    /// The bearer token, when the server requires auth.
    pub token: Option<String>,
    /// The worker's poll cadence. Default 2 s.
    pub poll_interval: Duration,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for ArgoOptions {
    fn default() -> Self {
        Self {
            server: String::new(),
            namespace: "default".to_string(),
            token: None,
            poll_interval: Duration::from_secs(2),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// The Argo Workflows engine.
#[derive(Debug)]
pub struct ArgoWorkflowEngine {
    options: ArgoOptions,
    http: reqwest::Client,
    /// The worker's stop switch + live poll count.
    worker: Arc<WorkerState>,
}

#[derive(Debug, Default)]
struct WorkerState {
    stopped: AtomicBool,
    runs: AtomicU64,
    /// The poll cadence (mutable so the worker's options can differ
    /// from the client's).
    poll_interval: Mutex<Duration>,
}

impl ArgoWorkflowEngine {
    /// Builds the engine; fails when the server is unset.
    pub fn new(options: ArgoOptions) -> Result<Self, WorkflowError> {
        if options.server.trim().is_empty() {
            return Err(WorkflowError::InvalidQuery(
                "argo: server unset".to_string(),
            ));
        }
        Ok(Self {
            options: ArgoOptions {
                server: options.server.trim_end_matches('/').to_string(),
                ..options
            },
            http: reqwest::Client::new(),
            worker: Arc::new(WorkerState {
                poll_interval: Mutex::new(options.poll_interval),
                ..WorkerState::default()
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape —
    /// snake_case (`server`, `namespace`, `token`, `poll_interval_ms`),
    /// unknown fields ignored.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, WorkflowError> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct SettingsWire {
            server: String,
            namespace: Option<String>,
            token: Option<String>,
            poll_interval_ms: Option<u64>,
        }
        let wire: SettingsWire = serde_json::from_value(settings)
            .map_err(|e| WorkflowError::InvalidQuery(format!("argo settings: {e}")))?;
        Self::new(ArgoOptions {
            server: wire.server,
            namespace: wire.namespace.unwrap_or_else(|| "default".to_string()),
            token: wire.token,
            poll_interval: wire
                .poll_interval_ms
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_secs(2)),
            request_timeout: Duration::from_secs(10),
        })
    }

    /// The worker half over this engine (shares the HTTP client).
    pub fn worker(&self) -> ArgoWorkflowWorker {
        ArgoWorkflowWorker {
            engine: Arc::new(Self {
                options: self.options.clone(),
                http: self.http.clone(),
                worker: Arc::clone(&self.worker),
            }),
        }
    }

    /// The namespace a spec resolves to: the spec's, or the engine's
    /// default.
    fn resolve_namespace(&self, spec: &WorkflowSpec) -> String {
        if spec.namespace.is_empty() {
            return self.options.namespace.clone();
        }
        spec.namespace.clone()
    }

    /// One authenticated request; 404 maps to NotFound, the rest of
    /// non-2xx fails.
    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
        context: &str,
    ) -> Result<serde_json::Value, WorkflowError> {
        let url = format!("{}{}", self.options.server, path);
        let mut request = self
            .http
            .request(method, &url)
            .timeout(self.options.request_timeout);
        if let Some(token) = &self.options.token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = &body {
            request =
                request
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(body).map_err(|e| {
                        WorkflowError::Failed(format!("argo {context} encode: {e}"))
                    })?);
        }
        let response = request
            .send()
            .await
            .map_err(|e| WorkflowError::Failed(format!("argo {context}: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| WorkflowError::Failed(format!("argo {context}: {e}")))?;
        if status.as_u16() == 404 {
            return Err(WorkflowError::NotFound);
        }
        if !status.is_success() {
            return Err(WorkflowError::Failed(format!(
                "argo {context}: HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            )));
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| WorkflowError::Failed(format!("argo {context}: {e}")))
    }
}

impl WorkflowClient for ArgoWorkflowEngine {
    fn submit<'a>(
        &'a self,
        spec: &'a WorkflowSpec,
    ) -> BoxFuture<'a, Result<WorkflowHandle, WorkflowError>> {
        Box::pin(async move {
            let namespace = self.resolve_namespace(spec);
            let mut manifest = spec.manifest.clone();
            // The name rides the manifest: the spec's, or the
            // manifest's own generateName.
            let named = spec.name.clone().or_else(|| {
                manifest
                    .get("metadata")
                    .and_then(|meta| meta.get("generateName"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
            if named.is_none()
                && manifest
                    .get("metadata")
                    .and_then(|m| m.get("generateName"))
                    .is_none()
            {
                return Err(WorkflowError::InvalidQuery(
                    "argo: a workflow needs a name or a generateName in its manifest".to_string(),
                ));
            }
            if let (Some(meta), Some(name)) = (manifest.get_mut("metadata"), named.as_ref()) {
                if let Some(meta) = meta.as_object_mut() {
                    meta.insert("name".to_string(), json!(name));
                    meta.insert("namespace".to_string(), json!(namespace));
                }
            }
            let created = self
                .send(
                    reqwest::Method::POST,
                    &format!("/api/v1/workflows/{namespace}"),
                    Some(manifest),
                    &format!("submit {namespace}"),
                )
                .await?;
            let minted = created
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    WorkflowError::Failed("argo submit: no metadata.name in the response".into())
                })?;
            Ok(WorkflowHandle {
                name: minted.to_string(),
                namespace,
            })
        })
    }

    fn status<'a>(
        &'a self,
        handle: &'a WorkflowHandle,
    ) -> BoxFuture<'a, Result<WorkflowStatus, WorkflowError>> {
        Box::pin(async move {
            let body = self
                .send(
                    reqwest::Method::GET,
                    &format!("/api/v1/workflows/{}/{}", handle.namespace, handle.name),
                    None,
                    &format!("status {}", handle.name),
                )
                .await?;
            let phase = body
                .pointer("/status/phase")
                .and_then(Value::as_str)
                .map(parse_phase)
                .unwrap_or(Phase::Unknown);
            Ok(WorkflowStatus {
                handle: handle.clone(),
                phase,
            })
        })
    }

    fn cancel<'a>(
        &'a self,
        handle: &'a WorkflowHandle,
    ) -> BoxFuture<'a, Result<(), WorkflowError>> {
        Box::pin(async move {
            self.send(
                reqwest::Method::POST,
                &format!(
                    "/api/v1/workflows/{}/{}/terminate",
                    handle.namespace, handle.name
                ),
                Some(json!({})),
                &format!("cancel {}", handle.name),
            )
            .await
            .map(|_| ())
        })
    }

    fn close(&self) -> Result<(), WorkflowError> {
        // Connectionless REST: nothing to release.
        Ok(())
    }
}

/// Maps Argo's phase strings onto the contract's.
fn parse_phase(phase: &str) -> Phase {
    match phase {
        "Running" => Phase::Running,
        "Succeeded" => Phase::Succeeded,
        "Failed" => Phase::Failed,
        "Cancelled" => Phase::Cancelled,
        _ => Phase::Unknown,
    }
}

/// The Argo workflow worker: submits through the shared client and
/// polls status to a terminal phase, honoring the stop switch.
#[derive(Debug)]
pub struct ArgoWorkflowWorker {
    engine: Arc<ArgoWorkflowEngine>,
}

impl WorkflowWorker for ArgoWorkflowWorker {
    fn run<'a>(
        &'a self,
        spec: &'a WorkflowSpec,
    ) -> BoxFuture<'a, Result<WorkflowStatus, WorkflowError>> {
        Box::pin(async move {
            self.engine.worker.runs.fetch_add(1, Ordering::SeqCst);
            let outcome = async {
                let handle = WorkflowClient::submit(&*self.engine, spec).await?;
                let interval = *self
                    .engine
                    .worker
                    .poll_interval
                    .lock()
                    .expect("poll interval poisoned");
                loop {
                    if self.engine.worker.stopped.load(Ordering::SeqCst) {
                        let _ = WorkflowClient::cancel(&*self.engine, &handle).await;
                        return Err(WorkflowError::Cancelled);
                    }
                    let status = WorkflowClient::status(&*self.engine, &handle).await?;
                    if is_terminal(status.phase) {
                        return Ok(status);
                    }
                    tokio::time::sleep(interval).await;
                }
            };
            let outcome = outcome.await;
            self.engine.worker.runs.fetch_sub(1, Ordering::SeqCst);
            outcome
        })
    }

    fn stop(&self) -> Result<(), WorkflowError> {
        self.engine.worker.stopped.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.engine.worker.runs.load(Ordering::SeqCst) > 0
    }
}

/// Whether a phase ends a run loop.
fn is_terminal(phase: Phase) -> bool {
    matches!(phase, Phase::Succeeded | Phase::Failed | Phase::Cancelled)
}
