//! Temporal engine for the RushWind workflow contract — the server's
//! gRPC-gateway HTTP API (JSON over HTTP/1.1) over hand-written
//! reqwest, no SDK.
//!
//! # The mapping
//!
//! The spec's manifest carries the StartWorkflowExecutionRequest
//! fields the caller owns — `workflowType` (with its `name`),
//! `taskQueue` (with its `name`), `input`, and any policy fields; the
//! contract transports it untouched.
//!
//! - **Submit** — `POST /api/v1/namespaces/{namespace}/workflows/{id}`
//!   with the manifest; the handle rides the path's workflow id. A
//!   spec without a name (and a manifest without `workflowId`) refuses
//!   — Temporal's ids are caller-minted, the engine does not invent
//!   them.
//! - **Status** — `GET /api/v1/namespaces/{namespace}/workflows/{id}`
//!   → `workflowExecutionInfo.status`, the gRPC enum's wire name
//!   (`WORKFLOW_EXECUTION_STATUS_RUNNING` …), mapped onto [`Phase`].
//!   `WORKFLOW_EXECUTION_STATUS_TIMED_OUT` reads `Failed` (a dead
//!   run is a dead run); `CONTINUED_AS_NEW` reads `Running` (the
//!   successor is the same logical workflow).
//! - **Cancel** — `POST /api/v1/namespaces/{namespace}/workflows/{id}/terminate`
//!   with a reason.
//! - **Auth** — a bearer token the deployment supplies (the
//!   api-key/Authorization header shape Temporal's HTTP gateway
//!   accepts); namespace comes from the options or the spec.
//! - **Worker** — polls describe on an interval to a terminal phase,
//!   honoring the stop switch.

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

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct TemporalOptions {
    /// The Temporal server HTTP base URL (`http://127.0.0.1:8233` —
    /// the gRPC-gateway port).
    pub server: String,
    /// The namespace workflows live in. Default `default`.
    pub namespace: String,
    /// The bearer token (the api-key/gateway JWT), when required.
    pub token: Option<String>,
    /// The worker's poll cadence. Default 2 s.
    pub poll_interval: Duration,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for TemporalOptions {
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

/// The Temporal workflow engine.
#[derive(Debug)]
pub struct TemporalWorkflowEngine {
    options: TemporalOptions,
    http: reqwest::Client,
    worker: Arc<WorkerState>,
}

#[derive(Debug, Default)]
struct WorkerState {
    stopped: AtomicBool,
    runs: AtomicU64,
    poll_interval: Mutex<Duration>,
}

/// The namespace a spec resolves to: the spec's, or the engine's
/// default.
fn resolve_namespace(spec: &WorkflowSpec, default: &str) -> String {
    if spec.namespace.is_empty() {
        return default.to_string();
    }
    spec.namespace.clone()
}

/// The workflow id: the spec's name, or the manifest's `workflowId`.
fn resolve_id(spec: &WorkflowSpec) -> Result<String, WorkflowError> {
    if let Some(name) = &spec.name {
        if !name.is_empty() {
            return Ok(name.clone());
        }
    }
    if let Some(id) = spec
        .manifest
        .pointer("/workflowId")
        .and_then(serde_json::Value::as_str)
    {
        return Ok(id.to_string());
    }
    Err(WorkflowError::InvalidQuery(
        "temporal: a workflow needs a name (or a manifest workflowId)".to_string(),
    ))
}

/// Maps the gRPC enum's wire name onto the contract's phases.
fn parse_phase(status: &str) -> Phase {
    match status {
        "WORKFLOW_EXECUTION_STATUS_RUNNING" => Phase::Running,
        "WORKFLOW_EXECUTION_STATUS_COMPLETED" => Phase::Succeeded,
        "WORKFLOW_EXECUTION_STATUS_FAILED" => Phase::Failed,
        "WORKFLOW_EXECUTION_STATUS_CANCELED" | "WORKFLOW_EXECUTION_STATUS_TERMINATED" => {
            Phase::Cancelled
        }
        "WORKFLOW_EXECUTION_STATUS_TIMED_OUT" => Phase::Failed,
        "WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW" => Phase::Running,
        _ => Phase::Unknown,
    }
}

impl TemporalWorkflowEngine {
    /// Builds the engine; fails when the server is unset.
    pub fn new(options: TemporalOptions) -> Result<Self, WorkflowError> {
        if options.server.trim().is_empty() {
            return Err(WorkflowError::InvalidQuery(
                "temporal: server unset".to_string(),
            ));
        }
        Ok(Self {
            options: TemporalOptions {
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
            .map_err(|e| WorkflowError::InvalidQuery(format!("temporal settings: {e}")))?;
        Self::new(TemporalOptions {
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
    pub fn worker(&self) -> TemporalWorkflowWorker {
        TemporalWorkflowWorker {
            engine: Arc::new(Self {
                options: self.options.clone(),
                http: self.http.clone(),
                worker: Arc::clone(&self.worker),
            }),
        }
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
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(body).map_err(|e| {
                    WorkflowError::Failed(format!("temporal {context} encode: {e}"))
                })?);
        }
        let response = request
            .send()
            .await
            .map_err(|e| WorkflowError::Failed(format!("temporal {context}: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| WorkflowError::Failed(format!("temporal {context}: {e}")))?;
        if status.as_u16() == 404 {
            return Err(WorkflowError::NotFound);
        }
        if !status.is_success() {
            return Err(WorkflowError::Failed(format!(
                "temporal {context}: HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            )));
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| WorkflowError::Failed(format!("temporal {context}: {e}")))
    }
}

impl WorkflowClient for TemporalWorkflowEngine {
    fn submit<'a>(
        &'a self,
        spec: &'a WorkflowSpec,
    ) -> BoxFuture<'a, Result<WorkflowHandle, WorkflowError>> {
        Box::pin(async move {
            let id = resolve_id(spec)?;
            let namespace = resolve_namespace(spec, &self.options.namespace);
            let mut manifest = spec.manifest.clone();
            if let Some(meta) = manifest.as_object_mut() {
                meta.entry("workflowId".to_string())
                    .or_insert_with(|| json!(id));
                meta.entry("namespace".to_string())
                    .or_insert_with(|| json!(namespace));
            }
            self.send(
                reqwest::Method::POST,
                &format!("/api/v1/namespaces/{namespace}/workflows/{id}"),
                Some(manifest),
                &format!("start {id}"),
            )
            .await?;
            Ok(WorkflowHandle {
                name: id,
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
                    &format!(
                        "/api/v1/namespaces/{}/workflows/{}",
                        handle.namespace, handle.name
                    ),
                    None,
                    &format!("describe {}", handle.name),
                )
                .await?;
            let phase = body
                .pointer("/workflowExecutionInfo/status")
                .and_then(serde_json::Value::as_str)
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
                    "/api/v1/namespaces/{}/workflows/{}/terminate",
                    handle.namespace, handle.name
                ),
                Some(json!({})),
                &format!("terminate {}", handle.name),
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

/// The Temporal workflow worker: submits through the shared client and
/// polls describe to a terminal phase, honoring the stop switch.
#[derive(Debug)]
pub struct TemporalWorkflowWorker {
    engine: Arc<TemporalWorkflowEngine>,
}

impl WorkflowWorker for TemporalWorkflowWorker {
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
