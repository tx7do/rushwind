//! Netflix/Orkes Conductor engine for the RushWind workflow contract
//! — the server's REST API over hand-written reqwest, no SDK (the
//! argo/temporal precedent).
//!
//! # The mapping
//!
//! The spec's manifest carries the StartWorkflowRequest fields the
//! caller owns — `name` (the workflow definition's name, required),
//! `version`, `input`, `correlationId`, `taskToDomain`, any policy
//! fields; the contract transports it untouched.
//!
//! - **Submit** — `POST /workflow` with the manifest. The engine
//!   stamps `workflowId` when the spec has a name (caller-minted);
//!   conductor's ids are also server-mintable — a spec without a name
//!   submits without one and the response's minted id becomes the
//!   handle (the honest difference from argo/temporal, whose ids are
//!   caller-owned). A manifest without `name` refuses as
//!   [`WorkflowError::InvalidQuery`] — the engine does not invent
//!   workflow types. The response body is the id as plain text.
//! - **Status** — `GET /workflow/{id}`; the `status` field's wire
//!   names map onto [`Phase`]: `RUNNING` (and `PAUSED` — a paused run
//!   is alive) read `Running`, `COMPLETED` reads `Succeeded`,
//!   `FAILED` and `TIMED_OUT` read `Failed` (a dead run is a dead
//!   run), `TERMINATED` and `CANCELED` read `Cancelled`, unknown
//!   strings read `Unknown`. 404 is [`WorkflowError::NotFound`].
//! - **Cancel** — `DELETE /workflow/{id}` (conductor's terminate
//!   face); 404 is [`WorkflowError::NotFound`].
//! - **Auth** — a bearer token the deployment supplies; its
//!   lifecycle is the deployer's (the gcpubsub/OIDC token stance).
//! - **Namespaces** — conductor has none; the spec's namespace rides
//!   the handle verbatim and stays off the wire.
//! - **Worker** — polls status on an interval to a terminal phase,
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
use serde_json::Value;

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct ConductorOptions {
    /// The Conductor server base URL (`http://conductor.example:8080`).
    pub server: String,
    /// The bearer token, when the server requires auth.
    pub token: Option<String>,
    /// The worker's poll cadence. Default 2 s.
    pub poll_interval: Duration,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for ConductorOptions {
    fn default() -> Self {
        Self {
            server: String::new(),
            token: None,
            poll_interval: Duration::from_secs(2),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// The Conductor workflow engine.
#[derive(Debug)]
pub struct ConductorWorkflowEngine {
    options: ConductorOptions,
    http: reqwest::Client,
    worker: Arc<WorkerState>,
}

#[derive(Debug, Default)]
struct WorkerState {
    stopped: AtomicBool,
    runs: AtomicU64,
    poll_interval: Mutex<Duration>,
}

/// Maps the status enum's wire name onto the contract's phases.
fn parse_phase(status: &str) -> Phase {
    match status {
        "RUNNING" | "PAUSED" => Phase::Running,
        "COMPLETED" => Phase::Succeeded,
        "FAILED" | "TIMED_OUT" => Phase::Failed,
        "TERMINATED" | "CANCELED" => Phase::Cancelled,
        _ => Phase::Unknown,
    }
}

/// Whether a phase ends a run loop.
fn is_terminal(phase: Phase) -> bool {
    matches!(phase, Phase::Succeeded | Phase::Failed | Phase::Cancelled)
}

impl ConductorWorkflowEngine {
    /// Builds the engine; fails when the server is unset.
    pub fn new(options: ConductorOptions) -> Result<Self, WorkflowError> {
        if options.server.trim().is_empty() {
            return Err(WorkflowError::InvalidQuery(
                "conductor: server unset".to_string(),
            ));
        }
        Ok(Self {
            options: ConductorOptions {
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
    /// snake_case (`server`, `token`, `poll_interval_ms`), unknown
    /// fields ignored.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, WorkflowError> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct SettingsWire {
            server: String,
            token: Option<String>,
            poll_interval_ms: Option<u64>,
        }
        let wire: SettingsWire = serde_json::from_value(settings)
            .map_err(|e| WorkflowError::InvalidQuery(format!("conductor settings: {e}")))?;
        Self::new(ConductorOptions {
            server: wire.server,
            token: wire.token,
            poll_interval: wire
                .poll_interval_ms
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_secs(2)),
            request_timeout: Duration::from_secs(10),
        })
    }

    /// The worker half over this engine (shares the HTTP client).
    pub fn worker(&self) -> ConductorWorkflowWorker {
        ConductorWorkflowWorker {
            engine: Arc::new(Self {
                options: self.options.clone(),
                http: self.http.clone(),
                worker: Arc::clone(&self.worker),
            }),
        }
    }

    /// One authenticated request; 404 maps to NotFound, the rest of
    /// non-2xx fails. Returns the response bytes — the start's body
    /// is plain text (the minted id), the rest is JSON.
    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
        context: &str,
    ) -> Result<(u16, Vec<u8>), WorkflowError> {
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
                    WorkflowError::Failed(format!("conductor {context} encode: {e}"))
                })?);
        }
        let response = request
            .send()
            .await
            .map_err(|e| WorkflowError::Failed(format!("conductor {context}: {e}")))?;
        let status = response.status().as_u16();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| WorkflowError::Failed(format!("conductor {context}: {e}")))?;
        if status == 404 {
            return Err(WorkflowError::NotFound);
        }
        if !(200..300).contains(&status) {
            return Err(WorkflowError::Failed(format!(
                "conductor {context}: HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        Ok((status, bytes.to_vec()))
    }
}

impl WorkflowClient for ConductorWorkflowEngine {
    fn submit<'a>(
        &'a self,
        spec: &'a WorkflowSpec,
    ) -> BoxFuture<'a, Result<WorkflowHandle, WorkflowError>> {
        Box::pin(async move {
            let mut manifest = spec.manifest.clone();
            // The workflow type is the caller's knowledge; the engine
            // never invents it.
            if manifest
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .is_empty()
            {
                return Err(WorkflowError::InvalidQuery(
                    "conductor: the manifest needs the workflow definition's name".to_string(),
                ));
            }
            // The instance id: the spec's name when it has one;
            // conductor mints one otherwise.
            if let Some(name) = &spec.name {
                if !name.is_empty() {
                    if let Some(meta) = manifest.as_object_mut() {
                        meta.entry("workflowId".to_string())
                            .or_insert_with(|| serde_json::json!(name));
                    }
                }
            }
            let (status, bytes) = self
                .send(reqwest::Method::POST, "/workflow", Some(manifest), "start")
                .await?;
            if !(200..300).contains(&status) {
                return Err(WorkflowError::Failed(format!(
                    "conductor start: HTTP {status}"
                )));
            }
            // The response body is the id as plain text.
            let id = String::from_utf8_lossy(&bytes).trim().to_string();
            if id.is_empty() {
                return Err(WorkflowError::Failed(
                    "conductor start: the server returned no id".to_string(),
                ));
            }
            Ok(WorkflowHandle {
                name: id,
                namespace: spec.namespace.clone(),
            })
        })
    }

    fn status<'a>(
        &'a self,
        handle: &'a WorkflowHandle,
    ) -> BoxFuture<'a, Result<WorkflowStatus, WorkflowError>> {
        Box::pin(async move {
            let (_, bytes) = self
                .send(
                    reqwest::Method::GET,
                    &format!("/workflow/{}", handle.name),
                    None,
                    &format!("describe {}", handle.name),
                )
                .await?;
            let body: Value = serde_json::from_slice(&bytes)
                .map_err(|e| WorkflowError::Failed(format!("conductor describe: {e}")))?;
            let phase = body
                .get("status")
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
                reqwest::Method::DELETE,
                &format!("/workflow/{}", handle.name),
                None,
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

/// The Conductor workflow worker: submits through the shared client
/// and polls status to a terminal phase, honoring the stop switch.
#[derive(Debug)]
pub struct ConductorWorkflowWorker {
    engine: Arc<ConductorWorkflowEngine>,
}

impl WorkflowWorker for ConductorWorkflowWorker {
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
