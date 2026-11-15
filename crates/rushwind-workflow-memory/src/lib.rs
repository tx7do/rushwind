//! In-memory reference engine for the RushWind workflow contract —
//! the semantic baseline every external workflow adapter is judged
//! against.
//!
//! # The shape
//!
//! Submissions land in an in-process store keyed by handle; phases
//! move only when the test (or deployment) drives them through
//! [`MemoryWorkflowEngine::complete`] — the engine has no execution
//! semantics of its own, because the contract's manifests are opaque
//! and running them is the external system's job. `Worker::run` polls
//! the stored phase on an interval until terminal, honoring the
//! worker's stop switch; `is_running` reflects live runs.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_workflow::{
    BoxFuture, Phase, WorkflowClient, WorkflowError, WorkflowHandle, WorkflowSpec, WorkflowStatus,
    WorkflowWorker,
};

/// The poll cadence `Worker::run` uses between status reads. Default
/// 100 ms — the reference engine's phase changes are in-process and
/// instant, so the cadence only bounds the wakeups.

#[derive(Debug, Default)]
struct Inner {
    /// name → phase (the store; one namespace folded in — the memory
    /// engine has no multi-tenancy to model).
    workflows: Mutex<BTreeMap<String, Phase>>,
    /// The worker's stop switch.
    stopped: AtomicBool,
    /// Live run-loop count.
    runs: AtomicU64,
    /// The poll cadence.
    poll_interval: Mutex<Duration>,
}

/// The in-memory workflow engine.
#[derive(Debug, Default)]
pub struct MemoryWorkflowEngine {
    inner: Arc<Inner>,
}

impl Default for MemoryWorkflowWorker {
    fn default() -> Self {
        Self::new(Arc::new(MemoryWorkflowEngine::default()))
    }
}

impl MemoryWorkflowWorker {
    /// Builds a worker over the shared engine.
    pub fn new(engine: Arc<MemoryWorkflowEngine>) -> Self {
        Self { engine }
    }
}

/// The in-memory workflow worker: polls the shared engine's store.
#[derive(Debug)]
pub struct MemoryWorkflowWorker {
    engine: Arc<MemoryWorkflowEngine>,
}

impl MemoryWorkflowEngine {
    /// Builds the engine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the poll cadence future workers use.
    pub fn with_poll_interval(self, interval: Duration) -> Self {
        *self.inner.poll_interval.lock().expect("poll interval") = interval;
        self
    }

    /// Drives a workflow's phase directly — the test hook the
    /// reference engine's honesty rests on (manifests are opaque; the
    /// engine does not execute them). A missing workflow is
    /// [`WorkflowError::NotFound`].
    pub fn complete(&self, handle: &WorkflowHandle, phase: Phase) -> Result<(), WorkflowError> {
        let mut workflows = self.inner.workflows.lock().expect("workflows poisoned");
        let Some(slot) = workflows.get_mut(&handle.name) else {
            return Err(WorkflowError::NotFound);
        };
        *slot = phase;
        Ok(())
    }

    /// The live run count — `is_running`'s source.
    pub fn live_runs(&self) -> u64 {
        self.inner.runs.load(Ordering::SeqCst)
    }
}

impl WorkflowClient for MemoryWorkflowEngine {
    fn submit<'a>(
        &'a self,
        spec: &'a WorkflowSpec,
    ) -> BoxFuture<'a, Result<WorkflowHandle, WorkflowError>> {
        Box::pin(async move {
            let name = match &spec.name {
                Some(name) if !name.is_empty() => name.clone(),
                Some(_) => {
                    return Err(WorkflowError::InvalidQuery(
                        "workflow name is empty".to_string(),
                    ))
                }
                None => format!("wf-{}", uuid_mint()),
            };
            let mut workflows = self.inner.workflows.lock().expect("workflows poisoned");
            if workflows.contains_key(&name) {
                return Err(WorkflowError::Failed(format!(
                    "workflow {name} already exists"
                )));
            }
            workflows.insert(name.clone(), Phase::Running);
            Ok(WorkflowHandle {
                name,
                namespace: spec.namespace.clone(),
            })
        })
    }

    fn status<'a>(
        &'a self,
        handle: &'a WorkflowHandle,
    ) -> BoxFuture<'a, Result<WorkflowStatus, WorkflowError>> {
        Box::pin(async move {
            let workflows = self.inner.workflows.lock().expect("workflows poisoned");
            let Some(phase) = workflows.get(&handle.name) else {
                return Err(WorkflowError::NotFound);
            };
            Ok(WorkflowStatus {
                handle: handle.clone(),
                phase: *phase,
            })
        })
    }

    fn cancel<'a>(
        &'a self,
        handle: &'a WorkflowHandle,
    ) -> BoxFuture<'a, Result<(), WorkflowError>> {
        Box::pin(async move { self.complete(handle, Phase::Cancelled) })
    }

    fn close(&self) -> Result<(), WorkflowError> {
        Ok(())
    }
}

impl WorkflowWorker for MemoryWorkflowWorker {
    fn run<'a>(
        &'a self,
        spec: &'a WorkflowSpec,
    ) -> BoxFuture<'a, Result<WorkflowStatus, WorkflowError>> {
        Box::pin(async move {
            self.engine.inner.runs.fetch_add(1, Ordering::SeqCst);
            let handle = match rushwind_workflow::WorkflowClient::submit(&*self.engine, spec).await
            {
                Ok(handle) => handle,
                Err(e) => {
                    self.engine.inner.runs.fetch_sub(1, Ordering::SeqCst);
                    return Err(e);
                }
            };
            let interval = *self
                .engine
                .inner
                .poll_interval
                .lock()
                .expect("poll interval");
            let outcome = loop {
                if self.engine.inner.stopped.load(Ordering::SeqCst) {
                    let _ = rushwind_workflow::WorkflowClient::cancel(&*self.engine, &handle).await;
                    break Err(WorkflowError::Cancelled);
                }
                match rushwind_workflow::WorkflowClient::status(&*self.engine, &handle).await {
                    Ok(status) if is_terminal(status.phase) => break Ok(status),
                    Ok(_) => {}
                    Err(e) => break Err(e),
                }
                tokio::time::sleep(interval).await;
            };
            self.engine.inner.runs.fetch_sub(1, Ordering::SeqCst);
            outcome
        })
    }

    fn stop(&self) -> Result<(), WorkflowError> {
        self.engine.inner.stopped.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.engine.inner.runs.load(Ordering::SeqCst) > 0
    }
}

/// Whether a phase ends a run loop.
fn is_terminal(phase: Phase) -> bool {
    matches!(phase, Phase::Succeeded | Phase::Failed | Phase::Cancelled)
}

/// A process-unique suffix for minted names — nanos, good enough for
/// a reference engine (collisions error as Failed on submit).
fn uuid_mint() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{}-{}", nanos, COUNTER.fetch_add(1, Ordering::SeqCst))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_mints_and_status_reads() {
        let engine = MemoryWorkflowEngine::new();
        let handle = futures_block(WorkflowClient::submit(
            &engine,
            &WorkflowSpec::new(serde_json::json!({"kind": "demo"})).named("wf-1"),
        ))
        .expect("submit");
        assert_eq!(handle.name, "wf-1");
        let status = futures_block(WorkflowClient::status(&engine, &handle)).expect("status");
        assert_eq!(status.phase, Phase::Running);
    }

    #[test]
    fn complete_drives_the_phase() {
        let engine = MemoryWorkflowEngine::new();
        let handle = futures_block(WorkflowClient::submit(
            &engine,
            &WorkflowSpec::new(serde_json::json!({"kind": "demo"})).named("wf-2"),
        ))
        .expect("submit");
        engine
            .complete(&handle, Phase::Succeeded)
            .expect("complete");
        let status = futures_block(WorkflowClient::status(&engine, &handle)).expect("status");
        assert_eq!(status.phase, Phase::Succeeded);
    }

    #[test]
    fn cancel_and_missing_read_as_typed() {
        let engine = MemoryWorkflowEngine::new();
        let handle = futures_block(WorkflowClient::submit(
            &engine,
            &WorkflowSpec::new(serde_json::json!({"kind": "demo"})).named("wf-3"),
        ))
        .expect("submit");
        futures_block(rushwind_workflow::WorkflowClient::cancel(&engine, &handle)).expect("cancel");
        let status = futures_block(WorkflowClient::status(&engine, &handle)).expect("status");
        assert_eq!(status.phase, Phase::Cancelled);

        let err = futures_block(WorkflowClient::status(
            &engine,
            &WorkflowHandle {
                name: "absent".to_string(),
                namespace: String::new(),
            },
        ))
        .unwrap_err();
        assert!(matches!(err, WorkflowError::NotFound));
    }

    fn futures_block<T>(future: impl std::future::Future<Output = T>) -> T {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        rt.block_on(future)
    }
}
