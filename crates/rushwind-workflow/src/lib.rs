//! Workflow contract for RushWind — the upstream go-wind workflow
//! module's lifecycle face (`Client.Close`, `Worker.Stop`,
//! `Worker.IsRunning`) grown the minimal surface a framework caller
//! needs: submit a workflow, read its status, cancel it, and a
//! worker's run-to-terminal loop.
//!
//! # The core shapes
//!
//! A [`WorkflowSpec`] is an **opaque JSON manifest** plus an optional
//! name — the engine owns what a manifest means (Argo's CRD shape, a
//! local task, anything); the contract transports it untouched.
//! [`WorkflowStatus`] carries the engine-minted handle and a
//! [`Phase`]; phases are the terminal four every workflow system
//! agrees on (`Succeeded`/`Failed`/`Cancelled`) plus `Running` and
//! `Unknown` for engines whose status is momentarily unreadable.
//!
//! [`Client::close`] is the upstream `Client.Close`; [`Worker::stop`]
//! and [`Worker::is_running`] are the upstream `Worker.Stop` /
//! `Worker.IsRunning` — a worker's run loop (`Worker::run` submits
//! and polls to a terminal phase) ends when the workflow reaches a
//! terminal phase, the caller drops the returned future, or `stop`
//! fires.
//!
//! # Engines
//!
//! `rushwind-workflow-memory` is the in-process reference engine (the
//! semantic baseline every external adapter is judged against);
//! `rushwind-workflow-argo` adapts the Argo Server REST API
//! hand-written over reqwest. Engine choice is a constructor detail.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use serde_json::Value;

/// Future type used across the workflow contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

use std::future::Future;
use std::pin::Pin;

/// Errors surfaced by workflow engines.
#[derive(Debug)]
#[non_exhaustive]
pub enum WorkflowError {
    /// The engine could not complete the operation.
    Failed(String),
    /// The workflow (or its handle) does not exist.
    NotFound,
    /// The request is malformed (missing name, empty manifest).
    InvalidQuery(String),
    /// The workflow was cancelled before reaching a terminal phase.
    Cancelled,
}

impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "workflow operation failed: {msg}"),
            Self::NotFound => write!(f, "workflow not found"),
            Self::InvalidQuery(msg) => write!(f, "invalid workflow query: {msg}"),
            Self::Cancelled => write!(f, "workflow cancelled"),
        }
    }
}

impl std::error::Error for WorkflowError {}

/// A workflow to run: the engine's own manifest plus an optional name
/// (engines that mint names leave `None` to the server's naming).
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowSpec {
    /// The engine's own manifest — opaque to the contract.
    pub manifest: Value,
    /// The workflow name; `None` lets the engine (or its server) mint
    /// one.
    pub name: Option<String>,
    /// The namespace the workflow lives in (engine semantics; empty
    /// reads as the engine's default).
    pub namespace: String,
}

impl WorkflowSpec {
    /// Builds a spec from a manifest, unnamed, default namespace.
    pub fn new(manifest: Value) -> Self {
        Self {
            manifest,
            name: None,
            namespace: String::new(),
        }
    }

    /// Names the workflow, builder style.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the namespace, builder style.
    pub fn in_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }
}

/// The engine-minted workflow identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowHandle {
    /// The workflow name (minted by the engine or the spec's).
    pub name: String,
    /// The namespace it lives in.
    pub namespace: String,
}

/// The workflow phase — the terminal four every workflow system agrees
/// on, plus `Running` and `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// The workflow is executing.
    Running,
    /// The workflow finished successfully.
    Succeeded,
    /// The workflow finished unsuccessfully.
    Failed,
    /// The workflow was cancelled before finishing.
    Cancelled,
    /// The engine cannot currently read the phase.
    Unknown,
}

/// One workflow's observed state.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowStatus {
    /// The workflow handle.
    pub handle: WorkflowHandle,
    /// The observed phase.
    pub phase: Phase,
}

/// The workflow client face: submit, observe, cancel, close. The
/// upstream `Client.Close`.
pub trait WorkflowClient: Send + Sync {
    /// Submits the workflow and returns its minted handle.
    fn submit<'a>(
        &'a self,
        spec: &'a WorkflowSpec,
    ) -> BoxFuture<'a, Result<WorkflowHandle, WorkflowError>>;

    /// Reads the workflow's observed status.
    fn status<'a>(
        &'a self,
        handle: &'a WorkflowHandle,
    ) -> BoxFuture<'a, Result<WorkflowStatus, WorkflowError>>;

    /// Cancels the workflow; a missing workflow is
    /// [`WorkflowError::NotFound`].
    fn cancel<'a>(&'a self, handle: &'a WorkflowHandle)
        -> BoxFuture<'a, Result<(), WorkflowError>>;

    /// Releases the client's resources. The upstream `Client.Close`.
    fn close(&self) -> Result<(), WorkflowError> {
        Ok(())
    }
}

/// The workflow worker face: run-to-terminal with a stop switch. The
/// upstream `Worker.Stop` / `Worker.IsRunning`.
pub trait WorkflowWorker: Send + Sync {
    /// Submits the workflow and polls it to a terminal phase —
    /// `Running` phases keep polling; terminal phases return. Ends
    /// early (with [`WorkflowError::Cancelled`]) when
    /// [`WorkflowWorker::stop`] fires or the returned future is
    /// dropped.
    fn run<'a>(
        &'a self,
        spec: &'a WorkflowSpec,
    ) -> BoxFuture<'a, Result<WorkflowStatus, WorkflowError>>;

    /// Stops the worker's in-flight runs. The upstream `Worker.Stop`.
    /// In-flight runs end with [`WorkflowError::Cancelled`].
    fn stop(&self) -> Result<(), WorkflowError>;

    /// Whether the worker is running (has live in-flight runs). The
    /// upstream `Worker.IsRunning`.
    fn is_running(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The upstream lifecycle face is object safe: heterogeneous
    /// collections are the deployment shape.
    #[test]
    fn the_faces_are_object_safe() {
        use std::sync::Arc;
        fn takes_client(_: Arc<dyn WorkflowClient>) {}
        fn takes_worker(_: Arc<dyn WorkflowWorker>) {}
        struct Noop;
        impl WorkflowClient for Noop {
            fn submit<'a>(
                &'a self,
                _spec: &'a WorkflowSpec,
            ) -> BoxFuture<'a, Result<WorkflowHandle, WorkflowError>> {
                Box::pin(async { Err(WorkflowError::Failed("noop".into())) })
            }
            fn status<'a>(
                &'a self,
                _handle: &'a WorkflowHandle,
            ) -> BoxFuture<'a, Result<WorkflowStatus, WorkflowError>> {
                Box::pin(async { Err(WorkflowError::NotFound) })
            }
            fn cancel<'a>(
                &'a self,
                _handle: &'a WorkflowHandle,
            ) -> BoxFuture<'a, Result<(), WorkflowError>> {
                Box::pin(async { Err(WorkflowError::NotFound) })
            }
        }
        impl WorkflowWorker for Noop {
            fn run<'a>(
                &'a self,
                _spec: &'a WorkflowSpec,
            ) -> BoxFuture<'a, Result<WorkflowStatus, WorkflowError>> {
                Box::pin(async { Err(WorkflowError::Cancelled) })
            }
            fn stop(&self) -> Result<(), WorkflowError> {
                Ok(())
            }
            fn is_running(&self) -> bool {
                false
            }
        }
        takes_client(Arc::new(Noop));
        takes_worker(Arc::new(Noop));
    }
}
