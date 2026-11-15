//! The memory reference engine's semantic pins: submit/status/cancel,
//! the complete() test hook, and the worker's run-to-terminal with the
//! stop switch.

use std::sync::Arc;
use std::time::Duration;

use rushwind_workflow::{Phase, WorkflowClient, WorkflowError, WorkflowSpec, WorkflowWorker};
use rushwind_workflow_memory::{MemoryWorkflowEngine, MemoryWorkflowWorker};

fn spec(name: &str) -> WorkflowSpec {
    WorkflowSpec::new(serde_json::json!({"kind": "demo"})).named(name)
}

#[tokio::test]
async fn submit_mints_and_status_reads() {
    let engine = MemoryWorkflowEngine::new();
    let handle = WorkflowClient::submit(&engine, &spec("wf-1"))
        .await
        .expect("submit");
    assert_eq!(handle.name, "wf-1");
    let status = WorkflowClient::status(&engine, &handle)
        .await
        .expect("status");
    assert_eq!(status.phase, Phase::Running);
}

#[tokio::test]
async fn complete_drives_the_phase() {
    let engine = MemoryWorkflowEngine::new();
    let handle = WorkflowClient::submit(&engine, &spec("wf-2"))
        .await
        .expect("submit");
    engine
        .complete(&handle, Phase::Succeeded)
        .expect("complete");
    let status = WorkflowClient::status(&engine, &handle)
        .await
        .expect("status");
    assert_eq!(status.phase, Phase::Succeeded);
}

#[tokio::test]
async fn cancel_and_missing_read_as_typed() {
    let engine = MemoryWorkflowEngine::new();
    let handle = WorkflowClient::submit(&engine, &spec("wf-3"))
        .await
        .expect("submit");
    WorkflowClient::cancel(&engine, &handle)
        .await
        .expect("cancel");
    let status = WorkflowClient::status(&engine, &handle)
        .await
        .expect("status");
    assert_eq!(status.phase, Phase::Cancelled);

    let err = WorkflowClient::status(
        &engine,
        &rushwind_workflow::WorkflowHandle {
            name: "absent".to_string(),
            namespace: String::new(),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, WorkflowError::NotFound));
}

#[tokio::test]
async fn worker_runs_to_terminal_and_stop_cancels() {
    let engine = Arc::new(MemoryWorkflowEngine::new());
    let worker = Arc::new(MemoryWorkflowWorker::new(engine.clone()));

    // Run to a driven terminal phase.
    let engine_for_run = engine.clone();
    let worker_for_run = worker.clone();
    let runner =
        tokio::spawn(async move { WorkflowWorker::run(&*worker_for_run, &spec("wf-run")).await });
    // Give the run loop a beat to submit, then drive the phase.
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
        let store = engine_for_run
            .status(&rushwind_workflow::WorkflowHandle {
                name: "wf-run".to_string(),
                namespace: String::new(),
            })
            .await
            .expect("submitted");
        assert_eq!(store.phase, Phase::Running);
    }
    engine_for_run
        .complete(
            &rushwind_workflow::WorkflowHandle {
                name: "wf-run".to_string(),
                namespace: String::new(),
            },
            Phase::Succeeded,
        )
        .expect("complete");
    let outcome = runner.await.expect("run task joins").expect("run succeeds");
    assert_eq!(outcome.phase, Phase::Succeeded);
    assert!(!worker.is_running(), "the run loop is done");
}

#[tokio::test]
async fn worker_stop_cancels_in_flight_runs() {
    let engine = Arc::new(MemoryWorkflowEngine::new());
    let worker = Arc::new(MemoryWorkflowWorker::new(engine.clone()));
    let worker_for_stop = worker.clone();
    let runner =
        tokio::spawn(async move { WorkflowWorker::run(&*worker_for_stop, &spec("wf-stop")).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(worker.is_running(), "the run loop is live");

    worker.stop().expect("stop");
    let outcome = runner.await.expect("run task joins");
    assert!(matches!(outcome, Err(WorkflowError::Cancelled)));
    assert!(!worker.is_running(), "stopped means no live runs");
}
