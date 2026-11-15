//! Wire pins for the Conductor engine: the start path (the manifest
//! riding untouched with the stamped caller id, or the server-minted
//! one), the describe phase mapping (the status enum's wire names),
//! terminate cancellation, 404 → NotFound, the nameless-manifest
//! refusal, and the bearer token.

#![cfg(test)]

use std::time::Duration;

use rushwind_workflow::{Phase, WorkflowClient, WorkflowError, WorkflowHandle, WorkflowSpec};
use rushwind_workflow_conductor::{ConductorOptions, ConductorWorkflowEngine};

mod common;
use common::spawn_conductor;

fn engine(server: &str) -> ConductorWorkflowEngine {
    ConductorWorkflowEngine::new(ConductorOptions {
        server: server.to_string(),
        token: Some("cd-token".to_string()),
        request_timeout: Duration::from_secs(5),
        ..ConductorOptions::default()
    })
    .expect("options valid")
}

fn manifest() -> serde_json::Value {
    serde_json::json!({
        "name": "hello_workflow",
        "version": 1,
        "input": {"orderId": 7},
        "correlationId": "corr-1",
    })
}

#[tokio::test]
async fn submit_stamps_the_caller_id_and_sends_the_manifest() {
    let (server, mock) = spawn_conductor().await;
    let engine = engine(&server);

    let spec = WorkflowSpec::new(manifest()).named("hello-1");
    let handle = engine.submit(&spec).await.expect("submit");
    assert_eq!(handle.name, "hello-1");

    // The manifest rode untouched; the engine stamped the caller's id.
    let stored = mock.last_start_body();
    assert_eq!(stored["workflowId"], serde_json::json!("hello-1"));
    assert_eq!(stored["name"], serde_json::json!("hello_workflow"));
    assert_eq!(stored["input"]["orderId"], serde_json::json!(7));
    assert_eq!(stored["correlationId"], serde_json::json!("corr-1"));
    assert_eq!(mock.last_bearer(), "Bearer cd-token");
}

#[tokio::test]
async fn a_nameless_submit_lets_the_server_mint() {
    let (server, mock) = spawn_conductor().await;
    let engine = engine(&server);

    // The honest difference from argo/temporal: conductor's ids are
    // server-mintable — the submit carries no workflowId and the
    // minted one becomes the handle.
    let spec = WorkflowSpec::new(manifest());
    let handle = engine.submit(&spec).await.expect("submit");
    assert!(handle.name.starts_with("wf-"), "{}", handle.name);

    let stored = mock.last_start_body();
    assert!(
        stored.get("workflowId").is_none(),
        "the engine must not mint ids: {stored:?}"
    );

    // The minted handle describes.
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Running);
}

#[tokio::test]
async fn a_manifest_without_name_refuses() {
    let (server, _mock) = spawn_conductor().await;
    let engine = engine(&server);
    // The workflow definition's name is the caller's knowledge; the
    // engine does not invent it.
    let spec = WorkflowSpec::new(serde_json::json!({"input": {}}));
    let err = engine.submit(&spec).await.unwrap_err();
    assert!(
        matches!(err, WorkflowError::InvalidQuery(ref m) if m.contains("name")),
        "{err:?}"
    );
}

#[tokio::test]
async fn describe_maps_the_status_enum_wire_names() {
    let (server, mock) = spawn_conductor().await;
    let engine = engine(&server);
    let handle = WorkflowHandle {
        name: "hello-1".to_string(),
        namespace: String::new(),
    };

    engine
        .submit(&WorkflowSpec::new(manifest()).named("hello-1"))
        .await
        .expect("submit");

    // Running (the birth status).
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Running);

    // PAUSED reads Running — a paused run is alive.
    mock.set_status("hello-1", "PAUSED");
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Running);

    // COMPLETED → Succeeded.
    mock.set_status("hello-1", "COMPLETED");
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Succeeded);

    // TIMED_OUT reads Failed — a dead run is a dead run.
    mock.set_status("hello-1", "TIMED_OUT");
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Failed);

    // TERMINATED reads Cancelled.
    mock.set_status("hello-1", "TERMINATED");
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Cancelled);
}

#[tokio::test]
async fn terminate_cancels_and_missing_reads_not_found() {
    let (server, _mock) = spawn_conductor().await;
    let engine = engine(&server);
    let handle = WorkflowHandle {
        name: "wf-1".to_string(),
        namespace: String::new(),
    };
    engine
        .submit(&WorkflowSpec::new(manifest()).named("wf-1"))
        .await
        .expect("submit");

    engine.cancel(&handle).await.expect("terminate");
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Cancelled);

    let missing = WorkflowHandle {
        name: "absent".to_string(),
        namespace: String::new(),
    };
    let err = engine.status(&missing).await.unwrap_err();
    assert!(matches!(err, WorkflowError::NotFound), "{err:?}");
    let err = engine.cancel(&missing).await.unwrap_err();
    assert!(matches!(err, WorkflowError::NotFound), "{err:?}");
}

#[tokio::test]
async fn the_worker_polls_to_terminal() {
    let (server, mock) = spawn_conductor().await;
    let engine = ConductorWorkflowEngine::new(ConductorOptions {
        server: server.clone(),
        poll_interval: Duration::from_millis(20),
        request_timeout: Duration::from_secs(5),
        ..ConductorOptions::default()
    })
    .expect("options valid");
    let worker = engine.worker();

    let run = {
        let worker = &worker;
        let spec = WorkflowSpec::new(manifest()).named("wf-worker");
        async move { rushwind_workflow::WorkflowWorker::run(worker, &spec).await }
    };
    let (outcome, _) = tokio::join!(run, async {
        // The workflow completes while the worker polls.
        tokio::time::sleep(Duration::from_millis(80)).await;
        mock.set_status("wf-worker", "COMPLETED");
    });
    let status = outcome.expect("run to terminal");
    assert_eq!(status.phase, Phase::Succeeded);
    assert_eq!(status.handle.name, "wf-worker");
}

#[tokio::test]
async fn lifecycle_and_options_validation() {
    // The unset server refuses both constructor faces.
    let err = ConductorWorkflowEngine::new(ConductorOptions::default()).expect_err("server unset");
    assert!(
        matches!(err, WorkflowError::InvalidQuery(ref m) if m.contains("server")),
        "{err:?}"
    );
    let err =
        ConductorWorkflowEngine::from_settings(serde_json::json!({})).expect_err("server unset");
    assert!(
        matches!(err, WorkflowError::InvalidQuery(ref m) if m.contains("server")),
        "{err:?}"
    );

    // The settings wire takes the snake_case shape, ignoring unknown
    // fields.
    let engine = ConductorWorkflowEngine::from_settings(serde_json::json!({
        "server": "http://127.0.0.1:8080",
        "token": "t",
        "poll_interval_ms": 500,
        "unknown_field": 1,
    }))
    .expect("settings valid");
    assert!(engine.close().is_ok());
}
