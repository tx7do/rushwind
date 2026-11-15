//! Wire pins for the Temporal engine: the start path (manifest +
//! workflowId/namespace stamping), the describe phase mapping (gRPC
//! enum wire names), terminate cancellation, 404 → NotFound, and the
//! bearer token.

#![cfg(test)]

use std::time::Duration;

use rushwind_workflow::{Phase, WorkflowClient, WorkflowError, WorkflowHandle, WorkflowSpec};
use rushwind_workflow_temporal::{TemporalOptions, TemporalWorkflowEngine};

mod common;
use common::spawn_temporal;

fn engine(server: &str) -> TemporalWorkflowEngine {
    TemporalWorkflowEngine::new(TemporalOptions {
        server: server.to_string(),
        namespace: "rushwind".to_string(),
        token: Some("sa-token".to_string()),
        request_timeout: Duration::from_secs(5),
        ..TemporalOptions::default()
    })
    .expect("options valid")
}

fn manifest() -> serde_json::Value {
    serde_json::json!({
        "workflowType": {"name": "HelloWorkflow"},
        "taskQueue": {"name": "rushwind-tq"},
        "input": {"payloads": []}
    })
}

#[tokio::test]
async fn start_posts_the_manifest_with_id_and_namespace() {
    let (server, mock) = spawn_temporal().await;
    let engine = engine(&server);

    let spec = WorkflowSpec::new(manifest())
        .named("hello-1")
        .in_namespace("rushwind");
    let handle = engine.submit(&spec).await.expect("submit");
    assert_eq!(handle.name, "hello-1");
    assert_eq!(handle.namespace, "rushwind");

    // The manifest gained the stamped workflowId/namespace; the type
    // name rode untouched.
    let stored = mock.last_start_body();
    assert_eq!(stored["workflowId"], serde_json::json!("hello-1"));
    assert_eq!(stored["namespace"], serde_json::json!("rushwind"));
    assert_eq!(
        stored["workflowType"]["name"],
        serde_json::json!("HelloWorkflow")
    );
    assert_eq!(mock.last_bearer(), "Bearer sa-token");
}

#[tokio::test]
async fn describe_maps_the_grpc_enum_names() {
    let (server, mock) = spawn_temporal().await;
    let engine = engine(&server);
    let handle = WorkflowHandle {
        name: "hello-1".to_string(),
        namespace: "rushwind".to_string(),
    };

    engine
        .submit(
            &WorkflowSpec::new(manifest())
                .named("hello-1")
                .in_namespace("rushwind"),
        )
        .await
        .expect("submit");

    // Running (the birth status).
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Running);

    // Completed → Succeeded.
    mock.set_status("rushwind", "hello-1", common::ExecutionStatus::Completed);
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Succeeded);

    // TimedOut reads Failed — a dead run is a dead run.
    mock.set_status("rushwind", "hello-1", common::ExecutionStatus::TimedOut);
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Failed);

    // Canceled reads Cancelled.
    mock.set_status("rushwind", "hello-1", common::ExecutionStatus::Canceled);
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Cancelled);
}

#[tokio::test]
async fn terminate_cancels_and_missing_reads_not_found() {
    let (server, _mock) = spawn_temporal().await;
    let engine = engine(&server);
    let handle = WorkflowHandle {
        name: "wf-1".to_string(),
        namespace: "rushwind".to_string(),
    };
    // Plant a workflow directly through a start.
    engine
        .submit(
            &WorkflowSpec::new(manifest())
                .named("wf-1")
                .in_namespace("rushwind"),
        )
        .await
        .expect("submit");

    engine.cancel(&handle).await.expect("terminate");
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Cancelled);

    let missing = WorkflowHandle {
        name: "absent".to_string(),
        namespace: "rushwind".to_string(),
    };
    let err = engine.status(&missing).await.unwrap_err();
    assert!(matches!(err, WorkflowError::NotFound), "{err:?}");
    let err = engine.cancel(&missing).await.unwrap_err();
    assert!(matches!(err, WorkflowError::NotFound), "{err:?}");
}

#[tokio::test]
async fn a_nameless_workflow_without_manifest_id_refuses() {
    let (server, _mock) = spawn_temporal().await;
    let engine = engine(&server);
    // No spec name and no manifest workflowId: Temporal's ids are
    // caller-minted, the engine does not invent them.
    let spec = WorkflowSpec::new(manifest());
    let err = engine.submit(&spec).await.unwrap_err();
    assert!(
        matches!(err, WorkflowError::InvalidQuery(ref m) if m.contains("name")),
        "{err:?}"
    );
}
