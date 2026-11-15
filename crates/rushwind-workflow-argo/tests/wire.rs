//! Wire pins for the Argo Workflows engine: the submit path (manifest
//! and namespace stamping), the status phase mapping, the terminate
//! cancel, 404 mapping to NotFound, and the bearer token.

#![cfg(test)]

use std::time::Duration;

use rushwind_workflow::{Phase, WorkflowClient, WorkflowError, WorkflowHandle, WorkflowSpec};
use rushwind_workflow_argo::{ArgoOptions, ArgoWorkflowEngine};

mod common;
use common::spawn_argo;

fn engine(server: &str) -> ArgoWorkflowEngine {
    ArgoWorkflowEngine::new(ArgoOptions {
        server: server.to_string(),
        namespace: "rushwind".to_string(),
        token: Some("sa-token".to_string()),
        request_timeout: Duration::from_secs(5),
        ..ArgoOptions::default()
    })
    .expect("options valid")
}

#[tokio::test]
async fn submit_posts_the_manifest_and_mints_the_handle() {
    let (server, mock) = spawn_argo().await;
    let engine = engine(&server);

    let spec = WorkflowSpec::new(serde_json::json!({
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {"generateName": "hello-"},
        "spec": {"entrypoint": "main"}
    }))
    .named("hello-1")
    .in_namespace("rushwind");

    let handle = engine.submit(&spec).await.expect("submit");
    assert_eq!(handle.name, "hello-1");
    assert_eq!(handle.namespace, "rushwind");

    let path = mock.last_path();
    assert_eq!(path, "/api/v1/workflows/rushwind", "{path}");
    assert_eq!(mock.last_bearer(), "Bearer sa-token");
}

#[tokio::test]
async fn status_maps_the_argo_phases() {
    let (server, mock) = spawn_argo().await;
    let engine = engine(&server);
    mock.set_phase("rushwind", "wf-1", common::Phase::Succeeded);

    let handle = WorkflowHandle {
        name: "wf-1".to_string(),
        namespace: "rushwind".to_string(),
    };
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Succeeded);

    // A live workflow reads Running.
    mock.set_phase("rushwind", "wf-2", common::Phase::Running);
    let status = engine
        .status(&WorkflowHandle {
            name: "wf-2".to_string(),
            namespace: "rushwind".to_string(),
        })
        .await
        .expect("status");
    assert_eq!(status.phase, Phase::Running);
}

#[tokio::test]
async fn missing_workflows_read_not_found() {
    let (server, _mock) = spawn_argo().await;
    let engine = engine(&server);
    let handle = WorkflowHandle {
        name: "absent".to_string(),
        namespace: "rushwind".to_string(),
    };
    let err = engine.status(&handle).await.unwrap_err();
    assert!(matches!(err, WorkflowError::NotFound), "{err:?}");
    let err = engine.cancel(&handle).await.unwrap_err();
    assert!(matches!(err, WorkflowError::NotFound), "{err:?}");
}

#[tokio::test]
async fn cancel_terminates_through_argo() {
    let (server, mock) = spawn_argo().await;
    let engine = engine(&server);
    let handle = WorkflowHandle {
        name: "wf-1".to_string(),
        namespace: "rushwind".to_string(),
    };
    mock.set_phase("rushwind", "wf-1", common::Phase::Running);

    engine.cancel(&handle).await.expect("terminate");
    // The mock drove the phase to Cancelled.
    let status = engine.status(&handle).await.expect("status");
    assert_eq!(status.phase, Phase::Cancelled);
}

#[tokio::test]
async fn nameless_manifests_without_generate_name_refuse() {
    let (server, _mock) = spawn_argo().await;
    let engine = engine(&server);
    let spec = WorkflowSpec::new(serde_json::json!({"spec": {"entrypoint": "main"}}));
    let err = engine.submit(&spec).await.unwrap_err();
    assert!(
        matches!(err, WorkflowError::InvalidQuery(ref m) if m.contains("generateName")),
        "{err:?}"
    );
}
