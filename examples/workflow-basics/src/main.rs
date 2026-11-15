//! One workflow lifecycle, two engines.
//!
//! The memory reference engine runs always (phases driven by the
//! demo's own `complete()` hook — the reference engine has no
//! execution semantics of its own). The Argo adapter runs when
//! `ARGO_SERVER` names a reachable Argo Server; there the demo
//! submits a real manifest and polls its status.

use std::sync::Arc;
use std::time::Duration;

use rushwind_workflow::{Phase, WorkflowClient, WorkflowSpec, WorkflowWorker};
use rushwind_workflow_memory::{MemoryWorkflowEngine, MemoryWorkflowWorker};

fn manifest() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "spec": {"entrypoint": "main"}
    })
}

async fn memory_demo() {
    println!("--- memory ---");
    let engine =
        Arc::new(MemoryWorkflowEngine::new().with_poll_interval(Duration::from_millis(50)));
    let worker = MemoryWorkflowWorker::new(engine.clone());

    // Submit, observe Running, drive to Succeeded through the test
    // hook (the reference engine has no execution semantics).
    let spec = WorkflowSpec::new(manifest()).named("demo-1");
    let handle = WorkflowClient::submit(&*engine, &spec)
        .await
        .expect("submit");
    let status = WorkflowClient::status(&*engine, &handle)
        .await
        .expect("status");
    println!("submitted: {} ({:?})", status.handle.name, status.phase);
    engine
        .complete(&handle, Phase::Succeeded)
        .expect("complete");

    // The worker: run to terminal — the demo drives the phase from a
    // side task (the worker's poll loop ends the moment it lands).
    let driver_engine = engine.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let handle = rushwind_workflow::WorkflowHandle {
            name: "demo-2".to_string(),
            namespace: String::new(),
        };
        let _ = driver_engine.complete(&handle, Phase::Succeeded);
    });
    let outcome = WorkflowWorker::run(&worker, &WorkflowSpec::new(manifest()).named("demo-2"))
        .await
        .expect("run to terminal");
    println!("worker run: {} ({:?})", outcome.handle.name, outcome.phase);
}

async fn argo_demo() {
    let Ok(server) = std::env::var("ARGO_SERVER") else {
        println!("--- argo --- (skipped: set ARGO_SERVER to run)");
        return;
    };
    println!("--- argo ---");
    let engine =
        rushwind_workflow_argo::ArgoWorkflowEngine::new(rushwind_workflow_argo::ArgoOptions {
            server,
            namespace: "rushwind".to_string(),
            ..Default::default()
        })
        .expect("argo options valid");

    let spec = WorkflowSpec::new(manifest())
        .named("demo-1")
        .in_namespace("rushwind");
    let handle = WorkflowClient::submit(&engine, &spec)
        .await
        .expect("submit");
    let status = WorkflowClient::status(&engine, &handle)
        .await
        .expect("status");
    println!("submitted: {} ({:?})", status.handle.name, status.phase);

    // Cancel drives Argo's terminate face.
    WorkflowClient::cancel(&engine, &handle)
        .await
        .expect("cancel");
    let status = WorkflowClient::status(&engine, &handle)
        .await
        .expect("status");
    println!("after cancel: {:?}", status.phase);
}

#[tokio::main]
async fn main() {
    memory_demo().await;
    argo_demo().await;
}
