use crate::*;
use rushwind_script::MemSource;
use std::sync::Arc;

async fn engine() -> SharedEngine {
    let engine = factory().expect("engine");
    engine.init().await.expect("init");
    engine
}

#[tokio::test]
async fn arithmetic_evaluates() {
    let engine = engine().await;
    assert_eq!(
        engine.execute_string("t", "1 + 2").await.expect("eval"),
        ScriptValue::Int(3)
    );
}

#[tokio::test]
async fn arrays_and_objects_bridge() {
    let engine = engine().await;
    assert_eq!(
        engine
            .execute_string("t", "[1, 2, 3]")
            .await
            .expect("array"),
        ScriptValue::Array(vec![
            ScriptValue::Int(1),
            ScriptValue::Int(2),
            ScriptValue::Int(3)
        ])
    );
    assert_eq!(
        engine
            .execute_string("t", "({a: 1, b: 'x'})")
            .await
            .expect("object"),
        ScriptValue::Map(HashMap::from([
            ("a".to_string(), ScriptValue::Int(1)),
            ("b".to_string(), ScriptValue::String("x".to_string()))
        ]))
    );
}

#[tokio::test]
async fn execution_returns_the_result_array() {
    let engine = engine().await;
    engine.load_string("a", "1").await.expect("load a");
    engine.load_string("b", "2").await.expect("load b");
    assert_eq!(
        engine.execute().await.expect("array of results"),
        ScriptValue::Array(vec![ScriptValue::Int(1), ScriptValue::Int(2)])
    );
}

#[tokio::test]
async fn globals_bridge_both_ways() {
    let engine = engine().await;
    engine
        .register_global("x", ScriptValue::Int(5))
        .expect("register global");
    assert_eq!(
        engine.get_global("x").expect("get global"),
        ScriptValue::Int(5)
    );
    assert_eq!(
        engine
            .execute_string("t", "x * 2")
            .await
            .expect("script reads global"),
        ScriptValue::Int(10)
    );
}

#[tokio::test]
async fn modules_become_global_objects() {
    let engine = engine().await;
    let mut map = HashMap::new();
    map.insert("a".to_string(), ScriptValue::Int(1));
    engine
        .register_module("m", ScriptValue::Map(map))
        .expect("register module");
    assert_eq!(
        engine
            .execute_string("t", "m.a + 1")
            .await
            .expect("module object"),
        ScriptValue::Int(2)
    );
}

#[tokio::test]
async fn script_functions_call_with_bridged_args() {
    let engine = engine().await;
    engine
        .load_string("s", "function double(a) { return a * 2 }")
        .await
        .expect("load");
    engine.execute().await.expect("execute");
    assert_eq!(
        engine
            .call_function("double", &[ScriptValue::Int(21)])
            .await
            .expect("call"),
        ScriptValue::Int(42)
    );
}

#[tokio::test]
async fn host_functions_are_rejected() {
    let engine = engine().await;
    let host: rushwind_script::HostFunction = Arc::new(
        |_args: &[ScriptValue]| -> BoxFuture<'static, Result<ScriptValue, ScriptError>> {
            Box::pin(async { Ok(ScriptValue::Null) })
        },
    );
    assert!(matches!(
        engine.register_function("h", host),
        Err(ScriptError::Failed(msg)) if msg.contains("no-unsafe")
    ));
}

#[tokio::test]
async fn broken_syntax_fails() {
    let engine = engine().await;
    assert!(matches!(
        engine.execute_string("t", "var ").await,
        Err(ScriptError::Failed(msg)) if msg.contains("javascript engine")
    ));
}

#[tokio::test]
async fn thrown_errors_fail() {
    let engine = engine().await;
    assert!(matches!(
        engine.execute_string("t", "throw new Error('boom')").await,
        Err(ScriptError::Failed(msg)) if msg.contains("javascript engine")
    ));
}

#[tokio::test]
async fn sources_drive_loads() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("prog", "6 * 7");
    engine.set_source(Some(source));
    assert_eq!(
        engine
            .execute_from_key("prog")
            .await
            .expect("source-driven eval"),
        ScriptValue::Int(42)
    );
}

#[tokio::test]
async fn a_wall_clock_budget_is_reported_post_run() {
    let engine = engine().await;
    {
        let quota: Arc<dyn QuotaController> = engine
            .clone()
            .as_quota_controller()
            .expect("quota capability");
        quota.set_quota(Quota {
            timeout: Some(std::time::Duration::from_nanos(1)),
            max_instructions: None,
        });
    }
    assert_eq!(
        engine
            .execute_string("t", "let t = 0; for (let i = 0; i < 100; i++) t += i")
            .await
            .unwrap_err(),
        ScriptError::QuotaExceeded
    );
}

#[tokio::test]
async fn nothing_loaded_fails() {
    let engine = engine().await;
    assert!(matches!(
        engine.execute().await,
        Err(ScriptError::Failed(msg)) if msg.contains("no program loaded")
    ));
}

#[tokio::test]
async fn a_watch_signal_reloads_the_program() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("prog", "1");
    engine.set_source(Some(source.clone()));
    engine.load("prog").await.expect("initial load");
    assert_eq!(
        engine.execute().await.expect("initial run"),
        ScriptValue::Array(vec![ScriptValue::Int(1)])
    );
    engine.start_watch("prog").await.expect("start watch");
    source.set("prog", "2");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    // The watch task reloaded the program; the loaded set now
    // holds both, and execution answers with both results.
    assert_eq!(
        engine.execute().await.expect("reloaded run"),
        ScriptValue::Array(vec![ScriptValue::Int(1), ScriptValue::Int(2)])
    );
    engine.stop_watch("prog").expect("stop watch");
}

#[tokio::test]
async fn uninitialized_and_lifecycle_guards_hold() {
    let engine = factory().expect("engine");
    assert!(!engine.is_initialized());
    assert!(engine.execute().await.is_err());
    assert!(engine.register_global("x", ScriptValue::Int(1)).is_err());
    engine.init().await.expect("init");
    engine.init().await.expect_err("second init refused");
    engine.close().expect("close");
    assert!(!engine.is_initialized());
    assert!(engine.execute().await.is_err());
}

#[tokio::test]
async fn probes_offer_the_full_capability_set() {
    let engine: Arc<dyn ScriptEngine> = factory().expect("engine");
    assert!(engine.clone().as_loader().is_some());
    assert!(engine.clone().as_executor().is_some());
    assert!(engine.clone().as_global_accessor().is_some());
    assert!(engine.clone().as_function_registrar().is_some());
    assert!(engine.clone().as_module_registrar().is_some());
    assert!(engine.clone().as_watcher().is_some());
    assert!(engine.clone().as_runtime_hook_registrar().is_some());
    assert!(engine.clone().as_sync_executor().is_some());
    assert!(engine.as_quota_controller().is_some());
}
