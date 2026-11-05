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
async fn strings_evaluate() {
    let engine = engine().await;
    assert_eq!(
        engine
            .execute_string("t", r#""hello" + " " + "world""#)
            .await
            .expect("eval"),
        ScriptValue::String("hello world".to_string())
    );
}

#[tokio::test]
async fn a_broken_expression_fails_to_compile() {
    let engine = engine().await;
    assert!(matches!(
        engine.execute_string("t", "a b c").await,
        Err(ScriptError::Failed(msg)) if msg.contains("compile failed")
    ));
    // The upstream parser panics on some malformed inputs —
    // including the empty expression — and the compile wrapper
    // converts the panic into the same failure.
    assert!(matches!(
        engine.execute_string("t", "").await,
        Err(ScriptError::Failed(msg)) if msg.contains("compile failed")
    ));
    assert!(engine.last_error().is_some());
}

#[tokio::test]
async fn an_unknown_variable_fails_the_evaluation() {
    let engine = engine().await;
    assert!(matches!(
        engine.execute_string("t", "missing_var").await,
        Err(ScriptError::Failed(msg)) if msg.contains("evaluation failed")
    ));
}

#[tokio::test]
async fn globals_reach_the_expressions() {
    let engine = engine().await;
    engine
        .register_global("x", ScriptValue::Int(10))
        .expect("register global");
    assert_eq!(
        engine.get_global("x").expect("get global"),
        ScriptValue::Int(10)
    );
    assert_eq!(
        engine.execute_string("t", "x * 2").await.expect("eval"),
        ScriptValue::Int(20)
    );
}

#[tokio::test]
async fn an_unknown_global_errors() {
    let engine = engine().await;
    assert!(matches!(
        engine.get_global("missing"),
        Err(ScriptError::Failed(msg)) if msg.contains("global not found")
    ));
}

#[tokio::test]
async fn modules_flatten_into_prefixed_globals() {
    let engine = engine().await;
    let mut map = HashMap::new();
    map.insert("a".to_string(), ScriptValue::Int(1));
    map.insert("b".to_string(), ScriptValue::Int(2));
    engine
        .register_module("m", ScriptValue::Map(map))
        .expect("register module");
    assert_eq!(
        engine.execute_string("t", "m_a + m_b").await.expect("eval"),
        ScriptValue::Int(3)
    );
}

#[tokio::test]
async fn a_non_map_module_is_rejected() {
    let engine = engine().await;
    assert!(matches!(
        engine.register_module("m", ScriptValue::Int(1)),
        Err(ScriptError::Failed(msg)) if msg.contains("expects a map")
    ));
}

#[tokio::test]
async fn host_functions_call_through_the_engine() {
    let engine = engine().await;
    let host: HostFunction = Arc::new(
        |args: &[ScriptValue]| -> BoxFuture<'static, Result<ScriptValue, ScriptError>> {
            let sum = args
                .iter()
                .map(|v| match v {
                    ScriptValue::Int(i) => *i,
                    _ => 0,
                })
                .sum::<i64>();
            Box::pin(async move { Ok(ScriptValue::Int(sum)) })
        },
    );
    engine
        .register_function("sum", host)
        .expect("register function");
    let args = [ScriptValue::Int(1), ScriptValue::Int(2)];
    assert_eq!(
        engine
            .call_function("sum", &args)
            .await
            .expect("call function"),
        ScriptValue::Int(3)
    );
}

#[tokio::test]
async fn an_unknown_function_errors() {
    let engine = engine().await;
    assert!(matches!(
        engine.call_function("missing", &[]).await,
        Err(ScriptError::Failed(msg)) if msg.contains("function not found")
    ));
}

#[tokio::test]
async fn sources_drive_loads_and_evaluations() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("expr", "6 * 7");
    engine.set_source(Some(source));
    assert_eq!(
        engine.execute_from_key("expr").await.expect("eval"),
        ScriptValue::Int(42)
    );
}

#[tokio::test]
async fn without_a_source_loads_fail() {
    let engine = engine().await;
    assert!(matches!(
        engine.load("k").await,
        Err(ScriptError::Failed(msg)) if msg.contains("no source bound")
    ));
}

#[tokio::test]
async fn nothing_loaded_fails() {
    let engine = engine().await;
    assert!(matches!(
        engine.execute().await,
        Err(ScriptError::Failed(msg)) if msg.contains("no expression loaded")
    ));
}

#[tokio::test]
async fn a_watch_signal_reloads_the_expression() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("expr", "1 + 1");
    engine.set_source(Some(source.clone()));
    assert_eq!(
        engine.execute_from_key("expr").await.expect("initial eval"),
        ScriptValue::Int(2)
    );
    engine.start_watch("expr").await.expect("start watch");
    source.set("expr", "2 + 3");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        engine
            .execute_from_key("expr")
            .await
            .expect("reloaded eval"),
        ScriptValue::Int(5)
    );
    engine.stop_watch("expr").expect("stop watch");
}

#[tokio::test]
async fn uninitialized_engines_refuse_everything() {
    let engine = factory().expect("engine");
    assert!(!engine.is_initialized());
    assert!(engine.execute().await.is_err());
    assert!(engine.register_global("x", ScriptValue::Int(1)).is_err());
    engine.init().await.expect("init");
    engine.init().await.expect_err("second init refused");
    engine.close().expect("close");
    assert!(!engine.is_initialized());
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
    assert!(engine.clone().as_sandbox_configurator().is_none());
    assert!(engine.clone().as_runtime_hook_registrar().is_none());
    assert!(engine.clone().as_sync_executor().is_none());
    assert!(engine.as_quota_controller().is_none());
}
