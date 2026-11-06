use crate::*;
use rushwind_script::MemSource;
use std::sync::Arc;

async fn engine() -> SharedEngine {
    let engine = factory().expect("engine");
    engine.init().await.expect("init");
    engine
}

#[tokio::test]
async fn execution_discards_results_but_call_function_bridges() {
    let engine = engine().await;
    engine
        .load_string("s", "function add(a, b) return a + b end")
        .await
        .expect("load script");
    assert_eq!(engine.execute().await.expect("execute"), ScriptValue::Null);
    let args = [ScriptValue::Int(1), ScriptValue::Int(2)];
    assert_eq!(
        engine
            .call_function("add", &args)
            .await
            .expect("call function"),
        ScriptValue::Int(3)
    );
}

#[tokio::test]
async fn a_broken_chunk_fails_to_compile() {
    let engine = engine().await;
    assert!(matches!(
        engine.load_string("s", "function (").await,
        Err(ScriptError::Failed(msg)) if msg.contains("compile failed")
    ));
}

#[tokio::test]
async fn a_runtime_error_fails_the_run() {
    let engine = engine().await;
    engine
        .load_string("s", "error('boom')")
        .await
        .expect("load");
    assert!(matches!(
        engine.execute().await,
        Err(ScriptError::Failed(msg)) if msg.contains("lua engine")
    ));
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
    engine
        .load_string("s", "y = x * 2")
        .await
        .expect("load script");
    engine.execute().await.expect("execute");
    assert_eq!(
        engine.get_global("y").expect("script global read back"),
        ScriptValue::Int(10)
    );
}

#[tokio::test]
async fn host_functions_reach_scripts_and_callers() {
    let engine = engine().await;
    let host: HostFunction = Arc::new(
        |args: &[ScriptValue]| -> BoxFuture<'static, Result<ScriptValue, ScriptError>> {
            let v = args.first().cloned().unwrap_or(ScriptValue::Null);
            Box::pin(async move { Ok(v) })
        },
    );
    engine
        .register_function("echo", host)
        .expect("register function");
    engine
        .load_string("s", "function f() return echo(42) end")
        .await
        .expect("load script");
    engine.execute().await.expect("execute");
    assert_eq!(
        engine.call_function("f", &[]).await.expect("call"),
        ScriptValue::Int(42)
    );
}

#[tokio::test]
async fn modules_are_rejected() {
    let engine = engine().await;
    assert!(matches!(
        engine.register_module("m", ScriptValue::Map(HashMap::new())),
        Err(ScriptError::Failed(msg)) if msg.contains("native module")
    ));
}

#[tokio::test]
async fn the_sandbox_allow_list_shrinks_the_libraries() {
    let engine = factory().expect("engine");
    {
        let sandbox: Arc<dyn SandboxConfigurator> = engine
            .clone()
            .as_sandbox_configurator()
            .expect("sandbox capability");
        sandbox.set_open_libs(&["base", "string", "math"]);
    }
    engine.init().await.expect("init");
    engine
        .load_string("s", "function hasio() return io ~= nil end")
        .await
        .expect("load script");
    engine.execute().await.expect("execute");
    assert_eq!(
        engine
            .call_function("hasio", &[])
            .await
            .expect("io is absent"),
        ScriptValue::Bool(false)
    );
}

#[tokio::test]
async fn the_default_library_set_is_the_minimal_safe_set() {
    let engine = engine().await;
    engine
        .load_string("s", "function hasio() return io ~= nil end")
        .await
        .expect("load script");
    engine.execute().await.expect("execute");
    assert_eq!(
        engine
            .call_function("hasio", &[])
            .await
            .expect("io is absent"),
        ScriptValue::Bool(false),
        "the default sandbox must not open io"
    );
}

#[tokio::test]
async fn an_instruction_budget_interrupts_a_runaway_script() {
    let engine = engine().await;
    {
        let quota: Arc<dyn QuotaController> = engine
            .clone()
            .as_quota_controller()
            .expect("quota capability");
        quota.set_quota(Quota {
            timeout: None,
            max_instructions: Some(64),
        });
    }
    engine
        .load_string("s", "while true do end")
        .await
        .expect("load script");
    let sync: Arc<dyn SyncExecutor> = engine.clone().as_sync_executor().expect("sync capability");
    assert_eq!(
        sync.execute_sync().await.unwrap_err(),
        ScriptError::QuotaExceeded
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
    engine
        .load_string("s", "local t = 0 for i = 1, 100 do t = t + i end")
        .await
        .expect("load script");
    let sync: Arc<dyn SyncExecutor> = engine.clone().as_sync_executor().expect("sync capability");
    assert_eq!(
        sync.execute_sync().await.unwrap_err(),
        ScriptError::QuotaExceeded
    );
}

#[tokio::test]
async fn a_watch_signal_reloads_the_chunk() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("chunk", "function f() return 1 end");
    engine.set_source(Some(source.clone()));
    engine.load("chunk").await.expect("initial load");
    engine.execute().await.expect("initial execute");
    assert_eq!(
        engine.call_function("f", &[]).await.expect("v1"),
        ScriptValue::Int(1)
    );
    engine.start_watch("chunk").await.expect("start watch");
    source.set("chunk", "function f() return 2 end");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    // The watch task reloaded the chunk into the engine; running
    // the loaded set defines the reloaded function over the old
    // one — the reload-then-execute flow.
    engine.execute().await.expect("post-reload execute");
    assert_eq!(
        engine.call_function("f", &[]).await.expect("reloaded"),
        ScriptValue::Int(2)
    );
    engine.stop_watch("chunk").expect("stop watch");
}

#[tokio::test]
async fn sources_drive_loads() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("chunk", "x = 40 + 2");
    engine.set_source(Some(source));
    engine.load("chunk").await.expect("load");
    engine.execute().await.expect("execute");
    assert_eq!(
        engine.get_global("x").expect("source-driven global"),
        ScriptValue::Int(42)
    );
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
    assert!(engine.clone().as_sandbox_configurator().is_some());
    assert!(engine.clone().as_runtime_hook_registrar().is_some());
    assert!(engine.clone().as_sync_executor().is_some());
    assert!(engine.as_quota_controller().is_some());
}

#[tokio::test]
async fn the_default_sandbox_opens_no_io_os_or_package() {
    let engine = engine().await;
    engine
        .load_string(
            "probe",
            "function probe() return io == nil and os == nil and package == nil end",
        )
        .await
        .expect("load probe");
    engine.execute().await.expect("execute defines the global");
    let verdict = engine.call_function("probe", &[]).await.expect("probe");
    assert_eq!(
        verdict,
        ScriptValue::Bool(true),
        "default sandbox must not open io/os/package"
    );
}

#[tokio::test]
async fn an_explicit_open_libs_call_opts_into_io() {
    let engine = factory().expect("engine");
    let sandbox = Arc::clone(&engine)
        .as_sandbox_configurator()
        .expect("sandbox capability");
    sandbox.set_open_libs(&["string", "io"]);
    engine.init().await.expect("init");
    engine
        .load_string("probe", "function probe() return io ~= nil end")
        .await
        .expect("load probe");
    engine.execute().await.expect("execute defines the global");
    assert_eq!(
        engine.call_function("probe", &[]).await.expect("probe"),
        ScriptValue::Bool(true),
        "an explicit io opt-in must open io"
    );
}
