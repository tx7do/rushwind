use crate::*;
use rushwind_script::MemSource;
use std::sync::Arc;

async fn engine() -> SharedEngine {
    let engine = factory().expect("engine");
    engine.init().await.expect("init");
    engine
}

#[tokio::test]
async fn the_engine_type_is_starlark() {
    let engine = engine().await;
    assert_eq!(engine.engine_type(), NAME);
}

#[tokio::test]
async fn init_and_close_lifecycle() {
    let engine = factory().expect("engine");
    assert!(!engine.is_initialized());
    assert!(engine.execute().await.is_err());
    engine.init().await.expect("init");
    assert!(engine.is_initialized());
    assert!(engine.init().await.is_err());
    engine.close().expect("close");
    assert!(!engine.is_initialized());
    assert!(engine.close().is_err());
}

#[tokio::test]
async fn operations_before_init_fail() {
    let engine = factory().expect("engine");
    assert!(engine.load("k").await.is_err());
    assert!(engine.load_string("k", "x = 1\n").await.is_err());
    assert!(engine.execute().await.is_err());
    assert!(engine.execute_from_key("k").await.is_err());
    assert!(engine.execute_string("k", "x = 1\n").await.is_err());
    assert!(engine.register_global("x", ScriptValue::Int(1)).is_err());
    assert!(engine.get_global("x").is_err());
    assert!(engine
        .register_module("m", ScriptValue::Map(HashMap::new()))
        .is_err());
    assert!(engine.call_function("f", &[]).await.is_err());
    assert!(engine.start_watch("k").await.is_err());
}

#[tokio::test]
async fn sources_bind_and_unbind() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    engine.set_source(Some(source.clone()));
    assert!(engine.get_source().is_some());
    engine.set_source(None);
    assert!(engine.get_source().is_none());
}

#[tokio::test]
async fn load_without_a_bound_source_fails() {
    let engine = engine().await;
    assert!(engine.load("script.star").await.is_err());
}

#[tokio::test]
async fn execute_from_key_without_a_bound_source_fails() {
    let engine = engine().await;
    assert!(engine.execute_from_key("script.star").await.is_err());
}

#[tokio::test]
async fn arithmetic_assignments_reach_the_frozen_environment() {
    let engine = engine().await;
    engine
        .execute_string("test", "x = 1 + 2\n")
        .await
        .expect("exec");
    assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(3));
}

#[tokio::test]
async fn string_concatenation_assignments_read_back() {
    let engine = engine().await;
    engine
        .execute_string("test", "s = \"hello\" + \" \" + \"world\"\n")
        .await
        .expect("exec");
    assert_eq!(
        engine.get_global("s").expect("s"),
        ScriptValue::String("hello world".to_string())
    );
}

#[tokio::test]
async fn boolean_assignments_read_back() {
    let engine = engine().await;
    engine
        .execute_string("test", "b = True and False\n")
        .await
        .expect("exec");
    assert_eq!(engine.get_global("b").expect("b"), ScriptValue::Bool(false));
}

#[tokio::test]
async fn script_functions_call_with_bridged_args() {
    let engine = engine().await;
    engine
        .execute_string("test", "def add(a, b):\n    return a + b\n")
        .await
        .expect("exec");
    assert_eq!(
        engine
            .call_function("add", &[ScriptValue::Int(3), ScriptValue::Int(4)])
            .await
            .expect("call"),
        ScriptValue::Int(7)
    );
}

#[tokio::test]
async fn script_functions_of_two_args_multiply() {
    let engine = engine().await;
    engine
        .execute_string("test", "def multiply(a, b):\n    return a * b\n")
        .await
        .expect("exec");
    assert_eq!(
        engine
            .call_function("multiply", &[ScriptValue::Int(6), ScriptValue::Int(7)])
            .await
            .expect("call"),
        ScriptValue::Int(42)
    );
}

#[tokio::test]
async fn broken_syntax_fails() {
    let engine = engine().await;
    assert!(matches!(
        engine.execute_string("test", "def broken(\n").await,
        Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
    ));
}

#[tokio::test]
async fn nothing_loaded_fails() {
    let engine = engine().await;
    assert!(matches!(
        engine.execute().await,
        Err(ScriptError::Failed(msg)) if msg.contains("no script loaded")
    ));
}

#[tokio::test]
async fn queued_scripts_share_globals_in_one_run() {
    let engine = engine().await;
    engine.load_string("a", "a = 10\n").await.expect("load a");
    engine
        .load_string("b", "b = a + 20\n")
        .await
        .expect("load b");
    engine.execute().await.expect("execute");
    assert_eq!(engine.get_global("b").expect("b"), ScriptValue::Int(30));
}

#[tokio::test]
async fn runtime_errors_fail() {
    let engine = engine().await;
    assert!(matches!(
        engine
            .execute_string("test", "x = [1, 2, 3]\ny = x[10]\n")
            .await,
        Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
    ));
}

#[tokio::test]
async fn multi_statement_scripts_assign() {
    let engine = engine().await;
    engine
        .execute_string("test", "x = 10\ny = 20\nz = x + y\n")
        .await
        .expect("exec");
    assert_eq!(engine.get_global("z").expect("z"), ScriptValue::Int(30));
}

#[tokio::test]
async fn host_integer_globals_feed_scripts() {
    let engine = engine().await;
    engine
        .register_global("x", ScriptValue::Int(42))
        .expect("register");
    engine
        .execute_string("test", "y = x + 8\n")
        .await
        .expect("exec");
    assert_eq!(engine.get_global("y").expect("y"), ScriptValue::Int(50));
}

#[tokio::test]
async fn host_string_globals_feed_scripts() {
    let engine = engine().await;
    engine
        .register_global("name", ScriptValue::String("Alice".to_string()))
        .expect("register");
    engine
        .execute_string("test", "greeting = \"Hello, \" + name\n")
        .await
        .expect("exec");
    assert_eq!(
        engine.get_global("greeting").expect("greeting"),
        ScriptValue::String("Hello, Alice".to_string())
    );
}

#[tokio::test]
async fn host_boolean_globals_feed_scripts() {
    let engine = engine().await;
    engine
        .register_global("flag", ScriptValue::Bool(true))
        .expect("register");
    engine
        .execute_string("test", "result = flag and True\n")
        .await
        .expect("exec");
    assert_eq!(
        engine.get_global("result").expect("result"),
        ScriptValue::Bool(true)
    );
}

#[tokio::test]
async fn host_global_registration_overwrites() {
    let engine = engine().await;
    engine
        .register_global("x", ScriptValue::Int(10))
        .expect("register");
    engine
        .register_global("x", ScriptValue::Int(99))
        .expect("re-register");
    assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(99));
}

#[tokio::test]
async fn host_floats_round_trip_through_scripts() {
    let engine = engine().await;
    engine
        .register_global("pi", ScriptValue::Float(2.5))
        .expect("register");
    engine
        .execute_string("test", "twice = pi * 2.0\n")
        .await
        .expect("exec");
    assert_eq!(
        engine.get_global("twice").expect("twice"),
        ScriptValue::Float(5.0)
    );
}

#[tokio::test]
async fn host_globals_read_back_directly() {
    let engine = engine().await;
    engine
        .register_global("x", ScriptValue::Int(42))
        .expect("register");
    assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(42));
}

#[tokio::test]
async fn missing_globals_fail() {
    let engine = engine().await;
    assert!(matches!(
        engine.get_global("nonexistent"),
        Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
    ));
}

#[tokio::test]
async fn script_defined_globals_read_back() {
    let engine = engine().await;
    engine
        .execute_string("test", "counter = 100\nname = \"test\"\n")
        .await
        .expect("exec");
    assert_eq!(
        engine.get_global("counter").expect("counter"),
        ScriptValue::Int(100)
    );
    assert_eq!(
        engine.get_global("name").expect("name"),
        ScriptValue::String("test".to_string())
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
        Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
    ));
}

#[tokio::test]
async fn call_function_on_missing_names_fails() {
    let engine = engine().await;
    assert!(matches!(
        engine.call_function("nonexistent", &[]).await,
        Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
    ));
}

#[tokio::test]
async fn module_registration_flattens_entries() {
    let engine = engine().await;
    let mut map = HashMap::new();
    map.insert("timeout".to_string(), ScriptValue::Int(30));
    map.insert("name".to_string(), ScriptValue::String("myapp".to_string()));
    engine
        .register_module("config", ScriptValue::Map(map))
        .expect("register module");
    assert_eq!(
        engine.get_global("config_timeout").expect("timeout"),
        ScriptValue::Int(30)
    );
    assert_eq!(
        engine.get_global("config_name").expect("name"),
        ScriptValue::String("myapp".to_string())
    );
}

#[tokio::test]
async fn module_registration_rejects_non_maps() {
    let engine = engine().await;
    assert!(matches!(
        engine.register_module("bad", ScriptValue::Int(1)),
        Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
    ));
}

#[tokio::test]
async fn source_driven_loads_execute() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("script1.star", "x = 1 + 2\n");
    engine.set_source(Some(source));
    engine.load("script1.star").await.expect("load");
    engine.execute().await.expect("execute");
    assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(3));
}

#[tokio::test]
async fn load_multi_runs_in_order() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("a.star", "a = 10\n");
    source.set("b.star", "b = a + 20\n");
    engine.set_source(Some(source));
    engine
        .load_multi(&["a.star".to_string(), "b.star".to_string()])
        .await
        .expect("load multi");
    engine.execute().await.expect("execute");
    assert_eq!(engine.get_global("b").expect("b"), ScriptValue::Int(30));
}

#[tokio::test]
async fn load_multi_aborts_on_missing_keys() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("a.star", "x = 1\n");
    engine.set_source(Some(source));
    assert!(engine
        .load_multi(&["a.star".to_string(), "missing.star".to_string()])
        .await
        .is_err());
}

#[tokio::test]
async fn execute_from_key_runs_the_fetched_script() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("expr.star", "x = 5 * 6\n");
    engine.set_source(Some(source));
    engine.execute_from_key("expr.star").await.expect("exec");
    assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(30));
}

#[tokio::test]
async fn execute_from_keys_answers_null_per_key() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("a.star", "a = 10\n");
    source.set("b.star", "b = 20\n");
    engine.set_source(Some(source));
    let results = engine
        .execute_from_keys(&["a.star".to_string(), "b.star".to_string()])
        .await
        .expect("execute from keys");
    assert_eq!(results, vec![ScriptValue::Null, ScriptValue::Null]);
}

#[tokio::test]
async fn list_iteration_language_features() {
    let engine = engine().await;
    engine
            .execute_string(
                "test",
                "def calc_total():\n    items = [1, 2, 3, 4, 5]\n    total = 0\n    for i in items:\n        total = total + i\n    return total\n\ntotal = calc_total()\n",
            )
            .await
            .expect("exec");
    assert_eq!(
        engine.get_global("total").expect("total"),
        ScriptValue::Int(15)
    );
}

#[tokio::test]
async fn dict_indexing_language_features() {
    let engine = engine().await;
    engine
        .execute_string(
            "test",
            "d = {\"a\": 1, \"b\": 2}\nresult = d[\"a\"] + d[\"b\"]\n",
        )
        .await
        .expect("exec");
    assert_eq!(
        engine.get_global("result").expect("result"),
        ScriptValue::Int(3)
    );
}

#[tokio::test]
async fn if_else_language_features() {
    let engine = engine().await;
    engine
            .execute_string(
                "test",
                "def check(x):\n    if x > 5:\n        return \"big\"\n    else:\n        return \"small\"\n\nresult = check(10)\n",
            )
            .await
            .expect("exec");
    assert_eq!(
        engine.get_global("result").expect("result"),
        ScriptValue::String("big".to_string())
    );
}

#[tokio::test]
async fn for_loop_language_features() {
    let engine = engine().await;
    engine
            .execute_string(
                "test",
                "def calc_sum():\n    total = 0\n    for i in range(10):\n        total = total + i\n    return total\n\ntotal = calc_sum()\n",
            )
            .await
            .expect("exec");
    assert_eq!(
        engine.get_global("total").expect("total"),
        ScriptValue::Int(45)
    );
}

#[tokio::test]
async fn comprehensions_read_back_as_arrays() {
    let engine = engine().await;
    engine
        .execute_string("test", "squares = [x * x for x in range(5)]\n")
        .await
        .expect("exec");
    assert_eq!(
        engine.get_global("squares").expect("squares"),
        ScriptValue::Array(vec![
            ScriptValue::Int(0),
            ScriptValue::Int(1),
            ScriptValue::Int(4),
            ScriptValue::Int(9),
            ScriptValue::Int(16)
        ])
    );
}

#[tokio::test]
async fn nested_functions_close_over_their_environment() {
    let engine = engine().await;
    engine
            .execute_string(
                "test",
                "def make_multiplier(n):\n    def multiply(x):\n        return x * n\n    return multiply\n\ntriple = make_multiplier(3)\nresult = triple(14)\n",
            )
            .await
            .expect("exec");
    assert_eq!(
        engine.get_global("result").expect("result"),
        ScriptValue::Int(42)
    );
}

#[tokio::test]
async fn last_error_records_and_clears() {
    let engine = engine().await;
    assert!(engine.last_error().is_none());
    let _ = engine.execute_string("bad.star", "x = 1 +\n").await;
    assert!(engine.last_error().is_some());
    engine.clear_error();
    assert!(engine.last_error().is_none());
}

#[tokio::test]
async fn watch_requeues_the_changed_script() {
    let engine = engine().await;
    let source = Arc::new(MemSource::new());
    source.set("watch_script.star", "x = 1\n");
    engine.set_source(Some(source.clone()));
    engine.load("watch_script.star").await.expect("load");
    engine
        .start_watch("watch_script.star")
        .await
        .expect("watch");
    source.set("watch_script.star", "x = 2\n");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    // The watch task requeued the changed script; execution now
    // runs both copies and the later assignment wins.
    engine.execute().await.expect("execute");
    assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(2));
    engine.stop_watch("watch_script.star").expect("stop watch");
}

#[tokio::test]
async fn watch_without_a_bound_source_fails() {
    let engine = engine().await;
    assert!(engine.start_watch("script.star").await.is_err());
}

#[tokio::test]
async fn watch_on_non_watcher_sources_fails() {
    struct BareSource;
    impl rushwind_script::ScriptSource for BareSource {
        fn load<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
            Box::pin(async { Err(ScriptError::Failed("bare source".to_string())) })
        }
    }
    let engine = engine().await;
    engine.set_source(Some(Arc::new(BareSource)));
    assert!(engine.start_watch("script.star").await.is_err());
}

#[tokio::test]
async fn stop_watch_without_registration_is_a_no_op() {
    let engine = engine().await;
    engine.stop_watch("nonexistent").expect("stop watch");
}

#[tokio::test]
async fn concurrent_executions_serialize() {
    let engine = engine().await;
    engine
        .load_string("expr", "x = 1 + 2\n")
        .await
        .expect("load");
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..5 {
        let engine = engine.clone();
        tasks.spawn(async move { engine.execute().await });
    }
    let mut outcomes = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        outcomes.push(joined.expect("joined").expect("execution"));
    }
    assert_eq!(outcomes.len(), 5);
    for outcome in outcomes {
        assert_eq!(outcome, ScriptValue::Null);
    }
}

#[tokio::test]
async fn probes_offer_the_full_capability_set() {
    let engine: Arc<dyn ScriptEngine> = factory().expect("engine");
    assert!(engine.clone().as_loader().is_some());
    assert!(engine.clone().as_executor().is_some());
    assert!(engine.clone().as_global_accessor().is_some());
    assert!(engine.clone().as_function_registrar().is_some());
    assert!(engine.clone().as_module_registrar().is_some());
    assert!(engine.as_watcher().is_some());
}

#[tokio::test]
async fn probes_refuse_the_absent_capabilities() {
    let engine: Arc<dyn ScriptEngine> = factory().expect("engine");
    assert!(engine.clone().as_sandbox_configurator().is_none());
    assert!(engine.clone().as_runtime_hook_registrar().is_none());
    assert!(engine.clone().as_sync_executor().is_none());
    assert!(engine.as_quota_controller().is_none());
}
