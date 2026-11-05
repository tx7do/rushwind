//! The JavaScript engine for the Rust script contract, built over
//! [`boa_engine`].
//!
//! boa's [`Context`] owns a garbage-collected heap behind `Rc`
//! handles and is therefore `!Send`, while the contract shares
//! engines behind `Arc` (which demands `Send + Sync`). The engine is
//! consequently an **actor**: a dedicated worker thread owns the
//! [`Context`], and every runtime operation is a command sent down a
//! channel with the answer coming back on a oneshot. Execution is
//! serialized — one
//! script at a time — while the engine object itself stays freely
//! shareable.
//!
//! Semantics:
//!
//! - `load` keeps the program source; `execute` runs every loaded
//!   program in order and answers with the **array** of results
//!   (unlike Lua/CEL, which answer with
//!   `Null`/last respectively).
//! - `register_global`/`get_global` bridge values both ways; a value
//!   map registered as a module becomes a global object.
//! - `call_function` invokes a script-defined function with bridged
//!   arguments and results.
//! - Runtime hooks registered before init replay during init; on an
//!   initialized engine they run immediately.
//! - `start_watch` reloads the key on source change ticks, the
//!   weak-task + abort-handle shape.
//!
//! Divergences and limits:
//!
//! - `register_function` **always fails**: registering a capturing
//!   native closure in boa requires `NativeFunction::from_closure`,
//!   which is `unsafe` (a lifetime transmute), and the workspace
//!   forbids unsafe code outright. `call_function` — invoking
//!   script-defined functions — works.
//! - There is no wall-clock interruption point in boa, so the
//!   quota is checked post-run — an over-budget run completes and is
//!   reported after the fact — and the instruction budget has no
//!   counterpart at all.
//! - Precompiled program handles have no boa counterpart;
//!   programs are stored as source strings and re-parsed per run.
//! - The value bridge rides [`serde_json`] through boa's built-in
//!   JSON converters: the contract's data-only value set maps onto
//!   JSON's, and anything beyond it (functions, symbols, getters)
//!   bridges as `Null`.
//!
//! # Engine matrix
//!
//! | Capability | Status |
//! |:---|:---|
//! | loader / executor / globals / modules / watch / runtime hooks / sync+quota (post-hoc) / lifecycle | implemented |
//! | host functions (`register_function`) | rejected — boa needs unsafe for capturing closures |
//! | sandbox | not offered |

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Instant;

use boa_engine::{property::Attribute, Context, JsString, JsValue, Source};
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;

use rushwind_script::{
    register_factory, BoxFuture, GlobalAccessor, Quota, QuotaController, RuntimeHook,
    RuntimeHookRegistrar, ScriptEngine, ScriptError, ScriptExecutor, ScriptLoader, ScriptValue,
    ScriptWatcher, SharedEngine, SharedScriptSource, SyncExecutor,
};

/// The registry name.
pub const NAME: &str = "javascript";

/// One command for the worker thread that owns the [`Context`].
enum Command {
    /// Run every loaded program, answering with the array of results.
    EvalAll {
        codes: Vec<String>,
        reply: oneshot::Sender<Result<Vec<ScriptValue>, ScriptError>>,
    },
    /// Run one program, answering with its result.
    EvalOne {
        code: String,
        reply: oneshot::Sender<Result<ScriptValue, ScriptError>>,
    },
    /// Register a global property.
    SetGlobal {
        name: String,
        value: ScriptValue,
        reply: oneshot::Sender<Result<(), ScriptError>>,
    },
    /// Read a global property.
    GetGlobal {
        name: String,
        reply: oneshot::Sender<Result<ScriptValue, ScriptError>>,
    },
    /// Register a value map as a global object.
    SetModule {
        name: String,
        value: ScriptValue,
        reply: oneshot::Sender<Result<(), ScriptError>>,
    },
    /// Invoke a script-defined function.
    Call {
        name: String,
        args: Vec<ScriptValue>,
        reply: oneshot::Sender<Result<ScriptValue, ScriptError>>,
    },
    /// Shut the worker down.
    Drop,
}

/// The `javascript` engine: [`boa_engine`] behind an actor thread.
pub struct JsEngine {
    sender: Mutex<Option<mpsc::UnboundedSender<Command>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    initialized: Mutex<bool>,
    source: Mutex<Option<SharedScriptSource>>,
    programs: Mutex<Vec<String>>,
    hooks: Mutex<Vec<RuntimeHook>>,
    quota: Mutex<Quota>,
    watchers: Mutex<HashMap<String, AbortHandle>>,
    weak: Mutex<Weak<Self>>,
    last_error: Mutex<Option<ScriptError>>,
}

impl JsEngine {
    /// Builds the engine uninitialized; call [`ScriptEngine::init`]
    /// before use. Prefer [`factory`], which also arms the weak
    /// self-reference the watch tasks need.
    pub fn new() -> Self {
        Self {
            sender: Mutex::new(None),
            worker: Mutex::new(None),
            initialized: Mutex::new(false),
            source: Mutex::new(None),
            programs: Mutex::new(Vec::new()),
            hooks: Mutex::new(Vec::new()),
            quota: Mutex::new(Quota::default()),
            watchers: Mutex::new(HashMap::new()),
            weak: Mutex::new(Weak::new()),
            last_error: Mutex::new(None),
        }
    }

    fn set_last_error(&self, error: ScriptError) {
        *self.last_error.lock().expect("js engine last-error lock") = Some(error);
    }

    fn clear_last_error(&self) {
        *self.last_error.lock().expect("js engine last-error lock") = None;
    }

    fn guard_initialized(&self) -> Result<(), ScriptError> {
        if !*self.initialized.lock().expect("js engine init lock") {
            let err = ScriptError::Failed("javascript engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        Ok(())
    }

    fn stop_all_watchers(&self) {
        let watchers: Vec<AbortHandle> = {
            let mut watchers = self.watchers.lock().expect("js engine watcher lock");
            watchers.drain().map(|(_, handle)| handle).collect()
        };
        for handle in watchers {
            handle.abort();
        }
    }

    fn quota_guard_start(&self) -> (Quota, Instant) {
        (
            *self.quota.lock().expect("js engine quota lock"),
            Instant::now(),
        )
    }

    /// The post-hoc quota check: boa offers no interruption point, so
    /// an over-budget run completes and is reported after the fact.
    fn quota_guard_end(&self, quota: Quota, start: Instant) -> Result<(), ScriptError> {
        if let Some(budget) = quota.timeout {
            if start.elapsed() >= budget {
                let err = ScriptError::QuotaExceeded;
                self.set_last_error(err.clone());
                return Err(err);
            }
        }
        Ok(())
    }

    /// The source-fetch step: the bound source's script
    /// for `key`. [`ScriptLoader::load`] queues the fetched program;
    /// [`ScriptExecutor::execute_from_key`] fetches and runs it alone.
    async fn load_code(&self, key: &str) -> Result<String, ScriptError> {
        self.guard_initialized()?;
        let source = self.source.lock().expect("js engine source lock").clone();
        let Some(source) = source else {
            let err = ScriptError::Failed("javascript engine: no source bound".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        match source.load(key).await {
            Ok(code) => Ok(code),
            Err(err) => {
                self.set_last_error(err.clone());
                Err(err)
            }
        }
    }

    /// Settles a worker reply: a failure is recorded as the engine's
    /// last error before it propagates.
    fn settle<T>(&self, outcome: Result<T, ScriptError>) -> Result<T, ScriptError> {
        match outcome {
            Ok(value) => Ok(value),
            Err(err) => {
                self.set_last_error(err.clone());
                Err(err)
            }
        }
    }

    fn send<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, ScriptError>>) -> Command,
    ) -> Result<oneshot::Receiver<Result<T, ScriptError>>, ScriptError> {
        let sender = self.sender.lock().expect("js engine sender lock").clone();
        let Some(sender) = sender else {
            let err = ScriptError::Failed("javascript engine: worker unavailable".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        let (tx, rx) = oneshot::channel();
        if sender.send(make(tx)).is_err() {
            let err = ScriptError::Failed("javascript engine: worker unavailable".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        Ok(rx)
    }
}

impl Default for JsEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeHookRegistrar for JsEngine {
    /// Hooks registered before init replay during init; on an
    /// initialized engine they run immediately on an inline executor
    /// — hooks must not park on runtime resources.
    fn add_runtime_hook(&self, hook: RuntimeHook) -> Result<(), ScriptError> {
        if !*self.initialized.lock().expect("js engine init lock") {
            self.hooks.lock().expect("js engine hook lock").push(hook);
            return Ok(());
        }
        let _ = futures::executor::block_on(hook());
        Ok(())
    }
}

impl QuotaController for JsEngine {
    fn set_quota(&self, quota: Quota) {
        *self.quota.lock().expect("js engine quota lock") = quota;
    }
}

impl ScriptEngine for JsEngine {
    fn engine_type(&self) -> &'static str {
        NAME
    }

    fn init(&self) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async move {
            let (sender, receiver) = mpsc::unbounded_channel();
            let worker = {
                let mut initialized = self.initialized.lock().expect("js engine init lock");
                if *initialized {
                    self.set_last_error(ScriptError::Failed(
                        "javascript engine: already initialized".to_string(),
                    ));
                    return Err(ScriptError::Failed(
                        "javascript engine: already initialized".to_string(),
                    ));
                }
                *initialized = true;
                let worker = std::thread::spawn(move || worker_loop(receiver));
                *self.sender.lock().expect("js engine sender lock") = Some(sender);
                worker
            };
            *self.worker.lock().expect("js engine worker lock") = Some(worker);
            // Replay hooks registered before init — they run through
            // the actor like every other engine operation.
            let hooks: Vec<RuntimeHook> = self
                .hooks
                .lock()
                .expect("js engine hook lock")
                .drain(..)
                .collect();
            for hook in hooks {
                if hook().await.is_err() {
                    return Err(ScriptError::Failed(
                        "javascript engine: runtime hook failed".to_string(),
                    ));
                }
            }
            self.clear_last_error();
            Ok(())
        })
    }

    fn close(&self) -> Result<(), ScriptError> {
        self.stop_all_watchers();
        let worker = {
            let mut initialized = self.initialized.lock().expect("js engine init lock");
            *initialized = false;
            let sender = self.sender.lock().expect("js engine sender lock").take();
            if let Some(sender) = sender {
                let _ = sender.send(Command::Drop);
            }
            self.worker.lock().expect("js engine worker lock").take()
        };
        if let Some(worker) = worker {
            let _ = worker.join();
        }
        *self.source.lock().expect("js engine source lock") = None;
        self.programs
            .lock()
            .expect("js engine program lock")
            .clear();
        self.hooks.lock().expect("js engine hook lock").clear();
        self.clear_last_error();
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        *self.initialized.lock().expect("js engine init lock")
    }

    fn last_error(&self) -> Option<ScriptError> {
        self.last_error
            .lock()
            .expect("js engine last-error lock")
            .clone()
    }

    fn clear_error(&self) {
        self.clear_last_error();
    }

    fn as_loader(self: Arc<Self>) -> Option<Arc<dyn ScriptLoader>> {
        Some(self)
    }

    fn as_executor(self: Arc<Self>) -> Option<Arc<dyn ScriptExecutor>> {
        Some(self)
    }

    fn as_global_accessor(self: Arc<Self>) -> Option<Arc<dyn GlobalAccessor>> {
        Some(self)
    }

    fn as_function_registrar(
        self: Arc<Self>,
    ) -> Option<Arc<dyn rushwind_script::FunctionRegistrar>> {
        Some(self)
    }

    fn as_module_registrar(self: Arc<Self>) -> Option<Arc<dyn rushwind_script::ModuleRegistrar>> {
        Some(self)
    }

    fn as_watcher(self: Arc<Self>) -> Option<Arc<dyn ScriptWatcher>> {
        Some(self)
    }

    fn as_runtime_hook_registrar(self: Arc<Self>) -> Option<Arc<dyn RuntimeHookRegistrar>> {
        Some(self)
    }

    fn as_sync_executor(self: Arc<Self>) -> Option<Arc<dyn SyncExecutor>> {
        Some(self)
    }

    fn as_quota_controller(self: Arc<Self>) -> Option<Arc<dyn QuotaController>> {
        Some(self)
    }
}

/// The value bridge on the worker side: contract values and boa
/// values meeting over [`serde_json`], whose data model the contract
/// value set mirrors.
mod bridge {
    use super::*;

    /// Converts a contract value into a serde JSON value.
    pub fn to_json(value: &ScriptValue) -> serde_json::Value {
        match value {
            ScriptValue::Null => serde_json::Value::Null,
            ScriptValue::Bool(v) => serde_json::Value::Bool(*v),
            ScriptValue::Int(v) => serde_json::Value::Number((*v).into()),
            ScriptValue::UInt(v) => serde_json::Value::Number((*v).into()),
            ScriptValue::Float(v) => serde_json::Value::Number(
                serde_json::Number::from_f64(*v).unwrap_or(serde_json::Number::from(0)),
            ),
            ScriptValue::String(v) => serde_json::Value::String(v.clone()),
            ScriptValue::Bytes(v) => serde_json::Value::Array(
                v.iter()
                    .map(|b| serde_json::Value::Number((*b).into()))
                    .collect(),
            ),
            ScriptValue::Array(items) => {
                serde_json::Value::Array(items.iter().map(to_json).collect())
            }
            ScriptValue::Map(map) => serde_json::Value::Object(
                map.iter().map(|(k, v)| (k.clone(), to_json(v))).collect(),
            ),
            _ => serde_json::Value::Null,
        }
    }

    /// Converts a serde JSON value back into a contract value.
    pub fn from_json(value: serde_json::Value) -> ScriptValue {
        match value {
            serde_json::Value::Null => ScriptValue::Null,
            serde_json::Value::Bool(v) => ScriptValue::Bool(v),
            serde_json::Value::Number(v) => {
                if let Some(i) = v.as_i64() {
                    ScriptValue::Int(i)
                } else if let Some(u) = v.as_u64() {
                    ScriptValue::UInt(u)
                } else if let Some(f) = v.as_f64() {
                    ScriptValue::Float(f)
                } else {
                    ScriptValue::Null
                }
            }
            serde_json::Value::String(v) => ScriptValue::String(v),
            serde_json::Value::Array(items) => {
                ScriptValue::Array(items.into_iter().map(from_json).collect())
            }
            serde_json::Value::Object(map) => {
                ScriptValue::Map(map.into_iter().map(|(k, v)| (k, from_json(v))).collect())
            }
        }
    }
}

/// The worker loop: owns the [`Context`] and answers commands until
/// dropped.
fn worker_loop(mut receiver: mpsc::UnboundedReceiver<Command>) {
    let mut context = Context::default();
    loop {
        let Some(command) = receiver.blocking_recv() else {
            break;
        };
        match command {
            Command::Drop => break,
            Command::EvalAll { codes, reply } => {
                // The first failing program
                // aborts the run — no partial result array comes back.
                let mut results = Vec::with_capacity(codes.len());
                let mut failure = None;
                for code in &codes {
                    match worker_eval(&mut context, code) {
                        Ok(value) => results.push(value),
                        Err(err) => {
                            failure = Some(err);
                            break;
                        }
                    }
                }
                let _ = reply.send(match failure {
                    Some(err) => Err(err),
                    None => Ok(results),
                });
            }
            Command::EvalOne { code, reply } => {
                let outcome = worker_eval(&mut context, &code);
                let _ = reply.send(outcome);
            }
            Command::SetGlobal { name, value, reply } => {
                // The bridged value
                // becomes a global property.
                let js_value = JsValue::from_json(&bridge::to_json(&value), &mut context)
                    .unwrap_or_else(|_| JsValue::undefined());
                let outcome = context
                    .register_global_property(
                        JsString::from(name.as_str()),
                        js_value,
                        Attribute::all(),
                    )
                    .map_err(|err| ScriptError::Failed(format!("javascript engine: {err}")));
                let _ = reply.send(outcome);
            }
            Command::GetGlobal { name, reply } => {
                let outcome = context
                    .global_object()
                    .get(JsString::from(name.as_str()), &mut context)
                    .map(|value| {
                        value
                            .to_json(&mut context)
                            .ok()
                            .flatten()
                            .map_or(ScriptValue::Null, bridge::from_json)
                    })
                    .map_err(|err| ScriptError::Failed(format!("javascript engine: {err}")));
                let _ = reply.send(outcome);
            }
            Command::SetModule { name, value, reply } => {
                // A value-map module becomes a global
                // object with the entries copied in, anything else a
                // plain global. The bridge's from_json builds that
                // object graph itself, so both branches collapse into
                // the same registration here.
                let js_value = JsValue::from_json(&bridge::to_json(&value), &mut context)
                    .unwrap_or_else(|_| JsValue::undefined());
                let outcome = context
                    .register_global_property(
                        JsString::from(name.as_str()),
                        js_value,
                        Attribute::all(),
                    )
                    .map_err(|err| ScriptError::Failed(format!("javascript engine: {err}")));
                let _ = reply.send(outcome);
            }
            Command::Call { name, args, reply } => {
                let function = context
                    .global_object()
                    .get(JsString::from(name.as_str()), &mut context);
                let Ok(function) = function else {
                    let _ = reply.send(Err(ScriptError::Failed(format!(
                        "javascript engine: not callable: {name}"
                    ))));
                    continue;
                };
                let Some(object) = function.as_object() else {
                    let _ = reply.send(Err(ScriptError::Failed(format!(
                        "javascript engine: not callable: {name}"
                    ))));
                    continue;
                };
                if !object.is_callable() {
                    let _ = reply.send(Err(ScriptError::Failed(format!(
                        "javascript engine: not callable: {name}"
                    ))));
                    continue;
                }
                let js_args: Vec<JsValue> = args
                    .iter()
                    .map(|arg| {
                        JsValue::from_json(&bridge::to_json(arg), &mut context)
                            .unwrap_or_else(|_| JsValue::undefined())
                    })
                    .collect();
                let outcome = object
                    .call(&JsValue::undefined(), js_args.as_slice(), &mut context)
                    .and_then(|value| value.to_json(&mut context))
                    .map(|json| json.map_or(ScriptValue::Null, bridge::from_json))
                    .map_err(|err| ScriptError::Failed(format!("javascript engine: {err}")));
                let _ = reply.send(outcome);
            }
        }
    }
}

/// One worker-side evaluation: parse and run `code`, bridging the
/// result — or the failure — back.
fn worker_eval(context: &mut Context, code: &str) -> Result<ScriptValue, ScriptError> {
    context
        .eval(Source::from_bytes(code))
        .and_then(|value| value.to_json(context))
        .map(|json| json.map_or(ScriptValue::Null, bridge::from_json))
        .map_err(|err| ScriptError::Failed(format!("javascript engine: {err}")))
}

impl ScriptLoader for JsEngine {
    fn set_source(&self, source: Option<SharedScriptSource>) {
        *self.source.lock().expect("js engine source lock") = source;
    }

    fn get_source(&self) -> Option<SharedScriptSource> {
        self.source.lock().expect("js engine source lock").clone()
    }

    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            // Fetch, then queue — LoadString's
            // append step.
            let code = self.load_code(key).await?;
            self.programs
                .lock()
                .expect("js engine program lock")
                .push(code);
            self.clear_last_error();
            Ok(())
        })
    }

    fn load_multi<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            for key in keys {
                self.load(key).await?;
            }
            Ok(())
        })
    }

    fn load_string<'a>(
        &'a self,
        _name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<(), ScriptError>> {
        let outcome = (|| {
            self.guard_initialized()?;
            self.programs
                .lock()
                .expect("js engine program lock")
                .push(code.to_string());
            self.clear_last_error();
            Ok(())
        })();
        Box::pin(async move { outcome })
    }
}

impl ScriptExecutor for JsEngine {
    /// Every loaded program runs and the array of
    /// results comes back — bridged as [`ScriptValue::Array`].
    fn execute(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let codes = self
                .programs
                .lock()
                .expect("js engine program lock")
                .clone();
            if codes.is_empty() {
                let err = ScriptError::Failed("javascript engine: no program loaded".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            }
            let (quota, start) = self.quota_guard_start();
            let rx = self.send(|reply| Command::EvalAll { codes, reply })?;
            let outcome = self.settle(rx.await.map_err(|_| {
                ScriptError::Failed("javascript engine: worker unavailable".to_string())
            })?)?;
            self.quota_guard_end(quota, start)?;
            self.clear_last_error();
            Ok(ScriptValue::Array(outcome))
        })
    }

    /// From-key execution: the freshly loaded program
    /// runs **alone** and answers with its single result.
    fn execute_from_key<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let code = self.load_code(key).await?;
            let (quota, start) = self.quota_guard_start();
            let rx = self.send(|reply| Command::EvalOne { code, reply })?;
            let outcome = self.settle(rx.await.map_err(|_| {
                ScriptError::Failed("javascript engine: worker unavailable".to_string())
            })?)?;
            self.quota_guard_end(quota, start)?;
            self.clear_last_error();
            Ok(outcome)
        })
    }

    fn execute_from_keys<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<ScriptValue>, ScriptError>> {
        Box::pin(async move {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                results.push(self.execute_from_key(key).await?);
            }
            Ok(results)
        })
    }

    fn execute_string<'a>(
        &'a self,
        _name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let (quota, start) = self.quota_guard_start();
            let rx = self.send(|reply| Command::EvalOne {
                code: code.to_string(),
                reply,
            })?;
            let outcome = self.settle(rx.await.map_err(|_| {
                ScriptError::Failed("javascript engine: worker unavailable".to_string())
            })?)?;
            self.quota_guard_end(quota, start)?;
            self.clear_last_error();
            Ok(outcome)
        })
    }
}

impl SyncExecutor for JsEngine {
    fn execute_sync(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let codes = self
                .programs
                .lock()
                .expect("js engine program lock")
                .clone();
            if codes.is_empty() {
                let err = ScriptError::Failed("javascript engine: no program loaded".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            }
            let (quota, start) = self.quota_guard_start();
            let rx = self.send(|reply| Command::EvalAll { codes, reply })?;
            let outcome = self.settle(rx.await.map_err(|_| {
                ScriptError::Failed("javascript engine: worker unavailable".to_string())
            })?)?;
            self.quota_guard_end(quota, start)?;
            self.clear_last_error();
            Ok(ScriptValue::Array(outcome))
        })
    }
}

impl GlobalAccessor for JsEngine {
    fn register_global(&self, name: &str, value: ScriptValue) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        let rx = self.send(|reply| Command::SetGlobal {
            name: name.to_string(),
            value,
            reply,
        })?;
        let outcome = futures::executor::block_on(async move {
            match rx.await {
                Ok(result) => result,
                Err(_) => Err(ScriptError::Failed(
                    "javascript engine: worker unavailable".to_string(),
                )),
            }
        });
        self.settle(outcome)?;
        self.clear_last_error();
        Ok(())
    }

    fn get_global(&self, name: &str) -> Result<ScriptValue, ScriptError> {
        self.guard_initialized()?;
        let rx = self.send(|reply| Command::GetGlobal {
            name: name.to_string(),
            reply,
        })?;
        let outcome = futures::executor::block_on(async move {
            match rx.await {
                Ok(result) => result,
                Err(_) => Err(ScriptError::Failed(
                    "javascript engine: worker unavailable".to_string(),
                )),
            }
        });
        let value = self.settle(outcome)?;
        self.clear_last_error();
        Ok(value)
    }
}

impl rushwind_script::FunctionRegistrar for JsEngine {
    /// Always fails — see the crate docs: boa needs an `unsafe`
    /// closure transmute for capturing natives, and the workspace
    /// forbids unsafe code.
    fn register_function(
        &self,
        _name: &str,
        _function: rushwind_script::HostFunction,
    ) -> Result<(), ScriptError> {
        let err = ScriptError::Failed(
            "javascript engine: host functions are unavailable under the no-unsafe policy"
                .to_string(),
        );
        self.set_last_error(err.clone());
        Err(err)
    }

    fn call_function<'a>(
        &'a self,
        name: &'a str,
        args: &'a [ScriptValue],
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let (quota, start) = self.quota_guard_start();
            let rx = self.send(|reply| Command::Call {
                name: name.to_string(),
                args: args.to_vec(),
                reply,
            })?;
            let outcome = self.settle(rx.await.map_err(|_| {
                ScriptError::Failed("javascript engine: worker unavailable".to_string())
            })?)?;
            self.quota_guard_end(quota, start)?;
            self.clear_last_error();
            Ok(outcome)
        })
    }
}

impl rushwind_script::ModuleRegistrar for JsEngine {
    fn register_module(&self, name: &str, module: ScriptValue) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        let rx = self.send(|reply| Command::SetModule {
            name: name.to_string(),
            value: module,
            reply,
        })?;
        let outcome = futures::executor::block_on(async move {
            match rx.await {
                Ok(result) => result,
                Err(_) => Err(ScriptError::Failed(
                    "javascript engine: worker unavailable".to_string(),
                )),
            }
        });
        self.settle(outcome)?;
        self.clear_last_error();
        Ok(())
    }
}

impl ScriptWatcher for JsEngine {
    fn start_watch<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let source = self.source.lock().expect("js engine source lock").clone();
            let Some(source) = source else {
                let err = ScriptError::Failed("javascript engine: no source bound".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            };
            let stream = match source.watch(key).await {
                Ok(stream) => stream,
                Err(err) => {
                    self.set_last_error(err.clone());
                    return Err(err);
                }
            };
            let weak = self.weak.lock().expect("js engine weak lock").clone();
            if weak.upgrade().is_none() {
                let err =
                    ScriptError::Failed("javascript engine: engine handle unavailable".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            }
            let key_owned = key.to_string();
            let handle = tokio::spawn(async move {
                let mut stream = stream;
                loop {
                    if stream.next().await.is_none() {
                        break;
                    }
                    let Some(engine) = weak.upgrade() else {
                        break;
                    };
                    let _ = engine.load(&key_owned).await;
                }
            })
            .abort_handle();
            self.watchers
                .lock()
                .expect("js engine watcher lock")
                .insert(key.to_string(), handle);
            self.clear_last_error();
            Ok(())
        })
    }

    fn stop_watch(&self, key: &str) -> Result<(), ScriptError> {
        let handle = self
            .watchers
            .lock()
            .expect("js engine watcher lock")
            .remove(key);
        if let Some(handle) = handle {
            handle.abort();
        }
        Ok(())
    }
}

/// Builds an engine, arming the weak self-reference the watch tasks
/// need. [`register`] installs it in the factory registry under
/// [`NAME`].
pub fn factory() -> Result<SharedEngine, ScriptError> {
    let engine = Arc::new(JsEngine::new());
    *engine.weak.lock().expect("js engine weak lock") = Arc::downgrade(&engine);
    Ok(engine)
}

/// Installs the engine factory in the registry under [`NAME`]. Call
/// once at startup.
pub fn register() {
    let factory_fn: rushwind_script::EngineFactory = Arc::new(factory);
    let _ = register_factory(NAME, factory_fn);
}

#[cfg(test)]
mod tests;
