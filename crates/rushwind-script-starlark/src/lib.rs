//! The Starlark engine for the Rust script contract, built over Meta's
//! [`starlark`]
//! crate (starlark-rust).
//!
//! Semantics:
//!
//! - `load`/`load_multi`/`load_string` queue (name, source) pairs;
//!   `execute` runs every queued script in order over one shared
//!   environment, later scripts seeing earlier scripts' globals;
//!   `execute_from_key`/`execute_string` run one fetched or inline
//!   script the same way. Results are discarded — every execution
//!   answers `Null`.
//! - Globals accumulate across runs. The engine accumulates every
//!   successfully run script — queued and ad-hoc
//!   alike — and re-runs the whole set on each evaluation,
//!   reproducing an accumulated environment observably (starlark-rust
//!   cannot carry values between its
//!   per-evaluation heaps without `unsafe` conversions). The frozen
//!   successor of the latest run backs `get_global`'s readback.
//! - `register_global` populates the host environment;
//!   `register_module` flattens its entries into `name_key` globals.
//!   `get_global` reads the
//!   frozen environment first, then the host map.
//! - `call_function` invokes a script-defined function — the
//!   accumulated script set defines it again in the run's module —
//!   with bridged arguments and a bridged result.
//! - `start_watch`/`stop_watch` requeue the watched key on change
//!   ticks — reload only, never execution.
//!
//! Divergences and limits:
//!
//! - `register_function` **always fails**: a runtime-registered host
//!   callable needs `NativeFunction`, which starlark-rust keeps
//!   `pub(crate)` — no public constructor for a native function from
//!   a closure exists. `call_function` on script-defined functions
//!   works.
//! - Every evaluation re-runs the accumulated script set rather than
//!   carrying a globals dict forward: for deterministic scripts the
//!   resulting environment is identical, but a
//!   script with side effects (output, mutation of host-injected
//!   data) re-runs those effects on every evaluation. A finer
//!   origin split between script-owned and host-owned globals — each
//!   with its own
//!   precedence — is likewise unrecoverable; fresh host
//!   registrations land on top of the merged state each run.
//! - Integer globals clamp to Starlark's 32-bit range — arbitrary-
//!   precision integers have no counterpart here —
//!   and byte strings bridge as lossy UTF-8.
//! - The bridge-out rides the value JSON serializer: data values
//!   cross exactly as they do inbound, and anything the serializer
//!   rejects (functions, types) bridges as `Null`.
//! - The standard-library selection is fixed: every run evaluates
//!   against `Globals::standard()`, the crate's Starlark
//!   standard environment.
//!
//! # Engine matrix
//!
//! | Capability | Status |
//! |:---|:---|
//! | loader / executor / globals / modules / watch / lifecycle | implemented |
//! | host functions (`register_function`) | rejected — starlark-rust's native-function constructor is crate-private |
//! | sandbox / runtime hooks / sync executor / quota | not offered |

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use starlark::environment::{FrozenModule, Globals, Module};
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect};
use starlark::values::dict::AllocDict;
use starlark::values::list::AllocList;
use starlark::values::Value;
use tokio::task::AbortHandle;

use rushwind_script::{
    register_factory, BoxFuture, GlobalAccessor, ScriptEngine, ScriptError, ScriptExecutor,
    ScriptLoader, ScriptValue, ScriptWatcher, SharedEngine, SharedScriptSource,
};

/// The registry name.
pub const NAME: &str = "starlark";

/// The `starlark` engine: [`starlark`] — starlark-rust — behind a
/// frozen-module accumulation of the script globals.
pub struct StarlarkEngine {
    initialized: Mutex<bool>,
    source: Mutex<Option<SharedScriptSource>>,
    scripts: Mutex<Vec<(String, String)>>,
    host_globals: Mutex<HashMap<String, ScriptValue>>,
    frozen_globals: Mutex<Option<FrozenModule>>,
    watchers: Mutex<HashMap<String, AbortHandle>>,
    weak: Mutex<Weak<Self>>,
    last_error: Mutex<Option<ScriptError>>,
}

impl StarlarkEngine {
    /// Builds the engine uninitialized; call [`ScriptEngine::init`]
    /// before use. Prefer [`factory`], which also arms the weak
    /// self-reference the watch tasks need.
    pub fn new() -> Self {
        Self {
            initialized: Mutex::new(false),
            source: Mutex::new(None),
            scripts: Mutex::new(Vec::new()),
            host_globals: Mutex::new(HashMap::new()),
            frozen_globals: Mutex::new(None),
            watchers: Mutex::new(HashMap::new()),
            weak: Mutex::new(Weak::new()),
            last_error: Mutex::new(None),
        }
    }

    fn set_last_error(&self, error: ScriptError) {
        *self
            .last_error
            .lock()
            .expect("starlark engine last-error lock") = Some(error);
    }

    fn clear_last_error(&self) {
        *self
            .last_error
            .lock()
            .expect("starlark engine last-error lock") = None;
    }

    fn guard_initialized(&self) -> Result<(), ScriptError> {
        if !*self.initialized.lock().expect("starlark engine init lock") {
            let err = ScriptError::Failed("starlark engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        Ok(())
    }

    fn stop_all_watchers(&self) {
        let watchers: Vec<AbortHandle> = {
            let mut watchers = self.watchers.lock().expect("starlark engine watcher lock");
            watchers.drain().map(|(_, handle)| handle).collect()
        };
        for handle in watchers {
            handle.abort();
        }
    }

    /// The source-fetch step — shared by
    /// [`ScriptLoader::load`] and
    /// [`ScriptExecutor::execute_from_key`], which each fetch
    /// independently.
    async fn load_code(&self, key: &str) -> Result<String, ScriptError> {
        self.guard_initialized()?;
        let source = self
            .source
            .lock()
            .expect("starlark engine source lock")
            .clone();
        let Some(source) = source else {
            let err = ScriptError::Failed("starlark engine: no source bound".to_string());
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

    /// The predeclared merge, host side: the host
    /// registrations into the run's module. The script side — the
    /// persistent script globals — is covered by
    /// the accumulated script set re-running; see the crate docs for
    /// that design note.
    fn inject_globals<'v>(module: &Module<'v>, host: &HashMap<String, ScriptValue>) {
        for (name, value) in host {
            module.set(name, bridge::to_starlark(module, value));
        }
    }

    /// The execution loop: every script runs in order
    /// over one module — later scripts seeing earlier scripts'
    /// globals — and the module's frozen successor, the readback
    /// snapshot, comes back. A failing script aborts the loop with
    /// nothing accumulated.
    fn run_scripts(
        host: &HashMap<String, ScriptValue>,
        scripts: &[(String, String)],
    ) -> Result<FrozenModule, ScriptError> {
        Module::with_temp_heap(|module| {
            Self::inject_globals(&module, host);
            let globals = Globals::standard();
            {
                let mut eval = Evaluator::new(&module);
                for (name, src) in scripts {
                    let ast =
                        AstModule::parse(name, src.clone(), &Dialect::Standard).map_err(|err| {
                            ScriptError::Failed(format!(
                                "starlark engine: exec failed: {name}: {err}"
                            ))
                        })?;
                    eval.eval_module(ast, &globals).map_err(|err| {
                        ScriptError::Failed(format!("starlark engine: exec failed: {name}: {err}"))
                    })?;
                }
            }
            module
                .freeze()
                .map_err(|_| ScriptError::Failed("starlark engine: freeze failed".to_string()))
        })
    }

    /// Runs the accumulated script set plus `extra`, stores the
    /// frozen successor for readback, and — on success — folds
    /// `extra` into the accumulated set: the
    /// accumulate-then-clear-errors tail, answering `Null` for the
    /// run itself.
    fn run_and_store(&self, extra: &[(String, String)]) -> Result<ScriptValue, ScriptError> {
        let mut scripts = self
            .scripts
            .lock()
            .expect("starlark engine script lock")
            .clone();
        scripts.extend_from_slice(extra);
        let host = self
            .host_globals
            .lock()
            .expect("starlark engine host-global lock")
            .clone();
        let outcome = Self::run_scripts(&host, &scripts);
        let frozen = match outcome {
            Ok(frozen) => frozen,
            Err(err) => {
                self.set_last_error(err.clone());
                return Err(err);
            }
        };
        if !extra.is_empty() {
            self.scripts
                .lock()
                .expect("starlark engine script lock")
                .extend_from_slice(extra);
        }
        *self
            .frozen_globals
            .lock()
            .expect("starlark engine frozen-global lock") = Some(frozen);
        self.clear_last_error();
        Ok(ScriptValue::Null)
    }
}

impl Default for StarlarkEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptEngine for StarlarkEngine {
    fn engine_type(&self) -> &'static str {
        NAME
    }

    fn init(&self) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async move {
            {
                let mut initialized = self.initialized.lock().expect("starlark engine init lock");
                if *initialized {
                    let err =
                        ScriptError::Failed("starlark engine: already initialized".to_string());
                    self.set_last_error(err.clone());
                    return Err(err);
                }
                *initialized = true;
            }
            // Init resets the whole engine state.
            self.scripts
                .lock()
                .expect("starlark engine script lock")
                .clear();
            self.host_globals
                .lock()
                .expect("starlark engine host-global lock")
                .clear();
            *self
                .frozen_globals
                .lock()
                .expect("starlark engine frozen-global lock") = None;
            self.clear_last_error();
            Ok(())
        })
    }

    fn close(&self) -> Result<(), ScriptError> {
        {
            let mut initialized = self.initialized.lock().expect("starlark engine init lock");
            if !*initialized {
                let err = ScriptError::Failed("starlark engine: not initialized".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            }
            *initialized = false;
        }
        self.stop_all_watchers();
        *self.source.lock().expect("starlark engine source lock") = None;
        self.scripts
            .lock()
            .expect("starlark engine script lock")
            .clear();
        self.host_globals
            .lock()
            .expect("starlark engine host-global lock")
            .clear();
        *self
            .frozen_globals
            .lock()
            .expect("starlark engine frozen-global lock") = None;
        self.clear_last_error();
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        *self.initialized.lock().expect("starlark engine init lock")
    }

    fn last_error(&self) -> Option<ScriptError> {
        self.last_error
            .lock()
            .expect("starlark engine last-error lock")
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
}

/// The value bridge: contract values and Starlark values meeting over
/// the crate's allocation and JSON-serialization surfaces.
mod bridge {
    use super::*;

    /// Converts a JSON value back into a contract value.
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

    /// Clamps a contract integer into Starlark's 32-bit integer
    /// range — a documented divergence from
    /// arbitrary-precision integers.
    fn clamp_int(value: i128) -> i32 {
        value.clamp(i32::MIN as i128, i32::MAX as i128) as i32
    }

    /// Bridges a contract value into a Starlark value on the module's
    /// heap. Integers clamp to 32 bits; byte strings bridge as lossy
    /// UTF-8.
    pub fn to_starlark<'v>(module: &Module<'v>, value: &ScriptValue) -> Value<'v> {
        match value {
            ScriptValue::Null => Value::new_none(),
            ScriptValue::Bool(v) => module.heap().alloc(*v),
            ScriptValue::Int(v) => module.heap().alloc(clamp_int(*v as i128)),
            ScriptValue::UInt(v) => module.heap().alloc(clamp_int(*v as i128)),
            ScriptValue::Float(v) => module.heap().alloc(*v),
            ScriptValue::String(v) => module.heap().alloc(v.as_str()),
            ScriptValue::Bytes(v) => {
                let lossy = String::from_utf8_lossy(v);
                module.heap().alloc(lossy.as_ref())
            }
            ScriptValue::Array(items) => module.heap().alloc(AllocList(
                items
                    .iter()
                    .map(|item| to_starlark(module, item))
                    .collect::<Vec<Value>>(),
            )),
            ScriptValue::Map(map) => module.heap().alloc(AllocDict(
                map.iter()
                    .map(|(k, v)| {
                        (
                            to_starlark(module, &ScriptValue::String(k.clone())),
                            to_starlark(module, v),
                        )
                    })
                    .collect::<Vec<(Value, Value)>>(),
            )),
            _ => Value::new_none(),
        }
    }

    /// Bridges a Starlark value back through the value JSON
    /// serializer. Whatever the serializer rejects — functions, types
    /// — bridges as `Null`.
    pub fn from_starlark(value: Value<'_>) -> ScriptValue {
        match value.to_json_value() {
            Ok(json) => from_json(json),
            Err(_) => ScriptValue::Null,
        }
    }
}

impl ScriptLoader for StarlarkEngine {
    fn set_source(&self, source: Option<SharedScriptSource>) {
        *self.source.lock().expect("starlark engine source lock") = source;
    }

    fn get_source(&self) -> Option<SharedScriptSource> {
        self.source
            .lock()
            .expect("starlark engine source lock")
            .clone()
    }

    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            // Fetch, then queue — LoadString's
            // append step.
            let code = self.load_code(key).await?;
            self.scripts
                .lock()
                .expect("starlark engine script lock")
                .push((key.to_string(), code));
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
        name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<(), ScriptError>> {
        let outcome = (|| {
            self.guard_initialized()?;
            self.scripts
                .lock()
                .expect("starlark engine script lock")
                .push((name.to_string(), code.to_string()));
            self.clear_last_error();
            Ok(())
        })();
        Box::pin(async move { outcome })
    }
}

impl ScriptExecutor for StarlarkEngine {
    fn execute(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let scripts = self
                .scripts
                .lock()
                .expect("starlark engine script lock")
                .clone();
            if scripts.is_empty() {
                let err = ScriptError::Failed("starlark engine: no script loaded".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            }
            self.run_and_store(&[])
        })
    }

    /// From-key execution: the fetched script runs
    /// alone, its globals accumulating like any other run.
    fn execute_from_key<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let code = self.load_code(key).await?;
            let adhoc = [(key.to_string(), code)];
            self.run_and_store(&adhoc)
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
        name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let adhoc = [(name.to_string(), code.to_string())];
            self.run_and_store(&adhoc)
        })
    }
}

impl GlobalAccessor for StarlarkEngine {
    fn register_global(&self, name: &str, value: ScriptValue) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        self.host_globals
            .lock()
            .expect("starlark engine host-global lock")
            .insert(name.to_string(), value);
        self.clear_last_error();
        Ok(())
    }

    fn get_global(&self, name: &str) -> Result<ScriptValue, ScriptError> {
        self.guard_initialized()?;
        // The frozen environment first, then the host map.
        let frozen = self
            .frozen_globals
            .lock()
            .expect("starlark engine frozen-global lock")
            .clone();
        if let Some(frozen) = frozen {
            if let Ok(Some(value)) = frozen.get_option(name) {
                let bridged = bridge::from_starlark(value.value());
                self.clear_last_error();
                return Ok(bridged);
            }
        }
        let host = self
            .host_globals
            .lock()
            .expect("starlark engine host-global lock")
            .get(name)
            .cloned();
        if let Some(value) = host {
            self.clear_last_error();
            return Ok(value);
        }
        let err = ScriptError::Failed(format!("starlark engine: global not found: {name}"));
        self.set_last_error(err.clone());
        Err(err)
    }
}

impl rushwind_script::FunctionRegistrar for StarlarkEngine {
    /// Always fails — see the crate docs: starlark-rust keeps its
    /// native-function constructor crate-private, so a runtime host
    /// callable cannot be built.
    fn register_function(
        &self,
        _name: &str,
        _function: rushwind_script::HostFunction,
    ) -> Result<(), ScriptError> {
        let err = ScriptError::Failed(
            "starlark engine: host functions are unavailable — starlark-rust's native-function constructor is crate-private"
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
            let scripts = self
                .scripts
                .lock()
                .expect("starlark engine script lock")
                .clone();
            let host = self
                .host_globals
                .lock()
                .expect("starlark engine host-global lock")
                .clone();
            let outcome = Module::with_temp_heap(|module| {
                Self::inject_globals(&module, &host);
                // The accumulated script set defines the function
                // again in this run's module.
                {
                    let globals = Globals::standard();
                    let mut eval = Evaluator::new(&module);
                    for (script_name, src) in &scripts {
                        let ast = AstModule::parse(script_name, src.clone(), &Dialect::Standard)
                            .map_err(|err| {
                                ScriptError::Failed(format!(
                                    "starlark engine: exec failed: {script_name}: {err}"
                                ))
                            })?;
                        eval.eval_module(ast, &globals).map_err(|err| {
                            ScriptError::Failed(format!(
                                "starlark engine: exec failed: {script_name}: {err}"
                            ))
                        })?;
                    }
                }
                let Some(function) = module.get(name) else {
                    return Err(ScriptError::Failed(format!(
                        "starlark engine: function not found: {name}"
                    )));
                };
                let starlark_args: Vec<_> = args
                    .iter()
                    .map(|arg| bridge::to_starlark(&module, arg))
                    .collect();
                let mut eval = Evaluator::new(&module);
                let result = eval
                    .eval_function(function, &starlark_args, &[])
                    .map_err(|err| {
                        ScriptError::Failed(format!("starlark engine: call failed: {name}: {err}"))
                    })?;
                Ok(bridge::from_starlark(result))
            });
            match outcome {
                Ok(value) => {
                    self.clear_last_error();
                    Ok(value)
                }
                Err(err) => {
                    self.set_last_error(err.clone());
                    Err(err)
                }
            }
        })
    }
}

impl rushwind_script::ModuleRegistrar for StarlarkEngine {
    /// Flattening: every module entry
    /// becomes a `name_key` host global.
    fn register_module(&self, name: &str, module: ScriptValue) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        let ScriptValue::Map(map) = module else {
            let err = ScriptError::Failed("starlark engine: module must be a map".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        {
            let mut host = self
                .host_globals
                .lock()
                .expect("starlark engine host-global lock");
            for (key, value) in map {
                host.insert(format!("{name}_{key}"), value);
            }
        }
        self.clear_last_error();
        Ok(())
    }
}

impl ScriptWatcher for StarlarkEngine {
    fn start_watch<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let source = self
                .source
                .lock()
                .expect("starlark engine source lock")
                .clone();
            let Some(source) = source else {
                let err = ScriptError::Failed("starlark engine: no source bound".to_string());
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
            let weak = self.weak.lock().expect("starlark engine weak lock").clone();
            if weak.upgrade().is_none() {
                let err =
                    ScriptError::Failed("starlark engine: engine handle unavailable".to_string());
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
                .expect("starlark engine watcher lock")
                .insert(key.to_string(), handle);
            self.clear_last_error();
            Ok(())
        })
    }

    fn stop_watch(&self, key: &str) -> Result<(), ScriptError> {
        let handle = self
            .watchers
            .lock()
            .expect("starlark engine watcher lock")
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
    let engine = Arc::new(StarlarkEngine::new());
    *engine.weak.lock().expect("starlark engine weak lock") = Arc::downgrade(&engine);
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
