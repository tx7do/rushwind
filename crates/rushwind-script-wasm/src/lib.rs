//! The WebAssembly engine for the Rust script contract, built over
//! [`wasmi`], the
//! pure-Rust interpreter.
//!
//! Semantics:
//!
//! - `load` compiles a module and keeps it; `execute` instantiates
//!   the **last** loaded module and invokes its `_start` export.
//! - A module without a `_start` export instantiates silently and the
//!   run answers `Null` — the module stays instantiated.
//! - Globals register into the engine and link into every
//!   instantiation under the `env` import module
//!   (`register_global` marshals the wasm scalars only — Int/UInt →
//!   `i64`, Float → `f64`, Bool → `i32`; anything else is refused at
//!   registration). Scripts cannot persist mutations: each run gets a
//!   fresh store, so the registry is the source of truth and
//!   `get_global` reads it.
//! - `call_function` invokes a **named export of the last-loaded
//!   module** with marshaled arguments — Bool → any integer param,
//!   Int/UInt → `i32`/`i64` (range-checked, never truncated), Float →
//!   `f64`/`f32` (f32 narrows, the signature demands it); results come
//!   back Int/Float, and non-scalar results answer `Null`. Arity and
//!   type mismatches refuse as `Failed` — nothing is silently coerced
//!   across the boundary.
//! - `register_function` stays refused: the contract's
//!   [`HostFunction`](rushwind_script::HostFunction) is async while
//!   wasmi executes synchronously, and bridging the two honestly needs
//!   a reactor (inline-polling inside the interpreter deadlocks any
//!   host function that awaits a runtime-cooperative primitive). The
//!   invocation half of the capability — calling into the module — is
//!   the implemented face.
//!
//! Divergences and limits:
//!
//! - Wasm bytes ride the contract's [`ScriptSource`](rushwind_script)
//!   `String` carrier, which is UTF-8; modules must
//!   therefore be UTF-8-clean byte strings. Hand-encoded minimal sections
//!   are; compiler output generally is not.
//! - A separate runtime handle and its close
//!   bookkeeping collapse into the engine dropping its
//!   [`wasmi::Engine`] on close.
//!
//! # Engine matrix
//!
//! | Capability | Status |
//! |:---|:---|
//! | loader / executor / lifecycle | implemented |
//! | globals | implemented (`env`-linked registry; wasm scalars only) |
//! | functions | `call_function` (export invocation) implemented; `register_function` refused — see above |
//! | modules, watch, sandbox, hooks, sync+quota | not offered |

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use wasmi::{Engine, Global, Instance, Linker, Module, Mutability, Store, Val, ValType};

use rushwind_script::{
    register_factory, BoxFuture, ScriptEngine, ScriptError, ScriptExecutor, ScriptLoader,
    ScriptValue, SharedEngine, SharedScriptSource,
};

/// The import module name registered globals link under.
const GLOBAL_MODULE: &str = "env";

/// The `wasm` engine: [`wasmi`] module compilation and `_start`
/// invocation.
pub struct WasmEngine {
    engine: Mutex<Option<Engine>>,
    source: Mutex<Option<SharedScriptSource>>,
    modules: Mutex<Vec<Module>>,
    globals: Mutex<BTreeMap<String, ScriptValue>>,
    initialized: Mutex<bool>,
    last_error: Mutex<Option<ScriptError>>,
}

impl WasmEngine {
    /// Builds the engine uninitialized; call
    /// [`ScriptEngine::init`] before use.
    pub fn new() -> Self {
        Self {
            engine: Mutex::new(None),
            source: Mutex::new(None),
            modules: Mutex::new(Vec::new()),
            globals: Mutex::new(BTreeMap::new()),
            initialized: Mutex::new(false),
            last_error: Mutex::new(None),
        }
    }

    fn set_last_error(&self, error: ScriptError) {
        *self.last_error.lock().expect("wasm engine last-error lock") = Some(error);
    }

    fn clear_last_error(&self) {
        *self.last_error.lock().expect("wasm engine last-error lock") = None;
    }

    /// Instantiates the last-loaded module into a fresh store, with
    /// the registered globals linked under the `env` import module.
    /// Each run is isolated: in-run global mutations die with the
    /// store, the registry stays the source of truth.
    fn instantiate_last(&self) -> Result<(Store<()>, Instance), ScriptError> {
        let (engine, module) = {
            let engine = self.engine.lock().expect("wasm engine handle lock").clone();
            let module = self
                .modules
                .lock()
                .expect("wasm engine module lock")
                .last()
                .cloned();
            (engine, module)
        };
        let Some(engine) = engine else {
            return Err(ScriptError::Failed(
                "wasm engine: not initialized".to_string(),
            ));
        };
        let Some(module) = module else {
            return Err(ScriptError::Failed(
                "wasm engine: no module loaded".to_string(),
            ));
        };
        let mut store = Store::new(&engine, ());
        let mut linker = Linker::new(&engine);
        {
            let globals = self.globals.lock().expect("wasm engine globals lock");
            for (name, value) in globals.iter() {
                let val = script_global_to_wasm(value).ok_or_else(|| {
                    ScriptError::Failed(format!(
                        "wasm engine: global {name} does not marshal to a wasm scalar"
                    ))
                })?;
                linker
                    .define(
                        GLOBAL_MODULE,
                        name,
                        Global::new(&mut store, val, Mutability::Const),
                    )
                    .map_err(|e| ScriptError::Failed(format!("wasm engine: global {name}: {e}")))?;
            }
        }
        let instance = linker
            .instantiate_and_start(&mut store, &module)
            .map_err(|err| {
                ScriptError::Failed(format!("wasm engine: instantiation failed: {err}"))
            })?;
        Ok((store, instance))
    }

    /// A missing `_start` export answers `Null`; the run proceeds only
    /// for an exported zero-argument zero-result function.
    fn run_last(&self) -> Result<ScriptValue, ScriptError> {
        if !self.is_initialized() {
            let err = ScriptError::Failed("wasm engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        let (mut store, instance) = match self.instantiate_last() {
            Ok(pair) => pair,
            Err(err) => {
                self.set_last_error(err.clone());
                return Err(err);
            }
        };
        let Some(func_extern) = instance.get_export(&store, "_start") else {
            self.clear_last_error();
            return Ok(ScriptValue::Null);
        };
        let Some(func) = func_extern.into_func() else {
            self.clear_last_error();
            return Ok(ScriptValue::Null);
        };
        let Ok(start) = func.typed::<(), ()>(&store) else {
            self.clear_last_error();
            return Ok(ScriptValue::Null);
        };
        if let Err(err) = start.call(&mut store, ()) {
            let wrapped = ScriptError::Failed(format!("wasm engine: _start trapped: {err}"));
            self.set_last_error(wrapped.clone());
            return Err(wrapped);
        }
        self.clear_last_error();
        Ok(ScriptValue::Null)
    }

    /// Invokes the named export of the last-loaded module with
    /// marshaled arguments. Arity must match exactly and every
    /// argument must marshal to the signature's param type — the
    /// boundary refuses rather than coerces.
    fn call_export(&self, name: &str, args: &[ScriptValue]) -> Result<ScriptValue, ScriptError> {
        let (mut store, instance) = self.instantiate_last()?;
        let Some(export) = instance.get_export(&store, name) else {
            return Err(ScriptError::Failed(format!(
                "wasm engine: no export named {name}"
            )));
        };
        let Some(func) = export.into_func() else {
            return Err(ScriptError::Failed(format!(
                "wasm engine: export {name} is not a function"
            )));
        };
        let ty = func.ty(&store);
        if ty.params().len() != args.len() {
            return Err(ScriptError::Failed(format!(
                "wasm engine: export {name} takes {} arguments, got {}",
                ty.params().len(),
                args.len()
            )));
        }
        let mut inputs: Vec<Val> = Vec::with_capacity(args.len());
        for (arg, param) in args.iter().zip(ty.params()) {
            inputs.push(marshal_argument(arg, *param)?);
        }
        let mut outputs = vec![Val::I32(0); ty.results().len()];
        func.call(&mut store, &inputs, &mut outputs)
            .map_err(|err| ScriptError::Failed(format!("wasm engine: {name} trapped: {err}")))?;
        Ok(unmarshal_results(&outputs))
    }
}

/// Marshals a registered global into its wasm scalar; `None` for
/// values with no wasm representation (refused at registration, so
/// this only guards the boundary).
fn script_global_to_wasm(value: &ScriptValue) -> Option<Val> {
    Some(match value {
        ScriptValue::Bool(b) => Val::I32(i32::from(*b)),
        ScriptValue::Int(i) => Val::I64(*i),
        ScriptValue::UInt(u) => Val::I64(i64::try_from(*u).ok()?),
        ScriptValue::Float(f) => Val::F64((*f).into()),
        _ => return None,
    })
}

/// Marshals one call argument against the signature's param type.
/// Strictness: Bool maps to integer params (wasm's canonical bool),
/// Int/UInt to i32/i64 with range checks (never truncated), Float to
/// f64 — and to f32 only there, where the signature demands the
/// narrowing. Anything else refuses.
fn marshal_argument(arg: &ScriptValue, param: ValType) -> Result<Val, ScriptError> {
    let mismatch = || {
        Err(ScriptError::Failed(format!(
            "wasm engine: argument {arg:?} does not marshal to param type {param:?}"
        )))
    };
    Ok(match (arg, param) {
        (ScriptValue::Bool(b), ValType::I32) => Val::I32(i32::from(*b)),
        (ScriptValue::Bool(b), ValType::I64) => Val::I64(i64::from(*b)),
        (ScriptValue::Int(i), ValType::I32) => Val::I32(i32::try_from(*i).map_err(|_| {
            ScriptError::Failed(format!("wasm engine: {i} does not fit an i32 param"))
        })?),
        (ScriptValue::Int(i), ValType::I64) => Val::I64(*i),
        (ScriptValue::UInt(u), ValType::I32) => Val::I32(i32::try_from(*u).map_err(|_| {
            ScriptError::Failed(format!("wasm engine: {u} does not fit an i32 param"))
        })?),
        (ScriptValue::UInt(u), ValType::I64) => Val::I64(i64::try_from(*u).map_err(|_| {
            ScriptError::Failed(format!("wasm engine: {u} does not fit an i64 param"))
        })?),
        (ScriptValue::Float(f), ValType::F64) => Val::F64((*f).into()),
        (ScriptValue::Float(f), ValType::F32) => Val::F32(wasmi::F32::from_float(*f as f32)),
        _ => return mismatch(),
    })
}

/// Unmarshals a call's results: wasm scalars become Int/Float, and
/// anything without a data-shape representation (funcref, externref,
/// v128) answers `Null` — the contract's documented bridge rule.
fn unmarshal_results(outputs: &[Val]) -> ScriptValue {
    match outputs {
        [Val::I32(v)] => ScriptValue::Int(i64::from(*v)),
        [Val::I64(v)] => ScriptValue::Int(*v),
        [Val::F32(v)] => ScriptValue::Float(f64::from(v.to_float())),
        [Val::F64(v)] => ScriptValue::Float(v.to_float()),
        _ => ScriptValue::Null,
    }
}

impl Default for WasmEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptEngine for WasmEngine {
    fn engine_type(&self) -> &'static str {
        "wasm"
    }

    fn init(&self) -> BoxFuture<'_, Result<(), ScriptError>> {
        let outcome = {
            let mut initialized = self.initialized.lock().expect("wasm engine init lock");
            if *initialized {
                Err(ScriptError::Failed(
                    "wasm engine: already initialized".to_string(),
                ))
            } else {
                *initialized = true;
                *self.engine.lock().expect("wasm engine handle lock") = Some(Engine::default());
                Ok(())
            }
        };
        match &outcome {
            Err(err) => self.set_last_error(err.clone()),
            Ok(()) => self.clear_last_error(),
        }
        Box::pin(async move { outcome })
    }

    fn close(&self) -> Result<(), ScriptError> {
        {
            let mut initialized = self.initialized.lock().expect("wasm engine init lock");
            *initialized = false;
            *self.engine.lock().expect("wasm engine handle lock") = None;
            self.modules
                .lock()
                .expect("wasm engine module lock")
                .clear();
            *self.source.lock().expect("wasm engine source lock") = None;
        }
        self.clear_last_error();
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        *self.initialized.lock().expect("wasm engine init lock")
    }

    fn last_error(&self) -> Option<ScriptError> {
        self.last_error
            .lock()
            .expect("wasm engine last-error lock")
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

    fn as_global_accessor(self: Arc<Self>) -> Option<Arc<dyn rushwind_script::GlobalAccessor>> {
        Some(self)
    }

    fn as_function_registrar(
        self: Arc<Self>,
    ) -> Option<Arc<dyn rushwind_script::FunctionRegistrar>> {
        Some(self)
    }
}

impl WasmEngine {
    fn compile(&self, code: &str) -> Result<(), ScriptError> {
        if !self.is_initialized() {
            let err = ScriptError::Failed("wasm engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        let engine = self.engine.lock().expect("wasm engine handle lock").clone();
        let Some(engine) = engine else {
            let err = ScriptError::Failed("wasm engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        match Module::new(&engine, code.as_bytes()) {
            Ok(module) => {
                self.modules
                    .lock()
                    .expect("wasm engine module lock")
                    .push(module);
                self.clear_last_error();
                Ok(())
            }
            Err(err) => {
                let wrapped = ScriptError::Failed(format!("wasm engine: compile failed: {err}"));
                self.set_last_error(wrapped.clone());
                Err(wrapped)
            }
        }
    }

    /// The sync half of a source-driven load: the bound-source check.
    fn bound_source(&self) -> Result<SharedScriptSource, ScriptError> {
        if !self.is_initialized() {
            let err = ScriptError::Failed("wasm engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        let source = self.source.lock().expect("wasm engine source lock").clone();
        let Some(source) = source else {
            let err = ScriptError::Failed("wasm engine: no source bound".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        Ok(source)
    }
}

impl ScriptLoader for WasmEngine {
    fn set_source(&self, source: Option<SharedScriptSource>) {
        *self.source.lock().expect("wasm engine source lock") = source;
    }

    fn get_source(&self) -> Option<SharedScriptSource> {
        self.source.lock().expect("wasm engine source lock").clone()
    }

    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            let source = self.bound_source()?;
            let code = match source.load(key).await {
                Ok(code) => code,
                Err(err) => {
                    self.set_last_error(err.clone());
                    return Err(err);
                }
            };
            self.compile(&code)
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
        Box::pin(async move { self.compile(code) })
    }
}

impl ScriptExecutor for WasmEngine {
    fn execute(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
        Box::pin(async move { self.run_last() })
    }

    fn execute_from_key<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.load(key).await?;
            self.run_last()
        })
    }

    fn execute_from_keys<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<ScriptValue>, ScriptError>> {
        Box::pin(async move {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                let value = self.execute_from_key(key).await?;
                results.push(value);
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
            self.compile(code)?;
            self.run_last()
        })
    }
}

/// The factory hook: a fresh uninitialized engine.
pub fn factory() -> Result<SharedEngine, ScriptError> {
    Ok(Arc::new(WasmEngine::new()))
}

/// Installs the engine factory in the registry under the `wasm`
/// type. Call once at startup.
pub fn register() {
    let factory_fn: rushwind_script::EngineFactory = Arc::new(factory);
    let _ = register_factory("wasm", factory_fn);
}

// ---------------------------------------------------------------------
// The aggregate's remaining faces. Globals and export invocation are
// real capabilities; host-function registration, modules, and watch
// stay as absent-capability stubs (the trait set stays satisfied, the
// stubs answer CapabilityNotSupported, and the watchers' probes on
// the core trait stay `None` so well-behaved callers never reach
// them).
// ---------------------------------------------------------------------

impl rushwind_script::GlobalAccessor for WasmEngine {
    /// Registers or overwrites a global. Only the wasm scalars are
    /// accepted, in their representable range — a global a module
    /// could not import would be a lie in the registry.
    fn register_global(&self, name: &str, value: ScriptValue) -> Result<(), ScriptError> {
        if name.is_empty() {
            return Err(ScriptError::Failed(
                "wasm engine: global name is empty".to_string(),
            ));
        }
        let marshals = match &value {
            ScriptValue::Bool(_) | ScriptValue::Int(_) | ScriptValue::Float(_) => true,
            ScriptValue::UInt(u) => *u <= i64::MAX as u64,
            _ => false,
        };
        if !marshals {
            return Err(ScriptError::Failed(format!(
                "wasm engine: global {name} must be a wasm scalar in i64/f64 range (Bool/Int/UInt/Float), got {value:?}"
            )));
        }
        self.globals
            .lock()
            .expect("wasm engine globals lock")
            .insert(name.to_string(), value);
        self.clear_last_error();
        Ok(())
    }

    /// Reads a global from the registry — the source of truth (in-run
    /// mutations die with the run's store).
    fn get_global(&self, name: &str) -> Result<ScriptValue, ScriptError> {
        self.globals
            .lock()
            .expect("wasm engine globals lock")
            .get(name)
            .cloned()
            .ok_or_else(|| ScriptError::Failed(format!("wasm engine: no global named {name}")))
    }
}

impl rushwind_script::FunctionRegistrar for WasmEngine {
    /// Refused: the contract's host functions are async while wasmi
    /// executes synchronously, and an inline bridge would deadlock any
    /// host function awaiting a runtime primitive. See the module
    /// docs.
    fn register_function(
        &self,
        _name: &str,
        _function: rushwind_script::HostFunction,
    ) -> Result<(), ScriptError> {
        Err(ScriptError::CapabilityNotSupported)
    }

    /// Invokes the named export of the last-loaded module.
    fn call_function<'a>(
        &'a self,
        name: &'a str,
        args: &'a [ScriptValue],
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move { self.call_export(name, args) })
    }
}

impl rushwind_script::ModuleRegistrar for WasmEngine {
    fn register_module(&self, _name: &str, _module: ScriptValue) -> Result<(), ScriptError> {
        Err(ScriptError::CapabilityNotSupported)
    }
}

impl rushwind_script::ScriptWatcher for WasmEngine {
    fn start_watch<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async { Err(ScriptError::CapabilityNotSupported) })
    }

    fn stop_watch(&self, _key: &str) -> Result<(), ScriptError> {
        Err(ScriptError::CapabilityNotSupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_script::{FunctionRegistrar, GlobalAccessor, MemSource};
    use std::sync::Arc;

    /// A minimal UTF-8-clean module exporting `()->()` as `_start`.
    const START_MODULE: &str = concat!(
        "\u{0}asm\u{1}\u{0}\u{0}\u{0}",
        "\u{1}\u{4}\u{1}`\u{0}\u{0}",
        "\u{3}\u{2}\u{1}\u{0}",
        "\u{7}\u{a}\u{1}\u{6}_start\u{0}\u{0}",
        "\u{a}\u{4}\u{1}\u{2}\u{0}\u{b}",
    );

    /// The same module with the export section dropped: valid, with a
    /// function body but no exports, so instantiation is silent.
    const SILENT_MODULE: &str = concat!(
        "\u{0}asm\u{1}\u{0}\u{0}\u{0}",
        "\u{1}\u{4}\u{1}`\u{0}\u{0}",
        "\u{3}\u{2}\u{1}\u{0}",
        "\u{a}\u{4}\u{1}\u{2}\u{0}\u{b}",
    );

    async fn engine() -> Arc<WasmEngine> {
        let engine = Arc::new(WasmEngine::new());
        engine.init().await.expect("init");
        engine
    }

    #[tokio::test]
    async fn a_start_module_runs_and_answers_null() {
        let engine = engine().await;
        engine
            .load_string("m", START_MODULE)
            .await
            .expect("compile");
        assert_eq!(engine.execute().await.expect("run"), ScriptValue::Null);
    }

    #[tokio::test]
    async fn a_module_without_start_instantiates_silently() {
        let engine = engine().await;
        engine
            .load_string("m", SILENT_MODULE)
            .await
            .expect("compile");
        assert_eq!(
            engine.execute().await.expect("silent run"),
            ScriptValue::Null
        );
    }

    #[tokio::test]
    async fn garbage_fails_to_compile() {
        let engine = engine().await;
        assert!(matches!(
            engine.load_string("m", "not wasm at all").await,
            Err(ScriptError::Failed(msg)) if msg.contains("compile failed")
        ));
        assert!(engine.last_error().is_some());
        engine.clear_error();
        assert!(engine.last_error().is_none());
    }

    #[tokio::test]
    async fn nothing_loaded_fails() {
        let engine = engine().await;
        assert!(matches!(
            engine.execute().await,
            Err(ScriptError::Failed(msg)) if msg.contains("no module loaded")
        ));
    }

    #[tokio::test]
    async fn without_init_everything_fails() {
        let engine = Arc::new(WasmEngine::new());
        assert!(!engine.is_initialized());
        assert!(engine.load_string("m", START_MODULE).await.is_err());
        assert!(engine.execute().await.is_err());
        engine.init().await.expect("init");
        engine.init().await.expect_err("second init fails");
        assert!(engine.is_initialized());
        engine.close().expect("close");
        assert!(!engine.is_initialized());
    }

    #[tokio::test]
    async fn sources_drive_the_load() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("mod", START_MODULE);
        engine.set_source(Some(source));
        assert!(engine.get_source().is_some());
        engine.load("mod").await.expect("source-driven compile");
        assert_eq!(engine.execute().await.expect("run"), ScriptValue::Null);
    }

    #[tokio::test]
    async fn probes_offer_loader_executor_globals_and_functions() {
        let engine: Arc<dyn ScriptEngine> = Arc::new(WasmEngine::new());
        assert!(engine.clone().as_loader().is_some());
        assert!(engine.clone().as_executor().is_some());
        assert!(engine.clone().as_global_accessor().is_some());
        assert!(engine.clone().as_function_registrar().is_some());
        assert!(engine.clone().as_module_registrar().is_none());
        assert!(engine.clone().as_watcher().is_none());
        assert!(engine.clone().as_sandbox_configurator().is_none());
        assert!(engine.clone().as_runtime_hook_registrar().is_none());
        assert!(engine.clone().as_sync_executor().is_none());
        assert!(engine.as_quota_controller().is_none());
    }

    /// A module exporting `(param i64 i64) (result i64)` as `add`,
    /// computing `i64.add` — all bytes below 0x80 (the UTF-8 carrier).
    const ADD_MODULE: &str = concat!(
        "\u{0}asm\u{1}\u{0}\u{0}\u{0}",
        // type section: (i64, i64) -> i64
        "\u{1}\u{7}\u{1}`\u{2}\u{7e}\u{7e}\u{1}\u{7e}",
        // function section: one function of type 0
        "\u{3}\u{2}\u{1}\u{0}",
        // export section: "add" -> func 0
        "\u{7}\u{7}\u{1}\u{3}add\u{0}\u{0}",
        // code section: local.get 0; local.get 1; i64.add; end
        "\u{a}\u{9}\u{1}\u{7}\u{0}\u{20}\u{0}\u{20}\u{1}\u{7c}\u{0b}",
    );

    /// A module importing `env.answer: i32` and exporting
    /// `(result i32)` as `get`, returning the global.
    const GLOBAL_MODULE_BYTES: &str = concat!(
        "\u{0}asm\u{1}\u{0}\u{0}\u{0}",
        // type section: () -> i64
        "\u{1}\u{5}\u{1}`\u{0}\u{1}\u{7e}",
        // import section: env.answer, global i64 (immutable)
        "\u{2}\u{f}\u{1}\u{3}env\u{6}answer\u{3}\u{7e}\u{0}",
        // function section: one function of type 0
        "\u{3}\u{2}\u{1}\u{0}",
        // export section: "get" -> func 0
        "\u{7}\u{7}\u{1}\u{3}get\u{0}\u{0}",
        // code section: global.get 0; end
        "\u{a}\u{6}\u{1}\u{4}\u{0}\u{23}\u{0}\u{0b}",
    );

    #[tokio::test]
    async fn call_function_invokes_a_named_export_with_marshaled_args() {
        let engine = engine().await;
        engine.load_string("m", ADD_MODULE).await.expect("compile");
        let result = engine
            .call_function("add", &[ScriptValue::Int(20), ScriptValue::Int(22)])
            .await
            .expect("call");
        assert_eq!(result, ScriptValue::Int(42));
    }

    #[tokio::test]
    async fn call_function_marshals_bools_and_floats_strictly() {
        let engine = engine().await;
        engine.load_string("m", ADD_MODULE).await.expect("compile");

        // Bools marshal into integer params (wasm's canonical bool).
        let result = engine
            .call_function("add", &[ScriptValue::Bool(true), ScriptValue::Int(41)])
            .await
            .expect("bool to i64");
        assert_eq!(result, ScriptValue::Int(42));

        // Floats refuse integer params — no silent coercion.
        let err = engine
            .call_function("add", &[ScriptValue::Float(1.5), ScriptValue::Int(1)])
            .await
            .expect_err("float to i64 must refuse");
        assert!(matches!(err, ScriptError::Failed(msg) if msg.contains("does not marshal")));

        // Arity must match exactly.
        let err = engine
            .call_function("add", &[ScriptValue::Int(1)])
            .await
            .expect_err("arity mismatch");
        assert!(matches!(err, ScriptError::Failed(msg) if msg.contains("takes 2 arguments")));

        // Unknown exports refuse by name.
        let err = engine
            .call_function("nope", &[])
            .await
            .expect_err("unknown export");
        assert!(matches!(err, ScriptError::Failed(msg) if msg.contains("no export named nope")));
    }

    #[tokio::test]
    async fn registered_globals_link_into_instantiations() {
        let engine = engine().await;
        engine
            .register_global("answer", ScriptValue::Int(42))
            .expect("register");
        assert_eq!(
            engine.get_global("answer").expect("readback"),
            ScriptValue::Int(42)
        );
        engine
            .load_string("m", GLOBAL_MODULE_BYTES)
            .await
            .expect("compile");
        let result = engine.call_function("get", &[]).await.expect("call");
        assert_eq!(result, ScriptValue::Int(42));

        // Overwrite: the registry is the source of truth for the next
        // instantiation.
        engine
            .register_global("answer", ScriptValue::Int(7))
            .expect("overwrite");
        let result = engine.call_function("get", &[]).await.expect("call");
        assert_eq!(result, ScriptValue::Int(7));
    }

    #[tokio::test]
    async fn global_refusals_are_typed() {
        let engine = engine().await;

        // Non-scalars refuse at registration — a global a module could
        // never import would be a lie in the registry.
        for bad in [
            ScriptValue::String("s".to_string()),
            ScriptValue::Null,
            ScriptValue::Array(vec![]),
        ] {
            let err = engine
                .register_global("bad", bad)
                .expect_err("non-scalar must refuse");
            assert!(
                matches!(err, ScriptError::Failed(ref msg) if msg.contains("must be a wasm scalar")),
                "{err:?}"
            );
        }

        let err = engine.get_global("absent").expect_err("absent global");
        assert!(matches!(err, ScriptError::Failed(msg) if msg.contains("no global named absent")));

        // A module importing an unregistered global fails to
        // instantiate, naming the import.
        engine
            .load_string("m", GLOBAL_MODULE_BYTES)
            .await
            .expect("compile");
        let err = engine
            .call_function("get", &[])
            .await
            .expect_err("missing import");
        assert!(
            matches!(err, ScriptError::Failed(ref msg) if msg.contains("instantiation failed")),
            "{err:?}"
        );
    }

    /// UInt globals marshal within range and refuse past it.
    #[tokio::test]
    async fn uint_globals_marshal_within_range() {
        let engine = engine().await;
        engine
            .register_global("answer", ScriptValue::UInt(42))
            .expect("u64 fits");
        engine
            .load_string("m", GLOBAL_MODULE_BYTES)
            .await
            .expect("compile");
        assert_eq!(
            engine.call_function("get", &[]).await.expect("call"),
            ScriptValue::Int(42)
        );

        let err = engine
            .register_global("huge", ScriptValue::UInt(u64::MAX))
            .expect_err("u64::MAX does not fit an i64 global");
        assert!(matches!(err, ScriptError::Failed(msg) if msg.contains("must be a wasm scalar")));
    }
}
