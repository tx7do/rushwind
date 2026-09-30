//! Config-driven assembly for RushWind applications.
//!
//! [`Bootstrap`] turns a YAML document into a running-shaped
//! application: every engine family the document names is assembled
//! from its registered factories and exposed on the [`Bootstrapped`]
//! result for the application's own machinery and on the [`RouteInput`]
//! every route pack and server factory receives. Handlers, schemas,
//! gates, and job bodies stay in code — configuration picks which
//! registered pieces mount, it never contains logic.
//!
//! # The assembly matrix
//!
//! Every engine family follows one shape: a config section names a
//! factory from the registry the application stocked, the factory turns
//! engine-specific settings into the engine, and the assembled instance
//! lands on [`Bootstrapped`] and [`RouteInput`]. Engine crates whose
//! constructors carry a `from_settings` wire shape parse the settings
//! node directly inside the factory closure; the rest hand-assemble in
//! the closure over their builder options.
//!
//! | Family | Config section | Factory registration | Exposure |
//! |:---|:---|:---|:---|
//! | storage | `storage` | [`Bootstrap::storage_factory`] | [`Bootstrapped::repository`] |
//! | registry | `registry` | [`Bootstrap::registry_factory`] | [`Bootstrapped::discovery`] (the announcement rides [`Bootstrapped::registration`]) |
//! | authn | `authn.<instance>.engine` | [`Bootstrap::authn_factory`] | [`Bootstrapped::authn`] |
//! | authz | `authz.<instance>.engine` | [`Bootstrap::authz_factory`] | [`Bootstrapped::authz`] |
//! | broker | `brokers.<instance>.engine` | [`Bootstrap::broker_factory`] | [`Bootstrapped::brokers`] |
//! | cache | `caches.<instance>.engine` | [`Bootstrap::cache_factory`] | [`Bootstrapped::caches`] |
//! | circuit breaker | `circuitbreakers.<instance>.engine` | [`Bootstrap::circuitbreaker_factory`] | [`Bootstrapped::circuitbreakers`] |
//! | rate limiter | `limiters.<instance>.engine` | [`Bootstrap::limiter_factory`] | [`Bootstrapped::limiters`] |
//! | AI chat | `ai.engine` | [`Bootstrap::ai_factory`] | [`Bootstrapped::ai`] |
//! | object storage | `oss.engine` | [`Bootstrap::oss_factory`] | [`Bootstrapped::object_storage`] |
//! | metrics | `metrics.engine` | [`Bootstrap::metrics_factory`] | [`Bootstrapped::metrics`] |
//! | config sources | `config_sources[].engine` | [`Bootstrap::config_source_factory`] | [`Bootstrapped::config`] (multiple sources compose into the contract's priority fallback) |
//! | script engines | `scripts.<name>.engine` | the script domain's own factory registry (engines self-register via their `register()` functions) | [`Bootstrapped::scripts`] (a name-keyed [`Manager`] closed by a shutdown sweep) |
//!
//! The metrics family additionally carries the built-in Prometheus
//! engine under this crate's `metrics` feature — the one engine whose
//! concrete type the `/metrics` scrape mount needs.
//!
//! # The HTTP edge
//!
//! The built-in `http` server kind assembles through
//! [`HttpEdge`](rushwind_http::HttpEdge): the per-server `edge` block
//! toggles the request-id, logging, and recovery middlewares (defaults
//! on), sets the request budget, and configures CORS — through the
//! tower-http layer or the gorilla-compatible one when `compat` is set.
//! Listener addresses take the standard `host:port` form or the
//! host-any `":port"` form; request budgets take a second count or a
//! duration string (`"10s"`).
//! The domain mounts ride per-server `mounts` flags, each behind its
//! cargo feature: `health` serves `/healthz` + `/readyz` from the
//! aggregated health section, `metrics` serves `/metrics` from the
//! built-in Prometheus engine. A mount or section whose feature is
//! compiled out fails the assembly loudly.
//!
//! ## Per-subtree guards
//!
//! `route_packs[]` and `storage_endpoints[]` entries optionally name an
//! assembled authn instance and a permission point (an assembled authz
//! instance plus fixed action/resource axes, and a project axis that is
//! either a fixed string or the name of a credential claim). The wrap
//! composes the bridges [`with_authn`], [`with_authorization`],
//! [`with_authorization_for`], and [`with_authorization_claim`] around
//! that one subtree — the whitelisting model of the HTTP edge, where
//! public subtrees stay unwrapped and merge with the protected ones.
//! A pack's own closure receives its `route_packs[].settings` node
//! verbatim alongside the [`RouteInput`], and may wrap inner subtrees
//! further with anything the input carries.
//!
//! # Session transports
//!
//! Route packs yield a [`RouteSurface`]: the pack's router plus any
//! session-shutdown buses its sessions drain on. The ws family's
//! `WsRoute::build` yields its bus alongside the mountable route; the
//! pack forwards both and the assembler registers the bus with the
//! axum server, so a server shutdown relays into live sessions.
//!
//! Server kinds beyond `http` — the ws, quic, webtransport, h3, and
//! mqtt glue — mount through [`Bootstrap::server_factory`]: the
//! application's closure owns the TLS material, session handlers, and
//! gates (including the authn contract's `AuthenticationGate` over any
//! assembled authenticator on [`RouteInput`]) and reads its bind or
//! endpoint knobs from the settings node. The `cron` kind is built in:
//! registered jobs ( [`Bootstrap::cron_job`], application code) mount by
//! name, exactly like route packs.
//!
//! # Deliberately not wired
//!
//! - **Codecs** — the encoding domain is a process-wide registry of
//!   stateless engines with no settings; the application's `register()`
//!   one-liners are the whole integration.
//! - **Job storage** — the apalis Postgres backend is generic over the
//!   job payload type, which is application knowledge; its
//!   `from_settings` wire shape serves the application's own assembly.
//! - **Retry** — a static policy helper, nothing to instantiate.
//!
//! # A minimal document
//!
//! ```yaml
//! app:
//!   name: demo
//!   version: v0.1.0
//! storage:
//!   engine: memory
//!   settings: {}
//! servers:
//!   - kind: http
//!     bind: 127.0.0.1:8080
//!     route_packs:
//!       - name: health
//! ```
//!
//! `route_packs[].name` names a pack registered with
//! [`Bootstrap::route_pack`]; the `memory` engine names a factory
//! registered with [`Bootstrap::storage_factory`].

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use rushwind_ai::ChatModel;
use rushwind_authn::Authenticator;
use rushwind_authz::Engine as AuthzEngine;
use rushwind_broker::Broker;
use rushwind_cache::Cache;
use rushwind_circuitbreaker::CircuitBreaker;
use rushwind_config::{FallbackSource, SharedSource, Source};
use rushwind_core::App;
use rushwind_http::{
    with_authn, with_authorization, with_authorization_claim, with_authorization_for, HttpEdge,
};
use rushwind_metrics::Metrics;
use rushwind_oss::ObjectStorage;
use rushwind_ratelimit::Limiter;
use rushwind_registry::{Discovery, Registrar, Registration, RegistrationHandle};
use rushwind_script::Manager as ScriptManager;
use rushwind_storage::Repository;
use rushwind_storage_axum::CrudApi;
use rushwind_transport::{Instance, Server, ServerError, StopSignal};
use rushwind_transport_axum::AxumServer;
use rushwind_transport_cron::{CronJob, CronServer};

#[cfg(feature = "health")]
use rushwind_health::Health;
#[cfg(feature = "health")]
use rushwind_http::mount_health;
#[cfg(feature = "metrics")]
use rushwind_http::mount_metrics;
#[cfg(feature = "metrics")]
use rushwind_metrics_prometheus::PrometheusMetrics;
#[cfg(feature = "trace")]
use rushwind_tracer::{SdkTracerProvider, TracerProviderBuilder};

mod config;
mod wire;

pub use config::{
    AppConfig, AuthzRef, BootstrapConfig, EngineConfig, RegistryConfig, RoutePackRef, ScriptConfig,
    ServerConfig, StorageConfig, StorageEndpointConfig,
};
use wire::*;
pub use wire::{BindWire, DurationWire};

/// Future type used by registered factories.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

impl std::fmt::Debug for Bootstrapped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bootstrapped").finish_non_exhaustive()
    }
}

/// Everything an assembled application exposes after [`Bootstrap::build`].
pub struct Bootstrapped {
    /// The assembled application: configured servers attached, ready for
    /// [`App::run`].
    pub app: App,
    /// The configured storage engine, if the document declared one.
    pub repository: Option<Arc<dyn Repository>>,
    /// The endpoint of every configured server, in configuration order.
    pub endpoints: Vec<String>,
    /// The discovery half of the configured registry backend, when one
    /// is configured and provides it. Consumer machinery — clients,
    /// admin consoles — reads service views through it.
    pub discovery: Option<Arc<dyn Discovery>>,
    /// The handle keeping the startup announcement alive, when one was
    /// made. Dropping it begins the backend's best-effort eventual
    /// removal; the matching deregistration runs as a before-shutdown
    /// hook regardless.
    pub registration: Option<RegistrationHandle>,
    /// The assembled authn instances, keyed by their configured
    /// instance names.
    pub authn: HashMap<String, Arc<dyn Authenticator>>,
    /// The assembled authz engines, keyed by their configured instance
    /// names.
    pub authz: HashMap<String, Arc<dyn AuthzEngine>>,
    /// The assembled brokers, keyed by their configured instance names.
    pub brokers: HashMap<String, Arc<dyn Broker>>,
    /// The assembled caches, keyed by their configured instance names.
    pub caches: HashMap<String, Arc<dyn Cache>>,
    /// The assembled circuit breakers, keyed by their configured
    /// instance names.
    pub circuitbreakers: HashMap<String, Arc<dyn CircuitBreaker>>,
    /// The assembled rate limiters, keyed by their configured instance
    /// names.
    pub limiters: HashMap<String, Arc<dyn Limiter>>,
    /// The assembled AI chat model, when the document declared one.
    pub ai: Option<Arc<dyn ChatModel>>,
    /// The assembled object-storage engine, when the document declared
    /// one.
    pub object_storage: Option<Arc<dyn ObjectStorage>>,
    /// The assembled config source — a lone source or the priority
    /// fallback over the configured list, when the document declared
    /// any.
    pub config: Option<SharedSource>,
    /// The assembled metrics engine, when the document declared one.
    pub metrics: Option<Arc<dyn Metrics>>,
    /// The script engine manager holding the assembled, initialized
    /// script engines. A shutdown sweep closes and clears it.
    pub scripts: Option<Arc<ScriptManager>>,
    /// The assembled health aggregator, when the document declared one
    /// (feature `health`). The application registers its checkers on
    /// it.
    #[cfg(feature = "health")]
    pub health: Option<Arc<Health>>,
    /// The assembled OTLP tracer provider, when the document declared
    /// one (feature `trace`). The provider is an owned value the
    /// application hands to the layers that need tracing.
    #[cfg(feature = "trace")]
    pub tracer: Option<SdkTracerProvider>,
}

/// What a registry factory yields: the two halves of a configured
/// backend.
pub struct RegistryEndpoint {
    /// The registration half, when the backend provides one.
    pub registrar: Option<Arc<dyn Registrar>>,
    /// The discovery half, when the backend provides one.
    pub discovery: Option<Arc<dyn Discovery>>,
}

/// What a route pack yields: the pack's router plus any
/// session-shutdown buses its sessions drain on. Packs without session
/// surfaces construct this with [`RouteSurface::new`]; the ws family's
/// `WsRoute::build` yields its bus alongside the mountable route, and
/// the pack forwards both so the assembler can register the bus with
/// the server — a server shutdown then relays into live sessions.
pub struct RouteSurface {
    /// The pack's router.
    pub router: Router,
    /// Session-shutdown buses the assembler registers with the server.
    pub aux: Vec<StopSignal>,
}

impl RouteSurface {
    /// A plain router surface — no session buses.
    pub fn new(router: Router) -> Self {
        Self {
            router,
            aux: Vec::new(),
        }
    }

    /// Adds one session-shutdown bus to the surface.
    pub fn with_aux_shutdown(mut self, bus: StopSignal) -> Self {
        self.aux.push(bus);
        self
    }
}

/// What a route pack or server factory receives when building: every
/// engine the document assembled, keyed the way [`Bootstrapped`]
/// exposes them.
#[derive(Clone)]
pub struct RouteInput {
    /// The configured storage engine, if any. Shared with every pack
    /// and server on this bootstrap.
    pub repository: Option<Arc<dyn Repository>>,
    /// The assembled authn instances, if any.
    pub authn: HashMap<String, Arc<dyn Authenticator>>,
    /// The assembled authz engines, if any.
    pub authz: HashMap<String, Arc<dyn AuthzEngine>>,
    /// The assembled brokers, if any.
    pub brokers: HashMap<String, Arc<dyn Broker>>,
    /// The assembled caches, if any.
    pub caches: HashMap<String, Arc<dyn Cache>>,
    /// The assembled circuit breakers, if any.
    pub circuitbreakers: HashMap<String, Arc<dyn CircuitBreaker>>,
    /// The assembled rate limiters, if any.
    pub limiters: HashMap<String, Arc<dyn Limiter>>,
    /// The assembled AI chat model, if any.
    pub ai: Option<Arc<dyn ChatModel>>,
    /// The assembled object-storage engine, if any.
    pub object_storage: Option<Arc<dyn ObjectStorage>>,
    /// The assembled config source, if any.
    pub config: Option<SharedSource>,
    /// The assembled metrics engine, if any.
    pub metrics: Option<Arc<dyn Metrics>>,
    /// The script engine manager, if any script instances were
    /// configured.
    pub scripts: Option<Arc<ScriptManager>>,
}

/// Errors surfaced by assembly.
#[derive(Debug)]
#[non_exhaustive]
pub enum BootstrapError {
    /// The YAML document could not be parsed.
    Config(String),
    /// A `servers[].kind` with no registered factory.
    UnknownServerKind(String),
    /// A `route_packs[].name` with no registered pack.
    UnknownRoutePack(String),
    /// A `storage.engine` with no registered factory.
    UnknownStorageEngine(String),
    /// A `registry.engine` with no registered factory.
    UnknownRegistryEngine(String),
    /// A `storage_endpoints[].api` with no registered pack.
    UnknownApiPack(String),
    /// A configured engine, instance, or job name with no registered
    /// factory or no assembled instance.
    UnknownEngine {
        /// The family the name was looked up in.
        domain: String,
        /// The unknown name.
        name: String,
    },
    /// `storage_endpoints` declared without a `storage` section.
    StorageEndpointWithoutStorage,
    /// A transport server failed to construct (e.g. bind refused).
    Server(rushwind_transport::ServerError),
    /// A factory failed.
    Failed(String),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "bootstrap config: {msg}"),
            Self::UnknownServerKind(kind) => write!(f, "unknown server kind: {kind}"),
            Self::UnknownRoutePack(name) => write!(f, "unknown route pack: {name}"),
            Self::UnknownStorageEngine(engine) => {
                write!(f, "unknown storage engine: {engine}")
            }
            Self::UnknownRegistryEngine(engine) => {
                write!(f, "unknown registry engine: {engine}")
            }
            Self::UnknownApiPack(name) => write!(f, "unknown storage api pack: {name}"),
            Self::UnknownEngine { domain, name } => write!(f, "unknown {domain}: {name}"),
            Self::StorageEndpointWithoutStorage => {
                write!(f, "storage_endpoints requires a storage section")
            }
            Self::Server(e) => write!(f, "server construction failed: {e}"),
            Self::Failed(msg) => write!(f, "bootstrap failed: {msg}"),
        }
    }
}

impl std::error::Error for BootstrapError {}

impl From<rushwind_transport::ServerError> for BootstrapError {
    fn from(e: rushwind_transport::ServerError) -> Self {
        Self::Server(e)
    }
}

/// The route-pack closure type: the pack's verbatim settings node plus
/// the shared assembly input.
type RoutePackFn = Box<
    dyn Fn(serde_json::Value, RouteInput) -> Result<RouteSurface, BootstrapError> + Send + Sync,
>;

/// The storage-factory closure type.
type StorageFactoryFn = Box<
    dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<Arc<dyn Repository>, BootstrapError>>
        + Send
        + Sync,
>;

/// The registry-factory closure type: engine-specific settings into a
/// [`RegistryEndpoint`].
type RegistryFactoryFn = Box<
    dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<RegistryEndpoint, BootstrapError>>
        + Send
        + Sync,
>;

/// The api-pack closure type: one storage HTTP edge, mounted under a
/// prefix. The built-in `"crud"` pack (backed by `rushwind-storage-axum`)
/// ships with the bootstrap; application packs register alongside it.
type ApiPackFn = Box<dyn Fn(RouteInput) -> Result<Router, BootstrapError> + Send + Sync>;

/// The server-factory closure type, for kinds beyond the built-in
/// `http` and `cron` (ws, quic, webtransport, h3, mqtt glue registers
/// here).
type ServerFactoryFn = Box<
    dyn Fn(
            serde_json::Value,
            RouteInput,
        )
            -> BoxFuture<'static, Result<Arc<dyn rushwind_transport::Server>, BootstrapError>>
        + Send
        + Sync,
>;

/// Emits one engine-family factory closure type — the boxed
/// settings-to-engine constructor every family shares.
macro_rules! factory_fn_type {
    ($alias:ident, $object:ty) => {
        type $alias = Box<
            dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<Arc<$object>, BootstrapError>>
                + Send
                + Sync,
        >;
    };
}

factory_fn_type!(AuthnFactoryFn, dyn Authenticator);
factory_fn_type!(AuthzFactoryFn, dyn AuthzEngine);
factory_fn_type!(BrokerFactoryFn, dyn Broker);
factory_fn_type!(CacheFactoryFn, dyn Cache);
factory_fn_type!(CircuitBreakerFactoryFn, dyn CircuitBreaker);
factory_fn_type!(LimiterFactoryFn, dyn Limiter);
factory_fn_type!(MetricsFactoryFn, dyn Metrics);
factory_fn_type!(AiFactoryFn, dyn ChatModel);
factory_fn_type!(OssFactoryFn, dyn ObjectStorage);
factory_fn_type!(ConfigSourceFactoryFn, dyn Source);

/// Config-driven application assembler. See the crate docs.
pub struct Bootstrap {
    config: BootstrapConfig,
    route_packs: HashMap<String, RoutePackFn>,
    storage_factories: HashMap<String, StorageFactoryFn>,
    registry_factories: HashMap<String, RegistryFactoryFn>,
    server_factories: HashMap<String, ServerFactoryFn>,
    api_packs: HashMap<String, ApiPackFn>,
    cron_jobs: HashMap<String, CronJob>,
    authn_factories: HashMap<String, AuthnFactoryFn>,
    authz_factories: HashMap<String, AuthzFactoryFn>,
    broker_factories: HashMap<String, BrokerFactoryFn>,
    cache_factories: HashMap<String, CacheFactoryFn>,
    circuitbreaker_factories: HashMap<String, CircuitBreakerFactoryFn>,
    limiter_factories: HashMap<String, LimiterFactoryFn>,
    metrics_factories: HashMap<String, MetricsFactoryFn>,
    ai_factories: HashMap<String, AiFactoryFn>,
    oss_factories: HashMap<String, OssFactoryFn>,
    config_source_factories: HashMap<String, ConfigSourceFactoryFn>,
}

impl std::fmt::Debug for Bootstrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bootstrap").finish_non_exhaustive()
    }
}

/// Emits one engine-family factory registration method.
macro_rules! factory_builder {
    ($method:ident, $field:ident, $object:ty, $doc:literal) => {
        #[doc = $doc]
        pub fn $method<F, Fut>(mut self, name: impl Into<String>, factory: F) -> Self
        where
            F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<Arc<$object>, BootstrapError>> + Send + 'static,
        {
            self.$field.insert(
                name.into(),
                Box::new(move |settings| Box::pin(factory(settings))),
            );
            self
        }
    };
}

impl Bootstrap {
    /// Parses a YAML document.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, BootstrapError> {
        let config = serde_yaml::from_str(yaml)
            .map_err(|e| BootstrapError::Config(format!("YAML parse: {e}")))?;
        Ok(Self::from_config(config))
    }

    /// Reads and parses a YAML file.
    pub fn from_yaml_path(path: impl AsRef<std::path::Path>) -> Result<Self, BootstrapError> {
        let yaml = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            BootstrapError::Config(format!("read {}: {e}", path.as_ref().display()))
        })?;
        Self::from_yaml_str(&yaml)
    }

    /// Wraps an already-parsed configuration.
    pub fn from_config(config: BootstrapConfig) -> Self {
        let mut api_packs: HashMap<String, ApiPackFn> = HashMap::new();
        api_packs.insert("crud".to_string(), Box::new(crud_api_pack));
        Self {
            config,
            route_packs: HashMap::new(),
            storage_factories: HashMap::new(),
            registry_factories: HashMap::new(),
            server_factories: HashMap::new(),
            api_packs,
            cron_jobs: HashMap::new(),
            authn_factories: HashMap::new(),
            authz_factories: HashMap::new(),
            broker_factories: HashMap::new(),
            cache_factories: HashMap::new(),
            circuitbreaker_factories: HashMap::new(),
            limiter_factories: HashMap::new(),
            metrics_factories: HashMap::new(),
            ai_factories: HashMap::new(),
            oss_factories: HashMap::new(),
            config_source_factories: HashMap::new(),
        }
    }

    /// Registers a named storage-endpoint api pack, mountable from
    /// `storage_endpoints[].api`. The built-in `"crud"` pack serves the
    /// storage line's HTTP edge; application packs override it by
    /// registering the same name.
    pub fn api_pack<F>(mut self, name: impl Into<String>, pack: F) -> Self
    where
        F: Fn(RouteInput) -> Result<Router, BootstrapError> + Send + Sync + 'static,
    {
        self.api_packs.insert(name.into(), Box::new(pack));
        self
    }

    /// Registers a named route pack. The closure receives the pack's
    /// settings node from `route_packs[].settings` verbatim, alongside
    /// the shared assembly input.
    pub fn route_pack<F>(mut self, name: impl Into<String>, pack: F) -> Self
    where
        F: Fn(serde_json::Value, RouteInput) -> Result<RouteSurface, BootstrapError>
            + Send
            + Sync
            + 'static,
    {
        self.route_packs.insert(name.into(), Box::new(pack));
        self
    }

    /// Registers a named storage factory. The schema is application
    /// knowledge: capture it in the closure, read engine knobs from
    /// `settings`.
    pub fn storage_factory<F, Fut>(mut self, name: impl Into<String>, factory: F) -> Self
    where
        F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<dyn Repository>, BootstrapError>> + Send + 'static,
    {
        self.storage_factories.insert(
            name.into(),
            Box::new(move |settings| Box::pin(factory(settings))),
        );
        self
    }

    /// Registers a named registry factory, mountable from
    /// `registry.engine`. The closure turns engine-specific settings
    /// into the backend's registration and discovery halves.
    pub fn registry_factory<F, Fut>(mut self, name: impl Into<String>, factory: F) -> Self
    where
        F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RegistryEndpoint, BootstrapError>> + Send + 'static,
    {
        self.registry_factories.insert(
            name.into(),
            Box::new(move |settings| Box::pin(factory(settings))),
        );
        self
    }

    /// Registers a named server factory for a kind beyond the built-in
    /// `http` and `cron` (ws, quic, webtransport, h3, mqtt glue
    /// registers here).
    pub fn server_factory<F, Fut>(mut self, kind: impl Into<String>, factory: F) -> Self
    where
        F: Fn(serde_json::Value, RouteInput) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<dyn rushwind_transport::Server>, BootstrapError>>
            + Send
            + 'static,
    {
        self.server_factories.insert(
            kind.into(),
            Box::new(move |settings, input| Box::pin(factory(settings, input))),
        );
        self
    }

    /// Registers a named cron job, mountable from a `cron` server's
    /// `jobs` list. The handler is application code; the name is what
    /// configuration mounts.
    pub fn cron_job(mut self, name: impl Into<String>, job: CronJob) -> Self {
        self.cron_jobs.insert(name.into(), job);
        self
    }

    factory_builder!(
        authn_factory,
        authn_factories,
        dyn Authenticator,
        "Registers a named authn factory, mountable from `authn.<instance>.engine`. The closure turns engine-specific settings into the authenticator; the assembled instance is referenceable from `route_packs[].authn` and `storage_endpoints[].authn`."
    );
    factory_builder!(
        authz_factory,
        authz_factories,
        dyn AuthzEngine,
        "Registers a named authz factory, mountable from `authz.<instance>.engine`. The closure turns engine-specific settings into the engine; the assembled instance is referenceable from the `authz.engine` field of a permission point."
    );
    factory_builder!(
        broker_factory,
        broker_factories,
        dyn Broker,
        "Registers a named broker factory, mountable from `brokers.<instance>.engine`. The closure turns engine-specific settings into the broker."
    );
    factory_builder!(
        cache_factory,
        cache_factories,
        dyn Cache,
        "Registers a named cache factory, mountable from `caches.<instance>.engine`. The closure turns engine-specific settings into the cache."
    );
    factory_builder!(
        circuitbreaker_factory,
        circuitbreaker_factories,
        dyn CircuitBreaker,
        "Registers a named circuit-breaker factory, mountable from `circuitbreakers.<instance>.engine`. The closure turns engine-specific settings into the breaker."
    );
    factory_builder!(
        limiter_factory,
        limiter_factories,
        dyn Limiter,
        "Registers a named rate-limiter factory, mountable from `limiters.<instance>.engine`. The closure turns engine-specific settings into the limiter."
    );
    factory_builder!(
        metrics_factory,
        metrics_factories,
        dyn Metrics,
        "Registers a named metrics factory, mountable from `metrics.engine`. The closure turns engine-specific settings into the metrics engine. The built-in Prometheus engine (feature `metrics`) serves when no factory is registered under its name."
    );
    factory_builder!(
        ai_factory,
        ai_factories,
        dyn ChatModel,
        "Registers a named AI chat-model factory, mountable from `ai.engine`. The closure turns engine-specific settings into the chat model."
    );
    factory_builder!(
        oss_factory,
        oss_factories,
        dyn ObjectStorage,
        "Registers a named object-storage factory, mountable from `oss.engine`. The closure turns engine-specific settings into the object-storage engine."
    );
    factory_builder!(
        config_source_factory,
        config_source_factories,
        dyn Source,
        "Registers a named config-source factory, mountable from `config_sources[].engine`. The closure turns engine-specific settings into the source; the assembled list composes into the config domain's priority fallback."
    );

    /// Assembles the application: the engines first (route packs and
    /// server factories share them through the [`RouteInput`]), then
    /// every configured server, then the [`App`] identity and lifecycle
    /// settings.
    pub async fn build(self) -> Result<Bootstrapped, BootstrapError> {
        let mut builder = App::builder();
        if let Some(name) = &self.config.app.name {
            builder = builder.name(name.clone());
        }
        if let Some(version) = &self.config.app.version {
            builder = builder.version(version.clone());
        }
        if let Some(secs) = self.config.app.stop_timeout_secs {
            builder = builder.stop_timeout(Duration::from_secs(secs));
        }

        // Storage first: route packs may share the engine.
        let repository = match &self.config.storage {
            Some(storage) => {
                let factory = self
                    .storage_factories
                    .get(&storage.engine)
                    .ok_or_else(|| BootstrapError::UnknownStorageEngine(storage.engine.clone()))?;
                Some(factory(storage.settings.clone()).await?)
            }
            None => None,
        };

        // The named engine families: every configured instance resolved
        // through its factory and keyed by its instance name.
        macro_rules! assemble_named {
            ($target:ident, $config:ident, $factories:ident, $object:ty, $domain:literal) => {
                let mut $target: HashMap<String, Arc<$object>> = HashMap::new();
                for (instance, engine) in &self.config.$config {
                    let factory = self.$factories.get(&engine.engine).ok_or_else(|| {
                        BootstrapError::UnknownEngine {
                            domain: $domain.to_string(),
                            name: engine.engine.clone(),
                        }
                    })?;
                    $target.insert(instance.clone(), factory(engine.settings.clone()).await?);
                }
            };
        }
        assemble_named!(
            authn,
            authn,
            authn_factories,
            dyn Authenticator,
            "authn engine"
        );
        assemble_named!(
            authz,
            authz,
            authz_factories,
            dyn AuthzEngine,
            "authz engine"
        );
        assemble_named!(
            brokers,
            brokers,
            broker_factories,
            dyn Broker,
            "broker engine"
        );
        assemble_named!(caches, caches, cache_factories, dyn Cache, "cache engine");
        assemble_named!(
            circuitbreakers,
            circuitbreakers,
            circuitbreaker_factories,
            dyn CircuitBreaker,
            "circuitbreaker engine"
        );
        assemble_named!(
            limiters,
            limiters,
            limiter_factories,
            dyn Limiter,
            "limiter engine"
        );

        // The AI chat model: a single instance.
        let ai = match &self.config.ai {
            Some(engine) => {
                let factory = self.ai_factories.get(&engine.engine).ok_or_else(|| {
                    BootstrapError::UnknownEngine {
                        domain: "ai engine".to_string(),
                        name: engine.engine.clone(),
                    }
                })?;
                Some(factory(engine.settings.clone()).await?)
            }
            None => None,
        };

        // The object-storage engine: a single instance.
        let object_storage = match &self.config.oss {
            Some(engine) => {
                let factory = self.oss_factories.get(&engine.engine).ok_or_else(|| {
                    BootstrapError::UnknownEngine {
                        domain: "oss engine".to_string(),
                        name: engine.engine.clone(),
                    }
                })?;
                Some(factory(engine.settings.clone()).await?)
            }
            None => None,
        };

        // The metrics engine: an application-registered factory, or the
        // built-in Prometheus engine under the `metrics` feature — the
        // one engine whose concrete type the scrape mount needs.
        let mut metrics: Option<Arc<dyn Metrics>> = None;
        #[cfg(feature = "metrics")]
        let mut prometheus_scrape: Option<Arc<PrometheusMetrics>> = None;
        if let Some(engine) = &self.config.metrics {
            match self.metrics_factories.get(&engine.engine) {
                Some(factory) => {
                    metrics = Some(factory(engine.settings.clone()).await?);
                }
                None => {
                    if engine.engine == "prometheus" {
                        #[cfg(feature = "metrics")]
                        {
                            let provider =
                                PrometheusMetrics::from_settings(engine.settings.clone()).map_err(
                                    |e| BootstrapError::Config(format!("prometheus settings: {e}")),
                                )?;
                            let provider = Arc::new(provider);
                            prometheus_scrape = Some(Arc::clone(&provider));
                            let erased: Arc<dyn Metrics> = provider;
                            metrics = Some(erased);
                        }
                        #[cfg(not(feature = "metrics"))]
                        {
                            return Err(BootstrapError::UnknownEngine {
                                domain: "metrics engine".to_string(),
                                name: engine.engine.clone(),
                            });
                        }
                    } else {
                        return Err(BootstrapError::UnknownEngine {
                            domain: "metrics engine".to_string(),
                            name: engine.engine.clone(),
                        });
                    }
                }
            }
        }

        // Config sources: a lone source stays itself; a list composes
        // into the config domain's priority fallback.
        let mut config: Option<SharedSource> = None;
        if !self.config.config_sources.is_empty() {
            let mut sources: Vec<SharedSource> = Vec::new();
            for engine in &self.config.config_sources {
                let factory = self
                    .config_source_factories
                    .get(&engine.engine)
                    .ok_or_else(|| BootstrapError::UnknownEngine {
                        domain: "config source engine".to_string(),
                        name: engine.engine.clone(),
                    })?;
                sources.push(factory(engine.settings.clone()).await?);
            }
            config = if sources.len() == 1 {
                Some(sources.swap_remove(0))
            } else {
                let fallback = FallbackSource::new(sources)
                    .map_err(|e| BootstrapError::Failed(format!("config fallback: {e}")))?;
                let erased: SharedSource = Arc::new(fallback);
                Some(erased)
            };
        }

        // Script engines: built and initialized through the script
        // domain's own factory registry and held in a name-keyed
        // manager. A shutdown sweep closes and clears the manager.
        let mut scripts: Option<Arc<ScriptManager>> = None;
        if !self.config.scripts.is_empty() {
            let manager = Arc::new(ScriptManager::new());
            for (name, script) in &self.config.scripts {
                let engine = rushwind_script::new_script_engine(&script.engine).map_err(|_| {
                    BootstrapError::UnknownEngine {
                        domain: "script engine".to_string(),
                        name: script.engine.clone(),
                    }
                })?;
                engine
                    .init()
                    .await
                    .map_err(|e| BootstrapError::Failed(format!("script init {name}: {e}")))?;
                manager
                    .register(name, engine)
                    .map_err(|e| BootstrapError::Failed(format!("script register {name}: {e}")))?;
            }
            let hook_manager = Arc::clone(&manager);
            builder = builder.before_stop(move |_budget| {
                let manager = Arc::clone(&hook_manager);
                async move {
                    let _ = manager.close_all();
                    Ok::<(), ServerError>(())
                }
            });
            scripts = Some(manager);
        }

        // The health aggregator (feature `health`).
        #[cfg(feature = "health")]
        let health = match &self.config.health {
            Some(settings) => Some(Arc::new(
                Health::from_settings(settings.clone())
                    .map_err(|e| BootstrapError::Config(format!("health settings: {e}")))?,
            )),
            None => None,
        };
        #[cfg(not(feature = "health"))]
        if self.config.health.is_some() {
            return Err(BootstrapError::Failed(
                "the health section requires building rushwind-bootstrap with feature 'health'"
                    .to_string(),
            ));
        }

        // The OTLP tracer provider (feature `trace`). An owned value the
        // application hands to the layers that need tracing.
        #[cfg(feature = "trace")]
        let tracer = match &self.config.tracer {
            Some(settings) => {
                let builder = TracerProviderBuilder::from_settings(settings.clone())
                    .map_err(|e| BootstrapError::Config(format!("tracer settings: {e}")))?;
                let provider = builder
                    .build()
                    .map_err(|e| BootstrapError::Failed(format!("tracer provider build: {e}")))?;
                Some(provider)
            }
            None => None,
        };
        #[cfg(not(feature = "trace"))]
        if self.config.tracer.is_some() {
            return Err(BootstrapError::Failed(
                "the tracer section requires building rushwind-bootstrap with feature 'trace'"
                    .to_string(),
            ));
        }

        let input = RouteInput {
            repository: repository.clone(),
            authn: authn.clone(),
            authz: authz.clone(),
            brokers: brokers.clone(),
            caches: caches.clone(),
            circuitbreakers: circuitbreakers.clone(),
            limiters: limiters.clone(),
            ai: ai.clone(),
            object_storage: object_storage.clone(),
            config: config.clone(),
            metrics: metrics.clone(),
            scripts: scripts.clone(),
        };

        // Storage endpoints: HTTP edges mounted over the configured
        // storage. They require a storage section (there is nothing to
        // mount otherwise) and merge into every http server's router.
        let mut storage_routers: Vec<(String, Router)> = Vec::new();
        for endpoint_config in &self.config.storage_endpoints {
            if repository.is_none() {
                return Err(BootstrapError::StorageEndpointWithoutStorage);
            }
            let pack = self
                .api_packs
                .get(&endpoint_config.api)
                .ok_or_else(|| BootstrapError::UnknownApiPack(endpoint_config.api.clone()))?;
            let mut edge_router = pack(input.clone())?;
            edge_router = apply_guards(
                edge_router,
                &input,
                &endpoint_config.authn,
                &endpoint_config.authz,
            )?;
            storage_routers.push((endpoint_config.nest.clone(), edge_router));
        }

        let mut endpoints = Vec::new();
        let mut servers = Vec::new();
        for server_config in &self.config.servers {
            // The built-in `http` and `cron` kinds assemble here, where
            // the pack and job registries are in scope; every other
            // kind goes through the server-factory registry.
            let (server, endpoint) = match server_config.kind.as_str() {
                "http" => {
                    let http: HttpServerConfig =
                        serde_json::from_value(server_config.settings.clone()).map_err(|e| {
                            BootstrapError::Config(format!("http server settings: {e}"))
                        })?;
                    let mut router = Router::new();
                    let mut aux: Vec<StopSignal> = Vec::new();
                    for pack_ref in &http.route_packs {
                        let pack = self.route_packs.get(&pack_ref.name).ok_or_else(|| {
                            BootstrapError::UnknownRoutePack(pack_ref.name.clone())
                        })?;
                        let mut surface = pack(pack_ref.settings.clone(), input.clone())?;
                        surface.router =
                            apply_guards(surface.router, &input, &pack_ref.authn, &pack_ref.authz)?;
                        router = router.merge(surface.router);
                        aux.extend(surface.aux);
                    }
                    for (prefix, edge_router) in &storage_routers {
                        router = router.nest(prefix, edge_router.clone());
                    }
                    // The domain mounts, each behind its feature.
                    if http.mounts.health {
                        #[cfg(feature = "health")]
                        {
                            let health = health.as_ref().ok_or_else(|| {
                                BootstrapError::Failed(
                                    "the health mount requires a health section".to_string(),
                                )
                            })?;
                            router = mount_health(router, Arc::clone(health));
                        }
                        #[cfg(not(feature = "health"))]
                        {
                            return Err(BootstrapError::Failed(
                                "the health mount requires building rushwind-bootstrap with feature 'health'"
                                    .to_string(),
                            ));
                        }
                    }
                    if http.mounts.metrics {
                        #[cfg(feature = "metrics")]
                        {
                            let scrape = prometheus_scrape.as_ref().ok_or_else(|| {
                                BootstrapError::Failed(
                                    "the metrics mount requires the built-in prometheus metrics engine"
                                        .to_string(),
                                )
                            })?;
                            router = mount_metrics(router, Arc::clone(scrape));
                        }
                        #[cfg(not(feature = "metrics"))]
                        {
                            return Err(BootstrapError::Failed(
                                "the metrics mount requires building rushwind-bootstrap with feature 'metrics'"
                                    .to_string(),
                            ));
                        }
                    }
                    // The edge stack: the configured toggles over the
                    // defaults, then the wrap.
                    let mut edge = HttpEdge::new();
                    if !http.edge.request_id {
                        edge = edge.without_request_id();
                    }
                    if !http.edge.logging {
                        edge = edge.without_logging();
                    }
                    if !http.edge.recovery {
                        edge = edge.without_recovery();
                    }
                    if let Some(timeout) = &http.edge.timeout {
                        edge = edge.with_timeout(timeout.0);
                    }
                    if let Some(cors) = &http.edge.cors {
                        let options = cors_options_from(cors);
                        edge = if cors.compat {
                            edge.with_cors_compat(options)
                        } else {
                            edge.with_cors(options)
                        };
                    }
                    router = edge.wrap(router);
                    let mut server = AxumServer::new(http.bind.0, router)?;
                    for bus in aux {
                        server = server.with_aux_shutdown(bus);
                    }
                    let endpoint = server.endpoint()?;
                    (
                        Arc::new(server) as Arc<dyn rushwind_transport::Server>,
                        endpoint,
                    )
                }
                "cron" => {
                    let cron: CronServerConfig =
                        serde_json::from_value(server_config.settings.clone()).map_err(|e| {
                            BootstrapError::Config(format!("cron server settings: {e}"))
                        })?;
                    let mut server = CronServer::new(cron.endpoint.clone());
                    for job_name in &cron.jobs {
                        let job = self.cron_jobs.get(job_name).ok_or_else(|| {
                            BootstrapError::UnknownEngine {
                                domain: "cron job".to_string(),
                                name: job_name.clone(),
                            }
                        })?;
                        // One mount per server: the spec clones, the
                        // handler is a shared arc.
                        server = server.with_job(CronJob {
                            name: job.name.clone(),
                            spec: job.spec.clone(),
                            handler: Arc::clone(&job.handler),
                        });
                    }
                    let endpoint = server.endpoint()?;
                    (
                        Arc::new(server) as Arc<dyn rushwind_transport::Server>,
                        endpoint,
                    )
                }
                kind => {
                    let factory = self
                        .server_factories
                        .get(kind)
                        .ok_or_else(|| BootstrapError::UnknownServerKind(kind.to_string()))?;
                    let server = factory(server_config.settings.clone(), input.clone()).await?;
                    let endpoint = server.endpoint()?;
                    (server, endpoint)
                }
            };
            endpoints.push(endpoint);
            servers.push(server);
        }

        // The registry backend, when configured: its two halves come
        // from the registered factory. With the registration half, a
        // complete identity (name and version), and at least one
        // assembled endpoint, the application announces itself; the
        // announcement's deregistration is wired as a before-shutdown
        // hook. Anything missing leaves the application
        // unannounced.
        let mut discovery: Option<Arc<dyn Discovery>> = None;
        let mut registration: Option<RegistrationHandle> = None;
        if let Some(registry_config) = &self.config.registry {
            let factory = self
                .registry_factories
                .get(&registry_config.engine)
                .ok_or_else(|| {
                    BootstrapError::UnknownRegistryEngine(registry_config.engine.clone())
                })?;
            let endpoint = factory(registry_config.settings.clone()).await?;
            if let (Some(name), Some(version)) = (
                self.config.app.name.clone(),
                self.config.app.version.clone(),
            ) {
                if let (Some(registrar), false) = (endpoint.registrar, endpoints.is_empty()) {
                    let instance = Instance {
                        id: random_instance_id(),
                        name,
                        version,
                        endpoints: endpoints.clone(),
                    };
                    let registration_record = Registration::new(instance);
                    let handle = registrar
                        .register(registration_record.clone())
                        .await
                        .map_err(|e| BootstrapError::Failed(format!("registry register: {e}")))?;
                    let hook_registrar = registrar;
                    let hook_record = registration_record;
                    builder = builder.before_stop(move |_budget| {
                        let registrar = Arc::clone(&hook_registrar);
                        let record = hook_record.clone();
                        async move {
                            let _ = registrar.deregister(record).await;
                            Ok::<(), ServerError>(())
                        }
                    });
                    registration = Some(handle);
                }
            }
            discovery = endpoint.discovery;
        }

        for server in servers {
            builder = builder.erased_server(server);
        }

        Ok(Bootstrapped {
            app: builder.build(),
            repository,
            endpoints,
            discovery,
            registration,
            authn,
            authz,
            brokers,
            caches,
            circuitbreakers,
            limiters,
            ai,
            object_storage,
            config,
            metrics,
            scripts,
            #[cfg(feature = "health")]
            health,
            #[cfg(feature = "trace")]
            tracer,
        })
    }
}

/// A fresh random instance identifier — 16 CSPRNG bytes, hex-formatted.
/// A degenerate all-zero id is the documented fallback when the OS
/// entropy source fails.
fn random_instance_id() -> String {
    let mut bytes = [0u8; 16];
    let _ = getrandom::fill(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The built-in `"crud"` api pack: the storage line's HTTP edge
/// (`rushwind-storage-axum`'s [`CrudApi`]) bound to the configured
/// repository. Expects the schema's column fields as JSON in and out.
fn crud_api_pack(input: RouteInput) -> Result<Router, BootstrapError> {
    let repo = input.repository.ok_or_else(|| {
        BootstrapError::Failed("crud api pack requires a configured storage".to_string())
    })?;
    Ok(CrudApi::new(repo).router())
}

/// Applies the configured per-subtree security wraps to one router: the
/// authorization bridge first, the authentication bridge second — later
/// `layer` calls wrap earlier ones, so the authenticator ends up
/// outermost and the claims it inserts reach the permission point
/// beneath it. The permission point's project axis is fixed,
/// claim-carried, or empty — the two explicit shapes are mutually
/// exclusive.
fn apply_guards(
    router: Router,
    input: &RouteInput,
    authn: &Option<String>,
    authz: &Option<AuthzRef>,
) -> Result<Router, BootstrapError> {
    let router = apply_guards_authz(router, input, authz)?;
    let Some(authn_instance) = authn else {
        return Ok(router);
    };
    let authenticator =
        input
            .authn
            .get(authn_instance)
            .ok_or_else(|| BootstrapError::UnknownEngine {
                domain: "authn instance".to_string(),
                name: authn_instance.clone(),
            })?;
    Ok(with_authn(router, Arc::clone(authenticator)))
}

/// The authorization half of [`apply_guards`].
fn apply_guards_authz(
    router: Router,
    input: &RouteInput,
    authz: &Option<AuthzRef>,
) -> Result<Router, BootstrapError> {
    let Some(reference) = authz else {
        return Ok(router);
    };
    let engine =
        input
            .authz
            .get(&reference.engine)
            .ok_or_else(|| BootstrapError::UnknownEngine {
                domain: "authz instance".to_string(),
                name: reference.engine.clone(),
            })?;
    Ok(match (&reference.project, &reference.project_claim) {
        (Some(_), Some(_)) => {
            return Err(BootstrapError::Config(
                "authz reference: project and project_claim are mutually exclusive".to_string(),
            ));
        }
        (Some(project), None) => with_authorization_for(
            router,
            Arc::clone(engine),
            reference.action.clone(),
            reference.resource.clone(),
            project.clone(),
        ),
        (None, Some(claim)) => with_authorization_claim(
            router,
            Arc::clone(engine),
            reference.action.clone(),
            reference.resource.clone(),
            claim.clone(),
        ),
        (None, None) => with_authorization(
            router,
            Arc::clone(engine),
            reference.action.clone(),
            reference.resource.clone(),
        ),
    })
}
