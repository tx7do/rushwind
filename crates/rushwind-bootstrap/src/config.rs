//! The YAML document shape — the config structs [`Bootstrap::build`](crate::Bootstrap)
//! parses. Data only: the assembly reads them, nothing here runs.

use std::collections::HashMap;

use serde::Deserialize;

/// The YAML document shape.
#[derive(Debug, Default, Deserialize)]
pub struct BootstrapConfig {
    /// Application identity and lifecycle settings.
    #[serde(default)]
    pub app: AppConfig,
    /// Storage engine selection; omit for storage-less applications.
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    /// Registry backend selection; omit for registry-less applications.
    #[serde(default)]
    pub registry: Option<RegistryConfig>,
    /// HTTP edge mounted over the configured storage, in order. Requires
    /// `storage`.
    #[serde(default)]
    pub storage_endpoints: Vec<StorageEndpointConfig>,
    /// Servers to assemble, in order.
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
    /// Authn instances to assemble, keyed by instance name. Reference
    /// the names from `route_packs[].authn` and
    /// `storage_endpoints[].authn`.
    #[serde(default)]
    pub authn: HashMap<String, EngineConfig>,
    /// Authz engines to assemble, keyed by instance name. Reference
    /// the names from `route_packs[].authz.engine` and
    /// `storage_endpoints[].authz.engine`.
    #[serde(default)]
    pub authz: HashMap<String, EngineConfig>,
    /// Brokers to assemble, keyed by instance name.
    #[serde(default)]
    pub brokers: HashMap<String, EngineConfig>,
    /// Caches to assemble, keyed by instance name.
    #[serde(default)]
    pub caches: HashMap<String, EngineConfig>,
    /// Circuit breakers to assemble, keyed by instance name.
    #[serde(default)]
    pub circuitbreakers: HashMap<String, EngineConfig>,
    /// Rate limiters to assemble, keyed by instance name.
    #[serde(default)]
    pub limiters: HashMap<String, EngineConfig>,
    /// The AI chat model selection; omit for AI-less applications.
    #[serde(default)]
    pub ai: Option<EngineConfig>,
    /// The object-storage engine selection; omit when the application
    /// stores nothing.
    #[serde(default)]
    pub oss: Option<EngineConfig>,
    /// The metrics engine selection; omit for metrics-less
    /// applications.
    #[serde(default)]
    pub metrics: Option<EngineConfig>,
    /// Config sources in priority order — the first entry has the
    /// highest priority. Two or more entries compose into the config
    /// domain's fallback source.
    #[serde(default)]
    pub config_sources: Vec<EngineConfig>,
    /// Script engine instances to assemble into the script manager,
    /// keyed by instance name. The `engine` field names a factory in
    /// the script domain's own registry — the engine crates
    /// self-register there via their `register()` functions, which the
    /// application calls once at startup.
    #[serde(default)]
    pub scripts: HashMap<String, ScriptConfig>,
    /// The health aggregator settings (feature `health`); the section
    /// fails the assembly when the feature is compiled out.
    #[serde(default)]
    pub health: Option<serde_json::Value>,
    /// The OTLP tracer provider settings (feature `trace`); the section
    /// fails the assembly when the feature is compiled out.
    #[serde(default)]
    pub tracer: Option<serde_json::Value>,
}

/// Application identity and lifecycle settings.
#[derive(Debug, Default, Deserialize)]
pub struct AppConfig {
    /// Application name (optional).
    #[serde(default)]
    pub name: Option<String>,
    /// Application version (optional).
    #[serde(default)]
    pub version: Option<String>,
    /// Per-phase shutdown budget in seconds (default: the core's 10 s).
    #[serde(default)]
    pub stop_timeout_secs: Option<u64>,
}

/// Storage engine selection.
#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    /// The registered factory name.
    pub engine: String,
    /// Engine-specific settings, passed to the factory verbatim.
    #[serde(default)]
    pub settings: serde_json::Value,
}

/// Registry backend selection.
#[derive(Debug, Deserialize)]
pub struct RegistryConfig {
    /// The registered factory name.
    pub engine: String,
    /// Engine-specific settings, passed to the factory verbatim.
    #[serde(default)]
    pub settings: serde_json::Value,
}

/// One engine-family selection: the factory name plus its verbatim
/// settings node.
#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    /// The registered factory name.
    pub engine: String,
    /// Engine-specific settings, passed to the factory verbatim.
    #[serde(default)]
    pub settings: serde_json::Value,
}

/// One script-engine instance selection. Construction takes no settings
/// — the script domain's factory registry builds engines from their
/// type name alone.
#[derive(Debug, Deserialize)]
pub struct ScriptConfig {
    /// The script engine type, naming a factory in the script domain's
    /// own registry.
    pub engine: String,
}

/// One route-pack mount reference: the pack name, the pack's verbatim
/// settings node, and the optional security wraps applied to the pack's
/// whole router.
#[derive(Debug, Deserialize)]
pub struct RoutePackRef {
    /// The registered route-pack name.
    pub name: String,
    /// The assembled authn instance wrapping this pack, if any.
    #[serde(default)]
    pub authn: Option<String>,
    /// The permission point wrapping this pack, if any.
    #[serde(default)]
    pub authz: Option<AuthzRef>,
    /// Pack-specific settings, passed verbatim to the pack's closure.
    #[serde(default)]
    pub settings: serde_json::Value,
}

/// One permission point: the assembled authz instance plus the fixed
/// action/resource axes, and a project axis that is either a fixed
/// string or the name of a credential claim carrying it. The two
/// project shapes are mutually exclusive; absent both, the permission
/// point evaluates under the empty project.
#[derive(Debug, Deserialize)]
pub struct AuthzRef {
    /// The assembled authz instance name.
    pub engine: String,
    /// The action axis.
    pub action: String,
    /// The resource axis.
    pub resource: String,
    /// A fixed project axis.
    #[serde(default)]
    pub project: Option<String>,
    /// A named credential claim carrying the project axis.
    #[serde(default)]
    pub project_claim: Option<String>,
}

/// One HTTP edge mounted over the configured storage.
#[derive(Debug, Deserialize)]
pub struct StorageEndpointConfig {
    /// The path prefix the api router nests under (e.g. `/widgets`).
    pub nest: String,
    /// The registered api pack name (`"crud"` is built in).
    pub api: String,
    /// Pack-specific settings, passed verbatim.
    #[serde(default)]
    pub settings: serde_json::Value,
    /// The assembled authn instance wrapping this edge, if any.
    #[serde(default)]
    pub authn: Option<String>,
    /// The permission point wrapping this edge, if any.
    #[serde(default)]
    pub authz: Option<AuthzRef>,
}

/// One server to assemble.
#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    /// The registered factory name (`http` and `cron` are built in).
    pub kind: String,
    /// Factory-specific settings, passed verbatim.
    #[serde(flatten)]
    pub settings: serde_json::Value,
}
