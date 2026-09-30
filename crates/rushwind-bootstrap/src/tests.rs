//! The bootstrap assembly tests, verbatim from the pre-split
//! lib.rs inline module (private access preserved: a root child
//! still sees the crate root's items).

use super::*;
use crate::wire::*;

use rushwind_ai::AiError;
use rushwind_authn::{AuthClaims, AuthnError};
use rushwind_authz::{
    Action, AuthzError, Pairs, PolicyMap, Project, Projects, Resource, RoleMap, Subject,
    Subjects,
};
use rushwind_registry::{BoxFuture, RegistryError, Watcher};
use rushwind_transport::Instance;
use rushwind_transport_cron::CronSpec;
use std::collections::HashMap;
use std::sync::Mutex;

/// A registry mock recording every registration and deregistration.
#[derive(Default)]
struct MockRegistry {
    registered: Mutex<Vec<Registration>>,
    deregistered: Mutex<Vec<Registration>>,
}

impl Registrar for MockRegistry {
    fn register<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<RegistrationHandle, RegistryError>> {
        Box::pin(async move {
            self.registered.lock().unwrap().push(registration.clone());
            Ok(RegistrationHandle::from_cancel(|| {}))
        })
    }

    fn deregister<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<(), RegistryError>> {
        Box::pin(async move {
            self.deregistered.lock().unwrap().push(registration);
            Ok(())
        })
    }
}

impl Discovery for MockRegistry {
    fn get_service<'a>(
        &'a self,
        _service_name: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn watch<'a>(
        &'a self,
        _service_name: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn Watcher>, RegistryError>> {
        Box::pin(async move { Ok(Box::new(MockWatcher { first: true }) as Box<dyn Watcher>) })
    }
}

struct MockWatcher {
    first: bool,
}

impl Watcher for MockWatcher {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            if self.first {
                self.first = false;
                return Ok(Vec::new());
            }
            Err(RegistryError::Failed("watcher stopped".to_string()))
        })
    }

    fn stop(&mut self) {}
}

const CONFIG: &str = r#"
app:
  name: demo
  version: v0.1.0
registry:
  engine: mock
  settings: {}
servers:
  - kind: http
bind: 127.0.0.1:0
"#;

const NO_IDENTITY: &str = r#"
registry:
  engine: mock
  settings: {}
servers:
  - kind: http
bind: 127.0.0.1:0
"#;

/// The registry section parses.
#[test]
fn registry_section_parses() {
    let config: BootstrapConfig = serde_yaml::from_str(CONFIG).expect("config must parse");
    assert_eq!(
        config.registry.as_ref().expect("registry section").engine,
        "mock"
    );
}

/// The announcement, the exposure, and the shutdown
/// deregistration: a configured backend receives the identity and
/// every assembled endpoint at build time, the discovery half is
/// exposed, and a full shutdown run deregisters through the
/// before-stop hook.
#[tokio::test]
async fn announcement_exposure_and_deregistration() {
    let mock = Arc::new(MockRegistry::default());
    let factory_mock = Arc::clone(&mock);
    let bootstrapped = Bootstrap::from_yaml_str(CONFIG)
        .expect("bootstrap parses")
        .registry_factory("mock", move |_settings| {
            let registrar = Arc::clone(&factory_mock) as Arc<dyn Registrar>;
            let discovery = Arc::clone(&factory_mock) as Arc<dyn Discovery>;
            async move {
                Ok(RegistryEndpoint {
                    registrar: Some(registrar),
                    discovery: Some(discovery),
                })
            }
        })
        .build()
        .await
        .expect("assembly must succeed");

    {
        let registered = mock.registered.lock().unwrap();
        assert_eq!(registered.len(), 1, "exactly one announcement");
        let instance = &registered[0].instance;
        assert_eq!(instance.name, "demo");
        assert_eq!(instance.version, "v0.1.0");
        assert_eq!(instance.endpoints, bootstrapped.endpoints);
        assert!(!instance.id.is_empty());
    }
    assert!(bootstrapped.registration.is_some());
    assert!(bootstrapped.discovery.is_some());

    bootstrapped.app.stop();
    let _ = bootstrapped
        .app
        .run(rushwind_transport::StopSignal::new())
        .await;
    assert_eq!(
        mock.deregistered.lock().unwrap().len(),
        1,
        "the shutdown hook deregisters"
    );
}

/// Without identity there is no announcement — but the discovery
/// half is still exposed.
#[tokio::test]
async fn no_identity_no_announcement() {
    let mock = Arc::new(MockRegistry::default());
    let factory_mock = Arc::clone(&mock);
    let bootstrapped = Bootstrap::from_yaml_str(NO_IDENTITY)
        .expect("bootstrap parses")
        .registry_factory("mock", move |_settings| {
            let registrar = Arc::clone(&factory_mock) as Arc<dyn Registrar>;
            let discovery = Arc::clone(&factory_mock) as Arc<dyn Discovery>;
            async move {
                Ok(RegistryEndpoint {
                    registrar: Some(registrar),
                    discovery: Some(discovery),
                })
            }
        })
        .build()
        .await
        .expect("assembly must succeed");

    assert!(mock.registered.lock().unwrap().is_empty());
    assert!(bootstrapped.registration.is_none());
    assert!(bootstrapped.discovery.is_some());
}

/// A registry section naming an unregistered engine fails the
/// assembly.
#[tokio::test]
async fn unknown_registry_engine_fails() {
    let result = Bootstrap::from_yaml_str(CONFIG)
        .expect("bootstrap parses")
        .build()
        .await;
    assert!(matches!(
        result,
        Err(BootstrapError::UnknownRegistryEngine(name)) if name == "mock"
    ));
}

// -----------------------------------------------------------------
// Engine-family assembly: mock engines proving every family's
// factory registry, instance maps, and single-instance exposures.
// -----------------------------------------------------------------

struct MockAuthn;
impl Authenticator for MockAuthn {
    fn scheme(&self) -> &'static str {
        "mock"
    }
    fn authenticate_token(&self, _token: &str) -> Result<AuthClaims, AuthnError> {
        Ok(AuthClaims::default())
    }
    fn create_identity(&self, _claims: &AuthClaims) -> Result<String, AuthnError> {
        Ok("mock".to_string())
    }
}

struct MockAuthz;
impl rushwind_authz::Engine for MockAuthz {
    fn name(&self) -> String {
        "mock".to_string()
    }
    fn is_authorized(
        &self,
        _subject: Subject,
        _action: Action,
        _resource: Resource,
        _project: Project,
    ) -> Result<bool, AuthzError> {
        Ok(true)
    }
    fn projects_authorized(
        &self,
        _subjects: Subjects,
        _action: Action,
        _resource: Resource,
        projects: Projects,
    ) -> Result<Projects, AuthzError> {
        Ok(projects)
    }
    fn filter_authorized_pairs(
        &self,
        _subjects: Subjects,
        pairs: Pairs,
    ) -> Result<Pairs, AuthzError> {
        Ok(pairs)
    }
    fn filter_authorized_projects(&self, _subjects: Subjects) -> Result<Projects, AuthzError> {
        Ok(Vec::new())
    }
    fn set_policies(&self, _policies: PolicyMap, _roles: RoleMap) -> Result<(), AuthzError> {
        Ok(())
    }
}

struct MockSubscriber;
impl rushwind_broker::Subscriber for MockSubscriber {
    fn topic(&self) -> &str {
        "mock"
    }
    fn unsubscribe(
        &mut self,
    ) -> rushwind_broker::BoxFuture<'_, Result<(), rushwind_broker::BrokerError>> {
        Box::pin(async { Ok(()) })
    }
}

struct MockBroker;
impl rushwind_broker::Broker for MockBroker {
    fn name(&self) -> &'static str {
        "mock"
    }
    fn connect(
        &self,
    ) -> rushwind_broker::BoxFuture<'_, Result<(), rushwind_broker::BrokerError>> {
        Box::pin(async { Ok(()) })
    }
    fn disconnect(
        &self,
    ) -> rushwind_broker::BoxFuture<'_, Result<(), rushwind_broker::BrokerError>> {
        Box::pin(async { Ok(()) })
    }
    fn publish<'a>(
        &'a self,
        _topic: &'a str,
        _message: rushwind_broker::Message,
    ) -> rushwind_broker::BoxFuture<'a, Result<(), rushwind_broker::BrokerError>> {
        Box::pin(async { Ok(()) })
    }
    fn subscribe<'a>(
        &'a self,
        _topic: &'a str,
        _handler: rushwind_broker::Handler,
    ) -> rushwind_broker::BoxFuture<
        'a,
        Result<Box<dyn rushwind_broker::Subscriber>, rushwind_broker::BrokerError>,
    > {
        let _ = _handler;
        Box::pin(async { Ok(Box::new(MockSubscriber) as Box<dyn rushwind_broker::Subscriber>) })
    }
}

struct MockCache;
impl rushwind_cache::Cache for MockCache {
    fn get<'a>(
        &'a self,
        _key: &'a str,
    ) -> rushwind_cache::BoxFuture<'a, Result<Option<Vec<u8>>, rushwind_cache::CacheError>>
    {
        Box::pin(async { Ok(None) })
    }
    fn set<'a>(
        &'a self,
        _key: &'a str,
        _value: &'a [u8],
        _ttl: Option<Duration>,
    ) -> rushwind_cache::BoxFuture<'a, Result<(), rushwind_cache::CacheError>> {
        Box::pin(async { Ok(()) })
    }
    fn set_nx<'a>(
        &'a self,
        _key: &'a str,
        _value: &'a [u8],
        _ttl: Option<Duration>,
    ) -> rushwind_cache::BoxFuture<'a, Result<bool, rushwind_cache::CacheError>> {
        Box::pin(async { Ok(true) })
    }
    fn delete<'a>(
        &'a self,
        _key: &'a str,
    ) -> rushwind_cache::BoxFuture<'a, Result<(), rushwind_cache::CacheError>> {
        Box::pin(async { Ok(()) })
    }
    fn has<'a>(
        &'a self,
        _key: &'a str,
    ) -> rushwind_cache::BoxFuture<'a, Result<bool, rushwind_cache::CacheError>> {
        Box::pin(async { Ok(false) })
    }
    fn get_multi<'a>(
        &'a self,
        _keys: &'a [String],
    ) -> rushwind_cache::BoxFuture<'a, Result<Vec<Option<Vec<u8>>>, rushwind_cache::CacheError>>
    {
        let _ = _keys;
        Box::pin(async { Ok(Vec::new()) })
    }
    fn set_multi<'a>(
        &'a self,
        _items: &'a [rushwind_cache::Item],
    ) -> rushwind_cache::BoxFuture<'a, Result<(), rushwind_cache::CacheError>> {
        let _ = _items;
        Box::pin(async { Ok(()) })
    }
    fn close(&self) -> rushwind_cache::BoxFuture<'_, Result<(), rushwind_cache::CacheError>> {
        Box::pin(async { Ok(()) })
    }
}

struct MockCircuitBreaker;
impl rushwind_circuitbreaker::CircuitBreaker for MockCircuitBreaker {
    fn allow(&self) -> Result<(), rushwind_circuitbreaker::CircuitError> {
        Ok(())
    }
    fn mark_success(&self) {}
    fn mark_failure(&self) {}
    fn state(&self) -> rushwind_circuitbreaker::State {
        rushwind_circuitbreaker::State::Closed
    }
    fn close(&self) {}
}

struct MockLimiter;
impl rushwind_ratelimit::Limiter for MockLimiter {
    fn allow(&self) -> bool {
        true
    }
    fn wait(
        &self,
    ) -> rushwind_ratelimit::BoxFuture<'_, Result<(), rushwind_ratelimit::RateLimitError>>
    {
        Box::pin(async { Ok(()) })
    }
    fn close(&self) {}
}

struct MockMetrics;
impl rushwind_metrics::Metrics for MockMetrics {
    fn counter(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    fn histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    fn gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
}

struct MockChatModel;
impl rushwind_ai::ChatModel for MockChatModel {
    fn chat<'a>(
        &'a self,
        _request: rushwind_ai::ChatRequest,
    ) -> rushwind_ai::BoxFuture<'a, Result<rushwind_ai::ChatResponse, AiError>> {
        let _ = _request;
        Box::pin(async { Err(AiError::Request("mock".to_string())) })
    }
}

struct MockObjectStorage;
impl rushwind_oss::ObjectStorage for MockObjectStorage {
    fn put<'a>(
        &'a self,
        _key: &'a str,
        _body: &'a [u8],
        _content_type: Option<&'a str>,
    ) -> rushwind_oss::BoxFuture<'a, Result<(), rushwind_oss::StorageError>> {
        Box::pin(async { Ok(()) })
    }
    fn get<'a>(
        &'a self,
        _key: &'a str,
    ) -> rushwind_oss::BoxFuture<'a, Result<Vec<u8>, rushwind_oss::StorageError>> {
        Box::pin(async { Err(rushwind_oss::StorageError::NotFound) })
    }
    fn delete<'a>(
        &'a self,
        _key: &'a str,
    ) -> rushwind_oss::BoxFuture<'a, Result<(), rushwind_oss::StorageError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A config-source mock answering from a fixed map; one instance
/// answers nothing, the other one key.
struct MockSource {
    values: HashMap<String, Vec<u8>>,
}
impl rushwind_config::Source for MockSource {
    fn load<'a>(
        &'a self,
        key: &'a str,
    ) -> rushwind_config::BoxFuture<'a, Result<Option<Vec<u8>>, rushwind_config::ConfigError>>
    {
        let value = self.values.get(key).cloned();
        Box::pin(async move { Ok(value) })
    }
}

const FAMILIES_YAML: &str = r#"
authn:
  a1: { engine: mockauthn, settings: {} }
authz:
  z1: { engine: mockauthz, settings: {} }
brokers:
  b1: { engine: mockbroker, settings: {} }
caches:
  c1: { engine: mockcache, settings: {} }
circuitbreakers:
  cb1: { engine: mockcb, settings: {} }
limiters:
  l1: { engine: mocklimiter, settings: {} }
metrics:
  engine: mockmetrics
  settings: {}
ai:
  engine: mockchat
  settings: {}
oss:
  engine: mockoss
  settings: {}
config_sources:
  - engine: mocksource-empty
settings: {}
  - engine: mocksource-primed
settings: {}
servers: []
"#;

/// Every family assembles from its registered factory into its
/// exposure: the named families into their instance maps, the
/// single-instance families into their options, and the config
/// source list into the priority fallback.
#[tokio::test]
async fn families_assemble_into_their_exposures() {
    let bootstrapped = Bootstrap::from_yaml_str(FAMILIES_YAML)
        .expect("yaml must parse")
        .authn_factory("mockauthn", |_settings| {
            Box::pin(async move { Ok(Arc::new(MockAuthn) as Arc<dyn Authenticator>) })
        })
        .authz_factory("mockauthz", |_settings| {
            Box::pin(async move { Ok(Arc::new(MockAuthz) as Arc<dyn AuthzEngine>) })
        })
        .broker_factory("mockbroker", |_settings| {
            Box::pin(async move { Ok(Arc::new(MockBroker) as Arc<dyn Broker>) })
        })
        .cache_factory("mockcache", |_settings| {
            Box::pin(async move { Ok(Arc::new(MockCache) as Arc<dyn Cache>) })
        })
        .circuitbreaker_factory("mockcb", |_settings| {
            Box::pin(async move { Ok(Arc::new(MockCircuitBreaker) as Arc<dyn CircuitBreaker>) })
        })
        .limiter_factory("mocklimiter", |_settings| {
            Box::pin(async move { Ok(Arc::new(MockLimiter) as Arc<dyn Limiter>) })
        })
        .metrics_factory("mockmetrics", |_settings| {
            Box::pin(async move { Ok(Arc::new(MockMetrics) as Arc<dyn Metrics>) })
        })
        .ai_factory("mockchat", |_settings| {
            Box::pin(async move { Ok(Arc::new(MockChatModel) as Arc<dyn ChatModel>) })
        })
        .oss_factory("mockoss", |_settings| {
            Box::pin(async move { Ok(Arc::new(MockObjectStorage) as Arc<dyn ObjectStorage>) })
        })
        .config_source_factory("mocksource-empty", |_settings| {
            Box::pin(async move {
                Ok(Arc::new(MockSource {
                    values: HashMap::new(),
                }) as Arc<dyn rushwind_config::Source>)
            })
        })
        .config_source_factory("mocksource-primed", |_settings| {
            Box::pin(async move {
                let mut values = HashMap::new();
                values.insert("k".to_string(), b"from-primed".to_vec());
                Ok(Arc::new(MockSource { values }) as Arc<dyn rushwind_config::Source>)
            })
        })
        .build()
        .await
        .expect("assembly must succeed");

    assert_eq!(bootstrapped.authn.len(), 1, "authn instance map");
    assert!(bootstrapped.authn.contains_key("a1"));
    assert_eq!(bootstrapped.authz.len(), 1, "authz instance map");
    assert!(bootstrapped.authz.contains_key("z1"));
    assert_eq!(bootstrapped.brokers.len(), 1, "broker instance map");
    assert!(bootstrapped.brokers.contains_key("b1"));
    assert_eq!(bootstrapped.caches.len(), 1, "cache instance map");
    assert!(bootstrapped.caches.contains_key("c1"));
    assert_eq!(
        bootstrapped.circuitbreakers.len(),
        1,
        "circuitbreaker instance map"
    );
    assert!(bootstrapped.circuitbreakers.contains_key("cb1"));
    assert_eq!(bootstrapped.limiters.len(), 1, "limiter instance map");
    assert!(bootstrapped.limiters.contains_key("l1"));
    assert!(bootstrapped.ai.is_some(), "ai single instance");
    assert!(
        bootstrapped.object_storage.is_some(),
        "object storage single instance"
    );
    assert!(bootstrapped.metrics.is_some(), "metrics single instance");

    // The fallback walks in priority order: the empty source
    // answers nothing, the primed one answers; a key no source
    // answers is the unresolved error.
    let source = bootstrapped.config.expect("fallback must assemble");
    let answered = source
        .load("k")
        .await
        .expect("the primed source answers")
        .expect("the value is present");
    assert_eq!(answered, b"from-primed".to_vec());
    assert!(
        source.load("missing").await.is_err(),
        "all-absent resolves to the unresolved error"
    );
}

/// An engine name with no registered factory fails the assembly
/// with the family-named error.
#[tokio::test]
async fn unknown_engine_is_an_error() {
    let yaml = r#"
brokers:
  b1: { engine: nope, settings: {} }
servers: []
"#;
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("unknown engine must fail");
    assert!(matches!(
        error,
        BootstrapError::UnknownEngine { domain, name }
            if domain == "broker engine" && name == "nope"
    ));
}

/// A route-pack reference naming an authn instance that was never
/// assembled fails the assembly.
#[tokio::test]
async fn unknown_authn_instance_is_an_error() {
    let yaml = r#"
servers:
  - kind: http
bind: 127.0.0.1:0
route_packs:
  - name: p
    authn: ghost
"#;
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .route_pack("p", |_settings, _input| {
            Ok(RouteSurface::new(Router::new()))
        })
        .build()
        .await
        .expect_err("unknown instance must fail");
    assert!(matches!(
        error,
        BootstrapError::UnknownEngine { domain, name }
            if domain == "authn instance" && name == "ghost"
    ));
}

/// A permission point carrying both project axes is a config
/// error.
#[tokio::test]
async fn project_axes_are_mutually_exclusive() {
    let yaml = r#"
authz:
  z1: { engine: mockauthz, settings: {} }
servers:
  - kind: http
bind: 127.0.0.1:0
route_packs:
  - name: p
    authz:
      engine: z1
      action: a
      resource: r
      project: fixed
      project_claim: claim
"#;
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .authz_factory("mockauthz", |_settings| {
            Box::pin(async move { Ok(Arc::new(MockAuthz) as Arc<dyn AuthzEngine>) })
        })
        .route_pack("p", |_settings, _input| {
            Ok(RouteSurface::new(Router::new()))
        })
        .build()
        .await
        .expect_err("both project axes must fail");
    assert!(matches!(error, BootstrapError::Config(_)));
}

/// The bind wire parses the standard socket-address form and the
/// host-any `":port"` form, and rejects hostnames and malformed
/// text.
#[test]
fn bind_wires_parse_both_forms() {
    assert!(matches!(
        parse_bind(":7788"),
        Some(addr) if addr.port() == 7788 && addr.ip().is_unspecified()
    ));
    assert!(matches!(
        parse_bind("127.0.0.1:8080"),
        Some(addr) if addr.port() == 8080 && addr.ip().is_loopback()
    ));
    assert_eq!(parse_bind("localhost:8080"), None);
    assert_eq!(parse_bind("nope"), None);
}

/// Duration strings parse into seconds across the unit set.
#[test]
fn duration_strings_parse_to_seconds() {
    assert_eq!(parse_duration_string("300s"), Some(300.0));
    assert_eq!(parse_duration_string("90m"), Some(5400.0));
    assert_eq!(parse_duration_string("1.5h"), Some(5400.0));
    assert_eq!(parse_duration_string("0.4s"), Some(0.4));
}

/// Malformed duration strings reject.
#[test]
fn malformed_durations_reject() {
    assert_eq!(parse_duration_string(""), None);
    assert_eq!(parse_duration_string("abc"), None);
    assert_eq!(parse_duration_string("12q"), None);
}

/// The edge wire takes its request budget as a duration string or
/// a plain second count; an absent field leaves no budget.
#[test]
fn edge_wires_accept_both_budget_forms() {
    let string_form: EdgeWire =
        serde_yaml::from_str("timeout: 10s").expect("duration-string form must parse");
    assert!(matches!(string_form.timeout, Some(d) if d.0.as_secs() == 10));
    let count_form: EdgeWire =
        serde_yaml::from_str("timeout: 10").expect("second-count form must parse");
    assert!(matches!(count_form.timeout, Some(d) if d.0.as_secs() == 10));
    let absent: EdgeWire = serde_yaml::from_str("{}").expect("empty edge must parse");
    assert!(absent.timeout.is_none());
}

/// Registered cron jobs mount by name on the `cron` server kind,
/// and the server reports its configured endpoint.
#[tokio::test]
async fn cron_jobs_mount_on_cron_servers() {
    let yaml = r#"
servers:
  - kind: cron
endpoint: cron://unit
jobs: [tick]
"#;
    let bootstrapped = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .cron_job(
            "tick",
            CronJob::new(
                "tick",
                CronSpec::parse("* * * * *").expect("spec must parse"),
                || Box::pin(async {}),
            ),
        )
        .build()
        .await
        .expect("assembly must succeed");
    assert_eq!(bootstrapped.endpoints, vec!["cron://unit".to_string()]);
}

/// A cron job name that was never registered fails the assembly.
#[tokio::test]
async fn unknown_cron_job_is_an_error() {
    let yaml = r#"
servers:
  - kind: cron
endpoint: cron://unit
jobs: [ghost]
"#;
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("unknown job must fail");
    assert!(matches!(
        error,
        BootstrapError::UnknownEngine { domain, name }
            if domain == "cron job" && name == "ghost"
    ));
}

/// Script instances assemble through the script domain's own
/// factory registry into the manager, and the shutdown sweep
/// closes and clears them.
#[tokio::test]
async fn scripts_assemble_and_close_on_shutdown() {
    rushwind_script_lua::register();
    let yaml = r#"
scripts:
  s1: { engine: lua }
"#;
    let bootstrapped = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect("assembly must succeed");
    let manager = bootstrapped.scripts.expect("manager must assemble");
    assert!(
        manager.get("s1").is_some(),
        "the instance must be registered"
    );

    bootstrapped.app.stop();
    let _ = bootstrapped
        .app
        .run(rushwind_transport::StopSignal::new())
        .await;
    assert!(
        manager.get("s1").is_none(),
        "the shutdown sweep closes and clears"
    );
}

/// The health section fails loudly when the feature is compiled
/// out.
#[cfg(not(feature = "health"))]
#[tokio::test]
async fn health_section_requires_the_feature() {
    let yaml = "health:\n  timeout_ms: 1\nservers: []\n";
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("featureless health must fail");
    assert!(matches!(
        error,
        BootstrapError::Failed(msg) if msg.contains("feature 'health'")
    ));
}

/// The health mount fails loudly when the feature is compiled out.
#[cfg(not(feature = "health"))]
#[tokio::test]
async fn health_mount_requires_the_feature() {
    let yaml =
        "servers:\n  - kind: http\n    bind: 127.0.0.1:0\n    mounts:\n      health: true\n";
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("featureless health mount must fail");
    assert!(matches!(
        error,
        BootstrapError::Failed(msg) if msg.contains("feature 'health'")
    ));
}

/// The tracer section fails loudly when the feature is compiled
/// out.
#[cfg(not(feature = "trace"))]
#[tokio::test]
async fn tracer_section_requires_the_feature() {
    let yaml = "tracer:\n  endpoint: 127.0.0.1:4317\nservers: []\n";
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("featureless tracer must fail");
    assert!(matches!(
        error,
        BootstrapError::Failed(msg) if msg.contains("feature 'trace'")
    ));
