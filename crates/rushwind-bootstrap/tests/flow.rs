//! End-to-end assembly tests: YAML → storage engine + HTTP server with
//! route packs and the edge stack, the security wraps over packs and
//! storage endpoints, the cron transport, and the unknown-name error
//! paths — plus the feature-gated domain mounts and tracer assembly.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use rushwind_authn::Authenticator;
use rushwind_authn_apikey::{ApiKeyAuthenticator, ApiKeyOptions};
use rushwind_authn_noop::NoopAuthenticator;
use rushwind_authz::Engine as AuthzEngine;
use rushwind_authz_acl::{AclEngine, AclOptions};
use rushwind_bootstrap::{Bootstrap, BootstrapError, RouteInput, RouteSurface};
use rushwind_storage::{ColumnKind, QueryCtx, Record, Repository, Schema};
use rushwind_storage_memory::MemoryRepo;
use rushwind_transport::StopSignal;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, timeout};

const FLOW_YAML: &str = r#"
app:
  name: flow-test
  version: v0.1.0
  stop_timeout_secs: 1
storage:
  engine: memory
  settings: {}
storage_endpoints:
  - nest: /widgets
    api: crud
    viewer: all
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs:
      - name: health
      - name: wired
"#;

fn schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .build()
        .expect("schema is valid")
}

/// The application-side glue: the storage factory captures the schema
/// (application knowledge) and reads engine knobs from settings.
fn register_memory_engine(bootstrap: Bootstrap) -> Bootstrap {
    bootstrap.storage_factory("memory", |_settings| {
        Box::pin(async move {
            let repo =
                MemoryRepo::new(schema()).map_err(|e| BootstrapError::Failed(e.to_string()))?;
            Ok(Arc::new(repo) as Arc<dyn Repository>)
        })
    })
}

fn register_packs(bootstrap: Bootstrap) -> Bootstrap {
    bootstrap
        .route_pack("health", |_settings, _input| {
            Ok(RouteSurface::new(
                Router::new().route("/health", get(|| async { "ok" })),
            ))
        })
        .route_pack("wired", |_settings, input: RouteInput| {
            // Proves the storage engine reaches the pack: the route
            // reports whether the repository was injected.
            let repository = input.repository.clone();
            Ok(RouteSurface::new(Router::new().route(
                "/wired",
                get(move || {
                    let repository = repository.clone();
                    async move { format!("repo={}", repository.is_some()) }
                }),
            )))
        })
}

/// A minimal raw HTTP/1.1 GET; `Connection: close` makes the server end
/// the stream so `read_to_end` completes.
async fn http_get(addr: SocketAddr, path: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect must succeed");
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .expect("write must succeed");
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .expect("read must complete");
    String::from_utf8(buf).expect("response must be utf-8")
}

/// A raw HTTP request with an optional JSON body; returns the full
/// response text.
async fn http_send(addr: SocketAddr, method: &str, path: &str, body: &str) -> String {
    http_headers(addr, method, path, &[], body).await
}

/// A raw HTTP request with extra headers and an optional JSON body;
/// returns the full response text.
async fn http_headers(
    addr: SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
    body: &str,
) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect must succeed");
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: test\r\n");
    for (name, value) in extra_headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str(&format!(
        "Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    ));
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write must succeed");
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .expect("read must complete");
    String::from_utf8(buf).expect("response must be utf-8")
}

/// The endpoint of the first assembled http server.
fn first_endpoint(bootstrapped: &rushwind_bootstrap::Bootstrapped) -> SocketAddr {
    bootstrapped.endpoints[0]
        .trim_start_matches("http://")
        .parse()
        .expect("endpoint must be host:port")
}

#[tokio::test]
async fn assembles_storage_and_http_under_one_lifecycle() {
    let bootstrapped = register_packs(register_memory_engine(
        Bootstrap::from_yaml_str(FLOW_YAML).expect("yaml must parse"),
    ))
    .build()
    .await
    .expect("assembly must succeed");

    // The configured engine was constructed and shared into the packs.
    assert!(bootstrapped.repository.is_some());
    assert_eq!(bootstrapped.endpoints.len(), 1);
    assert!(bootstrapped.endpoints[0].starts_with("http://127.0.0.1:"));
    let addr = first_endpoint(&bootstrapped);

    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let health = timeout(Duration::from_secs(5), http_get(addr, "/health"))
        .await
        .expect("health must respond");
    assert!(health.contains("200"), "health must return 200: {health}");
    assert!(health.contains("ok"), "health body: {health}");

    let wired = timeout(Duration::from_secs(5), http_get(addr, "/wired"))
        .await
        .expect("wired must respond");
    assert!(
        wired.contains("repo=true"),
        "route packs must receive the configured storage: {wired}"
    );

    // Lifecycle shutdown returns a clean outcome.
    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn unknown_server_kind_is_an_error() {
    let yaml = r#"
servers:
  - kind: websocket
    bind: 127.0.0.1:0
"#;
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("unknown kind must fail");
    assert!(matches!(error, BootstrapError::UnknownServerKind(kind) if kind == "websocket"));
}

#[tokio::test]
async fn unknown_route_pack_is_an_error() {
    let yaml = r#"
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs:
      - name: does-not-exist
"#;
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("unknown pack must fail");
    assert!(matches!(error, BootstrapError::UnknownRoutePack(name) if name == "does-not-exist"));
}

#[tokio::test]
async fn unknown_storage_engine_is_an_error() {
    let yaml = r#"
storage:
  engine: postgres
servers: []
"#;
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("unknown engine must fail");
    assert!(matches!(error, BootstrapError::UnknownStorageEngine(engine) if engine == "postgres"));
}

#[tokio::test]
async fn malformed_yaml_is_a_config_error() {
    let error =
        Bootstrap::from_yaml_str("app: [unclosed").expect_err("malformed yaml must fail at parse");
    assert!(matches!(error, BootstrapError::Config(_)));
}

#[tokio::test]
async fn storage_endpoint_serves_crud_over_http() {
    let bootstrapped = register_packs(register_memory_engine(
        Bootstrap::from_yaml_str(FLOW_YAML).expect("yaml must parse"),
    ))
    .build()
    .await
    .expect("assembly must succeed");
    let addr = first_endpoint(&bootstrapped);

    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    // The nested crud edge serves the suite schema: a create over real
    // HTTP lands in the configured storage and reads back.
    let created = timeout(
        Duration::from_secs(5),
        http_send(addr, "POST", "/widgets", r#"{"name":"bolt"}"#),
    )
    .await
    .expect("create must respond");
    assert!(created.contains("201"), "create must return 201: {created}");

    let listed = timeout(Duration::from_secs(5), http_get(addr, "/widgets"))
        .await
        .expect("list must respond");
    assert!(listed.contains("200"), "list must return 200: {listed}");
    assert!(
        listed.contains("bolt"),
        "created row must be listed: {listed}"
    );

    // Lifecycle shutdown returns a clean outcome.
    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

/// The crud edge's deny default (audit H2): a `storage_endpoints[]`
/// entry without a `viewer` policy mounts a CrudApi with no viewer —
/// every request sees nothing. All-access is the explicit `viewer: all`
/// opt-in FLOW_YAML takes.
#[tokio::test]
async fn crud_endpoint_without_viewer_policy_denies_reads() {
    let yaml = r#"
app:
  name: deny-test
  version: v0.1.0
  stop_timeout_secs: 1
storage:
  engine: memory
  settings: {}
storage_endpoints:
  - nest: /widgets
    api: crud
servers:
  - kind: http
    bind: 127.0.0.1:0
"#;
    let bootstrapped =
        register_memory_engine(Bootstrap::from_yaml_str(yaml).expect("yaml must parse"))
            .build()
            .await
            .expect("assembly must succeed");

    // Seed directly through the assembled engine: the row exists, the
    // edge just must not show it.
    bootstrapped
        .repository
        .as_ref()
        .expect("storage assembled")
        .create(
            QueryCtx::all_access(),
            Record::new().set("name", "bolt").set("id", 1),
        )
        .await
        .expect("seed lands");

    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let listed = timeout(Duration::from_secs(5), http_get(addr, "/widgets"))
        .await
        .expect("list must respond");
    assert!(
        listed.contains("200"),
        "the request itself still serves: {listed}"
    );
    assert!(
        !listed.contains("bolt"),
        "the deny default lists nothing: {listed}"
    );

    let row = timeout(Duration::from_secs(5), http_get(addr, "/widgets/1"))
        .await
        .expect("get must respond");
    assert!(
        row.contains("404"),
        "the deny default hides every row: {row}"
    );

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

// ---------------------------------------------------------------------
// The edge stack: request-id default and opt-out, CORS, timeout,
// recovery. The plain pack serves one unguarded route.
// ---------------------------------------------------------------------

const EDGE_YAML: &str = r#"
app:
  name: edge-test
  version: v0.1.0
  stop_timeout_secs: 1
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs:
      - name: plain
"#;

const NO_REQUEST_ID_YAML: &str = r#"
app:
  name: edge-test
  version: v0.1.0
  stop_timeout_secs: 1
servers:
  - kind: http
    bind: 127.0.0.1:0
    edge:
      request_id: false
    route_packs:
      - name: plain
"#;

fn plain_pack(bootstrap: Bootstrap) -> Bootstrap {
    bootstrap.route_pack("plain", |_settings, _input| {
        Ok(RouteSurface::new(
            Router::new().route("/plain", get(|| async { "ok" })),
        ))
    })
}

/// The default edge mints a request id on every response.
#[tokio::test]
async fn default_edge_mints_request_ids() {
    let bootstrapped = plain_pack(Bootstrap::from_yaml_str(EDGE_YAML).expect("yaml must parse"))
        .build()
        .await
        .expect("assembly must succeed");
    assert!(bootstrapped.endpoints[0].starts_with("http://127.0.0.1:"));
    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let response = timeout(Duration::from_secs(5), http_get(addr, "/plain"))
        .await
        .expect("plain must respond");
    assert!(
        response.contains("200"),
        "plain must return 200: {response}"
    );
    assert!(
        response.to_ascii_lowercase().contains("x-request-id:"),
        "the default edge mints a request id: {response}"
    );

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

/// The configured opt-out drops the request-id middleware.
#[tokio::test]
async fn request_id_opt_out_is_honored() {
    let bootstrapped =
        plain_pack(Bootstrap::from_yaml_str(NO_REQUEST_ID_YAML).expect("yaml must parse"))
            .build()
            .await
            .expect("assembly must succeed");
    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let response = timeout(Duration::from_secs(5), http_get(addr, "/plain"))
        .await
        .expect("plain must respond");
    assert!(
        response.contains("200"),
        "plain must return 200: {response}"
    );
    assert!(
        !response.to_ascii_lowercase().contains("x-request-id:"),
        "the opt-out drops the request id: {response}"
    );

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

/// The CORS policy answers preflights for the configured origin.
#[tokio::test]
async fn cors_preflight_answers_configured_origin() {
    let yaml = r#"
app:
  name: edge-test
  version: v0.1.0
  stop_timeout_secs: 1
servers:
  - kind: http
    bind: 127.0.0.1:0
    edge:
      cors:
        origins: [https://example.com]
        methods: [GET]
    route_packs:
      - name: plain
"#;
    let bootstrapped = plain_pack(Bootstrap::from_yaml_str(yaml).expect("yaml must parse"))
        .build()
        .await
        .expect("assembly must succeed");
    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let preflight = timeout(
        Duration::from_secs(5),
        http_headers(
            addr,
            "OPTIONS",
            "/plain",
            &[
                ("Origin", "https://example.com"),
                ("Access-Control-Request-Method", "GET"),
            ],
            "",
        ),
    )
    .await
    .expect("preflight must respond");
    assert!(
        preflight
            .to_ascii_lowercase()
            .contains("access-control-allow-origin: https://example.com"),
        "the configured origin gets its allow header: {preflight}"
    );

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

/// The request budget bounds the handler; over budget answers the 504
/// envelope.
#[tokio::test]
async fn timeout_budget_answers_504() {
    let yaml = r#"
app:
  name: edge-test
  version: v0.1.0
  stop_timeout_secs: 1
servers:
  - kind: http
    bind: 127.0.0.1:0
    edge:
      timeout: 1
    route_packs:
      - name: slow
"#;
    let bootstrapped = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .route_pack("slow", |_settings, _input| {
            Ok(RouteSurface::new(Router::new().route(
                "/slow",
                get(|| async {
                    sleep(Duration::from_secs(5)).await;
                    "late"
                }),
            )))
        })
        .build()
        .await
        .expect("assembly must succeed");
    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let response = timeout(Duration::from_secs(8), http_get(addr, "/slow"))
        .await
        .expect("slow must respond");
    assert!(
        response.contains("504"),
        "the budget bounds the handler: {response}"
    );

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

/// A panicking handler answers the 500 envelope instead of tearing the
/// connection down.
#[tokio::test]
async fn recovery_answers_500_on_panic() {
    let yaml = r#"
app:
  name: edge-test
  version: v0.1.0
  stop_timeout_secs: 1
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs:
      - name: panicky
"#;
    let bootstrapped = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .route_pack("panicky", |_settings, _input| {
            Ok(RouteSurface::new(Router::new().route(
                "/panicky",
                get(|| async {
                    panic!("boom");
                    #[allow(unreachable_code)]
                    "unreachable"
                }),
            )))
        })
        .build()
        .await
        .expect("assembly must succeed");
    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let response = timeout(Duration::from_secs(5), http_get(addr, "/panicky"))
        .await
        .expect("panicky must respond");
    assert!(
        response.contains("500"),
        "recovery answers the envelope: {response}"
    );

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

// ---------------------------------------------------------------------
// The security wraps: a configured authn instance over the crud
// storage endpoint, and the noop-authn + ACL chain over a route pack.
// ---------------------------------------------------------------------

const AUTHN_CRUD_YAML: &str = r#"
app:
  name: guard-test
  version: v0.1.0
  stop_timeout_secs: 1
storage:
  engine: memory
  settings: {}
storage_endpoints:
  - nest: /widgets
    api: crud
    viewer: all
    authn: apikey1
authn:
  apikey1:
    engine: apikey
    settings: {}
servers:
  - kind: http
    bind: 127.0.0.1:0
"#;

/// The storage endpoint's authn wrap rejects anonymous callers and
/// accepts the configured key.
#[tokio::test]
async fn crud_endpoint_enforces_configured_authn() {
    let bootstrapped = register_memory_engine(
        Bootstrap::from_yaml_str(AUTHN_CRUD_YAML)
            .expect("yaml must parse")
            .authn_factory("apikey", |_settings| {
                Box::pin(async move {
                    Ok(Arc::new(ApiKeyAuthenticator::new(
                        ApiKeyOptions::new().with_keys(&["testkey"]),
                    )) as Arc<dyn Authenticator>)
                })
            }),
    )
    .build()
    .await
    .expect("assembly must succeed");
    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    // Anonymous and wrong-key callers get the 401 envelope.
    let anonymous = timeout(
        Duration::from_secs(5),
        http_send(addr, "POST", "/widgets", r#"{"name":"bolt"}"#),
    )
    .await
    .expect("anonymous create must respond");
    assert!(
        anonymous.contains("401"),
        "anonymous callers are rejected: {anonymous}"
    );
    let wrong_key = timeout(
        Duration::from_secs(5),
        http_headers(
            addr,
            "POST",
            "/widgets",
            &[("Authorization", "Bearer wrong")],
            r#"{"name":"bolt"}"#,
        ),
    )
    .await
    .expect("wrong-key create must respond");
    assert!(
        wrong_key.contains("401"),
        "wrong keys are rejected: {wrong_key}"
    );

    // The configured key passes the wrap and reaches the crud edge.
    let authorized = timeout(
        Duration::from_secs(5),
        http_headers(
            addr,
            "POST",
            "/widgets",
            &[("Authorization", "Bearer testkey")],
            r#"{"name":"bolt"}"#,
        ),
    )
    .await
    .expect("authorized create must respond");
    assert!(
        authorized.contains("201"),
        "the configured key passes the wrap: {authorized}"
    );

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

const ACL_YAML: &str = r#"
app:
  name: guard-test
  version: v0.1.0
  stop_timeout_secs: 1
authn:
  noop1:
    engine: noop
    settings: {}
authz:
  acl1:
    engine: acl
    settings: {}
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs:
      - name: open
      - name: guarded
        authn: noop1
        authz:
          engine: acl1
          action: write
          resource: vault
"#;

fn acl_packs(bootstrap: Bootstrap) -> Bootstrap {
    bootstrap
        .route_pack("open", |_settings, _input| {
            Ok(RouteSurface::new(
                Router::new().route("/open", get(|| async { "open" })),
            ))
        })
        .route_pack("guarded", |_settings, _input| {
            Ok(RouteSurface::new(
                Router::new().route("/guarded", get(|| async { "guarded" })),
            ))
        })
}

/// A ruleless ACL engine denies every request on the guarded pack —
/// including the plain 200 on the unguarded pack beside it.
#[tokio::test]
async fn acl_without_rules_denies_the_guarded_pack() {
    let bootstrapped = acl_packs(
        Bootstrap::from_yaml_str(ACL_YAML)
            .expect("yaml must parse")
            .authn_factory("noop", |_settings| {
                Box::pin(
                    async move { Ok(Arc::new(NoopAuthenticator::new()) as Arc<dyn Authenticator>) },
                )
            })
            .authz_factory("acl", |_settings| {
                Box::pin(async move {
                    Ok(Arc::new(AclEngine::new(AclOptions::new())) as Arc<dyn AuthzEngine>)
                })
            }),
    )
    .build()
    .await
    .expect("assembly must succeed");
    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let open = timeout(Duration::from_secs(5), http_get(addr, "/open"))
        .await
        .expect("open must respond");
    assert!(open.contains("200"), "the unguarded pack serves: {open}");

    // The noop authenticator accepts any bearer token; the ruleless
    // engine then denies the permission point.
    let guarded = timeout(
        Duration::from_secs(5),
        http_headers(
            addr,
            "GET",
            "/guarded",
            &[("Authorization", "Bearer anon")],
            "",
        ),
    )
    .await
    .expect("guarded must respond");
    assert!(
        guarded.contains("403"),
        "the ruleless engine denies: {guarded}"
    );

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

/// A rule for the pack's action/resource axes allows the guarded pack,
/// and the two project-axis shapes of the permission point evaluate
/// identically under the ACL engine.
#[tokio::test]
async fn acl_with_rules_allows_the_guarded_pack() {
    let yaml = r#"
app:
  name: guard-test
  version: v0.1.0
  stop_timeout_secs: 1
authn:
  noop1:
    engine: noop
    settings: {}
authz:
  acl1:
    engine: acl
    settings: {}
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs:
      - name: guarded-fixed
        authn: noop1
        authz:
          engine: acl1
          action: write
          resource: vault
          project: tenant-a
      - name: guarded-claim
        authn: noop1
        authz:
          engine: acl1
          action: write
          resource: vault
          project_claim: tenant_id
"#;
    let bootstrapped = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .authn_factory("noop", |_settings| {
            Box::pin(
                async move { Ok(Arc::new(NoopAuthenticator::new()) as Arc<dyn Authenticator>) },
            )
        })
        .authz_factory("acl", |_settings| {
            Box::pin(async move {
                Ok(Arc::new(AclEngine::new(
                    AclOptions::new().with_rule("", "write", "vault"),
                )) as Arc<dyn AuthzEngine>)
            })
        })
        .route_pack("guarded-fixed", |_settings, _input| {
            Ok(RouteSurface::new(
                Router::new().route("/guarded-fixed", get(|| async { "fixed" })),
            ))
        })
        .route_pack("guarded-claim", |_settings, _input| {
            Ok(RouteSurface::new(
                Router::new().route("/guarded-claim", get(|| async { "claim" })),
            ))
        })
        .build()
        .await
        .expect("assembly must succeed");
    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let fixed = timeout(
        Duration::from_secs(5),
        http_headers(
            addr,
            "GET",
            "/guarded-fixed",
            &[("Authorization", "Bearer anon")],
            "",
        ),
    )
    .await
    .expect("guarded-fixed must respond");
    assert!(
        fixed.contains("200"),
        "the fixed project axis evaluates through the engine: {fixed}"
    );

    let claim = timeout(
        Duration::from_secs(5),
        http_headers(
            addr,
            "GET",
            "/guarded-claim",
            &[("Authorization", "Bearer anon")],
            "",
        ),
    )
    .await
    .expect("guarded-claim must respond");
    assert!(
        claim.contains("200"),
        "the claim-carried project axis evaluates through the engine: {claim}"
    );

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

// ---------------------------------------------------------------------
// The feature-gated domain mounts and the tracer assembly.
// ---------------------------------------------------------------------

/// The health mount serves the liveness and readiness probes from the
/// aggregated health section (feature `health`).
#[cfg(feature = "health")]
#[tokio::test]
async fn health_mount_serves_probes() {
    let yaml = r#"
app:
  name: mount-test
  version: v0.1.0
  stop_timeout_secs: 1
health:
  timeout_ms: 1000
servers:
  - kind: http
    bind: 127.0.0.1:0
    mounts:
      health: true
    route_packs:
      - name: plain
"#;
    let bootstrapped = plain_pack(Bootstrap::from_yaml_str(yaml).expect("yaml must parse"))
        .build()
        .await
        .expect("assembly must succeed");
    assert!(bootstrapped.health.is_some(), "the aggregator assembles");
    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let liveness = timeout(Duration::from_secs(5), http_get(addr, "/healthz"))
        .await
        .expect("liveness must respond");
    assert!(liveness.contains("200"), "liveness serves: {liveness}");

    let readiness = timeout(Duration::from_secs(5), http_get(addr, "/readyz"))
        .await
        .expect("readiness must respond");
    assert!(readiness.contains("200"), "readiness serves: {readiness}");

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

/// The health mount without a health section fails the assembly.
#[cfg(feature = "health")]
#[tokio::test]
async fn health_mount_requires_a_health_section() {
    let yaml = r#"
app:
  name: mount-test
  version: v0.1.0
  stop_timeout_secs: 1
servers:
  - kind: http
    bind: 127.0.0.1:0
    mounts:
      health: true
    route_packs:
      - name: plain
"#;
    let error = plain_pack(Bootstrap::from_yaml_str(yaml).expect("yaml must parse"))
        .build()
        .await
        .expect_err("sectionless mount must fail");
    assert!(matches!(error, BootstrapError::Failed(_)));
}

/// The metrics mount serves the scrape endpoint from the built-in
/// Prometheus engine (feature `metrics`).
#[cfg(feature = "metrics")]
#[tokio::test]
async fn metrics_mount_serves_scrape() {
    let yaml = r#"
app:
  name: mount-test
  version: v0.1.0
  stop_timeout_secs: 1
metrics:
  engine: prometheus
  settings: {}
servers:
  - kind: http
    bind: 127.0.0.1:0
    mounts:
      metrics: true
    route_packs:
      - name: plain
"#;
    let bootstrapped = plain_pack(Bootstrap::from_yaml_str(yaml).expect("yaml must parse"))
        .build()
        .await
        .expect("assembly must succeed");
    let addr = first_endpoint(&bootstrapped);
    let trigger = StopSignal::new();
    let app = Arc::new(bootstrapped.app);
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    sleep(Duration::from_millis(200)).await;

    let scrape = timeout(Duration::from_secs(5), http_get(addr, "/metrics"))
        .await
        .expect("scrape must respond");
    assert!(
        scrape.contains("200"),
        "the scrape endpoint serves: {scrape}"
    );
    assert!(
        scrape.to_ascii_lowercase().contains("text/plain"),
        "the scrape renders the text format: {scrape}"
    );

    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

/// The metrics mount without the built-in engine fails the assembly.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn metrics_mount_requires_the_builtin_engine() {
    let yaml = r#"
app:
  name: mount-test
  version: v0.1.0
  stop_timeout_secs: 1
servers:
  - kind: http
    bind: 127.0.0.1:0
    mounts:
      metrics: true
    route_packs:
      - name: plain
"#;
    let error = plain_pack(Bootstrap::from_yaml_str(yaml).expect("yaml must parse"))
        .build()
        .await
        .expect_err("engineless mount must fail");
    assert!(matches!(error, BootstrapError::Failed(_)));
}

/// The tracer section assembles the OTLP provider (feature `trace`).
#[cfg(feature = "trace")]
#[tokio::test]
async fn tracer_assembles_provider() {
    let yaml =
        "tracer:\n  endpoint: 127.0.0.1:4317\n  transport: grpc\n  insecure: true\nservers: []\n";
    let bootstrapped = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect("assembly must succeed");
    assert!(bootstrapped.tracer.is_some(), "the provider must assemble");
}
