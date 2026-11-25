//! The HTTP edge over `tower::ServiceExt::oneshot` — no listening
//! socket anywhere. Every wrapper's wire behavior is pinned here: the
//! envelope shape, the panic/timeout/CORS answers, the request-id
//! echo/mint, and the authn/authz bridges' verdicts.
//!
//! Composition note: `Router::layer` entrains only the routes already
//! in the router, so every test builds its full route set first and
//! wraps last — the same order a real application assembles in.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use axum::response::Response;
use axum::routing::get;
use axum::{Extension, Router};
use rushwind_authn::{AuthClaims, Authenticator, AuthnError};
use rushwind_authz::{
    Action, AuthzError, Engine, Pairs, PolicyMap, Project, Projects, Resource, RoleMap, Subject,
    Subjects,
};
use rushwind_http::*;
use serde_json::{json, Value};
use tower::ServiceExt;

// ---------------------------------------------------------------------
// Stubs
// ---------------------------------------------------------------------

/// `Bearer good` authenticates as `alice`; every other credential is
/// rejected with the taxonomy variant the wire answer should pin.
struct StubAuthn;

impl Authenticator for StubAuthn {
    fn scheme(&self) -> &'static str {
        rushwind_authn::SCHEME_BEARER
    }

    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError> {
        match token {
            "good" => {
                let mut map = serde_json::Map::new();
                map.insert("sub".to_owned(), json!("alice"));
                map.insert("tenant".to_owned(), json!("acme"));
                Ok(AuthClaims(map))
            }
            "expired" => Err(AuthnError::TokenExpired),
            "misconfigured" => Err(AuthnError::GetKeyFailed),
            _ => Err(AuthnError::Unauthenticated),
        }
    }

    fn create_identity(&self, _claims: &AuthClaims) -> Result<String, AuthnError> {
        Ok("good".to_owned())
    }
}

/// A policy engine whose single verdict is set at construction; `fail`
/// exercises the engine-error path.
struct StubEngine {
    verdict: bool,
    fail: bool,
}

impl Engine for StubEngine {
    fn name(&self) -> String {
        "stub".to_owned()
    }

    fn is_authorized(
        &self,
        _subject: Subject,
        _action: Action,
        _resource: Resource,
        _project: Project,
    ) -> Result<bool, AuthzError> {
        if self.fail {
            Err(AuthzError::InvalidClaims)
        } else {
            Ok(self.verdict)
        }
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

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn get_request(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

fn authed_request() -> Request<Body> {
    Request::builder()
        .uri("/thing")
        .header("authorization", "Bearer good")
        .body(Body::empty())
        .unwrap()
}

async fn send(app: Router, request: Request<Body>) -> Response {
    app.oneshot(request).await.unwrap()
}

async fn body_bytes(response: Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

async fn get_json(app: Router, uri: &str) -> (axum::http::StatusCode, Value) {
    let response = send(app, get_request(uri)).await;
    let status = response.status();
    let bytes = body_bytes(response).await;
    (status, serde_json::from_slice(&bytes).unwrap())
}

/// Same, but authenticated — the envelope's JSON still pins.
async fn get_json_authed(app: Router, uri: &str) -> (axum::http::StatusCode, Value) {
    let request = Request::builder()
        .uri(uri)
        .header("authorization", "Bearer good")
        .body(Body::empty())
        .unwrap();
    let response = send(app, request).await;
    let status = response.status();
    let bytes = body_bytes(response).await;
    (status, serde_json::from_slice(&bytes).unwrap())
}

// ---------------------------------------------------------------------
// The envelope
// ---------------------------------------------------------------------

#[tokio::test]
async fn envelope_renders_from_a_handler_error() {
    async fn handler() -> Result<&'static str, HttpError> {
        Err(HttpError::not_found("USER_NOT_FOUND", "no such user"))
    }
    let app = Router::new().route("/thing", get(handler));
    let (status, body) = get_json(app, "/thing").await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
    assert_eq!(
        body,
        json!({"code": "NOT_FOUND", "reason": "USER_NOT_FOUND", "message": "no such user"})
    );
}

// ---------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------

#[tokio::test]
async fn recovery_answers_the_envelope_without_leaking_the_panic() {
    let app = with_recovery(protected_router().route(
        "/boom",
        get(|| async {
            panic!("handler detail");
            #[allow(unreachable_code)]
            "unreachable"
        }),
    ));
    let (status, body) = get_json(app, "/boom").await;
    assert_eq!(status, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["reason"], json!(REASON_PANIC));
    assert_eq!(body["message"], json!("internal server error"));
}

/// A panic reporter capturing the context the edge scopes onto each
/// request — the wire between the recovery layer and the process-wide
/// rushwind-panic hook.
struct ContextRecorder {
    reports: std::sync::Mutex<Vec<rushwind_panic::PanicReport>>,
}

impl rushwind_panic::PanicReporter for ContextRecorder {
    fn report(&self, report: &rushwind_panic::PanicReport) {
        self.reports
            .lock()
            .expect("recorder poisoned")
            .push(report.clone());
    }
}

#[tokio::test]
async fn handler_panics_report_with_the_request_identity() {
    let recorder = Arc::new(ContextRecorder {
        reports: std::sync::Mutex::new(Vec::new()),
    });
    let _guard = rushwind_panic::plug(recorder.clone() as Arc<dyn rushwind_panic::PanicReporter>);

    let app = HttpEdge::new().wrap(protected_router().route(
        "/boom",
        get(|| async {
            panic!("ctx-boom");
            #[allow(unreachable_code)]
            "unreachable"
        }),
    ));
    let (status, body) = get_json(app, "/boom").await;
    assert_eq!(status, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["reason"], json!(REASON_PANIC));

    // The report carries the request identity: request-id outermost
    // (the edge's assembled order) lets recovery read the extension.
    let reports = recorder.reports.lock().expect("recorder poisoned");
    let report = reports
        .iter()
        .find(|r| r.message.contains("ctx-boom"))
        .expect("the panic report delivered");
    let context = &report.context;
    assert!(context.contains(&("method".to_owned(), "GET".to_owned())));
    assert!(context.contains(&("path".to_owned(), "/boom".to_owned())));
    let request_id = context
        .iter()
        .find(|(k, _)| k == "request_id")
        .map(|(_, v)| v.clone())
        .expect("request id in the panic context");
    assert!(!request_id.is_empty());
}

// ---------------------------------------------------------------------
// Request id
// ---------------------------------------------------------------------

#[tokio::test]
async fn request_id_is_echoed_and_stamped_on_the_response() {
    let app = with_request_id(protected_router().route(
        "/whoami",
        get(|id: Extension<RequestId>| async move { id.0.as_str().to_owned() }),
    ));

    let mut request = get_request("/whoami");
    request
        .headers_mut()
        .insert("x-request-id", "my-id-42".parse().unwrap());
    let response = send(app.clone(), request).await;
    assert_eq!(response.headers()["x-request-id"], "my-id-42");
    assert_eq!(&body_bytes(response).await[..], b"my-id-42");

    // No inbound header: the plain route still gets a minted stamp.
    let response = send(app, get_request("/thing")).await;
    let minted = response.headers()["x-request-id"].to_str().unwrap();
    assert_eq!(minted.len(), 32);
    assert!(minted.chars().all(|c| c.is_ascii_hexdigit()));
}

// ---------------------------------------------------------------------
// Timeout
// ---------------------------------------------------------------------

#[tokio::test]
async fn timeout_answers_504_when_the_budget_runs_out() {
    let app = with_timeout(
        protected_router().route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                "slow"
            }),
        ),
        Duration::from_millis(20),
    );
    let (status, body) = get_json(app, "/slow").await;
    assert_eq!(status, axum::http::StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(body["code"], json!("DEADLINE_EXCEEDED"));
    assert_eq!(body["reason"], json!(REASON_DEADLINE_EXCEEDED));
}

// ---------------------------------------------------------------------
// CORS
// ---------------------------------------------------------------------

#[tokio::test]
async fn cors_answers_preflight_and_stamps_actual_requests() {
    let options = CorsOptions::default()
        .with_allow_origin("https://admin.example")
        .with_allow_credentials(true)
        .with_allow_method("PATCH")
        .with_allow_header("x-captcha-value");
    let app = with_cors(protected_router(), options);

    let preflight = send(
        app.clone(),
        Request::builder()
            .method("OPTIONS")
            .uri("/thing")
            .header("origin", "https://admin.example")
            .header("access-control-request-method", "PATCH")
            .header("access-control-request-headers", "x-captcha-value")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(preflight.status().is_success());
    assert_eq!(
        preflight.headers()["access-control-allow-origin"],
        "https://admin.example"
    );
    assert_eq!(
        preflight.headers()["access-control-allow-credentials"],
        "true"
    );

    let actual = send(
        app,
        Request::builder()
            .uri("/thing")
            .header("origin", "https://admin.example")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        actual.headers()["access-control-allow-origin"],
        "https://admin.example"
    );
}

#[tokio::test]
async fn cors_star_with_credentials_mirrors_the_request_origin() {
    let options = CorsOptions::default()
        .with_allow_origin("*")
        .with_allow_credentials(true);
    let app = with_cors(protected_router(), options);
    let response = send(
        app,
        Request::builder()
            .uri("/thing")
            .header("origin", "https://anywhere.example")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        response.headers()["access-control-allow-origin"],
        "https://anywhere.example"
    );
}

// ---------------------------------------------------------------------
// The authn bridge
// ---------------------------------------------------------------------

#[tokio::test]
async fn authn_rejects_missing_and_invalid_credentials_with_the_taxonomy_reasons() {
    let app = with_authn(protected_router(), Arc::new(StubAuthn));

    let (status, body) = get_json(app.clone(), "/thing").await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
    assert_eq!(body["reason"], json!("AUTHN_MISSING_BEARER_TOKEN"));

    let for_wrong = send(
        app.clone(),
        Request::builder()
            .uri("/thing")
            .header("authorization", "Bearer wrong")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(for_wrong.status(), axum::http::StatusCode::UNAUTHORIZED);

    let for_expired = send(
        app,
        Request::builder()
            .uri("/thing")
            .header("authorization", "Bearer expired")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(for_expired.status(), axum::http::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn authn_configuration_failures_answer_500() {
    let app = with_authn(protected_router(), Arc::new(StubAuthn));
    let response = send(
        app,
        Request::builder()
            .uri("/thing")
            .header("authorization", "Bearer misconfigured")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        response.status(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn authn_inserts_claims_for_the_extractor() {
    async fn whoami(claims: Authenticated) -> String {
        claims.0.get_subject().unwrap()
    }
    let app = with_authn(
        Router::new().route("/whoami", get(whoami)),
        Arc::new(StubAuthn),
    );
    let response = send(
        app,
        Request::builder()
            .uri("/whoami")
            .header("authorization", "Bearer good")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    assert_eq!(&body_bytes(response).await[..], b"alice");
}

#[tokio::test]
async fn optional_authenticator_tolerates_anonymous_callers() {
    async fn optional(who: OptionalAuthenticated) -> String {
        match who.0 {
            Some(claims) => claims.get_subject().unwrap(),
            None => "anonymous".to_owned(),
        }
    }

    // Without the authn layer the extractor stays None — never a
    // rejection.
    let public = Router::new().route("/thing", get(optional));
    let response = send(public.clone(), get_request("/thing")).await;
    assert_eq!(&body_bytes(response).await[..], b"anonymous");

    // With the layer, a valid credential surfaces as Some.
    let protected = with_authn(public, Arc::new(StubAuthn));
    let response = send(protected, authed_request()).await;
    assert_eq!(&body_bytes(response).await[..], b"alice");
}

// ---------------------------------------------------------------------
// The authz bridge
// ---------------------------------------------------------------------

#[tokio::test]
async fn authorization_requires_the_authn_layer_first() {
    let app = with_authorization(
        protected_router(),
        Arc::new(StubEngine {
            verdict: true,
            fail: false,
        }),
        "read",
        "thing",
    );
    let (status, body) = get_json(app, "/thing").await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
    assert_eq!(body["reason"], json!(REASON_UNAUTHENTICATED));
}

#[tokio::test]
async fn authorization_enforces_the_permission_point() {
    let authn = Arc::new(StubAuthn) as Arc<dyn Authenticator>;
    let allow = with_authn(
        with_authorization(
            protected_router(),
            Arc::new(StubEngine {
                verdict: true,
                fail: false,
            }),
            "read",
            "thing",
        ),
        authn.clone(),
    );
    let response = send(allow, authed_request()).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let deny = with_authn(
        with_authorization(
            protected_router(),
            Arc::new(StubEngine {
                verdict: false,
                fail: false,
            }),
            "read",
            "thing",
        ),
        authn,
    );
    let (status, body) = get_json_authed(deny, "/thing").await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN);
    assert_eq!(body["reason"], json!(REASON_PERMISSION_DENIED));
}

#[tokio::test]
async fn authorization_reads_the_project_from_a_claim() {
    let authn = Arc::new(StubAuthn) as Arc<dyn Authenticator>;
    // The stub engine denies everything; the point is that the claim
    // path evaluates without tripping the no-claims rejection.
    let app = with_authn(
        with_authorization_claim(
            protected_router(),
            Arc::new(StubEngine {
                verdict: false,
                fail: false,
            }),
            "read",
            "thing",
            "tenant",
        ),
        authn,
    );
    let (status, body) = get_json_authed(app, "/thing").await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN);
    assert_eq!(body["reason"], json!(REASON_PERMISSION_DENIED));
}

#[tokio::test]
async fn authorization_engine_errors_follow_the_taxonomy_anchor() {
    let app = with_authn(
        with_authorization(
            protected_router(),
            Arc::new(StubEngine {
                verdict: true,
                fail: true,
            }),
            "read",
            "thing",
        ),
        Arc::new(StubAuthn),
    );
    let (status, body) = get_json_authed(app, "/thing").await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN);
    assert_eq!(body["reason"], json!("AUTHZ_INVALID_CLAIMS"));
}

// ---------------------------------------------------------------------
// The assembled stack
// ---------------------------------------------------------------------

fn protected_router() -> Router {
    Router::new().route("/thing", get(|| async { "ok" }))
}

#[tokio::test]
async fn the_edge_applies_the_default_stack() {
    let app = HttpEdge::new().wrap(protected_router());
    let response = send(app, get_request("/thing")).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    // The request-id layer sits outside recovery, outside the route:
    // its header on the answer proves the chain assembled.
    assert!(response.headers().get(HEADER_X_REQUEST_ID).is_some());
}

#[tokio::test]
async fn the_edge_respects_opt_outs_and_additions() {
    let app = HttpEdge::new()
        .without_logging()
        .without_request_id()
        .with_timeout(Duration::from_secs(5))
        .wrap(protected_router());
    let response = send(app, get_request("/thing")).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    assert!(response.headers().get(HEADER_X_REQUEST_ID).is_none());
}

#[tokio::test]
async fn standalone_recovery_reports_method_and_path_only() {
    // Standalone recovery — no request-id layer — falls back to
    // method/path context; the request_id key must be absent, not
    // empty.
    let recorder = Arc::new(ContextRecorder {
        reports: std::sync::Mutex::new(Vec::new()),
    });
    let _guard = rushwind_panic::plug(recorder.clone() as Arc<dyn rushwind_panic::PanicReporter>);

    let app = with_recovery(protected_router().route(
        "/solo-boom",
        get(|| async {
            panic!("solo-boom");
            #[allow(unreachable_code)]
            "unreachable"
        }),
    ));
    let (status, body) = get_json(app, "/solo-boom").await;
    assert_eq!(status, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["reason"], json!(REASON_PANIC));

    let reports = recorder.reports.lock().expect("recorder poisoned");
    let report = reports
        .iter()
        .find(|r| r.message.contains("solo-boom"))
        .expect("the panic report delivered");
    assert!(report.context.contains(&("method".to_owned(), "GET".to_owned())));
    assert!(report.context.contains(&("path".to_owned(), "/solo-boom".to_owned())));
    assert!(
        !report.context.iter().any(|(k, _)| k == "request_id"),
        "standalone recovery has no request id to attach"
    );
}
