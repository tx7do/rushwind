//! The session stage against stub engines and checkers: the failure
//! classes and their fixed wire texts, the whitelist+blacklist
//! conjunction, and the utilities.

use async_trait::async_trait;
use rushwind_authn::{AuthClaims, Authenticator, AuthnError};
use rushwind_authn_gate::{
    authenticate_and_check_session, parse_unverified_bearer_jwt, trace_id, AccessTokenChecker,
    SessionError,
};
use std::sync::Mutex;

/// The stub engine: authenticates exactly one bearer value.
struct StubAuth {
    bearer: &'static str,
}

impl Authenticator for StubAuth {
    fn scheme(&self) -> &'static str {
        "Bearer"
    }

    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError> {
        if token == self.bearer {
            let mut map = serde_json::Map::new();
            map.insert("uid".into(), serde_json::json!(7));
            map.insert("jti".into(), serde_json::json!("j-1"));
            return Ok(AuthClaims(map));
        }
        if token.is_empty() {
            return Err(AuthnError::MissingBearerToken);
        }
        Err(AuthnError::Unauthenticated)
    }

    fn create_identity(&self, _claims: &AuthClaims) -> Result<String, AuthnError> {
        Err(AuthnError::Unauthenticated)
    }
}

/// The stub checker: whitelist admits `good`, `bl-jti` is blocked.
struct StubChecker {
    calls: Mutex<Vec<String>>,
}

#[async_trait]
impl AccessTokenChecker for StubChecker {
    async fn is_valid_access_token(&self, _uid: u32, jti: &str, token: &str) -> bool {
        self.calls
            .lock()
            .unwrap()
            .push(format!("valid:{jti}:{token}"));
        token == "good"
    }

    async fn is_blocked_access_token(&self, jti: &str) -> bool {
        self.calls.lock().unwrap().push(format!("blocked:{jti}"));
        jti == "bl-jti"
    }
}

fn headers(bearer: Option<&str>) -> Vec<(String, String)> {
    bearer
        .map(|b| vec![("authorization".to_string(), format!("Bearer {b}"))])
        .unwrap_or_default()
}

#[tokio::test]
async fn missing_bearer_class_carries_its_wire_text() {
    let err = authenticate_and_check_session(
        &StubAuth { bearer: "good" },
        &StubChecker {
            calls: Mutex::new(Vec::new()),
        },
        &headers(None),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err, SessionError::MissingBearer);
    assert_eq!(err.message(), "missing bearer token");
}

#[tokio::test]
async fn session_rejections_share_the_expired_text() {
    // Whitelist miss: the stored token differs.
    let checker = StubChecker {
        calls: Mutex::new(Vec::new()),
    };
    let err = authenticate_and_check_session(
        &StubAuth { bearer: "good" },
        &checker,
        &headers(Some("revoked")),
        Some("revoked"),
    )
    .await
    .unwrap_err();
    assert_eq!(err, SessionError::InvalidOrExpired);
    assert_eq!(err.message(), "access token expired");

    // Blacklist hit: valid but blocked.
    let checker = StubChecker {
        calls: Mutex::new(Vec::new()),
    };
    let err = authenticate_and_check_session(
        &StubAuth { bearer: "good" },
        &checker,
        &headers(Some("good-bl")),
        Some("good-bl"),
    )
    .await
    .unwrap_err();
    assert_eq!(err, SessionError::InvalidOrExpired);
}

#[tokio::test]
async fn happy_path_returns_the_claims_after_both_checks() {
    let checker = StubChecker {
        calls: Mutex::new(Vec::new()),
    };
    let claims = authenticate_and_check_session(
        &StubAuth { bearer: "good" },
        &checker,
        &headers(Some("good")),
        Some("good"),
    )
    .await
    .unwrap();
    assert_eq!(claims.0.get("uid").and_then(|v| v.as_u64()), Some(7));
    let calls = checker.calls.lock().unwrap();
    assert_eq!(
        *calls,
        vec!["valid:j-1:good".to_string(), "blocked:j-1".to_string()],
        "whitelist then blacklist, in order"
    );
}

#[test]
fn trace_id_reads_traceparent_then_request_id() {
    let mut h = axum::http::HeaderMap::new();
    assert_eq!(trace_id(&h), "");
    h.insert(
        "traceparent",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
            .parse()
            .unwrap(),
    );
    assert_eq!(trace_id(&h), "0af7651916cd43dd8448eb211c80319c");
    h.remove("traceparent");
    h.insert("x-request-id", "req-42".parse().unwrap());
    assert_eq!(trace_id(&h), "req-42");
}

#[test]
fn unverified_bearer_sniffs_uid_and_jti() {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    let payload = serde_json::json!({ "uid": 7u64, "jti": "abc-123" });
    let seg = URL_SAFE_NO_PAD.encode(payload.to_string());
    let token = format!("e30.{seg}.c2lg");
    assert_eq!(
        parse_unverified_bearer_jwt(&token),
        Some((7, "abc-123".to_string()))
    );
    // junk forms land None
    assert_eq!(parse_unverified_bearer_jwt("not-a-jwt"), None);
    assert_eq!(parse_unverified_bearer_jwt("a.####.c"), None);
    // zero uid is not an identity
    let zero = URL_SAFE_NO_PAD.encode(r#"{"uid":0,"jti":"x"}"#);
    assert_eq!(parse_unverified_bearer_jwt(&format!("h.{zero}.s")), None);
}
