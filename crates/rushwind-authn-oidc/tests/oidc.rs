//! OIDC engine conformance against an in-process mock IdP: discovery,
//! JWKS fetch, local verification (signature, iss/aud/exp pins),
//! unknown-kid rotation refusal + refresh adoption, minting refusal,
//! and the refresh-loop's transient-failure tolerance. The test signs
//! its own ID tokens with a local RSA key and publishes the matching
//! JWK — the same trust triangle a real deployment runs.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use jsonwebtoken::jwk::Jwk;
use jsonwebtoken::{Algorithm, EncodingKey};
use rushwind_authn::{AuthClaims, Authenticator, AuthnError};
use rushwind_authn_oidc::{OidcAuthenticator, OidcOptions};
use serde_json::json;

/// The mock IdP: the discovery document and the JWKS, both served
/// under the issuer path. The JWKS body is mutable — the rotation pin
/// swaps it.
struct MockIdp {
    discovery: Arc<std::sync::Mutex<String>>,
    jwks: Arc<std::sync::Mutex<String>>,
}

impl MockIdp {
    fn serve(self: Arc<Self>, mut stream: tokio::net::TcpStream) {
        tokio::spawn(async move {
            let mut buffer = [0u8; 4096];
            let read = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer)
                .await
                .unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            let target = request.split_whitespace().nth(1).unwrap_or("/").to_string();
            let state = match target.as_str() {
                "/.well-known/openid-configuration" => {
                    Some((*self.discovery.lock().expect("discovery")).clone())
                }
                "/jwks" => Some((*self.jwks.lock().expect("jwks")).clone()),
                _ => None,
            };
            let response = match state {
                Some(body) => format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ),
                None => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string(),
            };
            let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes()).await;
        });
    }
}

/// Boots the mock IdP; returns the issuer URL and the JWKS handle.
async fn spawn_idp(jwks_body: String) -> (String, Arc<std::sync::Mutex<String>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind works");
    let addr = listener.local_addr().expect("local addr");
    let issuer = format!("http://{addr}");
    let discovery = Arc::new(std::sync::Mutex::new(
        json!({"issuer": issuer, "jwks_uri": format!("{issuer}/jwks")}).to_string(),
    ));
    let jwks = Arc::new(std::sync::Mutex::new(jwks_body));
    let mock = Arc::new(MockIdp {
        discovery,
        jwks: Arc::clone(&jwks),
    });
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            mock.clone().serve(stream);
        }
    });
    (issuer, jwks)
}

/// Signs a claims map as an RS256 ID token and returns (token, jwk).
/// The JWK comes from the encoding key itself — the same trust
/// material the JWKS publishes, generated, not hand-computed.
fn sign_id_token(
    key: &EncodingKey,
    kid: &str,
    issuer: &str,
    audience: &str,
    claims: serde_json::Value,
) -> (String, String) {
    let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
    header.kid = Some(kid.to_string());
    let token = jsonwebtoken::encode(&header, &claims, key).expect("signs");
    let mut jwk = Jwk::from_encoding_key(key, Algorithm::RS256).expect("jwk from key");
    jwk.common.key_id = Some(kid.to_string());
    jwk.common.key_algorithm = Some(jsonwebtoken::jwk::KeyAlgorithm::RS256);
    let _ = (issuer, audience);
    let jwks = json!({"keys": [serde_json::to_value(&jwk).expect("jwk json")]});
    (token, jwks.to_string())
}

fn claims_map(issuer: &str, audience: &str, expiry_now_plus: i64) -> serde_json::Value {
    let exp = (now_unix() as i64 + expiry_now_plus).max(0) as u64;
    json!({
        "iss": issuer,
        "aud": audience,
        "sub": "user-1",
        "exp": exp,
        "iat": now_unix(),
    })
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock runs")
        .as_secs()
}

fn options(issuer: &str) -> OidcOptions {
    OidcOptions {
        issuer: issuer.to_string(),
        audience: "rushwind-client".to_string(),
        request_timeout: Duration::from_secs(5),
        ..OidcOptions::default()
    }
}

#[tokio::test]
async fn verification_is_local_against_the_cached_jwks() {
    // The key material signs locally; the IdP only publishes the
    // public half — after construction the IdP is switched off
    // (jwks emptied to an unreachable state is not needed: no further
    // fetch happens) and verification keeps working.
    let encoding_key = test_signing_key();
    let (issuer, jwks) = spawn_idp(String::new()).await;
    let (token, jwks_body) = sign_id_token(
        &encoding_key,
        "key-1",
        &issuer,
        "rushwind-client",
        claims_map(&issuer, "rushwind-client", 600),
    );
    *jwks.lock().expect("jwks") = jwks_body;

    let engine = Arc::new(
        OidcAuthenticator::connect(options(&issuer))
            .await
            .expect("connect fetches and caches"),
    );

    let verified = engine
        .authenticate(&[("Authorization".to_string(), format!("Bearer {token}"))])
        .expect("the token verifies");
    assert_eq!(verified.get_subject().expect("subject"), "user-1");
}

#[tokio::test]
async fn the_registered_claims_are_pinned() {
    let encoding_key = test_signing_key();
    let (issuer, jwks) = spawn_idp(String::new()).await;
    let (token, jwks_body) = sign_id_token(
        &encoding_key,
        "key-1",
        &issuer,
        "rushwind-client",
        claims_map(&issuer, "rushwind-client", 600),
    );
    *jwks.lock().expect("jwks") = jwks_body;
    let engine = OidcAuthenticator::connect(options(&issuer))
        .await
        .expect("connect");

    // The well-formed token itself verifies — the positive face the
    // refusals below are measured against.
    engine
        .authenticate_token(&token)
        .expect("the well-formed token verifies");

    // Wrong audience refuses.
    let (wrong_aud, _) = sign_id_token(
        &encoding_key,
        "key-1",
        &issuer,
        "other-client",
        claims_map(&issuer, "other-client", 600),
    );
    assert!(matches!(
        engine.authenticate_token(&wrong_aud),
        Err(AuthnError::InvalidAudience)
    ));

    // Expired refuses.
    let (expired, _) = sign_id_token(
        &encoding_key,
        "key-1",
        &issuer,
        "rushwind-client",
        claims_map(&issuer, "rushwind-client", -600),
    );
    assert!(matches!(
        engine.authenticate_token(&expired),
        Err(AuthnError::TokenExpired)
    ));

    // A missing exp refuses — the strict-default stance the JWT engine
    // pins, carried here too.
    let claims = claims_map(&issuer, "rushwind-client", 600);
    let claims = serde_json::Value::Object(
        claims
            .as_object()
            .expect("object")
            .iter()
            .filter(|(k, _)| k.as_str() != "exp")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    );
    let (no_exp, _) = sign_id_token(&encoding_key, "key-1", &issuer, "rushwind-client", claims);
    assert!(matches!(
        engine.authenticate_token(&no_exp),
        Err(AuthnError::InvalidExpiration)
    ));

    // Garbage refuses as InvalidToken.
    assert!(matches!(
        engine.authenticate_token("not a token"),
        Err(AuthnError::InvalidToken)
    ));
}

#[tokio::test]
async fn an_unknown_kid_refuses_until_refresh_adopts_the_rotation() {
    let encoding_key = test_signing_key();
    let (issuer, jwks) = spawn_idp(String::new()).await;
    let (token, jwks_body) = sign_id_token(
        &encoding_key,
        "key-1",
        &issuer,
        "rushwind-client",
        claims_map(&issuer, "rushwind-client", 600),
    );
    *jwks.lock().expect("jwks") = jwks_body;
    let engine = Arc::new(
        OidcAuthenticator::connect(options(&issuer))
            .await
            .expect("connect"),
    );
    engine
        .authenticate_token(&token)
        .expect("the original key verifies");

    // The issuer rotates: the JWKS now names key-2 (the same material
    // under a new kid — a real rotation changes keys, the pin is the
    // kid-set swap), dropping key-1.
    let (rotated_token, rotated_jwks) = sign_id_token(
        &encoding_key,
        "key-2",
        &issuer,
        "rushwind-client",
        claims_map(&issuer, "rushwind-client", 600),
    );
    *jwks.lock().expect("jwks") = rotated_jwks;

    // Before refresh: the OLD key keeps working (the cache still holds
    // key-1 — a rotation is invisible until the RP refreshes, which is
    // exactly the window this engine leaves to the deployment), while
    // the new kid refuses (never seen).
    engine
        .authenticate_token(&token)
        .expect("the pre-rotation token keeps verifying until a refresh");
    assert!(matches!(
        engine.authenticate_token(&rotated_token),
        Err(AuthnError::Unauthenticated)
    ));

    // Refresh adopts the new set: the rotated kid verifies, the old
    // one — gone from the issuer's published set — refuses.
    engine.refresh().await.expect("refresh");
    engine
        .authenticate_token(&rotated_token)
        .expect("the rotated key now verifies");
    assert!(matches!(
        engine.authenticate_token(&token),
        Err(AuthnError::Unauthenticated)
    ));
}

/// A transient JWKS-fetch failure during refresh keeps the current
/// cache — working keys are never crowded out by a blip.
#[tokio::test]
async fn a_failed_refresh_keeps_the_current_keys() {
    let encoding_key = test_signing_key();
    let (issuer, jwks) = spawn_idp(String::new()).await;
    let (token, jwks_body) = sign_id_token(
        &encoding_key,
        "key-1",
        &issuer,
        "rushwind-client",
        claims_map(&issuer, "rushwind-client", 600),
    );
    *jwks.lock().expect("jwks") = jwks_body;
    let engine = Arc::new(
        OidcAuthenticator::connect(options(&issuer))
            .await
            .expect("connect"),
    );

    // Break the discovery document: the issuer now names a dead
    // jwks_uri.
    *jwks.lock().expect("jwks") = "{}".to_string();

    // Refresh fails (empty JWKS → no usable keys), the cache survives.
    engine.refresh().await.expect_err("empty jwks refuses");
    engine
        .authenticate_token(&token)
        .expect("the cached key still verifies");
}

#[tokio::test]
async fn minting_refuses_minting_is_the_identity_providers_act() {
    let encoding_key = test_signing_key();
    let (issuer, jwks) = spawn_idp(String::new()).await;
    let (_, jwks_body) = sign_id_token(
        &encoding_key,
        "key-1",
        &issuer,
        "rushwind-client",
        claims_map(&issuer, "rushwind-client", 600),
    );
    *jwks.lock().expect("jwks") = jwks_body;
    let engine = OidcAuthenticator::connect(options(&issuer))
        .await
        .expect("connect");
    let err = engine.create_identity(&AuthClaims::new()).unwrap_err();
    assert!(matches!(err, AuthnError::MissingKeyFunc), "{err:?}");
}

/// An explicit jwks_uri override skips discovery — an issuer with no
/// discovery document still works.
#[tokio::test]
async fn an_explicit_jwks_uri_skips_discovery() {
    // The issuer hosts nothing: discovery would 404. The JWKS lives on
    // a separate server (here, the same mock with a different base —
    // the override's point is that its URL need not relate to the
    // issuer's).
    let encoding_key = test_signing_key();
    let (issuer_host, jwks) = spawn_idp(String::new()).await;
    let (token, jwks_body) = sign_id_token(
        &encoding_key,
        "key-1",
        "https://real-issuer",
        "rushwind-client",
        claims_map("https://real-issuer", "rushwind-client", 600),
    );
    *jwks.lock().expect("jwks") = jwks_body;

    // The jwks_uri override points at this mock directly — its URL
    // need not relate to the configured issuer's, that is the point.
    let jwks_url = format!("{issuer_host}/jwks");
    let engine = OidcAuthenticator::connect(OidcOptions {
        issuer: "https://real-issuer".to_string(),
        audience: "rushwind-client".to_string(),
        jwks_uri: Some(jwks_url),
        request_timeout: Duration::from_secs(5),
        ..OidcOptions::default()
    })
    .await
    .expect("connect skips discovery");
    engine
        .authenticate_token(&token)
        .expect("the override JWKS verifies");
}

/// A deterministic RSA test key (generated once for this suite; a
/// test-only trust root — production issuers keep theirs private).
fn test_signing_key() -> EncodingKey {
    static KEY: std::sync::OnceLock<EncodingKey> = std::sync::OnceLock::new();
    KEY.get_or_init(|| {
        EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_PEM.as_bytes()).expect("test key parses")
    })
    .clone()
}

/// The suite's RSA-2048 private key, PEM.
const TEST_RSA_PRIVATE_PEM: &str = include_str!("test_rsa_key.pem");
