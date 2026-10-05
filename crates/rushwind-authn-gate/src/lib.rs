//! The auth gate's session stage — the layer both deployment auth
//! crates ran by copy-paste.
//!
//! Wire order of a protected request: the engine's
//! `Authenticate(ACCESS)` (signature + expiry) → the session stage
//! (the Redis whitelist `at:{ct}:{uid}:{jti}` exact compare + the
//! blacklist `bl:{jti}` existence). This crate carries the stage's
//! contract ([`AccessTokenChecker`]) and its head
//! ([`authenticate_and_check_session`]); the later stages (the tenant
//! gate, the authorization evaluator) compose on top of the returned
//! claims in the deployment's own gate. Failures are classes, not
//! envelopes — the reason → HTTP status anchor is each deployment's
//! own error tables, so the envelope rendering stays deployment-side.
//!
//! Two stage-feed utilities ride here: [`trace_id`] (the `traceparent`
//! → `X-Request-Id` resolution the authorization/audit trails
//! correlate on) and [`parse_unverified_bearer_jwt`] (the refresh
//! fallback's claim sniff — the unverified payload only NAMES the
//! refresh binding key; the refresh token value itself is validated
//! elsewhere).

use async_trait::async_trait;
use rushwind_authn::{AuthClaims, Authenticator};

/// The server-side session checks (Redis whitelist/blacklist), split
/// out of the engine so the storage stays service-side.
#[async_trait]
pub trait AccessTokenChecker: Send + Sync {
    /// `at:{ct}:{uid}:{jti}` exact-match — false means revoked/expired.
    async fn is_valid_access_token(&self, uid: u32, jti: &str, token: &str) -> bool;
    /// `bl:{jti}` existence.
    async fn is_blocked_access_token(&self, jti: &str) -> bool;
}

/// The session stage's failure classes, carrying the branch's fixed
/// message text — the wire texts the deployments pin: `missing bearer
/// token` for the absent credential, `access token expired` for every
/// session-stage rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    MissingBearer,
    InvalidOrExpired,
}

impl SessionError {
    /// The branch's fixed message text.
    pub fn message(&self) -> &'static str {
        match self {
            SessionError::MissingBearer => "missing bearer token",
            SessionError::InvalidOrExpired => "access token expired",
        }
    }
}

/// The head of every protected request: the engine's authenticate +
/// the session stage. `Ok` carries the claim bag to inject into the
/// request extensions (the request-context injection the binding glue
/// consumes); `Err` carries the failure class for the deployment's
/// envelope.
pub async fn authenticate_and_check_session(
    auth: &dyn Authenticator,
    checker: &dyn AccessTokenChecker,
    headers: &[(String, String)],
    bearer: Option<&str>,
) -> Result<AuthClaims, SessionError> {
    let claims = auth.authenticate(headers).map_err(|e| match e {
        rushwind_authn::AuthnError::MissingBearerToken => SessionError::MissingBearer,
        _ => SessionError::InvalidOrExpired,
    })?;

    let uid = claims
        .0
        .get("uid")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(0);
    let jti = claims.get_jwt_id().unwrap_or_default();
    let valid = if jti.is_empty() {
        false
    } else {
        checker
            .is_valid_access_token(uid, &jti, bearer.unwrap_or_default())
            .await
            && !checker.is_blocked_access_token(&jti).await
    };
    if !valid {
        return Err(SessionError::InvalidOrExpired);
    }
    Ok(claims)
}

/// The trace id: `traceparent`'s trace segment (the second field, 32
/// hex chars), else `X-Request-Id` — the same source the audit layer
/// logs, so one request correlates across trails.
pub fn trace_id(headers: &axum::http::HeaderMap) -> String {
    if let Some(tp) = headers
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        let segments: Vec<&str> = tp.split('-').collect();
        if segments.len() >= 2 && segments[1].len() == 32 {
            return segments[1].to_owned();
        }
    }
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("")
        .to_owned()
}

/// The refresh fallback's claim sniff: decodes the JWT payload WITHOUT
/// verifying the signature. The (possibly expired) access token is not
/// a credential here — it only names the refresh binding key (uid/jti);
/// the core validates the refresh token value itself. Mirrors the
/// reference's ParseUnverifiedBearerJWTClaims.
pub fn parse_unverified_bearer_jwt(token: &str) -> Option<(u32, String)> {
    let payload_b64 = token.split('.').nth(1)?;
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload_b64))
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let uid = match claims.get("uid")? {
        serde_json::Value::Number(n) => n.as_u64()? as u32,
        serde_json::Value::String(s) => s.parse().ok()?,
        _ => return None,
    };
    if uid == 0 {
        return None;
    }
    let jti = claims.get("jti")?.as_str()?.to_string();
    Some((uid, jti))
}
