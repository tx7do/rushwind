//! The `/events` handler and its preflight — the wire behavior minus
//! the authorization sequence, which rides the deployment's [`Gate`].

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use tokio::sync::broadcast;

use rushwind_http_binding::envelope::StatusError;

use crate::hub::Hub;

/// The authorization failure class: the reason literal rides the
/// deployment's status table (via [`SseApp::status_error`]) for the
/// envelope code — `UNAUTHORIZED` for credential failures, `FORBIDDEN`
/// for the blocked/rejected classes.
pub struct Failure {
    pub reason: &'static str,
    pub message: String,
}

impl Failure {
    pub fn new(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }
}

/// The deployment's authorize sequence — the HandleAuthorize
/// ValidateTokenRequest(ACCESS) shape: signature + expiry via the JWT
/// engine, then the deployment's validity gate (the local Redis
/// whitelist/blacklist store, or the core service's remote validation).
/// `Ok` carries the authenticated userId.
pub trait Gate: Send + Sync + 'static {
    fn authorize(
        &self,
        token: &str,
    ) -> impl std::future::Future<Output = Result<u32, Failure>> + Send;
}

/// The transport's application state: the hub, the deployment's gate,
/// and the table-anchored status constructor.
pub struct SseApp<A: Gate> {
    pub hub: Hub,
    pub gate: Arc<A>,
    /// The deployment's status constructor (reason → the error-table
    /// HTTP status) — `StatusError::new(status, reason, message)`.
    pub status_error: fn(&'static str, String) -> StatusError,
}

/// The token's three wire sources, in order: `Authorization: Bearer`,
/// `X-Token`, `?token=`.
pub fn extract_token(headers: &HeaderMap, query_token: Option<&String>) -> Option<String> {
    if let Some(t) = rushwind_http_binding::ctx::bearer_token(headers) {
        return Some(t);
    }
    if let Some(v) = headers.get("x-token").and_then(|v| v.to_str().ok()) {
        if !v.is_empty() {
            return Some(v.to_string());
        }
    }
    query_token.filter(|t| !t.is_empty()).cloned()
}

/// The OPTIONS preflight answer: a fixed CORS header set with no
/// authorization gate — the transport answers before the authorize
/// check.
pub async fn events_preflight() -> Response {
    (
        StatusCode::NO_CONTENT,
        [
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
            (header::ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS"),
            (
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                "Content-Type, Authorization, X-Token, Last-Event-ID",
            ),
            (header::ACCESS_CONTROL_MAX_AGE, "86400"),
        ],
    )
        .into_response()
}

/// The authorize-failure shape: an Unauthorized status with the
/// plain-text error line and the nosniff marker, no CORS headers (the
/// transport's SSE header pass has not run at that point). The body's
/// `code` carries the status-table value for the reason — 403 for the
/// forbidden-reason bodies — while the status line stays 401.
pub fn sse_error(err: StatusError) -> Response {
    let code = err.status;
    let reason = err.reason;
    let message = &err.message;
    (
        StatusCode::UNAUTHORIZED,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        format!(
            "error: code = {code} reason = {reason} message = {message} metadata = map[] cause = <nil>\n"
        ),
    )
        .into_response()
}

/// GET /events — authorize through the deployment's gate, then hold the
/// SSE stream open, forwarding this user's notifications.
pub async fn events<A: Gate>(
    State(state): State<Arc<SseApp<A>>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let error = |reason: &'static str, message: String| -> Response {
        sse_error((state.status_error)(reason, message))
    };

    let Some(token) = extract_token(&headers, params.get("token")) else {
        return error("UNAUTHORIZED", "invalid token".into());
    };

    // Signature + expiry and the deployment's validity gate — the
    // HandleAuthorize ValidateTokenRequest(ACCESS) sequence.
    let uid = match state.gate.authorize(&token).await {
        Ok(uid) => uid,
        Err(f) => return error(f.reason, f.message),
    };

    // The stream must be the token's own userId.
    if !params
        .get("stream")
        .and_then(|s| s.parse::<u32>().ok())
        .is_some_and(|s| s == uid)
    {
        return error("FORBIDDEN", "stream user mismatch".into());
    }

    let rx = state.hub.subscribe();
    let stream = futures::stream::unfold((rx, uid), |(mut rx, uid)| async move {
        loop {
            match rx.recv().await {
                Ok((user_id, payload)) => {
                    // Only this connection's own userId's payloads are
                    // forwarded — the per-stream subscription filter.
                    if user_id != uid || payload.is_empty() {
                        continue;
                    }
                    let event = Event::default()
                        .id(uuid::Uuid::new_v4().to_string())
                        .data(payload)
                        .event("notification");
                    return Some((Ok::<_, Infallible>(event), (rx, uid)));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    let mut response = Sse::new(stream).into_response();
    // The transport's SSE header pass: CORS pair + keep-alive on top of
    // the content-type/cache-control the SSE body already carries.
    let headers = response.headers_mut();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Content-Type"),
    );
    headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    response
}
