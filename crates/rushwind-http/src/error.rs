//! The error envelope for every HTTP rejection.
//!
//! A gRPC-aligned [`Code`] maps to the HTTP status, the stable `reason`
//! string is the i18n key the frontend substitutes, `message` is
//! human-readable, `details` carries optional
//! structured context. [`HttpError::into_response`] renders the JSON
//! envelope every rejection shares.

use std::borrow::Cow;
use std::fmt;

use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::Value;

use rushwind_authn::AuthnError;
use rushwind_storage::StorageError;

/// The reason stamped on panic recoveries: the handler died, the client
/// gets no detail.
pub const REASON_PANIC: &str = "INTERNAL_PANIC";
/// The reason for a request that exceeded its time budget.
pub const REASON_DEADLINE_EXCEEDED: &str = "DEADLINE_EXCEEDED";
/// The reason for rejections where no claims extension is present — the
/// authn layer did not run, or ran and failed silently.
pub const REASON_UNAUTHENTICATED: &str = "AUTHN_UNAUTHENTICATED";
/// The reason a malformed storage filter/query surfaces under.
pub const REASON_INVALID_QUERY: &str = "INVALID_QUERY";

/// The error class — the codes align with gRPC's;
/// [`Code::status`] is the HTTP status mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Code {
    /// The request is malformed — 400.
    BadRequest,
    /// No credential, or an invalid one — 401.
    Unauthorized,
    /// Authenticated, but not allowed — 403.
    Forbidden,
    /// The target does not exist, or is out of the viewer's scope — 404.
    NotFound,
    /// The write violates a constraint — 409.
    Conflict,
    /// The caller is sending too fast — 429.
    TooManyRequests,
    /// Something broke on this side — 500.
    Internal,
    /// The capability does not exist here — 501.
    NotImplemented,
    /// The dependency is down — 503.
    Unavailable,
    /// The time budget ran out — 504.
    DeadlineExceeded,
}

impl Code {
    /// The HTTP status this code maps to.
    pub const fn status(self) -> u16 {
        match self {
            Code::BadRequest => 400,
            Code::Unauthorized => 401,
            Code::Forbidden => 403,
            Code::NotFound => 404,
            Code::Conflict => 409,
            Code::TooManyRequests => 429,
            Code::Internal => 500,
            Code::NotImplemented => 501,
            Code::Unavailable => 503,
            Code::DeadlineExceeded => 504,
        }
    }

    /// The stable wire string for the envelope's `code` field — the
    /// gRPC code name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Code::BadRequest => "BAD_REQUEST",
            Code::Unauthorized => "UNAUTHENTICATED",
            Code::Forbidden => "PERMISSION_DENIED",
            Code::NotFound => "NOT_FOUND",
            Code::Conflict => "ALREADY_EXISTS",
            Code::TooManyRequests => "RESOURCE_EXHAUSTED",
            Code::Internal => "INTERNAL",
            Code::NotImplemented => "UNIMPLEMENTED",
            Code::Unavailable => "UNAVAILABLE",
            Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        }
    }
}

/// The one error shape every HTTP rejection returns: `code`, `reason`,
/// `message`, `details` — with no server-side
/// `Cause`/`StackTrace` on the wire (those belong to the log, not the
/// wire).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct HttpError {
    /// The error class; anchors the HTTP status.
    pub code: Code,
    /// The stable machine-readable reason — the i18n key the frontend
    /// substitutes. Messages may change; reasons do not.
    pub reason: Cow<'static, str>,
    /// The human-readable description.
    pub message: String,
    /// Optional structured context (e.g. the offending field). Boxed so
    /// the error stays small — every handler's `Result` carries it.
    pub details: Option<Box<Value>>,
}

impl HttpError {
    /// Builds an error from its parts.
    pub fn new(
        code: Code,
        reason: impl Into<Cow<'static, str>>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            reason: reason.into(),
            message: message.into(),
            details: None,
        }
    }

    /// Attaches structured details.
    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(Box::new(details));
        self
    }

    /// A 400 rejection.
    pub fn bad_request(reason: impl Into<Cow<'static, str>>, message: impl Into<String>) -> Self {
        Self::new(Code::BadRequest, reason, message)
    }

    /// A 401 rejection.
    pub fn unauthorized(reason: impl Into<Cow<'static, str>>, message: impl Into<String>) -> Self {
        Self::new(Code::Unauthorized, reason, message)
    }

    /// A 403 rejection.
    pub fn forbidden(reason: impl Into<Cow<'static, str>>, message: impl Into<String>) -> Self {
        Self::new(Code::Forbidden, reason, message)
    }

    /// A 404 rejection.
    pub fn not_found(reason: impl Into<Cow<'static, str>>, message: impl Into<String>) -> Self {
        Self::new(Code::NotFound, reason, message)
    }

    /// A 409 rejection.
    pub fn conflict(reason: impl Into<Cow<'static, str>>, message: impl Into<String>) -> Self {
        Self::new(Code::Conflict, reason, message)
    }

    /// A 429 rejection.
    pub fn too_many_requests(
        reason: impl Into<Cow<'static, str>>,
        message: impl Into<String>,
    ) -> Self {
        Self::new(Code::TooManyRequests, reason, message)
    }

    /// A 500 rejection.
    pub fn internal(reason: impl Into<Cow<'static, str>>, message: impl Into<String>) -> Self {
        Self::new(Code::Internal, reason, message)
    }

    /// A 501 rejection.
    pub fn not_implemented(
        reason: impl Into<Cow<'static, str>>,
        message: impl Into<String>,
    ) -> Self {
        Self::new(Code::NotImplemented, reason, message)
    }

    /// A 503 rejection.
    pub fn unavailable(reason: impl Into<Cow<'static, str>>, message: impl Into<String>) -> Self {
        Self::new(Code::Unavailable, reason, message)
    }

    /// A 504 rejection.
    pub fn deadline_exceeded(
        reason: impl Into<Cow<'static, str>>,
        message: impl Into<String>,
    ) -> Self {
        Self::new(Code::DeadlineExceeded, reason, message)
    }

    /// The HTTP status the code anchors.
    pub const fn status(&self) -> u16 {
        self.code.status()
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}: {}",
            self.code.as_str(),
            self.reason,
            self.message
        )
    }
}

impl std::error::Error for HttpError {}

/// The JSON wire shape of a rejection.
/// Borrowing keeps the render allocation-free beyond the body itself.
#[derive(Debug, Serialize)]
pub struct ErrorEnvelope<'a> {
    /// The class string, e.g. `"NOT_FOUND"`.
    pub code: &'static str,
    /// The stable reason, e.g. `"USER_NOT_FOUND"`.
    pub reason: &'a str,
    /// The human-readable message.
    pub message: &'a str,
    /// The optional structured details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl<'a> From<&'a HttpError> for ErrorEnvelope<'a> {
    fn from(error: &'a HttpError) -> Self {
        Self {
            code: error.code.as_str(),
            reason: &error.reason,
            message: &error.message,
            details: error.details.as_deref().cloned(),
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = axum::http::StatusCode::from_u16(self.status())
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        (status, axum::Json(ErrorEnvelope::from(&self))).into_response()
    }
}

/// The handler-facing result alias.
pub type HttpResult<T> = Result<T, HttpError>;

impl From<AuthnError> for HttpError {
    fn from(error: AuthnError) -> Self {
        // The taxonomy anchors its own statuses: 401 for authentication
        // failures, 500 for configuration failures; anything else is a
        // bug in the table, and internal is the safe landing.
        let code = match error.status() {
            401 => Code::Unauthorized,
            _ => Code::Internal,
        };
        Self::new(code, error.code(), error.to_string())
    }
}

impl From<StorageError> for HttpError {
    fn from(error: StorageError) -> Self {
        // The taxonomy's variant set maps one-to-one onto the classes;
        // `Unsupported` is a capability the endpoint does not offer, so
        // it lands on 501 rather than 500.
        match &error {
            StorageError::NotFound => Self::not_found("NOT_FOUND", error.to_string()),
            StorageError::MissingViewer => {
                // Tenancy enforcement failed closed: the request carried no
                // viewer identity against a tenant-scoped table — a caller
                // misconfiguration, a 4xx and never a 5xx.
                Self::bad_request("MISSING_VIEWER", error.to_string())
            }
            StorageError::InvalidQuery(detail) => {
                Self::bad_request(REASON_INVALID_QUERY, format!("invalid query: {detail}"))
            }
            StorageError::Conflict(_) => Self::conflict("CONFLICT", error.to_string()),
            StorageError::Unsupported(_) => Self::not_implemented("UNSUPPORTED", error.to_string()),
            StorageError::Timeout => {
                Self::deadline_exceeded(REASON_DEADLINE_EXCEEDED, error.to_string())
            }
            StorageError::Backend(_) => Self::internal("BACKEND_FAILURE", error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn code_status_table_matches_the_go_mapping() {
        for (code, status) in [
            (Code::BadRequest, 400),
            (Code::Unauthorized, 401),
            (Code::Forbidden, 403),
            (Code::NotFound, 404),
            (Code::Conflict, 409),
            (Code::TooManyRequests, 429),
            (Code::Internal, 500),
            (Code::NotImplemented, 501),
            (Code::Unavailable, 503),
            (Code::DeadlineExceeded, 504),
        ] {
            assert_eq!(code.status(), status, "{code:?}");
        }
    }

    #[test]
    fn code_wire_strings_are_the_grpc_names() {
        assert_eq!(Code::Unauthorized.as_str(), "UNAUTHENTICATED");
        assert_eq!(Code::Conflict.as_str(), "ALREADY_EXISTS");
        assert_eq!(Code::TooManyRequests.as_str(), "RESOURCE_EXHAUSTED");
        assert_eq!(Code::DeadlineExceeded.as_str(), "DEADLINE_EXCEEDED");
    }

    #[test]
    fn envelope_serializes_without_absent_details() {
        let error = HttpError::not_found("USER_NOT_FOUND", "no such user");
        let envelope = serde_json::to_value(ErrorEnvelope::from(&error)).unwrap();
        assert_eq!(
            envelope,
            json!({
                "code": "NOT_FOUND",
                "reason": "USER_NOT_FOUND",
                "message": "no such user",
            })
        );
    }

    #[test]
    fn envelope_carries_details_when_present() {
        let error = HttpError::bad_request("VALIDATION_FAILED", "bad input")
            .with_details(json!({ "field": "age" }));
        let envelope = serde_json::to_value(ErrorEnvelope::from(&error)).unwrap();
        assert_eq!(envelope["details"], json!({ "field": "age" }));
        assert_eq!(error.status(), 400);
    }

    #[test]
    fn display_leads_with_code_and_reason() {
        let error = HttpError::deadline_exceeded(REASON_DEADLINE_EXCEEDED, "too slow");
        assert_eq!(
            error.to_string(),
            "DEADLINE_EXCEEDED/DEADLINE_EXCEEDED: too slow"
        );
    }

    #[test]
    fn authn_errors_map_by_their_status_anchor() {
        let missing = HttpError::from(AuthnError::MissingBearerToken);
        assert_eq!(missing.code, Code::Unauthorized);
        assert_eq!(missing.reason, "AUTHN_MISSING_BEARER_TOKEN");

        let configured_wrong = HttpError::from(AuthnError::GetKeyFailed);
        assert_eq!(configured_wrong.code, Code::Internal);
        assert_eq!(configured_wrong.reason, "AUTHN_GET_KEY_FAILED");
    }

    #[test]
    fn storage_errors_map_by_variant() {
        let cases = [
            (StorageError::NotFound, Code::NotFound, "NOT_FOUND"),
            (
                StorageError::InvalidQuery("bad op".into()),
                Code::BadRequest,
                REASON_INVALID_QUERY,
            ),
            (
                StorageError::Conflict("dup".into()),
                Code::Conflict,
                "CONFLICT",
            ),
            (
                StorageError::Unsupported("cursor".into()),
                Code::NotImplemented,
                "UNSUPPORTED",
            ),
            (
                StorageError::Timeout,
                Code::DeadlineExceeded,
                REASON_DEADLINE_EXCEEDED,
            ),
            (
                StorageError::Backend("boom".into()),
                Code::Internal,
                "BACKEND_FAILURE",
            ),
        ];
        for (error, code, reason) in cases {
            let mapped = HttpError::from(error);
            assert_eq!(mapped.code, code, "{reason}");
            assert_eq!(mapped.reason, reason);
        }
    }
}
