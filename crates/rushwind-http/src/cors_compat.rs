//! The gorilla-compatible CORS layer — byte-for-byte the ServeHTTP
//! behavior of `gorilla/handlers@v1.5.2`, for deployments that wire
//! their CORS through that package as a
//! server-level filter.
//!
//! It diverges from tower-http's CORS (the [`crate::cors`] path) in
//! emission rules, not just configuration:
//!
//! * preflight responses carry `Access-Control-Allow-Methods` **only for
//!   methods outside the package's internal default set** (GET/HEAD/POST)
//!   — the value is the single requested method, never the configured
//!   list;
//! * `Access-Control-Allow-Headers` reflects the *requested* headers that
//!   pass the configured list (the package's default headers are always
//!   allowed but never reflected), canonicalized and comma-joined;
//! * preflight responses carry no body; a preflight whose requested
//!   method/header fails the configured lists answers bare
//!   405/403 with no CORS headers; a missing
//!   `Access-Control-Request-Method` answers bare 400;
//! * an origin that fails the allow-list gets NO headers — and an OPTIONS
//!   request with a disallowed origin gets an empty 200 rather than the
//!   inner handler's response;
//! * `Vary: Origin` is set (replacing any existing Vary) when more than
//!   one origin is configured;
//! * the exposure header and max-age are never emitted (the reference
//!   wiring passes no such options).
//!
//! Non-preflight allowed requests continue to the inner handler with the
//! allow-origin/credentials headers attached.

use axum::extract::Request;
use axum::http::{header, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::Router;

use crate::cors::CorsOptions;

/// The package's internal default methods — a requested method in this
/// set gets NO `Access-Control-Allow-Methods` header.
const DEFAULT_METHODS: &[&str] = &["GET", "HEAD", "POST"];

/// The package's internal always-allowed headers — allowed but never
/// reflected in `Access-Control-Allow-Headers`.
const DEFAULT_HEADERS: &[&str] = &["Accept", "Accept-Language", "Content-Language", "Origin"];

/// The package's parse-time default headers, the base the (always-passed)
/// header option appends to.
const PARSE_DEFAULT_HEADERS: &[&str] = DEFAULT_HEADERS;

/// The normalized CORS state, per the package's option parsing: method
/// and header options REPLACE the parse defaults (the deployment always
/// passes them), origins collapse `*` to the singleton list, and the
/// never-wired options (expose, max-age) are fixed off.
struct CompatCors {
    allowed_methods: Vec<String>,
    allowed_headers: Vec<String>,
    allowed_origins: Vec<String>,
    allow_credentials: bool,
}

impl CompatCors {
    fn from_options(options: &CorsOptions) -> Self {
        // AllowedMethods: replace the parse default with the uppercased,
        // trimmed, deduplicated configured list (the deployment always
        // passes the option, so an empty list stays empty).
        let mut allowed_methods: Vec<String> = Vec::new();
        for value in &options.allow_methods {
            let normalized = value.trim().to_ascii_uppercase();
            if normalized.is_empty() || allowed_methods.iter().any(|m| m == &normalized) {
                continue;
            }
            allowed_methods.push(normalized);
        }

        // AllowedHeaders: APPEND the canonicalized configured list to the
        // package's always-allowed defaults.
        let mut allowed_headers: Vec<String> = PARSE_DEFAULT_HEADERS
            .iter()
            .map(|h| (*h).to_string())
            .collect();
        for value in &options.allow_headers {
            let canonical = canonical_header_key(value.trim());
            if canonical.is_empty() || allowed_headers.iter().any(|h| h == &canonical) {
                continue;
            }
            allowed_headers.push(canonical);
        }

        // AllowedOrigins: a `*` entry collapses the list to the singleton.
        let mut allowed_origins: Vec<String> = options.allow_origins.clone();
        if allowed_origins.iter().any(|o| o == "*") {
            allowed_origins = vec!["*".to_string()];
        }

        Self {
            allowed_methods,
            allowed_headers,
            allowed_origins,
            allow_credentials: options.allow_credentials,
        }
    }

    /// The package's `isMatch`: exact, case-sensitive membership.
    fn is_match(&self, needle: &str, haystack: &[String]) -> bool {
        haystack.iter().any(|v| v == needle)
    }

    /// The package's `isOriginAllowed`: empty origin never allowed; an
    /// empty configured list allows everything; otherwise exact match or
    /// a `*` entry.
    ///
    /// Note the deliberate divergence from [`crate::cors`]: the
    /// tower-http path treats an empty origin list as "CORS off", this
    /// compat layer — mirroring the reference Go package — treats it
    /// as "allow every origin". A deployment that reads `CorsOptions`'s
    /// "empty means off" doc and lands on the compat layer gets the
    /// wide-open semantics; configure an explicit origin list there.
    fn is_origin_allowed(&self, origin: &str) -> bool {
        if origin.is_empty() {
            return false;
        }
        if self.allowed_origins.is_empty() {
            return true;
        }
        self.allowed_origins.iter().any(|o| o == origin || o == "*")
    }

    /// The package's `returnOrigin`: `*` when nothing is configured,
    /// otherwise the request origin (the `*` entry short-circuits).
    fn return_origin(&self, origin: &str) -> String {
        if self.allowed_origins.is_empty() {
            return "*".to_string();
        }
        if self.allowed_origins.iter().any(|o| o == "*") {
            return "*".to_string();
        }
        origin.to_string()
    }
}

/// Canonicalizes a header key the way `http.CanonicalHeaderKey` does for
/// valid tokens: the first
/// letter and every letter after `-` uppercased, the rest lowercased;
/// anything else maps to the empty string (invalid tokens canonicalize
/// to "").
fn canonical_header_key(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper_next = true;
    for c in name.chars() {
        if !c.is_ascii_alphanumeric() && c != '-' {
            return String::new();
        }
        if c == '-' {
            out.push('-');
            upper_next = true;
        } else if upper_next {
            out.push(c.to_ascii_uppercase());
            upper_next = false;
        } else {
            out.push(c.to_ascii_lowercase());
        }
    }
    out
}

fn set_header(response: &mut Response, name: &'static str, value: String) {
    if let (Ok(name), Ok(value)) = (
        HeaderName::from_bytes(name.as_bytes()),
        HeaderValue::from_str(&value),
    ) {
        response.headers_mut().insert(name, value);
    }
}

/// The port of the package's ServeHTTP body.
async fn compat_run(state: CompatCors, req: Request, next: Next) -> Response {
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let is_options = req.method() == Method::OPTIONS;

    if !state.is_origin_allowed(&origin) {
        // Disallowed origin: no headers. An OPTIONS request is cut off
        // with an empty 200 (the package's bare return); anything else
        // continues to the inner handler untouched.
        if !is_options {
            return next.run(req).await;
        }
        return Response::builder()
            .status(StatusCode::OK)
            .body(axum::body::Body::empty())
            .unwrap_or_default();
    }

    if is_options {
        // The preflight branch. A missing request-method header answers
        // bare 400; a requested method outside the configured list bare
        // 405; a requested header outside the (default ∪ configured) list
        // bare 403 — all without CORS headers.
        let Some(request_method) = req
            .headers()
            .get("access-control-request-method")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string())
        else {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(axum::body::Body::empty())
                .unwrap_or_default();
        };
        if !state.is_match(&request_method, &state.allowed_methods) {
            return Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(axum::body::Body::empty())
                .unwrap_or_default();
        }

        // Requested headers that pass the allowed list are reflected —
        // the package's defaults are allowed but never reflected.
        let requested = req
            .headers()
            .get("access-control-request-headers")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let mut reflected: Vec<String> = Vec::new();
        for raw in requested.split(',') {
            let canonical = canonical_header_key(raw.trim());
            if canonical.is_empty() || DEFAULT_HEADERS.contains(&canonical.as_str()) {
                continue;
            }
            if !state.is_match(&canonical, &state.allowed_headers) {
                return Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body(axum::body::Body::empty())
                    .unwrap_or_default();
            }
            reflected.push(canonical);
        }

        // The preflight response: empty body, the reflected headers, and
        // the allow-methods header ONLY for methods outside the package's
        // internal default set — the value being the single requested
        // method. The never-wired max-age stays absent.
        let mut response = Response::builder()
            .status(StatusCode::OK)
            .body(axum::body::Body::empty())
            .unwrap_or_default();
        if !reflected.is_empty() {
            set_header(
                &mut response,
                "access-control-allow-headers",
                reflected.join(","),
            );
        }
        if is_default_method(&request_method) {
            // In the package's default set: no allow-methods header.
        } else {
            set_header(
                &mut response,
                "access-control-allow-methods",
                request_method.clone(),
            );
        }
        if state.allow_credentials {
            set_header(
                &mut response,
                "access-control-allow-credentials",
                "true".to_string(),
            );
        }
        if state.allowed_origins.len() > 1 {
            set_header(&mut response, "vary", "Origin".to_string());
        }
        set_header(
            &mut response,
            "access-control-allow-origin",
            state.return_origin(&origin),
        );
        return response;
    }

    // Allowed non-preflight: continue to the inner handler, then attach
    // the headers to its response.
    let mut response = next.run(req).await;
    if state.allow_credentials {
        set_header(
            &mut response,
            "access-control-allow-credentials",
            "true".to_string(),
        );
    }
    if state.allowed_origins.len() > 1 {
        set_header(&mut response, "vary", "Origin".to_string());
    }
    set_header(
        &mut response,
        "access-control-allow-origin",
        state.return_origin(&origin),
    );
    response
}

/// The package's internal default-method membership: the set that
/// suppresses the allow-methods header.
fn is_default_method(method: &str) -> bool {
    DEFAULT_METHODS.contains(&method)
}

/// Wraps the router with the gorilla-compatible CORS layer built from
/// `options` (see the module docs; [`crate::cors::with_cors`] is the
/// tower-http path).
pub fn with_cors_compat(router: Router, options: CorsOptions) -> Router {
    router.layer(axum::middleware::from_fn(move |req, next| {
        let state = CompatCors::from_options(&options);
        async move { compat_run(state, req, next).await }
    }))
}
