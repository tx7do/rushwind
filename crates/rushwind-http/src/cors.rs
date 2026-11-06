//! The CORS middleware, configured with an explicit origin list,
//! credential flag, method
//! and header lists, and max age.
//!
//! A `*` origin delegates to `AllowOrigin::any()`; combined with
//! credentials it switches to per-request origin echo instead (browsers
//! reject `Access-Control-Allow-Origin: *` together with
//! `Access-Control-Allow-Credentials: true`, so echoing the request
//! origin is the only
//! shape that works). An empty origin list allows nothing — CORS stays
//! off until configured.
//!
//! Two security notes every deployment should read before enabling:
//!
//! - **`*` + credentials means every origin is trusted with the
//!   user's credentials.** The per-request echo makes any web page
//!   able to carry that user's cookies/session and read the response
//!   cross-origin — it is the moral equivalent of disabling the
//!   same-origin policy for credentialed requests. Prefer an explicit
//!   origin list whenever credentials are on; there is no
//!   constructor-time guard, so this is a deployment-level decision.
//!
//! - **The gorilla-compat layer (`with_cors_compat`) has the opposite
//!   empty-list semantics.** Matching the reference Go package, an
//!   empty origin list there allows *everything* and answers
//!   `Access-Control-Allow-Origin: *` — see `cors_compat`. The
//!   "empty means off" contract in this module holds only for the
//!   tower-http path.

use std::time::Duration;

use axum::http::{HeaderName, HeaderValue, Method};
use axum::Router;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer, ExposeHeaders};

use crate::request_id::HEADER_X_REQUEST_ID;

/// The default cross-origin request methods when none are configured.
pub const DEFAULT_ALLOW_METHODS: &[&str] =
    &["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"];
/// The default preflight-request headers when none are configured.
pub const DEFAULT_ALLOW_HEADERS: &[&str] = &["content-type", "authorization", HEADER_X_REQUEST_ID];
/// The default response-exposed headers when none are configured.
pub const DEFAULT_EXPOSE_HEADERS: &[&str] = &[HEADER_X_REQUEST_ID];

/// The CORS policy: origin list, credential flag, methods, headers,
/// exposed headers, and max age.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorsOptions {
    pub(crate) allow_origins: Vec<String>,
    pub(crate) allow_credentials: bool,
    pub(crate) allow_methods: Vec<String>,
    pub(crate) allow_headers: Vec<String>,
    pub(crate) expose_headers: Vec<String>,
    pub(crate) max_age: Option<Duration>,
    /// Opt-out of any `Access-Control-Allow-Methods` emission — the
    /// tower-http "never called `allow_methods`" state, where preflights
    /// get no method permission at all.
    pub(crate) no_allow_methods: bool,
    /// Opt-out of any `Access-Control-Allow-Headers` emission — the
    /// tower-http "never called `allow_headers`" state.
    pub(crate) no_allow_headers: bool,
}

impl CorsOptions {
    /// Allows one origin — an exact match, or the literal `*` for any.
    pub fn with_allow_origin(mut self, origin: impl Into<String>) -> Self {
        self.allow_origins.push(origin.into());
        self
    }

    /// Allows credentialed requests (`Access-Control-Allow-Credentials`).
    pub fn with_allow_credentials(mut self, allow: bool) -> Self {
        self.allow_credentials = allow;
        self
    }

    /// Allows one request method, e.g. `"PATCH"`.
    pub fn with_allow_method(mut self, method: impl Into<String>) -> Self {
        self.allow_methods.push(method.into());
        self
    }

    /// Allows one request header, e.g. `"x-captcha-value"`.
    pub fn with_allow_header(mut self, header: impl Into<String>) -> Self {
        self.allow_headers.push(header.into());
        self
    }

    /// Exposes one response header to the browser.
    pub fn with_expose_header(mut self, header: impl Into<String>) -> Self {
        self.expose_headers.push(header.into());
        self
    }

    /// Sets the preflight cache duration.
    pub fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    /// Opts out of `Access-Control-Allow-Methods` emission entirely:
    /// tower-http's `allow_methods` is never called, so every preflight
    /// fails the method check and the header stays absent.
    pub fn without_allow_methods(mut self) -> Self {
        self.no_allow_methods = true;
        self
    }

    /// Opts out of `Access-Control-Allow-Headers` emission entirely:
    /// tower-http's `allow_headers` is never called.
    pub fn without_allow_headers(mut self) -> Self {
        self.no_allow_headers = true;
        self
    }

    /// Builds the tower-http layer — the only place that knows CORS
    /// lives in tower-http. The opt-outs leave tower-http's
    /// `allow_methods`/`allow_headers` unset, matching the state of a
    /// deployment that never wired those fields.
    pub(crate) fn to_layer(&self) -> CorsLayer {
        let any_origin = self.allow_origins.iter().any(|origin| origin == "*");
        let origin = if any_origin && self.allow_credentials {
            // Credentialed + any-origin is a browser-rejected shape;
            // echo the request origin instead. Note the consequence:
            // every origin is now trusted with the user's credentials
            // — the module docs should be read before enabling this
            // combination.
            AllowOrigin::predicate(|_, _| true)
        } else if any_origin {
            AllowOrigin::any()
        } else {
            AllowOrigin::list(
                self.allow_origins
                    .iter()
                    .filter_map(|origin| HeaderValue::from_str(origin).ok()),
            )
        };

        let mut layer = CorsLayer::new()
            .allow_origin(origin)
            .allow_credentials(self.allow_credentials);
        if !self.no_allow_methods {
            let methods: Vec<Method> =
                parse_or_default(&self.allow_methods, DEFAULT_ALLOW_METHODS, |m| {
                    Method::from_bytes(m.as_bytes()).ok()
                });
            layer = layer.allow_methods(AllowMethods::list(methods));
        }
        if !self.no_allow_headers {
            let headers: Vec<HeaderName> =
                parse_or_default(&self.allow_headers, DEFAULT_ALLOW_HEADERS, |header| {
                    HeaderName::from_lowercase(header.as_bytes()).ok()
                });
            layer = layer.allow_headers(AllowHeaders::list(headers));
        }
        let expose: Vec<HeaderName> =
            parse_or_default(&self.expose_headers, DEFAULT_EXPOSE_HEADERS, |header| {
                HeaderName::from_lowercase(header.as_bytes()).ok()
            });
        layer = layer.expose_headers(ExposeHeaders::list(expose));
        if let Some(max_age) = self.max_age {
            layer = layer.max_age(max_age);
        }
        layer
    }
}

/// Parses the configured entries; an empty configuration falls back to
/// the defaults, invalid entries are skipped.
fn parse_or_default<T>(
    configured: &[String],
    defaults: &[&str],
    parse: impl Fn(&str) -> Option<T>,
) -> Vec<T> {
    if configured.is_empty() {
        defaults.iter().filter_map(|entry| parse(entry)).collect()
    } else {
        configured
            .iter()
            .filter_map(|entry| parse(entry.as_str()))
            .collect()
    }
}

/// Wraps the router with the CORS middleware built from `options`.
pub fn with_cors(router: Router, options: CorsOptions) -> Router {
    router.layer(options.to_layer())
}
