//! The assembled middleware stack — [`HttpEdge`] chains the request
//! middlewares in the canonical order with one call.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;

use crate::{cors, cors_compat, logging, recovery, request_id, timeout};

/// The request-stack builder. Defaults: recovery, request-id and
/// logging on; CORS and timeout off until configured. [`HttpEdge::wrap`]
/// applies the enabled middlewares so that at request time they run in
/// canonical order — request-id, recovery, logging, CORS,
/// timeout — no matter the order the options were set in. Request-id
/// outermost lets the recovery layer scope its panic reports with the
/// request identity.
#[derive(Clone)]
pub struct HttpEdge {
    request_id: Option<Arc<dyn Fn() -> String + Send + Sync>>,
    logging: bool,
    cors: Option<cors::CorsOptions>,
    /// Whether the CORS options route through the gorilla-compatible
    /// layer ([`crate::cors_compat`]) instead of tower-http — the
    /// compatibility choice for deployments that require
    /// gorilla/handlers-style CORS semantics.
    cors_compat: bool,
    timeout: Option<Duration>,
    recovery: bool,
}

impl Default for HttpEdge {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpEdge {
    /// The default stack: recovery + request-id + logging.
    pub fn new() -> Self {
        Self {
            request_id: Some(Arc::new(request_id::generate_request_id)),
            logging: true,
            cors: None,
            cors_compat: false,
            timeout: None,
            recovery: true,
        }
    }

    /// Uses a custom request-id generator.
    pub fn with_request_id_generator(
        mut self,
        generate: Arc<dyn Fn() -> String + Send + Sync>,
    ) -> Self {
        self.request_id = Some(generate);
        self
    }

    /// Disables the request-id middleware.
    pub fn without_request_id(mut self) -> Self {
        self.request_id = None;
        self
    }

    /// Disables the request-logging middleware.
    pub fn without_logging(mut self) -> Self {
        self.logging = false;
        self
    }

    /// Disables panic recovery.
    pub fn without_recovery(mut self) -> Self {
        self.recovery = false;
        self
    }

    /// Enables CORS with the given policy.
    pub fn with_cors(mut self, options: cors::CorsOptions) -> Self {
        self.cors = Some(options);
        self
    }

    /// Enables CORS with the given policy through the gorilla-compatible
    /// layer (see [`crate::cors_compat`]).
    pub fn with_cors_compat(mut self, options: cors::CorsOptions) -> Self {
        self.cors = Some(options);
        self.cors_compat = true;
        self
    }

    /// Bounds every request's downstream execution by `budget`.
    pub fn with_timeout(mut self, budget: Duration) -> Self {
        self.timeout = Some(budget);
        self
    }

    /// Wraps the business router with the enabled middlewares.
    ///
    /// Authn/authz are deliberately not part of the stack — they are
    /// per-subtree decisions ([`crate::with_authn`] /
    /// [`crate::with_authorization`], route whitelists included). Wrap
    /// the protected subtrees first, then hand the merged router here.
    pub fn wrap(&self, router: Router) -> Router {
        // Later `layer` calls wrap earlier ones, so applying in reverse
        // request order puts request-id outermost — the recovery layer
        // beneath it can then read the RequestId extension and scope
        // its panic reports with the request identity.
        let mut wrapped = router;
        if let Some(budget) = self.timeout {
            wrapped = timeout::with_timeout(wrapped, budget);
        }
        if let Some(options) = &self.cors {
            wrapped = if self.cors_compat {
                cors_compat::with_cors_compat(wrapped, options.clone())
            } else {
                cors::with_cors(wrapped, options.clone())
            };
        }
        if self.logging {
            wrapped = logging::with_logging(wrapped);
        }
        if self.recovery {
            wrapped = recovery::with_recovery(wrapped);
        }
        if let Some(generate) = &self.request_id {
            wrapped = request_id::with_request_id_generator(wrapped, Arc::clone(generate));
        }
        wrapped
    }
}
