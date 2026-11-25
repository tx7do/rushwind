//! The panic-recovery middleware.
//!
//! axum has no built-in panic isolation: a panicking handler aborts the
//! connection. This wrapper does two things: it scopes the request
//! identity — request id, method, path — as
//! [`rushwind_panic`] context, so the process-wide hook's
//! `rushwind.panic` report carries the request (one line, no double
//! logging), and it catches the unwind to answer the shared error
//! envelope — `500` with reason `INTERNAL_PANIC` and no handler detail
//! on the wire.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::IntoResponse;

use crate::error::{HttpError, REASON_PANIC};

/// Wraps the router with the panic-recovery middleware. Place it
/// outside the request-id layer — [`HttpEdge`](crate::HttpEdge) does —
/// so the panic context carries the request id; standalone use runs
/// with method/path context only.
pub fn with_recovery(router: axum::Router) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        |req: Request, next: Next| async move {
            // The request state is only crossed by the unwind, never aliased
            // behind it — AssertUnwindSafe is sound here.
            let request_id = req
                .extensions()
                .get::<crate::request_id::RequestId>()
                .map(|id| id.as_str().to_owned());
            let method = req.method().to_string();
            let path = req.uri().path().to_owned();
            let mut context: Vec<(&str, String)> = vec![("method", method), ("path", path)];
            if let Some(id) = request_id {
                context.push(("request_id", id));
            }
            match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                rushwind_panic::scoped(context, next.run(req)),
            ))
            .await
            {
                Ok(response) => response,
                Err(_payload) => {
                    // The payload was already rendered by the
                    // process-wide rushwind-panic hook — it never
                    // reaches the wire.
                    HttpError::internal(REASON_PANIC, "internal server error").into_response()
                }
            }
        },
    ))
}
