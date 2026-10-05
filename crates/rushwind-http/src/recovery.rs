//! The panic-recovery middleware.
//!
//! axum has no built-in panic isolation: a panicking handler aborts the
//! connection. This wrapper catches the unwind, logs the payload under
//! the `rushwind.http` target, and answers the shared error envelope —
//! `500` with reason `INTERNAL_PANIC` and no handler detail on the wire.

use std::any::Any;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::IntoResponse;

use crate::error::{HttpError, REASON_PANIC};

/// Wraps the router with the panic-recovery middleware. Place it
/// outermost — [`HttpEdge`](crate::HttpEdge) does.
pub fn with_recovery(router: axum::Router) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        |req: Request, next: Next| async move {
            // The request state is only crossed by the unwind, never aliased
            // behind it — AssertUnwindSafe is sound here.
            match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(next.run(req)))
                .await
            {
                Ok(response) => response,
                Err(payload) => {
                    tracing::error!(
                        target: "rushwind.http",
                        panic = %panic_message(&payload),
                        "handler panicked"
                    );
                    HttpError::internal(REASON_PANIC, "internal server error").into_response()
                }
            }
        },
    ))
}

fn panic_message(payload: &Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}
