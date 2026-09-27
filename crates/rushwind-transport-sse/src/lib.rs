//! The SSE notification transport — the `/events` wire contract the
//! two BFF deployments carried by copy-paste, extracted once.
//!
//! Wire behavior (transport sse/http.go + module:18-31, service
//! HandleAuthorize:120-163):
//! * `OPTIONS` preflight — 204 with a fixed CORS header set (origin
//!   `*`, methods `GET, OPTIONS`, headers `Content-Type, Authorization,
//!   X-Token, Last-Event-ID`, max-age `86400`), answered before any
//!   authorization check;
//! * every authorize failure — 401 with the plain-text error line
//!   (`error: code = … reason = … message = … metadata = map[] cause =
//!   <nil>`, `text/plain; charset=utf-8`, `X-Content-Type-Options:
//!   nosniff`) and no CORS headers. The status is Unauthorized for
//!   every failure class: the transport's forbidden-sentinel check
//!   never fires on the generated status errors, so the blocked-token
//!   and stream-mismatch bodies carry `code = 403` under a 401 status
//!   line;
//! * token from `Authorization: Bearer`, `X-Token`, or `?token=`;
//! * authorization plugs in through the [`Gate`] trait — the
//!   deployments' HandleAuthorize ValidateTokenRequest(ACCESS)
//!   sequences (signature + expiry via the JWT engine, the Redis
//!   whitelist/blacklist or the core service's remote validation) are
//!   adapters, not framework code;
//! * `?stream=` must equal the authenticated userId (anti cross-user
//!   subscription);
//! * the live stream carries `text/event-stream`, `no-cache`,
//!   `Connection: keep-alive`, and the transport-level CORS pair —
//!   silent when idle (no keep-alive pings); events are `notification`,
//!   id = GUIDv4, each connection forwarding only its own userId's
//!   payloads;
//! * the bootstrap factory assembles the router (handler + preflight
//!   under the wire's address and path) into a lifecycle server.
//!
//! The [`SseApp::status_error`] constructor is the deployment's
//! table-anchored one (reason → the error-table HTTP status), so the
//! failure envelopes carry the deployment's own reason codes.

mod factory;
mod handler;
mod hub;

pub use factory::{factory, SseWire};
pub use handler::{events, events_preflight, extract_token, Failure, Gate, SseApp};
pub use hub::Hub;
