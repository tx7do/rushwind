//! WebSocket session routes for axum-based RushWind servers.
//!
//! [`WsRoute`] builds an axum [`MethodRouter`] that mounts a
//! session-handshake gate chain and a listener-scoped admission policy in
//! front of a session handler, and returns the route together with a
//! **session-shutdown bus**.
//!
//! # Composition
//!
//! Mount the returned router into an [`axum::Router`] with
//! `Router::route`, and serve that router through `AxumServer` (crate
//! `rushwind-transport-axum`). Register the bus with
//! `AxumServer::with_aux_shutdown` so the session handlers wind down when
//! the lifecycle's shutdown path runs; an unregistered bus means the
//! sessions terminate only when the process exits.
//!
//! # What the gates see
//!
//! Gates run at HTTP-request time, before the upgrade: they see the
//! request's header pairs and nothing else. The session socket itself —
//! including frames — is invisible to the gate chain; frame-level policy
//! is a future tier (see `docs/session-middleware.md`).
//!
//! The session handler receives axum's native
//! [`WebSocket`](axum::extract::ws::WebSocket). There is deliberately no
//! cross-transport session abstraction: the chain (gates, policy, bus) is
//! uniform across session transports, the session type stays native.
//!
//! # Admission policy
//!
//! [`SessionPolicy`] caps simultaneously live sessions per route. Handshakes
//! over the cap are refused before the upgrade; sessions that raced past
//! the pre-check are closed immediately at session start.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, MethodRouter};
use rushwind_transport::{
    GateChain, Handshake, HandshakeGate, Rejection, ServerError, SessionCounter, SessionPolicy,
    StopSignal,
};

/// Session-handler function type: receives the live native socket.
type SessionHandlerFn = Box<dyn Fn(WebSocket) -> SessionFuture + Send + Sync>;

/// Session-handler future type.
type SessionFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Shared per-route state: the gate chain, admission policy, session
/// handler, admission counter, and session-shutdown bus.
struct RouteState {
    gates: GateChain,
    policy: SessionPolicy,
    session: SessionHandlerFn,
    counter: SessionCounter,
    bus: StopSignal,
}

impl RouteState {
    /// Whether the route is currently at one of its configured session
    /// caps — the route-wide one, or (when the peer address is visible)
    /// that peer's per-IP cap. Advisory only — the authoritative check
    /// happens atomically inside
    /// [`SessionCounter::admit_with_remote`].
    fn at_capacity_with_remote(&self, remote: Option<std::net::SocketAddr>) -> bool {
        // Both advisory probes: the route-wide cap and this peer's
        // per-IP cap — a 429 answers before the upgrade; the
        // authoritative check still runs atomically at admission
        // (admit_with_remote).
        self.counter
            .at_cap(self.policy.get_max_concurrent_sessions())
            || self
                .counter
                .at_per_remote_cap(self.policy.get_max_sessions_per_remote(), remote)
    }
}

/// Maps a gate rejection to an HTTP response.
fn rejection_response(rej: Rejection) -> Response {
    let status = StatusCode::from_u16(rej.status).unwrap_or(StatusCode::FORBIDDEN);
    (status, rej.reason).into_response()
}

/// Builder for a gated WebSocket session route.
pub struct WsRoute {
    gates: GateChain,
    policy: SessionPolicy,
    session: Option<SessionHandlerFn>,
}

impl WsRoute {
    /// Starts building an empty WebSocket session route. Gates default to
    /// none (every handshake accepted) and the admission policy to
    /// uncapped — mount gates explicitly on auth-sensitive routes.
    pub fn new() -> Self {
        Self {
            gates: GateChain::new(),
            policy: SessionPolicy::new(),
            session: None,
        }
    }

    /// Appends a handshake gate. Gates run in registration order; the
    /// first rejection refuses the handshake.
    pub fn gate(mut self, gate: Arc<dyn HandshakeGate>) -> Self {
        self.gates = self.gates.gate(gate);
        self
    }

    /// Caps simultaneously live sessions on this route.
    pub fn max_concurrent_sessions(mut self, n: usize) -> Self {
        self.policy = self.policy.max_concurrent_sessions(n);
        self
    }

    /// Caps simultaneously live sessions **per peer IP** — one peer
    /// cannot crowd the route's whole budget. Enforced at admission
    /// through the ConnectInfo-stamped peer address (every AxumServer
    /// mount carries it).
    pub fn max_sessions_per_remote(mut self, n: usize) -> Self {
        self.policy = self.policy.max_sessions_per_remote(n);
        self
    }

    /// Sets the session handler invoked for every accepted handshake. The
    /// handler runs until it returns, the socket closes, or the
    /// session-shutdown bus fires — whichever comes first.
    pub fn session_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(WebSocket) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.session = Some(Box::new(move |socket: WebSocket| {
            Box::pin(handler(socket)) as SessionFuture
        }));
        self
    }

    /// Builds the route.
    ///
    /// Returns the mountable [`MethodRouter`] together with the route's
    /// **session-shutdown bus**. Register the bus with
    /// `AxumServer::with_aux_shutdown` (crate `rushwind-transport-axum`)
    /// so the route's sessions terminate when the lifecycle's shutdown
    /// path runs; an unregistered bus means the sessions terminate only
    /// when the process exits.
    ///
    /// Errors when no session handler was registered — there is no
    /// default session behavior.
    pub fn build(self) -> Result<(MethodRouter, StopSignal), ServerError> {
        let session = self
            .session
            .ok_or_else(|| ServerError::Failed("ws route has no session handler".to_string()))?;
        let bus = StopSignal::new();
        let state = Arc::new(RouteState {
            gates: self.gates,
            policy: self.policy,
            session,
            counter: SessionCounter::new(),
            bus: bus.clone(),
        });
        let handler_state = Arc::clone(&state);
        let handler =
            move |upgrade: WebSocketUpgrade,
                  headers: HeaderMap,
                  connect_info: axum::extract::ConnectInfo<std::net::SocketAddr>| async move {
                // The peer address rides the ConnectInfo extension the
                // server's make-service stamps in — every AxumServer
                // mount carries it, so gates and the per-remote cap see
                // the real peer on this transport.
                let remote = Some(connect_info.0);
                let handshake = Handshake {
                    headers: headers
                        .iter()
                        .map(|(name, value)| {
                            (
                                name.as_str().to_string(),
                                value.to_str().unwrap_or_default().to_string(),
                            )
                        })
                        .collect(),
                    remote,
                };
                if let Err(rej) = handler_state.gates.evaluate(&handshake) {
                    return rejection_response(rej);
                }
                if handler_state.at_capacity_with_remote(remote) {
                    return (StatusCode::TOO_MANY_REQUESTS, "session capacity reached")
                        .into_response();
                }
                let callback_state = Arc::clone(&handler_state);
                upgrade
                    .on_upgrade(move |socket| {
                        let state = callback_state;
                        async move {
                            let (_total_guard, _per_remote_guard) =
                                match state.counter.admit_with_remote(
                                    state.policy.get_max_concurrent_sessions(),
                                    state.policy.get_max_sessions_per_remote(),
                                    remote,
                                ) {
                                    Some(guards) => guards,
                                    None => return,
                                };
                            let session_fut = (state.session)(socket);
                            tokio::select! {
                                _ = state.bus.wait() => {}
                                _ = session_fut => {}
                            }
                        }
                    })
                    .into_response()
            };
        Ok((get(handler), bus))
    }
}

impl Default for WsRoute {
    fn default() -> Self {
        Self::new()
    }
}
