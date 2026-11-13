//! QUIC adapter for the RushWind lifecycle.
//!
//! [`QuicServer`] binds a quinn [`Endpoint`] eagerly at build time and runs
//! its accept loop inside [`Server::start`]. This is the first
//! *raw-socket* session transport: the handshake snapshot carries the peer
//! address and no headers, and the full session middleware chain runs
//! inside this crate with the semantics defined in
//! `docs/session-middleware.md`:
//!
//! - **Gates** evaluate at handshake time on the raw-transport snapshot;
//!   a rejection maps to [`quinn::Incoming::refuse`], the protocol's
//!   native refusal primitive.
//! - **The admission cap** is an atomic check at connection establishment
//!   (the shared [`SessionCounter`] of the contract layer); over-cap
//!   connections are closed explicitly, because quinn connections do not
//!   close when handles drop.
//! - **The handshake deadline** races the handshake future against the
//!   configured budget and drops it mid-flight on expiry.
//!
//! Each admitted session runs as its own task, racing the session handler
//! against the stop signal; whichever ends first, the connection is closed
//! explicitly. A panicking session dies alone — the admission guard drops
//! with its task, so the cap can never leak slots.
//!
//! # Shutdown mapping
//!
//! Unlike the axum adapter, whose stack releases everything when its serve
//! future is dropped, quinn's endpoint is a long-lived object that survives
//! the accept loop. [`Server::stop`] therefore performs a **real release**:
//! it closes the endpoint, which tears down the listener and every
//! connection it owns. This is the rule-2 case of the architecture
//! document: where the stack offers no self-releasing serve path, the
//! adapter's stop phase must do the release itself.
//!
//! # Certificate handling
//!
//! The adapter is transport-only: the caller supplies the quinn
//! [`quinn::ServerConfig`] — certificate chain included — at build time.
//! Certificate provisioning is application policy, not transport policy.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use quinn::{Connection, Endpoint};
use rushwind_transport::{
    GateChain, Handshake, HandshakeGate, Server, ServerError, ServerFuture, SessionCounter,
    SessionPolicy, StopSignal,
};

/// Session-handler function type: receives the live connection handle.
type SessionHandlerFn = Box<dyn Fn(Connection) -> SessionFuture + Send + Sync>;

/// Session-handler future type.
type SessionFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Shared server state: the gate chain, admission policy, session handler,
/// and admission counter.
struct ServerState {
    gates: GateChain,
    policy: SessionPolicy,
    session: SessionHandlerFn,
    counter: SessionCounter,
}

/// A QUIC server bound to the RushWind lifecycle.
pub struct QuicServer {
    quinn_endpoint: Endpoint,
    local_addr: SocketAddr,
    state: Arc<ServerState>,
}

impl QuicServer {
    /// Starts building a QUIC server with the supplied quinn server
    /// configuration (certificate chain included).
    pub fn builder(quinn_config: quinn::ServerConfig) -> Builder {
        Builder {
            quinn_config,
            gates: GateChain::new(),
            policy: SessionPolicy::new(),
            session: None,
        }
    }

    /// The actually-bound address (OS-assigned port included for `:0`
    /// binds).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Builder for [`QuicServer`].
pub struct Builder {
    quinn_config: quinn::ServerConfig,
    gates: GateChain,
    policy: SessionPolicy,
    session: Option<SessionHandlerFn>,
}

impl Builder {
    /// Appends a handshake gate. Gates run in registration order at
    /// handshake time; the first rejection refuses the handshake with the
    /// protocol's refusal primitive.
    pub fn gate(mut self, gate: Arc<dyn HandshakeGate>) -> Self {
        self.gates = self.gates.gate(gate);
        self
    }

    /// Caps simultaneously live sessions on this server.
    pub fn max_concurrent_sessions(mut self, n: usize) -> Self {
        self.policy = self.policy.max_concurrent_sessions(n);
        self
    }

    /// Bounds the wall-clock budget a single handshake may consume.
    pub fn handshake_timeout(mut self, d: Duration) -> Self {
        self.policy = self.policy.handshake_timeout(d);
        self
    }

    /// Sets the session handler invoked for every admitted connection.
    /// The handler runs until it returns, the connection closes, or the
    /// lifecycle's stop signal fires — whichever comes first.
    pub fn session_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Connection) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.session = Some(Box::new(move |conn: Connection| {
            Box::pin(handler(conn)) as SessionFuture
        }));
        self
    }

    /// Eagerly binds `addr` and freezes the builder into a
    /// [`QuicServer`].
    ///
    /// Errors when no session handler was registered — there is no default
    /// session behavior — or when the endpoint bind fails.
    pub fn build(self, addr: SocketAddr) -> Result<QuicServer, ServerError> {
        let session = self
            .session
            .ok_or_else(|| ServerError::Failed("quic server has no session handler".to_string()))?;
        let endpoint = Endpoint::server(self.quinn_config, addr)
            .map_err(|e| ServerError::Failed(format!("bind {addr}: {e}")))?;
        let local_addr = endpoint
            .local_addr()
            .map_err(|e| ServerError::Failed(format!("local_addr: {e}")))?;
        Ok(QuicServer {
            quinn_endpoint: endpoint,
            local_addr,
            state: Arc::new(ServerState {
                gates: self.gates,
                policy: self.policy,
                session,
                counter: SessionCounter::new(),
            }),
        })
    }
}

impl Server for QuicServer {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(format!("quic://{}", self.local_addr))
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        let endpoint = self.quinn_endpoint.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            loop {
                // Waiting for the next handshake attempt also observes
                // the stop signal; quinn's accept future is Notify-based
                // and therefore cancel-safe across loop iterations.
                let incoming = tokio::select! {
                    _ = stop.wait() => return Err(ServerError::Cancelled),
                    maybe = endpoint.accept() => match maybe {
                        Some(incoming) => incoming,
                        None => return Err(ServerError::Cancelled),
                    },
                };

                // Gate evaluation on the raw-transport handshake snapshot:
                // the peer address, and nothing else.
                let handshake = Handshake {
                    headers: Vec::new(),
                    remote: Some(incoming.remote_address()),
                };
                if state.gates.evaluate(&handshake).is_err() {
                    // Raw transports map rejection to the protocol's
                    // refusal primitive; the rejection record itself has
                    // no carrier on this transport and is dropped.
                    incoming.refuse();
                    continue;
                }

                let connecting = match incoming.accept() {
                    Ok(connecting) => connecting,
                    Err(_) => continue, // handshake already failed
                };

                // The handshake completes when this future is awaited; the
                // configured deadline, if any, races it and drops the
                // handshake mid-flight on expiry.
                let connection = match state.policy.get_handshake_timeout() {
                    None => tokio::select! {
                        _ = stop.wait() => return Err(ServerError::Cancelled),
                        conn = connecting => match conn {
                            Ok(conn) => conn,
                            Err(_) => continue,
                        },
                    },
                    Some(budget) => tokio::select! {
                        _ = stop.wait() => return Err(ServerError::Cancelled),
                        conn = tokio::time::timeout(budget, connecting) => match conn {
                            Ok(Ok(conn)) => conn,
                            Ok(Err(_connection_error)) => continue,
                            Err(_deadline_exceeded) => continue,
                        },
                    },
                };

                // Atomic admission check at connection establishment —
                // the route-wide cap and (the peer address being visible
                // on this transport) the per-peer cap together; over-cap
                // connections are closed explicitly.
                let (guard, _per_remote) = match state.counter.admit_with_remote(
                    state.policy.get_max_concurrent_sessions(),
                    state.policy.get_max_sessions_per_remote(),
                    handshake.remote,
                ) {
                    Some(guards) => guards,
                    None => {
                        connection.close(0u8.into(), b"session capacity reached");
                        continue;
                    }
                };

                // The session runs as its own task: the handler races the
                // stop signal, and whichever way it ends, the connection
                // is closed explicitly — quinn connections do not close
                // when handles drop.
                let handle = connection.clone();
                let session_stop = stop.clone();
                let session_handler = (state.session)(connection);
                tokio::spawn(async move {
                    let _guard = guard;
                    tokio::select! {
                        _ = session_stop.wait() => {
                            handle.close(0u8.into(), b"server shutting down");
                        }
                        _ = session_handler => {
                            handle.close(0u8.into(), b"session ended");
                        }
                    }
                });
            }
        })
    }

    fn stop(&self) -> ServerFuture<'_> {
        // quinn's endpoint is a long-lived object that survives the accept
        // loop; closing it releases the listener and every connection it
        // owns. This is a real release, not a no-op — see the
        // shutdown-mapping note in the crate docs.
        self.quinn_endpoint.close(0u8.into(), b"server stopped");
        Box::pin(async { Ok(()) })
    }
}
