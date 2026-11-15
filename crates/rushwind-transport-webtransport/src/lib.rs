//! WebTransport adapter for the RushWind lifecycle.
//!
//! [`WebTransportServer`] binds a wtransport [`Endpoint`] eagerly at
//! build time and runs its accept loop inside [`Server::start`].
//! The full session middleware chain runs inside this crate with
//! the semantics defined in `docs/session-middleware.md`:
//!
//! - **Gates** evaluate at session-request time — the HTTP-family
//!   handshake moment. The snapshot carries the request's
//!   `:authority` and `:path` pseudo-headers plus its header map;
//!   `remote` stays `None`, the HTTP-family contract. A gate
//!   rejection drops the session request: wtransport exposes no
//!   refusal response for the CONNECT at this stage (the
//!   `IncomingSession::refuse` primitive predates the handshake
//!   and the request is already past it), so the client observes a
//!   closed connection. The rejection record itself has no carrier
//!   and is dropped.
//! - **The admission cap** is an atomic check at connection
//!   establishment (the shared [`SessionCounter`] of the contract
//!   layer); over-cap connections are closed explicitly.
//! - **The handshake deadline** races the session-request future —
//!   the QUIC handshake plus the request receipt — against the
//!   configured budget and drops it mid-flight on expiry.
//!
//! Each admitted session runs as its own task racing the
//! lifecycle's stop signal directly — no shutdown bus: this server
//! owns its accept loop, and `docs/session-middleware.md` wires
//! such transports straight to the signal. Whichever ends first,
//! the connection is closed explicitly. A panicking session dies
//! alone — the admission guard drops with its task, so the cap can
//! never leak slots.
//!
//! # Shutdown mapping
//!
//! wtransport's endpoint is a long-lived object that survives the
//! accept loop. [`Server::stop`] therefore performs a **real
//! release**: it closes the endpoint, which tears down the listener
//! and every connection it owns — the rule-2 case of the
//! architecture document, the same mapping as the QUIC adapter.
//!
//! # Certificate handling
//!
//! WebTransport rides HTTP/3, which is always TLS: the caller
//! supplies the wtransport [`Identity`] (certificate chain
//! included) at build time. Certificate provisioning is application
//! policy, not transport policy.
//!
//! # Testing
//!
//! Integration tests run against the adapter itself with real
//! wtransport clients — no external broker.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rushwind_transport::{
    GateChain, Handshake, HandshakeGate, Server, ServerError, ServerFuture, SessionCounter,
    SessionPolicy, StopSignal,
};
use wtransport::endpoint::Endpoint as WtEndpoint;

/// The wtransport server-side endpoint type.
type ServerEndpoint = WtEndpoint<wtransport::endpoint::endpoint_side::Server>;

/// Session-handler function type: receives the live native session.
type SessionHandlerFn = Box<dyn Fn(wtransport::Connection) -> SessionFuture + Send + Sync>;

/// Session-handler future type.
type SessionFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Shared server state: the gate chain, admission policy, session
/// handler, and admission counter.
struct ServerState {
    gates: GateChain,
    policy: SessionPolicy,
    session: SessionHandlerFn,
    counter: SessionCounter,
}

/// A WebTransport server bound to the RushWind lifecycle.
pub struct WebTransportServer {
    endpoint: ServerEndpoint,
    local_addr: SocketAddr,
    state: Arc<ServerState>,
}

impl WebTransportServer {
    /// Starts building a WebTransport server with the supplied
    /// wtransport identity (certificate chain included).
    pub fn builder(identity: wtransport::Identity) -> Builder {
        Builder {
            identity,
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

/// Builder for [`WebTransportServer`].
pub struct Builder {
    identity: wtransport::Identity,
    gates: GateChain,
    policy: SessionPolicy,
    session: Option<SessionHandlerFn>,
}

impl Builder {
    /// Appends a handshake gate. Gates run in registration order at
    /// session-request time; the first rejection drops the request.
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

    /// Sets the session handler invoked for every admitted
    /// connection. The handler runs until it returns, the connection
    /// closes, or the lifecycle's stop signal fires — whichever comes
    /// first.
    pub fn session_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(wtransport::Connection) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.session = Some(Box::new(move |session: wtransport::Connection| {
            Box::pin(handler(session)) as SessionFuture
        }));
        self
    }

    /// Eagerly binds `addr` and freezes the builder into a
    /// [`WebTransportServer`].
    ///
    /// Errors when no session handler was registered — there is no
    /// default session behavior — or when the endpoint bind fails.
    pub fn build(self, addr: SocketAddr) -> Result<WebTransportServer, ServerError> {
        let session = self.session.ok_or_else(|| {
            ServerError::Failed("webtransport server has no session handler".to_string())
        })?;
        let config = wtransport::ServerConfig::builder()
            .with_bind_address(addr)
            .with_identity(self.identity)
            .build();
        let endpoint = WtEndpoint::server(config)
            .map_err(|e| ServerError::Failed(format!("bind {addr}: {e}")))?;
        let local_addr = endpoint
            .local_addr()
            .map_err(|e| ServerError::Failed(format!("local_addr: {e}")))?;
        Ok(WebTransportServer {
            endpoint,
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

impl Server for WebTransportServer {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(format!("webtransport://{}", self.local_addr))
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        let endpoint = &self.endpoint;
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            loop {
                // Waiting for the next session attempt also observes
                // the stop signal; wtransport's accept future is
                // quinn-backed and cancel-safe across iterations.
                let incoming_session = tokio::select! {
                    _ = stop.wait() => return Err(ServerError::Cancelled),
                    incoming = endpoint.accept() => incoming,
                };

                // The session-request future covers the QUIC handshake
                // and the request receipt; the configured deadline, if
                // any, races it and drops the attempt mid-flight on
                // expiry.
                // The peer address is visible before the handshake
                // consumes the incoming session — capture it first.
                let peer = incoming_session.remote_address();
                let session_request = match state.policy.get_handshake_timeout() {
                    None => tokio::select! {
                        _ = stop.wait() => return Err(ServerError::Cancelled),
                        request = incoming_session => match request {
                            Ok(request) => request,
                            Err(_) => continue,
                        },
                    },
                    Some(budget) => tokio::select! {
                        _ = stop.wait() => return Err(ServerError::Cancelled),
                        request = tokio::time::timeout(budget, incoming_session) => match request {
                            Ok(Ok(request)) => request,
                            Ok(Err(_connection_error)) => continue,
                            Err(_deadline_exceeded) => continue,
                        },
                    },
                };

                // Gate evaluation on the HTTP-family handshake
                // snapshot: the request's :authority and :path
                // pseudo-headers and its header map. A rejection drops
                // the request — wtransport offers no refusal response
                // for an established session request, so the client
                // observes a closed connection, and the rejection
                // record has no carrier on this transport.
                let mut headers: Vec<(String, String)> = vec![
                    (
                        String::from(":authority"),
                        session_request.authority().to_string(),
                    ),
                    (String::from(":path"), session_request.path().to_string()),
                ];
                for (name, value) in session_request.headers() {
                    headers.push((name.clone(), value.clone()));
                }
                let handshake = Handshake {
                    headers,
                    // The peer address rides the IncomingSession (visible
                    // before the QUIC handshake completes) — gates and
                    // the per-remote quota see the real peer here.
                    remote: Some(peer),
                };
                if state.gates.evaluate(&handshake).is_err() {
                    drop(session_request);
                    continue;
                }

                let connection = tokio::select! {
                    _ = stop.wait() => return Err(ServerError::Cancelled),
                    connection = session_request.accept() => match connection {
                        Ok(connection) => connection,
                        Err(_) => continue,
                    },
                };

                // Atomic admission check at connection establishment;
                // over-cap connections are closed explicitly.
                let guard = match state
                    .counter
                    .admit(state.policy.get_max_concurrent_sessions())
                {
                    Some(guard) => guard,
                    None => {
                        connection
                            .close(wtransport::VarInt::from_u32(0), b"session capacity reached");
                        continue;
                    }
                };

                // The session runs as its own task: the handler races
                // the stop signal, and whichever way it ends, the
                // connection is closed explicitly — wtransport
                // connections do not close when handles drop.
                let handle = connection.clone();
                let session_stop = stop.clone();
                let session_handler = (state.session)(connection);
                tokio::spawn(async move {
                    let _guard = guard;
                    tokio::select! {
                        _ = session_stop.wait() => {
                            handle.close(wtransport::VarInt::from_u32(0), b"server shutting down");
                        }
                        _ = session_handler => {
                            handle.close(wtransport::VarInt::from_u32(0), b"session ended");
                        }
                    }
                });
            }
        })
    }

    fn stop(&self) -> ServerFuture<'_> {
        // wtransport's endpoint is a long-lived object that survives
        // the accept loop; closing it releases the listener and every
        // connection it owns. This is a real release, not a no-op —
        // see the shutdown-mapping note in the crate docs.
        self.endpoint
            .close(wtransport::VarInt::from_u32(0), b"server stopped");
        Box::pin(async { Ok(()) })
    }
}
