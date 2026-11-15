//! HTTP/3 adapter for the RushWind lifecycle.
//!
//! [`H3Server`] binds a quinn [`quinn::Endpoint`] eagerly at build
//! time and serves HTTP/3 over it via `h3`/`h3-quinn` inside
//! [`Server::start`]. The session middleware chain runs with the
//! semantics defined in `docs/session-middleware.md`:
//!
//! - **Gates** evaluate at request time — the HTTP-family
//!   handshake moment, the same surface the WebSocket adapter
//!   guards. The snapshot carries the request's header pairs and
//!   nothing else; `remote` stays `None`, the HTTP-family
//!   contract. A gate rejection maps to an HTTP response carrying
//!   the rejection's status — the WebSocket adapter's mapping; the
//!   rejection reason has no carrier on this transport and is
//!   dropped.
//! - **The admission cap** is an atomic check at connection
//!   establishment (the shared [`SessionCounter`] of the contract
//!   layer), bounding QUIC connections — HTTP/3 multiplexes many
//!   requests per connection. Over-cap connections are closed
//!   explicitly.
//! - **The handshake deadline** races the QUIC `Connecting` future
//!   against the configured budget and drops it mid-flight on
//!   expiry.
//!
//! Request handlers receive the raw `h3` surface — the
//! [`http::Request`] and its [`RequestStream`] — and own the
//! response path entirely; there is no routing framework on this
//! transport.
//!
//! Each connection runs as its own task racing the lifecycle's stop
//! signal directly — no shutdown bus, per the session middleware
//! document: this server owns its accept loop. Whichever way a
//! connection ends, it is closed explicitly. A panicking handler
//! request dies alone in its own task.
//!
//! # Shutdown mapping
//!
//! quinn's endpoint is a long-lived object that survives the accept
//! loop. [`Server::stop`] therefore performs a **real release**:
//! it closes the endpoint, which tears down the listener and every
//! connection it owns — the rule-2 case of the architecture
//! document, the same mapping as the QUIC and WebTransport
//! adapters.
//!
//! # Certificate handling
//!
//! The adapter is transport-only: the caller supplies the quinn
//! [`quinn::ServerConfig`] — certificate chain included — at build
//! time. Certificate provisioning is application policy, not
//! transport policy.
//!
//! # Testing
//!
//! Integration tests run against the adapter itself with a real
//! `h3` client over quinn.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use h3::server::RequestStream;
use http::{Request, Response, StatusCode};
use rushwind_transport::{
    GateChain, Handshake, HandshakeGate, Server, ServerError, ServerFuture, SessionCounter,
    SessionPolicy, StopSignal,
};

/// Request-handler function type: receives the raw request and its
/// stream.
type RequestHandlerFn = Box<
    dyn Fn(
            Request<()>,
            RequestStream<h3_quinn::BidiStream<bytes::Bytes>, bytes::Bytes>,
        ) -> RequestFuture
        + Send
        + Sync,
>;

/// Request-handler future type.
type RequestFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Shared server state: the gate chain, admission policy, request
/// handler, and admission counter.
struct ServerState {
    gates: GateChain,
    policy: SessionPolicy,
    requests: RequestHandlerFn,
    counter: SessionCounter,
}

/// An HTTP/3 server bound to the RushWind lifecycle.
pub struct H3Server {
    quinn_endpoint: quinn::Endpoint,
    local_addr: SocketAddr,
    state: Arc<ServerState>,
}

impl H3Server {
    /// Starts building an HTTP/3 server with the supplied quinn
    /// server configuration (certificate chain included).
    pub fn builder(quinn_config: quinn::ServerConfig) -> Builder {
        Builder {
            quinn_config,
            gates: GateChain::new(),
            policy: SessionPolicy::new(),
            requests: None,
        }
    }

    /// The actually-bound address (OS-assigned port included for `:0`
    /// binds).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Builder for [`H3Server`].
pub struct Builder {
    quinn_config: quinn::ServerConfig,
    gates: GateChain,
    policy: SessionPolicy,
    requests: Option<RequestHandlerFn>,
}

impl Builder {
    /// Appends a request gate. Gates run in registration order at
    /// request time; the first rejection answers the request with
    /// the rejection's status.
    pub fn gate(mut self, gate: Arc<dyn HandshakeGate>) -> Self {
        self.gates = self.gates.gate(gate);
        self
    }

    /// Caps simultaneously live connections on this server.
    pub fn max_concurrent_sessions(mut self, n: usize) -> Self {
        self.policy = self.policy.max_concurrent_sessions(n);
        self
    }

    /// Bounds the wall-clock budget a single handshake may consume.
    pub fn handshake_timeout(mut self, d: Duration) -> Self {
        self.policy = self.policy.handshake_timeout(d);
        self
    }

    /// Sets the request handler invoked for every admitted request.
    /// The handler owns the response path entirely.
    pub fn request_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Request<()>, RequestStream<h3_quinn::BidiStream<bytes::Bytes>, bytes::Bytes>) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.requests =
            Some(
                Box::new(
                    move |request: Request<()>,
                          stream: RequestStream<
                        h3_quinn::BidiStream<bytes::Bytes>,
                        bytes::Bytes,
                    >| { Box::pin(handler(request, stream)) as RequestFuture },
                ),
            );
        self
    }

    /// Eagerly binds `addr` and freezes the builder into an
    /// [`H3Server`].
    ///
    /// Errors when no request handler was registered — there is no
    /// default request behavior — or when the endpoint bind fails.
    pub fn build(self, addr: SocketAddr) -> Result<H3Server, ServerError> {
        let requests = self
            .requests
            .ok_or_else(|| ServerError::Failed("h3 server has no request handler".to_string()))?;
        let endpoint = quinn::Endpoint::server(self.quinn_config, addr)
            .map_err(|e| ServerError::Failed(format!("bind {addr}: {e}")))?;
        let local_addr = endpoint
            .local_addr()
            .map_err(|e| ServerError::Failed(format!("local_addr: {e}")))?;
        Ok(H3Server {
            quinn_endpoint: endpoint,
            local_addr,
            state: Arc::new(ServerState {
                gates: self.gates,
                policy: self.policy,
                requests,
                counter: SessionCounter::new(),
            }),
        })
    }
}

impl Server for H3Server {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(format!("https://{}", self.local_addr))
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        let endpoint = &self.quinn_endpoint;
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

                // Gate evaluation happens per request, below: the
                // raw QUIC handshake carries no headers, and this
                // transport is HTTP-family — its gate surface is
                // the request's header pairs, not the raw
                // handshake.

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

                // Atomic admission check at connection establishment;
                // over-cap connections are closed explicitly.
                let guard = match state
                    .counter
                    .admit(state.policy.get_max_concurrent_sessions())
                {
                    Some(guard) => guard,
                    None => {
                        connection.close(0u8.into(), b"session capacity reached");
                        continue;
                    }
                };

                // The connection runs as its own task: the h3 accept
                // loop races the stop signal, and whichever way it ends,
                // the connection is closed explicitly — quinn connections
                // do not close when handles drop.
                let handle = connection.clone();
                let conn_stop = stop.clone();
                let conn_state = Arc::clone(&state);
                let peer = connection.remote_address();
                tokio::spawn(async move {
                    let _guard = guard;

                    let mut h3_connection = match h3::server::builder()
                        .build(h3_quinn::Connection::new(connection))
                        .await
                    {
                        Ok(h3_connection) => h3_connection,
                        Err(_) => {
                            handle.close(0u8.into(), b"h3 handshake failed");
                            return;
                        }
                    };

                    loop {
                        let resolver = tokio::select! {
                            _ = conn_stop.wait() => {
                                handle.close(0u8.into(), b"server shutting down");
                                return;
                            }
                            resolver = h3_connection.accept() => match resolver {
                                Ok(Some(resolver)) => resolver,
                                Ok(None) | Err(_) => {
                                    handle.close(0u8.into(), b"session ended");
                                    return;
                                }
                            },
                        };

                        let (request, mut stream) = match resolver.resolve_request().await {
                            Ok(pair) => pair,
                            Err(_) => {
                                handle.close(0u8.into(), b"session ended");
                                return;
                            }
                        };

                        // Gate evaluation on the request's header pairs —
                        // the HTTP-family handshake snapshot. A rejection
                        // answers the request with the rejection's status;
                        // the reason has no carrier on this transport.
                        let handshake = Handshake {
                            headers: request
                                .headers()
                                .iter()
                                .map(|(name, value)| {
                                    (
                                        name.as_str().to_string(),
                                        value.to_str().unwrap_or_default().to_string(),
                                    )
                                })
                                .collect(),
                            // The peer address rides the QUIC connection —
                            // gates and the per-remote quota see the real
                            // peer on this transport.
                            remote: Some(peer),
                        };
                        if let Err(rejection) = conn_state.gates.evaluate(&handshake) {
                            let status = StatusCode::from_u16(rejection.status)
                                .unwrap_or(StatusCode::FORBIDDEN);
                            // The status is always valid post-normalization.
                            let response = Response::builder()
                                .status(status)
                                .body(())
                                .expect("normalized status builds");
                            let _ = stream.send_response(response).await;
                            let _ = stream.finish().await;
                            continue;
                        }

                        let handler = (conn_state.requests)(request, stream);
                        tokio::spawn(handler);
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
