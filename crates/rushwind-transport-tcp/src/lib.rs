//! Raw TCP transport for the RushWind [`Server`] lifecycle contract —
//! the session matrix's last column, carrying the full session
//! middleware face on a bare socket.
//!
//! # The shape
//!
//! The listener binds eagerly at construction (bind failures surface
//! at the registration site); the accept loop races the stop signal
//! inside [`Server::start`]. Every admitted connection carries the
//! session middleware's enforcement points:
//!
//! - **Gate chain** at accept time on the handshake snapshot — the
//!   peer address is the whole snapshot (a bare socket presents no
//!   headers); rejection closes the socket immediately, the protocol's
//!   refusal primitive for a connection nobody spoke on yet.
//! - **Route-wide + per-remote quotas** admitted atomically at accept
//!   through `admit_with_remote` (the peer address is native here);
//!   over-cap connections close before the session handler runs.
//! - **Byte budget** needs no engine-side enforcement: the engine
//!   reads nothing before the session runs, and a session that reads
//!   slowly backpressures the peer through TCP flow control — the
//!   unauthenticated peer's send window is the budget. The session
//!   handler owns every byte from there; in-band authentication is
//!   the app protocol's opening move, as with any raw socket service.
//! - **Handshake timeout**: N/A — a TCP connection *is* established at
//!   accept; there is no observable engine-side handshake phase to
//!   race (the session-middleware doc's matrix records this per
//!   column).
//!
//! The session handler receives the native [`TcpStream`] — no framing,
//! no wrapping; read and write are the app protocol's.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use rushwind_transport::{
    GateChain, Handshake, HandshakeGate, Server, ServerError, ServerFuture, SessionCounter,
    SessionPolicy, StopSignal,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

/// The session-handler future type.
pub type SessionFuture = Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// The session-handler function type: receives the live socket.
pub type SessionHandlerFn = Box<dyn Fn(TcpSession) -> SessionFuture + Send + Sync>;

/// A raw TCP server carrying the session middleware face.
pub struct TcpServer {
    /// The eagerly bound listener, converted to an async listener on
    /// first start. Taken out of the option so a second start fails
    /// loudly.
    listener: Mutex<Option<std::net::TcpListener>>,
    local_addr: SocketAddr,
    gates: GateChain,
    policy: SessionPolicy,
    session: SharedSessionHandler,
    closed: Arc<AtomicBool>,
}

/// The native session type: the raw socket behind a Passthrough
/// wrapper. Exposes `AsyncRead`/`AsyncWrite` — the app protocol reads
/// and writes it like any socket.
#[derive(Debug)]
pub struct TcpSession {
    inner: TcpStream,
}

impl AsyncRead for TcpSession {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpSession {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl TcpSession {
    /// The peer address of the underlying socket.
    pub fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.peer_addr()
    }
}

/// Builder for [`TcpServer`].
pub struct Builder {
    addr: SocketAddr,
    gates: GateChain,
    policy: SessionPolicy,
    session: Option<SessionHandlerFn>,
}

impl Builder {
    /// Appends a handshake gate. Gates run in registration order at
    /// accept time; the first rejection closes the socket.
    pub fn gate(mut self, gate: Arc<dyn HandshakeGate>) -> Self {
        self.gates = self.gates.gate(gate);
        self
    }

    /// Caps simultaneously live sessions on this server.
    pub fn max_concurrent_sessions(mut self, n: usize) -> Self {
        self.policy = self.policy.max_concurrent_sessions(n);
        self
    }

    /// Caps simultaneously live sessions per peer IP.
    pub fn max_sessions_per_remote(mut self, n: usize) -> Self {
        self.policy = self.policy.max_sessions_per_remote(n);
        self
    }

    /// Sets the session handler invoked for every admitted connection.
    /// The handler runs until it returns, the peer closes, or the
    /// lifecycle's stop signal fires — whichever comes first.
    pub fn session_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(TcpSession) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.session = Some(Box::new(move |session: TcpSession| {
            Box::pin(handler(session)) as SessionFuture
        }));
        self
    }

    /// Eagerly binds `addr` and freezes the builder into a server.
    pub fn build(self) -> Result<TcpServer, ServerError> {
        let session = self
            .session
            .ok_or_else(|| ServerError::Failed("tcp server has no session handler".to_string()))?;
        let listener = std::net::TcpListener::bind(self.addr)
            .and_then(|l| l.set_nonblocking(true).map(|()| l))
            .map_err(|e| ServerError::Failed(format!("bind {}: {e}", self.addr)))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| ServerError::Failed(format!("local_addr: {e}")))?;
        Ok(TcpServer {
            listener: Mutex::new(Some(listener)),
            local_addr,
            gates: self.gates,
            policy: self.policy,
            session: Arc::from(session),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }
}

impl TcpServer {
    /// Starts a builder bound to `addr`.
    pub fn builder(addr: SocketAddr) -> Builder {
        Builder {
            addr,
            gates: GateChain::new(),
            policy: SessionPolicy::new(),
            session: None,
        }
    }

    /// The actually-bound address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Server for TcpServer {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(format!("tcp://{}", self.local_addr))
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        let listener = match self.listener.lock().expect("listener poisoned").take() {
            Some(l) => l,
            None => {
                return Box::pin(async {
                    Err(ServerError::Failed(
                        "start called more than once".to_string(),
                    ))
                })
            }
        };
        let state = ServerState {
            gates: self.gates.clone(),
            policy: self.policy.clone(),
            session: Arc::clone(&self.session),
            counter: SessionCounter::new(),
        };
        Box::pin(async move {
            let listener = match tokio::net::TcpListener::from_std(listener) {
                Ok(l) => l,
                Err(e) => return Err(ServerError::Failed(format!("from_std: {e}"))),
            };
            loop {
                let (socket, peer) = tokio::select! {
                    _ = stop.wait() => return Err(ServerError::Cancelled),
                    accepted = listener.accept() => match accepted {
                        Ok(pair) => pair,
                        Err(_) => continue,
                    },
                };

                // Gate evaluation on the raw-transport handshake snapshot:
                // the peer address, and nothing else. Rejection closes the
                // socket — no bytes were spoken, so no refusal record has
                // a carrier.
                let handshake = Handshake {
                    headers: Vec::new(),
                    remote: Some(peer),
                };
                if state.gates.evaluate(&handshake).is_err() {
                    drop(socket);
                    continue;
                }

                // Atomic admission under both caps (the peer address is
                // native here); over-cap connections close before any
                // session runs.
                // The admission guards live for the session's lifetime:
                // they ride the spawned task and drop when the session
                // ends (or the task aborts), freeing both slots.
                let (guard, per_remote) = match state.counter.admit_with_remote(
                    state.policy.get_max_concurrent_sessions(),
                    state.policy.get_max_sessions_per_remote(),
                    Some(peer),
                ) {
                    Some(guards) => guards,
                    None => {
                        drop(socket);
                        continue;
                    }
                };

                let session_handler = (state.session)(TcpSession { inner: socket });
                tokio::spawn(async move {
                    let _guard = guard;
                    let _per_remote = per_remote;
                    session_handler.await;
                });
            }
        })
    }

    fn stop(&self) -> ServerFuture<'_> {
        // The listener closes when start's future is dropped at the
        // drain (the axum adapter's rule 1); live sessions are app
        // tasks joined by the runtime at shutdown.
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

/// Shared server state.
type SharedSessionHandler = Arc<dyn Fn(TcpSession) -> SessionFuture + Send + Sync>;

struct ServerState {
    gates: GateChain,
    policy: SessionPolicy,
    session: SharedSessionHandler,
    counter: SessionCounter,
}

/// Re-boxes the handler: the stored `SessionHandlerFn` returns the
/// crate-local future type, the per-connection spawn needs a
impl std::fmt::Debug for Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("addr", &self.addr)
            .field("gates", &self.gates.gate_names())
            .field("policy", &"SessionPolicy")
            .field("session", &self.session.as_ref().map(|_| "set"))
            .finish()
    }
}

impl std::fmt::Debug for TcpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpServer")
            .field("local_addr", &self.local_addr)
            .field("gates", &self.gates.gate_names())
            .field("policy", &"SessionPolicy")
            .field("session", &"set")
            .finish()
    }
}
