//! Axum adapter for the RushWind lifecycle.
//!
//! [`AxumServer`] binds a TCP listener eagerly at construction and serves
//! the supplied [`axum::Router`] inside [`Server::start`]. Eager binding
//! means bind failures surface immediately at the registration site, and
//! that [`Server::endpoint`] reports the actually-bound address — including
//! the OS-assigned port for `:0` binds.
//!
//! # Shutdown mapping
//!
//! axum's graceful-shutdown mode is wired directly to the RushWind stop
//! signal. When the signal fires, axum stops accepting new connections and
//! drains in-flight requests; once the drain completes, [`Server::start`]
//! returns [`ServerError::Cancelled`] per the contract. By the time the
//! orchestrator's stop phase runs, the listener is already closed and there
//! is nothing left to release, so [`Server::stop`] is a no-op returning
//! success.
//!
//! The in-flight drain happens *inside* start, bounded by the orchestrator's
//! drain deadline: a connection that never completes is abandoned together
//! with the start future when that deadline lapses.
//!
//! # Scope
//!
//! Plain HTTP only. TLS termination belongs in a front proxy; if TLS
//! termination inside the process is ever needed, it arrives as a separate
//! adapter rather than a feature of this one.
//!
//! # Session-shutdown bus
//!
//! Routes that carry long-lived sessions (e.g.
//! `rushwind-transport-ws`) cannot see this server's stop signal on their
//! own — axum hands routes an opaque handler closure at assembly time,
//! before any lifecycle exists. [`AxumServer::with_aux_shutdown`] lets
//! such routes register a shutdown bus; when the lifecycle's stop signal
//! fires, this server relays it onto every registered bus so mounted
//! session handlers can wind down cooperatively instead of lingering
//! until the process exits. See `docs/session-middleware.md`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::net::SocketAddr;
use std::sync::Mutex;

use axum::Router;
use rushwind_transport::{Server, ServerError, ServerFuture, StopSignal};

/// An axum-backed HTTP server bound to the RushWind lifecycle.
///
/// Construct with [`AxumServer::new`], register with
/// [`App`](rushwind_core::App) via its builder, and the router is served
/// until the lifecycle's shutdown path runs.
pub struct AxumServer {
    /// The eagerly bound listener, converted to an async listener on first
    /// start. Taken out of the option so a second start fails loudly.
    listener: Mutex<Option<std::net::TcpListener>>,
    /// The address actually bound (differs from the requested address when
    /// port 0 was asked for).
    local_addr: SocketAddr,
    /// The router served on start. Cloned into the serve future because
    /// `axum::Router` is cheaply cloneable (it is an `Arc` internally).
    router: Router,
    /// Session-shutdown buses registered by mounted session routes; see the
    /// crate-level "Session-shutdown bus" section.
    aux_shutdown: Vec<StopSignal>,
}

impl AxumServer {
    /// Binds `addr` eagerly and stores `router` for serving.
    ///
    /// Bind and non-blocking-mode errors are reported immediately — the
    /// registration site learns about an unusable address at registration
    /// time, not at lifecycle start.
    pub fn new(addr: SocketAddr, router: Router) -> Result<Self, ServerError> {
        let listener = std::net::TcpListener::bind(addr)
            .and_then(|l| l.set_nonblocking(true).map(|()| l))
            .map_err(|e| ServerError::Failed(format!("bind {addr}: {e}")))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| ServerError::Failed(format!("local_addr: {e}")))?;
        Ok(Self {
            listener: Mutex::new(Some(listener)),
            local_addr,
            router,
            aux_shutdown: Vec::new(),
        })
    }

    /// The actually-bound address. Equal to the requested address unless
    /// port 0 was requested, in which case this reports the OS-assigned
    /// port.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Registers a session-shutdown bus (obtained from a session route
    /// builder such as `rushwind-transport-ws`'s `WsRoute::build`) with
    /// this server. When the lifecycle's stop signal fires, the signal is
    /// relayed onto every registered bus.
    ///
    /// Call before registering the server with the application — after
    /// [`Server::start`] runs, registration has no effect.
    pub fn with_aux_shutdown(mut self, bus: StopSignal) -> Self {
        self.aux_shutdown.push(bus);
        self
    }
}

impl Server for AxumServer {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(format!("http://{}", self.local_addr))
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        // Take the listener out before entering the async block: the mutex
        // guard must not live across an await point, and a second start
        // must fail deterministically.
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
        let app = self.router.clone();
        // Relay future: wait for the lifecycle stop signal, then propagate
        // it onto every registered session-shutdown bus so mounted session
        // handlers can wind down alongside the server itself.
        let aux = self.aux_shutdown.clone();
        Box::pin(async move {
            let listener = match tokio::net::TcpListener::from_std(listener) {
                Ok(l) => l,
                Err(e) => {
                    return Err(ServerError::Failed(format!("from_std: {e}")));
                }
            };
            let shutdown = async move {
                stop.wait().await;
                for bus in aux {
                    bus.signal();
                }
            };
            // ConnectInfo plumbing: the make-service stamps every
            // request with the peer's socket address, so session routes
            // (and any handler extracting ConnectInfo) see the remote —
            // the per-remote session quota's prerequisite.
            let served = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown)
            .await;
            match served {
                // Per axum's contract the serve future returns Ok only
                // after the shutdown signal fired and the drain completed —
                // i.e. this is the cooperative exit.
                Ok(()) => Err(ServerError::Cancelled),
                Err(e) => Err(ServerError::Failed(format!("serve: {e}"))),
            }
        })
    }

    fn stop(&self) -> ServerFuture<'_> {
        // The listener is already closed and drained inside start (see the
        // shutdown-mapping note in the crate docs). Nothing remains to
        // release here.
        Box::pin(async { Ok(()) })
    }
}
