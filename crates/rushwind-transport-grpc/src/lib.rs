//! gRPC transport for the RushWind [`Server`] lifecycle contract —
//! tonic 0.14 serving caller-supplied [`Routes`].
//!
//! The adapter mirrors the axum one exactly: the listener binds
//! **eagerly at construction** (bind failures surface at the
//! registration site, `:0` reports the OS-assigned port through
//! [`Server::endpoint`] as `grpc://host:port`), the serve future runs
//! inside [`Server::start`] racing the stop signal, and tonic's own
//! graceful shutdown drains the in-flight RPCs before
//! `serve_with_incoming_shutdown` returns `Ok` — which the adapter
//! maps to the cooperative `Err(ServerError::Cancelled)` exit.
//! [`Server::stop`] is therefore empty: the stack released everything
//! on the start path (the axum adapter's shutdown-mapping rule 1).
//!
//! The routes come from the caller — proto-build's tonic generator
//! compiles the domain's services, the deployment overrides the
//! `Unimplemented` stubs, and this adapter wires the result into the
//! lifecycle. The crate stays corpus-free: it carries no proto of its
//! own.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::net::SocketAddr;
use std::sync::Mutex;

use rushwind_transport::{Server, ServerError, ServerFuture, StopSignal};
use tonic::service::Routes;
use tonic::transport::Server as TonicServer;

/// A gRPC server over a [`Routes`] table.
#[derive(Debug)]
pub struct GrpcServer {
    /// The eagerly bound listener, converted to an async listener on
    /// first start. Taken out of the option so a second start fails
    /// loudly.
    listener: Mutex<Option<std::net::TcpListener>>,
    /// The address actually bound (differs from the requested address
    /// when port 0 was asked for).
    local_addr: SocketAddr,
    /// The routes served on start. `Routes` is an `axum::Router`
    /// wrapper — cheaply cloneable.
    routes: Routes,
}

impl GrpcServer {
    /// Binds `addr` eagerly and stores `routes` for serving.
    ///
    /// Bind and non-blocking-mode errors are reported immediately —
    /// the registration site learns about an unusable address at
    /// registration time, not at lifecycle start.
    pub fn new(addr: SocketAddr, routes: Routes) -> Result<Self, ServerError> {
        let listener = std::net::TcpListener::bind(addr)
            .and_then(|l| l.set_nonblocking(true).map(|()| l))
            .map_err(|e| ServerError::Failed(format!("bind {addr}: {e}")))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| ServerError::Failed(format!("local_addr: {e}")))?;
        Ok(Self {
            listener: Mutex::new(Some(listener)),
            local_addr,
            routes,
        })
    }

    /// The actually-bound address. Equal to the requested address
    /// unless port 0 was requested, in which case this reports the
    /// OS-assigned port.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Server for GrpcServer {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(format!("grpc://{}", self.local_addr))
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        // Take the listener out before entering the async block: the
        // mutex guard must not live across an await point, and a
        // second start must fail deterministically.
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
        let routes = self.routes.clone();
        Box::pin(async move {
            let listener = match tokio::net::TcpListener::from_std(listener) {
                Ok(l) => l,
                Err(e) => {
                    return Err(ServerError::Failed(format!("from_std: {e}")));
                }
            };
            let incoming = tonic::transport::server::TcpIncoming::from(listener);
            // Per tonic's contract the serve future returns Ok only
            // after the shutdown signal fired and the in-flight RPC
            // drain completed — i.e. this is the cooperative exit.
            let served = TonicServer::builder()
                .add_routes(routes)
                .serve_with_incoming_shutdown(incoming, async move {
                    stop.wait().await;
                })
                .await;
            match served {
                Ok(()) => Err(ServerError::Cancelled),
                Err(e) => Err(ServerError::Failed(format!("serve: {e}"))),
            }
        })
    }

    fn stop(&self) -> ServerFuture<'_> {
        // The listener is already closed and drained inside start (the
        // axum adapter's shutdown-mapping rule 1). Nothing remains to
        // release here.
        Box::pin(async { Ok(()) })
    }
}
