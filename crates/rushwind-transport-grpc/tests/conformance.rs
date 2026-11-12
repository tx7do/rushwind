//! The lifecycle conformance suite, run against the gRPC adapter —
//! the same endpoint-shape and cooperative-shutdown pins every
//! transport passes, over a fresh eager-bound server per test.

use std::net::SocketAddr;
use std::sync::Arc;

use rushwind_transport::Server;
use rushwind_transport_grpc::GrpcServer;

mod common;

/// A fresh eager-bound server per call: port 0, the echo routes.
fn grpc_server() -> Arc<dyn Server> {
    let address: SocketAddr = "127.0.0.1:0".parse().expect("static address");
    Arc::new(GrpcServer::new(address, common::echo_routes()).expect("bind works"))
}

rushwind_testkit::rushwind_conformance_suite!(crate::grpc_server);
