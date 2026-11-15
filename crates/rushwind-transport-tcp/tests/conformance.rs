//! The lifecycle conformance suite, run against the raw TCP adapter —
//! the same endpoint-shape and cooperative-shutdown pins every
//! transport passes, over a fresh eager-bound server per test.

use std::net::SocketAddr;
use std::sync::Arc;

use rushwind_transport::Server;

mod echo;
use echo::echo_server;

fn tcp_server() -> Arc<dyn Server> {
    Arc::new(echo_server(SocketAddr::from(([127, 0, 0, 1], 0))))
}

rushwind_testkit::rushwind_conformance_suite!(crate::tcp_server);
