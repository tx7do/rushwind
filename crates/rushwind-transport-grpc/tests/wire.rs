//! Wire pins for the gRPC adapter: a real RPC round trip through the
//! tonic server (raw h2 client, every framing byte explicit), the
//! OS-assigned endpoint echo, and the error frames.

#![cfg(test)]

use std::net::SocketAddr;

use rushwind_transport::{Server, StopSignal};
use rushwind_transport_grpc::GrpcServer;

mod common;

fn server() -> GrpcServer {
    let address: SocketAddr = "127.0.0.1:0".parse().expect("static address");
    GrpcServer::new(address, common::echo_routes()).expect("bind works")
}

#[tokio::test]
async fn a_unary_rpc_round_trips_byte_exact() {
    let server = server();
    let address = server.local_addr();
    let handle = tokio::spawn(async move {
        let _ = server.start(StopSignal::new()).await;
    });
    // Wait for the listener to accept.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let message = b"hello grpc";
    let (echoed, grpc_status) = common::echo_call(address, message)
        .await
        .expect("the RPC must complete");
    assert_eq!(&echoed, message, "the echo must be byte-exact");
    assert_eq!(grpc_status, "0", "an echoed call carries grpc-status 0");

    handle.abort();
}

/// The empty message is a legal gRPC frame — the echo keeps it legal.
#[tokio::test]
async fn an_empty_message_round_trips() {
    let server = server();
    let address = server.local_addr();
    let handle = tokio::spawn(async move {
        let _ = server.start(StopSignal::new()).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let (echoed, grpc_status) = common::echo_call(address, b"")
        .await
        .expect("the RPC must complete");
    assert!(echoed.is_empty());
    assert_eq!(grpc_status, "0");

    handle.abort();
}

/// The lifecycle stop signal drains the serve future to the
/// cooperative exit — pinned end to end, with a live RPC completing
/// before the drain.
#[tokio::test]
async fn the_stop_signal_drains_cooperatively() {
    let server = server();
    let address = server.local_addr();
    let stop = StopSignal::new();
    let stop_for_task = stop.clone();
    let handle = tokio::spawn(async move { server.start(stop_for_task).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let (echoed, _) = common::echo_call(address, b"in-flight")
        .await
        .expect("an in-flight RPC completes");
    assert_eq!(&echoed, b"in-flight");

    stop.signal();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
        .await
        .expect("the serve future must end")
        .expect("task joins");
    assert!(
        matches!(outcome, Err(rushwind_transport::ServerError::Cancelled)),
        "the drain completes as the cooperative exit, got {outcome:?}"
    );
}
