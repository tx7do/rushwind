//! Wire pins for the raw TCP transport: the echo round trip, the gate
//! chain refusing on the peer address, the route-wide cap, and the
//! per-remote cap (the peer address is native here — the last column
//! of the session matrix).

#![cfg(test)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rushwind_transport::{GateVerdict, Handshake, HandshakeGate, Rejection, Server, StopSignal};
use rushwind_transport_tcp::{TcpServer, TcpSession};

mod echo;
use echo::{echo_round_trip, echo_server};

/// A gate capturing the handshake it saw.
struct PeerCapture(Arc<std::sync::Mutex<Option<Handshake>>>);

impl HandshakeGate for PeerCapture {
    fn name(&self) -> String {
        "peer-capture".to_string()
    }
    fn inspect(&self, handshake: &Handshake) -> GateVerdict {
        *self.0.lock().expect("capture lock") = Some(handshake.clone());
        GateVerdict::Continue
    }
}

/// A gate refusing everything.
struct DenyAll;

impl HandshakeGate for DenyAll {
    fn name(&self) -> String {
        "deny-all".to_string()
    }
    fn inspect(&self, _handshake: &Handshake) -> GateVerdict {
        GateVerdict::Reject(Rejection {
            status: 403,
            reason: "denied by test gate".to_string(),
        })
    }
}

#[tokio::test]
async fn a_session_echoes_bytes() {
    let server = echo_server(SocketAddr::from(([127, 0, 0, 1], 0)));
    let address = server.local_addr();
    let handle = tokio::spawn(async move {
        let _ = server.start(StopSignal::new()).await;
    });

    let echoed = echo_round_trip(address, b"hello tcp")
        .await
        .expect("the round trip must complete");
    assert_eq!(echoed, b"hello tcp");

    handle.abort();
}

#[tokio::test]
async fn the_gate_sees_the_peer_address() {
    let seen = Arc::new(std::sync::Mutex::new(None));
    let capture = seen.clone();
    let server = TcpServer::builder(SocketAddr::from(([127, 0, 0, 1], 0)))
        .gate(Arc::new(PeerCapture(capture)))
        .session_handler(|_session: TcpSession| async move {})
        .build()
        .expect("bind works");
    let address = server.local_addr();
    let handle = tokio::spawn(async move {
        let _ = server.start(StopSignal::new()).await;
    });

    // A client connects; the gate fires at accept with the peer.
    let _socket = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect succeeds");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while seen.lock().expect("capture").is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the gate never observed a handshake"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let handshake = seen.lock().expect("capture").clone().expect("handshake");
    let Some(remote) = handshake.remote else {
        panic!("the handshake must carry the peer address on this transport");
    };
    assert_eq!(remote.ip().to_string(), "127.0.0.1", "the loopback peer");

    handle.abort();
}

#[tokio::test]
async fn a_denying_gate_closes_connections() {
    let server = TcpServer::builder(SocketAddr::from(([127, 0, 0, 1], 0)))
        .gate(Arc::new(DenyAll))
        .session_handler(|_session: TcpSession| async move {})
        .build()
        .expect("bind works");
    let address = server.local_addr();
    let handle = tokio::spawn(async move {
        let _ = server.start(StopSignal::new()).await;
    });

    // The connect itself may succeed (the accept happened) but the
    // socket closes with no session: any read or write fails.
    let mut socket = tokio::net::TcpStream::connect(address)
        .await
        .expect("the tcp connect completes");
    use tokio::io::AsyncWriteExt;
    let outcome = socket.write_all(b"hello").await;
    // Either the write fails (socket already closed) or the subsequent
    // read sees EOF — either way the refused peer gets no session.
    match outcome {
        Err(_) => {}
        Ok(_) => {
            use tokio::io::AsyncReadExt;
            let mut buffer = [0u8; 8];
            let read = socket.read(&mut buffer).await.unwrap_or(0);
            assert_eq!(read, 0, "a gate-refused connection carries no session");
        }
    }

    handle.abort();
}

#[tokio::test]
async fn per_remote_cap_refuses_same_peer_connections() {
    // Every test client comes from the loopback: a per-IP cap of 1
    // admits exactly one connection.
    let server = TcpServer::builder(SocketAddr::from(([127, 0, 0, 1], 0)))
        .max_sessions_per_remote(1)
        .session_handler(|mut session: TcpSession| async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buffer = [0u8; 256];
            loop {
                let read = match session.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => read,
                };
                if session.write_all(&buffer[..read]).await.is_err() {
                    return;
                }
            }
        })
        .build()
        .expect("bind works");
    let address = server.local_addr();
    let handle = tokio::spawn(async move {
        let _ = server.start(StopSignal::new()).await;
    });

    // The first connection stays OPEN: its session holds the peer's
    // single per-IP slot.
    let holder = tokio::net::TcpStream::connect(address)
        .await
        .expect("the first connection completes");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The same peer's second connection closes with no session — the
    // per-IP budget is held by the live one.
    let mut second = tokio::net::TcpStream::connect(address)
        .await
        .expect("the tcp connect completes");
    tokio::time::sleep(Duration::from_millis(100)).await;
    use tokio::io::AsyncReadExt;
    let mut buffer = [0u8; 8];
    let read = second.read(&mut buffer).await.unwrap_or(0);
    assert_eq!(read, 0, "the over-cap connection carries no session");

    drop(holder);
    handle.abort();
}
