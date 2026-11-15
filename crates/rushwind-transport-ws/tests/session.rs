//! Integration tests: gated WebSocket sessions under the full RushWind
//! lifecycle, exercised with real WebSocket clients.
//!
//! Each test spins the complete stack — `WsRoute` mounted in a router,
//! served by `AxumServer` with its shutdown bus registered, driven by
//! `App::run` — and asserts one slice of the session contract:
//! gate rejection, admission-cap refusal, lifecycle-aligned session
//! teardown, and teardown covering *every* live session.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::WebSocket;
use axum::Router;
use futures::{SinkExt, StreamExt};
use rushwind_core::App;
use rushwind_transport::{
    GateVerdict, Handshake, HandshakeGate, Rejection, ServerError, StopSignal,
};
use rushwind_transport_axum::AxumServer;
use rushwind_transport_ws::WsRoute;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

struct AllowAll;

impl HandshakeGate for AllowAll {
    fn name(&self) -> String {
        "allow-all".to_string()
    }
    fn inspect(&self, _handshake: &Handshake) -> GateVerdict {
        GateVerdict::Continue
    }
}

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

async fn echo(mut socket: WebSocket) {
    while let Some(Ok(msg)) = socket.recv().await {
        if socket.send(msg).await.is_err() {
            break;
        }
    }
}

/// Builds the full lifecycle around `router` and returns the WebSocket
/// URL, the shutdown trigger, and the run task's join handle.
#[allow(clippy::type_complexity)]
async fn spawn_lifecycle(
    router: Router,
    bus: StopSignal,
) -> (
    String,
    StopSignal,
    tokio::task::JoinHandle<Result<(), ServerError>>,
) {
    let server = AxumServer::new(SocketAddr::from(([127, 0, 0, 1], 0)), router)
        .expect("ephemeral bind must succeed")
        .with_aux_shutdown(bus);
    let addr = server.local_addr();
    let app = Arc::new(
        App::builder()
            .erased_server(Arc::new(server))
            .stop_timeout(Duration::from_millis(500))
            .build(),
    );
    let run_app = Arc::clone(&app);
    let trigger = StopSignal::new();
    let run_trigger = trigger.clone();
    let handle = tokio::spawn(async move { run_app.run(run_trigger).await });
    (format!("ws://{addr}/ws"), trigger, handle)
}

/// Drives the run task to completion after triggering shutdown and asserts
/// a clean outcome.
async fn finish_lifecycle(
    trigger: StopSignal,
    run: tokio::task::JoinHandle<Result<(), ServerError>>,
) {
    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn session_echoes_and_dies_with_lifecycle() {
    let (route, bus) = WsRoute::new()
        .gate(Arc::new(AllowAll))
        .session_handler(echo)
        .build()
        .expect("route must build");
    let (url, trigger, run) = spawn_lifecycle(Router::new().route("/ws", route), bus).await;

    let (mut client, _resp) = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await
    .expect("connect must complete")
    .expect("connect must succeed");
    client
        .send(Message::Text("ping".into()))
        .await
        .expect("send must succeed");
    let echoed = timeout(Duration::from_secs(5), client.next())
        .await
        .expect("echo must arrive")
        .expect("stream must be open")
        .expect("echo must be ok");
    assert!(matches!(echoed, Message::Text(t) if t.as_str() == "ping"));

    // Lifecycle shutdown must tear the session down with the listener:
    // the client sees the close, and run returns a clean outcome.
    trigger.signal();
    let closed = timeout(Duration::from_secs(5), client.next())
        .await
        .expect("close must arrive");
    assert!(matches!(closed, Some(Err(_)) | None));
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

/// Upstream review round: `http.Server.Shutdown` leaves hijacked upgraded
/// sockets open, so the Go server's Stop now closes every session through
/// the session manager. The Rust counterpart is the session bus raced
/// against each session task — this pins that the bus closes **every**
/// live session, not just the first one that observes the signal.
#[tokio::test]
async fn stop_closes_every_live_session() {
    let (route, bus) = WsRoute::new()
        .gate(Arc::new(AllowAll))
        .session_handler(echo)
        .build()
        .expect("route must build");
    let (url, trigger, run) = spawn_lifecycle(Router::new().route("/ws", route), bus).await;

    let mut sessions = Vec::new();
    for _ in 0..2 {
        let (client, _resp) = timeout(
            Duration::from_secs(5),
            tokio_tungstenite::connect_async(url.as_str()),
        )
        .await
        .expect("connect must complete")
        .expect("connect must succeed");
        sessions.push(client);
    }

    trigger.signal();
    for (i, mut session) in sessions.into_iter().enumerate() {
        let closed = timeout(Duration::from_secs(5), session.next())
            .await
            .expect("close must arrive");
        assert!(
            matches!(closed, Some(Err(_)) | None),
            "session {i} stayed open past stop"
        );
    }
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn gate_rejection_refuses_upgrade() {
    let (route, bus) = WsRoute::new()
        .gate(Arc::new(DenyAll))
        .session_handler(echo)
        .build()
        .expect("route must build");
    let (url, trigger, run) = spawn_lifecycle(Router::new().route("/ws", route), bus).await;

    let refused = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await
    .expect("attempt must complete");
    match refused {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            assert_eq!(resp.status().as_u16(), 403);
        }
        other => panic!("expected http 403 refusal, got {other:?}"),
    }

    finish_lifecycle(trigger, run).await;
}

#[tokio::test]
async fn session_cap_refuses_second_session() {
    let (route, bus) = WsRoute::new()
        .max_concurrent_sessions(1)
        .session_handler(echo)
        .build()
        .expect("route must build");
    let (url, trigger, run) = spawn_lifecycle(Router::new().route("/ws", route), bus).await;

    // Hold one live session.
    let (holder, _resp) = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await
    .expect("connect must complete")
    .expect("first session must be admitted");
    // Let the server-side session callback register before the second
    // handshake.
    sleep(Duration::from_millis(300)).await;

    // The cap must refuse the second handshake at the pre-admission
    // check.
    let refused = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await
    .expect("attempt must complete");
    match refused {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            assert_eq!(resp.status().as_u16(), 429);
        }
        other => panic!("expected http 429 cap refusal, got {other:?}"),
    }

    drop(holder);
    finish_lifecycle(trigger, run).await;
}

/// A gate capturing the handshake it saw — the per-remote pin reads
/// the peer address the ConnectInfo pipeline now carries.
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

/// The ConnectInfo pipeline: the gate's handshake snapshot carries the
/// peer address on this transport — gates can rule on the real peer,
/// and Handshake.remote is Some.
#[tokio::test]
async fn gates_observe_peer_address() {
    let seen = Arc::new(std::sync::Mutex::new(None));
    let (route, bus) = WsRoute::new()
        .gate(Arc::new(PeerCapture(seen.clone())))
        .session_handler(echo)
        .build()
        .expect("route must build");
    let (url, trigger, run) = spawn_lifecycle(Router::new().route("/ws", route), bus).await;

    let (client, _resp) = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await
    .expect("connect must complete")
    .expect("connect must succeed");
    // Give the upgrade handler a beat to run the gate.
    sleep(Duration::from_millis(300)).await;

    let seen = seen.lock().expect("capture lock").clone();
    let Some(handshake) = seen else {
        panic!("the gate never observed a handshake");
    };
    let Some(remote) = handshake.remote else {
        panic!("the handshake must carry the peer address on this transport");
    };
    assert_eq!(remote.ip().to_string(), "127.0.0.1", "the loopback peer");
    assert!(remote.port() > 0, "an ephemeral client port");
    drop(client);
    finish_lifecycle(trigger, run).await;
}

/// The per-remote quota: one peer (the loopback, as every test client)
/// filling its per-IP budget is refused with 429 — the route-wide cap
/// is not even reached.
#[tokio::test]
async fn per_remote_cap_refuses_the_second_session() {
    let (route, bus) = WsRoute::new()
        .max_sessions_per_remote(1)
        .session_handler(echo)
        .build()
        .expect("route must build");
    let (url, trigger, run) = spawn_lifecycle(Router::new().route("/ws", route), bus).await;

    // Hold one live session from the loopback peer.
    let (holder, _resp) = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await
    .expect("connect must complete")
    .expect("first session must be admitted");
    sleep(Duration::from_millis(300)).await;

    // The same peer's second session refuses — its per-IP budget is
    // full even though the route-wide cap (unset) has room.
    let refused = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await
    .expect("attempt must complete");
    match refused {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            assert_eq!(resp.status().as_u16(), 429);
        }
        other => panic!("expected http 429 per-remote refusal, got {other:?}"),
    }

    drop(holder);
    finish_lifecycle(trigger, run).await;
}
