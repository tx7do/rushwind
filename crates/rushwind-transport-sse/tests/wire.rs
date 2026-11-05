//! The wire-contract tests: the preflight shape, the 401 plain-text
//! error line, the 403-under-a-401-status-line quirk for the
//! forbidden/rejected classes, the anti cross-user stream check, the
//! per-user stream filtering, the fan-out payload identity across
//! concurrent subscribers, stream survival across empty payloads, the
//! non-blocking publish bound under a stalled receiver, and the
//! explicit 404/405 router fallbacks — the behavior the reference
//! transport pins and the deployments carried verbatim.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{Request, StatusCode};
use futures::StreamExt;
use rushwind_http_binding::envelope::StatusError;
use rushwind_transport_sse::{events, events_preflight, Failure, Gate, Hub, SseApp};
use tokio::time::timeout;
use tower::ServiceExt;

/// The stub gate: `good` authorizes userId 7, `blocked` answers the
/// forbidden class, everything else is an invalid token.
struct FakeGate;

impl Gate for FakeGate {
    async fn authorize(&self, token: &str) -> Result<u32, Failure> {
        match token {
            "good" => Ok(7),
            "blocked" => Err(Failure::new("FORBIDDEN", "token is blocked")),
            _ => Err(Failure::new("UNAUTHORIZED", "invalid token")),
        }
    }
}

/// The table-anchored status constructor stand-in (reason → status).
fn status_error(reason: &'static str, message: String) -> StatusError {
    let status = match reason {
        "FORBIDDEN" => 403,
        _ => 401,
    };
    StatusError::new(status, reason, message)
}

fn app() -> Arc<SseApp<FakeGate>> {
    Arc::new(SseApp {
        hub: Hub::default(),
        gate: Arc::new(FakeGate),
        status_error,
    })
}

fn router(app: Arc<SseApp<FakeGate>>) -> axum::Router {
    axum::Router::new().route(
        "/events",
        axum::routing::get(events::<FakeGate>)
            .options(events_preflight)
            .with_state(app),
    )
}

#[tokio::test]
async fn preflight_answers_before_any_authorization() {
    let res = router(app())
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/events")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        res.headers().get("access-control-allow-origin").unwrap(),
        "*"
    );
    assert_eq!(
        res.headers().get("access-control-allow-headers").unwrap(),
        "Content-Type, Authorization, X-Token, Last-Event-ID"
    );
}

#[tokio::test]
async fn missing_token_answers_the_plain_text_error_line() {
    let res = router(app())
        .oneshot(
            Request::builder()
                .uri("/events?stream=7")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        res.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
    let body = text(res).await;
    assert_eq!(
        body,
        "error: code = 401 reason = UNAUTHORIZED message = invalid token metadata = map[] cause = <nil>\n"
    );
}

#[tokio::test]
async fn forbidden_class_carries_403_code_under_the_401_status_line() {
    let res = router(app())
        .oneshot(
            Request::builder()
                .uri("/events?stream=7&token=blocked")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // The reference transport's forbidden-sentinel check never fires on
    // the generated status errors: the body's code carries 403 while
    // the status line stays 401.
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let body = text(res).await;
    assert!(
        body.starts_with("error: code = 403 reason = FORBIDDEN message = token is blocked"),
        "{body}"
    );
}

#[tokio::test]
async fn stream_must_be_the_authenticated_user() {
    let res = router(app())
        .oneshot(
            Request::builder()
                .uri("/events?stream=8&token=good")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let body = text(res).await;
    assert!(body.contains("reason = FORBIDDEN"), "{body}");
    assert!(body.contains("stream user mismatch"), "{body}");
}

#[tokio::test]
async fn the_stream_forwards_only_its_own_user() {
    let app = app();
    let res = router(Arc::clone(&app))
        .oneshot(
            Request::builder()
                .uri("/events?stream=7&token=good")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
    assert_eq!(res.headers().get("connection").unwrap(), "keep-alive");

    // Publish for another user first — the stream must never see it —
    // then this user's payload, which arrives as a framed event.
    app.hub.publish(8, r#"{"secret":"other"}"#.into());
    app.hub.publish(7, r#"{"title":"hello"}"#.into());

    let mut stream = res.into_body().into_data_stream();
    let mut seen_other = false;
    let own = loop {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("stream yields within 5s")
            .expect("stream open")
            .expect("chunk");
        let chunk = String::from_utf8_lossy(&chunk).into_owned();
        if chunk.contains("other") {
            seen_other = true;
        }
        if chunk.contains("hello") {
            break chunk;
        }
    };
    assert!(!seen_other, "another user's payload leaked: {seen_other}");
    assert!(own.contains("event: notification"), "{own}");
    assert!(own.contains("data: {\"title\":\"hello\"}"), "{own}");
    assert!(own.starts_with("id: "), "{own}");
}

/// Upstream review round: the Go hub encoded the shared event in place
/// during dispatch, so stream 2+ received base64-of-base64. The Rust hub
/// fans out via `tokio::sync::broadcast`, which clones the payload per
/// receiver and never mutates it in place — this pins that three
/// concurrent subscribers see byte-identical, single-encoded payloads.
#[tokio::test]
async fn fanout_delivers_identical_single_encoded_payloads() {
    let app = app();
    let mut bodies = Vec::new();
    for _ in 0..3 {
        let res = router(Arc::clone(&app))
            .oneshot(
                Request::builder()
                    .uri("/events?stream=7&token=good")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        bodies.push(res.into_body().into_data_stream());
    }

    app.hub.publish(7, r#"{"title":"fan"}"#.into());

    let expected = r#"data: {"title":"fan"}"#;
    for mut body in bodies {
        let chunk = timeout(Duration::from_secs(5), body.next())
            .await
            .expect("stream yields within 5s")
            .expect("stream open")
            .expect("chunk");
        let chunk = String::from_utf8_lossy(&chunk).into_owned();
        let data = chunk
            .lines()
            .find(|line| line.starts_with("data: "))
            .expect("every subscriber sees a data line");
        assert_eq!(data, expected, "{chunk}");
    }
}

/// Upstream review round: the Go send loop broke on an event with no
/// Data and no Comment, disconnecting clients on legal metadata-only
/// frames (Retry/ID/Event). The Rust counterpart of such a frame is the
/// empty payload — the stream loop must skip it and keep the connection
/// open; only the explicit close sentinel (the hub's sender dropping)
/// terminates the loop.
#[tokio::test]
async fn empty_payload_does_not_kill_the_stream() {
    let app = app();
    let res = router(Arc::clone(&app))
        .oneshot(
            Request::builder()
                .uri("/events?stream=7&token=good")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let mut stream = res.into_body().into_data_stream();

    app.hub.publish(7, String::new());

    // The connection survived the empty payload: a real event published
    // afterwards still arrives on the same stream.
    app.hub.publish(7, r#"{"title":"alive"}"#.into());
    let chunk = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("stream yields within 5s")
        .expect("stream must still be open")
        .expect("chunk");
    let chunk = String::from_utf8_lossy(&chunk).into_owned();
    assert!(chunk.contains(r#"data: {"title":"alive"}"#), "{chunk}");
}

/// Upstream review round: the Go SendMessage channel send could block
/// forever on a stalled write pump, dragging every broadcaster with it.
/// The Rust hub's ring is bounded (1024) and `publish` is a non-blocking
/// send — overflowing it drops for the stalled receiver, it never blocks
/// the publisher.
#[tokio::test]
async fn publish_never_blocks_on_a_stalled_receiver() {
    let app = app();
    let res = router(Arc::clone(&app))
        .oneshot(
            Request::builder()
                .uri("/events?stream=7&token=good")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    // Hold the stream open without ever reading it: the subscriber stalls.
    let _stalled = res.into_body();

    // More events than the ring holds, against the stalled receiver: the
    // loop must sail through (a blocking fan-out would hang here).
    let flood = async {
        for i in 0..4096 {
            app.hub.publish(7, format!(r#"{{"i":{i}}}"#));
        }
    };
    timeout(Duration::from_secs(5), flood)
        .await
        .expect("publish must never block on a stalled receiver");
}

/// Upstream review round: the Go mux's NotFound/MethodNotAllowed fell
/// through to the global default handler, exposing whatever else was
/// registered on the shared mux. The Rust factory mounts the events
/// route on a fresh per-server router, whose built-in fallbacks answer
/// unknown paths with 404 and known-path-wrong-method with 405 — nothing
/// leaks into other registered services.
#[tokio::test]
async fn unknown_paths_and_wrong_methods_answer_explicit_fallbacks() {
    let not_found = router(app())
        .oneshot(
            Request::builder()
                .uri("/nope")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(not_found.status(), StatusCode::NOT_FOUND);

    let not_allowed = router(app())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/events")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(not_allowed.status(), StatusCode::METHOD_NOT_ALLOWED);
}

async fn text(res: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024)
        .await
        .unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

// The unused-import guards for the Infallible the stream carries.
#[allow(unused)]
fn _infallible_witness(_: Infallible) {}
