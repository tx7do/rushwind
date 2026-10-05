//! The wire-contract tests: the preflight shape, the 401 plain-text
//! error line, the 403-under-a-401-status-line quirk for the
//! forbidden/rejected classes, the anti cross-user stream check, and
//! the per-user stream filtering — the behavior the reference
//! transport pins and the deployments carried verbatim.

use std::convert::Infallible;
use std::sync::Arc;

use axum::http::{Request, StatusCode};
use futures::StreamExt;
use rushwind_http_binding::envelope::StatusError;
use rushwind_transport_sse::{events, events_preflight, Failure, Gate, Hub, SseApp};
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

async fn text(res: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024)
        .await
        .unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

// The unused-import guards for the Infallible the stream carries.
#[allow(unused)]
fn _infallible_witness(_: Infallible) {}
