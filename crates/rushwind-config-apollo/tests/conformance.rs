//! Apollo source conformance against an in-process mock of the two
//! REST faces the engine speaks — config read and notifications
//! long-poll — no external Apollo needed.

#![cfg(test)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_config::Source;
use rushwind_config_apollo::{ApolloOptions, ApolloSource};

/// The mutable state behind the mock: the namespace's configurations
/// JSON (`None` → 404) and the release version notifications report.
#[derive(Default)]
struct ApolloMock {
    body: Mutex<Option<String>>,
    version: AtomicU64,
}

impl ApolloMock {
    fn serve(self: Arc<Self>, mut stream: tokio::net::TcpStream) {
        tokio::spawn(async move {
            let mut buffer = [0u8; 2048];
            let read = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer)
                .await
                .unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            // "GET /configs/… HTTP/1.1" — the second token is the target.
            let target = request.split_whitespace().nth(1).unwrap_or("/");
            let (path, query) = target.split_once('?').unwrap_or((target, ""));

            let namespace = query
                .split('&')
                .find_map(|pair| pair.strip_prefix("notifications="))
                .and_then(|encoded| {
                    // The probe arrives percent-encoded; the mock only
                    // needs the namespaceName out of it.
                    encoded.find("namespaceName").and_then(|at| {
                        let rest = &encoded[at..];
                        let start = rest.find("%22").map(|i| i + 3)?..;
                        let rest = &rest[start];
                        let end = rest.find("%22")?;
                        Some(rest[..end].replace("%20", " "))
                    })
                });

            let response = if path.starts_with("/configs/") {
                match *self.body.lock().expect("mock body") {
                    Some(ref body) => ok_json(body.clone()),
                    None => {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string()
                    }
                }
            } else if path == "/notifications/v2" {
                // Instant answers (no hold): 200 with the current
                // version — the engine's poll gap paces the loop.
                let version = self.version.load(Ordering::SeqCst);
                let namespace = namespace.unwrap_or_else(|| "application".to_string());
                ok_json(
                    serde_json::json!([{
                        "namespaceName": namespace,
                        "notificationId": version,
                    }])
                    .to_string(),
                )
            } else {
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string()
            };
            let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes()).await;
        });
    }
}

fn ok_json(body: String) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Boots the mock; returns its base URL and the shared state handle.
async fn spawn_apollo(body: Option<String>) -> (String, Arc<ApolloMock>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind works");
    let addr = listener.local_addr().expect("local addr");
    let mock = Arc::new(ApolloMock {
        body: Mutex::new(body),
        version: AtomicU64::new(1),
    });
    let server = mock.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            server.clone().serve(stream);
        }
    });
    (format!("http://{addr}"), mock)
}

fn source(server: &str) -> ApolloSource {
    ApolloSource::new(ApolloOptions {
        server: server.to_string(),
        app_id: "rushwind-test".to_string(),
        poll_gap: Duration::from_millis(50),
        ..ApolloOptions::default()
    })
    .expect("options valid")
}

/// A read serves the canonical key-sorted JSON of the configurations
/// map.
#[tokio::test]
async fn load_serves_canonical_map_json() {
    let (server, _mock) = spawn_apollo(Some(
        r#"{"configurations":{"beta":"2","alpha":"1"}}"#.to_string(),
    ))
    .await;
    let source = source(&server);
    let value = source.load("application").await.expect("load must succeed");
    assert_eq!(value, Some(br#"{"alpha":"1","beta":"2"}"#.to_vec()));
}

/// A 404 namespace is the contract's absent answer.
#[tokio::test]
async fn missing_namespace_reads_absent() {
    let (server, _mock) = spawn_apollo(None).await;
    let source = source(&server);
    let value = source.load("application").await.expect("load must succeed");
    assert!(value.is_none(), "404 maps to Ok(None)");
}

/// Construction refuses an unset server or app id.
#[tokio::test]
async fn construction_requires_server_and_app() {
    assert!(ApolloSource::new(ApolloOptions::default()).is_err());
    assert!(ApolloSource::new(ApolloOptions {
        server: "http://apollo".to_string(),
        ..ApolloOptions::default()
    })
    .is_err());
}

/// The long-poll watch pushes the value on release, stays quiet on
/// same-content releases, and pushes again after a real change.
#[tokio::test]
async fn watch_pushes_on_release_changes() {
    let body = r#"{"configurations":{"feature-x":"off"}}"#.to_string();
    let (server, mock) = spawn_apollo(Some(body)).await;
    let source = source(&server);

    let mut stream = source
        .watch_value("application")
        .await
        .expect("watch must succeed");

    // The first release (version 1) pushes the current value.
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first push arrives")
        .expect("stream open");
    assert_eq!(first, br#"{"feature-x":"off"}"#.to_vec());

    // A version bump with the same canonical content notifies, but the
    // engine does not re-push unchanged bytes.
    mock.version.store(2, Ordering::SeqCst);
    let unchanged = tokio::time::timeout(Duration::from_millis(700), stream.next()).await;
    assert!(unchanged.is_err(), "unchanged content must not re-push");

    // A real release changes the content: the next push carries it.
    *mock.body.lock().expect("mock body") =
        Some(r#"{"configurations":{"feature-x":"on"}}"#.to_string());
    mock.version.store(3, Ordering::SeqCst);
    let second = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("changed push arrives")
        .expect("stream open");
    assert_eq!(second, br#"{"feature-x":"on"}"#.to_vec());
}
