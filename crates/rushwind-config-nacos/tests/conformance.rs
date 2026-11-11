//! Nacos source conformance against an in-process mock of the one
//! REST face the engine speaks — the v1 cs/configs read — no external
//! Nacos needed.

#![cfg(test)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_config::Source;
use rushwind_config_nacos::{NacosOptions, NacosSource};

/// The mutable state behind the mock: the config's stored content
/// (`None` → 404) and the query the last request carried.
#[derive(Default)]
struct NacosMock {
    body: Mutex<Option<String>>,
    last_query: Mutex<String>,
}

impl NacosMock {
    fn serve(self: Arc<Self>, mut stream: tokio::net::TcpStream) {
        tokio::spawn(async move {
            let mut buffer = [0u8; 2048];
            let read = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer)
                .await
                .unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            // "GET /nacos/v1/cs/configs?… HTTP/1.1" — the second token
            // is the target.
            let target = request.split_whitespace().nth(1).unwrap_or("/");
            let (path, query) = target.split_once('?').unwrap_or((target, ""));
            *self.last_query.lock().expect("mock query") = query.to_string();

            let response = if path == "/nacos/v1/cs/configs" {
                match *self.body.lock().expect("mock body") {
                    Some(ref body) => ok_content(body.clone()),
                    None => {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string()
                    }
                }
            } else {
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string()
            };
            let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes()).await;
        });
    }

    fn last_query(&self) -> String {
        self.last_query.lock().expect("mock query").clone()
    }
}

fn ok_content(body: String) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Boots the mock; returns its base URL and the shared state handle.
async fn spawn_nacos(body: Option<String>) -> (String, Arc<NacosMock>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind works");
    let addr = listener.local_addr().expect("local addr");
    let mock = Arc::new(NacosMock {
        body: Mutex::new(body),
        last_query: Mutex::new(String::new()),
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

fn source(server: &str) -> NacosSource {
    NacosSource::new(NacosOptions {
        server: server.to_string(),
        data_id: "app.properties".to_string(),
        namespace: "test-ns".to_string(),
        access_token: Some("tok-1".to_string()),
        poll_interval: Duration::from_millis(50),
        ..NacosOptions::default()
    })
    .expect("options valid")
}

/// A read serves the stored content bytes verbatim.
#[tokio::test]
async fn load_serves_raw_content() {
    let (server, _mock) = spawn_nacos(Some("app.name=rushwind\napp.port=8080".to_string())).await;
    let source = source(&server);
    let value = source
        .load("app.properties")
        .await
        .expect("load must succeed");
    assert_eq!(value, Some(b"app.name=rushwind\napp.port=8080".to_vec()));
}

/// The read carries dataId, group, tenant and the access token.
#[tokio::test]
async fn the_read_carries_identifiers_and_token() {
    let (server, mock) = spawn_nacos(Some("x=1".to_string())).await;
    let source = source(&server);
    source.load("app.properties").await.expect("load");
    let query = mock.last_query();
    assert!(query.contains("dataId=app.properties"), "{query}");
    assert!(query.contains("group=DEFAULT_GROUP"), "{query}");
    assert!(query.contains("tenant=test-ns"), "{query}");
    assert!(query.contains("accessToken=tok-1"), "{query}");

    // A namespace-less, token-less engine omits both parameters.
    let (server, mock) = spawn_nacos(Some("x=1".to_string())).await;
    let source = NacosSource::new(NacosOptions {
        server: server.to_string(),
        data_id: "app.properties".to_string(),
        ..NacosOptions::default()
    })
    .unwrap();
    source.load("").await.expect("default dataId load");
    let query = mock.last_query();
    assert!(query.contains("dataId=app.properties"), "{query}");
    assert!(!query.contains("tenant="), "{query}");
    assert!(!query.contains("accessToken="), "{query}");
}

/// A 404 config is the contract's absent answer.
#[tokio::test]
async fn missing_config_reads_absent() {
    let (server, _mock) = spawn_nacos(None).await;
    let source = source(&server);
    let value = source
        .load("app.properties")
        .await
        .expect("load must succeed");
    assert!(value.is_none(), "404 maps to Ok(None)");
}

/// Construction refuses an unset server or dataId.
#[tokio::test]
async fn construction_requires_server_and_data_id() {
    assert!(NacosSource::new(NacosOptions::default()).is_err());
    assert!(NacosSource::new(NacosOptions {
        server: "http://nacos".to_string(),
        ..NacosOptions::default()
    })
    .is_err());
}

/// The poll watch pushes the first read, stays quiet on unchanged
/// polls, and pushes again after a real change.
#[tokio::test]
async fn watch_pushes_on_changes() {
    let (server, mock) = spawn_nacos(Some("feature-x=off".to_string())).await;
    let source = source(&server);

    let mut stream = source
        .watch_value("app.properties")
        .await
        .expect("watch must succeed");

    // The first read pushes the current value.
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first push arrives")
        .expect("stream open");
    assert_eq!(first, b"feature-x=off".to_vec());

    // Unchanged polls do not re-push.
    let unchanged = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(unchanged.is_err(), "unchanged content must not re-push");

    // A real change pushes the new bytes.
    *mock.body.lock().expect("mock body") = Some("feature-x=on".to_string());
    let second = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("changed push arrives")
        .expect("stream open");
    assert_eq!(second, b"feature-x=on".to_vec());
}
