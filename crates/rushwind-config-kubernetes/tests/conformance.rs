//! Kubernetes ConfigMap source conformance against an in-process mock
//! of the one REST face the engine speaks — the v1 configmaps read —
//! no external cluster needed.

#![cfg(test)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_config::Source;
use rushwind_config_kubernetes::{KubernetesOptions, KubernetesSource};

/// The mutable state behind the mock: the ConfigMap's data JSON
/// (`None` → 404) and the last request's target + headers.
#[derive(Default)]
struct KubernetesMock {
    body: Mutex<Option<String>>,
    last_target: Mutex<String>,
    last_authorization: Mutex<String>,
}

impl KubernetesMock {
    fn serve(self: Arc<Self>, mut stream: tokio::net::TcpStream) {
        tokio::spawn(async move {
            let mut buffer = [0u8; 4096];
            let read = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer)
                .await
                .unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            let mut lines = request.lines();
            let request_line = lines.next().unwrap_or_default();
            let target = request_line.split_whitespace().nth(1).unwrap_or("/");
            let authorization = lines
                .take_while(|line| !line.is_empty())
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.trim()
                        .eq_ignore_ascii_case("authorization")
                        .then(|| value.trim().to_string())
                })
                .unwrap_or_default();
            *self.last_target.lock().expect("mock target") = target.to_string();
            *self.last_authorization.lock().expect("mock auth") = authorization;

            let response = if target.starts_with("/api/v1/namespaces/") {
                match *self.body.lock().expect("mock body") {
                    Some(ref body) => ok_json(body.clone()),
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

    fn last_target(&self) -> String {
        self.last_target.lock().expect("mock target").clone()
    }

    fn last_authorization(&self) -> String {
        self.last_authorization.lock().expect("mock auth").clone()
    }
}

fn ok_json(body: String) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Boots the mock; returns its base URL and the shared state handle.
async fn spawn_kubernetes(body: Option<String>) -> (String, Arc<KubernetesMock>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind works");
    let addr = listener.local_addr().expect("local addr");
    let mock = Arc::new(KubernetesMock::default());
    *mock.body.lock().expect("mock body") = body;
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

fn source(server: &str) -> KubernetesSource {
    KubernetesSource::new(KubernetesOptions {
        server: server.to_string(),
        namespace: "rushwind".to_string(),
        configmap: "app-config".to_string(),
        token: Some("sa-token".to_string()),
        poll_interval: Duration::from_millis(50),
        ..KubernetesOptions::default()
    })
    .expect("options valid")
}

/// A read serves the canonical key-sorted JSON of the ConfigMap's
/// data map.
#[tokio::test]
async fn load_serves_canonical_map_json() {
    let (server, mock) = spawn_kubernetes(Some(
        r#"{"kind":"ConfigMap","metadata":{"name":"app-config"},"data":{"beta":"2","alpha":"1"}}"#
            .to_string(),
    ))
    .await;
    let source = source(&server);
    let value = source.load("app-config").await.expect("load must succeed");
    assert_eq!(value, Some(br#"{"alpha":"1","beta":"2"}"#.to_vec()));

    let target = mock.last_target();
    assert!(
        target.starts_with("/api/v1/namespaces/rushwind/configmaps/app-config"),
        "the read hits the namespace-scoped configmaps path, got {target}"
    );
    assert_eq!(mock.last_authorization(), "Bearer sa-token");
}

/// A 404 ConfigMap is the contract's absent answer.
#[tokio::test]
async fn missing_configmap_reads_absent() {
    let (server, _mock) = spawn_kubernetes(None).await;
    let source = source(&server);
    let value = source.load("app-config").await.expect("load must succeed");
    assert!(value.is_none(), "404 maps to Ok(None)");
}

/// Construction refuses an unset server or ConfigMap name.
#[tokio::test]
async fn construction_requires_server_and_configmap() {
    assert!(KubernetesSource::new(KubernetesOptions::default()).is_err());
    assert!(KubernetesSource::new(KubernetesOptions {
        server: "https://kubernetes.default.svc".to_string(),
        ..KubernetesOptions::default()
    })
    .is_err());
}

/// The poll watch pushes the first read, stays quiet on unchanged
/// polls, and pushes again after a real change.
#[tokio::test]
async fn watch_pushes_on_changes() {
    let body = r#"{"data":{"feature-x":"off"}}"#.to_string();
    let (server, mock) = spawn_kubernetes(Some(body)).await;
    let source = source(&server);

    let mut stream = source
        .watch_value("app-config")
        .await
        .expect("watch must succeed");

    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first push arrives")
        .expect("stream open");
    assert_eq!(first, br#"{"feature-x":"off"}"#.to_vec());

    let unchanged = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(unchanged.is_err(), "unchanged content must not re-push");

    *mock.body.lock().expect("mock body") = Some(r#"{"data":{"feature-x":"on"}}"#.to_string());
    let second = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("changed push arrives")
        .expect("stream open");
    assert_eq!(second, br#"{"feature-x":"on"}"#.to_vec());
}
