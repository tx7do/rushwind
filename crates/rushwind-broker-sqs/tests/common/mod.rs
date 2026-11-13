//! An in-process mock of the SQS Query API subset the engine speaks —
//! SendMessage into an in-memory queue, ReceiveMessage (visibility
//! timeout semantics), DeleteMessage, ChangeMessageVisibility. Form
//! bodies parsed; signatures accepted (the signing chain is pinned
//! separately against the reference vector).

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
struct Received {
    body: String,
    receipt: String,
    attributes: Vec<(String, String)>,
}

#[derive(Default)]
pub struct SqsMock {
    inner: Mutex<SqsMockInner>,
}

#[derive(Default)]
struct SqsMockInner {
    queue: VecDeque<Received>,
    /// receipt handle → deleted count (the settle pins read it).
    deleted: std::collections::BTreeMap<String, i64>,
    /// receipt handle → current visibility timeout.
    visibility: std::collections::BTreeMap<String, i64>,
    next_receipt: i64,
}

impl SqsMock {
    pub fn deleted_count(&self, receipt: &str) -> i64 {
        self.inner
            .lock()
            .expect("mock")
            .deleted
            .get(receipt)
            .copied()
            .unwrap_or(0)
    }

    pub fn visibility(&self, receipt: &str) -> Option<i64> {
        self.inner
            .lock()
            .expect("mock")
            .visibility
            .get(receipt)
            .copied()
    }

    /// Plants a message: body plus one attribute.
    pub fn enqueue(&self, body: &str, attribute_key: &str, attribute_value: &str) {
        let mut inner = self.inner.lock().expect("mock");
        inner.queue.push_back(Received {
            body: body.to_string(),
            receipt: String::new(), // minted at receive
            attributes: vec![(attribute_key.to_string(), attribute_value.to_string())],
        });
    }

    fn handle(&self, body_text: &str) -> (u16, String) {
        let pairs: Vec<(String, String)> = form_urlencoded::parse(body_text.as_bytes())
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let get = |name: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        };
        let action = get("Action");
        let mut inner = self.inner.lock().expect("mock");
        match action.as_str() {
            "SendMessage" => {
                inner.queue.push_back(Received {
                    body: get("MessageBody"),
                    receipt: String::new(),
                    attributes: {
                        let mut attributes = Vec::new();
                        let mut index = 1;
                        while let Some(name) =
                            pairs.iter().find(|(k, _)| {
                                k == &format!("MessageAttribute.{index}.Name")
                            })
                            .map(|(_, v)| v.clone())
                        {
                            let value = pairs
                                .iter()
                                .find(|(k, _)| {
                                    k == &format!(
                                        "MessageAttribute.{index}.Value.StringValue"
                                    )
                                })
                                .map(|(_, v)| v.clone())
                                .unwrap_or_default();
                            attributes.push((name, value));
                            index += 1;
                        }
                        attributes
                    },
                });
                (200, "<SendMessageResponse><SendMessageResult/></SendMessageResponse>".into())
            }
            "ReceiveMessage" => {
                let max = get("MaxNumberOfMessages")
                    .parse::<usize>()
                    .unwrap_or(10)
                    .min(10);
                let mut received = Vec::new();
                for _ in 0..max {
                    let Some(mut message) = inner.queue.pop_front() else {
                        break;
                    };
                    inner.next_receipt += 1;
                    let receipt = format!("receipt-{}", inner.next_receipt);
                    message.receipt = receipt.clone();
                    inner.visibility.insert(receipt.clone(), 30);
                    let mut xml = format!(
                        "<Message><ReceiptHandle>{receipt}</ReceiptHandle><Body>{}</Body>",
                        message.body
                    );
                    for (index, (name, value)) in message.attributes.iter().enumerate() {
                        xml.push_str(&format!(
                            "<MessageAttribute.{0}><Name>{name}</Name><Value><StringValue>{value}</StringValue></Value></MessageAttribute.{0}>",
                            index + 1
                        ));
                    }
                    xml.push_str("</Message>");
                    received.push(xml);
                }
                let body_xml = received.join("");
                (
                    200,
                    format!(
                        "<ReceiveMessageResponse><ReceiveMessageResult>{body_xml}</ReceiveMessageResult></ReceiveMessageResponse>"
                    ),
                )
            }
            "DeleteMessage" => {
                let receipt = get("ReceiptHandle");
                inner.visibility.remove(&receipt);
                *inner.deleted.entry(receipt).or_insert(0) += 1;
                (200, "<DeleteMessageResponse/>".into())
            }
            "ChangeMessageVisibility" => {
                let receipt = get("ReceiptHandle");
                let timeout = get("VisibilityTimeout").parse::<i64>().unwrap_or(30);
                inner.visibility.insert(receipt, timeout);
                (200, "<ChangeMessageVisibilityResponse/>".into())
            }
            _ => (400, format!("<ErrorResponse><Error><Message>unknown action {action}</Message></Error></ErrorResponse>")),
        }
    }
}

fn respond(status: u16, body: String) -> String {
    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        status,
        if status < 400 { "OK" } else { "Error" },
        body.len()
    )
}

/// Boots the mock; returns the endpoint (to post to — the queue url
/// rides the form) and the shared handle.
pub async fn spawn_sqs() -> (String, Arc<SqsMock>) {
    let mock = Arc::new(SqsMock::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind works");
    let address = listener.local_addr().expect("local addr");
    let server = mock.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let server = server.clone();
            tokio::spawn(async move {
                serve(server, stream).await;
            });
        }
    });
    (format!("http://{address}"), mock)
}

async fn serve(mock: Arc<SqsMock>, mut stream: tokio::net::TcpStream) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    let header_end = loop {
        if let Some(position) = find_header_end(&buffer) {
            break position;
        }
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    };
    let headers_text = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let content_length = headers_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    while buffer.len() < header_end + 4 + content_length {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    }
    let body_text = String::from_utf8_lossy(&buffer[header_end + 4..]).to_string();
    let (status, body) = mock.handle(&body_text);
    let _ = stream.write_all(respond(status, body).as_bytes()).await;
    let _ = stream.flush().await;
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}
