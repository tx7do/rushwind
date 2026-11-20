//! An in-process mock of the SQS Query API subset the engine speaks —
//! SendMessage into an in-memory queue, ReceiveMessage (visibility
//! timeout semantics with a real in-flight table), DeleteMessage,
//! ChangeMessageVisibility. Form bodies parsed; signatures accepted
//! (the signing chain is pinned separately against the reference
//! vector). Responses render XML-escaped text the way the real service
//! does, and carry `MD5OfBody`, so the engine's decode path (entities,
//! integrity check, base64 envelope) runs end to end.

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
struct Received {
    body: String,
    receipt: String,
    attributes: Vec<(String, String)>,
}

/// XML text-node escaping, the way the real responses render text.
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Hex MD5 — the response's body-integrity digest, over the unescaped
/// body string (the same input the engine verifies against).
fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(data);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Default)]
pub struct SqsMock {
    inner: Mutex<SqsMockInner>,
}

#[derive(Default)]
struct SqsMockInner {
    /// Visible messages, in queue order.
    queue: VecDeque<Received>,
    /// receipt handle → the message sitting in flight (received,
    /// visibility running). Delete removes it; a zeroed visibility
    /// returns it to the queue — SQS's requeue.
    in_flight: BTreeMap<String, Received>,
    /// receipt handle → deleted count (the settle pins read it).
    deleted: BTreeMap<String, i64>,
    /// receipt handle → the visibility timeout last set (the settle
    /// pins read it).
    visibility: BTreeMap<String, i64>,
    next_receipt: i64,
    /// The receipt minted by the latest receive — survives the
    /// message leaving the in-flight table (a requeue moves it back to
    /// the queue), so the settle pins can name it.
    last_receipt: Option<String>,
    /// ReceiveMessage call count — the stop pins read it.
    receives: u64,
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

    /// Total DeleteMessage calls across all receipts — the settle
    /// assertion for pins that do not know the minted receipt.
    pub fn deleted_total(&self) -> i64 {
        self.inner.lock().expect("mock").deleted.values().sum()
    }

    pub fn visibility(&self, receipt: &str) -> Option<i64> {
        self.inner
            .lock()
            .expect("mock")
            .visibility
            .get(receipt)
            .copied()
    }

    /// The receipt minted by the latest receive (messages arrive one
    /// batch at a time in these pins, so "last" is "the pin's").
    pub fn last_receipt(&self) -> Option<String> {
        self.inner.lock().expect("mock").last_receipt.clone()
    }

    /// The ReceiveMessage call count, for the stop pins: a stopped
    /// pump must stop receiving.
    pub fn receive_count(&self) -> u64 {
        self.inner.lock().expect("mock").receives
    }

    /// Plants a message: body plus one attribute. The body rides as
    /// the queue's raw text — the mock escapes it on render, the way
    /// the real service escapes, so the engine's decode path sees the
    /// real shape.
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
                inner.receives += 1;
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
                    inner.last_receipt = Some(receipt.clone());
                    inner.visibility.insert(receipt.clone(), 30);
                    inner.in_flight.insert(receipt.clone(), message.clone());
                    let mut xml = format!(
                        "<Message><ReceiptHandle>{receipt}</ReceiptHandle><MD5OfBody>{}</MD5OfBody><Body>{}</Body>",
                        md5_hex(message.body.as_bytes()),
                        xml_escape(&message.body)
                    );
                    // The real response shape: repeated unnumbered
                    // <MessageAttribute> elements with the value nested
                    // inside <Value>, text escaped the way the service
                    // escapes it.
                    for (name, value) in message.attributes.iter() {
                        xml.push_str(&format!(
                            "<MessageAttribute><Name>{}</Name><Value><StringValue>{}</StringValue></Value></MessageAttribute>",
                            xml_escape(name),
                            xml_escape(value)
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
                inner.in_flight.remove(&receipt);
                *inner.deleted.entry(receipt).or_insert(0) += 1;
                (200, "<DeleteMessageResponse/>".into())
            }
            "ChangeMessageVisibility" => {
                let receipt = get("ReceiptHandle");
                let timeout = get("VisibilityTimeout").parse::<i64>().unwrap_or(30);
                inner.visibility.insert(receipt.clone(), timeout);
                // A zeroed timeout is SQS's requeue: the message leaves
                // the in-flight table and re-enters the visible queue,
                // so the next ReceiveMessage really redelivers it.
                if timeout == 0 {
                    if let Some(message) = inner.in_flight.remove(&receipt) {
                        inner.queue.push_back(message);
                    }
                }
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
