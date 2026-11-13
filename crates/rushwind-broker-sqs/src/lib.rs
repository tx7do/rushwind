//! Amazon SQS engine for the RushWind broker contract — the Query
//! API over hand-written reqwest with a hand-rolled SigV4 signer
//! (pinned against a fixed reference vector), no SDK, no AWS
//! credential chain.
//!
//! # The mapping
//!
//! - A contract **topic** is the queue name suffix of
//!   [`SqsOptions::queue_url`]; the queue url is the engine's own —
//!   one queue per engine, every subscribe shares it.
//! - Publish rides `Action=SendMessage` (the payload as
//!   `MessageBody`, the headers as numbered `MessageAttribute.N`
//!   entries — SQS's ten-attribute cap refuses the eleventh header
//!   with a typed failure rather than silently dropping one).
//! - Deliveries ride `Action=ReceiveMessage` (bounded batches,
//!   `WaitTimeSeconds: 0` so the pump stays cancellation-responsive).
//!   [`AckMode::Auto`] (the default) deletes every received message at
//!   dispatch — SQS redelivers only messages whose visibility timeout
//!   lapses, so settling on dispatch means never redelivering.
//!   [`AckMode::Manual`] hands the receipt handle to the handler:
//!   the ack hook deletes (settled, never redelivered), the nack hook
//!   zeroes the visibility timeout for [`NackAction::Requeue`] (the
//!   message re-enters the receive stream immediately) and deletes for
//!   [`NackAction::Drop`] (settled away).
//! - **Auth**: static keys
//!   ([`SqsOptions::access_key`] / [`SqsOptions::secret_key`]) signed
//!   per request — SigV4 computed by [`sigv4_post`], the same
//!   algorithm the AWS docs publish, with the reference vector pinned
//!   in the crate's tests. Credential rotation is the deployment's
//!   business (rebuild the engine).

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use hmac::Mac;
use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};
use tokio::sync::{mpsc, Mutex, Notify};

/// The engine's settings.
#[derive(Debug, Clone, Default)]
pub struct SqsOptions {
    /// The queue URL (`https://sqs.us-east-1.amazonaws.com/123456789012/my-queue`).
    /// The region and host for signing are derived from it.
    pub queue_url: String,
    /// The endpoint override for mocks/emulators (LocalStack); absent
    /// derives the API base from the queue URL's host.
    pub endpoint: Option<String>,
    /// The static access key id.
    pub access_key: String,
    /// The static secret key.
    pub secret_key: String,
    /// The optional session token (`x-amz-security-token`).
    pub session_token: Option<String>,
    /// The acknowledgment policy. Default [`AckMode::Auto`].
    pub ack_mode: AckMode,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

#[derive(Debug)]
struct Inner {
    options: SqsOptions,
    http: reqwest::Client,
    closed: std::sync::atomic::AtomicBool,
    subscriptions: Mutex<HashMap<String, Arc<SubscriptionShared>>>,
}

#[derive(Debug)]
struct SubscriptionShared {
    done: Arc<Notify>,
}

/// The Amazon SQS broker.
#[derive(Debug)]
pub struct SqsBroker {
    inner: Arc<Inner>,
}

impl SqsBroker {
    /// Builds the broker from the options. Fails when the queue url or
    /// the keys are unset.
    pub fn new(options: SqsOptions) -> Result<Self, BrokerError> {
        if options.queue_url.is_empty() {
            return Err(BrokerError::Failed("sqs: queue_url unset".to_string()));
        }
        if options.access_key.is_empty() || options.secret_key.is_empty() {
            return Err(BrokerError::Failed("sqs: keys unset".to_string()));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                http: reqwest::Client::new(),
                closed: std::sync::atomic::AtomicBool::new(false),
                subscriptions: Mutex::new(HashMap::new()),
                options,
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape —
    /// snake_case (`queue_url`, `endpoint`, `access_key`, `secret_key`,
    /// `session_token`, `ack_mode`), unknown fields ignored.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct SettingsWire {
            queue_url: String,
            endpoint: Option<String>,
            access_key: String,
            secret_key: String,
            session_token: Option<String>,
            ack_mode: Option<AckMode>,
        }
        let wire: SettingsWire = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::new(SqsOptions {
            queue_url: wire.queue_url,
            endpoint: wire.endpoint,
            access_key: wire.access_key,
            secret_key: wire.secret_key,
            session_token: wire.session_token,
            ack_mode: wire.ack_mode.unwrap_or_default(),
            ..SqsOptions::default()
        })
    }

    /// The host the request targets — the override's, or the queue
    /// URL's.
    fn host(&self) -> String {
        if let Some(endpoint) = &self.inner.options.endpoint {
            return endpoint
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .trim_end_matches('/')
                .to_string();
        }
        url_host(&self.inner.options.queue_url)
    }

    /// The API base the request posts to.
    fn api_base(&self) -> String {
        if let Some(endpoint) = &self.inner.options.endpoint {
            return endpoint.trim_end_matches('/').to_string();
        }
        format!("https://{}", url_host(&self.inner.options.queue_url))
    }

    /// Posts one Query-API action: form-encoded body, SigV4-signed.
    /// Returns the XML response body as text.
    async fn call(
        &self,
        action: &str,
        params: &[(String, String)],
        context: &str,
    ) -> Result<String, BrokerError> {
        let queue_url = &self.inner.options.queue_url;
        let mut pairs: Vec<(String, String)> = vec![("Action".to_string(), action.to_string())];
        pairs.push(("QueueUrl".to_string(), queue_url.clone()));
        pairs.extend(params.iter().cloned());
        pairs.push(("Version".to_string(), "2012-11-05".to_string()));
        let body = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .finish();

        let host = self.host();
        let path = self
            .inner
            .options
            .endpoint
            .as_ref()
            .map(|_| "/".to_string())
            .unwrap_or_else(|| queue_path(queue_url));
        let region = queue_region(queue_url).unwrap_or_else(|| "us-east-1".to_string());
        let now = unix_now();
        let amz_date = format_amz_date(now);
        let (authorization, _x_amz_date) = sigv4_post(
            &host,
            &path,
            &region,
            "sqs",
            &self.inner.options.access_key,
            &self.inner.options.secret_key,
            self.inner.options.session_token.as_deref(),
            &body,
            &amz_date,
        );

        let url = format!("{}{path}", self.api_base());
        let mut request = self
            .inner
            .http
            .post(&url)
            .timeout(self.inner.options.request_timeout)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .header("x-amz-date", amz_date)
            .header(reqwest::header::AUTHORIZATION, authorization)
            .body(body);
        if let Some(token) = &self.inner.options.session_token {
            request = request.header("x-amz-security-token", token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| BrokerError::Failed(format!("sqs {context}: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| BrokerError::Failed(format!("sqs {context}: {e}")))?;
        if !status.is_success() {
            return Err(BrokerError::Failed(format!(
                "sqs {context}: HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            )));
        }
        Ok(String::from_utf8_lossy(&bytes).to_string())
    }

    /// Settles one received message by receipt handle.
    async fn settle(&self, receipt_handle: &str, action: SettleAction) -> Result<(), BrokerError> {
        match action {
            SettleAction::Delete => self
                .call(
                    "DeleteMessage",
                    &[("ReceiptHandle".to_string(), receipt_handle.to_string())],
                    "delete",
                )
                .await
                .map(|_| ()),
            SettleAction::Requeue => {
                // A zeroed visibility timeout makes the message visible
                // again immediately — SQS's own requeue.
                self.call(
                    "ChangeMessageVisibility",
                    &[
                        ("ReceiptHandle".to_string(), receipt_handle.to_string()),
                        ("VisibilityTimeout".to_string(), "0".to_string()),
                    ],
                    "requeue",
                )
                .await
                .map(|_| ())
            }
        }
    }
}

/// The pump's settlement commands.
enum SettleAction {
    Delete,
    Requeue,
}

impl Broker for SqsBroker {
    fn name(&self) -> &'static str {
        "sqs"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        // Connectionless REST; connect is the lifecycle's live mark.
        Box::pin(async { Ok(()) })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.inner
                .closed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let subscriptions = self.inner.subscriptions.lock().await;
            for shared in subscriptions.values() {
                shared.done.notify_waiters();
            }
            Ok(())
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            if self.inner.closed.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let _ = topic; // The engine's queue is the destination.
            if message.headers.len() > 10 {
                return Err(BrokerError::Failed(
                    "sqs: more than ten message attributes".to_string(),
                ));
            }
            let mut params: Vec<(String, String)> = vec![(
                "MessageBody".to_string(),
                String::from_utf8_lossy(&message.payload).to_string(),
            )];
            for (index, (key, value)) in message.headers.iter().enumerate() {
                params.push((format!("MessageAttribute.{}.Name", index + 1), key.clone()));
                params.push((
                    format!("MessageAttribute.{}.Value.StringValue", index + 1),
                    value.clone(),
                ));
                params.push((
                    format!("MessageAttribute.{}.Value.DataType", index + 1),
                    "String".to_string(),
                ));
            }
            self.call("SendMessage", &params, "publish")
                .await
                .map(|_| ())
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            if self.inner.closed.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let done = Arc::new(Notify::new());
            let shared = Arc::new(SubscriptionShared { done: done.clone() });
            {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                subscriptions.insert(topic.to_string(), Arc::clone(&shared));
            }

            let engine = Arc::new(Self {
                inner: Arc::clone(&self.inner),
            });
            let topic_for_task = topic.to_string();
            let handler = Arc::clone(&handler);
            let task_done = done.clone();
            let ack_mode = self.inner.options.ack_mode;
            let (settle_tx, settle_rx) = mpsc::unbounded_channel::<(String, SettleAction)>();
            tokio::spawn(async move {
                let mut settle_rx = settle_rx;
                loop {
                    if engine
                        .inner
                        .closed
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        return;
                    }
                    tokio::select! {
                        _ = task_done.notified() => return,
                        settle = settle_rx.recv() => match settle {
                            Some((receipt, action)) => {
                                let _ = engine.settle(&receipt, action).await;
                            }
                            None => return,
                        },
                        received = engine.receive_once(&topic_for_task, settle_tx.clone()) => match received {
                            Ok(deliveries) => {
                                for delivery in deliveries {
                                    match ack_mode {
                                        AckMode::Auto => {
                                            let event = Event::new(
                                                delivery.topic.clone(),
                                                delivery.message.clone(),
                                            );
                                            tokio::spawn(handler(event));
                                            let _ = engine
                                                .settle(&delivery.receipt, SettleAction::Delete)
                                                .await;
                                        }
                                        AckMode::Manual => {
                                            let ack_tx = delivery.settle_tx.clone();
                                            let ack_receipt = delivery.receipt.clone();
                                            let nack_tx = delivery.settle_tx.clone();
                                            let nack_receipt = delivery.receipt.clone();
                                            let event = Event::settle_with(
                                                delivery.topic.clone(),
                                                delivery.message.clone(),
                                                move || {
                                                    let tx = ack_tx.clone();
                                                    let receipt = ack_receipt.clone();
                                                    Box::pin(async move {
                                                        tx.send((receipt, SettleAction::Delete))
                                                            .map_err(|e| BrokerError::Failed(
                                                                format!("sqs settle: {e}"),
                                                            ))
                                                    })
                                                        as BoxFuture<'static, Result<(), BrokerError>>
                                                },
                                                move |action| {
                                                    let tx = nack_tx.clone();
                                                    let receipt = nack_receipt.clone();
                                                    Box::pin(async move {
                                                        let settled = match action {
                                                            // The zeroed visibility
                                                            // timeout requeues
                                                            // immediately.
                                                            NackAction::Requeue => SettleAction::Requeue,
                                                            // Deleted poison —
                                                            // settled away.
                                                            NackAction::Drop => SettleAction::Delete,
                                                        };
                                                        tx.send((receipt, settled))
                                                            .map_err(|e| BrokerError::Failed(
                                                                format!("sqs settle: {e}"),
                                                            ))
                                                    })
                                                        as BoxFuture<'static, Result<(), BrokerError>>
                                                },
                                            );
                                            tokio::spawn(handler(event));
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                tokio::time::sleep(Duration::from_millis(250)).await;
                            }
                        },
                    }
                }
            });

            Ok(Box::new(SqsSubscriber {
                topic: topic.to_string(),
                done,
            }) as Box<dyn Subscriber>)
        })
    }

    fn request<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<Message, BrokerError>> {
        let _ = (topic, message);
        Box::pin(async {
            Err(BrokerError::Failed(
                "request not implemented by this engine".to_string(),
            ))
        })
    }
}

/// One received message en route to the handler.
struct Delivery {
    topic: String,
    message: Message,
    receipt: String,
    settle_tx: mpsc::UnboundedSender<(String, SettleAction)>,
}

impl SqsBroker {
    /// One bounded receive: ReceiveMessage with a zero wait time, the
    /// batch decoded into deliveries carrying their receipt handles.
    async fn receive_once(
        &self,
        topic: &str,
        settle_tx: mpsc::UnboundedSender<(String, SettleAction)>,
    ) -> Result<Vec<Delivery>, BrokerError> {
        let xml = self
            .call(
                "ReceiveMessage",
                &[
                    ("MaxNumberOfMessages".to_string(), "10".to_string()),
                    ("WaitTimeSeconds".to_string(), "0".to_string()),
                    ("MessageAttributeName.1".to_string(), "All".to_string()),
                ],
                &format!("receive {topic}"),
            )
            .await?;
        let mut deliveries = Vec::new();
        for block in split_xml_blocks(&xml, "Message") {
            let receipt = xml_tag(&block, "ReceiptHandle").unwrap_or_default();
            let body = xml_tag(&block, "Body").unwrap_or_default();
            let mut message = Message::from_payload(body.into_bytes());
            for (key, value) in extract_attributes(&block) {
                message.headers.insert(key, value);
            }
            deliveries.push(Delivery {
                topic: topic.to_string(),
                message,
                receipt,
                settle_tx: settle_tx.clone(),
            });
        }
        Ok(deliveries)
    }
}

/// The subscription handle: dropping it (or unsubscribing) stops the
/// receive loop.
struct SqsSubscriber {
    topic: String,
    done: Arc<Notify>,
}

impl Subscriber for SqsSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        self.done.notify_waiters();
        Box::pin(async { Ok(()) })
    }
}

// ---- wire helpers ------------------------------------------------------

/// The host portion of a URL.
pub fn url_host(url: &str) -> String {
    url.trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or_default()
        .to_string()
}

/// The queue path portion of a queue URL (`/123456789012/my-queue`).
pub fn queue_path(url: &str) -> String {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    match rest.find('/') {
        Some(at) => format!("/{}", &rest[at + 1..]),
        None => "/".to_string(),
    }
}

/// The region encoded in a queue URL's host
/// (`sqs.{region}.amazonaws.com`).
pub fn queue_region(url: &str) -> Option<String> {
    let host = url_host(url);
    let rest = host.strip_prefix("sqs.")?;
    let region = rest.split(".amazonaws.com").next()?;
    (!region.is_empty()).then_some(region.to_string())
}

/// Seconds since the epoch.
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// `YYYYMMDDTHHMMSSZ` from epoch seconds, the UTC civil conversion done
/// by hand (no chrono dependency for one date format).
pub fn format_amz_date(secs: u64) -> String {
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (hour, minute, second) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Civil-from-days (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hour, minute, second
    )
}

fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(key).expect("hmac accepts any key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex(data: &[u8]) -> String {
    data.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// SigV4 for a form-encoded POST against `/`: the canonical request
/// fixes the signed headers to `content-type;host;x-amz-date`, the
/// AWS-documented shape for the Query API. Returns the Authorization
/// header value and the `x-amz-date` to send alongside.
#[allow(clippy::too_many_arguments)] // the AWS algorithm IS these inputs
pub fn sigv4_post(
    host: &str,
    path: &str,
    region: &str,
    service: &str,
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
    body: &str,
    amz_date: &str,
) -> (String, String) {
    let date_stamp = &amz_date[..8];
    let payload_hash = hex(&sha256(body.as_bytes()));
    let mut canonical_headers = format!(
        "content-type:application/x-www-form-urlencoded\nhost:{host}\nx-amz-date:{amz_date}\n"
    );
    let mut signed_headers = "content-type;host;x-amz-date".to_string();
    if session_token.is_some() {
        canonical_headers = format!(
            "content-type:application/x-www-form-urlencoded\nhost:{host}\nx-amz-date:{amz_date}\nx-amz-security-token:{token}\n",
            token = session_token.unwrap_or_default()
        );
        signed_headers = "content-type;host;x-amz-date;x-amz-security-token".to_string();
    }
    let canonical_request =
        format!("POST\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&sha256(canonical_request.as_bytes()))
    );
    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));
    let credential = format!("{access_key}/{scope}");
    (
        format!(
            "AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders={signed_headers}, Signature={signature}"
        ),
        amz_date.to_string(),
    )
}

/// Pulls the inner text of every `<block>` element out of an XML
/// document — the SQS Query API's flat shape, parsed by hand (the
/// engine's only XML need; a parser dependency would out-weigh it).
fn split_xml_blocks(document: &str, block: &str) -> Vec<String> {
    let open = format!("<{block}>");
    let close = format!("</{block}>");
    let mut blocks = Vec::new();
    let mut rest = document;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else {
            break;
        };
        blocks.push(after[..end].to_string());
        rest = &after[end + close.len()..];
    }
    blocks
}

/// The inner text of `<tag>…</tag>` within a block.
fn xml_tag(block: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = block.find(&open)? + open.len();
    let rest = &block[start..];
    let end = rest.find(&close)?;
    Some(rest[..end].to_string())
}

/// The `MessageAttribute.N` entries of a receive block, as key-values.
fn extract_attributes(block: &str) -> Vec<(String, String)> {
    let mut attributes = Vec::new();
    let mut index = 1;
    while let Some(name) = xml_tag(block, &format!("MessageAttribute.{index}.Name")) {
        let entry_start = block
            .find(&format!("<MessageAttribute.{index}>"))
            .unwrap_or(block.len());
        let entry_end = block
            .find(&format!("</MessageAttribute.{index}>"))
            .unwrap_or(block.len());
        let entry = &block[entry_start..entry_end];
        let value = xml_tag(entry, "Value.StringValue").unwrap_or_default();
        attributes.push((name, value));
        index += 1;
    }
    attributes
}
