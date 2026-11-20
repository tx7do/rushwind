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
//! - Publish rides `Action=SendMessage`: the payload rides `MessageBody`
//!   in a base64 envelope (SQS's body is a text field, the contract's
//!   payload is bytes — the same stance gcpubsub and nsq take), and the
//!   headers ride numbered `MessageAttribute.N` entries — SQS's
//!   ten-attribute cap refuses the eleventh header with a typed failure
//!   rather than silently dropping one. On receive, a body that decodes
//!   as the envelope yields its bytes; one that does not — a foreign
//!   producer's plain text — rides as its own bytes.
//! - Deliveries ride `Action=ReceiveMessage` (bounded batches,
//!   `WaitTimeSeconds: 0` so the pump stays cancellation-responsive,
//!   an empty poll spacing itself with a short backoff). The response's
//!   body MD5 is verified — a mismatch fails the receive and the
//!   messages ride their visibility timeout rather than delivering
//!   corrupt text once. [`AckMode::Auto`] (the default) deletes every
//!   received message at dispatch — SQS redelivers only messages whose
//!   visibility timeout lapses, so settling on dispatch means never
//!   redelivering. [`AckMode::Manual`] hands the receipt handle to the
//!   handler: the ack hook deletes (settled, never redelivered), the
//!   nack hook zeroes the visibility timeout for [`NackAction::Requeue`]
//!   (the message re-enters the receive stream immediately) and deletes
//!   for [`NackAction::Drop`] (settled away).
//! - The receive pump is a sequential loop: the checked `stopped` flag
//!   is read at every loop top and before every dispatch, every pending
//!   settle drains before the next receive, and one receive runs to
//!   completion — a stop (unsubscribe, drop of the handle, or
//!   disconnect) lands at the next loop top regardless of timing, and
//!   a delivery already received when the stop lands is dropped without
//!   dispatch (its visibility timeout returns it to the queue
//!   server-side). A duplicate subscribe retires the previous pump —
//!   the newest handler owns the filter, the rabbitmq/redis shape.
//!   Settles enqueued by handlers whose pump has already exited fail
//!   with a clear error: the message re-enters the receive stream after
//!   its visibility timeout.
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use hmac::{KeyInit, Mac};
use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};
use tokio::sync::{mpsc, Mutex};

/// The receive loop's empty-poll spacing: with `WaitTimeSeconds: 0`
/// (kept for cancellation responsiveness) an empty queue would
/// otherwise hot-poll the endpoint.
const EMPTY_POLL_BACKOFF: Duration = Duration::from_millis(200);

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
    closed: AtomicBool,
    subscriptions: Mutex<HashMap<String, Arc<SubscriptionShared>>>,
}

/// One subscription's stop face. A checked flag — read at the pump's
/// loop top and before every dispatch — not a wakeup: a notify-style
/// signal landing between the pump's await points would be lost, and
/// the pump would keep receiving (and, under Auto, deleting) messages
/// for a subscription that was unsubscribed.
#[derive(Debug)]
struct SubscriptionShared {
    stopped: AtomicBool,
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
                closed: AtomicBool::new(false),
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
            self.inner.closed.store(true, Ordering::SeqCst);
            let subscriptions = self.inner.subscriptions.lock().await;
            for shared in subscriptions.values() {
                shared.stopped.store(true, Ordering::SeqCst);
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
            if self.inner.closed.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let _ = topic; // The engine's queue is the destination.
            if message.headers.len() > 10 {
                return Err(BrokerError::Failed(
                    "sqs: more than ten message attributes".to_string(),
                ));
            }
            // SQS's MessageBody is a text field and the contract's
            // payload is bytes: the payload rides the same base64
            // envelope the gcpubsub and nsq engines use. On receive, a
            // body that decodes as the envelope yields its bytes; one
            // that does not — a foreign producer's plain text, or the
            // mock's planted bodies — rides as its own bytes.
            let mut params: Vec<(String, String)> =
                vec![("MessageBody".to_string(), BASE64.encode(&message.payload))];
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
            if self.inner.closed.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let shared = Arc::new(SubscriptionShared {
                stopped: AtomicBool::new(false),
            });
            {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                // A duplicate subscribe retires the old pump — the
                // rabbitmq/redis engines' same-topic shape: the newest
                // handler owns the filter, and the previous pump exits at
                // its next loop top instead of leaking past the
                // overwritten registry entry.
                if let Some(previous) = subscriptions.get(topic) {
                    previous.stopped.store(true, Ordering::SeqCst);
                }
                subscriptions.insert(topic.to_string(), Arc::clone(&shared));
            }

            let engine = Arc::new(Self {
                inner: Arc::clone(&self.inner),
            });
            let topic_for_task = topic.to_string();
            let handler = Arc::clone(&handler);
            let ack_mode = self.inner.options.ack_mode;
            let shared_for_handle = Arc::clone(&shared);
            let (settle_tx, settle_rx) = mpsc::unbounded_channel::<(String, SettleAction)>();
            tokio::spawn(async move {
                let mut settle_rx = settle_rx;
                loop {
                    // The checked stop, read at every loop top: there is
                    // no wakeup to lose — the flag is the signal, so a
                    // stop landing between the pump's await points takes
                    // effect here regardless of timing.
                    if engine.inner.closed.load(Ordering::SeqCst)
                        || shared.stopped.load(Ordering::SeqCst)
                    {
                        return;
                    }
                    // Every pending settle drains before the next receive:
                    // settles and receives never overlap and no settle is
                    // abandoned mid-request. A settle enqueued while the
                    // receive is in flight waits for the next loop top —
                    // the receive is never cancelled under it.
                    while let Ok((receipt, action)) = settle_rx.try_recv() {
                        let _ = engine.settle(&receipt, action).await;
                    }
                    match engine
                        .receive_once(&topic_for_task, settle_tx.clone())
                        .await
                    {
                        Ok(deliveries) if deliveries.is_empty() => {
                            tokio::time::sleep(EMPTY_POLL_BACKOFF).await;
                        }
                        Ok(deliveries) => {
                            for delivery in deliveries {
                                // The delivery-face stop: a delivery already
                                // received when the stop lands is dropped
                                // without dispatch — the destructive Auto
                                // delete must not run for a stopped
                                // subscription, and under Manual the event
                                // would settle into a channel no pump drains
                                // anymore. The message's visibility timeout
                                // returns it to the queue server-side.
                                if shared.stopped.load(Ordering::SeqCst)
                                    || engine.inner.closed.load(Ordering::SeqCst)
                                {
                                    break;
                                }
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
                                                        .map_err(|e| {
                                                            BrokerError::Failed(format!(
                                                                "sqs settle: {e}"
                                                            ))
                                                        })
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
                                                        NackAction::Requeue => {
                                                            SettleAction::Requeue
                                                        }
                                                        // Deleted poison —
                                                        // settled away.
                                                        NackAction::Drop => SettleAction::Delete,
                                                    };
                                                    tx.send((receipt, settled)).map_err(|e| {
                                                        BrokerError::Failed(format!(
                                                            "sqs settle: {e}"
                                                        ))
                                                    })
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
                    }
                }
            });

            Ok(Box::new(SqsSubscriber {
                topic: topic.to_string(),
                shared: shared_for_handle,
                inner: Arc::clone(&self.inner),
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
            // A message without a receipt handle can never be settled:
            // skipping it leaves the message to its visibility timeout
            // instead of dispatching an event whose every settlement
            // would fail invisibly.
            let Some(receipt) = xml_tag(&block, "ReceiptHandle") else {
                continue;
            };
            let body_raw = xml_tag(&block, "Body").unwrap_or_default();
            let body = xml_decode(&body_raw);
            // The response's integrity check: the MD5 of the body as the
            // sender wrote it (the digest covers the unescaped body, so
            // it verifies after the entity decode). A mismatch fails the
            // whole receive — the messages ride their visibility timeout
            // back into the queue instead of delivering a corrupt
            // payload once.
            if let Some(expected) = xml_tag(&block, "MD5OfBody") {
                if md5_hex(body.as_bytes()) != expected {
                    return Err(BrokerError::Failed(
                        "sqs receive: body MD5 mismatch".to_string(),
                    ));
                }
            }
            let payload = match BASE64.decode(body.as_bytes()) {
                Ok(bytes) => bytes,
                // Not the engine's envelope: a foreign producer's plain
                // text rides as its own bytes.
                Err(_) => body.into_bytes(),
            };
            let mut message = Message::from_payload(payload);
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

/// The subscription handle. `unsubscribe` stops the receive loop
/// authoritatively; dropping it stops the same loop best-effort — the
/// registry's lock may be contended at drop time, in which case only
/// the explicit call is guaranteed.
struct SqsSubscriber {
    topic: String,
    shared: Arc<SubscriptionShared>,
    inner: Arc<Inner>,
}

impl SqsSubscriber {
    /// Stops the pump this handle owns, if it still owns the registry
    /// entry: a duplicate subscribe replaces the entry, and a stale
    /// handle's teardown must then touch nothing — stopping the
    /// replacement's pump would orphan its deliveries.
    async fn stop_owned(&self) {
        let mut subscriptions = self.inner.subscriptions.lock().await;
        let owned = subscriptions
            .get(&self.topic)
            .map(|current| Arc::ptr_eq(current, &self.shared))
            .unwrap_or(false);
        if owned {
            self.shared.stopped.store(true, Ordering::SeqCst);
            subscriptions.remove(&self.topic);
        }
    }
}

impl Subscriber for SqsSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.stop_owned().await;
            Ok(())
        })
    }
}

impl Drop for SqsSubscriber {
    fn drop(&mut self) {
        // The best-effort half of the stop contract: try_lock fails
        // under an active holder, and only the explicit unsubscribe
        // guarantees the stop.
        if let Ok(mut subscriptions) = self.inner.subscriptions.try_lock() {
            let owned = subscriptions
                .get(&self.topic)
                .map(|current| Arc::ptr_eq(current, &self.shared))
                .unwrap_or(false);
            if owned {
                self.shared.stopped.store(true, Ordering::SeqCst);
                subscriptions.remove(&self.topic);
            }
        }
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

/// Decodes the XML entities the Query API escapes text values with —
/// the five named entities plus decimal and hex numeric references. A
/// malformed reference (no closing `;`, unknown name) passes its `&`
/// through literally.
fn xml_decode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        let tail = &rest[at..];
        let Some(end) = tail.find(';') else {
            out.push('&');
            rest = &rest[at + 1..];
            continue;
        };
        let entity = &tail[1..end];
        let decoded = match entity {
            "amp" => Some('&'.to_string()),
            "lt" => Some('<'.to_string()),
            "gt" => Some('>'.to_string()),
            "quot" => Some('"'.to_string()),
            "apos" => Some('\''.to_string()),
            _ => {
                if let Some(digits) = entity.strip_prefix('#') {
                    let code = match digits
                        .strip_prefix('x')
                        .or_else(|| digits.strip_prefix('X'))
                    {
                        Some(hex) => u32::from_str_radix(hex, 16).ok(),
                        None => digits.parse::<u32>().ok(),
                    };
                    code.and_then(char::from_u32).map(|c| c.to_string())
                } else {
                    None
                }
            }
        };
        match decoded {
            Some(text) => {
                out.push_str(&text);
                rest = &tail[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[at + 1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// Hex MD5 — the response's body-integrity digest.
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

/// The `MessageAttribute.N` entries of a receive block, as key-values.
fn extract_attributes(block: &str) -> Vec<(String, String)> {
    // The ReceiveMessage response nests each attribute as a repeated
    // `<MessageAttribute>` element (no numbered suffix — that shape
    // belongs to the *request* parameters), with the value inside a
    // nested `<Value>`: `<Name>hop</Name><Value><StringValue>one`.
    // Binary attributes (`BinaryValue`, base64) decode into the same
    // String header as lossy UTF-8 — the contract's header is a
    // String, so the bytes render with U+FFFD replacements.
    let mut attributes = Vec::new();
    let mut rest = block;
    while let Some(start) = rest.find("<MessageAttribute>") {
        let after = &rest[start + "<MessageAttribute>".len()..];
        let Some(end) = after.find("</MessageAttribute>") else {
            break;
        };
        let entry = &after[..end];
        rest = &after[end + "</MessageAttribute>".len()..];
        let Some(name) = xml_tag(entry, "Name") else {
            continue;
        };
        let value = match xml_tag(entry, "StringValue") {
            Some(text) => xml_decode(&text),
            None => match xml_tag(entry, "BinaryValue") {
                Some(binary) => match BASE64.decode(binary.as_bytes()) {
                    Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
                    Err(_) => binary,
                },
                None => String::new(),
            },
        };
        attributes.push((xml_decode(&name), value));
    }
    attributes
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The entity decode: named entities, decimal and hex numeric
    /// references, and malformed references passing their `&` through.
    #[test]
    fn xml_decode_expands_named_and_numeric_entities() {
        assert_eq!(xml_decode("a&amp;b"), "a&b");
        assert_eq!(xml_decode("&lt;tag&gt;"), "<tag>");
        assert_eq!(xml_decode("&quot;q&quot; &apos;a&apos;"), "\"q\" 'a'");
        assert_eq!(xml_decode("&#65;&#x42;"), "AB");
        assert_eq!(xml_decode("no-semi&amp"), "no-semi&amp");
        assert_eq!(xml_decode("unknown&bogus;entity"), "unknown&bogus;entity");
    }

    /// The payload's decode stance: the envelope decodes to its bytes;
    /// a non-envelope body (a foreign producer's plain text — note the
    /// `-` outside the base64 alphabet) rides as its own bytes.
    #[test]
    fn body_decode_prefers_the_envelope_and_falls_back_to_raw() {
        let encoded = BASE64.encode(b"plain");
        assert_eq!(BASE64.decode(encoded.as_bytes()).unwrap(), b"plain");
        assert!(BASE64.decode(b"delivered-text").is_err());
    }

    /// Binary attributes decode as lossy UTF-8 into the String header.
    #[test]
    fn binary_attributes_decode_as_lossy_utf8() {
        let block = format!(
            "<MessageAttribute><Name>blob</Name><Value><BinaryValue>{}</BinaryValue></Value></MessageAttribute>",
            BASE64.encode([0u8, 255])
        );
        let attributes = extract_attributes(&block);
        assert_eq!(attributes.len(), 1);
        assert_eq!(attributes[0].0, "blob");
        assert_eq!(attributes[0].1, "\u{0}\u{FFFD}");
    }
}
