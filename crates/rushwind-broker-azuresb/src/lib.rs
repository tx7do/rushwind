//! Azure Service Bus engine for the RushWind broker contract — the
//! entity's HTTP REST face over hand-written reqwest with a
//! hand-rolled SAS signer (pinned against a fixed reference vector),
//! no SDK, no Azure credential chain.
//!
//! # The mapping
//!
//! Service Bus's HTTP face is the broker subset: a fixed number of
//! REST verbs on the entity URL, metadata in the `BrokerProperties`
//! header, user properties as HTTP headers, the payload as the body.
//!
//! - A contract **topic** is informational — the entity is the
//!   engine's own ([`AzureSbOptions::entity_url`]); every subscribe
//!   shares it (the SQS engine's one-queue shape).
//! - Publish rides `POST {entity}/messages` — the payload as the
//!   body, the headers as user-property headers, the message id in
//!   the `BrokerProperties` header (`{"MessageId": …}`). Property
//!   names ride HTTP, so names must be header-safe (letters, digits,
//!   dash) — the honest constraint of the REST face.
//! - Deliveries ride the head-of-queue receives:
//!   [`AckMode::Auto`] (the default) uses the destructive receive
//!   (`DELETE {entity}/messages/head`) — settle at dispatch, the
//!   message is never locked and never redelivered.
//!   [`AckMode::Manual`] uses the peek-lock receive
//!   (`POST {entity}/messages/head`) — the response's
//!   `BrokerProperties` carries the `LockToken` the settlement rides:
//!   the ack hook completes (`DELETE {entity}/messages/{id}/{token}`,
//!   settled, never redelivered), the nack hook abandons for
//!   [`NackAction::Requeue`] (`PUT {entity}/messages/{id}/{token}` —
//!   the lock releases, `DeliveryCount` increments, the message
//!   re-enters the receive stream) and completes for
//!   [`NackAction::Drop`] (settled away).
//! - **Auth**: a Shared Access Signature minted per request from
//!   static keys ([`AzureSbOptions::sas_key_name`] /
//!   [`AzureSbOptions::sas_key`]) — `sig` is the base64 HMAC-SHA256
//!   over the URL-encoded entity URI and the expiry (the algorithm
//!   the SAS docs publish, reference vector pinned in the crate's
//!   tests), or a pre-supplied token ([`AzureSbOptions::sas_token`])
//!   whose lifecycle is the deployment's (the gcpubsub token
//!   stance). Credential rotation is the deployment's business
//!   (rebuild the engine).

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use hmac::{KeyInit, Mac};
use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};
use tokio::sync::{mpsc, Mutex};

/// The `BrokerProperties` header — the metadata envelope the REST
/// face carries the message id and lock token in. HTTP header names
/// are case-insensitive; the wire name has no hyphen.
const BROKER_PROPERTIES: &str = "BrokerProperties";
/// The headers the wire owns; user properties never collide with
/// them.
const RESERVED_HEADERS: [&str; 4] = [
    "authorization",
    "content-type",
    "content-length",
    "transfer-encoding",
];

/// The engine's settings.
#[derive(Debug, Clone, Default)]
pub struct AzureSbOptions {
    /// The entity URL (`https://{namespace}.servicebus.windows.net/{queue}`).
    /// The resource URI the SAS token signs is derived from it.
    pub entity_url: String,
    /// The endpoint override for mocks/emulators; absent targets the
    /// entity URL itself.
    pub endpoint: Option<String>,
    /// The SAS key name (`sk`).
    pub sas_key_name: String,
    /// The SAS key (the signing material). Empty requires a
    /// pre-supplied [`AzureSbOptions::sas_token`].
    pub sas_key: String,
    /// A pre-supplied Shared Access Signature token; its lifecycle is
    /// the deployment's.
    pub sas_token: Option<String>,
    /// The acknowledgment policy. Default [`AckMode::Auto`].
    pub ack_mode: AckMode,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

#[derive(Debug)]
struct Inner {
    options: AzureSbOptions,
    http: reqwest::Client,
    closed: std::sync::atomic::AtomicBool,
    subscriptions: Mutex<HashMap<String, Arc<SubscriptionShared>>>,
}

#[derive(Debug)]
struct SubscriptionShared {
    /// The subscription's stop flag: the pump checks it at each loop
    /// top — a checked flag, not a one-shot wakeup, because a wakeup
    /// landing between the pump's loop iterations is lost (a
    /// `notify_waiters` with no waiter registered) and the pump would
    /// keep receiving past the unsubscribe — and the delivery face
    /// guards the in-flight delivery (a delivery arriving after the
    /// stop drops — a stopped subscription's handler sees nothing).
    stopped: std::sync::atomic::AtomicBool,
}

/// The Azure Service Bus broker.
#[derive(Debug)]
pub struct AzureSbBroker {
    inner: Arc<Inner>,
}

impl AzureSbBroker {
    /// Builds the broker from the options. Fails when the entity url
    /// is unset or no SAS credential (keys or token) is present.
    pub fn new(options: AzureSbOptions) -> Result<Self, BrokerError> {
        if options.entity_url.is_empty() {
            return Err(BrokerError::Failed("azuresb: entity_url unset".to_string()));
        }
        let keys_set = !options.sas_key_name.is_empty() && !options.sas_key.is_empty();
        if !keys_set && options.sas_token.is_none() {
            return Err(BrokerError::Failed(
                "azuresb: no SAS credential (keys or token)".to_string(),
            ));
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
    /// snake_case (`entity_url`, `endpoint`, `sas_key_name`,
    /// `sas_key`, `sas_token`, `ack_mode`), unknown fields ignored.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct SettingsWire {
            entity_url: String,
            endpoint: Option<String>,
            sas_key_name: String,
            sas_key: String,
            sas_token: Option<String>,
            ack_mode: Option<AckMode>,
        }
        let wire: SettingsWire = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::new(AzureSbOptions {
            entity_url: wire.entity_url,
            endpoint: wire.endpoint,
            sas_key_name: wire.sas_key_name,
            sas_key: wire.sas_key,
            sas_token: wire.sas_token,
            ack_mode: wire.ack_mode.unwrap_or_default(),
            ..AzureSbOptions::default()
        })
    }

    /// The base URL every request rides: the endpoint override, or
    /// the entity URL itself.
    fn base(&self) -> String {
        self.inner.options.endpoint.clone().unwrap_or_else(|| {
            self.inner
                .options
                .entity_url
                .trim_end_matches('/')
                .to_string()
        })
    }

    /// The resource URI the SAS token signs (URL-encoded at
    /// token-build time).
    fn resource_uri(&self) -> String {
        self.inner
            .options
            .entity_url
            .trim_end_matches('/')
            .to_string()
    }

    /// The Authorization header value: the pre-supplied token, or one
    /// minted from the keys (expiry = now + one hour — a minted
    /// token's life is one call's).
    fn authorization(&self) -> String {
        if let Some(token) = &self.inner.options.sas_token {
            return token.clone();
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        sas_token(
            &self.resource_uri(),
            &self.inner.options.sas_key_name,
            &self.inner.options.sas_key,
            now + 3600,
        )
    }

    /// One REST call against the entity: method, path, headers (the
    /// user properties), optional body. Returns the status, the
    /// response headers (BrokerProperties and user properties ride
    /// back in it), and the response body (the received payload).
    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        headers: &HashMap<String, String>,
        body: Option<Vec<u8>>,
        context: &str,
    ) -> Result<(u16, reqwest::header::HeaderMap, Vec<u8>), BrokerError> {
        let url = format!("{}{}", self.base(), path);
        let mut request = self
            .inner
            .http
            .request(method, &url)
            .timeout(self.inner.options.request_timeout)
            .header(reqwest::header::AUTHORIZATION, self.authorization());
        for (key, value) in headers {
            request = request.header(key.as_str(), value.as_str());
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        let response = request
            .send()
            .await
            .map_err(|e| BrokerError::Failed(format!("azuresb {context}: {e}")))?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| BrokerError::Failed(format!("azuresb {context}: {e}")))?;
        Ok((status, headers, bytes.to_vec()))
    }
}

/// One received message en route to the handler.
struct Delivery {
    topic: String,
    message: Message,
    message_id: String,
    lock_token: String,
    settle_tx: mpsc::UnboundedSender<(String, String, SettleAction)>,
}

/// The pump's settlement commands.
enum SettleAction {
    /// Complete: settled, never redelivered.
    Complete,
    /// Abandon: the lock releases, the message re-enters the stream.
    Abandon,
}

impl AzureSbBroker {
    /// One destructive receive (receive-and-delete): the message is
    /// settled the moment it is handed out.
    async fn receive_and_delete(&self, topic: &str) -> Result<Option<Message>, BrokerError> {
        let (status, headers, body) = self
            .call(
                reqwest::Method::DELETE,
                "/messages/head",
                &HashMap::new(),
                None,
                &format!("receive {topic}"),
            )
            .await?;
        match status {
            200 => decode_message(&headers, &body).map(Some),
            204 => Ok(None),
            _ => Err(BrokerError::Failed(format!(
                "azuresb receive {topic}: HTTP {status}"
            ))),
        }
    }

    /// One peek-lock receive: the message arrives locked; its
    /// `LockToken` is the settlement receipt.
    async fn receive_peek_lock(
        &self,
        topic: &str,
        settle_tx: mpsc::UnboundedSender<(String, String, SettleAction)>,
    ) -> Result<Option<Delivery>, BrokerError> {
        let (status, headers, body) = self
            .call(
                reqwest::Method::POST,
                "/messages/head",
                &HashMap::new(),
                None,
                &format!("receive {topic}"),
            )
            .await?;
        match status {
            200 | 201 => {
                let message = decode_message(&headers, &body)?;
                let broker = broker_properties(&headers)?;
                let message_id = broker
                    .get("MessageId")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let lock_token = broker
                    .get("LockToken")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                Ok(Some(Delivery {
                    topic: topic.to_string(),
                    message,
                    message_id,
                    lock_token,
                    settle_tx,
                }))
            }
            204 => Ok(None),
            _ => Err(BrokerError::Failed(format!(
                "azuresb receive {topic}: HTTP {status}"
            ))),
        }
    }

    /// Settles one received message by its id and lock token.
    async fn settle(
        &self,
        message_id: &str,
        lock_token: &str,
        action: SettleAction,
    ) -> Result<(), BrokerError> {
        let method = match action {
            SettleAction::Complete => reqwest::Method::DELETE,
            SettleAction::Abandon => reqwest::Method::PUT,
        };
        let path = format!("/messages/{message_id}/{lock_token}");
        let (status, _, _) = self
            .call(method, &path, &HashMap::new(), None, "settle")
            .await?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(BrokerError::Failed(format!(
                "azuresb settle: HTTP {status}"
            )))
        }
    }
}

impl Broker for AzureSbBroker {
    fn name(&self) -> &'static str {
        "azuresb"
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
                shared
                    .stopped
                    .store(true, std::sync::atomic::Ordering::SeqCst);
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
            let _ = topic; // The engine's entity is the destination.
            for key in message.headers.keys() {
                if !header_safe(key) {
                    return Err(BrokerError::Failed(format!(
                        "azuresb: property name not header-safe: {key:?}"
                    )));
                }
            }
            // The message id rides the BrokerProperties envelope; the
            // headers ride as user-property headers; the payload as
            // the body.
            let message_id = if message.id.is_empty() {
                format!("sb-{}", uuid_like())
            } else {
                message.id.clone()
            };
            let broker = serde_json::json!({ "MessageId": message_id });
            let mut headers = HashMap::new();
            headers.insert(BROKER_PROPERTIES.to_string(), broker.to_string());
            for (key, value) in &message.headers {
                headers.insert(key.clone(), value.clone());
            }
            let (status, _, _) = self
                .call(
                    reqwest::Method::POST,
                    "/messages",
                    &headers,
                    Some(message.payload.clone()),
                    "publish",
                )
                .await?;
            if (200..300).contains(&status) {
                Ok(())
            } else {
                Err(BrokerError::Failed(format!(
                    "azuresb publish: HTTP {status}"
                )))
            }
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
            let shared = Arc::new(SubscriptionShared {
                stopped: std::sync::atomic::AtomicBool::new(false),
            });
            {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                subscriptions.insert(topic.to_string(), Arc::clone(&shared));
            }

            let engine = Arc::new(Self {
                inner: Arc::clone(&self.inner),
            });
            let topic_for_task = topic.to_string();
            let handler = Arc::clone(&handler);
            let ack_mode = self.inner.options.ack_mode;
            let (settle_tx, mut settle_rx) =
                mpsc::unbounded_channel::<(String, String, SettleAction)>();
            let pump_shared = Arc::clone(&shared);
            tokio::spawn(async move {
                loop {
                    if engine
                        .inner
                        .closed
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        return;
                    }
                    if pump_shared
                        .stopped
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        return;
                    }
                    // Settle everything pending BEFORE the next
                    // receive: the settle and the receive never
                    // overlap. A receive racing a settle (the select
                    // shape) completes the settle first and drops the
                    // in-flight receive — and the entity has already
                    // peek-locked that message server-side, its lock
                    // token orphaned with no delivery to settle: the
                    // message stops redelivering (the lock self-expires
                    // only after the minutes-long server window).
                    while let Ok((message_id, lock_token, action)) = settle_rx.try_recv() {
                        let _ = engine.settle(&message_id, &lock_token, action).await;
                    }
                    match engine
                        .receive_dispatch(
                            &topic_for_task,
                            ack_mode,
                            &pump_shared,
                            settle_tx.clone(),
                            handler.clone(),
                        )
                        .await
                    {
                        Ok(()) => {}
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(250)).await;
                        }
                    }
                }
            });

            Ok(Box::new(AzureSbSubscriber {
                topic: topic.to_string(),
                shared: Arc::clone(&shared),
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

impl AzureSbBroker {
    /// One receive-and-dispatch step: under Auto, a destructive
    /// receive hands the message straight to the handler; under
    /// Manual, a peek-lock receive hands it over with the settlement
    /// hooks wired to the lock token.
    async fn receive_dispatch(
        &self,
        topic: &str,
        ack_mode: AckMode,
        shared: &Arc<SubscriptionShared>,
        settle_tx: mpsc::UnboundedSender<(String, String, SettleAction)>,
        handler: Handler,
    ) -> Result<(), BrokerError> {
        match ack_mode {
            AckMode::Auto => {
                if let Some(message) = self.receive_and_delete(topic).await? {
                    // The destructive receive settled the message
                    // server-side; if the subscription stopped while
                    // the receive was in flight, the delivery drops —
                    // a stopped subscription's handler sees nothing.
                    if shared.stopped.load(std::sync::atomic::Ordering::SeqCst) {
                        return Ok(());
                    }
                    let event = Event::new(topic.to_string(), message);
                    tokio::spawn(handler(event));
                }
                Ok(())
            }
            AckMode::Manual => {
                if let Some(delivery) = self.receive_peek_lock(topic, settle_tx.clone()).await? {
                    // Same guard: a peek-locked delivery arriving after
                    // the stop lands drops instead of reaching the
                    // handler (the lock self-expires server-side).
                    if shared.stopped.load(std::sync::atomic::Ordering::SeqCst) {
                        return Ok(());
                    }
                    let ack_tx = delivery.settle_tx.clone();
                    let ack_id = delivery.message_id.clone();
                    let ack_token = delivery.lock_token.clone();
                    let nack_tx = delivery.settle_tx.clone();
                    let nack_id = delivery.message_id.clone();
                    let nack_token = delivery.lock_token.clone();
                    let event = Event::settle_with(
                        delivery.topic.clone(),
                        delivery.message,
                        move || {
                            let tx = ack_tx.clone();
                            let id = ack_id.clone();
                            let token = ack_token.clone();
                            Box::pin(async move {
                                tx.send((id, token, SettleAction::Complete)).map_err(|e| {
                                    BrokerError::Failed(format!("azuresb settle: {e}"))
                                })
                            })
                                as BoxFuture<'static, Result<(), BrokerError>>
                        },
                        move |action| {
                            let tx = nack_tx.clone();
                            let id = nack_id.clone();
                            let token = nack_token.clone();
                            Box::pin(async move {
                                let settled = match action {
                                    // The lock releases, DeliveryCount
                                    // increments — the message re-enters
                                    // the receive stream.
                                    NackAction::Requeue => SettleAction::Abandon,
                                    // Completed poison — settled away.
                                    NackAction::Drop => SettleAction::Complete,
                                };
                                tx.send((id, token, settled)).map_err(|e| {
                                    BrokerError::Failed(format!("azuresb settle: {e}"))
                                })
                            })
                                as BoxFuture<'static, Result<(), BrokerError>>
                        },
                    );
                    tokio::spawn(handler(event));
                }
                Ok(())
            }
        }
    }
}

/// The subscription handle: dropping it (or unsubscribing) stops the
/// receive loop.
struct AzureSbSubscriber {
    topic: String,
    shared: Arc<SubscriptionShared>,
}

impl Subscriber for AzureSbSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        self.shared
            .stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

// ---- decode ------------------------------------------------------------

/// Decodes one received message: the payload is the body, the
/// contract headers are the user-property headers (every response
/// header except the wire-owned ones).
fn decode_message(
    headers: &reqwest::header::HeaderMap,
    body: &[u8],
) -> Result<Message, BrokerError> {
    let mut message = Message::from_payload(body.to_vec());
    let broker = broker_properties(headers)?;
    if let Some(id) = broker.get("MessageId").and_then(serde_json::Value::as_str) {
        message.id = id.to_string();
    }
    for (name, value) in headers.iter() {
        let name = name.as_str().to_ascii_lowercase();
        if RESERVED_HEADERS.contains(&name.as_str())
            || name == BROKER_PROPERTIES.to_ascii_lowercase()
        {
            continue;
        }
        message
            .headers
            .insert(name, String::from_utf8_lossy(value.as_bytes()).to_string());
    }
    Ok(message)
}

/// The `BrokerProperties` header, decoded.
fn broker_properties(
    headers: &reqwest::header::HeaderMap,
) -> Result<serde_json::Value, BrokerError> {
    let raw = headers
        .get(BROKER_PROPERTIES)
        .ok_or_else(|| BrokerError::Failed("azuresb: BrokerProperties header absent".to_string()))?
        .to_str()
        .map_err(|e| BrokerError::Failed(format!("azuresb: BrokerProperties: {e}")))?;
    serde_json::from_str(raw)
        .map_err(|e| BrokerError::Failed(format!("azuresb: BrokerProperties: {e}")))
}

// ---- SAS token ---------------------------------------------------------

/// The percent-encoding set for the SAS token's `sr` and `sig` —
/// everything outside RFC 3986's unreserved set (`A-Z a-z 0-9 - . _ ~`).
const SAS_SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Builds a Shared Access Signature token: `sig` is the base64
/// HMAC-SHA256 over `"{url-encoded resource}\n{expiry}"` keyed with
/// the SAS key, the algorithm the SAS docs publish. The reference
/// vector in the crate's tests pins the full chain.
pub fn sas_token(resource_uri: &str, key_name: &str, key: &str, expiry: u64) -> String {
    let encoded_resource = percent_encoding::utf8_percent_encode(resource_uri, SAS_SET).to_string();
    let string_to_sign = format!("{encoded_resource}\n{expiry}");
    let mut mac =
        hmac::Hmac::<sha2::Sha256>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key");
    mac.update(string_to_sign.as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    let encoded_sig = percent_encoding::utf8_percent_encode(&signature, SAS_SET).to_string();
    format!(
        "SharedAccessSignature sr={encoded_resource}&sig={encoded_sig}&se={expiry}&sk={key_name}"
    )
}

/// Whether a property name can ride an HTTP header (the REST face's
/// constraint): letters, digits, dash.
fn header_safe(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

/// A process-unique fallback message id.
fn uuid_like() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::SeqCst)
}
