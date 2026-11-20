//! Google Cloud Pub/Sub engine for the RushWind broker contract —
//! the v1 REST API over hand-written reqwest, no SDK (the house's
//! S3/Apollo/nacos precedent).
//!
//! # The mapping
//!
//! - A contract **topic** is a Pub/Sub topic name; the engine owns one
//!   subscription ([`GcpPubSubOptions::subscription_id`]) that every
//!   subscribe call shares. Publishes ride `topics/{topic}:publish`
//!   (payload base64, headers as attributes); deliveries ride
//!   `subscriptions/{subscription}:pull` (a bounded long-poll pull —
//!   `maxMessages` caps the batch, the pull returns promptly so the
//!   pump stays cancellation-responsive).
//! - [`AckMode::Auto`] (the default) acknowledges every pulled message
//!   the moment it is dispatched — the implicit-acknowledgment shape
//!   (Pub/Sub redelivers only unacked messages past their ack
//!   deadline, so settling on dispatch means never redelivering).
//!   [`AckMode::Manual`] hands the delivery's native settlement to the
//!   handler: the ack hook sends `acknowledge` (the message is
//!   settled), the nack hook sends `modifyAckDeadline` with zero —
//!   Pub/Sub has no NACK frame, and a zeroed deadline makes the
//!   message eligible for immediate redelivery, the honest Requeue.
//!   A [`NackAction::Drop`] under Manual acknowledges the poison
//!   message away (settled, never redelivered).
//! - The pull pump is a sequential loop: the checked `stopped` flag is
//!   read at every loop top and before every dispatch, every pending
//!   settle drains before the next pull, and one pull runs to
//!   completion — a stop (unsubscribe, drop of the handle, or
//!   disconnect) lands at the next loop top regardless of timing, and
//!   a delivery already pulled when the stop lands is dropped without
//!   dispatch (its ack deadline redelivers it server-side). A
//!   duplicate subscribe retires the previous pump — the newest
//!   handler owns the filter, the rabbitmq/redis shape. Settles
//!   enqueued by handlers whose pump has already exited fail with a
//!   clear error: the message redelivers after its ack deadline.
//! - **Auth** is a bearer token the deployment supplies
//!   ([`GcpPubSubOptions::token`]) — obtaining and refreshing the
//!   service-account token is the deployment's business (mirroring
//!   the OIDC engine's stance: the engine consumes credentials, the
//!   deployment owns their lifecycle).
//! - Base64 payloads decode/encode at the engine boundary; a body
//!   that does not decode as the envelope rides as its own bytes (a
//!   foreign producer's raw data — failing the batch instead would
//!   starve the subscription on one undecodable message). Header maps
//!   pass through as Pub/Sub attributes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};
use serde_json::json;
use tokio::sync::{mpsc, Mutex};

/// The default REST endpoint; a mock or emulator overrides it.
const DEFAULT_ENDPOINT: &str = "https://pubsub.googleapis.com";

/// The per-pull message cap — bounded so the pump stays
/// cancellation-responsive and a burst cannot stall shutdown.
const PULL_BATCH: i64 = 32;

/// The pull loop's empty-poll spacing: with `returnImmediately: true`
/// an empty subscription would otherwise hot-poll the endpoint.
const EMPTY_PULL_BACKOFF: Duration = Duration::from_millis(200);

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct GcpPubSubOptions {
    /// The REST endpoint. Default `https://pubsub.googleapis.com`;
    /// the emulator (or a mock) overrides it.
    pub endpoint: Option<String>,
    /// The GCP project id.
    pub project_id: String,
    /// The topic publishes ride.
    pub topic_id: String,
    /// The subscription pulls ride — one subscription per engine,
    /// shared across contract subscribes.
    pub subscription_id: String,
    /// The bearer token; obtain/refresh it in the deployment.
    pub token: Option<String>,
    /// The acknowledgment policy. Default [`AckMode::Auto`].
    pub ack_mode: AckMode,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for GcpPubSubOptions {
    fn default() -> Self {
        Self {
            endpoint: None,
            project_id: String::new(),
            topic_id: String::new(),
            subscription_id: String::new(),
            token: None,
            ack_mode: AckMode::Auto,
            request_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug)]
struct Inner {
    options: GcpPubSubOptions,
    http: reqwest::Client,
    closed: AtomicBool,
    subscriptions: Mutex<HashMap<String, Arc<SubscriptionShared>>>,
}

/// One subscription's stop face. A checked flag — read at the pump's
/// loop top and before every dispatch — not a wakeup: a notify-style
/// signal landing between the pump's await points would be lost, and
/// the pump would keep pulling (and, under Auto, acknowledging)
/// messages for a subscription that was unsubscribed.
#[derive(Debug)]
struct SubscriptionShared {
    stopped: AtomicBool,
}

/// The Google Cloud Pub/Sub broker.
#[derive(Debug)]
pub struct GcpPubSubBroker {
    inner: Arc<Inner>,
}

impl GcpPubSubBroker {
    /// Builds the broker from the options. Fails when the project,
    /// topic or subscription is unset — loads would have to guess.
    pub fn new(options: GcpPubSubOptions) -> Result<Self, BrokerError> {
        if options.project_id.is_empty() {
            return Err(BrokerError::Failed(
                "gcpubsub: project_id unset".to_string(),
            ));
        }
        if options.topic_id.is_empty() {
            return Err(BrokerError::Failed("gcpubsub: topic_id unset".to_string()));
        }
        if options.subscription_id.is_empty() {
            return Err(BrokerError::Failed(
                "gcpubsub: subscription_id unset".to_string(),
            ));
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
    /// snake_case (`endpoint`, `project_id`, `topic_id`,
    /// `subscription_id`, `token`, `ack_mode`), unknown fields ignored.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct SettingsWire {
            endpoint: Option<String>,
            project_id: String,
            topic_id: String,
            subscription_id: String,
            token: Option<String>,
            ack_mode: Option<AckMode>,
        }
        let wire: SettingsWire = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::new(GcpPubSubOptions {
            endpoint: wire.endpoint,
            project_id: wire.project_id,
            topic_id: wire.topic_id,
            subscription_id: wire.subscription_id,
            token: wire.token,
            ack_mode: wire.ack_mode.unwrap_or_default(),
            ..GcpPubSubOptions::default()
        })
    }

    /// One authenticated REST call to the v1 API; returns the JSON
    /// body of a 2xx.
    async fn call(
        &self,
        path: &str,
        body: serde_json::Value,
        context: &str,
    ) -> Result<serde_json::Value, BrokerError> {
        let endpoint = self
            .inner
            .options
            .endpoint
            .as_deref()
            .unwrap_or(DEFAULT_ENDPOINT)
            .trim_end_matches('/');
        let url = format!("{endpoint}/v1/{path}");
        let mut request = self
            .inner
            .http
            .post(&url)
            .timeout(self.inner.options.request_timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(
                serde_json::to_vec(&body)
                    .map_err(|e| BrokerError::Failed(format!("gcpubsub {context} encode: {e}")))?,
            );
        if let Some(token) = &self.inner.options.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| BrokerError::Failed(format!("gcpubsub {context}: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| BrokerError::Failed(format!("gcpubsub {context}: {e}")))?;
        if !status.is_success() {
            return Err(BrokerError::Failed(format!(
                "gcpubsub {context}: HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            )));
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| BrokerError::Failed(format!("gcpubsub {context} decode: {e}")))
    }

    /// Settles one pulled delivery: acknowledge, or zero its deadline
    /// so Pub/Sub's redelivery takes the message back.
    async fn settle(&self, ack_id: &str, action: SettleAction) -> Result<(), BrokerError> {
        let subscription = format!(
            "projects/{}/subscriptions/{}",
            self.inner.options.project_id, self.inner.options.subscription_id
        );
        let (path, body) = match action {
            SettleAction::Ack => (
                format!("{subscription}:acknowledge"),
                json!({"ackIds": [ack_id]}),
            ),
            SettleAction::Requeue => (
                format!("{subscription}:modifyAckDeadline"),
                json!({"ackIds": [ack_id], "ackDeadlineSeconds": 0}),
            ),
        };
        self.call(&path, body, &format!("settle {ack_id}"))
            .await
            .map(|_| ())
    }
}

/// The pump's settlement commands.
enum SettleAction {
    Ack,
    Requeue,
}

impl Broker for GcpPubSubBroker {
    fn name(&self) -> &'static str {
        "gcpubsub"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        // Connectionless REST: every call carries its own auth; connect
        // is the lifecycle's live mark and a no-op here.
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
            let data = base64::engine::general_purpose::STANDARD.encode(&message.payload);
            let attributes: serde_json::Map<String, serde_json::Value> = message
                .headers
                .iter()
                .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
                .collect();
            self.call(
                &format!(
                    "projects/{}/topics/{}:publish",
                    self.inner.options.project_id,
                    if topic.is_empty() {
                        self.inner.options.topic_id.as_str()
                    } else {
                        topic
                    }
                ),
                json!({"messages": [{"data": data, "attributes": attributes}]}),
                "publish",
            )
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

            let engine = Arc::new(self.clone_inner());
            let topic_for_task = topic.to_string();
            let handler = Arc::clone(&handler);
            let ack_mode = self.inner.options.ack_mode;
            // Manual settlement rides this channel back to the pump —
            // the settle calls need the engine's &self, which only the
            // pump holds.
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
                    // Every pending settle drains before the next pull:
                    // settles and pulls never overlap and no settle is
                    // abandoned mid-request. A settle enqueued while the
                    // pull is in flight waits for the next loop top —
                    // the pull is never cancelled under it.
                    while let Ok((ack_id, action)) = settle_rx.try_recv() {
                        let _ = engine.settle(&ack_id, action).await;
                    }
                    match engine.pull_once(&topic_for_task, settle_tx.clone()).await {
                        Ok(deliveries) if deliveries.is_empty() => {
                            tokio::time::sleep(EMPTY_PULL_BACKOFF).await;
                        }
                        Ok(deliveries) => {
                            for delivery in deliveries {
                                // The delivery-face stop: a delivery already
                                // pulled when the stop lands is dropped
                                // without dispatch — the destructive Auto
                                // acknowledge must not run for a stopped
                                // subscription, and under Manual the event
                                // would settle into a channel no pump drains
                                // anymore. The message's ack deadline
                                // redelivers it server-side.
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
                                            .settle(&delivery.ack_id, SettleAction::Ack)
                                            .await;
                                    }
                                    AckMode::Manual => {
                                        let ack_tx = delivery.settle_tx.clone();
                                        let ack_id = delivery.ack_id.clone();
                                        let nack_tx = delivery.settle_tx.clone();
                                        let nack_id = delivery.ack_id.clone();
                                        let event = Event::settle_with(
                                            delivery.topic.clone(),
                                            delivery.message.clone(),
                                            move || {
                                                let tx = ack_tx.clone();
                                                let id = ack_id.clone();
                                                Box::pin(async move {
                                                    tx.send((id, SettleAction::Ack)).map_err(|e| {
                                                        BrokerError::Failed(format!(
                                                            "gcpubsub settle: {e}"
                                                        ))
                                                    })
                                                })
                                                    as BoxFuture<'static, Result<(), BrokerError>>
                                            },
                                            move |action| {
                                                let tx = nack_tx.clone();
                                                let id = nack_id.clone();
                                                Box::pin(async move {
                                                    let settled = match action {
                                                        // No NACK frame in
                                                        // Pub/Sub: a zeroed
                                                        // ack deadline makes
                                                        // the message
                                                        // immediately
                                                        // redeliverable.
                                                        NackAction::Requeue => {
                                                            SettleAction::Requeue
                                                        }
                                                        // Settled away —
                                                        // acknowledged, never
                                                        // redelivered.
                                                        NackAction::Drop => SettleAction::Ack,
                                                    };
                                                    tx.send((id, settled)).map_err(|e| {
                                                        BrokerError::Failed(format!(
                                                            "gcpubsub settle: {e}"
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
                        // A transient pull failure backs off briefly; the
                        // loop keeps pulling.
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(250)).await;
                        }
                    }
                }
            });

            Ok(Box::new(GcpPubSubSubscriber {
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

/// One pulled delivery en route to the handler.
struct Delivery {
    topic: String,
    message: Message,
    ack_id: String,
    settle_tx: mpsc::UnboundedSender<(String, SettleAction)>,
}

impl GcpPubSubBroker {
    fn clone_inner(&self) -> GcpPubSubBroker {
        GcpPubSubBroker {
            inner: Arc::clone(&self.inner),
        }
    }

    /// One bounded pull: POST the subscription's pull endpoint, decode
    /// the batch, and dispatch — under Manual, settlement commands ride
    /// the channel back to this pump (the settle calls need `&self`,
    /// which the pump holds).
    async fn pull_once(
        &self,
        topic: &str,
        settle_tx: mpsc::UnboundedSender<(String, SettleAction)>,
    ) -> Result<Vec<Delivery>, BrokerError> {
        let subscription = format!(
            "projects/{}/subscriptions/{}",
            self.inner.options.project_id, self.inner.options.subscription_id
        );
        let body = self
            .call(
                &format!("{subscription}:pull"),
                json!({
                    "maxMessages": PULL_BATCH,
                    // Return promptly so cancellation stays responsive;
                    // the pump loops and re-pulls.
                    "returnImmediately": true,
                }),
                &format!("pull {topic}"),
            )
            .await?;
        let mut deliveries = Vec::new();
        let received = body
            .get("receivedMessages")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        for received_message in &received {
            let Some(ack_id) = received_message.get("ackId").and_then(Value::as_str) else {
                continue;
            };
            let Some(message_body) = received_message.get("message") else {
                continue;
            };
            let data = message_body
                .get("data")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let payload = match base64::engine::general_purpose::STANDARD.decode(data) {
                Ok(bytes) => bytes,
                // Not the engine's envelope: a foreign producer's raw
                // data rides as its own bytes. Failing the batch here
                // would starve the whole subscription — one undecodable
                // message would redeliver forever (its ack deadline
                // keeps lapsing) and nothing after it would ever get
                // through.
                Err(_) => data.as_bytes().to_vec(),
            };
            let mut message = Message::from_payload(payload);
            if let Some(attributes) = message_body.get("attributes").and_then(Value::as_object) {
                for (key, value) in attributes {
                    if let Some(value) = value.as_str() {
                        message.headers.insert(key.clone(), value.to_string());
                    }
                }
            }
            if let Some(message_id) = message_body.get("messageId").and_then(Value::as_str) {
                message
                    .metadata
                    .insert("messageId".to_string(), message_id.to_string());
            }
            deliveries.push(Delivery {
                topic: topic.to_string(),
                message,
                ack_id: ack_id.to_string(),
                settle_tx: settle_tx.clone(),
            });
        }
        Ok(deliveries)
    }
}

/// The subscription handle. `unsubscribe` stops the pull loop
/// authoritatively; dropping it stops the same loop best-effort — the
/// registry's lock may be contended at drop time, in which case only
/// the explicit call is guaranteed.
struct GcpPubSubSubscriber {
    topic: String,
    shared: Arc<SubscriptionShared>,
    inner: Arc<Inner>,
}

impl GcpPubSubSubscriber {
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

impl Subscriber for GcpPubSubSubscriber {
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

impl Drop for GcpPubSubSubscriber {
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

use serde_json::Value;
