//! RabbitMQ engine for the RushWind broker, over `lapin`.
//!
//! # The wire behavior
//!
//! Publishes go to the **`amq.topic` topic exchange** (durable and
//! pre-existing on every broker) with
//! the topic as the routing key; the message headers ride the AMQP
//! `headers` table — RabbitMQ, unlike MQTT/Redis/NATS, does carry
//! them. Each subscription declares an anonymous, exclusive,
//! auto-delete queue bound to the exchange with the topic as the
//! routing key, and consumes it with automatic acknowledgment.
//!
//! Topic routing follows AMQP topic-match rules (`*` one word, `#`
//! zero or more words, `.` separators).
//!
//! # Connection lifecycle
//!
//! The AMQP connection is live from construction; `Subscribe` opens
//! the consume path synchronously — the call returning means the
//! queue is declared, bound and consuming, so a publish from that
//! point on is delivered. ONE reader task per subscription then
//! drains the deliveries and owns every re-open: when the stream
//! ends, it re-consumes in place on a live connection, or drives a
//! single-flight re-dial (exponential backoff, 100 ms → 30 s) when
//! the connection died, and resumes on the new connection — a
//! subscription never runs two consumers (the upstream Go consumer
//! model after its rework). A publish against a dead connection kicks
//! off the same re-dial in the background.
//!
//! [`Broker::disconnect`] closes the connection and is idempotent; a
//! second call is a no-op (the upstream Go second close panicked on a
//! channel that was never recreated). While closed, publishes and
//! subscribes return [`BrokerError::NotConnected`]; [`Broker::connect`]
//! re-establishes the connection. Subscribing a topic that already
//! has a live subscription retires the previous flow first — the old
//! consumer is cancelled and its queue gone before the new one starts
//! — and a failed declare/bind never leaves a registered entry.
//!
//! # Divergences
//!
//! - One exchange (`amq.topic`); per-call exchange selection
//!   and custom exchange registration are not available.
//! - Acknowledgment follows [`AckMode`]: `Auto` (the default)
//!   acknowledges at dispatch and the event hooks are no-ops;
//!   `Manual` subscribes without auto-settlement and hands the
//!   delivery's native AMQP acker to both hooks — ack on success,
//!   nack with `requeue` per the [`NackAction`] on poison.
//! - The delivery mode (persistent/transient) is not configurable —
//!   AMQP's unset default treats deliveries as transient.
//!
//! # Testing
//!
//! Live conformance tests run against a real RabbitMQ via the `live`
//! feature; there is no embedded RabbitMQ for CI's unit lanes. The
//! broker cannot be constructed offline (the AMQP handshake dials at
//! construction), so the upstream-fix behaviors here are pinned by
//! the live suite.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::StreamExt;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, QueueBindOptions,
    QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Consumer};
use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};
use tokio::sync::{watch, Mutex};
use tokio_util::sync::CancellationToken;

/// The exchange every publish and bind goes through.
const DEFAULT_EXCHANGE: &str = "amq.topic";

/// Re-open backoff floor — the upstream `defaultMinResubscribeDelay`.
const MIN_RESUBSCRIBE_DELAY: Duration = Duration::from_millis(100);
/// Re-open backoff cap — the upstream `defaultMaxResubscribeDelay`.
const MAX_RESUBSCRIBE_DELAY: Duration = Duration::from_secs(30);

/// A subscription's shared teardown state. The registry keeps one per
/// topic so a duplicate subscribe can retire the previous delivery
/// flow — cancel the reader, wait for it, close its channel — even
/// though the old [`RabbitmqSubscriber`] handle still lives in the
/// caller's hands.
struct SubscriptionShared {
    /// Cancels the reader task.
    done: CancellationToken,
    /// The reader's current consumer channel; closing it cancels the
    /// consumer server-side and the exclusive auto-delete queue dies
    /// with it. The reader swaps it on every re-open.
    channel: Mutex<Option<Channel>>,
    /// The reader task, taken and awaited by whichever teardown runs
    /// first (unsubscribe or duplicate-subscribe retirement).
    reader: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct Inner {
    /// The one input every connection — initial or re-dialed —
    /// derives from. The upstream Go reconnect lost the address once;
    /// here the URL outlives any connection.
    url: String,
    /// The live AMQP connection, swapped wholesale on re-dial.
    connection: RwLock<Arc<Connection>>,
    /// Bumped on every install; subscription readers wait on it — the
    /// upstream `waitConnection` channel, reshaped as a watch.
    generation: watch::Sender<u64>,
    /// Stands after [`Broker::disconnect`]: publishes and subscribes
    /// refuse, readers exit.
    closed: AtomicBool,
    /// Single-flight guard so one dead connection dials once, not once
    /// per subscription.
    redial: Mutex<()>,
    /// Subscribed topics and their teardown state, keyed by topic.
    subscriptions: Mutex<HashMap<String, Arc<SubscriptionShared>>>,
}

impl Inner {
    fn snapshot(&self) -> Arc<Connection> {
        self.connection
            .read()
            .expect("connection lock poisoned")
            .clone()
    }

    fn current_generation(&self) -> u64 {
        *self.generation.borrow()
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Installs a freshly dialed connection and wakes every
    /// subscription reader onto it.
    fn install(&self, connection: Connection) {
        let next = self.current_generation() + 1;
        *self.connection.write().expect("connection lock poisoned") = Arc::new(connection);
        self.generation.send_replace(next);
    }

    /// Dials once and installs; the manual [`Broker::connect`] path.
    async fn dial(&self) -> Result<(), BrokerError> {
        let connection = Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .map_err(|e| BrokerError::Failed(format!("rabbitmq connect {}: {e}", self.url)))?;
        self.install(connection);
        Ok(())
    }

    /// Drives the re-dial until the generation moves past `known` or
    /// the engine closes — exponential backoff, single-flight: the
    /// first reader that noticed does the dialing, the rest ride its
    /// install (the upstream supervisor + `waitConnection` shape).
    async fn ensure_connection(
        &self,
        known: u64,
        delay: &mut Duration,
        cancelled: &CancellationToken,
    ) {
        if cancelled.is_cancelled() || self.current_generation() != known {
            return;
        }
        let _guard = self.redial.lock().await;
        // Re-checked under the guard: a rival dial may have landed
        // while this caller waited on the mutex.
        if self.current_generation() != known {
            return;
        }
        loop {
            if cancelled.is_cancelled() || self.is_closed() {
                return;
            }
            match Connection::connect(&self.url, ConnectionProperties::default()).await {
                Ok(connection) => {
                    self.install(connection);
                    return;
                }
                Err(_) => {
                    // A failed dial sleeps with growing backoff;
                    // cancellation or a rival install interrupts it.
                    tokio::select! {
                        _ = cancelled.cancelled() => return,
                        _ = tokio::time::sleep(*delay) => {
                            *delay = (*delay * 2).min(MAX_RESUBSCRIBE_DELAY);
                        }
                    }
                    if cancelled.is_cancelled() || self.current_generation() != known {
                        return;
                    }
                }
            }
        }
    }

    /// One background re-dial attempt for the publish path — a
    /// publish-only broker also recovers, without stalling the
    /// failing call.
    async fn redial_once(self: &Arc<Self>) {
        if self.is_closed() {
            return;
        }
        let _guard = self.redial.lock().await;
        if self.is_closed() || self.snapshot().status().connected() {
            return;
        }
        if let Ok(connection) =
            Connection::connect(&self.url, ConnectionProperties::default()).await
        {
            self.install(connection);
        }
    }

    fn spawn_redial_once(self: &Arc<Self>) {
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            inner.redial_once().await;
        });
    }
}

/// A RabbitMQ-backed broker over AMQP 0-9-1.
pub struct RabbitmqBroker {
    inner: Arc<Inner>,
    /// The subscription acknowledgment policy. `Auto` (the default)
    /// acknowledges at dispatch; `Manual` hands the native ack/nack to
    /// the handler through the event's hooks — the poison-message
    /// settlement face.
    ack_mode: AckMode,
}

/// The bootstrap factory's settings wire shape for
/// [`RabbitmqBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct RabbitmqSettings {
    /// The AMQP connection URL.
    pub url: String,
    /// The acknowledgment policy; absent reads as [`AckMode::Auto`].
    pub ack_mode: Option<AckMode>,
}

impl RabbitmqBroker {
    /// Connects to RabbitMQ at `url` (e.g.
    /// `amqp://guest:guest@127.0.0.1:5672`).
    pub async fn connect(url: &str) -> Result<Self, BrokerError> {
        Self::with_ack_mode(url, AckMode::Auto).await
    }

    /// Connects under an explicit acknowledgment policy —
    /// [`AckMode::Manual`] hands the native ack/nack to handlers
    /// through the event's hooks (the poison-message settlement face).
    pub async fn with_ack_mode(url: &str, ack_mode: AckMode) -> Result<Self, BrokerError> {
        let connection = Connection::connect(url, ConnectionProperties::default())
            .await
            .map_err(|e| BrokerError::Failed(format!("rabbitmq connect {url}: {e}")))?;
        let (generation, _) = watch::channel(1);
        Ok(Self {
            inner: Arc::new(Inner {
                url: url.to_string(),
                connection: RwLock::new(Arc::new(connection)),
                generation,
                closed: AtomicBool::new(false),
                redial: Mutex::new(()),
                subscriptions: Mutex::new(HashMap::new()),
            }),
            ack_mode,
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `url` (required), `ack_mode` (optional).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: RabbitmqSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::with_ack_mode(&settings.url, settings.ack_mode.unwrap_or_default()).await
    }
}

/// Opens the consume path on `connection`: an anonymous, exclusive,
/// auto-delete queue bound to the exchange with the topic as the
/// routing key, consumed per the engine's [`AckMode`].
async fn open_consumer(
    connection: &Connection,
    topic: &str,
) -> Result<(Channel, Consumer), BrokerError> {
    let channel = connection
        .create_channel()
        .await
        .map_err(|e| BrokerError::Failed(format!("rabbitmq channel: {e}")))?;
    let queue = channel
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| BrokerError::Failed(format!("rabbitmq queue declare: {e}")))?
        .name()
        .to_string();
    channel
        .queue_bind(
            queue.as_str().into(),
            DEFAULT_EXCHANGE.into(),
            topic.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|e| BrokerError::Failed(format!("rabbitmq bind {topic}: {e}")))?;
    let consumer = channel
        .basic_consume(
            queue.as_str().into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|e| BrokerError::Failed(format!("rabbitmq consume {topic}: {e}")))?;
    Ok((channel, consumer))
}

/// Dispatches one delivery: fire-and-forget handler spawn (a handler
/// error or panic cannot kill the reader loop). Under [`AckMode::Auto`]
/// the automatic acknowledgment rides the spawn; under
/// [`AckMode::Manual`] the delivery's native acker/nacker ride the
/// event's hooks instead and nothing settles until the handler does —
/// a handler error can nack the poison message back to the queue.
async fn dispatch(delivery: Delivery, handler: &Handler, mode: AckMode) {
    let routing_key = delivery.routing_key.to_string();
    let payload = delivery.data.clone();
    let mut message = Message::from_payload(payload);
    if let Some(table) = delivery.properties.headers() {
        for (key, value) in table.inner().iter() {
            if let Some(bytes) = value.as_long_string() {
                message.headers.insert(
                    key.to_string(),
                    String::from_utf8_lossy(bytes.as_bytes()).to_string(),
                );
            }
        }
    }
    match mode {
        AckMode::Auto => {
            let event = Event::new(routing_key, message);
            tokio::spawn(handler(event));
            let _ = delivery.ack(BasicAckOptions::default()).await;
        }
        AckMode::Manual => {
            // Both hooks carry clones of the delivery's own acker — the
            // AMQP settlement handle for this exact delivery tag. The
            // handler acks on success; on poison it nacks, requeue per
            // the action, and the broker's redelivery takes over.
            let ack_acker = delivery.acker.clone();
            let event = Event::settle_with(
                routing_key,
                message,
                move || {
                    let acker = ack_acker.clone();
                    Box::pin(async move {
                        acker
                            .ack(BasicAckOptions::default())
                            .await
                            .map(|_| ())
                            .map_err(|e| BrokerError::Failed(format!("rabbitmq ack: {e}")))
                    }) as BoxFuture<'static, Result<(), BrokerError>>
                },
                move |action| {
                    let acker = delivery.acker.clone();
                    let requeue = action == NackAction::Requeue;
                    Box::pin(async move {
                        acker
                            .nack(BasicNackOptions {
                                requeue,
                                ..BasicNackOptions::default()
                            })
                            .await
                            .map(|_| ())
                            .map_err(|e| BrokerError::Failed(format!("rabbitmq nack: {e}")))
                    }) as BoxFuture<'static, Result<(), BrokerError>>
                },
            );
            tokio::spawn(handler(event));
        }
    }
}

/// Closes the subscription's current channel: the close cancels the
/// consumer server-side and the exclusive auto-delete queue dies with
/// it. Best-effort — a dead connection rejects the close frames.
async fn retire_channel(shared: &SubscriptionShared) {
    if let Some(channel) = shared.channel.lock().await.take() {
        let _ = channel
            .close(200, lapin::types::ShortString::from("unsubscribed"))
            .await;
    }
}

impl Broker for RabbitmqBroker {
    fn name(&self) -> &'static str {
        "rabbitmq"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            if self.inner.closed.swap(false, Ordering::SeqCst) {
                // Connect after Disconnect re-establishes the
                // connection — the upstream rebuild of its wait
                // channel exists exactly so this cycle works.
                return self.inner.dial().await;
            }
            let connection = self.inner.snapshot();
            if connection.status().connected() {
                // The AMQP connection is live from construction; a
                // channel probe verifies it eagerly.
                connection
                    .create_channel()
                    .await
                    .map_err(|e| BrokerError::Failed(format!("rabbitmq channel: {e}")))?;
                Ok(())
            } else {
                // Dead but not closed — a network drop: re-dial.
                self.inner.dial().await
            }
        })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // Idempotent: the upstream second close panicked on the
            // never-recreated wait channel; a repeat here is a no-op.
            if self.inner.closed.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            let connection = self.inner.snapshot();
            // Best-effort: an already-dead connection rejects the
            // close frames; the reader tasks exit on the closed flag
            // either way.
            let _ = connection
                .close(200, lapin::types::ShortString::from("broker disconnect"))
                .await;
            Ok(())
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            if self.inner.is_closed() {
                return Err(BrokerError::NotConnected);
            }
            let connection = self.inner.snapshot();
            if !connection.status().connected() {
                // The connection died: kick the background re-dial and
                // fail this call — deliveries recover on the new
                // connection, the next publish gets it fresh.
                self.inner.spawn_redial_once();
                return Err(BrokerError::NotConnected);
            }
            let channel = match connection.create_channel().await {
                Ok(channel) => channel,
                Err(error) => {
                    // A channel refusal on a nominally live connection
                    // is usually a connection that just died; the same
                    // background re-dial applies.
                    self.inner.spawn_redial_once();
                    return Err(BrokerError::Failed(format!("rabbitmq channel: {error}")));
                }
            };
            let mut headers = FieldTable::default();
            for (key, value) in &message.headers {
                headers.insert(
                    key.as_str().into(),
                    lapin::types::AMQPValue::LongString(value.as_bytes().into()),
                );
            }
            channel
                .basic_publish(
                    DEFAULT_EXCHANGE.into(),
                    topic.into(),
                    BasicPublishOptions::default(),
                    &message.payload,
                    BasicProperties::default().with_headers(headers),
                )
                .await
                .map_err(|e| BrokerError::Failed(format!("rabbitmq publish: {e}")))?
                .await
                .map_err(|e| BrokerError::Failed(format!("rabbitmq publish confirm: {e}")))?;
            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            if self.inner.is_closed() {
                return Err(BrokerError::NotConnected);
            }
            let connection = self.inner.snapshot();
            if !connection.status().connected() {
                return Err(BrokerError::NotConnected);
            }
            // The consume path opens synchronously: Subscribe
            // returning means the queue is declared, bound AND
            // consuming — a publish from this point on is delivered.
            // Failure here happens before any registry change, so a
            // failed subscription never stays registered (and a failed
            // duplicate subscribe leaves the previous flow untouched).
            let (channel, consumer) = open_consumer(&connection, topic).await?;

            let done = CancellationToken::new();
            let shared = Arc::new(SubscriptionShared {
                done: done.clone(),
                channel: Mutex::new(Some(channel)),
                reader: Mutex::new(None),
            });

            // Duplicate subscribe retires the previous flow before the
            // new reader starts: two anonymous queues bound to the
            // same routing key would both receive every publish. The
            // retirement awaits the old reader, so once this subscribe
            // returns the topic has exactly one consumer again.
            {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                if let Some(previous) = subscriptions.insert(topic.to_string(), Arc::clone(&shared))
                {
                    previous.done.cancel();
                    if let Some(previous_reader) = previous.reader.lock().await.take() {
                        let _ = previous_reader.await;
                    }
                    if let Some(previous_channel) = previous.channel.lock().await.take() {
                        let _ = previous_channel
                            .close(200, lapin::types::ShortString::from("replaced"))
                            .await;
                    }
                }
            }

            let task_done = done.clone();
            let inner = Arc::clone(&self.inner);
            let inner_for_task = Arc::clone(&self.inner);
            let shared_for_task = Arc::clone(&shared);
            let topic_for_task = topic.to_string();
            let ack_mode = self.ack_mode;
            let reader = tokio::spawn(async move {
                let inner = inner_for_task;
                let shared = shared_for_task;
                let mut generation = inner.generation.subscribe();
                let mut consumer = consumer;
                let mut delay = MIN_RESUBSCRIBE_DELAY;
                loop {
                    // Drain the open consumer until it ends, the
                    // engine re-dials (the generation moves — drop this
                    // consumer and re-open on the new connection), or
                    // the subscription closes.
                    loop {
                        tokio::select! {
                            _ = task_done.cancelled() => {
                                retire_channel(&shared).await;
                                return;
                            }
                            _ = generation.changed() => break,
                            delivery = consumer.next() => match delivery {
                                Some(Ok(delivery)) => dispatch(delivery, &handler, ack_mode).await,
                                // The stream ended: channel or
                                // connection gone.
                                _ => break,
                            },
                        }
                    }
                    // The re-open stage — one loop serves both the
                    // first re-open and every reconnect: wait for a
                    // connection, consume, drain; never two consumers
                    // for one subscription.
                    loop {
                        if task_done.is_cancelled() || inner.is_closed() {
                            retire_channel(&shared).await;
                            return;
                        }
                        let known = *generation.borrow_and_update();
                        let connection = inner.snapshot();
                        if connection.status().connected() {
                            // A live connection re-consumes in place
                            // (the upstream waitConnection gate passes
                            // immediately when the connection is fine).
                            if let Ok((next_channel, next_consumer)) =
                                open_consumer(&connection, &topic_for_task).await
                            {
                                let previous = shared.channel.lock().await.replace(next_channel);
                                if let Some(previous) = previous {
                                    let _ = previous
                                        .close(200, lapin::types::ShortString::from("re-consumed"))
                                        .await;
                                }
                                consumer = next_consumer;
                                delay = MIN_RESUBSCRIBE_DELAY;
                                break;
                            }
                        } else {
                            // Dead connection: whoever noticed first
                            // dials, everyone else rides the install.
                            inner.ensure_connection(known, &mut delay, &task_done).await;
                            continue;
                        }
                        // Backoff before the next attempt; cancellation
                        // interrupts it so an unsubscribe cannot stall
                        // on a dead broker.
                        tokio::select! {
                            _ = task_done.cancelled() => {
                                retire_channel(&shared).await;
                                return;
                            }
                            _ = tokio::time::sleep(delay) => {
                                delay = (delay * 2).min(MAX_RESUBSCRIBE_DELAY);
                            }
                        }
                    }
                }
            });
            *shared.reader.lock().await = Some(reader);

            Ok(Box::new(RabbitmqSubscriber {
                topic: topic.to_string(),
                shared,
                inner,
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The RabbitMQ subscription handle.
struct RabbitmqSubscriber {
    topic: String,
    shared: Arc<SubscriptionShared>,
    inner: Arc<Inner>,
}

impl Subscriber for RabbitmqSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // The registry entry is removed only while this
            // subscription still owns it: a duplicate subscribe
            // replaced it, and that newer flow must stay registered.
            {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                if subscriptions
                    .get(&self.topic)
                    .map(|entry| Arc::ptr_eq(entry, &self.shared))
                    .unwrap_or(false)
                {
                    subscriptions.remove(&self.topic);
                }
            }
            self.shared.done.cancel();
            if let Some(reader) = self.shared.reader.lock().await.take() {
                // The reader's exit path closes the channel slot.
                let _ = reader.await;
            }
            if let Some(channel) = self.shared.channel.lock().await.take() {
                match channel
                    .close(200, lapin::types::ShortString::from("unsubscribed"))
                    .await
                {
                    Ok(()) => {}
                    // A close against a dead connection is vacuous —
                    // the exclusive queue died with it; only surface a
                    // refusal from a live broker.
                    Err(error) => {
                        if self.inner.snapshot().status().connected() {
                            return Err(BrokerError::Failed(format!("rabbitmq close: {error}")));
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

impl Drop for RabbitmqSubscriber {
    fn drop(&mut self) {
        // Best-effort: cancelling the reader and dropping the channel
        // tears down the exclusive queue without awaiting the close
        // frames — the explicit unsubscribe is the reliable path.
        self.shared.done.cancel();
        if let Ok(mut reader) = self.shared.reader.try_lock() {
            if let Some(reader) = reader.take() {
                reader.abort();
            }
        }
        if let Ok(mut channel) = self.shared.channel.try_lock() {
            channel.take();
        }
    }
}
