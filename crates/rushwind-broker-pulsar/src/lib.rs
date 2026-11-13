//! Pulsar engine for the RushWind broker, over `pulsar` (pulsar-rs).
//!
//! # The wire behavior
//!
//! Publishes go through the crate's multi-topic producer, which
//! creates and caches one producer per topic, with the caching
//! inside the client library. The
//! payload rides raw (the crate's byte passthrough), the message
//! headers map to the record's user properties, and a nonempty
//! contract key becomes the partition key. The send awaits the
//! broker's receipt.
//!
//! Subscribes build a `Shared`-type consumer under a fixed
//! subscription name. Deliveries surface as events on their
//! origin topic with the payload (raw), the user properties as
//! headers, and the partition key lossily decoded into the
//! contract's string key. Each delivery is acknowledged
//! immediately after its handler is spawned rather than after the
//! handler returns, so this is
//! an implicit-acknowledgment engine and [`Event::ack`] is a no-op.
//!
//! The consumer ends on `unsubscribe` or on the stream's own end —
//! a closed consumer or an error — without a retry loop. A
//! duplicate-topic subscribe replaces the topic's live flow first
//! (unsub-old-then-sub-new): the fixed Shared subscription name means
//! a second consumer on the same topic would otherwise siphon half
//! the deliveries away from the first handler.
//!
//! `disconnect` closes the client and clears the producers; a
//! subsequent `connect` rebuilds both from the stored address, so a
//! disconnect never leaves the broker permanently unusable.
//! Publishing and subscribing while disconnected fail with
//! [`BrokerError::NotConnected`].
//!
//! # Divergences
//!
//! - Unsubscribe closes the consumer but sends no
//!   unsubscribe-request: the server-side subscription and its
//!   backlog survive until the broker's retention reaps them
//!   (pulsar-rs exposes no unsubscribe call).
//! - Producer rotation and retry on a cached producer's
//!   failure are out of scope: a failed send surfaces as an error
//!   and the producer stays cached (the library owns the cache).
//! - Batch knobs, send timeouts, TLS and auth dialer knobs,
//!   dead-letter policies, and tracers are not exposed: the
//!   constructor takes the address only. An empty address falls
//!   back to the `pulsar://127.0.0.1:6650` default.
//!
//! # Testing
//!
//! Live conformance tests run against a real Apache Pulsar
//! (standalone) via the `live` feature; there is no embedded Pulsar
//! for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::TryStreamExt;
use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// The fixed subscription name every consumer registers with.
const SUBSCRIPTION_NAME: &str = "my-subscription";

/// Wraps a pulsar-rs error as a broker failure.
fn pulsar_err(error: pulsar::Error) -> BrokerError {
    BrokerError::Failed(format!("pulsar: {error}"))
}

struct Inner {
    /// The service URL the client was built from — `connect` needs it
    /// to rebuild after a `disconnect` closed the client.
    addr: String,
    client: Mutex<Option<pulsar::Pulsar<pulsar::TokioExecutor>>>,
    producer: Mutex<Option<pulsar::MultiTopicProducer<pulsar::TokioExecutor>>>,
    connected: AtomicBool,
    /// The live subscription flow per topic. Re-subscribing a topic
    /// tears the previous flow down first — the fixed Shared
    /// subscription name would otherwise let the new consumer siphon
    /// half the deliveries from the old handler (upstream:
    /// duplicate-topic subscribe left the old consumer consuming).
    subscriptions: Mutex<HashMap<String, Arc<SubscriptionFlow>>>,
}

/// The delivery flow of one subscription: the cancellation token and
/// the pump task, shared between the caller's subscriber handle and
/// the engine's topic registry so both paths tear the same pump down.
struct SubscriptionFlow {
    done: CancellationToken,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl SubscriptionFlow {
    /// Cancels the pump and waits for it to wind down. Idempotent: a
    /// second call finds no task and is a no-op.
    async fn stop(&self) {
        self.done.cancel();
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }

    /// Best-effort teardown for `Drop`: cancel and abort without
    /// awaiting.
    fn abort(&self) {
        self.done.cancel();
        if let Ok(mut task) = self.task.try_lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }
}

/// A Pulsar-backed broker over pulsar-rs.
pub struct PulsarBroker {
    inner: Arc<Inner>,
    /// The subscription acknowledgment policy (see [`AckMode`]).
    ack_mode: AckMode,
}

/// The bootstrap factory's settings wire shape for
/// [`PulsarBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct PulsarSettings {
    /// The Pulsar service URL (`pulsar://host:port`). Absent falls
    /// back to the default localhost service.
    pub addr: Option<String>,
    /// The acknowledgment policy; absent reads as [`AckMode::Auto`].
    pub ack_mode: Option<AckMode>,
}

/// Dials the Pulsar service and returns the client with its
/// multi-topic producer. Shared by the constructor and
/// [`Broker::connect`]'s rebuild path.
async fn establish(
    addr: &str,
) -> Result<
    (
        pulsar::Pulsar<pulsar::TokioExecutor>,
        pulsar::MultiTopicProducer<pulsar::TokioExecutor>,
    ),
    BrokerError,
> {
    let client: pulsar::Pulsar<pulsar::TokioExecutor> =
        pulsar::Pulsar::builder(addr.to_string(), pulsar::TokioExecutor)
            .build()
            .await
            .map_err(pulsar_err)?;
    let producer = client.clone().producer().build_multi_topic();
    Ok((client, producer))
}

/// The service URL fallback when the caller passes an empty address.
const DEFAULT_ADDR: &str = "pulsar://127.0.0.1:6650";

impl PulsarBroker {
    /// Connects to the Pulsar service at `addr` (e.g.
    /// `pulsar://127.0.0.1:6650`). An empty address falls back to
    /// the default localhost service.
    pub async fn connect(addr: &str) -> Result<Self, BrokerError> {
        Self::with_ack_mode(addr, AckMode::Auto).await
    }

    /// Connects under an explicit acknowledgment policy —
    /// [`AckMode::Manual`] routes the native ack/nack (by message id,
    /// through the pump's settlement channel) to the handler's hooks.
    pub async fn with_ack_mode(addr: &str, ack_mode: AckMode) -> Result<Self, BrokerError> {
        let addr = if addr.is_empty() { DEFAULT_ADDR } else { addr };
        let (client, producer) = establish(addr).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                addr: addr.to_string(),
                client: Mutex::new(Some(client)),
                producer: Mutex::new(Some(producer)),
                connected: AtomicBool::new(true),
                subscriptions: Mutex::new(HashMap::new()),
            }),
            ack_mode,
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (optional; empty falls back to the default
    /// localhost service).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: PulsarSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::with_ack_mode(
            settings.addr.as_deref().unwrap_or(""),
            settings.ack_mode.unwrap_or_default(),
        )
        .await
    }
}

/// Maps a delivered record onto a contract event: raw payload, user
/// properties as headers, partition key as the contract's string
/// key.
fn to_event(record: &pulsar::consumer::Message<Vec<u8>>) -> Event {
    let mut message = Message::from_payload(record.deserialize());
    for pair in &record.metadata().properties {
        message.headers.insert(pair.key.clone(), pair.value.clone());
    }
    message.key = record.key().unwrap_or_default();
    Event::new(record.topic.clone(), message)
}

impl Broker for PulsarBroker {
    fn name(&self) -> &'static str {
        "pulsar"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // The client is live from construction — unless a
            // disconnect closed it. Rebuild it from the stored address:
            // a closed client left in place would make the broker
            // permanently unusable (upstream made Connect recreate the
            // closed client for the same reason).
            if self.inner.connected.load(Ordering::SeqCst)
                && self.inner.client.lock().await.is_some()
            {
                return Ok(());
            }
            let (client, producer) = establish(&self.inner.addr).await?;
            *self.inner.client.lock().await = Some(client);
            *self.inner.producer.lock().await = Some(producer);
            self.inner.connected.store(true, Ordering::SeqCst);
            Ok(())
        })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.inner.connected.store(false, Ordering::SeqCst);
            // Close every per-topic producer, then drop the
            // multi-topic producer and the client.
            let mut guard = self.inner.producer.lock().await;
            if let Some(producer) = guard.as_mut() {
                for topic in producer.topics() {
                    let _ = producer.close_producer(topic).await;
                }
            }
            *guard = None;
            *self.inner.client.lock().await = None;
            Ok(())
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            if !self.inner.connected.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let mut guard = self.inner.producer.lock().await;
            let Some(producer) = guard.as_mut() else {
                return Err(BrokerError::NotConnected);
            };
            // The record carries the payload raw, the headers as user
            // properties, and a nonempty key as the partition key.
            let record = pulsar::producer::Message {
                payload: message.payload,
                properties: message.headers,
                partition_key: if message.key.is_empty() {
                    None
                } else {
                    Some(message.key)
                },
                ..Default::default()
            };
            producer
                .send_non_blocking(topic, record)
                .await
                .map_err(pulsar_err)?
                .await
                .map_err(pulsar_err)?;
            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            if !self.inner.connected.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let client = match self.inner.client.lock().await.clone() {
                Some(client) => client,
                None => return Err(BrokerError::NotConnected),
            };
            // The consumer is built first: on a failure nothing is
            // registered and the topic's previous flow stays intact
            // (failed registration cleanup).
            let consumer: pulsar::consumer::Consumer<Vec<u8>, pulsar::TokioExecutor> = client
                .consumer()
                .with_topic(topic.to_string())
                .with_subscription_type(pulsar::SubType::Shared)
                .with_subscription(SUBSCRIPTION_NAME)
                .build()
                .await
                .map_err(pulsar_err)?;

            let flow = Arc::new(SubscriptionFlow {
                done: CancellationToken::new(),
                task: Mutex::new(None),
            });
            let task_done = flow.done.clone();
            let handler = Arc::clone(&handler);
            // Manual mode's settlement channel: the consumer is `&mut
            // self` on its ack/nack, so the pump stays its sole owner
            // and the handlers' settle hooks ride commands back to it —
            // no lock, no waiting on the consumer's receive path.
            let (settle_tx, mut settle_rx) =
                tokio::sync::mpsc::unbounded_channel::<(pulsar::proto::MessageIdData, NackAction)>(
                );
            let ack_mode = self.ack_mode;
            let record_topic = topic.to_string();
            let task = tokio::spawn(async move {
                let mut consumer = consumer;
                loop {
                    let delivery = tokio::select! {
                        _ = task_done.cancelled() => {
                            let _ = consumer.close().await;
                            return;
                        }
                        settle = settle_rx.recv() => match settle {
                            Some((msg_id, action)) => {
                                let outcome = match action {
                                    NackAction::Requeue => {
                                        consumer.nack_with_id(&record_topic, msg_id).await
                                    }
                                    NackAction::Drop => {
                                        consumer.ack_with_id(&record_topic, msg_id).await
                                    }
                                };
                                if let Err(e) = outcome {
                                    // A failed settlement is the
                                    // redelivery timer's business; the
                                    // loop continues.
                                    let _ = e;
                                }
                                continue;
                            }
                            None => {
                                let _ = consumer.close().await;
                                return;
                            }
                        },
                        delivery = consumer.try_next() => delivery,
                    };
                    match delivery {
                        Ok(Some(record)) => match ack_mode {
                            AckMode::Auto => {
                                let event = to_event(&record);
                                tokio::spawn(handler(event));
                                // Auto-ack semantics:
                                // every delivery is acknowledged once
                                // dispatched.
                                let _ = consumer.ack(&record).await;
                            }
                            AckMode::Manual => {
                                // Each hook owns its own sender/id copies —
                                // the outer pair stays live for the rest of
                                // the pump loop.
                                let ack_tx = settle_tx.clone();
                                let ack_id = record.message_id().clone();
                                let nack_tx = settle_tx.clone();
                                let nack_id = record.message_id().clone();
                                let event = Event::settle_with(
                                    record.topic.clone(),
                                    to_event(&record).message().clone(),
                                    move || {
                                        Box::pin(async move {
                                            // The success settle maps onto
                                            // the Drop action: the pump
                                            // routes Drop to the native
                                            // ack — the message will not
                                            // redeliver.
                                            ack_tx.send((ack_id, NackAction::Drop)).map_err(|e| {
                                                BrokerError::Failed(format!("pulsar settle: {e}"))
                                            })
                                        })
                                            as BoxFuture<'static, Result<(), BrokerError>>
                                    },
                                    move |action| {
                                        Box::pin(async move {
                                            nack_tx.send((nack_id, action)).map_err(|e| {
                                                BrokerError::Failed(format!("pulsar settle: {e}"))
                                            })
                                        })
                                            as BoxFuture<'static, Result<(), BrokerError>>
                                    },
                                );
                                tokio::spawn(handler(event));
                            }
                        },
                        Ok(None) | Err(_) => {
                            // The stream ended: closed consumer or an
                            // error. The pump ends with it.
                            let _ = consumer.close().await;
                            return;
                        }
                    }
                }
            });
            *flow.task.lock().await = Some(task);

            // Unsub-old-then-sub-new: the new pump is live, so swap the
            // registry entry and stop the previous flow — under the
            // fixed Shared subscription name an old consumer left
            // running would take a share of the deliveries away from
            // this handler.
            let previous = {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                subscriptions.insert(topic.to_string(), Arc::clone(&flow))
            };
            if let Some(previous) = previous {
                previous.stop().await;
            }

            Ok(Box::new(PulsarSubscriber {
                topic: topic.to_string(),
                flow,
                inner: Arc::clone(&self.inner),
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The Pulsar subscription handle: its flow tears the delivery pump
/// down on unsubscribe or drop.
struct PulsarSubscriber {
    topic: String,
    flow: Arc<SubscriptionFlow>,
    /// The engine's shared state, so an unsubscribe can retire its own
    /// (still-registered) flow entry from the topic registry.
    inner: Arc<Inner>,
}

impl Subscriber for PulsarSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // Retire the registry entry only while it still points at
            // this handle's flow — a re-subscribe has already swapped
            // in a newer one.
            {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                if subscriptions
                    .get(&self.topic)
                    .map(|flow| Arc::ptr_eq(flow, &self.flow))
                    .unwrap_or(false)
                {
                    subscriptions.remove(&self.topic);
                }
            }
            self.flow.stop().await;
            Ok(())
        })
    }
}

impl Drop for PulsarSubscriber {
    fn drop(&mut self) {
        // Best-effort: cancel and abort without awaiting the task.
        self.flow.abort();
    }
}
