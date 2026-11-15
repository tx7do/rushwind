//! In-process reference broker for the RushWind broker contract — the
//! semantic baseline every external broker adapter is judged against,
//! and a test double that lets broker-consuming code run without an
//! external system.
//!
//! # The shape
//!
//! Exact-topic pub/sub (the nsq end of the fleet's topic spectrum —
//! the contract's richer filters stay the engine's business). The
//! delivery semantics are what make it the baseline rather than a
//! stub:
//!
//! - [`AckMode::Auto`] settles at dispatch — a handler error cannot
//!   redeliver.
//! - [`AckMode::Manual`] parks the delivery **in-flight** (keyed by a
//!   minted message id): the event's ack settles it, the nack with
//!   [`NackAction::Requeue`] returns it to the topic queue head —
//!   immediate redelivery to the next subscriber slot — and
//!   [`NackAction::Drop`] settles it away. Redelivery is real, not
//!   simulated. A handler that neither acks nor nacks leaves the
//!   message in-flight forever — the honest consequence; production
//!   brokers time it out, the reference engine's pins drive
//!   settlement explicitly.
//! - `disconnect` marks the engine down (publish/subscribe refuse);
//!   queued-but-undelivered messages were never handed out and
//!   survive; in-flight messages are dropped (their subscribers were
//!   told to settle).

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};

/// The engine's settings (reserved for future knobs).
#[derive(Debug, Clone, Default)]
pub struct MemoryBrokerOptions {}

#[derive(Clone)]
struct SubscriberSlot {
    id: u64,
    handler: Handler,
    ack_mode: AckMode,
}

impl Inner {
    /// Whether the engine is connected.
    fn is_connected(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            queue: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
            subscriptions: Mutex::new(HashMap::new()),
            connected: AtomicBool::new(true),
            next_id: AtomicU64::new(1),
            next_slot: AtomicU64::new(1),
            cursor: AtomicU64::new(0),
        }
    }
}

struct Inner {
    /// topic → queued messages (not yet handed to a subscriber).
    queue: Mutex<HashMap<String, VecDeque<Message>>>,
    /// hex id → parked Manual delivery (topic + message).
    in_flight: Mutex<HashMap<String, (String, Message)>>,
    /// topic → live subscriber slots.
    subscriptions: Mutex<HashMap<String, Vec<SubscriberSlot>>>,
    connected: AtomicBool,
    next_id: AtomicU64,
    next_slot: AtomicU64,
    /// The persistent round-robin cursor: consecutive pump calls keep
    /// alternating so single-message pumps still spread across slots.
    cursor: AtomicU64,
}

/// The in-process reference broker.
pub struct MemoryBroker {
    inner: Arc<Inner>,
}

impl MemoryBroker {
    /// Builds the broker.
    pub fn new() -> Self {
        Self::with_options(MemoryBrokerOptions::default())
    }

    /// Builds the broker from options.
    pub fn with_options(_options: MemoryBrokerOptions) -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }

    /// The queued-but-undelivered message count for a topic — the
    /// pins' window into the store.
    pub fn queued_len(&self, topic: &str) -> usize {
        self.inner
            .queue
            .lock()
            .expect("queue poisoned")
            .get(topic)
            .map(VecDeque::len)
            .unwrap_or_default()
    }

    /// The parked (delivered, unsettled under Manual) message count.
    pub fn in_flight_len(&self) -> usize {
        self.inner
            .in_flight
            .lock()
            .expect("in-flight poisoned")
            .len()
    }

    /// Drives the queue for a topic (see the free [`pump`] fn).
    fn pump_topic(&self, topic: &str) {
        pump(&self.inner, topic);
    }
}

/// Drives the queue for a topic: hands each queued message to the
/// topic's subscriber slots round-robin (settling at dispatch under
/// Auto, parking in-flight under Manual), then wakes their handlers.
fn pump(inner: &Arc<Inner>, topic: &str) {
    let slots = {
        let subscriptions = inner.subscriptions.lock().expect("subs poisoned");
        subscriptions.get(topic).cloned().unwrap_or_default()
    };
    if slots.is_empty() {
        return;
    }
    let mut queue = inner.queue.lock().expect("queue poisoned");
    let Some(queue_for_topic) = queue.get_mut(topic) else {
        return;
    };
    // The round-robin cursor persists across pump calls (an Inner
    // counter), so consecutive single-message pumps still alternate.
    let mut slot_index = inner.cursor.fetch_add(0, Ordering::SeqCst) as usize;
    while !queue_for_topic.is_empty() {
        let slot = &slots[slot_index % slots.len()];
        let Some(message) = queue_for_topic.pop_front() else {
            break;
        };
        let dispatch_topic = topic.to_string();
        match slot.ack_mode {
            AckMode::Auto => {
                let event = Event::new(dispatch_topic, message);
                tokio::spawn((slot.handler)(event));
            }
            AckMode::Manual => {
                let id = format!("mf-{}", inner.next_id.fetch_add(1, Ordering::SeqCst));
                inner
                    .in_flight
                    .lock()
                    .expect("in-flight poisoned")
                    .insert(id.clone(), (dispatch_topic.clone(), message.clone()));
                let ack_inner = Arc::clone(inner);
                let ack_id = id.clone();
                let nack_inner = Arc::clone(inner);
                let nack_id = id.clone();
                let event = Event::settle_with(
                    dispatch_topic.clone(),
                    message,
                    move || {
                        let inner = Arc::clone(&ack_inner);
                        let id = ack_id.clone();
                        Box::pin(async move {
                            inner
                                .in_flight
                                .lock()
                                .expect("in-flight poisoned")
                                .remove(&id);
                            Ok(())
                        }) as BoxFuture<'static, Result<(), BrokerError>>
                    },
                    move |action| {
                        let inner = Arc::clone(&nack_inner);
                        let id = nack_id.clone();
                        Box::pin(async move {
                            let parked = inner
                                .in_flight
                                .lock()
                                .expect("in-flight poisoned")
                                .remove(&id);
                            let Some((topic, message)) = parked else {
                                return Ok(());
                            };
                            if action == NackAction::Requeue {
                                inner
                                    .queue
                                    .lock()
                                    .expect("queue poisoned")
                                    .entry(topic.clone())
                                    .or_default()
                                    .push_front(message);
                                // Redeliver immediately: the hook holds the
                                // shared state, so it re-pumps the topic
                                // itself.
                                pump(&inner, &topic);
                            }
                            Ok(())
                        }) as BoxFuture<'static, Result<(), BrokerError>>
                    },
                );
                tokio::spawn((slot.handler)(event));
            }
        }
        slot_index += 1;
        inner.cursor.store(slot_index as u64, Ordering::SeqCst);
    }
}

impl Default for MemoryBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl Broker for MemoryBroker {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.inner
                .connected
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.inner
                .connected
                .store(false, std::sync::atomic::Ordering::SeqCst);
            // Queued-but-undelivered messages were never handed out and
            // survive; parked in-flight messages belong to handlers that
            // were told to settle — they drop with the disconnect.
            self.inner
                .in_flight
                .lock()
                .expect("in-flight poisoned")
                .clear();
            Ok(())
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            if !self
                .inner
                .connected
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(BrokerError::NotConnected);
            }
            self.inner
                .queue
                .lock()
                .expect("queue poisoned")
                .entry(topic.to_string())
                .or_default()
                .push_back(message);
            self.pump_topic(topic);
            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            if !self
                .inner
                .connected
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(BrokerError::NotConnected);
            }
            let slot_id = self.inner.next_slot.fetch_add(1, Ordering::SeqCst);
            self.inner
                .subscriptions
                .lock()
                .expect("subs poisoned")
                .entry(topic.to_string())
                .or_default()
                .push(SubscriberSlot {
                    id: slot_id,
                    handler,
                    ack_mode: AckMode::Auto,
                });
            self.pump_topic(topic);
            Ok(Box::new(MemorySubscriber {
                topic: topic.to_string(),
                registry: Arc::clone(&self.inner),
                slot_id,
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

impl MemoryBroker {
    /// Subscribes under an explicit acknowledgment policy —
    /// [`AckMode::Manual`] parks deliveries in-flight and hands the
    /// settlement to the handler's hooks (the poison-message face).
    pub fn subscribe_with_mode<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
        ack_mode: AckMode,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            if !self
                .inner
                .connected
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(BrokerError::NotConnected);
            }
            let slot_id = self.inner.next_slot.fetch_add(1, Ordering::SeqCst);
            self.inner
                .subscriptions
                .lock()
                .expect("subs poisoned")
                .entry(topic.to_string())
                .or_default()
                .push(SubscriberSlot {
                    id: slot_id,
                    handler,
                    ack_mode,
                });
            pump(&self.inner, topic);
            Ok(Box::new(MemorySubscriber {
                topic: topic.to_string(),
                registry: Arc::clone(&self.inner),
                slot_id,
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The subscription handle: unsubscribing removes the slot so no
/// further dispatch reaches its handler.
struct MemorySubscriber {
    topic: String,
    registry: Arc<Inner>,
    slot_id: u64,
}

impl Subscriber for MemorySubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        let mut subscriptions = self.registry.subscriptions.lock().expect("subs poisoned");
        if let Some(slots) = subscriptions.get_mut(&self.topic) {
            slots.retain(|slot| slot.id != self.slot_id);
        }
        Box::pin(async { Ok(()) })
    }
}

impl std::fmt::Debug for MemoryBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBroker")
            .field("connected", &self.inner.is_connected())
            .finish()
    }
}
