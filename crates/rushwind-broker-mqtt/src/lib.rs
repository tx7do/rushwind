//! MQTT engine for the RushWind broker, over `rumqttc`.
//!
//! # The wire behavior
//!
//! Publishes default to QoS 1 without
//! the retain flag and carry **only the payload** — headers and
//! metadata have no MQTT 3.1.1 carrier, so they are dropped.
//! Subscriptions default to QoS 1 and follow
//! MQTT topic filters (`+` single-level, `#` multi-level); incoming
//! publishes are routed to every handler whose filter matches.
//!
//! # Connection lifecycle
//!
//! `connect` spawns the event-loop pump; the loop drives rumqttc's
//! automatic reconnection and, on every successful CONNACK, re-arms
//! every live subscription, as if resubscribed on connect.
//! `disconnect` retires the pump generation — without that the
//! eventloop would re-dial within a second and re-arm every
//! subscription, resurrecting a broker that was asked to shut down.
//! Publishing while disconnected fails with
//! [`BrokerError::NotConnected`]; a subscription registered while
//! disconnected is deferred and armed on the next `connect`. A
//! re-`connect` starts a fresh pump generation; the previous pump
//! exits.
//!
//! `Subscriber::unsubscribe` waits for the broker's UNSUBACK before
//! returning (bounded at ten seconds, mirroring upstream's
//! token-wait): rumqttc only queues the request, so returning on
//! enqueue alone would report a teardown the broker has not
//! confirmed yet.
//!
//! # Divergences
//!
//! - Deliveries dispatch concurrently (one task each); handler
//!   ordering is not guaranteed.
//! - Per-call QoS and retain options are not exposed — publishes
//!   use the defaults (QoS 1, no retain).
//! - Re-subscribing a topic replaces the handler in place; the wire
//!   SUBSCRIBE is re-sent and the broker substitutes the filter's
//!   subscription (MQTT-3.8.4-3), so no unsubscribe round-trip
//!   precedes the new subscribe.
//! - Dropping a [`Subscriber`] removes its filter best-effort; the
//!   explicit [`Subscriber::unsubscribe`] is the reliable path.
//!
//! # Testing
//!
//! Live conformance tests run against a real broker (EMQX works) via
//! the `live` feature; there is no embedded MQTT broker for CI's unit
//! lanes. The one wire face that a real broker is required to observe
//! — which PUBACKs the engine emits under each acknowledgment policy —
//! is pinned by a minimal frame-level mock ([`tests/manual.rs`]): it
//! speaks just CONNECT/CONNACK, SUBSCRIBE/SUBACK, PUBLISH and PUBACK,
//! which is enough to pin Auto's settle-on-dispatch, Manual's ack, and
//! Manual's Requeue-withholds / Drop-PUBACKs split.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};
use tokio::sync::{mpsc, watch, Mutex, Notify};

/// How long an unsubscribe waits for the broker's UNSUBACK before
/// failing — the bound upstream's `token.WaitTimeout(10s)` used.
const UNSUB_CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);

/// One pump generation's stop face. The `Notify` gives the pump a
/// prompt exit; the `retired` flag is the checked fallback that makes
/// the stop loss-proof: a `notify_waiters` landing between the pump's
/// await points registers no waiter and is lost, but the flag is read
/// at every loop top regardless of notification timing — without it a
/// lost notification means the pump re-dials through rumqttc's
/// automatic reconnect and resurrects the broker that was asked to
/// shut down.
struct PumpStop {
    notify: Arc<Notify>,
    retired: Arc<AtomicBool>,
}

impl PumpStop {
    fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            retired: Arc::new(AtomicBool::new(false)),
        }
    }

    fn stop(&self) {
        self.retired.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

struct Shared {
    /// The subscriptions' topic filters and their handlers, re-armed
    /// on every reconnect.
    subscriptions: Mutex<HashMap<String, Handler>>,
    connected: Mutex<bool>,
    /// Topics whose UNSUBSCRIBE is in flight, in wire order. The
    /// UNSUBACK packet identifies nothing — the pump pops the front,
    /// which the server's in-order acknowledgment guarantees is the
    /// right one.
    pending_unsubs: Mutex<VecDeque<String>>,
    /// Bumped whenever `pending_unsubs` changes. Unsubscribe waiters
    /// watch this instead of a `Notify`: a `notify_waiters` fired
    /// between the pending-check and the await would be lost, while a
    /// watch keeps the bump pending for the next `changed()` call.
    unsub_generation: watch::Sender<u64>,
    /// The live pump generation, shared with the broker so a
    /// subscriber handle can tell whether its connection still exists.
    generation: Arc<AtomicU64>,
    /// Wakes the pump when a subscription changes so reconnect
    /// backoffs cannot strand a fresh subscription.
    rearm: Notify,
}

/// An MQTT-backed broker over one rumqttc event loop.
pub struct MqttBroker {
    mqtt_options: rumqttc::MqttOptions,
    client: Mutex<Option<rumqttc::AsyncClient>>,
    shared: Arc<Shared>,
    /// The live pump generation; older pumps exit when it moves on.
    generation: Arc<AtomicU64>,
    /// Per-generation pump stop face (prompt notify + checked flag).
    shutdown: Mutex<Option<Arc<PumpStop>>>,
    /// The subscription acknowledgment policy (see [`AckMode`]).
    ack_mode: AckMode,
}

/// The MQTT engine's connection settings.
#[derive(Debug, Clone)]
pub struct MqttOptions {
    /// The broker address as `host:port`.
    pub addr: String,
    /// The MQTT client id. Default: a random `rushwind-mqtt-…` id.
    pub client_id: String,
    /// The username, when the broker demands authentication.
    pub username: Option<String>,
    /// The password.
    pub password: Option<String>,
    /// The keep-alive interval. Default: 30 s.
    pub keep_alive: Duration,
}

impl MqttOptions {
    /// Settings for `addr` with a random client id.
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            client_id: random_client_id(),
            username: None,
            password: None,
            keep_alive: Duration::from_secs(30),
        }
    }
}

impl MqttBroker {
    /// Builds the broker; call [`Broker::connect`] to go live.
    pub fn new(options: MqttOptions) -> Self {
        Self::with_ack_mode(options, AckMode::Auto)
    }

    /// Builds the broker under an explicit acknowledgment policy.
    /// [`AckMode::Manual`] hands the MQTT PUBACK to handlers through
    /// the event's ack hook. MQTT 3.1.1 has no native NACK frame, so
    /// the nack hook's honest split is: [`NackAction::Requeue`]
    /// *withholds* the PUBACK — the message stays unacknowledged and
    /// redelivery becomes the broker's decision (MQTT 3.1.1 brokers
    /// re-send unacknowledged QoS 1 messages on session resume) —
    /// while [`NackAction::Drop`] sends the PUBACK, the only wire
    /// action that stops the re-sends.
    ///
    /// A publish matching several handlers is broadcast: every handler
    /// gets its own event with its own settle hooks, but one packet id
    /// admits a single PUBACK — the first settle (ack or Drop) wins
    /// and sends it; later settles are honest no-ops. A Manual filter
    /// whose handlers only ever `Requeue` therefore never PUBACKs, and
    /// the broker's in-flight window eventually forces the re-sends.
    pub fn with_ack_mode(options: MqttOptions, ack_mode: AckMode) -> Self {
        let generation = Arc::new(AtomicU64::new(0));
        Self {
            mqtt_options: to_rumqtt_options(&options),
            client: Mutex::new(None),
            shared: Arc::new(Shared {
                subscriptions: Mutex::new(HashMap::new()),
                connected: Mutex::new(false),
                pending_unsubs: Mutex::new(VecDeque::new()),
                unsub_generation: watch::Sender::new(0),
                generation: Arc::clone(&generation),
                rearm: Notify::new(),
            }),
            generation,
            shutdown: Mutex::new(None),
            ack_mode,
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (required), `client_id`, `username`, `password`.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: MqttSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        let mut options = MqttOptions::new(settings.addr);
        if let Some(client_id) = settings.client_id {
            options.client_id = client_id;
        }
        options.username = settings.username;
        options.password = settings.password;
        Ok(Self::new(options))
    }
}

/// The bootstrap factory's settings wire shape for
/// [`MqttBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct MqttSettings {
    /// The broker address as `host:port`.
    pub addr: String,
    /// The MQTT client id. Default: a random id.
    pub client_id: Option<String>,
    /// The username.
    pub username: Option<String>,
    /// The password.
    pub password: Option<String>,
}

fn to_rumqtt_options(options: &MqttOptions) -> rumqttc::MqttOptions {
    let mut mqtt = rumqttc::MqttOptions::new(
        &options.client_id,
        host_of(&options.addr),
        port_of(&options.addr),
    );
    mqtt.set_keep_alive(options.keep_alive);
    // The engine owns every PUBACK: Auto's settle-on-dispatch and the
    // Manual hooks both go through `client.ack`. Without manual acks,
    // rumqttc auto-acknowledges at the protocol layer — Auto would
    // double-PUBACK every delivery (a protocol violation a strict
    // broker answers with a disconnect), and Manual's withheld PUBACK
    // would silently become an ack (the poison message confirmed and
    // never redelivered).
    mqtt.set_manual_acks(true);
    if let Some(username) = &options.username {
        mqtt.set_credentials(
            username.clone(),
            options.password.clone().unwrap_or_default(),
        );
    }
    mqtt
}

fn fresh_client(mqtt: &rumqttc::MqttOptions) -> (rumqttc::AsyncClient, rumqttc::EventLoop) {
    rumqttc::AsyncClient::new(mqtt.clone(), 64)
}

fn host_of(addr: &str) -> String {
    addr.rsplit_once(':')
        .map(|(host, _)| host.to_string())
        .unwrap_or_else(|| addr.to_string())
}

fn port_of(addr: &str) -> u16 {
    addr.rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .unwrap_or(1883)
}

/// A random `rushwind-mqtt-…` client id.
fn random_client_id() -> String {
    let mut bytes = [0u8; 8];
    let _ = getrandom::fill(&mut bytes);
    format!(
        "rushwind-mqtt-{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

/// MQTT topic-filter matching: `+` matches one level, `#` matches the
/// remaining levels.
fn topic_matches(filter: &str, topic: &str) -> bool {
    let mut filter_levels = filter.split('/');
    let mut topic_levels = topic.split('/');
    loop {
        match (filter_levels.next(), topic_levels.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => continue,
            (Some(f), Some(t)) if f == t => continue,
            (None, None) => return true,
            _ => return false,
        }
    }
}

impl Broker for MqttBroker {
    fn name(&self) -> &'static str {
        "mqtt"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // Retire the previous pump generation — both the prompt
            // notify and the checked flag, so the exit survives a
            // notify landing between the old pump's await points.
            if let Some(previous) = self.shutdown.lock().await.take() {
                previous.stop();
            }
            let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;

            let (client, mut eventloop) = fresh_client(&self.mqtt_options);
            *self.client.lock().await = Some(client.clone());

            let stop = Arc::new(PumpStop::new());
            *self.shutdown.lock().await = Some(Arc::clone(&stop));

            let shared = Arc::clone(&self.shared);
            let generations = Arc::clone(&self.generation);
            let ack_mode = self.ack_mode;
            tokio::spawn(async move {
                let mut backoff_secs = 1u64;
                loop {
                    // The checked stop: read at every loop top, so a
                    // lost `notify_waiters` still retires this pump at
                    // its next iteration instead of resurrecting the
                    // broker through rumqttc's automatic reconnect.
                    if generations.load(Ordering::SeqCst) != generation
                        || stop.retired.load(Ordering::SeqCst)
                    {
                        return;
                    }
                    let reconnect = tokio::select! {
                        _ = stop.notify.notified() => return,
                        _ = shared.rearm.notified() => false,
                        result = eventloop.poll() => match result {
                            Ok(event) => {
                                match event {
                                    rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_)) => {
                                        // Re-arm every live subscription —
                                        // resubscribe on connect.
                                        let filters = shared
                                            .subscriptions
                                            .lock()
                                            .await
                                            .keys()
                                            .cloned()
                                            .collect::<Vec<_>>();
                                        for filter in filters {
                                            let _ = client
                                                .subscribe(&filter, rumqttc::QoS::AtLeastOnce)
                                                .await;
                                        }
                                        *shared.connected.lock().await = true;
                                    }
                                    rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) => {
                                        let topic = publish.topic.clone();
                                        let payload = publish.payload.to_vec();
                                        let handlers: Vec<Handler> = {
                                            let subs = shared.subscriptions.lock().await;
                                            subs.iter()
                                                .filter(|(filter, _)| {
                                                    topic_matches(filter, &topic)
                                                })
                                                .map(|(_, handler)| Arc::clone(handler))
                                                .collect()
                                        };
                                        match ack_mode {
                                            AckMode::Auto => {
                                                for handler in handlers {
                                                    let event = Event::new(
                                                        topic.clone(),
                                                        Message::from_payload(payload.clone()),
                                                    );
                                                    rushwind_broker::spawn_dispatch(event, Arc::clone(&handler));
                                                }
                                                // Settle on dispatch: the
                                                // PUBACK rides once, whatever
                                                // the handlers did.
                                                let _ = client.ack(&publish).await;
                                            }
                                            AckMode::Manual => {
                                                // One packet id admits one
                                                // PUBACK: the settle flag makes
                                                // the broadcast honest — the
                                                // first settle (ack or Drop)
                                                // sends it, later settles are
                                                // no-ops against a packet the
                                                // broker has already confirmed.
                                                let settled = Arc::new(AtomicBool::new(false));
                                                for handler in handlers {
                                                    // Each hook owns its own
                                                    // client/publish copies: the
                                                    // outer pair stays live for the
                                                    // rest of the pump loop.
                                                    let ack_client = client.clone();
                                                    let ack_publish = publish.clone();
                                                    let ack_gate = Arc::clone(&settled);
                                                    let nack_client = client.clone();
                                                    let nack_publish = publish.clone();
                                                    let nack_gate = Arc::clone(&settled);
                                                    let event = Event::settle_with(
                                                        topic.clone(),
                                                        Message::from_payload(payload.clone()),
                                                        move || {
                                                            Box::pin(async move {
                                                                if ack_gate.swap(true, Ordering::SeqCst) {
                                                                    return Ok(());
                                                                }
                                                                ack_client.ack(&ack_publish).await.map_err(
                                                                    |e| BrokerError::Failed(
                                                                        format!("mqtt ack: {e}"),
                                                                    ),
                                                                )
                                                            })
                                                                as BoxFuture<'static, Result<(), BrokerError>>
                                                        },
                                                        // MQTT 3.1.1 carries no NACK
                                                        // frame. Requeue's honest
                                                        // shape is *withholding* the
                                                        // PUBACK — the packet stays
                                                        // unacknowledged and the
                                                        // broker re-sends it on session
                                                        // resume. Drop takes the
                                                        // PUBACK: it is the only wire
                                                        // action that stops the
                                                        // re-sends.
                                                        move |action| {
                                                            Box::pin(async move {
                                                                if action == NackAction::Requeue {
                                                                    return Ok(());
                                                                }
                                                                if nack_gate.swap(true, Ordering::SeqCst) {
                                                                    return Ok(());
                                                                }
                                                                nack_client
                                                                    .ack(&nack_publish)
                                                                    .await
                                                                    .map_err(|e| BrokerError::Failed(
                                                                        format!("mqtt nack drop: {e}"),
                                                                    ))
                                                            })
                                                                as BoxFuture<'static, Result<(), BrokerError>>
                                                        },
                                                    );
                                                    rushwind_broker::spawn_dispatch(event, Arc::clone(&handler));
                                                }
                                            }
                                        }
                                    }
                                    rumqttc::Event::Incoming(rumqttc::Packet::UnsubAck(_)) => {
                                        // The UNSUBACK names no topic: pop the
                                        // in-flight queue's front — the server
                                        // acknowledges UNSUBSCRIBEs in the order
                                        // they were sent.
                                        shared.pending_unsubs.lock().await.pop_front();
                                        let generation = *shared.unsub_generation.borrow();
                                        shared.unsub_generation.send_replace(generation + 1);
                                    }
                                    _ => {}
                                }
                                false
                            }
                            Err(_) => {
                                *shared.connected.lock().await = false;
                                // The session dies with the connection
                                // (rumqttc defaults to a clean session), so
                                // in-flight unsubscribes are moot: release
                                // their waiters instead of leaving them for
                                // the confirmation timeout.
                                shared.pending_unsubs.lock().await.clear();
                                let generation = *shared.unsub_generation.borrow();
                                shared.unsub_generation.send_replace(generation + 1);
                                true
                            }
                        },
                    };
                    if reconnect {
                        // rumqttc's poll retries the connection on the
                        // next call; the backoff just spaces the polls.
                        // The sleep stays interruptible: a stop (or a
                        // fresh subscription's rearm) must not wait out
                        // a 30 s backoff step.
                        tokio::select! {
                            _ = stop.notify.notified() => return,
                            _ = shared.rearm.notified() => {}
                            _ = tokio::time::sleep(Duration::from_secs(backoff_secs)) => {}
                        }
                        backoff_secs = (backoff_secs * 2).min(30);
                    } else {
                        backoff_secs = 1;
                    }
                }
            });

            // The pump's first polls drive the connect; wait for the
            // CONNACK so callers can publish straight away.
            for _ in 0..100 {
                if *self.shared.connected.lock().await {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(BrokerError::Failed("mqtt connect timed out".to_string()))
        })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            *self.shared.connected.lock().await = false;
            // Enqueue the DISCONNECT packet while the pump can still
            // flush it, then retire the pump generation. Without the
            // retirement the eventloop would re-dial within a second and
            // re-arm every subscription on CONNACK, so a broker told to
            // shut down comes back to life and publish-after-disconnect
            // starts succeeding again — the same torn-down-then-alive
            // bug upstream pinned on kafka's writer map. rumqttc has no
            // disconnect token (the packet is best-effort); the session
            // dies with the socket either way, clean session being the
            // default.
            let enqueued = match self.client.lock().await.as_ref() {
                Some(client) => client.disconnect().await,
                None => Ok(()),
            };
            self.client.lock().await.take();
            // The checked stop is the load-bearing half: a
            // notify_waiters landing between the pump's await points
            // (subscription re-arm, delivery dispatch, ack flush)
            // registers no waiter and is lost — without the flag the
            // pump re-dials on rumqttc's automatic reconnect and
            // resurrects the broker that was asked to shut down.
            if let Some(previous) = self.shutdown.lock().await.take() {
                previous.stop();
            }
            enqueued.map_err(|e| BrokerError::Failed(format!("mqtt disconnect: {e}")))
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            let Some(client) = self.client.lock().await.clone() else {
                return Err(BrokerError::NotConnected);
            };
            if !*self.shared.connected.lock().await {
                return Err(BrokerError::NotConnected);
            }
            // Publishes are body-only at QoS 1 without
            // retain; headers and metadata have no MQTT 3.1.1 carrier.
            client
                .publish(topic, rumqttc::QoS::AtLeastOnce, false, message.payload)
                .await
                .map_err(|e| BrokerError::Failed(format!("mqtt publish: {e}")))
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            self.shared
                .subscriptions
                .lock()
                .await
                .insert(topic.to_string(), Arc::clone(&handler));
            if let (Some(client), true) = (
                self.client.lock().await.clone(),
                *self.shared.connected.lock().await,
            ) {
                if let Err(e) = client.subscribe(topic, rumqttc::QoS::AtLeastOnce).await {
                    // Failed registration cleanup: the wire subscribe was
                    // rejected, so an entry left behind would route
                    // deliveries — and reconnect re-arms — to a handler
                    // whose subscription never existed.
                    self.shared.subscriptions.lock().await.remove(topic);
                    return Err(BrokerError::Failed(format!("mqtt subscribe {topic}: {e}")));
                }
            }
            self.shared.rearm.notify_waiters();
            Ok(Box::new(MqttSubscriber {
                topic: topic.to_string(),
                client: self.client.lock().await.clone(),
                generation: self.generation.load(Ordering::SeqCst),
                shared: Arc::clone(&self.shared),
                handler: Arc::clone(&handler),
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The MQTT subscription handle.
struct MqttSubscriber {
    topic: String,
    client: Option<rumqttc::AsyncClient>,
    /// The pump generation this subscription armed on: a re-connect
    /// replaces the client, and the handle's clone belongs to a
    /// session the broker no longer holds.
    generation: u64,
    shared: Arc<Shared>,
    /// This handle's own handler — the identity the teardown checks
    /// against the registry, so a stale handle cannot strip the
    /// replacement that re-subscribed the same filter.
    handler: Handler,
}

impl MqttSubscriber {
    /// Whether this handle still owns the topic's registry entry — a
    /// duplicate subscribe replaces the entry, and a stale handle's
    /// teardown (explicit or on drop) must then touch nothing: removing
    /// the entry would orphan the replacement's deliveries, and the wire
    /// UNSUBSCRIBE would kill the live filter at the broker.
    async fn owns_registration(&self) -> bool {
        self.shared
            .subscriptions
            .lock()
            .await
            .get(&self.topic)
            .map(|current| Arc::ptr_eq(current, &self.handler))
            .unwrap_or(false)
    }
}

impl Subscriber for MqttSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            if !self.owns_registration().await {
                return Ok(());
            }
            self.shared.subscriptions.lock().await.remove(&self.topic);
            if self.generation != self.shared.generation.load(Ordering::SeqCst) {
                // The subscription armed on a retired generation: its
                // client is gone, and with the clean session the
                // broker-side subscription died with that connection —
                // nothing to unsubscribe, and pushing the request into
                // the stale handle would only fail (publish and
                // subscribe after disconnect must surface clear errors,
                // not resurrect stale handles).
                return Ok(());
            }
            if let (Some(client), true) = (self.client.clone(), *self.shared.connected.lock().await)
            {
                // The queue entry goes in under the same critical
                // section as the wire send. Two concurrent
                // unsubscribers could otherwise push in the opposite
                // order of their sends, and UNSUBACKs — which follow
                // the wire order — would confirm the wrong waiter,
                // reporting a teardown the broker has not confirmed.
                // Wait for the broker's UNSUBACK before reporting
                // success: the client call only queues the request, so
                // returning on enqueue alone would claim a teardown the
                // broker has not confirmed (upstream paho waited on the
                // unsubscribe token for the same reason).
                {
                    let mut pending = self.shared.pending_unsubs.lock().await;
                    pending.push_back(self.topic.clone());
                    if let Err(e) = client.unsubscribe(&self.topic).await {
                        pending.retain(|topic| topic != &self.topic);
                        return Err(BrokerError::Failed(format!(
                            "mqtt unsubscribe {}: {e}",
                            self.topic
                        )));
                    }
                }
                let mut generation = self.shared.unsub_generation.subscribe();
                let confirmed = async {
                    loop {
                        {
                            let pending = self.shared.pending_unsubs.lock().await;
                            if !pending.iter().any(|topic| topic == &self.topic) {
                                return;
                            }
                        }
                        // The watch buffers every bump, so a confirmation
                        // landing between the check above and this await
                        // still resolves immediately.
                        generation.changed().await.ok();
                    }
                };
                if tokio::time::timeout(UNSUB_CONFIRM_TIMEOUT, confirmed)
                    .await
                    .is_err()
                {
                    self.shared
                        .pending_unsubs
                        .lock()
                        .await
                        .retain(|topic| topic != &self.topic);
                    return Err(BrokerError::Failed(format!(
                        "mqtt unsubscribe {}: broker confirmation timed out",
                        self.topic
                    )));
                }
            }
            Ok(())
        })
    }
}

impl Drop for MqttSubscriber {
    fn drop(&mut self) {
        // Same ownership rule as `unsubscribe`: only the handle whose
        // handler is still the registry's current entry may remove it — a
        // stale handle's drop must not strip a replacement's registration.
        if let Ok(mut subscriptions) = self.shared.subscriptions.try_lock() {
            subscriptions.retain(|topic, current| {
                !(topic == &self.topic && Arc::ptr_eq(current, &self.handler))
            });
        }
    }
}

/// A test helper: pairs an unbounded event channel with a handler
/// that forwards every delivery into it.
pub fn event_channel() -> (
    tokio::sync::mpsc::UnboundedReceiver<Event>,
    impl Fn(Event) -> BoxFuture<'static, Result<(), BrokerError>> + Send + Sync,
) {
    let (tx, rx) = mpsc::unbounded_channel::<Event>();
    let forward = move |event: Event| {
        let tx = tx.clone();
        Box::pin(async move {
            let _ = tx.send(event);
            Ok::<(), BrokerError>(())
        }) as BoxFuture<'static, Result<(), BrokerError>>
    };
    (rx, forward)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> MqttBroker {
        MqttBroker::new(MqttOptions::new("127.0.0.1:1883"))
    }

    fn sink() -> Handler {
        let (_rx, forward) = event_channel();
        Arc::new(forward)
    }

    /// Publishing without a connection fails with NotConnected — a
    /// clear error, never a panic on a stale handle.
    #[tokio::test]
    async fn publish_before_connect_is_not_connected() {
        let broker = broker();
        let result = broker
            .publish("probe/never", Message::from_payload(vec![]))
            .await;
        assert!(
            matches!(result, Err(BrokerError::NotConnected)),
            "unconnected publish must fail with NotConnected, got {result:?}"
        );
    }

    /// Disconnect is idempotent: a double disconnect — including one
    /// before any connect — returns without panicking, and publishing
    /// afterwards still fails with NotConnected.
    #[tokio::test]
    async fn disconnect_is_idempotent() {
        let broker = broker();
        broker.disconnect().await.expect("first disconnect");
        broker.disconnect().await.expect("second disconnect");
        let result = broker
            .publish("probe/never", Message::from_payload(vec![]))
            .await;
        assert!(matches!(result, Err(BrokerError::NotConnected)));
    }

    /// Subscribing while disconnected defers the wire registration
    /// (armed on the next connect) and returns a usable handle; the
    /// handle's unsubscribe tears the deferred registration down
    /// without any wire traffic and without hanging.
    #[tokio::test]
    async fn subscribe_before_connect_defers_and_unsubscribes() {
        let broker = broker();
        let mut subscriber = broker
            .subscribe("probe/deferred", sink())
            .await
            .expect("offline subscribe defers the registration");
        assert_eq!(subscriber.topic(), "probe/deferred");
        assert_eq!(
            broker.shared.subscriptions.lock().await.len(),
            1,
            "the deferred registration is kept for the next connect"
        );
        subscriber
            .unsubscribe()
            .await
            .expect("offline unsubscribe removes the registration");
        assert!(
            broker.shared.subscriptions.lock().await.is_empty(),
            "unsubscribe tears the deferred registration down"
        );
    }

    /// Duplicate-topic subscribe replaces the single registration per
    /// filter instead of stacking a second one: the map has exactly
    /// one entry and it holds the newest handler. (Upstream paho kept
    /// one route per topic and its unsub-after-sub ordering deleted
    /// the NEW route; this engine's in-place replacement leaves no old
    /// delivery flow to orphan.)
    #[tokio::test]
    async fn duplicate_topic_subscribe_replaces_the_handler() {
        let broker = broker();
        // Both handles stay alive: dropping a handle removes its
        // registration best-effort, which would mask the replacement
        // under test.
        let _first = broker
            .subscribe("probe/dup", sink())
            .await
            .expect("first subscribe");
        let _second = broker
            .subscribe("probe/dup", sink())
            .await
            .expect("duplicate subscribe");
        let subscriptions = broker.shared.subscriptions.lock().await;
        assert_eq!(
            subscriptions.len(),
            1,
            "one registration per filter, not a stacked pair"
        );
        assert!(subscriptions.contains_key("probe/dup"));
    }

    /// A stale handle's teardown must not strip the replacement: after a
    /// duplicate subscribe, the first handle's explicit unsubscribe and
    /// its drop both leave the second handler registered — otherwise the
    /// topic's deliveries silently stop (the stale-handle hazard the
    /// sibling engines guard with the same ptr-eq identity check).
    #[tokio::test]
    async fn stale_handle_teardown_preserves_the_replacement() {
        let broker = broker();
        let mut first = broker
            .subscribe("probe/stale", sink())
            .await
            .expect("first subscribe");
        let mut second = broker
            .subscribe("probe/stale", sink())
            .await
            .expect("replacement subscribe");

        first
            .unsubscribe()
            .await
            .expect("stale unsubscribe reports success");
        {
            let subscriptions = broker.shared.subscriptions.lock().await;
            assert!(
                subscriptions.contains_key("probe/stale"),
                "a stale unsubscribe must not strip the replacement"
            );
        }

        drop(first);
        {
            let subscriptions = broker.shared.subscriptions.lock().await;
            assert!(
                subscriptions.contains_key("probe/stale"),
                "a stale handle's drop must not strip the replacement either"
            );
        }

        // The replacement is the owner: its teardown does clean up.
        second
            .unsubscribe()
            .await
            .expect("owner unsubscribe succeeds");
        {
            let subscriptions = broker.shared.subscriptions.lock().await;
            assert!(
                !subscriptions.contains_key("probe/stale"),
                "the owning handle's unsubscribe removes the registration"
            );
        }
    }

    /// The MQTT topic-filter matching table: literal segments, `+` one
    /// level, `#` any remaining levels.
    #[test]
    fn topic_filter_matching_follows_the_spec() {
        let cases: &[(&str, &str, bool)] = &[
            ("a/b/c", "a/b/c", true),
            ("a/+/c", "a/x/c", true),
            ("a/+/c", "a/x/y", false),
            ("a/#", "a/b/c", true),
            ("a/#", "a", true),
            ("#", "a/b", true),
            ("a/b", "a", false),
            ("a/b", "a/b/c", false),
        ];
        for (filter, topic, expected) in cases {
            assert_eq!(
                topic_matches(filter, topic),
                *expected,
                "filter {filter:?} topic {topic:?}"
            );
        }
    }
}
