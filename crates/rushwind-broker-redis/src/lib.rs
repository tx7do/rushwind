//! Redis engine for the RushWind broker, pub-sub mode over the `redis`
//! crate.
//!
//! # The wire behavior
//!
//! Publishes are `PUBLISH
//! topic payload` — payload-only; headers and metadata have no Redis
//! carrier. Each subscription owns a dedicated connection in
//! SUBSCRIBE mode (Redis turns a subscribed connection into a
//! push-only channel), reading `message` events and routing them to
//! the handler. Exact topics only — subscriptions use no patterns,
//! so there is no wildcard matching, unlike the MQTT
//! engine.
//!
//! Redis pub/sub has no acknowledgment and no persistence: a
//! delivery is at-most-once, the event's ack is a no-op, and
//! unsubscribing drops the connection — after which the topic's
//! messages are simply never seen; the subscription's connection
//! is closed with it.
//!
//! # Connection lifecycle
//!
//! Connections are lazy: publishes ride short-lived multiplexed
//! handles and subscriptions open their dedicated connection on
//! subscribe. [`Broker::disconnect`] flips a gate — afterwards
//! publishes and subscribes return [`BrokerError::NotConnected`] and
//! live subscriptions are torn down (the upstream Go pool went nil
//! after Disconnect); a second disconnect is a no-op, and
//! [`Broker::connect`] clears the gate again.
//!
//! Subscribing to a topic that already has a live subscription
//! retires the previous flow first — the old connection is closed
//! before the new reader starts, so a topic never has two consumers
//! (the upstream Go registry unsubscribes the old subscriber on
//! duplicate subscribe).
//!
//! # Authentication
//!
//! The password rides the URL's userinfo
//! (`redis://:secret@host:6379` — URL-escape special characters) or
//! the settings' [`RedisSettings::password`] field, which overrides
//! whatever the URL embeds.
//!
//! # Divergences
//!
//! - Publishes ride a shared multiplexed connection.
//! - A Redis Streams mode (consumer groups) is
//!   not implemented — it is a different delivery contract
//!   (at-least-once, acknowledgments, replay) and would be its own
//!   engine.
//! - No automatic reconnection: a dropped subscription connection
//!   ends its delivery flow; the caller re-subscribes.
//!
//! # Testing
//!
//! Live conformance tests run against a real Redis via the `live`
//! feature; there is no embedded Redis for CI's unit lanes. The
//! offline unit suite pins the settings shape, the password wiring,
//! and the disconnect gate.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Handler, Message, Subscriber};

use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

/// A subscription's shared teardown state. The registry keeps one per
/// topic so a duplicate subscribe can retire the previous delivery
/// flow — cancel the reader and wait for it to drop the connection —
/// even though the old [`RedisSubscriber`] handle still lives in the
/// caller's hands.
struct SubscriptionShared {
    /// Cancels the reader task; the subscribed connection drops with
    /// it.
    done: CancellationToken,
    /// The reader task, taken and awaited by whichever teardown runs
    /// first (unsubscribe or duplicate-subscribe retirement).
    reader: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

struct Inner {
    client: redis::Client,
    /// Stands after [`Broker::disconnect`]: publishes and subscribes
    /// refuse. The upstream Go broker kept dereferencing a nil pool
    /// after Disconnect; here the gate turns that into a clear
    /// `NotConnected` error.
    disconnected: AtomicBool,
    /// Subscribed topics and their teardown state, keyed by topic.
    subscriptions: Mutex<HashMap<String, Arc<SubscriptionShared>>>,
}

/// A Redis pub/sub-backed broker.
pub struct RedisBroker {
    inner: Arc<Inner>,
}

/// The bootstrap factory's settings wire shape for
/// [`RedisBroker::from_settings`].
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RedisSettings {
    /// The Redis connection URL.
    pub url: String,
    /// The AUTH password. Optional; when set it overrides any
    /// password embedded in the URL's userinfo.
    #[serde(default)]
    pub password: Option<String>,
}

impl RedisSettings {
    /// Settings for `url` with no explicit password.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            password: None,
        }
    }

    /// Sets the AUTH password. Every builder field accumulates —
    /// setting one never resets the others (the upstream Go options
    /// reset the whole struct to zero values on the first `With*`).
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }
}

/// Builds the redis client configuration: the URL (which may embed
/// username and password in its userinfo) with an explicit password
/// override. The override wins, mirroring the upstream
/// `WithPassword` → `redis.DialPassword` wiring.
fn connection_info(
    url: &str,
    password: Option<&str>,
) -> Result<redis::ConnectionInfo, BrokerError> {
    use redis::IntoConnectionInfo;
    let mut info = url
        .into_connection_info()
        .map_err(|e| BrokerError::Failed(format!("redis client open: {e}")))?;
    if let Some(password) = password {
        let handshake = info.redis_settings().clone().set_password(password);
        info = info.set_redis_settings(handshake);
    }
    Ok(info)
}

impl RedisBroker {
    /// Connects to Redis at `url` (e.g. `redis://127.0.0.1:6379`).
    ///
    /// The password may ride the URL's userinfo
    /// (`redis://:secret@host:6379`; escape `@ : / # ?` characters) —
    /// see [`RedisSettings::password`] for the explicit form.
    pub fn connect(url: &str) -> Result<Self, BrokerError> {
        Self::with_connection_info(connection_info(url, None)?)
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `url` (required), `password` (optional).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: RedisSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::with_connection_info(connection_info(
            &settings.url,
            settings.password.as_deref(),
        )?)
    }

    fn with_connection_info(info: redis::ConnectionInfo) -> Result<Self, BrokerError> {
        let client = redis::Client::open(info)
            .map_err(|e| BrokerError::Failed(format!("redis client open: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                disconnected: AtomicBool::new(false),
                subscriptions: Mutex::new(HashMap::new()),
            }),
        })
    }
}

impl Broker for RedisBroker {
    fn name(&self) -> &'static str {
        "redis"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        // The pub-sub engine connects lazily per publish and per
        // subscription; this call re-opens the gate a disconnect
        // closed. The upstream Go Connect rebuilds the pool — the
        // address rides the client here, so there is nothing else to
        // re-derive.
        Box::pin(async move {
            self.inner.disconnected.store(false, Ordering::SeqCst);
            Ok(())
        })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // Idempotent: a second disconnect is a no-op (the upstream
            // Go Disconnect dereferenced a pool that was already nil).
            if self.inner.disconnected.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            // Live subscriptions die with the gate: their dedicated
            // connections are of no use afterwards. Cancellation ends
            // each reader and drops its connection; the caller-held
            // subscriber handles unwind on their next call.
            let subscriptions: Vec<Arc<SubscriptionShared>> = {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                subscriptions.drain().map(|(_, entry)| entry).collect()
            };
            for subscription in subscriptions {
                subscription.done.cancel();
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
            if self.inner.disconnected.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            // Publishes are payload-only, through PUBLISH.
            let mut connection = self
                .inner
                .client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| BrokerError::Failed(format!("redis connect: {e}")))?;
            let (): () = redis::AsyncCommands::publish(&mut connection, topic, message.payload)
                .await
                .map_err(|e| BrokerError::Failed(format!("redis publish: {e}")))?;
            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            if self.inner.disconnected.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            // A subscribed Redis connection is push-only: each
            // subscription owns one. The connection is opened before
            // the registry is touched — a failure here leaves the
            // previous subscription untouched.
            let mut pubsub = self
                .inner
                .client
                .get_async_pubsub()
                .await
                .map_err(|e| BrokerError::Failed(format!("redis subscribe {topic}: {e}")))?;
            pubsub
                .subscribe(topic)
                .await
                .map_err(|e| BrokerError::Failed(format!("redis subscribe {topic}: {e}")))?;

            let done = CancellationToken::new();
            let shared = Arc::new(SubscriptionShared {
                done: done.clone(),
                reader: RwLock::new(None),
            });

            // Duplicate subscribe retires the previous flow before the
            // new reader starts: both connections would otherwise
            // receive every publish and the topic would have two
            // consumers. The retirement waits for the old reader, so
            // once this subscribe returns the old connection is gone.
            let reader = {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                if let Some(previous) = subscriptions.remove(topic) {
                    previous.done.cancel();
                    if let Some(previous_reader) = previous.reader.write().await.take() {
                        let _ = previous_reader.await;
                    }
                }
                let task_done = done.clone();
                let topic_for_task = topic.to_string();
                let reader = tokio::spawn(async move {
                    let mut stream = pubsub.into_on_message();
                    loop {
                        let message = tokio::select! {
                            _ = task_done.cancelled() => return,
                            message = stream.next() => match message {
                                Some(message) => message,
                                None => return,
                            },
                        };
                        let payload = message.get_payload_bytes().to_vec();
                        let event =
                            Event::new(topic_for_task.clone(), Message::from_payload(payload));
                        // Fire-and-forget dispatch: a handler error or
                        // panic cannot kill the reader loop — each
                        // delivery lives in its own task.
                        tokio::spawn(handler(event));
                    }
                });
                *shared.reader.write().await = Some(reader);
                subscriptions.insert(topic.to_string(), Arc::clone(&shared));
                Arc::clone(&shared)
            };

            Ok(Box::new(RedisSubscriber {
                topic: topic.to_string(),
                shared: reader,
                inner: Arc::clone(&self.inner),
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The Redis subscription handle.
struct RedisSubscriber {
    topic: String,
    shared: Arc<SubscriptionShared>,
    inner: Arc<Inner>,
}

impl Subscriber for RedisSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // Remove the registry entry only while this subscription
            // still owns it — a duplicate subscribe replaced it, and
            // that newer flow must not be deregistered here.
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
            // Cancel ends the reader task; awaiting it closes the
            // subscribed connection before returning.
            self.shared.done.cancel();
            if let Some(reader) = self.shared.reader.write().await.take() {
                let _ = reader.await;
            }
            Ok(())
        })
    }
}

impl Drop for RedisSubscriber {
    fn drop(&mut self) {
        // Same teardown as unsubscribe, best-effort when the explicit
        // path was skipped.
        self.shared.done.cancel();
        if let Ok(mut reader) = self.shared.reader.try_write() {
            if let Some(reader) = reader.take() {
                reader.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> RedisBroker {
        RedisBroker::connect("redis://127.0.0.1:6379").expect("settings parse")
    }

    /// The settings wire shape parses `url` alone; the password field
    /// defaults to absent, so older configuration documents keep
    /// working.
    #[test]
    fn settings_default_password_is_absent() {
        let settings: RedisSettings =
            serde_json::from_value(serde_json::json!({ "url": "redis://h:1" }))
                .expect("settings parse");
        assert_eq!(settings.url, "redis://h:1");
        assert_eq!(settings.password, None);
    }

    /// An explicit settings password parses.
    #[test]
    fn settings_parse_password() {
        let settings: RedisSettings = serde_json::from_value(serde_json::json!({
            "url": "redis://h:1",
            "password": "secret",
        }))
        .expect("settings parse");
        assert_eq!(settings.password.as_deref(), Some("secret"));
    }

    /// The builder accumulates: setting the password must not reset
    /// the URL (the upstream Go options reset the whole struct to
    /// zero values on the first `With*`).
    #[test]
    fn settings_builder_accumulates() {
        let settings = RedisSettings::new("redis://h:1").with_password("secret");
        assert_eq!(settings.url, "redis://h:1");
        assert_eq!(settings.password.as_deref(), Some("secret"));
    }

    /// An explicit password override reaches the client configuration
    /// while the URL's own userinfo (user, database) is preserved —
    /// the override wins, mirroring `redis.DialPassword`.
    #[test]
    fn password_override_reaches_the_client_config() {
        let info =
            connection_info("redis://alice:embedded@h:6379/2", Some("override")).expect("info");
        assert_eq!(info.redis_settings().password(), Some("override"));
        assert_eq!(info.redis_settings().username(), Some("alice"));
        assert_eq!(info.redis_settings().db(), 2);
    }

    /// Without an override, a URL-embedded password flows through —
    /// the documented `redis://:secret@host` form needs no settings.
    #[test]
    fn url_embedded_password_flows_through() {
        let info = connection_info("redis://:embedded@h:6379", None).expect("info");
        assert_eq!(info.redis_settings().password(), Some("embedded"));
        assert_eq!(info.redis_settings().username(), None);
    }

    /// After a disconnect, publish returns the not-connected error —
    /// the clear failure the upstream Go broker lacked when it
    /// dereferenced the nil pool.
    #[tokio::test]
    async fn publish_after_disconnect_is_not_connected() {
        let broker = broker();
        broker.disconnect().await.expect("disconnect");
        let result = broker
            .publish("probe/off", Message::from_payload(b"x".to_vec()))
            .await;
        assert!(
            matches!(result, Err(BrokerError::NotConnected)),
            "publish after disconnect must be NotConnected, got {result:?}"
        );
    }

    /// After a disconnect, subscribe returns the not-connected error
    /// too.
    #[tokio::test]
    async fn subscribe_after_disconnect_is_not_connected() {
        let broker = broker();
        broker.disconnect().await.expect("disconnect");
        let result = broker
            .subscribe(
                "probe/off",
                Arc::new(|_event| {
                    Box::pin(async { Ok(()) }) as BoxFuture<'static, Result<(), BrokerError>>
                }),
            )
            .await
            .map(|_| ());
        assert!(
            matches!(result, Err(BrokerError::NotConnected)),
            "subscribe after disconnect must be NotConnected, got {result:?}"
        );
    }

    /// A second disconnect is a no-op, and `connect` re-opens the
    /// gate a disconnect closed.
    #[tokio::test]
    async fn disconnect_is_idempotent_and_connect_restores() {
        let broker = broker();
        broker.disconnect().await.expect("first disconnect");
        broker.disconnect().await.expect("second disconnect");
        broker.connect().await.expect("re-connect");
        broker
            .disconnect()
            .await
            .expect("disconnect after reconnect");
    }
}
