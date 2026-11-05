//! MQTT consumer bridge for the RushWind lifecycle.
//!
//! [`MqttBridge`] subscribes to topics on an **external** broker (the
//! broker is infrastructure deployed out-of-process — the bridge never
//! embeds one) and pumps every received message into a handler,
//! under the lifecycle's [`Server`] contract.
//!
//! # Handler dispatch
//!
//! Handlers register per subscription
//! ([`BridgeBuilder::subscribe_with_handler`],
//! [`BridgeBuilder::subscribe_with_qos_handler`]); the default
//! handler ([`BridgeBuilder::session_handler`]) serves the
//! subscriptions without one. Dispatch follows MQTT topic-filter
//! matching (MQTT-3.3.2.3): `#` any tail including the parent, `+`
//! exactly one level, wildcards never matching `$`-prefixed topics.
//! When several filters match a delivery, the first registered one
//! wins. A delivery with neither a matching subscription nor a
//! default handler is dropped.
//!
//! # Shape
//!
//! This transport has **no session surface**: the bridge holds one
//! pre-trusted connection to the broker, whose credentials live in the
//! caller-supplied [`rumqttc::MqttOptions`] — there are no per-client
//! handshakes, so the session middleware chain (gates, admission policy)
//! has nothing to guard here. The security boundary is the broker
//! configuration, and message payloads are untrusted application input
//! (see `docs/threat-model.md`).
//!
//! # Connection semantics
//!
//! The invoked handler runs **serially** on the pump loop — one message at a
//! time, in broker delivery order. That is deliberate backpressure: a slow
//! handler slows delivery instead of growing an unbounded queue; QoS 1
//! deliveries queue at the broker.
//!
//! Connection loss is not an error: the pump reconnects with a capped
//! exponential backoff, re-establishing every registered subscription on
//! each generation, forever — a broker restart must not end the
//! application. The stop signal always wins the race; when it fires, the
//! bridge returns `Cancelled` and the connection drops with the pump
//! future.
//!
//! # Restart
//!
//! Registration parameters are frozen in the bridge, and every start
//! (re)establishes all of them on a fresh connection generation — so
//! stop-then-start genuinely resubscribes, and there is no registry
//! to hand off between the two: no lock is ever held across a broker
//! call, and a lifecycle cancelled mid-registration leaves nothing
//! behind (the half-built connection drops with the start future and
//! its subscriptions die with the session).

//! `Server::stop` is a no-op (the connection lives inside the start
//! future and drops with it — the axum-style rule-1 case of the
//! architecture document).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rumqttc::{AsyncClient, Incoming, MqttOptions};
use rushwind_transport::{Server, ServerError, ServerFuture, StopSignal};

/// Reconnect backoff floor.
const BACKOFF_BASE: Duration = Duration::from_millis(100);
/// Reconnect backoff ceiling.
const BACKOFF_CAP: Duration = Duration::from_secs(5);

/// One message delivered by the broker.
pub struct MqttMessage {
    /// The topic the message was published on.
    pub topic: String,
    /// The message payload bytes. Untrusted application input.
    pub payload: Vec<u8>,
}

/// Session-handler function type: receives each delivered message.
type MessageHandlerFn = Box<dyn Fn(MqttMessage) -> MessageFuture + Send + Sync>;

/// Message-handler future type.
type MessageFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// One registered subscription: its topic filter, its QoS, and —
/// when registered with [`BridgeBuilder::subscribe_with_handler`] —
/// the handler bound to it.
struct Subscription {
    /// The topic filter registered with the broker.
    filter: String,
    /// The QoS the filter was registered with.
    qos: rumqttc::QoS,
    /// The handler bound to this subscription, if one was.
    handler: Option<MessageHandlerFn>,
}

/// MQTT topic-filter matching (MQTT-3.3.2.3): `#` matches any tail
/// including the filter's parent level, `+` exactly one level,
/// everything else literal. Filters starting with a wildcard never
/// match `$`-prefixed topics.
fn topic_matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter.starts_with('#') || filter.starts_with('+')) {
        return false;
    }
    let mut filter_levels = filter.split('/');
    let mut topic_levels = topic.split('/');
    loop {
        match (filter_levels.next(), topic_levels.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(filter_level), Some(topic_level)) => {
                if filter_level != topic_level {
                    return false;
                }
            }
            (None, None) => return true,
            (Some(_), None) | (None, Some(_)) => return false,
        }
    }
}

/// Shared pump state, frozen at build time.
struct BridgeState {
    broker: SocketAddr,
    client_id: String,
    subscriptions: Vec<Subscription>,
    default_handler: Option<MessageHandlerFn>,
}

/// A bridge from an external MQTT broker into handlers, under the
/// RushWind lifecycle.
pub struct MqttBridge {
    state: Arc<BridgeState>,
}

impl MqttBridge {
    /// Starts building a bridge to the broker at `broker`, identifying
    /// itself to it as `client_id`.
    pub fn builder(broker: SocketAddr, client_id: impl Into<String>) -> BridgeBuilder {
        BridgeBuilder {
            broker,
            client_id: client_id.into(),
            subscriptions: Vec::new(),
            default_handler: None,
        }
    }
}

/// Builder for [`MqttBridge`].
pub struct BridgeBuilder {
    broker: SocketAddr,
    client_id: String,
    subscriptions: Vec<Subscription>,
    default_handler: Option<MessageHandlerFn>,
}

impl BridgeBuilder {
    /// Subscribes to `topic` at QoS 1 (at-least-once delivery); its
    /// deliveries go to the default handler when one is registered.
    pub fn subscribe(self, topic: impl Into<String>) -> Self {
        self.subscribe_with_qos(topic, rumqttc::QoS::AtLeastOnce)
    }

    /// Subscribes to `topic` at an explicit QoS; its deliveries go to
    /// the default handler when one is registered.
    pub fn subscribe_with_qos(mut self, topic: impl Into<String>, qos: rumqttc::QoS) -> Self {
        self.subscriptions.push(Subscription {
            filter: topic.into(),
            qos,
            handler: None,
        });
        self
    }

    /// Subscribes to `topic` at QoS 1 with a handler bound to this
    /// subscription: its deliveries — those the broker matches to
    /// this filter — go to this handler, never the default.
    pub fn subscribe_with_handler<F, Fut>(self, topic: impl Into<String>, handler: F) -> Self
    where
        F: Fn(MqttMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.subscribe_with_qos_handler(topic, rumqttc::QoS::AtLeastOnce, handler)
    }

    /// Subscribes to `topic` at an explicit QoS with a handler bound
    /// to this subscription.
    pub fn subscribe_with_qos_handler<F, Fut>(
        mut self,
        topic: impl Into<String>,
        qos: rumqttc::QoS,
        handler: F,
    ) -> Self
    where
        F: Fn(MqttMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.subscriptions.push(Subscription {
            filter: topic.into(),
            qos,
            handler: Some(Box::new(move |message: MqttMessage| {
                Box::pin(handler(message)) as MessageFuture
            })),
        });
        self
    }

    /// Sets the default handler invoked for deliveries on
    /// subscriptions without a bound handler. The handler runs until
    /// it returns or the lifecycle's stop signal fires — whichever
    /// comes first.
    pub fn session_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(MqttMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.default_handler = Some(Box::new(move |message: MqttMessage| {
            Box::pin(handler(message)) as MessageFuture
        }));
        self
    }

    /// Freezes the builder into an [`MqttBridge`].
    ///
    /// Errors when a subscription has neither a bound handler nor a
    /// default handler to receive its deliveries.
    pub fn build(self) -> Result<MqttBridge, ServerError> {
        if self
            .subscriptions
            .iter()
            .any(|subscription| subscription.handler.is_none())
            && self.default_handler.is_none()
        {
            return Err(ServerError::Failed(
                "mqtt bridge subscription has no handler".to_string(),
            ));
        }
        Ok(MqttBridge {
            state: Arc::new(BridgeState {
                broker: self.broker,
                client_id: self.client_id,
                subscriptions: self.subscriptions,
                default_handler: self.default_handler,
            }),
        })
    }
}

impl Server for MqttBridge {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(format!("mqtt://{}", self.state.broker))
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let mut backoff = BACKOFF_BASE;
            loop {
                // Each generation is a fresh client/eventloop pair;
                // subscriptions are re-established per generation so a
                // broker restart cannot silently drop them.
                let options = MqttOptions::new(
                    state.client_id.clone(),
                    state.broker.ip().to_string(),
                    state.broker.port(),
                );
                let (client, mut eventloop) = AsyncClient::new(options, 16);
                for subscription in &state.subscriptions {
                    let _ = client
                        .subscribe(&subscription.filter, subscription.qos)
                        .await;
                }

                let mut connected = false;
                loop {
                    let event = tokio::select! {
                        _ = stop.wait() => return Err(ServerError::Cancelled),
                        polled = eventloop.poll() => match polled {
                            Ok(event) => event,
                            Err(_connection_error) => break,
                        },
                    };
                    match event {
                        rumqttc::Event::Incoming(Incoming::ConnAck(_)) => {
                            // A real connection existed: the next failure
                            // starts from the base backoff again.
                            connected = true;
                            backoff = BACKOFF_BASE;
                        }
                        rumqttc::Event::Incoming(Incoming::Publish(publish)) => {
                            // Dispatch by MQTT topic-filter matching:
                            // the first subscription whose filter
                            // matches, its bound handler or the
                            // default; a delivery with neither is
                            // dropped.
                            let handler = state
                                .subscriptions
                                .iter()
                                .find(|subscription| {
                                    topic_matches(&subscription.filter, &publish.topic)
                                })
                                .and_then(|subscription| {
                                    subscription
                                        .handler
                                        .as_ref()
                                        .or(state.default_handler.as_ref())
                                })
                                .or_else(|| state.default_handler.as_ref());
                            if let Some(handler) = handler {
                                (handler)(MqttMessage {
                                    topic: publish.topic,
                                    payload: publish.payload.to_vec(),
                                })
                                .await;
                            }
                        }
                        _ => {}
                    }
                }

                // Connection lost (or never established): back off with
                // the stop signal always winning the race.
                if !connected {
                    backoff = (backoff * 2).min(BACKOFF_CAP);
                }
                tokio::select! {
                    _ = stop.wait() => return Err(ServerError::Cancelled),
                    _ = tokio::time::sleep(backoff) => {}
                }
            }
        })
    }

    fn stop(&self) -> ServerFuture<'_> {
        // The broker connection lives inside the start future and drops
        // with it; the bridge holds no long-lived resource of its own.
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::topic_matches;

    /// The MQTT-3.3.2.3 topic-filter matching table: literal
    /// segments, `+` one level, `#` any tail including the parent,
    /// wildcard filters never matching `$`-prefixed topics.
    #[test]
    fn topic_filter_matching_follows_the_spec() {
        let cases: &[(&str, &str, bool)] = &[
            ("a/b/c", "a/b/c", true),
            ("a/b/c", "a/b/d", false),
            ("a/+/c", "a/x/c", true),
            ("a/+/c", "a/x/y", false),
            ("a/+/c", "a/x", false),
            ("a/#", "a/b/c", true),
            ("a/#", "a", true),
            ("#", "a/b", true),
            ("+/+", "a/b", true),
            ("+/+", "a", false),
            ("$SYS/#", "$SYS/x", true),
            ("#", "$SYS/x", false),
            ("+/x", "$SYS/x", false),
            ("a", "a/b", false),
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
