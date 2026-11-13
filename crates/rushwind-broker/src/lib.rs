//! Message broker contract for RushWind: the [`Broker`] interface with
//! its connect/publish/subscribe/request lifecycle, the [`Message`]
//! wire shape, the delivery [`Event`], and the [`Subscriber`] handle.
//!
//! # The core shapes
//!
//! The message travels as bytes ([`Message::payload`]) and
//! applications encode and decode at the edges with serde — no
//! dynamically typed body plus codec machinery in between.
//! [`json_handler`] is the typed-helper equivalent — it wraps a typed
//! closure into a [`Handler`] that JSON-decodes the payload first,
//! and [`json_message`] builds a JSON-encoded [`Message`].
//!
//! [`Event::ack`] acknowledges a delivery: engines whose deliveries
//! need acknowledging (Kafka offsets, RabbitMQ acks) wire the native
//! ack into the event; engines with implicit acknowledgment (MQTT)
//! leave it a no-op. [`Event::nack`] is the rejection half — the face
//! poison-message routing needs: an engine whose native delivery
//! supports negative acknowledgment wires a [`Nacker`] into the event
//! and [`NackAction`] asks requeue or drop; implicit-ack engines
//! leave it the documented no-op (the delivery is already settled —
//! there is nothing to hand back). Today every engine runs implicit
//! acknowledgment; the nacker face is wired per engine as each grows
//! a manual-ack mode.
//!
//! [`with_error_handler`] is the error-handler hook: it wraps a
//! [`Handler`] so handler errors reach an [`ErrorHandler`] observation
//! point — the upstream `ErrorHandler` reshaped into the contract's
//! composition style — without changing the delivery loops' own
//! guarantee (a handler error never kills the subscription). Message
//! metadata narrows to string values — the
//! native broker metadata surfaces are string-typed. A native-message
//! handle slot has no equivalent: engines expose what their delivery
//! actually needs through the event.
//!
//! [`Broker::request`] carries a default implementation returning the
//! not-implemented error — the shape every engine without a native
//! request-reply surface inherits — which engines with one (NATS)
//! override.
//!
//! # Middleware
//!
//! The [`PublishMiddleware`]/[`SubscriberMiddleware`] chains ride on
//! [`MiddlewareBroker`], a decorator that runs the chains around
//! another broker. The chain semantics are fixed: middlewares apply
//! backward, so the first-registered middleware is the outermost
//! wrapper and runs first.
//!
//! # Engines
//!
//! Engines live in `rushwind-broker-*` crates (`mqtt`, `nats`, ...)
//! and implement [`Broker`] over their native client. Per-call
//! publish/subscribe options narrow to what the engine can
//! actually honor; engine-specific knobs live on the engine's
//! constructor options, not per call.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Future type used across the broker contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors surfaced by broker engines.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BrokerError {
    /// The engine could not complete the operation.
    Failed(String),
    /// The broker connection is not established.
    NotConnected,
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "broker operation failed: {msg}"),
            Self::NotConnected => write!(f, "broker not connected"),
        }
    }
}

impl std::error::Error for BrokerError {}

/// The message wire shape, with the payload as bytes.
#[derive(Debug, Clone, Default)]
pub struct Message {
    /// Message id. Engines generate one when the native message
    /// carries none.
    pub id: String,
    /// String headers carried with the message.
    pub headers: HashMap<String, String>,
    /// The payload bytes.
    pub payload: Vec<u8>,
    /// The ordering key — Kafka's key, RabbitMQ's routing key.
    pub key: String,
    /// Additional string metadata about the message.
    pub metadata: HashMap<String, String>,
    /// The partition the message was read from (Kafka), when known.
    pub partition: Option<i64>,
    /// The offset of the message within its partition (Kafka), when
    /// known.
    pub offset: Option<i64>,
}

impl Message {
    /// Builds a message from a payload.
    pub fn from_payload(payload: Vec<u8>) -> Self {
        Self {
            payload,
            ..Message::default()
        }
    }

    /// Sets a header, replacing any previous value.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Reads a header, defaulting to empty.
    pub fn header(&self, key: &str) -> &str {
        self.headers
            .get(key)
            .map(|value| value.as_str())
            .unwrap_or("")
    }

    /// Sets a metadata entry, replacing any previous value.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// The acknowledgment hook an engine wires into an event when the
/// native delivery needs one.
pub type Acker = Box<dyn FnOnce() -> BoxFuture<'static, Result<(), BrokerError>> + Send>;

/// The acknowledgment policy a subscription runs under — the second
/// poison-message prerequisite the upstream asked the contract to
/// carry. `Auto` (every engine's default) acknowledges the delivery
/// the moment it is dispatched: handler errors cannot redeliver, and
/// [`Event::nack`] is a documented no-op. `Manual` hands settlement to
/// the handler — the [`Event::ack`]/
/// [`Event::nack`](Event::nack) hooks carry the engine's native
/// settlement, so a handler that errors can requeue or drop the
/// poison message. Engines whose native delivery has no
/// acknowledgment surface (core NATS, Redis pub/sub) stay `Auto`
/// structurally: their options accept the mode and document that
/// manual settlement has nothing to settle onto.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckMode {
    /// Settle on dispatch — the historical behavior, every engine's
    /// default.
    #[default]
    Auto,
    /// Settle by the handler, through the event's ack/nack hooks.
    Manual,
}

/// What a rejection asks the engine to do with the delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NackAction {
    /// Hand the delivery back to the broker for redelivery — to this
    /// or another consumer, per the broker's native redelivery rules.
    Requeue,
    /// Drop the delivery — the poison-message terminus. Dead-letter
    /// routing is the deployment's business, via the broker's own
    /// DLX/exchange configuration.
    Drop,
}

/// The rejection hook an engine wires into an event when the native
/// delivery supports negative acknowledgment.
pub type Nacker = Box<dyn FnOnce(NackAction) -> BoxFuture<'static, Result<(), BrokerError>> + Send>;

/// One delivery: the subscribed topic and the message, with the
/// engine's acknowledgment (and rejection, where supported) wired in.
pub struct Event {
    topic: String,
    message: Message,
    acker: Option<Acker>,
    nacker: Option<Nacker>,
}

impl Event {
    /// Builds an event without an acknowledgment hook — the shape of
    /// every engine whose deliveries acknowledge implicitly.
    pub fn new(topic: impl Into<String>, message: Message) -> Self {
        Self {
            topic: topic.into(),
            message,
            acker: None,
            nacker: None,
        }
    }

    /// Builds an event with an acknowledgment hook.
    pub fn with_acker(
        topic: impl Into<String>,
        message: Message,
        acker: impl FnOnce() -> BoxFuture<'static, Result<(), BrokerError>> + Send + 'static,
    ) -> Self {
        Self {
            topic: topic.into(),
            message,
            acker: Some(Box::new(acker)),
            nacker: None,
        }
    }

    /// Builds an event carrying **both** settlement hooks — the
    /// manual-ack shape: the handler settles by outcome (ack on
    /// success, nack with an [`NackAction`] on poison). Exactly one
    /// settlement fires; whichever hook the handler consumes is the
    /// one the delivery settles through.
    pub fn settle_with(
        topic: impl Into<String>,
        message: Message,
        acker: impl FnOnce() -> BoxFuture<'static, Result<(), BrokerError>> + Send + 'static,
        nacker: impl FnOnce(NackAction) -> BoxFuture<'static, Result<(), BrokerError>> + Send + 'static,
    ) -> Self {
        Self {
            topic: topic.into(),
            message,
            acker: Some(Box::new(acker)),
            nacker: Some(Box::new(nacker)),
        }
    }

    /// Builds an event with a rejection hook — the shape of an engine
    /// whose native delivery supports negative acknowledgment.
    pub fn with_nacker(
        topic: impl Into<String>,
        message: Message,
        nacker: impl FnOnce(NackAction) -> BoxFuture<'static, Result<(), BrokerError>> + Send + 'static,
    ) -> Self {
        Self {
            topic: topic.into(),
            message,
            acker: None,
            nacker: Some(Box::new(nacker)),
        }
    }

    /// The topic the message arrived on.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The delivered message.
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// Acknowledges the delivery. A no-op for engines with implicit
    /// acknowledgment, such as MQTT.
    pub async fn ack(self) -> Result<(), BrokerError> {
        match self.acker {
            Some(acker) => acker().await,
            None => Ok(()),
        }
    }

    /// Rejects the delivery with `action` — the poison-message face.
    /// A no-op for engines with implicit acknowledgment: the delivery
    /// is already settled, there is nothing to hand back (the error
    /// handler, not the nacker, is the observation point there).
    pub async fn nack(self, action: NackAction) -> Result<(), BrokerError> {
        match self.nacker {
            Some(nacker) => nacker(action).await,
            None => Ok(()),
        }
    }
}

/// The handler invoked per delivery; cancellation rides the future
/// instead of a context parameter.
pub type Handler = Arc<dyn Fn(Event) -> BoxFuture<'static, Result<(), BrokerError>> + Send + Sync>;

/// The error-observation point [`with_error_handler`] runs when a
/// handler errors: the delivered message and the handler's error.
/// Observation only — the delivery loop's own settlement semantics
/// (ack/nack per the engine, the never-kill-the-subscription
/// guarantee) stay the engine's.
pub type ErrorHandler = Arc<dyn Fn(Message, BrokerError) -> BoxFuture<'static, ()> + Send + Sync>;

/// Wraps a handler with an error handler: every error the inner
/// handler returns reaches `error_handler` (the delivered message and
/// the error) *and* still surfaces as the wrapper's `Err` — the
/// observation point never swallows the outcome, and the engine's
/// delivery loop keeps owning settlement. Errors the error handler
/// itself raises while observing are its business; the hook returns
/// `()` by shape.
pub fn with_error_handler(handler: Handler, error_handler: ErrorHandler) -> Handler {
    Arc::new(move |event: Event| {
        let handler = Arc::clone(&handler);
        let error_handler = Arc::clone(&error_handler);
        Box::pin(async move {
            let message = event.message().clone();
            let result = handler(event).await;
            if let Err(error) = &result {
                error_handler(message, error.clone()).await;
            }
            result
        }) as BoxFuture<'static, Result<(), BrokerError>>
    })
}

/// The subscription handle.
pub trait Subscriber: Send {
    /// The subscribed topic.
    fn topic(&self) -> &str;

    /// Unsubscribes and releases the engine-side subscription.
    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>>;
}

/// The broker interface. Engines must be
/// callable through shared references (`&self`).
pub trait Broker: Send + Sync {
    /// The engine name.
    fn name(&self) -> &'static str;

    /// Establishes the broker connection.
    /// Idempotent engines may treat repeated calls as no-ops.
    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>>;

    /// Tears the connection down.
    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>>;

    /// Publishes a message to a topic.
    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>>;

    /// Subscribes a handler to a topic.
    /// Topic filters follow the engine's native syntax (MQTT's `+`/`#`
    /// wildcards, NATS's `*`/`>`, Kafka's literal topics).
    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>>;

    /// Sends a request and awaits a response. The default is a stub:
    /// every engine without a native request-reply surface returns
    /// the not-implemented error; engines with one override this.
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

/// The publish-call surface a [`PublishMiddleware`] wraps. Engine
/// publish futures borrow their brokers, so the wrapped call is a
/// by-reference trait object
/// rather than an `Fn` returning `'static` futures.
pub trait PublishCall: Send + Sync {
    /// Performs the wrapped publish.
    fn call<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>>;
}

/// Wraps a publish-call surface, polymorphic over the wrapped
/// surface's object lifetime.
pub type PublishMiddleware =
    Arc<dyn for<'x> Fn(Arc<dyn PublishCall + 'x>) -> Arc<dyn PublishCall + 'x> + Send + Sync>;

/// Chains publish middlewares around a base surface: middlewares
/// apply backward, so the first-registered middleware is the
/// outermost wrapper and runs first.
fn chain_publish<'a>(
    base: Arc<dyn PublishCall + 'a>,
    middlewares: &[PublishMiddleware],
) -> Arc<dyn PublishCall + 'a> {
    let mut handler = base;
    for middleware in middlewares.iter().rev() {
        handler = middleware(handler);
    }
    handler
}

/// The [`PublishCall`] view of the wrapped broker's publish — the
/// chain's innermost element.
struct BrokerPublishCall<'a, B: Broker> {
    broker: &'a B,
}

impl<B: Broker> PublishCall for BrokerPublishCall<'_, B> {
    fn call<'b>(
        &'b self,
        topic: &'b str,
        message: Message,
    ) -> BoxFuture<'b, Result<(), BrokerError>> {
        self.broker.publish(topic, message)
    }
}

/// Wraps a subscriber handler.
pub type SubscriberMiddleware = Arc<dyn Fn(Handler) -> Handler + Send + Sync>;

/// Chains subscriber middlewares around a handler: first-registered
/// runs first.
fn chain_subscriber(base: Handler, middlewares: &[SubscriberMiddleware]) -> Handler {
    let mut handler = base;
    for middleware in middlewares.iter().rev() {
        handler = middleware(handler);
    }
    handler
}

/// A [`Broker`] decorator running publish and subscriber middleware
/// chains around another broker — the
/// `PublishMiddlewares`/`SubscriberMiddlewares` options reshaped into
/// a composable layer: the wrapped broker
/// stays untouched, the chains apply only through the decorator, and
/// every non-publish/non-subscribe call delegates unchanged.
///
/// The chains follow a fixed order: the
/// first-registered middleware is the outermost wrapper and runs
/// first.
pub struct MiddlewareBroker<B: Broker> {
    inner: Arc<B>,
    publish_middlewares: Vec<PublishMiddleware>,
    subscriber_middlewares: Vec<SubscriberMiddleware>,
}

impl<B: Broker> MiddlewareBroker<B> {
    /// Wraps `inner`: publishes made through the decorator run
    /// through the publish chain, handlers subscribed through it
    /// through the subscriber chain.
    pub fn new(
        inner: Arc<B>,
        publish_middlewares: Vec<PublishMiddleware>,
        subscriber_middlewares: Vec<SubscriberMiddleware>,
    ) -> Self {
        Self {
            inner,
            publish_middlewares,
            subscriber_middlewares,
        }
    }
}

impl<B: Broker> Broker for MiddlewareBroker<B> {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.connect().await })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.disconnect().await })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        let base: Arc<dyn PublishCall + 'a> = Arc::new(BrokerPublishCall {
            broker: self.inner.as_ref(),
        });
        let chained = chain_publish(base, &self.publish_middlewares);
        // The block owns the chained surfaces (their Arcs); every
        // borrow the nested calls take closes inside it.
        Box::pin(async move { chained.call(topic, message).await })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        let inner = Arc::clone(&self.inner);
        let chained = chain_subscriber(handler, &self.subscriber_middlewares);
        Box::pin(async move { inner.subscribe(topic, chained).await })
    }

    fn request<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<Message, BrokerError>> {
        // Request is left unwrapped by the publish chain; the
        // decorator delegates it unchanged.
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.request(topic, message).await })
    }
}

/// Builds a JSON-encoded [`Message`] from any serializable value.
pub fn json_message<T: Serialize>(value: &T) -> Result<Message, BrokerError> {
    let payload =
        serde_json::to_vec(value).map_err(|e| BrokerError::Failed(format!("json encode: {e}")))?;
    Ok(Message::from_payload(payload))
}

/// Wraps a typed closure into a [`Handler`] that JSON-decodes each
/// delivery's payload first; a decode failure surfaces as a handler
/// error.
pub fn json_handler<T, F, Fut>(handler: F) -> Handler
where
    T: DeserializeOwned + Send + 'static,
    F: Fn(Event, T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BrokerError>> + Send + 'static,
{
    Arc::new(move |event: Event| {
        let future = {
            let payload = event.message().payload.clone();
            match serde_json::from_slice::<T>(&payload) {
                Ok(value) => handler(event, value),
                Err(e) => {
                    return Box::pin(async move {
                        Err::<(), BrokerError>(BrokerError::Failed(format!("json decode: {e}")))
                    }) as BoxFuture<'static, Result<(), BrokerError>>;
                }
            }
        };
        Box::pin(future) as BoxFuture<'static, Result<(), BrokerError>>
    })
}

use serde::de::DeserializeOwned;
use serde::Serialize;

#[cfg(test)]
mod tests {
    use super::*;

    /// The JSON helpers round trip a payload through a message.
    #[tokio::test]
    async fn json_roundtrip() {
        #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
        struct Payload {
            count: u32,
        }

        let message = json_message(&Payload { count: 7 }).expect("json encode");
        let event = Event::new("topic", message);

        let handler = json_handler(|event: Event, payload: Payload| async move {
            assert_eq!(payload.count, 7);
            assert_eq!(event.topic(), "topic");
            Ok(())
        });
        handler(Event::new("topic", event.message().clone()))
            .await
            .expect("handler must succeed");
    }

    /// A decode failure surfaces as a handler error, not a panic.
    #[tokio::test]
    async fn json_decode_failure_is_an_error() {
        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct Payload {
            count: u32,
        }

        let handler =
            json_handler(
                |_event: Event, _payload: Payload| async move { Ok::<(), BrokerError>(()) },
            );
        let result = handler(Event::new(
            "topic",
            Message::from_payload(b"not json".to_vec()),
        ))
        .await;
        assert!(result.is_err(), "decode failure must surface as an error");
    }

    /// Headers and metadata follow last-write-wins builder semantics.
    #[test]
    fn message_builder_semantics() {
        let message = Message::from_payload(vec![])
            .with_header("a", "1")
            .with_header("a", "2")
            .with_metadata("kind", "demo");
        assert_eq!(message.header("a"), "2", "later writes replace");
        assert_eq!(message.header("missing"), "", "missing headers read empty");
        assert_eq!(
            message.metadata.get("kind").map(String::as_str),
            Some("demo")
        );
    }

    /// The nacker receives the action verbatim.
    #[tokio::test]
    async fn nack_passes_the_action_to_the_nacker() {
        for (expected, description) in
            [(NackAction::Requeue, "requeue"), (NackAction::Drop, "drop")]
        {
            let seen = Arc::new(std::sync::Mutex::new(None::<NackAction>));
            let sink = Arc::clone(&seen);
            let event =
                Event::with_nacker("topic", Message::from_payload(vec![1]), move |action| {
                    let sink = Arc::clone(&sink);
                    Box::pin(async move {
                        *sink.lock().unwrap() = Some(action);
                        Ok(())
                    }) as BoxFuture<'static, Result<(), BrokerError>>
                });
            event.nack(expected).await.expect("nack must settle");
            assert_eq!(*seen.lock().unwrap(), Some(expected), "{description}");
        }
    }

    /// A nack on an implicit-ack event is the documented no-op.
    #[tokio::test]
    async fn nack_without_a_nacker_is_a_noop() {
        let event = Event::new("topic", Message::from_payload(vec![]));
        let result = event.nack(NackAction::Requeue).await;
        assert!(
            result.is_ok(),
            "implicit-ack engines have nothing to hand back, got {result:?}"
        );
    }

    /// The error-handler hook observes failures without swallowing
    /// them: the wrapper's result stays the inner handler's, and the
    /// delivered message travels to the observer.
    #[tokio::test]
    async fn the_error_handler_observes_without_swallowing() {
        let observed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let poison = Message::from_payload(b"poison".to_vec()).with_header("kind", "bad");
        let inner_error = BrokerError::Failed("decode blew up".to_string());

        let sink = Arc::clone(&observed);
        let observer: ErrorHandler = Arc::new(move |message, error| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                sink.lock().unwrap().push(format!(
                    "{}:{error}",
                    String::from_utf8_lossy(&message.payload)
                ));
            })
        });
        let handler = with_error_handler(
            Arc::new(move |_event: Event| {
                let error = inner_error.clone();
                Box::pin(async move { Err(error) }) as BoxFuture<'static, Result<(), BrokerError>>
            }),
            observer,
        );

        let result = handler(Event::new("topic", poison)).await;
        assert!(result.is_err(), "the error must still surface");
        assert_eq!(
            *observed.lock().unwrap(),
            vec!["poison:broker operation failed: decode blew up"],
            "the observer saw the message and the error"
        );

        // A success path never reaches the observer.
        let handler = with_error_handler(
            Arc::new(|_event: Event| {
                Box::pin(async { Ok(()) }) as BoxFuture<'static, Result<(), BrokerError>>
            }),
            Arc::new(|_message, _error| Box::pin(async {}) as BoxFuture<'static, ()>),
        );
        handler(Event::new("topic", Message::from_payload(vec![])))
            .await
            .expect("success passes through");
        assert_eq!(
            observed.lock().unwrap().len(),
            1,
            "the observer fired only for the failure"
        );
    }

    /// The error-handler hook composes with the subscriber middleware
    /// chain: the wrapped handler is just another Handler.
    #[tokio::test]
    async fn the_error_handler_composes_with_middlewares() {
        let order = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
        let sink = Arc::clone(&order);
        let observer: ErrorHandler = Arc::new(move |_message, _error| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                sink.lock().unwrap().push("observer");
            })
        });
        let logged = with_error_handler(
            Arc::new(|_event: Event| {
                Box::pin(async { Err(BrokerError::NotConnected) })
                    as BoxFuture<'static, Result<(), BrokerError>>
            }),
            observer,
        );
        let middleware: SubscriberMiddleware = Arc::new(|handler: Handler| {
            Arc::new(move |event: Event| {
                let handler = Arc::clone(&handler);
                Box::pin(async move { handler(event).await })
                    as BoxFuture<'static, Result<(), BrokerError>>
            }) as Handler
        });
        let chained = chain_subscriber(logged, &[middleware]);
        let result = chained(Event::new("topic", Message::from_payload(vec![]))).await;
        assert!(result.is_err());
        assert_eq!(
            *order.lock().unwrap(),
            vec!["observer"],
            "the hook fires inside the chain"
        );
    }

    /// The default [`Broker::request`] returns the not-implemented
    /// error.
    #[tokio::test]
    async fn default_request_is_not_implemented() {
        let broker = MiddlewareBroker::new(Arc::new(NullBroker), vec![], vec![]);
        let result = broker.request("topic", Message::from_payload(vec![])).await;
        assert!(
            matches!(result, Err(BrokerError::Failed(_))),
            "default request must be the not-implemented error, got {result:?}"
        );
    }

    /// The middleware chains apply backward — the first-registered
    /// middleware is the outermost wrapper and runs first, wrapping
    /// the base handler last.
    #[tokio::test]
    async fn middleware_chain_order_is_canonical() {
        let publish_log = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
        let subscribe_log = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));

        // Base publish handler: the wrapped broker.
        let broker_publish_log = Arc::clone(&publish_log);
        let broker = Arc::new(RecordingBroker {
            publish_log: broker_publish_log,
            subscribe_log: Arc::clone(&subscribe_log),
        });

        // Publish middlewares p1, p2: each records its marker, then
        // runs the wrapped call surface.
        let publish_middlewares: Vec<PublishMiddleware> = [1, 2]
            .map(|index| {
                let log = Arc::clone(&publish_log);
                let marker: &'static str = if index == 1 { "p1" } else { "p2" };
                let middleware: PublishMiddleware =
                    Arc::new(move |wrapped| marker_publish_call(wrapped, log.clone(), marker));
                middleware
            })
            .into();

        // Subscriber middlewares s1, s2 wrapping a base handler that
        // records "s-base".
        let subscriber_middlewares = [1, 2].map(|index| {
            let log = Arc::clone(&subscribe_log);
            let marker: &'static str = if index == 1 { "s1" } else { "s2" };
            Arc::new(move |handler: Handler| {
                let log = Arc::clone(&log);
                Arc::new(move |event: Event| {
                    let log = Arc::clone(&log);
                    let handler = Arc::clone(&handler);
                    Box::pin(async move {
                        log.lock().unwrap().push(marker);
                        handler(event).await
                    }) as BoxFuture<'static, Result<(), BrokerError>>
                }) as Handler
            }) as SubscriberMiddleware
        });
        let base_subscriber_log = Arc::clone(&subscribe_log);
        let base_subscriber: Handler = Arc::new(move |_event: Event| {
            let log = Arc::clone(&base_subscriber_log);
            Box::pin(async move {
                log.lock().unwrap().push("s-base");
                Ok(())
            }) as BoxFuture<'static, Result<(), BrokerError>>
        });

        let decorated =
            MiddlewareBroker::new(broker, publish_middlewares, subscriber_middlewares.into());

        // The publish chain: p1 (first-registered) outermost, then
        // p2, then the wrapped broker.
        decorated
            .publish("topic", Message::from_payload(vec![]))
            .await
            .expect("decorated publish");
        assert_eq!(
            *publish_log.lock().unwrap(),
            vec!["p1", "p2", "p-base"],
            "publish chain runs first-registered middleware first"
        );

        // The subscriber chain: s1, s2, then the base handler; the
        // wrapped broker observes only the chained handler.
        decorated
            .subscribe("topic", base_subscriber)
            .await
            .expect("decorated subscribe");
        assert_eq!(
            *subscribe_log.lock().unwrap(),
            vec!["base-sub", "s1", "s2", "s-base"],
            "subscriber chain runs first-registered middleware first"
        );
    }

    /// A [`PublishCall`] wrapper recording its marker before
    /// delegating — the chain-order test's middleware element.
    struct MarkerPublishCall<'a> {
        wrapped: Arc<dyn PublishCall + 'a>,
        log: Arc<std::sync::Mutex<Vec<&'static str>>>,
        marker: &'static str,
    }

    impl PublishCall for MarkerPublishCall<'_> {
        fn call<'b>(
            &'b self,
            topic: &'b str,
            message: Message,
        ) -> BoxFuture<'b, Result<(), BrokerError>> {
            self.log.lock().unwrap().push(self.marker);
            self.wrapped.call(topic, message)
        }
    }

    /// Builds a [`MarkerPublishCall`] as a publish-call surface — the
    /// helper gives the middleware closure a concrete
    /// lifetime-polymorphic signature.
    fn marker_publish_call<'a>(
        wrapped: Arc<dyn PublishCall + 'a>,
        log: Arc<std::sync::Mutex<Vec<&'static str>>>,
        marker: &'static str,
    ) -> Arc<dyn PublishCall + 'a> {
        Arc::new(MarkerPublishCall {
            wrapped,
            log,
            marker,
        })
    }

    /// The [`Broker`] stub the chain-order test wraps: its publish
    /// records `p-base`, its subscribe invokes the chained handler and
    /// records `base-sub`.
    struct RecordingBroker {
        publish_log: Arc<std::sync::Mutex<Vec<&'static str>>>,
        subscribe_log: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    impl Broker for RecordingBroker {
        fn name(&self) -> &'static str {
            "recording"
        }

        fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
            Box::pin(async { Ok(()) })
        }

        fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
            Box::pin(async { Ok(()) })
        }

        fn publish<'a>(
            &'a self,
            topic: &'a str,
            message: Message,
        ) -> BoxFuture<'a, Result<(), BrokerError>> {
            let _ = (topic, message);
            self.publish_log.lock().unwrap().push("p-base");
            Box::pin(async { Ok(()) })
        }

        fn subscribe<'a>(
            &'a self,
            topic: &'a str,
            handler: Handler,
        ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
            let _ = topic;
            self.subscribe_log.lock().unwrap().push("base-sub");
            let chained = handler(Event::new("topic", Message::from_payload(vec![])));
            Box::pin(async move {
                chained.await?;
                Ok(Box::new(NullSubscriber) as Box<dyn Subscriber>)
            })
        }
    }

    /// A [`Broker`] stub with no behavior, for the default-request
    /// test.
    struct NullBroker;

    impl Broker for NullBroker {
        fn name(&self) -> &'static str {
            "null"
        }

        fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
            Box::pin(async { Ok(()) })
        }

        fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
            Box::pin(async { Ok(()) })
        }

        fn publish<'a>(
            &'a self,
            topic: &'a str,
            message: Message,
        ) -> BoxFuture<'a, Result<(), BrokerError>> {
            let _ = (topic, message);
            Box::pin(async { Ok(()) })
        }

        fn subscribe<'a>(
            &'a self,
            topic: &'a str,
            handler: Handler,
        ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
            let _ = (topic, handler);
            Box::pin(async { Ok(Box::new(NullSubscriber) as Box<dyn Subscriber>) })
        }
    }

    /// A [`Subscriber`] stub.
    struct NullSubscriber;

    impl Subscriber for NullSubscriber {
        fn topic(&self) -> &str {
            "null"
        }

        fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
            Box::pin(async { Ok(()) })
        }
    }
}
