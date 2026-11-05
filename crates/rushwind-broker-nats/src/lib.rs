//! NATS engine for the RushWind broker, core-NATS mode over
//! `async-nats`.
//!
//! # The wire behavior
//!
//! Publishes are `PublishMsg` on the
//! subject — payload-only, NATS headers are not attached. Topics
//! are NATS subjects and follow NATS wildcards (`*` one token, `>`
//! the rest); subscriptions are plain (no queue group).
//!
//! NATS core is at-most-once fire-and-forget: the event ack is a
//! no-op, unsubscribing drops the server-side subscription, and a
//! dropped [`Subscriber`] removes it best-effort.
//!
//! Request/reply rides core NATS request semantics — the client
//! library's managed inbox subscription — and the response carries
//! the payload only, matching the subscribe path.
//!
//! # Connection lifecycle
//!
//! `disconnect` drains the connection: subscriptions are closed
//! server-side first, then the connection shuts down — further
//! publishes fail with [`BrokerError::NotConnected`] instead of
//! resurrecting traffic on a torn-down session. A `connect` after a
//! `disconnect` rebuilds the client from the stored address; a
//! closed client would otherwise leave the broker permanently
//! unusable. `disconnect` is idempotent.
//!
//! # Divergences
//!
//! - The wire metadata has no NATS carrier beyond headers.
//!
//! # Testing
//!
//! Live conformance tests run against a real NATS via the `live`
//! feature; there is no embedded NATS for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use futures::StreamExt;
use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Handler, Message, Subscriber};

struct Inner {
    /// The server URL the client was built from — `connect` needs it
    /// to rebuild after a `disconnect` closed the client.
    addr: String,
    /// The live client; `None` once disconnected.
    client: Mutex<Option<async_nats::Client>>,
}

/// Dials NATS and returns the live client.
async fn establish(addr: &str) -> Result<async_nats::Client, BrokerError> {
    async_nats::connect(addr)
        .await
        .map_err(|e| BrokerError::Failed(format!("nats connect {addr}: {e}")))
}

/// A NATS-backed broker over core NATS.
pub struct NatsBroker {
    inner: Arc<Inner>,
}

impl NatsBroker {
    /// Connects to NATS at `addr` (e.g. `nats://127.0.0.1:4222`).
    pub async fn connect(addr: &str) -> Result<Self, BrokerError> {
        let client = establish(addr).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                addr: addr.to_string(),
                client: Mutex::new(Some(client)),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (required).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: NatsSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::connect(&settings.addr).await
    }
}

/// The bootstrap factory's settings wire shape for
/// [`NatsBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct NatsSettings {
    /// The NATS server URL.
    pub addr: String,
}

impl Broker for NatsBroker {
    fn name(&self) -> &'static str {
        "nats"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // async-nats reconnects the live client internally; only a
            // disconnect-closed client needs rebuilding — a closed
            // client left in place would make the broker permanently
            // unusable (upstream rebuilt the closed pulsar client in
            // Connect for the same reason).
            if self.inner.client.lock().await.is_some() {
                return Ok(());
            }
            let client = establish(&self.inner.addr).await?;
            *self.inner.client.lock().await = Some(client);
            Ok(())
        })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // Drain closes orderly: subscriptions are unsubscribed
            // server-side first, remaining traffic flushed, then the
            // connection shuts down and every further client command
            // fails — a torn-down session must not keep accepting
            // publishes. Double disconnect finds no client and is a
            // no-op.
            let Some(client) = self.inner.client.lock().await.take() else {
                return Ok(());
            };
            client
                .drain()
                .await
                .map_err(|e| BrokerError::Failed(format!("nats disconnect: {e}")))
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            let Some(client) = self.inner.client.lock().await.clone() else {
                return Err(BrokerError::NotConnected);
            };
            client
                .publish(topic.to_string(), message.payload.into())
                .await
                .map_err(|e| BrokerError::Failed(format!("nats publish: {e}")))
        })
    }

    fn request<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<Message, BrokerError>> {
        // Core NATS request-reply: the client library manages the
        // inbox subscription. The response carries the payload only,
        // like the subscribe path. A failure returns before any
        // response message is constructed.
        Box::pin(async move {
            let Some(client) = self.inner.client.lock().await.clone() else {
                return Err(BrokerError::NotConnected);
            };
            let response = client
                .request(topic.to_string(), message.payload.into())
                .await
                .map_err(|e| BrokerError::Failed(format!("nats request: {e}")))?;
            Ok(Message::from_payload(response.payload.to_vec()))
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            let Some(client) = self.inner.client.lock().await.clone() else {
                return Err(BrokerError::NotConnected);
            };
            let mut subscription = client
                .subscribe(topic.to_string())
                .await
                .map_err(|e| BrokerError::Failed(format!("nats subscribe {topic}: {e}")))?;

            // The SUB rides the wire without a server ack; the flush
            // rides a PING/PONG on the same connection, so once this
            // returns the server has processed the registration.
            // Without it a racing publish is silently dropped — core
            // NATS matches subscriptions only at publish time.
            if let Err(e) = client.flush().await {
                // Failed registration cleanup: the server-side
                // subscription already exists, so an early return that
                // left it live would consume deliveries with no reader
                // attached — unsubscribe it on the way out.
                let _ = subscription.unsubscribe().await;
                return Err(BrokerError::Failed(format!(
                    "nats subscribe {topic} flush: {e}"
                )));
            }

            // The reader owns the server-side subscription; cancelling
            // it drops the subscription, which unsubscribes.
            let done = CancellationToken::new();
            let task_done = done.clone();
            let reader = tokio::spawn(async move {
                loop {
                    let message = tokio::select! {
                        _ = task_done.cancelled() => return,
                        message = subscription.next() => match message {
                            Some(message) => message,
                            None => return,
                        },
                    };
                    // The message carries the actual published
                    // subject, which the wildcard filter over-matches.
                    let subject = message.subject.to_string();
                    let payload = message.payload.to_vec();
                    let event = Event::new(subject, Message::from_payload(payload));
                    tokio::spawn(handler(event));
                }
            });

            Ok(Box::new(NatsSubscriber {
                topic: topic.to_string(),
                done,
                reader: Mutex::new(Some(reader)),
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The NATS subscription handle.
struct NatsSubscriber {
    topic: String,
    done: CancellationToken,
    reader: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Subscriber for NatsSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // Cancel ends the reader, whose dropped subscription
            // unsubscribes server-side.
            self.done.cancel();
            if let Some(reader) = self.reader.lock().await.take() {
                let _ = reader.await;
            }
            Ok(())
        })
    }
}

impl Drop for NatsSubscriber {
    fn drop(&mut self) {
        self.done.cancel();
        if let Ok(mut reader) = self.reader.try_lock() {
            if let Some(reader) = reader.take() {
                reader.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Connecting to a dead server fails with a mapped error instead
    /// of panicking — the constructor surfaces the dial failure
    /// verbatim.
    #[tokio::test]
    async fn dead_address_connect_fails_cleanly() {
        let result = NatsBroker::connect("nats://127.0.0.1:1").await;
        assert!(
            matches!(result, Err(BrokerError::Failed(_))),
            "a dead address must surface as Failed"
        );
    }
}
