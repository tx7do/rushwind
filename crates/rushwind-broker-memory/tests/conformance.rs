//! The shared broker conformance suite over the memory reference
//! engine — the baseline every external adapter is judged against.

#![cfg(test)]

use std::sync::{Arc, Mutex};

use rushwind_broker::{AckMode, BoxFuture, Broker, BrokerError, Handler, Subscriber};
use rushwind_broker_memory::MemoryBroker;
use rushwind_testkit::broker_conformance::{broker_conformance_suite, BrokerHarness};

/// The harness: a fresh engine per pin-batch, the latest concrete
/// broker kept so the per-subscription Manual face can route.
#[derive(Default)]
struct Harness {
    latest: Mutex<Option<Arc<MemoryBroker>>>,
}

impl BrokerHarness for Harness {
    fn build(&self, _ack_mode: AckMode) -> BoxFuture<'_, Result<Arc<dyn Broker>, BrokerError>> {
        Box::pin(async move {
            let broker = Arc::new(MemoryBroker::new());
            broker.connect().await?;
            *self.latest.lock().expect("harness") = Some(Arc::clone(&broker));
            Ok(broker as Arc<dyn Broker>)
        })
    }

    fn subscribe<'a>(
        &self,
        _broker: &'a Arc<dyn Broker>,
        topic: &'a str,
        handler: Handler,
        ack_mode: AckMode,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        // The concrete broker clones out BEFORE the pinned future: the
        // future owns it, so no borrow of `self` crosses the await.
        let concrete = self
            .latest
            .lock()
            .expect("harness")
            .clone()
            .expect("build ran first");
        Box::pin(async move {
            match ack_mode {
                AckMode::Manual => concrete.subscribe_with_mode(topic, handler, ack_mode).await,
                _ => concrete.subscribe(topic, handler).await,
            }
        })
    }
}

#[tokio::test]
async fn the_memory_engine_is_conformant() {
    broker_conformance_suite(&Harness::default(), "memory").await;
}
