//! The shared broker conformance suite over the nsq engine and its
//! in-process mock — a fresh mock per pin-batch, so no pin's residue
//! can reach the next.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, BoxFuture, Broker, BrokerError};
use rushwind_broker_nsq::{NsqBroker, NsqOptions};
use rushwind_testkit::broker_conformance::{broker_conformance_suite, BrokerHarness};

mod common;

struct Harness;

impl BrokerHarness for Harness {
    fn build(&self, ack_mode: AckMode) -> BoxFuture<'_, Result<Arc<dyn Broker>, BrokerError>> {
        Box::pin(async move {
            let (addr, _mock) = common::spawn_nsq().await;
            let broker = NsqBroker::new(NsqOptions {
                addr,
                channel_id: "rushwind".to_string(),
                ack_mode,
                request_timeout: Duration::from_secs(5),
            })?;
            broker.connect().await?;
            Ok(Arc::new(broker) as Arc<dyn Broker>)
        })
    }
}

#[tokio::test]
async fn the_nsq_engine_is_conformant() {
    broker_conformance_suite(&Harness, "nsq").await;
}
