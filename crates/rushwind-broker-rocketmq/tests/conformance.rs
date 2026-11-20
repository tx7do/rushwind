//! The shared broker conformance suite over the rocketmq engine and
//! its in-process mock — a fresh mock per pin-batch.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, BoxFuture, Broker, BrokerError};
use rushwind_broker_rocketmq::{RocketmqBroker, RocketmqOptions};
use rushwind_testkit::broker_conformance::{broker_conformance_suite, BrokerHarness};

mod common;

struct Harness;

impl BrokerHarness for Harness {
    fn build(&self, ack_mode: AckMode) -> BoxFuture<'_, Result<Arc<dyn Broker>, BrokerError>> {
        Box::pin(async move {
            let (addr, _mock) = common::spawn_rocketmq().await;
            let broker = RocketmqBroker::new(RocketmqOptions {
                broker_addr: addr,
                broker_name: "broker-a".to_string(),
                producer_group: "rushwind".to_string(),
                consumer_group: "rushwind".to_string(),
                ack_mode,
                request_timeout: Duration::from_secs(5),
            })?;
            broker.connect().await?;
            Ok(Arc::new(broker) as Arc<dyn Broker>)
        })
    }
}

#[tokio::test]
async fn the_rocketmq_engine_is_conformant() {
    broker_conformance_suite(&Harness, "rocketmq").await;
}
