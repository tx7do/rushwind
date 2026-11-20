//! The shared broker conformance suite over the gcpubsub engine and
//! its in-process mock — a fresh mock per pin-batch.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, BoxFuture, Broker, BrokerError};
use rushwind_broker_gcpubsub::{GcpPubSubBroker, GcpPubSubOptions};
use rushwind_testkit::broker_conformance::{broker_conformance_suite, BrokerHarness};

mod common;

struct Harness;

impl BrokerHarness for Harness {
    fn build(&self, ack_mode: AckMode) -> BoxFuture<'_, Result<Arc<dyn Broker>, BrokerError>> {
        Box::pin(async move {
            let (endpoint, _mock) = common::spawn_gcpubsub().await;
            let broker = GcpPubSubBroker::new(GcpPubSubOptions {
                endpoint: Some(endpoint),
                project_id: "rushwind".to_string(),
                topic_id: "events".to_string(),
                subscription_id: "events-sub".to_string(),
                token: Some("sa-token".to_string()),
                ack_mode,
                request_timeout: Duration::from_secs(5),
            })?;
            broker.connect().await?;
            Ok(Arc::new(broker) as Arc<dyn Broker>)
        })
    }
}

#[tokio::test]
async fn the_gcpubsub_engine_is_conformant() {
    broker_conformance_suite(&Harness, "gcpubsub").await;
}
