//! The shared broker conformance suite over the azuresb engine and
//! its in-process mock — a fresh mock per pin-batch.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, BoxFuture, Broker, BrokerError};
use rushwind_broker_azuresb::{AzureSbBroker, AzureSbOptions};
use rushwind_testkit::broker_conformance::{broker_conformance_suite, BrokerHarness};

mod common;

const ENTITY_URL: &str = "https://rushwind.servicebus.windows.net/queue";

struct Harness;

impl BrokerHarness for Harness {
    fn build(&self, ack_mode: AckMode) -> BoxFuture<'_, Result<Arc<dyn Broker>, BrokerError>> {
        Box::pin(async move {
            let (endpoint, _mock) = common::spawn_azuresb().await;
            let broker = AzureSbBroker::new(AzureSbOptions {
                entity_url: ENTITY_URL.to_string(),
                endpoint: Some(endpoint),
                sas_key_name: "rushwind".to_string(),
                sas_key: "the-sas-key".to_string(),
                sas_token: None,
                ack_mode,
                request_timeout: Duration::from_secs(5),
            })?;
            broker.connect().await?;
            Ok(Arc::new(broker) as Arc<dyn Broker>)
        })
    }
}

#[tokio::test]
async fn the_azuresb_engine_is_conformant() {
    broker_conformance_suite(&Harness, "azuresb").await;
}
