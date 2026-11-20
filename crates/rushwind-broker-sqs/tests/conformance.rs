//! The shared broker conformance suite over the sqs engine and its
//! in-process mock — a fresh mock per pin-batch.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, BoxFuture, Broker, BrokerError};
use rushwind_broker_sqs::{SqsBroker, SqsOptions};
use rushwind_testkit::broker_conformance::{broker_conformance_suite, BrokerHarness};

mod common;

const QUEUE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue";

struct Harness;

impl BrokerHarness for Harness {
    fn build(&self, ack_mode: AckMode) -> BoxFuture<'_, Result<Arc<dyn Broker>, BrokerError>> {
        Box::pin(async move {
            let (endpoint, _mock) = common::spawn_sqs().await;
            let broker = SqsBroker::new(SqsOptions {
                queue_url: QUEUE_URL.to_string(),
                endpoint: Some(endpoint),
                access_key: "AKIDEXAMPLE".to_string(),
                secret_key: "secret".to_string(),
                session_token: None,
                ack_mode,
                request_timeout: Duration::from_secs(5),
            })?;
            broker.connect().await?;
            Ok(Arc::new(broker) as Arc<dyn Broker>)
        })
    }
}

#[tokio::test]
async fn the_sqs_engine_is_conformant() {
    broker_conformance_suite(&Harness, "sqs").await;
}
