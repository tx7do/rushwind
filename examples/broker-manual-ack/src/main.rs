//! Manual settlement, end to end.
//!
//! The demo wires an MQTT broker in `AckMode::Manual` (set
//! `MQTT_ADDR` to a reachable broker, e.g. `127.0.0.1:1883`): one
//! handler acks well-formed deliveries, a second nacks the poison
//! payload back to the queue, and every failure passes the
//! `with_error_handler` observation point without killing the
//! subscription. The same shapes apply to every engine carrying the
//! contract's AckMode face (mqtt, rabbitmq, pulsar, stomp, sqs,
//! gcpubsub).

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{
    with_error_handler, AckMode, Broker, BrokerError, ErrorHandler, Event, Handler, Message,
    NackAction,
};
use rushwind_broker_mqtt::{MqttBroker, MqttOptions};

fn poison_handler(observed: Arc<std::sync::Mutex<Vec<String>>>) -> Handler {
    let observer: ErrorHandler = Arc::new(move |message, error| {
        let observed = observed.clone();
        Box::pin(async move {
            observed.lock().unwrap().push(format!(
                "observed failure: {} — {error}",
                String::from_utf8_lossy(&message.payload)
            ));
        })
    });
    // The poison detector: payloads that do not parse as JSON nack
    // back to the queue; everything else acks.
    let wrapped =
        rushwind_broker::json_handler(|event: Event, payload: serde_json::Value| async move {
            let _ = (event, payload);
            Ok(())
        });
    let poison_aware: Handler = Arc::new(move |event: Event| {
        let is_json =
            serde_json::from_slice::<serde_json::Value>(event.message().payload.as_slice()).is_ok();
        let wrapped = wrapped.clone();
        Box::pin(async move {
            if is_json {
                wrapped(event).await
            } else {
                event.nack(NackAction::Requeue).await?;
                Err(BrokerError::Failed("poison message requeued".to_string()))
            }
        }) as rushwind_broker::BoxFuture<'static, Result<(), BrokerError>>
    });
    with_error_handler(poison_aware, observer)
}

#[tokio::main]
async fn main() {
    let addr = std::env::var("MQTT_ADDR").unwrap_or_else(|_| "127.0.0.1:1883".to_string());
    let broker = MqttBroker::with_ack_mode(
        {
            let mut options = MqttOptions::new(addr.clone());
            options.keep_alive = Duration::from_secs(10);
            options
        },
        AckMode::Manual,
    );
    broker.connect().await.expect("broker connects");

    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler = poison_handler(Arc::clone(&observed));

    let _subscriber = broker
        .subscribe("demo/manual", handler)
        .await
        .expect("subscribe");
    println!("subscribed to demo/manual under AckMode::Manual — publishing three messages:");
    for payload in [r#"{"ok":true}"#, "not json at all", r#"{"ok":2}"#] {
        broker
            .publish(
                "demo/manual",
                Message::from_payload(payload.as_bytes().to_vec()).with_header("kind", "demo"),
            )
            .await
            .expect("publish");
    }
    // Give the pump a moment to deliver and settle.
    tokio::time::sleep(Duration::from_secs(2)).await;
    for line in observed.lock().unwrap().iter() {
        println!("{line}");
    }
    println!("done — the subscription is still live after the poison message");
}
