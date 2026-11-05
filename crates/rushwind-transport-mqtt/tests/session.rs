//! Integration tests: the MQTT consumer bridge against an embedded
//! rumqttd broker, under the full RushWind lifecycle.
//!
//! The bridge's contract surface is deliberately small — one pre-trusted
//! broker connection, no session middleware (see
//! `docs/session-middleware.md`) — so the integration suite asserts the
//! two things that matter: delivery of subscribed messages to the
//! handler, and a clean lifecycle shutdown while the pump is live.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_core::App;
use rushwind_transport::{Server, ServerError, StopSignal};
use rushwind_transport_mqtt::MqttBridge;
use tokio::time::{sleep, timeout};

mod common;

/// Builds the full lifecycle around `bridge` and returns the shutdown
/// trigger and the run task's join handle.
#[allow(clippy::type_complexity)]
async fn spawn_lifecycle(
    bridge: MqttBridge,
) -> (StopSignal, tokio::task::JoinHandle<Result<(), ServerError>>) {
    let app = Arc::new(
        App::builder()
            .erased_server(Arc::new(bridge))
            .stop_timeout(Duration::from_millis(500))
            .build(),
    );
    let run_app = Arc::clone(&app);
    let trigger = StopSignal::new();
    let run_trigger = trigger.clone();
    let handle = tokio::spawn(async move { run_app.run(run_trigger).await });
    (trigger, handle)
}

/// Drives the run task to completion after triggering shutdown and asserts
/// a clean outcome.
async fn finish_lifecycle(
    trigger: StopSignal,
    run: tokio::task::JoinHandle<Result<(), ServerError>>,
) {
    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn subscribed_messages_reach_the_handler() {
    let addr = common::broker();
    let log: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let handler_log = Arc::clone(&log);

    let bridge = MqttBridge::builder(addr, "rushwind-bridge-test")
        .subscribe("demo/#")
        .session_handler(move |message| {
            let log = Arc::clone(&handler_log);
            async move {
                log.lock().expect("log poisoned").push((
                    message.topic,
                    String::from_utf8_lossy(&message.payload).into_owned(),
                ));
            }
        })
        .build()
        .expect("bridge must build");
    let (trigger, run) = spawn_lifecycle(bridge).await;

    // Let the subscription register with the broker before publishing.
    sleep(Duration::from_millis(300)).await;
    common::publish(addr, "publisher-1", "demo/one", b"hello").await;
    common::publish(addr, "publisher-1", "demo/two", b"world").await;

    // Both messages must land, with topic and payload intact.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        {
            let log = log.lock().expect("log poisoned");
            let has_one = log
                .iter()
                .any(|(topic, payload)| topic == "demo/one" && payload == "hello");
            let has_two = log
                .iter()
                .any(|(topic, payload)| topic == "demo/two" && payload == "world");
            if has_one && has_two {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "subscribed messages must reach the handler"
        );
        sleep(Duration::from_millis(50)).await;
    }

    // A clean lifecycle shutdown while the pump is live.
    finish_lifecycle(trigger, run).await;
}

/// Per-subscription dispatch: a subscription with a bound handler
/// receives its own deliveries, one without goes to the default
/// handler, and a delivery matching no filter reaches neither.
#[tokio::test]
async fn per_subscription_handlers_dispatch_by_filter() {
    let addr = common::broker();
    let bound_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let default_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let bound_log_for_handler = Arc::clone(&bound_log);
    let default_log_for_handler = Arc::clone(&default_log);

    let bridge = MqttBridge::builder(addr, "rushwind-bridge-dispatch")
        .subscribe_with_handler("demo/#", move |message| {
            let log = Arc::clone(&bound_log_for_handler);
            async move {
                log.lock().expect("log poisoned").push(message.topic);
            }
        })
        .subscribe("other/#")
        .session_handler(move |message| {
            let log = Arc::clone(&default_log_for_handler);
            async move {
                log.lock().expect("log poisoned").push(message.topic);
            }
        })
        .build()
        .expect("bridge must build");
    let (trigger, run) = spawn_lifecycle(bridge).await;

    // Let the subscriptions register with the broker before publishing.
    sleep(Duration::from_millis(300)).await;
    common::publish(addr, "publisher-2", "demo/bound", b"bound").await;
    common::publish(addr, "publisher-2", "other/default", b"default").await;

    // The bound handler sees only its filter's deliveries; the
    // default handler only the unbound subscription's.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        {
            let bound = bound_log.lock().expect("log poisoned");
            let default = default_log.lock().expect("log poisoned");
            if bound.iter().any(|topic| topic == "demo/bound")
                && default.iter().any(|topic| topic == "other/default")
            {
                assert_eq!(bound.len(), 1, "bound handler sees only its filter");
                assert_eq!(
                    default.len(),
                    1,
                    "default handler sees only the unbound subscription"
                );
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "per-subscription deliveries must arrive"
        );
        sleep(Duration::from_millis(50)).await;
    }

    // A topic matching no filter: neither handler may see it.
    common::publish(addr, "publisher-2", "unmatched/topic", b"nothing").await;
    sleep(Duration::from_millis(300)).await;
    assert!(
        !bound_log
            .lock()
            .expect("log poisoned")
            .iter()
            .any(|topic| topic == "unmatched/topic"),
        "unmatched topic must not reach the bound handler"
    );
    assert!(
        !default_log
            .lock()
            .expect("log poisoned")
            .iter()
            .any(|topic| topic == "unmatched/topic"),
        "unmatched topic must not reach the default handler"
    );

    finish_lifecycle(trigger, run).await;
}

/// A subscription without a bound handler and without a default
/// handler must fail the build — its deliveries would go nowhere.
#[tokio::test]
async fn subscription_without_any_handler_fails_to_build() {
    let addr = common::broker();
    let result = MqttBridge::builder(addr, "rushwind-bridge-no-handler")
        .subscribe("demo/#")
        .build();
    assert!(
        result.is_err(),
        "a subscription with no reachable handler must fail the build"
    );
}

/// Restart actually resubscribes: a stop-then-start cycle on the same
/// bridge re-registers every frozen subscription, so deliveries keep
/// flowing after the restart (upstream's stop/start hand-off lost the
/// registration parameters; the bridge freezes them in the state, and
/// each start rebuilds the connection generation from it).
#[tokio::test]
async fn restart_resubscribes_and_delivers_again() {
    let addr = common::broker();
    let log: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let handler_log = Arc::clone(&log);

    let bridge = MqttBridge::builder(addr, "rushwind-bridge-restart")
        .subscribe("restart/#")
        .session_handler(move |message| {
            let log = Arc::clone(&handler_log);
            async move {
                log.lock().expect("log poisoned").push((
                    message.topic,
                    String::from_utf8_lossy(&message.payload).into_owned(),
                ));
            }
        })
        .build()
        .expect("bridge must build");
    let server: Arc<dyn Server> = Arc::new(bridge);

    // First life: delivery works.
    let first_trigger = StopSignal::new();
    let run_trigger = first_trigger.clone();
    let first_run = {
        let server = Arc::clone(&server);
        tokio::spawn(async move { server.start(run_trigger).await })
    };
    sleep(Duration::from_millis(300)).await;
    common::publish(addr, "publisher-restart-1", "restart/first", b"one").await;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let delivered = log
            .lock()
            .expect("log poisoned")
            .iter()
            .any(|(topic, payload)| topic == "restart/first" && payload == "one");
        if delivered {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the first life must deliver"
        );
        sleep(Duration::from_millis(50)).await;
    }

    // Stop, and wait for the pump future to wind down.
    first_trigger.signal();
    let outcome = timeout(Duration::from_secs(5), first_run)
        .await
        .expect("the first start must finish")
        .expect("the first start must not panic");
    assert!(
        matches!(outcome, Err(ServerError::Cancelled)),
        "a signalled start returns Cancelled, got {outcome:?}"
    );

    // Second life on the same bridge: the frozen registration
    // parameters must re-register on the fresh generation.
    let second_trigger = StopSignal::new();
    let run_trigger = second_trigger.clone();
    let second_run = {
        let server = Arc::clone(&server);
        tokio::spawn(async move { server.start(run_trigger).await })
    };
    sleep(Duration::from_millis(300)).await;
    common::publish(addr, "publisher-restart-2", "restart/second", b"two").await;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let delivered = log
            .lock()
            .expect("log poisoned")
            .iter()
            .any(|(topic, payload)| topic == "restart/second" && payload == "two");
        if delivered {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the restarted bridge must resubscribe and deliver"
        );
        sleep(Duration::from_millis(50)).await;
    }

    second_trigger.signal();
    let outcome = timeout(Duration::from_secs(5), second_run)
        .await
        .expect("the second start must finish")
        .expect("the second start must not panic");
    assert!(matches!(outcome, Err(ServerError::Cancelled)));
}
