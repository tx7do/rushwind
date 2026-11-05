//! Live conformance against a real eureka server, driven by CI's
//! service container. Compile-gated behind the `live` feature; the
//! server address comes from `REGISTRY_EUREKA_ADDRESS`. These tests
//! pin the interoperability contract where it matters most — a real
//! eureka serving the same application view any external consumer
//! would see.
//!
//! Two eureka realities shape the tests: the server caches its
//! responses for ~30 s, and this adapter's cache exists only for
//! watched applications (an unwatched `get_service` always falls
//! through to the never-unwrapped single-application
//! fetch and sees nothing — reproduced here). So the round trip goes
//! through a watcher, and the polls allow the cache periods to elapse.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_registry::{Discovery, Registrar, Registration};
use rushwind_registry_eureka::EurekaRegistry;
use rushwind_transport::Instance;

fn endpoint() -> String {
    std::env::var("REGISTRY_EUREKA_ADDRESS").unwrap_or_else(|_| "http://127.0.0.1:8761".to_string())
}

/// Eureka keys instances by `{ip}.{app}.{port}`: each test gets its own
/// port so parallel tests hold distinct server-side instances.
fn sample(port: u16) -> Registration {
    Registration::new(Instance {
        id: "order-01".to_string(),
        name: "order-service".to_string(),
        version: "v1.0.0".to_string(),
        endpoints: vec![format!("grpc://127.0.0.1:{port}")],
    })
}

/// The round trip, through a watcher as the discovery shape
/// requires: register, wait for the refresh loop to land the instance
/// in the watched cache (get_service then serves it too), then
/// deregister and wait for the removal.
#[tokio::test]
async fn registered_instance_round_trips_then_leaves() {
    let registry = EurekaRegistry::connect(&[endpoint()]).expect("eureka connects");
    let registration = sample(9000);
    let mut watcher = registry
        .watch("order-service")
        .await
        .expect("watch must succeed");

    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    let mut roundtrip = false;
    for _ in 0..24 {
        let seen = match registry.get_service("order-service").await {
            Ok(instances) => instances.iter().any(|instance| {
                instance.id == "order-01"
                    && instance.endpoints == registration.instance.endpoints
                    && instance.version == "v1.0.0"
            }),
            Err(_) => false,
        };
        if seen {
            roundtrip = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    assert!(
        roundtrip,
        "the registered instance must round trip through the watched cache"
    );

    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
    let mut gone = false;
    for _ in 0..24 {
        let still_listed = match registry.get_service("order-service").await {
            Ok(instances) => instances.iter().any(|instance| instance.id == "order-01"),
            Err(_) => false,
        };
        if !still_listed {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    assert!(gone, "the deregistered instance must leave the cache");
    watcher.stop();
}

/// The reactive path: the watcher's cache updates deliver both the
/// registration and the deregistration.
#[tokio::test]
async fn watcher_delivers_addition_then_removal() {
    let registry = EurekaRegistry::connect(&[endpoint()]).expect("eureka connects");
    let registration = sample(9001);
    let mut watcher = registry
        .watch("order-service")
        .await
        .expect("watch must succeed");
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    let mut delivered = false;
    for _ in 0..24 {
        let snapshot = match tokio::time::timeout(Duration::from_secs(30), watcher.next()).await {
            Ok(Ok(instances)) => instances,
            _ => continue,
        };
        if snapshot.iter().any(|instance| {
            instance.id == "order-01"
                && instance.endpoints == registration.instance.endpoints
                && instance.version == "v1.0.0"
        }) {
            delivered = true;
            break;
        }
    }
    assert!(
        delivered,
        "the watcher must deliver the registered instance, endpoint and version intact"
    );

    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
    let mut removed = false;
    for _ in 0..24 {
        let snapshot = match tokio::time::timeout(Duration::from_secs(30), watcher.next()).await {
            Ok(Ok(instances)) => instances,
            _ => continue,
        };
        if snapshot.iter().all(|instance| instance.id != "order-01") {
            removed = true;
            break;
        }
    }
    assert!(removed, "the watcher must deliver the removal");
    watcher.stop();
}
