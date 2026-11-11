//! Live conformance against a real Redis, driven by CI's broker
//! integration job (the redis container it already starts).
//! Compile-gated behind the `live` feature; the URL comes from
//! `LOCK_REDIS_URL`.

#![cfg(feature = "live")]

use std::sync::Arc;

use rushwind_lock::LockManager;
use rushwind_lock_redis::RedisLockManager;

async fn manager() -> Arc<dyn LockManager> {
    let url =
        std::env::var("LOCK_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    // The prefix namespaces the suite's keys from anything else living
    // in the CI redis — including other runs of this suite.
    let manager = RedisLockManager::connect_with(&url, "rushwind-lock-suite:")
        .await
        .expect("redis connects");
    Arc::new(manager)
}

rushwind_testkit::rushwind_lock_conformance_suite!(crate::manager);
