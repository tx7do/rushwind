//! Redis engine for the RushWind lock contract.
//!
//! # The wire behavior
//!
//! [`LockManager::acquire`] is one `SET key token NX PX ttl` — the
//! canonical Redis lock take. The token is a fresh random UUID per
//! acquire, so ownership is distinguishable across workers and
//! process restarts.
//!
//! [`LockManager::release`] and [`LockManager::extend`] run as Lua
//! scripts — the compare-and-act has to be atomic on the server:
//!
//! ```lua
//! -- release
//! if redis.call("get", KEYS[1]) == ARGV[1] then
//!     return redis.call("del", KEYS[1])
//! else
//!     return 0
//! end
//! -- extend
//! if redis.call("get", KEYS[1]) == ARGV[1] then
//!     return redis.call("pexpire", KEYS[1], ARGV[2])
//! else
//!     return 0
//! end
//! ```
//!
//! A TTL already lapsed means `GET` misses (or returns the successor's
//! token) — `Ok(false)`, exactly the memory engine's answer. An
//! optional key prefix namespaces every key.
//!
//! # Testing
//!
//! Live conformance runs against a real Redis via the `live` feature
//! (`LOCK_REDIS_URL`); CI's broker integration job runs it against the
//! redis container it already starts.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use redis::{AsyncCommands, Script};
use rushwind_lock::{BoxFuture, LockError, LockHandle, LockManager};
use serde::Deserialize;
use uuid::Uuid;

/// Release only when the stored token matches the holder's.
const RELEASE_LUA: &str = r#"
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"#;

/// Extend (full new TTL) only when the stored token matches.
const EXTEND_LUA: &str = r#"
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"#;

struct Inner {
    connection: redis::aio::ConnectionManager,
    key_prefix: String,
    release: Script,
    extend: Script,
}

/// A Redis-backed lock manager over a multiplexed connection.
pub struct RedisLockManager {
    inner: Arc<Inner>,
}

impl RedisLockManager {
    /// Connects to Redis at `url` (e.g. `redis://127.0.0.1:6379`)
    /// with no key prefix.
    pub async fn connect(url: &str) -> Result<Self, LockError> {
        Self::connect_with(url, "").await
    }

    /// Connects with a key prefix prepended to every lock key.
    pub async fn connect_with(url: &str, key_prefix: &str) -> Result<Self, LockError> {
        let client = redis::Client::open(url.to_string())
            .map_err(|e| LockError::Failed(format!("redis client open: {e}")))?;
        let connection = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(|e| LockError::Failed(format!("redis connect: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                connection,
                key_prefix: key_prefix.to_string(),
                release: Script::new(RELEASE_LUA),
                extend: Script::new(EXTEND_LUA),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `url` (required), `key_prefix` (optional).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, LockError> {
        #[derive(Deserialize)]
        struct RedisLockSettings {
            url: String,
            key_prefix: Option<String>,
        }
        let settings: RedisLockSettings = serde_json::from_value(settings)
            .map_err(|e| LockError::Failed(format!("settings parse: {e}")))?;
        Self::connect_with(&settings.url, &settings.key_prefix.unwrap_or_default()).await
    }

    fn key(&self, key: &str) -> String {
        format!("{}{}", self.inner.key_prefix, key)
    }

    /// Refuses the un-usable TTLs up front.
    fn check_ttl(ttl: Duration) -> Result<u64, LockError> {
        if ttl.is_zero() {
            return Err(LockError::InvalidTtl(
                "a lock always carries a positive ttl".to_string(),
            ));
        }
        Ok(ttl.as_millis() as u64)
    }
}

impl LockManager for RedisLockManager {
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<LockHandle>, LockError>> {
        let millis = Self::check_ttl(ttl);
        Box::pin(async move {
            let millis = millis?;
            let token = Uuid::new_v4().to_string();
            let mut connection = self.inner.connection.clone();
            // SET key token NX PX ttl — one round-trip take.
            let options = redis::SetOptions::default()
                .conditional_set(redis::ExistenceCheck::NX)
                .with_expiration(redis::SetExpiry::PX(millis));
            let set: Option<String> = connection
                .set_options(self.key(key), token.clone(), options)
                .await
                .map_err(|e| LockError::Failed(format!("redis set nx px: {e}")))?;
            Ok(set.map(|_| LockHandle {
                key: key.to_string(),
                token,
            }))
        })
    }

    fn release<'a>(&'a self, lock: &'a LockHandle) -> BoxFuture<'a, Result<bool, LockError>> {
        Box::pin(async move {
            let mut connection = self.inner.connection.clone();
            let freed: i64 = self
                .inner
                .release
                .key(self.key(&lock.key))
                .arg(&lock.token)
                .invoke_async(&mut connection)
                .await
                .map_err(|e| LockError::Failed(format!("redis release script: {e}")))?;
            Ok(freed == 1)
        })
    }

    fn extend<'a>(
        &'a self,
        lock: &'a LockHandle,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LockError>> {
        let millis = Self::check_ttl(ttl);
        Box::pin(async move {
            let millis = millis?;
            let mut connection = self.inner.connection.clone();
            let extended: i64 = self
                .inner
                .extend
                .key(self.key(&lock.key))
                .arg(&lock.token)
                .arg(millis)
                .invoke_async(&mut connection)
                .await
                .map_err(|e| LockError::Failed(format!("redis extend script: {e}")))?;
            Ok(extended == 1)
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), LockError>> {
        // The connection manager stays open — it is shared infrastructure,
        // not this manager's to drop (same stance as the cache engine).
        Box::pin(std::future::ready(Ok(())))
    }
}
