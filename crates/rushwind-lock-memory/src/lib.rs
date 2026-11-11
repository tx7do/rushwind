//! In-process engine for the RushWind lock contract.
//!
//! A `Mutex<HashMap<key, (token, expiry)>>` with lazy expiry checks —
//! `acquire`/`release`/`extend` compare `Instant::now()` against the
//! stored deadline; nothing sweeps. Ownership is the engine-minted
//! random token, so a lapsed holder's release answers `false` exactly
//! like the distributed engines.
//!
//! **Single-process only**: the "distributed" in distributed lock is a
//! lie here — the use is tests and single-instance deployments that
//! want the lock discipline without the infrastructure. Every engine
//! passes the same conformance suite, so swapping in the redis engine
//! later is drop-in.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::future::ready;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rushwind_lock::{BoxFuture, LockError, LockHandle, LockManager};
use serde::Deserialize;
use uuid::Uuid;

/// The in-process engine's settings — none today; the type exists so
/// the settings wire stays parseable.
#[derive(Debug, Default, Deserialize)]
struct MemoryLockSettings {}

/// The in-process lock manager.
#[derive(Debug, Default)]
pub struct MemoryLockManager {
    locks: Mutex<HashMap<String, (String, Instant)>>,
}

impl MemoryLockManager {
    /// An empty manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// the in-process engine has no knobs — `{}` (unknown fields are
    /// ignored, matching serde's default).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, LockError> {
        let _settings: MemoryLockSettings = serde_json::from_value(settings)
            .map_err(|e| LockError::Failed(format!("settings parse: {e}")))?;
        Ok(Self::new())
    }

    /// Refuses the un-usable TTLs up front — the shared prelude of
    /// every operation that takes one.
    fn check_ttl(ttl: Duration) -> Result<(), LockError> {
        if ttl.is_zero() {
            return Err(LockError::InvalidTtl(
                "a lock always carries a positive ttl".to_string(),
            ));
        }
        Ok(())
    }
}

impl LockManager for MemoryLockManager {
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<LockHandle>, LockError>> {
        Box::pin(ready(self.acquire_now(key, ttl)))
    }

    fn release<'a>(&'a self, lock: &'a LockHandle) -> BoxFuture<'a, Result<bool, LockError>> {
        Box::pin(ready(self.release_now(lock)))
    }

    fn extend<'a>(
        &'a self,
        lock: &'a LockHandle,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LockError>> {
        Box::pin(ready(self.extend_now(lock, ttl)))
    }

    fn close(&self) -> BoxFuture<'_, Result<(), LockError>> {
        self.locks.lock().expect("lock map poisoned").clear();
        Box::pin(ready(Ok(())))
    }
}

impl MemoryLockManager {
    /// The synchronous acquire the future face completes immediately.
    fn acquire_now(&self, key: &str, ttl: Duration) -> Result<Option<LockHandle>, LockError> {
        Self::check_ttl(ttl)?;
        let token = Uuid::new_v4().to_string();
        let now = Instant::now();
        let mut locks = self.locks.lock().expect("lock map poisoned");
        // Free means absent, or held by an expiry that already lapsed.
        let free = locks.get(key).map_or(true, |(_, expiry)| *expiry <= now);
        if !free {
            return Ok(None);
        }
        locks.insert(key.to_string(), (token.clone(), now + ttl));
        Ok(Some(LockHandle {
            key: key.to_string(),
            token,
        }))
    }

    /// The synchronous release the future face completes immediately.
    fn release_now(&self, lock: &LockHandle) -> Result<bool, LockError> {
        let now = Instant::now();
        let mut locks = self.locks.lock().expect("lock map poisoned");
        match locks.get(&lock.key) {
            Some((token, expiry)) if token == &lock.token && *expiry > now => {
                locks.remove(&lock.key);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// The synchronous extend the future face completes immediately.
    fn extend_now(&self, lock: &LockHandle, ttl: Duration) -> Result<bool, LockError> {
        Self::check_ttl(ttl)?;
        let now = Instant::now();
        let mut locks = self.locks.lock().expect("lock map poisoned");
        match locks.get_mut(&lock.key) {
            Some((token, expiry)) if token == &lock.token && *expiry > now => {
                *expiry = now + ttl;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tokens are fresh per acquire — two successive holds never share
    /// one.
    #[tokio::test]
    async fn tokens_are_fresh() {
        let manager = MemoryLockManager::new();
        let first = manager
            .acquire("job", Duration::from_millis(10))
            .await
            .expect("acquire")
            .expect("free");
        manager.release(&first).await.expect("release");
        let second = manager
            .acquire("job", Duration::from_millis(10))
            .await
            .expect("acquire")
            .expect("free again");
        assert_ne!(first.token, second.token);
    }
}
