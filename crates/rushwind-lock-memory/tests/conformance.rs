//! The lock contract conformance suite, run against the in-process
//! engine.

use std::sync::Arc;

use rushwind_lock::LockManager;
use rushwind_lock_memory::MemoryLockManager;

async fn manager() -> Arc<dyn LockManager> {
    Arc::new(MemoryLockManager::new())
}

rushwind_testkit::rushwind_lock_conformance_suite!(crate::manager);
