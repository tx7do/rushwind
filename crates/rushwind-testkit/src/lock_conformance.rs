//! The lock conformance suite.
//!
//! [`rushwind_lock_conformance_suite!`] expands to a module of
//! `#[tokio::test]` functions pinning the [`LockManager`](rushwind_lock::LockManager)
//! contract on a concrete engine: try-once acquire, token-checked
//! release/extend, TTL lapse, and the error faces. The suite uses
//! **real short TTLs** (tens of milliseconds, like the cache suite) —
//! engine-agnostic and wall-clock honest.
//!
//! # Wiring it up
//!
//! The consuming crate needs the feature-gated testkit plus tokio:
//!
//! ```toml
//! [dev-dependencies]
//! rushwind-testkit = { version = "0.0.1", features = ["lock"] }
//! tokio = { version = "1", features = ["macros", "rt", "time"] }
//! ```
//!
//! Then, in the engine crate's `tests/conformance.rs`, define the
//! factory at the test-crate root and reference it by crate-rooted
//! path:
//!
//! ```ignore
//! use std::sync::Arc;
//! use rushwind_lock::LockManager;
//!
//! async fn manager() -> Arc<dyn LockManager> {
//!     Arc::new(MyEngine::new().await.expect("connect"))
//! }
//!
//! rushwind_testkit::rushwind_lock_conformance_suite!(crate::manager);
//! ```
//!
//! `$factory` must produce a **fresh** manager per call; the suite
//! keys every test uniquely, so instances may share a backend.

/// Generates the lock conformance suite for a
/// [`LockManager`](rushwind_lock::LockManager) implementation.
///
/// `$factory` must be a **crate-rooted path** to an async function of
/// type `async fn() -> Arc<dyn LockManager>` (e.g. `crate::manager`).
#[macro_export]
macro_rules! rushwind_lock_conformance_suite {
    ($factory:expr) => {
        mod rushwind_lock_conformance {
            //! The shared lock conformance tests; each function pins
            //! one slice of the locking contract semantics.

            use std::sync::Arc;
            use std::time::Duration;

            use $crate::rushwind_lock::{LockError, LockHandle, LockManager};

            async fn manager() -> Arc<dyn LockManager> {
                $factory().await
            }

            /// Acquire is try-once: the first take succeeds, a second
            /// against the held key reads `None`, not an error.
            #[tokio::test]
            async fn acquire_is_try_once() {
                let manager = manager().await;
                let held = manager
                    .acquire("conformance:try-once", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                    .expect("the key was free");
                assert_eq!(held.key, "conformance:try-once");
                assert!(!held.token.is_empty(), "the token is never empty");

                match manager
                    .acquire("conformance:try-once", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                {
                    None => {}
                    Some(second) => panic!("a held key must not be taken: {second:?}"),
                }
            }

            /// Release by the owner frees the key: the next acquire
            /// succeeds immediately, with a fresh token.
            #[tokio::test]
            async fn release_frees_for_the_next_holder() {
                let manager = manager().await;
                let held = manager
                    .acquire("conformance:release", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                    .expect("the key was free");
                assert!(
                    manager.release(&held).await.expect("release must succeed"),
                    "release by the owner must free"
                );
                let next = manager
                    .acquire("conformance:release", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                    .expect("released key is free again");
                assert_ne!(next.token, held.token);
            }

            /// Release with a forged token answers `false` and never
            /// deletes the real holder's lock.
            #[tokio::test]
            async fn forged_release_does_not_steal() {
                let manager = manager().await;
                let held = manager
                    .acquire("conformance:forged", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                    .expect("the key was free");
                let forged = LockHandle {
                    key: held.key.clone(),
                    token: format!("forged-{}", held.token),
                };
                assert!(
                    !manager
                        .release(&forged)
                        .await
                        .expect("release must succeed"),
                    "a forged token must not free the lock"
                );
                match manager
                    .acquire("conformance:forged", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                {
                    None => {}
                    Some(stolen) => panic!("the real holder's lock survived: {stolen:?}"),
                }
            }

            /// Release of a key nobody holds answers `false`, not an
            /// error.
            #[tokio::test]
            async fn release_of_unheld_key_is_false() {
                let manager = manager().await;
                let ghost = LockHandle {
                    key: "conformance:never-acquired".to_string(),
                    token: "ghost".to_string(),
                };
                assert!(
                    !manager.release(&ghost).await.expect("release must succeed"),
                    "releasing an unheld key reads false"
                );
            }

            /// The TTL is the liveness: once it lapses the key is free
            /// again, and the lapsed holder's release answers `false`.
            #[tokio::test]
            async fn ttl_lapse_frees_the_key() {
                let manager = manager().await;
                let held = manager
                    .acquire("conformance:ttl", Duration::from_millis(60))
                    .await
                    .expect("acquire must succeed")
                    .expect("the key was free");

                tokio::time::sleep(Duration::from_millis(200)).await;
                let successor = manager
                    .acquire("conformance:ttl", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                    .expect("the lapsed ttl freed the key");
                assert_ne!(successor.token, held.token);
                assert!(
                    !manager.release(&held).await.expect("release must succeed"),
                    "the lapsed holder must not free the successor's lock"
                );
            }

            /// Extend pushes the expiry a full new TTL out: a sleep
            /// past the original budget does not free the key.
            #[tokio::test]
            async fn extend_prolongs_the_hold() {
                let manager = manager().await;
                let held = manager
                    .acquire("conformance:extend", Duration::from_millis(80))
                    .await
                    .expect("acquire must succeed")
                    .expect("the key was free");
                assert!(
                    manager
                        .extend(&held, Duration::from_millis(2_000))
                        .await
                        .expect("extend must succeed"),
                    "extend by the owner must succeed"
                );

                // Past the original 80 ms budget — only the extension
                // keeps the key held.
                tokio::time::sleep(Duration::from_millis(300)).await;
                match manager
                    .acquire("conformance:extend", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                {
                    None => {}
                    Some(stolen) => panic!("extend must have prolonged the hold: {stolen:?}"),
                }
                assert!(
                    manager.release(&held).await.expect("release must succeed"),
                    "the extending holder still owns the key"
                );
            }

            /// Extend after the TTL lapsed answers `false` — the caller
            /// must re-acquire.
            #[tokio::test]
            async fn extend_after_lapse_is_false() {
                let manager = manager().await;
                let held = manager
                    .acquire("conformance:extend-lapsed", Duration::from_millis(50))
                    .await
                    .expect("acquire must succeed")
                    .expect("the key was free");
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert!(
                    !manager
                        .extend(&held, Duration::from_secs(30))
                        .await
                        .expect("extend must succeed"),
                    "a lapsed hold cannot extend"
                );
            }

            /// Distinct keys are independent locks.
            #[tokio::test]
            async fn distinct_keys_are_independent() {
                let manager = manager().await;
                let first = manager
                    .acquire("conformance:key-a", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                    .expect("free");
                let second = manager
                    .acquire("conformance:key-b", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                    .expect("a different key is a different lock");
                assert_ne!(first.key, second.key);
                assert!(manager.release(&first).await.expect("release must succeed"));
                assert!(manager
                    .release(&second)
                    .await
                    .expect("release must succeed"));
            }

            /// A zero TTL refuses with `InvalidTtl` — there is no
            /// forever-lock — on acquire and on extend alike.
            #[tokio::test]
            async fn zero_ttl_refuses() {
                let manager = manager().await;
                match manager
                    .acquire("conformance:zero-ttl", Duration::ZERO)
                    .await
                {
                    Err(LockError::InvalidTtl(_)) => {}
                    other => panic!("zero-ttl acquire must refuse: {other:?}"),
                }
                let held = manager
                    .acquire("conformance:zero-ttl", Duration::from_secs(30))
                    .await
                    .expect("acquire must succeed")
                    .expect("free");
                match manager.extend(&held, Duration::ZERO).await {
                    Err(LockError::InvalidTtl(_)) => {}
                    other => panic!("zero-ttl extend must refuse: {other:?}"),
                }
            }
        }
    };
}
