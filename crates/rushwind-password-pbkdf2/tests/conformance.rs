//! The password contract conformance suite, run against the pbkdf2
//! engine. The factory uses a fast-but-valid round count (the suite pins
//! semantics, not costs — the engine's own tests pin the OWASP defaults
//! and the policy thresholds).

use std::sync::Arc;

use rushwind_password::PasswordHasher;
use rushwind_password_pbkdf2::Pbkdf2Hasher;

fn hasher() -> Arc<dyn PasswordHasher> {
    Arc::new(Pbkdf2Hasher::with_rounds(1_000).expect("fast rounds are valid"))
}

rushwind_testkit::rushwind_password_conformance_suite!(crate::hasher);
