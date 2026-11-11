//! The password contract conformance suite, run against the argon2
//! engine. The factory uses a fast-but-valid cost policy (the suite pins
//! semantics, not costs — the engine's own tests pin the OWASP defaults
//! and the policy thresholds).

use std::sync::Arc;

use rushwind_password::PasswordHasher;
use rushwind_password_argon2::Argon2Hasher;

fn hasher() -> Arc<dyn PasswordHasher> {
    Arc::new(Argon2Hasher::with_params(64, 1, 1).expect("fast parameters are valid"))
}

rushwind_testkit::rushwind_password_conformance_suite!(crate::hasher);
