//! The password contract conformance suite, run against the scrypt
//! engine. The factory uses a fast-but-valid cost policy (the suite pins
//! semantics, not costs — the engine's own tests pin the OWASP defaults
//! and the policy thresholds).

use std::sync::Arc;

use rushwind_password::PasswordHasher;
use rushwind_password_scrypt::ScryptHasher;

fn hasher() -> Arc<dyn PasswordHasher> {
    Arc::new(ScryptHasher::with_params(4, 8, 1, 64).expect("fast parameters are valid"))
}

rushwind_testkit::rushwind_password_conformance_suite!(crate::hasher);
