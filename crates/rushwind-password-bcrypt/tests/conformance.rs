//! The password contract conformance suite, run against the bcrypt
//! engine. The factory uses the algorithm's minimum cost (the suite
//! pins semantics, not costs — the engine's own tests pin the default
//! and the policy thresholds).

use std::sync::Arc;

use rushwind_password::PasswordHasher;
use rushwind_password_bcrypt::BcryptHasher;

fn hasher() -> Arc<dyn PasswordHasher> {
    Arc::new(BcryptHasher::with_cost(4).expect("minimum cost is valid"))
}

rushwind_testkit::rushwind_password_conformance_suite!(crate::hasher);
