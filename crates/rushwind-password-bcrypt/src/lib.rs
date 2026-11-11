//! bcrypt engine for the RushWind password contract.
//!
//! Hashes mint bcrypt MCF strings (`$2b$<cost>$<22-char salt><31-char
//! digest>`) with a fresh 16-byte salt per call, at the engine's cost
//! policy — cost 12 unless overridden at construction (the algorithm's
//! envelope is 4..=31).
//!
//! Verification delegates to the `bcrypt` crate, whose comparison is
//! constant time and whose parameters (cost, salt) ride the stored
//! string exactly like PHC — a hash minted under an older, cheaper cost
//! still verifies, and [`PasswordHasher::needs_rehash`] is where the
//! current policy speaks. Legacy version prefixes (`$2a$`, `$2x$`,
//! `$2y$`) verify but read as needing an upgrade.
//!
//! # The 72-byte edge, stated where the deployer reads it
//!
//! The bcrypt algorithm consumes **at most 72 bytes** of input; the
//! implementation truncates longer passwords, and it truncates on both
//! the hashing and the verification side identically. The observable
//! consequence: two passwords that share their first 72 bytes and differ
//! only after them verify as the *same* password. That is the algorithm's
//! behavior, not this engine's choice — callers storing secrets longer
//! than 72 bytes should pre-hash or pick Argon2id instead.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use rushwind_password::{PasswordError, PasswordHasher};

/// The version prefixes bcrypt has shipped over the years. All of them
/// verify; only `$2b$` is what this engine mints.
const KNOWN_VERSIONS: [&str; 4] = ["2a", "2b", "2x", "2y"];

/// The bcrypt engine.
///
/// Construct with [`BcryptHasher::new`] (cost 12) or
/// [`BcryptHasher::with_cost`].
#[derive(Debug, Clone)]
pub struct BcryptHasher {
    cost: u32,
}

impl BcryptHasher {
    /// The lowest cost the algorithm accepts.
    pub const MIN_COST: u32 = 4;
    /// The highest cost the algorithm accepts.
    pub const MAX_COST: u32 = 31;

    /// The engine at cost 12 — the `bcrypt` crate's default, in the
    /// cost band OWASP's 2023 guidance lands in for interactive login.
    pub fn new() -> Self {
        Self { cost: 12 }
    }

    /// The engine at a custom cost.
    ///
    /// # Errors
    ///
    /// [`PasswordError::InvalidParams`] when `cost` is outside
    /// 4..=31.
    pub fn with_cost(cost: u32) -> Result<Self, PasswordError> {
        if !(Self::MIN_COST..=Self::MAX_COST).contains(&cost) {
            return Err(PasswordError::InvalidParams(format!(
                "cost {cost} outside the bcrypt envelope {}..={}",
                Self::MIN_COST,
                Self::MAX_COST
            )));
        }
        Ok(Self { cost })
    }

    /// The configured cost.
    pub fn cost(&self) -> u32 {
        self.cost
    }
}

impl Default for BcryptHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl PasswordHasher for BcryptHasher {
    fn hash(&self, password: &str) -> Result<String, PasswordError> {
        bcrypt::hash(password, self.cost).map_err(|e| PasswordError::Hash(e.to_string()))
    }

    fn verify(&self, password: &str, hash: &str) -> Result<bool, PasswordError> {
        // The crate's verify errors only on a malformed stored string —
        // exactly the InvalidHash face of the contract. A wrong password
        // is Ok(false).
        bcrypt::verify(password, hash).map_err(|e| PasswordError::InvalidHash(e.to_string()))
    }

    fn needs_rehash(&self, hash: &str) -> bool {
        let Some(cost) = Self::stored_cost(hash) else {
            return true;
        };
        cost < self.cost
    }

    fn algorithm(&self) -> &'static str {
        "bcrypt"
    }
}

impl BcryptHasher {
    /// Reads the cost out of an MCF string; `None` when the string is
    /// not a recognizable bcrypt hash (unknown version prefix, cost not
    /// a number, wrong segment count).
    fn stored_cost(hash: &str) -> Option<u32> {
        let segments: Vec<&str> = hash.split('$').collect();
        // ["", "2b", "12", "<22-char salt><31-char digest>"] — bcrypt's
        // MCF variant keeps salt and digest in one segment.
        if segments.len() != 4 {
            return None;
        }
        if !KNOWN_VERSIONS.contains(&segments[1]) {
            return None;
        }
        segments[2].parse::<u32>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default-cost engine round-trips a password.
    #[test]
    fn default_engine_round_trips() {
        let hasher = BcryptHasher::new();
        assert_eq!(hasher.cost(), 12);
        let stored = hasher.hash("correct horse").expect("hash");
        assert!(hasher.verify("correct horse", &stored).expect("verify"));
        assert!(!(hasher.verify("wrong", &stored).expect("verify")));
        assert!(!hasher.needs_rehash(&stored));
    }

    /// A hash minted below the current policy verifies but reads as
    /// needing an upgrade; a foreign version prefix also upgrades.
    #[test]
    fn cheaper_and_legacy_hashes_need_rehash() {
        let hasher = BcryptHasher::new();
        let cheap = bcrypt::hash("legacy", 6).expect("cheap hash");
        assert!(hasher.verify("legacy", &cheap).expect("verify"));
        assert!(hasher.needs_rehash(&cheap));

        // Re-letter the version prefix of a real hash to $2a$ — a
        // legacy face that must still verify and must upgrade.
        let legacy = format!("$2a${}", &cheap["$2b$".len()..]);
        assert!(hasher.verify("legacy", &legacy).expect("verify"));
        assert!(hasher.needs_rehash(&legacy));
    }

    /// Cost validation refuses the envelope's outside; the edges are
    /// accepted.
    #[test]
    fn cost_validation() {
        assert!(matches!(
            BcryptHasher::with_cost(3),
            Err(PasswordError::InvalidParams(_))
        ));
        assert!(matches!(
            BcryptHasher::with_cost(32),
            Err(PasswordError::InvalidParams(_))
        ));
        assert!(BcryptHasher::with_cost(4).is_ok());
        assert!(BcryptHasher::with_cost(31).is_ok());
    }

    /// Passwords beyond 72 bytes are truncated by the algorithm on both
    /// sides: a password sharing its first 72 bytes with the stored one
    /// verifies. This pins the edge the crate docs state, so a crate
    /// upgrade that changes it fails loudly here.
    #[test]
    fn past_72_bytes_passwords_are_the_same_password() {
        let hasher = BcryptHasher::with_cost(4).expect("min cost");
        let long = "a".repeat(80);
        let stored = hasher.hash(&long).expect("hash");

        let same_prefix = format!("{}tail", "a".repeat(72));
        assert!(
            hasher.verify(&same_prefix, &stored).expect("verify"),
            "byte 73+ is invisible to the algorithm"
        );
        let other = format!("{}tail", "b".repeat(72));
        assert!(
            !(hasher.verify(&other, &stored).expect("verify")),
            "a different first 72 bytes is a different password"
        );
    }

    /// A malformed stored string is an InvalidHash, not a wrong
    /// password.
    #[test]
    fn malformed_hash_is_invalid_hash() {
        let hasher = BcryptHasher::new();
        assert!(matches!(
            hasher.verify("anything", "not-a-hash"),
            Err(PasswordError::InvalidHash(_))
        ));
    }
}
