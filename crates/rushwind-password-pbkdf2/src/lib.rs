//! PBKDF2-HMAC-SHA256 engine for the RushWind password contract.
//!
//! Hashes mint `pbkdf2-sha256` PHC strings
//! (`$pbkdf2-sha256$i=600000,l=32$salt$digest`) with a fresh 16-byte
//! salt per call, at the engine's cost policy — [OWASP's recommended
//! rounds](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html#pbkdf2)
//! (600,000 with HMAC-SHA-256) unless overridden at construction.
//!
//! Verification rebuilds the derivation from the **stored hash's
//! embedded parameters** (`i` rounds, `l` length, and the PRF named in
//! the algorithm id) and re-derives the digest with the stored salt,
//! comparing in constant time: a hash minted under an older, cheaper
//! policy still verifies, and [`PasswordHasher::needs_rehash`] is where
//! the current policy speaks. SHA-512 hashes verify too (a stronger
//! PRF is not a weaker policy); SHA-1 and unknown PRF ids refuse as
//! invalid hashes and read as needing an upgrade.
//!
//! PBKDF2 is CPU-bound only: the 600,000-round default costs tens of
//! milliseconds per derivation. Callers on async runtimes should wrap
//! calls in `spawn_blocking`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use password_hash::{rand_core::OsRng, Output, PasswordHash, PasswordHasher as _, SaltString};
use pbkdf2::{pbkdf2_hmac, Pbkdf2};
use rushwind_password::{PasswordError, PasswordHasher};
use sha2::{Sha256, Sha512};

/// The PBKDF2 engine.
///
/// Construct with [`Pbkdf2Hasher::new`] (OWASP-default rounds) or
/// [`Pbkdf2Hasher::with_rounds`].
#[derive(Debug, Clone)]
pub struct Pbkdf2Hasher {
    rounds: u32,
}

impl Pbkdf2Hasher {
    /// The engine at OWASP's recommended rounds: 600,000 with
    /// HMAC-SHA-256, 32-byte digest.
    pub fn new() -> Self {
        Self { rounds: 600_000 }
    }

    /// The engine at a custom round count.
    ///
    /// # Errors
    ///
    /// [`PasswordError::InvalidParams`] when `rounds` is zero.
    pub fn with_rounds(rounds: u32) -> Result<Self, PasswordError> {
        if rounds == 0 {
            return Err(PasswordError::InvalidParams(
                "rounds must be at least 1".to_string(),
            ));
        }
        Ok(Self { rounds })
    }

    /// The configured round count.
    pub fn rounds(&self) -> u32 {
        self.rounds
    }
}

impl Default for Pbkdf2Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl PasswordHasher for Pbkdf2Hasher {
    fn hash(&self, password: &str) -> Result<String, PasswordError> {
        let salt = SaltString::generate(&mut OsRng);
        Pbkdf2
            .hash_password_customized(
                password.as_bytes(),
                Some(pbkdf2::Algorithm::Pbkdf2Sha256.ident()),
                None,
                pbkdf2::Params {
                    rounds: self.rounds,
                    output_length: 32,
                },
                &salt,
            )
            .map(|h| h.to_string())
            .map_err(|e| PasswordError::Hash(e.to_string()))
    }

    fn verify(&self, password: &str, hash: &str) -> Result<bool, PasswordError> {
        let parsed = Self::parse(hash)?;
        // The stored hash's own parameters drive the derivation — never
        // the engine's current policy.
        let params = pbkdf2::Params::try_from(&parsed)
            .map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        let salt = parsed.salt.as_ref().expect("parse refuses saltless hashes");
        let digest = parsed.hash.as_ref().expect("parse refuses empty digests");
        let mut salt_buf = [0u8; 64];
        let salt_bytes = salt
            .decode_b64(&mut salt_buf)
            .map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        let mut out = vec![0u8; digest.len()];
        match parsed.algorithm.as_str() {
            "pbkdf2-sha256" => {
                pbkdf2_hmac::<Sha256>(password.as_bytes(), salt_bytes, params.rounds, &mut out)
            }
            "pbkdf2-sha512" => {
                pbkdf2_hmac::<Sha512>(password.as_bytes(), salt_bytes, params.rounds, &mut out)
            }
            other => {
                return Err(PasswordError::InvalidHash(format!(
                    "unsupported PRF: {other}"
                )));
            }
        }
        let derived = Output::new(&out).map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        Ok(parsed.hash.as_ref() == Some(&derived))
    }

    fn needs_rehash(&self, hash: &str) -> bool {
        let Ok(parsed) = PasswordHash::new(hash) else {
            return true;
        };
        // SHA-1's bare `pbkdf2` id and anything unknown upgrades.
        if !matches!(parsed.algorithm.as_str(), "pbkdf2-sha256" | "pbkdf2-sha512") {
            return true;
        }
        let Ok(params) = pbkdf2::Params::try_from(&parsed) else {
            return true;
        };
        params.rounds < self.rounds
    }

    fn algorithm(&self) -> &'static str {
        "pbkdf2-sha256"
    }
}

impl Pbkdf2Hasher {
    /// Parses a stored PHC string, refusing anything outside the
    /// pbkdf2 family. SHA-512 hashes parse fine — they verify, and
    /// [`PasswordHasher::needs_rehash`] is what flags policy distance.
    fn parse(hash: &str) -> Result<PasswordHash<'_>, PasswordError> {
        let parsed =
            PasswordHash::new(hash).map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        if !matches!(parsed.algorithm.as_str(), "pbkdf2-sha256" | "pbkdf2-sha512") {
            return Err(PasswordError::InvalidHash(format!(
                "not a pbkdf2-sha256/sha512 hash: {}",
                parsed.algorithm.as_str()
            )));
        }
        if parsed.salt.is_none() || parsed.hash.is_none() {
            return Err(PasswordError::InvalidHash(
                "hash is missing its salt or digest".to_string(),
            ));
        }
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default policy is OWASP's 600,000 rounds; the fast-policy
    /// engine round-trips (the 600k derivation itself is too expensive
    /// to repeat per assertion).
    #[test]
    fn default_policy_and_fast_round_trip() {
        assert_eq!(Pbkdf2Hasher::new().rounds(), 600_000);
        assert!(Pbkdf2Hasher::with_rounds(600_000).is_ok());

        let hasher = Pbkdf2Hasher::with_rounds(2_000).expect("fast rounds");
        let stored = hasher.hash("correct horse").expect("hash");
        assert!(hasher.verify("correct horse", &stored).expect("verify"));
        assert!(!(hasher.verify("wrong", &stored).expect("verify")));
        assert!(!hasher.needs_rehash(&stored));
    }

    /// A hash minted below the current policy verifies but reads as
    /// needing an upgrade. A SHA-512 hash verifies and is *not* weaker.
    #[test]
    fn cheaper_hash_verifies_and_needs_rehash() {
        let hasher = Pbkdf2Hasher::with_rounds(10_000).expect("policy");
        let cheap = Pbkdf2
            .hash_password_customized(
                "legacy".as_bytes(),
                Some(pbkdf2::Algorithm::Pbkdf2Sha256.ident()),
                None,
                pbkdf2::Params {
                    rounds: 1_000,
                    output_length: 32,
                },
                &SaltString::generate(&mut OsRng),
            )
            .expect("cheap hash")
            .to_string();
        assert!(hasher.verify("legacy", &cheap).expect("verify"));
        assert!(hasher.needs_rehash(&cheap));

        let sha512 = Pbkdf2
            .hash_password_customized(
                "stronger-prf".as_bytes(),
                Some(pbkdf2::Algorithm::Pbkdf2Sha512.ident()),
                None,
                pbkdf2::Params {
                    rounds: 10_000,
                    output_length: 32,
                },
                &SaltString::generate(&mut OsRng),
            )
            .expect("sha512 hash")
            .to_string();
        assert!(hasher.verify("stronger-prf", &sha512).expect("verify"));
        assert!(
            !hasher.needs_rehash(&sha512),
            "a stronger PRF is not a weaker policy"
        );
    }

    /// The bare `pbkdf2` id (the SHA-1 legacy face) refuses as an
    /// invalid hash and reads as needing an upgrade.
    #[test]
    fn sha1_legacy_face_refuses() {
        let hasher = Pbkdf2Hasher::new();
        let sha1_hash = "$pbkdf2$i=10000$l=32$YWJjZGVmZ2hpamtsbW5vcA$aGVsbG93b3JsZGhlbGxvd29ybGQ";
        assert!(matches!(
            hasher.verify("anything", sha1_hash),
            Err(PasswordError::InvalidHash(_))
        ));
        assert!(hasher.needs_rehash(sha1_hash));
    }

    /// Zero rounds are refused at construction.
    #[test]
    fn zero_rounds_are_refused() {
        assert!(matches!(
            Pbkdf2Hasher::with_rounds(0),
            Err(PasswordError::InvalidParams(_))
        ));
        assert!(Pbkdf2Hasher::with_rounds(1).is_ok());
    }

    /// A bcrypt string is a foreign scheme: verify refuses it as an
    /// invalid hash rather than reporting a wrong password.
    #[test]
    fn foreign_scheme_is_invalid_hash() {
        let hasher = Pbkdf2Hasher::new();
        let bcrypt_hash = "$2b$12$C6UzMDM.H6dfI/f/IKcEeO7ZbEJN7bErG1QZ99ZfV8Uu3tFtCKnIu";
        assert!(matches!(
            hasher.verify("anything", bcrypt_hash),
            Err(PasswordError::InvalidHash(_))
        ));
    }
}
