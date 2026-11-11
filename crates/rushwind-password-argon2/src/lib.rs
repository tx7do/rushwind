//! Argon2id engine for the RushWind password contract.
//!
//! Hashes mint Argon2id PHC strings (`$argon2id$v=19$m=…,t=…,p=…$salt$digest`)
//! with a fresh 16-byte salt per call, at the engine's cost policy —
//! [OWASP's recommended defaults](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html#argon2id)
//! (`m=19456` KiB, `t=2`, `p=1`) unless overridden at construction.
//!
//! Verification rebuilds the [`Argon2`] instance from the **stored hash's
//! embedded parameters** (algorithm id, version, m/t/p), never from the
//! engine's current policy: a hash minted under an older, cheaper policy
//! still verifies, and [`PasswordHasher::needs_rehash`] is where the
//! current policy speaks. An `argon2i`/`argon2d` hash, a `v=16` hash, or
//! one whose m/t/p sit below policy reads as "weaker" and should be
//! upgraded the next time its password verifies.
//!
//! Argon2 is memory-hard: the m=19456 default commits ~19 MiB per
//! derivation. Callers on async runtimes should wrap calls in
//! `spawn_blocking`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use argon2::{Algorithm, Argon2, Params, Version};
use password_hash::{rand_core::OsRng, Output, PasswordHash, PasswordHasher as _, SaltString};
use rushwind_password::{PasswordError, PasswordHasher};

/// The Argon2id engine.
///
/// Construct with [`Argon2Hasher::new`] (OWASP-default costs) or
/// [`Argon2Hasher::with_params`].
#[derive(Debug, Clone)]
pub struct Argon2Hasher {
    params: Params,
}

impl Argon2Hasher {
    /// The engine at OWASP's recommended costs: `m=19456` KiB, `t=2`,
    /// `p=1`.
    pub fn new() -> Self {
        // The OWASP trio is inside argon2's own validation envelope;
        // construction cannot fail for the fixed defaults.
        Self::with_params(19_456, 2, 1).expect("OWASP-default parameters are valid")
    }

    /// The engine at a custom cost policy: memory cost `m_cost` in KiB,
    /// time cost `t_cost` iterations, parallelism `p_cost` lanes.
    ///
    /// # Errors
    ///
    /// [`PasswordError::InvalidParams`] when the algorithm rejects the
    /// trio (`m_cost` below `8 * p_cost`, zero time cost, …).
    pub fn with_params(m_cost: u32, t_cost: u32, p_cost: u32) -> Result<Self, PasswordError> {
        let params = Params::new(m_cost, t_cost, p_cost, None)
            .map_err(|e| PasswordError::InvalidParams(e.to_string()))?;
        Ok(Self { params })
    }

    /// The configured cost policy as `(m_cost, t_cost, p_cost)`.
    pub fn costs(&self) -> (u32, u32, u32) {
        (
            self.params.m_cost(),
            self.params.t_cost(),
            self.params.p_cost(),
        )
    }
}

impl Default for Argon2Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl PasswordHasher for Argon2Hasher {
    fn hash(&self, password: &str) -> Result<String, PasswordError> {
        let salt = SaltString::generate(&mut OsRng);
        Argon2::new(Algorithm::Argon2id, Version::V0x13, self.params.clone())
            .hash_password(password.as_bytes(), &salt)
            .map(|h| h.to_string())
            .map_err(|e| PasswordError::Hash(e.to_string()))
    }

    fn verify(&self, password: &str, hash: &str) -> Result<bool, PasswordError> {
        let parsed = Self::parse(hash)?;
        // The stored hash's own parameters drive the derivation — never
        // the engine's current policy.
        let derived = Self::derive(password, &parsed)?;
        Ok(parsed.hash.as_ref() == Some(&derived))
    }

    fn needs_rehash(&self, hash: &str) -> bool {
        let Ok(parsed) = PasswordHash::new(hash) else {
            return true;
        };
        // Only argon2id at the current wire version meets the policy;
        // argon2i/argon2d and v=16 are legacy faces of the algorithm.
        if parsed.algorithm.as_str() != Algorithm::Argon2id.ident().as_str() {
            return true;
        }
        let Ok(params) = Params::try_from(&parsed) else {
            return true;
        };
        let (m_cost, t_cost, p_cost) = self.costs();
        params.m_cost() < m_cost || params.t_cost() < t_cost || params.p_cost() < p_cost
    }

    fn algorithm(&self) -> &'static str {
        "argon2id"
    }
}

impl Argon2Hasher {
    /// Parses a stored PHC string, refusing anything outside the argon2
    /// family. Legacy faces (argon2i/argon2d, v=16) parse fine — they
    /// verify, and [`PasswordHasher::needs_rehash`] is what flags them
    /// for upgrade.
    fn parse(hash: &str) -> Result<PasswordHash<'_>, PasswordError> {
        let parsed =
            PasswordHash::new(hash).map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        Algorithm::try_from(parsed.algorithm).map_err(|_| {
            PasswordError::InvalidHash(format!("not an argon2 hash: {}", parsed.algorithm.as_str()))
        })?;
        if parsed.salt.is_none() || parsed.hash.is_none() {
            return Err(PasswordError::InvalidHash(
                "hash is missing its salt or digest".to_string(),
            ));
        }
        Ok(parsed)
    }

    /// Re-derives the digest with the parsed hash's embedded parameters
    /// and compares it against the stored digest in constant time (the
    /// `Output` equality is the password-hash crate's constant-time one).
    fn derive(password: &str, parsed: &PasswordHash<'_>) -> Result<Output, PasswordError> {
        let algorithm = Algorithm::try_from(parsed.algorithm)
            .map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        let version_u32 = parsed
            .version
            .ok_or_else(|| PasswordError::InvalidHash("hash is missing its version".to_string()))?;
        let version = Version::try_from(version_u32)
            .map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        let params =
            Params::try_from(parsed).map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        let salt = parsed.salt.as_ref().expect("parse refuses saltless hashes");
        let mut salt_buf = [0u8; 64];
        let salt_bytes = salt
            .decode_b64(&mut salt_buf)
            .map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        let digest = parsed.hash.as_ref().expect("parse refuses empty digests");
        let mut out = vec![0u8; digest.len()];
        Argon2::new(algorithm, version, params)
            .hash_password_into(password.as_bytes(), salt_bytes, &mut out)
            .map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        Output::new(&out).map_err(|e| PasswordError::InvalidHash(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The OWASP-default engine round-trips a password.
    #[test]
    fn default_engine_round_trips() {
        let hasher = Argon2Hasher::new();
        assert_eq!(hasher.costs(), (19_456, 2, 1));
        let stored = hasher.hash("correct horse").expect("hash");
        assert!(hasher.verify("correct horse", &stored).expect("verify"));
        assert!(!(hasher.verify("wrong", &stored).expect("verify")));
    }

    /// A hash minted below the current policy verifies (parameters ride
    /// in the string) but reads as needing an upgrade.
    #[test]
    fn cheaper_hash_verifies_and_needs_rehash() {
        let hasher = Argon2Hasher::new();
        let cheap = Argon2::new(
            Algorithm::Argon2id,
            Version::V0x13,
            Params::new(64, 1, 1, None).expect("cheap params"),
        )
        .hash_password("legacy".as_bytes(), &SaltString::generate(&mut OsRng))
        .expect("cheap hash")
        .to_string();

        assert!(hasher.verify("legacy", &cheap).expect("verify"));
        assert!(hasher.needs_rehash(&cheap), "below-policy costs rehash");
        assert!(
            !hasher.needs_rehash(&hasher.hash("fresh").expect("hash")),
            "a fresh hash meets the policy"
        );
    }

    /// argon2i and v=16 hashes are legacy faces: they verify but read as
    /// needing an upgrade.
    #[test]
    fn legacy_faces_need_rehash() {
        let hasher = Argon2Hasher::new();
        for (algorithm, version) in [
            (Algorithm::Argon2i, Version::V0x13),
            (Algorithm::Argon2id, Version::V0x10),
        ] {
            let legacy = Argon2::new(
                algorithm,
                version,
                Params::new(64, 1, 1, None).expect("cheap params"),
            )
            .hash_password("legacy".as_bytes(), &SaltString::generate(&mut OsRng))
            .expect("legacy hash")
            .to_string();
            assert!(hasher.verify("legacy", &legacy).expect("verify"));
            assert!(hasher.needs_rehash(&legacy), "{legacy} reads as legacy");
        }
    }

    /// Parameter validation refuses trios the algorithm rejects.
    #[test]
    fn invalid_params_are_refused() {
        // m_cost below 8 * p_cost is out of spec.
        assert!(matches!(
            Argon2Hasher::with_params(8, 0, 4),
            Err(PasswordError::InvalidParams(_))
        ));
        assert!(Argon2Hasher::with_params(64, 1, 1).is_ok());
    }

    /// A bcrypt string is a foreign scheme: verify refuses it as an
    /// invalid hash rather than reporting a wrong password.
    #[test]
    fn foreign_scheme_is_invalid_hash() {
        let hasher = Argon2Hasher::new();
        let bcrypt_hash = "$2b$12$C6UzMDM.H6dfI/f/IKcEeO7ZbEJN7bErG1QZ99ZfV8Uu3tFtCKnIu";
        assert!(matches!(
            hasher.verify("anything", bcrypt_hash),
            Err(PasswordError::InvalidHash(_))
        ));
    }
}
