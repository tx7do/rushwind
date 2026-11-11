//! scrypt engine for the RushWind password contract.
//!
//! Hashes mint scrypt PHC strings (`$scrypt$ln=17,r=8,p=1$salt$digest`)
//! with a fresh 16-byte salt per call, at the engine's cost policy —
//! [OWASP's recommended parameters](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html#scrypt)
//! (`ln=17` → N=2¹⁷, `r=8`, `p=1`, 64-byte digest) unless overridden at
//! construction.
//!
//! Verification rebuilds the parameters from the **stored hash's
//! embedded `ln`/`r`/`p`** and re-derives the digest with the stored
//! salt, comparing in constant time: a hash minted under an older,
//! cheaper policy still verifies, and [`PasswordHasher::needs_rehash`]
//! is where the current policy speaks. A hash whose embedded log-N, r,
//! or p sits below policy reads as "weaker" and should be upgraded the
//! next time its password verifies.
//!
//! scrypt is memory-hard: the `ln=17, r=8` default commits ~16 MiB per
//! derivation. Callers on async runtimes should wrap calls in
//! `spawn_blocking`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use password_hash::{rand_core::OsRng, Output, PasswordHash, PasswordHasher as _, SaltString};
use rushwind_password::{PasswordError, PasswordHasher};
use scrypt::{Params, Scrypt};

/// The scrypt engine.
///
/// Construct with [`ScryptHasher::new`] (OWASP-default costs) or
/// [`ScryptHasher::with_params`].
#[derive(Debug, Clone)]
pub struct ScryptHasher {
    log_n: u8,
    r: u32,
    p: u32,
    len: usize,
}

impl ScryptHasher {
    /// The engine at OWASP's recommended costs: `ln=17`, `r=8`, `p=1`,
    /// 64-byte digest.
    pub fn new() -> Self {
        Self::with_params(17, 8, 1, 64).expect("OWASP-default parameters are valid")
    }

    /// The engine at a custom cost policy: `log_n` is log₂ of scrypt's
    /// work factor N, `r` the resource factor, `p` the parallelization,
    /// `out_len` the digest length in bytes.
    ///
    /// # Errors
    ///
    /// [`PasswordError::InvalidParams`] when the algorithm rejects the
    /// policy (`log_n ≥ 16·r`, overflowing cost products, digest length
    /// past the parameter ceiling, …).
    pub fn with_params(log_n: u8, r: u32, p: u32, out_len: usize) -> Result<Self, PasswordError> {
        Params::new(log_n, r, p, out_len)
            .map(|_| Self {
                log_n,
                r,
                p,
                len: out_len,
            })
            .map_err(|_| {
                PasswordError::InvalidParams(format!(
                    "scrypt rejects ln={log_n}, r={r}, p={p}, len={out_len}"
                ))
            })
    }

    /// The configured cost policy as `(log_n, r, p)`.
    pub fn costs(&self) -> (u8, u32, u32) {
        (self.log_n, self.r, self.p)
    }
}

impl Default for ScryptHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl PasswordHasher for ScryptHasher {
    fn hash(&self, password: &str) -> Result<String, PasswordError> {
        let params =
            Params::new(self.log_n, self.r, self.p, self.len).expect("policy already validated");
        let salt = SaltString::generate(&mut OsRng);
        Scrypt
            .hash_password_customized(password.as_bytes(), None, None, params, &salt)
            .map(|h| h.to_string())
            .map_err(|e| PasswordError::Hash(e.to_string()))
    }

    fn verify(&self, password: &str, hash: &str) -> Result<bool, PasswordError> {
        let parsed = Self::parse(hash)?;
        // The stored hash's own parameters drive the derivation — never
        // the engine's current policy.
        let params =
            Params::try_from(&parsed).map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        let salt = parsed.salt.as_ref().expect("parse refuses saltless hashes");
        let digest = parsed.hash.as_ref().expect("parse refuses empty digests");
        let mut salt_buf = [0u8; 64];
        let salt_bytes = salt
            .decode_b64(&mut salt_buf)
            .map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        let mut out = vec![0u8; digest.len()];
        scrypt::scrypt(password.as_bytes(), salt_bytes, &params, &mut out)
            .map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        let derived = Output::new(&out).map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        Ok(parsed.hash.as_ref() == Some(&derived))
    }

    fn needs_rehash(&self, hash: &str) -> bool {
        let Ok(parsed) = PasswordHash::new(hash) else {
            return true;
        };
        let Ok(params) = Params::try_from(&parsed) else {
            return true;
        };
        let (log_n, r, p) = self.costs();
        // Below-policy on any axis reads as weaker; above-policy costs
        // are stronger than policy, and stronger does not rehash.
        params.log_n() < log_n || params.r() < r || params.p() < p
    }

    fn algorithm(&self) -> &'static str {
        "scrypt"
    }
}

impl ScryptHasher {
    /// Parses a stored PHC string, refusing anything that is not a
    /// well-formed scrypt hash.
    fn parse(hash: &str) -> Result<PasswordHash<'_>, PasswordError> {
        let parsed =
            PasswordHash::new(hash).map_err(|e| PasswordError::InvalidHash(e.to_string()))?;
        if parsed.algorithm.as_str() != "scrypt" {
            return Err(PasswordError::InvalidHash(format!(
                "not a scrypt hash: {}",
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

    /// The OWASP-default engine round-trips a password. The default
    /// derivation itself is pinned exactly once (it is the expensive
    /// one); everything below runs at fast parameters.
    #[test]
    fn default_engine_round_trips() {
        let hasher = ScryptHasher::new();
        assert_eq!(hasher.costs(), (17, 8, 1));
        let stored = hasher.hash("correct horse").expect("hash");
        assert!(hasher.verify("correct horse", &stored).expect("verify"));
        assert!(!hasher.needs_rehash(&stored));
    }

    /// A hash minted below the current policy verifies but reads as
    /// needing an upgrade; above-policy costs are stronger and stay.
    #[test]
    fn cheaper_hash_verifies_and_needs_rehash() {
        let hasher = ScryptHasher::with_params(5, 8, 1, 64).expect("policy");
        let cheap = Scrypt
            .hash_password_customized(
                "legacy".as_bytes(),
                None,
                None,
                Params::new(4, 8, 1, 64).expect("cheap params"),
                &SaltString::generate(&mut OsRng),
            )
            .expect("cheap hash")
            .to_string();
        assert!(hasher.verify("legacy", &cheap).expect("verify"));
        assert!(hasher.needs_rehash(&cheap));

        let strong = Scrypt
            .hash_password_customized(
                "strong".as_bytes(),
                None,
                None,
                Params::new(6, 8, 1, 64).expect("strong params"),
                &SaltString::generate(&mut OsRng),
            )
            .expect("strong hash")
            .to_string();
        assert!(
            !hasher.needs_rehash(&strong),
            "stronger-than-policy does not rehash"
        );
    }

    /// Parameter validation refuses policies the algorithm rejects.
    #[test]
    fn invalid_params_are_refused() {
        // log_n >= 16 * r violates scrypt's own check.
        assert!(matches!(
            ScryptHasher::with_params(130, 8, 1, 64),
            Err(PasswordError::InvalidParams(_))
        ));
        assert!(ScryptHasher::with_params(4, 8, 1, 64).is_ok());
    }

    /// A bcrypt string is a foreign scheme: verify refuses it as an
    /// invalid hash rather than reporting a wrong password.
    #[test]
    fn foreign_scheme_is_invalid_hash() {
        let hasher = ScryptHasher::new();
        let bcrypt_hash = "$2b$12$C6UzMDM.H6dfI/f/IKcEeO7ZbEJN7bErG1QZ99ZfV8Uu3tFtCKnIu";
        assert!(matches!(
            hasher.verify("anything", bcrypt_hash),
            Err(PasswordError::InvalidHash(_))
        ));
    }
}
