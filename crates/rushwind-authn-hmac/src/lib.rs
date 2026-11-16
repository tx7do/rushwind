//! HMAC engine for the Rust authentication contract.
//!
//! Credentials are HMAC-SHA256 request signatures riding as bearer
//! tokens:
//!
//! ```text
//! Authorization: Bearer <keyID>.<timestamp>.<signature>
//! ```
//!
//! where `signature = hex(HMAC-SHA256(secret, "<keyID>.<timestamp>"))`.
//! The timestamp is Unix seconds. Validation:
//!
//! 1. split into three non-empty components;
//! 2. reject timestamps outside the clock-skew window (default 5 min,
//!    both directions — a stale timestamp and a future one both expire);
//! 3. resolve the secret for `keyID` — a [`resolver`] callback or a
//!    static table;
//! 4. recompute the HMAC and compare in constant time
//!    ([`Mac::verify_slice`](hmac::Mac::verify_slice)).
//!
//! A valid signature authenticates the `keyID` as the `sub` claim and
//! nothing else.
//!
//! The signature covers only `keyID` and `timestamp` — not the request
//! body or path. It proves possession of the secret at a moment in time,
//! not the integrity of any request; pair it with transport-level
//! integrity (TLS) and per-endpoint body signing where replay or
//! tampering matters.
//!
//! Design note: minting reports a missing subject as
//! [`AuthnError::InvalidSubject`] and an unresolvable secret as
//! [`AuthnError::MissingKeyFunc`] instead of plain errors —
//! the observable outcome, failure, is unchanged.
//!
//! [`resolver`]: HmacOptions::with_secret_resolver

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit, Mac};
use rushwind_authn::{AuthClaims, Authenticator, AuthnError, CLAIM_FIELD_SUBJECT, SCHEME_BEARER};
use sha2::Sha256;

/// A callback resolving one key ID to its HMAC secret. This is the seam
/// for key rotation and per-key secrets held in an external source.
pub type SecretResolver = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// The default clock-skew window: 5 minutes.
const DEFAULT_MAX_SKEW: Duration = Duration::from_secs(300);

/// Builder for [`HmacAuthenticator`].
#[derive(Default)]
pub struct HmacOptions {
    secrets: HashMap<String, String>,
    resolver: Option<SecretResolver>,
    max_skew: Option<Duration>,
}

impl HmacOptions {
    /// Options with no secrets and the default clock skew.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one key-ID/secret pair to the static table.
    pub fn with_secret(mut self, key_id: &str, secret: &str) -> Self {
        self.secrets.insert(key_id.to_string(), secret.to_string());
        self
    }

    /// Replaces the static secret table.
    pub fn with_secrets(mut self, secrets: HashMap<String, String>) -> Self {
        self.secrets = secrets;
        self
    }

    /// Installs the resolver callback; it takes precedence over the
    /// static table.
    pub fn with_secret_resolver(mut self, resolver: SecretResolver) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Sets the clock-skew window. A zero or absent setting keeps the
    /// default.
    pub fn with_max_skew(mut self, skew: Duration) -> Self {
        if !skew.is_zero() {
            self.max_skew = Some(skew);
        }
        self
    }
}

/// An HMAC-signature authenticator.
pub struct HmacAuthenticator {
    options: HmacOptions,
}

impl HmacAuthenticator {
    /// Builds the engine from its options.
    pub fn new(options: HmacOptions) -> Self {
        Self { options }
    }

    /// The effective secret source: resolver callback first, else the
    /// static table.
    fn resolve_secret(&self, key_id: &str) -> Option<String> {
        if let Some(resolver) = &self.options.resolver {
            return resolver(key_id);
        }
        self.options.secrets.get(key_id).cloned()
    }

    /// The effective skew window: the configured value when positive,
    /// else the 5-minute default.
    fn effective_max_skew(&self) -> Duration {
        self.options
            .max_skew
            .filter(|skew| !skew.is_zero())
            .unwrap_or(DEFAULT_MAX_SKEW)
    }
}

impl Authenticator for HmacAuthenticator {
    fn scheme(&self) -> &'static str {
        SCHEME_BEARER
    }

    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError> {
        // 1. Structure: three non-empty dot-separated components.
        let mut parts = token.splitn(3, '.');
        let (Some(key_id), Some(timestamp), Some(signature), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(AuthnError::InvalidToken);
        };
        if key_id.is_empty() || timestamp.is_empty() || signature.is_empty() {
            return Err(AuthnError::InvalidToken);
        }

        // 2. Freshness: the timestamp must sit inside the skew window,
        //    both directions.
        let Ok(ts) = timestamp.parse::<i64>() else {
            return Err(AuthnError::InvalidToken);
        };
        let now = unix_now();
        if now.abs_diff(ts) > self.effective_max_skew().as_secs() {
            return Err(AuthnError::TokenExpired);
        }

        // 3. Secret resolution.
        let Some(secret) = self.resolve_secret(key_id) else {
            return Err(AuthnError::Unauthenticated);
        };

        // 4. Recompute and compare in constant time. A non-hex signature
        //    simply never matches.
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .expect("hmac-sha256 accepts any key length");
        mac.update(format!("{key_id}.{timestamp}").as_bytes());
        let Some(signature_bytes) = hex_decode(signature) else {
            return Err(AuthnError::Unauthenticated);
        };
        if mac.verify_slice(&signature_bytes).is_err() {
            return Err(AuthnError::Unauthenticated);
        }

        let mut map = serde_json::Map::new();
        map.insert(
            CLAIM_FIELD_SUBJECT.to_string(),
            serde_json::Value::String(key_id.to_string()),
        );
        Ok(AuthClaims(map))
    }

    fn create_identity(&self, claims: &AuthClaims) -> Result<String, AuthnError> {
        // Minting maps a missing subject to InvalidSubject and an
        // unresolvable secret to MissingKeyFunc.
        let key_id = claims.get_subject().unwrap_or_default();
        if key_id.is_empty() {
            return Err(AuthnError::InvalidSubject);
        }
        let Some(secret) = self.resolve_secret(&key_id) else {
            return Err(AuthnError::MissingKeyFunc);
        };
        let timestamp = unix_now().to_string();
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .expect("hmac-sha256 accepts any key length");
        mac.update(format!("{key_id}.{timestamp}").as_bytes());
        let signature = hex_encode(&mac.finalize().into_bytes());
        Ok(format!("{key_id}.{timestamp}.{signature}"))
    }
}

type HmacSha256 = Hmac<Sha256>;

/// Unix seconds now.
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Lowercase hex encoding.
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

/// Lowercase hex decoding; `None` on any
/// non-hex character, uppercase hex letters, or odd length.
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 || s.bytes().any(|b| b.is_ascii_uppercase()) {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_authn::{AuthnError, HEADER_AUTHORIZE, SCHEME_BEARER};

    fn static_secret() -> HmacOptions {
        HmacOptions::new().with_secret("key-1", "super-secret")
    }

    fn minted_token(auth: &HmacAuthenticator, key_id: &str) -> String {
        let mut map = serde_json::Map::new();
        map.insert(
            CLAIM_FIELD_SUBJECT.to_string(),
            serde_json::Value::String(key_id.to_string()),
        );
        auth.create_identity(&AuthClaims(map)).unwrap()
    }

    #[test]
    fn minted_tokens_round_trip_against_static_secrets() {
        let auth = HmacAuthenticator::new(static_secret());
        let token = minted_token(&auth, "key-1");
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "key-1");
    }

    #[test]
    fn minted_tokens_round_trip_against_the_resolver() {
        let auth =
            HmacAuthenticator::new(HmacOptions::new().with_secret_resolver(Arc::new(|key_id| {
                (key_id == "key-1").then(|| "resolved-secret".to_string())
            })));
        let token = minted_token(&auth, "key-1");
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "key-1");
    }

    #[test]
    fn wrong_shape_is_an_invalid_token() {
        let auth = HmacAuthenticator::new(static_secret());
        for token in ["just-a-string", "..", "a.b", "a.b.c.d"] {
            assert_eq!(
                auth.authenticate_token(token).unwrap_err(),
                AuthnError::InvalidToken
            );
        }
    }

    #[test]
    fn non_numeric_timestamps_are_invalid_tokens() {
        let auth = HmacAuthenticator::new(static_secret());
        let token = "key-1.not-a-number.deadbeef";
        assert_eq!(
            auth.authenticate_token(token).unwrap_err(),
            AuthnError::InvalidToken
        );
    }

    #[test]
    fn stale_timestamps_expire() {
        let auth = HmacAuthenticator::new(static_secret());
        // A timestamp a day old, signed with the right secret.
        let stale = unix_now() - 86_400;
        let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
        mac.update(format!("key-1.{stale}").as_bytes());
        let token = format!("key-1.{stale}.{}", hex_encode(&mac.finalize().into_bytes()));
        assert_eq!(
            auth.authenticate_token(&token).unwrap_err(),
            AuthnError::TokenExpired
        );
    }

    #[test]
    fn future_timestamps_expire() {
        let auth = HmacAuthenticator::new(static_secret());
        let future = unix_now() + 3_600;
        let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
        mac.update(format!("key-1.{future}").as_bytes());
        let token = format!(
            "key-1.{future}.{}",
            hex_encode(&mac.finalize().into_bytes())
        );
        assert_eq!(
            auth.authenticate_token(&token).unwrap_err(),
            AuthnError::TokenExpired
        );
    }

    #[test]
    fn tampered_signatures_are_unauthenticated() {
        let auth = HmacAuthenticator::new(static_secret());
        let token = minted_token(&auth, "key-1");
        let mut parts: Vec<String> = token.split('.').map(|s| s.to_string()).collect();
        // Flip the first hex digit of the signature.
        let sig = parts[2].clone();
        let flipped = if sig.starts_with('0') {
            sig.replacen('0', "1", 1)
        } else {
            sig.replacen(&sig[..1], "0", 1)
        };
        parts[2] = flipped;
        let tampered = parts.join(".");
        assert_eq!(
            auth.authenticate_token(&tampered).unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn unknown_key_ids_are_unauthenticated() {
        let auth = HmacAuthenticator::new(static_secret());
        let ts = unix_now().to_string();
        let mut mac = HmacSha256::new_from_slice(b"some-secret").unwrap();
        mac.update(format!("unknown-key.{ts}").as_bytes());
        let token = format!(
            "unknown-key.{ts}.{}",
            hex_encode(&mac.finalize().into_bytes())
        );
        assert_eq!(
            auth.authenticate_token(&token).unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn missing_configuration_is_unauthenticated() {
        // A fresh timestamp passes the skew window; the missing secret
        // is what rejects. (A stale timestamp would hit TokenExpired
        // first — freshness is checked before secrets.)
        let auth = HmacAuthenticator::new(HmacOptions::new());
        let ts = unix_now().to_string();
        assert_eq!(
            auth.authenticate_token(&format!("key-1.{ts}.deadbeef"))
                .unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn mint_requires_a_subject_and_a_secret() {
        let auth = HmacAuthenticator::new(static_secret());
        assert_eq!(
            auth.create_identity(&AuthClaims::new()).unwrap_err(),
            AuthnError::InvalidSubject
        );
        // A subject with no secret configured for it.
        let mut map = serde_json::Map::new();
        map.insert(
            CLAIM_FIELD_SUBJECT.to_string(),
            serde_json::Value::String("nobody".to_string()),
        );
        assert_eq!(
            auth.create_identity(&AuthClaims(map)).unwrap_err(),
            AuthnError::MissingKeyFunc
        );
    }

    #[test]
    fn authenticate_collapses_missing_credentials() {
        let auth = HmacAuthenticator::new(static_secret());
        assert_eq!(
            auth.authenticate(&[]).unwrap_err(),
            AuthnError::MissingBearerToken
        );
        let token = minted_token(&auth, "key-1");
        let headers = vec![(
            HEADER_AUTHORIZE.to_string(),
            format!("{SCHEME_BEARER} {token}"),
        )];
        assert_eq!(
            auth.authenticate(&headers).unwrap().get_subject().unwrap(),
            "key-1"
        );
    }

    #[test]
    fn hex_round_trips() {
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(
            hex_decode("deadbeef").unwrap(),
            vec![0xde, 0xad, 0xbe, 0xef]
        );
        assert!(hex_decode("nothex").is_none());
        assert!(hex_decode("abc").is_none());
    }
}
