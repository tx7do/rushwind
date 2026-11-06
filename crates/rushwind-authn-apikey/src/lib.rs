//! API-key engine for the RushWind authentication contract.
//!
//! Keys ride as bearer tokens:
//!
//! ```text
//! Authorization: Bearer <api-key>
//! ```
//!
//! Validation consults, in order:
//!
//! 1. a [`validator`] callback, when configured — the key-to-claims
//!    decision of an external source (a database, a cache) rendered as a
//!    local closure;
//! 2. a static key set ([`with_keys`]) — membership only;
//! 3. per-key claims ([`with_key_claims`]) — claims attached to a static
//!    key; a valid key without an attachment authenticates with empty
//!    claims.
//!
//! Minting ([`create_identity`](ApiKeyAuthenticator::create_identity))
//! echoes the `sub` claim back as the key, including
//! the empty-string mint for a missing claim.
//!
//! Keys are opaque strings with no internal structure; nothing
//! distinguishes an API key from any other bearer token except this
//! engine's tables. Use per-key claims to carry the identity the key
//! maps to.
//!
//! [`validator`]: ApiKeyOptions::with_validator
//! [`with_keys`]: ApiKeyOptions::with_keys
//! [`with_key_claims`]: ApiKeyOptions::with_key_claims

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rushwind_authn::{constant_time_eq, AuthClaims, Authenticator, AuthnError, SCHEME_BEARER};

/// A callback validating one API key and producing the claims it carries.
///
/// Return `None` to reject the key.
pub type KeyValidator = Arc<dyn Fn(&str) -> Option<AuthClaims> + Send + Sync>;

/// Builder for [`ApiKeyAuthenticator`].
#[derive(Default)]
pub struct ApiKeyOptions {
    keys: Option<HashSet<String>>,
    claims: HashMap<String, AuthClaims>,
    validator: Option<KeyValidator>,
}

impl ApiKeyOptions {
    /// Options with no keys, no claims, and no validator — the engine
    /// rejects everything.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the static key set.
    pub fn with_keys(mut self, keys: &[&str]) -> Self {
        self.keys = Some(keys.iter().map(|k| (*k).to_string()).collect());
        self
    }

    /// Attaches claims to one static key; the claims are returned when
    /// that key authenticates.
    pub fn with_key_claims(mut self, key: &str, claims: AuthClaims) -> Self {
        self.claims.insert(key.to_string(), claims);
        self
    }

    /// Installs the validator callback; it takes precedence over the
    /// static tables.
    pub fn with_validator(mut self, validator: KeyValidator) -> Self {
        self.validator = Some(validator);
        self
    }
}

/// An API-key authenticator: opaque keys against a validator callback or
/// a static set.
pub struct ApiKeyAuthenticator {
    options: ApiKeyOptions,
}

impl ApiKeyAuthenticator {
    /// Builds the engine from its options.
    pub fn new(options: ApiKeyOptions) -> Self {
        Self { options }
    }
}

impl Authenticator for ApiKeyAuthenticator {
    fn scheme(&self) -> &'static str {
        SCHEME_BEARER
    }

    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError> {
        // The validator takes precedence.
        if let Some(validator) = &self.options.validator {
            return match validator(token) {
                Some(claims) => Ok(claims),
                None => Err(AuthnError::Unauthenticated),
            };
        }

        // The static key set. No set configured → reject.
        let Some(keys) = &self.options.keys else {
            return Err(AuthnError::Unauthenticated);
        };
        // Membership by constant-time scan rather than a hash-table
        // probe: the probe's timing encodes how much of the presented
        // key matched a bucket.
        if !keys
            .iter()
            .any(|candidate| constant_time_eq(candidate, token))
        {
            return Err(AuthnError::Unauthenticated);
        }

        // A valid key with attached claims returns them; a valid key
        // without an attachment returns empty claims.
        Ok(self.options.claims.get(token).cloned().unwrap_or_default())
    }

    fn create_identity(&self, claims: &AuthClaims) -> Result<String, AuthnError> {
        // Minting: the subject claim echoed as the key, empty when
        // absent, never an error.
        let subject = claims.get_subject().unwrap_or_default();
        Ok(subject)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_authn::{HEADER_AUTHORIZE, SCHEME_BEARER};

    fn subject_claims(subject: &str) -> AuthClaims {
        let mut map = serde_json::Map::new();
        map.insert(
            rushwind_authn::CLAIM_FIELD_SUBJECT.to_string(),
            serde_json::Value::String(subject.to_string()),
        );
        AuthClaims(map)
    }

    fn auth_headers(token: &str) -> Vec<(String, String)> {
        vec![(
            HEADER_AUTHORIZE.to_string(),
            format!("{SCHEME_BEARER} {token}"),
        )]
    }

    #[test]
    fn static_keys_authenticate_with_empty_claims() {
        let auth = ApiKeyAuthenticator::new(ApiKeyOptions::new().with_keys(&["key-1", "key-2"]));
        let claims = auth.authenticate_token("key-1").unwrap();
        // No claims attached: an empty bag.
        assert_eq!(claims.get_subject().unwrap(), "");
    }

    #[test]
    fn attached_claims_surface_on_authentication() {
        let auth = ApiKeyAuthenticator::new(
            ApiKeyOptions::new()
                .with_keys(&["key-1"])
                .with_key_claims("key-1", subject_claims("alice")),
        );
        let claims = auth.authenticate_token("key-1").unwrap();
        assert_eq!(claims.get_subject().unwrap(), "alice");
    }

    #[test]
    fn validator_claims_surface_on_authentication() {
        let auth =
            ApiKeyAuthenticator::new(ApiKeyOptions::new().with_validator(Arc::new(|key: &str| {
                (key == "valid-key").then(|| subject_claims("bob"))
            })));
        let claims = auth.authenticate_token("valid-key").unwrap();
        assert_eq!(claims.get_subject().unwrap(), "bob");
    }

    #[test]
    fn unknown_keys_are_unauthenticated() {
        let auth = ApiKeyAuthenticator::new(ApiKeyOptions::new().with_keys(&["key-1"]));
        assert_eq!(
            auth.authenticate_token("bad-key").unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn validator_rejections_are_unauthenticated() {
        let auth =
            ApiKeyAuthenticator::new(ApiKeyOptions::new().with_validator(Arc::new(|_| None)));
        assert_eq!(
            auth.authenticate_token("any-key").unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn missing_configuration_is_unauthenticated() {
        let auth = ApiKeyAuthenticator::new(ApiKeyOptions::new());
        assert_eq!(
            auth.authenticate_token("any-key").unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn authenticate_extracts_and_validates() {
        let auth = ApiKeyAuthenticator::new(
            ApiKeyOptions::new()
                .with_keys(&["key-1"])
                .with_key_claims("key-1", subject_claims("alice")),
        );
        let claims = auth.authenticate(&auth_headers("key-1")).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "alice");
    }

    #[test]
    fn authenticate_collapses_missing_credentials() {
        let auth = ApiKeyAuthenticator::new(ApiKeyOptions::new().with_keys(&["key-1"]));
        // No Authorization header, and a wrong-scheme header: both
        // collapse to the single missing-credential error.
        assert_eq!(
            auth.authenticate(&[]).unwrap_err(),
            AuthnError::MissingBearerToken
        );
        assert_eq!(
            auth.authenticate(&[("Authorization".to_string(), "Basic abc".to_string())])
                .unwrap_err(),
            AuthnError::MissingBearerToken
        );
    }

    #[test]
    fn authenticate_collapses_unknown_keys_to_their_own_error() {
        // Extraction succeeds; validation rejects on its own taxonomy.
        let auth = ApiKeyAuthenticator::new(ApiKeyOptions::new().with_keys(&["key-1"]));
        assert_eq!(
            auth.authenticate(&auth_headers("bad-key")).unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn mint_echoes_the_subject_claim() {
        let auth = ApiKeyAuthenticator::new(ApiKeyOptions::new().with_keys(&["key-1"]));
        assert_eq!(
            auth.create_identity(&subject_claims("key-1")).unwrap(),
            "key-1"
        );
        // No subject claim: minting returns the empty string, not an
        // error.
        assert_eq!(auth.create_identity(&AuthClaims::new()).unwrap(), "");
    }
}
