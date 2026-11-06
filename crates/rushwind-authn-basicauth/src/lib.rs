//! Basic-auth engine for the RushWind authentication contract.
//!
//! Credentials follow RFC 7617:
//!
//! ```text
//! Authorization: Basic base64(username:password)
//! ```
//!
//! Validation consults a [`validator`] callback when configured — an
//! external source's verdict rendered as a local closure — else a static
//! user table ([`with_user`]). A successful validation authenticates the
//! username as the `sub` claim and nothing else.
//!
//! Minting ([`create_identity`](BasicAuthAuthenticator::create_identity))
//! base64-encodes `username:password` **from the static table** — a
//! validator-only configuration cannot mint, because the password is not
//! in this process.
//!
//! Basic auth without TLS sends the password in reversible encoding per
//! request; the RFC's own advice applies — use it inside a tunnel or for
//! internal low-stakes surfaces.
//!
//! [`validator`]: BasicAuthOptions::with_validator
//! [`with_user`]: BasicAuthOptions::with_user

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use rushwind_authn::{
    constant_time_eq, AuthClaims, Authenticator, AuthnError, CLAIM_FIELD_SUBJECT, SCHEME_BASIC,
};

/// A callback verifying one username/password pair. Return `false` to
/// reject.
pub type CredentialValidator = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

/// Builder for [`BasicAuthAuthenticator`].
#[derive(Default)]
pub struct BasicAuthOptions {
    users: HashMap<String, String>,
    validator: Option<CredentialValidator>,
}

impl BasicAuthOptions {
    /// Options with an empty user table and no validator — the engine
    /// rejects everything.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one username/password entry to the static table.
    pub fn with_user(mut self, username: &str, password: &str) -> Self {
        self.users
            .insert(username.to_string(), password.to_string());
        self
    }

    /// Replaces the static table.
    pub fn with_users(mut self, users: HashMap<String, String>) -> Self {
        self.users = users;
        self
    }

    /// Installs the validator callback; it takes precedence over the
    /// static table.
    pub fn with_validator(mut self, validator: CredentialValidator) -> Self {
        self.validator = Some(validator);
        self
    }
}

/// A basic-auth authenticator: RFC 7617 credentials against a validator
/// callback or a static user table.
pub struct BasicAuthAuthenticator {
    options: BasicAuthOptions,
}

impl BasicAuthAuthenticator {
    /// Builds the engine from its options.
    pub fn new(options: BasicAuthOptions) -> Self {
        Self { options }
    }
}

impl Authenticator for BasicAuthAuthenticator {
    fn scheme(&self) -> &'static str {
        SCHEME_BASIC
    }

    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError> {
        let decoded = BASE64_STANDARD
            .decode(token)
            .map_err(|_| AuthnError::InvalidToken)?;
        let Ok(decoded) = String::from_utf8(decoded) else {
            return Err(AuthnError::InvalidToken);
        };
        let Some((username, password)) = decoded.split_once(':') else {
            return Err(AuthnError::InvalidToken);
        };

        if !self.validate(username, password) {
            return Err(AuthnError::Unauthenticated);
        }

        let mut map = serde_json::Map::new();
        map.insert(
            CLAIM_FIELD_SUBJECT.to_string(),
            serde_json::Value::String(username.to_string()),
        );
        Ok(AuthClaims(map))
    }

    fn create_identity(&self, claims: &AuthClaims) -> Result<String, AuthnError> {
        // Minting requires a subject claim and a table entry. A missing
        // claim reports InvalidSubject and a missing table entry reports
        // MissingKeyFunc — the observable outcome, failure, is
        // unchanged.
        let username = claims.get_subject().unwrap_or_default();
        if username.is_empty() {
            return Err(AuthnError::InvalidSubject);
        }
        let Some(password) = self.options.users.get(&username) else {
            return Err(AuthnError::MissingKeyFunc);
        };
        let credential = format!("{username}:{password}");
        Ok(BASE64_STANDARD.encode(credential.as_bytes()))
    }
}

impl BasicAuthAuthenticator {
    /// Validation: validator callback first, else the static table.
    fn validate(&self, username: &str, password: &str) -> bool {
        if let Some(validator) = &self.options.validator {
            return validator(username, password);
        }
        self.options
            .users
            .get(username)
            .is_some_and(|expected| constant_time_eq(expected, password))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_authn::{HEADER_AUTHORIZE, SCHEME_BASIC, SCHEME_BEARER};

    fn encode(user: &str, pass: &str) -> String {
        BASE64_STANDARD.encode(format!("{user}:{pass}").as_bytes())
    }

    fn auth_headers(token: &str) -> Vec<(String, String)> {
        vec![(
            HEADER_AUTHORIZE.to_string(),
            format!("{SCHEME_BASIC} {token}"),
        )]
    }

    fn one_user() -> BasicAuthOptions {
        BasicAuthOptions::new().with_user("alice", "wonderland")
    }

    #[test]
    fn static_table_authenticates_the_username() {
        let auth = BasicAuthAuthenticator::new(one_user());
        let claims = auth
            .authenticate_token(&encode("alice", "wonderland"))
            .unwrap();
        assert_eq!(claims.get_subject().unwrap(), "alice");
    }

    #[test]
    fn validator_authenticates_without_a_table() {
        let auth = BasicAuthAuthenticator::new(
            BasicAuthOptions::new().with_validator(Arc::new(|u, p| u == "admin" && p == "secret")),
        );
        let claims = auth.authenticate_token(&encode("admin", "secret")).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "admin");
    }

    #[test]
    fn wrong_password_is_unauthenticated() {
        let auth = BasicAuthAuthenticator::new(one_user());
        assert_eq!(
            auth.authenticate_token(&encode("alice", "wrong"))
                .unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn unknown_user_is_unauthenticated() {
        let auth = BasicAuthAuthenticator::new(one_user());
        assert_eq!(
            auth.authenticate_token(&encode("bob", "whatever"))
                .unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn non_base64_payloads_are_invalid_tokens() {
        let auth = BasicAuthAuthenticator::new(one_user());
        assert_eq!(
            auth.authenticate_token("!!!not-base64!!!").unwrap_err(),
            AuthnError::InvalidToken
        );
    }

    #[test]
    fn colonless_payloads_are_invalid_tokens() {
        let auth = BasicAuthAuthenticator::new(one_user());
        // Valid base64 of "nopassword": no colon, no split.
        let token = BASE64_STANDARD.encode(b"nopassword");
        assert_eq!(
            auth.authenticate_token(&token).unwrap_err(),
            AuthnError::InvalidToken
        );
    }

    #[test]
    fn authenticate_extracts_and_validates() {
        let auth = BasicAuthAuthenticator::new(one_user());
        let claims = auth
            .authenticate(&auth_headers(&encode("alice", "wonderland")))
            .unwrap();
        assert_eq!(claims.get_subject().unwrap(), "alice");
    }

    #[test]
    fn authenticate_collapses_missing_or_wrong_scheme() {
        let auth = BasicAuthAuthenticator::new(one_user());
        assert_eq!(
            auth.authenticate(&[]).unwrap_err(),
            AuthnError::MissingBearerToken
        );
        // Bearer where Basic is expected: extraction
        // fails, and the collapse applies.
        let bearer = vec![(
            HEADER_AUTHORIZE.to_string(),
            format!("{SCHEME_BEARER} {}", encode("alice", "wonderland")),
        )];
        assert_eq!(
            auth.authenticate(&bearer).unwrap_err(),
            AuthnError::MissingBearerToken
        );
    }

    #[test]
    fn mint_round_trips_through_the_table() {
        let auth = BasicAuthAuthenticator::new(one_user());
        let mut map = serde_json::Map::new();
        map.insert(
            CLAIM_FIELD_SUBJECT.to_string(),
            serde_json::Value::String("alice".to_string()),
        );
        let token = auth.create_identity(&AuthClaims(map)).unwrap();
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "alice");
    }

    #[test]
    fn mint_requires_a_subject() {
        let auth = BasicAuthAuthenticator::new(one_user());
        assert_eq!(
            auth.create_identity(&AuthClaims::new()).unwrap_err(),
            AuthnError::InvalidSubject
        );
    }

    #[test]
    fn mint_requires_a_table_entry() {
        // A validator-only configuration: username "admin" authenticates
        // but cannot mint — the password lives outside this process.
        let auth = BasicAuthAuthenticator::new(
            BasicAuthOptions::new().with_validator(Arc::new(|u, p| u == "admin" && p == "secret")),
        );
        let mut map = serde_json::Map::new();
        map.insert(
            CLAIM_FIELD_SUBJECT.to_string(),
            serde_json::Value::String("admin".to_string()),
        );
        assert_eq!(
            auth.create_identity(&AuthClaims(map)).unwrap_err(),
            AuthnError::MissingKeyFunc
        );
    }
}
