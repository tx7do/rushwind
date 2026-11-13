//! OpenID Connect engine for the RushWind authentication contract —
//! the deployable resolution of the security doc's deferred row:
//! verification is **local against cached keys** (sync pure CPU, so it
//! sits in the session gate chain exactly like the JWT engine), while
//! the remote IO — discovery and the JWKS fetch — happens at
//! construction and on explicit refresh. No token introspection HTTP
//! on the hot path: an OIDC ID token is a JWT, its signature is
//! verifiable with the issuer's published public keys.
//!
//! # The flow
//!
//! [`OidcAuthenticator::connect`] runs the IO half: the issuer's
//! `/.well-known/openid-configuration` discovery document names the
//! `jwks_uri` (an explicit [`OidcOptions::jwks_uri`] override skips
//! discovery for issuers that publish none); the JWKS fetch mints one
//! cached [`jsonwebtoken::DecodingKey`] per `kid`. Verification then
//! rides [`Authenticator::authenticate_token`]: parse the header's
//! `kid`, look up the cached key, validate the signature and the
//! registered claims (`iss` pinned to the configured issuer, `aud`
//! pinned to the configured audience, `exp` required — an expired or
//! audience-mismatched token refuses, tokens signed by an unknown or
//! rotated-out `kid` refuse until [`OidcAuthenticator::refresh`]
//! adopts the issuer's new keys).
//!
//! [`OidcAuthenticator::refresh`] re-fetches the JWKS and **replaces**
//! the cache — a rotation is atomic to the readers (the cache swap is
//! a single `Arc` publish). [`OidcAuthenticator::spawn_refresh`] is
//! the convenience half: a background task refreshing on an interval,
//! tied to no lifecycle (the deployment owns its own cancellation by
//! dropping the handle it gets back).
//!
//! # Minting
//!
//! [`Authenticator::create_identity`] refuses with
//! [`AuthnError::MissingKeyFunc`]: an OIDC engine holds no issuer
//! signing key — minting is the identity provider's act, never the
//! relying party's.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use rushwind_authn::{AuthClaims, Authenticator, AuthnError, SCHEME_BEARER};
use serde::Deserialize;

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct OidcOptions {
    /// The issuer URL — the discovery base and the pinned `iss` claim.
    pub issuer: String,
    /// The audience this deployment identifies as (the OIDC `client_id`
    /// the tokens were issued to).
    pub audience: String,
    /// The JWKS URL, overriding the discovery document. `None` runs
    /// discovery first.
    pub jwks_uri: Option<String>,
    /// The per-request timeout for discovery and JWKS fetches.
    /// Default 10 s.
    pub request_timeout: Duration,
    /// The leeway granted to time-based claim checks (`exp`/`nbf`),
    /// in seconds. Default 60 — clock skew between the issuer and this
    /// deployment is real.
    pub leeway_seconds: u64,
}

impl Default for OidcOptions {
    fn default() -> Self {
        Self {
            issuer: String::new(),
            audience: String::new(),
            jwks_uri: None,
            request_timeout: Duration::from_secs(10),
            leeway_seconds: 60,
        }
    }
}

/// The discovery document's fields this engine consumes.
#[derive(Debug, Deserialize)]
struct DiscoveryWire {
    #[serde(rename = "jwks_uri")]
    jwks_uri: String,
}

/// The cached verification state: one decoding key per `kid`, plus the
/// algorithm each key's JWK declared. Cloning is an `Arc` bump —
/// readers copy the cache out under a momentary read lock.
#[derive(Debug, Default, Clone)]
struct KeyCache {
    keys: Arc<HashMap<String, (DecodingKey, Algorithm)>>,
}

/// The OpenID Connect authenticator.
#[derive(Debug)]
pub struct OidcAuthenticator {
    options: OidcOptions,
    http: reqwest::Client,
    /// The lock guards only the cache swap — the fetch runs outside it,
    /// the publish inside is one assignment, so reads (the hot path)
    /// never wait on network IO.
    keys: RwLock<KeyCache>,
}

impl OidcAuthenticator {
    /// Connects: runs discovery (unless [`OidcOptions::jwks_uri`] is
    /// set) and fetches the JWKS, caching one key per `kid`. The IO
    /// half — everything after this constructor is sync.
    pub async fn connect(options: OidcOptions) -> Result<Self, AuthnError> {
        if options.issuer.is_empty() {
            return Err(AuthnError::GetKeyFailed);
        }
        if options.audience.is_empty() {
            return Err(AuthnError::InvalidAudience);
        }
        let http = reqwest::Client::builder()
            .timeout(options.request_timeout)
            .build()
            .map_err(|_| AuthnError::GetKeyFailed)?;
        let engine = Self {
            keys: RwLock::new(KeyCache::default()),
            http,
            options,
        };
        let jwks_uri = engine.resolve_jwks_uri().await?;
        let cache = engine.fetch_keys(&jwks_uri).await?;
        *engine.keys.write().expect("key cache poisoned") = cache;
        Ok(engine)
    }

    /// The configured issuer.
    pub fn issuer(&self) -> &str {
        &self.options.issuer
    }

    /// The configured audience.
    pub fn audience(&self) -> &str {
        &self.options.audience
    }

    /// Re-fetches the JWKS and atomically replaces the cached keys —
    /// the rotation half. Readers never observe a half-swapped cache
    /// (the publish is one `Arc` assignment).
    pub async fn refresh(&self) -> Result<(), AuthnError> {
        let jwks_uri = self.resolve_jwks_uri().await?;
        let cache = self.fetch_keys(&jwks_uri).await?;
        *self.keys.write().expect("key cache poisoned") = cache;
        Ok(())
    }

    /// Spawns a background task refreshing every `interval`; returns
    /// its handle (dropping it does not stop the task — abort it or
    /// own the runtime; the deployment decides). A failed refresh
    /// keeps the current cache and retries on the next tick: rotated
    /// keys never crowd out working ones on a transient blip.
    pub fn spawn_refresh(self: &Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if engine.refresh().await.is_err() {
                    // Keep the current keys; the next tick retries.
                    continue;
                }
            }
        })
    }

    /// The JWKS URL: the explicit override, or the discovery
    /// document's `jwks_uri`.
    async fn resolve_jwks_uri(&self) -> Result<String, AuthnError> {
        if let Some(uri) = &self.options.jwks_uri {
            return Ok(uri.clone());
        }
        let url = format!(
            "{}/.well-known/openid-configuration",
            self.options.issuer.trim_end_matches('/')
        );
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|_| AuthnError::GetKeyFailed)?;
        if !response.status().is_success() {
            return Err(AuthnError::GetKeyFailed);
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| AuthnError::GetKeyFailed)?;
        let discovery: DiscoveryWire =
            serde_json::from_slice(&bytes).map_err(|_| AuthnError::GetKeyFailed)?;
        Ok(discovery.jwks_uri)
    }

    /// Fetches the JWKS and builds the per-kid key cache. A JWK
    /// without a `kid`, or carrying an algorithm this installation
    /// cannot verify, is skipped — a JWKS listing five keys, one
    /// malformed, still yields four working ones.
    async fn fetch_keys(&self, jwks_uri: &str) -> Result<KeyCache, AuthnError> {
        let response = self
            .http
            .get(jwks_uri)
            .send()
            .await
            .map_err(|_| AuthnError::GetKeyFailed)?;
        if !response.status().is_success() {
            return Err(AuthnError::GetKeyFailed);
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| AuthnError::GetKeyFailed)?;
        let jwks: jsonwebtoken::jwk::JwkSet =
            serde_json::from_slice(&bytes).map_err(|_| AuthnError::GetKeyFailed)?;
        let mut keys = HashMap::new();
        for jwk in &jwks.keys {
            let Some(kid) = jwk.common.key_id.clone() else {
                continue;
            };
            let algorithm = match &jwk.common.key_algorithm {
                Some(alg) => match Algorithm::try_from(*alg) {
                    Ok(algorithm) => algorithm,
                    Err(_) => continue,
                },
                None => continue,
            };
            let Ok(key) = DecodingKey::from_jwk(jwk) else {
                continue;
            };
            keys.insert(kid, (key, algorithm));
        }
        if keys.is_empty() {
            return Err(AuthnError::GetKeyFailed);
        }
        Ok(KeyCache {
            keys: Arc::new(keys),
        })
    }
}

impl Authenticator for OidcAuthenticator {
    fn scheme(&self) -> &'static str {
        SCHEME_BEARER
    }

    /// Local verification against the cached keys: `kid` lookup,
    /// signature, and the registered claims (`iss`/`aud` pinned,
    /// `exp` required with the configured leeway). An unknown `kid`
    /// refuses as [`AuthnError::Unauthenticated`] — a rotated-out key
    /// is an authentication failure, not a configuration failure; the
    /// fix is [`OidcAuthenticator::refresh`], not an incident.
    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError> {
        let header = jsonwebtoken::decode_header(token).map_err(|_| AuthnError::InvalidToken)?;
        let kid = header.kid.ok_or(AuthnError::InvalidToken)?;
        // The momentary read lock: cloning the cache is an Arc bump, the
        // critical section never spans IO.
        let cache = self.keys.read().expect("key cache poisoned").clone();
        let Some((key, algorithm)) = cache.keys.get(&kid) else {
            return Err(AuthnError::Unauthenticated);
        };
        let mut validation = Validation::new(*algorithm);
        validation.set_issuer(&[self.options.issuer.as_str()]);
        validation.set_audience(&[self.options.audience.as_str()]);
        validation.leeway = self.options.leeway_seconds;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.required_spec_claims = ["exp".to_string()].into_iter().collect();
        // The claims decode into the raw map, exactly the JWT engine's
        // bridge: AuthClaims wraps the map without its own Deserialize.
        let decoded = jsonwebtoken::decode::<serde_json::Map<String, serde_json::Value>>(
            token,
            key,
            &validation,
        )
        .map_err(map_jwt_error)?;
        Ok(AuthClaims(decoded.claims))
    }

    /// Refused: an OIDC engine holds no issuer signing key — minting
    /// is the identity provider's act.
    fn create_identity(&self, _claims: &AuthClaims) -> Result<String, AuthnError> {
        Err(AuthnError::MissingKeyFunc)
    }
}

/// Maps the jwt crate's error taxonomy onto the contract's — the same
/// mapping the JWT engine runs.
fn map_jwt_error(error: jsonwebtoken::errors::Error) -> AuthnError {
    use jsonwebtoken::errors::ErrorKind;
    match error.kind() {
        ErrorKind::ExpiredSignature => AuthnError::TokenExpired,
        ErrorKind::InvalidSignature => AuthnError::Unauthenticated,
        ErrorKind::InvalidIssuer => AuthnError::InvalidIssuer,
        ErrorKind::InvalidAudience => AuthnError::InvalidAudience,
        ErrorKind::InvalidAlgorithm => AuthnError::UnsupportedSigningMethod,
        ErrorKind::MissingRequiredClaim(claim) if claim == "exp" => AuthnError::InvalidExpiration,
        ErrorKind::ImmatureSignature => AuthnError::TokenExpired,
        _ => AuthnError::InvalidToken,
    }
}
