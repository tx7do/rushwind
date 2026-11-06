//! The error taxonomy for authorization: one variant per failure mode in
//! the request path.
//!
//! Both variants carry an HTTP status (403) and a stable
//! machine-readable code, exposed through [`AuthzError::status`] and
//! [`AuthzError::code`].
//!
//! Engines produce these checks' inputs at the middleware layer — the
//! engine methods themselves always succeed; the taxonomy rides along
//! with the trait so a future middleware layer returns the same surface.

use std::fmt;

/// An authorization failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AuthzError {
    /// The request context carries no authz claims to evaluate.
    MissingAuthClaims,
    /// The authz claims are malformed.
    InvalidClaims,
    /// An installed policy was rejected — a rule whose `effect` is not
    /// `allow` or `deny` (an effect only ever evaluated as allow would
    /// silently widen access; the install fails instead).
    InvalidPolicy,
}

impl AuthzError {
    /// The HTTP status anchored to this error.
    pub const fn status(&self) -> u16 {
        403
    }

    /// The stable machine-readable code for this error.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::MissingAuthClaims => "AUTHZ_MISSING_CLAIMS",
            Self::InvalidClaims => "AUTHZ_INVALID_CLAIMS",
            Self::InvalidPolicy => "AUTHZ_INVALID_POLICY",
        }
    }
}

impl fmt::Display for AuthzError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Self::MissingAuthClaims => "context missing authz claims",
            Self::InvalidClaims => "invalid claims",
            Self::InvalidPolicy => "policy rejected: rule effect must be \"allow\" or \"deny\"",
        };
        write!(f, "{}: {}", self.code(), msg)
    }
}

impl std::error::Error for AuthzError {}
