//! Compile-time embedded configuration source for RushWind.
//!
//! The carrier is a static key→bytes tree fed by
//! [`include_bytes!`](std::include_bytes!) /
//! [`include_str!`](std::include_str!) — configuration baked into the
//! binary: immutable, zero I/O, always present, immune to a missing
//! file on the deploy target.
//!
//! ```
//! use rushwind_config_embedded::EmbeddedSource;
//!
//! // In a deployment: .with_str("app.yaml", include_str!("../config/app.yaml"))
//! let source = EmbeddedSource::new()
//!     .with_str("app/default.yaml", "name: rushwind\n")
//!     .with("app/logo.svg", &b"<svg/>"[..]);
//!
//! assert!(source.keys().contains(&"app/default.yaml"));
//! assert!(source.keys().contains(&"app/logo.svg"));
//! ```
//!
//! No watch capability: the tree is fixed when the binary is linked,
//! and the default `NotWatchable` answer is the honest one. The
//! composition this engine exists for: an [`EmbeddedSource`] as the
//! always-present floor of a [`FallbackSource`](rushwind_config::FallbackSource)
//! — file/env/http layers override it, but the process never boots
//! without its defaults.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;

use rushwind_config::{BoxFuture, ConfigError, Source};

/// The compile-time embedded configuration source: a static tree of
/// key → bytes, grown at construction time.
#[derive(Debug, Default)]
pub struct EmbeddedSource {
    entries: HashMap<&'static str, &'static [u8]>,
}

impl EmbeddedSource {
    /// An empty tree.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a key with raw bytes — the `include_bytes!` carrier:
    ///
    /// ```ignore
    /// EmbeddedSource::new().with("schema.sql", include_bytes!("schema.sql"))
    /// ```
    #[must_use]
    pub fn with(mut self, key: &'static str, value: &'static [u8]) -> Self {
        self.entries.insert(key, value);
        self
    }

    /// Adds a key with a string value — the `include_str!` carrier:
    ///
    /// ```ignore
    /// EmbeddedSource::new().with_str("app.yaml", include_str!("app.yaml"))
    /// ```
    #[must_use]
    pub fn with_str(self, key: &'static str, value: &'static str) -> Self {
        self.with(key, value.as_bytes())
    }

    /// Builds the tree from key/value pairs in one call.
    #[must_use]
    pub fn from_pairs(pairs: &[(&'static str, &'static [u8])]) -> Self {
        let mut source = Self::new();
        for (key, value) in pairs {
            source = source.with(key, value);
        }
        source
    }

    /// The keys the tree holds, in unspecified order.
    pub fn keys(&self) -> Vec<&'static str> {
        self.entries.keys().copied().collect()
    }
}

impl Source for EmbeddedSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
        Box::pin(async move {
            if key.is_empty() {
                return Err(ConfigError::Failed(
                    "embedded: no key specified".to_string(),
                ));
            }
            // The static bytes copy out per read — the value is small and
            // the contract owns its answer.
            Ok(self.entries.get(key).map(|bytes| bytes.to_vec()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tree serves what was planted; missing keys read absent.
    #[tokio::test]
    async fn tree_round_trips() {
        let source = EmbeddedSource::new()
            .with_str("app.yaml", "name: rushwind\n")
            .with("bin", &[1u8, 2, 3]);
        assert_eq!(
            source.load("app.yaml").await.unwrap(),
            Some(b"name: rushwind\n".to_vec())
        );
        assert_eq!(source.load("bin").await.unwrap(), Some(vec![1, 2, 3]));
        assert_eq!(source.load("missing").await.unwrap(), None);
    }

    /// An empty key is a caller error, not an absent answer.
    #[tokio::test]
    async fn empty_key_refuses() {
        let source = EmbeddedSource::new();
        assert!(source.load("").await.is_err());
    }

    /// `from_pairs` plants the whole tree at once; later plants win.
    #[tokio::test]
    async fn from_pairs_and_last_write_wins() {
        let source = EmbeddedSource::from_pairs(&[
            ("a", b"first" as &[u8]),
            ("b", b"second"),
            ("a", b"override"),
        ]);
        assert_eq!(source.load("a").await.unwrap().unwrap(), b"override");
        assert_eq!(source.load("b").await.unwrap().unwrap(), b"second");
        assert_eq!(source.keys().len(), 2);
    }

    /// Not watchable — the honest default for a link-time tree.
    #[tokio::test]
    async fn not_watchable() {
        use rushwind_config::ConfigError;
        let source = EmbeddedSource::new();
        assert!(matches!(
            source.watch_value("app.yaml").await,
            Err(ConfigError::NotWatchable)
        ));
    }
}
