//! The local-disk [`ObjectStorage`] engine — the S3 engine's twin for
//! the no-endpoint profile: one engine instance per bucket, the object
//! key relative to the bucket root, directories materializing on put
//! and missing files reading as [`StorageError::NotFound`].
//!
//! The key is the path under the bucket root, sanitized against
//! traversal (`..` segments, rooted (`/x`, `\x`) and Windows
//! drive/UNC-prefixed forms are rejected before the filesystem sees
//! them — the deployments filter their upload inputs, the engine does
//! not trust that alone). The content type is accepted
//! and ignored: a filesystem stores no content-type metadata.

#![forbid(unsafe_code)]
use std::path::{Path, PathBuf};

use rushwind_oss::{ListedObject, ObjectStorage, PresignMethod, StorageError};

/// The local-disk engine for one bucket: objects land under `root`.
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    /// Builds the engine whose bucket root is `root` (the deployment's
    /// data directory joined with the bucket name).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The filesystem path for an object key, sanitized: only plain
    /// relative segments survive. A leading `/` is rooted-but-not-
    /// absolute on Windows (`is_absolute` is false there, yet
    /// `Path::join` replaces the buffer with it), a drive/UNC prefix
    /// replaces it outright, and a `..` segment walks out — each is
    /// refused before the filesystem sees them.
    fn resolve(&self, key: &str) -> Result<PathBuf, StorageError> {
        if key.is_empty() {
            return Err(StorageError::EmptyObjectKey);
        }
        let relative = Path::new(key);
        if relative.components().any(|c| {
            !matches!(
                c,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        }) {
            return Err(StorageError::Failed(format!(
                "object key escapes the bucket root: {key}"
            )));
        }
        Ok(self.root.join(relative))
    }
}

impl ObjectStorage for LocalStorage {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: &'a [u8],
        _content_type: Option<&'a str>,
    ) -> rushwind_oss::BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let path = self.resolve(key)?;
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| StorageError::Failed(format!("storage mkdir: {e}")))?;
            }
            tokio::fs::write(&path, body)
                .await
                .map_err(|e| StorageError::Failed(format!("storage write: {e}")))
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> rushwind_oss::BoxFuture<'a, Result<Vec<u8>, StorageError>> {
        Box::pin(async move {
            let path = self.resolve(key)?;
            tokio::fs::read(&path).await.map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => StorageError::NotFound,
                _ => StorageError::Failed(format!("storage read: {e}")),
            })
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> rushwind_oss::BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let path = self.resolve(key)?;
            match tokio::fs::remove_file(&path).await {
                Ok(()) => Ok(()),
                // Removing an absent object is a no-op — the S3 face's
                // delete idempotence.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(StorageError::Failed(format!("storage delete: {e}"))),
            }
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        after: &'a str,
        limit: usize,
    ) -> rushwind_oss::BoxFuture<'a, Result<Vec<ListedObject>, StorageError>> {
        Box::pin(async move {
            if limit == 0 {
                return Err(StorageError::InvalidLimit);
            }
            // Walk the bucket root; keys are the relative paths with the
            // platform separator normalized to `/` so a listing reads
            // the same key space put/get address.
            let mut keys: Vec<(String, u64)> = Vec::new();
            let mut stack = vec![self.root.clone()];
            while let Some(dir) = stack.pop() {
                let mut entries = match tokio::fs::read_dir(&dir).await {
                    Ok(entries) => entries,
                    // An absent bucket root is an empty bucket.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => {
                        return Err(StorageError::Failed(format!("storage list: {e}")));
                    }
                };
                while let Some(entry) = entries
                    .next_entry()
                    .await
                    .map_err(|e| StorageError::Failed(format!("storage list: {e}")))?
                {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                        continue;
                    }
                    let relative = path
                        .strip_prefix(&self.root)
                        .map_err(|e| StorageError::Failed(format!("storage list: {e}")))?;
                    let key = relative.to_string_lossy().replace('\\', "/");
                    let size = entry
                        .metadata()
                        .await
                        .map_err(|e| StorageError::Failed(format!("storage list: {e}")))?
                        .len();
                    keys.push((key, size));
                }
            }
            keys.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(keys
                .into_iter()
                .filter(|(key, _)| key.starts_with(prefix))
                // The strict-after cursor: S3's `start-after` semantics —
                // keys strictly greater in the same ascending order.
                .filter(|(key, _)| key.as_str() > after)
                .take(limit)
                .map(|(key, size)| ListedObject { key, size })
                .collect())
        })
    }

    fn presign(
        &self,
        key: &str,
        method: PresignMethod,
        expires: std::time::Duration,
    ) -> Result<String, StorageError> {
        let _ = (key, method, expires);
        // A `file://` path carries no signature: there is no authority
        // vouching for the grant, so the honest answer is refusal.
        Err(StorageError::Unsupported)
    }
}
