//! The local-disk [`ObjectStorage`] engine — the S3 engine's twin for
//! the no-endpoint profile: one engine instance per bucket, the object
//! key relative to the bucket root, directories materializing on put
//! and missing files reading as [`StorageError::NotFound`].
//!
//! The key is the path under the bucket root, sanitized against
//! traversal (`..` segments and absolute forms are rejected before the
//! filesystem sees them — the deployments filter their upload inputs,
//! the engine does not trust that alone). The content type is accepted
//! and ignored: a filesystem stores no content-type metadata.

use std::path::{Path, PathBuf};

use rushwind_oss::{ObjectStorage, StorageError};

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

    /// The filesystem path for an object key, sanitized: absolute and
    /// `..`-carrying keys are rejected up front.
    fn resolve(&self, key: &str) -> Result<PathBuf, StorageError> {
        if key.is_empty() {
            return Err(StorageError::EmptyObjectKey);
        }
        let relative = Path::new(key);
        if relative.is_absolute()
            || relative
                .components()
                .any(|c| c == std::path::Component::ParentDir)
        {
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
}
