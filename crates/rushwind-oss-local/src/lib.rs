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

use std::sync::atomic::{AtomicU64, Ordering};

use rushwind_oss::{ListedObject, MultipartUploadId, ObjectStorage, PresignMethod, StorageError};

/// The staging directory multipart uploads write their parts into,
/// under the bucket root.
const MULTIPART_DIR: &str = ".rushwind-multipart";

/// The counter backing the multipart upload id mint — nanos alone can
/// collide across two begins within the same nanosecond on coarse
/// clocks.
static MULTIPART_COUNTER: AtomicU64 = AtomicU64::new(0);

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

    /// The staging directory of an upload id, guarded: minted ids are
    /// plain names, so anything carrying a separator or a `..` is not a
    /// handle this engine minted — `NotFound`, without ever resolving
    /// the path.
    fn multipart_dir(&self, id: &str) -> Result<PathBuf, StorageError> {
        if id.is_empty() || id.contains('/') || id.contains('\\') || id.contains("..") {
            return Err(StorageError::NotFound);
        }
        Ok(self.root.join(MULTIPART_DIR).join(id))
    }

    /// Validates a multipart part number against S3's 1..=10000 bound —
    /// the same rule every engine speaks.
    fn validate_part_number(part_number: usize) -> Result<(), StorageError> {
        if (1..=10000).contains(&part_number) {
            Ok(())
        } else {
            Err(StorageError::InvalidPart(format!(
                "part number {part_number} is outside 1..=10000"
            )))
        }
    }

    /// The complete face's shape rule: non-empty, ascending part
    /// numbers contiguous from 1 — the assembly order and the
    /// numbering rule in one check.
    fn validate_parts_shape(parts: &[(usize, String)]) -> Result<(), StorageError> {
        if parts.is_empty() {
            return Err(StorageError::InvalidPart(
                "complete needs at least one part".to_string(),
            ));
        }
        let contiguous = parts
            .iter()
            .enumerate()
            .all(|(index, (number, _))| *number == index + 1);
        if !contiguous {
            return Err(StorageError::InvalidPart(
                "parts must be numbered contiguously from 1 in ascending order".to_string(),
            ));
        }
        Ok(())
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

    fn multipart_begin<'a>(
        &'a self,
        key: &'a str,
    ) -> rushwind_oss::BoxFuture<'a, Result<MultipartUploadId, StorageError>> {
        Box::pin(async move {
            self.resolve(key)?;
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default();
            let counter = MULTIPART_COUNTER.fetch_add(1, Ordering::SeqCst);
            let id = format!("mp-{nanos}-{counter}");
            let staging = self.multipart_dir(&id)?;
            tokio::fs::create_dir_all(&staging)
                .await
                .map_err(|e| StorageError::Failed(format!("storage multipart mkdir: {e}")))?;
            Ok(MultipartUploadId(id))
        })
    }

    fn multipart_put<'a>(
        &'a self,
        key: &'a str,
        upload: &'a MultipartUploadId,
        part_number: usize,
        body: &'a [u8],
    ) -> rushwind_oss::BoxFuture<'a, Result<String, StorageError>> {
        Box::pin(async move {
            self.resolve(key)?;
            Self::validate_part_number(part_number)?;
            if body.is_empty() {
                return Err(StorageError::EmptyObjectBody);
            }
            let staging = self.multipart_dir(&upload.0)?;
            let part_path = staging.join(part_number.to_string());
            if !staging.exists() {
                return Err(StorageError::NotFound);
            }
            tokio::fs::write(&part_path, body)
                .await
                .map_err(|e| StorageError::Failed(format!("storage multipart write: {e}")))?;
            // The part token is the byte size: completion verifies the
            // staged part still matches what was uploaded.
            Ok(body.len().to_string())
        })
    }

    fn multipart_complete<'a>(
        &'a self,
        key: &'a str,
        upload: &'a MultipartUploadId,
        parts: &'a [(usize, String)],
    ) -> rushwind_oss::BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let path = self.resolve(key)?;
            Self::validate_parts_shape(parts)?;
            let staging = self.multipart_dir(&upload.0)?;
            if !staging.exists() {
                return Err(StorageError::NotFound);
            }
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| StorageError::Failed(format!("storage multipart mkdir: {e}")))?;
            }
            // Assembly streams part by part: memory holds one part at a
            // time, not the object.
            let mut object = tokio::fs::File::create(&path)
                .await
                .map_err(|e| StorageError::Failed(format!("storage multipart create: {e}")))?;
            use tokio::io::AsyncWriteExt;
            for (part_number, token) in parts {
                let part_path = staging.join(part_number.to_string());
                let bytes = tokio::fs::read(&part_path)
                    .await
                    .map_err(|e| match e.kind() {
                        std::io::ErrorKind::NotFound => StorageError::NotFound,
                        _ => StorageError::Failed(format!("storage multipart read: {e}")),
                    })?;
                let expected = token.parse::<u64>().map_err(|_| {
                    StorageError::Failed(format!(
                        "storage multipart: part {part_number} carries an unrecognizable token"
                    ))
                })?;
                if bytes.len() as u64 != expected {
                    return Err(StorageError::Failed(format!(
                        "storage multipart: part {part_number} no longer matches its token"
                    )));
                }
                object
                    .write_all(&bytes)
                    .await
                    .map_err(|e| StorageError::Failed(format!("storage multipart write: {e}")))?;
            }
            object
                .flush()
                .await
                .map_err(|e| StorageError::Failed(format!("storage multipart flush: {e}")))?;
            tokio::fs::remove_dir_all(&staging)
                .await
                .map_err(|e| StorageError::Failed(format!("storage multipart cleanup: {e}")))?;
            Ok(())
        })
    }

    fn multipart_abort<'a>(
        &'a self,
        key: &'a str,
        upload: &'a MultipartUploadId,
    ) -> rushwind_oss::BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            self.resolve(key)?;
            let staging = self.multipart_dir(&upload.0)?;
            match tokio::fs::remove_dir_all(&staging).await {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(StorageError::NotFound),
                Err(e) => Err(StorageError::Failed(format!(
                    "storage multipart abort: {e}"
                ))),
            }
        })
    }
}
