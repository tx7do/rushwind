//! Object storage contract for RushWind: put/get/delete/list of byte
//! objects in an S3-compatible bucket, presigned direct access, and
//! the multipart upload face (begin/put/complete/abort — parts stage
//! invisibly, completion assembles in part-number order, abort
//! discards).
//!
//! # The core shapes
//!
//! One contract, [`ObjectStorage`], and one engine shape cover the
//! domain: an S3-compatible client speaks the same API against AWS S3
//! and MinIO alike (MinIO **is** an S3-compatible server), so a
//! single engine, `rushwind-oss-s3`, suffices.
//!
//! Guard-style error sentinels are unneeded here — the type system
//! makes nil inputs impossible; validation failures surface as
//! [`StorageError`] variants
//! (`EmptyBucket`, `EmptyObjectKey`, `InvalidLimit`, `InvalidExpiry`)
//! plus `NotFound` for a missing
//! object read.
//!
//! # Engines
//!
//! - `rushwind-oss-s3` — SigV4-signed REST over reqwest; works
//!   against AWS S3, MinIO, and any S3-compatible endpoint.
//! - `rushwind-oss-local` — the local-disk twin for the no-endpoint
//!   profile.
//!
//! Presigned URLs need a signing authority: the S3 engine mints them
//! (query-string signing is its native wire form), the local engine
//! answers [`StorageError::Unsupported`] — a `file://` path would
//! carry no signature, and pretending otherwise would be a false
//! grant.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Future type used across the oss contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors surfaced by object storage engines.
#[derive(Debug)]
#[non_exhaustive]
pub enum StorageError {
    /// The engine could not complete the operation.
    Failed(String),
    /// The object key does not exist.
    NotFound,
    /// The configured bucket is empty.
    EmptyBucket,
    /// The object key is empty.
    EmptyObjectKey,
    /// The object body is empty where content was required.
    EmptyObjectBody,
    /// The operation has no meaning for this engine (presigned URLs
    /// against a local disk — there is no signing authority to vouch
    /// for the grant).
    Unsupported,
    /// A list page size of zero was requested.
    InvalidLimit,
    /// A presign expiry of zero was requested — the URL would be dead
    /// at birth.
    InvalidExpiry,
    /// A multipart part is malformed: a part number outside 1..=10000,
    /// a complete call without parts or with a gap in the numbering.
    InvalidPart(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "oss operation failed: {msg}"),
            Self::NotFound => write!(f, "oss: object not found"),
            Self::EmptyBucket => write!(f, "oss: bucket is empty"),
            Self::EmptyObjectKey => write!(f, "oss: object key is empty"),
            Self::EmptyObjectBody => write!(f, "oss: object body is empty"),
            Self::Unsupported => write!(f, "oss: operation unsupported by this engine"),
            Self::InvalidLimit => write!(f, "oss: list limit must be greater than zero"),
            Self::InvalidExpiry => write!(f, "oss: presign expiry must be greater than zero"),
            Self::InvalidPart(msg) => write!(f, "oss: invalid multipart part: {msg}"),
        }
    }
}

impl std::error::Error for StorageError {}

/// The connection settings for an S3-compatible endpoint.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct StorageConfig {
    /// The S3-compatible endpoint host (without scheme), e.g.
    /// `127.0.0.1:9100`.
    pub endpoint: String,
    /// The SigV4 region (e.g. `us-east-1`).
    pub region: String,
    /// The access key id.
    pub access_key: String,
    /// The secret access key.
    pub secret_key: String,
    /// The optional session token (STS).
    pub token: Option<String>,
    /// Whether to speak HTTPS. Default: false (HTTP).
    pub use_ssl: bool,
    /// Whether to address the bucket in the path
    /// (`http://host/bucket/key`) instead of the virtual host
    /// (`http://bucket.host/key`). Required for MinIO and local
    /// endpoints. Default: false.
    pub force_path_style: bool,
    /// The bucket every operation targets.
    pub bucket: String,
}

/// The handle of an in-flight multipart upload, minted by the engine
/// at [`ObjectStorage::multipart_begin`] and echoed to every multipart
/// call. Opaque: the engine owns whatever state the id names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUploadId(
    /// The engine's opaque upload identifier.
    pub String,
);

/// One entry of a [`ObjectStorage::list`] page: the key and the byte
/// size. Keys come back in ascending byte order — the S3 list order —
/// so pagination by "everything strictly after the last key seen"
/// composes with the engine's own ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    /// The object key, relative to the bucket root.
    pub key: String,
    /// The object size in bytes.
    pub size: u64,
}

/// The HTTP method a presigned URL grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresignMethod {
    /// `GET` — direct download.
    Get,
    /// `PUT` — direct upload.
    Put,
}

/// The object storage contract.
/// Engines must be callable through shared references (`&self`).
pub trait ObjectStorage: Send + Sync {
    /// Stores the object bytes under `key`, with an optional content
    /// type.
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: &'a [u8],
        content_type: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// Reads the object bytes for `key`; `NotFound` when the object
    /// does not exist.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StorageError>>;

    /// Removes the object; a no-op when missing.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), StorageError>>;

    /// Lists object keys in ascending byte order, at most `limit` of
    /// them (engines may cap the page below `limit` — S3's own cap is
    /// 1000), strictly after `after` in that order (an empty `after`
    /// starts from the beginning) and under `prefix` (an empty prefix
    /// matches everything). The strict-after cursor plus the fixed
    /// ordering make `list(prefix, last_key_seen, limit)` a complete
    /// enumeration of any bucket. A zero `limit` is
    /// [`StorageError::InvalidLimit`].
    fn list<'a>(
        &'a self,
        prefix: &'a str,
        after: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<ListedObject>, StorageError>>;

    /// Mints a presigned URL granting `method` on `key` for `expires`.
    /// The signature rides the query string, so the grant is
    /// executable by any HTTP client with no credential headers. A
    /// zero expiry is [`StorageError::InvalidExpiry`]. The default
    /// implementation answers [`StorageError::Unsupported`] — only
    /// engines with a signing authority override it.
    fn presign(
        &self,
        key: &str,
        method: PresignMethod,
        expires: Duration,
    ) -> Result<String, StorageError> {
        let _ = (key, method, expires);
        Err(StorageError::Unsupported)
    }

    /// Probes the engine with a timeout bound for liveness checks —
    /// the default implementation is a no-op.
    fn health_check(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move { Ok(()) })
    }

    /// Opens a multipart upload for `key`: the engine mints an opaque
    /// [`MultipartUploadId`]; parts stream through
    /// [`ObjectStorage::multipart_put`] until
    /// [`ObjectStorage::multipart_complete`] assembles the object or
    /// [`ObjectStorage::multipart_abort`] discards them. Nothing is
    /// visible to `get`/`list` until completion — an in-flight upload
    /// is not an object.
    fn multipart_begin<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<MultipartUploadId, StorageError>>;

    /// Uploads one part of an in-flight upload. Part numbers live in
    /// 1..=10000 (S3's own bound); an empty body is
    /// [`StorageError::EmptyObjectBody`] and an out-of-range or
    /// unknown-upload request is refused. The returned part token (on
    /// S3, the part's ETag) is opaque — echo it back, unchanged, in
    /// [`ObjectStorage::multipart_complete`].
    fn multipart_put<'a>(
        &'a self,
        key: &'a str,
        upload: &'a MultipartUploadId,
        part_number: usize,
        body: &'a [u8],
    ) -> BoxFuture<'a, Result<String, StorageError>>;

    /// Assembles the object from the listed parts, ascending part
    /// numbers, contiguous from 1 — every token exactly as
    /// [`ObjectStorage::multipart_put`] returned it. Parts assemble in
    /// the listed order; an empty part list or a gap in the numbering
    /// is [`StorageError::InvalidPart`], a token that no longer
    /// matches its part is [`StorageError::Failed`], an unknown
    /// upload is [`StorageError::NotFound`]. On success the object
    /// reads as one body and the upload is closed.
    fn multipart_complete<'a>(
        &'a self,
        key: &'a str,
        upload: &'a MultipartUploadId,
        parts: &'a [(usize, String)],
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// Discards an in-flight upload: every uploaded part is dropped
    /// and the object is never created. A missing upload is
    /// [`StorageError::NotFound`].
    fn multipart_abort<'a>(
        &'a self,
        key: &'a str,
        upload: &'a MultipartUploadId,
    ) -> BoxFuture<'a, Result<(), StorageError>>;
}
