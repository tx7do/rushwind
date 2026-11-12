//! S3 engine for the RushWind oss contract — SigV4 query-signed REST
//! over reqwest, using `rusty-s3` for the signing. Covers AWS S3,
//! MinIO, and any S3-compatible endpoint with one engine, because
//! MinIO **is** an S3-compatible server.
//!
//! # The wire behavior
//!
//! Publishes are PUT requests, reads GET, removals DELETE. Query-
//! string signing (the presigned-URL form) keeps the request headers
//! out of the signature; the payload integrity is still enforced by
//! the required `x-amz-content-sha256` signing input rusty-s3 emits.
//!
//! Path-style addressing (`http://host/bucket/key`) suits MinIO and
//! local endpoints; virtual-host style (`http://bucket.host/key`)
//! suits AWS. Session tokens (`x-amz-security-token`) pass through
//! rusty-s3's credentials.
//!
//! [`ObjectStorage::list`] rides ListObjectsV2 (`encoding-type=url` so
//! keys arrive percent-decoded, `start-after` as the strict-after
//! cursor, `max-keys` clamped to S3's 1000 ceiling); `presign` is the
//! same query-string signing the engine already speaks — mint the URL,
//! hand it to any client, no credential headers needed.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::time::Duration;

use rushwind_oss::BoxFuture;
use rushwind_oss::{
    ListedObject, MultipartUploadId, ObjectStorage, PresignMethod, StorageConfig, StorageError,
};
use rusty_s3::actions::{
    AbortMultipartUpload, CompleteMultipartUpload, CreateMultipartUpload, ListObjectsV2, UploadPart,
};
use rusty_s3::{Bucket, Credentials};
use rusty_s3::{S3Action, UrlStyle};

struct Inner {
    http: reqwest::Client,
    bucket: Bucket,
    credentials: Credentials,
}

/// An S3-compatible object storage engine.
pub struct S3Storage {
    inner: Arc<Inner>,
}

use std::sync::Arc;

/// How long a signed URL stays valid. Query signing has no session;
/// every request is signed fresh, so this only bounds clock skew.
const SIGNATURE_VALIDITY: Duration = Duration::from_secs(3600);

impl S3Storage {
    /// Builds an engine from the connection settings, validating the
    /// bucket eagerly and folding the guard into a `Result`.
    pub fn new(config: StorageConfig) -> Result<Self, StorageError> {
        if config.bucket.is_empty() {
            return Err(StorageError::EmptyBucket);
        }
        let scheme = if config.use_ssl { "https" } else { "http" };
        let endpoint = format!("{scheme}://{}", config.endpoint.trim_end_matches('/'))
            .parse()
            .map_err(|e| StorageError::Failed(format!("s3 endpoint parse: {e}")))?;
        let url_style = if config.force_path_style {
            UrlStyle::Path
        } else {
            UrlStyle::VirtualHost
        };
        let bucket = Bucket::new(
            endpoint,
            url_style,
            config.bucket.clone(),
            config.region.clone(),
        )
        .map_err(|e| StorageError::Failed(format!("s3 bucket url: {e}")))?;
        let credentials = match &config.token {
            Some(token) => Credentials::new_with_token(
                config.access_key.clone(),
                config.secret_key.clone(),
                token.clone(),
            ),
            None => Credentials::new(config.access_key.clone(), config.secret_key.clone()),
        };
        Ok(Self {
            inner: Arc::new(Inner {
                http: reqwest::Client::new(),
                bucket,
                credentials,
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape
    /// (the [`StorageConfig`] fields, camelCase keys).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, StorageError> {
        let config: StorageConfig = serde_json::from_value(settings)
            .map_err(|e| StorageError::Failed(format!("settings parse: {e}")))?;
        Self::new(config)
    }

    /// Creates the bucket — a setup convenience beyond the core
    /// contract, useful against empty MinIO instances.
    pub async fn create_bucket(&self) -> Result<(), StorageError> {
        let action = self.inner.bucket.create_bucket(&self.inner.credentials);
        let url = action.sign(SIGNATURE_VALIDITY);
        let response = reqwest::Client::new()
            .put(url)
            .send()
            .await
            .map_err(|e| StorageError::Failed(format!("s3 create bucket: {e}")))?;
        // 409 (already exists) is a successful no-op.
        if response.status().is_success() || response.status().as_u16() == 409 {
            Ok(())
        } else {
            Err(StorageError::Failed(format!(
                "s3 create bucket: HTTP {}",
                response.status().as_u16()
            )))
        }
    }

    fn validate_key(key: &str) -> Result<(), StorageError> {
        if key.is_empty() {
            Err(StorageError::EmptyObjectKey)
        } else {
            Ok(())
        }
    }

    /// S3's own hard cap on a single list page.
    const MAX_LIST_PAGE: usize = 1000;

    /// Validates a multipart part number against S3's 1..=10000 bound.
    fn validate_part_number(part_number: usize) -> Result<u16, StorageError> {
        u16::try_from(part_number)
            .ok()
            .filter(|part| (1..=10000).contains(part))
            .ok_or_else(|| {
                StorageError::InvalidPart(format!("part number {part_number} is outside 1..=10000"))
            })
    }

    /// The complete face's tokens: ascending part numbers, contiguous
    /// from 1 — the assembly order and the numbering rule in one check.
    fn ordered_part_tokens(parts: &[(usize, String)]) -> Result<Vec<String>, StorageError> {
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
        Ok(parts.iter().map(|(_, token)| token.clone()).collect())
    }
}

impl ObjectStorage for S3Storage {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: &'a [u8],
        content_type: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            Self::validate_key(key)?;
            if body.is_empty() {
                return Err(StorageError::EmptyObjectBody);
            }
            let mut action = self
                .inner
                .bucket
                .put_object(Some(&self.inner.credentials), key);
            if let Some(content_type) = content_type {
                action
                    .headers_mut()
                    .insert("content-type", content_type.to_string());
            }
            let url = action.sign(SIGNATURE_VALIDITY);
            let mut request = self.inner.http.put(url).body(body.to_vec());
            if let Some(content_type) = content_type {
                request = request.header(reqwest::header::CONTENT_TYPE, content_type);
            }
            let response = request
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 put {key}: {e}")))?;
            if !response.status().is_success() {
                return Err(StorageError::Failed(format!(
                    "s3 put {key}: HTTP {}",
                    response.status().as_u16()
                )));
            }
            Ok(())
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StorageError>> {
        Box::pin(async move {
            Self::validate_key(key)?;
            let action = self
                .inner
                .bucket
                .get_object(Some(&self.inner.credentials), key);
            let url = action.sign(SIGNATURE_VALIDITY);
            let response = self
                .inner
                .http
                .get(url)
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 get {key}: {e}")))?;
            if response.status().as_u16() == 404 {
                return Err(StorageError::NotFound);
            }
            if !response.status().is_success() {
                return Err(StorageError::Failed(format!(
                    "s3 get {key}: HTTP {}",
                    response.status().as_u16()
                )));
            }
            response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|e| StorageError::Failed(format!("s3 get {key}: {e}")))
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            Self::validate_key(key)?;
            let action = self
                .inner
                .bucket
                .delete_object(Some(&self.inner.credentials), key);
            let url = action.sign(SIGNATURE_VALIDITY);
            let response = self
                .inner
                .http
                .delete(url)
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 delete {key}: {e}")))?;
            if !response.status().is_success() {
                return Err(StorageError::Failed(format!(
                    "s3 delete {key}: HTTP {}",
                    response.status().as_u16()
                )));
            }
            Ok(())
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        after: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<ListedObject>, StorageError>> {
        Box::pin(async move {
            if limit == 0 {
                return Err(StorageError::InvalidLimit);
            }
            let mut action = self
                .inner
                .bucket
                .list_objects_v2(Some(&self.inner.credentials));
            if !prefix.is_empty() {
                action.with_prefix(prefix);
            }
            if !after.is_empty() {
                action.with_start_after(after);
            }
            action.with_max_keys(limit.min(Self::MAX_LIST_PAGE));
            let url = action.sign(SIGNATURE_VALIDITY);
            let response = self
                .inner
                .http
                .get(url)
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 list {prefix}: {e}")))?;
            if !response.status().is_success() {
                return Err(StorageError::Failed(format!(
                    "s3 list {prefix}: HTTP {}",
                    response.status().as_u16()
                )));
            }
            let text = response
                .text()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 list {prefix}: {e}")))?;
            let parsed = ListObjectsV2::parse_response(&text)
                .map_err(|e| StorageError::Failed(format!("s3 list parse: {e}")))?;
            Ok(parsed
                .contents
                .into_iter()
                .map(|entry| ListedObject {
                    key: entry.key,
                    size: entry.size,
                })
                .collect())
        })
    }

    fn presign(
        &self,
        key: &str,
        method: PresignMethod,
        expires: Duration,
    ) -> Result<String, StorageError> {
        Self::validate_key(key)?;
        if expires.is_zero() {
            return Err(StorageError::InvalidExpiry);
        }
        let url = match method {
            PresignMethod::Get => self
                .inner
                .bucket
                .get_object(Some(&self.inner.credentials), key)
                .sign(expires),
            PresignMethod::Put => self
                .inner
                .bucket
                .put_object(Some(&self.inner.credentials), key)
                .sign(expires),
        };
        Ok(url.to_string())
    }

    fn multipart_begin<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<MultipartUploadId, StorageError>> {
        Box::pin(async move {
            Self::validate_key(key)?;
            let action =
                CreateMultipartUpload::new(&self.inner.bucket, Some(&self.inner.credentials), key);
            let url = action.sign(SIGNATURE_VALIDITY);
            let response = self
                .inner
                .http
                .post(url)
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 multipart begin {key}: {e}")))?;
            if !response.status().is_success() {
                return Err(StorageError::Failed(format!(
                    "s3 multipart begin {key}: HTTP {}",
                    response.status().as_u16()
                )));
            }
            let text = response
                .text()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 multipart begin {key}: {e}")))?;
            let parsed = CreateMultipartUpload::parse_response(&text)
                .map_err(|e| StorageError::Failed(format!("s3 multipart parse: {e}")))?;
            Ok(MultipartUploadId(parsed.upload_id().to_string()))
        })
    }

    fn multipart_put<'a>(
        &'a self,
        key: &'a str,
        upload: &'a MultipartUploadId,
        part_number: usize,
        body: &'a [u8],
    ) -> BoxFuture<'a, Result<String, StorageError>> {
        Box::pin(async move {
            Self::validate_key(key)?;
            let part = Self::validate_part_number(part_number)?;
            if body.is_empty() {
                return Err(StorageError::EmptyObjectBody);
            }
            let action = UploadPart::new(
                &self.inner.bucket,
                Some(&self.inner.credentials),
                key,
                part,
                &upload.0,
            );
            let url = action.sign(SIGNATURE_VALIDITY);
            let response = self
                .inner
                .http
                .put(url)
                .body(body.to_vec())
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 multipart put {key}/{part}: {e}")))?;
            if response.status().as_u16() == 404 {
                return Err(StorageError::NotFound);
            }
            if !response.status().is_success() {
                return Err(StorageError::Failed(format!(
                    "s3 multipart put {key}/{part}: HTTP {}",
                    response.status().as_u16()
                )));
            }
            // The part's token is S3's ETag; the quotes ride the wire and
            // come off here so the caller echoes a bare token.
            let etag = response
                .headers()
                .get("etag")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.trim_matches('"').to_string())
                .ok_or_else(|| {
                    StorageError::Failed(format!(
                        "s3 multipart put {key}/{part}: no ETag on the response"
                    ))
                })?;
            Ok(etag)
        })
    }

    fn multipart_complete<'a>(
        &'a self,
        key: &'a str,
        upload: &'a MultipartUploadId,
        parts: &'a [(usize, String)],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            Self::validate_key(key)?;
            let etags = Self::ordered_part_tokens(parts)?;
            let action = CompleteMultipartUpload::new(
                &self.inner.bucket,
                Some(&self.inner.credentials),
                key,
                &upload.0,
                etags.iter().map(String::as_str),
            );
            let url = action.sign(SIGNATURE_VALIDITY);
            let response = self
                .inner
                .http
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/xml")
                .body(action.body())
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 multipart complete {key}: {e}")))?;
            if response.status().as_u16() == 404 {
                return Err(StorageError::NotFound);
            }
            if !response.status().is_success() {
                let status = response.status().as_u16();
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("<unreadable body>"));
                return Err(StorageError::Failed(format!(
                    "s3 multipart complete {key}: HTTP {status}: {body}"
                )));
            }
            Ok(())
        })
    }

    fn multipart_abort<'a>(
        &'a self,
        key: &'a str,
        upload: &'a MultipartUploadId,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            Self::validate_key(key)?;
            let action = AbortMultipartUpload::new(
                &self.inner.bucket,
                Some(&self.inner.credentials),
                key,
                &upload.0,
            );
            let url = action.sign(SIGNATURE_VALIDITY);
            let response = self
                .inner
                .http
                .delete(url)
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 multipart abort {key}: {e}")))?;
            if response.status().as_u16() == 404 {
                return Err(StorageError::NotFound);
            }
            if !response.status().is_success() {
                return Err(StorageError::Failed(format!(
                    "s3 multipart abort {key}: HTTP {}",
                    response.status().as_u16()
                )));
            }
            Ok(())
        })
    }
}

// The ObjectStorage methods need the health_check default removed in
// favor of a head_bucket probe when the engine has a bucket.
impl S3Storage {
    /// Head-bucket probe — the setup/liveness convenience pairing
    /// with [`S3Storage::create_bucket`].
    pub async fn head_bucket(&self) -> Result<(), StorageError> {
        let action = self.inner.bucket.head_bucket(Some(&self.inner.credentials));
        let url = action.sign(SIGNATURE_VALIDITY);
        let response = reqwest::Client::new()
            .head(url)
            .send()
            .await
            .map_err(|e| StorageError::Failed(format!("s3 head bucket: {e}")))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(StorageError::Failed(format!(
                "s3 head bucket: HTTP {}",
                response.status().as_u16()
            )))
        }
    }
}
