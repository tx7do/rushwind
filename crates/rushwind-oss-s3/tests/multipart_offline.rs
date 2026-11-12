//! Offline multipart pins: the four actions are pure signing shapes —
//! no server needed to pin the wire URLs, the part-number bounds, and
//! the assembly rules. The lifecycle rides the live MinIO suite.

use rushwind_oss::{MultipartUploadId, ObjectStorage, StorageConfig, StorageError};
use rushwind_oss_s3::S3Storage;

fn config() -> StorageConfig {
    StorageConfig {
        endpoint: "127.0.0.1:9100".to_string(),
        region: "us-east-1".to_string(),
        access_key: "AKIDEXAMPLE".to_string(),
        secret_key: "secret".to_string(),
        token: None,
        use_ssl: false,
        force_path_style: true,
        bucket: "bucket".to_string(),
    }
}

fn storage() -> S3Storage {
    S3Storage::new(config()).expect("storage builds")
}

#[test]
fn part_numbers_are_bounded_before_any_wire_touch() {
    let storage = storage();
    let upload = MultipartUploadId("id".to_string());
    for bad in [0usize, 10001] {
        let err = storage
            .multipart_put("k", &upload, bad, b"x".as_slice())
            .now_or_never()
            .expect("validation fails before any request")
            .unwrap_err();
        assert!(
            matches!(err, StorageError::InvalidPart(ref m) if m.contains(&bad.to_string())),
            "{bad}: {err:?}"
        );
    }
}

#[test]
fn empty_parts_and_gaps_refuse_offline() {
    let storage = storage();
    let upload = MultipartUploadId("id".to_string());

    let err = storage
        .multipart_complete("k", &upload, &[])
        .now_or_never()
        .expect("empty parts fail before any request")
        .unwrap_err();
    assert!(matches!(err, StorageError::InvalidPart(ref m) if m.contains("at least one part")));

    let gapped = vec![(1usize, "a".to_string()), (3usize, "b".to_string())];
    let err = storage
        .multipart_complete("k", &upload, &gapped)
        .now_or_never()
        .expect("a gap fails before any request")
        .unwrap_err();
    assert!(matches!(err, StorageError::InvalidPart(ref m) if m.contains("contiguously")));
}

/// An empty part body is the same refusal the plain put gives.
#[test]
fn empty_part_bodies_refuse() {
    let storage = storage();
    let upload = MultipartUploadId("id".to_string());
    let err = storage
        .multipart_put("k", &upload, 1, &[])
        .now_or_never()
        .expect("empty bodies fail before any request")
        .unwrap_err();
    assert!(matches!(err, StorageError::EmptyObjectBody));
}

#[test]
fn empty_keys_refuse_every_multipart_face() {
    let storage = storage();
    let upload = MultipartUploadId("id".to_string());
    assert!(matches!(
        storage.multipart_begin("").now_or_never().unwrap(),
        Err(StorageError::EmptyObjectKey)
    ));
    assert!(matches!(
        storage
            .multipart_put("", &upload, 1, b"x".as_slice())
            .now_or_never()
            .unwrap(),
        Err(StorageError::EmptyObjectKey)
    ));
    assert!(matches!(
        storage
            .multipart_complete("", &upload, &[(1usize, "1".to_string())])
            .now_or_never()
            .unwrap(),
        Err(StorageError::EmptyObjectKey)
    ));
    assert!(matches!(
        storage.multipart_abort("", &upload).now_or_never().unwrap(),
        Err(StorageError::EmptyObjectKey)
    ));
}

/// The `now_or_never` shim: the multipart futures are validation-first,
/// so the refusal cases resolve without polling a runtime.
trait NowOrNever {
    type Output;
    fn now_or_never(self) -> Option<Self::Output>;
}

impl<T> NowOrNever for rushwind_oss::BoxFuture<'_, Result<T, StorageError>> {
    type Output = Result<T, StorageError>;
    fn now_or_never(self) -> Option<Self::Output> {
        use std::future::Future;
        use std::task::{Context, Poll};
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut pinned = Box::pin(self);
        match pinned.as_mut().poll(&mut cx) {
            Poll::Ready(outcome) => Some(outcome),
            Poll::Pending => None,
        }
    }
}
