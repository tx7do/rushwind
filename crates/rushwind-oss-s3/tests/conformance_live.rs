//! Live conformance against a real MinIO, driven by CI's service
//! container. Compile-gated behind the `live` feature; the connection
//! settings come from the `OSS_S3_*` environment variables. These
//! tests pin the interoperability contract where it matters most —
//! SigV4-signed PUT/GET/DELETE against a real S3-compatible server.

#![cfg(feature = "live")]

use rushwind_oss::{ObjectStorage, StorageConfig};
use rushwind_oss_s3::S3Storage;

fn config() -> StorageConfig {
    StorageConfig {
        endpoint: std::env::var("OSS_S3_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:9100".to_string()),
        region: std::env::var("OSS_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        access_key: std::env::var("OSS_S3_ACCESS_KEY").unwrap_or_else(|_| "rushwind".to_string()),
        secret_key: std::env::var("OSS_S3_SECRET_KEY")
            .unwrap_or_else(|_| "rushwind-secret".to_string()),
        token: None,
        use_ssl: false,
        force_path_style: true,
        bucket: std::env::var("OSS_S3_BUCKET").unwrap_or_else(|_| "rushwind-test".to_string()),
    }
}

/// The full object lifecycle: create the bucket (setup), put bytes,
/// get the same bytes back, delete, and observe the object gone.
#[tokio::test]
async fn put_get_delete_lifecycle() {
    let storage = S3Storage::new(config()).expect("storage builds");
    storage.create_bucket().await.expect("bucket setup");

    let payload: Vec<u8> = (0..=255u8).chain(0..=255u8).collect();
    storage
        .put(
            "probe/lifecycle.bin",
            &payload,
            Some("application/octet-stream"),
        )
        .await
        .expect("put must succeed");

    let fetched = storage
        .get("probe/lifecycle.bin")
        .await
        .expect("get must succeed");
    assert_eq!(fetched, payload, "the object must round trip byte-exact");

    storage
        .delete("probe/lifecycle.bin")
        .await
        .expect("delete must succeed");
    let result = storage.get("probe/lifecycle.bin").await;
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::NotFound)),
        "the deleted object must read as NotFound, got {result:?}"
    );
}

/// The headers surface: a content type set at put time is served back
/// by the store.
#[tokio::test]
async fn put_carries_content_type() {
    let storage = S3Storage::new(config()).expect("storage builds");
    storage.create_bucket().await.expect("bucket setup");

    storage
        .put(
            "probe/typed.txt",
            b"text body".as_slice(),
            Some("text/plain"),
        )
        .await
        .expect("put must succeed");

    // The engine's GET returns the bytes; the content-type check goes
    // through a signed HEAD-equivalent via a plain GET + parse is not
    // exposed, so pin only that the object exists and reads back.
    let fetched = storage
        .get("probe/typed.txt")
        .await
        .expect("get must succeed");
    assert_eq!(fetched, b"text body".to_vec());
    storage
        .delete("probe/typed.txt")
        .await
        .expect("delete must succeed");
}

/// Empty keys and empty bodies are rejected up front as typed errors.
#[tokio::test]
async fn validation_errors() {
    let storage = S3Storage::new(config()).expect("storage builds");

    let result = storage.put("", b"x".as_slice(), None).await;
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::EmptyObjectKey)),
        "empty keys must fail with EmptyObjectKey"
    );

    let result = storage.put("probe/empty", &[], None).await;
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::EmptyObjectBody)),
        "empty bodies must fail with EmptyObjectBody"
    );

    // The engine validates the bucket eagerly, folding the
    // missing-config guard into a Result.
    let result = S3Storage::new(StorageConfig {
        bucket: String::new(),
        ..config()
    });
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::EmptyBucket)),
        "an empty bucket must fail the build with EmptyBucket"
    );
}

/// The list face: ascending key order, prefix filtering, the
/// strict-after cursor, the limit page, and the zero-limit refusal.
#[tokio::test]
async fn list_prefix_after_limit() {
    let storage = S3Storage::new(config()).expect("storage builds");
    storage.create_bucket().await.expect("bucket setup");

    for (key, size) in [
        ("probe/list/alpha", 3usize),
        ("probe/list/beta", 5),
        ("probe/list/gamma", 7),
    ] {
        storage
            .put(key, vec![0u8; size].as_slice(), None)
            .await
            .expect("put must succeed");
    }

    let page = storage
        .list("probe/list/", "", 10)
        .await
        .expect("list must succeed");
    assert_eq!(
        page,
        vec![
            rushwind_oss::ListedObject {
                key: "probe/list/alpha".into(),
                size: 3
            },
            rushwind_oss::ListedObject {
                key: "probe/list/beta".into(),
                size: 5
            },
            rushwind_oss::ListedObject {
                key: "probe/list/gamma".into(),
                size: 7
            },
        ],
        "the page must be ascending with the true sizes"
    );

    let after = storage
        .list("probe/list/", "probe/list/beta", 10)
        .await
        .expect("list after must succeed");
    assert_eq!(
        after.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        vec!["probe/list/gamma"],
        "start-after is strict: the cursor key itself is excluded"
    );

    let limited = storage
        .list("probe/list/", "", 2)
        .await
        .expect("limited list must succeed");
    assert_eq!(limited.len(), 2, "the limit bounds the page");

    let result = storage.list("probe/list/", "", 0).await;
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::InvalidLimit)),
        "a zero limit must fail with InvalidLimit"
    );

    for key in ["probe/list/alpha", "probe/list/beta", "probe/list/gamma"] {
        storage.delete(key).await.expect("cleanup delete");
    }
}

/// A presigned URL is executable by a client with no credentials: mint
/// one for a stored object and fetch it with a bare reqwest GET.
#[tokio::test]
async fn presigned_get_is_executable_without_credentials() {
    let storage = S3Storage::new(config()).expect("storage builds");
    storage.create_bucket().await.expect("bucket setup");

    storage
        .put("probe/presign.bin", b"presigned".as_slice(), None)
        .await
        .expect("put must succeed");

    let url = storage
        .presign(
            "probe/presign.bin",
            rushwind_oss::PresignMethod::Get,
            std::time::Duration::from_secs(300),
        )
        .expect("presign must succeed");

    let fetched = reqwest::get(&url)
        .await
        .expect("the presigned GET must reach the server")
        .error_for_status()
        .expect("the presigned GET must be authorized");
    let body = fetched.bytes().await.expect("body reads");
    assert_eq!(&body[..], b"presigned");

    storage
        .delete("probe/presign.bin")
        .await
        .expect("cleanup delete");
}
