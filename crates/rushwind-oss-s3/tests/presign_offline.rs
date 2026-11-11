//! Offline presign tests: query-string signing is pure computation —
//! no server needed to pin the URL shapes, the expiry echo, and the
//! typed refusals. The signature's executability is pinned live
//! (`presigned_get_is_executable_without_credentials`).

use std::time::Duration;

use rushwind_oss::{ObjectStorage, PresignMethod, StorageConfig, StorageError};
use rushwind_oss_s3::S3Storage;

fn config(force_path_style: bool) -> StorageConfig {
    StorageConfig {
        endpoint: "127.0.0.1:9100".to_string(),
        region: "us-east-1".to_string(),
        access_key: "AKIDEXAMPLE".to_string(),
        secret_key: "secret".to_string(),
        token: None,
        use_ssl: false,
        force_path_style,
        bucket: "bucket".to_string(),
    }
}

#[test]
fn presigned_get_carries_the_full_query_signature() {
    let storage = S3Storage::new(config(true)).expect("storage builds");
    let url = storage
        .presign(
            "probe/object.bin",
            PresignMethod::Get,
            Duration::from_secs(60),
        )
        .expect("presign must succeed");

    // Path style: the bucket rides the path, the key after it.
    assert!(
        url.starts_with("http://127.0.0.1:9100/bucket/probe/object.bin?"),
        "path-style presigned URL, got {url}"
    );
    assert!(
        url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"),
        "the algorithm must be advertised, got {url}"
    );
    assert!(
        url.contains("X-Amz-Credential=AKIDEXAMPLE%2F"),
        "the credential scoping must carry the access key, got {url}"
    );
    assert!(
        url.contains("X-Amz-Expires=60"),
        "the expiry must be echoed verbatim, got {url}"
    );
    assert!(
        url.contains("X-Amz-SignedHeaders=host"),
        "host must be the only signed header, got {url}"
    );
    let signature = url
        .split("X-Amz-Signature=")
        .nth(1)
        .expect("the signature parameter must be present");
    assert_eq!(
        signature.len(),
        64,
        "the signature must be 256-bit hex, got {signature}"
    );
}

#[test]
fn virtual_host_style_puts_the_bucket_in_the_authority() {
    // Virtual-host addressing needs a name, not an IP — `bucket.127.0.0.1`
    // is not a valid host (the constructor refuses it).
    let config = StorageConfig {
        endpoint: "s3.example.com".to_string(),
        ..config(false)
    };
    let storage = S3Storage::new(config).expect("storage builds");
    let url = storage
        .presign("k", PresignMethod::Get, Duration::from_secs(60))
        .expect("presign must succeed");
    assert!(
        url.starts_with("http://bucket.s3.example.com/k?"),
        "virtual-host style must address the bucket in the authority, got {url}"
    );
}

#[test]
fn get_and_put_mint_different_grants() {
    let storage = S3Storage::new(config(true)).expect("storage builds");
    let get_url = storage
        .presign("k", PresignMethod::Get, Duration::from_secs(60))
        .unwrap();
    let put_url = storage
        .presign("k", PresignMethod::Put, Duration::from_secs(60))
        .unwrap();
    // The canonical request differs by method, so the signatures cannot
    // collide: a GET grant must never upload.
    assert_ne!(get_url, put_url);
}

#[test]
fn refusals_are_typed() {
    let storage = S3Storage::new(config(true)).expect("storage builds");

    let result = storage.presign("k", PresignMethod::Get, Duration::ZERO);
    assert!(
        matches!(result, Err(StorageError::InvalidExpiry)),
        "a zero expiry must fail with InvalidExpiry"
    );

    let result = storage.presign("", PresignMethod::Get, Duration::from_secs(60));
    assert!(
        matches!(result, Err(StorageError::EmptyObjectKey)),
        "an empty key must fail with EmptyObjectKey"
    );
}
