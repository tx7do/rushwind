//! Multipart upload and presigned access, engine by engine.
//!
//! The local engine runs always (a temp directory under
//! `target/oss-demo`): three parts upload out of order, complete
//! assembles by part number, abort discards a second in-flight
//! upload. The S3 engine runs when `OSS_S3_ENDPOINT` names a
//! reachable MinIO/S3 — there the demo also mints a presigned GET
//! and fetches it with no credential headers.

use rushwind_oss::{ObjectStorage, PresignMethod, StorageConfig, StorageError};
use rushwind_oss_local::LocalStorage;

async fn local_demo(root: std::path::PathBuf) {
    let store = LocalStorage::new(root);
    println!("--- local ---");

    // One assembly: parts arrive out of order, complete reorders.
    let upload = store
        .multipart_begin("demo/assembly.bin")
        .await
        .expect("begin");
    let p3 = store
        .multipart_put("demo/assembly.bin", &upload, 3, b"part-3")
        .await
        .expect("put 3");
    let p1 = store
        .multipart_put("demo/assembly.bin", &upload, 1, b"part-1")
        .await
        .expect("put 1");
    let p2 = store
        .multipart_put("demo/assembly.bin", &upload, 2, b"part-2")
        .await
        .expect("put 2");
    store
        .multipart_complete("demo/assembly.bin", &upload, &[(1, p1), (2, p2), (3, p3)])
        .await
        .expect("complete");
    let assembled = store.get("demo/assembly.bin").await.expect("get");
    println!("assembled: {}", String::from_utf8_lossy(&assembled));

    // One abort: the object never comes into existence.
    let upload = store
        .multipart_begin("demo/abandoned.bin")
        .await
        .expect("begin");
    store
        .multipart_put("demo/abandoned.bin", &upload, 1, b"junk")
        .await
        .expect("put");
    store
        .multipart_abort("demo/abandoned.bin", &upload)
        .await
        .expect("abort");
    match store.get("demo/abandoned.bin").await {
        Err(StorageError::NotFound) => println!("aborted: object never existed"),
        other => println!("unexpected: {other:?}"),
    }
}

async fn s3_demo() {
    let Ok(endpoint) = std::env::var("OSS_S3_ENDPOINT") else {
        println!("--- s3 --- (skipped: set OSS_S3_ENDPOINT et al to run)");
        return;
    };
    let config = StorageConfig {
        endpoint,
        region: std::env::var("OSS_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        access_key: std::env::var("OSS_S3_ACCESS_KEY").unwrap_or_else(|_| "rushwind".to_string()),
        secret_key: std::env::var("OSS_S3_SECRET_KEY")
            .unwrap_or_else(|_| "rushwind-secret".to_string()),
        token: None,
        use_ssl: false,
        force_path_style: true,
        bucket: std::env::var("OSS_S3_BUCKET").unwrap_or_else(|_| "rushwind-demo".to_string()),
    };
    let store = rushwind_oss_s3::S3Storage::new(config).expect("storage builds");
    store.create_bucket().await.expect("bucket setup");
    println!("--- s3 ---");

    let upload = store
        .multipart_begin("demo/multipart.bin")
        .await
        .expect("begin");
    let first = store
        .multipart_put("demo/multipart.bin", &upload, 1, b"part-1")
        .await
        .expect("put 1");
    let second = store
        .multipart_put("demo/multipart.bin", &upload, 2, b"part-2")
        .await
        .expect("put 2");
    store
        .multipart_complete("demo/multipart.bin", &upload, &[(1, first), (2, second)])
        .await
        .expect("complete");
    println!(
        "assembled: {}",
        String::from_utf8_lossy(&store.get("demo/multipart.bin").await.expect("get"))
    );

    // Presign: a URL any client can execute with no credentials.
    store
        .put(
            "demo/public.txt",
            b"fetch me without keys".as_slice(),
            Some("text/plain"),
        )
        .await
        .expect("put");
    let url = store
        .presign(
            "demo/public.txt",
            PresignMethod::Get,
            std::time::Duration::from_secs(300),
        )
        .expect("presign");
    println!("presigned GET: {url}");
    let fetched = reqwest::get(&url).await.expect("fetch");
    println!("anonymous fetch: {}", fetched.text().await.expect("body"));
}

#[tokio::main]
async fn main() {
    let root = std::path::PathBuf::from("target/oss-demo");
    std::fs::create_dir_all(&root).expect("demo root");
    local_demo(root).await;
    s3_demo().await;
}
