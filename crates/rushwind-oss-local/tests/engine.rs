//! The engine against a real temp directory: the put/get/delete round
//! trip, the directory materialization, the NotFound reads, the
//! idempotent delete, and the traversal refusal.

use rushwind_oss::ObjectStorage;
use rushwind_oss_local::LocalStorage;

/// A unique scratch root per test (the house pattern: temp_dir +
/// process-unique names, cleaned at the end).
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("rushwind-oss-local-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn round_trip_materializes_directories() {
    let scratch = Scratch::new("roundtrip");
    let store = LocalStorage::new(scratch.path().to_path_buf());

    store
        .put(
            "uploads/deep/dir/object.bin",
            b"payload".as_slice(),
            Some("text/plain"),
        )
        .await
        .unwrap();
    let read = store.get("uploads/deep/dir/object.bin").await.unwrap();
    assert_eq!(read, b"payload");

    // The content type is accepted-and-ignored: a second put over the
    // same key replaces the bytes.
    store
        .put("uploads/deep/dir/object.bin", b"v2".as_slice(), None)
        .await
        .unwrap();
    assert_eq!(
        store.get("uploads/deep/dir/object.bin").await.unwrap(),
        b"v2"
    );
}

#[tokio::test]
async fn missing_files_read_as_not_found() {
    let scratch = Scratch::new("missing");
    let store = LocalStorage::new(scratch.path().to_path_buf());
    let err = store.get("no/such/key").await.unwrap_err();
    assert!(
        matches!(err, rushwind_oss::StorageError::NotFound),
        "{err:?}"
    );
}

#[tokio::test]
async fn delete_is_idempotent() {
    let scratch = Scratch::new("delete");
    let store = LocalStorage::new(scratch.path().to_path_buf());
    store.put("k", b"v".as_slice(), None).await.unwrap();
    store.delete("k").await.unwrap();
    assert!(store.get("k").await.is_err());
    // The second delete is a no-op, not an error.
    store.delete("k").await.unwrap();
}

#[tokio::test]
async fn traversal_and_empty_keys_are_refused() {
    let scratch = Scratch::new("traversal");
    let store = LocalStorage::new(scratch.path().to_path_buf());
    for bad in ["../escape", "a/../../b", "/absolute"] {
        let err = store.put(bad, b"x".as_slice(), None).await.unwrap_err();
        assert!(
            matches!(err, rushwind_oss::StorageError::Failed(_)),
            "{bad}: {err:?}"
        );
        assert!(store.get(bad).await.is_err(), "{bad}");
    }
    let err = store.get("").await.unwrap_err();
    assert!(matches!(err, rushwind_oss::StorageError::EmptyObjectKey));
}
