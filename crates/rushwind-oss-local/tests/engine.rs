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
    // Hostile on every platform: `..` walks out, a leading `/` is
    // rooted on both (and `Path::join` replaces the buffer with it).
    for bad in ["../escape", "a/../../b", "/absolute"] {
        let err = store.put(bad, b"x".as_slice(), None).await.unwrap_err();
        assert!(
            matches!(err, rushwind_oss::StorageError::Failed(_)),
            "{bad}: {err:?}"
        );
        assert!(store.get(bad).await.is_err(), "{bad}");
    }
    // Windows-only spellings: a drive-relative or UNC prefix replaces
    // the join buffer just the same (on Unix these are plain names).
    #[cfg(windows)]
    for bad in ["C:evil", "\\\\srv\\share\\obj"] {
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

#[tokio::test]
async fn list_walks_ascending_with_prefix_after_limit() {
    let scratch = Scratch::new("list");
    let store = LocalStorage::new(scratch.path().to_path_buf());

    for (key, size) in [
        ("probe/list/alpha", 3usize),
        ("probe/list/beta", 5),
        ("probe/list/gamma", 7),
    ] {
        store
            .put(key, vec![0u8; size].as_slice(), None)
            .await
            .unwrap();
    }

    let page = store.list("probe/list/", "", 10).await.unwrap();
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

    let after = store
        .list("probe/list/", "probe/list/beta", 10)
        .await
        .unwrap();
    assert_eq!(
        after.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        vec!["probe/list/gamma"],
        "the cursor is strict: the cursor key itself is excluded"
    );

    let limited = store.list("probe/list/", "", 2).await.unwrap();
    assert_eq!(limited.len(), 2, "the limit bounds the page");

    let empty_prefix = store.list("", "", 10).await.unwrap();
    assert_eq!(empty_prefix.len(), 3, "an empty prefix matches everything");

    let result = store.list("probe/list/", "", 0).await;
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::InvalidLimit)),
        "a zero limit must fail with InvalidLimit"
    );
}

#[tokio::test]
async fn list_over_an_absent_root_is_an_empty_page() {
    let scratch = Scratch::new("list-absent");
    // Point the engine one level BELOW the scratch dir: the root itself
    // never materializes (nothing is put there).
    let store = LocalStorage::new(scratch.path().join("never-created"));
    let page = store.list("", "", 10).await.unwrap();
    assert!(page.is_empty(), "an absent bucket root reads as empty");
}

#[tokio::test]
async fn presign_is_unsupported_on_local_disk() {
    let scratch = Scratch::new("presign");
    let store = LocalStorage::new(scratch.path().to_path_buf());
    let result = store.presign(
        "k",
        rushwind_oss::PresignMethod::Get,
        std::time::Duration::from_secs(60),
    );
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::Unsupported)),
        "a file path carries no signature authority: the honest answer is Unsupported"
    );
}

#[tokio::test]
async fn multipart_lifecycle_assembles_in_part_order() {
    let scratch = Scratch::new("multipart");
    let store = LocalStorage::new(scratch.path().to_path_buf());

    let upload = store.multipart_begin("uploads/big.bin").await.unwrap();
    // Parts arrive out of order on purpose: the assembly is by part
    // number, not arrival.
    let token_three = store
        .multipart_put("uploads/big.bin", &upload, 3, b"tail".as_slice())
        .await
        .unwrap();
    let token_one = store
        .multipart_put("uploads/big.bin", &upload, 1, b"head".as_slice())
        .await
        .unwrap();
    let token_two = store
        .multipart_put("uploads/big.bin", &upload, 2, b"-".as_slice())
        .await
        .unwrap();

    let tokens = vec![(1usize, token_one), (2, token_two), (3, token_three)];
    store
        .multipart_complete("uploads/big.bin", &upload, &tokens)
        .await
        .expect("complete assembles");

    assert_eq!(
        store.get("uploads/big.bin").await.unwrap(),
        b"head-tail".to_vec(),
        "the parts concatenate by part number"
    );
    // The staging directory is gone: completion closes the upload.
    assert!(
        store
            .multipart_complete("uploads/big.bin", &upload, &tokens)
            .await
            .is_err(),
        "a completed upload is closed"
    );
}

#[tokio::test]
async fn multipart_abort_discards_everything() {
    let scratch = Scratch::new("multipart-abort");
    let store = LocalStorage::new(scratch.path().to_path_buf());

    let upload = store
        .multipart_begin("uploads/abandoned.bin")
        .await
        .unwrap();
    store
        .multipart_put("uploads/abandoned.bin", &upload, 1, b"junk".as_slice())
        .await
        .unwrap();
    store
        .multipart_abort("uploads/abandoned.bin", &upload)
        .await
        .expect("abort discards");

    // The object never existed; the upload is gone.
    assert!(store.get("uploads/abandoned.bin").await.is_err());
    assert!(store
        .multipart_put("uploads/abandoned.bin", &upload, 1, b"x".as_slice())
        .await
        .is_err());
    assert!(
        store
            .multipart_abort("uploads/abandoned.bin", &upload)
            .await
            .is_err(),
        "a second abort reads as NotFound"
    );
}

#[tokio::test]
async fn multipart_refusals_are_typed() {
    let scratch = Scratch::new("multipart-refusals");
    let store = LocalStorage::new(scratch.path().to_path_buf());

    // Part numbers live in 1..=10000.
    let upload = store.multipart_begin("k").await.unwrap();
    for bad in [0usize, 10001] {
        let err = store
            .multipart_put("k", &upload, bad, b"x".as_slice())
            .await
            .unwrap_err();
        assert!(
            matches!(err, rushwind_oss::StorageError::InvalidPart(ref m) if m.contains(&bad.to_string())),
            "{bad}: {err:?}"
        );
    }
    // An empty part body is the EmptyObjectBody refusal, same as put.
    let err = store.multipart_put("k", &upload, 1, &[]).await.unwrap_err();
    assert!(matches!(err, rushwind_oss::StorageError::EmptyObjectBody));

    store
        .multipart_put("k", &upload, 1, b"real".as_slice())
        .await
        .unwrap();

    // Complete demands the parts back: none, or a gap, refuses.
    for bad_parts in [
        vec![],
        vec![(1usize, "4".to_string()), (3usize, "0".to_string())],
    ] {
        let err = store
            .multipart_complete("k", &upload, &bad_parts)
            .await
            .unwrap_err();
        assert!(
            matches!(err, rushwind_oss::StorageError::InvalidPart(_)),
            "{err:?}"
        );
    }

    // A token that no longer matches the staged part refuses at
    // completion — the assembly verifies what it assembles.
    let err = store
        .multipart_complete("k", &upload, &[(1usize, "999".to_string())])
        .await
        .unwrap_err();
    assert!(
        matches!(err, rushwind_oss::StorageError::Failed(ref m) if m.contains("token")),
        "{err:?}"
    );

    // Unknown uploads read as NotFound — including a hostile id that
    // tries to escape the staging directory.
    let stranger = rushwind_oss::MultipartUploadId("no-such-upload".to_string());
    assert!(store
        .multipart_put("k", &stranger, 1, b"x".as_slice())
        .await
        .is_err());
    let escape = rushwind_oss::MultipartUploadId("../../escape".to_string());
    assert!(
        store
            .multipart_put("k", &escape, 1, b"x".as_slice())
            .await
            .is_err(),
        "a traversal-shaped id never resolves"
    );
}
