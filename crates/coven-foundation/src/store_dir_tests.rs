use super::*;

use std::time::Duration;

/// A normal id partitions by its first two dash-stripped byte-pairs, with the
/// full id (dashes kept) as the file name — the layout the cloud and local
/// stores share.
#[test]
fn hashed_path_partitions_a_normal_id() {
    let id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
    assert_eq!(
        StoreDir::hashed_path("images", id).expect("valid id"),
        format!("images/a1/b2/{id}"),
    );
}

/// An id too short to take the `{ab}/{cd}` prefix cannot index a two-byte
/// shard, so it is rejected as `Unindexable` rather than slicing past its end.
#[test]
fn hashed_path_refuses_a_short_id_instead_of_panicking() {
    assert_eq!(
        StoreDir::hashed_path("images", "a"),
        Err(PathTokenError::Unindexable),
    );
}

/// An id whose dash-stripped form splits a multi-byte char at the prefix
/// boundary is unindexable too, not a panic.
#[test]
fn hashed_path_refuses_a_misaligned_multibyte_id() {
    // 'é' is two bytes; "aé" puts a char boundary failure at byte 2.
    assert_eq!(
        StoreDir::hashed_path("images", "aé"),
        Err(PathTokenError::Unindexable),
    );
}

/// An id or namespace carrying a separator, a `..`, or a NUL is a traversal
/// attempt and is refused before any path is built.
#[test]
fn hashed_path_refuses_traversal_tokens() {
    assert_eq!(
        StoreDir::hashed_path("images", "ab/../../etc/passwd"),
        Err(PathTokenError::Separator),
    );
    assert_eq!(
        StoreDir::hashed_path("images", ".."),
        Err(PathTokenError::ParentDir),
    );
    assert_eq!(
        StoreDir::hashed_path("images", "a\0b"),
        Err(PathTokenError::NulByte),
    );
    assert_eq!(
        StoreDir::hashed_path("im/ages", "abcd"),
        Err(PathTokenError::Separator),
    );
}

/// `validate_path_token` accepts an ordinary single token and rejects each
/// escape shape: separators (`/` and `\`), a leading slash (an absolute path),
/// a bare `..` and a bare `.` (both directory-relative references that
/// normalize away to land off a child), a NUL, a `:` (a Windows
/// alternate-data-stream / drive-relative reference), and the empty string.
#[test]
fn validate_path_token_accepts_safe_and_rejects_escapes() {
    assert_eq!(validate_path_token("abc123"), Ok(()));
    assert_eq!(validate_path_token(""), Err(PathTokenError::Empty));
    assert_eq!(validate_path_token("a/b"), Err(PathTokenError::Separator));
    assert_eq!(validate_path_token("a\\b"), Err(PathTokenError::Separator));
    assert_eq!(validate_path_token("/abs"), Err(PathTokenError::Separator));
    assert_eq!(validate_path_token(".."), Err(PathTokenError::ParentDir));
    assert_eq!(validate_path_token("."), Err(PathTokenError::CurDir));
    assert_eq!(validate_path_token("a\0b"), Err(PathTokenError::NulByte));
    assert_eq!(validate_path_token("foo:bar"), Err(PathTokenError::Colon));
    assert_eq!(validate_path_token("c:"), Err(PathTokenError::Colon));
}

/// A lone `.` is rejected just as `..` is: a trailing `.` component is
/// normalized away when the path resolves, so `stores/.` would land on
/// `stores`'s parent (the data dir) rather than name a child of `stores/`
/// — an escape. The unit under test is the rejection itself.
#[test]
fn validate_path_token_rejects_lone_current_dir() {
    assert_eq!(validate_path_token("."), Err(PathTokenError::CurDir));
}

/// A `cloud_path` is a readable object key, so an interior `/` is legitimate
/// (nested path), but every component must itself be a canonical path token.
#[test]
fn validate_cloud_path_allows_nesting_but_rejects_escapes() {
    assert_eq!(validate_cloud_path("Artist - Album/cover.jpg"), Ok(()));
    assert_eq!(validate_cloud_path(""), Err(PathTokenError::Empty));
    assert_eq!(
        validate_cloud_path("../escape"),
        Err(PathTokenError::ParentDir),
    );
    assert_eq!(
        validate_cloud_path("a/../../escape"),
        Err(PathTokenError::ParentDir),
    );
    assert_eq!(validate_cloud_path("/abs"), Err(PathTokenError::Separator));
    assert_eq!(validate_cloud_path("a\\b"), Err(PathTokenError::Separator),);
    assert_eq!(validate_cloud_path("a/./b"), Err(PathTokenError::CurDir));
    assert_eq!(validate_cloud_path("a//b"), Err(PathTokenError::Empty));
    assert_eq!(validate_cloud_path("a/b/"), Err(PathTokenError::Empty));
    assert_eq!(validate_cloud_path("C:/b"), Err(PathTokenError::Colon));
    assert_eq!(validate_cloud_path("a/C:b"), Err(PathTokenError::Colon));
    assert_eq!(validate_cloud_path("a/b\0c"), Err(PathTokenError::NulByte));
}

#[test]
fn outbound_blob_spool_is_keyed_by_locator_hash() {
    let store = StoreDir::new("/stores/example");
    let locator_hash = crate::object_hash::ObjectHash::digest(b"locator");

    assert_eq!(
        store.outbound_blob_spool_path(locator_hash),
        store
            .storage_dir()
            .join("outbound-blobs")
            .join(locator_hash.to_string())
    );
}

#[test]
fn remote_cache_paths_are_keyed_by_locator_hash() {
    let store = StoreDir::new("/stores/example");
    let locator_hash = crate::object_hash::ObjectHash::digest(b"locator");
    let shard = StoreDir::id_shard(&locator_hash.to_string()).expect("hash shard");

    assert_eq!(
        store
            .cache_blob_path("images", locator_hash)
            .expect("cache path"),
        store.storage_dir().join("cache/images").join(&shard)
    );
    assert_eq!(
        store
            .pinned_blob_path("images", locator_hash)
            .expect("pinned path"),
        store.storage_dir().join("pinned/images").join(shard)
    );
}

/// The open-time orphan sweep clears crash-left atomic-write temps under
/// every blob folder that predate this open, while leaving current-process
/// temps and committed blobs untouched.
#[test]
fn orphan_sweep_clears_stale_blob_temps_but_keeps_fresh_ones() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let store_dir = StoreDir::new(tmp.path());
    let process_start = std::time::SystemTime::now();
    let stale = process_start - Duration::from_secs(3600);
    let fresh = process_start + Duration::from_secs(3600);

    let write_with_mtime = |path: &Path, mtime: std::time::SystemTime| {
        std::fs::create_dir_all(path.parent().unwrap()).expect("create dir");
        std::fs::write(path, b"x").expect("write file");
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open to set mtime")
            .set_modified(mtime)
            .expect("set mtime");
    };
    let cache_shard = store_dir.storage_dir().join("cache/release_files/ab/cd");
    let pinned_shard = store_dir.storage_dir().join("pinned/photos/ef/gh");
    let local_namespace = store_dir.storage_dir().join("local/audio");
    let runtime = tokio::runtime::Runtime::new().expect("test runtime");
    let staged_temp = |destination: PathBuf| {
        runtime.block_on(async {
            crate::local_file::AtomicStagedFile::create(&destination)
                .await
                .expect("create test staging file")
                .leave_unpublished_for_test()
        })
    };

    let stale_cache_temp = staged_temp(cache_shard.join("stale-cache"));
    let fresh_cache_temp = staged_temp(cache_shard.join("fresh-cache"));
    let committed_cache = cache_shard.join("blob0aaa");
    let stale_pinned_temp = staged_temp(pinned_shard.join("stale-pinned"));
    let fresh_pinned_temp = staged_temp(pinned_shard.join("fresh-pinned"));
    let stale_local_temp = staged_temp(local_namespace.join("stale-local"));
    let fresh_local_temp = staged_temp(local_namespace.join("fresh-local"));
    let committed_local = local_namespace.join("blob0bbb");
    let stale_local_stage = runtime.block_on(async {
        let mut stage = crate::local_file::AtomicStagedFile::create(&local_namespace.join("blob"))
            .await
            .expect("local staging path");
        stage.write_bytes(b"x").await.expect("write local stage");
        stage.leave_unpublished_for_test()
    });

    for path in [
        &stale_cache_temp,
        &committed_cache,
        &stale_pinned_temp,
        &stale_local_temp,
        &committed_local,
    ] {
        write_with_mtime(path, stale);
    }
    for path in [&fresh_cache_temp, &fresh_pinned_temp, &fresh_local_temp] {
        write_with_mtime(path, fresh);
    }
    std::fs::OpenOptions::new()
        .write(true)
        .open(&stale_local_stage)
        .expect("open local stage to set mtime")
        .set_modified(stale)
        .expect("set local stage mtime");

    store_dir
        .remove_orphaned_blob_temps(process_start)
        .expect("orphan sweep");

    for path in [
        &stale_cache_temp,
        &stale_pinned_temp,
        &stale_local_temp,
        &stale_local_stage,
    ] {
        assert!(!path.exists(), "stale temp remained: {}", path.display());
    }
    for path in [
        &fresh_cache_temp,
        &fresh_pinned_temp,
        &fresh_local_temp,
        &committed_cache,
        &committed_local,
    ] {
        assert!(
            path.exists(),
            "live or committed blob removed: {}",
            path.display()
        );
    }
}
