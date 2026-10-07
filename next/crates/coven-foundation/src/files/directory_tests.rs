use super::*;
use crate::files::StoreLayout;
use crate::id_source::UuidIds;
use uuid::Uuid;

#[test]
fn cancelled_writes_retain_the_store_until_blocking_io_finishes() {
    use std::{future::Future, task::Poll};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        for finish in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let layout = StoreLayout::new(root.path().to_owned());
            let store = layout
                .create_store_dir(StoreId(Uuid::from_u128(123)), "Files", &UuidIds)
                .unwrap();
            let name = FileName::new("cancelled").unwrap();
            let mut writer = store
                .file(FileArea::Cache, &name)
                .create_writer(store.lock_read_only().unwrap())
                .unwrap();
            if finish {
                writer.append(b"queued bytes").await.unwrap();
            }
            let (started, ready) = std::sync::mpsc::channel();
            let (release, gate) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                gate.recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
            });
            ready
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            let mut write = Box::pin(async move {
                if finish {
                    writer.finish().await
                } else {
                    writer.append(b"queued bytes").await
                }
            });
            std::future::poll_fn(|cx| {
                assert!(write.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(write);
            let refused = matches!(
                store.lock_for_deletion(),
                Err(StoreLockError::AlreadyOpen(_))
            );
            release.send(()).unwrap();
            blocker.await.unwrap();
            // One blocking thread makes this a completion barrier for the queued write.
            tokio::task::spawn_blocking(|| {}).await.unwrap();
            assert!(
                refused,
                "cancelled I/O released the store before its file handle"
            );
            assert_eq!(
                store
                    .file(FileArea::Cache, &name)
                    .read_optional()
                    .unwrap()
                    .unwrap(),
                b"queued bytes"
            );
            store
                .lock_for_deletion()
                .unwrap()
                .unwrap()
                .remove_directory()
                .unwrap();
        }
    });
}

#[tokio::test]
async fn a_streaming_writer_prevents_store_deletion_until_it_closes() {
    for area in [FileArea::AppProvided, FileArea::Cache] {
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().to_owned());
        let id = StoreId(Uuid::from_u128(123));
        let store = layout.create_store_dir(id, "Files", &UuidIds).unwrap();
        let name = FileName::new("streamed").unwrap();
        let mut writer = store
            .file(area, &name)
            .create_writer(store.lock_read_only().unwrap())
            .unwrap();
        assert!(matches!(
            store.lock_for_deletion(),
            Err(StoreLockError::AlreadyOpen(_))
        ));
        writer.append(b"kept bytes").await.unwrap();
        writer.finish().await.unwrap();
        assert_eq!(
            store.file(area, &name).read_optional().unwrap().unwrap(),
            b"kept bytes"
        );
        store
            .lock_for_deletion()
            .unwrap()
            .unwrap()
            .remove_directory()
            .unwrap();
    }
}

#[test]
fn named_file_capabilities_stay_in_their_own_areas() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(directory.path().to_owned());
    let id = StoreId(Uuid::from_u128(123));
    let store = layout.create_store_dir(id, "Household", &UuidIds).unwrap();
    let name = FileName::new("attachment.data").unwrap();
    store
        .file(FileArea::AppProvided, &name)
        .replace(b"app-provided")
        .unwrap();
    store
        .file(FileArea::Cache, &name)
        .replace(b"cache")
        .unwrap();
    let storage = store.owned_file(StoreFile::StorageSettings);
    assert_eq!(storage.read_optional().unwrap(), None);
    storage.replace(b"provider-owned format").unwrap();
    let store_keys = store.owned_file(StoreFile::StoreKeys);
    let member_keys = store.owned_file(StoreFile::MemberKeys);
    store_keys.replace(b"store keys").unwrap();
    member_keys.replace(b"member keys").unwrap();
    assert_eq!(store_keys.read_optional().unwrap().unwrap(), b"store keys");
    assert_eq!(
        member_keys.read_optional().unwrap().unwrap(),
        b"member keys"
    );
    assert_eq!(
        storage.read_optional().unwrap().unwrap(),
        b"provider-owned format"
    );
    assert_eq!(
        store
            .file(FileArea::AppProvided, &name)
            .read_optional()
            .unwrap()
            .unwrap(),
        b"app-provided"
    );
    assert_eq!(
        store
            .file(FileArea::Cache, &name)
            .read_optional()
            .unwrap()
            .unwrap(),
        b"cache"
    );
    assert_eq!(store.settings().unwrap().name, "Household");
    assert_eq!(store.database_path().file_name().unwrap(), "store.db");
    assert_eq!(store.id(), id);
}

#[test]
fn filenames_reject_path_traversal_and_platform_aliases() {
    for bad in [
        "",
        ".",
        "..",
        "/absolute",
        "../escape",
        "nested/file",
        r"nested\file",
        "C:drive",
        "nul",
        "NUL.txt",
        "com1.log",
        "LPT9",
        "trailing.",
        "trailing ",
        "nul\0byte",
        "with space",
        ".hidden",
    ] {
        assert!(FileName::new(bad).is_err(), "accepted {bad:?}");
    }
    assert_eq!(FileName::new("x".repeat(256)), Err(FileNameError::Length));
    for good in [
        "attachment",
        "sha256-0123456789",
        "chunk_42.data",
        "COM10",
        "file.name",
    ] {
        assert!(FileName::new(good).is_ok(), "refused {good}");
    }
}
