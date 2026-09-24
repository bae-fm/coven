//! Recording a make-remote inside the host write that creates the root.

use crate::blob_facade_tests::open_local;

async fn write_note_made_remote(
    handle: &crate::CovenHandle,
    path: &std::path::Path,
    bytes: &[u8],
    fail_after: bool,
) -> Result<(), crate::CovenError> {
    let prepared = crate::prepare_external_blob(path, |_| {}).await?;
    let size = bytes.len() as i64;
    handle
        .write(move |sql| {
            sql.execute(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES ('note-1', 'Note', 0, ?1, ?1)",
                [sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
                 VALUES ('photo-1', 'note-1', 'photo', ?1, NULL, ?2, ?2)",
                rusqlite::params![size, sql.stamp()],
            )?;
            sql.register_external_blob("note_photos", "photo-1", prepared)?;
            sql.make_remote(
                "notes",
                "note-1",
                "Notes Root",
                false,
                &[("note_photos", "photo-1")],
            )?;
            if fail_after {
                return Err(crate::CovenError::from(crate::DbError::Message(
                    "the host abandons its write".to_string(),
                )));
            }
            Ok(())
        })
        .await
        .map(|_| ())
}

/// An import that is Remote from the start commits its rows and the
/// make-remote decision together, with no cloud connected: nothing can leave
/// the rows committed Local with the choice lost, and being offline does not
/// refuse the choice.
#[tokio::test]
async fn a_host_write_records_make_remote_offline_and_atomically() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));
    let user_dir = tempfile::tempdir().expect("user directory");
    let bytes = b"a photo imported straight to the cloud".to_vec();
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, &bytes).expect("write the user's file");

    assert!(
        write_note_made_remote(&handle, &path, &bytes, true)
            .await
            .is_err(),
        "the host's own failure aborts the write"
    );
    assert!(handle
        .queued_uploads()
        .await
        .expect("read queue")
        .is_empty());
    assert_eq!(
        handle
            .make_remote_progress("notes", "note-1")
            .await
            .expect("read the transition"),
        None,
        "an aborted write leaves no intent behind"
    );

    write_note_made_remote(&handle, &path, &bytes, false)
        .await
        .expect("commit the rows and their make-remote together, offline");
    let queued = handle.queued_uploads().await.expect("read queue");
    assert_eq!(queued.len(), 1, "{queued:?}");
    assert_eq!(queued[0].root_id, "note-1");
    assert_eq!(queued[0].blob.row_id(), "photo-1");
    assert_eq!(
        handle
            .make_remote_progress("notes", "note-1")
            .await
            .expect("read the transition"),
        Some(crate::MakeRemoteProgress::Uploading),
    );
}

/// The make-remote a write recorded is the ordinary durable transition: once
/// a cloud is connected, the drain uploads it and moves the root past
/// uploading, the same as a standalone `make_remote`.
#[tokio::test]
async fn a_make_remote_recorded_in_a_write_is_drained_once_connected() {
    tokio::spawn(run_a_make_remote_recorded_in_a_write_is_drained_once_connected())
        .await
        .expect("drain task");
}

async fn run_a_make_remote_recorded_in_a_write_is_drained_once_connected() {
    use coven_replication::sync::test_helpers::*;

    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let owner = coven_keys::keys::UserKeypair::generate();
    let encryption = crate::EncryptionService::from_key([42; 32]);
    let handle = crate::blob_facade_tests::builder(crate::StoreDir::new_ephemeral(tmp.path()))
        .synced_tables(test_synced_tables_with_blob(crate::BlobDecl::new(
            "photos",
            crate::Provenance::UserProvided,
            crate::CacheFill::CacheLazy,
        )))
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(crate::MasterKeyring::from(
            encryption.clone(),
        )))
        .identity_custody(crate::IdentityCustody::InMemory(owner.clone()))
        .open()
        .expect("open the store");
    let home = test_cloud_home();
    handle
        .create_test_store("make-remote-in-write", owner, home.clone())
        .await
        .expect("create the Store");
    let user_dir = tempfile::tempdir().expect("user directory");
    let bytes = b"a photo imported straight to the cloud".to_vec();
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, &bytes).expect("write the user's file");
    write_note_made_remote(&handle, &path, &bytes, false)
        .await
        .expect("commit the rows and their make-remote together");

    handle
        .connect_sync_with_test_home_caller_driven(
            home,
            coven_storage::CloudCipher::Encrypted(encryption),
        )
        .await
        .expect("connect the store to its home");
    let outcome = handle.drain_uploads().await.expect("drain the queue");
    assert!(
        matches!(
            outcome,
            coven_replication::blob::DrainOutcome::Drained { uploaded: 1, .. }
        ),
        "{outcome:?}"
    );
    assert_ne!(
        handle
            .make_remote_progress("notes", "note-1")
            .await
            .expect("read the transition"),
        Some(crate::MakeRemoteProgress::Uploading),
    );
}
