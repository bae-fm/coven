use coven_replication::sync::test_helpers::*;

use crate::blob_facade_tests::{builder, open_local, ExternalPhotoTestHost};

#[tokio::test]
async fn preparation_reports_bytes_and_registration_sets_the_row_hash() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));
    let user_dir = tempfile::tempdir().expect("user directory");
    let bytes = vec![7_u8; (1 << 20) + 17];
    let expected_size = bytes.len() as u64;
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, &bytes).expect("write the user's file");
    let progress = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let prepared = crate::prepare_external_blob(&path, {
        let progress = progress.clone();
        move |consumed| progress.lock().expect("progress lock").push(consumed)
    })
    .await
    .expect("prepare the external file");

    handle
        .write(move |sql| {
            sql.execute(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES ('note-1', 'Note', 0, ?1, ?1)",
                rusqlite::params![sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
                 VALUES ('photo-1', 'note-1', 'photo', ?1, NULL, ?2, ?2)",
                rusqlite::params![expected_size as i64, sql.stamp()],
            )?;
            sql.register_external_blob("note_photos", "photo-1", prepared)?;
            Ok(())
        })
        .await
        .expect("register the prepared file");

    assert_eq!(
        progress.lock().expect("progress lock").last().copied(),
        Some(expected_size),
    );
    let stored_hash = handle
        .read(|sql| {
            sql.query_row(
                "SELECT hash FROM note_photos WHERE id = 'photo-1'",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(crate::CovenError::from)
        })
        .await
        .expect("read Coven's stored hash");
    assert_eq!(stored_hash, crate::content_hash(&bytes));
}

#[tokio::test]
async fn registration_rejects_a_row_size_that_differs_from_the_prepared_file() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));
    let user_dir = tempfile::tempdir().expect("user directory");
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, b"bytes").expect("write external file");
    let prepared = crate::prepare_external_blob(&path, |_| {})
        .await
        .expect("prepare external file");

    handle
        .write(move |sql| {
            sql.execute(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES ('note-1', 'Note', 0, ?1, ?1)",
                rusqlite::params![sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
                 VALUES ('photo-1', 'note-1', 'photo', 6, NULL, ?1, ?1)",
                rusqlite::params![sql.stamp()],
            )?;
            sql.register_external_blob("note_photos", "photo-1", prepared)?;
            Ok(())
        })
        .await
        .expect_err("the declared size must equal the prepared file");

    let rows = handle
        .read(|sql| {
            sql.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get::<_, i64>(0))
                .map_err(crate::CovenError::from)
        })
        .await
        .expect("count committed notes");
    assert_eq!(rows, 0, "the refused write commits no host rows");
}

#[tokio::test]
async fn registration_rejects_a_file_changed_after_preparation() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));
    let user_dir = tempfile::tempdir().expect("user directory");
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, b"before").expect("write external file");
    let prepared = crate::prepare_external_blob(&path, |_| {})
        .await
        .expect("prepare external file");
    std::fs::write(&path, b"changed bytes").expect("change prepared file");

    handle
        .write(move |sql| {
            sql.execute(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES ('note-1', 'Note', 0, ?1, ?1)",
                rusqlite::params![sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
                 VALUES ('photo-1', 'note-1', 'photo', 6, NULL, ?1, ?1)",
                rusqlite::params![sql.stamp()],
            )?;
            sql.register_external_blob("note_photos", "photo-1", prepared)?;
            Ok(())
        })
        .await
        .expect_err("a prepared file cannot change before registration");
}

#[tokio::test]
async fn a_blob_row_cannot_commit_without_a_prepared_registration() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));

    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES ('note-1', 'Note', 0, ?1, ?1)",
                rusqlite::params![sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
                 VALUES ('photo-1', 'note-1', 'photo', 5, NULL, ?1, ?1)",
                rusqlite::params![sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect_err("a blob row with no content hash cannot commit");
}

/// A file coven references but does not own reads back through the handle, and
/// the registration can be dropped and remade.
#[tokio::test]
async fn an_external_file_reads_back_through_the_handle() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));
    let user_dir = tempfile::tempdir().expect("user directory");

    let bytes = b"the user's own file, never copied".to_vec();
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, &bytes).expect("write the user's file");

    handle
        .write_note_with_external_photo("note-1", "photo-1", &path, &bytes)
        .await
        .expect("register the external file in the write that created the row");

    let reference = handle
        .row_blob_ref("note_photos", "photo-1")
        .await
        .expect("resolve the row's blob reference");
    assert_eq!(
        handle.read_blob(&reference).await.expect("read the blob"),
        bytes,
        "the bytes come from the user's file",
    );

    handle
        .write(|sql| {
            sql.clear_external_blob("note_photos", "photo-1")?;
            Ok(())
        })
        .await
        .expect("clear the registration");
    let cleared = handle.row_blob_ref("note_photos", "photo-1").await;
    let cleared = match cleared {
        Ok(reference) => handle.read_blob(&reference).await.err(),
        Err(_) => None,
    };
    assert!(
        cleared.is_some(),
        "a cleared registration leaves the blob unreadable",
    );

    let moved = user_dir.path().join("photo-moved.jpg");
    std::fs::rename(&path, &moved).expect("the user moves their file");
    let prepared = crate::prepare_external_blob(&moved, |_| {})
        .await
        .expect("prepare the moved file");
    handle
        .write(move |sql| {
            sql.execute(
                "UPDATE note_photos SET _updated_at = ?1 WHERE id = 'photo-1'",
                rusqlite::params![sql.stamp()],
            )?;
            sql.register_external_blob("note_photos", "photo-1", prepared)?;
            Ok(())
        })
        .await
        .expect("re-register the moved file");
    let reference = handle
        .row_blob_ref("note_photos", "photo-1")
        .await
        .expect("resolve the row's blob reference again");
    assert_eq!(
        handle.read_blob(&reference).await.expect("read the blob"),
        bytes,
        "the re-registered file reads back",
    );
}

#[tokio::test]
async fn a_host_provided_table_refuses_an_external_file() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = builder(crate::StoreDir::new_ephemeral(tmp.path()))
        .synced_tables(test_synced_tables_with_blob(crate::BlobDecl::new(
            "photos",
            crate::Provenance::HostProvided,
            crate::CacheFill::CacheLazy,
        )))
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(crate::MasterKeyring::generate()))
        .identity_custody(crate::IdentityCustody::InMemory(
            coven_keys::keys::UserKeypair::generate(),
        ))
        .open()
        .expect("open the store");
    let user_dir = tempfile::tempdir().expect("user directory");
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, b"bytes").expect("write the user's file");

    let refused = handle
        .write_note_with_external_photo("note-1", "photo-1", &path, b"bytes")
        .await;
    assert!(
        refused.is_err(),
        "a host-provided table must refuse an external file, got {refused:?}",
    );
}

#[tokio::test]
async fn registering_against_a_table_with_no_blob_is_refused() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));
    let user_dir = tempfile::tempdir().expect("user directory");
    let path = user_dir.path().join("file");
    std::fs::write(&path, b"bytes").expect("write external file");
    let prepared = crate::prepare_external_blob(&path, |_| {})
        .await
        .expect("prepare external file");

    let refused = handle
        .write(move |sql| {
            sql.execute(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES ('n', 'N', 0, ?1, ?1)",
                rusqlite::params![sql.stamp()],
            )?;
            sql.register_external_blob("notes", "n", prepared)?;
            Ok(())
        })
        .await;
    assert!(
        refused.is_err(),
        "a table with no blob declaration is refused, got {refused:?}",
    );
}
