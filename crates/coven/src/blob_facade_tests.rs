//! External files and the upload queue, through coven's own surface.
//!
//! A host depends on the `coven` crate alone, so these drive the two operations
//! the way it must: registering a user's file inside the write that created the
//! row, and reading the durable upload queue without touching the drain that
//! works it. Nothing in the flow names an engine type.

use coven_replication::sync::test_helpers::*;

/// A blob-bearing child of a gated root whose bytes stay in the user's own
/// file — the shape an import produces.
fn user_file_decl() -> crate::BlobDecl {
    crate::BlobDecl::new(
        "photos",
        crate::Provenance::UserProvided,
        crate::CacheFill::CacheLazy,
    )
}

/// The `notes` gated root with a `note_photos` child carrying user files.
fn note_tables() -> Vec<crate::SyncedTable> {
    test_synced_tables_with_blob(user_file_decl())
}

fn config(dir: crate::StoreDir) -> crate::Config {
    crate::Config::with_defaults(
        "blob-facade-test".to_string(),
        "owner-device".to_string(),
        dir,
        "Blob Facade Store".to_string(),
    )
}

/// Open a store whose blobs are user files, with nothing connected: an upload
/// queue reader must work before sync exists.
fn open_local(dir: crate::StoreDir) -> crate::CovenHandle {
    crate::Coven::builder(config(dir))
        .synced_tables(note_tables())
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(crate::MasterKeyring::from(
            crate::EncryptionService::from_key([42; 32]),
        )))
        .identity_custody(crate::IdentityCustody::InMemory(
            coven_keys::keys::UserKeypair::generate(),
        ))
        .open()
        .expect("open the store")
}

/// Write a note and a photo row pointing at `path`, registering the file in the
/// same write — the order a host must use, since the registration binds to the
/// row version this write produces.
trait ExternalPhotoTestHost {
    async fn write_note_with_external_photo(
        &self,
        note_id: &str,
        photo_id: &str,
        path: &std::path::Path,
        bytes: &[u8],
    ) -> Result<(), crate::CovenError>;
}

impl ExternalPhotoTestHost for crate::CovenHandle {
    async fn write_note_with_external_photo(
        &self,
        note_id: &str,
        photo_id: &str,
        path: &std::path::Path,
        bytes: &[u8],
    ) -> Result<(), crate::CovenError> {
        let hash = crate::content_hash(bytes);
        let size = bytes.len() as i64;
        let note = note_id.to_string();
        let photo = photo_id.to_string();
        let path = path.to_path_buf();
        self.sql(move |sql| {
            sql.execute(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES (?1, 'Note', 0, ?2, ?2)",
                rusqlite::params![note, sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
                 VALUES (?1, ?2, 'photo', ?3, ?4, ?5, ?5)",
                rusqlite::params![photo, note, size, hash, sql.stamp()],
            )?;
            sql.register_external_blob("note_photos", &photo, &path)?;
            Ok(())
        })
        .await
        .map(|_| ())
    }
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

    // Clearing the registration leaves the row, and the blob has nowhere to be
    // read from until it is registered again.
    handle
        .sql(|sql| {
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

    // Re-registering the same file restores the read.
    let moved = user_dir.path().join("photo-moved.jpg");
    std::fs::rename(&path, &moved).expect("the user moves their file");
    handle
        .sql({
            let moved = moved.clone();
            move |sql| {
                sql.execute(
                    "UPDATE note_photos SET _updated_at = ?1 WHERE id = 'photo-1'",
                    rusqlite::params![sql.stamp()],
                )?;
                sql.register_external_blob("note_photos", "photo-1", &moved)?;
                Ok(())
            }
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

/// A table whose blobs coven copies takes no external file: the registration
/// would be written and never read, so it is refused instead.
#[tokio::test]
async fn a_host_provided_table_refuses_an_external_file() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = crate::Coven::builder(config(crate::StoreDir::new_ephemeral(tmp.path())))
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

/// An undeclared table, or one with no blob, is named in the error rather than
/// silently registering nothing.
#[tokio::test]
async fn registering_against_a_table_with_no_blob_is_refused() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));

    let refused = handle
        .sql(|sql| {
            sql.execute(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES ('n', 'N', 0, ?1, ?1)",
                rusqlite::params![sql.stamp()],
            )?;
            sql.register_external_blob("notes", "n", std::path::Path::new("/tmp/x"))?;
            Ok(())
        })
        .await;
    assert!(
        refused.is_err(),
        "a table with no blob declaration is refused, got {refused:?}",
    );
}

/// Open the same store again over the same directory and key, the way a
/// relaunched app does.
///
/// The caller stops sync and drops the previous handle before entering this
/// construction boundary, matching a relaunched process with no prior owner.
fn reopen(
    dir: crate::StoreDir,
    keyring: crate::MasterKeyring,
    owner: coven_keys::keys::UserKeypair,
) -> crate::CovenHandle {
    crate::Coven::builder(config(dir))
        .synced_tables(note_tables())
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(keyring))
        .identity_custody(crate::IdentityCustody::InMemory(owner))
        .open()
        .expect("reopen the store after closing its previous owner")
}

/// A queued upload is visible the moment `make_remote` enqueues it — before any
/// transfer runs, and after a restart — and the per-root question is answered
/// from the same durable state.
#[tokio::test]
async fn the_upload_queue_is_readable_before_any_transfer_and_across_a_restart() {
    tokio::spawn(run_the_upload_queue_is_readable_before_any_transfer_and_across_a_restart())
        .await
        .expect("upload queue task");
}

async fn run_the_upload_queue_is_readable_before_any_transfer_and_across_a_restart() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let dir = crate::StoreDir::new_ephemeral(tmp.path());
    let owner = coven_keys::keys::UserKeypair::generate();
    let encryption = crate::EncryptionService::from_key([42; 32]);
    let keyring = crate::MasterKeyring::from(encryption.clone());
    let handle = crate::Coven::builder(config(dir.clone()))
        .synced_tables(note_tables())
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(keyring.clone()))
        .identity_custody(crate::IdentityCustody::InMemory(owner.clone()))
        .open()
        .expect("open the store");
    let home = coven_replication::sync::test_helpers::test_cloud_home();
    handle
        .create_test_store("blob-facade-test", owner.clone(), home.clone())
        .await
        .expect("create the Store");
    handle
        .connect_sync_with_test_home_caller_driven(
            home.clone(),
            coven_storage::CloudCipher::Encrypted(encryption),
        )
        .await
        .expect("connect the store to its home");

    let user_dir = tempfile::tempdir().expect("user directory");
    let bytes = b"a photo the user owns".to_vec();
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, &bytes).expect("write the user's file");
    handle
        .write_note_with_external_photo("note-1", "photo-1", &path, &bytes)
        .await
        .expect("write the note and register its photo");

    // Nothing is queued until a transition asks for it.
    assert!(handle
        .queued_uploads()
        .await
        .expect("read the queue")
        .is_empty(),);
    assert_eq!(
        handle
            .make_remote_progress("notes", "note-1")
            .await
            .expect("read the transition state"),
        None,
        "a root with no transition reports none",
    );

    handle
        .make_remote("notes", "note-1", true)
        .await
        .expect("start the transition");

    // The upload is queued and readable immediately — no transfer has run.
    let queued = handle.queued_uploads().await.expect("read the queue");
    assert_eq!(queued.len(), 1, "one photo is queued: {queued:?}");
    let upload = &queued[0];
    assert_eq!(upload.namespace, "photos");
    // The table declares no separate blob-id column, so the row's own id is the
    // blob's id.
    assert_eq!(upload.blob_id, "photo-1");
    assert_eq!(upload.table_name, "note_photos");
    assert_eq!(upload.row_id, "photo-1");
    assert_eq!(upload.root_table, "notes");
    assert_eq!(upload.root_id, "note-1");
    assert!(
        upload.retain_pinned,
        "the transition asked to keep it cached"
    );
    assert_eq!(
        upload.attempt_count, 0,
        "a freshly queued upload has not been tried",
    );
    assert_eq!(upload.last_error, None);
    assert!(!upload.created_at.is_empty());
    assert_eq!(upload.last_attempt_at, None);
    assert_eq!(
        handle
            .make_remote_progress("notes", "note-1")
            .await
            .expect("read the transition state"),
        Some(crate::MakeRemoteProgress::Uploading),
    );
    // The per-root question the queue also answers.
    assert!(
        queued
            .iter()
            .any(|queued| queued.root_table == "notes" && queued.root_id == "note-1"),
        "the queue names the root each upload belongs to",
    );

    // A relaunched app reads the same queue: it is a table in the store, not
    // anything the process was holding.
    handle.disconnect_sync();
    drop(handle);
    let reopened = reopen(dir, keyring, owner);
    let after_restart = reopened
        .queued_uploads()
        .await
        .expect("read the queue again");
    assert_eq!(after_restart, queued, "the queue survived the restart");
    assert_eq!(
        reopened
            .make_remote_progress("notes", "note-1")
            .await
            .expect("read the transition state"),
        Some(crate::MakeRemoteProgress::Uploading),
        "so did the transition state",
    );

    // Draining performs the transfers the queue was holding. The reconnect starts
    // no loop, so this drain is the only one — what it reports is what moved.
    reopened
        .connect_sync_with_test_home_caller_driven(
            home,
            coven_storage::CloudCipher::Encrypted(crate::EncryptionService::from_key([42; 32])),
        )
        .await
        .expect("reconnect the relaunched store");
    let outcome = reopened.drain_uploads().await.expect("drain the queue");
    assert!(
        matches!(
            outcome,
            coven_protocol::blob::DrainOutcome::Drained { uploaded: 1, .. }
        ),
        "the drain uploaded the queued photo: {outcome:?}",
    );

    // The upload has landed, so the transition has left the uploading stage.
    let after_drain = reopened
        .make_remote_progress("notes", "note-1")
        .await
        .expect("read the transition state");
    assert_ne!(
        after_drain,
        Some(crate::MakeRemoteProgress::Uploading),
        "the drain moved the transition past uploading",
    );
    assert!(
        reopened
            .queued_uploads()
            .await
            .expect("read the queue after draining")
            .iter()
            .all(|queued| queued.attempt_count == 0),
        "a drained upload records no failed attempt",
    );
}

/// A published blob's cloud object outlives the row that referenced it, so
/// deleting the row queues a tombstone for it — durably, in the same write.
#[tokio::test]
async fn deleting_a_published_row_queues_its_cloud_object_for_removal() {
    tokio::spawn(run_deleting_a_published_row_queues_its_cloud_object_for_removal())
        .await
        .expect("cloud tombstone task");
}

async fn run_deleting_a_published_row_queues_its_cloud_object_for_removal() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let dir = crate::StoreDir::new_ephemeral(tmp.path());
    let owner = coven_keys::keys::UserKeypair::generate();
    let encryption = crate::EncryptionService::from_key([42; 32]);
    // A blob coven copies and publishes, so the row ends up with a cloud
    // object behind it — the thing a tombstone removes.
    let handle = crate::Coven::builder(config(dir.clone()))
        .synced_tables(test_synced_tables_with_blob(crate::BlobDecl::new(
            "photos",
            crate::Provenance::HostProvided,
            crate::CacheFill::CacheLazy,
        )))
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(crate::MasterKeyring::from(
            encryption.clone(),
        )))
        .identity_custody(crate::IdentityCustody::InMemory(owner.clone()))
        .open()
        .expect("open the store");
    let store = handle
        .create_test_store(
            "blob-facade-test",
            owner,
            coven_replication::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create the Store");
    let bytes = b"a published photo".to_vec();
    let hash = crate::content_hash(&bytes);
    let size = bytes.len() as i64;
    handle
        .write(
            {
                let bytes = bytes.clone();
                move |batch| {
                    batch.put_blob("photos", "photo-1", bytes);
                    Ok(())
                }
            },
            move |sql| {
                sql.execute(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                     VALUES ('note-1', 'Note', 1, ?1, ?1)",
                    rusqlite::params![sql.stamp()],
                )?;
                sql.execute(
                    "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
                     VALUES ('photo-1', 'note-1', 'photo', ?1, ?2, ?3, ?3)",
                    rusqlite::params![size, hash, sql.stamp()],
                )?;
                Ok(())
            },
        )
        .await
        .expect("write the note and its photo");
    handle
        .publish_test_store(&store)
        .await
        .expect("publish the photo");

    // Capture the reference before the row goes: the cloud object it names is
    // the only thing that can identify the tombstone afterwards.
    let reference = handle
        .row_blob_ref("note_photos", "photo-1")
        .await
        .expect("resolve the published row's blob reference");
    assert!(
        reference.stored().is_some(),
        "the published blob has a cloud object: {reference:?}",
    );
    assert!(
        handle
            .queued_deletes()
            .await
            .expect("read the tombstone queue")
            .is_empty(),
        "nothing is queued for removal yet",
    );

    handle
        .sql({
            let reference = reference.clone();
            move |sql| {
                sql.execute("DELETE FROM note_photos WHERE id = 'photo-1'", [])?;
                sql.enqueue_blob_delete(&reference)?;
                Ok(())
            }
        })
        .await
        .expect("delete the row and queue its cloud object for removal");

    let queued = handle
        .queued_deletes()
        .await
        .expect("read the tombstone queue");
    assert_eq!(queued.len(), 1, "one cloud object is queued: {queued:?}");
    assert_eq!(queued[0].namespace, "photos");
    assert_eq!(queued[0].blob_id, "photo-1");
    assert_eq!(queued[0].attempt_count, 0);
    assert_eq!(queued[0].last_error, None);

    // Queueing the same object again is the same tombstone, not a second one.
    handle
        .sql(move |sql| {
            sql.enqueue_blob_delete(&reference)?;
            Ok(())
        })
        .await
        .expect("re-queue the same cloud object");
    assert_eq!(
        handle
            .queued_deletes()
            .await
            .expect("read the tombstone queue")
            .len(),
        1,
        "the same cloud object queues once",
    );
}

/// A blob that was never uploaded has no cloud object, so there is nothing to
/// tombstone and the enqueue says so rather than queueing a phantom removal.
#[tokio::test]
async fn a_local_only_blob_has_no_cloud_object_to_remove() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));
    let user_dir = tempfile::tempdir().expect("user directory");
    let bytes = b"never uploaded".to_vec();
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, &bytes).expect("write the user's file");
    handle
        .write_note_with_external_photo("note-1", "photo-1", &path, &bytes)
        .await
        .expect("write the note and register its photo");

    let reference = handle
        .row_blob_ref("note_photos", "photo-1")
        .await
        .expect("resolve the row's blob reference");
    let refused = handle
        .sql(move |sql| {
            sql.enqueue_blob_delete(&reference)?;
            Ok(())
        })
        .await;
    assert!(
        refused.is_err(),
        "a local-only blob has no cloud object to remove, got {refused:?}",
    );
}

/// A host that needs the user's actual file — to re-read its tags, to find what
/// it produced — asks the handle where it is, and gets `None` once the row no
/// longer points at one.
#[tokio::test]
async fn the_handle_reports_where_a_row_s_user_file_lives() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));
    let user_dir = tempfile::tempdir().expect("user directory");

    let bytes = b"the original the host still needs to read".to_vec();
    let path = user_dir.path().join("original.flac");
    std::fs::write(&path, &bytes).expect("write the user's file");
    handle
        .write_note_with_external_photo("note-1", "photo-1", &path, &bytes)
        .await
        .expect("register the user's file");

    let external = handle
        .external_blob("note_photos", "photo-1")
        .await
        .expect("read the registration")
        .expect("a registered row names its file");
    assert_eq!(external.path, path, "the host gets the file it registered");
    assert_eq!(
        external.size,
        bytes.len() as u64,
        "with the length the row was registered at",
    );
    // The file itself is where the bytes are — coven never copied them.
    assert_eq!(
        std::fs::read(&external.path).expect("read the user's file directly"),
        bytes,
    );

    handle
        .sql(|sql| {
            sql.clear_external_blob("note_photos", "photo-1")?;
            Ok(())
        })
        .await
        .expect("clear the registration");
    assert_eq!(
        handle
            .external_blob("note_photos", "photo-1")
            .await
            .expect("read the cleared registration"),
        None,
        "a cleared registration is an absence, not a failure",
    );
}
