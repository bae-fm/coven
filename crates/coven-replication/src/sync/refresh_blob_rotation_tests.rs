use super::*;

const ROOT_ID: &str = "rotation-blob-root";
const T0: &str = "2026-08-20T00:00:00Z";

struct RotationFixture {
    device: crate::sync::test_helpers::TestDevice,
    /// Held so the database outlives the device that publishes through it.
    _db: coven_database::Database,
}

/// A Store holding one published user-provided blob, whose key has since
/// rotated because a member was removed.
async fn store_whose_key_rotated_after_an_upload() -> RotationFixture {
    let owner = UserKeypair::generate();
    let removed_member = UserKeypair::generate();
    let initial = EncryptionService::from_key([61u8; 32]);
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_with_blob(
        db_store_dir.clone(),
        coven_protocol::synced_schema::BlobDecl::new(
            "photos",
            coven_protocol::blob::Provenance::UserProvided,
            coven_protocol::blob::CacheFill::CacheLazy,
        ),
    );
    let (store, _connection) = Box::pin(TestStore::create_encrypted_with_connection(
        &db,
        db_store_dir.clone(),
        LIB_ID,
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
        initial.clone(),
    ))
    .await
    .expect("create the rotation fixture Store");
    Box::pin(store.open_into(&db, db_store_dir.clone()))
        .await
        .expect("open the rotation fixture Store");
    store
        .admit_exact_member(
            &db,
            db_store_dir.clone(),
            &owner,
            &removed_member,
            MemberRole::Member,
            &initial,
        )
        .await;

    // Upload and publish the blob under the first key generation.
    let database = coven_database::StoreDatabase::new(&db);
    let bytes = b"the bytes an earlier generation sealed";
    db.insert_local_upload_rows_for_test(ROOT_ID, &[("rotation-blob", bytes.as_slice())])
        .await
        .expect("plant the local blob row");
    let source = db_store_dir
        .db_path()
        .parent()
        .expect("Store directory has a parent")
        .join("rotation-blob.source");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&source, bytes)
        .await
        .expect("write the blob source");
    database
        .register_external_blob_for_test("note_photos", "rotation-blob", &source)
        .await;
    crate::sync::test_owner_graph::TestOwnerGraph::new(database.clone(), db_store_dir.clone())
        .make_remote("notes", ROOT_ID, "Notes Root", false)
        .await
        .expect("enqueue the blob upload");
    let device = store
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open the uploading device");
    device
        .drain_uploads(&SystemClock, None, None)
        .await
        .expect("upload the blob under the first key");
    assert!(
        device
            .prepare_pending_store_write()
            .await
            .expect("prepare the blob's Store write"),
        "the uploaded blob produces a Store write",
    );
    device
        .drain_store_writes()
        .await
        .expect("publish the blob's Store write");

    // Remove the member. That rotates the Store key, and the owner adopts
    // it, so everything sealed from here on uses a new generation.
    let custody = TestCustody::default();
    custody.set_initial_key(initial.key_bytes());
    let pending_rotation = PendingRotation::none();
    let rotated_membership = store
        .revoke_member_durable(
            &db,
            db_store_dir.clone(),
            &owner,
            &pubkey_hex(&removed_member),
            "0000000004000-0000-owner",
            &initial,
            &pending_rotation,
        )
        .await
        .expect("remove the member and rotate the Store key");
    let rotated = crate::sync::store::open_store_keyring(&owner, &rotated_membership)
        .expect("open the accepted rotation keys");
    // The running device adopts the rotation, so from here it seals under
    // the new generation while the blob it already published carries the old.
    device
        .adopt_key_rotation(&rotated, &custody)
        .expect("the owner device adopts the rotation");
    device
        .complete_revoke_rotation_adoption_for_test(&pending_rotation, rotated.current_generation())
        .await
        .expect("complete the removal journal");
    assert_ne!(
        rotated.seal_key_fingerprint(),
        initial.seal_key_fingerprint(),
        "the removal really did change the key new uploads are sealed under",
    );

    RotationFixture { device, _db: db }
}

/// Snapshot publication preflights every user-provided blob. Asking whether
/// the stored locator equals one minted under today's key answers "no" for
/// every blob uploaded before the rotation, and the Store never snapshots
/// again — the live wedge, which repeated every cycle.
#[tokio::test]
async fn a_snapshot_after_a_rotation_publishes_what_an_earlier_generation_uploaded() {
    let fixture = store_whose_key_rotated_after_an_upload().await;
    let mut writer = fixture
        .device
        .authorize_writer()
        .await
        .expect("authorize the snapshot writer");

    let cut = writer
        .snapshots()
        .capture_snapshot_cut(None)
        .await
        .expect("capture the snapshot cut");
    writer
        .snapshots()
        .push_snapshot_cut(cut, T0.to_string())
        .await
        .expect("a rotation does not strand the blobs already published");
}
