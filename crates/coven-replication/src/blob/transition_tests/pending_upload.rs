//! Who owns a cloud object an upload created before its Store write was
//! accepted.
//!
//! Such an object is in nobody's accepted history: no snapshot inventory names
//! it, so accepted reclaim cannot see it. Its owner is the make_remote journal
//! that created it, and these tests pin the two ways that ownership has to end —
//! the transition refuses to hand the object to anyone else, and it takes the
//! object back out itself once it can no longer publish it.

use super::*;

/// Drive `make_remote` to the point where one photo's object is in the cloud and
/// the transition is still `uploading`, by removing the second photo's source so
/// its upload fails. Returns the created object.
async fn upload_one_of_two_photos(
    db: &Database,
    storage: &std::sync::Arc<TestStore>,
    lib: &StoreDir,
    owners: &TestOwnerGraph,
    user: &std::path::Path,
) -> coven_protocol::blob::locator::StoredBlobRef {
    owners
        .seed_local_release(user, "n1", "photoaaa", "cv/photoaaa.jpg", b"first")
        .await;
    let second = user.join("photobbb.jpg");
    std::fs::write(&second, b"second").unwrap();
    db.add_local_photo_for_test("n1", "photobbb", "cv/photobbb.jpg", b"second", &second)
        .await;
    owners
        .make_remote("notes", "n1", "Notes Root", true)
        .await
        .expect("make_remote");
    std::fs::remove_file(&second).unwrap();
    storage
        .drain_uploads(&StoreDatabase::new(db), lib, &SystemClock, None, None)
        .await
        .expect("partial drain");
    created_upload_blob(db, "photoaaa").await
}

/// A root whose blobs are uploaded but whose gate flip has not been accepted is
/// mid-transition, not Remote. Bringing it Local there would release objects
/// that only the upload journal can delete, so make_local refuses instead — and
/// refuses before it writes a single byte.
#[tokio::test]
async fn make_local_refuses_a_root_whose_make_remote_has_not_been_accepted() {
    let (db, storage, cloud_storage, tmp, lib, owners) = photo_transition_fixture().await;
    let user = tmp.path().join("user");
    owners
        .seed_local_release(&user, "n1", "photoaaa", "cv/photoaaa.jpg", b"first")
        .await;
    owners
        .make_remote("notes", "n1", "Notes Root", true)
        .await
        .expect("make_remote");
    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("drain the upload");
    assert_eq!(shared_flag(&db, "n1").await, 1, "the gate flip is captured");
    assert!(
        db.make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent"),
        "and its Store write has not been accepted yet",
    );
    let created = created_upload_blob(&db, "photoaaa").await;

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    let error = owners
        .make_local(
            cloud_storage.clone(),
            None,
            None,
            "notes",
            "n1",
            &dest,
            &cancel,
        )
        .await
        .expect_err("make_local refuses an unaccepted transition");
    assert!(
        matches!(
            error,
            crate::blob::transition::MakeLocalError::TransitionInProgress(ref table, ref id)
                if table == "notes" && id == "n1"
        ),
        "{error}",
    );
    assert!(!dest_path.exists(), "nothing was materialized");
    assert_eq!(shared_flag(&db, "n1").await, 1, "the root did not move");
    cloud_storage
        .clone()
        .verify_blob_object(&created)
        .await
        .expect("the pending object is untouched");
}

/// A row edited under an unaccepted make_remote leaves the object that upload
/// created with nothing left to publish it. The transition is what created the
/// object, so the transition is what takes it back out: the intent enters its
/// unwind and the drain deletes the object, its spool and its cached copy.
///
/// The whole of that decision is durable before any of it runs, so the database
/// is closed and reopened between the edit and the unwind: the drain that
/// settles it reads nothing the earlier one left in memory.
#[tokio::test]
async fn a_replaced_row_retires_its_created_upload_across_a_restart() {
    let lib = crate::sync::test_helpers::test_store_dir();
    let db = restartable_photo_db(&lib);
    let home = crate::sync::test_helpers::test_cloud_home();
    let (storage, cloud_storage) =
        create_store(&db, lib.clone(), UserKeypair::generate(), home.clone()).await;
    let tmp = tempfile::tempdir().expect("create external blob fixture directory");
    let user = tmp.path().join("user");
    let owners = TestOwnerGraph::new(StoreDatabase::new(&db), lib.clone());
    let created = upload_one_of_two_photos(&db, &storage, &lib, &owners, &user).await;
    cloud_storage
        .clone()
        .verify_blob_object(&created)
        .await
        .expect("the first photo reached the cloud");
    assert!(
        lib.pinned_blob_path("photos", created.locator().locator_hash())
            .unwrap()
            .exists(),
        "and its pinned copy is on disk",
    );

    // Repoint the uploaded row at different bytes. The journal still names the
    // version that was uploaded, which the root no longer has.
    db.execute_test_host_write(
        "UPDATE note_photos SET size = 9, hash = \
         '1f7a7a472abf3dd9643fd615f6da379c4acb3e3a1dc0b5fd1b3f9e1b4f6a4f2d', \
         _updated_at = '0000000002000-0000-A' WHERE id = 'photoaaa'",
    )
    .await;

    drop(owners);
    drop(db);
    let db = restartable_photo_db(&lib);
    assert!(
        db.make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent")
            && pending_uploads(&db).await == 2,
        "the reopened database carries the transition and both journals",
    );
    StoreDatabase::new(&db)
        .reset_outbox_backoff()
        .await
        .expect("a restart re-attempts past the backoff window");

    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("drain the unwind");

    assert_eq!(shared_flag(&db, "n1").await, 0, "the root stays Local");
    assert!(
        !db.make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent"),
        "the transition finished its unwind",
    );
    assert_eq!(pending_uploads(&db).await, 0, "both journals are consumed");
    assert!(
        cloud_storage
            .clone()
            .verify_blob_object(&created)
            .await
            .is_err(),
        "the object the replaced row orphaned is out of the cloud",
    );
    assert!(
        !lib.pinned_blob_path("photos", created.locator().locator_hash())
            .unwrap()
            .exists(),
        "and so is its pinned copy",
    );
}

/// A discarded publication is the third way a make_remote can stop short, and
/// the objects its uploads created are as ownerless as a cancelled one's: the
/// write that would have flipped the gate is gone, so nothing will ever publish
/// them. Retiring the write hands the transition back to its unwind, and the
/// drain takes the objects out — across a close and reopen of the database,
/// because the whole decision is a durable row before any of it runs.
#[tokio::test]
async fn a_discarded_publication_retires_its_created_upload_across_a_restart() {
    let lib = crate::sync::test_helpers::test_store_dir();
    let db = restartable_photo_db(&lib);
    let signer = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let (storage, cloud_storage) =
        create_store(&db, lib.clone(), signer.clone(), home.clone()).await;
    let tmp = tempfile::tempdir().expect("create external blob fixture directory");
    let owners = TestOwnerGraph::new(StoreDatabase::new(&db), lib.clone());
    owners
        .seed_local_release(
            &tmp.path().join("user"),
            "n1",
            "photoaaa",
            "cv/photoaaa.jpg",
            b"first",
        )
        .await;
    owners
        .make_remote("notes", "n1", "Notes Root", true)
        .await
        .expect("make_remote");
    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("drain the upload");

    let created = created_upload_blob(&db, "photoaaa").await;
    cloud_storage
        .clone()
        .verify_blob_object(&created)
        .await
        .expect("the photo reached the cloud");
    assert_eq!(shared_flag(&db, "n1").await, 1, "the gate flip is captured");
    let Some(coven_database::MakeRemoteIntentState::Publishing(write_id)) = StoreDatabase::new(&db)
        .make_remote_intent_state("notes", "n1")
        .await
        .expect("read the make_remote intent")
    else {
        panic!("a completed make_remote waits on its publication");
    };

    // Retire that publication through the path a host reaches for a write it
    // cannot publish: block it, then discard it and every unpublished write
    // behind it.
    StoreDatabase::new(&db)
        .set_write_status(
            &write_id,
            coven_protocol::write::WriteStatus::Blocked(
                coven_protocol::write::WriteBlock::InvalidProtocolState {
                    reason: "the gate flip cannot publish".to_string(),
                },
            ),
        )
        .await
        .expect("block the publication");
    let device = storage
        .bind_device_in(&db, lib.clone(), &signer)
        .await
        .expect("bind the owner device");
    assert_eq!(
        device
            .discard_blocked_write(write_id.clone())
            .await
            .expect("discard the blocked publication"),
        vec![write_id],
    );

    assert_eq!(
        shared_flag(&db, "n1").await,
        0,
        "reversing the write puts the root back where it was",
    );
    assert!(
        matches!(
            StoreDatabase::new(&db)
                .make_remote_intent_state("notes", "n1")
                .await
                .expect("read the make_remote intent"),
            Some(coven_database::MakeRemoteIntentState::Cancelling),
        ),
        "the transition the discarded write was carrying enters its unwind",
    );
    assert_eq!(
        pending_uploads(&db).await,
        1,
        "and its journal is still there, because only the drain can retire it",
    );

    drop(device);
    drop(owners);
    drop(db);
    let db = restartable_photo_db(&lib);
    assert!(
        matches!(
            StoreDatabase::new(&db)
                .make_remote_intent_state("notes", "n1")
                .await
                .expect("read the make_remote intent"),
            Some(coven_database::MakeRemoteIntentState::Cancelling),
        ) && pending_uploads(&db).await == 1,
        "the reopened database carries the unwind and its journal",
    );

    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("drain the unwind");

    assert_eq!(shared_flag(&db, "n1").await, 0, "the root stays Local");
    assert!(
        !db.make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent"),
        "the transition finished its unwind",
    );
    assert_eq!(pending_uploads(&db).await, 0, "the journal is consumed");
    assert!(
        cloud_storage
            .clone()
            .verify_blob_object(&created)
            .await
            .is_err(),
        "the object the discarded publication orphaned is out of the cloud",
    );
    assert!(
        staged_upload_copies(&lib).is_empty(),
        "and so is its upload spool",
    );
    assert!(
        !lib.pinned_blob_path("photos", created.locator().locator_hash())
            .unwrap()
            .exists(),
        "and its pinned copy",
    );
}
