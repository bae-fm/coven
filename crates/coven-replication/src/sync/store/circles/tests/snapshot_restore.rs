use super::*;
use crate::sync::test_helpers::{open_test_db_schema, test_store_dir};

fn tables() -> Vec<coven_protocol::synced_schema::SyncedTable> {
    vec![coven_protocol::synced_schema::SyncedTable::new(
        "documents",
        coven_protocol::synced_schema::RowIdentity::IndependentUuid,
    )
    .scoped_by("audience")]
}

fn migrations() -> Vec<coven_database::Migration> {
    vec![coven_database::Migration::sql(
        1,
        "Circle restore documents",
        "CREATE TABLE documents (
            id TEXT PRIMARY KEY,
            audience TEXT,
            _updated_at TEXT NOT NULL
        ) STRICT;",
    )]
}

#[tokio::test]
async fn excluded_device_continues_from_a_snapshot_after_a_later_circle_control() {
    let source_dir = test_store_dir();
    let source = open_test_db_schema(source_dir.clone(), tables(), migrations());
    let (store, home, signer, founder) =
        persist_merge_operation(&source, source_dir.clone(), "restore-excluded-device").await;
    let circle = founder.circle_id();
    let owner = store
        .bind_device_in(&source, source_dir.clone(), &signer)
        .await
        .expect("bind owner");
    owner
        .resume_circle_operations()
        .await
        .expect("create Circle");

    let silent_dir = test_store_dir();
    let silent = open_test_db_schema(silent_dir.clone(), tables(), migrations());
    let silent_device = store
        .activate_joined_device(
            &source,
            source_dir.clone(),
            &silent,
            silent_dir,
            &signer,
            "2026-07-24T01:00:00Z",
        )
        .await
        .expect("join a second device for the same identity");
    let silent_id = silent
        .local_store_device_id_for_test()
        .await
        .expect("read second device registration");
    let removed = keys::public_key_hex(&UserKeypair::generate());
    let routing = EncryptionService::from_key([42; 32]);
    store
        .admit_member(
            &source,
            source_dir.clone(),
            &signer,
            &removed,
            None,
            MemberRole::Member,
            &routing,
            "Restore excluded device",
        )
        .await
        .expect("admit the member whose removal will close the epoch");
    let components = prepare_owner_sync_components(
        &source,
        &store,
        &home,
        &source_dir,
        &signer,
        "restore-excluded-device",
        circle_test_custody(),
    )
    .await;
    components
        .add_circle_member(circle, removed.clone(), CircleRole::Member)
        .await
        .expect("add Circle member");
    let row = "00000000-0000-4000-8000-000000000001";
    source
        .capture_circle_document_for_test(row, circle, "0000000003000-0000-owner")
        .await
        .expect("capture a row before the close");
    components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish the covered Circle row");
    let (_, pulled) = silent_device
        .pull_store()
        .await
        .expect("second device reads the Circle");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let continuation = silent_device
        .export_activated_device_continuation()
        .await
        .expect("export the original device continuation");

    components
        .remove_circle_member(circle, removed)
        .await
        .expect("open the epoch close");
    owner
        .publish_circle_epoch_close_response()
        .await
        .expect("owner responds to close");
    components
        .exclude_circle_close_device(circle, silent_id)
        .await
        .expect("exclude the silent device");
    components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("finalize close with the second device excluded");
    let source_database = StoreDatabase::new(&source);
    let finalized = source_database
        .current_circle_control(circle)
        .await
        .expect("read finalization")
        .expect("active successor");
    let (closed_state, _) = source_database
        .circle_authoring_context(circle, &keys::public_key_hex(&signer))
        .await
        .expect("read metadata after closing the epoch");
    coven_protocol::hlc::Timestamp::parse(&closed_state.metadata.metadata_stamp)
        .expect("epoch close uses the logical clock for metadata ordering");
    components
        .rename_circle(circle, "Renamed after exclusion")
        .await
        .expect("advance within the successor epoch");
    let renamed = source_database
        .current_circle_control(circle)
        .await
        .expect("read renamed control")
        .expect("active renamed control");
    assert_ne!(renamed, finalized);
    let (renamed_state, _) = source_database
        .circle_authoring_context(circle, &keys::public_key_hex(&signer))
        .await
        .expect("read renamed metadata");
    assert_eq!(renamed_state.metadata.name, "Renamed after exclusion");
    let mut writer = owner
        .authorize_writer()
        .await
        .expect("authorize snapshot publisher");
    let cut = writer
        .snapshots()
        .capture_snapshot_cut(Some(&routing))
        .await
        .expect("capture accepted snapshot prefix");
    writer
        .snapshots()
        .push_snapshot_cut(cut, "2026-07-24T02:00:00Z".to_string())
        .await
        .expect("publish snapshot after the rename");
    drop(writer);
    let membership = owner
        .membership_for_test()
        .await
        .expect("read restore membership floor");
    let destination = tempfile::tempdir().expect("create restore destination");
    let directory = coven_foundation::store_dir::StoreDir::new_ephemeral(destination.path());
    let database_path = directory.db_path();
    let mut restored = store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            source.schema_version(),
            &database_path,
            &signer,
        )
        .await
        .expect("verify the shared snapshot")
        .install(
            &directory,
            tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "restored-host".to_string(),
            Arc::new(coven_foundation::clock::SystemClock),
            &migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            Some(&routing),
        )
        .await
        .expect("restore Circle content under the recipient identity");
    let pulled = restored
        .pull(Some(&routing))
        .await
        .expect("pull snapshot continuation");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    restored
        .install_activated_device_continuation(continuation)
        .await
        .expect("continue the excluded device after resetting its Circle image");
    restored
        .circle_publication_context_for_test(circle, renamed)
        .await
        .expect("the restored current image permits Circle publication");

    let rows = coven_database::DatabaseImageTest::open(&database_path).expect("read restored rows");
    let document: (String, String) = rows
        .query_row(
            "SELECT id, audience FROM documents WHERE id = ?1",
            [row],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("covered Circle row survives same-device recovery");
    assert_eq!(document, (row.to_string(), circle.to_string()));
}

#[tokio::test]
async fn later_added_member_restores_its_ancestor_bootstrap_after_same_epoch_rename() {
    restore_later_member(AncestorImage::Present).await;
}

#[tokio::test]
async fn later_member_restore_rejects_a_missing_ancestor_bootstrap() {
    restore_later_member(AncestorImage::Missing).await;
}

#[tokio::test]
async fn later_member_restore_rejects_a_corrupted_ancestor_bootstrap() {
    restore_later_member(AncestorImage::Corrupted).await;
}

enum AncestorImage {
    Present,
    Missing,
    Corrupted,
}

async fn restore_later_member(image: AncestorImage) {
    let source_dir = test_store_dir();
    let source = open_test_db_schema(source_dir.clone(), tables(), migrations());
    let (store, home, signer, founder) =
        persist_merge_operation(&source, source_dir.clone(), "restore-earlier-member-image").await;
    let circle = founder.circle_id();
    let owner = store
        .bind_device_in(&source, source_dir.clone(), &signer)
        .await
        .expect("bind owner");
    owner
        .resume_circle_operations()
        .await
        .expect("create Circle");
    let routing = EncryptionService::from_key([42; 32]);
    let components = prepare_owner_sync_components(
        &source,
        &store,
        &home,
        &source_dir,
        &signer,
        "restore-earlier-member-image",
        circle_test_custody(),
    )
    .await;
    let row = "00000000-0000-4000-8000-000000000002";
    source
        .capture_circle_document_for_test(row, circle, "0000000003000-0000-owner")
        .await
        .expect("capture content before the member joins");
    assert!(owner
        .publish_pending_store_database()
        .await
        .expect("publish content before the member joins"));
    assert!(
        !home.exact_creates().iter().any(|slot| {
            slot.logical_key().starts_with("circles/")
                && slot.logical_key().contains("/snapshot-images/")
        }),
        "the member's bootstrap is the only Circle image available for restoration"
    );
    let member = UserKeypair::generate();
    let member_pubkey = keys::public_key_hex(&member);
    store
        .admit_member(
            &source,
            source_dir.clone(),
            &signer,
            &member_pubkey,
            None,
            MemberRole::Member,
            &routing,
            "Restore earlier member image",
        )
        .await
        .expect("admit Store member");
    home.clear_exact_creates();
    components
        .add_circle_member(circle, member_pubkey, CircleRole::Member)
        .await
        .expect("publish the member's Circle bootstrap");
    let bootstrap_images: Vec<_> = home
        .exact_creates()
        .into_iter()
        .filter(|slot| {
            slot.logical_key().contains("/bootstraps/") && slot.logical_key().ends_with(".db")
        })
        .collect();
    assert_eq!(
        bootstrap_images.len(),
        1,
        "only the added member requires a bootstrap"
    );
    let bootstrap = &bootstrap_images[0];
    let database = StoreDatabase::new(&source);
    let added = database
        .current_circle_control(circle)
        .await
        .unwrap()
        .unwrap();
    let (before, _) = database
        .circle_authoring_context(circle, &keys::public_key_hex(&signer))
        .await
        .unwrap();
    components
        .rename_circle(circle, "Renamed with a later member")
        .await
        .expect("rename within the member's epoch");
    let renamed = database
        .current_circle_control(circle)
        .await
        .unwrap()
        .unwrap();
    let (after, _) = database
        .circle_authoring_context(circle, &keys::public_key_hex(&signer))
        .await
        .unwrap();
    assert_ne!(added, renamed);
    assert_eq!(
        before.control.value.epoch_id(),
        after.control.value.epoch_id()
    );
    let mut writer = owner.authorize_writer().await.unwrap();
    let cut = writer
        .snapshots()
        .capture_snapshot_cut(Some(&routing))
        .await
        .unwrap();
    writer
        .snapshots()
        .push_snapshot_cut(cut, "2026-07-24T02:00:00Z".to_string())
        .await
        .expect("publish shared snapshot after rename");
    drop(writer);
    let membership = owner.membership_for_test().await.unwrap();
    assert!(
        membership
            .current_members()
            .iter()
            .any(|(pubkey, _)| pubkey == &keys::public_key_hex(&member)),
        "the restore floor includes the admitted member"
    );
    let destination = tempfile::tempdir().unwrap();
    let directory = coven_foundation::store_dir::StoreDir::new_ephemeral(destination.path());
    let path = directory.db_path();
    match image {
        AncestorImage::Present => {}
        AncestorImage::Missing => home.remove_exact_object(bootstrap),
        AncestorImage::Corrupted => {
            home.replace_exact_object(bootstrap, b"changed image bytes".to_vec())
        }
    }
    home.clear_exact_reads();
    let restored = store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            source.schema_version(),
            &path,
            &member,
        )
        .await
        .expect("authenticate the shared image")
        .install(
            &directory,
            tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "restored-member".into(),
            Arc::new(coven_foundation::clock::SystemClock),
            &migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            Some(&routing),
        )
        .await;
    if !matches!(image, AncestorImage::Present) {
        assert!(
            restored.is_err(),
            "a required ancestor image failure must reject restoration; reads={:?}",
            home.exact_reads()
        );
        assert!(
            home.exact_reads().contains(bootstrap),
            "the required ancestor image was requested"
        );
        assert!(
            !path.exists(),
            "failed Circle restoration exposes no database"
        );
        return;
    }
    let restored = restored.expect("restore a member whose bootstrap precedes the current control");
    assert!(
        home.exact_reads().contains(bootstrap),
        "restoration must read the member's exact ancestor bootstrap, not infer founder access; expected={bootstrap:?}, reads={:?}",
        home.exact_reads()
    );
    let rows = coven_database::DatabaseImageTest::open(&path).unwrap();
    let actual: String = rows
        .query_row(
            "SELECT audience FROM documents WHERE id = ?1",
            [row],
            |row| row.get(0),
        )
        .expect("the member receives content from before its admission");
    assert_eq!(actual, circle.to_string());
    drop(restored);
}
