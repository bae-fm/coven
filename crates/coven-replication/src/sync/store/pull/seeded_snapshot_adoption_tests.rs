use crate::sync::test_helpers::{
    photo_decl, test_cloud_home, test_migrations, test_store_dir, test_synced_tables_with_blob,
    user_keypair_from_seed, TestDevice, TestStore,
};
use coven_database::{
    Database, DbError, HostWriteOperation, StoreDatabase, StoreRowWrites, WriteBatch,
};
use coven_foundation::store_dir::StoreDir;
use coven_keys::keys::UserKeypair;
use coven_protocol::blob::locator::StoredBlobRef;
use coven_protocol::write::{WriteId, WriteReceipt, WriteStatus};
use coven_storage::CloudSyncObjectStorage;
use futures_util::FutureExt;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

// The command generator selects host edits and delivery boundaries. Both sides
// execute the production owners; neither side implements replay in the test.
#[tokio::test]
async fn seeded_delivery_restart_and_retirement_preserve_rows_blobs_and_pending_work() {
    for seed in [0x18d7_aae1, 0x59c4_312b, 0xb07e_924d] {
        let result = std::panic::AssertUnwindSafe(exercise_sequence(seed))
            .catch_unwind()
            .await;
        assert!(result.is_ok(), "seed {seed:#x}; see the failing step above");
    }
}

#[path = "held_snapshot_adoption_tests.rs"]
mod held_predecessor;

#[derive(Debug, Clone, Copy)]
enum Edit {
    DeleteTag,
    ReplaceBlob,
    Withdraw,
    DeleteParent,
}

struct SnapshotAdoptionReceiver {
    database: Database,
    device: TestDevice,
    directory: StoreDir,
}

impl SnapshotAdoptionReceiver {
    async fn join(
        store: &TestStore,
        source: &Database,
        source_directory: &StoreDir,
        signer: &UserKeypair,
        context: &str,
    ) -> Self {
        let directory = test_store_dir();
        let database = open_database(&directory, "joining-seeded-receiver", context);
        let device = store
            .activate_joined_device(
                source,
                source_directory.clone(),
                &database,
                directory.clone(),
                signer,
                "2026-09-08T00:00:00Z",
            )
            .await
            .expect(context);
        Self {
            database,
            device,
            directory,
        }
    }

    async fn restart(self, store: &TestStore, signer: &UserKeypair, context: &str) -> Self {
        let Self {
            database,
            device,
            directory,
        } = self;
        let device_id = device.device_id();
        let before_rows = database.query_test_text(ROWS).await;
        let before_journal = StoreDatabase::new(&database)
            .store_write_journal_for_test()
            .await
            .expect(context);
        drop(device);
        drop(database);
        let database = open_database(&directory, &device_id, context);
        let device = store
            .bind_device_in(&database, directory.clone(), signer)
            .await
            .expect(context);
        assert_eq!(
            database.query_test_text(ROWS).await,
            before_rows,
            "{context}"
        );
        assert_eq!(
            StoreDatabase::new(&database)
                .store_write_journal_for_test()
                .await
                .expect(context),
            before_journal,
            "{context}: restart changes journal"
        );
        Self {
            database,
            device,
            directory,
        }
    }

    async fn pull(&self, context: &str) {
        let (_, pulled) = self.device.pull_store().await.expect(context);
        assert!(pulled.held_positions.is_empty(), "{context}: {pulled:?}");
    }
}

fn open_database(directory: &StoreDir, device_id: &str, context: &str) -> Database {
    Database::open_synthetic_for_test(
        // Join installs its image at db_path before copying it into this live
        // fixture database. Keep the receiving connection off that destination.
        &directory.db_path().with_file_name("scenario.db"),
        directory.clone(),
        test_synced_tables_with_blob(photo_decl().with_id_column("blob_id")),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        device_id.into(),
        Arc::new(coven_foundation::clock::SystemClock),
        &test_migrations(),
    )
    .expect(context)
}

async fn capture(
    database: &Database,
    device: &TestDevice,
    batch: WriteBatch,
    sql: String,
    context: &str,
) -> WriteReceipt<()> {
    StoreRowWrites::new(StoreDatabase::new(database))
        .execute(
            HostWriteOperation::new(batch, move |sql_context| {
                sql_context.execute_batch(&sql)?;
                Ok::<_, DbError>(())
            }),
            None,
            Some(Box::new(device.host_write_blob_staging())),
        )
        .await
        .expect(context)
}

async fn publish(device: &TestDevice, context: &str) {
    assert!(
        device
            .publish_pending_store_database()
            .await
            .expect(context),
        "{context}"
    );
}

fn photo_sql(
    root: &str,
    photo: &str,
    blob: &str,
    bytes: &[u8],
    shared: bool,
    stamp: &str,
) -> String {
    format!(
        "INSERT INTO notes(id, title, body, shared, _updated_at, created_at)
         VALUES ('{root}', '{root}', 'Original body', {}, '{stamp}', '2026-09-08');
         INSERT INTO note_photos(id, note_id, kind, blob_id, size, hash, _updated_at, created_at)
         VALUES ('{photo}', '{root}', 'image', '{blob}', {}, '{}', '{stamp}', '2026-09-08');",
        i32::from(shared),
        bytes.len(),
        coven_protocol::blob::content_hash(bytes),
    )
}

async fn exercise_sequence(seed: u64) {
    let mut random = StdRng::seed_from_u64(seed);
    let context = format!("seed {seed:#x}, setup");
    let source_directory = test_store_dir();
    let source = open_database(&source_directory, "seeded-source", &context);
    let signer = user_keypair_from_seed([61; 32]);
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_directory.clone(),
        "seeded-snapshot-adoption",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect(&context);
    let owner = store
        .bind_device_in(&source, source_directory.clone(), &signer)
        .await
        .expect(&context);
    let equal_bytes = b"equal bytes with distinct exact source objects";
    let mut sources = BTreeMap::new();
    let mut batch = WriteBatch::new();
    let mut sql = "INSERT INTO notes(id, title, body, shared, _updated_at, created_at)
        VALUES ('edit-target', 'Original title', 'Original body', 1,
        '0000000001000-0000-source', '2026-09-08');"
        .to_string();
    for id in ["exact-a", "exact-b"] {
        sources.insert(id.to_string(), equal_bytes.to_vec());
        batch.put_blob("photos", id, equal_bytes.to_vec());
        sql.push_str(&photo_sql(
            id,
            id,
            id,
            equal_bytes,
            true,
            "0000000001000-0000-source",
        ));
    }
    capture(&source, &owner, batch, sql, &context).await;
    publish(&owner, &context).await;
    let mut online =
        SnapshotAdoptionReceiver::join(&store, &source, &source_directory, &signer, &context).await;
    let mut delayed =
        SnapshotAdoptionReceiver::join(&store, &source, &source_directory, &signer, &context).await;
    owner.pull_store().await.expect(&context);
    online.pull(&context).await;
    delayed.pull(&context).await;

    let private_bytes = b"folded receiver private source";
    for receiver in [&online, &delayed] {
        let mut batch = WriteBatch::new();
        batch.put_blob("photos", "folded-private", private_bytes.to_vec());
        let receipt = capture(
            &receiver.database,
            &receiver.device,
            batch,
            photo_sql(
                "folded-private",
                "folded-private",
                "folded-private",
                private_bytes,
                false,
                "0000000002000-0000-receiver",
            ),
            &context,
        )
        .await;
        assert_eq!(receipt.status, WriteStatus::LocalOnly, "{context}");
    }
    let mut previous = owner
        .publish_snapshot_generation_for_test()
        .await
        .expect(&context);
    for receiver in [&online, &delayed] {
        receiver.pull(&context).await;
        receiver
            .device
            .stand_on_accepted_snapshot()
            .await
            .expect(&context);
        assert_eq!(
            StoreDatabase::new(&receiver.database)
                .store_write_status_count_for_test(WriteStatus::LocalOnly)
                .await
                .expect(&context),
            0,
            "{context}: private prefix did not fold"
        );
    }

    let pending_bytes = b"unpublished receiver private source";
    let mut pending = Vec::new();
    for receiver in [&online, &delayed] {
        let mut batch = WriteBatch::new();
        batch.put_blob("photos", "pending-private", pending_bytes.to_vec());
        let mut sql = photo_sql(
            "pending-private",
            "pending-private",
            "pending-private",
            pending_bytes,
            false,
            "0000000003000-0000-receiver",
        );
        sql.push_str(
            "UPDATE notes SET title = 'Recorded local title',
            _updated_at = '0000000003000-0000-receiver' WHERE id = 'edit-target';",
        );
        let receipt = capture(&receiver.database, &receiver.device, batch, sql, &context).await;
        let mut writer = receiver.device.authorize_writer().await.expect(&context);
        assert!(
            writer.prepare_pending_store_write().await.expect(&context),
            "{context}"
        );
        drop(writer);
        let records = StoreDatabase::new(&receiver.database);
        let original = records
            .store_write_capture_for_test(receipt.write_id.clone())
            .await
            .expect(&context);
        let active = records
            .active_store_publication()
            .await
            .expect(&context)
            .expect(&context);
        assert_eq!(
            records
                .write_status(&receipt.write_id)
                .await
                .expect(&context),
            WriteStatus::Publishing,
            "{context}: initial candidate was not prepared"
        );
        assert!(!active.is_awaiting_preparation(), "{context}");
        pending.push((receipt.write_id, original, active));
    }
    let mut edits = [
        Edit::DeleteTag,
        Edit::ReplaceBlob,
        Edit::Withdraw,
        Edit::DeleteParent,
    ];
    edits.shuffle(&mut random);
    let mut completed = Vec::new();
    for (step, edit) in edits.into_iter().enumerate() {
        let context = format!("seed {seed:#x}, step {step}, {edit:?}");
        let root = format!("generated-{step}");
        let photo = format!("photo-{step}");
        let tag = format!("tag-{step}");
        let bytes = format!("seed {seed:#x} original blob {step}").into_bytes();
        sources.insert(photo.clone(), bytes.clone());
        let stamp = format!("00000000{:05}-0000-source", 4000 + step * 100);
        let mut batch = WriteBatch::new();
        batch.put_blob("photos", &photo, bytes.clone());
        let mut sql = photo_sql(&root, &photo, &photo, &bytes, false, &stamp);
        sql.push_str(&format!(
            "INSERT INTO note_tags VALUES
            ('{tag}', '{root}', 'Private child', '{stamp}', '2026-09-08');"
        ));
        let private = capture(&source, &owner, batch, sql, &context).await;
        assert_eq!(private.status, WriteStatus::LocalOnly, "{context}");
        let stamp = format!("00000000{:05}-0000-source", 4001 + step * 100);
        capture(
            &source,
            &owner,
            WriteBatch::new(),
            format!(
                "UPDATE notes SET title = 'Edited while sharing', shared = 1,
             _updated_at = '{stamp}' WHERE id = '{root}';"
            ),
            &context,
        )
        .await;
        publish(&owner, &context).await;
        online
            .pull(&format!("{context}, online after sharing"))
            .await;
        if random.random_bool(0.5) {
            delayed
                .pull(&format!("{context}, delayed after sharing"))
                .await;
        }
        let stamp = format!("00000000{:05}-0000-source", 4050 + step * 100);
        let mut batch = WriteBatch::new();
        let mut sql = format!(
            "UPDATE notes SET body = 'Accepted body {step}',
            _updated_at = '{stamp}' WHERE id = 'edit-target';"
        );
        match edit {
            Edit::DeleteTag => sql.push_str(&format!("DELETE FROM note_tags WHERE id = '{tag}';")),
            Edit::ReplaceBlob => {
                let replacement = format!("replacement-{step}");
                let bytes = format!("seed {seed:#x} replacement blob {step}").into_bytes();
                sources.insert(replacement.clone(), bytes.clone());
                batch.put_blob("photos", &replacement, bytes.clone());
                sql.push_str(&format!(
                    "UPDATE note_photos SET blob_id = '{replacement}', size = {},
                    hash = '{}', _updated_at = '{stamp}' WHERE id = '{photo}';",
                    bytes.len(),
                    coven_protocol::blob::content_hash(&bytes)
                ));
            }
            Edit::Withdraw => sql.push_str(&format!(
                "UPDATE notes SET shared = 0,
                title = 'Private again', _updated_at = '{stamp}' WHERE id = '{root}';"
            )),
            Edit::DeleteParent => sql.push_str(&format!("DELETE FROM notes WHERE id = '{root}';")),
        }
        capture(&source, &owner, batch, sql, &context).await;
        publish(&owner, &context).await;
        if matches!(edit, Edit::Withdraw) {
            let receipt = capture(
                &source,
                &owner,
                WriteBatch::new(),
                format!(
                    "UPDATE note_tags SET tag = 'Edited after withdrawal',
                 _updated_at = '{stamp}' WHERE id = '{tag}';"
                ),
                &context,
            )
            .await;
            assert_eq!(receipt.status, WriteStatus::LocalOnly, "{context}");
        }
        completed.push((step, edit));
        online.pull(&format!("{context}, online after edit")).await;
        if random.random_bool(0.5) {
            online = online.restart(&store, &signer, &context).await;
        }
        // The last interval is always withheld, guaranteeing that the two
        // receivers exercise different reconstruction inputs for every seed.
        if step != 3 && random.random_bool(0.5) {
            delayed
                .pull(&format!("{context}, delayed after edit"))
                .await;
        }
        if step % 2 == 1 || random.random_bool(0.5) {
            let before_rows = source.query_test_text(ROWS).await;
            let online_before = StoreDatabase::new(&online.database)
                .store_current_publication()
                .await
                .expect(&context);
            assert_eq!(
                online_before,
                StoreDatabase::new(&source)
                    .store_current_publication()
                    .await
                    .expect(&context),
                "{context}: current receiver has not observed the complete prefix"
            );
            let online_frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
                online.device.materialized_frontier().await.expect(&context),
            )
            .expect(&context);
            let current = owner
                .publish_snapshot_generation_for_test()
                .await
                .expect(&context);
            assert_eq!(
                &current.meta.publication_predecessor,
                online_before.record(),
                "{context}: snapshot captured a different accepted predecessor"
            );
            assert_eq!(
                current.meta.coverage, online_frontier,
                "{context}: current receiver has not materialized the snapshot cut"
            );
            owner.stand_on_accepted_snapshot().await.expect(&context);
            home.clear_exact_reads();
            online
                .pull(&format!("{context}, online after new snapshot"))
                .await;
            online
                .device
                .stand_on_accepted_snapshot()
                .await
                .expect(&context);
            assert!(
                !home
                    .exact_reads()
                    .contains(current.meta.image.object.slot()),
                "{context}: current receiver downloaded an image despite owning the interval"
            );
            owner.reclaim_packages().await.expect(&context);
            assert!(
                home.contains_exact_object(&current.meta.image.object),
                "{context}: reclaim removed the current accepted snapshot image"
            );
            assert!(
                !home.contains_exact_object(&previous.reference.object),
                "{context}: the previous snapshot metadata was not physically retired"
            );
            assert!(
                !home.contains_exact_object(&previous.meta.image.object),
                "{context}: the previous image was not physically retired"
            );
            assert_eq!(
                source.query_test_text(ROWS).await,
                before_rows,
                "{context}: compaction changed the publisher's private/shared projection"
            );
            // Reclaim publishes its authorization and completion commits. Both
            // receivers must observe that suffix before their state is compared.
            online
                .pull(&format!("{context}, online after reclaim suffix"))
                .await;
            if random.random_bool(0.5) || step == 3 {
                delayed = delayed.restart(&store, &signer, &context).await;
            }
            home.clear_exact_reads();
            delayed.pull(&format!("{context}, delayed adoption")).await;
            delayed
                .device
                .stand_on_accepted_snapshot()
                .await
                .expect(&context);
            if step == 3 {
                assert!(
                    home.exact_reads()
                        .contains(current.meta.image.object.slot()),
                    "{context}: delayed receiver did not exercise downloaded-image adoption"
                );
            }
            for retired in [&previous.reference.object, &previous.meta.image.object] {
                assert!(
                    !home.exact_reads().contains(retired.slot()),
                    "{context}: attempted to read a retired snapshot artifact {retired:?}"
                );
            }
            assert_receivers(&online, &delayed, storage.as_ref(), &sources, &context).await;
            assert_generated_outcomes(
                &source,
                &source_directory,
                [&online, &delayed],
                &completed,
                &sources,
                &context,
            )
            .await;
            for (receiver, (write_id, original, reservation)) in
                [&online, &delayed].into_iter().zip(&pending)
            {
                assert_pending(receiver, write_id, original, reservation, &context).await;
            }
            previous = current;
        }
    }
    online = online.restart(&store, &signer, &context).await;
    delayed = delayed.restart(&store, &signer, &context).await;
    let context = format!("seed {seed:#x}, after final restart");
    assert_receivers(&online, &delayed, storage.as_ref(), &sources, &context).await;
    assert_generated_outcomes(
        &source,
        &source_directory,
        [&online, &delayed],
        &completed,
        &sources,
        &context,
    )
    .await;
    for (receiver, (write_id, original, reservation)) in
        [&online, &delayed].into_iter().zip(&pending)
    {
        assert_pending(receiver, write_id, original, reservation, &context).await;
    }
}

async fn assert_pending(
    receiver: &SnapshotAdoptionReceiver,
    write_id: &WriteId,
    original: &(String, String, String),
    reservation: &coven_database::ActiveStorePublication,
    context: &str,
) {
    let records = StoreDatabase::new(&receiver.database);
    assert_eq!(
        records
            .store_write_capture_for_test(write_id.clone())
            .await
            .expect(context),
        *original,
        "{context}: changed immutable capture for {write_id}"
    );
    assert_eq!(
        records.write_status(write_id).await.expect(context),
        WriteStatus::Pending,
        "{context}: retired preparation did not return the write to pending"
    );
    let active = records
        .active_store_publication()
        .await
        .expect(context)
        .expect(context);
    assert_eq!(
        active.commit_reservation(),
        reservation.commit_reservation(),
        "{context}: rebase changed the logical author reservation"
    );
    assert!(
        active.is_awaiting_preparation(),
        "{context}: snapshot did not retire old preparation"
    );
    assert_eq!(
        records
            .pending_writes()
            .await
            .expect(context)
            .iter()
            .map(|pending| &pending.write_id)
            .collect::<Vec<_>>(),
        vec![write_id],
        "{context}: lost or duplicated the unresolved mixed write"
    );
    assert_eq!(
        records
            .write_blob_lease_count_for_test(write_id)
            .await
            .expect(context),
        1,
        "{context}: pending private source lease"
    );
}

const ROWS: &str = "SELECT json_array(
    (SELECT json_group_array(json_array(id, title, body, shared)) FROM (SELECT * FROM notes ORDER BY id)),
    (SELECT json_group_array(json_array(id, note_id, tag)) FROM (SELECT * FROM note_tags ORDER BY id)),
    (SELECT json_group_array(json_array(id, note_id, kind, blob_id, size, hash))
     FROM (SELECT * FROM note_photos ORDER BY id)))";

async fn assert_receivers(
    left: &SnapshotAdoptionReceiver,
    right: &SnapshotAdoptionReceiver,
    storage: &dyn CloudSyncObjectStorage,
    sources: &BTreeMap<String, Vec<u8>>,
    context: &str,
) {
    assert_eq!(
        left.database.query_test_text(ROWS).await,
        right.database.query_test_text(ROWS).await,
        "{context}: row values, relationships or locality differ"
    );
    let left_records = StoreDatabase::new(&left.database);
    let right_records = StoreDatabase::new(&right.database);
    assert_eq!(
        left_records
            .store_current_publication()
            .await
            .expect(context),
        right_records
            .store_current_publication()
            .await
            .expect(context),
        "{context}: accepted state"
    );
    let left_baseline = left_records
        .installed_replay_baseline()
        .await
        .expect(context);
    let right_baseline = right_records
        .installed_replay_baseline()
        .await
        .expect(context);
    assert_eq!(
        left_baseline.coverage(),
        right_baseline.coverage(),
        "{context}: coverage"
    );
    assert_eq!(
        left_baseline.snapshot().map(|snapshot| &snapshot.reference),
        right_baseline
            .snapshot()
            .map(|snapshot| &snapshot.reference),
        "{context}: baseline identity"
    );
    let snapshot = left_baseline.snapshot().expect(context);
    let current_owner = coven_protocol::remote_object::SnapshotObjectOwner::Store {
        metadata_slot: snapshot.reference.object.slot().clone(),
    };
    let protected_snapshots = snapshot
        .meta
        .history_summary
        .pending_device_join_snapshot_slots();
    assert_eq!(
        left_records
            .row_blob_bindings_for_test()
            .await
            .expect(context),
        right_records
            .row_blob_bindings_for_test()
            .await
            .expect(context),
        "{context}: exact bindings"
    );
    let mut retained = Vec::new();
    for receiver in [left, right] {
        retained.push(
            receiver
                .device
                .retained_merge_replay_inputs_for_test()
                .await
                .expect(context)
                .iter()
                .map(|input| serde_json::to_string(input.commit_ref()).expect(context))
                .collect::<BTreeSet<_>>(),
        );
        assert_eq!(
            receiver
                .database
                .query_test_text("SELECT title FROM notes WHERE id = 'edit-target'")
                .await,
            "Recorded local title",
            "{context}: pending column was replaced"
        );
        for (id, bytes) in [
            (
                "folded-private",
                b"folded receiver private source".as_slice(),
            ),
            (
                "pending-private",
                b"unpublished receiver private source".as_slice(),
            ),
        ] {
            let reference = StoreDatabase::new(&receiver.database)
                .row_blob_ref("note_photos", id)
                .await
                .expect(context);
            assert!(
                reference.stored().is_none(),
                "{context}: private blob became shared"
            );
            assert_eq!(
                receiver
                    .database
                    .query_test_text(&format!(
                        "SELECT blob_id FROM note_photos WHERE id = '{id}'"
                    ))
                    .await,
                id,
                "{context}: private source identity changed"
            );
            assert_eq!(
                receiver
                    .directory
                    .read_local_blob("photos", id, bytes.len() as u64)
                    .await
                    .expect(context),
                Some(bytes.to_vec()),
                "{context}: private bytes {id}"
            );
            assert_eq!(
                receiver
                    .database
                    .query_test_text(&format!(
                        "SELECT CAST(shared AS TEXT) FROM notes WHERE id = '{id}'"
                    ))
                    .await,
                "0",
                "{context}: private row locality {id}"
            );
        }
    }
    assert_eq!(
        retained[0], retained[1],
        "{context}: retained exact replay references"
    );
    let mut exact_objects = BTreeMap::new();
    let shared_photos: Vec<String> = serde_json::from_str(
        &left
            .database
            .query_test_text(
                "SELECT json_group_array(id) FROM (SELECT p.id FROM note_photos p
         JOIN notes n ON n.id = p.note_id WHERE n.shared = 1 ORDER BY p.id)",
            )
            .await,
    )
    .expect(context);
    for id in &shared_photos {
        let left_blob = left_records
            .row_blob_ref("note_photos", id)
            .await
            .expect(context);
        let right_blob = right_records
            .row_blob_ref("note_photos", id)
            .await
            .expect(context);
        assert_eq!(left_blob, right_blob, "{context}: exact blob identity {id}");
        let stored = left_blob.stored().expect(context);
        exact_objects.insert(id.as_str(), stored.object().clone());
        assert_plaintext(
            storage,
            stored,
            sources.get(stored.locator().blob_id()).expect(context),
            context,
        )
        .await;
        let left_owner = left
            .database
            .remote_object_for_test(stored.object().clone())
            .await
            .expect(context);
        let right_owner = right
            .database
            .remote_object_for_test(stored.object().clone())
            .await
            .expect(context);
        let left_pins = left
            .database
            .retained_replay_pins_for_test(stored.object().clone())
            .await
            .expect(context);
        let right_pins = right
            .database
            .retained_replay_pins_for_test(stored.object().clone())
            .await
            .expect(context);
        for (side, remote) in [("online", &left_owner), ("delayed", &right_owner)] {
            assert!(
                remote.snapshot_owners().all(|owner| {
                    owner == &current_owner
                        || matches!(owner,
                            coven_protocol::remote_object::SnapshotObjectOwner::Store { metadata_slot }
                                if protected_snapshots.contains(metadata_slot))
                }),
                "{context}: {side} retained an unprotected old snapshot owner"
            );
            assert!(
                remote.snapshot_owners().any(|owner| owner == &current_owner),
                "{context}: {side} omitted current snapshot ownership; protected snapshots: {protected_snapshots:?}"
            );
        }
        assert_eq!(
            left_owner.snapshot_owners().collect::<Vec<_>>(),
            right_owner.snapshot_owners().collect::<Vec<_>>(),
            "{context}: snapshot blob owners"
        );
        assert_eq!(left_pins, right_pins, "{context}: replay blob pins");
        assert_eq!(
            left_owner.stored_blob_commit_owners(),
            right_owner.stored_blob_commit_owners(),
            "{context}: exact blob provenance"
        );
    }
    assert_ne!(
        exact_objects["exact-a"], exact_objects["exact-b"],
        "{context}: equal content collapsed exact identity"
    );
}

async fn assert_generated_outcomes(
    source: &Database,
    source_directory: &StoreDir,
    receivers: [&SnapshotAdoptionReceiver; 2],
    completed: &[(usize, Edit)],
    sources: &BTreeMap<String, Vec<u8>>,
    context: &str,
) {
    let (latest_step, _) = completed.last().expect(context);
    for receiver in receivers {
        assert_eq!(
            receiver
                .database
                .query_test_text("SELECT body FROM notes WHERE id = 'edit-target'")
                .await,
            format!("Accepted body {latest_step}"),
            "{context}: the untouched accepted column was lost"
        );
    }
    for (step, edit) in completed {
        let root = format!("generated-{step}");
        let photo = format!("photo-{step}");
        let tag = format!("tag-{step}");
        match edit {
            Edit::DeleteTag => {
                for database in [source, &receivers[0].database, &receivers[1].database] {
                    assert!(
                        !database
                            .test_row_exists(&format!("SELECT 1 FROM note_tags WHERE id = '{tag}'"))
                            .await,
                        "{context}: deleted private-origin child {tag} returned"
                    );
                    assert!(
                        database
                            .test_row_exists(&format!(
                                "SELECT 1 FROM notes WHERE id = '{root}' AND shared = 1"
                            ))
                            .await,
                        "{context}: shared parent {root} disappeared"
                    );
                }
            }
            Edit::ReplaceBlob => {
                let expected_id = format!("replacement-{step}");
                for database in [source, &receivers[0].database, &receivers[1].database] {
                    let blob = StoreDatabase::new(database)
                        .row_blob_ref("note_photos", &photo)
                        .await
                        .expect(context);
                    assert_eq!(
                        blob.stored().expect(context).locator().blob_id(),
                        expected_id,
                        "{context}: old blob remained after content replacement"
                    );
                }
            }
            Edit::Withdraw => {
                assert_eq!(
                    source
                        .query_test_text(&format!(
                            "SELECT title || ':' || shared FROM notes WHERE id = '{root}'"
                        ))
                        .await,
                    "Private again:0",
                    "{context}: withdrawn row changed"
                );
                assert_eq!(
                    source
                        .query_test_text(&format!("SELECT tag FROM note_tags WHERE id = '{tag}'"))
                        .await,
                    "Edited after withdrawal",
                    "{context}: later private edit disappeared"
                );
                let bytes = sources.get(&photo).expect(context);
                assert_eq!(
                    source_directory
                        .read_local_blob("photos", &photo, bytes.len() as u64)
                        .await
                        .expect(context),
                    Some(bytes.clone()),
                    "{context}: withdrawn private bytes"
                );
                for receiver in receivers {
                    assert!(
                        !receiver
                            .database
                            .test_row_exists(&format!("SELECT 1 FROM notes WHERE id = '{root}'"))
                            .await,
                        "{context}: a withdrawn row was exposed to a peer"
                    );
                    assert!(
                        !receiver
                            .database
                            .test_row_exists(&format!(
                                "SELECT 1 FROM note_photos WHERE id = '{photo}'"
                            ))
                            .await,
                        "{context}: a withdrawn photo was exposed to a peer"
                    );
                }
            }
            Edit::DeleteParent => {
                for database in [source, &receivers[0].database, &receivers[1].database] {
                    assert!(
                        !database
                            .test_row_exists(&format!("SELECT 1 FROM notes WHERE id = '{root}'"))
                            .await,
                        "{context}: deleted parent {root} returned"
                    );
                    for (table, id) in [("note_tags", &tag), ("note_photos", &photo)] {
                        assert!(
                            !database
                                .test_row_exists(&format!(
                                    "SELECT 1 FROM {table} WHERE id = '{id}'"
                                ))
                                .await,
                            "{context}: child {table}/{id} survived parent deletion"
                        );
                    }
                }
            }
        }
    }
}

async fn assert_plaintext(
    storage: &dyn CloudSyncObjectStorage,
    blob: &StoredBlobRef,
    bytes: &[u8],
    context: &str,
) {
    let directory = tempfile::tempdir().expect(context);
    let destination = directory.path().join("plaintext");
    let stage = StoreDir::new_ephemeral(directory.path())
        .stage_atomic_file(&destination)
        .await
        .expect(context);
    let plaintext = storage
        .stage_verified_store_blob_plaintext(
            blob,
            stage,
            coven_storage::cloud::no_download_progress(),
        )
        .await
        .expect(context);
    assert_eq!(
        tokio::fs::read(plaintext.path()).await.expect(context),
        bytes,
        "{context}: blob bytes"
    );
    plaintext.commit().await.expect(context);
}
