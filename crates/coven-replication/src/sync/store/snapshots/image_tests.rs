use std::collections::{BTreeMap, BTreeSet};

use super::*;
use coven_database::Database;
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;
use coven_protocol::store_commit::CommitFrontier;

fn scoped_snapshot_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "documents",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience"),
        SyncedTable::new(
            "paragraphs",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        )
        .inherits_audience_through("document_id"),
    ]
}

fn open_scoped_snapshot_test_db(store_dir: coven_foundation::store_dir::StoreDir) -> Database {
    crate::sync::test_helpers::open_test_db_schema(
        store_dir,
        scoped_snapshot_tables(),
        vec![Migration::sql(
            1,
            "scoped snapshot schema",
            "CREATE TABLE documents (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     body TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE paragraphs (
                     id TEXT PRIMARY KEY,
                     document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                     body TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
        )],
    )
}

async fn create_snapshot_circle(
    source: &Database,
    store: &crate::sync::test_helpers::TestStore,
    store_dir: &coven_foundation::store_dir::StoreDir,
) -> coven_protocol::circle::CircleId {
    store
        .open_into(source, store_dir.clone())
        .await
        .expect("open snapshot Circle owner")
        .create_circle("0000000001000-0000-owner", "Snapshot Circle")
        .await
        .expect("publish snapshot Circle creation")
}

async fn seed_scoped_snapshot_rows(
    source: &Database,
    store: &crate::sync::test_helpers::TestStore,
    store_dir: &coven_foundation::store_dir::StoreDir,
) -> coven_protocol::circle::CircleId {
    let database = StoreDatabase::new(source);
    let circle = create_snapshot_circle(source, store, store_dir).await;
    let write_circle = circle;
    database
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            move |transaction| {
                transaction.execute(
                    "INSERT INTO documents VALUES (?1, NULL, ?2, ?3)",
                    (
                        "01890a5d-ac96-774b-bcce-b302099c3f74",
                        "Store document",
                        "0000000001000-0000-owner",
                    ),
                )?;
                transaction.execute(
                    "INSERT INTO paragraphs VALUES (?1, ?2, ?3, ?4)",
                    (
                        "f47ac10b-58cc-4372-a567-0e02b2c3d479",
                        "01890a5d-ac96-774b-bcce-b302099c3f74",
                        "Store paragraph",
                        "0000000001001-0000-owner",
                    ),
                )?;
                transaction.execute(
                    "INSERT INTO documents VALUES (?1, ?2, ?3, ?4)",
                    (
                        "2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7",
                        write_circle.to_string(),
                        "Circle document",
                        "0000000001002-0000-owner",
                    ),
                )?;
                transaction.execute(
                    "INSERT INTO paragraphs VALUES (?1, ?2, ?3, ?4)",
                    (
                        "82df8bb7-52f0-44db-a8e7-3ec0e44cd609",
                        "2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7",
                        "Circle paragraph",
                        "0000000001003-0000-owner",
                    ),
                )?;
                transaction.execute(
                    "INSERT INTO documents VALUES (?1, 'local', ?2, ?3)",
                    (
                        "4a1b99f1-9d07-40d3-b6ac-b746e8d59983",
                        "Local document",
                        "0000000001004-0000-owner",
                    ),
                )?;
                transaction.execute(
                    "INSERT INTO paragraphs VALUES (?1, ?2, ?3, ?4)",
                    (
                        "5fe26b58-ecf7-48b1-bb20-13469b5b9be9",
                        "4a1b99f1-9d07-40d3-b6ac-b746e8d59983",
                        "Local paragraph",
                        "0000000001005-0000-owner",
                    ),
                )?;
                Ok(())
            },
        )
        .await
        .expect("commit scoped snapshot rows");
    assert!(store
        .publish_pending(source, store_dir)
        .await
        .expect("publish scoped snapshot rows"));
    circle
}

fn edit_snapshot_image(
    _image_dir: &Path,
    image: Vec<u8>,
    edit: impl FnOnce(&coven_database::DatabaseImageTest),
) -> Vec<u8> {
    let connection =
        coven_database::DatabaseImageTest::from_bytes(&image).expect("open edited snapshot image");
    edit(&connection);
    connection
        .into_bytes()
        .expect("serialize edited snapshot image")
}

#[tokio::test]
async fn snapshot_keeps_its_exact_device_state_tip_and_retires_earlier_references() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let signer = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "snapshot-device-state-frontier",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create device-state snapshot Store");
    let loaded_store = store
        .bind_device_in(&source, source_store_dir.clone(), &signer)
        .await
        .expect("load device-state snapshot Store");
    let mut writer = loaded_store
        .authorize_writer()
        .await
        .expect("authorize device-state snapshot writer");
    for sequence in 1..=3 {
        source
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('snapshot-state-{sequence}', 'state', NULL, 1, \
                             '000000000100{sequence}-0000-state', '2026-07-21')"
            ))
            .await;
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare snapshot history write"));
        assert_eq!(
            writer
                .drain_store_writes()
                .await
                .expect("publish snapshot history write"),
            1,
        );
    }
    let accepted = source
        .store_device_state_snapshot_refs_for_test()
        .await
        .expect("load accepted device-state references")
        .into_iter()
        .map(|reference| serde_json::to_string(&reference).expect("encode accepted reference"))
        .collect::<BTreeSet<_>>();
    assert_eq!(accepted.len(), 3);
    let expected = StoreDatabase::new(&source)
        .materialized_frontier()
        .await
        .expect("exact accepted frontier")
        .into_values()
        .map(|reference| serde_json::to_string(&reference).expect("encode frontier reference"))
        .collect::<BTreeSet<_>>();
    assert_eq!(expected.len(), 1);
    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image_path = image_dir.path().to_path_buf();
    let root = store.root().clone();
    let image = StoreDatabase::new(&source)
        .capture_snapshot_image_for_test(root, image_path, None)
        .await
        .expect("create scoped snapshot image");
    let scoped =
        coven_database::DatabaseImageTest::from_bytes(&image).expect("open scoped snapshot image");
    let actual = scoped
        .store_device_state_snapshot_refs()
        .expect("read scoped device states")
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
    assert_eq!(
        scoped
            .coven_table_row_count(coven_database::DatabaseTestTable::named(
                "store_device_states"
            ))
            .expect("count shared state bodies"),
        1,
    );
}

#[tokio::test]
async fn bootstrap_installs_the_verified_exact_store_root() {
    Box::pin(async {
        let source_store_dir = crate::sync::test_helpers::test_store_dir();
        let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            source_store_dir.clone(),
            "snapshot-bootstrap-exact-root",
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create exact bootstrap Store");
        let device = store
            .open_into(&source, source_store_dir.clone())
            .await
            .expect("open bootstrap Store membership");
        let membership = device
            .membership_for_test()
            .await
            .expect("project bootstrap Store membership");
        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let tables = crate::sync::test_helpers::test_synced_tables();
        let root = store.root().clone();
        let source_database = StoreDatabase::new(&source);
        let image = source_database
            .capture_snapshot_image_for_test(root, image_path, None)
            .await
            .expect("create bootstrap database image");
        let coverage = captured_coverage(&source_database).await;
        let published_snapshot = device
            .publish_snapshot(image, coverage.clone())
            .await
            .expect("publish bootstrap database image");
        device
            .stage_acknowledgement(coverage, "2026-07-16T00:00:01Z".to_string())
            .await
            .expect("stage snapshot stability acknowledgement");
        device
            .drain_acknowledgements()
            .await
            .expect("activate snapshot stability acknowledgement");

        let destination = tempfile::tempdir().expect("bootstrap destination");
        let database_path = destination.path().join("store.db");
        let bootstrap = store
            .prepare_snapshot_bootstrap(
                &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
                1,
                &database_path,
                &signer,
            )
            .await
            .expect("verify bootstrap authority");
        let store_dir = coven_foundation::store_dir::StoreDir::new_ephemeral(destination.path());
        let installed = bootstrap
            .install(
                &store_dir,
                tables,
                coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
                coven_protocol::blob::TransferLimits::one_at_a_time(),
                "joining-device".to_string(),
                std::sync::Arc::new(coven_foundation::clock::SystemClock),
                &crate::sync::test_helpers::test_migrations(),
                coven_database::CovenMigrationPolicy::ApplyPending,
                None,
            )
            .await
            .expect("install bootstrap authority");

        assert_eq!(
            installed
                .installed_store_root_for_test()
                .await
                .expect("read installed Store root"),
            Some(store.root().clone()),
        );
        let baseline = installed
            .generation_zero_replay_baseline_for_test()
            .await
            .expect("load installed snapshot replay baseline");
        assert_eq!(baseline.exact_cut, published_snapshot.coverage);
        match &baseline.authority {
            coven_database::RetainedReplayAuthority::InstalledSnapshot(authority) => {
                assert_eq!(authority.store_root, store.root());
                assert_eq!(authority.metadata, published_snapshot);
            }
            coven_database::RetainedReplayAuthority::Genesis(_) => {
                panic!("snapshot bootstrap installed a genesis replay baseline")
            }
        }
        let mut tampered = baseline.authority.clone();
        let coven_database::RetainedReplayAuthority::InstalledSnapshot(authority) = &mut tampered
        else {
            panic!("snapshot bootstrap installed a genesis replay baseline")
        };
        authority.metadata.corrupt_signature_for_test();
        authority
            .validate()
            .expect_err("retained snapshot authority must re-open its signed metadata");
        let authority_bytes = serde_json::to_vec(&tampered).expect("serialize tampered authority");
        installed
            .replace_generation_zero_replay_authority_for_test(authority_bytes)
            .await
            .expect("tamper retained snapshot metadata");
        installed
            .generation_zero_replay_baseline_for_test()
            .await
            .expect_err("restart must reject retained snapshot metadata with another signature");
    })
    .await;
}

/// Installing a snapshot does not depend on the store's other devices having
/// caught up to it. Whether they have is reclaim's question — deleting history
/// behind a snapshot needs everyone provably past it — and answering it here
/// pinned a store with one joined-and-idle device to its generation-zero image
/// forever, because a device that authors no commit can never acknowledge
/// anything.
///
/// What the restore still refuses is unchanged and checked elsewhere: metadata
/// that does not recompose from the verified history, an author who is not an
/// owner, and a cut the author's own stream does not contain.
#[tokio::test]
async fn bootstrap_installs_an_owner_snapshot_no_device_has_acknowledged() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let signer = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "snapshot-bootstrap-requires-stability",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create unstable bootstrap Store");
    let device = store
        .open_into(&source, source_store_dir.clone())
        .await
        .expect("open unstable bootstrap Store membership");
    let membership = device
        .membership_for_test()
        .await
        .expect("project unstable bootstrap Store membership");
    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image_path = image_dir.path().to_path_buf();
    let root = store.root().clone();
    let source_database = StoreDatabase::new(&source);
    let image = source_database
        .capture_snapshot_image_for_test(root, image_path, None)
        .await
        .expect("create unstable bootstrap database image");
    // Deliberately unacknowledged — this case is about stability. The coverage
    // is still the frontier the captured image holds.
    let coverage = captured_coverage(&source_database).await;
    device
        .publish_snapshot(image, coverage.clone())
        .await
        .expect("publish unacknowledged bootstrap database image");

    let destination = tempfile::tempdir().expect("bootstrap destination");
    let database_path = destination.path().join("store.db");
    let bootstrap = store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            1,
            &database_path,
            &signer,
        )
        .await
        .expect("an unacknowledged owner snapshot is still installable");

    assert_eq!(
        bootstrap.coverage_count(),
        coverage.position_count(),
        "the bootstrap installs the coverage its snapshot signed",
    );
}

#[tokio::test]
async fn snapshot_removes_the_closed_merge_materialization_graph() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "snapshot-merge-materialization-graph",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create snapshot materialization Store");
    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('snapshot-row', 'Snapshot', 1, \
                         '0000000001000-0000-snapshot', '2026-01-01')",
        ])
        .await;
    store
        .publish_changeset("snapshot", 1, &changeset, 1)
        .await
        .expect("publish snapshot materialization fixture");
    let live_counts = source
        .materialization_graph_counts_for_test()
        .await
        .expect("count live materialization graph");
    assert!(live_counts.0 > 0);
    assert!(live_counts.1 > 0);
    assert!(live_counts.2 > 0);

    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image_path = image_dir.path().to_path_buf();
    let root = store.root().clone();
    let image = StoreDatabase::new(&source)
        .capture_snapshot_image_for_test(root, image_path, None)
        .await
        .expect("create materialization snapshot");
    let snapshot =
        coven_database::DatabaseImageTest::from_bytes(&image).expect("open inspected snapshot");
    assert_eq!(
        snapshot
            .materialization_graph_counts()
            .expect("count snapshot materialization graph"),
        (0, 0, 0)
    );
    let foreign_key_violations = snapshot
        .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get::<_, i64>(0)
        })
        .expect("check snapshot materialization foreign keys");
    assert_eq!(foreign_key_violations, 0);
}

#[tokio::test]
async fn snapshot_keeps_the_authenticated_blob_graph_closed() {
    Box::pin(async {
        let declaration = coven_protocol::synced_schema::BlobDecl::new(
            "photos",
            coven_protocol::blob::Provenance::HostProvided,
            coven_protocol::blob::CacheFill::CacheEager,
        );
        let source_store_dir = crate::sync::test_helpers::test_store_dir();
        let source = crate::sync::test_helpers::open_test_db_with_blob(
            source_store_dir.clone(),
            declaration,
        );
        let signer = UserKeypair::generate();
        let home = crate::sync::test_helpers::test_cloud_home();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            source_store_dir.clone(),
            "snapshot-blob-ownership-graph",
            signer.clone(),
            home.clone(),
        )
        .await
        .expect("create exact blob Store");
        source
            .execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
             VALUES ('n1', 'Album', 1, '0000000001000-0000-owner', '2026-01-01')",
            )
            .await;
        source
            .execute_test_host_write(&format!(
                "INSERT INTO note_photos
                 (id, note_id, kind, size, hash, _updated_at, created_at)
                 VALUES ('photo1', 'n1', 'cover', 11, '{}',
                         '0000000001000-0000-owner', '2026-01-01')",
                coven_protocol::blob::content_hash(b"cover-bytes"),
            ))
            .await;
        let source_dir = source_store_dir.clone();
        coven_foundation::store_dir::StoreDir::store_local_blob(
            &source_dir,
            "photos",
            "photo1",
            b"cover-bytes",
        )
        .await
        .expect("stage source blob");
        let writer = coven_storage::CloudSyncConnection::new(
            home,
            coven_storage::CloudCipher::Encrypted(
                coven_keys::encryption::EncryptionService::from_key([42; 32]),
            ),
            coven_storage::BlobPathScheme::Hashed,
            "snapshot-blob-ownership-graph",
            signer.clone(),
        );
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&source),
            source_dir.clone(),
        )
        .run_sync_cycle(writer, signer)
        .await
        .expect("publish source blob");

        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root().clone();
        let image = StoreDatabase::new(&source)
            .capture_snapshot_image_for_test(root, image_path, None)
            .await
            .expect("create blob snapshot");
        let snapshot =
            coven_database::DatabaseImageTest::from_bytes(&image).expect("open inspected snapshot");
        let graph = snapshot
            .snapshot_blob_graph()
            .expect("read closed snapshot blob graph");
        assert_eq!(graph.0, "note_photos");
        assert_eq!(graph.1, "photo1");
        assert_eq!(graph.2, "id");
        assert_eq!(graph.3, "0000000001000-0000-owner");
        assert_eq!(graph.4.len(), 64);
        assert_eq!(graph.5.object_id().to_string().len(), 64);
        assert!(
            !serde_json::to_string(&graph.5)
                .expect("serialize snapshot remote blob")
                .contains(source_dir.storage_dir().to_string_lossy().as_ref()),
            "snapshot remote blob state must not carry its source StoreDir",
        );
        assert!(matches!(
            graph.5.payloads(),
            coven_protocol::remote_object::RemoteObjectPayloads::RowBlob { .. }
        ));
        for table in ["row_blob_locators", "blob_locators", "remote_objects"] {
            let count = snapshot
                .coven_table_row_count(coven_database::DatabaseTestTable::named(table))
                .expect("count snapshot blob ownership table");
            assert_eq!(count, 1, "snapshot carries one {table} row");
        }
        let foreign_key_violations: i64 = snapshot
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })
            .expect("check snapshot blob foreign keys");
        assert_eq!(foreign_key_violations, 0);
    })
    .await;
}

fn blob_graph_binding(
    row_id: &str,
    stamp: &str,
    bytes: &[u8],
) -> coven_protocol::audience_package::RowBlobLocatorBinding {
    let plaintext_hash = coven_protocol::store_commit::ObjectHash::digest(bytes);
    let uploader_bytes = b"blob graph test uploader registration";
    let uploader = coven_protocol::store_commit::StoreDeviceRegistrationRef {
        device_id: "aa".repeat(32).parse().expect("valid blob graph device id"),
        registration_hash: coven_protocol::store_commit::ObjectHash::digest(uploader_bytes),
        object: coven_protocol::objects::ExactObjectRef::new(
            coven_protocol::objects::ObjectSlot::logical(
                "store-v1/devices/blob-graph-test-uploader.json".to_string(),
            )
            .expect("valid blob graph uploader slot"),
            uploader_bytes.len() as u64,
            coven_protocol::store_commit::ObjectHash::digest(uploader_bytes),
        ),
    };
    let locator = coven_protocol::blob::locator::BlobLocator::browsable(
        "images",
        row_id,
        uploader,
        format!("photos/{row_id}.bin"),
        bytes.len() as u64,
        plaintext_hash,
    )
    .expect("valid blob graph locator");
    let slot = coven_protocol::objects::ObjectSlot::logical(locator.semantic_key())
        .expect("valid blob graph object slot");
    let object = coven_protocol::objects::ExactObjectRef::new(
        slot,
        bytes.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(bytes),
    );
    coven_protocol::audience_package::RowBlobLocatorBinding::new(
        "photos",
        row_id,
        stamp,
        "id",
        coven_protocol::blob::locator::StoredBlobRef::new(locator, object)
            .expect("valid blob graph stored blob"),
    )
    .expect("valid blob graph row binding")
}

/// The image installer's `ON CONFLICT ... DO NOTHING` on `row_blob_locators`
/// keeps whatever binding the image already carries. When that pre-existing
/// binding at the same row stamp points at different exact content, the
/// install must fail loudly instead of shipping an image whose row binding
/// contradicts the prepared blob.
#[test]
fn blob_graph_install_rejects_a_conflicting_existing_row_binding() {
    let dir = tempfile::tempdir().expect("blob graph conflict directory");
    let image_path = dir.path().join("image.db");
    let owner = coven_protocol::remote_object::SnapshotObjectOwner::Store {
        metadata_slot: coven_protocol::objects::ObjectSlot::logical(
            "store-v1/test/blob-graph-conflict/snapshot.json".to_string(),
        )
        .expect("valid standalone blob graph snapshot slot"),
    };
    let existing = blob_graph_binding(
        "photo-conflict",
        "0000000001000-0000-owner",
        b"existing blob bytes",
    );
    let existing_remote =
        coven_protocol::remote_object::RemoteObjectRecord::snapshot_activated_blob(
            existing.blob(),
            owner.clone(),
        )
        .expect("activate existing blob graph object");
    {
        let connection =
            coven_database::DatabaseImageTest::open(&image_path).expect("open blob graph image");
        connection
            .apply_coven_schema()
            .expect("apply blob graph schema");
        connection
            .install_snapshot_blob_binding(&existing, &existing_remote)
            .expect("install existing blob graph binding");
    }
    let image = std::fs::read(&image_path).expect("read blob graph image");

    // Same row, column, and stamp; different content, so a different
    // locator and object.
    let replacement = blob_graph_binding(
        "photo-conflict",
        "0000000001000-0000-owner",
        b"replacement blob bytes",
    );
    let replacement_remote =
        coven_protocol::remote_object::RemoteObjectRecord::snapshot_activated_blob(
            replacement.blob(),
            owner.clone(),
        )
        .expect("activate replacement blob graph object")
        .into_record();
    let prepared = coven_database::PreparedSnapshotBlob {
        bindings: vec![replacement],
        authority: coven_protocol::audience_package::PackageAudience::Store,
        remote: replacement_remote,
    };
    let store_dir = coven_foundation::store_dir::StoreDir::new_ephemeral(dir.path());
    let error =
        SnapshotDatabaseImage::replace(store_dir.as_ref().join("snapshot-closure.db"), &image)
            .and_then(|image| image.install_blob_graph(&owner, &[prepared], &BTreeSet::new()))
            .expect_err("a conflicting existing row binding must fail the image install");
    assert!(
        error
            .to_string()
            .contains("already bound to different exact content"),
        "{error}"
    );
}

/// The frontier a captured image covers. A snapshot published over real history
/// while declaring no coverage tells every device that installs it to resolve
/// that history again from the cloud, and no test built on it can see the
/// difference — so the fixtures declare what they captured.
async fn captured_coverage(database: &StoreDatabase) -> CommitFrontier {
    CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("read the captured materialized frontier"),
    )
    .expect("the materialized frontier is a commit frontier")
}

#[path = "image_tests/circle.rs"]
mod circle;
#[path = "image_tests/scoped.rs"]
mod scoped;
