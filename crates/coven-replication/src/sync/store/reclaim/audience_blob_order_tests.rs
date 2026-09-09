use coven_keys::keys::UserKeypair;
use coven_storage::CloudSyncObjectStorage;

/// `documents` is scoped by its audience column; `document_files` inherits its
/// document's audience and carries the blob. A document that leaves the Store
/// audience therefore strands the ciphertext the Store package published for
/// it, which is the shape the audience-blob reclaim exists for.
fn scoped_blob_tables() -> Vec<coven_protocol::synced_schema::SyncedTable> {
    vec![
        coven_protocol::synced_schema::SyncedTable::new(
            "documents",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience"),
        coven_protocol::synced_schema::SyncedTable::new(
            "document_files",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        )
        .inherits_audience_through("document_id")
        .carries_blob(coven_protocol::synced_schema::BlobDecl::new(
            "files",
            coven_protocol::blob::Provenance::HostProvided,
            coven_protocol::blob::CacheFill::CacheEager,
        )),
    ]
}

fn scoped_blob_migrations() -> Vec<coven_database::Migration> {
    vec![coven_database::Migration::sql(
        1,
        "Audience-scoped documents carrying files",
        "CREATE TABLE documents (
             id TEXT PRIMARY KEY,
             audience TEXT,
             _updated_at TEXT NOT NULL
         ) STRICT;
         CREATE TABLE document_files (
             id TEXT PRIMARY KEY,
             document_id TEXT NOT NULL REFERENCES documents(id),
             size INTEGER NOT NULL,
             hash TEXT NOT NULL,
             _updated_at TEXT NOT NULL
         ) STRICT;",
    )]
}

/// A one-device owner Store over the audience-scoped blob schema.
/// The initialized production sync components the owner drives: the cycle is
/// what carries the row-routing key a scoped write and its blob upload need.
async fn prepare_owner_sync_components(
    db: &coven_database::Database,
    store: &crate::sync::test_helpers::TestStore,
    home: &std::sync::Arc<coven_storage::InMemoryCloudHome>,
    store_dir: &coven_foundation::store_dir::StoreDir,
    signer: &UserKeypair,
    store_id: &str,
) -> crate::sync::cycle::SyncComponents {
    let custody = std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default());
    custody.set_initial_key([42; 32]);
    crate::sync::cycle::PreparedSyncComponents::prepare(
        coven_database::StoreDatabase::new(db),
        store_dir.clone(),
        coven_storage::CloudSyncConnection::new(
            home.clone(),
            coven_storage::CloudCipher::Encrypted(
                coven_keys::encryption::EncryptionService::from_key([42; 32]),
            ),
            coven_storage::BlobPathScheme::Hashed,
            store_id,
            signer.clone(),
        ),
        signer.clone(),
        crate::sync::cycle::StoreInitialization::OpenStore {
            expected_store_root: store.root().clone(),
        },
        Some(coven_keys::encryption::EncryptionService::from_key(
            [42; 32],
        )),
        custody,
    )
    .await
    .expect("prepare the owner sync components")
    .initialize(None)
    .await
    .expect("initialize the owner sync components")
}

struct AudienceBlobPackageFixture {
    db: coven_database::Database,
    store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
    storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
    home: std::sync::Arc<coven_storage::InMemoryCloudHome>,
    device: crate::sync::test_helpers::TestDevice,
    store_dir: coven_foundation::store_dir::StoreDir,
    /// The production cycle, which is what carries the row-routing key a scoped
    /// write and its blob upload need.
    components: crate::sync::cycle::SyncComponents,
}

impl AudienceBlobPackageFixture {
    async fn build(store_id: &str) -> Self {
        let store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db_schema(
            store_dir.clone(),
            scoped_blob_tables(),
            scoped_blob_migrations(),
        );
        let signer = UserKeypair::generate();
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, storage) = crate::sync::test_helpers::TestStore::create_with_connection(
            &db,
            store_dir.clone(),
            store_id,
            signer.clone(),
            home.clone(),
        )
        .await
        .expect("create Store");
        let device = store
            .bind_device_in(&db, store_dir.clone(), &signer)
            .await
            .expect("bind the owner device");
        let components =
            prepare_owner_sync_components(&db, &store, &home, &store_dir, &signer, store_id).await;
        Self {
            db,
            store,
            storage,
            home,
            device,
            store_dir,
            components,
        }
    }

    /// Publish everything staged: the host write becomes a commit with its
    /// package, and the blob it binds is uploaded.
    async fn run_cycle(&self) {
        self.components
            .run_cycle(
                &coven_foundation::clock::SystemClock,
                None,
                coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
            )
            .await
            .expect("run the owner sync cycle");
    }

    async fn capture_document_with_file(
        &self,
        document_id: &str,
        file_id: &str,
        bytes: &[u8],
        stamp: &str,
    ) {
        self.db
            .capture_document_with_file_for_test(document_id, file_id, None, bytes, stamp)
            .await
            .expect("capture the document and its file row");
        coven_foundation::store_dir::StoreDir::store_local_blob(
            &self.store_dir,
            "files",
            file_id,
            bytes,
        )
        .await
        .expect("stage the document file bytes");
    }

    /// Take the document out of every cloud audience. Its file row follows, so
    /// no live row binds the ciphertext the Store package published any more,
    /// and nothing is republished in its place.
    async fn make_document_local(&self, document_id: &str, stamp: &str) {
        let document_id = document_id.to_string();
        let stamp = stamp.to_string();
        let staging = self
            .components
            .host_write_blob_staging(tokio::runtime::Handle::current());
        coven_database::StoreDatabase::new(&self.db)
            .run_host_store_write_for_test(
                Some(coven_keys::encryption::EncryptionService::from_key(
                    [42; 32],
                )),
                Some(Box::new(staging) as Box<dyn coven_database::AudienceBlobMoveStaging>),
                move |transaction| {
                    transaction
                        .execute(
                            "UPDATE documents SET audience = 'local', _updated_at = ?2 \
                             WHERE id = ?1",
                            rusqlite::params![document_id, stamp],
                        )
                        .map(|_| ())
                        .map_err(coven_database::DbError::from)
                },
            )
            .await
            .expect("move the document out of the Store audience");
    }

    /// The exact stored blob and its original publishing package.
    async fn published_blob_and_its_package(
        &self,
    ) -> (
        coven_protocol::blob::locator::StoredBlobRef,
        coven_protocol::reclaim::StorePackageReclaimTarget,
    ) {
        let candidates = coven_database::StoreDatabase::new(&self.db)
            .stored_blob_reclaim_candidates_for_test()
            .await
            .expect("read stored blob candidates");
        let [(stored, owners)] = candidates.as_slice() else {
            panic!("the Store document published exactly one blob: {candidates:?}");
        };
        let [activation] = owners.as_slice() else {
            panic!("the blob was published by exactly one commit: {owners:?}");
        };
        let commit = self
            .device
            .load_commit_for_test(activation)
            .await
            .expect("load the commit that published the blob");
        let package = commit
            .value()
            .store_package()
            .expect("the publishing commit carries a Store package")
            .clone();
        (
            stored.clone(),
            coven_protocol::reclaim::StorePackageReclaimTarget {
                package,
                activation: activation.clone(),
            },
        )
    }

    /// Publish a document whose file blob is then stranded, and cover the
    /// commit that published it with an accepted snapshot. The blob and the
    /// package that bound it are both in reach of the next reclaim run.
    async fn strand_a_published_blob(
        &self,
    ) -> (
        coven_protocol::blob::locator::StoredBlobRef,
        coven_protocol::reclaim::StorePackageReclaimTarget,
    ) {
        // Publish the initial empty image before creating the payload, so the
        // explicit successor below owns the first snapshot cut containing it.
        self.run_cycle().await;

        let document = "00000000-0000-4000-8000-0000000000e6";
        self.capture_document_with_file(
            document,
            "00000000-0000-4000-8000-0000000000f6",
            b"store attachment whose package is snapshot-covered",
            "2026-07-23T00:10:00Z",
        )
        .await;
        self.run_cycle().await;
        let (source, binding) = self.published_blob_and_its_package().await;
        assert!(
            self.store
                .contains_stored_blob_object(&source)
                .await
                .expect("read the exact stored blob"),
            "the published ciphertext is uploaded"
        );
        assert!(
            self.package_is_present(&binding).await,
            "the package that bound the blob is at the provider"
        );

        // The successor image records the orphan's exact original ownership,
        // while its live payload graph excludes the released row.
        self.make_document_local(document, "2026-07-23T00:20:00Z")
            .await;
        self.run_cycle().await;
        self.device
            .publish_snapshot_generation_for_test()
            .await
            .expect("publish an accepted covering snapshot");
        self.db
            .release_retained_replay_ownership_for_test()
            .await
            .expect("release retained replay ownership");
        (source, binding)
    }

    async fn package_is_present(
        &self,
        target: &coven_protocol::reclaim::StorePackageReclaimTarget,
    ) -> bool {
        let prefix = coven_protocol::store_commit::package_semantic_prefix(
            target.package.candidate_family,
            &target.activation.coord.stream_id.to_string(),
            target.activation.coord.sequence(),
            target.package.content_hash,
        );
        let context = coven_protocol::objects::ProtocolObjectContext::store_encrypted(
            self.store.root().store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StorePackage,
        );
        match self
            .storage
            .read_protocol_object(&context, &target.package.object, &prefix)
            .await
        {
            Ok(_) => true,
            Err(coven_protocol::objects::StorageError::NotFound(_)) => false,
            Err(error) => panic!("read the binding package object: {error}"),
        }
    }
}

/// The accepted snapshot carries the blob's binding after the source package
/// is retired; both released objects can finish in the same reclaim run.
#[tokio::test]
async fn an_accepted_snapshot_reclaims_its_orphan_blob_and_original_package() {
    let fixture = AudienceBlobPackageFixture::build("blob-reclaim-package-order").await;
    let (source, binding) = fixture.strand_a_published_blob().await;

    let run = fixture
        .device
        .reclaim_packages()
        .await
        .expect("one run reclaims both released objects");
    assert!(
        run.store_packages.targets_considered >= 1,
        "the covering snapshot put the binding package in reach: {:?}",
        run.store_packages
    );
    assert!(
        !fixture
            .store
            .contains_stored_blob_object(&source)
            .await
            .expect("read the exact stored blob"),
        "the stranded ciphertext is deleted: {:?}",
        run.store_packages
    );
    assert!(
        !fixture.package_is_present(&binding).await,
        "the released package is deleted: {:?}",
        run.store_packages
    );
    // A further run is idempotent: nothing is re-authorized or re-deleted.
    let again = fixture
        .device
        .reclaim_packages()
        .await
        .expect("a further run finds nothing left to reclaim");
    assert_eq!(again.packages_deleted, 0, "{:?}", again.store_packages);
}

/// A failed Store blob delete stays blocked while its original package retires;
/// the accepted snapshot preserves the exact evidence needed for retry.
#[tokio::test]
async fn a_stuck_store_blob_reclaim_does_not_retain_its_original_package() {
    let fixture = AudienceBlobPackageFixture::build("blob-reclaim-stuck-retention").await;
    let (source, binding) = fixture.strand_a_published_blob().await;
    fixture
        .home
        .fail_nth_exact_delete_of_permanently(&[source.object().slot()], 1);

    let run = fixture
        .device
        .reclaim_packages()
        .await
        .expect("a refused blob delete does not fail the run");
    assert_eq!(run.stuck, 1, "the refused blob operation is stuck");
    assert!(
        fixture
            .store
            .contains_stored_blob_object(&source)
            .await
            .expect("read the exact stored blob"),
        "the refused ciphertext is still at the provider"
    );
    assert!(
        !fixture.package_is_present(&binding).await,
        "the original package retires while its blob remains blocked"
    );

    let again = fixture
        .device
        .reclaim_packages()
        .await
        .expect("a later run finds the operation still stuck");
    assert_eq!((again.packages_deleted, again.stuck), (0, 1));
    assert!(
        !fixture.package_is_present(&binding).await,
        "blocked reporting remains available after source package retirement"
    );
}

/// A cycle that reached storage and left a reclaim operation stuck reports
/// `Blocked` with that operation — never `Synchronized`. A stuck operation is
/// waiting on a person, so the host has to be told every cycle, not once.
#[tokio::test]
async fn a_cycle_that_leaves_a_reclaim_stuck_reports_it_as_blocked() {
    let fixture = AudienceBlobPackageFixture::build("blob-reclaim-stuck-status").await;
    let (source, _) = fixture.strand_a_published_blob().await;
    fixture
        .home
        .fail_nth_exact_delete_of_permanently(&[source.object().slot()], 1);

    fixture.run_cycle().await;

    let blocked = fixture
        .components
        .blocked_operations()
        .await
        .expect("read the blocked operations after the cycle");
    let [crate::sync::sync_loop::BlockedOperation::Reclaim(stuck)] = blocked.as_slice() else {
        panic!("the cycle leaves exactly one blocked operation, the stuck reclaim: {blocked:?}");
    };
    let operation = stuck.clone();
    assert_eq!(
        operation.target.object(),
        source.object(),
        "the reported operation names the blob the provider refused to delete"
    );
    assert!(
        operation.error.contains("refused to delete"),
        "and carries the refusal the host shows: {}",
        operation.error
    );

    let status = crate::sync::sync_loop::current_success_status(blocked, success());
    assert!(
        matches!(
            &status,
            crate::sync::SyncLoopStatus::Blocked { operations, .. }
                if operations.len() == 1
                    && operations[0].id()
                        == crate::sync::BlockedOperationId::Reclaim(operation.operation_id)
        ),
        "a successful cycle with a stuck operation is Blocked, not Synchronized: {status:?}"
    );
}

/// A cycle outcome with nothing else on it, so the status assertion above turns
/// only on the blocked operations.
fn success() -> crate::sync::loop_policy::SyncLoopSuccess {
    crate::sync::loop_policy::SyncLoopSuccess {
        last_sync_time: "2026-07-23T00:30:00Z".to_string(),
        device_count: 1,
        device_activity: Vec::new(),
        data_changed: false,
        row_changes: None,
        alerts: crate::sync::SyncLoopAlerts {
            rotation_pending: None,
            held_positions: Vec::new(),
            local_blob_cleanup_pending: false,
        },
    }
}

#[tokio::test]
async fn a_snapshot_preserves_its_blob_after_package_reclaim_until_a_successor_releases_it() {
    let fixture = AudienceBlobPackageFixture::build("snapshot-blob-retirement").await;
    fixture.run_cycle().await;
    let document = "00000000-0000-4000-8000-0000000000e7";
    fixture
        .capture_document_with_file(
            document,
            "00000000-0000-4000-8000-0000000000f7",
            b"snapshot payload outlives its package",
            "2026-07-23T00:10:00Z",
        )
        .await;
    fixture.run_cycle().await;
    let (blob, package) = fixture.published_blob_and_its_package().await;
    super::tests::publish_current_snapshot(&fixture.device).await;
    fixture
        .device
        .stand_on_accepted_snapshot()
        .await
        .expect("retire covered replay inputs");
    fixture
        .device
        .reclaim_packages()
        .await
        .expect("reclaim covered source package");
    assert!(
        !fixture.package_is_present(&package).await,
        "the snapshot replaces its source package"
    );
    assert!(fixture
        .store
        .contains_stored_blob_object(&blob)
        .await
        .expect("read snapshot payload"));

    fixture
        .make_document_local(document, "2026-07-23T00:20:00Z")
        .await;
    fixture
        .device
        .publish_pending_store_database()
        .await
        .expect("publish the row's Store deletion");
    fixture
        .device
        .reclaim_packages()
        .await
        .expect("evaluate a blob whose source package was retired");
    assert!(
        fixture
            .store
            .contains_stored_blob_object(&blob)
            .await
            .expect("read retained snapshot payload"),
        "the accepted snapshot still needs its blob after the live row leaves Store"
    );

    super::tests::publish_current_snapshot(&fixture.device).await;
    fixture
        .device
        .stand_on_accepted_snapshot()
        .await
        .expect("adopt the snapshot that excludes the blob");
    let snapshot = coven_database::StoreDatabase::new(&fixture.db)
        .latest_local_store_snapshot()
        .await
        .expect("read accepted orphan snapshot")
        .expect("accepted snapshot exists");
    let bytes = fixture
        .storage
        .read_protocol_object(
            &coven_protocol::objects::ProtocolObjectContext::store_encrypted(
                fixture.store.root().store_root_hash,
                coven_protocol::objects::ProtocolObjectDomain::StoreSnapshotImage,
            ),
            &snapshot.meta.image.object,
            &coven_protocol::store_commit::snapshot_image_semantic_prefix(
                snapshot.reference.object.slot(),
                snapshot.meta.image.image_hash,
            ),
        )
        .await
        .expect("read accepted exact orphan inventory");
    assert!(
        coven_database::SnapshotDatabaseImage::contains_reclaimable_store_blob(
            &bytes,
            &snapshot.meta,
            &blob,
        )
        .expect("accepted inventory proves the exact blob")
    );
    let mut omitted_owner = snapshot.meta.clone();
    assert!(omitted_owner
        .body_mut()
        .history_summary
        .causal_cut
        .remove(&package.activation.coord)
        .is_some());
    let error = coven_database::SnapshotDatabaseImage::contains_reclaimable_store_blob(
        &bytes,
        &omitted_owner,
        &blob,
    )
    .expect_err("an inventory record cannot invent its original publication");
    assert!(
        error
            .to_string()
            .contains("exact accepted publication owner"),
        "{error}"
    );
    fixture.device.reclaim_packages().await.expect(
        "reclaim the superseded snapshot payload without rereading its deleted source package",
    );
    assert!(!fixture
        .store
        .contains_stored_blob_object(&blob)
        .await
        .expect("read released blob"));
}
