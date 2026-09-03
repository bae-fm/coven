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

/// A one-device owner Store over the audience-scoped blob schema. One device is
/// the whole point: the snapshot every reclaim run needs acknowledged is
/// acknowledged by this device alone, so the run is deterministic without a
/// second device's acknowledgement chain in the way.
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

    /// The position in the provider's delete log at which `key`'s logical slot
    /// was first deleted. Opaque exact slots record as
    /// `<logical_key>#exact#<provider_id>`, so the logical part is compared.
    fn first_delete_of(&self, key: &str) -> Option<usize> {
        self.home
            .deletes_seen()
            .iter()
            .position(|deleted| deleted.split("#exact#").next() == Some(key))
    }

    /// Publish everything staged: the host write becomes a commit with its
    /// package, and the blob it binds is uploaded.
    async fn run_cycle(&self) {
        self.components
            .run_cycle(&coven_foundation::clock::SystemClock, None)
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

    /// The one stored blob this Store published, and the Store package of the
    /// commit that published it — the package a reclaim of that blob re-reads.
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
    /// commit that published it with an acknowledged snapshot. The blob and the
    /// package that bound it are both in reach of the next reclaim run.
    async fn strand_a_published_blob(
        &self,
    ) -> (
        coven_protocol::blob::locator::StoredBlobRef,
        coven_protocol::reclaim::StorePackageReclaimTarget,
    ) {
        // Snapshot the empty store first. A published image is read by devices
        // that restore from it, so every blob one lists is pinned against
        // reclaim for good — and the cadence publishes an image on the first
        // cycle whatever is in it. Spending that first image on an empty store
        // keeps the blob below out of every image, which is what leaves it
        // reclaimable once its rows let go of it.
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

        // Strand the ciphertext first, then snapshot: the image lists no blob a
        // live row does not bind, while the coverage takes in the commit whose
        // package bound it. Both are then reclaim targets of one run.
        self.make_document_local(document, "2026-07-23T00:20:00Z")
            .await;
        self.run_cycle().await;
        self.device
            .publish_snapshot_generation_for_test()
            .await
            .expect("publish and acknowledge a covering snapshot");
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

/// A blob reclaim proves its claim by re-reading the package that published the
/// blob, so that package has to outlive the blob operation. With the blob and
/// its package both in reach of one run, the blob is deleted first and the
/// package after it — never the other way round, which would strand the blob
/// operation at a read that can no longer succeed.
#[tokio::test]
async fn a_package_a_pending_blob_reclaim_names_is_deleted_only_after_the_blob() {
    let fixture = AudienceBlobPackageFixture::build("blob-reclaim-package-order").await;
    let (source, binding) = fixture.strand_a_published_blob().await;

    let run = fixture
        .device
        .reclaim_packages()
        .await
        .expect("one run reclaims the blob and then its package");
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
        "the package is deleted once its blob is gone: {:?}",
        run.store_packages
    );
    let blob_deleted = fixture
        .first_delete_of(source.object().slot().logical_key())
        .expect("the provider saw the blob's delete");
    let package_deleted = fixture
        .first_delete_of(binding.package.object.slot().logical_key())
        .expect("the provider saw the package's delete");
    assert!(
        blob_deleted < package_deleted,
        "the blob's delete ({blob_deleted}) precedes its package's ({package_deleted})"
    );

    // A further run is idempotent: nothing is re-authorized or re-deleted.
    let again = fixture
        .device
        .reclaim_packages()
        .await
        .expect("a further run finds nothing left to reclaim");
    assert_eq!(again.packages_deleted, 0, "{:?}", again.store_packages);
}

/// A blob reclaim the provider refuses for good is stuck, not finished: the
/// package that published the blob stays retained behind it.
///
/// Executing the blob operation re-reads that package to prove the binding, so
/// a package reclaim that deleted it first would strand the blob operation at a
/// read that can never succeed. Being stuck means waiting on a person, which is
/// exactly the state in which the package must not go.
#[tokio::test]
async fn a_stuck_blob_reclaim_keeps_its_package_retained() {
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
    assert_eq!(
        run.store_packages.retained_for_blob_reclaim, 1,
        "the package is held for the stuck blob operation: {:?}",
        run.store_packages
    );
    assert!(
        fixture.package_is_present(&binding).await,
        "so the package that bound the blob is not deleted"
    );

    let again = fixture
        .device
        .reclaim_packages()
        .await
        .expect("a later run finds the operation still stuck");
    assert_eq!((again.packages_deleted, again.stuck), (0, 1));
    assert!(
        fixture.package_is_present(&binding).await,
        "and it stays retained for as long as the blob operation waits"
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
