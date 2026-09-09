use super::*;

#[derive(Clone, Copy)]
enum ScopedSnapshotImage {
    Valid,
    UnauthenticatedRoute,
    CircleRow,
    InvalidCircleMirror,
    OrphanStoreMirror,
}

struct PublishedScopedSnapshot {
    store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
    membership: coven_protocol::membership::MembershipChain,
    _store_dir_temp: tempfile::TempDir,
    store_dir: coven_foundation::store_dir::StoreDir,
}

impl PublishedScopedSnapshot {
    async fn publish(store_id: &str, image_kind: ScopedSnapshotImage) -> Self {
        let source_store_dir = crate::sync::test_helpers::test_store_dir();
        let source = open_scoped_snapshot_test_db(source_store_dir.clone());
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            source_store_dir.clone(),
            store_id,
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create published scoped snapshot Store");
        let device = store
            .open_into(&source, source_store_dir.clone())
            .await
            .expect("load published scoped snapshot membership");
        let membership = device
            .membership_for_test()
            .await
            .expect("project published scoped snapshot membership");
        seed_scoped_snapshot_rows(&source, &store, &source_store_dir).await;

        let image_dir = tempfile::tempdir().expect("published scoped snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root().clone();
        let image = StoreDatabase::new(&source)
            .capture_snapshot_image_for_test(
                root,
                image_path,
                Some(coven_keys::encryption::EncryptionService::from_key(
                    [42; 32],
                )),
            )
            .await
            .expect("create published scoped snapshot image");
        let image = match image_kind {
            ScopedSnapshotImage::Valid => image,
            ScopedSnapshotImage::UnauthenticatedRoute => {
                edit_snapshot_image(image_dir.path(), image, |connection| {
                    connection
                        .corrupt_document_route_id()
                        .expect("tamper private route id");
                })
            }
            ScopedSnapshotImage::CircleRow => {
                let route = source
                    .document_circle_route_for_test("2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7")
                    .await
                    .expect("load Circle row route");
                edit_snapshot_image(image_dir.path(), image, |connection| {
                    connection
                        .execute(
                            "INSERT INTO documents VALUES (?1, ?2, ?3, ?4)",
                            (
                                "2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7",
                                &route.0,
                                "Circle document",
                                &route.2,
                            ),
                        )
                        .expect("insert Circle row into Store snapshot");
                    connection
                        .install_row_route(
                            &route.1,
                            "documents",
                            "2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7",
                            &route.2,
                        )
                        .expect("insert Circle private route into Store snapshot");
                })
            }
            ScopedSnapshotImage::InvalidCircleMirror => {
                edit_snapshot_image(image_dir.path(), image, |connection| {
                    connection
                        .replace_first_circle_audience(Some("local"))
                        .expect("replace Circle mirror with Local audience");
                })
            }
            ScopedSnapshotImage::OrphanStoreMirror => {
                edit_snapshot_image(image_dir.path(), image, |connection| {
                    connection
                        .replace_first_circle_audience(None)
                        .expect("replace Circle mirror with orphan Store audience");
                })
            }
        };
        let coverage = captured_coverage(&StoreDatabase::new(&source)).await;
        device
            .publish_snapshot(image, coverage.clone())
            .await
            .expect("publish scoped snapshot");
        device
            .publish_acknowledgement(coverage)
            .await
            .expect("publish scoped snapshot acknowledgement");

        let (store_dir_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        Self {
            store,
            membership,
            _store_dir_temp: store_dir_temp,
            store_dir,
        }
    }

    async fn open<'storage>(
        &'storage self,
        database_path: &Path,
    ) -> Result<crate::sync::store::RestoringStore<'storage>, SnapshotError> {
        let restorer_identity = coven_keys::keys::UserKeypair::generate();
        let bootstrap = self
            .store
            .prepare_snapshot_bootstrap(
                &coven_protocol::membership::MembershipFloor(self.membership.head_refs().to_vec()),
                1,
                database_path,
                &restorer_identity,
            )
            .await?;
        let routing = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        bootstrap
            .install(
                &self.store_dir,
                scoped_snapshot_tables(),
                coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
                coven_protocol::blob::TransferLimits::one_at_a_time(),
                "joining-device".to_string(),
                std::sync::Arc::new(coven_foundation::clock::SystemClock),
                &crate::sync::test_helpers::test_migrations(),
                coven_database::CovenMigrationPolicy::ApplyPending,
                Some(&routing),
            )
            .await
    }
}

#[tokio::test]
async fn snapshot_preserves_authenticated_routes_for_every_scoped_row() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = open_scoped_snapshot_test_db(source_store_dir.clone());
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "snapshot-authenticated-routes",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create scoped snapshot Store");
    seed_scoped_snapshot_rows(&source, &store, &source_store_dir).await;

    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image_path = image_dir.path().to_path_buf();
    let root = store.root().clone();
    let image = StoreDatabase::new(&source)
        .capture_snapshot_image_for_test(
            root,
            image_path,
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
        )
        .await
        .expect("create scoped snapshot image");
    let inspected = coven_database::DatabaseImageTest::from_bytes(&image)
        .expect("open inspected scoped snapshot");
    let routes = inspected
        .coven_table_row_count(coven_database::DatabaseTestTable::named(
            "_coven_row_routes",
        ))
        .expect("count snapshot private routes");
    let mirrors = inspected
        .coven_table_row_count(coven_database::DatabaseTestTable::named("_coven_audience"))
        .expect("count snapshot audience mirrors");
    let materialized: (i64, i64) = inspected
        .query_row(
            "SELECT
                     (SELECT COUNT(*) FROM documents),
                     (SELECT COUNT(*) FROM paragraphs)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("count scoped snapshot rows");
    assert_eq!(materialized, (1, 1));
    assert_eq!((routes, mirrors), (2, 4), "Store root {:?}", store.root());
}

#[tokio::test]
async fn snapshot_refuses_an_unauthenticated_live_private_route() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = open_scoped_snapshot_test_db(source_store_dir.clone());
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "snapshot-invalid-live-route",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create invalid-live-route Store");
    seed_scoped_snapshot_rows(&source, &store, &source_store_dir).await;
    source
        .corrupt_live_document_route_id_for_test("01890a5d-ac96-774b-bcce-b302099c3f74")
        .await
        .expect("corrupt live private route");

    let image_dir = tempfile::tempdir().expect("invalid-live-route snapshot directory");
    let image_path = image_dir.path().to_path_buf();
    let root = store.root().clone();
    let result = StoreDatabase::new(&source)
        .capture_snapshot_image_for_test(
            root,
            image_path,
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
        )
        .await;
    let error = match result {
        Ok(_) => panic!("unauthenticated live private route must block snapshot creation"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("private route id does not authenticate"),
        "{error}"
    );
    assert_eq!(
        std::fs::read_dir(image_dir.path())
            .expect("read invalid-live-route snapshot directory")
            .count(),
        0,
        "route validation fails before creating snapshot files"
    );
}

#[tokio::test]
async fn bootstrap_installs_a_valid_scoped_snapshot_with_authenticated_routes() {
    let store_id = "snapshot-valid-private-routes";
    let fixture = PublishedScopedSnapshot::publish(store_id, ScopedSnapshotImage::Valid).await;
    let destination = tempfile::tempdir().expect("valid-route bootstrap destination");
    let database_path = destination.path().join("store.db");
    let database = fixture
        .open(&database_path)
        .await
        .expect("open valid scoped snapshot");
    let counts = database
        .scoped_snapshot_counts_for_test()
        .await
        .expect("inspect valid scoped bootstrap");
    assert_eq!(counts, (1, 1, 2));
}

#[tokio::test]
async fn bootstrap_migrates_before_validating_scoped_snapshot_routes() {
    const DOCUMENT_SCHEMA: &str = "CREATE TABLE documents (
             id TEXT PRIMARY KEY,
             audience TEXT,
             body TEXT NOT NULL,
             _updated_at TEXT NOT NULL
         ) STRICT;";
    let source_tables = vec![SyncedTable::new(
        "documents",
        coven_protocol::synced_schema::RowIdentity::IndependentUuid,
    )
    .scoped_by("audience")];
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db_schema(
        source_store_dir.clone(),
        source_tables.clone(),
        vec![Migration::sql(1, "document schema", DOCUMENT_SCHEMA)],
    );
    let signer = UserKeypair::generate();
    let store_id = "snapshot-scoped-migration";
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        store_id,
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create scoped migration Store");
    let device = store
        .open_into(&source, source_store_dir.clone())
        .await
        .expect("load scoped migration membership");
    let membership = device
        .membership_for_test()
        .await
        .expect("project scoped migration membership");
    StoreDatabase::new(&source)
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            move |transaction| {
                transaction
                    .execute(
                        "INSERT INTO documents VALUES (?1, NULL, ?2, ?3)",
                        (
                            "6b432d70-7440-4ba8-b824-f17d6733f252",
                            "Migrated document",
                            "0000000002000-0000-owner",
                        ),
                    )
                    .map(|_| ())
                    .map_err(coven_database::DbError::from)
            },
        )
        .await
        .expect("commit pre-migration scoped row");

    let image_dir = tempfile::tempdir().expect("scoped migration snapshot directory");
    let image_path = image_dir.path().to_path_buf();
    let root = store.root().clone();
    let image = StoreDatabase::new(&source)
        .capture_snapshot_image_for_test(
            root,
            image_path,
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
        )
        .await
        .expect("create pre-migration scoped snapshot");
    let coverage = CommitFrontier(BTreeMap::new());
    device
        .publish_snapshot(image, coverage.clone())
        .await
        .expect("publish pre-migration scoped snapshot");
    device
        .publish_acknowledgement(coverage)
        .await
        .expect("publish pre-migration snapshot acknowledgement");

    let target_tables = source_tables;
    let target_migrations = vec![
        Migration::sql(1, "document schema", DOCUMENT_SCHEMA),
        Migration::sql(
            2,
            "ordinary document column",
            "ALTER TABLE documents
                     ADD COLUMN ordinary TEXT NOT NULL DEFAULT 'ordinary';
                 CREATE INDEX documents_ordinary ON documents(ordinary);",
        ),
    ];
    let destination = tempfile::tempdir().expect("scoped migration bootstrap destination");
    let database_path = destination.path().join("store.db");
    let bootstrap = store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            2,
            &database_path,
            &signer,
        )
        .await
        .expect("verify pre-migration scoped snapshot");
    let routing = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let store_dir = coven_foundation::store_dir::StoreDir::new_ephemeral(destination.path());
    let database = bootstrap
        .install(
            &store_dir,
            target_tables,
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "joining-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &target_migrations,
            coven_database::CovenMigrationPolicy::ApplyPending,
            Some(&routing),
        )
        .await
        .expect("migrate and validate scoped snapshot");
    assert_eq!(database.schema_version_for_test(), 2);
    let migrated = database
        .migrated_scoped_snapshot_facts_for_test()
        .await
        .expect("inspect migrated scoped snapshot");
    assert_eq!(migrated, (1, 1, "ordinary".to_string()));
}

#[tokio::test]
async fn bootstrap_rejects_a_signed_snapshot_with_an_unauthenticated_private_route() {
    let store_id = "snapshot-invalid-private-route";
    let fixture =
        PublishedScopedSnapshot::publish(store_id, ScopedSnapshotImage::UnauthenticatedRoute).await;
    let destination = tempfile::tempdir().expect("route-tamper bootstrap destination");
    let database_path = destination.path().join("store.db");
    let result = fixture.open(&database_path).await;
    let error = match result {
        Ok(_) => panic!("unauthenticated private route must block bootstrap"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("private route id does not authenticate"),
        "{error}"
    );
    assert!(
        !database_path.exists(),
        "failed bootstrap removes the unauthenticated database image"
    );
}

#[tokio::test]
async fn bootstrap_rejects_a_store_snapshot_containing_a_circle_row() {
    let store_id = "snapshot-store-image-circle-row";
    let fixture = PublishedScopedSnapshot::publish(store_id, ScopedSnapshotImage::CircleRow).await;
    let destination = tempfile::tempdir().expect("Circle-row bootstrap destination");
    let database_path = destination.path().join("store.db");
    let result = fixture.open(&database_path).await;
    let error = match result {
        Ok(_) => panic!("Store snapshot containing a Circle row must block bootstrap"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("Store snapshot contains Circle row"),
        "{error}"
    );
    assert!(
        !database_path.exists(),
        "failed bootstrap removes the Circle-bearing Store image"
    );
}

#[tokio::test]
async fn bootstrap_rejects_an_invalid_opaque_circle_mirror() {
    let store_id = "snapshot-invalid-opaque-circle-mirror";
    let fixture =
        PublishedScopedSnapshot::publish(store_id, ScopedSnapshotImage::InvalidCircleMirror).await;
    let destination = tempfile::tempdir().expect("invalid-mirror bootstrap destination");
    let database_path = destination.path().join("store.db");
    let result = fixture.open(&database_path).await;
    let error = match result {
        Ok(_) => panic!("invalid opaque Circle mirror must block bootstrap"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("Store audience mirror has invalid audience"),
        "{error}"
    );
    assert!(
        !database_path.exists(),
        "failed bootstrap removes the invalid-mirror Store image"
    );
}

#[tokio::test]
async fn bootstrap_rejects_an_orphan_store_mirror() {
    let store_id = "snapshot-orphan-store-mirror";
    let fixture =
        PublishedScopedSnapshot::publish(store_id, ScopedSnapshotImage::OrphanStoreMirror).await;
    let destination = tempfile::tempdir().expect("orphan-mirror bootstrap destination");
    let database_path = destination.path().join("store.db");
    let result = fixture.open(&database_path).await;
    let error = match result {
        Ok(_) => panic!("orphan Store mirror must block bootstrap"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("Store audience mirror has no materialized row"),
        "{error}"
    );
    assert!(
        !database_path.exists(),
        "failed bootstrap removes the orphan-mirror Store image"
    );
}
