use super::*;

fn circle_bootstrap_reference(
    source: &Database,
    rows: &[u8],
) -> coven_protocol::circle::CircleBootstrapRef {
    let image_hash = coven_protocol::store_commit::ObjectHash::digest(rows);
    coven_protocol::circle::CircleBootstrapRef {
        coverage: CommitFrontier(BTreeMap::new()),
        schema_version: source.schema_version(),
        sync_routing_hash: source.sync_routing_hash(),
        image: coven_protocol::store_commit::SnapshotImageRef {
            image_hash,
            object: coven_protocol::objects::ExactObjectRef::new(
                coven_protocol::objects::ObjectSlot::logical(
                    "circle-bootstrap-routing.changeset".to_string(),
                )
                .expect("construct Circle bootstrap routing slot"),
                rows.len() as u64,
                image_hash,
            ),
        },
        blobs: Vec::new(),
    }
}

/// The bootstrap rows staged on the capturing database's own schema, so a test
/// reads what the payload states through the ordinary image readers.
async fn staged_bootstrap_rows(
    source: &Database,
    rows: &[u8],
) -> coven_database::DatabaseImageTest {
    let staged = StoreDatabase::new(source)
        .circle_bootstrap_rows_image_for_test(rows.to_vec())
        .await
        .expect("stage the Circle bootstrap rows");
    coven_database::DatabaseImageTest::from_bytes(&staged).expect("open the staged bootstrap rows")
}

/// Stage a payload, edit the staged rows, and state the result as a payload
/// again — how a test builds a bootstrap no honest capture would produce.
async fn edit_bootstrap_rows(
    source: &Database,
    rows: Vec<u8>,
    edit: impl FnOnce(&coven_database::DatabaseImageTest),
) -> Vec<u8> {
    let staged = staged_bootstrap_rows(source, &rows).await;
    edit(&staged);
    let edited = staged.into_bytes().expect("serialize the edited rows");
    StoreDatabase::new(source)
        .circle_bootstrap_rows_from_image_for_test(edited)
        .await
        .expect("state the edited rows as a bootstrap payload")
}

#[tokio::test]
async fn circle_bootstrap_verification_requires_authenticated_routing() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = open_scoped_snapshot_test_db(source_store_dir.clone());
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "circle-bootstrap-routing-key",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Circle bootstrap routing Store");
    let circle_id = seed_scoped_snapshot_rows(&source, &store, &source_store_dir).await;
    let root = store.root().clone();
    let rows = StoreDatabase::new(&source)
        .capture_circle_snapshot_image_for_test(
            root,
            coven_keys::encryption::EncryptionService::from_key([42; 32]),
            circle_id,
        )
        .await
        .expect("create Circle bootstrap routing rows");
    let reference = circle_bootstrap_reference(&source, &rows);

    let error = StoreDatabase::new(&source)
        .verify_circle_bootstrap_image(rows, reference, circle_id, None)
        .await
        .expect_err("scoped Circle bootstrap verification must require its routing key");
    assert!(
        error
            .to_string()
            .contains("requires Store routing authentication"),
        "{error}"
    );
}

#[tokio::test]
async fn circle_bootstrap_verification_rejects_scoped_store_rows() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = open_scoped_snapshot_test_db(source_store_dir.clone());
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "circle-bootstrap-store-row",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Circle bootstrap Store-row Store");
    let circle_id = seed_scoped_snapshot_rows(&source, &store, &source_store_dir).await;
    let root = store.root().clone();
    let rows = StoreDatabase::new(&source)
        .capture_circle_snapshot_image_for_test(
            root,
            coven_keys::encryption::EncryptionService::from_key([42; 32]),
            circle_id,
        )
        .await
        .expect("create Circle projection for Store-row tampering");
    let routing_key = coven_protocol::circle::derive_row_routing_key(
        &coven_keys::encryption::EncryptionService::from_key([42; 32]),
        StoreDatabase::new(&source)
            .local_store_root_ref()
            .await
            .expect("read Store-row Store root")
            .expect("Store-row Store root is installed")
            .store_root_hash,
    )
    .expect("derive Store-row routing key");
    let store_row_id = "00000000-0000-4000-8000-000000000008";
    let store_row_stamp = "0000000001008-0000-owner";
    let store_routing_id =
        coven_protocol::circle::row_routing_id(&routing_key, "documents", store_row_id).to_string();
    let rows = edit_bootstrap_rows(&source, rows, |connection| {
        connection
            .execute(
                "INSERT INTO documents VALUES (?1, NULL, ?2, ?3)",
                (store_row_id, "Store row in Circle image", store_row_stamp),
            )
            .expect("insert scoped Store row into Circle bootstrap");
        connection
            .install_audience_mirror(&store_routing_id, None, store_row_stamp)
            .expect("insert scoped Store audience mirror into Circle bootstrap");
    })
    .await;
    let reference = circle_bootstrap_reference(&source, &rows);

    let error = StoreDatabase::new(&source)
        .verify_circle_bootstrap_image(rows, reference, circle_id, Some(routing_key))
        .await
        .expect_err("Circle bootstrap must reject a scoped Store row");
    assert!(
        error
            .to_string()
            .contains("outside its exact audience closure"),
        "{error}"
    );
}

#[tokio::test]
async fn circle_bootstrap_verification_rejects_unscoped_rows() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db_schema(
        source_store_dir.clone(),
        vec![
            SyncedTable::new(
                "documents",
                coven_protocol::synced_schema::RowIdentity::IndependentUuid,
            )
            .scoped_by("audience"),
            SyncedTable::new(
                "settings",
                coven_protocol::synced_schema::RowIdentity::IndependentUuid,
            ),
        ],
        vec![Migration::sql(
            1,
            "Circle bootstrap unscoped schema",
            "CREATE TABLE documents (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     body TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE settings (
                     id TEXT PRIMARY KEY,
                     value TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
        )],
    );
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "circle-bootstrap-unscoped-row",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Circle bootstrap unscoped-row Store");
    let circle_id = create_snapshot_circle(&source, &store, &source_store_dir).await;
    let root = store.root().clone();
    let rows = StoreDatabase::new(&source)
        .capture_circle_snapshot_image_for_test(
            root,
            coven_keys::encryption::EncryptionService::from_key([42; 32]),
            circle_id,
        )
        .await
        .expect("create Circle bootstrap unscoped rows");
    let rows = edit_bootstrap_rows(&source, rows, |connection| {
        connection
            .execute(
                "INSERT INTO settings VALUES (?1, ?2, ?3)",
                (
                    "00000000-0000-4000-8000-000000000009",
                    "not Circle-scoped",
                    "0000000001000-0000-owner",
                ),
            )
            .expect("insert unscoped row into Circle bootstrap");
    })
    .await;
    let reference = circle_bootstrap_reference(&source, &rows);
    let routing_key = coven_protocol::circle::derive_row_routing_key(
        &coven_keys::encryption::EncryptionService::from_key([42; 32]),
        StoreDatabase::new(&source)
            .local_store_root_ref()
            .await
            .expect("read unscoped-row Store root")
            .expect("unscoped-row Store root is installed")
            .store_root_hash,
    )
    .expect("derive unscoped-row routing key");

    let error = StoreDatabase::new(&source)
        .verify_circle_bootstrap_image(rows, reference, circle_id, Some(routing_key))
        .await
        .expect_err("Circle bootstrap must reject an unscoped synced row");
    assert!(
        error
            .to_string()
            .contains("outside its exact audience closure"),
        "{error}"
    );
}

#[tokio::test]
async fn circle_snapshot_states_only_its_rows_and_mirrors() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = open_scoped_snapshot_test_db(source_store_dir.clone());
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "snapshot-circle-projection",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Circle snapshot Store");
    let circle_id = seed_scoped_snapshot_rows(&source, &store, &source_store_dir).await;

    let root = store.root().clone();
    let rows = StoreDatabase::new(&source)
        .capture_circle_snapshot_image_for_test(
            root,
            coven_keys::encryption::EncryptionService::from_key([42; 32]),
            circle_id,
        )
        .await
        .expect("create Circle snapshot rows");

    let changes = coven_database::circle_bootstrap_changes_for_test(&rows)
        .expect("read the Circle bootstrap changes");
    assert!(
        changes
            .iter()
            .all(|(_, operation, _)| operation == "insert"),
        "a bootstrap states every row as an insert: {changes:?}"
    );
    assert_eq!(
        changes
            .iter()
            .map(|(table, _, _)| table.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "_coven_audience".to_string(),
            "documents".to_string(),
            "paragraphs".to_string(),
        ]),
        "a bootstrap names only the Circle's projection tables"
    );

    let inspected = staged_bootstrap_rows(&source, &rows).await;
    let materialized = inspected
        .query_row(
            "SELECT
                     (SELECT group_concat(body, ',') FROM documents),
                     (SELECT group_concat(body, ',') FROM paragraphs)",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .expect("inspect Circle snapshot rows");
    assert_eq!(
        materialized,
        (
            "Circle document".to_string(),
            "Circle paragraph".to_string()
        )
    );
    assert_eq!(
        inspected
            .coven_table_row_count(coven_database::DatabaseTestTable::named("_coven_audience"))
            .expect("count Circle snapshot audience mirrors"),
        2,
    );
}

#[tokio::test]
async fn circle_snapshot_keeps_only_referenced_store_parent_rows() {
    let tables = vec![
        SyncedTable::new(
            "folders",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        ),
        SyncedTable::new(
            "documents",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience"),
    ];
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db_schema(
        source_store_dir.clone(),
        tables.clone(),
        vec![Migration::sql(
            1,
            "Circle snapshot Store parent schema",
            "CREATE TABLE folders (
                     id TEXT PRIMARY KEY,
                     name TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE documents (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     folder_id TEXT NOT NULL REFERENCES folders(id),
                     body TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
        )],
    );
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "snapshot-circle-store-parent",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Circle parent snapshot Store");
    let database = StoreDatabase::new(&source);
    let circle_id = create_snapshot_circle(&source, &store, &source_store_dir).await;
    let write_circle_id = circle_id;
    database
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            move |transaction| {
                transaction.execute(
                    "INSERT INTO folders VALUES (?1, 'kept', ?2)",
                    (
                        "93c8343e-6a43-4d66-9aba-f275825047ac",
                        "0000000001000-0000-owner",
                    ),
                )?;
                transaction.execute(
                    "INSERT INTO folders VALUES (?1, 'omitted', ?2)",
                    (
                        "7d748d61-0a3b-4c79-9651-75be31988680",
                        "0000000001001-0000-owner",
                    ),
                )?;
                transaction.execute(
                    "INSERT INTO documents VALUES (?1, ?2, ?3, 'Circle document', ?4)",
                    (
                        "17052cff-e9ce-469a-8987-bf4e02c2ce0d",
                        write_circle_id.to_string(),
                        "93c8343e-6a43-4d66-9aba-f275825047ac",
                        "0000000001002-0000-owner",
                    ),
                )?;
                Ok(())
            },
        )
        .await
        .expect("commit Circle row with Store parent");

    let root = store.root().clone();
    let rows = StoreDatabase::new(&source)
        .capture_circle_snapshot_image_for_test(
            root,
            coven_keys::encryption::EncryptionService::from_key([42; 32]),
            circle_id,
        )
        .await
        .expect("create Circle snapshot with Store parent");
    let reference = circle_bootstrap_reference(&source, &rows);
    let routing_key = coven_protocol::circle::derive_row_routing_key(
        &coven_keys::encryption::EncryptionService::from_key([42; 32]),
        StoreDatabase::new(&source)
            .local_store_root_ref()
            .await
            .expect("read Circle parent Store root")
            .expect("Circle parent Store root is installed")
            .store_root_hash,
    )
    .expect("derive Circle parent routing key");
    let rows = StoreDatabase::new(&source)
        .verify_circle_bootstrap_image(rows, reference, circle_id, Some(routing_key))
        .await
        .expect("verify Circle bootstrap with its required Store parent");
    let inspected = staged_bootstrap_rows(&source, &rows).await;
    let installed = inspected
        .query_row(
            "SELECT
                     (SELECT group_concat(name, ',') FROM folders),
                     (SELECT group_concat(body, ',') FROM documents)",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .expect("inspect Circle parent snapshot rows");
    assert_eq!(
        installed,
        ("kept".to_string(), "Circle document".to_string())
    );
    assert_eq!(
        inspected
            .coven_table_row_count(coven_database::DatabaseTestTable::named("_coven_audience",))
            .expect("count Circle parent audiences"),
        1
    );
}
