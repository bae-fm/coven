use crate::*;
use std::sync::Arc;

fn builder(app: &TestCoven, layout: StoreLayout) -> CovenBuilder {
    app.builder(layout)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::sql(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL)",
        )])
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
}

#[tokio::test]
async fn application_lifecycle_two_stores_lock_read_only_live_query_and_reopen() {
    set_keyring_service("com.example.notes").unwrap();
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let first = app
        .create_store(&layout, "Household", ids.clone())
        .await
        .unwrap();
    let second = app.create_store(&layout, "Work", ids).await.unwrap();
    assert_eq!(
        layout
            .stores()
            .await
            .unwrap()
            .iter()
            .map(|s| &s.name)
            .collect::<Vec<_>>(),
        [&"Household", &"Work"]
    );
    let a = builder(&app, layout.clone())
        .open(first.id())
        .await
        .unwrap();
    let b = builder(&app, layout.clone())
        .open(second.id())
        .await
        .unwrap();
    assert!(matches!(
        builder(&app, layout.clone()).open(first.id()).await,
        Err(CovenError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    assert!(matches!(
        app.delete_store(&first, &[]).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    let reader = builder(&app, layout.clone())
        .open_read_only(first.id())
        .await
        .unwrap();
    let mut live = a.subscribe(|sql| {
        Ok(
            sql.query("SELECT title FROM notes ORDER BY title", [], |r| {
                r.get::<_, String>(0)
            })?,
        )
    });
    assert!(live.next().await.unwrap().is_empty());
    a.write(|sql| {
        sql.execute("INSERT INTO notes VALUES(?1,?2)", ("a", "Paint colors"))?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(live.next().await.unwrap(), ["Paint colors"]);
    let titles = reader
        .read(|sql| Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?))
        .process(|mut titles| {
            titles.sort();
            Ok(titles)
        })
        .await
        .unwrap();
    assert_eq!(titles, ["Paint colors"]);
    assert_eq!(
        b.read(|sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?))
            .await
            .unwrap(),
        0
    );
    assert!(a.lost_values().await.unwrap().is_empty());
    let mut losses = a.subscribe_lost_values();
    assert!(losses.next().await.unwrap().is_empty());
    a.dismiss_lost_values(&[]).await.unwrap();
    let mut by_id = a.subscribe_reconfigurable("a".to_owned(), |id, sql| {
        Ok(sql.query("SELECT title FROM notes WHERE id=?1", [id], |r| {
            r.get::<_, String>(0)
        })?)
    });
    let requests = by_id.requests();
    assert_eq!(by_id.next().await.result.unwrap(), ["Paint colors"]);
    let revision = requests.set("missing".into()).unwrap();
    let event = by_id.next().await;
    assert_eq!(event.revision, revision);
    assert!(event.result.unwrap().is_empty());
    let clone = a.clone();
    a.close().await.unwrap();
    assert!(matches!(
        clone.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(
        clone.host_secret("token"),
        Err(KeyError::StoreClosed)
    ));
    assert!(matches!(
        live.next().await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(
        app.delete_store(&first, &[]).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    reader.close().await.unwrap();
    let a = builder(&app, layout.clone())
        .open(first.id())
        .await
        .unwrap();
    assert_eq!(
        a.read(|sql| Ok(sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?))
            .await
            .unwrap(),
        "Paint colors"
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
    app.delete_store(&first, &[]).await.unwrap();
    app.delete_store(&first, &[]).await.unwrap();
    assert_eq!(
        layout.stores().await.unwrap(),
        vec![StoreInfo {
            id: second.id(),
            name: "Work".into()
        }]
    );
}

#[tokio::test]
async fn host_secrets_identity_and_failed_deletion_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let directory = app
        .create_store(&layout, "private", Arc::new(UuidIds))
        .await
        .unwrap();
    let key_id = KeyId(UuidIds.new_id());
    let keys = StoreKeyring::new(StoreKey::generate(key_id).unwrap());
    let sealed = keys
        .seal_app_data(key_id, b"store secret", b"row/1")
        .unwrap();
    app.keep_store_keys(&directory, &keys).unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    let member = handle.initialize_identity().unwrap();
    assert!(matches!(
        handle.initialize_identity(),
        Err(IdentityError::AlreadyInitialized)
    ));
    handle.set_host_secret("token", "the app's token").unwrap();
    assert!(matches!(
        handle.set_host_secret("device-id", "bad"),
        Err(KeyError::SecretName(SecretNameError::Reserved))
    ));
    handle.close().await.unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    assert!(matches!(
        handle.initialize_identity(),
        Err(IdentityError::AlreadyInitialized)
    ));
    assert_eq!(
        handle.host_secret("token").unwrap().as_deref(),
        Some("the app's token")
    );
    handle.close().await.unwrap();
    app.fail_next_keychain_operation();
    assert!(matches!(
        app.delete_store(&directory, &["token"]).await,
        Err(StoreDeletionError::Key(_))
    ));
    assert_eq!(layout.stores().await.unwrap().len(), 1);
    app.delete_store(&directory, &["token"]).await.unwrap();
    assert!(layout.stores().await.unwrap().is_empty());
    // Recreate exactly this store id to prove deletion removed all its keychain entries.
    struct SameStore(std::sync::Mutex<Option<StoreId>>, UuidIds);
    impl IdSource for SameStore {
        fn new_id(&self) -> uuid::Uuid {
            match self.0.lock().unwrap().take() {
                Some(id) => id.0,
                None => self.1.new_id(),
            }
        }
    }
    let directory = app
        .create_store(
            &layout,
            "replacement",
            Arc::new(SameStore(
                std::sync::Mutex::new(Some(directory.id())),
                UuidIds,
            )),
        )
        .await
        .unwrap();
    let replacement = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    assert_eq!(replacement.host_secret("token").unwrap(), None);
    assert!(matches!(
        replacement.open_app_data(&sealed, b"row/1"),
        Err(SealError::NoStoreKeys)
    ));
    assert_ne!(replacement.initialize_identity().unwrap(), member);
    replacement.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_device_identity_prevents_publication() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    app.fail_next_keychain_operation();
    assert!(matches!(
        app.create_store(&layout, "refused", Arc::new(UuidIds))
            .await,
        Err(StoreCreationError::Initialization { .. })
    ));
    assert!(layout.stores().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_breaking_migration_converts_the_waiting_write() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let directory = app
        .create_store(&layout, "migrations", Arc::new(UuidIds))
        .await
        .unwrap();
    let first = || {
        Migration::sql(
            1,
            "initial",
            "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL)",
        )
    };
    let table = || vec![SyncedTable::new("attachments", RowIdentity::SharedKey)];
    let handle = app
        .builder(layout.clone())
        .synced_tables(table())
        .migrations(vec![first()])
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
        .open(directory.id())
        .await
        .unwrap();
    handle
        .write(|sql| {
            sql.execute("INSERT INTO attachments VALUES('a','Swatches')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    handle.close().await.unwrap();
    let converted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = converted.clone();
    let migration = Migration::sql(
        2,
        "rename_attachment_title",
        "ALTER TABLE attachments RENAME COLUMN title TO name",
    )
    .writes(move |change| {
        if change.table == "attachments" {
            change.rename_column("title", "name");
        }
        output.lock().unwrap().push(change.clone());
        Ok(())
    });
    let handle = app
        .builder(layout.clone())
        .synced_tables(table())
        .migrations(vec![first(), migration])
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
        .open(directory.id())
        .await
        .unwrap();
    {
        let converted = converted.lock().unwrap();
        assert_eq!(converted.len(), 1);
        assert!(converted[0]
            .columns
            .iter()
            .any(|column| column.name == "name"));
    }
    assert_eq!(
        handle
            .read(
                |sql| Ok(sql.query_row("SELECT name FROM attachments", [], |r| r
                    .get::<_, String>(0))?)
            )
            .await
            .unwrap(),
        "Swatches"
    );
    handle.close().await.unwrap();
}
