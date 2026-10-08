use crate::{DatabaseBuilder, Migration, RowIdentity, SyncedTable};
use coven_foundation::{
    files::{FileName, StoreLayout},
    id_source::{StoreId, UuidIds},
};

#[tokio::test]
async fn rebuilding_schema_does_not_author_seed_rows_or_migration_writes() {
    let root = tempfile::tempdir().unwrap();
    let store = StoreLayout::new(root.path().into())
        .create_store_dir(StoreId(uuid::Uuid::from_u128(1)), "Recovery", &UuidIds)
        .unwrap();
    std::fs::write(store.database_path(), "damaged").unwrap();
    let lock = store.lock_exclusive().unwrap();
    let database = DatabaseBuilder::new(store)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::sql(
            1,
            "seed",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); INSERT INTO notes VALUES('seed')",
        )])
        .open_reloading_locked(lock, FileName::new("test").unwrap())
        .await
        .unwrap();
    assert!(database.test_queued_writes().await.unwrap().is_empty());
    let count: i64 = database
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(count, 0);
    database.close().await.unwrap();
}

#[tokio::test]
async fn rebuilding_converts_older_waiting_writes_or_marks_them_lost() {
    use coven_format::write::WriteDisposition;
    for convert in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let store = StoreLayout::new(root.path().into())
            .create_store_dir(StoreId(uuid::Uuid::from_u128(1)), "Recovery", &UuidIds)
            .unwrap();
        let initial = || {
            Migration::sql(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL)",
            )
        };
        let builder = || {
            DatabaseBuilder::new(store.clone())
                .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        };
        let db = builder().migrations(vec![initial()]).open().await.unwrap();
        db.write(|sql| {
            sql.execute("INSERT INTO notes VALUES('waiting','offline')", [])?;
            Ok(())
        })
        .await
        .unwrap();
        let original = db.test_queued_writes().await.unwrap().remove(0);
        db.close().await.unwrap();
        let mut rename =
            Migration::sql(2, "rename", "ALTER TABLE notes RENAME COLUMN title TO name");
        if convert {
            rename = rename.writes(|row| {
                row.rename_column("title", "name");
                Ok(())
            });
        }
        let db = builder()
            .migrations(vec![initial(), rename])
            .migration_operation(|_| panic!("reconstruction must not publish a migration"))
            .open_reloading_locked(
                store.lock_exclusive().unwrap(),
                FileName::new("test").unwrap(),
            )
            .await
            .unwrap();
        let records = db.test_queued_writes().await.unwrap();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.header.position, original.header.position);
        assert_eq!(record.header.timestamp, original.header.timestamp);
        assert_eq!(record.header.had_read, original.header.had_read);
        assert_eq!(
            record.header.disposition,
            if convert {
                WriteDisposition::Apply
            } else {
                WriteDisposition::Lost(2)
            }
        );
        assert_eq!(record.header.schema_version, if convert { 2 } else { 1 });
        let count: i64 = db
            .read(|sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, 0);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_failed_rebuild_conversion_retries_from_the_archived_schema() {
    use crate::{CovenError, DbError, MigrationError};
    let root = tempfile::tempdir().unwrap();
    let store = StoreLayout::new(root.path().into())
        .create_store_dir(StoreId(uuid::Uuid::from_u128(1)), "Recovery", &UuidIds)
        .unwrap();
    let past = || {
        vec![
            Migration::sql(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL)",
            ),
            Migration::sql(2, "name", "ALTER TABLE notes RENAME COLUMN title TO name")
                .writes(|_| panic!("source schema already ran this conversion")),
        ]
    };
    let builder = || {
        DatabaseBuilder::new(store.clone())
            .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
    };
    let db = builder().migrations(past()).open().await.unwrap();
    db.write(|sql| {
        sql.execute("INSERT INTO notes VALUES('waiting','offline')", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let original = db.test_queued_writes().await.unwrap();
    db.close().await.unwrap();
    let bytes = std::fs::read(store.database_path()).unwrap();
    for fail in [true, false] {
        let mut migrations = past();
        migrations.push(
            Migration::sql(
                3,
                "heading",
                "ALTER TABLE notes RENAME COLUMN name TO heading",
            )
            .writes(move |row| {
                row.rename_column("name", "heading");
                if fail {
                    return Err(DbError::ClockOutOfRange);
                }
                Ok(())
            }),
        );
        migrations.push(Migration::sql(
            4,
            "color",
            "ALTER TABLE notes ADD COLUMN color TEXT",
        ));
        let result = builder()
            .migrations(migrations)
            .open_reloading_locked(
                store.lock_exclusive().unwrap(),
                FileName::new("test").unwrap(),
            )
            .await;
        assert_eq!(
            std::fs::read(
                store
                    .database_path()
                    .parent()
                    .unwrap()
                    .join("damaged-database-test/store.db")
            )
            .unwrap(),
            bytes
        );
        if fail {
            assert!(matches!(
                result,
                Err(CovenError::Migration(MigrationError::Failed {
                    version: 3,
                    ..
                }))
            ));
            assert!(matches!(
                builder().migrations(past()).open().await,
                Err(CovenError::Lock(
                    coven_foundation::files::StoreLockError::RecoveryPending(_)
                ))
            ));
        } else {
            let db = result.unwrap();
            let records = db.test_queued_writes().await.unwrap();
            assert_eq!(records.len(), original.len());
            let old = original.last().unwrap();
            let new = records.last().unwrap();
            assert_eq!(new.header.position, old.header.position);
            assert_eq!(new.header.schema_version, 4);
            let coven_merge::Operation::Insert(columns) = &new.parts[0].rows[0].change.operation
            else {
                panic!("insert")
            };
            assert_eq!(
                columns["heading"].value,
                coven_format::value::Value::Text("offline".into())
            );
            assert!(!columns.contains_key("name"));
            db.close().await.unwrap();
        }
    }
}
