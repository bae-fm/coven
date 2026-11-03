use crate::tests::TestStore;
use crate::write::tests::{notes, sql, NOTES};
use crate::{migration_names::MigrationMatch, DbError};
use coven_format::value::Value;
use coven_merge::Audience;
use std::collections::BTreeMap;

/// A store whose one counted note is captured as published state, with the write
/// metadata table moved away so reading a write's metadata fails in SQLite.
/// `check` runs inside a savepoint that is rolled back afterwards.
async fn unreadable_metadata(
    check: impl FnOnce(&crate::sqlite::DatabaseConnection, &crate::write_schema::WriteSchema),
) {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    sql(&database, "INSERT INTO notes(id,title) VALUES('a','A')").await.unwrap();
    crate::tests::remote_update(&database, "notes", "a", &[("title", Value::Text("B".into()))])
        .await;
    database.inspect_writer_schema(|db, schema| {
        db.batch("SAVEPOINT unreadable").unwrap();
        let store = crate::merge_store::MergeStore::from_schema(db, &schema.schema);
        let rows = db
            .query(
                "SELECT table_name,key,audience FROM _coven_rows",
                [],
                crate::row_queries::read_identity,
            )
            .unwrap();
        crate::published::capture_rows(db, &store, &rows).unwrap();
        db.batch("ALTER TABLE _coven_writes RENAME TO _coven_writes_moved").unwrap();
        check(db, schema);
        db.batch("ROLLBACK TO unreadable; RELEASE unreadable").unwrap();
    });
}

fn is_sqlite_failure(error: &DbError) -> bool {
    matches!(error, DbError::Sqlite(rusqlite::Error::SqliteFailure(..)))
}

#[tokio::test]
async fn reading_the_snapshot_view_fails_with_the_metadata_read_error() {
    unreadable_metadata(|db, schema| {
        let error =
            crate::snapshot_view::rows(db, schema, &BTreeMap::new(), &Audience::Store, |_, _| {
                Ok::<_, DbError>(())
            })
            .unwrap_err();
        assert!(is_sqlite_failure(&error), "{error:?}");
    })
    .await;
}

#[tokio::test]
async fn carrying_published_rows_fails_with_the_metadata_read_error() {
    unreadable_metadata(|db, schema| {
        let names = MigrationMatch {
            tables: BTreeMap::from([("notes".to_owned(), "notes".to_owned())]),
            columns: ["id", "title", "body"]
                .into_iter()
                .map(|c| (("notes".to_owned(), c.to_owned()), c.to_owned()))
                .collect(),
        };
        let error = crate::published_migration::carry(db, &names, schema, &[]).unwrap_err();
        assert!(is_sqlite_failure(&error), "{error:?}");
    })
    .await;
}
