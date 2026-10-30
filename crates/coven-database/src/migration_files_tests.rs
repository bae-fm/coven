//! This device's file references follow a migration's renames (§18).
use crate::{
    file_write::tests::{complete_uploads, has_record},
    tests::TestStore,
    DbError, FileColumn, FilePlace, Migration, Namespace, OldWrites, Provenance, RowIdentity,
    SyncedTable,
};

/// §18: a migration renaming a table, or its file column, keeps the row's
/// reference to its file under the new names. The upload completes into the
/// same file, and the write removing the renamed row's reference deletes the
/// file's record.
#[tokio::test]
async fn file_references_follow_a_rename() {
    const SCHEMA: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT)";
    let declare = |table: &str, column: &str| {
        vec![SyncedTable::new(table, RowIdentity::SharedKey).file_column(FileColumn::new(column))]
    };
    let namespaces = || vec![Namespace::new("files", Provenance::UserProvided, 1 << 30)];
    for (rename, table, column) in [
        ("ALTER TABLE files RENAME TO documents", "documents", "file"),
        (
            "ALTER TABLE files RENAME COLUMN file TO blob",
            "files",
            "blob",
        ),
    ] {
        let store = TestStore::new();
        let original = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(original.path(), b"original").unwrap();
        let db = store
            .builder(
                declare("files", "file"),
                vec![Migration::addition(1, "files", SCHEMA)],
            )
            .namespaces(namespaces())
            .open()
            .await
            .unwrap();
        let prepared = crate::prepare_user_file(original.path(), |_| {})
            .await
            .unwrap();
        let file = db
            .write_with_files::<_, _, _, _, DbError>(
                move |batch| Ok(batch.put_user_file("files", crate::UserSource::Cloud(prepared))),
                |sql, file| {
                    sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                    Ok(file)
                },
            )
            .await
            .unwrap();
        let recorded = db.user_file(file).await.unwrap();
        assert!(recorded.is_some());
        db.close().await.unwrap();
        let db = store
            .builder(
                declare(table, column),
                vec![
                    Migration::addition(1, "files", SCHEMA),
                    Migration::breaking(2, "rename", rename, OldWrites::Drop),
                ],
            )
            .namespaces(namespaces())
            .open()
            .await
            .unwrap();
        assert_eq!(db.user_file(file).await.unwrap(), recorded);
        assert!(matches!(
            db.file_ref(file).await.unwrap().place(),
            FilePlace::DeviceOnly { .. }
        ));
        complete_uploads(&db).await;
        assert!(matches!(
            db.file_ref(file).await.unwrap().place(),
            FilePlace::Cloud { .. }
        ));
        let named: String = db.inspect_writer(|sql| {
            sql.query_row(
                &format!("SELECT {column} FROM {table} WHERE id='7'"),
                [],
                |r| r.get(0),
            )
            .unwrap()
        });
        assert_eq!(named, file.to_string());
        let delete = format!("DELETE FROM {table}");
        db.write(move |sql| {
            sql.execute(&delete, [])?;
            Ok::<_, DbError>(())
        })
        .await
        .unwrap();
        assert!(
            !has_record(&db, file),
            "the rename kept the reference this delete removes"
        );
        db.close().await.unwrap();
    }
}
