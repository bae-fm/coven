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

/// §18.4: a breaking migration that leaves a file's record and the row
/// referencing it as they were sets neither, so a later write on each names
/// the write that staged the file as its source. The migration write's
/// changes reach other devices only in its raise's snapshot, so a later write
/// naming it would have no source on a device that adopts another device's
/// raise instead.
#[tokio::test]
async fn a_migration_leaves_file_records_and_references_with_their_writes() {
    use crate::file_write::tests::{open, stage, tables, SCHEMA};
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, b"cover".to_vec(), true).await.unwrap();
    let staged = crate::write::tests::records(&db)[0].header.position;
    db.close().await.unwrap();
    let db = store
        .builder(
            tables(),
            vec![
                Migration::addition(1, "files", SCHEMA),
                Migration::breaking(
                    2,
                    "titles",
                    "CREATE INDEX files_title ON files(title)",
                    OldWrites::Drop,
                ),
            ],
        )
        .namespaces(crate::file_write::tests::namespaces(
            Provenance::AppProvided,
        ))
        .open()
        .await
        .unwrap();
    let marker = crate::write::tests::records(&db)[1].header.position;
    let set: i64 = db.inspect_writer(|sql| {
        sql.query_row(
            "SELECT (SELECT count(*) FROM _coven_rows r JOIN _coven_writes w ON w.id=r.write_id
                     WHERE w.number=?1)
                  + (SELECT count(*) FROM _coven_cells c JOIN _coven_writes w ON w.id=c.write_id
                     WHERE w.number=?1)",
            [marker.number.to_be_bytes()],
            |r| r.get(0),
        )
        .unwrap()
    });
    assert_eq!(set, 0, "the migration sets no generation and no cell");
    complete_uploads(&db).await;
    db.write(|sql| {
        sql.execute("UPDATE files SET title='later' WHERE id='7'", [])?;
        Ok::<_, DbError>(())
    })
    .await
    .unwrap();
    let records = crate::write::tests::records(&db);
    let mut sources = Vec::new();
    for record in &records[records.len() - 2..] {
        for part in &record.parts {
            for row in &part.rows {
                sources.push((row.row.table.clone(), row.sources.generation.clone()));
            }
        }
    }
    sources.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        sources
            .iter()
            .map(|(table, generation)| (
                table.as_str(),
                generation.as_ref().map(|source| source.write)
            ))
            .collect::<Vec<_>>(),
        [("coven_files", Some(staged)), ("files", Some(staged))],
    );
    db.close().await.unwrap();
}
