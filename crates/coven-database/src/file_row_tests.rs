use crate::{
    file_write::tests::{attach, local_count, owned_paths, tables, SCHEMA},
    tests::TestStore,
    *,
};

#[tokio::test]
async fn invalid_batches_report_the_namespace_and_file_without_leaving_bytes() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let duplicate = db
        .write_with_files::<_, _, _, crate::DbError>(
            |batch| {
                batch.put_file("files", "7", vec![1]);
                batch.put_file("files", "7", vec![2]);
                Ok(())
            },
            |_| Ok(()),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(duplicate, DbError::FileBatchDuplicate { namespace, id } if namespace == "files" && id == "7")
    );
    let namespace = db
        .write_with_files::<_, _, _, crate::DbError>(
            |batch| {
                batch.put_file("unknown", "7", vec![1]);
                Ok(())
            },
            |_| Ok(()),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(namespace, DbError::FileNamespaceNotAppProvided { namespace } if namespace == "unknown")
    );
    assert!(owned_paths(&store).is_empty());
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    let original = tempfile::NamedTempFile::new().unwrap();
    let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
    let error = db
        .write(move |sql| sql.register_user_file("files", "7", prepared))
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::FileTableNotUserProvided { table } if table == "files"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn invalid_file_lookups_report_the_declaration_and_key_shape() {
    let store = TestStore::new();
    let mut declarations = tables(Provenance::AppProvided);
    declarations.push(SyncedTable::new("notes", RowIdentity::SharedKey));
    let schema = format!("{SCHEMA}; CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY)");
    let db = store
        .builder(
            declarations,
            vec![Migration::run(1, "tables", move |sql| {
                sql.execute_batch(&schema)?;
                Ok(())
            })],
        )
        .open()
        .await
        .unwrap();
    assert!(
        matches!(db.file_ref("missing", "7").await, Err(DbError::FileTableNotSynced { table }) if table == "missing")
    );
    assert!(
        matches!(db.file_ref("notes", "7").await, Err(DbError::FileNotDeclared { table }) if table == "notes")
    );
    assert!(
        matches!(db.file_ref("files", RowKey(vec![])).await, Err(DbError::FileKeyArity { table, expected: 1, actual: 0 }) if table == "files")
    );
    assert!(
        matches!(db.file_ref("files", "7").await, Err(DbError::FileAbsent { table, key }) if table == "files" && key == RowKey::from("7"))
    );
    attach(&db, vec![1], true).await.unwrap();
    let error = db
        .write(|sql| {
            sql.execute("UPDATE files SET size=2", [])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, DbError::FileBytesRequired { table, key } if table == "files" && key == RowKey::from("7"))
    );
    assert_eq!(db.file_ref("files", "7").await.unwrap().plaintext_size(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn invalid_original_rows_report_id_size_and_attachment_changes() {
    let store = TestStore::new();
    let declaration = SyncedTable::new("files", RowIdentity::SharedKey).carries_files(
        FileDecl::new("files", Provenance::UserProvided, CacheFill::CacheLazy)
            .with_id_column("file"),
    );
    let db = store.schema(vec![declaration], "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,size INTEGER,hash BLOB,location TEXT)").await.unwrap();
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), [1]).unwrap();
    for case in 0..3 {
        let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
        let error = db
            .write(move |sql| {
                sql.execute(
                    "INSERT INTO files(id,file,size) VALUES('7',?1,?2)",
                    (
                        if case == 0 { None } else { Some("original") },
                        if case == 1 { -1 } else { 1 },
                    ),
                )?;
                sql.register_user_file("files", "7", prepared)?;
                if case == 2 {
                    sql.execute("UPDATE files SET file='changed'", [])?;
                }
                Ok(())
            })
            .await
            .unwrap_err();
        match case {
            0 => assert!(matches!(error, DbError::FileIdMissing { column } if column == "file")),
            1 => assert!(
                matches!(error, DbError::FileSizeInvalid { column, value: types::Value::Integer(-1) } if column == "size")
            ),
            2 => assert!(
                matches!(error, DbError::FileAttachmentChanged { table, key } if table == "files" && key == RowKey::from("7"))
            ),
            _ => unreachable!(),
        }
        assert_eq!(local_count(&db, "files"), 0);
        assert_eq!(local_count(&db, "_coven_user_files"), 0);
    }
    assert_eq!(std::fs::read(original.path()).unwrap(), [1]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn inconsistent_hash_and_location_report_the_row_and_refuse_the_write() {
    for column in ["hash", "location"] {
        let store = TestStore::new();
        let db = store
            .schema(tables(Provenance::AppProvided), SCHEMA)
            .await
            .unwrap();
        attach(&db, vec![1], true).await.unwrap();
        // Inject damage behind app SQL's managed-column authorization.
        db.commit_writer(|sql| {
            sql.internal_execute(&format!("UPDATE files SET {column}=NULL"), [])
                .unwrap()
        });
        let error = db
            .write(|sql| {
                sql.execute("UPDATE files SET title='must roll back'", [])?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, DbError::FileHashLocationMismatch { table, key } if table == "files" && key == RowKey::from("7"))
        );
        assert_eq!(
            db.read(|sql| Ok(sql.query_row("SELECT title FROM files", [], |r| r
                .get::<_, Option<String>>(0))?))
                .await
                .unwrap(),
            None
        );
        assert_eq!(owned_paths(&store).len(), 1);
        db.close().await.unwrap();
    }
}
