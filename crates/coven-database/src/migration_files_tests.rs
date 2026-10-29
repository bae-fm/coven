//! This device's file records follow a migration's renames (§18).
use crate::{
    file_write::tests::complete_uploads, tests::TestStore, FileDecl, Migration, OldWrites,
    Provenance, RowIdentity, SyncedTable,
};

/// §18: a migration renaming a table, or the column naming its file, keeps
/// this device's pending attachment and its original's record under the new
/// names. The upload completes into the renamed row, the `FileRef` taken
/// after the rename stays valid through completion, and nothing is abandoned.
#[tokio::test]
async fn a_pending_attachment_and_its_original_follow_a_rename() {
    const SCHEMA: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,size INTEGER,hash BLOB,location TEXT)";
    let declare = |table: &str, column: &str| {
        vec![
            SyncedTable::new(table, RowIdentity::SharedKey).carries_files(
                FileDecl::new("files", 1 << 30, Provenance::UserProvided).with_id_column(column),
            ),
        ]
    };
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
            .open()
            .await
            .unwrap();
        let prepared = crate::prepare_user_file(original.path(), |_| {})
            .await
            .unwrap();
        db.write(move |sql| {
            sql.insert_user_file(
                "files",
                "7",
                prepared,
                "INSERT INTO files(id,file,size) VALUES('7','photo',8)",
                &[],
            )
        })
        .await
        .unwrap();
        let recorded = db.user_file("files", "7").await.unwrap();
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
            .open()
            .await
            .unwrap();
        assert_eq!(db.user_file(table, "7").await.unwrap(), recorded);
        let pending = db.file_ref(table, "7").await.unwrap();
        assert_eq!((pending.table(), pending.column()), (table, column));
        complete_uploads(&db).await;
        assert!(db.abandoned_attachments().await.unwrap().is_empty());
        assert_eq!(db.file_ref(table, "7").await.unwrap(), pending);
        assert_eq!(db.user_file(table, "7").await.unwrap(), recorded);
        db.close().await.unwrap();
    }
}

/// A carried pin keeps its file through a rename: its captured reference
/// names the renamed table and still validates against the row (§17.6).
#[tokio::test]
async fn a_carried_pin_follows_a_rename() {
    const SCHEMA: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,size INTEGER,hash BLOB,location TEXT)";
    let declare = |table: &str| {
        vec![
            SyncedTable::new(table, RowIdentity::SharedKey).carries_files(
                FileDecl::new("files", 1 << 30, Provenance::UserProvided).with_id_column("file"),
            ),
        ]
    };
    let store = TestStore::new();
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"original").unwrap();
    let db = store
        .builder(
            declare("files"),
            vec![Migration::addition(1, "files", SCHEMA)],
        )
        .open()
        .await
        .unwrap();
    let prepared = crate::prepare_user_file(original.path(), |_| {})
        .await
        .unwrap();
    db.write(move |sql| {
        sql.insert_user_file(
            "files",
            "7",
            prepared,
            "INSERT INTO files(id,file,size) VALUES('7','photo',8)",
            &[],
        )
    })
    .await
    .unwrap();
    let owner = crate::FileDatabase::new(db.clone());
    let pending = db.file_ref("files", "7").await.unwrap();
    assert!(matches!(
        owner.pin_complete(&pending).await.unwrap(),
        Some(crate::PinnedFile::Queued)
    ));
    complete_uploads(&db).await;
    assert_eq!(owner.pin_requests().await.unwrap(), [pending]);
    db.close().await.unwrap();
    let db = store
        .builder(
            declare("documents"),
            vec![
                Migration::addition(1, "files", SCHEMA),
                Migration::breaking(
                    2,
                    "rename",
                    "ALTER TABLE files RENAME TO documents",
                    OldWrites::Drop,
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    let owner = crate::FileDatabase::new(db.clone());
    let [carried] = owner.pin_requests().await.unwrap().try_into().unwrap();
    assert_eq!(carried.table(), "documents");
    assert_eq!(db.file_ref("documents", "7").await.unwrap(), carried);
    owner.validate(&carried).await.unwrap();
    db.close().await.unwrap();
}
