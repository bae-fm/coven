use crate::{
    file_write::tests::{attach, complete_uploads, SCHEMA},
    tests::TestStore,
    write::tests::records,
    CacheFill, FileDatabase, FileDecl, Provenance, RowIdentity, SyncedTable,
};
use coven_foundation::id_source::SequentialIds;

fn eager() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
            "files",
            1 << 30,
            Provenance::AppProvided,
            CacheFill::CacheEager,
        )),
    ]
}

async fn work(db: &crate::Database) -> Vec<crate::FileRef> {
    FileDatabase::new(db.clone()).eager_work().await.unwrap()
}

/// Only a changed fixed file becomes eager work: an edit to another column
/// requests nothing again, and work is noted in the transaction that
/// changes the row (§16.4, SP, Files and waiting).
#[tokio::test]
async fn only_changed_file_references_become_eager_work() {
    let ids = SequentialIds::new();
    let (a_store, b_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
    let a = a_store.schema(eager(), SCHEMA).await.unwrap();
    let b = b_store.schema(eager(), SCHEMA).await.unwrap();
    attach(&a, b"first".to_vec(), true).await.unwrap();
    // Queued here, the attaching device's file is already local.
    assert!(work(&a).await.is_empty());
    let mut sent = records(&a).len();
    b.receive(records(&a).remove(0).into()).await.unwrap();
    let first = b.file_ref("files", "7").await.unwrap();
    assert_eq!(work(&b).await, vec![first.clone()]);
    FileDatabase::new(b.clone())
        .settle_eager_work(vec![first.clone()])
        .await
        .unwrap();
    // Another column changes; the file does not.
    a.write(|sql| {
        sql.execute("UPDATE files SET title='renamed'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    for record in records(&a).split_off(sent) {
        b.receive(record.into()).await.unwrap();
    }
    sent = records(&a).len();
    assert!(work(&b).await.is_empty());
    // A different file is new work.
    attach(&a, b"second".to_vec(), false).await.unwrap();
    for record in records(&a).split_off(sent) {
        b.receive(record.into()).await.unwrap();
    }
    let second = b.file_ref("files", "7").await.unwrap();
    assert_ne!(second.id(), first.id());
    assert_eq!(work(&b).await, vec![second]);
    // Completing its own upload leaves the attaching device's bytes to its
    // cache: the eager row's current file becomes its work.
    complete_uploads(&a).await;
    assert_eq!(
        work(&a).await,
        vec![a.file_ref("files", "7").await.unwrap()]
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
}
