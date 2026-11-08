/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_row_file_refs SELECT device,substr(key,1,16),tab,key FROM fixture;
         INSERT INTO _coven_stored_files SELECT 'file_' || i,audience,device,substr(key,17,16),'file_' || i,key FROM fixture;
         INSERT INTO _coven_file_references SELECT 'file_' || i,tab,key,'column_' || i FROM fixture;
         INSERT INTO _coven_unreferenced_files SELECT 'file_' || i FROM fixture;
         INSERT INTO _coven_queued_file_refs SELECT device,substr(key,1,16),device,n FROM fixture;",
    )
    .unwrap();
}

/// The scans this module's statements may make, with their reasons, for the
/// plan audit (`internal_schema_scan_reasons.rs`): `template` is the statement as
/// written, `sql` as run.
pub(crate) fn scan_reason(
    file: &str,
    template: &str,
    _sql: &str,
    relation: &str,
) -> Option<&'static str> {
    match (file, template, relation) {
("file_protection.rs", "SELECT DISTINCT table_name,key FROM _coven_file_references", "_coven_file_references") =>
    Some("once per migration: rebuild every referencing row's file references"),
("file_protection.rs", "DELETE FROM _coven_unreferenced_files WHERE EXISTS( SELECT 1 FROM _coven_file_references AS r WHERE r.file=_coven_unreferenced_files.file) OR NOT EXISTS(SELECT 1 FROM main.coven_files AS f WHERE f.id=_coven_unreferenced_files.file)", "_coven_unreferenced_files") =>
    Some("files other devices' writes left for this device's next write, which concurrent removals leave; normally none"),
        _ => None,
    }
}

fn references(db: &crate::Database) -> Vec<(coven_foundation::id_source::FileId, String)> {
    db.inspect_writer(|sql| {
        sql.query("SELECT file,column_name FROM _coven_file_references ORDER BY file", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap()
    })
}

/// A local write's references are those of the merged state it gave its
/// rows, whichever way a file cell changes: unset to set, set to another
/// file, set to NULL, and NULL and set again within the one write.
#[tokio::test]
async fn a_local_writes_references_follow_the_cells_it_merged() {
    use crate::file_write::tests::{has_record, open, stage};
    let store = crate::tests::TestStore::new();
    let db = open(&store, crate::Provenance::AppProvided).await;
    db.write(|sql| {
        sql.execute("INSERT INTO files(id) VALUES('7')", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(references(&db), []);
    let first = stage(&db, b"first".to_vec(), false).await.unwrap();
    assert_eq!(references(&db), [(first, "file".into())]);
    // Set to NULL and back to the same file within one write: still referenced.
    db.write(move |sql| {
        sql.execute("UPDATE files SET file=NULL WHERE id='7'", [])?;
        sql.execute("UPDATE files SET file=?1 WHERE id='7'", [first])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(references(&db), [(first, "file".into())]);
    assert!(has_record(&db, first));
    let second = stage(&db, b"second".to_vec(), false).await.unwrap();
    assert_eq!(references(&db), [(second, "file".into())]);
    assert!(!has_record(&db, first));
    db.write(|sql| {
        sql.execute("UPDATE files SET file=NULL WHERE id='7'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(references(&db), []);
    assert!(!has_record(&db, second));
    db.close().await.unwrap();
}
