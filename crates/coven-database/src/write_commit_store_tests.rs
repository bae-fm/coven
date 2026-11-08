//! How many statements a write's merge state takes, and what the
//! multi-row ones return.

use crate::tests::TestStore;
use crate::write::tests::{count, sql};
use crate::{Database, SyncedTable};

/// A write's merge state is stored in a statement for each kind of record,
/// not one for each row or column. A generation just inserted has no written
/// values to clear; one a write updates may. A generation's id is never read
/// back: storing it returns the id, and recording its removal state uses that.
#[tokio::test]
async fn a_row_stores_its_cells_in_one_statement() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT)",
        )
        .await
        .unwrap();
    crate::write::tests::sql(
        &db,
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<50)
         INSERT INTO notes SELECT 'note ' || i, 'title', 'body' FROM n",
    )
    .await
    .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let runs = |text: &str| statements.iter().filter(|s| s.sql.contains(text)).count();
    assert_eq!(runs("INSERT INTO _coven_cells"), 1);
    assert_eq!(runs("INSERT INTO _coven_rows"), 1);
    assert_eq!(runs(crate::written_values::tests::CLEARED), 0);
    assert_eq!(runs("SELECT constraint_id,value FROM _coven_claims"), 0);
    assert_eq!(runs("SELECT DISTINCT table_name,key,audience FROM _coven_rows"), 0);
    assert_eq!(runs("SELECT id FROM _coven_rows WHERE"), 0);
    assert_eq!(count(&db, "_coven_cells"), 150);
    crate::write::tests::sql(&db, "UPDATE notes SET title='again' WHERE id<'note 2'")
        .await
        .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let updated = count(&db, "notes WHERE title='again'") as usize;
    let cleared =
        statements.iter().filter(|s| s.sql.contains(crate::written_values::tests::CLEARED)).count();
    let claims = statements
        .iter()
        .filter(|s| s.sql.contains("SELECT constraint_id,value FROM _coven_claims"))
        .count();
    assert!(updated > 0);
    assert_eq!((cleared, claims), (updated, updated));
    db.close().await.unwrap();
}

/// A write's references are upserted in a statement for the write, not one
/// for each reference: a row naming another parent takes the conflict path
/// and keeps its identity, and a write that leaves every reference alone
/// upserts none.
#[tokio::test]
async fn a_write_upserts_its_references_in_one_statement() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                SyncedTable::new("notes", crate::RowIdentity::SharedKey),
                SyncedTable::new("tags", crate::RowIdentity::SharedKey),
            ],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT);
             CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY, note TEXT REFERENCES notes(id), label TEXT)",
        )
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes VALUES('a','one'),('b','two'),('c','three')").await.unwrap();
    let upserts = |db: &Database| {
        db.inspect_writer(|writer| writer.profiled_statements())
            .iter()
            .filter(|s| s.sql.contains("INSERT INTO _coven_references"))
            .count()
    };
    assert_eq!(upserts(&db), 0, "notes name no parent");
    let parents = |db: &Database| {
        db.inspect_writer(|writer| {
            writer.query(
                "SELECT r.key,f.parent_key FROM _coven_references f JOIN _coven_rows r ON r.id=f.row_id ORDER BY r.key",
                [],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
        })
        .unwrap()
    };
    sql(
        &db,
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<60)
         INSERT INTO tags SELECT 'tag ' || printf('%02d', i), CASE WHEN i%2 THEN 'a' ELSE 'b' END, 'x' FROM n",
    )
    .await
    .unwrap();
    assert_eq!(upserts(&db), 1);
    let first = parents(&db);
    assert_eq!(first.len(), 60);
    sql(&db, "UPDATE tags SET note='c' WHERE id<'tag 31'").await.unwrap();
    assert_eq!(upserts(&db), 1);
    let second = parents(&db);
    assert_eq!(second.len(), 60);
    let moved = second.iter().zip(&first).filter(|(now, before)| now.1 != before.1).count();
    assert_eq!(moved, 30);
    assert_eq!(second[30..], first[30..]);
    assert!(second[..30].iter().all(|(_, parent)| *parent == second[0].1));
    sql(&db, "UPDATE tags SET label='y'").await.unwrap();
    assert_eq!(upserts(&db), 0, "no reference changed");
    sql(&db, "UPDATE notes SET title='again'").await.unwrap();
    assert_eq!(upserts(&db), 0, "a parent's own change leaves its children's references");
    assert_eq!(parents(&db), second);
    db.close().await.unwrap();
}

/// Generations stored together: a new one is fresh, one already stored is
/// found, given the new writer and not fresh, and the others keep theirs.
#[tokio::test]
async fn stored_generations_tell_inserted_from_found() {
    use coven_merge::{Audience, RowId};
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT)",
        )
        .await
        .unwrap();
    let row = |n: u8| RowId { table: "notes".into(), key: vec![n], audience: Audience::Store };
    let (one, two, three) = (row(1), row(2), row(3));
    db.inspect_writer(|writer| {
        writer
            .transaction(|writer| {
                let write = |n: u8| {
                    writer.query_row(
                        "INSERT INTO _coven_writes(timestamp,number,read_through,slot) VALUES(?1,?2,zeroblob(8),NULL) RETURNING id",
                        rusqlite::params![vec![n; 16], vec![0, 0, 0, 0, 0, 0, 0, n]],
                        |r| r.get::<_, i64>(0),
                    )
                };
                let (early, late) = (write(1)?, write(2)?);
                let first = crate::write_commit_store::store_generations(writer, &[(&one, 1, early), (&two, 1, early)])?;
                assert!(first.values().all(|stored| stored.fresh));
                let second = crate::write_commit_store::store_generations(
                    writer,
                    &[(&one, 1, late), (&three, 1, late), (&one, 2, late)],
                )?;
                assert!(!second[&(&one, 1)].fresh);
                assert_eq!(second[&(&one, 1)].ordinal, first[&(&one, 1)].ordinal);
                assert!(second[&(&three, 1)].fresh);
                assert!(second[&(&one, 2)].fresh);
                let writers: Vec<(Vec<u8>, Vec<u8>, i64)> = writer.query(
                    "SELECT key,generation,write_id FROM _coven_rows ORDER BY key,generation",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?;
                let generation = |n: u64| n.to_be_bytes().to_vec();
                assert_eq!(
                    writers,
                    [
                        (vec![1], generation(1), late),
                        (vec![1], generation(2), late),
                        (vec![2], generation(1), early),
                        (vec![3], generation(1), late),
                    ]
                );
                Ok(())
            })
            .unwrap();
    });
    db.close().await.unwrap();
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
("write_commit_store.rs", "INSERT INTO _coven_rows(table_name,key,audience,generation,write_id) VALUES{values} ON CONFLICT(table_name,key,audience,generation) DO NOTHING RETURNING table_name,key,audience,generation,id", "2-ROW") => Some("a write's rows are the statement's own values"),
("write_commit_store.rs", "INSERT INTO _coven_cells(column_id,row_id,write_id) VALUES{values} ON CONFLICT(column_id,row_id) DO UPDATE SET write_id=excluded.write_id", "2-ROW") =>
    Some("a row's cells are the statement's own values"),
("write_commit_store.rs", "INSERT INTO _coven_references(row_id,column_id,foreign_key_id,parent_table,parent_key,parent_audience,parent_generation) VALUES{values} ON CONFLICT(row_id,column_id,foreign_key_id) DO UPDATE SET parent_table=excluded.parent_table,parent_key=excluded.parent_key,parent_audience=excluded.parent_audience,parent_generation=excluded.parent_generation", "2-ROW") =>
    Some("a write's references are the statement's own values"),
        _ => None,
    }
}
