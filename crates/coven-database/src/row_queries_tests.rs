use crate::tests::TestStore;
use crate::write::tests::sql;
use crate::{Database, Migration, RowIdentity, SyncedTable};
use coven_merge::ForeignKey;

type Registered = (Vec<(String, String)>, Vec<(String, ForeignKey)>);

fn registered(db: &Database) -> Registered {
    db.inspect_writer(|db| {
        let mut columns = db
            .query("SELECT table_name,column_name FROM _coven_columns", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        let mut keys = db
            .query("SELECT table_name,identity FROM _coven_foreign_keys", [], |r| {
                Ok((
                    r.get(0)?,
                    coven_format::merge_fields::decode_foreign_key(&r.get::<_, Vec<u8>>(1)?)
                        .unwrap(),
                ))
            })
            .unwrap();
        columns.sort();
        keys.sort();
        (columns, keys)
    })
}

/// These app tables' columns and foreign keys, beside coven's files table,
/// which syncs in every store.
fn expected(columns: &[(&str, &[&str])], keys: &[(&str, &str)]) -> Registered {
    let files: (&str, &[&str]) =
        ("coven_files", &["audience", "coordinates", "hash", "id", "namespace", "size"]);
    let mut columns: Vec<_> = columns
        .iter()
        .chain([&files])
        .flat_map(|(table, names)| {
            names.iter().map(|name| ((*table).to_owned(), (*name).to_owned()))
        })
        .collect();
    let mut keys: Vec<_> = keys
        .iter()
        .map(|(table, column)| ((*table).to_owned(), ForeignKey::new([*column], "notes", ["id"])))
        .collect();
    columns.sort();
    keys.sort();
    (columns, keys)
}

/// Appendix B.1: `_coven_columns` has one row per synced column and `_coven_foreign_keys`
/// one per foreign key of a synced table, before any write names them, through
/// additions, a dropped column and a renamed one. Local tables have none, and
/// writes only look the rows up.
#[tokio::test]
async fn every_synced_column_and_foreign_key_is_registered_before_any_write_names_it() {
    let steps = [
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE CASCADE,other TEXT REFERENCES notes(id) ON DELETE SET NULL); CREATE TABLE pins(note_id TEXT REFERENCES notes(id) ON DELETE CASCADE)",
        "CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE CASCADE); ALTER TABLE links ADD COLUMN extra TEXT",
        "CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE CASCADE,extra TEXT); INSERT INTO rebuilt SELECT id,note_id,extra FROM links; DROP TABLE links; ALTER TABLE rebuilt RENAME TO links",
        "ALTER TABLE links RENAME COLUMN note_id TO note",
    ];
    let migrations = |through: usize| {
        steps[..through]
            .iter()
            .enumerate()
            .map(|(index, sql)| {
                if index < 2 {
                    Migration::addition(index as u32 + 1, "step", sql)
                } else {
                    Migration::breaking(
                        index as u32 + 1,
                        "step",
                        sql,
                        crate::OldWrites::Drop,
                        Vec::new(),
                    )
                }
            })
            .collect::<Vec<_>>()
    };
    let tables = |tags: bool| {
        let mut tables = vec![
            SyncedTable::new("notes", RowIdentity::SharedKey),
            SyncedTable::new("links", RowIdentity::SharedKey),
        ];
        if tags {
            tables.push(SyncedTable::new("tags", RowIdentity::SharedKey));
        }
        tables
    };
    let stages = [
        expected(
            &[("notes", &["id"]), ("links", &["id", "note_id", "other"])],
            &[("links", "note_id"), ("links", "other")],
        ),
        expected(
            &[
                ("notes", &["id"]),
                ("links", &["id", "note_id", "other", "extra"]),
                ("tags", &["id", "note_id"]),
            ],
            &[("links", "note_id"), ("links", "other"), ("tags", "note_id")],
        ),
        expected(
            &[
                ("notes", &["id"]),
                ("links", &["id", "note_id", "extra"]),
                ("tags", &["id", "note_id"]),
            ],
            &[("links", "note_id"), ("tags", "note_id")],
        ),
        expected(
            &[
                ("notes", &["id"]),
                ("links", &["id", "note", "extra"]),
                ("tags", &["id", "note_id"]),
            ],
            &[("links", "note"), ("tags", "note_id")],
        ),
    ];
    let store = TestStore::new();
    for (index, stage) in stages.iter().enumerate() {
        let db = store.builder(tables(index > 0), migrations(index + 1)).open().await.unwrap();
        assert_eq!(&registered(&db), stage, "after step {}", index + 1);
        db.close().await.unwrap();
    }
    let db = store.builder(tables(true), migrations(steps.len())).open().await.unwrap();
    sql(
        &db,
        "INSERT INTO notes VALUES('n'); INSERT INTO links VALUES('l','n','x'); INSERT INTO tags VALUES('t','n'); INSERT INTO pins VALUES('n')",
    )
    .await
    .unwrap();
    assert_eq!(&registered(&db), stages.last().unwrap());
    db.close().await.unwrap();
}

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_columns SELECT i,tab,'column_' || i,1 FROM fixture;
         INSERT INTO _coven_constraints SELECT i,tab,n FROM fixture;
         INSERT INTO _coven_foreign_keys SELECT i,tab,n FROM fixture;
         INSERT INTO _coven_owner_tables SELECT DISTINCT 'table_' || i FROM fixture;",
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
("row_queries.rs", "DELETE FROM _coven_columns WHERE id=?1"
    | "DELETE FROM _coven_columns WHERE id=?1 AND NOT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=?1)", "_coven_cells") =>
    Some("once per migration: drop a column, and SQLite then checks the cells' foreign key"),
("_coven_lost_column_deleted" | "_coven_lost_column_moved", "DELETE FROM _coven_columns WHERE id=?4 AND synced=0 AND NOT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=?4)", "_coven_cells") =>
    Some("only a dropped column's last frozen loss deletes a row, and SQLite then checks the cells' foreign key"),
("row_queries.rs", "WITH k(t,k,a) AS (VALUES {values}) SELECT DISTINCT k.t,k.k,k.a FROM k CROSS JOIN _coven_references v ON v.parent_table=k.t AND v.parent_key=k.k AND v.parent_audience=k.a", "2" | "k") =>
    Some("a write's inserted rows are the statement's own values"),
        _ => None,
    }
}
