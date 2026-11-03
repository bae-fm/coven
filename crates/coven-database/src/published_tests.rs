//! Test support for the published merge state's tables: filling them for the
//! plan audit, and the full scans their statements' plans may contain.

/// Fill the published merge state's tables, thousands of rows each but one marker,
/// from the rows of the plan audit's `fixture` table, inside its transaction
/// (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_published VALUES(1);
         INSERT INTO _coven_published_rows SELECT tab,key,audience,x'00',i%2 FROM fixture;
         INSERT INTO _coven_published_losses SELECT i,tab,key,audience,x'00' FROM fixture;
         INSERT INTO _coven_published_creations SELECT tab,key,audience,CAST(device || n AS BLOB),x'00' FROM fixture;
         INSERT INTO _coven_published_file_refs SELECT device,substr(key,17,16) FROM fixture;",
    )
    .unwrap();
}

/// The full scans the published merge state's statements'
/// plans may contain, each with its reason, as the plan audit asks
/// (`internal_schema_scan_reasons.rs`): `template` is the statement as
/// written, `sql` as run.
pub(crate) fn scan_reason(
    file: &str,
    template: &str,
    sql: &str,
    relation: &str,
) -> Option<&'static str> {
    match (file, template, relation) {
("published.rs", "SELECT EXISTS(SELECT 1 FROM _coven_published)", "_coven_published") =>
    Some("the counted projection marker has at most one row"),
("published.rs", "DELETE FROM {table}", table) if sql == format!("DELETE FROM {table}") =>
    Some("releasing the counted projection clears all of its retained records"),
("published.rs", "SELECT table_name,key,audience FROM _coven_published_rows UNION SELECT table_name,key,audience FROM _coven_published_losses", "_coven_published_rows" | "_coven_published_losses") =>
    Some("restoration visits every saved row identity"),
("published.rs", "SELECT record FROM _coven_published_rows ORDER BY audience,table_name,key", "_coven_published_rows") =>
    Some("restoration reads every preserved row"),
("published.rs", "SELECT record FROM _coven_published_losses ORDER BY id", "_coven_published_losses") =>
    Some("restoration reads every preserved loss"),
("published.rs", "INSERT INTO _coven_creations SELECT * FROM _coven_published_creations", "_coven_published_creations") =>
    Some("restoration installs every preserved creation"),
("published_migration.rs", "CREATE TEMP TABLE _coven_carried_rows AS SELECT * FROM _coven_published_rows; DELETE FROM _coven_published_rows", "_coven_published_rows") =>
    Some("a migration carries every preserved row through schema changes"),
_ => None,
    }
}

use crate::tests::TestStore;
use crate::write::tests::{records, sql};
use crate::{DbError, Migration, OldWrites, RowIdentity, SyncedTable};
use coven_format::value::Value;
use coven_merge::Audience;

fn synced(tables: &[&str]) -> Vec<SyncedTable> {
    tables.iter().map(|t| SyncedTable::new(*t, RowIdentity::SharedKey)).collect()
}

/// Count every write this device has made so far, as its slots coming back would.
async fn count_own(database: &crate::Database) {
    for record in records(database) {
        database.receive(record.into()).await.unwrap();
    }
}

fn published(database: &crate::Database, table: &str) -> i64 {
    database.inspect_writer(|db| {
        db.query_row(
            "SELECT count(*) FROM _coven_published_rows WHERE table_name=?1",
            [table],
            |r| r.get(0),
        )
        .unwrap()
    })
}

/// Ana's library holds a hundred counted tracks and one note she edited on
/// this device, not yet counted. A migration of `notes` keeps the counted
/// state of only the rows it changes beside the queue: the note, never the
/// tracks it doesn't touch.
#[tokio::test]
async fn a_migration_keeps_the_counted_state_of_only_the_rows_it_changes() {
    const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT);
         CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, title TEXT)";
    let store = TestStore::new();
    let initial = || vec![Migration::addition(1, "schema", SCHEMA)];
    let tables = || synced(&["notes", "tracks"]);
    let db = store.builder(tables(), initial()).open().await.unwrap();
    sql(
        &db,
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100)
           INSERT INTO tracks SELECT printf('t%03d',i),'Track' FROM n;
         INSERT INTO notes VALUES('n','Title')",
    )
    .await
    .unwrap();
    count_own(&db).await;
    sql(&db, "UPDATE notes SET title='Queued' WHERE id='n'").await.unwrap();
    assert_eq!((published(&db, "notes"), published(&db, "tracks")), (1, 0));
    db.close().await.unwrap();
    let mut migrations = initial();
    migrations.push(Migration::breaking(
        2,
        "name",
        "ALTER TABLE notes RENAME COLUMN title TO name",
        OldWrites::Drop,
    ));
    let db = store.builder(tables(), migrations).open().await.unwrap();
    assert_eq!(published(&db, "tracks"), 0, "the migration kept untouched tracks");
    assert_eq!(published(&db, "notes"), 1);
    db.close().await.unwrap();
}

/// Whether the counted projection a snapshot reads shows `albums` row `id`.
fn counted_visible(database: &crate::Database, id: &str) -> bool {
    let key = coven_format::key::encode_key(&[Value::Text(id.into())]).unwrap();
    database.inspect_writer_schema(|db, schema| {
        let mut visible = None;
        crate::snapshot_view::rows(db, schema, &Audience::Store, |row, synced| {
            if row.state.row().table == "albums" && row.state.row().key == key {
                visible = Some(synced.is_some());
            }
            Ok::<_, DbError>(())
        })
        .unwrap();
        visible.expect("the album has merge state")
    })
}

/// A migration's new rules recompute removals over every row of the tables
/// it changes, and each row it recomputes keeps its counted state first, as a
/// write's rows do, so applying a slot afterwards puts the counted state back.
/// Here `albums.artist` gains a foreign key while Ana's new artist Ben waits
/// in the queue; her counted album keeps its counted artist.
///
/// This can't fail today: for the recompute to change a counted row because
/// of a queued write, the queued write would have to take its parent away,
/// and SQLite then refuses the migration's rebuild of the table; a queued
/// write's unique claim is stamped after every counted one, so it never wins
/// over a counted row; and a CHECK reads only its own row. It guards the
/// capture against a rule that one day lets the queue reach a counted row.
#[tokio::test]
async fn a_migrations_recomputed_rows_keep_their_counted_state() {
    const SCHEMA: &str = "CREATE TABLE artists(id TEXT NOT NULL PRIMARY KEY, name TEXT);
         CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, artist TEXT)";
    const REFERENCE: &str =
        "CREATE TABLE albums_new(id TEXT NOT NULL PRIMARY KEY, artist TEXT REFERENCES artists(id));
         INSERT INTO albums_new SELECT id,artist FROM albums;
         DROP TABLE albums;
         ALTER TABLE albums_new RENAME TO albums";
    let store = TestStore::new();
    let initial = || vec![Migration::addition(1, "schema", SCHEMA)];
    let tables = || synced(&["artists", "albums"]);
    let db = store.builder(tables(), initial()).open().await.unwrap();
    sql(&db, "INSERT INTO artists VALUES('ana','Ana'); INSERT INTO albums VALUES('first','ana')")
        .await
        .unwrap();
    count_own(&db).await;
    sql(&db, "INSERT INTO artists VALUES('ben','Ben')").await.unwrap();
    db.close().await.unwrap();
    let mut migrations = initial();
    migrations.push(Migration::breaking(2, "reference", REFERENCE, OldWrites::Drop));
    let db = store.builder(tables(), migrations).open().await.unwrap();
    assert!(counted_visible(&db, "first"));
    crate::tests::remote_update(&db, "artists", "ana", &[("name", Value::Text("Ana B".into()))])
        .await;
    assert!(counted_visible(&db, "first"));
    let shown: Vec<String> =
        db.inspect_writer(|db| db.query("SELECT id FROM albums", [], |r| r.get(0)).unwrap());
    assert_eq!(shown, ["first"]);
    db.close().await.unwrap();
}
