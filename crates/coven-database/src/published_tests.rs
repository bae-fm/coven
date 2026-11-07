//! Test support for the published merge state's tables: filling them for the
//! plan audit, and the full scans their statements' plans may contain.

/// Fill the published merge state's tables, thousands of rows each but one marker,
/// from the rows of the plan audit's `fixture` table, inside its transaction
/// (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_published VALUES(1);
         INSERT INTO _coven_published_rows SELECT tab,key,audience,x'00' FROM fixture;
         INSERT INTO _coven_published_losses SELECT i,tab,key,audience,x'00' FROM fixture;
         INSERT INTO _coven_published_creations SELECT tab,key,audience,CAST(device || n AS BLOB),x'00' FROM fixture;
         INSERT INTO _coven_published_dropped SELECT tab,key,audience FROM fixture;
         INSERT INTO _coven_published_file_refs SELECT tab,key,audience,device,substr(key,17,16) FROM fixture;",
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
("published.rs", "SELECT table_name,key,audience FROM _coven_published_rows UNION SELECT table_name,key,audience FROM _coven_published_losses UNION SELECT table_name,key,audience FROM _coven_published_dropped", "_coven_published_rows" | "_coven_published_losses" | "_coven_published_dropped") =>
    Some("restoration visits every saved row identity"),
("published.rs", "SELECT table_name,key,audience FROM _coven_published_rows", "_coven_published_rows") =>
    Some("the counted removal view touches every kept row"),
("published.rs", "SELECT DISTINCT lower(table_name) FROM _coven_columns WHERE synced=1", "_coven_columns") =>
    Some("once per migration while rows are kept: discover every synced table to read its rules"),
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

/// Ana wrote 1,000 notes on this device while it was offline, and none is
/// counted yet. Then 40 slots from Ben's device arrive, each editing a
/// counted note of his that none of hers touches. Applying a slot puts back
/// and keeps aside again only the rows it reaches, so none of her 1,000.
#[tokio::test]
async fn slots_beside_unsent_rows_put_back_only_the_rows_they_reach() {
    const UNSENT: i64 = 1_000;
    const SLOTS: usize = 40;
    let store = TestStore::new();
    let migrations = vec![Migration::addition(
        1,
        "notes",
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT)",
    )];
    let db = store.builder(synced(&["notes"]), migrations).open().await.unwrap();
    sql(
        &db,
        "WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<39)
           INSERT INTO notes SELECT 'counted '||i,'Ben' FROM n",
    )
    .await
    .unwrap();
    count_own(&db).await;
    sql(
        &db,
        "WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<999)
           INSERT INTO notes SELECT 'unsent '||i,'Ana' FROM n",
    )
    .await
    .unwrap();
    assert_eq!(published(&db, "notes"), UNSENT);
    let counter = crate::test_utils::WorkCounter::new();
    db.test_track_work(counter.clone()).await.unwrap();
    for slot in 0..SLOTS {
        let title = [("title", Value::Text(format!("edit {slot}")))];
        crate::tests::remote_update_beside_queue(&db, "notes", &format!("counted {slot}"), &title)
            .await;
    }
    let rebuilt = counter.take().published_rows;
    assert_eq!(published(&db, "notes"), UNSENT);
    let titles: Vec<(String, String)> = db
        .read(|sql| {
            Ok::<_, DbError>(sql.query(
                "SELECT id,title FROM notes WHERE id IN ('counted 0','counted 39','unsent 0','unsent 999') ORDER BY id",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        titles,
        [
            ("counted 0".into(), "edit 0".into()),
            ("counted 39".into(), "edit 39".into()),
            ("unsent 0".into(), "Ana".into()),
            ("unsent 999".into(), "Ana".into()),
        ]
    );
    assert_eq!(rebuilt, 0);
    db.close().await.unwrap();
}

/// Whether the counted projection a snapshot reads shows `albums` row `id`.
fn counted_visible(database: &crate::Database, id: &str) -> bool {
    let key = crate::tests::text_key(id);
    database.inspect_writer_schema(|db, schema| {
        let mut visible = None;
        let removed = crate::published::removals(db, &schema.schema, &schema.rules).unwrap();
        crate::snapshot_view::rows(db, schema, &removed, &Audience::Store, |row, synced| {
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

/// The counted projection a snapshot reads of `albums` row `id`: its
/// `artist`, or `None` while it shows no such row.
fn counted_artist(database: &crate::Database, id: &str) -> Option<Value> {
    let key = crate::tests::text_key(id);
    database.inspect_writer_schema(|db, schema| {
        let mut shown = None;
        let removed = crate::published::removals(db, &schema.schema, &schema.rules).unwrap();
        crate::snapshot_view::rows(db, schema, &removed, &Audience::Store, |row, synced| {
            if row.state.row().table == "albums" && row.state.row().key == key {
                shown = Some(synced.map(|synced| synced.columns["artist"].value.clone()));
            }
            Ok::<_, DbError>(())
        })
        .unwrap();
        shown.expect("the album has merge state")
    })
}

/// Open a store whose counted album `first` is by counted artist Ana, then
/// run `migration` as version 2 over it.
async fn migrate_counted_album(
    schema: &'static str,
    migration: &'static str,
) -> (TestStore, crate::Database) {
    let store = TestStore::new();
    let initial = || vec![Migration::addition(1, "schema", schema)];
    let tables = || synced(&["artists", "albums"]);
    let db = store.builder(tables(), initial()).open().await.unwrap();
    sql(&db, "INSERT INTO artists VALUES('ana','Ana'); INSERT INTO albums VALUES('first','ana')")
        .await
        .unwrap();
    count_own(&db).await;
    db.close().await.unwrap();
    let mut migrations = initial();
    migrations.push(Migration::breaking(2, "parents", migration, OldWrites::Drop));
    let db = store.builder(tables(), migrations).open().await.unwrap();
    assert_eq!(published(&db, "albums"), 1, "the migration kept the album aside");
    (store, db)
}

/// Apply a slot from another device that renames counted artist Ana, ahead
/// of the migration's write still in the queue.
async fn rename_ana(database: &crate::Database) {
    let name = [("name", Value::Text("Ana B".into()))];
    crate::tests::remote_update_beside_queue(database, "artists", "ana", &name).await;
    let waiting = database.inspect_writer(|db| crate::queued_writes::present(db).unwrap());
    assert!(waiting, "the migration's write still waits for its slot");
}

/// A migration that deletes a parent row deletes its children too, through
/// their foreign key's ON DELETE CASCADE. Those children keep their counted
/// state beside the queue as the parent does: until the migration's write is
/// counted, a snapshot shows Ana's album as the slots left it (§18.1).
#[tokio::test]
async fn a_migrations_cascading_delete_keeps_the_childrens_counted_state() {
    let (_store, db) = migrate_counted_album(
        "CREATE TABLE artists(id TEXT NOT NULL PRIMARY KEY, name TEXT);
         CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY,
           artist TEXT REFERENCES artists(id) ON DELETE CASCADE)",
        "DELETE FROM artists WHERE id='ana'",
    )
    .await;
    let shown: Vec<String> =
        db.inspect_writer(|db| db.query("SELECT id FROM albums", [], |r| r.get(0)).unwrap());
    assert!(shown.is_empty(), "the cascade deleted the album");
    assert_eq!(counted_artist(&db, "first"), Some(Value::Text("ana".into())));
    // A slot puts the counted state back, applies over it, and folds the
    // migration's write over the result again.
    rename_ana(&db).await;
    assert_eq!(counted_artist(&db, "first"), Some(Value::Text("ana".into())));
    let shown: Vec<String> =
        db.inspect_writer(|db| db.query("SELECT id FROM albums", [], |r| r.get(0)).unwrap());
    assert!(shown.is_empty(), "the migration's write deletes the album again");
    db.close().await.unwrap();
}

/// A migration that rebuilds a parent table drops the old one, and SQLite
/// deletes its rows first, setting each child's reference to NULL through
/// ON DELETE SET NULL. The children keep their counted state as the parent
/// does (§18.1).
#[tokio::test]
async fn a_migrations_dropped_parent_keeps_the_childrens_counted_state() {
    let (_store, db) = migrate_counted_album(
        "CREATE TABLE artists(id TEXT NOT NULL PRIMARY KEY, name TEXT);
         CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY,
           artist TEXT REFERENCES artists(id) ON DELETE SET NULL)",
        "CREATE TABLE artists_new(id TEXT NOT NULL PRIMARY KEY, name TEXT, born INTEGER);
         INSERT INTO artists_new SELECT id,name,NULL FROM artists;
         DROP TABLE artists;
         ALTER TABLE artists_new RENAME TO artists",
    )
    .await;
    let artist: Option<String> = db.inspect_writer(|db| {
        db.query_row("SELECT artist FROM albums WHERE id='first'", [], |r| r.get(0)).unwrap()
    });
    assert_eq!(artist, None, "dropping the parent table set the reference to NULL");
    assert_eq!(counted_artist(&db, "first"), Some(Value::Text("ana".into())));
    rename_ana(&db).await;
    assert_eq!(counted_artist(&db, "first"), Some(Value::Text("ana".into())));
    let artist: Option<String> = db.inspect_writer(|db| {
        db.query_row("SELECT artist FROM albums WHERE id='first'", [], |r| r.get(0)).unwrap()
    });
    assert_eq!(artist, None, "the migration's write sets the reference to NULL again");
    db.close().await.unwrap();
}

/// Ana's counted album `first` holds a count another device set below the
/// CHECK that version 1 declares, so the slots take it out (§9.3), while her
/// own edit of its title waits in the queue and keeps its counted state
/// aside. Version 2 drops that CHECK. The counted projection is the slots'
/// state under the schema the database has now, so a snapshot shows the
/// album again: whether a kept row shows is worked out under the current
/// rules, never kept from the rules it was captured under.
#[tokio::test]
async fn a_kept_row_shows_under_the_rules_a_migration_leaves() {
    const SCHEMA: &str = "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, title TEXT,
           count INTEGER CHECK(count > 0))";
    const UNCHECKED: &str =
        "CREATE TABLE albums_new(id TEXT NOT NULL PRIMARY KEY, title TEXT, count INTEGER);
         INSERT INTO albums_new SELECT id,title,count FROM albums;
         DROP TABLE albums;
         ALTER TABLE albums_new RENAME TO albums";
    let store = TestStore::new();
    let initial = || vec![Migration::addition(1, "schema", SCHEMA)];
    let tables = || synced(&["albums"]);
    let db = store.builder(tables(), initial()).open().await.unwrap();
    sql(&db, "INSERT INTO albums VALUES('first','First',1)").await.unwrap();
    count_own(&db).await;
    sql(&db, "UPDATE albums SET title='Queued' WHERE id='first'").await.unwrap();
    crate::tests::remote_update_beside_queue(
        &db,
        "albums",
        "first",
        &[("count", Value::Integer(-1))],
    )
    .await;
    assert!(!counted_visible(&db, "first"), "the CHECK takes the counted album out");
    db.close().await.unwrap();
    let mut migrations = initial();
    migrations.push(Migration::breaking(2, "unchecked", UNCHECKED, OldWrites::Drop));
    let db = store.builder(tables(), migrations).open().await.unwrap();
    assert_eq!(published(&db, "albums"), 1, "the album is still kept aside");
    assert!(counted_visible(&db, "first"), "no rule takes the counted album out now");
    db.close().await.unwrap();
}

/// The counted losses a snapshot of the store reads.
fn counted_losses(database: &crate::Database) -> Vec<coven_format::loss::Loss> {
    database.inspect_writer_schema(|db, schema| {
        let removed = crate::published::removals(db, &schema.schema, &schema.rules).unwrap();
        let mut losses = Vec::new();
        crate::snapshot_view::losses(db, &removed, &Audience::Store, |loss| {
            losses.push(loss);
            Ok::<_, DbError>(())
        })
        .unwrap();
        losses
    })
}

/// The `title` a whole-row loss holds.
fn lost_title(loss: &coven_format::loss::Loss) -> &Value {
    match &loss.values {
        coven_format::loss::LossValues::Row(cells) => &cells["title"].value.value,
        values => panic!("a whole-row loss, not {values:?}"),
    }
}

/// The losses the app shows.
fn app_losses(database: &crate::Database) -> Vec<coven_format::loss::Loss> {
    database.inspect_writer(|db| {
        let mut losses = Vec::new();
        crate::loss_record::visit(db, &Audience::Store, |loss| {
            losses.push(loss);
            Ok::<_, DbError>(())
        })
        .unwrap();
        losses
    })
}

/// The `title` of the one frozen whole-row loss `losses` holds, taken out by
/// the CHECK `count > 0`.
fn frozen_title(losses: &[coven_format::loss::Loss]) -> &Value {
    let [loss] = losses else { panic!("one frozen loss, not {losses:?}") };
    assert!(loss.frozen);
    assert!(matches!(&loss.cause, coven_format::loss::LossCause::Rules(rules)
        if *rules == [coven_merge::Rule::Check("count > 0".into())].into()));
    lost_title(loss)
}

/// Ana's counted album `first` is taken out by its CHECK while her queued
/// title edit keeps it aside, and a slot shows it as the counted removal it
/// is. Then version 2 drops `albums`. A dropped table keeps its rows'
/// removal losses frozen (§18.7). The counted state's is the migration over
/// the slots so far: the counted title. The app's is the migration over her
/// whole queue, which her migration write records: her queued title. Each
/// state holds one, and once her edit and her migration write count, the
/// counted state holds the migration write's, as the raise's snapshot does
/// (§18.5).
#[tokio::test]
async fn a_dropped_table_freezes_a_kept_rows_counted_removal() {
    const SCHEMA: &str = "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, title TEXT,
           count INTEGER CHECK(count > 0)); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT)";
    let store = TestStore::new();
    let initial = || vec![Migration::addition(1, "schema", SCHEMA)];
    let db = store.builder(synced(&["albums", "notes"]), initial()).open().await.unwrap();
    sql(&db, "INSERT INTO albums VALUES('first','First',1); INSERT INTO notes VALUES('n','Note')")
        .await
        .unwrap();
    count_own(&db).await;
    sql(&db, "UPDATE albums SET title='Queued' WHERE id='first'").await.unwrap();
    crate::tests::remote_update_beside_queue(
        &db,
        "albums",
        "first",
        &[("count", Value::Integer(-1))],
    )
    .await;
    let [counted] = &counted_losses(&db)[..] else { panic!("one counted removal loss") };
    assert!(!counted.frozen);
    assert_eq!(lost_title(counted), &Value::Text("First".into()));
    db.close().await.unwrap();
    let mut migrations = initial();
    migrations.push(Migration::breaking(2, "drop", "DROP TABLE albums", OldWrites::Drop));
    let db = store.builder(synced(&["notes"]), migrations).open().await.unwrap();
    let first = Value::Text("First".into());
    let queued = Value::Text("Queued".into());
    assert_eq!(frozen_title(&counted_losses(&db)), &first);
    assert_eq!(frozen_title(&app_losses(&db)), &queued);
    // The next slot puts the counted state back and folds the queue again:
    // the migration write's freeze replaces the counted one in the app.
    let note = [("title", Value::Text("Ben's note".into()))];
    crate::tests::remote_update_beside_queue(&db, "notes", "n", &note).await;
    assert_eq!(frozen_title(&counted_losses(&db)), &first);
    assert_eq!(frozen_title(&app_losses(&db)), &queued);
    // Her edit and her migration write count.
    count_own(&db).await;
    assert_eq!(frozen_title(&counted_losses(&db)), &queued);
    assert_eq!(frozen_title(&app_losses(&db)), &queued);
    db.close().await.unwrap();
}

/// Ana's album `first` passes its CHECK `a > 0 OR b > 0` through `b`. She
/// sets `a` on this device, and Ben clears `b`: the slots alone take the
/// album out, while her queued edit keeps it showing in the app. Version 2
/// drops `albums`. Freezing the kept state freezes the counted removal; the
/// app's album has none to freeze, so her migration write puts no lost value
/// of it, and removes the counted one once it counts (§18.7). The write's
/// slot changes the album's lost values, so it reaches the album: once her
/// queue is empty, nothing kept is left to put the counted removal back.
#[tokio::test]
async fn a_migration_write_reaches_the_counted_removal_it_replaces_with_none() {
    const SCHEMA: &str = "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, a INTEGER, b INTEGER,
           CHECK(a > 0 OR b > 0)); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT)";
    let store = TestStore::new();
    let initial = || vec![Migration::addition(1, "schema", SCHEMA)];
    let db = store.builder(synced(&["albums", "notes"]), initial()).open().await.unwrap();
    sql(&db, "INSERT INTO albums VALUES('first',0,1); INSERT INTO notes VALUES('n','Note')")
        .await
        .unwrap();
    count_own(&db).await;
    sql(&db, "UPDATE albums SET a=1 WHERE id='first'").await.unwrap();
    crate::tests::remote_update_beside_queue(&db, "albums", "first", &[("b", Value::Integer(0))])
        .await;
    let [counted] = &counted_losses(&db)[..] else { panic!("one counted removal loss") };
    assert!(!counted.frozen);
    assert!(app_losses(&db).is_empty(), "{:?}", app_losses(&db));
    db.close().await.unwrap();
    let mut migrations = initial();
    migrations.push(Migration::breaking(2, "drop", "DROP TABLE albums", OldWrites::Drop));
    let db = store.builder(synced(&["notes"]), migrations).open().await.unwrap();
    let [counted] = &counted_losses(&db)[..] else { panic!("one counted removal loss") };
    assert!(counted.frozen);
    assert!(app_losses(&db).is_empty(), "{:?}", app_losses(&db));
    // Her edit and her migration write count.
    count_own(&db).await;
    assert!(counted_losses(&db).is_empty(), "{:?}", counted_losses(&db));
    assert!(app_losses(&db).is_empty(), "{:?}", app_losses(&db));
    db.close().await.unwrap();
}

/// Ana renames her counted album `first` on this device while Ben, who
/// hasn't seen that, renames it too. Folding her unsent rename over Ben's
/// leaves her title as a lost value the app shows, but the slots alone hold
/// only Ben's rename, so the counted projection has no loss. Version 2 drops
/// `albums`, freezing the app's lost value with the table (§18.7); the
/// counted projection still has none, before the next slot and after it.
#[tokio::test]
async fn a_dropped_table_keeps_a_kept_row_without_the_queues_losses() {
    const SCHEMA: &str = "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, title TEXT);
         CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT)";
    let store = TestStore::new();
    let initial = || vec![Migration::addition(1, "schema", SCHEMA)];
    let db = store.builder(synced(&["albums", "notes"]), initial()).open().await.unwrap();
    sql(&db, "INSERT INTO albums VALUES('first','First'); INSERT INTO notes VALUES('n','Note')")
        .await
        .unwrap();
    count_own(&db).await;
    sql(&db, "UPDATE albums SET title='Queued' WHERE id='first'").await.unwrap();
    crate::tests::remote_update_concurrent_with_queue(
        &db,
        "albums",
        "first",
        &[("title", Value::Text("Ben".into()))],
    )
    .await;
    assert_eq!(db.lost_values().await.unwrap().len(), 1, "the app shows Ana's title as lost");
    assert!(counted_losses(&db).is_empty());
    db.close().await.unwrap();
    let mut migrations = initial();
    migrations.push(Migration::breaking(2, "drop", "DROP TABLE albums", OldWrites::Drop));
    let db = store.builder(synced(&["notes"]), migrations).open().await.unwrap();
    assert_eq!(db.lost_values().await.unwrap().len(), 1, "the app keeps it frozen");
    assert!(counted_losses(&db).is_empty(), "{:?}", counted_losses(&db));
    // The next slot puts the counted state back and folds the queue again,
    // its migration's frozen loss included; the counted state still keeps
    // none.
    let note = [("title", Value::Text("Ben's note".into()))];
    crate::tests::remote_update_beside_queue(&db, "notes", "n", &note).await;
    assert_eq!(db.lost_values().await.unwrap().len(), 1);
    assert!(counted_losses(&db).is_empty(), "{:?}", counted_losses(&db));
    db.close().await.unwrap();
}

/// Another device's insert of `albums` row `id`, titled `title`, at
/// generation 1.
fn album_insert(id: &str, title: &str) -> coven_format::write::RowChange {
    use coven_merge::{Change, ColumnValue, Operation};
    let value =
        |value: &str| ColumnValue { value: Value::Text(value.into()), parents: Default::default() };
    coven_format::write::RowChange {
        row: crate::tests::store_row("albums", id),
        change: Change {
            generation: 0,
            operation: Operation::Insert(
                [("id".to_owned(), value(id)), ("title".to_owned(), value(title))].into(),
            ),
        },
        old: Default::default(),
        sources: Default::default(),
        creator: None,
        execution: Default::default(),
    }
}

/// Ana's queued rename of `first`, folded over Ben's concurrent one, keeps
/// it aside with her title as a lost value when version 2 drops `albums`;
/// version 3 makes a new `albums`, and Ben's device inserts its own `first`
/// there while her writes still wait. Kept rows go by table name and key, so
/// her dropped row's frozen lost value names Ben's new row too, and folding
/// her queue keeps that row aside: what is kept is the state the slots gave
/// it, so the next slot puts Ben's row back as it was, and the counted state
/// holds no lost value of it.
#[tokio::test]
async fn a_recreated_tables_row_keeps_its_counted_state_under_a_dropped_rows_name() {
    const SCHEMA: &str = "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, title TEXT);
         CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT)";
    let store = TestStore::new();
    let mut migrations = vec![Migration::addition(1, "schema", SCHEMA)];
    let db = store.builder(synced(&["albums", "notes"]), migrations.clone()).open().await.unwrap();
    sql(&db, "INSERT INTO albums VALUES('first','First'); INSERT INTO notes VALUES('n','Note')")
        .await
        .unwrap();
    count_own(&db).await;
    sql(&db, "UPDATE albums SET title='Queued' WHERE id='first'").await.unwrap();
    let rename = [("title", Value::Text("Ben".into()))];
    crate::tests::remote_update_concurrent_with_queue(&db, "albums", "first", &rename).await;
    db.close().await.unwrap();
    migrations.push(Migration::breaking(2, "drop", "DROP TABLE albums", OldWrites::Drop));
    let db = store.builder(synced(&["notes"]), migrations.clone()).open().await.unwrap();
    db.close().await.unwrap();
    migrations.push(Migration::addition(
        3,
        "albums again",
        "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, title TEXT)",
    ));
    let db = store.builder(synced(&["albums", "notes"]), migrations).open().await.unwrap();
    crate::tests::remote_write_beside_queue(&db, album_insert("first", "Ben's")).await;
    let shown = |db: &crate::Database| -> Vec<(String, String)> {
        db.inspect_writer(|db| {
            db.query("SELECT id,title FROM albums", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
        })
    };
    assert_eq!(shown(&db), [("first".to_owned(), "Ben's".to_owned())]);
    let note = [("title", Value::Text("Ben's note".into()))];
    crate::tests::remote_update_beside_queue(&db, "notes", "n", &note).await;
    assert_eq!(shown(&db), [("first".to_owned(), "Ben's".to_owned())]);
    assert!(counted_visible(&db, "first"));
    assert!(counted_losses(&db).is_empty(), "{:?}", counted_losses(&db));
    assert_eq!(db.lost_values().await.unwrap().len(), 1, "the app keeps Ana's title frozen");
    db.close().await.unwrap();
}

/// The notes the app shows, by id, with their titles.
fn note_titles(database: &crate::Database) -> Vec<(String, String)> {
    database.inspect_writer(|db| {
        db.query("SELECT id,title FROM notes ORDER BY id", [], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
    })
}

/// Ana's queued rename of note `n`, folded over Ben's concurrent one, leaves
/// a lost value, and she dismisses it in a second unsent write that changes
/// no row of its own. Ben's next slot edits the note's body: it reaches the
/// note, so both her writes fold again, the dismissal with them, and the
/// lost value stays gone (§6.2).
#[tokio::test]
async fn a_slot_reaching_a_dismissed_row_keeps_the_queued_dismissal() {
    let store = TestStore::new();
    let migrations = vec![Migration::addition(
        1,
        "notes",
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT)",
    )];
    let db = store.builder(synced(&["notes"]), migrations).open().await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','Note','')").await.unwrap();
    count_own(&db).await;
    sql(&db, "UPDATE notes SET title='Ana' WHERE id='n'").await.unwrap();
    let ben = [("title", Value::Text("Ben".into()))];
    crate::tests::remote_update_concurrent_with_queue(&db, "notes", "n", &ben).await;
    let lost = db.lost_values().await.unwrap();
    assert_eq!(lost.len(), 1, "{lost:?}");
    db.dismiss_lost_values(&lost).await.unwrap();
    assert!(db.lost_values().await.unwrap().is_empty());
    let shown = note_titles(&db);
    let body = [("body", Value::Text("Ben's body".into()))];
    crate::tests::remote_update_beside_queue(&db, "notes", "n", &body).await;
    assert!(db.lost_values().await.unwrap().is_empty(), "{:?}", db.lost_values().await);
    assert_eq!(note_titles(&db), shown);
    db.close().await.unwrap();
}

/// Ana's device writes note `x` under her member key, and an admin slot
/// replaces that key before the write gets a slot: the write still shows,
/// folded over the slots, until its own slot comes back and is refused as
/// signed with the old key (§14.1). Her later rename of another note still
/// waits, so the queue stays; the refused slot reaches note `x` and takes
/// her write off it.
#[tokio::test]
async fn a_refused_queued_write_comes_off_its_rows() {
    let store = TestStore::new();
    let migrations = vec![Migration::addition(
        1,
        "notes",
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT)",
    )];
    let db = store.builder(synced(&["notes"]), migrations).open().await.unwrap();
    TestStore::register(&db, store.device(), 1);
    sql(&db, "INSERT INTO notes VALUES('counted','Counted')").await.unwrap();
    count_own(&db).await;
    sql(&db, "INSERT INTO notes VALUES('x','Ana')").await.unwrap();
    let ana = crate::tests::member(1);
    let mut state = db.store_state().await.unwrap();
    state.members.get_mut(&ana).unwrap().keys = crate::tests::keys(8);
    db.admin_fixture(
        coven_format::admin::StoreChange::ReplaceMemberKeys {
            member: ana,
            expected: coven_crypto::MemberPublicKeys {
                signing: crate::test_utils::signer(),
                sealing: crate::tests::keys(1).sealing,
            },
            keys: crate::tests::keys(8),
        },
        state,
        None,
    )
    .await
    .unwrap();
    let queued = records(&db).pop().unwrap();
    sql(&db, "UPDATE notes SET title='Later' WHERE id='counted'").await.unwrap();
    assert_eq!(
        note_titles(&db),
        [("counted".to_owned(), "Later".to_owned()), ("x".to_owned(), "Ana".to_owned())]
    );
    db.receive(queued.into()).await.unwrap();
    let refused: bool = db.inspect_writer(|db| {
        db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_write_refusals)", [], |r| r.get(0))
            .unwrap()
    });
    assert!(refused, "the write's slot is refused");
    assert_eq!(note_titles(&db), [("counted".to_owned(), "Later".to_owned())]);
    db.close().await.unwrap();
}

/// Another device's insert of `artists` row `id` at generation 1.
fn artist_insert(id: &str) -> coven_format::write::RowChange {
    use coven_merge::{Change, ColumnValue, Operation};
    let value = ColumnValue { value: Value::Text(id.into()), parents: Default::default() };
    coven_format::write::RowChange {
        row: crate::tests::store_row("artists", id),
        change: Change {
            generation: 0,
            operation: Operation::Insert([("id".to_owned(), value)].into()),
        },
        old: Default::default(),
        sources: Default::default(),
        creator: None,
        execution: Default::default(),
    }
}

/// Ana's unsent writes create the artist `prince` with an album, then delete
/// the artist, the delete carrying to the album through ON DELETE CASCADE.
/// Ben's slot created `prince` first, which makes him its owner: her insert
/// and delete of the artist are refused, and the album's delete with them,
/// though his slot never touched the album. Both her writes change the
/// artist, so the slot reaches them and their album (§6.2).
#[tokio::test]
async fn a_slot_reaches_every_row_of_the_queued_writes_it_reaches() {
    const SCHEMA: &str = "CREATE TABLE artists(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY,
           artist TEXT REFERENCES artists(id) ON DELETE CASCADE)";
    let store = TestStore::new();
    let tables = vec![
        SyncedTable::new("artists", RowIdentity::SharedKey).owner_only(),
        SyncedTable::new("albums", RowIdentity::SharedKey),
    ];
    let migrations = vec![Migration::addition(1, "schema", SCHEMA)];
    let db = store.builder(tables, migrations).open().await.unwrap();
    TestStore::register(&db, store.device(), 1);
    sql(&db, "INSERT INTO artists VALUES('ana')").await.unwrap();
    count_own(&db).await;
    sql(&db, "INSERT INTO artists VALUES('prince'); INSERT INTO albums VALUES('purple','prince')")
        .await
        .unwrap();
    sql(&db, "DELETE FROM artists WHERE id='prince'").await.unwrap();
    let albums = |db: &crate::Database| -> Vec<String> {
        db.inspect_writer(|db| {
            db.query("SELECT id FROM albums ORDER BY id", [], |r| r.get(0)).unwrap()
        })
    };
    assert!(albums(&db).is_empty());
    let ben = coven_foundation::id_source::DeviceId(10_000 + db.applied_slot().await.unwrap());
    TestStore::register(&db, ben, 2);
    crate::tests::remote_write_beside_queue(&db, artist_insert("prince")).await;
    assert_eq!(albums(&db), ["purple"], "Ana's delete of Ben's artist is refused");
    db.close().await.unwrap();
}
