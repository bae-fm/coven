use super::*;
use crate::snapshot_write::tests::{frames, load_one, stream};
use crate::test_utils::tests::{insert, SCHEMA};
use crate::tests::TestStore;
use crate::write::tests::sql;
use crate::{Database, RowIdentity, SyncedTable};

fn owned() -> Vec<SyncedTable> {
    vec![SyncedTable::new("rows", RowIdentity::SharedKey).owner_only()]
}

fn row(n: usize) -> RowId {
    crate::tests::store_row("rows", &n.to_string())
}

fn selected(db: &Database, row: &RowId) -> Option<Creation> {
    db.inspect_writer(|c| creation(c, row).unwrap())
}

fn own(write: WriteId, row: RowId) -> Creation {
    Creation { write, creator: ChangeId { write, row } }
}

/// Two devices insert the same shared key concurrently. Whatever arrives
/// first in the store sequence is the creation (§9.6, Lean Owners).
#[tokio::test]
async fn the_first_slot_keeps_the_creation_regardless_of_timestamps() {
    let earliest = insert(0, 0).header.position;
    let later = insert(1, 0).header.position;
    for order in [[0, 1], [1, 0]] {
        let store = TestStore::new();
        let db = store.schema(owned(), SCHEMA).await.unwrap();
        // Two members' devices, so the earlier creation decides the owner.
        TestStore::register(&db, earliest.device, 1);
        TestStore::register(&db, later.device, 2);
        db.receive(insert(order[0], 0).into()).await.unwrap();
        // Alone, the first arrival is the creation.
        let first = if order[0] == 0 { earliest } else { later };
        assert_eq!(selected(&db, &row(0)), Some(own(first, row(0))));
        db.receive(insert(order[1], 0).into()).await.unwrap();
        assert_eq!(selected(&db, &row(0)), Some(own(first, row(0))), "{order:?}");
        db.close().await.unwrap();
    }
}

/// Updates, deletes and reinserts keep the original creation: none of them
/// is a generation-zero insert (§9.6).
#[tokio::test]
async fn later_generations_keep_the_original_creation() {
    let store = TestStore::new();
    let db = store.schema(owned(), SCHEMA).await.unwrap();
    // The creating device is another of this device's member's, who may
    // edit the row (§9.6).
    let creator = insert(0, 0);
    TestStore::register(&db, store.device(), 1);
    TestStore::register(&db, creator.header.position.device, 1);
    db.receive(creator.into()).await.unwrap();
    let original = selected(&db, &row(0));
    assert!(original.is_some());
    sql(&db, "UPDATE rows SET a='changed' WHERE id='0'").await.unwrap();
    sql(&db, "DELETE FROM rows WHERE id='0'").await.unwrap();
    sql(&db, "INSERT INTO rows VALUES('0','again','b')").await.unwrap();
    assert_eq!(selected(&db, &row(0)), original);
    // A new row is created by this device's own write.
    sql(&db, "INSERT INTO rows VALUES('5','a','b')").await.unwrap();
    let local = crate::write::tests::records(&db).pop().unwrap().header.position;
    assert_eq!(selected(&db, &row(5)), Some(own(local, row(5))));
    db.close().await.unwrap();
}

/// Changing whether an existing table is owner-only is a breaking schema
/// change: the database refuses to open with the other declaration.
#[tokio::test]
async fn an_owner_declaration_change_refuses_to_open() {
    let store = TestStore::new();
    let db = store.schema(owned(), SCHEMA).await.unwrap();
    db.close().await.unwrap();
    let error = store
        .schema(crate::test_utils::tests::tables(), SCHEMA)
        .await
        .err()
        .expect("a changed declaration refuses");
    assert!(
        matches!(
            crate::tests::database_error(error),
            DbError::Schema(crate::SchemaError::OwnerOnlyChanged { table }) if table == "rows"
        ),
        "a changed owner declaration"
    );
    // Reopening with the declaration it was created with still works.
    store.schema(owned(), SCHEMA).await.unwrap().close().await.unwrap();
}

/// A loaded snapshot supplies each row's creation as its source selected it,
/// with the creating write's evidence.
#[tokio::test]
async fn snapshots_carry_creations_through_reload() {
    let source_store = TestStore::new();
    let source = source_store.schema(owned(), SCHEMA).await.unwrap();
    for n in [1, 0] {
        TestStore::register(&source, insert(n, 0).header.position.device, n as u8 + 1);
    }
    // Streams keep each write's evidence, which the snapshot carries.
    for n in [1, 0] {
        source.receive_stream(stream(&insert(n, 0)), || Ok(())).await.unwrap();
    }
    let snapshot = frames(&source, Audience::Store).await;
    let target_store = TestStore::new();
    let target = target_store.schema(owned(), SCHEMA).await.unwrap();
    let (id, prefix, input, _) = snapshot.input();
    load_one(&target, id, prefix, input).await.unwrap();
    assert_eq!(selected(&target, &row(0)), selected(&source, &row(0)));
    assert_eq!(selected(&target, &row(0)), Some(own(insert(1, 0).header.position, row(0))));
    source.close().await.unwrap();
    target.close().await.unwrap();
}

/// Renaming the table keeps the creating write, and its original names:
/// the migration's author does not become the creator (§9.6, §18.4).
#[tokio::test]
async fn a_table_rename_keeps_the_original_creation() {
    let store = TestStore::new();
    let db = store.schema(owned(), SCHEMA).await.unwrap();
    db.receive_stream(stream(&insert(0, 0)), || Ok(())).await.unwrap();
    let original = selected(&db, &row(0)).unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(
            vec![SyncedTable::new("items", RowIdentity::SharedKey).owner_only()],
            vec![
                crate::Migration::addition(1, "schema", SCHEMA),
                crate::Migration::breaking(
                    2,
                    "rename",
                    "ALTER TABLE rows RENAME TO items",
                    crate::OldWrites::Drop,
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    let renamed = RowId { table: "items".into(), ..row(0) };
    assert_eq!(selected(&db, &renamed), Some(original));
    assert_eq!(selected(&db, &row(0)), None);
    db.close().await.unwrap();
}

/// A creation still in the local queue is provisional. When a published
/// creation already owns the key, the queued changes are checked against that
/// owner: this device's update to the row is refused, recorded as a lost value
/// naming the owner, and the row shows the owner's values (§9.6).
#[tokio::test]
async fn a_queued_creation_yields_to_a_published_owner() {
    let store = TestStore::new();
    let db = store.schema(owned(), SCHEMA).await.unwrap();
    let theirs = insert(1, 0);
    TestStore::register(&db, store.device(), 1);
    TestStore::register(&db, theirs.header.position.device, 2);
    sql(&db, "INSERT INTO rows VALUES('0','mine','b')").await.unwrap();
    sql(&db, "UPDATE rows SET a='changed' WHERE id='0'").await.unwrap();
    assert_eq!(crate::write::tests::records(&db).len(), 2);

    db.receive(theirs.clone().into()).await.unwrap();
    assert_eq!(selected(&db, &row(0)), Some(own(theirs.header.position, row(0))));
    let shown: (String, String) = db.inspect_writer(|c| {
        c.query_row("SELECT a,b FROM rows WHERE id='0'", [], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
    });
    assert_eq!(shown, ("a1".into(), "b1".into()));
    let lost = db.lost_values().await.unwrap();
    assert!(
        lost.iter().any(|value| matches!(
            &value.replaced_by,
            crate::Replacement::NotAuthorized {
                reason: crate::RowAuthorizationFailure::NotOwner { owner },
                ..
            } if *owner == crate::tests::member(2)
        )),
        "{lost:?}"
    );
    db.close().await.unwrap();
}

/// An internal write of a creation, for tests that run one beside the app's.
pub(crate) const CREATED: &str =
    "INSERT INTO _coven_creations VALUES('notes',x'01','store',zeroblob(16),x'00')";

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_creations SELECT tab,key,audience,CAST(device || n AS BLOB),x'00' FROM fixture;",
    )
    .unwrap();
}

/// A local write's rows offer their creation in one statement that keeps an
/// earlier creation, and retain their source records without reading for a
/// repeat: the write's number is new, so none exists.
#[tokio::test]
async fn a_new_write_offers_creations_and_retains_sources_without_reading() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT)",
        )
        .await
        .unwrap();
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES('a','one'),('b','one')").await.unwrap();
    let creations = |db: &Database| {
        db.inspect_writer(|writer| {
            writer.query("SELECT key,write_id FROM _coven_creations ORDER BY key", [], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
        })
        .unwrap()
    };
    let first = creations(&db);
    assert_eq!(first.len(), 2);
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES('c','two')").await.unwrap();
    crate::write::tests::sql(&db, "UPDATE notes SET title='three'").await.unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let runs = |text: &str| statements.iter().filter(|s| s.sql.contains(text)).count();
    assert_eq!(runs("SELECT write_id,creator FROM _coven_creations"), 0);
    assert_eq!(runs("SELECT (SELECT record FROM _coven_authored_changes"), 0);
    let all = creations(&db);
    assert_eq!(all.len(), 3);
    assert_eq!(all[..2], first[..], "an update leaves the earlier creations");
    db.close().await.unwrap();
}
