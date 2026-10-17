use super::*;
use crate::snapshot_write::tests::{frames, load_one, stream};
use crate::test_utils::tests::{insert, SCHEMA};
use crate::tests::TestStore;
use crate::write::tests::sql;
use crate::{Database, RowIdentity, SyncedTable};
use coven_format::value::Value;

fn owned() -> Vec<SyncedTable> {
    vec![SyncedTable::new("rows", RowIdentity::SharedKey).owner_only()]
}

fn row(n: usize) -> RowId {
    RowId {
        table: "rows".into(),
        key: coven_format::key::encode_key(&[Value::Text(n.to_string())]).unwrap(),
        audience: Audience::Store,
    }
}

fn selected(db: &Database, row: &RowId) -> Option<Creation> {
    db.inspect_writer(|c| creation(c, row).unwrap())
}

fn own(write: WriteId, row: RowId) -> Creation {
    Creation {
        write,
        creator: ChangeId { write, row },
    }
}

/// Two devices insert the same shared key concurrently. Whatever arrives
/// first in the store sequence is the creation (§8.8, Lean Owners).
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
        assert_eq!(
            selected(&db, &row(0)),
            Some(own(first, row(0))),
            "{order:?}"
        );
        db.close().await.unwrap();
    }
}

/// Updates, deletes and reinserts keep the original creation: none of them
/// is a generation-zero insert (§8.8).
#[tokio::test]
async fn later_generations_keep_the_original_creation() {
    let store = TestStore::new();
    let db = store.schema(owned(), SCHEMA).await.unwrap();
    // The creating device is another of this device's member's, who may
    // edit the row (§8.8).
    let creator = insert(0, 0);
    TestStore::register(&db, store.device(), 1);
    TestStore::register(&db, creator.header.position.device, 1);
    db.receive(creator.into()).await.unwrap();
    let original = selected(&db, &row(0));
    assert!(original.is_some());
    sql(&db, "UPDATE rows SET a='changed' WHERE id='0'")
        .await
        .unwrap();
    sql(&db, "DELETE FROM rows WHERE id='0'").await.unwrap();
    sql(&db, "INSERT INTO rows VALUES('0','again','b')")
        .await
        .unwrap();
    assert_eq!(selected(&db, &row(0)), original);
    // A new row is created by this device's own write.
    sql(&db, "INSERT INTO rows VALUES('5','a','b')")
        .await
        .unwrap();
    let local = crate::write::tests::records(&db)
        .pop()
        .unwrap()
        .header
        .position;
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
    store
        .schema(owned(), SCHEMA)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
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
        source
            .receive_stream(stream(&insert(n, 0)), || Ok(()))
            .await
            .unwrap();
    }
    let snapshot = frames(&source, Audience::Store).await;
    let target_store = TestStore::new();
    let target = target_store.schema(owned(), SCHEMA).await.unwrap();
    let (id, prefix, input, _) = snapshot.input();
    load_one(&target, id, prefix, input).await.unwrap();
    assert_eq!(selected(&target, &row(0)), selected(&source, &row(0)));
    assert_eq!(
        selected(&target, &row(0)),
        Some(own(insert(1, 0).header.position, row(0)))
    );
    source.close().await.unwrap();
    target.close().await.unwrap();
}

/// Renaming the table keeps the creating write, and its original names:
/// the migration's author does not become the creator (§8.8, §17.1).
#[tokio::test]
async fn a_table_rename_keeps_the_original_creation() {
    let store = TestStore::new();
    let db = store.schema(owned(), SCHEMA).await.unwrap();
    db.receive_stream(stream(&insert(0, 0)), || Ok(()))
        .await
        .unwrap();
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
    let renamed = RowId {
        table: "items".into(),
        ..row(0)
    };
    assert_eq!(selected(&db, &renamed), Some(original));
    assert_eq!(selected(&db, &row(0)), None);
    db.close().await.unwrap();
}
