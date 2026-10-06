use std::collections::BTreeSet;

use coven_foundation::id_source::CircleId;
use coven_merge::{Audience, Operation};

use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{DbError, RowIdentity, SyncedTable};

const ANA: &str = "00000000-0000-4000-8000-00000000000a";
const BEN: &str = "00000000-0000-4000-8000-00000000000b";
const NOTE: &str = "00000000-0000-4000-8000-000000000042";

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("attachments", RowIdentity::SharedKey)
            .key_columns(["note_id", "name"])
            .audience_from("note_id"),
        SyncedTable::new("details", RowIdentity::SharedKey)
            .key_columns(["note_id", "name"])
            .audience_from("note_id"),
    ]
}

const SCHEMA: &str = "
    CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, audience TEXT NOT NULL);
    CREATE TABLE attachments(note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE, name TEXT NOT NULL, body BLOB, PRIMARY KEY(note_id,name));
    CREATE TABLE details(note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE, name TEXT NOT NULL, PRIMARY KEY(note_id,name));
";

#[tokio::test]
async fn moving_note_42_moves_its_prewrite_descendants_and_readds_when_moved_back() {
    let store = TestStore::new();
    let database = store.schema(tables(), SCHEMA).await.unwrap();
    database
        .write(BTreeSet::new(), |context| {
            context.execute("INSERT INTO notes VALUES(?1,'Groceries','store')", [NOTE])?;
            context.execute(
                "INSERT INTO attachments VALUES(?1,'list.txt',x'0102')",
                [NOTE],
            )?;
            context.execute("INSERT INTO details VALUES(?1,'paper')", [NOTE])?;
            Ok(())
        })
        .await
        .unwrap();
    database
        .write(BTreeSet::new(), |context| {
            context.execute("UPDATE notes SET audience=?1", [ANA])?;
            context.execute("INSERT INTO attachments VALUES(?1,'new.txt',x'03')", [NOTE])?;
            context.execute("DELETE FROM details", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let writes = records(&database);
    let moved = &writes[1].parts;
    assert_eq!(moved.len(), 2);
    assert_eq!(moved[0].audience, Audience::Store);
    assert_eq!(moved[0].rows.len(), 3);
    assert!(moved[0]
        .rows
        .iter()
        .all(|c| c.change.generation == 1 && matches!(c.change.operation, Operation::Delete)));
    assert_eq!(
        moved[1].audience,
        Audience::Circle(CircleId(uuid::Uuid::parse_str(ANA).unwrap()))
    );
    assert_eq!(moved[1].rows.len(), 3);
    for row in &moved[1].rows {
        assert_eq!(row.change.generation, 0);
        let Operation::Insert(columns) = &row.change.operation else {
            panic!("full moved insert")
        };
        assert_eq!(columns.len(), 3);
        if row.row.table == "attachments" {
            let parent = &columns["note_id"].parents
                [&coven_merge::ForeignKey::new(["note_id"], "notes", ["id"])];
            assert_eq!(parent.row.audience, moved[1].audience);
            assert_eq!(parent.generation, 1);
        }
    }
    sql(&database, "UPDATE notes SET audience='store'")
        .await
        .unwrap();
    let back = &records(&database)[2].parts[0];
    assert_eq!(back.audience, Audience::Store);
    for change in &back.rows {
        let key = coven_format::key::decode_key(&change.row.key).unwrap();
        let was_present = change.row.table == "notes"
            || key[1] == coven_format::value::Value::Text("list.txt".into());
        assert_eq!(change.change.generation, if was_present { 2 } else { 0 });
    }
    database.close().await.unwrap();
}

#[tokio::test]
async fn repointing_a_descendant_moves_its_entire_subtree() {
    let store = TestStore::new();
    let database = store.schema(vec![
        SyncedTable::new("lists", RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("todos", RowIdentity::IndependentUuid).audience_from("list_id"),
        SyncedTable::new("todo_labels", RowIdentity::SharedKey).key_columns(["todo_id", "label"]).audience_from("todo_id"),
    ], "CREATE TABLE lists(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL); CREATE TABLE todos(id TEXT NOT NULL PRIMARY KEY, list_id TEXT NOT NULL REFERENCES lists(id)); CREATE TABLE todo_labels(todo_id TEXT NOT NULL REFERENCES todos(id),label TEXT NOT NULL,PRIMARY KEY(todo_id,label));").await.unwrap();
    database
        .write(BTreeSet::new(), |context| {
            context.execute("INSERT INTO lists VALUES(?1, 'store')", [ANA])?;
            context.execute("INSERT INTO lists VALUES(?1, ?2)", [BEN, ANA])?;
            context.execute("INSERT INTO todos VALUES(?1,?2)", [NOTE, ANA])?;
            context.execute("INSERT INTO todo_labels VALUES(?1,'urgent')", [NOTE])?;
            Ok(())
        })
        .await
        .unwrap();
    database
        .write(BTreeSet::new(), |context| {
            context.execute("UPDATE todos SET list_id=?1", [BEN])?;
            Ok(())
        })
        .await
        .unwrap();
    let record = &records(&database)[1];
    assert_eq!(record.parts.len(), 2);
    for part in &record.parts {
        assert_eq!(
            part.rows
                .iter()
                .map(|c| c.row.table.as_str())
                .collect::<Vec<_>>(),
            ["todo_labels", "todos"]
        );
    }
    database.close().await.unwrap();
}

#[tokio::test]
async fn circle_pins_can_read_store_notes_but_store_and_other_circles_cannot_read_the_pins() {
    let store = TestStore::new();
    let database = store.schema(vec![
        SyncedTable::new("notes", RowIdentity::SharedKey),
        SyncedTable::new("pins", RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("links", RowIdentity::SharedKey),
    ], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,note TEXT REFERENCES notes(id),pin TEXT REFERENCES pins(id)); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,pin TEXT REFERENCES pins(id));").await.unwrap();
    database
        .write(BTreeSet::new(), |context| {
            context.execute("INSERT INTO notes VALUES('42')", [])?;
            context.execute("INSERT INTO pins VALUES(?1,?2,'42',NULL)", [NOTE, ANA])?;
            Ok(())
        })
        .await
        .unwrap();
    for circle in ["store", BEN] {
        let error = database
            .write(BTreeSet::new(), move |context| {
                context.execute(
                    "INSERT INTO pins VALUES(?1,?2,NULL,?3)",
                    [BEN, circle, NOTE],
                )?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, DbError::ReferenceAudience { table, column, key } if table == "pins" && column == "pin" && key.values() == [crate::types::Value::Text(BEN.into())])
        );
    }
    let error = database
        .write(BTreeSet::new(), |context| {
            context.execute("INSERT INTO links VALUES('10',?1)", [NOTE])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, DbError::ReferenceAudience { table, column, .. } if table == "links" && column == "pin")
    );
    assert_eq!(count(&database, "links"), 0);
    assert_eq!(records(&database).len(), 1);
    database.close().await.unwrap();
}

#[tokio::test]
async fn a_note_cannot_be_added_to_gifts_after_its_deletion_was_applied() {
    let store = TestStore::new();
    let database = store.schema(tables(), SCHEMA).await.unwrap();
    let gifts = CircleId(uuid::Uuid::parse_str(ANA).unwrap());
    let error = database
        .write([gifts].into(), |context| {
            context.execute("INSERT INTO notes VALUES(?1,'Gift nine',?2)", [NOTE, ANA])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::DeletedCircle(circle) if circle == gifts));
    assert_eq!(count(&database, "notes"), 0);
    assert!(records(&database).is_empty());
    database.close().await.unwrap();
}

#[tokio::test]
async fn references_use_the_parents_affinity_before_and_after_cascade() {
    let store = TestStore::new();
    let db=store.schema(vec![SyncedTable::new("parents",RowIdentity::SharedKey),SyncedTable::new("children",RowIdentity::SharedKey)],"CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,code INT UNIQUE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(code) ON UPDATE CASCADE)").await.unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('p',1); INSERT INTO children VALUES('c','1')",
    )
    .await
    .unwrap();
    sql(&db, "UPDATE parents SET code=2 WHERE id='p'")
        .await
        .unwrap();
    let writes = records(&db);
    let child = writes[1].parts[0]
        .rows
        .iter()
        .find(|r| r.row.table == "children")
        .unwrap();
    assert_eq!(
        child.old["parent"],
        coven_format::value::Value::Text("1".into())
    );
    db.close().await.unwrap();
}
