use coven_foundation::id_source::CircleId;
use coven_merge::{Audience, Operation};

use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{DbError, RowIdentity, SyncedTable};

const ANA: &str = "00000000-0000-4000-8000-00000000000a";
const BEN: &str = "00000000-0000-4000-8000-00000000000b";
const NOTE: &str = "00000000-0000-4000-8000-000000000042";

/// A stored key coven can't decode is damage, however the key is read:
/// as a row's reported key or as the values that select its row.
#[test]
fn an_undecodable_stored_key_is_damaged_database() {
    let key = [0xff];
    assert!(matches!(crate::RowKey::decode(&key), Err(DbError::DamagedDatabase)));
    assert!(matches!(super::key_parameters(&key), Err(DbError::DamagedDatabase)));
}

#[test]
fn ordered_key_component_and_byte_limits_keep_the_exact_boundary() {
    use coven_format::value::Value;
    let components = 65_536;
    let mut values = vec![Value::Integer(0); components];
    assert_eq!(super::equality_key(&values).unwrap(), vec![0x12; components]);
    values.push(Value::Integer(0));
    assert!(matches!(
        super::equality_key(&values),
        Err(DbError::TooLarge { field: "key components", actual: 65_537, maximum: 65_536 })
    ));
    let maximum = 8 * 1024 * 1024;
    // A blob component has one tag and a two-byte terminator. Nonzero
    // payload bytes need no escaping, so this exercises the encoded limit.
    for length in [maximum - 3, maximum - 2] {
        let result = super::equality_key(&[Value::Blob(vec![1; length])]);
        if length == maximum - 3 {
            let key = result.unwrap();
            assert_eq!(key.len(), maximum);
            assert_eq!(key[0], 0x30);
            assert_eq!(&key[maximum - 2..], [0, 0]);
        } else {
            assert!(matches!(result,
                Err(DbError::TooLarge { field: "key bytes", actual, maximum: bound })
                    if actual == maximum as u64 + 1 && bound == maximum as u64));
        }
    }
}

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
    store.join_circle(&database, CircleId(uuid::Uuid::parse_str(ANA).unwrap()));
    database
        .write(|context| {
            context.execute("INSERT INTO notes VALUES(?1,'Groceries','store')", [NOTE])?;
            context.execute("INSERT INTO attachments VALUES(?1,'list.txt',x'0102')", [NOTE])?;
            context.execute("INSERT INTO details VALUES(?1,'paper')", [NOTE])?;
            Ok(())
        })
        .await
        .unwrap();
    database
        .write(|context| {
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
    assert_eq!(moved[1].audience, Audience::Circle(CircleId(uuid::Uuid::parse_str(ANA).unwrap())));
    assert_eq!(moved[1].rows.len(), 3);
    for row in &moved[1].rows {
        assert_eq!(row.change.generation, 0);
        let Operation::Insert(columns) = &row.change.operation else { panic!("full moved insert") };
        assert_eq!(columns.len(), 3);
        if row.row.table == "attachments" {
            let parent = &columns["note_id"].parents
                [&coven_merge::ForeignKey::new(["note_id"], "notes", ["id"])];
            assert_eq!(parent.row.audience, moved[1].audience);
            assert_eq!(parent.generation, 1);
        }
    }
    sql(&database, "UPDATE notes SET audience='store'").await.unwrap();
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
    store.join_circle(&database, CircleId(uuid::Uuid::parse_str(ANA).unwrap()));
    database
        .write(|context| {
            context.execute("INSERT INTO lists VALUES(?1, 'store')", [ANA])?;
            context.execute("INSERT INTO lists VALUES(?1, ?2)", [BEN, ANA])?;
            context.execute("INSERT INTO todos VALUES(?1,?2)", [NOTE, ANA])?;
            context.execute("INSERT INTO todo_labels VALUES(?1,'urgent')", [NOTE])?;
            Ok(())
        })
        .await
        .unwrap();
    database
        .write(|context| {
            context.execute("UPDATE todos SET list_id=?1", [BEN])?;
            Ok(())
        })
        .await
        .unwrap();
    let record = &records(&database)[1];
    assert_eq!(record.parts.len(), 2);
    for part in &record.parts {
        assert_eq!(
            part.rows.iter().map(|c| c.row.table.as_str()).collect::<Vec<_>>(),
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
    store.join_circle(&database, CircleId(uuid::Uuid::parse_str(ANA).unwrap()));
    database
        .write(|context| {
            context.execute("INSERT INTO notes VALUES('42')", [])?;
            context.execute("INSERT INTO pins VALUES(?1,?2,'42',NULL)", [NOTE, ANA])?;
            Ok(())
        })
        .await
        .unwrap();
    for circle in ["store", BEN] {
        let error = database
            .write(move |context| {
                context.execute("INSERT INTO pins VALUES(?1,?2,NULL,?3)", [BEN, circle, NOTE])?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, DbError::ReferenceAudience { table, column, key } if table == "pins" && column == "pin" && key.values() == [crate::types::Value::Text(BEN.into())])
        );
    }
    let error = database
        .write(|context| {
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
    crate::tests::delete_circle(&database, gifts).await.unwrap();
    let error = database
        .write(|context| {
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

/// A breaking migration that writes a note into Gifts after its deletion
/// applied fails as the deletion, and the store stays at its version
/// (§15.9, §18.4).
#[tokio::test]
async fn a_migration_cannot_write_into_gifts_after_its_deletion_was_applied() {
    use crate::{Migration, OldWrites};
    const GIFT: &str =
        "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000042','Gift nine',
           '00000000-0000-4000-8000-00000000000a')";
    let store = TestStore::new();
    let initial = || vec![Migration::addition(1, "schema", SCHEMA)];
    let database = store.builder(tables(), initial()).open().await.unwrap();
    let gifts = CircleId(uuid::Uuid::parse_str(ANA).unwrap());
    crate::tests::delete_circle(&database, gifts).await.unwrap();
    database.close().await.unwrap();
    let mut migrations = initial();
    migrations.push(Migration::breaking(2, "gift", GIFT, OldWrites::Drop, tables()));
    let error = store.builder(tables(), migrations).open().await.err().unwrap();
    let error = crate::tests::database_error(error);
    assert!(matches!(error, DbError::DeletedCircle(circle) if circle == gifts), "{error:?}");
    let database = store.builder(tables(), initial()).open().await.unwrap();
    assert_eq!(database.schema_version().await.unwrap(), 1);
    assert_eq!(count(&database, "notes"), 0);
    database.close().await.unwrap();
}

#[tokio::test]
async fn references_use_the_parents_affinity_before_and_after_cascade() {
    let store = TestStore::new();
    let db=store.schema(vec![SyncedTable::new("parents",RowIdentity::SharedKey).key_columns(["code"]),SyncedTable::new("children",RowIdentity::SharedKey)],"CREATE TABLE parents(id TEXT,code INT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(code) ON UPDATE CASCADE)").await.unwrap();
    sql(&db, "INSERT INTO parents VALUES('p',1); INSERT INTO children VALUES('c','1')")
        .await
        .unwrap();
    sql(&db, "UPDATE parents SET code=2 WHERE id='p'").await.unwrap();
    let writes = records(&db);
    let child = writes[1].parts[0].rows.iter().find(|r| r.row.table == "children").unwrap();
    let coven_merge::Operation::Update(columns) = &child.change.operation else {
        panic!("the cascade updates the child")
    };
    assert_eq!(columns["parent"].value, coven_format::value::Value::Text("2".into()));
    assert_eq!(columns["parent"].parents.len(), 1);
    assert!(child.old.is_empty());
    db.close().await.unwrap();
}

#[test]
fn row_id_order_keeps_table_key_and_audience_precedence() {
    use coven_format::value::Value;
    let circle = |id| Audience::Circle(CircleId(uuid::Uuid::from_u128(id)));
    let ordered = [
        ("a", 2, circle(u128::MAX)),
        ("b", 1, circle(u128::MAX)),
        ("b", 2, Audience::Store),
        ("b", 2, circle(1)),
        ("b", 2, circle(2)),
    ]
    .map(|(table, key, audience)| {
        let key_value = Value::Integer(key);
        let encoded = super::equality_key(std::slice::from_ref(&key_value)).unwrap();
        super::row_id(
            &(table.into(), encoded),
            &super::AppRow {
                values: [("id".into(), key_value)].into(),
                audience,
                parents: Default::default(),
            },
        )
    });
    for pair in ordered.windows(2) {
        assert!(pair[0] < pair[1], "{pair:?}");
    }
}

mod typed_bindings {
    use crate::tests::TestStore;
    use crate::{DbError, RowIdentity, SyncedTable};
    use coven_foundation::id_source::CircleId;
    use coven_foundation::uuid_text::UuidText;
    use coven_merge::Audience;

    const KEY: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    const CIRCLE: &str = "018f22bb-aaaa-7777-8ccc-000000000001";

    #[tokio::test]
    async fn typed_audiences_round_trip_and_uppercase_text_is_refused() {
        let store = TestStore::new();
        let db = store
            .schema(
                vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)
                    .audience_column("audience")],
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL)",
            )
            .await
            .unwrap();
        let circle = CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap());
        store.join_circle(&db, circle);
        db.write(|sql| {
            let key = UuidText(uuid::Uuid::parse_str(KEY).unwrap());
            sql.execute("INSERT INTO notes VALUES(?1,?2)", (key, Audience::Store))?;
            Ok(())
        })
        .await
        .unwrap();
        let audience = db
            .read(|sql| {
                Ok::<_, DbError>(
                    sql.query_row("SELECT audience FROM notes", [], |r| r.get::<_, Audience>(0))?,
                )
            })
            .await
            .unwrap();
        assert_eq!(audience, Audience::Store);
        db.write(move |sql| {
            sql.execute("UPDATE notes SET audience=?1", [circle])?;
            Ok(())
        })
        .await
        .unwrap();
        let (audience, read_circle, text) = db
            .read(|sql| {
                Ok::<_, DbError>(sql.query_row("SELECT audience FROM notes", [], |r| {
                    Ok((r.get::<_, Audience>(0)?, r.get::<_, CircleId>(0)?, r.get::<_, String>(0)?))
                })?)
            })
            .await
            .unwrap();
        assert_eq!(audience, Audience::Circle(circle));
        assert_eq!(read_circle, circle);
        assert_eq!(text, CIRCLE);
        db.write(move |sql| {
            sql.execute("UPDATE notes SET audience=?1", [Audience::Circle(circle)])?;
            Ok(())
        })
        .await
        .unwrap();
        let error = db
            .write(|sql| {
                sql.execute("UPDATE notes SET audience=?1", [CIRCLE.to_uppercase()])?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::InvalidAudience { .. }));
        let (key, text) = db
            .read(|sql| {
                Ok::<_, DbError>(sql.query_row("SELECT id FROM notes", [], |r| {
                    Ok((r.get::<_, UuidText>(0)?, r.get::<_, String>(0)?))
                })?)
            })
            .await
            .unwrap();
        assert_eq!(key.0, uuid::Uuid::parse_str(KEY).unwrap());
        assert_eq!(text, KEY);
        assert_eq!(crate::RowKey::from(key).values(), [rusqlite::types::Value::Text(KEY.into())]);
        let error = db
            .write(|sql| {
                sql.execute("UPDATE notes SET id=?1", [KEY.to_uppercase()])?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::KeyNotUuid { .. }));
        db.close().await.unwrap();
    }
}

fn foreign_key_failure(error: &DbError) -> bool {
    matches!(
        error,
        DbError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
            if failure.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_FOREIGNKEY
    )
}

const DEFERRED_ALBUMS: &str = "CREATE TABLE artists(id TEXT NOT NULL PRIMARY KEY);
     CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY,
       artist TEXT REFERENCES artists(id) DEFERRABLE INITIALLY DEFERRED)";

fn artists_and_albums() -> Vec<SyncedTable> {
    ["artists", "albums"].map(|t| SyncedTable::new(t, RowIdentity::SharedKey)).into()
}

/// SQLite checks a deferred foreign key only at commit. A write that leaves
/// Ana's album naming an artist no row holds fails as that commit would,
/// with SQLite's foreign-key failure, and writes nothing.
#[tokio::test]
async fn a_dangling_deferred_reference_fails_the_write_as_its_commit_would() {
    let store = TestStore::new();
    let db = store.schema(artists_and_albums(), DEFERRED_ALBUMS).await.unwrap();
    let error = sql(&db, "INSERT INTO albums VALUES('first','gone')").await.unwrap_err();
    assert!(foreign_key_failure(&error), "{error:?}");
    assert_eq!(count(&db, "albums"), 0);
    assert!(records(&db).is_empty());
    // A parent the same write inserts later is found.
    sql(&db, "INSERT INTO albums VALUES('first','ana'); INSERT INTO artists VALUES('ana')")
        .await
        .unwrap();
    assert_eq!(count(&db, "albums"), 1);
    db.close().await.unwrap();
}

/// A breaking migration that adds a deferred foreign key to albums while
/// one album's artist is gone fails as its commit would, with SQLite's
/// foreign-key failure, and leaves the schema as it was.
#[tokio::test]
async fn a_migration_adding_a_deferred_key_over_a_dangling_row_fails_as_its_commit_would() {
    use crate::{Migration, OldWrites};
    const BEFORE: &str = "CREATE TABLE artists(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, artist TEXT)";
    const REFERENCE: &str = "CREATE TABLE albums_new(id TEXT NOT NULL PRIMARY KEY,
           artist TEXT REFERENCES artists(id) DEFERRABLE INITIALLY DEFERRED);
         INSERT INTO albums_new SELECT id,artist FROM albums;
         DROP TABLE albums;
         ALTER TABLE albums_new RENAME TO albums";
    let store = TestStore::new();
    let initial = || vec![Migration::addition(1, "schema", BEFORE)];
    let db = store.builder(artists_and_albums(), initial()).open().await.unwrap();
    sql(&db, "INSERT INTO albums VALUES('first','gone')").await.unwrap();
    db.close().await.unwrap();
    let mut migrations = initial();
    migrations.push(Migration::breaking(
        2,
        "reference",
        REFERENCE,
        OldWrites::Drop,
        artists_and_albums(),
    ));
    let error = store.builder(artists_and_albums(), migrations).open().await.err().unwrap();
    let error = crate::tests::database_error(error);
    assert!(foreign_key_failure(&error), "{error:?}");
    let db = store.builder(artists_and_albums(), initial()).open().await.unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 1);
    db.close().await.unwrap();
}

/// The rows a write inserts name a parent through a foreign key; the parent
/// is read once for the write however many rows name it, and a write after
/// one that changed it reads it as it is.
#[tokio::test]
async fn a_parent_many_rows_name_is_read_once_per_write() {
    let store = TestStore::new();
    let db = store
        .builder(
            vec![
                SyncedTable::new("parents", RowIdentity::SharedKey),
                SyncedTable::new("kids", RowIdentity::SharedKey),
            ],
            vec![crate::Migration::addition(
                1,
                "kids",
                "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);
                 CREATE TABLE kids(id TEXT NOT NULL PRIMARY KEY, parent_id TEXT NOT NULL REFERENCES parents(id), name TEXT NOT NULL)",
            )],
        )
        .open()
        .await
        .unwrap();
    let parent_reads = |db: &crate::Database| {
        db.inspect_writer(|writer| {
            writer
                .profiled_statements()
                .iter()
                .filter(|statement| statement.sql.contains("FROM main.\"parents\" WHERE"))
                .count()
        })
    };
    // A write inserting `parent` and `kids` that name it.
    let add = |parent: &'static str, kids: &'static [&'static str]| {
        let db = db.clone();
        async move {
            db.write(move |sql| {
                sql.execute("INSERT INTO parents VALUES(?1, 'first')", [parent])?;
                for kid in kids {
                    sql.execute("INSERT INTO kids VALUES(?1, ?2, 'kid')", [kid, &parent])?;
                }
                Ok(())
            })
            .await
            .unwrap();
            parent_reads(&db)
        }
    };
    let add_to_existing = |db: &crate::Database, kids: &'static [&'static str]| {
        let db = db.clone();
        async move {
            db.write(move |sql| {
                for kid in kids {
                    sql.execute("INSERT INTO kids VALUES(?1, 'p', 'kid')", [kid])?;
                }
                Ok(())
            })
            .await
            .unwrap();
            parent_reads(&db)
        }
    };
    let one = add("p", &["a"]).await;
    let many = add("q", &["b", "c", "d", "e", "f"]).await;
    assert_eq!(many, one);
    // A parent changed by an earlier write is read again, not remembered.
    db.write(|sql| {
        sql.execute("UPDATE parents SET name='second' WHERE id='p'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let after_change = add_to_existing(&db, &["w"]).await;
    assert_eq!(add_to_existing(&db, &["x", "y", "z"]).await, after_change);
    assert_eq!(count(&db, "_coven_rows"), 2 + 6 + 4);
    db.close().await.unwrap();
}

/// Rows noted at one count of changed rows are served while the count
/// holds, and none after any row changes.
#[test]
fn noted_rows_are_served_only_while_no_row_changes() {
    use coven_format::value::Value;
    let key = ("notes".to_string(), vec![1]);
    let values = Some(super::AppValues::from([("id".to_string(), Value::Integer(1))]));
    let mut read = super::RowsRead::default();
    read.note(5, &key, &values);
    assert_eq!(read.get(5, &key), Some(values.clone()));
    assert_eq!(read.get(6, &key), None, "a row changed since it was read");
    assert_eq!(read.get(5, &key), None, "a count that went back doesn't bring them back");
    read.note(6, &key, &None);
    assert_eq!(read.get(6, &key), Some(None), "a row found missing is noted too");
}
