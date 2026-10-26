use crate::tests::{database_error, TestStore};
use crate::write::tests::sql;
use crate::{CovenError, DbError, Migration, RowIdentity, SyncedTable};

fn notes() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)";

const VIEWS: [&str; 5] = [
    "coven_uploads",
    "coven_pins",
    "coven_pending",
    "coven_members",
    "coven_devices",
];

/// The views are the app's only window on coven's tables: it reads them,
/// in a read, a write and a migration, but still not the tables they read.
#[tokio::test]
async fn the_app_reads_coven_views_but_not_covens_tables() {
    let store = TestStore::new();
    let db = store
        .builder(
            notes(),
            vec![
                Migration::addition(1, "notes", NOTES),
                // A migration reads a view too.
                Migration::addition(
                    2,
                    "counted",
                    "CREATE TABLE counted(views INTEGER NOT NULL); INSERT INTO counted SELECT count(*) FROM coven_members",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    for view in VIEWS {
        let statement = format!("SELECT * FROM {view}");
        let rows = db
            .read(move |sql| Ok(sql.query(&statement, [], |_| Ok(()))?))
            .await
            .unwrap();
        assert!(rows.is_empty(), "{view}");
        // A count reads no column of the tables behind the view.
        let statement = format!("SELECT count(*) FROM {view}");
        let count: i64 = db
            .read(move |sql| Ok(sql.query_row(&statement, [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, 0, "{view}");
    }
    db.write(|sql| {
        sql.execute(
            "INSERT INTO notes SELECT 'pending', count(*) FROM coven_pending",
            [],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    // A view in an app subquery, as an order or a filter reads it.
    let first: Vec<String> = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT id FROM notes ORDER BY EXISTS(SELECT 1 FROM coven_uploads AS u
                 WHERE u.table_name='notes' AND u.key=coven_key(notes.id)) DESC, id",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(first, ["pending"]);
    for statement in [
        "SELECT * FROM _coven_file_uploads",
        "SELECT * FROM _coven_members",
        // A table counted without a view in the statement.
        "SELECT count(*) FROM _coven_members",
        "SELECT * FROM coven_uploads JOIN _coven_pending",
    ] {
        let error = db
            .read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?))
            .await
            .unwrap_err();
        assert!(
            matches!(error, CovenError::Database(DbError::InternalTable { .. })),
            "{statement}: {error:?}"
        );
    }
    db.close().await.unwrap();
}

/// Nothing the app runs writes a view, or names an object of its own after
/// one.
#[tokio::test]
async fn the_app_writes_no_view_and_makes_no_object_in_its_name() {
    let store = TestStore::new();
    let db = store
        .builder(notes(), vec![Migration::addition(1, "notes", NOTES)])
        .open()
        .await
        .unwrap();
    for (statement, view) in [
        ("DELETE FROM coven_uploads", "coven_uploads"),
        ("UPDATE coven_pins SET kept=0", "coven_pins"),
        (
            "INSERT INTO coven_members(member,role,removed,is_self) VALUES('m','admin',0,1)",
            "coven_members",
        ),
    ] {
        // SQLite refuses to change a view, or the authorizer refuses to
        // write a name in coven's namespace first.
        match sql(&db, statement).await {
            Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(message)))) => {
                assert_eq!(
                    message,
                    format!("cannot modify {view} because it is a view")
                )
            }
            Err(DbError::InternalTable { table }) => assert_eq!(table, view),
            result => panic!("{statement}: expected a refusal, received {result:?}"),
        }
    }
    db.close().await.unwrap();
    for statement in [
        "CREATE TRIGGER coven_pins_changed AFTER INSERT ON notes BEGIN SELECT 1; END",
        "CREATE VIEW coven_files AS SELECT * FROM notes",
        "CREATE TRIGGER notes_uploads INSTEAD OF DELETE ON coven_uploads BEGIN SELECT 1; END",
    ] {
        let store = TestStore::new();
        let error = store
            .builder(
                notes(),
                vec![
                    Migration::addition(1, "notes", NOTES),
                    Migration::addition(2, "reserved", statement),
                ],
            )
            .open()
            .await
            .err()
            .expect("an object in coven's name is refused");
        assert!(
            matches!(database_error(error), DbError::InternalTable { .. }),
            "{statement}"
        );
    }
}

/// `coven_key` gives a row's key as coven's views hold it, for one column or
/// several, and an app index can hold it.
#[tokio::test]
async fn coven_key_encodes_a_rows_key_and_an_index_can_hold_it() {
    let store = TestStore::new();
    let db = store
        .builder(
            notes(),
            vec![Migration::addition(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL); CREATE INDEX notes_key ON notes(coven_key(id))",
            )],
        )
        .open()
        .await
        .unwrap();
    let (one, two): (Vec<u8>, Vec<u8>) = db
        .read(|sql| {
            Ok(
                sql.query_row("SELECT coven_key('a'), coven_key(7,'a')", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?,
            )
        })
        .await
        .unwrap();
    use coven_format::value::Value;
    assert_eq!(
        one,
        coven_format::key::encode_key(&[Value::Text("a".into())]).unwrap()
    );
    // format.md D3's example key.
    assert_eq!(
        two,
        [0x13, 0x04, 0x34, 0xE0, 0, 0, 0, 0, 0, 0, 0, 0x20, 0x61, 0, 0]
    );
    sql(&db, "INSERT INTO notes VALUES('a','apples')")
        .await
        .unwrap();
    db.inspect_readers(|reader| {
        let plan: Vec<String> = reader
            .query(
                "EXPLAIN QUERY PLAN SELECT title FROM notes WHERE coven_key(id)=coven_key('a')",
                [],
                |r| r.get(3),
            )
            .unwrap();
        assert!(plan.join(";").contains("INDEX notes_key"), "{plan:?}");
    });
    db.close().await.unwrap();
}
