use super::*;
use crate::internal_schema::{FOLLOWED, OUTSIDE_STORE_TABLES, READABLE};
use crate::tests::TestStore;

/// Each object of the readable objects and of `OUTSIDE_STORE_TABLES`, as
/// `(type, name, sql)`, with no SQL for an index SQLite made for a
/// constraint, and each followed table's row count, in order.
type Readable = (Vec<(String, String, Option<String>)>, Vec<(String, i64)>);

/// The tables, indexes and views SQLite keeps for one object, leaving out
/// triggers: in a store, coven's triggers that keep the followed tables are
/// on the tables the views read.
const OBJECTS: &str = "SELECT type,name,sql FROM sqlite_schema
    WHERE type IN ('table','index','view') AND tbl_name=?1 ORDER BY type,name";

/// This device's id outside a store.
const DEVICE: DeviceId = DeviceId(7);

/// Computations that decode the facts the views pass and show every state
/// as synced and every upload as queued: sync's own computations are in
/// coven-sync, which this crate can't reach.
pub(crate) fn sync_functions() -> crate::SyncFunctions {
    crate::SyncFunctions::new::<crate::sync_facts::tests::Record>(
        |_facts| {
            Ok(crate::SyncStateRow {
                state: "synced",
                pending: None,
                detail: serde_json::json!({}),
            })
        },
        |_facts| Ok(crate::UploadPhaseRow::Waiting("queued")),
    )
}

/// A connection outside any store, with coven's readable objects made on it.
fn outside_store() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    make_readable_objects(&connection, DEVICE, &sync_functions()).unwrap();
    connection
}

/// The schema a build checks app SQL against, opened from coven's setup of
/// the compiler.
pub(crate) fn build_probe() -> sqlive::SchemaProbe {
    let setup = coven_schema::compiler_setup();
    sqlive::SchemaProbe::open(&setup.sql, &setup.functions, &setup.collations).unwrap()
}

/// Every readable object, and every table in `OUTSIDE_STORE_TABLES`.
fn readable_names() -> impl Iterator<Item = &'static str> {
    READABLE.into_iter().chain(OUTSIDE_STORE_TABLES)
}

/// `make_readable_objects` makes each readable object, and each table of
/// coven's own it makes them with, outside a store as a new store holds it:
/// its columns, constraints and indexes, and the one row of the sync status
/// and the retry effect.
#[tokio::test]
async fn the_readable_objects_are_made_as_a_store_holds_them() {
    let outside = outside_store();
    let mut objects = Vec::new();
    let mut rows = Vec::new();
    for name in readable_names() {
        let mut statement = outside.prepare(OBJECTS).unwrap();
        let found = statement.query_map([name], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        objects.extend(found.map(Result::unwrap));
    }
    for followed in FOLLOWED {
        let count = format!("SELECT count(*) FROM {}", followed.name);
        rows.push((followed.name.to_owned(), outside.query_row(&count, [], |r| r.get(0)).unwrap()));
    }
    let outside: Readable = (objects, rows);

    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let in_store: Readable = db.inspect_writer(|sql, _| {
        let mut objects = Vec::new();
        let mut rows = Vec::new();
        for name in readable_names() {
            objects.extend(
                sql.query(OBJECTS, [name], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap(),
            );
        }
        for followed in FOLLOWED {
            let count = format!("SELECT count(*) FROM {}", followed.name);
            rows.push((followed.name.to_owned(), sql.query_row(&count, [], |r| r.get(0)).unwrap()));
        }
        (objects, rows)
    });
    db.close().await.unwrap();
    assert_eq!(outside, in_store);
    let made: Vec<&str> = outside
        .0
        .iter()
        .filter(|(kind, ..)| kind != "index")
        .map(|(_, name, _)| name.as_str())
        .collect();
    assert_eq!(made.len(), READABLE.len() + OUTSIDE_STORE_TABLES.len());
}

/// The text `readable_objects_sql` returns makes, on a connection with no
/// function registered, the objects `make_readable_objects` makes.
#[test]
fn the_sql_text_makes_the_objects_on_a_bare_connection() {
    let bare = Connection::open_in_memory().unwrap();
    bare.execute_batch(&coven_schema::readable_objects_sql()).unwrap();
    let outside = outside_store();
    let schema = |connection: &Connection| -> Vec<(String, String, Option<String>)> {
        let mut statement = connection
            .prepare("SELECT type,name,sql FROM sqlite_schema ORDER BY type,name")
            .unwrap();
        let rows = statement.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        rows.map(Result::unwrap).collect()
    };
    assert_eq!(schema(&bare), schema(&outside));
    for name in readable_names() {
        assert!(schema(&bare).iter().any(|(_, found, _)| found == name), "{name}");
    }
}

/// App SQL reads every readable object outside a store: each view finds the
/// tables and functions it reads.
#[test]
fn every_readable_object_reads_outside_a_store() {
    let outside = outside_store();
    for name in READABLE {
        let mut statement = outside.prepare(&format!("SELECT * FROM {name}")).unwrap();
        let mut rows = statement.query([]).unwrap();
        while rows.next().unwrap().is_some() {}
    }
    let network: (Option<bool>, bool, bool) = outside
        .query_row("SELECT usable,metered,unmetered_only FROM coven_network", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(network, (None, false, false));
    let state: String =
        outside.query_row("SELECT state FROM coven_sync_state", [], |r| r.get(0)).unwrap();
    assert_eq!(state, "synced");
}

/// An app's trigger on each followed table applies outside a store, and
/// fires on the rows a test writes there.
#[test]
fn an_app_trigger_on_each_followed_table_fires_outside_a_store() {
    let outside = outside_store();
    outside.execute_batch("CREATE TABLE heard(what TEXT NOT NULL PRIMARY KEY);").unwrap();
    for followed in FOLLOWED {
        outside
            .execute_batch(&format!(
                "CREATE TRIGGER heard_{name} AFTER UPDATE ON {name}
                 BEGIN INSERT INTO heard VALUES('{name}'); END;",
                name = followed.name,
            ))
            .unwrap();
    }
    outside
        .execute_batch(
            "UPDATE coven_sync_status SET starts=starts+1;
             UPDATE coven_retry_effect SET syncing=1;",
        )
        .unwrap();
    let heard: Vec<String> = outside
        .prepare("SELECT what FROM heard ORDER BY rowid")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(heard, ["coven_sync_status", "coven_retry_effect"]);
}

/// An app's schema whose trigger joins `coven_user_files` applies outside a
/// store, and a row that fires the trigger reads the original the test
/// recorded there, as `coven_user_files` shows it in a store.
#[test]
fn an_app_trigger_reads_a_recorded_original_outside_a_store() {
    let outside = outside_store();
    outside
        .execute_batch(
            "CREATE TABLE tracks(id INTEGER PRIMARY KEY, file TEXT NOT NULL);
             CREATE TABLE track_originals(track INTEGER NOT NULL PRIMARY KEY, path TEXT NOT NULL, size INTEGER NOT NULL,
                 modified_seconds INTEGER NOT NULL, modified_nanos INTEGER NOT NULL);
             CREATE TRIGGER track_original AFTER INSERT ON tracks BEGIN
                 INSERT INTO track_originals
                 SELECT NEW.id,coven_path_text(u.path),u.size,u.modified_seconds,
                     u.modified_nanos
                 FROM coven_user_files AS u WHERE u.file=NEW.file;
             END;",
        )
        .unwrap();
    let file = FileId(uuid::Uuid::from_bytes([3; 16]));
    // Nanoseconds in 100 ns steps, a precision every platform keeps: Windows
    // keeps 100 ns.
    let modified_at = std::time::UNIX_EPOCH + std::time::Duration::new(1_700_000_000, 300);
    let path = std::path::Path::new("/music/track 1.flac");
    record_user_file(&outside, file, path, modified_at, b"track one's bytes").unwrap();

    outside.execute("INSERT INTO tracks(file) VALUES(?1)", [file]).unwrap();
    let original: (i64, String, i64, i64, i64) = outside
        .query_row("SELECT * FROM track_originals", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap();
    assert_eq!(original, (1, "/music/track 1.flac".to_owned(), 17, 1_700_000_000, 300));
}

/// `coven_schema::SQL_FUNCTIONS` lists exactly the functions of coven's that
/// `make_readable_objects` registers, each with its argument count and
/// whether it is deterministic.
#[test]
fn the_exported_function_list_is_what_the_connection_registers() {
    const DETERMINISTIC: i64 = 0x800;
    let outside = outside_store();
    let mut statement = outside
        .prepare(
            "SELECT name, narg, flags FROM pragma_function_list
             WHERE name LIKE 'coven\\_%' ESCAPE '\\' OR name LIKE '\\_coven\\_%' ESCAPE '\\'
             ORDER BY name",
        )
        .unwrap();
    let registered: Vec<coven_schema::SqlFunction> = statement
        .query_map([], |r| {
            let name: String = r.get(0)?;
            Ok((name, r.get::<_, i32>(1)?, r.get::<_, i64>(2)? & DETERMINISTIC != 0))
        })
        .unwrap()
        .map(|row| {
            let (name, arguments, deterministic) = row.unwrap();
            let listed = coven_schema::SQL_FUNCTIONS.iter().find(|function| function.name == name);
            coven_schema::SqlFunction {
                name: listed.map_or("(not in the list)", |function| function.name),
                arguments,
                deterministic,
            }
        })
        .collect();
    assert_eq!(registered, coven_schema::SQL_FUNCTIONS);
}

/// sqlive reads none of coven's views whole: a live query narrows each
/// through the view's own SELECT (§4), so a read's report names the tables
/// the view reads, and no view of coven's.
#[test]
fn sqlive_reads_no_view_of_covens_whole() {
    let outside = outside_store();
    let views: Vec<&str> = READABLE
        .into_iter()
        .filter(|name| {
            outside
                .query_row("SELECT type='view' FROM sqlite_schema WHERE name=?1", [name], |r| {
                    r.get(0)
                })
                .unwrap()
        })
        .collect();
    assert!(views.contains(&"coven_uploads"), "{views:?}");
    let probe = build_probe();
    for name in READABLE {
        let report = probe.read_report(&format!("SELECT * FROM {name}")).unwrap();
        let sqlive::ReadReport::Entries(entries) = &report else { panic!("{name}: {report}") };
        for entry in entries {
            assert!(!views.contains(&entry.object.as_str()), "{name}: {report}");
        }
    }
}
