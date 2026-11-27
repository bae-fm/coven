use crate::tests::{QueryError, TestDatabase, TestOptions};
use crate::{HostTables, LiveQuery, SqlValue};

/// Tracks, the uploads the host has under way, by track, and its network.
const DECLARED: [&str; 2] = [
    "CREATE TABLE sending(track TEXT NOT NULL PRIMARY KEY, sent INTEGER NOT NULL)",
    "CREATE TABLE network(id INTEGER PRIMARY KEY, metered INTEGER NOT NULL)",
];

const SCHEMA: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, album TEXT NOT NULL);
    CREATE INDEX tracks_album ON tracks(album);
    CREATE VIEW album_sending AS
        SELECT t.album, s.track, s.sent FROM sending AS s CROSS JOIN tracks AS t ON t.id = s.track;
    CREATE TABLE notes(track TEXT NOT NULL PRIMARY KEY, body TEXT NOT NULL);
    INSERT INTO tracks VALUES ('a1','a'),('b1','b')";

fn open() -> (TestDatabase, HostTables) {
    let tables = HostTables::new(&DECLARED).unwrap();
    let db = TestDatabase::open_with(
        SCHEMA,
        TestOptions { host_tables: Some(tables.clone()), ..Default::default() },
    );
    (db, tables)
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.as_bytes().to_vec())
}

fn send(tables: &HostTables, track: &str, sent: i64) {
    tables.change(|rows| {
        rows.put("sending", vec![text(track), SqlValue::Integer(sent)]);
    });
}

fn end(tables: &HostTables, track: &str) {
    tables.change(|rows| rows.delete("sending", &[text(track)]));
}

fn live(db: &TestDatabase, sql: &'static str) -> LiveQuery<Vec<String>, QueryError> {
    db.subscribe(move |db| Ok(db.query(sql, [], |row| row.get::<_, String>(0))?))
}

async fn next(query: &mut LiveQuery<Vec<String>, QueryError>) -> Vec<String> {
    query.next_inline().await.expect("the query is open").expect("the run succeeds")
}

/// A read of one album's uploads, through a join and through a view of one,
/// runs again as an upload of its own tracks starts, moves or ends, and not
/// for another album's.
#[tokio::test]
async fn a_change_to_a_host_row_reruns_only_the_reads_that_join_its_key() {
    for sql in [
        "SELECT s.track || ' ' || s.sent FROM tracks AS t JOIN sending AS s ON s.track = t.id
         WHERE t.album = 'a'",
        "SELECT track || ' ' || sent FROM album_sending WHERE album = 'a'",
        "SELECT t.id || ' ' || s.sent FROM tracks AS t JOIN sending AS s ON s.track = t.id
         WHERE t.id = 'a1'",
    ] {
        let (db, tables) = open();
        let mut query = live(&db, sql);
        assert!(next(&mut query).await.is_empty(), "{sql}");
        send(&tables, "b1", 0);
        send(&tables, "b1", 1);
        end(&tables, "b1");
        assert!(!query.is_marked_for_rerun(), "{sql}");
        send(&tables, "a1", 0);
        assert_eq!(next(&mut query).await, ["a1 0"], "{sql}");
        // A row put as it is changes nothing.
        send(&tables, "a1", 0);
        assert!(!query.is_marked_for_rerun(), "{sql}");
        send(&tables, "a1", 7);
        assert_eq!(next(&mut query).await, ["a1 7"], "{sql}");
        end(&tables, "a1");
        assert!(next(&mut query).await.is_empty(), "{sql}");
        db.close();
    }
}

/// A count that reads no column of a host table, and a subquery that asks
/// whether it has a row for each track, run again as its rows come and go.
#[tokio::test]
async fn a_count_of_a_host_table_follows_its_rows() {
    let (db, tables) = open();
    let mut count = live(&db, "SELECT CAST(count(*) AS TEXT) FROM sending");
    let mut asks = live(
        &db,
        "SELECT CAST(EXISTS(SELECT 1 FROM sending AS s WHERE s.track = t.id) AS TEXT)
         FROM tracks AS t WHERE t.album = 'a'",
    );
    assert_eq!(next(&mut count).await, ["0"]);
    assert_eq!(next(&mut asks).await, ["0"]);
    send(&tables, "b1", 0);
    assert_eq!(next(&mut count).await, ["1"]);
    assert!(!asks.is_marked_for_rerun());
    send(&tables, "a1", 0);
    assert_eq!(next(&mut count).await, ["2"]);
    assert_eq!(next(&mut asks).await, ["1"]);
    db.close();
}

/// A host table of one row, read with no key, reruns the queries that read
/// the column that changed, and no others.
#[tokio::test]
async fn a_fact_with_no_key_reruns_only_the_queries_that_read_it() {
    let (db, tables) = open();
    let mut metered = live(
        &db,
        "SELECT CAST(coalesce(n.metered, 0) AS TEXT) FROM (SELECT 1) AS one
         LEFT JOIN network AS n",
    );
    let mut tracks = live(&db, "SELECT id FROM tracks ORDER BY id");
    assert_eq!(next(&mut metered).await, ["0"]);
    next(&mut tracks).await;
    tables.change(|rows| rows.put("network", vec![SqlValue::Integer(1), SqlValue::Integer(1)]));
    assert_eq!(next(&mut metered).await, ["1"]);
    assert!(!tracks.is_marked_for_rerun());
    db.close();
}

/// An upload starts while a write adds its track's first note, and the read
/// of the album's uploads and their notes runs again before the write
/// commits. The note joins the album only through the upload's row, which the
/// write didn't read: the write's lookups read the host rows as they are when
/// it commits, so the write reruns the read.
#[tokio::test]
async fn a_write_judges_against_the_host_rows_as_it_commits() {
    let (db, tables) = open();
    let mut query = live(
        &db,
        "SELECT s.track || ' ' || coalesce(n.body, 'none') FROM tracks AS t
         JOIN sending AS s ON s.track = t.id LEFT JOIN notes AS n ON n.track = s.track
         WHERE t.album = 'a'",
    );
    assert!(next(&mut query).await.is_empty());
    db.write_app(|writer| {
        writer.app_batch("INSERT INTO notes VALUES ('a1','loud')")?;
        send(&tables, "a1", 0);
        assert_eq!(futures_lite::future::block_on(next(&mut query)), ["a1 none"]);
        Ok(())
    })
    .unwrap();
    assert!(query.is_marked_for_rerun());
    assert_eq!(next(&mut query).await, ["a1 loud"]);
    db.close();
}

/// A read reads the host rows as they were when its snapshot was fixed,
/// though they change before its statement runs.
#[test]
fn a_read_takes_the_host_rows_as_its_snapshot_is_fixed() {
    let (db, tables) = open();
    send(&tables, "a1", 0);
    let count = || {
        db.read_app(|read| {
            send(&tables, "b1", 0);
            Ok::<_, QueryError>(
                read.query_row("SELECT count(*) FROM sending", [], |row| row.get::<_, i64>(0))?,
            )
        })
        .unwrap()
    };
    assert_eq!(count(), 1);
    assert_eq!(count(), 2);
    db.close();
}

/// A host table's rows come out in their key's order, and SQL can't write
/// them.
#[test]
fn sql_reads_a_host_table_in_key_order_and_cannot_write_it() {
    let (db, tables) = open();
    for track in ["c", "a", "b"] {
        send(&tables, track, 0);
    }
    let tracks = db
        .read_app(|read| {
            Ok::<_, QueryError>(
                read.query("SELECT track FROM sending", [], |row| row.get::<_, String>(0))?,
            )
        })
        .unwrap();
    assert_eq!(tracks, ["a", "b", "c"]);
    assert!(db.write("DELETE FROM sending").is_err());
    db.close();
}

/// A connection sqlive didn't open reads the rows as long as it lives, though
/// the host has dropped the tables.
#[test]
fn a_connection_keeps_the_rows_it_reads() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let tables = HostTables::new(&DECLARED).unwrap();
    send(&tables, "a1", 0);
    crate::Setup::on_test_connection(&connection, crate::NoPolicy, |setup| {
        setup.host_tables(&tables)
    })
    .unwrap();
    drop(tables);
    let track: String =
        connection.query_row("SELECT track FROM sending", [], |row| row.get(0)).unwrap();
    assert_eq!(track, "a1");
}

/// A compiled read of a host table runs with its shipped plan: a build
/// declares the table as an ordinary one, and the plan narrows the read as a
/// built one does.
#[tokio::test]
async fn a_compiled_read_of_a_host_table_runs_with_its_shipped_plan() {
    let probe =
        crate::SchemaProbe::open(&[DECLARED.join(";") + ";" + SCHEMA], &[], &[], &[]).unwrap();
    let statement = crate::tests::compiled(
        &probe,
        "SELECT track || ' ' || sent FROM album_sending WHERE album = 'a'",
    )
    .expect("sqlive analyzes the read");
    let (db, tables) = open();
    let mut query = db
        .subscribe(move |db| Ok(db.query_compiled(statement, [], |row| row.get::<_, String>(0))?));
    assert!(next(&mut query).await.is_empty());
    send(&tables, "b1", 0);
    assert!(!query.is_marked_for_rerun());
    send(&tables, "a1", 0);
    assert_eq!(next(&mut query).await, ["a1 0"]);
    db.close();
}
