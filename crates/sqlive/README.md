# sqlive

sqlive gives SQLite live queries. A live query runs once and returns its rows,
then runs again whenever a write commits a change to something the query read.
It is a Rust library for programs that show a database's contents and must
keep the display current as the database changes.

## What sqlive does

- A `Database` opens a SQLite file in WAL mode with one writer connection and
  several read-only connections beside it.
- A write is one transaction. sqlive captures each row change the transaction
  makes, and when it commits, sqlive judges the changes against every live
  query.
- A live query records what its statements read: the tables, the columns and
  the rows. A commit that changes none of them does not run the query again.
- A panic in a query or a write comes back as an error that names where the
  panic began, instead of unwinding through the caller.
- Options start upkeep threads that checkpoint the WAL and keep the query
  planner's statistics.
- A `Policy` that your program supplies judges each statement as SQLite
  prepares it, so the program can refuse statements.

SQLite comes bundled through `rusqlite`.

## Example

The program below opens a database, starts a live query that counts a table's
rows, adds a row, and receives the new count.

```rust
use panic_as_error::{Bug, BugSink, BugSinkRef, CarriesBug};
use sqlive::{
    Database, Error, Handle, HandleFailure, LiveQuery, NoPolicy, Options, PolicyError,
    ReconfigurableLiveQuery, Recorded, SqliteSettings, WholeViews,
};
use std::sync::Arc;

// A live query's error type holds sqlive's error and says whether it carries a bug.
#[derive(Debug)]
struct QueryError(PolicyError<NoPolicy>);

impl From<PolicyError<NoPolicy>> for QueryError {
    fn from(error: PolicyError<NoPolicy>) -> Self {
        Self(error)
    }
}

impl CarriesBug for QueryError {
    fn bug(&self) -> Option<&Bug> {
        match &self.0 {
            Error::Bug(bug) | Error::HandleFailed(bug) => Some(bug),
            _ => None,
        }
    }
}

// Where a bug goes when no caller waits for it. A real program reports it.
struct IgnoreBugs;

impl BugSink for IgnoreBugs {
    fn report(&self, _bug: &Bug) {}
}

fn main() {
    let directory = std::env::temp_dir().join(format!("sqlive-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let bugs: BugSinkRef = Arc::new(IgnoreBugs);
    let handle = Handle::<HandleFailure>::recording_all(bugs.clone());

    // Open the database at a path, creating its tables with the first connection.
    let (database, ()) = Database::open_with(
        &directory.join("library.sqlite"),
        handle.clone(),
        Options {
            settings: SqliteSettings::default(),
            readers: 2.try_into().unwrap(),
            prepare_on_reader: false,
            whole_views: Arc::new(WholeViews::new(Vec::new())),
            policy: &|| NoPolicy,
            setup: &|_, _| Ok(()),
            capture: Some(Default::default()),
            checkpoint_pages: None,
            statistics: false,
        },
        |writer| writer.batch("CREATE TABLE albums (id INTEGER PRIMARY KEY, title TEXT NOT NULL)"),
    )
    .unwrap();
    let database = Arc::new(database);

    // A live query that counts the albums.
    let reader = Arc::clone(&database);
    let mut count: LiveQuery<i64, QueryError> = LiveQuery::new(ReconfigurableLiveQuery::new(
        (),
        Arc::new(move |(), commits| {
            reader.reader().host(|reader| {
                reader.live_read(
                    &mut |reads, lookups| commits.finish_judging(reads, lookups),
                    |reader| {
                        reader
                            .app_query_row(Recorded::Yes, "SELECT count(*) FROM albums", [], |row| {
                                row.get(0)
                            })
                            .map_err(|error| QueryError(error.into()))
                    },
                )
            })
        }),
        QueryError,
        &handle,
        bugs,
    ));

    // The first call answers with the count now.
    assert_eq!(count.next_blocking().unwrap().unwrap(), 0);

    // A write that adds an album changes what the query read, so the query runs again.
    database
        .host(|writer| {
            writer.write_with(
                |writer, _operations| {
                    let _write = writer.write_scope();
                    writer
                        .app_batch("INSERT INTO albums (title) VALUES ('Album Title')")
                        .map_err(PolicyError::<NoPolicy>::from)
                },
                |_writer, _operations, ()| Ok(()),
            )
        })
        .unwrap();
    assert_eq!(count.next_blocking().unwrap().unwrap(), 1);

    handle.close();
    drop(count);
    Arc::into_inner(database).unwrap().close();
    std::fs::remove_dir_all(directory).unwrap();
}
```

## What a live query reruns on

sqlive parses each statement's SQL into the conditions that narrow each table
it reads, and a statement that reads one table depends only on the rows its
`WHERE` allows. A column compared with a constant or a bound value by `=`, `<`,
`<=`, `>`, `>=` or `IN`, and conditions like these joined by `AND` and `OR`,
narrow the rows. So `SELECT title FROM tracks WHERE release_id = ?1` does not
run again when a write changes another release's tracks. A statement with a
`LIMIT` narrows further once a run returns as many rows as the limit.

A one-shot read records none of this, because nothing runs it again.

## Policy

Every connection has a policy, which sqlive asks about each statement as SQLite
prepares it. `NoPolicy` adds nothing to sqlive's own rules. sqlive refuses app
SQL that controls transactions, runs a `PRAGMA`, attaches a database, loads an
extension, or changes the schema outside a migration.

## Crates

sqlive uses two small crates that it publishes beside it:

- `panic-as-error` turns a panic into an error value that names where the panic
  began.
- `windows-reserved-names` tells whether a file name is one that Windows opens
  as a device, such as `NUL` or `COM1`. sqlive uses it on Windows to refuse a
  database path that holds such a name.

## License

Apache-2.0.
