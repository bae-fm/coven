# sqlive

sqlive runs live queries over a SQLite database. A live query returns its rows
once, then returns them again each time a write commits a change to something
the query read. A Rust program that shows a database's contents uses it to keep
the display current.

A `Database` opens one SQLite file, bundled through `rusqlite`, in WAL mode
with one writer connection and several read-only connections. A write is one
transaction on the writer. sqlive captures each row change the transaction
makes and, when it commits, judges each row's net change, from the row before
the transaction to the row at its end, to find the live queries whose results
could differ now: a row that ends as it began changes nothing. sqlive judges
the changes only against the live queries whose scopes the changed rows could
pass, which it finds by the values the rows hold, so a write's judging doesn't
grow with the number of live queries. Options can also start threads that
checkpoint the WAL and keep the query planner's statistics.

[SPEC.md](SPEC.md) specifies what sqlive does: what a write captures, which
writes run a live query again, and the upkeep it runs while a database is open.

## Example

This program opens a database, starts a live query that counts the rows of a
table, adds a row, and receives the new count.

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
    let handle = Handle::<HandleFailure>::new(bugs.clone(), Vec::new());

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

A panic inside a query or a write comes back as an `Error::Bug` that names where
the panic began. The live query's error type says whether it carries a bug
(`CarriesBug`), and a query whose error carries one ends.

## Checking SQL against a schema

A build tool can check SQL before the program runs. `SchemaProbe::open` makes a
database in memory, with no rows, that has run the schema's SQL texts in order.
SQLite refuses to prepare SQL that names a function or a collation it doesn't
know, so the caller lists the functions (`StandInFunction`) and the collations
that the schema's SQL uses by name, and the probe registers a stand-in for each.
`SchemaProbe::prepare` prepares a statement and returns its result columns, with
the table column each one is when it is one unchanged (`ColumnOrigin`), and the
parameters it reads. `SchemaProbe::read_report` returns the `ReadReport` that
says what a live query of the statement depends on in each table: the rows one
value fixes, the rows a lookup finds, or every row, with the reason. The probe
opens no file and runs no SQL but the schema's.

`sqlive::Row` is the row a query's closure maps, so code that reads a row names
no other crate's type.

## Policy

The program that embeds sqlive, the host, implements `Policy` to judge each
statement as SQLite prepares it, and passes one to every connection. `NoPolicy`
adds nothing to sqlive's own rules, which refuse app SQL that controls
transactions, runs a `PRAGMA`, attaches a database, loads an extension or
changes the schema outside a migration.

## Related crates

sqlive publishes two small crates beside it:

- `panic-as-error` turns a panic into an error value that names where the panic
  began.
- `windows-reserved-names` tells whether a file name is one that Windows opens
  as a device, such as `NUL` or `COM1`. On Windows, sqlive uses it to refuse a
  database path that holds such a name.
