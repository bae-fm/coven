# Local data

A coven store is one SQLite database on the device, plus the files its rows
carry. The app opens it through one handle, declares which tables sync, and
runs ordinary SQL. This page is about the store on one device: opening it,
declaring tables, keys, and writing and reading.

## Opening a store

Each store lives in its own directory under the app's
[`StoreLayout`](rustdoc:struct:coven::StoreLayout). The directory holds the
database, coven's copies of files, the cache, and the store's settings: its id
and name, this device's id, and its storage location. The app never picks a
device id; creating, restoring or joining a store does.

```rust
let layout = StoreLayout::new(app_dir);

// First launch: make the store. Its member keys go to custody before anything else.
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .create_store("Household", "Ana's phone", "Ana", |member| format!("person:{member}"), &[], |_, _| Ok(()))
    .await?;

// Later launches: open it again.
let store = layout.stores().await?[0].id;
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .open(store, |progress| show_progress(progress))
    .await?;
```

Opening creates coven's own tables when the database holds none, then runs the app's pending migrations
([Schema evolution](/docs/schema-evolution)), then checks the schema against
the declared tables. A large migration, or rebuilding the indexes after a new
version of SQLite or of the app's own orders and functions, takes a while, so
`open` calls the app back with each step and how much of it is done, a
[`Progress`](rustdoc:struct:coven::Progress) of
[`OpenStep`](rustdoc:enum:coven::OpenStep), for a progress bar. Most opens have
no such step and call it never. It needs no network and doesn't start syncing:
the sync state of a store without storage set up is not synced, and one with
storage keeps how its last pass ended, with a queued file waiting for sync to
start ([Sync](/docs/sync-model#status-and-pending-work)).

One writable handle can have a store open at a time; a second open, in this
process or another, fails with `StoreLockError::AlreadyOpen`. Another process,
such as a widget, can read alongside the app with `open_read_only`, which
returns a [`CovenReadHandle`](rustdoc:struct:coven::CovenReadHandle). It runs
no migration and refuses a database whose schema needs migrating or is newer
than its own; while it is open, the store can't be deleted.

`handle.close()` stops syncing, closes every connection and releases the lock.
`handle.delete_store(IfUnsent::Refuse)` removes the store from this device: it
refuses while writes or files wait to upload, publishes this device's removal,
then deletes this device's own keychain entries and its directory. The
restore code and S3 key in iCloud Keychain stay for the person's other devices
([Keys](/docs/keys)). `IfUnsent::Discard` drops the
unsent work instead. For a store that no longer opens,
`Coven::discard_store(&store_dir)` deletes it without opening it. The copy in
storage is never touched.

### SQLite settings

App SQL can't run a `PRAGMA`, so the app chooses SQLite's settings on the
builder, as a [`SqliteSettings`](rustdoc:struct:coven::SqliteSettings). Coven
applies them to every connection a handle opens to the store's database, the
writer and the readers alike, as each one opens. A setting left `None` keeps
SQLite's default, except `checkpoint_fullfsync`, which coven turns on for
macOS and iOS, and `journal_size_limit`, 4 MiB. On macOS and iOS each
checkpoint flushes the drive's own cache, so a power loss can't damage the
database file; commits keep their plain `fsync`, so a power loss can still
take the latest few commits.

Coven checkpoints the WAL itself, never inside a commit: once the WAL holds
1,000 pages not yet copied, or `wal_autocheckpoint`, a thread of the store's own
copies it on a connection of its own and lets the next commit start the WAL over. A
live query rerunning after every commit no longer makes each commit wait for a
checkpoint it can't finish. `WalAutocheckpoint::Off` leaves checkpoints to the
app.

```rust
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .sqlite_settings(SqliteSettings {
        cache_size: Some(CacheSize::Kibibytes(64 * 1024)),
        wal_autocheckpoint: Some(WalAutocheckpoint::Off),
        ..SqliteSettings::default()
    })
    .open(store, |progress| show_progress(progress))
    .await?;

// After a large import, let the next writes reuse the WAL from its start.
let checkpoint = handle.checkpoint(CheckpointMode::Restart).await?;
```

The app can set the busy timeout, cache and memory-map sizes, where temporary
tables live, sorting threads, how large the WAL file stays and when coven
checkpoints it, secure delete, full fsync on Apple platforms, and the query
planner's automatic indexes and `ANALYZE` limit. The page size and auto-vacuum
mode apply only when the store's database is created; an existing store keeps
its own. `handle.sqlite_settings()` reads back what SQLite has in effect, and
`checkpoint` returns what SQLite reports: whether it was blocked, how many
frames the WAL holds, and how many it copied.

Coven gathers the query planner's statistics itself, with `PRAGMA optimize`,
as a store opens, after migrating, and as it closes. A run analyzes the tables
with no statistics or whose size changed tenfold, reading at most 2,000 rows of
each index, so it costs a few milliseconds the first time and almost nothing
after. While the store stays open, coven checks each time its writes have
changed 1,000 rows, and runs again once a table reaches 100 rows or ten times
the size its statistics count, so a table that fills in one session doesn't
plan blind until the next open. An app write can't run `ANALYZE`; a migration
can.

The settings coven depends on have no field, so the app can't change them: WAL
mode, `synchronous = FULL`, normal locking, foreign keys, recursive triggers
and an untrusted schema. The API reference lists every SQLite setting and why
each one is the app's or coven's.

### Collations

SQLite orders text by its own three collations, `BINARY`, `NOCASE` and
`RTRIM`. For any other order, such as listing "Note 9" before "Note 10", the
app registers a collation on the builder: a name, a version, and a function
that compares two strings. Its SQL then names it with `COLLATE`, in a query or
in the schema.

```rust
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .collation("natural_order", 1, natural_order)
    .open(store, |progress| show_progress(progress))
    .await?;

// A migration can declare an index in that order:
//   CREATE INDEX notes_by_title ON notes(title COLLATE natural_order)
// The build script names "natural_order" among the setup's collations.
coven::read!(writer NaturalTitles, r#"
    SELECT n.title FROM notes AS n ORDER BY n.title COLLATE natural_order
"#);
let titles = handle
    .read(|sql| Ok::<_, CovenError>(sql.fetch(&NaturalTitlesParams {})?))
    .await?;
```

Coven registers each collation on every connection it opens to the store's
database, before any app SQL runs there: the writer, which also migrates and
applies other devices' writes, the readers, and every connection of a
read-only handle. Every builder that opens the store, a
widget's read-only one included, registers the same collations.

An order is part of the store's schema. At one version, the function must be
deterministic, comparing the same two strings the same way on every device and
in every app version, and a total order. An index keeps its entries in the
order the function gave when they were written, so every change to the order
takes a new version. When a writable handle opens with a version the store's
indexes weren't built with, coven rebuilds every index in that order, before
the app's migrations, and records the version; an open at the same versions
rebuilds nothing. A read-only handle can't rebuild, and fails with
`DbError::VersionNotRebuilt` until a writable one has. Key and unique columns
of synced tables still compare by bytes.

A version can't change while a CHECK or generated column, or a unique index of
a synced table, uses the order: those decide which rows every device keeps, or
keep what the old order computed, and a rebuild can't make devices at different
versions agree. The open fails with `DbError::VersionChangeRefused` and changes
nothing. To change such an order, register it under a new name, migrate those
declarations to it, and keep registering the old name at its version.

A name that is one of SQLite's own fails the open with
`DbError::ReservedCollation`, and a name given twice, ignoring case, with
`DbError::DuplicateCollation`. A name that is an SQL keyword, such as
`natural`, has to be quoted: `COLLATE "natural"`. A panic in the function ends
the process, because SQLite has no way for a comparison to fail.

### Functions

SQLite's `lower`, `LIKE` and `NOCASE` fold only ASCII letters, so a search for
"école" misses a note titled "École". The app can give SQLite a function of its
own instead: a name, how many arguments it takes, whether it is deterministic
and at which version, and the Rust function that computes it. Its SQL then calls it by name, in a
query or in the schema.

```rust
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .function("fold", 1, Determinism::Deterministic { version: 1 }, |arguments| {
        Ok(arguments.get::<String>(0)?.to_lowercase())
    })
    .open(store, |progress| show_progress(progress))
    .await?;

// A migration can declare an index on it:
//   CREATE INDEX notes_folded ON notes(fold(title))
// The build script names `fold` among the setup's functions.
coven::read!(writer TitlesLike(wanted: String), r#"
    SELECT n.title FROM notes AS n WHERE fold(n.title) = fold(:wanted)
"#);
let wanted = TitlesLikeParams { wanted: "école".to_owned() };
let matches = handle
    .read(move |sql| Ok::<_, CovenError>(sql.fetch(&wanted)?))
    .await?;
```

Coven registers each function on the same connections as the collations, and
every builder that opens the store has to register the same ones. The function
returns any value that converts into an SQLite value, such as a `String`, an
`i64` or an `Option` of one, or an error that fails the statement calling it.
Coven marks every app function safe for triggers and views, which only the
app's migrations declare.

Only a deterministic function can appear in an index, a CHECK or a generated
column. One the schema uses is part of the store's schema, like a collation: at
its version it must give the same result for the same arguments on every device
and in every app version. Unicode lowercasing changes when a newer Unicode
version gives a letter a lowercase form, so register the function at a new
version then: a writable open rebuilds exactly the indexes that call it, and a
CHECK, generated column or synced unique index that calls it refuses the new
version, as for an order. A function that refuses some value with an error also stops this device from
applying another device's write that holds it, so a function the schema uses
should return a value for every argument.

A name SQLite or coven already has, such as `lower` or `coven_applying`, or
one starting with `coven_` or `_coven_`, fails the open with
`DbError::ReservedFunction`, and a name given twice, ignoring case, with
`DbError::DuplicateFunction`. A deterministic function takes at least one
argument; one that takes none is a constant, better written as a literal, and
fails the open with `DbError::ConstantFunction`. `regexp` is the app's to define: SQLite has
none, and `title REGEXP 'ban'` calls `regexp('ban', title)`. A panic in the
function ends the process.

## Declaring synced tables

The app tells coven which tables sync with a
[`SyncedTable`](rustdoc:struct:coven::SyncedTable) for each one. Every other
table stays on the device: window positions, a search index, per-device
settings.

```rust
fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid),
        SyncedTable::new("tags", RowIdentity::SharedKey),
        SyncedTable::new("note_tags", RowIdentity::SharedKey)
            .key_columns(["note_id", "tag_id"]),
    ]
}
```

A declaration can also say how the table's rows get their
[audience](/docs/circles#declaring-audiences) (`audience_column`,
`audience_from`), which of its columns reference a [file](/docs/files)
(`file_column`),
that only each row's creator may change it ([`owner_only`](/docs/merge#owner-tables)),
and which of its triggers are [shared](/docs/merge#triggers)
(`shared_trigger`). The schema itself is the app's: coven adds no columns to
synced tables.

### Keys

Every synced row is named by its table and its primary key, on every device.
Two devices that insert the same key insert the same row. So each synced table
declares what kind of key it has:

- **Independent** (`RowIdentity::IndependentUuid`): each new row gets a random
  UUID, so rows made on different devices never collide. Ana and Ben each
  create a note offline; they get two notes.
- **Shared** (`RowIdentity::SharedKey`): the app derives the key from what makes
  the row unique, so the same thing made on two devices is one row. Ana and
  Ben, both offline, each add the tag "urgent". Both derive the key `urgent`,
  their inserts are one row, and their cells merge.

Either kind can span several columns with `key_columns`; without it, the key
is the one column `id`. An independent key over several columns needs a UUID in
one of them.

Bind independent keys with [`UuidText`](rustdoc:struct:coven::UuidText), which
writes a UUID as canonical lowercase text. Inserting anything else, including
an uppercase UUID, fails with `DbError::KeyNotUuid`. A bare `uuid::Uuid` would
bind as a blob.

```rust
let note = UuidText(Uuid::now_v7());
handle.write(move |sql| {
    sql.run(&AddNoteParams { id: note, title: "Paint colors".to_owned() })?;
    sql.run(&AddTagParams { tag: "urgent".to_owned() })?;
    sql.run(&TagNoteParams { note, tag: "urgent".to_owned() })?;
    Ok::<_, CovenError>(())
}).await?;
```

Changing a row's primary key is a delete of the old row and an insert of the
new one, on every device ([Merge](/docs/merge#key-changes)).

Coven refuses, when the database opens and after every migration, keys that
can't mean the same thing on every device. The error names the table:

| Refused | Why | Error |
| --- | --- | --- |
| An integer rowid key, or any key SQLite picks | two devices would pick the same number for different rows | `SchemaError::GeneratedPrimaryKey` |
| A synced table without a primary key | coven can't tell which row a change belongs to | `SchemaError::NoPrimaryKey` |
| A key column that allows NULL, in a synced or a device-local table | a key with a NULL in it names no row, and live queries learn which local rows a write changed by their keys | `SchemaError::NullableKey` |
| An independent key with no text column for its UUID | | `SchemaError::IndependentKeyNotUuid` |
| A collation other than `BINARY` on a key, unique or foreign-key column | under `COLLATE NOCASE`, SQLite would hold "Urgent" and "urgent" as one row that coven treats as two | `SchemaError::KeyCollation` |
| Declared key columns that don't match the table's primary key | | `SchemaError::KeyColumns` |

The rules for foreign keys, unique constraints, CHECKs and triggers are on the
[Merge](/docs/merge#the-rules-sql-brings-with-it) page;
[Constraints](/docs/constraints) lists what can't be expressed at all.

## Compiled statements

An app writes its queries as `coven::read!`, `coven::write!` and
`coven::fragment!` calls. Its build script checks each one against the app's
schema and coven's objects, so a column the schema doesn't have fails the build,
and each read ships with what its live query needs to know about which writes
rerun it. A call has a name, its typed parameters and its SQL:

```rust
coven::write!(AddNote(id: coven::UuidText, title: String), r#"
    INSERT INTO notes (id, title) VALUES (:id, :title)
"#);
coven::write!(AddTag(tag: String), r#"
    INSERT INTO tags (id) VALUES (:tag) ON CONFLICT DO NOTHING
"#);
coven::write!(TagNote(note: coven::UuidText, tag: String), r#"
    INSERT INTO note_tags (note_id, tag_id) VALUES (:note, :tag)
"#);
coven::read!(writer NoteList, r#"
    SELECT n.id, n.title FROM notes AS n ORDER BY n.title
"#);
```

The build writes `NoteListParams` and `NoteListRow` for the read, and a
`…Params` struct for each write. A context runs a statement by that type: a read
with `fetch` or `fetch_row` (from the `SqlRead` trait), a write with `run`, or
`run_returning` for one with a RETURNING clause (from `SqlWrite`). A read marked
`writer` runs on `CovenHandle`; one without the mark runs on a
[`CovenReadHandle`](rustdoc:struct:coven::CovenReadHandle). A column the app
computes needs a type in the call's header, as in
`coven::read!(writer NoteCount { n: i64 }, …)`.

The app's crate depends on `coven`, and its build script on `coven-schema` and
on `sqlive-compose` with its `compiler` feature; neither compiles an async
runtime:

```toml
[dependencies]
coven = { git = "https://github.com/bae-fm/coven" }

[build-dependencies]
sqlive-compose = { git = "https://github.com/bae-fm/coven", features = ["compiler"] }
coven-schema = { git = "https://github.com/bae-fm/coven", features = ["compiler"] }
```

The script hands the compiler the schema. `coven_schema::compiler_setup()` gives
coven's part, and the script appends the SQL of each migration in order, with the
app's own functions and collations that the schema or a statement names. It names
`coven` as the crate the calls are written through. Copy
[`examples/notes/build.rs`](https://github.com/bae-fm/coven/blob/main/examples/notes/build.rs),
which does all of it. What a call may say, such as fragments that several reads
share, is in
[sqlive-compose's spec](https://github.com/bae-fm/coven/blob/main/crates/sqlive-compose/SPEC.md).

### SQL strings

Every context also takes SQL as a string, with `query`, `query_row`, `execute`
and `execute_batch`. Nothing checks a string before it runs. It suits a
statement the app builds as it runs, such as a table whose columns the person
picks:

```rust
let sql = format!("SELECT {} FROM notes ORDER BY title", columns.join(", "));
let table = handle
    .read(move |context| {
        Ok::<_, CovenError>(context.query(&sql, [], |row| row.get::<_, String>(0))?)
    })
    .await?;
```

Migrations run strings too: each runs on the schema of its own version.

## Writing

A write is one SQLite transaction through `handle.write`. Its closure gets a
[`CovenWrite`](rustdoc:struct:coven::CovenWrite), which runs the app's
statements, and coven's own steps, such as
retrying an operation, which commit or roll back with the app's statements. If
the closure returns an error, or a statement or a step fails, the whole
transaction rolls back, even when the closure drops the error and returns
success (`DbError::StatementFailed`, `DbError::StepFailed`). A conflict the app
expects belongs in its SQL, with `ON CONFLICT` or `INSERT OR IGNORE`.

When a write commits, coven records in the same transaction:

- the rows as they now stand;
- a *write record* for upload: each row change with the columns it touched and
  their old and new values, the write's device and number, its timestamp, the
  schema version it was made with, and how far this device had read the
  store's sequence (`read_through`);
- the facts later [owner checks](/docs/merge#owner-tables) need.

Here is the write record for a write that fixes a note's title and deletes a
tag:

```
ana-phone, write 3, 2026-10-02 12:00:00.000 #0
  read_through: slot 20
  notes  row 42  update  title: "Grocry list" → "Grocery list"
  tags   "errands"  delete
```

Writing needs no key and no storage. The record waits on the device unencrypted
and unsigned; it is encrypted and signed when it is sent
([Sync](/docs/sync-model#a-sync-pass)). Ana can use the app before she has set
storage up at all.

A commit never waits for the network, but it can be refused for local reasons:
a SQL constraint or trigger, a write into a circle the member isn't in, a
change to another member's owned row, or an audience in the middle of a
reload. The [`DbError`](rustdoc:enum:coven::DbError) names each. Coven also
refuses SQL that would get around it: transaction control, `PRAGMA`, `ATTACH`
or loading an extension (`DbError::StatementForbidden`), and anything that
reads or changes coven's own objects (`DbError::InternalTable`). App tables,
indexes and triggers may not start with `_coven_` or `coven_`.

**Rowids.** In a table whose rowid isn't its primary key, the app can read the
rowid but can't change it once the row exists. The rowid is SQLite's private
address for the row: coven doesn't sync it, and `VACUUM` can renumber it.

**A rollback that fails.** If a transaction fails and rolling it back fails
too, a connection may still be inside that transaction. Every later call on
that handle, read-only or not, then fails with `DbError::RollbackFailed` until
the store is reopened.

## Reading

`handle.read` runs a closure on one of several read-only connections, in one
transaction, so every statement in it sees the same committed state. Reads run
alongside each other and alongside the writer, and a read after an awaited
write sees that write. SQLite itself refuses writes on these connections.

```rust
coven::read!(writer EveryTitle, r#"SELECT n.title FROM notes AS n"#);

let titles: Vec<String> = handle
    .read(|sql| Ok::<_, CovenError>(sql.fetch(&EveryTitleParams {})?))
    .process(|rows| {
        let mut titles: Vec<String> = rows.into_iter().map(|row| row.title).collect();
        titles.sort();
        Ok(titles)
    })
    .await?;
```

`process` runs on a separate worker after the read has released its
connection, so expensive work doesn't hold one. Fetch every database input in
the read; `process` gets owned values and no SQL.

### Live queries

`handle.subscribe` runs a query once, then again whenever a write commits that
changes rows it read, whether the write was local or arrived from another
device. Coven records the tables, columns and keys the query reads, so an
unrelated write doesn't rerun it. `next_blocking()`, called on a thread that
may block, returns the current result first, then each changed one; an async
caller awaits `changed()` and calls `run_changed()` on such a thread. An error is
a result too and doesn't end the query, except the one saying the store closed
or its handle failed: after it, `next_blocking()` returns `None`, so a loop over
it ends. Dropping the `LiveQuery` ends it.

```rust
let mut notes: LiveQuery<_, CovenError> =
    handle.subscribe(|sql| Ok(sql.fetch(&NoteListParams {})?));
while let Some(Ok(rows)) = notes.next_blocking() {
    show_notes(rows);
}
```

`handle.subscribe_reconfigurable(request, query)` takes a request, such as a
page number or a search term, that the app can replace through
`requests().set(..)` without starting a new subscription. Each result says
which request and revision it answers, and whether a new request, a write, or
both caused it.

A screen's read is the live query and nothing else. Everything the screen shows
comes from SQL: the rows, and beside them any count, total, group, header, sum
or facet, in the same query or in a live query of its own. Keep nothing from one
result to use in the next, don't walk the rows in a second pass beside the
query, and don't write a value or replace a request only to make the query read
again: it reruns because what it read changed, and a request changes because the
person asked for something else. When a query is slow, the fix is in SQL (the
query, an index, or a table triggers keep). A value that changes many times a
second and lives in no table, such as a transfer's progress, goes to the view on
a stream of its own, not into a live query. State that lives only on screen, such as
hover, focus or an open popover, stays in the view and never goes into SQL.

A long list's count is never in the same live query as its rows. A count
changes with every insert, so a query holding both reruns, and rereads every
window it holds, on every write. Subscribe each window the screen shows as its
own live query, with the window's first key as its request, and the count as
another:

```rust
// A window: the 100 notes from one title on.
coven::read!(writer NotesFrom(from: String), r#"
    SELECT n.id, n.title FROM notes AS n
    WHERE n.title >= :from ORDER BY n.title LIMIT 100
"#);
coven::read!(writer NoteCount { n: i64 }, r#"SELECT count(*) AS n FROM notes AS counted"#);

let window = |from: &String, sql: SqlReadContext<'_>| {
    Ok::<_, CovenError>(sql.fetch(&NotesFromParams { from: from.clone() })?)
};
let top = handle.subscribe_reconfigurable(String::new(), window);
// The count, read by itself.
let count: LiveQuery<_, CovenError> =
    handle.subscribe(|sql| Ok(sql.fetch_row(&NoteCountParams {})?.n));
```

A write that adds a row reruns `count`, and reruns the window only when the row
sorts at or before the window's last row: when a run returns all 100 rows, a
note that sorts after the last of them can't appear in the window. Each run sets
that bound again from its own last row, so a note inserted or deleted inside the
window moves the bound to the window's new last row. A note equal to the last
row on every ORDER BY column counts as inside, since SQLite could sort it before
the last row. A run that returns fewer than 100 rows already holds every note
after its start, and reruns for a change to any of them.

Coven ignores changes to rows that sort after a window's last row when:

- the statement is one SELECT, with an `ORDER BY` and a `LIMIT` and no
  `OFFSET`;
- each `ORDER BY` term names a column of the same table, with no `COLLATE` and
  no `NULLS FIRST` or `NULLS LAST`, and the column is stored and uses BINARY
  collation;
- that table isn't the right side of a `LEFT JOIN`;
- the statement returns each `ORDER BY` column as a result column listed before
  any `*`, since coven reads the last row's values from the result;
- the SELECT has no `GROUP BY` and calls no aggregate or window function in its
  result columns.

`ORDER BY` terms may be ascending or descending, in any mix. Any other window,
such as one ordered by an expression or with an `OFFSET`, reruns for a change to
any row its `WHERE` takes in: `LIMIT 100 OFFSET 200` reruns for any change at
all. A jump to a far position, such as a scrollbar drag, reads the key at that
position once and makes it the window's request.

Coven's own state has ready-made live queries with the same results as their
one-shot reads: `subscribe_members`, `subscribe_lost_values`,
`subscribe_pin_progress` and others. They all work offline. The upload queue
and pending records are live queries over coven's views instead (below).

### Coven's state in SQL

A list often depends on coven's own state: albums still uploading come first,
a filter keeps the rows this device has pinned, a person's row shows whether
they are still a member. The app's SQL reads that state through views and
tables like these, so the list stays one query over the database:

| View | One row per | Columns |
| --- | --- | --- |
| `coven_files` (a table) | file this device has a record of | `id`, `audience`, `size`, `hash`, `coordinates`, `namespace` |
| `coven_uploads` | file waiting in this device's upload queue | `file`, `position`, `paused`, `metered`, `retry_at`, `pending`, `bytes_confirmed`, `bytes_total`, `sending`, `move` |
| `coven_sending` | queued file this process is sending now | `file`, `position` |
| `coven_upload_pause` | store, always exactly one row | `paused` |
| `coven_cache_bytes` | namespace the app has declared | `namespace`, `budget`, `cached`, `pinned`, `pinning` |
| `coven_library_bytes` (a table) | part of the store's own files | `part`, `bytes` |
| `coven_archive_bytes` (a table) | archive a reset kept | `archive`, `bytes` |
| `coven_network` | store, always exactly one row | `usable`, `metered`, `unmetered_only` |
| `coven_sync_state` | store, always exactly one row | `state`, `last_synced`, `pending`, `detail` |
| `coven_upload_queue` (a table) | file waiting in this device's upload queue | `file`, `position` |
| `coven_pins` (a table) | file this device keeps pinned | `file` |
| `coven_pin_requests` (a table) | file a switch to the cloud asked this device to keep whole, not yet pinned | `file` |
| `coven_moves` (a table) | file of a move to the cloud still moving or failed | `move`, `state`, `file` |
| `coven_pending` | pending record | `record`, `file`, `position`, `cause` |
| `coven_invites` | invite this device made that hasn't ended | `invite`, `operation` |
| `coven_members` | member, former ones included | `member`, `role`, `removed`, `is_self` |
| `coven_devices` | device a member registered | `device`, `member`, `name`, `state`, `is_self` |

The storage views and tables count every byte coven keeps on the device
outside the app's own files. `coven_cache_bytes` gives the cache's bytes, one
row per namespace. `coven_library_bytes` gives the size in bytes of each part
of the store's own files, one row per part: `database` and `inputs` give the
two database files as their last checkpoints left them, without their WALs,
`copies` gives coven's copies of app files, and `rebuild` gives a rebuild's
store while one runs. `coven_archive_bytes` gives the size in bytes of each
old installation that a reset kept. A settings screen sums them in one live
query:

```rust
coven::read!(writer DeviceBytes { bytes: i64 }, r#"
    SELECT (SELECT coalesce(sum(c.cached + c.pinned + c.pinning), 0) FROM coven_cache_bytes AS c)
         + (SELECT sum(l.bytes) FROM coven_library_bytes AS l)
         + (SELECT coalesce(sum(a.bytes), 0) FROM coven_archive_bytes AS a) AS bytes
"#);

let device_bytes: LiveQuery<_, CovenError> =
    handle.subscribe(|sql| Ok(sql.fetch_row(&DeviceBytesParams {})?.bytes));
```

The file views and tables name a file by its id, the id the app's file
reference columns hold, so a join reads `u.file = p.file`:

```rust
coven::read!(writer UploadingFirst, r#"
    SELECT a.title FROM albums AS a
    ORDER BY EXISTS(SELECT 1 FROM photos AS p JOIN coven_uploads AS u
                    ON u.file = p.file
                    WHERE p.album_id = a.id) DESC, a.title
"#);

let uploading_first: LiveQuery<_, CovenError> =
    handle.subscribe(|sql| Ok(sql.fetch(&UploadingFirstParams {})?));
```

Each view is a query over coven's own tables, so it is never out of date, and a
live query that reads one reruns when coven changes what it read: an upload
finishing, a pin, a member removed. The views are read-only, and coven's tables
themselves stay out of reach. `pending` and `record` read as a
`PendingRecord`, `member` as a `MemberId`, `device` as a `DeviceId`, and `file`
and `id` as a `FileId`. The API reference lists every column.

`coven_sync_state` is the sync state a status line shows: its kind in `state`,
such as `synced` or `storage_full`, when sync last finished, and the whole
state in `detail`, which reads as a `SyncState` with what it waits for and its
cause. Coven computes it in the read, from the facts its tables hold, with the
same Rust code that coven's tests check against its formal model of sync,
written in the Lean proof language. A screen that lists notes under its sync
line reads both in one live query, so the line never lags the list:

```rust
coven::read!(writer SyncLine { state: String }, r#"
    SELECT s.state FROM coven_sync_state AS s
"#);

let screen: LiveQuery<_, CovenError> = handle.subscribe(|sql| {
    let state = sql.fetch_row(&SyncLineParams {})?.state;
    Ok((state, sql.fetch(&NoteListParams {})?))
});
```

`coven_pins` is a table rather than a view so the app can follow it: a local
trigger declared `AFTER INSERT` or `AFTER DELETE ON coven_pins` keeps a count
of each album's pinned tracks in a device-local table, and "how many albums
aren't kept offline" becomes an indexed read instead of a probe per track. The
trigger may write only local tables, since a pin is this device's alone, and
it reads only what the app's SQL may.

`coven_upload_queue` is a table for the same reason. Local triggers on it keep
each album's queued count, first place and bytes in the app's own table, which
an outbox reads by index however long the queue is; grouping `coven_uploads`
by album reads every queued file, with what each waits for.

`coven_pin_requests` and `coven_moves` are tables for the same reason. A
trigger on `coven_pin_requests` follows each file still to be kept whole after
its switch to the cloud. One on `coven_moves` follows each file of a move as
the move starts, as it fails, which is when the app can offer to cancel it,
and as a cancel or a switch ends it. `move` reads as a
`MoveId`, the id `cancel_move` takes.

Local triggers can follow `coven_files` too, the records themselves. When
another device finishes uploading a track, its write switches the track's
record to the cloud, changing only `coordinates` and `hash`; no row of the
app's own tables changes. A trigger declared `AFTER UPDATE OF coordinates ON
coven_files` runs as that write applies here, as well as on the uploader, so
an album list can keep each album's count of stored tracks in a local table.
Triggers on its INSERT and DELETE follow each record made and deleted, here
and on other devices. Adding or dropping one changes nothing that syncs, so
its migration is an addition.

Whether a file is being sent right now lasts only as long as the process, so
nothing stores it: `coven_uploads.sending` and the `coven_sending` view read it
from the upload worker's memory. A live query that reads either runs again
each time an upload starts or ends, and a reopened store shows every file
waiting. `coven_sending` holds one row per upload in flight, so counting an
album's files being sent takes a lookup per file, however long the queue:

```rust
coven::read!(writer SendingOfAlbum(album: String) { n: i64 }, r#"
    SELECT count(*) AS n FROM coven_sending AS s JOIN photos AS p ON s.file = p.file
    WHERE p.album_id = :album
"#);

let of_album = SendingOfAlbumParams { album };
let sending: LiveQuery<_, CovenError> =
    handle.subscribe(move |sql| Ok(sql.fetch_row(&of_album)?.n));
```

The bytes the provider has confirmed do last, so `coven_uploads` gives them as
`bytes_confirmed` and `bytes_total`, NULL for a file sent in one request: an
album's upload bar is `SUM(bytes_confirmed)` over `SUM(bytes_total)` for its
photos. `coven_pending` gives each pending record's error as `cause`, the
text coven writes with the record; a retry that fails the same way with a
different error changes it, and a live query runs again for it. Opening clears
every cause, so a record from an earlier run shows none until its failure
happens again.
