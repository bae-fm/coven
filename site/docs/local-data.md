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
    .create_store("Household")
    .await?;

// Later launches: open it again.
let store = layout.stores().await?[0].id;
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .open(store)
    .await?;
```

Opening migrates coven's own tables, then runs the app's pending migrations
([Schema evolution](/docs/schema-evolution)), then checks the schema against
the declared tables. It needs no network and doesn't start syncing: a store
with storage set up opens as `Stopped`, one without as `Disconnected`
([Sync](/docs/sync-model#status-and-pending-work)).

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
macOS and iOS. There each checkpoint flushes the drive's own cache, so a
power loss can't damage the database file; commits keep their plain `fsync`,
so a power loss can still take the latest few commits.

```rust
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .sqlite_settings(SqliteSettings {
        cache_size: Some(CacheSize::Kibibytes(64 * 1024)),
        wal_autocheckpoint: Some(WalAutocheckpoint::Off),
        ..SqliteSettings::default()
    })
    .open(store)
    .await?;

// After a large import, let the next writes reuse the WAL from its start.
let checkpoint = handle.checkpoint(CheckpointMode::Restart).await?;
```

The app can set the busy timeout, cache and memory-map sizes, where temporary
tables live, sorting threads, how large the WAL file stays and when commits
checkpoint it, secure delete, full fsync on Apple platforms, and the query
planner's automatic indexes and `ANALYZE` limit. The page size and auto-vacuum
mode apply only when the store's database is created; an existing store keeps
its own. `handle.sqlite_settings()` reads back what SQLite has in effect, and
`checkpoint` returns what SQLite reports: whether it was blocked, how many
frames the WAL holds, and how many it copied.

The settings coven depends on have no field, so the app can't change them: WAL
mode, `synchronous = FULL`, normal locking, foreign keys, recursive triggers
and an untrusted schema. The API reference lists every SQLite setting and why
each one is the app's or coven's.

### Collations

SQLite orders text by its own three collations, `BINARY`, `NOCASE` and
`RTRIM`. For any other order, such as listing "Note 9" before "Note 10", the
app registers a collation on the builder: a name and a function that compares
two strings. Its SQL then names it with `COLLATE`, in a query or in the schema.

```rust
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .collation("natural_order", natural_order)
    .open(store)
    .await?;

// A migration can declare an index in that order:
//   CREATE INDEX notes_by_title ON notes(title COLLATE natural_order)
let titles = handle
    .read(|sql| {
        Ok(sql.query(
            "SELECT title FROM notes ORDER BY title COLLATE natural_order",
            [],
            |row| row.get::<_, String>(0),
        )?)
    })
    .await?;
```

Coven registers each collation on every connection it opens to the store's
database, before any app SQL runs there: the writer, which also migrates,
applies other devices' writes and checks integrity, the readers, and every
connection of a read-only handle. Every builder that opens the store, a
widget's read-only one included, registers the same collations.

An order is part of the store's schema. The function must be deterministic,
comparing the same two strings the same way on every device and in every app
version, and a total order. An index keeps its entries in the order the
function gave when they were written, so a changed order makes queries return
the wrong rows and the next open fails its integrity check as a damaged
database. To change an order, register it under a new name, migrate the
indexes and declarations to it, and keep registering the old name with the old
order. Key and unique columns of synced tables still compare by bytes.

A name that is one of SQLite's own fails the open with
`DbError::ReservedCollation`, and a name given twice, ignoring case, with
`DbError::DuplicateCollation`. A name that is an SQL keyword, such as
`natural`, has to be quoted: `COLLATE "natural"`. A panic in the function ends
the process, because SQLite has no way for a comparison to fail.

### Functions

SQLite's `lower`, `LIKE` and `NOCASE` fold only ASCII letters, so a search for
"école" misses a note titled "École". The app can give SQLite a function of its
own instead: a name, how many arguments it takes, whether it is deterministic,
and the Rust function that computes it. Its SQL then calls it by name, in a
query or in the schema.

```rust
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .function("fold", 1, Determinism::Deterministic, |arguments| {
        Ok(arguments.get::<String>(0)?.to_lowercase())
    })
    .open(store)
    .await?;

// A migration can declare an index on it:
//   CREATE INDEX notes_folded ON notes(fold(title))
let matches = handle
    .read(|sql| {
        Ok(sql.query(
            "SELECT title FROM notes WHERE fold(title) = fold(?1)",
            ["école"],
            |row| row.get::<_, String>(0),
        )?)
    })
    .await?;
```

Coven registers each function on the same connections as the collations, and
every builder that opens the store has to register the same ones. The function
returns any value that converts into an SQLite value, such as a `String`, an
`i64` or an `Option` of one, or an error that fails the statement calling it.
Coven marks every app function safe for triggers and views, which only the
app's migrations declare.

Only a deterministic function can appear in an index, a CHECK or a generated
column. One the schema uses is part of the store's schema, like a collation: it
must give the same result for the same arguments on every device and in every
app version. Unicode lowercasing changes when a newer Unicode version gives a
letter a lowercase form, so change a function the way you change an order:
register the new one under a new name, migrate to it, and keep the old one. A
function that refuses some value with an error also stops this device from
applying another device's write that holds it, so a function the schema uses
should return a value for every argument.

A name SQLite or coven already has, such as `lower` or `coven_applying`, or
one starting with `coven_` or `_coven_`, fails the open with
`DbError::ReservedFunction`, and a name given twice, ignoring case, with
`DbError::DuplicateFunction`. `regexp` is the app's to define: SQLite has
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
`audience_from`), that its rows carry a [file](/docs/files) (`carries_files`),
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
    sql.execute("INSERT INTO notes (id, title) VALUES (?1, ?2)", (note, "Paint colors"))?;
    sql.execute("INSERT INTO tags (id) VALUES ('urgent') ON CONFLICT DO NOTHING", [])?;
    sql.execute("INSERT INTO note_tags (note_id, tag_id) VALUES (?1, 'urgent')", (note,))?;
    Ok(())
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
| A key column that allows NULL | a key with a NULL in it names no row | `SchemaError::NullableKey` |
| An independent key with no text column for its UUID | | `SchemaError::IndependentKeyNotUuid` |
| A collation other than `BINARY` on a key, unique or foreign-key column | under `COLLATE NOCASE`, SQLite would hold "Urgent" and "urgent" as one row that coven treats as two | `SchemaError::KeyCollation` |
| Declared key columns that don't match the table's primary key | | `SchemaError::KeyColumns` |

The rules for foreign keys, unique constraints, CHECKs and triggers are on the
[Merge](/docs/merge#the-rules-sql-brings-with-it) page;
[Constraints](/docs/constraints) lists what can't be expressed at all.

## Writing

A write is one SQLite transaction through `handle.write`. Its closure gets a
[`SqlContext`](rustdoc:struct:coven::SqlContext) with `execute`,
`execute_batch`, `query_row` and `query`. If the closure returns an error, or a
statement fails, the whole transaction rolls back.

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
let titles: Vec<String> = handle
    .read(|sql| Ok(sql.query("SELECT title FROM notes", [], |row| row.get(0))?))
    .process(|mut titles| {
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
unrelated write doesn't rerun it. The first `next()` returns the current
result; an error is a result too and doesn't end the query. Dropping the
`LiveQuery` ends it.

```rust
let mut notes = handle.subscribe(|sql| {
    Ok(sql.query("SELECT id, title FROM notes ORDER BY title", [], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?)
});
while let Ok(rows) = notes.next().await {
    show_notes(rows);
}
```

`handle.subscribe_reconfigurable(request, query)` takes a request, such as a
page number or a search term, that the app can replace through
`requests().set(..)` without starting a new subscription. Each result says
which request and revision it answers, and whether a new request, a write, or
both caused it.

Coven's own state has ready-made live queries with the same results as their
one-shot reads: `subscribe_members`, `subscribe_lost_values`,
`subscribe_pending`, `subscribe_uploads`, `subscribe_pin_progress` and others.
They all work offline.
