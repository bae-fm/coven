# sqlive

sqlive runs live queries over one SQLite database. A *live query* returns its
rows once, then returns them again each time a write commits a change to
something the query read. A program that shows a database's contents uses it to
keep what it shows current, without polling and without running every query
after every write. This document specifies what sqlive does, for engineers who
change sqlive or build a host on it.

A database has one writer connection and several read-only connections beside
it. A *write* is one transaction on the writer, and a *row change* is one row's
insert, update or delete inside it. sqlive captures each row change a write
makes. A live query's run records what its statements read: the tables, the
columns, and the rows their conditions and joins allow, which this document
calls each table's *scope*. When a write commits, sqlive judges each row's net
change, from the row before the write to the row at its end, against the live
queries whose scopes the row could pass, and runs only those again.

Take a music app's album page, which reads:

```sql
SELECT title FROM tracks WHERE release_id = ?1 ORDER BY position
```

The page's run binds `?1` to its release, so its scope on `tracks` is that
release's rows. A write that adds a track to another release changes no row the
scope passes, and the page doesn't run again. A write that moves a track into
this release or out of it does, since sqlive checks the changed row as it was
before the write and as it is after. With 200 album pages open, each for its own
release, sqlive judges a track added to one release against that release's page
alone.

The *host* is the program that embeds sqlive, such as coven, which syncs a
database between devices. The host opens the database, decides what its
connections run, and runs SQL of two kinds on them: its own, and the *app's*,
the SQL of the program the host serves. The host's *policy* judges each
statement as SQLite prepares it, on top of sqlive's own rules. The host's setup
registers its SQL functions, collations and the values it keeps in memory on
every connection. And the host names the views a live query reads whole, such
as coven's views of its own state, which change with what coven holds in memory.

## 1. A database and its connections

A `Database` holds one SQLite file's connections: one writer, the read-only
readers beside it, and the threads that checkpoint its WAL and keep its
planner's statistics (section 10). Its host opens it with `Database::open_with`,
giving the file's path, the `Handle` the connections serve, the `Options` below,
and a `prepare` step, such as the host's migrations. sqlive opens the writer,
switches the database to SQLite's WAL journal mode with `synchronous = FULL`,
runs `prepare`, gathers the planner's statistics when `Options` asks for them,
and only then opens the readers, so they plan with the statistics it found
(section 10). WAL mode lets readers keep their snapshots while a write commits.
If SQLite keeps another journal mode, the open fails with `WalUnavailable`,
naming the mode SQLite kept.

A database whose checks must not write sets `prepare_on_reader`. sqlive then
runs `prepare` on the first reader and opens the writer only once it passes,
leaving the journal mode as it is: a writer opened first would create the file,
or the schema, that the checks look for.

`Options` holds:

- `settings`, the SQLite settings of every connection (section 10);
- `readers`, how many read-only connections the database opens beside the
  writer;
- `prepare_on_reader`, as above;
- `whole_views`, the views and tables a live query reads whole (section 3);
- `policy`, which makes each connection's policy as the connection opens;
- `setup`, which registers what each connection needs before any SQL runs on it;
- `capture`, which makes the writer's commits wake the handle's live queries,
  and names the tables whose every row operation a write records for the host
  (section 2);
- `checkpoint_pages`, the WAL size at which sqlive checkpoints, or `None`;
- `statistics`, whether sqlive keeps the query planner's statistics.

Other processes can open the same file read-only while the database is open,
since sqlive keeps SQLite's normal locking mode.

### Host SQL and app SQL

The host runs its own SQL on a connection through `HostSql`'s `query`,
`query_row`, `for_each`, `execute` and `batch`, and the app's through
`app_query`, `app_query_row`, `app_execute` and `app_batch`. The host's SQL may
read and write tables the app's SQL can't. Every statement passes sqlive's
authorizer as SQLite prepares it. For app SQL, and for the app's triggers that a
host statement runs, sqlive applies its own rules first. They refuse, with
`StatementForbidden` naming the operation:

- transaction control, since every transaction is the host's;
- a `PRAGMA`, and the `pragma_` table functions;
- attaching or detaching a database;
- loading an extension;
- changing the schema in a write, since a schema change belongs in a migration;
- `ANALYZE` in a write (below);
- any statement that writes, in a read.

Then the host's policy judges every action that passed, the host's own
statements' included. A host implements `Policy` to refuse what it forbids, such
as coven refusing app SQL that names its bookkeeping tables, and `NoPolicy` adds
nothing to sqlive's rules. An app statement `PRAGMA journal_mode = DELETE` fails
with `StatementForbidden { operation: "PRAGMA" }` before SQLite runs anything,
so the app can't take the database out of WAL mode, which readers' snapshots
rest on. A host that needs a different setting asks for it in `Options`
(section 10).

`Setup` registers the host's SQL functions, its collations, and the values it
keeps in memory that its functions read (section 3), on every connection as it
opens and before any statement runs there. A function registered with
`scalar_function` that panics fails the statement that called it, and the app's
call fails with `Error::Bug`; the function changed nothing, so the connection
goes on. A function the host
registers by a name it learns only as the connection opens
(`named_function`) has no such guard: it ends the process itself, or doesn't
panic.

**Edge cases.**

- **Prepared statements.** Each connection keeps the app's statements prepared,
  as it does the host's, since SQLite compiles into each one every trigger it
  can fire and the views those read. sqlive prepares a statement afresh the
  first time in each kind of call, read or write, because SQLite reports what a
  statement reads, and sqlive refuses what it may not do, only as it prepares;
  a later use from the cache records the same reads. The host's own SQL, a
  migration's and that of the host's local scope are prepared afresh each time,
  since preparing them has effects beyond judging. A schema change prepares
  every statement again. A read or a write changes no schema, so each checks
  for a change once, not before each statement. An app statement whose triggers
  read a view of six tables took 16 µs to prepare and runs from the cache in
  about 3 µs.
- **ANALYZE in a write.** App SQL in a write can't run `ANALYZE`, and fails
  with `StatementForbidden`: a write's capture records every table it admits,
  `sqlite_stat1` included, so the statistics `ANALYZE` changes would land among
  the write's changes. Leaving `sqlite_stat1` out of the capture would stop
  that; sqlive refuses the statement instead. sqlive's own statistics runs
  happen outside any write (section 10), and a migration may still run
  `ANALYZE`.

## 2. Writes and what they capture

A write is one transaction on the writer, from `BEGIN IMMEDIATE` until it commits
or rolls back. The host runs one with `write_with`, in two phases. First the
app's SQL runs while SQLite's pre-update hook tells sqlive each row operation as
it happens. Then the host's own work runs under an SQLite session, with the
operations the app's SQL made, such as coven turning them into the write's
record. sqlive then judges the live queries, commits, and tells the
subscriptions.

What sqlive keeps of each operation depends on the table:

- On a table the host names in `Options::capture`, sqlive keeps every operation,
  in order, with every column's value before and after it. coven names its
  synced tables there, since each write's record lists each row change with the
  values it set.
- On any other table, sqlive keeps only what wakes a live query: the table, the
  columns the write changed and, for a table some live query reads, each changed
  row's key and the values of the columns live queries read or narrow by, and no
  other values.

Filling a 1,000,000-row table the host doesn't capture took 0.39 s through a write
against 0.35 s in plain SQLite. Clearing it took 0.35 s against 0.01 s: while a
pre-update hook watches, SQLite deletes row by row instead of dropping the
table's pages at once. A bulk write (section 9) has no hook, and clears it in
one step, as plain SQLite does.

### Net changes

sqlive judges each row a write changes by the row's *net change*: the row as it
was before the write and as it is when the write commits. A write can change one
row more than once. A music app's import marks a folder busy as a write starts
to read its files, and marks it idle again before the write commits. The app
rewrites an album's list row with `INSERT OR REPLACE`, which SQLite runs as a
delete and an insert even when every value comes out the same. Neither row
changed, so neither write runs a live query again. A column that ends with the
value it began with changed nothing either, and a row changed twice runs a query
again as one change from its first values to its last would.

For each row a write changes in a table that some live query reads, sqlive keeps
the row's key and the values of the columns live queries read or narrow by, as
they were before the write first changed the row and after it last did. A column
whose values sqlive doesn't keep counts as changed once a statement changes it.
No key names a row of a table with no primary key, so sqlive refuses a schema
that has one, with an error that names the table, wherever it captures changes
or runs a live read.

### Streamed transactions

A host runs some transactions of its own on the writer as *streamed
transactions* (`stream_transaction`, or `stream_then_write` when a write's
phases follow in the same transaction): coven applies another device's changes,
loads a snapshot and keeps its upload bookkeeping this way. As a streamed
transaction's rows change, sqlive notes each table's changed columns and, for a
table a live query reads, the changed rows' keys and the values live queries
read or test, and judges each row's net change as it does for a write. It runs
the write's lookups (section 7) in the transaction itself, before it commits.

### Tables live queries can't read

A host can name tables whose changes no write keeps for live queries
(`Handle::new`'s `unrecorded`, read back as `UnrecordedTables`), such as coven's
records of authorship and merge state that a write keeps beside its rows. sqlive
keeps none of their changes, so a write that changes them keeps less, and a live
query whose run reads one of them fails at once instead of missing their
changes.

**Edge cases.**

- **Writes of many rows.** A write keeps up to 10,000 of the rows it changed in
  one table that the host doesn't capture, and a streamed transaction up to
  10,000 in any one table (`ROWS_PER_TABLE`). Past that it keeps only that the
  table changed, so a large write's memory stays bounded, and every live query
  that read the table runs again. A write that changes more than 10,000 rows of
  any one table looks none of them up: every query that joins the table runs
  again, so a write's lookups stay bounded too.
- **Values a change leaves alone.** SQLite's session, which records the host's
  phase of a write, names a row's key and the columns an update changed, not the
  others. When a live query narrows by one of those others, sqlive reads its
  value from the row before the transaction commits.
- **A lookup fails.** The write fails with the error and rolls back, as when one
  of its own statements fails.
- **A callback panics.** The unfinished transaction rolls back, the callback
  gives its connection back, and the call fails with `Error::Bug`, the panic's
  text kept. Later calls use the writer and the readers again.
- **A rollback fails.** Then the connection may still be inside the
  transaction. The writer would treat its uncommitted changes as committed in
  its own later reads and writes, and a reader would keep reading its old
  snapshot of the database. So the handle fails with `EngineFailure::Rollback`,
  and every later call on it, read-only or not, fails with the host's failure
  until the host reopens the database. Each live query reports it once and then
  ends, as it does when the database closes: repeating the error would keep a
  loop over the query busy with nothing to show.
  - A write or check that failed and then failed to roll back reports both
    failures (`Error::Rollback`).
  - A read ends by rolling back. If only that rollback fails, the read reports
    it; a read that had already failed keeps its own cause.
  - If a callback panicked, the call fails with its `Bug`, and sqlive logs the
    failed rollback.

## 3. Live queries

A live query runs once, then again whenever a write commits that changes rows
it read. A host makes one with `ReconfigurableLiveQuery::new`, which takes the
first request, the query's run, how its error type holds sqlive's errors, the
handle and where bugs go; `LiveQuery::new` wraps one whose request never
changes. A run takes the request, reads on a reader inside `live_read`, and
releases the reader before it returns, handing sqlive what its statements read.
A one-shot read records nothing of what it reads: nothing runs it again. Both
run only their own statements; the writes that change a joined table run the
lookups that judge them (section 7).

A music app's album list is one live query. Its first call returns the albums
now. A write that adds an album wakes it, and the next call returns the list
with the new album. A write that changes only a column the list doesn't read
wakes nothing, and a run whose result equals the last is never delivered.

A live query's calls need no async runtime, and only `next_blocking` waits:

- `changed` returns a future that resolves on any executor once a write changed
  what the query read, a new request came, the handle failed or closed, the
  query ended, or no run has answered yet. Dropping the future loses nothing.
- `run_changed` then runs the query once on the calling thread, without waiting,
  and returns `None` when nothing changed, when a write's rerun returned a result
  equal to the last, and once the query has ended.
- `next_blocking` returns the first result at once. After that it parks the
  calling thread until `changed` would resolve, runs the query on it, and
  returns the new result.

An async program awaits `changed` and runs `run_changed` on a blocking thread,
which leaves no thread waiting when a write leaves the result equal. A program
with no runtime calls `next_blocking` alone.

A query's errors are results of its error type and don't end it, except three:
the one saying the database closed, the one saying its handle fails every call,
and one that carries a bug, as a run that panicked does with `Error::Bug`. After
one of those, `next_blocking` and `run_changed` return `None` and never repeat
it, so a loop over the query ends. sqlive doesn't know what a run that panicked
read, so no commit could tell whether to run it again. The error type says
whether it carries a bug (`CarriesBug`).

The run's closure only runs SQL, and holds its read snapshot and a reader
connection while it runs. `process` maps each result to the program's types
after the run releases the connection, and compares the processed values to
tell an equal result.

### Requests

A reconfigurable live query takes a *request*, such as a page, a filter, a sort
or an id, which the program replaces without starting another query:
`requests` gives a `LiveQueryRequests`, whose `set` replaces the request and
returns the revision whose result will answer it. Equal consecutive requests
share their revision. A replacement before a run supersedes the pending
revision, which no result then answers. Each event of the query carries the
request and revision that produced it, and its cause: a new request, a write, or
both arrived before the run (`LiveQueryCause`). `set` fails with
`LiveQueryClosed` once the query is dropped.

### Values the host keeps in memory

Some facts a host shows last only as long as its process, so it keeps them in
memory instead of a table: coven keeps the files it is uploading this way, and
shows them through views (coven's spec,
[§4](../../spec/coven.md#4-writing-locally)). The host registers each such value
with `Setup::snapshot_value`, and its SQL functions read it.

A read sees such a value as it stood at its snapshot, as it sees the tables. A
statement reads the tables from its transaction's snapshot, which fixes the
commits it sees. A function that read the host's memory as each call ran could
join those commits to memory that a later commit's consequences had already
changed, a moment that never existed. So each transaction takes every value
once, as its snapshot is fixed, and every call in the transaction reads what it
took. A read fixes its snapshot and takes the values while no commit of the
database is underway, so the values are those of a moment between its
snapshot's last commit and the next. A write takes them as it begins: it holds
the database's write lock, so no other commit can come between its snapshot and
its values. A read that fixes its snapshot while a write commits waits for that
commit to finish.

When a value changes, the host calls `Handle::changed_without_write` with the
view columns that show it, and each live query that read one of those columns
runs again, as if a write had changed what it read.

### Views read whole

The host names, in `Options::whole_views`, the views and tables a live query
reads whole: those whose rows change with what the host holds in memory too,
such as coven's views of its uploads. A read through such a view reads the view
too, so a query that only counts the view's rows, or asks whether it has any,
and reads none of its columns, still runs again as the view changes. A view's
own tables are read whole as well (section 8).

**Edge cases.**

- **A statement inside a row mapping.** A query can run a statement from inside
  another's row mapping, as an album list reads each row's cover. The inner
  statement records its own reads, and the one around it keeps its own range of
  keys, so the list doesn't rerun for a write to an album it doesn't show.
- **A write while a query runs.** A live query's run doesn't know its scope
  until it ends, so a write that commits meanwhile can't be judged against it.
  The query keeps the write's changes and judges them as the run ends, with
  lookups in the run's own snapshot: a value matches a join there exactly when
  the run's partners held it. A lookup that fails there fails the run, and the
  query runs again. The write copies the values the query's run before tested,
  since a run of the same statements tests the same columns. Without them, a
  write that met a run would rerun it whatever rows it changed. A music app's
  import writes a batch of folders every few milliseconds while its import list
  is open: each run would meet the next batch, and the list would run again and
  again for as long as the import goes. A changed row missing a value the new
  run tests, as a write met by a query's first run is, reruns the query.
- **A run between a write's lookups and its commit.** A query can start a run
  after a write looked its values up and before the write commits, as when the
  program replaces its request then. That run doesn't see the write, and the
  lookups answered for the query's earlier scope. So the query judges the write
  again against its new scope, counting every join's value as matched: the
  write's transaction has ended, and no lookup can read it.

### The live query types

```rust
/// A subscription's results and lifetime; dropping it ends future runs. Its
/// results fail with `E`, the query closure's own error type.
pub struct LiveQuery<T, E> { /* private fields */ }

/// A subscription whose request may be replaced while it runs, its results
/// failing with `E` as a `LiveQuery`'s do.
pub struct ReconfigurableLiveQuery<Q, T, E> { /* private fields */ }

/// Shared access to replacing a live query's request.
pub struct LiveQueryRequests<Q> { /* private fields */ }

/// A request revision assigned by one live query.
pub struct LiveQueryRevision(pub u64);

/// The query was dropped before its request could be replaced.
pub struct LiveQueryClosed;

/// One result with the request and revision that produced it.
pub struct ReconfigurableLiveQueryEvent<Q, T, E> {
    /// The request used by this run.
    pub request: Q,
    /// The revision assigned to that request.
    pub revision: LiveQueryRevision,
    /// The reason the query ran.
    pub cause: LiveQueryCause,
    /// A value or a failure, of the query's own error type; failures do
    /// not end the query.
    pub result: Result<T, E>,
}

/// Why a run was requested; the first run answers the initial request.
pub enum LiveQueryCause {
    /// A new request, including the initial one.
    Request,
    /// A commit changed a column the query read, in a row its conditions
    /// allow, or a view it read changed with no change to its tables.
    Write,
    /// A new request and a relevant commit arrived before this run.
    RequestAndWrite,
}

impl<T: Send + 'static, E: CarriesBug + Send + 'static> LiveQuery<T, E> {
    pub fn new(inner: ReconfigurableLiveQuery<(), T, E>) -> Self;
    pub fn changed(&self) -> impl Future<Output = ()> + Send + 'static;
    pub fn run_changed(&mut self) -> Option<Result<T, E>>
    where
        T: Clone + PartialEq;
    pub fn next_blocking(&mut self) -> Option<Result<T, E>>
    where
        T: Clone + PartialEq;
    /// Whether the database closed, the handle failed or a run's error
    /// carried a bug, and a result said so: the query answers nothing more.
    pub fn has_ended(&self) -> bool;
    pub fn process<P, U>(self, process: P) -> LiveQuery<U, E>
    where
        T: Clone + PartialEq,
        P: Fn(T) -> Result<U, E> + Send + Sync + 'static,
        U: Send + 'static;
    /// How often this query has run so far, its processed form's runs too.
    #[cfg(feature = "test-utils")]
    pub fn runs(&self) -> LiveQueryRuns;
    /// Whether a commit since this idle query's last result changes what it
    /// reads, so asking for its next result runs it again. A commit marks the
    /// queries it changes as it commits, so this answers with no waiting.
    #[cfg(feature = "test-utils")]
    pub fn is_marked_for_rerun(&self) -> bool;
}

impl<Q, T, E> ReconfigurableLiveQuery<Q, T, E>
where
    Q: Clone + PartialEq + Send + Sync + 'static,
    T: Send + 'static,
    E: CarriesBug + Send + 'static,
{
    /// A query that answers `request` first, with `run`, on `handle`; its
    /// error type holds sqlive's errors through `failed`.
    pub fn new<F: Clone + Send + Sync + 'static, R: 'static>(
        request: Q,
        run: Run<Q, T, E>,
        failed: fn(Error<F, R>) -> E,
        handle: &Handle<F>,
        bugs: BugSinkRef,
    ) -> Self;
    /// As `LiveQuery::changed`, and a new request resolves it too.
    pub fn changed(&self) -> impl Future<Output = ()> + Send + 'static;
    /// As `LiveQuery::run_changed`, returning the event.
    pub fn run_changed(&mut self) -> Option<ReconfigurableLiveQueryEvent<Q, T, E>>
    where
        T: Clone + PartialEq;
    /// As `LiveQuery::next_blocking`, returning the event.
    pub fn next_blocking(&mut self) -> Option<ReconfigurableLiveQueryEvent<Q, T, E>>
    where
        T: Clone + PartialEq;
    /// Process each value after its run releases the connection. Request,
    /// revision and cause stay on the event; errors remain results.
    /// Equality compares processed values. Existing request handles still work.
    pub fn process<P, U>(self, process: P) -> ReconfigurableLiveQuery<Q, U, E>
    where
        T: Clone + PartialEq,
        P: Fn(T) -> Result<U, E> + Send + Sync + 'static,
        U: Send + 'static;
    pub fn requests(&self) -> LiveQueryRequests<Q>;
    #[cfg(feature = "test-utils")]
    pub fn runs(&self) -> LiveQueryRuns;
    #[cfg(feature = "test-utils")]
    pub fn is_marked_for_rerun(&self) -> bool;
}

impl<Q: Clone + PartialEq> LiveQueryRequests<Q> {
    /// Replace the request, returning the revision whose result will answer
    /// it. Fails once the query is dropped.
    pub fn set(&self, request: Q) -> Result<LiveQueryRevision, LiveQueryClosed>;
}
```

## 4. What a statement reads

A live query depends on what its statements read. As SQLite prepares each
statement, it tells sqlive the tables and columns the statement reads, and a
commit that changes none of those columns doesn't run the query again. sqlive
also reads the statement's SQL, to find which rows of each table its conditions
and joins allow: its scope on each table, which sections 5 to 8 describe.

SQLite leaves some columns out of what it reports, and sqlive adds them itself.
SQLite leaves out the columns a USING or NATURAL join compares, so sqlive adds
them for each table such a join compares in the statement or in one of the
app's views it reads: a write that changes only `tracks.release_id` reruns a
query that joins `tracks` to `releases` with `USING (release_id)`. SQLite also
leaves out the columns that decide which rows a LIMIT keeps, which row an
aggregate takes a value from, and which of the rows that compare equal a
DISTINCT or a compound keeps, and sqlive adds them too, as section 6 says.

sqlive adds no column for a CTE that no query of the statement reads: SQLite
never runs it, so its joins and its LIMIT change no row. A music app's import
screens share one WITH, whose `candidate` CTE picks one row of `scan_folders`
with `LIMIT 1`. A screen's statement that reads only `scan_files` doesn't rerun
when a write changes another row of `scan_folders`. When sqlive can't parse the
statement, or a view it reads, sqlive counts every column of every table as
read, so every write runs the query again.

A live query's run can also run statements of the host's own through
`HostSql::host_live_read`, which may read tables app SQL can't. sqlive reads
such a statement as it reads an app's, so it narrows by the same rules and the
query runs again only for a write that changes a row the statement read. When a
write asks SQLite a question about such a row, as for a join's partner rows,
SQLite answers it as the host's own SQL. A host statement that a live query runs
through any other call, as `query` or `for_each`, reads each table it names
whole.

sqlive reads a statement's SQL to work out what it depends on when a live query
first runs it. The more a statement joins, the longer that takes, so sqlive logs
at debug level how many FROM items and SELECTs it read in each statement, and
how many edges it found between the FROM items. An *edge* is an equality that
the rows of one FROM item meet with a value of another's, by which sqlive
narrows the first. One equality can give an edge each way, and the sides of an
OR, of a COALESCE or of a compound subquery give none. When an album page is
slow to open the first time, the log shows how large its statement is.

**Edge cases.**

- **One plan for every connection.** sqlive reads from each statement of a live
  query which rows of each table the query depends on, by the rules of sections
  5 to 8, the first time a live query runs it: the statement's *scope plan*.
  The plan depends on the statement's text, the schema, and the SQL functions
  and the views read whole that the connection was set up with. sqlive sets up
  every connection of a database alike, so they share the plans each one reads,
  and a schema change reads them again. Kept by each connection alone, a plan
  would cost a screen again on each reader the screen first runs on. A music
  app's import pane has 22 statements, which took 2 s to read in a debug build,
  so each of the first switches between candidates would wait 2 s. When two
  readers first run a statement at the same moment, one reads it and the other
  waits for what that one reads, since reading it twice would only take more of
  the processor. When that reading fails, the waiting reader reads the
  statement itself.

## 5. Narrowing one table

Within a table, a statement that reads one table depends only on the rows its
WHERE allows: a column compared with a constant or a bound value by `=`, `<`,
`<=`, `>`, `>=` or `IN` with a list, and conditions like these joined by AND and
OR. An album page reads `SELECT title FROM tracks WHERE release_id = ?1 ORDER BY
position`. A write to another release's tracks doesn't rerun it; one that moves
a track into this release or out of it does, since sqlive checks the changed row
as it was before the write and as it is after. Functions, aggregates, grouping,
ordering and limits work only on the rows the WHERE allows, so `SELECT count(*)
FROM tracks WHERE release_id = ?1` narrows the same way.

`IS` with a value, `IS NULL` and `IS NOT NULL` narrow too, and so do their
spellings `ISNULL`, `NOTNULL` and `NOT NULL`. `x IS 5` is `x = 5`, and `x IS ?1`
narrows by whichever the run's value is: the rows where `x` is NULL, or those
where it equals the value. An album grid sorted by artist lists the albums with
no credits first, so its first window holds `credit_sort_key IS NULL`. An album
added further down the list, with a credit, doesn't rerun that window.

The truth of a NULL test or of a comparison, compared with a value, narrows to
the rows where that comparison holds. An album grid sorted by year lists the
albums with no year last, so its order begins with `year IS NULL`. A window's
statements hold `(year IS NULL) = 0`, which narrows to the albums that have a
year, or compare `year IS NULL` between two bounds. An album added in another
window's years doesn't rerun this window.

### Where sqlive compares values

A condition narrows only where sqlive compares values as SQLite does. One on a
column whose collation isn't BINARY, or on a generated column, doesn't, and nor
does one whose value SQLite would convert first, as text compared with an
INTEGER column: there SQLite's answer and sqlive's could differ. A NULL test
looks through a COLLATE on its column, since a collation doesn't change whether
a value is NULL.

### Conditions SQLite judges

Any other condition on one table's row narrows that table too, however it is
written: a function of a column, a CASE, `LIKE` and `GLOB`, `<>`, `NOT`, `IS
NOT` a value, a row value, a COLLATE. sqlive doesn't compare such a condition
itself. To judge a write it asks SQLite whether the changed row passes, as it
was before the write and as it is after, with each value given its column's
type so the condition compares as it does in the table. An artist picker lists
the artists the typed text finds first, ordered by `CASE WHEN lower(name) = ?1
THEN 0 WHEN lower(name) GLOB ?2 THEN 1 ELSE 2 END`, so a window of it holds
`CASE ... END = 2` over `artists`. sqlive asks SQLite whether an artist written
while the picker is open lands in that window's rank, so an artist added whose
name the text doesn't find, and which sorts in another window, doesn't rerun it.
The question is a SELECT over the changed row's values, with a text kept as
`CAST(?3 AS TEXT)` in a TEXT column and an integer as `CAST(?3 AS INTEGER)` in a
numeric one, so `name = 5` is true of the text `'5'` there as it is in the
table.

A condition is judged this way when it names columns of one table of its own
SELECT and no other table, each a column sqlive narrows by (stored, BINARY), and
holds no subquery, no `rowid`, no clock (`CURRENT_TIME`, `date('now')` and the
other date and time functions) and no function that isn't deterministic. A
condition that doesn't qualify, such as `b = (SELECT b FROM u LIMIT 1)`, `c >
unixepoch('now')` or `t.a + u.a = 3`, narrows nothing, and a statement with no
condition that narrows depends on every row of each table it read. When sqlive
can't tell, it counts the row as a match, so the query runs again: a value with
no cast that keeps it, such as a blob in a TEXT column, a value the write didn't
record, and an error. When a question runs past the lookups' step limit
(section 7), sqlive runs the query again whatever the write's other questions
answer. A condition judged this way is never read from a view's or subquery's
rows: it narrows the tables the statement reads directly, and the tables behind
a view keep the conditions they have.

A condition SQLite judges costs a write one small SELECT for each row it changes
and each such condition a live query holds, and rows that agree on the columns a
condition names share one answer. A music app's import writes 270 rows at a
time. With four queries open, each holding such a condition on the table it
writes, the write ran 33 SQLite steps a row and took 4.7 µs a row, against
1.8 µs with none open, on a busy machine.

## 6. LIMIT and the order SQLite reads rows in

A statement with a LIMIT narrows further when a run returns as many rows as its
LIMIT. A music app's album list shows each window as one live query, `SELECT
title, id FROM albums WHERE title >= ?1 ORDER BY title LIMIT 100`. A run that
returns 100 rows depends only on the albums that sort at or before its last row:
an album that sorts after that row would sort after every row the run returned,
so it can't appear in the result. So sqlive narrows `albums` to the rows whose
`title` is at most the last row's, and each run sets that bound again from its
own last row. An album added further down the list doesn't rerun the window. One
added inside the window does, and so does deleting one, after which the window
holds the next album too. A row equal to the last row on every ORDER BY column
counts as inside, since SQLite could sort it before the last row. A run that
returns fewer rows than its LIMIT already holds every row its WHERE allows, so
its scope stays as the WHERE gives it.

The bound compares rows as the ORDER BY does: term by term, each in its own
direction, with NULL first in ascending order and last in descending. sqlive
reads the last row's values from the result, so the statement must also return
each ORDER BY term as a result column, listed before any `*` and written as the
term writes it, with or without the COLLATE it ends in (below). Each term
reads one table, with no NULLS FIRST or LAST, and is one of these:

- a stored column, in any collation;
- an expression over that table's stored BINARY columns, such as `year IS
  NULL` or `coalesce(year, 0) + 1`, that holds no subquery, no `rowid`, no
  clock and no function that isn't deterministic;
- a generated column whose collation is BINARY and whose expression names only
  that table's stored BINARY columns.

A term may end in one COLLATE, as `title COLLATE NOCASE`, which gives it that
collation. A COLLATE anywhere else in the term, or an expression over a column
that isn't BINARY, leaves the scope as the WHERE gives it. Without a COLLATE, a
stored column sorts in the collation it declares and any other term in BINARY,
as SQLite sorts it. The collation can be one of SQLite's own, `BINARY`,
`NOCASE` or `RTRIM`, or one the host registered (section 1). sqlive keeps no
list of collations, so a collation the host adds later narrows with no change to
sqlive.

To judge a write for a term, sqlive asks SQLite to compare the term's value in
the changed row with the last row's, as it judges a condition on one table's
row (section 5): a SELECT over the row's values that evaluates the expression, the
column or a generated column's own expression, in the term's collation. sqlive
writes the expression after a unary plus and names the collation, as
`+(title) COLLATE natural_order < ?3`. The plus takes away the expression's
affinity, so SQLite compares the two values as the ORDER BY does, and the
COLLATE makes it compare text by the app's function, the same function the
ORDER BY runs. Values that are not text compare as they always do. For a
generated column, sqlive gives the last row's value the column's type, as
`CAST(?3 AS INTEGER)` in an INTEGER column, so SQLite converts the
expression's value as it does the column's. Two values that the collation
ties, such as `'Rock'` and `'rock'` in NOCASE, count as equal, so a row that
ties with the last row on every term still passes. An import list sorts paths
with `ORDER BY path COLLATE natural_order, id` and returns both terms, so a full
window that ends at "Track 12" doesn't rerun when the app adds "Track 40". An
album grid sorted by year orders by `year IS NULL, year, id` and returns each
term, so a full window that ends in 1998 doesn't rerun when the app adds an
album from 2004. A storage list sorts by `storage_order`, a generated column
that ranks how each release is stored, and a release added with a later rank
doesn't rerun a full window.

The terms can read more than one table. An album grid grouped by artist
joins `artists` to `albums` with `JOIN albums AS al ON al.artist_id = ar.id`
and orders by `ar.sort_key, ar.id, al.title, al.id`, returning each term. The
ORDER BY's terms fall into runs of consecutive terms over one table: here the
two terms over `artists`, then the two over `albums`. A row sorts at or before
the last row when its first run's values sort before the last row's, or equal
them and its other values sort at or before the last row's. So sqlive narrows
the first run's table, `artists`, to the artists whose own terms sort at or
before the last row's artist. It narrows a later run's table, `albums`, to the
albums whose own terms sort at or before the last row's album, and to the
albums that join an artist that sorts before the last row's artist. A write
that changes an album asks for that artist as it asks for a joined row
(section 7), within the artists the run's scope allows. An album added to an
artist after the window, or to the window's last artist after its last album,
doesn't rerun the window; one added to an earlier artist does, wherever its
title sorts. A later run's table gets this bound only when sqlive reads it
after the table of each earlier run and an equality with a column of that
table narrows it. When a condition on `albums` alone makes sqlive read
`albums` first, only the artists' bound narrows the window, and an album
that meets that condition reruns it wherever it sorts.

No table a term reads is the right side of a LEFT JOIN. There, when a write
adds a row that matches a left row that had no match, SQLite no longer returns
the row of NULLs it returned for that left row, and that row of NULLs can sort
before the last row. The statement is one SELECT with no OFFSET, no GROUP BY,
and no aggregate or window function in its result columns, since there one
result row's values depend on many table rows. A join that narrows another
table by a sorted table's rows takes only the rows within that table's bound.
Any other ORDER BY, such as one by a result column's position or by an
expression over two tables, and any OFFSET leave the scope as the WHERE gives
it.

### Which rows SQLite reads first

Which rows a LIMIT keeps can depend on columns the statement never names.
SQLite keeps the first rows it reads, and it reads them in the order of the
plan it chooses. A photo app shows one photo of each album with `p.id IN
(SELECT f.id FROM photos AS f WHERE f.album_id = a.id LIMIT 1)`, and the app
has an index on `photos(album_id, taken_at)`. SQLite reads each album's photos
through that index, in the order they were taken, so a write that changes only
one photo's `taken_at` can change the photo the album shows. So a query with a
LIMIT or an OFFSET depends on every column of each table it reads: the tables
of its FROM, of the views and CTEs it reads, and of the queries inside it. So
does a subquery whose first row a comparison takes, as `x = (SELECT ...)` or
`(a, b) = (SELECT ...)`, since SQLite reads it as if it had `LIMIT 1`. The
query's rows still narrow as its conditions and the LIMIT's bound say: a write
to a photo of an album the screen doesn't show doesn't rerun it.

An ORDER BY that sorts by the rowid of each of the query's tables tells every
two of its rows apart, so the order SQLite reads them in can't change which
rows the LIMIT keeps. Such a query depends only on the columns its statements
name. The album list above, ordered by `title` alone, depends on every column
of `albums`: two albums with the same title can come in either order, and an
index SQLite reads them through decides which one ends the window. Ordered by
`title, id`, where `id` is the table's INTEGER PRIMARY KEY, the window depends
only on the columns it names. A table's rowid is its INTEGER PRIMARY KEY
column, or `rowid` where no column has that name. Each ORDER BY term names it
as a column, by its name, by its result column's alias or by its position. The
query is one SELECT with no DISTINCT, GROUP BY or aggregate, and each of its
FROM items is a table. A WITHOUT ROWID table has no rowid, and a PRIMARY KEY
that isn't the rowid, as `id INT PRIMARY KEY`, can hold NULL in many rows, so
an ORDER BY over either still leaves the query depending on every column. A
SELECT that calls an aggregate with no GROUP BY returns one row, which every
order keeps, so a subquery such as `(SELECT count(*) FROM tracks AS t WHERE
t.release_id = r.id)` depends only on the columns it names too.

An aggregate can depend on the order SQLite reads rows in, with no LIMIT. A
music app's artist list shows each artist's album count and the title of one
album: `SELECT b.artist_id, b.title, count(*) FROM albums AS b GROUP BY
b.artist_id`. `b.title` is no GROUP BY term and sits in no aggregate, so SQLite
takes it from one of the artist's albums, which the order its plan reads them in
decides. The app has an index on `albums(artist_id, year)`, so SQLite reads each
artist's albums by year, and a write that changes only one album's `year` can
change the title the list shows. `group_concat(b.title, ', ')` joins the titles
in that same order. So a SELECT that groups its rows or calls an aggregate
depends on every column of each table it reads, as a query with a LIMIT does, in
four cases:

- A result column, the HAVING or an ORDER BY term names a column, or holds a
  subquery, outside every aggregate and every GROUP BY term, or the SELECT
  returns a `*`.
- The SELECT calls an aggregate other than `count`, `min` and `max`, and the
  call has no ORDER BY of its own that sorts by the rowid of each of the
  SELECT's tables. `group_concat` and `json_group_array` join their values in
  the order SQLite reads them, and `sum`, `total` and `avg` add reals in that
  order, which can change the last digits of the result.
- The SELECT calls `min` or `max` of anything but the rowid or a stored column
  of one of its tables whose collation is BINARY and whose declared type is
  neither BLOB nor missing. Among values that compare equal and still differ,
  `min` and `max` keep the value SQLite reads first: an untyped column can
  hold both `1` and `1.0`, a NOCASE column both `'Abc'` and `'ABC'`, and an
  expression can give either.
- A result column, the HAVING or an ORDER BY term shows a GROUP BY term other
  than a column that the third case allows. Among values of the term that
  compare equal and still differ, a group keeps the value SQLite reads first,
  so `SELECT b.genre, count(*) FROM albums AS b GROUP BY b.genre`, where
  `genre` is a NOCASE column, can show `'Rock'` or `'rock'`.

A column is a GROUP BY term when the GROUP BY names it as the SELECT writes it,
or names its result column by position or alias. SQLite tells an aggregate from
a scalar function by the function's name and the number of arguments the call
passes: `max(b.year)` calls the aggregate, and `max(b.year, b.month)` calls
SQLite's scalar max, which takes the larger value of each row, so its columns
sit in no aggregate. `count` gives the same value in every order, and so do
`min` and `max` of a column that the third case allows, so `SELECT b.artist_id,
count(*), max(b.year) FROM albums AS b GROUP BY b.artist_id`, where `artist_id`
and `year` are INTEGER columns, depends only on the columns it names.
`group_concat(b.title, ', ' ORDER BY b.id)`, where `id` is the table's INTEGER
PRIMARY KEY, depends only on the columns it names. Ordered by `b.year` alone, it
depends on every column of `albums`, since two albums of the same year can come
in either order. The SELECT's conditions still narrow its rows: a write to an
album of an artist the list doesn't show doesn't rerun it.

A window function depends on that order too. `row_number() OVER (PARTITION BY
b.artist_id ORDER BY b.year)` numbers an artist's two albums of the same year
in the order SQLite reads them, and with no ORDER BY in its OVER, it numbers
all of an artist's albums in that order. So a SELECT that calls a window
function depends on every column of each table it reads, unless the window's
ORDER BY sorts by the rowid of each of the SELECT's tables and the SELECT
neither groups its rows nor calls an aggregate. `row_number() OVER (PARTITION
BY b.artist_id ORDER BY b.year, b.id)` depends only on the columns it names.

DISTINCT, UNION, INTERSECT and EXCEPT keep one of each set of rows that
compare equal. Among rows whose values compare equal and still differ, the
order SQLite reads them in decides which one the result shows, so `SELECT
DISTINCT b.genre FROM albums AS b`, where `genre` is a NOCASE column, can show
`'Rock'` or `'rock'`, and so can `SELECT b.genre FROM albums AS b UNION SELECT
s.genre FROM singles AS s`. So a SELECT DISTINCT, and a query that combines
its SELECTs by UNION, INTERSECT or EXCEPT, depend on every column of each
table they read, unless each result column of each such SELECT is a column
that the third case of the aggregate rules allows.

## 7. Joins and lookups

A statement that joins tables narrows each one by the rows the others matched.
A music app's album page lists its tracks with their credited artists:

```sql
SELECT t.title, a.name FROM tracks AS t
LEFT JOIN track_artists AS c ON c.track_id = t.id
LEFT JOIN artists AS a ON a.id = c.artist_id
WHERE t.release_id = ?1 ORDER BY t.position
```

The WHERE narrows `tracks` to this release's rows. The join narrows
`track_artists` to the rows whose `track_id` is the id of one of those tracks,
and `artists` to the artists those rows name. A credit added to another
release's track doesn't rerun the page; one added to this release's tracks
does.

### Lookups

The page's run reads nothing for its joins: the write that changes a joined
table decides. When a write credits an artist on track 7, sqlive asks, in that
write's own transaction before it commits, whether one of this release's
tracks has id 7. That question is a *lookup*, one for each live query that
joins `track_artists`:

```sql
SELECT 1 FROM tracks AS t WHERE t.id = 7 AND t.release_id = ?1 LIMIT 1
```

where `?1` is the release the page's run was bound to. sqlive looks up the
row's value before the write and after it, so a credit moved onto this
release's track or off it reruns the page. A lookup for `artists` asks the
same of `track_artists`, with the tracks of this release inside it. sqlive asks
the tables, not the query's result: a release with no tracks yet has no row in
the result, and its first track must still rerun a query that joins it.

Asking at the write keeps a live query's run as fast as a one-shot read of the
same SQL, and its memory the same whatever its partners hold: it keeps its
statements' conditions, no values. Reading the joined values in each run
instead could read a whole table for one small window. On a machine busy with
other work, scrolling an import list of 50,000 folders took 0.5 to 1.7 s a
window that way, and 4 to 10 ms with lookups.

A write pays for the narrowing instead. A lookup that an index serves takes a
few of SQLite's steps, and a write looks each value up once for each query that
joins its table. Over 10,000 releases of 12 tracks, a write that credits
artists on 1,000 tracks of other releases took 0.5 ms more with the album page
open, about 0.5 µs a track. An import that writes 270 rows at a time, with four
of an album's screens open, each joining tables the import writes, ran
about 50 lookups a write, and cost the same 94 µs a row as with none open.

### Which equalities narrow a join

A join narrows a table this way through an equality between one of its columns,
or an expression over its own columns, and a column, or an expression over the
columns, of one other table in the join. Each expression holds no clock
(`CURRENT_TIMESTAMP`, `date('now')`) and no function SQLite doesn't flag
deterministic, as `random()` or a function the host registered as not
deterministic: a write's lookup evaluates it again and could get another answer
than the query did, so such a join narrows nothing. That holds for a column of a
subquery or VALUES whose expression holds one, as `page.at` of `(SELECT
CURRENT_TIMESTAMP AS at) AS page`, on either side of the `=` and in a USING
join. So does a column of a subquery that reads a view read whole, which can
show what the host holds in memory; a join on the view's own column still
narrows. An aggregate or window function gives its rows' answer and doesn't
count. A row value equality, as `(t.a, t.b) = (u.a, u.b)`, narrows as the
equalities of its columns: SQLite gives it the truth of their AND, NULL
included. An equality with COALESCE or IFNULL of such values narrows by each of
them, since COALESCE returns one of its arguments: a value of one other table, a
subquery's returned column or a constant. A lookup compares each value with the
joined column's affinity, as SQLite gives it to COALESCE's value.

An album choice joins a record to its owner by `(CASE WHEN rr.kind =
'album' THEN rr.key ELSE rr.album_key END) = o.album_key`: for a record a write
changes, sqlive asks SQLite for the CASE's value in the record's row and whether
an owner in the query's scope holds it, so a record of another album doesn't
rerun the choice. SQLite gives such an expression's value the other side's
affinity, and sqlive compares it the same way. An expression of a table's own
columns that holds a COLLATE narrows nothing, and one over a table a LEFT JOIN
adds narrows that table only when it is NULL wherever a column it names is
NULL, as the next paragraph says of an expression over the other table.

An expression over a table a LEFT JOIN adds gives the joined column values
only when it is NULL wherever a column it names is NULL. The album page can
show each track's cover with `LEFT JOIN tags AS g ON g.track_id = t.id JOIN
covers AS cv ON cv.kind = CASE WHEN g.kind IS NULL THEN 'plain' ELSE g.kind
END`. A track with no tag shows the `'plain'` cover. A lookup asks the tags
table, which has no row for that track, so it would never find the `'plain'`
cover, and a new `'plain'` cover would not rerun the page. So over a LEFT
JOIN's table, sqlive narrows by an expression built only of columns,
constants, parameters, arithmetic, comparisons, `||`, the bit operators,
`NOT`, the signs, CAST, COLLATE, `LIKE`, `GLOB`, `->`, `->>` and the built-in
functions that return NULL whenever an argument is NULL, such as `lower`,
`substr`, `length`, `abs`, `round` and `json_extract`. An expression with any
other part narrows nothing there: CASE, `iif`, `nullif`, `IS`, `IS NULL`, AND,
OR, BETWEEN, an IN list, `quote`, `typeof`, `hex`, `concat`, `printf`, a
COALESCE inside another expression, and every function the host registers,
whose NULLs sqlive can't know. The album page reads `covers` whole. The same
CASE over a table an inner join adds narrows: every row the join keeps has a
tag, which a lookup finds.

An OR narrows a table when each of its sides does, to the rows either side
allows: an import finds an artist by `a.discogs_id = t.dg OR a.id = t.dg + 100`,
and an artist neither side names doesn't rerun it. sqlive looks up each side's
equalities in the tables placed before that table, and an OR with a side whose
table comes later narrows nothing there; a side that compares with a subquery's
value waits for the subquery's table, as a column compared with one does.

A join that compares several columns of a table with one partner is one
condition. An import pane reads a cell's cards with `found.step = lookup.step
AND found.position = lookup.position AND found.cell = lookup.cell`, three
equalities of one join. A lookup for a change to `found` asks whether one row of
`lookup`, in its scope, holds all three values, not whether some row holds each:
a row of another cell that holds the step doesn't count for this cell's
position. One question reads the partner's scope once, where three questions
read it three times, and a partner whose scope joins more tables repeats its
whole chain in each. The pane's read ran past the 1,000 steps a lookup may run
when each question read the chain of tables behind `lookup`, and so ran again for
every label written to any verdict. The equalities of one join are one condition
when each compares a column of the joined table by `=` with a column of the
partner that the partner's scope doesn't fix, or with an expression over the
partner's columns. An equality with a subquery's value, one by `IS`, one with an
expression over the joined table's own columns, and one that sqlive passes into
a derived item's arm from an equality of the derived item each narrow alone,
since the rows that satisfy them need not be one row of the partner.

### LEFT JOIN

The ON of a
LEFT JOIN narrows only its right side: it decides which right rows join each
left row, and a left row none joins still appears, with
NULLs in their place. Any other condition on the right side, in the WHERE or an
inner join's ON, makes the LEFT JOIN an inner join. Each condition sqlive narrows
by is false where its column is NULL, except the conditions the next paragraph
lists, so a left row with NULLs in the right
side's place fails it, and can't appear whatever the LEFT JOIN's ON matched:
only the rows an inner join gives are left. So sqlive narrows such a join as an
inner join, by its ON both ways. The album page can list only the album's
producers, with `JOIN tracks AS t ON t.release_id = r.id LEFT JOIN track_artists
AS c ON c.track_id = t.id` and `WHERE r.album_id = ?1 AND c.role = 'producer'`.
That narrows `track_artists` to the producer credits on this album's tracks, so
crediting a producer on another album, or a singer on this one, doesn't rerun
the page.

Five kinds of condition can be true of the NULLs a missing right row gives: a
NULL test; a truth compared with a value; a condition SQLite judges, such as
`lower(c.role) = 'x'`; a join on `IS`; and an OR with a side that holds one of
these. On the right side, such a condition doesn't make the join inner, and it
narrows nothing there. The
album page can list the tracks no artist is credited on with `LEFT JOIN
track_artists AS c ON c.track_id = t.id WHERE c.track_id IS NULL`. Crediting an
artist on one of those tracks takes it off the page, so sqlive reads
`track_artists` whole for this page: narrowing it to the rows with a NULL
`track_id` would miss that credit. Another condition that makes the join inner,
such as `AND c.role = 'producer'`, lets the NULL test narrow `track_artists`
too. In the LEFT JOIN's own ON, as `ON c.track_id = t.id AND c.role IS NULL`, a
NULL test narrows the right side as any condition there does.

### The order of lookups

sqlive looks the tables of a join up one after another. Each takes values from
the tables before it, and the first one, a table that a condition of its own
narrows, takes values from none. An equality between two columns also carries a
condition on one of them to the other, so that first table still narrows by what
the other side's conditions say of the column the join compares. The album page
can list only the numbered tracks of a release, with `SELECT t.title FROM tracks
AS t JOIN releases AS r ON t.release_id = r.id WHERE r.id = ?1 AND t.position >
0`. sqlive looks `tracks` up first, for `t.position > 0`, and `r.id = ?1` with
the equality gives `tracks` the condition `t.release_id = ?1` too. A numbered
track added to another release doesn't rerun the page, and a numbered track
added to this release does. A CTE, a subquery in FROM or a view carries its own
tables' conditions the same way, through a column that passes a table's column
on: `tracks` joined to `WITH r AS (SELECT id FROM releases WHERE id = ?1 LIMIT
1)` by `t.release_id = r.id` narrows by `t.release_id = ?1`. A query with a
SELECT that groups its rows or calls an aggregate or window function carries
nothing out, even in a column its GROUP BY lists. A carried condition narrows
only the table it is carried to: it doesn't change which table comes first, and
an equality with an expression carries none.

A table narrowed by a subquery's value comes after the subquery's table, and the
tables that reach rows through it come after it. An import pane reads a cell's
cards with `FROM found AS f JOIN matched AS m ON m.verdict_id = f.verdict_id JOIN
label AS l ON l.verdict_id = m.verdict_id WHERE f.verdict_id = (SELECT v.verdict_id
FROM verdict AS v WHERE v.verdict_id = ?1)`. `found` waits for `verdict`, and
`matched` and `label` wait for `found`, since the only tables that narrow them
are ones that wait: placed first, they would take values from no table and read
every verdict's rows. So `matched` and `label` narrow to the cell's verdict with
no condition of their own, and a label added to another verdict doesn't rerun the
read. A group of joined tables waits when none of its tables has a condition of
its own or a partner placed already, and one of them is joined to a table that
waits.

### How a lookup compares

A lookup compares as the statement does. A join on an expression, which has no
affinity, gives the looked-up value the joined column's, so a text `'5'` the
expression makes equals the 5 an INTEGER column holds. So does a column of a
subquery that is a literal or a parameter, as in `(SELECT ?1 AS release) AS
page`: it has no affinity and compares by BINARY, so `t.release_id =
page.release` and `page.release = t.release_id` narrow `t` alike. Where sqlive
couldn't compare as SQLite does, the join narrows nothing: between columns of
different affinities, by a collation other than BINARY, as when its first side
is a NOCASE column, and on anything but an equality or `IS`. A column declared
with no type still gives its values to a column of another affinity: SQLite
compares a TEXT column with it as they are and gives its values an INTEGER
column's affinity, and the lookup does the same. A join on `IS` matches NULL
with NULL, and its lookup asks for a NULL the same way. An import joins a
candidate's failures to it by `f.hash IS v.hash AND f.run IS v.run`, where a run
can be NULL, so a failure the import writes for another candidate doesn't rerun
the list of this candidate's failures that the app has open. A join on `IS`
takes no values from a table a LEFT JOIN adds, whose missing row gives NULLs
that no lookup finds, and it can be true of the NULLs a missing right row gives,
so it never makes a LEFT JOIN an inner join. NATURAL, RIGHT and FULL joins
narrow nothing either. A join with USING narrows as the ON it stands for: `JOIN
r USING (k)` compares `r.k` with the `k` of the leftmost table before it that
has one, by that table's collation.

### Joins in parentheses, table functions and FTS5

A join in parentheses narrows as its joins do, as if its tables were joined
one after another, wherever that gives the rows SQLite gives. A people
screen lists each member with their person and avatar: `FROM members AS m LEFT
JOIN (people AS p LEFT JOIN files AS a ON a.id = p.avatar_id) ON p.id =
m.member`. SQLite joins `people` and `files` first, and gives a member whom no
person joins NULLs in both tables. Joining `people` to the member, then
`files` to the person, gives the same rows, since `a.id = p.avatar_id` can't
hold where `people` has no row. So the ON around the parentheses narrows
`people` as a LEFT JOIN's ON narrows its right side, and `files` narrows to
the avatars of the people it allows: a file that is no listed person's avatar
doesn't rerun the screen. SQLite reads a join in parentheses of two tables or
more that follows another item of the FROM as a SELECT of every column of its
tables, so a write to any column of `people` or `files` in those rows reruns
the screen. An inner join of a join in parentheses always
narrows this way, its ON read with the WHERE. A LEFT JOIN of one narrows
this way only when its ON names no table in the parentheses but the first,
holds no subquery, and each later table there joins by a LEFT JOIN whose ON
compares a column of a table before it, other than by `IS`. A statement with
any other join in parentheses narrows nothing, and so does one whose join in
parentheses has an alias, starts with another, or joins a table by USING.

A view the host names whole (section 3), or a table function, in a join narrows
the tables beside it, as `json_each(?1)` lists the ids an app passes in: a
lookup asks the view or the function. The view's own tables are read whole. A
table function of another table's columns, as `json_each(l.items)` or
`json_each(l.rowid)`, narrows nothing: it lists rows only beside that table's
rows, so a lookup can't ask it alone. Nor does one whose arguments read the
clock or a function that isn't deterministic, as
`json_each(json_array(CURRENT_TIMESTAMP))`: a write's lookup would ask it again
and get another list than the query did. A condition on a table's own rows
doesn't put it ahead of such a function: a list window that reads `json_each(?1)
AS w JOIN placement AS p ON p.key = unhex(w.value) AND p.item <> 'group'` still
narrows `placement` to the keys the function lists, so a write to the selection
rows of `p.key` that the window checks with `EXISTS (SELECT 1 FROM selection AS
chosen WHERE chosen.key = p.key)` reruns only the window listing that key.

An FTS5 table that keeps its own content narrows as a table does. FTS5 keeps
its rows in a content table of its own, and sqlive reads each change to that
table as the same change to the FTS5 table's row, so conditions on the FTS5
table narrow its rows and its joins narrow the tables beside it. A lookup
reads the content table, which costs fewer of SQLite's steps than the FTS5
table. A library search reads `a.id IN (SELECT album_id FROM
album_search WHERE title GLOB ?1)`: an edit to an album the search doesn't
find doesn't rerun it, and neither does a search row whose title the pattern
doesn't match. FTS5 deletes a row of its content table and inserts it again
when an UPDATE names it. sqlive judges the row's net change, one update of the
columns whose values changed, so an UPDATE of a column the search doesn't read
doesn't rerun it. A statement that names
a column FTS5 adds narrows nothing, since sqlive can't judge one on a single
row: `rank`, whose value depends on the table's other rows, and the column
that bears the table's name, which `MATCH` and `bm25` name. An FTS5 table
without a content table of its own, and any other virtual table, narrows
nothing either.

### What a lookup costs

A lookup's cost depends on the app's indexes. One no index serves reads its
partner row by row, so it could cost a write as much as reading the whole table.
A lookup stops after 1,000 of SQLite's steps instead, and the query runs again
whatever the write's other lookups find, which is always right, and the write's
cost stays bounded. Over a partner of a few rows the lookup finishes and still
narrows. A test can count the work: with sqlive's test-utils feature, a
connection's `test_track_work` counts the SQLite steps of the app's statements
and of the lookups apart (section 12). A test runs the album page over 10
releases and then over 1,000, and credits an artist on another release's track.
The page and the write's lookup should run as many steps over both, and the test
fails if they grow.

### The index of waiting queries

sqlive judges a write only against the live queries whose scopes its rows
could pass. sqlive files each waiting live query under each table the query
reads and, when an equality or an IN list narrows the table, under the
values that the equality or list compares with. sqlive judges a change to a
row against the queries it filed under a value the row holds before or
after the change, and against those whose scope on the table has no
equality. With 200 album pages open, each reading `SELECT title FROM tracks
WHERE release_id = ?1` for its own release, sqlive judges a track a write adds
to one release against that release's page alone.

## 8. Subqueries, CTEs and views

A subquery is narrowed the same way, by its own conditions and by the values it
compares with columns of the query around it. An album page marks the tracks
that have no credited artist yet with `NOT EXISTS (SELECT 1 FROM track_artists
AS c WHERE c.track_id = t.id)`. sqlive narrows `track_artists` to the rows whose
`track_id` is one of this release's track ids, so a credit added to another
release's track doesn't rerun the page. Values pass into a subquery: whether a
subquery finds a row doesn't narrow the query around it, since NOT EXISTS keeps
the very rows EXISTS drops, and a condition inside a subquery on the outer
query's columns alone narrows nothing. Only a column the subquery returns as it
is comes out of one, as the next paragraphs say. A SELECT with no FROM, as
`SELECT EXISTS (...)`, depends on what its subqueries read, and each arm of a
UNION, INTERSECT or EXCEPT on the rows its own conditions allow.

### A column compared with a subquery's value

A column compared with the value a subquery returns narrows by the subquery's
source. An album grid shows each album's cover, picked as the cover of the
album's first release: `WHERE cover.id = (SELECT r.id FROM releases AS r WHERE
r.album_id = a.id ORDER BY r.album_order, r.id LIMIT 1)`. The value the subquery
returns is the `id` of a row of `releases`, and sqlive narrows `releases` to the
releases of the albums the window shows. So `covers` narrows to the covers of
those releases, as `JOIN releases AS r ON r.id = cover.id` would narrow it, and
a cover written for another album's release doesn't rerun the window. A cover
written for a release of an album the window shows does, even when that release
isn't the album's first. sqlive looks up the release of the cover, not the pick:
running the pick once for each album the window shows would cost more as the
window grows, and past 1,000 steps a lookup stops and the window runs again.
This holds for `col = (SELECT x.c ...)`, `(SELECT x.c ...) = col` and `col IN
(SELECT x.c ...)`, where the subquery's single column is a bare column `x.c` of
one of its own sources, and where both columns have the same affinity, or `x.c`
is declared with no type, and compare by BINARY, as a join's do. A subquery that
combines SELECTs by UNION, INTERSECT or EXCEPT returns rows its SELECTs return,
so it narrows by each of them when each returns such a column or a constant:
`hash IN (SELECT hash FROM scan WHERE path = ?1 UNION ALL SELECT hash FROM scan
WHERE path = ?2)` narrows `hash` to the hashes of either path. One with VALUES
narrows nothing. A row value narrows the same way, as `(a, b) = (SELECT x.c, x.d
...)` or `(a, b) IN (SELECT x.c, x.d ...)`: each pair narrows as that pair alone
would, since the comparison holds only where every pair is equal. A subquery
returning an expression, as `max(x.c)` or `x.c + 1`, or a column of the query
around it, returns no row's own value, so it narrows nothing, and neither do
`NOT IN`, `<>` and `<` with a subquery. The files beside the covers narrow the
same way: `files.id = cover.file` takes its files from the covers. A subquery
that names the compared column's own source, as `cover.id = (SELECT r.id FROM
releases AS r WHERE r.kind = cover.kind)`, narrows its own table without the
correlation: sqlive can't read the subquery's source before the covers it takes
values from.

### A subquery's own LIMIT

A subquery that keeps the first rows for each row around it narrows the way a
statement's LIMIT does. The album grid can show the title of each album's first
release with `(SELECT r.title FROM releases AS r WHERE r.album_id = a.id ORDER
BY r.album_order, r.id LIMIT 1)` as a result column. For each album, the
subquery returns the first row of the album's window: the releases whose
`album_id` holds the album's `id`, in the subquery's order. A release that
sorts after the first of its album's releases changes nothing the grid shows,
wherever it sorts in another album's window. So sqlive narrows `releases` to the
rows that fewer than LIMIT rows of their own window sort before. A release added
as an album's second doesn't rerun the grid. One added before the
album's first release does, and so do deleting the first release and moving a
release to another album where it sorts first. A release whose `album_id` is
NULL sits in no window, since `=` holds of no NULL. To judge a write, sqlive
asks SQLite for the first LIMIT rows of the changed row's window, in the
subquery's order, and whether the changed row sorts before the last of them.
The question reads the database a lookup reads, so the run keeps no value.

The subquery is one SELECT of one table, with a LIMIT whose value is a positive
integer and no OFFSET, and with no GROUP BY, DISTINCT, aggregate or window
function, since there one row depends on many. Each of its conditions is one of
two kinds: a condition that names only that table's columns and that SQLite can
judge row by row, as a condition on one table's row (section 5), or an equality of
one of its columns with a value of the query around it that sqlive reads as a
join's. The rows those conditions keep for a row around the subquery are then
exactly that row's window. Each ORDER BY term is an expression over the table's
columns of the first kind, written out rather than as a position, with no
COLLATE and no NULLS FIRST or LAST. The window narrows only the table's own
scope, not the rows another table's lookup reads. The cover above,
`cover.id = (SELECT r.id ... ORDER BY r.album_order, r.id LIMIT 1)`, takes its
value from the row the subquery returns, which sits inside the album's window,
so `releases` narrows to each album's first release there too: a release that
moves after its album's first doesn't rerun the grid. A cover written for any
release of an album the grid shows still reruns it, since the lookup of
`covers` reads every release of the album, as above.

### Subqueries in FROM, CTEs and views

A subquery in FROM, a CTE and one of the app's own views are read as queries of
their own. A condition on one of their columns that passes a table's column on
unchanged narrows that table. A music app defines `CREATE VIEW album_tracks AS
SELECT r.album_id, t.title FROM releases AS r JOIN tracks AS t ON t.release_id =
r.id`, and its album page reads `SELECT title FROM album_tracks WHERE album_id =
?1`: that narrows `releases` to the album's releases and `tracks` to theirs, as
the join written out would. A LEFT JOIN's right side in the view takes the
condition too, which makes that join an inner join, as a WHERE condition does. A
condition passes into each arm of a compound, but not into a query that calls a
window function or has a LIMIT: a row of such a query depends on which other
rows there are, so a condition on it says nothing of each. Nor does it pass into
a query that groups its rows or calls an aggregate, whose row comes of many rows
of its tables, except a condition on a column the GROUP BY lists, which holds
the same value in each row of a group. A release page counts the release's files
that wait to upload with `LEFT JOIN (SELECT f.release_id, count(*) AS n FROM
waiting AS f GROUP BY f.release_id) AS w ON w.release_id = r.id`. Each row of a
group holds the group's `release_id`, so the join narrows `waiting` to this
release's files, and a file of another release that starts to wait doesn't rerun
the page. A condition on `n`, or on a column the query computes from the key,
narrows nothing there. A view the host names whole (section 3) is read whole,
since what it shows changes with what the host holds in memory too. A view
sqlive can't read this way, as one with a recursive CTE, is read whole; a lookup
still asks it, so it narrows the tables beside it. SQLite names each read inside
a view that sqlive reads whole by the innermost view or CTE the read goes
through. When the statement's own CTE or view has the name of one inside such a
view, sqlive can't tell their reads apart, so no read through that name narrows
its table. Otherwise a statement whose CTE narrows `releases` to one row would
take the view's reads of `releases` for the CTE's, and a write to another row,
which changes the view, wouldn't rerun the statement. When sqlive's parser can't
read such a view, sqlive doesn't know the names inside it, and no read through
any view or CTE narrows. A CTE gives the tables joined to it their values
wherever the statement reads it, in a subquery or in another CTE too, and so
does a subquery in FROM, as long as its SQL names no row of the query around it.
One that names such a row, as a subquery correlated with the row around it,
lists rows only beside that row, so a lookup can't ask it alone, and the tables
joined to it narrow by their own conditions only. A WITH inside a subquery or a
CTE can declare a CTE under the name of an outer one. Where the inner WITH's SQL
names it, the name means the inner CTE, as SQLite reads it, and an outer CTE
whose own SQL names it still reads the outer one. A FROM item that names such a
CTE narrows the tables of the CTE it reads. A statement with a recursive CTE of
its own, or with `x IN table`, narrows nothing: it depends on every row of each
table it read.

### What is read whole

sqlive logs at debug level each statement that reads a table or a view whole,
with the statement's SQL. The log names each table one of the statement's own
SELECTs, CTEs or views reads whole and each view sqlive reads whole, with the
reason. The log doesn't name the tables behind such a view, and for a statement
that narrows nothing it gives one reason for every table. A write to any row of
a table read whole can run the live query again, and nothing in the result shows
why. When a music app reads every one of its 100,000 tracks because a second
subquery of the screen's statement leaves out its WHERE, the log names `tracks`
and that subquery's name for it.

## 9. Bulk writes

Some changes rewrite a whole table that no host captures. A music app plays a
library of 100,000 tracks: it empties its play queue, a table that stays on the
device, and fills it again from the library. Through a write, the pre-update
hook notes each row change for live queries, and while a pre-update hook
watches, SQLite deletes a table's rows one by one instead of dropping its pages
at once. So the host runs that SQL as a *bulk write* instead
(`HostSql::bulk_write`): one transaction of plain SQLite, with no hook watching
it. Over eight runs on a busy machine, clearing the 100,000-row queue took a
median of 150 ms through a write and 6 ms through a bulk write, and filling it
with one `INSERT ... SELECT` 210 ms and 70 ms.

A bulk write keeps no row change, so its host decides which tables one may
change. As SQLite prepares each statement, the authorizer reports every table
the statement would write, including through the triggers and foreign-key
actions SQLite compiles into it, and the host's policy judges the statement in
the bulk scope (`StatementScope::bulk`). coven refuses there every statement
that would write a table whose changes a write must record (coven's spec,
[§4](../../spec/coven.md#4-writing-locally)). A bulk write may read any table
the app's SQL reads, so the queue fills from the library's tables, and the
app's triggers still run.

That same report tells live queries what a bulk write changed. sqlive keeps the
tables its statements would write, and after it commits, every live query that
read one of them runs again once, whatever rows it read. A table a statement
might change counts even when it didn't, as one a trigger writes only when its
`WHEN` holds. So a change of a few rows that should rerun only the live queries
reading those rows is a write, not a bulk write.

**Edge cases.**

- **A bulk write's statements aren't kept.** SQLite decides how a statement
  deletes as it prepares it, and empties a table in one step only where no
  pre-update hook watches. Kept, a bulk write's `DELETE FROM queue` would empty
  the table in one step in a later write too, unseen by the hook, and live
  queries reading the queue wouldn't run again. So sqlive prepares each of a
  bulk write's statements afresh and drops it after it runs.
- **A table SQLite can't empty in one step.** SQLite deletes row by row from a
  table with a trigger that runs on delete, or a foreign key into or out of it,
  even in a bulk write.

## 10. Upkeep

While a database stays open, sqlive checkpoints its WAL, keeps the query
planner's statistics, and applies the host's SQLite settings to every
connection. Each upkeep thread lives as long as the database. App SQL can't run
a PRAGMA, so the host chooses SQLite's settings through `Options::settings` when
it opens the database. A notes app imports a thousand notes while a live query
shows the list. It opens the database with a 64 MB page cache and sqlive's
checkpoints off. After the import it checkpoints the WAL itself with RESTART
(`HostSql::wal_checkpoint`), so the next writes reuse the WAL file from its
start.

### Checkpoints

Unless the host turns them off, sqlive checkpoints the WAL itself, and never
inside a commit. SQLite's own automatic checkpoint runs in the commit that takes
the WAL past 1,000 pages, which then waits for it, `F_FULLFSYNC` included on
macOS and iOS (coven's spec, [§3](../../spec/coven.md#3-what-coven-assumes)). A
live query that runs again after every commit keeps a reader's snapshot open
nearly all the time, so that checkpoint can't copy the whole WAL, and every
later commit runs another. A music app imported 2,000 small writes in 0.36 s
with nobody reading, and in 2.3 to 4.5 s with its list's live query running
again after each one, while the WAL grew to 23 MB. So none of the database's
connections runs a checkpoint as it commits. The writer doesn't, and neither
does the connection that keeps the planner's statistics while the database
stays open, because that connection's commits hold the writer. Each commit
wakes a thread of the database's own instead, which checkpoints on a connection
of its own once the WAL holds 1,000 pages, or the host's `wal_autocheckpoint`,
not yet copied. The thread copies what it can while writes go on and syncs the
database file, then holds the writer, copies and syncs what was committed
meanwhile, and waits up to 100 ms for the readers to leave the WAL, so the next
commit writes it from its start. A reader that started before then finishes
within that, and later ones read the database file alone. The same import took
0.42 s with the live query, and 0.47 s with a reader reading the whole table
again and again without a pause, and the WAL stayed under 5 MB. Once the WAL
starts over, its file shrinks back to 4 MiB, or the host's
`journal_size_limit`. A reader that holds one snapshot longer than the wait
keeps the WAL from starting over; sqlive tries again once the WAL has grown by
the same amount again. Closing the database still checkpoints the whole WAL and
removes its file.

The sync before the thread holds the writer keeps writes from waiting for it.
SQLite syncs the database file only when a checkpoint copies the whole WAL, and
while the app writes, a commit lands during nearly every copy and leaves it
short. The checkpoint holding the writer would then sync every page copied while
writes went on. So sqlive syncs that copy itself, the way SQLite syncs a
checkpoint, `F_FULLFSYNC` included on macOS and iOS, through SQLite's own handle
to the file: closing any other handle to it would drop every lock SQLite holds
on it in the process. A commit that lands during the copy gets a copy and sync
of its own, again while each leaves fewer frames behind, so the checkpoint
holding the writer copies and syncs only what was committed during the last of
them and since, and nothing when no commit landed. A music app imported 2,000
writes of ten releases each, every track with its file, some 1,860 pages of the
WAL a write, on a busy machine. Without these syncs its writes waited a median
29 to 42 ms at each checkpoint, 36 to 61 s over the import; with them, 18 to
24 ms, and 11 to 17 s. The import took about as long either way: the drive
writes the same pages, only not while the writes wait. These syncs come on top
of the checkpoint's own, which still comes before the WAL starts over. A copy
SQLite can't start, because another connection is checkpointing the WAL or
committing to it right then, is tried again at the next commit, without holding
the writer.

After each checkpoint that copies, sqlive's checkpointer's or the host's, the
database tells its host the main file's size (`Database::checkpointed_size`), so
the host can show the file's size without reading the file itself.

A checkpoint can fail. When SQLite finds the database busy or locked past its
wait, sqlive tries again at the next commit. So it does when SQLite can't start
the checkpoint at all: another connection was checkpointing the WAL, or was
committing to it just as SQLite read where it ends, and SQLite gives up at once,
reporting the database busy and no frames (`WalCheckpoint::NotStarted`). Nothing
is wrong then, only busy. Any other failure would stop sqlive's checkpoints for
good, and the WAL would then grow without bound while the database stayed open.
So a checkpoint that SQLite fails, as on a full disk, fails the handle with
`EngineFailure::Checkpoint` and SQLite's cause, which the host turns into its
own failure, as coven's `CheckpointFailed`. Every later call on that handle and
every live query fails with it until the host reopens the database, as after a
failed rollback.

The checkpointing thread lives exactly as long as the database: it starts as
the database opens, and closing stops it and waits for it to end before the
writer closes. It runs on no async runtime of the host's. A music app opened its
database on a runtime it shut down when its import window closed, and kept the
database open; a checkpointer that was that runtime's task would have ended with
it, and the WAL would have grown with every later write. A checkpoint that
panics is a bug in sqlive. The database must not stay open with no one
checkpointing it, so the thread fails the handle with the bug: the bug goes to
the handle's bug sink once, and every later call and live query fails with
`Error::HandleFailed` carrying it until the host reopens the database, as after
any failure that fails a handle. Closing the failed handle and opening the
database again starts a new checkpointer, which copies the WAL the next commit
past the threshold wakes it to. sqlive never reopens by itself: a panic that
happened once will likely happen again, and the host shows the failure and
offers the reopen. A system that refuses the thread fails the open with
`Error::UpkeepThread`.

### The planner's statistics

When `Options::statistics` asks, sqlive keeps the query planner's statistics
itself. SQLite plans a query by how many rows each table and index holds, which
`ANALYZE` records in `sqlite_stat1`; without them it guesses. So sqlive runs
`PRAGMA optimize` as a writable database opens, after `prepare` and before the
readers open, so they plan with what it finds, and again as the database closes.
A run analyzes each table that has no statistics, or whose size changed tenfold
since it was last analyzed. It reads at most 2,000 rows of each index, or the
host's lower `analysis_limit`, and fewer in a schema of over 100 tables and
indexes. On a coven store of four 20,000-row tables with two indexes each,
beside coven's own tables, the first run took about 2 ms and a run with nothing
to do about 0.2 ms, where analyzing every row took 13 ms. A run gathers no
`sqlite_stat4` samples, so the planner doesn't evaluate the host's functions
against them while it plans.

sqlive keeps no statistics for a table of fewer than 100 rows. A music app
restores its library on a new phone. Its database is created empty, except that
each FTS5 search index already holds two rows of its own. Statistics saying an
index holds two rows make SQLite read the whole index to clean up after each
merge of its pages, instead of looking up the pages' range, and FTS5 merges as
nearly every write to the index commits. So as the restore filled the index,
nearly every applied row read all of it, and each cleanup went from 38 µs to
233 µs. SQLite may plan to read a table that small whole: in SQLite 3.53
reading a range by rowid lost to a full scan for a table counted at 2 rows, and
a range of an index for tables counted at 8 to 50. Without statistics, SQLite
plans each table as a large one, which a small table answers as quickly. So
after each run sqlive forgets the statistics of every table under 100 rows, and
a later run gathers them once the table holds more.

The run that forgets them happens on a connection of its own as the database
opens. A connection that analyzes a table loads the count it found, and
forgetting the statistics afterwards doesn't unload it, so the writer would keep
planning with the small count. The writer and the readers then read what that
run kept.

A table can also fill while the database stays open. A music app's first scan
of its folder fills the scan's candidates from empty, and every candidate holds
the same folder root. Without statistics, SQLite takes a root to hold a few
rows, so it looks up the candidates under one file root by reading every
candidate of the root in order: at 10,000 candidates, 50,020 SQLite steps and
442 µs a lookup, against 56 steps and 18 µs through the file root's index. So
each time the writer's commits have changed 1,000 rows since the last check, a
thread of sqlive's own asks SQLite which tables a run would analyze, without
running it, and reads at most 100 rows of each. On that store of four
20,000-row tables, a check took about 80 µs. When one of those tables holds 100
rows or more, the thread runs on its own connection as an open's run does,
holding the writer so no write finds the database locked. So a table has
statistics within 1,000 changed rows of reaching 100 rows, or of SQLite
estimating it at ten times its counted size. SQLite reads statistics again only
as it reads the schema, so the run also creates and drops a table,
`sqlive_statistics_gathered`, in its
transaction. Every connection, the read-only readers among them and the run's
own, then reads the schema and plans with the new statistics from its next
statement. The thread lives as long as the database, and a panic in it fails the
handle with the bug, as with the checkpointing thread. A check or run that fails
other than on a busy or locked database stops it, and the handle fails with
`EngineFailure::Statistics` and SQLite's cause, which coven names
`StatisticsFailed`, until the host reopens the database: the planner would
otherwise plan with statistics that no longer hold for as long as the database
stayed open.

### Memory

Each connection keeps the pages it read cached, up to its `cache_size`, so the
next read needn't load them again. On a phone that memory counts against the
app: the OS warns an app when memory runs low, and ends a background app that
holds a lot. So the host can give it back: `HostSql::release_memory` gives back
the cached pages no statement on that connection is using, as SQLite's
`sqlite3_db_release_memory` does. Pages a transaction in progress changed and
hasn't written out stay, so a release never waits for a write. SQLite built with
`SQLITE_ENABLE_MEMORY_MANAGEMENT` keeps the pages no statement is using in one
pool for the whole process, and a release on any connection empties the whole
pool. So one release, on an idle reader, frees the writer's cached pages, the
checkpointer's and the planner statistics' too, and those of every other
database the process has open. A reader that had read every page of a database
with 200 rows of 4 KB, and the write of them waiting to upload, held 1.6 MB, and
10 KB after. The next reads load what they need from the database file again.

### The database's path on Windows

On Windows a database's path is verbatim (`\\?\D:\…`), and SQLite takes a path
that starts `\\` for a network share. There it locks the WAL's shared-memory
file through one handle for every connection in the process, and two
connections taking one lock at once can leave the handle holding it for good: no
checkpoint could then copy the WAL or start it over. So sqlive opens SQLite's
files by the plain path, `D:\…`, and refuses a database whose path holds a name
Windows reads as another file without the prefix: `.` or `..`, a name ending in
a dot or a space, or a device's name such as `NUL` (`Error::Path`).

### SQLite's settings

sqlive applies the host's settings to every connection it opens to the
database, as each one opens and before any statement of its own, so the writer
and the readers agree. `SqliteSettings` holds typed values, never PRAGMA text; a
field left `None` keeps SQLite's default, except `checkpoint_fullfsync`, which
is on for macOS and iOS, `journal_size_limit`, which is 4 MiB, and
`auto_vacuum`, which is `Incremental`. A value larger than SQLite can hold, such
as a busy timeout past `i32::MAX` milliseconds, fails the open with
`SqliteSettingOutOfRange`.

The page size and auto-vacuum mode belong to the database file instead: they
apply when the database is created, and an existing database keeps its own;
reopening it with others changes nothing. Where the host sets no auto-vacuum
mode, a new database is `Incremental`, so the host can give back the pages its
own writes free: `give_back_free_pages` gives back the free pages past 4 MiB,
the same amount the WAL file keeps (`journal_size_limit`), so the next writes
reuse those pages instead of growing the file. coven calls it each time a claim
of its writes lands. `Off` and `Full` keep their SQLite meaning, and sqlive
runs no vacuum on them.

`HostSql::sqlite_settings` reads back the settings SQLite has in effect, which
every connection shares, except that a writable database's writer and statistics
connection never checkpoint as they commit. SQLite lowers `mmap_size` and
`threads` to the limits it was built with, and the readback shows the lowered
value.

The settings sqlive's guarantees rest on stay fixed, and `SqliteSettings` has no
field for them. WAL mode lets readers keep their snapshots. `synchronous = FULL`
makes a committed write survive a power loss, except the latest ones on macOS
and iOS unless the host turns `fullfsync` on. Normal locking lets the readers
and other processes open the database. Foreign keys and recursive triggers are
on. An untrusted schema keeps triggers and views to functions marked safe for
them. SQLite's defensive mode refuses SQL that could damage the database file:
writing an FTS5 index's own tables directly rather than through the index,
writing the schema as a table, setting the schema's version number, and turning
the journal off. A notes app writing `note_search_data` itself could leave its
search index out of step with the notes it indexes. The host's own statements
need none of these.

Every SQLite setting, and who chooses it. Settings marked *host* are fields of
`SqliteSettings`; the rest have no field.

| Setting | Chosen by | Why, and when it applies |
|---|---|---|
| `busy_timeout` | host | how long a connection waits for a lock; unset keeps the 5 seconds rusqlite sets at open; RESTART and TRUNCATE checkpoints wait this long for readers |
| `cache_size`, `cache_spill` | host | memory each connection uses, and when a transaction writes changed pages early |
| `mmap_size` | host | reads through memory-mapped I/O; SQLite caps it |
| `temp_store` | host | where temporary tables live; set as each connection opens, before the host makes its temporary objects, since changing it drops them |
| `threads` | host | helper threads for sorting; SQLite caps it |
| `journal_size_limit`, `wal_autocheckpoint` | host | how large the WAL file stays once a checkpoint lets it start over, unset is sqlive's 4 MiB; and how many pages not yet copied the WAL holds before sqlive checkpoints it, off the commit path, unset is 1,000 (`CHECKPOINT_PAGES`); with sqlive's checkpoints off, the WAL grows until the host checkpoints. The writer's own `wal_autocheckpoint`, and that of the connection that keeps the planner's statistics, is always 0, and the readers' holds sqlive's |
| `automatic_index`, `analysis_limit` | host | query planning; `analysis_limit` bounds every `ANALYZE`, and sqlive's own reads at most 2,000 rows of each index whatever it is |
| `cell_size_check` | host | extra damage checks as pages are read |
| `secure_delete` | host | overwrites deleted content |
| `fullfsync` | host | on macOS and iOS, a commit also flushes the drive's own cache; unset keeps SQLite's default, off, so a power loss can take the latest commits |
| `checkpoint_fullfsync` | host | on macOS and iOS, a checkpoint also flushes the drive's own cache; unset is on there, so a power loss can't damage the database file once the WAL starts over |
| `page_size` | host, at creation | fixed once the file has its first page, and WAL mode never changes it |
| `auto_vacuum` | host, at creation | unset is `Incremental`, whose free pages past 4 MiB the host gives back with `give_back_free_pages`; turning the mode on or off later needs a `VACUUM`, which sqlive never runs |
| `journal_mode` | sqlive: WAL | readers keep their snapshots while a write commits |
| `synchronous` | sqlive: FULL | a committed write survives a power loss |
| `locking_mode` | sqlive: NORMAL | an exclusive lock would shut out the readers and other processes |
| `foreign_keys` | sqlive: ON | the schema's foreign keys hold on every connection |
| `recursive_triggers` | sqlive: ON | a trigger's own changes run the triggers they fire, as any other statement's do |
| `trusted_schema` | sqlive: OFF | triggers and views call only functions marked safe for them |
| `application_id`, `user_version` | host, through its own SQL | app SQL can't run a PRAGMA; coven keeps its versions in them |
| `schema_version`, `writable_schema` | untouched, and refused by defensive mode | writing them would corrupt the schema or get around the host's checks of it |
| `query_only` | sqlive: off | the writer commits the host's changes; readers open read-only |
| `defer_foreign_keys` | host, in its own transactions | it lasts one transaction, and every transaction is the host's |
| `ignore_check_constraints`, `legacy_alter_table`, `case_sensitive_like`, `reverse_unordered_selects` | SQLite's default, off | app SQL can't run a PRAGMA; coven's spec says which of them coven rests on |
| `encoding` | SQLite's default, UTF-8 | app SQL can't run a PRAGMA |
| `max_page_count` | SQLite's default, unlimited | app SQL can't run a PRAGMA |
| `lock_proxy_file` | none | it changes how other processes lock the file |
| `read_uncommitted` | no effect | it affects only shared-cache connections, which sqlive never opens |
| `soft_heap_limit`, `hard_heap_limit`, `data_store_directory` | the process | they set every connection in the process, not one database's |
| `temp_store_directory` | host, for the process | `set_temporary_directory`, before the first database opens; an Android host names one, since SQLite finds no folder there |
| `count_changes`, `default_cache_size`, `empty_result_callbacks`, `full_column_names`, `short_column_names` | nobody | deprecated, though the bundled SQLite still runs them: `count_changes` makes a write return a row count, the column-name pragmas rename result columns, `empty_result_callbacks` changes `sqlite3_exec`'s callbacks, and `default_cache_size` writes a cache size into the file's header |
| `parser_trace`, `sql_trace`, `vdbe_*`, `activate_extensions` | nobody | they exist only in SQLite's debugging or encryption builds |
| `wal_checkpoint`, `incremental_vacuum`, `shrink_memory` | host, as calls | `wal_checkpoint`, `incremental_vacuum` and `release_memory` on a connection |
| `optimize` | sqlive, as calls | sqlive gathers the planner's statistics at a writable open, after `prepare` and before the readers open, at close, and while the database stays open once a table reaches 100 rows or ten times its counted size, checked each 1,000 rows the writer changes; it keeps none for a table under 100 rows; app writes can't run `ANALYZE` |
| `foreign_key_check` | nobody | a call sqlive doesn't offer |
| `integrity_check`, `quick_check` | nobody | they read every row and index; a statement that meets damage fails with `DamagedDatabase` instead |
| `collation_list`, `compile_options`, `database_list`, `data_version`, `foreign_key_list`, `freelist_count`, `function_list`, `index_info`, `index_list`, `index_xinfo`, `lock_status`, `module_list`, `page_count`, `pragma_list`, `stats`, `table_info`, `table_list`, `table_xinfo` | nobody | reports, not settings |
| `sqlite3_limit` limits | SQLite's | sqlive sets none |
| `sqlite3_db_config` options | sqlive | double-quoted string literals off; defensive mode on, so SQL that could damage the database file fails, such as writing a virtual table's own tables directly; the query planner's stability guarantee on, so the plan of every statement, the host's and the app's, never depends on the values bound to it: otherwise each use of a cached statement whose plan read a bound value parses and plans it again, once per row of a write's lookups, and a `LIKE ?` gives up the index range it would take from a bound prefix; the rest at SQLite's defaults, not offered |

```rust
/// SQLite settings for every connection sqlive opens to a database. `None`
/// keeps SQLite's default, except `checkpoint_fullfsync`, on for macOS and
/// iOS, `journal_size_limit`, 4 MiB, and `auto_vacuum`, `Incremental`.
/// `wal_autocheckpoint` is when sqlive checkpoints, off the commit path. Each
/// field's PRAGMA is its name. `Default` leaves every one unset.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqliteSettings {
    /// At most `i32::MAX` milliseconds.
    pub busy_timeout: Option<Duration>,
    pub cache_size: Option<CacheSize>,
    pub cache_spill: Option<CacheSpill>,
    /// At most `i64::MAX`; SQLite lowers it to its build's limit.
    pub mmap_size: Option<u64>,
    pub temp_store: Option<TempStore>,
    /// At most `i32::MAX`; SQLite lowers it to its build's limit.
    pub threads: Option<u32>,
    pub journal_size_limit: Option<JournalSizeLimit>,
    pub wal_autocheckpoint: Option<WalAutocheckpoint>,
    pub automatic_index: Option<bool>,
    pub cell_size_check: Option<bool>,
    pub secure_delete: Option<SecureDelete>,
    pub fullfsync: Option<bool>,
    pub checkpoint_fullfsync: Option<bool>,
    pub analysis_limit: Option<AnalysisLimit>,
    /// Applies only when the database is created.
    pub page_size: Option<PageSize>,
    /// Applies only when the database is created.
    pub auto_vacuum: Option<AutoVacuum>,
}

/// The same settings as SQLite reports them, every one known.
pub struct SqliteSettingsInEffect {
    pub busy_timeout: Duration,
    pub cache_size: CacheSize,
    /// When on, never below the cache's own page count.
    pub cache_spill: CacheSpill,
    pub mmap_size: u64,
    pub temp_store: TempStore,
    pub threads: u32,
    pub journal_size_limit: JournalSizeLimit,
    pub wal_autocheckpoint: WalAutocheckpoint,
    pub automatic_index: bool,
    pub cell_size_check: bool,
    pub secure_delete: SecureDelete,
    pub fullfsync: bool,
    pub checkpoint_fullfsync: bool,
    pub analysis_limit: AnalysisLimit,
    pub page_size: PageSize,
    pub auto_vacuum: AutoVacuum,
}

/// Counts at most `i32::MAX` unless noted.
pub enum CacheSize { Pages(u32), Kibibytes(u32) }
pub enum CacheSpill { Off, Pages(NonZeroU32) }
pub enum TempStore { Default, File, Memory }
/// Bytes at most `i64::MAX`.
pub enum JournalSizeLimit { Unlimited, Bytes(u64) }
pub enum WalAutocheckpoint { Off, Pages(NonZeroU32) }
pub enum SecureDelete { Off, On, Fast }
pub enum AnalysisLimit { Unlimited, Rows(NonZeroU32) }
pub enum PageSize {
    Bytes512, Bytes1024, Bytes2048, Bytes4096,
    Bytes8192, Bytes16384, Bytes32768, Bytes65536,
}
pub enum AutoVacuum { Off, Full, Incremental }

/// SQLite's checkpoint modes (`PRAGMA wal_checkpoint`).
pub enum CheckpointMode { Noop, Passive, Full, Restart, Truncate }

/// What a checkpoint did, as SQLite reports it.
pub enum WalCheckpoint {
    /// SQLite read nothing of the WAL: another connection was checkpointing
    /// it, or committing to it as SQLite read where it ends. SQLite gives up
    /// on this at once, without waiting out the busy timeout.
    NotStarted,
    /// SQLite read the WAL and copied what it could.
    Ran {
        /// `Full`, `Restart` or `Truncate` couldn't finish within the busy
        /// timeout, because a reader or another writer held on.
        busy: bool,
        /// Frames in the WAL.
        log_frames: u32,
        /// Frames copied into the database, this time or before.
        checkpointed_frames: u32,
    },
}

/// How many free pages an incremental vacuum gives back.
pub enum IncrementalVacuum { All, Pages(NonZeroU32) }

impl<P: Policy> HostSql<P> {
    /// The SQLite settings in effect on this connection.
    pub fn sqlite_settings(&self) -> Result<SqliteSettingsInEffect, PolicyError<P>>;
    /// Checkpoint the WAL outside any transaction. Every checkpoint sqlive
    /// runs on a database, its checkpointer's or the host's, goes through here.
    pub fn wal_checkpoint(&self, mode: CheckpointMode) -> Result<WalCheckpoint, PolicyError<P>>;
    /// Give back free pages of an incrementally vacuumed database.
    pub fn incremental_vacuum(&self, pages: IncrementalVacuum) -> Result<(), PolicyError<P>>;
    /// Give back the free pages of an incrementally vacuumed database past
    /// the 4 MiB sqlive leaves for later writes to reuse.
    pub fn give_back_free_pages(&self) -> Result<(), PolicyError<P>>;
    /// Give back the cached pages no statement is using.
    pub fn release_memory(&self) -> rusqlite::Result<()>;
}
```

**Edge cases.**

- **A small table.** SQLite keeps no statistics for a table with no rows, and
  sqlive none for one under 100 rows, so every run analyzes each again, which
  reads fewer than 100 rows of each.
- **A failed handle.** Closing a handle that fails every call skips the
  statistics run, since its writer may still hold a transaction.

## 11. Building screens on live queries

The database is what the app knows, and everything a screen shows comes from it
by a query. Live queries make that the natural way to build a screen:

- **One source of truth: the database.** Every row a screen shows, their order,
  which ones a filter keeps, how many there are and which group each sits in, is
  in the database or follows from it by a query. The app keeps no second copy of
  rows to sort, filter or count in its own code.
- **Data flows one way.** The app writes to the database, and live queries carry
  each change to every screen that shows it. A screen never patches its own copy
  after a write, and a write never pushes values into a screen. When a write
  commits, sqlive reruns each live query that read what it changed, and the
  screen draws the new result.
- **SQLite is fast, local and the app's own.** There is no server to wait on and
  no round trip, so each screen is exactly the query it needs. Its SQL returns
  the rows the screen draws, in the order it draws them, with the counts beside
  them. The app's code turns those rows into its types and does nothing else
  with them.

coven's spec measures one such screen, an import list of scanned album folders
whose rows a dozen tables decide, built on a table that local triggers keep
([§4.1](../../spec/coven.md#41-showing-data)). Which commits run a live query
again is sections 4 to 8's: what its statements read, narrowed by their
conditions and joins.

### Making each screen one query

- *One live query per screen.* A `LiveQuery` serves a fixed screen, and a
  `ReconfigurableLiveQuery` one with parameters, such as a window, a filter, a
  sort or an id; replacing them reruns the same query (section 3).
- *Do the work in SQL:* joins, rules as `CASE`, filters, sorts, counts with
  `GROUP BY`, positions with `row_number()`, windows with `LIMIT` and `OFFSET` or
  by key. The query's closure only runs SQL, and holds its read snapshot and a
  reader connection while it runs. `process` maps rows to the app's types after
  the snapshot is released.
- *Read only what the screen draws.* Every table and column a query reads is a
  reason to rerun it, and a query that reads a whole table to show fifty rows
  pays for the whole table on every rerun.
- *Index what the WHERE and ORDER BY name,* so a window touches about the window.
- *Keep a value computed across tables in a table of its own.* When a screen
  sorts or filters by a value that several tables decide, a table holds it,
  indexed, and triggers keep it in the same write that changes its inputs. The
  value is never stale, because it commits with that write.
- *Keep reruns cheap.* A checkpoint can't start the WAL over while a read
  snapshot is open, so expensive reruns back to back keep the WAL growing
  (section 10).

**Memory holds only values with no table:** progress, bytes per second, a
playback position between saves, what the person has scrolled to or folded shut.
Each goes on its own stream, keyed by what it belongs to, to the part of the
screen that draws it. Such a value never decides which rows a list shows, their
order, or a count. If it would, it belongs in a table, or in a value the host
keeps in memory and shows through a view (section 3), as coven does for its own
state.

**Orders and filters SQLite lacks** are the host's own collations and functions,
which its setup registers on every connection (section 1). A list sorted so
"Disc 9" comes before "Disc 10" names a natural-order collation, and a search
that finds "École" for "école" compares a Unicode case fold. Both can back an
index, so the sort or filter reads it like any other.

**Edge cases.**

- **An optional key.** `WHERE ?1 IS NULL OR id = ?1` depends on the whole table,
  and SQLite can't use the key's index for it. Two statements, one for each
  case, read by key.

## 12. Testing what a statement depends on

A test can check what a live query depends on without running a write.
`read_report`, with sqlive's test-utils feature, says which rows of each table
and view a statement depends on when a live query runs it, before any value is
bound. `SchemaProbe::read_report` gives the same report for a statement over a
schema in memory, with no rows (the crate's [README](README.md)). A test that
asserts the report of an album page fails when the page's statement starts to
read `tracks` whole.

The report takes each parameter that a column is compared with to hold a value
that isn't NULL and that SQLite compares as it is, and takes each run to return
as many rows as its LIMIT. A table is *keyed* by a column when every read of it
compares that column by `=`, `IN` or `IS` with a value, or joins it to a column
that the joined read itself compares so. A table is *looked up* when sqlive
narrows every read of it but no one column is compared in all of them. A table
is *whole* when some read of it depends on every row, and the report then gives
the reason for each such read. A view read whole is whole too, and so is each
table the view reads, whatever the report says of that table's other reads. A
statement sqlive can't narrow at all has one reason and no entries. The report's
text holds one line for each entry.

A test can also count the work. A connection's `test_track_work` counts into a
`ConnectionWorkCounter`, whose `take` returns the `ConnectionWork` since the last
take: the commits, the reads of SQLite's schema cookie, the SQLite steps of the
app's statements that return rows and of those that write, the steps of the
lookups apart, the live queries each write judged, the questions that stopped at
`LOOKUP_STEPS`, and the scope plans built, with their size and build time. A
test that runs the same read, or the same write beside the same live query, over
a small table and a large one fails when its steps grow with the table.

A live query's `runs` gives its `LiveQueryRuns`: the runs that have finished, of
those the runs a write caused with no new request, and of those the runs whose
result equaled the one before, which the query didn't return. A test of which
writes rerun a query asks `is_marked_for_rerun`, which answers at once whether a
commit since the query's last result marked it to run again.

```rust
/// What the live query that reads `whole` whole depends on when it runs
/// `sql` on `connection`.
#[cfg(feature = "test-utils")]
pub fn read_report(
    connection: &rusqlite::Connection,
    sql: &str,
    whole: &WholeViews,
) -> rusqlite::Result<ReadReport>;

/// What a live query's statement depends on. Its text holds one line for each
/// entry.
pub enum ReadReport {
    /// sqlive can't narrow the statement: it depends on every row of each
    /// table it reads.
    Whole { why: String },
    /// One entry for each table it narrows or reads whole and each view it
    /// reads whole, by name.
    Entries(Vec<ReadEntry>),
}

pub struct ReadEntry {
    /// The table or view, lowercase.
    pub object: String,
    pub rows: ReadRows,
}

/// Which rows of one table or view a statement depends on.
pub enum ReadRows {
    /// Those that hold one of a few values in `column`.
    Keyed { column: String },
    /// Those each read narrows it to, with no one column that all of them
    /// compare with values.
    LookedUp,
    /// Every row, with the reason for each read that nothing narrows.
    Whole { why: Vec<String> },
}

/// How often a live query had run when asked.
#[cfg(feature = "test-utils")]
impl LiveQueryRuns {
    /// The runs that have finished.
    pub fn runs(&self) -> u64;
    /// Of those, the runs a write caused, with no new request.
    pub fn writes(&self) -> u64;
    /// Of those, the runs whose result equaled the one before, which the
    /// query didn't return.
    pub fn equal(&self) -> u64;
}

/// How much of a statement sqlive read to build its scope plan, up to where
/// it stopped, if it did.
pub struct ScopePlanSize {
    /// Sources: the items of each FROM, those of the subqueries, CTEs and
    /// views read as their own included.
    pub sources: usize,
    /// SELECT cores.
    pub cores: usize,
    /// Edges between the sources: each an equality that the rows of one
    /// source meet with a value of another's, by which sqlive narrows it. One
    /// equality can give an edge each way, and an OR's sides give none.
    pub edges: usize,
}
```

## 13. Reference

### Errors

A call fails with an `Error<F, R>`, where `F` is the host's failure type, which
a handle the host failed carries as it is, and `R` is the type the host's
policy refuses with.

| Error | Meaning | Section |
|---|---|---|
| `Sqlite` | SQLite refused or failed a statement, with its error | 1 |
| `DamagedDatabase` | a statement met damage SQLite reports (`SQLITE_CORRUPT`, `SQLITE_NOTADB`) | 10 |
| `Refused` | the host's policy, or one of the host's SQL functions, refused a statement | 1 |
| `StatementForbidden` | app SQL tried transaction control, a PRAGMA, ATTACH, loading an extension, changing the schema outside a migration, `ANALYZE` in a write, or writing in a read | 1 |
| `TransactionEnded` | SQLite rolled the transaction back itself, so it can't go on | 2 |
| `Closed` | the handle closed the database | 3 |
| `Bug` | the call's work panicked, a bug, which the caller reports | 2 |
| `HandleFailed` | an earlier bug may have left the handle's state wrong, so every call fails with it until the host reopens the database | 10 |
| `Host` | the host failed the handle with its own failure, one of sqlive's `EngineFailure`s among them | 2, 10 |
| `Invariant` | sqlive broke a rule it relies on: a bug, not damage | |
| `WalUnavailable` | SQLite kept another journal mode instead of WAL | 1 |
| `SqliteSettingOutOfRange` | a SQLite setting is larger than SQLite can hold | 10 |
| `UpkeepThread` | the system refused a thread of sqlive's upkeep, so the database did not open | 10 |
| `Path` | opening the database's file failed; only on Windows can a name in the path mean another file once sqlive drops the path's verbatim prefix | 10 |
| `Rollback` | a transaction failed and rolling it back failed too | 2 |

An `EngineFailure` is a failure of sqlive's own that ends the handle: every
later call and live query fails with the host's failure built from it.

| Failure | Meaning | Section |
|---|---|---|
| `Rollback` | rolling back a failed transaction failed, so a connection may still be inside it | 2 |
| `Checkpoint` | one of sqlive's WAL checkpoints failed, other than on a busy or locked database, so checkpointing stopped | 10 |
| `Statistics` | checking or gathering the planner's statistics failed, other than on a busy or locked database, so sqlive stopped keeping them | 10 |
| `Hook` | SQLite refused to remove a transaction's hook, so what the connection calls next is unknown | 2 |

### Constants

| Constant | Value | Section |
|---|---|---|
| `LOOKUP_STEPS` | 1,000: the most SQLite steps one question about a changed row runs before the query runs again | 7 |
| `ROWS_PER_TABLE` | 10,000: past this many changed rows of one table, a write keeps only that the table changed | 2 |
| `CHECKPOINT_PAGES` | 1,000: the WAL size at which sqlive checkpoints where the host leaves it unset | 10 |

### What's proved

A Lean model in [proofs/](proofs/) states how sqlive narrows a live query to the
rows its statements can depend on, and which writes therefore run the query
again. Each row names a claim in plain words, its theorems, the file that proves
them and the section of this document it supports. A theorem proves what it
states about its model, under that model's assumptions; it doesn't prove the
Rust code. The model's write-up, [proofs/README.md](proofs/README.md), explains
the model and states its assumptions.

| Claim | Theorems | File | Section |
|---|---|---|---|
| A condition on one item's own columns, true of a binding, makes the term sqlive reads from it true of the item's row | `term_sound` | `proofs/SqliveNarrowing/Rules/Term.lean` | 5 |
| An equality between two items, true of a binding, makes the member column hold a value the partner's row gives | `edge_sound` | `proofs/SqliveNarrowing/Rules/Edge.lean` | 7 |
| An expression of which each operation gives NULL where an operand is NULL is NULL where every column it names is NULL | `eval_null` | `proofs/SqliveNarrowing/Reader.lean` | 7 |
| COALESCE, a CASE and `IS NULL` each make an equality with them true where the item they name has no row | `nonstrict_holds_without_partner` | `proofs/SqliveNarrowing/Rules/Edge.lean` | 7 |
| A row value equality between items, true of a binding, makes each pair's member column hold a value the partner's row gives | `row_edge_sound` | `proofs/SqliveNarrowing/Rules/RowEdge.lean` | 7 |
| A column equal to COALESCE of values, or equal to or IN a compound's rows, true of a binding, holds the value one argument or one arm gives | `one_of_sound` | `proofs/SqliveNarrowing/Rules/OneOf.lean` | 7, 8 |
| An OR, true of a binding, makes one side of each OR sqlive reads from it hold, each of its terms and edges as its rule promises | `or_sound` | `proofs/SqliveNarrowing/Rules/Or.lean` | 7 |
| `IS` between two items, true of a binding, makes the member column hold a value the partner's row gives, or both NULL | `is_edge_sound` | `proofs/SqliveNarrowing/Rules/IsEdge.lean` | 7 |
| A column equal to or IN a subquery's value holds a value the subquery's source gives in a binding its core keeps that gives a row the subquery returns, whatever columns the subquery returns after its first | `subquery_edge_sound` | `proofs/SqliveNarrowing/Rules/SubqueryEdge.lean` | 8 |
| A partner column of no affinity, which a lookup reads as an expression for a member column of another affinity, gives each row the value it holds | `columnValue_eval` | `proofs/SqliveNarrowing/Plan.lean` | 7 |
| Each column of a row value equal to or IN a subquery's row holds the value the subquery's source gives in its place, in a binding its core keeps that gives a row the subquery returns | `row_subquery_edge_sound` | `proofs/SqliveNarrowing/Rules/RowSubqueryEdge.lean` | 8 |
| Each condition's narrowing keeps the promise of the rule that reads it | `readCondition_sound` | `proofs/SqliveNarrowing/Rules.lean` | 5 to 8 |
| A term, an edge that compares by `=` or an OR that a row of NULLs fails, true of a binding with no row of its item, has an item no LEFT JOIN adds, or an edge's partner outside a subquery that no LEFT JOIN adds and the binding has no row of | `holds_none`, `HoldsIn.bound` | `proofs/SqliveNarrowing/Reader.lean` | 7 |
| A derived item's row holds its arm's item's value in each column the arm passes on, so a term true of the row is true of that item's row; a row of an arm that groups its rows holds, in each column its GROUP BY lists, the value of that column's item in every binding of its group | `push_down_sound`, `renamed_sound`, `group_key_sound` | `proofs/SqliveNarrowing/Rules/PushDown.lean` | 8 |
| An equality between two items, true of a binding, carries a term true of one side's row on its compared column to the other side's column; a derived item's row whose column isn't NULL passes what each of its arms' items' terms say of the column the arm passes on | `transfer_sound`, `pulled_sound` | `proofs/SqliveNarrowing/Rules/Transfer.lean` | 7 |
| A view, or a FROM subquery in no other core, returns the same rows under every binding, so a lookup reads it alone | `view_sound` | `proofs/SqliveNarrowing/Rules/View.lean` | 8 |
| A FROM subquery or CTE whose SQL names no row outside it returns, under a binding of none of its own items, the rows it returns alone, so a lookup reads it alone wherever the statement reads it | `closed_eval`, `closed_sound` | `proofs/SqliveNarrowing/Rules/View.lean` | 8 |
| An expression over one item's own columns, compared with another item's value, has in a binding the value it gives that item's row, so an equality with it narrows the item as one with its column does | `row_member_sound` | `proofs/SqliveNarrowing/Rules/RowMember.lean` | 7 |
| A parenthesized join gives the rows its tables give joined one after another: an inner join of one always, its ON read with the WHERE, and a LEFT JOIN of one when its ON names no table of the join but the first and each later table is a LEFT JOIN's whose ON can't hold where a table before it has no row | `left_group_flat`, `inner_group_flat` | `proofs/SqliveNarrowing/Rules/Parenthesized.lean` | 7 |
| A FROM item that names a CTE reads the innermost CTE of that name visible where it is, and a WITH that doesn't declare the name changes nothing of what it names inside, so an outer CTE's query reads the outer CTEs | `resolve_inner`, `resolve_outer` | `proofs/SqliveNarrowing/Rules/CteNames.lean` | 8 |
| A full run of a statement with a LIMIT depends only on the rows that sort at or before its last row; an ORDER BY term that is a column or an expression over one table's columns, in any collation that orders values by a key, gives each binding the value its key gives the table's row; a run of terms over one table bounds that table's rows at or before the last row's values of the run, a row that ties with the last row's values counting as at or before them, and a later run's table by those rows or by the rows that join an earlier run's table's rows that sort before that run's values, so each table's bound passes its row in each binding that sorts at or before the last row | `limit_sound`, `sortKey_value`, `termKey_value`, `sortedBefore_passes`, `atOrBefore_passes`, `runBound_passes`, `joined_passes` | `proofs/SqliveNarrowing/Rules/Limit.lean` | 6 |
| A LIMIT or OFFSET with no ORDER BY can keep any of its rows, as SQLite's plan orders them; a query that aggregates with no GROUP BY, or whose ORDER BY sorts by each table's rowid, keeps the same rows in every order; an aggregate's column outside every GROUP BY term can show any of its group's rows, `group_concat` with no ORDER BY of its own joins its values in the order SQLite reads them, and `max` and a GROUP BY term each keep the first of the values that compare equal that they read; DISTINCT, UNION, INTERSECT and EXCEPT keep one of each set of rows that compare equal, which the order they read them in decides; `row_number()` numbers rows its window's ORDER BY doesn't tell apart in the order SQLite reads them; any other SELECT gives the same rows in every order; a change to any column of a table that a query whose rows SQLite picks, or a SELECT whose rows follow that order, reads reruns it | `post_picks_any`, `fullyOrders_sound`, `bare_picks_any`, `concat_follows_order`, `max_keeps_first`, `distinct_keeps_first`, `rowNumber_follows_order`, `orderFree_rows_perm`, `picked_column_reruns` | `proofs/SqliveNarrowing/Rules/Pick.lean` | 6 |
| A statement read whole reruns on every change of a column it reads | `whole_sound` | `proofs/SqliveNarrowing/Rules/Whole.lean` | 4 |
| A row's update that FTS5 makes as a delete and an insert reruns a statement wherever the update would | `rewrite_reruns` | `proofs/SqliveNarrowing/Rules/Virtual.lean` | 7 |
| A statement read whole reruns on every write that any scopes rerun it on, so sqlive's judge, which narrows nothing for a write whose question stopped at the step limit, lets a write pass only when the judge without the limit does | `reruns_whole`, `Statement.rerunsLimited_sound` | `proofs/SqliveNarrowing/Judge.lean` | 7 |
| A statement returns the same rows from a database masked to the columns it reads, as sqlive keeps a changed row, as from the database | `result_mask` | `proofs/SqliveNarrowing/Mask.lean` | 2 |
| A write the judge lets pass leaves each slot whose masked row it changes holding rows that fail the table's scope before and after it, and each masked table holding the same rows that pass the table's scope, a slot it changes more than once included; a statement read whole then returns the same rows after the write | `Writes.unjudged`, `unjudged_slots`, `unjudged_rows`, `whole_narrowing_sound` | `proofs/SqliveNarrowing/Unjudged.lean` | 2 |
| A well-formed statement's query returns the same rows with any fuel past its number, so a lookup of a derived item whose arms sit in no core reads the rows the statement reads of it | `evalQuery_fuel`, `lookupRows_derived` | `proofs/SqliveNarrowing/Fuel.lean` | 8 |
| A well-formed statement's own query names no item outside its cores, and a core run under a binding that holds a row of each outer item its query names that no LEFT JOIN adds runs its derived items' queries and its subqueries under such bindings too | `outer_root`, `outer_derived`, `outer_sub`, `outer_subOn` | `proofs/SqliveNarrowing/Outer.lean` | 8 |
| A query returns the same rows under two bindings that hold the same rows of the outer items it names, so each ON condition of a LEFT JOIN holds of each binding its core keeps that holds a row of the join's item | `evalQuery_outer_congr`, `Statement.on_kept` | `proofs/SqliveNarrowing/Congr.lean` | 7 |
| A subquery of one table with an ORDER BY over that table's columns, a LIMIT and no OFFSET, each of whose conditions names only that table's columns or compares one of its columns by `=` with a value around it, returns for each binding around it the first rows of that binding's window; a row passes the window's scope exactly when it holds no NULL in a compared column and fewer than LIMIT rows its core keeps sort before it, so either every binding of the core passes, or the first LIMIT do in the database the write leaves; the subquery returns the same rows from two databases that hold the same bindings that pass; a binding that gives a row the subquery returns, as the one a comparison takes the subquery's value from, sorts after fewer than LIMIT rows in its database, and so passes the window in the database the write leaves when both databases hold the same rows that pass the table's test | `read_facts`, `partitionOf_holds`, `window_bindings`, `window_kept`, `window_passes`, `take_sortBy_before`, `count_first`, `take_sortBy_window`, `window_queryRows`, `plan_windowRows`, `query_row_at`, `givesRow_count`, `givesRow_windowTest`, `givesRow_windowPasses`, `nested_windowPasses` | `proofs/SqliveNarrowing/Rules/Window.lean`; `proofs/SqliveNarrowing/Windows.lean`; `proofs/SqliveNarrowing/Rules/Limit.lean`; `proofs/SqliveNarrowing/Restrict.lean`; `proofs/SqliveNarrowing/Rows.lean`; `proofs/SqliveNarrowing/Kept.lean` | 6 |
| Two databases that hold the same rows passing each item's test give a core the same bindings that decide its query's rows, and the statement the same rows, when every such binding holds rows that pass their tests; an arm that aggregates makes the same rows that pass a test from the bindings whose GROUP BY values a row that passes it holds as from all its bindings; when the judge lets a write pass, each masked table holds the same rows that pass the scope of an item reading it | `joinItems_restricted`, `coreBindings_restricted`, `Core.result_filter_key`, `queryRows_restricted`, `agree_of_kept`, `result_restricted`, `scoped_rows` | `proofs/SqliveNarrowing/Restrict.lean` | 5 to 8 |
| A statement whose own query is one SELECT with an ORDER BY, a LIMIT and no OFFSET, whose run returned as many rows as its LIMIT, returns the same rows from two databases whose bindings that sort at or before the last row, and whose other queries' bindings, hold rows that pass their tests | `limit_restricted` | `proofs/SqliveNarrowing/Restrict.lean` | 6 |
| A placed item's scope reads the scopes of the items placed before it, and passes a row of which its terms and carried terms hold, whose value each membership's partner gives from a row in the partner's scope, and whose values of the memberships that join one partner in the same join one row of the partner gives together, that a side of each OR it keeps passes, and that its LIMIT bound passes | `scopes_eq`, `scopeAt_passes`, `groupScopes_passes`, `joinedAll_passes_joined`, `membershipScope_passes`, `equalValues_passes`, `sideScope_passes`, `bind_passes` | `proofs/SqliveNarrowing/Scopes.lean` | 7 |
| The planned state holds an ON narrowing back only for an item a LEFT JOIN adds; each edge the planner passes into a derived item's arms comes from an edge the derived item holds, through the result column an arm passes the target's column on to, and makes the target an inner join's; the planner places each item once, and every item of every core; and each edge of the planned state whose partner is a subquery's source has its partner in an arm of some core's subquery, and each item's memberships are edges from it; and each item's memberships and the ORs it keeps take values from items placed before it | `plan_leftOn`, `plan_pushed`, `plan_order`, `plan_nestedOK`, `plan_placedBefore` | `proofs/SqliveNarrowing/PlannerState.lean` | 7 |
| The planner reads no core twice, and reads every core that sits in an arm of a query a core reads, at any depth, before that core | `readOrder_postorder` | `proofs/SqliveNarrowing/ReadOrder.lean` | 8 |
| The planner's state before it reads any condition, after it reads each core's conditions, and after it carries terms across a core's edges, says only what holds of every binding a core keeps: an item it holds no ON narrowing back for has a row, each term and carried term holds of its item's row, each edge and OR of an item with a row holds, and what it holds back for a LEFT JOIN's item holds where that item has a row | `PState.Sem.initial`, `PState.Sem.readConds`, `PState.Sem.carry` | `proofs/SqliveNarrowing/Facts.lean` | 5 to 8 |
| The planner only adds to an item's term; passing a term into a derived item's arms, and reading each core after every core below it, leaves the planner's state saying only what holds of each binding a core keeps and, below a core, of each binding that matters to its derived item's test | `PState.Sem.pushDown`, `PState.Sem.readCore`, `plan_readSem` | `proofs/SqliveNarrowing/ReadPhase.lean` | 8 |
| Placing each item, and passing the edges it takes into its arms, leaves the planned state saying only what holds of each binding a core keeps that matters to its query's test, as the planner's scopes give that test; each item's memberships are edges of the planned state | `PState.Sem.pushEdges`, `PState.Sem.placed`, `plan_sem`, `queryTest_good`, `plan_semTests`, `plan_membersEdges` | `proofs/SqliveNarrowing/OrderPhase.lean` | 7 |
| Every edge of the planned state, every edge of a side of each OR and every narrowing an ON holds back takes values from an item a lookup reads alone; each OR an item keeps is one of its ORs, each edge passed into a derived item's arm has a member its core lists, and the planner places only items a core lists; a derived item a lookup reads alone runs its query under any binding as under the binding of no row; every binding of an arm matters to a query whose every row matters, and the test of the statement's own query keeps every row | `plan_lookups`, `plan_order_listed`, `outerNames_readsAlone`, `evalQuery_readable`, `outer_readable`, `relevant_of_all`, `queryTest_root` | `proofs/SqliveNarrowing/Partners.lean` | 7 |
| In a binding that matters of a core run before or after the write, each row passes its item's test: its terms hold, its memberships' and kept ORs' partners, which the order places before it, give its values from rows of their lookups in their scopes, the edges passed into it pass, and its LIMIT bound passes; a row of an item a lookup reads alone is a row of the lookup; so such a binding holds no row only of an item the planned state holds an ON narrowing back for | `partner_found`, `pushed_passes`, `plan_kept_rows`, `plan_restricted` | `proofs/SqliveNarrowing/Kept.lean` | 7 |
| While the planner holds an ON narrowing back for a LEFT JOIN item, it adds to the item only what the item's ON says of it, and every other step leaves the item as it was; so each term, edge and OR the planned state holds of such an item holds wherever the item's ON holds of a row of it, and a row the ON holds for, in a binding whose rows pass their tests, passes the item's test | `untouched_narrow`, `untouched_pushDown`, `plan_held`, `plan_left_passes` | `proofs/SqliveNarrowing/Held.lean` | 7 |
| The planner's scopes keep their promise for every query at every fuel at which two databases return the same rows that matter; a write the judge lets pass, with the scopes the planner gives the run that read the database before it, leaves a well-formed statement's rows unchanged, the statement's own query with a LIMIT bound included, and so does a write sqlive's judge lets pass when a question stopped at the step limit | `plan_kept`, `narrowing_sound`, `narrowing_sound_limited` | `proofs/SqliveNarrowing/Sound.lean` | 5 to 8 |
| A row a scope passes holds one of its key's values in the key's column, so the index of live queries finds each query for every change that reruns it; the judge through the index reruns each query on exactly the writes the judge of every change reruns it on, and so lets pass only writes that leave a well-formed statement's rows unchanged | `key_passes`, `lookup_complete`, `index_judges_alike`, `indexed_narrowing_sound` | `proofs/SqliveNarrowing/Index.lean` | 7 |
| A write's net change of each row, from the row before its first change to the row after its last, takes the database before the write to the database after it, and holds exactly the rows that differ between the two; so the judge of the net changes lets pass only writes that leave a well-formed statement's rows unchanged, directly or through the index, its verdict depends only on the databases before and after, a write whose every row ends as it began reruns nothing, and it reruns a statement only where the judge of every change does | `firstOld_eq`, `lastNew_eq`, `net_writes`, `mem_net`, `net_sound`, `indexed_net_sound`, `net_judges_states`, `net_unchanged`, `net_reruns_imp` | `proofs/SqliveNarrowing/Net.lean` | 2 |
| When the judge lets a write pass, the tables before and after it hold the same rows that pass each item's test from the planner's scopes and passed edges, and a derived item's test keeps only rows that matter to its query; in a binding of the statement's own core that sorts at or before the last row, the LIMIT bound of each item the ORDER BY sorts passes the item's row when its memberships to other such items hold | `plan_restriction`, `Planned.bound_passes` | `proofs/SqliveNarrowing/Tests.lean` | 6 |
| Each result's assumptions and where its model stops | (write-up) | `proofs/README.md` | 4 to 8 |
