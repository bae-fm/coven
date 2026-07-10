# `sql_read`: journal-free reads on the full handle

## Situation

`CovenHandle::sql` is the only SQL entry point on the full handle, and it
journals every transaction: attach a SQLite session over the synced tables, run
the closure, capture the changeset (`Database::run_pending_journaled_transaction_on`).
A pure read pays session-attach for nothing, runs on the single writer
connection thread (queuing behind writes), and triggers the
"journaled transaction produced no synced changes" debug line — which host
read traffic fires tens of times per second (coven-torrent's piece storage is
the live example).

PR #203 already built the right machinery for the reader side:
`Database::open_read_only` opens a `SQLITE_OPEN_READONLY` connection on its own
connection thread against the same WAL database, with schema-too-new refusal
and synced-table validation. `CovenReadHandle::sql` exposes it as
`FnOnce(&rusqlite::Connection)`. This plan gives the full handle the same read
path, sharing that machinery at the `Database` level.

## Design

### `CovenHandle` gains a read-only companion connection

- `CovenBuilder::open()` (crates/coven/src/coven.rs), **after** the writer
  `Database::open` returns (migrations complete, schema exists), additionally
  opens `Database::open_read_only` on the same `db_path` with the same tables /
  grace / device id / migrations, and threads it into `CovenHandle::new` as a
  new field `read_db: Database`. A failure to open it fails `open()` loudly —
  no handle without its read path.
- `Database` is already `Clone` (clones share the connection thread), so
  `CovenHandle`'s existing `Clone` keeps working: all clones share one reader
  connection/thread, concurrent with (not queued behind) the writer thread.
  WAL makes this safe: many readers coexist with the one writer, each read
  seeing the last committed state.

### `CovenHandle::sql_read`

Signature identical to `CovenReadHandle::sql`, so a host closure written
against `&rusqlite::Connection` runs on either handle:

```rust
pub async fn sql_read<F, R>(&self, f: F) -> CovenResult<R>
where
    F: FnOnce(&rusqlite::Connection) -> CovenResult<R> + Send + 'static,
    R: Send + 'static,
{
    let outcome = self.read_db.call(move |conn| Ok(f(conn))).await
        .map_err(CovenError::from)?;
    outcome
}
```

No session, no journal, no stamper. The connection is `SQLITE_OPEN_READONLY`,
so a write statement inside the closure fails at the SQLite layer — reads
can't silently escape the sync journal; they can't write at all.

Doc contract to state on the method: read-your-writes holds for committed
writes — a `sql_read` issued after an awaited `sql()`/`write()` sees that
data (WAL reader reads the last committed state). It may not see another
task's write that has not yet committed.

### wasm

`Database::open_read_only` is native-only, and wasm has a single borrowed
connection. There, `sql_read` runs on the one connection via the existing
wasm `call`, wrapped in `PRAGMA query_only = ON` before the closure and
`OFF` after (reset also when the closure errors), preserving the same
fail-loud-on-write contract. No journal is attached either way. Gate the two
implementations with `#[cfg]` inside the one method (or two cfg'd methods with
one doc comment), matching how `Database::call` is already split.

### Elevate the empty-changeset line to `warn!`

`Database::insert_pending_changeset_on` (crates/coven-core/src/database.rs,
currently `debug!("journaled transaction produced no synced changes")`) becomes:

```rust
warn!("journaled sql transaction produced no synced changes; pure reads belong on sql_read");
```

This is deliberately a tripwire: after hosts migrate reads to `sql_read`, any
firing marks either a read still on the write path (move it) or a conditional
write that no-op'd this time (legitimate; stays on `sql()`). Document both
readings in a comment at the warn site.

**Prerequisite audit (part of this change):** find every caller of
`run_pending_journaled_transaction_on` inside coven itself. Coven-internal
machinery that only reads, or only writes coven-local (non-synced) tables —
sync bookkeeping, pending-changeset drain, uploader hints — must NOT run
through the journaled path (it would fire the warn every cycle). Route
internal pure reads through plain `Database::call` (they need no journal) and
confirm internal local-table writes already use `call` directly. If any
internal caller legitimately needs the journaled path and legitimately
produces empty changesets at steady state, say so in the report — that's a
finding to discuss, not something to paper over.

## What NOT to build

- No changes to `CovenReadHandle` (its `sql` already has the target shape).
- No read-connection pool — one reader connection/thread is today's shape.
- No host migrations here (coven-torrent and bae move their reads in
  follow-up changes in their own repos).
- No new public types: `sql_read` takes the plain-connection closure, not a
  `SqlContext` (there is no stamp on a read).

## Tests (crates/coven tests, alongside existing handle tests)

1. **Read-your-write:** open a full handle on a fresh library, `sql()` insert
   a row into a synced test table, then `sql_read` selects it back.
2. **Reads can't write:** `sql_read` running an `INSERT` returns an error
   (native: SQLITE_READONLY). Assert error, and that the row is absent via a
   subsequent read.
3. **Fresh-library open:** `Coven::builder(cfg).open()` succeeds on an empty
   directory (proves the read connection opens after migrations create the
   schema).
4. **Coexistence unchanged:** existing `CovenReadHandle` tests stay green —
   a full handle (now holding two connections) plus a read handle on the same
   library still work together.
5. **wasm:** `scripts/check-wasm.sh` passes (the wasm `sql_read` compiles).
6. Full local suite: `cargo fmt`, `cargo clippy --all-targets -- -D warnings`,
   `cargo test` across the workspace — the pre-commit hook enforces these;
   never `--no-verify`.

## Worktree notes

Work in the assigned worktree (branch `read-sql`), based on current `main`
(04d2a08). Single-concern: one commit (the API + the warn + the internal-caller
audit fixes it requires). Commit message: why not what. Include this plan file
in the commit.
