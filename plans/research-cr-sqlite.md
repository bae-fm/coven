# Research: cr-sqlite (2026-10-02)

Read at commit 43ab94c (vlcn-io/cr-sqlite). "Core" = core/rs/core/src.
Behaviors marked "Probe" were observed by building the extension and running
scripts, not inferred.

Terms:
- Clock row: one row of cr-sqlite's metadata, the version information for one
  cell.
- Causal length (cl): a per-row counter of how many times the row was created
  or deleted. Odd means it exists, even means deleted.
- Tombstone: the record kept for a deleted row so older changes to it are
  ignored.

## What it stores
- Per synced table (bootstrap.rs:195-235):
  - `<t>__crsql_clock(key, col_name, col_version, db_version, site_id, seq)`,
    keyed by (key, col_name), plus an index on db_version;
  - `<t>__crsql_pks(__crsql_key INTEGER PK, pk cols…)` mapping each real
    primary key to a local integer.
- One clock row per non-primary-key cell.
- Row existence is a clock row with `col_name='-1'` whose col_version is the
  causal length (c.rs:10-11); not written on first insert, cl defaults to 1.
- site_id is a small integer into `crsql_site_id`; the device's own id is a
  random 16-byte UUID (bootstrap.rs:9-15,43).
- Overhead (probe): 50k rows × 4 columns with a text UUID key went from
  6.4 MB to 19.1 MB, about 3×, ~64 bytes per cell. Issue #293 open.

## Ordering and tie-break
- col_version is a per-cell edit counter, and it decides conflicts: local
  write = old + 1 (tableinfo.rs:525-529); merge copies the incoming value.
  So the side that edited a cell more times wins, not the later one. Probe:
  5 early edits beat 1 later edit.
- db_version is a per-database transaction counter, used only for "what
  changed since X". On merge it becomes max(local+1, pending, incoming)
  (db_version.rs:55-75), so the stored db_version is the receiver's own
  number. Several source transactions can collapse into one local db_version
  with duplicate seq (issue #380, open).
- seq numbers changes inside one transaction.
- Equal col_version: larger value wins; type order Integer > Float > Text >
  Blob > Null (compare_values.rs:13-16). Equal values are a no-op unless
  `merge-equal-values` is on, then site id decides.
- A site-id tie-break was tried and reverted (PR 393 / #407); so was using
  db_version as col_version to keep a transaction's cells together
  (PR 386 / #405).

## Deletes and re-adds
- Delete: cl to 2 or +1 (tableinfo.rs:427-445); drops the row's cell clock
  rows; keeps the pk row and the existence row as a tombstone.
- Re-insert: cl to the next odd number.
- Merge (changes_vtab_write.rs:512-647): lower incoming cl ignored; even
  incoming cl means delete; a higher odd cl brings the row back with local cell
  versions reset to 0 so every incoming cell wins; at equal cl, cells compete
  as above.
- So "more create/delete toggles wins", in any arrival order. Probes:
  delete/re-add/delete (cl 4) beats a later delete+re-add (cl 3); a delete
  (cl 2) beats 5 edits (cl 1); a delete arriving before the insert leaves a
  tombstone and the insert is ignored.
- A partly arrived re-add shows a mix of new and old values until the rest
  arrive.

## Primary keys and identity
- `crsql_as_crr` refuses: unique indexes besides the pk; no pk or a nullable
  pk column; AUTOINCREMENT; any foreign key; NOT NULL without a default
  (tableinfo.rs:909-1001).
- A unique index created afterwards isn't checked; merge writes `INSERT … ON
  CONFLICT DO UPDATE` with no target, so a clash updates the other row.
  Probe: replicas diverge permanently, no error.
- Bug: changing a primary key value diverges peers that had the old row; cell
  clock rows move to the new key keeping their old db_version
  (after_update.rs:79-98,151-171), so a "since X" sync never sends them.
  Probe: peer ends with (9, NULL, NULL) vs origin (9,'x','y'). Not filed.

## Foreign keys
- Not allowed; any REFERENCES is rejected (tableinfo.rs:971-983). No rule for
  a child before its parent or a concurrent parent delete. Issue #369 open.

## Triggers
- Its own tracking triggers are gated on a connection flag set during merge.
- User triggers do fire during merge, but what they write is not recorded, so
  it never replicates. Probe confirmed.

## Extracting and applying changes
- `crsql_changes` reading: a UNION over clock tables ordered by
  (db_version, seq); the value is read from the live table, so it's the
  current value, not the historical one.
- Applying: one INSERT per cell; no grouping by source transaction. Probes: one
  remote transaction's two cells split, one won and one lost; a failed change
  left earlier changes of the batch committed. Issue #303 open.

## Schema changes
- `crsql_begin_alter` / `crsql_commit_alter` around the user's ALTER;
  clock rows for dropped columns deleted, new columns backfilled, all metadata
  dropped if pk columns change (alter.rs, backfill.rs).
- A change for an unknown table/column fails with a bare "SQL logic error".
  No schema version in changes (issues #186, #378 open).

## History and tombstones
- No log, only latest state per cell. Tombstones never cleaned (issue #319
  open; warns about a server restored from an old backup).

## Status
- Looks dormant; the maintainer moved to rocicorp.

## Takeaways for coven

Copy:
- Causal length per row, with cell versions scoped to it: cells compete only
  within the same causal length; a higher one resets cell versions. Answers
  delete-then-re-add of a shared identity.
- Record a tombstone even for a row never seen, so a late insert loses.
- Map each primary key and each site to a small local integer to keep
  per-cell metadata small.
- Existence-only row for tables with only primary-key columns.
- Gate tracking triggers on an "applying remote changes" flag.
- Reject unsafe schemas when a table is registered.

Different:
- Conflict order: compare (causal length, hybrid clock timestamp, site id),
  not edit counts and values. Every cell of a transaction gets the same
  timestamp, so concurrent transactions win or lose whole across the cells
  they share.
- Primary keys immutable, or a key change is delete plus full re-insert.
- Constraints re-checked on every schema change; apply and verify each remote
  transaction in one SQLite transaction; fail loud.
- Foreign keys need an explicit rule: orphans allowed and hidden, or a parent
  delete records child deletes as ordinary changes.
- Triggers: decide whether writes caused by remote changes are recorded or
  recomputed; never silently untracked.
- Apply each remote transaction as one SQLite transaction; keep the source's
  identity instead of renumbering.
- Tag each transaction with its schema version; hold newer-schema transactions
  until upgrade.
- Compaction and tombstone cleanup after every device has acknowledged, with
  the old-backup case handled.
