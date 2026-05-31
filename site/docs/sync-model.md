# Sync Model

coven syncs SQLite row changes as encrypted, signed changesets between devices
that share a library key. There is no coordinator; each device pushes its own
changesets and pulls others'.

## Change capture

The SQLite session extension records changes for tables declared by the host
via
[`session::set_synced_tables`](rustdoc:fn:coven::sync::session::set_synced_tables).
coven converts those changes into sync envelopes, stamps them with a hybrid
logical clock, signs them as the local author, and encrypts them before
upload.

## Ordering

Hybrid logical clocks provide sortable timestamps that include causality from
the local device and observed remote state. Synced tables use `_updated_at`
as the row conflict column.

## Conflict resolution

Conflicts resolve at row level: the row with the later `_updated_at` wins.
This keeps conflict semantics visible to the host schema instead of hiding
them in a server-side merge layer.

## The sync cycle

[`SyncManager::start_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::start_sync)
spawns a thread that runs
[`run_single_sync_cycle`](rustdoc:fn:coven::sync::cycle::run_single_sync_cycle)
on a tick and on demand via
[`SyncManager::trigger_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::trigger_sync).
Each cycle:

1. Grabs the outgoing changeset from the active `SyncSession`.
2. Ends the session (incoming applies must not contaminate outgoing).
3. Uploads referenced blobs through the cloud outbox.
4. Pushes the signed, encrypted envelope via `cycle::push_changeset`.
5. Pulls remote envelopes via `pull::pull_changes`, verifies signatures and
   schema, applies accepted changes.
6. Restarts a new session for the next round.

The host doesn't see those steps directly. What it sees is a
[`SyncLoopStatus`](rustdoc:struct:coven::sync::sync_manager::SyncLoopStatus)
emitted after every cycle:

```rust
pub struct SyncLoopStatus {
    pub configured: bool,
    pub syncing: bool,
    pub last_sync_time: Option<String>,
    pub error: Option<String>,
    pub device_count: u32,
    pub data_changed: bool,
    pub row_changes: Option<Vec<RowChange>>,
}
```

`error` is `Some(msg)` when the cycle produced a user-facing concern (a hard
failure, schema-too-old, asset downloads failed, schema skips); `msg` is
written to be shown verbatim. `data_changed` + `row_changes` let the host
map applied changesets to domain-level events.

## Schema versioning

Changesets carry a `schema_version`. coven's local
[`SCHEMA_VERSION`](rustdoc:const:coven::sync::push::SCHEMA_VERSION) is a
compile-time constant; storage holds an optional `min_schema_version` (the
floor every client must meet to read the library). Two distinct cases:

- **Hard floor.** If `SCHEMA_VERSION < min_schema_version`, pull returns
  [`PullError::SchemaVersionTooOld`](rustdoc:variant:coven::sync::pull::PullError::SchemaVersionTooOld).
  Its `Display` is the user-facing message — "Update bae to keep syncing —
  this library was upgraded by a newer device (schema vN; you have vM)."
- **Per-changeset skip.** A single changeset whose `schema_version >
  SCHEMA_VERSION` is skipped (cursor advances past it), counted in
  `PullResult::skipped_schema`, and surfaced through `SyncLoopStatus::error`
  as "N changes from a newer bae version were skipped. Update bae to apply
  them." The signal is transient — it clears next cycle when no new skipped
  changesets arrive; once the user updates, a fresh snapshot reconciles the
  missed rows.

## Backoff

A single exponential helper drives two callers with different caps:

- **Cycle-level.** Consecutive whole-cycle failures back off 30s → 60 → 120
  → 240, cap **300s** (5 min); a successful cycle resets the count. A
  recovered network resumes syncing within minutes, and the user's manual
  trigger always preempts the wait.
- **Per-item.** A failing outbox entry's retry window grows with its
  `attempt_count`, cap **3600s** (1 hour) — a persistently-failing entry
  doesn't block other items each cycle.

## Push and pull

Push writes encrypted changesets and encrypted blobs to storage. Pull reads
remote envelopes, validates signatures against the membership chain (if
any), decrypts, and applies. Storage is only transport; it does not
interpret data.
