# Tombstone Cancel Backoff

Tombstone-cancel rows retry their cloud delete every sync cycle after a failed
inline cancel. That retry is durable, but it is not backoff-gated, so one
persistently failing cancel can make a cloud delete request every cycle.

## Design

Use the existing `cloud_outbox.attempt_count`, `last_error`, and
`last_attempt_at` columns for `cancel` rows. A failed tombstone-cancel attempt
records the error and attempt timestamp, then later drains skip that row until
the existing `backoff_window` has elapsed. Rows with no `last_attempt_at` remain
immediately eligible. Rows with unparseable timestamps log a warning and retry so
bad local retry metadata cannot strand the cancel.

Do not add a third copy of the database retry update. Factor the existing upload
and delete failure updates through one private helper that takes the operation
name, then expose operation-specific public methods for upload, delete, and
cancel callers. The operation-specific wrappers keep call sites explicit while
the SQL update stays single.

## Components

- `crates/coven-core/src/database.rs`
  - Add a private `record_cloud_outbox_failure(id, operation, error, attempted_at)`.
  - Route `record_cloud_upload_failure` and `record_cloud_delete_failure` through
    it.
  - Add `record_cloud_cancel_failure(id, error, attempted_at)` scoped to
    `operation = 'cancel'`.

- `crates/coven-core/src/blob/delete.rs`
  - Add a `clock: &dyn crate::clock::Clock` parameter to
    `drain_tombstone_cancels`.
  - Read `now` once before the loop.
  - Skip a cancel row when `last_attempt_at + backoff_window(attempt_count)` is
    still in the future.
  - On `cancel_tombstone` failure, record a cancel failure and leave the row
    queued.
  - Keep row-removal failure behavior unchanged: the tombstone is already gone,
    so the lingering cancel row can retry the idempotent delete on a later pass.

- `crates/coven-core/src/sync/cycle.rs`
  - Pass the existing cycle clock into `drain_tombstone_cancels`.

## Correctness

- A cloud outage cannot drive per-cycle tombstone-cancel deletes: the first
  failed attempt records retry state and later drains respect that durable
  window.
- The cancel remains durable because the row is removed only after
  `cancel_tombstone` succeeds.
- A successful cancel still clears the row, so no retry state survives success.
- A row-removal failure after a successful cancel remains convergent because
  deleting an absent tombstone is idempotent.

## Tests

- Extend the durable cancel test so the first drain records
  `attempt_count = 1`, `last_error`, and `last_attempt_at`.
- Add an inside-window drain that makes no cloud delete call and leaves the row
  unchanged.
- Add an after-window drain that retries, deletes the tombstone, and clears the
  cancel row.
- Add a corrupt `last_attempt_at` test showing the row retries instead of being
  stranded.
