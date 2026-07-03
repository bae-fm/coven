# Tombstone Delete Backoff

Queued blob delete rows retry their cloud work every sync cycle when the
tombstone existence check or tombstone write fails. Upload rows have the correct
shape: the outbox row records an attempt count and timestamp, and the drain skips
the row until its retry window has elapsed. Delete rows should use the same
durable retry gate.

## Design

Use the existing `cloud_outbox.attempt_count`, `last_error`, and
`last_attempt_at` columns for delete rows. The delete drain records a failed
tombstone attempt with the same timestamped counter shape used by upload rows,
then skips that delete row while it is inside the existing exponential
`backoff_window`: 30s, 60s, 120s, capped at one hour.

Keep upload and delete semantics distinct:

- Upload failure notification still goes through the upload observer.
- Delete failure records only DB retry state and logs; there is no observer event.
- A successful delete drain still removes the outbox row once the tombstone is
  present.
- A row with no `last_attempt_at` remains immediately eligible. This preserves the
  host's explicit retry-now behavior and keeps freshly queued rows immediate.
- A row with an unparseable `last_attempt_at` remains eligible, with a warning, so
  corrupt local metadata cannot strand a deletion.

## Components

- `crates/coven-core/src/blob/upload.rs`
  - Make `backoff_window` visible inside the crate so the delete drain reuses the
    existing retry math instead of duplicating it.

- `crates/coven-core/src/database.rs`
  - Add `record_cloud_delete_failure(id, error, attempted_at)` beside
    `record_cloud_upload_failure`, updating the same retry columns but scoped to
    `operation = 'delete'`.

- `crates/coven-core/src/blob/delete.rs`
  - Before trying a delete row, check `last_attempt_at` against
    `backoff_window(entry.attempt_count)`.
  - On `cloud_home.exists` failure, tombstone serialization failure, or
    `cloud_home.write` failure, record the delete failure and leave the row queued.
  - Do not record a failure when the row-removal DB write fails after the
    tombstone is already present; that path should converge by finding the
    tombstone on the next drain.

## Correctness

- A cloud outage cannot drive per-cycle tombstone writes: after one failed
  attempt the row is skipped until the durable window expires.
- A successful tombstone still clears the durable delete row, so no extra state
  survives success.
- A failure to clear the row after a present tombstone remains immediately
  convergent: the next pass checks existence, sees the tombstone, and removes the
  row without touching the tombstone or extending the grace.
- Retry-now stays possible because clearing `last_attempt_at` makes the row
  eligible without erasing the failure count.

## Tests

- Add a delete-drain test where a cloud existence check fails once:
  1. first drain records `attempt_count = 1`, `last_error`, and `last_attempt_at`;
  2. a second drain inside 30s makes no cloud call and leaves the row unchanged;
  3. a third drain after 31s retries and succeeds.

- Keep existing delete, cancel, and upload tests passing to prove the shared
  outbox row shape did not drift.

## Boundary

This plan covers delete tombstone existence checks, tombstone serialization, and
tombstone writes. It does not change tombstone-cancel retries, tombstone garbage
collection retries, or pull/download retries.
