# `sql_local`: declared device-local writes, verified

## Situation

`CovenHandle::sql` journals every transaction and, since `bf5ced5`, warns
when the captured changeset is empty — a tripwire meaning "a pure read is
still on the write path, or a conditional write no-op'd." A third case turns
out to dominate in practice: **legitimate writes to device-local
(non-synced) tables**. bae persists playback position once per second during
playback (`INSERT OR REPLACE INTO playback_state`, a local table), so the
warn fires every second of normal use — noise that buries the signal the
tripwire exists for.

The gap: a host cannot *declare* that a write is local-only. Fix: a third
entry point that makes the declaration — and verifies it, so it cannot rot
into a hole in change capture.

## Design

### `CovenHandle::sql_local`

Same signature as `sql` (closure over `SqlContext` — a local write may still
want `sql.tx()`; `stamp()` is available but local tables have no
`_updated_at` convention, callers just won't use it. If giving it the plain
`&Connection` closure shape instead is cleaner — matching `sql_read` — do
that; there is no stamp to mint for a local table and no reason to imply
one. Decide once, document why).

Behavior:

- Attach the change-capture session over the synced tables and run the
  closure in a transaction, exactly like `sql` — capture stays structurally
  impossible to bypass.
- **Empty captured changeset → success, silently.** That is the declared,
  expected outcome for a local-only write.
- **Non-empty captured changeset → error, aborting the transaction** (roll
  back; nothing committed, nothing enqueued): the caller declared local-only
  but wrote a synced table. This is a host bug surfaced loudly at the exact
  call site, not a warning — and it is what makes the declaration verified
  rather than trusted.

Implementation lives next to `run_pending_journaled_transaction_on` in
coven-core (a variant that inverts the empty/non-empty handling and never
inserts into `pending_changesets`). The `sql()` warn's site comment drops
its "conditional write no-op'd" reading only if that reading is now fully
served by `sql_local`… it is not — a conditional write to a *synced* table
that no-ops this cycle still legitimately fires the warn from `sql()`.
Update the comment to name all three readings and where each belongs:
read → `sql_read`; local-only write → `sql_local`; synced conditional
no-op → stays on `sql`, warn tolerated.

### wasm

Mirror whatever the repo did for `sql_read` on wasm: if the wasm facade has
an analogous journaled `exec`, give it the same declared-local variant only
if the wasm build actually compiles this handle (check first — `sql_read`
was native-only because the `coven` crate does not build for wasm; the same
almost certainly holds, in which case native-only with no cfg'd dead code).

### Docs

- `site/docs/sync-model.md` "Reads" section: retitle or extend ("Reads and
  local writes") with a short paragraph on `sql_local` and its verified
  declaration; adjust the closing "write path polices itself" paragraph to
  the three-way split.
- `site/docs/index.md` "Who owns what" needs no change; the example page
  only if it mentions local tables' writes (check `local-data.md` — it
  documents undeclared tables; add one line pointing local-table writes at
  `sql_local`).
- README: no change (it shows the synced-write beat only).

## Tests

1. `sql_local` write to a non-synced table succeeds; the row is there via
   `sql_read`; no `pending_changesets` row was created.
2. `sql_local` write that touches a synced table fails with the new error;
   the transaction rolled back (row absent, no pending changeset).
3. `sql` on a synced-table write still journals (existing tests cover; add
   nothing).
4. Warn-site comment: no test — but the empty-changeset warn must NOT fire
   from `sql_local`'s path (assert via the tripwire-style subscriber if a
   test harness for log capture exists in-repo; if none exists, the
   pending_changesets assertions in (1) suffice — do not build a log-capture
   harness for this).
5. Full local suite: fmt, clippy -D warnings, workspace tests,
   scripts/check-wasm.sh. Pre-commit hook enforces; never --no-verify.

## Out of scope

- Host migrations (bae adopts in its own repo afterwards; coven-torrent has
  no local-only writes).
- No change to `sql`/`sql_read`/`write` behavior.

## Worktree notes

Branch `sql-local` in this worktree, based on coven main (e1c1032 — note:
main has moved past the sql_read commit; read the current state of
database.rs/coven.rs rather than assuming, e.g. item_keys was removed).
Single concern, one commit, plan file included. Commit message: why not
what.
