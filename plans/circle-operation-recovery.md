# Circle operation recovery

## Status

Implemented, except membership-revocation as a discard proof form (§3) and the
finalization-preparation audit (§4). Closes the durable-operation exit gaps from
`plans/circles.md`: typed block reasons (`Blocked(CircleOperationBlock)`), "retry
an operation idempotently", and "replace or discard only with verified
nonactivation". The block reason is the typed `CircleOperationBlock`; a blocked
operation retries from its captured phase; and an operation whose successor slot
was claimed by a different verified winner (or whose author was excluded) is
discarded — its candidate-exclusive objects exact-deleted with absence verified
and its journal row cleared, resumable from a durable `Discarding` state.

Discard reuses the Merge candidate-abandonment object-graph machine verbatim
(`begin_blocked_merge_candidate_nonactivation_on`,
`merge_candidate_cleanup_targets_on`, the per-object cleanup transitions), with
the durable home parameterized: a Merge candidate lives in
`store_writes.prepared`, a Circle operation in the `circle_operations` journal.
Unlike Merge, discard never publishes an abandonment commit to race for the
slot — it is invoked after the slot is resolved, so it observes the winner (or
author exclusion) directly and records the nonactivation, which is the same proof
the Merge `Lost` outcome records.

## Design

### 1. Typed block reason

Replace `reason: String` in `CircleOperationProgress::Blocked`,
`CircleOperationState::Blocked`, and `CircleOperationError::Blocked` with:

```rust
pub enum CircleOperationBlock {
    /// The author's exact grant no longer has current Store write
    /// authority (today's only production block site,
    /// `publication.rs:80-84`).
    AuthorityLost { grant_id: MembershipGrantId },
}
```

One variant, because there is one producer — do not invent speculative
variants; each future block site adds its own when it exists. The journal
row serializes the typed value (greenfield wire/DB change; no compatibility
reader). `get_circle_operations` surfaces it typed.

### 2. Retry

`Store::retry_circle_operation(operation_id)`: a `Blocked` operation
returns to the phase captured at block time (`phase` +
`operation` payload are already retained), revalidates against refreshed
signed state, and re-enters the normal publish/finalize pipeline. Retry is
**initiator-driven** — the cycle never auto-unblocks (a blocked operation
is a fact reported to its initiator, who retries or discards; automatic
resurrection of a stale operation is the self-heal shape we refuse).
Idempotent: retrying a not-blocked operation is a typed refusal; retrying
twice converges because publication is already per-step idempotent.

### 3. Discard with verified permanent nonactivation

`Store::discard_circle_operation(operation_id)`: legal only with proof the
prepared activation can never win. Two proof forms are implemented
(`abandonment.rs::discard_candidate_nonactivation`):

- the prepared device-stream successor slot holds a **different verified
  winner** (exact-read + verify via `observe_excluded_candidate_head` /
  `VerifiedMergeWinner::verified_nonactivation`) — a standalone proof: the
  candidate is bound to that create-once slot and can never take it, whatever
  the author's status; or
- the author was permanently **excluded** (registration revoked), covered by
  `excluded_candidate_nonactivation`.

Membership-revocation as a discard proof form is not yet wired (it is produced
only for terminal Merge cleanup today, not by the discard proof driver).

Unlike Merge abandonment, discard never publishes an abandonment commit to
race for the slot: it runs after the slot is already resolved, so it observes
the outcome directly. The recorded nonactivation is the exact same proof the
Merge `Lost` outcome records once its abandonment commit loses — discard just
skips the doomed publication. With proof, discard reuses the
candidate-abandonment object-graph machine
(`begin_blocked_merge_candidate_nonactivation_on`,
`merge_candidate_cleanup_targets_on`, and the per-object cleanup transitions),
parameterized over the durable home: it builds the same `PreparedMergeCandidate`
from the journal's `PreparedCircleOperation`, records the nonactivation across
the candidate-exclusive graph (bootstrap blobs included, so their shared
ownership is released), exact-deletes only candidate-exclusive objects after
proving no surviving owner, and clears the journal row in the completing
transaction. The transition to a durable `CircleOperationProgress::Discarding`
state makes the cleanup restart-safe: a crash after the recorded proof resumes
the same idempotent cleanup on the next `resume_circle_operations`. Without
proof: typed `DiscardRequiresNonactivation` refusal — it never assumes that an
unseen candidate failed to activate.

### 4. Finalization preparation owns its uploads

Rule: **no remote write before a durable journal row owns the exact object
set.** Audit `finalize_ready_circle_epoch_closes` / preparation ordering
(`close_responses.rs`, `preparation.rs:1394+`): any path that generates
random material (the successor Circle key) or uploads prerequisites before
`begin_circle_operation_finalization` records the `Finalizing` payload must
be reordered: prepare the complete payload in memory → record it in the
journal transaction → then upload each step idempotently. Restart then
resumes from the recorded payload and never regenerates keys or slots, so
no orphaned uploads exist. If the audit finds preparation is already
durable-first, this item reduces to a test proving it (kill between
payload-record and first upload; kill between two uploads; assert exact
resume with identical object keys).

## Correctness cases

- **Blocked → authority restored → retry**: revalidation passes and the
  original prepared commit publishes; nothing was regenerated, so slots
  and object keys are identical to the pre-block attempt.
- **Blocked → discard without proof**: refused typed; the journal remains —
  never assume nonactivation.
- **Slot lost to another writer → discard**: exact-read proves the
  different winner; abandonment cleans candidate-exclusive objects only;
  shared/activated objects survive (the existing abandonment
  never-candidate-exclusive list applies verbatim).
- **Crash during discard**: abandonment is already restart-safe for Merge
  candidates; the Circle-operation extension inherits it.

## Tests

1. Publication block (revoke the author's grant mid-operation, existing
   fixture) → journal shows typed `AuthorityLost`; retry after re-grant
   publishes the identical prepared commit (assert object keys unchanged).
2. Discard without nonactivation proof → typed refusal.
3. Claim the operation's successor slot with a competing verified winner →
   discard succeeds; candidate-exclusive objects exact-deleted with
   absence verified; the winner's shared objects untouched; journal
   cleared.
4. Kill during finalization at every boundary (before payload record,
   between record and first upload, between uploads, after uploads before
   activation) → resume produces byte-identical objects; storage holds no
   object outside the journal's manifest at any point.
5. Retry idempotence: double retry, retry-of-active-operation refusal.

## Out of scope

- New block producers (each lifecycle feature adds its own variant with
  its code).
- The application-facing operation-inspection API shape (API plan; the
  typed state here is its substrate).
