# Circle operation recovery

## Status

Implemented. Closes the durable-operation exit gaps from
`plans/circles.md`: typed block reasons (`Blocked(CircleOperationBlock)`), "retry
an operation idempotently", and "replace or discard only with verified
nonactivation". The block reason is the typed `CircleOperationBlock`; a blocked
operation retries from its captured phase; and an operation whose successor slot
was claimed by a different verified winner, whose author was excluded, or whose
exact Store membership grant was revoked by an accepted Store commit is
discarded — its candidate-exclusive objects exact-deleted with absence verified
and its journal row cleared, resumable from a durable `Discarding` state.

Discard reuses the Merge candidate-abandonment object-graph machine verbatim
(`begin_blocked_merge_candidate_nonactivation_on`,
`merge_candidate_cleanup_targets_on`, the per-object cleanup transitions), with
the durable home parameterized: a Merge candidate lives in
`store_writes.prepared`, a Circle operation in the `circle_operations` journal.
Unlike Merge, discard never publishes an abandonment commit to race for the
slot — it is invoked only after a permanent result is directly provable, so it
observes the winner, author exclusion, or accepted grant revocation and records
the nonactivation, which is the same proof the Merge `Lost` outcome records.

## Design

### 1. Typed block reason

Replace `reason: String` in `CircleOperationProgress::Blocked`,
`CircleOperationState::Blocked`, and `CircleOperationError::Blocked` with:

```rust
pub enum CircleOperationBlock {
    /// The author's exact grant no longer has current Store write authority.
    AuthorityLost { grant_id: MembershipGrantId },
    /// A different verified commit took the operation's immutable device-stream
    /// position between composition and publication.
    PositionLost { winner_commit: ObjectHash },
}
```

Each variant corresponds to a production block site. The journal row serializes
the typed value directly, and `get_circle_operations` surfaces it typed.

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
prepared activation can never win. Three proof forms are implemented
(`abandonment.rs::discard_candidate_nonactivation`):

- the prepared device-stream successor slot holds a **different verified
  winner** (exact-read + verify via `observe_excluded_candidate_head` /
  `VerifiedMergeWinner::verified_nonactivation`) — a standalone proof: the
  candidate is bound to that create-once slot and can never take it, whatever
  the author's status; or
- the author was permanently **excluded** (registration revoked), covered by
  `excluded_candidate_nonactivation`; or
- an accepted Store commit names a resolved membership state that tombstones
  the candidate's exact Store membership grant, and the commit's predecessor
  cut does not cover the candidate. The durable `AuthorityLost` block supplies
  that exact grant. The discard driver considers only locally retained,
  verified Store materializations and then re-verifies the witness's exact
  accepted head, replayed history, membership state, grant record, candidate
  authority, and predecessor cut before recording nonactivation.

Unlike Merge abandonment, discard never publishes an abandonment commit to
race for the slot: it runs after nonactivation is already provable, so it
observes the outcome directly. The recorded nonactivation is the exact same
proof the Merge `Lost` outcome records once its abandonment commit loses —
discard skips the doomed publication. With proof, discard reuses the
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
set.** Finalization prepares the complete payload in memory, records its exact
successor key, slots, objects, and settlement kind in the journal transaction,
then uploads each step idempotently. Restart resumes from that recorded payload
without regenerating keys or slots. Fault tests stop between the payload record
and first upload and between uploads, then verify byte-identical resumption.

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
4. Revoke the operation author's exact Store membership grant through an
   accepted Store commit whose predecessor cut excludes the candidate →
   discard succeeds and clears the journal.
5. Kill during finalization at every boundary (before payload record,
   between record and first upload, between uploads, after uploads before
   activation) → resume produces byte-identical objects; storage holds no
   object outside the journal's manifest at any point.
6. Retry idempotence: double retry, retry-of-active-operation refusal.
7. Lose the prepared stream position to a verified competing commit →
   operation becomes typed `PositionLost`, the queue advances past it, retry
   re-observes the winner and re-blocks, and discard verifies the winner before
   clearing the operation.
