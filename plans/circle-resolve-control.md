# Circle control-conflict resolution

## Status

Implemented. Implements the `plans/circles.md` requirement: "Concurrent valid
roster or control successors are retained and surface `ControlConflict` …
The Owner resolves the conflict with a signed transition that names the
complete conflicting set and the chosen successor state."

## What already exists (do not rebuild)

- Conflict detection and retention: `CircleCurrentState::ControlConflict {
  branches }` with canonical ordering and duplicate rejection
  (`sync/store/circle_controls/activation/state.rs`). Conflicted Circles
  already refuse authoring (`authoring_state()` → None) and package access.
- Collapse semantics: `CircleCurrentState::advance` retains every branch the
  incoming control does not causally cover; when the incoming control covers
  all of them the conflict collapses to that control. **A covering successor
  is already the resolution mechanism** — nothing new is needed on the
  receiving side's reduction.
- Resolution authority: `MergeCircleOwnerAuthorityRef::ConflictResolution {
  conflict_hash, resolution_hash }` and
  `derive_circle_resolution_grant` (`sync/circle_control.rs:1323`,
  `sync/circle_roster.rs`), plus roster-level
  `resolve_circle_roster_conflict` — used when the branches disagree about
  roster grants.
- Durable operation journal, preparation, publication, activation recording
  (`sync/store/circle_controls/{journal,preparation,publication}.rs`).

## What this adds

The authoring side (an operation that builds the covering successor) and the
application-facing surface (conflicted Circles must be visible, not silently
omitted).

### 1. Operation intent and request

- `CircleOperationIntent::ResolveControl { chosen: CircleControlCoord }`
  (`journal.rs`), a matching `CircleOperationKind` variant
  (`sync/circle.rs:198`), and `CircleOperationRequest::ResolveControl`
  (`commands.rs`) carrying:
  - `circle_id`,
  - the complete conflicting set: every retained branch's
    `CircleCurrentControl` (coord + verified control),
  - the chosen branch coord.

### 2. Command

`Store::resolve_circle_control(circle_id, chosen: CircleControlCoord)`
(`commands.rs`, next to `rename_circle`):

- load `CircleCurrentState`; require `ControlConflict` — resolving a
  non-conflicted Circle is a typed refusal, not a no-op;
- require `chosen` to be one of the retained branches;
- capture the complete retained branch set into the request — preparation
  verifies the set it was given still equals the currently retained set
  inside the journal transaction, and activation competes at exact slots, so
  a branch discovered between command and activation surfaces as a new
  conflict rather than being silently swallowed;
- journal the durable operation and publish through the existing pipeline.

### 3. Resolution transition

`CircleTransitionDraft::resolve` (`sync/circle_control.rs`, next to
`rename`): a successor of the **chosen** branch that

- sets `previous_control` to the chosen branch;
- adds every losing branch's head to its causal dependencies so the new
  control causally covers the complete conflicting set (this is what makes
  `advance` collapse the conflict everywhere, deterministically);
- inherits the chosen branch's state verbatim: same epoch, key generation,
  access root, roster heads, metadata heads
  (`CircleRosterDraftPolicy::Inherited`, no metadata successor, no access
  regeneration);
- carries owner authority. Default is ordinary `Roster` authority at the
  chosen branch's predecessor roster. When the conflicting branches disagree
  about roster grants such that the chosen roster cannot prove the author
  (the case `resolve_circle_roster_conflict` exists for), use
  `MergeCircleOwnerAuthorityRef::ConflictResolution` with the conflict and
  resolution hashes over the canonical conflicting set — reuse the existing
  derivation, do not invent a second one.

Verification on pull needs no new path: the control verifies exactly like
any successor (predecessor, dependencies, authority, create-once slot), and
`advance` performs the collapse. Add verification only where the existing
one is too weak for the `ConflictResolution` authority arm (the verifier
must recompute the conflict hash from the named set and refuse a resolution
naming a set that does not hash to it).

### 4. Surfacing

`get_circles` (`sync/store/database/circle_operations.rs:47`) currently
omits conflicted Circles entirely. Include them: `CircleInfo` reports the
conflict (the branch coords at minimum). A Circle that forks must be visible
to the application as conflicted — today it simply disappears, which is the
bug this section fixes.

## Correctness cases

- **Resolution races another resolution**: both are successor controls at
  create-once slots; the losing candidate fails activation exactly like any
  losing control candidate; the winning one collapses the conflict. The
  loser's durable operation surfaces the existing candidate-replacement
  refusal.
- **Late-discovered branch after resolution activates**: `advance` retains
  it against the resolution control → new `ControlConflict` between
  resolution and late branch → Owner resolves again. Nothing pretends the
  first resolution covered a set it did not.
- **Non-owner attempts resolution**: authority verification refuses at the
  predecessor roster (or resolution-grant derivation), same as any
  owner-signed transition.
- **Chosen branch is an EpochClose**: the resolution inherits it and the
  Circle resumes `Closing`; close machinery continues unchanged. The choice
  between "the close happened" and "the rename happened" is exactly the
  membership/keys/deletion intent the plan says must never be silently
  chosen.

## Tests

In `sync/store/circle_controls/tests/` (the state-reduction tests already
construct conflicts; reuse those fixtures for end-to-end):

1. Two concurrent valid successors (two Owner devices) → both retained,
   `get_circles` reports the conflicted Circle with both branches; authoring
   and package publication refuse.
2. `resolve_circle_control` with the chosen branch → activation collapses to
   the resolution on every device in either arrival order; authoring
   resumes under the resolution control.
3. Resolution naming a stale set (a third branch retained since the command)
   → preparation/activation surfaces it; after the resolution activates, the
   late branch resurfaces `ControlConflict` against it.
4. Non-owner resolution attempt → typed refusal.
5. Resolving a non-conflicted Circle → typed refusal.
6. Restart mid-operation (kill between journal insert and publication, and
   between publication and activation) → resume completes idempotently
   (existing resume fixtures pattern).
7. Two concurrent epoch closes → direct resolution to either closing branch is
   refused; cancelling the local waiting close reopens that exact branch while
   retaining the other close, then resolving to the reopened branch collapses
   the conflict.

## Out of scope

- Roster-grant conflict resolution inside a single control chain (exists).
- The public `CircleState` enum (application-API work; `get_circles`
  surfacing here is the internal substrate).
- Typed `Blocked` reasons and replace/discard (operation-recovery plan).

## Follow-up 1: merge the metadata/roster head frontiers on resolution

The first resolution merged only the control head frontier (`covered_control_heads`),
inheriting the chosen branch's metadata and roster head frontiers verbatim. That
leaves every author-stream head the chosen branch did not carry uncovered. Across
devices this corrupts nothing but stalls loud: resolve to a branch authored by
device B, then have device A (whose losing branch advanced device A's metadata
stream) author again. Device A re-derives its metadata sequence from the head the
chosen branch left — one behind the head A's losing branch already published — and
re-allocates that create-once head slot, hitting a metadata-head `SlotCollision`.
The conflict "collapsed" but never actually ended for the losing author.

Fix (`CircleTransitionDraft::resolve`, `preparation.rs`, `resolve_circle_control`):
the resolution merges the control, metadata, and roster head frontiers across every
conflicting branch — the union of covered heads, one head per author stream at its
deepest position (`merge_frontier_head`). Every branch's heads become covered, so
no author-stream head is re-allocated. It still inherits the chosen branch's epoch,
key, and roster contents verbatim.

The name is the one piece that is *not* inherited from the chosen branch. Covering
a losing branch's metadata head brings that branch's metadata into the resolved
control's covered history, and metadata carries its own deterministic conflict
resolution — the canonical maximum entry by `(stamp, author, device, hash)`, checked
at activation over the full covered history. So the resolution re-derives its name
as that canonical maximum across the merged frontier (equivalently, the maximum
across the branches' own selections, since each is already canonical over its own
history). The metadata layer resolves its own conflict independent of which control
branch the Owner chose; when the Owner chooses the metadata-canonical branch the
name equals the chosen branch's, and either way the selection is valid at activation
for any chosen branch rather than failing loud when a non-canonical branch is picked.

Divergent *roster* frontiers (branches that added different members) would resolve
to a merged roster the chosen branch's inherited `state_hash` does not name; that
re-resolution is the roster-conflict-resolution machinery listed out of scope, and
until it exists such a resolution fails loud at activation rather than dropping a
losing roster head.

## Resolving a closing (`EpochClose`) branch

A resolution is a *successor* control — it necessarily has a new control
coordinate. An epoch close binds its participant responses to the closing
control's coordinate: `CircleEpochCloseResponse.close_control` is the control coord,
and `verify_for`/finalize require every response's `close_control` to equal the
finalizing control's coord. The response *slots* are keyed by `close_id` (stable
across a resolution), but the response *values* are not.

So resolving to a closing branch would re-anchor the close under the resolution's
new coordinate, and any response already made against the original closing control
(reachable: a device — including the closing owner itself — that saw the resolved
`Closing` state before the racing branch arrived responds at its create-once slot)
can never be re-made and no longer verifies for the finalizing control. The close
would deadlock. Byte-verbatim inheritance keeps `close_id`/slots identical but does
not fix the coordinate-bound responses.

Decision: refuse it with a typed `CircleOperationError::ResolveToClosingBranch`
(replacing the prior untyped `InvalidCurrentState` the `active_epoch()` check
produced), checked at the command before drafting. The Owner resolves to a
non-closing branch to discard the close instead.

When every retained branch is closing, the device that owns a waiting close
operation cancels its exact branch by the operation-derived `close_id`.
Cancellation is valid while the Circle is conflicted: its reopening control
covers that close only, so the other close remains retained rather than being
silently discarded. The Owner then resolves the remaining conflict to the
reopened branch. This preserves create-once response binding and gives
concurrent closes an explicit exit without introducing a second settlement
mechanism.
