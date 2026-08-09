# Retained Merge history performance

## Objective

Remove repeated cumulative-history cloning, validation, JSON encoding, and
hashing without weakening exact Merge verification, offline successor
publication, conflict handling, remote-object reclaim, snapshot verification,
or restore.

This file records decisions only after their current producers and consumers
have been traced. Historical plans and commit messages are leads, not evidence
for current behavior.

## Measured behavior

- The 637-test `coven-replication` suite runs in roughly 128 seconds on the
  profiling machine.
- The 107 Circle integration tests run in 49.59 seconds and consume 312.83
  seconds of user CPU plus 221.36 seconds of system CPU.
- The 60 cycle tests run in 20.38 seconds; the 83 pull tests run in 12.50
  seconds; the blob tests run in 9.67 seconds.
- Suppressing all 358 `F_FULLFSYNC` calls made by the Circle group changed its
  wall time from 49.59 seconds to 47.08 seconds. Physical durability is now a
  minority of that group's remaining time.
- Samples of Circle and cycle tests place active CPU in `serde_json`, object
  hashing, signature verification, hex conversion, and Rust runtime checks.
  The sampled JSON path repeatedly reaches
  `RetainedVerifiedMergeHistorySummary::digest` from
  `MergeHistoryVerifier::verify_refs`.
- Non-workspace dependencies use `opt-level = 3` in the test profile. `coven`,
  `coven-domain`, and `coven-replication` use `opt-level = 1`.
- Before the redesign, the production publication and persistence path
  produced a 29,415-byte retained materialization input for the first commit
  and a 44,662-byte input for the twelfth commit in the same linear stream.
- The first retained row has a distinct shape because it has no signed
  predecessor head, predecessor commit reference, or predecessor device-state
  reference. The regression therefore compares the second and twelfth rows
  and requires them to remain within 1,024 bytes.

## Current-code findings

### A full summary is necessary at a snapshot boundary

`verify_snapshot_authority` derives the canonical summary for a snapshot's
exact verified cut and rejects signed snapshot metadata whose carried summary
differs. The carried summary supplies registration, acknowledgement,
membership-control, announcement, and device-state authority after historical
remote objects have been reclaimed. The checkpoint tests sabotage those fields
and require rejection.

This means the portable snapshot proof cannot be deleted or replaced by an
unverified local cache.

### The same type was also used as per-commit working state

Before this change, `VerifiedMergeHistoryCommit` retained an
`OpenedRetainedMergeHistorySummary` for every verified commit.
`compose_merge_history_successor` merged and cloned the complete predecessor
maps, inserted one commit's evidence, validated the whole result, and returned
it. `RetainedVerifiedMergeHistorySummary::open` then validated that result
again and JSON-encoded it to compare its digest with the accepted head.

`RetainedMergeMaterializationInput` then embedded that cumulative summary in
each retained commit's canonical JSON row. A linear history therefore repeated
every earlier proof in every later row.

The portable snapshot proof and the per-commit derived state have different
lifetimes and should not be assumed to need one representation.

## Invariants the replacement must preserve

- A receiver advances only over a completely verified commit and exact
  predecessor closure.
- Successor publication does not reread materialized historical commit or head
  objects from cloud storage.
- Missing retained materialization input fails loudly; it has no cloud
  fallback.
- Device state at every retained commit is exact and tamper-evident.
- Registration, acknowledgement, membership-control, and announcement proof
  omissions are rejected even when an authorized device signs the containing
  head or snapshot again.
- Snapshot restore receives enough self-contained authority to verify its cut
  after reclaim.
- Concurrent predecessor histories merge deterministically or fail on a
  conflict; no arrival-order choice is accepted.

## Design candidates under evaluation

### Cache the current full-summary digest

This removes repeat encoding of one unchanged value but leaves cumulative map
cloning, whole-summary validation, and the full summary copied into every
materialization row. It does not address the confirmed storage and execution
shape.

### Sign a predecessor commitment plus a per-commit delta

This can authenticate incremental history, but it adds a second history chain
beside the Store commit graph. Before adopting it, the current code must show a
guarantee that the existing signed commit references and verified snapshot
comparison do not already provide.

### Keep full portable summaries only at snapshot boundaries

Under this shape, verified commits and retained materialization rows hold the
per-commit evidence from which the verifier derives history. Snapshot creation
flattens the verified cut into the existing portable summary once. This reuses
the Store commit graph as the history graph and introduces no parallel chain.

This is the selected design. The retained row already carries the exact signed
commit, activation head, registration activations, device operations, Circle
activations, packages, and membership object references. It will additionally
carry the verified acknowledgement proof and membership proof introduced by
that commit. Those values are the complete per-commit evidence needed to fold
a self-contained snapshot proof without cloud reads.

The activation head will no longer carry a cumulative-history hash. The signed
Store commit already names its exact predecessor and dependency closure and
every proof-bearing object introduced by the commit. Verification dispatches
from those signed refs. A second cumulative commitment authenticates no new
input; it requires reconstructing and encoding the entire closure at every
commit.

`VerifiedMergeHistoryCommit` will retain the current commit's evidence and
resolved state, not a flattened predecessor proof. Snapshot creation will walk
the exact verified closure and fold that evidence once. Retained outbound
authorization will reconstruct the same verified commit graph from local
materialization rows and the retained snapshot baseline; missing rows remain a
hard error.

## Decision log

- Retain the self-contained snapshot proof. Deleting it loses reclaim and
  restore functionality.
- Reject digest caching as the complete fix. It leaves the cumulative row and
  map shape intact.
- Do not introduce an incremental commitment until necessity is demonstrated.
  The existing commit graph may already authenticate the same transitions.
- The current source demonstrates that no second commitment is necessary. Use
  the Store commit graph as the only per-commit history authentication.
- Retain full summaries only as snapshot metadata and retained snapshot
  baselines. Ordinary activation heads and materialization rows carry no full
  summary.
- Retain acknowledgement and membership proof values in the commit that
  introduces them. Registration values and activation heads are already
  retained in that commit's materialization input.
- Validate a retained commit's evidence only after the exact commit reference
  and membership proof both exist. The closed prepared-commit boundary owns
  that completeness check; transient successor preparation cannot perform it.
- Recompute every cached retained device-state row from the exact retained
  commit application when opening a checkpoint. Canonical encoding alone does
  not authenticate a cached projection.
- Box the optional acknowledgement and membership proof values inside the
  per-commit evidence. This bounds async and journal stack frames without
  changing the serialized evidence shape.
- Remove the old summary digest and per-commit summary-opening API after all
  callers moved to commit evidence and snapshot-only summaries.
- Keep snapshot acknowledgement selection over the verifier cache: after
  tracing the call chain, an acknowledgement matching an active registration
  must be authored on that registration's verified stream, and the accepted
  cut advances to that stream's discovered head. Such an acknowledgement is
  therefore already in the accepted cut's closure; cached commits cannot add
  another eligible acknowledgement.

## Result

- The retained-row regression passes when comparing sequence 2 with sequence
  12. Per-commit evidence remains fixed-size; retained row size no longer grows
  with the number of earlier proof entries.
- The full 637-test `coven-replication` suite executes in 119.55 seconds,
  compared with roughly 128 seconds before the redesign.
- The 111-test Circle group executes in 45.32 seconds, compared with 49.59
  seconds before the redesign.
- `coven-protocol` passes 182 tests and `coven-database` passes 170 tests. The
  bounded-stack cancellation and device-join replay tests also pass with the
  boxed evidence representation.
