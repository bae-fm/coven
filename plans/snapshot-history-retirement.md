# Retire snapshot history without waiting for offline devices

## Objective and scope

Implement one shared publication order for Store commits and Store snapshots.
A snapshot closes the accepted history it represents. Later publication must
continue from that snapshot, allowing covered commit-to-device-state mappings
and ordinary replay history to be retired without waiting for every active
device. A returning device preserves its local data and rebases unpublished
work whose original shared context has been retired.

This document specifies implementation work; it does not describe an
implemented protocol. Writing and committing this plan does not authorize
production changes, a deployment, a dependency repin, or live-library repair.

Build on [local replay causality](local-replay-causality.md), including its
private-state and blob-preservation fixes. Integrate the completed work from
`/Users/dima/dev/bae/.worktrees/coven-local-replay-causality` before modifying
overlapping production paths. Do not edit or commit that checkout's outstanding
changes as part of this plan.

Coven is greenfield. Replace the superseded internal protocol, schema, tests,
fixtures, and descriptions directly. Maintain the application-facing schema
migration capability; introduce no compatibility reader or internal migration.

## Decisions

- Commit acceptance and snapshot publication share one authoritative order.
- Uploading candidate objects does not accept a commit. Acceptance is the
  successful atomic advance of the current publication record.
- A snapshot covers the entire accepted prefix immediately before it.
- Accepted work does not become unpublished because of snapshot retirement.
- Unpublished edits may require rebase; conflicts retain the work and are
  reported to the host.
- Any currently authorized owner may publish a Store snapshot. There is no
  permanent publisher or separate publisher-election protocol.
- The host supplies a positive soft threshold N for accepted Store commits
  since the latest published snapshot. Publication may overshoot N.
- Snapshot image installation is conditional on the recipient's available
  reconstruction inputs. The shared retirement boundary is authoritative.
- Original observed history, shared publication position, and the state used
  for a successful rebase are distinct facts.
- Snapshot compaction must also compact its publication history. Do not move
  lifetime growth from the device-state index into an acceptance ledger.

## Source-grounded starting points

The following files were read completely while preparing this plan. Their
behavior establishes the boundaries to change, rather than precedent for the
new protocol.

| Source | Relevant existing behavior |
| --- | --- |
| [Commit identifiers](../crates/coven-protocol/src/store_commit/identifiers.rs) | `StoreCommitOrder` carries an author sequence, exact predecessor, and cross-author dependencies. It has no snapshot base or shared publication position. `CommitFrontier` treats lower sequences as covered prefixes, but matches exact references at equal positions. |
| [Commit body](../crates/coven-protocol/src/store_commit/batch_commit.rs) | One signed commit names its WriteId, author, order, declared authority, packages, and control operations. |
| [Snapshot metadata](../crates/coven-protocol/src/store_commit/ack_snapshot.rs) | Snapshot metadata carries image, coverage, membership rollup, state, and history summary; its publication sequence belongs to an author-specific snapshot stream. |
| [Write preparation](../crates/coven-replication/src/sync/store/commit_publication/operation/preparation.rs) | Preparation preserves captured cross-author dependencies, chooses the own-author predecessor, and prepares a commit and author head. Pulling newer history does not rebase the captured write. |
| [Snapshot publication](../crates/coven-replication/src/sync/store/snapshots/publication.rs) | Durable snapshot publication uploads required blobs, image, and membership rollup before publishing metadata and completing the local operation. |
| [Replay owner](../crates/coven-database/src/store/store_session/retained_merge_replay/cache.rs) | Replay uses dependency readiness and canonical ready batches. It walks a local journal alongside accepted commits, associates exact publications, and rejects crossing earlier local work. |
| [Device-state storage](../crates/coven-database/src/store/store_device_state.rs) | State bodies are deduplicated by hash; exact commit mappings remain required by historical state resolution, covered baseline loading, and exclusion evidence. |
| [Storage capability](../crates/coven-storage/src/cloud_object_storage.rs) | Owners can allocate, prepare, create, read, and delete exact objects. The interface has no conditional update of a current publication record. |
| [Provider interface](../crates/coven-storage/src/cloud/mod.rs) | Exact slots support create and delete; raw object writes may overwrite. `CloudObjectVersion` exists, but the traits do not provide the conditional update needed below. |
| [Exact-upload settlement](../crates/coven-storage/src/cloud/exact_upload.rs) | Occupied and ambiguous creates require exact outcome verification; an unverified response cannot establish success. |
| [Provider probes](../crates/coven-storage/src/provider_probe.rs) | The exact-create race uses one provider identity. The cross-principal probe exercises access and deletion, not competing conditional updates of one shared record. |

The local replay plan establishes the existing all-current-writer retirement
requirement and the private-state contract. Read the completed implementation
again before changing that requirement, including owner recovery and membership
changes made in the overlapping checkout.

## Acceptance must survive deletion of old objects

Retirement is permanent: after a snapshot closes a publication prefix, no
position in that prefix can ever become newly accepted. Reclaiming stored bytes
must preserve that rule. A permanent exclusion marker per deleted position
would itself retain lifetime history; the protected current record represents
the retired prefix together with its accepted continuation.

The create-once-slot sketch is insufficient when old slots can be deleted.
After deletion, a delayed writer could successfully create an old logical slot
again. Its create response would look successful even though a snapshot had
already retired that position. Retaining every occupied slot forever would
preserve the retention problem.

For example, A reads that slot 50 is next and pauses before sending its create.
Other devices publish through 100, publish a snapshot, and reclaim slot 50.
A then resumes the prepared create at 50 and receives success because that
location is empty. Reclamation never touched the current slot: the defect is
mistaking a delayed create at a retired position for current acceptance.

Use immutable candidate objects plus one provider-enforced, conditionally
updated current publication record. The provider changes this record only if
its revision still equals the revision the writer read. This is the acceptance
point for both commits and snapshots. Creating a candidate object alone never
advances shared history, including when its location was previously deleted.

The current record's location is bound by Store creation and remains present
through compaction. Readers open that exact location; a listing cannot decide
the current accepted head. Each accepted replacement advances the publication
position, names its exact predecessor, and carries the latest accepted snapshot
reference. Immutable publication entries retain the history after that snapshot.

The provider revision is an opaque storage concurrency token, separate from the
signed publication position and content hashes. Compose existing provider and
protocol types where they represent these facts. Do not implement conditional
update as read-then-overwrite, a local mutex, or an overwrite followed by checking
which bytes happened to remain.

Implement and verify this capability for every supported provider. Provider
adapter support has not been established by this planning review. This is an
implementation prerequisite, not a claim that current adapters provide it.
If a provider cannot enforce it, report the exact capability failure before
changing provider support or substituting an acceptance mechanism. Do not retain
the unsafe slot-only path as a compatibility mode.

### Follow-up: permanently single-use slots

Investigate whether exact slots can remain permanently consumed after their
stored objects are reclaimed. Create-if-absent prevents replacing an occupied
slot; it does not by itself promise rejection after deletion. The current
interface and probes do not establish that stronger lifetime guarantee.

Check each provider's actual contract and adapter behavior, including reuse of
the same opaque object ID, recreation at the same logical key, allocation of a
different provider ID for that key, delayed requests, and independent principals.
Test create, reclaim, and attempted recreation through the real storage owner.
Determine where permanent consumption is recorded and whether it introduces
another lifetime-growing record set.

If permanent single-use slots can enforce retired positions, evaluate replacing
the conditional current-record mechanism with them. Preserve snapshot discovery,
authority verification, compacted outcome settlement, and the shared acceptance
order. Establish the complete contract before revising the mechanism; do not
introduce two acceptance modes. This investigation is a follow-up and does not
block committing this implementation plan.

## Protocol values and validation

Model the following facts under existing protocol owners. Use enums with
associated data for alternatives; avoid parallel optional fields.

1. A publication reference identifies the Store, monotonically increasing
   position, and exact immutable entry.
2. A publication entry names its preceding accepted entry, publishing author,
   and either a commit reference or a snapshot reference. Authentication binds
   the whole entry and its Store.
3. The current publication record identifies that entry and the latest
   accepted snapshot. Its replacement binds the previous record's identity;
   provider conditional update binds the storage revision.
4. A commit's shared base is genesis or an exact accepted snapshot, followed
   by any required exact references in the retained interval. Author sequence
   and WriteId remain distinct from global publication position.
5. A snapshot names the accepted prefix it represents, image, schema,
   membership and device state, and the authority needed to resume from it.
   It also records the cumulative author coverage needed for publication
   continuity and retry settlement.

Avoid hash cycles: prepare the image and snapshot metadata first, then the
immutable publication entry referring to that metadata, then the current-record
replacement referring to the entry. Metadata names the preceding accepted
prefix, not its own eventual publication hash.

Validate all of the following before accepting a candidate locally or preparing
the current-record replacement:

- Exact object hashes, Store binding, signatures, and author identity agree.
- The candidate extends the expected accepted position without a gap.
- Its publishing device and principal have permission at that acceptance
  boundary. Historical authority does not revive a removed member.
- Dependencies are either represented by the named snapshot base or exact
  retained references. A lower numeric coordinate cannot authenticate an
  arbitrary supplied historical hash.
- The candidate continues from the latest accepted snapshot. A candidate
  signed for an earlier base cannot cross a snapshot publication unchanged.
- Required public objects have verified durable remote bytes before the
  acceptance record can name them.
- Snapshot publication is owner-authorized and covers every accepted entry
  through its expected predecessor.

Apply the same boundary to all Store operations affecting accepted history:
row packages, membership, device activation and recovery, exclusions, Circle
controls, acknowledgements, reclamation, and candidate abandonment. Their
prepared objects cannot activate through an independent author-head path.
Keep audience-specific authorization under its existing owners.

Explicit undo, exclusion, and retraction require particular care: snapshot
retirement cannot silently convert an accepted edit into pending work, and a
later control cannot require replaying discarded inputs. Summarize continuing
control state and express subsequent authorized effects from the retained base.
Preserve explicit host resolution semantics and test the resulting state
against executions retaining the original inputs.

## Publication operation

One owner serializes local publication bookkeeping across row and control
operations. It uses storage and database capabilities through their owners.

1. Read and verify the exact current record, retaining its provider revision.
2. Obtain the accepted interval and latest snapshot needed to prepare the
   operation. Settle any previously uncertain attempt before replacing it.
3. Prepare the operation against that accepted boundary. Preserve the original
   captured observation record; perform an actual rebase if its base is retired.
4. Persist the candidate identity, expected current record and revision,
   publication bytes, payload claims, and exact retry inputs locally.
5. Upload and verify every required immutable object. Persist preparation
   ownership before an object becomes remotely discoverable.
6. Conditionally replace the current publication record.
7. On success, atomically install the local accepted association, projection,
   positions, authority, and ownership changes. Report a local completion
   failure explicitly while preserving the durable accepted operation for retry.
8. On revision conflict, read and verify the winner. If it is a commit, update
   the expected boundary and revalidate authorization and dependencies. If it
   is a snapshot, update the base and rebase affected pending work. A snapshot
   contender rebuilds its image to include intervening accepted commits.

An ordinary competing commit does not automatically require rebasing the host
edit. Distinguish re-preparing its publication envelope from reapplying its row
effects. Never replace captured observations with the newly pulled frontier
merely because publication lost a race.

An uncertain conditional-write response is not a conflict or proof of failure.
Read the authoritative record and retained interval, or the accepted snapshot
coverage if compaction intervened. Do not publish a replacement while the
original acceptance remains unresolved. Surface an unavailable verification
result to the initiator rather than returning success.

## Stable write identity through retries and compaction

Keep the logical WriteId across candidate replacements; a new commit hash is
not a new user edit. Do not introduce a lifetime global set of every WriteId as
the duplicate-prevention mechanism.

Use serialized author publication with a durable mapping from the next author
sequence to one logical operation. Competing candidates for that sequence are
attempts at that operation. Unrelated local row or control operations cannot
take the reserved sequence while its outcome is uncertain. Accepted author
sequences are contiguous; the snapshot retains cumulative author coverage.

This permits a local durable operation to determine that its sequence was
consumed even after the corresponding acceptance entry is compacted. Exact
candidate identity and logical acceptance are different outcomes: do not invent
an old accepted commit hash from a numeric watermark. Preserve an exact receipt
when known; represent snapshot-covered acceptance explicitly when that is the
available proof.

Implement this invariant through preparation, cancellation, candidate
replacement, operation publication, restore, and owner recovery. A recovered
writer must settle its old identity's attempts before assigning replacement
publication under a new identity. Reusing an author sequence for an unrelated
operation requires proving the prior attempt cannot still become accepted.

Test an upload whose response is lost, followed by multiple peer publications,
snapshot compaction, origin restart, and retry. It must produce one logical
accepted edit without retaining all publication entries. This test is a gate
for the identity model, not permission to infer exact lower hashes from coverage.

## Replay order and the local journal

Shared acceptance order establishes complete snapshot prefixes. Preserve the
existing dependency-aware row replay semantics within each interval between
snapshots. Do not substitute slot-number row application for the existing
canonical scheduler without demonstrating the local replay contract.

Every replay path must honor snapshot boundaries, including a device retaining
all historical bytes and choosing not to install a downloaded image. Replay
the complete accepted prefix, establish the snapshot baseline, then replay the
next interval. Otherwise a later concurrent commit could be scheduled into
history that another device had already folded.

Continue the journal cursor alongside shared replay. Preserve each write's
observed history, original changeset for explicit reversal, audience packages,
private partition, and exact blob facts. Associated accepted packages and the
author's private effects remain one atomic step. Do not substitute the original
host SQL for the published representation or copy private values over shared
authority after replay.

The boundary proof replaces the all-current-writer crossing requirement for
retirement. It does not remove the checks that the local fold consumes a valid
journal prefix, includes associated private effects, and owns every input still
needed afterward. Keep acknowledgement responsibilities that remain necessary
for access, exclusion, or payload ownership; remove only the superseded
unanimity requirement and its dead consumers.

## Snapshot production and adoption

Construct the shared image from the entire accepted prefix before the attempted
snapshot entry. Exclude unpublished host effects and private data. An owner's
private or unpublished work must neither leak into the image nor require its
discard in order to publish. Use the replay projection owner to obtain the
audience image and keep its local journal intact.

Prepare the image, required Circle state or bootstrap references, membership
rollup, authority summary, and blob graph together. Upload and verify them before
advancing the current publication record. A losing snapshot candidate remains
unaccepted; release its objects only through exact candidate ownership after
settling the publication attempt.

The accepted snapshot becomes directly discoverable from the current record.
Restore and join validate its Store-rooted authority and exact inclusion in
accepted publication. An owner's signature alone does not show that a competing
snapshot won. Snapshot authority must remain verifiable after covered ordinary
commits and publication entries are removed. Summarize required authority rather
than embedding their full history or requiring a genesis-to-tip publication walk.

For a recipient, prepare one of two inputs to the same installation owner:

- A locally reconstructed baseline at the accepted boundary when it owns the
  necessary history and passes the replay checks.
- A verified downloaded shared image when retired history is unavailable.

Both paths preserve the recipient's private baseline, including private effects
already folded out of its journal. Preserve recipient-specific Circle state
under its own authorization; a Store image does not replace all Circle images.
Do not treat restoring pending writes as sufficient preservation of private data.

Build and validate the replacement projection before altering durable live
state. Commit baseline, rows, routing, authority, covered positions, accepted
receipts, journal associations, and payload ownership together. Failures retain
the full before-state. In-memory caches change only with the committed result.

## Rebase of unpublished work

Before selecting edits for replacement, resolve their acceptance against the
snapshot plus accepted history after it. Already accepted edits keep their
logical identity and are not replayed as new publications.

Select the remaining unpublished journal suffix together with dependent local
writes. Reapply it in local order against the prepared shared baseline and
accepted interval. Preserve untouched-column identity, sharing transitions,
private routing, blob provenance, and atomic mixed-audience transaction behavior.

Use the owning capture, partition, and replay capabilities to produce valid
replacement effects and publication inputs. Keep original observations as the
record of capture; record the validated replacement base with the successful
rebase. Re-signing old bytes with different dependency references is insufficient.

The automatic operation reapplies recorded edits; it does not rerun arbitrary
application commands or invent user intent. A missing target, conflicting
private/shared identity, invalid Circle context, unavailable required blob, or
unsatisfied dependent edit produces a typed conflict. Report the affected
WriteIds and relevant row or object identity through existing host error/status
owners. Preserve the complete unresolved work for explicit host resolution.

Install the rebase result atomically with the baseline change and candidate
replacement journal. Remote publication follows from those durable prepared
inputs. Do not mark rebased work published until its conditional acceptance
succeeds. A second intervening snapshot may require another safe attempt.

## Retention and reclamation

Replace historical state lookup with resolution from the named snapshot base
and exact retained interval. Audit every caller before deleting mappings:
commit verification, outgoing preparation, registration/recovery, exclusions,
snapshot verification, retained replay, and Circle control/bootstrap consumers.

Store the baseline device state explicitly and keep distinct bodies deduplicated
by hash. Retain commit-to-state mappings for the interval after the baseline.
Delete covered mappings and prune a state body only after its last remaining
baseline, retained-interval, or explicit live-evidence owner is gone, in the
same transaction. Keep immutable in-memory states shared by hash as well.

Reclamation follows verified ownership, not coverage numbers alone. Snapshot
publication must transfer ownership of every needed shared payload before
covered commit/package owners release it. Recipient-private inputs remain
locally owned independently of remote history deletion.

Do not release an old live blob needed by an accepted tail commit, unresolved
write, private baseline, Circle snapshot, transfer, or explicit control proof.
Do not retain every historical commit to support such consumers: materialize
their exact continuing evidence under the consumer's existing owner.

Old immutable publication entries and covered snapshots are reclaimable after
their required continuation and payload state is carried forward. Never delete
or recreate the authoritative current-record location during compaction.
Provider version-history retention is a separate storage setting and must not
be mistaken for logical protocol compaction.

N bounds neither the whole database nor unresolved private work. Current
membership/device state, active control evidence, local receipts, and live blobs
have their own lifetimes. Report retained interval size and concrete reasons
compaction or local adoption cannot advance, without silently restoring the
requirement to wait for offline devices.

## Host policy

Inject a positive N through the existing Store/sync configuration owner. Count
accepted Store commits across all authors after the accepted snapshot, including
Store control commits; exclude snapshot entries and private-only writes.
Evaluate snapshot eligibility from the shared prefix, not the publisher's local
author sequence. Use the latest accepted snapshot to reset the count.

When an authorized owner syncs and the threshold is reached, it attempts a
snapshot. Other owners may contend through the same publication operation.
Ordinary publication continues if no owner is available; N is a trigger, not a
hard rejection limit. Preserve suppression of acknowledgements that add no new
assertion so acknowledgements cannot manufacture an idle snapshot loop.

## Implementation sequence

1. Integrate and reread the completed local replay work. Inventory every
   publication activation, retirement proof, restore path, and historical-state
   lookup using actual callers. Preserve its regression suite as acceptance
   coverage. Read matching full rules before editing each affected path.
2. Add failing production-owner regressions for the two publication-race
   outcomes, delayed writes to deleted historical locations, and uncertain
   acceptance followed by compaction. Confirm their failures establish the
   missing contract before implementing it.
3. Implement provider conditional read/update and exact outcome settlement.
   Exercise independent clients and principals against the same record,
   opaque locations, permission changes, stale revisions, and lost responses.
   Update provider setup/probes to require this capability.
4. Implement shared publication references, authenticated records, snapshot
   bases, and validation. Update Store creation to bind the current-record
   location. Integrate owner construction without exposing retained internals.
5. Route every Store activation through the durable publication operation.
   Replace author-head acceptance and independent Store snapshot acceptance.
   Implement stable author-operation identity and compacted outcome settlement.
6. Update discovery, pull, restore, and authority verification to start from the
   accepted snapshot and retained interval. Preserve exact validation and
   distinguish prepared, accepted, applied, and locally conflicted work.
7. Implement snapshot-boundary replay and replace the all-writer retirement
   proof. Preserve the journal cursor, private/shared checks, and atomic fold.
8. Implement shared-image adoption and unpublished-suffix rebase, including
   folded private state, Circle ownership, candidate replacement, and host
   conflict reporting. Exercise the same installation owner for both local
   reconstruction and downloaded images.
9. Replace old-reference consumers, prune covered mappings and obsolete
   publication objects, and transfer payload ownership. Add the host's soft N
   policy and observable retention/adoption outcomes.
10. Remove superseded protocol types, persistence, paths, fixtures, comments,
    and documentation. Run the complete validation contract and normal hooks
    before committing the implementation.

These are implementation dependencies for one protocol change, not separately
deployed modes. Do not leave an alternative old acceptance path callable.

## Required validation

Tests must use real publication, storage, database, replay, and capture owners.
Use controlled provider barriers for races and fault injection at durable
boundaries; do not reconstruct the acceptance algorithm inside tests.

| Scenario | Required result |
| --- | --- |
| Commit wins against snapshot | The snapshot attempt loses its expected revision and includes the accepted edit before retrying. |
| Snapshot wins against commit | The old candidate remains unaccepted; replacement uses the accepted snapshot and applies once. |
| Two owners publish snapshots | One wins; the losing candidate cannot establish a competing boundary. |
| Delayed write to a deleted historical object | Object creation cannot accept it; stale current-record update fails. |
| Competing updates from separate principals | One accepted current-record transition, one conflict, and consistent exact readback. |
| Update response lost, then head advances and compacts | Origin determines its logical outcome without duplicate publication or lifetime acceptance history. |
| Restart at each preparation/publication/install boundary | Complete valid before-state or committed after-state; unresolved outcomes stay explicit. |
| Removed member or changed owner during preparation | The stale authority cannot activate work after the accepted authority change. |
| Restore/recovery competes with old prepared publication | One valid author continuation; no duplicate logical edit or borrowed historical authority. |
| B reconnects with an edit already in S or its accepted interval | No replacement publication and no duplicate private effect. |
| B reconnects with unpublished work and dependent private writes | Correct ordered rebase or explicit conflict retaining all inputs. |
| Private rows already folded out of the journal | Rows, routes, and exact blob bytes survive downloaded-image adoption. |
| Mixed Store/Circle/Local edits and sharing transitions | Replay causality, private authority, and audience ownership match the established contract. |
| Partial-column update rebased over an unrelated peer update | Untouched peer columns survive; captured column identity is preserved. |
| Missing target, private/shared collision, or missing blob | Atomic conflict with affected identity; no successful publication or lost input. |
| Caught-up device adopts without image download | Same baseline and resulting state as image adoption, preserving its own private data. |
| Full-history replay versus snapshot-boundary replay | Same shared result and preserved local ordering; no post-snapshot insertion into retired history. |
| Explicit retraction/exclusion across a retired boundary | Authorized effect remains implementable from retained state; retirement itself never withdraws accepted edits. |
| Repeated snapshots with many unchanged device states | Baseline plus retained-interval references; shared state bodies; no covered lifetime map in images or memory. |
| Deleted publication prefix and a newly joining device | Verified current snapshot and interval suffice; no traversal of deleted entries. |
| Reclaim overlaps download, publication, and private replay | Every live owner retains exact required bytes; uncertain candidates are not deleted. |
| N reached across multiple authors; publisher unavailable | Store-wide trigger, allowed overshoot, normal publication continues. |
| Idle sync after snapshot and acknowledgements | No endless acknowledgement or snapshot production. |

Run generated operation/delivery/restart/compaction sequences with reproducible
seeds through the production owners. Compare values, locality, shared state,
exact blob identities and availability, journal status, and retained references;
row counts alone are insufficient.

Run targeted protocol/storage/replay/snapshot tests during implementation, then
`scripts/check.sh` for ownership, formatting, strict lint, documentation links,
shipping feature combinations, and both default/all-feature test suites. Keep
incremental compilation enabled and reuse configured build targets. Exercise
provider integration behavior separately; a mock cannot establish a remote
provider's conditional-write guarantee. Name unavailable credentials or provider
checks explicitly instead of treating them as passing.

Before committing, search for the removed author-head acceptance path, old
all-writer retirement dependency, lifetime covered-state loading, independent
Store snapshot activation, and stale terminology in callers/tests/docs.
Classify surviving acknowledgement and historical-reference uses by their
actual owner and continuing requirement. Commit targeted paths with normal
hooks. Publishing, repinning bae, and repairing live data require their own
explicit task authorization.
