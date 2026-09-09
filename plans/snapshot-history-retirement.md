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

### Device exclusion uses the accepted publication boundary

Preserve the host's proposal, cancellation, and explicit finalization commands.
A pending proposal does not revoke the target: it may publish accepted edits
and, when authorized, cancel the proposal. Finalization removes permission for
subsequent publication while preserving every edit accepted before it. Explicit
host discard, row withdrawal, and Circle cutoff behavior retain their separate
contracts.

Remove the remaining-device acknowledgement certificate and device-local read
freeze from Store exclusion. They select independent observed author cuts even
though the shared publication already decides which target commits precede
finalization. In particular, a local freeze must not hold an already accepted
target commit. Remove their protocol fields, persistence, validation, and
self-retaining exclusion-locator consumers after checking each continuing
owner; do not change Circle exclusion certificates or bootstrap cuts.

The durable accepted authority result must bind the effective membership
frontier immediately before the winning exclusion publication. Derive it from
the exact accepted interval retained by the publication owner, including any
intervening accepted target heads. The candidate's captured membership state is
insufficient: an envelope retry can preserve that candidate while accepting it
after another head. Reading a later current record also selects the wrong
boundary. Compose the existing membership frontier and post-acceptance result
under their owners rather than introducing a second ordering ledger.

Portable verification uses that exact hash-linked frontier to reject a removed
device's later authority append, even if the device signs a result claiming an
older publication position. Verify the exclusion issuer independently under
the rooted accepted authority, including other device exclusions. Preserve
legitimate authority accepted before finalization. Retain the continuing exact
proof through snapshot compaction without retaining every ordinary commit.

Before implementation, independently review the winning-predecessor binding.
Gate it with the reproduced backdated-authority attack and a real concurrent
target head accepted after outcome preparation but before finalization. Also
exercise pending target writes and cancellation, no remaining-device ACK,
lost response and restart, and cold verification after compaction. Compare the
accepted row effects before and after exclusion; exclusion cannot turn them
back into unpublished work or retract them.

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

### Store snapshot candidate identity and retry ownership

Remove the independent per-author Store snapshot generation, predecessor, and
reserved-successor chain. The shared publication order is the accepted snapshot
order. Replace its enumeration, author continuation, cadence, and per-author
reclamation consumers with verified current publication, retained history, and
exact object ownership. Circle snapshot and acknowledgement chains retain their
separate contracts.

Allocate each Store snapshot candidate's metadata `ObjectSlot` before preparing
its image and blob graph. Use that full slot as the Store snapshot ownership
identity, composing it into `SnapshotObjectOwner::Store { metadata_slot }`;
Circle ownership continues to carry its activation and generation. A fresh
candidate gets a distinct metadata slot. Metadata hash and exact reference are
computed after its image is prepared; the accepted publication authenticates
that complete reference. The reserved slot identifies an owner, never proof of
acceptance. Do not repurpose commit `CandidateFamilyId` for snapshots.

During shared-image construction, replace inherited snapshot ownership in the
image copy with the current candidate's owner. Preserve legitimate retained
replay ownership. The live database must continue merging all still-live owners;
changing an exported image is not permission to release another live owner's
blobs or membership rollup. Reused blob identities remain unchanged.

Keep reconstruction under `AuthorizedSnapshots` and acceptance verification and
settlement under `AuthorizedStoreHistory`. A competing accepted transition
requires recapturing the shared image, coverage, authority, and payload graph
before atomically replacing the durable candidate and publication attempt.
Retain exact cleanup obligations for replaced objects until their deletion
completes. A held accepted input prevents constructing an incomplete prefix.

After a lost response, exact accepted evidence finishes the original candidate
without moving the observed publication tip backward. A verified newer snapshot
may instead supersede the checkpoint request if it covers the requested boundary
and carries the required payloads. Represent that outcome explicitly; it does
not assert that the original candidate either won or lost. Cleanup still
requires proof that each object has no remaining live owner. Unverifiable
settlement remains an error, and this retry model does not replace the separate
requirement for Store-rooted snapshot authority after history retirement.

The durable active snapshot operation carries its exact retired candidate
objects while replacement preparation and deletion remain unfinished. A
`SnapshotSuperseded` operation instead retains the original immutable attempt,
the verified newer checkpoint that fulfills the request, and its exact cleanup
obligations. Publication dispatches on that state before attempting upload.
Remote deletion completes before the database atomically releases the original
journal, payload claims, and reservation; interruption or restart resumes the
same exact deletion obligations. Accepted artifact records remain owned by the
accepted snapshot retirement lifecycle rather than candidate cleanup.

The production-owner regressions in
`crates/coven-replication/src/sync/store/snapshots/publication_race_tests.rs`
exercise both contender outcomes, cleanup interruption and reopen, replacement
artifact protection, and lost responses followed by either an accepted peer
commit or a newer compacting snapshot. They also reject a stale captured cut
and verify that snapshot capture preserves unpublished local effects while
exporting the accepted shared state. These eight cases passed together; the
separate distributed artifact reuse/deletion regression established that two
snapshot candidates cannot safely reuse an exact membership rollup slot.

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

Portable authority must distinguish prepared authority objects from accepted
changes. A snapshot cannot authorize its own signer by asserting an unpublished
Owner promotion in its summary. Retain a discoverable pending authority boundary
before conditional publication, and an exact acceptance result produced only
after acceptance under the existing operation journal. An unresolved boundary
must fail bootstrap and prevent compaction; it cannot silently restore the
preceding authority. Verify the result's issuer from independently rooted prior
authority and bind the exact winning publication, including envelope replacement.
Existing permanent membership streams should own their continuing evidence.

Apply that requirement to every change in snapshot-signing permission, including
device exclusion and recovery where the principal remains an Owner. Membership
proof alone cannot establish that a signing device is still active. Verify the
membership and device authority floor independently of the candidate image,
including accepted revocations whose completion was interrupted. Neither a
candidate's own summary nor a local-only finalization guard proves that omitted
shared authority changes do not exist. Exercise cold and reused verifiers,
interruption after acceptance, restarted completion, and withheld results before
treating this as a replacement for historical authority verification.

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

### Installation preserves current device-local relationships

Install authoritative projected rows together with the foreign-key effects on
current device-local descendants. Match those descendants to the original
parent identities before changing rows. A coupled unique-key swap must not
transfer a child to a different parent, and a projected child that changes its
parent must retain its own local descendants. Synthetic replacement deletes
are not logical deletions and cannot generate additional cascade effects.

Discover local tables only when a changed parent has an affected relationship.
Evaluate parent matching, column affinity, collation, generated values and
defaults through SQLite. Evaluate defaults in the receiving connection's
context at the logical relationship transition, independently of the source
connection's original SQL. Isolate affinity and generated-value evaluation so
its temporary writes cannot change `last_insert_rowid()`, `changes()`, or
`total_changes()` seen by subsequent defaults. Propagate the resulting actions
through local relationships and validate the complete result against the original
schema in the installation transaction. Conflicting assignments or unsatisfied constraints
fail atomically; do not choose an ordering that silently loses a local row.

Preserve physical row IDs as well as visible values. When a required local table
shadows every SQLite rowid alias, expose its existing identity with a native
column rename scoped through installation, then apply the inverse rename.
SQLite restores dependent indexes, views, triggers and foreign-key references;
its canonical identifier quoting does not change their semantics. Preserve both
existing schema fingerprint checks without exceptions. A failure restores the
exposed names before returning, or rolls the enclosing transaction back if
restoration itself fails. No temporary identifier may survive a committed result.

Exercise recursive cascades, coupled local unique-key swaps, reparented projected
children, composite keys, collations, native defaults and generated columns,
hidden row IDs, quoted dependent schema objects, and constraint-failure rollback
through the actual projection installation owner. Also retain end-to-end
publication and snapshot-rebase coverage: preserving rows in the final installer
does not establish that earlier changeset replay preserved them.

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

### Implementation contract: reserved work awaiting preparation

Keep acceptance settlement in `AuthorizedStoreHistory`. Resolve an old attempt
against authenticated snapshot coverage and the accepted continuation before
replacing it. Exact accepted evidence completes that candidate; cumulative
author coverage may establish logical completion without inventing its exact
hash. A changed snapshot base proves that old bytes cannot be accepted in the
future, but does not establish that they were never accepted in the past.

Separate the existing active operation's reservation from its publication
attempt. A commit reservation retains the same owner, WriteId, author
registration, and author sequence through `AwaitingPreparation`, the prepared
`Commit` attempt, and `Discarding`. `AwaitingPreparation` owns durable replacement
inputs without claiming that signed publication bytes exist. Do not release the
reservation, create a replacement WriteId, or reserve speculative sequences for
the remaining journal suffix. Other local operations remain excluded from the
reserved author position.

Use `RetainedReplayCache` and `ReplayProjection` to build the validated shared
baseline and replay the remaining local journal in its existing causal and
ordinal order. Preserve the old private baseline, including private effects
already folded out of the journal. The same installation owner accepts either
locally reconstructed history or an authenticated downloaded shared image.
Original captured observations and changeset bytes remain immutable. Persist
the successfully validated replacement base, replacement effects, audience
partitions, and exact blob facts as distinct current preparation inputs.

Factor session capture, routing, partitioning, and blob-fact calculation from
the existing host capture owner. Reapply recorded sparse edits through that
owner on the projection; never rerun arbitrary host commands or treat expanded
audience packages as the original edit. Ordinary remote LWW/NOTFOUND omission
is not successful rebase: missing targets, conflicting touched columns,
private/shared identity collisions, invalid Circle context, unavailable blob
inputs, and unsatisfied dependent edits produce typed conflicts containing
the affected WriteIds and row or object identities. Untouched peer columns
survive. A conflict preserves the entire unresolved before-state.

One verified database transaction installs the baseline, resulting local
projection, regenerated suffix inputs and ownership, and the active
`AwaitingPreparation` reservation. Compare the expected baseline, observed
publication, journal manifest, and active owner before installation. Transfer
the superseded exact commit/package/envelope obligations to durable cleanup;
retain every input and provenance lease needed to prepare the replacement.
Cleanup must not delete an object still required by those inputs. Advancing
authentic observation separately does not authorize installing a partially
realized materialized baseline.

`AuthorizedWriterOperation` resumes ordinary package encryption, signing and
candidate preparation from those durable inputs. Its final preparation
transaction changes the exact reserved owner from `AwaitingPreparation` to
`Commit`; it does not claim a free author slot. Validate the effective base,
journal identity, current authority and reserved predecessor again. A newer
snapshot requires another recorded-edit rebase; a same-base revision race only
requires another envelope. Reuse the already-held authoring turn in drain,
and reload the candidate after replacement so upload/completion cannot use
the old borrowed batch. Preparation errors retain the reservation and report
through existing write status/error owners. Completion requires its exact
prepared `Commit` attempt.

Explicit discard of blocked awaiting work first commits `Discarding` with the
same reservation and verified nonacceptance proof. This state forbids retrying
publication or rebasing the journal while owned remote objects and unused
spools are being retired. A deletion failure preserves this resumable state.
After exact cleanup, reverse the effective suffix, resolve its WriteIds, release
its payload leases, and clear the reservation in one database transaction.
Exercise interruption during file removal after remote absence verification,
then reopen and resume discard; no partially discarded operation may publish.

Select spool claims from the effective replacement blob facts when present,
otherwise from the original capture, matching preparation and subsequent rebase.
Original observations remain immutable but do not retain an obsolete spool
once the effective input uses a verified remote source. Existing prepared
writes, outbox transfers, and snapshot candidates still retain their own files.
Keep cleanup obligations under their current owners; do not add a second source
lifetime ledger. Exercise a sharing transition whose blob upload succeeds while
its package fails, followed by snapshot rebase and source transfer.

Exercise the production path through partial-column peer edits, deleted
targets, mixed private/shared/Circle suffixes, sharing transitions and blob
relocation. Add restart immediately after `AwaitingPreparation`, preparation
failure/retry, competing local operation exclusion, a second accepted snapshot,
old-candidate cleanup interruption, and accepted-before-snapshot settlement.
Assert exact WriteId/author sequence continuity, retained inputs and leases,
row values, unchanged captured observations, and exactly one accepted edit.
Exercise explicit discard against the replacement effects as well as retry.

## Retention and reclamation

### Snapshot authority before image decryption

Carry the canonical resolved device state in the signed snapshot state itself.
Keep exact state references where a commit or summary binds a frontier, but do
not duplicate the same resolved body beside snapshot metadata in retained
authority. A joining device must establish membership, device activation,
exclusion and recovery authority before receiving the Store decryption key.
Opening the encrypted database image cannot be a prerequisite for that check.

Authenticate accepted authority through the rooted membership controls and
post-acceptance results first. Check that the signed final state preserves every
accepted registration and exclusion effect, including effects later cancelled
or superseded under the existing reducer. Effect containment does not establish
its issuer or replace the ordinary publication preconditions at the actual
accepted predecessor. Require both the pending-registration rejection and the
accepted-registration omission regression.

Resolve later cuts from exact retained commit states, or from the complete
checkpoint and a verified exact suffix. Never assign the checkpoint's aggregate
state to an individual historical commit or authenticate an arbitrary earlier
cut through sequence comparisons. Image installation still authenticates the
plaintext image hash and verifies its represented state against this metadata.

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

### Exact accepted artifact ownership

Derive each Store snapshot's image and membership rollup paths from the full
reserved metadata `ObjectSlot`, including an opaque provider identifier when
present. Distinct candidates cannot share those exact artifacts. Content hashes
still bind their bytes, and accepted row blobs keep their existing exact remote
identities. A local publication permit cannot prevent another device from
retiring an old artifact while a pending candidate reuses it; candidate-specific
artifact ownership removes that distributed race. Validate this relationship
when parsing metadata, not only when preparing an upload.

The accepted successor carries the exact obsolete snapshot metadata, image,
rollup and publication entry, together with exact ordinary publication entries
covered by its cut. This is unfinished deletion ownership, not a lifetime
acceptance ledger. Copy still-pending obligations from the previous accepted
snapshot and add only exact accepted interval references. A subsequent snapshot
may omit an obligation only after the provider confirms that its exact object
is absent; a conflicting occupant is an error. Verification permits obligations
that became absent after candidate capture, while rejecting invented targets or
omissions of objects that remain present. Validate every target's domain and
its position before the accepted successor.

Release obsolete local image and rollup leases, and locally published metadata
rows, atomically when the accepted successor is completed or adopted. Preserve
current, pending, replay and blob owners. The successor's signed inventory then
owns physical deletion independently of those local rows. Delete exact objects
through the existing storage primitive and fail visibly on error; a reopened
operation resumes the remaining inventory. Do not create per-artifact reclaim
authorization or receipt commits: that would manufacture another publication
history while deleting the old one. Circle image reclamation retains its
existing acknowledgement and authority contract.

Exercise interruption during the exact deletions, reopen, a subsequent snapshot
that drops confirmed-absent obligations, and a cold reader after the obsolete
objects are removed. Verify that physical cleanup does not advance publication
and does not read deleted metadata as a prerequisite. Continuing operation
proofs, including promotion requests, must be retained by their own authority
owners before ordinary source history is physically removed.

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

## Implementation and verification status — 2026-09-09

This records verification before landing `fix/snapshot-history-retirement`.
At that point Coven main is
`efcfc95598074f26f15bb5d243b37f8c9febf09e`; Bae still pins that revision. Keep this
worktree after landing, as subsequently requested by the user. Both main
branches must be pushed immediately when advanced.

### Verified receipts

The latest complete all-feature runtime inventory is `check104`: 2,304 passed,
zero failed and 11 ignored across all 18 test targets, plus 8 passing doctests.
It runs through the actual `scripts/check.sh`, after ownership, formatting,
strict all-target/all-feature Clippy, strict rustdoc, documentation links and
all nine shipping-feature checks pass. Replication contributes 947 passes.
The same script completes the default configuration with 2,192 passing runtime
tests, zero failures, 11 ignored and 8 passing doctests. The complete script exits
successfully. The site's actual `npm run build` also passes, including rustdoc,
VitePress bundles and page rendering. Commit and both main pushes remain
outstanding. The eight S3 test variables are unset and the local
test endpoint is unavailable; no live-provider success is claimed.

The earlier complete all-feature runtime inventory was `full83`: 2,288 passed,
zero failed, 11 ignored across all 18 compiled test targets. Its eight libraries
contribute 2,185 passes, integration tests 14, and the owner checker 89. It ran
build 83 binaries without an external stack override and four test threads per target,
at most two targets concurrently. No tests were excluded; nested subprocess
summaries are counted only in their outer target result. This verifies the
snapshot owner, installed Join clock, finalization, producer fixture and strict
lint corrections. The subsequent generated test and obsolete-head cleanup are
not included. Doctests and default-feature tests remain required.

The prior all-feature library inventory was `full77`: 2,185 passed,
zero failed, 11 ignored across all eight crates. It ran build 77b binaries with
no external stack override and no excluded tests. Counts:
coven 200, database 299, domain 90, foundation 65, keys 118, protocol 202,
replication 931, storage 280. The original 1.5 MiB Circle cancellation case and
the domain four-transfer retry/restart case both passed. These are library
results; the remaining full-check contract below is still required. The joining
transport fixtures retain their pre-existing internal 64 MiB thread helper;
they do not establish ordinary-stack coverage. The dedicated Circle and domain
retry/restart regressions above establish their own stack limits separately.

The previous `full74` inventory had 2,175 passed, 6 failed and 11 ignored. Its
failures were eager artwork after open, the losing-blob locator index assertion,
two membership journal/reservation assertions, conflicting-email admission retry,
and a malformed Circle deletion fixture. All pass in full77.

Build 75 compiled all eight library test binaries. Focused verification passes
all eight admission continuation/refusal cases, both original stack regressions,
eleven excluded-authority cases, and two rotation-staging cases. Admission now
reuses a durably completed provider step for the same candidate; replacement
resets that step atomically with its new plan. Rejected completion refuses
unfinished physical cleanup, a matching accepted grant, and a stale accepted
boundary while preserving the durable request and its reservation. The
conflicting-email regression was red on build 74 and passes on build 75.

Build 76b focused verification passes database remote ownership (7), membership
publication/history (7), and Circle deletion (1). It verifies both accepted blob
locator identities independently of the sole winning row binding; reconstructs
immutable membership/publication objects instead of mistaking an ExactObjectRef
field for carried bytes; and exercises host-write/removal reservation protection
in both directions through exact-once acceptance. The Circle fixture now uses
typed activated blob records instead of an invalid `{}` remote record.
Membership publication/history tests are split into a sibling file; their shared
reader stays in the parent for the head-acceptance tests. Restricted-path
visibility passes.

The eager-artwork diagnostic passed in isolation in 30.63 seconds after full74's
timeout. Its helper accepts a previous Synchronized status immediately, without
proving the newly created child write published before the receiver pulls.
Build 77 replaces that wait with the existing write-specific publication status
helper, preserving the actual post-open fill test and removing diagnostic state.

Build 77 also removes writer methods returning opened key services. Admission
and promotion compose opening, signing and exact preparation inside the writer
and receive the existing PreparedWrappedStoreKey. Internal removal and refresh
use the private keyring owner directly. Build 77b compiled all eight library
binaries and full77 verifies their runtime behavior. The owner-return diagnostic
still reports a test-only unadopted-removal method returning an EncryptionService;
that capability propagates through otherwise closed authorization constructors.
Build 78 removes that return: refresh fixtures retain only accepted membership
and independently authenticate its exact wrapped keys with test-owned cloud and
identity. Concurrent-rotation tests preserve their captured earlier membership.
Build 78 compiles all eight library test binaries. Focused refresh verification
passes all 12 cases, including custody, concurrent rotations, stale-key refusal
and the previously uploaded blob's snapshot after rotation. The latest complete
library inventory remains full77; this is a focused receipt for subsequent edits.

The build-78 source owner-return diagnostic removes the unadopted key-service
return and its propagated authorization findings. Remaining findings include
prepared snapshot ownership, the consuming authorship transfer, the installed
join test getter, composition registrations and retained-service classifications.
No checker exemptions have been added. Snapshot installation currently opens a
transient source under PreparedStoreSnapshot but passes its connection and
payload directory separately to StoreSession. The pending owner-boundary change
keeps the consuming installation under that existing prepared artifact and
removes the arbitrary-source entry point. Preserve transaction rollback and
preparation cleanup throughout; runtime verification remains required.

Build 79b compiles the snapshot owner change after fixing its exports through
the existing Store module. The source connection and its payload directory stay
inside PreparedStoreSnapshot's consuming installation. DatabaseCore closes the
preparation; ConnectionWorker transfers it directly to the artifact without a
forwarding seal method. Focused verification passes four preparation lifecycle
tests, fifteen publication/snapshot tests and five warm-adoption tests. The raw
SQLite/Coven SQL findings in publication tests are removed by using the existing
DatabaseImageTest owner; original row, retained-control and private-access
assertions remain. Visibility passes. Formatting identified 73 files, and
workspace formatting was applied; the final formatting gate remains required.

The installed-device-join database getter has three test consumers. Two now
read/reopen the completed image using test-owned database operations. The Circle
row-clock case must keep observing the same running clock before and after the
real completion, because reopening would test restart seeding instead. Its
consuming test operation keeps that database internally and returns no
dependency. The getter and all three callers have been replaced in source;
their rebuild and runtime verification remain required.

Strict lint run 80 first failed in sccache with the process file limit of 256.
Restarting the shared cache server with a 4,096 limit and running subsequent
checks with that same limit reached actual compiler diagnostics. Nine database
lint findings were corrected; run 81 passed that crate and reported 21 replication
findings. Their source corrections remove redundant borrows/copies, an Owner
promotion loop whose branches both immediately returned, duplicate abandonment
match arms, and a unit-valued test binding. Strict lint run 82 passes all targets
and features with warnings denied. Formatting check 82 and diff whitespace
checks pass. Runtime verification of these newer edits remains required.

The owner checker review separates newly produced operation results and
consuming operation transfers from retained dependency getters. Its first test
run passed 85 and failed three: stale composition registrations and a transfer
fixture whose returned wrapper itself leaked a borrowed permit. After correcting
the registrations and separating allowed transfers from leaking-wrapper cases,
all 89 checker tests pass. Gate 80b then reported one real finding: TestStore
returned its retained producer device. The fixture now receives immutable root
metadata and its existing prepared commit; publication stays inside TestStore
and the caller asserts the exact published reference. The current-source full
owner gate 83 passes without another exemption. Formatting and visibility 83
pass. Build 83 compiles every all-feature test target, including the checker;
all 18 targets pass their full runtime runs as recorded above.

The required seeded production-owner sequence comparisons have not yet been
established by the existing test inventory. Search found deterministic fixture
identity seeds but no generated operation/delivery/restart/compaction scenario
runner. Review confirmed the gap and identified an implementation using existing
Store owners, two disk-backed receivers, delayed delivery and physical retirement;
implementation and execution remain open alongside the complete check contract.
The first generated scenario source is saved and compiling, with independent
expected row/blob outcomes identified as an additional required assertion beyond
receiver-to-receiver equality. The final stale-API audit identified unused
StoreDeviceHead loaders/upload markers and a MergeWinner receipt constructed
only by a synthetic test. Their real consumers and replacement tests are being
traced for removal under this plan; current acknowledgement/Circle chains and
StoreAnnouncements-derived stream identity must retain their real purpose.
Live S3 test configuration is absent: all eight endpoint/bucket/credential/object
variables checked are unset, and localhost:19000 refuses a connection. The nine
ignored live-provider tests have not been represented as passing. After build83,
with no compiler active, 12 incremental-cache directories idle over 30 minutes
were removed (5,098,823,680 allocated bytes); current artifacts and test binaries
remain. Free space was 14 GiB after that cleanup.

Strict rustdoc 85 passes for all nine workspace members with broken/private
intra-doc links denied. The site-to-generated-rustdoc link check also passes.
The first seeded-test build 84 compiles, but it was not used as runtime evidence:
review identified a Join-install destination collision in its fixture and missing
independent expected outcomes. The revised fixture uses a separate live database
path, checks deleted/withdrawn/replaced content directly, preserves the pending
title alongside the accepted body, and verifies every current shared blob against
its original generated bytes. Build 86 compiles but the new runtime comparison
fails: the caught-up receiver downloads the current snapshot image. Build 87
adds exact preconditions proving that the receiver observed the snapshot
predecessor and materialized the exact signed coverage before the unchanged
no-download assertion. Build 87 compiles and reproduces that same failure
after all three preconditions pass (0 passed, 1 failed).

Source review locates the selection error in load_store_publications_for_replay:
a newer snapshot chooses Checkpoint without checking locally owned history.
The retained interval installer already reconstructs the baseline, preserves
private effects, and rebases pending writes in its installation transaction.
The saved correction selects it when both the exact predecessor and materialized
coverage match, reading coverage with the existing atomic replay inputs; both
callers consume the extended read result. Independent source review found no
issue in that selection. A receiver with held rows must continue to use the
downloaded checkpoint; the new opposite-case regression and final restart
pending-write assertions are saved and await runtime verification.

The obsolete StoreDeviceHead/MergeWinner upload, verification and receipt
replacement chains have been removed from source; their remaining fixtures use
real accepted snapshot or ACK evidence. Compilation and runtime checks after
that removal remain required. Build 87 compiled the initial removal; the orphan
Store-head slot/reservation chain is also being traced out while preserving the
author-stream identity domain.

Before the next build, with no compiler active and both agents holding Cargo,
three incremental directories idle over 30 minutes were removed. The receipt
is reclaim88-incremental.json (7,324,008,448 allocated bytes across those trees;
APFS sharing means this is not a physical reclaimed-byte measurement). Free
space increased from 7.3 to 10 GiB. Current binaries, libraries, and recent
incremental caches remain.

Earlier `full63`/`full63b` diagnostic inventory: 2,153 passed, 19 assertion
failures, 11 ignored, with enlarged-stack diagnostic runs and the separately
failing fixed-stack case excluded. Those diagnostics are superseded by full77.

- Build 63: predecessor-result interruption blocks a subsequent membership
  publication before journal staging; ordinary peer row publication remains
  available, and finalization retry permits the membership transition.
- Build 63: both cold Join history depths use 12 membership operations. The
  previous absolute budget of six still fails; the exact request diagnostic
  must establish the necessary count.
- Build 63: cycle reclamation passes both cases and snapshot-generation restore
  passes. Documentation site generation also passes.
- Build 63: interrupted membership cleanup and already-satisfied removal tests
  exposed a fixture host-identity mismatch and a facade attempting to complete
  a journal already consumed by verified satisfaction. Corrections are saved
  for build 64. The rotation cleanup API still accepts an optional argument;
  a real omission regression is saved before removing it.
- Build 63: the fixed-stack Circle flow overflows while composing snapshot
  history during acknowledgement publication. LLDB identifies accumulated
  verification frames; raising the test cap is not the fix.

- Build 62: the prepared Owner removal no longer revives the Member previously
  removed by that Owner. The real retry publishes an abandonment, cleans the
  rejected candidate, and publishes a replacement; a fresh reader preserves
  the earlier retirement.
- Build 61: mixed Store/Circle history physically retires the covered Store
  package while retaining the Circle input; successor snapshot capture and
  restore preserve the Circle row and exact blob.
- Build 62: all three ancestor-image cases pass: the required older bootstrap
  restores the row, and missing/corrupt selected images reject restoration.
- Build 62: one cold Join membership-head traversal passes. The depth check
  still fails at 12 versus 20 membership operations.
- Build 61: seven Join finalization cases pass, including lost response,
  restart, retired handoff artifacts, and lazy-blob retention across images.
- Build 62: snapshot publication request counts, excluding separately asserted
  exact inherited retirement checks, are `[31, 27, 27, 27]` at all tested
  history depths. Each inherited retirement object is still checked twice.

Raw receipts are local `.verification-logs/` artifacts and are not staged.

### Focused verification after the inventory

- Build 65: 12 admission/removal authority cases pass. The replacement retains
  WriteId, advances its Store coordinate, removes the superseded candidate and
  delivers a keyring that decrypts content published after rotation. Database
  completion derives the cleanup generation from the retained request.
- Build 65: predecessor finalization and alternate signed rollup-result tests
  pass, as does the excluded pending-successor guard. The unchanged 1.5 MiB
  Circle stack test still aborts; child-future measurements are prepared.
- Build 65: all six blob-capture cases pass with database-owned image and binding
  operations, including the independent SQL/object identity guard. The conflicting
  admission test preserves the accepted grant and subsequent pull access.
- Build 65: exact covered reclamation succeeds; forged lower-commit and
  substituted-package references are refused. Stranded-source reclamation now
  finishes its publication, but its physical Circle blob remains pinned by
  retained Circle replay. The fixture must advance actual Circle coverage.
- Build 64: ten snapshot-authority cases and the Owner recovery test pass. The
  latter physically deletes the retired snapshot before testing recovery.
- Build 65: settled cycles issue nine provider operations. Snapshot publication
  issues `[32, 28, 28, 28]` after independently checked inherited retirement
  operations are excluded, at each tested depth of 1, 4 and 8 rounds. Existing
  budgets still fail; operation-class diagnostics are prepared before revising
  them. These focused receipts do not replace the suite-wide inventory.

### Saved work awaiting further verification

Build 71 compiled all eight library binaries. Both staging regressions and all
11 excluded-authority tests pass. Locator preservation exposed a missing local
publication registration path; build 72 passes stranded-source reclamation, all
six blob-capture tests and all seven Join-finalization tests. Two reviewers hit an
account usage limit; the snapshot reviewer continues the independent review.

Build 67 focused results:

- The real initial-removal staging regression fails: omitting the caller-supplied
  generation leaves no durable rotation gate. Staging now receives the prepared
  candidate and derives its reservation and generation. Initial staging and
  replacement share exact candidate-object validation. The regression passes on
  build 68, as do all 11 excluded-authority/removal tests.
- All 17 acknowledged-history tests pass, including settled and snapshot request
  budgets at all tested depths. All seven Join finalization and six blob-capture
  tests pass. The typed missing-head/cursor test passes.
- The unchanged capped Circle regression and the ordinary domain four-retry Join
  test still overflow. Boxing the measured Join children does not resolve these
  gates.
- Stranded-source reclamation reaches its physical assertion without overflow,
  but the Store source remains activated by a retained Store commit. Its exact
  ownership record is in `root-focused67-2.log`; investigation continues.
- A domain LLDB trace stops in dropping the covered-write database reply, beneath
  admission/publication calls during test setup. This is a stack-depth observation,
  not evidence that the reply owner itself is the cause. The trace is retained in
  `domain-join-stack68-lldb.log` (captured while the build 67 binary was installed).
- Build 69 removes the discarded pending-entry projection in admission; the
  accepted publication verifier owns predecessor validation. Admission continuation
  now reloads durable provider progress before both satisfaction and replacement.
- Build 69: same-member peer admission and admission after rotation pass. The
  different-role case reaches `ExistingMemberMismatch`; its stale string assertion
  prevented checking journal release, so build 70 uses the typed error assertion.
- Independent staging review found that exact object-reference equality does not
  bind a wrap's pending candidate owner. A new regression substitutes another real
  commit as owner while preserving the exact wrap and its bytes. Production
  validation remains unchanged pending that failing receipt.
- Stranded-source diagnostics now distinguish row binding, retained replay claim
  and accepted-image eligibility. These temporary diagnostics must be removed
  after the real skip reason is established.
- Build 70b: initial rotation staging passes; substituting another real commit as
  a wrap's owner is wrongly accepted. The shared staging/replacement validator now
  requires each supplied record to be prepared for the exact candidate; verification
  is pending.
- Build 70b: same-member admission and admission after rotation pass; different-role
  admission returns the typed mismatch but leaves the abandoned journal reserved.
  Its terminal cleanup needs a result distinct from successful admission.
- Build 70b reclamation diagnostics show orphaned sources retained before snapshot
  replacement, then absent from candidate enumeration despite surviving remote
  ownership rows. Review traced locator indexing to row-winner installation and
  replay projection replacement; the preservation fix awaits verification.
- Build 71: both rotation staging tests and all 11 excluded-authority tests pass.
  Blob capture fails all six tests, and Join finalization passes six with one
  failure: local publication activates remote blob records without recording
  their locators before replay installs row links. The stranded-source fixture
  fails at that same earlier foreign-key check. The reviewer saved exact locator
  registration in the local completion transaction for build 72; pull and image
  activation already use that owner.
- Build 71: the unchanged 1.5 MiB Circle test still overflows. LLDB stops while
  cloning retained history in PublicationVerification::new, beneath publication
  verification and Store write publication. The changing bottom frame does not
  establish a new cause. Current Rust layouts measured using the compiled crates:
  membership proof 8,416 bytes, Store commit 2,776 bytes, acknowledgement chain
  2,960 bytes. No new stack change is saved; review is tracing shared owner calls.
- Admission review requires a distinct rejected terminal outcome, authenticated
  against the accepted membership and the exact cleaned abandoned candidate.
  Reusing the rejected request's provider grant before noticing a changed
  predecessor can also reapply an obsolete email; check before that side effect.
  Preserve accepted access, and cover different role and different email.
- Build 72 compiles all eight library binaries. Stranded-source reclamation passes
  its exact physical-deletion and retained-locator assertions; all six blob-capture
  and all seven Join-finalization tests pass. Logs: `root-focused72-{0,1,2}.log`.
- Build 73 compiled all eight binaries with the reviewed immutable verified-record change:
  VerifiedStoreBatchCommit shares its private commit and author via Arc, retaining
  the verification constructors and borrowed API. Before this change the verified
  record is 3,600 bytes and CoveredStoreWrite is 7,992 bytes. Both unchanged stack
  regressions must pass; a different crash leaf is not evidence of resolution.
- Build 73 also includes a different-provider-email admission regression and
  stronger exact peer-wrap/source-deletion assertions for conflicting requests.
  Admission termination and provider-side-effect behavior are unchanged, awaiting
  that failing receipt and the authenticated terminal cleanup implementation.
- Build 73: the unchanged 1.5 MiB Circle cancellation regression passes. The
  domain four-transfer retry still overflows, now in Join registration finalization.
  VerifiedStoreBatchCommit is 208 bytes and CoveredStoreWrite 4,600 bytes; the
  retained publication journal completion still carries a 4,344-byte abandonment
  payload through every variant. Build 74 boxes that existing payload and updates
  its sole producer and consuming finalizer, without changing verification.
- Build 73: admission after rotation and same-member admission pass. Both unequal
  role and unequal email reach the typed mismatch but retain the abandoned journal.
  Build 74 adds an authenticated rejected-admission completion beside satisfied
  completion; both share one transaction and exact boundary/cleanup checks in
  membership_request_completion.rs. Independent review found those checks intact.
- Provider access review: Granted/Activated records a completed step before CAS.
  Resuming that same candidate should reuse its join_info; Pending checks the
  current predecessor before calling the provider. Replacement must reset progress
  to Pending atomically with its plan because a peer may have revoked access.
  Build 74 records provider access requests and checks that conflicting-email
  retry preserves accepted access without repeating the obsolete request. The
  completed-step/reservation-reset change awaits that regression receipt.
- OAuth providers grant permissions independently by email; CloudKit addresses
  the member public key, and S3 does not support per-member credential revocation.
  The journal does not prove ownership of an old email permission. Terminal
  rejection ensures accepted access and never blindly revokes the rejected email;
  this is not a claim that independent preexisting email permissions were removed.
- Owner-boundary gate 73 remains red; the complete findings are retained in
  owner-gate73.log. No main branch or dependency pin advanced during these builds.
- These focused results do not replace the latest full-suite inventory above.

Build 66b focused results:

- Pull authorization failure preserves the optional genesis boundary, and retry
  installs the exact commit and row. Refresh refuses missing newly published
  membership evidence while preserving the installed boundary, frontier and key.
- Warm membership reuses retained exact evidence; a fresh reader refuses the
  missing entry. Cold Join matches its traced request classes at both depths.
- Offline-peer snapshot retirement and the package/publication-object deletion
  journal assertions pass.
- Admission after rotation passes. Two new peer-admission continuation cases
  fail at `MissingConflictHeads`: the admission path appends its older pending
  entry to the current chain before reaching publication predecessor validation.
- The stranded Circle blob fixture now enters the real epoch-close/bootstrap
  path but overflows its normal thread stack. Its physical-deletion outcome is
  not yet verified.
- The uncapped cancellation diagnostic passes. Measured Join child futures are
  provider authorization 125,360 bytes and completion 85,760 bytes. Build 67
  boxes those two child operations; the unchanged capped regression remains
  the required check.
- Provider traces explain settled nine operations and normalized snapshot
  counts `[32, 28, 28, 28]`. Budgets are updated and temporary traces removed
  for build 67. The missing-head cursor test now checks typed exact NotFound
  and unchanged cursors rather than requiring an obsolete error string.
- Initial staging still accepts an optional duplicate rotation generation. Its
  omission regression was not saved when the agent stopped; root completed it
  for build 67 using the real prepared removal and restored pre-staging image.
  The production fix remains pending the failing receipt.

Saved work includes:

- Signed predecessor receipt references and exact receipt transport in
  membership rollups, replacing repeated provider reads of covered receipts.
  The real failed-result-upload regression was red on build 62; the producer
  must refuse another membership transition until the predecessor is finalized.
- Database checks that abandonment retires an actually superseded publication
  attempt and cannot consume an already covered author position.
- Retention of the original signed membership request through remote cleanup.
  Replacement or verified satisfaction consumes its proof, journal, rotation
  candidate, and reservation in one transaction. A shared accepted-membership
  check now serves both Circle discard and satisfied removal.
- Negative unsatisfied-completion, interrupted cleanup/restart, hostile cold
  reader, and already-satisfied removal tests. These must pass before claiming
  the continuation boundaries are verified.
- Database-owned image and binding test operations replace raw SQLite and
  Coven-table SQL in snapshot authority and blob-capture fixtures, retaining
  exact row-stamp, object identity, state, and pending-write assertions.
- Corrected recovery and reclamation fixtures using real accepted snapshot
  coverage and physical object deletion, plus test-file splits and current
  Circle/bootstrap documentation.

### Verification after local reconstruction selection

Build 88 compiles the selection correction and head-slot removal. The held-row
checkpoint regression passes; the generated scenario passes its original
no-image assertion, then fails because only one receiver had consumed the
accepted reclamation suffix. Inspection confirmed reclamation publishes
control commits. The fixture now delivers that same suffix to both receivers
before comparing results, preserving the original no-download assertion.

Build 89 compiles the revised fixture (SnapshotAdoptionReceiver) and passes the
held-row test. The generated scenario now agrees on accepted boundary, baseline,
rows, exact blob bindings, retained commit references and private bytes, then
fails on snapshot blob ownership: local reconstruction retains only the prior
snapshot owner while downloaded adoption also carries the current owner.
Ownership expectations and both installation paths are being traced before
changing the assertion or production code. The complete seeded scenario remains
unverified (89: 1 passed, 1 failed). The strengthened oracle independently
requires the current snapshot owner and permits only historical owners protected
by signed pending-Join evidence. It must derive ownership from the reconstructed
cut image, not current rows that unpublished work may have changed. Combined
verification 90 includes that oracle, the schema regression and existing warm
adoption cases; no ownership or schema fix was applied before this run.

The owner checker now registers exactly the fixture's join/restart composition
constructors. Renaming the fixture removed its collision with Tokio Receiver
in the checker. Checker 90 found one stale verification-read registration for the deleted
commit/announcements.rs. After removing that exact path, checker 90b passes all
89 tests. Its raw-read policy is unchanged; no exemption was added. Full source
checker execution remains required after the current edits settle.
Source review confirmed that reconstruction bypasses the checkpoint-only
application-schema guard. A real migration/same-cut regression now requires
SnapshotRestoration(SchemaTooNew), no image read, and unchanged receiver state.
The proposed correction moves that existing policy to the shared verified
publication read before either installation path. Join/bootstrap already guard
their schema, and ACK advancement reads only an installed accepted snapshot.
Production schema code is unchanged pending the failing regression receipt.

Encryption documentation now distinguishes encrypted data from signed readable
control records and describes current exact paths, key fingerprints, device
signing and chunk framing; doc links and whitespace checks pass. Its source
comments and unused reconstructed-cloud-path API are being updated together.
The latter has no Bae runtime consumer: only a test wrapper compares guessed
paths, although that test already holds exact stored blob references. Remove
the wrapper and compare actual locator slots during the Bae pin update.

Verification 90 compiles and runs the warm-adoption namespace: 6 passed,
2 failed. The strengthened ownership check fails because the local receiver
lacks the current owner; its historical owner is explicitly protected by the
signed pending-Join set. The schema case fails before exercising the guard:
its publisher reopen used a signed registration ID instead of its configured
host database ID. That fixture now reopens with the original host ID; the real
migration and exact predecessor/coverage assertions remain. Schema production
code still awaits its failing receipt. The ownership correction is being built
from live exact Store blob references in the validated cut, without importing
orphan historical inventory into the current receiver.

Full owner-boundary source gate 91 passes all flags in the repository script.
The orphan blob-path API chain and two dead path helpers are removed, while
path validation tests exercise the actual cache path builder. Current locator,
protection and declaration-policy documentation has been corrected without
changing the readable-name or write-once validation policies. The naming-rule
simplification is recorded for later in the ignored main-checkout follow-up
outline, not folded into this behavior change. The Bae test-wrapper update is
still required with the pin.

Verification 91 compiles and runs the warm-adoption namespace: 6 passed,
2 failed. The local reconstruction path now retains the current snapshot's
live exact Store blobs in the adoption transaction. The prepared-baseline
owner validates and reads one cut image for both leases and snapshot ownership;
it does not import orphan inventory or derive ownership from later live rows.
The generated scenario passes its first snapshot ownership comparison, then
fails an incorrect status expectation. Source review confirms rebase retains
the author reservation in AwaitingPreparation and sets the write to Pending;
Publishing applies once replacement bytes have been prepared. The test will
assert that exact transition while preserving all identity and lease checks.

The corrected schema fixture reaches its intended failure in 91: an older
schema accepts a newer-schema snapshot at the same cut. The existing schema
guard has now moved from downloaded-image preparation to the common verified
publication read, before either installation path. Verification 92 will cover
that correction and the generated status assertion. Independent review is
also mapping remaining membership and receipt requirements to actual owners
and regression coverage. No main branch or Bae pin advanced during 91.

Verification 92 compiles; the schema guard returns the exact expected error,
but the fixture's source-chain downcast targets the inner boxed value instead
of its exposed StorePullError wrapper. The corrected typed assertion passes
in 93, including every no-image and unchanged-state check. The generated case
passes its first row/blob ownership and reserved-write comparison, then fails
in a later pull authorization while reclaim verification reads a physically
absent snapshot image. Its exact image identity is being correlated with the
installed baseline and current accepted snapshot before changing production.
93 warm-adoption result: 7 passed, 1 failed; full current-source verification
remains outstanding.

Independent audit confirmed an unimplemented issuer-authority-loss requirement:
membership continuation tries to publish an abandonment after its initiator's
grant is retired. The new production-owner admission test reaches that failure
in 93 (0 passed, 1 failed), with its journal still retained. The proposed fix
reuses the authenticated grant-retirement proof already used by Circle discard
and the existing retired membership request completion transaction. It must
preserve the original author reservation, require exact cleanup, and author no
new abandonment. Physical uploaded-object, interrupted-cleanup and reopened-owner
coverage is being added before production changes. Owner-promotion issuer loss
and remaining receipt/stack requirements are also being audited. No main branch
or Bae pin advanced in 92 or 93.

The independent requirements 2–3 audit found existing coverage for finalized
successor receipt selection, pending/excluded heads, alternate valid signed
receipts, restart and compaction. It verified the original capped Circle
cancellation and normal-stack domain retry tests, plus the real Circle
epoch-close/bootstrap stranded-source deletion assertions, against full83's
passing receipts. These await current-source reruns rather than new fixtures.
Owner-promotion issuer retirement remains a separate active source audit.

Verification 94 is running with the generated current-image-survival assertion
and temporary exact image identity diagnostics, plus eleven admission tests
covering uploaded candidate objects, interrupted deletion/reopen and invalid
retirement proof refusal. Production reclaim and membership retirement changes
have not been applied yet. Before this run, three replication incremental caches
idle for over 72 minutes were removed after verifying no compiler was active;
the active compilation cache was preserved. The cleanup receipt is
reclaim94-incremental.json and free space afterward was 7.4 GiB.

### Verification 96–97: snapshot inventory and issuer retirement

Build 96 compiles. Warm adoption passes 7 and fails the generated sequence
while authorizing delayed adoption after physical retirement. The missing image
is the receiver's older installed replay baseline, not the fresh accepted
snapshot. Admission passes 11 and fails the interrupted-abandonment fixture
before candidate upload. Both promotion-request cases reach incorrect success;
both promotion-merge cases also stop at their upload precondition.

The upload faults counted one create too few: membership-head upload precedes
the commit. Corrected fixtures interrupt the following publication entry and
assert actual candidate bytes before removing the issuer. Build 97 compiles
and reaches the intended production failures: admission retirement rejects the
active abandonment as a different reserved candidate; promotion merge retry
fails publication authority without completing retirement. The two promotion
request cases still succeed incorrectly. These five failures now exercise real
persisted/uploaded candidates rather than fixture setup.

The snapshot verifier now walks commits and snapshots in accepted entry order.
Store-blob reclaim validation uses the commit's exact accepted snapshot base,
whose identity the publication interval validates, rather than the older floor
where retained history traversal stops. It preserves that floor and the existing
exact metadata/image validation; no alternate lookup or second history is added.
Independent source review found acceptance binding and rollback intact. Build 97
passes all eight warm-adoption tests, including all three generated delivery,
restart and physical-retirement sequences (18.97 seconds). Temporary image
identity diagnostics were then removed; seed/step/stage context and independent
row/blob/ownership/journal assertions remain. The full replication binary run
is in progress; complete current-source workspace gates remain outstanding.

The promotion audit identified why request publication can survive issuer loss:
its non-Operations body bypasses the ordinary membership creation-grant check.
The acceptance correction requires a race regression with issuer removal after
preflight and before conditional publication. Admission's pending-abandonment
cleanup and promotion's existing terminal journal states are being implemented
under their existing operation owners. Neither main branch nor the Bae pin
advanced during these verifications.

Full replication 97 terminates with 936 passed and 6 failed, no ignored or
filtered tests, on ordinary stacks with four test threads (130.82 seconds).
Five failures are the issuer-retirement cases above. The sixth is the existing
concurrent checkpoint-import cleanup fixture: its caught-up receiver now
correctly reconstructs locally and returns before the downloaded-image pause.
The fixture now publishes an unseen peer row before the successor snapshot,
checks that its predecessor differs from the receiver's observation, and checks
that the unseen row is installed. Its exact installation pause, concurrent
ownership change, candidate deletion, and retained-blob assertions remain.
Verification of that corrected downloaded-image setup is pending build 98.
Restricted-path visibility 97 passes, and searches find no StoreDeviceHead,
MergeWinner, UncreatedVerified or removed inventory-floor helper references in
Rust sources or the repository/site Markdown.

Build 98 is compiling the saved issuer-retirement implementations. Admission
retains the original membership candidate and, when present, its exact pending
abandonment under the same reservation. Both receive independent authenticated
nonactivation proofs in one transaction. The owner validates the bundle at
construction, persisted decoding and consumption, and completion requires each
candidate's exact cleanup. Added cases cover interruption/reopen with both
candidates and rejection of a malformed persisted bundle containing duplicate
original requests. Existing accepted-abandonment continuation remains distinct.

Promotion retirement uses its existing Nonactivated and Stale journal states.
The database owns proof derivation, exact candidate and publication cleanup,
and reservation release; replication awaits provider deletion before recording
completion. Generic journal advancement cannot bypass nonactivation proof, and
replacement refuses outstanding cleanup. A new race fixture pauses request
publication after preflight while a peer removes its issuer. The shared request
membership gate intentionally remains unchanged until this fixture establishes
its failure. Independent source review of admission's bundle found no issue;
review of the persisted decoding addition and promotion remains underway.

Before build 98, no Cargo/Rust/Swift/Xcode compiler was active. Removed 57
incremental-cache directories with no descendant writes for over 90 minutes,
retaining current compilation caches and every binary/library/source/worktree.
The receipt records 4,912,107,520 allocated bytes; actual free space rose from
4.5 to 8.3 GiB. No main branch or dependency pin advanced.

Build 98 compiles (2m48s). Warm adoption passes all 8 cases, including the
three generated sequences after diagnostic removal. All 3 covered-write
completion tests pass, including the corrected concurrent downloaded-image
fixture. Promotion retirement passes all four request/merge deletion/reopen
cases; the new concurrent-removal test fails because the shared gate accepts
the request after its issuer is removed. The correction dispatches the request
body explicitly and checks its actual current Owner grant and exact creation
authority; ordinary operations and separately authorized reclamation/cancellation
bodies keep their distinct contracts.

Admission 98 passes 11 and fails all three pending-abandonment bundle cases:
the abandonment candidate deliberately has no membership creation authority.
This disproves the proposed second grant-retirement proof. Its signed manifest
binds the original candidate, root, device, position and predecessor; an active
device may finish that already-authored cancellation after the principal's grant
ends. Resume the existing prepared abandonment and its AcceptedAbandonment
cleanup, then complete the original retired-grant request. Do not author a new
abandonment after grant retirement. The speculative bundle/variant/decoder check
and bundle-corruption test are being removed, while retaining interruption and
reopen coverage through the actual existing lifecycle. Independent review is
checking this distinction before build 99. Full98 workspace tests were not run.

The full source owner gate 98 reports the speculative admission test's raw
Coven-table SQL (removed with that disproved test) and a private raw Connection
return from the reconstructed-image helper. Checker review confirms raw DB
returns are intentionally forbidden even for a newly opened private result.
Keep one fresh image local to each existing prepared-owner operation, sharing
its population/validation and derived lease calculation without returning the
connection or adding another owner type. Verification of that correction is
pending the next gate run.

Promotion review also identified a copied terminal cleanup list that is checked
at transition but not loaded-journal validation. Its receipt already derives the
same exact graph. Review is checking removal of that duplicate list and a real
paused same-operation upload against concurrent retirement; current provider
failure/reopen tests do not establish exclusion of an already-running upload.
No broad mutual-exclusion claim is made from the existing permits alone.

### Verification 99: existing cancellation and promotion ownership

Build 99 compiles. Admission passes all 14 cases, including already-authored
abandonment settlement after issuer retirement, interrupted accepted-abandonment
cleanup followed by reopen, and a successful conditional replacement whose
response was lost. The latter resumes without new uploads. The discarded second
nonactivation bundle is absent. Warm snapshot adoption passes all 8 cases and
covered-write completion passes all 3 cases.

Promotion passes 5 and fails 2 new production-owner regressions. The shared
request membership gate now refuses the request when its issuer is removed
during publication. The remaining failures demonstrate an in-flight upload
recreating its entry after concurrent retirement released the reservation, and
reopened journal validation accepting a substituted terminal cleanup inventory.
Use the existing own-stream authorship turn across journal loading, publication,
settlement and retirement; derive the cleanup graph from the validated receipt
instead of persisting a second list. These corrections are not yet verified.

The reconstructed-image helper now keeps its fresh connection local to each
prepared-image operation and shares validation that returns derived leases.
The full owner gate passes after this correction and removal of the speculative
admission fixture, before the final two promotion regressions were added. Its
rules were not weakened. Full replication 99 finishes with 945 passed and the
same 2 promotion regressions failed, no ignored or filtered tests, in 137.21
seconds. Complete workspace gates, landing and the Bae pin remain outstanding.

### Verification 100: serialized promotion and exact derived cleanup

Build 100 compiles (2m49s); all 53 promotion/admission tests pass (7.35 seconds).
The existing own-stream authoring turn now covers promotion journal loading,
preparation, request-result upload, activation and candidate retirement without
reacquiring the same permit. The paused-upload regression passes with both
callers returning their errors, the reservation released, the accepted record
unchanged and every candidate object physically absent. Terminal cleanup derives
its objects from the validated receipt; the duplicate inventory and its helper
are removed. Independent reviewers found no issue in the permit handoff,
receipt validation or accepted/lost-response continuation.

Review identified one further terminal retry case: replacement retains the old
promotion-id journal but changes the target index. The completed old attempt
must still return its original typed outcome. Cleanup currently checks both
indexes before checking whether the old attempt still owns a reservation. A
production-owner regression is being added before changing that ordering.

Before build 100, no compiler was active. Removed only 7,409 stale intermediate
objects matching `coven_replication-4eb1c8f7818f2523.*.rcgu.o`, all untouched for
over 9,730 seconds. Preserved every test binary, library, current build object,
incremental cache and worktree. Free space rose from 6.4 to 27 GiB; the local
receipt is `reclaim100-stale-objects.json`. No main branch or Bae pin advanced.

Verification 101 compiles (1m33s) and reproduces the terminal-retry error through
the real failed-attempt replacement and finalizer: the old acceptance returns
`promotion retirement lost its exact journal` instead of its retained typed
Stale result. The correction keeps exact ID-journal validation for every retry,
and requires the current target index only while that operation still owns its
active reservation. Initial retirement continues validating both indexes.
The live-provider availability check still finds all eight S3 test variables
unset and the local test endpoint refusing connections; these checks are not
reported as passing.

Verification 102 passes all 54 promotion/admission tests (7.61 seconds), including
the retained terminal outcome after target replacement. Independent review of
the saved correction found exact ID/target/reservation validation intact and no
mutation of replacement state. The complete `scripts/check.sh` is now running
against these sources; no source edits are planned while it runs. This receipt
does not yet establish full workspace validation or landing.

Full gate 102 passes ownership and stops on three assertion-format differences;
the formatter applies them without behavior changes. Gate 103 then reaches
strict Clippy and finds a test-only `SyncCycleCause` re-export compiled in the
production library. Its only external consumers are membership tests; gating
the re-export with `cfg(test)` preserves the production cause and error chain.
Independent review confirms that scope. Full gate 104 passes ownership,
formatting, strict all-target/all-feature Clippy, strict rustdoc and site links;
all nine shipping feature checks and both runtime configurations subsequently
pass. The complete run exits successfully, with the counts recorded above;
the site build also passes. No Rust source changed after this verification.

A lean bae worktree at
`/Users/dima/dev/bae/.worktrees/coven-snapshot-retirement-pin` prepares the
consumer update without a platform build prime. It shares the existing FFmpeg
distribution. The obsolete test-only blob-key facade is removed; cover replacement
checks compare the actual recorded cloud keys and exact stored references.
The dependency revision is unchanged until Coven lands, and the unrelated main
checkout localization edit remains untouched.

### Verification and remaining landing work

The complete check104 run covers membership continuation and issuer retirement,
receipt compression, retained-history and snapshot adoption, physical Circle
source preservation, generated production-owner sequences, and the original
stack regressions. Independent reviewers checked the final snapshot, admission,
promotion serialization, derived cleanup and terminal retry corrections. The
ownership/visibility, formatting, strict lint, documentation and feature gates
pass. Live S3 checks remain unavailable for the reasons recorded above; ignored
tests are not counted as successful checks.

1. Commit the reviewed source and documentation paths with normal hooks, rebase
   onto Coven main, merge fast-forward only and push Coven main immediately.
2. Update Bae's pin to the pushed Coven revision, verify its affected components,
   commit with normal hooks, merge fast-forward only and push Bae main.
3. Confirm the final local branches, remote branches and dependency revision.
   Preserve both worktrees and the ignored main-checkout follow-up outline.
