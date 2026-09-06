# Preserve local state through causal replay

## Decision and status

Implementation plan following investigation of Coven `f0dc1155` (production
source at `853d58f7`). The investigation used real host capture, publication,
pull, conflict application, and baseline advancement. It changed no production
code or live application data.

Keep the shared commit scheduler and walk the existing local write journal
alongside it. Complete the journal's Local partition so it describes what the
author keeps after the published audience packages apply. Retain the full
shared frontier observed by each write. Retire history only at a boundary that
later accepted work cannot cross.

Remove the proposed second event graph, separate capture/publication nodes,
extra local sequence and baseline counter, and separate stored publication base.
Do not substitute original host SQL for published SQL: the investigation
demonstrated different conflict results for those representations.

A private value must also not silently become shared authority when an incoming
relationship makes its ancestor shared. Conflicting private/shared identities
are rejected atomically, identifying the row and commit. Discarding private
values or publishing them requires an explicit host operation.

## Investigation results

Diagnostic programs used synthetic libraries, an in-memory cloud provider, and
production `test-utils` owners. Cached dependencies were matched through Cargo
fingerprints; their recorded source dependencies were older than their compiled
artifacts and the checkout had no source changes. This was not a fresh rebuild
or a full test-suite run.

| Production execution | Observed result | Consequence |
| --- | --- | --- |
| Create privately, share, delete child, advance baseline | Live and accepted-only replay have zero children; the advanced baseline has one. Starting shared gives zero throughout. | Old private INSERTs are replayed after later shared DELETEs. |
| Create shared, make local, advance baseline | Live retains one private note; the baseline retains none. Starting private retains it only through the earlier INSERT. | Reordering the existing incomplete partitions is insufficient. |
| Capture a peer update, make the author's note local, publish the peer update, pull | Pull succeeds but removes the private note, without a baseline advance. | Incoming materialization needs the repaired model too. |
| A private write reads a peer commit beyond an older snapshot cut, then that cut is acknowledged | The private write is folded and its journal removed, while its observed peer commit is absent from the baseline. | Status alone does not establish a foldable prefix. |
| Apply the real host sharing UPDATE versus its generated full INSERT to the same concurrent row | UPDATE gives `Author title / Peer body`; INSERT gives `Author title / Base body`. | Skipping an author's published SQL changes shared conflict semantics. |

Drivers, compilation commands, outputs, and a synthetic baseline are preserved
under `/private/tmp/straight-ahead-diagnosis/`; the record is
`current-investigation-results.md`. Diagnostic executables were removed,
releasing about 161 MB; retained evidence occupies about 744 KB.

Source review established two further required regressions:

- Anchoring private writes at the *next* publication is unstable. A tail folded
  before that publication existed would be placed on a different side of
  intervening commits when replaying without compaction.
- A `SharedKey` ancestor can be private on A and shared on B. A's private value
  at stamp 20 beats B's incoming INSERT at stamp 10; B's incoming descendant
  then makes that ancestor shared on A. The devices expose different shared
  values at the same frontier. This case is source-derived, not production-run
  yet. Matching package SQL alone does not ensure matching input state.

## Necessity review

| Mechanism | Decision and reason |
| --- | --- |
| Shared causal replay | Keep. Concurrent discovery, retraction, Circle cutoff and bootstrap can change which history supplies the projection. Replaying previous local application results preserves invalidated results. |
| Ordered write journal | Keep. It owns unpublished work, private effects, reversal, and mixed private/shared transaction resolution. Current private rows alone cannot identify which effects a retracted mixed transaction introduced. |
| Full observed frontier | Keep once per retained write. Without it, the post-cut experiment folds work before its inputs. Include the author's stream. |
| Separate publication base | Remove. Derive cross-stream dependencies from the observed frontier; choose and validate the own predecessor at preparation. |
| Two-event graph | Remove. Published writes have exact commit anchors; LocalOnly entries drain in ordinal order when their observed dependencies are covered. |
| Another residual payload | Remove. Complete the existing Local partition. Keep the original full changeset for reversal. |
| Baseline local counter | Remove. Atomic adoption consumes an exact retained journal prefix; the remaining journal says what is still owed. No dependency names a retired local ordinal. |
| Skip own package SQL | Reject. The executed UPDATE/INSERT experiment disproves equivalence. |
| Copy current private rows over replay | Reject. It bypasses mixed-write retraction, historical cuts, and private/shared ancestor conflicts. |
| Clone a database to simulate publication during every capture | Reject. Existing audience moves, full-state INSERT generation, and gate walkers supply the missing author rows directly. |
| Direct incoming apply followed by optional replay | Remove as a separate behavior. Preliminary live application can fail or trigger cleanup before the correct projection exists. |
| Acknowledgement requires immediate local retirement | Remove. Readiness to release cloud history and permission to discard local reconstruction inputs are different facts. Reuse acknowledgement proofs, without another coordination protocol. |

Rebuilding for each accepted row-bearing change increases replay work compared
with the direct path. This is a runtime tradeoff. Keep the verified-input cache
and advance baselines when admissible; do not introduce another application mode.

## Capture and stored shape

In `CapturedStoreWriteTransaction::execute`, read
`materialized_frontier_on(tx, None)` in the same transaction as the host write.
Store that `CommitFrontier` for every retained write, including LocalOnly.
An empty frontier means no shared history was observed, not missing metadata.

Replace the stored cross-stream-only `base` with this frontier. Preparation
excludes the actual publication stream when deriving outgoing dependencies.
Its chosen own predecessor must cover any position in that stream observed by
capture. Preserve capture-time cross-stream dependencies; upload-time remote
history must not replace them.

Reuse ordinal, WriteId, original changeset hash, partitions, status, exact
accepted position, and blob facts. A replay write has required captured data;
a folded publication receipt is not a replay write with empty defaults. Load
retained inputs using `changeset_hash` presence and validate status/data pairs.
Update internal schema and fixtures to one shape under Coven's greenfield rule;
do not manufacture missing frontiers for old journals. Shared wire ordering
and application schema migration semantics are unchanged.

### Complete the Local partition

Its contract is the author's private row/routing effect accompanying the
Store/Circle representations of the same captured transaction. All parts are
one atomic replay step. It is not a correction copied from current live state.

Extend `partition_outbound` while actual post-write state is available:

1. Keep original changes to rows remaining private, including real DELETEs.
2. Collect rows moving to Local and outbound host-row DELETEs whose rows still
   exist privately after the host transaction. Expand to their private
   retention and foreign-key closure, including ancestor-owned assets removed
   by outbound retraction.
3. Emit full post-write INSERT images for this set with
   `full_state_diff(Inserts)` and existing gate walkers. Exclude rows still
   shared through another descendant. `required_store_ancestors` alone is
   insufficient: it returns Parent rows, missing their inheriting assets.
4. Suppress original Local INSERT/UPDATE entries represented by full images,
   as existing destination materialization already does. Real deleted rows
   cannot enter the retained set.
5. Include captured private routing changes and complete private route images
   where needed, using the existing route helper. `routing.private_routes`
   currently excludes Local; it is not an available Local partition.
6. Capture facts and retain ownership for every blob these private effects need,
   independently of upload leases. Use existing fact/payload owners. Transfer
   surviving ownership to the baseline before retiring a write.

Store/Circle moves still use verified destination packages and routing.
Circle-to-Local additionally needs private row images, routes, and locally
materialized blob facts. Mixed transactions and reparenting use the same rule.

At the original capture context, replaying audience parts plus Local must
reproduce host rows, locality, routing, and exact blob facts. Test actual capture,
including transitions whose original changeset contains only a root UPDATE.

## Replay algorithm

### Existing shared order and one journal cursor

Keep accepted commit selection, authority checks, predecessor/dependency
readiness, canonical ready-batch order, Circle bootstrap coverage, epoch cutoff,
and exclusions under the existing replay owner.

Load retained writes in ordinal order. The shared frontier is the baseline
coverage plus successfully applied commits. One journal cursor follows:

| Head | Rule |
| --- | --- |
| LocalOnly | Apply Local as soon as its full observed frontier is covered; advance. |
| Associated accepted publication | Wait for its exact verified commit. Apply canonical audience packages and its Local part together; advance on success. |
| Pending/Publishing/Blocked without accepted publication | Wait until accepted shared history is exhausted. For current author state, apply captured Store/Circle/Local parts atomically without remote activation, then continue. |
| Resolved | Apply nothing, after the resolution dependency checks below. |

Drain eligible LocalOnly heads before the first shared application and after
each successful shared application, including between entries of a ready batch.
Do not move them to the end or a future publication. A preceding unaccepted
write blocks the local suffix. An unavailable exact dependency is an error for
current replay; a dependency beyond a historical cut prevents folding that
write and the suffix.

Before an associated publication applies, all earlier surviving journal inputs
must have been consumed. Its signed predecessor/dependency ancestry must cover
the earlier local entries' observed contexts. Otherwise identify the write and
missing reference; do not reorder public commits to make the cursor fit.

Association requires exact WriteId and accepted reference validated against the
publication record. Identity alone is insufficient: restored own commits may
have no original journal. Such commits apply canonical packages normally.

Published and unpublished replay use the same captured row representations.
Only accepted publication activates remote objects. Original full host SQL is
for explicit reversal, not a substitute for packages.

### Atomic realization and private conflicts

Apply an associated commit's audience packages and Local part before final
foreign-key checks, scoped pruning, and binding validation, within one
savepoint. Visibility cleanup must not remove the Local part. A held package
rolls back both parts, so retry cannot apply either twice.

Validate the shared result independently before private retention is allowed to
satisfy final constraints. Every surviving shared row must have its parents and
accepted row values supplied by shared state. A Local INSERT must not repair a
missing parent of a concurrent shared sibling: a peer has no such Local input.
Private-only foreign-key obligations may be completed by the Local part; shared
ones may not. Use the gate/FK closure and known incoming row representations
inside the same savepoint, without a second scheduler or persistent database.
An indeterminate closure fails explicitly rather than borrowing private rows.

Validate Local realization against replay-time visibility too. A row captured
as private may now be shared through a concurrent descendant; its Local effect
must not overwrite, delete, or supply shared authority. Return an atomic conflict
if that captured private effect cannot coexist with the shared result. The own
publication exception applies to its public representation, not arbitrary Local
rows. This also covers concurrent withdrawal plus a new shared sibling.

Continue verified membership/device controls, Circle rules, remote activation,
and exact winning blob bindings under their owners. Separate row/routing/binding
realization from publication bookkeeping and filesystem cleanup. Historical
DELETEs followed by private retention must not cancel live transfers or delete
their bytes. Unpublished realization uses captured facts without inventing
activated locators or signed commits; invalid Circle context fails explicitly.

Before an incoming step changes the shared set, check private rows it touches
and ancestors/assets whose visibility changes. A private value is not an
accepted shared predecessor:

- Exact associated own publication may introduce its captured private values;
  that representation is the recorded sharing operation.
- An incoming complete image may adopt an equivalent private row if all host
  data columns, foreign-key identities, and blob content identities agree.
  Exclude only version/locality/routing/binding metadata from equivalence and
  replace those with validated accepted values. Do not keep a private timestamp
  as shared authority or omit schema/blob validation.
- Conflicting state, destruction of private retained data, or insufficient
  complete state to establish equivalence returns `PrivateSharedConflict`,
  identifying table, row, and exact commit. Roll back everything together.

Check indirect foreign-key visibility changes too; roots alone miss the
SharedKey ancestor case. Use before/after gate classification and verified
incoming representations, not the final row's timestamp, to establish authority.
Resolution must be replay-admissible: an ordinary appended correction only
works if its observed dependencies place it before the blocked step. Otherwise
require an explicit authorized suffix resolution or return a dependency
conflict. Do not claim an appended edit repairs an earlier replay slot. Do not
automatically republish or discard private values, or keep a hidden second row
version. Expose this error distinctly from malformed history.

### One installation path

Remove direct incoming preflight application. Inside the owning transaction,
prepare the verified candidate as a replay input without advancing durable
positions, construct the projection including it, and install the projection,
retained input, positions, authority, and bindings together. Reuse the existing
projection table-copy boundary.

Publication completion uses the same path after establishing its exact accepted
association. Replace optimistic unaccepted state with accepted packages plus
Local, then reapply the retained suffix. Finalize receipt/transfer bookkeeping
in the same transaction. Derive live cleanup from the installed before/after
result, not intermediate scratch-projection deletions.

Keep requests explicit: current author state, foldable author prefix at a cut,
or audience image at a cut. Audience images apply accepted own packages too
and use existing audience pruning before export. A baseline may hold private
rows; omitting new journal inputs alone does not make it shareable. Preserve
Circle cutoff capture and bootstrap through the same rules.

## Baseline and acknowledgement

### Admit a prefix that future work cannot cross

The cursor supplies the exact foldable journal prefix. Every included observed
frontier is covered, every included publication is represented, and no
unaccepted write is crossed. Conversely, a covered own publication's Local part
must be included; otherwise decline the cut.

The shared cut must also remain a prefix of canonical application order after
retirement. A device's own acknowledgement does not prove this: another device
can still reveal a pre-cut concurrent commit. Folding private effects before
that discovery changes their placement during replay.

Use existing verified acknowledgement and registration history to admit a cut:

1. Verify the snapshot and identify **every currently active writer**, including
   devices activated after the snapshot. Require an activated acknowledgement
   whose `store_cut` covers the cut from each. Determine current writers using
   both device status and current principal membership; removed members' devices
   can remain marked Active. Validate each acknowledgement against its own
   registration and declared device state. Do not require it to name this older
   snapshot or use that snapshot's older device-state hash: a newly activated
   device cannot make that assertion. Existing snapshot-stability verification demands
   acknowledgements only from devices active at the snapshot and still active;
   that proof is weaker than this retirement requirement.
2. Materialize the complete predecessor/dependency closure of those
   acknowledgement activating commits. Reading their metadata is insufficient:
   pre-acknowledgement row commits must be present in local replay inputs.
3. Run the actual canonical shared scheduler over those inputs. Commits included
   in the cut must form an application prefix. An outside-cut commit preceding
   an included commit makes this cut inadmissible: retain inputs and use a later
   snapshot, without pretending that commit happened after the cut.
4. Check the journal prefix against that same run. Capture and adoption must
   agree on baseline, membership/device evidence, retained input identities,
   exact cut, write statuses, and payload hashes.

Future commits on each crossed writer's stream follow its acknowledgement
through the existing exact predecessor chain. Newly activated writers must
descend the crossed authority through verified activation/bootstrap history;
refuse retirement if that descent is not established. Excluded writers and
invalidated candidates remain subject to exact exclusion/retraction proofs.

This strengthens local admission without new wire fields or a new shared order.
The database retirement entry point accepts this verified admission evidence,
composed from existing snapshot/acknowledgement/registration types, rather than
an installable-snapshot authority alone. Keep it distinct from the existing
weaker acknowledged-snapshot proof; revalidate its exact inputs on adoption.
Preserve snapshot-required authority closure, Circle bootstrap/epoch evidence,
and exact historical device-state references during retirement.

### Publish acknowledgements independently of retirement

The current path advances the baseline *before* publishing its acknowledgement.
Waiting for all acknowledgements inside that call would deadlock. Separate them:

- Before acknowledging, verify the snapshot and that the device durably owns
  its baseline and the complete retained replay/payload/authority closure for
  the covered work and surviving local writes. Keep those owners pinned. This
  promises that cloud deletion cannot remove its reconstruction inputs; it
  does not require already removing its local journal.
- Publish through the existing acknowledgement chain.
- Advance and retire locally when all-writer closure and prefix checks hold.
  Missing acknowledgements and non-prefix cuts are typed declines, not sync
  failures or permission to discard inputs.

Do not stage another reconstructed baseline just to acknowledge. Existing
retained inputs remain valid durable state throughout. Another device cannot
see this device's local replay pins and may reclaim cloud history after the
acknowledgement. Before publishing it, ensure every required package, bootstrap,
authority payload, and locally retained blob input is available under durable
local ownership independently of that deletion. Metadata naming missing cloud
objects is insufficient. This device's own reclaim continues to honor its local
owners. Update acknowledgement/reclaim documentation and tests together so
neither path assumes acknowledgements remove local pins.

An offline active writer can delay history retirement. Surface this disk
retention consequence through existing decline/reclaim reporting. Existing
explicit exclusion can remove its authority; timeout-based deletion is excluded.

### Atomic consumption without another counter

The replay result carries an in-memory manifest of the exact prefix it consumed:
write identity, status/position, observed frontier, and input hashes. Adoption
revalidates it in the transaction, installs the image, transfers surviving
ownership, and consumes the prefix atomically.

Delete consumed LocalOnly inputs and reduce published/resolved inputs to their
existing receipts. Next replay loads remaining retained inputs in ordinal order.
No dependency points at a retired local ordinal, so another persisted counter
adds no information. A payload-free receipt needs baseline coverage or verified
resolution, never an empty-data fallback.

Failure preserves old image, journal, shared coverage, authority cache, and
payload claims. Never release an input before the adopting image commits.

Allow a new image at the same admitted shared cut when it consumes additional
eligible LocalOnly inputs. The exact consumed-prefix manifest establishes
progress even when those writes cancel each other and leave the image hash
unchanged. Do not require a different image hash or another counter. Decline only
when neither shared coverage nor consumed local inputs advance. Update the
coverage-only early return and cache invalidation accordingly; a stale retry
must not consume the prefix twice.

This permits retirement of eligible private inputs, not bounded retention for
every private workload. Writes observing acknowledgements beyond the admitted
cut need a later admissible snapshot, and a preceding unaccepted write still
blocks its suffix. Retain those inputs explicitly rather than folding across
missing history. This plan adds no separate private compaction protocol and
makes no disk bound while admission remains unavailable.

## Discard and retraction

Local order is conservative: a transaction may read earlier values and copy
them into unrelated rows. Row overlap cannot prove absence of a dependency.

Before removing a write or accepted dependency, inspect the surviving journal
suffix and observed frontiers. If private/unpublished dependents are outside
the explicit resolution operation, return a typed dependency conflict listing
WriteIds before changing anything. Receiving a protocol retraction does not
authorize silently discarding dependent private work.

An explicit discard can authorize an exact suffix including LocalOnly inputs.
Reverse original full host changesets in reverse ordinal order, then resolve
the authorized set atomically under existing remote-cleanup requirements.
Replace the query that silently excludes LocalOnly writes. If strict reversal
conflicts with intervening accepted work, preserve everything and report it;
do not force an inverse over newer state.

Successful resolution cannot leave pre-existing dependent writes outside its
resolved set. Newly captured writes observe the resulting valid frontier.
No resolution event stream or dependency counter is required. Terminal shared
retraction still needs its exact existing shared closure; apply the local
dependency check before removing any private effect or ownership.

## Implementation order

1. **Start with failing tests.** Port the five executed scenarios into real
   production-owner tests. Add SharedKey private/shared collision, unanchored-tail
   compaction, and pre-cut concurrent discovery. Confirm failures before changing
   production behavior; use sibling test files under the file-size policy.
2. **Capture the correct inputs.** Store one full observed frontier and derive
   publication dependencies. Complete Local row/routing/blob retention within
   the gate/capture owners. Test capture equivalence across supported transitions.
3. **Replace overlay replay.** Add the ordinal cursor to the existing scheduler,
   realize canonical packages plus Local atomically, and enforce the private/shared
   identity boundary. Remove `MergeReplayWriteOverlay` and the shared-then-local
   loaders when all callers use retained write inputs.
4. **Unify installation.** Route incoming and publication completion through the
   projection transaction. Separate historical realization from live transfer
   cleanup while preserving all control and binding checks.
5. **Admit retirement.** Separate acknowledgement publication from local
   retirement. Add all-current-writer/closure/prefix admission under existing
   history owners, validate exact journal consumption, and enforce resolution
   dependencies before discarding or retracting inputs.
6. **Verify and integrate.** Exercise the contract below, obtain independent
   review, remove obsolete fields/types/comments/fixtures, and run required
   hooks before committing and pushing the implementation.

These are implementation dependencies within one coherent repair, not deployed
half-states or compatibility modes. Tests use actual owning services, not a
reimplemented scheduler or gate resolver.

| Responsibility | Source |
| --- | --- |
| Signed order/frontier | [identifiers.rs](../crates/coven-protocol/src/store_commit/identifiers.rs) |
| Host journal/blob capture | [host_write_capture.rs](../crates/coven-database/src/store/store_session/host_write_capture.rs) |
| Stored writes/payload claims | [store_transaction.rs](../crates/coven-database/src/store/store_session/store_transaction.rs), [write_models.rs](../crates/coven-database/src/write_models.rs) |
| Publication dependencies | [preparation.rs](../crates/coven-replication/src/sync/store/commit_publication/operation/preparation.rs), [pending_publication.rs](../crates/coven-database/src/store/store_session/pending_publication.rs) |
| Author Local representation | [partitioning.rs](../crates/coven-database/src/gate/audience/partitioning.rs), [outbound.rs](../crates/coven-database/src/gate/outbound.rs), [routing.rs](../crates/coven-database/src/gate/audience/routing.rs) |
| Replay inputs/scheduling | [materialization_io.rs](../crates/coven-database/src/store/store_session/retained_merge_replay/materialization_io.rs), [cache.rs](../crates/coven-database/src/store/store_session/retained_merge_replay/cache.rs) |
| Row/routing/blob realization | [application.rs](../crates/coven-database/src/store/store_session/merge_materialization_transaction/application.rs), [replay_projection.rs](../crates/coven-database/src/store/store_session/replay_projection.rs) |
| Projection installation | [materialization.rs](../crates/coven-database/src/store/store_session/materialization.rs), [publication.rs](../crates/coven-database/src/store/store_session/publication.rs) |
| Snapshot/retirement | [snapshot_image.rs](../crates/coven-database/src/store/store_session/snapshot_image.rs), [baseline_advance.rs](../crates/coven-database/src/store/store_session/store_records/baseline_advance.rs) |
| Ack proofs/ordering | [snapshots.rs](../crates/coven-replication/src/sync/store/commit_verification/merge_history/snapshots.rs), [acknowledgements/mod.rs](../crates/coven-replication/src/sync/store/acknowledgements/mod.rs) |
| Resolution | [write_lifecycle.rs](../crates/coven-database/src/store/store_session/write_lifecycle.rs), [retraction.rs](../crates/coven-database/src/store/store_session/merge_materialization_transaction/retraction.rs) |

## Required validation

Compare production executions across delivery order, restart, and admitted
compaction schedules. Assert row values, locality, exact blob bindings and byte
availability, routing, foreign keys, receipts, and retained ownership. Counts
alone do not establish equivalence.

| Scenario | Required result |
| --- | --- |
| Local create/share/delete, then reuse the child's unique key | No resurrection or false uniqueness conflict, including restart and successive baseline advances. |
| Shared-first control | Same final shared result as local-first. |
| Share/local/edit/share with and without prior private history | Author retains recorded private values; recipients see accepted shared intervals. |
| Late peer update after withdrawal | No silent private deletion; canonical application or explicit atomic private/shared conflict according to the representation. |
| Host UPDATE versus generated INSERT under concurrent edits | Replay uses package conflict outcomes. |
| Private/shared SharedKey ancestor | Conflicting values hold atomically; equivalent values adopt accepted version/binding. No private value becomes shared authority. |
| Private/shared conflict, explicit correction/resolution, retry | Replay-admissible correction admits the same candidate once; a correction ordered after the conflict reports a dependency conflict. No failed-attempt positions, bindings, or cleanup persist. |
| Mixed roots, ancestors/assets, reparenting | Author retention and shared foreign-key closure survive. |
| Concurrent withdrawal and new shared sibling | A private parent image cannot satisfy a missing shared FK or overwrite shared values; hold atomically if the representations cannot coexist. |
| Store/Circle/Local moves, exclusion, and close | Exact controls, routes, winning bindings, and retained local bytes remain valid. |
| Private write observes history beyond the cut | It and its dependent suffix remain retained. |
| Private tail followed by publication after compaction | Placement/results match replay without compaction. |
| Pre-cut concurrent commit arrives before/after attempted retirement | Retirement waits for all-writer closure and prefix proof. |
| Post-snapshot writer activation, exclusion, pre-ack prepared write | Every authorized writer and exact predecessor chain is covered; no metadata-only shortcut. |
| Acknowledging while retirement declines | No deadlock; inputs remain pinned and sync continues. |
| Another device reclaims cloud history during that decline | Restart and replay use locally owned inputs and require no deleted cloud object. |
| Repeated baselines with unresolved suffix | Same projection, exact consumed prefix, no premature release. |
| Only private writes after an admitted shared cut | Same-cut compaction consumes eligible private inputs; restart and later publication match replay without compaction. |
| Eligible private create/delete or edit/revert leaves image unchanged | The nonempty manifest advances consumption atomically despite an unchanged image hash; stale retry does not reapply inputs. |
| Private writes observe acknowledgement commits beyond the admitted cut | Same-cut compaction retains those writes and their suffix until a covering cut is admissible. |
| Discard/retraction followed by private writes | Explicit dependency conflict or exact authorized suffix resolution; no dropped or revived effects. |
| Same-author restore without original journal | Canonical packages apply; identity does not imply substitution. |
| Audience image from private-bearing baseline | Only eligible data and bindings are exported. |
| Failures during realization, copy, adoption, ownership transfer | Complete before-state survives or complete after-state commits; retry is idempotent. |

Add reproducible generated sequences through real owners, varying delivery and
compaction. Failures print seed and operation sequence. Run focused database and
replication regressions, affected gate/replay/snapshot/blob/exclusion/ack suites,
then normal format, ownership, strict lint, and commit hooks. The design is
selected; failing acceptance tests require correcting it, not weakening assertions.

Use an independent background reviewer, without the code-rules-review skill.
Reuse configured build targets and avoid duplicate caches. Rebase implementation
onto current main, integrate with fast-forward-only history, and push after
checks. Publish Coven and repin bae after verification; neither operation is
part of writing this plan.

Already-corrupted baselines and journals missing observed history need explicit
recovery inputs. Do not infer causality from timestamps or add empty defaults.
Preserve working rows and unpublished data, validate recovery on a copy, and
keep live-library repair explicit. This investigation performed no live reset.
