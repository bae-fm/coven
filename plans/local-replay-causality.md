# Preserve local write causality through replay

## Status and objective

Implementation plan, verified against Coven `853d58f7`.

Rebuilding a device's database must preserve the relationships between its
local writes, accepted shared commits, and changes in who can see a row.
Advancing the saved replay database must preserve the same result and retain
every input needed by work beyond its boundary. A release-specific deletion
exception is not the repair.

This plan changes Coven's local capture, replay, and retirement model. It reuses
the Store commit protocol's exact dependency references and conflict rules.
Private writes remain private; peers do not acquire dependencies on local
journal entries they cannot download.

## Verified source behavior

The preceding device-state change does not change host capture, write insertion,
outgoing partitioning, the replay scheduler, or settled-write selection. Those
files are byte-identical to `fab6b47e`. The new shared `store_device_states`
representation and exact per-commit references must survive this work.

| Owner | Verified behavior |
| --- | --- |
| [Store commit order](../crates/coven-protocol/src/store_commit/identifiers.rs) | `StoreCommitOrder` has a sequence, exact same-stream predecessor, and exact cross-stream dependencies. `CommitFrontier` represents covered shared history. |
| [Host capture](../crates/coven-database/src/store/store_session/host_write_capture.rs) | One transaction captures the original application changeset and audience partitions. It computes `StoreWriteBase.dependencies` from the materialized frontier, excluding the current author's announcement stream. |
| [Write insertion](../crates/coven-database/src/store/store_session/store_transaction.rs) | Every retained write gets an auto-incremented journal ordinal and `WriteId`. Only publishable writes retain `base`; local-only writes deliberately store `NULL`. The complete original changeset and outgoing partitions are separate payloads. |
| [Publication preparation](../crates/coven-replication/src/sync/store/commit_publication/operation/preparation.rs) | Publication uses the captured cross-stream dependencies and obtains the author's previous shared commit when preparing publication. Capture and publication are different events. The signed commit carries the original `WriteId`. |
| [Publication completion](../crates/coven-database/src/store/store_session/publication.rs) | The author's live completion activates accepted objects and installs bindings for currently winning rows without executing the published package's SQL again. This existing separation is a primitive for replay to reuse. |
| [Audience partitioning](../crates/coven-database/src/gate/audience/partitioning.rs) and [outgoing gates](../crates/coven-database/src/gate/outbound.rs) | Sharing can synthesize complete INSERTs; making a root local can synthesize DELETEs for peers. These payloads describe the recipient's database, not necessarily the author's original edit. |
| [Replay input loading](../crates/coven-database/src/store/store_session/retained_merge_replay/materialization_io.rs) | Local replay inputs retain partition bytes and write IDs, but omit observed shared history. Journal order comes from `ORDER BY ordinal`. Published writes contribute only their remaining local partition because shared packages are replayed separately. |
| [Replay scheduling](../crates/coven-database/src/store/store_session/retained_merge_replay/cache.rs) | Accepted shared commits are replayed after their predecessors and dependencies. Only after all of them are applied does a separate loop apply journal overlays. |
| [Projection application](../crates/coven-database/src/store/store_session/replay_projection.rs) | An overlay applies Store, Circle, and local partitions inside one transaction, using existing changeset conflict handling. It does not consume the original full host changeset. |
| [Incoming materialization](../crates/coven-database/src/store/store_session/materialization.rs) | Concurrent discovery and certain control changes can rebuild and replace the live projection using the same replay path. The defect is not confined to snapshot creation. |
| [Snapshot capture](../crates/coven-database/src/store/store_session/snapshot_image.rs) and [baseline retirement](../crates/coven-database/src/store/store_session/store_records/baseline_advance.rs) | Internal capture replays to a shared cut and includes a settled journal prefix. Local-only writes are considered settled without checking observed dependencies. Retirement releases their payloads and removes or reduces the journal rows in the transaction that installs the new image. |

The confirmed regression is a local INSERT whose row later becomes shared and
is deleted by shared history. Appending the old local INSERT after that history
resurrects the row. The earlier production-code reproduction demonstrated a
deleted child absent from the live database and shared replay but present after
internal baseline advancement. It was run against `fab6b47e`; it has not been
rerun against `853d58f7`. The relevant algorithms are unchanged.

Two additional constraints follow from the code and require regression tests:

- Sorting the existing partitions cannot, by itself, reproduce every original
  local transaction. An outgoing make-local DELETE means removal on a peer,
  while the author retained the row with a changed locality flag.
- A shared snapshot cut is not enough to decide whether a local-only write can
  be absorbed. That write may have observed shared commits beyond the cut.

## Required invariants

1. Every captured host write retains its original position among host writes
   and the exact shared history visible in its transaction, including the
   author's own accepted stream position.
2. Capture order, shared publication order, and delivery order remain distinct.
   A later publication cannot rewrite what an earlier host transaction observed.
3. Shared commit readiness and authority continue to come from the existing
   signed predecessor/dependency graph. Local metadata cannot authorize a commit,
   fabricate a shared dependency, or satisfy a peer's missing input.
4. A local host transaction has one application effect on its author. Publishing
   its recipient representation does not execute that effect a second time.
5. Audience transitions preserve the appropriate result on each device: local
   retention on the author, removal or introduction on recipients, correct
   routing, and exact blob bindings. A routing change is not an application
   deletion and must not be confused with one.
6. Concurrent shared edits retain the protocol's defined conflict outcomes.
   Adding unrelated private writes cannot change the canonical result for shared
   rows. No timestamp sort substitutes for causal dependencies.
7. Replay, direct incoming application, restart, snapshot capture, and replay
   from an advanced baseline agree for equivalent retained history and local
   writes. Compare actual row values, locality, routing, and blob bindings, not
   only row counts or shared frontier equality.
8. Installation and retirement commit together. A failed projection, missing
   causal predecessor, invalid binding, or constraint failure leaves the old
   database, baseline, payload claims, and write receipts intact.

## Durable information

Separate the local replay context from the publication base; they answer
different questions. Do not expand the publication dependency map with the
author's capture-time predecessor and then reuse it unchanged for signing.
Publication currently supplies its own predecessor at preparation time.

Retain for each host write:

- The existing `WriteId` and journal `ordinal`, representing identity and local
  write order. Derive adjacent local-write relationships from this order; do not
  introduce a competing local sequence or timestamps for ordering.
- A full observed `CommitFrontier`, captured in the same SQLite transaction as
  the application effect. Retain it for local-only as well as publishable writes.
  Reuse exact commit references; an empty frontier is legitimate only when no
  shared history has been observed, never as a missing-data default.
- The original captured changeset, already stored by content hash, as the source
  of the author's application effect. Preserve any additional routing/transition
  facts that the original bytes demonstrably do not encode; establish this with
  the transition tests before adding fields.
- Existing publication status and its exact accepted commit reference. Link the
  original effect and shared activation through `WriteId`; validate that the
  linkage names the same write rather than inferring it from a clock or row ID.
- Existing captured blob facts and ownership required to realize those rows.
  Local replay retention must cover these facts for as long as its effect is
  retained, independently of whether an upload is still pending.

The saved replay database must describe both the shared frontier it includes
and the local journal prefix it includes. Persist the covered local position
with the baseline so removing journal rows does not remove the meaning of the
boundary. A downloaded cloud snapshot supplies shared coverage and no history
of the joining device's private writes; the founder begins with neither.

Update Coven's internal schema and all producers/consumers to one shape, as
required by its greenfield policy. Do not synthesize absent observed history for
old journals or silently treat it as empty. Application schema migration support
and Store commit wire compatibility are separate concerns.

## Replay design

### One dependency model, distinct events

Extend the database owner's replay planning to understand accepted Store commits
and captured host writes. Keep capture and publication as distinct events: the
shared commit may not exist when the host write commits.

The planner must express and check these relationships:

- An accepted commit follows its exact predecessor and shared dependencies,
  using the existing checks and canonical ordering of concurrent shared inputs.
- A host write follows its recorded observed shared frontier and preceding
  retained host writes, with a covered baseline position satisfying retired
  predecessors.
- When a projection substitutes the originating host effect for publication
  SQL, that effect must precede its exact accepted publication: add the explicit
  host-write-to-publication dependency, or prove the effect is already included
  in the baseline's local coverage. Association by itself does not impose order.
  A peer only has the accepted publication.
- Shared dependencies beyond a requested historical cut prevent the dependent
  host write from being folded into that cut. Missing or conflicting exact
  references cause an explicit failure, not an arbitrary scheduling choice.

The local replay graph must not introduce a second public ordering protocol or
leak private journal positions into public dependencies. Erasing private events
must leave the accepted shared graph and its conflict semantics unchanged.

### Apply the right representation exactly once

Replace partition overlays as the authority for reproducing a host transaction.
Use the original host effect for this device and the verified recipient
representation for received shared writes. Keep package verification, membership
and device controls, activation records, routing, blob ownership, and winning
blob bindings under their existing transaction owners.

Reuse the separation in `complete_prepared_store_write`: original SQL has
already run, while accepted objects and winning blob bindings still need
activation. Factor the shared application/activation capability under the
materialization owner instead of copying publication's implementation. Replay
must not repeat live upload handoff consumption or filesystem cleanup merely
because it is reconstructing historical activation state.

Do not implement this as “skip packages whose author is this device.” A package
can contain synthesized ancestors, existing shared rows re-emitted for a new
recipient, or blob activations. After restore, a commit authored by the same
identity may have no local journal effect to substitute. Dispatch on a verified,
retained write-to-commit association and the replay boundary, not author identity.

Shared-only projections, including the existing `ReplayWriteOverlays::Omit`
callers, must continue to apply accepted audience packages for own commits too:
they contain no private host events to substitute. Make the requested projection
explicit in the replacement model. A host effect can replace published SQL only
when that exact effect is represented in that projection or its baseline.

Before replacing the scheduler, establish the application contract for an
associated host effect and publication. The contract must explain which row
operations are applied once and which accepted protocol/blob effects still run.
Exercise delayed publication, intervening incoming changes, and re-emission of
other shared rows. If raw host changeset substitution cannot preserve shared
conflict outcomes, introduce the required explicit association/representation
inside the materialization owner; do not suppress the discrepancy or claim that
topological ordering alone solves it.

This is the principal design proof still required. The source establishes the
missing information and incorrect overlay order; it does not establish that
arbitrarily interleaving raw changesets is a correct replacement algorithm.

Both normal replay and historical capture must consume the resulting planner
and application contract. Direct incoming application must honor the same local
context: checking only the shared frontier cannot establish that no relevant
local writes are present. Preserve a direct path only when its precondition
proves equivalence to replay.

### Historical cuts, retirement, and resolution

Choose the local prefix together with the shared cut. Inclusion must be closed
over the host writes' observed shared references, required earlier local effects,
and publication relationships. A prefix that cannot be represented at that cut
must remain retained; it must not be absorbed by pretending it is independent.

Handle the inverse obligation too: a shared commit included in the cut may
require local history for the author's representation. Retain enough information
to reproduce the author result or decline that baseline advancement. Never
retire a shared/local relationship needed by the unfurled suffix.

Resolved or retracted writes must not regain application effects. Integrate
their causal dependents with the existing explicit resolution policy: remove
only effects with verified resolution authority, or fail with a diagnostic
identifying the unresolved dependent writes. Do not silently drop a dependent
local-only edit because it has no publication receipt.

Save the image, both coverage boundaries, and retained dependency/authority/blob
closure atomically before releasing the replaced history's ownership. Preserve
the exact device-state references introduced by `853d58f7` and the existing
Circle bootstrap, epoch-cutoff, and author-exclusion evidence.

## Implementation sequence

1. Add failing tests through real host capture, publication, incoming
   materialization, and baseline advancement. Start with the confirmed
   local-create/share/delete regression, then sharing transitions and observed
   remote history. Establish failures before changing production behavior.
2. Add full local observed-history retention and baseline local coverage.
   Update capture, schema, stored models, payload ownership, and fixture
   construction together. Verify that publication still uses the intended
   captured cross-stream base and preparation-time predecessor.
3. Establish and test the once-only application contract for original host
   effects and accepted recipient representations. Resolve the design proof
   above before relying on it for scheduling or retirement.
4. Replace the shared-then-local loops with dependency-aware replay planning
   under the existing database owner. Thread verified context through replay
   loading rather than consulting the current live rows to guess past order.
   Update direct incoming application, live rebuilds, and historical projections.
5. Replace settled-status-only folding with explicit causal coverage. Update
   snapshot installation/advancement, restart, discard/retraction, and payload
   retirement together. Keep failed attempts atomic and retryable.
6. Delete superseded overlay types/loaders and assumptions after every caller
   uses the replacement. Update the Store protocol documentation's host capture
   and replay descriptions. Verify all old references, tests, and fixtures.

These are implementation dependencies, not independently deployable protocol
modes. The final change must satisfy the whole contract before integration.

## Acceptance tests

Tests call production owners; fixtures may supply storage, clocks, identities,
and application schemas. Do not recreate replay or partitioning in the test.

| Scenario | Required observation |
| --- | --- |
| Local creation, sharing, deletion, baseline advancement, then a new row using the deleted row's unique key | No resurrection and no false uniqueness conflict, before and after restart. |
| Creation while already shared, followed by the same deletion | Same shared result as the local-first path. |
| Share, make local, edit, share again; repeat across baselines | Author retains the correct local values when private; eligible peers see exactly the shared intervals and values. |
| One transaction edits local and shared roots, their common ancestor, or reparents a child | Complete transaction effects and foreign-key closure survive every projection. |
| A local-only write observes a remote commit beyond an older acknowledged snapshot cut | The write is not folded into a baseline omitting its dependency. Its inputs remain available. |
| Pending capture, incoming commit or local control publication, later host edit, then publication | Capture order and publication order remain distinct; exact dependencies and final conflict outcomes are preserved. |
| Other shared rows re-emitted during a sharing transition | Author and peers agree on shared values; skipping duplicate original effects does not skip required recipient state. |
| Shared-only historical capture of an own commit, compared with author-preserving replay | The shared-only image applies the accepted audience package; the author image applies the original effect once and still installs valid accepted bindings. |
| Concurrent update/delete and different-column/same-column edits on two devices, varied delivery orders | Existing conflict policy and shared convergence hold. Unrelated private edits do not change the outcome. |
| Delayed remote edit from an earlier shared interval while the author has made the item private | Define and test the locality/routing outcome through the existing audience authority; private rows and blobs are not accidentally removed or republished by replay. |
| Blob-bearing local/shared/Circle transitions | Row versions, exact locators, byte availability, visibility, and payload claims agree with the chosen row representation. |
| Discard, rejected publication, verified retraction, Circle close/exclusion | Removed effects do not reappear; surviving dependents retain valid authority or fail explicitly. |
| Consecutive baseline advances, including one with unresolved suffix writes | Replay from each baseline equals replay of the same complete retained history; no needed dependency or blob is retired. |
| Join/restore, including the same author identity without its old local journal | Downloaded accepted history applies correctly without inventing local effects. |
| Injected failure before image adoption, during projection replacement, or during retirement | Database, coverage, journal, and ownership remain wholly before or wholly after the operation. |
| Missing or contradictory local causal context | An actionable failure names the write/reference; no empty default, arbitrary reorder, or silent skip. |

Include generated operation sequences across multiple devices and snapshot
boundaries. Compare production executions with different delivery/compaction
schedules; use explicit expected states for the targeted regressions. Check
foreign keys and exact relevant values as well as counts. Random seeds must be
reproducible, and a failure must print the operation sequence through the test
failure output.

## Verification and integration

Run the new regressions against the original implementation and record their
failures. After implementation, run focused database and replication tests,
formatting, strict lint, the required repository hooks, and the affected existing
replay, gate, snapshot, blob, and exclusion suites. Obtain an independent review
of causal coverage and author-versus-recipient application. Do not use the
code-rules-review skill.

Reuse configured build caches; do not override `CARGO_TARGET_DIR` or
`RUSTC_WRAPPER`, create duplicate build directories, or retain test databases
beyond diagnostic need. Rebase implementation work onto current main, integrate
with fast-forward-only history, and push after the completed change passes its
checks. Publish Coven and repin bae only after the generic repair is verified.

An already-corrupted baseline requires separate, evidence-based recovery. Missing
local dependency history cannot be reconstructed from timestamps. Preserve the
working database and unpublished writes before replacing any saved replay state;
validate recovery inputs and resulting rows on a copy first. This plan does not
authorize mutating a user's live library, resetting its data, or silently
accepting older journals with invented causal context.
