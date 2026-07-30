# Mixed Store and Circle restore

## Status

Planned. Implements the `plans/circles.md` restore requirement: "Restore
stages Store and Circle images separately, verifies every byte and
authority chain, and keeps their coverage frontiers explicit. It installs
all selected images, replays the missing commits, runs audience filtering
and final foreign-key validation, then commits one final database and
Store frontier atomically."

Today restore installs exactly one Store image
(`bootstrap_from_snapshot` → `BootstrapResult` →
`VerifiedSnapshotBootstrapInstall`, `sync/store/snapshot/image.rs:932`,
`database.rs:436`); a restored device gets Circle control indexes from the
Store image but no Circle rows until/unless a fresh bootstrap arrives —
and Circle packages older than reclaimed history could never be
rematerialized at all.

Depends on `plans/circle-snapshots.md` (per-Circle images to stage) —
though the recipient's own member-addition bootstrap (named by its access
leaf) is an equally valid staged image and must work before standalone
snapshots exist in the wild.

## Design

### 1. Staged image set

Widen the restore staging from one image to a set with explicit per-image
coverage:

```rust
pub struct StagedRestore {
    pub store: VerifiedStoreSnapshot,            // exactly one
    pub circles: Vec<StagedCircleImage>,          // zero or more
}

pub struct StagedCircleImage {
    pub circle_id: CircleId,
    /// The verified image: a standalone Circle snapshot or the
    /// recipient's own bootstrap named by its access leaf — identical
    /// image format, verified by verify_circle_bootstrap_image either way.
    pub coverage: CircleBootstrapCoverageRef,
    pub image: VerifiedCircleImage,
}
```

Selection per Circle, after the Store image is selected and its Circle
control indexes are known: prefer the maximal stable standalone Circle
snapshot whose lineage the retained controls prove; else the identity's
own bootstrap named by its current access leaf; else stage nothing for
that Circle (its packages replay from live history if retained — and if
they are not retained, the Circle simply has no coverage to restore, which
selection reports explicitly rather than silently).

### 2. One installing transaction

Extend `VerifiedSnapshotBootstrapInstall` to carry the staged Circle
images. `open_database` installs, in its single existing transaction:

- the Store image (unchanged);
- for each staged Circle image: the image rows and routes, the
  `circle_bootstrap_coverage` row, and the blob graph — reusing the exact
  machinery recipient bootstrap installation runs during pull
  (`apply_circle_bootstrap_projection_on`,
  `install_circle_bootstrap_blob_graph_on`,
  `install_circle_bootstrap_remote_objects_on`, `pull/replay.rs`);
- migrations, routing-graph validation, final foreign-key component,
  frontier baseline — the existing steps, now over the union.

Failure anywhere rolls back everything: a partially installed image set is
never exposed as the current database (the existing single-transaction
guarantee, extended, not re-proven).

### 3. Replay past the cuts

Nothing new: incremental pull replays commits past the Store frontier, and
per-Circle bootstrap-seeded replay (already landed) skips exactly the
packages each Circle's coverage cut covers. The coverage rows written at
install are what make this work — restore and pull share one coverage
representation by construction.

### 3b. Scoped build design (from the foundation engineer's handoff)

The foundation (durable coverage-row image bytes; unified replay-input
reconstruction; digest binding at write and read) is committed on the
branch. The staging builds on it exactly as follows:

**Selection** (per Circle, after the Store image is on disk, opened
read-only): enumerate circles + controls from the preserved
`circle_control_activations`. RE-RESOLVE the restoring identity's access —
never the author's preserved caches — via
`load_circle_activations_with_prefix(identity = restorer)` against each
control's activating retained materialization
(`load_retained_merge_materialization_by_ref_on` → `as_verified`). No
active local access → `ClearCoverage(circle_id)`. Active access → the
maximal verified coverage the identity can decrypt and prove lineage for,
among three candidates: the preserved author-coverage row; the identity's
own leaf-named bootstrap (`leaf.disposition.bootstrap`); and the
standalone snapshot streams across
`activated_store_device_registration_records` (via
`load_circle_snapshot_stream`, stability via `circle_snapshot_is_stable`,
lineage via `verified_circle_control_covers_on`, maximal via
`select_maximal_circle_snapshot` — promote these from `cfg(test)`; this
is their first production consumer). A cut not covered by replayable
history from the Store frontier is a typed selection error.

**Install**: extend `VerifiedSnapshotBootstrapInstall` with
`Vec<StagedCircleDecision>` where
`StagedCircleDecision = Install(activation_commit, VerifiedCircleImage)
| ClearCoverage(CircleId)`. Inside `install_on`'s single transaction:
each `Install` runs `apply_circle_bootstrap_projection_on` +
`record_circle_bootstrap_coverage_on` (whose replacement validation
writes the image bytes, accepts newer coverage, refuses regression) +
`install_circle_bootstrap_blob_graph_on`; each `ClearCoverage` deletes
the preserved row — stage-nothing must leave no coverage row the
restoring identity cannot decrypt, or the replay bomb returns for the
removed-member case. The whole set commits or rolls back atomically.

**Wiring**: `bootstrap_from_snapshot` → a `StagedRestore` carrying the
decisions; `StagedRestore::open_database` installs the union; `join.rs`
threads it; `restore_from_cloud` passes through; restore codes untouched.

### 3c. As built (where the implementation diverged from 3b, and why)

The install, decision types, atomic-transaction, `ClearCoverage`, and
maximal-selection design landed as written. Three parts of 3b changed
against the reality of a restored device; each change is a correctness
requirement, not a shortcut.

**Access is re-resolved by the restoring identity's own leaf, not by
`load_circle_activations_with_prefix`.** A full activation re-verification
re-walks the control's covered-head lineage and resolves the control,
roster, and metadata author streams through the per-cycle
`registered_stream_activation` index — none of which a restored device
has. A Store snapshot does not preserve that index, and a reclaimed device
no longer retains the old-epoch control materializations the lineage walk
would need, so the walk cannot complete. The head control the restoring
identity actually needs is already verified inside the head commit's
retained materialization (`owned.circle_activations().circles()`); the only
identity-specific step is decrypting the identity's own access envelope.
The identity-specific block of `load_circle_activations` — find the
recipient's envelope, decrypt the leaf, verify its context — is extracted
as `resolve_identity_access_leaf`, and restore calls it through
`resolve_local_circle_access`, which reads only the identity's own access
envelope and the Store membership checkpoint. `load_circle_activations`
calls the same extracted helper, so the two paths cannot diverge. The gate
is unchanged: no decryptable active leaf → `ClearCoverage`.

**The activation commit reference resolves from the retained
materialization, not the materialized ledger.** `materialized_commits` is
empty on a restored device until the pull replays it, so
`circle_activation_commit_ref_on` now reads the commit reference from
`retained_merge_materializations` — which the snapshot preserves, and which
every caller opens next regardless. The head control is the retained-commit
control no other retained control's lineage covers; a control whose commit
the snapshot reclaimed is superseded and filtered out before the walk.

**The `circle_snapshot_is_stable` re-check is not run at restore.**
Stability gates a *reclamation* decision — whether a snapshot may retire the
history it covers — not restore safety. An unstable but verified snapshot
still installs correctly and the device replays forward from its cut; the
image is verified byte-exact against the retained control and routing key
either way. The acknowledgements stability reads are not preserved in a
Store snapshot, so the check cannot run against the restored image, and it
protects nothing restore relies on. A standalone snapshot's lineage
(`verified_circle_control_covers_on`) and its cut against the Store frontier
are still enforced.

**The leaf-named-bootstrap candidate is kept** (3b named it; it is not
redundant). A Circle the owner authored has live history and no coverage
row on the owner's device — coverage rows are written only when a device
*installs* a bootstrap or snapshot — so an owner-authored Store snapshot
preserves no coverage row for it. A non-owner member restoring from that
snapshot holds active access but cannot replay the Circle's pre-join
packages, so its own leaf-named bootstrap is the only baseline. The image
download and verification are the same `build_verified_leaf_bootstrap_image`
`load_circle_activations` uses.

**Staging is folded into `open_database`, not a separate `StagedRestore`
type.** The per-Circle decision `Vec` is the real shape; the wrapper type
was scaffolding. `open_database` selects against a throwaway copy installed
through the same borrowed install authority, then installs the Store image
and every decision in one `install_on` transaction. `join.rs` and
`restore_from_cloud` pass the restoring identity and storage through; no
decision vector crosses a public boundary.

**A Circle image is selectable only when its current control's activating
commit is retained.** A control activates in one Store commit and its
content lands in later commits; a Store snapshot keeps a materialization
only when it is a bootstrap-coverage activation, an author-exclusion, or
carries a package no coverage cut covers. So a live, never-reclaimed Circle
prunes its current control's activating commit from the image, the head
control cannot be resolved, and selection reports no coverage — the content
below the Store frontier is recoverable only through a coverage image, which
is exactly why bootstrap/snapshot retention exists. Reclamation (an epoch
close's successor bootstrap, or a standalone snapshot) is what makes the
current control's activating commit retained and a Circle image selectable.

**Standalone Circle snapshots are sealed under the Circle epoch key**, not
the Store routing key — their metadata stream and image both. Selection
reads them with the epoch key the restoring identity's active leaf carries
(`LocalCircleAccess::Active.epoch_encryption`), the same key the leaf
bootstrap is sealed under. Reading them with the Store routing key decrypts
nothing, so no standalone snapshot is ever selected. This was found and
fixed while building tests; the fix is confirmed by tracing the seal key,
but its end-to-end receipt — a restore that selects a standalone snapshot as
the maximal image — is not yet in place. Building it requires a reclamation
fixture whose standalone snapshot dominates the successor bootstrap, i.e.
covering post-close content written under the successor Circle-epoch key, and
that write path stack-overflows instead of failing loud (a separate
robustness bug). The receipt and that overflow fix are tracked follow-ups;
candidate2's image download and verification (the same
`verify_circle_bootstrap_image` and `build_verified_leaf_bootstrap_image`
candidate3 uses) are covered by the sabotage test.

**Sabotage threat model.** Restore is an attacker-facing input path: a
hostile storage provider serves the image bytes. The bytes are input to
`verify_circle_bootstrap_image`, never trusted for being served — their
digest must equal the image hash the signed access leaf or snapshot
reference pins. Of the three rejections, only a hash mismatch is reachable
by a storage provider: it is the only tamper that keeps the signed
reference intact, and any wrong-contract or foreign-audience-row image has
different bytes, so it fails the digest check before the contract or
audience-closure check. Those two defend against a malicious *author* — one
who could produce a validly-signed reference to a bad image — which
authoring already prevents; they are the verifier's own concern, exercised
by its own tests. The restore path's job is to run the verifier on every
storage-hosted candidate and fail the whole restore with no database exposed
when it rejects. The staged database-image owner retains the exact image and
its SQLite sidecars until installation commits; any rejection finishes through
that owner and reports cleanup failure instead of exposing the database.

**Resolved by moving membership-head deserialization off the async poll stack.**
`post_close_circle_store_snapshot_restores_and_converges` stack-overflowed
(SIGABRT) at the default 2 MB thread stack. The earlier note here misattributed
it to founder-graph loading and a `PreparedExactObject` whose `stored_bytes:
Vec<u8>` is itself serialized JSON; direct backtrace and frame measurement
disproved that. The founder graph is structurally flat (JSON depth 5–11) and its
load was never on the failing stack. The real cause was stack-frame exhaustion,
not depth-proportional recursion: the whole stack was ~60 frames spanning ~2 MB,
dominated by large `async fn` state-machine frames along `add_circle_member` →
`prepare_circle_operation_request` → `load_and_persist_owner_anchor` → the
`exact_chain` membership loaders, terminating in a heavy monomorphized serde
deserialize of `AuthorHead`. It passed under a 1 GB stack because the depth is
fixed and finite; `main`'s added authoring frames were only what crossed the
2 MB boundary. Two leaves in `exact_chain.rs` ran `serde_json::from_slice::<
AuthorHead>` inline on the async poll stack, unlike the entry/head-by-reference
loaders, which already run their parse on a fresh blocking-pool stack through
`load_exact_object`. Routing those two head parses through the same
`run_blocking_object_verification` path keeps the deep async descent and the
heavy deserialization off one stack. Fixed on this branch as its own commit
(subject: the stack budget, not restore); the test flips from SIGABRT to pass at
the default stack, with no stack enlargement and no test dropped. The 5-layer
`exact_chain` async loader collapse remains a separate follow-up. The epoch-key
snapshot-authoring write-path overflow noted above was not separately
reproduced; it is most likely the same frame-exhaustion class at a different
serde leaf, not a distinct defect.

### 4. App-level wiring

`crates/coven/src/sync/restore.rs` (`restore_from_cloud`) passes through
the staged set; restore codes and device-continuation state are untouched
(they name the Store snapshot; Circle selection derives from the restored
control indexes, not from the restore code).

## Correctness cases

- **Circle image older than the Store image**: fine — the Circle cut is
  explicit and per-Circle replay fills forward from it.
- **Circle image newer than the Store image's frontier**: impossible to
  install coherently; selection rejects a Circle image whose cut is not
  covered by replayable history from the Store frontier — typed selection
  error, not a silent skip.
- **No access to a Circle at restore time** (removed member): no leaf, no
  image staged, control indexes still restore; the Circle correctly
  materializes nothing.
- **Crash mid-install**: single transaction; either the prior (empty)
  database or the complete set.

## Tests

1. Two-member Store with one Circle, history reclaimed up to a Circle
   snapshot: fresh device restores Store + Circle images and converges
   with live devices (rows, routes, blobs, coverage row all present).
2. Restore with no Circle image available → Store restores; Circle rows
   arrive via ordinary replay when retained; selection reports the
   no-coverage Circle.
3. Sabotaged Circle image (hash mismatch, wrong contract, foreign row
   outside the audience closure) → whole restore fails, no database
   exposed.
4. Removed member restores → no Circle materialization, control indexes
   present.
5. Crash injected between Store-image install and Circle-image install
   inside the transaction → rollback leaves no partial database.

## Out of scope

- Reclamation eligibility (next plan) — this plan is why bootstrap/snapshot
  retention rules exist.
- Multi-Store restore (out of product scope entirely).
