# Verified-reuse consolidation

A survey of every mechanism that avoids re-verifying or re-reading already
verified replication artifacts found twenty of them in two lifetime tiers:
connection-lifetime state on `DatabaseCore.verified_store_authority`
(works across cycles) and per-cycle state on `MergeHistoryVerifier` /
`StoreCommitVerifier`, rebuilt empty by every `authorize*()` call. Every
"Reuse verified X" commit (30962624, ac56df5f, 4c6680d3, 7cffcace, 424cce46,
1a5a980f, a4ce015d) landed in the per-cycle tier; their tests all assert
`..._within_a_cycle` and nothing asserts a second cycle reads less than the
first. That gap is why pull cost keeps returning.

## Landed

- **Pull's retained history is served from its durable rows.** Measured with a
  read-counting fixture: one provider read per retained commit and one per its
  activation head, on every cycle, unchanged on the second and third cycle —
  the per-cycle memos deduplicated perfectly within a cycle and carried nothing
  across one. A pull over twenty-four retained commits made 75 provider reads;
  it now makes 27, the same count as one retained commit, and none of them
  touch a retained commit or head.

  What kept the two tiers apart was the order `prepare_retained_history` asked
  in: it verified every retained ref from the cloud first and passed the
  resulting proofs down to the database, so the durable authority depended on a
  fresh remote verification rather than being it. Reading the durable rows
  first and seeding `StoreCommitVerifier`'s `commits`, `verified_heads`, and
  `accepted_announcements` from them inverts that. Seeding the accepted
  announcement path also ends the head-slot re-walk, which is why the remaining
  count is flat rather than merely smaller.

  `RetainedCommitAuthorities::Operation` was the API that demanded those proofs
  and is deleted, along with its `replay_inputs_with_verified_commits_on` and
  `retained_merge_replay_inputs_with_verified_commits` entry points and
  `MergeHistoryVerifier::retained_commit_proofs`. `RetainedCommitAuthority`
  (singular) stays: the write path really has just verified the commit it
  retains.

  Divergence from the direction above, deliberate: `verify_refs` still runs
  over the retained refs, now entirely out of the seeded memos. Removing that
  call as well would mean rebuilding each retained commit's
  `predecessor_membership`, `predecessor_state`, `state_after`, `registrations`,
  `operations`, `acknowledgement`, and `membership_control` from checkpoints —
  and the candidate path's own `verify_refs` walks into retained predecessors,
  so they must be in `history.commits` either way. The remaining cost is CPU
  only: re-parsing and re-checking signatures over local bytes, a few
  milliseconds for twenty-four commits against the 36 s of provider latency
  that is gone. Worth doing when the per-commit CPU starts to show; not worth
  the divergence risk to do blind.

  New test class, the one the family lacked: `repeated_pulls_over_unchanged_
  retained_history_read_none_of_it` and `retained_history_depth_does_not_
  change_what_a_pull_reads` assert across cycles, not within one.
  `a_pull_over_retained_history_still_probes_the_next_announcement_slot` guards
  the failure mode seeding could introduce — a device that silently stops
  discovering what its peers publish.

  No home left the verification-artifact allowlist: `commit_verification/`
  still fetches, because commits this device has *not* verified still come from
  the provider. That is the boundary working as intended — it separates the
  first verification from the repeat, and only the repeat went away.

- **The two acknowledgement caches are one, and retained acks come from the
  durable row.** `MergeHistoryVerifier.verified_acknowledgements` is deleted;
  `StoreCommitVerifier.acknowledgements` is now keyed by the exact object, which
  is what both ways of asking have in common — by reference, how a commit names
  the ack it activates, and by predecessor object, how a chain walk steps back.
  `load_store_ack_predecessor` reads and writes it, so a walk over a prefix
  another walk covered now stops at the first ack already held. `load_store_ack`
  returns `StoreAck` rather than a `VerifiedObject` whose bytes only one caller
  wanted; that caller re-encodes canonically, as the registration check beside it
  already did.

  This was the live gap, and it is why the previous fix measured clean in a
  fixture and cost 119-192 s per cycle in the field. A lone device publishes an
  acknowledgement only occasionally, so almost none of its retained commits
  activate one; with two devices nearly every commit does, and each named a
  distinct ack the pull re-read from the provider every cycle. The retained row
  already carries the whole verified chain in
  `history_evidence.acknowledgement`, so `admit_retained_history` seeds it.

- **Snapshot metadata has a memo at all now.** `load_store_snapshot` had none, and
  every commit activating an acknowledgement that names a snapshot re-read it:
  measured 36 reads of 4 distinct objects on a six-round two-device history, 20
  on a two-round one. Flat at 10 after, both depths.

- **The test class that catches this shape, not just this instance.**
  `acknowledged_history_depth_does_not_change_what_a_settled_cycle_reads`
  asserts a settled cycle's total provider reads are identical at two and six
  rounds, naming no object kind. The kind-specific assertion beside it passed
  while the snapshot cost was still there; the depth one caught it. Prefer it
  when adding coverage here — it is the assertion that does not need the bug to
  be imagined first.

- **The founder registration is held once.** It lives in
  `StoreCommitVerifier.registrations` with every other registration. The two
  things that used to be copies of it are now names for it: the `OnceLock`
  remembers which reference is the founder — needed because the founder is
  reached by slot from the root descriptor, so it cannot be asked for by
  reference until it has been read once — and `MergeHistoryVerifier` keeps that
  reference as the identity it validated against its root at construction.

  Most of what asked for the founder only ever wanted its reference, which is
  why three copies went unnoticed: the snapshot authority and the device-join
  bootstrap's `founder_reference` both rebuilt the reference from a cloned
  object. One caller does want the object — the device-join bootstrap carries
  the registration itself — and now loads it, which made one private helper
  async.

- **Registrations are not represented four ways — checked and closed, no code
  change.** The four names turn out to be one store, one isolation copy, one
  borrow, and one different artifact:

  - `VerifiedStoreAuthority.registrations` is the durable-side store: registrations
    verified from stored rows, for this connection's life.
  - `VerifiedStoreAuthorityTransaction` copies it to stage verification beside a
    SQL transaction. That copy is load-bearing, not sprawl: a registration read
    during a transaction that then rolls back must not stay in the connection's
    cache, or the cache would answer for a row that no longer exists.
  - `CachedVerifiedRegistrations` holds no registrations at all. It is a borrow
    adapter, so `replay_inputs_on` can take `&mut self.retained_replay` and
    `&mut self.registrations` in one call.
  - `StoreCommitVerifier.registrations` is the other artifact class: read from the
    provider, keyed with its bytes, per cycle. Same name, different thing.

  Both halves were measured rather than argued. Registrations number one per
  device — two on the fixture — so the per-transaction copy of that map is
  nothing. The `RetainedReplayCache` cloned beside it is the part that grows with
  history, and it costs about 2.4 µs per retained entry per transaction (48 µs
  for twenty, debug build), roughly 420 µs per transaction at the field's 174
  rows. Against the tens of seconds this plan is about, that is not a cost worth
  a redesign of the transaction authority — which is what removing the copy would
  take, since a delta would have to borrow the connection maps and the
  transaction is moved out of its SQL closure on commit.

  Provider-side registration reads are flat: two per settled cycle on a
  two-device store at every history depth, already pinned by
  `retained_history_depth_does_not_repeat_exact_registration_reads`.

- **The baseline read-path bypass is closed** —
  `load_retained_merge_history_checkpoint_on` now takes the baseline its caller
  already holds, and the caller reads it once from
  `VerifiedStoreAuthority::retained_replay_baseline_on`, which is the memo. Same
  move a00088f8 made on the install path, now on the read path.

  The second site the entry named (`:640-645`) turned out not to be a bypass: it
  is `generation_zero_replay_baseline_on`, the loader `RetainedReplayCache::
  baseline_on` calls to fill the memo, so it is the one place that should load.
  Its public wrapper on `StoreDatabase` has only test callers.

  Measured before changing anything: loading a baseline costs 8.5-14 ms, all of
  it deserializing the image into a fresh in-memory connection and revalidating
  it — so the per-call cost the entry claimed is real. The site is cold, though:
  one hit across the whole 672-test replication suite, because it only fires for
  a reference below a snapshot cut. Fixed anyway, since it is fifteen
  milliseconds a call on the one path that gets busier as snapshots accumulate,
  and the fix is a parameter rather than a redesign.

  Thin coverage, stated plainly: the suite exercises this path once, so the
  change rests on it being the same baseline from the same loader, validated
  once per connection instead of once per call.

- **`owner_anchor` is not a verification memo — checked and closed, no code
  change.** It guards installation, not verification: `reuses_owner_anchor` tells
  `install_store_owner_anchor` the anchor is already installed so it can skip
  writing it again. Skipping the SQL write is the whole job, so "saves only the
  SQL write" describes the field working, not falling short. The audit swept it
  up because the name reads like a reuse cache.

  The observation underneath it is real and separately measured:
  `validated_store_owner` re-derives the founder genesis on every call without
  consulting the anchor — 35 calls against 2 short-circuits on the two-device
  fixture, roughly five per cycle. It costs 12 µs, flat across every call,
  because what it re-derives is one founder record and a small genesis row: a
  single SQL read, one JSON parse, and a state derivation, none of which grow
  with history. Not worth a short-circuit.

- **The boundary covers slot reads and the progress alias.** `read_protocol_slot`
  is the call the announcement walk makes per head — half the measured
  retained-history cost, and outside the gate the entire time it was being paid.
  Six new homes, exactly the ones the entry predicted.

  `read_protocol_object_with_progress` went in with it at no cost: its one caller
  was already a home, and it is the same read with a callback, so gating the
  plain name alone left an alias that anything could reach for. Method patterns
  match exactly, so every spelling has to be named.

- **The snapshot stream walk resumes instead of restarting.**
  `load_store_snapshot_stream` had no memo: it walked from generation zero to the
  first absent slot on every call, and a settled cycle walks each device's stream
  four times — from publication, from history loading, and from reclaim. It now
  resumes from the prefix this verifier has already walked and probes on from its
  end, so a generation published since the last walk is still found. Same shape
  as the accepted announcement prefix, for the same reason.

  Measured on the two-device fixture: ten snapshot reads a settled cycle before,
  eight after. Stated plainly, that is the whole demonstrated gain — the fixture
  holds four snapshot generations whether it runs four rounds or twelve, so it
  cannot show the part that actually matters, which is that a walk was
  O(generations) per call and is now O(new generations). That claim rests on the
  shape of the loop, not on a measurement.

  What remains is a constant this fixture cannot reduce: a device with no
  snapshots must have its first slot probed on every walk, since an empty stream
  is indistinguishable from one whose first generation just landed.

- **The one funnel for content-addressed reads now has a memo.**
  `load_exact_object` is where every verified protocol object is fetched by
  reference — membership heads and entries, acknowledgements, snapshots,
  commits, registrations, packages — and it had no reuse at all, so a cycle paid
  a provider round trip per ask. It now remembers bytes keyed by
  `ExactObjectRef`. Bytes, not verdicts: a hit still runs the caller's
  verification, and the reference carries the semantic hash that verification
  checks, so an answer from the memo is the answer a read would have produced.

  Measured on the two-device fixture, settled cycle: 49 provider reads before,
  39 after, at every depth from 18 to 282 retained rows. Membership, the largest
  block, went 25 to 15.

- **The retained-history residual does not scale with depth — the fixture is
  decisive.** A settled two-device cycle reads 45 objects at depth 18, 45 at
  depth 122, and 49 at depth 282 (the extra four are two more announcement heads
  and two more snapshot generations, not per-row). The field's 6.6-15.9 s on a
  quiet store of ~320 rows is those constant reads meeting GCS latency: 49 round
  trips at 135-325 ms each spans exactly that range. Nothing round-trips per
  retained row any more.

  Local work does still grow with depth — the settled cycle takes 17.6 ms at
  depth 18, 99.8 ms at 122, and 506 ms at 282 — but that is a fraction of a
  second at field scale against seconds of latency. The retained replay cache
  serves every row (123 rows, 123 hits, 0 loads, about 2 ms a call, four calls a
  cycle), so the growth is the walking, not re-verification.

## In flight
- `publish pending writes` measured 25.9 s for one 40-blob release: the
  `prepared_remote_objects` loop writes each object serially. Stage timings
  being added inside the publish path; likely fix is the bounded fan-out the
  upload drain already uses.

## Recorded, not yet scheduled

- **Membership objects are still read three times over.** After the
  `load_exact_object` memo a settled cycle makes 15 membership reads covering
  only 5 distinct objects, at every depth. The memo cannot catch them: those
  reads come in through `read_protocol_slot` (`commit/membership.rs:226`), which
  addresses a slot rather than an exact object, and a slot's contents are not
  immutable — one can be empty now and filled later — so remembering a slot read
  is a cache that can lie. The announcement walk solved the same problem with a
  verified-prefix memo plus a live probe past its end; membership wants that
  shape, not a slot cache. Worth roughly ten of the thirty-nine reads a field
  cycle makes.

- **`read_prepared_protocol_slot` is the one protocol read still ungated.** It
  is the fourth read on the storage trait and reads an artifact from the provider
  like the three now gated, keeping the stored representation for a durable retry
  journal. Its callers lean toward write-confirmation rather than verification —
  reading back a slot the caller just prepared — which is why it was left for a
  decision rather than folded in. Adding it needs six homes beyond the current
  thirty-one: `acknowledgements/mod.rs`,
  `authorization/history/construction.rs`, `authorization/registration.rs`,
  `circles/authorized_writer/commands.rs`, `device_exclusion/mod.rs`, and
  `restore/recovery_preparation.rs`.


- **Verified commit bytes stored twice**: `StoreCommitVerifier.commits` and a
  clone inside each `VerifiedMergeHistoryCommit`; `load_ref` probes both.
- **Live remeasure of the retained-history and acknowledgement fixes is still
  open.** The mac app is booted with sync locked (screen-lock keychain refusal),
  so the field numbers for `prepare retained history` after `e3e5740f` land once
  the user unlocks. The fixture says a settled cycle's provider reads no longer
  move with retained depth; if the live cycle disagrees, the disagreement is the
  finding, and it belongs in
  `acknowledged_history_depth_does_not_change_what_a_settled_cycle_reads` as a
  case rather than in a new one-off test.

- **References below a snapshot cut still have almost no coverage, and the
  obvious fixture does not reach them.** The suite hits
  `load_retained_merge_history_checkpoint_on`'s snapshot branch exactly once —
  `circles::tests::rotation_required::post_close_circle_store_snapshot_restores_and_converges`,
  found by bisecting the suite with a probe on the branch.

  An attempt to build a plain fixture for it failed, and what it established
  narrows the next one considerably. Each of these was verified, not assumed:

  - Publishing and acknowledging a snapshot does **not** prune the publisher's
    retained rows. `retain_snapshot_replay_inputs` runs against the snapshot
    *image*, so pruning is what a device does when it **installs** one.
  - Restoring from the snapshot does prune: the covered commit's retained row is
    gone on the restored device.
  - But a restored device that pulls re-materializes the covered history and
    gives it retained rows again, so every checkpoint comes back `Commit`.
  - And a restored device that has not pulled has an empty materialized
    frontier, while the publisher's frontier — captured either side of the
    snapshot — has no `snapshot_coverage` row on it, so the walk falls through to
    `retained_materialization_identity` and fails with `QueryReturnedNoRows`.

  So the branch needs two conditions at once, which ordinary restored history
  does not meet: the reference must have a `snapshot_coverage` row and be a tip
  of the snapshot summary's signed frontier, *and* it must not have been
  re-materialized. The one test that gets there does so because a Circle
  successor bootstrap covers old-epoch content the restored device never
  materializes. Reclaimed remote objects would be the other way in.

  The next attempt should start from that test's shape rather than from
  `AcknowledgedHistory`. The scaffolding it needs and that does work:
  `capture_snapshot_cut` → `push_snapshot_cut` → `stage_acknowledgement` →
  `drain_acknowledgements` to publish an acknowledged snapshot, then
  `prepare_snapshot_bootstrap` → `install` into a fresh temp directory to
  restore. `RestoringStore` needs test hooks for
  `retained_merge_replay_inputs` and `retained_merge_history_frontier`; it has
  neither today.

## Direction

One reuse mechanism per artifact class, at the lifetime its immutability
allows: verification of an immutable artifact is durable knowledge and
belongs at connection lifetime or in SQLite rows, not in per-cycle maps.
Per-cycle maps are only right for state that a cycle can actually change.
Every consolidation must delete the mechanisms it replaces.

Enforced as a script failure, not a convention: a boundary check (same
mechanism as the clock gating in check-owner-boundaries.sh) confines the
storage reads that fetch verification artifacts to the one module that owns
the durable verified-artifact store. Any other module calling those reads
fails the pre-commit hook. The read-counter test fixture stays as the
behavioral backstop (the boundary proves who reads, not how often).

The homes are named file by file, not by directory. The directory form let
thirty-nine files that read nothing sit inside an allowance meant for the
twenty-one that do, so a new reader could appear in any of them without the gate
noticing — which is the whole thing the gate exists to stop. Twenty-five files
is a longer list that says what is true.
