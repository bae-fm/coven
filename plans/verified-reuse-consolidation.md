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

## In flight
- `publish pending writes` measured 25.9 s for one 40-blob release: the
  `prepared_remote_objects` loop writes each object serially. Stage timings
  being added inside the publish path; likely fix is the bounded fan-out the
  upload drain already uses.

## Recorded, not yet scheduled

- **Baseline cache bypass on the read path.**
  `materialization_io.rs:447` and `:640-645` call the generation-zero
  baseline loaders directly instead of `RetainedReplayCache::baseline_on`;
  each call deserializes and revalidates the entire baseline DB image into a
  fresh in-memory SQLite connection. Same class a00088f8 removed from the
  install path.
- **Founder registration held three ways** in one verifier graph: a
  `OnceLock`, a force-inserted `registrations` entry, and
  `MergeHistoryVerifier.founder`.
- **Registrations represented four ways** across the DB boundary:
  `VerifiedStoreAuthority.registrations`, its per-transaction clone
  (`VerifiedStoreAuthorityTransaction`), the borrow adapter
  `CachedVerifiedRegistrations`, and `StoreCommitVerifier.registrations`.
- **Verified commit bytes stored twice**: `StoreCommitVerifier.commits` and a
  clone inside each `VerifiedMergeHistoryCommit`; `load_ref` probes both.
- **`owner_anchor` memo re-derives its checks on every hit**
  (`validate_owner_anchor_cache`), so it saves only the SQL write, not the
  verification.
- **Snapshot reads are flat but not minimal.** After the snapshot memo landed a
  settled two-device cycle reads ten snapshot objects covering four distinct
  ones, at every history depth. Something outside `load_store_snapshot` re-reads
  them within one cycle — `load_store_snapshot_stream` is the likely path, since
  it walks a device's snapshot anchor rather than asking by reference. Does not
  scale with retained history, so it was not part of the 119-192 s; it is a
  plain within-cycle duplicate of the class every "Reuse verified X" commit
  removed.
- **Live remeasure of the retained-history and acknowledgement fixes is still
  open.** The mac app is booted with sync locked (screen-lock keychain refusal),
  so the field numbers for `prepare retained history` after `e3e5740f` land once
  the user unlocks. The fixture says a settled cycle's provider reads no longer
  move with retained depth; if the live cycle disagrees, the disagreement is the
  finding, and it belongs in
  `acknowledged_history_depth_does_not_change_what_a_settled_cycle_reads` as a
  case rather than in a new one-off test.

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
