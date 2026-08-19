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

## In flight

- Pull's `prepare_retained_history` re-verifies the whole retained merge
  history from the cloud every cycle (measured 34-38 s per cycle on a small
  two-device store over GCS). Fix underway: recover predecessor membership
  from durable retained checkpoints the way `authorize_retained_outbound`
  already does (zero cloud reads), stop feeding retained refs through
  `verify_refs`, and add the missing test class: a second cycle over an
  unchanged retained history performs no cloud reads for retained commits.
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
- **Acknowledgements cached twice with disjoint populate paths.**
  `StoreCommitVerifier.acknowledgements` (keyed `StoreAckRef`, filled only by
  `load_store_ack`) and `MergeHistoryVerifier.verified_acknowledgements`
  (keyed `ExactObjectRef`, filled only by `load_acknowledgement_proof_chain`).
  `load_store_ack_predecessor` — the per-sequence walk — consults neither.
  One cache should exist.
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
- **`RetainedCommitAuthorities::Operation` requires a complete proofs map it
  never reads on the cache-hit path** (`retained_merge_replay/cache.rs:257-296`
  vs `:360-370`) — the API contract taxes callers for proofs the hit path
  ignores. (Being fixed as part of the pull change if it falls out naturally.)

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
