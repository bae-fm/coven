# Replication test performance

## Objective

Profile the complete replication suite, remove avoidable runtime work at its
source, remeasure, and repeat until the remaining runtime follows the behavior
under test rather than a shared implementation bottleneck.

## Baseline after retained-history redesign

- The full 637-test replication suite executes in 119.55 seconds.
- The 111 Circle tests execute in 45.90 seconds and consume 281.50 seconds of
  user CPU plus 244.28 seconds of system CPU.
- The Circle process records 8,092,954 involuntary context switches.
- Limiting the harness from the machine's 14 logical cores to eight concurrent
  tests reduces the Circle group to 37.55 seconds, system CPU to 24.34 seconds,
  and involuntary context switches to 2,645,318. The improvement demonstrates
  cross-test contention rather than work required by Circle behavior.

## Profile evidence

A 20-second `sample` profile of the default Circle run records 41,605 stacks
waiting on `__psynch_mutexwait`. Their callers are SQLite allocation paths:
`sqlite3Malloc`, `dbMallocRawFinish`, SQL parsing, statement preparation, and
schema construction. Independent test databases are serialized by SQLite's
process-wide `SQLITE_MUTEX_STATIC_MEM` lock.

The bundled SQLite source ties that lock to memory allocation statistics. When
`SQLITE_DEFAULT_MEMSTATUS=0`, `sqlite3Malloc` and `sqlite3_free` call the
thread-safe system allocator directly instead of taking the global statistics
mutex. Coven does not call SQLite's memory-use, high-water, soft-limit,
hard-limit, or status interfaces, so the statistics drive no product decision.

## Decision

Compile the bundled SQLite with `SQLITE_DEFAULT_MEMSTATUS=0` and assert the
compile option through the real SQLite connection. Keep SQLite thread safety,
connection ownership, WAL, synchronous mode, transactions, and durability
unchanged.

The source-controlled build setting reduces the 111 Circle tests from 45.90
seconds to 26.85 seconds. System CPU falls from 244.28 seconds to 13.34 seconds
and involuntary context switches fall from 8,092,954 to 902,179.

The full 637-test replication suite executes in 55.85 seconds, down from 119.55
seconds after the retained-history redesign. All 637 unit tests and the
replication documentation test pass with the setting enabled.

## Workspace test profile

After removing SQLite contention, the Circle profile is dominated by protocol
JSON serialization and parsing, object-hash hex conversion, signature checks,
and database authority decoding. These paths live in workspace crates compiled
at test `opt-level=0`; only `coven`, `coven-domain`, and `coven-replication` had
package overrides.

Using test `opt-level=1` for every workspace crate reduces Circle execution
from 26.85 seconds to 9.44 seconds and the complete replication suite from
57.76 seconds to 20.95 seconds. A cold internal-package rebuild with external
dependencies retained takes 163.37 seconds including the test run, compared
with 165.93 seconds for the prior profile. The compile-plus-test gate is not
slower without a build cache, and cached test execution removes 36.81 seconds.

Set `profile.test.opt-level` to 1 at the workspace level and retain the existing
level-3 wildcard for non-workspace dependencies. Test assertions and overflow
checks remain enabled.

## Connection-owned retained materializations

The optimized Circle sample still spends CPU in
`retain_merge_materialization_with_authority_on` and
`load_retained_merge_materialization_with_authority_on`. The database runtime
already owns a cache of fully verified retained inputs. Received commits clone
that cache, add the verified input produced by the transaction, commit, and
then replace the live cache. Locally published commits persist and return the
same verified input but discard it, so the next replay hashes, parses, and
verifies local history again.

The existing retained-input corruption test establishes the intended boundary:
an open connection continues using its verified value, while a reopened
connection verifies the durable bytes and rejects corruption. Extend that same
atomic cache update to every local transaction that retains a verified Merge
materialization. The cache update must become visible only after its database
transaction commits.

The regression publishes a local Store commit, corrupts its durable retained
input before any replay read, and requires the open connection to return the
value verified by publication. It fails before the cache update because replay
rehashes the corrupted row. After local publication, Circle activation, device
join, bootstrap, and Owner recovery publish their retained values to the cache
only after committing the transaction, the regression and all 638 replication
tests pass. Circle execution falls from 9.44 seconds to 7.74 seconds. In the
sample, SHA-256 stacks directly under retained materialization load/retain fall
from 377 samples to 14.

## Cycle-test isolation

With retained inputs cached, the 60 cycle tests execute in 4.01 seconds. Running
each test alone identifies two outliers: snapshot count cadence at 3.25 seconds
and registration acceptance serialization at 2.10 seconds.

The registration test spends two seconds in its assertion, not in Coven. It
holds device-join acceptance after that operation has claimed the owner's Store
stream position, starts the normal write drain, and requires a two-second
`tokio::time::timeout` to expire before releasing acceptance. Keep that exact
negative assertion, but run the test with Tokio's paused clock. Tokio advances
the timeout only after the write drain has no runnable path, preserving the
ordering check without consuming wall-clock time.

The isolated registration test falls from 2.10 seconds to 0.09 seconds. The
parallel cycle group remains 4.00 seconds because the snapshot cadence test is
now its longest path.

The snapshot cadence test is different: it publishes 100 signed local commits
because 100 is the production count threshold. Do not replace those commits
with reconstructed database state or lower the production threshold for the
test. Profile that publication path before deciding whether the runtime is
required protocol work or another repeated implementation bottleneck.

## Connection-owned replay baseline

Phase timing isolates 2.35 seconds of the cadence test in its 100 local
publications and another 0.74 seconds in the final sync cycle. A Time Profiler
sample finds both paths repeatedly calling
`load_generation_zero_replay_baseline_on`: writer authorization reaches it
through `validated_store_owner`, while canonical replay loads it directly.
The loader opens and validates the baseline database image, parses and
canonicalizes its authority, checks its payload hashes, validates its schema,
and reconstructs founder device state. The baseline is installed once when the
local Store authority is established and has no production update path.

The database runtime already retains verified replay materializations for the
life of the open connection, with durable bytes checked again when a database
is reopened. The replay baseline has the same trust boundary and is the root
input to those materializations. Extend that existing cache to own the verified
baseline as well, and rename it from a materialization cache to the retained
replay cache. Writer authorization must continue checking the live root, owner
anchor, founder registration, and stored genesis on every operation; only the
immutable baseline load is retained. Canonical replay opens a fresh writable
copy of the retained baseline image on each run, but no longer re-verifies the
immutable source bytes within one connection.

Prove the boundary through the real Store authorization path: authorize once,
replace the durable baseline authority bytes, and require a second
authorization on the same open connection to use the already verified
baseline. The existing raw baseline loader test continues proving that reading
durable baseline bytes rejects altered authority.

The regression fails before the cache change because the second authorization
parses the replaced authority and passes afterward through the production
membership authorization path. The raw loader rejection, all 32 membership
tests, all 60 cycle tests, and all 183 database tests pass. The isolated
snapshot cadence test falls from 3.25 seconds to 2.50 seconds; the complete
cycle group falls from 4.00 seconds to 3.03 seconds.

## Connection-owned retained history checkpoints

The next Time Profiler sample no longer includes replay-baseline validation in
the publication hot path. Its dominant database operation is
`retained_merge_history_frontier`: 628 inclusive samples, including 545 in
`open_retained_merge_history_checkpoint_on`. Every successor walks its retained
ancestors. Although their retained inputs already come from `RetainedReplayCache`,
each walk reloads the corresponding device-state snapshot, parses and validates
it, derives the state from the verified commit again, and compares both values.
The resulting work grows with every retained successor.

The state snapshot and retained materialization are written in one SQLite
transaction. The retained input alone is not enough to trust a checkpoint after
opening an existing database, because durable state could have been altered.
Validate their relationship the first time the open connection uses that exact
materialization as a checkpoint, then retain the verified relationship beside
the cached input. Do not mark a newly inserted materialization checked: this
keeps corruption before first checkpoint use observable. Rebuilding the cache
from durable rows preserves a checked marker only when the coordinate, exact
commit reference, and retained input hash still match.

The regression publishes three commits, which makes the first two commits
verified history checkpoints while preparing their successors, deletes the
first commit's durable state row, and publishes a fourth commit through the
same connection. Before the cache owns checkpoint verification, the fourth
publication reloads the deleted ancestor and fails. The existing sabotage test
still deletes or forges a checkpoint before its first use and must continue to
fail publication.

The regression fails before the cache change on the removed first-commit state
and passes afterward. The missing/forged checkpoint sabotage test, all 11
retained-history checkpoint tests, all 60 cycle tests, and the complete
all-feature workspace test run pass. The isolated snapshot cadence test falls
from 2.50 seconds to 1.91 seconds, and the cycle group falls from 3.03 seconds
to 2.52 seconds.

A new Time Profiler sample shows the intended boundary: retained-history
frontier work falls from 628 inclusive samples to 96, and
`open_retained_merge_history_checkpoint_on` falls from 545 to 2. The next
dominant named application path is exact announcement-head loading and parsing:
`load_exact_announcement_path` has 273 inclusive samples and Store device-head
parsing has 308, with 544 samples in signature verification across the run.
Trace that call path from current code before deciding which verification can
be retained and which belongs to each new publication.

## Verifier-owned announcement path

Sample timestamps place announcement traversal in the final sync cycle rather
than the 100-publication loop. `AuthorizedPull::execute` first calls
`prepare_retained_history`, which passes every retained commit reference to one
fresh `MergeHistoryVerifier`. `verify_refs` processes those commits in causal
order, but `exact_next_announcement_slot` starts at the registration's first
head slot for every commit. Verifying a stream through sequence 100 therefore
reads and verifies 5,050 accepted heads. Stream discovery then performs its own
single linear scan to detect new remote heads; that scan is distinct live
discovery and must remain.

An accepted head path is meaningful only within the `StoreCommitVerifier` that
read it from the provider. Retain each verified head, its exact object, and its
successor slot on that verifier. A request for a later accepted commit extends
the path from its verified frontier; a request for an earlier coordinate checks
the cached exact commit. Insert an entry only after both the head and its commit
have authenticated for the requested registration, so a failed or occupied
slot never advances the path. This keeps provider acceptance verification while
making one verifier read each accepted head once.

The regression runs a real pull over retained history and counts provider reads
of every retained announcement slot. A cycle performs several independent
whole-stream checks, but each should read all retained slots equally. Requiring
the maximum and minimum counts to differ by at most one rejects the restart
pattern without conflating those whole-stream checks.

Before the verifier retains the prefix, the 12 head slots are read in the
descending pattern 16, 15, 14, through 5; afterward their counts are flat. The
isolated snapshot cadence test falls from 1.91 seconds to 1.60 seconds, and the
60 cycle tests fall from 2.52 seconds to 2.11 seconds. A fresh Time Profiler
sample has 6 inclusive samples in `exact_next_announcement_slot`, down from 273
in the removed full-path loader. The remaining linear live scan has 27 samples,
and retained-history preparation has 32.

Tracing the new acceptance cache also exposes a missing invariant in the old
traversal: a signed head read from announcement position N did not have to name
commit coordinate N. A two-head regression replaces both authenticated heads
with a valid chain that names commit 2 twice. The exact-position lookup accepts
that chain before the check. Require every accepted head's commit stream to be
the registration's announcement stream and its sequence to equal the position
being traversed before retaining it. This rejects the malformed chain at its
first head instead of allowing the cache to give the misplaced commit meaning.
All 13 retained-history checkpoint tests and the complete all-feature workspace
test run pass with both checks in place.
