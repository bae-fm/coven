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

## Connection-owned Store authority

After announcement-prefix retention, the cadence profile is dominated by
reopening authority already verified on the same database connection.
`load_store_root_authority_on` has 163 inclusive samples, including 144 in
signed root parsing. Activated registration parsing has 178 samples, and total
signature verification has 375. The Store root is installed once, and each
device id has one immutable registration activation. Neither has a production
replacement path on an open database.

The existing connection-owned cache is no longer only a replay cache: it owns
the verified baseline, verified retained inputs, and their checkpoint facts.
Rename it to describe the connection lifetime, and add the exact Store root and
exact activated registrations to it. Absence is never cached, so Store creation
can install authority after an earlier empty read. A cached registration is
keyed by its full exact reference, not only its device id. Publication and
writer authorization then consume the connection-owned verified values; a
reopened database still parses and authenticates durable bytes before use.

Prove both boundaries through publication. Publish once to verify the authority,
then either delete the durable Store-root row or corrupt the activated
registration bytes and publish again on the same open connection. Before the
authority belongs to the connection, each second publication fails while
reparsing the sabotaged row. Afterward it must use the retained verified value.
Existing open/raw-loader corruption tests continue to prove that sabotage before
first verification or before reopen is rejected.

## Process-owned expected schema

The complete-suite profile after connection-owned Store authority is dominated
by database opening. `expected_coven_schema_manifest` creates an in-memory
SQLite database, creates every Coven table and trigger, then reads SQLite's
canonical schema. An initialized writable open called it four times while
checking the same binary-owned schema before and after the host migration
transaction. Across parallel test databases, the profile records 3,792 samples
in expected-manifest construction and 2,135 in live-schema validation.

The two expected manifests — with and without scoped-routing tables — are
immutable properties of the running binary. Construct each once and return the
same borrowed value to every database open. Keep both live-schema reads around
the migration transaction: they validate durable state before host code runs
and validate the transaction's resulting state before it commits.

A regression requires repeated requests for either routing shape to return the
same process-lifetime value. The 648-test replication suite falls from 17.42
seconds to 16.12 seconds. The next full-suite sample has no
`expected_coven_schema_manifest` entry; live schema validation remains.

## Verifier-owned membership chains

The next complete-suite profile is dominated by membership graph loading:
exact membership graph objects account for 918 inclusive samples, the graph
loader for 868, and anchored-chain construction for 790. A retained-history
pull reconstructs the same exact membership chain once for each commit while
verifying its history, then reconstructs every predecessor membership again
when collecting retained candidates for terminal-retraction checks.

Retain a membership chain on `MergeHistoryVerifier` only after the chain is
complete for a verified Merge membership prefix and its full membership-state
reference matches the verified predecessor device state. Reuse requires the
same exact membership heads and conflict resolutions and a current verified
prefix that contains the cached prefix. A commit carrying a pending conflict
resolution is not cached because that resolution is not yet part of the
verified prefix. Complete-prefix validation and full state-reference validation
still run at every use, so newly accepted controls, recovery cursors, and the
combined state hash remain checked.

Retained materializations need no second membership load. `verify_refs` already
stores each verified commit's exact predecessor membership. The pull consumes
that verified value by commit reference instead of discarding it and reading
the provider again. The regression compares provider reads for one retained
commit with reads for twelve. Before either reuse path it reports 11 and 33;
with verifier-only reuse it reports 8 and 19. Disabling the final verifier cache
after both changes reports 10 and 21, proving that both retained values are
used. With both paths active, retained-history depth no longer increases exact
membership-head reads.

A cache inside the low-level object verifier was rejected. Although it removed
the repeated reads, its shared lock and per-object lookup increased the complete
suite from the 16-second range to the 19–21-second range. Membership authority
is the reusable result, so the cache belongs to `MergeHistoryVerifier`, where
it stores one checked chain rather than every intermediate remote object.

The post-change complete-suite sample reduces exact membership graph-object
loading from 918 inclusive samples to 490 and graph loading from 868 to 445.
Membership loading remains visible across other test paths, so trace those
callers next rather than extending the retained-history cache beyond its owner.

## Current membership must be refreshed

The remaining graph loads include `Store::authorize`, but its current membership
cannot join the immutable connection-owned authority. Durable membership heads
are a lower bound while the provider may publish a successor at any time. A
connection cache keyed by those durable heads hid later membership changes until
14 correctness tests rejected it: deleted or malformed exact objects, cursor
regression, suppressed removals, and newly published heads all stopped being
observed.

Adding each verified terminal head and probing only its successor preserves
new-head discovery, but still hides deletion or replacement of previously
verified provider objects. The existing refresh contract deliberately rereads
the exact chain so those failures remain visible. Keep membership reuse scoped
to one `MergeHistoryVerifier`, where a fixed verified Store-history prefix makes
the authority immutable. Across authorization or sync refreshes, reload and
verify the provider chain.

## Publish the installed replay baseline

The next complete-suite sample records 1,520 inclusive samples in generation-zero
replay baseline loading and validation. Store installation already reloads the
new row, reads both payloads, validates the image, checks its authority, and
compares the reconstructed baseline with the value it intended to install before
the SQL transaction commits. That verified value was then discarded. The first
owner validation on the same connection repeated the entire load to fill
`VerifiedStoreAuthority`'s replay cache.

Return the installed value from the write-and-reread operation and publish it to
the connection-owned authority after the SQL commit, beside the root and founder
registration produced by the same transaction. A failed transaction publishes
nothing. The connection asserts that all three values name the same Store root.
Reopening still reloads and validates the durable baseline.

The regression installs a real owner anchor, removes the durable baseline through
the test SQL surface, and asks the same open database to validate its owner. It
fails before the installed baseline is published and succeeds from the value
verified during installation afterward. The existing injected-trigger test still
proves that a failed baseline insert does not publish any connection authority.

## Verifier-owned Store registrations

After the installed baseline is published, the 12-second complete-suite profile
records 802 inclusive samples in remote registration verification and 1,269 in
registration parsing. `StoreCommitVerifier::load_registration` rereads and
reauthenticates the same immutable exact registration for every commit and every
protocol edge that names it, even though one verifier already owns the Store root
needed to check all of them.

Retain a `VerifiedObject<StoreDeviceRegistration>` by its full exact reference on
`StoreCommitVerifier`. Insert only after the provider bytes pass exact-object,
Store-root, reference, origin, slot, and signature checks. A new authorization
history constructs a new verifier and therefore refreshes provider state; reuse is
limited to one history verification operation.

The retained-history regression counts reads of the exact author-registration
slot during a real cycle. Before reuse, one retained commit reads it 20 times and
twelve read it 31 times. After reuse, history depth no longer increases that
count. The complete suite remains at 651 passing tests and 14.85 seconds. In the
next 12-second sample, remote registration verification falls from 802 inclusive
samples to 406, registration parsing from 1,269 to 870, and the cache lookup itself
accounts for 20 inclusive samples.

## Content-addressed payload reinstalls

Temporary instrumentation of every payload-spool commit during the complete
replication suite counted 12,127 installs. Of those, 1,913 replaced a path that
already held the same content hash. These are real production calls reached by
retried or shared remote-object persistence, not a generation-zero-only fixture
artifact.

The byte-slice write paths already know the complete bytes and can compute their
name before opening a temporary file. Read an existing hash-named file first. If
its bytes are identical, return the hash without creating, writing, flushing, or
renaming a replacement. If it is absent or differs, atomically install the given
bytes exactly as before. The streaming file-copy path still hashes while writing
because it does not hold the source bytes in memory.

The regression routes durability through the file-sync observer. Before the
change, one initial write and two identical reinstalls issue six sync requests;
the expected count is the initial write's two requests. It also corrupts the
hash-named file and proves a later install atomically replaces the different
bytes, raising the count by exactly two rather than treating the pathname alone
as proof of content.

In the next 12-second complete-suite sample, payload-spool stage creation falls
from 1,063 inclusive samples to 906 and atomic-file commit rename work from 822
to 571. The byte-slice path no longer uses the hashing stream writer; hashing
the bytes before the lookup keeps one digest on a first install and supplies the
name needed to recognize a repeat. A warm unsampled suite reports 15.80 seconds,
within the observed run-to-run spread but above the preceding 14.85-second run;
the sampled call-path reduction, not that single wall-time comparison, supports
the change.

## Reuse mapped-query statements

SQLite statement parsing remains one of the largest database costs: the next
sample records 3,894 inclusive samples in `sqlite3RunParser` and 3,196 in
prepare/locking. Coven's shared `query_mapped_rows` helper called `prepare` for
every invocation, so even identical static queries on one owned connection were
parsed every time.

Enable rusqlite's connection-local statement cache and have this helper request
a cached statement. The regression installs SQLite's prepare-time authorizer as
an observer. Two identical helper calls produce two `Select` preparations before
the change and one afterward. Only this internal helper opts into the cache;
host SQL and dynamically prepared statements keep their existing paths.

After rebuilding from an empty target, the complete replication suite passes all
651 tests in 13.76 seconds, repeated at 13.76 seconds on the warm run. The next
12-second profile still records 3,965 parser samples because most database calls
do not use this helper; mapped queries are no longer guaranteed contributors to
each repeated prepare. Treat the remaining parser work by its concrete callers,
not by turning every dynamic SQL operation into a cache entry.

## Founder registration joins the verifier cache

`StoreCommitVerifier` retained exact registrations loaded by reference, but its
founder loader always reread the founder slot, parsed the object, and verified
its signature. History construction, membership controls, device joins,
snapshots, and recovery checks all call that special loader on the same verifier.

Retain the first verified founder object in a `OnceLock` owned by the verifier
and place it in the existing exact-registration map. The root descriptor fixes
the founder slot, and reuse lasts only for the verifier operation, matching the
scope already established for other exact registrations. A later authorization
constructs another verifier and refreshes the provider object.

The regression authorizes one history and invokes the founder loader twice. Its
prepare-time authorization also needs the founder, so before the change the same
slot is read three times; afterward all three consumers share one read. The full
suite passes 651 tests in 13.61 seconds. In the next 12-second profile, founder
and ordinary registration parsing falls from 939 inclusive samples to 674 and
`verify_opened_registration` from 389 to 136.

## One verified Store announcement prefix per verifier

The retained-history regression's earlier flat-read assertion still allowed
every head to be reread equally. Tightening it to require at most one read per
exact head exposed four consumers in one cycle: retained-history verification,
exact-head reopening, pull discovery, and snapshot-reclaim discovery. Stack
traces confirmed that all four use the same `StoreCommitVerifier`.

Make the verifier's accepted announcement path the shared verified prefix. Each
accepted coordinate records its authenticated head, commit, and next slot.
Exact-head loading reuses the authenticated object, while each live discovery
starts at the cached next slot and still probes there for a newly appended head.
An inactive stream can reuse only the part at or below its accepted cut. Cache
inconsistencies fail rather than falling back to provider reads.

Before the change, every one of the twelve retained head objects was fetched
four times in one cycle. Afterward the regression observes at most one fetch per
head. The complete 651-test suite passes in 13.45 seconds. In the following
12-second profile, Store device-head parsing accounts for 480 inclusive samples,
down from 603, and total signature verification falls from 4,166 to 4,076.

## One complete owner anchor per database connection

The next profile's largest named Coven path was generation-zero replay baseline
loading. `load_and_install_owner_membership` refreshes the live membership chain
for every Store initialization, but then passed eight separate root, founder,
genesis, owner, and byte values into the database's owner-anchor installer. The
installer reopened the immutable replay baseline even when the same database
connection had already committed and retained the root, founder registration,
and baseline as one authority.

Represent the installation input as `StoreOwnerAnchor`, composed from the two
verified exact protocol objects and their references. `VerifiedStoreAuthority`
records the root/founder coordinate only after the SQL installation commits or a
pre-existing durable owner anchor is fully validated. A later initialization
must present the same exact typed root and founder values; it then persists only
the refreshed membership cursors. The baseline, root, and founder caches remain
private and can no longer be published independently after an owner install.

The regression alters the durable baseline authority after Store creation and
initializes another Store handle through the same connection. It fails by
parsing the altered payload before the change and succeeds from the
connection-owned anchor afterward. The interrupted-creation regression now
opens the same database through a fresh connection before asserting that an
exact founder reinstalls its missing owner pin; a process restart cannot retain
the old connection cache.

All 652 replication tests and the compile-fail documentation test pass in 13.80
seconds. In equal 12-second `sample` captures, the sum of baseline-loader stack
counts falls from 1,872 to 958; the remaining calls belong to first installation
and first validation on new connections. The loader no longer appears in the
collapsed top-of-stack report. The captured profile is
`/private/tmp/coven-replication-owner-anchor.sample.txt`.

## Use the processor's SHA-256 implementation

The next complete-suite sample records 6,223 top-of-stack samples in
`sha2::sha256::compress256`. Coven's direct `sha2` 0.10 dependency uses the
software implementation on ARM64 unless its assembly feature is enabled, even
though the test profile already compiles dependencies at optimization level 3.
The current 0.11 release detects ARM SHA-256 instructions at runtime without an
optional assembly dependency. Update the directly used SHA-2, HMAC, and HKDF
crates together so they share the current digest traits.

All 652 replication tests pass in 12.76 seconds, compared with 13.45 seconds
before the update. In the following 12-second sample, SHA-256 compression falls
to 1,511 top-of-stack samples and runs in the ARM hardware backend. The captured
profile is `/private/tmp/coven-replication-sha11.sample.txt`.

## Build snapshot projections in memory

With hashing reduced, the sampled suite records 6,761 top-of-stack samples in
`pwrite`; 2,779 of them are below `SnapshotDatabaseImage::capture_on`. Capture
first runs `VACUUM INTO` to write the complete live database to its plaintext
temporary path, projects that disk copy, and then runs another `VACUUM` to write
the retained projection again. Circle snapshots similarly write the complete
copy before removing transport state and vacuuming the result.

SQLite serialization already supplies the transaction-visible database image
used by retained replay, including WAL databases and uncommitted founder state.
Open that image as a private in-memory connection, perform the existing
projection and validation there, then serialize the final image to the staged
path once. The snapshot's public path-based ownership and cleanup contract stays
unchanged; only the intermediate full-database disk copy disappears.

All 652 replication tests pass in 12.25 seconds. System CPU falls from 26.74 to
21.39 seconds. In the following complete-suite sample, total `pwrite` falls from
6,761 to 2,452 top-of-stack samples, and the 2,779 samples below
`SnapshotDatabaseImage::capture_on` disappear. The captured profile is
`/private/tmp/coven-replication-memory-snapshot.sample.txt`.

## Disable SQLite physical durability in test stores

Test Store directories already inject file operations that omit physical sync,
but the database open path separately forced SQLite `synchronous=FULL`. Carry a
closed connection-durability choice from database construction into the owned
connection. Production opens select `FULL`; test Store opens select `OFF`.

The regression opens both choices through the real connection opener and reads
SQLite's effective `PRAGMA synchronous`: production is `2` and test construction
is `0`. All 652 replication tests pass in 11.89 seconds, with 21.00 seconds of
system CPU. The following sample records 548 `fsync` top-of-stack samples,
compared with 683 before the injection; the remaining named journal syncs come
from the separately owned pending-device-join database. The captured profile is
`/private/tmp/coven-replication-sqlite-durability.sample.txt`.

## Give the pending-join database the same durability ownership

The remaining named sync path belonged to `DeviceJoinJournalStore`, a separate
SQLite owner that retained only a path and reopened a default connection for
every journal operation. Have the owner retain one locked connection and apply
the construction-time durability choice before retaining it. Production
construction selects `FULL`; replication fixtures explicitly select the test
constructor. Clone handles share the same connection owner, and no operation
returns or accepts the connection.

The regression opens the pending-join owner through its test constructor and
observes SQLite `synchronous=OFF` through a closed test operation. All 652
replication tests pass in 11.86 seconds. In the following complete-suite sample,
`fsync` no longer appears in the collapsed top-of-stack report and no
`DeviceJoinJournalStore` stack contains it. The captured profile is
`/private/tmp/coven-replication-owned-pending-journal.sample.txt`.

## Keep attached journal completion crash-atomic

The next profile showed that disabling SQLite synchronization did not remove
the WAL's page writes: Store connections still wrote each commit to a WAL,
checkpointed it during long tests, and checkpointed the remainder when the
connection owner closed. Test construction now selects an in-memory rollback
journal as part of the same durability choice. All 652 replication tests pass
in 11.57 seconds, and the next sample records 1,208 `pwrite` top-of-stack
samples instead of 7,193. The remaining largest write stacks are the Store
database pages that SQLite writes when real transactions commit. The captured
profile is `/private/tmp/coven-replication-memory-journal.sample.txt`.

That profile also exposed physical synchronization under
`complete_device_join_from_pending_on`. Completion attaches the pending journal
to the Store connection, so the pending owner's connection setting cannot
govern that attached schema. More importantly, the bundled SQLite commit code's
`aMJNeeded` table excludes both WAL and MEMORY journal modes from its
super-journal transaction: the existing production Store WAL therefore could
not make the Store insertion and pending-journal deletion crash-atomic.

Durable connections must use `journal_mode=DELETE` with `synchronous=FULL`, the
combination SQLite includes in its super-journal commit. Test connections use
`journal_mode=MEMORY` with `synchronous=OFF`. The Store connection owner retains
that construction choice and applies it to the attached pending schema before
starting completion. A secondary read-only connection remains able to coexist
with the rollback-journal writer and observe its committed rows.

All 652 replication tests pass in 11.50 seconds. The following complete-suite
sample records 1,329 `pwrite` samples and eight `fsync` samples. The pending-join
completion path contains no physical synchronization; all eight remaining
samples are below `SnapshotDatabaseImage::install_blob_graph`. The captured
profile is `/private/tmp/coven-replication-rollback-journal.sample.txt`.

## Install the snapshot blob graph in memory

The staged snapshot database is owned and unpublished while its blob graph is
installed. Opening that file as a disk SQLite database creates a rollback
journal, commits and synchronizes it, vacuums the database, and synchronizes the
result. Open the staged bytes as a private in-memory connection instead, perform
the same transaction and vacuum, serialize the resulting database, and write it
back to the owned stage. A failed write still discards the stage through the
existing ownership path.

The regression reserves the staged image's `-journal` path with a directory;
the disk-backed implementation fails with a SQLite disk I/O error, while the
in-memory implementation installs and preserves the database image without a
sidecar. All 189 database tests and all 652 replication tests pass; the latter
finish in 11.54 seconds once built. The following complete-suite sample records
zero `fsync` samples, 1,077 `pwrite` samples, and 5,088 `open` samples. The
captured profile is
`/private/tmp/coven-replication-memory-blob-graph.sample.txt`.

## Put protocol-sized payloads through the database owner

The next profile's largest named Coven path is payload installation below
`persist_prepared_remote_object_on`: content-addressed temporary-file creation,
write, and rename. Temporary complete-suite instrumentation records 11,500
blocking byte-slice installs. Their sizes are bimodal: 10,191 are at most 16
KiB, while 1,299 are large artifacts, usually about 0.5 MiB. The byte-slice
calls carry 24.4 MiB in the smaller group and 779.3 MiB in the larger group.

Call-site instrumentation of new installations attributes 6,797 of the small
values to `remote_object_records`; these are protocol objects, not database
images or changesets. It also exposes two write paths for Circle preparation:
the preparer writes every exact object to the file spool, then
`insert_circle_operation` receives the same prepared bytes, persists the remote
records, and reopens or rewrites those payloads before it commits their owner
claims. Preparation has no durable row to own those files and the database call
already has every byte needed to install them.

First make Circle insertion the single durable boundary: preparation returns
the journal and prepared objects without touching local payload storage;
insertion installs every prepared object before committing the operation and
its claims. Publication reads those bytes through the database owner rather
than constructing a second payload capability from `StoreDir`.

Then make the database-owned payload capability select the representation.
Protocol-sized byte slices belong in a content-addressed SQLite payload table;
large and streaming inputs remain content-addressed files. A catalog row is the
authoritative storage tag, so reads dispatch directly instead of probing both.
The owner and cleanup rows reference that catalog. This preserves one content
identity and one claim graph while removing one-file-per-protocol-object work.

The database now owns the complete payload path. Every value is stored as one
LZ4 frame while its content address and reported size continue to describe the
original bytes. Compressed values through 64 KiB live in
`payload_storage.compressed_bytes`; larger values use the file named by the same
content hash. Streaming writes make the same choice from their finished
compressed size. `payload_owners` and `payload_cleanup` both reference the
catalog, and replication tests use closed `StoreDatabase` test operations rather
than reaching through `StoreDir`. Installation is refused outside a transaction,
and snapshot, Circle snapshot, Circle operation, retained replay, host write,
and test-import paths install bytes in the transaction that writes the owning
row. The regression first demonstrated that a bare connection could leave an
unowned catalog row, then passed after the transaction gate and caller moves.

All 186 database tests pass in 0.54 seconds. All 653 replication tests pass in
9.24–9.57 seconds once built, compared with 11.63 seconds before inline storage.
The following complete-suite sample records payload file staging in 267 samples;
the larger filesystem totals also contain test-directory and database lifecycle
work. The captured profile is
`/private/tmp/coven-replication-payload-store.sample.txt`.

## Authenticate each membership entry once per chain

The next complete-suite profile records 209 samples in
`MembershipChain::rebuild` below membership-entry signature verification.
Adding one entry rebuilt the complete derived membership state and verified the
signature of every immutable entry again. Activating a different head also
rebuilt the derived state and repeated the same signatures even though it does
not change the entries.

Membership entries now cross their authentication boundary in the chain
constructor or when an entry is added. Rebuilding derived state validates the
causal and semantic relationships but does not repeat signatures for entries
the chain already owns. Standalone membership-object loads retain their own
authentication boundary.

All 170 protocol tests and all 653 replication tests pass; the latter finish in
8.65 seconds once built. In comparable eight-second samples, the removed
rebuild verification site falls from 209 samples to zero and the principal
Curve25519 multiplication leaf falls from 1,249 to 1,140 samples. The captured
profiles are `/private/tmp/coven-replication-lazy-stage.sample.txt` and
`/private/tmp/coven-replication-authenticated-chain.sample.txt`.

## Validate a newly installed replay baseline once

Founder and snapshot baseline construction installed the database image, opened
and validated it, inserted the baseline row, then immediately reloaded and
validated the same stored image again before the transaction committed. The
constructors have no independent caller; the write-and-reload step is the
durable authentication boundary and already checks the payload hash, image
shape, schema, routing metadata, authority, row metadata, and exact value.

Return the constructed metadata after installing its image and let baseline
insertion perform the one complete write-and-reload validation. Failure still
rolls back the baseline row and its payload claims, and no value reaches the
connection-owned authority until the transaction commits.

All 186 database tests and all 653 replication tests pass. The unsampled suite
falls from 8.43 to 8.28 seconds. In equal eight-second samples,
`RetainedReplayBaseline::validate_image` falls from 1,598 inclusive samples to
857 and generation-zero baseline installation falls from 5,097 to 4,447. The
captured profile is
`/private/tmp/coven-replication-single-baseline-validation.sample.txt`.

## Encode object hashes without temporary allocations

Object hashes crossed JSON and SQLite boundaries by formatting through an
allocated `String`, and deserialization first requested an owned `String`
before parsing it into the fixed 32-byte value. Parsing separately validated,
allocated a decoded vector, and copied that vector into the fixed array.

Formatting and serialization now encode lowercase hexadecimal into a fixed
64-byte stack buffer. Deserialization accepts a borrowed string, and parsing
validates and decodes each digit pair directly into the fixed 32-byte array.
Invalid values retain their original text in `InvalidObjectHash`, so only the
error path allocates.

All 46 foundation tests and all 653 replication tests pass; the latter finish
in 8.38 seconds once built. In complete-suite samples, `ObjectHash`
serialization falls from 1,094 inclusive samples to 286, parsing from 750 to
113, formatting from 923 to 28, and deserialization disappears from the named
hot paths. The captured profile is
`/private/tmp/coven-replication-fixed-hash-codec.sample.txt`.

## Compress replay images through the payload store

The suite constructs 919 generation-zero replay baselines. Their source
databases total 588.1 MB and the vacuumed baseline images total 544.1 MB. The
images are mostly empty SQLite pages required by the complete application and
Coven schema; writing each raw image to the payload file area made payload
installation as expensive as constructing the image.

Two alternatives do not improve the complete suite. Reconstructing an empty
database from `sqlite_schema` avoids copying excluded bytes but takes 9.86
seconds because SQLite reparses and rebuilds every schema object. Enabling
`secure_delete`, deleting excluded rows, and retaining free pages takes 11.66
seconds because SQLite overwrites the deleted content and the payload remains
larger. The existing copy, delete, and `VACUUM` projection remains faster at
8.38 seconds.

The projection also retained `sqlite_sequence` rows because the user-table
enumeration intentionally excludes SQLite's internal tables. A regression
inserts an excluded `AUTOINCREMENT` row and proves its counter is absent from
the projected image. Projection now clears those counters before vacuuming.

The first implementation compressed replay images before handing them to the
payload store, which made `image_payload_hash` name the LZ4 frame instead of the
SQLite image. Compression now belongs to the payload store for every payload.
Replay installs and reads raw database-image bytes, while the store hashes those
raw bytes and transparently encodes and decodes the stored frame. The measured
regression image is 536,576 bytes before storage and its frame is 17,120 bytes.
The earlier replay-only version took the unsampled replication suite from 8.38
to 7.67 seconds; the generalized path requires a new complete-suite measurement.
The earlier profile is `/private/tmp/coven-compressed-baseline.sample.txt`.

The generalized path passes all 193 database tests and all 655 replication
tests. Once built, the replication suite finishes in 7.58 seconds. A complete
sampled run finishes in 13.33 seconds and is captured at
`/private/tmp/coven-replication-final-design.sample.txt`. Its active leaf work
is curve-signature arithmetic, SHA-256, SQLite execution and parsing, and JSON
encoding and decoding. LZ4 accounts for 36 compression and 17 decompression
leaf samples; no Coven-owned repeated authority load, replay validation, or
payload path dominates the remaining execution. Further runtime tuning would
optimize required primitives rather than expose another ownership or repeated-
work design fault, so the performance investigation stops at this boundary.

The complete workspace suite then exposed that LZ4 frames for the same logical
payload differ with write chunking. Content-addressed idempotence had compared
the encoded frame, causing snapshot publication to reject bytes first installed
through the streaming writer and later presented as a slice. Payload lookup now
classifies the existing logical value once as absent, verified, or a repairable
file. Both install paths reuse any verified logical payload without changing its
inline/file representation; only an unreadable file is replaced. The regression
covers inline and file payloads in both slice-to-stream and stream-to-slice
orders.

The CI workflow reports test compilation and execution separately. On commit
`8b1a533d`, the Linux runner spent 14 minutes 17 seconds compiling unit and
integration test binaries, then 1 minute 16 seconds running all tests and
doctests. The full Linux, macOS, and Windows workflow passed. CI duration is
therefore dominated by Rust compilation rather than test execution.
