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
