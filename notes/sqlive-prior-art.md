# sqlive prior art

Other projects that tell an app when a query's result has changed, measured
against what sqlive does. Surveyed 2026-11-19 from each project's docs and
source; a claim marked *unconfirmed* came from a page that did not say.

## What sqlive does

An app subscribes plain SQL over its own SQLite database, with whatever schema
it likes. sqlive reruns a subscribed query only when a write could change its
result. For each query it works out a scope, the rows a write must touch to
matter:

- **WHERE:** a write matters only if it touches a row the filter could select.
- **ORDER BY … LIMIT with no OFFSET:** once the query returned a full LIMIT of
  rows, a write matters only if it touches a row that sorts at or before the
  last row returned.
- **Joins:** a write to a joined table is judged by bounded lookups at write
  time, to the rows of the result it could reach.

sqlive keeps no results: only each query's parameters and its last row's sort
values. The rerun is SQLite's.

## The closest projects

| Project | Language | Reruns or updates on | ORDER BY + LIMIT | Joins | Keeps |
|---|---|---|---|---|---|
| [Zero](https://github.com/rocicorp/mono) (Rocicorp) | TypeScript | Incremental results over a SQLite replica of Postgres | Exact: its [`Take`](https://raw.githubusercontent.com/rocicorp/mono/main/packages/zql/src/ivm/take.ts) keeps a bound, the last row it accepted, per partition | Exact: partners fetched at push time; [`TakeGate`](https://raw.githubusercontent.com/rocicorp/mono/main/packages/zql/src/ivm/take-gate.ts) stops a parent fetch at the window's bound | Results, operator state, indexes |
| [Convex](https://stack.convex.dev/how-convex-works) | Rust | A rerun when a commit overlaps the index intervals a query read | Exact by construction: an interval ends at the last key the query consumed (`crates/database/src/query/index_range.rs`) | Through app-code lookups, each recording its reads | Read sets |
| [Oracle Continuous Query Notification](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/cqn.html) | Database feature | A notification when a registered query's result may have changed; the app queries again | ORDER BY and GROUP BY get only best-effort mode; row limits are not mentioned | Guaranteed mode covers single tables and some inner equijoins | Registrations |
| [SQL Server Query Notifications](https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms181122(v=sql.105)) | Database feature | A notification through indexed-view change detection | TOP is forbidden | Inner only; no outer joins, self-joins or subqueries | Registrations |
| [GRDB](https://github.com/groue/GRDB.swift) `ValueObservation` | Swift | A refetch when a write touches the query's region: tables, columns, and rowids for primary-key filters written in its query builder | No | Table level | The region |

## Window logic in single-collection systems

These keep the last row of a sorted, limited window and judge each change
against it, as sqlive does, but they keep results and have no joins.

| Project | How |
|---|---|
| [Firestore client SDK](https://raw.githubusercontent.com/firebase/firebase-js-sdk/main/packages/firestore/src/core/view.ts) | Compares each change with the window's limit edges and refills from its local cache. |
| [Meteor's oplog observer](https://github.com/meteor/docs/blob/master/long-form/oplog-observe-driver.md) | Keeps the published set and a heap of rows past the window. A query with skip or limit but no sort falls back to polling. |
| [RxDB event-reduce](https://github.com/pubkey/event-reduce) | Computes the new result from the old one and one event; reruns when a row must move into the window. |
| [RethinkDB changefeeds](https://rethinkdb.com/docs/changefeeds/javascript/) | `orderBy.limit` changefeeds need an index and no filter after the limit. |
| [TanStack DB](https://tanstack.com/db/latest/docs/contributing/oracle-reviews/perf-ordered-window-repair) | Repairs a bounded window from indexed sources; a joined filter falls back to a full read. |

## Incremental view maintenance

These keep each query's result, or the state needed to update it, and apply
each write's changes to it. They update instead of rerunning, and their memory
grows with the data.

| Project | Notes |
|---|---|
| [DBSP / Feldera](https://docs.rs/crate/dbsp/latest/source/src/operator/group/topk.rs) | Rust. Has top-k operators; keeps integrated input state. |
| [Materialize](https://materialize.com/docs/transform-data/idiomatic-materialize-sql/top-k/) | Rust, on differential dataflow. Top-k with memory hints. |
| [Noria](https://www.usenix.org/conference/osdi18/presentation/gjengset) / [ReadySet](https://readyset.io/docs/reference/features/queries) | Rust. Partially stateful dataflow: state is evicted and rebuilt on demand. ReadySet lists ORDER BY and LIMIT as supported. |
| [Turso materialized views](https://docs.turso.tech/sql-reference/statements/create-materialized-view) | Rust. DBSP inside SQLite's transaction; experimental. |
| [pg_ivm](https://github.com/sraoss/pg_ivm) | C, Postgres triggers. ORDER BY and LIMIT unsupported. |
| [Electric Circuits](https://electric.ax/blog/2026/07/17/electric-circuits) | On DBSP; keeps keys, not rows. Research preview. |

## Coarser invalidation

These rerun or notify at table, column or collection level, or poll.

| Project | Granularity |
|---|---|
| [Room](https://developer.android.com/reference/androidx/room/InvalidationTracker), [Drift](https://drift.simonbinder.eu/dart_api/streams/), SQLDelight, [LiveStore](https://docs.livestore.dev/reference/reactivity-system/), [Riffle](https://riffle.systems/essays/prelude/), [sqlite-watcher](https://docs.rs/sqlite-watcher/latest/sqlite_watcher/) | Table |
| [cr-sqlite rx-tbl](https://vlcn.io/docs/cr-sqlite/js/reactivity) | Table, or one row by rowid |
| [PowerSync](https://docs.powersync.com/client-sdks/watch-queries.md) | Table, then a diff of the result sets |
| [resqlite](https://danreynolds.github.io/resqlite/blog/stories/2026-04-30-column-level-reactivity.html) | Column for updates (authorizer and pre-update hook); table for inserts and deletes |
| [Dexie liveQuery](https://dexie.org/docs/liveQuery()) | Key ranges per index, plus the returned rows' keys; no limit narrowing |
| [Electric shapes](https://electric.ax/docs/guides/shapes) | WHERE clauses matched against changes, indexed by constant values; no ORDER BY, LIMIT or joins |
| [PostGraphile live queries](https://postgraphile.org/postgraphile/4/live-queries/) | A relevant record or collection change; reruns the whole query |
| [Supabase Postgres Changes](https://supabase.com/docs/guides/realtime/postgres-changes) | Row events with one AND filter |
| [Hasura](https://raw.githubusercontent.com/hasura/graphql-engine/master/architecture/live-queries.md) | Polling, with parameterized queries multiplexed into one; its design notes call mapping events to queries "intractable for anything complicated" |
| [Instant](https://www.instantdb.com/essays/architecture) | WAL topics matched against query topics in a tree; window and join handling not documented |
| [homestead](https://docs.rs/homestead/latest/homestead/) | Rust, rusqlite, 0.1.2 published 2026-11-18; mechanism not documented |

*Unconfirmed:* how SQLDelight, WatermelonDB, Evolu, Triplit, Jazz and Parse
LiveQuery decide what to rerun.

## Papers

- Blakeley, Coburn and Larson, [Updating derived relations: detecting
  irrelevant and autonomously computable updates](https://www.vldb.org/conf/1986/P457.PDF)
  (VLDB 1986). The exact condition for a select-project-join query, which
  sqlive's scopes approximate.
- Levy and Sagiv, [Queries independent of updates](https://www.vldb.org/conf/1993/P171.PDF)
  (VLDB 1993).
- Yi et al., [Efficient maintenance of materialized top-k
  views](https://home.cse.ust.hk/~yike/topk/icde03.pdf) (ICDE 2003). Deletes
  inside a window force a refill; keeping between k and kmax rows avoids it.

## What sqlive takes from them

sqlive takes each idea below on before its first release.

- **Index live queries by their constants.** sqlive judges each write against
  every waiting query (`Handle::judge`). Electric indexes shapes by their
  constant values and Instant matches topics in a tree, so an equality or IN
  scope stops costing per live query.
- **Narrow a LIMIT for each parent.** Zero keeps a `Take` bound per partition;
  a correlated subquery with ORDER BY … LIMIT could narrow per parent row.
- **Test precision, and test the tests.** TanStack DB checks a plain sorted
  array model, a bound on rows read per change, and mutants of its engine.
  sqlive's random scopes and Lean proofs check that no needed rerun is missed;
  a count of unneeded reruns and mutation testing would add precision and
  strength.
- **Judge a transaction's net change.** Oracle's guaranteed mode sends nothing
  for two updates that cancel out.
