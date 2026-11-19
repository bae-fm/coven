# Live queries that rerun only when they must

*Draft. The survey behind it is [notes/sqlive-prior-art.md](../../notes/sqlive-prior-art.md),
done 2026-11-19 from each project's docs and source. sqlive is the crate at `crates/sqlive`.*

Other titles to consider: "Which writes change which queries?", "The last row is enough",
"What Oracle knew in 2007".

## The problem

bae is a music library app. Its library is one SQLite database on each device, and every screen
shows the rows of a SQL query: the album grid, an artist's page, the import list. When the library
changes, each screen has to change with it.

The simple way is to run every screen's query again after every write. That works until the
library is large and the writes are many. A scan of 100,000 folders writes one row per folder, and
each write would rerun the album grid, the import list's counts and every other open screen.

Most libraries that offer live queries over SQLite do a little better: they rerun a query when a
write touches a table it reads. Android's Room, Dart's Drift, SQLDelight, LiveStore and others work
this way. It is simple and never misses a change. But the album grid reads the albums table, and
so does almost everything else, so almost every write still reruns almost every query.

What we wanted was the exact condition: rerun a query only when a write could change its result.
And we wanted it for the SQL bae actually writes, with its own schema, not for a query language
or schema shaped for the tool.

## What sqlive does

sqlive works out a scope for each subscribed query: the rows a write must touch to matter.

- A **WHERE** clause narrows it: the import list's Skipped tab can't change when the scan adds a folder to Found.
- A **join** narrows it too: sqlive judges a write to a joined table with bounded lookups at write
  time, to see whether it reaches a row of the result.
- An **ORDER BY … LIMIT** window narrows it most. The album grid reads a window of albums sorted by
  title, say 60 of them. Once the query returned a full 60 rows, a write that touches an album
  sorting after the 60th can't change what the window shows. So sqlive keeps the last row's sort values and passes
  every write that sorts after them.

sqlive keeps no results. For each query it holds the parameters and, for a window, the last row's
sort values. When a write does matter, SQLite runs the query again.

## Who else does this

We surveyed the projects that tell an app when a query's result changes. Three groups stand out.

**Databases, long ago.** Oracle has offered Continuous Query Notification since 11g: an app
registers a query, and Oracle calls back when its result may have changed. Its guaranteed mode
covers single-table queries and some inner equijoins; a query with ORDER BY falls back to best
effort, and row limits aren't mentioned. SQL Server's Query Notifications forbid TOP, outer joins
and subqueries outright. So the idea of notifying on the result rather than the table is nearly
twenty years old, but only for simple queries. The exact condition itself is older still:
Blakeley, Coburn and Larson worked out when an update is irrelevant to a select-project-join view
in 1986.

**The last row of a window.** Several systems keep a window's last row and judge each change
against it: Firestore's client SDK, Meteor's oplog observer, RxDB's event-reduce, RethinkDB's
`orderBy.limit` changefeeds. All of them keep the results in memory, and all of them work on one
collection, with no joins. Zero, from Rocicorp, goes furthest: its `Take` operator keeps "a bound
of the last item it has accepted" for each partition, and a `TakeGate` stops a join's parent fetch
at that bound. Zero maintains results incrementally over a SQLite replica of Postgres, in its own
query language, as a server product. Convex arrives at the same window from the other side: it
records the index interval each query read, ending at the last key the query consumed, and reruns
when a commit overlaps it. A `take(n)` gets narrowed for free.

**Incremental view maintenance.** Materialize, Feldera, ReadySet and Turso's new materialized
views don't rerun at all: they keep a query's result, or the state needed to update it, and apply
each write's changes. That reads almost nothing after a write, but the state grows with the data,
and it has to stay in memory for every live query.

## Where sqlive sits

We found no project that takes arbitrary SQL over an app's own SQLite file, narrows each query by
its WHERE, its joins and its ORDER BY … LIMIT window, and keeps nothing but parameters and a last
row. The closest technique is Zero's; the closest library is GRDB, which narrows raw SQL to the
tables and columns it reads.

The survey also gave us four things to take on before sqlive's first release:

- **Index live queries by their constants.** Today each write is judged against every waiting
  query. Electric indexes its shapes by their constant values, so a write is judged only against
  the queries whose equality or IN values it could match.
- **Narrow a LIMIT for each parent.** Zero keeps a bound per partition. A correlated subquery with
  ORDER BY … LIMIT could narrow per parent row the same way.
- **Test precision, not only soundness.** sqlive's random tests and Lean proofs show that no needed
  rerun is missed. TanStack DB also counts the rows each change reads and runs its tests against
  deliberately broken versions of its engine. Counting unneeded reruns and mutation testing would
  show how precise sqlive is and how strong its tests are.
- **Judge a transaction's net change.** Oracle sends nothing when two updates in one transaction
  cancel out.

## Open questions for the post

- Numbers: reruns per write in bae's import scan with table-level invalidation versus sqlive's
  scopes.
- A diagram of one window: the 60 rows, the last row, and the writes on each side of it.
