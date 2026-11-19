# Live query narrowing

The Lean package in [crates/sqlive/proofs](../../crates/sqlive/proofs/) models
how sqlive narrows a live query to the rows its statements can depend on (§4),
and which writes therefore run the query again. The package states a statement
as sqlive's planner reads it, ports the planner, and proves each rule's step:
the rows a rule leaves out of a table's scope are rows the statement's result
doesn't depend on. A differential test runs sqlive, the model and SQLite on
random statements and writes and compares all three.

## The model

[Syntax.lean](../../crates/sqlive/proofs/SqliveNarrowing/Syntax.lean) states a
statement as the planner numbers it: its FROM items, the SELECT cores that hold
them, the queries whose arms the cores are, and the conditions on each core's
rows, each in the WHERE or in a LEFT JOIN's ON. A join's USING stands for the
equality of each column it names with the column of that name of the leftmost
item before it that has one, as SQLite compares them. A subquery is a query
number, so an expression names it without nesting. COALESCE of more than two
arguments nests in its second. A CASE with one WHEN and an ELSE, which is also
how the model reads `iif`, and `IS NULL` as a value stand for the expressions
that can be other than NULL where every column they name is NULL. A call of
`max` with two
arguments is SQLite's scalar max, an expression of one row, which the aggregate
`max` of one argument is not. A SELECT DISTINCT keeps the first of each set of
its rows that are equal. A value is NULL or an integer, and
the differential's tables hold only those, in columns SQLite compares without
converting a value.

[Semantics.lean](../../crates/sqlive/proofs/SqliveNarrowing/Semantics.lean)
evaluates a statement on a database whose tables hold rows by rowid slot. A
core joins its items left to right, keeps the bindings its conditions hold for,
and makes a row of each binding or of each group. A LEFT JOIN gives a left row
that no right row joins a row of NULLs. A core that sits in no other core, as a
statement's own, a view's, or a FROM subquery's of such a core, sees no row
around it.

[Planner.lean](../../crates/sqlive/proofs/SqliveNarrowing/Planner.lean) ports
sqlive's planner (`scope_plan.rs`). Each core's conditions give terms on one
item's rows and edges between items; the planner reads the conditions that a
row of NULLs fails first, holds back what a LEFT JOIN's ON says of other items
until a WHERE condition makes the join an inner join's, carries each item's
terms on a column an equality compares to the item on the equality's other
side, passes a derived item's terms into its arms, and places the items so
each takes values from items placed before it. A run binds each item's scope; a table's scope passes a row
that any item of the table passes.

[Changes.lean](../../crates/sqlive/proofs/SqliveNarrowing/Changes.lean) and
[Judge.lean](../../crates/sqlive/proofs/SqliveNarrowing/Judge.lean) judge a
write as sqlive does. The write's row changes are one per row that each of its
statements changed, as sqlive's pre-update hook records them. A change reruns
the query when it inserts or deletes a row of a table the statement reads, or
changes a column the statement reads, and the row before or after passes the
table's scope. A join's lookups read the database the write leaves. The model
takes the columns a statement reads to be the columns its clauses name, and
every column of each table that a query whose rows SQLite picks reads; the
differential checks that sqlive reads the same ones from SQLite.

[WellFormed.lean](../../crates/sqlive/proofs/SqliveNarrowing/WellFormed.lean)
states what the runner's JSON reader guarantees of every statement it reads
(`Statement.wellFormed`). The root is a query, and each query's arms are
cores. A query has a higher number than every query its arms read, as FROM
items or subqueries, because the reader numbers a query after everything it
holds. Each item a core lists belongs to that core. A derived item that the
planner reads alone has a query that a lookup can read alone: its arms sit in
no other core, or its query names nothing outside it. A LEFT JOIN's ON names
only the item the join adds, items before it in the FROM, and items of the
cores around its own. Every column a core's clauses name belongs to an item of
the core or of a core around it. The proof that puts the rules together
assumes these facts, and the differential checks that every statement it
generates has them.

## Each rule's theorem

Each rule is one file under
[Rules](../../crates/sqlive/proofs/SqliveNarrowing/Rules/), with its reader or
step and its theorem.

A condition's reader keeps a promise (`ReadCondition.Sound` in
[Reader.lean](../../crates/sqlive/proofs/SqliveNarrowing/Reader.lean)): when the
condition holds of a binding, whatever its subqueries return, each term it
reads holds of its item's row, and each edge's member column holds a value
that its partner's row in the binding gives. For a subquery's source, the
partner's row is in a binding of the subquery's core under the same binding.
`readCondition_sound` proves that the narrowing a condition gives keeps the
promise of whichever rule reads it.

- **A condition on one item's own columns.** `term_sound` proves the promise
  for comparisons with a constant, IN lists, NULL tests, and AND and OR of
  these, and for any other condition on one table's columns, which SQLite
  judges row by row.
- **An equality between two items.** `edge_sound` proves that `a.x = b.y`, true
  of a binding, makes `a.x` hold a value that `b`'s row gives, neither NULL,
  in either direction and for an expression over `b`'s columns. The reader
  takes an expression over an item a LEFT JOIN adds only when it is strict:
  each operation in it gives NULL where an operand is NULL. `eval_null`
  proves that a strict expression is NULL where `b` has no row, so the
  equality is false there. `nonstrict_holds_without_partner` shows why the
  reader needs that: with COALESCE, a CASE or `IS NULL`, the equality can hold
  where `b` has no row, as a LEFT JOIN's missing row, and no lookup finds a
  row that gives the value. Over any other item, the edge's promise allows a
  binding with no row of `b`, as an `IS` edge's does: no binding a core keeps
  lacks one.
- **A row value equality between two items.** `row_edge_sound` proves the
  same of each pair of `(a.x, a.y) = (b.z, b.w)`, which holds only where
  every pair is equal.
- **`IS` between two items.** `is_edge_sound` proves that `a.x IS b.y`, true
  of a binding, makes `a.x` hold a value that `b`'s row gives, or makes both
  NULL, `b`'s column NULL in its row or for want of one. The partner is never
  an item a LEFT JOIN adds, so a binding a core keeps holds its row, and a
  lookup that asks for a NULL finds it. An `IS` edge can hold where its
  member is NULL, so it never makes a LEFT JOIN an inner join's, and an
  equality on `IS` carries no term from a derived partner's arms, whose NULL
  says nothing of their items.
- **An OR whose sides narrow one item.** `or_sound` proves that an OR, true
  of a binding, makes one of its sides true, and with it every term and edge
  that the other rules read from that side's conjuncts. An item gets a
  disjunction when every side narrows it, so its scope can be the OR of the
  sides' scopes. The OR reads its sides with the other rules, so it is read
  after them, and not as one of `conditionRules`. The planner keeps a
  disjunction for an item only when every partner of its sides' edges is
  placed before the item.
- **A value that is one of several partners' values.** `one_of_sound` proves
  that `m.c = COALESCE(a, b, ...)`, true of a binding, makes `m.c` hold the
  value of one of the arguments, and that `m.c = (SELECT ...)` or `m.c IN
  (SELECT ...)` over a compound makes it hold the value one arm returns in a
  binding the arm keeps. Each argument or arm gives one side of a
  disjunction: an edge, a subquery's edge or the column equal to a constant.
  An argument that is an expression over an item a LEFT JOIN adds is strict,
  as an equality's is, and an equality with any other expression over a
  COALESCE gives no edge from such an item. The rule gives disjunctions, so
  it is not one of `conditionRules`: the model reads it before them, as
  sqlive does.
- **A column compared with a subquery's value.** `subquery_edge_sound` proves
  that `x.c = (SELECT y.d ...)` or `x.c IN (SELECT y.d ...)`, true of a
  binding, makes `x.c` hold the value `y.d` holds in a binding the subquery's
  core keeps.
- **A row value compared with a subquery's row.** `row_subquery_edge_sound`
  proves the same of each pair of `(x.a, x.b) = (SELECT y.c, y.d ...)`, the
  subquery on either side, and of `(x.a, x.b) IN (SELECT y.c, y.d ...)`, since
  a row value comparison holds only where every pair is equal.
- **A condition passed into a FROM subquery, CTE or view.** `push_down_sound`
  proves that a row of a derived item's query is a row of one of its arms, and
  that a row of an arm that doesn't aggregate holds, in each column that is a
  bare column of one of the arm's items, that item's value. `renamed_sound`
  proves that a term true of the derived row is then true of the item's row.
- **A condition carried across an equality.** `transfer_sound` proves that
  when `a.x = b.y` holds of a binding, a term true of `b`'s row on `y` is
  true of `a`'s row on `x`. The planner adds such a term to `a`'s scope,
  whichever of the two items it places first, so `JOIN b ON a.x = b.y WHERE
  b.y = 5 AND a.z = 1` narrows `a` by `a.x = 5` too, though `a.z = 1` makes
  `a` the item the planner places first. The carried term decides nothing
  of that order, so an item that only carried terms narrow still takes
  values from the items it is joined to. A derived item's own terms on a
  column include what its arms say of it: `pulled_sound` proves that a row
  of a derived item's query whose column isn't NULL passes, joined with OR
  across the arms, the term of the item each arm passes the column on from,
  when no arm aggregates or calls a window function. A LIMIT on the query
  keeps only rows its arms give, so the term holds under one too.
- **A view or FROM subquery read alone.** `view_sound` proves that a query
  whose arms sit in no other core returns the same rows under every binding
  around it, so a lookup that reads it alone reads the rows the statement
  reads. `closed_eval` proves that queries whose cores name only their own
  items, read no other query and hold no other subquery return under a
  binding the rows they return under its rows of their own items, and
  `closed_sound` that a FROM subquery or CTE of such queries, inside an
  expression subquery, returns under a binding of none of its items the rows
  a lookup reads alone.
- **The LIMIT bound.** `limit_sound` proves that when a run returned as many
  rows as its LIMIT, rows that sort after its last row change nothing in the
  rows it returns. Each ORDER BY term sorts one item's rows by a key: one of
  its columns, which sqlive compares itself, or an expression over its
  columns with no subquery, which sqlive asks SQLite to evaluate on each row
  a write changes. `sortKey_value` proves that in a binding where the term's
  item has a row, the term's result column holds the key's value in that row.
  The terms fall into runs of consecutive terms over one item.
  `sortedBefore_passes` proves that a run's scope passes exactly the rows
  whose keys sort before the last row's values of the run, NULL first
  ascending and last descending, or tie with them and pass a given scope, and
  `atOrBefore_passes` that the bound of a run passes exactly the rows whose
  keys sort at or before those values. An item's bound is its first run's,
  or, for each earlier run, a lookup of a row of that run's item that sorts
  before the run's values, through the first of the item's memberships that
  takes values from that item; an item without such a membership for each
  earlier run has no bound. `runBound_passes` proves that in a binding whose
  ORDER BY values sort at or before the last row's, each item's bound passes
  its row, when each lookup passes the item's row wherever the partner's row
  passes the lookup's scope, and `joined_passes` proves that a membership's
  lookup does, in a binding where its edge holds.
- **The rows SQLite picks.** A query that keeps some of its rows, by a LIMIT
  or an OFFSET or as a subquery whose first row a comparison takes, keeps the
  rows SQLite reads first, in the order of the plan it chooses.
  `post_picks_any` proves that with no ORDER BY, some order of the rows puts
  any of them among the rows the query keeps, so an index on a column no
  clause names can decide which rows those are. `fullyOrders_sound` proves
  that a query that aggregates with no GROUP BY, and so returns one row, or
  whose ORDER BY sorts by the rowid of each of its tables, keeps the same rows
  in every order, on a database whose tables hold each row's rowid in its
  first column, never NULL and different in each row.
  A SELECT that aggregates picks rows too. `bare_picks_any` proves that for
  any row of a group, some order of the group's rows gives that row's value to
  a result column outside every GROUP BY term, and `concat_follows_order`
  proves that `group_concat` with no ORDER BY of its own joins two values in
  the order SQLite reads them. `max_keeps_first` proves, over any comparison,
  that among two values that compare equal `max` keeps the one it reads
  first, so values that compare equal and differ, as `1` and `1.0`, give `max`
  a different value in each order. The model's values are integers, which
  compare equal only when they are the same, so the model counts a `max`
  as following the order unless its argument is a column whose equal values
  are the same. `distinct_keeps_first` proves the same of a GROUP BY term,
  which keeps one of each set of values that compare equal, the one it reads
  first, so the model counts a SELECT whose result column shows a GROUP BY
  term as following the order unless the term is such a column. DISTINCT,
  UNION, INTERSECT and EXCEPT keep one of each set of rows that compare equal
  in the same way: DISTINCT the first it reads, and a compound the first or
  the last as its plan decides. So the model counts a SELECT DISTINCT, and a
  compound, as following the order unless each result column of each of its
  SELECTs is such a column. A SELECT that calls a window function picks rows too:
  `rowNumber_follows_order` proves that `row_number()` whose window has no
  ORDER BY numbers two rows in the order SQLite reads them.
  `orderFree_rows_perm` proves that a SELECT with none of these, whose
  `group_concat` calls and windows each sort by the rowid of each of its
  tables, gives the same rows in some order from every order of its rows.
  `picked_column_reruns` proves that a change to any column of a table that a
  query of the first kind, or a SELECT whose rows follow the order SQLite reads
  them in, reads runs the query again, when the row before or after passes the
  table's scope.
- **The whole read.** `whole_sound` proves that with no scope, every change of
  a column the statement reads runs the query again.

## What the proofs don't cover

The theorems prove each rule's step. No theorem yet proves that the planner
puts the steps together soundly: that the order in which it places items, the
ON conditions it holds back, the terms it carries across equalities, the terms
it passes into derived items and the lookups of each join's partner give scopes outside of which a write leaves the
result unchanged. The differential checks that composition against SQLite on
every statement it generates.

The model leaves out text, real and blob values, collations and the
conversions an affinity makes: sqlive's checks that refuse them are modelled as
refusals. The model has no generated columns: sqlive sorts by a generated
column's expression, which the model states as an expression term. The
differential's tables hold no generated column: sqlive records no value of
one for a write and counts it as changed by every write to its row, which the
model doesn't state. The model also leaves out bound parameters, table functions, the views sqlive
reads whole as a join's partner, the terms that compare a NULL test's truth or
a column `IS` a parameter, and the step limit on a lookup.

## The runner and the differential

`narrowingRunner` reads one case as JSON on stdin: the schema (each table's
columns and their affinities, and each view's query), the statement as a tree
that names its sources by alias, the database before and after the write, and
the write's row changes. The runner reads a CTE where a query names it, so a
CTE that no query reads, which SQLite never runs, adds nothing to the model's
statement. The runner writes whether the write reruns the statement,
whether the statement is well formed, whether the model reads the statement whole, each table's scope, and the
statement's rows before and after.

The differential in sqlive (`narrowing_model` among the live judging tests)
has one test per rule, `<rule>_matches_lean`, and `mixed_shapes_match_lean`
for statements that hold several rules. Each rule's generator writes
statements of its rule's shape. The CTEs' generator also writes, beside the
CTEs a statement reads, one that no query reads, with a LIMIT or a USING
join. For each statement the test opens a live query and makes random writes. After each write it fails when the model finds the
statement not well formed, when sqlive and the model disagree on whether the write reruns the query, when a write that either lets
pass changed SQLite's rows, or when the model's rows differ from SQLite's.
Under a LIMIT or a scalar subquery, SQLite may pick other rows than the model
among rows that sort the same, and an aggregate's column outside every GROUP BY
term, or a `group_concat` or a window with no ORDER BY that sorts by each
table's rowid, may take other rows' values. The test compares the rows there only through
the reruns. The model's values are integers, so the differential reads
`group_concat` as `CAST(group_concat(e, '') AS INTEGER)`, the joined digits read
back as one integer, which the model computes the same way.

SQLite's authorizer reports no read of the columns a USING or NATURAL join
compares, nor of the columns that decide which rows a query whose rows SQLite
picks keeps, or which row an aggregate takes a value from, so sqlive adds those
columns to a statement's reads (§4), and the
model's reads and sqlive's agree.
