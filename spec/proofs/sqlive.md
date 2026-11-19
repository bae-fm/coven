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
table's scope. sqlive keeps of a changed row only the values its scopes test,
so the model tests the row masked to the columns the statement reads: their
values, and NULL in every other column. A join's lookups read the database the
write leaves, which the model masks the same way. The model takes the columns
a statement reads to be the columns its clauses name, and every column of
each table that a query whose rows SQLite picks reads; the differential checks
that sqlive reads the same ones from SQLite.

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
the core or of a core around it. The statement is a tree: each core lists each
item once, one place reads each query, no core reads the statement's own
query, and each core is an arm of one query, since the reader gives each FROM
item, CTE reference and subquery a query of its own. The cores sit in one
another as the queries hold one another: the statement's own query sits in no
core, each core sits in an arm of a query numbered after its own, a
subquery's arms sit in the core that holds it, and a derived item's query's
arms sit in no core, as a view's do, or in the core around the item's core,
as a FROM subquery's or a CTE's do. The subqueries of a LEFT JOIN's ON name,
of the ON's own core, only the item the join adds and items before it.
`whole_narrowing_sound`, `evalQuery_fuel` and the restriction below assume
these facts, and the differential checks that every statement it generates
has them.

## Each rule's theorem

Each rule is one file under
[Rules](../../crates/sqlive/proofs/SqliveNarrowing/Rules/), with its reader or
step and its theorem.

A condition's reader keeps a promise (`ReadCondition.Sound` in
[Reader.lean](../../crates/sqlive/proofs/SqliveNarrowing/Reader.lean)): when the
condition holds of a binding, whatever its subqueries return, each term it
reads holds of its item's row, and each edge's member column holds a value
that its partner's row in the binding gives. A subquery's source sits in an
arm of one of the condition's subqueries, and its row is in a binding of that
arm under the same binding.
Any other partner is an item the condition names, so a binding that holds a
row of each item a condition names, as each binding a core keeps does of its
items that no LEFT JOIN adds, holds a row of the partner. An edge's member
value is NULL where the member has no row, unless no LEFT JOIN adds the
member. A reader that reads ORs promises that each side narrows its item with
a term or an edge. `readCondition_sound` proves that the narrowing a condition
gives keeps the promise of whichever rule reads it.

The planner makes a LEFT JOIN an inner join's when a condition outside its ON
narrows its item by a part that a row of NULLs fails. `holds_none` proves
that such a term is false of a missing row. `HoldsIn.bound` proves that an
edge that compares by `=`, true of a binding with no row of its member, has
a member that no LEFT JOIN adds, or a partner outside a subquery that no LEFT
JOIN adds and that the binding has no row of. `Disjunction.HoldsIn.bound`
proves the same of an OR that a row of NULLs fails, through the side that
holds. A binding a core keeps holds a row of each of its items that no LEFT
JOIN adds, so where such a part holds, its item has a row.

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
- **An expression of the member's own row.** The member of an equality can
  be an expression over one table item's own columns, as `CASE WHEN a.k = 1
  THEN a.x ELSE a.y END = b.z`. `row_member_sound` proves that the member's
  value in a binding's row of its item is the value the expression has in
  the binding, so `edge_sound` holds of such a member as of a column: where
  the equality holds, the item's row gives a value some row of `b` gives. A
  write asks SQLite for the expression's value in each row it changes. The
  reader takes an expression over an item a LEFT JOIN adds only when it is
  strict, as it does an expression over the partner, since the equality
  could otherwise hold of the NULLs a missing right row gives and make the
  LEFT JOIN an inner join's wrongly.
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
- **Two CTEs of one name.** A WITH inside a subquery, a CTE or a view can
  declare a CTE under the name of one around it. `resolve_inner` proves
  that the runner reads a FROM item naming a CTE as the innermost CTE of
  that name visible where the item is, whose query names the CTEs of its
  own WITH and of the WITHs around it. `resolve_outer` proves that a WITH
  that doesn't declare a name changes nothing of what the name names inside
  it, so an outer CTE's query reads the outer CTEs even where an inner WITH
  declares one of their names. The model's statement numbers each query, so
  its rules read the CTE each FROM item names, and the differential checks
  that its rows are SQLite's.
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

## Putting the rules together

Some steps of putting the rules together are proved, each with its own
theorem.

A statement's rows depend only on the columns it reads.
[Mask.lean](../../crates/sqlive/proofs/SqliveNarrowing/Mask.lean)
masks a database as sqlive keeps it: each row of a table the statement reads,
masked to the columns the statement reads, and no row of any other table.
`result_mask` proves that a statement whose cores list only items it has
returns the same rows from the masked database as from the database. So the
judge's lookups in the masked database read the rows the statement reads.

A write the judge lets pass leaves the rows each scope passes unchanged.
[Unjudged.lean](../../crates/sqlive/proofs/SqliveNarrowing/Unjudged.lean)
states a write's row changes as steps that each change one slot of a table,
from the change's row before to its row after (`Writes`). A slot can change
more than once in one write, as when one statement updates a row and the next
deletes it. `Writes.unjudged` proves that when the masked row of a slot ends
different from where it started, the slot's first and last rows each fail the
table's scope, as long as every change that changes a masked row has rows
before and after that fail it. `unjudged_rows` puts the two together: when the
judge lets a write pass, each masked table holds the same rows that pass the
table's scope before and after the write, in the same order. For a statement
sqlive reads whole, the scope passes every row, so the masked tables are the
same before and after, and `whole_narrowing_sound` proves that the write
leaves the statement's rows unchanged.

A lookup of a derived item reads it alone, with more fuel than the statement
has left where the item sits.
[Fuel.lean](../../crates/sqlive/proofs/SqliveNarrowing/Fuel.lean) proves that the
extra fuel changes nothing: `evalQuery_fuel` proves that a well-formed statement's
query returns the same rows with any fuel past its number, and
`lookupRows_derived` that a lookup of a derived item whose arms sit in no core
reads the rows the statement reads of it.

Two databases that hold the same rows passing each item's scope give the
statement the same rows, as long as every binding that decides those rows
holds only rows that pass their scopes.
[Restrict.lean](../../crates/sqlive/proofs/SqliveNarrowing/Restrict.lean)
proves this from a promise that it states apart from the planner (`Kept`):
each binding a core keeps that decides the rows its query returns holds rows
that pass their items' tests, and holds no row of a LEFT JOIN item only when
that item's ON can't hold for a row that fails its test. A binding of a
derived item's arm decides the derived item's rows when its row passes the
derived item's test; a binding of an arm that aggregates or calls a window
function always decides them. `joinItems_restricted` and
`coreBindings_restricted` prove that a core then keeps the same such bindings
from both databases: a join reads the same rows that pass each item's test,
and the ON and WHERE conditions read the same subquery rows under a binding
whose rows pass their tests. Such a binding holds a row of each item that no
LEFT JOIN adds and that the join has reached: an ON reads its subqueries
where the join has reached the ON's item, and the WHERE where it has joined
every item. The promise for a LEFT JOIN item's ON takes only such a
binding. `queryRows_restricted` carries that through
DISTINCT, UNION, EXCEPT, INTERSECT and the ORDER BY of a query with no LIMIT,
`agree_of_kept` through every nested query at every fuel, and
`result_restricted` to the statement's rows when every row of its own query
matters. `scoped_rows` gives the restriction its tables: when the judge lets
a write pass, each masked table holds the same rows that pass the scope of
an item reading it, before and after the write.

A query's cores, and the cores of the queries they read, name items of their
own and items of the cores around them.
[Outer.lean](../../crates/sqlive/proofs/SqliveNarrowing/Outer.lean) states the
bindings a run of a query reads those outer items' rows from: a binding that
holds a row of each outer item the query names that no LEFT JOIN adds
(`Statement.Outer`). `outer_root` proves that the statement's own query names
no outer item. `outer_derived`, `outer_sub` and `outer_subOn` prove that a
core run under such a binding runs its derived items' queries and its
subqueries under such bindings too: a derived item's query under the binding
the core's items join onto, a subquery of the WHERE or of a result column
under a binding that holds a row of each of the core's items that no LEFT
JOIN adds, and a subquery of an ON under a binding that holds a row of each
such item up to the ON's own. The restriction compares each query's runs
under these bindings.

A statement with a LIMIT bound needs only the bindings of its own core whose
rows sort at or before the last row the run before the write returned.
`limit_restricted` proves that when the statement's own query is one SELECT
with an ORDER BY, a LIMIT and no OFFSET, and its run returned as many rows as
the LIMIT, the statement returns the same rows from both databases, as long as
those bindings, and the bindings of every other query, keep the promise: the
two runs return the same rows that sort at or before the last row, and
`limit_sound` gives the same first rows.

The promise rests on what makes a row pass the scope the planner gives an
item. [Scopes.lean](../../crates/sqlive/proofs/SqliveNarrowing/Scopes.lean)
states it. `Planned.scopes_eq` proves that a placed item's scope reads the
scopes of the items placed before it, which keep those scopes to the end.
`scopeAt_passes` proves that the scope passes a row of which the item's terms
and what its edges carry hold, that each of its memberships' scopes passes, that
a side of each OR it keeps passes, and that its LIMIT bound passes.
`membershipScope_passes` proves that a membership's scope passes a row whose
value a row of the partner in the partner's scope gives, NULL only for an `IS`
edge, through `equalValues_passes` when the partner's scope lists its values;
`sideScope_passes` and `bind_passes` do the same for an OR's side and a term.
[PlannerState.lean](../../crates/sqlive/proofs/SqliveNarrowing/PlannerState.lean)
states what the planner's state keeps however it reads the conditions.
`plan_leftOn` proves that the planned state holds an ON narrowing back only
for an item a LEFT JOIN adds: the planner starts by holding one back for
each such item, and each step that makes an item an inner join's releases
it for good. The restriction lets a binding hold no row only of such an
item. `plan_order` proves that the planner places each item once, and every
item of every core: each step places one item that is not placed yet, and
the planner takes a step for each item. `Planned.scopes_eq` takes both.
`plan_pushed` proves that each edge the planner passes into a derived item's
arms comes from an edge the derived item holds, as a membership or as an edge
passed into it, through the result column that an arm passes the target's
column on to unchanged, and that the target is an inner join's. The planner
keeps the edges it passes in a list of their own (`PState.pushed`), apart
from the edges conditions give, and no step of the planner reads that list.
`plan_nestedOK` proves that each edge of the planned state whose partner is
a subquery's source has its partner in an arm of some core's subquery, as
each condition's reader promises, because the planner changes only an
edge's member when it passes the edge into a derived item's arm, and that
each item's memberships are edges from it. `plan_placedBefore` proves that
each item's memberships, and the sides of each OR it keeps, take values from
items the planner placed before it: the planner sets them only as it places
the item, from the edges and sides whose partners it placed before, and no
other step changes them.

[Tests.lean](../../crates/sqlive/proofs/SqliveNarrowing/Tests.lean) states the
tests the restriction takes from the planner's scopes. An item's row passes
its test when it passes the item's scope, with lookups in what sqlive keeps
of the database the write leaves, and the scope of each edge the planner
passed into the item, and, for an item a lookup reads alone, is among the
rows the lookup reads. A query's row matters when it passes the scope and
passed edges of the derived item that reads the query, and every row of a
query with a LIMIT or an OFFSET, or that no derived item reads, matters.
`plan_restriction` proves that when the judge lets a write pass, the tables
before and after the write hold the same rows that pass each item's test,
and that each derived item's test keeps only rows that matter to its query,
since one place reads each query.
`Planned.bound_passes` proves the LIMIT bound's part of the promise: in a
binding of the statement's own core whose ORDER BY values sort at or before
the last row's, the bound of each item the ORDER BY sorts passes the item's
row, when the binding holds a row of each such item and each membership of
the item to another such item holds of the binding, with the partner's row
passing the partner's scope. The ORDER
BY sorts items of the statement's own core, which sits in no core, so no
such membership takes a subquery's value (`plan_nestedOK`), and
`joined_passes` gives each lookup the bound makes. The runs of the ORDER
BY's terms keep the terms in order, so a binding's values run by run are its
ORDER BY values.

Whether these tests keep the promise `Kept` is the step that remains: whether
the planner's state gives each item terms, edges and ORs that hold of the
bindings that matter, through the conditions it reads, the ON conditions it
holds back, the terms it carries and passes into derived items, and the order
it places items in.

## What the proofs don't cover

The theorems prove each rule's step and the steps of putting them together
that the section above names. For a statement sqlive doesn't read whole, no
theorem yet proves that a write the judge lets pass leaves the statement's
rows unchanged: that the order in which the planner places items, the ON
conditions it holds back, the terms it carries across equalities, the terms
it passes into derived items, the lookups of each join's partner and the
LIMIT bound give scopes that keep the promise `agree_of_kept` and
`limit_restricted` take. The
differential checks that composition against SQLite on every statement it
generates.

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
statement, and it reads the CTE that SQLite resolves the name to. The runner writes whether the write reruns the statement,
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
