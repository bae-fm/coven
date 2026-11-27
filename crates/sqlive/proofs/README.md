# Live query narrowing

The Lean package in this directory models how sqlive narrows a live query to the
rows its statements can depend on ([sqlive's spec](../SPEC.md), sections 4 to
8), and which writes therefore run the query again. The package states a
statement as sqlive's planner reads it, ports the planner, and proves each
rule's step: the rows a rule leaves out of a table's scope are rows the
statement's result doesn't depend on. `narrowing_sound` puts the steps together:
for a well-formed statement, a write the judge lets pass leaves the statement's
rows unchanged. sqlive judges with a scope for each place a statement names a
table, and `references_sound` states the same of that judge. A differential test runs sqlive, the model and SQLite on random
statements and writes and compares all three.

## The model

[Syntax.lean](SqliveNarrowing/Syntax.lean) states a
statement as the planner numbers it: its FROM items, each with its table's
unique keys and NOT NULL columns, or whether it is a FROM subquery written in
place, the SELECT cores that hold them, the queries whose arms the cores are, and the conditions on each core's
rows, each in the WHERE or in a LEFT JOIN's ON. A join's USING stands for the
equality of each column it names with the column of that name of the leftmost
item before it that has one, as SQLite compares them: where that column is a
table's with a collation other than BINARY, by that collation
(`Cond.collated`), which compares two values by their keys and which the
planner reads as any condition SQLite judges. The runner reads a NATURAL join's
columns the same way, from the list of them that the differential gives it. A
parenthesized join's
tables are items of the core it sits in. A subquery is a query
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

[Semantics.lean](SqliveNarrowing/Semantics.lean)
evaluates a statement on a database whose tables hold rows by rowid slot. A
core joins its items left to right, keeps the bindings its conditions hold for,
and makes a row of each binding or of each group. A LEFT JOIN gives a left row
that no right row joins a row of NULLs. A core that sits in no other core, as a
statement's own, a view's, or a FROM subquery's of such a core, sees no row
around it.

[Planner.lean](SqliveNarrowing/Planner.lean) ports
the planner of sqlive-reads (`scope_plan.rs`). Each core's conditions give terms on one
item's rows and edges between items; the planner reads the conditions that a
row of NULLs fails first, holds back what a LEFT JOIN's ON says of other items
until a WHERE condition makes the join an inner join's, carries each item's
terms on a column an equality compares to the item on the equality's other
side, passes a derived item's terms, equalities and ORs into its arms, and places the items so
each takes values from items placed before it. An item that takes a
subquery's value waits for the subquery's item, and so does each item a
waiting derived item could pass that value into. A run binds each item's
scope; a table's scope passes a row that any item of the table passes.

[Changes.lean](SqliveNarrowing/Changes.lean) and
[Judge.lean](SqliveNarrowing/Judge.lean) judge a
write as sqlive does. The write's row changes are one per row that each of its
statements changed, as sqlive's pre-update hook records them, and the judge
reads each row's net change over the write (below). A change reruns
the query when it inserts or deletes a row of a table the statement reads, or
changes a column the statement reads, and the row before or after passes the
table's scope. sqlive keeps of a changed row only the values of the columns
live queries read or test, so the model tests the row masked to the columns
the statement reads: their values, and NULL in every other column. A join's lookups read the database the
write leaves, which the model masks the same way. The model takes the columns
a statement reads to be the columns its clauses name, and every column of
each table that a query whose rows SQLite picks reads; the differential checks
that sqlive reads the same ones from SQLite.

sqlive stops each question it asks SQLite about a changed row, a lookup or a
condition SQLite judges, after 1,000 of SQLite's steps, so that a lookup no
index serves bounds a write's cost. sqlive can't tell then whether the row
passes its scope, so the statement narrows nothing for that write: every
change of a column it reads reruns it (`Statement.rerunsLimited`). Which
questions stop depends on the plan SQLite picks for them, which the model
doesn't state, so the runner takes from sqlive whether one of the write's
questions stopped.

[WellFormed.lean](SqliveNarrowing/WellFormed.lean)
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
[Rules](SqliveNarrowing/Rules/), with its reader or
step and its theorem.

A condition's reader keeps a promise (`ReadCondition.Sound` in
[Reader.lean](SqliveNarrowing/Reader.lean)): when the
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
  these, and for any other condition on one item's columns, which SQLite
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
  be an expression over one item's own columns, as `CASE WHEN a.k = 1
  THEN a.x ELSE a.y END = b.z`. `row_member_sound` proves that the member's
  value in a binding's row of its item is the value the expression has in
  the binding, so `edge_sound` holds of such a member as of a column: where
  the equality holds, the item's row gives a value some row of `b` gives. A
  write asks SQLite for the expression's value in each row it changes. The
  reader takes an expression over an item a LEFT JOIN adds only when it is
  strict, as it does an expression over the partner, since the equality
  could otherwise hold of the NULLs a missing right row gives and make the
  LEFT JOIN an inner join's wrongly.
- **A row value ordered against another.** `lead_sound`
  ([Rules/RowOrder.lean](SqliveNarrowing/Rules/RowOrder.lean)) proves that
  `(a, b) >= (?1, ?2)`, and the same by `<=`, `>` or `<`, is true only where
  the first pair orders that way or ties, in the pair's collation, so
  `terms_sound` gives the condition the terms of that comparison too, besides
  what the rules read of the whole of it (`RowOrder.terms`). The term reader
  reads an ordering of a column that declares a collation against a constant as
  a condition SQLite judges (`collatedOrder`). The model has no generated
  columns, whose orderings sqlive reads the same way.
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
- **An equality through a correlated FROM subquery or CTE.** A FROM
  subquery or CTE whose SQL names a row around it lists rows only beside that
  row, so no lookup reads it alone. When its query is one SELECT that neither
  aggregates nor calls a window function, and the compared column is a bare
  column of one of that SELECT's items, which a lookup reads alone
  (`passedSource`), an equality with the column gives an edge to that source
  item, through the derived item (`Edge.via`). The member is a column or an
  expression of the derived item's core joined to it, or a column compared
  with a subquery's value read from the derived item, as `c.x = (SELECT d.y
  FROM d)`, where the derived item sits in the subquery's one core.
  `orPassed_sound` and `edge_props` prove that such an edge, true of a
  binding, makes the member hold the value the derived item's row holds in
  the column, and `Passed.edge_holds` that the derived row sits in the
  binding, or in a binding of the subquery's core that gives the row the
  subquery returns. The derived item's row is the row of a binding its one
  arm keeps, whose source row holds the column's value
  (`derived_source_passes`). The planner places the member after the derived
  item and the source (`Edge.placedIn`, `plan_before`). A LEFT JOIN's ON
  holds back each such edge, and each OR with a side that holds one, until
  the join is an inner join's: the bindings before the join's item hold the
  derived item's row, but not the binding of its arm that gives the row.
- **A row value compared with a subquery's row.** `row_subquery_edge_sound`
  proves the same of each pair of `(x.a, x.b) = (SELECT y.c, y.d ...)`, the
  subquery on either side, and of `(x.a, x.b) IN (SELECT y.c, y.d ...)`, since
  a row value comparison holds only where every pair is equal.
- **An OR passed into a FROM subquery, CTE or view.** As the planner places a
  derived item, it passes each OR the item keeps into the items its arms pass
  columns on from, one level: an item that takes one keeps it, when it is
  placed after, and passes it on then (`PushDown.pushOrs`). An item takes one when no
  LEFT JOIN adds it and each side, rewritten to its columns, still has a term
  or an edge. The planner keeps the ORs it passes in a list of their own
  (`PState.pushedOrs`). `plan_pushedOrs` proves where each comes from, and
  `pushedOr_passes` that a row that matters to the derived item passes a side
  of the item's OR, so the target's row in its arm's binding passes that side
  rewritten.
- **A condition passed into a FROM subquery, CTE or view.** `push_down_sound`
  proves that a row of a derived item's query is a row of one of its arms, and
  that a row of an arm that doesn't aggregate holds, in each column that is a
  bare column of one of the arm's items, that item's value. `renamed_sound`
  proves that a term true of the derived row is then true of the item's row.
  A condition SQLite judges passes as the same condition over the item's
  columns (`Cond.passed`), when each column it names passes on from that item,
  and so does the value an edge computes from its member's row
  (`PValue.passed`): `judgedOn_sound` and `value_passed_sound` prove that
  each holds, or gives its value, on the item's row as on the derived item's.
  A value that isn't strict passes into no item a LEFT JOIN adds, and
  `value_passed_none` proves that a strict one is NULL where such an item has
  no row. sqlive keeps the condition's SQL and the names it reads: a write
  asks it over the changed row's values under those names, and a lookup over
  a SELECT that gives the table's columns those names (`scope_row.rs`).
  An arm that groups its rows or calls an aggregate passes on only the
  columns its GROUP BY lists as bare columns: `group_key_sound` proves that a
  row of such an arm is the row of a group of the bindings the arm keeps, and
  that every binding of the group holds the row's value in each such column.
  So a term true of the row is true of the item's row in every binding the
  row is made from, and a row of the item that fails the term is in a group
  whose row fails it. An arm that calls a window function passes on nothing.
- **A column nothing reads.** sqlive plans a statement without the columns
  of a subquery, CTE or view that nothing reads (`unread_columns.rs`). The
  model's `Statement.pruned` writes NULL for each, once for each column the
  statement has, since leaving one out can leave another unnamed, and then
  empties the cores the statement no longer reads. A column goes only where
  the rows of its SELECT are the same without it: one written as an
  expression, which calls no aggregate and no window function, in a core
  that is neither DISTINCT nor grouped, of a query with no ORDER BY and no
  compound but UNION ALL that is neither the statement's own nor a subquery.
  `result_pruned` proves that the pruned statement returns the statement's
  rows from every database, and `pruned_narrowing_sound` that a write its
  judge lets pass leaves them unchanged. The pruned statement reads, of each
  table it still reads, the columns its own clauses name and those the
  statement reads that no clause names (`Statement.unclaused`). So a column
  that only a column nothing reads names isn't read, though SQLite reports
  it, and `result_pruned` holds whatever columns the statement is said to
  read. The runner judges each case with the pruned statement. A view read
  whole in such a column goes with its core, as any other item there does,
  since `result_pruned` holds whatever the core's items read. The model has
  no names of SQLite's reports, so the live judging tests check which of a
  view's reads sqlive drops.
- **A condition carried across an equality.** `transfer_sound` proves that
  when `a.x = b.y` holds of a binding, a term true of `b`'s row on `y` is
  true of `a`'s row on `x`. The planner adds such a term to `a`'s scope,
  whichever of the two items it places first, so `JOIN b ON a.x = b.y WHERE
  b.y = 5 AND a.z = 1` narrows `a` by `a.x = 5` too, though `a.z = 1` makes
  `a` the item the planner places first. The carried term decides nothing
  of that order, so an item that only carried terms narrow still takes
  values from the items it is joined to. A term carried to a derived item
  passes into the items its arms pass its columns on from, and through them
  further down, unless a LEFT JOIN adds one (`Transfer.pushCarried`).
  `PState.Sem.pushCarried` proves that what the target then takes holds of
  its row in each binding of the arm that matters. A derived item's own terms on a
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
- **A parenthesized join.** SQLite joins the tables of `m LEFT JOIN (p LEFT
  JOIN a ON a.x = p.y) ON p.z = m.w` among themselves first, keeps the rows
  its ON holds for, and gives a left row that no such row joins NULLs in
  every table of the join. `groupJoin` states that reading. The runner reads
  the join's tables as items of the core, joined one after another, the
  first by the ON around the parentheses, and only where that gives the
  same rows. `inner_group_flat` proves that it always does for an inner
  join, whose ON holds no subquery and names no table after it, as one more
  condition on the joined rows. `left_group_flat` proves that it does for a
  LEFT JOIN whose ON holds no subquery and names no table of the join but
  the first, when each later table of the join is a LEFT JOIN's whose ON has
  a comparison, other than `IS`, with a column of a table before it, which
  can't hold where that table has no row. The runner refuses any other join
  in parentheses, and sqlive reads such a statement whole. SQLite reads a
  join in parentheses of two tables or more that follows another item of its
  FROM through a `SELECT *` of its own, and its authorizer reports every
  column of those tables, so the model counts every column of them as read.
- **An FTS5 table.** A contentful FTS5 table's rows are the rows of its
  content table, an ordinary table, which holds each row's rowid and its
  columns. sqlive records each of the content table's row changes as the
  same change of the FTS5 table, so the model reads the FTS5 table as a
  table whose columns have no affinity and compare by BINARY. FTS5 deletes a
  row of its content table and inserts it again when an UPDATE names it,
  changed or not. sqlive judges the row's net change, one update of the
  columns that changed, or none when no column changed. `rewrite_reruns`
  proves that the two changes rerun a statement wherever the update as one
  change would. A lookup reads the FTS5 table's rows from its content table, which
  costs fewer of SQLite's steps. sqlive doesn't know the FTS5 table's first
  column to hold its rowid, so the model reads a rowid as a unique key only of
  a table whose first column is its INTEGER PRIMARY KEY
  (`Item.rowidFirst`). A statement that names `rank` or the column
  that bears the table's name, whose values depend on the table's other
  rows, and one that reads any other virtual table, is read whole.
- **The LIMIT bound.** `limit_sound` proves that when a run returned as many
  rows as its LIMIT, rows that sort after its last row change nothing in the
  rows it returns. Each ORDER BY term sorts one item's rows by a key: one of
  its columns, or an expression over its columns with no subquery, and by a
  collation. A stored column of a collation other than BINARY is a key only
  as a term's own column (`Item.collated`, `termKey`). The model gives a collation as a key into the integers' order
  (`Dir`): two values compare as their keys do and tie when their keys are
  equal, as `'a'` and `'A'` do in NOCASE, so any total preorder on values is
  one. The bound reads the ORDER BY's terms up to the first it can't read
  (`Limit.leading`): `limit_sound` keeps the rows that sort at or before the
  last row on every term, and `keyLe_take` gives that each of them does so on
  the leading ones, which are what each item's bound compares. A write asks SQLite for the key's value in each row it changes
  (`RScope.ordered`). `sortKey_value` and `termKey_value` prove that in a binding where the term's
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
- **The LIMIT bound inside a derived item.** A run's item can be derived. When
  its query has one arm, with no LIMIT, OFFSET or window function, that passes
  on the columns the run's first terms read from one of its own items, which
  no LEFT JOIN adds, that item takes the run's bound over those terms, and an
  item inside it in turn (`Limit.Level`, `innerLevels`), for the first run
  over each item. Its lookups are the memberships of the run's item, read over
  its columns (`Planned.lookupThrough`), within each partner's final scope. The
  bound narrows the item's table's scope and no lookup of another item
  (`Planned.innerScope`). `innerLevels_chain`
  ([Rules/InnerLimit.lean](SqliveNarrowing/Rules/InnerLimit.lean)) proves that
  each level's terms have, in a row that holds the derived row's values, the
  directions and values of the first terms of the level around it, and
  `inner_step` that a level has a bound only where the one around it has one,
  which passes the derived row only if the level's passes its row.
- **A subquery's window.** A subquery that is one SELECT of one table, with
  an ORDER BY over that table's columns, a LIMIT and no OFFSET, returns for
  each binding around it the first rows of that binding's window: the
  table's rows that its own conditions pass and that hold the binding's value
  in each column an equality compares with a value around it.
  [Rules/Window.lean](SqliveNarrowing/Rules/Window.lean)
  states the reader (`Window.read`), and `read_facts` proves what it checks:
  every condition of the subquery names only the table's columns
  (`ownCond`) or compares one of them by `=` with a value around it, as the
  edge reader reads it (`partitionOf_holds`), and each ORDER BY term is an
  expression over the table's columns that names its result column. The
  window's scope passes a row that holds no NULL in a compared column and
  that fewer than LIMIT rows of its window sort before, with the table's
  rows read as a lookup reads them (`RScope.window`).
  [Windows.lean](SqliveNarrowing/Windows.lean)
  proves the rule. `window_bindings` proves that the subquery's core keeps,
  under a binding around it, a binding of each table row its conditions
  keep, and `window_kept` that the rows the conditions keep are exactly the
  window of each of them. So `window_passes` proves that the scope passes
  such a row exactly when fewer than LIMIT rows the core keeps sort before
  it, a test of its ORDER BY values that is closed downward.
  `count_first` proves that of LIMIT rows or more, the first LIMIT in their
  order pass that test, and `take_sortBy_window` that two lists of rows
  that hold the same rows passing such a test, of which at least LIMIT pass
  in one, have the same first LIMIT rows.
- **The rows SQLite picks.** A query that keeps some of its rows, by a LIMIT
  or an OFFSET or as a subquery whose first row a comparison takes, keeps the
  rows SQLite reads first, in the order of the plan it chooses.
  `post_picks_any` proves that with no ORDER BY, some order of the rows puts
  any of them among the rows the query keeps, so an index on a column no
  clause names can decide which rows those are.
  [Tells.lean](SqliveNarrowing/Tells.lean) states when the expressions a sort
  compares and the equalities of a core's conditions tell its bindings apart
  (`Statement.tellsApart`): each item, in turn, adds one row at most to each
  binding of those before it, a table by a unique key of it and a FROM
  subquery by returning one row at most (`Statement.oneRow`).
  `tellsApart_separates`
  ([Rules/TellsApart.lean](SqliveNarrowing/Rules/TellsApart.lean)) proves that two bindings of such a core that tie in
  those expressions are one binding, with `oneRow_sound` for the FROM
  subqueries, which takes the columns of the subquery the sort compares along
  with those the equalities cover. `fullyOrders_sound` proves that a query that aggregates with no
  GROUP BY, and so returns one row, or whose rows its ORDER BY tells apart by
  those rules, each term in a collation that ties no two different values
  (`Dir.exact`), keeps the same rows in every order, on a database that holds
  what the schema constrains (`Db.Constrained`): each row's rowid in its first
  column, never NULL and different in each row; no NULL in a NOT NULL column;
  and no two rows that hold one value, none of it NULL, in every column of a
  unique key.
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
  `group_concat` calls and windows each tell its rows apart, gives the same rows in some order from every order of its rows.
  `picked_column_reruns` proves that a change to any column of a table that a
  query of the first kind, or a SELECT whose rows follow the order SQLite reads
  them in, reads runs the query again, when the row before or after passes the
  table's scope.
- **The whole read.** `whole_sound` proves that with no scope, every change of
  a column the statement reads runs the query again.

## Putting the rules together

`narrowing_sound` proves that the rules together are sound, through the
steps below, each with its own theorem.

A statement's rows depend only on the columns it reads.
[Mask.lean](SqliveNarrowing/Mask.lean)
masks a database as sqlive keeps it: each row of a table the statement reads,
masked to the columns the statement reads, and no row of any other table.
`result_mask` proves that a statement whose cores list only items it has
returns the same rows from the masked database as from the database. So the
judge's lookups in the masked database read the rows the statement reads.

A write the judge lets pass leaves the rows each scope passes unchanged.
[Unjudged.lean](SqliveNarrowing/Unjudged.lean)
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

The step limit keeps the judge sound. `reruns_whole` proves that a statement
read whole reruns on every write that any scopes rerun it on, so
`Statement.rerunsLimited_sound` proves that a write sqlive lets pass, whether
or not one of its questions stopped, is a write the judge without the limit
lets pass. Every theorem here that takes a write the judge lets pass holds of
sqlive's judge too.

A lookup of a derived item reads it alone, with more fuel than the statement
has left where the item sits.
[Fuel.lean](SqliveNarrowing/Fuel.lean) proves that the
extra fuel changes nothing: `evalQuery_fuel` proves that a well-formed statement's
query returns the same rows with any fuel past its number, and
`lookupRows_derived` that a lookup of a derived item whose arms sit in no core
reads the rows the statement reads of it.

Two databases that hold the same rows passing each item's scope give the
statement the same rows, as long as every binding that decides those rows
holds only rows that pass their scopes.
[Restrict.lean](SqliveNarrowing/Restrict.lean)
proves this from a promise that it states apart from the planner (`Kept`):
each binding a core keeps that decides the rows its query returns holds rows
that pass their items' tests, and holds no row of a LEFT JOIN item only when
that item's ON can't hold for a row that fails its test. A binding of a
derived item's arm decides the derived item's rows when its row passes the
derived item's test. A binding of an arm that aggregates decides them when
its group can make a row that passes the test: some row that passes it holds
the binding's GROUP BY values in the result columns that are bare columns
the GROUP BY lists (`keyMatters`). Its group's bindings all share those
values, so `Core.result_filter_key` proves that the arm makes the same rows
that pass the test from those bindings as from all of them, and a term the
planner passes into the arm through such a column holds of every one of
them. A binding of an arm that calls a window function always decides them.
`joinItems_restricted` and
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
[Outer.lean](SqliveNarrowing/Outer.lean) states the
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
[Congr.lean](SqliveNarrowing/Congr.lean) proves
that a query's rows depend, of the binding a run reads around it, only on the
rows of the outer items the query names (`evalQuery_outer_congr`): a run
binds a core's own items as it joins them. So each ON condition of a LEFT
JOIN holds of each binding its core keeps that holds a row of the join's item
(`Statement.on_kept`), as it held when the join added the item, because the
ON names only that item, items before it and items outside the core.

A statement with a LIMIT bound needs only the bindings of its own core whose
rows sort at or before the last row the run before the write returned.
`limit_restricted` proves that when the statement's own query is one SELECT
with an ORDER BY, a LIMIT and no OFFSET, and its run returned as many rows as
the LIMIT, the statement returns the same rows from both databases, as long as
those bindings, and the bindings of every other query, keep the promise: the
two runs return the same rows that sort at or before the last row, and
`limit_sound` gives the same first rows.

A subquery's window needs only the bindings of its core that pass the window.
`window_queryRows` proves that a query of one core, with an ORDER BY, a LIMIT
and no OFFSET, returns the same rows from both databases when its bindings
that pass a test of their ORDER BY values closed downward agree, and either
every binding passes the test in both databases, or at least LIMIT bindings
pass it in the database the write leaves. The promise `Kept` counts a
binding as one that matters only when it passes its windows
(`Planned.windowPasses`), so the restriction carries, for each query a
binding fails the windows of, what `window_queryRows` takes
(`Restriction.windows`). `agree_of_kept` takes it for each such query and
the old argument for the rest.

The promise rests on what makes a row pass the scope the planner gives an
item. [Scopes.lean](SqliveNarrowing/Scopes.lean)
states it. `Planned.scopes_eq` proves that a placed item's scope reads the
scopes of the items placed before it, which keep those scopes to the end.
`scopeAt_passes` proves that the scope passes a row of which the item's terms
and what its edges carry hold, that each of its memberships' scopes passes, that
a side of each OR it keeps passes, and that its LIMIT bound passes. The
memberships that join one partner by `=`, each of a column of the partner's rows that
no scope fixes, are the equalities of one join, which one row of the partner
satisfies together: the scope also holds `joinedAll`, the rows that one row of
the partner in its scope gives every such membership's value (`groupScopes`; a
membership to a subquery's source, or passed into a derived item's arm, takes
no part, since the rows that satisfy those need not be one row). `scopeAt_passes`
takes for it that one row of the partner gives a row all its memberships' values,
which a binding gives, since it holds one row of each partner of its core
(`partner_found` says the row is the binding's); `joinedAll_passes_joined`
proves that `joinedAll` implies each membership's own scope, which `scopeAt_parts`
and everything after it read.
`membershipScope_passes` proves that a membership's scope passes a row whose
value a row of the partner in the partner's scope gives, NULL only for an `IS`
edge, through `equalValues_passes` when the partner's scope lists its values;
`sideScope_passes` and `bind_passes` do the same for an OR's side and a term.
[PlannerState.lean](SqliveNarrowing/PlannerState.lean)
states what the planner's state keeps however it reads the conditions.
`plan_leftOn` proves that the planned state holds an ON narrowing back only
for an item a LEFT JOIN adds: the planner starts by holding one back for
each such item, and each step that makes an item an inner join's releases
it for good. The restriction lets a binding hold no row only of such an
item. `plan_order` proves that the planner places each item once, and every
item of every core: each step places one item that is not placed yet, and
the planner takes a step for each item. `Planned.scopes_eq` takes both. The
theorems hold for any order of steps, so which item the planner places next,
and so how many rows it narrows each item to, is no part of what they prove:
the Rust planner's test of a join chain, whose first table waits for a
subquery's table, checks that choice.
`plan_pushed` proves that each edge the planner passes into a derived item's
arms comes from an edge the derived item holds, as a membership or as an edge
passed into it, through the result column that an arm passes the target's
column on to unchanged, from an arm that groups no rows or groups them by
that column, and that the target is an inner join's. The planner
keeps the edges it passes in a list of their own (`PState.pushed`), apart
from the edges conditions give, and no step of the planner reads that list.
`plan_nestedOK` proves that each edge of the planned state whose partner is
a subquery's source has its partner in an arm of some core's subquery, as
each condition's reader promises, because the planner changes only an
edge's member when it passes the edge into a derived item's arm, and that
each item's memberships are edges from it, and that each edge through a
derived item reads a column that the derived item's one arm passes on from a
source the arm lists, where the derived item sits in the member's core or in
a core of the condition's subquery (`Edge.NestedIn`). `plan_before` proves that
each item's memberships, and the sides of each OR it keeps, take values from
items the planner placed before it, through derived items it placed before
it: the planner sets them only as it places the item, from the edges and
sides whose partners and derived items it placed before, and no other step
changes them. `plan_placedBefore` states the partners' part of it. The
planner adds an edge only when it doesn't hold it (`addOnce`): an equality
that reaches a column through several columns of a derived item passes an
edge into it for each, and a copy of an edge passes no row the edge doesn't.
`plan_edges_once` proves that the planned state holds no edge twice, and
`plan_once` that no item of it has a membership twice.
[ReadOrder.lean](SqliveNarrowing/ReadOrder.lean)
states the order the planner reads cores in. A core sits below another when
it is an arm of a query the other reads, as a derived item's or a subquery's,
at any depth. `readOrder_postorder` proves that the planner reads no core
twice, and reads every core below a core before the core itself: each core is
an arm of one query, and one core reads each query, so the cores a core sits
below lie on one chain.

[Tests.lean](SqliveNarrowing/Tests.lean) states the
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
`plan_windowRows` proves the windows' part: a core whose binding fails its
windows is the one core of a window's subquery, whose bindings pass by
`window_passes`, and either every binding passes, when the database the
write leaves holds fewer than LIMIT rows the core keeps under the binding
around it, or the first LIMIT do (`count_first`). The window's scope enters
only its table's scope, not the scopes that the lookups of other items read,
so it changes no other item's test. A comparison that takes a subquery's
value takes it from a binding of the subquery's core that gives a row the
subquery returns: the edge reader promises such a binding
(`Statement.GivesRow`), from `query_row_at`. `givesRow_count` proves that
such a binding of a window's core sorts after fewer than LIMIT rows its core
keeps, in the database it reads. `givesRow_windowTest` proves that it then
passes the window in the database the write leaves: otherwise LIMIT rows
sort before it there that pass the window, so pass their item's test, and
the two databases hold the same rows that pass that test.
`nested_windowPasses` gives these to the item an edge narrows, through
`givesRow_windowPasses`, so the partner that the edge finds passes its test.
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
ORDER BY values. `inner_passes`
([LimitTests.lean](SqliveNarrowing/LimitTests.lean)) proves the same of each bound from inside a derived item, from the binding that
matters alone: the derived item's test keeps a row that holds the binding's
values in the columns its arm passes on, and passes its own bounds, which
`inner_step` takes down to the item's row. `innerScope_left` proves that no
such bound narrows an item a LEFT JOIN adds.

[Facts.lean](SqliveNarrowing/Facts.lean) states
what the planner's state says of the bindings a core keeps (`PState.Sem`):
an item the state holds no ON narrowing back for has a row, each item's terms
hold of its row, each edge and OR of an item with a row holds, and what the
state holds back for a LEFT JOIN's item holds where that item has a row.
`PState.Sem.initial` proves that the state before the planner reads any
condition keeps it, and `PState.Sem.readConds` that reading a core's
conditions keeps it. A WHERE condition holds of every binding the core keeps,
and an ON condition of each binding that holds a row of the join's item, so
the reader's promise gives each term, edge and OR at the binding itself. An
item the planner makes an inner join's holds a fact that a row of NULLs fails
(`holds_none`, `HoldsIn.bound`, `Disjunction.HoldsIn.bound`), so every such
binding holds its row, and what its ON held back holds there too.
`PState.Sem.carry` proves that carrying terms across a core's edges keeps it,
when the planner has passed no edge into a derived item's arm yet and a
derived item's row passes what its arms say of each column that isn't NULL:
an edge an item with a row holds holds of the binding, so its partner has a
row, and `transfer_sound` carries the partner's term to the member.
[ReadPhase.lean](SqliveNarrowing/ReadPhase.lean)
carries the invariant through every core the planner reads. The planner
only adds to an item's term, with AND (`PState.TermGrows`), so the term an
item holds when the planner passes it into the item's arms is true of every
row the item's final term is true of. A binding of an arm whose row matters
to the derived item holds, in each column the arm passes on, the value of a
row that the derived item's test keeps (`relevant_cells`), so the term the
planner passes in holds of the arm's item's row there, and
`PState.Sem.pushDown` proves that passing a term into an arm keeps the
invariant for such bindings. The planner reads a core after every core below
it, so when it carries terms across a core's edges, it has passed nothing
into its derived items' arms yet, and their items' terms hold of every
binding the arms keep, as the carry needs. Once the planner has passed a
core's items' terms into their arms, it passes their carried terms in too,
and `PState.Sem.pushCarried` proves that this keeps the invariant, by the
same argument: it only adds to an item's carried term, with AND
(`PState.CarriedGrows`). `PState.Sem.readCore` proves
that reading a core keeps the invariant, and `plan_readSem` that the state
after the planner reads every core says only what holds of each binding a
core keeps, and of each binding below a core that matters.
[OrderPhase.lean](SqliveNarrowing/OrderPhase.lean)
carries it through the order the planner places items in. As the planner
places an item, it takes the item's edges to items placed before it as
memberships and passes them into the item's arms when the item is derived.
A passed edge joins the edges the planner keeps apart (`PState.pushed`), and
makes its target an inner join's where a row of NULLs fails it: a binding of
the arm that matters holds, in the column the arm passes on, the value of a
row the derived item's test keeps, and the test keeps a row only where the
edge's member column holds a value, so the target has a row there.
`PState.Sem.pushEdges` proves that passing edges into an arm keeps the
invariant, `PState.Sem.placed` that placing an item does, and `plan_sem`
that the planned state says only what holds of each binding a core keeps
that matters to its query's test. `queryTest_good` proves that the tests the
planner's scopes give are such tests: a row the test of a derived item's
query keeps passes the derived item's scope and passed edges, so the
derived item's term holds of it (`scopeAt_parts`), and it holds a value in
the member column of each of the derived item's memberships and passed edges
that compares by `=` (`membershipScope_value`). `plan_semTests` gives the
planned state's facts for those tests.
`plan_membersEdges` proves that each item's memberships are edges of the
planned state: the planner takes them from the state's edges as it places
the item, and no step drops an edge, so what the invariant says of the
state's edges it says of each membership.
[Partners.lean](SqliveNarrowing/Partners.lean)
proves where the planned state's edges take their values from
(`plan_lookups`). A condition's reader gives only edges whose partner a
lookup reads alone, and the planner moves an edge's member only, so every
edge of the planned state, every edge of a side of each OR and every
narrowing an ON holds back has such a partner. Each OR an item keeps is one
of its ORs, and each edge the planner passes into a derived item's arm has a
member its core lists. The planner places only items a core lists
(`plan_order_listed`). A query readable alone names no item outside its
cores (`Statement.outerNames_readsAlone`), so a derived item a lookup reads
alone runs its query under any binding as under the binding of no row
(`evalQuery_readable`, `outer_readable`). Every binding of an arm matters to
a query whose every row matters (`relevant_of_all`), and no derived item
reads the statement's own query, so its test keeps every row
(`queryTest_root`).

[Kept.lean](SqliveNarrowing/Kept.lean) proves
the promise for the bindings that matter. Each item's test (`planTest`) asks
of its row what the item's scope asks, with the scopes of the items placed
before it, and what each edge passed into the item asks. In a binding that
matters, the planned state's facts hold (`plan_semTests`): each term holds,
and each membership and each side of a kept OR holds. An edge's partner has
its row in the binding or, for a subquery's source, in a binding of the
subquery's core under it. An edge through a derived item finds its partner's
row in the binding of the derived item's arm that gives the derived item's
row, and that binding matters to the derived item's query when the derived
row passes its test (`derived_source_passes`). The planner places each
partner, and each derived item an edge reads through, before its member, so
by induction on the order the partner's row passes its test. That row is
then a row of the partner's lookup that passes the partner's scope
(`partner_found`), and the membership's scope passes the member's row. An
edge passed into a derived item's arm passes because the binding's row is one
the derived item's test keeps (`pushed_passes`, `membershipScope_pushed`). A
row of an item a lookup reads alone is a row of the lookup: both databases
hold a table row that passes the item's test, and both runs return a derived
item's row at the same fuel. `plan_kept_rows` proves that each row of a
binding that matters passes its test, and `plan_restricted` that such a
binding holds no row only of an item the planned state still holds an ON
narrowing back for.

[Held.lean](SqliveNarrowing/Held.lean) proves the
promise for a LEFT JOIN item the planned state still holds an ON narrowing
back for. While the planner holds one back, it adds to the item only what the
item's own ON says of it. Any other narrowing, and any pass into the item,
makes the item an inner join's first, and such a step leaves every item the
planner still holds back as it was (`untouched_narrow`, `untouched_pushDown`). The reader's promise holds wherever its condition
holds, so each term, edge and OR the planned state holds of such an item
holds at each binding where the item's ON holds of a row of it
(`plan_held`). A term the planner carries to the item from a partner holds
there too, when the partner's row passes what the planned state says of it:
the partner's term, or, for a derived partner, what its arms say of the
compared column of a row its lookup reads (`PState.Held.carry`,
`PState.Sem.pull_lookup`). `plan_left_passes` proves that a row the item's
ON holds for passes the item's test. Each partner the item's ON gives sits
before the item in the order, so its row in the binding, or in a binding of
the ON's subquery under it, passes its test, and no edge passes into the
item.

[Sound.lean](SqliveNarrowing/Sound.lean) puts the
steps together. `plan_kept` proves that the planner's scopes keep the promise
`Kept` at every fuel at which the databases before and after the write return
the same rows that matter, so `agree_of_kept` gives every query but the
statement's own the same rows that matter. No derived item reads the
statement's own query, so all its rows matter. When the run that read the
database before the write returned as many rows as its LIMIT, the LIMIT bound
narrows the statement's own core to the bindings that sort at or before the
run's last row (`Limit.lastValues_some`, `Statement.result_outRow`), and
`limit_restricted` takes those. `narrowing_sound` proves that a write the
judge lets pass leaves a well-formed statement's rows unchanged, and
`narrowing_sound_limited` that the same holds of sqlive's judge, which
narrows nothing for a write whose question stopped at the step limit.

## A write's net change of each row

A write can change one row more than once: a statement inserts a row and the
next deletes it, an update sets a column and a later one sets it back, or
`INSERT OR REPLACE` deletes a row and inserts it again with the same values.
sqlive judges each row's net change instead of each change
(`net_changes.rs`).
[Net.lean](SqliveNarrowing/Net.lean) names the
slot each change changes, as sqlive names a row by its key, and folds the
changes of each slot into one change, from the slot's row before the write's
first change of it to its row after the last, with none for a slot that ends
as it began (`net`, in
[Changes.lean](SqliveNarrowing/Changes.lean)).

`SlotWrites.firstOld_eq` and `SlotWrites.lastNew_eq` prove that those rows are
the slot's rows before and after the write, so the net changes take the
database before the write to the database after it (`net_writes`), and
`net_sound` and `indexed_net_sound` prove that a write whose net changes the
judge lets pass leaves a well-formed statement's rows unchanged. `mem_net`
proves that the net changes are exactly the changes of the slots whose rows
differ before and after the write. So the judge's verdict on them depends only
on those two databases (`net_judges_states`), a write whose every slot ends as
it began reruns no statement (`net_unchanged`), and the judge of the net
changes reruns a statement only where the judge of every change does
(`net_reruns_imp`): `unjudged_slots` proves that when the judge of every
change lets a write pass, each slot whose kept row the write changes holds
rows that fail the scope before and after it.

## Which live queries judge a write

[Index.lean](SqliveNarrowing/Index.lean) models
how a handle finds the live queries that a write could rerun. The handle
files each table a waiting live query reads under the key of the table's
scope: a column that the scope's equalities or IN lists test, and the values
that every row the scope passes holds in it (`RScope.key`). The handle files
a table whose scope has no key under the table alone. The judge judges each
change only against the live queries the handle filed under the change's
table with no key, or with a key whose values include the value the changed
row holds in the key's column before or after the change.
`RScope.key_passes` proves that a row the scope passes holds one of its
key's values. So the index finds each live query for every change that
reruns it (`lookup_complete`), and the judge through the index reruns each
live query on exactly the writes that the judge of every change reruns it on
(`index_judges_alike`). `indexed_narrowing_sound` then proves that a write
the judge through the index lets pass leaves a well-formed statement's rows
unchanged.

## One read for each place a table is named

sqlive judges a write against one read for each place a statement names a
table, with that place's columns and scope (`read_capture.rs`).
[References.lean](SqliveNarrowing/References.lean) states that judge as the
judge above of another statement, `Statement.split`, in which each FROM item
that names a table reads a table of its own, numbered as the item. There each
table's columns are one item's, with every column of an item that
`Statement.reads` gives every column of, and each table's scope is one item's.
A column SQLite reports that no item's columns hold goes to every item of its
table. `Db.split` gives each item's table the rows of the table it names, and
`Statement.splitChanges` gives a write's change of a table to each of them.
`Statement.split_wellFormed` proves that the split statement is well formed,
`split_result` that it returns the statement's rows, and `split_writes` that
the split changes take the one split database to the other. So
`references_sound` is `narrowing_sound` of the split statement, and
`references_net_sound` and `references_indexed_sound` state it of a write's
net changes and through the index of waiting queries.

## What the proofs don't cover

The theorems prove each rule's step and `narrowing_sound`, which puts the
rules together for every well-formed statement the model states. They prove
nothing of sqlive's own code: the differential checks sqlive's planner and
judge against the model, and the model against SQLite, on every statement it
generates.

The model leaves out text, real and blob values, the conversions an affinity
makes, and every collation but those of ORDER BY terms and of the equalities
that a USING or NATURAL join stands for: sqlive's checks that refuse them are
modelled as refusals. A partner column of no affinity gives a member column of another
affinity its values: SQLite compares a TEXT member with it as they are, and
gives its values an INTEGER member's affinity, which leaves an integer as it
is. The differential's column of no affinity holds integers, so it checks the
INTEGER case, and a unit test checks the TEXT case against SQLite. The model has
no generated columns: sqlive sorts by a generated
column's expression, which the model states as an expression term. The
differential's tables hold no generated column: sqlive records no value of
one for a write and counts it as changed by every write that changes a stored
column of its row, which the model doesn't state. The model also leaves out bound parameters, table functions, the views sqlive
reads whole as a join's partner, and the terms that compare a NULL test's truth or
a column `IS` a parameter. The model's index holds only integers, so it doesn't
state that sqlive files an integer and a real that compare equal under one
value. Every changed row in the model holds a value in each column, so the
model doesn't state that a row whose value a write didn't record, or a
change that names no row, reaches every live query sqlive filed under its
table. The model names a row by its rowid. sqlive names it by its primary key,
and refuses a table with none. The model keeps
every column of a changed row, so it doesn't state that sqlive counts a column
whose values it doesn't keep, one no live query reads or tests, as changed
once a statement changes it.

## The runner and the differential

`narrowingRunner` reads one case as JSON on stdin: the schema (each table's
columns and their affinities, and each view's query), the statement as a tree
that names its sources by alias, the database before and after the write, the
write's row changes, and whether one of sqlive's questions about the write
stopped at the step limit. The runner reads each change's slot from the rowid
in the first column of its row, and judges the write's net change of each
row. The runner judges the statement the model plans, without the columns
nothing reads (`Statement.pruned`), and checks that it and the statement as
read are both well formed. The runner reads a CTE where a query names it, so a
CTE that no query reads, which SQLite never runs, adds nothing to the model's
statement, and it reads the CTE that SQLite resolves the name to. The runner writes whether sqlive's judge of places (`Statement.rerunsReferences`) reruns the statement,
whether the statement is well formed, whether the model reads the statement whole, each place's scope, and the
statement's rows before and after.

The differential in sqlive (`narrowing_model` among the live judging tests)
has one test per rule, `<rule>_matches_lean`, `mixed_shapes_match_lean`
for statements that hold several rules, `references_match_lean` for statements
that name one table in several places, and `net_change_matches_lean`, whose
writes change one row more than once: a pair of statements that cancel out,
as an insert and a delete, an update and an update back, a delete and an
insert of the same row, or a key changed and changed back, or a row changed
and changed again. One of its tables is a contentful
FTS5 table, whose `id` its writes keep equal to its rowid. Its changes are
the operations SQLite makes on its content table, which a pre-update hook on
the test's copy of the database records, as sqlive's own pre-update hook
records the test's writes. Each rule's generator writes
statements of its rule's shape. The CTEs' generator also writes, beside the
CTEs a statement reads, one that no query reads, with a LIMIT or a USING
join. For each statement the test opens a live query and makes random writes,
and counts the questions sqlive stops at the step limit as it judges each
write. After each write it fails when the model finds the
statement not well formed, when sqlive and the model disagree on whether the write reruns the query, when a write that either lets
pass changed SQLite's rows, or when the model's rows differ from SQLite's.
Under a LIMIT or a scalar subquery, SQLite may pick other rows than the model
among rows that sort the same, and an aggregate's column outside every GROUP BY
term, or a `group_concat` or a window with no ORDER BY that sorts by each
table's rowid, may take other rows' values. The test compares the rows there only through
the reruns. The model's values are integers, so the differential reads
`group_concat` as `CAST(group_concat(e, '') AS INTEGER)`, the joined digits read
back as one integer, which the model computes the same way. A subquery's
ORDER BY term that its result doesn't return is a result column the model's
query returns after the SQL's columns: SQLite allows no second column in a
scalar subquery, and a subquery's value, EXISTS and a comparison with a
subquery read only the first column. The edge reader for a column compared
with a subquery's value reads the first column the same way.

SQLite's authorizer reports no read of the columns a USING or NATURAL join
compares, nor of the columns that decide which rows a query whose rows SQLite
picks keeps, or which row an aggregate takes a value from, so sqlive adds those
columns to a statement's reads
([sqlive's spec, section 4](../SPEC.md#4-what-a-statement-reads)), and the
model's reads and sqlive's agree.
