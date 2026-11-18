# Live query narrowing

The Lean package in [crates/sqlive/proofs](../../crates/sqlive/proofs/) models
how sqlive narrows a live query to the rows its statements can depend on (§4),
and which writes therefore run the query again. The package states a statement
as sqlive's planner reads it, ports the planner, and proves each rule's step:
the rows a rule leaves out of a table's scope are rows the statement's result
doesn't depend on.

## The model

[Syntax.lean](../../crates/sqlive/proofs/SqliveNarrowing/Syntax.lean) states a
statement as the planner numbers it: its FROM items, the SELECT cores that hold
them, the queries whose arms the cores are, and the conditions on each core's
rows, each in the WHERE or in a LEFT JOIN's ON. A subquery is a query number,
so an expression names it without nesting. A value is NULL or an integer, and
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
until a WHERE condition makes the join an inner join's, passes a derived item's
terms into its arms, and places the items so each takes values from items
placed before it. A run binds each item's scope; a table's scope passes a row
that any item of the table passes.

[Changes.lean](../../crates/sqlive/proofs/SqliveNarrowing/Changes.lean) and
[Judge.lean](../../crates/sqlive/proofs/SqliveNarrowing/Judge.lean) judge a
write as sqlive does. The write's row changes are one per row that each of its
statements changed, as sqlive's pre-update hook records them. A change reruns
the query when it inserts or deletes a row of a table the statement reads, or
changes a column the statement reads, and the row before or after passes the
table's scope. A join's lookups read the database the write leaves. The model
takes the columns a statement reads to be the columns its clauses name.

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
  in either direction and for an expression over `b`'s columns.
- **A column compared with a subquery's value.** `subquery_edge_sound` proves
  that `x.c = (SELECT y.d ...)` or `x.c IN (SELECT y.d ...)`, true of a
  binding, makes `x.c` hold the value `y.d` holds in a binding the subquery's
  core keeps.
- **A condition passed into a FROM subquery, CTE or view.** `push_down_sound`
  proves that a row of a derived item's query is a row of one of its arms, and
  that a row of an arm that doesn't aggregate holds, in each column that is a
  bare column of one of the arm's items, that item's value. `renamed_sound`
  proves that a term true of the derived row is then true of the item's row.
- **A view or FROM subquery read alone.** `view_sound` proves that a query
  whose arms sit in no other core returns the same rows under every binding
  around it, so a lookup that reads it alone reads the rows the statement
  reads.
- **The LIMIT bound.** `limit_sound` proves that when a run returned as many
  rows as its LIMIT, rows that sort after its last row change nothing in the
  rows it returns. `atOrBefore_passes` proves that the bound's scope passes
  exactly the rows that sort at or before the last row, NULL first ascending
  and last descending.
- **The whole read.** `whole_sound` proves that with no scope, every change of
  a column the statement reads runs the query again.

## What the proofs don't cover

The theorems prove each rule's step. No theorem yet proves that the planner
puts the steps together soundly: that the order in which it places items, the
ON conditions it holds back, the terms it passes into derived items and the
lookups of each join's partner give scopes outside of which a write leaves the
result unchanged.

The model leaves out text, real and blob values, collations and the
conversions an affinity makes: sqlive's checks that refuse them are modelled as
refusals. The model also leaves out bound parameters, table functions, the views sqlive
reads whole as a join's partner, the terms that compare a NULL test's truth or
a column `IS` a parameter, and the step limit on a lookup.
