import SqliveNarrowing.Planner

/-! Whether a write reruns a live query, with the scopes sqlive's planner
gives each table, or every row of each table for a statement it reads
whole. -/
namespace SqliveNarrowing

/-- The scope of each table for a run on `db`, or `none` when the statement
is read whole. -/
def Statement.tableScopes (s : Statement) (db : Db) : Option (Nat → Option RScope) :=
  if Whole.readsWhole s then none
  else
    let planned := plan s
    let last := planned.sorted.bind fun sorted => Limit.lastValues sorted (s.result db)
    some (tableScope s (planned.scopes last))

/-- Whether sqlive's narrowing, as the model states it, reruns the statement
whose last run read `db`, for a write whose `changes` leave `db'`. -/
def Statement.reruns (s : Statement) (db : Db) (changes : List Change) (db' : Db) : Bool :=
  SqliveNarrowing.reruns s ((s.tableScopes db).getD fun _ => none) s.reads changes db'

end SqliveNarrowing
