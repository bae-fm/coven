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
    some (tableScope s (planned.itemScopes last))

/-- Whether sqlive's narrowing, as the model states it, reruns the statement
whose last run read `db`, for a write whose `changes` leave `db'`. -/
def Statement.reruns (s : Statement) (db : Db) (changes : List Change) (db' : Db) : Bool :=
  SqliveNarrowing.reruns s ((s.tableScopes db).getD fun _ => none) s.reads changes db'

/-- Whether sqlive reruns the statement for a write, given whether a question
that judged the write ran past sqlive's step limit (`LOOKUP_STEPS`) and
`stopped`. sqlive can't tell then whether the changed row passes its scope,
so the statement narrows nothing for that write: every change of a column it
reads reruns it. Which questions stop depends on the plan SQLite picks for
them, which the model doesn't state, so the runner takes `stopped` from
sqlive. -/
def Statement.rerunsLimited (s : Statement) (db : Db) (changes : List Change) (db' : Db)
    (stopped : Bool) : Bool :=
  if stopped then SqliveNarrowing.reruns s (fun _ => none) s.reads changes db'
  else s.reruns db changes db'

/-- A statement read whole reruns on every write that reruns it under any
scopes: with no scope, every row passes. -/
theorem reruns_whole {s : Statement} {scopeOf : Nat → Option RScope}
    {reads : List (Nat × List Nat)} {changes : List Change} {db' : Db}
    (h : SqliveNarrowing.reruns s scopeOf reads changes db' = true) :
    SqliveNarrowing.reruns s (fun _ => none) reads changes db' = true := by
  unfold SqliveNarrowing.reruns at h
  obtain ⟨ch, hch, hrun⟩ := List.any_eq_true.mp h
  cases hf : reads.find? (·.1 == ch.table) with
  | none => simp [hf] at hrun
  | some read =>
    obtain ⟨t, columns⟩ := read
    simp only [hf, Bool.and_eq_true] at hrun
    exact Whole.whole_sound hch hf hrun.1

/-- The step limit keeps the judge sound: a write that sqlive, stopping
questions at its step limit, lets pass, the judge without the limit lets
pass too, so every theorem that takes a write the judge lets pass holds of
sqlive's judge. -/
theorem Statement.rerunsLimited_sound {s : Statement} {db db' : Db} {changes : List Change}
    {stopped : Bool} (h : s.rerunsLimited db changes db' stopped = false) :
    s.reruns db changes db' = false := by
  unfold Statement.rerunsLimited at h
  cases stopped with
  | false => simpa using h
  | true =>
    simp only [↓reduceIte] at h
    cases hr : s.reruns db changes db' with
    | false => rfl
    | true =>
      unfold Statement.reruns at hr
      rw [reruns_whole hr] at h
      exact absurd h (by simp)

end SqliveNarrowing
