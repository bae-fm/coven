import SqliveNarrowing.Reader

/-! Rule 5: one of the app's views is read as its query's cores, like a FROM
subquery, and a lookup reads it alone by its name wherever the statement
reads it; a FROM subquery or CTE is readable alone only outside any
expression subquery, where nothing around it can name an outer row
(`read_derived_item`). -/
namespace SqliveNarrowing.View

/-- Whether a derived item is readable alone: a view always, a FROM subquery
or a CTE when the core holding it sits in no other. -/
def readable (view : Bool) (parent : Option Nat) : Bool := view || parent.isNone

/-- Rule 5's theorem: a query whose arms sit in no other core, as a view's
or a FROM subquery's or CTE's outside any expression subquery, returns the
same rows under every binding around it, so a lookup that reads it alone
reads the rows the statement reads. -/
theorem view_sound {s : Statement} {q : Nat} (h : ∀ k ∈ (s.query q).arms, (s.core k).parent = none)
    (db : Db) (f : Nat) (b : Bind) : evalQuery s db f b q = evalQuery s db f Bind.empty q := by
  cases f with
  | zero => rfl
  | succ f =>
    simp only [evalQuery, queryRows]
    congr 2
    apply List.map_congr_left
    intro k hk
    simp [coreBindings, h k hk]

end SqliveNarrowing.View
