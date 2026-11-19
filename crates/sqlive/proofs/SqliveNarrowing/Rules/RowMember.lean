import SqliveNarrowing.Reader

/-! An expression of the member's own row compared with a partner: an edge's
member is a column of one of a core's items, or an expression over the
columns of one table item of the core, with no subquery, each column one
sqlive narrows by (`Planner::member_expression`). A write asks SQLite for the
expression's value in each row it changes, and the edge narrows the table to
the rows whose value some row of the partner gives. The expression is NULL
wherever its columns are, or its item is no LEFT JOIN's: another, as a CASE
or COALESCE, could make the equality true of the NULLs a missing right row
gives, which no row of the table holds, and the equality would then make the
LEFT JOIN an inner join's wrongly. An expression's value has no affinity, so
the member's affinity is BLOB, which converts nothing, and a partner column
of any affinity gives it values, as SQLite gives the expression's value the
column's affinity. -/
namespace SqliveNarrowing.RowMember

/-- The item an expression member `e`, written in core `k`, reads: a table
item of the core whose columns, each one sqlive narrows by, are all `e`
names, with no subquery, and that is no LEFT JOIN's unless `e` is NULL
wherever they are. -/
def exprItem (s : Statement) (k : Nat) (e : Expr) : Option Nat :=
  match e.items.head? with
  | none => none
  | some m =>
    if e.queries.isEmpty && e.items.all (· == m) && (s.item m).core == k &&
        (s.table? m).isSome && e.refs.all (fun (_, c) => (s.affinity m c).isSome) &&
        (e.strict || s.alwaysBound m) then some m
    else none

/-- The member `member`, written in core `k`, gives: its item, the value it
compares of the item's row, and its affinity. -/
def read (s : Statement) (k : Nat) : Expr → Option (Nat × PValue × Affinity)
  | .col m c => if (s.item m).core = k then (s.affinity m c).map fun a => (m, .column c, a) else none
  | e => (exprItem s k e).map fun m => (m, .expr e, .blob)

theorem exprItem_facts {s : Statement} {k : Nat} {e : Expr} {m : Nat}
    (h : exprItem s k e = some m) :
    e.queries = [] ∧ (∀ j ∈ e.items, j = m) ∧ (s.item m).core = k ∧
    (e.strict = true ∨ s.alwaysBound m = true) ∧ e.refs ≠ [] := by
  unfold exprItem at h
  split at h
  · cases h
  · rename_i m' hhead
    split at h
    · rename_i hok
      cases h
      simp only [Bool.and_eq_true, List.isEmpty_iff, List.all_eq_true, beq_iff_eq,
        Bool.or_eq_true] at hok
      refine ⟨hok.1.1.1.1.1, hok.1.1.1.1.2, hok.1.1.1.2, hok.2, fun hnil => ?_⟩
      simp [Expr.items, hnil] at hhead
    · cases h

/-- The theorem: a member's value in a binding's row of its item is the
value the member, as written, has in the binding, so wherever the equality
holds, the item's row gives the value it compares. The member is an item of
its core, and an expression member is NULL wherever its columns are, or its
item is no LEFT JOIN's, so a member's value is NULL for a missing row
unless every binding holds its item's row. -/
theorem row_member_sound {s : Statement} {k : Nat} {member : Expr} {m : Nat} {v : PValue}
    {a : Affinity} (h : read s k member = some (m, v, a)) :
    (s.item m).core = k ∧ (∀ b sub, v.on (b m) = member.eval b sub) ∧
    (v.on none = none ∨ s.alwaysBound m = true) := by
  cases member
  case col i c =>
    simp only [read] at h
    split at h
    · rename_i hk
      obtain ⟨a', _, he⟩ := Option.map_eq_some_iff.mp h
      cases he
      exact ⟨hk, fun b sub => rfl, Or.inl rfl⟩
    · cases h
  all_goals
    simp only [read, Option.map_eq_some_iff] at h
    obtain ⟨m', hm', he⟩ := h
    cases he
    obtain ⟨hq, hitems, hk, hst, hrefs⟩ := exprItem_facts hm'
    refine ⟨hk, fun b sub => (Expr.eval_rowBind _ hitems hq).symm, ?_⟩
    rcases hst with hst | hst
    · exact Or.inl (Expr.eval_null _ hrefs hq hst fun _ _ _ => rfl)
    · exact Or.inr hst

end SqliveNarrowing.RowMember
