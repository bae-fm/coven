import SqliveNarrowing.Rules.RowMember
import SqliveNarrowing.Rules.PushDown

/-! An equality with a column of a derived item that a lookup can't read
alone, as a CTE whose rows read a row around the statement that holds it,
narrows the member to the rows whose value a row of the source the derived
item's arm passes the column on from gives (`Planner::passed_edge`). The
derived item's query is one SELECT that neither aggregates nor calls a
window function, and its result column is a bare column of one of its own
items, which a lookup reads alone (`passedSource`): each row of the derived
item is a row of a binding its arm keeps, whose source row holds the
column's value. The member's affinity and the source column's decide the
value as for an edge to the source (`columnValue`). The member waits for the
derived item and the source (`Planner::waiting`). The rules that read an
equality (`Edge`) and a column compared with a subquery's value
(`SubqueryEdge`) give this edge where the column is the derived item's. An
expression of the derived item's columns, each of which the arm passes on
from one source, gives the edge to that source whose value is the expression
over the source's columns (`exprEdge`, `Planner::passed_expression_edge`),
when the expression is strict, NULL where the source has no row as it is
where its columns are, or when no LEFT JOIN adds the derived item or the
source, so a binding a core keeps holds a row of each. -/
namespace SqliveNarrowing.Passed

/-- The edge from `member`, written in core `k`, to the source that derived
item `d`'s column `pc` passes on, through `d`. -/
def edge (s : Statement) (k : Nat) (member : Expr) (d pc : Nat) : Option Edge := do
  let (m, memberValue, affinity) ← RowMember.read s k member
  if d = m then none
  let (x, xc) ← passedSource s d pc
  let xa ← s.affinity x xc
  let value ← match memberValue with
    | .column _ => columnValue affinity xa x xc
    -- SQLite gives an expression's value the column's affinity.
    | .expr _ => some (PValue.column xc)
  pure
    { member := m, memberValue, affinity, partner := x, value, nested := true,
      via := some (d, .column pc) }

/-- The column of source `x` that derived item `d`'s arm passes on as `d`'s
column `c`, when it passes `c` on from `x`. -/
def renameFrom (s : Statement) (d x c : Nat) : Option Nat :=
  match passedSource s d c with
  | some (x', c') => if x' = x then some c' else none
  | none => none

/-- The edge from `member`, written in core `k`, to the source from which
derived item `d`'s arm passes on every column the expression `other` over
`d`'s columns names, through `d`: `other` over the source's columns, when
`other` is strict or no LEFT JOIN adds `d` or the source. -/
def exprEdge (s : Statement) (k : Nat) (member : Expr) (d : Nat) (other : Expr) : Option Edge := do
  let (m, memberValue, affinity) ← RowMember.read s k member
  if d = m then none
  let (_, c) ← other.refs.head?
  let (x, _) ← passedSource s d c
  if other.strict = false ∧ (s.alwaysBound d = false ∨ s.alwaysBound x = false) then none
  let value ← (PValue.expr other).passed d x (renameFrom s d x)
  pure
    { member := m, memberValue, affinity, partner := x, value, nested := true,
      via := some (d, .expr other) }

/-- What `passedSource` checks of the derived item and its source. -/
theorem passedSource_facts {s : Statement} {d pc x xc : Nat} (h : passedSource s d pc = some (x, xc)) :
    ∃ q v a, (s.item d).kind = .derived q v ∧ (s.query q).arms = [a] ∧ (s.item d).readable = false ∧
      (s.core a).aggregated = false ∧ (s.core a).windowed = false ∧
      d ∈ (s.core (s.item d).core).items ∧ (s.core a).output[pc]? = some (.expr (.col x xc)) ∧
      (s.item x).core = a ∧ (s.item x).readable = true := by
  unfold passedSource at h
  split at h
  · cases h
  · rename_i q v hkind
    split at h
    · rename_i a harms
      split at h
      · cases h
      · rename_i hok
        simp only [Bool.or_eq_true, Bool.not_eq_true', not_or, Bool.not_eq_true] at hok
        split at h
        · rename_i x' xc' hout
          split at h
          · rename_i hx
            cases h
            exact ⟨q, v, a, hkind, harms, hok.1.1.1, hok.1.1.2, hok.1.2, by simpa using hok.2, hout,
              hx.1, hx.2⟩
          · cases h
        · cases h
    · cases h

/-- A member column is its own item's column. -/
theorem read_col {s : Statement} {k m c m' : Nat} {v : PValue} {a : Affinity}
    (h : RowMember.read s k (.col m c) = some (m', v, a)) : m' = m ∧ v = .column c := by
  simp only [RowMember.read] at h
  split at h
  · obtain ⟨_, _, he⟩ := Option.map_eq_some_iff.mp h
    cases he
    exact ⟨rfl, rfl⟩
  · cases h

/-- What an edge `edge` reads says of it. -/
theorem edge_facts {s : Statement} {k : Nat} {member : Expr} {d pc : Nat} {e : Edge}
    (h : edge s k member d pc = some e) :
    (∀ b sub, e.memberValue.on (b e.member) = member.eval b sub) ∧ (s.item e.member).core = k ∧
    e.via = some (d, .column pc) ∧ e.Passed s d (.column pc) ∧ (s.item e.partner).readable = true ∧
    e.memberStrict s ∧
    d ≠ e.member ∧ ∀ m c, member = .col m c → e.member = m ∧ e.memberValue = .column c := by
  unfold edge at h
  simp only [bind, Option.bind] at h
  cases hmo : RowMember.read s k member with
  | none => simp [hmo] at h
  | some found =>
    obtain ⟨m, mv, aff⟩ := found
    obtain ⟨hk, hmv, hnone⟩ := RowMember.row_member_sound hmo
    simp only [hmo] at h
    split at h
    · cases h
    rename_i hdm
    cases hps : passedSource s d pc with
    | none => simp [hps] at h
    | some found =>
      obtain ⟨x, xc⟩ := found
      obtain ⟨_, _, _, _, _, _, _, _, _, _, _, hread⟩ := passedSource_facts hps
      -- The column passes on as the source's column `xc`.
      have hpassed : ∀ v : PValue, (∀ row, v.on row = cellOf row xc) →
          Edge.Passed s
            { member := m, memberValue := mv, affinity := aff, partner := x, value := v,
              nested := true, via := some (d, .column pc) } d (.column pc) := by
        intro v hv
        refine ⟨rfl, Or.inl (by rw [hv]; rfl), fun c => if c = pc then some xc else none, ⟨pc, xc, by simp⟩,
          fun c c' hc => ?_, fun rd row hcells => ?_⟩
        · by_cases hcp : c = pc
          · subst hcp
            simp only [ite_true, Option.some.injEq] at hc
            subst hc
            exact hps
          · simp [hcp] at hc
        · rw [hv row]
          exact (hcells pc xc (by simp)).symm
      simp only [hps] at h
      cases hxa : s.affinity x xc with
      | none => simp [hxa] at h
      | some xa =>
        simp only [hxa] at h
        cases mv with
        | column c =>
          cases hv : columnValue aff xa x xc with
          | none => simp [hv] at h
          | some v =>
            simp only [hv] at h
            cases h
            exact ⟨hmv, hk, rfl, hpassed v (columnValue_on hv), hread, hnone, hdm,
              fun m' c' hc => by subst hc; exact read_col hmo⟩
        | expr ex =>
          simp only at h
          cases h
          exact ⟨hmv, hk, rfl, hpassed _ fun _ => rfl, hread, hnone, hdm,
            fun m' c' hc => by subst hc; exact read_col hmo⟩

/-- What an edge `exprEdge` reads says of it, and of the expression: the
expression is strict or no LEFT JOIN adds `d` or the partner, and names
columns of `d` only, and some, and no subquery. -/
theorem exprEdge_facts {s : Statement} {k : Nat} {member : Expr} {d : Nat} {other : Expr} {e : Edge}
    (h : exprEdge s k member d other = some e) :
    (∀ b sub, e.memberValue.on (b e.member) = member.eval b sub) ∧ (s.item e.member).core = k ∧
    e.via = some (d, .expr other) ∧ e.Passed s d (.expr other) ∧
    (s.item e.partner).readable = true ∧ e.memberStrict s ∧ d ≠ e.member ∧
    (other.strict = true ∨ s.alwaysBound d = true ∧ s.alwaysBound e.partner = true) ∧
    other.refs ≠ [] ∧ other.queries = [] ∧ ∀ j ∈ other.items, j = d := by
  unfold exprEdge at h
  simp only [bind, Option.bind] at h
  cases hmo : RowMember.read s k member with
  | none => simp [hmo] at h
  | some found =>
    obtain ⟨m, mv, aff⟩ := found
    obtain ⟨hk, hmv, hnone⟩ := RowMember.row_member_sound hmo
    simp only [hmo] at h
    split at h
    · cases h
    rename_i hdm
    cases hr : other.refs.head? with
    | none => simp [hr] at h
    | some ref =>
      obtain ⟨j, c⟩ := ref
      simp only [hr] at h
      cases hps : passedSource s d c with
      | none => simp [hps] at h
      | some found =>
        obtain ⟨x, c0⟩ := found
        obtain ⟨_, _, _, _, _, _, _, _, _, _, _, hread⟩ := passedSource_facts hps
        simp only [hps] at h
        split at h
        · cases h
        rename_i hok
        -- The expression is strict, or no LEFT JOIN adds `d` or `x`.
        have hok' : other.strict = true ∨ s.alwaysBound d = true ∧ s.alwaysBound x = true := by
          cases hst : other.strict
          · right
            cases hd : s.alwaysBound d <;> cases hx : s.alwaysBound x <;> simp_all
          · exact Or.inl rfl
        cases hv : (PValue.expr other).passed d x (renameFrom s d x) with
        | none => simp [hv] at h
        | some v =>
          simp only [hv, pure, Option.some.injEq] at h
          subst h
          have hv' := hv
          simp only [PValue.passed] at hv'
          split at hv'
          · rename_i hc
            simp only [Bool.and_eq_true, Bool.not_eq_true', List.isEmpty_eq_false_iff,
              List.isEmpty_iff] at hc
            obtain ⟨⟨hrefs, hq⟩, hren⟩ := hc
            have hitems : ∀ i ∈ other.items, i = d := by
              intro i hi
              obtain ⟨⟨i', c'⟩, hc', rfl⟩ := List.mem_map.mp hi
              have := List.all_eq_true.mp hren (i', c') hc'
              simp only [Bool.and_eq_true, beq_iff_eq] at this
              exact this.1
            refine ⟨hmv, hk, rfl, ⟨rfl, ?_, renameFrom s d x, ⟨c, c0, by simp [renameFrom, hps]⟩,
              fun c c' hc => ?_, fun rd row hcells => PushDown.value_passed_sound hv hcells⟩,
              hread, hnone, hdm, hok', hrefs, hq, hitems⟩
            · rcases hok' with hst | ⟨_, hx⟩
              · -- A row of NULLs gives the expression NULL, as no row of the source does.
                left
                have hnull := PushDown.value_passed_sound hv (r := []) (row := none)
                  (fun _ _ _ => by simp [Row.cell, cellOf])
                rw [hnull]
                exact other.eval_null hrefs hq hst (fun _ _ _ => by simp [rowBind, cellOf, Row.cell])
              · exact Or.inr hx
            · unfold renameFrom at hc
              split at hc
              · rename_i x' c'' hps'
                split at hc
                · rename_i hx
                  cases hc
                  rw [hps', hx]
                · cases hc
              · cases hc
          · cases hv'

/-- An edge `edge` reads holds of a binding where the member's value is `x`
and column `pc` of the derived item's row holds `x`: the derived item's row
in the binding, when the item sits in the member's core, or its row in a
binding of its core, another, under the binding that gives a row of the
subquery its core is an arm of. -/
theorem edge_holds {s : Statement} {k : Nat} {member : Expr} {d pc : Nat} {e : Edge}
    (h : edge s k member d pc = some e) {db : Db} {f : Nat} {b : Bind} {sub : Subs} {x : Int}
    (hm : member.eval b sub = some x)
    (hrow : ((s.item d).core = k ∧ ∃ rd, b d = some rd ∧ rd.cell pc = some x) ∨
      (s.item d).core ≠ k ∧
      ∃ f', f = f' + 1 ∧ ∃ b' ∈ coreBindings s db (evalQuery s db f') b (s.item d).core,
        s.GivesRow db f' b (s.item d).core b' ∧ ∃ rd, b' d = some rd ∧ rd.cell pc = some x) :
    e.HoldsIn s db f b := by
  obtain ⟨hmv, hk, hvia, ⟨hnest, _⟩, _⟩ := edge_facts h
  refine Or.inr (Or.inr (Or.inr (Or.inl ⟨d, .column pc, x, hvia, hnest, by rw [hmv b sub]; exact hm, ?_⟩)))
  rcases hrow with ⟨hd, hr⟩ | hr
  · exact Or.inl ⟨by rw [hd, hk], hr⟩
  · exact Or.inr (Or.inl ⟨by rw [hk]; exact hr.1, hr.2⟩)

/-- An edge `exprEdge` reads, from a derived item of the member's core, holds
of a binding where the member's value is `x` and so is the expression's: a
binding with no row of `d` gives a strict expression NULL, and is no binding
a core keeps when no LEFT JOIN adds `d`. -/
theorem exprEdge_holds {s : Statement} {k : Nat} {member : Expr} {d : Nat} {other : Expr} {e : Edge}
    (h : exprEdge s k member d other = some e) (hd : (s.item d).core = k) {db : Db} {f : Nat}
    {b : Bind} {sub : Subs} {x : Int} (hm : member.eval b sub = some x)
    (ho : other.eval b sub = some x) : e.HoldsIn s db f b := by
  obtain ⟨hmv, hk, hvia, ⟨hnest, _⟩, _, _, _, hst, hrefs, hq, hitems⟩ := exprEdge_facts h
  rw [other.eval_rowBind hitems hq] at ho
  cases hbd : b d with
  | none =>
    rcases hst with hst | ⟨hab, _⟩
    · rw [hbd, other.eval_null hrefs hq hst (fun _ _ _ => by simp [rowBind, cellOf])] at ho
      cases ho
    · exact Or.inr (Or.inr (Or.inr (Or.inl ⟨d, .expr other, x, hvia, hnest,
        by rw [hmv b sub]; exact hm, Or.inr (Or.inr ⟨by rw [hd, hk], hab, hbd⟩)⟩)))
  | some rd =>
    rw [hbd] at ho
    exact Or.inr (Or.inr (Or.inr (Or.inl ⟨d, .expr other, x, hvia, hnest,
      by rw [hmv b sub]; exact hm, Or.inl ⟨by rw [hd, hk], rd, hbd, ho⟩⟩)))

end SqliveNarrowing.Passed
