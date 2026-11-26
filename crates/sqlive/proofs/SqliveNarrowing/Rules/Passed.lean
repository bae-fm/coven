import SqliveNarrowing.Rules.RowMember

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
(`SubqueryEdge`) give this edge where the column is the derived item's. -/
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
  pure { member := m, memberValue, affinity, partner := x, value, nested := true, via := some (d, pc) }

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
    e.via = some (d, pc) ∧ e.Passed s d pc ∧ (s.item e.partner).readable = true ∧ e.memberStrict s ∧
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
            exact ⟨hmv, hk, rfl, ⟨rfl, xc, hps, columnValue_eval hv⟩, hread, hnone, hdm,
              fun m' c' hc => by subst hc; exact read_col hmo⟩
        | expr ex =>
          simp only at h
          cases h
          exact ⟨hmv, hk, rfl, ⟨rfl, xc, hps, fun _ => rfl⟩, hread, hnone, hdm,
            fun m' c' hc => by subst hc; exact read_col hmo⟩

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
  refine Or.inr (Or.inr (Or.inr ⟨d, pc, x, hvia, hnest, by rw [hmv b sub]; exact hm, ?_⟩))
  rcases hrow with ⟨hd, hr⟩ | hr
  · exact Or.inl ⟨by rw [hd, hk], hr⟩
  · exact Or.inr ⟨by rw [hk]; exact hr.1, hr.2⟩

end SqliveNarrowing.Passed
