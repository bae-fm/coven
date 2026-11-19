import SqliveNarrowing.Reader

/-! Rule 3: a column compared with the value a subquery returns,
`col = (SELECT x.c ...)`, `(SELECT x.c ...) = col` or `col IN (SELECT x.c
...)`, narrows the column's item to the rows whose column holds a value some
row of `x` in its scope holds (`scope_subquery.rs`). The subquery is one
SELECT whose one result column is a bare column of one of its own items,
readable alone, of the member column's affinity. -/
namespace SqliveNarrowing.SubqueryEdge

/-- The column a condition compares and the subquery it compares with. -/
def compared : Cond → Option (Expr × Nat)
  | .cmp .eq (.sub q) m | .cmp .eq m (.sub q) =>
    match m with
    | .col .. => some (m, q)
    | _ => none
  | .inQuery m q false =>
    match m with
    | .col .. => some (m, q)
    | _ => none
  | _ => none

/-- The edge from `member`, written in core `k`, to the source of the column
query `q` returns. -/
def edge (s : Statement) (k : Nat) (member : Expr) (q : Nat) : Option Edge := do
  let [sub] := (s.query q).arms | none
  let [.expr (.col p pc)] := (s.core sub).output | none
  if (s.item p).core ≠ sub ∨ !(s.item p).readable then none
  let pa ← s.affinity p pc
  let .col m c := member | none
  if (s.item m).core ≠ k then none
  let affinity ← s.affinity m c
  if affinity ≠ pa then none
  pure { member := m, memberValue := .column c, affinity, partner := p, value := .column pc, nested := true }

def readCondition : ReadCondition := fun s k cd => do
  let (member, q) ← compared cd
  let e ← edge s k member q
  pure { edges := [e] }

/-! The proof: the condition true of a binding gives a binding of the
subquery's core, under the same binding, whose source row holds the member
column's value. -/

theorem edge_facts {s : Statement} {k : Nat} {member : Expr} {q : Nat} {e : Edge}
    (h : edge s k member q = some e) :
    ∃ sub pc c, (s.query q).arms = [sub] ∧ (s.core sub).output = [.expr (.col e.partner pc)] ∧
      e.value = .column pc ∧ (s.item e.partner).core = sub ∧ (s.item e.partner).readable ∧
      member = .col e.member c ∧ e.memberValue = .column c ∧ (s.item e.member).core = k ∧
      e.nested = true := by
  unfold edge at h
  simp only [bind, Option.bind] at h
  split at h
  · rename_i sub harms
    split at h
    · rename_i p pc hout
      split at h
      · cases h
      rename_i hbad
      simp only [not_or, Bool.not_eq_true] at hbad
      cases hpa : s.affinity p pc with
      | none => simp [hpa] at h
      | some pa =>
        simp only [hpa] at h
        cases member
        case lit | add | greatest | sub | coalesce | iif | isNull => simp at h
        case col m c =>
          simp only at h
          split at h
          · cases h
          rename_i hk
          cases hma : s.affinity m c with
          | none => simp [hma] at h
          | some ma =>
            simp only [hma] at h
            split at h
            · cases h
            · cases h
              exact ⟨sub, pc, _, harms, hout, rfl, by simpa using hbad.1, by simpa using hbad.2,
                rfl, rfl, by simpa using hk, rfl⟩
    · cases h
  · cases h

/-- A scalar subquery's value equal to `a`: `a` is a value, and the first
column of a row the subquery returns. -/
theorem scalar_eq_true {rows : List Row} {a : Value}
    (h : Op.apply .eq ((rows.head?.bind List.head?).join) a = some true ∨
      Op.apply .eq a ((rows.head?.bind List.head?).join) = some true) :
    ∃ x row, a = some x ∧ row ∈ rows ∧ row.cell 0 = some x := by
  rcases rows with _ | ⟨row, rest⟩
  · simp [Op.apply] at h
  rcases row with _ | ⟨v, vs⟩
  · simp [Op.apply] at h
  cases v <;> cases a <;> simp [Op.apply, Op.compare] at h
  rename_i y z
  have hz : y = z := by rcases h with h | h <;> omega
  subst hz
  exact ⟨y, some y :: vs, rfl, by simp, by simp [Row.cell]⟩

/-- The condition `compared` reads, true of a binding, gives the member's
value and a row the subquery returns whose first column holds it. -/
theorem compared_sound {cd : Cond} {member : Expr} {q : Nat} (h : compared cd = some (member, q))
    {b : Bind} {sub : Subs} (hc : isTrue (cd.eval b sub)) :
    ∃ x row, member.eval b sub = some x ∧ row ∈ sub q ∧ row.cell 0 = some x := by
  unfold compared at h
  split at h
  · split at h
    · cases h
      simp only [Cond.eval, Expr.eval, isTrue, beq_iff_eq] at hc
      exact scalar_eq_true (Or.inl hc)
    · cases h
  · split at h
    · cases h
      simp only [Cond.eval, Expr.eval, isTrue, beq_iff_eq] at hc
      exact scalar_eq_true (Or.inr hc)
    · cases h
  · split at h
    · cases h
      simp only [Cond.eval, isTrue, beq_iff_eq, Bool.false_eq_true, ↓reduceIte] at hc
      revert hc
      generalize Expr.eval b sub _ = a
      intro hc
      cases a with
      | none =>
        simp only [memberTruth] at hc
        split at hc <;> simp_all
      | some x =>
        simp only [memberTruth] at hc
        split at hc
        · rename_i hx
          simp only [List.contains_iff_exists_mem_beq, List.mem_map, beq_iff_eq] at hx
          obtain ⟨_, ⟨row, hrow, rfl⟩, hcell⟩ := hx
          exact ⟨x, row, rfl, hrow, hcell.symm⟩
        · split at hc <;> simp_all
    · cases h
  · cases h

/-- Rule 3's theorem: a column compared with a subquery's value, true of a
binding, takes a value a binding of the subquery's core gives its source. -/
theorem subquery_edge_sound : ReadCondition.Sound readCondition where
  terms := by
    intro s k cd n h i t ht
    cases hcmp : compared cd with
    | none => simp [readCondition, hcmp] at h
    | some mq =>
      obtain ⟨member, q⟩ := mq
      cases hed : edge s k member q with
      | none => simp [readCondition, hcmp, hed] at h
      | some e' =>
        simp only [readCondition, hcmp, hed, bind, Option.bind, pure, Option.some.injEq] at h
        subst h
        simp at ht
  edges := by
    intro s k cd n h e he
    cases hcmp : compared cd with
    | none => simp [readCondition, hcmp] at h
    | some mq =>
    obtain ⟨member, q⟩ := mq
    cases hedge : edge s k member q with
    | none => simp [readCondition, hcmp, hedge] at h
    | some e' =>
    simp only [readCondition, hcmp, hedge, bind, Option.bind, pure, Option.some.injEq] at h
    subst h
    simp only [List.mem_singleton] at he
    subst he
    obtain ⟨sub, pc, mc, harms, hout, hval, hpcore, hread, hmem, hmv, hk, hnest⟩ := edge_facts hedge
    refine ⟨hk, fun h => by simp [hnest] at h, fun h => by simp [hnest] at h, hread,
      fun db f b hc => ?_⟩
    -- The condition gives the member's value and a row of the subquery with it.
    have hrow : ∃ x row, member.eval b (evalQuery s db f b) = some x ∧
        row ∈ evalQuery s db f b q ∧ row.cell 0 = some x := compared_sound hcmp hc
    obtain ⟨x, row, hmx, hrowq, hcell⟩ := hrow
    refine Or.inl ⟨x, ?_, ?_⟩
    · rw [hmem] at hmx
      simpa [Expr.eval, hmv, PValue.on] using hmx
    · simp only [hnest, ↓reduceIte]
      cases f with
      | zero => simp [evalQuery] at hrowq
      | succ f' =>
        refine ⟨f', rfl, ?_⟩
        rcases mem_query_single harms hout hrowq with hnone | ⟨b', hb', hrow'⟩
        · subst hnone
          simp [Row.cell] at hcell
        · subst hrow'
          simp only [Expr.eval, Row.cell, List.getElem?_cons_zero, Option.join_some] at hcell
          cases hp : b' e.partner with
          | none => simp [hp, cellOf] at hcell
          | some r =>
            rw [hpcore]
            refine ⟨b', hb', r, hp, ?_⟩
            simp only [hp, cellOf] at hcell
            simp [hval, PValue.eval, hcell]

end SqliveNarrowing.SubqueryEdge
