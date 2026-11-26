import SqliveNarrowing.Rules.Passed

/-! Rule 3: a column compared with the value a subquery returns,
`col = (SELECT x.c ...)`, `(SELECT x.c ...) = col` or `col IN (SELECT x.c
...)`, narrows the column's item to the rows whose column holds a value some
row of `x` in its scope holds (`scope_subquery.rs`). The subquery is one
SELECT whose one result column is a bare column of one of its own items,
readable alone, of the member column's affinity or of none (`columnValue`).
A comparison reads only the subquery's first column, so the model's
subquery may return more columns after it: its ORDER BY terms that the SQL
doesn't return. A column of a derived item of the subquery's core that a
lookup can't read alone gives the edge to the source its arm passes the
column on from, through the item (`Passed`, `throughEdge`). -/
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
query `q` returns, when a lookup reads the source alone. -/
def sourceEdge (s : Statement) (k : Nat) (member : Expr) (q : Nat) : Option Edge := do
  let [sub] := (s.query q).arms | none
  let some (OutCol.expr (Expr.col p pc)) := (s.core sub).output[0]? | none
  if (s.item p).core ≠ sub ∨ !(s.item p).readable then none
  let pa ← s.affinity p pc
  let .col m c := member | none
  if (s.item m).core ≠ k then none
  let affinity ← s.affinity m c
  let value ← columnValue affinity pa p pc
  pure { member := m, memberValue := .column c, affinity, partner := p, value, nested := true }

/-- The edge from the column `member`, written in core `k`, through the
derived item whose column query `q` returns, when a lookup can't read the
item alone (`Planner::arm_edge`). -/
def throughEdge (s : Statement) (k : Nat) (member : Expr) (q : Nat) : Option Edge := do
  let [sub] := (s.query q).arms | none
  let some (OutCol.expr (Expr.col d pc)) := (s.core sub).output[0]? | none
  if (s.item d).core ≠ sub ∨ sub = k ∨ (s.item d).readable then none
  let .col .. := member | none
  Passed.edge s k member d pc

/-- The edge from `member` that query `q`'s column gives: `sourceEdge`'s, or
else `throughEdge`'s. -/
def edge (s : Statement) (k : Nat) (member : Expr) (q : Nat) : Option Edge :=
  (sourceEdge s k member q).orElse fun _ => throughEdge s k member q

def readCondition : ReadCondition := fun s k cd => do
  let (member, q) ← compared cd
  let e ← edge s k member q
  pure { edges := [e] }

/-! The proof: the condition true of a binding gives a binding of the
subquery's core, under the same binding, whose source row holds the member
column's value. -/

theorem sourceEdge_facts {s : Statement} {k : Nat} {member : Expr} {q : Nat} {e : Edge}
    (h : sourceEdge s k member q = some e) :
    ∃ sub pc c, (s.query q).arms = [sub] ∧ (s.core sub).output[0]? = some (.expr (.col e.partner pc)) ∧
      (∀ r, e.value.eval r = r.cell pc) ∧ (s.item e.partner).core = sub ∧ (s.item e.partner).readable ∧
      member = .col e.member c ∧ e.memberValue = .column c ∧ (s.item e.member).core = k ∧
      e.nested = true ∧ e.via = none := by
  unfold sourceEdge at h
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
            cases hv : columnValue ma pa p pc with
            | none => simp [hv] at h
            | some v =>
              simp only [hv] at h
              cases h
              exact ⟨sub, pc, _, harms, hout, columnValue_eval hv, by simpa using hbad.1,
                by simpa using hbad.2, rfl, rfl, by simpa using hk, rfl, rfl⟩
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

/-- The subquery `compared` reads is one the condition holds. -/
theorem compared_queries {cd : Cond} {member : Expr} {q : Nat} (h : compared cd = some (member, q)) :
    q ∈ cd.queries := by
  unfold compared at h
  split at h
  · split at h
    · cases h; simp [Cond.queries, Expr.queries]
    · cases h
  · split at h
    · cases h; simp [Cond.queries, Expr.queries]
    · cases h
  · split at h
    · cases h; simp [Cond.queries]
    · cases h
  · cases h

/-- An edge `throughEdge` reads is `Passed.edge`'s, through an item of the
subquery's one core whose column the subquery returns first. -/
theorem throughEdge_facts {s : Statement} {k : Nat} {member : Expr} {q : Nat} {e : Edge}
    (h : throughEdge s k member q = some e) :
    ∃ sub d pc m c, (s.query q).arms = [sub] ∧ (s.core sub).output[0]? = some (.expr (.col d pc)) ∧
      (s.item d).core = sub ∧ sub ≠ k ∧ member = .col m c ∧ Passed.edge s k member d pc = some e := by
  unfold throughEdge at h
  simp only [bind, Option.bind] at h
  split at h
  · rename_i sub harms
    split at h
    · rename_i d pc hout
      split at h
      · cases h
      rename_i hok
      simp only [not_or, Bool.not_eq_true, ne_eq, Decidable.not_not] at hok
      split at h
      · rename_i m c
        exact ⟨sub, d, pc, m, c, harms, hout, hok.1, hok.2.1, rfl, h⟩
      · cases h
    · cases h
  · cases h

/-- What an edge `edge` reads says of it: its member is the compared column,
of the core; it waits for its partner, which a lookup reads alone; it sits
in an arm of the subquery, or reads through an item of that arm; and it
holds of a binding where the member's value is the first column of a row
the subquery returns. -/
theorem edge_props {s : Statement} {k : Nat} {member : Expr} {q : Nat} {e : Edge}
    (h : edge s k member q = some e) :
    ∃ c, member = .col e.member c ∧ e.memberValue = .column c ∧ (s.item e.member).core = k ∧
      e.nested = true ∧ (s.item e.partner).readable ∧ e.memberStrict s ∧
      (∀ queries, q ∈ queries → e.NestedIn s queries) ∧
      ∀ db f b x row, cellOf (b e.member) c = some x → row ∈ evalQuery s db f b q →
        row.cell 0 = some x → e.HoldsIn s db f b := by
  unfold edge at h
  cases hse : sourceEdge s k member q with
  | some e' =>
    rw [hse] at h
    cases h
    obtain ⟨sub, pc, mc, harms, hout, hval, hpcore, hread, hmem, hmv, hk, hnest, hvia⟩ :=
      sourceEdge_facts hse
    refine ⟨mc, hmem, hmv, hk, hnest, hread, Or.inl (by rw [hmv]; rfl),
      fun queries hq => Edge.nestedIn_of hvia fun _ => ⟨q, hq, by rw [hpcore, harms]; simp⟩,
      fun db f b x row hm hrowq hcell => ?_⟩
    refine Or.inl ⟨x, by simpa [hmv, PValue.on] using hm, ?_⟩
    simp only [hnest, ↓reduceIte]
    cases f with
    | zero => simp [evalQuery] at hrowq
    | succ f' =>
      refine ⟨hvia, f', rfl, ?_⟩
      rcases query_row_at harms hout hrowq with hnone | ⟨b', hb', hcell', hrow'⟩
      · simp [Row.cell, hnone] at hcell
      · simp only [Row.cell, hcell', Option.join_some, Expr.eval] at hcell
        cases hp : b' e.partner with
        | none => simp [hp, cellOf] at hcell
        | some r =>
          rw [hpcore]
          refine ⟨b', hb', ⟨q, by rw [harms]; simp, fun _ hagg hwin => ?_⟩, r, hp, ?_⟩
          · rw [← hrow' hagg hwin]
            exact hrowq
          · simp only [hp, cellOf] at hcell
            simp [hval, hcell]
  | none =>
    rw [hse] at h
    obtain ⟨sub, d, pc, m, c, harms, hout, hdcore, hsk, rfl, hp⟩ := throughEdge_facts h
    obtain ⟨_, hk, hvia, hpassed, hread, hstrict, _, hcol⟩ := Passed.edge_facts hp
    obtain ⟨rfl, hmvc⟩ := hcol m c rfl
    refine ⟨c, rfl, hmvc, hk, hpassed.1, hread, hstrict,
      fun queries hq => ⟨fun _ hv => (by rw [hvia] at hv; cases hv), fun d' pc' hv => ?_⟩,
      fun db f b x row hm hrowq hcell => ?_⟩
    · rw [hvia] at hv
      obtain ⟨rfl, rfl⟩ := Prod.mk.inj (Option.some.inj hv)
      exact ⟨hpassed, Or.inr ⟨q, hq, by rw [hdcore, harms]; simp⟩⟩
    · cases f with
      | zero => simp [evalQuery] at hrowq
      | succ f' =>
        rcases query_row_at harms hout hrowq with hnone | ⟨b', hb', hcell', hrow'⟩
        · simp [Row.cell, hnone] at hcell
        · simp only [Row.cell, hcell', Option.join_some, Expr.eval] at hcell
          cases hbd : b' d with
          | none => simp [hbd, cellOf] at hcell
          | some rd =>
            simp only [hbd, cellOf] at hcell
            refine Passed.edge_holds hp (b := b) (sub := evalQuery s db (f' + 1) b)
              (by simpa [Expr.eval] using hm) (Or.inr ⟨by rw [hdcore]; exact hsk, f', rfl, b',
                by rw [hdcore]; exact hb',
                ⟨q, by rw [hdcore, harms]; simp, fun _ hagg hwin => ?_⟩, rd, hbd, hcell⟩)
            rw [hdcore] at hagg hwin ⊢
            rw [← hrow' hagg hwin]
            exact hrowq

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
    obtain ⟨c, hmem, hmv, hk, hnest, hread, hstrict, hin, hh⟩ := edge_props hedge
    refine ⟨hk, fun h => by simp [hnest] at h, fun h => by simp [hnest] at h, hread, hstrict,
      hin _ (compared_queries hcmp), fun db f b hc => ?_⟩
    -- The condition gives the member's value and a row of the subquery with it.
    obtain ⟨x, row, hmx, hrowq, hcell⟩ := compared_sound hcmp hc
    rw [hmem] at hmx
    exact hh db f b x row (by simpa [Expr.eval] using hmx) hrowq hcell

end SqliveNarrowing.SubqueryEdge
