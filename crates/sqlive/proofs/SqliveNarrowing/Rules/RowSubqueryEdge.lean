import SqliveNarrowing.Reader

/-! A row value compared with a subquery's row, `(m1, ..., mn) = (SELECT
x.c1, ..., x.cn ...)` with the subquery on either side, or `(m1, ..., mn) IN
(SELECT x.c1, ..., x.cn ...)`, narrows each `mi` as `mi = (SELECT x.ci ...)`
alone would: to the rows whose column holds a value some row of `x` in its
scope holds (`scope_subquery.rs`). The comparison holds only where every
pair is equal and neither side NULL, so each pair's edge holds of every
binding the comparison keeps. A pair gives an edge when its row side is a
bare column of one of the core's items and the subquery, one SELECT,
returns in its place a bare column of one of its own items, readable alone,
of the same affinity; any other pair gives none. -/
namespace SqliveNarrowing.RowSubqueryEdge

/-- The edge from `member`, written in core `k`, to the item whose column
the one arm of query `q` returns at `position`. -/
def edge (s : Statement) (k : Nat) (member : Expr) (q position : Nat) : Option Edge := do
  let [sub] := (s.query q).arms | none
  let some (.expr (.col p pc)) := (s.core sub).output[position]? | none
  if (s.item p).core ≠ sub ∨ !(s.item p).readable then none
  let pa ← s.affinity p pc
  let .col m c := member | none
  if (s.item m).core ≠ k then none
  let affinity ← s.affinity m c
  if affinity ≠ pa then none
  pure { member := m, column := c, affinity, partner := p, value := .column pc, nested := true }

/-- The reader: `(m1, ..., mn) = (SELECT ...)` or `IN (SELECT ...)`, each
pair's edge. -/
def readCondition : ReadCondition := fun s k cd =>
  match cd with
  | .rowSub vs q _ false =>
    some { edges := (List.range vs.length).filterMap fun position => do
      edge s k (← vs[position]?) q position }
  | _ => none

/-! The proof. -/

theorem edge_facts {s : Statement} {k : Nat} {member : Expr} {q position : Nat} {e : Edge}
    (h : edge s k member q position = some e) :
    ∃ sub pc, (s.query q).arms = [sub] ∧
      (s.core sub).output[position]? = some (.expr (.col e.partner pc)) ∧
      e.value = .column pc ∧ (s.item e.partner).core = sub ∧ (s.item e.partner).readable ∧
      member = .col e.member e.column ∧ (s.item e.member).core = k ∧ e.nested = true := by
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
        case lit | add | greatest | sub => simp at h
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
              exact ⟨sub, pc, harms, hout, rfl, by simpa using hbad.1, by simpa using hbad.2,
                rfl, by simpa using hk, rfl⟩
    · cases h
  · cases h

theorem tand_true {x y : Truth} (h : tand x y = some true) : x = some true ∧ y = some true := by
  revert h
  rcases x with _ | _ | _ <;> rcases y with _ | _ | _ <;> simp [tand]

/-- A row value equal to a row: each value of the one at a position the
other has is a value, equal to the other's there. -/
theorem rowEqTruth_at : ∀ {as bs : List Value} {i : Nat} {a v : Value},
    rowEqTruth as bs = some true → as[i]? = some a → bs[i]? = some v → ∃ x, a = some x ∧ v = some x
  | a' :: as, b' :: bs, 0, a, v, h, ha, hv => by
    simp only [rowEqTruth] at h
    simp only [List.getElem?_cons_zero, Option.some.injEq] at ha hv
    subst ha hv
    have h1 := (tand_true h).1
    revert h1
    cases a' <;> cases b' <;> simp [Op.apply, Op.compare] <;> omega
  | a' :: as, b' :: bs, i + 1, a, v, h, ha, hv => by
    simp only [rowEqTruth] at h
    simp only [List.getElem?_cons_succ] at ha hv
    exact rowEqTruth_at (tand_true h).2 ha hv
  | [], _, _, _, _, _, ha, _ => by simp at ha
  | _ :: _, [], _, _, _, _, _, hv => by simp at hv

/-- A row value IN rows: some row equals it. -/
theorem rowMemberTruth_true {vs : List Value} {rows : List (List Value)}
    (h : rowMemberTruth vs rows = some true) : ∃ r ∈ rows, rowEqTruth vs r = some true := by
  unfold rowMemberTruth at h
  induction rows with
  | nil => simp at h
  | cons r rest ih =>
    simp only [List.foldr_cons] at h
    cases hr : rowEqTruth vs r with
    | some t =>
      cases t
      · rw [hr] at h
        obtain ⟨r', hr', heq⟩ := ih (by revert h; cases List.foldr _ _ rest <;> simp [tor] <;>
          (try rename_i t; cases t) <;> simp)
        exact ⟨r', by simp [hr'], heq⟩
      · exact ⟨r, by simp, hr⟩
    | none =>
      rw [hr] at h
      obtain ⟨r', hr', heq⟩ := ih (by revert h; cases List.foldr _ _ rest <;> simp [tor] <;>
        (try rename_i t; cases t) <;> simp)
      exact ⟨r', by simp [hr'], heq⟩

/-- A row of a one-arm query holds, at a position whose result column is
`e`, NULL or `e`'s value in a binding the arm keeps. -/
theorem query_row_at {s : Statement} {db : Db} {f : Nat} {b : Bind} {q k i : Nat} {e : Expr}
    (ha : (s.query q).arms = [k]) (ho : (s.core k).output[i]? = some (.expr e)) {r : Row}
    (hr : r ∈ evalQuery s db (f + 1) b q) :
    r[i]? = some none ∨ ∃ b' ∈ coreBindings s db (evalQuery s db f) b k,
      r[i]? = some (e.eval b' (evalQuery s db f b')) := by
  simp only [evalQuery, queryRows, ha, List.map_cons, List.map_nil, combineAll_single] at hr
  have hr := mem_result (mem_post hr)
  unfold Core.rows at hr
  split at hr
  · simp only [List.mem_map] at hr
    obtain ⟨g, hg, rfl⟩ := hr
    simp only [List.getElem?_map, ho, Option.map_some, OutCol.aggregate]
    split
    · rename_i b' hlast
      right
      refine ⟨b', ?_, rfl⟩
      have hb : b' ∈ g := List.mem_of_getLast? hlast
      split at hg
      · simp only [List.mem_singleton] at hg
        subst hg
        exact hb
      · exact mem_groupsOf hg hb
    · left; rfl
  · simp only [List.mem_map] at hr
    obtain ⟨⟨b', j⟩, hb', rfl⟩ := hr
    right
    exact ⟨b', List.fst_mem_of_mem_zipIdx hb', by simp [List.getElem?_map, ho, OutCol.plain]⟩

/-- The theorem: a row value compared with a subquery's row, true of a
binding, makes each pair's member column hold a value the pair's partner
gives in a binding of the subquery's core under the same binding. -/
theorem row_subquery_edge_sound : ReadCondition.Sound readCondition where
  terms := by
    intro s k cd n h i t ht
    unfold readCondition at h
    split at h
    · cases h; simp at ht
    · cases h
  edges := by
    intro s k cd n h e he
    unfold readCondition at h
    split at h
    · rename_i vs q isIn
      cases h
      simp only [List.mem_filterMap, List.mem_range] at he
      obtain ⟨position, _, hpos⟩ := he
      cases hm : vs[position]? with
      | none => simp [hm] at hpos
      | some member =>
      simp only [hm, bind, Option.bind] at hpos
      obtain ⟨sub, pc, harms, hout, hval, hpcore, hread, hmem, hk, hnest⟩ := edge_facts hpos
      refine ⟨hk, fun h => by simp [hnest] at h, hread, fun db f b hc => ?_⟩
      -- The comparison gives a row of the subquery equal to the row value.
      have hrow : ∃ row ∈ evalQuery s db f b q, rowEqTruth
          (vs.map (Expr.eval b (evalQuery s db f b))) row = some true := by
        simp only [Cond.eval, isTrue, beq_iff_eq, Bool.false_eq_true, ↓reduceIte] at hc
        split at hc
        · exact rowMemberTruth_true hc
        · split at hc
          · rename_i row hhead
            exact ⟨row, List.mem_of_head? hhead, hc⟩
          · cases hc
      obtain ⟨row, hrowq, heq⟩ := hrow
      cases f with
      | zero => simp [evalQuery] at hrowq
      | succ f' =>
      have hvs : (vs.map (Expr.eval b (evalQuery s db (f' + 1) b)))[position]? =
          some (cellOf (b e.member) e.column) := by
        simp [List.getElem?_map, hm, hmem, Expr.eval]
      rcases query_row_at harms hout hrowq with hnull | ⟨b', hb', hcell⟩
      · obtain ⟨x, _, hx⟩ := rowEqTruth_at heq hvs hnull
        cases hx
      · obtain ⟨x, hx, hy⟩ := rowEqTruth_at heq hvs hcell
        refine ⟨x, hx, ?_⟩
        simp only [hnest, ↓reduceIte]
        refine ⟨f', rfl, ?_⟩
        cases hp : b' e.partner with
        | none => simp [Expr.eval, hp, cellOf] at hy
        | some r =>
          rw [hpcore]
          refine ⟨b', hb', r, hp, ?_⟩
          simp only [Expr.eval, hp, cellOf] at hy
          simp [hval, PValue.eval, hy]
    · cases h

end SqliveNarrowing.RowSubqueryEdge
