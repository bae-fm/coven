import SqliveNarrowing.Changes
import SqliveNarrowing.Rows

/-! What a condition reader promises, and the facts about evaluating a
condition that each rule's proof uses.

A reader reads a condition of core `k` as terms on items' rows and edges
between items. Its promise (`ReadCondition.Sound`): whenever the condition
holds of a binding, each term holds of its item's row, and each edge's
member column holds a value that a row of its partner gives. A rule proves
its reader keeps this promise, and the planner's proof uses only the
promise, so a new rule adds its reader and its proof and changes no other
proof. -/
namespace SqliveNarrowing

/-! Evaluation reads only the cells and subqueries an expression names. -/

theorem Expr.eval_congr (e : Expr) {b b' : Bind} {sub sub' : Subs}
    (hb : ∀ i c, (i, c) ∈ e.refs → cellOf (b i) c = cellOf (b' i) c)
    (hs : ∀ q ∈ e.queries, sub q = sub' q) : e.eval b sub = e.eval b' sub' := by
  induction e with
  | col i c => exact hb i c (by simp [Expr.refs])
  | lit v => rfl
  | add l r ihl ihr | greatest l r ihl ihr =>
    simp only [Expr.eval]
    rw [ihl (fun i c h => hb i c (by simp [Expr.refs, h]))
        (fun q h => hs q (by simp [Expr.queries, h])),
      ihr (fun i c h => hb i c (by simp [Expr.refs, h]))
        (fun q h => hs q (by simp [Expr.queries, h]))]
  | sub q => simp [Expr.eval, hs q (by simp [Expr.queries])]

theorem Cond.eval_congr (cd : Cond) {b b' : Bind} {sub sub' : Subs}
    (hb : ∀ i c, (i, c) ∈ cd.refs → cellOf (b i) c = cellOf (b' i) c)
    (hs : ∀ q ∈ cd.queries, sub q = sub' q) : cd.eval b sub = cd.eval b' sub' := by
  induction cd with
  | cmp op l r =>
    simp only [Cond.eval]
    rw [l.eval_congr (fun i c h => hb i c (by simp [Cond.refs, h]))
        (fun q h => hs q (by simp [Cond.queries, h])),
      r.eval_congr (fun i c h => hb i c (by simp [Cond.refs, h]))
        (fun q h => hs q (by simp [Cond.queries, h]))]
  | null e n =>
    simp only [Cond.eval]
    rw [e.eval_congr (fun i c h => hb i c (by simp [Cond.refs, h]))
        (fun q h => hs q (by simp [Cond.queries, h]))]
  | inList e vs =>
    simp only [Cond.eval]
    rw [e.eval_congr (fun i c h => hb i c (by simp [Cond.refs, h]))
        (fun q h => hs q (by simp [Cond.queries, h]))]
    congr 1
    apply List.map_congr_left
    intro v hv
    exact v.eval_congr
      (fun i c h => hb i c (by simp only [Cond.refs, List.mem_append, List.mem_flatMap]; exact Or.inr ⟨v, hv, h⟩))
      (fun q h => hs q (by simp only [Cond.queries, List.mem_append, List.mem_flatMap]; exact Or.inr ⟨v, hv, h⟩))
  | inQuery e q n =>
    simp only [Cond.eval]
    rw [e.eval_congr (fun i c h => hb i c (by simp [Cond.refs, h]))
        (fun q h => hs q (by simp [Cond.queries, h])),
      hs q (by simp [Cond.queries])]
  | «exists» q n => simp [Cond.eval, hs q (by simp [Cond.queries])]
  | rowSub vs q isIn n =>
    simp only [Cond.eval]
    have hvs : vs.map (Expr.eval b sub) = vs.map (Expr.eval b' sub') := by
      apply List.map_congr_left
      intro v hv
      exact v.eval_congr
        (fun i c h => hb i c (by simp only [Cond.refs, List.mem_flatMap]; exact ⟨v, hv, h⟩))
        (fun q h => hs q (by
          simp only [Cond.queries, List.mem_append, List.mem_flatMap]; exact Or.inl ⟨v, hv, h⟩))
    rw [hvs, hs q (by simp [Cond.queries])]
  | rowEq ls rs =>
    simp only [Cond.eval]
    have hls : ls.map (Expr.eval b sub) = ls.map (Expr.eval b' sub') := by
      apply List.map_congr_left
      intro v hv
      exact v.eval_congr
        (fun i c h => hb i c (by
          simp only [Cond.refs, List.mem_append, List.mem_flatMap]; exact Or.inl ⟨v, hv, h⟩))
        (fun q h => hs q (by
          simp only [Cond.queries, List.mem_append, List.mem_flatMap]; exact Or.inl ⟨v, hv, h⟩))
    have hrs : rs.map (Expr.eval b sub) = rs.map (Expr.eval b' sub') := by
      apply List.map_congr_left
      intro v hv
      exact v.eval_congr
        (fun i c h => hb i c (by
          simp only [Cond.refs, List.mem_append, List.mem_flatMap]; exact Or.inr ⟨v, hv, h⟩))
        (fun q h => hs q (by
          simp only [Cond.queries, List.mem_append, List.mem_flatMap]; exact Or.inr ⟨v, hv, h⟩))
    rw [hls, hrs]
  | and l r ihl ihr =>
    simp only [Cond.eval]
    rw [ihl (fun i c h => hb i c (by simp [Cond.refs, h]))
        (fun q h => hs q (by simp [Cond.queries, h])),
      ihr (fun i c h => hb i c (by simp [Cond.refs, h]))
        (fun q h => hs q (by simp [Cond.queries, h]))]
  | or l r ihl ihr =>
    simp only [Cond.eval]
    rw [ihl (fun i c h => hb i c (by simp [Cond.refs, h]))
        (fun q h => hs q (by simp [Cond.queries, h])),
      ihr (fun i c h => hb i c (by simp [Cond.refs, h]))
        (fun q h => hs q (by simp [Cond.queries, h]))]
  | not c ih => simp only [Cond.eval]; rw [ih hb hs]

/-- An expression with a column and no subquery is NULL where every column
it names is. -/
theorem Expr.eval_null (e : Expr) {b : Bind} {sub : Subs} (hr : e.refs ≠ [])
    (hq : e.queries = []) (hn : ∀ i c, (i, c) ∈ e.refs → cellOf (b i) c = none) :
    e.eval b sub = none := by
  induction e with
  | col i c => exact hn i c (by simp [Expr.refs])
  | lit v => simp [Expr.refs] at hr
  | add l r ihl ihr | greatest l r ihl ihr =>
    simp only [Expr.queries, List.append_eq_nil_iff] at hq
    simp only [Expr.eval]
    by_cases hl : l.refs = []
    · have hr' : r.refs ≠ [] := by simpa [Expr.refs, hl] using hr
      rw [ihr hr' hq.2 (fun i c h => hn i c (by simp [Expr.refs, h]))]
      split <;> simp_all
    · rw [ihl hl hq.1 (fun i c h => hn i c (by simp [Expr.refs, h]))]
  | sub q => simp [Expr.queries] at hq

/-- A condition naming one item's cells only, and no subquery, holds of a
binding as it does of the binding where every item has that item's row. -/
theorem Cond.eval_rowBind (cd : Cond) {b : Bind} {sub : Subs} {i : Nat}
    (hi : ∀ j ∈ cd.items, j = i) (hq : cd.queries = []) :
    cd.eval b sub = cd.eval (rowBind (b i)) noSubs := by
  apply cd.eval_congr
  · intro j c h
    have : j = i := hi j (by simp only [Cond.items, List.mem_map]; exact ⟨(j, c), h, rfl⟩)
    subst this
    rfl
  · intro q h
    simp [hq] at h

theorem Expr.eval_rowBind (e : Expr) {b : Bind} {sub : Subs} {i : Nat}
    (hi : ∀ j ∈ e.items, j = i) (hq : e.queries = []) :
    e.eval b sub = e.eval (rowBind (b i)) noSubs := by
  apply e.eval_congr
  · intro j c h
    have : j = i := hi j (by simp only [Expr.items, List.mem_map]; exact ⟨(j, c), h, rfl⟩)
    subst this
    rfl
  · intro q h
    simp [hq] at h

theorem mem_of_eraseDups_singleton {l : List Nat} {i : Nat} (h : l.eraseDups = [i]) :
    ∀ j ∈ l, j = i := by
  intro j hj
  have : j ∈ l.eraseDups := List.mem_eraseDups.mpr hj
  simpa [h] using this

/-! The promise. -/

/-- An edge holds of a binding `b` whose conditions read subqueries with fuel
`f`: the member's column holds a value `x`, and a row of the partner gives
`x`: the partner's row in `b`, or, for a subquery's source, its row in a
binding of the subquery's core under `b`. -/
def Edge.HoldsIn (s : Statement) (db : Db) (f : Nat) (b : Bind) (e : Edge) : Prop :=
  ∃ x, cellOf (b e.member) e.column = some x ∧
    if e.nested then
      ∃ f', f = f' + 1 ∧ ∃ b' ∈ coreBindings s db (evalQuery s db f') b (s.item e.partner).core,
        ∃ r, b' e.partner = some r ∧ e.value.eval r = some x
    else ∃ r, b e.partner = some r ∧ e.value.eval r = some x

/-- What a reader promises of the narrowing it reads from a condition of
core `k`: when the condition holds of a binding, with its subqueries read at
fuel `f`, each term holds of its item's row and each edge holds. Each term
and edge is on an item of core `k`, and an edge's partner is readable alone
and, outside a subquery, isn't its member. -/
structure ReadCondition.Sound (read : ReadCondition) : Prop where
  terms : ∀ {s k cd n}, read s k cd = some n → ∀ i t, (i, t) ∈ n.terms →
    (s.item i).core = k ∧
    ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → t.holds (b i)
  edges : ∀ {s k cd n}, read s k cd = some n → ∀ e ∈ n.edges,
    (s.item e.member).core = k ∧ (e.nested = false → e.partner ≠ e.member) ∧
    (s.item e.partner).readable ∧
    ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → e.HoldsIn s db f b

end SqliveNarrowing
