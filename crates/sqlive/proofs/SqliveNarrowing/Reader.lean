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
  | coalesce l r ihl ihr =>
    simp only [Expr.eval]
    rw [ihl (fun i c h => hb i c (by simp [Expr.refs, h]))
        (fun q h => hs q (by simp [Expr.queries, h])),
      ihr (fun i c h => hb i c (by simp [Expr.refs, h]))
        (fun q h => hs q (by simp [Expr.queries, h]))]
  | iif t y n iht ihy ihn =>
    simp only [Expr.eval]
    rw [iht (fun i c h => hb i c (by simp [Expr.refs, h]))
        (fun q h => hs q (by simp [Expr.queries, h])),
      ihy (fun i c h => hb i c (by simp [Expr.refs, h]))
        (fun q h => hs q (by simp [Expr.queries, h])),
      ihn (fun i c h => hb i c (by simp [Expr.refs, h]))
        (fun q h => hs q (by simp [Expr.queries, h]))]
  | isNull e ih =>
    simp only [Expr.eval]
    rw [ih (fun i c h => hb i c (by simp [Expr.refs, h]))
        (fun q h => hs q (by simp [Expr.queries, h]))]

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

/-- A strict expression with a column and no subquery is NULL where every
column it names is. -/
theorem Expr.eval_null (e : Expr) {b : Bind} {sub : Subs} (hr : e.refs ≠ [])
    (hq : e.queries = []) (hst : e.strict = true)
    (hn : ∀ i c, (i, c) ∈ e.refs → cellOf (b i) c = none) :
    e.eval b sub = none := by
  induction e with
  | col i c => exact hn i c (by simp [Expr.refs])
  | lit v => simp [Expr.refs] at hr
  | add l r ihl ihr | greatest l r ihl ihr =>
    simp only [Expr.queries, List.append_eq_nil_iff] at hq
    simp only [Expr.strict, Bool.and_eq_true] at hst
    simp only [Expr.eval]
    by_cases hl : l.refs = []
    · have hr' : r.refs ≠ [] := by simpa [Expr.refs, hl] using hr
      rw [ihr hr' hq.2 hst.2 (fun i c h => hn i c (by simp [Expr.refs, h]))]
      split <;> simp_all
    · rw [ihl hl hq.1 hst.1 (fun i c h => hn i c (by simp [Expr.refs, h]))]
  | sub q => simp [Expr.queries] at hq
  | coalesce l r | iif t y n | isNull e => simp [Expr.strict] at hst

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
binding of the subquery's core under `b`. Or the edge compares by `IS`, the
member's column is NULL, and the partner's row in `b`, if it has one, gives
NULL; the partner is no item a LEFT JOIN adds, so a binding a core keeps
has its row. Or the partner's value isn't strict, `b` holds no row of the
partner, and the partner is no item a LEFT JOIN adds, so `b` is no binding a
core keeps. -/
def Edge.HoldsIn (s : Statement) (db : Db) (f : Nat) (b : Bind) (e : Edge) : Prop :=
  (∃ x, e.memberValue.on (b e.member) = some x ∧
    if e.nested then
      ∃ f', f = f' + 1 ∧ ∃ b' ∈ coreBindings s db (evalQuery s db f') b (s.item e.partner).core,
        ∃ r, b' e.partner = some r ∧ e.value.eval r = some x
    else ∃ r, b e.partner = some r ∧ e.value.eval r = some x) ∨
  (e.nulls = true ∧ e.nested = false ∧ s.alwaysBound e.partner = true ∧
    e.memberValue.on (b e.member) = none ∧ ∀ r, b e.partner = some r → e.value.eval r = none) ∨
  (e.value.strict = false ∧ e.nested = false ∧ s.alwaysBound e.partner = true ∧
    b e.partner = none)

/-- An OR that narrows `d.item` holds of a binding: one of its sides does,
each of its terms true of the item's row and each of its edges holding. -/
def Disjunction.HoldsIn (s : Statement) (db : Db) (f : Nat) (b : Bind) (d : Disjunction) : Prop :=
  ∃ side ∈ d.sides, (∀ t ∈ side.terms, t.holds (b d.item)) ∧ ∀ e ∈ side.edges, e.HoldsIn s db f b

/-- Whether an edge's member value is NULL for a missing row, or its member
is no item a LEFT JOIN adds, so every binding a core keeps holds its row:
either way, an edge that compares by `=` holds of no binding of the core
whose member has no row, unless its partner has none either. -/
def Edge.memberStrict (s : Statement) (e : Edge) : Prop :=
  e.memberValue.on none = none ∨ s.alwaysBound e.member = true

/-- What a reader that reads a condition as ORs promises: it gives no term
and no edge, and of each OR it gives, the item is one of core `k`'s, each
side narrows it, each side's edges are from that item to a partner readable
alone that, outside a subquery, isn't the item and is an item the condition
names, with a member value NULL for a missing row unless every binding holds
the item's row, and wherever the condition holds, one side does. -/
def ReadCondition.Disjoins (read : ReadCondition) : Prop :=
  ∀ {s k cd n}, read s k cd = some n → n.terms = [] ∧ n.edges = [] ∧
    ∀ d ∈ n.ors, (s.item d.item).core = k ∧ (∀ side ∈ d.sides, side.narrows = true) ∧
      (∀ side ∈ d.sides, ∀ e ∈ side.edges, e.member = d.item ∧
        (e.nested = false → e.partner ≠ e.member) ∧ (e.nested = false → e.partner ∈ cd.items) ∧
        (s.item e.partner).readable ∧ e.memberStrict s) ∧
      ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → d.HoldsIn s db f b

/-- What a reader promises of the narrowing it reads from a condition of
core `k`: when the condition holds of a binding, with its subqueries read at
fuel `f`, each term holds of its item's row and each edge holds. Each term
and edge is on an item of core `k`, and an edge's partner is readable alone
and, outside a subquery, isn't its member and is an item the condition
names, so a binding that holds a row of each item the condition names holds
one of the partner. An edge's member value is NULL for a missing row, unless
every binding holds the member's row. -/
structure ReadCondition.Sound (read : ReadCondition) : Prop where
  terms : ∀ {s k cd n}, read s k cd = some n → ∀ i t, (i, t) ∈ n.terms →
    (s.item i).core = k ∧
    ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → t.holds (b i)
  edges : ∀ {s k cd n}, read s k cd = some n → ∀ e ∈ n.edges,
    (s.item e.member).core = k ∧ (e.nested = false → e.partner ≠ e.member) ∧
    (e.nested = false → e.partner ∈ cd.items) ∧ (s.item e.partner).readable ∧ e.memberStrict s ∧
    ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → e.HoldsIn s db f b

/-! What makes a LEFT JOIN an inner join's: a part of a narrowing that a row
of NULLs fails, true of a binding, says its item has a row there, unless an
edge's partner, which every binding a core keeps holds a row of, has none. -/

/-- A term a row of NULLs can't pass holds of no missing row. -/
theorem STerm.holds_none {t : STerm} (h : t.mayHoldOnNulls = false) : t.holds none = false := by
  induction t with
  | compare => rfl
  | inList => rfl
  | null c n =>
    simp only [STerm.mayHoldOnNulls, Bool.not_eq_false'] at h
    simp [STerm.holds, cellOf, h]
  | row => simp [STerm.mayHoldOnNulls] at h
  | and l r ihl ihr =>
    simp only [STerm.mayHoldOnNulls, Bool.and_eq_false_iff] at h
    rcases h with h | h <;> simp [STerm.holds, ihl, ihr, h]
  | or l r ihl ihr =>
    simp only [STerm.mayHoldOnNulls, Bool.or_eq_false_iff] at h
    simp [STerm.holds, ihl h.1, ihr h.2]

/-- An edge that compares by `=`, holding of a binding with no row of its
member, has a member every binding a core keeps holds a row of, or a partner
outside a subquery that every such binding holds a row of and this one
doesn't. -/
theorem Edge.HoldsIn.bound {s : Statement} {db : Db} {f : Nat} {b : Bind} {e : Edge}
    (hh : e.HoldsIn s db f b) (hn : e.nulls = false) (hs : e.memberStrict s)
    (hb : b e.member = none) :
    s.alwaysBound e.member = true ∨
      (e.nested = false ∧ s.alwaysBound e.partner = true ∧ b e.partner = none) := by
  rcases hh with ⟨x, hx, _⟩ | ⟨hnulls, _⟩ | ⟨_, hnest, hab, hp⟩
  · rcases hs with hs | hs
    · rw [hb, hs] at hx
      cases hx
    · exact Or.inl hs
  · rw [hn] at hnulls
    cases hnulls
  · exact Or.inr ⟨hnest, hab, hp⟩

/-- An OR a row of NULLs can't pass, each of whose sides narrows its item
with edges from it whose member value is NULL for a missing row unless every
binding holds the item's row, holding of a binding with no row of its item,
has an item every binding a core keeps holds a row of, or an edge whose
partner outside a subquery every such binding holds a row of and this one
doesn't. -/
theorem Disjunction.HoldsIn.bound {s : Statement} {db : Db} {f : Nat} {b : Bind}
    {d : Disjunction} (hh : d.HoldsIn s db f b) (hn : d.mayHoldOnNulls = false)
    (hnarrows : ∀ side ∈ d.sides, side.narrows = true)
    (hedges : ∀ side ∈ d.sides, ∀ e ∈ side.edges, e.member = d.item ∧ e.memberStrict s)
    (hb : b d.item = none) :
    s.alwaysBound d.item = true ∨ ∃ side ∈ d.sides, ∃ e ∈ side.edges,
      e.nested = false ∧ s.alwaysBound e.partner = true ∧ b e.partner = none := by
  obtain ⟨side, hside, hterms, hholds⟩ := hh
  have hsn : side.mayHoldOnNulls = false := by
    simp only [Disjunction.mayHoldOnNulls, List.any_eq_false] at hn
    simpa using hn side hside
  simp only [Side.mayHoldOnNulls, Bool.or_eq_false_iff, List.any_eq_false,
    Bool.not_eq_true] at hsn
  have hnar := hnarrows side hside
  simp only [Side.narrows, Bool.or_eq_true, Bool.not_eq_true', List.isEmpty_eq_false_iff] at hnar
  rcases hnar with hnar | hnar
  · obtain ⟨t, ht⟩ := List.exists_mem_of_ne_nil _ hnar
    have := hterms t ht
    rw [hb, STerm.holds_none (hsn.1 t ht)] at this
    cases this
  · obtain ⟨e, he⟩ := List.exists_mem_of_ne_nil _ hnar
    obtain ⟨hmem, hstrict⟩ := hedges side hside e he
    rcases (hholds e he).bound (hsn.2 e he) hstrict (by rw [hmem]; exact hb) with h | h
    · exact Or.inl (hmem ▸ h)
    · exact Or.inr ⟨side, hside, e, he, h⟩

end SqliveNarrowing
