import SqliveNarrowing.Rules.SubqueryEdge
import SqliveNarrowing.Rules.Edge

/-! A column equal to a value that is one of several partners' values
narrows by each of them (`scope_one_of.rs`): `m.c = COALESCE(a, b, ...)`
narrows `m` to the rows whose column some argument gives, and `m.c IN
(SELECT ... UNION SELECT ...)`, or `=` with such a subquery, to the rows
whose column some arm returns. COALESCE returns one of its arguments, and a
compound returns a row one of its arms returns, so a kept row's column holds
the value of one of them, and the OR of their narrowings holds of it.

An argument narrows as a value of one other item, which a lookup compares
as an expression with the member column's affinity, as SQLite applies it to
COALESCE's value; as a subquery's returned column, which gives the edge a
column compared with the subquery gives; or as a constant, which gives the
term the member column equal to it. An arm narrows as a bare column of one
of its own items, of the member column's affinity, or as a constant. Any
other argument or arm makes the comparison narrow nothing. -/
namespace SqliveNarrowing.OneOf

/-- COALESCE's arguments, its nested calls flattened. -/
def args : Expr → List Expr
  | .coalesce l r => args l ++ args r
  | e => [e]

/-- The edge from `m.c` to the one other item an expression with no subquery
names, compared as an expression: a strict one, or one over an item that
every binding holds a row of, as `Edge.edge` reads an equality's. -/
def valueEdge (s : Statement) (m c : Nat) (affinity : Affinity) (arg : Expr) : Option Edge := do
  if !arg.queries.isEmpty then none
  let p ← arg.items.head?
  if !arg.items.all (· = p) ∨ p = m ∨ !(s.item p).readable ∨
    !(arg.strict || s.alwaysBound p) then none
  pure { member := m, memberValue := .column c, affinity, partner := p, value := .expr arg, nested := false }

/-- The side one argument of COALESCE gives `m.c`, written in core `k`. -/
def argSide (s : Statement) (k m c : Nat) (affinity : Affinity) : Expr → Option Side
  | .lit (some v) => some { terms := [.compare c .eq v] }
  | .sub q => (SubqueryEdge.edge s k (.col m c) q).map fun e => { edges := [e] }
  | arg => (valueEdge s m c affinity arg).map fun e => { edges := [e] }

/-- The side one arm of a compound gives `m.c`. -/
def armSide (s : Statement) (m c : Nat) (affinity : Affinity) (arm : Nat) : Option Side :=
  match (s.core arm).output with
  | [.expr (.lit (some v))] => some { terms := [.compare c .eq v] }
  | [.expr (.col p pc)] =>
    if (s.item p).core = arm ∧ (s.item p).readable ∧ s.affinity p pc = some affinity then
      some { edges := [{ member := m, memberValue := .column c, affinity, partner := p, value := .column pc,
                         nested := true }] }
    else none
  | _ => none

/-- The member column `member` names, of core `k`, with its affinity. -/
def memberOf (s : Statement) (k : Nat) : Expr → Option (Nat × Nat × Affinity)
  | .col m c => if (s.item m).core = k then (s.affinity m c).map (m, c, ·) else none
  | _ => none

/-- `member = COALESCE(...)`. -/
def coalesced (s : Statement) (k : Nat) (member other : Expr) : Option Narrowing := do
  let (m, c, affinity) ← memberOf s k member
  let .coalesce .. := other | none
  let sides ← (args other).mapM (argSide s k m c affinity)
  pure { ors := [⟨m, sides⟩] }

/-- `member` compared with a compound subquery `q`. -/
def compound (s : Statement) (k : Nat) (member : Expr) (q : Nat) : Option Narrowing := do
  let (m, c, affinity) ← memberOf s k member
  let arms := (s.query q).arms
  if arms.length < 2 then none
  let sides ← arms.mapM (armSide s m c affinity)
  pure { ors := [⟨m, sides⟩] }

/-- The reader. -/
def readCondition : ReadCondition := fun s k cd =>
  match cd with
  | .cmp .eq l r =>
    match l, r with
    | .sub q, m | m, .sub q => compound s k m q
    | _, _ => (coalesced s k l r).orElse fun _ => coalesced s k r l
  | .inQuery m q false => compound s k m q
  | _ => none

/-! The proof. -/

/-- Each element `mapM` maps, and each it gives. -/
theorem mapM_mem {α β : Type} {g : α → Option β} :
    ∀ {l : List α} {l' : List β}, l.mapM g = some l' →
      (∀ a ∈ l, ∃ b ∈ l', g a = some b) ∧ (∀ b ∈ l', ∃ a ∈ l, g a = some b)
  | [], l', h => by simp at h; subst h; simp
  | a :: as, l', h => by
    simp only [List.mapM_cons, Option.bind_eq_bind, Option.bind_eq_some_iff, Option.pure_def,
      Option.some.injEq] at h
    obtain ⟨b, hb, bs, hbs, rfl⟩ := h
    obtain ⟨h1, h2⟩ := mapM_mem hbs
    refine ⟨fun a' ha' => ?_, fun b' hb' => ?_⟩
    · simp only [List.mem_cons] at ha'
      rcases ha' with rfl | ha'
      · exact ⟨b, by simp, hb⟩
      · obtain ⟨b', hb', h⟩ := h1 a' ha'
        exact ⟨b', by simp [hb'], h⟩
    · simp only [List.mem_cons] at hb'
      rcases hb' with rfl | hb'
      · exact ⟨a, by simp, hb⟩
      · obtain ⟨a', ha', h⟩ := h2 b' hb'
        exact ⟨a', by simp [ha'], h⟩

/-- COALESCE's value is one of its arguments' values. -/
theorem args_eval {e : Expr} {b : Bind} {sub : Subs} {x : Int} (h : e.eval b sub = some x) :
    ∃ a ∈ args e, a.eval b sub = some x := by
  induction e with
  | coalesce l r ihl ihr =>
    simp only [Expr.eval] at h
    cases hl : l.eval b sub with
    | some y =>
      rw [hl] at h
      cases h
      obtain ⟨a, ha, he⟩ := ihl hl
      exact ⟨a, by simp [args, ha], he⟩
    | none =>
      rw [hl] at h
      obtain ⟨a, ha, he⟩ := ihr h
      exact ⟨a, by simp [args, ha], he⟩
  | col i c => exact ⟨_, by simp [args], h⟩
  | lit v => exact ⟨_, by simp [args], h⟩
  | add l r _ _ => exact ⟨_, by simp [args], h⟩
  | greatest l r _ _ => exact ⟨_, by simp [args], h⟩
  | sub q => exact ⟨_, by simp [args], h⟩
  | iif t y n _ _ _ => exact ⟨_, by simp [args], h⟩
  | isNull e _ => exact ⟨_, by simp [args], h⟩

/-- A row of compound `q` whose first column holds `x`: a row an arm gives,
in a binding the arm's core keeps. -/
theorem arm_row {s : Statement} {db : Db} {f : Nat} {b : Bind} {q : Nat} {r : Row}
    (hr : r ∈ evalQuery s db (f + 1) b q) :
    ∃ k ∈ (s.query q).arms, r ∈ (s.core k).rows (evalQuery s db f)
      (coreBindings s db (evalQuery s db f) b k) := by
  simp only [evalQuery, queryRows] at hr
  obtain ⟨rows, hrows, hr⟩ := mem_combineAll (mem_post hr)
  simp only [List.mem_map] at hrows
  obtain ⟨k, hk, rfl⟩ := hrows
  exact ⟨k, hk, mem_result hr⟩

/-- What a value edge says of its partner. -/
theorem valueEdge_facts {s : Statement} {m c : Nat} {aff : Affinity} {arg : Expr} {e : Edge}
    (h : valueEdge s m c aff arg = some e) :
    e.member = m ∧ e.memberValue = .column c ∧ e.nested = false ∧ e.nulls = false ∧ e.partner ≠ m ∧
    (s.item e.partner).readable ∧ arg.queries = [] ∧
    (arg.strict = true ∨ s.alwaysBound e.partner = true) ∧
    (∀ j ∈ arg.items, j = e.partner) ∧ arg.refs ≠ [] ∧ e.value = .expr arg := by
  unfold valueEdge at h
  simp only [bind, Option.bind] at h
  split at h
  · cases h
  rename_i hq
  cases hp : arg.items.head? with
  | none => simp [hp] at h
  | some p =>
    simp only [hp] at h
    split at h
    · cases h
    rename_i hbad
    simp only [not_or, Bool.not_eq_true] at hbad
    cases h
    have hst : arg.strict = true ∨ s.alwaysBound p = true := by
      cases hs : arg.strict
      · exact Or.inr (by simpa [hs] using hbad.2.2.2)
      · exact Or.inl rfl
    refine ⟨rfl, rfl, rfl, rfl, hbad.2.1, by simpa using hbad.2.2.1, by simpa using hq,
      hst, by simpa using hbad.1, ?_, rfl⟩
    intro hnil
    simp [Expr.items, hnil] at hp

/-- A value edge holds where its member equals the argument's value. -/
theorem valueEdge_holds {s : Statement} {m c : Nat} {aff : Affinity} {arg : Expr} {e : Edge}
    (h : valueEdge s m c aff arg = some e) {db : Db} {f : Nat} {b : Bind} {sub : Subs} {x : Int}
    (hm : cellOf (b m) c = some x) (ha : arg.eval b sub = some x) : e.HoldsIn s db f b := by
  obtain ⟨hmem, hcol, hn, _, _, _, hq, hst, hitems, hrefs, hval⟩ := valueEdge_facts h
  rw [arg.eval_rowBind hitems hq] at ha
  cases hb : b e.partner with
  | none =>
    cases hs : arg.strict with
    | true =>
      rw [hb, arg.eval_null hrefs hq hs (fun _ _ _ => by simp [rowBind, cellOf])] at ha
      cases ha
    | false =>
      have hab : s.alwaysBound e.partner = true := by simpa [hs] using hst
      exact Or.inr (Or.inr ⟨by simp [hval, PValue.strict, hs], hn, hab, hb⟩)
  | some r =>
    refine Or.inl ⟨x, by rw [hmem, hcol]; exact hm, ?_⟩
    simp only [hn, Bool.false_eq_true, ↓reduceIte]
    exact ⟨r, hb, by rw [hval, PValue.eval, ← hb, ha]⟩

/-- A subquery edge holds where the member equals the subquery's value. -/
theorem subEdge_holds {s : Statement} {k m c q : Nat} {e : Edge}
    (h : SubqueryEdge.edge s k (.col m c) q = some e) {db : Db} {f : Nat} {b : Bind} {x : Int}
    (hm : cellOf (b m) c = some x)
    (hs : (Expr.sub q).eval b (evalQuery s db f b) = some x) : e.HoldsIn s db f b := by
  obtain ⟨sub', pc, mc, harms, hout, hval, hpcore, _, hmem, hmv, _, hnest⟩ :=
    SubqueryEdge.edge_facts h
  simp only [Expr.col.injEq] at hmem
  obtain ⟨rfl, rfl⟩ := hmem
  refine Or.inl ⟨x, by simpa [hmv, PValue.on] using hm, ?_⟩
  simp only [hnest, ↓reduceIte]
  simp only [Expr.eval] at hs
  cases hhead : (evalQuery s db f b q).head? with
  | none => simp [hhead] at hs
  | some row =>
    rw [hhead] at hs
    have hrow := List.mem_of_head? hhead
    cases f with
    | zero => simp [evalQuery] at hrow
    | succ f' =>
      refine ⟨f', rfl, ?_⟩
      rcases mem_query_single harms hout hrow with hnone | ⟨b', hb', hrow'⟩
      · subst hnone
        simp at hs
      · subst hrow'
        simp only [Expr.eval, Option.bind_some, List.head?_cons, Option.join_some] at hs
        cases hp : b' e.partner with
        | none => simp [hp, cellOf] at hs
        | some r =>
          rw [hpcore]
          refine ⟨b', hb', r, hp, ?_⟩
          simp only [hp, cellOf] at hs
          simp [hval, PValue.eval, hs]

/-- What a side's edges say statically: each is from `m`, outside a
subquery its partner isn't `m` and is among the items `named`, and inside
one it sits in an arm of one of the subqueries `queries`. -/
def SideFacts (s : Statement) (m : Nat) (named queries : List Nat) (side : Side) : Prop :=
  ∀ e ∈ side.edges, e.member = m ∧ (e.nested = false → e.partner ≠ e.member) ∧
    (e.nested = false → e.partner ∈ named) ∧ (s.item e.partner).readable ∧ e.memberStrict s ∧
    e.NestedIn s queries

/-- The items an argument of COALESCE names are items the COALESCE names. -/
theorem args_items {e arg : Expr} (h : arg ∈ args e) : ∀ j ∈ arg.items, j ∈ e.items := by
  induction e with
  | coalesce l r ihl ihr =>
    intro j hj
    simp only [args, List.mem_append] at h
    simp only [Expr.items, Expr.refs, List.map_append, List.mem_append]
    rcases h with h | h
    · exact Or.inl (ihl h j hj)
    · exact Or.inr (ihr h j hj)
  | _ =>
    simp only [args, List.mem_singleton] at h
    subst h
    exact fun j hj => hj

/-- The subqueries an argument of COALESCE holds are subqueries the
COALESCE holds. -/
theorem args_queries {e arg : Expr} (h : arg ∈ args e) : ∀ q ∈ arg.queries, q ∈ e.queries := by
  induction e with
  | coalesce l r ihl ihr =>
    intro q hq
    simp only [args, List.mem_append] at h
    simp only [Expr.queries, List.mem_append]
    rcases h with h | h
    · exact Or.inl (ihl h q hq)
    · exact Or.inr (ihr h q hq)
  | _ =>
    simp only [args, List.mem_singleton] at h
    subst h
    exact fun q hq => hq

/-- Each side an argument gives narrows the member. -/
theorem argSide_narrows {s : Statement} {k m c : Nat} {aff : Affinity} {arg : Expr} {side : Side}
    (h : argSide s k m c aff arg = some side) : side.narrows = true := by
  unfold argSide at h
  split at h
  · cases h; rfl
  · obtain ⟨e', _, rfl⟩ := Option.map_eq_some_iff.mp h; rfl
  · obtain ⟨e', _, rfl⟩ := Option.map_eq_some_iff.mp h; rfl

/-- Each side an arm gives narrows the member. -/
theorem armSide_narrows {s : Statement} {m c : Nat} {aff : Affinity} {arm : Nat} {side : Side}
    (h : armSide s m c aff arm = some side) : side.narrows = true := by
  unfold armSide at h
  split at h
  · cases h; rfl
  · split at h
    · cases h; rfl
    · cases h
  · cases h

theorem argSide_facts {s : Statement} {k m c : Nat} {aff : Affinity} {arg : Expr} {side : Side}
    (h : argSide s k m c aff arg = some side) : SideFacts s m arg.items arg.queries side := by
  intro e he
  unfold argSide at h
  split at h
  · cases h; simp at he
  · rename_i q
    obtain ⟨e', he', rfl⟩ := Option.map_eq_some_iff.mp h
    simp only [List.mem_singleton] at he
    subst he
    obtain ⟨_, _, _, harms, _, _, hpcore, hread, hmem, hmv, _, hnest⟩ := SubqueryEdge.edge_facts he'
    simp only [Expr.col.injEq] at hmem
    exact ⟨hmem.1.symm, fun h => by simp [hnest] at h, fun h => by simp [hnest] at h, hread,
      Or.inl (by rw [hmv]; rfl), fun _ => ⟨q, by simp [Expr.queries], by rw [hpcore, harms]; simp⟩⟩
  · obtain ⟨e', he', rfl⟩ := Option.map_eq_some_iff.mp h
    simp only [List.mem_singleton] at he
    subst he
    obtain ⟨hmem, hmv, hnest, _, hne, hread, _, _, hitems, hrefs, _⟩ := valueEdge_facts he'
    have hpart : e.partner ∈ arg.items := by
      cases hr : arg.refs with
      | nil => exact absurd hr hrefs
      | cons p ps =>
        have hp : p.1 ∈ arg.items := by simp [Expr.items, hr]
        rw [← hitems p.1 hp]
        exact hp
    exact ⟨hmem, fun _ => by rw [hmem]; exact hne, fun _ => hpart, hread, Or.inl (by rw [hmv]; rfl),
      fun h => by simp [hnest] at h⟩

/-- A side of an argument that gives the member's value holds. -/
theorem argSide_holds {s : Statement} {k m c : Nat} {aff : Affinity} {arg : Expr} {side : Side}
    (h : argSide s k m c aff arg = some side) {db : Db} {f : Nat} {b : Bind} {x : Int}
    (hm : cellOf (b m) c = some x) (ha : arg.eval b (evalQuery s db f b) = some x) :
    (∀ t ∈ side.terms, t.holds (b m)) ∧ ∀ e ∈ side.edges, e.HoldsIn s db f b := by
  unfold argSide at h
  split at h
  · rename_i v
    cases h
    simp only [Expr.eval, Option.some.injEq] at ha
    subst ha
    refine ⟨fun t ht => ?_, fun e he => by simp at he⟩
    simp only [List.mem_singleton] at ht
    subst ht
    simp [STerm.holds, hm, Op.compare]
  · rename_i q
    obtain ⟨e', he', rfl⟩ := Option.map_eq_some_iff.mp h
    refine ⟨fun t ht => by simp at ht, fun e he => ?_⟩
    simp only [List.mem_singleton] at he
    subst he
    exact subEdge_holds he' hm ha
  · obtain ⟨e', he', rfl⟩ := Option.map_eq_some_iff.mp h
    refine ⟨fun t ht => by simp at ht, fun e he => ?_⟩
    simp only [List.mem_singleton] at he
    subst he
    exact valueEdge_holds he' hm ha

theorem armSide_facts {s : Statement} {m c : Nat} {aff : Affinity} {arm : Nat} {side : Side}
    (named : List Nat) {queries : List Nat} (harm : ∃ q ∈ queries, arm ∈ (s.query q).arms)
    (h : armSide s m c aff arm = some side) : SideFacts s m named queries side := by
  intro e he
  unfold armSide at h
  split at h
  · cases h; simp at he
  · split at h
    · rename_i hp
      cases h
      simp only [List.mem_singleton] at he
      subst he
      exact ⟨rfl, fun h => by simp at h, fun h => by simp at h, hp.2.1, Or.inl rfl,
        fun _ => by rw [hp.1]; exact harm⟩
    · cases h
  · cases h

/-- A side of an arm that returned the member's value holds. -/
theorem armSide_holds {s : Statement} {m c : Nat} {aff : Affinity} {arm : Nat} {side : Side}
    (h : armSide s m c aff arm = some side) {db : Db} {f : Nat} {b : Bind} {x : Int}
    (hm : cellOf (b m) c = some x) {r : Row}
    (hr : r ∈ (s.core arm).rows (evalQuery s db f) (coreBindings s db (evalQuery s db f) b arm))
    (hx : r.cell 0 = some x) :
    (∀ t ∈ side.terms, t.holds (b m)) ∧ ∀ e ∈ side.edges, e.HoldsIn s db (f + 1) b := by
  unfold armSide at h
  split at h
  · rename_i v hout
    cases h
    rcases mem_rows_single hout hr with rfl | ⟨b', _, rfl⟩
    · simp [Row.cell] at hx
    · simp only [Expr.eval, Row.cell, List.getElem?_cons_zero, Option.join_some,
        Option.some.injEq] at hx
      subst hx
      refine ⟨fun t ht => ?_, fun e he => by simp at he⟩
      simp only [List.mem_singleton] at ht
      subst ht
      simp [STerm.holds, hm, Op.compare]
  · rename_i p pc hout
    split at h
    · rename_i hp
      cases h
      refine ⟨fun t ht => by simp at ht, fun e he => ?_⟩
      simp only [List.mem_singleton] at he
      subst he
      rcases mem_rows_single hout hr with rfl | ⟨b', hb', rfl⟩
      · simp [Row.cell] at hx
      · simp only [Expr.eval, Row.cell, List.getElem?_cons_zero, Option.join_some] at hx
        refine Or.inl ⟨x, hm, ?_⟩
        simp only [↓reduceIte]
        refine ⟨f, rfl, ?_⟩
        cases hb : b' p with
        | none => simp [hb, cellOf] at hx
        | some r' =>
          simp only [hp.1]
          refine ⟨b', hb', r', hb, ?_⟩
          simp only [hb, cellOf] at hx
          simp [PValue.eval, hx]
    · cases h
  · cases h

theorem memberOf_facts {s : Statement} {k : Nat} {member : Expr} {m c : Nat} {aff : Affinity}
    (h : memberOf s k member = some (m, c, aff)) :
    member = .col m c ∧ (s.item m).core = k := by
  unfold memberOf at h
  split at h
  · rename_i m' c'
    split at h
    · rename_i hk
      obtain ⟨_, _, h⟩ := Option.map_eq_some_iff.mp h
      cases h
      exact ⟨rfl, hk⟩
    · cases h
  · cases h

/-- The comparison `m.c = (SELECT ...)` or `m.c IN (SELECT ...)`, true of a
binding, gives a row of the subquery whose first column holds the member's
value. -/
theorem compared_row {s : Statement} {cd : Cond} {member : Expr} {q m c : Nat}
    (hmem : member = .col m c) {db : Db} {f : Nat} {b : Bind}
    (hcd : cd = .cmp .eq (.sub q) member ∨ cd = .cmp .eq member (.sub q) ∨
      cd = .inQuery member q false)
    (hc : isTrue (cd.eval b (evalQuery s db f b))) :
    ∃ x row, cellOf (b m) c = some x ∧ row ∈ evalQuery s db f b q ∧ row.cell 0 = some x := by
  subst hmem
  rcases hcd with rfl | rfl | rfl
  all_goals first
    | (obtain ⟨x, row, hm, hrow, hcell⟩ := SubqueryEdge.compared_sound (member := .col m c) (q := q)
        (by simp [SubqueryEdge.compared]) hc
       exact ⟨x, row, by simpa [Expr.eval] using hm, hrow, hcell⟩)

/-- The theorem: a column equal to COALESCE of values, or compared with a
compound's rows, true of a binding, makes one side of the disjunction hold:
the side of the argument COALESCE returned, or of the arm that returned the
row. -/
theorem one_of_sound : ReadCondition.Disjoins readCondition := by
  intro s k cd n hn
  -- Each case gives one disjunction for the member column.
  have compound_sound : ∀ {member : Expr} {q : Nat}, compound s k member q = some n →
      q ∈ cd.queries →
      (∀ {m c}, member = .col m c → ∀ {db f b}, isTrue (cd.eval b (evalQuery s db f b)) →
        ∃ x row, cellOf (b m) c = some x ∧ row ∈ evalQuery s db f b q ∧ row.cell 0 = some x) →
      n.terms = [] ∧ n.edges = [] ∧ ∀ d ∈ n.ors, (s.item d.item).core = k ∧
        (∀ side ∈ d.sides, side.narrows = true) ∧
        (∀ side ∈ d.sides, ∀ e ∈ side.edges, e.member = d.item ∧
          (e.nested = false → e.partner ≠ e.member) ∧ (e.nested = false → e.partner ∈ cd.items) ∧
          (s.item e.partner).readable ∧ e.memberStrict s ∧ e.NestedIn s cd.queries) ∧
        ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → d.HoldsIn s db f b := by
    intro member q h hq hrow
    unfold compound at h
    simp only [bind, Option.bind] at h
    cases hmo : memberOf s k member with
    | none => simp [hmo] at h
    | some mca =>
      obtain ⟨m, c, aff⟩ := mca
      obtain ⟨hmember, hk⟩ := memberOf_facts hmo
      simp only [hmo] at h
      split at h
      · cases h
      cases hsides : (s.query q).arms.mapM (armSide s m c aff) with
      | none => simp [hsides] at h
      | some sides =>
        simp only [hsides, Option.pure_def, Option.some.injEq] at h
        subst h
        refine ⟨rfl, rfl, fun d hd => ?_⟩
        simp only [List.mem_singleton] at hd
        subst hd
        refine ⟨hk, fun side hside => ?_, fun side hside => ?_, fun db f b hc => ?_⟩
        · obtain ⟨arm, _, harm⟩ := (mapM_mem hsides).2 side hside
          exact armSide_narrows harm
        · obtain ⟨arm, _, harm⟩ := (mapM_mem hsides).2 side hside
          exact armSide_facts cd.items ⟨q, hq, by assumption⟩ harm
        · obtain ⟨x, row, hm, hrowq, hx⟩ := hrow hmember hc
          cases f with
          | zero => simp [evalQuery] at hrowq
          | succ f' =>
            obtain ⟨arm, harm, hrows⟩ := arm_row hrowq
            obtain ⟨side, hside, hsome⟩ := (mapM_mem hsides).1 arm harm
            obtain ⟨hterms, hedges⟩ := armSide_holds hsome hm hrows hx
            exact ⟨side, hside, hterms, hedges⟩
  have coalesced_sound : ∀ {member other : Expr}, coalesced s k member other = some n →
      (∀ j ∈ other.items, j ∈ cd.items) → (∀ q ∈ other.queries, q ∈ cd.queries) →
      (∀ {db f b}, isTrue (cd.eval b (evalQuery s db f b)) →
        ∃ x, member.eval b (evalQuery s db f b) = some x ∧
          other.eval b (evalQuery s db f b) = some x) →
      n.terms = [] ∧ n.edges = [] ∧ ∀ d ∈ n.ors, (s.item d.item).core = k ∧
        (∀ side ∈ d.sides, side.narrows = true) ∧
        (∀ side ∈ d.sides, ∀ e ∈ side.edges, e.member = d.item ∧
          (e.nested = false → e.partner ≠ e.member) ∧ (e.nested = false → e.partner ∈ cd.items) ∧
          (s.item e.partner).readable ∧ e.memberStrict s ∧ e.NestedIn s cd.queries) ∧
        ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → d.HoldsIn s db f b := by
    intro member other h hnamed hqueries hval
    unfold coalesced at h
    simp only [bind, Option.bind] at h
    cases hmo : memberOf s k member with
    | none => simp [hmo] at h
    | some mca =>
      obtain ⟨m, c, aff⟩ := mca
      obtain ⟨hmember, hk⟩ := memberOf_facts hmo
      simp only [hmo] at h
      split at h
      · rename_i l r
        cases hsides : (args (.coalesce l r)).mapM (argSide s k m c aff) with
        | none => simp [hsides] at h
        | some sides =>
          simp only [hsides, Option.pure_def, Option.some.injEq] at h
          subst h
          refine ⟨rfl, rfl, fun d hd => ?_⟩
          simp only [List.mem_singleton] at hd
          subst hd
          refine ⟨hk, fun side hside => ?_, fun side hside => ?_, fun db f b hc => ?_⟩
          · obtain ⟨arg, _, harg⟩ := (mapM_mem hsides).2 side hside
            exact argSide_narrows harg
          · obtain ⟨arg, hargm, harg⟩ := (mapM_mem hsides).2 side hside
            intro e he
            obtain ⟨h1, h2, h3, h4, h5, h6⟩ := argSide_facts harg e he
            refine ⟨h1, h2, fun hn => hnamed _ (args_items hargm _ (h3 hn)), h4, h5, fun hn => ?_⟩
            obtain ⟨q, hq, harm⟩ := h6 hn
            exact ⟨q, hqueries _ (args_queries hargm _ hq), harm⟩
          · obtain ⟨x, hm, ho⟩ := hval hc
            rw [hmember] at hm
            simp only [Expr.eval] at hm
            obtain ⟨arg, harg, ha⟩ := args_eval ho
            obtain ⟨side, hside, hsome⟩ := (mapM_mem hsides).1 arg harg
            obtain ⟨hterms, hedges⟩ := argSide_holds hsome hm ha
            exact ⟨side, hside, hterms, hedges⟩
      · cases h
  have heq : ∀ {l r : Expr} {db f b}, cd = .cmp .eq l r →
      isTrue (cd.eval b (evalQuery s db f b)) →
      ∃ x, l.eval b (evalQuery s db f b) = some x ∧ r.eval b (evalQuery s db f b) = some x := by
    intro l r db f b hcd hc
    subst hcd
    simp only [Cond.eval, isTrue, beq_iff_eq] at hc
    cases hl : l.eval b (evalQuery s db f b) <;> cases hr : r.eval b (evalQuery s db f b) <;>
      simp_all [Op.apply, Op.compare]
  have viaRow : ∀ {member : Expr} {q : Nat},
      (cd = .cmp .eq (.sub q) member ∨ cd = .cmp .eq member (.sub q) ∨
        cd = .inQuery member q false) →
      ∀ {m c}, member = .col m c → ∀ {db f b}, isTrue (cd.eval b (evalQuery s db f b)) →
        ∃ x row, cellOf (b m) c = some x ∧ row ∈ evalQuery s db f b q ∧ row.cell 0 = some x :=
    fun {_ _} hcd {_ _} hmem {_ _ _} hc => compared_row hmem hcd hc
  unfold readCondition at hn
  split at hn
  · rename_i l r
    split at hn
    · rename_i q m
      exact compound_sound hn (by simp [Cond.queries, Expr.queries]) (viaRow (Or.inl rfl))
    · rename_i m q _
      exact compound_sound hn (by simp [Cond.queries, Expr.queries]) (viaRow (Or.inr (Or.inl rfl)))
    · cases hco : coalesced s k l r with
      | some n' =>
        simp only [hco, Option.orElse_some, Option.some.injEq] at hn
        subst hn
        exact coalesced_sound hco (Edge.items_right) (fun q hq => by simp [Cond.queries, hq])
          (fun hc => heq rfl hc)
      | none =>
        simp only [hco, Option.orElse_none] at hn
        exact coalesced_sound hn (Edge.items_left) (fun q hq => by simp [Cond.queries, hq]) (fun hc => by
          obtain ⟨x, hl, hr⟩ := heq rfl hc
          exact ⟨x, hr, hl⟩)
  · rename_i m q
    exact compound_sound hn (by simp [Cond.queries]) (viaRow (Or.inr (Or.inr rfl)))
  · cases hn

end SqliveNarrowing.OneOf
