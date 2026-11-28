import SqliveNarrowing.Rules.PushDown

/-! Rule 8: an equality between two items of a core carries what one side's
own terms say of its compared column to the other side's column
(`Planner::carry_terms`). In a binding the core keeps, `a.x = b.y` holds, so
`a.x` holds the value `b.y` does, and a term true of `b`'s row on `y` is
true of `a`'s row on `x`: `JOIN b ON a.x = b.y WHERE b.y = 5` narrows `a` as
`a.x = 5` would, whichever item the planner places first.

A derived item's own terms on a column include what its arms' items' terms
say of the column each arm passes on (`pulled`): every row of the derived
item's query is a row of one of its arms, which holds the arm's item's value
in that column. A LIMIT keeps only rows the arms give, so it changes nothing
here; an arm that aggregates or calls a window function passes on no row's
own value, so such an arm gives nothing. -/
namespace SqliveNarrowing.Transfer

/-- Terms joined with OR: none when there are none, or when any is none. -/
def orAll : List (Option STerm) → Option STerm
  | [] => none
  | [t] => t
  | t :: ts => do
    let a ← t
    let b ← orAll ts
    pure (.or a b)

/-- What arm `k`'s item says of output column `y`: the term of the item the
column passes on unchanged, renamed to `y`. -/
def armTerm (s : Statement) (st : PState) (k y : Nat) : Option STerm :=
  match (s.core k).output[y]? with
  | some (.expr (.col j c)) =>
    if (s.item j).core = k then
      (st.items j).term.bind
        (·.renamed (fun c' => if c' = c then some y else none) fun _ _ => none)
    else none
  | _ => none

/-- What derived item `i`'s arms say of its column `y`, joined with OR;
none for a table, and when an arm aggregates or calls a window function or
says nothing of the column (`derived::pulled`). -/
def pulled (s : Statement) (st : PState) (i y : Nat) : Option STerm :=
  match (s.item i).kind with
  | .table _ => none
  | .derived q _ =>
    let arms := (s.query q).arms
    if arms.any fun k => (s.core k).aggregated || (s.core k).windowed then none
    else orAll (arms.map fun k => armTerm s st k y)

/-- The terms the edges between items of core `k` carry, each to its
member: the partner's own term on the compared column, then what a derived
partner's arms' own terms say of it, renamed to the member's column. An edge
that compares with an expression carries none, and an `IS` edge carries no
arm's term: a derived row whose column is NULL says nothing of its arm's
items. -/
def carried (s : Statement) (st : PState) (k : Nat) : List (Nat × STerm) :=
  st.edges.flatMap fun e =>
    if (s.item e.member).core = k ∧ (s.item e.partner).core = k ∧ e.nested = false ∧ e.via = none then
      match e.value, e.memberValue with
      | .column y, .column mc =>
        let rename (c : Nat) : Option Nat := if c = y then some mc else none
        let pulledTerm := if e.nulls then none else pulled s st e.partner y
        ((st.items e.partner).term.bind (·.renamed rename fun _ _ => none)).toList.map
            (e.member, ·) ++
          (pulledTerm.bind (·.renamed rename fun _ _ => none)).toList.map (e.member, ·)
      | _, _ => []
    else []

/-- Add `t` to what the edges carry to item `i`, with AND. -/
def addCarried (st : PState) (i : Nat) (t : STerm) : PState :=
  st.update i fun p =>
    { p with carried := some (match p.carried with | some e => .and e t | none => t) }

/-- Add the terms the edges of core `k` carry, each to what its member's
edges carry, with AND. A carried term narrows the member's scope, and,
through a derived item, the items its arms pass their columns on from
(`pushCarried`): it carries nothing further and decides nothing of
the order, so an item a carried term alone narrows still takes values from
the items its edges join it to. A LEFT JOIN item stays one: a carried term
holds of each of its rows that joins, and says nothing of the row of NULLs. -/
def carry (s : Statement) (st : PState) (k : Nat) : PState :=
  (carried s st k).foldl (fun st (i, t) => addCarried st i t) st

/-- Give `target` what `term`, carried to a derived item whose arm passes into
it, says of its row, as a term its own edges carry, when no LEFT JOIN adds
it, then pass it further with `next`. -/
def passCarried (s : Statement) (next : PState → Nat → Option STerm → PState) (target : Nat)
    (rename : Nat → Option Nat) (term : STerm) (st : PState) : PState :=
  match term.renamed rename (STerm.judgedOn target rename) with
  | some renamed =>
    if s.alwaysBound target && (st.items target).leftOn.isNone then
      next (addCarried st target renamed) target (some renamed)
    else st
  | none => st

/-- Pass `term`, which the edges carry to item `item`, into the items its arms
pass their columns on from, and through them further down
(`derived::push_carried`). -/
def pushCarried (s : Statement) : Nat → PState → Nat → Option STerm → PState
  | 0, st, _, _ => st
  | fuel + 1, st, item, term =>
    match PushDown.arms s item, term with
    | some found, some term => found.foldl (fun st arm =>
        (term.columns.filterMap fun c => (arm[c]?).join.map (·.1)).eraseDups.foldl
          (fun st target => passCarried s (pushCarried s fuel) target (PushDown.renameInto arm target) term st)
          st) st
    | _, _ => st

/-! The proof. -/

/-- A term over a missing row, whose columns `rename` passes on to another
row that is NULL in each of them, holds of that row as renamed. -/
theorem renamed_none {t t' : STerm} {rename : Nat → Option Nat}
    (h : t.renamed rename (fun _ _ => none) = some t')
    {row : Option Row} (hcols : ∀ c c', rename c = some c' → none = cellOf row c')
    (ht : t.holds none) : t'.holds row := by
  induction t generalizing t' with
  | compare c op v => simp [STerm.holds, cellOf] at ht
  | inList c vs => simp [STerm.holds, cellOf] at ht
  | null c n =>
    simp only [STerm.renamed, Option.map_eq_some_iff] at h
    obtain ⟨c', hc', rfl⟩ := h
    simp only [STerm.holds] at ht ⊢
    rw [← hcols c c' hc']
    exact ht
  | row i cd => simp [STerm.renamed] at h
  | and l r ihl ihr =>
    simp only [STerm.holds, Bool.and_eq_true] at ht
    simp only [STerm.renamed] at h
    split at h
    · rename_i lt rt hl hr
      cases h
      simp [STerm.holds, ihl hl ht.1, ihr hr ht.2]
    · rename_i t'' hl _
      cases h
      exact ihl hl ht.1
    · rename_i t'' _ hr
      cases h
      exact ihr hr ht.2
    · cases h
  | or l r ihl ihr =>
    simp only [STerm.renamed] at h
    split at h
    · rename_i lt rt hl hr
      cases h
      simp only [STerm.holds, Bool.or_eq_true] at ht ⊢
      rcases ht with ht | ht
      · exact Or.inl (ihl hl ht)
      · exact Or.inr (ihr hr ht)
    · cases h

/-- Rule 8's theorem: when an edge that compares with the partner's column
`y` holds of a binding, a term true of the partner's row is true, renamed
from `y` to the member's column, of the member's row. -/
theorem transfer_sound {s : Statement} {db : Db} {f : Nat} {b : Bind} {e : Edge} {y : Nat}
    {mc : Nat} {t t' : STerm} (hv : e.value = .column y) (hm : e.memberValue = .column mc)
    (hn : e.nested = false) (hvn : e.via = none)
    (hh : e.HoldsIn s db f b) (ht : t.holds (b e.partner))
    (hr : t.renamed (fun c => if c = y then some mc else none) (fun _ _ => none) = some t') :
    t'.holds (b e.member) := by
  rcases hh with ⟨x, hx, hp⟩ | ⟨_, _, _, _, hx, hnull⟩ | ⟨hvs, _⟩ | ⟨_, _, _, _, hnt, _⟩ | ⟨_, _, _, hv', _⟩
  · simp only [hm, PValue.on] at hx
    simp only [hn, Bool.false_eq_true, ite_false] at hp
    obtain ⟨r, hb, hval⟩ := hp
    rw [hb] at ht
    refine PushDown.renamed_sound hr (fun c c' hc => ?_) (fun _ _ _ h => by cases h) ht
    by_cases hcy : c = y
    · subst hcy
      simp only [ite_true, Option.some.injEq] at hc
      subst hc
      rw [hx]
      simpa [hv, PValue.eval] using hval
    · simp [hcy] at hc
  · simp only [hm, PValue.on] at hx
    -- An `IS` edge whose member is NULL: the partner's column is NULL too,
    -- in its row or for want of one.
    have hcell : cellOf (b e.partner) y = none := by
      cases hb : b e.partner with
      | none => rfl
      | some r => simpa [hv, PValue.eval, cellOf] using hnull r hb
    cases hb : b e.partner with
    | none =>
      rw [hb] at ht
      refine renamed_none hr (fun c c' hc => ?_) ht
      by_cases hcy : c = y
      · subst hcy
        simp only [ite_true, Option.some.injEq] at hc
        subst hc
        exact hx.symm
      · simp [hcy] at hc
    | some r =>
      rw [hb] at ht
      refine PushDown.renamed_sound hr (fun c c' hc => ?_) (fun _ _ _ h => by cases h) ht
      by_cases hcy : c = y
      · subst hcy
        simp only [ite_true, Option.some.injEq] at hc
        subst hc
        rw [hx]
        simpa [hb, cellOf] using hcell
      · simp [hcy] at hc
  · -- A column is a strict value.
    simp [hv, PValue.strict] at hvs
  · rw [hn] at hnt
    cases hnt
  · rw [hvn] at hv'
    cases hv'

theorem orAll_isSome {l : List (Option STerm)} {t : STerm} (h : orAll l = some t) :
    ∀ o ∈ l, o.isSome := by
  induction l generalizing t with
  | nil => simp [orAll] at h
  | cons o os ih =>
    cases os with
    | nil => simp_all [orAll]
    | cons o' os' =>
      simp only [orAll, Option.bind_eq_bind, Option.bind_eq_some_iff, Option.pure_def,
        Option.some.injEq] at h
      obtain ⟨a, ha, b, hb, _⟩ := h
      intro o'' ho''
      simp only [List.mem_cons] at ho''
      rcases ho'' with rfl | ho''
      · simp [ha]
      · exact ih hb o'' (by simpa using ho'')

/-- A term among those `orAll` joins, true of a row, makes their OR true of it. -/
theorem orAll_holds {l : List (Option STerm)} {t u : STerm} {row : Option Row}
    (h : orAll l = some t) (hu : some u ∈ l) (hh : u.holds row) : t.holds row := by
  induction l generalizing t with
  | nil => simp at hu
  | cons o os ih =>
    cases os with
    | nil =>
      simp only [orAll] at h
      simp only [List.mem_singleton] at hu
      rw [← hu] at h
      cases h
      exact hh
    | cons o' os' =>
      simp only [orAll, Option.bind_eq_bind, Option.bind_eq_some_iff, Option.pure_def,
        Option.some.injEq] at h
      obtain ⟨a, ha, b, hb, rfl⟩ := h
      simp only [List.mem_cons] at hu
      simp only [STerm.holds, Bool.or_eq_true]
      rcases hu with hu | hu
      · rw [← hu] at ha
        cases ha
        exact Or.inl hh
      · exact Or.inr (ih hb (by simpa using hu))

/-- Rule 8's theorem for a derived partner: when each arm's items' terms
hold of the rows the bindings the arm keeps hold of them, a row of the derived item's
query whose column `y` isn't NULL passes what its arms say of `y`. -/
theorem pulled_sound {s : Statement} {st : PState} {i y q : Nat} {view : Bool}
    (hk : (s.item i).kind = .derived q view) {t : STerm} (hp : pulled s st i y = some t)
    {db : Db} {f : Nat} {b : Bind} {r : Row} (hr : r ∈ evalQuery s db (f + 1) b q)
    (hterms : ∀ k ∈ (s.query q).arms, ∀ b' ∈ coreBindings s db (evalQuery s db f) b k,
      ∀ j u r0, (s.item j).core = k → (st.items j).term = some u → b' j = some r0 →
        u.holds (some r0))
    (hy : r.cell y ≠ none) : t.holds (some r) := by
  simp only [pulled, hk] at hp
  split at hp
  · cases hp
  rename_i hany
  obtain ⟨k, hk', hrow⟩ := PushDown.push_down_sound hr
  have hagg : (s.core k).aggregated = false := by
    simp only [List.any_eq_true, Bool.or_eq_true, not_exists, not_and, not_or,
      Bool.not_eq_true] at hany
    exact (hany k hk').1
  rcases hrow with hrow | ⟨b', hb', hcols⟩
  · rw [hagg] at hrow
    cases hrow
  have hsome := orAll_isSome hp (armTerm s st k y) (List.mem_map_of_mem hk')
  obtain ⟨u, hu⟩ := Option.isSome_iff_exists.mp hsome
  have hmem : some u ∈ (s.query q).arms.map fun k => armTerm s st k y := by
    rw [← hu]
    exact List.mem_map_of_mem hk'
  refine orAll_holds hp hmem ?_
  unfold armTerm at hu
  split at hu
  · rename_i j c hout
    split at hu
    · rename_i hj
      cases hterm : (st.items j).term with
      | none => simp [hterm] at hu
      | some v =>
        simp only [hterm, Option.bind_some] at hu
        have hcell := hcols y j c hout
        cases hbj : b' j with
        | none =>
          simp [hbj, cellOf] at hcell
          exact absurd hcell hy
        | some r0 =>
          have hv := hterms k hk' b' hb' j v r0 hj hterm hbj
          refine PushDown.renamed_sound hu (fun c₁ c₂ hc => ?_) (fun _ _ _ h => by cases h) hv
          by_cases hc₁ : c₁ = c
          · subst hc₁
            simp only [ite_true, Option.some.injEq] at hc
            subst hc
            rw [hbj] at hcell
            simp [cellOf, hcell]
          · simp [hc₁] at hc
    · cases hu
  · cases hu

end SqliveNarrowing.Transfer
