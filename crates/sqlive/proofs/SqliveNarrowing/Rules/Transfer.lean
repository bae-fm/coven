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
      (st.items j).term.bind (·.renamed fun c' => if c' = c then some y else none)
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
that compares with an expression carries none. -/
def carried (s : Statement) (st : PState) (k : Nat) : List (Nat × STerm) :=
  st.edges.flatMap fun e =>
    if (s.item e.member).core = k ∧ (s.item e.partner).core = k ∧ e.nested = false then
      match e.value with
      | .column y =>
        let rename (c : Nat) : Option Nat := if c = y then some e.column else none
        ((st.items e.partner).term.bind (·.renamed rename)).toList.map (e.member, ·) ++
          ((pulled s st e.partner y).bind (·.renamed rename)).toList.map (e.member, ·)
      | .expr _ => []
    else []

/-- Add `t` to what the edges carry to item `i`, with AND. -/
def addCarried (st : PState) (i : Nat) (t : STerm) : PState :=
  st.update i fun p =>
    { p with carried := some (match p.carried with | some e => .and e t | none => t) }

/-- Add the terms the edges of core `k` carry, each to what its member's
edges carry, with AND. A carried term narrows the member's scope only: it
passes into no derived item, carries nothing further and decides nothing of
the order, so an item a carried term alone narrows still takes values from
the items its edges join it to. A LEFT JOIN item stays one: a carried term
holds of each of its rows that joins, and says nothing of the row of NULLs. -/
def carry (s : Statement) (st : PState) (k : Nat) : PState :=
  (carried s st k).foldl (fun st (i, t) => addCarried st i t) st

/-! The proof. -/

/-- Rule 8's theorem: when an edge that compares with the partner's column
`y` holds of a binding, a term true of the partner's row is true, renamed
from `y` to the member's column, of the member's row. -/
theorem transfer_sound {s : Statement} {db : Db} {f : Nat} {b : Bind} {e : Edge} {y : Nat}
    {t t' : STerm} (hv : e.value = .column y) (hn : e.nested = false)
    (hh : e.HoldsIn s db f b) (ht : t.holds (b e.partner))
    (hr : t.renamed (fun c => if c = y then some e.column else none) = some t') :
    t'.holds (b e.member) := by
  obtain ⟨x, hx, hp⟩ := hh
  simp only [hn, Bool.false_eq_true, ite_false] at hp
  obtain ⟨r, hb, hval⟩ := hp
  rw [hb] at ht
  refine PushDown.renamed_sound hr (fun c c' hc => ?_) ht
  by_cases hcy : c = y
  · subst hcy
    simp only [ite_true, Option.some.injEq] at hc
    subst hc
    rw [hx]
    simpa [hv, PValue.eval] using hval
  · simp [hcy] at hc

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
hold of their rows in the bindings the arm keeps, a row of the derived item's
query whose column `y` isn't NULL passes what its arms say of `y`. -/
theorem pulled_sound {s : Statement} {st : PState} {i y q : Nat} {view : Bool}
    (hk : (s.item i).kind = .derived q view) {t : STerm} (hp : pulled s st i y = some t)
    {db : Db} {f : Nat} {b : Bind} {r : Row} (hr : r ∈ evalQuery s db (f + 1) b q)
    (hterms : ∀ k ∈ (s.query q).arms, ∀ b' ∈ coreBindings s db (evalQuery s db f) b k,
      ∀ j u, (s.item j).core = k → (st.items j).term = some u → u.holds (b' j))
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
          have hv := hterms k hk' b' hb' j v hj hterm
          rw [hbj] at hv
          refine PushDown.renamed_sound hu (fun c₁ c₂ hc => ?_) hv
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
