import SqliveNarrowing.Mask

/-! A join's lookup reads a derived item alone, with the fuel
`lookupRows` gives, while the statement reads the item's rows with whatever
fuel is left where the item sits. A well-formed statement numbers each query
after the queries its arms read, so a query returns the same rows at every
fuel past its number (`evalQuery_fuel`), and a lookup of a derived item whose
arms sit in no core reads the rows the statement reads of it
(`lookupRows_derived`). -/
namespace SqliveNarrowing

/-- A query returns the same rows from two ways of running the queries its
arms read that agree on those queries. -/
theorem queryRows_congr {s : Statement} {db : Db} {E E' : Bind → Nat → List Row} {q : Nat}
    (h : ∀ k ∈ (s.query q).arms, ∀ q' ∈ s.nestedQueries k, ∀ b, E b q' = E' b q') (b : Bind) :
    queryRows s db E b q = queryRows s db E' b q := by
  unfold queryRows
  dsimp only
  congr 2
  apply List.map_congr_left
  intro k hk
  let c := s.core k
  have hsub : ∀ q' ∈ c.subqueries, ∀ x, E x q' = E' x q' := fun q' hq' =>
    h k hk q' (List.mem_append_right _ hq')
  have hderived : ∀ i ∈ c.items, ∀ q' v, (s.item i).kind = .derived q' v → ∀ x, E x q' = E' x q' := by
    intro i hi q' v hkind
    apply h k hk q'
    refine List.mem_append_left _ (List.mem_filterMap.mpr ⟨i, hi, ?_⟩)
    simp [hkind]
  have hcond : ∀ cd ∈ c.conds.map (·.1), ∀ x, cd.eval x (E' x) = cd.eval x (E x) := by
    intro cd hcd x
    obtain ⟨⟨cd', p⟩, hmem, rfl⟩ := List.mem_map.mp hcd
    apply Cond.eval_congr
    · intro _ _ _; rfl
    · intro q' hq'
      refine (hsub q' ?_ x).symm
      unfold Core.subqueries
      exact List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_append_right _
        (List.mem_flatMap.mpr ⟨(cd', p), hmem, hq'⟩)))
  have hall : ∀ (cds : List Cond), (∀ cd ∈ cds, cd ∈ c.conds.map (·.1)) → ∀ x,
      (cds.all fun cd => isTrue (cd.eval x (E' x))) = (cds.all fun cd => isTrue (cd.eval x (E x))) := by
    intro cds hcds x
    induction cds with
    | nil => rfl
    | cons cd rest ihc =>
      simp only [List.all_cons]
      rw [hcond cd (hcds cd (by simp)) x, ihc (fun c' h' => hcds c' (by simp [h']))]
  have honOf : ∀ i cd, cd ∈ c.onOf i → cd ∈ c.conds.map (·.1) := by
    intro i cd hcd
    simp only [Core.onOf, List.mem_filterMap] at hcd
    obtain ⟨⟨cd', p⟩, hmem, hp⟩ := hcd
    split at hp
    · cases hp; exact List.mem_map.mpr ⟨_, hmem, rfl⟩
    · cases hp
  have hinner : ∀ cd, cd ∈ c.innerConds → cd ∈ c.conds.map (·.1) := by
    intro cd hcd
    simp only [Core.innerConds, List.mem_filterMap] at hcd
    obtain ⟨⟨cd', p⟩, hmem, hp⟩ := hcd
    split at hp
    · cases hp; exact List.mem_map.mpr ⟨_, hmem, rfl⟩
    · cases hp
  let b0 := if c.parent.isNone then Bind.empty else b
  let rowsOf (i : Nat) : List Row :=
    match (s.item i).kind with
    | .table t => db.rows t
    | .derived q _ => E b0 q
  let rowsOf' (i : Nat) : List Row :=
    match (s.item i).kind with
    | .table t => db.rows t
    | .derived q _ => E' b0 q
  have hrowsOf : ∀ i ∈ c.items, rowsOf' i = (rowsOf i).map ((fun _ => id) i) := by
    intro i hi
    rw [List.map_id]
    simp only [rowsOf, rowsOf']
    cases hkind : (s.item i).kind with
    | table t => rfl
    | derived q' v => exact (hderived i hi q' v hkind _).symm
  have hj := joinItems_map s c (M := id) (mr := fun _ => id) (fun x i r => by simp) c.items
    hrowsOf (fun i _ x => hall _ (fun cd hcd => honOf i cd hcd) x) [b0] (subs := E) (subs' := E')
  rw [List.map_id, List.map_id] at hj
  have hbind : coreBindings s db E' b k = coreBindings s db E b k := by
    unfold coreBindings
    change (joinItems s c rowsOf' E' c.items [b0]).filter _ =
      (joinItems s c rowsOf E c.items [b0]).filter _
    rw [← hj]
    congr 1
    funext x
    exact hall _ (fun cd hcd => hinner cd hcd) x
  have hrows : ∀ L : List Bind, c.rows E' (L.map id) = c.rows E L := by
    apply View.rows_restrict
    intro e he x
    apply e.eval_congr
    · intro _ _ _; rfl
    · intro q' hq'
      refine (hsub q' ?_ x).symm
      unfold Core.subqueries
      simp only [View.outExprs, List.mem_append, List.mem_flatMap] at he
      refine List.mem_eraseDups.mpr ?_
      rcases he with ⟨o, ho, he⟩ | he
      · exact List.mem_append_left _ (List.mem_append_left _
          (List.mem_flatMap.mpr ⟨o, ho, List.mem_flatMap.mpr ⟨e, he, hq'⟩⟩))
      · exact List.mem_append_right _ (List.mem_flatMap.mpr ⟨e, he, hq'⟩)
  change c.result E (coreBindings s db E b k) = c.result E' (coreBindings s db E' b k)
  rw [hbind]
  unfold Core.result
  rw [← hrows, List.map_id]

/-- A well-formed statement numbers each query after the queries its arms
read. -/
theorem Statement.wellFormed_nested {s : Statement} (h : s.wellFormed = true) {q : Nat}
    (hq : q < s.queries.length) {k : Nat} (hk : k ∈ (s.query q).arms) {q' : Nat}
    (hq' : q' ∈ s.nestedQueries k) : q' < q :=
  ((s.wellFormed_parts h).arms q hq k hk).2 q' hq'

/-- A query of a well-formed statement returns the same rows at every fuel
past its number. -/
theorem evalQuery_fuel {s : Statement} (hwf : s.wellFormed = true) (db : Db) :
    ∀ q, q < s.queries.length → ∀ f, q < f → ∀ b, evalQuery s db f b q = evalQuery s db (q + 1) b q := by
  intro q
  induction q using Nat.strongRecOn with
  | ind q ih =>
    intro hq f hf b
    obtain ⟨f', rfl⟩ : ∃ f', f = f' + 1 := ⟨f - 1, by omega⟩
    simp only [evalQuery]
    apply queryRows_congr
    intro k hk q' hq' b'
    have hlt := s.wellFormed_nested hwf hq hk hq'
    rw [ih q' hlt (by omega) f' (by omega) b', ih q' hlt (by omega) q (by omega) b']

/-- A lookup of a derived item whose query's arms sit in no core reads the
rows the statement reads of it, under any binding and at any fuel past the
query's number. -/
theorem lookupRows_derived {s : Statement} (hwf : s.wellFormed = true) (db : Db) {i q : Nat}
    {view : Bool} (hk : (s.item i).kind = .derived q view) (hq : q < s.queries.length)
    (harms : ∀ k ∈ (s.query q).arms, (s.core k).parent = none) {f : Nat} (hf : q < f) (b : Bind) :
    evalQuery s db f b q = lookupRows s db i := by
  unfold lookupRows
  rw [hk]
  simp only
  rw [View.view_sound harms db f b, evalQuery_fuel hwf db q hq f hf,
    evalQuery_fuel hwf db q hq (s.queries.length + 1) (by omega)]

end SqliveNarrowing
