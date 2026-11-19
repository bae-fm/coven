import SqliveNarrowing.Filter

/-! The restriction lemma (the third step of `narrowing_sound`): two
databases that hold the same rows passing each item's test give a statement
the same rows, when every binding that matters to the rows a query returns
holds only rows that pass their items' tests.

Each item `i` has a test `G i` on its rows, which the planner's scopes give,
and each query `q` a test `rel q` on its rows: the rows of `q` that matter
to the core reading it. A binding of an arm of `q` matters when its row
passes `rel q`, or always when the arm aggregates or calls a window function
(`relevant`). The promise the planner's scopes make (`Kept`) is that a
binding a core keeps that matters holds only rows that pass their items'
tests, and holds no row of an item only where `Left` allows, and that a
LEFT JOIN item that `Left` allows to hold no row has no row its ON holds for
that fails its test.

From that promise, `coreBindings_restricted` proves that a core keeps the
same bindings that matter from both databases: a join reads the same rows
that pass each item's test, and the ON and WHERE conditions read the same
subquery rows under a binding whose rows pass their tests. `arm_restricted`
and `queryRows_restricted` carry it to a query's rows that matter, through
DISTINCT, the compounds and the ORDER BY, and `agree_of_kept` to every query
but the statement's own at every fuel past its number, under the outer
bindings a run reads it under (`Restriction` names what the statement and
the tests must give). `result_restricted` gives the statement's rows when
they all matter. -/
namespace SqliveNarrowing

attribute [local instance] Classical.propDecidable

/-! Bindings whose rows pass their items' tests. -/

/-- Every row binding `b` holds passes its item's test. -/
def Passes (G : Nat → Row → Bool) (b : Bind) : Prop := ∀ i r, b i = some r → G i r = true

theorem Passes.empty (G : Nat → Row → Bool) : Passes G Bind.empty := fun _ _ h => by cases h

theorem Passes.set {G : Nat → Row → Bool} {b : Bind} (hb : Passes G b) {i : Nat} {x : Option Row}
    (hx : ∀ r, x = some r → G i r = true) : Passes G (b.set i x) := by
  intro j r h
  by_cases hji : j = i
  · subst hji
    exact hx r (by simpa [Bind.set] using h)
  · exact hb j r (by simpa [Bind.set, hji] using h)

/-- A binding whose rows pass their items' tests, and whose items among `S`
that hold no row are items `Left` allows to hold none. -/
def Restricted (G : Nat → Row → Bool) (Left : Nat → Prop) (S : List Nat) (b : Bind) : Prop :=
  Passes G b ∧ ∀ j ∈ S, b j = none → Left j

theorem restricted_set {G : Nat → Row → Bool} {Left : Nat → Prop} {S : List Nat} {b : Bind}
    {i : Nat} {x : Option Row} (hi : i ∉ S) (hbi : ∀ r, b i = some r → G i r = true) :
    Restricted G Left (S ++ [i]) (b.set i x) ↔
      Restricted G Left S b ∧ (∀ r, x = some r → G i r = true) ∧ (x = none → Left i) := by
  constructor
  · rintro ⟨hp, hl⟩
    refine ⟨⟨fun j r h => ?_, fun j hj h => ?_⟩, fun r h => ?_, fun h => ?_⟩
    · by_cases hji : j = i
      · subst hji; exact hbi r h
      · exact hp j r (by simp [Bind.set, hji, h])
    · have hji : j ≠ i := fun e => hi (e ▸ hj)
      exact hl j (by simp [hj]) (by simp [Bind.set, hji, h])
    · exact hp i r (by simp [Bind.set, h])
    · exact hl i (by simp) (by simp [Bind.set, h])
  · rintro ⟨⟨hp, hl⟩, hx, hn⟩
    refine ⟨hp.set hx, fun j hj h => ?_⟩
    by_cases hji : j = i
    · subst hji; exact hn (by simpa [Bind.set] using h)
    · have hjS : j ∈ S := by simpa [hji] using hj
      exact hl j hjS (by simpa [Bind.set, hji] using h)

/-- Whether item `i`'s ON conditions hold of a binding. -/
def onHolds (c : Core) (subs : Bind → Subs) (i : Nat) (b : Bind) : Bool :=
  (c.onOf i).all fun cd => isTrue (cd.eval b (subs b))

theorem mem_joinStep {s : Statement} {c : Core} {rowsOf : Nat → List Row} {subs : Bind → Subs}
    {i : Nat} {b b' : Bind} (h : b' ∈ View.joinStep s c rowsOf subs i b) : ∃ x, b' = b.set i x := by
  unfold View.joinStep at h
  simp only at h
  split at h
  · exact ⟨none, by simpa using h⟩
  · obtain ⟨r, _, hr⟩ := List.mem_filterMap.mp h
    split at hr
    · exact ⟨some r, (Option.some.inj hr).symm⟩
    · cases hr

/-- One item's step of a join keeps, of a binding whose rows pass their
tests, the same bindings that do from two sources that agree on the rows
that pass the item's test. -/
theorem joinStep_restricted {s : Statement} {c : Core} {rowsX rowsY : Nat → List Row}
    {subsX subsY : Bind → Subs} {G : Nat → Row → Bool} {Left : Nat → Prop} {V : Bind → Prop}
    {S : List Nat} {i : Nat}
    (hi : i ∉ S) (hrows : (rowsX i).filter (G i) = (rowsY i).filter (G i))
    (hon : ∀ b, Passes G b → V b → onHolds c subsX i b = onHolds c subsY i b)
    (hleftX : Left i → ∀ b, Passes G b → V b → ∀ r ∈ rowsX i,
      onHolds c subsX i (b.set i (some r)) = true → G i r = true)
    (hleftY : Left i → ∀ b, Passes G b → V b → ∀ r ∈ rowsY i,
      onHolds c subsY i (b.set i (some r)) = true → G i r = true)
    {b : Bind} (hVb : V b) (hVi : ∀ x, V (b.set i x)) (hbi : ∀ r, b i = some r → G i r = true) :
    (View.joinStep s c rowsX subsX i b).filter (fun b => decide (Restricted G Left (S ++ [i]) b)) =
      (View.joinStep s c rowsY subsY i b).filter
        (fun b => decide (Restricted G Left (S ++ [i]) b)) := by
  by_cases hb : Restricted G Left S b
  · let f (subs : Bind → Subs) (r : Row) : Option Bind :=
      if onHolds c subs i (b.set i (some r)) then some (b.set i (some r)) else none
    have hset : ∀ r, G i r = true → Passes G (b.set i (some r)) := fun r hg =>
      hb.1.set (fun r' h => by rw [← Option.some.inj h]; exact hg)
    -- The joined bindings that pass the tests are those of the rows that pass `G i`.
    have hfilter : ∀ (rows : List Row) (subs : Bind → Subs),
        (rows.filterMap (f subs)).filter (fun b => decide (Restricted G Left (S ++ [i]) b)) =
          (rows.filter (G i)).filterMap (f subs) := by
      intro rows subs
      rw [List.filter_filterMap, List.filterMap_filter]
      congr 1
      funext r
      simp only [f]
      split
      · have := restricted_set (Left := Left) (x := some r) hi hbi
        by_cases hg : G i r = true
        · simp [hg, Option.filter, this.mpr ⟨hb, fun r' h => by rw [← Option.some.inj h]; exact hg,
            by simp⟩]
        · have hn : ¬Restricted G Left (S ++ [i]) (b.set i (some r)) := fun h =>
            hg ((this.mp h).2.1 r rfl)
          simp [hg, Option.filter, hn]
      · split <;> rfl
    have hsame : ((rowsX i).filter (G i)).filterMap (f subsX) =
        ((rowsY i).filter (G i)).filterMap (f subsY) := by
      rw [hrows]
      apply filterMap_congr_mem
      intro r hr
      have hg : G i r = true := (List.mem_filter.mp hr).2
      simp only [f, hon _ (hset r hg) (hVi _)]
    have hstep : ∀ (rows : Nat → List Row) (subs : Bind → Subs),
        View.joinStep s c rows subs i b =
          if (s.item i).left && (rows i |>.filterMap (f subs)).isEmpty then [b.set i none]
          else (rows i).filterMap (f subs) := fun _ _ => rfl
    by_cases hl : Left i
    · -- Every row its ON holds for passes the test, so both steps are the same.
      have hall : ∀ (rows : List Row) (subs : Bind → Subs),
          (Left i → ∀ b, Passes G b → V b → ∀ r ∈ rows,
            onHolds c subs i (b.set i (some r)) = true → G i r = true) →
          rows.filterMap (f subs) = (rows.filter (G i)).filterMap (f subs) := by
        intro rows subs hleft
        rw [List.filterMap_filter]
        apply filterMap_congr_mem
        intro r hr
        by_cases hg : G i r = true
        · simp [hg]
        · simp only [hg, Bool.false_eq_true, ↓reduceIte, f]
          split
          · rename_i hon'
            exact absurd (hleft hl b hb.1 hVb r hr hon') hg
          · rfl
      rw [hstep, hstep, hall _ _ hleftX, hall _ _ hleftY, hsame]
    · -- A binding that holds no row of the item fails the test.
      have hnone : ¬Restricted G Left (S ++ [i]) (b.set i none) := fun h =>
        hl (((restricted_set hi hbi).mp h).2.2 rfl)
      have hcases : ∀ (rows : Nat → List Row) (subs : Bind → Subs),
          (View.joinStep s c rows subs i b).filter
              (fun b => decide (Restricted G Left (S ++ [i]) b)) =
            ((rows i).filterMap (f subs)).filter
              (fun b => decide (Restricted G Left (S ++ [i]) b)) := by
        intro rows subs
        rw [hstep]
        split
        · rename_i hif
          simp only [Bool.and_eq_true, List.isEmpty_iff] at hif
          rw [hif.2]
          simp [hnone]
        · rfl
      rw [hcases, hcases, hfilter, hfilter, hsame]
  · -- A binding that fails the tests gives only bindings that fail them.
    have hnil : ∀ (rows : Nat → List Row) (subs : Bind → Subs),
        (View.joinStep s c rows subs i b).filter
          (fun b => decide (Restricted G Left (S ++ [i]) b)) = [] := by
      intro rows subs
      apply List.filter_eq_nil_iff.mpr
      intro b' hb'
      obtain ⟨x, rfl⟩ := mem_joinStep hb'
      simpa using fun h => hb ((restricted_set hi hbi).mp h).1
    rw [hnil, hnil]

/-- A join keeps the same bindings whose rows pass their tests from two
sources that agree on the rows that pass each item's test, when the ON
conditions agree under such bindings and a LEFT JOIN item that holds no row
in such a binding has no row its ON holds for that fails its test. -/
theorem joinItems_restricted {s : Statement} {c : Core} {rowsX rowsY : Nat → List Row}
    {subsX subsY : Bind → Subs} {G : Nat → Row → Bool} {Left : Nat → Prop} {V : Bind → Prop} :
    ∀ (is S : List Nat), (S ++ is).Nodup → (∀ b j x, V b → j ∈ is → V (b.set j x)) →
    (∀ i ∈ is, (rowsX i).filter (G i) = (rowsY i).filter (G i)) →
    (∀ i ∈ is, ∀ b, Passes G b → V b → onHolds c subsX i b = onHolds c subsY i b) →
    (∀ i ∈ is, Left i → ∀ b, Passes G b → V b → ∀ r ∈ rowsX i,
      onHolds c subsX i (b.set i (some r)) = true → G i r = true) →
    (∀ i ∈ is, Left i → ∀ b, Passes G b → V b → ∀ r ∈ rowsY i,
      onHolds c subsY i (b.set i (some r)) = true → G i r = true) →
    ∀ (bsX bsY : List Bind), (∀ b ∈ bsX ++ bsY, V b) →
    (∀ b ∈ bsX ++ bsY, ∀ j ∈ is, ∀ r, b j = some r → G j r = true) →
    bsX.filter (fun b => decide (Restricted G Left S b)) =
      bsY.filter (fun b => decide (Restricted G Left S b)) →
    (joinItems s c rowsX subsX is bsX).filter (fun b => decide (Restricted G Left (S ++ is) b)) =
      (joinItems s c rowsY subsY is bsY).filter (fun b => decide (Restricted G Left (S ++ is) b))
  | [], S, _, _, _, _, _, _, bsX, bsY, _, _, h => by simpa [joinItems] using h
  | i :: is, S, hnd, hV, hrows, hon, hleftX, hleftY, bsX, bsY, hVbs, hgood, h => by
    rw [View.joinItems_cons, View.joinItems_cons]
    have hnd' : (S ++ [i] ++ is).Nodup := by simpa using hnd
    have hiS : i ∉ S := by
      intro hm
      have := (List.nodup_append.mp hnd).2.2 i hm i (by simp)
      exact this rfl
    have hiis : i ∉ is := by
      have := (List.nodup_append.mp hnd).2.1
      exact (List.nodup_cons.mp this).1
    have ih := joinItems_restricted (s := s) is (S ++ [i]) hnd'
      (fun b j x hb hj => hV b j x hb (List.mem_cons_of_mem i hj))
      (fun j hj => hrows j (List.mem_cons_of_mem i hj))
      (fun j hj => hon j (List.mem_cons_of_mem i hj))
      (fun j hj => hleftX j (List.mem_cons_of_mem i hj))
      (fun j hj => hleftY j (List.mem_cons_of_mem i hj))
    rw [List.append_assoc, List.singleton_append] at ih
    apply ih
    · intro b' hb'
      rcases List.mem_append.mp hb' with hb' | hb' <;>
      · obtain ⟨b, hb, hb'⟩ := List.mem_flatMap.mp hb'
        obtain ⟨x, rfl⟩ := mem_joinStep hb'
        exact hV b i x (hVbs b (by simp [hb])) (by simp)
    · intro b' hb' j hj r hr
      have hji : j ≠ i := fun e => hiis (e ▸ hj)
      rcases List.mem_append.mp hb' with hb' | hb' <;>
      · obtain ⟨b, hb, hb'⟩ := List.mem_flatMap.mp hb'
        obtain ⟨x, rfl⟩ := mem_joinStep hb'
        exact hgood b (by simp [hb]) j (List.mem_cons_of_mem i hj) r (by simpa [Bind.set, hji] using hr)
    · rw [List.filter_flatMap, List.filter_flatMap]
      have hbi : ∀ b ∈ bsX ++ bsY, ∀ r, b i = some r → G i r = true :=
        fun b hb => hgood b hb i (by simp)
      have hdrop : ∀ (bs : List Bind) (rows : Nat → List Row) (subs : Bind → Subs),
          (∀ b ∈ bs, ∀ r, b i = some r → G i r = true) →
          (bs.flatMap fun b => (View.joinStep s c rows subs i b).filter
              (fun b => decide (Restricted G Left (S ++ [i]) b))) =
            (bs.filter fun b => decide (Restricted G Left S b)).flatMap fun b =>
              (View.joinStep s c rows subs i b).filter
                (fun b => decide (Restricted G Left (S ++ [i]) b)) := by
        intro bs rows subs hbs
        apply flatMap_filter_of_nil
        intro b hb hfail
        apply List.filter_eq_nil_iff.mpr
        intro b' hb'
        obtain ⟨x, rfl⟩ := mem_joinStep hb'
        have hnot : ¬Restricted G Left S b := by simpa using hfail
        simpa using fun hr => hnot ((restricted_set hiS (hbs b hb)).mp hr).1
      rw [hdrop bsX rowsX subsX (fun b hb => hbi b (List.mem_append_left _ hb)),
        hdrop bsY rowsY subsY (fun b hb => hbi b (List.mem_append_right _ hb)), h]
      apply flatMap_congr_mem
      intro b hb
      have hbY : b ∈ bsY := (List.mem_filter.mp hb).1
      have hVb := hVbs b (List.mem_append_right _ hbY)
      exact joinStep_restricted hiS (hrows i (by simp)) (hon i (by simp))
        (hleftX i (by simp)) (hleftY i (by simp)) hVb (fun x => hV b i x hVb (by simp))
        (hbi b (List.mem_append_right _ hbY))

/-- What every binding a join starts from has, and each item's step keeps,
every binding the join keeps has. -/
theorem joinItems_preserves {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} {P : Bind → Prop} :
    ∀ (is : List Nat) (bs : List Bind), (∀ b ∈ bs, P b) → (∀ b i x, P b → i ∈ is → P (b.set i x)) →
      ∀ b ∈ joinItems s c rowsOf subs is bs, P b
  | [], _, hbs, _, b, hb => hbs b hb
  | i :: is, bs, hbs, hset, b, hb => by
    rw [View.joinItems_cons] at hb
    refine joinItems_preserves is _ (fun b' hb' => ?_)
      (fun b j x h hj => hset b j x h (List.mem_cons_of_mem i hj)) b hb
    obtain ⟨b0, hb0, hb'⟩ := List.mem_flatMap.mp hb'
    obtain ⟨x, rfl⟩ := mem_joinStep hb'
    exact hset b0 i x (hbs b0 hb0) (by simp)

/-- The binding a core's items join onto under the binding `o` around it:
none for a core in no other (`coreBindings`). -/
def Statement.outerOf (s : Statement) (k : Nat) (o : Bind) : Bind :=
  if (s.core k).parent.isNone then Bind.empty else o

/-- The rows a core joins of item `i`, with `E` running a derived item's
query under the binding `b0` its core's items join onto. -/
def Statement.itemRows (s : Statement) (Z : Db) (E : Bind → Nat → List Row) (b0 : Bind) (i : Nat) :
    List Row :=
  match (s.item i).kind with
  | .table t => Z.rows t
  | .derived q _ => E b0 q

/-- Whether binding `b` holds, outside core `k`'s items, the binding that
core `k`'s items join onto under `o`: as each binding a join of the core
builds does. -/
def Statement.Within (s : Statement) (k : Nat) (o b : Bind) : Prop :=
  ∀ j, j ∉ (s.core k).items → b j = s.outerOf k o j

/-- The third step's core lemma: a core keeps, under an outer binding whose
rows pass their tests, the same bindings that matter from two databases
whose tables hold the same rows that pass each item's test, whose derived
items' queries return the same rows that pass the item's test, and whose
subqueries return the same rows under a binding whose rows pass their tests,
when every binding that matters holds rows that pass their tests (and no
row only of an item `Left` allows), and a LEFT JOIN item `Left` allows has
no row its ON holds for that fails its test. -/
theorem coreBindings_restricted {s : Statement} {X Y : Db} {EX EY : Bind → Nat → List Row}
    {G : Nat → Row → Bool} {Left : Nat → Prop} {o : Bind} {k : Nat}
    (hnodup : (s.core k).items.Nodup) (ho : Passes G o)
    (htables : ∀ i ∈ (s.core k).items, ∀ t, s.table? i = some t →
      (X.rows t).filter (G i) = (Y.rows t).filter (G i))
    (hderived : ∀ i ∈ (s.core k).items, ∀ q v, (s.item i).kind = .derived q v →
      (EX (s.outerOf k o) q).filter (G i) = (EY (s.outerOf k o) q).filter (G i))
    (hsubs : ∀ b, Passes G b → s.Within k o b → ∀ q ∈ (s.core k).subqueries, EX b q = EY b q)
    (hleftX : ∀ i ∈ (s.core k).items, Left i → ∀ b, Passes G b → s.Within k o b →
      ∀ r ∈ s.itemRows X EX (s.outerOf k o) i, onHolds (s.core k) EX i (b.set i (some r)) = true →
        G i r = true)
    (hleftY : ∀ i ∈ (s.core k).items, Left i → ∀ b, Passes G b → s.Within k o b →
      ∀ r ∈ s.itemRows Y EY (s.outerOf k o) i, onHolds (s.core k) EY i (b.set i (some r)) = true →
        G i r = true)
    {RX RY : Bind → Bool} (hR : ∀ b, Passes G b → s.Within k o b → RX b = RY b)
    (hinvX : ∀ b ∈ coreBindings s X EX o k, RX b = true → Restricted G Left (s.core k).items b)
    (hinvY : ∀ b ∈ coreBindings s Y EY o k, RY b = true → Restricted G Left (s.core k).items b) :
    (coreBindings s X EX o k).filter RX = (coreBindings s Y EY o k).filter RY := by
  let c := s.core k
  have hcond : ∀ cd ∈ c.conds.map (·.1), ∀ b, Passes G b → s.Within k o b →
      cd.eval b (EX b) = cd.eval b (EY b) := by
    intro cd hcd b hb hw
    obtain ⟨⟨cd', p⟩, hmem, rfl⟩ := List.mem_map.mp hcd
    apply Cond.eval_congr
    · intro _ _ _; rfl
    · intro q hq
      apply hsubs b hb hw q
      unfold Core.subqueries
      exact List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_append_right _
        (List.mem_flatMap.mpr ⟨(cd', p), hmem, hq⟩)))
  have hall : ∀ (cds : List Cond), (∀ cd ∈ cds, cd ∈ c.conds.map (·.1)) → ∀ b, Passes G b →
      s.Within k o b →
      (cds.all fun cd => isTrue (cd.eval b (EX b))) = (cds.all fun cd => isTrue (cd.eval b (EY b))) := by
    intro cds hcds b hb hw
    induction cds with
    | nil => rfl
    | cons cd rest ihc =>
      simp only [List.all_cons]
      rw [hcond cd (hcds cd (by simp)) b hb hw, ihc (fun c' h' => hcds c' (by simp [h']))]
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
  let b0 := s.outerOf k o
  have hb0 : Passes G b0 := by
    simp only [b0, Statement.outerOf]
    split
    · exact Passes.empty G
    · exact ho
  let rowsOf (Z : Db) (E : Bind → Nat → List Row) : Nat → List Row := s.itemRows Z E b0
  let J (Z : Db) (E : Bind → Nat → List Row) := joinItems s c (rowsOf Z E) E c.items [b0]
  have hVset : ∀ b j x, s.Within k o b → j ∈ c.items → s.Within k o (b.set j x) := by
    intro b j x hb hj j' hj'
    have hne : j' ≠ j := fun e => hj' (e ▸ hj)
    simp only [Bind.set, hne, ↓reduceIte]
    exact hb j' hj'
  have hV0 : s.Within k o b0 := fun _ _ => rfl
  have hJ := joinItems_restricted (s := s) (c := c) (rowsX := rowsOf X EX) (rowsY := rowsOf Y EY)
    (subsX := EX) (subsY := EY) (G := G) (Left := Left) (V := s.Within k o) c.items []
    (by simpa using hnodup) hVset
    (fun i hi => by
      simp only [rowsOf, Statement.itemRows]
      cases hkind : (s.item i).kind with
      | table t => exact htables i hi t (by simp [Statement.table?, hkind])
      | derived q v => exact hderived i hi q v hkind)
    (fun i _ b hb hw => hall _ (fun cd hcd => honOf i cd hcd) b hb hw)
    hleftX hleftY [b0] [b0] (fun b hb => by
      simp only [List.mem_append, List.mem_singleton, or_self] at hb
      subst hb
      exact hV0) (fun b hb j _ r hr => by
      simp only [List.mem_append, List.mem_singleton, or_self] at hb
      subst hb
      exact hb0 j r hr) rfl
  simp only [List.nil_append] at hJ
  have hbind : ∀ (Z : Db) (E : Bind → Nat → List Row), coreBindings s Z E o k =
      (J Z E).filter fun b => c.innerConds.all fun cd => isTrue (cd.eval b (E b)) := fun _ _ => rfl
  have hW : ∀ (Z : Db) (E : Bind → Nat → List Row) (R : Bind → Bool),
      (∀ b ∈ coreBindings s Z E o k, R b = true → Restricted G Left c.items b) →
      (coreBindings s Z E o k).filter R =
        ((J Z E).filter fun b => decide (Restricted G Left c.items b)).filter
          fun b => R b && c.innerConds.all fun cd => isTrue (cd.eval b (E b)) := by
    intro Z E R hinv
    rw [hbind, List.filter_filter, filter_filter_of_imp]
    intro b hb hp
    simp only [Bool.and_eq_true] at hp
    exact decide_eq_true (hinv b (by rw [hbind]; exact List.mem_filter.mpr ⟨hb, hp.2⟩) hp.1)
  rw [hW X EX RX hinvX, hW Y EY RY hinvY, hJ]
  apply List.filter_congr
  intro b hb
  have hp : Passes G b := (of_decide_eq_true (List.mem_filter.mp hb).2).1
  have hw : s.Within k o b := joinItems_preserves c.items [b0]
    (fun b hb => by simp only [List.mem_singleton] at hb; subst hb; exact hV0) hVset b
    (List.mem_filter.mp hb).1
  rw [hR b hp hw, hall _ (fun cd hcd => hinner cd hcd) b hp hw]

/-! A query's rows that matter. -/

/-- A core's row of a binding, as a core that calls no window function makes
it. -/
def Core.outRow (c : Core) (E : Bind → Nat → List Row) (b : Bind) : Row :=
  c.output.map fun o => match o with
    | .expr e => e.eval b (E b)
    | _ => none

/-- Whether a binding of arm `k` of query `q` matters to the rows of `q` that
`rel q` keeps: its row passes `rel q`, or the arm aggregates or calls a
window function, which makes a row of many bindings. -/
def relevant (s : Statement) (rel : Nat → Row → Bool) (q k : Nat) (E : Bind → Nat → List Row)
    (b : Bind) : Bool :=
  (s.core k).aggregated || (s.core k).windowed || rel q ((s.core k).outRow E b)

theorem Core.rows_plain {c : Core} (hagg : c.aggregated = false) (hwin : c.windowed = false)
    (E : Bind → Nat → List Row) (kept : List Bind) : c.rows E kept = kept.map (c.outRow E) := by
  unfold Core.rows
  simp only [hagg, Bool.false_eq_true, ↓reduceIte]
  have hno : ∀ o ∈ c.output, ∀ p ob, o ≠ .rowNumber p ob := by
    intro o ho p ob he
    subst he
    have : c.windowed = true := List.any_eq_true.mpr ⟨_, ho, rfl⟩
    rw [hwin] at this
    cases this
  have : (kept.zipIdx.map fun (b, j) => c.output.map (OutCol.plain E kept b j)) =
      (kept.zipIdx.map Prod.fst).map (c.outRow E) := by
    rw [List.map_map]
    apply List.map_congr_left
    intro ⟨b, j⟩ _
    simp only [Function.comp, Core.outRow]
    apply List.map_congr_left
    intro o ho
    cases o with
    | expr e => rfl
    | rowNumber p ob => exact absurd rfl (hno _ ho p ob)
    | _ => rfl
  rw [this, List.zipIdx_map_fst]

/-- An arm's rows that pass a test, from two runs that keep the same
bindings that matter and read the same subquery rows under them. -/
theorem arm_restricted {s : Statement} {rel : Nat → Row → Bool} {q k : Nat}
    {EX EY : Bind → Nat → List Row} {keptX keptY : List Bind}
    (hkept : keptX.filter (relevant s rel q k EX) = keptY.filter (relevant s rel q k EY))
    (hE : ∀ b ∈ keptX.filter (relevant s rel q k EX), ∀ q' ∈ (s.core k).subqueries,
      EX b q' = EY b q') :
    ((s.core k).result EX keptX).filter (rel q) = ((s.core k).result EY keptY).filter (rel q) := by
  let c := s.core k
  have hout : ∀ b ∈ keptX.filter (relevant s rel q k EX), c.outRow EX b = c.outRow EY b := by
    intro b hb
    apply List.map_congr_left
    intro o ho
    cases o with
    | expr e =>
      apply e.eval_congr
      · intro _ _ _; rfl
      · intro q' hq'
        apply hE b hb q'
        unfold Core.subqueries
        exact List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_append_left _
          (List.mem_flatMap.mpr ⟨_, ho, List.mem_flatMap.mpr ⟨e, by simp [OutCol.exprs], hq'⟩⟩)))
    | _ => rfl
  cases hmany : c.aggregated || c.windowed with
  | true =>
    have htrue : ∀ E b, relevant s rel q k E b = true := by
      intro E b
      unfold relevant
      rw [show ((s.core k).aggregated || (s.core k).windowed) = true from hmany, Bool.true_or]
    have hX : keptX.filter (relevant s rel q k EX) = keptX := List.filter_eq_self.mpr fun b _ => htrue _ b
    have hY : keptY.filter (relevant s rel q k EY) = keptY := List.filter_eq_self.mpr fun b _ => htrue _ b
    rw [hX, hY] at hkept
    rw [hX] at hE
    subst hkept
    rw [Core.result_congr hE]
  | false =>
    simp only [Bool.or_eq_false_iff] at hmany
    have hres : ∀ E (kept : List Bind), (c.result E kept).filter (rel q) =
        (if c.distinct then List.eraseDups else id)
          ((kept.filter (relevant s rel q k E)).map (c.outRow E)) := by
      intro E kept
      have hrel : kept.filter (relevant s rel q k E) = kept.filter (rel q ∘ c.outRow E) := by
        apply List.filter_congr
        intro b _
        simp [relevant, c, hmany.1, hmany.2]
      unfold Core.result
      rw [Core.rows_plain hmany.1 hmany.2, hrel, ← List.filter_map]
      split
      · simp [filter_eraseDups]
      · rfl
    show (c.result EX keptX).filter (rel q) = (c.result EY keptY).filter (rel q)
    rw [hres, hres, List.map_congr_left hout, hkept]

/-- A query's rows that pass `rel q`, from two runs whose arms keep the same
bindings that matter and read the same subquery rows under them, when a
query with a LIMIT or an OFFSET keeps every row. -/
theorem queryRows_restricted {s : Statement} {rel : Nat → Row → Bool} {q : Nat} {X Y : Db}
    {EX EY : Bind → Nat → List Row} {o : Bind}
    (hpost : ((s.query q).limit.isSome || (s.query q).offset.isSome) = true → ∀ r, rel q r = true)
    (harms : ∀ k ∈ (s.query q).arms,
      (coreBindings s X EX o k).filter (relevant s rel q k EX) =
        (coreBindings s Y EY o k).filter (relevant s rel q k EY) ∧
      ∀ b ∈ (coreBindings s X EX o k).filter (relevant s rel q k EX),
        ∀ q' ∈ (s.core k).subqueries, EX b q' = EY b q') :
    (queryRows s X EX o q).filter (rel q) = (queryRows s Y EY o q).filter (rel q) := by
  unfold queryRows
  simp only
  have harm : ∀ k ∈ (s.query q).arms,
      ((s.core k).result EX (coreBindings s X EX o k)).filter (rel q) =
        ((s.core k).result EY (coreBindings s Y EY o k)).filter (rel q) :=
    fun k hk => arm_restricted (harms k hk).1 (harms k hk).2
  cases hcut : (s.query q).limit.isSome || (s.query q).offset.isSome with
  | true =>
    have htrue := hpost hcut
    have hid : ∀ l : List Row, l.filter (rel q) = l := fun l => List.filter_eq_self.mpr fun r _ => htrue r
    rw [hid, hid]
    congr 2
    apply List.map_congr_left
    intro k hk
    have := harm k hk
    rwa [hid, hid] at this
  | false =>
    simp only [Bool.or_eq_false_iff, Option.isSome_eq_false_iff, Option.isNone_iff_eq_none] at hcut
    rw [post_filter hcut.1 hcut.2, post_filter hcut.1 hcut.2, combineAll_filter, combineAll_filter,
      List.map_map, List.map_map]
    congr 2
    exact List.map_congr_left harm

/-! Every fuel. -/

/-- What the restriction needs of the statement, the tests and the outer
bindings `Outer q` that query `q` runs under, beside the promise: cores
that list each item once, queries numbered after the queries their arms
read, the statement's own query read by no core, tables that hold the same
rows that pass each item's test, a derived item's query whose rows that
matter include those that pass the item's test, subqueries all of whose
rows matter, a LIMIT or an OFFSET whose query's rows all matter, and the
bindings a derived item's query and a subquery run under among their outer
bindings. -/
structure Restriction (s : Statement) (X Y : Db) (G : Nat → Row → Bool) (rel : Nat → Row → Bool)
    (Outer : Nat → Bind → Prop) : Prop where
  nodup : ∀ q, ∀ k ∈ (s.query q).arms, (s.core k).items.Nodup
  nested : ∀ q, ∀ k ∈ (s.query q).arms, ∀ q' ∈ s.nestedQueries k, q' < q
  root : ∀ q, ∀ k ∈ (s.query q).arms, s.root ∉ s.nestedQueries k
  tables : ∀ i t, s.table? i = some t → (X.rows t).filter (G i) = (Y.rows t).filter (G i)
  derived : ∀ i q v, (s.item i).kind = .derived q v → ∀ r, G i r = true → rel q r = true
  subs : ∀ q, ∀ k ∈ (s.query q).arms, ∀ q' ∈ (s.core k).subqueries, ∀ r, rel q' r = true
  post : ∀ q, q ≠ s.root →
    ((s.query q).limit.isSome || (s.query q).offset.isSome) = true → ∀ r, rel q r = true
  derivedOuter : ∀ q, ∀ k ∈ (s.query q).arms, ∀ o, Outer q o → ∀ i ∈ (s.core k).items,
    ∀ q' v, (s.item i).kind = .derived q' v → Outer q' (s.outerOf k o)
  subOuter : ∀ q, ∀ k ∈ (s.query q).arms, ∀ o, Outer q o → ∀ b, s.Within k o b →
    ∀ q' ∈ (s.core k).subqueries, Outer q' b

/-- At fuel `f`, two databases' runs of each query below `f` but the
statement's own return the same rows that `rel` keeps, under any of its
outer bindings whose rows pass their tests. -/
def Agree (s : Statement) (X Y : Db) (G : Nat → Row → Bool) (rel : Nat → Row → Bool)
    (Outer : Nat → Bind → Prop) (f : Nat) : Prop :=
  ∀ q, q < f → q ≠ s.root → ∀ o, Outer q o → Passes G o →
    (evalQuery s X f o q).filter (rel q) = (evalQuery s Y f o q).filter (rel q)

/-- What the planner's scopes promise of a run on `Z` at fuel `f`, for each
query `q` up to `f` and each of its outer bindings whose rows pass their
tests: every binding of an arm of `q` that its core keeps, and that matters,
holds rows that pass their tests, and no row only of an item `Left` allows;
and an item `Left` allows has no row its ON holds for, in a binding of the
core whose rows pass their tests, that fails its test. -/
def Kept (s : Statement) (Z : Db) (G : Nat → Row → Bool) (Left : Nat → Prop)
    (rel : Nat → Row → Bool) (Outer : Nat → Bind → Prop) (f : Nat) : Prop :=
  ∀ q, q ≤ f → ∀ k ∈ (s.query q).arms, ∀ o, Outer q o → Passes G o →
    (∀ b ∈ coreBindings s Z (evalQuery s Z f) o k, relevant s rel q k (evalQuery s Z f) b = true →
      Restricted G Left (s.core k).items b) ∧
    ∀ i ∈ (s.core k).items, Left i → ∀ b, Passes G b → s.Within k o b →
      ∀ r ∈ s.itemRows Z (evalQuery s Z f) (s.outerOf k o) i,
        onHolds (s.core k) (evalQuery s Z f) i (b.set i (some r)) = true → G i r = true

/-- What two runs share at fuel `f` for a query up to `f` whose arms the
planner's scopes keep their promise for: its rows that `rel` keeps. -/
theorem queryRows_agree {s : Statement} {X Y : Db} {G : Nat → Row → Bool} {Left : Nat → Prop}
    {rel : Nat → Row → Bool} {Outer : Nat → Bind → Prop} (h : Restriction s X Y G rel Outer)
    {f q : Nat} (hq : q ≤ f) {o : Bind} (hout : Outer q o) (ho : Passes G o)
    (hpost : ((s.query q).limit.isSome || (s.query q).offset.isSome) = true → ∀ r, rel q r = true)
    (hagree : Agree s X Y G rel Outer f) (hX : Kept s X G Left rel Outer f)
    (hY : Kept s Y G Left rel Outer f) :
    (queryRows s X (evalQuery s X f) o q).filter (rel q) =
      (queryRows s Y (evalQuery s Y f) o q).filter (rel q) := by
  apply queryRows_restricted hpost
  intro k hk
  have hsubs : ∀ b, Passes G b → s.Within k o b → ∀ q' ∈ (s.core k).subqueries,
      evalQuery s X f b q' = evalQuery s Y f b q' := by
    intro b hb hw q' hq'
    have hmem : q' ∈ s.nestedQueries k := List.mem_append_right _ hq'
    have hne : q' ≠ s.root := fun e => h.root q k hk (e ▸ hmem)
    have := hagree q' (Nat.lt_of_lt_of_le (h.nested q k hk q' hmem) hq) hne b
      (h.subOuter q k hk o hout b hw q' hq') hb
    rwa [List.filter_eq_self.mpr (fun r _ => h.subs q k hk q' hq' r),
      List.filter_eq_self.mpr (fun r _ => h.subs q k hk q' hq' r)] at this
  have hb0 : Passes G (s.outerOf k o) := by
    unfold Statement.outerOf
    split
    · exact Passes.empty G
    · exact ho
  have hkeep := coreBindings_restricted (k := k) (o := o) (h.nodup q k hk) ho
    (fun i _ t ht => h.tables i t ht)
    (fun i hi q' v hkind => by
      have hmem : q' ∈ s.nestedQueries k :=
        List.mem_append_left _ (List.mem_filterMap.mpr ⟨i, hi, by simp [hkind]⟩)
      have hne : q' ≠ s.root := fun e => h.root q k hk (e ▸ hmem)
      have := congrArg (List.filter (G i)) (hagree q' (Nat.lt_of_lt_of_le
        (h.nested q k hk q' hmem) hq) hne _ (h.derivedOuter q k hk o hout i hi q' v hkind) hb0)
      rwa [filter_filter_of_imp (fun r _ hr => h.derived i q' v hkind r hr),
        filter_filter_of_imp (fun r _ hr => h.derived i q' v hkind r hr)] at this)
    hsubs ((hX q hq k hk o hout ho).2) ((hY q hq k hk o hout ho).2)
    (RX := relevant s rel q k (evalQuery s X f)) (RY := relevant s rel q k (evalQuery s Y f))
    (fun b hb hw => by
      unfold relevant
      congr 2
      apply List.map_congr_left
      intro o' ho'
      cases o' with
      | expr e =>
        apply e.eval_congr
        · intro _ _ _; rfl
        · intro q' hq'
          apply hsubs b hb hw q'
          unfold Core.subqueries
          exact List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_append_left _
            (List.mem_flatMap.mpr ⟨_, ho', List.mem_flatMap.mpr ⟨e, by simp [OutCol.exprs], hq'⟩⟩)))
      | _ => rfl)
    ((hX q hq k hk o hout ho).1) ((hY q hq k hk o hout ho).1)
  refine ⟨hkeep, fun b hb q' hq' => hsubs b ?_ ?_ q' hq'⟩
  · have hb' := List.mem_filter.mp hb
    exact ((hX q hq k hk o hout ho).1 b hb'.1 hb'.2).1
  · intro j hj
    exact joinItems_preserves (P := s.Within k o) (s.core k).items [s.outerOf k o]
      (fun b hb => by simp only [List.mem_singleton] at hb; subst hb; exact fun _ _ => rfl)
      (fun b j' x hb hj' j'' hj'' => by
        have hne : j'' ≠ j' := fun e => hj'' (e ▸ hj')
        simp only [Bind.set, hne, ↓reduceIte]
        exact hb j'' hj'')
      b (List.mem_filter.mp (List.mem_filter.mp hb).1).1 j hj

/-- The third step's theorem: when the planner's scopes keep their promise
at each fuel, given that two runs return the same rows that matter at every
lower fuel, the runs return the same rows that matter of every query but the
statement's own, at every fuel past the query's number. -/
theorem agree_of_kept {s : Statement} {X Y : Db} {G : Nat → Row → Bool} {Left : Nat → Prop}
    {rel : Nat → Row → Bool} {Outer : Nat → Bind → Prop} (h : Restriction s X Y G rel Outer)
    (hkept : ∀ f, (∀ f' ≤ f, Agree s X Y G rel Outer f') →
      Kept s X G Left rel Outer f ∧ Kept s Y G Left rel Outer f) :
    ∀ f, Agree s X Y G rel Outer f := by
  have key : ∀ f, ∀ f' ≤ f, Agree s X Y G rel Outer f' := by
    intro f
    induction f with
    | zero =>
      intro f' hf' q _ _ o _ _
      rw [Nat.le_zero.mp hf']
      rfl
    | succ f ih =>
      intro f' hf'
      rcases Nat.lt_or_eq_of_le hf' with hlt | heq
      · exact ih f' (Nat.le_of_lt_succ hlt)
      · subst heq
        obtain ⟨hX, hY⟩ := hkept f ih
        intro q hq hne o hout ho
        exact queryRows_agree h (Nat.le_of_lt_succ hq) hout ho (h.post q hne)
          (ih f (Nat.le_refl _)) hX hY
  exact fun f => key f f (Nat.le_refl _)

/-- The statement's rows from two databases, when every row of its own
query matters and the planner's scopes keep their promise: with no LIMIT
bound, every binding of the statement's own core matters. -/
theorem result_restricted {s : Statement} {X Y : Db} {G : Nat → Row → Bool} {Left : Nat → Prop}
    {rel : Nat → Row → Bool} {Outer : Nat → Bind → Prop} (h : Restriction s X Y G rel Outer)
    (hroot : s.root < s.queries.length) (hrootOuter : Outer s.root Bind.empty)
    (hall : ∀ r, rel s.root r = true)
    (hkept : ∀ f, (∀ f' ≤ f, Agree s X Y G rel Outer f') →
      Kept s X G Left rel Outer f ∧ Kept s Y G Left rel Outer f) :
    s.result X = s.result Y := by
  have hagree := agree_of_kept h hkept
  unfold Statement.result
  simp only [evalQuery]
  have := queryRows_agree h (q := s.root) (f := s.queries.length) (Nat.le_of_lt hroot) hrootOuter
    (Passes.empty G) (fun _ => hall) (hagree _) (hkept _ fun f' _ => hagree f').1
    (hkept _ fun f' _ => hagree f').2
  rwa [List.filter_eq_self.mpr (fun r _ => hall r), List.filter_eq_self.mpr (fun r _ => hall r)] at this

/-! The tests the planner's scopes give. -/

/-- A table's scope passes each row that the scope of an item reading it
passes: the table's scope joins its items' scopes with OR. -/
theorem tableScope_passes {s : Statement} {scopes : Nat → RScope} {rowsOf : Nat → List Row}
    {i t : Nat} (hi : i < s.items.length) (ht : s.table? i = some t) {r : Row}
    (hr : (scopes i).passes rowsOf r = true) :
    ((tableScope s scopes t).getD .all).passes rowsOf r = true := by
  let P : Option RScope → Prop := fun found => ∃ sc, found = some sc ∧ sc.passes rowsOf r = true
  have step : ∀ (l : List Nat) (found : Option RScope), (P found ∨ i ∈ l) →
      P (l.foldl (fun found j =>
        if s.table? j = some t then
          some (match found with
            | some other => other.either (scopes j)
            | none => scopes j)
        else found) found) := by
    intro l
    induction l with
    | nil =>
      intro found h
      simpa using h
    | cons j l ih =>
      intro found h
      simp only [List.foldl_cons]
      apply ih
      by_cases hj : j = i
      · subst hj
        left
        rw [ite_eq_left_iff.mpr (fun h => absurd ht h)]
        cases found with
        | none => exact ⟨_, rfl, hr⟩
        | some other => exact ⟨_, rfl, by rw [Limit.passes_either, hr, Bool.or_true]⟩
      · rcases h with h | h
        · left
          split
          · obtain ⟨sc, rfl, hsc⟩ := h
            exact ⟨_, rfl, by rw [Limit.passes_either, hsc, Bool.true_or]⟩
          · exact h
        · right
          rcases List.mem_cons.mp h with h | h
          · exact absurd h.symm hj
          · exact h
  obtain ⟨sc, hsc, hpass⟩ := step s.itemNumbers none
    (Or.inr (by simpa [Statement.itemNumbers] using hi))
  have hsc' : tableScope s scopes t = some sc := hsc
  rw [hsc']
  exact hpass

/-- The restriction's tables: when the judge lets a write pass, what sqlive
keeps of each table holds, before and after the write, the same rows that
pass any test stronger than the scope of an item reading it, with lookups in
what it keeps of the database after the write. -/
theorem scoped_rows {s : Statement} {db db' : Db} {changes : List Change}
    (hw : Writes db changes db') (hr : s.reruns db changes db' = false) {scopes : Nat → RScope}
    (hscopes : s.tableScopes db = some (tableScope s scopes)) {i t : Nat}
    (hi : i < s.items.length) (ht : s.table? i = some t) {G : Row → Bool}
    (hG : ∀ r, G r = true → (scopes i).passes (lookupRows s (db'.mask s.reads)) r = true) :
    ((db.mask s.reads).rows t).filter G = ((db'.mask s.reads).rows t).filter G := by
  have h := unjudged_rows hw hr t
  simp only [hscopes, Option.getD_some] at h
  have himp : ∀ (l : List Row), ∀ r ∈ l, G r = true →
      ((tableScope s scopes t).getD .all).passes (lookupRows s (db'.mask s.reads)) r = true :=
    fun _ r _ hg => tableScope_passes hi ht (hG r hg)
  have e1 := filter_filter_of_imp (himp ((db.mask s.reads).rows t))
  have e2 := filter_filter_of_imp (himp ((db'.mask s.reads).rows t))
  rw [← e1, ← e2, h]

end SqliveNarrowing
