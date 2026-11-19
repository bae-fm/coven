import SqliveNarrowing.Filter
import SqliveNarrowing.Outer

/-! The restriction lemma: two databases that hold the same rows passing each
item's test give a statement the same rows, when every binding that matters to
the rows a query returns holds only rows that pass their items' tests.

Each item `i` has a test `G i` on its rows, which the planner's scopes give,
and each query `q` a test `rel q` on its rows: the rows of `q` that matter
to the core reading it. A binding of an arm of `q` matters when its row
passes `rel q`; in an arm that aggregates, when its group can make a row that
passes `rel q`, as its GROUP BY values say (`keyMatters`), which all its
group's bindings share; and always in an arm that calls a window function
(`relevant`). The promise the planner's scopes make (`Kept`) is that a
binding a core keeps that matters holds only rows that pass their items'
tests, and holds no row of an item only where `Left` allows, and that a
LEFT JOIN item that `Left` allows to hold no row has no row its ON holds for
that fails its test.

From that promise, `coreBindings_restricted` proves that a core keeps the
same bindings that matter from both databases: a join reads the same rows
that pass each item's test, and the ON and WHERE conditions read the same
subquery rows under a binding whose rows pass their tests and that holds a
row of each item no LEFT JOIN adds that the join has reached (`BoundIn`). `arm_restricted`
and `queryRows_restricted` carry it to a query's rows that matter, through
DISTINCT, the compounds and the ORDER BY, and `agree_of_kept` to every query
but the statement's own at every fuel past its number, under the outer
bindings a run reads it under (`Statement.Outer`; `Restriction` names what
the statement and the tests must give). `result_restricted` gives the statement's rows when
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

/-- A binding that only items `Left` allows hold no row of, when `Left`
allows none that no LEFT JOIN adds, holds a row of each item of `S` that no
LEFT JOIN adds. -/
theorem Restricted.boundIn {s : Statement} {G : Nat → Row → Bool} {Left : Nat → Prop}
    (hLeft : ∀ j, Left j → s.alwaysBound j = false) {S : List Nat} {b : Bind}
    (h : Restricted G Left S b) : s.BoundIn S b := by
  intro j hj hab hn
  rw [hLeft j (h.2 j hj hn)] at hab
  cases hab

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
    (hLeft : ∀ j, Left j → s.alwaysBound j = false)
    (hon : ∀ b, Passes G b → V b → s.BoundIn (S ++ [i]) b →
      onHolds c subsX i b = onHolds c subsY i b)
    (hleftX : Left i → ∀ b, Passes G b → V b → s.BoundIn S b → ∀ r ∈ rowsX i,
      onHolds c subsX i (b.set i (some r)) = true → G i r = true)
    (hleftY : Left i → ∀ b, Passes G b → V b → s.BoundIn S b → ∀ r ∈ rowsY i,
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
    have hbound : s.BoundIn S b := hb.boundIn hLeft
    have hboundSet : ∀ r, s.BoundIn (S ++ [i]) (b.set i (some r)) := by
      intro r j hj hab
      by_cases hji : j = i
      · subst hji
        simp [Bind.set]
      · have hjS : j ∈ S := by simpa [hji] using hj
        simpa [Bind.set, hji] using hbound j hjS hab
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
      simp only [f, hon _ (hset r hg) (hVi _) (hboundSet r)]
    have hstep : ∀ (rows : Nat → List Row) (subs : Bind → Subs),
        View.joinStep s c rows subs i b =
          if (s.item i).left && (rows i |>.filterMap (f subs)).isEmpty then [b.set i none]
          else (rows i).filterMap (f subs) := fun _ _ => rfl
    by_cases hl : Left i
    · -- Every row its ON holds for passes the test, so both steps are the same.
      have hall : ∀ (rows : List Row) (subs : Bind → Subs),
          (Left i → ∀ b, Passes G b → V b → s.BoundIn S b → ∀ r ∈ rows,
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
            exact absurd (hleft hl b hb.1 hVb hbound r hr hon') hg
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
    {subsX subsY : Bind → Subs} {G : Nat → Row → Bool} {Left : Nat → Prop} {V : Bind → Prop}
    (hLeft : ∀ j, Left j → s.alwaysBound j = false) :
    ∀ (is S : List Nat), (S ++ is).Nodup → (∀ b j x, V b → j ∈ is → V (b.set j x)) →
    (∀ i ∈ is, (rowsX i).filter (G i) = (rowsY i).filter (G i)) →
    (∀ i ∈ is, ∀ b, Passes G b → V b → s.BoundIn (S ++ is.takeWhile (· ≠ i) ++ [i]) b →
      onHolds c subsX i b = onHolds c subsY i b) →
    (∀ i ∈ is, Left i → ∀ b, Passes G b → V b → s.BoundIn (S ++ is.takeWhile (· ≠ i)) b →
      ∀ r ∈ rowsX i, onHolds c subsX i (b.set i (some r)) = true → G i r = true) →
    (∀ i ∈ is, Left i → ∀ b, Passes G b → V b → s.BoundIn (S ++ is.takeWhile (· ≠ i)) b →
      ∀ r ∈ rowsY i, onHolds c subsY i (b.set i (some r)) = true → G i r = true) →
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
    -- An item after `i` comes after `S`, `i` and the items between.
    have htake : ∀ j ∈ is, (i :: is).takeWhile (· ≠ j) = i :: is.takeWhile (· ≠ j) := by
      intro j hj
      have hij : i ≠ j := fun e => hiis (e ▸ hj)
      simp [hij]
    have ih := joinItems_restricted (s := s) hLeft is (S ++ [i]) hnd'
      (fun b j x hb hj => hV b j x hb (List.mem_cons_of_mem i hj))
      (fun j hj => hrows j (List.mem_cons_of_mem i hj))
      (fun j hj b hb hv hbd => hon j (List.mem_cons_of_mem i hj) b hb hv
        (by rw [htake j hj]; simpa using hbd))
      (fun j hj hl b hb hv hbd => hleftX j (List.mem_cons_of_mem i hj) hl b hb hv
        (by rw [htake j hj]; simpa using hbd))
      (fun j hj hl b hb hv hbd => hleftY j (List.mem_cons_of_mem i hj) hl b hb hv
        (by rw [htake j hj]; simpa using hbd))
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
      exact joinStep_restricted hiS (hrows i (by simp)) hLeft
        (fun b hb hv hbd => hon i (by simp) b hb hv (by simpa using hbd))
        (fun hl b hb hv hbd => hleftX i (by simp) hl b hb hv (by simpa using hbd))
        (fun hl b hb hv hbd => hleftY i (by simp) hl b hb hv (by simpa using hbd))
        hVb (fun x => hV b i x hVb (by simp)) (hbi b (List.mem_append_right _ hbY))

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

/-- The rows a core joins of item `i`, with `E` running a derived item's
query under the binding `b0` its core's items join onto. -/
def Statement.itemRows (s : Statement) (Z : Db) (E : Bind → Nat → List Row) (b0 : Bind) (i : Nat) :
    List Row :=
  match (s.item i).kind with
  | .table t => Z.rows t
  | .derived q _ => E b0 q

/-- A core keeps, under an outer binding whose rows pass their tests, the same
bindings that matter from two databases whose tables hold the same rows that
pass each item's test, whose derived items' queries return the same rows that
pass the item's test, and whose subqueries return the same rows under a binding
whose rows pass their tests (an ON's under one that holds a row of each item up
to its own that no LEFT JOIN adds, any other's under one that holds a row of
each such item), when every binding that matters holds rows that pass their
tests (and no row only of an item `Left` allows, which allows none that no LEFT
JOIN adds), and a LEFT JOIN item `Left` allows has no row its ON holds for that
fails its test. -/
theorem coreBindings_restricted {s : Statement} {X Y : Db} {EX EY : Bind → Nat → List Row}
    {G : Nat → Row → Bool} {Left : Nat → Prop} {o : Bind} {k : Nat}
    (hLeft : ∀ j, Left j → s.alwaysBound j = false)
    (hnodup : (s.core k).items.Nodup) (ho : Passes G o)
    (htables : ∀ i ∈ (s.core k).items, ∀ t, s.table? i = some t →
      (X.rows t).filter (G i) = (Y.rows t).filter (G i))
    (hderived : ∀ i ∈ (s.core k).items, ∀ q v, (s.item i).kind = .derived q v →
      (EX (s.outerOf k o) q).filter (G i) = (EY (s.outerOf k o) q).filter (G i))
    (hsubsOn : ∀ i ∈ (s.core k).items, ∀ b, Passes G b → s.Within k o b →
      s.BoundIn ((s.core k).items.takeWhile (· ≠ i) ++ [i]) b →
      ∀ cd ∈ (s.core k).onOf i, ∀ q ∈ cd.queries, EX b q = EY b q)
    (hsubs : ∀ b, Passes G b → s.Within k o b → s.BoundIn (s.core k).items b →
      ∀ q ∈ (s.core k).subqueries, EX b q = EY b q)
    (hleftX : ∀ i ∈ (s.core k).items, Left i → ∀ b, Passes G b → s.Within k o b →
      s.BoundIn ((s.core k).items.takeWhile (· ≠ i)) b →
      ∀ r ∈ s.itemRows X EX (s.outerOf k o) i, onHolds (s.core k) EX i (b.set i (some r)) = true →
        G i r = true)
    (hleftY : ∀ i ∈ (s.core k).items, Left i → ∀ b, Passes G b → s.Within k o b →
      s.BoundIn ((s.core k).items.takeWhile (· ≠ i)) b →
      ∀ r ∈ s.itemRows Y EY (s.outerOf k o) i, onHolds (s.core k) EY i (b.set i (some r)) = true →
        G i r = true)
    {RX RY : Bind → Bool}
    (hR : ∀ b, Passes G b → s.Within k o b → s.BoundIn (s.core k).items b → RX b = RY b)
    (hinvX : ∀ b ∈ coreBindings s X EX o k, RX b = true → Restricted G Left (s.core k).items b)
    (hinvY : ∀ b ∈ coreBindings s Y EY o k, RY b = true → Restricted G Left (s.core k).items b) :
    (coreBindings s X EX o k).filter RX = (coreBindings s Y EY o k).filter RY := by
  let c := s.core k
  have hall : ∀ (cds : List Cond) (b : Bind), (∀ cd ∈ cds, ∀ q ∈ cd.queries, EX b q = EY b q) →
      (cds.all fun cd => isTrue (cd.eval b (EX b))) = (cds.all fun cd => isTrue (cd.eval b (EY b))) := by
    intro cds b hcds
    induction cds with
    | nil => rfl
    | cons cd rest ihc =>
      simp only [List.all_cons]
      rw [Cond.eval_congr cd (fun _ _ _ => rfl) (hcds cd (by simp)),
        ihc (fun c' h' => hcds c' (by simp [h']))]
  have hcondQ : ∀ cd ∈ c.conds.map (·.1), ∀ q ∈ cd.queries, q ∈ c.subqueries := by
    intro cd hcd q hq
    obtain ⟨⟨cd', p⟩, hmem, rfl⟩ := List.mem_map.mp hcd
    unfold Core.subqueries
    exact List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_append_right _
      (List.mem_flatMap.mpr ⟨(cd', p), hmem, hq⟩)))
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
    (subsX := EX) (subsY := EY) (G := G) (Left := Left) (V := s.Within k o) hLeft c.items []
    (by simpa using hnodup) hVset
    (fun i hi => by
      simp only [rowsOf, Statement.itemRows]
      cases hkind : (s.item i).kind with
      | table t => exact htables i hi t (by simp [Statement.table?, hkind])
      | derived q v => exact hderived i hi q v hkind)
    (fun i hi b hb hw hbd => hall _ b (hsubsOn i hi b hb hw (by simpa using hbd)))
    (fun i hi hl b hb hw hbd => hleftX i hi hl b hb hw (by simpa using hbd))
    (fun i hi hl b hb hw hbd => hleftY i hi hl b hb hw (by simpa using hbd))
    [b0] [b0] (fun b hb => by
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
  have hres : Restricted G Left c.items b := of_decide_eq_true (List.mem_filter.mp hb).2
  have hw : s.Within k o b := joinItems_preserves c.items [b0]
    (fun b hb => by simp only [List.mem_singleton] at hb; subst hb; exact hV0) hVset b
    (List.mem_filter.mp hb).1
  have hbd := hres.boundIn hLeft
  rw [hR b hres.1 hw hbd, hall _ b (fun cd hcd q hq =>
    hsubs b hres.1 hw hbd q (hcondQ cd (hinner cd hcd) q hq))]

/-! A query's rows that matter. -/

/-- A core's row of a binding, as a core that calls no window function makes
it. -/
def Core.outRow (c : Core) (E : Bind → Nat → List Row) (b : Bind) : Row :=
  c.output.map fun o => match o with
    | .expr e => e.eval b (E b)
    | _ => none

/-- Whether a group of arm `k` of query `q` whose GROUP BY values are `key`
can make a row that `rel q` keeps: some row `rel q` keeps holds, in each
result column that is a bare column the GROUP BY lists, that column's value
in `key`. -/
noncomputable def keyMatters (s : Statement) (rel : Nat → Row → Bool) (q k : Nat)
    (key : List Value) : Bool :=
  decide (∃ r, rel q r = true ∧ ∀ (c n j c' : Nat), (s.core k).output[c]? = some (.expr (.col j c')) →
    (s.core k).groupBy[n]? = some (Expr.col j c') → key[n]? = some (r.cell c))

/-- A binding's values of the GROUP BY of arm `k`. -/
def Statement.groupKey (s : Statement) (k : Nat) (E : Bind → Nat → List Row) (b : Bind) :
    List Value :=
  (s.core k).groupBy.map fun e => e.eval b (E b)

/-- Whether a binding of arm `k` of query `q` matters to the rows of `q` that
`rel q` keeps: its row passes `rel q`; in an arm that aggregates, its group
can make a row `rel q` keeps, as its GROUP BY values say; and in an arm that
calls a window function, whose row depends on the arm's other rows, always. -/
noncomputable def relevant (s : Statement) (rel : Nat → Row → Bool) (q k : Nat)
    (E : Bind → Nat → List Row) (b : Bind) : Bool :=
  (s.core k).windowed ||
    if (s.core k).aggregated then keyMatters s rel q k (s.groupKey k E b)
    else rel q ((s.core k).outRow E b)

/-- A group of bindings that share their GROUP BY values `key`, of an arm
that aggregates, makes a row that `rel q` keeps only when `key` matters:
the row holds, in each result column that is a bare column the GROUP BY
lists, that column's value in the group's last binding. -/
theorem keyMatters_of_group {s : Statement} {rel : Nat → Row → Bool} {q k : Nat}
    {E : Bind → Nat → List Row} {g : List Bind} {key : List Value}
    (hkey : ∀ b ∈ g, s.groupKey k E b = key) (hne : g ≠ [] ∨ (s.core k).groupBy = [])
    (hrel : rel q ((s.core k).output.map (OutCol.aggregate E g)) = true) :
    keyMatters s rel q k key = true := by
  unfold keyMatters
  refine decide_eq_true ⟨_, hrel, fun c n j c' hc hn => ?_⟩
  have hgne : g ≠ [] := by
    rcases hne with hne | hne
    · exact hne
    · rw [hne] at hn; cases hn
  obtain ⟨bl, hbl⟩ := Option.ne_none_iff_exists'.mp (mt List.getLast?_eq_none_iff.mp hgne)
  have hmem := List.mem_of_getLast? hbl
  rw [← hkey bl hmem]
  simp [Statement.groupKey, List.getElem?_map, hn, Row.cell, hc, OutCol.aggregate, hbl, Expr.eval]

/-- An arm that aggregates makes the same rows that `rel q` keeps from the
bindings whose groups matter as from all its bindings. -/
theorem Core.result_filter_key {s : Statement} {rel : Nat → Row → Bool} {q k : Nat}
    (hagg : (s.core k).aggregated = true) (E : Bind → Nat → List Row) (kept : List Bind) :
    ((s.core k).result E kept).filter (rel q) =
      ((s.core k).result E (kept.filter fun b => keyMatters s rel q k (s.groupKey k E b))).filter
        (rel q) := by
  let c := s.core k
  let key := s.groupKey k E
  let C := keyMatters s rel q k
  let row (g : List Bind) : Row := c.output.map (OutCol.aggregate E g)
  suffices hrows : (c.rows E kept).filter (rel q) = (c.rows E (kept.filter (C ∘ key))).filter (rel q) by
    show (c.result E kept).filter (rel q) = (c.result E (kept.filter (C ∘ key))).filter (rel q)
    unfold Core.result
    split
    · rw [filter_eraseDups, filter_eraseDups, hrows]
    · exact hrows
  unfold Core.rows
  simp only [show c.aggregated = true from hagg, ↓reduceIte]
  cases hgb : c.groupBy.isEmpty with
  | true =>
    have hnil : c.groupBy = [] := List.isEmpty_iff.mp hgb
    have hkey : ∀ b, key b = [] := fun b => by simp [key, Statement.groupKey, c, hnil]
    simp only [↓reduceIte, List.map_cons, List.map_nil]
    cases hC : C [] with
    | true =>
      rw [show kept.filter (C ∘ key) = kept from
        List.filter_eq_self.mpr fun b _ => by simp only [Function.comp, hkey b, hC]]
    | false =>
      have hfail : ∀ g : List Bind, rel q (row g) = false := by
        intro g
        cases hr : rel q (row g) with
        | false => rfl
        | true =>
          have := keyMatters_of_group (g := g) (fun b _ => hkey b) (Or.inr hnil) hr
          exact absurd (show C [] = true from this) (by rw [hC]; simp)
      simp only [List.filter_cons, List.filter_nil]
      rw [show rel q (c.output.map (OutCol.aggregate E kept)) = false from hfail kept,
        show rel q (c.output.map (OutCol.aggregate E (kept.filter (C ∘ key)))) = false from hfail _]
      rfl
  | false =>
    simp only [Bool.false_eq_true, ↓reduceIte]
    have hgroups : ∀ l : List Bind, groupsOf (fun b => c.groupBy.map (Expr.eval b (E b))) l =
        ((l.map key).eraseDups).map fun kv => l.filter fun b => key b == kv := fun _ => rfl
    rw [hgroups, hgroups]
    -- The filtered bindings' keys are the keys that matter, each with the same group.
    have hkeys : ((kept.filter (C ∘ key)).map key).eraseDups = ((kept.map key).eraseDups).filter C := by
      rw [← List.filter_map, filter_eraseDups]
    have hsame : ∀ kv ∈ ((kept.map key).eraseDups).filter C,
        (kept.filter (C ∘ key)).filter (fun b => key b == kv) = kept.filter fun b => key b == kv := by
      intro kv hkv
      have hC : C kv = true := (List.mem_filter.mp hkv).2
      rw [List.filter_filter]
      apply List.filter_congr
      intro b _
      cases hb : key b == kv with
      | false => simp
      | true =>
        simp only [beq_iff_eq] at hb
        simp [Function.comp, hb, hC]
    rw [hkeys, List.map_congr_left hsame, List.map_map, List.map_map, List.filter_map,
      List.filter_map, List.filter_filter]
    congr 1
    apply List.filter_congr
    intro kv hkv
    simp only [Function.comp]
    cases hr : rel q (c.output.map (OutCol.aggregate E (kept.filter fun b => key b == kv))) with
    | false => simp
    | true =>
      have hm : kv ∈ kept.map key := List.mem_eraseDups.mp hkv
      obtain ⟨b0, hb0, hkb0⟩ := List.mem_map.mp hm
      have hgne : kept.filter (fun b => key b == kv) ≠ [] :=
        List.ne_nil_of_mem (List.mem_filter.mpr ⟨hb0, by simp [hkb0]⟩)
      have := keyMatters_of_group (rel := rel) (q := q) (g := kept.filter fun b => key b == kv)
        (fun b hb => by simpa using (List.mem_filter.mp hb).2) (Or.inl hgne) hr
      simp [show C kv = true from this]

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
  have hcases : c.windowed = true ∨ (c.windowed = false ∧ c.aggregated = true) ∨
      (c.aggregated = false ∧ c.windowed = false) := by
    cases c.windowed <;> cases c.aggregated <;> simp
  rcases hcases with hwin | ⟨hwin, hagg⟩ | hmany
  · have htrue : ∀ E b, relevant s rel q k E b = true := by
      intro E b
      unfold relevant
      rw [show (s.core k).windowed = true from hwin, Bool.true_or]
    have hX : keptX.filter (relevant s rel q k EX) = keptX := List.filter_eq_self.mpr fun b _ => htrue _ b
    have hY : keptY.filter (relevant s rel q k EY) = keptY := List.filter_eq_self.mpr fun b _ => htrue _ b
    rw [hX, hY] at hkept
    rw [hX] at hE
    subst hkept
    rw [Core.result_congr hE]
  · have hrel : ∀ E (kept : List Bind), kept.filter (relevant s rel q k E) =
        kept.filter fun b => keyMatters s rel q k (s.groupKey k E b) := by
      intro E kept
      apply List.filter_congr
      intro b _
      simp [relevant, c, hwin, hagg]
    rw [hrel, hrel] at hkept
    rw [hrel] at hE
    show (c.result EX keptX).filter (rel q) = (c.result EY keptY).filter (rel q)
    rw [Core.result_filter_key hagg, Core.result_filter_key (E := EY) hagg, Core.result_congr hE,
      hkept]
  · have hres : ∀ E (kept : List Bind), (c.result E kept).filter (rel q) =
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

/-! A subquery's window. -/

/-- What makes a query's rows the same from two runs that keep the same
bindings that pass `win k`: its one core makes a row of each binding, it
keeps its first `n` rows in an ORDER BY whose values hold no subquery, and
under each outer binding whose rows pass their tests, whether a binding
passes `win k` is a test `P` of its ORDER BY values closed downward, and
either every binding of both runs passes it, or at least `n` of the second
run's do (`Window`). -/
structure WindowRows (s : Statement) (X Y : Db) (G : Nat → Row → Bool)
    (win : Nat → Bind → Bool) (q k : Nat) : Prop where
  arms : (s.query q).arms = [k]
  agg : (s.core k).aggregated = false
  windowed : (s.core k).windowed = false
  distinct : (s.core k).distinct = false
  offset : (s.query q).offset = none
  order : (s.query q).orderBy ≠ []
  keys : ∀ (E E' : Bind → Nat → List Row) b,
    Limit.orderKey (s.query q) ((s.core k).outRow E b) =
      Limit.orderKey (s.query q) ((s.core k).outRow E' b)
  limit : ∃ n, (s.query q).limit = some n ∧
    ∀ o, s.Outer q o → Passes G o → ∀ EX EY : Bind → Nat → List Row, ∃ P : List Value → Bool,
      (∀ a b, a.length = (s.query q).orderBy.length → b.length = (s.query q).orderBy.length →
        keyLe ((s.query q).orderBy.map (·.2)) a b = true → P b = true → P a = true) ∧
      (∀ b ∈ coreBindings s X EX o k,
        win k b = P (Limit.orderKey (s.query q) ((s.core k).outRow EX b))) ∧
      (∀ b ∈ coreBindings s Y EY o k,
        win k b = P (Limit.orderKey (s.query q) ((s.core k).outRow EY b))) ∧
      (((∀ b ∈ coreBindings s X EX o k, win k b = true) ∧
          ∀ b ∈ coreBindings s Y EY o k, win k b = true) ∨
        n ≤ ((coreBindings s Y EY o k).filter (win k)).length)

/-- A query whose rows are a window's returns the same rows from two runs
that keep the same bindings that pass `win k` and read the same subquery
rows under them. -/
theorem window_queryRows {s : Statement} {X Y : Db} {G : Nat → Row → Bool}
    {win : Nat → Bind → Bool} {q k : Nat} (hw : WindowRows s X Y G win q k) {o : Bind}
    (hout : s.Outer q o) (ho : Passes G o) {EX EY : Bind → Nat → List Row}
    (hkeep : (coreBindings s X EX o k).filter (win k) = (coreBindings s Y EY o k).filter (win k))
    (hE : ∀ b ∈ (coreBindings s X EX o k).filter (win k), ∀ q' ∈ (s.core k).subqueries,
      EX b q' = EY b q') :
    queryRows s X EX o q = queryRows s Y EY o q := by
  obtain ⟨n, hlim, hP⟩ := hw.limit
  obtain ⟨P, hdown, hPX, hPY, hcount⟩ := hP o hout ho EX EY
  let c := s.core k
  let query := s.query q
  let ds := query.orderBy.map (·.2)
  let le (a b : Row) : Bool := keyLe ds (Limit.orderKey query a) (Limit.orderKey query b)
  have hklen : ∀ r, (Limit.orderKey query r).length = ds.length := by
    intro r; simp [Limit.orderKey, ds]
  have hne : query.orderBy.isEmpty = false := by simpa using hw.order
  have hpost : ∀ rows : List Row, query.post rows = (sortBy le rows).take n := by
    intro rows
    simp only [Query.post, query, hne, Bool.false_eq_true, ↓reduceIte, hw.offset, Option.getD_none,
      List.drop_zero, hlim]
    rfl
  have hres : ∀ (E : Bind → Nat → List Row) (kept : List Bind),
      c.result E kept = kept.map (c.outRow E) := by
    intro E kept
    unfold Core.result
    simp only [c, hw.distinct, Bool.false_eq_true, ↓reduceIte]
    exact Core.rows_plain hw.agg hw.windowed E kept
  have hrows : ∀ (Z : Db) (E : Bind → Nat → List Row),
      queryRows s Z E o q = (sortBy le ((coreBindings s Z E o k).map (c.outRow E))).take n := by
    intro Z E
    unfold queryRows
    simp only [hw.arms, List.map_cons, List.map_nil, combineAll_single]
    rw [hres, hpost]
  rw [hrows, hrows]
  have hout_eq : ∀ b ∈ (coreBindings s X EX o k).filter (win k), c.outRow EX b = c.outRow EY b := by
    intro b hb
    have := Core.result_congr (c := c) (kept := [b]) (fun b' hb' q' hq' => by
      obtain rfl := List.mem_singleton.mp hb'
      exact hE _ hb q' hq')
    rw [hres, hres] at this
    simpa using this
  rcases hcount with ⟨hallX, hallY⟩ | hn
  · have hX : (coreBindings s X EX o k).filter (win k) = coreBindings s X EX o k :=
      List.filter_eq_self.mpr hallX
    have hY : (coreBindings s Y EY o k).filter (win k) = coreBindings s Y EY o k :=
      List.filter_eq_self.mpr hallY
    rw [hX, hY] at hkeep
    rw [hX] at hout_eq
    rw [List.map_congr_left hout_eq, hkeep]
  · let P' : Row → Bool := fun r => P (Limit.orderKey query r)
    have hfX : ((coreBindings s X EX o k).map (c.outRow EX)).filter P' =
        ((coreBindings s X EX o k).filter (win k)).map (c.outRow EX) := by
      rw [List.filter_map]
      congr 1
      apply List.filter_congr
      intro b hb
      show P (Limit.orderKey query (c.outRow EX b)) = win k b
      rw [hPX b hb]
    have hfY : ((coreBindings s Y EY o k).map (c.outRow EY)).filter P' =
        ((coreBindings s Y EY o k).filter (win k)).map (c.outRow EY) := by
      rw [List.filter_map]
      congr 1
      apply List.filter_congr
      intro b hb
      show P (Limit.orderKey query (c.outRow EY b)) = win k b
      rw [hPY b hb]
    have heq : ((coreBindings s X EX o k).map (c.outRow EX)).filter P' =
        ((coreBindings s Y EY o k).map (c.outRow EY)).filter P' := by
      rw [hfX, hfY, List.map_congr_left hout_eq, hkeep]
    apply Limit.take_sortBy_window (le := le) (P := P')
    · intro a b; exact Limit.keyLe_total _ _ _
    · intro a b c' h1 h2; exact Limit.keyLe_trans (hklen a) (hklen b) (hklen c') h1 h2
    · intro a b hab hb
      exact hdown _ _ (by simpa [ds] using hklen a) (by simpa [ds] using hklen b) hab hb
    · exact heq
    · rw [hfY, List.length_map]; exact hn

/-! Every fuel. -/

/-- What the restriction needs of the statement and the tests, beside the
promise: a well-formed statement, tables that hold the same rows that pass
the test of each item a core lists, a derived item's query whose rows that
matter include those that pass the test of each item a core lists of it,
subqueries all of whose rows matter, a LIMIT or an OFFSET whose query's
rows all matter, and a query whose bindings `win` doesn't all pass, whose
rows are a window's. -/
structure Restriction (s : Statement) (X Y : Db) (G : Nat → Row → Bool) (rel : Nat → Row → Bool)
    (win : Nat → Bind → Bool) : Prop where
  wf : s.wellFormed = true
  tables : ∀ k, ∀ i ∈ (s.core k).items, ∀ t, s.table? i = some t →
    (X.rows t).filter (G i) = (Y.rows t).filter (G i)
  derived : ∀ k, ∀ i ∈ (s.core k).items, ∀ q v, (s.item i).kind = .derived q v →
    ∀ r, G i r = true → rel q r = true
  subs : ∀ q, ∀ k ∈ (s.query q).arms, ∀ q' ∈ (s.core k).subqueries, ∀ r, rel q' r = true
  post : ∀ q, q ≠ s.root →
    ((s.query q).limit.isSome || (s.query q).offset.isSome) = true → ∀ r, rel q r = true
  windows : ∀ q, q ≠ s.root → ∀ k ∈ (s.query q).arms, ∀ b, win k b = false →
    WindowRows s X Y G win q k

/-- At fuel `f`, two databases' runs of each query below `f` but the
statement's own return the same rows that `rel` keeps, under any of its
outer bindings whose rows pass their tests. -/
def Agree (s : Statement) (X Y : Db) (G : Nat → Row → Bool) (rel : Nat → Row → Bool)
    (f : Nat) : Prop :=
  ∀ q, q < f → q ≠ s.root → ∀ o, s.Outer q o → Passes G o →
    (evalQuery s X f o q).filter (rel q) = (evalQuery s Y f o q).filter (rel q)

/-- What the planner's scopes promise of a run on `Z` of query `q` under the
outer binding `o`, with `E` running the queries its arms read: every binding
of an arm that its core keeps, and that matters by `R`, holds rows that pass
their tests, and no row only of an item `Left` allows; and an item `Left`
allows has no row its ON holds for, in a binding of the core whose rows pass
their tests and that holds a row of each item before it that no LEFT JOIN
adds, that fails its test. -/
def ArmsKept (s : Statement) (Z : Db) (G : Nat → Row → Bool) (Left : Nat → Prop)
    (R : Nat → (Bind → Nat → List Row) → Bind → Bool) (q : Nat) (o : Bind)
    (E : Bind → Nat → List Row) : Prop :=
  ∀ k ∈ (s.query q).arms,
    (∀ b ∈ coreBindings s Z E o k, R k E b = true → Restricted G Left (s.core k).items b) ∧
    ∀ i ∈ (s.core k).items, Left i → ∀ b, Passes G b → s.Within k o b →
      s.BoundIn ((s.core k).items.takeWhile (· ≠ i)) b → ∀ r ∈ s.itemRows Z E (s.outerOf k o) i,
        onHolds (s.core k) E i (b.set i (some r)) = true → G i r = true

/-- The promise holds for a test of the bindings that matter that passes
fewer of them. -/
theorem ArmsKept.mono {s : Statement} {Z : Db} {G : Nat → Row → Bool} {Left : Nat → Prop}
    {R R' : Nat → (Bind → Nat → List Row) → Bind → Bool} {q : Nat} {o : Bind}
    {E : Bind → Nat → List Row} (h : ArmsKept s Z G Left R q o E)
    (himp : ∀ k b, R' k E b = true → R k E b = true) : ArmsKept s Z G Left R' q o E :=
  fun k hk => ⟨fun b hb hR => (h k hk).1 b hb (himp k b hR), (h k hk).2⟩

/-- The promise for every query up to `f` but the statement's own, run on `Z`
at fuel `f` under any of its outer bindings whose rows pass their tests,
where a binding matters when its row passes `rel` and it passes `win`. -/
def Kept (s : Statement) (Z : Db) (G : Nat → Row → Bool) (Left : Nat → Prop)
    (rel : Nat → Row → Bool) (win : Nat → Bind → Bool) (f : Nat) : Prop :=
  ∀ q, q ≤ f → q ≠ s.root → ∀ o, s.Outer q o → Passes G o →
    ArmsKept s Z G Left (fun k E b => relevant s rel q k E b && win k b) q o (evalQuery s Z f)

/-- Whether a binding matters depends, of the runs of its core's subqueries,
only on the rows they return under it. -/
theorem relevant_congr {s : Statement} {rel : Nat → Row → Bool} {q k : Nat}
    {EX EY : Bind → Nat → List Row} {b : Bind}
    (hsubs : ∀ q' ∈ (s.core k).subqueries, EX b q' = EY b q') :
    relevant s rel q k EX b = relevant s rel q k EY b := by
  unfold relevant
  congr 1
  split
  · congr 1
    unfold Statement.groupKey
    apply List.map_congr_left
    intro e he
    apply e.eval_congr
    · intro _ _ _; rfl
    · intro q' hq'
      apply hsubs q'
      unfold Core.subqueries
      exact List.mem_eraseDups.mpr (List.mem_append_right _ (List.mem_flatMap.mpr ⟨e, he, hq'⟩))
  · congr 1
    apply List.map_congr_left
    intro o' ho'
    cases o' with
    | expr e =>
      apply e.eval_congr
      · intro _ _ _; rfl
      · intro q' hq'
        apply hsubs q'
        unfold Core.subqueries
        exact List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_append_left _
          (List.mem_flatMap.mpr ⟨_, ho', List.mem_flatMap.mpr ⟨e, by simp [OutCol.exprs], hq'⟩⟩)))
    | _ => rfl

/-- Two runs at fuel `f` whose arms of query `q` keep the promise, with any
test `R` of the bindings that matter that depends, of the runs of a core's
subqueries, only on the rows they return under the binding, keep the same
bindings that matter and read the same subquery rows under them. -/
theorem arms_agree {s : Statement} {X Y : Db} {G : Nat → Row → Bool} {Left : Nat → Prop}
    {rel : Nat → Row → Bool} {win : Nat → Bind → Bool}
    {R : Nat → (Bind → Nat → List Row) → Bind → Bool} (h : Restriction s X Y G rel win)
    (hLeft : ∀ j, Left j → s.alwaysBound j = false) {f q : Nat} (hq : q ≤ f) {o : Bind} (hout : s.Outer q o) (ho : Passes G o)
    (hagree : Agree s X Y G rel f)
    (hR : ∀ k b, (∀ q' ∈ (s.core k).subqueries, evalQuery s X f b q' = evalQuery s Y f b q') →
      R k (evalQuery s X f) b = R k (evalQuery s Y f) b)
    (hX : ArmsKept s X G Left R q o (evalQuery s X f))
    (hY : ArmsKept s Y G Left R q o (evalQuery s Y f)) :
    ∀ k ∈ (s.query q).arms,
      (coreBindings s X (evalQuery s X f) o k).filter (R k (evalQuery s X f)) =
        (coreBindings s Y (evalQuery s Y f) o k).filter (R k (evalQuery s Y f)) ∧
      ∀ b ∈ (coreBindings s X (evalQuery s X f) o k).filter (R k (evalQuery s X f)),
        ∀ q' ∈ (s.core k).subqueries, evalQuery s X f b q' = evalQuery s Y f b q' := by
  intro k hk
  -- A subquery returns the same rows under a binding its outer bindings hold.
  have hsub : ∀ b, Passes G b → ∀ q' ∈ (s.core k).subqueries, s.Outer q' b →
      evalQuery s X f b q' = evalQuery s Y f b q' := by
    intro b hb q' hq' hout'
    have hmem : q' ∈ s.nestedQueries k := List.mem_append_right _ hq'
    have hne : q' ≠ s.root := fun e => s.wellFormed_root h.wf k (e ▸ hmem)
    have := hagree q' (Nat.lt_of_lt_of_le (s.wellFormed_nested h.wf (s.query_lt hk) hk hmem) hq) hne b hout' hb
    rwa [List.filter_eq_self.mpr (fun r _ => h.subs q k hk q' hq' r),
      List.filter_eq_self.mpr (fun r _ => h.subs q k hk q' hq' r)] at this
  have hsubs : ∀ b, Passes G b → s.Within k o b → s.BoundIn (s.core k).items b →
      ∀ q' ∈ (s.core k).subqueries, evalQuery s X f b q' = evalQuery s Y f b q' :=
    fun b hb hw hbd q' hq' => hsub b hb q' hq' (s.outer_sub h.wf hk hout hw hbd hq')
  have hb0 : Passes G (s.outerOf k o) := by
    unfold Statement.outerOf
    split
    · exact Passes.empty G
    · exact ho
  have hkeep := coreBindings_restricted (k := k) (o := o) hLeft ((s.wellFormed_parts h.wf).nodup k ((s.wellFormed_parts h.wf).arms q (s.query_lt hk) k hk).1) ho
    (fun i hi t ht => h.tables k i hi t ht)
    (fun i hi q' v hkind => by
      have hmem : q' ∈ s.nestedQueries k :=
        List.mem_append_left _ (List.mem_filterMap.mpr ⟨i, hi, by simp [hkind]⟩)
      have hne : q' ≠ s.root := fun e => s.wellFormed_root h.wf k (e ▸ hmem)
      have := congrArg (List.filter (G i)) (hagree q' (Nat.lt_of_lt_of_le
        (s.wellFormed_nested h.wf (s.query_lt hk) hk hmem) hq) hne _ (s.outer_derived h.wf hk hout hi hkind) hb0)
      rwa [filter_filter_of_imp (fun r _ hr => h.derived k i hi q' v hkind r hr),
        filter_filter_of_imp (fun r _ hr => h.derived k i hi q' v hkind r hr)] at this)
    (fun i hi b hb hw hbd cd hcd q' hq' => hsub b hb q' (Core.onOf_queries hcd hq')
      (s.outer_subOn h.wf hk hout hw hbd hcd hq'))
    hsubs ((hX k hk).2) ((hY k hk).2)
    (RX := R k (evalQuery s X f)) (RY := R k (evalQuery s Y f))
    (fun b hb hw hbd => hR k b (hsubs b hb hw hbd))
    ((hX k hk).1) ((hY k hk).1)
  have hres : ∀ b ∈ (coreBindings s X (evalQuery s X f) o k).filter
      (R k (evalQuery s X f)), Restricted G Left (s.core k).items b := by
    intro b hb
    have hb' := List.mem_filter.mp hb
    exact (hX k hk).1 b hb'.1 hb'.2
  refine ⟨hkeep, fun b hb q' hq' => hsubs b (hres b hb).1 ?_ ((hres b hb).boundIn hLeft) q' hq'⟩
  · intro j hj
    exact joinItems_preserves (P := s.Within k o) (s.core k).items [s.outerOf k o]
      (fun b hb => by simp only [List.mem_singleton] at hb; subst hb; exact fun _ _ => rfl)
      (fun b j' x hb hj' j'' hj'' => by
        have hne : j'' ≠ j' := fun e => hj'' (e ▸ hj')
        simp only [Bind.set, hne, ↓reduceIte]
        exact hb j'' hj'')
      b (List.mem_filter.mp (List.mem_filter.mp hb).1).1 j hj

/-- When the planner's scopes keep their promise at each fuel, given that two
runs return the same rows that matter at every lower fuel, the runs return the
same rows that matter of every query but the statement's own, at every fuel
past the query's number. -/
theorem agree_of_kept {s : Statement} {X Y : Db} {G : Nat → Row → Bool} {Left : Nat → Prop}
    {rel : Nat → Row → Bool} {win : Nat → Bind → Bool} (h : Restriction s X Y G rel win)
    (hLeft : ∀ j, Left j → s.alwaysBound j = false)
    (hkept : ∀ f, (∀ f' ≤ f, Agree s X Y G rel f') →
      Kept s X G Left rel win f ∧ Kept s Y G Left rel win f) :
    ∀ f, Agree s X Y G rel f := by
  have key : ∀ f, ∀ f' ≤ f, Agree s X Y G rel f' := by
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
        have hq' := Nat.le_of_lt_succ hq
        by_cases hall : ∀ k ∈ (s.query q).arms, ∀ b, win k b = true
        · have hmono : ∀ (Z : Db), ArmsKept s Z G Left
              (fun k E b => relevant s rel q k E b && win k b) q o (evalQuery s Z f) →
              ArmsKept s Z G Left (relevant s rel q) q o (evalQuery s Z f) := by
            intro Z hZ k hk
            refine ⟨fun b hb hR => (hZ k hk).1 b hb ?_, (hZ k hk).2⟩
            simp [hR, hall k hk b]
          exact queryRows_restricted (h.post q hne) (arms_agree h hLeft hq' hout ho
            (ih f (Nat.le_refl _)) (fun _ _ hs => relevant_congr hs)
            (hmono X (hX q hq' hne o hout ho)) (hmono Y (hY q hq' hne o hout ho)))
        · -- A query whose bindings `win` doesn't all pass is a window's.
          obtain ⟨k, hk, b0, hb0⟩ : ∃ k ∈ (s.query q).arms, ∃ b, win k b ≠ true :=
            Classical.byContradiction fun hne' => hall fun k hk b =>
              Classical.byContradiction fun hb => hne' ⟨k, hk, b, hb⟩
          have hw := h.windows q hne k hk b0 (by simpa using hb0)
          obtain ⟨n, hlim, _⟩ := hw.limit
          have hcut : ((s.query q).limit.isSome || (s.query q).offset.isSome) = true := by
            simp [hlim]
          have hrel : ∀ r, rel q r = true := h.post q hne hcut
          have hmono : ∀ (Z : Db), ArmsKept s Z G Left
              (fun k E b => relevant s rel q k E b && win k b) q o (evalQuery s Z f) →
              ArmsKept s Z G Left (fun k _ b => win k b) q o (evalQuery s Z f) := by
            intro Z hZ k' hk'
            rw [hw.arms, List.mem_singleton] at hk'
            subst hk'
            refine ⟨fun b hb hR => (hZ k' (by rw [hw.arms]; simp)).1 b hb ?_,
              (hZ k' (by rw [hw.arms]; simp)).2⟩
            simp [relevant, hw.windowed, hw.agg, hrel, hR]
          have harms := arms_agree (R := fun k _ b => win k b) h hLeft hq' hout ho
            (ih f (Nat.le_refl _)) (fun _ _ _ => rfl)
            (hmono X (hX q hq' hne o hout ho)) (hmono Y (hY q hq' hne o hout ho)) k
            (by rw [hw.arms]; simp)
          have hid : ∀ l : List Row, l.filter (rel q) = l :=
            fun l => List.filter_eq_self.mpr fun r _ => hrel r
          rw [hid, hid]
          exact window_queryRows hw hout ho harms.1 harms.2
  exact fun f => key f f (Nat.le_refl _)

/-- The statement's rows from two databases, when every row of its own
query matters and the planner's scopes keep their promise for it and for
every other query: with no LIMIT bound, every binding of the statement's own
core matters. -/
theorem result_restricted {s : Statement} {X Y : Db} {G : Nat → Row → Bool} {Left : Nat → Prop}
    {rel : Nat → Row → Bool} {win : Nat → Bind → Bool} (h : Restriction s X Y G rel win)
    (hLeft : ∀ j, Left j → s.alwaysBound j = false)
    (hall : ∀ r, rel s.root r = true)
    (hkept : ∀ f, (∀ f' ≤ f, Agree s X Y G rel f') →
      Kept s X G Left rel win f ∧ Kept s Y G Left rel win f)
    (hrootX : ArmsKept s X G Left (relevant s rel s.root) s.root Bind.empty
      (evalQuery s X s.queries.length))
    (hrootY : ArmsKept s Y G Left (relevant s rel s.root) s.root Bind.empty
      (evalQuery s Y s.queries.length)) :
    s.result X = s.result Y := by
  have hagree := agree_of_kept h hLeft hkept
  unfold Statement.result
  simp only [evalQuery]
  have := queryRows_restricted (fun _ => hall) (arms_agree h hLeft (Nat.le_of_lt (s.wellFormed_parts h.wf).root) (s.outer_root h.wf)
    (Passes.empty G) (hagree _) (fun _ _ hs => relevant_congr hs) hrootX hrootY)
  rwa [List.filter_eq_self.mpr (fun r _ => hall r), List.filter_eq_self.mpr (fun r _ => hall r)] at this

/-- A statement whose own query is one SELECT with an ORDER BY, a LIMIT and no
OFFSET, whose run on `X` returned as many rows as its LIMIT, the last with
ORDER BY values `last`, returns the same rows from `Y`, when the planner's
scopes keep their promise for every other query and for the bindings of its own
core whose rows sort at or before `last`. -/
theorem limit_restricted {s : Statement} {X Y : Db} {G : Nat → Row → Bool} {Left : Nat → Prop}
    {rel : Nat → Row → Bool} {win : Nat → Bind → Bool} (h : Restriction s X Y G rel win)
    (hLeft : ∀ j, Left j → s.alwaysBound j = false)
    {n : Nat} (hlimit : (s.query s.root).limit = some n) (hoffset : (s.query s.root).offset = none)
    (horder : (s.query s.root).orderBy ≠ []) {last : List Value}
    (hlen : (s.result X).length = n)
    (hlast : (s.result X).getLast?.map (Limit.orderKey (s.query s.root)) = some last)
    (hkept : ∀ f, (∀ f' ≤ f, Agree s X Y G rel f') →
      Kept s X G Left rel win f ∧ Kept s Y G Left rel win f)
    (hrootX : ArmsKept s X G Left (relevant s (fun _ r => keyLe ((s.query s.root).orderBy.map (·.2))
      (Limit.orderKey (s.query s.root) r) last) s.root) s.root Bind.empty
      (evalQuery s X s.queries.length))
    (hrootY : ArmsKept s Y G Left (relevant s (fun _ r => keyLe ((s.query s.root).orderBy.map (·.2))
      (Limit.orderKey (s.query s.root) r) last) s.root) s.root Bind.empty
      (evalQuery s Y s.queries.length)) :
    s.result Y = s.result X := by
  let P : Row → Bool := fun r =>
    keyLe ((s.query s.root).orderBy.map (·.2)) (Limit.orderKey (s.query s.root) r) last
  have hagree := agree_of_kept h hLeft hkept
  have harms := arms_agree (R := relevant s (fun _ => P) s.root) h hLeft (Nat.le_of_lt (s.wellFormed_parts h.wf).root) (s.outer_root h.wf) (Passes.empty G)
    (hagree _) (fun _ _ hs => relevant_congr hs) hrootX hrootY
  have hcombine : ∀ (Z : Db), (combineAll (s.query s.root).ops ((s.query s.root).arms.map fun k =>
      (s.core k).result (evalQuery s Z s.queries.length)
        (coreBindings s Z (evalQuery s Z s.queries.length) Bind.empty k))).filter P =
      combineAll (s.query s.root).ops ((s.query s.root).arms.map fun k =>
        ((s.core k).result (evalQuery s Z s.queries.length)
          (coreBindings s Z (evalQuery s Z s.queries.length) Bind.empty k)).filter P) := by
    intro Z
    rw [combineAll_filter, List.map_map]
    rfl
  unfold Statement.result at hlen hlast ⊢
  simp only [evalQuery, queryRows] at hlen hlast ⊢
  apply Limit.limit_sound hlimit hoffset horder hlen hlast
  rw [hcombine X, hcombine Y]
  congr 1
  apply List.map_congr_left
  intro k hk
  exact arm_restricted (harms k hk).1 (harms k hk).2

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
