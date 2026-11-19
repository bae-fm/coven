import SqliveNarrowing.Restrict

/-! A subquery's window restricts its rows. A window's subquery reads one
table item, and its conditions keep, under a binding around it, the item's
rows that its own conditions pass and that hold that binding's values in
the partition columns (`window_kept`). Those are the rows of the window that
a row the core keeps sits in, so the window's scope passes such a row
exactly when fewer than LIMIT rows the core keeps sort before it
(`window_passes`). Whether a binding passes is then a test of its ORDER BY
values closed downward, and either every binding passes, when the database
the write leaves holds fewer than LIMIT rows the core keeps, or the first
LIMIT of them do (`plan_windowRows`): what `window_queryRows` takes. -/
namespace SqliveNarrowing

open Window

attribute [local instance] Classical.propDecidable

/-- Whether binding `b` of core `k` passes the window of each of its items
that a window sorts, with lookups in `Y`. -/
def Planned.windowPasses (p : Planned) (s : Statement) (Y : Db) (k : Nat) (b : Bind) : Bool :=
  (s.core k).items.all fun j =>
    match b j with
    | some r => (p.windowScope j).passes (lookupRows s Y) r
    | none => true

theorem plan_windows (s : Statement) : (plan s).windows = Window.windows s (plan s).state := rfl

/-- A window of the planned state is the one `read` gives its query. -/
theorem plan_window_read {s : Statement} {w : Window} (hw : w ∈ (plan s).windows) :
    read s (plan s).state w.query = some w ∧ ReadFacts s (plan s).state w := by
  rw [plan_windows] at hw
  obtain ⟨q, _, hq⟩ := List.mem_filterMap.mp hw
  obtain ⟨rfl, hf⟩ := read_facts hq
  exact ⟨hq, hf⟩

/-- Two windows of the planned state that sort one item are one window. -/
theorem plan_window_unique {s : Statement} (hwf : s.wellFormed = true) {w w' : Window}
    (hw : w ∈ (plan s).windows) (hw' : w' ∈ (plan s).windows) (hi : w.item = w'.item) : w = w' := by
  obtain ⟨hr, hf⟩ := plan_window_read hw
  obtain ⟨hr', hf'⟩ := plan_window_read hw'
  have hk : (s.item w.item).core = w.core := by
    have hmem : w.item ∈ (s.core w.core).items := by rw [hf.items]; simp
    exact ((s.wellFormed_parts hwf).items _ (s.core_lt hmem) _ hmem).2
  have hk' : (s.item w'.item).core = w'.core := by
    have hmem : w'.item ∈ (s.core w'.core).items := by rw [hf'.items]; simp
    exact ((s.wellFormed_parts hwf).items _ (s.core_lt hmem) _ hmem).2
  have hcore : w.core = w'.core := by rw [← hk, ← hk', hi]
  have hq : w.query = w'.query :=
    s.wellFormed_armOf hwf (k := w.core) (by rw [hf.arms]; simp) (by rw [hcore, hf'.arms]; simp)
  rw [hq] at hr
  exact Option.some.inj (hr.symm.trans hr')

/-- The scope a window gives its item. -/
theorem plan_windowScope {s : Statement} (hwf : s.wellFormed = true) {w : Window}
    (hw : w ∈ (plan s).windows) : (plan s).windowScope w.item = w.scope := by
  unfold Planned.windowScope
  cases hfind : (plan s).windows.find? (·.item == w.item) with
  | none => exact absurd (List.find?_eq_none.mp hfind w hw) (by simp)
  | some w' =>
    have hw' := List.mem_of_find?_eq_some hfind
    have hi : w'.item = w.item := by simpa using List.find?_some hfind
    rw [plan_window_unique hwf hw' hw hi]

/-- A binding that fails its windows is a binding of a window's core. -/
theorem windowPasses_false {s : Statement} {Y : Db} {k : Nat} {b : Bind}
    (h : (plan s).windowPasses s Y k b = false) :
    ∃ w ∈ (plan s).windows, w.item ∈ (s.core k).items := by
  unfold Planned.windowPasses at h
  obtain ⟨j, hj, hfail⟩ := List.all_eq_false.mp h
  cases hb : b j with
  | none => simp [hb] at hfail
  | some r =>
    simp only [hb] at hfail
    unfold Planned.windowScope at hfail
    cases hfind : (plan s).windows.find? (·.item == j) with
    | none => simp [hfind, RScope.passes] at hfail
    | some w =>
      have hi : w.item = j := by simpa using List.find?_some hfind
      exact ⟨w, List.mem_of_find?_eq_some hfind, hi ▸ hj⟩

/-! The window's ORDER BY. -/

theorem keyAt_facts {s : Statement} {c : Core} {i pos : Nat} {e : Expr}
    (h : keyAt s c i pos = some e) :
    c.output[pos]? = some (.expr e) ∧ e.queries = [] ∧ ∀ j ∈ e.items, j = i := by
  unfold keyAt at h
  split at h
  · rename_i e' he
    cases hk : Limit.sortKey s i e' with
    | none => simp [hk] at h
    | some _ =>
      simp only [hk, Option.map_some, Option.some.injEq] at h
      subst h
      unfold Limit.sortKey at hk
      split at hk
      · rename_i hcheck
        simp only [Bool.and_eq_true, List.isEmpty_iff, Bool.not_eq_true', List.isEmpty_eq_false_iff,
          List.all_eq_true] at hcheck
        refine ⟨he, hcheck.1.1, fun j hj => ?_⟩
        simp only [Expr.items, List.mem_map] at hj
        obtain ⟨⟨j', c'⟩, hmem, rfl⟩ := hj
        have := hcheck.2 (j', c') hmem
        simp only [beq_iff_eq] at this
        exact this.1
      · cases hk
  · cases h

/-- The keys `keyAt` reads of the ORDER BY terms `L` give each binding of
core `c` the values those terms give it, in the same directions. -/
theorem keyAt_map {s : Statement} {c : Core} {i : Nat} (E : Bind → Nat → List Row) (b : Bind) :
    ∀ L : List (Nat × Bool), (∀ p ∈ L, (keyAt s c i p.1).isSome = true) →
      L.map (fun (p, _) => (c.outRow E b).cell p) =
        (L.filterMap fun (pos, d) => (keyAt s c i pos).map (·, d)).map
          (fun (e, _) => e.eval (rowBind (b i)) noSubs) ∧
      (L.filterMap fun (pos, d) => (keyAt s c i pos).map (·, d)).map (·.2) = L.map (·.2)
  | [], _ => ⟨rfl, rfl⟩
  | (pos, d) :: rest, h => by
    obtain ⟨e, he⟩ := Option.isSome_iff_exists.mp (h (pos, d) (by simp))
    obtain ⟨ho, hq, hitems⟩ := keyAt_facts he
    obtain ⟨ih1, ih2⟩ := keyAt_map E b rest (fun p hp => h p (by simp [hp]))
    simp only [List.filterMap_cons, he, Option.map_some, List.map_cons] at ih1 ⊢
    refine ⟨?_, by rw [ih2]⟩
    rw [ih1]
    congr 1
    simp only [Core.outRow, Row.cell, List.getElem?_map, ho, Option.map_some, Option.join_some]
    rw [e.eval_rowBind hitems hq]

/-- The window's keys sort a binding of its core as the subquery's ORDER BY
does, and in the same directions. -/
theorem window_keys {s : Statement} {st : PState} {w : Window} (hf : ReadFacts s st w)
    (E : Bind → Nat → List Row) (b : Bind) :
    Limit.orderKey (s.query w.query) ((s.core w.core).outRow E b) =
        w.keys.map (fun (e, _) => e.eval (rowBind (b w.item)) noSubs) ∧
      w.keys.map (·.2) = (s.query w.query).orderBy.map (·.2) := by
  rw [hf.keys]
  exact keyAt_map E b _ hf.keyed

/-! The rows the window's core keeps. -/

/-- Whether row `r` of the window's item is one its core keeps under the
binding `o` around it. -/
def windowKept (s : Statement) (w : Window) (o : Bind) (r : Row) : Bool :=
  (s.core w.core).conds.all fun (cd, _) => isTrue (cd.eval (o.set w.item (some r)) noSubs)

/-- A window's core keeps under `o` the bindings of its item's rows it keeps. -/
theorem window_bindings {s : Statement} {st : PState} {w : Window} (hf : ReadFacts s st w)
    {t : Nat} (ht : s.table? w.item = some t) (Z : Db) (E : Bind → Nat → List Row) (o : Bind) :
    coreBindings s Z E o w.core =
      ((Z.rows t).filter (windowKept s w o)).map fun r => o.set w.item (some r) := by
  obtain ⟨p, hp, _⟩ := hf.parent
  have hkind : (s.item w.item).kind = .table t := by
    unfold Statement.table? at ht
    split at ht
    · cases ht; assumption
    · cases ht
  have hon : (s.core w.core).onOf w.item = [] := by
    unfold Core.onOf
    apply List.filterMap_eq_nil_iff.mpr
    rintro ⟨cd, pl⟩ hx
    have := (hf.conds _ hx).1
    simp only at this
    subst this
    simp
  have hinner : (s.core w.core).innerConds = (s.core w.core).conds.map (·.1) := by
    unfold Core.innerConds
    have key : ∀ l : List (Cond × Place), (∀ x ∈ l, x.2 = .inner) →
        l.filterMap (fun (cd, p) => if p == .inner then some cd else none) = l.map (·.1) := by
      intro l hl
      induction l with
      | nil => rfl
      | cons x xs ih =>
        obtain ⟨cd, pl⟩ := x
        have := hl (cd, pl) List.mem_cons_self
        simp only at this
        subst this
        rw [List.filterMap_cons, List.map_cons, ← ih (fun y hy => hl y (List.mem_cons_of_mem _ hy))]
        rfl
    exact key _ fun x hx => (hf.conds x hx).1
  have hnoq : ∀ x ∈ (s.core w.core).conds, x.1.queries = [] := by
    intro x hx
    rcases (hf.conds x hx).2 with hown | hpart
    · simp only [ownCond, Bool.and_eq_true, List.isEmpty_iff] at hown
      exact hown.1.1
    · obtain ⟨c, hc⟩ := Option.isSome_iff_exists.mp hpart
      exact (partitionOf_holds hc).choose_spec.2.2.1
  unfold coreBindings
  simp only [hp, Option.isNone_some, Bool.false_eq_true, ↓reduceIte, hf.items]
  rw [View.joinItems_cons]
  simp only [joinItems, List.flatMap_cons, List.flatMap_nil, List.append_nil, View.joinStep, hon,
    List.all_nil, ↓reduceIte, hf.left, Bool.false_and, Bool.false_eq_true]
  have hfm : ∀ l : List Row, l.filterMap (fun r => some (o.set w.item (some r))) =
      l.map fun r => o.set w.item (some r) := fun l => by
    induction l with
    | nil => rfl
    | cons x xs ih => simp [ih]
  rw [hfm, List.filter_map, hinner]
  simp only [hkind]
  congr 1
  apply List.filter_congr
  intro r _
  simp only [Function.comp, windowKept, List.all_map]
  apply Bool.eq_iff_iff.mpr
  simp only [List.all_eq_true, Function.comp]
  refine forall_congr' fun x => imp_congr_right fun hx => ?_
  rw [x.1.eval_congr (sub' := noSubs) (fun _ _ _ => rfl) (by simp [hnoq x hx])]

/-- A partition equality holds of the item's row `r'` under a binding around
the core exactly when `r'` holds, not NULL, the value the binding gives. -/
theorem partition_eval {s : Statement} {k i : Nat} {cd : Cond} {c : Nat}
    (h : partitionOf s k i cd = some c) :
    ∃ v : Bind → Value, ∀ o r', isTrue (cd.eval (o.set i (some r')) noSubs) = eqHolds (r'.cell c) (v o) := by
  obtain ⟨other, hq, hitems, _, heval⟩ := partitionOf_holds h
  refine ⟨fun o => other.eval o noSubs, fun o r' => ?_⟩
  have hother : other.eval (o.set i (some r')) noSubs = other.eval o noSubs := by
    apply other.eval_congr
    · intro j c' hj
      have hji : j ≠ i := hitems j (by simp only [Expr.items, List.mem_map]; exact ⟨(j, c'), hj, rfl⟩)
      simp [Bind.set, hji]
    · intro q _
      rfl
  rw [heval, hother]
  simp [Bind.set, cellOf]

/-- Of a row the window's core keeps under `o`, another row sits in the same
window exactly when the core keeps it too, and it holds no NULL in a
partition column. -/
theorem window_kept {s : Statement} {st : PState} {w : Window} (hf : ReadFacts s st w) {o : Bind}
    {r : Row} (hr : windowKept s w o r = true) :
    w.partition.all (fun c => (r.cell c).isSome) = true ∧
      ∀ r', (w.own.all (fun cd => isTrue (cd.eval (rowBind (some r')) noSubs)) &&
        w.partition.all (fun c => r'.cell c == r.cell c)) = windowKept s w o r' := by
  have hholds : ∀ r', windowKept s w o r' = true → ∀ x ∈ (s.core w.core).conds,
      isTrue (x.1.eval (o.set w.item (some r')) noSubs) = true := by
    intro r' h x hx
    exact List.all_eq_true.mp h x hx
  have hown : ∀ x ∈ (s.core w.core).conds, ownCond s w.item x.1 = true → ∀ r',
      x.1.eval (o.set w.item (some r')) noSubs = x.1.eval (rowBind (some r')) noSubs := by
    intro x _ ho r'
    rw [ownCond_eval ho]
    simp [Bind.set]
  have hpartOf : ∀ c ∈ w.partition, ∃ x ∈ (s.core w.core).conds,
      ownCond s w.item x.1 = false ∧ partitionOf s w.core w.item x.1 = some c := by
    intro c hc
    rw [hf.partition] at hc
    obtain ⟨x, hx, hxc⟩ := List.mem_filterMap.mp hc
    refine ⟨x, hx, ?_, ?_⟩
    · cases ho : ownCond s w.item x.1
      · rfl
      · simp [ho] at hxc
    · cases ho : ownCond s w.item x.1
      · simpa [ho] using hxc
      · simp [ho] at hxc
  refine ⟨List.all_eq_true.mpr fun c hc => ?_, fun r' => ?_⟩
  · obtain ⟨x, hx, _, hpc⟩ := hpartOf c hc
    obtain ⟨v, hv⟩ := partition_eval hpc
    have := hholds r hr x hx
    rw [hv] at this
    obtain ⟨y, hy, _⟩ := eqHolds_iff.mp this
    simp [hy]
  · apply Bool.eq_iff_iff.mpr
    simp only [Bool.and_eq_true, List.all_eq_true, beq_iff_eq]
    constructor
    · rintro ⟨howns, hparts⟩
      apply List.all_eq_true.mpr
      intro x hx
      cases ho : ownCond s w.item x.1
      · have hsome := (hf.conds x hx).2
        rw [ho] at hsome
        simp only [Bool.false_eq_true, false_or] at hsome
        obtain ⟨c, hc⟩ := Option.isSome_iff_exists.mp hsome
        have hcmem : c ∈ w.partition := by
          rw [hf.partition]
          exact List.mem_filterMap.mpr ⟨x, hx, by simp [ho, hc]⟩
        obtain ⟨v, hv⟩ := partition_eval hc
        have hrx := hholds r hr x hx
        show isTrue (x.1.eval (o.set w.item (some r')) noSubs) = true
        rw [hv] at hrx ⊢
        rw [hparts c hcmem]
        exact hrx
      · show isTrue (x.1.eval (o.set w.item (some r')) noSubs) = true
        rw [hown x hx ho]
        apply howns
        rw [hf.own]
        exact List.mem_filterMap.mpr ⟨x, hx, by simp [ho]⟩
    · intro hk
      refine ⟨fun cd hcd => ?_, fun c hc => ?_⟩
      · rw [hf.own] at hcd
        obtain ⟨x, hx, hxc⟩ := List.mem_filterMap.mp hcd
        cases ho : ownCond s w.item x.1
        · simp [ho] at hxc
        · simp only [ho, ↓reduceIte, Option.some.injEq] at hxc
          subst hxc
          rw [← hown x hx ho]
          exact hholds r' hk x hx
      · obtain ⟨x, hx, _, hpc⟩ := hpartOf c hc
        obtain ⟨v, hv⟩ := partition_eval hpc
        have h1 := hholds r' hk x hx
        have h2 := hholds r hr x hx
        rw [hv] at h1 h2
        obtain ⟨y1, hy1, hv1⟩ := eqHolds_iff.mp h1
        obtain ⟨y2, hy2, hv2⟩ := eqHolds_iff.mp h2
        rw [hy1, hy2, ← Option.some.inj (hv1.symm.trans hv2)]


/-! Whether a binding passes the window, as a test of its ORDER BY values. -/

/-- Whether fewer than the window's LIMIT rows the core keeps under `o`, in
`rows`, sort before the ORDER BY values `v`. -/
def windowTest (s : Statement) (w : Window) (o : Bind) (rows : List Row) (v : List Value) : Bool :=
  decide ((rows.filter fun r' =>
    windowKept s w o r' && keyCompare (w.keys.map (·.2)) (keyValues w.keys r') v == .lt).length <
      w.limit)

/-- The window's scope passes a row the core keeps under `o` exactly when
fewer than LIMIT rows the core keeps sort before it. -/
theorem window_passes {s : Statement} {st : PState} {w : Window} (hf : ReadFacts s st w) {o : Bind}
    {r : Row} (hr : windowKept s w o r = true) (rowsOf : Nat → List Row) :
    w.scope.passes rowsOf r = windowTest s w o (rowsOf w.item) (keyValues w.keys r) := by
  obtain ⟨hnull, hsame⟩ := window_kept hf hr
  simp only [Window.scope, RScope.passes, hnull, Bool.true_and, windowTest]
  congr 3
  apply List.filter_congr
  intro r' _
  rw [← hsame r']
  simp [windowBefore, Bool.and_assoc]

theorem keyCompare_lt_iff {ds : List Bool} {a b : List Value} :
    keyCompare ds a b = .lt ↔ keyLe ds b a = false := by
  simp only [keyLe, Limit.keyCompare_swap ds a b, bne_eq_false_iff_eq]
  cases keyCompare ds a b <;> simp [Ordering.swap]

theorem keyCompare_lt_of_le {ds : List Bool} {x a b : List Value} (hx : x.length = ds.length)
    (ha : a.length = ds.length) (hb : b.length = ds.length) (hlt : keyCompare ds x a = .lt)
    (hle : keyLe ds a b = true) : keyCompare ds x b = .lt := by
  rw [keyCompare_lt_iff] at hlt ⊢
  cases hbx : keyLe ds b x with
  | false => rfl
  | true => rw [Limit.keyLe_trans ha hb hx hle hbx] at hlt; cases hlt

theorem length_filter_mono {α : Type} {p p' : α → Bool} (h : ∀ a, p a = true → p' a = true) :
    ∀ l : List α, (l.filter p).length ≤ (l.filter p').length
  | [] => Nat.le_refl _
  | x :: xs => by
    have ih := length_filter_mono h xs
    cases hx : p x <;> cases hx' : p' x <;> simp [hx, hx'] <;> try omega
    exact absurd (h x hx) (by simp [hx'])

/-- The window's test is closed downward: values that sort at or before
values that pass it pass it. -/
theorem windowTest_down {s : Statement} (w : Window) (o : Bind) (rows : List Row)
    {a b : List Value} (ha : a.length = w.keys.length) (hb : b.length = w.keys.length)
    (hle : keyLe (w.keys.map (·.2)) a b = true) (hpass : windowTest s w o rows b = true) :
    windowTest s w o rows a = true := by
  simp only [windowTest, decide_eq_true_eq] at hpass ⊢
  refine Nat.lt_of_le_of_lt (length_filter_mono (fun r' h => ?_) rows) hpass
  simp only [Bool.and_eq_true, beq_iff_eq] at h ⊢
  refine ⟨h.1, keyCompare_lt_of_le (by simp [keyValues]) (by simpa using ha) (by simpa using hb) h.2 hle⟩

/-- Of `n` rows or more, at least `n` have fewer than `n` rows that sort
before them: the first `n` in their order. -/
theorem count_first {α : Type} {le : α → α → Bool} (total : ∀ a b, le a b || le b a)
    (trans : ∀ a b c, le a b → le b c → le a c) (L : List α) {n : Nat} (hn : n ≤ L.length) :
    n ≤ (L.filter fun x => decide ((L.filter fun y => !le x y).length < n)).length := by
  let S := sortBy le L
  have hS : S.Pairwise (le · ·) := Limit.pairwise_sortBy total trans L
  have hlen : ∀ P : α → Bool, (L.filter P).length = (S.filter P).length := by
    intro P
    rw [← Limit.sortBy_filter total trans, Limit.length_sortBy]
  rw [hlen]
  have hrefl : ∀ a, le a a = true := fun a => by simpa using total a a
  -- Each of the first `n` sorted rows has fewer than `n` rows before it.
  have hfirst : ∀ j (hj : j < S.length), j < n →
      (L.filter fun y => !le S[j] y).length < n := by
    intro j hj hjn
    rw [hlen]
    have hsplit : S = S.take j ++ S[j] :: S.drop (j + 1) := by
      rw [← List.drop_eq_getElem_cons hj, List.take_append_drop]
    generalize S[j] = x at hsplit ⊢
    have hpw := hS
    rw [hsplit, List.pairwise_append, List.pairwise_cons] at hpw
    have hafter : ∀ z ∈ x :: S.drop (j + 1), le x z = true := by
      intro z hz
      rcases List.mem_cons.mp hz with rfl | hz
      · exact hrefl _
      · exact hpw.2.1.1 z hz
    rw [hsplit, List.filter_append, List.length_append]
    have hnil : ((x :: S.drop (j + 1)).filter fun y => !le x y) = [] :=
      List.filter_eq_nil_iff.mpr fun z hz => by simp [hafter z hz]
    rw [hnil, List.length_nil, Nat.add_zero]
    calc ((S.take j).filter _).length ≤ (S.take j).length := List.length_filter_le _ _
      _ ≤ j := List.length_take_le _ _
      _ < n := hjn
  have hlenS : n ≤ S.length := by rw [Limit.length_sortBy]; exact hn
  calc n = (S.take n).length := by rw [List.length_take]; omega
    _ = ((S.take n).filter fun x => decide ((L.filter fun y => !le x y).length < n)).length := by
        congr 1
        symm
        apply List.filter_eq_self.mpr
        intro x hx
        obtain ⟨j, hj, rfl⟩ := List.getElem_of_mem hx
        rw [List.getElem_take]
        have hjn : j < n := by rw [List.length_take] at hj; omega
        exact decide_eq_true (hfirst j (by rw [List.length_take] at hj; omega) hjn)
    _ ≤ (S.filter fun x => decide ((L.filter fun y => !le x y).length < n)).length :=
        (List.take_sublist n S).filter _ |>.length_le

/-- The planner's windows restrict their subqueries' rows: a query with a
binding that fails its window is a window's, whose bindings pass it by a
test of their ORDER BY values closed downward, and of which either every
binding passes, or the first LIMIT do in the database the write leaves. -/
theorem plan_windowRows {s : Statement} (hwf : s.wellFormed = true) {X Y : Db}
    {G : Nat → Row → Bool} {q k : Nat} (hk : k ∈ (s.query q).arms) {b0 : Bind}
    (hb0 : (plan s).windowPasses s Y k b0 = false) :
    WindowRows s X Y G ((plan s).windowPasses s Y) q k := by
  obtain ⟨w, hw, hwi⟩ := windowPasses_false hb0
  obtain ⟨_, hf⟩ := plan_window_read hw
  have hcore : w.core = k := by
    have h1 : w.item ∈ (s.core w.core).items := by rw [hf.items]; simp
    rw [← ((s.wellFormed_parts hwf).items _ (s.core_lt h1) _ h1).2,
      ((s.wellFormed_parts hwf).items _ (s.core_lt hwi) _ hwi).2]
  subst hcore
  have hq : w.query = q := s.wellFormed_armOf hwf (by rw [hf.arms]; simp) hk
  subst hq
  obtain ⟨t, ht⟩ := Option.isSome_iff_exists.mp hf.table
  have hkind : (s.item w.item).kind = .table t := by
    unfold Statement.table? at ht
    split at ht
    · cases ht; assumption
    · cases ht
  have hlook : lookupRows s Y w.item = Y.rows t := by simp [lookupRows, hkind]
  have hscope := plan_windowScope hwf hw
  have hdir := (window_keys hf (fun _ _ => []) Bind.empty).2
  have hklen : w.keys.length = (s.query w.query).orderBy.length := by
    simpa using congrArg List.length hdir
  -- A binding of the window's core passes it as its ORDER BY values pass the test.
  have hpass : ∀ (Z : Db) (E : Bind → Nat → List Row) (o : Bind), ∀ b ∈ coreBindings s Z E o w.core,
      (plan s).windowPasses s Y w.core b =
        windowTest s w o (Y.rows t) (Limit.orderKey (s.query w.query) ((s.core w.core).outRow E b)) := by
    intro Z E o b hb
    rw [window_bindings hf ht] at hb
    obtain ⟨r, hr, rfl⟩ := List.mem_map.mp hb
    have hkept := (List.mem_filter.mp hr).2
    rw [(window_keys hf E _).1]
    simp only [Planned.windowPasses, hf.items, List.all_cons, List.all_nil, Bool.and_true,
      Bind.set, ↓reduceIte, hscope]
    rw [window_passes hf hkept, hlook]
    rfl
  refine ⟨hf.arms, hf.agg, hf.windowed, hf.distinct, hf.offset, hf.order,
    fun E E' b => by rw [(window_keys hf E b).1, (window_keys hf E' b).1],
    ⟨w.limit, hf.limit, fun o _ _ EX EY => ⟨windowTest s w o (Y.rows t), ?_, hpass X EX o,
      hpass Y EY o, ?_⟩⟩⟩
  · intro a b ha hb hle hP
    rw [← hdir] at hle
    exact windowTest_down w o _ (by omega) (by omega) hle hP
  · let K := (Y.rows t).filter (windowKept s w o)
    let kv := keyValues w.keys
    have hkv : ∀ r, (kv r).length = (w.keys.map (·.2)).length := by intro r; simp [kv, keyValues]
    have htest : ∀ v, windowTest s w o (Y.rows t) v =
        decide ((K.filter fun r' => keyCompare (w.keys.map (·.2)) (kv r') v == .lt).length < w.limit) := by
      intro v
      simp only [windowTest, K, List.filter_filter, kv, Bool.and_comm]
    by_cases hsmall : K.length < w.limit
    · left
      have hall : ∀ (Z : Db) (E : Bind → Nat → List Row), ∀ b ∈ coreBindings s Z E o w.core,
          (plan s).windowPasses s Y w.core b = true := by
        intro Z E b hb
        rw [hpass Z E o b hb, htest]
        exact decide_eq_true (Nat.lt_of_le_of_lt (List.length_filter_le _ _) hsmall)
      exact ⟨hall X EX, hall Y EY⟩
    · right
      have hbinds := window_bindings hf ht Y EY o
      rw [hbinds, List.filter_map, List.length_map]
      let ds : List Bool := w.keys.map (·.2)
      have hcount := count_first (le := fun a b : Row => keyLe ds (kv a) (kv b))
        (fun a b => Limit.keyLe_total _ _ _)
        (fun a b c h1 h2 => Limit.keyLe_trans (hkv a) (hkv b) (hkv c) h1 h2) K (n := w.limit)
        (by omega)
      refine Nat.le_trans hcount (Nat.le_of_eq ?_)
      congr 1
      apply List.filter_congr
      intro r hr
      have hkr : w.keys.map (fun (e, _) => e.eval (rowBind ((o.set w.item (some r)) w.item)) noSubs) =
          kv r := by simp [Bind.set, kv, keyValues]
      simp only [Function.comp]
      rw [hpass Y EY o _ (by rw [hbinds]; exact List.mem_map.mpr ⟨r, hr, rfl⟩),
        (window_keys hf EY _).1, hkr, htest]
      have hpt : ∀ y, (keyCompare ds (kv y) (kv r) == .lt) = !keyLe ds (kv r) (kv y) := by
        intro y
        cases hy : keyLe ds (kv r) (kv y)
        · simp [keyCompare_lt_iff.mpr hy]
        · have : keyCompare ds (kv y) (kv r) ≠ .lt := fun h => by
            rw [keyCompare_lt_iff] at h; rw [h] at hy; cases hy
          simp [this]
      exact congrArg (fun l : List Row => decide (l.length < w.limit))
        (List.filter_congr fun y _ => (hpt y).symm)

/-! A subquery's source whose value a comparison takes has no window. -/

/-- Each binding of the core of a subquery's source whose value the planned
state takes passes its windows: `read` gives no window to such a core. -/
theorem windowPasses_taken {s : Statement} (hwf : s.wellFormed = true) {p : Nat}
    (htaken : ∀ i, (s.item i).core = (s.item p).core →
      Window.takesValues s (plan s).state s.itemNumbers i = true)
    (Y : Db) (b : Bind) : (plan s).windowPasses s Y (s.item p).core b = true := by
  cases h : (plan s).windowPasses s Y (s.item p).core b with
  | true => rfl
  | false =>
    obtain ⟨w, hw, hwi⟩ := windowPasses_false h
    have hcore := ((s.wellFormed_parts hwf).items _ (s.core_lt hwi) _ hwi).2
    have := (plan_window_read hw).2.takes
    rw [htaken w.item hcore] at this
    cases this

theorem takesValues_edge {s : Statement} {st : PState} {items : List Nat} {i : Nat} {e : Edge}
    (he : e ∈ st.edges) (hn : e.nested = true) (hc : (s.item e.partner).core = (s.item i).core) :
    Window.takesValues s st items i = true := by
  simp only [Window.takesValues, Bool.or_eq_true, List.any_eq_true]
  exact Or.inl ⟨e, he, by simp [hn, hc]⟩

theorem takesValues_side {s : Statement} {st : PState} {items : List Nat} {i j : Nat}
    {sides : List Side} {side : Side} {e : Edge} (hj : j ∈ items) (hs : sides ∈ (st.items j).ors)
    (hside : side ∈ sides) (he : e ∈ side.edges) (hn : e.nested = true)
    (hc : (s.item e.partner).core = (s.item i).core) :
    Window.takesValues s st items i = true := by
  simp only [Window.takesValues, Bool.or_eq_true, List.any_eq_true]
  exact Or.inr ⟨j, hj, sides, hs, side, hside, e, he, by simp [hn, hc]⟩

/-- A LEFT JOIN's item has no window. -/
theorem windowScope_left {s : Statement} {i : Nat}
    (hl : (s.item i).left = true) : (plan s).windowScope i = .all := by
  unfold Planned.windowScope
  cases hfind : (plan s).windows.find? (·.item == i) with
  | none => rfl
  | some w =>
    have hi : w.item = i := by simpa using List.find?_some hfind
    have := (plan_window_read (List.mem_of_find?_eq_some hfind)).2.left
    rw [hi, hl] at this
    cases this

/-- The statement's own query has no window. -/
theorem windowPasses_root {s : Statement} (hwf : s.wellFormed = true) {k : Nat}
    (hk : k ∈ (s.query s.root).arms) (Y : Db) (b : Bind) : (plan s).windowPasses s Y k b = true := by
  cases h : (plan s).windowPasses s Y k b with
  | true => rfl
  | false =>
    obtain ⟨w, hw, hwi⟩ := windowPasses_false h
    obtain ⟨_, hf⟩ := plan_window_read hw
    have h1 : w.item ∈ (s.core w.core).items := by rw [hf.items]; simp
    have hcore : w.core = k := by
      rw [← ((s.wellFormed_parts hwf).items _ (s.core_lt h1) _ h1).2,
        ((s.wellFormed_parts hwf).items _ (s.core_lt hwi) _ hwi).2]
    subst hcore
    have hq : w.query = s.root := s.wellFormed_armOf hwf (by rw [hf.arms]; simp) hk
    obtain ⟨p, hp, _⟩ := hf.parent
    rw [(s.wellFormed_parts hwf).rootOutside _ hk] at hp
    cases hp

end SqliveNarrowing
