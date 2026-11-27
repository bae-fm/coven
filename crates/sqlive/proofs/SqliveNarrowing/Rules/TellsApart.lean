import SqliveNarrowing.Congr

/-! Rule 7's test of what tells a core's bindings apart: on a database that
holds what its schema constrains (`Db.Constrained`), two bindings a core keeps
that tie in expressions that tell its bindings apart (`Statement.tellsApart`,
`Tells.lean`) are one binding (`tellsApart_separates`). The proof takes the
items in the order the test determines them: two bindings that agree on the
items before hold one row of the next, or both none. -/
namespace SqliveNarrowing.Pick

/-- A database whose tables hold each row's rowid in its first column, never
NULL and different in each row, as each slot holds the row of its rowid
(`Json.readDb`). -/
def _root_.SqliveNarrowing.Db.Keyed (db : Db) : Prop :=
  ∀ t, ∀ r ∈ db.rows t, r.cell 0 ≠ none ∧ ∀ r' ∈ db.rows t, r.cell 0 = r'.cell 0 → r = r'

/-- A binding that joining `is` onto `bs` gives is a binding of `bs` with
each item of `is` bound to one of its rows, or to none. -/
theorem mem_joinItems {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} :
    ∀ (is : List Nat) (bs : List Bind) (b' : Bind), b' ∈ joinItems s c rowsOf subs is bs →
      ∃ b ∈ bs, (∀ j, j ∉ is → b' j = b j) ∧
        ∀ i ∈ is, b' i = none ∨ ∃ r ∈ rowsOf i, b' i = some r
  | [], bs, b', h => ⟨b', h, fun _ _ => rfl, fun _ hi => by simp at hi⟩
  | i :: is, bs, b', h => by
    simp only [joinItems] at h
    obtain ⟨b'', hb'', hout, hin⟩ := mem_joinItems is _ b' h
    obtain ⟨b, hb, hset⟩ := List.mem_flatMap.mp hb''
    have hrow : b'' = b.set i none ∨ ∃ r ∈ rowsOf i, b'' = b.set i (some r) := by
      split at hset
      · simp only [List.mem_singleton] at hset
        exact Or.inl hset
      · obtain ⟨r, hr, hsome⟩ := List.mem_filterMap.mp hset
        split at hsome
        · exact Or.inr ⟨r, hr, (Option.some.inj hsome).symm⟩
        · cases hsome
    refine ⟨b, hb, ?_, ?_⟩
    · intro j hj
      simp only [List.mem_cons, not_or] at hj
      rw [hout j hj.2]
      rcases hrow with rfl | ⟨r, _, rfl⟩ <;> simp [Bind.set, hj.1]
    · intro i' hi'
      by_cases hmem : i' ∈ is
      · exact hin i' hmem
      · have hi : i' = i := by simpa [hmem] using hi'
        subst hi
        rw [hout i' hmem]
        rcases hrow with rfl | ⟨r, hr, rfl⟩
        · simp [Bind.set]
        · exact Or.inr ⟨r, hr, by simp [Bind.set]⟩

/-! What tells a core's bindings apart. -/

/-- A database whose tables hold what each item declares of its table: no NULL
in a column declared NOT NULL, and no two rows that hold, not NULL, the same
values in each column of a unique index. -/
def _root_.SqliveNarrowing.Db.Constrained (db : Db) (s : Statement) : Prop :=
  ∀ i t, (s.item i).kind = .table t →
    (∀ c ∈ (s.item i).notNull, ∀ r ∈ db.rows t, r.cell c ≠ none) ∧
    ∀ key ∈ (s.item i).keys, ∀ r ∈ db.rows t, ∀ r' ∈ db.rows t,
      (∀ c ∈ key, r.cell c = r'.cell c ∧ r.cell c ≠ none) → r = r'

/-- What `one` promises of queries run with fuel `g`: a query it says returns
one row at most for values of `columns` among rows that tie in `compared`
returns no two different rows that hold, not NULL, the same values in
`columns`, and the same values, NULL or not, in `compared`. -/
def OneSound (s : Statement) (db : Db) (g : Nat) (one : Nat → List Nat → List Nat → Bool) :
    Prop :=
  ∀ q columns compared, one q columns compared = true →
    ∀ b, ∀ r ∈ evalQuery s db g b q, ∀ r' ∈ evalQuery s db g b q,
    (∀ p ∈ columns, r.cell p = r'.cell p ∧ r.cell p ≠ none) →
    (∀ p ∈ compared, r.cell p = r'.cell p) → r = r'

/-- A binding a join keeps holds a row of each item it joins that no LEFT JOIN
adds. -/
theorem joinItems_bound {s : Statement} {c : Core} {rowsOf : Nat → List Row} {subs : Bind → Subs} :
    ∀ (is : List Nat) (L : List Bind), is.Nodup → ∀ b ∈ joinItems s c rowsOf subs is L,
      ∀ i ∈ is, (s.item i).left = false → b i ≠ none
  | [], _, _, _, _, i, hi, _ => by cases hi
  | a :: rest, L, hnd, b, hb, i, hi, hl => by
    rw [View.joinItems_cons] at hb
    have har : a ∉ rest := (List.nodup_cons.mp hnd).1
    by_cases hia : i = a
    · subst hia
      obtain ⟨z, hz, hbz⟩ := joinItems_origin rest _ b hb
      obtain ⟨y0, _, hzs⟩ := List.mem_flatMap.mp hz
      rw [hbz i har]
      unfold View.joinStep at hzs
      simp only [hl, Bool.false_and, Bool.false_eq_true, ↓reduceIte] at hzs
      obtain ⟨r, _, hr⟩ := List.mem_filterMap.mp hzs
      split at hr
      · obtain rfl := Option.some.inj hr
        simp [Bind.set]
      · cases hr
    · exact joinItems_bound rest _ (List.nodup_cons.mp hnd).2 b hb i
        ((List.mem_cons.mp hi).resolve_left hia) hl

/-- A binding a join keeps that holds no row of an item it joins agrees,
outside the items joined from that item on, with a binding under which the
item's ON holds of none of its rows. -/
theorem joinItems_unjoined {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} :
    ∀ (is : List Nat) (L : List Bind), is.Nodup → ∀ b ∈ joinItems s c rowsOf subs is L,
      ∀ i ∈ is, b i = none →
        ∃ pre rest, is = pre ++ i :: rest ∧ ∃ y : Bind, (∀ j, j ≠ i → j ∉ rest → y j = b j) ∧
          ∀ r ∈ rowsOf i, onHolds c subs i (y.set i (some r)) = false
  | [], _, _, _, _, i, hi, _ => by cases hi
  | a :: rest, L, hnd, b, hb, i, hi, hbi => by
    rw [View.joinItems_cons] at hb
    have har : a ∉ rest := (List.nodup_cons.mp hnd).1
    by_cases hia : i = a
    · subst hia
      obtain ⟨z, hz, hbz⟩ := joinItems_origin rest _ b hb
      obtain ⟨y0, _, hzs⟩ := List.mem_flatMap.mp hz
      have hzi : z i = none := by rw [← hbz i har]; exact hbi
      refine ⟨[], rest, rfl, y0, ?_, ?_⟩
      · intro j hj hjr
        rw [hbz j hjr]
        obtain ⟨x, rfl⟩ := mem_joinStep hzs
        simp [Bind.set, hj]
      · intro r hr
        unfold View.joinStep at hzs
        simp only at hzs
        split at hzs
        · rename_i hempty
          simp only [Bool.and_eq_true, List.isEmpty_iff, List.filterMap_eq_nil_iff] at hempty
          have := hempty.2 r hr
          unfold onHolds
          split at this
          · cases this
          · rename_i hon
            simpa using hon
        · obtain ⟨r', _, hr'⟩ := List.mem_filterMap.mp hzs
          split at hr'
          · obtain rfl := Option.some.inj hr'
            simp [Bind.set] at hzi
          · cases hr'
    · have hir : i ∈ rest := (List.mem_cons.mp hi).resolve_left hia
      obtain ⟨pre, rest', heq, y, hy, hnone⟩ :=
        joinItems_unjoined rest _ (List.nodup_cons.mp hnd).2 b hb i hir hbi
      exact ⟨a :: pre, rest', by rw [heq]; rfl, y, hy, hnone⟩

section Told

variable {s : Statement} {db : Db} {q k g : Nat} {outer : Bind}

/-- The rows core `k`'s item `i` joins under the binding `outer`. -/
def itemRows (s : Statement) (db : Db) (g : Nat) (outer : Bind) (k i : Nat) : List Row :=
  match (s.item i).kind with
  | .table t => db.rows t
  | .derived q _ => evalQuery s db g (if (s.core k).parent.isNone then Bind.empty else outer) q

/-- A binding core `k` keeps holds the binding it ran under outside the core's
items, and a row of each item's rows, or none. -/
theorem kept_rows {b : Bind} (hb : b ∈ coreBindings s db (evalQuery s db g) outer k) :
    (∀ j ∉ (s.core k).items,
      b j = (if (s.core k).parent.isNone then Bind.empty else outer) j) ∧
    ∀ i ∈ (s.core k).items, ∀ r, b i = some r → r ∈ itemRows s db g outer k i := by
  simp only [coreBindings, List.mem_filter] at hb
  obtain ⟨b0, hb0, hout, hin⟩ := mem_joinItems _ _ _ hb.1
  simp only [List.mem_singleton] at hb0
  subst hb0
  refine ⟨hout, fun i hi r hr => ?_⟩
  rcases hin i hi with h | ⟨r', hr', h⟩
  · rw [hr] at h; cases h
  · rw [hr] at h; cases h; exact hr'

/-- Two bindings core `k` keeps that agree on the items `known` agree on each
item among `deps` when every item of the core there is known. -/
theorem agree_within {ba bb : Bind} (hba : ba ∈ coreBindings s db (evalQuery s db g) outer k)
    (hbb : bb ∈ coreBindings s db (evalQuery s db g) outer k) {known : List Nat}
    (hknown : ∀ j ∈ known, ba j = bb j) {deps : List Nat}
    (h : s.within k known deps = true) : ∀ j ∈ deps, ba j = bb j := by
  intro j hj
  simp only [Statement.within, List.all_eq_true, Bool.or_eq_true, Bool.not_eq_true',
    List.contains_iff_mem] at h
  rcases h j hj with h | h
  · have hn : j ∉ (s.core k).items := by simpa using h
    rw [(kept_rows hba).1 j hn, (kept_rows hbb).1 j hn]
  · exact hknown j h

/-- An expression takes one value in two bindings that agree on the items it
depends on. -/
theorem eval_agree (hwf : s.wellFormed = true) {ba bb : Bind} {e : Expr}
    (h : ∀ j ∈ s.exprDeps e, ba j = bb j) :
    e.eval ba (evalQuery s db g ba) = e.eval bb (evalQuery s db g bb) := by
  apply e.eval_congr
  · intro i c hic
    rw [h i (List.mem_append_left _ (List.mem_map.mpr ⟨(i, c), hic, rfl⟩))]
  · intro q' hq'
    apply evalQuery_outer_congr hwf db
    intro j hj
    exact h j (List.mem_append_right _ (List.mem_flatMap.mpr ⟨q', hq', hj⟩))

/-- An equality of column `c` of item `i` with a value the known items give
fixes the column: two bindings it holds of that agree on those items hold one
value there, not NULL. -/
theorem equates_fixes (hwf : s.wellFormed = true) {ba bb : Bind}
    (hba : ba ∈ coreBindings s db (evalQuery s db g) outer k)
    (hbb : bb ∈ coreBindings s db (evalQuery s db g) outer k) {known : List Nat}
    (hknown : ∀ j ∈ known, ba j = bb j) {i c : Nat} {cd : Cond}
    (h : s.equates k known i c cd = true)
    (ha : isTrue (cd.eval ba (evalQuery s db g ba)) = true)
    (hb : isTrue (cd.eval bb (evalQuery s db g bb)) = true) :
    cellOf (ba i) c = cellOf (bb i) c ∧ cellOf (ba i) c ≠ none := by
  have eq_true : ∀ x y : Value, isTrue (Op.apply .eq x y) = true → x = y ∧ x ≠ none := by
    intro x y h
    cases x <;> cases y <;> simp_all [isTrue, Op.apply, Op.compare]
  unfold Statement.equates at h
  split at h
  · rename_i l r
    simp only [Cond.eval] at ha hb
    obtain ⟨ha1, ha2⟩ := eq_true _ _ ha
    obtain ⟨hb1, _⟩ := eq_true _ _ hb
    simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at h
    rcases h with ⟨hl, ⟨hw, _⟩, _⟩ | ⟨hr, ⟨hw, _⟩, _⟩
    · subst hl
      have hr := eval_agree (db := db) (g := g) hwf (agree_within hba hbb hknown hw)
      simp only [Expr.eval] at ha1 ha2 hb1
      exact ⟨by rw [ha1, hr, ← hb1], ha2⟩
    · subst hr
      have hl := eval_agree (db := db) (g := g) hwf (agree_within hba hbb hknown hw)
      simp only [Expr.eval] at ha1 ha2 hb1
      exact ⟨by rw [← ha1, hl, hb1], ha1 ▸ ha2⟩
  · cases h

/-- A column an equality of the WHERE or an inner join's ON fixes, or the
values `fixed` hold, takes one value, not NULL, in two kept bindings. -/
theorem anchored_cell (hwf : s.wellFormed = true) {ba bb : Bind}
    (hba : ba ∈ coreBindings s db (evalQuery s db g) outer k)
    (hbb : bb ∈ coreBindings s db (evalQuery s db g) outer k) {known : List Nat}
    (hknown : ∀ j ∈ known, ba j = bb j) {fixed : List (Nat × Nat)}
    (hfixed : ∀ p ∈ fixed, cellOf (ba p.1) p.2 = cellOf (bb p.1) p.2 ∧ cellOf (ba p.1) p.2 ≠ none)
    {i c : Nat} (h : (fixed.contains (i, c) || s.innerEquated k known i c) = true) :
    cellOf (ba i) c = cellOf (bb i) c ∧ cellOf (ba i) c ≠ none := by
  rcases Bool.or_eq_true _ _ ▸ h with h | h
  · exact hfixed (i, c) (by simpa using h)
  · unfold Statement.innerEquated at h
    obtain ⟨⟨cd, p⟩, hmem, hcd⟩ := List.any_eq_true.mp h
    simp only [Bool.and_eq_true, beq_iff_eq] at hcd
    obtain ⟨rfl, hcd⟩ := hcd
    have hinner : cd ∈ (s.core k).innerConds := by
      simp only [Core.innerConds, List.mem_filterMap]
      exact ⟨(cd, .inner), hmem, by simp⟩
    have holds : ∀ x ∈ coreBindings s db (evalQuery s db g) outer k,
        isTrue (cd.eval x (evalQuery s db g x)) = true := by
      intro x hx
      simp only [coreBindings, List.mem_filter, List.all_eq_true] at hx
      exact hx.2 cd hinner
    exact equates_fixes hwf hba hbb hknown hcd (holds ba hba) (holds bb hbb)

/-- Where both bindings hold a row of item `i`, a column any equality fixes
takes one value, not NULL, in both: the ON of its LEFT JOIN holds of each. -/
theorem fixed_cell (hwf : s.wellFormed = true) (hk : k ∈ (s.query q).arms) {ba bb : Bind}
    (hba : ba ∈ coreBindings s db (evalQuery s db g) outer k)
    (hbb : bb ∈ coreBindings s db (evalQuery s db g) outer k) {known : List Nat}
    (hknown : ∀ j ∈ known, ba j = bb j) {fixed : List (Nat × Nat)}
    (hfixed : ∀ p ∈ fixed, cellOf (ba p.1) p.2 = cellOf (bb p.1) p.2 ∧ cellOf (ba p.1) p.2 ≠ none)
    {i c : Nat} (hi : i ∈ (s.core k).items) {ra rb : Row} (hra : ba i = some ra)
    (hrb : bb i = some rb)
    (h : (fixed.contains (i, c) || s.innerEquated k known i c || s.onEquated k known i c) = true) :
    cellOf (ba i) c = cellOf (bb i) c ∧ cellOf (ba i) c ≠ none := by
  rcases Bool.or_eq_true _ _ ▸ h with h | h
  · exact anchored_cell hwf hba hbb hknown hfixed h
  · unfold Statement.onEquated at h
    obtain ⟨⟨cd, p⟩, hmem, hcd⟩ := List.any_eq_true.mp h
    simp only [Bool.and_eq_true, beq_iff_eq] at hcd
    obtain ⟨rfl, hcd⟩ := hcd
    have hon : cd ∈ (s.core k).onOf i := by
      simp only [Core.onOf, List.mem_filterMap]
      exact ⟨(cd, .leftOn i), hmem, by simp⟩
    exact equates_fixes hwf hba hbb hknown hcd
      (s.on_kept hwf db hk hba hi hra cd hon) (s.on_kept hwf db hk hbb hi hrb cd hon)

/-- Where the known items decide whether item `i`'s LEFT JOIN finds a row, no
kept binding holds a row of it while another that agrees on them holds
none. -/
theorem onKnown_none (hwf : s.wellFormed = true) (hk : k ∈ (s.query q).arms) {ba bb : Bind}
    (hba : ba ∈ coreBindings s db (evalQuery s db g) outer k)
    (hbb : bb ∈ coreBindings s db (evalQuery s db g) outer k) {known : List Nat}
    (hknown : ∀ j ∈ known, ba j = bb j) {i : Nat} (hi : i ∈ (s.core k).items)
    (h : s.onKnown k known i = true) {rb : Row} (hna : ba i = none) (hrb : bb i = some rb) :
    False := by
  have hklt := ((s.wellFormed_parts hwf).arms q (s.query_lt hk) k hk).1
  have hnd := (s.wellFormed_parts hwf).nodup k hklt
  have hrows : rb ∈ itemRows s db g outer k i := (kept_rows hbb).2 i hi rb hrb
  have hbb' := s.on_kept hwf db hk hbb hi hrb
  have hjoin := hba
  unfold coreBindings at hjoin
  obtain ⟨pre, rest, heq, y, hy, hnone⟩ :=
    joinItems_unjoined _ _ hnd ba (List.mem_filter.mp hjoin).1 i hi hna
  have hfalse := hnone rb hrows
  have hndi : (pre ++ i :: rest).Nodup := heq ▸ hnd
  have htw : (s.core k).items.takeWhile (· ≠ i) = pre := by rw [heq]; exact takeWhile_split hndi
  have hout : ∀ j, (j ∈ pre ∨ j ∉ (s.core k).items) → j ≠ i ∧ j ∉ rest := by
    intro j hj
    rcases hj with hjp | hjk
    · have hne : j ≠ i := fun e => by
        subst e
        exact (List.nodup_append.mp hndi).2.2 j hjp j List.mem_cons_self rfl
      exact ⟨hne, fun hjr =>
        (List.nodup_append.mp hndi).2.2 j hjp j (List.mem_cons_of_mem i hjr) rfl⟩
    · exact ⟨fun e => hjk (e ▸ hi),
        fun hjr => hjk (by rw [heq]; exact List.mem_append_right _ (List.mem_cons_of_mem i hjr))⟩
  have htrue : onHolds (s.core k) (evalQuery s db g) i (y.set i (some rb)) = true := by
    unfold onHolds
    rw [List.all_eq_true]
    intro cd hcdi
    have hcd := Core.mem_onOf hcdi
    rw [← hbb' cd hcdi]
    congr 1
    have hdeps : s.within k (i :: known) (s.condDeps cd) = true := by
      unfold Statement.onKnown at h
      exact List.all_eq_true.mp h cd hcdi
    -- Each item the condition depends on holds in `y` with `rb` what it holds
    -- in `bb`.
    have hsame : ∀ j ∈ s.condDeps cd, (j = i ∨ j ∈ pre ∨ j ∉ (s.core k).items) →
        (y.set i (some rb)) j = bb j := by
      intro j hj hplace
      by_cases hji : j = i
      · subst hji; simp [Bind.set, hrb]
      · have hp := hout j (hplace.resolve_left hji)
        simp only [Bind.set, hji, ↓reduceIte]
        rw [hy j hp.1 hp.2]
        simp only [Statement.within, List.all_eq_true, Bool.or_eq_true, Bool.not_eq_true',
          List.contains_iff_mem, List.mem_cons] at hdeps
        rcases hdeps j hj with hn | hm
        · have hn' : j ∉ (s.core k).items := by simpa using hn
          rw [(kept_rows hba).1 j hn', (kept_rows hbb).1 j hn']
        · exact hknown j (hm.resolve_left hji)
    apply Cond.eval_congr
    · intro j col hj
      have hjc : j ∈ cd.items := List.mem_map.mpr ⟨(j, col), hj, rfl⟩
      rw [hsame j (List.mem_append_left _ hjc) (by
        rcases s.wellFormed_onNamesEarlier hwf hklt hcd hjc with e | hjp | hjk
        · exact Or.inl e
        · rw [htw] at hjp; exact Or.inr (Or.inl hjp)
        · exact Or.inr (Or.inr hjk))]
    · intro q' hq'
      apply evalQuery_outer_congr hwf db
      intro j hj
      refine hsame j (List.mem_append_right _ (List.mem_flatMap.mpr ⟨q', hq', hj⟩)) ?_
      by_cases hcore : (s.item j).core = k
      · have := (s.wellFormed_parts hwf).onNames k hklt cd i hcd q' hq' j hj hcore
        rw [htw] at this
        rcases List.mem_append.mp this with hjp | hji
        · exact Or.inr (Or.inl hjp)
        · exact Or.inl (List.mem_singleton.mp hji)
      · exact Or.inr (Or.inr fun hjk => hcore ((s.wellFormed_parts hwf).items k hklt j hjk).2)
  rw [htrue] at hfalse
  cases hfalse

/-- An item the items `known` determine holds one row, or none, in two kept
bindings that agree on those items, tie in `sorted` and hold `fixed`'s values
alike. -/
theorem determines_agree (hwf : s.wellFormed = true) (hdb : db.Keyed)
    (hcons : db.Constrained s) (hk : k ∈ (s.query q).arms) {one : Nat → List Nat → List Nat → Bool}
    (hone : OneSound s db g one) {ba bb : Bind}
    (hba : ba ∈ coreBindings s db (evalQuery s db g) outer k)
    (hbb : bb ∈ coreBindings s db (evalQuery s db g) outer k)
    {sorted : List Expr} {sa sb : Subs} (hsorted : ∀ e ∈ sorted, e.eval ba sa = e.eval bb sb)
    {fixed : List (Nat × Nat)}
    (hfixed : ∀ p ∈ fixed, cellOf (ba p.1) p.2 = cellOf (bb p.1) p.2 ∧ cellOf (ba p.1) p.2 ≠ none)
    {known : List Nat} (hknown : ∀ j ∈ known, ba j = bb j) {i : Nat} (hi : i ∈ (s.core k).items)
    (h : s.determines one k sorted fixed known i = true) : ba i = bb i := by
  have hklt := ((s.wellFormed_parts hwf).arms q (s.query_lt hk) k hk).1
  have hnd := (s.wellFormed_parts hwf).nodup k hklt
  -- A LEFT JOIN alone leaves a kept binding with no row of the item.
  have hleft : ∀ x ∈ coreBindings s db (evalQuery s db g) outer k, x i = none →
      (s.item i).left = true := by
    intro x hx hn
    cases hl : (s.item i).left with
    | true => rfl
    | false =>
      unfold coreBindings at hx
      exact absurd hn (joinItems_bound _ _ hnd x (List.mem_filter.mp hx).1 i hi hl)
  have hsame : ∀ c, s.sortedColumn sorted i c = true → cellOf (ba i) c = cellOf (bb i) c := by
    intro c hc
    simp only [Statement.sortedColumn, Bool.and_eq_true, List.contains_iff_mem] at hc
    simpa [Expr.eval] using hsorted _ hc.1
  -- A sorted column that holds no NULL holds a value in each row.
  have hfull : ∀ t, (s.item i).kind = .table t → ∀ c, s.neverNull i c = true →
      ∀ r ∈ db.rows t, r.cell c ≠ none := by
    intro t ht c hc r hr
    simp only [Statement.neverNull, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq,
      List.contains_iff_mem] at hc
    rcases hc with ⟨rfl, _⟩ | hc
    · exact (hdb t r hr).1
    · exact (hcons i t ht).1 c hc r hr
  have hrowsa := (kept_rows hba).2 i hi
  have hrowsb := (kept_rows hbb).2 i hi
  unfold Statement.determines at h
  -- One binding holds a row of the item and the other none only where
  -- nothing the check asks for holds.
  have hmixed : ∀ x y : Bind, x ∈ coreBindings s db (evalQuery s db g) outer k →
      y ∈ coreBindings s db (evalQuery s db g) outer k → (∀ j ∈ known, x j = y j) →
      x i = none → ∀ r, y i = some r → s.onKnown k known i = true → False :=
    fun x y hx hy hxy hn r hr hon => onKnown_none hwf hk hx hy hxy hi hon hn hr
  cases hkind : (s.item i).kind with
  | table t =>
    rw [hkind] at h
    obtain ⟨key, hkey, hcheck⟩ := List.any_eq_true.mp h
    simp only [Bool.and_eq_true, List.all_eq_true] at hcheck
    obtain ⟨hcovers, hnulls⟩ := hcheck
    have hrow : ∀ r ∈ itemRows s db g outer k i, r ∈ db.rows t := by
      intro r hr
      simpa [itemRows, hkind] using hr
    have none_some : ∀ x y : Bind, x ∈ coreBindings s db (evalQuery s db g) outer k →
        y ∈ coreBindings s db (evalQuery s db g) outer k → (∀ j ∈ known, x j = y j) →
        (∀ c, s.sortedColumn sorted i c = true → cellOf (x i) c = cellOf (y i) c) →
        (∀ c, (fixed.contains (i, c) || s.innerEquated k known i c) = true →
          cellOf (x i) c = cellOf (y i) c ∧ cellOf (x i) c ≠ none) →
        x i = none → ∀ r, y i = some r → r ∈ db.rows t → False := by
      intro x y hx hy hxy hsort hanchor hn r hr hin
      have hl := hleft x hx hn
      simp only [hl, Bool.not_true, Bool.false_or, Bool.or_eq_true] at hnulls
      rcases hnulls with hany | hon
      · obtain ⟨c, _, hc⟩ := List.any_eq_true.mp hany
        unfold Statement.anchors at hc
        rw [Bool.or_assoc, Bool.or_eq_true] at hc
        rcases hc with hc | hc
        · have heq := hsort c hc
          simp only [Statement.sortedColumn, Bool.and_eq_true] at hc
          rw [hn, hr] at heq
          exact hfull t hkind c hc.2 r hin heq.symm
        · exact (hanchor c hc).2 (by rw [hn]; rfl)
      · exact hmixed x y hx hy hxy hn r hr hon
    have flip : ∀ c, (fixed.contains (i, c) || s.innerEquated k known i c) = true →
        cellOf (bb i) c = cellOf (ba i) c ∧ cellOf (bb i) c ≠ none := by
      intro c hc
      have := anchored_cell hwf hba hbb hknown hfixed hc
      exact ⟨this.1.symm, this.1 ▸ this.2⟩
    cases hja : ba i with
    | none =>
      cases hjb : bb i with
      | none => rfl
      | some rb =>
        exact (none_some ba bb hba hbb hknown hsame
          (fun c hc => anchored_cell hwf hba hbb hknown hfixed hc) hja rb hjb
          (hrow rb (hrowsb rb hjb))).elim
    | some ra =>
      cases hjb : bb i with
      | none =>
        exact (none_some bb ba hbb hba (fun j hj => (hknown j hj).symm)
          (fun c hc => (hsame c hc).symm) flip hjb ra hja (hrow ra (hrowsa ra hja))).elim
      | some rb =>
        have hra := hrow ra (hrowsa ra hja)
        have hrb := hrow rb (hrowsb rb hjb)
        have hcell : ∀ c ∈ key, ra.cell c = rb.cell c ∧ ra.cell c ≠ none := by
          intro c hc
          have hcov := hcovers c hc
          unfold Statement.covers at hcov
          rw [Bool.or_assoc, Bool.or_assoc, Bool.or_eq_true] at hcov
          rcases hcov with hs | hf
          · have heq := hsame c hs
            simp only [Statement.sortedColumn, Bool.and_eq_true] at hs
            rw [hja, hjb] at heq
            exact ⟨heq, hfull t hkind c hs.2 ra hra⟩
          · rw [← Bool.or_assoc] at hf
            have := fixed_cell hwf hk hba hbb hknown hfixed hi hja hjb hf
            rw [hja, hjb] at this
            exact this
        simp only [Statement.keysOf, List.mem_append] at hkey
        rcases hkey with hkey | hkey
        · split at hkey
          · simp only [List.mem_singleton] at hkey
            subst hkey
            rw [(hdb t ra hra).2 rb hrb (hcell 0 (by simp)).1]
          · cases hkey
        · rw [(hcons i t hkind).2 key hkey ra hra rb hrb hcell]
  | derived q' view =>
    rw [hkind] at h
    simp only [Bool.and_eq_true] at h
    obtain ⟨⟨_, hq'⟩, hnulls⟩ := h
    have none_some : ∀ x y : Bind, x ∈ coreBindings s db (evalQuery s db g) outer k →
        y ∈ coreBindings s db (evalQuery s db g) outer k → (∀ j ∈ known, x j = y j) →
        (∀ c, (fixed.contains (i, c) || s.innerEquated k known i c) = true →
          cellOf (x i) c = cellOf (y i) c ∧ cellOf (x i) c ≠ none) →
        x i = none → ∀ r, y i = some r → False := by
      intro x y hx hy hxy hanchor hn r hr
      have hl := hleft x hx hn
      simp only [hl, Bool.not_true, Bool.false_or, Bool.or_eq_true] at hnulls
      rcases hnulls with hany | hon
      · obtain ⟨c, _, hc⟩ := List.any_eq_true.mp hany
        have hc' : (fixed.contains (i, c) || s.innerEquated k known i c) = true := by
          simpa [Statement.anchors, Statement.sortedColumn] using hc
        exact (hanchor c hc').2 (by rw [hn]; rfl)
      · exact hmixed x y hx hy hxy hn r hr hon
    have flip : ∀ c, (fixed.contains (i, c) || s.innerEquated k known i c) = true →
        cellOf (bb i) c = cellOf (ba i) c ∧ cellOf (bb i) c ≠ none := by
      intro c hc
      have := anchored_cell hwf hba hbb hknown hfixed hc
      exact ⟨this.1.symm, this.1 ▸ this.2⟩
    cases hja : ba i with
    | none =>
      cases hjb : bb i with
      | none => rfl
      | some rb =>
        exact (none_some ba bb hba hbb hknown
          (fun c hc => anchored_cell hwf hba hbb hknown hfixed hc) hja rb hjb).elim
    | some ra =>
      cases hjb : bb i with
      | none =>
        exact (none_some bb ba hbb hba (fun j hj => (hknown j hj).symm) flip hjb ra hja).elim
      | some rb =>
        have hra := hrowsa ra hja
        have hrb := hrowsb rb hjb
        simp only [itemRows, hkind] at hra hrb
        congr 1
        refine hone q' _ _ hq' _ ra hra rb hrb (fun c hc => ?_) (fun c hc => ?_)
        · simp only [Statement.fixedColumns, List.mem_filter] at hc
          have := fixed_cell hwf hk hba hbb hknown hfixed hi hja hjb hc.2
          rw [hja, hjb] at this
          exact this
        · simp only [Statement.comparedColumns, List.mem_filter, List.contains_iff_mem] at hc
          have := hsorted _ hc.2
          simpa [Expr.eval, hja, hjb, cellOf] using this

/-- Each item the rounds add holds one row, or none, in two bindings, when
each step's items do. -/
theorem toldRounds_agree {determines : List Nat → Nat → Bool} {items : List Nat}
    {ba bb : Bind}
    (hstep : ∀ known, (∀ j ∈ known, ba j = bb j) → ∀ i ∈ items, determines known i = true →
      ba i = bb i) :
    ∀ rounds known, (∀ j ∈ known, ba j = bb j) →
      ∀ j ∈ toldRounds determines items rounds known, ba j = bb j
  | 0, _, h => h
  | rounds + 1, known, h => by
    unfold toldRounds
    apply toldRounds_agree hstep rounds
    intro j hj
    rcases List.mem_append.mp hj with hj | hj
    · exact h j hj
    · simp only [List.mem_filter, Bool.and_eq_true] at hj
      exact hstep known h j hj.1 hj.2.2

/-- Two bindings core `k` keeps that tie in `sorted` and hold `fixed`'s values
alike are one binding when each item of the core is determined. -/
theorem allTold_separates (hwf : s.wellFormed = true) (hdb : db.Keyed)
    (hcons : db.Constrained s) (hk : k ∈ (s.query q).arms) {one : Nat → List Nat → List Nat → Bool}
    (hone : OneSound s db g one) {sorted : List Expr} {fixed : List (Nat × Nat)}
    (hall : s.allTold one k sorted fixed = true) {ba bb : Bind}
    (hba : ba ∈ coreBindings s db (evalQuery s db g) outer k)
    (hbb : bb ∈ coreBindings s db (evalQuery s db g) outer k)
    {sa sb : Subs} (hsorted : ∀ e ∈ sorted, e.eval ba sa = e.eval bb sb)
    (hfixed : ∀ p ∈ fixed, cellOf (ba p.1) p.2 = cellOf (bb p.1) p.2 ∧ cellOf (ba p.1) p.2 ≠ none) :
    ba = bb := by
  funext j
  by_cases hj : j ∈ (s.core k).items
  · simp only [Statement.allTold, List.all_eq_true, List.contains_iff_mem] at hall
    exact toldRounds_agree (fun known hknown i hi h =>
      determines_agree hwf hdb hcons hk hone hba hbb hsorted hfixed hknown hi h)
      _ [] (fun _ h => by cases h) j (hall j hj)
  · rw [(kept_rows hba).1 j hj, (kept_rows hbb).1 j hj]

end Told

/-- A core that aggregates with no GROUP BY makes one row of all its
bindings. -/
theorem one_row {c : Core} (hagg : c.aggregated = true) (hgroup : c.groupBy.isEmpty = true)
    {subs : Bind → Subs} {kept : List Bind} {r : Row} (hr : r ∈ c.rows subs kept) :
    r = c.output.map (OutCol.aggregate subs kept) := by
  unfold Core.rows at hr
  simp only [hagg, hgroup, ↓reduceIte, List.map_cons, List.map_nil, List.mem_singleton] at hr
  exact hr

/-- A row of a core that calls no window function holds its binding's values
whatever its place among the bindings. -/
theorem plain_any_place {c : Core} (hwin : c.windowed = false) (eval : Bind → Nat → List Row)
    (kept : List Bind) (b : Bind) (j j' : Nat) :
    c.output.map (OutCol.plain eval kept b j) = c.output.map (OutCol.plain eval kept b j') := by
  apply List.map_congr_left
  intro o ho
  cases o with
  | rowNumber =>
    unfold Core.windowed at hwin
    rw [List.any_eq_false] at hwin
    exact absurd rfl (hwin _ ho)
  | _ => rfl

/-- The cell of a result column that passes an expression on holds the
expression's value. -/
theorem cell_passes {c : Core} {p : Nat} {e : Expr} (h : c.passes p = some e)
    (f : OutCol → Value) : Row.cell (c.output.map f) p = f (.expr e) := by
  unfold Core.passes at h
  split at h
  · rename_i e' he
    obtain rfl := Option.some.inj h
    simp [Row.cell, List.getElem?_map, he]
  · cases h

/-- `Statement.oneRow` keeps its promise: a query it says returns one row at
most for values of some columns returns no two rows that hold them alike. -/
theorem oneRow_sound {s : Statement} (hwf : s.wellFormed = true) {db : Db} (hdb : db.Keyed)
    (hcons : db.Constrained s) : ∀ f g, OneSound s db g (s.oneRow f)
  | 0, _ => fun _ _ _ h => by simp [Statement.oneRow] at h
  | f + 1, 0 => fun _ _ _ _ _ r hr => by simp [evalQuery] at hr
  | f + 1, g + 1 => by
    intro q columns compared h b r hr r' hr' hcells hties
    unfold Statement.oneRow at h
    split at h
    case h_2 => cases h
    rename_i k harms
    have hk : k ∈ (s.query q).arms := by rw [harms]; exact List.mem_singleton_self _
    have hcore : ∀ r ∈ evalQuery s db (g + 1) b q,
        r ∈ (s.core k).rows (evalQuery s db g) (coreBindings s db (evalQuery s db g) b k) := by
      intro r hr
      simp only [evalQuery, queryRows, harms, List.map_cons, List.map_nil, combineAll_single] at hr
      exact mem_result (mem_post hr)
    replace hr := hcore r hr
    replace hr' := hcore r' hr'
    simp only [Bool.or_eq_true, Bool.and_eq_true, Bool.not_eq_true'] at h
    rcases h with (⟨hagg, hgroup⟩ | ⟨hgroup, hterms⟩) | ⟨⟨hagg, hwin⟩, hall⟩
    · exact (one_row hagg hgroup hr).trans (one_row hagg hgroup hr').symm
    · have hagg : (s.core k).aggregated = true := by simp [Core.aggregated, hgroup]
      unfold Core.rows at hr hr'
      simp only [hagg, ↓reduceIte, hgroup, Bool.false_eq_true] at hr hr'
      obtain ⟨group, hg, rfl⟩ := List.mem_map.mp hr
      obtain ⟨group', hg', rfl⟩ := List.mem_map.mp hr'
      let key := fun b' => (s.core k).groupBy.map (Expr.eval b' (evalQuery s db g b'))
      simp only [groupsOf, List.mem_map] at hg hg'
      obtain ⟨v, _, rfl⟩ := hg
      obtain ⟨v', _, rfl⟩ := hg'
      -- Some GROUP BY term's cell holds a value, so each group's last
      -- binding gives each term the same one.
      have hne : (s.core k).groupBy ≠ [] := fun e => by simp [e] at hgroup
      have hterm : ∀ t ∈ (s.core k).groupBy, ∃ x x',
          x ∈ (coreBindings s db (evalQuery s db g) b k).filter (fun b' => key b' == v) ∧
          x' ∈ (coreBindings s db (evalQuery s db g) b k).filter (fun b' => key b' == v') ∧
          t.eval x (evalQuery s db g x) = t.eval x' (evalQuery s db g x') := by
        intro t ht
        have := List.all_eq_true.mp hterms t ht
        simp only [Bool.and_eq_true, List.any_eq_true, beq_iff_eq] at this
        obtain ⟨_, p, hp, hpass⟩ := this
        have hc := hcells p hp
        rw [cell_passes hpass, cell_passes hpass] at hc
        simp only [OutCol.aggregate] at hc
        cases hl : ((coreBindings s db (evalQuery s db g) b k).filter
            fun b' => key b' == v).getLast? with
        | none => rw [show (List.filter (fun b_1 => List.map (fun x => Expr.eval b_1
              (evalQuery s db g b_1) x) (s.core k).groupBy == v)
              (coreBindings s db (evalQuery s db g) b k)).getLast? = none from hl] at hc
                  exact absurd rfl hc.2
        | some x =>
          cases hl' : ((coreBindings s db (evalQuery s db g) b k).filter
              fun b' => key b' == v').getLast? with
          | none =>
            rw [show (List.filter (fun b_1 => List.map (fun x => Expr.eval b_1
              (evalQuery s db g b_1) x) (s.core k).groupBy == v)
              (coreBindings s db (evalQuery s db g) b k)).getLast? = some x from hl,
              show (List.filter (fun b_1 => List.map (fun x => Expr.eval b_1
              (evalQuery s db g b_1) x) (s.core k).groupBy == v')
              (coreBindings s db (evalQuery s db g) b k)).getLast? = none from hl'] at hc
            exact absurd hc.1 hc.2
          | some x' =>
            rw [show (List.filter (fun b_1 => List.map (fun x => Expr.eval b_1
              (evalQuery s db g b_1) x) (s.core k).groupBy == v)
              (coreBindings s db (evalQuery s db g) b k)).getLast? = some x from hl,
              show (List.filter (fun b_1 => List.map (fun x => Expr.eval b_1
              (evalQuery s db g b_1) x) (s.core k).groupBy == v')
              (coreBindings s db (evalQuery s db g) b k)).getLast? = some x' from hl'] at hc
            exact ⟨x, x', List.mem_of_getLast? hl, List.mem_of_getLast? hl', hc.1⟩
      have hkey : ∀ x ∈ (coreBindings s db (evalQuery s db g) b k).filter (fun b' => key b' == v),
          key x = v := fun x hx => by simpa using (List.mem_filter.mp hx).2
      have hkey' : ∀ x ∈ (coreBindings s db (evalQuery s db g) b k).filter (fun b' => key b' == v'),
          key x = v' := fun x hx => by simpa using (List.mem_filter.mp hx).2
      have hvv : v = v' := by
        obtain ⟨t0, ht0⟩ := List.exists_mem_of_ne_nil _ hne
        obtain ⟨x0, x0', hx0, hx0', _⟩ := hterm t0 ht0
        rw [← hkey x0 hx0, ← hkey' x0' hx0']
        apply List.map_congr_left
        intro t ht
        obtain ⟨x, x', hx, hx', heq⟩ := hterm t ht
        have at_ : ∀ (u w : List Value) (y z : Bind), key y = u → key z = u →
            t.eval y (evalQuery s db g y) = t.eval z (evalQuery s db g z) := by
          intro u _ y z hy hz
          exact List.map_inj_left.mp (hy.trans hz.symm) t ht
        rw [at_ v v x0 x (hkey x0 hx0) (hkey x hx), heq,
          at_ v' v' x' x0' (hkey' x' hx') (hkey' x0' hx0')]
      rw [hvv]
    · obtain ⟨ba, hba, ja, rfl⟩ := mem_rows_plain hagg hr
      obtain ⟨bb, hbb, jb, rfl⟩ := mem_rows_plain hagg hr'
      have hsame : ba = bb := by
        refine allTold_separates hwf hdb hcons hk (oneRow_sound hwf hdb hcons f g) hall hba hbb
          (sa := evalQuery s db g ba) (sb := evalQuery s db g bb) ?_ ?_
        · intro e he
          obtain ⟨⟨i, c⟩, hp, rfl⟩ := List.mem_map.mp he
          simp only [Statement.passedColumns, List.mem_filterMap] at hp
          obtain ⟨p, hpc, hp⟩ := hp
          split at hp
          · rename_i i' c' hpass
            split at hp
            · obtain ⟨rfl, rfl⟩ := Prod.mk.inj (Option.some.inj hp)
              have hc := hties p hpc
              rw [cell_passes hpass, cell_passes hpass] at hc
              simpa [OutCol.plain, Expr.eval] using hc
            · cases hp
          · cases hp
        intro (i, c) hp
        simp only [Statement.passedColumns, List.mem_filterMap] at hp
        obtain ⟨p, hpc, hp⟩ := hp
        split at hp
        · rename_i i' c' hpass
          split at hp
          · obtain ⟨rfl, rfl⟩ := Prod.mk.inj (Option.some.inj hp)
            have hc := hcells p hpc
            rw [cell_passes hpass, cell_passes hpass] at hc
            simpa [OutCol.plain, Expr.eval] using hc
          · cases hp
        · cases hp
      rw [hsame]
      exact plain_any_place hwin _ _ _ _ _

/-- Two bindings core `k` keeps that tie in expressions that tell its bindings
apart are one binding. -/
theorem tellsApart_separates {s : Statement} (hwf : s.wellFormed = true) {db : Db}
    (hdb : db.Keyed) (hcons : db.Constrained s) {q k g : Nat} (hk : k ∈ (s.query q).arms)
    {sorted : List Expr} (htells : s.tellsApart k sorted = true) {outer ba bb : Bind}
    (hba : ba ∈ coreBindings s db (evalQuery s db g) outer k)
    (hbb : bb ∈ coreBindings s db (evalQuery s db g) outer k)
    {sa sb : Subs} (hkey : ∀ e ∈ sorted, e.eval ba sa = e.eval bb sb) : ba = bb :=
  allTold_separates hwf hdb hcons hk (oneRow_sound hwf hdb hcons _ g) htells hba hbb hkey
    (fun _ h => by cases h)

end SqliveNarrowing.Pick
