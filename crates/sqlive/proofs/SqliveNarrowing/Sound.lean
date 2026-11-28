import SqliveNarrowing.Held

/-! The composed theorem. A write the judge lets pass leaves the rows of a
statement sqlive reads whole unchanged (`whole_narrowing_sound`). For any
other statement, what sqlive keeps of each table holds, before and after the
write, the same rows that pass each item's test (`plan_restriction`); every
binding that matters to a query's rows holds rows that pass their tests,
and a LEFT JOIN's item the planned state holds back has no row its ON holds
for that fails its test (`plan_kept`), so the two databases give every query
the same rows that matter (`agree_of_kept`). The statement's own rows all
matter, unless the LIMIT bound narrows its run to the rows that sort at or
before its last row, which are all the rows a full run returns
(`limit_restricted`). So the statement returns the same rows before and after
the write (`narrowing_sound`). -/
namespace SqliveNarrowing

attribute [local instance] Classical.propDecidable

/-- The planner's promise for the arms of query `q`, run on `X` or `Y` at
fuel `f` under a binding whose rows pass their tests, for the bindings `R`
says matter, when each matters to the query's test and, in the statement's
own core, sorts at or before the run's last row. -/
theorem plan_armsKept {s : Statement} {X Y : Db} {last : Option (List (List Value))} {f : Nat}
    (base : KeptBase s X Y last f) {Z : Db} (hZ : Z = X ∨ Z = Y) {q : Nat} (hq : q ≤ f) {o : Bind}
    (ho : s.Outer q o) (hoG : Passes (planTest s Y last) o)
    {R : Nat → (Bind → Nat → List Row) → Bind → Bool}
    (hR : ∀ k ∈ (s.query q).arms, ∀ b ∈ coreBindings s Z (evalQuery s Z f) o k,
      R k (evalQuery s Z f) b = true →
        relevant s (planRel s Y last) q k (evalQuery s Z f) b = true ∧
          (q = s.root → (plan s).AtOrBefore s last k (evalQuery s Z f) b) ∧
          (plan s).windowPasses s Y k b = true) :
    ArmsKept s Z (planTest s Y last) (planLeft s) R q o (evalQuery s Z f) := by
  intro k hk
  refine ⟨fun b hb hRb => ?_, fun i hi hl b hb hw hbd hbr r hr hon => ?_⟩
  · obtain ⟨hrel, hroot, hwin⟩ := hR k hk b hb hRb
    exact plan_restricted base hZ (Nat.le_refl f) hq ho hoG hk hb hrel hroot hwin
  · exact plan_left_passes base hZ hq ho hoG hk hi hl hb hw hbd hbr hr hon

/-- The planner's scopes keep their promise at fuel `f` on `X` and `Y`, when
the two databases return the same rows that matter at every fuel up to `f`. -/
theorem plan_kept {s : Statement} {X Y : Db} {last : Option (List (List Value))} {f : Nat}
    (base : KeptBase s X Y last f) {Z : Db} (hZ : Z = X ∨ Z = Y) :
    Kept s Z (planTest s Y last) (planLeft s) (planRel s Y last) ((plan s).windowPasses s Y) f :=
  fun _ hq hne _ ho hoG => plan_armsKept base hZ hq ho hoG fun _ _ _ _ hR => by
    simp only [Bool.and_eq_true] at hR
    exact ⟨hR.1, fun he => absurd he hne, hR.2⟩

/-! The LIMIT bound's run. -/

/-- A LIMIT bound is of a statement whose own query has a LIMIT, the
bound's, no OFFSET and an ORDER BY. -/
theorem Limit.sortedLimit_query {s : Statement} {st : PState} {sorted : Limit.SortedLimit}
    (h : Limit.sortedLimit s st = some sorted) :
    (s.query s.root).limit = some sorted.limit ∧ (s.query s.root).offset = none ∧
      (s.query s.root).orderBy ≠ [] := by
  unfold Limit.sortedLimit at h
  cases harms : (s.query s.root).arms with
  | nil => simp [harms] at h
  | cons k rest =>
    cases rest with
    | cons _ _ => simp [harms] at h
    | nil =>
      simp only [harms, Option.bind_eq_bind, Option.bind_eq_some_iff] at h
      obtain ⟨n, hn, h⟩ := h
      split at h
      · simp at h
      split at h
      · simp at h
      rename_i hcond _
      split at h
      · simp at h
      simp only [Option.pure_def, Option.bind_some, Option.some.injEq] at h
      subst h
      simp only [not_or] at hcond
      obtain ⟨h1, _, _, h4⟩ := hcond
      refine ⟨hn, ?_, fun he => h1 (by simp [he])⟩
      cases ho : (s.query s.root).offset with
      | none => rfl
      | some _ => simp [ho] at h4

/-- A run that returned as many rows as its LIMIT gives the LIMIT values of
its last row, run by run. -/
theorem Limit.lastValues_some {sorted : Limit.SortedLimit} {rows : List Row}
    {values : List (List Value)} (h : Limit.lastValues sorted rows = some values) :
    rows.length = sorted.limit ∧ ∃ r, rows.getLast? = some r ∧
      values = sorted.runs.map fun run => run.terms.map fun (_, _, p) => r.cell p := by
  unfold Limit.lastValues at h
  split at h
  · rename_i hlen
    cases hl : rows.getLast? with
    | none => simp [hl] at h
    | some r =>
      simp only [hl, Option.map_some, Option.some.injEq] at h
      exact ⟨hlen, r, rfl, h.symm⟩
  · cases h

/-- Each row of a statement whose own query is one SELECT that neither
aggregates nor calls a window function is the core's row of a binding. -/
theorem Statement.result_outRow {s : Statement} {db : Db} {k : Nat}
    (harms : (s.query s.root).arms = [k]) (hagg : (s.core k).aggregated = false)
    (hwin : (s.core k).windowed = false) {r : Row} (hr : r ∈ s.result db) :
    ∃ (E : Bind → Nat → List Row) (b : Bind), r = (s.core k).outRow E b := by
  unfold Statement.result at hr
  simp only [evalQuery, queryRows, harms, List.map_cons, List.map_nil, combineAll_single] at hr
  have := mem_result (mem_post hr)
  rw [Core.rows_plain hagg hwin] at this
  obtain ⟨b, _, rfl⟩ := List.mem_map.mp this
  exact ⟨_, b, rfl⟩

/-! The theorem. -/

/-- Narrowing's soundness: for a well-formed statement, a write the judge
lets pass, with the scopes sqlive's planner gives the run that read `db`,
leaves the statement's rows unchanged. -/
theorem narrowing_sound {s : Statement} (hwf : s.wellFormed = true) {db db' : Db}
    {changes : List Change} (hw : Writes db changes db') (hr : s.reruns db changes db' = false) :
    s.result db' = s.result db := by
  cases hwhole : Whole.readsWhole s
  · have hitems := fun k i hi => s.wellFormed_items hwf (k := k) (i := i) hi
    rw [← result_mask hitems db, ← result_mask hitems db']
    obtain ⟨last, hlast⟩ : ∃ last, (plan s).lastOf s db = last := ⟨_, rfl⟩
    have hR : Restriction s (db.mask s.reads) (db'.mask s.reads) (planTest s (db'.mask s.reads) last)
        (planRel s (db'.mask s.reads) last) ((plan s).windowPasses s (db'.mask s.reads)) := by
      have := plan_restriction hwf hw hr hwhole
      dsimp only at this
      rw [hlast] at this
      exact this
    have hLeft : ∀ j, planLeft s j → s.alwaysBound j = false := fun j hj => plan_leftOn s hj
    have hscopes : s.tableScopes db = some (tableScope s ((plan s).itemScopes last)) := by
      rw [← hlast]
      simp [Statement.tableScopes, hwhole, Planned.lastOf]
    have htab : ∀ i t, s.table? i = some t → i < s.items.length →
        ((db.mask s.reads).rows t).filter
            ((plan s).scopeTest ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s (db'.mask s.reads)) i) =
          ((db'.mask s.reads).rows t).filter
            ((plan s).scopeTest ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s (db'.mask s.reads)) i) :=
      fun i t ht hi => scoped_rows hw hr hscopes hi ht
        (G := (plan s).scopeTest ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s (db'.mask s.reads)) i)
        (fun r hg => by
          simp only [Planned.scopeTest, Bool.and_eq_true] at hg
          simp only [Planned.itemScopes, Limit.passes_both, Bool.and_eq_true]
          exact hg.1)
    have hkept : ∀ f, (∀ f' ≤ f, Agree s (db.mask s.reads) (db'.mask s.reads)
        (planTest s (db'.mask s.reads) last) (planRel s (db'.mask s.reads) last) f') →
        Kept s (db.mask s.reads) (planTest s (db'.mask s.reads) last) (planLeft s)
          (planRel s (db'.mask s.reads) last) ((plan s).windowPasses s (db'.mask s.reads)) f ∧
        Kept s (db'.mask s.reads) (planTest s (db'.mask s.reads) last) (planLeft s)
          (planRel s (db'.mask s.reads) last) ((plan s).windowPasses s (db'.mask s.reads)) f :=
      fun f hagree => ⟨plan_kept ⟨hwf, htab, hagree⟩ (Or.inl rfl), plan_kept ⟨hwf, htab, hagree⟩ (Or.inr rfl)⟩
    have hagree := agree_of_kept hR hLeft hkept
    have base : KeptBase s (db.mask s.reads) (db'.mask s.reads) last s.queries.length :=
      ⟨hwf, htab, fun g _ => hagree g⟩
    have hrootLe : s.root ≤ s.queries.length := Nat.le_of_lt (s.wellFormed_parts hwf).root
    have hrel : ∀ k E b, relevant s (planRel s (db'.mask s.reads) last) s.root k E b = true :=
      fun _ _ _ => relevant_of_all (fun r => queryTest_root hwf _ _ _ _ r) _ _
    cases last with
    | none =>
      have hroot : ∀ Z, (Z = db.mask s.reads ∨ Z = db'.mask s.reads) →
          ArmsKept s Z (planTest s (db'.mask s.reads) none) (planLeft s)
            (relevant s (planRel s (db'.mask s.reads) none) s.root) s.root Bind.empty
            (evalQuery s Z s.queries.length) :=
        fun Z hZ => plan_armsKept base hZ hrootLe (s.outer_root hwf) (Passes.empty _)
          fun k hk b _ _ => ⟨hrel k _ b, (fun _ _ _ _ hl => by cases hl), windowPasses_root hwf hk _ b⟩
      exact (result_restricted hR hLeft (fun r => queryTest_root hwf _ _ _ _ r) hkept
        (hroot _ (Or.inl rfl)) (hroot _ (Or.inr rfl))).symm
    | some values =>
      simp only [Planned.lastOf, Option.bind_eq_some_iff] at hlast
      obtain ⟨sorted, hs, hlv⟩ := hlast
      have hs' := hs
      rw [plan_sorted] at hs'
      obtain ⟨k0, _, harms, hagg, hwin, _⟩ := Limit.sortedLimit_facts hs'
      obtain ⟨hlimit, hoffset, horder⟩ := Limit.sortedLimit_query hs'
      obtain ⟨hlen, lastRow, hget, hvals⟩ := Limit.lastValues_some hlv
      obtain ⟨E', b', hrow⟩ := Statement.result_outRow harms hagg hwin (List.mem_of_getLast? hget)
      have hroot : ∀ Z, (Z = db.mask s.reads ∨ Z = db'.mask s.reads) →
          ArmsKept s Z (planTest s (db'.mask s.reads) (some values)) (planLeft s)
            (relevant s (fun _ r => keyLe ((s.query s.root).orderBy.map (·.2))
              (Limit.orderKey (s.query s.root) r) (Limit.orderKey (s.query s.root) lastRow)) s.root)
            s.root Bind.empty (evalQuery s Z s.queries.length) := by
        intro Z hZ
        refine plan_armsKept base hZ hrootLe (s.outer_root hwf) (Passes.empty _)
          fun k hk b _ hRb => ⟨hrel k _ b, fun _ sorted' values' hs'' hl => ?_, windowPasses_root hwf hk _ b⟩
        rw [harms, List.mem_singleton] at hk
        subst hk
        rw [hs] at hs''
        obtain rfl := Option.some.inj hs''
        obtain rfl := Option.some.inj hl
        refine ⟨E', b', ?_, ?_⟩
        · rw [hvals, hrow]
        · unfold relevant at hRb
          simp only [hwin, hagg, Bool.false_or, Bool.false_eq_true, ↓reduceIte] at hRb
          rw [← hrow]
          exact hRb
      exact limit_restricted hR hLeft hlimit hoffset horder
        (by rw [result_mask hitems db]; exact hlen)
        (by rw [result_mask hitems db, hget]; rfl) hkept (hroot _ (Or.inl rfl)) (hroot _ (Or.inr rfl))
  · exact whole_narrowing_sound hwf hwhole hw hr

/-- The same for sqlive's judge, which narrows nothing for a write whose
question stopped at the step limit. -/
theorem narrowing_sound_limited {s : Statement} (hwf : s.wellFormed = true) {db db' : Db}
    {changes : List Change} {stopped : Bool} (hw : Writes db changes db')
    (hr : s.rerunsLimited db changes db' stopped = false) : s.result db' = s.result db :=
  narrowing_sound hwf hw (Statement.rerunsLimited_sound hr)

end SqliveNarrowing
