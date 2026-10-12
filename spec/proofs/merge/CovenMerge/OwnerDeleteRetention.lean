import CovenMerge.OwnerDeleteRule

/-! A trial can withhold a current removal without losing any retained insert
or update. Earlier deletes remain generation evidence. This is separate from
actual rejection, which still excludes all changes using rejected evidence. -/
namespace CovenMerge.Ownership

variable {W Row Col Table Member : Type}
  [DecidableEq W] [DecidableEq Row] [DecidableEq Member]

private theorem round_spec (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (received : List W) (causal : CausalOrder M received) :
    IsSpec (DataDependency.effective M valid p (roundAdmission P candidates active))
      (fun w => w ∈ received) (roundState M valid p P candidates active received) :=
  isSpec_congr (fun _ => by simp)
    (Data.isSpec M valid p _ _ closed_empty St.init isSpec_init received
      (causalFrom_of_causalOrder causal)).1

omit [DecidableEq Row] in
/-- A delete is a data source only for the next insertion's generation.
Replaced-cell evidence always names an insert or update in the same incarnation. -/
theorem delete_source_is_generation (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (w a : W) (r : Row) (ch : Change Col)
    (change : M.chg w r = some ch) (source : a ∈ (p.provenance w r).sources) :
    ∃ ca, M.chg a r = some ca ∧
      (ca.kind ≠ .del ∨ (ch.kind = .ins ∧ ca.gen + 1 = ch.gen)) := by
  rcases List.mem_append.mp source with generation | setter
  · rcases p.generation_source w r ch change with ⟨_, absent⟩ | ⟨b, cb, origin, written, kind, gen⟩
    · simp [absent] at generation
    · have equal : a = b := by simpa [origin] using generation
      subst b
      refine ⟨cb, written, ?_⟩
      by_cases deleted : cb.kind = .del
      · have odd := kind_ne_ins_odd valid written (by simp [deleted])
        exact Or.inr ⟨(valid.parity w r ch change).mpr (by omega), gen⟩
      · exact Or.inl deleted
  · obtain ⟨⟨c, b⟩, mem, eq⟩ := List.mem_map.mp setter
    simp only at eq
    subst b
    obtain ⟨original, ca, written, caWritten, kind, _, _⟩ := p.setter_source w r c a mem
    exact ⟨ca, caWritten, Or.inl kind⟩

omit [DecidableEq W] [DecidableEq Row] in
private theorem retained_mono (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (a b : W → Row → Bool)
    (sub : ∀ w r, a w r = true → b w r = true) (w : W) (r : Row)
    (kept : DataDependency.retained (DataDependency.authoredState M valid p a) w r = true) :
    DataDependency.retained (DataDependency.authoredState M valid p b) w r = true := by
  obtain ⟨base, present, sources⟩ := (DataDependency.retained_iff _ w r).mp kept
  apply (DataDependency.retained_iff _ w r).mpr
  refine ⟨sub w r base, present, ?_⟩
  intro x hx
  exact retained_mono M valid p a b sub x r (sources x hx)
termination_by M.ts w
decreasing_by exact valid.past_ts w x (p.read_source w r x hx)

omit [DecidableEq W] [DecidableEq Row] in
theorem incarnation_context_subset (M : Writes W Row Col) (P : Policy W Row Table Member)
    (st : St W Row Col) (active : List (W × Row)) (w : W) :
    ∀ op ∈ incarnationContext M P st active w, op ∈ active := by
  intro op mem
  exact (List.mem_filter.mp mem).1

private theorem admission_subset (P : Policy W Row Table Member)
    (candidates a b : List (W × Row)) (sub : ∀ op ∈ a, op ∈ b)
    (w : W) (r : Row) (kept : roundAdmission P candidates a w r = true) :
    roundAdmission P candidates b w r = true := by
  simp only [roundAdmission, Bool.and_eq_true, Bool.or_eq_true, Bool.not_eq_true',
    decide_eq_false_iff_not, decide_eq_true_eq] at kept ⊢
  exact ⟨kept.1, kept.2.imp_right (sub (w, r))⟩

/-- Every retained insert or update survives withholding, as does each
earlier delete needed to reach a later retained incarnation. No condition on
the reference value, audience, or length of the provenance chain is needed. -/
theorem context_retains (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (received : List W) (causal : CausalOrder M received)
    (context : List (W × Row))
    (keeps : ∀ w r ch, M.chg w r = some ch →
      (ch.kind ≠ .del ∨ ch.gen + 1 < (roundState M valid p P candidates active received).gen r) →
      (w, r) ∈ active → (w, r) ∈ context) (w : W) (r : Row) (ch : Change Col) (mem : w ∈ received)
    (change : M.chg w r = some ch)
    (kept : DataDependency.retained
      (DataDependency.authoredState M valid p (roundAdmission P candidates active)) w r = true)
    (needed : ch.kind ≠ .del ∨ ch.gen + 1 < (roundState M valid p P candidates active received).gen r) :
    DataDependency.retained (DataDependency.authoredState M valid p (roundAdmission P candidates
      context)) w r = true := by
  obtain ⟨base, present, sources⟩ := (DataDependency.retained_iff _ w r).mp kept
  apply (DataDependency.retained_iff _ w r).mpr
  refine ⟨?_, present, ?_⟩
  · simp only [DataDependency.authoredState, roundAdmission, Bool.and_eq_true, Bool.or_eq_true,
      Bool.not_eq_true', decide_eq_false_iff_not, decide_eq_true_eq] at base ⊢
    refine ⟨base.1, base.2.imp_right fun member => ?_⟩
    exact keeps w r ch change needed member
  · intro a source
    have read := p.read_source w r a source
    have closed := (foldl_isSpec valid closed_empty isSpec_init (causalFrom_of_causalOrder causal)).2
    have seen : a ∈ received := (closed w (Or.inr mem) a read).resolve_left id
    obtain ⟨ca, written, dependency⟩ := delete_source_is_generation M valid p w a r ch change source
    have required : ca.kind ≠ .del ∨ ca.gen + 1 < (roundState M valid p P candidates active received).gen r := by
      rcases dependency with nondelete | ⟨insert, generation⟩
      · exact Or.inl nondelete
      · have upper := gen_ge (round_spec M valid p P candidates active received causal)
          (r := r) (x := w) (n := ch.gen + 1)
          ⟨mem, ch, by simp [DataDependency.effective, kept, change], by simp [insert], rfl⟩
        exact Or.inr (by omega)
    exact context_retains M valid p P candidates active received causal context keeps a r ca seen written
      (sources a source) required
termination_by M.ts w
decreasing_by exact valid.past_ts w a (p.read_source w r a source)

/-- Withholding cannot create a new admitted change. -/
theorem context_no_new_change (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active context : List (W × Row))
    (sub : ∀ op ∈ context, op ∈ active) (w : W) (r : Row)
    (kept : DataDependency.retained (DataDependency.authoredState M valid p (roundAdmission P candidates
      context)) w r = true) :
    DataDependency.retained (DataDependency.authoredState M valid p (roundAdmission P candidates active)) w r = true :=
  retained_mono M valid p _ _
    (admission_subset P candidates _ active sub) w r kept

/-- Exactly the same original setters are available, at every incarnation.
This includes a reference update after any number of delete/reinsert pairs. -/
theorem context_setters (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (received : List W) (causal : CausalOrder M received)
    (context : List (W × Row))
    (keeps : ∀ w r ch, M.chg w r = some ch →
      (ch.kind ≠ .del ∨ ch.gen + 1 < (roundState M valid p P candidates active received).gen r) →
      (w, r) ∈ active → (w, r) ∈ context)
    (sub : ∀ op ∈ context, op ∈ active) (w : W) (r : Row) (c : Col) (k : Nat) :
    Setter (DataDependency.effective M valid p (roundAdmission P candidates
      context))
        (fun a => a ∈ received) r c w k ↔
    Setter (DataDependency.effective M valid p (roundAdmission P candidates active))
        (fun a => a ∈ received) r c w k := by
  constructor
  · rintro ⟨mem, ch, change, kind, sets, inc⟩
    simp only [DataDependency.effective] at change
    split at change
    · rename_i kept
      have retained := context_no_new_change M valid p P candidates active context sub w r kept
      exact ⟨mem, ch, by simp [DataDependency.effective, retained, change], kind, sets, inc⟩
    · contradiction
  · rintro ⟨mem, ch, change, kind, sets, inc⟩
    simp only [DataDependency.effective] at change
    split at change
    · rename_i kept
      have retained := context_retains M valid p P candidates active received causal context keeps w r ch
        mem change kept (Or.inl kind)
      exact ⟨mem, ch, by simp [DataDependency.effective, retained, change], kind, sets, inc⟩
    · contradiction

/-- Withholding can restore the incarnation immediately before the last
removal; it cannot fall back through earlier delete/reinsert pairs. -/
theorem context_generations (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (received : List W) (causal : CausalOrder M received)
    (context : List (W × Row))
    (keeps : ∀ w r ch, M.chg w r = some ch →
      (ch.kind ≠ .del ∨ ch.gen + 1 < (roundState M valid p P candidates active received).gen r) →
      (w, r) ∈ active → (w, r) ∈ context)
    (sub : ∀ op ∈ context, op ∈ active) (r : Row) :
    (roundState M valid p P candidates context received).gen r ≤
      (roundState M valid p P candidates active received).gen r ∧
    (roundState M valid p P candidates active received).gen r ≤
      (roundState M valid p P candidates context received).gen r + 1 := by
  have original := round_spec M valid p P candidates active received causal
  have trial := round_spec M valid p P candidates context received causal
  change (roundState M valid p P candidates context received).gen r ≤
      (roundState M valid p P candidates active received).gen r ∧
    (roundState M valid p P candidates active received).gen r ≤
      (roundState M valid p P candidates context received).gen r + 1
  constructor
  ·
    by_cases zero : (roundState M valid p P candidates context received).gen r = 0
    · omega
    · obtain ⟨w, mem, ch, change, kind, gen⟩ := (trial.gen r).2 _ (by omega) (Nat.le_refl _)
      simp only [DataDependency.effective] at change
      split at change
      · rename_i kept
        have retained := context_no_new_change M valid p P candidates active context sub w r kept
        exact gen_ge original ⟨mem, ch, by simp [DataDependency.effective, retained, change], kind, gen⟩
      · contradiction
  · by_cases low : (roundState M valid p P candidates active received).gen r ≤ 1
    · omega
    · obtain ⟨w, mem, ch, change, kind, gen⟩ := (original.gen r).2
        ((roundState M valid p P candidates active received).gen r - 1) (by omega) (by omega)
      simp only [DataDependency.effective] at change
      split at change
      · rename_i kept
        have retained := context_retains M valid p P candidates active received causal context keeps w r ch
          mem change kept (Or.inr (by omega))
        have lower := gen_ge trial (r := r)
          ⟨mem, ch, by simp [DataDependency.effective, retained, change], kind, gen⟩
        omega
      · contradiction

/-- A row currently present keeps its actual incarnation and every winning
cell in the trial. Reference values are included; no old version can replace
them merely because a foreign removal is withheld. -/
theorem context_current (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (received : List W) (causal : CausalOrder M received)
    (context : List (W × Row))
    (keeps : ∀ w r ch, M.chg w r = some ch →
      (ch.kind ≠ .del ∨ ch.gen + 1 < (roundState M valid p P candidates active received).gen r) →
      (w, r) ∈ active → (w, r) ∈ context)
    (sub : ∀ op ∈ context, op ∈ active) (r : Row)
    (present : (roundState M valid p P candidates active received).gen r % 2 = 1) :
    (roundState M valid p P candidates context received).gen r =
      (roundState M valid p P candidates active received).gen r ∧
    ∀ c, (roundState M valid p P candidates context received).cell r c =
      (roundState M valid p P candidates active received).cell r c := by
  have original := round_spec M valid p P candidates active received causal
  have trial := round_spec M valid p P candidates context received causal
  have upper := (context_generations M valid p P candidates active received causal context keeps sub r).1
  change (roundState M valid p P candidates context received).gen r ≤
    (roundState M valid p P candidates active received).gen r at upper
  have equal : (roundState M valid p P candidates context received).gen r =
      (roundState M valid p P candidates active received).gen r := by
    obtain ⟨w, mem, ch, change, kind, gen⟩ := (original.gen r).2 _ (by omega) (Nat.le_refl _)
    simp only [DataDependency.effective] at change
    split at change
    · rename_i kept
      have nondelete : ch.kind ≠ .del := by
        intro deleted
        have odd := kind_ne_ins_odd valid change (by simp [deleted])
        omega
      have retained := context_retains M valid p P candidates active received causal context keeps w r ch
        mem change kept (Or.inl nondelete)
      have lower := gen_ge trial (r := r)
        ⟨mem, ch, by simp [DataDependency.effective, retained, change], kind, gen⟩
      omega
    · contradiction
  refine ⟨equal, fun c => ?_⟩
  apply option_eq_of_iff
  intro w
  rw [trial.cell_some, original.cell_some, equal]
  simp only [MaxSetter, context_setters M valid p P candidates active received causal context keeps sub]
  rfl

/-- The deleting member context satisfies the shared retention conditions. -/
theorem incarnation_context_retains (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (received : List W) (causal : CausalOrder M received)
    (actor w : W) (r : Row) (ch : Change Col) (mem : w ∈ received)
    (change : M.chg w r = some ch)
    (kept : DataDependency.retained
      (DataDependency.authoredState M valid p (roundAdmission P candidates active)) w r = true)
    (needed : ch.kind ≠ .del ∨ ch.gen + 1 < (roundState M valid p P candidates active received).gen r) :
    DataDependency.retained (DataDependency.authoredState M valid p (roundAdmission P candidates
      (incarnationContext M P (roundState M valid p P candidates active received) active actor))) w r = true :=
  context_retains M valid p P candidates active received causal _
    (fun a q ca written needed mem =>
      (incarnation_context_exact M P _ active actor a q ca written).mpr ⟨mem, Or.inl needed⟩) w r ch mem change kept needed

theorem incarnation_context_no_new_change (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (st : St W Row Col) (actor w : W) (r : Row)
    (kept : DataDependency.retained (DataDependency.authoredState M valid p (roundAdmission P candidates
      (incarnationContext M P st active actor))) w r = true) :
    DataDependency.retained (DataDependency.authoredState M valid p (roundAdmission P candidates active)) w r = true :=
  context_no_new_change M valid p P candidates active _
    (incarnation_context_subset M P st active actor) w r kept

theorem incarnation_context_setters (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (received : List W) (causal : CausalOrder M received)
    (actor w : W) (r : Row) (c : Col) (k : Nat) :
    Setter (DataDependency.effective M valid p (roundAdmission P candidates
      (incarnationContext M P (roundState M valid p P candidates active received) active actor)))
        (fun a => a ∈ received) r c w k ↔
    Setter (DataDependency.effective M valid p (roundAdmission P candidates active))
        (fun a => a ∈ received) r c w k :=
  context_setters M valid p P candidates active received causal _
    (fun a q ca written needed mem =>
      (incarnation_context_exact M P _ active actor a q ca written).mpr ⟨mem, Or.inl needed⟩)
    (incarnation_context_subset M P _ active actor) w r c k

theorem incarnation_context_generations (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (received : List W) (causal : CausalOrder M received)
    (actor : W) (r : Row) :
    let context := incarnationContext M P (roundState M valid p P candidates active received) active actor
    (roundState M valid p P candidates context received).gen r ≤
      (roundState M valid p P candidates active received).gen r ∧
    (roundState M valid p P candidates active received).gen r ≤
      (roundState M valid p P candidates context received).gen r + 1 :=
  context_generations M valid p P candidates active received causal _
    (fun a q ca written needed mem =>
      (incarnation_context_exact M P _ active actor a q ca written).mpr ⟨mem, Or.inl needed⟩)
    (incarnation_context_subset M P _ active actor) r

theorem incarnation_context_current (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (received : List W) (causal : CausalOrder M received)
    (actor : W) (r : Row)
    (present : (roundState M valid p P candidates active received).gen r % 2 = 1) :
    let context := incarnationContext M P (roundState M valid p P candidates active received) active actor
    (roundState M valid p P candidates context received).gen r =
      (roundState M valid p P candidates active received).gen r ∧
    ∀ c, (roundState M valid p P candidates context received).cell r c =
      (roundState M valid p P candidates active received).cell r c :=
  context_current M valid p P candidates active received causal _
    (fun a q ca written needed mem =>
      (incarnation_context_exact M P _ active actor a q ca written).mpr ⟨mem, Or.inl needed⟩)
    (incarnation_context_subset M P _ active actor) r present

private theorem before_delete_keeps (M : Writes W Row Col) (P : Policy W Row Table Member)
    (st : St W Row Col) (active : List (W × Row)) (w : W) (r : Row) (ch : Change Col)
    (change : M.chg w r = some ch) (deletes : ch.kind = .del)
    (a : W) (q : Row) (ca : Change Col) (written : M.chg a q = some ca)
    (needed : ca.kind ≠ .del ∨ ca.gen + 1 < st.gen q) (mem : (a, q) ∈ active) :
    (a, q) ∈ beforeDelete st (incarnationContext M P st active w) w r ch := by
  have selected := (incarnation_context_exact M P st active w a q ca written).mpr ⟨mem, Or.inl needed⟩
  unfold beforeDelete
  split
  · exact selected
  · rename_i current
    apply List.mem_filter.mpr
    refine ⟨selected, ?_⟩
    simp only [decide_eq_true_eq]
    intro equal
    have ⟨ha, hq⟩ := Prod.mk.inj equal
    subst a; subst q
    have same : ca = ch := Option.some.inj (written.symm.trans change)
    subst ca
    exact needed.elim (fun h => h deletes) current

private theorem before_delete_subset (M : Writes W Row Col) (P : Policy W Row Table Member)
    (st : St W Row Col) (active : List (W × Row)) (w : W) (r : Row) (ch : Change Col) :
    ∀ op ∈ beforeDelete st (incarnationContext M P st active w) w r ch, op ∈ active := by
  intro op mem
  apply incarnation_context_subset M P st active w op
  unfold beforeDelete at mem
  split at mem
  · exact mem
  · exact (List.mem_filter.mp mem).1

/-- The full-effects before view retains every present row's actual generation
and winning original setters, including later incarnations of the target. -/
theorem before_delete_current (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (candidates active : List (W × Row)) (received : List W) (causal : CausalOrder M received)
    (w : W) (r : Row) (ch : Change Col) (change : M.chg w r = some ch) (deletes : ch.kind = .del)
    (q : Row) (present : (roundState M valid p P candidates active received).gen q % 2 = 1) :
    let st := roundState M valid p P candidates active received
    let before := beforeDelete st (incarnationContext M P st active w) w r ch
    (roundState M valid p P candidates before received).gen q = st.gen q ∧
    ∀ c, (roundState M valid p P candidates before received).cell q c = st.cell q c :=
  context_current M valid p P candidates active received causal _
    (before_delete_keeps M P _ active w r ch change deletes)
    (before_delete_subset M P _ active w r ch) q present

end CovenMerge.Ownership
