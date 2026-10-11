import CovenMerge.OwnerExamples

namespace CovenMerge.Ownership.ReinsertExamples

inductive Write where
  | create | delete | reinsert | child
  deriving DecidableEq, Repr

open Write

/-- Ben deletes and reinserts his parent before Ana inserts a child referring
to the new generation. The old deletion must survive to support reinsertion. -/
def history : Writes Write Bool Unit where
  ts | .create => 0 | .delete => 1 | .reinsert => 2 | .child => 3
  past
    | .delete, .create | .reinsert, .create | .reinsert, .delete
    | .child, .create | .child, .delete | .child, .reinsert => true
    | _, _ => false
  chg
    | .create, false | .child, true => some ⟨.ins, 0, fun _ => true⟩
    | .delete, false => some ⟨.del, 1, fun _ => false⟩
    | .reinsert, false => some ⟨.ins, 2, fun _ => true⟩
    | _, _ => none

theorem valid : Valid history where
  ts_inj a b := by cases a <;> cases b <;> simp [history]
  past_ts w a := by cases w <;> cases a <;> decide
  gen_seen w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    · exact Or.inl rfl
    · exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
    · exact Or.inr ⟨.delete, _, rfl, rfl, by decide, rfl⟩
    · exact Or.inl rfl
  parity w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch; decide

def provenance : DataDependency.Authored history where
  provenance
    | .delete, false => ⟨some .create, [((), .create)]⟩
    | .reinsert, false => ⟨some .delete, []⟩
    | _, _ => ⟨none, []⟩
  read_source w r a h := by
    cases w <;> cases r <;> cases a <;> simp_all [DataDependency.Provenance.sources, history]
  generation_source w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    · exact Or.inl ⟨rfl, rfl⟩
    · exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
    · exact Or.inr ⟨.delete, _, rfl, rfl, by decide, rfl⟩
    · exact Or.inl ⟨rfl, rfl⟩
  setter_source w r c a h := by
    cases w <;> cases r <;> cases a <;> simp at h
    exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

def policy : Policy Write Bool Unit Bool where
  table _ := ()
  owned _ := true
  creator r := if r then .child else .create
  author w := w == .child

def inputs (st : St Write Bool Unit) : EntryInputs Bool Unit where
  rows := [false, true]
  present r := decide (st.gen r % 2 = 1)
  refs r := if r then [⟨false, decide (st.gen false ≠ 3)⟩] else []
  checkFails _ := false
  inDeletedCircle _ := none
  claims _ := []
  rank r := if r then 1 else 0

def trial := deleteTrial policy [false, true] inputs (fun w _ => w == .delete)
def candidates : List (Write × Bool) := [(.delete, false)]
def received : List Write := [.create, .delete, .reinsert, .child]

theorem causal : CausalOrder history received := by
  apply CausalOrder.snoc (M := history) (L := [.create, .delete, .reinsert]) (w := .child)
  · apply CausalOrder.snoc (M := history) (L := [.create, .delete]) (w := .reinsert)
    · apply CausalOrder.snoc (M := history) (L := [.create]) (w := .delete)
      · apply CausalOrder.snoc (M := history) (L := []) (w := .create) .nil <;> simp [history]
      · decide
      · intro a h; cases a <;> simp_all [history]
    · decide
    · intro a h; cases a <;> simp_all [history]
  · decide
  · intro a h; cases a <;> simp_all [history]

def kept (admission : Write → Bool → Bool) : Write → Bool → Bool
  | .create, false => admission .create false
  | .delete, false => admission .delete false && admission .create false
  | .reinsert, false => admission .reinsert false && (admission .delete false && admission .create false)
  | .child, true => admission .child true
  | _, _ => false

theorem retained (admission : Write → Bool → Bool) (w : Write) (r : Bool) :
    DataDependency.retained (DataDependency.authoredState history valid provenance admission) w r =
      kept admission w r := by
  have create : DataDependency.retained
      (DataDependency.authoredState history valid provenance admission) .create false = admission .create false := by
    rw [DataDependency.retained_eq]
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources]
  have deletion : DataDependency.retained
      (DataDependency.authoredState history valid provenance admission) .delete false =
      (admission .delete false && admission .create false) := by
    rw [DataDependency.retained_eq]
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources] at *
    rw [create]
  cases w <;> cases r <;> rw [DataDependency.retained_eq] <;>
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources, kept] at *
  · rw [create]
  · rw [deletion]

def rebuilt (active : List (Write × Bool)) := roundState history valid provenance policy candidates active received

/-- Rejecting the old delete drops its dependent reinsertion. Ana's child
then refers to an unavailable generation and is removed by the real passes. -/
theorem old_delete_supports_child :
    (rebuilt candidates).gen false = 3 ∧ (view (inputs (rebuilt candidates)).erase).shown true = true ∧
    (rebuilt []).gen false = 1 ∧ (view (inputs (rebuilt [])).erase).shown true = false := by
  unfold rebuilt roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

/-- Ignoring generations would reject the old deletion even though keeping
it is necessary for Ana's current child to remain visible. -/
theorem row_key_guard_rejects_valid_history :
    Examples.rowKeyReferenceDenied (inputs (rebuilt candidates)).erase
      (fun r => !r) (fun r => some r) false = true := by
  unfold rebuilt roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

theorem old_delete_allowed :
    roundTrial history valid provenance policy candidates trial received candidates (.delete, false) = none := by
  unfold roundTrial roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

def evaluated := protectedResult history valid provenance policy candidates trial
  (fun w _ _ => some w) inputs received

theorem generations_preserved :
    evaluated.1.merged.gen false = 3 ∧ evaluated.1.view.shown true = true ∧
    evaluated.2 .delete false = none ∧ evaluated.2 .reinsert false = none := by
  have guards : settledGuard history valid provenance policy candidates trial received = fun _ _ => none := by
    funext w r
    have safe := old_delete_allowed
    simp only [candidates] at safe
    cases w <;> cases r <;> rw [settledGuard, rejectionReason.eq_def] <;>
      simp [candidates, safe, keepRound]
  unfold evaluated protectedResult result
  rw [guards]
  simp only [merged, Data.merged, DataDependency.effective, loss, DataDependency.dependsOnDropped, retained]
  decide

example : evaluated.1.view.shown true = true := generations_preserved.2.1

end CovenMerge.Ownership.ReinsertExamples
