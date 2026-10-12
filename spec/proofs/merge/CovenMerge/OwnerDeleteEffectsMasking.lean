import CovenMerge.OwnerDeleteEffectsExamples

namespace CovenMerge.Ownership.DeleteEffectsMasking

open DeleteRuleExamples (Row graph nullUniqueInputs)

inductive Write where
  | create | rival | leftDelete | rightDelete
  deriving DecidableEq, Repr

open Write

/-- Ben creates parent and child in one write; Ana creates the rival.
Two Ben devices delete the parent concurrently, each having read Ben's
creation but neither Ana's rival nor the other delete. -/
def history : Writes Write Row Unit where
  ts | .create => 0 | .rival => 1 | .leftDelete => 2 | .rightDelete => 3
  past w a := decide ((w = .leftDelete ∨ w = .rightDelete) ∧ a = .create)
  chg
    | .create, .parent | .create, .child | .rival, .rival => some ⟨.ins, 0, fun _ => true⟩
    | .leftDelete, .parent | .rightDelete, .parent => some ⟨.del, 1, fun _ => false⟩
    | _, _ => none

theorem valid : Valid history where
  ts_inj a b := by cases a <;> cases b <;> simp [history]
  past_ts w a := by cases w <;> cases a <;> decide
  gen_seen w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    · exact Or.inl rfl
    · exact Or.inl rfl
    · exact Or.inl rfl
    all_goals exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
  parity w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch; decide

def provenance : DataDependency.Authored history where
  provenance
    | .leftDelete, .parent | .rightDelete, .parent => ⟨some .create, [((), .create)]⟩
    | _, _ => ⟨none, []⟩
  read_source w r a h := by
    cases w <;> cases r <;> cases a <;> simp_all [DataDependency.Provenance.sources, history]
  generation_source w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    · exact Or.inl ⟨rfl, rfl⟩
    · exact Or.inl ⟨rfl, rfl⟩
    · exact Or.inl ⟨rfl, rfl⟩
    all_goals exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
  setter_source w r c a h := by
    cases w <;> cases r <;> cases a <;> simp at h
    all_goals exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

def policy : Policy Write Row Bool Bool where
  table r := r == .parent
  owned _ := true
  creator | .parent | .child => .create | .rival => .rival
  author w := w == .rival

def inputs (st : St Write Row Unit) : EntryInputs Row Nat := nullUniqueInputs st 0 1

def trial := deleteTrial history policy graph inputs

def candidates : List (Write × Row) := [(.leftDelete, .parent), (.rightDelete, .parent)]

def values (w : Write) (r : Row) (c : Unit) : Option Write :=
  if (history.chg w r).any (fun ch => ch.sets c) then some w else none

def kept (admission : Write → Row → Bool) : Write → Row → Bool
  | .create, .parent => admission .create .parent
  | .create, .child => admission .create .child
  | .rival, .rival => admission .rival .rival
  | .leftDelete, .parent => admission .leftDelete .parent && admission .create .parent
  | .rightDelete, .parent => admission .rightDelete .parent && admission .create .parent
  | _, _ => false

theorem retained (admission : Write → Row → Bool) (w : Write) (r : Row) :
    DataDependency.retained (DataDependency.authoredState history valid provenance admission) w r =
      kept admission w r := by
  have parent : DataDependency.retained
      (DataDependency.authoredState history valid provenance admission) .create .parent = admission .create .parent := by
    rw [DataDependency.retained_eq]
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources]
  cases w <;> cases r <;> rw [DataDependency.retained_eq] <;>
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources, kept] at *
  all_goals rw [parent]

def received : List Write := [.create, .rival, .leftDelete, .rightDelete]

theorem causal : CausalOrder history received := by
  apply causalOrder_of_ts_sorted valid
  · intro w mem a read
    cases w <;> cases a <;> simp_all [history, received]
  · decide
  · decide

def rebuilt (active : List (Write × Row)) := roundState history valid provenance policy candidates active received

theorem each_delete_masks_the_other :
    (entryView (inputs (rebuilt []))).shown .rival = true ∧
    (entryView (inputs (rebuilt [(.leftDelete, .parent)]))).shown .rival = false ∧
    (entryView (inputs (rebuilt [(.rightDelete, .parent)]))).shown .rival = false ∧
    roundTrial history valid provenance policy candidates trial received candidates (.leftDelete, .parent) = none ∧
    roundTrial history valid provenance policy candidates trial received candidates (.rightDelete, .parent) = none := by
  unfold roundTrial rebuilt roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

theorem guards_empty : settledGuard history valid provenance policy candidates trial received = fun _ _ => none := by
  funext w r
  obtain ⟨_, _, _, left, right⟩ := each_delete_masks_the_other
  simp only [candidates] at left right
  cases w <;> cases r <;> rw [settledGuard, rejectionReason.eq_def] <;>
    simp [candidates, left, right, keepRound]

def evaluated := protectedResult history valid provenance policy candidates trial values inputs received

/-- Both completed-view comparisons pass. The installed result nevertheless
hides Ana's row and records only its ordinary unique loss. -/
theorem foreign_row_hidden :
    evaluated.1.view.shown .child = true ∧ evaluated.1.view.shown .rival = false ∧
    evaluated.2 .leftDelete .parent = none ∧ evaluated.2 .rightDelete .parent = none ∧
    (evaluated.1.losses .rival none).map (fun record => record.cause) = some (.rules [.unique]) ∧
    (evaluated.1.losses .rival none).map (fun record => record.values ()) = some (some .rival) := by
  unfold evaluated protectedResult result
  rw [guards_empty]
  unfold merged Data.merged
  simp only [DataDependency.effective, retained, loss, DataDependency.dependsOnDropped]
  decide

example : evaluated.1.view.shown .rival = false := foreign_row_hidden.2.1

theorem final_owner_protection_refuted :
    ¬ (∀ r, owner policy r ≠ policy.author .leftDelete →
      (entryView (inputs (rebuilt []))).shown r = true → evaluated.1.view.shown r = true) := by
  intro protects
  have visible := protects .rival (by decide) each_delete_masks_the_other.1
  rw [foreign_row_hidden.2.1] at visible
  contradiction

/-- Either deletion alone is rejected by the full-effects trial. Removing the
rival claim also removes this loss. Both candidates are required for masking. -/
theorem reductions :
    roundTrial history valid provenance policy candidates trial received [(.leftDelete, .parent)]
      (.leftDelete, .parent) = some (.delete .rival true) ∧
    roundTrial history valid provenance policy candidates trial received [(.rightDelete, .parent)]
      (.rightDelete, .parent) = some (.delete .rival true) ∧
    (entryView ({ inputs (rebuilt candidates) with claims := fun _ => [] } : EntryInputs Row Nat)).shown .rival = true := by
  unfold roundTrial rebuilt roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

end CovenMerge.Ownership.DeleteEffectsMasking
