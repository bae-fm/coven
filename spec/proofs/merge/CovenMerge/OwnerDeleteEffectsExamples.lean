import CovenMerge.OwnerDeleteRuleExamples

namespace CovenMerge.Ownership.DeleteEffectsExamples

open DeleteRuleExamples

theorem unique_delete_blocked :
    roundTrial history valid provenance policy candidates
      (deleteTrial history policy graph inputs) received candidates (.erase, .parent) =
        some (.delete .rival true) := by
  unfold roundTrial roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

def trial := deleteTrial history policy graph inputs

theorem guard_reason (w : Write) (r : Row) :
    settledGuard history valid provenance policy candidates trial received w r =
      if w = .erase ∧ r = .parent then some (.delete .rival true) else none := by
  have blocked := unique_delete_blocked
  simp only [candidates] at blocked
  cases w <;> cases r <;> rw [settledGuard, rejectionReason.eq_def] <;>
    simp [candidates, trial, blocked]

def values (w : Write) (r : Row) (c : Unit) : Option Write :=
  if (history.chg w r).any (fun ch => ch.sets c) then some w else none

def evaluated := protectedResult history valid provenance policy candidates trial values inputs received

/-- The set-null contest is evaluated before installing any result. Ben's
original empty deletion becomes a loss naming Ana's rival, and all rows show. -/
theorem unique_delete_recorded :
    evaluated.1.view.shown .parent = true ∧ evaluated.1.view.shown .child = true ∧
    evaluated.1.view.shown .rival = true ∧ evaluated.1.losses .rival none = none ∧
    (evaluated.2 .erase .parent).map (fun record => record.cause) = some (.delete .rival true) ∧
    (evaluated.2 .erase .parent).map (fun record => (record.write, record.row, record.generation, record.values ())) =
      some (.erase, .parent, 1, none) := by
  have guards : settledGuard history valid provenance policy candidates trial received =
      fun w r => if w = .erase ∧ r = .parent then some (.delete .rival true) else none := by
    funext w r; exact guard_reason w r
  unfold evaluated protectedResult result
  rw [guards]
  unfold merged Data.merged
  simp only [DataDependency.effective, retained, loss, DataDependency.dependsOnDropped]
  decide

example : evaluated.1.view.shown .rival = true := unique_delete_recorded.2.2.1

end CovenMerge.Ownership.DeleteEffectsExamples
