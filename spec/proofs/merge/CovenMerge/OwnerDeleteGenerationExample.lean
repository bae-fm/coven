import CovenMerge.OwnerDeleteRule
import CovenMerge.OwnerDeleteContexts

/-! Withholding a foreign delete also excludes its dependent reinsertion.
The restored old incarnation can have different references from the final
new incarnation, concealing the row the delete rule needs to protect. -/
namespace CovenMerge.Ownership.DeleteGenerationExample

inductive Write where
  | parent | oldChild | childDelete | newChild | parentDelete
  deriving DecidableEq, Repr

/-- False is Ben's parent; true is Ana's child. Its old incarnation has no
parent. Ana deletes and reinserts it with a parent reference, concurrently
with Ben's parent deletion. -/
def history : Writes Write Bool Unit where
  ts | .parent => 0 | .oldChild => 1 | .childDelete => 2 | .newChild => 3 | .parentDelete => 4
  past
    | .childDelete, .oldChild | .newChild, .parent | .newChild, .oldChild
    | .newChild, .childDelete | .parentDelete, .parent => true
    | _, _ => false
  chg
    | .parent, false | .oldChild, true => some ⟨.ins, 0, fun _ => true⟩
    | .childDelete, true | .parentDelete, false => some ⟨.del, 1, fun _ => false⟩
    | .newChild, true => some ⟨.ins, 2, fun _ => true⟩
    | _, _ => none

theorem valid : Valid history where
  ts_inj a b := by cases a <;> cases b <;> simp [history]
  past_ts w a := by cases w <;> cases a <;> decide
  gen_seen w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    · exact Or.inl rfl
    · exact Or.inl rfl
    · exact Or.inr ⟨.oldChild, _, rfl, rfl, by decide, rfl⟩
    · exact Or.inr ⟨.childDelete, _, rfl, rfl, by decide, rfl⟩
    · exact Or.inr ⟨.parent, _, rfl, rfl, by decide, rfl⟩
  parity w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch; decide

def provenance : DataDependency.Authored history where
  provenance
    | .childDelete, true => ⟨some .oldChild, [((), .oldChild)]⟩
    | .newChild, true => ⟨some .childDelete, []⟩
    | .parentDelete, false => ⟨some .parent, [((), .parent)]⟩
    | _, _ => ⟨none, []⟩
  read_source w r a h := by
    cases w <;> cases r <;> cases a <;> simp_all [DataDependency.Provenance.sources, history]
  generation_source w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    · exact Or.inl ⟨rfl, rfl⟩
    · exact Or.inl ⟨rfl, rfl⟩
    · exact Or.inr ⟨.oldChild, _, rfl, rfl, by decide, rfl⟩
    · exact Or.inr ⟨.childDelete, _, rfl, rfl, by decide, rfl⟩
    · exact Or.inr ⟨.parent, _, rfl, rfl, by decide, rfl⟩
  setter_source w r c a h := by
    cases w <;> cases r <;> cases a <;> simp at h
    all_goals exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

def policy : Policy Write Bool Unit Bool where
  table _ := ()
  owned _ := true
  creator r := if r then .oldChild else .parent
  author w := decide (w = .oldChild ∨ w = .childDelete ∨ w = .newChild)

def inputs (st : St Write Bool Unit) : EntryInputs Bool Unit where
  rows := [false, true]
  present r := decide (st.gen r % 2 = 1)
  refs r := if r ∧ st.gen true = 3 then [⟨false, decide (2 ≤ st.gen false)⟩] else []
  checkFails _ := false
  inDeletedCircle _ := none
  claims _ := []
  rank r := if r then 1 else 0

def graph (st : St Write Bool Unit) : ReferenceGraph (Bool × Nat) where
  rows := [(false, st.gen false), (true, st.gen true)]
  present node := decide (st.gen node.1 % 2 = 1)
  parents node := (inputs st).refs node.1 |>.map fun ref => (ref.parent, 1)

def candidates : List (Write × Bool) := [(.childDelete, true), (.parentDelete, false)]
def received : List Write := [.parent, .oldChild, .childDelete, .newChild, .parentDelete]
def trial := deleteTrial history policy graph

def kept (admission : Write → Bool → Bool) : Write → Bool → Bool
  | .parent, false => admission .parent false
  | .oldChild, true => admission .oldChild true
  | .childDelete, true => admission .childDelete true && admission .oldChild true
  | .newChild, true => admission .newChild true && (admission .childDelete true && admission .oldChild true)
  | .parentDelete, false => admission .parentDelete false && admission .parent false
  | _, _ => false

theorem retained (admission : Write → Bool → Bool) (w : Write) (r : Bool) :
    DataDependency.retained (DataDependency.authoredState history valid provenance admission) w r =
      kept admission w r := by
  have parent : DataDependency.retained
      (DataDependency.authoredState history valid provenance admission) .parent false = admission .parent false := by
    rw [DataDependency.retained_eq]
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources]
  have oldChild : DataDependency.retained
      (DataDependency.authoredState history valid provenance admission) .oldChild true = admission .oldChild true := by
    rw [DataDependency.retained_eq]
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources]
  have childDelete : DataDependency.retained
      (DataDependency.authoredState history valid provenance admission) .childDelete true =
        (admission .childDelete true && admission .oldChild true) := by
    rw [DataDependency.retained_eq]
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources] at *
    rw [oldChild]
  cases w <;> cases r <;> rw [DataDependency.retained_eq] <;>
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources, kept] at *
  · rw [oldChild]
  · rw [childDelete]
  · rw [parent]

theorem causal : CausalOrder history received := by
  apply CausalOrder.snoc (M := history) (L := [.parent, .oldChild, .childDelete, .newChild]) (w := .parentDelete)
  · apply CausalOrder.snoc (M := history) (L := [.parent, .oldChild, .childDelete]) (w := .newChild)
    · apply CausalOrder.snoc (M := history) (L := [.parent, .oldChild]) (w := .childDelete)
      · apply CausalOrder.snoc (M := history) (L := [.parent]) (w := .oldChild)
        · apply CausalOrder.snoc (M := history) (L := []) (w := .parent) .nil <;> simp [history]
        · decide
        · intro a h; cases a <;> simp_all [history]
      · decide
      · intro a h; cases a <;> simp_all [history]
    · decide
    · intro a h; cases a <;> simp_all [history]
  · decide
  · intro a h; cases a <;> simp_all [history]

def rebuilt (active : List (Write × Bool)) := roundState history valid provenance policy candidates active received

/-- The context used by Ben contains Ana's old, unlinked incarnation.
Keeping Ana's delete instead exposes her new child and its parent link. -/
theorem withholding_conceals_reference :
    (rebuilt [(.parentDelete, false)]).gen true = 1 ∧
    (inputs (rebuilt [(.parentDelete, false)])).refs true = [] ∧
    (rebuilt candidates).gen true = 3 ∧
    ((inputs (rebuilt candidates)).refs true).map Ref.parent = [false] := by
  unfold rebuilt roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

/-- The exhaustive comparison keeps the protected owner's delete, so its
trial sees the new child and rejects the parent's deletion. -/
theorem exhaustive_guard_sees_child :
    contextDeleteReason (W := Write)
      { rows := [false, true]
        owner := fun r => some (owner policy r)
        actor := fun op => policy.author op.1
        isDelete := fun op => (history.chg op.1 op.2).any (fun ch => decide (ch.kind = .del))
        inputs := fun context => (inputs (rebuilt context)).erase }
      candidates (.parentDelete, false) = some (.delete true true) := by
  unfold rebuilt roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

theorem both_deletes_pass :
    roundTrial history valid provenance policy candidates trial received candidates (.parentDelete, false) = none ∧
    roundTrial history valid provenance policy candidates trial received candidates (.childDelete, true) = none := by
  unfold roundTrial roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

theorem guards_empty : settledGuard history valid provenance policy candidates trial received = fun _ _ => none := by
  funext w r
  obtain ⟨parent, child⟩ := both_deletes_pass
  simp only [candidates] at parent child
  cases w <;> cases r <;> rw [settledGuard, rejectionReason.eq_def] <;>
    simp [candidates, parent, child, keepRound]

def evaluated := protectedResult history valid provenance policy candidates trial (fun w _ _ => some w) inputs received

/-- Two rows suffice without CHECK, uniqueness, set null or audience rules.
Ana's generation-three child is visible with only her own delete and hidden
by Ben's accepted parent deletion in the final merged state. -/
theorem foreign_child_hidden :
    owner policy true ≠ policy.author .parentDelete ∧
    (entryView (inputs (rebuilt [(.childDelete, true)]))).shown true = true ∧
    evaluated.1.merged.gen true = 3 ∧ evaluated.1.view.shown true = false ∧
    evaluated.2 .parentDelete false = none ∧ evaluated.2 .childDelete true = none ∧
    (evaluated.1.losses true none).map (fun loss => loss.values ()) = some (some .newChild) ∧
    (evaluated.1.losses true none).map (fun loss => loss.cause) = some (.rules [.foreignKey]) := by
  unfold evaluated protectedResult result
  rw [guards_empty]
  unfold rebuilt roundState merged Data.merged
  simp only [DataDependency.effective, retained, loss, DataDependency.dependsOnDropped]
  decide

example : evaluated.1.view.shown true = false := foreign_child_hidden.2.2.2.1

end CovenMerge.Ownership.DeleteGenerationExample
