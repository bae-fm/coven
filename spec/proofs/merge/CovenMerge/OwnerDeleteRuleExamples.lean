import CovenMerge.OwnerDeleteCost
import CovenMerge.OwnerExamples

namespace CovenMerge.Ownership.DeleteRuleExamples

/-- One circle entry and one foreign-owned row, the former restoration-cycle
example. The seed stands for the row's derived audience being deleted. -/
def oneRow : ReferenceGraph Unit := ⟨[()], fun _ => true, fun _ => []⟩

def circleTrial (_active : List Unit) (_op : Unit) : Option (Cause Unit Bool Unit) :=
  referenceReason oneRow (fun _ => some true) id false (fun _ => true)

theorem one_row_no_cycle :
    rejectionReason circleTrial [()] () = some (.delete () true) ∧
    settle (fun active op => (circleTrial active op).isSome) [()] = [] ∧
    settle (fun active op => (circleTrial active op).isSome)
      (settle (fun active op => (circleTrial active op).isSome) [()]) = [] := by
  have trial : ∀ active op, circleTrial active op = some (.delete () true) := by intros; rfl
  have settled : settle (fun active op => (circleTrial active op).isSome) [()] = [] := by
    simp [settle, keepRound, trial]
  exact ⟨by rw [rejectionReason.eq_def]; simp [trial], settled, by rw [settle_idempotent, settled]⟩

/-- False is Ben deleting his parent; true is Ana deleting her child.
Ana's concurrent delete is withheld in Ben's test. After Ben reads it,
it is kept in his test, even if both deletes are still candidates. -/
def retryGraph (active : List Bool) : ReferenceGraph Nat where
  rows := [0, 1]
  present r := if r = 0 then !decide (false ∈ active) else r == 1 && !decide (true ∈ active)
  parents r := if r = 1 then [0] else []

def retryReason (readChildDelete : Bool) (active : List Bool) : Option (Cause Bool Bool Nat) :=
  referenceReason (retryGraph (memberContext id (fun _ => true) (fun op => readChildDelete && op) active false))
    (fun r => some (r == 1)) id false (fun r => r == 0)

theorem concurrent_owner_delete_requires_retry :
    retryReason false [false, true] = some (.delete 1 true) ∧
    retryReason true [false, true] = none := by decide

inductive Row where
  | parent | child | rival
  deriving DecidableEq, Repr

inductive Write where
  | parent | child | rival | erase
  deriving DecidableEq, Repr

/-- Three creations and one delete. The child read the parent, but its
creation is concurrent with the parent's deletion. Ana's rival has no
reference to either of Ben's rows. -/
def history : Writes Write Row Unit where
  ts | .parent => 0 | .child => 1 | .rival => 2 | .erase => 3
  past w a := decide ((w = .child ∨ w = .erase) ∧ a = .parent)
  chg
    | .parent, .parent | .child, .child | .rival, .rival => some ⟨.ins, 0, fun _ => true⟩
    | .erase, .parent => some ⟨.del, 1, fun _ => false⟩
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
    · exact Or.inr ⟨.parent, _, rfl, rfl, by decide, rfl⟩
  parity w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch; decide

def provenance : DataDependency.Authored history where
  provenance
    | .erase, .parent => ⟨some .parent, [((), .parent)]⟩
    | _, _ => ⟨none, []⟩
  read_source w r a h := by
    cases w <;> cases r <;> cases a <;> simp_all [DataDependency.Provenance.sources, history]
  generation_source w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    · exact Or.inl ⟨rfl, rfl⟩
    · exact Or.inl ⟨rfl, rfl⟩
    · exact Or.inl ⟨rfl, rfl⟩
    · exact Or.inr ⟨.parent, _, rfl, rfl, by decide, rfl⟩
  setter_source w r c a h := by
    cases w <;> cases r <;> cases a <;> simp at h
    exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

def policy : Policy Write Row Bool Bool where
  table r := r == .parent
  owned _ := true
  creator | .parent => .parent | .child => .child | .rival => .rival
  author w := w == .rival

def inputs (st : St Write Row Unit) : EntryInputs Row Nat where
  rows := [.parent, .child, .rival]
  present r := decide (st.gen r % 2 = 1)
  refs r := if r = .child ∧ st.gen .parent < 2 then [⟨.parent, false⟩] else []
  checkFails _ := false
  inDeletedCircle _ := none
  claims r :=
    if r = .child then [⟨⟨["coalesce(parent_id, -1)"], none⟩, if st.gen .parent < 2 then 1 else 0, 1, false⟩]
    else if r = .rival then [⟨⟨["coalesce(parent_id, -1)"], none⟩, 0, 2, false⟩] else []
  rank | .parent => 0 | .child => 1 | .rival => 2

def graph (st : St Write Row Unit) : ReferenceGraph (Row × Nat) where
  rows := [.parent, .child, .rival].map fun r => (r, st.gen r)
  present node := decide (st.gen node.1 % 2 = 1)
  -- Keep the authored reference even when its displayed value is null.
  parents node := if node.1 = .child then [(.parent, 1)] else []

def trial := deleteTrial history policy graph

def candidates : List (Write × Row) := [(.erase, .parent)]
def received : List Write := [.parent, .child, .rival, .erase]

def kept (admission : Write → Row → Bool) : Write → Row → Bool
  | .parent, .parent => admission .parent .parent
  | .child, .child => admission .child .child
  | .rival, .rival => admission .rival .rival
  | .erase, .parent => admission .erase .parent && admission .parent .parent
  | _, _ => false

theorem retained (admission : Write → Row → Bool) (w : Write) (r : Row) :
    DataDependency.retained (DataDependency.authoredState history valid provenance admission) w r =
      kept admission w r := by
  have parent : DataDependency.retained
      (DataDependency.authoredState history valid provenance admission) .parent .parent = admission .parent .parent := by
    rw [DataDependency.retained_eq]
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources]
  cases w <;> cases r <;> rw [DataDependency.retained_eq] <;>
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources, kept] at *
  rw [parent]

theorem causal : CausalOrder history received := by
  apply CausalOrder.snoc (M := history) (L := [.parent, .child, .rival]) (w := .erase)
  · apply CausalOrder.snoc (M := history) (L := [.parent, .child]) (w := .rival)
    · apply CausalOrder.snoc (M := history) (L := [.parent]) (w := .child)
      · apply CausalOrder.snoc (M := history) (L := []) (w := .parent) .nil <;> simp [history]
      · decide
      · intro a h; cases a <;> simp_all [history]
    · decide
    · intro a h; cases a <;> simp_all [history]
  · decide
  · intro a h; cases a <;> simp_all [history]

theorem trial_permits :
    roundTrial history valid provenance policy candidates trial received candidates (.erase, .parent) = none := by
  unfold roundTrial roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

theorem guards_empty : settledGuard history valid provenance policy candidates trial received = fun _ _ => none := by
  funext w r
  have allowed := trial_permits
  simp only [candidates] at allowed
  cases w <;> cases r <;> rw [settledGuard, rejectionReason.eq_def] <;>
    simp [candidates, allowed, keepRound]

def evaluated := protectedResult history valid provenance policy candidates trial (fun w _ _ => some w) inputs received

def withoutDelete := roundState history valid provenance policy candidates [] received

/-- There is no foreign removal to withhold, independently of the full
round's generations. Keeping reinsertion evidence cannot change this trial. -/
theorem context_unchanged (st : St Write Row Unit) :
    incarnationContext history policy st candidates .erase = candidates := by
  simp [incarnationContext, memberContext, candidates, history, policy]

/-- A reference-free foreign row is hidden by a changed unique claim. The
reference rule accepts the deletion; the final ordinary loss still records Ana's
value, so value accounting does not imply owner protection. -/
theorem foreign_row_hidden :
    (entryView (inputs withoutDelete)).shown .rival = true ∧
    evaluated.1.view.shown .child = true ∧ evaluated.1.view.shown .rival = false ∧
    evaluated.2 .erase .parent = none ∧
    (evaluated.1.losses .rival none).map (fun loss => loss.values ()) = some (some .rival) ∧
    (evaluated.1.losses .rival none).map (fun loss => loss.cause) = some (.rules [.unique]) := by
  unfold evaluated protectedResult result
  rw [guards_empty]
  unfold withoutDelete roundState merged Data.merged
  simp only [DataDependency.effective, retained, loss, DataDependency.dependsOnDropped]
  decide

example : evaluated.1.view.shown .rival = false := foreign_row_hidden.2.2.1

/-- The final protection assertion is false even with valid writes, checked
provenance and a causal received history. The before view has no foreign delete. -/
theorem final_owner_protection_refuted :
    ¬ (∀ r, owner policy r ≠ policy.author .erase →
      (entryView (inputs withoutDelete)).shown r = true → evaluated.1.view.shown r = true) := by
  intro protects
  have visible := protects .rival (by decide) foreign_row_hidden.1
  rw [foreign_row_hidden.2.2.1] at visible
  contradiction

/-- Removing any of the three roles eliminates this set-null/unique history. -/
theorem three_roles_needed :
    (view { Examples.nullUnique true with claims := fun r => if r = 1 then [] else (Examples.nullUnique true).claims r }).shown 2 = true ∧
    (view (Examples.nullUnique false)).shown 2 = true ∧
    referenceBlocker
      { rows := [0, 1], present := fun _ => true, parents := fun r => if r = 1 then [0] else [] }
      (fun r => some (r == 1)) false (fun r => r == 0) = some (1, true) := by decide

end CovenMerge.Ownership.DeleteRuleExamples
