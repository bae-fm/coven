import CovenMerge.OwnerDeleteRule

namespace CovenMerge.Ownership.DeleteExamples

inductive Write where
  | parent | child | leftDelete | rightDelete
  deriving DecidableEq, Repr

open Write

/-- Both deletes race with the child insertion and with each other. Only the
parent creation precedes them. False is Ben's parent; true is Ana's child. -/
def history : Writes Write Bool Unit where
  ts | .parent => 0 | .child => 1 | .leftDelete => 2 | .rightDelete => 3
  past w a := decide (w ≠ .parent ∧ a = .parent)
  chg
    | .parent, false | .child, true => some ⟨.ins, 0, fun _ => true⟩
    | .leftDelete, false | .rightDelete, false => some ⟨.del, 1, fun _ => false⟩
    | _, _ => none

theorem valid : Valid history where
  ts_inj a b := by cases a <;> cases b <;> simp [history]
  past_ts w a := by cases w <;> cases a <;> decide
  gen_seen w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    · exact Or.inl rfl
    · exact Or.inl rfl
    all_goals exact Or.inr ⟨.parent, _, rfl, rfl, by decide, rfl⟩
  parity w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch; decide

def provenance : DataDependency.Authored history where
  provenance
    | .leftDelete, false | .rightDelete, false => ⟨some .parent, [((), .parent)]⟩
    | _, _ => ⟨none, []⟩
  read_source w r a h := by
    cases w <;> cases r <;> cases a <;> simp_all [DataDependency.Provenance.sources, history]
  generation_source w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    · exact Or.inl ⟨rfl, rfl⟩
    · exact Or.inl ⟨rfl, rfl⟩
    all_goals exact Or.inr ⟨.parent, _, rfl, rfl, by decide, rfl⟩
  setter_source w r c a h := by
    cases w <;> cases r <;> cases a <;> simp at h
    all_goals exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

def policy : Policy Write Bool Unit Bool where
  table _ := ()
  owned _ := true
  creator r := if r then .child else .parent
  author w := w == .child

def inputs (st : St Write Bool Unit) : EntryInputs Bool Unit where
  rows := [false, true]
  present r := decide (st.gen r % 2 = 1)
  refs r := if r then [⟨false, decide (2 ≤ st.gen false)⟩] else []
  checkFails _ := false
  inDeletedCircle _ := none
  claims _ := []
  rank r := if r then 1 else 0

def graph (st : St Write Bool Unit) : ReferenceGraph (Bool × Nat) where
  rows := [(false, st.gen false), (true, st.gen true)]
  present node := decide (st.gen node.1 % 2 = 1)
  parents node := if node.1 then [(false, 1)] else []

def trial := deleteTrial history policy graph inputs

def candidates : List (Write × Bool) := [(.leftDelete, false), (.rightDelete, false)]

def values (_w : Write) (r : Bool) (_c : Unit) : Option Write :=
  some (if r then .child else .parent)

def kept (admission : Write → Bool → Bool) : Write → Bool → Bool
  | .parent, false => admission .parent false
  | .child, true => admission .child true
  | .leftDelete, false => admission .leftDelete false && admission .parent false
  | .rightDelete, false => admission .rightDelete false && admission .parent false
  | _, _ => false

theorem retained (admission : Write → Bool → Bool) (w : Write) (r : Bool) :
    DataDependency.retained (DataDependency.authoredState history valid provenance admission) w r =
      kept admission w r := by
  have parent : DataDependency.retained
      (DataDependency.authoredState history valid provenance admission) .parent false = admission .parent false := by
    rw [DataDependency.retained_eq]
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources]
  cases w <;> cases r <;> rw [DataDependency.retained_eq] <;>
    simp [DataDependency.authoredState, provenance, history, DataDependency.Provenance.sources, kept] at *
  all_goals rw [parent]

def first : List Write := [.parent, .child, .leftDelete, .rightDelete]
def second : List Write := [.parent, .rightDelete, .leftDelete, .child]

theorem first_causal : CausalOrder history first := by
  apply CausalOrder.snoc (M := history) (L := [.parent, .child, .leftDelete]) (w := .rightDelete)
  · apply CausalOrder.snoc (M := history) (L := [.parent, .child]) (w := .leftDelete)
    · apply CausalOrder.snoc (M := history) (L := [.parent]) (w := .child)
      · apply CausalOrder.snoc (M := history) (L := []) (w := .parent) .nil <;> simp [history]
      · decide
      · intro a h; cases a <;> simp_all [history]
    · decide
    · intro a h; cases a <;> simp_all [history]
  · decide
  · intro a h; cases a <;> simp_all [history]

theorem second_causal : CausalOrder history second := by
  apply CausalOrder.snoc (M := history) (L := [.parent, .rightDelete, .leftDelete]) (w := .child)
  · apply CausalOrder.snoc (M := history) (L := [.parent, .rightDelete]) (w := .leftDelete)
    · apply CausalOrder.snoc (M := history) (L := [.parent]) (w := .rightDelete)
      · apply CausalOrder.snoc (M := history) (L := []) (w := .parent) .nil <;> simp [history]
      · decide
      · intro a h; cases a <;> simp_all [history]
    · decide
    · intro a h; cases a <;> simp_all [history]
  · decide
  · intro a h; cases a <;> simp_all [history]

def evaluated := protectedResult history valid provenance policy candidates trial values inputs first

theorem initial_deletion_trial (w : Write) (deletes : w = .leftDelete ∨ w = .rightDelete) :
    roundTrial history valid provenance policy candidates trial first candidates (w, false) =
      some (.delete true true) := by
  unfold roundTrial roundState Data.merged
  simp only [DataDependency.effective, retained]
  rcases deletes with rfl | rfl <;> decide

theorem guard_reason (w : Write) (r : Bool) :
    settledGuard history valid provenance policy candidates trial first w r =
      if (w = .leftDelete ∨ w = .rightDelete) ∧ r = false then some (.delete true true) else none := by
  have left := initial_deletion_trial .leftDelete (Or.inl rfl)
  have right := initial_deletion_trial .rightDelete (Or.inr rfl)
  simp only [candidates] at left right
  cases w <;> cases r <;> rw [settledGuard, rejectionReason.eq_def] <;>
    simp [candidates, left, right]

theorem orders_agree : evaluated =
    protectedResult history valid provenance policy candidates trial values inputs second :=
  protected_result_converges history valid provenance policy candidates trial values inputs
    first_causal second_causal (by intro w; cases w <;> decide)

/-- This executes the original-write merge, protection rounds, loss recorder
and all three removal passes, with an actually concurrent inserted child. -/
theorem rows_restored_and_losses_recorded :
    evaluated.1.merged.gen false = 1 ∧ evaluated.1.merged.gen true = 1 ∧
    evaluated.1.view.shown false = true ∧ evaluated.1.view.shown true = true ∧
    (evaluated.2 .leftDelete false).map (fun record => record.cause) = some (.delete true true) ∧
    (evaluated.2 .rightDelete false).map (fun record => record.cause) = some (.delete true true) := by
  unfold evaluated protectedResult result
  have guards : settledGuard history valid provenance policy candidates trial first =
      fun w r => if (w = .leftDelete ∨ w = .rightDelete) ∧ r = false then some (.delete true true) else none := by
    funext w r; exact guard_reason w r
  rw [guards]
  simp only [merged, Data.merged, DataDependency.effective, retained]
  decide

example : evaluated.1.view.shown true = true := rows_restored_and_losses_recorded.2.2.2.1

end CovenMerge.Ownership.DeleteExamples
