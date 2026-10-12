import CovenMerge.OwnerDeleteRetention

/-! Checked original-write histories for a reference update, a cascade through
an unowned row, and a transfer into another audience followed by an update. -/
namespace CovenMerge.Ownership.DeleteDependents

inductive Scenario where
  | update | cascade | move
  deriving DecidableEq, Repr

inductive Row where
  | parent | child | bridge | destination
  deriving DecidableEq, Repr

inductive Write where
  | parent | initial | remove | revise | erase
  deriving DecidableEq, Repr

/-- The move's source and destination have one logical key, but separate
audiences and generations. Its destination reference is set after the move. -/
def history (s : Scenario) : Writes Write Row Unit where
  ts | .parent => 0 | .initial => 1 | .remove => 2 | .revise => 3 | .erase => 4
  past
    | .initial, .parent => decide (s = .cascade)
    | .remove, .parent => decide (s = .cascade)
    | .remove, .initial => decide (s ≠ .update)
    | .revise, .parent | .revise, .initial | .erase, .parent => true
    | .revise, .remove => decide (s ≠ .update)
    | _, _ => false
  chg
    | .parent, .parent | .initial, .child => some ⟨.ins, 0, fun _ => true⟩
    | .initial, .bridge => if s = .cascade then some ⟨.ins, 0, fun _ => true⟩ else none
    | .remove, .child => if s ≠ .update then some ⟨.del, 1, fun _ => false⟩ else none
    | .remove, .destination => if s = .move then some ⟨.ins, 0, fun _ => true⟩ else none
    | .revise, .child => match s with
        | .update => some ⟨.upd, 1, fun _ => true⟩
        | .cascade => some ⟨.ins, 2, fun _ => true⟩
        | .move => none
    | .revise, .destination => if s = .move then some ⟨.upd, 1, fun _ => true⟩ else none
    | .erase, .parent => some ⟨.del, 1, fun _ => false⟩
    | _, _ => none

theorem valid (s : Scenario) : Valid (history s) where
  ts_inj a b := by cases a <;> cases b <;> simp [history]
  past_ts w a := by cases s <;> cases w <;> cases a <;> decide
  gen_seen w r ch h := by
    cases s <;> cases w <;> cases r <;> simp [history] at h
    all_goals subst ch
    all_goals first
      | exact Or.inl rfl
      | exact Or.inr ⟨.parent, _, rfl, rfl, by decide, rfl⟩
      | exact Or.inr ⟨.initial, _, rfl, rfl, by decide, rfl⟩
      | exact Or.inr ⟨.remove, _, rfl, rfl, by decide, rfl⟩
  parity w r ch h := by
    cases s <;> cases w <;> cases r <;> simp [history] at h
    all_goals subst ch; decide

def provenance (s : Scenario) : DataDependency.Authored (history s) where
  provenance
    | .remove, .child => if s ≠ .update then ⟨some .initial, [((), .initial)]⟩ else ⟨none, []⟩
    | .revise, .child => match s with
        | .update => ⟨some .initial, [((), .initial)]⟩
        | .cascade => ⟨some .remove, []⟩
        | .move => ⟨none, []⟩
    | .revise, .destination => if s = .move then ⟨some .remove, [((), .remove)]⟩ else ⟨none, []⟩
    | .erase, .parent => ⟨some .parent, [((), .parent)]⟩
    | _, _ => ⟨none, []⟩
  read_source w r a h := by
    cases s <;> cases w <;> cases r <;> cases a <;>
      simp_all [DataDependency.Provenance.sources, history]
  generation_source w r ch h := by
    cases s <;> cases w <;> cases r <;> simp [history] at h
    all_goals subst ch
    all_goals first
      | exact Or.inl ⟨rfl, rfl⟩
      | exact Or.inr ⟨.parent, _, rfl, rfl, by decide, rfl⟩
      | exact Or.inr ⟨.initial, _, rfl, rfl, by decide, rfl⟩
      | exact Or.inr ⟨.remove, _, rfl, rfl, by decide, rfl⟩
  setter_source w r c a h := by
    cases s <;> cases w <;> cases r <;> cases a <;> simp at h
    all_goals exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

def policy : Policy Write Row Row Bool where
  table r := if r = .destination then .child else r
  owned r := r != .bridge
  creator | .parent => .parent | _ => .initial
  author | .parent | .erase => false | _ => true

def protectedRow : Scenario → Row
  | .move => .destination
  | _ => .child

def parents (s : Scenario) (st : St Write Row Unit) (r : Row) : List (Row × Nat) :=
  if s = .cascade ∧ r = .bridge then [(.parent, 1)]
  else if r = protectedRow s ∧ st.cell r () = some .revise then
    [(if s = .cascade then .bridge else .parent, 1)]
  else []

def inputs (s : Scenario) (st : St Write Row Unit) : EntryInputs Row Unit where
  rows := [.parent, .child, .bridge, .destination]
  present r := decide (st.gen r % 2 = 1)
  refs r := (parents s st r).map fun (p, k) => ⟨p, decide (k < st.gen p)⟩
  checkFails _ := false
  inDeletedCircle _ := none
  claims r := if s = .move ∧ (r = .child ∨ r = .destination) then
    [⟨⟨[], none⟩, (), if r = .child then 0 else 1, true⟩] else []
  rank | .parent => 0 | .bridge => 1 | .child => 2 | .destination => 3

def graph (s : Scenario) (st : St Write Row Unit) : ReferenceGraph (Row × Nat) where
  rows := (inputs s st).rows.map fun r => (r, st.gen r)
  present node := (inputs s st).present node.1
  parents node := parents s st node.1

def candidates (s : Scenario) : List (Write × Row) :=
  (if s = .update then [] else [(.remove, .child)]) ++ [(.erase, .parent)]

def received (s : Scenario) : List Write :=
  [.parent, .initial] ++ (if s = .update then [] else [.remove]) ++ [.revise, .erase]

theorem causal (s : Scenario) : CausalOrder (history s) (received s) := by
  apply causalOrder_of_ts_sorted (valid s)
  · intro w mem a read
    cases s <;> cases w <;> cases a <;> simp_all [history, received]
  · cases s <;> decide
  · cases s <;> decide

/-- These equations unfold the actual provenance filter for computation;
the arbitrary-admission equality below checks them against `retained`. -/
def kept (s : Scenario) (admission : Write → Row → Bool) : Write → Row → Bool
  | .parent, .parent => admission .parent .parent
  | .initial, .child => admission .initial .child
  | .initial, .bridge => decide (s = .cascade) && admission .initial .bridge
  | .remove, .child => decide (s ≠ .update) && admission .remove .child && admission .initial .child
  | .remove, .destination => decide (s = .move) && admission .remove .destination
  | .revise, .child => match s with
      | .update => admission .revise .child && admission .initial .child
      | .cascade => admission .revise .child && (admission .remove .child && admission .initial .child)
      | .move => false
  | .revise, .destination => decide (s = .move) && admission .revise .destination && admission .remove .destination
  | .erase, .parent => admission .erase .parent && admission .parent .parent
  | _, _ => false

set_option maxHeartbeats 1600000 in
theorem retained (s : Scenario) (admission : Write → Row → Bool) (w : Write) (r : Row) :
    DataDependency.retained (DataDependency.authoredState (history s) (valid s) (provenance s) admission) w r =
      kept s admission w r := by
  have parent : DataDependency.retained
      (DataDependency.authoredState (history s) (valid s) (provenance s) admission) .parent .parent =
      admission .parent .parent := by
    rw [DataDependency.retained_eq]
    simp [DataDependency.authoredState, history, provenance, DataDependency.Provenance.sources]
  have initial : DataDependency.retained
      (DataDependency.authoredState (history s) (valid s) (provenance s) admission) .initial .child =
      admission .initial .child := by
    rw [DataDependency.retained_eq]
    simp [DataDependency.authoredState, history, provenance, DataDependency.Provenance.sources]
  have removeChild : DataDependency.retained
      (DataDependency.authoredState (history s) (valid s) (provenance s) admission) .remove .child =
      (decide (s ≠ .update) && admission .remove .child && admission .initial .child) := by
    rw [DataDependency.retained_eq]
    cases s <;> simp_all [DataDependency.authoredState, history, provenance, DataDependency.Provenance.sources]
  have removeDestination : DataDependency.retained
      (DataDependency.authoredState (history s) (valid s) (provenance s) admission) .remove .destination =
      (decide (s = .move) && admission .remove .destination) := by
    rw [DataDependency.retained_eq]
    cases s <;> simp [DataDependency.authoredState, history, provenance, DataDependency.Provenance.sources]
  cases s <;> cases w <;> cases r <;> rw [DataDependency.retained_eq] <;>
    simp_all [DataDependency.authoredState, history, provenance, DataDependency.Provenance.sources, kept]

def trial (s : Scenario) := deleteTrial (history s) policy (graph s)
def rebuilt (s : Scenario) (active : List (Write × Row)) :=
  roundState (history s) (valid s) (provenance s) policy (candidates s) active (received s)

/-- Every scenario keeps the latest reference setter in Ben's trial. The
move withholds only the source removal, while its destination remains. -/
theorem trial_keeps_current (s : Scenario) :
    (rebuilt s (incarnationContext (history s) policy (rebuilt s (candidates s)) (candidates s) .erase)).cell
      (protectedRow s) () = some .revise := by
  unfold rebuilt roundState Data.merged
  simp only [DataDependency.effective, retained]
  cases s <;> decide

/-- Restoring the store copy hides its circle copy in the ordinary view.
The trial still reads the circle copy's current reference before that pass. -/
theorem move_trial_before_audience_choice :
    let context := incarnationContext (history .move) policy (rebuilt .move (candidates .move))
      (candidates .move) .erase
    (.remove, .child) ∉ context ∧
    (rebuilt .move context).gen .child = 1 ∧
    (rebuilt .move context).gen .destination = 1 ∧
    (entryView (inputs .move (rebuilt .move context))).shown .destination = false ∧
    (graph .move (rebuilt .move context)).present (.destination, 1) = true ∧
    owner policy .child = owner policy .destination := by
  unfold rebuilt roundState Data.merged
  simp only [DataDependency.effective, retained]
  decide

theorem erase_blocked (s : Scenario) :
    roundTrial (history s) (valid s) (provenance s) policy (candidates s) (trial s) (received s)
      (candidates s) (.erase, .parent) = some (.delete (protectedRow s) true) := by
  unfold roundTrial roundState Data.merged
  simp only [DataDependency.effective, retained]
  cases s <;> decide

theorem remove_passes (s : Scenario) :
    roundTrial (history s) (valid s) (provenance s) policy (candidates s) (trial s) (received s)
      (candidates s) (.remove, .child) = none ∧
    roundTrial (history s) (valid s) (provenance s) policy (candidates s) (trial s) (received s)
      [(.remove, .child)] (.remove, .child) = none := by
  unfold roundTrial roundState Data.merged
  simp only [DataDependency.effective, retained]
  cases s <;> decide

theorem guard_reason (s : Scenario) (w : Write) (r : Row) :
    settledGuard (history s) (valid s) (provenance s) policy (candidates s) (trial s) (received s) w r =
      if w = .erase ∧ r = .parent then some (.delete (protectedRow s) true) else none := by
  have erase := erase_blocked s
  obtain ⟨remove, removeOnly⟩ := remove_passes s
  cases s <;> simp only [candidates, reduceCtorEq, ↓reduceIte, List.nil_append, List.cons_append] at *
  all_goals cases w <;> cases r <;> rw [settledGuard, rejectionReason.eq_def] <;>
    simp [erase, remove, keepRound]
  all_goals rw [rejectionReason.eq_def]; simp [removeOnly, keepRound]

def evaluated (s : Scenario) := protectedResult (history s) (valid s) (provenance s) policy (candidates s)
  (trial s) (fun w _ _ => some w) (inputs s) (received s)

/-- Each complete merge records Ben's delete and keeps Ana's final reference.
In the move case the source row is deleted and the destination is visible. -/
theorem protected_final (s : Scenario) :
    (evaluated s).1.view.shown .parent = true ∧
    (evaluated s).1.view.shown (protectedRow s) = true ∧
    (evaluated s).1.merged.cell (protectedRow s) () = some .revise ∧
    ((evaluated s).2 .erase .parent).map (fun record => record.cause) =
      some (.delete (protectedRow s) true) := by
  have guards : settledGuard (history s) (valid s) (provenance s) policy (candidates s) (trial s) (received s) =
      fun w r => if w = .erase ∧ r = .parent then some (.delete (protectedRow s) true) else none := by
    funext w r; exact guard_reason s w r
  unfold evaluated protectedResult result
  rw [guards]
  unfold merged Data.merged
  simp only [DataDependency.effective, retained, loss, DataDependency.dependsOnDropped]
  cases s <;> decide

example : (evaluated .update).1.view.shown .child = true := (protected_final .update).2.1
example : (evaluated .cascade).1.view.shown .child = true := (protected_final .cascade).2.1
example : (evaluated .move).1.view.shown .destination = true := (protected_final .move).2.1

theorem move_source_stays_deleted : (evaluated .move).1.merged.gen .child = 2 ∧
    (evaluated .move).1.view.shown .child = false ∧ (evaluated .move).2 .remove .child = none := by
  have guards : settledGuard (history .move) (valid .move) (provenance .move) policy
      (candidates .move) (trial .move) (received .move) =
      fun w r => if w = .erase ∧ r = .parent then some (.delete .destination true) else none := by
    funext w r; exact guard_reason .move w r
  unfold evaluated protectedResult result
  rw [guards]
  unfold merged Data.merged
  simp only [DataDependency.effective, retained, loss, DataDependency.dependsOnDropped]
  decide

end CovenMerge.Ownership.DeleteDependents
