import CovenMerge.OwnerAdmission

/-! Ben's local track deletion may cascade into Ana's review. Its derived
record retains the cause even though Ben knowingly replaced the old value. -/
namespace CovenMerge.Ownership.CascadeAuthorization

inductive Row where
  | track | review
  deriving DecidableEq, Repr

inductive Write where
  | track | review | erase
  deriving DecidableEq, Repr

inductive Member where
  | ben | ana
  deriving DecidableEq, Repr

/-- Ben creates the track, Ana creates its review, then Ben reads the review
and deletes the track. SQLite's CASCADE contributes the second delete to
Ben's write. There are no concurrent writes or additional constraints. -/
def history : Writes Write Row Unit where
  ts | .track => 0 | .review => 1 | .erase => 2
  past
    | .review, .track | .erase, .track | .erase, .review => true
    | _, _ => false
  chg
    | .track, .track | .review, .review => some ⟨.ins, 0, fun _ => true⟩
    | .erase, _ => some ⟨.del, 1, fun _ => false⟩
    | _, _ => none

theorem valid : Valid history where
  ts_inj a b := by cases a <;> cases b <;> simp [history]
  past_ts w a := by cases w <;> cases a <;> decide
  gen_seen w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    all_goals first
      | exact Or.inl rfl
      | exact Or.inr ⟨.track, _, rfl, rfl, by decide, rfl⟩
      | exact Or.inr ⟨.review, _, rfl, rfl, by decide, rfl⟩
  parity w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch; decide

def creator : Row → Write
  | .track => .track
  | .review => .review

def provenance : DataDependency.Authored history where
  provenance w r := if w = .erase then ⟨some (creator r), [((), creator r)]⟩ else ⟨none, []⟩
  read_source w r a h := by
    cases w <;> cases r <;> cases a <;>
      simp_all [DataDependency.Provenance.sources, history, creator]
  generation_source w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    all_goals first
      | exact Or.inl ⟨rfl, rfl⟩
      | exact Or.inr ⟨.track, _, rfl, rfl, by decide, rfl⟩
      | exact Or.inr ⟨.review, _, rfl, rfl, by decide, rfl⟩
  setter_source w r c a h := by
    cases w <;> cases r <;> cases a <;> simp [creator] at h
    all_goals exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

def policy : Policy Write Row Row Member where
  table := id
  owned _ := true
  creator := creator
  author | .review => .ana | _ => .ben

def received : List Write := [.track, .review, .erase]

theorem causal : CausalOrder history received := by
  apply causalOrder_of_ts_sorted valid
  · intro w mem a read; cases w <;> cases a <;> simp_all [history, received]
  · decide
  · decide

def inputs (st : St Write Row Unit) : EntryInputs Row Unit where
  rows := [.track, .review]
  present r := decide (st.gen r % 2 = 1)
  refs | .track => [] | .review => [⟨.track, decide (2 ≤ st.gen .track)⟩]
  checkFails _ := false
  inDeletedCircle _ := none
  claims _ := []
  rank | .track => 0 | .review => 1

theorem before_delete :
    (entryDevice history inputs [.track, .review]).view.shown .track = true ∧
    (entryDevice history inputs [.track, .review]).view.shown .review = true ∧
    history.past .erase .review = true ∧
    owner policy .review = .ana ∧ policy.author .erase = .ben := by decide

open ForeignKey Admission

def payload (w : Write) (r : Row) : Option (Effect Row Unit Nat) :=
  (history.chg w r).map fun change =>
    ⟨r, change.gen, change.kind, if change.kind = .del then [] else [((), some 1)]⟩

def reference (action : Action) : Reference Row Unit Nat :=
  ⟨.track, 1, .review, 1, [()], [some 1], action, .cascade, [()], []⟩

/-- This history's direct actions have no source roles beyond the net
changes' authenticated generation/setter provenance. -/
def authoredDirect (w : Write) (r : Row) : Option (Direct Write Row (Effect Row Unit Nat)) :=
  (payload w r).map fun value => ⟨value, []⟩

def evidence (action : Action) : Evidence Write Row (Effect Row Unit Nat) where
  payload := payload
  execution w := {
    direct := fun r => if w = .erase ∧ r = .review then none else authoredDirect w r
    origin := fun r => if w = .erase ∧ r = .review then .foreign (.foreign (.direct .track) 0) else .direct
    schema := resolvedSchema [reference action] }

def changes : List (Write × Row) :=
  [(.track, .track), (.review, .review), (.erase, .track), (.erase, .review)]

def causes := dependencies history provenance policy (evidence .cascade) changes

def prior (_ : Write) (_ : Row) : Option (Cause Write Member Row) := none

abbrev permission := Admission.guard policy (evidence .cascade) causes prior

def values (w : Write) (r : Row) (_ : Unit) : Option Write :=
  some (if w = .erase then creator r else w)

def project (effect : Effect Row Unit Nat) : Change Unit :=
  ⟨effect.kind, effect.generation, fun c => decide (c ∈ effect.cells.map Prod.fst)⟩

theorem captured : Capture history (evidence .cascade) (fun e => some (project e)) Effect.row where
  target_exact w r value h := by
    cases w <;> cases r <;> simp [evidence, payload, history] at h <;> cases h <;> rfl
  direct_target_exact w r value h := by
    cases w <;> cases r <;> simp [Execution.directPayload, authoredDirect, evidence, payload, history] at h <;> cases h <;> rfl
  exact w r := by cases w <;> cases r <;> rfl

theorem cascade_checked :
    allowed policy .erase .review = false ∧
    check policy (evidence .cascade) .erase .track = true ∧
    check policy (evidence .cascade) .erase .review = true := by decide

theorem retained_permission (w : Write) (r : Row) :
    DataDependency.retained (DataDependency.authoredState history valid provenance (base permission)) w r =
      (history.chg w r).isSome := by
  have initial (w : Write) (r : Row) (notErase : w ≠ .erase) :
      DataDependency.retained (DataDependency.authoredState history valid provenance (base permission)) w r =
        (history.chg w r).isSome := by
    rw [DataDependency.retained_eq]
    cases w <;> cases r <;> first | contradiction | decide
  cases w with
  | track => exact initial .track r (by decide)
  | review => exact initial .review r (by decide)
  | erase =>
    rw [DataDependency.retained_eq]
    cases r <;> change (true && true && [_, _].all _) = true
    all_goals simp only [List.all_cons, List.all_nil, Bool.and_true, Bool.true_and, creator]
    · rw [initial .track .track (by decide)]; rfl
    · rw [initial .review .review (by decide)]; rfl

/-- The original three-write witness is accepted, with both recorded deletes. -/
theorem cascade_applied :
    (merged history valid provenance permission received).gen .track = 2 ∧
    (merged history valid provenance permission received).gen .review = 2 ∧
    (loss history valid provenance permission values received .erase .review) = none := by
  simp only [merged, Data.merged, DataDependency.effective, loss,
    DataDependency.dependsOnDropped, retained_permission]
  decide

theorem cascade_recorded :
    derivedRecord history valid provenance policy (evidence .cascade) causes prior received .erase .review =
      some ⟨.erase, .review, ⟨.review, 1, .del, []⟩,
        .foreign (.direct .track) 0, [(.erase, .track)]⟩ := by
  simp only [derivedRecord, retained_permission]
  decide

theorem restrict_no_derivation :
    check policy (evidence .restrict) .erase .review = false ∧
    check policy (evidence .noAction) .erase .review = false := by decide

/-- A shared trigger's edit has the direct origin, even if the trigger was
fired by a foreign-key action. It cannot borrow the track's permission. -/
theorem trigger_is_direct :
    check policy { evidence .cascade with execution := fun w =>
      { (evidence .cascade).execution w with origin := fun _ => .direct, direct := authoredDirect w } } .erase .review = false := by decide

theorem trigger_cannot_claim_foreign :
    check policy { evidence .cascade with execution := fun w =>
      { (evidence .cascade).execution w with direct := authoredDirect w } } .erase .review = false := by decide

def forged (effect : Effect Row Unit Nat) : Evidence Write Row (Effect Row Unit Nat) :=
  { evidence .cascade with payload := fun w r =>
      if w = .erase ∧ r = .review then some effect else payload w r }

theorem forged_payloads_refused :
    check policy (forged ⟨.review, 1, .upd, [((), some 99)]⟩) .erase .review = false ∧
    check policy (forged ⟨.review, 3, .del, []⟩) .erase .review = false ∧
    check policy (forged ⟨.review, 1, .del, [((), some 99)]⟩) .erase .review = false := by decide

def droppedParent (w : Write) (r : Row) : Option (Cause Write Member Row) :=
  if w = .erase ∧ r = .track then some (.dependency [(.track, .track)]) else none

abbrev afterDrop := Admission.guard policy (evidence .cascade) causes droppedParent

theorem retained_after_drop (w : Write) (r : Row) :
    DataDependency.retained (DataDependency.authoredState history valid provenance (base afterDrop)) w r =
      ((history.chg w r).isSome && decide (w ≠ .erase)) := by
  rw [DataDependency.retained_eq]
  cases w <;> cases r <;> decide

theorem cascade_reversed :
    (merged history valid provenance afterDrop received).gen .track = 1 ∧
    (merged history valid provenance afterDrop received).gen .review = 1 ∧
    (result history valid provenance afterDrop values inputs received).1.view.shown .review = true ∧
    (merged history valid provenance afterDrop received).cell .review () = some .review ∧
    (loss history valid provenance afterDrop values received .erase .review).isSome = true ∧
    derivedRecord history valid provenance policy (evidence .cascade) causes droppedParent received .erase .review = none := by
  simp only [merged, Data.merged, DataDependency.effective, result, loss,
    DataDependency.dependsOnDropped, derivedRecord, retained_after_drop]
  decide

theorem no_cause_no_permission :
    check { policy with author := fun w => if w = .erase then .ana else policy.author w }
      (evidence .cascade) .erase .track = false ∧
    check { policy with author := fun w => if w = .erase then .ana else policy.author w }
      (evidence .cascade) .erase .review = false := by decide

example : Valid history ∧ CausalOrder history received ∧
    (merged history valid provenance permission received).gen .review = 2 ∧
    (merged history valid provenance afterDrop received).gen .review = 1 :=
  ⟨valid, causal, cascade_applied.2.1, cascade_reversed.2.1⟩

end CovenMerge.Ownership.CascadeAuthorization
