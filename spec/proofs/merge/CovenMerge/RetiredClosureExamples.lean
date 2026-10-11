import CovenMerge.RetiredClosure
import CovenMerge.ReceiveExamples

namespace CovenMerge.Receive.RetiredClosure.Examples

open CovenMerge.Receive.Examples
open CovenMerge.Receive.Examples.Write

def pair : List Write := [.create, .edit]
def triple : List Write := [.create, .edit, .delete]

theorem pair_counts (w : Write) : Counts policy history.past pair w ↔ w ≠ .delete := by
  constructor
  · intro h
    have := counts_received h
    cases w <;> simp_all [pair]
  · intro h
    have edit : Counts policy history.past pair .edit := .seed (by decide) (by decide) (by decide)
    cases w with
    | create => exact .reader edit (by decide) (by decide) (by decide) (by decide)
    | edit => exact edit
    | delete => exact (h rfl).elim

theorem triple_counts (w : Write) : Counts policy history.past triple w := by
  cases w with
  | create =>
    exact .reader (.seed (w := .edit) (by decide) (by decide) (by decide))
      (by decide) (by decide) (by decide) (by decide)
  | edit => exact .seed (by decide) (by decide) (by decide)
  | delete => exact .seed (by decide) (by decide) (by decide)

theorem pair_counting : counting policy history.past pair = (fun w => w != .delete) := by
  funext w
  apply Bool.eq_iff_iff.mpr
  simp [pair_counts]

theorem triple_counting : counting policy history.past triple = (fun _ => true) := by
  funext w
  exact (counting_iff _ _ _ _).mpr (triple_counts w)

theorem retirement_generation_restored :
    withinWindow policy .create = false ∧
    SnapshotGenerations history noSnapshot (counting policy history.past pair) := by
  refine ⟨by decide, ?_⟩
  rw [pair_counting]
  intro w r ch kept change
  cases w <;> simp only [history, reduceCtorEq, ↓reduceIte, Option.some.injEq] at change
  · subst ch; exact Or.inl rfl
  · subst ch; exact Or.inr ⟨.create, _, rfl, by decide, rfl, by decide, rfl⟩
  · simp [snapshotRetains, noSnapshot] at kept

theorem boundary_generations : SnapshotGenerations history noSnapshot
    (boundaryEligible policy.reset policy.readEntry policy.schema) := by
  intro w r ch kept change
  rcases authored_valid.gen_seen w r ch change with h | ⟨a, ca, read, ha, hk, hg⟩
  · exact Or.inl h
  · exact Or.inr ⟨a, ca, read, by cases a <;> decide, ha, hk, hg⟩

theorem retirement_value_accounted (I : EntryInputs Unit Unit) (present : I.present () = true) :
    Setter (effective history policy pair) (inputs policy history.past pair creationAndEdit)
      () () .edit 1 ∧
    ValueAccounted (effective history policy pair) (inputs policy history.past pair creationAndEdit)
      (merged history policy pair St.init creationAndEdit) I () () .edit 1 := by
  have setter : Setter (effective history policy pair) (inputs policy history.past pair creationAndEdit)
      () () .edit 1 := by
    refine ⟨Or.inr ?_, ⟨⟨.upd, 1, fun _ => true⟩, ?_, by decide, rfl, rfl⟩⟩
    · rw [applied, pair_counting]; decide
    · rw [effective, pair_counting]; rfl
  refine ⟨setter, ?_⟩
  apply receive_value_accounted authored_valid policy pair past exact_past (fun _ _ => true)
    (arrivalRun true) boundary_generations
    (by intro w hw a ha; cases w <;> cases a <;> simp_all [pair, history])
    (by intro w _ _ a _; cases a <;> decide)
    (by intro w h; cases h) (by intro w h; cases h) St.init
    (isSpec_congr (by intro w; simp [policy, noSnapshot]) isSpec_init) 5 I _ setter
  intro r
  cases r
  rw [merged, pair_counting]
  exact present

theorem retirement_loss_agrees :
    (merged history policy triple St.init editThenDelete).lost () () .edit = some (1, .delete) ∧
    (merged history policy triple St.init deleteThenEdit).lost () () .edit = some (1, .delete) := by
  simp only [merged, triple_counting]
  constructor <;> rfl

theorem triple_generations : SnapshotGenerations history noSnapshot
    (counting policy history.past triple) :=
  generations policy triple boundary_generations
    (by intro w _ a _; cases a <;> decide)
    (by intro w _ _ a _; cases a <;> decide)
    (by intro w h; cases h)

/-- This applies the general theorem to the original legal receive runs,
including their different causal application orders. Equality is of every
merge field, not only the one previously disagreeing cell loss. -/
theorem receive_runs_agree :
    merged history policy triple St.init ((arrivalRun true).state 6) =
      merged history policy triple St.init ((arrivalRun false).state 6) := by
  apply receive_converges authored_valid policy triple past exact_past
    (fun _ _ => true) (fun _ _ => true) (arrivalRun true) (arrivalRun false)
    boundary_generations
    (by intro w _ a _; cases a <;> decide)
    (by intro w _ _ a _; cases a <;> decide)
    (by intro w h; cases h)
    (by intro w h; cases h) St.init
  · exact isSpec_congr (by intro w; simp [policy, noSnapshot]) isSpec_init
  · intro w
    simp only [inputs, applied, triple_counting]
    cases w <;> decide

theorem receive_loss_records_agree (rules : St Write Unit Unit → EntryInputs Unit Unit) :
    lossRecord (merged history policy triple St.init ((arrivalRun true).state 6))
        (entryView (rules (merged history policy triple St.init ((arrivalRun true).state 6)))) =
      lossRecord (merged history policy triple St.init ((arrivalRun false).state 6))
        (entryView (rules (merged history policy triple St.init ((arrivalRun false).state 6)))) := by
  rw [receive_runs_agree]

/-- Receive supplies all retirement-specific safety premises itself. The
candidate lists are the two actual consumed prefixes, in their own orders. -/
theorem receive_prefix_runs_agree :
    merged history policy ((arrivalRun true).state 6).passed St.init ((arrivalRun true).state 6) =
      merged history policy ((arrivalRun false).state 6).passed St.init ((arrivalRun false).state 6) := by
  apply receive_prefix_converges authored_valid policy (fun _ => rfl)
    (fun _ => rfl) past exact_past (fun _ _ => true) (fun _ _ => true)
    (arrivalRun true) (arrivalRun false)
    (by intro _ w _; cases w <;> decide) (by intro _ w _; cases w <;> decide)
  intro w
  cases w <;> decide

/-- One late write alone is excluded. A subsequently received active reader
retains it, even though the retirement and both publication times are fixed. -/
theorem later_reader_revives :
    ¬ Counts policy history.past [.create] .create ∧ Counts policy history.past pair .create := by
  refine ⟨?_, (pair_counts .create).mpr (by decide)⟩
  intro h
  obtain ⟨root, mem, _, window⟩ := counts_have_seed h
  simp only [List.mem_singleton] at mem
  subst root
  cases window

/-- The target's retirement is unchanged. Keeping a retirement of its only
reader removes the last seed; replay must remove the target's effects too. -/
def readerRetired : Policy Write :=
  { policy with retirements := [⟨0, 0, 0⟩, ⟨1, 1, 0⟩] }

theorem dropped_reader_stops_counting :
    Counts policy history.past pair .create ∧
    ¬ Counts readerRetired history.past pair .create := by
  refine ⟨(pair_counts .create).mpr (by decide), ?_⟩
  intro h
  obtain ⟨root, mem, _, window⟩ := counts_have_seed h
  cases root <;> simp_all [pair, withinWindow, retirementAllows, readerRetired, policy, logs]

example : Counts policy history.past pair .create ∧
    ¬ Counts readerRetired history.past pair .create := dropped_reader_stops_counting

/-- A retired edit is kept only because an active reader saw it. Its recorded
past contains a refused creation. Closure does not authorize that creation. -/
def refusedPolicy : Policy Write :=
  { readerRetired with readEntry := fun w e => decide (w = .create ∧ e = 0) }

def chainedPast : Write → Write → Bool :=
  fun w a => decide ((w = .delete ∧ a = .edit) ∨ (w = .edit ∧ a = .create))

/-- Two applications of the rule are necessary: the root lists only the
middle write, whose own recorded past retains the first write. -/
theorem repeated_closure :
    withinWindow readerRetired .create = false ∧ withinWindow readerRetired .edit = false ∧
    chainedPast .delete .create = false ∧ Counts readerRetired chainedPast triple .create := by
  refine ⟨by decide, by decide, by decide, ?_⟩
  have root : Counts readerRetired chainedPast triple .delete :=
    .seed (by decide) (by decide) (by decide)
  have middle : Counts readerRetired chainedPast triple .edit :=
    .reader root (by decide) (by decide) (by decide) (by decide)
  exact .reader middle (by decide) (by decide) (by decide) (by decide)

theorem revived_reader_cannot_authorize_refused :
    withinWindow refusedPolicy .edit = false ∧
    Counts refusedPolicy chainedPast triple .edit ∧
    ¬ Counts refusedPolicy chainedPast triple .create := by
  refine ⟨by decide, .reader (.seed (w := .delete) (by decide) (by decide) (by decide))
    (by decide) (by decide) (by decide) (by decide), ?_⟩
  exact post_read_refused refusedPolicy chainedPast triple .create ⟨0, 0, 0⟩
    (by decide) rfl (by decide)

/-- `Valid` alone does not establish honest authoring after a boundary. Two
writes already expose that obligation: the active edit claims a generation
created by a refused write. The closure correctly refuses its witness. -/
def forbiddenCreation : Policy Write :=
  { policy with readEntry := fun w e => decide (w = .create ∧ e = 0) }

theorem forbidden_pair_counting : counting forbiddenCreation history.past pair = (fun w => w == .edit) := by
  funext w
  apply Bool.eq_iff_iff.mpr
  simp only [counting_iff]
  cases w with
  | create =>
    simp only [beq_iff_eq, reduceCtorEq, iff_false]
    exact post_read_refused forbiddenCreation history.past pair .create ⟨0, 0, 0⟩
      (by decide) rfl (by decide)
  | edit =>
    simp only [beq_iff_eq, iff_true]
    exact .seed (by decide) (by decide) (by decide)
  | delete =>
    simp only [beq_iff_eq, reduceCtorEq, iff_false]
    intro h
    have := counts_received h
    contradiction

theorem refused_witness_still_fails :
    Valid history ∧ ¬ SnapshotGenerations history noSnapshot (counting forbiddenCreation history.past pair) := by
  refine ⟨authored_valid, ?_⟩
  rw [forbidden_pair_counting]
  intro h
  have witness := h .edit () ⟨.upd, 1, fun _ => true⟩ (by decide) rfl
  rcases witness with impossible | ⟨w, ch, _, kept, change, kind, gen⟩
  · cases impossible
  · cases w <;> simp [history] at change <;> subst ch
    · contradiction
    · exact kind rfl
    · simp at gen

example : Valid history ∧
    ¬ SnapshotGenerations history noSnapshot (counting forbiddenCreation history.past pair) :=
  refused_witness_still_fails

theorem refused_witness_value_unaccounted (I : EntryInputs Unit Unit) :
    ¬ ValueAccounted (effective history forbiddenCreation pair)
      (inputs forbiddenCreation history.past pair creationAndEdit)
      (merged history forbiddenCreation pair St.init creationAndEdit) I () () .edit 1 := by
  have cell : (merged history forbiddenCreation pair St.init creationAndEdit).cell () () = none := by
    rw [merged, forbidden_pair_counting]; rfl
  have lost : (merged history forbiddenCreation pair St.init creationAndEdit).lost () () .edit = none := by
    rw [merged, forbidden_pair_counting]; rfl
  intro accounted
  rcases accounted with shown | ⟨w, _, read⟩ | recorded | hidden
  · simp [cell] at shown
  · have unread : (effective history forbiddenCreation pair).past w .edit = false := by
      rw [effective, forbidden_pair_counting]
      cases w <;> rfl
    simp [unread] at read
  · simp [lost] at recorded
  · simp [cell] at hidden

example (I : EntryInputs Unit Unit) :
    ¬ ValueAccounted (effective history forbiddenCreation pair)
      (inputs forbiddenCreation history.past pair creationAndEdit)
      (merged history forbiddenCreation pair St.init creationAndEdit) I () () .edit 1 :=
  refused_witness_value_unaccounted I

end CovenMerge.Receive.RetiredClosure.Examples
