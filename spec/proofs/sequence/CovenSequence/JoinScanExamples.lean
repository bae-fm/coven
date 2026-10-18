import CovenSequence.JoinScan
import CovenSequence.ReaderExamples

namespace CovenSequence.JoinScanExamples
open CovenSequence

/-- The missing-slot rule is sound, but requires evidence of a later slot. -/
theorem missing_below_existing_is_deleted {S : Sig} (R : Rules S)
    {st : Store S} {seq : Nat → Slot S} (good : Good R st seq)
    {missing later : Nat} (positive : 1 ≤ missing) (ordered : missing < later)
    (absent : st.slot missing = none) (present : (st.slot later).isSome = true) :
    missing < st.floor.start := by
  have bound : later ≤ st.head := by
    by_cases h : later ≤ st.head
    · exact h
    · rw [good.slot_out later (Or.inr (by omega))] at present
      cases present
  by_cases h : missing < st.floor.start
  · exact h
  · rw [good.kept_genuine missing (by omega),
      good.first_genuine missing positive (by omega)] at absent
    cases absent

abbrev sig : Sig :=
  { ReaderExamples.sig with Mem := Bool, decMem := inferInstanceAs (DecidableEq Bool) }

def initial : Ledger sig where
  name := "Household"
  member := fun member => !member
  retired := fun _ => false
  role := fun _ => .admin
  deviceOf := fun _ => some false
  removed := fun _ => false
  keyGen := fun _ => 0
  aux := ()
  copies := []
  refused := fun _ => false
  claims := fun _ => none
  writeHistory := fun _ => none

def audience : AudLedger sig where
  owner := fun _ => none
  snapshots := []
  maxCover := 0
  lastBoundary := 0
  lastReset := 0
  preparations := []
  version := 0
  refused := fun _ => false

def rules : Rules sig where
  authorized := fun _ _ _ => true
  applyOp := fun a _ => a
  copyOk := fun _ _ _ _ => true
  mayWrite := fun _ _ _ => true
  merge := fun rows batch => rows ++ [batch]
  convert := fun _ _ batch => batch
  migrate := fun _ rows => rows
  resetRows := fun _ => []
  boundaryAllowed := fun _ _ _ _ => true
  sinceReset := fun _ batch => batch
  number := fun slot _ => slot
  digest := fun slot _ => slot
  ledger0 := initial
  aud0 := fun _ => audience
  rows0 := fun _ => []

/-- Ana snapshots through 12, then sends Dan's copy and admits him. -/
def history (slot : Nat) : Slot sig :=
  if slot = 13 then .snapshot () () 12 1
  else if slot = 14 then .keyCopy () true 0
  else if slot = 15 then .admin () (.addMember true .member)
  else .writes () 0 (fun _ => none)

/-- The floor captured at 13 has no copy for Dan. Retention deletes 1..12. -/
def retained : Store sig where
  slot := fun i => if 13 ≤ i ∧ i ≤ 15 then some (history i) else none
  first := fun i => if 1 ≤ i ∧ i ≤ 15 then some (history i) else none
  head := 15
  floor := ⟨13, fun _ => 12, fun _ => 1⟩
  body := fun _ => some (stateAt rules history () 12)

theorem retained_good : Good rules retained history := by
  constructor
  · intro i positive bound
    simp only [retained] at bound ⊢
    simp [positive, bound]
  · intro i outside
    simp only [retained] at outside ⊢
    simp [show ¬ (1 ≤ i ∧ i ≤ 15) by omega]
  · intro i outside
    simp only [retained] at outside ⊢
    simp [show ¬ (13 ≤ i ∧ i ≤ 15) by omega]
  · intro i kept
    simp only [retained] at kept ⊢
    by_cases bound : i ≤ 15
    · simp [kept, bound, show 1 ≤ i by omega]
    · simp [bound]
  · intro _; simp [retained]
  · intro _; simp [retained]
  · intro a; cases a; rfl
  · intro a
    cases a
    exact Or.inr ⟨13, (), by decide, by decide, rfl, by unfold AppliedTo; decide⟩
  · intro a r author kind covered path _ _ boundary _
    simp only [history] at boundary
    split at boundary <;> first
      | contradiction
      | (split at boundary <;> first
          | contradiction
          | (split at boundary <;> contradiction))

theorem approval_is_kept :
    retained.slot 14 = some (.keyCopy () true 0) ∧
    retained.slot 15 = some (.admin () (.addMember true .member)) ∧
    Applied rules history 14 ∧ Applied rules history 15 ∧
    (ledgerAt rules history 15).member true = true ∧
    (ledgerAt rules history 13).copies = [] := by
  exact ⟨rfl, rfl, by unfold Applied; decide, by unfold Applied; decide, by decide, by decide⟩

/-- Every representable exponential probe from missing slot 4 misses.
Even probing beyond the first miss cannot obtain a binary-search bracket. -/
theorem exponential_probes_miss_approval :
    ∀ exponent ∈ List.range 64,
      (retained.slot (4 + 2 ^ exponent)).isNone = true := by
  decide

theorem missing_slot_is_not_the_end :
    retained.slot 4 = none ∧ (retained.slot 14).isSome = true := by
  exact ⟨rfl, rfl⟩


end CovenSequence.JoinScanExamples
