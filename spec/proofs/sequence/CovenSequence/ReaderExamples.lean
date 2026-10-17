import CovenSequence.Reader

/-!
# C6 alone does not remove the raise hold

C6 (a raise must cover every snapshot announced before it) is not enough to
delete covered slots safely. A snapshot announced after a raise can cover a
slot before the raise. Raising the floor to it and deleting the slots it
covers removes slots the raise's replay needs, so a joiner that loads it is
stuck at the raise forever, while every other device reached the head.

The rule that closes this is `versionRule`: a snapshot announced at slot `a`
must cover the last raise before `a`, so it is at the store's version. With it
the same announcement is refused, and `deletion_keeps_reachable` holds.

Slots: 1 and 2 are batches; 3 raises to version 1 with a snapshot covering
slot 1; 4 announces a snapshot covering slot 2, from before the raise.
-/

namespace CovenSequence.ReaderExamples

open CovenSequence

/-- One device of one member; paths are numbers. -/
def sig : Sig where
  Dev := Unit
  Mem := Unit
  Key := Unit
  Path := Nat
  Batch := Nat
  Rows := List Nat
  Op := Unit
  Aux := Unit
  decPath := inferInstanceAs (DecidableEq Nat)

def ledger0 : Ledger sig where
  member := fun _ => true
  deviceOf := fun _ => some ()
  removed := fun _ => false
  keyGen := fun _ => 0
  aux := ()
  owner := fun _ => none
  copies := []
  snapshots := []
  maxCover := 0
  lastRaise := 0
  version := 0
  refused := fun _ => false

/-- The rules with or without the version rule. -/
def rules (versionRule : Bool) : Rules sig where
  authorized := fun _ _ _ => true
  applyOp := fun a _ => a
  copyOk := fun _ _ _ _ => true
  merge := fun (r : List Nat) (b : Nat) => r ++ [b]
  convert := fun _ _ b => b
  migrate := fun v (r : List Nat) => r ++ [1000 + v]
  ledger0 := ledger0
  rows0 := ([] : List Nat)
  versionRule := versionRule

/-- C6 only. -/
abbrev R6 := rules false

def seqEx : Nat → Slot sig
  | 1 => .writes () 0 0 [] (10 : Nat)
  | 2 => .writes () 0 0 [] (20 : Nat)
  | 3 => .raise () 1 1 (1 : Nat)
  | 4 => .snapshot () 2 (2 : Nat)
  | _ => .writes () 0 9 [] (0 : Nat)

/-- Every slot claimed, the floor at the start. Path 0 holds the empty state,
path 1 the raise snapshot, path 2 the late snapshot. -/
def st0 : Storage sig where
  slot := fun i => if 1 ≤ i ∧ i ≤ 4 then some (seqEx i) else none
  floorStart := 1
  floorCovers := 0
  floorPath := (0 : Nat)
  body := fun (p : Nat) =>
    if p = 0 then some (stateAt R6 seqEx 0)
    else if p = 1 then some (ledger0, R6.migrate 1 (rowsAt R6 seqEx 1))
    else some (stateAt R6 seqEx 2)

/-- The floor raised to the late snapshot: the log starts at slot 3. -/
def st1 : Storage sig := { st0 with floorStart := 3, floorCovers := 2, floorPath := (2 : Nat) }
/-- Slots 1 and 2 deleted. -/
def st2 : Storage sig := { st1 with slot := fun j => if j = 1 then none else st1.slot j }
def st3 : Storage sig := { st2 with slot := fun j => if j = 2 then none else st2.slot j }

/-- A joiner after loading the floor's snapshot. -/
def stuck : Reader sig := ⟨2, ledgerAt R6 seqEx 2, rowsAt R6 seqEx 2⟩

theorem st0_good : Good R6 st0 seqEx 4 := by
  refine ⟨fun i x h => ?_, fun i h1 h2 h3 => ?_, by decide, by decide, rfl, Or.inl rfl, ?_⟩
  · simp only [st0] at h
    split at h
    · rename_i hi; simp at h; exact ⟨hi.1, hi.2, h.symm⟩
    · cases h
  · simp only [st0]; simp [h2, h3]
  · intro r a v c p h1 h2 hs _
    rcases (by omega : r = 1 ∨ r = 2 ∨ r = 3 ∨ r = 4) with rfl | rfl | rfl | rfl
    · simp [seqEx] at hs
    · simp [seqEx] at hs
    · simp only [seqEx] at hs
      obtain ⟨-, rfl, rfl, rfl⟩ := hs
      refine ⟨fun _ => ⟨ledger0, by simp [st0]⟩, fun L' rows hb => ?_⟩
      simp [st0] at hb
      exact hb.2.symm
    · simp [seqEx] at hs

theorem slot4_applied : Applied R6 seqEx 4 := by
  unfold Applied; rfl

theorem retained_to_st3 : Retains R6 seqEx 4 st0 st3 :=
  Retains.step (Retain.raiseFloor st0 3 4 () 2 (2 : Nat) (by decide) (by decide) (by decide) (by decide) (by decide)
      rfl slot4_applied (by simp [st0]))
    (Retains.step (Retain.delete st1 1 (by decide))
      (Retains.step (Retain.delete st2 2 (by decide)) (Retains.refl st3)))

/-- **Counterexample: C6 alone strands a reader.** Storage starts good, the
floor is raised to the snapshot announced at slot 4 (valid under C6 alone),
slots 1 and 2 are deleted as covered, and storage stays good. A joiner loads
the floor's snapshot and is then stuck below the head: slot 3 is a valid raise
whose replay needs the deleted slot 2. With the version rule, slot 4 is
refused, so the floor could never name it. -/
theorem c6_alone_strands_reader :
    Good R6 st0 seqEx 4 ∧ Retains R6 seqEx 4 st0 st3 ∧ Good R6 st3 seqEx 4 ∧
    ReadStep R6 st3 ⟨0, ledger0, []⟩ stuck ∧ stuck.pos < 4 ∧
    (¬ ∃ d', ReadStep R6 st3 stuck d') ∧
    valid (rules true) (ledgerAt (rules true) seqEx 3) 4 (seqEx 4) = false := by
  have hgood3 : Good R6 st3 seqEx 4 :=
    retain_good R6 (retain_good R6 (retain_good R6 st0_good
      (Retain.raiseFloor st0 3 4 () 2 (2 : Nat) (by decide) (by decide) (by decide) (by decide) (by decide)
        rfl slot4_applied (by simp [st0])))
      (Retain.delete st1 1 (by decide))) (Retain.delete st2 2 (by decide))
  refine ⟨st0_good, retained_to_st3, hgood3, ?_, by decide, ?_, rfl⟩
  · exact ReadStep.reload _ _ _ (by decide) (by simp [st3, st2, st1, st0, stateAt])
  · rintro ⟨d', step⟩
    cases step with
    | apply x hx hnr =>
      have hx3 : x = seqEx 3 := by
        simp [st3, st2, st1, st0, stuck] at hx; exact hx.symm
      subst hx3
      have := hnr () 1 1 (1 : Nat) rfl
      revert this
      simp only [stuck]
      decide
    | raise a v c p hx hv L' rows hb get hget =>
      simp [st3, st2, st1, st0, stuck, seqEx] at hx
      obtain ⟨-, rfl, rfl, rfl⟩ := hx
      have := hget 2 (by decide) (by decide)
      simp [st3] at this
    | reload L rows hc hb =>
      simp [st3, st2, st1, st0, stuck] at hc

end CovenSequence.ReaderExamples
