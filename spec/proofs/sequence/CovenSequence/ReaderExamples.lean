import CovenSequence.Reader

/-!
# Why a snapshot must cover the last raise

C6 (a raise must cover every snapshot announced before it) is not enough to
delete covered slots safely. A snapshot announced after a raise can cover a
slot before the raise. If the floor named it and the slots it covers were
deleted, the raise's replay would need a deleted slot, so a joiner that loads
it is stuck at the raise forever, while every other device reached the head.

The version rule refuses that announcement: a snapshot announced at slot `a`
must cover the last raise of its audience before `a`. So the floor can never
name it (`Good.floor_announced`), and `deletion_keeps_reachable` holds.

Slots, in one audience: 1 and 2 are batches; 3 raises to version 1 with a
snapshot covering slot 1; 4 announces a snapshot covering slot 2, from before
the raise. A device can produce slot 4: it captures at slot 2, its claim of
slot 3 is refused by the raise, and it claims slot 4 with the same snapshot.
-/

namespace CovenSequence.ReaderExamples

open CovenSequence

/-- One device of one member, one audience; paths are numbers. -/
def sig : Sig where
  Dev := Unit
  Mem := Unit
  Key := Unit
  Path := Nat
  Batch := Nat
  Rows := List Nat
  Op := Unit
  Aux := Unit
  Aud := Unit
  decPath := inferInstanceAs (DecidableEq Nat)

def ledger0 : Ledger sig where
  member := fun _ => true
  deviceOf := fun _ => some ()
  removed := fun _ => false
  keyGen := fun _ => 0
  aux := ()
  copies := []
  refused := fun _ => false

def aud0 : AudLedger sig where
  owner := fun _ => none
  snapshots := []
  maxCover := 0
  lastRaise := 0
  version := 0
  refused := fun _ => false

def rules : Rules sig where
  authorized := fun _ _ _ => true
  applyOp := fun a _ => a
  copyOk := fun _ _ _ _ => true
  mayWrite := fun _ _ _ => true
  merge := fun (r : List Nat) (b : Nat) => r ++ [b]
  convert := fun _ _ b => b
  migrate := fun v (r : List Nat) => r ++ [1000 + v]
  ledger0 := ledger0
  aud0 := fun _ => aud0
  rows0 := fun _ => ([] : List Nat)

def part (b : Nat) : Unit → Option (WPart sig) := fun _ => some ⟨0, [], b⟩

def seqEx : Nat → Slot sig
  | 1 => .writes () 0 (part 10)
  | 2 => .writes () 0 (part 20)
  | 3 => .raise () () 1 1 (1 : Nat)
  | 4 => .snapshot () () 2 (2 : Nat)
  | _ => .writes () 0 (fun _ => none)

/-- The version rule refuses slot 4. Every other condition on it holds: its
author is active and it covers less than its slot. The raise at 3 is valid
and meets C6. -/
theorem slot4_refused :
    audValid rules (ledgerAt rules seqEx 3) (audAt rules seqEx () 3) 4 () (seqEx 4) = false ∧
    active (ledgerAt rules seqEx 3) () = true ∧ 2 < 4 ∧
    (audAt rules seqEx () 3).lastRaise = 3 ∧
    audValid rules (ledgerAt rules seqEx 2) (audAt rules seqEx () 2) 3 () (seqEx 3) = true ∧
    (audAt rules seqEx () 2).maxCover ≤ 1 := by
  decide

/-- Storage with the floor naming slot 4's snapshot, slots 1 and 2 deleted.
Path 1 holds the raise snapshot, path 2 the late snapshot's true state. -/
def st : Store sig where
  slot := fun i => if 3 ≤ i ∧ i ≤ 4 then some (seqEx i) else none
  first := fun i => if 1 ≤ i ∧ i ≤ 4 then some (seqEx i) else none
  head := 4
  floor := ⟨3, fun _ => 2, fun _ => (2 : Nat)⟩
  body := fun (p : Nat) =>
    if p = 1 then some (ledger0, aud0, rules.migrate 1 (rowsAt rules seqEx () 1))
    else some (stateAt rules seqEx () 2)

/-- The joiner after loading the floor's snapshot. -/
def joiner : Reader sig :=
  ⟨2, ledgerAt rules seqEx 2, audAt rules seqEx () 2, rowsAt rules seqEx () 2, fun _ => none⟩

/-- **Counterexample: a floor naming a snapshot from before a raise strands a
joiner.** The floor's body is the true state at its coverage and every slot at
or above the first kept slot is genuine, but the joiner, holding the key, never
moves past slot 2 while the head is 4: slot 3 is a valid raise whose replay
needs the deleted slot 2. -/
theorem stale_floor_strands_joiner :
    st.body (st.floor.path ()) = some (stateAt rules seqEx () (st.floor.covers ())) ∧
    (∀ i, st.floor.start ≤ i → st.slot i = st.first i) ∧
    ∀ d', ReadSteps rules () true st joiner d' → d'.pos = 2 := by
  refine ⟨rfl, fun i hi => ?_, fun d' run => ?_⟩
  · simp only [st] at hi ⊢
    by_cases h : i ≤ 4
    · simp [show 3 ≤ i from hi, h, show 1 ≤ i by omega]
    · simp [h]
  · suffices h : ∀ a b, ReadSteps rules () true st a b → a.pos = 2 →
        a.ledger = ledgerAt rules seqEx 2 → a.aud = audAt rules seqEx () 2 →
        (∀ i x, a.buf i = some x → st.slot i = some x) →
        b.pos = 2 ∧ b.ledger = ledgerAt rules seqEx 2 ∧ b.aud = audAt rules seqEx () 2 ∧
          (∀ i x, b.buf i = some x → st.slot i = some x) from
      (h joiner d' run rfl rfl rfl (fun _ _ h => by simp [joiner] at h)).1
    intro a b run
    induction run with
    | refl => intro h1 h2 h3 h4; exact ⟨h1, h2, h3, h4⟩
    | @step x y z s _ ih =>
      intro hp hl ha hb
      apply ih
      all_goals cases s with
      | fetch i =>
        first
          | exact hp
          | exact hl
          | exact ha
          | (intro j y hy
             simp only at hy
             by_cases hj : j = i
             · subst hj; simpa using hy
             · simp only [hj, ite_false] at hy; exact hb j y hy)
      | apply x' hb' hf hnr hk =>
        exfalso
        rw [hp] at hb'
        have h3 := hb 3 x' hb'
        simp [st, seqEx] at h3
        subst h3
        have := hnr () 1 1 (1 : Nat) rfl
        rw [hl, ha, hp] at this
        revert this
        decide
      | raise au v c p' hb' hf hv hk L' P' rows hbody get hget =>
        exfalso
        rw [hp] at hb'
        have h3 := hb 3 _ hb'
        simp [st, seqEx] at h3
        obtain ⟨-, rfl, rfl, rfl⟩ := h3
        have := (hget 2 (by decide) (by omega)).2
        simp [st] at this
      | reload L P rows hc =>
        exfalso
        rw [hp] at hc
        simp [st] at hc

end CovenSequence.ReaderExamples
