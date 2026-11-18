namespace CovenStorage.ResetBytes

/-! §17.11, §19.4: the bytes a device keeps for its files on it only, across
resets. A record that puts a file on this device only needs this device's
bytes for it: the original's record, or coven's copy, with their hashes. A
change takes the record off this device: a write deleting it, a switch to the
cloud, or a switch to another device. The change is this device's own write,
queued until it lands at a slot, or another device's, applied at its slot.

A reset can undo such a change. It names a snapshot of some coverage, and
ignores every write outside the snapshot that didn't read the reset entry
(§19.4): one this device still queues, and one that landed after the
snapshot's coverage, even one this device applied before the entry. Every
announcement covers at least as much as every earlier one and the latest
raise or reset, and an entry names only such an announcement (§16.3,
`audValid` in the sequence model), so a later reset never names coverage below
the highest coverage announced or the slot of the last raise or reset. That
is the audience's *bound*. A change at a slot at or below the bound is in
every snapshot a later reset can name.

So this device forgets the bytes a change took off it only once the bound
reaches the change's slot, and only if no record puts the file here again.
The model follows one device and one audience. A reset brings back the record
of any file whose change it may undo, and takes off any record it likes, as
one staged after its coverage.

Proved here: in every state the steps reach, every record that puts a file on
this device has its bytes here (`reset_finds_bytes`), and so does every
change the bound hasn't reached (`kept_until_bound`). The counterexamples
show why the bytes wait for the bound: forgetting them with this device's
change (`forget_on_change_strands`), as coven did, or once the write that
made it lands (`forget_on_landing_strands`), or with another device's change
(`forget_on_receipt_strands`), lets a reset bring back a record whose bytes
are gone. -/

/-- Where a change that took a file's record off this device stands: in a
write of this device's own that hasn't landed, or applied at a slot. -/
inductive Change where
  | queued
  | landed (slot : Nat)
  deriving DecidableEq, Repr

/-- One device's files, by id: whether a record puts each on this device
only, whether this device keeps its bytes, and the latest change that took
its record off this device; the last slot this device applied, and the
audience's bound. -/
structure State where
  here : Nat → Bool
  bytes : Nat → Bool
  takenOff : Nat → Option Change
  applied : Nat
  bound : Nat

def set {α : Type} (f : Nat → α) (n : Nat) (a : α) : Nat → α :=
  fun x => if x = n then a else f x

/-- Whether a reset naming coverage `c` may undo the change `k`: one still
queued, or one at a slot after `c`. -/
def undoable (c : Nat) : Option Change → Bool
  | some .queued => true
  | some (.landed s) => decide (c < s)
  | none => false

/-- A change a reset naming more coverage may undo, one naming less may undo
too. -/
theorem undoable_lower {c d : Nat} (le : c ≤ d) {k : Option Change}
    (h : undoable d k = true) : undoable c k = true := by
  cases k with
  | none => simp [undoable] at h
  | some k =>
    cases k with
    | queued => simp [undoable]
    | landed s => simp [undoable] at h ⊢; omega

/-- Faulty variants checked as counterexamples. -/
inductive Fault where
  | none
  /-- The bytes go with the change that took the record off this device. -/
  | forgetOnChange
  /-- The bytes go once the write that made the change lands. -/
  | forgetOnLanding
  deriving DecidableEq

/-- When this device may forget a file's bytes: no record puts the file here,
and no reset a later entry may name can undo its change. The faulty variants
wait for less. -/
def mayForget (fault : Fault) (s : State) (f : Nat) : Bool :=
  match fault with
  | .none => !s.here f && !undoable s.bound (s.takenOff f)
  | .forgetOnChange => !s.here f
  | .forgetOnLanding => !s.here f && s.takenOff f != some .queued

/-- What a device and the sequence may do. `stage` records a file on this
device only, with its bytes. `takeOff` is this device's write taking a
record off it, `land` its writes landing at the next slot, and `receive`
another device's write taking a record off it at the next slot. `slot` is any
other slot, `announce` an announcement of coverage `c`, and `reset` a reset
entry naming coverage `c`, which brings back the records whose changes it
undoes. `forget` drops bytes. -/
inductive Step (fault : Fault) : State → State → Prop where
  | stage (s : State) (f : Nat) :
      Step fault s { s with here := set s.here f true, bytes := set s.bytes f true }
  | takeOff (s : State) (f : Nat) (on : s.here f = true) :
      Step fault s { s with here := set s.here f false, takenOff := set s.takenOff f (some .queued) }
  | land (s : State) (lands : Nat → Bool)
      (queued : ∀ f, lands f = true → s.takenOff f = some .queued) :
      Step fault s { s with
        applied := s.applied + 1
        takenOff := fun f => if lands f then some (.landed (s.applied + 1)) else s.takenOff f }
  | receive (s : State) (f : Nat) (on : s.here f = true) :
      Step fault s { s with
        applied := s.applied + 1
        here := set s.here f false
        takenOff := set s.takenOff f (some (.landed (s.applied + 1))) }
  | slot (s : State) : Step fault s { s with applied := s.applied + 1 }
  | announce (s : State) (c : Nat) (above : s.bound ≤ c) (below : c ≤ s.applied) :
      Step fault s { s with applied := s.applied + 1, bound := c }
  | reset (s : State) (c : Nat) (back gone : Nat → Bool)
      (above : s.bound ≤ c) (below : c ≤ s.applied)
      (undone : ∀ f, back f = true → undoable c (s.takenOff f) = true) :
      Step fault s { s with
        applied := s.applied + 1
        bound := s.applied + 1
        here := fun f => (s.here f && !gone f) || back f }
  | forget (s : State) (f : Nat) (may : mayForget fault s f = true) :
      Step fault s { s with bytes := set s.bytes f false }

inductive Steps (fault : Fault) : State → State → Prop where
  | refl (s : State) : Steps fault s s
  | step {a b c : State} : Step fault a b → Steps fault b c → Steps fault a c

def State.empty : State := ⟨fun _ => false, fun _ => false, fun _ => none, 0, 0⟩

/-- What every reachable state satisfies: a record here has its bytes, and
so does every change a reset a later entry may name can undo. -/
structure Inv (s : State) : Prop where
  here_bytes : ∀ f, s.here f = true → s.bytes f = true
  kept_bytes : ∀ f, undoable s.bound (s.takenOff f) = true → s.bytes f = true
  bound_applied : s.bound ≤ s.applied

theorem inv_empty : Inv State.empty :=
  ⟨by simp [State.empty], by simp [State.empty, undoable], by simp [State.empty]⟩

theorem step_inv {s t : State} (st : Step .none s t) (hi : Inv s) : Inv t := by
  cases st with
  | stage f =>
    refine ⟨fun x hx => ?_, fun x hx => ?_, hi.bound_applied⟩
    · by_cases hn : x = f
      · subst hn; simp [set]
      · simp only [set, hn, ite_false] at hx ⊢; exact hi.here_bytes x hx
    · by_cases hn : x = f
      · subst hn; simp [set]
      · simp only [set, hn, ite_false]; exact hi.kept_bytes x hx
  | takeOff f on =>
    refine ⟨fun x hx => ?_, fun x hx => ?_, hi.bound_applied⟩
    · by_cases hn : x = f
      · subst hn; simp [set] at hx
      · simp only [set, hn, ite_false] at hx; exact hi.here_bytes x hx
    · by_cases hn : x = f
      · subst hn; exact hi.here_bytes x on
      · simp only [set, hn, ite_false] at hx; exact hi.kept_bytes x hx
  | land lands queued =>
    refine ⟨hi.here_bytes, fun x hx => ?_, by simp only; have := hi.bound_applied; omega⟩
    by_cases hl : lands x = true
    · exact hi.kept_bytes x (by simp [queued x hl, undoable])
    · simp only [hl, Bool.false_eq_true, ite_false] at hx; exact hi.kept_bytes x hx
  | receive f on =>
    refine ⟨fun x hx => ?_, fun x hx => ?_, by simp only; have := hi.bound_applied; omega⟩
    · by_cases hn : x = f
      · subst hn; simp [set] at hx
      · simp only [set, hn, ite_false] at hx; exact hi.here_bytes x hx
    · by_cases hn : x = f
      · subst hn; exact hi.here_bytes x on
      · simp only [set, hn, ite_false] at hx; exact hi.kept_bytes x hx
  | slot => exact ⟨hi.here_bytes, hi.kept_bytes, by simp only; have := hi.bound_applied; omega⟩
  | announce c above below =>
    exact ⟨hi.here_bytes, fun x hx => hi.kept_bytes x (undoable_lower above hx),
      by simp only; omega⟩
  | reset c back gone above below undone =>
    refine ⟨fun x hx => ?_, fun x hx => ?_, by simp only; omega⟩
    · simp only [Bool.or_eq_true, Bool.and_eq_true] at hx
      rcases hx with ⟨h, _⟩ | hb
      · exact hi.here_bytes x h
      · exact hi.kept_bytes x (undoable_lower above (undone x hb))
    · exact hi.kept_bytes x (undoable_lower (show s.bound ≤ s.applied + 1 by have := hi.bound_applied; omega) hx)
  | forget f may =>
    simp only [mayForget, Bool.and_eq_true, Bool.not_eq_true'] at may
    refine ⟨fun x hx => ?_, fun x hx => ?_, hi.bound_applied⟩
    · by_cases hn : x = f
      · subst hn; simp [may.1] at hx
      · simp only [set, hn, ite_false]; exact hi.here_bytes x hx
    · by_cases hn : x = f
      · subst hn; simp [may.2] at hx
      · simp only [set, hn, ite_false]; exact hi.kept_bytes x hx

theorem steps_inv {s t : State} (run : Steps .none s t) (hi : Inv s) : Inv t := by
  induction run with
  | refl => exact hi
  | step st _ ih => exact ih (step_inv st hi)

/-- **A reset finds the bytes of every record it brings back:** in every
reachable state, a record that puts a file on this device has its bytes
here. -/
theorem reset_finds_bytes {s : State} (run : Steps .none State.empty s) (f : Nat)
    (on : s.here f = true) : s.bytes f = true :=
  (steps_inv run inv_empty).here_bytes f on

/-- **The bytes wait for the bound:** in every reachable state, a change
still queued, or at a slot the bound hasn't reached, keeps its file's bytes
here, wherever the record is now. -/
theorem kept_until_bound {s : State} (run : Steps .none State.empty s) (f : Nat)
    (waiting : undoable s.bound (s.takenOff f) = true) : s.bytes f = true :=
  (steps_inv run inv_empty).kept_bytes f waiting

/-- **The bound only rises,** so a change it has reached stays in every
snapshot a later reset can name. -/
theorem bound_rises {fault : Fault} {s t : State} (st : Step fault s t) (hi : Inv s) :
    s.bound ≤ t.bound := by
  cases st with
  | announce c above below => exact above
  | reset c back gone above below undone => simp only; have := hi.bound_applied; omega
  | _ => exact Nat.le_refl _

/-- The record of file 1 on this device, with its bytes. -/
def staged : State :=
  { State.empty with
    here := set State.empty.here 1 true
    bytes := set State.empty.bytes 1 true }

/-- A reset naming coverage 0 that brings back file 1. -/
def resetBack (s : State) : State :=
  { s with
    applied := s.applied + 1
    bound := s.applied + 1
    here := fun f => (s.here f && !(fun _ => false) f) || (fun x => x == 1) f }

/-- **Counterexample: forgetting the bytes with this device's change.** This
device deletes the last row naming a file on it only and forgets the file's
bytes at once; a reset that ignores the queued write brings the record back,
with no bytes here. -/
theorem forget_on_change_strands :
    ∃ s, Steps .forgetOnChange State.empty s ∧ s.here 1 = true ∧ s.bytes 1 = false := by
  let s1 : State := { staged with
    here := set staged.here 1 false
    takenOff := set staged.takenOff 1 (some .queued) }
  let s2 : State := { s1 with bytes := set s1.bytes 1 false }
  exact ⟨resetBack s2,
    Steps.step (Step.stage _ 1)
      (Steps.step (Step.takeOff staged 1 (by simp [staged, set]))
      (Steps.step (Step.forget s1 1 (by simp [mayForget, s1, set]))
      (Steps.step (Step.reset s2 0 (fun x => x == 1) (fun _ => false)
        (by simp [s2, s1, staged, State.empty]) (by simp)
        (by intro f hf; simp at hf; subst hf; simp [s2, s1, set, undoable]))
      (Steps.refl _)))),
    by simp [resetBack], by simp [resetBack, s2, set]⟩

/-- **Counterexample: forgetting the bytes once the write lands.** The
write deleting the record lands at slot 1, and this device forgets the bytes
as it consumes it; a reset naming coverage 0 still ignores the write and
brings the record back, with no bytes here. -/
theorem forget_on_landing_strands :
    ∃ s, Steps .forgetOnLanding State.empty s ∧ s.here 1 = true ∧ s.bytes 1 = false := by
  let s1 : State := { staged with
    here := set staged.here 1 false
    takenOff := set staged.takenOff 1 (some .queued) }
  let s2 : State := { s1 with
    applied := s1.applied + 1
    takenOff := fun f => if (fun x => x == 1) f then some (.landed (s1.applied + 1))
      else s1.takenOff f }
  let s3 : State := { s2 with bytes := set s2.bytes 1 false }
  exact ⟨resetBack s3,
    Steps.step (Step.stage _ 1)
      (Steps.step (Step.takeOff staged 1 (by simp [staged, set]))
      (Steps.step (Step.land s1 (fun x => x == 1)
        (by intro f hf; simp at hf; subst hf; simp [s1, set]))
      (Steps.step (Step.forget s2 1 (by simp [mayForget, s2, s1, set]))
      (Steps.step (Step.reset s3 0 (fun x => x == 1) (fun _ => false)
        (by simp [s3, s2, s1, staged, State.empty]) (by simp [s3, s2])
        (by intro f hf; simp at hf; subst hf
            simp [s3, s2, s1, staged, State.empty, undoable]))
      (Steps.refl _))))),
    by simp [resetBack], by simp [resetBack, s3, set]⟩

/-- **Counterexample: forgetting the bytes with another device's change.**
Ben's write deleting the record applies at slot 1, and this device forgets
the bytes; a reset naming coverage 0 ignores Ben's write and brings the
record back, with no bytes here. -/
theorem forget_on_receipt_strands :
    ∃ s, Steps .forgetOnChange State.empty s ∧ s.here 1 = true ∧ s.bytes 1 = false := by
  let s1 : State := { staged with
    applied := staged.applied + 1
    here := set staged.here 1 false
    takenOff := set staged.takenOff 1 (some (.landed (staged.applied + 1))) }
  let s2 : State := { s1 with bytes := set s1.bytes 1 false }
  exact ⟨resetBack s2,
    Steps.step (Step.stage _ 1)
      (Steps.step (Step.receive staged 1 (by simp [staged, set]))
      (Steps.step (Step.forget s1 1 (by simp [mayForget, s1, set]))
      (Steps.step (Step.reset s2 0 (fun x => x == 1) (fun _ => false)
        (by simp [s2, s1, staged, State.empty]) (by simp [s2, s1])
        (by intro f hf; simp at hf; subst hf
            simp [s2, s1, staged, State.empty, set, undoable]))
      (Steps.refl _)))),
    by simp [resetBack], by simp [resetBack, s2, set]⟩

end CovenStorage.ResetBytes
