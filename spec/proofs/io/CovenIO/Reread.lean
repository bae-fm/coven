/-! What a refusal judges (§19.1, "Refusals and failures"), as
`SequenceSync::block` confirms a refused slot.

A refusal claims the stored object fails its check, and stops the device for
good under its build. But a device judges the bytes a read returned, and a
slot carries nothing that checks its transfer. A read returns the stored
bytes, or bytes a fault on the way changed. A fault changes them its own way,
so two different reads agree only when both returned the stored bytes
(`Reads.agree`).

So a check that refuses the kept copy of an object (read `k`) is not yet a
refusal: the object is read again (read `k + 1`), and refused only when the
two reads agree. When they differ, the read failed: the kept copy is dropped,
and the next pass reads the object again and judges that read (`pass`).

Proved here: a refusal is a verdict on the stored bytes
(`refusal_judges_stored`), so an object whose stored bytes pass is never
refused, whatever its reads returned (`honest_never_refused`), and a kept copy
that is the stored bytes applies (`stored_copy_applies`). Refusing on one
read instead, one fault in transfer refuses an honest object, and since every
later check of the kept copy refuses it again, no build ever applies it
(`one_read_refuses_honest`); with the second read, the same faulty read fails
the pass, and the next pass reads the object again and applies it
(`fault_recovers`). -/

namespace CovenIO.Reread

/-- What the complete reads of one immutable object returned: `read i` is
the `i`th read's bytes. Two different reads agree only on the stored bytes. -/
structure Reads (Bytes : Type) where
  stored : Bytes
  read : Nat → Bytes
  agree : ∀ i j, i ≠ j → read i = read j → read i = stored

/-- What one pass does with the object. -/
inductive Outcome where
  | applies | refused | failedRead
  deriving DecidableEq, Repr

variable {Bytes : Type} [DecidableEq Bytes]

/-- A pass judges the kept copy, read `k`; `refuses b` is whether the running
build's check refuses bytes `b`. A refused copy is read again, and the object
is refused only when the second read returns the same bytes. Otherwise the
read failed, the kept copy is dropped, and a later pass judges a later read. -/
def pass (refuses : Bytes → Bool) (r : Reads Bytes) (k : Nat) : Outcome :=
  if refuses (r.read k) then
    if r.read (k + 1) = r.read k then .refused else .failedRead
  else .applies

theorem refusal_judges_stored (refuses : Bytes → Bool) (r : Reads Bytes) (k : Nat)
    (refused : pass refuses r k = .refused) : refuses r.stored = true := by
  unfold pass at refused
  split at refused
  · rename_i judged
    split at refused
    · rename_i same
      rw [← r.agree (k + 1) k (Nat.succ_ne_self k) same, same]
      exact judged
    · cases refused
  · cases refused

theorem honest_never_refused (refuses : Bytes → Bool) (r : Reads Bytes) (k : Nat)
    (honest : refuses r.stored = false) : pass refuses r k ≠ .refused := by
  intro refused
  have := refusal_judges_stored refuses r k refused
  simp_all

theorem stored_copy_applies (refuses : Bytes → Bool) (r : Reads Bytes) (k : Nat)
    (kept : r.read k = r.stored) (honest : refuses r.stored = false) :
    pass refuses r k = .applies := by
  simp [pass, kept, honest]

/-- Refusing on the kept copy alone, as a single read does. -/
def passOnce (refuses : Bytes → Bool) (r : Reads Bytes) (k : Nat) : Outcome :=
  if refuses (r.read k) then .refused else .applies

/-- The object `0`, whose first read a fault turned into `1`; every later
read returns it as stored. -/
def faulty : Reads Nat where
  stored := 0
  read i := if i = 0 then 1 else 0
  agree i j differ same := by
    by_cases hi : i = 0
    · subst hi
      by_cases hj : j = 0
      · exact absurd hj.symm differ
      · simp [hj] at same
    · simp [hi]

/-- The check that refuses every bytes but the honest object's. -/
def refusesFaults (b : Nat) : Bool := b != 0

theorem one_read_refuses_honest :
    refusesFaults faulty.stored = false ∧ passOnce refusesFaults faulty 0 = .refused := by
  decide

theorem fault_recovers :
    pass refusesFaults faulty 0 = .failedRead ∧ pass refusesFaults faulty 2 = .applies := by
  decide

end CovenIO.Reread
