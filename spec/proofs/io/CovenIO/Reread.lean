/-! What a refusal judges (§19.1, "Refusals and failures"), as
`SequenceSync::block` confirms a refused slot, a file's fetch confirms a
refused chunk, a snapshot's read confirms a refused prefix or body, and
`SequenceSync::judge_staged` confirms a check that refused a snapshot's
records as this device staged them, against a second staging.

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

/-! A snapshot is announced in a slot with the digest of its bytes, and is
kept nowhere whole: its body streams once (§16.2). Bytes whose digest matches
the announcement are the announced bytes, so the announcement checks their
transfer, and a check that refuses them needs no second read. A snapshot whose
digest differs from its announcement is read again, keeping only the first
read's digest, and refused as not the announced object only when the second
read's digest is the first's (`passAnnounced`). The model takes two digests
to be equal exactly when their bytes are, and a fault never to produce the
announced bytes when storage holds other bytes (`Announced.unforged`).

Proved here: a refusal of an announced object is a verdict on what storage
holds: it is not the announced object, or the announced object fails the check
(`announced_refusal_judges_stored`); an honestly stored object that passes is
never refused (`honest_announced_never_refused`). Refused as soon as its
digest differs, one fault makes an honestly stored snapshot not the announced
one (`digest_once_refuses_honest`); read again, the next read applies it
(`announced_fault_recovers`). -/

/-- An announced object's reads: what its announcement says its bytes are. -/
structure Announced (Bytes : Type) extends Reads Bytes where
  announced : Bytes
  unforged : ∀ i, read i = announced → stored = announced

/-- A read whose bytes are the announced ones is judged at once; any other
is read again, and refused only when the two reads agree. -/
def passAnnounced (refuses : Bytes → Bool) (r : Announced Bytes) (k : Nat) : Outcome :=
  if r.read k = r.announced then
    if refuses (r.read k) then .refused else .applies
  else if r.read (k + 1) = r.read k then .refused else .failedRead

theorem announced_refusal_judges_stored (refuses : Bytes → Bool) (r : Announced Bytes)
    (k : Nat) (refused : passAnnounced refuses r k = .refused) :
    r.stored ≠ r.announced ∨ refuses r.stored = true := by
  unfold passAnnounced at refused
  split at refused
  · rename_i matched
    split at refused
    · rename_i judged
      right
      rw [r.unforged k matched, ← matched]
      exact judged
    · cases refused
  · rename_i differs
    split at refused
    · rename_i same
      left
      have stored := r.agree (k + 1) k (Nat.succ_ne_self k) same
      rw [same] at stored
      rw [← stored]
      exact differs
    · cases refused

theorem honest_announced_never_refused (refuses : Bytes → Bool) (r : Announced Bytes)
    (k : Nat) (stored : r.stored = r.announced) (honest : refuses r.announced = false) :
    passAnnounced refuses r k ≠ .refused := by
  intro refused
  rcases announced_refusal_judges_stored refuses r k refused with differs | judged
  · exact differs stored
  · rw [stored, honest] at judged
    cases judged

/-- Refusing as soon as the digest differs from the announcement. -/
def digestOnce (refuses : Bytes → Bool) (r : Announced Bytes) (k : Nat) : Outcome :=
  if r.read k = r.announced then
    if refuses (r.read k) then .refused else .applies
  else .refused

/-- The snapshot `0`, announced as `0`, whose first read a fault turned
into `1`. -/
def faultyAnnounced : Announced Nat where
  toReads := faulty
  announced := 0
  unforged _ _ := rfl

theorem digest_once_refuses_honest :
    faultyAnnounced.stored = faultyAnnounced.announced ∧
      digestOnce refusesFaults faultyAnnounced 0 = .refused := by
  decide

theorem announced_fault_recovers :
    passAnnounced refusesFaults faultyAnnounced 0 = .failedRead ∧
      passAnnounced refusesFaults faultyAnnounced 2 = .applies := by
  decide

/-! A file's cached copy (§17.8) is kept only once it passed the check, and a
chunk's check is its authentication: bytes that pass it are the stored bytes
(`Cached.authentic`). So a cached copy that fails its check changed on this
device after it was kept, and says nothing about the stored file. The device
reads the file from storage instead, judges those reads as `pass` does, and
writes the stored bytes back over the copy (`cachedPass`).

Proved here: the stored bytes of a file with a cached copy pass
(`cached_stored_passes`), so a file whose cached copy fails its check is never
refused (`damaged_copy_never_refused`), and a copy that passes applies with no
read of storage (`passing_copy_applies`). Refused on the copy instead, a file
whose stored bytes pass is refused (`copy_once_refuses_honest`). -/

/-- An object's storage reads, with the cached copy of it this device kept
once it passed `refuses`, and what that copy holds now. -/
structure Cached (Bytes : Type) (refuses : Bytes → Bool) extends Reads Bytes where
  kept : Bytes
  passed : refuses kept = false
  authentic : ∀ b, refuses b = false → b = stored
  copy : Bytes

/-- A copy that passes is read; one that fails is set aside, and storage's
reads are judged from read `k`. -/
def cachedPass (refuses : Bytes → Bool) (c : Cached Bytes refuses) (k : Nat) : Outcome :=
  if refuses c.copy then pass refuses c.toReads k else .applies

omit [DecidableEq Bytes] in
theorem cached_stored_passes (refuses : Bytes → Bool) (c : Cached Bytes refuses) :
    refuses c.stored = false := by
  rw [← c.authentic c.kept c.passed]
  exact c.passed

theorem damaged_copy_never_refused (refuses : Bytes → Bool) (c : Cached Bytes refuses)
    (k : Nat) : cachedPass refuses c k ≠ .refused := by
  unfold cachedPass
  split
  · exact honest_never_refused refuses c.toReads k (cached_stored_passes refuses c)
  · intro applies
    cases applies

theorem passing_copy_applies (refuses : Bytes → Bool) (c : Cached Bytes refuses) (k : Nat)
    (passes : refuses c.copy = false) : cachedPass refuses c k = .applies := by
  simp [cachedPass, passes]

/-- Refusing on the cached copy alone. -/
def copyOnce (refuses : Bytes → Bool) (c : Cached Bytes refuses) : Outcome :=
  if refuses c.copy then .refused else .applies

/-- The file `0`, kept as `0`, whose copy turned into `1` on this device. -/
def damagedCopy : Cached Nat refusesFaults where
  toReads := faulty
  kept := 0
  passed := rfl
  authentic b passes := by simpa [refusesFaults, faulty] using passes
  copy := 1

theorem copy_once_refuses_honest :
    refusesFaults damagedCopy.stored = false ∧ copyOnce refusesFaults damagedCopy = .refused := by
  decide

end CovenIO.Reread
