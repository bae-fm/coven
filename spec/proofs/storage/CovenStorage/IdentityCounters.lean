import CovenStorage.Identity

/-! §10: three rules the identity check and reset follow, beyond `checkIdentity`.

- Own key copies have their own counter. Each writer reserves its copies
  gap-free from one sequence and confirms them in order, so a live copy's
  stored copies never pass its reservation, and one beyond it resets:
  `live_copy_sends`, `key_copy_ahead_resets`.
- Own snapshot numbers are operation ids: one local sequence numbers every
  operation, and a snapshot takes its operation's id. Comparing stored
  snapshot numbers against that sequence never resets a live copy
  (`live_copy_sends`), and every snapshot reserved after a passing check gets
  a number beyond every listed one (`later_numbers_fresh`). It is weaker than
  §10's comparison with the last reserved snapshot number: a restored copy's
  other operations advance the sequence past a newer own snapshot, so the
  restored copy sends, and its own earlier snapshot operation publishes at a
  path its newer self used and deleted (`operation_ids_hide_newer_snapshot`).
- A store that never set up storage has no stored history and published
  nothing. Its reset is the new-device path with nothing to load and no
  registration: an empty database under the fresh id, whose first write is
  a new path (`unconnected_reset_is_new_device`, `unconnected_reset_empty`,
  `unconnected_first_write_new_path`). -/

namespace CovenStorage.IdentityCounters
open CovenStorage

/-! ## Reservation sequences -/

/-- One local numbering sequence. `last` is the greatest number it reserved;
`kind` holds the reserved numbers that name an object of the counted kind.
Writes, entries and key copies reserve only their own kind. The operation
sequence (`sqlite_sequence` of `_coven_operations`) numbers every operation,
and only a snapshot operation's id is a snapshot number. Finishing an
operation never lowers the sequence. -/
structure Sequence where
  last : Nat
  kind : List Nat
  deriving DecidableEq, Repr

def Sequence.empty : Sequence := ⟨0, []⟩

/-- Reserve the next number, for an object of the counted kind or another. -/
def Sequence.reserve (s : Sequence) (counted : Bool) : Sequence :=
  ⟨s.last + 1, if counted then (s.last + 1) :: s.kind else s.kind⟩

def Sequence.run (s : Sequence) (steps : List Bool) : Sequence :=
  steps.foldl Sequence.reserve s

/-- Every counted number is one the sequence reserved. -/
def Sequence.Valid (s : Sequence) : Prop := ∀ n ∈ s.kind, 1 ≤ n ∧ n ≤ s.last

theorem empty_valid : Sequence.empty.Valid := by
  simp [Sequence.Valid, Sequence.empty]

theorem reserve_valid {s : Sequence} (counted : Bool) (h : s.Valid) :
    (s.reserve counted).Valid := by
  intro n member
  cases counted with
  | false => have := h n member; simp [Sequence.reserve] at *; omega
  | true =>
      simp only [Sequence.reserve, ite_true, List.mem_cons] at member ⊢
      rcases member with rfl | member
      · omega
      · have := h n member; omega

theorem run_valid {s : Sequence} (steps : List Bool) (h : s.Valid) : (s.run steps).Valid := by
  induction steps generalizing s with
  | nil => exact h
  | cons b bs ih => exact ih (reserve_valid b h)

/-- The greatest number in a list, zero for none: what a listing or a counter
of one kind reports. -/
def greatest (l : List Nat) : Nat := l.foldr max 0

theorem greatest_le {l : List Nat} {m : Nat} (h : ∀ n ∈ l, n ≤ m) : greatest l ≤ m := by
  induction l with
  | nil => simp [greatest]
  | cons x xs ih =>
      simp only [greatest, List.foldr_cons] at *
      have hx := h x List.mem_cons_self
      have rest := ih fun n hn => h n (List.mem_cons_of_mem _ hn)
      omega

/-- Storage lists only own objects this copy published, from numbers it
reserved; deletion only removes. So the listing never passes the sequence. -/
theorem listed_within {s : Sequence} (valid : s.Valid) (listed : List Nat)
    (published : ∀ n ∈ listed, n ∈ s.kind) : greatest listed ≤ s.last :=
  greatest_le fun n hn => (valid n (published n hn)).2

/-- Every number the sequence reserves after a moment exceeds everything it
had reserved then, so after a check that found nothing beyond the sequence,
new numbers are beyond every listed one. -/
theorem later_numbers_fresh {s : Sequence} (steps : List Bool) {m : Nat} (h : m ≤ s.last) :
    ∀ n ∈ (s.run steps).kind, n ∉ s.kind → m < n := by
  induction steps generalizing s with
  | nil => intro n member fresh; exact absurd member fresh
  | cons b bs ih =>
      intro n member fresh
      by_cases old : n ∈ (s.reserve b).kind
      · cases b with
        | false => exact absurd old (by simpa [Sequence.reserve] using fresh)
        | true =>
            simp only [Sequence.reserve, ite_true, List.mem_cons] at old
            rcases old with rfl | old
            · omega
            · exact absurd old fresh
      · exact ih (by simp [Sequence.reserve]; omega) n member old

/-! ## A live copy is never reset by its own key copies or snapshots -/

/-- One live copy's own counters: write and entry ends, its key-copy sequence
(every reservation a copy, Rust `key_upload::last_reserved`) and its
operation sequence (Rust `sqlite_sequence`). Stored copies and snapshots are
ones it published. With custody and no removal or replacement, the check
sends, whatever it deleted (§10: an outstanding reservation is not stale). -/
theorem live_copy_sends (d : Nat) (logs : Ends) (keys ops : Sequence)
    (keysValid : keys.Valid) (opsValid : ops.Valid) (copies snapshots : List Nat)
    (copiesPublished : ∀ n ∈ copies, n ∈ keys.kind)
    (snapshotsPublished : ∀ n ∈ snapshots, n ∈ ops.kind) (e : Evidence)
    (removed : e.removed = false) (replaced : e.replaced = false)
    (listedCopies : e.listed.keyCopies = greatest copies)
    (listedSnapshots : e.listed.snapshot = greatest snapshots)
    (logsWithin : ∀ evidence ∈ [e.listed.logs, e.covered, e.posted],
      evidence.writes ≤ logs.writes ∧ evidence.entries ≤ logs.entries) :
    checkIdentity d ⟨logs, keys.last, ops.last⟩ (some d) (.ok e) = .send :=
  send_of_within d _ e removed replaced
    (listedCopies ▸ listed_within keysValid copies copiesPublished)
    (listedSnapshots ▸ listed_within opsValid snapshots snapshotsPublished) logsWithin

/-- A stored own key copy beyond the reservation resets the copy. -/
theorem key_copy_ahead_resets (d : Nat) (r : Counters) (e : Evidence)
    (removed : e.removed = false) (replaced : e.replaced = false)
    (ahead : r.keyCopies < e.listed.keyCopies) :
    checkIdentity d r (some d) (.ok e) = .reset .storageAhead := by
  have : e.observed.ahead r = true := by
    simp [Counters.ahead, Evidence.observed, ahead]
  simp [checkIdentity, removed, replaced, this]

/-- A key-copy writer reserves only copies: its counter is its sequence. -/
theorem key_copies_counted (steps : List Bool) (copies : ∀ b ∈ steps, b = true) :
    greatest (Sequence.empty.run steps).kind = (Sequence.empty.run steps).last := by
  have shape : ∀ (s : Sequence), greatest s.kind = s.last →
      greatest (s.run steps).kind = (s.run steps).last := by
    induction steps with
    | nil => intro s h; exact h
    | cons b bs ih =>
        intro s h
        have hb := copies b List.mem_cons_self
        subst hb
        exact (ih fun c hc => copies c (List.mem_cons_of_mem _ hc)) _ (by
          simp only [Sequence.reserve, ite_true, greatest, List.foldr_cons] at h ⊢
          omega)
  exact shape _ rfl

/-! ## The operation sequence hides a newer snapshot -/

/-- Ana's device 7 at the backup: operations 1 to 5, none a snapshot. -/
def backupOps : Sequence := ⟨5, []⟩

/-- Her live copy then writes store snapshots 6 and 7. Snapshot 7 covers
everything 6 covers, so she deletes 6 (§15); storage lists only 7. -/
def liveOps : Sequence := (backupOps.reserve true).reserve true
def stillListed : List Nat := [7]

/-- The backup is restored with custody intact. Before its first check it
starts one snapshot operation, say a schema raise (id 6), and one other
operation, say an invite (id 7). -/
def restoredOps : Sequence := (backupOps.reserve true).reserve false

/-- Nothing else separates the copies: same write and entry ends. -/
def ends : Ends := ⟨0, 1⟩
def found : Evidence := ⟨⟨ends, 0, greatest stillListed⟩, ⟨0, 0⟩, ⟨0, 0⟩, false, false⟩

/-- Storage after the live copy published 6 and 7 and deleted 6. -/
def remove (s : Storage) (p : Path) : Storage := fun q => if q = p then none else s q
def liveStorage : Storage :=
  remove (create (create emptyStorage (.snapshot 0 7 6) [60] 10) (.snapshot 0 7 7) [70] 11)
    (.snapshot 0 7 6)

/-- Snapshot 7 is the live copy's, made after the backup, and the restored
copy reserved no snapshot beyond 6. Rust compares it with the operation
sequence (7) and sends; §10's comparison with the last reserved snapshot
number (6) resets it. The restored snapshot operation 6 then finds its path
empty and creates it: the live copy's deleted snapshot 6 and the restored
one share a path with different bytes, and no comparison sees it. -/
theorem operation_ids_hide_newer_snapshot :
    7 ∈ liveOps.kind ∧ 7 ∉ restoredOps.kind ∧
      checkIdentity 7 ⟨ends, 0, restoredOps.last⟩ (some 7) (.ok found) = .send ∧
      checkIdentity 7 ⟨ends, 0, greatest restoredOps.kind⟩ (some 7) (.ok found) =
        .reset .storageAhead ∧
      6 ∈ liveOps.kind ∧ 6 ∈ restoredOps.kind ∧
      compareOccupied [61] (.ok (liveStorage (.snapshot 0 7 6))) = .absent ∧
      create liveStorage (.snapshot 0 7 6) [61] 12 (.snapshot 0 7 6) = some ⟨[61], 12⟩ := by
  decide

/-! ## Reset of a store that never set up storage -/

/-- A store that never set up storage has published nothing: no stored
history to load and no registration entry. Its reset is an empty database
under the fresh id, with the notice (Rust `reset_unconnected`). -/
def resetUnconnected (r : Replacement) : Local := loadDevice r.fresh 0 []

/-- It is the new-device path with an empty store and no registration. -/
theorem unconnected_reset_is_new_device (r : Replacement) :
    resetUnconnected r = finishReset { r with registrationNumber := 0 } [] := rfl

theorem unconnected_reset_empty (r : Replacement) :
    (resetUnconnected r).device = r.fresh ∧ (resetUnconnected r).queue = [] ∧
      (resetUnconnected r).applied = [] ∧ (resetUnconnected r).reserved = ⟨⟨0, 0⟩, 0, 0⟩ :=
  ⟨rfl, rfl, rfl, rfl⟩

/-- Its first write is number 1 under the fresh id, a path no old write used. -/
theorem unconnected_first_write_new_path (r : Replacement) (fresh : r.old ≠ r.fresh)
    (bytes : Bytes) (oldNumber : Nat) :
    (commitWrite (resetUnconnected r) bytes).reserved.logs.writes = 1 ∧
      (commitWrite (resetUnconnected r) bytes).device = r.fresh ∧
      Path.write r.fresh 1 ≠ .write r.old oldNumber := by
  refine ⟨rfl, rfl, ?_⟩
  intro he
  exact fresh (Path.write.inj he).1.symm

end CovenStorage.IdentityCounters
