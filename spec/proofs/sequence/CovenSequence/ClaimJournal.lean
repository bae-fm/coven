import CovenSequence.Settle

/-!
# One claim counter, one claim at a time (§5.1)

`Settle.settle_correct` takes `Own`, the facts true of a single copy's claims,
as its premise. This file derives `Own` from the journal rules of coven.md
§5.1, for every kind of slot alike:

- **one counter:** a claim of any kind (writes, admin entry, key copy,
  announcement; its `value` here) takes the next number;
- **one claim at a time:** a device begins a claim only with none open, and
  ends one only once settling says it landed or not;
- **journal, then create:** the device sends only the attempt its journal
  records, and a retry after losing keeps the number and records a new attempt
  for the slot after the ones it then applied.

A create the device sent stays in flight and can land at any later time, while
its slot is still empty. Other devices claim slots at any time. The model holds
the genuine first bytes of each slot (`seq`, through `head`): deleting slots
below a floor changes none of them, and settling from the floor reads the same
records (`Settle.settle_from_floor`).

`journal_own`: in every state the steps reach from empty storage, the open
claim satisfies `Own`, so `journal_settles` gives `settle_correct` with no
premise about claim numbers. The three counterexamples show each rule is
needed: separate counters per slot kind (`kind_counters_report_copy`), two
claims open at once (`two_open_claims_report_copy`) and a create sent before it
was recorded (`unrecorded_create_looks_like_copy`) each make one copy of a
device read as another copy using its id.
-/
namespace CovenSequence.ClaimJournal

open Settle

/-- The journal's open claim: its number, the slot its current attempt claims,
that attempt, and the work it carries. -/
structure Open where
  number : Nat
  slot : Nat
  attempt : Nat
  value : Nat
  deriving DecidableEq

/-- The bytes an open claim's current attempt sends. -/
def Open.bytes {Dev : Type} (d : Dev) (o : Open) : Rec Dev := .claim d o.number o.attempt o.value

/-- Storage's genuine slots and one device's journal. -/
structure World (Dev : Type) where
  seq : Nat → Rec Dev
  head : Nat
  /-- The last claim number the device reserved, across every slot kind. -/
  counter : Nat
  /-- The slots the device has applied. -/
  applied : Nat
  journal : Option Open
  /-- Creates sent and possibly still in flight: slot and bytes. -/
  sent : List (Nat × Rec Dev)

variable {Dev : Type} [DecidableEq Dev] (d : Dev)

/-- Slot `head + 1` filled with `x`. -/
def World.fill (w : World Dev) (x : Rec Dev) : World Dev :=
  { w with seq := fun j => if j = w.head + 1 then x else w.seq j, head := w.head + 1 }

inductive Step : World Dev → World Dev → Prop
  /-- Begin a claim of any kind: the next number, at the slot after those
  applied, recorded before anything is sent. -/
  | begin (w : World Dev) (value : Nat) (none : w.journal = none) :
      Step w { w with counter := w.counter + 1,
                      journal := some ⟨w.counter + 1, w.applied + 1, 0, value⟩ }
  /-- Send the recorded attempt's create, again after a lost reply. -/
  | send (w : World Dev) (o : Open) (h : w.journal = some o) :
      Step w { w with sent := (o.slot, o.bytes d) :: w.sent }
  /-- A create in flight lands, its slot being the empty one after the head. -/
  | land (w : World Dev) (x : Rec Dev) (h : (w.head + 1, x) ∈ w.sent) : Step w (w.fill x)
  /-- Another device's slot. -/
  | other (w : World Dev) (x : Rec Dev) (h : anotherClaim d x = false) : Step w (w.fill x)
  /-- With no claim open, apply the next slot. -/
  | read (w : World Dev) (none : w.journal = none) (h : w.applied < w.head) :
      Step w { w with applied := w.applied + 1 }
  /-- Settling finds the attempt's bytes: the claim landed, and the device
  applies its slot. -/
  | landed (w : World Dev) (o : Open) (h : w.journal = some o) (filled : o.slot ≤ w.head)
      (own : w.seq o.slot = o.bytes d) :
      Step w { w with journal := none, applied := o.slot }
  /-- Settling finds other bytes: the device applies the slots through `k` and
  retries with the same number and a new attempt at `k + 1`. -/
  | lost (w : World Dev) (o : Open) (k : Nat) (h : w.journal = some o) (filled : o.slot ≤ k)
      (le : k ≤ w.head) (other : w.seq o.slot ≠ o.bytes d) :
      Step w { w with journal := some { o with slot := k + 1, attempt := o.attempt + 1 },
                      applied := k }
  /-- Or its work, no longer allowed, ends the claim unlanded. -/
  | abandon (w : World Dev) (o : Open) (k : Nat) (h : w.journal = some o) (filled : o.slot ≤ k)
      (le : k ≤ w.head) (other : w.seq o.slot ≠ o.bytes d) :
      Step w { w with journal := none, applied := k }

inductive Steps : World Dev → World Dev → Prop
  | refl (w : World Dev) : Steps w w
  | step {w w' w'' : World Dev} : Step d w w' → Steps w' w'' → Steps w w''

/-- Empty storage, and a device that has claimed nothing. -/
def empty : World Dev := ⟨fun _ => .other, 0, 0, 0, none, []⟩

/-- What the steps keep true of this device's slots and creates. -/
structure Inv (w : World Dev) : Prop where
  applied_le : w.applied ≤ w.head
  numbers_le : ∀ j m t v, 1 ≤ j → j ≤ w.head → w.seq j = .claim d m t v → m ≤ w.counter
  /-- With no claim open, every slot of this device is applied and every create
  it sent targets a filled slot, so none can land. -/
  closed : w.journal = none →
    (∀ j m t v, 1 ≤ j → j ≤ w.head → w.seq j = .claim d m t v → j ≤ w.applied) ∧
    ∀ i x, (i, x) ∈ w.sent → i ≤ w.head
  /-- With one open, it holds the counter's number at the slot after those
  applied; this device's other slots are earlier claims, applied; and a create
  in flight that can still land is its current attempt. -/
  opened : ∀ o, w.journal = some o →
    o.number = w.counter ∧ 1 ≤ o.number ∧ o.slot = w.applied + 1 ∧
    (∀ j m t v, 1 ≤ j → j ≤ w.head → w.seq j = .claim d m t v →
      (m < w.counter ∧ j ≤ w.applied) ∨ (j = o.slot ∧ m = o.number ∧ t = o.attempt ∧ v = o.value)) ∧
    ∀ i x, (i, x) ∈ w.sent → i ≤ w.head ∨ (i = o.slot ∧ x = o.bytes d)

omit [DecidableEq Dev] in
theorem empty_inv : Inv d (empty (Dev := Dev)) where
  applied_le := Nat.le_refl _
  numbers_le _ _ _ _ h1 h2 _ := by simp [empty] at h2; omega
  closed _ := ⟨fun _ _ _ _ h1 h2 _ => by simp [empty] at h2; omega, fun _ _ h => by simp [empty] at h⟩
  opened _ h := by simp [empty] at h

omit [DecidableEq Dev] in
theorem fill_seq (w : World Dev) (x : Rec Dev) {j : Nat} (hj : j ≤ w.head) : (w.fill x).seq j = w.seq j := by
  simp [World.fill, show j ≠ w.head + 1 by omega]

theorem step_inv {w w' : World Dev} (s : Step d w w') (inv : Inv d w) : Inv d w' := by
  cases s with
  | begin value none =>
    obtain ⟨own, sent⟩ := inv.closed none
    refine ⟨inv.applied_le, fun j m t v h1 h2 hs => ?_, fun h => by simp at h, fun o h => ?_⟩
    · have := inv.numbers_le j m t v h1 h2 hs; dsimp only; omega
    · simp only [Option.some.injEq] at h
      subst h
      refine ⟨rfl, Nat.succ_pos _, rfl, fun j m t v h1 h2 hs => Or.inl ⟨?_, own j m t v h1 h2 hs⟩,
        fun i x hx => Or.inl (sent i x hx)⟩
      have := inv.numbers_le j m t v h1 h2 hs; dsimp only; omega
  | send o h =>
    refine ⟨inv.applied_le, inv.numbers_le, fun hn => by simp [h] at hn, fun o' h' => ?_⟩
    simp only [h, Option.some.injEq] at h'
    subst h'
    obtain ⟨a, b, c, e, f⟩ := inv.opened o h
    refine ⟨a, b, c, e, fun i x hx => ?_⟩
    simp only [List.mem_cons, Prod.mk.injEq] at hx
    rcases hx with ⟨rfl, rfl⟩ | hx
    · exact Or.inr ⟨rfl, rfl⟩
    · exact f i x hx
  | land x hx =>
    have landed : w.journal ≠ none ∧ ∃ o, w.journal = some o ∧ w.head + 1 = o.slot ∧ x = o.bytes d := by
      cases hj : w.journal with
      | none => have := (inv.closed hj).2 _ _ hx; omega
      | some o =>
        rcases (inv.opened o hj).2.2.2.2 _ _ hx with h | ⟨h1, h2⟩
        · omega
        · exact ⟨by simp, o, rfl, h1, h2⟩
    obtain ⟨_, o, hj, hslot, hbytes⟩ := landed
    obtain ⟨a, b, c, e, f⟩ := inv.opened o hj
    have hl := inv.applied_le
    refine ⟨by simp only [World.fill]; omega, fun j m t v h1 h2 hs => ?_, fun hn => by simp [World.fill, hj] at hn,
      fun o' h' => ?_⟩
    · by_cases hh : j = w.head + 1
      · simp only [World.fill, hh, ite_true, hbytes, Open.bytes, Rec.claim.injEq] at hs
        simp only [World.fill]; omega
      · have hj' : j ≤ w.head := by simp only [World.fill] at h2; omega
        rw [fill_seq w x hj'] at hs
        exact inv.numbers_le j m t v h1 hj' hs
    · simp only [World.fill, hj, Option.some.injEq] at h'
      subst h'
      refine ⟨a, b, c, fun j m t v h1 h2 hs => ?_, fun i y hy => ?_⟩
      · by_cases hh : j = w.head + 1
        · simp only [World.fill, hh, ite_true, hbytes, Open.bytes, Rec.claim.injEq] at hs
          obtain ⟨-, rfl, rfl, rfl⟩ := hs
          exact Or.inr ⟨by omega, rfl, rfl, rfl⟩
        · have hj' : j ≤ w.head := by simp only [World.fill] at h2; omega
          rw [fill_seq w x hj'] at hs
          exact e j m t v h1 hj' hs
      · rcases f i y hy with h | h
        · exact Or.inl (by simp only [World.fill]; omega)
        · exact Or.inr h
  | other x hx =>
    have hl := inv.applied_le
    have notMine : ∀ m t v, x ≠ .claim d m t v := by
      intro m t v he; subst he; simp [anotherClaim] at hx
    have back : ∀ j m t v, j ≤ w.head + 1 → (w.fill x).seq j = .claim d m t v → j ≤ w.head ∧ w.seq j = .claim d m t v := by
      intro j m t v h2 hs
      by_cases hh : j = w.head + 1
      · simp only [World.fill, hh, ite_true] at hs; exact absurd hs (notMine m t v)
      · have hj' : j ≤ w.head := by omega
        rw [fill_seq w x hj'] at hs
        exact ⟨hj', hs⟩
    refine ⟨by simp only [World.fill]; omega, fun j m t v h1 h2 hs => ?_, fun hn => ?_, fun o h' => ?_⟩
    · obtain ⟨hj, hs⟩ := back j m t v h2 hs
      exact inv.numbers_le j m t v h1 hj hs
    · obtain ⟨own, sent⟩ := inv.closed hn
      refine ⟨fun j m t v h1 h2 hs => ?_, fun i y hy => ?_⟩
      · obtain ⟨hj, hs⟩ := back j m t v h2 hs
        exact own j m t v h1 hj hs
      · have := sent i y hy; simp only [World.fill]; omega
    · obtain ⟨a, b, c, e, f⟩ := inv.opened o h'
      refine ⟨a, b, c, fun j m t v h1 h2 hs => ?_, fun i y hy => ?_⟩
      · obtain ⟨hj, hs⟩ := back j m t v h2 hs
        exact e j m t v h1 hj hs
      · rcases f i y hy with h | h
        · exact Or.inl (by simp only [World.fill]; omega)
        · exact Or.inr h
  | read none h =>
    obtain ⟨own, sent⟩ := inv.closed none
    refine ⟨by dsimp only; omega, inv.numbers_le, fun _ => ⟨fun j m t v h1 h2 hs => ?_, sent⟩,
      fun o h' => by simp [none] at h'⟩
    have := own j m t v h1 h2 hs; dsimp only; omega
  | landed o h filled own =>
    obtain ⟨a, b, c, e, f⟩ := inv.opened o h
    refine ⟨filled, inv.numbers_le, fun _ => ⟨fun j m t v h1 h2 hs => ?_, fun i x hx => ?_⟩,
      fun o' h' => by simp at h'⟩
    · rcases e j m t v h1 h2 hs with ⟨_, hj⟩ | ⟨hj, _⟩
      · dsimp only; omega
      · dsimp only; omega
    · rcases f i x hx with hi | ⟨hi, _⟩
      · exact hi
      · dsimp only; omega
  | lost o k h filled le other =>
    obtain ⟨a, b, c, e, f⟩ := inv.opened o h
    have notHere : ∀ j m t v, 1 ≤ j → j ≤ w.head → w.seq j = .claim d m t v →
        m < w.counter ∧ j ≤ w.applied := by
      intro j m t v h1 h2 hs
      rcases e j m t v h1 h2 hs with h | ⟨hj, hm, ht, hv⟩
      · exact h
      · subst hj hm ht hv; exact absurd hs other
    refine ⟨le, inv.numbers_le, fun hn => by simp at hn, fun o' h' => ?_⟩
    simp only [Option.some.injEq] at h'
    subst h'
    refine ⟨a, b, rfl, fun j m t v h1 h2 hs => Or.inl ?_, fun i x hx => Or.inl ?_⟩
    · have := notHere j m t v h1 h2 hs; dsimp only; omega
    · rcases f i x hx with hi | ⟨hi, _⟩
      · exact hi
      · dsimp only; omega
  | abandon o k h filled le other =>
    obtain ⟨a, b, c, e, f⟩ := inv.opened o h
    refine ⟨le, inv.numbers_le, fun _ => ⟨fun j m t v h1 h2 hs => ?_, fun i x hx => ?_⟩,
      fun o' h' => by simp at h'⟩
    · rcases e j m t v h1 h2 hs with ⟨_, hj⟩ | ⟨hj, hm, ht, hv⟩
      · dsimp only; omega
      · subst hj hm ht hv; exact absurd hs other
    · rcases f i x hx with hi | ⟨hi, _⟩
      · exact hi
      · dsimp only; omega

theorem steps_inv {w w' : World Dev} (run : Steps d w w') (inv : Inv d w) : Inv d w' := by
  induction run with
  | refl => exact inv
  | step s _ ih => exact ih (step_inv d s inv)

/-- **The journal rules give `Own`.** In every state reached from empty storage,
under any interleaving with other devices' slots and creates landing late, the
open claim's number is the greatest of this device's claims, its earlier
claims are in earlier slots, and its number appears only at its current
attempt's slot, with that attempt's bytes. -/
theorem journal_own {w : World Dev} (run : Steps d empty w) {o : Open} (h : w.journal = some o) :
    Own w.seq w.head d o.number o.slot o.attempt o.value := by
  have inv := steps_inv d run (empty_inv d)
  obtain ⟨a, b, c, e, _⟩ := inv.opened o h
  refine ⟨b, by omega, fun j m t v h1 h2 hs => ?_, fun j m t v h1 h2 hs hm => ?_,
    fun j t v h1 h2 hs => ?_⟩
  · rw [a]; exact inv.numbers_le j m t v h1 h2 hs
  · rcases e j m t v h1 h2 hs with ⟨_, hj⟩ | ⟨_, hm', _, _⟩
    · omega
    · omega
  · rcases e j o.number t v h1 h2 hs with ⟨hm, _⟩ | ⟨hj, _, ht, hv⟩
    · omega
    · exact ⟨hj, ht, hv⟩

/-- **A device that keeps the journal rules settles correctly.** Its open
claim, of any kind, settles as `landed` exactly when its attempt's bytes fill
the slot, `notLanded` only when its number is nowhere in the sequence, `retry`
only while the slot is empty, and never as another copy. -/
theorem journal_settles {H : Type} [DecidableEq H] {w : World Dev} (run : Steps d empty w)
    {o : Open} (h : w.journal = some o) (hash : Nat → Rec Dev → H) {start c : Nat}
    (hsc : start ≤ c + 1) (hch : c ≤ w.head) {read : Option (Rec Dev)}
    (hread : start ≤ o.slot → read = if o.slot ≤ w.head then some (w.seq o.slot) else none) :
    let verdict := settle start (lastClaim w.seq hash d c) o.slot (o.bytes d) o.number
      (hash o.slot (o.bytes d)) (anotherClaim d) read
    (verdict = .landed ↔ o.slot ≤ w.head ∧ w.seq o.slot = o.bytes d) ∧
    (verdict = .notLanded → ∀ j t v, 1 ≤ j → j ≤ w.head → w.seq j ≠ .claim d o.number t v) ∧
    (verdict = .retry → w.head < o.slot) ∧ verdict ≠ .copy :=
  settle_correct hash (journal_own d run h) hsc hch hread

/-! ## Each rule is needed -/

/-- **Counterexample: a counter per slot kind.** Device `true` numbers its
writes and its admin entries separately. Its writes claim 1 landed at slot 1;
its admin claim, also number 1, lost slot 2 to another device, and the floor
has passed both. The floor's record for `true` is claim 1 with slot 1's digest,
so settling the admin claim reports another copy using the device's id. With
one counter the admin claim is number 2, and the same record settles it as not
landed. -/
theorem kind_counters_report_copy :
    let hash := fun (k : Nat) (x : Rec Bool) => (k, x)
    let seq : Nat → Rec Bool := fun k => if k = 1 then .claim true 1 0 5 else .other
    let covered := lastClaim seq hash true 2
    covered = some ⟨1, (1, .claim true 1 0 5)⟩ ∧
    settleFloor covered 1 (hash 2 (.claim true 1 0 7)) = .copy ∧
    settleFloor covered 2 (hash 2 (.claim true 2 0 7)) = .notLanded := by
  decide

/-- **Counterexample: two claims open at once.** Device `true` opens claim 1
and claim 2, both for slot 1. Claim 2's create lands. Settling claim 1 reads
the device's own id with other bytes at its slot, which only another copy could
have sent, and reports one. -/
theorem two_open_claims_report_copy :
    settle 1 (none : Option (ClaimRecord Nat)) 1 (Rec.claim true 1 0 5) 1 0 (anotherClaim true)
      (some (.claim true 2 0 7)) = .copy := by
  decide

/-- **Counterexample: a create sent before it is recorded.** A device sends
its claim of slot 1, crashes before recording it, and reopens with a journal
that never attempted it. The slot it reads there carries its own id with bytes
its journal lacks, the evidence `foreign_slot_means_copy` reads as another
copy, though this device is the only one. -/
theorem unrecorded_create_looks_like_copy :
    let after : CWorld (Dev := Unit) Unit := ⟨Claim.create (fun _ => none) 1 ((), 7), fun _ => []⟩
    after.slots 1 = some ((), 7) ∧ (1, 7) ∉ after.journal () ∧ ¬ ∃ κ : Unit, κ ≠ () := by
  refine ⟨by decide, by simp, ?_⟩
  rintro ⟨κ, h⟩
  exact h rfl

end CovenSequence.ClaimJournal
