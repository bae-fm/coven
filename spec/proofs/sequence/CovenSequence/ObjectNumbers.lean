import CovenSequence.Settle

/-!
# Numbered objects outside the sequence (§16.6, §19.3)

A device names its snapshots and floor bodies `(id, number)` with a number of
its own, never reused (§19.3). Copies of one id (§5.3) each count from where
their own journal stood when they parted, so a copy that left a body at a path
the keeper will number the same leaves bytes the keeper didn't write. Unlike a
slot, such an object isn't claimed: it is created, and a create at a taken
path fails.

This module says what a copy does when its next path holds other bytes: it
takes the next number. Bytes at a path are identified by a token; a copy's
bytes at a number are its own (`own`), sealed with the path in them, so
another copy's bytes at that path, or the same copy's at another number,
differ.
-/
namespace CovenSequence.ObjectNumbers

/-- What each path `(id, number)` of one id holds: a token of its bytes. -/
abbrev Objects := Nat → Option Nat

/-- A path a copy may use: nothing is there, or its own bytes are, stored
before a reply was lost. -/
def usable (objects : Objects) (own : Nat → Nat) (n : Nat) : Bool :=
  match objects n with
  | none => true
  | some b => decide (b = own n)

/-- The number a copy lands its object at, starting from `n`: the first path it
may use. A path holding other bytes is burned, and the copy moves on, taking
the next number. -/
def place (objects : Objects) (own : Nat → Nat) : Nat → Nat → Nat
  | 0, n => n
  | fuel + 1, n => if usable objects own n then n else place objects own fuel (n + 1)

/-- The paths after the copy creates its object at `m`. -/
def land (objects : Objects) (own : Nat → Nat) (m : Nat) : Objects :=
  fun p => if p = m then some (own m) else objects p

theorem place_spec (objects : Objects) (own : Nat → Nat) (fuel : Nat) :
    ∀ n, usable objects own (n + fuel) = true →
      n ≤ place objects own fuel n ∧ place objects own fuel n ≤ n + fuel ∧
      usable objects own (place objects own fuel n) = true ∧
      ∀ k, n ≤ k → k < place objects own fuel n → usable objects own k = false := by
  induction fuel with
  | zero =>
    intro n h
    simp only [place, Nat.add_zero] at h ⊢
    exact ⟨Nat.le_refl _, Nat.le_refl _, h, fun k h1 h2 => absurd h2 (by omega)⟩
  | succ fuel ih =>
    intro n h
    by_cases here : usable objects own n = true
    · have e : place objects own (fuel + 1) n = n := by simp [place, here]
      rw [e]
      exact ⟨Nat.le_refl _, by omega, here, fun k h1 h2 => absurd h2 (by omega)⟩
    · have burned : usable objects own n = false := by simpa using here
      have e : place objects own (fuel + 1) n = place objects own fuel (n + 1) := by
        simp [place, burned]
      rw [e]
      have next := ih (n + 1) (by simpa [show n + 1 + fuel = n + (fuel + 1) by omega] using h)
      obtain ⟨lo, hi, ok, before⟩ := next
      refine ⟨by omega, by omega, ok, fun k h1 h2 => ?_⟩
      by_cases same : k = n
      · subst same; exact burned
      · exact before k (by omega) h2

/-- **A burned number is safe.** A copy whose next path holds another copy's
bytes takes the next number, and so on past every taken path. Its slot then
names the path it landed at, never one of the burned paths, which keep what
they held; the path it lands at was empty or already held its own bytes; and
afterwards it holds exactly its own bytes. -/
theorem burned_number_safe (objects : Objects) (own : Nat → Nat) (fuel n : Nat)
    (room : usable objects own (n + fuel) = true) :
    let m := place objects own fuel n
    let after := land objects own m
    n ≤ m ∧ (objects m = none ∨ objects m = some (own m)) ∧ after m = some (own m) ∧
    (∀ k, n ≤ k → k < m → k ≠ m ∧ after k = objects k ∧ objects k ≠ none ∧
      objects k ≠ some (own k)) ∧
    ∀ p, p ≠ m → after p = objects p := by
  intro m after
  obtain ⟨lo, hi, ok, before⟩ := place_spec objects own fuel n room
  refine ⟨lo, ?_, by simp [after, land], fun k h1 h2 => ?_, fun p hp => by simp [after, land, hp]⟩
  · have ok' : usable objects own m = true := ok
    cases h : objects m with
    | none => exact Or.inl rfl
    | some b =>
      simp only [usable, h, decide_eq_true_eq] at ok'
      exact Or.inr (by rw [ok'])
  · have burned := before k h1 h2
    have differs : k ≠ m := by omega
    refine ⟨differs, by simp [after, land, differs], ?_, ?_⟩ <;>
      cases h : objects k <;> simp [usable, h] at burned <;> simp [burned]

/-- Ana's restored copy numbered a snapshot 5 and left its body there; the
keeper's next snapshot is also 5. The keeper finds 5 holds other bytes and
takes 6; had 6 been taken too, 7. -/
theorem keeper_skips_the_restored_copys_body :
    let objects : Objects := fun n => if n = 5 then some 99 else none
    let own : Nat → Nat := fun n => n
    place objects own 3 5 = 6 ∧
    place (fun n => if n = 5 ∨ n = 6 then some 99 else none) own 3 5 = 7 := by decide

end CovenSequence.ObjectNumbers
