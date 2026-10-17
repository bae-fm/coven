/-! Per-device snapshot evidence. Digests denote SHA-256 of complete slot bytes;
cryptographic security is an explicit premise of settlement, not an axiom. -/
namespace CovenSequence

structure ClaimRecord (Digest : Type) where
  number : Nat
  digest : Digest
  deriving DecidableEq

namespace ClaimRecord

/-- Absence contributes zero to the numeric bound and has no digest. -/
def numberOf : Option (ClaimRecord H) → Nat
  | none => 0
  | some r => r.number

/-- Keep the greatest number; a later slot replaces the digest on a tie. -/
def remember (old : Option (ClaimRecord H)) (r : ClaimRecord H) : Option (ClaimRecord H) :=
  match old with
  | none => some r
  | some p => if p.number ≤ r.number then some r else some p

theorem remember_number (old : Option (ClaimRecord H)) (r : ClaimRecord H) :
    numberOf (remember old r) = max (numberOf old) r.number := by
  cases old with
  | none => simp [remember, numberOf]
  | some p => simp only [remember]; split <;> simp only [numberOf] <;> omega

end ClaimRecord

namespace Claims
variable {Dev H : Type} [DecidableEq Dev]

/-- The record in a snapshot covering slots 1 through k, including refused slots. -/
def last (seq : Nat → Option (Dev × ClaimRecord H)) (d : Dev) : Nat → Option (ClaimRecord H)
  | 0 => none
  | k + 1 => match seq (k + 1) with
    | some (a, r) => if a = d then ClaimRecord.remember (last seq d k) r else last seq d k
    | none => last seq d k

/-- Every stored digest came from an included slot with that exact number. -/
theorem witness (seq : Nat → Option (Dev × ClaimRecord H)) (d : Dev) :
    ∀ k r, last seq d k = some r → ∃ j, 1 ≤ j ∧ j ≤ k ∧ seq j = some (d, r)
  | 0, _, h => by simp [last] at h
  | k + 1, r, h => by
    simp only [last] at h
    cases hs : seq (k + 1) with
    | none =>
      rw [hs] at h
      obtain ⟨j, h1, h2, hj⟩ := witness seq d k r h
      exact ⟨j, h1, by omega, hj⟩
    | some p =>
      obtain ⟨a, next⟩ := p
      rw [hs] at h
      by_cases ha : a = d
      · subst a
        simp only [ite_true] at h
        cases hp : last seq d k with
        | none =>
          simp [hp, ClaimRecord.remember] at h
          subst r
          exact ⟨k + 1, by omega, by omega, hs⟩
        | some prev =>
          simp only [hp, ClaimRecord.remember] at h
          split at h
          · cases h; exact ⟨k + 1, by omega, by omega, hs⟩
          · cases h
            obtain ⟨j, h1, h2, hj⟩ := witness seq d k _ hp
            exact ⟨j, h1, by omega, hj⟩
      · simp only [ha, ite_false] at h
        obtain ⟨j, h1, h2, hj⟩ := witness seq d k r h
        exact ⟨j, h1, by omega, hj⟩

/-- A positive number is covered exactly when an included slot reaches it. -/
theorem ge (seq : Nat → Option (Dev × ClaimRecord H)) (d : Dev) {n : Nat} (hn : 1 ≤ n) :
    ∀ k, n ≤ ClaimRecord.numberOf (last seq d k) ↔
      ∃ j r, 1 ≤ j ∧ j ≤ k ∧ seq j = some (d, r) ∧ n ≤ r.number
  | 0 => by simp only [last, ClaimRecord.numberOf]; constructor <;> intro h
            · omega
            · obtain ⟨j, _, h1, h2, _⟩ := h; omega
  | k + 1 => by
    have ih := ge seq d hn k
    simp only [last]
    cases hs : seq (k + 1) with
    | none =>
      simp only
      rw [ih]
      constructor
      · rintro ⟨j, r, h1, h2, hj, hr⟩; exact ⟨j, r, h1, by omega, hj, hr⟩
      · rintro ⟨j, r, h1, h2, hj, hr⟩
        by_cases he : j = k + 1
        · subst j; rw [hs] at hj; cases hj
        · exact ⟨j, r, h1, by omega, hj, hr⟩
    | some p =>
      obtain ⟨a, r⟩ := p
      simp only
      by_cases ha : a = d
      · subst a
        simp only [ite_true, ClaimRecord.remember_number]
        constructor
        · intro h
          by_cases hn' : n ≤ r.number
          · exact ⟨k + 1, r, by omega, by omega, hs, hn'⟩
          · obtain ⟨j, r', h1, h2, hj, hr⟩ := ih.1 (by omega)
            exact ⟨j, r', h1, by omega, hj, hr⟩
        · rintro ⟨j, r', h1, h2, hj, hr⟩
          by_cases he : j = k + 1
          · subst j; rw [hs] at hj; cases hj; omega
          · have := ih.2 ⟨j, r', h1, by omega, hj, hr⟩; omega
      · simp only [ha, ite_false]
        rw [ih]
        constructor
        · rintro ⟨j, r', h1, h2, hj, hr⟩; exact ⟨j, r', h1, by omega, hj, hr⟩
        · rintro ⟨j, r', h1, h2, hj, hr⟩
          by_cases he : j = k + 1
          · subst j; rw [hs] at hj; cases hj; exact absurd rfl ha
          · exact ⟨j, r', h1, by omega, hj, hr⟩

end Claims
end CovenSequence
