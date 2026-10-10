import CovenIO.Storage

namespace CovenIO.Publication

/-- Durable reservations survive cancellation and crash. Published is the
historical publication sequence; retention can remove its storage objects. -/
structure Writer where
  reserved : List Bytes
  published : List Bytes
  confirmed : Nat
  deriving DecidableEq, Repr

def initial : Writer := ⟨[], [], 0⟩

def Valid (s : Writer) : Prop :=
  s.confirmed ≤ s.published.length ∧ s.published.length ≤ s.confirmed + 1 ∧
  s.published <+: s.reserved

inductive Step : Writer → Writer → Prop
  | reserve (s : Writer) (bytes : Bytes) :
      Step s { s with reserved := s.reserved ++ [bytes] }
  | land (s : Writer) (bytes : Bytes) (tail : List Bytes)
      (ready : s.confirmed = s.published.length)
      (next : s.reserved = s.published ++ bytes :: tail) :
      Step s { s with published := s.published ++ [bytes] }
  | confirm (s : Writer) (landed : s.published.length = s.confirmed + 1) :
      Step s { s with confirmed := s.confirmed + 1 }
  | retry (s : Writer) : Step s s
  | crash (s : Writer) : Step s s
  | cancelInitiator (s : Writer) : Step s s

theorem step_valid {a b : Writer} (h : Step a b) (valid : Valid a) : Valid b := by
  obtain ⟨lo, hi, hprefix⟩ := valid
  cases h with
  | reserve bytes =>
      refine ⟨lo, hi, ?_⟩
      obtain ⟨tail, ht⟩ := hprefix
      exact ⟨tail ++ [bytes], by simp [← ht, List.append_assoc]⟩
  | land bytes tail ready next =>
      refine ⟨by simp; omega, by simp; omega, tail, ?_⟩
      simp [next, List.append_assoc]
  | confirm landed => exact ⟨by dsimp; omega, by dsimp; omega, hprefix⟩
  | retry | crash | cancelInitiator => exact ⟨lo, hi, hprefix⟩

inductive Run : Writer → Prop
  | initial : Run initial
  | step {a b : Writer} : Run a → Step a b → Run b

theorem reachable_valid {s : Writer} (h : Run s) : Valid s := by
  induction h with
  | initial => exact ⟨by decide, by decide, [], rfl⟩
  | step _ step ih => exact step_valid step ih

/-- Every published number is in 1..k, and every number in 1..k has bytes. -/
theorem gap_free {s : Writer} (h : Run s) (n : Nat) (positive : 0 < n) :
    (∃ bytes, s.published[n - 1]? = some bytes) ↔ n ≤ s.published.length := by
  have _ := reachable_valid h
  simp only [List.getElem?_eq_some_iff]
  constructor
  · rintro ⟨_, bound, _⟩; omega
  · intro bound
    have index : n - 1 < s.published.length := by omega
    exact ⟨s.published[n - 1], index, rfl⟩

theorem reservations_irrevocable {a b : Writer} (h : Step a b) : a.reserved <+: b.reserved := by
  cases h with
  | reserve bytes => exact ⟨[bytes], rfl⟩
  | land | confirm | retry | crash | cancelInitiator => exact ⟨[], by simp⟩

theorem publication_fixed {s : Writer} (h : Run s) : s.published <+: s.reserved :=
  (reachable_valid h).2.2

/-- Materialize the historical log by its actual one-based exact paths.
Metadata supplies each object's first publication time, never a retry time. -/
def objects (s : Writer) (kind : Kind) (writer : Nat) (storedAt : Nat → Nat) : Store :=
  fun p => match p with
  | .log k w n =>
      if k = kind ∧ w = writer ∧ 0 < n then
        (s.published[n - 1]?).map (fun bytes => ⟨⟨bytes, storedAt n⟩, 0⟩)
      else none
  | _ => none

theorem exact_path_prefix {s : Writer} (h : Run s) (kind : Kind) (writer n : Nat)
    (storedAt : Nat → Nat) (positive : 0 < n) :
    (∃ o, objects s kind writer storedAt (.log kind writer n) = some o) ↔
      n ≤ s.published.length := by
  have numbered := gap_free h n positive
  simp only [objects, positive, and_self, ↓reduceIte]
  cases found : s.published[n - 1]? <;> simp_all

/-- A transition of one writer/log leaves every other writer/log untouched. -/
abbrev Writers := Kind → Nat → Writer

def change (writers : Writers) (kind : Kind) (writer : Nat) (next : Writer) : Writers :=
  fun k w => if k = kind ∧ w = writer then next else writers k w

theorem interleaved_valid (writers : Writers) (valid : ∀ k w, Valid (writers k w))
    (k : Kind) (w : Nat) (next : Writer) (step : Step (writers k w) next) :
    ∀ j v, Valid (change writers k w next j v) := by
  intro j v
  by_cases eq : j = k ∧ v = w
  · rcases eq with ⟨rfl, rfl⟩; simpa [change] using step_valid step (valid _ _)
  · simpa [change, eq] using valid j v

end CovenIO.Publication
