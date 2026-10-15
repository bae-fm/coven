import CovenIO.RaiseCoverage

namespace CovenIO.RaiseCoverage

/-! # Selection under a possible boundary

Which counting snapshot replay's boundary admits as newest, and how a landing
changes that selection. [`RaiseHistory`](RaiseHistory.lean) holds deletions
to this selection under every boundary replay could still select. -/
namespace RaiseHistory

/-- Usable under a possible boundary: everything without one; otherwise the
named snapshot and every snapshot whose signed store-log view read an entry
naming it (`reads` pairs a reader with the named snapshot). -/
def admits (reads : List (Snapshot × Snapshot)) : Option Snapshot → Snapshot → Bool
  | none, _ => true
  | some named, s => samePath s named || decide ((s, named) ∈ reads)

/-- §15's newest counting snapshot of audience `a` under boundary `b`. -/
def selected (order : SnapshotOrder) (landed : List Snapshot)
    (reads : List (Snapshot × Snapshot)) (a : Nat) (b : Option Snapshot) : Option Snapshot :=
  newest order (landed.filter fun s => s.audience == a && s.counts && admits reads b s)

def CoversIn (selection : Option Snapshot) (w : Write) (a : Nat) : Prop :=
  match selection with
  | none => False
  | some s => s.covers w a

instance (selection : Option Snapshot) (w : Write) (a : Nat) : Decidable (CoversIn selection w a) := by
  cases selection <;> unfold CoversIn <;> infer_instance

theorem samePath_self (s : Snapshot) : samePath s s = true := by simp [samePath]

theorem samePath_symm (a b : Snapshot) : samePath a b = samePath b a := by
  have comm : ∀ x y : Nat, (x == y) = (y == x) := fun x y => by
    by_cases e : x = y
    · subst e; rfl
    · have e' : y ≠ x := fun h => e h.symm
      rw [beq_false_of_ne e, beq_false_of_ne e']
  unfold samePath
  rw [comm a.audience b.audience, comm a.writer b.writer, comm a.number b.number]

/-- The newest of a list with one more element covering the write still covers it. -/
theorem newest_cons_covers (order : SnapshotOrder) (snap : Snapshot) (rest : List Snapshot)
    (w : Write) (a : Nat) (new : snap.covers w a)
    (old : rest = [] ∨ CoversIn (newest order rest) w a) :
    CoversIn (newest order (snap :: rest)) w a := by
  simp only [newest]
  cases h : newest order rest with
  | none => simpa [CoversIn] using new
  | some other =>
      have previous : other.covers w a := by
        rcases old with empty | covered
        · subst empty; simp [newest] at h
        · simpa [h, CoversIn] using covered
      by_cases pick : newer order snap other = true
      · simpa [pick, CoversIn] using new
      · simpa [pick, CoversIn] using previous

theorem newest_none (order : SnapshotOrder) (xs : List Snapshot) :
    newest order xs = none ↔ xs = [] := by
  cases xs with
  | nil => simp [newest]
  | cons x rest =>
    simp only [newest, reduceCtorEq, iff_false]
    cases newest order rest <;> simp

theorem admits_old (reads : List (Snapshot × Snapshot)) (read : List Snapshot)
    (snap x : Snapshot) (b : Option Snapshot) (other : samePath x snap = false) :
    admits (read.map (snap, ·) ++ reads) b x = admits reads b x := by
  cases b with
  | none => rfl
  | some named =>
      have different : x ≠ snap := by
        intro same; subst same; simp [samePath_self] at other
      simp only [admits, List.mem_append, List.mem_map]
      congr 1
      apply decide_eq_decide.mpr
      constructor
      · rintro (⟨n, _, eq⟩ | old)
        · exact absurd (congrArg Prod.fst eq).symm different
        · exact old
      · exact Or.inr

/-- Landing changes a possible boundary's selection only by the new snapshot. -/
theorem selected_after_land (landed : List Snapshot)
    (reads : List (Snapshot × Snapshot)) (read : List Snapshot) (snap : Snapshot)
    (a : Nat) (b : Option Snapshot) (fresh : ∀ x ∈ landed, samePath x snap = false) :
    (landed.filter fun s => s.audience == a && s.counts &&
        admits (read.map (snap, ·) ++ reads) b s) =
      landed.filter fun s => s.audience == a && s.counts && admits reads b s := by
  apply List.filter_congr
  intro x member
  rw [admits_old reads read snap x b (fresh x member)]

/-- After a landing, a selection is the new snapshot's newest with the old
candidates, or the old selection. -/
theorem landed_selection (order : SnapshotOrder) (landed : List Snapshot)
    (reads : List (Snapshot × Snapshot)) (read : List Snapshot) (snap : Snapshot)
    (a : Nat) (b : Option Snapshot) (fresh : ∀ x ∈ landed, samePath x snap = false)
    (w : Write) (covers : snap.audience = a → snap.counts = true → snap.covers w a) :
    (selected order landed reads a b = none ∨ CoversIn (selected order landed reads a b) w a) →
      (selected order (snap :: landed) (read.map (snap, ·) ++ reads) a b = none ∨
        CoversIn (selected order (snap :: landed) (read.map (snap, ·) ++ reads) a b) w a) ∧
      (CoversIn (selected order landed reads a b) w a →
        CoversIn (selected order (snap :: landed) (read.map (snap, ·) ++ reads) a b) w a) := by
  intro before
  unfold selected at before ⊢
  rw [List.filter_cons, selected_after_land landed reads read snap a b fresh]
  split
  · rename_i admitted
    simp only [Bool.and_eq_true, beq_iff_eq] at admitted
    have previous : (landed.filter fun s => s.audience == a && s.counts && admits reads b s) = [] ∨
        CoversIn (newest order (landed.filter fun s =>
          s.audience == a && s.counts && admits reads b s)) w a := by
      rcases before with none | covered
      · exact .inl ((newest_none order _).mp none)
      · exact .inr covered
    have new := newest_cons_covers order snap _ w a (covers admitted.1.1 admitted.1.2) previous
    exact ⟨.inr new, fun _ => new⟩
  · exact ⟨before, id⟩

end RaiseHistory

end CovenIO.RaiseCoverage
