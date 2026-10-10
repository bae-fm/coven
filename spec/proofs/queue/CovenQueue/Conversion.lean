import CovenQueue.Upload
import CovenQueue.Verdicts

namespace CovenQueue

/-- Local retained writes distinguish attempted bytes from untried plaintext.
Received writes are tried; only the author's queue may contain untried records. -/
abbrev PendingHistory := Nat → Pending × Option Refusal

def checkedHistory (h : PendingHistory) : History :=
  fun w => ⟨(h w).1.record, (h w).2⟩

def resetIgnores (v : View) (r : Record) : Bool :=
  match v.reset with
  | none => false
  | some b => beforeReset b r

/-- A reload converts in causal order. Its successor sees the converted head's
verdict, not a guess from the head's old schema number. A tried predecessor
remains immutable and can therefore exclude the whole untried tail (§17.1).
Pre-reset writes keep their records; they create no schema loss (§19.3). -/
def rebuild (c : Conversion) (v : View) (h : PendingHistory) (w : Nat) : Pending × Verdict :=
  let answers := ((causes (checkedHistory h) v w).attach.map fun a => (rebuild c v h a.val).2)
  let old := (h w).1
  let next := if resetIgnores v old.record then old else
    convertPending c (excludedInput old.record answers).isSome old
  (next, classify v ⟨next.record, (h w).2⟩ answers)
termination_by w
decreasing_by exact cause_earlier a.property

theorem rebuild_eq (c : Conversion) (v : View) (h : PendingHistory) (w : Nat) :
    rebuild c v h w =
      let answers := (causes (checkedHistory h) v w).map fun a => (rebuild c v h a).2
      let old := (h w).1
      let next := if resetIgnores v old.record then old else
        convertPending c (excludedInput old.record answers).isSome old
      (next, classify v ⟨next.record, (h w).2⟩ answers) := by
  rw [rebuild]
  simp

theorem rebuild_preserves_identity (c : Conversion) (v : View) (h : PendingHistory) (w : Nat) :
    (rebuild c v h w).1.record.identity = (h w).1.record.identity := by
  rw [rebuild_eq]
  dsimp only
  split
  · rfl
  · exact conversion_preserves_identity _ _ _

theorem rebuild_preserves_attempt (c : Conversion) (v : View) (h : PendingHistory)
    (w : Nat) (a : Attempt) (ha : (h w).1 = .tried a) :
    (rebuild c v h w).1 = .tried a := by
  rw [rebuild_eq]
  simp [ha, convertPending]

def rebuiltHistory (c : Conversion) (v : View) (h : PendingHistory) : History :=
  fun w => ⟨(rebuild c v h w).1.record, (h w).2⟩

theorem rebuilt_causes (c : Conversion) (v : View) (h : PendingHistory) (w : Nat) :
    causes (rebuiltHistory c v h) v w = causes (checkedHistory h) v w := by
  simp only [causes, rebuiltHistory, checkedHistory, rebuild_preserves_identity]

/-- The author's reload has exactly the verdict a receiver derives from the
converted records. The proof is through dependencies, not two copied algorithms. -/
theorem reload_matches_receiver (c : Conversion) (v : View) (h : PendingHistory) (w : Nat) :
    (rebuild c v h w).2 = evaluate (rebuiltHistory c v h) v w := by
  induction w using Nat.strongRecOn with
  | ind w ih =>
    rw [evaluate_eq, rebuilt_causes]
    have eq : ((causes (checkedHistory h) v w).map fun a => (rebuild c v h a).2) =
        (causes (checkedHistory h) v w).map (evaluate (rebuiltHistory c v h) v) := by
      apply List.map_congr_left
      intro a ha
      exact ih a (cause_earlier ha)
    change (rebuild c v h w).2 = classify v
      ⟨(rebuild c v h w).1.record, (h w).2⟩ _
    rw [← eq, rebuild_eq c v h w]

/-- Successive breaking migrations are supplied in version order (E13).
Each pass keeps the preceding pass's records and immutable attempt choices. -/
def runConversions : List (Conversion × View) → PendingHistory → PendingHistory
  | [], h => h
  | (c, v) :: cs, h => runConversions cs (fun w => ((rebuild c v h w).1, (h w).2))

theorem conversions_preserve_attempt (cs : List (Conversion × View)) (h : PendingHistory)
    (w : Nat) (a : Attempt) (ha : (h w).1 = .tried a) :
    ((runConversions cs h) w).1 = .tried a := by
  induction cs generalizing h with
  | nil => exact ha
  | cons c cs ih => exact ih _ (rebuild_preserves_attempt _ _ _ _ _ ha)

end CovenQueue
