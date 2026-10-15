import CovenIO.AwayRead
import CovenIO.ReloadExamples

namespace CovenIO.AwayReadExamples
open AwayRead SnapshotPublication

/-! Two checked histories. A store-only device that joins after Ana's
Gifts-only write 1 is deleted stops at that miss forever under the rule that
trusts misses after discovering only its readable audiences, although her
store write 2 is stored; the away read passes write 1 and receives write 2.
A catalog listed only after the miss cannot certify the deletion: it can
cover a write stored after the miss, and passing that number loses it. -/

/-- Ana's Gifts-only write 1 and her store write 2 (audience 0 is the store,
audience 1 Gifts). -/
def giftsWrite : Write := ⟨0, 1, 0, [1]⟩
def storeWrite : Write := ⟨0, 2, month, [0]⟩
/-- A counting Gifts snapshot through Ana's write 1. -/
def giftsSnapshot : Snapshot := ⟨1, 0, 1, 0, 0, [(0, 1)]⟩

/-- The catalog after write 1's deletion, as `ReloadExamples.crossAudienceState`
reaches it; before any listing nothing is known. -/
def joined (t : Nat) : SnapshotHistory.State :=
  if t = 0 then ⟨0, [], []⟩ else ⟨month, [giftsSnapshot], [giftsWrite]⟩

def joinedWorld : World := fun _ => snapshotStore ReloadExamples.crossAudienceState

def joinedPublished (w : Write) : Prop := w = giftsWrite ∨ w = storeWrite

theorem joined_runs (order : SnapshotOrder) (t : Nat) :
    SnapshotHistory.Run joinedPublished order (joined t) := by
  by_cases ht : t = 0
  · simp only [joined, ht, ↓reduceIte]; exact .initial
  · simp only [joined, ht, ↓reduceIte]
    have sound : SnapshotHistory.Sound joinedPublished giftsSnapshot := by
      constructor
      all_goals
        intro w pub ha
        rcases pub with rfl | rfl
        all_goals simp_all [giftsWrite, storeWrite, giftsSnapshot, Snapshot.covers, Snapshot.position]
    have landed : SnapshotHistory.Run joinedPublished order ⟨0, [giftsSnapshot], []⟩ :=
      .step .initial (.land _ giftsSnapshot rfl sound)
    have aged : SnapshotHistory.Run joinedPublished order ⟨month, [giftsSnapshot], []⟩ :=
      .step landed (.tick _ month (by decide))
    exact .step aged (.delete _ giftsWrite (Or.inl rfl) [] (fun _ _ => 0) True
      (by cases order <;> decide))

/-- The history satisfies every premise of the away read's theorems. -/
def joinedSource (order : SnapshotOrder) : Source order 0 where
  world := joinedWorld
  history := joined
  published := joinedPublished
  unique := by
    intro u v pu pv _ number
    rcases pu with rfl | rfl <;> rcases pv with rfl | rfl <;> simp_all [giftsWrite, storeWrite]
  runs := joined_runs order
  listedStored := by
    intro t s member _ n positive within
    by_cases ht : t = 0
    · simp [joined, ht] at member
    · simp only [joined, ht, ↓reduceIte, List.mem_singleton] at member
      subst member
      have one : n = 1 := by simp [giftsSnapshot, Snapshot.position] at within; omega
      exact ⟨giftsWrite, Or.inl rfl, rfl, one.symm, by simp [giftsWrite]; omega⟩
  absent := by
    intro t w pub _ _ missing
    rcases pub with rfl | rfl
    · simp [joined]
    · have hit : snapshotStore ReloadExamples.crossAudienceState
          (.log .write 0 storeWrite.number) ≠ none := by decide
      exact False.elim (hit missing)

theorem joined_ordered (order : SnapshotOrder) : Ordered (joinedSource order) := by
  constructor
  · intro v pub _ n positive within
    rcases pub with rfl | rfl
    · exact ⟨giftsWrite, Or.inl rfl, rfl, by simp [giftsWrite] at within ⊢; omega⟩
    · by_cases one : n = 1
      · exact ⟨giftsWrite, Or.inl rfl, rfl, one.symm⟩
      · exact ⟨storeWrite, Or.inr rfl, rfl, by simp [storeWrite] at within ⊢; omega⟩
  · intro u v pu pv _ _ numbers
    rcases pu with rfl | rfl <;> rcases pv with rfl | rfl <;>
      simp_all [giftsWrite, storeWrite, month, day]

/-- The rule in force: the device lists only the store's snapshots, finds
none to load, and then trusts every miss. GET 1 misses at every time,
while write 2, with a store part, stays stored; the device stays at 0. -/
theorem store_only_reader_stalls (order : SnapshotOrder) (fuel time : Nat) :
    catalogCurrent order (joined 1).landed 0 = none ∧
    needsSnapshots (some month) month 0 = false ∧
    (readRecentLog joinedWorld (fun _ => none) (fun _ => false) 0 (fuel + 1) time 0
      (some month) month (fun _ => 0)).stop = .miss ∧
    (readRecentLog joinedWorld (fun _ => none) (fun _ => false) 0 (fuel + 1) time 0
      (some month) month (fun _ => 0)).position = 0 ∧
    joinedWorld time (.log .write 0 2) ≠ none ∧ 0 ∈ storeWrite.audiences := by
  have missing : joinedWorld time (.log .write 0 1) = none := by
    simp only [joinedWorld]; decide
  have stored : joinedWorld time (.log .write 0 2) ≠ none := by
    simp only [joinedWorld]; decide
  have recent : needsSnapshots (some month) month 0 = false := by decide
  refine ⟨by cases order <;> decide, recent, ?_, ?_, stored, by decide⟩ <;>
    simp [readRecentLog, readLog, missing, recent]

/-- The away read passes deleted write 1, which has no store part, and
receives write 2; its terminal miss at 3 is certified by a later catalog. -/
theorem away_read_passes (order : SnapshotOrder) :
    scan joinedWorld joined order [0] 0 3 (month + 1) 0 (joined 1).landed =
      .miss (month + 3) 2 [2] [1] := by
  cases order <;> decide

/-- The general theorem applies: write 2 is received. -/
theorem away_read_receives (order : SnapshotOrder) :
    storeWrite.number ∈ [2] ∧
      ∃ t o, month + 1 ≤ t ∧ joinedWorld t (.log .write 0 storeWrite.number) = some o :=
  away_complete (joinedSource order) (joined_ordered order) [0] 3 (month + 1) 0 (joined 1).landed
    (month + 3) 2 [2] [1] ⟨1, by decide, rfl⟩ (away_read_passes order)
    (by intro w pub _; rcases pub with rfl | rfl <;> simp [giftsWrite, storeWrite])
    storeWrite (Or.inr rfl) rfl (by decide) (by simp [storeWrite, month, day])
    ⟨0, by simp, by simp [storeWrite]⟩

/-- Ana's store write 1 is stored right after the device's GET of it misses,
and a Gifts snapshot through it lands before the next listing. -/
def lateWrite : Write := ⟨0, 1, 1, [0]⟩
def lateSnapshot : Snapshot := ⟨1, 1, 1, 1, 1, [(0, 1)]⟩

def late (t : Nat) : SnapshotHistory.State :=
  if t ≤ 1 then ⟨0, [], []⟩ else ⟨t, [lateSnapshot], []⟩

def lateWorld : World := fun t path =>
  if t ≤ 1 then none else match path with
    | .log .write 0 1 => some ⟨⟨[1], 1⟩, 0⟩
    | _ => none

def latePublished (w : Write) : Prop := w = lateWrite

theorem late_runs (order : SnapshotOrder) (t : Nat) :
    SnapshotHistory.Run latePublished order (late t) := by
  by_cases ht : t ≤ 1
  · simp only [late, ht, ↓reduceIte]; exact .initial
  · simp only [late, ht, ↓reduceIte]
    have sound : SnapshotHistory.Sound latePublished lateSnapshot := by
      constructor
      all_goals
        intro w pub ha
        subst pub
        simp [lateWrite, lateSnapshot] at ha
    have ticked : SnapshotHistory.Run latePublished order ⟨1, [], []⟩ :=
      .step .initial (.tick _ 1 (by decide))
    have landed : SnapshotHistory.Run latePublished order ⟨1, [lateSnapshot], []⟩ :=
      .step ticked (.land _ lateSnapshot rfl sound)
    exact .step landed (.tick _ t (by show 1 ≤ t; omega))

def lateSource (order : SnapshotOrder) : Source order 0 where
  world := lateWorld
  history := late
  published := latePublished
  unique := by intro u v pu pv _ _; rw [pu, pv]
  runs := late_runs order
  listedStored := by
    intro t s member _ n positive within
    by_cases ht : t ≤ 1
    · simp [late, ht] at member
    · simp only [late, ht, ↓reduceIte, List.mem_singleton] at member
      subst member
      have one : n = 1 := by simp [lateSnapshot, Snapshot.position] at within; omega
      exact ⟨lateWrite, rfl, rfl, one.symm, by simp [lateWrite]; omega⟩
  absent := by
    intro t w pub _ stored missing
    subst pub
    have later : ¬ t ≤ 1 := by simp [lateWrite] at stored; omega
    simp [lateWorld, later, lateWrite] at missing

/-- The device's GET of write 1 at time 1 misses before it is stored; the
catalog listed after shows Gifts coverage through it and no store snapshot.
Judged by that catalog alone, the number is passed, though write 1 is stored
with a store part from then on. The away read reads it again and receives it. -/
theorem after_catalog_skips_live_write (order : SnapshotOrder) :
    scanAfterOnly lateWorld late order [0] 0 2 1 0 = .miss 2 1 [] [1] ∧
    lateWorld 2 (.log .write 0 1) ≠ none ∧ 0 ∈ lateWrite.audiences ∧
    scan lateWorld late order [0] 0 3 1 0 (late 1).landed = .miss 3 1 [1] [] := by
  refine ⟨by cases order <;> decide, by decide, by decide, by cases order <;> decide⟩

end CovenIO.AwayReadExamples
