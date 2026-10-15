import CovenIO.AwayRead

namespace CovenIO.AwayReadExamples
open AwayRead

/-! Ana's Gifts-only write 1 is deleted and her store write 2 stays stored;
Dave reads only the store and joins later. Retention refuses write 1's
deletion until a store snapshot covers it too, so Dave's miss of write 1
loads that snapshot, and he then receives write 2. -/

/-- Ana's Gifts-only write 1 and her store write 2 (audience 0 is the store,
audience 1 Gifts). -/
def giftsWrite : Write := ⟨0, 1, 0, [1]⟩
def storeWrite : Write := ⟨0, 2, month, [0]⟩
/-- Counting Gifts and store snapshots through Ana's write 1. -/
def giftsSnapshot : Snapshot := ⟨1, 0, 1, 0, 0, [(0, 1)]⟩
def storeSnapshot : Snapshot := ⟨0, 0, 2, 0, 0, [(0, 1)]⟩

/-- Gifts' coverage alone no longer lets write 1 go: the store's current
newest must cover it as well. This is the deletion that left a store-only
reader with no snapshot naming write 1, stopped at its miss forever. -/
theorem gifts_only_deletion_refused (order : SnapshotOrder) :
    ¬ Eligible order [giftsSnapshot] [] (fun _ _ => 0) month True giftsWrite := by
  cases order <;> decide

/-- The catalog after write 1's deletion; before any listing nothing is known. -/
def joined (t : Nat) : SnapshotHistory.State :=
  if t = 0 then ⟨0, [], []⟩ else ⟨month, [storeSnapshot, giftsSnapshot], [giftsWrite]⟩

/-- Ana's log as Dave reads it: write 1 deleted, write 2 stored. -/
def joinedWorld : World := fun _ path =>
  match path with
  | .log .write 0 2 => some ⟨⟨[2], month⟩, 0⟩
  | _ => none

def joinedPublished (w : Write) : Prop := w = giftsWrite ∨ w = storeWrite

theorem joined_runs (order : SnapshotOrder) (t : Nat) :
    SnapshotHistory.Run joinedPublished order (joined t) := by
  by_cases ht : t = 0
  · simp only [joined, ht, ↓reduceIte]; exact .initial
  · simp only [joined, ht, ↓reduceIte]
    have sound : ∀ snap ∈ [giftsSnapshot, storeSnapshot], SnapshotHistory.Sound joinedPublished snap := by
      intro snap member
      constructor
      all_goals
        intro w pub ha
        simp only [List.mem_cons, List.not_mem_nil, or_false] at member
        rcases member with rfl | rfl <;> rcases pub with rfl | rfl <;>
          simp_all [giftsWrite, storeWrite, giftsSnapshot, storeSnapshot, Snapshot.covers,
            Snapshot.position]
    have gifts : SnapshotHistory.Run joinedPublished order ⟨0, [giftsSnapshot], []⟩ :=
      .step .initial (.land _ giftsSnapshot rfl (sound _ (by simp)))
    have both : SnapshotHistory.Run joinedPublished order ⟨0, [storeSnapshot, giftsSnapshot], []⟩ :=
      .step gifts (.land _ storeSnapshot rfl (sound _ (by simp)))
    have aged : SnapshotHistory.Run joinedPublished order ⟨month, [storeSnapshot, giftsSnapshot], []⟩ :=
      .step both (.tick _ month (by decide))
    exact .step aged (.delete _ giftsWrite (Or.inl rfl) [] (fun _ _ => 0) True
      (by cases order <;> decide))

/-- The history satisfies every premise of the away read's theorems. -/
def joinedSource (order : SnapshotOrder) : Source order 0 where
  world := joinedWorld
  history := joined
  published := joinedPublished
  runs := joined_runs order
  absent := by
    intro t w pub _ _ missing
    rcases pub with rfl | rfl
    · simp [joined]
    · simp [joinedWorld, storeWrite] at missing

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

/-- Dave's GET of write 1 misses; the store's current newest snapshot
reaches it, so he loads that snapshot. From its position he receives write 2,
and his miss of write 3 is terminal. -/
theorem away_read_reloads (order : SnapshotOrder) :
    scan joinedWorld joined order 0 3 (month + 1) 0 = .reload (month + 2) 1 ∧
    scan joinedWorld joined order 0 3 (month + 2) 1 = .miss (month + 3) 2 [2] := by
  constructor <;> cases order <;> decide

/-- The general theorem applies: write 2 is received. -/
theorem away_read_receives (order : SnapshotOrder) :
    storeWrite.number ∈ [2] ∧
      ∃ t o, month + 2 ≤ t ∧ joinedWorld t (.log .write 0 storeWrite.number) = some o :=
  away_complete (joinedSource order) (joined_ordered order) 3 (month + 2) 1
    (month + 3) 2 [2] (away_read_reloads order).2 storeWrite (Or.inr rfl) rfl (by decide)
    (by simp [storeWrite, month, day])

/-! The reload's step that a deleted write an audience's current newest
does not reach has no part there stays. Snapshot metadata names no part
audiences, so nothing else tells Gifts that Ana's store-only write 1 had
none. In this history no Gifts snapshot ever reaches it, so a reload waiting
for one would wait forever. The number is not passed: the store's selection
holds it (`CommonReload.committed_store_holds`). -/

def storeOnly : Write := ⟨0, 1, 0, [0]⟩
def bothParts : Write := ⟨0, 2, month, [0, 1]⟩
def storeThrough : Snapshot := ⟨0, 0, 1, 0, 0, [(0, 1)]⟩

def storeDeleted (t : Nat) : SnapshotHistory.State :=
  if t = 0 then ⟨0, [], []⟩ else ⟨month, [storeThrough], [storeOnly]⟩

def bothWorld : World := fun _ path =>
  match path with
  | .log .write 0 2 => some ⟨⟨[2], month⟩, 0⟩
  | _ => none

theorem store_deleted_runs (order : SnapshotOrder) (t : Nat) :
    SnapshotHistory.Run (fun w => w = storeOnly ∨ w = bothParts) order (storeDeleted t) := by
  by_cases ht : t = 0
  · simp only [storeDeleted, ht, ↓reduceIte]; exact .initial
  · simp only [storeDeleted, ht, ↓reduceIte]
    have sound : SnapshotHistory.Sound (fun w => w = storeOnly ∨ w = bothParts) storeThrough := by
      constructor
      all_goals
        intro w pub ha
        rcases pub with rfl | rfl <;>
          simp_all [storeOnly, bothParts, storeThrough, Snapshot.covers, Snapshot.position]
    have landed : SnapshotHistory.Run (fun w => w = storeOnly ∨ w = bothParts) order ⟨0, [storeThrough], []⟩ :=
      .step .initial (.land _ storeThrough rfl sound)
    have aged : SnapshotHistory.Run (fun w => w = storeOnly ∨ w = bothParts) order
        ⟨month, [storeThrough], []⟩ := .step landed (.tick _ month (by decide))
    exact .step aged (.delete _ storeOnly (Or.inl rfl) [] (fun _ _ => 0) True
      (by cases order <;> decide))

/-- At every time after the deletion Gifts' current newest stops short of
write 1, and Gifts' reload still completes with write 2's part. -/
theorem no_part_needs_no_snapshot (order : SnapshotOrder) (t : Nat) :
    FixedReload.position (catalogCurrent order (storeDeleted (t + 1)).landed 1) 0 < storeOnly.number ∧
    FixedReload.scan bothWorld storeDeleted order 1 none [] [storeOnly, bothParts] (t + 1) =
      .complete [bothParts] := by
  constructor
  · cases order <;> simp [storeDeleted, catalogCurrent, newest, storeThrough, FixedReload.position, storeOnly]
  · cases order <;> simp [FixedReload.scan, storeDeleted, bothWorld, catalogCurrent, newest, storeThrough,
      FixedReload.position, FixedReload.deletedNumber, storeOnly, bothParts, Snapshot.position, Snapshot.counts]

end CovenIO.AwayReadExamples
