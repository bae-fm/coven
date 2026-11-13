import CovenSequence.CircleRaise

/-!
# The raise body coven writes

The model's raise body is the audience's rows at the raise's coverage `c`,
and the store's, migrated: `preparedRows (.raise v) p (raiseView c) A`. Coven's raising device
migrates the rows it held when its app migrated, at slot `m`, and judges each
part that lands after `m` as a late write: converted to the new version, or
dropped where the migration declares `Drop` (§18.5, §18.6). Its capture then
moves to the slots it has applied, `c`, so its body is the rows at `m` migrated,
with the parts of `m+1 .. c` converted (`replay` from `m`).

The two agree on a part when migrating after the part equals converting the
part after migrating (`one_part_agrees`): a converter that is the migration
applied to one change. They differ when a converter drops the part: a write on
the old app landing after the raising device migrated, and before its
coverage, is in the model's body and not in coven's (`late_part_differs`).

A holder that raises by migrating in place has that gap. A holder that
rebuilds instead, from a fresh marker of its own, does not: it queues a marker,
reads the circle and the store from storage at the marker's read slot `m'`, and
migrates those, so its body is the model's body at `m'`
(`holder_body_is_rebuilt`). Every write before `m'` is in it, including one that
landed after the holder's first migration (`write_before_fresh_marker_kept`).
Only a part after `m'` is a late write, converted or dropped as for any
rebuilder (`write_after_fresh_marker_late`, §18.6).
-/

namespace CovenSequence.RaiseBody

open CovenSequence

section General
variable {S : Sig} (R : Rules S)

/-- **The bodies agree on one part when the converter commutes with the
migration.** Slot `m+1` applies a part of `A`. Migrating the rows read at
`m+1` equals converting the part onto the rows migrated at `m` exactly when the
converter does what the migration does to that change. -/
theorem one_part_agrees (seq : Nat → Slot S) (A : S.Aud) (m v : Nat) {d : S.Dev} {g : Nat}
    {parts : S.Aud → Option (WPart S)} {w : WPart S}
    (slot : seq (m + 1) = .writes d g parts) (part : parts A = some w)
    (applied : AppliedTo R seq A (m + 1))
    (commute : R.migrate v (raiseView R seq A (m + 1)) A =
      R.merge (R.migrate v (raiseView R seq A m) A)
        (R.convert w.version v (R.sinceReset (audAt R seq A m).lastReset w.batch))) :
    preparedRows R (.raise v) p (raiseView R seq A (m + 1)) A =
      applyWrite R A v (audAt R seq A m).lastReset (R.migrate v (raiseView R seq A m) A) (seq (m + 1)) := by
  have valid := applied.2
  simp only [Nat.add_sub_cancel] at valid
  rw [slot] at valid ⊢
  simp [part, applyWrite, preparedRows, commute]

end General

open ReaderExamples CircleRaise

/-- A migration that declares `Drop`: a part converted up a version is empty,
and merging an empty part changes nothing. -/
def dropping : Rules sig :=
  { baseOne with
    merge := fun (r : List Nat) (b : Nat) => if b = 0 then r else r ++ [b]
    convert := fun older newer b => if older < newer then 0 else b }

/-- Slot 1 holds a part written before the raising device migrated; slot 2 a
part on the old app that lands after it migrated and before its coverage. -/
def lateHistory : Nat → Slot sig
  | 1 => .writes () 0 (fun _ => some ⟨1, [], 10, 0⟩)
  | 2 => .writes () 0 (fun _ => some ⟨1, [], 20, 0⟩)
  | _ => .writes () 0 (fun _ => none)

/-- The rows of `lateHistory` through slot 1 and through slot 2. -/
theorem late_views :
    raiseView dropping lateHistory () 1 = (fun _ => [10]) ∧
    raiseView dropping lateHistory () 2 = (fun _ => [10, 20]) := by
  have one : rowsAt dropping lateHistory () 1 = [10] := by
    rw [rowsAt_other dropping lateHistory () 0 (by intros; simp [lateHistory]), rowsAt]
    decide
  have two : rowsAt dropping lateHistory () 2 = [10, 20] := by
    rw [rowsAt_other dropping lateHistory () 1 (by intros; simp [lateHistory]), one]
    decide
  constructor
  · funext B; simp [raiseView, inputSlot, one]
  · funext B; simp [raiseView, inputSlot, two]

/-- **Counterexample: coven's raise body isn't the model's.** The raising
device migrated after slot 1 and its raise covers slot 2. The model's body
migrates both parts, `[10, 20, 1002]`. Coven's migrates slot 1's rows and judges
slot 2's part as a late write, which `Drop` leaves out: `[10, 1002]`. -/
theorem late_part_differs :
    AppliedTo dropping lateHistory () 2 ∧
    preparedRows dropping (.raise 2) (6 : Nat) (raiseView dropping lateHistory () 2) () = [10, 20, 1002] ∧
    replay dropping () lateHistory (fun _ => false) 2 0
      (dropping.migrate 2 (raiseView dropping lateHistory () 1) ()) 2 1 = [10, 1002] := by
  obtain ⟨view₁, view₂⟩ := late_views
  refine ⟨by unfold AppliedTo; decide, ?_, ?_⟩
  · rw [view₂]; rfl
  · rw [view₁]; rfl

section Holder
variable {S : Sig} (R : Rules S)

/-- **A holder's body from a fresh marker is the rebuilder's.** A holder queues
a marker of its own that read through slot `m'`, and rebuilds the circle `A`
from storage as a rebuilder does, reading the circle and the store up to `m'`
and migrating a view of just those two audiences. Its body is the raise body
prepared at `m'`, the model's body there: `rebuilt_body_honest` with the
holder's marker slot as the rebuild slot. -/
theorem holder_body_is_rebuilt {st : Store S} {seq : Nat → Slot S} (g : Good R st seq)
    (A : S.Aud) (v m' : Nat) (p : S.Path) (head : m' ≤ st.head)
    (floorA : st.floor.covers A ≤ m')
    (floorS : st.floor.covers R.store ≤ inputSlot R seq A m' R.store)
    (dA dS : Reader S) (okA : ReaderOk R st seq A dA) (okS : ReaderOk R st seq R.store dS)
    (startA : dA.pos ≤ m') (startS : dS.pos ≤ inputSlot R seq A m' R.store) :
    ∃ rA rS : Reader S,
      ReadSteps R A true st dA rA ∧ rA.pos = m' ∧
      ReadSteps R R.store true st dS rS ∧ rS.pos = inputSlot R seq A m' R.store ∧
      R.migrate v (CircleRaise.rebuildView R A rA.rows rS.rows) A =
        preparedRows R (.raise v) p (raiseView R seq A m') A :=
  CircleRaise.rebuilt_body_honest R g A v m' p head floorA floorS dA dS okA okS startA startS

end Holder

/-- **A write that landed after the holder's first migration and before its
fresh marker is kept.** In `lateHistory` the holder migrated after slot 1; slot
2's part, written on the old app, lands before its fresh marker reads through
slot 2. The body is `[10, 20, 1002]`: the part is in it although `Drop` would
lose it as a late write (`late_part_differs`). -/
theorem write_before_fresh_marker_kept :
    preparedRows dropping (.raise 2) (6 : Nat) (raiseView dropping lateHistory () 2) () =
      [10, 20, 1002] := by
  rw [late_views.2]; rfl

/-- **A write after the fresh marker's read slot is late.** With the marker
reading through slot 1, slot 2's part is judged late as for any rebuilder, and
`Drop` leaves it out: the rows at slot 1 migrated, `[10, 1002]`. -/
theorem write_after_fresh_marker_late :
    replay dropping () lateHistory (fun _ => false) 2 0
      (dropping.migrate 2 (raiseView dropping lateHistory () 1) ()) 2 1 = [10, 1002] := by
  rw [late_views.1]; rfl

end CovenSequence.RaiseBody
