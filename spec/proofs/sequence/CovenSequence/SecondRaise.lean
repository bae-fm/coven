import CovenSequence.WriteVersion

/-!
# A second breaking migration and a circle joined after the first

Ben's app migrated to version 2 while Garden was at version 1. Ana, on the old
app, then added him to Garden, and he wrote into it: his write is at version 2,
so it waits for Garden's raise to 2 (coven.md §18.5). Before anyone raises
Garden, Ben's app runs a second breaking migration, to version 3. It captures
the writes through its new marker, his held Garden write among them, and the
audiences it raises. What Garden's capture holds depends on how Ben came to read it:

- **Converted on reading.** Ben read Garden's version-1 parts converted to his
  version 2 and never ran migration 2 on its rows, so the rows he migrates to 3
  aren't Garden's rows migrated to 3 (`second_capture_not_body`), the joiner's
  problem of `joiner_cannot_raise` one version on. **Leaving Garden out**
  instead sends his held write ahead of Garden's version: the model refuses
  its part, and the raise a holder later makes covers that slot without it
  (`skipped_capture_drops_held`). That is the model's outcome; coven's devices
  judge such a part as a late write (`RaiseBody.ahead_part_differs`).
- **Rebuilt.** Ben rebuilds Garden at version 1 and runs migration 2 on its
  rows (`rebuilt_body_honest`), as a member who held it would. His second
  migration then runs step 3 on those rows, and what it captures is Garden's
  rows migrated to 3: the honest body (`rebuilt_second_capture_is_body`).
-/

namespace CovenSequence.SecondRaise

open CovenSequence ReaderExamples CircleRaise

/-- A circle at version 1 whose breaking migrations are steps: migrating rows
to `v` applies steps 2 to `v`, each adding its mark `1000 + step`. -/
def steps : Rules sig :=
  { baseOne with
    migrate := fun v f A => f A ++ (List.range' 2 (v - 1)).map (1000 + ·)
    migrate_reads := by intro v f g A h _; simp [h] }

/-- Slot 1 is Ana's write at version 1; slot 2 her snapshot covering it. -/
def joined : Nat → Slot sig
  | 1 => .writes () 0 (fun _ => some ⟨1, [], 10, 0⟩)
  | 2 => .snapshot () () 1 1 (7 : Nat)
  | _ => .writes () 0 (fun _ => none)

/-- **Counterexample: a second migration's capture of a joined circle isn't
its raise body.** A raise to 3 after slot 2 must cover slot 1, and its body is
slot 1's rows migrated through steps 2 and 3, `[10, 1002, 1003]`. Ben holds the
part he converted on reading, `[10]`, and his second migration runs only step
3 on it, `[10, 1003]`. -/
theorem second_capture_not_body :
    AppliedTo steps joined () 2 ∧
    audValid steps (ledgerAt steps joined 2) (audAt steps joined () 2) 3 ()
      (.prepare () () (.raise 3) 0 3 (6 : Nat)) = false ∧
    preparedRows steps (.raise 3) (6 : Nat) (raiseView steps joined () 1) () = [10, 1002, 1003] ∧
    steps.merge (steps.rows0 ()) (steps.convert 1 2 10) ++ [1003] = [10, 1003] := by
  refine ⟨by unfold AppliedTo; decide, by decide, ?_, by decide⟩
  have one : rowsAt steps joined () 1 = [10] := by
    rw [rowsAt_other steps joined () 0 (by intros; simp [joined]), rowsAt]
    decide
  have view : raiseView steps joined () 1 = fun _ => [10] := by
    funext B; simp [raiseView, inputSlot, one]
  rw [view]
  rfl

/-- **A second migration's capture of a rebuilt circle is its raise body.**
Ben rebuilt Garden at version 1 and migrated it to 2, holding slot 1's rows
migrated, `[10, 1002]`. His second migration runs step 3 on them, and the
result is the circle's rows at that coverage migrated to 3, the body a raise to
3 must carry. -/
theorem rebuilt_second_capture_is_body :
    steps.migrate 2 (raiseView steps joined () 1) () = [10, 1002] ∧
    steps.migrate 2 (raiseView steps joined () 1) () ++ [1003] =
      preparedRows steps (.raise 3) (6 : Nat) (raiseView steps joined () 1) () := by
  have one : rowsAt steps joined () 1 = [10] := by
    rw [rowsAt_other steps joined () 0 (by intros; simp [joined]), rowsAt]
    decide
  have view : raiseView steps joined () 1 = fun _ => [10] := by
    funext B; simp [raiseView, inputSlot, one]
  rw [view]
  exact ⟨rfl, rfl⟩

/-- Slot 1 is Ben's held write, its part at version 2, sent in his second
migration's capture; slots 2 and 3 are Ana's raise of Garden to 3, covering
slot 1. -/
def heldSent : Nat → Slot sig
  | 1 => .writes () 0 (fun _ => some ⟨2, [], 20, 0⟩)
  | 2 => .prepare () () (.raise 3) 1 3 (6 : Nat)
  | 3 => .boundary () () (.raise 3) 1 (6 : Nat)
  | _ => .writes () 0 (fun _ => none)

/-- **Counterexample: in the model, leaving the joined circle out of the capture
loses the held write.** Its part, at version 2 in a circle at version 1, is
refused; the raise to 3 that covers its slot counts, and Garden's rows after it
hold the migration's marks and not the write. Coven's devices judge the part as
a late write instead (`RaiseBody.ahead_part_differs`). -/
theorem skipped_capture_drops_held :
    ¬ AppliedTo steps heldSent () 1 ∧ AppliedTo steps heldSent () 3 ∧
    rowsAt steps heldSent () 3 = [1002, 1003] := by
  have one : rowsAt steps heldSent () 1 = [] := by
    rw [rowsAt_other steps heldSent () 0 (by intros; simp [heldSent]), rowsAt]
    decide
  refine ⟨by unfold AppliedTo; decide, by unfold AppliedTo; decide, ?_⟩
  rw [rowsAt_boundary steps heldSent () 2 (by rfl)]
  have valid : audValid steps (ledgerAt steps heldSent 2) (audAt steps heldSent () 2) 3 ()
      (.boundary () () (.raise 3) 1 (6 : Nat)) = true := by decide
  have view : raiseView steps heldSent () 1 = fun _ => [] := by
    funext B; simp [raiseView, inputSlot, one]
  simp only [valid, and_self, ite_true, view]
  decide

end CovenSequence.SecondRaise
