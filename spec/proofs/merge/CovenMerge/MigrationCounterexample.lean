import CovenMerge.Migration

/-! One circle row has no independent removal cause. The database differential
checks Rust's retirement; these examples check the specified selection and
the reductions that remove the difference. Finality is a replay input. -/
namespace CovenMerge.MigrationCounterexample

private def inputs : EntryInputs Unit Unit where
  rows := [()]
  present _ := true
  refs _ := []
  checkFails _ := false
  inDeletedCircle _ := some 1
  claims _ := [⟨⟨[], none⟩, (), 1, true⟩]
  rank _ := 0

theorem nonfinal_retained :
    (entryView inputs).removed () = true ∧
    migrationRemoves inputs (fun _ => false) () = false := by decide

theorem no_row_no_selection :
    migrationRemoves { inputs with present := fun _ => false }
      (fun _ => false) () = false := by decide

theorem no_deletion_no_selection :
    migrationRemoves { inputs with inDeletedCircle := fun _ => none }
      (fun _ => false) () = false := by decide

theorem final_selected :
    migrationRemoves inputs (fun _ => true) () = true := by decide

end CovenMerge.MigrationCounterexample
