import CovenMerge.Circles

/-! One circle row and one kept deletion entry distinguish the Boolean removal
result from the entry-named result. The Rust differential checks both outputs
against the actual implementations, including deletion of either event. -/

namespace CovenMerge.EntryCounterexample

private def writes : Writes Unit Unit Unit where
  ts _ := 1
  past _ _ := false
  chg _ _ := some ⟨.ins, 0, fun _ => false⟩

private def state : St Unit Unit Unit := [()].foldl (step writes) St.init

private def inputs : EntryInputs Unit Unit where
  rows := [()]
  present r := decide (state.gen r % 2 = 1)
  refs _ := []
  checkFails _ := false
  inDeletedCircle _ := some 1
  claims _ := []
  rank _ := 0

theorem writes_valid : Valid writes where
  ts_inj _ _ _ := Subsingleton.elim _ _
  past_ts _ _ seen := Bool.noConfusion seen
  gen_seen _ _ ch change := by
    cases Option.some.inj change
    exact Or.inl rfl
  parity _ _ ch change := by
    cases Option.some.inj change
    decide

/-- Both hide the row and retain generation 1, but only `entryView` names the
responsible entry. This is the missing information in Rust's `DeletedCircle`. -/
theorem entry_identity_gap :
    state.gen () = 1 ∧ state.cell () () = none ∧
    (view inputs.erase).removed () = true ∧ (entryView inputs).removed () = true ∧
    (view inputs.erase).rules () = [.deletedCircle] ∧
    (entryView inputs).rules () = [.deletedCircleEntry 1] := by decide

/-- No write means no present row and hence no removed-row cause to differ. -/
theorem no_row_no_gap :
    let absent := { inputs with present := fun _ => false }
    (view absent.erase).rules () = [] ∧ (entryView absent).rules () = [] := by decide

/-- Without the deletion entry neither implementation removes the row. -/
theorem no_entry_no_gap :
    let kept := { inputs with inDeletedCircle := fun _ => none }
    (view kept.erase).shown () = true ∧ (entryView kept).shown () = true ∧
    (view kept.erase).rules () = [] ∧ (entryView kept).rules () = [] := by decide

end CovenMerge.EntryCounterexample
