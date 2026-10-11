import CovenMerge.Reset

/-! An empty reset snapshot and one unread insert. The database differential
checks the Rust result; these examples check the reset's specified result and
its reductions without reproducing Rust's eligibility predicate. -/
namespace CovenMerge.ResetCounterexample

private def writes : Writes Unit Unit Unit where
  ts _ := 1
  past _ _ := false
  chg _ _ := some ⟨.ins, 0, fun _ => true⟩

private def boundary : ResetBoundary Unit where
  covered _ := false
  inputs _ := false
  inputs_covered _ h := h
  entry := 1

theorem writes_valid : Valid writes where
  ts_inj _ _ _ := Subsingleton.elim _ _
  past_ts _ _ h := Bool.noConfusion h
  gen_seen _ _ ch h := by cases Option.some.inj h; exact Or.inl rfl
  parity _ _ ch h := by cases Option.some.inj h; decide

theorem unread_insert_ignored :
    (reloadReset writes boundary (fun _ _ => false) St.init [()]).gen () = 0 ∧
    (reloadReset writes boundary (fun _ _ => false) St.init [()]).cell () () = none ∧
    (reloadReset writes boundary (fun _ _ => false) St.init [()]).lost () () () = none := by decide

theorem read_reset_applies :
    (reloadReset writes boundary (fun _ _ => true) St.init [()]).gen () = 1 ∧
    (reloadReset writes boundary (fun _ _ => true) St.init [()]).cell () () = some () := by decide

theorem no_write_no_gap :
    reloadReset writes boundary (fun _ _ => false) (St.init : St Unit Unit Unit) [] = St.init := rfl

theorem no_reset_applies :
    (reloadSnapshot writes boundary.toSnapshotBoundary (fun _ => true) St.init [()]).gen () = 1 := by decide

end CovenMerge.ResetCounterexample
