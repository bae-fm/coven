import CovenStorelog.Horizon
import CovenStorelog.FinalityExamples

namespace CovenStorelog.Horizon.Examples

set_option maxRecDepth 32768

/-- Two concurrent resets land at the same provider time. A third reset,
attempted with the same past, lands after the deadline and is dropped. -/
def tied : Finality.History :=
  { log := fun e => if e = 0 then ⟨0, 0, [], .create "initial"⟩
      else ⟨0, e, [0], .reset ⟨.store, e⟩⟩
    stored := fun e => if e = 0 then 0 else if e < 3 then 1 else 40
    attempted := fun e => if e = 0 then 0 else 1 }

theorem tied_causal : CovenStorelog.Valid tied.log 4 := validCheck_sound _ _ (by decide)

theorem tied_online :
    (∀ e : Fin 4, tied.attempted e ≤ tied.stored e) ∧
    (∀ e a : Fin 4, hadRead tied.log e a = true ↔ tied.stored a < tied.attempted e) := by
  decide

theorem tied_boundaries :
    tied.stored 1 = tied.stored 2 ∧
    Finality.late tied 4 (fun _ => true) 1 = false ∧
    Finality.late tied 4 (fun _ => true) 2 = false ∧
    quiet tied 30 4 31 = true ∧
    horizon tied 30 4 [31] = 1 ∧
    finalSet tied 30 4 [31] 0 = true ∧
    finalSet tied 30 4 [31] 1 = false ∧
    finalSet tied 30 4 [31] 2 = false ∧
    horizon tied 30 4 [31, 32] = 2 ∧
    finalSet tied 30 4 [31, 32] 1 = true ∧
    finalSet tied 30 4 [31, 32] 2 = true := by decide

theorem late_drop_does_not_reopen :
    Finality.tooLate tied 30 4 (fun _ => true) 3 = true ∧
    quiet tied 30 4 40 = false ∧
    horizon tied 30 4 [31, 32, 40] = 2 ∧
    finalSet tied 30 4 [31, 32, 40] 1 = true ∧
    finalSet tied 30 4 [31, 32, 40] 2 = true ∧
    finalSet tied 30 4 [31, 32, 40] 3 = false ∧
    3 ∈ (CurrentReplay.resolve tied 30 4 (fun _ => true)).dropped := by decide

theorem inclusive_recent_window :
    quiet tied 30 4 70 = false ∧
    horizon tied 30 4 [32, 70] = 2 ∧
    quiet tied 30 4 71 = true ∧
    horizon tied 30 4 [32, 70, 71] = 41 ∧
    finalSet tied 30 4 [32, 70, 71] 3 = true := by decide

/-- Retained history can certify day 39's quiet window even if the device
did not run a check then. The late entry at day 40 blocks only newer windows. -/
theorem earlier_window_recovered :
    quiet tied 30 4 40 = false ∧
    horizon tied 30 4 [40] = 0 ∧
    recovered tied 30 4 40 = 9 ∧
    recovered tied 30 4 70 = 9 ∧
    recovered tied 30 4 71 = 41 := by decide

/-- Creation, one reset on day 1, then a retry on day 40 that missed it.
Recomputing only the latest quiet test forgets previously certified finality. -/
theorem latest_check_forgets_finality :
    Finality.Valid (Finality.Examples.history 40) 3 ∧
    finalSet (Finality.Examples.history 40) 30 3 [32] 1 = true ∧
    finalSet (Finality.Examples.history 40) 30 3 [40] 1 = false ∧
    finalSet (Finality.Examples.history 40) 30 3 [32, 40] 1 = true := by
  exact ⟨Finality.Examples.history_valid 40 (by decide), by decide⟩

example : finalSet (Finality.Examples.history 40) 30 3 [32] 1 = true ∧
    finalSet (Finality.Examples.history 40) 30 3 [40] 1 = false := by decide

theorem current_replay_stays_fixed :
    (CurrentReplay.resolve tied 30 4 (Finality.atTime tied 32)).kept = [1, 0] ∧
    (CurrentReplay.resolve tied 30 4 (Finality.atTime tied 40)).kept = [1, 0] ∧
    (CurrentReplay.resolve tied 30 4 (Finality.atTime tied 32)).dropped = [2] ∧
    (CurrentReplay.resolve tied 30 4 (Finality.atTime tied 40)).dropped = [2, 3] := by decide

end CovenStorelog.Horizon.Examples
