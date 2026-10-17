import CovenStorage.KeyDelivery

/-! A provider key minted by another copy cannot replace a recorded key (§4).
Provider-side reclaim scheduling belongs to sequence effects; this example
checks only the fresh-principal property of the shared mint operation. -/

namespace CovenStorage.KeyDelivery.Listing
open CovenStorage.KeyDelivery

/-- The clone was taken after journaling the operation and before minting.
The original makes and records key 100. The clone's mint names a fresh principal
and makes key 101 beside it. -/
theorem clone_mint_spares_recorded_key :
    let after := run (start [7] 100)
      [.begin 0, .clone 0, .mint 0, .record 0, .mint 1]
    after.recorded = [100] ∧ after.provider = [⟨100, 7⟩, ⟨101, 7⟩] := by
  decide

end CovenStorage.KeyDelivery.Listing
