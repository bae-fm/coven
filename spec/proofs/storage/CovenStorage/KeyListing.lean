import CovenStorage.KeyDelivery

/-! T204 as decided: every key coven makes is named at the provider with its
store and member, so an admin can list the store's keys; an admin's catch-up
compares that listing with the keys kept entries record and makes every
unrecorded one pending `DeleteAccessKey` in its own database.

The model has one store, so every provider key is in the listing. The
catch-up leaves out only the key its own database is still making. -/

namespace CovenStorage.KeyDelivery.Listing
open CovenStorage.KeyDelivery

/-- The listed keys of the store that no kept entry records. -/
def unrecorded (w : World) (i : Nat) : List Nat :=
  (w.provider.filter fun p => p.id ∉ w.recorded ∧ heldKey (w.copies i) ≠ some p.id).map (·.id)

/-- Admin copy `i`'s catch-up records each unrecorded listed key as pending
deletion, in one transaction. -/
def catchUp (w : World) (i : Nat) : World :=
  { w with pending := fun k => if k = i then unrecorded w i ++ w.pending k else w.pending k }

/-- Copy `i` deletes every key it holds pending deletion (§13). -/
def deletePending (w : World) (i : Nat) : World :=
  { w with provider := w.provider.filter (fun p => p.id ∉ w.pending i)
           pending := fun k => if k = i then [] else w.pending k }

/-- After any surviving admin copy's completed catch-up every live key is
recorded, in that copy's journal, or pending deletion there: also a key whose
minting database was lost, which no reset could carry. -/
theorem catchUp_accounts (w : World) (i : Nat) (hi : i < w.slots) :
    Accounted (catchUp w i) := by
  intro p hp
  by_cases hr : p.id ∈ w.recorded
  · exact .inl hr
  by_cases hh : heldKey (w.copies i) = some p.id
  · exact .inr (.inl ⟨i, hi, hh⟩)
  · refine .inr (.inr ⟨i, hi, ?_⟩)
    simp only [catchUp, ↓reduceIte, unrecorded]
    apply List.mem_append_left
    exact List.mem_map.mpr ⟨p, List.mem_filter.mpr ⟨hp, by simp [hr, hh]⟩, rfl⟩

/-- Ana's desktop (device 7, slot 0) and Carol's laptop (device 9, slot 1),
both admins. Ana makes key 101 for Ben and her database is then lost: no
copy journals 101 and nothing records it. Carol's catch-up lists it and
keeps it pending deletion. -/
theorem lost_database_key_found_by_listing :
    let lost := run .random (start [7, 9] 100) [.begin 0, .mint 0, .drop 0]
    lost.provider = [⟨101, 100⟩] ∧ ¬ Accounted lost ∧
    (catchUp lost 1).pending 1 = [101] ∧ Accounted (catchUp lost 1) := by
  decide

/-- The rule as decided is unsafe while another admin's replacement is in
flight. Ana makes key 101 for Ben and delivers it; before her set-access
entry is kept, Carol's catch-up lists 101 as unrecorded. Ana then records
101 as Ben's access, and Carol deletes what her catch-up made pending:
Ben's recorded current key is gone at the provider, so every device of his
that installs it is locked out (§13). An outstanding invite's key, which no
entry records until the person joins, is deleted the same way. -/
theorem catch_up_deletes_in_flight_key :
    let delivered := run .random (start [7, 9] 100) [.begin 0, .mint 0, .sealKey 0, .deliver 0]
    let after := deletePending (record (catchUp delivered 1) 0) 1
    delivered.provider = [⟨101, 100⟩] ∧
    (catchUp delivered 1).pending 1 = [101] ∧
    after.recorded = [101] ∧ after.provider = [] := by
  decide

end CovenStorage.KeyDelivery.Listing
