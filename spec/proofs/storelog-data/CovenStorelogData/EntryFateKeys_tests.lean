import CovenStorelogData.EntryFateKeys
import CovenStorelogData.EntryExamples
import CovenStorelogData.EntryFate_tests

namespace CovenStorelogData.EntryFate.KeyTests

open CovenStorelog

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

def old : FreshKey := ⟨0, 0, .circle 0⟩
def fresh : FreshKey := ⟨0, 1, .circle 0⟩
def known : Deliveries := [(old, ⟨0, 0⟩), (old, ⟨1, 1⟩)]
def actual : Deliveries := deliver old [⟨2, 2⟩] known
def before := resolve EntryExamples.additionLog 9 (entrySet (List.range 7))
def added := resolve EntryExamples.additionLog 9 (entrySet (List.range 7 ++ [8]))
def final := EntryExamples.additionResult

/-- One online circle addition is sufficient. Ana delivers K to Carol; Ben
has not received that addition or its delivery and still uses K with an
audience excluding Carol. All data can be online; calls and reads interleave.
The second concurrent removal later makes the addition drop as well. -/
theorem unseen_delivery_counterexample :
    authorized (authorView EntryExamples.additionLog 8) (EntryExamples.additionLog 8) = true ∧
    inCircle added.state 0 2 = true ∧ inCircle before.state 0 2 = false ∧
    safeKey before known old = true ∧
    refresh before known old fresh = old ∧ safeKey before actual old = false ∧
    8 ∈ final.dropped ∧ inCircle final.state 0 2 = false := by decide

example : safeKey before actual (refresh before known old fresh) ≠ true := by decide

/-- The re-kept removal history is repaired when disclosure is known: Carol
learned the removal's key while its entry was dropped; it is replaced, never
reused. The history without that disclosure keeps its original safe key. -/
def returning : FreshKey := ⟨0, 8, .store⟩
def replacement : FreshKey := ⟨0, 9, .store⟩
def unexposed : Deliveries := [(returning, ⟨0, 0⟩), (returning, ⟨1, 1⟩)]
def exposed : Deliveries := deliver returning [⟨2, 2⟩] unexposed
def finalReplay := EntryExamples.keyResult [6, 7, 8]

theorem known_disclosure_rotates :
    safeKey finalReplay exposed returning = false ∧
    refresh finalReplay exposed returning replacement = replacement ∧
    safeKey finalReplay (deliver replacement [⟨0, 0⟩, ⟨1, 1⟩] exposed) replacement = true ∧
    refresh finalReplay unexposed returning replacement = returning := by decide

/-- Same entries and no new writes, but distinct keys currently used. Both
obey the exception. Equal entries/writes alone do not imply equal key ids. -/
theorem key_convergence_counterexample :
    refresh finalReplay exposed returning replacement ≠
      refresh finalReplay unexposed returning replacement := by decide

/-- Even with a complete custody ledger, Ana's two admin devices can choose
distinct fresh ids for the same unsafe key. Their paths do not race. Neither
per-path create-once nor replay specifies a common winner for these repairs. -/
theorem concurrent_refresh_counterexample :
    let second : FreshKey := ⟨3, 9, .store⟩
    running finalReplay.state 0 0 = true ∧ running finalReplay.state 0 3 = true ∧
    admin finalReplay.state 0 = true ∧
    safeKey finalReplay exposed replacement = true ∧
    safeKey finalReplay exposed second = true ∧
    refresh finalReplay exposed returning replacement ≠
      refresh finalReplay exposed returning second := by decide

/-- One device removal already invalidates every key the device held, even
though the member stays. Fresh material sealed to that member does not create
a device-specific cryptographic boundary. The ability below is not a claim
that an honest stopped sync loop fetches the envelope. -/
theorem device_custody_matters :
    let result := Tests.deviceStopped.result
    let previous : FreshKey := ⟨0, 0, .store⟩
    let next : FreshKey := ⟨0, 1, .store⟩
    let custody : Deliveries := [(previous, ⟨0, 0⟩), (previous, ⟨0, 2⟩)]
    member result.state 0 = true ∧
    recipientAllowed result .store ⟨0, 2⟩ = false ∧
    refresh result custody previous next = next ∧
    canOpenMemberSeal 0 ⟨0, 0⟩ = true ∧ canOpenMemberSeal 0 ⟨0, 2⟩ = true := by decide

end CovenStorelogData.EntryFate.KeyTests
