import CovenStorelog.CurrentExamples
import CovenStorelogData.KeyPhases
import CovenStorelogData.Access
import CovenStorelog.ExamplesSupport

/-! §11: permanent copies prevent reuse when listed; a later share is the
stated residual window. All removals are key-free, with separate rotations. -/
namespace CovenStorelogData.SecurityExamples
open CovenStorelog KeySelection
open CovenStorelog.CurrentExamples (key)

def entry := CurrentReplay.Entry.mk

/-- Ana and Ben are admins; Carol is a member with her own device. Demotion,
promotion and removal are concurrent. Carol rotates after reading removal. -/
def log : CurrentReplay.Log
  | 0 => entry 0 0 [] (.create "ana" (key 0))
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol")
  | 3 => entry 1 1 [0, 1, 2] (.addDevice 1 1)
  | 4 => entry 1 1 [0, 1, 2, 3] (.addDevice 1 4)
  | 5 => entry 0 0 [0, 1, 2, 3, 4] (.addDevice 0 3)
  | 6 => entry 2 2 (List.range 6) (.addDevice 2 2)
  | 7 => entry 0 0 (List.range 7) (.changeRole 1 .member)
  | 8 => entry 0 3 (List.range 7) (.changeRole 2 .admin)
  | 9 => entry 1 1 (List.range 7) (.removeMember 0)
  | _ => entry 2 2 (List.range 7 ++ [9]) (.rotateKey .store (key 1))

def storageHistory : CurrentReplay.History :=
  ⟨log, fun e => if e < 7 then e else if e = 9 then 7 else if e = 10 then 9 else e + 3,
    fun e => if e < 7 then e else if e = 10 then 8 else 7⟩
def result (tail : List Nat) :=
  CurrentReplay.resolve storageHistory 30 11 (entrySet (List.range 7 ++ tail))
def removed := result [9, 10, 8, 7]
def members := audienceMembers removed.state
def initialKey : KeySelection.Key := ⟨0, .store, key 0⟩
def replacement : KeySelection.Key := ⟨10, .store, key 1⟩
def custody := [initialKey, replacement]
def listed : KeySelection.Copies := [(initialKey, 0), (initialKey, 1), (initialKey, 2),
  (replacement, 1), (replacement, 2)]
def phone : KeySelection.Pass := ⟨custody, listed⟩
def actual := listed ++ [(replacement, 0)]
def tabletMembers := audienceMembers (result [9, 10, 7]).state

def choose (pass : KeySelection.Pass) :=
  selectInReplay storageHistory 30 11 (fun _ => true) pass 1 .store

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

theorem sharing_history :
    Valid storageHistory.membership.log 11 ∧
    CausalOrder storageHistory.membership.log (List.range 7 ++ [9, 10, 7, 8]) ∧
    CausalOrder storageHistory.membership.log (List.range 7 ++ [9, 10, 8, 7]) ∧
    introductions log 9 = [] ∧
    receivedAuthorized storageHistory 30 11 (entrySet (List.range 7 ++ [9, 10])) replacement = true ∧
    9 ∈ (result [9, 10]).kept ∧ 9 ∈ (result [9, 10, 7]).dropped ∧
    member (result [9, 10, 7]).state 0 = true ∧
    lookup (result [9, 10, 7]).state.devices 4 = some 1 ∧
    share tabletMembers custody listed 0 replacement = some actual ∧
    9 ∈ (result [9, 10, 8]).kept ∧ 9 ∈ removed.kept ∧
    member removed.state 0 = false ∧ member removed.state 1 = true := by
  refine ⟨validCheck_sound _ _ (by decide), ?_, ?_, ?_⟩
  · apply causalCheck_sound _ .nil; decide
  · apply causalCheck_sound _ .nil; decide
  · decide

theorem listing_prevents_reuse :
    beginPass custody actual (.ok ()) = .ok ⟨custody, actual⟩ ∧
    choose ⟨custody, actual⟩ = none ∧
    exposed members ⟨custody, actual⟩ replacement = true := ⟨rfl, by decide⟩

theorem residual_window_counterexample :
    beginPass custody listed (.ok ()) = .ok phone ∧
    choose phone = some replacement ∧ exposed members phone replacement = false ∧
    member removed.state 0 = false ∧ (replacement, 0) ∈ actual ∧
    choose ⟨custody, actual⟩ = none := ⟨rfl, by decide⟩

example : choose phone = some replacement ∧
    member removed.state 0 = false ∧ (replacement, 0) ∈ actual := by decide

theorem storage_history_valid : Finality.Valid storageHistory.membership 11 := by
  constructor
  · exact sharing_history.1
  · have checked : ∀ a b : Fin 11,
        storageHistory.stored a = storageHistory.stored b → a = b := by decide
    intro a b ha hb same
    exact congrArg Fin.val (checked ⟨a, ha⟩ ⟨b, hb⟩ same)
  · have checked : ∀ e : Fin 11, storageHistory.attempted e ≤ storageHistory.stored e := by decide
    intro e he; exact checked ⟨e, he⟩
  · have checked : ∀ w a : Fin 11,
        hadRead storageHistory.membership.log w a = true ↔
          storageHistory.stored a < storageHistory.attempted w := by decide
    intro w a hw ha; exact checked ⟨w, hw⟩ ⟨a, ha⟩

/-- The single owner's old request completes before the opposite begins. -/
def inFlight : Access.Account := ⟨"carol", false, true, some (.sending false)⟩
def returned := Access.adopt true inFlight

theorem serialized_regrant :
    Access.start returned = none ∧
    Access.complete returned (.ok ()) = some ⟨"carol", true, false, some (.ready true)⟩ ∧
    Access.complete ⟨"carol", true, false, some (.sending true)⟩ (.ok ()) =
      some ⟨"carol", true, true, none⟩ := by decide

example : ¬ ∃ actual : Bool, actual = true ∧ actual = false := by decide

def rotationLog : CurrentReplay.Log
  | 11 => entry 1 1 (List.range 11) (.rotateKey .store (key 2))
  | 12 => entry 2 2 (List.range 11) (.rotateKey .store (key 3))
  | e => log e

def rotationHistory : CurrentReplay.History :=
  ⟨rotationLog, fun e => if e < 11 then storageHistory.stored e else e + 3,
    fun e => if e < 11 then storageHistory.attempted e else 12⟩
def rotated : KeySelection.Key := ⟨11, .store, key 2⟩
def laterRotation : KeySelection.Key := ⟨12, .store, key 3⟩
def rotatedPass : KeySelection.Pass := ⟨rotated :: laterRotation :: custody,
  actual ++ [(rotated, 1), (rotated, 2), (laterRotation, 1), (laterRotation, 2)]⟩

theorem rotations_coexist :
    Valid rotationHistory.membership.log 13 ∧
    11 ∈ (CurrentReplay.resolve rotationHistory 30 13 (fun _ => true)).kept ∧
    12 ∈ (CurrentReplay.resolve rotationHistory 30 13 (fun _ => true)).kept ∧
    selectInReplay rotationHistory 30 13 (fun _ => true) rotatedPass 1 .store = some laterRotation ∧
    (laterRotation, 0) ∉ rotatedPass.copies := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

theorem recipient_return_releases_retirement :
    exposed (fun _ => [1, 2]) ⟨custody, actual⟩ replacement = true ∧
    exposed (fun _ => [0, 1, 2]) ⟨custody, actual⟩ replacement = false := by decide

end CovenStorelogData.SecurityExamples
