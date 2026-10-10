import CovenStorelogData.KeySelection
import CovenStorelogData.Access
import CovenStorelogData.Keys
import CovenStorelogData.CurrentReplay
import CovenStorelog.ExamplesSupport

/-! Checked boundaries of §3's Revocation sentence and §11's knowledge rule.
The competing changes below contradict no pair of states: they concern
different members. Their outcomes depend on the last-admin check, shared by
the earlier and current replay rules, rather than on removed key conflicts. -/
namespace CovenStorelogData.SecurityExamples
open CovenStorelog
open CovenStorelog.Examples (entry)
open KeySelection

/-- Ana and Ben are admins; Carol is a member. Ana has two devices,
Ben has two devices. Three concurrent changes follow their common prefix. -/
def log : Log
  | 0 => entry 0 0 [] (.create "ana")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol")
  | 3 => entry 1 1 [0, 1, 2] (.addDevice 1 1)
  | 4 => entry 1 1 [0, 1, 2, 3] (.addDevice 1 4)
  | 5 => entry 0 0 [0, 1, 2, 3, 4] (.addDevice 0 3)
  | 6 => entry 0 0 (List.range 6) (.changeRole 1 .member)
  | 7 => entry 0 3 (List.range 6) (.changeRole 2 .admin)
  | _ => entry 1 1 (List.range 6) (.removeMember 0 [])

def storageHistory : Finality.History := ⟨log, id, fun e => if e < 6 then e else 6⟩
def result (tail : List Nat) :=
  CurrentReplay.resolve storageHistory 30 9 (entrySet (List.range 6 ++ tail))
def removed := result [8, 7, 6]
def members (_ : Audience) : List Nat := removed.state.members.map Prod.fst
def receivedAuthorized (e : Nat) : Bool :=
  e < 9 && authorized (CurrentReplay.authorViews storageHistory 30 9 9 e) (log e)
def initialKey : KeySelection.Key := ⟨0, 0, .store⟩
def replacement : KeySelection.Key := ⟨1, 8, .store⟩
def phone : Knowledge := learn members
  ⟨[initialKey, replacement], [(initialKey, 0), (initialKey, 1), (initialKey, 2),
    (replacement, 1), (replacement, 2)], []⟩ []

/-- Ben's tablet first receives the demotion: removing Ana would leave no
admin, so it drops and the tablet shares its key with Ana (§11). Ben's phone
receives Carol's promotion first; it never sees its removal drop. Neither a
membership entry nor a receipt tells it that the tablet disclosed the key. -/
def actual := phone.delivered ++ [(replacement, 0)]
def tabletMembers (_ : Audience) := (result [8, 6]).state.members.map Prod.fst

theorem revocation_counterexample :
    Valid log 9 ∧
    CausalOrder log (List.range 6 ++ [8, 6, 7]) ∧
    CausalOrder log (List.range 6 ++ [8, 7, 6]) ∧
    authorized (authorView log 8) (log 8) = true ∧
    initialRecipients log ⟨8, .store⟩ = [2, 1] ∧
    8 ∈ (result [8]).kept ∧ 8 ∈ (result [8, 6]).dropped ∧
    member (result [8, 6]).state 0 = true ∧
    lookup (result [8, 6]).state.devices 4 = some 1 ∧
    share tabletMembers phone 0 replacement = some { phone with delivered := actual } ∧
    8 ∈ (result [8, 7]).kept ∧ 8 ∈ removed.kept ∧
    member removed.state 0 = false ∧
    member removed.state 1 = true ∧ lookup removed.state.devices 1 = some 1 ∧
    select members receivedAuthorized phone 1 .store = some replacement ∧
    exposed members phone replacement = false ∧
    (replacement, 0) ∈ actual := by
  refine ⟨validCheck_sound _ _ (by decide), ?_, ?_, ?_⟩
  · apply causalCheck_sound _ .nil; decide
  · apply causalCheck_sound _ .nil; decide
  · decide

theorem storage_history_valid : Finality.Valid storageHistory 9 := by
  constructor
  · exact validCheck_sound _ _ (by decide)
  · intro a b _ _ same; exact same
  · intro e _; change (if e < 6 then e else 6) ≤ e; split <;> omega
  · have checked : ∀ w a : Fin 9,
        hadRead log w a = true ↔ a.val < storageHistory.attempted w := by decide
    intro w a hw ha
    exact checked ⟨w, hw⟩ ⟨a, ha⟩

/-- §3's absolute conclusion fails although the sender knows the removal and
obeys §11. A demotion, promotion and removal, one unseen historical-key copy,
and one first attempt suffice after setup. -/
example :
    select members receivedAuthorized phone 1 .store = some replacement ∧
    member removed.state 0 = false ∧ (replacement, 0) ∈ actual := by decide

theorem learning_delivery_prevents_reuse :
    select members receivedAuthorized
      (learn members phone [(replacement, 0)]) 1 .store = none := by decide

/-- The owner has one request in flight. A changed replay records the grant,
then the old revoke completes and queues that grant before reporting success. -/
def inFlight : Access.Account :=
  ⟨"carol", false, true, some (.sending false)⟩
def returned := Access.adopt true inFlight

theorem serialized_regrant :
    Access.start returned = none ∧
    Access.complete returned (.ok ()) =
      some ⟨"carol", true, false, some (.ready true)⟩ ∧
    Access.complete ⟨"carol", true, false, some (.sending true)⟩ (.ok ()) =
      some ⟨"carol", true, true, none⟩ := by decide

/-- A provider cannot simultaneously equal two different local intentions.
§4 explicitly promises intended access, and exposes pending provider work. -/
example : ¬ ∃ actual : Bool, actual = true ∧ actual = false := by decide

/-- Several rotations coexist; the newest usable introduction is selected.
A retired key remains unavailable even after its old recipient returns. -/
def older : KeySelection.Key := ⟨8, 4, .store⟩
def newerA : KeySelection.Key := ⟨9, 5, .store⟩
def newerB : KeySelection.Key := ⟨10, 5, .store⟩

theorem rotations_coexist :
    select (fun _ => [0]) (fun _ => true) ⟨[newerA, older, newerB], [], []⟩
      0 .store = some newerB ∧
    select (fun _ => [0, 1]) (fun _ => true)
      ⟨[replacement], [(replacement, 1)], [replacement]⟩ 0 .store = none := by decide

end CovenStorelogData.SecurityExamples
