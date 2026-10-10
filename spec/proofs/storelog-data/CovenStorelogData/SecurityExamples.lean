import CovenStorelogData.KeySelection
import CovenStorelogData.Access
import CovenStorelogData.Keys
import CovenStorelog.CurrentReplay
import CovenStorelog.ExamplesSupport

/-! §11: sealed copies listed before selection prevent the historical
revocation failure; a copy published after the listing is the residual window. -/
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
def keyIds (e : Nat) (_ : Audience) : Nat := if e = 8 then 1 else 0
def initialKey : KeySelection.Key := ⟨0, 0, .store⟩
def replacement : KeySelection.Key := ⟨1, 8, .store⟩
def custody := [initialKey, replacement]
def listed : KeySelection.Copies := [(initialKey, 0), (initialKey, 1), (initialKey, 2),
  (replacement, 1), (replacement, 2)]
def phone : KeySelection.Pass := ⟨custody, listed⟩
def actual := listed ++ [(replacement, 0)]
def tabletMembers (_ : Audience) := (result [8, 6]).state.members.map Prod.fst

def choose (pass : KeySelection.Pass) :=
  selectInReplay storageHistory 30 9 (fun _ => true) keyIds pass 1 .store

/-- Ben's tablet sees his removal of Ana drop, then shares K with Ana.
His phone sees that removal kept throughout. Both obey their replay. -/
theorem sharing_history :
    Valid log 9 ∧
    CausalOrder log (List.range 6 ++ [8, 6, 7]) ∧
    CausalOrder log (List.range 6 ++ [8, 7, 6]) ∧
    authorized (authorView log 8) (log 8) = true ∧
    initialRecipients log ⟨8, .store⟩ = [2, 1] ∧
    8 ∈ (result [8]).kept ∧ 8 ∈ (result [8, 6]).dropped ∧
    member (result [8, 6]).state 0 = true ∧
    lookup (result [8, 6]).state.devices 4 = some 1 ∧
    share tabletMembers custody listed 0 replacement = some actual ∧
    8 ∈ (result [8, 7]).kept ∧ 8 ∈ removed.kept ∧
    member removed.state 0 = false ∧
    member removed.state 1 = true ∧ lookup removed.state.devices 1 = some 1 := by
  refine ⟨validCheck_sound _ _ (by decide), ?_, ?_, ?_⟩
  · apply causalCheck_sound _ .nil; decide
  · apply causalCheck_sound _ .nil; decide
  · decide

/-- The copy precedes the listing. The phone retires both exposed keys and
waits for rotation; it needs no previous observation of the removal dropping. -/
theorem listing_prevents_reuse :
    beginPass custody actual (.ok ()) = .ok ⟨custody, actual⟩ ∧
    choose ⟨custody, actual⟩ = none ∧
    exposed members ⟨custody, actual⟩ replacement = true := ⟨rfl, by decide⟩

/-- The same copy follows the phone's listing. Its pass still selects K,
which Ana can open. The next complete listing prevents another such choice. -/
theorem residual_window_counterexample :
    beginPass custody listed (.ok ()) = .ok phone ∧
    choose phone = some replacement ∧ exposed members phone replacement = false ∧
    member removed.state 0 = false ∧ (replacement, 0) ∈ actual ∧
    choose ⟨custody, actual⟩ = none := ⟨rfl, by decide⟩

example : choose phone = some replacement ∧
    member removed.state 0 = false ∧ (replacement, 0) ∈ actual := by decide

theorem storage_history_valid : Finality.Valid storageHistory 9 := by
  constructor
  · exact validCheck_sound _ _ (by decide)
  · intro a b _ _ same; exact same
  · intro e _; change (if e < 6 then e else 6) ≤ e; split <;> omega
  · have checked : ∀ w a : Fin 9,
        hadRead log w a = true ↔ a.val < storageHistory.attempted w := by decide
    intro w a hw ha
    exact checked ⟨w, hw⟩ ⟨a, ha⟩

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

/-- Ben and Carol rotate after reading the removal. Their tag-15 entries
coexist; the phone selects the later received, authorized introduction. -/
def rotationLog : Log
  | 9 => entry 1 1 (List.range 9) (.rotateKey .store 2)
  | 10 => entry 2 2 (List.range 9) (.rotateKey .store 3)
  | e => log e

def rotationHistory : Finality.History :=
  ⟨rotationLog, id, fun e => if e < 9 then storageHistory.attempted e else 9⟩
def rotated : KeySelection.Key := ⟨2, 9, .store⟩
def laterRotation : KeySelection.Key := ⟨3, 10, .store⟩
def rotatedPass : KeySelection.Pass := ⟨rotated :: laterRotation :: custody,
  actual ++ [(rotated, 1), (rotated, 2), (laterRotation, 1), (laterRotation, 2)]⟩

theorem rotations_coexist :
    Valid rotationLog 11 ∧
    9 ∈ (CurrentReplay.resolve rotationHistory 30 11 (fun _ => true)).kept ∧
    10 ∈ (CurrentReplay.resolve rotationHistory 30 11 (fun _ => true)).kept ∧
    selectInReplay rotationHistory 30 11 (fun _ => true) keyIds rotatedPass 1 .store =
      some laterRotation ∧
    (laterRotation, 0) ∉ rotatedPass.copies := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

/-- A recipient returning is no longer excluded. No local retirement record
can keep a key unavailable once all its listed recipients belong again. -/
theorem recipient_return_releases_retirement :
    exposed (fun _ => [1, 2]) ⟨custody, actual⟩ replacement = true ∧
    exposed (fun _ => [0, 1, 2]) ⟨custody, actual⟩ replacement = false := by decide

end CovenStorelogData.SecurityExamples
