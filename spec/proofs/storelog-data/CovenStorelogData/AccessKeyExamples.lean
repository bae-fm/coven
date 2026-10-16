import CovenStorelogData.AccessKeys
import CovenStorelog.ExamplesSupport

/-! Executable S3 histories: Ana and Ben are admins, Carol is a member.
The two replacements and removal start from the same online recorded view. -/
namespace CovenStorelogData.AccessKeyExamples
open CovenStorelog CovenStorelogData.Pending Access Access.S3

def entry := CurrentReplay.Entry.mk

def log (secondAuthor : Nat) : CurrentReplay.Log
  | 0 => entry 0 0 [] (.create "ana" ⟨0, Vector.replicate 32 0⟩)
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol-old")
  | 3 => entry 1 1 (List.range 3) (.addDevice 1)
  | 4 => entry 2 2 (List.range 4) (.addDevice 2)
  | 5 => entry 0 3 (List.range 5) (.addDevice 0)
  | 6 => entry 0 0 (List.range 6) (.setAccess 2 "carol-ana")
  | 7 => entry secondAuthor secondAuthor (List.range 6) (.setAccess 2 "carol-other")
  | 8 => entry 0 3 (List.range 6) (.removeMember 2)
  | _ => entry 2 2 (List.range 8) (.setAccess 2 "carol-late")

def history (secondAuthor : Nat) : CurrentReplay.History :=
  ⟨log secondAuthor, fun e => if e = 9 then 40 else e,
    fun e => if e < 6 then e else if e = 9 then 8 else 6⟩

def apply (author : Nat) (s : Journal) (incoming : List Nat) : Journal :=
  (applyEvent (history author) 30 10 s (.entries incoming)).1

def initial := apply 1 Journal.empty (List.range 6)
def replaced (author : Nat) := apply author initial [6]
def raced (author : Nat) := apply author (replaced author) [7]
def removed (author : Nat) := apply author (raced author) [8]

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

theorem checked_metadata :
    validCheck (history 1).membership.log 10 = true ∧
    validCheck (history 2).membership.log 10 = true ∧
    (∀ e a : Fin 10, hadRead (history 1).membership.log e a = true ↔
      (history 1).stored a < (history 1).attempted e) ∧
    (∀ e a : Fin 10, hadRead (history 2).membership.log e a = true ↔
      (history 2).stored a < (history 2).attempted e) ∧
    (∀ e : Fin 10, (history 1).attempted e ≤ (history 1).stored e) ∧
    (∀ e : Fin 10, (history 2).attempted e ≤ (history 2).stored e) := by decide

theorem admin_replacement :
    6 ∈ (replaced 1).result.kept ∧ member (replaced 1).result.state 2 = true ∧
    lookup (replaced 1).result.state.access 2 = some "carol-ana" ∧
    keys (replaced 1) = ["carol-old"] := by decide

/-- An admin and another admin, or an admin and Carol, retain every retired
key. The winning credential is still current until the member is removed. -/
theorem concurrent_replacements : ∀ author : Fin 3, author.val ≠ 0 →
    6 ∈ (raced author).result.kept ∧ 7 ∈ (raced author).result.dropped ∧
    lookup (raced author).result.state.access 2 = some "carol-ana" ∧
    "carol-old" ∈ keys (raced author) ∧ "carol-other" ∈ keys (raced author) ∧
    "carol-ana" ∉ keys (raced author) ∧ (keys (raced author)).length = 2 ∧
    "carol-old" ∈ keys (removed author) ∧ "carol-ana" ∈ keys (removed author) ∧
    "carol-other" ∈ keys (removed author) ∧ (keys (removed author)).length = 3 := by decide

theorem removal_and_later_record :
    8 ∈ (removed 1).result.kept ∧ 6 ∈ (removed 1).result.dropped ∧
    7 ∈ (removed 1).result.dropped ∧
    9 ∈ (apply 1 (removed 1) [9]).result.dropped ∧
    "carol-late" ∈ keys (apply 1 (removed 1) [9]) ∧
    (keys (apply 1 (removed 1) [9])).length = 4 ∧
    Finality.tooLate (history 1).membership 30 10 (fun _ => true) 9 = true := by decide

theorem removal_before_replacements :
    "carol-old" ∈ keys (apply 1 initial [8]) ∧
    "carol-old" ∈ keys (apply 1 (apply 1 initial [8]) [7, 6]) ∧
    "carol-ana" ∈ keys (apply 1 (apply 1 initial [8]) [7, 6]) ∧
    "carol-other" ∈ keys (apply 1 (apply 1 initial [8]) [7, 6]) ∧
    (keys (apply 1 (apply 1 initial [8]) [7, 6])).length = 3 := by decide

theorem opposite_replacement_arrivals :
    keys (apply 1 (apply 1 initial [7]) [6]) = keys (raced 1) ∧
    keys (apply 2 (apply 2 initial [7]) [6]) = keys (raced 2) := by decide

/-- Access-key replacements keep §9's ordinary timestamp rule, distinct from
its admin-over-self priority for signing/sealing-key replacements. -/
def selfFirstLog : CurrentReplay.Log
  | 6 => entry 2 2 (List.range 6) (.setAccess 2 "carol-self")
  | 7 => entry 0 0 (List.range 6) (.setAccess 2 "carol-admin")
  | e => log 1 e

def selfFirst : CurrentReplay.History := { history 1 with log := selfFirstLog }
def selfFirstApplied := (applyEvent selfFirst 30 10 Journal.empty (.entries (List.range 8))).1

theorem self_replacement_can_win :
    validCheck selfFirst.membership.log 10 = true ∧
    6 ∈ selfFirstApplied.result.kept ∧ 7 ∈ selfFirstApplied.result.dropped ∧
    lookup selfFirstApplied.result.state.access 2 = some "carol-self" ∧
    "carol-old" ∈ keys selfFirstApplied ∧ "carol-admin" ∈ keys selfFirstApplied ∧
    "carol-self" ∉ keys selfFirstApplied ∧ (keys selfFirstApplied).length = 2 := by decide

def demotionLog : CurrentReplay.Log
  | 6 => entry 0 0 (List.range 6) (.changeRole 1 .member)
  | 7 => entry 1 1 (List.range 6) (.setAccess 2 "carol-ben")
  | 8 => entry 1 1 (List.range 8) (.setAccess 2 "carol-refused")
  | e => log 1 e

def demotion : CurrentReplay.History := ⟨demotionLog, id, fun e => if e = 7 then 6 else e⟩

theorem authority_uses_recorded_view :
    validCheck demotion.membership.log 9 = true ∧
    admin (authorViews demotion 30 9 9 7) 1 = true ∧
    admin (resolve demotion 30 9 (entrySet (List.range 9))).state 1 = false ∧
    7 ∈ (resolve demotion 30 9 (entrySet (List.range 9))).kept ∧
    8 ∈ (resolve demotion 30 9 (entrySet (List.range 9))).dropped ∧
    authorized (authorViews demotion 30 9 9 8) (demotionLog 8) = false ∧
    authorized initial.result.state (entry 2 2 (List.range 6) (.setAccess 1 "forged")) = false ∧
    authorized initial.result.state (entry 2 2 (List.range 6) (.setAccess 2 "own")) = true := by decide

def clear (s : Journal) (event : Event) := (applyEvent (history 1) 30 10 s event).1
def absence (key : String) : Option S3.Observation := some ⟨key, .ok .absent⟩

theorem deletion_observations :
    verifyDeletion "carol-old" false (.ok ()) (absence "carol-old") = .error .adminRequired ∧
    verifyDeletion "carol-old" true (.error (.storage .byItself)) (absence "carol-old") =
      .error (.deleteFailed (.storage .byItself)) ∧
    verifyDeletion "carol-old" true (.ok ()) none = .error .observationMissing ∧
    verifyDeletion "carol-old" true (.ok ()) (some ⟨"carol-old", .error (.storage .byItself)⟩) =
      .error (.observationFailed (.storage .byItself)) ∧
    verifyDeletion "carol-old" true (.ok ()) (some ⟨"carol-old", .ok .present⟩) =
      .error .stillPresent ∧
    verifyDeletion "carol-old" true (.ok ()) (absence "carol-other") = .error .wrongKey ∧
    verifyDeletion "carol-old" true (.ok ()) (absence "carol-old") = .ok () := by
  exact ⟨rfl, rfl, rfl, rfl, rfl, rfl, rfl⟩

theorem failed_or_missing_read_keeps_pending :
    keys (clear (removed 1) (.providerDeletion "carol-old" true (.ok ()) none)) = keys (removed 1) ∧
    keys (clear (removed 1) (.providerDeletion "carol-old" true (.ok ())
      (some ⟨"carol-old", .error (.storage .byItself)⟩))) = keys (removed 1) ∧
    keys (clear (removed 1) (.providerDeletion "carol-old" true (.ok ())
      (absence "carol-other"))) = keys (removed 1) := by decide

def cleared := clear (removed 1)
  (.providerDeletion "carol-old" true (.ok ()) (absence "carol-old"))
def confirmed := clear cleared (.confirmAccessKeyDeleted "carol-other")

theorem both_clear_paths :
    "carol-old" ∉ keys cleared ∧ "carol-other" ∈ keys cleared ∧
    "carol-ana" ∈ keys cleared ∧ keys confirmed = ["carol-ana"] ∧
    "carol-old" ∉ keys (apply 1 confirmed (List.range 10)) ∧
    "carol-other" ∉ keys (apply 1 confirmed (List.range 10)) ∧
    "carol-late" ∈ keys (apply 1 confirmed (List.range 10)) := by decide

theorem late_access_after_finished_removal :
    keys (clear confirmed (.confirmAccessKeyDeleted "carol-ana")) = [] ∧
    keys (apply 1 (clear confirmed (.confirmAccessKeyDeleted "carol-ana")) [9]) =
      ["carol-late"] := by decide

theorem failed_save_keeps_pending :
    keys (commit (history 1) 30 10 (removed 1)
      (.providerDeletion "carol-old" true (.ok ()) (absence "carol-old"))
      (.error .database)).1 = keys (removed 1) := rfl

def operation (key : String) : Nat :=
  if key = "carol-old" then 100 else if key = "carol-ana" then 101 else 102

theorem all_keys_visible : ∀ key ∈ ["carol-old", "carol-ana", "carol-other"],
    ⟨.operation (operation key), 0, .deleteAccessKey key⟩ ∈
      pendingRecords (removed 1) 0 operation := by decide

/-- Reading only the latest replay loses a previously scheduled deletion:
Ben removes Ana, then Ana's concurrent earlier removal of Ben defeats it. -/
def reversalLog : CurrentReplay.Log
  | 2 => entry 0 0 [0, 1] (.removeMember 1)
  | 3 => entry 1 1 [0, 1] (.removeMember 0)
  | e => log 1 e

def reversal : CurrentReplay.History := ⟨reversalLog, id, fun e => if e = 3 then 2 else e⟩
def firstRemoval := (applyEvent reversal 30 4 Journal.empty (.entries [0, 1, 3])).1
def reversed := (applyEvent reversal 30 4 firstRemoval (.entries [2])).1

theorem replay_reversal_retains_deletion :
    validCheck reversal.membership.log 4 = true ∧
    "ana" ∈ keys firstRemoval ∧ 3 ∈ reversed.result.dropped ∧
    member reversed.result.state 0 = true ∧ "ana" ∈ keys reversed := by decide

/-- Checked counterexample to deriving pending work only from current replay,
without the retained journal. No clearing event occurs in this history. -/
example :
    "ana" ∈ revokedCredentials reversal.log firstRemoval.received firstRemoval.result.state [] ∧
    "ana" ∉ revokedCredentials reversal.log reversed.received reversed.result.state [] ∧
    "ana" ∈ keys reversed := by decide

end CovenStorelogData.AccessKeyExamples
