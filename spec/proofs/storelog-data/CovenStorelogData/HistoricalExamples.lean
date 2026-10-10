import CovenStorelog.CurrentExamples
import CovenStorelogData.HistoricalKeys
import CovenStorelogData.Access
import CovenStorelogData.DeliveryExamples
import CovenStorelogData.CurrentData

/-! Ana removes Ben; Carol rotates Gifts in the next phase and writes.
Ben's earlier concurrent removal returns him; a running Carol shares K2.
Ben's receiving installation never applied its own removal (§10). -/
namespace CovenStorelogData.HistoricalExamples
open CovenStorelog
open CovenStorelog.CurrentExamples (key)
open KeySelection (receivedAuthorized)

def entry := CurrentReplay.Entry.mk

def log : CurrentReplay.Log
  | 0 => entry 0 0 [] (.create "ana" (key 0))
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol")
  | 3 => entry 1 1 (List.range 3) (.addDevice 1 1)
  | 4 => entry 2 2 (List.range 4) (.addDevice 2 2)
  | 5 => entry 0 0 (List.range 5) (.makeCircle 0 "Gifts" (key 1))
  | 6 => entry 0 0 (List.range 6) (.addToCircle 0 1)
  | 7 => entry 0 0 (List.range 7) (.addToCircle 0 2)
  | 8 => entry 1 1 (List.range 8) (.removeMember 0)
  | 9 => entry 0 0 (List.range 8) (.removeMember 1)
  | _ => entry 2 2 (List.range 8 ++ [9]) (.rotateKey (.circle 0) (key 2))

def history : CurrentReplay.History :=
  ⟨log, fun e => if e < 8 then e else if e = 8 then 11 else if e = 9 then 8 else 10,
    fun e => if e < 8 then e else if e = 10 then 9 else 8⟩
def before := List.range 8 ++ [9]
def afterRotation := before ++ [10]
def afterReturn := afterRotation ++ [8]
def result (received : List Nat) := CurrentReplay.resolve history 30 11 (entrySet received)
def oldKey : KeySelection.Key := ⟨5, .circle 0, key 1⟩
def k2 : KeySelection.Key := ⟨10, .circle 0, key 2⟩
def copies : KeySelection.Copies := [(oldKey, 0), (oldKey, 1), (oldKey, 2)]
def caught : KeyPhases.CaughtUp := ⟨before, ⟨[oldKey], copies⟩⟩
def rotated : KeyPhases.CaughtUp :=
  ⟨afterRotation, ⟨[k2, oldKey], copies ++ [(k2, 2), (k2, 0)]⟩⟩
def shared := rotated.observation.copies ++ [(k2, 1)]

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

theorem valid_history : Finality.Valid history.membership 11 := by
  constructor
  · exact validCheck_sound _ _ (by decide)
  · have checked : ∀ a b : Fin 11, history.stored a = history.stored b → a = b := by decide
    intro a b ha hb same
    exact congrArg Fin.val (checked ⟨a, ha⟩ ⟨b, hb⟩ same)
  · have checked : ∀ e : Fin 11, history.attempted e ≤ history.stored e := by decide
    intro e he; exact checked ⟨e, he⟩
  · have checked : ∀ w a : Fin 11, hadRead history.membership.log w a = true ↔
        history.stored a < history.attempted w := by decide
    intro w a hw ha; exact checked ⟨w, hw⟩ ⟨a, ha⟩

theorem rotation_in_second_phase :
    KeyPhases.begin [oldKey] (.ok before) (.ok copies) = .ok caught ∧
    KeySelection.introductions log 9 = [] ∧
    KeyPhases.firstSend history 30 11 2 (.circle 0) caught [42] = none ∧
    KeyPhases.rotate history 30 11 2 2 caught 10 (.ok ()) (.ok ()) = .ok rotated ∧
    KeyPhases.firstSend history 30 11 2 (.circle 0) rotated [42] = some ⟨k2, [42]⟩ ∧
    (k2, 1) ∉ rotated.observation.copies :=
  ⟨rfl, rfl, rfl, rfl, rfl, by decide⟩

theorem failed_rotation_waits :
    KeyPhases.rotate history 30 11 2 2 caught 10 (.error (.storage true)) (.ok ()) =
      .error (.storage true) ∧
    KeyPhases.rotate history 30 11 2 2 caught 10 (.ok ()) (.error (.storage true)) =
      .error (.storage true) := ⟨rfl, rfl⟩

theorem carol_revokes_ben (later : KeySelection.Copies) (noLaterCopy : (k2, 1) ∉ later) :
    (k2, 1) ∉ rotated.observation.copies ++ later :=
  (KeyPhases.revocation history 30 11 2 1 (.circle 0) rotated [42] ⟨k2, [42]⟩ later
    (by decide) (by decide) noLaterCopy).2

theorem removal_reversal_and_sharing :
    CausalOrder history.membership.log afterReturn ∧
    CausalOrder history.membership.log (List.range 11) ∧
    9 ∈ (result afterRotation).kept ∧ 9 ∈ (result afterReturn).dropped ∧
    inCircle (result afterRotation).state 0 1 = false ∧
    inCircle (result afterReturn).state 0 1 = true ∧
    running (result afterReturn).state 2 2 = true ∧
    KeySelection.share (audienceMembers (result afterReturn).state)
      rotated.observation.custody rotated.observation.copies 1 k2 = some shared ∧
    receivedAuthorized history 30 11 (entrySet afterReturn) k2 = true := by
  refine ⟨?_, ?_, by decide⟩
  all_goals apply causalCheck_sound _ .nil; decide

theorem ben_never_stopped :
    ([8, 9, 10].foldl (CurrentData.receive history 30 11 1 1)
      ⟨List.range 8, false⟩).stopped = false := by decide

def headers (_ : Fin 1) : Header (Fin 1) := ⟨0, [], .apply⟩
def carol : Author := ⟨2, 2, afterRotation⟩
def empty (received : List Nat) : HistoricalKeys.Reader (Fin 1) Unit :=
  ⟨received, ⟨CovenMerge.St.init, [], []⟩⟩
def applyPart (who : Nat) (custody : List KeySelection.Key)
    (reader : HistoricalKeys.Reader (Fin 1) Unit) :=
  HistoricalKeys.loadPart history 30 11 who custody k2 DeliveryExamples.writes headers []
    carol reader 0

def value (reader : HistoricalKeys.Reader (Fin 1) Unit) :=
  reader.data.data.cell DeliveryExamples.row ()

def snapshot : Snapshot (Fin 1) Unit :=
  ⟨CovenMerge.step DeliveryExamples.writes CovenMerge.St.init 0, [0], [], []⟩

def benReads := (HistoricalKeys.acquire history 30 11 (entrySet afterReturn) [] shared 1 k2).bind
  (fun held => applyPart 1 held (empty afterReturn))

/-- The exact same encrypted part supplies the original value, without a
replacement write. Replay only changes its membership inputs. -/
theorem historical_parts_apply :
    (applyPart 2 [k2] (empty afterRotation)).map value = .ok (some 0) ∧
    (applyPart 2 [k2] (empty afterRotation)).map
      (fun reader => value (HistoricalKeys.replay reader afterReturn)) = .ok (some 0) ∧
    applyPart 1 [] (empty afterReturn) = .error (.keyUnavailable (.circle 0) 2) ∧
    (k2, 1) ∈ shared ∧
    benReads.map value = .ok (some 0) := by
  exact ⟨rfl, rfl, rfl, by decide, rfl⟩

def shown (reader : HistoricalKeys.Reader (Fin 1) Unit) : Bool :=
  (CurrentData.observe DeliveryExamples.schema DeliveryExamples.writes history 30 11
    reader.received reader.data.data).view.shown DeliveryExamples.row

theorem membership_keeps_part_visible :
    (applyPart 2 [k2] (empty afterRotation)).map shown = .ok true ∧
    (applyPart 2 [k2] (empty afterRotation)).map
      (fun reader => shown (HistoricalKeys.replay reader afterReturn)) = .ok true ∧
    benReads.map shown = .ok true := ⟨rfl, rfl, rfl⟩

theorem historical_snapshot_loads :
    (HistoricalKeys.loadSnapshot history 30 11 2 (entrySet afterRotation) [k2] k2 snapshot).map
      (fun loaded => loaded.data.cell DeliveryExamples.row ()) = .ok (some 0) ∧
    (HistoricalKeys.loadSnapshot history 30 11 1 (entrySet afterReturn) [k2] k2 snapshot).map
      (fun loaded => loaded.data.cell DeliveryExamples.row ()) = .ok (some 0) := ⟨rfl, rfl⟩

/-- Replay records the job before the removal call returns. Provider success
is a later event, with an owner wait on other devices in the meantime. -/
def accessBefore : Access.Applied :=
  ⟨List.range 8, result (List.range 8), ⟨"ben", true, true, none⟩⟩
def accessRemoved := (Access.applyEntries history 30 11 [] accessBefore before (.ok ())).1

theorem removal_access_one_path :
    Access.removalCall accessRemoved 9 = some () ∧
    accessRemoved.account.actual = true ∧ accessRemoved.account.request = some (.ready false) ∧
    Pending.records [Access.ownerWork false 2 100 accessRemoved.account] =
      [⟨.operation 100, 2, .pendingOwner "ben"⟩] ∧
    Pending.records [Access.ownerWork true 0 100 accessRemoved.account] =
      [⟨.operation 100, 0, .providerPending .revoke "ben"⟩] ∧
    ((Access.start accessRemoved.account).bind (fun sending => Access.complete sending (.ok ()))).map
      (fun finished => Pending.records [Access.work 0 100 finished]) = some [] := ⟨rfl, rfl, rfl, rfl, rfl, rfl⟩

def reversalDuringRevoke := (Access.start accessRemoved.account).map fun account =>
  (Access.applyEntries history 30 11 [] { accessRemoved with account } afterReturn (.ok ())).1

theorem replay_reversal_queues_regrant :
    reversalDuringRevoke.map (fun s => s.account.request) = some (some (.sending false)) ∧
    (reversalDuringRevoke.bind fun s => Access.complete s.account (.ok ())) =
      some ⟨"ben", true, false, some (.ready true)⟩ := by decide

/-- A later, dropped access entry still contributes its distinct S3 key. -/
def accessLog : CurrentReplay.Log
  | 11 => entry 1 1 (List.range 8) (.setAccess 1 "ben-new")
  | e => log e

def accessHistory : CurrentReplay.History :=
  ⟨accessLog, fun e => if e = 11 then 12 else history.stored e,
    fun e => if e = 11 then 8 else history.attempted e⟩
def accessApplied :=
  (Access.applyEntries accessHistory 30 12 [] accessRemoved (afterRotation ++ [11]) (.ok ())).1
def accessResult := accessApplied.result

theorem dropped_access_stays_pending :
    11 ∈ accessResult.dropped ∧
    Access.revokedCredentials accessHistory.log accessApplied.received accessResult.state [] =
      ["ben", "ben-new"] ∧
    Access.revokedCredentials accessHistory.log accessApplied.received accessResult.state
      ["ben"] = ["ben-new"] ∧
    Pending.Record.mk (.operation 101) 0 (.deleteAccessKey "ben-new") ∈
      Access.credentialRecords 0 (fun key => if key = "ben" then 100 else 101)
        accessHistory.log accessApplied.received accessApplied.result.state ["ben"] := by decide

end CovenStorelogData.HistoricalExamples
