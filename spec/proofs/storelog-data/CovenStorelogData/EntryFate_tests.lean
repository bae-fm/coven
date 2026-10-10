import CovenStorelogData.EntryFateLive
import CovenStorelogData.EntryExamples
import CovenStorelogData.RetentionExamples

namespace CovenStorelogData.EntryFate.Tests

open CovenStorelog
open CovenStorelog.Examples (entry)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

def resources : Resources (Fin 1) Unit :=
  ⟨[⟨DeliveryExamples.row, .owned, 0⟩], [DeliveryExamples.row], [0], [], [0], [1]⟩

/-- One circle row, Ben's deletion and Ana's earlier concurrent removal of
Ben. Unlike the implemented operation, the alternative authors no row delete.
The retained child and owned file therefore return with the row. -/
def fileState (order : List (Event (Fin 1))) :=
  order.foldl (CovenStorelogData.step DeliveryExamples.writes EntryExamples.fileLog 8)
    (EntryExamples.fileStart .owned).state

def fileView (order : List (Event (Fin 1))) :=
  view DeliveryExamples.schema DeliveryExamples.writes EntryExamples.fileLog 8 resources
    (fileState order)

theorem entry_only_restores :
    (fileView [.entry 7]).data.view.shown DeliveryExamples.row = false ∧
    ((fileView [.entry 7]).data.losses DeliveryExamples.row none).isSome = true ∧
    (fileView [.entry 7]).sources = resources.sources ∧
    (fileView [.entry 7]).children = [] ∧
    (fileView [.entry 7, .entry 6]).data.view.shown DeliveryExamples.row = true ∧
    (fileView [.entry 7, .entry 6]).data.losses DeliveryExamples.row none = none ∧
    (fileView [.entry 7, .entry 6]).children = resources.children ∧
    (fileView [.entry 7, .entry 6]).uploaded = [0] := by decide

/-- Compare both arrivals to erasing the dropped deletion's *effects*, while
retaining the same ordinary write and local inputs. -/
example :
    (fileView [.entry 7, .entry 6]).children = (fileView [.entry 6]).children ∧
    (fileView [.entry 7, .entry 6]).sources = (fileView [.entry 6, .entry 7]).sources ∧
    (fileView [.entry 7, .entry 6]).data.view.shown DeliveryExamples.row =
      (fileView [.entry 6, .entry 7]).data.view.shown DeliveryExamples.row := by decide

/-- Current grants can be requested both ways, including re-granting Carol
when Ben's removal loses. The provider does not participate in replay. -/
theorem access_requests_reverse :
    requestAccess (EntryExamples.accessRun false [6]).1.result 2 = ⟨false⟩ ∧
    requestAccess (EntryExamples.accessRun false [6, 5]).1.result 2 = ⟨true⟩ := by decide

/-- Just one removal and a second device that has not received it suffice:
no actual provider grant can equal both devices' current replay at once. -/
theorem provider_fate_counterexample :
    ¬ ∃ actual : Bool,
      actual = member EntryExamples.accessStart.result.state 2 ∧
      actual = member (EntryExamples.accessRun false [6]).1.result.state 2 := by
  apply incompatible_access
  decide

/-- Ben's removal queues revoke; Ana's earlier removal arrives and queues
grant. Grant completes, then the older revoke completes successfully. Even
with identical entries the last provider reply leaves Carol excluded. -/
theorem stale_provider_request_counterexample :
    let before := (EntryExamples.accessRun false [6]).1.result
    let after := (EntryExamples.accessRun false [6, 5]).1.result
    completeAccess (requestAccess after 2) = true ∧
    completeAccess (requestAccess before 2) = false ∧ member after.state 2 = true := by decide

def resetFirst := resolve EntryExamples.resetLog 4 (entrySet [0, 1, 3])
def resetFinal := resolve EntryExamples.resetLog 4 (entrySet (List.range 4))

/-- Suppression replaces reset's irreversible clearing; the permanent failed
object is still known when the reset loses. Peer reports are independent. -/
theorem reports_return :
    reports EntryExamples.resetLog 4 resetFirst [0] = [] ∧
    reports EntryExamples.resetLog 4 resetFinal [0] = [0] := by decide

def observed (order : List (Event (Fin 1))) : AppValue :=
  ⟨((fileView order).data.losses DeliveryExamples.row none).isSome,
    (fileView order).localReports, false⟩

/-- Two competing entries and one delivered callback. After the drop the
current query agrees, but the app cannot unlearn the temporary lost value. -/
theorem notification_fate_counterexample :
    observed [.entry 7, .entry 6] = observed [.entry 6, .entry 7] ∧
    tell (tell [] (observed [.entry 7])) (observed [.entry 7, .entry 6]) ≠
      tell (tell [] (observed [.entry 6])) (observed [.entry 6, .entry 7]) := by decide

example :
    tell (tell [] ⟨false, reports EntryExamples.resetLog 4 resetFirst [0], false⟩)
      ⟨false, reports EntryExamples.resetLog 4 resetFinal [0], false⟩ ≠
    tell (tell [] ⟨false, reports EntryExamples.resetLog 4 resetFinal [0], false⟩)
      ⟨false, reports EntryExamples.resetLog 4 resetFinal [0], false⟩ := by decide

/-- Offering a lost row elsewhere is an ordinary app write. The model does
not retract it when the original returns. This is allowed by §8/E4 (rule
losses may return), not a false claim that every LostValue is permanent. -/
def restoredElsewhere : Row := ⟨0, 1, .store⟩
def copyRow (w : Fin 2) : Row := if w == 0 then DeliveryExamples.row else restoredElsewhere
def copyWrites : CovenMerge.Writes (Fin 2) Row Unit where
  ts w := w.val
  past w a := w == 1 && a == 0
  chg w r := if r = copyRow w
    then some ⟨.ins, 0, fun _ => true⟩ else none

theorem copy_writes_valid : CovenMerge.Valid copyWrites where
  ts_inj _ _ h := Fin.eq_of_val_eq h
  past_ts := by decide
  gen_seen w r ch h := by
    simp only [copyWrites] at h
    split at h <;> cases h
    exact Or.inl rfl
  parity w r ch h := by
    simp only [copyWrites] at h
    split at h <;> cases h
    decide

def copySchema : Schema (Fin 2) Unit Nat :=
  ⟨[DeliveryExamples.row, restoredElsewhere], fun _ _ => [], fun _ _ => false, fun _ _ => []⟩
def copied : CovenStorelogData.State (Fin 2) Unit :=
  ⟨(fileState [.entry 7, .entry 6]).log,
    [0, 1].foldl (CovenMerge.step copyWrites) CovenMerge.St.init⟩

theorem restore_duplicates :
    writeAuthority EntryExamples.fileLog 8 ⟨0, 2, List.range 6 ++ [7]⟩ = true ∧
    (observe copySchema copyWrites EntryExamples.fileLog copied).view.shown DeliveryExamples.row = true ∧
    (observe copySchema copyWrites EntryExamples.fileLog copied).view.shown restoredElsewhere = true := by decide

/-- A reset and an earlier concurrent raise, or a raise and an earlier reset,
can reverse an exclusion. This variant needs one old-schema write. Both admins
start online from the same prefix; their calls overlap. -/
def boundaryLog : Log
  | 0 => entry 0 0 [] (.create "ana")
  | 1 => entry 0 0 [0] (.addDevice 0 1)
  | 2 => entry 0 0 [0, 1] (.reset ⟨.store, 0⟩)
  | _ => entry 0 1 [0, 1] (.raiseSchema 2 ⟨.store, 1⟩)

theorem boundaries_valid : Valid boundaryLog 4 ∧
    CausalOrder boundaryLog [0, 1, 3, 2] ∧ CausalOrder boundaryLog [0, 1, 2, 3] := by
  refine ⟨validCheck_sound _ _ (by decide), ?_, ?_⟩
  all_goals apply causalCheck_sound _ .nil; decide

def emptySnapshot : Snapshot (Fin 4) (Fin 2) := ⟨CovenMerge.St.init, [], [], []⟩
def boundaryData (entries : List Nat) :=
  rebuild MigrationExamples.writes MigrationExamples.headers boundaryLog 4
    (resolve boundaryLog 4 (entrySet entries)) .store (fun _ => emptySnapshot) emptySnapshot [0]

theorem schema_loss_returns :
    3 ∈ (resolve boundaryLog 4 (entrySet [0, 1, 3])).kept ∧
    (boundaryData [0, 1, 3]).rejected = [(0, .schema 2)] ∧
    (boundaryData [0, 1, 3]).data.cell MigrationExamples.note 0 = none ∧
    3 ∈ (resolve boundaryLog 4 (entrySet [0, 1, 3, 2])).dropped ∧
    (boundaryData [0, 1, 3, 2]).rejected = [] ∧
    (boundaryData [0, 1, 3, 2]).data.cell MigrationExamples.note 0 = some 0 := by decide

/-- Reversible exclusion satisfies entry fate but not an unqualified promise
that an already excluded old-schema value is never applied. -/
theorem permanent_schema_loss_counterexample :
    ¬ PermanentLoss [⟨!(boundaryData [0, 1, 3]).rejected.isEmpty, [], false⟩,
      ⟨!(boundaryData [0, 1, 3, 2]).rejected.isEmpty, [], false⟩] := by
  change ¬ PermanentLoss [⟨true, [], false⟩, ⟨false, [], false⟩]
  simp [PermanentLoss]

/-- These derived exclusions cannot implement an API promising permanence
for every delivered history. -/
example : ¬ ∀ trace : List AppValue, PermanentLoss trace := by
  intro promised
  exact permanent_schema_loss_counterexample (promised _)

/-- A local migration freezes a transient deleted-circle loss before its
store raise. Keeping only that frozen result is not enough: the deletion can
lose while the store raise stays kept. No circle snapshot publication is
assumed while the circle is deleted. -/
def freezeLog : Log
  | 8 => entry 0 2 (List.range 6 ++ [7]) (.raiseSchema 2 ⟨.store, 0⟩)
  | n => EntryExamples.fileLog n

theorem freeze_valid : Valid freezeLog 9 ∧ CausalOrder freezeLog (List.range 6 ++ [7, 8, 6]) := by
  constructor
  · exact validCheck_sound _ _ (by decide)
  · apply causalCheck_sound _ .nil; decide

theorem frozen_dependency_counterexample :
    let final := resolve freezeLog 9 (entrySet (List.range 9))
    7 ∈ final.dropped ∧ 8 ∈ final.kept ∧
    deletedCircle freezeLog final 0 = false ∧
    RetentionExamples.frozenFile.frozen.isEmpty = false ∧
    RetentionExamples.frozenFile.data.cell DeliveryExamples.row () = none := by decide

/-- The retained-input alternative recomputes the row's rule even across an
identity-preserving migration; it never feeds the last hidden view back as its
input. General SQL transformations require retained versioned inputs too. -/
example :
    (fileView [.entry 7, .entry 6]).data.view.shown DeliveryExamples.row ≠
      decide (RetentionExamples.frozenFile.data.gen DeliveryExamples.row % 2 = 1) := by decide

def memberLog : Log
  | 0 => entry 0 0 [] (.create "S3")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "S3")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "S3")
  | 3 => entry 1 1 (List.range 3) (.addDevice 1 1)
  | 4 => entry 2 2 (List.range 4) (.addDevice 2 2)
  | 5 => entry 0 0 (List.range 5) (.removeMember 1 [])
  | _ => entry 1 1 (List.range 5) (.removeMember 2 [])

def memberStopped : Device :=
  ⟨entrySet (List.range 5 ++ [6]), resolve memberLog 7 (entrySet (List.range 5 ++ [6]))⟩

def deviceLog : Log
  | 0 => entry 0 0 [] (.create "S3")
  | 1 => entry 0 0 [0] (.addDevice 0 1)
  | 2 => entry 0 0 [0, 1] (.addDevice 0 2)
  | 3 => entry 0 0 (List.range 3) (.removeDevice 0 1)
  | _ => entry 0 1 (List.range 3) (.removeDevice 0 2)

def deviceStopped : Device :=
  ⟨entrySet [0, 1, 2, 4], resolve deviceLog 5 (entrySet [0, 1, 2, 4])⟩

theorem removals_valid : Valid memberLog 7 ∧ Valid deviceLog 5 ∧
    CausalOrder memberLog (List.range 5 ++ [6, 5]) ∧
    CausalOrder deviceLog [0, 1, 2, 4, 3] := by
  refine ⟨validCheck_sound _ _ (by decide), validCheck_sound _ _ (by decide), ?_, ?_⟩
  all_goals apply causalCheck_sound _ .nil; decide

theorem removal_drops :
    running memberStopped.result.state 2 2 = false ∧
    running (CovenStorelog.step memberLog 7 memberStopped 5).result.state 2 2 = true ∧
    6 ∈ (CovenStorelog.step memberLog 7 memberStopped 5).result.dropped ∧
    running deviceStopped.result.state 0 2 = false ∧
    running (CovenStorelog.step deviceLog 5 deviceStopped 3).result.state 0 2 = true ∧
    4 ∈ (CovenStorelog.step deviceLog 5 deviceStopped 3).result.dropped := by decide

/-- Every future poll can succeed at storage, yet the stop gate never reads
the defeating entry. These are infinite stuttering extensions, not a scheduler
that merely postpones a needed event for a finite time. -/
theorem member_never_resumes (arrivals : List Nat) :
    arrivals.foldl (poll .stopAll memberLog 7 2 2 true) memberStopped = memberStopped :=
  stopped_forever _ _ _ _ _ (by decide) arrivals

theorem device_never_resumes (arrivals : List Nat) :
    arrivals.foldl (poll .stopAll deviceLog 5 0 2 true) deviceStopped = deviceStopped :=
  stopped_forever _ _ _ _ _ (by decide) arrivals

theorem observing_resumes :
    running (poll .observeEntries memberLog 7 2 2 true memberStopped 5).result.state 2 2 = true ∧
    running (poll .observeEntries deviceLog 5 0 2 true deviceStopped 3).result.state 0 2 = true := by decide

/-- E5's current removal status can change when evidence arrives, but an
already delivered Removed result remains part of what the app was told. -/
theorem removal_status_returns :
    removalStatus 2 2 (poll .stopAll memberLog 7 2 2 true memberStopped 5) = true ∧
    removalStatus 2 2 (poll .observeEntries memberLog 7 2 2 true memberStopped 5) = false ∧
    tell [⟨false, [], removalStatus 2 2 memberStopped⟩]
      ⟨false, [], removalStatus 2 2 (poll .observeEntries memberLog 7 2 2 true memberStopped 5)⟩ =
      [⟨false, [], true⟩, ⟨false, [], false⟩] := by decide

end CovenStorelogData.EntryFate.Tests
