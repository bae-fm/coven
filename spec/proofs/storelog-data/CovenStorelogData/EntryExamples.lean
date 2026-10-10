import CovenStorelogData.EntryEffects
import CovenStorelogData.CircleExamples
import CovenStorelogData.DeliveryExamples
import CovenStorelogData.MigrationExamples

namespace CovenStorelogData.EntryExamples

open CovenStorelog
open CovenStorelog.Examples (entry)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- A row deleted by the operation's ordinary write is neither restored nor
lost after its entry drops. Equal ordinary writes do still converge. -/
example :
    let actual := CircleExamples.afterDrop
    let absent := CircleExamples.state (List.range 7) [0, 1]
    actual.log.result = absent.log.result ∧
    actual.data.cell (CircleExamples.note 7) () = none ∧
    actual.data.lost (CircleExamples.note 7) () 0 = none ∧
    absent.data.cell (CircleExamples.note 7) () = some 0 := by decide

/-- Ana has two devices. Her phone's earlier removal beats Ben's deletion;
her tablet can receive those two concurrent entries in either order. -/
def fileLog : Log
  | 0 => entry 0 0 [] (.create "ana")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 0 0 (List.range 3) (.addDevice 0 2)
  | 4 => entry 0 0 (List.range 4) (.makeCircle 0 "Gifts")
  | 5 => entry 0 0 (List.range 5) (.addToCircle 0 1)
  | 6 => entry 0 0 (List.range 6) (.removeFromCircle 0 1)
  | _ => entry 1 1 (List.range 6) (.deleteCircle 0)

theorem files_valid : Valid fileLog 8 ∧
    CausalOrder fileLog (List.range 6 ++ [7, 6]) ∧
    CausalOrder fileLog (List.range 8) := by
  refine ⟨validCheck_sound _ _ (by decide), ?_, ?_⟩
  all_goals apply causalCheck_sound _ .nil; decide

/-- Ana's attached file was not on Ben's deleting device. Both histories
have the same write before the two entries; only one sees Gifts deleted. -/
def fileStart (source : Source) : FileDevice (Fin 1) Unit :=
  ⟨⟨⟨entrySet (List.range 6), resolve fileLog 8 (entrySet (List.range 6))⟩,
      CovenMerge.step DeliveryExamples.writes CovenMerge.St.init 0⟩,
   [⟨DeliveryExamples.row, source, 0⟩]⟩

def fileRun (source : Source) (order : List Nat) : FileDevice (Fin 1) Unit :=
  order.foldl (fileEntry DeliveryExamples.writes fileLog 8) (fileStart source)

theorem file_fate_counterexample :
    ∀ source, (fileRun source [7, 6]).files = [] ∧
      (fileRun source [6, 7]).files = [⟨DeliveryExamples.row, source, 0⟩] ∧
      (fileRun source [7, 6]).state.log.result = (fileRun source [6, 7]).state.log.result ∧
      (observe DeliveryExamples.schema DeliveryExamples.writes fileLog
        (fileRun source [7, 6]).state).view.shown DeliveryExamples.row = true := by
  intro source
  cases source <;> decide

/-- The row and its file name return, but the owned bytes do not. An original
file's bytes survive outside coven; its lost registration does not return. -/
example : (fileRun .owned [7, 6]).files ≠ (fileRun .owned [6, 7]).files := by decide

theorem local_cascade_counterexample :
    let before := (fileRun .owned []).state
    let deleted := (fileRun .owned [7]).state
    let restored := (fileRun .owned [7, 6]).state
    let children := [DeliveryExamples.row]
    cascadeLocal DeliveryExamples.schema DeliveryExamples.writes fileLog before
      children = children ∧
    cascadeLocal DeliveryExamples.schema DeliveryExamples.writes fileLog restored
      (cascadeLocal DeliveryExamples.schema DeliveryExamples.writes fileLog deleted
        children) = [] ∧
    cascadeLocal DeliveryExamples.schema DeliveryExamples.writes fileLog restored
      children = children := by decide

/-- Ana's phone removes Ben, concurrently with Ben removing Carol. Ana's
tablet can revoke Carol first; Ana remains owner throughout both histories. -/
def accessLog : Log
  | 0 => entry 0 0 [] (.create "ana")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol")
  | 3 => entry 1 1 [0, 1, 2] (.addDevice 1 1)
  | 4 => entry 0 0 (List.range 4) (.addDevice 0 3)
  | 5 => entry 0 0 (List.range 5) (.removeMember 1 [])
  | _ => entry 1 1 (List.range 5) (.removeMember 2 [])

def accessStart : Device :=
  ⟨entrySet (List.range 5), resolve accessLog 7 (entrySet (List.range 5))⟩

def accessRun (s3 : Bool) (order : List Nat) : Device × AccessState :=
  order.foldl (fun (d, s) e => accessEntry s3 accessLog 7 d s e)
    (accessStart, ⟨["ana", "ben", "carol"], [], []⟩)

theorem access_valid : Valid accessLog 7 ∧
    CausalOrder accessLog (List.range 5 ++ [6, 5]) ∧
    CausalOrder accessLog (List.range 7) := by
  refine ⟨validCheck_sound _ _ (by decide), ?_, ?_⟩
  all_goals apply causalCheck_sound _ .nil; decide

theorem access_fate_counterexample :
    (accessRun false [6, 5]).1.result = (accessRun false [5, 6]).1.result ∧
    member (accessRun false [6, 5]).1.result.state 2 = true ∧
    (accessRun false [6, 5]).2.grants = ["ana"] ∧
    (accessRun false [5, 6]).2.grants = ["ana", "carol"] ∧
    (accessRun true [6, 5]).2.notices = ["carol", "ben"] ∧
    (accessRun true [5, 6]).2.notices = ["ben"] := by decide

example : (accessRun false [6, 5]).2 ≠ (accessRun false [5, 6]).2 := by decide

/-- Confirming the console deletion cannot be undone by dropping its cause.
Recording it again also cannot resurrect the notice or the deleted credential. -/
example :
    let s := confirmAccess "carol" (accessRun true [6]).2
    let after := accessEntry true accessLog 7 (accessRun true [6]).1 s 5
    "carol" ∉ after.2.grants ∧ "carol" ∉ after.2.notices ∧
    "carol" ∈ after.2.confirmed := by decide

/-- Two admins and Carol. The demotion makes Ben's removal unsafe; Ana's
later removal, previously beaten by Ben's, becomes effective again. -/
def keyLog : Log
  | 0 => entry 0 0 [] (.create "ana")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol")
  | 3 => entry 1 1 [0, 1, 2] (.addDevice 1 1)
  | 4 => entry 2 2 (List.range 4) (.addDevice 2 2)
  | 5 => entry 0 0 (List.range 5) (.addDevice 0 3)
  | 6 => entry 0 3 (List.range 6) (.changeRole 1 .member)
  | 7 => entry 1 1 (List.range 6) (.removeMember 0 [])
  | _ => entry 0 0 (List.range 6) (.removeMember 2 [])

def keyResult (tail : List Nat) := resolve keyLog 9 (entrySet (List.range 6 ++ tail))
theorem keys_valid : Valid keyLog 9 ∧
    CausalOrder keyLog (List.range 6 ++ [8, 7, 6]) ∧
    CausalOrder keyLog (List.range 9) := by
  refine ⟨validCheck_sound _ _ (by decide), ?_, ?_⟩
  all_goals apply causalCheck_sound _ .nil; decide

/-- Adding Carol to Gifts uploads its history key before the entry; Ben's
concurrent removal of Ana beats the addition. Carol remains a store member,
can fetch the already-sealed history, and is not in Gifts. -/
def additionLog : Log
  | 0 => entry 0 0 [] (.create "ana")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol")
  | 3 => entry 1 1 [0, 1, 2] (.addDevice 1 1)
  | 4 => entry 2 2 (List.range 4) (.addDevice 2 2)
  | 5 => entry 0 0 (List.range 5) (.makeCircle 0 "Gifts")
  | 6 => entry 0 0 (List.range 6) (.addToCircle 0 1)
  | 7 => entry 1 1 (List.range 7) (.removeFromCircle 0 0)
  | _ => entry 0 0 (List.range 7) (.addToCircle 0 2)

def additionResult := resolve additionLog 9 (entrySet (List.range 9))
def additionCopies := shareAddition additionLog 8 (introductionCopies additionLog (List.range 9))

theorem addition_valid : Valid additionLog 9 := validCheck_sound _ _ (by decide)

theorem addition_key_counterexample :
    8 ∈ additionResult.dropped ∧ inCircle additionResult.state 0 2 = false ∧
    (additionCopies.map (fun copies =>
      decide (Key.mk 5 (.circle 0) ∈ acquireKeys additionLog additionResult copies 2 2 []))) =
      some true := by decide

example : (additionCopies.map (fun copies =>
    part additionLog additionResult copies ⟨5, .circle 0⟩ 2)) = some .apply := by decide

/-- Both operations publish secrets before their entries. Ben's earlier
removal of Ana defeats her circle creation or her approval of Carol. -/
def introductionLog (action : Action) : Log
  | 0 => entry 0 0 [] (.create "ana")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 1 1 (List.range 3) (.removeMember 0 [])
  | _ => entry 0 0 (List.range 3) action

def creationLog := introductionLog (.makeCircle 0 "Gifts")
def invitationLog := introductionLog (.addMember 2 .member "carol")

theorem introductions_valid : Valid creationLog 5 ∧ Valid invitationLog 5 := by
  constructor <;> apply validCheck_sound <;> decide

theorem creation_key_counterexample :
    let first := resolve creationLog 5 (entrySet [0, 1, 2, 4])
    let final := resolve creationLog 5 (entrySet (List.range 5))
    let copies := introductionCopies creationLog (List.range 5)
    4 ∈ first.kept ∧ 4 ∈ final.dropped ∧
    lookup final.state.circles 0 = none ∧
    Key.mk 4 (.circle 0) ∈ acquireKeys creationLog final copies 0 0
      (acquireKeys creationLog first copies 0 0 []) := by decide

/-- Even when bootstrap never accepts Carol's membership, the copy already
sealed to her private key remains readable. This tests disclosure, not whether
an honest stopped client will finish installing its credentials. -/
theorem invitation_key_counterexample :
    let final := resolve invitationLog 5 (entrySet (List.range 5))
    let shared := shareAddition invitationLog 4
      (introductionCopies invitationLog (List.range 5))
    4 ∈ final.dropped ∧ member final.state 2 = false ∧
    (shared.map (fun copies => decide ((Key.mk 0 .store, 2) ∈ copies))) = some true := by decide

/-- load_new_device registers an install before loading its snapshot. Ben's
concurrent removal of Ana can later defeat that registration; check_stopped
does not erase the installed rows or the member's already opened keys. -/
def registrationLog : Log
  | 4 => entry 0 2 (List.range 3) (.addDevice 0 2)
  | n => introductionLog (.addDevice 0 2) n

theorem registration_valid : Valid registrationLog 5 := validCheck_sound _ _ (by decide)

theorem registration_fate_counterexample :
    let first := resolve registrationLog 5 (entrySet [0, 1, 2, 4])
    let installed : State (Fin 4) (Fin 2) :=
      ⟨⟨entrySet [0, 1, 2, 4], first⟩,
        CovenMerge.step MigrationExamples.writes CovenMerge.St.init 0⟩
    let after := step MigrationExamples.writes registrationLog 5 installed (.entry 3)
    4 ∈ first.kept ∧ 4 ∈ after.log.result.dropped ∧
    lookup after.log.result.state.devices 2 = none ∧
    after.data.cell MigrationExamples.note 0 = some 0 := by decide

/-- Two devices replace the same member's access concurrently. The later
entry can finish locally before learning of the earlier winner. -/
def credentialLog : Log
  | 0 => entry 0 0 [] (.create "original")
  | 1 => entry 0 0 [0] (.addDevice 0 1)
  | 2 => entry 0 0 [0, 1] (.setAccess 0 "earlier")
  | _ => entry 0 1 [0, 1] (.setAccess 0 "later")

theorem credentials_valid : Valid credentialLog 4 := validCheck_sound _ _ (by decide)

theorem credential_fate_counterexample :
    let installed : CredentialDevice :=
      ⟨⟨entrySet [0, 1], resolve credentialLog 4 (entrySet [0, 1])⟩, "later"⟩
    let kept := credentialEntry credentialLog 4 installed 3
    let dropped := credentialEntry credentialLog 4 kept 2
    3 ∈ kept.log.result.kept ∧ 3 ∈ dropped.log.result.dropped ∧
    lookup dropped.log.result.state.access 0 = some "earlier" ∧
    dropped.credential = "later" := by decide

/-- An old-app reset clears a local stuck report before a concurrent raise
arrives. Raising the app and reloading the winning snapshot does not put that
report back. A device which first sees the raise never applies the reset. -/
def resetLog : Log
  | 2 => entry 0 0 [0, 1] (.raiseSchema 2 ⟨.store, 0⟩)
  | 3 => entry 0 1 [0, 1] (.reset ⟨.store, 1⟩)
  | n => credentialLog n

theorem resets_valid : Valid resetLog 4 := validCheck_sound _ _ (by decide)

theorem reset_judgments_counterexample :
    let first := resolve resetLog 4 (entrySet [0, 1, 3])
    let final := resolve resetLog 4 (entrySet (List.range 4))
    let reports : ResetJudgments := ⟨[], [0], [1]⟩
    let applied := reloadJudgments resetLog 4 final (reloadJudgments resetLog 4 first reports)
    let skipped := reloadJudgments resetLog 4 final reports
    3 ∈ first.kept ∧ 3 ∈ final.dropped ∧
    applied.localReports = [] ∧ skipped.localReports = [0] ∧
    applied.peerReports = skipped.peerReports := by decide

example :
    let reports : ResetJudgments := ⟨[], [0], []⟩
    let first := resolve resetLog 4 (entrySet [0, 1, 3])
    let final := resolve resetLog 4 (entrySet (List.range 4))
    reloadJudgments resetLog 4 final (reloadJudgments resetLog 4 first reports) ≠
      reloadJudgments resetLog 4 final reports := by decide

end CovenStorelogData.EntryExamples
