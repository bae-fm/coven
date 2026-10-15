import CovenQueue.Operations

namespace CovenQueue.Operations.Examples

def codec : Codec where
  entry r := [r.audience, r.key, r.keyHash, r.sealingKey] ++ r.entryPlaintext
  openEntry
    | audience :: key :: hash :: _ => some ⟨audience, key, hash⟩
    | _ => none
  entry_roundtrip _ := rfl
  openSnapshot key bytes := if key = 7 ∧ bytes = [70, 80] then some [10, 20] else none

def rotation : Rotation := ⟨⟨0, 2, 22⟩, [22], [⟨1, 0, [200]⟩, ⟨2, 1, [201]⟩], 1, [9], 2⟩
def snapshot : Snapshot := ⟨1, 7, [70, 80], [10, 20]⟩
def kept : Kept := fun _ => True

def staged (d : Data) : State :=
  { initial 1 d with memory := ⟨some d.localBytes, none⟩ }

def durable (d : Data) : State := { staged d with retained := some d.localBytes }

def prepared (d : Data) : State :=
  { initial 1 d with journal := .pending ⟨1, d, 1⟩, retained := some d.localBytes }

theorem prepared_reachable (d : Data) (check : d.localChecked codec) (accepted : Kept) :
    Reachable codec accepted 1 d (prepared d) :=
  .step (.step (.step .initial (.stageLocal _ ⟨1, d, 0⟩ rfl rfl))
    (.persistLocal (staged d) ⟨1, d, 0⟩ rfl rfl rfl))
    (.commitLocal (durable d) ⟨1, d, 0⟩ rfl rfl rfl check)

theorem crash_between_artifact_and_journal (d : Data) (check : d.localChecked codec) :
    Reachable codec kept 1 d (prepared d) ∧
    (durable d).crash.journal.row = some ⟨1, d, 0⟩ ∧
    (durable d).crash.retained = some d.localBytes ∧
    (staged d).crash.retained = none := by
  have persisted : Reachable codec kept 1 d (durable d) :=
    .step (.step .initial (.stageLocal _ ⟨1, d, 0⟩ rfl rfl))
      (.persistLocal _ ⟨1, d, 0⟩ rfl rfl rfl)
  refine ⟨.step (.step persisted (.crash _))
    (.commitLocal _ ⟨1, d, 0⟩ rfl rfl rfl check), rfl, rfl, rfl⟩

/-- These states are the actual targets of the transition relation below. -/
def copyObject : Object := ⟨.copy 1, [200]⟩
def secondCopyObject : Object := ⟨.copy 2, [201]⟩
def entryObject : Object := ⟨.entry 1, codec.entry rotation⟩
def copySent : State := { prepared (.rotation rotation) with sent := [(0, copyObject)] }
def copyStored : State := { copySent.crash with stored := create copySent.stored copyObject }
def copyReply : State := { copyStored with memory := ⟨none, some copyObject⟩ }
def firstCopyConfirmed : State :=
  { copyStored with journal := .pending ⟨1, .rotation rotation, 2⟩ }
def secondCopySent : State := { firstCopyConfirmed with sent := (1, secondCopyObject) :: firstCopyConfirmed.sent }
def secondCopyStored : State := { secondCopySent with stored := create secondCopySent.stored secondCopyObject }
def secondCopyReply : State := { secondCopyStored with memory := ⟨none, some secondCopyObject⟩ }
def copiesConfirmed : State :=
  { secondCopyStored with journal := .pending ⟨1, .rotation rotation, 3⟩ }
def entrySent : State := { copiesConfirmed with sent := (2, entryObject) :: copiesConfirmed.sent }
def entryStored : State := { entrySent with stored := create entrySent.stored entryObject }
def entryReply : State := { entryStored.crash with memory := ⟨none, some entryObject⟩ }
def publicationConfirmed : State :=
  { entryStored.crash with journal := .pending ⟨1, .rotation rotation, 4⟩ }
def rotationFinished : State :=
  { publicationConfirmed with journal := .finished 1 (.rotation rotation) }

theorem copy_stored_reachable (accepted : Kept) :
    Reachable codec accepted 1 (.rotation rotation) copyStored := by
  have send : Reachable codec accepted 1 (.rotation rotation) copySent :=
    .step (prepared_reachable (.rotation rotation) trivial accepted)
      (.send _ ⟨1, .rotation rotation, 1⟩ 0 copyObject rfl rfl (by decide))
  exact .step (.step send (.crash _)) (.land _ 0 copyObject (by decide))

theorem publication_confirmed_reachable (accepted : Kept) :
    Reachable codec accepted 1 (.rotation rotation) publicationConfirmed := by
  have reply : Reachable codec accepted 1 (.rotation rotation) copyReply :=
    .step (copy_stored_reachable accepted)
      (.receive _ ⟨1, .rotation rotation, 1⟩ 0 copyObject rfl rfl (by decide) (by decide))
  have firstCopy : Reachable codec accepted 1 (.rotation rotation) firstCopyConfirmed :=
    .step reply (.checkpoint _ ⟨1, .rotation rotation, 1⟩ 0 copyObject rfl rfl
      (by decide) rfl (by decide))
  have secondSent : Reachable codec accepted 1 (.rotation rotation) secondCopySent :=
    .step firstCopy (.send _ ⟨1, .rotation rotation, 2⟩ 1 secondCopyObject rfl rfl (by decide))
  have secondStored : Reachable codec accepted 1 (.rotation rotation) secondCopyStored :=
    .step secondSent (.land _ 1 secondCopyObject (by decide))
  have secondReply : Reachable codec accepted 1 (.rotation rotation) secondCopyReply :=
    .step secondStored (.receive _ ⟨1, .rotation rotation, 2⟩ 1 secondCopyObject rfl rfl
      (by decide) (by decide))
  have copies : Reachable codec accepted 1 (.rotation rotation) copiesConfirmed :=
    .step secondReply (.checkpoint _ ⟨1, .rotation rotation, 2⟩ 1 secondCopyObject rfl rfl
      (by decide) rfl (by decide))
  have send : Reachable codec accepted 1 (.rotation rotation) entrySent :=
    .step (.step copies (.crash _)) (.send _ ⟨1, .rotation rotation, 3⟩ 2 entryObject rfl rfl (by decide))
  have stored : Reachable codec accepted 1 (.rotation rotation) entryStored :=
    .step send (.land _ 2 entryObject (by decide))
  have received : Reachable codec accepted 1 (.rotation rotation) entryReply :=
    .step (.step stored (.crash _))
      (.receive _ ⟨1, .rotation rotation, 3⟩ 2 entryObject rfl rfl (by decide) (by decide))
  exact .step received (.checkpoint _ ⟨1, .rotation rotation, 3⟩ 2 entryObject rfl rfl
    (by decide) rfl (by decide))

theorem rotation_finishes_after_crashes :
    Reachable codec kept 1 (.rotation rotation) rotationFinished :=
  .step (publication_confirmed_reachable kept)
    (.finishRotation _ ⟨1, .rotation rotation, 4⟩ rotation rfl rfl rfl trivial)

/-- §18.2 already exposes a copy before the operation finishes. A peer sees
no introducing entry at this point. -/
theorem copies_visible_before_finish :
    Reachable codec kept 1 (.rotation rotation) copyStored ∧
    copyStored.stored (.copy 1) = some [200] ∧ copyStored.journal.row ≠ none ∧
    readIntroduction codec copyStored rotation.entryNumber = none :=
  ⟨copy_stored_reachable kept, by decide⟩

example : ∃ s, Reachable codec kept 1 (.rotation rotation) s ∧
    s.stored (.copy 1) = some [200] ∧ s.journal.row ≠ none :=
  ⟨copyStored, copies_visible_before_finish.1, copies_visible_before_finish.2.1,
    copies_visible_before_finish.2.2.1⟩

/-- A permanent late-entry drop is allowed by §9 even though storage accepted
every object. The same entry can never finish this operation. -/
theorem publication_without_kept_entry :
    Reachable codec (fun _ => False) 1 (.rotation rotation) publicationConfirmed ∧
    (∀ o ∈ Data.objects codec (.rotation rotation),
      publicationConfirmed.stored o.path = some o.bytes) ∧
    (∀ s, Reachable codec (fun _ => False) 1 (.rotation rotation) s → s.journal.row ≠ none) := by
  refine ⟨publication_confirmed_reachable _, ?_, ?_⟩
  · decide
  · intro s reachable done
    exact finished_rotation_requires_kept reachable done

def freshRotation : Rotation := { rotation with
  key := 3
  keyBytes := [33]
  keyHash := 33
  copies := [⟨3, 0, [300]⟩]
  entryNumber := 2
  sealingKey := 3 }

def restarted : State := { publicationConfirmed with
  journal := .pending ⟨1, .rotation freshRotation, 0⟩
  memory := .empty }

/-- Two members receive copies; one is then excluded. The dropped attempt's
key is exposed, so the "nothing to do" exception cannot avoid a new key. -/
theorem restart_changes_key :
    Restart codec (fun _ => True) publicationConfirmed restarted ∧
    rotation.key ≠ freshRotation.key ∧ codec.entry rotation ≠ codec.entry freshRotation ∧
    restarted.stored (.copy 1) = some [200] ∧
    restarted.stored (.entry 1) = some (codec.entry rotation) := by
  refine ⟨.rotation _ ⟨1, .rotation rotation, 4⟩ rotation freshRotation
    rfl rfl rfl trivial (by decide) (by decide) rfl (by decide), ?_⟩
  decide

def exposedKey : Key := ⟨rotation.key, 1, true, true, rotation.copies.map Copy.recipient⟩
def replacementRequest : KeyRequest := ⟨rotation.audience, true, false, [1], [], [exposedKey]⟩

theorem restart_still_needs_key : chooseKey replacementRequest = none ∧
    1 ∈ exposedKey.knownRecipients ∧ 1 ∈ replacementRequest.excludedMembers := by decide

example : ∃ before after,
    Reachable codec (fun _ => False) 1 (.rotation rotation) before ∧
    Restart codec (fun _ => True) before after ∧
    chooseKey replacementRequest = none ∧ before.journal.data ≠ after.journal.data :=
  ⟨publicationConfirmed, restarted, publication_confirmed_reachable _,
    restart_changes_key.1, restart_still_needs_key.1, by decide⟩

def snapshotObject : Object := ⟨.snapshot 1, snapshot.sealed⟩
def snapshotSent : State := { prepared (.snapshot snapshot) with sent := [(0, snapshotObject)] }
def snapshotStored : State := { snapshotSent with stored := create snapshotSent.stored snapshotObject }
def snapshotReply : State := { snapshotStored with memory := ⟨none, some snapshotObject⟩ }
def snapshotUploaded : State :=
  { snapshotStored with journal := .pending ⟨1, .snapshot snapshot, 2⟩ }
def snapshotFinished : State := { snapshotUploaded with journal := .finished 1 (.snapshot snapshot) }

theorem snapshot_stored_reachable : Reachable codec kept 1 (.snapshot snapshot) snapshotStored := by
  have sent : Reachable codec kept 1 (.snapshot snapshot) snapshotSent :=
    .step (prepared_reachable (.snapshot snapshot) rfl kept)
      (.send _ ⟨1, .snapshot snapshot, 1⟩ 0 snapshotObject rfl rfl (by decide))
  exact .step sent (.land _ 0 snapshotObject (by decide))

theorem snapshot_finishes_after_crash :
    Reachable codec kept 1 (.snapshot snapshot) snapshotFinished := by
  have reply : Reachable codec kept 1 (.snapshot snapshot) snapshotReply :=
    .step snapshot_stored_reachable
      (.receive _ ⟨1, .snapshot snapshot, 1⟩ 0 snapshotObject rfl rfl (by decide) (by decide))
  have uploaded : Reachable codec kept 1 (.snapshot snapshot) snapshotUploaded :=
    .step reply (.checkpoint _ ⟨1, .snapshot snapshot, 1⟩ 0 snapshotObject rfl rfl
      (by decide) rfl (by decide))
  exact .step (.step uploaded (.crash _)) (.finishSnapshot _ ⟨1, .snapshot snapshot, 2⟩ snapshot rfl rfl rfl)

/-- A reader can load the whole checked snapshot before the publisher's final
row deletion. Literal invisibility until the last step is false; the reader's
own atomic application is the relevant boundary. -/
theorem snapshot_visible_before_finish :
    Reachable codec kept 1 (.snapshot snapshot) snapshotStored ∧
    snapshotStored.stored (.snapshot 1) = some snapshot.sealed ∧
    snapshotStored.journal.row ≠ none ∧
    codec.openSnapshot snapshot.key snapshot.sealed = some snapshot.captured :=
  ⟨snapshot_stored_reachable, by decide⟩

example : ∃ s, Reachable codec kept 1 (.snapshot snapshot) s ∧
    s.stored (.snapshot 1) = some snapshot.sealed ∧ s.journal.row ≠ none :=
  ⟨snapshotStored, snapshot_visible_before_finish.1, snapshot_visible_before_finish.2.1,
    snapshot_visible_before_finish.2.2.1⟩

end CovenQueue.Operations.Examples
