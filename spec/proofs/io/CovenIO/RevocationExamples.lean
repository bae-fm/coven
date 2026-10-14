import CovenIO.Revocation
import CovenStorelog.ExamplesSupport

namespace CovenIO.RevocationExamples
open CovenStorelog CovenStorelogData Revocation

def initialKey : KeySelection.Key := ⟨0, .store, ⟨0, Vector.replicate 32 0⟩⟩
def key : KeySelection.Key := ⟨3, .store, ⟨1, Vector.replicate 32 1⟩⟩

/-- Ana's devices concurrently add Ben and rotate without seeing that addition.
Ben's initial key precedes admission (§12.2); the rotation publishes copies
for its recorded audience. Historical sharing then reserves K1 for Ben. -/
def history : CurrentReplay.History :=
  ⟨fun e => match e with
    | 0 => ⟨0, 0, [], .create "ana" initialKey.key⟩
    | 1 => ⟨0, 1, [0], .addDevice 0 1⟩
    | 2 => ⟨0, 0, [0, 1], .addMember 1 .member "ben"⟩
    | 3 => ⟨0, 1, [0, 1], .rotateKey .store key.key⟩
    | _ => ⟨0, 0, [0, 1, 2, 3], .removeMember 1⟩,
   id, fun e => if e = 3 then 2 else e⟩

def prior : KeyPhases.CaughtUp :=
  ⟨[0, 1, 2, 3], ⟨[initialKey, key], [(initialKey, 0), (initialKey, 1), (key, 0)]⟩⟩
def removed : KeyPhases.CaughtUp := ⟨[0, 1, 2, 3, 4], prior.observation⟩
def attempt : KeySelection.Attempt := ⟨key, [7]⟩
def finalTime := month + 5
def finalAt := finalTime + listingMargin + 1

def entryWriter (e : Nat) : Nat := if e = 1 ∨ e = 3 then 1 else 0
def entryNumber (e : Nat) : Nat := match e with | 0 | 1 => 1 | 2 | 3 => 2 | _ => 3

def entryStore : Store := fun path => match path with
  | .log .entry 0 1 => some ⟨⟨[0], 0⟩, 0⟩
  | .log .entry 0 2 => some ⟨⟨[2], 2⟩, 0⟩
  | .log .entry 0 3 => some ⟨⟨[4], 4⟩, 0⟩
  | .log .entry 1 1 => some ⟨⟨[1], 1⟩, 0⟩
  | .log .entry 1 2 => some ⟨⟨[3], 3⟩, 0⟩
  | .log .copy 0 1 => some ⟨⟨[0, 0], 0⟩, 0⟩
  | .log .copy 0 2 => some ⟨⟨[0, 1], 2⟩, 0⟩
  | .log .copy 1 1 => some ⟨⟨[1, 0], 3⟩, 0⟩
  | _ => none

def copyObject : Object := ⟨⟨[1, 1], finalAt + 3⟩, 0⟩
def withCopy : Store := CovenIO.put entryStore (.log .copy 0 3) [1, 1] (finalAt + 3)
def world : World := fun t => if t < finalAt + 3 then entryStore else withCopy
def providers : Nat → Provider := fun t => ⟨world t, t⟩
def decodeEntries (object : Object) : List Nat := object.value.bytes
/-- Symbolic bytes encode a key id and recipient; ciphertext is not modeled. -/
def decodeCopies (object : Object) : KeySelection.Copies := match object.value.bytes with
  | [0, recipient] => [(initialKey, recipient)]
  | [1, recipient] => [(key, recipient)]
  | _ => []

def pass : CompletedPass world where
  listedAt := finalAt + 1
  writers := [0, 1]
  folders := by
    intro writer object present
    by_cases zero : writer = 0
    · simp [zero]
    · by_cases one : writer = 1
      · simp [one]
      · simp [world, entryStore, zero, one, finalAt] at present
  startTime := fun _ _ => finalAt + 1
  startPosition := fun kind writer => match kind with
    | .entry => 2 | .copy => if writer = 0 then 2 else 1 | .write => 0
  finishTime := fun kind writer =>
    if kind = .entry ∧ writer = 0 then finalAt + 2 else finalAt + 1
  finishPosition := fun kind writer => match kind with
    | .entry => if writer = 0 then 3 else 2
    | .copy => if writer = 0 then 2 else 1 | .write => 0
  reads := fun kind writer => match kind with
    | .entry => if writer = 0 then
        [⟨finalAt + 1, .log .entry 0 3, some ⟨⟨[4], 4⟩, 0⟩⟩,
         ⟨finalAt + 2, .log .entry 0 4, none⟩]
      else [⟨finalAt + 1, .log .entry writer 3, none⟩]
    | .copy => [⟨finalAt + 1, .log .copy writer (if writer = 0 then 3 else 2), none⟩]
    | .write => [⟨finalAt + 1, .log .write writer 1, none⟩]
  afterListing := by intros; exact Nat.le_refl _
  scans := by
    intro kind writer known
    have cases : writer = 0 ∨ writer = 1 := by simpa using known
    rcases cases with rfl | rfl <;> cases kind
    · exact .hit _ _ _ (by decide) (.miss _ _ (by decide))
    all_goals exact .miss _ _ (by decide)

def catchup : Catchup := ⟨finalAt + 1, finalAt + 2, .send, false⟩
def writeRequest : Request := .create (.log .write 1 1) attempt.bytes
def events : List IOEvent :=
  [.beginPass catchup.started, .checked catchup.finished .send false,
   .request .pass (finalAt + 4) writeRequest]

theorem send_trace : IOTrace ⟨none, none⟩ events ⟨none, some catchup⟩ :=
  .cons rfl (.cons rfl (.cons rfl (.nil _)))

theorem final_removal :
    CovenStorelog.Valid history.membership.log 5 ∧
    StorageFinality.certified history month 5 finalTime (entrySet removed.received) 4 = true ∧
    4 ∈ (CurrentReplay.resolve history month 5 (entrySet removed.received)).kept ∧
    1 ∉ audienceMembers (CurrentReplay.resolve history month 5 (entrySet removed.received)).state .store := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

theorem storage_history_valid : Finality.Valid history.membership 5 := by
  constructor
  · exact final_removal.1
  · intro a b ha hb same
    exact same
  · intro e he
    change (if e = 3 then 2 else e) ≤ e
    split <;> omega
  · have checked : ∀ w a : Fin 5,
        hadRead history.membership.log w a = true ↔
          history.stored a < history.attempted w := by decide
    intro w a hw ha
    exact checked ⟨w, hw⟩ ⟨a, ha⟩

/-- Admission's initial copy is already published, and a concurrent rotation
uses its own recorded audience. K1 is shared with Ben only after its author
learns his addition. Neither prerequisite publication is delayed. -/
theorem prerequisite_copies_published :
    entryStore (.log .copy 0 2) = some ⟨⟨[0, 1], 2⟩, 0⟩ ∧
    entryStore (.log .copy 1 1) = some ⟨⟨[1, 0], 3⟩, 0⟩ ∧
    KeyPhases.rotate history month 5 0 1
      ⟨[0, 1], ⟨[initialKey], [(initialKey, 0)]⟩⟩ 3 (.ok ()) (.ok ()) =
        .ok ⟨[0, 1, 3], ⟨[key, initialKey], [(initialKey, 0), (key, 0)]⟩⟩ ∧
    KeySelection.usable (audienceMembers
      (CurrentReplay.resolve history month 5 (entrySet removed.received)).state)
      (KeySelection.receivedAuthorized history month 5 (entrySet removed.received))
      removed.observation initialKey = false := ⟨rfl, rfl, rfl, by decide⟩

theorem scan_supplies_removal :
    caughtUp pass prior decodeEntries decodeCopies = removed ∧
    KeyPhases.firstSend history month 5 0 .store
      (caughtUp pass prior decodeEntries decodeCopies) [7] = some attempt := by decide

/-- Full finality-discovery bridge, including a newly read removal. Provider
time advances at exactly one second per elapsed second in this history. -/
theorem caught_up_final_prefix :
    StorageFinality.CompleteOld history month 5 finalTime
      (entrySet (caughtUp pass prior decodeEntries decodeCopies).received) := by
  apply caughtUp_complete_old history 5 1
    ⟨entryStore, finalAt⟩ ⟨entryStore, finalAt⟩ (.refl _) (by decide)
    providers pass prior decodeEntries decodeCopies entryWriter entryNumber
    (fun w => if w = 0 then 3 else 2)
  · intro e he
    have options : e = 0 ∨ e = 1 ∨ e = 2 ∨ e = 3 ∨ e = 4 := by omega
    rcases options with rfl | rfl | rfl | rfl | rfl <;> exact ⟨_, rfl, rfl⟩
  · intro e he object stored
    simp only [pass, entryWriter]; split <;> simp
  · intro w hw
    exact .step (.refl _) (.tick _ _ (by dsimp [pass]; omega))
  · intro w hw t ht
    by_cases early : t < finalAt + 3
    · simpa [providers, world, early, pass] using
        ProviderRun.step (.refl (⟨entryStore, finalAt + 1⟩ : Provider)) (.tick _ t ht)
    · have create := ProviderRun.step
        (.step (.refl (⟨entryStore, finalAt + 1⟩ : Provider))
          (.tick _ (finalAt + 3) (by dsimp; omega)))
        (.create _ (.log .copy 0 3) [1, 1])
      simpa [providers, world, early, pass, withCopy] using
        ProviderRun.step create (.tick _ t (by dsimp; omega))
  · intro w hw t ht hf n hn hn'
    have options : w = 0 ∨ w = 1 := by simpa [pass] using hw
    rcases options with rfl | rfl
    · have number : n = 3 := by change 2 < n at hn; change n ≤ 3 at hn'; omega
      subst n
      refine ⟨⟨⟨[4], 4⟩, 0⟩, ?_⟩
      have early : t < finalAt + 3 := by change t ≤ finalAt + 2 at hf; omega
      simp [providers, world, early, entryStore]
    · change 2 < n at hn; change n ≤ 2 at hn'; omega
  · intro e he object stored
    have options : e = 0 ∨ e = 1 ∨ e = 2 ∨ e = 3 ∨ e = 4 := by omega
    rcases options with rfl | rfl | rfl | rfl | rfl <;> decide
  · intro e he cached
    have options : e = 0 ∨ e = 1 ∨ e = 2 ∨ e = 3 ∨ e = 4 := by omega
    rcases options with rfl | rfl | rfl | rfl | rfl <;> first | decide | contradiction
  · intro e he object stored
    have options : e = 0 ∨ e = 1 ∨ e = 2 ∨ e = 3 ∨ e = 4 := by omega
    rcases options with rfl | rfl | rfl | rfl | rfl <;>
      simp [entryWriter, entryNumber, entryStore] at stored <;> subst object <;> decide

/-- At completion, before the delayed copy lands, every premise of the
joined theorem holds. The following counterexample changes the send instant. -/
theorem joined_guarantee_instantiated :
    ∀ writer number object, world (finalAt + 2) (.log .copy writer number) = some object →
      (attempt.key, 1) ∉ decodeCopies object := by
  have result := gated_first_send_revocation history 5 finalTime finalAt 4 0 1 .store
    writeRequest
    (show IOTrace ⟨none, none⟩
      ([.beginPass catchup.started, .checked catchup.finished .send false] ++
       [.request .pass (finalAt + 2) writeRequest]) ⟨none, some catchup⟩ from
      .cons rfl (.cons rfl (.cons rfl (.nil _))))
    (Or.inl rfl) catchup rfl pass (by decide)
    (by intro kind writer known; simp only [pass]; split <;> simp [catchup])
    prior decodeEntries decodeCopies removed
    (by rw [scan_supplies_removal.1]; intros; assumption)
    (by rw [scan_supplies_removal.1]; intros; assumption)
    (entrySet removed.received) final_removal.1 (by decide)
    (by rw [← scan_supplies_removal.1]; exact caught_up_final_prefix)
    caught_up_final_prefix (by decide) (by decide) (by decide) rfl final_removal.2.2.1
    (by intro e bound later; omega)
    [7] attempt [] (.log .write 1 1) rfl (by decide) (by simp)
    (by
      intro writer number object stored copy decoded
      have early : finalAt + 2 < finalAt + 3 := by omega
      simp only [world, early, ↓reduceIte, entryStore] at stored
      split at stored
      all_goals try { rename_i eq; cases eq }
      all_goals try contradiction
      all_goals
        cases stored
        rw [scan_supplies_removal.1]
        simp_all [decodeCopies, removed, prior, key, initialKey])
  exact result.2.2.2.2.2.2.2

/-- A copy reserved while Ben belonged stays fixed even when its initiator
is cancelled. Neither a fresh gate nor final removal discards that reservation. -/
theorem reserved_copy_survives :
    KeySelection.share (audienceMembers (CurrentReplay.resolve history month 5
      (entrySet prior.received)).state) prior.observation.custody prior.observation.copies 1 key =
        some (prior.observation.copies ++ [(key, 1)]) ∧
    Publication.Run (⟨[[0, 0], [0, 1], [1, 1]], [[0, 0], [0, 1]], 2⟩ : Publication.Writer) ∧
    Publication.Step (⟨[[0, 0], [0, 1], [1, 1]], [[0, 0], [0, 1]], 2⟩ : Publication.Writer)
      ⟨[[0, 0], [0, 1], [1, 1]], [[0, 0], [0, 1], [1, 1]], 2⟩ := by
  refine ⟨by decide, ?_, .land _ [1, 1] [] rfl rfl⟩
  exact .step (.step (.step (.step (.step (.step (.step .initial
    (.reserve _ [0, 0])) (.land _ [0, 0] [] rfl rfl)) (.confirm _ rfl))
    (.reserve _ [0, 1])) (.land _ [0, 1] [] rfl rfl)) (.confirm _ rfl)) (.reserve _ [1, 1])

/-- The sender is Ana, who remains active. The already reserved Ben copy
is sent before the reader's miss and lands after it, within one day. -/
theorem in_flight_after_finality :
    IOTrace ⟨none, none⟩ events ⟨none, some catchup⟩ ∧
    finalAt < catchup.started ∧
    KeyPhases.firstSend history month 5 0 .store removed [7] = some attempt ∧
    IOTrace ⟨none, none⟩
      [.beginPass finalAt, .checked finalAt .send false,
       .request .outside finalAt (.create (.log .copy 0 3) [1, 1])]
      ⟨none, some ⟨finalAt, finalAt, .send, false⟩⟩ ∧
    finalAt + 3 ≤ finalAt + day ∧
    world (finalAt + 1) (.log .copy 0 3) = none ∧
    world (finalAt + 4) (.log .copy 0 3) = some copyObject ∧
    (attempt.key, 1) ∈ decodeCopies copyObject := by
  refine ⟨send_trace, by decide, by decide, ?_, by decide⟩
  exact .cons rfl (.cons rfl (.cons rfl (.nil _)))

/-- Counterexample to unconditional post-finality secrecy. §3 expressly
requires no later copy; §11 expressly permits this residual window. -/
example : StorageFinality.certified history month 5 finalTime (entrySet removed.received) 4 = true ∧
    finalAt < catchup.started ∧ SendAllowed catchup (finalAt + 4) ∧
    KeyPhases.firstSend history month 5 0 .store removed [7] = some attempt ∧
    world (finalAt + 4) (.log .copy 0 3) = some copyObject ∧
    (attempt.key, 1) ∈ decodeCopies copyObject := by decide

/-- Removal lands at second 4, after this writer's miss at 3 but before
completion at 5. It need not be in that pass's received set. -/
def during : World := fun t => if t < 4 then
  CovenIO.put (fun path => if path = .log .entry 0 3 then none else entryStore path)
    (.log .copy 0 3) [1, 1] 3
  else CovenIO.put entryStore (.log .copy 0 3) [1, 1] 3

def duringPrior : KeyPhases.CaughtUp :=
  ⟨prior.received, ⟨prior.observation.custody, prior.observation.copies ++ [(key, 1)]⟩⟩

theorem removal_during_catchup :
    Scan during .entry 0 3 2 3 2 [⟨3, .log .entry 0 3, none⟩] ∧
    during 4 (.log .entry 0 3) = some ⟨⟨[4], 4⟩, 0⟩ ∧
    during 3 (.log .copy 0 3) = some ⟨⟨[1, 1], 3⟩, 0⟩ ∧
    IOTrace ⟨none, none⟩ [.beginPass 3, .checked 5 .send false, .request .pass 5 writeRequest]
      ⟨none, some ⟨3, 5, .send, false⟩⟩ ∧
    KeyPhases.firstSend history month 5 0 .store duringPrior [7] = some attempt := by
  refine ⟨.miss _ _ (by decide), rfl, rfl, ?_, by decide⟩
  exact .cons rfl (.cons rfl (.cons rfl (.nil _)))

/-- §11 says writes sent before learning removal retain their readability.
Finality-before-start is deliberately absent, so §3 is not contradicted. -/
example : SendAllowed ⟨3, 5, .send, false⟩ 5 ∧ 3 < history.stored 4 ∧ history.stored 4 < 5 ∧
    KeyPhases.firstSend history month 5 0 .store duringPrior [7] = some attempt := by decide

/-- Observation protects immediately, without waiting for finality. If the
only held key is exposed, the first send waits for acquisition or rotation. -/
theorem nonfinal_removal_blocks_exposed_key :
    StorageFinality.certified history month 5 5 (entrySet removed.received) 4 = false ∧
    4 ∈ (CurrentReplay.resolve history month 5 (entrySet removed.received)).kept ∧
    KeyPhases.firstSend history month 5 0 .store
      ⟨removed.received, ⟨prior.observation.custody, prior.observation.copies ++ [(key, 1)]⟩⟩ [7] = none := by decide

/-- Two admins' concurrent removals: Ben's removal of Ana can be kept
before Ana's earlier removal of Ben arrives. It is not yet final. -/
def racing : CurrentReplay.History :=
  ⟨fun e => match e with
    | 0 => ⟨0, 0, [], .create "ana" initialKey.key⟩
    | 1 => ⟨0, 0, [0], .addMember 1 .admin "ben"⟩
    | 2 => ⟨1, 1, [0, 1], .addDevice 1 1⟩
    | 3 => ⟨0, 0, [0, 1, 2], .removeMember 1⟩
    | _ => ⟨1, 1, [0, 1, 2], .removeMember 0⟩,
   id, fun e => min e 3⟩

theorem nonfinal_removal_reverses :
    CovenStorelog.Valid racing.membership.log 5 ∧
    StorageFinality.certified racing month 5 5 (entrySet [0, 1, 2, 4]) 4 = false ∧
    4 ∈ (CurrentReplay.resolve racing month 5 (entrySet [0, 1, 2, 4])).kept ∧
    member (CurrentReplay.resolve racing month 5 (entrySet [0, 1, 2, 4])).state 0 = false ∧
    4 ∈ (CurrentReplay.resolve racing month 5 (entrySet [0, 1, 2, 3, 4])).dropped ∧
    member (CurrentReplay.resolve racing month 5 (entrySet [0, 1, 2, 3, 4])).state 0 = true := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

/-- §9 allows this reversal; an observed removal is not finality evidence. -/
example :
    member (CurrentReplay.resolve racing month 5 (entrySet [0, 1, 2, 4])).state 0 = false ∧
    member (CurrentReplay.resolve racing month 5 (entrySet [0, 1, 2, 3, 4])).state 0 = true := by decide

/-- Finality fixes the old entry's fate; it does not forbid a new addition.
This is why the joined theorem states the no-return condition separately. -/
def rejoined : CurrentReplay.History :=
  ⟨fun e => if e = 5 then ⟨0, 0, [0, 1, 2, 3, 4], .addMember 1 .member "ben"⟩ else history.log e,
   fun e => if e = 5 then finalAt + 1 else history.stored e,
   fun e => if e = 5 then finalAt + 1 else history.attempted e⟩

example :
    StorageFinality.certified rejoined month 6 finalTime (entrySet removed.received) 4 = true ∧
    4 ∈ (CurrentReplay.resolve rejoined month 6 (entrySet [0, 1, 2, 3, 4, 5])).kept ∧
    member (CurrentReplay.resolve rejoined month 6 (entrySet [0, 1, 2, 3, 4, 5])).state 1 = true := by decide

/-- A pre-retirement request can land afterward and count. §10's inclusive
30-day admission rule does not grant permission to start another send. -/
theorem retired_write_lands :
    IOTrace ⟨none, none⟩ [.beginPass 1, .checked 1 .send false,
      .request .pass 1 (.create (.log .write 1 1) [7])]
      ⟨none, some ⟨1, 1, .send, false⟩⟩ ∧
    Retired.writeAllowed [⟨2, 2, [1]⟩] false ⟨1, 1, 3, [0]⟩ = true ∧
    1 ≤ 3 ∧ 3 ≤ 1 + day ∧
    gateStep ⟨none, some ⟨3, 3, .send, true⟩⟩
      (.request .pass 3 (.create (.log .write 1 2) [8])) = none := by
  refine ⟨?_, by decide⟩
  exact .cons rfl (.cons rfl (.cons rfl (.nil _)))

example : Retired.writeAllowed [⟨2, 2, [1]⟩] false ⟨1, 1, 2 + month, [0]⟩ = true ∧
    Retired.writeAllowed [⟨2, 2, [1]⟩] false ⟨1, 1, 3 + month, [0]⟩ = false := by decide

/-- Retired.Publication covers a copy already in flight at finality. The
actual IOTrace and one-day request bound supply its strict drain deadline. -/
theorem retired_in_flight_copy :
    299 + day < 1 + freshFor + day := by
  apply retired_copy_settles .pass 299 0 1 1 (299 + day) [1]
    (before := ⟨none, none⟩) (after := ⟨none, some ⟨0, 0, .send, false⟩⟩)
    (events := [.beginPass 0, .checked 0 .send false])
  · exact .cons rfl (.cons rfl (.cons rfl (.nil _)))
  · rfl
  · decide
  · decide
  · intro impossible; simp at impossible

/-- A retry passes the new gate and still uses its old exposed key. This is
explicitly required by §3, not a counterexample to its first-attempt promise. -/
example : SendAllowed catchup (finalAt + 4) ∧
    (KeySelection.retry attempt).key = key ∧
    (key, 1) ∈ ([(key, 0), (key, 1)] : KeySelection.Copies) := by decide

end CovenIO.RevocationExamples
