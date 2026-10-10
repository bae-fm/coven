import CovenQueue.Verdicts
import CovenQueue.Conversion
import CovenQueue.Progress
import CovenQueue.Migration

namespace CovenQueue.Examples

def record (device number version : Nat) (read : List WriteId := [])
    (storeRead : List Nat := []) (body : List Nat := [42]) : Record :=
  ⟨⟨⟨device, number⟩, number, read, storeRead⟩, version, .ordinary, body⟩

def raised : View := ⟨some ⟨⟨10, []⟩, 2⟩, none, []⟩

/-- Ben's attempted 1 is outside S; untried 2 was converted but still read 1.
After atomic adoption, 3 reads passed positions without discarded values. -/
def writes (n : Nat) : CheckedWrite :=
  ⟨record 0 (n + 1) (if n = 0 then 1 else 2) [] (if 2 ≤ n then [10] else []), none⟩

theorem own_history_causal : Causal writes := by
  intro w a hr
  simp [reads, writes, record] at hr
  omega

theorem exclusion_chain_and_new_write :
    evaluate writes raised 0 = .excluded 10 ∧
    evaluate writes raised 1 = .excluded 10 ∧
    evaluate writes raised 2 = .applied := by
  simp [evaluate_eq, List.range_succ, causes, writes, record, raised, classify, classify.schema,
    classify.inherited, outsideRaise, excludedInput, reads, discardedBefore]

def raisedAgain : View := ⟨some ⟨⟨11, []⟩, 3⟩, none,
  [adoptionFrom writes raised 10 2]⟩

def convertedAgain (n : Nat) : CheckedWrite :=
  if n < 2 then writes n else
    ⟨{ (writes n).record with version := 3 }, none⟩

/-- The old attempted write is excluded at each raise. A new write made
after adopting the first raise never used its discarded values, even after
conversion for the second raise. -/
theorem repeated_raise_preserves_discarded_inputs :
    (adoptionFrom writes raised 10 2).discarded = [⟨0, 1⟩, ⟨0, 2⟩] ∧
    evaluate convertedAgain raisedAgain 2 = .applied := by
  simp [adoptionFrom, evaluate_eq, List.range_succ, causes, convertedAgain,
    writes, record, raised, raisedAgain, classify, classify.schema,
    classify.inherited, outsideRaise, excludedInput, reads, discardedBefore]

example : Judged writes raised 2 .applied := by
  have h := every_write_judged writes raised (fun _ => rfl) 2
  exact exclusion_chain_and_new_write.2.2 ▸ h

def retained : View := ⟨some ⟨⟨10, [⟨0, 1⟩]⟩, 2⟩, none, []⟩

def peerWrites (n : Nat) : CheckedWrite :=
  if n = 0 then ⟨record 0 1 1, none⟩
  else ⟨record 1 n 2 [⟨0, 1⟩], none⟩

example : evaluate peerWrites retained 0 = .applied ∧
    evaluate peerWrites retained 1 = .applied := by
  simp [evaluate_eq, List.range_succ, causes, peerWrites, record, retained, classify, classify.schema,
    classify.inherited, outsideRaise, excludedInput, reads, discardedBefore]

def resetView : View := ⟨none, some ⟨20, []⟩, []⟩
def resetWrites (n : Nat) : CheckedWrite :=
  ⟨record 0 (n + 1) 1 [] (if 2 ≤ n then [20] else []), none⟩

theorem reset_history :
    evaluate resetWrites resetView 0 = .ignored 20 ∧
    evaluate resetWrites resetView 1 = .ignored 20 ∧
    evaluate resetWrites resetView 2 = .applied := by
  simp [evaluate_eq, List.range_succ, causes, resetWrites, record, resetView, classify, classify.schema,
    classify.inherited, beforeReset, excludedInput, reads, discardedBefore]

/-- One write can be ignored in the reset circle and applied in the store.
An unrestricted whole-write verdict is therefore not the spec's statement. -/
example : evaluate resetWrites resetView 0 = .ignored 20 ∧
    evaluate resetWrites ⟨none, none, []⟩ 0 = .applied := by
  simp [evaluate_eq, causes, resetWrites, record, resetView, classify, classify.schema,
    classify.inherited, beforeReset, excludedInput]

example : classify raised ⟨record 0 1 2, some .notAuthorized⟩ [] =
    .refused .notAuthorized := by decide

example : (Materialized.load [0]).consume 0 .applied = Materialized.load [0] ∧
    ((Materialized.load []).consume 0 .applied).consume 0 .applied =
      (Materialized.load []).consume 0 .applied := by decide

example : commitInto (.reloading raised (.load [])) = .audienceReloading ∧
    finishReload (.reloading raised (.load [])) none = .reloading raised (.load []) ∧
    commitInto (finishReload (.reloading raised (.load [])) (some (.load [0]))) =
      .accepted raised := by decide

def key : Key := ⟨7, 1, true, true, [0]⟩
def request : KeyRequest := ⟨0, true, false, [], [], [key]⟩
def rename : Conversion := ⟨2, some (fun xs => 99 :: xs)⟩
def firstRecord : Record := record 0 1 1
def attempt : Attempt := ⟨firstRecord, 2, [7]⟩

def untriedHistory (n : Nat) : Pending × Option Refusal :=
  ⟨.untried (record 0 (n + 1) 1), none⟩

def attemptedHistory (n : Nat) : Pending × Option Refusal :=
  if n = 0 then ⟨.tried attempt, none⟩ else untriedHistory n

/-- Eligible untried predecessors convert first, so they do not falsely
exclude their successors. An uncovered attempted predecessor does. -/
theorem conversion_uses_actual_predecessor :
    (rebuild rename raised untriedHistory 1).2 = .applied ∧
    (rebuild rename raised attemptedHistory 1).2 = .excluded 10 ∧
    (rebuild rename raised attemptedHistory 1).1.record.disposition = .lost 2 := by
  have hu : (rebuild rename raised untriedHistory 0).2 = .applied := by
    rw [rebuild_eq]; decide
  have ht : (rebuild rename raised attemptedHistory 0).2 = .excluded 10 := by
    rw [rebuild_eq]; decide
  simp only [rename, raised] at hu ht
  rw [rebuild_eq rename raised untriedHistory 1,
    rebuild_eq rename raised attemptedHistory 1]
  simp [checkedHistory, causes, List.range_succ, untriedHistory,
    attemptedHistory, rename, raised, record, attempt, firstRecord, reads,
    discardedBefore, resetIgnores, convertPending, Pending.record, classify,
    classify.schema, classify.inherited, outsideRaise, excludedInput, hu, ht]


/-- An injective concrete encoding is unnecessary for the byte equality
theorems. This example includes body, version, format and key choices. -/
def encode (a : Attempt) : Bytes :=
  [a.record.identity.id.device, a.record.identity.id.number,
   a.record.version, a.format] ++ a.keys ++ a.record.body

def committed : World := { World.empty 0 with queue := (World.empty 0).queue.commit firstRecord }
def prepared : World := { committed with queue := committed.queue.prepare 2 [7] }
def sent : World := prepared.send
def converted : World := { sent with queue := sent.queue.convert rename (fun _ => false) }
def landed : World := converted.land encode attempt
def settled : World := { landed with queue := landed.queue.finish }

theorem committed_reachable : Reachable encode 0 committed :=
  .next .start (.commit _ _ [.ready ⟨none, none, []⟩ (.load [])]
    (by intro a ha; simp only [List.mem_singleton] at ha; subst a; exact ⟨_, rfl⟩))

theorem prepared_reachable : Reachable encode 0 prepared := by
  exact .next committed_reachable
    (.prepare committed 2 [request] [key] 1 1 firstRecord []
      (by decide) (by decide) (by decide) (by decide)
      ⟨request, [], rfl, rfl, rfl⟩)

theorem sent_reachable : Reachable encode 0 sent := by
  exact .next prepared_reachable
    (.send prepared ⟨1, 0, 0⟩ (some ⟨some 0, false, ⟨0, 0, 0⟩⟩) 1 1
      (by decide) (by decide) (by decide))

/-- Send seems to fail, a read still finds no object, migration runs, then
the original request lands. Confirmation compares identical original bytes. -/
theorem failed_then_landed :
    sent.stored 1 = none ∧ converted.queue.head = some attempt ∧
    landed.stored 1 = some (encode attempt) ∧
    confirm encode attempt (some (landed.stored 1)) = .stored ∧
    settled.queue.settled = 1 ∧ settled.queue.waiting = [] := by decide

theorem settled_reachable : Reachable encode 0 settled := by
  have hc : Reachable encode 0 converted := .next sent_reachable (.convert _ _ _)
  have hl : Reachable encode 0 landed := .next hc (.land _ attempt (by decide))
  exact .next hl (.acknowledge _ attempt (by decide) (by decide))

/-- A raise snapshot can cover the author's unuploaded head. It says nothing
about the still-empty storage slot, and the queue remains attempted. -/
theorem coverage_is_not_storage :
    classify retained ⟨firstRecord, none⟩ [] = .applied ∧
    prepared.queue.head = some attempt ∧ prepared.stored 1 = none ∧
    prepared.queue.settled = 0 := by decide

example : confirm encode attempt none = .failedRead ∧
    confirm encode attempt (some (some [])) = .staleCopy ∧
    confirm encode attempt (some none) = .pending := by decide

example : (committed.queue.commit (record 0 9 1)).waiting.map
      (fun p => p.record.identity.id.number) = [1, 2] ∧
    (committed.queue.commit (record 0 9 1)).reserved = 2 := by decide

example : convertPending rename false (.untried firstRecord) =
      .untried { firstRecord with version := 2, body := [99, 42] } ∧
    convertPending rename true (.untried firstRecord) =
      .untried { firstRecord with disposition := .lost 2 } ∧
    convertPending rename false (.tried attempt) = .tried attempt := by decide

example : checkIdentity 0 ⟨5, 0, 0⟩ (some ⟨some 0, false, ⟨7, 0, 0⟩⟩) = .storageAhead ∧
    checkIdentity 0 ⟨7, 0, 0⟩ (some ⟨some 0, false, ⟨7, 0, 0⟩⟩) = .ready ∧
    checkIdentity 0 ⟨7, 0, 0⟩ (some ⟨none, false, ⟨7, 0, 0⟩⟩) = .identityMismatch ∧
    checkIdentity 0 ⟨7, 0, 0⟩ (some ⟨some 0, true, ⟨7, 0, 0⟩⟩) = .replaced := by decide

def oldCircleKey : Key := ⟨1, 1, true, true, [0, 1]⟩
def newCircleKey : Key := ⟨2, 2, true, true, [0]⟩
def remaining : KeyRequest := ⟨1, true, false, [1], [], [oldCircleKey, newCircleKey]⟩
def departed : KeyRequest := ⟨1, false, true, [1], [1], [oldCircleKey]⟩

theorem remaining_member_rotates : chooseKey remaining = some newCircleKey ∧
    1 ∉ newCircleKey.knownRecipients := by decide

/-- Smallest removal exception, §6 and §14.6: two circle members share K;
Ben queues an edit, learns Ana removed him, then first sends that edit with K.
The literal claim that every first send excludes the removed member fails. -/
theorem departed_member_counterexample :
    departed.currentMember = false ∧ 1 ∈ departed.excludedMembers ∧
    chooseKey departed = some oldCircleKey ∧ 1 ∈ oldCircleKey.knownRecipients := by decide

example : ∃ r k, 1 ∈ r.excludedMembers ∧ chooseKey r = some k ∧ 1 ∈ k.knownRecipients :=
  ⟨departed, oldCircleKey, by decide, by decide, by decide⟩

/-- Storage accepts every retried write, vacuously: this old app never reaches
a first attempt. One queued write is enough to refute storage-only progress. -/
def noUpdate : Run encode where
  state _ := committed
  valid := (reachable_invariants committed_reachable).1
  step _ := .wait _

theorem storage_acceptance_alone_insufficient :
    (∀ t a, (noUpdate.state t).queue.head = some a →
      ∃ u, t ≤ u ∧ (noUpdate.state u).stored a.record.identity.id.number = some (encode a)) ∧
    (∀ t, (noUpdate.state t).queue.reserved = 1 ∧ (noUpdate.state t).queue.settled = 0) := by
  constructor
  · intro t a ha; simp [noUpdate, committed, World.empty, Queue.commit, Queue.head] at ha
  · intro t; exact ⟨rfl, rfl⟩

example : ¬ ∃ t, 1 ≤ (noUpdate.state t).queue.settled := by
  intro ⟨t, ht⟩
  have := (storage_acceptance_alone_insufficient.2 t).2
  omega

namespace RetiredRow

/-- Insert, migration deletion, and converted concurrent edit. -/
def changes : CovenMerge.Writes (Fin 3) Unit Unit where
  ts w := w.val
  past w a := w.val > 0 && a.val == 0
  chg w _ := some (if w.val = 0 then ⟨.ins, 0, fun _ => true⟩
    else if w.val = 1 then ⟨.del, 1, fun _ => false⟩ else ⟨.upd, 1, fun _ => true⟩)

theorem valid : CovenMerge.Valid changes where
  ts_inj _ _ h := Fin.eq_of_val_eq h
  past_ts := by decide
  gen_seen w r ch hc := by
    rcases w with ⟨i, hi⟩
    have hs : i = 0 ∨ i = 1 ∨ i = 2 := by omega
    rcases hs with rfl | rfl | rfl
    all_goals simp [changes] at hc
    · subst ch; exact Or.inl rfl
    · subst ch; exact Or.inr ⟨0, ⟨.ins, 0, fun _ => true⟩, rfl, rfl, by decide, rfl⟩
    · subst ch; exact Or.inr ⟨0, ⟨.ins, 0, fun _ => true⟩, rfl, rfl, by decide, rfl⟩
  parity w r ch hc := by
    rcases w with ⟨i, hi⟩
    have hs : i = 0 ∨ i = 1 ∨ i = 2 := by omega
    rcases hs with rfl | rfl | rfl
    all_goals simp [changes] at hc
    all_goals subst ch; decide

def initial := CovenMerge.step changes CovenMerge.St.init 0
def retired := CovenMerge.step changes initial 1
def result := CovenMerge.step changes retired 2

def captured : List FrozenCell := [⟨"title", 42, ⟨0, 1⟩⟩]
def migrationResult := migrateHiddenRows changes initial 1 ⟨0, 2⟩ 2
  (fun _ => .independentOrFinal) (fun _ => captured)

theorem retirement_keeps_read_values : migrationResult.1 = retired ∧
    migrationResult.2 () = some ⟨1, ⟨0, 2⟩, 2, captured⟩ ∧
    retired.lost () () 0 = none := by
  exact ⟨rfl, rfl, by decide⟩

theorem converted_edit_to_retired_row :
    retired.gen () = 2 ∧ result.gen () = 2 ∧ result.cell () () = none ∧
    result.genWrite () 2 = some 1 ∧ result.lost () () 2 = some (1, 1) := by decide

example : result.cell () () ≠ some 2 := by decide

end RetiredRow
end CovenQueue.Examples
