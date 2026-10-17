import CovenQueue.Verdicts
import CovenQueue.Conversion
import CovenQueue.Keys
import CovenQueue.Migration

namespace CovenQueue.Examples

def record (device number version : Nat) (read : List WriteId := [])
    (storeRead : List Nat := []) (body : List Nat := [42]) : Record :=
  ⟨⟨⟨device, number⟩, number, read, storeRead⟩, version, .ordinary,
    [⟨0, 0, .ins, ⟨none, []⟩, .values body⟩]⟩

def raised : View := ⟨some ⟨⟨10, []⟩, 2⟩, none, []⟩

/-- Implicit own reads do not inherit exclusion. These row changes carry no
source from the excluded old-schema write. -/
def writes (n : Nat) : CheckedWrite :=
  ⟨record 0 (n + 1) (if n = 0 then 1 else 2) [] (if 2 ≤ n then [10] else []), none⟩

theorem own_history_causal : Causal writes := by
  intro w a hr
  simp [reads, writes, record] at hr
  omega

theorem independent_own_tail :
    evaluate writes raised 0 = .excluded 10 2 ∧
    evaluate writes raised 1 = .applied ∧
    evaluate writes raised 2 = .applied := by decide

def raisedAgain : View := ⟨some ⟨⟨11, []⟩, 3⟩, none, []⟩
def convertedAgain (n : Nat) : CheckedWrite :=
  if n < 2 then writes n else ⟨{ (writes n).record with version := 3 }, none⟩

theorem repeated_raise_independent : evaluate convertedAgain raisedAgain 2 = .applied := by decide

example : Judged writes raised 2 .applied := by
  have h := every_write_judged writes raised (fun _ => rfl) 2
  exact independent_own_tail.2.2 ▸ h

def retained : View := ⟨some ⟨⟨10, [⟨0, 1⟩]⟩, 2⟩, none, []⟩
def peerWrites (n : Nat) : CheckedWrite :=
  if n = 0 then ⟨record 0 1 1, none⟩ else ⟨record 1 n 2 [⟨0, 1⟩], none⟩

example : evaluate peerWrites retained 0 = .applied ∧ evaluate peerWrites retained 1 = .applied := by decide

def resetView : View := ⟨none, some ⟨20, []⟩, []⟩
def resetWrites (n : Nat) : CheckedWrite :=
  ⟨record 0 (n + 1) 1 [] (if 2 ≤ n then [20] else []), none⟩

theorem reset_history :
    evaluate resetWrites resetView 0 = .ignored 20 ∧
    evaluate resetWrites resetView 1 = .ignored 20 ∧
    evaluate resetWrites resetView 2 = .applied := by decide

example : evaluate resetWrites resetView 0 = .ignored 20 ∧
    evaluate resetWrites ⟨none, none, []⟩ 0 = .applied := by decide

example : classify raised ⟨record 0 1 2, some .notAuthorized⟩ = .refused .notAuthorized := by decide

example : (Materialized.load [0]).consume 0 .applied = Materialized.load [0] ∧
    ((Materialized.load []).consume 0 .applied).consume 0 .applied =
      (Materialized.load []).consume 0 .applied := by decide

example : commitInto (.reloading raised (.load [])) = .audienceReloading ∧
    finishReload (.reloading raised (.load [])) none = .reloading raised (.load []) ∧
    commitInto (finishReload (.reloading raised (.load [])) (some (.load [0]))) =
      .accepted raised := by decide

def rename : Conversion := ⟨2, some (fun xs => 99 :: xs)⟩
def firstRecord : Record := record 0 1 1
def attempt : Attempt := ⟨firstRecord, 2, [7]⟩

def untriedHistory (n : Nat) : Pending × Option Refusal :=
  ⟨.untried (record 0 (n + 1) 1), none⟩

def attemptedHistory (n : Nat) : Pending × Option Refusal :=
  if n = 0 then ⟨.tried attempt, none⟩ else untriedHistory n

theorem untried_valid : DataValid (checkedHistory untriedHistory) where
  uniqueRows w := by simp [checkedHistory, untriedHistory, record, Pending.record]
  earlier w r a ha := by
    by_cases row : 0 = r <;>
      simp [provenance, rowChange, checkedHistory, untriedHistory, Pending.record,
        record, row, CovenMerge.DataDependency.Provenance.sources] at ha
  sourcePresent w r a ha := by
    by_cases row : 0 = r <;>
      simp [provenance, rowChange, checkedHistory, untriedHistory, Pending.record,
        record, row, CovenMerge.DataDependency.Provenance.sources] at ha

theorem attempted_same : checkedHistory attemptedHistory = checkedHistory untriedHistory := by
  funext w
  by_cases zero : w = 0 <;>
    simp [checkedHistory, attemptedHistory, untriedHistory, zero, attempt, firstRecord, Pending.record]

theorem attempted_valid : DataValid (checkedHistory attemptedHistory) := attempted_same ▸ untried_valid

/-- Attempt status cannot change receive conversion or rewrite upload originals. -/
theorem conversion_uses_actual_predecessor :
    (receive rename raised (checkedHistory untriedHistory) untried_valid 1).2 = .applied ∧
    (receive rename raised (checkedHistory attemptedHistory) attempted_valid 1).2 = .applied ∧
    (receive rename raised (checkedHistory attemptedHistory) attempted_valid 1).1.body =
      [⟨0, 0, .ins, ⟨none, []⟩, .values [99, 42]⟩] := by
  simp only [attempted_same]
  have kept : CovenMerge.DataDependency.retained
      (preparedState rename raised (checkedHistory untriedHistory) untried_valid) 1 0 = true := by
    rw [CovenMerge.DataDependency.retained_eq]
    rfl
  have loss : conversionLosses rename raised (checkedHistory untriedHistory) untried_valid 1 0 = none := by
    change (if CovenMerge.DataDependency.retained
      (preparedState rename raised (checkedHistory untriedHistory) untried_valid) 1 0 then none else some _) = none
    rw [kept]; rfl
  refine ⟨by decide, by decide, ?_⟩
  change [convertChange (fun xs => 99 :: xs)
    (conversionLosses rename raised (checkedHistory untriedHistory) untried_valid 1)
      ⟨0, 0, .ins, ⟨none, []⟩, .values [42]⟩] = _
  simp [convertChange, loss]

example : convertRecord rename (fun _ => none) firstRecord =
      { firstRecord with version := 2, body := [⟨0, 0, .ins, ⟨none, []⟩, .values [99, 42]⟩] } ∧
    convertRecord rename (fun _ => some (.dependency 1 [0])) firstRecord =
      { firstRecord with version := 2, body := [⟨0, 0, .ins, ⟨none, []⟩, .frozen (.dependency 1 [0]) [42]⟩] } ∧
    (rebuild rename raised ⟨attemptedHistory, checkedHistory attemptedHistory, attempted_valid⟩).uploads 0 =
      (.tried attempt, none) := by decide

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
  (fun _ => true) (fun _ => captured)

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
