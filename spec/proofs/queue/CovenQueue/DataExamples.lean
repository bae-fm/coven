import CovenQueue.Examples

namespace CovenQueue.DataExamples

open CovenMerge.DataDependency

def insert (row value : Nat) : RowChange := ⟨row, 0, .ins, ⟨none, []⟩, .values [value]⟩
def edit (row generationSource setter value : Nat) : RowChange :=
  ⟨row, 1, .upd, ⟨some generationSource, [(0, setter)]⟩, .values [value]⟩

def records (setterOnly : Bool) (n : Nat) : Record :=
  { Examples.record 0 (n + 1) 1 with body :=
    if n = 0 then [insert 0 10] ++ if setterOnly then [insert 1 20] else []
    else if n = 1 then [if setterOnly then edit 1 0 0 21 else insert 1 20]
    else if n = 2 then [edit 0 0 0 11, edit 1 (if setterOnly then 0 else 1) 1 22]
    else if n = 3 then [edit 0 0 2 12, edit 1 (if setterOnly then 0 else 1) 2 23]
    else if n = 4 then [⟨1, 1, .del, ⟨some (if setterOnly then 0 else 1), [(0, 2)]⟩, .values []⟩]
    else [] }

/-- Write 1 already lost its receive interpretation by an explicit drop choice.
The upload record remains ordinary; later callbacks cannot revive this loss. -/
def history (mode : Bool) : History := fun w =>
  ⟨if w = 1 then { records mode w with disposition := .lost 2 } else records mode w, none⟩

theorem excluded_source_keeps_original (mode : Bool) :
    convertRecord ⟨2, .drop⟩ (fun _ => none) (records mode 1) = (history mode 1).record := by
  cases mode <;> decide

def raised : View := ⟨some ⟨⟨10, [⟨0, 1⟩]⟩, 2⟩, none, []⟩
def conversion : Conversion := ⟨2, .convert (fun xs => xs.map (· + 100))⟩

theorem history_body (mode : Bool) (w : Nat) :
    (history mode w).record.body = (records mode w).body := by
  simp only [history]
  split <;> rfl

theorem valid (mode : Bool) : DataValid (history mode) where
  uniqueRows w := by
    simp only [history_body]
    have cases : w = 0 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 4 ∨ 4 < w := by omega
    have outside : 4 < w → w ≠ 0 ∧ w ≠ 1 ∧ w ≠ 2 ∧ w ≠ 3 ∧ w ≠ 4 := by omega
    rcases cases with rfl | rfl | rfl | rfl | rfl | hw <;> cases mode <;>
      simp_all [records, insert, edit]
  earlier w r a ha := by
    have rowCases : r = 0 ∨ r = 1 ∨ 1 < r := by omega
    have rowOutside : 1 < r → 0 ≠ r ∧ 1 ≠ r := by omega
    have cases : w = 0 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 4 ∨ 4 < w := by omega
    have outside : 4 < w → w ≠ 0 ∧ w ≠ 1 ∧ w ≠ 2 ∧ w ≠ 3 ∧ w ≠ 4 := by omega
    rcases cases with rfl | rfl | rfl | rfl | rfl | hw <;> cases mode <;>
      rcases rowCases with rfl | rfl | hr <;>
      simp_all [provenance, rowChange, history_body, records, insert, edit,
        CovenMerge.DataDependency.Provenance.sources] <;> omega
  sourcePresent w r a ha := by
    have rowCases : r = 0 ∨ r = 1 ∨ 1 < r := by omega
    have rowOutside : 1 < r → 0 ≠ r ∧ 1 ≠ r := by omega
    have acases : a = 0 ∨ a = 1 ∨ a = 2 ∨ 2 < a := by omega
    have aoutside : 2 < a → a ≠ 0 ∧ a ≠ 1 ∧ a ≠ 2 := by omega
    have cases : w = 0 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 4 ∨ 4 < w := by omega
    have outside : 4 < w → w ≠ 0 ∧ w ≠ 1 ∧ w ≠ 2 ∧ w ≠ 3 ∧ w ≠ 4 := by omega
    rcases cases with rfl | rfl | rfl | rfl | rfl | hw <;> cases mode <;>
      rcases acases with rfl | rfl | rfl | ha2 <;>
      rcases rowCases with rfl | rfl | hr <;>
      simp_all [provenance, rowChange, history_body, records, insert, edit,
        CovenMerge.DataDependency.Provenance.sources]

def state (mode : Bool) := preparedState conversion raised (history mode) (valid mode)

theorem root_kept (mode : Bool) : retained (state mode) 0 0 = true := by
  rw [retained_eq]
  cases mode <;> rfl

theorem root_other_kept : retained (state true) 0 1 = true := by
  rw [retained_eq]
  rfl

theorem bad_dropped (mode : Bool) : retained (state mode) 1 1 = false := by
  apply dropped_stays_dropped
  cases mode <;> decide

theorem mixed_good (mode : Bool) : retained (state mode) 2 0 = true := by
  rw [retained_eq]
  cases mode <;>
    change (true && true && [0, 0].all (fun a => retained (state _) a 0)) = true
  all_goals simp [root_kept]

theorem mixed_bad (mode : Bool) : retained (state mode) 2 1 = false := by
  rw [retained_eq]
  cases mode
  · change (true && true && [1, 1].all (fun a => retained (state false) a 1)) = false
    simp [bad_dropped]
  · change (true && true && [0, 1].all (fun a => retained (state true) a 1)) = false
    simp [root_other_kept, bad_dropped]

theorem descendant_good (mode : Bool) : retained (state mode) 3 0 = true := by
  rw [retained_eq]
  cases mode <;>
    change (true && true && [0, 2].all (fun a => retained (state _) a 0)) = true
  all_goals simp [root_kept, mixed_good]

theorem descendant_bad (mode : Bool) : retained (state mode) 3 1 = false := by
  rw [retained_eq]
  cases mode
  · change (true && true && [1, 2].all (fun a => retained (state false) a 1)) = false
    simp [bad_dropped, mixed_bad]
  · change (true && true && [0, 2].all (fun a => retained (state true) a 1)) = false
    simp [root_other_kept, mixed_bad]

theorem good_conversion (mode : Bool) :
    conversionLosses conversion raised (history mode) (valid mode) 2 0 = none := by
  change (if retained (state mode) 2 0 then none else some _) = none
  rw [mixed_good]
  rfl

theorem bad_conversion (mode : Bool) :
    conversionLosses conversion raised (history mode) (valid mode) 2 1 =
      some (CovenMerge.LossCause.dependency 2 (if mode then [1] else [1, 1])) := by
  change (if retained (state mode) 2 1 then none else some (CovenMerge.LossCause.dependency 2 (droppedSources (state mode) 2 1))) = _
  rw [mixed_bad]
  cases mode
  · change some (CovenMerge.LossCause.dependency 2 ([1, 1].filter (fun a => !retained (state false) a 1))) = _
    simp [bad_dropped]
  · change some (CovenMerge.LossCause.dependency 2 ([0, 1].filter (fun a => !retained (state true) a 1))) = _
    simp [root_other_kept, bad_dropped]

/-- The same write converts its retained row and freezes its dependent row.
Its implicit read of write 1 does not determine schema eligibility. -/
theorem mixed_write (mode : Bool) :
    (receive conversion raised (history mode) (valid mode) 2).1.body =
      [edit 0 0 0 111,
       { edit 1 (if mode then 0 else 1) 1 22 with
         content := .frozen (.dependency 2 (if mode then [1] else [1, 1])) [22] }] := by
  change [convertChange (fun xs => xs.map (· + 100))
      (conversionLosses conversion raised (history mode) (valid mode) 2) (edit 0 0 0 11),
    convertChange (fun xs => xs.map (· + 100))
      (conversionLosses conversion raised (history mode) (valid mode) 2)
      (edit 1 (if mode then 0 else 1) 1 22)] = _
  simp only [convertChange, edit, good_conversion, bad_conversion]
  cases mode <;> rfl

theorem own_reads_do_not_exclude (mode : Bool) :
    reads (records mode 2).identity (records mode 1).identity = true ∧
    (receive conversion raised (history mode) (valid mode) 2).2 = .applied := by
  constructor
  · cases mode <;> decide
  · rw [receive_matches_evaluation, received_admission]
    cases mode <;> decide

theorem actual_setter_decides :
    retained (state true) 0 1 = true ∧ retained (state true) 1 1 = false ∧
    retained (state true) 2 1 = false ∧ retained (state true) 2 0 = true :=
  ⟨root_other_kept, bad_dropped true, mixed_bad true, mixed_good true⟩

theorem partial_write_chain (mode : Bool) :
    retained (state mode) 2 0 = true ∧ retained (state mode) 2 1 = false ∧
    retained (state mode) 3 0 = true ∧ retained (state mode) 3 1 = false :=
  ⟨mixed_good mode, mixed_bad mode, descendant_good mode, descendant_bad mode⟩

theorem direct_schema_loss (mode : Bool) :
    classifyChange (preparedHistory conversion raised (history mode))
      (preparedValid conversion raised (history mode) (valid mode)) raised 1
      (if mode then edit 1 0 0 21 else insert 1 20) = .frozen (.schemaChange 2 1) := by
  cases mode <;> decide

theorem delete_keeps_identity :
    conversionLosses conversion raised (history false) (valid false) 4 1 =
      some (CovenMerge.LossCause.dependency 4 [1, 2]) := by
  have dropped : retained (state false) 4 1 = false := by
    rw [retained_eq]
    change (true && true && [1, 2].all (fun a => retained (state false) a 1)) = false
    simp [bad_dropped, mixed_bad]
  change (if retained (state false) 4 1 then none else some (CovenMerge.LossCause.dependency 4 (droppedSources (state false) 4 1))) = _
  rw [dropped]
  change some (CovenMerge.LossCause.dependency 4 ([1, 2].filter (fun a => !retained (state false) a 1))) = _
  simp [bad_dropped, mixed_bad]

example : retained (state true) 2 1 = false ∧ retained (state true) 2 0 = true :=
  ⟨mixed_bad true, mixed_good true⟩

def secondRaise : View := ⟨some ⟨⟨11, [⟨0, 1⟩, ⟨0, 2⟩]⟩, 3⟩, none,
  captureDrops (preparedHistory conversion raised (history true))
    (preparedValid conversion raised (history true) (valid true)) raised [1]⟩

theorem covered_drop_stays_dropped :
    retained (preparedState ⟨3, .convert id⟩ secondRaise (history true) (valid true)) 1 1 = false := by
  apply dropped_stays_dropped
  decide

theorem covered_loss_keeps_cause :
    classifyChange (preparedHistory ⟨3, .convert id⟩ secondRaise (history true))
      (preparedValid ⟨3, .convert id⟩ secondRaise (history true) (valid true)) secondRaise 1
      (edit 1 0 0 21) = .frozen (.schemaChange 2 1) := by decide

theorem covered_snapshot_keeps_partial_write :
    let next := preparedState ⟨3, .convert id⟩ secondRaise (history true) (valid true)
    retained next 2 0 = true ∧ retained next 2 1 = false := by
  dsimp only
  have root0 : retained (preparedState ⟨3, .convert id⟩ secondRaise (history true) (valid true)) 0 0 = true := by
    rw [retained_eq]; rfl
  have root1 : retained (preparedState ⟨3, .convert id⟩ secondRaise (history true) (valid true)) 0 1 = true := by
    rw [retained_eq]; rfl
  constructor
  · rw [retained_eq]
    change (true && true && [0, 0].all (fun a => retained
      (preparedState ⟨3, .convert id⟩ secondRaise (history true) (valid true)) a 0)) = true
    simp [root0]
  · rw [retained_eq]
    change (true && true && [0, 1].all (fun a => retained
      (preparedState ⟨3, .convert id⟩ secondRaise (history true) (valid true)) a 1)) = false
    simp [root1, covered_drop_stays_dropped]

end CovenQueue.DataExamples
