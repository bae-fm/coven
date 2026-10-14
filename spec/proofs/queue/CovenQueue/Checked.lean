import CovenQueue.Verdicts

/-! Finite histories supplied by differential runners. `dataOK` is a decidable
check over the supplied records; `checkedData` turns a successful check into
`DataValid` for the history those records denote. -/
namespace CovenQueue

def emptyRecord : CheckedWrite :=
  ⟨⟨⟨⟨0,0⟩,0,[],[]⟩,0,.ordinary,[]⟩,none⟩

/-- Indices past the supplied records hold no changes. -/
def recordHistory (records : Array CheckedWrite) : History :=
  fun w => records[w]?.getD emptyRecord

def dataOK (records : Array CheckedWrite) : Prop :=
  ∀ w : Fin records.size,
    ((records[w]).record.body.map RowChange.row).Nodup ∧
    ∀ ch ∈ (records[w]).record.body, ∀ a ∈ ch.provenance.sources,
      a < w.val ∧ (rowChange (recordHistory records) a ch.row).isSome = true

instance (records : Array CheckedWrite) : Decidable (dataOK records) := by
  unfold dataOK; infer_instance

theorem checkedData (records : Array CheckedWrite) (good : dataOK records) :
    DataValid (recordHistory records) := by
  have outside (w : Nat) (h : ¬w < records.size) : (recordHistory records w).record.body = [] := by
    simp [recordHistory, Array.getElem?_eq_none (by omega : records.size ≤ w), emptyRecord]
  have inside (w : Nat) (h : w < records.size) : recordHistory records w = records[w] := by
    simp [recordHistory, Array.getElem?_eq_getElem h]
  have source (w r a : Nat) (mem : a ∈ (provenance (recordHistory records) w r).sources) :
      a < w ∧ (rowChange (recordHistory records) a r).isSome = true := by
    cases found : rowChange (recordHistory records) w r with
    | none => simp [provenance, found, CovenMerge.DataDependency.Provenance.sources] at mem
    | some ch =>
      have row : ch.row = r := by simpa [rowChange] using List.find?_some found
      have member := List.mem_of_find?_eq_some found
      change ch ∈ (recordHistory records w).record.body at member
      by_cases bound : w < records.size
      · rw [inside w bound] at member
        have hs : a ∈ ch.provenance.sources := by simpa [provenance, found] using mem
        simpa [row] using (good ⟨w,bound⟩).2 ch member a hs
      · rw [outside w bound] at member
        contradiction
  exact {
    uniqueRows := fun w => by
      by_cases bound : w < records.size
      · rw [inside w bound]; exact (good ⟨w,bound⟩).1
      · rw [outside w bound]; simp
    earlier := fun w r a mem => (source w r a mem).1
    sourcePresent := fun w r a mem => (source w r a mem).2 }

end CovenQueue
