import CovenQueue.Verdicts
import Lean

/-! Checked adapter for actual Rust-authored histories. The finite checks below
construct `DataDependency.Authored` and `DataValid`; expected source identities
come from the merge model's author-visible state, not from the supplied sources. -/
open Lean CovenMerge CovenMerge.DataDependency

private def generationOK {W R C : Type} (M : Writes W R C)
    (p : W → R → Provenance W C) (w : W) (r : R) : Prop :=
  match M.chg w r with
  | none => True
  | some ch => match (p w r).generation with
    | none => ch.gen = 0
    | some a => match M.chg a r with
      | none => False
      | some ca => ca.kind ≠ .upd ∧ ca.gen + 1 = ch.gen

private def setterOK {W R C : Type} (M : Writes W R C)
    (w a : W) (r : R) (c : C) : Prop :=
  match M.chg w r, M.chg a r with
  | some ch, some ca => ca.kind ≠ .del ∧ ca.sets c = true ∧ ca.inc = ch.inc
  | _, _ => False

private instance generationDecidable {W R C : Type} (M : Writes W R C)
    (p : W → R → Provenance W C) (w : W) (r : R) : Decidable (generationOK M p w r) := by
  unfold generationOK
  split
  · infer_instance
  · split
    · infer_instance
    · split <;> infer_instance

private instance setterDecidable {W R C : Type} (M : Writes W R C)
    (w a : W) (r : R) (c : C) : Decidable (setterOK M w a r c) := by
  unfold setterOK
  split <;> infer_instance

private def checkedAuthored {n rows cols : Nat} (M : Writes (Fin n) (Fin rows) (Fin cols))
    (p : Fin n → Fin rows → Provenance (Fin n) (Fin cols))
    (reads : ∀ w r a, a ∈ (p w r).sources → M.past w a = true)
    (generation : ∀ w r, generationOK M p w r)
    (setters : ∀ w r c a, (c,a) ∈ (p w r).setters → setterOK M w a r c) : Authored M where
  provenance := p
  read_source := reads
  generation_source w r ch found := by
    have h := generation w r
    unfold generationOK at h
    rw [found] at h
    cases source : (p w r).generation with
    | none => exact Or.inl ⟨by simpa [source] using h, rfl⟩
    | some a =>
      cases original : M.chg a r with
      | none => simp [source, original] at h
      | some ca =>
        have parts : ca.kind ≠ .upd ∧ ca.gen + 1 = ch.gen := by simpa [source, original] using h
        exact Or.inr ⟨a,ca,rfl,original,parts.1,parts.2⟩
  setter_source w r c a member := by
    have h := setters w r c a member
    unfold setterOK at h
    cases change : M.chg w r <;> cases original : M.chg a r <;> simp [change, original] at h
    rename_i ch ca
    exact ⟨ch, ca, rfl, rfl, h⟩

private def emptyRecord : CovenQueue.CheckedWrite :=
  ⟨⟨⟨⟨0,0⟩,0,[],[]⟩,0,.ordinary,[]⟩,none⟩

private def history (records : Array CovenQueue.CheckedWrite) : CovenQueue.History :=
  fun w => records[w]?.getD emptyRecord

private def dataOK (records : Array CovenQueue.CheckedWrite) : Prop :=
  ∀ w : Fin records.size,
    ((records[w]).record.body.map CovenQueue.RowChange.row).Nodup ∧
    ∀ ch ∈ (records[w]).record.body, ∀ a ∈ ch.provenance.sources,
      a < w.val ∧ (CovenQueue.rowChange (history records) a ch.row).isSome = true

private theorem checkedData (records : Array CovenQueue.CheckedWrite) (good : dataOK records) :
    CovenQueue.DataValid (history records) := by
  have outside (w : Nat) (h : ¬w < records.size) : (history records w).record.body = [] := by
    simp [history, Array.getElem?_eq_none (by omega : records.size ≤ w), emptyRecord]
  have inside (w : Nat) (h : w < records.size) : history records w = records[w] := by
    simp [history, Array.getElem?_eq_getElem h]
  have source (w r a : Nat) (mem : a ∈ (CovenQueue.provenance (history records) w r).sources) :
      a < w ∧ (CovenQueue.rowChange (history records) a r).isSome = true := by
    cases found : CovenQueue.rowChange (history records) w r with
    | none => simp [CovenQueue.provenance, found, Provenance.sources] at mem
    | some ch =>
      have row : ch.row = r := by simpa [CovenQueue.rowChange] using List.find?_some found
      have member := List.mem_of_find?_eq_some found
      change ch ∈ (history records w).record.body at member
      by_cases bound : w < records.size
      · rw [inside w bound] at member
        have hs : a ∈ ch.provenance.sources := by simpa [CovenQueue.provenance, found] using mem
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

private def bounded (limit value : Nat) : Except String (Fin limit) :=
  if h : value < limit then pure ⟨value,h⟩ else throw "identity outside history domain"

private def readKind (n : Nat) : Except String Kind :=
  match n with | 0 => pure .ins | 1 => pure .upd | 2 => pure .del | _ => throw "operation"

private def run (j : Json) : Except String Json := do
  let rows ← j.getObjValAs? Nat "rows"
  let cols ← j.getObjValAs? Nat "columns"
  let input ← j.getObjValAs? (Array Json) "writes"
  let n := input.size
  let parsed ← input.mapM fun w => do
    let past ← (← w.getObjValAs? (Array Nat) "past").mapM (bounded n)
    let stamp ← w.getObjValAs? Nat "timestamp"
    let body ← (← w.getObjValAs? (Array Json) "changes").mapM fun ch => do
      let row ← bounded rows (← ch.getObjValAs? Nat "row")
      let kind ← readKind (← ch.getObjValAs? Nat "kind")
      let gen ← ch.getObjValAs? Nat "generation"
      let columns ← (← ch.getObjValAs? (Array Nat) "columns").mapM (bounded cols)
      let source ← (← ch.getObjValAs? (Option Nat) "source").mapM (bounded n)
      let setters ← (← ch.getObjValAs? (Array (Nat × Nat)) "setters").mapM fun (c,a) => do
        pure (← bounded cols c, ← bounded n a)
      pure (row, (⟨kind,gen,columns.contains⟩ : Change (Fin cols)),
        (⟨source,setters.toList⟩ : Provenance (Fin n) (Fin cols)))
    pure (stamp,past,body)
  if length : parsed.size = n then do
   let get (w : Fin n) := parsed[w.val]' (by omega)
   let M : Writes (Fin n) (Fin rows) (Fin cols) := {
     ts := fun w => (get w).1
     past := fun w a => (get w).2.1.contains a
     chg := fun w r => ((get w).2.2.find? (·.1 == r)).map (·.2.1) }
   let p := fun (w : Fin n) (r : Fin rows) => match (get w).2.2.find? (·.1 == r) with
     | some ch => ch.2.2
     | none => (⟨none,[]⟩ : Provenance (Fin n) (Fin cols))
   let reads : Prop := ∀ (w : Fin n) (r : Fin rows) (a : Fin n), a ∈ (p w r).sources → M.past w a = true
   let generation : Prop := ∀ (w : Fin n) (r : Fin rows), generationOK M p w r
   let setters : Prop := ∀ (w : Fin n) (r : Fin rows) (c : Fin cols) (a : Fin n), (c,a) ∈ (p w r).setters → setterOK M w a r c
   letI : Decidable reads := by unfold reads; infer_instance
   letI : Decidable generation := by unfold generation; infer_instance
   letI : Decidable setters := by unfold setters; infer_instance
   let records : Array CovenQueue.CheckedWrite := parsed.map fun (stamp,past,body) =>
     ⟨⟨⟨⟨stamp,1⟩,stamp,past.toList.map (fun a => ⟨a.val,1⟩),[]⟩,0,.ordinary,
       body.toList.map fun (r,ch,p) => ⟨r.val,ch.gen,ch.kind,
         ⟨p.generation.map Fin.val,p.setters.map (fun (c,a) => (c.val,a.val))⟩,.values []⟩⟩,none⟩
   letI : Decidable (dataOK records) := by unfold dataOK; infer_instance
   let dataValid := if h : dataOK records then
     let _checked : CovenQueue.DataValid (history records) := checkedData records h
     true
     else false
   let earlier : Prop := ∀ (w a : Fin n), M.past w a = true → M.ts a < M.ts w
   letI : Decidable earlier := by unfold earlier; infer_instance
   if valid : reads ∧ generation ∧ setters ∧ earlier then do
     let authored := checkedAuthored M p valid.1 valid.2.1 valid.2.2.1
     let expected := (List.finRange n).flatMap fun w =>
       let visible := (List.finRange n).filter (M.past w)
       let st := visible.foldl (step M) St.init
       (get w).2.2.toList.map fun (r,ch,_) =>
         let setters := (List.finRange cols).filterMap fun c =>
           if ch.kind = Kind.del ∨ (ch.kind = Kind.upd ∧ ch.sets c = true) then
             (st.cell r c).map fun (a : Fin n) => (c.val,a.val) else none
         Json.mkObj [("write",toJson w.val),("row",toJson r.val),
           ("source",toJson ((st.genWrite r ch.gen).map Fin.val)),("setters",toJson setters),
           ("roles",toJson ((authored.provenance w r).sources.map Fin.val))]
     return Json.mkObj [("authored",toJson true),("dataValid",toJson dataValid),("expected",toJson expected)]
   else return Json.mkObj [("authored",toJson false),("dataValid",toJson dataValid)]
  else throw "history length changed"

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
