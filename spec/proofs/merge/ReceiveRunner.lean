import CovenMerge.DataDependency
import Lean

open Lean

/-- One row's changes, indexed by authored rank: their own admission and the
generation and replaced-setter sources they used. The caller has checked that
every source precedes its change; `earlier` restates it for the model. -/
private def precede (w : Nat) (p : CovenMerge.DataDependency.Provenance Nat Nat) :
    CovenMerge.DataDependency.Provenance Nat Nat :=
  ⟨match p.generation with
    | some g => if g < w then some g else none
    | none => none,
   p.setters.filter (·.2 < w)⟩

private theorem precede_earlier (w : Nat) (p : CovenMerge.DataDependency.Provenance Nat Nat) :
    ∀ a ∈ (precede w p).sources, a < w := by
  intro a mem
  simp only [CovenMerge.DataDependency.Provenance.sources, precede, List.mem_append,
    List.mem_map, List.mem_filter] at mem
  rcases mem with h | ⟨x, ⟨_, lt⟩, rfl⟩
  · cases hg : p.generation with
    | none => simp [hg] at h
    | some g =>
      by_cases lt : g < w
      · simp [hg, lt] at h; omega
      · simp [hg, lt] at h
  · simpa using lt

private def dependencyState (table : Array (Bool × CovenMerge.DataDependency.Provenance Nat Nat)) :
    CovenMerge.DataDependency.State Nat Unit Nat where
  rank := id
  present w _ := decide (w < table.size)
  base w _ := (table[w]?.map (·.1)).getD false
  provenance w _ := precede w ((table[w]?.map (·.2)).getD ⟨none, []⟩)
  earlier w _ a mem := precede_earlier w _ a mem

/-- `retained` and `droppedSources` for every change of one row. -/
private def dependency (j : Json) : Except String Json := do
  let changes ← j.getObjValAs? (Array Json) "changes"
  let table ← changes.mapM fun c => do
    let generation ← match c.getObjVal? "generation" with
      | .ok .null => pure none
      | .ok value => some <$> fromJson? value
      | .error _ => pure none
    let setters ← c.getObjValAs? (Array Nat) "setters"
    pure (← c.getObjValAs? Bool "base",
      (⟨generation, setters.toList.map fun s => (0, s)⟩ :
        CovenMerge.DataDependency.Provenance Nat Nat))
  for h : w in [0:table.size] do
    if (table[w]'(Membership.get_elem_helper h rfl)).2.sources.any (· ≥ w) then
      throw "a source must precede the change that used it"
  let s := dependencyState table
  pure (toJson ((List.range table.size).map fun w =>
    Json.mkObj [("retained", toJson (CovenMerge.DataDependency.retained s w ())),
      ("dropped", toJson (CovenMerge.DataDependency.droppedSources s w ()))]))

private def run (j : Json) : Except String Json := do
  let cases ← j.getArr?
  let results ← cases.mapM fun c => do
    match ← c.getObjValAs? String "check" with
    | "dependency" => dependency c
    | check => throw s!"unknown receive check {check}"
  pure (toJson results)

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
