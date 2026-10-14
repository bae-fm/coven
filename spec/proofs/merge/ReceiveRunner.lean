import CovenMerge.ReceivePositions
import CovenMerge.Retirement
import CovenMerge.DataDependency
import Lean

open Lean CovenMerge.Receive

private def position (j : Json) : Except String CovenMerge.Receive.Position := do
  let pair ← j.getArr?
  if pair.size != 2 then throw "a position must have device and number"
  let device ← pair[0]!.getNat?
  let number ← pair[1]!.getNat?
  if number = 0 then throw "positions start at one"
  pure ⟨device, number⟩

/-- Execute the premises of the publication-bound theorem for a completed
observation. A phase beginning at finality sees retirement. -/
private def publication (j : Json) : Except String Bool := do
  let finalAt ← j.getObjValAs? Nat "final"
  let start ← j.getObjValAs? Nat "start"
  let sent ← j.getObjValAs? Nat "sent"
  let stored ← j.getObjValAs? Nat "stored"
  let observed := decide (finalAt ≤ start)
  if allowed : observed = false ∧ sent < start + 300 ∧ stored ≤ sent + 86400 then
    have _bound := send_gate_publication_bound finalAt start sent stored observed
      (fun h => by simp [observed, h]) allowed.1 allowed.2.1 allowed.2.2
    if stored < finalAt + 300 + 86400 then return true
    else throw "publication theorem contradicted"
  else return false

private def retirement (j : Json) : Except String Retirement := do
  pure ⟨← j.getObjValAs? Nat "device", ← j.getObjValAs? Nat "entry", ← j.getObjValAs? Nat "stored"⟩

/-- One write's own retirement window: its device and first publication time
against the kept retirements. The entries its recorded past read are carried
for the snapshot boundary; the window ignores them (`window_ignores_reads`). -/
private def policy (j : Json) (retirements : List Retirement) : Except String (Policy Unit) := do
  let device ← j.getObjValAs? Nat "device"
  let stored ← j.getObjValAs? Nat "stored"
  let read ← j.getObjValAs? (Array Nat) "read"
  pure {
    boundary := ⟨fun _ => false, fun _ => false, by simp⟩
    logs := ⟨fun _ => device, fun _ => 1⟩
    reset := none
    readEntry := fun _ e => read.contains e
    schema := fun _ => .eligible
    retirements
    stored := fun _ => stored
    viewRetired := fun _ => false }

/-- `withinWindow` under the kept retirements, and, when a later kept set is
given with a final anchor, the same verdict under it (`retirement_verdict_final`). -/
private def window (j : Json) : Except String Json := do
  let kept ← (← j.getObjValAs? (Array Json) "kept").toList.mapM retirement
  let verdict := withinWindow (← policy j kept) ()
  match j.getObjVal? "later" with
  | .ok later =>
    let later ← (← later.getArr?).toList.mapM retirement
    pure (toJson [verdict, withinWindow (← policy j later) ()])
  | .error _ => pure (toJson verdict)

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
    if let .ok "publication" := c.getObjValAs? String "check" then
      return toJson (← publication c)
    if let .ok "window" := c.getObjValAs? String "check" then
      return ← window c
    if let .ok "dependency" := c.getObjValAs? String "check" then
      return ← dependency c
    let w ← position (← c.getObjVal? "write")
    let past ← (← c.getObjValAs? (Array Json) "past").mapM position
    let passed ← (← c.getObjValAs? (Array Json) "passed").mapM position
    pure (toJson (positionsReady w past.toList passed.toList))
  pure (toJson results)

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
