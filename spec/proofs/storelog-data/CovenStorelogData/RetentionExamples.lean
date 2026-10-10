import CovenStorelogData.Retention
import CovenStorelogData.MigrationExamples
import CovenStorelogData.EntryExamples

namespace CovenStorelogData.RetentionExamples

open CovenStorelog
open CovenStorelog.Examples (entry)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- A retention-contract witness, conditional on the supplied snapshots being
loadable: raise 2, reset, raise 3, then a replacement raise 3 following the reset.
Receiving raise 2 defeats the reset and re-keeps the original raise 3. Both
raises remain effective, so reload needs both named prefixes. This model does
not establish SQL/schema reachability of the replacement publication. -/
def log : Log
  | 0 => entry 0 0 [] (.create "ana")
  | 1 => entry 0 0 [0] (.addDevice 0 1)
  | 2 => entry 0 0 [0, 1] (.addDevice 0 2)
  | 3 => entry 0 0 (List.range 3) (.raiseSchema 2 ⟨.store, 0⟩)
  | 4 => entry 0 1 (List.range 3) (.reset ⟨.store, 1⟩)
  | 5 => entry 0 2 (List.range 3) (.raiseSchema 3 ⟨.store, 2⟩)
  | _ => entry 0 2 (List.range 3 ++ [4, 5]) (.raiseSchema 3 ⟨.store, 3⟩)

def result (tail : List Nat) := resolve log 7 (entrySet (List.range 3 ++ tail))

theorem valid_log : Valid log 7 ∧
    CausalOrder log (List.range 3 ++ [5, 4, 6, 3]) ∧ CausalOrder log (List.range 7) := by
  refine ⟨validCheck_sound _ _ (by decide), ?_, ?_⟩
  all_goals apply causalCheck_sound _ .nil; decide

/-- The supplied contents have no row changes. Marker 0 belongs to raise 2,
1 to the first raise 3, 2 to its republication. Consumed coverage includes the
excluded marker. Schema admissibility is a separate, unproved precondition. -/
def snapshot (n : Nat) : StoredSnapshot (Fin 3) Unit :=
  ⟨⟨.store, n⟩, if n = 3 then 2 else n, ⟨0, .store⟩,
    if n = 3 then List.range 3 ++ [4, 5] else List.range 3,
    ⟨CovenMerge.St.init,
      if n = 0 then [0] else if n = 1 then [] else if n = 2 then [1] else [1, 2],
      [], []⟩⟩
def stored := [snapshot 0, snapshot 1, snapshot 2, snapshot 3]
def pruned := pruneSnapshots log 7 (result [5, 4, 6]) 2 [] (snapshot 3) stored

/-- The replacement covers the old snapshot, but cannot replace its named
prefix when replay makes the original raise effective again. -/
theorem snapshot_pin_contract_counterexample :
    5 ∈ (result [5]).kept ∧ 5 ∈ (result [5, 4, 6]).dropped ∧
    5 ∈ (result [5, 4, 6, 3]).kept ∧
    (preferredSnapshot (snapshotCandidates log 7 (result [5, 4, 6]) .store stored)).map
      (fun s => s.id) = some ⟨.store, 3⟩ ∧
    pruned.map (fun s => s.id) = [⟨.store, 0⟩, ⟨.store, 1⟩, ⟨.store, 3⟩] ∧
    boundaryInputsAvailable log 7 (result [5, 4, 6, 3]) .store stored = true ∧
    boundaryInputsAvailable log 7 (result [5, 4, 6, 3]) .store pruned = false := by decide

example : boundaryInputsAvailable log 7 (result [5, 4, 6, 3]) .store pruned ≠
    boundaryInputsAvailable log 7 (result [5, 4, 6, 3]) .store stored := by decide

/-- Retention only after all entries arrive preserves the original boundary. -/
example :
    (pruneSnapshots log 7 (result [3, 4, 5, 6]) 2 [] (snapshot 3) stored).map
      (fun s => s.id) = stored.map (fun s => s.id) := by decide

/-- A frozen loss still carries its value/setter, but file reference scanning
only visits merge/synced rows. This is the state produced when migration retires
a hidden file row; it is not an ordinary causal deletion of that row. -/
def frozenFile : Snapshot (Fin 1) Unit :=
  ⟨CovenMerge.St.init, [0],
    (CovenMerge.lossRecord
      (CovenMerge.step DeliveryExamples.writes CovenMerge.St.init 0)
      (observe DeliveryExamples.schema DeliveryExamples.writes EntryExamples.fileLog
        (EntryExamples.fileRun .owned [7]).state).view DeliveryExamples.row none).toList,
    []⟩

theorem frozen_file_counterexample :
    frozenFile.frozen.isEmpty = false ∧
    uploadedReferences [DeliveryExamples.row] [()] (fun (_ : Fin 1) => some 0) frozenFile = [] ∧
    mayDeleteFile 0 [] [] (some (uploadedReferences [DeliveryExamples.row] [()]
      (fun (_ : Fin 1) => some 0) frozenFile)) = true := by decide

example : mayDeleteFile 0 [] [] none = false := by decide

end CovenStorelogData.RetentionExamples
