import CovenStorelogData.Snapshots
import CovenStorelogData.Durability

namespace CovenStorelogData.MigrationExamples

open CovenStorelog
open CovenStorelog.Examples (entry)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- Both devices have migrated the same note. Ana's earlier raise names her
snapshot; Ben's later concurrent raise names his own. -/
def log : Log
  | 0 => entry 0 0 [] (.create "S3")
  | 1 => entry 0 0 [0] (.addMember 1 .member "S3")
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 0 0 [0, 1, 2] (.raiseSchema 2 ⟨.store, 0⟩)
  | _ => entry 1 1 [0, 1, 2] (.raiseSchema 2 ⟨.store, 1⟩)

theorem valid_log : Valid log 5 := validCheck_sound _ _ (by decide)

def note : Row := ⟨0, 0, .store⟩

/-- Column 0 is title; column 1 is body. Write 0 inserts Groceries/body.
Ben's ordinary write 1 sets title to Shopping. The identical migration
UPDATE notes SET body=title sets body to Groceries in write 2 (Ana) and
Shopping in write 3 (Ben). Ana has not read either of Ben's writes. -/
def writes : CovenMerge.Writes (Fin 4) Row (Fin 2) where
  ts w := w.val
  past w a := (w.val != 0 && a.val == 0) || (w.val == 3 && a.val == 1)
  chg w r := if r = note then
    if w.val = 0 then some ⟨.ins, 0, fun _ => true⟩
    else some ⟨.upd, 1, fun c => if w.val = 1 then c.val == 0 else c.val == 1⟩
    else none

theorem valid_writes : CovenMerge.Valid writes where
  ts_inj _ _ h := Fin.eq_of_val_eq h
  past_ts := by decide
  gen_seen w r ch h := by
    simp only [writes] at h
    split at h
    · rename_i hr
      split at h
      · cases h; exact Or.inl rfl
      · rename_i hn
        cases h
        exact Or.inr ⟨0, ⟨.ins, 0, fun _ => true⟩,
          by simp [writes, hn], by simp [writes, hr], by decide, rfl⟩
    · cases h
  parity w r ch h := by
    simp only [writes] at h
    split at h
    · split at h <;> cases h <;> simp
    · cases h

def ana : Snapshot (Fin 4) (Fin 2) :=
  ⟨[0, 2].foldl (CovenMerge.step writes) CovenMerge.St.init, [0, 2], [], []⟩
def ben : Snapshot (Fin 4) (Fin 2) :=
  ⟨[0, 1, 3].foldl (CovenMerge.step writes) CovenMerge.St.init, [0, 1, 3], [], []⟩
def headers (w : Fin 4) : Header (Fin 4) :=
  ⟨if w.val = 0 then 1 else 2, (List.finRange 4).filter (writes.past w),
   if w.val < 2 then .apply else .migration⟩
def result := resolve log 5 (entrySet (List.range 5))
def snapshots (id : SnapshotId) : Snapshot (Fin 4) (Fin 2) :=
  if id.number = 0 then ana else ben
def afterReload : Reloaded (Fin 4) (Fin 2) :=
  reload writes headers .store [.schema 2 .store ana.included] ana [1, 3]
def schema : Schema (Fin 4) (Fin 2) Nat := ⟨[note], fun _ _ => [], fun _ _ => false, fun _ _ => []⟩

theorem selected_winner :
    result.dropped = [4] ∧ selectedSnapshot log 5 result .store = some ⟨.store, 0⟩ ∧
    ((reloadSelected writes headers log 5 result .store snapshots [1, 3]).map
      (fun s => (s.data.cell note 0, s.data.cell note 1, s.rejected.isEmpty))) =
      some (some 1, some 2, true) := by decide

/-- The migration really wrote this value locally; neither the winning
migration nor any other write has read it. Reload records no write loss,
cell loss or current removal rule explaining its disappearance. -/
theorem durability_counterexample :
    ben.data.cell note 1 = some 3 ∧
    afterReload.data.cell note 0 = some 1 ∧ afterReload.data.cell note 1 = some 2 ∧
    afterReload.data.lost note 1 3 = none ∧ afterReload.frozen.isEmpty = true ∧
    afterReload.rejected = [] ∧ (∀ w, writes.past w 3 = false) ∧
    (CovenMerge.view (inputs schema writes log result afterReload.data)).rules note = [] := by decide

example : ¬ Accounted writes (fun _ => True) afterReload.data
    (inputs schema writes log result afterReload.data) note 1 3 1 := by
  intro h
  rcases h with h | ⟨w, _, read⟩ | ⟨x, loss⟩ | h
  · have : afterReload.data.cell note 1 ≠ some 3 := by decide
    exact this h.1
  · have : writes.past w 3 = false := by exact durability_counterexample.2.2.2.2.2.2.1 w
    simp [this] at read
  · have : afterReload.data.lost note 1 3 = none := by decide
    simp [this] at loss
  · have : afterReload.data.cell note 1 ≠ some 3 := by decide
    exact this h.1

/-- Reset losses have an explicit cause, unlike the migration marker above. -/
example : excludes (2 : Nat) 1 .store [1] (.reset 5 .store [0]) = true ∧
    excludes (2 : Nat) 1 .store [] (.reset 5 .store [0]) = false ∧
    excludes (2 : Nat) 1 .store [0, 1] (.reset 5 .store [0]) = false := by decide

def resets : Log
  | 1 => entry 0 0 [0] (.addMember 1 .admin "S3")
  | 3 => entry 0 0 [0, 1, 2] (.reset ⟨.store, 0⟩)
  | 4 => entry 1 1 [0, 1, 2] (.reset ⟨.store, 1⟩)
  | n => log n

theorem competing_reset_finishes :
    let r := resolve resets 5 (entrySet (List.range 5))
    selectedSnapshot resets 5 r .store = some ⟨.store, 0⟩ ∧
    r.dropped = [4] ∧ resetFinished r 4 = true ∧
    publicationOrder (Publication.migration (2 : Fin 4) 2 [⟨.store, 0⟩]) =
      [.migrate 2, .uploadSnapshot ⟨.store, 0⟩, .publish (.raiseSchema 2 ⟨.store, 0⟩)] := by decide

end CovenStorelogData.MigrationExamples
