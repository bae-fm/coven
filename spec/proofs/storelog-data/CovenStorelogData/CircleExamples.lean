import CovenStorelogData.Durability
import CovenStorelogData.Convergence
import CovenStorelogData.Operations

namespace CovenStorelogData.CircleExamples

open CovenStorelog
open CovenStorelog.Examples (entry)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- Ana and Ben share Gifts; Ana removes Ben with the earlier stamp while
Ben deletes the circle. Each operation read only the common prefix. -/
def log : Log
  | 0 => entry 0 0 [] (.create "S3")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "S3")
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 0 0 [0, 1, 2] (.makeCircle 0 "Gifts")
  | 4 => entry 0 0 [0, 1, 2, 3] (.addToCircle 0 1)
  | 5 => entry 0 0 (List.range 5) (.removeFromCircle 0 1)
  | _ => entry 1 1 (List.range 5) (.deleteCircle 0)

theorem valid_log : Valid log 7 := validCheck_sound _ _ (by decide)

def note (n : Nat) : Row := ⟨0, n, .circle 0⟩
def known (r : Row) : Bool := r == note 7 || r == note 8

/-- Write 0 inserts notes 7 and 8. Write 1 inserts note 9 concurrently with
write 2, Ben's explicit deletion of the rows he knows. -/
def writes : CovenMerge.Writes (Fin 3) Row Unit where
  ts w := w.val
  past w a := w.val != 0 && a.val == 0
  chg w r :=
    if w.val = 0 then if known r then some ⟨.ins, 0, fun _ => true⟩ else none
    else if w.val = 1 then if r = note 9 then some ⟨.ins, 0, fun _ => true⟩ else none
    else if known r then some ⟨.del, 1, fun _ => false⟩ else none

theorem valid_writes : CovenMerge.Valid writes where
  ts_inj _ _ h := Fin.eq_of_val_eq h
  past_ts w a h := by
    simp [writes] at h ⊢
    omega
  gen_seen w r ch h := by
    simp only [writes] at h
    split at h
    · split at h <;> cases h
      exact Or.inl rfl
    · split at h
      · split at h <;> cases h
        exact Or.inl rfl
      · split at h <;> cases h
        rename_i hn hi hk
        exact Or.inr ⟨0, ⟨.ins, 0, fun _ => true⟩,
          by simp [writes, hn], by simp [writes, hk], by decide, rfl⟩
  parity w r ch h := by
    simp only [writes] at h
    split at h
    · split at h <;> cases h
      decide
    · split at h
      · split at h <;> cases h
        decide
      · split at h <;> cases h
        decide

def schema : Schema (Fin 3) Unit Nat where
  rows := [note 7, note 8, note 9]
  refs _ _ := []
  checkFails _ _ := false
  claims _ _ := []

def state (received : List Nat) (order : List (Fin 3)) : CovenStorelogData.State (Fin 3) Unit :=
  ⟨⟨entrySet received, resolve log 7 (entrySet received)⟩,
    order.foldl (CovenMerge.step writes) CovenMerge.St.init⟩

def beforeDrop := state [0, 1, 2, 3, 4, 6] [0, 2, 1]
def afterDrop := CovenStorelogData.step writes log 7 beforeDrop (.entry 5)

/-- §14.7: ordinary deletes remove the known notes; the concurrent note
retains its value in a whole-row loss naming the deleted circle. -/
theorem deletion_example :
    (observe schema writes log beforeDrop).view.shown (note 7) = false ∧
    beforeDrop.data.gen (note 7) = 2 ∧
    beforeDrop.data.gen (note 8) = 2 ∧
    beforeDrop.data.cell (note 9) () = some 1 ∧
    (observe schema writes log beforeDrop).view.rules (note 9) = [.deletedCircle] ∧
    ((observe schema writes log beforeDrop).losses (note 9) none).isSome = true := by decide

/-- Counterexample to §9's unqualified claim that the circle's rows return:
the rule reverses; the causal row delete does not. One known note suffices. -/
theorem gifts_counterexample :
    deletedCircle log afterDrop.log.result 0 = false ∧
    afterDrop.log.result.dropped = [6] ∧
    (observe schema writes log afterDrop).view.shown (note 7) = false ∧
    afterDrop.data.gen (note 7) = 2 ∧
    afterDrop.data.cell (note 7) () = none ∧
    afterDrop.data.lost (note 7) () 0 = none ∧
    writes.past 2 0 = true ∧
    (observe schema writes log afterDrop).view.shown (note 9) = true := by decide

/-- The prose failure needs only one row and its causal delete. -/
example :
    let one := CovenMerge.project writes id (fun r => r == note 7)
    let s : CovenStorelogData.State (Fin 3) Unit :=
      ⟨afterDrop.log, [0, 2].foldl (CovenMerge.step one) CovenMerge.St.init⟩
    (observe { schema with rows := [note 7] } one log s).view.shown (note 7) = false ∧
    s.data.cell (note 7) () = none ∧ s.data.lost (note 7) () 0 = none := by decide

/-- Ben cannot redo the dropped deletion after Ana has removed him. Its
ordinary write has nevertheless already taken effect. -/
theorem retry_checks_current_membership :
    pollDeletion (W := Fin 3) afterDrop.log.result (.published 6) = .ready ∧
    retryDeletion (W := Fin 3) afterDrop.log.result.state 1 0 = .pending ∧
    pollDeletion (W := Fin 3) afterDrop.log.result .finished = .finished := by decide

def beforeDelete : CovenMerge.St (Fin 3) Row Unit :=
  [0].foldl (CovenMerge.step writes) CovenMerge.St.init

theorem deletion_matches_rows : IsDeletion writes beforeDelete 0 (some 2) := by
  intro r
  by_cases h7 : r = note 7
  · subst r; rfl
  by_cases h8 : r = note 8
  · subst r; rfl
  simp [writes, known, h7, h8, deleteChanges, beforeDelete, CovenMerge.step,
    CovenMerge.genStep, CovenMerge.St.init]

theorem deletion_write_precedes_entry :
    (commitDeletion writes beforeDelete 0 (some 2) deletion_matches_rows).1.gen
        (note 7) = 2 ∧
    PublishDeletion (Deletion.committed (some (2 : Fin 3))) .uploaded ∧
    PublishDeletion (W := Fin 3) .uploaded (.published 6) :=
  ⟨by decide, .stored 2, .entry 6⟩

/-- Rechecking authority uses the write's past. Removal does not revoke a
write made by Ben's registered device against the common prefix. -/
theorem removed_author_still_counts :
    writeAuthority log 7 ⟨1, 1, List.range 5⟩ = true := by decide

/-- Other-audience selection is recomputed after resurrection too. The
store's same-key row still beats the circle row; no stale deletion rule stays. -/
def storeNote : Row := ⟨0, 9, .store⟩

def withStoreWrites : CovenMerge.Writes (Fin 3) Row Unit :=
  { writes with chg w r :=
    if w = 0 ∧ r = storeNote then some ⟨.ins, 0, fun _ => true⟩ else writes.chg w r }

def withStoreRow : CovenMerge.St (Fin 3) Row Unit :=
  [0, 2, 1].foldl (CovenMerge.step withStoreWrites) CovenMerge.St.init

theorem other_audience_after_drop :
    let i := inputs { schema with rows := storeNote :: schema.rows } withStoreWrites log
      afterDrop.log.result withStoreRow
    (CovenMerge.view i).shown storeNote = true ∧
    (CovenMerge.view i).rules (note 9) = [.otherAudience] := by decide

/-- The two actual causal arrival orders produce the same data, including
losses, even though only one device temporarily keeps the circle deletion. -/
theorem opposite_arrivals :
    (observe schema writes log (state [0, 1, 2, 3, 4, 5, 6] [0, 1, 2])).view.shown =
      (observe schema writes log afterDrop).view.shown := by
  funext r
  simp only [observe, state, afterDrop, beforeDrop, CovenStorelogData.step]
  have data : ([0, 1, 2] : List (Fin 3)).foldl (CovenMerge.step writes) CovenMerge.St.init =
      [0, 2, 1].foldl (CovenMerge.step writes) CovenMerge.St.init := by
    apply CovenMerge.merge_converges valid_writes
    · apply CovenMerge.CausalOrder.snoc (w := 2)
        (CovenMerge.CausalOrder.snoc (w := 1)
          (CovenMerge.CausalOrder.snoc (w := 0) .nil (by simp) ?_) (by decide) ?_) (by decide)
      all_goals intro a h; simp [writes] at h <;> simp_all
    · apply CovenMerge.CausalOrder.snoc (w := 1)
        (CovenMerge.CausalOrder.snoc (w := 2)
          (CovenMerge.CausalOrder.snoc (w := 0) .nil (by simp) ?_) (by decide) ?_) (by decide)
      all_goals intro a h; simp [writes] at h <;> simp_all
    · intro w; simp [or_comm]
  rw [data]
  congr 3

end CovenStorelogData.CircleExamples
