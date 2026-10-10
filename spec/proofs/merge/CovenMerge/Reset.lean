import CovenMerge.Snapshot

/-! Reset eligibility uses the kept entry in `store_log_read` (§19.3).
The shared snapshot merge handles positions, generations and dependencies. -/

namespace CovenMerge

structure ResetBoundary (W : Type) extends SnapshotBoundary W where
  entry : Nat

section
variable {W Row Col : Type} [DecidableEq W]

def resetRetains (B : ResetBoundary W) (read : W → Nat → Bool) : W → Bool :=
  snapshotRetains B.toSnapshotBoundary (fun w => read w B.entry)

def postReset (B : ResetBoundary W) (read : W → Nat → Bool) : W → Bool :=
  afterSnapshot B.toSnapshotBoundary (fun w => read w B.entry)

def resetWrites (M : Writes W Row Col) (B : ResetBoundary W)
    (read : W → Nat → Bool) : Writes W Row Col :=
  snapshotWrites M B.toSnapshotBoundary (fun w => read w B.entry)

abbrev ResetGenerations (M : Writes W Row Col) (B : ResetBoundary W)
    (read : W → Nat → Bool) : Prop :=
  SnapshotGenerations M B.toSnapshotBoundary (fun w => read w B.entry)

theorem reset_valid {M : Writes W Row Col} (valid : Valid M)
    (B : ResetBoundary W) (read : W → Nat → Bool) (generations : ResetGenerations M B read) :
    Valid (resetWrites M B read) := snapshot_valid valid B.toSnapshotBoundary _ generations

theorem reset_causal {M : Writes W Row Col} (B : ResetBoundary W) (read : W → Nat → Bool)
    {L : List W} (h : CausalFrom M (fun w => B.covered w = true) L) :
    CausalFrom (resetWrites M B read) (fun w => B.inputs w = true)
      (L.filter (postReset B read)) := snapshot_causal B.toSnapshotBoundary _ h

def reloadReset (M : Writes W Row Col) (B : ResetBoundary W) (read : W → Nat → Bool)
    (snapshot : St W Row Col) (received : List W) : St W Row Col :=
  reloadSnapshot M B.toSnapshotBoundary (fun w => read w B.entry) snapshot received

theorem reset_exact (M : Writes W Row Col) (B : ResetBoundary W) (read : W → Nat → Bool)
    (snapshot : St W Row Col) (received : List W) :
    reloadReset M B read snapshot received =
      (received.filter (fun w => !B.covered w && read w B.entry)).foldl
        (step (resetWrites M B read)) snapshot := rfl

theorem reset_ignored_no_effect (M : Writes W Row Col) (B : ResetBoundary W)
    (read : W → Nat → Bool) (snapshot : St W Row Col) (received : List W) (w : W)
    (unread : read w B.entry = false) :
    reloadReset M B read snapshot (w :: received) = reloadReset M B read snapshot received :=
  snapshot_ignored_no_effect M B.toSnapshotBoundary _ snapshot received w unread

theorem reset_covered_not_reapplied (M : Writes W Row Col) (B : ResetBoundary W)
    (read : W → Nat → Bool) (snapshot : St W Row Col) (received : List W) (w : W)
    (covered : B.covered w = true) :
    reloadReset M B read snapshot (w :: received) = reloadReset M B read snapshot received :=
  snapshot_covered_not_reapplied M B.toSnapshotBoundary _ snapshot received w covered

theorem post_reset_ignores_old_dependency (M : Writes W Row Col) (B : ResetBoundary W)
    (read : W → Nat → Bool) (w a : W)
    (uncovered : B.covered w = false) (adopted : read w B.entry = true)
    (old : resetRetains B read a = false) :
    postReset B read w = true ∧ (resetWrites M B read).past w a = false :=
  snapshot_ignores_old_dependency M B.toSnapshotBoundary _ w a uncovered adopted old

theorem reset_converges {M : Writes W Row Col} (valid : Valid M)
    (B : ResetBoundary W) (read : W → Nat → Bool) (generations : ResetGenerations M B read)
    {snapshot : St W Row Col}
    (closed : Closed (resetWrites M B read) (fun w => B.inputs w = true))
    (correct : IsSpec (resetWrites M B read) (fun w => B.inputs w = true) snapshot)
    {a b : List W} (ca : CausalFrom M (fun w => B.covered w = true) a)
    (cb : CausalFrom M (fun w => B.covered w = true) b)
    (same : ∀ w, postReset B read w = true → (w ∈ a ↔ w ∈ b)) :
    reloadReset M B read snapshot a = reloadReset M B read snapshot b :=
  snapshot_inputs_converge valid B.toSnapshotBoundary _ generations closed correct ca cb same

/-- Installing a reset replaces exactly its audience, including on the author. -/
def replaceAudience {A : Type} [DecidableEq A] (aud : Row → A) (target : A)
    (old replacement : St W Row Col) : St W Row Col where
  gen r := if aud r = target then replacement.gen r else old.gen r
  genWrite r := if aud r = target then replacement.genWrite r else old.genWrite r
  cell r := if aud r = target then replacement.cell r else old.cell r
  lost r := if aud r = target then replacement.lost r else old.lost r

theorem reset_every_device {A : Type} [DecidableEq A] (aud : Row → A) (target : A)
    (old : St W Row Col) (M : Writes W Row Col) (B : ResetBoundary W)
    (read : W → Nat → Bool) (snapshot : St W Row Col) (received : List W)
    (r : Row) (hr : aud r = target) :
    atRow (replaceAudience aud target old (reloadReset M B read snapshot received)) r =
      atRow (reloadReset M B read snapshot received) r := by
  simp [replaceAudience, atRow, hr]

theorem reset_other_audience {A : Type} [DecidableEq A] (aud : Row → A) (target : A)
    (old replacement : St W Row Col) (r : Row) (hr : aud r ≠ target) :
    atRow (replaceAudience aud target old replacement) r = atRow old r := by
  simp [replaceAudience, atRow, hr]

end
end CovenMerge
