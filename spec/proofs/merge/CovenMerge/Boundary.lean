import CovenMerge.Accounting

/-! The selected boundary snapshot and incoming parts, together (§8, §17.1,
§19.3). Selection of the kept store-log entry and schema conversion verdicts
are inputs; the effects below do not depend on the device or arrival order. -/

namespace CovenMerge

structure MergeSnapshot (W Row Col : Type) where
  merged : St W Row Col
  frozen : List (FrozenLoss W Row Col)

structure BoundaryDevice (W Row Col : Type) where
  current : Device W Row Col
  snapshotFrozen : List (FrozenLoss W Row Col)
  excluded : W → Row → Option (FrozenLoss W Row Col)

section
variable {W Row Col K : Type} [DecidableEq W] [DecidableEq Row]
  [DecidableEq Col] [DecidableEq K]

def boundaryEligible (reset : Option Nat) (read : W → Nat → Bool)
    (schema : W → SchemaDisposition) : W → Bool :=
  fun w => resetAllows reset read w && decide (schema w = .eligible)

theorem applies_iff_post (B : SnapshotBoundary W) (reset : Option Nat) (read : W → Nat → Bool)
    (schema : W → SchemaDisposition) (w : W) :
    disposition B reset read schema w = .apply ↔ afterSnapshot B (boundaryEligible reset read schema) w = true := by
  cases hc : B.covered w <;> cases hr : resetAllows reset read w <;>
    cases hs : schema w <;> simp [disposition, afterSnapshot, boundaryEligible, hc, hr, hs]

/-- Values of excluded row changes are supplied as written, including a
delete's old scalars. An unchanged row has no loss. -/
def receivedLoss (M : Writes W Row Col) (B : SnapshotBoundary W) (reset : Option Nat) (read : W → Nat → Bool)
    (schema : W → SchemaDisposition) (values : W → Row → Col → Option W)
    (received : List W) (w : W) (r : Row) : Option (FrozenLoss W Row Col) :=
  if w ∈ received then
    (M.chg w r).bind fun ch => boundaryLoss B reset read schema w r ch.gen (values w r)
  else none

def boundaryDevice (M : Writes W Row Col) (B : SnapshotBoundary W) (reset : Option Nat) (read : W → Nat → Bool)
    (schema : W → SchemaDisposition) (values : W → Row → Col → Option W)
    (inputs : St W Row Col → EntryInputs Row K) (snapshot : MergeSnapshot W Row Col)
    (received : List W) : BoundaryDevice W Row Col :=
  let st := reloadSnapshot M B (boundaryEligible reset read schema) snapshot.merged received
  let v := entryView (inputs st)
  ⟨⟨st, v, lossRecord st v⟩, snapshot.frozen, receivedLoss M B reset read schema values received⟩

theorem boundary_converges {M : Writes W Row Col} (valid : Valid M)
    (B : SnapshotBoundary W) (reset : Option Nat) (read : W → Nat → Bool) (schema : W → SchemaDisposition)
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    (snapshot : MergeSnapshot W Row Col)
    (generations : SnapshotGenerations M B (boundaryEligible reset read schema))
    (closed : Closed (snapshotWrites M B (boundaryEligible reset read schema)) (fun w => B.inputs w = true))
    (correct : IsSpec (snapshotWrites M B (boundaryEligible reset read schema))
      (fun w => B.inputs w = true) snapshot.merged)
    {a b : List W} (ca : CausalFrom M (fun w => B.covered w = true) a)
    (cb : CausalFrom M (fun w => B.covered w = true) b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    boundaryDevice M B reset read schema values inputs snapshot a =
      boundaryDevice M B reset read schema values inputs snapshot b := by
  have merged := snapshot_inputs_converge valid B (boundaryEligible reset read schema) generations closed correct
    ca cb (fun w _ => same w)
  have losses : receivedLoss M B reset read schema values a = receivedLoss M B reset read schema values b := by
    funext w r
    simp only [receivedLoss, same w]
  simp only [boundaryDevice, merged, losses]

theorem boundary_rule_order {M : Writes W Row Col} (valid : Valid M)
    (B : SnapshotBoundary W) (reset : Option Nat) (read : W → Nat → Bool) (schema : W → SchemaDisposition)
    (inputs : St W Row Col → EntryInputs Row K) (snapshot : MergeSnapshot W Row Col)
    (generations : SnapshotGenerations M B (boundaryEligible reset read schema))
    (closed : Closed (snapshotWrites M B (boundaryEligible reset read schema)) (fun w => B.inputs w = true))
    (correct : IsSpec (snapshotWrites M B (boundaryEligible reset read schema))
      (fun w => B.inputs w = true) snapshot.merged)
    {a b : List W} (ca : CausalFrom M (fun w => B.covered w = true) a)
    (cb : CausalFrom M (fun w => B.covered w = true) b) (same : ∀ w, w ∈ a ↔ w ∈ b)
    {D E : Row → Bool}
    (hd : let I := (inputs (reloadSnapshot M B (boundaryEligible reset read schema) snapshot.merged a)).erase
      Stratified I.rows (FiresP (fires I)) (rivalBefore I) (start I) D)
    (he : let I := (inputs (reloadSnapshot M B (boundaryEligible reset read schema) snapshot.merged b)).erase
      Stratified I.rows (FiresP (fires I)) (rivalBefore I) (start I) E) : D = E := by
  have merged := snapshot_inputs_converge valid B (boundaryEligible reset read schema) generations closed correct
    ca cb (fun w _ => same w)
  simp only at hd he
  rw [any_order_removal _ hd, any_order_removal _ he, merged]

/-- Reset installs the snapshot's frozen records as well as its merge rows.
Pre-reset effects cannot reappear as schema losses. -/
theorem boundary_reset_no_loss (M : Writes W Row Col) (B : SnapshotBoundary W) (reset : Option Nat)
    (read : W → Nat → Bool) (schema : W → SchemaDisposition)
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    (snapshot : MergeSnapshot W Row Col) (received : List W) (w : W)
    (outside : B.covered w = false) (unread : resetAllows reset read w = false) :
    (boundaryDevice M B reset read schema values inputs snapshot received).snapshotFrozen = snapshot.frozen ∧
    ∀ r, (boundaryDevice M B reset read schema values inputs snapshot received).excluded w r = none := by
  refine ⟨rfl, ?_⟩
  intro r
  simp only [boundaryDevice, receivedLoss]
  split
  · cases hc : M.chg w r <;> simp [boundaryLoss, disposition, outside, unread]
  · rfl

theorem excluded_values_recorded (M : Writes W Row Col) (B : SnapshotBoundary W) (reset : Option Nat)
    (read : W → Nat → Bool) (schema : W → SchemaDisposition)
    (values : W → Row → Col → Option W) (received : List W) (w : W) (r : Row)
    (ch : Change Col) (version : Nat) (mem : w ∈ received) (change : M.chg w r = some ch)
    (outside : B.covered w = false) (adopted : resetAllows reset read w = true)
    (excluded : schema w = .excluded version) :
    receivedLoss M B reset read schema values received w r =
      some (excludedLoss w r ch.gen (values w r) version) := by
  simp [receivedLoss, mem, change, boundaryLoss, disposition, outside, adopted, excluded]

/-- Value accounting for the state actually produced by boundary replay,
including migration deletes represented in its selected snapshot. -/
theorem boundary_value_accounted {M : Writes W Row Col} (valid : Valid M)
    (B : SnapshotBoundary W) (reset : Option Nat) (read : W → Nat → Bool) (schema : W → SchemaDisposition)
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    (snapshot : MergeSnapshot W Row Col)
    (generations : SnapshotGenerations M B (boundaryEligible reset read schema))
    (closed : Closed (snapshotWrites M B (boundaryEligible reset read schema)) (fun w => B.inputs w = true))
    (correct : IsSpec (snapshotWrites M B (boundaryEligible reset read schema))
      (fun w => B.inputs w = true) snapshot.merged)
    (received : List W) (causal : CausalFrom M (fun w => B.covered w = true) received)
    (present : ∀ st r, (inputs st).present r = decide (st.gen r % 2 = 1))
    (r : Row) (c : Col) (w : W) (k : Nat)
    (setter : Setter (snapshotWrites M B (boundaryEligible reset read schema))
      (fun w => B.inputs w = true ∨ w ∈ received.filter (afterSnapshot B (boundaryEligible reset read schema))) r c w k) :
    let st := (boundaryDevice M B reset read schema values inputs snapshot received).current.merged
    ValueAccounted (snapshotWrites M B (boundaryEligible reset read schema))
      (fun w => B.inputs w = true ∨ w ∈ received.filter (afterSnapshot B (boundaryEligible reset read schema)))
      st (inputs st) r c w k := by
  have v := snapshot_valid valid B (boundaryEligible reset read schema) generations
  have result := foldl_isSpec v closed correct (snapshot_causal B (boundaryEligible reset read schema) causal)
  exact value_accounted v result.2 result.1 _ (present _) setter

end
end CovenMerge
