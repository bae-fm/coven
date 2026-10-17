import CovenMerge.Accounting

/-! The selected boundary snapshot and incoming parts, together (§8, §17.1,
§19.3). Selection of the applied sequence boundary and schema conversion verdicts
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

/-! Per-change boundary application. The whole-write boundary functions above
and this path share the same selected boundary inputs. This path carries actual data
provenance and derives validity after drops rather than assuming witnesses. -/
namespace Data

variable {W Row Col K : Type} [DecidableEq W]

def loss (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (base : W → Row → Bool) (direct : W → Row → Option (FrozenLoss W Row Col))
    (values : W → Row → Col → Option W) (received : List W)
    (w : W) (r : Row) : Option (FrozenLoss W Row Col) :=
  if w ∈ received then
    (M.chg w r).bind fun ch =>
      if base w r then
        if DataDependency.retained (DataDependency.authoredState M valid p base) w r then none
        else some (dependentLoss (DataDependency.authoredState M valid p base) w r ch.gen (values w r))
      else direct w r
  else none

def merged (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (base : W → Row → Bool) (snapshot : St W Row Col) (received : List W) : St W Row Col :=
  received.foldl (step (DataDependency.effective M valid p base)) snapshot

theorem isSpec (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (base : W → Row → Bool) (C : W → Prop)
    (closed : Closed M C) (snapshot : St W Row Col)
    (correct : IsSpec (DataDependency.effective M valid p base) C snapshot)
    (received : List W) (causal : CausalFrom M C received) :
    IsSpec (DataDependency.effective M valid p base) (fun w => C w ∨ w ∈ received)
      (merged M valid p base snapshot received) ∧
    Closed (DataDependency.effective M valid p base) (fun w => C w ∨ w ∈ received) :=
  foldl_isSpec (DataDependency.effective_valid M valid p base) closed correct
    (DataDependency.causal_from M valid p base causal)

theorem converges (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (base : W → Row → Bool) (C : W → Prop)
    (closed : Closed M C) (snapshot : St W Row Col)
    (correct : IsSpec (DataDependency.effective M valid p base) C snapshot)
    {a b : List W} (ca : CausalFrom M C a) (cb : CausalFrom M C b)
    (same : ∀ w, w ∈ a ↔ w ∈ b) :
    merged M valid p base snapshot a = merged M valid p base snapshot b :=
  isSpec_unique (isSpec M valid p base C closed snapshot correct a ca).1
    (isSpec_congr (fun w => by rw [same w])
      (isSpec M valid p base C closed snapshot correct b cb).1)

theorem loss_converges (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (base : W → Row → Bool) (direct : W → Row → Option (FrozenLoss W Row Col))
    (values : W → Row → Col → Option W) {a b : List W}
    (same : ∀ w, w ∈ a ↔ w ∈ b) :
    loss M valid p base direct values a = loss M valid p base direct values b := by
  funext w r
  simp only [loss, same w]

theorem dependent_recorded (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (base : W → Row → Bool) (direct : W → Row → Option (FrozenLoss W Row Col))
    (values : W → Row → Col → Option W) (received : List W) (w : W) (r : Row)
    (ch : Change Col) (mem : w ∈ received) (change : M.chg w r = some ch)
    (admitted : base w r = true)
    (depends : DataDependency.dependsOnDropped (DataDependency.authoredState M valid p base) w r = true) :
    loss M valid p base direct values received w r =
      some (dependentLoss (DataDependency.authoredState M valid p base) w r ch.gen (values w r)) := by
  simp [loss, mem, change, admitted, DataDependency.dependent_not_retained _ _ _ depends]

variable [DecidableEq Row] [DecidableEq Col] [DecidableEq K]

/-- Ordinary merge accounting plus a frozen loss for this original change.
The latter includes setters which never entered the effective merge. -/
def ValueAccounted (M : Writes W Row Col) (S : W → Prop) (st : St W Row Col)
    (I : EntryInputs Row K) (losses : W → Row → Option (FrozenLoss W Row Col))
    (r : Row) (c : Col) (w : W) (k : Nat) : Prop :=
  CovenMerge.ValueAccounted M S st I r c w k ∨
    ∃ record, losses w r = some record ∧ record.record.values c = some w

/-- Accounts for the original setter, including a change filtered out of the
effective merge. A deletion is separately covered by `dependent_recorded`. -/
theorem value_accounted (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (base : W → Row → Bool) (direct : W → Row → Option (FrozenLoss W Row Col))
    (values : W → Row → Col → Option W) (C : W → Prop)
    (closed : Closed M C) (snapshot : St W Row Col)
    (correct : IsSpec (DataDependency.effective M valid p base) C snapshot)
    (received : List W) (causal : CausalFrom M C received)
    (I : EntryInputs Row K)
    (present : ∀ r, I.present r = decide ((merged M valid p base snapshot received).gen r % 2 = 1))
    (w : W) (r : Row) (c : Col) (k : Nat) (mem : w ∈ received)
    (setter : Setter M (fun a => C a ∨ a ∈ received) r c w k)
    (admitted : base w r = true) (written : values w r c = some w) :
    ValueAccounted (DataDependency.effective M valid p base) (fun a => C a ∨ a ∈ received)
      (merged M valid p base snapshot received) I
      (loss M valid p base direct values received) r c w k := by
  obtain ⟨hw, ch, change, kind, sets, inc⟩ := setter
  cases kept : DataDependency.retained (DataDependency.authoredState M valid p base) w r
  · exact Or.inr ⟨dependentLoss (DataDependency.authoredState M valid p base) w r ch.gen (values w r),
      by simp [loss, mem, change, admitted, kept], written⟩
  · have spec := isSpec M valid p base C closed snapshot correct received causal
    exact Or.inl (CovenMerge.value_accounted (DataDependency.effective_valid M valid p base)
      spec.2 spec.1 I present ⟨hw, ch, by simp [DataDependency.effective, kept, change], kind, sets, inc⟩)

end Data
end CovenMerge
