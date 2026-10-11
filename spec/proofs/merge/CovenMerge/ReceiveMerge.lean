import CovenMerge.Receive

/-! Application for one audience under fixed kept store-log decisions.
Changing a boundary or retirement requires an atomic rebuild from retained
inputs (§9, §10); it starts another run, not an edit to a running prefix. -/

namespace CovenMerge.Receive

structure Retirement where
  device : Nat
  entry : Nat
  stored : Nat
  deriving DecidableEq, Repr

def retirementAllows {W : Type} (logs : Logs W) (readEntry : W → Nat → Bool)
    (stored : W → Nat) (retirements : List Retirement) (w : W) : Bool :=
  retirements.all fun e =>
    if e.device = logs.device w then
      !readEntry w e.entry && decide (stored w ≤ e.stored + 30 * 86400)
    else true

structure Policy (W : Type) where
  boundary : SnapshotBoundary W
  logs : Logs W
  reset : Option Nat
  readEntry : W → Nat → Bool
  schema : W → SchemaDisposition
  retirements : List Retirement
  stored : W → Nat

/-- Storage times are seconds; exactly thirty days counts (§10). Only kept
retirements are supplied. The caller's gate must refuse writes made after
reading their own removal. -/
def withinWindow {W : Type} (P : Policy W) (w : W) : Bool :=
  retirementAllows P.logs P.readEntry P.stored P.retirements w

def eligible {W : Type} (P : Policy W) (w : W) : Bool :=
  withinWindow P w && boundaryEligible P.reset P.readEntry P.schema w

theorem window_exact {W : Type} (P : Policy W) (w : W) :
    withinWindow P w = true ↔ ∀ e ∈ P.retirements, e.device = P.logs.device w →
      P.readEntry w e.entry = false ∧ P.stored w ≤ e.stored + 30 * 86400 := by
  simp only [withinWindow, retirementAllows, List.all_eq_true]
  constructor
  · intro h e he hd
    simpa [hd] using h e he
  · intro h e he
    by_cases hd : e.device = P.logs.device w
    · simpa [hd] using h e he hd
    · simp [hd]

section
variable {W Row Col K : Type} [DecidableEq W]

def applied (P : Policy W) (s : State W) : List W :=
  s.passed.filter (afterSnapshot P.boundary (eligible P))

/-- Own write admission has no reader input. Snapshot contributions still
obey the retirement window when replay rebuilds the selected boundary. -/
def base (P : Policy W) (w : W) (_r : Row) : Bool :=
  withinWindow P w && snapshotRetains P.boundary
    (boundaryEligible P.reset P.readEntry P.schema) w

def effective (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W) : Writes W Row Col := DataDependency.effective M valid p (base P)

theorem retired_no_effects (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W) (w : W)
    (outside : withinWindow P w = false) : ∀ r, (effective M valid p P).chg w r = none := by
  intro r
  have dropped := DataDependency.dropped_stays_dropped
    (DataDependency.authoredState M valid p (base P)) w r (by simp [DataDependency.authoredState, base, outside])
  simp [effective, DataDependency.effective, dropped]

def merged (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W) (snapshot : St W Row Col) (s : State W) : St W Row Col :=
  Data.merged M valid p (base P) snapshot s.passed

/-- This set includes consumed metadata; `effective` alone supplies changes. -/
def inputs (P : Policy W) (s : State W) : W → Prop :=
  fun w => P.boundary.covered w = true ∨ w ∈ s.passed

theorem passed_not_applied (P : Policy W) (s : State W) (w : W)
    (passed : w ∈ s.passed) (excluded : eligible P w = false) :
    consumed P.boundary s w = true ∧ w ∉ applied P s := by
  simp [consumed, passed, applied, afterSnapshot, excluded]

theorem retired_outside_not_applied (P : Policy W) (s : State W) (w : W)
    (outside : withinWindow P w = false) : w ∉ applied P s := by
  simp [applied, afterSnapshot, eligible, outside]

/-- Every counting write in an eventually available, causally closed history
is eventually applied. This uses authored timestamp order to discharge the
well-foundedness condition of the queue's progress proof. -/
theorem receive_eventually_applied {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate) (fair : Fair run)
    (S : W → Prop) (closed : Closed M S)
    (arrives : ∀ w, S w → P.boundary.covered w = true ∨ ∃ t, w ∈ (run.state t).downloaded)
    (opens : ∀ w, S w → ∃ t, ∀ u, t ≤ u → gate u w = true)
    (w : W) (member : S w) (counts : afterSnapshot P.boundary (eligible P) w = true) :
    ∃ t, w ∈ applied P (run.state t) := by
  apply eventually_applied run (eligible P) w counts
  exact arriving_past_eventually_passed run fair M.ts
    (fun w a ha => valid.past_ts w a ((exactPast w a).mp ha)) S
    (fun w hw a ha => closed w hw a ((exactPast w a).mp ha)) arrives opens w member

theorem receive_isSpec (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate)
    (closed : Closed M (fun w => P.boundary.covered w = true))
    (snapshot : St W Row Col)
    (correct : IsSpec (effective M valid p P) (fun w => P.boundary.covered w = true) snapshot)
    (t : Nat) : IsSpec (effective M valid p P) (inputs P (run.state t))
      (merged M valid p P snapshot (run.state t)) ∧
      Closed (effective M valid p P) (inputs P (run.state t)) :=
  Data.isSpec M valid p (base P) _ closed snapshot correct _
    (passed_causal P.boundary P.logs past exactPast gate run t)

/-- No retained-generation premise, for retirement or schema exclusion.
The provenance contract names the actual authored source, before any drop. -/
theorem receive_converges (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gateA gateB : Nat → W → Bool)
    (a : Run P.boundary P.logs past gateA) (b : Run P.boundary P.logs past gateB)
    (closed : Closed M (fun w => P.boundary.covered w = true))
    (snapshot : St W Row Col)
    (correct : IsSpec (effective M valid p P) (fun w => P.boundary.covered w = true) snapshot)
    (ta tb : Nat) (same : ∀ w, inputs P (a.state ta) w ↔ inputs P (b.state tb) w) :
    merged M valid p P snapshot (a.state ta) = merged M valid p P snapshot (b.state tb) :=
  isSpec_unique (receive_isSpec M valid p P past exactPast gateA a closed snapshot correct ta).1
    (isSpec_congr (fun w => (same w).symm)
      (receive_isSpec M valid p P past exactPast gateB b closed snapshot correct tb).1)

variable [DecidableEq Row] [DecidableEq Col] [DecidableEq K]

def result (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W) (snapshot : MergeSnapshot W Row Col)
    (values : W → Row → Col → Option W) (rules : St W Row Col → EntryInputs Row K)
    (s : State W) : BoundaryDevice W Row Col :=
  let st := merged M valid p P snapshot.merged s
  let v := entryView (rules st)
  ⟨⟨st, v, lossRecord st v⟩, snapshot.frozen,
    Data.loss M valid p (base P)
      (fun w r => if withinWindow P w then
        receivedLoss M P.boundary P.reset P.readEntry P.schema values s.passed w r else none)
      values s.passed⟩

theorem receive_result_converges (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gateA gateB : Nat → W → Bool)
    (a : Run P.boundary P.logs past gateA) (b : Run P.boundary P.logs past gateB)
    (closed : Closed M (fun w => P.boundary.covered w = true))
    (snapshot : MergeSnapshot W Row Col)
    (correct : IsSpec (effective M valid p P) (fun w => P.boundary.covered w = true) snapshot.merged)
    (values : W → Row → Col → Option W) (rules : St W Row Col → EntryInputs Row K)
    (ta tb : Nat) (same : ∀ w, w ∈ (a.state ta).passed ↔ w ∈ (b.state tb).passed) :
    result M valid p P snapshot values rules (a.state ta) =
      result M valid p P snapshot values rules (b.state tb) := by
  have hm := receive_converges M valid p P past exactPast gateA gateB a b closed
    snapshot.merged correct ta tb (fun w => by simp [inputs, same w])
  have losses : (result M valid p P snapshot values rules (a.state ta)).excluded =
      (result M valid p P snapshot values rules (b.state tb)).excluded := by
    funext w r
    simp only [result, Data.loss, receivedLoss, same w]
  simp only [result] at losses ⊢
  rw [hm, losses]

theorem receive_value_accounted (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate)
    (closed : Closed M (fun w => P.boundary.covered w = true))
    (snapshot : MergeSnapshot W Row Col)
    (correct : IsSpec (effective M valid p P) (fun w => P.boundary.covered w = true) snapshot.merged)
    (values : W → Row → Col → Option W) (rules : St W Row Col → EntryInputs Row K)
    (t : Nat)
    (present : ∀ r, (rules (merged M valid p P snapshot.merged (run.state t))).present r =
      decide ((merged M valid p P snapshot.merged (run.state t)).gen r % 2 = 1))
    (w : W) (r : Row) (c : Col) (k : Nat) (mem : w ∈ (run.state t).passed)
    (setter : Setter M (inputs P (run.state t)) r c w k)
    (admitted : base P w r = true) (written : values w r c = some w) :
    Data.ValueAccounted (effective M valid p P) (inputs P (run.state t))
      (merged M valid p P snapshot.merged (run.state t))
      (rules (merged M valid p P snapshot.merged (run.state t)))
      (result M valid p P snapshot values rules (run.state t)).excluded r c w k :=
  Data.value_accounted M valid p (base P)
    (fun w r => if withinWindow P w then
      receivedLoss M P.boundary P.reset P.readEntry P.schema values (run.state t).passed w r else none)
    values _ closed snapshot.merged correct _
    (passed_causal P.boundary P.logs past exactPast gate run t) _ present w r c k mem setter admitted written

theorem receive_schema_loss (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W)
    (snapshot : MergeSnapshot W Row Col) (values : W → Row → Col → Option W)
    (rules : St W Row Col → EntryInputs Row K) (s : State W) (w : W) (r : Row)
    (ch : Change Col) (v : Nat) (passed : w ∈ s.passed) (change : M.chg w r = some ch)
    (window : withinWindow P w = true) (uncovered : P.boundary.covered w = false)
    (reset : resetAllows P.reset P.readEntry w = true) (schema : P.schema w = .excluded v) :
    (result M valid p P snapshot values rules s).excluded w r =
      some (excludedLoss w r ch.gen (values w r) v) := by
  have notInput : P.boundary.inputs w = false := by
    cases hi : P.boundary.inputs w
    · rfl
    · have := P.boundary.inputs_covered w hi; simp [uncovered] at this
  simp [result, Data.loss, base, snapshotRetains, boundaryEligible, schema, notInput,
    receivedLoss, passed, change, window, boundaryLoss, disposition, uncovered, reset]

theorem receive_reset_no_loss (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W)
    (snapshot : MergeSnapshot W Row Col) (values : W → Row → Col → Option W)
    (rules : St W Row Col → EntryInputs Row K) (s : State W) (w : W)
    (uncovered : P.boundary.covered w = false) (reset : resetAllows P.reset P.readEntry w = false) :
    w ∉ applied P s ∧ ∀ r, (result M valid p P snapshot values rules s).excluded w r = none := by
  have notInput : P.boundary.inputs w = false := by
    cases hi : P.boundary.inputs w
    · rfl
    · have := P.boundary.inputs_covered w hi; simp [uncovered] at this
  constructor
  · simp [applied, afterSnapshot, eligible, boundaryEligible, reset]
  · intro r
    simp only [result, Data.loss, base, snapshotRetains, notInput, boundaryEligible, reset]
    split
    · cases hc : M.chg w r <;>
        simp [receivedLoss, hc, boundaryLoss, disposition, uncovered, reset]
    · rfl

end
end CovenMerge.Receive
