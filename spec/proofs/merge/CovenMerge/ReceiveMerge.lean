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

def effective (M : Writes W Row Col) (P : Policy W) : Writes W Row Col :=
  snapshotWrites M P.boundary (eligible P)

/-- An out-of-window write contributes neither changes nor input edges,
including through the installed snapshot. -/
theorem retired_no_effects (M : Writes W Row Col) (P : Policy W) (w : W)
    (snapshotWithinWindow : ∀ a, P.boundary.inputs a = true → withinWindow P a = true)
    (outside : withinWindow P w = false) :
    (∀ r, (effective M P).chg w r = none) ∧ ∀ a, (effective M P).past a w = false := by
  have notInput : P.boundary.inputs w = false := by
    cases hi : P.boundary.inputs w
    · rfl
    · have := snapshotWithinWindow w hi
      simp [outside] at this
  simp [effective, snapshotWrites, snapshotRetains, notInput, eligible, outside]

def merged (M : Writes W Row Col) (P : Policy W) (snapshot : St W Row Col)
    (s : State W) : St W Row Col := (applied P s).foldl (step (effective M P)) snapshot

def inputs (P : Policy W) (s : State W) : W → Prop :=
  fun w => P.boundary.inputs w = true ∨ w ∈ applied P s

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

theorem receive_isSpec {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate)
    (generations : SnapshotGenerations M P.boundary (eligible P))
    (closed : Closed (effective M P) (fun w => P.boundary.inputs w = true))
    (snapshot : St W Row Col)
    (correct : IsSpec (effective M P) (fun w => P.boundary.inputs w = true) snapshot)
    (t : Nat) : IsSpec (effective M P) (inputs P (run.state t)) (merged M P snapshot (run.state t)) ∧
      Closed (effective M P) (inputs P (run.state t)) :=
  foldl_isSpec (snapshot_valid valid P.boundary (eligible P) generations) closed correct
    (applied_causal P.boundary P.logs past exactPast (eligible P) gate run t)

/-- No receive-side causal-order assumption. Retained generation witnesses
and correctness of the chosen snapshot remain explicit. -/
theorem receive_converges {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gateA gateB : Nat → W → Bool)
    (a : Run P.boundary P.logs past gateA) (b : Run P.boundary P.logs past gateB)
    (generations : SnapshotGenerations M P.boundary (eligible P))
    (closed : Closed (effective M P) (fun w => P.boundary.inputs w = true))
    (snapshot : St W Row Col)
    (correct : IsSpec (effective M P) (fun w => P.boundary.inputs w = true) snapshot)
    (ta tb : Nat) (same : ∀ w, inputs P (a.state ta) w ↔ inputs P (b.state tb) w) :
    merged M P snapshot (a.state ta) = merged M P snapshot (b.state tb) :=
  isSpec_unique (receive_isSpec valid P past exactPast gateA a generations closed snapshot correct ta).1
    (isSpec_congr (fun w => (same w).symm)
      (receive_isSpec valid P past exactPast gateB b generations closed snapshot correct tb).1)

variable [DecidableEq Row] [DecidableEq Col] [DecidableEq K]

def result (M : Writes W Row Col) (P : Policy W) (snapshot : MergeSnapshot W Row Col)
    (values : W → Row → Col → Option W) (rules : St W Row Col → EntryInputs Row K)
    (s : State W) : BoundaryDevice W Row Col :=
  let st := merged M P snapshot.merged s
  let v := entryView (rules st)
  ⟨⟨st, v, lossRecord st v⟩, snapshot.frozen,
    fun w r => if withinWindow P w then
      receivedLoss M P.boundary P.reset P.readEntry P.schema values s.passed w r else none⟩

theorem receive_result_converges {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gateA gateB : Nat → W → Bool)
    (a : Run P.boundary P.logs past gateA) (b : Run P.boundary P.logs past gateB)
    (generations : SnapshotGenerations M P.boundary (eligible P))
    (closed : Closed (effective M P) (fun w => P.boundary.inputs w = true))
    (snapshot : MergeSnapshot W Row Col)
    (correct : IsSpec (effective M P) (fun w => P.boundary.inputs w = true) snapshot.merged)
    (values : W → Row → Col → Option W) (rules : St W Row Col → EntryInputs Row K)
    (ta tb : Nat) (same : ∀ w, w ∈ (a.state ta).passed ↔ w ∈ (b.state tb).passed) :
    result M P snapshot values rules (a.state ta) = result M P snapshot values rules (b.state tb) := by
  have hm := receive_converges valid P past exactPast gateA gateB a b generations closed
    snapshot.merged correct ta tb (fun w => by simp [inputs, applied, same w])
  have losses : receivedLoss M P.boundary P.reset P.readEntry P.schema values (a.state ta).passed =
      receivedLoss M P.boundary P.reset P.readEntry P.schema values (b.state tb).passed := by
    funext w r
    simp [receivedLoss, same w]
  simp only [result, hm, losses]

theorem receive_value_accounted {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate)
    (generations : SnapshotGenerations M P.boundary (eligible P))
    (closed : Closed (effective M P) (fun w => P.boundary.inputs w = true))
    (snapshot : St W Row Col)
    (correct : IsSpec (effective M P) (fun w => P.boundary.inputs w = true) snapshot)
    (t : Nat) (I : EntryInputs Row K)
    (present : ∀ r, I.present r = decide ((merged M P snapshot (run.state t)).gen r % 2 = 1))
    {r : Row} {c : Col} {w : W} {k : Nat}
    (setter : Setter (effective M P) (inputs P (run.state t)) r c w k) :
    ValueAccounted (effective M P) (inputs P (run.state t))
      (merged M P snapshot (run.state t)) I r c w k := by
  have correct := receive_isSpec valid P past exactPast gate run generations closed snapshot correct t
  exact value_accounted (snapshot_valid valid P.boundary (eligible P) generations)
    correct.2 correct.1 I present setter

theorem receive_schema_loss (M : Writes W Row Col) (P : Policy W)
    (snapshot : MergeSnapshot W Row Col) (values : W → Row → Col → Option W)
    (rules : St W Row Col → EntryInputs Row K) (s : State W) (w : W) (r : Row)
    (ch : Change Col) (v : Nat) (passed : w ∈ s.passed) (change : M.chg w r = some ch)
    (window : withinWindow P w = true) (uncovered : P.boundary.covered w = false)
    (reset : resetAllows P.reset P.readEntry w = true) (schema : P.schema w = .excluded v) :
    (result M P snapshot values rules s).excluded w r =
      some (excludedLoss w r ch.gen (values w r) v) := by
  simp only [result, window, ↓reduceIte]
  exact excluded_values_recorded M P.boundary P.reset P.readEntry P.schema values s.passed
    w r ch v passed change uncovered reset schema

theorem receive_reset_no_loss (M : Writes W Row Col) (P : Policy W)
    (snapshot : MergeSnapshot W Row Col) (values : W → Row → Col → Option W)
    (rules : St W Row Col → EntryInputs Row K) (s : State W) (w : W)
    (uncovered : P.boundary.covered w = false) (reset : resetAllows P.reset P.readEntry w = false) :
    w ∉ applied P s ∧ ∀ r, (result M P snapshot values rules s).excluded w r = none := by
  constructor
  · simp [applied, afterSnapshot, eligible, boundaryEligible, reset]
  · intro r
    simp only [result, receivedLoss]
    split
    · split
      · cases hc : M.chg w r <;> simp [boundaryLoss, disposition, uncovered, reset]
      · rfl
    · rfl

end
end CovenMerge.Receive
