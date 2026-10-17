import CovenMerge.Boundary

/-! Abstract causal application (§7.1). Each attempt checks a supplied past
against consumed inputs, including inputs consumed without applying data.
Other admission checks are an explicit gate. Download order is unrestricted;
the sequence model establishes the concrete slot reader's order. All state
changes below are completed atomic transactions. -/

namespace CovenMerge.Receive

def ready {W : Type} (past : List W) (passed : W → Bool) : Bool :=
  past.all passed

theorem ready_iff {W : Type} (past : List W) (passed : W → Bool) :
    ready past passed = true ↔ ∀ a ∈ past, passed a = true := List.all_eq_true

structure State (W : Type) where
  downloaded : List W
  passed : List W
  deriving DecidableEq, Repr

def initial {W : Type} : State W := ⟨[], []⟩

section
variable {W Row Col : Type} [DecidableEq W]

def consumed (B : SnapshotBoundary W) (s : State W) (w : W) : Bool :=
  B.covered w || decide (w ∈ s.passed)

def enabled (B : SnapshotBoundary W) (past : W → List W) (s : State W)
    (gate : W → Bool) (w : W) : Bool :=
  decide (w ∈ s.downloaded) && !consumed B s w && gate w && ready (past w) (consumed B s)

def attempt (B : SnapshotBoundary W) (past : W → List W) (s : State W)
    (gate : W → Bool) (w : W) : State W :=
  if enabled B past s gate w then { s with passed := s.passed ++ [w] } else s

inductive Action (W : Type) where
  | download (w : W)
  | attempt (w : W)
  | idle

/-- Downloads add available inputs. Only an attempt with a complete causal
past can consume them; unavailable or in-flight inputs do not count. -/
inductive Step (B : SnapshotBoundary W) (past : W → List W)
    (gate : W → Bool) : Action W → State W → State W → Prop where
  | download {s : State W} {w : W} (fresh : w ∉ s.downloaded)
      (uncovered : B.covered w = false) :
      Step B past gate (.download w) s { s with downloaded := s.downloaded ++ [w] }
  | attempt (s : State W) (w : W) :
      Step B past gate (.attempt w) s (Receive.attempt B past s gate w)
  | idle (s : State W) : Step B past gate .idle s s

structure Run (B : SnapshotBoundary W) (past : W → List W)
    (gate : Nat → W → Bool) where
  state : Nat → State W
  action : Nat → Action W
  start : state 0 = initial
  next : ∀ t, Step B past (gate t) (action t) (state t) (state (t + 1))

theorem attempt_causal {M : Writes W Row Col} (B : SnapshotBoundary W)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (s : State W) (gate : W → Bool) (w : W)
    (causal : CausalFrom M (fun a => B.covered a = true) s.passed) :
    CausalFrom M (fun a => B.covered a = true) (attempt B past s gate w).passed := by
  unfold attempt
  split
  · rename_i h
    simp only [enabled, Bool.and_eq_true, Bool.not_eq_true', consumed,
      Bool.or_eq_false_iff, decide_eq_false_iff_not] at h
    refine .snoc causal (by simp [h.1.1.2.1]) h.1.1.2.2 ?_
    intro a ha
    have hp := (ready_iff _ _).mp h.2 a ((exactPast w a).mpr ha)
    simpa [consumed] using hp
  · exact causal

theorem step_causal {M : Writes W Row Col} {B : SnapshotBoundary W}
    {past : W → List W}
    (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    {gate : W → Bool} {action : Action W} {s s' : State W}
    (h : Step B past gate action s s')
    (causal : CausalFrom M (fun a => B.covered a = true) s.passed) :
    CausalFrom M (fun a => B.covered a = true) s'.passed := by
  cases h with
  | download => exact causal
  | attempt s w => exact attempt_causal B past exactPast s gate w causal
  | idle => exact causal

theorem step_attempt {B : SnapshotBoundary W} {past : W → List W}
    {gate : W → Bool} {s s' : State W} {w : W}
    (h : Step B past gate (.attempt w) s s') : s' = attempt B past s gate w := by
  cases h; rfl

theorem passed_causal {M : Writes W Row Col} (B : SnapshotBoundary W)
    (past : W → List W)
    (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gate : Nat → W → Bool) (run : Run B past gate) (t : Nat) :
    CausalFrom M (fun a => B.covered a = true) (run.state t).passed := by
  induction t with
  | zero => rw [run.start]; exact .nil
  | succ t ih => exact step_causal exactPast (run.next t) ih

/-- This is the causal sequence of changes that actually count, including
snapshot inputs; consumed exclusions do not become merge inputs. -/
theorem applied_causal {M : Writes W Row Col} (B : SnapshotBoundary W)
    (past : W → List W)
    (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (eligible : W → Bool) (gate : Nat → W → Bool) (run : Run B past gate) (t : Nat) :
    CausalFrom (snapshotWrites M B eligible) (fun a => B.inputs a = true)
      ((run.state t).passed.filter (afterSnapshot B eligible)) :=
  snapshot_causal B eligible (passed_causal B past exactPast gate run t)

omit [DecidableEq W] in
theorem causalOrder_of_empty_coverage {M : Writes W Row Col} {L : List W}
    (h : CausalFrom M (fun _ => False) L) : CausalOrder M L := by
  induction h with
  | nil => exact .nil
  | snoc _ _ fresh hp ih => exact .snoc ih fresh (fun a ha => (hp a ha).resolve_left id)

theorem applied_causalOrder {M : Writes W Row Col} (B : SnapshotBoundary W)
    (past : W → List W)
    (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (empty : ∀ a, B.inputs a = false) (eligible : W → Bool)
    (gate : Nat → W → Bool) (run : Run B past gate) (t : Nat) :
    CausalOrder (snapshotWrites M B eligible)
      ((run.state t).passed.filter (afterSnapshot B eligible)) := by
  have h := applied_causal B past exactPast eligible gate run t
  have eq : (fun a => B.inputs a = true) = (fun _ => False) := by
    funext a; simp [empty a]
  rw [eq] at h
  exact causalOrder_of_empty_coverage h

theorem received_causalOrder {M : Writes W Row Col} (B : SnapshotBoundary W)
    (past : W → List W)
    (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (empty : ∀ a, B.covered a = false)
    (gate : Nat → W → Bool) (run : Run B past gate) (t : Nat) :
    CausalOrder M (run.state t).passed := by
  have h := passed_causal B past exactPast gate run t
  have eq : (fun a => B.covered a = true) = (fun _ => False) := by
    funext a; simp [empty a]
  rw [eq] at h
  exact causalOrder_of_empty_coverage h

/-- Ordinary receive, before any exclusions or snapshots: the existing merge
theorem needs only valid authored writes and equal consumed sets. -/
theorem ordinary_receive_converges {M : Writes W Row Col} (valid : Valid M)
    (B : SnapshotBoundary W) (past : W → List W)
    (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true) (empty : ∀ a, B.covered a = false)
    (gateA gateB : Nat → W → Bool) (a : Run B past gateA) (b : Run B past gateB)
    (ta tb : Nat) (same : ∀ w, w ∈ (a.state ta).passed ↔ w ∈ (b.state tb).passed) :
    (a.state ta).passed.foldl (step M) St.init = (b.state tb).passed.foldl (step M) St.init :=
  merge_converges valid (received_causalOrder B past exactPast empty gateA a ta)
    (received_causalOrder B past exactPast empty gateB b tb) same

theorem step_monotone {B : SnapshotBoundary W} {past : W → List W}
    {gate : W → Bool} {action : Action W} {s s' : State W}
    (h : Step B past gate action s s') :
    (∀ w, w ∈ s.downloaded → w ∈ s'.downloaded) ∧
    (∀ w, w ∈ s.passed → w ∈ s'.passed) := by
  cases h with
  | download => exact ⟨fun w h => List.mem_append_left _ h, fun _ h => h⟩
  | attempt s w =>
    unfold attempt
    split
    · exact ⟨fun _ h => h, fun _ h => List.mem_append_left _ h⟩
    · exact ⟨fun _ h => h, fun _ h => h⟩
  | idle => exact ⟨fun _ h => h, fun _ h => h⟩

theorem run_monotone {B : SnapshotBoundary W} {past : W → List W}
    {gate : Nat → W → Bool} (run : Run B past gate) {t u : Nat} (le : t ≤ u) :
    (∀ w, w ∈ (run.state t).downloaded → w ∈ (run.state u).downloaded) ∧
    (∀ w, w ∈ (run.state t).passed → w ∈ (run.state u).passed) := by
  induction le with
  | refl => exact ⟨fun _ h => h, fun _ h => h⟩
  | @step u le ih =>
    have next := step_monotone (run.next u)
    exact ⟨fun w h => next.1 w (ih.1 w h), fun w h => next.2 w (ih.2 w h)⟩

theorem consumed_monotone {B : SnapshotBoundary W} {past : W → List W}
    {gate : Nat → W → Bool} (run : Run B past gate) {t u : Nat} (le : t ≤ u)
    (w : W) (h : consumed B (run.state t) w = true) : consumed B (run.state u) w = true := by
  simp only [consumed, Bool.or_eq_true, decide_eq_true_eq] at h ⊢
  exact h.elim Or.inl (fun h => Or.inr ((run_monotone run le).2 w h))

/-- An unconsumed retained write is eventually reconsidered. Nothing
requires retrying a completed write. Fairness assumes only an attempt,
not its success, and places no bound on unrelated arrivals. -/
def Fair {B : SnapshotBoundary W} {past : W → List W}
    {gate : Nat → W → Bool} (run : Run B past gate) : Prop :=
  ∀ w t, w ∈ (run.state t).downloaded → consumed B (run.state t) w = false →
    ∃ u, t ≤ u ∧ run.action u = .attempt w

theorem eventually_ready {B : SnapshotBoundary W} {past : W → List W}
    {gate : Nat → W → Bool} (run : Run B past gate) (ws : List W)
    (eventually : ∀ a ∈ ws, ∃ t, consumed B (run.state t) a = true) :
    ∃ t, ∀ u, t ≤ u → ready ws (consumed B (run.state u)) = true := by
  induction ws with
  | nil => exact ⟨0, fun _ _ => rfl⟩
  | cons a rest ih =>
    obtain ⟨ta, ha⟩ := eventually a (by simp)
    obtain ⟨tr, hr⟩ := ih (fun w hw => eventually w (by simp [hw]))
    refine ⟨max ta tr, fun u hu => ?_⟩
    have a := consumed_monotone run (show ta ≤ u by omega) a ha
    have rest := hr u (by omega)
    simpa [ready, a] using rest

/-- Once the finite recorded past is complete, a retained write cannot be
held forever by the causal queue. Other checks must eventually stay open. -/
theorem complete_past_eventually_passed {B : SnapshotBoundary W}
    {past : W → List W} {gate : Nat → W → Bool} (run : Run B past gate)
    (fair : Fair run) (w : W) (arrives : ∃ t, w ∈ (run.state t).downloaded)
    (opens : ∃ t, ∀ u, t ≤ u → gate u w = true)
    (complete : ∀ a ∈ past w, ∃ t, consumed B (run.state t) a = true) :
    ∃ t, consumed B (run.state t) w = true := by
  obtain ⟨td, hd⟩ := arrives
  obtain ⟨tg, hg⟩ := opens
  obtain ⟨tp, hp⟩ := eventually_ready run (past w) complete
  let lower := max td (max tg tp)
  by_cases already : consumed B (run.state lower) w = true
  · exact ⟨lower, already⟩
  have hd' := (run_monotone run (show td ≤ lower by omega)).1 w hd
  obtain ⟨u, bound, action⟩ := fair w lower hd' (by simpa using already)
  by_cases done : consumed B (run.state u) w = true
  · exact ⟨u, done⟩
  · have enabled : enabled B past (run.state u) (gate u) w = true := by
      simp [Receive.enabled, (run_monotone run (show td ≤ u by dsimp [lower] at bound; omega)).1 w hd,
        done, hg u (by dsimp [lower] at bound; omega), hp u (by dsimp [lower] at bound; omega)]
    have next := run.next u
    rw [action] at next
    exact ⟨u + 1, by rw [step_attempt next]; simp [attempt, enabled, consumed]⟩

/-- Eventual arrival of a dependency-closed set suffices: induction on the
authored timestamp turns arrival into consumption, without assuming that
the causes were already applied. -/
theorem arriving_past_eventually_passed {B : SnapshotBoundary W}
    {past : W → List W} {gate : Nat → W → Bool} (run : Run B past gate)
    (fair : Fair run) (rank : W → Nat)
    (earlier : ∀ w a, a ∈ past w → rank a < rank w) (S : W → Prop)
    (closed : ∀ w, S w → ∀ a ∈ past w, S a)
    (arrives : ∀ w, S w → B.covered w = true ∨ ∃ t, w ∈ (run.state t).downloaded)
    (opens : ∀ w, S w → ∃ t, ∀ u, t ≤ u → gate u w = true) :
    ∀ w, S w → ∃ t, consumed B (run.state t) w = true := by
  intro w
  generalize hr : rank w = n
  induction n using Nat.strongRecOn generalizing w with
  | ind n ih =>
    intro hw
    rcases arrives w hw with covered | downloaded
    · exact ⟨0, by simp [consumed, covered]⟩
    · apply complete_past_eventually_passed run fair w downloaded (opens w hw)
      intro a ha
      exact ih (rank a) (by have := earlier w a ha; omega) a rfl (closed w hw a ha)

theorem eventually_applied {B : SnapshotBoundary W}
    {past : W → List W} {gate : Nat → W → Bool} (run : Run B past gate)
    (eligible : W → Bool) (w : W) (counts : afterSnapshot B eligible w = true)
    (eventually : ∃ t, consumed B (run.state t) w = true) :
    ∃ t, w ∈ (run.state t).passed.filter (afterSnapshot B eligible) := by
  obtain ⟨t, ht⟩ := eventually
  have outside : B.covered w = false := by
    have h := counts
    simp only [afterSnapshot, Bool.and_eq_true, Bool.not_eq_true'] at h
    exact h.1
  refine ⟨t, List.mem_filter.mpr ⟨?_, counts⟩⟩
  simpa [consumed, outside] using ht

end
end CovenMerge.Receive
