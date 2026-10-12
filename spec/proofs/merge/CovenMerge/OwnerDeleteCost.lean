import CovenMerge.OwnerDeleteRule

/-! Work bounds for reference traversal and rejection rounds. One unit is a
row comparison or a supplied constant-time graph lookup. Building the merged
graph, replaying history, and SQL expression evaluation are not unit lookups. -/
namespace CovenMerge.Ownership

variable {Node Op : Type} [DecidableEq Node]

/-- `kill` adds one equality test to each membership lookup. A row scan
checks its own membership and at most every parent, plus loop control. -/
def scanCharge (g : ReferenceGraph Node) (depth : Nat) (r : Node) : Nat :=
  1 + (1 + (g.parents r).length) * (depth + 1)

/-- Work charged to the very scan used by `pick`, stopping at its first hit.
Charging all parents also bounds Boolean short-circuit evaluation. -/
def scanWork (g : ReferenceGraph Node) (depth : Nat) (removed : Node → Bool) : List Node → Nat
  | [] => 0
  | r :: rest => scanCharge g depth r +
    if !removed r && referenceFires g removed r then 0 else scanWork g depth removed rest

/-- Trace `closeN`'s actual choices. The seed membership predicate is one
lookup; each chosen `kill` extends that chain by exactly one comparison. -/
def closureWork (g : ReferenceGraph Node) : Nat → Nat → (Node → Bool) → Nat
  | 0, _, _ => 0
  | fuel + 1, depth, removed => scanWork g depth removed g.rows + 1 +
    match pick g.rows (referenceFires g) removed with
    | none => 0
    | some r => closureWork g fuel (depth + 1) (kill removed r)

theorem scan_work_bound (g : ReferenceGraph Node) (depth limit degree : Nat)
    (removed : Node → Bool) (rows : List Node) (depthBound : depth ≤ limit)
    (degreeBound : ∀ r ∈ rows, (g.parents r).length ≤ degree) :
    scanWork g depth removed rows ≤ rows.length * (1 + (1 + degree) * (limit + 1)) := by
  induction rows with
  | nil => simp [scanWork]
  | cons r rest ih =>
    have cost : scanCharge g depth r ≤ 1 + (1 + degree) * (limit + 1) := by
      exact Nat.add_le_add_left (Nat.mul_le_mul
        (Nat.add_le_add_left (degreeBound r (by simp)) 1) (by omega)) 1
    have tail := ih (fun x hx => degreeBound x (by simp [hx]))
    simp only [scanWork, List.length_cons, Nat.succ_mul]
    split <;> omega

theorem closure_work_bound (g : ReferenceGraph Node) (fuel depth limit degree : Nat)
    (removed : Node → Bool) (steps : fuel + depth ≤ limit)
    (degreeBound : ∀ r ∈ g.rows, (g.parents r).length ≤ degree) :
    closureWork g fuel depth removed ≤
      fuel * (g.rows.length * (1 + (1 + degree) * (limit + 1)) + 1) := by
  induction fuel generalizing depth removed with
  | zero => simp [closureWork]
  | succ fuel ih =>
    have scan := scan_work_bound g depth limit degree removed g.rows (by omega) degreeBound
    simp only [closureWork, Nat.succ_mul]
    cases chosen : pick g.rows (referenceFires g) removed with
    | none => simp only [Nat.add_zero]; omega
    | some r =>
      have next := ih (depth + 1) (kill removed r) (by omega)
      simp only
      omega

/-- For N rows and at most E parents per row this explicit polynomial is
O(N³E + N³). With E ≤ N the traversal is O(N⁴), including linear membership. -/
theorem reference_work_polynomial (g : ReferenceGraph Node) (targets : Node → Bool)
    (degree : Nat) (degreeBound : ∀ r ∈ g.rows, (g.parents r).length ≤ degree) :
    closureWork g g.rows.length 0 targets ≤
      g.rows.length * (g.rows.length * (1 + (1 + degree) * (g.rows.length + 1)) + 1) :=
  closure_work_bound g g.rows.length 0 g.rows.length degree targets (by omega) degreeBound

/-- Charge both occurrences of `keepRound` in a strict `settle` round and
one in its unchanged last round, without assuming compiler sharing. -/
def trialCalls (bad : List Op → Op → Bool) (active : List Op) : Nat :=
  active.length + if (keepRound bad active).length = active.length then 0
    else active.length + trialCalls bad (keepRound bad active)
termination_by active.length
decreasing_by
  have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
  omega

theorem trial_calls_polynomial (bad : List Op → Op → Bool) (active : List Op) :
    trialCalls bad active ≤ 2 * (active.length * active.length) := by
  rw [trialCalls]
  split
  · by_cases zero : active.length = 0
    · simp [zero]
    · have bound := Nat.mul_le_mul_left active.length (show 1 ≤ active.length by omega)
      simp only [Nat.mul_one] at bound
      omega
  · have ih := trial_calls_polynomial bad (keepRound bad active)
    have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
    have strict : (keepRound bad active).length + 1 ≤ active.length := by omega
    have mul := Nat.mul_le_mul_right active.length strict
    have sq := Nat.mul_le_mul_left (keepRound bad active).length bound
    rw [Nat.add_mul, Nat.one_mul] at mul
    have squareStep : active.length + (keepRound bad active).length * (keepRound bad active).length ≤
        active.length * active.length := by omega
    omega
termination_by active.length
decreasing_by
  have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
  omega

theorem trial_calls_at_least_candidates (bad : List Op → Op → Bool) (active : List Op) :
    active.length ≤ trialCalls bad active := by
  rw [trialCalls]
  split <;> omega

/-- Even distinct candidates at one row have no bound in the row count:
every candidate is tested in the first round. This is independent of the
cost of replaying their original writes or constructing their graphs. -/
theorem no_row_only_trial_bound (bound : Nat → Nat) :
    ∃ candidates : List (Nat × Unit), candidates.Nodup ∧
      (∀ op ∈ candidates, op.2 = ()) ∧
      ∀ bad, bound 1 < trialCalls bad candidates := by
  refine ⟨(List.range (bound 1 + 1)).map (fun w => (w, ())), ?_, ?_, ?_⟩
  · apply List.pairwise_map.mpr
    exact (List.nodup_range (n := bound 1 + 1)).imp (fun different equal =>
      different (congrArg Prod.fst equal))
  · intro op _; exact Unit.ext _ _
  · intro bad
    have lower := trial_calls_at_least_candidates bad
      ((List.range (bound 1 + 1)).map (fun w => (w, ())))
    simp only [List.length_map, List.length_range] at lower
    omega

private theorem round_path_shrinks (bad : List Op → Op → Bool) {a b : List Op}
    (path : Star (RoundStep bad) a b) : b.length ≤ a.length := by
  induction path with
  | refl => omega
  | head step _ ih => have := step.2; omega

theorem rejection_no_cycle (bad : List Op → Op → Bool) (a b : List Op)
    (step : RoundStep bad a b) : ¬ Star (RoundStep bad) b a := by
  intro path
  have := round_path_shrinks bad path
  have := step.2
  omega

end CovenMerge.Ownership
