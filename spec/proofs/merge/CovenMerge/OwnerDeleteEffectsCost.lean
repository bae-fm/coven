import CovenMerge.OwnerDeleteCost

/-! Work of the full removal comparison on supplied inputs. One unit is a
Boolean operation, row comparison, or supplied metadata/claim comparison.
Replay, SQL evaluation, and the byte cost of comparing keys are separate.
The charges include repeated evaluation of function-valued removal sets;
no memoization of `pass1` or `uniqueLosers` is assumed. -/
namespace CovenMerge.Ownership

variable {Row K : Type} [DecidableEq Row] [DecidableEq K]

def ruleScanCharge (I : Inputs Row K) (seedCost depth : Nat) (r : Row) : Nat :=
  8 + (1 + (I.refs r).length) * (depth + seedCost + 8)

def ruleScanWork (I : Inputs Row K) (seedCost depth : Nat) (removed : Row → Bool) : List Row → Nat
  | [] => 0
  | r :: rest => ruleScanCharge I seedCost depth r +
    if !removed r && fires I removed r then 0 else ruleScanWork I seedCost depth removed rest

/-- Follow precisely the choices of `closeN`, charging the supplied seed
predicate for every membership query made during a scan. -/
def ruleClosureWork (I : Inputs Row K) (seedCost : Nat) : Nat → Nat → (Row → Bool) → Nat
  | 0, _, _ => 0
  | fuel + 1, depth, removed => ruleScanWork I seedCost depth removed I.rows + 1 +
    match pick I.rows (fires I) removed with
    | none => 0
    | some r => ruleClosureWork I seedCost fuel (depth + 1) (kill removed r)

theorem rule_scan_bound (I : Inputs Row K) (seedCost depth limit degree : Nat)
    (removed : Row → Bool) (rows : List Row) (depthBound : depth ≤ limit)
    (degreeBound : ∀ r ∈ rows, (I.refs r).length ≤ degree) :
    ruleScanWork I seedCost depth removed rows ≤
      rows.length * (8 + (1 + degree) * (limit + seedCost + 8)) := by
  induction rows with
  | nil => simp [ruleScanWork]
  | cons r rest ih =>
    have charge : ruleScanCharge I seedCost depth r ≤ 8 + (1 + degree) * (limit + seedCost + 8) :=
      Nat.add_le_add_left (Nat.mul_le_mul (by have := degreeBound r (by simp); omega) (by omega)) 8
    have tail := ih (fun q hq => degreeBound q (by simp [hq]))
    simp only [ruleScanWork, List.length_cons, Nat.succ_mul]
    split <;> omega

theorem rule_closure_bound (I : Inputs Row K) (seedCost fuel depth limit degree : Nat)
    (removed : Row → Bool) (steps : fuel + depth ≤ limit)
    (degreeBound : ∀ r ∈ I.rows, (I.refs r).length ≤ degree) :
    ruleClosureWork I seedCost fuel depth removed ≤
      fuel * (I.rows.length * (8 + (1 + degree) * (limit + seedCost + 8)) + 1) := by
  induction fuel generalizing depth removed with
  | zero => simp [ruleClosureWork]
  | succ fuel ih =>
    have scan := rule_scan_bound I seedCost depth limit degree removed I.rows (by omega) degreeBound
    simp only [ruleClosureWork, Nat.succ_mul]
    cases chosen : pick I.rows (fires I) removed with
    | none => simp only [Nat.add_zero]; omega
    | some r =>
      have next := ih (depth + 1) (kill removed r) (by omega)
      simp only
      omega

/-- A query includes construction of the closure and evaluation of its final
membership function through at most N added comparisons. -/
def closeQueryWork (I : Inputs Row K) (seedCost : Nat) (seed : Row → Bool) : Nat :=
  ruleClosureWork I seedCost I.rows.length 0 seed + I.rows.length + seedCost

def closeQueryBound (rows degree seedCost : Nat) : Nat :=
  rows * (rows * (8 + (1 + degree) * (rows + seedCost + 8)) + 1) + rows + seedCost

theorem close_query_bound (I : Inputs Row K) (seedCost degree : Nat) (seed : Row → Bool)
    (degreeBound : ∀ r ∈ I.rows, (I.refs r).length ≤ degree) :
    closeQueryWork I seedCost seed ≤ closeQueryBound I.rows.length degree seedCost := by
  have bound := rule_closure_bound I seedCost I.rows.length 0 I.rows.length degree seed (by omega) degreeBound
  unfold closeQueryWork closeQueryBound
  omega

/-- Both ordinary and other-audience comparisons scan the two claim lists.
Each pair uses at most thirty-two supplied comparisons and Boolean operations. -/
def rivalWork (I : Inputs Row K) (x y : Row) : Nat :=
  4 + 2 * ((I.claims x).length * (4 + 32 * (I.claims y).length))

def rivalBound (claims : Nat) : Nat := 4 + 2 * (claims * (4 + 32 * claims))

theorem rival_work_bound (I : Inputs Row K) (claims : Nat)
    (claimBound : ∀ r, (I.claims r).length ≤ claims) (x y : Row) :
    rivalWork I x y ≤ rivalBound claims := by
  unfold rivalWork rivalBound
  exact Nat.add_le_add_left (Nat.mul_le_mul_left 2
    (Nat.mul_le_mul (claimBound x) (Nat.add_le_add_left (Nat.mul_le_mul_left 32 (claimBound y)) 4))) 4

/-- Charge every rival even if the actual Boolean scan stops sooner. Each
rival may evaluate pass one again; its work is included for every query. -/
def uniqueQueryWork (I : Inputs Row K) (r : Row) : Nat :=
  closeQueryWork I 2 (start I) + 1 +
    (I.rows.map fun y => 8 + rivalWork I r y + closeQueryWork I 2 (start I)).sum

def uniqueQueryBound (rows degree claims : Nat) : Nat :=
  closeQueryBound rows degree 2 + 1 +
    rows * (8 + rivalBound claims + closeQueryBound rows degree 2)

private theorem sum_work_bound {α : Type} (rows : List α) (work : α → Nat) (bound : Nat)
    (each : ∀ r ∈ rows, work r ≤ bound) : (rows.map work).sum ≤ rows.length * bound := by
  induction rows with
  | nil => simp
  | cons r rest ih =>
    have head := each r (by simp)
    have tail := ih (fun q hq => each q (by simp [hq]))
    simp only [List.map_cons, List.sum_cons, List.length_cons, Nat.succ_mul]
    omega

theorem unique_query_bound (I : Inputs Row K) (degree claims : Nat)
    (degreeBound : ∀ r ∈ I.rows, (I.refs r).length ≤ degree)
    (claimBound : ∀ r, (I.claims r).length ≤ claims) (r : Row) :
    uniqueQueryWork I r ≤ uniqueQueryBound I.rows.length degree claims := by
  have pass := close_query_bound I 2 degree (start I) degreeBound
  have sum := sum_work_bound I.rows
    (fun y => 8 + rivalWork I r y + closeQueryWork I 2 (start I))
    (8 + rivalBound claims + closeQueryBound I.rows.length degree 2) (by
      intro y _; have := rival_work_bound I claims claimBound r y; omega)
  unfold uniqueQueryWork uniqueQueryBound
  omega

/-- A uniform charge for `view.shown`, using the established bound on every
query of the unique seed. The second closure follows the real final pass. -/
def shownWork (I : Inputs Row K) (degree claims : Nat) : Nat :=
  closeQueryWork I (uniqueQueryBound I.rows.length degree claims)
    (uniqueLosers I.rows (rivalBefore I) (pass1 I)) + 4

def shownBound (rows degree claims : Nat) : Nat :=
  closeQueryBound rows degree (uniqueQueryBound rows degree claims) + 4

theorem shown_work_polynomial (I : Inputs Row K) (degree claims : Nat)
    (degreeBound : ∀ r ∈ I.rows, (I.refs r).length ≤ degree) :
    shownWork I degree claims ≤ shownBound I.rows.length degree claims := by
  have bound := close_query_bound I (uniqueQueryBound I.rows.length degree claims) degree
    (uniqueLosers I.rows (rivalBefore I) (pass1 I)) degreeBound
  unfold shownWork shownBound
  omega

/-- The blocker scan evaluates two completed views per row, plus ownership.
Claim-list bounds justify the unique seed charge by `unique_query_bound`.
All displayed bounds expand to polynomials in rows, references and claims. -/
def effectsWork (before after : EntryInputs Row K) (degree claims : Nat) : Nat :=
  before.rows.length * (16 + shownWork before.erase degree claims + shownWork after.erase degree claims)

theorem effects_work_polynomial (before after : EntryInputs Row K) (degree claims : Nat)
    (beforeDegree : ∀ r ∈ before.rows, (before.refs r).length ≤ degree)
    (afterDegree : ∀ r ∈ after.rows, (after.refs r).length ≤ degree)
    (beforeClaims : ∀ r, (before.claims r).length ≤ claims)
    (afterClaims : ∀ r, (after.claims r).length ≤ claims) :
    (∀ r, uniqueQueryWork before.erase r ≤ uniqueQueryBound before.rows.length degree claims) ∧
    (∀ r, uniqueQueryWork after.erase r ≤ uniqueQueryBound after.rows.length degree claims) ∧
    effectsWork before after degree claims ≤ before.rows.length *
      (16 + shownBound before.rows.length degree claims + shownBound after.rows.length degree claims) := by
  have left := shown_work_polynomial before.erase degree claims beforeDegree
  have right := shown_work_polynomial after.erase degree claims afterDegree
  refine ⟨unique_query_bound before.erase degree claims beforeDegree beforeClaims,
    unique_query_bound after.erase degree claims afterDegree afterClaims, ?_⟩
  change shownWork before.erase degree claims ≤ shownBound before.rows.length degree claims at left
  change shownWork after.erase degree claims ≤ shownBound after.rows.length degree claims at right
  exact Nat.mul_le_mul_left before.rows.length (by omega)

/-- The retained reference test, the effects context and its before filter
require at most three scans per charged trial call. -/
theorem full_context_checks_polynomial {Op : Type} (bad : List Op → Op → Bool) (active : List Op) :
    3 * contextChecks bad active ≤ 6 * (active.length * (active.length * active.length)) := by
  have bound := Nat.mul_le_mul_left 3 (context_checks_polynomial bad active)
  omega

end CovenMerge.Ownership
