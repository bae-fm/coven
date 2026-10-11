import CovenMerge.OwnerProtection

/-! Atomic protection evaluation on the actual per-change merge. Each round
rebuilds from original writes, excluding rejected candidates and their data
dependents. Guard extraction supplies retained graph/membership facts; the
loop, rebuilding, final removal passes and recorded reasons are executable. -/
namespace CovenMerge.Ownership

variable {W Row Col Table Member K : Type}
  [DecidableEq W] [DecidableEq Row] [DecidableEq Member]

def roundAdmission (P : Policy W Row Table Member) (candidates active : List (W × Row))
    (w : W) (r : Row) : Bool :=
  allowed P w r && (!decide ((w, r) ∈ candidates) || decide ((w, r) ∈ active))

def roundState (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates active : List (W × Row))
    (received : List W) : St W Row Col :=
  Data.merged M valid p (roundAdmission P candidates active) St.init received

theorem round_state_by_set (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates a b : List (W × Row)) (received : List W)
    (same : ∀ op, op ∈ a ↔ op ∈ b) :
    roundState M valid p P candidates a received = roundState M valid p P candidates b received := by
  have admission : roundAdmission P candidates a = roundAdmission P candidates b := by
    funext w r
    simp only [roundAdmission, same]
  simp only [roundState, admission]

def roundTrial (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List (W × Row))
    (trial : (List (W × Row) → St W Row Col) → List (W × Row) → W → Row → Option (Cause W Member Row))
    (received : List W) (active : List (W × Row)) (op : W × Row) : Option (Cause W Member Row) :=
  if DataDependency.retained
      (DataDependency.authoredState M valid p (roundAdmission P candidates active)) op.1 op.2 then
    trial (fun context => roundState M valid p P candidates context received) active op.1 op.2
  else none

/-- Ownership and dependency exclusions precede the protection trial. An
operation whose change is already absent cannot acquire a misleading guard
cause by pretending its effects were still active. -/
theorem unretained_not_tried (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates active : List (W × Row))
    (trial : (List (W × Row) → St W Row Col) → List (W × Row) → W → Row → Option (Cause W Member Row)) (received : List W) (op : W × Row)
    (dropped : DataDependency.retained
      (DataDependency.authoredState M valid p (roundAdmission P candidates active)) op.1 op.2 = false) :
    roundTrial M valid p P candidates trial received active op = none := by
  simp [roundTrial, dropped]

def settledGuard (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List (W × Row))
    (trial : (List (W × Row) → St W Row Col) → List (W × Row) → W → Row → Option (Cause W Member Row))
    (received : List W) (w : W) (r : Row) : Option (Cause W Member Row) :=
  rejectionReason (roundTrial M valid p P candidates trial received) candidates (w, r)

/-- Recording rejected causes yields exactly the same admission as retaining
the settled candidate set. Data dependency is evaluated by the shared filter. -/
theorem settled_admission (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List (W × Row))
    (trial : (List (W × Row) → St W Row Col) → List (W × Row) → W → Row → Option (Cause W Member Row)) (received : List W) :
    base P (settledGuard M valid p P candidates trial received) =
      roundAdmission P candidates
        (settle (fun active op => (roundTrial M valid p P candidates trial received active op).isSome) candidates) := by
  funext w r
  simp only [base, settledGuard, rejection_keeps, roundAdmission]
  apply Bool.eq_iff_iff.mpr
  simp

theorem settled_merge (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List (W × Row))
    (trial : (List (W × Row) → St W Row Col) → List (W × Row) → W → Row → Option (Cause W Member Row)) (received : List W) :
    merged M valid p P (settledGuard M valid p P candidates trial received) received =
      roundState M valid p P candidates
        (settle (fun active op => (roundTrial M valid p P candidates trial received active op).isSome) candidates)
        received := by
  simp only [merged, roundState, settled_admission]

/-- Every retained surviving candidate passes when judged against the actual
final merge, after exclusions and their data dependencies are reapplied. -/
theorem settled_safe (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List (W × Row))
    (trial : (List (W × Row) → St W Row Col) → List (W × Row) → W → Row → Option (Cause W Member Row)) (received : List W)
    (op : W × Row)
    (kept : op ∈ settle
      (fun active op => (roundTrial M valid p P candidates trial received active op).isSome) candidates)
    (retained : DataDependency.retained
      (DataDependency.authoredState M valid p (base P (settledGuard M valid p P candidates trial received)))
      op.1 op.2 = true) :
    trial (fun context => roundState M valid p P candidates context received)
      (settle (fun active op => (roundTrial M valid p P candidates trial received active op).isSome) candidates)
      op.1 op.2 = none := by
  have safe := settles_safe
    (fun active op => (roundTrial M valid p P candidates trial received active op).isSome) candidates op kept
  rw [settled_admission] at retained
  rw [roundTrial] at safe
  simp only [retained, ↓reduceIte] at safe
  simpa using safe

theorem settled_guard_converges (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List (W × Row))
    (trial : (List (W × Row) → St W Row Col) → List (W × Row) → W → Row → Option (Cause W Member Row))
    {a b : List W} (ca : CausalOrder M a) (cb : CausalOrder M b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    settledGuard M valid p P candidates trial a = settledGuard M valid p P candidates trial b := by
  have states : (fun context => roundState M valid p P candidates context a) =
      (fun context => roundState M valid p P candidates context b) := by
    funext context
    exact Data.converges M valid p (roundAdmission P candidates context) _ closed_empty St.init isSpec_init
      (causalFrom_of_causalOrder ca) (causalFrom_of_causalOrder cb) same
  have eq : roundTrial M valid p P candidates trial a = roundTrial M valid p P candidates trial b := by
    funext active op
    simp only [roundTrial, states]
  funext w r
  unfold settledGuard
  rw [eq]

variable [DecidableEq Col] [DecidableEq K]

/-- This result installs the completed core, the three-pass view, ordinary
losses and the original change records with their retained rejecting causes. -/
def protectedResult (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List (W × Row))
    (trial : (List (W × Row) → St W Row Col) → List (W × Row) → W → Row → Option (Cause W Member Row))
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    (received : List W) : Device W Row Col × (W → Row → Option (Loss W Row Col Member)) :=
  result M valid p P (settledGuard M valid p P candidates trial received) values inputs received

theorem protected_result_converges (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List (W × Row))
    (trial : (List (W × Row) → St W Row Col) → List (W × Row) → W → Row → Option (Cause W Member Row))
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    {a b : List W} (ca : CausalOrder M a) (cb : CausalOrder M b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    protectedResult M valid p P candidates trial values inputs a =
      protectedResult M valid p P candidates trial values inputs b := by
  simp only [protectedResult, settled_guard_converges M valid p P candidates trial ca cb same]
  exact result_converges M valid p P _ values inputs ca cb same

end CovenMerge.Ownership
