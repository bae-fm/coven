import CovenMerge.ForeignKeyActions

/-! Direct authorization and checked FK provenance feed the ordinary merge.
The additional dependency edges identify causes within a write. They do not
judge whether a deletion hides somebody's row. -/
namespace CovenMerge.Ownership.Admission

open ForeignKey

inductive Origin (Row : Type) where
  | direct
  | foreign (derivation : Derivation Row)
  deriving DecidableEq, Repr

/-- Direct execution retains its data sources even without a net row change. -/
structure Direct (W Row Payload : Type) where
  payload : Payload
  /-- Source roles not already supplied by the net change's provenance. -/
  additionalSources : List (W × Row)

/-- Reconstructed by the receiver from the original schema and authenticated
execution inputs. Origins include the complete coalescing order; they are not
sender labels or sender-selected derivations. SQL reconstruction is the same
explicit input boundary as constraint extraction into `EntryInputs`. -/
structure Execution (W Row Payload : Type) where
  direct : Row → Option (Direct W Row Payload)
  origin : Row → Origin Row
  schema : Schema Row Payload

/-- The proposed row payload is separate from the receiver's execution. -/
structure Evidence (W Row Payload : Type) where
  payload : W → Row → Option Payload
  execution : W → Execution W Row Payload

def Execution.directPayload (execution : Execution W Row Payload) (r : Row) : Option Payload :=
  (execution.direct r).map (·.payload)

variable {W Row Col Table Member Payload K : Type}
  [DecidableEq W] [DecidableEq Row] [DecidableEq Member] [DecidableEq Payload]

def direct (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (w : W) (r : Row) : Option Payload :=
  if allowed P w r then (E.execution w).directPayload r else none

/-- A row's proposed payload and the receiver's agree, left without their
generated cells (`Schema.withoutGenerated`). -/
abbrev Execution.agrees (execution : Execution W Row Payload) (a b : Option Payload) : Prop :=
  a.map execution.schema.withoutGenerated = b.map execution.schema.withoutGenerated

def check (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (w : W) (r : Row) : Bool :=
  match (E.execution w).origin r with
  | .direct => allowed P w r &&
      decide ((E.execution w).agrees (E.payload w r) ((E.execution w).directPayload r))
  | .foreign d => (!(((E.execution w).directPayload r).isSome) || allowed P w r) &&
      (E.payload w r).any fun value => verifies ((E.execution w).schema) (direct P E w) d r value

theorem direct_authorized (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (w : W) (r : Row) (origin : (E.execution w).origin r = .direct)
    (owned : P.owned (P.table r) = true) (kept : check P E w r = true) :
    P.author w = owner P r := by
  exact (by simpa [check, origin, allowed, owned] using kept :
    P.author w = owner P r ∧
      (E.execution w).agrees (E.payload w r) ((E.execution w).directPayload r)).1

/-- Coalescing an owned row with an FK action does not hide a direct edit.
Every actual statement/trigger target must still pass its own permission. -/
theorem statement_authorized (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (w : W) (r : Row) (target : ((E.execution w).directPayload r).isSome = true)
    (owned : P.owned (P.table r) = true) (kept : check P E w r = true) :
    P.author w = owner P r := by
  cases origin : (E.execution w).origin r with
  | direct => exact direct_authorized P E w r origin owned kept
  | foreign d =>
    simp only [check, origin, Bool.and_eq_true] at kept
    simpa [target, allowed, owned] using kept.1

/-- A kept derived row's payload is what its derivation evaluates to, but for
generated cells, which no step sets. -/
theorem derived_exact (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (w : W) (r : Row) (d : Derivation Row) (origin : (E.execution w).origin r = .foreign d)
    (kept : check P E w r = true) :
    ∃ value result, E.payload w r = some value ∧
      evaluate ((E.execution w).schema) (direct P E w) d = some (r, result) ∧
      (E.execution w).schema.withoutGenerated result =
        (E.execution w).schema.withoutGenerated value ∧
      Implied ((E.execution w).schema) (direct P E w) r result := by
  simp only [check, origin, Bool.and_eq_true, Option.any_eq_true] at kept
  obtain ⟨_, value, payload, verified⟩ := kept
  obtain ⟨result, evaluated, eq⟩ :=
    (verifies_iff ((E.execution w).schema) (direct P E w) d r value).mp verified
  exact ⟨value, result, payload, evaluated, eq,
    evaluate_sound ((E.execution w).schema) (direct P E w) d evaluated⟩

/-- Keeping the receiver's execution fixed leaves only one acceptable
payload for a derived row, even if the proposed bytes or labels differ, once
its generated cells are what SQLite computes: `fill` computes them from the
row's other cells, as SQLite's deterministic expressions do. -/
theorem derived_payload_unique (P : Policy W Row Table Member)
    (A B : Evidence W Row Payload) (w : W) (r : Row) (d : Derivation Row)
    (same : A.execution = B.execution)
    (origin : (A.execution w).origin r = .foreign d)
    (ha : check P A w r = true) (hb : check P B w r = true) (fill : Payload → Payload)
    (computedA : ∀ v, A.payload w r = some v → fill ((A.execution w).schema.withoutGenerated v) = v)
    (computedB : ∀ v, B.payload w r = some v → fill ((A.execution w).schema.withoutGenerated v) = v) :
    A.payload w r = B.payload w r := by
  obtain ⟨a, ra, pa, ea, sa, _⟩ := derived_exact P A w r d origin ha
  obtain ⟨b, rb, pb, eb, sb, _⟩ := derived_exact P B w r d (same ▸ origin) hb
  have roots : direct P A w = direct P B w := by
    funext row
    simp only [direct, same]
  rw [same, roots, eb] at ea
  have eq : rb = ra := congrArg Prod.snd (Option.some.inj ea)
  rw [← same, eq, sa] at sb
  rw [pa, pb, ← computedA a pa, ← computedB b pb, sb]

theorem derived_root_authorized (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (w : W) (r : Row) (d : Derivation Row) (origin : (E.execution w).origin r = .foreign d)
    (kept : check P E w r = true) :
    ((E.execution w).directPayload (root d)).isSome = true ∧ allowed P w (root d) = true := by
  obtain ⟨_, _, _, verified, _, _⟩ := derived_exact P E w r d origin kept
  obtain ⟨_, payload⟩ := evaluate_root ((E.execution w).schema) (direct P E w) d verified
  unfold direct at payload
  split at payload
  · exact ⟨by simp [payload], by assumption⟩
  · contradiction

theorem derived_roots_authorized (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (w : W) (r : Row) (d : Derivation Row) (origin : (E.execution w).origin r = .foreign d)
    (kept : check P E w r = true) (source : Row) (used : source ∈ roots d) :
    ((E.execution w).directPayload source).isSome = true ∧ allowed P w source = true := by
  obtain ⟨_, _, _, verified, _, _⟩ := derived_exact P E w r d origin kept
  obtain ⟨_, payload⟩ := evaluate_roots (E.execution w).schema (direct P E w) d verified source used
  unfold direct at payload
  split at payload
  · exact ⟨by simp [payload], by assumption⟩
  · contradiction

/-- Intermediate consequences retain their same-write causes as well as the
root. The receiver's executor supplies the complete per-row provenance when
several SQL actions coalesce into one recorded row change. -/
structure Dependencies (W Row : Type) where
  /-- Includes recorded changes and every direct execution target. -/
  changes : List (W × Row)
  sources : W → Row → List (W × Row)

/-- Combine net-change provenance, the direct execution's validated sources,
and same-write FK causes. No dependence is inferred from row visibility. -/
def dependencies (M : Writes W Row Col) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (changes : List (W × Row)) : Dependencies W Row where
  changes := changes
  sources w r := ((p.provenance w r).sources.map (·, r)) ++
    (((E.execution w).direct r).toList.flatMap (·.additionalSources) ++ match (E.execution w).origin r with
    | .direct => []
    | .foreign d => (ForeignKey.sources ((E.execution w).schema) (direct P E w) d).map (w, ·))

theorem derived_uses_root (M : Writes W Row Col) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (changes : List (W × Row)) (w : W) (r : Row) (d : Derivation Row)
    (origin : (E.execution w).origin r = .foreign d) :
    (w, root d) ∈ (dependencies M p P E changes).sources w r := by
  simp only [dependencies, origin]
  exact List.mem_append_right _ (List.mem_append_right _ (List.mem_map.mpr
    ⟨root d, root_mem_sources ((E.execution w).schema) (direct P E w) d, rfl⟩))

/-- Propagate a dropped cause through its actual data effects. A cycle without
any dropped cause adds no rejection; a rejected source drops all dependents.
The finite closure is the same terminating primitive used by removal rules. -/
def dropped (D : Dependencies W Row) (seed : W → Row → Bool) : W × Row → Bool :=
  close D.changes (fun out node => (D.sources node.1 node.2).any out)
    (fun node => seed node.1 node.2)

theorem seed_dropped (D : Dependencies W Row) (seed : W → Row → Bool)
    (w : W) (r : Row) (h : seed w r = true) : dropped D seed (w, r) = true :=
  star_grow (close_star _ _ _) (w, r) h

theorem source_dropped (D : Dependencies W Row) (seed : W → Row → Bool)
    (w : W) (r : Row) (source : W × Row) (mem : (w, r) ∈ D.changes)
    (used : source ∈ D.sources w r) (gone : dropped D seed source = true) :
    dropped D seed (w, r) = true :=
  normal_closed (close_normal _ _ _) (w, r) mem
    (List.any_eq_true.mpr ⟨source, used, gone⟩)

/-- A direct execution keeps its actual sources even when its net projection
is absent. Its dependent FK effects therefore cannot outlive those sources. -/
theorem direct_source_dropped (M : Writes W Row Col) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (changes : List (W × Row)) (seed : W → Row → Bool)
    (w : W) (r : Row) (action : Direct W Row Payload) (source : W × Row)
    (captured : (E.execution w).direct r = some action)
    (mem : (w, r) ∈ changes) (used : source ∈ action.additionalSources)
    (gone : dropped (dependencies M p P E changes) seed source = true) :
    dropped (dependencies M p P E changes) seed (w, r) = true := by
  apply source_dropped _ seed w r source mem _ gone
  simp only [dependencies, captured, Option.toList_some, List.flatMap_cons,
    List.flatMap_nil, List.append_nil]
  exact List.mem_append_right _ (List.mem_append_left _ used)

theorem dependency_rules_monotone (D : Dependencies W Row) :
    MonotoneRules (FiresP (fun out node => (D.sources node.1 node.2).any out)) := by
  intro a b node sub fired
  obtain ⟨source, used, gone⟩ := List.any_eq_true.mp fired
  exact List.any_eq_true.mpr ⟨source, used, sub source gone⟩

theorem dropped_order_independent (D : Dependencies W Row) (seed : W → Row → Bool)
    (changes : List (W × Row)) (same : ∀ node, node ∈ D.changes ↔ node ∈ changes) :
    dropped D seed = dropped { D with changes := changes } seed := by
  have least (nodes : List (W × Row)) := normal_least (dependency_rules_monotone D)
    (close_star nodes (fun out node => (D.sources node.1 node.2).any out) (fun node => seed node.1 node.2))
    (close_normal nodes (fun out node => (D.sources node.1 node.2).any out) (fun node => seed node.1 node.2))
  have forward := (least D.changes).2.2 _
    (fun node mem fired => (least changes).1 node ((same node).mp mem) fired) (least changes).2.1
  have backward := (least changes).2.2 _
    (fun node mem fired => (least D.changes).1 node ((same node).mpr mem) fired) (least D.changes).2.1
  funext node
  exact Bool.eq_iff_iff.mpr ⟨forward node, backward node⟩

def seed (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (prior : W → Row → Option (Cause W Member Row)) (w : W) (r : Row) : Bool :=
  (prior w r).isSome || !check P E w r

def guard (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (D : Dependencies W Row) (prior : W → Row → Option (Cause W Member Row)) (w : W) (r : Row) :
    Option (Cause W Member Row) :=
  if !check P E w r then
    some (if !allowed P w r then .notOwner (owner P r) else .foreignKey)
  else if (prior w r).isSome then prior w r
  else if dropped D (seed P E prior) (w, r) then
    some (.dependency ((D.sources w r).filter (dropped D (seed P E prior))))
  else none

theorem guard_keeps_check (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (D : Dependencies W Row) (prior : W → Row → Option (Cause W Member Row)) (w : W) (r : Row)
    (kept : guard P E D prior w r = none) : check P E w r = true := by
  cases h : check P E w r
  · simp [guard, h] at kept
  · rfl

theorem guard_keeps_source (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (D : Dependencies W Row) (prior : W → Row → Option (Cause W Member Row)) (w : W) (r : Row)
    (kept : guard P E D prior w r = none) :
    dropped D (seed P E prior) (w, r) = false := by
  have checked := guard_keeps_check P E D prior w r kept
  cases hp : prior w r with
  | none => simpa [guard, checked, hp] using kept
  | some cause => simp [guard, checked, hp] at kept

/-- The ordinary retained-source filter cannot restore a refused change. -/
theorem applied_checked (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (D : Dependencies W Row) (prior : W → Row → Option (Cause W Member Row))
    (w : W) (r : Row) (ch : Change Col)
    (applied : (DataDependency.effective M valid p (base (guard P E D prior))).chg w r = some ch) :
    check P E w r = true := by
  simp only [DataDependency.effective] at applied
  split at applied
  · rename_i kept
    have admitted := ((DataDependency.retained_iff _ w r).mp kept).1
    exact guard_keeps_check P E D prior w r (by simpa [DataDependency.authoredState, base] using admitted)
  · contradiction

theorem every_applied_direct_authorized (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (D : Dependencies W Row) (prior : W → Row → Option (Cause W Member Row))
    (w : W) (r : Row) (ch : Change Col) (origin : (E.execution w).origin r = .direct)
    (owned : P.owned (P.table r) = true)
    (applied : (DataDependency.effective M valid p (base (guard P E D prior))).chg w r = some ch) :
    P.author w = owner P r :=
  direct_authorized P E w r origin owned (applied_checked M valid p P E D prior w r ch applied)

theorem every_applied_statement_authorized (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (D : Dependencies W Row)
    (prior : W → Row → Option (Cause W Member Row))
    (w : W) (r : Row) (ch : Change Col)
    (target : ((E.execution w).directPayload r).isSome = true) (owned : P.owned (P.table r) = true)
    (applied : (DataDependency.effective M valid p (base (guard P E D prior))).chg w r = some ch) :
    P.author w = owner P r :=
  statement_authorized P E w r target owned (applied_checked M valid p P E D prior w r ch applied)

theorem every_applied_derived_exact (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (D : Dependencies W Row) (prior : W → Row → Option (Cause W Member Row))
    (w : W) (r : Row) (ch : Change Col) (d : Derivation Row)
    (origin : (E.execution w).origin r = .foreign d)
    (applied : (DataDependency.effective M valid p (base (guard P E D prior))).chg w r = some ch) :
    ∃ value result, E.payload w r = some value ∧
      evaluate ((E.execution w).schema) (direct P E w) d = some (r, result) ∧
      (E.execution w).schema.withoutGenerated result =
        (E.execution w).schema.withoutGenerated value ∧
      Implied ((E.execution w).schema) (direct P E w) r result :=
  derived_exact P E w r d origin (applied_checked M valid p P E D prior w r ch applied)

/-- A dropped FK cause excludes the original derived row change, including a
recorded delete. Replaying the remaining history restores its prior generation
and values subject to any independent remaining operation or removal rule. -/
theorem dropped_cause_no_effect (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (D : Dependencies W Row) (prior : W → Row → Option (Cause W Member Row))
    (w : W) (r : Row) (source : W × Row) (mem : (w, r) ∈ D.changes)
    (used : source ∈ D.sources w r) (gone : dropped D (seed P E prior) source = true) :
    (DataDependency.effective M valid p (base (guard P E D prior))).chg w r = none := by
  have lost := source_dropped D (seed P E prior) w r source mem used gone
  have denied : base (guard P E D prior) w r = false := by
    unfold base guard
    split
    · rfl
    · cases hp : prior w r with
      | none => simp
      | some cause => simp
  have h := DataDependency.dropped_stays_dropped
    (DataDependency.authoredState M valid p (base (guard P E D prior))) w r denied
  simp [DataDependency.effective, h]

/-- The full recorded payload is tied to the actual row merge input. This
prevents checking a harmless certificate while applying a different patch.
An authenticated direct execution can project to no net row change while
still causing recorded foreign-key effects. -/
structure Capture (M : Writes W Row Col) (E : Evidence W Row Payload)
    (project : Payload → Option (Change Col)) (target : Payload → Row) : Prop where
  target_exact : ∀ w r value, E.payload w r = some value → target value = r
  direct_target_exact : ∀ w r value, (E.execution w).directPayload r = some value → target value = r
  exact : ∀ w r, (E.payload w r).bind project = M.chg w r

theorem applied_payload_exact (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (D : Dependencies W Row)
    (prior : W → Row → Option (Cause W Member Row))
    (project : Payload → Option (Change Col)) (target : Payload → Row) (capture : Capture M E project target)
    (w : W) (r : Row) (ch : Change Col) (d : Derivation Row)
    (origin : (E.execution w).origin r = .foreign d)
    (applied : (DataDependency.effective M valid p (base (guard P E D prior))).chg w r = some ch) :
    ∃ value result, E.payload w r = some value ∧ target value = r ∧ project value = some ch ∧
      evaluate ((E.execution w).schema) (direct P E w) d = some (r, result) ∧
      (E.execution w).schema.withoutGenerated result =
        (E.execution w).schema.withoutGenerated value := by
  obtain ⟨value, result, payload, evaluated, generated, _⟩ :=
    every_applied_derived_exact M valid p P E D prior w r ch d origin applied
  have original : M.chg w r = some ch := by
    simp only [DataDependency.effective] at applied
    split at applied
    · exact applied
    · contradiction
  have eq := capture.exact w r
  rw [payload, original] at eq
  exact ⟨value, result, payload, capture.target_exact w r value payload, eq, evaluated, generated⟩

/-- Dropping the direct cause drops its derived changes through the computed
dependency list, rather than a sender's claimed cause list. -/
theorem dropped_root_no_effect (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (changes : List (W × Row))
    (prior : W → Row → Option (Cause W Member Row))
    (w : W) (r : Row) (d : Derivation Row) (origin : (E.execution w).origin r = .foreign d)
    (mem : (w, r) ∈ changes)
    (gone : dropped (dependencies M p P E changes) (seed P E prior) (w, root d) = true) :
    (DataDependency.effective M valid p
      (base (guard P E (dependencies M p P E changes) prior))).chg w r = none :=
  dropped_cause_no_effect M valid p P E _ prior w r (w, root d) mem
    (derived_uses_root M p P E changes w r d origin) gone

theorem dropped_contributing_root_no_effect (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (changes : List (W × Row))
    (prior : W → Row → Option (Cause W Member Row))
    (w : W) (r source : Row) (d : Derivation Row) (origin : (E.execution w).origin r = .foreign d)
    (mem : (w, r) ∈ changes) (used : source ∈ roots d)
    (gone : dropped (dependencies M p P E changes) (seed P E prior) (w, source) = true) :
    (DataDependency.effective M valid p
      (base (guard P E (dependencies M p P E changes) prior))).chg w r = none := by
  apply dropped_cause_no_effect M valid p P E _ prior w r (w, source) mem _ gone
  simp only [dependencies, origin]
  exact List.mem_append_right _ (List.mem_append_right _ (List.mem_map.mpr
    ⟨source, roots_mem_sources ((E.execution w).schema) (direct P E w) d source used, rfl⟩))

/-- Successful derived changes have their own durable causal record, even
when their old values were knowingly replaced and B14 requires no lost cell. -/
structure DerivedRecord (W Row Payload : Type) where
  write : W
  row : Row
  payload : Payload
  derivation : Derivation Row
  causes : List (W × Row)
  deriving DecidableEq

def derivedRecord (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (E : Evidence W Row Payload) (D : Dependencies W Row)
    (prior : W → Row → Option (Cause W Member Row)) (received : List W)
    (w : W) (r : Row) : Option (DerivedRecord W Row Payload) :=
  if w ∈ received ∧
      DataDependency.retained (DataDependency.authoredState M valid p (base (guard P E D prior))) w r = true then
    match (E.execution w).origin r with
    | .direct => none
    | .foreign d => (E.payload w r).map fun value => ⟨w, r, value, d, (roots d).map (w, ·)⟩
  else none

theorem applied_derived_recorded (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (D : Dependencies W Row)
    (prior : W → Row → Option (Cause W Member Row)) (received : List W)
    (w : W) (r : Row) (ch : Change Col) (d : Derivation Row)
    (mem : w ∈ received) (origin : (E.execution w).origin r = .foreign d)
    (applied : (DataDependency.effective M valid p (base (guard P E D prior))).chg w r = some ch) :
    ∃ value result, derivedRecord M valid p P E D prior received w r =
      some ⟨w, r, value, d, (roots d).map (w, ·)⟩ ∧
      evaluate ((E.execution w).schema) (direct P E w) d = some (r, result) ∧
      (E.execution w).schema.withoutGenerated result =
        (E.execution w).schema.withoutGenerated value := by
  obtain ⟨value, result, payload, checked, generated, _⟩ :=
    every_applied_derived_exact M valid p P E D prior w r ch d origin applied
  have kept : DataDependency.retained
      (DataDependency.authoredState M valid p (base (guard P E D prior))) w r = true := by
    simp only [DataDependency.effective] at applied
    split at applied
    · assumption
    · contradiction
  exact ⟨value, result, by simp [derivedRecord, mem, kept, origin, payload], checked, generated⟩

variable [DecidableEq Col] [DecidableEq K]

/-- Core merge, constraint losses, rejected changes, and derived causal
records agree on every device given the same authenticated inputs. -/
theorem result_converges (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (D : Dependencies W Row)
    (prior : W → Row → Option (Cause W Member Row))
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    {a b : List W} (ca : CausalOrder M a) (cb : CausalOrder M b)
    (same : ∀ w, w ∈ a ↔ w ∈ b) :
    (Ownership.result M valid p (guard P E D prior) values inputs a,
      derivedRecord M valid p P E D prior a) =
    (Ownership.result M valid p (guard P E D prior) values inputs b,
      derivedRecord M valid p P E D prior b) := by
  have records : derivedRecord M valid p P E D prior a = derivedRecord M valid p P E D prior b := by
    funext w r
    simp only [derivedRecord, same w]
  rw [Ownership.result_converges M valid p (guard P E D prior) values inputs ca cb same, records]

/-- The prior row state is recovered by ordinary replay, not by keeping a
second mutable copy of it. Equality covers generations, cells and cell losses. -/
theorem replay_restores_row (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (verdict : W → Row → Option (Cause W Member Row))
    (withoutCause : Writes W Row Col) (received : List W) (r : Row)
    (stamps : withoutCause.ts = M.ts) (past : withoutCause.past = M.past)
    (remaining : ∀ w, withoutCause.chg w r =
      (DataDependency.effective M valid p (base verdict)).chg w r) :
    atRow (merged M valid p verdict received) r =
      atRow (received.foldl (step withoutCause) St.init) r :=
  fold_atRow (M₁ := DataDependency.effective M valid p (base verdict)) (M₂ := withoutCause)
    stamps.symm past.symm (fun w => (remaining w).symm) received St.init St.init rfl

/-- Re-evaluation is a finite least closure of actual causes. It has no
operation that re-enables a loss during the same computation. -/
theorem no_rejection_cycle (D : Dependencies W Row) :
    Terminating (KillStep D.changes (FiresP (fun out node => (D.sources node.1 node.2).any out))) :=
  killStep_terminating _ _

end CovenMerge.Ownership.Admission
