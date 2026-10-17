import CovenQueue.Conversion

/-! §17.1: the conversions a received late old-schema write takes come from
the app's migration list and the kept raise, never from the device's rows.

Each step of the app's migration list is breaking or an addition. A step is
breaking when its schemas before and after differ by more than new tables and
columns, or when a statement inserts, updates or deletes rows of a synced
table, whether or not any row matches. A write at version `v` under a kept
raise to `r` takes the converters of the breaking steps after `v` through
`r`, in version order; one without a converter makes it lost. An empty
range converts by the identity (Rust `migration_receive::available`).

`Classifier` names how a device decides a step is breaking from its
statements and its own run's row effects. Deciding by statements
(`byStatements`) ignores the rows, so every device derives the same plan
(`statement_plans_agree`). Deciding by rows changed (Rust before T252,
`byRows`) depends on the device's data: a step that fills a new column from
existing rows is breaking on a device with rows and an addition on one
without, and the two devices interpret the same late write differently
(`row_classification_diverges`). -/

namespace CovenQueue.Chain

/-- One app migration step as every device runs it: whether its schemas
differ beyond additions, whether a statement writes rows of a synced table,
and its supplied converter. -/
structure Step where
  version : Nat
  schemaBreaking : Bool
  writesRows : Bool
  convert : Option (List Nat → List Nat)

/-- How a device classifies a step, given whether its own run changed rows. -/
abbrev Classifier := Step → Bool → Bool

/-- §17.1 as decided: the statements alone. -/
def byStatements : Classifier := fun s _ => s.schemaBreaking || s.writesRows

/-- Rust before T252: a writing statement counted only if it changed a row. -/
def byRows : Classifier := fun s changed => s.schemaBreaking || (s.writesRows && changed)

/-- A classifier that never consults the device's own row effects. -/
def DataIndependent (classify : Classifier) : Prop := ∀ s a b, classify s a = classify s b

theorem byStatements_independent : DataIndependent byStatements := fun _ _ _ => rfl

/-- The breaking steps after `older` through `raise`, as this device
classified them with its own row effects `changed`. -/
def between (classify : Classifier) (changed : Nat → Bool) (steps : List Step)
    (older raise : Nat) : List Step :=
  steps.filter fun s => classify s (changed s.version) && decide (older < s.version) &&
    decide (s.version ≤ raise)

/-- Compose converters in version order; a breaking step without one fails. -/
def chain : List Step → Option (List Nat → List Nat)
  | [] => some id
  | s :: rest => match s.convert, chain rest with
    | some f, some g => some (g ∘ f)
    | _, _ => none

/-- A write's plan under a kept raise. -/
def plan (classify : Classifier) (changed : Nat → Bool) (steps : List Step) (raise : Nat)
    (r : Record) : ConversionPlan :=
  if r.version < raise then
    match r.disposition with
    | .lost _ | .migration => .keep
    | .ordinary => match chain (between classify changed steps r.version raise) with
      | none => .lost raise
      | some f => .convert raise f
  else .keep

/-- Devices sharing the app's migration list and the kept raise derive the
same plan for every write, whatever rows each run changed, when the
classification ignores those rows. -/
theorem statement_plans_agree (classify : Classifier) (independent : DataIndependent classify)
    (steps : List Step) (raise : Nat) (r : Record) (mine theirs : Nat → Bool) :
    plan classify mine steps raise r = plan classify theirs steps raise r := by
  have same : between classify mine steps r.version raise =
      between classify theirs steps r.version raise := by
    unfold between
    congr 1
    funext s
    rw [independent s (mine s.version) (theirs s.version)]
  simp only [plan, same]

/-- One breaking step that is the raise is the per-raise `Conversion` the
receive proofs take, so every `Conversion` theorem applies to it. -/
theorem one_step_is_conversion (classify : Classifier) (changed : Nat → Bool) (s : Step)
    (breaking : classify s (changed s.version) = true) (r : Record) :
    (plan classify changed [s] s.version r).record =
      (conversionPlan ⟨s.version, s.convert⟩ r).record := by
  unfold plan conversionPlan
  by_cases older : r.version < s.version
  · simp only [older, ite_true]
    cases r.disposition with
    | lost _ => rfl
    | migration => rfl
    | ordinary =>
      simp only [between, List.filter, breaking, decide_eq_true older, Nat.le_refl,
        decide_true, Bool.and_self]
      rcases convert : s.convert with _ | f
      · simp [chain, convert]
      · simp [chain, convert]
  · simp [older]

/-- A range without breaking steps has the identity conversion. -/
theorem additions_pass_unchanged (classify : Classifier) (changed : Nat → Bool)
    (steps : List Step) (raise : Nat) (r : Record) (old : r.version < raise)
    (ordinary : r.disposition = .ordinary)
    (additions : between classify changed steps r.version raise = []) :
    plan classify changed steps raise r = .convert raise id := by
  simp [plan, old, ordinary, additions, chain]

/-- If every breaking step through the raise is at or below `newest`, a
write at or above that version crosses no breaking step. -/
theorem newest_breaking_range_empty (steps : List Step) (newest raise version : Nat)
    (latest : ∀ s ∈ steps, byStatements s false = true → s.version ≤ raise →
      s.version ≤ newest)
    (current : newest ≤ version) :
    between byStatements (fun _ => false) steps version raise = [] := by
  apply List.filter_eq_nil_iff.mpr
  intro s member
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  intro ⟨⟨breaking, after⟩, through⟩
  have := latest s member breaking through
  omega

/-- Eligible changes keep every field and value under identity conversion;
only the received interpretation's schema version advances. -/
theorem identity_record (r : Record) (version : Nat) :
    (ConversionPlan.convert version id).record (fun _ => none) r =
      { r with version } := by
  have unchanged : convertChange id (fun _ => none) = id := by
    funext ch
    rcases ch with ⟨row, generation, operation, provenance, content⟩
    cases content <;> rfl
  simp [ConversionPlan.record, ConversionPlan.prepare, ConversionPlan.change, unchanged]

/-- Writes at the newest breaking version through a raise are admitted with
unchanged values when there is no reset, refusal or failed data source. -/
theorem newest_breaking_write_admitted (steps : List Step) (newest raise entry : Nat)
    (r : Record) (old : r.version < raise) (ordinary : r.disposition = .ordinary)
    (latest : ∀ s ∈ steps, byStatements s false = true → s.version ≤ raise →
      s.version ≤ newest)
    (current : newest ≤ r.version) :
    let received := (plan byStatements (fun _ => false) steps raise r).record
      (fun _ => none) r
    received = { r with version := raise } ∧
      classify ⟨some ⟨⟨entry, []⟩, raise⟩, none, []⟩ ⟨received, none⟩ = .applied := by
  have empty := newest_breaking_range_empty steps newest raise r.version latest current
  rw [additions_pass_unchanged _ _ _ _ _ old ordinary empty, identity_record]
  simp [classify, classify.schema, outsideRaise, ordinary]

/-- Every retained breaking step needs a converter, including when additions
follow it. An empty range satisfies this requirement. -/
theorem chain_available_iff (steps : List Step) :
    (chain steps).isSome = steps.all (fun s => s.convert.isSome) := by
  induction steps with
  | nil => rfl
  | cons s rest ih =>
    simp only [chain, List.all_cons]
    cases s.convert <;> cases h : chain rest <;> simp_all

/-- One retained breaking step still uses exactly its callback, even when
its version precedes the raise. Absence still excludes the older write. -/
theorem retained_step_required (classify : Classifier) (changed : Nat → Bool)
    (steps : List Step) (raise : Nat) (r : Record) (s : Step)
    (old : r.version < raise) (ordinary : r.disposition = .ordinary)
    (retained : between classify changed steps r.version raise = [s]) :
    plan classify changed steps raise r =
      match s.convert with
      | none => .lost raise
      | some f => .convert raise f := by
  simp only [plan, old, ite_true, ordinary, retained, chain]
  cases s.convert <;> rfl

/-- An older write cannot skip a breaking step whose converter is absent,
regardless of the other steps and additions through the raise. -/
theorem below_breaking_needs_converter (steps : List Step) (raise : Nat) (r : Record)
    (s : Step) (member : s ∈ steps) (breaking : byStatements s false = true)
    (older : r.version < s.version) (through : s.version ≤ raise)
    (missing : s.convert = none) (ordinary : r.disposition = .ordinary) :
    plan byStatements (fun _ => false) steps raise r = .lost raise := by
  have retained : s ∈ between byStatements (fun _ => false) steps r.version raise := by
    simp [between, member, breaking, older, through]
  have absent : chain (between byStatements (fun _ => false) steps r.version raise) = none := by
    cases found : chain (between byStatements (fun _ => false) steps r.version raise) with
    | none => rfl
    | some f =>
      have all := chain_available_iff (between byStatements (fun _ => false) steps r.version raise)
      rw [found] at all
      have supplied := List.all_eq_true.mp all.symm s retained
      simp [missing] at supplied
  have old : r.version < raise := by omega
  simp [plan, old, ordinary, absent]

/-- Migration 2 adds `slug` and fills it from `title`, with a converter that
fills it for a late write. Ana's device had notes; Ben's ran it on an empty
table, as a fresh device does. -/
def slug : Step := ⟨2, false, true, some fun values => values ++ [1]⟩

def lateEdit : Record :=
  ⟨⟨⟨3, 9⟩, 9, [], []⟩, 1, .ordinary, [⟨0, 0, .upd, ⟨none, []⟩, .values [5]⟩]⟩

/-- Classified by rows changed, Ana converts the late edit and Ben, with no
step to convert through, applies it unchanged: the two devices hold different
rows (E-D1). -/
theorem row_classification_diverges :
    ((plan byRows (fun _ => true) [slug] 2 lateEdit).record (fun _ => none) lateEdit).body ≠
      ((plan byRows (fun _ => false) [slug] 2 lateEdit).record (fun _ => none) lateEdit).body := by
  decide

/-- Classified by statements, both convert it alike. -/
theorem statement_classification_converts :
    ((plan byStatements (fun _ => false) [slug] 2 lateEdit).record (fun _ => none) lateEdit).body =
      [⟨0, 0, .upd, ⟨none, []⟩, .values [5, 1]⟩] := by
  decide

/-- An addition after breaking version 2 leaves a version-2 write unchanged,
but version 1 still uses the retained converter, or loses if it is absent. -/
theorem addition_raise_cases :
    let addition : Step := ⟨3, false, false, none⟩
    let missing : Step := { slug with convert := none }
    let atTwo := { lateEdit with version := 2 }
    (plan byStatements (fun _ => false) [missing, addition] 3 atTwo).record
        (fun _ => none) atTwo = { atTwo with version := 3 } ∧
    (plan byStatements (fun _ => false) [missing, addition] 3 lateEdit).record
        (fun _ => none) lateEdit = { lateEdit with disposition := .lost 3 } ∧
    ((plan byStatements (fun _ => false) [slug, addition] 3 lateEdit).record
        (fun _ => none) lateEdit).body =
      [⟨0, 0, .upd, ⟨none, []⟩, .values [5, 1]⟩] := by
  decide

end CovenQueue.Chain
