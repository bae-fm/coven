import CovenQueue.Conversion

/-! §17.1: the conversions a received late old-schema write takes come from
the app's migration list and the kept raise, never from the device's rows.

Each step of the app's migration list is breaking or an addition. A step is
breaking when its schemas before and after differ by more than new tables and
columns, or when a statement inserts, updates or deletes rows of a synced
table, whether or not any row matches. A write at version `v` under a kept
raise to `r` takes the converters of the breaking steps after `v` through
`r`, in version order; one without a converter, or a range holding none,
makes it lost (Rust `migration_receive::available`).

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

/-- Compose converters in version order; a breaking step without one fails,
and so does an empty range. -/
def chain : List Step → Option (List Nat → List Nat)
  | [] => none
  | [s] => s.convert
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

/-- Migration 2 adds `slug` and fills it from `title`, with a converter that
fills it for a late write. Ana's device had notes; Ben's ran it on an empty
table, as a fresh device does. -/
def slug : Step := ⟨2, false, true, some fun values => values ++ [1]⟩

def lateEdit : Record :=
  ⟨⟨⟨3, 9⟩, 9, [], []⟩, 1, .ordinary, [⟨0, 0, .upd, ⟨none, []⟩, .values [5]⟩]⟩

/-- Classified by rows changed, Ana converts the late edit and Ben, with no
step to convert through, freezes it as lost: the two devices hold different
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

end CovenQueue.Chain
