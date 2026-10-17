import CovenMerge.ForeignKeyClassification

/-! What a write's author checks before the write leaves, and which rows'
net changes must end where their steps end (§9.6, D5).

* `LocalAccept`: the author runs the receivers' label-free rule (`cause`) on
  its own write and commits only when every step on a row its writer may not
  change is classified as an action. What SQLite's action code made never
  enters: `local_accept_iff` proves the author accepts exactly the writes
  whose every step the rule calls direct is on a row the writer may change,
  the condition every receiver applies. `rebuild_refusal_refuses` is one
  half: the author refuses whatever receivers would, so no device keeps an
  effect the others refuse. `accepted_rooted` composes the check with
  `every_step_rooted`: every step of an accepted write, on any row, has a
  certificate rooted in direct steps its writer may change, so every receiver
  admits it. `TriggerFirst.author_accepts` is the other half on a checked
  history: a trigger's step that equals the cascade's exact consequence is
  accepted by the author as by every receiver.
* `checkSteps` is the receiver's check as coven runs it. A row's net change is
  the payload that merges; its steps are the execution that classification
  reads. A direct row needs its writer's permission, and its net change must
  equal where its steps end only when its steps cause actions, since only then
  do they justify effects on other rows. `checkSteps_relaxed` reduces it to
  `Admission.check` over evidence that proposes those other rows' steps as
  their payload, so every Admission theorem holds for it.
  `applied_root_ends_where_steps_end` proves what the condition buys: every
  root of an applied derived change is a row whose merged net change is where
  its steps end. `InconsistentCause` is the counterexample without it: a track
  whose steps delete it but whose net change only renames it would still set
  Ana's note's reference NULL, while the track stays. -/
namespace CovenMerge.Ownership.Classification

variable {Row Ref Payload : Type} [DecidableEq Payload]

/-- The author's check before its write leaves: the receivers' rule, run on
its own steps. -/
def LocalAccept (R : Rule Row Ref Payload) (t : Trace Row Ref Payload)
    (permitted : Row → Bool) : Prop :=
  ∀ i s, t.steps[i]? = some s → permitted (R.row s) = false → (cause R t i).isSome = true

/-- **The author accepts exactly what receivers accept.** Its check holds
exactly when every step the receivers' rule calls direct is on a row its
writer may change (`every_step_rooted`'s condition). -/
theorem local_accept_iff (R : Rule Row Ref Payload) (t : Trace Row Ref Payload)
    (permitted : Row → Bool) :
    LocalAccept R t permitted ↔
      ∀ j p, cause R t j = none → t.steps[j]? = some p → permitted (R.row p) = true := by
  constructor
  · intro accept j p direct hp
    cases hj : permitted (R.row p) with
    | true => rfl
    | false =>
      have := accept j p hp hj
      rw [direct] at this
      cases this
  · intro direct_ok i s h forbidden
    cases hc : cause R t i with
    | some _ => rfl
    | none =>
      have := direct_ok i s hc h
      rw [forbidden] at this
      cases this

/-- **The author refuses whatever receivers would.** A step on a row its
writer may not change, which the receivers' rule calls direct, refuses the
whole write at its author. -/
theorem rebuild_refusal_refuses {R : Rule Row Ref Payload} {t : Trace Row Ref Payload}
    {permitted : Row → Bool} {i : Nat} {s : Payload}
    (h : t.steps[i]? = some s) (forbidden : permitted (R.row s) = false)
    (direct : cause R t i = none) : ¬ LocalAccept R t permitted := by
  intro accept
  have := accept i s h forbidden
  rw [direct] at this
  cases this

/-- **What the author commits, every receiver admits.** -/
theorem accepted_rooted {R : Rule Row Ref Payload} {t : Trace Row Ref Payload}
    {permitted : Row → Bool} (accept : LocalAccept R t permitted)
    (i : Nat) (s : Payload) (h : t.steps[i]? = some s) :
    ∃ d, ForeignKey.evaluate (stepSchema R t) (directSteps R t) d = some (i, s) ∧
      ∀ j ∈ ForeignKey.roots d, ∃ p, t.steps[j]? = some p ∧ permitted (R.row p) = true :=
  every_step_rooted R t permitted ((local_accept_iff R t permitted).mp accept) i s h

/-- In `TriggerFirst`, Ben may change only his track 0. A trigger made the
deletion of Ana's review 2, and receivers classify it as the track
deletion's action; its author runs the same rule and accepts the write. -/
theorem TriggerFirst.author_accepts :
    TriggerFirst.made 2 = .trigger ∧ (cause stepRule TriggerFirst.trace 2).isSome = true ∧
    LocalAccept stepRule TriggerFirst.trace (fun r => decide (r = 0)) := by
  refine ⟨rfl, by decide, fun i s h forbidden => ?_⟩
  rcases i with _ | _ | _ | i
  · simp [TriggerFirst.trace] at h
    subst h
    cases forbidden
  · decide
  · decide
  · simp [TriggerFirst.trace] at h

end CovenMerge.Ownership.Classification

namespace CovenMerge.Ownership.Admission

open ForeignKey

variable {W Row Col Table Member Payload : Type}
  [DecidableEq W] [DecidableEq Row] [DecidableEq Member] [DecidableEq Payload]

/-- The receiver's check as coven runs it: a direct row's net change must end
where its steps end only when its steps cause actions (`causes`). -/
def checkSteps (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (causes : W → Row → Bool) (w : W) (r : Row) : Bool :=
  match (E.execution w).origin r with
  | .direct => allowed P w r &&
      (!causes w r || decide (E.payload w r = (E.execution w).directPayload r))
  | .foreign _ => check P E w r

/-- The evidence `checkSteps` checks: a direct row whose steps cause nothing
proposes where its steps end, whatever its net change. -/
def relaxed (E : Evidence W Row Payload) (causes : W → Row → Bool) : Evidence W Row Payload where
  payload w r := match (E.execution w).origin r with
    | .direct => if causes w r then E.payload w r else (E.execution w).directPayload r
    | .foreign _ => E.payload w r
  execution := E.execution

theorem checkSteps_relaxed (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (causes : W → Row → Bool) (w : W) (r : Row) :
    checkSteps P E causes w r = check P (relaxed E causes) w r := by
  cases origin : (E.execution w).origin r with
  | direct =>
    simp only [checkSteps, check, origin, relaxed]
    cases causes w r <;> simp
  | foreign d =>
    simp only [checkSteps, check, origin, relaxed]
    rfl

/-- Where `causes` holds the relaxed evidence proposes the net change itself. -/
theorem relaxed_cause (E : Evidence W Row Payload) (causes : W → Row → Bool) (w : W) (r : Row)
    (c : causes w r = true) : (relaxed E causes).payload w r = E.payload w r := by
  simp only [relaxed]
  split <;> simp [c]

/-- **An applied action rests on what merges.** Every root of an applied
derived change, when it is a row whose steps cause actions, has its net
change exactly where its steps end. -/
theorem applied_root_ends_where_steps_end (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (causes : W → Row → Bool) (changes : List (W × Row))
    (prior : W → Row → Option (Cause W Member Row))
    (w : W) (r s : Row) (ch : Change Col) (d : Derivation Row)
    (origin : (E.execution w).origin r = .foreign d) (mem : (w, r) ∈ changes)
    (used : s ∈ roots d) (root : (E.execution w).origin s = .direct) (cause : causes w s = true)
    (applied : (DataDependency.effective M valid p (base (guard P (relaxed E causes)
      (dependencies M p P (relaxed E causes) changes) prior))).chg w r = some ch) :
    E.payload w s = (E.execution w).directPayload s := by
  let E' := relaxed E causes
  let D := dependencies M p P E' changes
  have kept : guard P E' D prior w r = none := by
    simp only [DataDependency.effective] at applied
    split at applied
    · rename_i retained
      have := ((DataDependency.retained_iff _ w r).mp retained).1
      simpa [DataDependency.authoredState, base] using this
    · contradiction
  have alive := guard_keeps_source P E' D prior w r kept
  have source : (w, s) ∈ D.sources w r := by
    simp only [D, dependencies, E', relaxed]
    refine List.mem_append_right _ (List.mem_append_right _ ?_)
    simp only [origin]
    exact List.mem_map.mpr ⟨s, roots_mem_sources _ _ d s used, rfl⟩
  have seeded : seed P E' prior w s = false := by
    cases h : seed P E' prior w s with
    | false => rfl
    | true =>
      have gone := source_dropped D (seed P E' prior) w r (w, s) mem source
        (seed_dropped D (seed P E' prior) w s h)
      rw [alive] at gone
      cases gone
  have checked : check P E' w s = true := by
    simp only [seed, Bool.or_eq_false_iff, Bool.not_eq_false'] at seeded
    exact seeded.2
  have := (checkSteps_relaxed P E causes w s).symm ▸ checked
  simp only [checkSteps, root, cause, Bool.not_true, Bool.false_or, Bool.and_eq_true,
    decide_eq_true_eq] at this
  exact this.2

end CovenMerge.Ownership.Admission

namespace CovenMerge.Ownership.InconsistentCause

/-! Ben's track 0 has steps that delete it, but its net change only renames it
(column 5 to the value 9). Ana's note 1 holds a SET NULL reference to it in
column 10. Ben may change only his track. -/

open ForeignKey Admission

def setNull : Reference Nat Nat Nat := ⟨0, 1, 1, 3, [10], [some 7], .setNull, .setNull, [20], []⟩

def stepsEnd (r : Nat) : Option (Effect Nat Nat Nat) :=
  if r = 0 then some ⟨0, 1, .del, []⟩ else none

def evidence : Evidence Nat Nat (Effect Nat Nat Nat) where
  payload _ r :=
    if r = 0 then some ⟨0, 1, .upd, [(5, some 9)]⟩
    else if r = 1 then some ⟨1, 3, .upd, [(10, none)]⟩ else none
  execution _ := {
    direct := fun r => (stepsEnd r).map fun e => ⟨e, []⟩
    origin := fun r => if r = 1 then .foreign (.foreign (.direct 0) 0) else .direct
    schema := resolvedSchema [setNull] }

def policy : Policy Nat Nat Nat Nat where
  table := id
  owned r := decide (r = 1)
  creator r := r
  author w := if w = 1 then 1 else 0

/-- Without the condition both changes pass: the note's NULL is the exact
consequence of a deletion the merge never applies, since the track's net
change is a rename. -/
theorem accepted_without_condition :
    checkSteps policy evidence (fun _ _ => false) 0 0 = true ∧
    checkSteps policy evidence (fun _ _ => false) 0 1 = true ∧
    evidence.payload 0 0 ≠ (evidence.execution 0).directPayload 0 := by decide

/-- With it the track's change is refused, so the note's change, which uses
it as a source, drops with it. -/
theorem refused_with_condition :
    checkSteps policy evidence (fun _ r => decide (r = 0)) 0 0 = false := by decide

end CovenMerge.Ownership.InconsistentCause
