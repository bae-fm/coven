import CovenMerge.Ownership

namespace CovenMerge.Ownership.FilterExamples

inductive Write where
  | create | intrude | dependent | concurrent
  deriving DecidableEq, Repr

open Write

def history : Writes Write Bool Unit where
  ts | .create => 0 | .intrude => 1 | .dependent => 2 | .concurrent => 3
  past w a := decide ((w ≠ .create ∧ a = .create) ∨ (w = .dependent ∧ a = .intrude))
  chg w _ := some ⟨if w = .create then .ins else .upd,
    if w = .create then 0 else 1, fun _ => true⟩

theorem valid : Valid history where
  ts_inj a b := by cases a <;> cases b <;> simp [history]
  past_ts w a := by cases w <;> cases a <;> decide
  gen_seen w r ch h := by
    cases w <;> simp only [history, reduceCtorEq, ↓reduceIte, Option.some.injEq] at h
    · subst ch; exact Or.inl rfl
    all_goals subst ch; exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
  parity w r ch h := by
    cases w <;> simp only [history, reduceCtorEq, ↓reduceIte, Option.some.injEq] at h <;>
      subst ch <;> decide

def source (w : Write) (r : Bool) : Write :=
  if w = .dependent ∧ r = false then .intrude else .create

def provenance : DataDependency.Authored history where
  provenance w r := if w = .create then ⟨none, []⟩ else ⟨some .create, [((), source w r)]⟩
  read_source w r a h := by
    cases w <;> cases r <;> cases a <;> simp_all [DataDependency.Provenance.sources, history, source]
  generation_source w r ch h := by
    cases w <;> simp only [history, reduceCtorEq, ↓reduceIte, Option.some.injEq] at h
    · subst ch; exact Or.inl ⟨rfl, rfl⟩
    all_goals subst ch; exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
  setter_source w r c a h := by
    cases w <;> cases r <;> cases a <;> simp [source] at h
    all_goals exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

def policy : Policy Write Bool Bool Nat where
  table := id
  permission r := if r then .anyone else .owner
  creator _ := .create
  author | .create | .dependent => 0 | .intrude | .concurrent => 1
  admin _ := false

def permission (w : Write) (r : Bool) : Option (Cause Write Nat Bool) :=
  if allowed policy w r then none else some (.notOwner (owner policy r))

abbrev evidence := DataDependency.authoredState history valid provenance (base permission)

theorem retained (w : Write) (r : Bool) :
    DataDependency.retained evidence w r = (r || decide (w = .create)) := by
  have create (r : Bool) : DataDependency.retained evidence .create r = true := by
    rw [DataDependency.retained_eq]; cases r <;> rfl
  have intrude (r : Bool) : DataDependency.retained evidence .intrude r = r := by
    rw [DataDependency.retained_eq]
    cases r
    · rfl
    · change ([Write.create, .create].all (fun a => DataDependency.retained evidence a true)) = true
      simp [create]
  cases w with
  | create => simp [create]
  | intrude => simp [intrude]
  | dependent =>
    rw [DataDependency.retained_eq]
    cases r
    · change ([Write.create, .intrude].all (fun a => DataDependency.retained evidence a false)) = false
      simp [create, intrude]
    · change ([Write.create, .create].all (fun a => DataDependency.retained evidence a true)) = true
      simp [create]
  | concurrent =>
    rw [DataDependency.retained_eq]
    cases r
    · rfl
    · change ([Write.create, .create].all (fun a => DataDependency.retained evidence a true)) = true
      simp [create]

def values (w : Write) (_r : Bool) (_c : Unit) : Option Write := some w

def first : List Write := [.create, .intrude, .dependent, .concurrent]
def second : List Write := [.create, .concurrent, .intrude, .dependent]

theorem first_causal : CausalOrder history first := by
  apply CausalOrder.snoc (M := history) (L := [.create, .intrude, .dependent]) (w := .concurrent)
  · apply CausalOrder.snoc (M := history) (L := [.create, .intrude]) (w := .dependent)
    · apply CausalOrder.snoc (M := history) (L := [.create]) (w := .intrude)
      · apply CausalOrder.snoc (M := history) (L := []) (w := .create) .nil <;> simp [history]
      · decide
      · intro a h; cases a <;> simp_all [history]
    · decide
    · intro a h; cases a <;> simp_all [history]
  · decide
  · intro a h; cases a <;> simp_all [history]

theorem second_causal : CausalOrder history second := by
  apply CausalOrder.snoc (M := history) (L := [.create, .concurrent, .intrude]) (w := .dependent)
  · apply CausalOrder.snoc (M := history) (L := [.create, .concurrent]) (w := .intrude)
    · apply CausalOrder.snoc (M := history) (L := [.create]) (w := .concurrent)
      · apply CausalOrder.snoc (M := history) (L := []) (w := .create) .nil <;> simp [history]
      · decide
      · intro a h; cases a <;> simp_all [history]
    · decide
    · intro a h; cases a <;> simp_all [history]
  · decide
  · intro a h; cases a <;> simp_all [history]

theorem orders_agree :
    merged history valid provenance permission first =
      merged history valid provenance permission second :=
  converges history valid provenance permission first_causal second_causal
    (by intro w; cases w <;> decide)

theorem per_change_keeps_unrelated :
    (merged history valid provenance permission first).cell false () = some .create ∧
    (merged history valid provenance permission first).cell true () = some .concurrent := by
  simp only [merged, Data.merged, DataDependency.effective, retained]
  decide

theorem non_owner_loss :
    (loss history valid provenance permission values first .intrude false).map (fun l => l.cause) =
      some (.notOwner 0) := by
  simp [loss, permission, first, history, allowed, policy, owner, changeLoss]

theorem dependent_loss :
    (loss history valid provenance permission values first .dependent false).map (fun l => l.cause) =
      some (.dependency [(.intrude, false)]) := by
  have dep : DataDependency.dependsOnDropped evidence .dependent false = true := by
    change [Write.create, .intrude].any (fun a => !DataDependency.retained evidence a false) = true
    simp [retained]
  have sources : DataDependency.droppedSources evidence .dependent false = [.intrude] := by
    change [Write.create, .intrude].filter (fun a => !DataDependency.retained evidence a false) = [.intrude]
    simp [retained]
  simp only [loss, first, List.mem_cons, reduceCtorEq, or_true,
    true_or, ↓reduceIte, history, Option.bind_some, permission, allowed, policy, owner]
  change (if DataDependency.dependsOnDropped evidence .dependent false then
    some (changeLoss .dependent false 1 (values .dependent false)
      (.dependency ((DataDependency.droppedSources evidence .dependent false).map (·, false))))
    else none).map (fun l => l.cause) = some (.dependency [(.intrude, false)])
  rw [dep, sources]
  rfl

/-! The same writes when row `false` is in a table limited to admins. Member
1 writes `.intrude` and `.concurrent`. Demoted before their slots, member 1's
changes of that row are refused as a non-admin's, whoever created the row; promoted
before them, they apply. -/

def adminPolicy (promoted : Bool) : Policy Write Bool Bool Nat :=
  { policy with
    permission := fun r => if r then .anyone else .admin
    admin := fun w => decide (policy.author w = 0) || promoted }

def adminPermission (promoted : Bool) (w : Write) (r : Bool) : Option (Cause Write Nat Bool) :=
  if allowed (adminPolicy promoted) w r then none else some (refusal (adminPolicy promoted) r)

/-- Demoted, member 1 may change row `false` exactly where the owner rule let
it: so the same changes survive. -/
theorem demoted_same_base : base (adminPermission false) = base permission := by
  funext w r; cases w <;> cases r <;> rfl

theorem demoted_loss :
    (loss history valid provenance (adminPermission false) values first .intrude false).map
      (fun l => l.cause) = some .notAdmin := by
  simp [loss, adminPermission, first, history, allowed, adminPolicy, policy, refusal, changeLoss]

theorem demoted_keeps_unrelated :
    (merged history valid provenance (adminPermission false) first).cell false () = some .create ∧
    (merged history valid provenance (adminPermission false) first).cell true () =
      some .concurrent := by
  simp only [merged, demoted_same_base]
  exact per_change_keeps_unrelated

/-- Promoted, every write may change both rows. -/
theorem promoted_base : base (adminPermission true) = fun _ _ => true := by
  funext w r; cases w <;> cases r <;> rfl

abbrev promotedEvidence :=
  DataDependency.authoredState history valid provenance (base (adminPermission true))

theorem promoted_retained (w : Write) (r : Bool) :
    DataDependency.retained promotedEvidence w r = true := by
  have create (r : Bool) : DataDependency.retained promotedEvidence .create r = true := by
    rw [DataDependency.retained_eq]; cases r <;> rfl
  have intrude (r : Bool) : DataDependency.retained promotedEvidence .intrude r = true := by
    rw [DataDependency.retained_eq]
    cases r
    · change ([Write.create, .create].all (fun a => DataDependency.retained promotedEvidence a false)) = true
      simp [create]
    · change ([Write.create, .create].all (fun a => DataDependency.retained promotedEvidence a true)) = true
      simp [create]
  cases w with
  | create => exact create r
  | intrude => exact intrude r
  | dependent =>
    rw [DataDependency.retained_eq]
    cases r
    · change ([Write.create, .intrude].all (fun a => DataDependency.retained promotedEvidence a false)) = true
      simp [create, intrude]
    · change ([Write.create, .create].all (fun a => DataDependency.retained promotedEvidence a true)) = true
      simp [create]
  | concurrent =>
    rw [DataDependency.retained_eq]
    cases r
    · change ([Write.create, .create].all (fun a => DataDependency.retained promotedEvidence a false)) = true
      simp [create]
    · change ([Write.create, .create].all (fun a => DataDependency.retained promotedEvidence a true)) = true
      simp [create]

theorem promoted_applies :
    loss history valid provenance (adminPermission true) values first .intrude false = none ∧
    (merged history valid provenance (adminPermission true) first).cell false () =
      some .concurrent := by
  constructor
  · have independent : DataDependency.dependsOnDropped promotedEvidence .intrude false = false :=
      DataDependency.retained_not_dependent _ _ _ (promoted_retained .intrude false)
    simp only [loss, first, List.mem_cons, reduceCtorEq, true_or, or_true, ↓reduceIte, history,
      Option.bind_some, adminPermission, allowed, adminPolicy, policy]
    change (if DataDependency.dependsOnDropped promotedEvidence .intrude false then _ else none) = none
    rw [independent]
    rfl
  · simp only [merged, Data.merged, DataDependency.effective, promoted_retained]
    decide

end CovenMerge.Ownership.FilterExamples
