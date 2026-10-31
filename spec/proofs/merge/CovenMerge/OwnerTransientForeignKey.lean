import CovenMerge.OwnerCascadeAuthorization

/-! An UPDATE SET NULL action can survive a direct target's return to its
original values. Its authenticated execution remains a cause even though
that target supplies no net row change to the core merge. -/
namespace CovenMerge.Ownership.TransientForeignKey

open ForeignKey Admission
open CascadeAuthorization (Row Write Member creator policy received prior)

/-- The author changes a referenced unique value from 7 to 8 and back to 7.
The first update nulls the child reference; the second no longer reaches it. -/
structure Recorded where
  first : Effect Row Unit Nat
  following : List (Effect Row Unit Nat)
  net : Option (Effect Row Unit Nat)
  deriving DecidableEq

def history : Writes Write Row Unit where
  ts := CascadeAuthorization.history.ts
  past := CascadeAuthorization.history.past
  chg
    | .track, .track | .review, .review => some ⟨.ins, 0, fun _ => true⟩
    | .erase, .review => some ⟨.upd, 1, fun _ => true⟩
    | _, _ => none

theorem valid : Valid history where
  ts_inj := CascadeAuthorization.valid.ts_inj
  past_ts := CascadeAuthorization.valid.past_ts
  gen_seen w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    all_goals first
      | exact Or.inl rfl
      | exact Or.inr ⟨.review, _, rfl, rfl, by decide, rfl⟩
  parity w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch; decide

def provenance : DataDependency.Authored history where
  provenance w r := if w = .erase ∧ r = .review then ⟨some .review, [((), .review)]⟩ else ⟨none, []⟩
  read_source w r a h := by
    cases w <;> cases r <;> cases a <;>
      simp_all [DataDependency.Provenance.sources, history, CascadeAuthorization.history]
  generation_source w r ch h := by
    cases w <;> cases r <;> simp only [history, Option.some.injEq, reduceCtorEq] at h
    all_goals subst ch
    all_goals first
      | exact Or.inl ⟨rfl, rfl⟩
      | exact Or.inr ⟨.review, _, rfl, rfl, by decide, rfl⟩
  setter_source w r c a h := by
    cases w <;> cases r <;> cases a <;> simp at h
    all_goals exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

def record (effect : Effect Row Unit Nat) : Recorded := ⟨effect, [], some effect⟩

def parent : Recorded :=
  ⟨⟨.track, 1, .upd, [((), some 8)]⟩, [⟨.track, 1, .upd, [((), some 7)]⟩], none⟩

def child := record ⟨.review, 1, .upd, [((), none)]⟩

def reference : Reference Row Unit Nat :=
  ⟨.track, 1, .review, 1, [()], [some 7], .cascade, .setNull, [()], []⟩

/-- The resolved authoring execution has one child action. Once it is NULL,
the restored parent key has no reference through which to act on that child. -/
def schema : Schema Row Recorded where
  consequences _ input := (consequence reference input.first).toList.map fun e => (e.row, record e)
  combine a b := do
    let first ← a.2.net
    let second ← b.2.net
    let result ← combineEffects first second
    pure (result.row, record result)
  withoutGenerated := id

def payload (w : Write) (r : Row) : Option Recorded :=
  if w = .erase then some (if r = .track then parent else child)
  else (history.chg w r).map fun ch => record ⟨r, ch.gen, ch.kind, [((), some 7)]⟩

def evidence : Evidence Write Row Recorded where
  payload := payload
  execution w := {
    direct := fun r => (if w = .erase ∧ r = .review then none else payload w r).map
      fun value => ⟨value, ((CascadeAuthorization.provenance.provenance w r).sources.map (·, r))⟩
    origin := fun r => if w = .erase ∧ r = .review then .foreign (.foreign (.direct .track) 0) else .direct
    schema := schema }

def project (input : Recorded) : Option (Change Unit) :=
  input.net.map CascadeAuthorization.project

theorem captured : Capture history evidence project (fun input => input.first.row) where
  target_exact w r value h := by
    cases w <;> cases r <;> simp [evidence, payload, history] at h <;> cases h <;> rfl
  direct_target_exact w r value h := by
    cases w <;> cases r <;> simp [Execution.directPayload, evidence, payload, history] at h <;> cases h <;> rfl
  exact w r := by cases w <;> cases r <;> rfl

def changes := CascadeAuthorization.changes

def causes := dependencies history provenance policy evidence changes

abbrev permission := Admission.guard policy evidence causes prior

def proposed (row : Row) (value : Recorded) : Evidence Write Row Recorded :=
  { evidence with payload := fun w r =>
      if w = .erase ∧ r = row then some value else payload w r }

def forged := proposed .review (record ⟨.review, 1, .upd, [((), some 99)]⟩)

def changedTrace := proposed .track
  ({ parent with following := [⟨.track, 1, .upd, [((), some 9)]⟩] } : Recorded)

theorem absent_net_cause_authorizes :
    history.chg .erase .track = none ∧ project parent = none ∧
    allowed policy .erase .review = false ∧
    check policy evidence .erase .track = true ∧
    permission .erase .review = none ∧
    check policy forged .erase .review = false := by decide

theorem changed_trace_refused :
    check policy changedTrace .erase .track = false := by decide

theorem retained (w : Write) (r : Row) :
    DataDependency.retained (DataDependency.authoredState history valid provenance (base permission)) w r =
      (history.chg w r).isSome := by
  rw [DataDependency.retained_eq]
  cases w <;> cases r
  all_goals first | rfl | skip
  change (true && true && [Write.review, .review].all _) = true
  simp only [List.all_cons, List.all_nil, Bool.and_true, Bool.true_and]
  rw [DataDependency.retained_eq]
  rfl

theorem applied :
    (merged history valid provenance permission received).gen .track = 1 ∧
    (merged history valid provenance permission received).cell .review () = some .erase ∧
    derivedRecord history valid provenance policy evidence causes prior received .erase .review =
      some ⟨.erase, .review, child, .foreign (.direct .track) 0, [(.erase, .track)]⟩ := by
  simp only [merged, Data.merged, DataDependency.effective, derivedRecord, retained]
  decide

abbrev refused := Admission.guard policy evidence causes CascadeAuthorization.droppedParent

theorem dropped_root_reverses :
    (merged history valid provenance refused received).cell .review () = some .review := by
  have kept (w : Write) (r : Row) :
      DataDependency.retained (DataDependency.authoredState history valid provenance (base refused)) w r =
        ((history.chg w r).isSome && decide (w ≠ .erase)) := by
    rw [DataDependency.retained_eq]
    cases w <;> cases r <;> decide
  simp only [merged, Data.merged, DataDependency.effective, kept]
  decide

end CovenMerge.Ownership.TransientForeignKey
