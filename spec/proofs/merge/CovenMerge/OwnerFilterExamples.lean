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
  owned r := !r
  creator _ := .create
  author | .create | .dependent => 0 | .intrude | .concurrent => 1

def unguarded (_w : Write) (_r : Bool) : Option (Cause Write Nat Bool) := none

abbrev evidence := DataDependency.authoredState history valid provenance (base policy unguarded)

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
    merged history valid provenance policy unguarded first =
      merged history valid provenance policy unguarded second :=
  converges history valid provenance policy unguarded first_causal second_causal
    (by intro w; cases w <;> decide)

theorem per_change_keeps_unrelated :
    (merged history valid provenance policy unguarded first).cell false () = some .create ∧
    (merged history valid provenance policy unguarded first).cell true () = some .concurrent := by
  simp only [merged, Data.merged, DataDependency.effective, retained]
  decide

theorem non_owner_loss :
    (loss history valid provenance policy unguarded values first .intrude false).map (fun l => l.cause) =
      some (.notOwner 0) := by
  simp [loss, first, history, allowed, policy, owner, changeLoss]

theorem dependent_loss :
    (loss history valid provenance policy unguarded values first .dependent false).map (fun l => l.cause) =
      some (.dependency [.intrude]) := by
  have dep : DataDependency.dependsOnDropped evidence .dependent false = true := by
    change [Write.create, .intrude].any (fun a => !DataDependency.retained evidence a false) = true
    simp [retained]
  have sources : DataDependency.droppedSources evidence .dependent false = [.intrude] := by
    change [Write.create, .intrude].filter (fun a => !DataDependency.retained evidence a false) = [.intrude]
    simp [retained]
  rw [Ownership.dependent_recorded history valid provenance policy unguarded values first .dependent false _
    (by decide) rfl (by decide) rfl dep]
  simp [sources, changeLoss]

end CovenMerge.Ownership.FilterExamples
