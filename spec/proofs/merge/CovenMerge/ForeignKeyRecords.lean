import CovenMerge.ForeignKeyClassification

/-! Steps as a write records them, and the consequence a receiver compares a
step with (§8.4, §8.8, D5). `ForeignKeyClassification` decides with any
`Rule`; this is the rule coven's receiver runs, which the merge runner's
`classify` mode compares with the Rust receiver.

* `Recorded` is a step as D5 records it: its effect, the parent each
  reference through each written column names, and for the old half of a
  primary-key update the row and incarnation its new half makes (§8.5).
* `Held` is a reference as a row holds it at one moment. Besides the
  reference's own columns, an action records every column sharing a reference
  with them, with the value it has at that moment (`carried`), and the parent
  each other reference over those columns names (`others`), all in the
  child's column affinity (`store`). `others` is `none` when the write does
  not determine those parents: a reference the action points at a row the
  write never touched names that row's incarnation in the authoring view,
  which no step carries.
* `heldConsequence` is the step an action through a held reference records.
  `held_follows_cause` is `follows_cause` for it: every parent an action
  names is the row and incarnation its cause leaves, the new half of a key
  update, or one the held reference already determined. -/
namespace CovenMerge.Ownership.Classification

open ForeignKey

variable {Row Col Value Name : Type}
  [DecidableEq Row] [DecidableEq Col] [DecidableEq Value] [DecidableEq Name]

structure Recorded (Row Col Value Name : Type) where
  effect : Effect Row Col Value
  parents : List (Col × Name × Row × Nat)
  becomes : Option (Row × Nat)
  deriving DecidableEq

structure Held (Row Col Value Name : Type) where
  ref : Reference Row Col Value
  name : Name
  store : List (Col × Value × Value)
  carried : List (Col × Option Value)
  others : Option (List (Col × Name × Row × Nat))

/-- A value as the child's column `c` stores it. -/
def Held.stored (h : Held Row Col Value Name) (c : Col) : Option Value → Option Value
  | none => none
  | some v => match h.store.find? (fun e => e.1 = c ∧ e.2.1 = v) with
    | some e => some e.2.2
    | none => some v

/-- The parent an action's own columns name: the new half of a key update,
or the reference's parent when the parent's key changed in place. -/
def namedParent (h : Held Row Col Value Name) (p : Recorded Row Col Value Name) : Row × Nat :=
  p.becomes.getD (h.ref.parent, h.ref.parentGeneration)

/-- The step an action through `h` on the parent step `p` records. A delete
records no cells. An update records the reference's own columns, NULL or the
parent's new key as the child stores it, then the carried columns; its own
columns name the parent when they hold a key, and the other references the
parents `others` gives. -/
def heldConsequence (h : Held Row Col Value Name) (p : Recorded Row Col Value Name) :
    Option (Recorded Row Col Value Name) :=
  h.others.bind fun others => (consequence h.ref p.effect).map fun e =>
    if e.kind = .upd then
      let own := e.cells.map fun cell => (cell.1, h.stored cell.1 cell.2)
      let named := if own.all (fun cell => cell.2.isSome) then
        h.ref.columns.map fun c => (c, h.name, namedParent h p) else []
      ⟨{ e with cells := own ++ h.carried }, named ++ others, none⟩
    else ⟨e, [], none⟩

def heldRule : Rule Row (Held Row Col Value Name) (Recorded Row Col Value Name) :=
  ⟨fun s => s.effect.row, heldConsequence⟩

/-- **An action follows its cause.** Every parent an action's step names is
either the row and incarnation its cause leaves, through the acting
reference's own columns, or one the held reference already determined. -/
theorem held_follows_cause
    {t : Trace Row (Held Row Col Value Name) (Recorded Row Col Value Name)} {i j : Nat}
    (h : cause heldRule t i = some j) :
    ∃ s p f, t.steps[i]? = some s ∧ t.steps[j]? = some p ∧ f ∈ t.refs i s.effect.row ∧
      ∀ named ∈ s.parents,
        named ∈ f.ref.columns.map (fun c => (c, f.name, namedParent f p)) ∨
        named ∈ f.others.getD [] := by
  obtain ⟨_, s, p, hs, hp, f, mem, act⟩ := cause_candidate h
  refine ⟨s, p, f, hs, hp, mem, fun named inParents => ?_⟩
  simp only [heldRule, heldConsequence] at act
  obtain ⟨others, ho, rest⟩ := Option.bind_eq_some_iff.mp act
  obtain ⟨e, _, eq⟩ := Option.map_eq_some_iff.mp rest
  rw [ho]
  simp only [Option.getD_some]
  split at eq
  · subst eq
    simp only [List.mem_append] at inParents
    rcases inParents with own | other
    · left
      split at own
      · exact own
      · cases own
    · exact Or.inr other
  · subst eq
    cases inParents

/-- Values only, with no other reference determined: the rule refuses. -/
theorem undetermined_refuses (h : Held Row Col Value Name) (p : Recorded Row Col Value Name)
    (unknown : h.others = Option.none) : heldConsequence h p = Option.none := by
  simp [heldConsequence, unknown]

end CovenMerge.Ownership.Classification
