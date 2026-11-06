/-!
# The saved restore code's name (coven.md §13.1)

A person's saved restore code carries the store's name and the slot that set
it: slot 1 for the name chosen at creation, or a rename's slot. Each of their
devices rewrites the name as it opens the store or applies a rename, but only
when its own slot is later than the entry's; a rewrite never creates an absent
entry. Devices fall behind and write in any order.

`saved_latest`: however many devices write, in whatever order, the entry ends
with the latest slot any of them saw and that slot's name, so it never goes
back to a name a device has moved past (`never_back`). `absent_stays`: a
forgotten entry stays forgotten. `slotless_puts_back` is the counterexample:
without the slot, a device that opens while behind puts back the old name.
-/
namespace CovenSequence.SavedName

variable {Name : Type}

/-- A saved entry's name and the slot that set it. -/
structure Entry (Name : Type) where
  slot : Nat
  name : Name

/-- A device that has applied through the slot setting `name` rewrites the
entry: only a later slot replaces it, and an absent entry stays absent. -/
def rewrite : Option (Entry Name) → Nat → Name → Option (Entry Name)
  | none, _, _ => none
  | some e, slot, name => if e.slot < slot then some ⟨slot, name⟩ else some e

theorem absent_stays (slot : Nat) (name : Name) : rewrite (none : Option (Entry Name)) slot name = none := rfl

theorem never_back (e : Entry Name) (slot : Nat) (name : Name) :
    ∃ e', rewrite (some e) slot name = some e' ∧ e.slot ≤ e'.slot := by
  by_cases h : e.slot < slot
  · exact ⟨⟨slot, name⟩, by simp [rewrite, h], by dsimp only; omega⟩
  · exact ⟨e, by simp [rewrite, h], Nat.le_refl _⟩

/-- Devices' rewrites in order, each with the slot it had reached; the name a
slot set is `nameAt` that slot. -/
def rewrites (nameAt : Nat → Name) (start : Option (Entry Name)) (slots : List Nat) :
    Option (Entry Name) :=
  slots.foldl (fun e s => rewrite e s (nameAt s)) start

/-- **The entry keeps the latest name.** Starting from an entry set at slot
`s0`, rewrites in any order leave the latest slot among them and its name. -/
theorem saved_latest (nameAt : Nat → Name) (s0 : Nat) :
    ∀ slots : List Nat, rewrites nameAt (some ⟨s0, nameAt s0⟩) slots =
      some ⟨slots.foldl max s0, nameAt (slots.foldl max s0)⟩ := by
  intro slots
  induction slots generalizing s0 with
  | nil => rfl
  | cons s rest ih =>
    simp only [rewrites, List.foldl_cons]
    by_cases h : s0 < s
    · have : rewrite (some ⟨s0, nameAt s0⟩) s (nameAt s) = some ⟨s, nameAt s⟩ := by
        simp [rewrite, h]
      rw [this, show max s0 s = s by omega]
      exact ih s
    · have : rewrite (some ⟨s0, nameAt s0⟩) s (nameAt s) = some ⟨s0, nameAt s0⟩ := by
        simp [rewrite, h]
      rw [this, show max s0 s = s0 by omega]
      exact ih s0

/-- Rewriting without the slot: whichever device writes last wins. -/
def overwrite (_ : Option (Entry Name)) (slot : Nat) (name : Name) : Option (Entry Name) :=
  some ⟨slot, name⟩

/-- **Counterexample: no slot.** Ana's laptop saves Family, from Ben's rename at
slot 41. Her phone, still behind at slot 1, then opens Household. Overwriting
puts Household back; comparing slots keeps Family. -/
theorem slotless_puts_back :
    let nameAt := fun s => if s = 41 then "Family" else "Household"
    let start : Option (Entry String) := some ⟨1, "Household"⟩
    ((overwrite (overwrite start 41 (nameAt 41)) 1 (nameAt 1)).map (·.name)) = some "Household" ∧
    ((rewrite (rewrite start 41 (nameAt 41)) 1 (nameAt 1)).map (·.name)) = some "Family" := by
  decide

end CovenSequence.SavedName
