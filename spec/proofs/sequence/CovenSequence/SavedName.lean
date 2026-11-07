/-!
# The saved restore code's name (coven.md §13.1)

A person's saved restore code carries the store's name and the slot that set
it: slot 1 for the name chosen at creation, or a rename's slot. Each of their
devices rewrites the name as it opens the store or applies a rename, but only
when its own slot is later than the entry's; a rewrite never creates an absent
entry, and keeps the code the entry holds (`follow_name`; only setup, joining,
restoring, resetting and member-key replacement save a code over a readable
one). Devices fall behind and write in any order.

`saved_latest`: however many devices write, in whatever order, the entry ends
with the latest slot any of them saw and that slot's name, so it never goes
back to a name a device has moved past (`never_back`), and with the code it
started with (`rewrite_keeps_code`, `rewrites_keep_code`). `absent_stays`: a
forgotten entry stays forgotten. `slotless_puts_back` is the counterexample:
without the slot, a device that opens while behind puts back the old name.
`overwrite_puts_back_code` is the other: a rewrite that saves the device's own
code puts back a code the person's other device has replaced.
-/
namespace CovenSequence.SavedName

variable {Name Code : Type}

/-- A saved entry's name, the slot that set it, and the restore code it holds. -/
structure Entry (Name Code : Type) where
  slot : Nat
  name : Name
  code : Code

/-- A device that has applied through the slot setting `name` rewrites the
entry: only a later slot replaces it, and an absent entry stays absent. It
writes the name and slot and keeps the entry's code. -/
def rewrite : Option (Entry Name Code) → Nat → Name → Option (Entry Name Code)
  | none, _, _ => none
  | some e, slot, name => if e.slot < slot then some ⟨slot, name, e.code⟩ else some e

theorem absent_stays (slot : Nat) (name : Name) :
    rewrite (none : Option (Entry Name Code)) slot name = none := rfl

theorem never_back (e : Entry Name Code) (slot : Nat) (name : Name) :
    ∃ e', rewrite (some e) slot name = some e' ∧ e.slot ≤ e'.slot := by
  by_cases h : e.slot < slot
  · exact ⟨⟨slot, name, e.code⟩, by simp [rewrite, h], by dsimp only; omega⟩
  · exact ⟨e, by simp [rewrite, h], Nat.le_refl _⟩

/-- **A name rewrite keeps the code the entry holds.** -/
theorem rewrite_keeps_code (e : Entry Name Code) (slot : Nat) (name : Name) :
    (rewrite (some e) slot name).map (·.code) = some e.code := by
  by_cases h : e.slot < slot <;> simp [rewrite, h]

/-- Devices' rewrites in order, each with the slot it had reached; the name a
slot set is `nameAt` that slot. -/
def rewrites (nameAt : Nat → Name) (start : Option (Entry Name Code)) (slots : List Nat) :
    Option (Entry Name Code) :=
  slots.foldl (fun e s => rewrite e s (nameAt s)) start

/-- **The entry keeps the latest name.** Starting from an entry set at slot
`s0`, rewrites in any order leave the latest slot among them and its name. -/
theorem saved_latest (nameAt : Nat → Name) (code : Code) (s0 : Nat) :
    ∀ slots : List Nat, rewrites nameAt (some ⟨s0, nameAt s0, code⟩) slots =
      some ⟨slots.foldl max s0, nameAt (slots.foldl max s0), code⟩ := by
  intro slots
  induction slots generalizing s0 with
  | nil => rfl
  | cons s rest ih =>
    simp only [rewrites, List.foldl_cons]
    by_cases h : s0 < s
    · have : rewrite (some ⟨s0, nameAt s0, code⟩) s (nameAt s) = some ⟨s, nameAt s, code⟩ := by
        simp [rewrite, h]
      rw [this, show max s0 s = s by omega]
      exact ih s
    · have : rewrite (some ⟨s0, nameAt s0, code⟩) s (nameAt s) = some ⟨s0, nameAt s0, code⟩ := by
        simp [rewrite, h]
      rw [this, show max s0 s = s0 by omega]
      exact ih s0

/-- **However many devices rewrite, in whatever order, the entry keeps the code
it started with.** -/
theorem rewrites_keep_code (nameAt : Nat → Name) (e : Entry Name Code) :
    ∀ slots : List Nat, (rewrites nameAt (some e) slots).map (·.code) = some e.code := by
  intro slots
  induction slots generalizing e with
  | nil => rfl
  | cons s rest ih =>
    simp only [rewrites, List.foldl_cons]
    obtain ⟨e', h, _⟩ := never_back e s (nameAt s)
    have kept := rewrite_keeps_code e s (nameAt s)
    rw [h] at kept ⊢
    simp only [Option.map_some, Option.some.injEq] at kept
    have := ih e'
    rw [← kept]
    exact this

/-- Rewriting without the slot: whichever device writes last wins. -/
def overwrite (_ : Option (Entry Name Code)) (slot : Nat) (name : Name) (code : Code) :
    Option (Entry Name Code) :=
  some ⟨slot, name, code⟩

/-- **Counterexample: no slot.** Ana's laptop saves Family, from Ben's rename at
slot 41. Her phone, still behind at slot 1, then opens Household. Overwriting
puts Household back; comparing slots keeps Family. -/
theorem slotless_puts_back :
    let nameAt := fun s => if s = 41 then "Family" else "Household"
    let start : Option (Entry String Nat) := some ⟨1, "Household", 0⟩
    ((overwrite (overwrite start 41 (nameAt 41) 0) 1 (nameAt 1) 0).map (·.name)) =
      some "Household" ∧
    ((rewrite (rewrite start 41 (nameAt 41)) 1 (nameAt 1)).map (·.name)) = some "Family" := by
  decide

/-- **Counterexample: a rewrite that saves the device's own code.** Ana's phone
replaced her member keys and saved code 2. Her laptop, still holding code 1,
then applies a rename and rewrites the entry. Writing its own code puts code 1
back; a name rewrite keeps code 2. -/
theorem overwrite_puts_back_code :
    let start : Option (Entry String Nat) := some ⟨1, "Household", 2⟩
    ((overwrite start 41 "Family" 1).map (·.code)) = some 1 ∧
    ((rewrite start 41 "Family").map (·.code)) = some 2 := by
  decide

end CovenSequence.SavedName
