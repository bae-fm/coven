/-!
# Newman's lemma

A terminating relation whose one-step forks always rejoin (locally
confluent) is confluent, so every element has exactly one normal form.
-/

namespace CovenMerge

section
variable {α : Type} (R : α → α → Prop)

/-- Zero or more `R` steps. -/
inductive Star : α → α → Prop where
  | refl (a : α) : Star a a
  | head {a b c : α} : R a b → Star b c → Star a c

variable {R}

theorem Star.trans {a b c : α} (h₁ : Star R a b) (h₂ : Star R b c) : Star R a c := by
  induction h₁ with
  | refl => exact h₂
  | head hab _ ih => exact Star.head hab (ih h₂)

theorem Star.single {a b : α} (h : R a b) : Star R a b := Star.head h (Star.refl b)

variable (R)

def Joinable (b c : α) : Prop := ∃ d, Star R b d ∧ Star R c d

def LocallyConfluent : Prop := ∀ a b c, R a b → R a c → Joinable R b c

def Confluent : Prop := ∀ a b c, Star R a b → Star R a c → Joinable R b c

/-- No infinite chain of steps: the reverse relation is well-founded. -/
def Terminating : Prop := WellFounded (fun b a => R a b)

def Normal (a : α) : Prop := ¬ ∃ b, R a b

variable {R}

/-- **Newman's lemma.** -/
theorem newman (ht : Terminating R) (hl : LocallyConfluent R) : Confluent R := by
  intro a
  induction a using WellFounded.induction ht with
  | _ a ih =>
    intro b c hab hac
    cases hab with
    | refl => exact ⟨c, hac, Star.refl c⟩
    | head hab₁ hb₁b =>
      rename_i b₁
      cases hac with
      | refl => exact ⟨b, Star.refl b, Star.head hab₁ hb₁b⟩
      | head hac₁ hc₁c =>
        rename_i c₁
        obtain ⟨d, hb₁d, hc₁d⟩ := hl a b₁ c₁ hab₁ hac₁
        obtain ⟨e, hbe, hde⟩ := ih b₁ hab₁ b d hb₁b hb₁d
        obtain ⟨f, hcf, hef⟩ := ih c₁ hac₁ c e hc₁c (hc₁d.trans hde)
        exact ⟨f, hbe.trans hef, hcf⟩

theorem star_normal_eq {a b : α} (hn : Normal R a) (h : Star R a b) : a = b := by
  cases h with
  | refl => rfl
  | head hab _ => exact absurd ⟨_, hab⟩ hn

/-- In a confluent system, an element reaches at most one normal form. -/
theorem unique_normal (hc : Confluent R) {a b c : α} (hab : Star R a b) (hac : Star R a c)
    (hb : Normal R b) (hc' : Normal R c) : b = c := by
  obtain ⟨d, hbd, hcd⟩ := hc a b c hab hac
  rw [star_normal_eq hb hbd, star_normal_eq hc' hcd]

/-- In a terminating system, every element reaches some normal form. -/
theorem exists_normal (ht : Terminating R) (a : α) : ∃ b, Star R a b ∧ Normal R b := by
  induction a using WellFounded.induction ht with
  | _ a ih =>
    by_cases h : ∃ b, R a b
    · obtain ⟨b, hab⟩ := h
      obtain ⟨c, hbc, hc⟩ := ih b hab
      exact ⟨c, Star.head hab hbc, hc⟩
    · exact ⟨a, Star.refl a, h⟩

end

end CovenMerge
