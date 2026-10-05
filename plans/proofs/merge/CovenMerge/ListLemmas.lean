/-! Counting lemmas used as termination measures. -/

namespace CovenMerge

theorem countP_le_of {α : Type} {l : List α} {p q : α → Bool}
    (hpq : ∀ y, p y = true → q y = true) : l.countP p ≤ l.countP q := by
  induction l with
  | nil => simp
  | cons y l ih =>
    simp only [List.countP_cons]
    cases hp : p y <;> cases hq : q y <;> simp
    · omega
    · omega
    · exact absurd (hpq y hp) (by simp [hq])
    · omega

theorem countP_lt_of {α : Type} {l : List α} {p q : α → Bool}
    (hpq : ∀ y, p y = true → q y = true)
    {x : α} (hx : x ∈ l) (hq : q x = true) (hp : p x = false) :
    l.countP p < l.countP q := by
  induction l with
  | nil => simp at hx
  | cons y l ih =>
    simp only [List.countP_cons]
    rcases List.mem_cons.1 hx with h | h
    · subst h
      rw [hp, hq]
      have := countP_le_of (l := l) hpq
      simp
      omega
    · have := ih h
      cases hp' : p y <;> cases hq' : q y <;> simp
      · omega
      · omega
      · exact absurd (hpq y hp') (by simp [hq'])
      · omega

theorem countP_pos_of {α : Type} {l : List α} {p : α → Bool} {x : α} (hx : x ∈ l)
    (h : p x = true) : 0 < l.countP p :=
  List.countP_pos_iff.2 ⟨x, hx, h⟩

theorem any_congr_mem {α : Type} {l : List α} {p q : α → Bool}
    (h : ∀ a ∈ l, p a = q a) : l.any p = l.any q := by
  induction l with
  | nil => rfl
  | cons a l ih =>
    simp only [List.any_cons]
    rw [h a List.mem_cons_self, ih (fun b hb => h b (List.mem_cons_of_mem a hb))]

end CovenMerge
