import SqliveNarrowing.Rules.Term

/-! A row value ordered against another, `(a1, ..., an) >= (b1, ..., bn)`, or
by `>`, `<=` or `<`, is true only where its first pair orders the same way or
ties: `a1 >= b1`. So it narrows as that comparison does too, which gives a
window that starts at a key the first term's bound (`leading_comparison`). -/
namespace SqliveNarrowing.RowOrder

/-- The ordering that passes what `op` passes, and a tie. -/
def orTied : Op → Option Op
  | .lt | .le => some .le
  | .gt | .ge => some .ge
  | _ => none

/-- The comparison of its first pair that a row value ordering holds only
with. -/
def lead : Cond → Option Cond
  | .rowOrder collation op l r _ =>
    (orTied op).map fun weak =>
      match collation with
      | none => .cmp weak l r
      | some key => .collated key weak l r
  | _ => none

/-- A row value ordering true of a binding makes its first pair's comparison
true of it. -/
theorem lead_sound {cd found : Cond} (h : lead cd = some found) {b : Bind} {sub : Subs}
    (hc : isTrue (cd.eval b sub)) : isTrue (found.eval b sub) := by
  unfold lead at h
  split at h
  · rename_i collation op l r rest
    simp only [Option.map_eq_some_iff] at h
    obtain ⟨weak, hweak, rfl⟩ := h
    simp only [Cond.eval, isTrue, beq_iff_eq] at hc
    cases hl : l.eval b sub with
    | none => simp [hl, rowOrderTruth] at hc
    | some x =>
      cases hr : r.eval b sub with
      | none => simp [hl, hr, rowOrderTruth] at hc
      | some y =>
        rw [hl, hr] at hc
        simp only [rowOrderTruth] at hc
        have hpass : ∀ x' y' : Int, x' = y' ∨ op.compare x' y' = true →
            weak.compare x' y' = true := by
          intro x' y' h'
          cases op <;> simp only [orTied, reduceCtorEq, Option.some.injEq] at hweak <;>
            subst hweak <;> simp only [Op.compare, decide_eq_true_eq] at h' ⊢ <;> omega
        have hfirst : ∀ key : Int → Int, collation.getD id = key →
            weak.compare (key x) (key y) = true := by
          intro key hkey
          rw [hkey] at hc
          apply hpass
          by_cases he : key x = key y
          · exact Or.inl he
          · simp only [he, ↓reduceIte, Option.some.injEq] at hc
            exact Or.inr hc
        have hop : ∀ a c : Int, weak.apply (some a) (some c) = some (weak.compare a c) := by
          intro a c
          cases op <;> simp only [orTied, reduceCtorEq, Option.some.injEq] at hweak <;>
            subst hweak <;> rfl
        cases collation with
        | none =>
          simp only [Cond.eval, hl, hr, isTrue, hop, beq_iff_eq, Option.some.injEq]
          exact hfirst id rfl
        | some key =>
          simp only [Cond.eval, hl, hr, isTrue, Option.map_some, hop, beq_iff_eq,
            Option.some.injEq]
          exact hfirst key rfl
  · cases h

/-- The terms a condition's first pair gives, besides what the rules read of
the whole of it: those the term reader reads of the comparison. -/
def terms (s : Statement) (k : Nat) (cd : Cond) : List (Nat × STerm) :=
  (((lead cd).bind (Term.readCondition s k)).map (·.terms)).getD []

/-- Each term is on an item of core `k`, and holds of its row wherever the
condition holds. -/
theorem terms_sound (s : Statement) (k : Nat) (cd : Cond) :
    ∀ i t, (i, t) ∈ terms s k cd → (s.item i).core = k ∧
      ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → t.holds (b i) := by
  intro i t ht
  unfold terms at ht
  cases hl : lead cd with
  | none => simp [hl] at ht
  | some found =>
    cases hr : Term.readCondition s k found with
    | none => simp [hl, hr] at ht
    | some n =>
      simp only [hl, Option.bind_some, hr, Option.map_some, Option.getD_some] at ht
      obtain ⟨hcore, hholds⟩ := Term.term_sound.terms hr i t ht
      exact ⟨hcore, fun db f b hc => hholds db f b (lead_sound hl hc)⟩

end SqliveNarrowing.RowOrder
