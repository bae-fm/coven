import CovenStorelogData.Model

namespace CovenStorelogData

open CovenMerge

/-- A value is accounted for by the app cell, a causal replacement, a cell
loss, or a whole-row loss carrying a rule that holds in the current view. -/
def Accounted {W Col K : Type} [DecidableEq W] [DecidableEq K]
    (M : Writes W Row Col) (S : W → Prop) (st : St W Row Col)
    (I : Inputs Row K) (r : Row) (c : Col) (a : W) (k : Nat) : Prop :=
  (st.cell r c = some a ∧ (view I).shown r = true) ∨
  (∃ w, Replacer M S r c a k w ∧ M.past w a = true) ∨
  (∃ x, st.lost r c a = some (k, x)) ∨
  (st.cell r c = some a ∧ (view I).removed r = true ∧ (view I).rules r ≠ [])

/-- No value disappears when ordinary writes and the current removal rules
are composed. Reversing a log entry cannot reverse a causal delete. -/
theorem durability {W Col K : Type} [DecidableEq W] [DecidableEq K]
    {M : Writes W Row Col} (hv : Valid M) {S : W → Prop} (hc : Closed M S)
    {st : St W Row Col} (hs : IsSpec M S st) (I : Inputs Row K)
    (present : ∀ r, I.present r = decide (st.gen r % 2 = 1))
    {r : Row} {c : Col} {a : W} {k : Nat} (ha : Setter M S r c a k) :
    Accounted M S st I r c a k := by
  classical
  by_cases read : ∃ w, Replacer M S r c a k w ∧ M.past w a = true
  · exact Or.inr (Or.inl read)
  by_cases replaced : ∃ w, Replacer M S r c a k w
  · obtain ⟨x, hx⟩ := canon_exists hv hc hs ha
    refine Or.inr (Or.inr (Or.inl ⟨x, (hs.lost r c a k x).mpr
      ⟨ha, replaced, ?_, hx⟩⟩))
    intro w hw
    cases hp : M.past w a
    · rfl
    · exact False.elim (read ⟨w, hw, hp⟩)
  · obtain ⟨gen, cell⟩ := unreplaced hv hc hs ha replaced
    have odd := setter_inc_odd hv ha
    have hp : I.present r = true := by simp [present, ← gen, odd]
    cases hr : removal I r
    · exact Or.inl ⟨cell, by simp [view, hp, hr]⟩
    · have removed : (view I).removed r = true := by simp [view, hp, hr]
      exact Or.inr (Or.inr (Or.inr ⟨cell, removed, removed_has_rule I removed⟩))

/-- The concrete coupling supplies the generation-based presence required by
durability; deleted circles come from replay, not an assumed permission bit. -/
theorem coupled_durability {W Col K : Type} [DecidableEq W] [DecidableEq K]
    {M : Writes W Row Col} (hv : Valid M) {S : W → Prop} (hc : Closed M S)
    {st : St W Row Col} (hs : IsSpec M S st) (schema : Schema W Col K)
    (log : CovenStorelog.Log) (result : CovenStorelog.Result)
    {r : Row} {c : Col} {a : W} {k : Nat} (ha : Setter M S r c a k) :
    Accounted M S st (inputs schema M log result st) r c a k :=
  durability hv hc hs _ (fun _ => by simp [inputs]) ha

end CovenStorelogData
