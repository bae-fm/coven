import CovenMerge.Converge
import CovenStorage.Clocks

/-! §7.2 and §8.1: a write made after seeing another wins over it. This is a
statement about competing values, not about unrelated cells or a third, later
write. Generation changes keep a deleted incarnation out after re-insertion
(§8.3). -/

namespace CovenMerge

section
variable {W Row Col : Type} [DecidableEq W] {M : Writes W Row Col}
  {S : W → Prop} {st : St W Row Col} {r : Row} {c : Col} {a w : W} {k : Nat}

/-- A replacement that read a value prevents that value from being an unread
loss, even when a concurrent replacement arrived first. -/
theorem seen_replacer_not_lost (hS : IsSpec M S st)
    (value : Setter M S r c a k) (replaces : Replacer M S r c a k w)
    (seen : M.past w a = true) :
    st.lost r c a = none := by
  cases hl : st.lost r c a with
  | none => rfl
  | some p =>
    obtain ⟨ka, x⟩ := p
    have loss := (hS.lost r c a ka x).1 hl
    have same : ka = k := setter_inc_eq loss.1 value
    subst ka
    have unread := loss.2.2.1 w replaces
    rw [seen] at unread
    contradiction

/-- The value from an older incarnation never becomes a current cell again. -/
theorem older_incarnation_not_current (hS : IsSpec M S st)
    (value : Setter M S r c a k) (older : k < st.gen r) : st.cell r c ≠ some a := by
  intro current
  have same := setter_inc_eq value ((hS.cell_some r c a).1 current).1
  omega

/-- §7.2 and §8.1, for every state satisfying the merge definition: a setter that
read another setter of the same cell and incarnation beats it. This remains
true after further writes, deletion, or re-insertion. -/
theorem cell_after_read_wins (valid : Valid M) (hS : IsSpec M S st)
    (value : Setter M S r c a k) (later : Setter M S r c w k)
    (seen : M.past w a = true) :
    st.cell r c ≠ some a ∧ st.lost r c a = none := by
  have stamp := valid.past_ts w a seen
  constructor
  · intro current
    have winner := (hS.cell_some r c a).1 current
    have same := setter_inc_eq value winner.1
    have bound := winner.2 w (same ▸ later)
    omega
  · exact seen_replacer_not_lost hS value (Or.inl ⟨later, stamp⟩) seen

/-- If the new setter has read every competing setter in the current
incarnation, its value is exactly the current cell's value. -/
theorem cell_after_all_reads_wins (valid : Valid M) (hS : IsSpec M S st)
    (later : Setter M S r c w (st.gen r))
    (seen : ∀ a, Setter M S r c a (st.gen r) → a ≠ w → M.past w a = true) :
    st.cell r c = some w := by
  apply (hS.cell_some r c w).2
  refine ⟨later, fun a ha => ?_⟩
  by_cases same : a = w
  · subst a; exact Nat.le_refl _
  · exact Nat.le_of_lt (valid.past_ts w a (seen a ha same))

/-- A delete made after reading a cell advances past its incarnation,
removes that value, and leaves no unread loss for it. -/
theorem delete_after_read_wins (hS : IsSpec M S st)
    (value : Setter M S r c a k) (deleted : Del M S r w k)
    (seen : M.past w a = true) :
    k < st.gen r ∧ st.cell r c ≠ some a ∧ st.lost r c a = none := by
  have older := del_lt hS deleted
  exact ⟨older, older_incarnation_not_current hS value older,
    seen_replacer_not_lost hS value (Or.inr deleted) seen⟩

/-- While a row is deleted, every cell is absent. -/
theorem deleted_row_has_no_cells (valid : Valid M) (hS : IsSpec M S st)
    (deleted : st.gen r % 2 = 0) : st.cell r c = none := by
  cases current : st.cell r c with
  | none => rfl
  | some a =>
    have odd := setter_inc_odd valid ((hS.cell_some r c a).1 current).1
    omega

/-- Re-inserting after a delete starts a later incarnation. The old delete
record remains, but cannot remove the new incarnation; old setters cannot
reach it, regardless of their timestamps. -/
theorem reinsert_after_delete_wins (valid : Valid M) (hS : IsSpec M S st)
    {d : W} {ch : Change Col} (deleted : Del M S r d k) (hw : S w)
    (change : M.chg w r = some ch) (insert : ch.kind = .ins)
    (generation : ch.gen = k + 1) (seen : M.past w d = true) :
    M.ts d < M.ts w ∧ k + 2 ≤ st.gen r ∧
    (∃ x, st.genWrite r (k + 1) = some x ∧ MinDel M S r k x) ∧
    (∀ c a, Setter M S r c a k → st.cell r c ≠ some a) := by
  have advanced : k + 2 ≤ st.gen r := gen_ge hS
    ⟨hw, ch, change, by rw [insert]; decide, by omega⟩
  refine ⟨valid.past_ts w d seen, advanced, ?_, ?_⟩
  · cases record : st.genWrite r (k + 1) with
    | none =>
      exact ((hS.gw_none r (k + 1) record d)
        ((genChange_succ_iff_del valid (del_odd valid deleted) d).2 deleted)).elim
    | some x => exact ⟨x, rfl, (gw_minDel valid hS (del_odd valid deleted) x).1 record⟩
  · intro c a value
    exact older_incarnation_not_current hS value (by omega)

/-- §7.2, §8.1 and §8.3 on every device, quantified over arbitrary causal application
orders containing the competing cell writes or the later delete. -/
theorem write_after_read_wins (valid : Valid M) {L : List W}
    (causal : CausalOrder M L) (value : Setter M (fun x => x ∈ L) r c a k)
    (later : Setter M (fun x => x ∈ L) r c w k ∨ Del M (fun x => x ∈ L) r w k)
    (seen : M.past w a = true) :
    (L.foldl (step M) St.init).cell r c ≠ some a ∧
      (L.foldl (step M) St.init).lost r c a = none := by
  have result := run_isSpec valid causal
  rcases later with setter | deleted
  · exact cell_after_read_wins valid result value setter seen
  · exact (delete_after_read_wins result value deleted seen).2

end

open CovenStorage.Clocks in
/-- D2's finite device ids turn the storage package's timestamp order into
the merge model's natural-number order. -/
theorem packed_stamp_before (a b : Timestamp) (deviceBound : a.device < 2 ^ 64)
    (before : Before a b) :
    a.tick.val * 2 ^ 64 + a.device < b.tick.val * 2 ^ 64 + b.device := by
  rcases before with earlier | ⟨same, device⟩ <;> omega

open CovenStorage.Clocks in
/-- The actual storage stamping theorem supplies `Valid.past_ts` under D2's
timestamp encoding, including a wall clock behind every read timestamp. -/
theorem stamp_supplies_past_ts (wall : Int) (device : Nat) (latest result : Timestamp)
    (seen : List Timestamp) (upper : ∀ s ∈ seen, s.tick.val ≤ latest.tick.val)
    (deviceBounds : ∀ s ∈ seen, s.device < 2 ^ 64)
    (stamped : stamp wall device latest = .ok result) :
    ∀ s ∈ seen, s.tick.val * 2 ^ 64 + s.device < result.tick.val * 2 ^ 64 + result.device := by
  intro s hs
  exact packed_stamp_before s result (deviceBounds s hs)
    (stamp_after_every_read wall device latest result seen upper stamped s hs)

end CovenMerge
