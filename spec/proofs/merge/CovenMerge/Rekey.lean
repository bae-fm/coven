import CovenMerge.Causality

/-!
# A write that takes a fresh device id keeps its place in every order (§7.2, §19.3)

A copy that finds another copy using its device id `O` takes a fresh id `F`
and re-keys its queued writes: each keeps its time and counter and carries `F`
as its device. `rekey_keeps_order` says when that changes no comparison with
any other stamp, and `same_order_same_result` says that a merged state depends
on timestamps only through their order, so every cell and every lost value
stays.
-/
namespace CovenMerge

open CovenStorage.Clocks

/-- The stamp a re-keyed write carries: the same time and counter, device `F`. -/
def rekeyed (q : Timestamp) (F : Nat) : Timestamp := { q with device := F }

/-- **Re-keying keeps every comparison with another stamp.** Let `q` carry
device `O` and be re-keyed to `F`. Another write's stamp `u` compares with `q`
as it did, whichever way, when `F` is not `u`'s device and, where `u` ties `q`
on time and counter, `F` lies on the same side of `u`'s device as `O`. -/
theorem rekey_keeps_order (q u : Timestamp) (O F : Nat) (hq : q.device = O) (other : u ≠ q)
    (fresh : F ≠ u.device)
    (side : u.tick = q.tick → (u.device < O ↔ u.device < F)) :
    (Before u q ↔ Before u (rekeyed q F)) ∧ (Before q u ↔ Before (rekeyed q F) u) := by
  have notO : u.tick = q.tick → u.device ≠ O := fun ht hd => other (by
    obtain ⟨ut, ud⟩ := u
    obtain ⟨qt, qd⟩ := q
    simp only at ht hd hq
    subst ht; subst hd; subst hq; rfl)
  unfold Before rekeyed
  simp only
  by_cases ht : u.tick.val = q.tick.val
  · have hte : u.tick = q.tick := Fin.ext ht
    have s := side hte
    have n := notO hte
    constructor
    · rw [← hq] at s
      constructor <;> intro h <;> omega
    · constructor <;> intro h <;> omega
  · constructor <;> constructor <;> intro h <;> omega

/-- **Two re-keyed writes compare as before.** Both carried `O`; both carry `F`. -/
theorem rekey_pair (q q' : Timestamp) (O F : Nat) (hq : q.device = O) (hq' : q'.device = O) :
    Before q q' ↔ Before (rekeyed q F) (rekeyed q' F) := by
  unfold Before rekeyed
  simp only
  constructor <;> intro h <;> omega

/-! ## A merged state depends on timestamps only through their order -/

section Order
variable {W Row Col : Type}

/-- `M'` has `M`'s reads and changes and orders its timestamps as `M` does. -/
structure SameOrder (M M' : Writes W Row Col) : Prop where
  lt : ∀ a b, M'.ts a < M'.ts b ↔ M.ts a < M.ts b
  past : M'.past = M.past
  chg : M'.chg = M.chg

variable {M M' : Writes W Row Col}

theorem SameOrder.le (h : SameOrder M M') (a b : W) : M'.ts a ≤ M'.ts b ↔ M.ts a ≤ M.ts b := by
  have := h.lt b a
  omega

theorem SameOrder.replacer (h : SameOrder M M') (S : W → Prop) (r : Row) (c : Col) (a : W)
    (k : Nat) (y : W) : Replacer M' S r c a k y ↔ Replacer M S r c a k y := by
  simp only [Replacer, Setter, Del, h.chg, h.lt]

theorem SameOrder.minGen (h : SameOrder M M') (S : W → Prop) (r : Row) (n : Nat) (x : W) :
    MinGen M' S r n x ↔ MinGen M S r n x := by
  simp only [MinGen, GenChange, h.chg, h.le]

theorem SameOrder.maxSetter (h : SameOrder M M') (S : W → Prop) (r : Row) (c : Col) (k : Nat)
    (x : W) : MaxSetter M' S r c k x ↔ MaxSetter M S r c k x := by
  simp only [MaxSetter, Setter, h.chg, h.le]

theorem SameOrder.minDel (h : SameOrder M M') (S : W → Prop) (r : Row) (k : Nat) (x : W) :
    MinDel M' S r k x ↔ MinDel M S r k x := by
  simp only [MinDel, Del, h.chg, h.le]

theorem SameOrder.lost (h : SameOrder M M') (S : W → Prop) (r : Row) (c : Col) (a : W) (k : Nat)
    (x : W) : Lost M' S r c a k x ↔ Lost M S r c a k x := by
  have canon : Canon M' S r c k x ↔ Canon M S r c k x := by
    simp only [Canon, h.minDel, h.maxSetter, Del, h.chg]
  simp only [Lost, Setter, h.past, h.replacer, canon, h.chg]

/-- **A state is the result for a set of writes under either of two timestamp
assignments that order the writes alike.** So re-stamping writes in a way that
keeps every comparison (`rekey_keeps_order`, `rekey_pair`) leaves every row's
generation, every cell's winner and every lost value as it was. -/
theorem same_order_same_result (h : SameOrder M M') {S : W → Prop} {st : St W Row Col} :
    IsSpec M' S st ↔ IsSpec M S st := by
  have gen : ∀ r G, IsGen M' S r G ↔ IsGen M S r G := fun r G => by
    simp only [IsGen, GenChange, h.chg]
  have setter : ∀ r c x k, Setter M' S r c x k ↔ Setter M S r c x k := fun r c x k => by
    simp only [Setter, h.chg]
  have genChange : ∀ r x n, GenChange M' S r x n ↔ GenChange M S r x n := fun r x n => by
    simp only [GenChange, h.chg]
  constructor
  · intro s
    exact
      { gen := fun r => (gen r _).1 (s.gen r)
        gw_some := fun r n x => (s.gw_some r n x).trans (h.minGen S r n x)
        gw_none := fun r n hn x hx => s.gw_none r n hn x ((genChange r x n).2 hx)
        cell_some := fun r c x => (s.cell_some r c x).trans (h.maxSetter S r c _ x)
        cell_none := fun r c hn x hx => s.cell_none r c hn x ((setter r c x _).2 hx)
        lost := fun r c a k x => (s.lost r c a k x).trans (h.lost S r c a k x) }
  · intro s
    exact
      { gen := fun r => (gen r _).2 (s.gen r)
        gw_some := fun r n x => (s.gw_some r n x).trans (h.minGen S r n x).symm
        gw_none := fun r n hn x hx => s.gw_none r n hn x ((genChange r x n).1 hx)
        cell_some := fun r c x => (s.cell_some r c x).trans (h.maxSetter S r c _ x).symm
        cell_none := fun r c hn x hx => s.cell_none r c hn x ((setter r c x _).1 hx)
        lost := fun r c a k x => (s.lost r c a k x).trans (h.lost S r c a k x).symm }

end Order

/-! ## A write that lands twice

A queued write the keeper also sent lands a second time, as another write: the
same row changes with the same time and counter, another device in its stamps.
The second landing did not read the first, so §8.2's rule records a lost value
-- the first landing's, equal to the value that replaced it. -/

/-- The first landing `false`, stamped 1, and its second landing `true`,
stamped 2, make the same insert; neither read the other. -/
def twiceLanded : Writes Bool Unit Unit where
  ts := fun w => if w then 2 else 1
  past := fun _ _ => false
  chg := fun _ _ => some ⟨.ins, 0, fun _ => true⟩

theorem twiceLanded_valid : Valid twiceLanded where
  ts_inj := by decide
  past_ts := by intro w a h; simp [twiceLanded] at h
  gen_seen := by intro w r ch h; simp [twiceLanded] at h; subst h; exact Or.inl rfl
  parity := by intro w r ch h; simp [twiceLanded] at h; subst h; simp

/-- **Counterexample: a write landing twice is not the same as landing once.**
One landing of the insert leaves no lost value. Both landings record the first
landing's value as lost, replaced by the second landing's identical value.
The merge rule of §8.2 as written does not make a second landing harmless; a
copy's carried write that the keeper also sent is dropped before it lands twice
only if the copy reads the keeper's slot before sending it. -/
theorem twice_landing_records_lost_value :
    Valid twiceLanded ∧
    Lost twiceLanded (fun _ => True) () () false 1 true ∧
    ¬ ∃ x, Lost twiceLanded (fun w => w = false) () () false 1 x := by
  have ins : ∀ y, twiceLanded.chg y () = some ⟨.ins, 0, fun _ => true⟩ := fun _ => rfl
  have setter : ∀ y, Setter twiceLanded (fun _ => True) () () y 1 := fun y =>
    ⟨trivial, _, ins y, by decide, rfl, rfl⟩
  refine ⟨twiceLanded_valid, ?_, ?_⟩
  · refine ⟨setter false, ⟨true, Or.inl ⟨setter true, by decide⟩⟩, ?_, ?_⟩
    · intro y hy; rfl
    · refine Or.inr ⟨?_, setter true, ?_⟩
      · rintro ⟨d, _, ch, hch, hk, _⟩
        rw [ins] at hch
        have := (Option.some.inj hch).symm
        subst this
        exact absurd hk (by decide)
      · intro y hy; cases y <;> decide
  · rintro ⟨x, _, ⟨y, hy⟩, _⟩
    rcases hy with ⟨⟨hy1, _⟩, hlt⟩ | ⟨_, ch, hch, hk, _⟩
    · subst hy1; exact absurd hlt (by decide)
    · rw [ins] at hch
      have := (Option.some.inj hch).symm
      subst this
      exact absurd hk (by decide)

end CovenMerge
