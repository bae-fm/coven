import CovenStorelog.Resolve

namespace CovenStorelog

/-- Every received identity is pending, kept, or dropped. A kept identity
is neither pending nor dropped. Drops may include entries still ahead in
timestamp order, because a restart begins with its previous drops. -/
structure Accounting (entries todo : List Nat) (r : Result) : Prop where
  covered : ∀ w, w ∈ entries ↔ w ∈ todo ∨ w ∈ r.kept ∨ w ∈ r.dropped
  separate : ∀ w ∈ r.kept, w ∉ todo ∧ w ∉ r.dropped
  unique : todo.Nodup

theorem Accounting.skip {entries todo : List Nat} {r : Result} {w : Nat}
    (h : Accounting entries (w :: todo) r) (hd : w ∈ r.dropped) :
    Accounting entries todo r := by
  constructor
  · intro x
    rw [h.covered x]
    simp only [List.mem_cons]
    constructor
    · rintro ((rfl | hx) | hx | hx)
      · exact Or.inr (Or.inr hd)
      · exact Or.inl hx
      · exact Or.inr (Or.inl hx)
      · exact Or.inr (Or.inr hx)
    · rintro (hx | hx | hx)
      · exact Or.inl (Or.inr hx)
      · exact Or.inr (Or.inl hx)
      · exact Or.inr (Or.inr hx)
  · intro x hx
    exact ⟨fun ht => (h.separate x hx).1 (List.mem_cons_of_mem w ht), (h.separate x hx).2⟩
  · exact (List.nodup_cons.mp h.unique).2

theorem Accounting.keep {entries todo : List Nat} {r : Result} {w : Nat}
    (h : Accounting entries (w :: todo) r) (hd : w ∉ r.dropped) (s : State) :
    Accounting entries todo ⟨s, w :: r.kept, r.dropped⟩ := by
  constructor
  · intro x
    rw [h.covered x]
    simp only [List.mem_cons]
    simp only [or_assoc, or_left_comm]
  · intro x hx
    rcases List.mem_cons.mp hx with rfl | hx
    · exact ⟨(List.nodup_cons.mp h.unique).1, hd⟩
    · exact ⟨fun ht => (h.separate x hx).1 (List.mem_cons_of_mem w ht), (h.separate x hx).2⟩
  · exact (List.nodup_cons.mp h.unique).2

theorem Accounting.drop {entries todo : List Nat} {r : Result} {w : Nat}
    (h : Accounting entries (w :: todo) r) :
    Accounting entries todo { r with dropped := w :: r.dropped } := by
  constructor
  · intro x
    rw [h.covered x]
    simp only [List.mem_cons]
    simp only [or_assoc, or_left_comm]
  · intro x hx
    have hs := h.separate x hx
    simp only [List.mem_cons, not_or] at hs ⊢
    exact ⟨hs.1.2, hs.1.1, hs.2⟩
  · exact (List.nodup_cons.mp h.unique).2

def Accounted (entries : List Nat) : Pass → Prop
  | .complete r => Accounting entries [] r
  | .restart drops => ∀ w ∈ drops, w ∈ entries

theorem scan_accounting (M : Log) (views : Nat → State) (entries : List Nat)
    {todo : List Nat} {r : Result} (h : Accounting entries todo r) :
    Accounted entries (scan M views todo r) := by
  induction todo generalizing r with
  | nil => exact h
  | cons w ws ih =>
      simp only [scan]
      split
      · exact ih (h.skip (by assumption))
      · rename_i hd
        split
        · exact ih h.drop
        · split
          · exact ih (h.keep hd r.state)
          · cases checkedEffect r.state w (M w) with
            | none => exact ih h.drop
            | some next =>
                simp only
                split
                · exact ih h.drop
                · split
                  · exact ih (h.keep hd next)
                  · intro x hx
                    rcases List.mem_append.mp hx with hx | hx
                    · exact (h.covered x).mpr (Or.inr (Or.inl (List.mem_filter.mp hx).1))
                    · exact (h.covered x).mpr (Or.inr (Or.inr hx))

theorem settleN_accounting (M : Log) (views : Nat → State) (entries : List Nat)
    (hu : entries.Nodup) {fuel : Nat} {drops : List Nat} {out : Result}
    (hd : ∀ w ∈ drops, w ∈ entries)
    (h : settleN M views entries fuel drops = some out) : Accounting entries [] out := by
  induction fuel generalizing drops with
  | zero => cases h
  | succ fuel ih =>
      have hi : Accounting entries entries ⟨State.empty, [], drops⟩ := by
        refine ⟨?_, by simp, hu⟩
        intro w
        simp only [List.not_mem_nil, false_or]
        exact ⟨Or.inl, fun hx => hx.elim id (hd w)⟩
      have hs := scan_accounting M views entries hi
      cases he : scan M views entries ⟨State.empty, [], drops⟩ with
      | complete r =>
          simp only [settleN, he, Option.some.injEq] at h
          subst out
          simpa [he, Accounted] using hs
      | restart next =>
          simp only [settleN, he] at h
          exact ih (by simpa [he, Accounted] using hs) h

/-- Every received entry has exactly one disposition. Restarts cannot lose
an identity or report an entry outside the received set. -/
theorem resolve_partition (M : Log) (n : Nat) (S : EntrySet) (w : Nat) :
    (w < n ∧ S w = true ↔ w ∈ (resolve M n S).kept ∨ w ∈ (resolve M n S).dropped) ∧
    (w ∈ (resolve M n S).kept → w ∉ (resolve M n S).dropped) := by
  have h := settleN_accounting M (authorViews M n) ((List.range n).filter S)
    ((List.nodup_range (n := n)).filter S) (by simp) (settle_eq_some M _ _)
  exact ⟨by simpa [resolve, materialize] using h.covered w, fun hw => (h.separate w hw).2⟩

theorem report_exactly_dropped (M : Log) (n : Nat) (S : EntrySet) (w : Nat) :
    w ∈ reports M (resolve M n S) (M w).author ↔
      w < n ∧ S w = true ∧ w ∉ (resolve M n S).kept := by
  rw [reported_to_author]
  have h := resolve_partition M n S w
  constructor
  · intro hd
    have hs := h.1.mpr (Or.inr hd)
    exact ⟨hs.1, hs.2, fun hk => h.2 hk hd⟩
  · rintro ⟨hw, hs, hk⟩
    exact (h.1.mp ⟨hw, hs⟩).resolve_left hk

end CovenStorelog
