import CovenStorelog.Resolve

namespace CovenStorelog

/-- Replaying a set reads historical views only for entries in that set. -/
theorem scan_views_congr (M : Log) (entries : List Nat) (a b : Nat → State)
    (hv : ∀ w ∈ entries, a w = b w) {todo : List Nat} (r : Result)
    (ht : ∀ w ∈ todo, w ∈ entries) (hk : ∀ w ∈ r.kept, w ∈ entries) :
    scan M a todo r = scan M b todo r := by
  induction todo generalizing r with
  | nil => rfl
  | cons w ws ih =>
      have hw := ht w List.mem_cons_self
      have hws : ∀ x ∈ ws, x ∈ entries := fun x hx => ht x (List.mem_cons_of_mem w hx)
      have hk' : ∀ x ∈ w :: r.kept, x ∈ entries := by
        intro x hx
        rcases List.mem_cons.mp hx with hx | hx
        · subst x; exact hw
        · exact hk x hx
      have hf : r.kept.filter (pairConflict M a w) = r.kept.filter (pairConflict M b w) := by
        apply List.filter_congr
        intro x hx
        simp only [pairConflict, hv w hw, hv x (hk x hx)]
      simp only [scan, hv w hw, hf]
      split
      · exact ih r hws hk
      · split
        · exact ih { r with dropped := w :: r.dropped } hws hk
        · split
          · exact ih { r with kept := w :: r.kept } hws hk'
          · cases he : checkedEffect r.state w (M w) with
            | none =>
                simp only
                exact ih { r with dropped := w :: r.dropped } hws hk
            | some next =>
                simp only
                split
                · exact ih { r with dropped := w :: r.dropped } hws hk
                · split
                  · exact ih { r with state := next, kept := w :: r.kept } hws hk'
                  · rfl

theorem settleN_views_congr (M : Log) (entries : List Nat) (a b : Nat → State)
    (hv : ∀ w ∈ entries, a w = b w) (fuel : Nat) (drops : List Nat) :
    settleN M a entries fuel drops = settleN M b entries fuel drops := by
  induction fuel generalizing drops with
  | zero => rfl
  | succ fuel ih =>
      have he := scan_views_congr M entries a b hv ⟨State.empty, [], drops⟩
        (fun _ h => h) (by simp)
      simp only [settleN, he]
      split
      · rfl
      · exact ih _

theorem settle_views_congr (M : Log) (entries : List Nat) (a b : Nat → State)
    (hv : ∀ w ∈ entries, a w = b w) : settle M a entries = settle M b entries := by
  have h := settleN_views_congr M entries a b hv (entries.length + 1) []
  rw [settle_eq_some, settle_eq_some] at h
  exact Option.some.inj h

/-- The table implementation is the recursive definition: replay precisely
the received timestamps using each entry's own had-read replay as its view. -/
theorem resolve_eq (M : Log) (n : Nat) (S : EntrySet) :
    resolve M n S = settle M (authorView M) ((List.range n).filter S) := by
  apply settle_views_congr
  intro w hw
  exact authorViews_at M (List.mem_range.mp (List.mem_filter.mp hw).1)

theorem range_filter_bound (S : EntrySet) {n m : Nat} (hnm : n ≤ m)
    (hs : ∀ w, S w = true → w < n) : (List.range m).filter S = (List.range n).filter S := by
  obtain ⟨k, rfl⟩ := Nat.le.dest hnm
  induction k with
  | zero => rfl
  | succ k ih =>
      have hf : S (n + k) = false := by
        cases he : S (n + k)
        · rfl
        · have := hs (n + k) he; omega
      change (List.range ((n + k) + 1)).filter S = _
      rw [List.range_succ, List.filter_append]
      simp [hf, ih]

/-- Different finite bounds containing the same received set give the same
state and reports; the bound cannot hide a dependency on receipt order. -/
theorem resolve_bound_independent (M : Log) (S : EntrySet) (n m : Nat)
    (hn : ∀ w, S w = true → w < n) (hm : ∀ w, S w = true → w < m) :
    resolve M n S = resolve M m S := by
  rw [resolve_eq, resolve_eq]
  rcases Nat.le_total n m with h | h
  · rw [range_filter_bound S h hn]
  · rw [range_filter_bound S h hm]

end CovenStorelog
