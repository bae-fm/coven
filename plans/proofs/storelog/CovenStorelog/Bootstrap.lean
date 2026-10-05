import CovenStorelog.Invariants

namespace CovenStorelog

theorem effect_created {s t : State} {w : Nat} {e : Entry}
    (hs : s.created = true) (h : effect s w e = some t) : t.created = true := by
  cases ha : e.action <;> simp only [effect, ha, bind, Option.bind] at h
  all_goals repeat' first | split at h | cases h
  all_goals simp_all
  rcases h with ⟨_, h⟩
  split at h <;> cases h <;> rfl

def RootPass : Pass → Prop
  | .complete r => r.state.created = true
  | .restart drops => 0 ∉ drops

/-- Later entries cannot reject creation: every such entry has read it. -/
theorem scan_root_suffix (M : Log) (views : Nat → State) {todo : List Nat} {r : Result}
    (ht : ∀ w ∈ todo, w ≠ 0 ∧ pairConflict M views w 0 = false)
    (hc : r.state.created = true) (hd : 0 ∉ r.dropped) :
    RootPass (scan M views todo r) := by
  induction todo generalizing r with
  | nil => exact hc
  | cons w ws ih =>
      have hw := ht w List.mem_cons_self
      have hws : ∀ x ∈ ws, x ≠ 0 ∧ pairConflict M views x 0 = false :=
        fun x hx => ht x (List.mem_cons_of_mem w hx)
      have hd' : 0 ∉ w :: r.dropped := by simpa [Ne.symm hw.1] using hd
      simp only [scan]
      split
      · exact ih hws hc hd
      · split
        · exact ih (r := { r with dropped := w :: r.dropped }) hws hc hd'
        · split
          · exact ih (r := { r with kept := w :: r.kept }) hws hc hd
          · cases he : checkedEffect r.state w (M w) with
            | none =>
                simp only
                exact ih (r := { r with dropped := w :: r.dropped }) hws hc hd'
            | some next =>
                simp only
                split
                · exact ih (r := { r with dropped := w :: r.dropped }) hws hc hd'
                · split
                  · exact ih (r := { r with state := next, kept := w :: r.kept })
                      hws (effect_created hc (checkedEffect_sound he).1) hd
                  · change 0 ∉ r.kept.filter (pairConflict M views w) ++ r.dropped
                    simpa [hw.2] using hd

theorem scan_root (M : Log) (views : Nat → State) (todo drops : List Nat)
    (hroot : (M 0).action = .create) (hv : views 0 = State.empty)
    (ht : ∀ w ∈ todo, w ≠ 0 ∧ pairConflict M views w 0 = false) (hd : 0 ∉ drops) :
    RootPass (scan M views (0 :: todo) ⟨State.empty, [], drops⟩) := by
  have hs := scan_root_suffix M views (r := ⟨{ State.empty with
    created := true, members := [((M 0).author, .admin)],
    devices := [((M 0).device, (M 0).author)] }, [0], drops⟩) ht rfl hd
  simpa [scan, hd, hv, hroot, authorized, alreadyInPlace, checkedEffect, effect,
    State.empty, safe, hasAdmin, admin, lookup] using hs

theorem settleN_created (M : Log) (views : Nat → State) (todo : List Nat)
    (hroot : (M 0).action = .create) (hv : views 0 = State.empty)
    (ht : ∀ w ∈ todo, w ≠ 0 ∧ pairConflict M views w 0 = false)
    {fuel : Nat} {drops : List Nat} {out : Result} (hd : 0 ∉ drops)
    (h : settleN M views (0 :: todo) fuel drops = some out) : out.state.created = true := by
  induction fuel generalizing drops with
  | zero => cases h
  | succ fuel ih =>
      have hg := scan_root M views todo drops hroot hv ht hd
      cases he : scan M views (0 :: todo) ⟨State.empty, [], drops⟩ with
      | complete r =>
          simp only [settleN, he, Option.some.injEq] at h
          subst out
          simpa [he, RootPass] using hg
      | restart next =>
          simp only [settleN, he] at h
          exact ih (by simpa [he, RootPass] using hg) h

/-- Creation is present in every nonempty, closed received history. It
cannot be defeated by a concurrent entry, since all entries have read it. -/
theorem resolve_created (M : Log) (n : Nat) (S : EntrySet) (hv : Valid M n)
    (hn : 0 < n) (hr : S 0 = true) : (resolve M n S).state.created = true := by
  cases n with
  | zero => omega
  | succ n =>
      have he : (List.range (n + 1)).filter S =
          0 :: ((List.range n).map Nat.succ).filter S := by
        simp [List.range_succ_eq_map, hr]
      have hview : authorViews M (n + 1) 0 = State.empty := by
        rw [authorViews_at M (by omega)]
        rfl
      have htodo : ∀ w ∈ ((List.range n).map Nat.succ).filter S,
          w ≠ 0 ∧ pairConflict M (authorViews M (n + 1)) w 0 = false := by
        intro w hw
        obtain ⟨a, ha, rfl⟩ := List.mem_map.mp (List.mem_filter.mp hw).1
        have hp := hv.read_root (a + 1) (by omega) (by have := List.mem_range.mp ha; omega)
        exact ⟨by omega, by simp [pairConflict, concurrent, hp]⟩
      have hs := settle_eq_some M (authorViews M (n + 1)) ((List.range (n + 1)).filter S)
      rw [he] at hs
      unfold resolve materialize
      rw [he]
      exact settleN_created M _ _ hv.root hview htodo (drops := []) (by simp) hs

theorem admin_invariant (M : Log) (n : Nat) (S : EntrySet) (hv : Valid M n)
    (hn : 0 < n) (hr : S 0 = true) : ∃ m, admin (resolve M n S).state m = true := by
  have hc := resolve_created M n S hv hn hr
  have hs := resolve_safe M n S
  simp only [safe, hc, Bool.not_true, Bool.false_or] at hs
  obtain ⟨p, _, hp⟩ := List.any_eq_true.mp hs
  exact ⟨p.1, hp⟩

/-- Any nonempty closed set contains creation; the admin theorem therefore
requires no supplied assertion that the output is a created store. -/
theorem closed_has_admin (M : Log) (n : Nat) (S : EntrySet) (hv : Valid M n)
    (hc : Closed M S) (hne : ∃ w, w < n ∧ S w = true) :
    ∃ m, admin (resolve M n S).state m = true := by
  obtain ⟨w, hw, hs⟩ := hne
  apply admin_invariant M n S hv (by omega)
  by_cases hz : w = 0
  · simpa [hz] using hs
  · exact hc w hs 0 (hv.read_root w (by omega) hw)

end CovenStorelog
