import CovenStorelog.Conflicts

namespace CovenStorelog

structure Result where
  state : State
  kept : List Nat
  dropped : List Nat
  deriving DecidableEq, Repr

inductive Pass where
  | complete (result : Result)
  | restart (dropped : List Nat)
  deriving DecidableEq, Repr

/-- Each pass follows timestamp order. Authority is first, including when
an effect is already present. A winning candidate drops all its opponents. -/
def scan (M : Log) (views : Nat → State) : List Nat → Result → Pass
  | [], r => .complete r
  | w :: ws, r =>
      if w ∈ r.dropped then scan M views ws r else
      if !authorized (views w) (M w) then
        scan M views ws { r with dropped := w :: r.dropped }
      else if alreadyInPlace r.state (M w) then
        scan M views ws { r with kept := w :: r.kept }
      else match checkedEffect r.state w (M w) with
        | none => scan M views ws { r with dropped := w :: r.dropped }
        | some next =>
            let opponents := r.kept.filter (pairConflict M views w)
            if !opponents.all (before M w) then
              scan M views ws { r with dropped := w :: r.dropped }
            else if opponents.isEmpty then
              scan M views ws { r with state := next, kept := w :: r.kept }
            else .restart (opponents ++ r.dropped)

/-- Bounded replay. The termination proof establishes that the chosen bound
cannot be exhausted; no unfinished replay is returned as a state. -/
def settleN (M : Log) (views : Nat → State) (entries : List Nat) :
    Nat → List Nat → Option Result
  | 0, _ => none
  | fuel + 1, dropped =>
      match scan M views entries ⟨State.empty, [], dropped⟩ with
      | .complete r => some r
      | .restart drops => settleN M views entries fuel drops

def Progress (entries old new : List Nat) : Prop :=
  (∀ w ∈ old, w ∈ new) ∧ ∃ w ∈ entries, w ∉ old ∧ w ∈ new

/-- A restart includes every old drop and at least one previously live entry.
The witness is an applied opponent, not an unreceived or already lost entry. -/
theorem scan_progress (M : Log) (views : Nat → State) (entries old : List Nat)
    (todo : List Nat) (r : Result)
    (ht : ∀ w ∈ todo, w ∈ entries)
    (hd : ∀ w ∈ old, w ∈ r.dropped)
    (hk : ∀ w ∈ r.kept, w ∈ entries ∧ w ∉ old)
    {drops : List Nat} (h : scan M views todo r = .restart drops) :
    Progress entries old drops := by
  induction todo generalizing r with
  | nil => cases h
  | cons w ws ih =>
      have hws : ∀ x ∈ ws, x ∈ entries := fun x hx => ht x (List.mem_cons_of_mem w hx)
      have hd' : ∀ x ∈ old, x ∈ w :: r.dropped :=
        fun x hx => List.mem_cons_of_mem w (hd x hx)
      simp only [scan] at h
      split at h
      · exact ih r hws hd hk h
      · rename_i hw
        have hwold : w ∉ old := fun hh => hw (hd w hh)
        have hk' : ∀ x ∈ w :: r.kept, x ∈ entries ∧ x ∉ old := by
          intro x hx
          rcases List.mem_cons.mp hx with hx | hx
          · subst x
            exact ⟨ht w List.mem_cons_self, hwold⟩
          · exact hk x hx
        split at h
        · exact ih { r with dropped := w :: r.dropped } hws hd' hk h
        · split at h
          · exact ih { r with kept := w :: r.kept } hws hd hk' h
          · cases he : checkedEffect r.state w (M w) with
            | none =>
                simp only [he] at h
                exact ih { r with dropped := w :: r.dropped } hws hd' hk h
            | some next =>
                simp only [he] at h
                split at h
                · exact ih { r with dropped := w :: r.dropped } hws hd' hk h
                · split at h
                  · exact ih { r with state := next, kept := w :: r.kept } hws hd hk' h
                  · rename_i hn
                    cases h
                    constructor
                    · exact fun x hx => List.mem_append_right _ (hd x hx)
                    · have hn : r.kept.filter (pairConflict M views w) ≠ [] := by
                        simpa using hn
                      obtain ⟨x, hx⟩ := List.exists_mem_of_ne_nil _ hn
                      have hkeep := (List.mem_filter.mp hx).1
                      exact ⟨x, (hk x hkeep).1, (hk x hkeep).2,
                        List.mem_append_left _ hx⟩

/-- The counting argument also works if a list representation repeats an id. -/
theorem countP_lt_of {α : Type} {l : List α} {p q : α → Bool}
    (hpq : ∀ y, p y = true → q y = true)
    {x : α} (hx : x ∈ l) (hq : q x = true) (hp : p x = false) :
    l.countP p < l.countP q := by
  have le : ∀ (xs : List α), xs.countP p ≤ xs.countP q := by
    intro xs
    induction xs with
    | nil => simp
    | cons y ys ih =>
        simp only [List.countP_cons]
        cases h1 : p y <;> cases h2 : q y <;> simp
        · omega
        · omega
        · exact absurd (hpq y h1) (by simp [h2])
        · omega
  induction l with
  | nil => simp at hx
  | cons y ys ih =>
      simp only [List.countP_cons]
      rcases List.mem_cons.mp hx with rfl | hx
      · rw [hp, hq]
        have := le ys
        simp
        omega
      · have := ih hx
        cases h1 : p y <;> cases h2 : q y <;> simp
        · omega
        · omega
        · exact absurd (hpq y h1) (by simp [h2])
        · omega

def remaining (entries dropped : List Nat) : Nat := entries.countP (fun w => w ∉ dropped)

theorem restart_decreases {entries old new : List Nat} (h : Progress entries old new) :
    remaining entries new < remaining entries old := by
  obtain ⟨hsub, x, hx, ho, hn⟩ := h
  apply countP_lt_of (x := x) ?_ hx (by simpa using ho) (by simpa using hn)
  intro y hy
  simp only [decide_eq_true_eq] at hy ⊢
  exact fun hh => hy (hsub y hh)

/-- Every replay finishes after at most one more pass than there are live
entries. Each restart strictly decreases their number. -/
theorem settleN_total (M : Log) (views : Nat → State) (entries : List Nat)
    (fuel : Nat) (drops : List Nat) (hbound : remaining entries drops < fuel) :
    ∃ r, settleN M views entries fuel drops = some r := by
  induction fuel generalizing drops with
  | zero => omega
  | succ fuel ih =>
      cases hs : scan M views entries ⟨State.empty, [], drops⟩ with
      | complete r => exact ⟨r, by simp [settleN, hs]⟩
      | restart new =>
          have hp := scan_progress M views entries drops entries ⟨State.empty, [], drops⟩
            (fun _ h => h) (fun _ h => h) (by simp) hs
          have decrease := restart_decreases hp
          obtain ⟨r, hr⟩ := ih new (by omega)
          exact ⟨r, by simp [settleN, hs, hr]⟩

end CovenStorelog
