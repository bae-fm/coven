import CovenStorelog.Invariants
import CovenStorelog.ReplayMachine

namespace CovenStorelog.ReplayMachine

theorem scan_progress (auth : Nat → Bool) (unchanged : σ → Nat → Bool) (realize : σ → Nat → Option σ)
    (conflict prefer : Nat → Nat → Bool) (entries old : List Nat)
    (todo : List Nat) (r : Result σ)
    (ht : ∀ w ∈ todo, w ∈ entries)
    (hd : ∀ w ∈ old, w ∈ r.dropped)
    (hk : ∀ w ∈ r.kept, w ∈ entries ∧ w ∉ old)
    {drops : List Nat} (h : scan auth unchanged realize conflict prefer todo r = .restart drops) :
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
          · cases he : realize r.state w with
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
                    · have hn : r.kept.filter (conflict w) ≠ [] := by
                        simpa using hn
                      obtain ⟨x, hx⟩ := List.exists_mem_of_ne_nil _ hn
                      have hkeep := (List.mem_filter.mp hx).1
                      exact ⟨x, (hk x hkeep).1, (hk x hkeep).2,
                        List.mem_append_left _ hx⟩

def settleN (initial : σ) (auth : Nat → Bool) (unchanged : σ → Nat → Bool)
    (realize : σ → Nat → Option σ) (conflict prefer : Nat → Nat → Bool)
    (entries : List Nat) (fuel : Nat) (drops : List Nat) : Option (Result σ) :=
  match fuel with
  | 0 => none
  | fuel + 1 =>
      match scan auth unchanged realize conflict prefer entries ⟨initial, [], drops⟩ with
      | .complete r => some r
      | .restart more => settleN initial auth unchanged realize conflict prefer entries fuel more

theorem settleN_total (initial : σ) (auth : Nat → Bool) (unchanged : σ → Nat → Bool) (entries : List Nat)
    (fuel : Nat) (drops : List Nat) (hbound : remaining entries drops < fuel)
    (conflict prefer : Nat → Nat → Bool) (realize : σ → Nat → Option σ) :
    ∃ r, settleN initial auth unchanged realize conflict prefer entries fuel drops = some r := by
  induction fuel generalizing drops with
  | zero => omega
  | succ fuel ih =>
      cases hs : scan auth unchanged realize conflict prefer entries ⟨initial, [], drops⟩ with
      | complete r => exact ⟨r, by simp [settleN, hs]⟩
      | restart new =>
          have hp := scan_progress auth unchanged realize conflict prefer entries drops entries ⟨initial, [], drops⟩
            (fun _ h => h) (fun _ h => h) (by simp) hs
          have decrease := restart_decreases hp
          obtain ⟨r, hr⟩ := ih new (by omega)
          exact ⟨r, by simp [settleN, hs, hr]⟩

/-- The restart bound is proved sufficient; failure cannot become a state. -/
def settle (initial : σ) (auth : Nat → Bool) (unchanged : σ → Nat → Bool)
    (realize : σ → Nat → Option σ) (conflict prefer : Nat → Nat → Bool)
    (entries : List Nat) : Result σ :=
  (settleN initial auth unchanged realize conflict prefer entries (entries.length + 1) []).get (by
    obtain ⟨r, hr⟩ := settleN_total initial auth unchanged entries (entries.length + 1) []
      (by simp [remaining]) conflict prefer realize
    simp [hr])

theorem settle_eq_some (initial : σ) (auth : Nat → Bool) (unchanged : σ → Nat → Bool)
    (realize : σ → Nat → Option σ) (conflict prefer : Nat → Nat → Bool) (entries : List Nat) :
    settleN initial auth unchanged realize conflict prefer entries (entries.length + 1) [] =
      some (settle initial auth unchanged realize conflict prefer entries) :=
  (Option.some_get _).symm

theorem scan_state (auth : Nat → Bool) (unchanged : σ → Nat → Bool) (P : σ → Prop)
    {conflict prefer : Nat → Nat → Bool}
    {realize : σ → Nat → Option σ}
    (hp : ∀ s w t, P s → realize s w = some t → P t)
    {todo : List Nat} {r out : Result σ} (hr : P r.state)
    (h : scan auth unchanged realize conflict prefer todo r = .complete out) : P out.state := by
  induction todo generalizing r with
  | nil => cases h; exact hr
  | cons w ws ih =>
      simp only [scan] at h
      split at h
      · exact ih hr h
      · split at h
        · exact ih (r := { r with dropped := w :: r.dropped }) hr h
        · split at h
          · exact ih (r := { r with kept := w :: r.kept }) hr h
          · cases he : realize r.state w with
            | none => simp only [he] at h; exact ih (r := { r with dropped := w :: r.dropped }) hr h
            | some next =>
                simp only [he] at h
                split at h
                · exact ih (r := { r with dropped := w :: r.dropped }) hr h
                · split at h
                  · exact ih (hp r.state w next hr he) h
                  · cases h

theorem settleN_state (initial : σ) (auth : Nat → Bool) (unchanged : σ → Nat → Bool)
    (realize : σ → Nat → Option σ) (conflict prefer : Nat → Nat → Bool)
    (P : σ → Prop) (hi : P initial)
    (hp : ∀ s w t, P s → realize s w = some t → P t) (entries : List Nat)
    {fuel : Nat} {drops : List Nat} {out : Result σ}
    (h : settleN initial auth unchanged realize conflict prefer entries fuel drops = some out) :
    P out.state := by
  induction fuel generalizing drops with
  | zero => cases h
  | succ fuel ih =>
      cases he : scan auth unchanged realize conflict prefer entries ⟨initial, [], drops⟩ with
      | complete r =>
          simp only [settleN, he, Option.some.injEq] at h
          subst out
          exact scan_state auth unchanged P hp hi he
      | restart more =>
          simp only [settleN, he] at h
          exact ih h

theorem settle_state (initial : σ) (auth : Nat → Bool) (unchanged : σ → Nat → Bool)
    (realize : σ → Nat → Option σ) (conflict prefer : Nat → Nat → Bool)
    (P : σ → Prop) (hi : P initial)
    (hp : ∀ s w t, P s → realize s w = some t → P t) (entries : List Nat) :
    P (settle initial auth unchanged realize conflict prefer entries).state :=
  settleN_state initial auth unchanged realize conflict prefer P hi hp entries
    (settle_eq_some initial auth unchanged realize conflict prefer entries)

theorem scan_kept (auth : Nat → Bool) (unchanged : σ → Nat → Bool) (entries : List Nat)
    {todo : List Nat} {r out : Result σ}
    (ht : ∀ w ∈ todo, w ∈ entries)
    (hk : ∀ w ∈ r.kept, w ∈ entries ∧ auth w = true)
    {conflict prefer : Nat → Nat → Bool}
    {realize : σ → Nat → Option σ}
    (h : scan auth unchanged realize conflict prefer todo r = .complete out) :
    ∀ w ∈ out.kept, w ∈ entries ∧ auth w = true := by
  induction todo generalizing r with
  | nil => cases h; exact hk
  | cons w ws ih =>
      have hws : ∀ x ∈ ws, x ∈ entries := fun x hx => ht x (List.mem_cons_of_mem w hx)
      simp only [scan] at h
      split at h
      · exact ih hws hk h
      · split at h
        · exact ih (r := { r with dropped := w :: r.dropped }) hws hk h
        · rename_i ha
          have haw : auth w = true := by simpa using ha
          have hk' : ∀ x ∈ w :: r.kept, x ∈ entries ∧ auth x = true := by
            intro x hx
            rcases List.mem_cons.mp hx with hx | hx
            · subst x; exact ⟨ht w List.mem_cons_self, haw⟩
            · exact hk x hx
          split at h
          · exact ih hws hk' h
          · cases he : realize r.state w with
            | none => simp only [he] at h; exact ih (r := { r with dropped := w :: r.dropped }) hws hk h
            | some next =>
                simp only [he] at h
                split at h
                · exact ih (r := { r with dropped := w :: r.dropped }) hws hk h
                · split at h
                  · exact ih hws hk' h
                  · cases h

theorem settleN_kept (initial : σ) (auth : Nat → Bool) (unchanged : σ → Nat → Bool)
    (realize : σ → Nat → Option σ) (conflict prefer : Nat → Nat → Bool) (entries : List Nat)
    {fuel : Nat} {drops : List Nat} {out : Result σ}
    (h : settleN initial auth unchanged realize conflict prefer entries fuel drops = some out) :
    ∀ w ∈ out.kept, w ∈ entries ∧ auth w = true := by
  induction fuel generalizing drops with
  | zero => cases h
  | succ fuel ih =>
      cases he : scan auth unchanged realize conflict prefer entries ⟨initial, [], drops⟩ with
      | complete r =>
          simp only [settleN, he, Option.some.injEq] at h
          subst out
          exact scan_kept auth unchanged entries (fun _ h => h) (by simp) he
      | restart more =>
          simp only [settleN, he] at h
          exact ih h

theorem settle_kept (initial : σ) (auth : Nat → Bool) (unchanged : σ → Nat → Bool)
    (realize : σ → Nat → Option σ) (conflict prefer : Nat → Nat → Bool) (entries : List Nat) :
    ∀ w ∈ (settle initial auth unchanged realize conflict prefer entries).kept,
      w ∈ entries ∧ auth w = true :=
  settleN_kept initial auth unchanged realize conflict prefer entries
    (settle_eq_some initial auth unchanged realize conflict prefer entries)

/-- The historical equation remains available to proofs that unfold it.
The generalized machine has exactly the same outcomes at that boundary. -/
def legacyPass : CovenStorelog.Pass → Pass CovenStorelog.State
  | .complete r => .complete ⟨r.state, r.kept, r.dropped⟩
  | .restart drops => .restart drops

theorem scan_matches_legacy (M : Log) (views : Nat → CovenStorelog.State)
    (conflict prefer : Nat → Nat → Bool) (realize : CovenStorelog.State → Nat → Entry → Option CovenStorelog.State)
    (todo : List Nat) (r : CovenStorelog.Result) :
    legacyPass (CovenStorelog.scan M views todo r conflict prefer realize) =
      scan (fun w => authorized (views w) (M w)) (fun s w => alreadyInPlace s (M w))
        (fun s w => realize s w (M w)) conflict prefer todo ⟨r.state, r.kept, r.dropped⟩ := by
  induction todo generalizing r with
  | nil => rfl
  | cons w ws ih =>
      simp only [CovenStorelog.scan, scan]
      split
      · exact ih r
      · split
        · exact ih _
        · split
          · exact ih _
          · cases realize r.state w (M w) with
            | none => exact ih _
            | some next =>
                simp only
                split
                · exact ih _
                · split
                  · exact ih _
                  · rfl

end CovenStorelog.ReplayMachine
