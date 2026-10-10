import CovenStorelog.Converge
import CovenStorelog.Ownership

namespace CovenStorelog

theorem checkedEffect_sound {s t : State} {w : Nat} {e : Entry}
    {remove : Nat → Circle → Nat → Option Circle}
    (h : checkedEffect s w e remove = some t) : effect s w e remove = some t ∧ safe t = true := by
  unfold checkedEffect at h
  cases he : effect s w e remove with
  | none => simp [he] at h
  | some next =>
      simp only [he, bind, Option.bind] at h
      split at h
      · cases h; exact ⟨rfl, by assumption⟩
      · cases h

/-- A state property preserved by effects is preserved by a complete pass.
Already-in-place entries keep the same state. A restart publishes no state. -/
theorem scan_state (M : Log) (views : Nat → State) (P : State → Prop)
    {conflict prefer : Nat → Nat → Bool}
    {realize : State → Nat → Entry → Option State}
    (hp : ∀ s w t, P s → realize s w (M w) = some t → P t)
    {todo : List Nat} {r out : Result} (hr : P r.state)
    (h : scan M views todo r conflict prefer realize = .complete out) : P out.state := by
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
          · cases he : realize r.state w (M w) with
            | none => simp only [he] at h; exact ih (r := { r with dropped := w :: r.dropped }) hr h
            | some next =>
                simp only [he] at h
                split at h
                · exact ih (r := { r with dropped := w :: r.dropped }) hr h
                · split at h
                  · exact ih (hp r.state w next hr he) h
                  · cases h

theorem settleN_state (M : Log) (views : Nat → State) (entries : List Nat)
    (P : State → Prop) (hi : P State.empty)
    {conflict prefer : Nat → Nat → Bool}
    {realize : State → Nat → Entry → Option State}
    (hp : ∀ s w t, P s → realize s w (M w) = some t → P t)
    {fuel : Nat} {drops : List Nat} {out : Result}
    (h : settleN M views entries fuel drops conflict prefer realize = some out) : P out.state := by
  induction fuel generalizing drops with
  | zero => cases h
  | succ fuel ih =>
      simp only [settleN] at h
      cases he : scan M views entries ⟨State.empty, [], drops⟩ conflict prefer realize with
      | complete r =>
          simp only [he, Option.some.injEq] at h
          subst out
          exact scan_state M views P hp hi he
      | restart next =>
          simp only [he] at h
          exact ih h

theorem resolve_safe (M : Log) (n : Nat) (S : EntrySet) :
    safe (resolve M n S).state = true := by
  apply settleN_state M (authorViews M n) ((List.range n).filter S)
    (fun s => safe s = true) (by decide) _ (settle_eq_some M _ _)
  intro s w t _ h
  exact (checkedEffect_sound h).2

theorem resolve_references (M : Log) (n : Nat) (S : EntrySet) :
    References (resolve M n S).state := by
  apply settleN_state M (authorViews M n) ((List.range n).filter S)
    References references_empty _ (settle_eq_some M _ _)
  intro s w t hs h
  exact effect_references hs (checkedEffect_sound h).1

/-- Every kept identity came from the received set and passed authority,
including identities whose requested changes were already in place. -/
theorem scan_kept (M : Log) (views : Nat → State) (entries : List Nat)
    {todo : List Nat} {r out : Result}
    (ht : ∀ w ∈ todo, w ∈ entries)
    (hk : ∀ w ∈ r.kept, w ∈ entries ∧ authorized (views w) (M w) = true)
    {conflict prefer : Nat → Nat → Bool}
    {realize : State → Nat → Entry → Option State}
    (h : scan M views todo r conflict prefer realize = .complete out) :
    ∀ w ∈ out.kept, w ∈ entries ∧ authorized (views w) (M w) = true := by
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
          have haw : authorized (views w) (M w) = true := by simpa using ha
          have hk' : ∀ x ∈ w :: r.kept, x ∈ entries ∧ authorized (views x) (M x) = true := by
            intro x hx
            rcases List.mem_cons.mp hx with hx | hx
            · subst x; exact ⟨ht w List.mem_cons_self, haw⟩
            · exact hk x hx
          split at h
          · exact ih hws hk' h
          · cases he : realize r.state w (M w) with
            | none => simp only [he] at h; exact ih (r := { r with dropped := w :: r.dropped }) hws hk h
            | some next =>
                simp only [he] at h
                split at h
                · exact ih (r := { r with dropped := w :: r.dropped }) hws hk h
                · split at h
                  · exact ih hws hk' h
                  · cases h

theorem settleN_kept (M : Log) (views : Nat → State) (entries : List Nat)
    {fuel : Nat} {drops : List Nat} {out : Result}
    {conflict prefer : Nat → Nat → Bool}
    {realize : State → Nat → Entry → Option State}
    (h : settleN M views entries fuel drops conflict prefer realize = some out) :
    ∀ w ∈ out.kept, w ∈ entries ∧ authorized (views w) (M w) = true := by
  induction fuel generalizing drops with
  | zero => cases h
  | succ fuel ih =>
      simp only [settleN] at h
      cases he : scan M views entries ⟨State.empty, [], drops⟩ conflict prefer realize with
      | complete r =>
          simp only [he, Option.some.injEq] at h
          subst out
          exact scan_kept M views entries (fun _ h => h) (by simp) he
      | restart next => simp only [he] at h; exact ih h

theorem kept_received (M : Log) (n : Nat) (S : EntrySet) {w : Nat}
    (hw : w ∈ (resolve M n S).kept) : w < n ∧ S w = true := by
  have h := (settleN_kept M _ _ (settle_eq_some M (authorViews M n) _) w hw).1
  simpa using h

theorem authority_uses_author_view (M : Log) (n : Nat) (S : EntrySet) {w : Nat}
    (hw : w ∈ (resolve M n S).kept) : authorized (authorView M w) (M w) = true := by
  have h := (settleN_kept M _ _ (settle_eq_some M (authorViews M n) _) w hw).2
  rwa [authorViews_at M (kept_received M n S hw).1] at h

theorem devices_have_members (M : Log) (n : Nat) (S : EntrySet) {d m : Nat}
    (h : (d, m) ∈ (resolve M n S).state.devices) :
    member (resolve M n S).state m = true := (resolve_references M n S).devices d m h

theorem circles_have_members (M : Log) (n : Nat) (S : EntrySet) {c : Nat} {circle : Circle}
    (h : (c, circle) ∈ (resolve M n S).state.circles) {m : Nat} (hm : m ∈ circle.members) :
    member (resolve M n S).state m = true := (resolve_references M n S).circles c circle h m hm

theorem circles_nonempty (M : Log) (n : Nat) (S : EntrySet) {c : Nat} {circle : Circle}
    (h : (c, circle) ∈ (resolve M n S).state.circles) : circle.members ≠ [] :=
  (resolve_references M n S).nonempty c circle h

theorem device_add_authority (M : Log) (n : Nat) (S : EntrySet) {w m d : Nat}
    (hw : w ∈ (resolve M n S).kept) (he : (M w).action = .addDevice m d) :
    member (authorView M w) (M w).author = true ∧ (M w).author = m := by
  simpa [authorized, he] using authority_uses_author_view M n S hw

theorem device_removal_authority (M : Log) (n : Nat) (S : EntrySet) {w m d : Nat}
    (hw : w ∈ (resolve M n S).kept) (he : (M w).action = .removeDevice m d) :
    lookup (authorView M w).devices d = some m ∧
      member (authorView M w) (M w).author = true ∧
      ((M w).author = m ∨ admin (authorView M w) (M w).author = true) := by
  simpa [authorized, he, and_assoc] using authority_uses_author_view M n S hw

theorem circle_delete_authority (M : Log) (n : Nat) (S : EntrySet) {w c : Nat}
    (hw : w ∈ (resolve M n S).kept) (he : (M w).action = .deleteCircle c) :
    inCircle (authorView M w) c (M w).author = true := by
  simpa [authorized, he] using authority_uses_author_view M n S hw

theorem circle_raise_authority (M : Log) (n : Nat) (S : EntrySet) {w c v snap : Nat}
    (hw : w ∈ (resolve M n S).kept)
    (he : (M w).action = .raiseSchema v ⟨.circle c, snap⟩) :
    inCircle (authorView M w) c (M w).author = true := by
  simpa [authorized, he] using authority_uses_author_view M n S hw

end CovenStorelog
