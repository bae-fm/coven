import CovenStorelogData.ReplayEffects

/-! §7.1, §9, §14.4: a readable circle comes from a kept creation in
the recorded past. Permanent time rejection applies in that past too. -/
namespace CovenStorelogData.CircleCreation
open CovenStorelog

private theorem effect_source {s t : State} {e : Entry} {w c : Nat} {circle : Circle}
    {remove : Nat → Circle → Nat → Option Circle}
    (effected : effect s w e remove = some t) (present : (c, circle) ∈ t.circles) :
    (∃ old, (c, old) ∈ s.circles) ∨ ∃ name, e.action = .makeCircle c name := by
  cases action : e.action with
  | create access =>
    simp only [effect, action] at effected
    split at effected
    · cases effected
    · cases effected; simp [State.empty] at present
  | addMember m role access =>
    simp only [effect, action] at effected
    split at effected
    · cases effected; exact Or.inl ⟨circle, present⟩
    · cases effected
  | setAccess m access =>
    simp only [effect, action] at effected
    split at effected
    · cases effected; exact Or.inl ⟨circle, present⟩
    · cases effected
  | changeRole m role =>
    simp only [effect, action] at effected
    split at effected
    · cases effected; exact Or.inl ⟨circle, present⟩
    · cases effected
  | addDevice m =>
    simp only [effect, action] at effected
    split at effected
    · cases effected; exact Or.inl ⟨circle, present⟩
    · cases effected
  | removeDevice m d =>
    simp only [effect, action] at effected
    split at effected
    · cases effected; exact Or.inl ⟨circle, present⟩
    · cases effected
  | raiseSchema v snapshot =>
    simp only [effect, action] at effected
    split at effected
    · cases effected; exact Or.inl ⟨circle, present⟩
    · cases effected
  | reset snapshot =>
    simp only [effect, action] at effected
    split at effected
    · cases effected; exact Or.inl ⟨circle, present⟩
    · cases effected
  | removeMember m keys =>
    simp only [effect, action] at effected
    split at effected
    · cases effected
      obtain ⟨⟨d, old⟩, mem, mapped⟩ := List.mem_filterMap.mp present
      obtain ⟨next, _, eq⟩ := Option.map_eq_some_iff.mp mapped
      cases eq
      exact Or.inl ⟨old, mem⟩
    · cases effected
  | makeCircle d name =>
    simp only [effect, action] at effected
    split at effected
    · cases effected
      rcases List.mem_cons.mp present with eq | mem
      · cases eq; exact Or.inr ⟨name, rfl⟩
      · exact Or.inl ⟨circle, (List.mem_filter.mp mem).1⟩
    · cases effected
  | deleteCircle d =>
    simp only [effect, action] at effected
    split at effected
    · cases effected; exact Or.inl ⟨circle, (List.mem_filter.mp present).1⟩
    · cases effected
  | renameCircle d name =>
    simp only [effect, action] at effected
    cases found : lookup s.circles d with
    | none => simp [found] at effected
    | some old =>
      simp only [found, bind, Option.bind, Option.some.injEq] at effected
      subst t
      rcases List.mem_cons.mp present with eq | mem
      · cases eq; exact Or.inl ⟨old, lookup_some_mem found⟩
      · exact Or.inl ⟨circle, (List.mem_filter.mp mem).1⟩
  | addToCircle d m =>
    simp only [effect, action] at effected
    cases found : lookup s.circles d with
    | none => simp [found] at effected
    | some old =>
      simp only [found, bind, Option.bind] at effected
      split at effected
      · cases effected
        rcases List.mem_cons.mp present with eq | mem
        · cases eq; exact Or.inl ⟨old, lookup_some_mem found⟩
        · exact Or.inl ⟨circle, (List.mem_filter.mp mem).1⟩
      · cases effected
  | removeFromCircle d m =>
    simp only [effect, action] at effected
    cases found : lookup s.circles d with
    | none => simp [found] at effected
    | some old =>
      simp only [found, bind, Option.bind] at effected
      split at effected
      · cases removed : remove d old m with
        | none =>
          simp only [removed, Option.some.injEq] at effected
          subst t
          exact Or.inl ⟨circle, (List.mem_filter.mp present).1⟩
        | some next =>
          simp only [removed, Option.some.injEq] at effected
          subst t
          rcases List.mem_cons.mp present with eq | mem
          · cases eq; exact Or.inl ⟨old, lookup_some_mem found⟩
          · exact Or.inl ⟨circle, (List.mem_filter.mp mem).1⟩
      · cases effected
  | rotateKey audience key =>
    simp only [effect, action, Option.some.injEq] at effected
    subst t
    exact Or.inl ⟨circle, present⟩

private theorem applied_source (M : Log) (views : Nat → State)
    {es : List Nat} {s t : State} {c : Nat} {circle : Circle}
    (applied : ReplayEffects.applyKept (ReplayPolicy.realize views) M es s = some t)
    (present : (c, circle) ∈ t.circles) :
    (∃ old, (c, old) ∈ s.circles) ∨
      ∃ e ∈ es, ∃ name, (M e).action = .makeCircle c name := by
  induction es generalizing s with
  | nil => cases applied; exact Or.inl ⟨circle, present⟩
  | cons e es ih =>
    simp only [ReplayEffects.applyKept] at applied
    cases step : ReplayEffects.applyEntry (ReplayPolicy.realize views) M s e with
    | none => simp [step] at applied
    | some next =>
      simp only [step, Option.bind] at applied
      rcases ih applied with ⟨old, mem⟩ | ⟨a, mem, name, action⟩
      · unfold ReplayEffects.applyEntry at step
        split at step
        · cases step; exact Or.inl ⟨old, mem⟩
        · have source := effect_source (checkedEffect_sound step).1 mem
          rcases source with prior | ⟨name, action⟩
          · exact Or.inl prior
          · exact Or.inr ⟨e, List.mem_cons_self, name, action⟩
      · exact Or.inr ⟨a, List.mem_cons_of_mem e mem, name, action⟩

/-- This follows the actual kept effects, including the finishing removal of
empty circles; circle existence is not an assumption about the kept set. -/
theorem readable_has_kept_creation (H : CurrentReplay.History) (W n : Nat)
    (S : EntrySet) (c m : Nat)
    (readable : inCircle (CurrentReplay.resolve H W n S).state c m = true) :
    ∃ e ∈ (CurrentReplay.resolve H W n S).kept,
      ∃ name, (H.membership.log e).action = .makeCircle c name := by
  have realized := ReplayEffects.entry_fate H W n S
  obtain ⟨raw, applied, finished⟩ := Option.map_eq_some_iff.mp realized
  cases found : lookup (CurrentReplay.resolve H W n S).state.circles c with
  | none => simp [inCircle, found] at readable
  | some circle =>
    have mem := lookup_some_mem found
    rw [← finished] at mem
    have rawMem := (List.mem_filter.mp mem).1
    rcases applied_source H.membership.log (CurrentReplay.authorViews H W n n)
        applied rawMem with absent | ⟨e, mem, name, action⟩
    · simp [State.empty] at absent
    · exact ⟨e, by simpa using (List.mem_filter.mp mem).2, name, action⟩

/-- Rule 1 rejects an entry in every received or recorded-past replay. -/
theorem kept_not_too_late (H : CurrentReplay.History) (W n : Nat)
    (S : EntrySet) (e : Nat) (kept : e ∈ (CurrentReplay.resolve H W n S).kept) :
    Finality.tooLate H.membership W n (fun _ => true) e = false := by
  have h := (settleN_kept H.membership.log _ _
    (settle_eq_some H.membership.log _ _ (ReplayPolicy.conflict H.membership.log _)
      (ReplayPolicy.prefer H.membership.log _) (ReplayPolicy.realize _)) e kept).1
  change e ∈ (List.range n).filter (ReplayPolicy.admitted H.membership W n S) at h
  have live := (List.mem_filter.mp h).2
  simp only [ReplayPolicy.admitted, Bool.and_eq_true, Bool.not_eq_true'] at live
  exact live.2

end CovenStorelogData.CircleCreation
