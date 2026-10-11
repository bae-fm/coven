import CovenStorelogData.ReplayEffects
import CovenStorelogData.KeyPhases

namespace CovenStorelogData.Revocation
open CovenStorelog ReplayEffects

/-- Phase two preserves both observations when it adds a published rotation.
This is receipt extension, not a second notion of successful catch-up. -/
theorem rotation_retains (H : CurrentReplay.History) (W n who device : Nat)
    (before after : KeyPhases.CaughtUp) (e : Nat) (copies entry : Except Pending.Reason Unit)
    (rotated : KeyPhases.rotate H W n who device before e copies entry = .ok after) :
    (∀ a ∈ before.received, a ∈ after.received) ∧
    (∀ copy ∈ before.observation.copies, copy ∈ after.observation.copies) := by
  unfold KeyPhases.rotate at rotated
  cases action : (H.log e).action <;> simp only [action] at rotated
  all_goals try contradiction
  all_goals
    simp only [Except.pure, Except.bind, bind, pure] at rotated
    split at rotated <;> try contradiction
    split at rotated <;> try contradiction
    cases copies <;> cases entry <;> simp_all
    subst after
    constructor
    · intro value present; exact List.mem_append_left _ present
    · intro key recipient present; exact List.mem_append_left _ present

theorem member_absent (s : CovenStorelog.State) (m : Nat) (absent : member s m = false) (a : Audience) :
    m ∉ audienceMembers s a := by
  cases a with
  | store =>
      have lookupNone : lookup s.members m = none := by simpa [member] using absent
      suffices ∀ xs : List (Nat × Role), lookup xs m = none → m ∉ xs.map Prod.fst from
        this s.members lookupNone
      intro xs
      induction xs with
      | nil => simp
      | cons pair rest ih =>
          rcases pair with ⟨key, role⟩
          by_cases eq : key = m <;> simp_all [lookup, Ne.symm]
  | circle c =>
      cases circle : lookup s.circles c <;> simp [audienceMembers, circle, absent]

/-- The only actions that can restore an absent membership are creation and
addition. A role change requires the membership to exist already. -/
theorem effect_preserves_absence (s t : CovenStorelog.State) (e : Entry) (w m : Nat)
    (remove : Nat → Circle → Nat → Option Circle)
    (absent : member s m = false)
    (noCreate : e.action.isCreation = false)
    (noAdd : ∀ role access, e.action ≠ .addMember m role access)
    (applied : effect s w e remove = some t) : member t m = false := by
  cases action : e.action <;> simp only [CovenStorelog.effect, action] at applied
  case create => simp [action, Action.isCreation] at noCreate
  case addMember who role access =>
    have ne : who ≠ m := by intro eq; subst who; exact noAdd role access action
    split at applied
    · cases applied; simpa [member, lookup_put, ne] using absent
    · contradiction
  case changeRole who role =>
    by_cases eq : who = m
    · subst who; simp [absent] at applied
    · split at applied
      · cases applied; simpa [member, lookup_put, eq] using absent
      · contradiction
  case removeMember who keys =>
    split at applied
    · cases applied
      by_cases eq : m = who
      · subst who; simp [member, lookup_erase_same]
      · simpa [member, lookup_erase_other _ _ _ eq] using absent
    · contradiction
  all_goals
    repeat first | exact absent | contradiction | simp only [Option.bind] at applied |
      dsimp at applied | split at applied | cases applied

theorem applied_preserves_absence (M : Log) (views : Nat → CovenStorelog.State)
    (s t : CovenStorelog.State) (e m : Nat) (absent : member s m = false)
    (noCreate : (M e).action.isCreation = false)
    (noAdd : ∀ role access, (M e).action ≠ .addMember m role access)
    (applied : applyEntry (ReplayPolicy.realize views) M s e = some t) : member t m = false := by
  unfold applyEntry at applied
  split at applied
  · cases applied; exact absent
  · obtain ⟨next, effect, checked⟩ := Option.bind_eq_some_iff.mp applied
    split at checked
    · cases checked
      exact effect_preserves_absence s t (M e) e m _ absent noCreate noAdd effect
    · contradiction

theorem applied_removal (M : Log) (views : Nat → CovenStorelog.State) (s t : CovenStorelog.State) (e m : Nat)
    (removal : (M e).action = .removeMember m [])
    (applied : applyEntry (ReplayPolicy.realize views) M s e = some t) : member t m = false := by
  unfold applyEntry at applied
  split at applied
  · rename_i already
    cases applied
    simpa [alreadyInPlace, removal] using already
  · obtain ⟨next, effect, checked⟩ := Option.bind_eq_some_iff.mp applied
    split at checked
    · cases checked
      simp only [CovenStorelog.effect, removal] at effect
      split at effect
      · cases effect; simp [member, lookup_erase_same]
      · contradiction
    · contradiction

theorem suffix_preserves_absence (M : Log) (views : Nat → CovenStorelog.State) (es : List Nat)
    (s t : CovenStorelog.State) (m : Nat) (absent : member s m = false)
    (noCreate : ∀ e ∈ es, (M e).action.isCreation = false)
    (noAdd : ∀ e ∈ es, ∀ role access, (M e).action ≠ .addMember m role access)
    (applied : applyKept (ReplayPolicy.realize views) M es s = some t) : member t m = false := by
  induction es generalizing s with
  | nil => cases applied; exact absent
  | cons e es ih =>
      obtain ⟨next, head, tail⟩ := Option.bind_eq_some_iff.mp applied
      exact ih next (applied_preserves_absence M views s next e m absent
        (noCreate e List.mem_cons_self) (noAdd e List.mem_cons_self) head)
        (fun a ha => noCreate a (List.mem_cons_of_mem _ ha))
        (fun a ha => noAdd a (List.mem_cons_of_mem _ ha)) tail

theorem replay_removal (M : Log) (views : Nat → CovenStorelog.State) (es : List Nat)
    (s t : CovenStorelog.State) (r m : Nat) (ordered : es.Pairwise (· < ·)) (kept : r ∈ es)
    (removal : (M r).action = .removeMember m [])
    (noCreate : ∀ e ∈ es, r < e → (M e).action.isCreation = false)
    (noAdd : ∀ e ∈ es, r < e → ∀ role access, (M e).action ≠ .addMember m role access)
    (applied : applyKept (ReplayPolicy.realize views) M es s = some t) : member t m = false := by
  induction es generalizing s with
  | nil => cases kept
  | cons e es ih =>
      obtain ⟨next, head, tail⟩ := Option.bind_eq_some_iff.mp applied
      obtain ⟨ahead, sorted⟩ := List.pairwise_cons.mp ordered
      rcases List.mem_cons.mp kept with eq | member
      · subst e
        exact suffix_preserves_absence M views es next t m
          (applied_removal M views s next r m removal head)
          (fun a ha => noCreate a (List.mem_cons_of_mem _ ha) (ahead a ha))
          (fun a ha => noAdd a (List.mem_cons_of_mem _ ha) (ahead a ha)) tail
      · exact ih next sorted member
          (fun a ha => noCreate a (List.mem_cons_of_mem _ ha))
          (fun a ha => noAdd a (List.mem_cons_of_mem _ ha)) tail

/-- A kept removal excludes its member from every audience unless a later
kept addition restores that membership. Finality alone does not prohibit a
new addition, so the no-return condition names that action explicitly. -/
theorem kept_removal_excludes (H : CurrentReplay.History) (W n r m : Nat) (S : EntrySet)
    (causal : CovenStorelog.Valid H.membership.log n)
    (removal : (H.log r).action = .removeMember m)
    (kept : r ∈ (CurrentReplay.resolve H W n S).kept)
    (noReturn : ∀ e, e < n → r < e → e ∈ (CurrentReplay.resolve H W n S).kept →
      ∀ role access, (H.log e).action ≠ .addMember m role access)
    (audience : Audience) : m ∉ audienceMembers (CurrentReplay.resolve H W n S).state audience := by
  have fate := ReplayEffects.entry_fate H W n S
  dsimp only at fate
  obtain ⟨state, applied, finished⟩ := Option.map_eq_some_iff.mp fate
  have present : r ∈ ((List.range n).filter (ReplayPolicy.admitted H.membership W n S none)).filter
      (fun e => e ∈ (CurrentReplay.resolve H W n S).kept) := by
    have accounting := (settleN_kept H.membership.log _ _
      (settle_eq_some H.membership.log _ _ (ReplayPolicy.conflict H.membership.log _)
        (ReplayPolicy.prefer H.membership.log _) (ReplayPolicy.realize _)) r kept).1
    exact List.mem_filter.mpr ⟨accounting, by simpa using kept⟩
  have absent := replay_removal H.membership.log (CurrentReplay.authorViews H W n n) _
    State.empty state r m ((List.pairwise_lt_range (n := n)).filter _ |>.filter _) present
    (by simp [CurrentReplay.History.membership, CurrentReplay.membershipLog,
      CurrentReplay.Entry.membership, CurrentReplay.Action.membership, removal])
    (by
      intro e member later
      have bound := List.mem_range.mp (List.mem_filter.mp (List.mem_filter.mp member).1).1
      cases creation : (H.membership.log e).action.isCreation
      · rfl
      · have root := causal.only_root e bound creation; omega)
    (by
      intro e member later role access same
      have h := List.mem_filter.mp member
      have bound := List.mem_range.mp (List.mem_filter.mp h.1).1
      have denied := noReturn e bound later (of_decide_eq_true h.2) role access
      cases action : (H.log e).action <;>
        simp [CurrentReplay.History.membership, CurrentReplay.membershipLog,
          CurrentReplay.Entry.membership, CurrentReplay.Action.membership, action] at same
      obtain ⟨rfl, rfl, rfl⟩ := same
      exact denied action) applied
  apply member_absent _ m ?_ audience
  rw [← finished]
  exact absent

end CovenStorelogData.Revocation
