import CovenSequence.CircleLifecycle

/-!
# A reload past a device's own removal (§14.1, §16.5)

A device offline until retention deleted the slot that removes it, or removes
its member, misses that slot: it reloads from the floor instead (§6.1). The
floor's snapshots cover a slot past the removal, so their rows hold what others
wrote after it, and for a member's removal the snapshots no longer hold the
circle the member alone was in. A device that read the removal's slot stopped
there and kept its copy, as its state was before that slot with the removal
applied. A reload stops at the same slot.

The reload reads history's states from the floor's store history and the kept
slots, in slot order, to its target. At the first slot whose state stops the
device, it stops, and installs what the slot path installs there from the
states just before and after that slot. Otherwise it installs history's state
at its target (`reload`).

The general part holds for any stop and anything a device installs at it
(`kept`): the reload installs what the slot path holds at every target
(`reload_eq_slotPath`). A device's own removal is the stop where it installs
history's state at the removal (`kept a b = b`). Its member's removal is the
stop where it also keeps every circle as it was before the slot
(`CircleLifecycle.keep`): a device of a removed member that reloads holds
exactly what `CircleLifecycle.ownStateAt` holds (`own_reload`), so the reload
past its member's removal keeps every circle as history held it just before
the removal and installs nothing of history after it
(`reload_keeps_circles_at_removal`).

The model is the store's state, which every device computes alike from
history. Rows are not in it: a reload that stops at the removal installs none
of the snapshots' rows, and the device keeps the rows of its copy (§14.1).
-/
namespace CovenSequence.RemovedReload

section Stops

variable {σ : Type} (h : Nat → σ) (stops : σ → Bool) (kept : σ → σ → σ)

/-- What a device holds after slots 1 to `k` on the slot path, from history's
states `h`: history's state until the first slot whose state stops it, then
what it installs at that slot from the states before and after it, and nothing
more after. -/
def slotPath : Nat → σ
  | 0 => h 0
  | k + 1 =>
    if stops (h k) then slotPath k
    else if stops (h (k + 1)) then kept (h k) (h (k + 1))
    else h (k + 1)

/-- The slot just before the first slot through `k` whose state stops the
device, read in slot order from slot 1. -/
def beforeStop : Nat → Option Nat
  | 0 => none
  | k + 1 =>
    match beforeStop k with
    | some r => some r
    | none => if stops (h (k + 1)) then some k else none

/-- What a reload to `target` installs: history's state at `target`, unless
history stops the device at slot `r + 1` by then. The reload then stops there
and installs what the slot path installs at that slot. -/
def reload (target : Nat) : σ :=
  match beforeStop h stops target with
  | some r => kept (h r) (h (r + 1))
  | none => h target

variable {h stops kept}

theorem beforeStop_none {k : Nat} (none : beforeStop h stops k = none) :
    ∀ j, 1 ≤ j → j ≤ k → stops (h j) = false := by
  induction k with
  | zero => intro j one le; omega
  | succ k ih =>
    simp only [beforeStop] at none
    split at none
    · cases none
    · rename_i prior
      split at none
      · cases none
      · rename_i last
        intro j one le
        by_cases at_end : j = k + 1
        · subst j; simpa using last
        · exact ih prior j one (by omega)

theorem beforeStop_some {k r : Nat} (found : beforeStop h stops k = some r) :
    r + 1 ≤ k ∧ stops (h (r + 1)) = true := by
  induction k with
  | zero => cases found
  | succ k ih =>
    simp only [beforeStop] at found
    split at found
    · rename_i prior
      cases found
      have := ih prior
      exact ⟨by omega, this.2⟩
    · split at found
      · rename_i last
        cases found
        exact ⟨Nat.le_refl _, last⟩
      · cases found

/-- **A reload installs what the slot path holds.** When the device is not
stopped at the start and a stop lasts in history, a reload to any target
installs the state a device that applied every slot to it holds. -/
theorem reload_eq_slotPath (start : stops (h 0) = false)
    (lasts : ∀ k, stops (h k) = true → stops (h (k + 1)) = true) :
    ∀ target, reload h stops kept target = slotPath h stops kept target
  | 0 => by simp [reload, beforeStop, slotPath]
  | k + 1 => by
    have prior := reload_eq_slotPath start lasts k
    cases found : beforeStop h stops k with
    | some r =>
      obtain ⟨le, stopped⟩ := beforeStop_some found
      have still : ∀ j, r + 1 ≤ j → stops (h j) = true := by
        intro j le
        induction j with
        | zero => omega
        | succ j ih =>
          by_cases same : r + 1 = j + 1
          · rw [← same]; exact stopped
          · exact lasts j (ih (by omega))
      have here : stops (h k) = true := still k le
      have next : beforeStop h stops (k + 1) = some r := by simp [beforeStop, found]
      simp only [reload, next, slotPath, here, ite_true]
      simp only [reload, found] at prior
      exact prior
    | none =>
      have clear := beforeStop_none found
      have here : stops (h k) = false := by
        cases k with
        | zero => exact start
        | succ k => exact clear (k + 1) (by omega) (Nat.le_refl _)
      by_cases last : stops (h (k + 1)) = true
      · have next : beforeStop h stops (k + 1) = some k := by simp [beforeStop, found, last]
        simp [reload, next, slotPath, here, last]
      · have next : beforeStop h stops (k + 1) = none := by simp [beforeStop, found, last]
        simp [reload, next, slotPath, here, last]

/-- A device on the slot path is stopped wherever history stops it, when what
it installs at a stop stops it too. -/
theorem slotPath_stops (stays : ∀ a b, stops b = true → stops (kept a b) = true) :
    ∀ k, stops (h k) = true → stops (slotPath h stops kept k) = true
  | 0, stopped => stopped
  | k + 1, stopped => by
    simp only [slotPath]
    by_cases before : stops (h k) = true
    · simp only [before, ite_true]; exact slotPath_stops stays k before
    · simp only [before, stopped, ite_true, Bool.false_eq_true, ite_false]
      exact stays _ _ stopped

end Stops

/-! ## A removed member's device -/

open CircleLifecycle

variable {M C : Type} [DecidableEq M] [DecidableEq C]

/-- Whether history's state `s` has removed member `own`, which stops every
device of `own` for good (§14.1). -/
def removedIn (own : M) (s : State M C) : Bool := decide (s.standing own = .removed)

/-- **Only the store's removal of a member removes it**: a slot that leaves
`own` removed, after a state where `own` wasn't, is the removal of `own`,
who was active before it. -/
theorem becomes_removed (s : State M C) (k : Nat) (e : Option (Entry M C)) (own : M)
    (before : s.standing own ≠ .removed) (after : (step s k e).standing own = .removed) :
    e = some (.removeMember own) ∧ s.standing own = .active := by
  cases e with
  | none => exact absurd after before
  | some e =>
    simp only [step] at after
    cases h : effect s k e with
    | refused => rw [h] at after; exact absurd after before
    | unchanged => rw [h] at after; exact absurd after before
    | applied next =>
      rw [h] at after
      simp only [Outcome.after] at after
      cases e with
      | circle a change =>
        rw [changeEffect_standing s k a change next h] at after
        exact absurd after before
      | removeMember m =>
        simp only [effect, removalEffect] at h
        split at h
        · cases h
        · cases h
        · rename_i active
          cases h
          by_cases same : own = m
          · subst m; exact ⟨rfl, active⟩
          · simp [same] at after; exact absurd after before
      | admit a m c n | recover a m c n =>
        simp only [effect, admissionEffect] at h
        repeat' split at h
        all_goals cases h
        all_goals
          by_cases same : own = m
          · subst m; simp at after
          · simp [same] at after; exact absurd after before

/-- A device of `own` holds what the slot path from history's states holds,
with `keep` as what it installs at its member's removal. -/
theorem ownStateAt_slotPath (own : M) (init : State M C) (seq : Nat → Option (Entry M C)) :
    ∀ k, ownStateAt own init seq k = slotPath (stateAt init seq) (removedIn own) keep k
  | 0 => rfl
  | k + 1 => by
    have prior := ownStateAt_slotPath own init seq k
    simp only [ownStateAt, slotPath]
    by_cases gone : removedIn own (stateAt init seq k) = true
    · have stopped : (ownStateAt own init seq k).standing own = .removed := by
        rw [prior]
        have := slotPath_stops (h := stateAt init seq) (stops := removedIn own) (kept := keep)
          (fun _ _ stopped => stopped) k gone
        simpa [removedIn] using this
      simp only [gone, ite_true]
      rw [← prior]
      cases seq (k + 1) with
      | none => rfl
      | some e => simp [ownStep, stopped]
    · have here : (stateAt init seq k).standing own ≠ .removed := by simpa [removedIn] using gone
      have agrees := own_agrees_until_removal own init seq k here
      simp only [gone, Bool.false_eq_true, ite_false]
      rw [agrees]
      by_cases next : removedIn own (stateAt init seq (k + 1)) = true
      · have removed : (stateAt init seq (k + 1)).standing own = .removed := by
          simpa [removedIn] using next
        simp only [stateAt] at removed
        obtain ⟨entry, active⟩ := becomes_removed _ _ _ own here removed
        simp only [next, ite_true]
        show _ = keep _ (step (stateAt init seq k) (k + 1) (seq (k + 1)))
        rw [entry]
        simp [ownStep, ownEffect, step, effect, removalEffect, active, Outcome.after]
      · simp only [next, Bool.false_eq_true, ite_false]
        have stays : (stateAt init seq (k + 1)).standing own ≠ .removed := by
          simpa [removedIn] using next
        have := own_agrees_until_removal own init seq (k + 1) stays
        simp only [ownStateAt] at this
        rw [agrees] at this
        exact this

/-- **A removed member's device that reloads holds what reading every slot
leaves it** (§14.1, §16.5). A device of `own`, an id not removed at the
start, that reloads to any target from history's states installs what
`ownStateAt` holds there. -/
theorem own_reload (own : M) (init : State M C) (seq : Nat → Option (Entry M C))
    (start : init.standing own ≠ .removed) (target : Nat) :
    reload (stateAt init seq) (removedIn own) keep target = ownStateAt own init seq target := by
  rw [ownStateAt_slotPath]
  apply reload_eq_slotPath
  · exact decide_eq_false start
  · intro k gone
    have removed : (stateAt init seq k).standing own = .removed := by simpa [removedIn] using gone
    exact decide_eq_true (step_keeps_removed _ (k + 1) (seq (k + 1)) own removed)

/-- **A reload past a member's removal stops at it** (§14.1, §15.8). Slot
`r + 1` removes `own`, an active member before it. A device of `own` that
reloads to any target from that slot on installs `own` removed and every
circle as history held it just before the removal: the same members, `own`
among them, and the same deletions. It installs none of history's state after
the removal. -/
theorem reload_keeps_circles_at_removal (own : M) (init : State M C)
    (seq : Nat → Option (Entry M C)) (r : Nat) (start : init.standing own ≠ .removed)
    (removal : seq (r + 1) = some (.removeMember own))
    (active : (stateAt init seq r).standing own = .active) (target : Nat) (past : r + 1 ≤ target) :
    let installed := reload (stateAt init seq) (removedIn own) keep target
    installed = reload (stateAt init seq) (removedIn own) keep (r + 1) ∧
      installed.standing own = .removed ∧
      installed.circles = (stateAt init seq r).circles ∧
      installed.deleted = (stateAt init seq r).deleted := by
  intro installed
  have at_target := removed_member_keeps_circles own init seq r removal active target past
  have at_removal := removed_member_keeps_circles own init seq r removal active (r + 1)
    (Nat.le_refl _)
  have frozen := own_stopped_stays own init seq (r + 1) at_removal.1 target past
  simp only [installed, own_reload own init seq start]
  exact ⟨frozen, at_target⟩

end CovenSequence.RemovedReload
