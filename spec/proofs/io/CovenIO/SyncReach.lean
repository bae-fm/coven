import CovenIO.SyncState

/-!
# What reaches each sync state and wait (§19.1)

`SyncState` says what a state means once it shows. This says which facts reach
it, and that each state and each wait is reached.

`state_iff` gives the state from the facts: it shows exactly when a fact names
it and none names one above it, and `candidate_iff` says which facts can name
one. `synced_iff` unfolds that for synced, the only state every other fact
keeps away. `wait_facts` gives what each wait says of the facts.

`Reached.every_state` and `Reached.every_wait` run the model's events from a
synced device to each state and each wait, so none of them is dead.
-/
namespace CovenIO.SyncState

/-- **The facts that reach a state.** With a location recorded, a state shows
exactly when a fact names it and none names one above it. -/
theorem state_iff (f : Facts) (recorded : f.storage ≠ .none) (s : State) :
    state f = s ↔ s ∈ f.candidates ∧ ∀ c ∈ f.candidates, c.rank ≤ s.rank := by
  have ⟨mem, highest, only⟩ := one_state f recorded
  constructor
  · intro e; subst e; exact ⟨mem, highest⟩
  · intro h; exact (only s h.1 h.2).symm

/-- **Which fact names a state.** The loop, a download, an upload, the missing
credentials or a pending record. -/
theorem candidate_iff (f : Facts) (s : State) :
    s ∈ f.candidates ↔ f.loop = s ∨ (s = .syncing ∧ f.downloading = true) ∨
      (∃ u ∈ f.uploads, u.candidate = some s) ∨ (s = .signedOut ∧ f.storage = .signedOut) ∨
      ∃ r ∈ f.records, r.state = some s := by
  simp only [Facts.candidates, Facts.rest, Facts.files, Facts.others, List.mem_cons,
    List.mem_append, List.mem_filterMap]
  constructor
  · rintro (e | (down | up) | (signed | named))
    · exact .inl e.symm
    · split at down
      · rename_i d; simp at down; exact .inr (.inl ⟨down, d⟩)
      · simp at down
    · exact .inr (.inr (.inl up))
    · split at signed
      · rename_i h; simp at signed; exact .inr (.inr (.inr (.inl ⟨signed, h⟩)))
      · simp at signed
    · exact .inr (.inr (.inr (.inr named)))
  · rintro (e | ⟨e, d⟩ | up | ⟨e, h⟩ | named)
    · exact .inl e.symm
    · exact .inr (.inl (.inl (by simp [d, e])))
    · exact .inr (.inl (.inr up))
    · exact .inr (.inr (.inl (by simp [h, e])))
    · exact .inr (.inr (.inr named))

/-- **Synced, exactly.** Synced has the lowest rank, so it shows only when it is
every candidate: a connected device whose last pass ended synced, with nothing
moving, every upload stored and no record naming a state. -/
theorem synced_iff (f : Facts) :
    state f = .synced ↔ f.storage = .connected ∧ f.last = .synced ∧ f.moving = false ∧
      f.downloading = false ∧ (∀ u ∈ f.uploads, u.progress = .stored) ∧
      ∀ r ∈ f.records, r.state = none := by
  constructor
  · intro h
    have recorded := recorded_of f _ h (by simp)
    have ⟨mem, highest⟩ := (state_iff f recorded .synced).mp h
    have only : ∀ c ∈ f.candidates, c = .synced := by
      intro c hc
      have := highest c hc
      cases c <;> simp [State.rank] at this ⊢
    have loop := only f.loop (List.mem_cons_self ..)
    have still : f.moving = false := by
      cases mv : f.moving
      · rfl
      · simp [Facts.loop, mv] at loop
    refine ⟨?_, ?_, still, ?_, synced_files_stored f h, ?_⟩
    · cases hs : f.storage with
      | none => exact absurd hs recorded
      | connected => rfl
      | signedOut =>
          have := only .signedOut ((candidate_iff f _).mpr (.inr (.inr (.inr (.inl ⟨rfl, hs⟩)))))
          cases this
    · cases hl : f.last with
      | synced => rfl
      | failed p =>
          have := failed_ended f p hl
          simp [Facts.loop, still] at loop
          exact absurd loop this
    · cases d : f.downloading
      · rfl
      · have := only .syncing ((candidate_iff f _).mpr (.inr (.inl ⟨rfl, d⟩)))
        cases this
    · intro r hr
      cases hs : r.state with
      | none => rfl
      | some s =>
          have := only s ((candidate_iff f _).mpr (.inr (.inr (.inr (.inr ⟨r, hr, hs⟩)))))
          subst this
          have := reason_named r _ hs
          simp [State.named] at this
  · rintro ⟨connected, last, still, down, stored, records⟩
    have recorded : f.storage ≠ .none := by rw [connected]; simp
    apply (state_iff f recorded .synced).mpr
    have loop : f.loop = .synced := by simp [Facts.loop, still, Facts.ended, last]
    refine ⟨(candidate_iff f _).mpr (.inl loop), fun c hc => ?_⟩
    rcases (candidate_iff f c).mp hc with e | ⟨_, d⟩ | ⟨u, mu, hu⟩ | ⟨_, s⟩ | ⟨r, hr, hs⟩
    · rw [← e, loop]; exact Nat.le_refl _
    · rw [down] at d; cases d
    · simp [Upload.candidate, stored u mu] at hu
    · rw [connected] at s; cases s
    · rw [records r hr] at hs; cases hs

/-- What each wait says of the facts. -/
def Facts.waitHolds (f : Facts) : SyncWait → Prop
  | .notSyncing => f.running = false
  | .coolingDown => f.running = true ∧ f.cooling = true
  | .blocked => f.running = true ∧ f.cooling = false ∧ ∃ r, f.last = .failed (.blocked r)
  | .paused => f.heldAll = none ∧ f.paused = true ∧ ∃ u ∈ f.uploads, u.bound = false
  | .metered => f.heldAll = none ∧ ∃ u ∈ f.uploads, f.meteredHolds u = true
  | .retrying => f.heldAll = none ∧ ∃ u ∈ f.uploads, u.retrying = true
  | .queued => f.heldAll = none

theorem held_holds (f : Facts) (w : SyncWait) (h : f.heldAll = some w) : f.waitHolds w := by
  simp only [Facts.heldAll] at h
  cases hr : f.running
  · simp [hr] at h; subst h; exact hr
  · cases hc : f.cooling
    · cases hb : f.blocked
      · simp [hr, hc, hb] at h
      · simp [hr, hc, hb] at h
        subst h
        simp only [Facts.blocked] at hb
        split at hb
        · rename_i r e; exact ⟨hr, hc, r, e⟩
        · cases hb
    · simp [hr, hc] at h; subst h; exact ⟨hr, hc⟩

/-- **Why waiting to sync waits.** Each wait named holds for the fact that
names it: sync stopped, a cooldown, a slot that can't apply, or, for a queued
upload, what holds that upload. -/
theorem wait_facts (f : Facts) (w : SyncWait) (h : w ∈ f.waits) : f.waitHolds w := by
  simp only [Facts.waits, List.mem_append, List.mem_filterMap] at h
  rcases h with pass | ⟨u, mu, hu⟩
  · split at pass
    · simp only [List.mem_singleton] at pass
      subst pass
      cases hh : f.heldAll with
      | none => simp only [Facts.passWait, hh, Option.getD_none]; exact hh
      | some x => simp only [Facts.passWait, hh, Option.getD_some]; exact held_holds f x hh
    · simp at pass
  · split at hu
    · simp only [Option.some.injEq] at hu
      simp only [Facts.uploadWait] at hu
      cases hh : f.heldAll with
      | some x => rw [hh] at hu; simp only at hu; subst hu; exact held_holds f _ hh
      | none =>
          rw [hh] at hu
          simp only at hu
          split at hu
          · rename_i p
            subst hu
            simp only [Bool.and_eq_true, Bool.not_eq_true'] at p
            exact ⟨hh, p.1, u, mu, p.2⟩
          · split at hu
            · rename_i m
              subst hu
              exact ⟨hh, u, mu, m⟩
            · split at hu
              · rename_i r
                subst hu
                exact ⟨hh, u, mu, r⟩
              · subst hu
                exact hh
    · simp at hu

namespace Reached

/-- A device connected and synced, with sync running. -/
def start : Facts :=
  ⟨.connected, .synced, false, false, false, [], false, [], true, false, false, false, false, none⟩

def upload (retrying bound : Bool) : Upload := ⟨.queued, bound, retrying, none⟩

/-- The run of the model's events that reaches each state. -/
def toState : State → List Event
  | .notSynced => [.storage .none none]
  | .syncing => [.begin, .move]
  | .waitingToSync => [.begin, .finish (.failed (.blocked .keyUnavailable)) 1]
  | .synced => [.begin, .finish .synced 1]
  | .offline => [.begin, .finish (.failed .offline) 1]
  | .offlineUnsent => [.begin, .finish (.failed .offline) 1, .queue true]
  | .signedOut => [.storage .signedOut none]
  | .noAccess => [.begin, .finish (.failed (.storage .permissionDenied)) 1]
  | .storageFull => [.begin, .finish (.failed (.storage .quotaExceeded)) 1]
  | .folderMissing => [.begin, .finish (.failed (.storage .containerNotFound)) 1]
  | .updateNeeded => [.begin, .finish (.failed .updateRequired) 1]
  | .removed => [.begin, .finish (.failed .removed) 1]
  | .needsReset => [.begin, .finish (.failed .deviceIdInUse) 1]
  | .clockWrong => [.begin, .finish (.failed (.storage .clockSkew)) 1]
  | .failed => [.begin, .finish (.failed .other) 1]

/-- The run that reaches each wait. -/
def toWait : SyncWait → List Event
  | .notSyncing => [.settings false false false false, .uploads [upload false false]]
  | .coolingDown => [.settings true true false false, .uploads [upload false false]]
  | .blocked => [.begin, .finish (.failed (.blocked .keyUnavailable)) 1]
  | .paused => [.settings true false true false, .uploads [upload false false]]
  | .metered => [.settings true false false true, .network true, .uploads [upload false false]]
  | .retrying => [.uploads [upload true false]]
  | .queued => [.uploads [upload false false]]

/-- **No state is dead.** Every state shows after some run of the model's
events from a synced device. -/
theorem every_state (s : State) : (run start (toState s)).map state = some s := by
  cases s <;> decide

/-- **No wait is dead.** Every wait is what waiting to sync names after some
run of the model's events. -/
theorem every_wait (w : SyncWait) :
    (run start (toWait w)).map (fun f => (state f, f.waits.head?)) =
      some (.waitingToSync, some w) := by
  cases w <;> decide

end Reached

end CovenIO.SyncState
