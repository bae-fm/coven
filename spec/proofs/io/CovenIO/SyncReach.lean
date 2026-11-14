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
synced device to each state and each wait, so none of them is dead. Which store
histories produce those facts is `StoreReach`'s.
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

/-- **Which fact names a state.** The loop, a bound upload, a queued write,
the missing credentials or a pending record. -/
theorem candidate_iff (f : Facts) (s : State) :
    s ∈ f.candidates ↔ f.loop = s ∨
      (∃ u ∈ f.bound, u.candidate f.held = some s) ∨
      (s = .work f.writeWait.isSome ∧ f.unsent = true) ∨
      (s = .signedOut ∧ f.storage = .signedOut) ∨ ∃ r ∈ f.records, r.state = some s := by
  simp only [Facts.candidates, Facts.rest, Facts.files, Facts.writes, Facts.others,
    List.mem_cons, List.mem_append, List.mem_filterMap]
  constructor
  · rintro (e | (up | write) | (signed | named))
    · exact .inl e.symm
    · exact .inr (.inl up)
    · split at write
      · rename_i w; simp at write; exact .inr (.inr (.inl ⟨write, w⟩))
      · simp at write
    · split at signed
      · rename_i h; simp at signed; exact .inr (.inr (.inr (.inl ⟨signed, h⟩)))
      · simp at signed
    · exact .inr (.inr (.inr (.inr named)))
  · rintro (e | up | ⟨e, w⟩ | ⟨e, h⟩ | named)
    · exact .inl e.symm
    · exact .inr (.inl (.inl up))
    · exact .inr (.inl (.inr (by simp [w, e])))
    · exact .inr (.inr (.inl (by simp [h, e])))
    · exact .inr (.inr (.inr named))

/-- **Synced, exactly.** Synced has the lowest rank, so it shows only when it is
every candidate: a connected device whose last pass ended synced, with nothing
moving, every bound upload stored, no write queued and no record naming a
state. -/
theorem synced_iff (f : Facts) :
    state f = .synced ↔ f.storage = .connected ∧ f.last = .synced ∧ f.moved = none ∧
      (∀ u ∈ f.bound, u.progress = .stored) ∧ f.unsent = false ∧
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
    have still : f.moved = none := by
      cases mv : f.moved
      · rfl
      · simp [Facts.loop, mv] at loop
    refine ⟨?_, ?_, still, synced_bound_stored f h, synced_writes_sent f h, ?_⟩
    · cases hs : f.storage with
      | none => exact absurd hs recorded
      | connected => rfl
      | signedOut =>
          have := only .signedOut
            ((candidate_iff f _).mpr (.inr (.inr (.inr (.inl ⟨rfl, hs⟩)))))
          cases this
    · cases hl : f.last with
      | synced => rfl
      | failed p =>
          have := failed_ended f p hl
          simp [Facts.loop, still] at loop
          exact absurd loop this
    · intro r hr
      cases hs : r.state with
      | none => rfl
      | some s =>
          have := only s ((candidate_iff f _).mpr (.inr (.inr (.inr (.inr ⟨r, hr, hs⟩)))))
          subst this
          have := reason_named r _ hs
          simp [State.named] at this
  · rintro ⟨connected, last, still, stored, sent, records⟩
    have recorded : f.storage ≠ .none := by rw [connected]; simp
    apply (state_iff f recorded .synced).mpr
    have loop : f.loop = .synced := by simp [Facts.loop, still, Facts.ended, last]
    refine ⟨(candidate_iff f _).mpr (.inl loop), fun c hc => ?_⟩
    rcases (candidate_iff f c).mp hc with e | ⟨u, mu, hu⟩ | ⟨_, w⟩ | ⟨_, s⟩ | ⟨r, hr, hs⟩
    · rw [← e, loop]; exact Nat.le_refl _
    · simp [Upload.candidate, stored u mu] at hu
    · rw [sent] at w; cases w
    · rw [connected] at s; cases s
    · rw [records r hr] at hs; cases hs

/-- What each wait says of the facts. -/
def Facts.waitHolds (f : Facts) : SyncWait → Prop
  | .notSyncing => f.running = false
  | .coolingDown => f.running = true ∧ f.cooling = true
  | .keyCopy =>
      (f.running = true ∧ f.cooling = false ∧
        ∃ r, f.last = .failed (.blocked r) ∧ r.slotWait = some .keyCopy) ∨
      (f.unsent = true ∧ f.writeKey = true ∧ f.passWait = none ∧ f.fileWait = none)
  | .snapshot =>
      f.running = true ∧ f.cooling = false ∧
        ∃ r, f.last = .failed (.blocked r) ∧ r.slotWait = some .snapshot
  | .retrying =>
      f.heldAll = none ∧
        ((∃ u ∈ f.bound, u.retrying = true) ∨ (f.lastRetries = true ∧ f.passRetrying = true))

theorem held_holds (f : Facts) (w : SyncWait) (h : f.heldAll = some w) : f.waitHolds w := by
  simp only [Facts.heldAll] at h
  cases hr : f.running
  · simp [hr] at h; subst h; exact hr
  · cases hc : f.cooling
    · simp only [hr, hc] at h
      simp only [Bool.not_true, Bool.false_eq_true, ite_false] at h
      simp only [Facts.slotWait] at h
      split at h
      · rename_i r e
        have kind : w = .keyCopy ∨ w = .snapshot := by
          cases r with
          | keyUnavailable holders =>
              cases holders <;> simp [Reason.slotWait] at h <;> simp [← h]
          | _ => simp [Reason.slotWait] at h <;> simp [← h]
        rcases kind with rfl | rfl
        · exact .inl ⟨hr, hc, r, e, h⟩
        · exact ⟨hr, hc, r, e, h⟩
      · simp at h
    · simp [hr, hc] at h; subst h; exact ⟨hr, hc⟩

/-- What the next pass waits for, the held-back pass's and a queued write's
wait, holds: what holds every transfer, else the backoff of a failure that
clears by itself; with neither, no wait is named. -/
theorem pass_wait_holds (f : Facts) (w : SyncWait) (h : f.passWait = some w) :
    f.waitHolds w := by
  cases hh : f.heldAll with
  | none =>
      simp only [Facts.passWait, hh] at h
      by_cases r : (f.lastRetries && f.passRetrying) = true
      · simp only [r, ite_true, Option.some.injEq] at h
        subst h
        exact ⟨hh, .inr (by simpa using r)⟩
      · simp only [r] at h
        cases h
  | some x =>
      simp only [Facts.passWait, hh, Option.some.injEq] at h
      subst h
      exact held_holds f x hh

/-- A bound upload's wait holds for what the upload waits for. -/
theorem upload_wait_holds (f : Facts) (u : Upload) (mu : u ∈ f.bound) (w : SyncWait)
    (h : f.uploadWait u = some w) : f.waitHolds w := by
  simp only [Facts.uploadWait] at h
  cases hh : f.heldAll with
  | some x =>
      rw [hh] at h
      simp only [Option.some.injEq] at h
      subst h
      exact held_holds f _ hh
  | none =>
      rw [hh] at h
      simp only at h
      split at h
      · rename_i r
        simp only [Option.some.injEq] at h
        subst h
        exact ⟨hh, .inl ⟨u, mu, r⟩⟩
      · cases h

/-- What the first waiting bound upload waits for holds. -/
theorem file_wait_holds (f : Facts) (w : SyncWait) (h : f.fileWait = some w) : f.waitHolds w := by
  have mem := List.mem_of_mem_head? h
  rw [List.mem_filterMap] at mem
  obtain ⟨u, mu, hu⟩ := mem
  split at hu
  · exact upload_wait_holds f u mu w hu
  · cases hu

/-- What a queued write waits for holds: the pass's wait, a bound upload's, or
the key the write is recorded as waiting for. -/
theorem write_wait_holds (f : Facts) (unsent : f.unsent = true) (w : SyncWait)
    (h : f.writeWait = some w) : f.waitHolds w := by
  simp only [Facts.writeWait] at h
  cases hp : f.passWait with
  | some x =>
      rw [hp] at h
      simp only [Option.orElse_some, Option.some.injEq] at h
      subst h
      exact pass_wait_holds f x hp
  | none =>
      cases hf : f.fileWait with
      | some x =>
          rw [hp, hf] at h
          simp only [Option.orElse_none, Option.orElse_some, Option.some.injEq] at h
          subst h
          exact file_wait_holds f x hf
      | none =>
          rw [hp, hf] at h
          cases hk : f.writeKey
          · simp [hk] at h
          · simp [hk] at h
            subst h
            exact .inr ⟨unsent, hk, hp, hf⟩

/-- **Why waiting to sync waits.** Each wait named holds for the fact that
names it: sync stopped, a cooldown, a key or a snapshot a slot waits for, a
retry, or, for a queued upload or write, what holds it. -/
theorem wait_facts (f : Facts) (w : SyncWait) (h : w ∈ f.waits) : f.waitHolds w := by
  simp only [Facts.waits, List.mem_append, List.mem_filterMap] at h
  rcases h with (pass | ⟨u, mu, hu⟩) | write
  · split at pass
    · exact pass_wait_holds f w (Option.mem_toList.mp pass)
    · simp at pass
  · split at hu
    · exact upload_wait_holds f u mu w hu
    · simp at hu
  · split at write
    · rename_i unsent
      exact write_wait_holds f unsent w (Option.mem_toList.mp write)
    · simp at write

namespace Reached

/-- A device connected and synced, with sync running. -/
def start : Facts :=
  ⟨.connected, .synced, false, none, [], false, [], true, false, false, false, false, false,
    false, false, none, false⟩

def upload (retrying bound : Bool) : Upload := ⟨.queued, bound, retrying, none⟩

/-- The run of the model's events that reaches each state. -/
def toState : State → List Event
  | .notSynced => [.storage .none none]
  | .syncing => [.begin, .move .read]
  | .waitingToSync => [.begin, .finish (.failed (.blocked (.keyUnavailable true))) 1]
  | .synced => [.begin, .finish .synced 1]
  | .offline => [.begin, .finish (.failed .offline) 1]
  | .offlineUnsent => [.begin, .finish (.failed .offline) 1, .queue true]
  | .signedOut => [.storage .signedOut none]
  | .noAccess => [.begin, .finish (.failed (.storage .permissionDenied)) 1]
  | .storageFull => [.begin, .finish (.failed (.storage .quotaExceeded)) 1]
  | .folderMissing => [.begin, .finish (.failed (.storage .containerNotFound)) 1]
  | .updateNeeded => [.begin, .finish (.failed .updateRequired) 1]
  | .removed => [.begin, .finish (.failed (.removed .device)) 1]
  | .needsReset => [.begin, .finish (.failed .replaced) 1]
  | .clockWrong => [.begin, .finish (.failed (.storage .clockSkew)) 1]
  | .failed => [.begin, .finish (.failed .other) 1]

/-- The run that reaches each wait, with a bound upload. -/
def toWait : SyncWait → List Event
  | .notSyncing => [.settings false false false false, .uploads [upload false true]]
  | .coolingDown => [.settings true true false false, .uploads [upload false true]]
  | .keyCopy => [.begin, .finish (.failed (.blocked (.keyUnavailable true))) 1]
  | .snapshot => [.begin, .finish (.failed (.blocked .waitsReload)) 1]
  | .retrying => [.uploads [upload true true]]

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
