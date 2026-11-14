import CovenIO.SyncReach

/-!
# Which removal, which reset, and what is unsent (§13.2, §14.1, §19.1, §19.3)

Removed and device needs reset each say which: an admin removed this device, its
member was removed or left, or the admission that published a joined store was
refused (`Removal`); another installation took this one's place, or the member's
keys were replaced with a pair this device lacks (`ResetReason`). The facts that
name the state each carry one, and the first, in the order candidates are
taken, is the one shown.

- `removed_names_which`: removed shows exactly when a fact names a removal, and
  the shown one is the first.
- `reset_names_why`: needs reset shows exactly when a fact names a reset reason
  and none names a removal, and the shown one is the first.
- `stopped_unsent`: the unsent work a stopped state shows is the send queue's
  and the upload queue's, whatever else holds the work.

`CovenSequence.own_writes_never_refused` says why a stopped device carries its
unsent work and not its `WritesRefused` records: a device that follows the claim
rules never has its own write refused by its own cut-off, so the writes a
removal or a replacement costs it are the ones it never sent.
-/
namespace CovenIO.SyncState

/-- The removal a pending reason names, if it names a removed device. -/
def Reason.removal : Reason → Option Removal
  | .removed r => some r
  | .cleanup _ c => c.removal
  | _ => none

/-- The reset reason a pending reason names, if it names a device that needs
reset. -/
def Reason.reset : Reason → Option ResetReason
  | .memberKeysReplaced => some .memberKeysReplaced
  | .cleanup _ c => c.reset
  | _ => none

/-- The removal a failed pass names. -/
def PassFailure.removal : PassFailure → Option Removal
  | .removed r => some r
  | .operation r => r.removal
  | .blocked r => r.removal
  | _ => none

/-- The reset reason a failed pass names. -/
def PassFailure.reset : PassFailure → Option ResetReason
  | .replaced => some .replaced
  | .operation r => r.reset
  | .blocked r => r.reset
  | _ => none

theorem reason_removal_iff (r : Reason) : r.state = some .removed ↔ r.removal.isSome = true := by
  induction r with
  | storage g _ => cases g <;> simp [Reason.state, Failure.named, Failure.byItself, Reason.removal]
  | failed l => cases l <;> simp [Reason.state, Reason.removal]
  | cleanup _ c _ ihc => simpa [Reason.state, Reason.removal] using ihc
  | _ => simp [Reason.state, Reason.removal]

theorem reason_reset_iff (r : Reason) : r.state = some .needsReset ↔ r.reset.isSome = true := by
  induction r with
  | storage g _ => cases g <;> simp [Reason.state, Failure.named, Failure.byItself, Reason.reset]
  | failed l => cases l <;> simp [Reason.state, Reason.reset]
  | cleanup _ c _ ihc => simpa [Reason.state, Reason.reset] using ihc
  | _ => simp [Reason.state, Reason.reset]

/-- A slot's reason that names no state of its own names failed or nothing. -/
theorem slot_state_failed (r : Reason) (h : r.state = none) (s : State)
    (e : r.slotState = some s) : s = .failed := by
  unfold Reason.slotState at e
  rw [h] at e
  cases r <;> simp at e <;> (obtain ⟨_, rfl⟩ := e; rfl)

theorem pass_removal_iff (p : PassFailure) : p.state = some .removed ↔ p.removal.isSome = true := by
  cases p with
  | storage g => cases g <;> simp [PassFailure.state, Failure.named, Failure.byItself, PassFailure.removal]
  | operation r =>
      have := reason_removal_iff r
      simp only [PassFailure.state, PassFailure.removal]
      cases h : r.state with
      | some s => simp [h] at this ⊢; rw [this]
      | none => cases r.byItself <;> simp [h] at this ⊢ <;> exact this
  | blocked r =>
      have := reason_removal_iff r
      simp only [PassFailure.state, PassFailure.removal]
      cases h : r.state with
      | some s => simp [Reason.slotState, h] at this ⊢; rw [this]
      | none =>
          have no : r.slotState ≠ some .removed := fun e => by
            have := slot_state_failed r h _ e; cases this
          have hn : r.removal = none := by
            cases hr : r.removal with
            | none => rfl
            | some x =>
                have := this.mpr (by simp [hr])
                rw [h] at this; cases this
          simp only [Reason.slotState, h] at no ⊢
          rw [hn]
          exact ⟨fun e => absurd e no, fun e => by simp at e⟩
  | _ => simp [PassFailure.state, PassFailure.removal]

theorem pass_reset_iff (p : PassFailure) : p.state = some .needsReset ↔ p.reset.isSome = true := by
  cases p with
  | storage g => cases g <;> simp [PassFailure.state, Failure.named, Failure.byItself, PassFailure.reset]
  | operation r =>
      have := reason_reset_iff r
      simp only [PassFailure.state, PassFailure.reset]
      cases h : r.state with
      | some s => simp [h] at this ⊢; rw [this]
      | none => cases r.byItself <;> simp [h] at this ⊢ <;> exact this
  | blocked r =>
      have := reason_reset_iff r
      simp only [PassFailure.state, PassFailure.reset]
      cases h : r.state with
      | some s => simp [Reason.slotState, h] at this ⊢; rw [this]
      | none =>
          have no : r.slotState ≠ some .needsReset := fun e => by
            have := slot_state_failed r h _ e; cases this
          have hn : r.reset = none := by
            cases hr : r.reset with
            | none => rfl
            | some x =>
                have := this.mpr (by simp [hr])
                rw [h] at this; cases this
          simp only [Reason.slotState, h] at no ⊢
          rw [hn]
          exact ⟨fun e => absurd e no, fun e => by simp at e⟩
  | _ => simp [PassFailure.state, PassFailure.reset]

/-- The removals the facts name, in the order candidates are taken: the last
pass when nothing moves, then each queued bound upload's record, then the
pending records. -/
def Facts.removals (f : Facts) : List Removal :=
  (match f.moved, f.last with
    | none, .failed p => p.removal.toList
    | _, _ => []) ++
  f.bound.filterMap (fun u => if u.progress = .queued then u.record.bind Reason.removal else none) ++
  f.records.filterMap Reason.removal

/-- The reset reasons the facts name, in the same order. -/
def Facts.resets (f : Facts) : List ResetReason :=
  (match f.moved, f.last with
    | none, .failed p => p.reset.toList
    | _, _ => []) ++
  f.bound.filterMap (fun u => if u.progress = .queued then u.record.bind Reason.reset else none) ++
  f.records.filterMap Reason.reset

/-- The removal shown with removed: the first one named. -/
def Facts.removal (f : Facts) : Option Removal := f.removals.head?

/-- The reset reason shown with needs reset: the first one named. -/
def Facts.resetReason (f : Facts) : Option ResetReason := f.resets.head?

/-- A standing state (removed or needs reset) is among the candidates exactly
when the last pass, a queued bound upload's record or a pending record names it. -/
theorem standing_candidate (f : Facts) (s : State) (hs : s = .removed ∨ s = .needsReset) :
    s ∈ f.candidates ↔
      (f.moved = none ∧ ∃ p, f.last = .failed p ∧ p.state = some s) ∨
      (∃ u ∈ f.bound, u.progress = .queued ∧ u.record.bind Reason.state = some s) ∨
      (∃ r ∈ f.records, r.state = some s) := by
  have ne : s ≠ .syncing ∧ s ≠ .waitingToSync ∧ s ≠ .signedOut ∧ s ≠ .synced ∧ s ≠ .offline ∧
      s ≠ .offlineUnsent := by
    rcases hs with rfl | rfl <;> simp
  simp only [Facts.candidates, Facts.rest, Facts.files, Facts.writes, Facts.others, Facts.loop,
    List.mem_cons, List.mem_append, List.mem_filterMap]
  obtain ⟨nsyncing, nwaiting, nsigned, nsynced, noffline, nofflineU⟩ := ne
  have nwork : ∀ b, s ≠ State.work b := fun b e => by
    rcases work_cases b with w | w <;> rw [w] at e
    · exact nwaiting e
    · exact nsyncing e
  have loop : (s = if f.moved.isSome = true then State.syncing else f.ended) ↔
      (f.moved = none ∧ ∃ p, f.last = LastPass.failed p ∧ p.state = some s) := by
    cases hm : f.moved with
    | some m => simp [nsyncing]
    | none =>
        simp only [Option.isSome_none, Bool.false_eq_true, ite_false, true_and]
        unfold Facts.ended
        cases hl : f.last with
        | synced => simp [nsynced]
        | failed p =>
            cases hp : p.state with
            | none => simp only [hp]; simp [hp, nwork]
            | some t =>
                simp only [hp]
                rcases hs with rfl | rfl <;> cases t <;> cases f.unsentWork <;> simp [hp]
  have files : (∃ a, a ∈ f.bound ∧ Upload.candidate f.held a = some s) ↔
      (∃ u, u ∈ f.bound ∧ u.progress = Progress.queued ∧ u.record.bind Reason.state = some s) := by
    constructor
    · rintro ⟨u, mu, h⟩
      refine ⟨u, mu, ?_⟩
      unfold Upload.candidate at h
      cases hp : u.progress with
      | moving => rw [hp] at h; simp at h; exact absurd h.symm nsyncing
      | stored => rw [hp] at h; simp at h
      | queued =>
          rw [hp] at h
          refine ⟨rfl, ?_⟩
          simp only [Option.some.injEq] at h
          cases hr : u.record.bind Reason.state with
          | some x => rw [hr] at h; simp at h; rw [h]
          | none => rw [hr] at h; simp at h; exact absurd h.symm (nwork _)
    · rintro ⟨u, mu, hq, h⟩
      refine ⟨u, mu, ?_⟩
      unfold Upload.candidate
      rw [hq]
      simp [h]
  have writes : s ∉ (if f.unsent = true then [State.work f.writeWait.isSome] else []) := by
    split
    · intro m; exact nwork _ (List.mem_singleton.mp m)
    · simp
  have signed : s ∉ (if f.storage = Storage.signedOut then [State.signedOut] else []) := by
    split
    · intro m; exact nsigned (List.mem_singleton.mp m)
    · simp
  rw [loop, files]
  simp only [writes, signed, or_false, false_or]

theorem nonempty_iff {α : Type} (l : List α) : l ≠ [] ↔ ∃ x, x ∈ l := by
  cases l with
  | nil => simp
  | cons x _ => simp

theorem some_iff {α : Type} (o : Option α) : (∃ x, o = some x) ↔ o.isSome = true := by
  cases o <;> simp

/-- The facts name a removal exactly when the state's candidates include `removed`. -/
theorem removals_iff (f : Facts) : f.removals ≠ [] ↔ State.removed ∈ f.candidates := by
  rw [standing_candidate f .removed (by simp)]
  have lastIff : (∃ x, x ∈ (match f.moved, f.last with
        | none, .failed p => p.removal.toList
        | _, _ => [])) ↔
      (f.moved = none ∧ ∃ p, f.last = .failed p ∧ p.state = some .removed) := by
    cases f.moved <;> cases f.last <;> simp [pass_removal_iff, ← some_iff, Option.mem_toList]
  have filesIff : (∃ x, x ∈ f.bound.filterMap (fun u =>
        if u.progress = .queued then u.record.bind Reason.removal else none)) ↔
      (∃ u ∈ f.bound, u.progress = .queued ∧ u.record.bind Reason.state = some .removed) := by
    simp only [List.mem_filterMap]
    constructor
    · rintro ⟨x, u, mu, h⟩
      refine ⟨u, mu, ?_⟩
      by_cases q : u.progress = .queued
      · refine ⟨q, ?_⟩
        simp only [q, ite_true] at h
        cases hr : u.record with
        | none => simp [hr] at h
        | some r =>
            simp only [hr, Option.bind_some] at h ⊢
            exact (reason_removal_iff r).mpr (by rw [h]; rfl)
      · simp [q] at h
    · rintro ⟨u, mu, q, h⟩
      cases hr : u.record with
      | none => simp [hr] at h
      | some r =>
          simp only [hr, Option.bind_some] at h
          have := (reason_removal_iff r).mp h
          obtain ⟨x, hx⟩ := (some_iff _).mpr this
          exact ⟨x, u, mu, by simp [q, hr, hx]⟩
  have recordsIff : (∃ x, x ∈ f.records.filterMap Reason.removal) ↔
      (∃ r ∈ f.records, r.state = some .removed) := by
    simp only [List.mem_filterMap]
    constructor
    · rintro ⟨x, r, mr, h⟩
      exact ⟨r, mr, (reason_removal_iff r).mpr (by rw [h]; rfl)⟩
    · rintro ⟨r, mr, h⟩
      obtain ⟨x, hx⟩ := (some_iff _).mpr ((reason_removal_iff r).mp h)
      exact ⟨x, r, mr, hx⟩
  rw [nonempty_iff]
  simp only [Facts.removals, List.mem_append, exists_or]
  rw [lastIff, filesIff, recordsIff]
  constructor
  · rintro ((a | b) | c)
    · exact .inl a
    · exact .inr (.inl b)
    · exact .inr (.inr c)
  · rintro (a | b | c)
    · exact .inl (.inl a)
    · exact .inl (.inr b)
    · exact .inr c

/-- The facts name a reset exactly when the state's candidates include `needsReset`. -/
theorem resets_iff (f : Facts) : f.resets ≠ [] ↔ State.needsReset ∈ f.candidates := by
  rw [standing_candidate f .needsReset (by simp)]
  have lastIff : (∃ x, x ∈ (match f.moved, f.last with
        | none, .failed p => p.reset.toList
        | _, _ => [])) ↔
      (f.moved = none ∧ ∃ p, f.last = .failed p ∧ p.state = some .needsReset) := by
    cases f.moved <;> cases f.last <;> simp [pass_reset_iff, ← some_iff, Option.mem_toList]
  have filesIff : (∃ x, x ∈ f.bound.filterMap (fun u =>
        if u.progress = .queued then u.record.bind Reason.reset else none)) ↔
      (∃ u ∈ f.bound, u.progress = .queued ∧ u.record.bind Reason.state = some .needsReset) := by
    simp only [List.mem_filterMap]
    constructor
    · rintro ⟨x, u, mu, h⟩
      refine ⟨u, mu, ?_⟩
      by_cases q : u.progress = .queued
      · refine ⟨q, ?_⟩
        simp only [q, ite_true] at h
        cases hr : u.record with
        | none => simp [hr] at h
        | some r =>
            simp only [hr, Option.bind_some] at h ⊢
            exact (reason_reset_iff r).mpr (by rw [h]; rfl)
      · simp [q] at h
    · rintro ⟨u, mu, q, h⟩
      cases hr : u.record with
      | none => simp [hr] at h
      | some r =>
          simp only [hr, Option.bind_some] at h
          have := (reason_reset_iff r).mp h
          obtain ⟨x, hx⟩ := (some_iff _).mpr this
          exact ⟨x, u, mu, by simp [q, hr, hx]⟩
  have recordsIff : (∃ x, x ∈ f.records.filterMap Reason.reset) ↔
      (∃ r ∈ f.records, r.state = some .needsReset) := by
    simp only [List.mem_filterMap]
    constructor
    · rintro ⟨x, r, mr, h⟩
      exact ⟨r, mr, (reason_reset_iff r).mpr (by rw [h]; rfl)⟩
    · rintro ⟨r, mr, h⟩
      obtain ⟨x, hx⟩ := (some_iff _).mpr ((reason_reset_iff r).mp h)
      exact ⟨x, r, mr, hx⟩
  rw [nonempty_iff]
  simp only [Facts.resets, List.mem_append, exists_or]
  rw [lastIff, filesIff, recordsIff]
  constructor
  · rintro ((a | b) | c)
    · exact .inl a
    · exact .inr (.inl b)
    · exact .inr (.inr c)
  · rintro (a | b | c)
    · exact .inl (.inl a)
    · exact .inl (.inr b)
    · exact .inr c

/-- The state shown is never not synced while a location is recorded, so a
state of rank 12 or more is removed or needs reset. -/
theorem standing_of_rank (f : Facts) (recorded : f.storage ≠ .none) (h : 12 ≤ (state f).rank) :
    state f = .needsReset ∨ state f = .removed := by
  have notSynced : state f ≠ .notSynced := fun e => recorded ((not_synced_iff f).mp e)
  cases hs : state f <;> simp_all [State.rank]

/-- **Removed names which removal.** Removed shows exactly when a fact names a
removal, and the removal shown is the first one named. -/
theorem removed_names_which (f : Facts) (recorded : f.storage ≠ .none) :
    (state f = .removed ↔ f.removal.isSome = true) ∧ f.removal = f.removals.head? := by
  refine ⟨?_, rfl⟩
  have ⟨mem, highest, _⟩ := one_state f recorded
  have some_iff_ne : f.removal.isSome = true ↔ f.removals ≠ [] := by
    cases hr : f.removals <;> simp [Facts.removal, hr]
  rw [some_iff_ne, removals_iff]
  constructor
  · intro e; rw [← e]; exact mem
  · intro named
    have := highest _ named
    have r13 : State.removed.rank = 13 := rfl
    rcases standing_of_rank f recorded (by omega) with e | e
    · rw [e] at this; simp [State.rank] at this
    · exact e

/-- **Needs reset names why.** Needs reset shows exactly when a fact names a
reset reason and none names a removal, which outranks it; the reason shown is
the first one named. -/
theorem reset_names_why (f : Facts) (recorded : f.storage ≠ .none) :
    (state f = .needsReset ↔ f.removals = [] ∧ f.resets ≠ []) ∧
      f.resetReason = f.resets.head? := by
  refine ⟨?_, rfl⟩
  have ⟨mem, highest, _⟩ := one_state f recorded
  constructor
  · intro e
    refine ⟨?_, (resets_iff f).mpr (e ▸ mem)⟩
    apply Classical.byContradiction
    intro ne
    have := highest _ ((removals_iff f).mp ne)
    rw [e] at this
    simp [State.rank] at this
  · rintro ⟨none, named⟩
    have gone : State.removed ∉ f.candidates := fun m => (removals_iff f).mpr m none
    have := highest _ ((resets_iff f).mp named)
    have r12 : State.needsReset.rank = 12 := rfl
    rcases standing_of_rank f recorded (by omega) with e | e
    · exact e
    · exact absurd (e ▸ mem) gone

/-- What a stopped state says was never sent: whether the send queue holds a
write and whether the upload queue holds a file (`UnsentWork`). -/
structure Unsent where
  writes : Bool
  files : Bool
  deriving DecidableEq, Repr

/-- The unsent work removed and needs reset show: the queues' own, whatever else
holds the work. -/
def Facts.unsentShown (f : Facts) : Unsent := ⟨f.unsent, f.unsentFiles⟩

/-- **A stopped state counts every queued write and file.** A write in the send
queue, or a bound file with bytes to send, shows as unsent work, so a device
that will never send them says so. The upload queue holds every bound file. -/
theorem stopped_unsent (f : Facts)
    (queued : ∀ u ∈ f.bound, u.progress ≠ .stored → f.unsentFiles = true) :
    f.unsentWork = true → f.unsentShown.writes = true ∨ f.unsentShown.files = true := by
  intro h
  simp only [Facts.unsentWork, Bool.or_eq_true, List.any_eq_true, bne_iff_ne, ne_eq] at h
  rcases h with w | ⟨u, mu, p⟩
  · exact .inl w
  · exact .inr (queued u mu p)

end CovenIO.SyncState
