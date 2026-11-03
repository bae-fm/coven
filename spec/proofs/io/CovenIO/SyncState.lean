/-! The sync state a status screen shows (§19.1, api.md E5 `SyncStatus`), as
`SyncStatus::read` computes it from `coven_sync_status`'s row, the send
queue, the transfers in flight and the pending records.

The state is one flat list, in the order a host lists them: not synced,
syncing, synced, offline, offline with unsent changes, signed out, no access
to storage, storage full, storage folder missing, update needed, removed from
the store, device needs reset, clock wrong, and failed with no named cause.
Coven names no words; the host picks them.

The state is computed, never stored, from facts that are each true on their
own: whether a location is recorded and this device holds credentials for it
(`Storage`), how the last pass that reached storage ended (`LastPass`),
whether a running pass has moved a slot (`moving`), whether a file is being
sent or downloaded (`transferring`), whether the send queue holds a write
(`unsent`), and the pending records. Each fact is a candidate state or none;
when several hold, the state is the one of highest rank (`State.rank`). So a
write that commits while offline turns Offline into Offline with unsent
changes in the same transaction, with no step of the loop's.

Syncing shows only while something moves: a pass that has read or claimed a
slot, or a file in flight while the last pass was synced. A pass that finds
nothing writes nothing the state reads, so the state doesn't change while it
runs, and ends as it was when it ends as the last one did (`quiet_pass_keeps`).

A wait names no state: a pass held on a key, a snapshot or another device's
object, a provider's cooldown, a pending invite. Such a pass reached storage,
so it counts as synced; it doesn't move the time of the last finished sync,
which only a pass reaching the read end sets. That time is kept across every
state and every reopen, and never goes backward, even when the clock does
(`last_synced_never_back`).

Proved here: the state is the one candidate no other outranks (`one_state`);
no storage recorded is not synced, and only that (`not_synced_iff`), while a
recorded location without credentials is signed out unless the device's own
standing outranks it (`signed_out_without_credentials`); offline with unsent
changes holds exactly when the state is offline and the send queue is not
empty (`offline_unsent_iff`, `offline_iff`); syncing only while a slot or a
file moves (`syncing_only_moving`); a quiet pass keeps the state
(`quiet_pass_keeps`); the time of the last finished sync never goes backward
and only a pass reaching the read end changes it (`last_synced_never_back`,
`last_synced_kept`). Each pending reason maps to one state or none by
`Reason.state`, a function; waits map to none (`waits_name_no_state`). -/

namespace CovenIO.SyncState

/-- What a status screen shows, in the order a host lists them. -/
inductive State
  | notSynced
  | syncing
  | synced
  | offline
  | offlineUnsent
  | signedOut
  | noAccess
  | storageFull
  | folderMissing
  | updateNeeded
  | removed
  | needsReset
  | clockWrong
  | failed
  deriving DecidableEq, Repr

/-- Which state shows when several hold: the highest. The device's own
standing first (removed, reset, update), then signing in, then what storage
said of the account or location, then reachability, then the catch-all
failure, then activity. -/
def State.rank : State → Nat
  | .synced => 0
  | .syncing => 1
  | .failed => 2
  | .offline => 3
  | .offlineUnsent => 4
  | .storageFull => 5
  | .clockWrong => 6
  | .folderMissing => 7
  | .noAccess => 8
  | .signedOut => 9
  | .updateNeeded => 10
  | .needsReset => 11
  | .removed => 12
  | .notSynced => 13

theorem rank_injective (a b : State) (h : a.rank = b.rank) : a = b := by
  cases a <;> cases b <;> simp_all [State.rank]

/-- A provider failure, as `StorageFailure` names it. -/
inductive Failure
  | providerUnavailable | accessNotRegistered | accessNotGranted
  | dropboxAppFolderNotShareable | catchUpRequired | network | authentication
  | permissionDenied | notFound | alreadyExists | conflict | containerNotFound
  | regionMismatch | storeExists | clockSkew | ownBucketRequired | quotaExceeded
  | rateLimited | invalidConfiguration | refused | protocol | invalidPath
  | invalidRange | notStoreOwner | inviteeIsStoreOwner | accountIdUnavailable
  | invitationMismatch | sessionMismatch | sessionExpired | sessionIdentityUnknown
  | invalidPart | singleRequestTooLarge | copyTooLarge | encoding | file
  | memberKeysMissing
  deriving DecidableEq, Repr

/-- The state a provider failure names: what storage said of this account or
location that a person must see. -/
def Failure.named : Failure → Option State
  | .authentication => some .signedOut
  | .permissionDenied | .accessNotGranted => some .noAccess
  | .quotaExceeded => some .storageFull
  | .containerNotFound => some .folderMissing
  | .clockSkew => some .clockWrong
  | _ => none

/-- The failures that clear by themselves (`Clears::ByItself`). -/
def Failure.byItself : Failure → Bool
  | .network | .conflict | .rateLimited | .notFound | .sessionExpired
  | .catchUpRequired | .quotaExceeded | .file => true
  | _ => false

/-- A local failure, as `LocalFailure` names it. -/
inductive Local
  | database | disk | keyCustody | crypto | operation | shutdown
  deriving DecidableEq, Repr

/-- A pending record's reason, as `PendingReason` names it. -/
inductive Reason
  | missing
  | waits
  | keyUnavailable
  | updateRequired
  | noStorage
  | storage (failure : Failure) (answered : Bool)
  | refused
  | writesRefused
  | providerPending
  | accountInUse
  | accessRemains
  | fileLost
  | fileSource
  | originalMissing
  | originalChanged
  | failed (failure : Local)
  | rejected
  | cleanup (operation cleanup : Reason)
  | paused
  | removed
  | memberKeysReplaced
  deriving DecidableEq, Repr

/-- The state a pending record names, by its reason. A wait names none, nor
does a verdict on an admin operation or on another member's writes, which
the app shows with that operation. A provider failure that clears by itself
names none either: it retries on its own, and the pass says whether storage
is reachable. Any other failure names the state its cause does, or failed. A
cleanup names what its cleanup waits on, as it retries that. -/
def Reason.state : Reason → Option State
  | .storage f _ =>
      match f.named with
      | some s => some s
      | none => if f.byItself then none else some .failed
  | .updateRequired => some .updateNeeded
  | .removed => some .removed
  | .memberKeysReplaced => some .needsReset
  | .refused | .fileLost | .fileSource | .originalChanged => some .failed
  | .failed l => if l = .shutdown then none else some .failed
  | .cleanup _ c => c.state
  | .missing | .waits | .keyUnavailable | .noStorage | .writesRefused | .providerPending
  | .accountInUse | .accessRemains | .originalMissing | .rejected | .paused => none

/-- Why a pass that reached storage, or the attempt to, didn't end synced:
`PassFailure` and `SyncFailure`. -/
inductive PassFailure
  /-- No provider response. -/
  | offline
  /-- The provider answered with this failure. -/
  | storage (failure : Failure)
  | updateRequired
  | removed
  | deviceIdInUse
  /-- The pass finished, but coven's own operation failed with this reason. -/
  | operation (reason : Reason)
  /-- A slot can't apply, for this reason. -/
  | blocked (reason : Reason)
  | other
  deriving DecidableEq, Repr

/-- The state a failed pass names; none for a wait, such as a provider's
cooldown or a slot waiting on another device's key. -/
def PassFailure.state : PassFailure → Option State
  | .offline => some .offline
  | .storage f =>
      match f.named with
      | some s => some s
      | none => if f = .rateLimited then none else some .failed
  | .updateRequired => some .updateNeeded
  | .removed => some .removed
  | .deviceIdInUse => some .needsReset
  | .operation r => some (r.state.getD .failed)
  | .blocked r => r.state
  | .other => some .failed

/-- How the last pass that reached storage, or failed trying, ended. -/
inductive LastPass
  | synced
  | failed (failure : PassFailure)
  deriving DecidableEq, Repr

/-- Whether a location is recorded and this device holds its credentials. -/
inductive Storage
  | none
  | signedOut
  | connected
  deriving DecidableEq, Repr

/-- What the state is computed from. `last`, `lastSynced` and `records`
survive reopening; `passing`, `moving` and `transferring` are this
process's. -/
structure Facts where
  storage : Storage
  last : LastPass
  passing : Bool
  moving : Bool
  transferring : Bool
  unsent : Bool
  records : List Reason
  /-- The time of the last pass that reached the read end. -/
  lastSynced : Option Nat
  deriving DecidableEq, Repr

/-- What the last pass says, with no slot or file moving: a wait counts as
synced, since the pass reached storage. -/
def Facts.ended (f : Facts) : State :=
  match f.last with
  | .synced => .synced
  | .failed p =>
      match p.state with
      | some .offline => if f.unsent then .offlineUnsent else .offline
      | some s => s
      | none => .synced

/-- The loop's own candidate: syncing while a slot moves, or a file while the
last pass was synced; else how the last pass ended. -/
def Facts.loop (f : Facts) : State :=
  if f.moving then .syncing
  else if f.transferring ∧ f.ended = .synced then .syncing
  else f.ended

/-- The candidates other than the loop's: signed out while no credentials are
held, and each pending record's state. -/
def Facts.others (f : Facts) : List State :=
  (if f.storage = .signedOut then [.signedOut] else []) ++ f.records.filterMap Reason.state

/-- Every candidate state. -/
def Facts.candidates (f : Facts) : List State := f.loop :: f.others

/-- The higher-ranked of two states. -/
def pick (a b : State) : State := if b.rank ≤ a.rank then a else b

/-- The highest-ranked of `first` and `rest`. -/
def top (first : State) (rest : List State) : State := rest.foldl pick first

/-- The state shown: not synced with no location recorded, else the
highest-ranked candidate. -/
def state (f : Facts) : State :=
  if f.storage = .none then .notSynced else top f.loop f.others

/-- What the state reads; `passing` and `lastSynced` it doesn't. -/
def Facts.view (f : Facts) :=
  (f.storage, f.last, f.moving, f.transferring, f.unsent, f.records)

theorem state_view {f g : Facts} (h : f.view = g.view) : state f = state g := by
  cases f; cases g
  simp only [Facts.view, Prod.mk.injEq] at h
  obtain ⟨h1, h2, h3, h4, h5, h6⟩ := h
  subst h1 h2 h3 h4 h5 h6
  rfl

/-! ## What each source can name -/

/-- The states that name a cause: everything but not synced, syncing,
synced and the two offline states. -/
def State.named : State → Bool
  | .signedOut | .noAccess | .storageFull | .folderMissing | .updateNeeded
  | .removed | .needsReset | .clockWrong | .failed => true
  | _ => false

theorem reason_named (r : Reason) : ∀ s, r.state = some s → s.named = true := by
  induction r with
  | storage g _ =>
      intro s h
      cases g <;> simp [Reason.state, Failure.named, Failure.byItself] at h <;> (subst h; rfl)
  | failed l =>
      intro s h
      cases l <;> simp [Reason.state] at h <;> (subst h; rfl)
  | cleanup _ c _ ihc =>
      intro s h
      exact ihc s (by simpa [Reason.state] using h)
  | _ =>
      intro s h
      simp [Reason.state] at h <;> (subst h; rfl)

theorem pass_named (p : PassFailure) (s : State) (h : p.state = some s) :
    s.named = true ∨ s = .offline := by
  cases p with
  | offline => simp [PassFailure.state] at h; exact .inr h.symm
  | storage g =>
      left
      cases g <;> simp [PassFailure.state, Failure.named] at h <;> (subst h; rfl)
  | operation r =>
      left
      simp only [PassFailure.state, Option.some.injEq] at h
      cases hr : r.state with
      | none => rw [hr] at h; subst h; rfl
      | some x => rw [hr] at h; subst h; exact reason_named r x hr
  | blocked r => exact .inl (reason_named r s h)
  | _ => left; simp [PassFailure.state] at h; subst h; rfl

theorem ended_kind (f : Facts) :
    f.ended = .synced ∨ f.ended = .offline ∨ f.ended = .offlineUnsent ∨ f.ended.named = true := by
  unfold Facts.ended
  split
  · exact .inl rfl
  · rename_i p _
    split
    · split <;> simp
    · rename_i s ne hp
      rcases pass_named p s hp with n | o
      · exact .inr (.inr (.inr n))
      · exact absurd o ne
    · exact .inl rfl

theorem loop_cases (f : Facts) : f.loop = .syncing ∨ f.loop = f.ended := by
  unfold Facts.loop
  split
  · exact .inl rfl
  · split
    · exact .inl rfl
    · exact .inr rfl

theorem others_named (f : Facts) (s : State) (h : s ∈ f.others) : s.named = true := by
  simp only [Facts.others, List.mem_append, List.mem_filterMap] at h
  rcases h with signed | ⟨r, _, named⟩
  · split at signed
    · simp at signed; subst signed; rfl
    · simp at signed
  · exact reason_named r s named

/-! ## Exactly one state -/

theorem pick_mem (a b : State) : pick a b = a ∨ pick a b = b := by
  unfold pick; split <;> simp

theorem pick_ge (a b : State) : a.rank ≤ (pick a b).rank ∧ b.rank ≤ (pick a b).rank := by
  unfold pick; split <;> constructor <;> omega

theorem top_mem (first : State) (rest : List State) :
    top first rest = first ∨ top first rest ∈ rest := by
  induction rest generalizing first with
  | nil => simp [top]
  | cons x xs ih =>
      simp only [top, List.foldl_cons] at ih ⊢
      rcases ih (pick first x) with h | h
      · rcases pick_mem first x with p | p
        · left; rw [h, p]
        · right; rw [h, p]; simp
      · right; exact List.mem_cons_of_mem _ h

theorem top_ge (first : State) (rest : List State) :
    first.rank ≤ (top first rest).rank ∧ ∀ s ∈ rest, s.rank ≤ (top first rest).rank := by
  induction rest generalizing first with
  | nil => simp [top]
  | cons x xs ih =>
      simp only [top, List.foldl_cons] at ih ⊢
      have ⟨hfirst, hrest⟩ := ih (pick first x)
      have ⟨pa, pb⟩ := pick_ge first x
      refine ⟨by omega, ?_⟩
      intro s hs
      rcases List.mem_cons.mp hs with eq | m
      · subst eq; omega
      · exact hrest s m

/-- The state shown is a candidate that no candidate outranks, and it is the
only such one: with a location recorded, two states that both qualify are
equal, since no two states share a rank. -/
theorem one_state (f : Facts) (recorded : f.storage ≠ .none) :
    state f ∈ f.candidates ∧ (∀ c ∈ f.candidates, c.rank ≤ (state f).rank) ∧
      ∀ s, s ∈ f.candidates → (∀ c ∈ f.candidates, c.rank ≤ s.rank) → s = state f := by
  have eq : state f = top f.loop f.others := by simp [state, recorded]
  have shown : state f ∈ f.candidates ∧ ∀ c ∈ f.candidates, c.rank ≤ (state f).rank := by
    rw [eq]
    refine ⟨?_, ?_⟩
    · rcases top_mem f.loop f.others with e | e
      · rw [e]; simp [Facts.candidates]
      · exact List.mem_cons_of_mem _ e
    · intro x hx
      have ⟨hc, hcs⟩ := top_ge f.loop f.others
      rcases List.mem_cons.mp hx with e | m
      · rw [e]; exact hc
      · exact hcs x m
  refine ⟨shown.1, shown.2, ?_⟩
  intro s mem highest
  apply rank_injective
  have := highest _ shown.1
  have := shown.2 _ mem
  omega

/-- A candidate that isn't named is the loop's. -/
theorem unnamed_is_loop (f : Facts) (recorded : f.storage ≠ .none)
    (unnamed : (state f).named = false) : f.loop = state f := by
  have ⟨mem, _, _⟩ := one_state f recorded
  rcases List.mem_cons.mp mem with e | m
  · exact e.symm
  · have := others_named f _ m; simp_all

/-! ## Not synced and signed out -/

/-- Not synced is shown exactly when no location is recorded. -/
theorem not_synced_iff (f : Facts) : state f = .notSynced ↔ f.storage = .none := by
  constructor
  · intro h
    apply Classical.byContradiction
    intro recorded
    have loop := unnamed_is_loop f recorded (by rw [h]; rfl)
    rw [h] at loop
    rcases loop_cases f with s | e
    · rw [s] at loop; cases loop
    · rw [e] at loop
      rcases ended_kind f with k | k | k | k <;> rw [loop] at k <;> cases k
  · intro h; simp [state, h]

/-- A recorded location whose credentials this device doesn't hold is never
not synced: it is signed out, unless the device's own standing (removed,
needs reset, update needed) outranks it. -/
theorem signed_out_without_credentials (f : Facts) (h : f.storage = .signedOut) :
    state f ≠ .notSynced ∧ State.signedOut.rank ≤ (state f).rank := by
  have recorded : f.storage ≠ .none := by rw [h]; simp
  refine ⟨fun n => recorded ((not_synced_iff f).mp n), ?_⟩
  have ⟨_, highest, _⟩ := one_state f recorded
  exact highest _ (by simp [Facts.candidates, Facts.others, h])

/-! ## Offline with unsent changes -/

theorem ended_offline (f : Facts) :
    (f.ended = .offline → f.unsent = false) ∧ (f.ended = .offlineUnsent → f.unsent = true) := by
  unfold Facts.ended
  split
  · simp
  · rename_i p _
    split
    · cases f.unsent <;> simp
    · rename_i s ne hp
      rcases pass_named p s hp with n | o
      · constructor <;> intro e <;> subst e <;> simp [State.named] at n
      · exact absurd o ne
    · simp

/-- When the state is offline or offline with unsent changes, which of the
two it is says whether the send queue holds a write. -/
theorem offline_unsent_iff (f : Facts) (recorded : f.storage ≠ .none)
    (offline : state f = .offline ∨ state f = .offlineUnsent) :
    state f = .offlineUnsent ↔ f.unsent = true := by
  have loop := unnamed_is_loop f recorded (by rcases offline with e | e <;> rw [e] <;> rfl)
  have ended : f.ended = state f := by
    rcases loop_cases f with s | e
    · rw [s] at loop; rcases offline with o | o <;> rw [o] at loop <;> cases loop
    · rw [← e]; exact loop
  have ⟨off, unsent⟩ := ended_offline f
  rcases offline with o | o
  · rw [o] at ended ⊢; simp [off ended]
  · rw [o] at ended ⊢; simp [unsent ended]

theorem offline_iff (f : Facts) (recorded : f.storage ≠ .none)
    (offline : state f = .offline ∨ state f = .offlineUnsent) :
    state f = .offline ↔ f.unsent = false := by
  have := offline_unsent_iff f recorded offline
  rcases offline with e | e <;> simp_all

/-! ## Syncing only while something moves -/

theorem syncing_only_moving (f : Facts) (h : state f = .syncing) :
    f.moving = true ∨ f.transferring = true := by
  have recorded : f.storage ≠ .none := by
    intro none; rw [(not_synced_iff f).mpr none] at h; cases h
  have loop := unnamed_is_loop f recorded (by rw [h]; rfl)
  rw [h] at loop
  revert loop
  unfold Facts.loop
  cases f.moving <;> cases f.transferring <;> simp
  all_goals intro e; rcases ended_kind f with k | k | k | k <;> rw [e] at k <;> cases k

/-! ## The loop's steps -/

/-- What changes the facts. -/
inductive Event
  /-- A pass begins: it has moved nothing yet. -/
  | begin
  /-- The running pass read a slot it hadn't, or claimed one. -/
  | move
  /-- The running pass ends, at the store clock's `now`. -/
  | finish (last : LastPass) (now : Nat)
  /-- A file starts or stops moving. -/
  | transfer (moving : Bool)
  /-- A write commits to the send queue, or the queue empties. -/
  | queue (unsent : Bool)
  /-- The pending records change. -/
  | pending (records : List Reason)
  /-- Opening, setup or forgetting records whether storage is set up here. -/
  | storage (s : Storage)
  /-- The process ends and the store opens again. -/
  | reopen
  deriving DecidableEq, Repr

/-- The later of a kept time and a new one. -/
def later : Option Nat → Nat → Nat
  | none, now => now
  | some kept, now => max kept now

def step (f : Facts) : Event → Option Facts
  | .begin => if f.passing then none else some { f with passing := true, moving := false }
  | .move => if f.passing then some { f with moving := true } else none
  | .finish last now =>
      if f.passing then
        some { f with passing := false, moving := false, last,
                      lastSynced := if last = .synced then some (later f.lastSynced now)
                                    else f.lastSynced }
      else none
  | .transfer m => some { f with transferring := m }
  | .queue u => some { f with unsent := u }
  | .pending rs => some { f with records := rs }
  | .storage s => some { f with storage := s }
  | .reopen => some { f with passing := false, moving := false, transferring := false }

def run (f : Facts) : List Event → Option Facts
  | [] => some f
  | e :: es => (step f e).bind fun next => run next es

/-- A pass that moves nothing changes nothing the state reads while it runs,
and leaves the state as it was when it ends as the last pass did. -/
theorem quiet_pass_keeps (f : Facts) (idle : f.passing = false) (still : f.moving = false)
    (now : Nat) :
    (step f .begin).map state = some (state f) ∧
      (run f [.begin, .finish f.last now]).map state = some (state f) := by
  constructor
  · simp only [step, idle, Bool.false_eq_true, ite_false, Option.map_some, Option.some.injEq]
    exact state_view (by simp [Facts.view, still])
  · simp only [run, step, idle, Bool.false_eq_true, ite_false, ite_true, Option.bind_some,
      Option.map_some, Option.some.injEq]
    exact state_view (by simp [Facts.view, still])

/-- The time of the last finished sync never goes backward, whatever the
clock says. -/
theorem last_synced_never_back (f g : Facts) (e : Event) (h : step f e = some g) (t : Nat)
    (kept : f.lastSynced = some t) : ∃ u, g.lastSynced = some u ∧ t ≤ u := by
  cases e with
  | finish last now =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h
        split
        · exact ⟨_, rfl, by simp [kept, later]; omega⟩
        · exact ⟨t, kept, Nat.le_refl t⟩
      · simp at h
  | begin =>
      simp only [step] at h
      split at h
      · simp at h
      · simp only [Option.some.injEq] at h; subst h; exact ⟨t, kept, Nat.le_refl t⟩
  | move =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h; exact ⟨t, kept, Nat.le_refl t⟩
      · simp at h
  | _ =>
      simp only [step, Option.some.injEq] at h; subst h; exact ⟨t, kept, Nat.le_refl t⟩

/-- Only a pass reaching the read end changes that time: every other event,
a reopen and every change of state included, keeps it. -/
theorem last_synced_kept (f g : Facts) (e : Event) (h : step f e = some g)
    (other : ∀ now, e ≠ .finish .synced now) : g.lastSynced = f.lastSynced := by
  cases e with
  | finish last now =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h
        have : last ≠ .synced := fun eq => other now (by rw [eq])
        simp [this]
      · simp at h
  | begin =>
      simp only [step] at h
      split at h
      · simp at h
      · simp only [Option.some.injEq] at h; subst h; rfl
  | move =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h; rfl
      · simp at h
  | _ => simp only [step, Option.some.injEq] at h; subst h; rfl

theorem ended_queued (f : Facts) (h : f.ended = .offline) :
    ({ f with unsent := true } : Facts).ended = .offlineUnsent := by
  revert h
  unfold Facts.ended
  simp only
  split
  · simp
  · split
    · simp
    · rename_i s ne _; intro e; exact absurd e ne
    · simp

/-- A write committed while offline turns offline into offline with unsent
changes in that same step: no pass runs for it. -/
theorem write_while_offline (f : Facts) (recorded : f.storage ≠ .none)
    (offline : state f = .offline) :
    (step f (.queue true)).map state = some .offlineUnsent := by
  have loop := unnamed_is_loop f recorded (by rw [offline]; rfl)
  rw [offline] at loop
  have ended : f.ended = .offline := by
    rcases loop_cases f with s | e
    · rw [s] at loop; cases loop
    · rw [← e]; exact loop
  have still : f.moving = false := by
    cases m : f.moving
    · rfl
    · simp [Facts.loop, m] at loop
  let g : Facts := { f with unsent := true }
  have gloop : g.loop = .offlineUnsent := by
    have ge : g.ended = .offlineUnsent := ended_queued f ended
    have gm : g.moving = false := still
    unfold Facts.loop
    rw [gm, ge]
    simp
  have ⟨_, highest, _⟩ := one_state f recorded
  have grecorded : g.storage ≠ .none := recorded
  have ⟨_, _, only⟩ := one_state g grecorded
  simp only [step, Option.map_some, Option.some.injEq]
  apply (only _ (by simp [Facts.candidates, gloop]) _).symm
  intro c hc
  rcases List.mem_cons.mp hc with e | m
  · rw [e, gloop]; exact Nat.le_refl _
  · have := highest c (List.mem_cons_of_mem _ m)
    rw [offline] at this
    simp only [State.rank] at this ⊢
    omega

/-! ## Each pending reason -/

/-- A wait names no state: on another device's object, key or registration,
on catching up, on a schema raise, a reload or a snapshot, on the provider,
on another member, on the app or the person. -/
theorem waits_name_no_state :
    Reason.missing.state = none ∧ Reason.waits.state = none ∧
      Reason.keyUnavailable.state = none ∧ Reason.noStorage.state = none ∧
      Reason.providerPending.state = none ∧ Reason.accountInUse.state = none ∧
      Reason.originalMissing.state = none ∧ Reason.paused.state = none := by
  simp [Reason.state]

/-- A provider's cooldown and a slot waiting on another device's key leave the
last pass synced: neither names a state. -/
theorem waiting_pass_synced (f : Facts) (r : Reason) (wait : r.state = none) :
    ({ f with last := .failed (.blocked r) } : Facts).ended = .synced ∧
      ({ f with last := .failed (.storage .rateLimited) } : Facts).ended = .synced := by
  simp [Facts.ended, PassFailure.state, wait, Failure.named]

namespace Examples

def connected : Facts :=
  ⟨.connected, .synced, false, false, false, false, [], some 100⟩

/-- Ana's laptop is synced; a pass at 130 finds nothing: synced all along,
last synced at 130. -/
example : (run connected [.begin, .finish .synced 130]).map (fun f => (state f, f.lastSynced)) =
    some (.synced, some 130) := by decide

/-- Ben's phone loses its network, then he edits a note: offline, then
offline with unsent changes; the last finished sync stays at 100. -/
example : (run connected [.begin, .finish (.failed .offline) 140, .queue true]).map
    (fun f => (state f, f.lastSynced)) = some (.offlineUnsent, some 100) := by decide

/-- The network comes back: the pass reads Ana's slot and sends Ben's write,
showing syncing while it does, then synced. -/
example : (run connected [.begin, .finish (.failed .offline) 140, .queue true, .begin, .move]).map
    state = some .syncing := by decide

/-- Carol's upload meets a full Dropbox while the pass syncs: storage full. -/
example : (run connected [.pending [.storage .quotaExceeded true]]).map state =
    some .storageFull := by decide

/-- Dan's sign-in is gone from the keychain: signed out, last synced kept. -/
example : (run connected [.storage .signedOut, .reopen]).map (fun f => (state f, f.lastSynced)) =
    some (.signedOut, some 100) := by decide

/-- Dan's device was removed while it was signed out: removed outranks. -/
example : (run connected [.storage .signedOut, .pending [.removed]]).map state =
    some .removed := by decide

/-- Ana's pending invite waits for Dan's request: no state of its own. -/
example : (run connected [.pending [.missing, .paused]]).map state = some .synced := by decide

end Examples

end CovenIO.SyncState
