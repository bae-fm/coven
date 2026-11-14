/-! The sync state a status screen shows (§19.1, api.md E5 `SyncStatus`), as
`sync_status()` computes it from `coven_sync_status`'s row, the send queue,
the upload queue and the pending records.

The state is one flat list, in the order a host lists them: not synced,
syncing, waiting to sync, synced, offline, offline with unsent changes,
signed out, no access to storage, storage full, storage folder missing,
update needed, removed from the store, device needs reset, clock wrong, and
failed with no named cause. Coven names no words; the host picks them.

Sync is the sequence and the bound files: writes in and out, and the files a
write waits for (§17.4). Synced means nothing of that waits: the last pass
reached the end of the sequence, no write waits in the send queue, and no
bound file of this device's waits to upload. A write queued with nothing
moving shows waiting to sync, for the next pass, which shows syncing as it
claims the write's slot. A bound file uploading shows syncing; one queued with
nothing moving shows waiting to sync, with what it waits for (`SyncWait`); one
whose source failed shows failed, naming the file. A trailing file, whose
write goes out first and whose bytes follow, and a download, never reach the
state: what a trailing file waits for is its own phase (`Transfers`).

The state is computed, never stored, from facts that are each true on their
own: whether a location is recorded and this device holds credentials for it
(`Storage`), how the last pass that reached storage ended (`LastPass`),
whether a running pass has moved a slot (`moving`), each upload in the queue
(`uploads`, of which the bound ones count), whether the send queue holds a
write (`unsent`), and the pending records the state reads. Each fact is a
candidate state or none; when several hold, the state is the one of highest
rank (`State.rank`). So a write that commits while offline turns Offline into
Offline with unsent changes in the same transaction, with no step of the
loop's.

Syncing shows only while something moves: a pass that has read or claimed a
slot, or a bound file in flight. A pass that finds nothing writes nothing the
state reads, so the state doesn't change while it runs, and ends as it was
when it ends as the last one did (`quiet_pass_keeps`).

A wait names no failure: a pass held on a key, a snapshot or another
device's object, a provider's cooldown, a pending invite. Such a pass didn't
get everything in or out, so it shows waiting to sync, naming what holds it
(`Facts.passWait`), and only a pass that ended synced can show synced
(`failed_pass_not_synced`). Nor does it move the time of the last finished
sync, which only a pass reaching the read end sets. That time is kept across
every state and every reopen, and never goes backward, even when the clock
does (`last_synced_never_back`). A wait for a bound upload names waiting to
sync: the file is not in storage yet.

Proved here: the state is the one candidate no other outranks (`one_state`);
no storage recorded is not synced, and only that (`not_synced_iff`), while a
recorded location without credentials is signed out unless the device's own
standing outranks it (`signed_out_without_credentials`); replacing the
trailing uploads with any others never changes the state
(`trailing_keeps_state`); offline with unsent changes holds exactly when the
state is offline and a write or a bound upload is queued
(`offline_unsent_iff`, `offline_iff`); syncing only while a slot or a bound
file moves (`syncing_only_moving`); waiting to sync only while nothing moves,
with a wait to name (`waiting_not_moving`); synced only with every bound file
of this device's in storage (`synced_bound_stored`) and no write queued
(`synced_writes_sent`); a file whose source failed shows failed or what
outranks it (`bad_file_fails`); a quiet pass keeps the state
(`quiet_pass_keeps`); the time of the last finished sync never goes
backward and only a pass reaching the read end changes it
(`last_synced_never_back`, `last_synced_kept`); a pass held back by a wait
never shows synced (`failed_pass_not_synced`). Each pending reason maps to
one state or none by `Reason.state`, a function; waits map to none
(`waits_name_no_state`), and a file that can't be uploaded maps to failed
(`file_failures_name_failed`). -/

namespace CovenIO.SyncState

/-- What a status screen shows, in the order a host lists them. -/
inductive State
  | notSynced
  | syncing
  | waitingToSync
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
failure, then activity: moving, then waiting. -/
def State.rank : State → Nat
  | .synced => 0
  | .waitingToSync => 1
  | .syncing => 2
  | .failed => 3
  | .offline => 4
  | .offlineUnsent => 5
  | .storageFull => 6
  | .clockWrong => 7
  | .folderMissing => 8
  | .noAccess => 9
  | .signedOut => 10
  | .updateNeeded => 11
  | .needsReset => 12
  | .removed => 13
  | .notSynced => 14

theorem rank_injective (a b : State) (h : a.rank = b.rank) : a = b := by
  cases a <;> cases b <;> simp_all [State.rank]

/-- A provider failure, as `StorageFailure` names it. -/
inductive Failure
  | providerUnavailable | accessNotRegistered | accessNotGranted
  | dropboxAppFolderNotShareable | catchUpRequired | network | corrupted
  | authentication
  | permissionDenied | notFound | alreadyExists | conflict | containerNotFound
  | regionMismatch | storeExists | clockSkew | ownLocationRequired | quotaExceeded
  | rateLimited | invalidConfiguration | refused | protocol | invalidPath
  | invalidRange | notStoreOwner | inviteeIsStoreOwner | sharingRefused | accountIdUnavailable
  | invitationMismatch | sessionMismatch | sessionExpired
  | invalidPart | singleRequestTooLarge | copyTooLarge | encoding | file
  | memberKeysMissing | bug
  deriving DecidableEq, Repr

/-- The state a provider failure names: what storage said of this account or
location that a person must see, or coven's own bug, which shows failed
while its work retries. -/
def Failure.named : Failure → Option State
  | .authentication => some .signedOut
  | .permissionDenied | .accessNotGranted => some .noAccess
  | .quotaExceeded => some .storageFull
  | .containerNotFound => some .folderMissing
  | .clockSkew => some .clockWrong
  | .bug => some .failed
  | _ => none

/-- The failures that clear by themselves (`Clears::ByItself`). -/
def Failure.byItself : Failure → Bool
  | .network | .corrupted | .conflict | .rateLimited | .notFound | .sessionExpired
  | .catchUpRequired | .quotaExceeded | .file | .bug => true
  | _ => false

/-- A local failure, as `LocalFailure` names it. -/
inductive Local
  | database | diskFull | diskDenied | diskOther | keyCustody | crypto | operation | shutdown
  | bug
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
is reachable. A file that can't be uploaded or read names failed: its
original missing or changed, coven's copy failing its check, its stored
bytes lost or refused. It holds back what waits for it and is never skipped,
so it shows until the file uploads or the person acts. Any other failure
names the state its cause does, or failed. A cleanup names what its cleanup
waits on, as it retries that. -/
def Reason.state : Reason → Option State
  | .storage f _ =>
      match f.named with
      | some s => some s
      | none => if f.byItself then none else some .failed
  | .updateRequired => some .updateNeeded
  | .removed => some .removed
  | .memberKeysReplaced => some .needsReset
  | .refused | .fileLost | .fileSource | .originalMissing | .originalChanged => some .failed
  | .failed l => if l = .shutdown then none else some .failed
  | .cleanup _ c => c.state
  | .missing | .waits | .keyUnavailable | .noStorage | .writesRefused | .providerPending
  | .accountInUse | .accessRemains | .rejected | .paused => none

/-- The reasons that clear by themselves (`PendingReason::clears`): a provider
failure of that class, a local failure that retries, or a cleanup of one. -/
def Reason.byItself : Reason → Bool
  | .storage f _ => f.byItself
  | .failed l =>
      match l with
      | .database | .diskFull | .diskDenied | .diskOther | .keyCustody | .shutdown | .bug => true
      | .crypto | .operation => false
  | .cleanup _ c => c.byItself
  | _ => false

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
cooldown or a slot waiting on another device's key, and for a failure that
clears by itself and names no state of its own, on the pass, on coven's own
operation or on a slot's application alike (`one_retry`). -/
def PassFailure.state : PassFailure → Option State
  | .offline => some .offline
  | .storage f =>
      match f.named with
      | some s => some s
      | none => if f.byItself then none else some .failed
  | .updateRequired => some .updateNeeded
  | .removed => some .removed
  | .deviceIdInUse => some .needsReset
  | .operation r =>
      match r.state with
      | some s => some s
      | none => if r.byItself then none else some .failed
  | .blocked r => r.state
  | .other => some .failed

/-- The failures that retry under the sync scope's backoff: those that clear by
themselves and name no state, whichever way the pass met them. -/
def PassFailure.retries : PassFailure → Bool
  | .storage f => f.named.isNone && f.byItself
  | .operation r => r.state.isNone && r.byItself
  | .blocked r => r.state.isNone && r.byItself
  | _ => false

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

/-- Where one upload in this device's queue stands (§17.9). -/
inductive Progress
  /-- An attempt is sending it now. -/
  | moving
  /-- Storage holds its bytes; it waits for the rest of its move. -/
  | stored
  /-- Queued, and no attempt is sending it. -/
  | queued
  deriving DecidableEq, Repr

/-- One upload in this device's queue. -/
structure Upload where
  progress : Progress
  /-- A bound reference's, which the pause doesn't hold (§17.4). -/
  bound : Bool
  /-- Its last attempt failed, and it waits out its backoff (§6.7). -/
  retrying : Bool
  /-- Its pending record's reason, if it has one. -/
  record : Option Reason
  deriving DecidableEq, Repr

/-- What waiting to sync waits for: what holds every transfer, the pass's,
each bound upload's and a queued write's alike, then what holds one bound
upload. -/
inductive SyncWait
  /-- Sync isn't running here. -/
  | notSyncing
  /-- The provider asked every request to wait (§6.7). -/
  | coolingDown
  /-- A slot can't apply yet: nothing goes out or comes in until it does
  (§5.4). -/
  | blocked
  /-- Its last attempt failed; it retries after its backoff. -/
  | retrying
  /-- Its turn hasn't come: an upload's, a write's, or the next pass's once a
  cooldown that held the last one is over. -/
  | queued
  deriving DecidableEq, Repr

/-- What the state is computed from. `last`, `lastSynced`, `records`, the
queues and the settings survive reopening; `passing`, `moving` and an
upload's moving are this process's, and so is `metered`, which the process
reads from the platform as the store opens and whenever the network changes.
The state reads the bound uploads and not the pause, the network or the
self-replacement pause, which only a trailing upload waits for (`Transfers`). -/
structure Facts where
  storage : Storage
  last : LastPass
  passing : Bool
  moving : Bool
  /-- This device's uploads, bound and trailing. -/
  uploads : List Upload
  unsent : Bool
  records : List Reason
  /-- A started connection will plan (`coven_retry_effect.syncing`). -/
  running : Bool
  /-- A provider cooldown holds every request. -/
  cooling : Bool
  /-- The app paused trailing uploads. -/
  paused : Bool
  /-- The setting to transfer only on unmetered networks is on. -/
  unmeteredOnly : Bool
  /-- The platform says the device's network is metered, or failed to
  answer after the store opened, so it may be. -/
  metered : Bool
  /-- A self-replacement of this member's keys pauses writes (§12.3). -/
  keysChanging : Bool
  /-- The sync scope's backoff runs: the last pass failed and retries then
  (`_coven_retries`, scope `sync`). -/
  passRetrying : Bool
  /-- The time of the last pass that reached the read end. -/
  lastSynced : Option Nat
  deriving DecidableEq, Repr

/-- The uploads the sync state reads: a bound upload holds the writes behind
it, so sync waits for it (§17.4). -/
def Facts.bound (f : Facts) : List Upload := f.uploads.filter (·.bound)

/-- A write or a bound upload with bytes to send waits to go out. -/
def Facts.unsentWork (f : Facts) : Bool :=
  f.unsent || f.bound.any (fun u => u.progress != .stored)

/-- What the last pass says, with no slot moving: a pass held back by a wait
waits to sync, since it didn't get everything in or out. -/
def Facts.ended (f : Facts) : State :=
  match f.last with
  | .synced => .synced
  | .failed p =>
      match p.state with
      | some .offline => if f.unsentWork then .offlineUnsent else .offline
      | some s => s
      | none => .waitingToSync

/-- The loop's own candidate: syncing while a slot moves, else how the last
pass ended. -/
def Facts.loop (f : Facts) : State :=
  if f.moving then .syncing else f.ended

/-- What an upload names: syncing while it moves; once queued, what its
record names, else waiting to sync; nothing once stored. -/
def Upload.candidate (u : Upload) : Option State :=
  match u.progress with
  | .moving => some .syncing
  | .stored => none
  | .queued => some ((u.record.bind Reason.state).getD .waitingToSync)

/-- The files' candidates: each bound upload's. -/
def Facts.files (f : Facts) : List State := f.bound.filterMap Upload.candidate

/-- The send queue's candidate: a write in it waits to sync. It goes out with
the next pass, which shows syncing once it claims the write's slot. -/
def Facts.writes (f : Facts) : List State := if f.unsent then [.waitingToSync] else []

/-- The other candidates: signed out while no credentials are held, and each
pending record's state. -/
def Facts.others (f : Facts) : List State :=
  (if f.storage = .signedOut then [.signedOut] else []) ++ f.records.filterMap Reason.state

/-- Every candidate but the loop's. -/
def Facts.rest (f : Facts) : List State := f.files ++ f.writes ++ f.others

/-- Every candidate state. -/
def Facts.candidates (f : Facts) : List State := f.loop :: f.rest

/-- The higher-ranked of two states. -/
def pick (a b : State) : State := if b.rank ≤ a.rank then a else b

/-- The highest-ranked of `first` and `rest`. -/
def top (first : State) (rest : List State) : State := rest.foldl pick first

/-- The state shown: not synced with no location recorded, else the
highest-ranked candidate. -/
def state (f : Facts) : State :=
  if f.storage = .none then .notSynced else top f.loop f.rest

/-- Whether the last pass is blocked at a slot that waits for something other
than a retry: a failure that clears by itself retries instead. -/
def Facts.blocked (f : Facts) : Bool :=
  match f.last with
  | .failed (.blocked r) => !(r.state.isNone && r.byItself)
  | _ => false

/-- Whether the last pass ended on a failure that retries under the sync
scope's backoff. -/
def Facts.lastRetries (f : Facts) : Bool :=
  match f.last with
  | .failed p => p.retries
  | _ => false

/-- What holds every transfer, the pass's, each upload's and a queued
write's alike: sync stopped, a provider's cooldown, or a slot that can't
apply yet. -/
def Facts.heldAll (f : Facts) : Option SyncWait :=
  if !f.running then some .notSyncing
  else if f.cooling then some .coolingDown
  else if f.blocked then some .blocked
  else none

/-- What the next pass waits for: what holds every transfer, else the backoff
of a failure that clears by itself, else its turn, once a cooldown that held
the last one is over. A pass held back by a wait waits for it, and so does a
queued write, which goes out with that pass. -/
def Facts.passWait (f : Facts) : SyncWait :=
  match f.heldAll with
  | some w => w
  | none => if f.lastRetries && f.passRetrying then .retrying else .queued

/-- What one queued bound upload waits for: what holds every transfer first,
then its backoff. -/
def Facts.uploadWait (f : Facts) (u : Upload) : SyncWait :=
  match f.heldAll with
  | some w => w
  | none => if u.retrying then .retrying else .queued

/-- What waits to sync, in order: the held-back pass, then each bound upload
waiting to sync, in queue order, then a queued write; waiting to sync names
the first. -/
def Facts.waits (f : Facts) : List SyncWait :=
  (if f.loop = .waitingToSync then [f.passWait] else []) ++
    f.bound.filterMap (fun u =>
      if u.candidate = some .waitingToSync then some (f.uploadWait u) else none) ++
    (if f.unsent then [f.passWait] else [])

/-- What the state reads; `passing`, the trailing uploads, the settings, the
network and `lastSynced` it doesn't. -/
def Facts.view (f : Facts) :=
  (f.storage, f.last, f.moving, f.bound, f.unsent, f.records)

theorem state_view {f g : Facts} (h : f.view = g.view) : state f = state g := by
  simp only [Facts.view, Prod.mk.injEq] at h
  obtain ⟨h1, h2, h3, h4, h5, h6⟩ := h
  have hu : f.unsentWork = g.unsentWork := by simp [Facts.unsentWork, h4, h5]
  have he : f.ended = g.ended := by simp [Facts.ended, hu, h2]
  have hl : f.loop = g.loop := by simp [Facts.loop, he, h3]
  have hr : f.rest = g.rest := by
    simp [Facts.rest, Facts.files, Facts.writes, Facts.others, h1, h4, h5, h6]
  simp [state, hl, hr, h1]

/-! ## What each source can name -/

/-- The states that name a cause: everything but not synced, syncing,
waiting to sync, synced and the two offline states. -/
def State.named : State → Bool
  | .signedOut | .noAccess | .storageFull | .folderMissing | .updateNeeded
  | .removed | .needsReset | .clockWrong | .failed => true
  | _ => false

theorem named_rank (s : State) (h : s.named = true) : State.failed.rank ≤ s.rank := by
  cases s <;> simp [State.named, State.rank] at h ⊢

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
      cases g <;> simp [PassFailure.state, Failure.named, Failure.byItself] at h <;>
        (subst h; rfl)
  | operation r =>
      left
      simp only [PassFailure.state] at h
      cases hr : r.state with
      | none =>
          rw [hr] at h
          cases hb : r.byItself <;> simp [hb] at h
          subst h; rfl
      | some x =>
          rw [hr] at h
          simp only [Option.some.injEq] at h
          subst h; exact reason_named r x hr
  | blocked r => exact .inl (reason_named r s h)
  | _ => left; simp [PassFailure.state] at h; subst h; rfl

theorem ended_kind (f : Facts) :
    f.ended = .synced ∨ f.ended = .offline ∨ f.ended = .offlineUnsent ∨
      f.ended = .waitingToSync ∨ f.ended.named = true := by
  unfold Facts.ended
  split
  · exact .inl rfl
  · rename_i p _
    split
    · split <;> simp
    · rename_i s ne hp
      rcases pass_named p s hp with n | o
      · exact .inr (.inr (.inr (.inr n)))
      · exact absurd o ne
    · exact .inr (.inr (.inr (.inl rfl)))

/-- A pass that didn't end synced never ends synced: it failed, or a wait
held it back. -/
theorem failed_ended (f : Facts) (p : PassFailure) (h : f.last = .failed p) :
    f.ended ≠ .synced := by
  unfold Facts.ended
  rw [h]
  simp only
  split
  · split <;> simp
  · rename_i s ne hp
    rcases pass_named p s hp with n | o
    · intro e; subst e; simp [State.named] at n
    · exact absurd o ne
  · simp

theorem loop_cases (f : Facts) : f.loop = .syncing ∨ f.loop = f.ended := by
  unfold Facts.loop
  split
  · exact .inl rfl
  · exact .inr rfl

theorem candidate_kind (u : Upload) (s : State) (h : u.candidate = some s) :
    s = .syncing ∨ s = .waitingToSync ∨ s.named = true := by
  unfold Upload.candidate at h
  split at h
  · simp at h; exact .inl h.symm
  · simp at h
  · simp only [Option.some.injEq] at h
    cases hr : u.record.bind Reason.state with
    | none => rw [hr] at h; simp at h; exact .inr (.inl h.symm)
    | some x =>
        rw [hr] at h; simp at h; subst h
        cases hu : u.record with
        | none => rw [hu] at hr; simp at hr
        | some r => rw [hu] at hr; exact .inr (.inr (reason_named r x (by simpa using hr)))

theorem rest_kind (f : Facts) (s : State) (h : s ∈ f.rest) :
    s = .syncing ∨ s = .waitingToSync ∨ s.named = true := by
  simp only [Facts.rest, Facts.files, Facts.writes, Facts.others, List.mem_append,
    List.mem_filterMap] at h
  rcases h with (⟨u, _, hu⟩ | write) | (signed | ⟨r, _, named⟩)
  · exact candidate_kind u s hu
  · split at write
    · simp at write; exact .inr (.inl write)
    · simp at write
  · split at signed
    · simp at signed; subst signed; exact .inr (.inr rfl)
    · simp at signed
  · exact .inr (.inr (reason_named r s named))

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
  have eq : state f = top f.loop f.rest := by simp [state, recorded]
  have shown : state f ∈ f.candidates ∧ ∀ c ∈ f.candidates, c.rank ≤ (state f).rank := by
    rw [eq]
    refine ⟨?_, ?_⟩
    · rcases top_mem f.loop f.rest with e | e
      · rw [e]; simp [Facts.candidates]
      · exact List.mem_cons_of_mem _ e
    · intro x hx
      have ⟨hc, hcs⟩ := top_ge f.loop f.rest
      rcases List.mem_cons.mp hx with e | m
      · rw [e]; exact hc
      · exact hcs x m
  refine ⟨shown.1, shown.2, ?_⟩
  intro s mem highest
  apply rank_injective
  have := highest _ shown.1
  have := shown.2 _ mem
  omega

/-- A candidate that isn't named, syncing or waiting to sync is the loop's. -/
theorem loop_state (f : Facts) (recorded : f.storage ≠ .none)
    (unnamed : (state f).named = false) (moves : state f ≠ .syncing)
    (waits : state f ≠ .waitingToSync) : f.loop = state f := by
  have ⟨mem, _, _⟩ := one_state f recorded
  rcases List.mem_cons.mp mem with e | m
  · exact e.symm
  · rcases rest_kind f _ m with s | w | n
    · exact absurd s moves
    · exact absurd w waits
    · simp_all

/-! ## Not synced and signed out -/

/-- Not synced is shown exactly when no location is recorded. -/
theorem not_synced_iff (f : Facts) : state f = .notSynced ↔ f.storage = .none := by
  constructor
  · intro h
    apply Classical.byContradiction
    intro recorded
    have loop := loop_state f recorded (by rw [h]; rfl) (by rw [h]; simp) (by rw [h]; simp)
    rw [h] at loop
    rcases loop_cases f with s | e
    · rw [s] at loop; cases loop
    · rw [e] at loop
      rcases ended_kind f with k | k | k | k | k <;> rw [loop] at k <;> cases k
  · intro h; simp [state, h]

theorem recorded_of (f : Facts) (s : State) (h : state f = s) (ne : s ≠ .notSynced) :
    f.storage ≠ .none := by
  intro none; rw [(not_synced_iff f).mpr none] at h; exact ne h.symm

/-- A recorded location whose credentials this device doesn't hold is never
not synced: it is signed out, unless the device's own standing (removed,
needs reset, update needed) outranks it. -/
theorem signed_out_without_credentials (f : Facts) (h : f.storage = .signedOut) :
    state f ≠ .notSynced ∧ State.signedOut.rank ≤ (state f).rank := by
  have recorded : f.storage ≠ .none := by rw [h]; simp
  refine ⟨fun n => recorded ((not_synced_iff f).mp n), ?_⟩
  have ⟨_, highest, _⟩ := one_state f recorded
  exact highest _ (by simp [Facts.candidates, Facts.rest, Facts.others, h])

/-! ## Offline with unsent changes -/

theorem ended_offline (f : Facts) :
    (f.ended = .offline → f.unsentWork = false) ∧
      (f.ended = .offlineUnsent → f.unsentWork = true) := by
  unfold Facts.ended
  split
  · simp
  · rename_i p _
    split
    · cases f.unsentWork <;> simp
    · rename_i s ne hp
      rcases pass_named p s hp with n | o
      · constructor <;> intro e <;> subst e <;> simp [State.named] at n
      · exact absurd o ne
    · simp

/-- When the state is offline or offline with unsent changes, which of the
two it is says whether a write or an upload is queued. -/
theorem offline_unsent_iff (f : Facts) (recorded : f.storage ≠ .none)
    (offline : state f = .offline ∨ state f = .offlineUnsent) :
    state f = .offlineUnsent ↔ f.unsentWork = true := by
  have loop := loop_state f recorded (by rcases offline with e | e <;> rw [e] <;> rfl)
    (by rcases offline with e | e <;> rw [e] <;> simp)
    (by rcases offline with e | e <;> rw [e] <;> simp)
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
    state f = .offline ↔ f.unsentWork = false := by
  have := offline_unsent_iff f recorded offline
  rcases offline with e | e <;> simp_all

/-! ## Syncing, waiting and synced -/

theorem syncing_only_moving (f : Facts) (h : state f = .syncing) :
    f.moving = true ∨ ∃ u ∈ f.bound, u.progress = .moving := by
  have recorded := recorded_of f _ h (by simp)
  have ⟨mem, _, _⟩ := one_state f recorded
  rw [h] at mem
  rcases List.mem_cons.mp mem with e | m
  · rcases loop_cases f with s | l
    · revert s; unfold Facts.loop
      cases f.moving <;> simp
      intro e; rcases ended_kind f with k | k | k | k | k <;> rw [e] at k <;> cases k
    · rw [← e] at l; rcases ended_kind f with k | k | k | k | k <;> rw [← l] at k <;> cases k
  · simp only [Facts.rest, Facts.files, Facts.writes, Facts.others, List.mem_append,
      List.mem_filterMap] at m
    rcases m with (⟨u, mu, hu⟩ | write) | (signed | ⟨r, _, named⟩)
    · refine .inr ⟨u, mu, ?_⟩
      unfold Upload.candidate at hu
      split at hu
      · rename_i p; exact p
      · simp at hu
      · simp only [Option.some.injEq] at hu
        cases hr : u.record.bind Reason.state with
        | none => rw [hr] at hu; simp at hu
        | some x =>
            rw [hr] at hu; simp at hu; subst hu
            cases hu' : u.record with
            | none => rw [hu'] at hr; simp at hr
            | some r => rw [hu'] at hr; have := reason_named r _ (by simpa using hr); simp [State.named] at this
    · split at write <;> simp at write
    · split at signed <;> simp at signed
    · have := reason_named r _ named; simp [State.named] at this

/-- Waiting to sync shows only while no slot and no file moves, and it names
a wait: the held-back pass's, else the first waiting upload's, else a queued
write's. -/
theorem waiting_not_moving (f : Facts) (h : state f = .waitingToSync) :
    f.waits ≠ [] ∧ f.moving = false ∧ ∀ u ∈ f.bound, u.progress ≠ .moving := by
  have recorded := recorded_of f _ h (by simp)
  have ⟨mem, highest, _⟩ := one_state f recorded
  rw [h] at mem highest
  refine ⟨?_, ?_, ?_⟩
  · rcases List.mem_cons.mp mem with e | m
    · intro empty
      simp [Facts.waits, ← e] at empty
    · simp only [Facts.rest, Facts.files, Facts.writes, Facts.others, List.mem_append,
        List.mem_filterMap] at m
      rcases m with (⟨u, mu, hu⟩ | write) | (signed | ⟨r, _, named⟩)
      · intro empty
        have : f.uploadWait u ∈ f.waits := by
          simp only [Facts.waits, List.mem_append, List.mem_filterMap]
          exact .inl (.inr ⟨u, mu, by simp [hu]⟩)
        rw [empty] at this; simp at this
      · split at write
        · rename_i queued; intro empty; simp [Facts.waits, queued] at empty
        · simp at write
      · split at signed <;> simp at signed
      · have := reason_named r _ named; simp [State.named] at this
  · cases mv : f.moving
    · rfl
    · have := highest f.loop (List.mem_cons_self ..)
      simp [Facts.loop, mv, State.rank] at this
  · intro u mu mv
    have := highest .syncing (List.mem_cons_of_mem _ (by
      simp only [Facts.rest, Facts.files, List.mem_append, List.mem_filterMap]
      exact .inl (.inl ⟨u, mu, by simp [Upload.candidate, mv]⟩)))
    simp [State.rank] at this

/-- Synced shows only when every bound upload of this device's has its bytes
in storage: none is moving, queued or failed. -/
theorem synced_bound_stored (f : Facts) (h : state f = .synced) :
    ∀ u ∈ f.bound, u.progress = .stored := by
  have recorded := recorded_of f _ h (by simp)
  have ⟨_, highest, _⟩ := one_state f recorded
  rw [h] at highest
  intro u mu
  cases p : u.progress with
  | stored => rfl
  | moving =>
      have := highest .syncing (List.mem_cons_of_mem _ (by
        simp only [Facts.rest, Facts.files, List.mem_append, List.mem_filterMap]
        exact .inl (.inl ⟨u, mu, by simp [Upload.candidate, p]⟩)))
      simp [State.rank] at this
  | queued =>
      cases hc : u.candidate with
      | none => simp [Upload.candidate, p] at hc
      | some s =>
          have := highest s (List.mem_cons_of_mem _ (by
            simp only [Facts.rest, Facts.files, List.mem_append, List.mem_filterMap]
            exact .inl (.inl ⟨u, mu, hc⟩)))
          rcases candidate_kind u s hc with e | e | n
          · subst e; simp [State.rank] at this
          · subst e; simp [State.rank] at this
          · have := named_rank s n; simp [State.rank] at *; omega

/-- Synced shows only with the send queue empty: a queued write waits to sync
until a pass sends it, however the rest stands. -/
theorem synced_writes_sent (f : Facts) (h : state f = .synced) : f.unsent = false := by
  have recorded := recorded_of f _ h (by simp)
  have ⟨_, highest, _⟩ := one_state f recorded
  rw [h] at highest
  cases queued : f.unsent
  · rfl
  · have := highest .waitingToSync (List.mem_cons_of_mem _ (by
      simp only [Facts.rest, List.mem_append]
      exact .inl (.inr (by simp [Facts.writes, queued]))))
    simp [State.rank] at this

/-- A pending record naming failed, such as an upload whose original is
missing, keeps the state at failed or what outranks it. -/
theorem bad_file_fails (f : Facts) (recorded : f.storage ≠ .none) (r : Reason)
    (mem : r ∈ f.records) (bad : r.state = some .failed) :
    State.failed.rank ≤ (state f).rank := by
  have ⟨_, highest, _⟩ := one_state f recorded
  exact highest _ (List.mem_cons_of_mem _ (by
    simp only [Facts.rest, Facts.others, List.mem_append, List.mem_filterMap]
    exact .inr (.inr ⟨r, mem, bad⟩)))

/-! ## The loop's steps -/

/-- What changes the facts. -/
inductive Event
  /-- A pass begins: it has moved nothing yet. -/
  | begin
  /-- The running pass read a slot it hadn't, or claimed one. -/
  | move
  /-- The running pass ends, at the store clock's `now`. -/
  | finish (last : LastPass) (now : Nat)
  /-- The upload queue changes: a file queued, an attempt starting or ending,
  a file stored or retired. -/
  | uploads (us : List Upload)
  /-- A write commits to the send queue, or the queue empties. -/
  | queue (unsent : Bool)
  /-- The pending records change. -/
  | pending (records : List Reason)
  /-- Opening, setup or forgetting records whether storage is set up here;
  setup, joining or restoring also records when it finished, which counts
  as the last finished sync. -/
  | storage (s : Storage) (finished : Option Nat)
  /-- The sync loop, or the attempt to connect that starts it, ends on a
  failure of its own work or a panic, in a pass or outside one, or the
  upload worker panics: the last pass shows the failure, nothing moves, and
  sync stops (api.md E15). -/
  | halt (failure : PassFailure)
  /-- Sync starts or stops, a cooldown begins or ends, the app pauses or
  resumes uploads, or turns the setting to transfer only on unmetered
  networks on or off. -/
  | settings (running cooling paused unmeteredOnly : Bool)
  /-- The platform says the device's network is now metered, or not, or
  fails to answer, which counts as metered. -/
  | network (metered : Bool)
  /-- A self-replacement of this member's keys starts or ends its write
  pause (§12.3). -/
  | keysChanging (changing : Bool)
  /-- The sync scope's backoff is armed or ends. -/
  | backoff (retrying : Bool)
  /-- The process ends and the store opens again. -/
  | reopen
  deriving DecidableEq, Repr

/-- The later of a kept time and a new one. -/
def later : Option Nat → Nat → Nat
  | none, now => now
  | some kept, now => max kept now

/-- An upload as a reopened store finds it: an attempt ends with its process. -/
def Upload.reopened (u : Upload) : Upload :=
  if u.progress = .moving then { u with progress := .queued } else u

def step (f : Facts) : Event → Option Facts
  | .begin => if f.passing then none else some { f with passing := true, moving := false }
  | .move => if f.passing then some { f with moving := true } else none
  | .finish last now =>
      if f.passing then
        some { f with passing := false, moving := false, last,
                      passRetrying := if last = .synced then false else f.passRetrying,
                      lastSynced := if last = .synced then some (later f.lastSynced now)
                                    else f.lastSynced }
      else none
  | .uploads us => some { f with uploads := us }
  | .queue u => some { f with unsent := u }
  | .pending rs => some { f with records := rs }
  | .storage s finished =>
      some { f with storage := s,
                    lastSynced := match finished with
                      | none => f.lastSynced
                      | some now => some (later f.lastSynced now) }
  | .halt p => some { f with passing := false, moving := false, last := .failed p, running := false }
  | .settings r c p m => some { f with running := r, cooling := c, paused := p, unmeteredOnly := m }
  | .network m => some { f with metered := m }
  | .keysChanging c => some { f with keysChanging := c }
  | .backoff b => some { f with passRetrying := b }
  | .reopen =>
      some { f with passing := false, moving := false,
                    uploads := f.uploads.map Upload.reopened, running := false,
                    cooling := false }

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
    exact state_view (by simp [Facts.view, Facts.bound, still])
  · simp only [run, step, idle, Bool.false_eq_true, ite_false, ite_true, Option.bind_some,
      Option.map_some, Option.some.injEq]
    exact state_view (by simp [Facts.view, Facts.bound, still])

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
  | storage s finished =>
      simp only [step, Option.some.injEq] at h; subst h
      cases finished with
      | none => exact ⟨t, kept, Nat.le_refl t⟩
      | some now => exact ⟨_, rfl, by simp [kept, later]; omega⟩
  | _ =>
      simp only [step, Option.some.injEq] at h; subst h; exact ⟨t, kept, Nat.le_refl t⟩

/-- Only a pass reaching the read end, or setup, joining or restoring
finishing, changes that time: every other event, a reopen and every change
of state included, keeps it. -/
theorem last_synced_kept (f g : Facts) (e : Event) (h : step f e = some g)
    (other : ∀ now, e ≠ .finish .synced now)
    (unfinished : ∀ s now, e ≠ .storage s (some now)) : g.lastSynced = f.lastSynced := by
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
  | storage s finished =>
      simp only [step, Option.some.injEq] at h; subst h
      cases finished with
      | none => rfl
      | some now => exact absurd rfl (unfinished s now)
  | _ => simp only [step, Option.some.injEq] at h; subst h; rfl

theorem ended_queued (f : Facts) (h : f.ended = .offline) :
    ({ f with unsent := true } : Facts).ended = .offlineUnsent := by
  revert h
  unfold Facts.ended Facts.unsentWork
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
  have loop := loop_state f recorded (by rw [offline]; rfl) (by rw [offline]; simp)
    (by rw [offline]; simp)
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
  · simp only [Facts.rest, List.mem_append] at m
    rcases m with (files | write) | others
    · have := highest c (List.mem_cons_of_mem _ (by
        simp only [Facts.rest, List.mem_append]; exact .inl (.inl files)))
      rw [offline] at this
      simp only [State.rank] at this ⊢
      omega
    · simp [g, Facts.writes] at write
      rw [write]; simp [State.rank]
    · have := highest c (List.mem_cons_of_mem _ (by
        simp only [Facts.rest, List.mem_append]; exact .inr others))
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
      Reason.paused.state = none := by
  simp [Reason.state]

/-- A file that can't be uploaded or read names failed: its original missing
or changed, coven's copy failing its check, its stored bytes lost or
refused. -/
theorem file_failures_name_failed :
    Reason.originalMissing.state = some .failed ∧ Reason.originalChanged.state = some .failed ∧
      Reason.fileSource.state = some .failed ∧ Reason.fileLost.state = some .failed ∧
      Reason.refused.state = some .failed := by
  simp [Reason.state]

/-- Synced shows only after a pass that ended synced: one a wait held back,
at a slot waiting on another device's key or by a provider's cooldown,
waits to sync, naming what holds it, and one that failed shows its
failure. -/
theorem failed_pass_not_synced (f : Facts) (p : PassFailure) (h : f.last = .failed p) :
    state f ≠ .synced := by
  intro hs
  have recorded := recorded_of f _ hs (by simp)
  have ⟨_, highest, _⟩ := one_state f recorded
  have le := highest f.loop (List.mem_cons_self ..)
  rw [hs] at le
  simp only [State.rank] at le
  have l0 : f.loop = .synced := rank_injective _ _ (by simp only [State.rank]; omega)
  rcases loop_cases f with s | e
  · rw [s] at l0; cases l0
  · rw [e] at l0; exact failed_ended f p h l0

/-- A pass held at a slot for a wait, or by a provider's cooldown, waits to
sync: neither is a failure. -/
theorem waiting_pass_waits (f : Facts) (r : Reason) (wait : r.state = none) :
    ({ f with last := .failed (.blocked r) } : Facts).ended = .waitingToSync ∧
      ({ f with last := .failed (.storage .rateLimited) } : Facts).ended = .waitingToSync := by
  simp [Facts.ended, PassFailure.state, wait, Failure.named, Failure.byItself]

/-! ## One retry wait -/

/-- **A failure that clears by itself names no state.** A pass ending on a
provider failure that clears by itself and names no state of its own, a
cooldown's included, never names failed: it waits to sync. -/
theorem self_clearing_waits (f : Failure) (clears : f.byItself = true) (unnamed : f.named = none) :
    (PassFailure.storage f).state = none ∧ (PassFailure.storage f).retries = true := by
  simp [PassFailure.state, PassFailure.retries, unnamed, clears]

/-- **One retry wait.** The same failure that clears by itself, as the last
pass's, as the block of a slot's application, or as the failure of coven's own
operation, names the same candidate on a quiet device with sync running and the
sync scope's backoff armed: waiting to sync, retrying. -/
theorem one_retry (f : Facts) (fail : Failure) (answered : Bool)
    (clears : fail.byItself = true) (unnamed : fail.named = none)
    (connected : f.storage = .connected) (still : f.moving = false)
    (idle : f.uploads.filter (·.bound) = []) (noWrites : f.unsent = false)
    (noRecords : f.records = []) (running : f.running = true) (cooling : f.cooling = false)
    (backoff : f.passRetrying = true)
    (last : f.last = .failed (.storage fail) ∨ f.last = .failed (.blocked (.storage fail answered)) ∨
      f.last = .failed (.operation (.storage fail answered))) :
    state f = .waitingToSync ∧ f.waits.head? = some .retrying := by
  rcases last with h | h | h <;>
    simp [state, connected, top, Facts.rest, Facts.files, Facts.writes, Facts.others, Facts.loop,
      Facts.ended, Facts.bound, Facts.waits, Facts.passWait, Facts.heldAll,
      Facts.blocked, Facts.lastRetries, PassFailure.state, PassFailure.retries, Reason.state,
      Reason.byItself, still, idle, noWrites, noRecords, running, cooling, backoff, h, unnamed,
      clears]

/-! ## The network -/

/-- A change of network never changes which state shows, only what a
waiting trailing upload waits for: the state doesn't read it. -/
theorem network_keeps_state (f : Facts) (m : Bool) :
    (step f (.network m)).map state = some (state f) := by
  simp only [step, Option.map_some, Option.some.injEq]
  exact state_view (by simp [Facts.view, Facts.bound])

/-! ## Workers ending, and storage set up -/

/-- Only opening, setup or forgetting changes whether storage is set up
here: no pass, worker's end, upload, write, record, setting or reopen does,
so a failure after setup stands shows through the state, never by undoing
the setup. -/
theorem only_setup_changes_storage (f g : Facts) (e : Event) (h : step f e = some g)
    (other : ∀ s t, e ≠ .storage s t) : g.storage = f.storage := by
  cases e with
  | storage s t => exact absurd rfl (other s t)
  | begin => simp only [step] at h; split at h <;> simp at h; subst h; rfl
  | move => simp only [step] at h; split at h <;> simp at h; subst h; rfl
  | finish last now => simp only [step] at h; split at h <;> simp at h; subst h; rfl
  | _ => simp only [step, Option.some.injEq] at h; subst h; rfl

/-- Once sync halts, every queued upload waits with sync not running, never
for its backoff or its turn: no upload goes out until a start, which a
panic's halt refuses until the store reopens. -/
theorem halted_uploads_wait (f g : Facts) (p : PassFailure) (h : step f (.halt p) = some g) :
    ∀ u, g.uploadWait u = .notSyncing := by
  simp only [step, Option.some.injEq] at h; subst h
  intro u; simp [Facts.uploadWait, Facts.heldAll]

/-- The loop's candidate is never above the state shown. -/
theorem loop_le_state (f : Facts) : f.loop.rank ≤ (state f).rank := by
  by_cases n : f.storage = .none
  · simp only [state, n, ite_true]
    cases f.loop <;> simp [State.rank]
  · exact (one_state f n).2.1 _ (List.mem_cons_self ..)

/-- A pass ending on a failure that names a state shows that state, or the
offline state with unsent changes beside it, with nothing moving. -/
theorem ended_names (f : Facts) (p : PassFailure) (s : State) (h : f.last = .failed p)
    (named : p.state = some s) : s.rank ≤ f.ended.rank := by
  unfold Facts.ended
  rw [h]
  simp only [named]
  cases s <;> cases f.unsentWork <;> simp [State.rank]

/-- A worker's end shows: once a pass, the sync loop or the attempt to
connect ends on a failure that names a state, the state shown is that one
or one that outranks it, and never synced. The setup stands meanwhile
(`only_setup_changes_storage`), so this is how such a failure shows. -/
theorem failed_worker_shows (f g : Facts) (p : PassFailure) (s : State)
    (named : p.state = some s) (e : Event)
    (ends : e = .halt p ∨ ∃ now, e = .finish (.failed p) now) (h : step f e = some g) :
    s.rank ≤ (state g).rank ∧ state g ≠ .synced := by
  have ⟨still, last⟩ : g.moving = false ∧ g.last = .failed p := by
    rcases ends with e' | ⟨now, e'⟩ <;> subst e'
    · simp only [step, Option.some.injEq] at h; subst h; exact ⟨rfl, rfl⟩
    · simp only [step] at h; split at h <;> simp at h; subst h; exact ⟨rfl, rfl⟩
  have loop : g.loop = g.ended := by simp [Facts.loop, still]
  refine ⟨?_, failed_pass_not_synced g p last⟩
  have := ended_names g p s last named
  have := loop_le_state g
  rw [loop] at this
  omega

namespace Examples

def connected : Facts :=
  ⟨.connected, .synced, false, false, [], false, [], true, false, false, false, false, false,
    false, some 100⟩

/-- A cover a write holds back, which the write waits for. -/
def cover (progress : Progress) : Upload := ⟨progress, true, false, none⟩

/-- A track whose write went out first, which its bytes follow. -/
def track (progress : Progress) : Upload := ⟨progress, false, false, none⟩

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

/-- Ana edits a note on her synced laptop: it waits to sync, its turn, until
the next pass claims its slot, which shows syncing, and once that pass has
sent it she is synced. With sync stopped, the note waits for sync to run. -/
example : (run connected [.queue true]).map (fun f => (state f, f.waits)) =
    some (.waitingToSync, [.queued]) := by decide
example : (run connected [.queue true, .begin, .move]).map state = some .syncing := by decide
example : (run connected [.queue true, .begin, .move, .queue false, .finish .synced 160]).map
    state = some .synced := by decide
example : (run connected [.settings false false false false, .queue true]).map
    (fun f => (state f, f.waits)) = some (.waitingToSync, [.notSyncing]) := by decide

/-- Dan writes to his library before he sets up storage: once setup
finishes, his writes wait to sync for the first pass. -/
example : (run { connected with storage := .none, lastSynced := none }
    [.queue true, .storage .connected (some 120)]).map (fun f => (state f, f.waits)) =
    some (.waitingToSync, [.queued]) := by decide

/-- Carol's note waits beside her track with uploads paused: the pause never
reaches the state, so the note's own wait shows. -/
example : (run connected [.settings true false true false, .uploads [track .queued],
    .queue true]).map (fun f => (state f, f.waits)) =
    some (.waitingToSync, [.queued]) := by decide

/-- Ben takes a photo while offline: its queued upload is unsent too, if a
write waits for it. A track queued after its write went out is not. -/
example : (run connected [.begin, .finish (.failed .offline) 140, .uploads [cover .queued]]).map
    state = some .offlineUnsent := by decide
example : (run connected [.begin, .finish (.failed .offline) 140, .uploads [track .queued]]).map
    state = some .offline := by decide

/-- Ana's phone reads a slot sealed with a key her laptop hasn't delivered
yet: with nothing of hers queued, she waits to sync, on the slot, not
synced. A provider's cooldown that cuts a pass short waits on the cooldown,
then, once it is over, on the next pass's turn. -/
example : (run connected [.begin, .finish (.failed (.blocked .keyUnavailable)) 150]).map
    (fun f => (state f, f.waits)) = some (.waitingToSync, [.blocked]) := by decide
example : (run connected [.settings true true false false, .begin,
    .finish (.failed (.storage .rateLimited)) 150]).map (fun f => (state f, f.waits)) =
    some (.waitingToSync, [.coolingDown]) := by decide
example : (run connected [.begin, .finish (.failed (.storage .rateLimited)) 150]).map
    (fun f => (state f, f.waits)) = some (.waitingToSync, [.queued]) := by decide

/-- Ben's pass meets a 503 from the provider, as a slot's block, as the pass's
own failure or as coven's own operation's: waiting to sync, retrying, once the
sync scope's backoff is armed, and never failed. -/
example : (run connected [.backoff true, .begin, .finish (.failed (.storage .network)) 150]).map
    (fun f => (state f, f.waits)) = some (.waitingToSync, [.retrying]) := by decide
example : (run connected [.backoff true, .begin,
    .finish (.failed (.blocked (.storage .network true))) 150]).map
    (fun f => (state f, f.waits)) = some (.waitingToSync, [.retrying]) := by decide
example : (run connected [.backoff true, .begin,
    .finish (.failed (.operation (.storage .network true))) 150]).map
    (fun f => (state f, f.waits)) = some (.waitingToSync, [.retrying]) := by decide

/-- Carol's cover uploads: syncing while it moves, and synced only once it is
stored. Her track never shows: not moving, queued, paused or failed. -/
example : (run connected [.uploads [cover .moving]]).map state = some .syncing := by decide
example : (run connected [.uploads [cover .stored]]).map state = some .synced := by decide
example : (run connected [.uploads [track .moving]]).map state = some .synced := by decide
example : (run connected [.settings true false true false, .uploads [track .queued]]).map
    state = some .synced := by decide

/-- Ben's cover upload got a server error and waits out its backoff: waiting
to sync, retrying; the server error itself names nothing. -/
example : (run connected [.uploads [⟨.queued, true, true, some (.storage .network true)⟩]]).map
    (fun f => (state f, f.waits)) = some (.waitingToSync, [.retrying]) := by decide

/-- Dan unplugs the drive holding a track his move uploads: the state stays
synced, and the track's phase says failed (`Transfers`). A cover whose original
is missing holds its write, so the state shows failed. -/
example : (run connected [.uploads [⟨.queued, true, true, none⟩], .pending [.originalMissing]]).map
    state = some .failed := by decide
example : (run connected [.uploads [⟨.queued, false, true, some .originalMissing⟩]]).map
    state = some .synced := by decide

/-- Carol's upload meets a full Dropbox while the pass syncs: storage full. -/
example : (run connected [.pending [.storage .quotaExceeded true]]).map state =
    some .storageFull := by decide

/-- Dan's sign-in is gone from the keychain: signed out, last synced kept. -/
example : (run connected [.storage .signedOut none, .reopen]).map (fun f => (state f, f.lastSynced)) =
    some (.signedOut, some 100) := by decide

/-- Dan's device was removed while it was signed out: removed outranks. -/
example : (run connected [.storage .signedOut none, .pending [.removed]]).map state =
    some .removed := by decide

/-- Ben's sync loop panics after reading a slot: failed, never syncing,
and sync stops. -/
example : (run connected [.begin, .move, .halt .other]).map (fun f => (state f, f.running)) =
    some (.failed, false) := by decide

/-- Carol's upload worker panics while her cover uploads: failed, and the
cover waits with sync not running. -/
example : (run connected [.uploads [cover .moving], .halt .other, .uploads [cover .queued]]).map
    (fun f => (state f, f.bound.map f.uploadWait)) = some (.failed, [.notSyncing]) := by decide

/-- Ana's pending invite waits for Dan's request: no state of its own. -/
example : (run connected [.pending [.missing, .paused]]).map state = some .synced := by decide

end Examples

end CovenIO.SyncState
