import CovenIO.SyncReach

/-!
# What a file's upload waits for (§17.4, §17.9)

The sync state names the bound files and nothing else (`SyncState.trailing_keeps_state`).
A trailing file, whose write goes out first and whose bytes follow, has a phase
of its own in the upload queue, which an app shows where files are:

- `uploading`: an attempt sends it now;
- `stored`: storage holds its bytes, and it waits for the rest of its move;
- `waiting`, naming what holds it (`UploadWait`);
- `failed`: its pending record names a state, a failure only a person clears or
  one that never clears.

What holds every transfer comes first: sync stopped, a provider's cooldown, a
slot that can't apply. Then what holds a trailing upload only: the app's
pause, a metered network the app moves files on only unmetered, a
self-replacement's write pause. Then the upload's own backoff, then its turn.
A bound upload is held by the first group only, so it is never `paused`,
`metered` or `keysChanging` (`paused_only_trailing`).

- `phase_failed_iff`, `phase_retrying_iff`: failed exactly when the record names
  a state, retrying exactly when the record doesn't and nothing before the
  backoff holds the upload.
- `metered_holds`, `metered_only_held`: with the setting on and the network
  metered, a trailing upload waits for what holds every transfer, the pause or
  the network, never for its backoff or its turn, and waits for the network only
  then.
- `every_phase`: each phase and each wait is reached by a run of the model's
  events, so none is dead.
-/
namespace CovenIO.SyncState

/-- What a queued upload waits for. -/
inductive UploadWait
  /-- Sync isn't running here. -/
  | notSyncing
  /-- The provider asked every request to wait (§6.7). -/
  | coolingDown
  /-- A slot can't apply yet: no transfer goes until it does (§5.4). -/
  | sync
  /-- The app paused uploads, and this one trails. -/
  | paused
  /-- The device's network is metered, the setting to transfer only on
  unmetered networks is on, and the upload trails. -/
  | metered
  /-- A self-replacement of this member's keys pauses writes, and this upload
  trails (§12.3). -/
  | keysChanging
  /-- Its last attempt failed; it retries after its backoff. -/
  | retrying
  /-- Its turn hasn't come. -/
  | queued
  deriving DecidableEq, Repr

/-- Where one upload stands. -/
inductive Phase
  | uploading
  | stored
  | waiting (wait : UploadWait)
  | failed
  deriving DecidableEq, Repr

/-- The uploads the setting to transfer only on unmetered networks holds while
the network is metered: the trailing ones, as the pause holds them. -/
def Upload.heldOnMetered (u : Upload) : Bool := !u.bound

/-- The setting holds this upload now: it is on, the network is metered, and
the upload is one it holds. -/
def Facts.meteredHolds (f : Facts) (u : Upload) : Bool :=
  f.unmeteredOnly && f.metered && u.heldOnMetered

/-- What one queued upload waits for: what holds every transfer first, then
what holds a trailing one (the pause, the network, a self-replacement), then
its backoff. The network comes before the backoff, since an upload whose
backoff ends still can't go out on it. -/
def Facts.transferWait (f : Facts) (u : Upload) : UploadWait :=
  if !f.running then .notSyncing
  else if f.cooling then .coolingDown
  else if f.blocked then .sync
  else if f.paused && !u.bound then .paused
  else if f.meteredHolds u then .metered
  else if f.keysChanging && !u.bound then .keysChanging
  else if u.retrying then .retrying
  else .queued

/-- The phase of one upload: sending, stored, failed once its record names a
state, else waiting for what holds it. -/
def Upload.phase (f : Facts) (u : Upload) : Phase :=
  match u.progress with
  | .moving => .uploading
  | .stored => .stored
  | .queued =>
      match u.record.bind Reason.state with
      | some _ => .failed
      | none => .waiting (f.transferWait u)

/-- **A trailing file never reaches the sync state.** Replacing the uploads
with any others that keep the bound ones leaves the state as it was. -/
theorem trailing_keeps_state (f : Facts) (us : List Upload)
    (same : us.filter (·.bound) = f.bound) : state { f with uploads := us } = state f :=
  state_view (by simp [Facts.view, Facts.bound, same])

/-- A file's phase is failed exactly when its record names a state: its
original or copy missing or changed, its stored bytes lost or refused, or a
failure only a person clears or that never clears. -/
theorem phase_failed_iff (f : Facts) (u : Upload) :
    u.phase f = .failed ↔ u.progress = .queued ∧ (u.record.bind Reason.state).isSome = true := by
  unfold Upload.phase
  cases p : u.progress <;> simp
  cases h : u.record.bind Reason.state <;> simp

/-- A file's phase is retrying exactly when it is queued, its record names no
state (a failure that clears by itself, or none), its backoff runs, and
nothing before the backoff holds it. -/
theorem phase_retrying_iff (f : Facts) (u : Upload) :
    u.phase f = .waiting .retrying ↔
      u.progress = .queued ∧ u.record.bind Reason.state = none ∧ u.retrying = true ∧
        f.heldAll = none ∧ (f.paused && !u.bound) = false ∧ f.meteredHolds u = false ∧
        (f.keysChanging && !u.bound) = false := by
  unfold Upload.phase Facts.transferWait Facts.heldAll
  cases p : u.progress <;> simp
  cases h : u.record.bind Reason.state
  · simp
    cases f.running <;> cases f.cooling <;> cases f.blocked <;> cases f.paused <;>
      cases u.bound <;> cases f.keysChanging <;> cases u.retrying <;> cases f.meteredHolds u <;>
      simp_all
  · simp
    intro hall
    cases hr : u.record <;> simp_all

/-- A bound upload is never held by the pause, the network or a
self-replacement: those hold trailing uploads only. -/
theorem paused_only_trailing (f : Facts) (u : Upload) (bound : u.bound = true) :
    u.phase f ≠ .waiting .paused ∧ u.phase f ≠ .waiting .metered ∧
      u.phase f ≠ .waiting .keysChanging := by
  unfold Upload.phase Facts.transferWait Facts.meteredHolds Upload.heldOnMetered
  cases p : u.progress <;> simp [bound]
  cases u.record.bind Reason.state <;> simp
  cases f.running <;> cases f.cooling <;> cases f.blocked <;> cases u.retrying <;> simp

/-- With the setting on and the network metered, a queued trailing upload
whose record names no state waits for what holds every transfer, for the
pause, or for the network: never for its backoff, a self-replacement or its
turn. -/
theorem metered_holds (f : Facts) (u : Upload) (on : f.unmeteredOnly = true)
    (metered : f.metered = true) (trailing : u.bound = false) (queued : u.progress = .queued)
    (open_ : u.record.bind Reason.state = none) :
    u.phase f ≠ .waiting .retrying ∧ u.phase f ≠ .waiting .keysChanging ∧
      u.phase f ≠ .waiting .queued := by
  unfold Upload.phase Facts.transferWait Facts.meteredHolds Upload.heldOnMetered
  simp only [queued, open_, trailing, on, metered]
  cases f.running <;> cases f.cooling <;> cases f.blocked <;> cases f.paused <;>
    cases f.keysChanging <;> cases u.retrying <;> simp

/-- An upload waits for the network only while the setting is on, the network
is metered and the setting holds it. -/
theorem metered_only_held (f : Facts) (u : Upload) (h : u.phase f = .waiting .metered) :
    f.unmeteredOnly = true ∧ f.metered = true ∧ u.heldOnMetered = true := by
  unfold Upload.phase Facts.transferWait Facts.meteredHolds at h
  cases p : u.progress <;> simp [p] at h
  cases hr : u.record.bind Reason.state <;> simp [hr] at h
  revert h
  cases f.running <;> cases f.cooling <;> cases f.blocked <;> cases f.paused <;>
    cases u.bound <;> cases f.keysChanging <;> cases u.retrying <;> cases f.unmeteredOnly <;>
    cases f.metered <;> simp_all

namespace Reached

/-- Each phase's run: a device connected and synced, then one upload and the
settings that put it there. -/
def toPhase : Phase → List Event
  | .uploading => [.uploads [⟨.moving, false, false, none⟩]]
  | .stored => [.uploads [⟨.stored, false, false, none⟩]]
  | .failed => [.uploads [⟨.queued, false, false, some .originalMissing⟩]]
  | .waiting .notSyncing => [.settings false false false false, .uploads [upload false false]]
  | .waiting .coolingDown => [.settings true true false false, .uploads [upload false false]]
  | .waiting .sync =>
      [.begin, .finish (.failed (.blocked .keyUnavailable)) 1, .uploads [upload false false]]
  | .waiting .paused => [.settings true false true false, .uploads [upload false false]]
  | .waiting .metered =>
      [.settings true false false true, .network true, .uploads [upload false false]]
  | .waiting .keysChanging => [.keysChanging true, .uploads [upload false false]]
  | .waiting .retrying => [.uploads [upload true false]]
  | .waiting .queued => [.uploads [upload false false]]

/-- **No phase is dead.** Every phase, and every wait a file can have, is the
phase of a trailing upload after some run of the model's events from a synced
device. -/
theorem every_phase (p : Phase) :
    (run start (toPhase p)).map (fun f => f.uploads.map (Upload.phase f)) = some [p] := by
  cases p with
  | waiting w => cases w <;> decide
  | _ => decide

end Reached

end CovenIO.SyncState
