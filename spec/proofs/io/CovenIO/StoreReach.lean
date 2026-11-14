import CovenIO.SyncReach
import CovenSequence.Device
import CovenSequence.Membership

/-!
# Which store histories reach a sync state (§5.4, §14.1, §19.1)

`SyncReach` goes from the facts to the state. This goes from a store history to
the fact a pass leaves behind, with `CovenSequence`'s device reading that
history (`Device`, `Good`, `DevSteps`).

`PassEnd` says how a pass ends on a device, from where its readers stand. A
pass that reads to the head ends synced. One held at a slot of its audience
without the key, or at a kept slot it can't open, ends blocked at that slot. One
whose reader has applied its member's removal, or a replacement of its member's
keys, stops. Rust's `pass` and `LastPass` make these reports; the link is by
name, and `io_differential_sync_state` checks what each one shows.

- `synced_reached`: on good storage, a device with its keys that can open every
  slot can end a pass synced.
- `key_wait_reached`, `unopened_reached`: a device held at a slot never ends
  synced from there, however it steps, and shows waiting to sync on the slot, or
  update needed, or failed.
- `removal_reached`, `replacement_reached`: once a reader has applied the slot,
  the device shows removed, or needs reset.
- `device_removal_reached`, `device_replacement_reached`: the same for this
  device's own removal, and for a registration taking its place.

`copy_precedes_need_fails` in `CovenSequence.CopyOrderExamples` is a history
that reaches `key_wait_reached` with every device following the rules.

The rest come from no store history. Offline, signed out, no access, storage
full, storage folder missing and clock wrong are what a provider or custody
answers; not synced is no location recorded; syncing is a pass or a file moving.
`SyncReach.every_state` reaches those from the answers themselves.
-/
namespace CovenIO.StoreReach

open CovenSequence (Sig Rules Store Good Device DevSteps DevOk Slot Applied Entry touches ledgerAt)
open CovenIO.SyncState (LastPass PassFailure Facts State SyncWait state run Event)

abbrev Why := CovenIO.SyncState.Reason

variable {S : Sig}

/-- How a pass ends on `dv`, reading `seq` from `st`, for the device `self` of
member `me` holding the key generation `gen`. `why` is what a slot the device
can't open reports: `refused` for damaged bytes, `updateRequired` for newer ones. -/
inductive PassEnd (seq : Nat → Slot S) (st : Store S) (self : S.Dev) (me : S.Mem) (gen : Nat)
    (dv : Device S) : LastPass → Prop
  /-- Every reader reached the head. -/
  | caughtUp (all : ∀ A ∈ dv.follows, (dv.reader A).pos = st.head) :
      PassEnd seq st self me gen dv .synced
  /-- A reader's next slot is its audience's and the device has no key for it. -/
  | keyless (B : S.Aud) (followed : B ∈ dv.follows) (missing : dv.keys B = false)
      (touch : touches B (seq ((dv.reader B).pos + 1)) = true) :
      PassEnd seq st self me gen dv (.failed (.blocked .keyUnavailable))
  /-- A reader's next slot is kept and the device can't open it. -/
  | unopened (B : S.Aud) (followed : B ∈ dv.follows) (why : Why)
      (reason : why = .refused ∨ why = .updateRequired)
      (kept : st.floor.start ≤ (dv.reader B).pos + 1) (held : (dv.reader B).pos < st.head)
      (closed : dv.opens ((dv.reader B).pos + 1) = false) :
      PassEnd seq st self me gen dv (.failed (.blocked why))
  /-- A reader has applied the member's removal. -/
  | removed (A : S.Aud) (followed : A ∈ dv.follows)
      (gone : (dv.reader A).ledger.member me = false) :
      PassEnd seq st self me gen dv (.failed .removed)
  /-- A reader has applied a replacement of the member's keys. -/
  | replaced (A : S.Aud) (followed : A ∈ dv.follows)
      (newer : gen < (dv.reader A).ledger.keyGen me) :
      PassEnd seq st self me gen dv (.failed (.blocked .memberKeysReplaced))
  /-- A reader has applied this device's removal. -/
  | deviceRemoved (A : S.Aud) (followed : A ∈ dv.follows) (i : Nat) (a : S.Dev)
      (slot : seq i = .admin a (.removeDevice self)) (past : i ≤ (dv.reader A).pos) :
      PassEnd seq st self me gen dv (.failed .removed)
  /-- A reader has applied a registration taking this device's place. -/
  | deviceReplaced (A : S.Aud) (followed : A ∈ dv.follows) (i : Nat) (a fresh : S.Dev) (m : S.Mem)
      (slot : seq i = .admin a (.addDevice fresh m (some self))) (past : i ≤ (dv.reader A).pos) :
      PassEnd seq st self me gen dv (.failed .deviceIdInUse)

/-- Nothing but the pass names a state: a connected device, not in a pass, with
no bound file moving or queued, no write queued and no record. -/
structure Quiet (f : Facts) : Prop where
  connected : f.storage = .connected
  idle : f.passing = false
  noUploads : f.uploads.filter (·.bound) = []
  noWrites : f.unsent = false
  noRecords : f.records = []

/-- A pass ending `last` on a quiet device shows what `last` names. -/
theorem quiet_shows {f : Facts} (q : Quiet f) (last : LastPass) (now : Nat) :
    (run f [.begin, .finish last now]).map state = some { f with last }.ended := by
  simp only [run, CovenIO.SyncState.step, q.idle, Bool.false_eq_true, ite_false, ite_true,
    Option.bind_some, Option.map_some, Option.some.injEq]
  cases last <;>
    simp [state, q.connected, CovenIO.SyncState.top, CovenIO.SyncState.Facts.rest,
      CovenIO.SyncState.Facts.files, CovenIO.SyncState.Facts.writes,
      CovenIO.SyncState.Facts.others, CovenIO.SyncState.Facts.loop,
      CovenIO.SyncState.Facts.ended, CovenIO.SyncState.Facts.unsentWork,
      CovenIO.SyncState.Facts.bound, CovenIO.SyncState.Facts.passWait,
      CovenIO.SyncState.Facts.heldAll, CovenIO.SyncState.Facts.blocked,
      CovenIO.SyncState.Facts.lastRetries, q.noUploads, q.noRecords, q.noWrites]

variable (R : Rules S)

/-- **A history that reads through ends synced.** On good storage, a device with
the key of every audience it follows that can open every slot has a run to the
head, and a pass ending there shows synced. -/
theorem synced_reached {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) {dv : Device S}
    (running : dv.stopped = false) (ok : DevOk R st seq dv) (keys : ∀ A ∈ dv.follows, dv.keys A = true)
    (opens : ∀ k, dv.opens k = true) (self : S.Dev) (me : S.Mem) (gen : Nat) {f : Facts} (q : Quiet f) (now : Nat) :
    ∃ dv', DevSteps R st dv dv' ∧ PassEnd seq st self me gen dv' .synced ∧
      (run f [.begin, .finish .synced now]).map state = some .synced := by
  obtain ⟨dv', run', _, head⟩ := CovenSequence.device_reaches_head R g dv running ok keys opens
  have same := (CovenSequence.devs_frame R run').1
  refine ⟨dv', run', .caughtUp fun A hA => head A (same ▸ hA), ?_⟩
  rw [quiet_shows q]
  rfl

/-- **A device without a key is held there.** On good storage, a device that
follows `B` without `B`'s key, whose `B` reader's next slot is `B`'s, ends every
pass from there on blocked at that slot, however it steps, and never reaches the
head: it shows waiting to sync, on the slot. -/
theorem key_wait_reached {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) {dv : Device S}
    {B : S.Aud} (followed : B ∈ dv.follows) (missing : dv.keys B = false) (ok : DevOk R st seq dv)
    (touch : touches B (seq ((dv.reader B).pos + 1)) = true) (held : (dv.reader B).pos < st.head)
    (self : S.Dev) (me : S.Mem) (gen : Nat) {f : Facts} (q : Quiet f)
    (running : f.running = true)
    (cooling : f.cooling = false) (now : Nat) :
    ∀ dv', DevSteps R st dv dv' →
      ¬ PassEnd seq st self me gen dv' .synced ∧
      PassEnd seq st self me gen dv' (.failed (.blocked .keyUnavailable)) ∧
      (run f [.begin, .finish (.failed (.blocked .keyUnavailable)) now]).map
        (fun x => (state x, x.waits)) = some (.waitingToSync, [.blocked]) := by
  intro dv' steps
  have stays := (CovenSequence.device_stops R g followed missing ok touch dv' steps).1
  obtain ⟨sameFollows, sameKeys, _⟩ := CovenSequence.devs_frame R steps
  refine ⟨?_, .keyless B (sameFollows ▸ followed) (sameKeys ▸ missing) (stays ▸ touch), ?_⟩
  · intro ended
    cases ended with
    | caughtUp all => have := all B (sameFollows ▸ followed); omega
  · simp [run, CovenIO.SyncState.step, q.idle, state, q.connected, CovenIO.SyncState.top,
      CovenIO.SyncState.Facts.rest, CovenIO.SyncState.Facts.files,
      CovenIO.SyncState.Facts.writes, CovenIO.SyncState.Facts.others,
      CovenIO.SyncState.Facts.loop, CovenIO.SyncState.Facts.ended,
      CovenIO.SyncState.PassFailure.state, CovenIO.SyncState.Reason.state,
      CovenIO.SyncState.Facts.waits, CovenIO.SyncState.Facts.passWait,
      CovenIO.SyncState.Facts.heldAll, CovenIO.SyncState.Facts.blocked, CovenIO.SyncState.Facts.bound,
      CovenIO.SyncState.Facts.lastRetries, CovenIO.SyncState.Reason.byItself,
      CovenIO.SyncState.State.work, q.noUploads,
      q.noWrites, q.noRecords, running, cooling]

/-- **A device that can't open a kept slot is held there.** It ends every pass
from there blocked at that slot, however it steps, and shows failed for damaged
bytes and update needed for newer ones. -/
theorem unopened_reached {st : Store S} {seq : Nat → Slot S} {dv : Device S} {B : S.Aud}
    (followed : B ∈ dv.follows) (kept : st.floor.start ≤ (dv.reader B).pos + 1)
    (held : (dv.reader B).pos < st.head) (closed : dv.opens ((dv.reader B).pos + 1) = false)
    (self : S.Dev) (me : S.Mem) (gen : Nat) (why : Why) (reason : why = .refused ∨ why = .updateRequired)
    {f : Facts} (q : Quiet f) (now : Nat) :
    ∀ dv', DevSteps R st dv dv' →
      ¬ PassEnd seq st self me gen dv' .synced ∧
      PassEnd seq st self me gen dv' (.failed (.blocked why)) ∧
      (run f [.begin, .finish (.failed (.blocked why)) now]).map state =
        some (if why = .refused then .failed else .updateNeeded) := by
  intro dv' steps
  have stays := (CovenSequence.unopened_stops R followed kept held closed dv' steps).1
  obtain ⟨sameFollows, _, sameOpens⟩ := CovenSequence.devs_frame R steps
  refine ⟨?_, .unopened B (sameFollows ▸ followed) why reason (stays ▸ kept) (stays ▸ held)
    (by rw [stays, sameOpens]; exact closed), ?_⟩
  · intro ended
    cases ended with
    | caughtUp all => have := all B (sameFollows ▸ followed); omega
  · rw [quiet_shows q]
    rcases reason with rfl | rfl <;> rfl

/-- **A removal ends the device.** Once a sound reader has applied its member's
removal, a pass ends removed, and a quiet device shows removed. -/
theorem removal_reached {st : Store S} {seq : Nat → Slot S} {dv : Device S} {A : S.Aud}
    (followed : A ∈ dv.follows) (ok : DevOk R st seq dv) {i : Nat} (positive : 1 ≤ i) {a : S.Dev}
    {me : S.Mem} (slot : seq i = .admin a (.removeMember me)) (applied : Applied R seq i)
    (past : i ≤ (dv.reader A).pos) (self : S.Dev) (gen : Nat) {f : Facts} (q : Quiet f) (now : Nat) :
    PassEnd seq st self me gen dv (.failed .removed) ∧
      (run f [.begin, .finish (.failed .removed) now]).map state = some .removed := by
  have gone := (CovenSequence.removed_member_never_active R seq i positive a me slot applied _ past).2
  rw [← (ok A followed).ledger] at gone
  refine ⟨.removed A followed gone, ?_⟩
  rw [quiet_shows q]
  rfl

/-- **A replacement ends the device's pair.** Once a sound reader has applied a
replacement of its member's keys, a pass on a device holding the older generation
ends blocked with `MemberKeysReplaced`, and a quiet device needs reset. -/
theorem replacement_reached {st : Store S} {seq : Nat → Slot S} {dv : Device S} {A : S.Aud}
    (followed : A ∈ dv.follows) (ok : DevOk R st seq dv) {i : Nat} (positive : 1 ≤ i) {a : S.Dev}
    {me : S.Mem} (slot : seq i = .admin a (.replaceKey me)) (applied : Applied R seq i)
    (past : i ≤ (dv.reader A).pos) (self : S.Dev) {gen : Nat}
    (older : gen ≤ (ledgerAt R seq (i - 1)).keyGen me) {f : Facts} (q : Quiet f) (now : Nat) :
    PassEnd seq st self me gen dv (.failed (.blocked .memberKeysReplaced)) ∧
      (run f [.begin, .finish (.failed (.blocked .memberKeysReplaced)) now]).map state =
        some .needsReset := by
  have bumped : (ledgerAt R seq i).keyGen me = (ledgerAt R seq (i - 1)).keyGen me + 1 := by
    rw [CovenSequence.applied_ledger R seq positive applied, slot]
    simp [CovenSequence.ledgerEffect, CovenSequence.applyEntry]
  have grown := CovenSequence.keyGen_mono R seq me i _ past
  rw [← (ok A followed).ledger] at grown
  refine ⟨.replaced A followed (by omega), ?_⟩
  rw [quiet_shows q]
  rfl

/-- **A device's own removal ends it.** Once a sound reader has applied it, the
reader's ledger counts the device removed, a pass ends removed, and a quiet
device shows removed. -/
theorem device_removal_reached {st : Store S} {seq : Nat → Slot S} {dv : Device S} {A : S.Aud}
    (followed : A ∈ dv.follows) (ok : DevOk R st seq dv) {i : Nat} (positive : 1 ≤ i) {a self : S.Dev}
    (slot : seq i = .admin a (.removeDevice self)) (applied : Applied R seq i)
    (past : i ≤ (dv.reader A).pos) (me : S.Mem) (gen : Nat) {f : Facts} (q : Quiet f) (now : Nat) :
    (dv.reader A).ledger.removed self = true ∧
      PassEnd seq st self me gen dv (.failed .removed) ∧
      (run f [.begin, .finish (.failed .removed) now]).map state = some .removed := by
  have gone : (ledgerAt R seq i).removed self = true := by
    rw [CovenSequence.applied_ledger R seq positive applied, slot]
    simp [CovenSequence.ledgerEffect, CovenSequence.applyEntry]
  have stays := CovenSequence.removed_stays R seq i gone _ past
  rw [← (ok A followed).ledger] at stays
  refine ⟨stays, .deviceRemoved A followed i a slot past, ?_⟩
  rw [quiet_shows q]
  rfl

/-- **A registration in a device's place ends it.** Once a sound reader has
applied a reset's or a restore's registration naming this device, the reader's
ledger counts it removed, a pass ends on another copy using its id, and a quiet
device needs reset. -/
theorem device_replacement_reached {st : Store S} {seq : Nat → Slot S} {dv : Device S} {A : S.Aud}
    (followed : A ∈ dv.follows) (ok : DevOk R st seq dv) {i : Nat} (positive : 1 ≤ i)
    {a fresh self : S.Dev} {m : S.Mem} (slot : seq i = .admin a (.addDevice fresh m (some self)))
    (applied : Applied R seq i) (past : i ≤ (dv.reader A).pos) (me : S.Mem) (gen : Nat) {f : Facts}
    (q : Quiet f) (now : Nat) :
    (dv.reader A).ledger.removed self = true ∧
      PassEnd seq st self me gen dv (.failed .deviceIdInUse) ∧
      (run f [.begin, .finish (.failed .deviceIdInUse) now]).map state = some .needsReset := by
  have gone : (ledgerAt R seq i).removed self = true := by
    rw [CovenSequence.applied_ledger R seq positive applied, slot]
    simp [CovenSequence.ledgerEffect, CovenSequence.applyEntry]
  have stays := CovenSequence.removed_stays R seq i gone _ past
  rw [← (ok A followed).ledger] at stays
  refine ⟨stays, .deviceReplaced A followed i a fresh m slot past, ?_⟩
  rw [quiet_shows q]
  rfl

end CovenIO.StoreReach
