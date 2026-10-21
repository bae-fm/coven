import CovenStorage.Objects

namespace CovenStorage.Files

/-- §17.2, D12: this whole reference is fixed by the attaching transaction. -/
structure Reference where
  uploader : Nat
  id : Nat
  key : Nat
  deriving DecidableEq, Repr

def Reference.path (r : Reference) : Path := .file r.uploader r.id

inductive SourceFailure where
  | missing | changed | integrity
  deriving DecidableEq, Repr

/-- §17.4: why a file can't be read from storage. `lost`: storage holds
nothing at a path a row names. `source`: this device's pending attachment,
whose source failed. -/
inductive MissingReason where
  | lost | source (failure : SourceFailure)
  deriving DecidableEq, Repr

inductive Status where
  | available
  | uploading
  | missing (device : Nat) (reason : MissingReason)
  deriving DecidableEq, Repr

/-- Why a status call or read gives no answer. A provider failure keeps its
category. `integrity` is damage: an object at the fixed path that cannot be
this file's complete encrypted bytes. -/
inductive FileError where
  | failed (e : ReadFailure)
  | integrity
  deriving DecidableEq, Repr

/-- §17.4: a confirmed absence at a path a row names. A row names a file only
once storage confirmed it (§17.2) and a named file is never deleted (§17.8), so
nothing on any device explains the absence: storage lost it. -/
def absence (r : Reference) : Status := .missing r.uploader .lost

/-- What this device knows locally about the file: `none` for a file a row
names, `some report` for this device's own pending attachment, with its source
failure if one is recorded (§17.2, §17.7). -/
abbrev Local := Option (Option SourceFailure)

/-- §17.4, E8. `size` is the file's complete encrypted size, which the row's
plaintext size determines (§17.3, D12). This device's pending attachment is
judged locally, with no storage request. For a named file, an object of that
size is available; an object of any other size is damage, not presence or
absence; a failure is never absence. Row/reference validation precedes this
function. -/
def status (r : Reference) (size : Nat) (remote : Except ReadFailure (Option Object))
    (here : Local) : Except FileError Status :=
  match here with
  | some none => .ok .uploading
  | some (some failure) => .ok (.missing r.uploader (.source failure))
  | none => match remote with
    | .error e => .error (.failed e)
    | .ok (some o) => if o.bytes.length = size then .ok .available else .error .integrity
    | .ok none => .ok (absence r)

/-- A named file's status depends only on what storage says at its path:
other devices' states and local reports cannot change it. -/
theorem status_agreement (r : Reference) (size : Nat)
    (a b : Except ReadFailure (Option Object)) (hs : a = b) :
    status r size a none = status r size b none := by
  subst b; rfl

/-- §17.4: this device's own pending attachment needs no storage: whatever
storage says, or fails to say, its status is uploading, or missing with its
source failure. -/
theorem pending_needs_no_storage (r : Reference) (size : Nat)
    (a b : Except ReadFailure (Option Object)) (report : Option SourceFailure) :
    status r size a (some report) = status r size b (some report) := by
  cases report <;> rfl

theorem presence_wins (r : Reference) (size : Nat) (o : Object)
    (h : o.bytes.length = size) :
    status r size (.ok (some o)) none = .ok .available := by
  simp [status, h]

/-- Status reports Available exactly when storage holds an object of the
file's complete encrypted size at the path a row names. -/
theorem available_iff (r : Reference) (size : Nat)
    (remote : Except ReadFailure (Option Object)) (here : Local) :
    status r size remote here = .ok .available ↔
      here = none ∧ ∃ o, remote = .ok (some o) ∧ o.bytes.length = size := by
  cases here with
  | some report => cases report <;> simp [status]
  | none =>
    cases remote with
    | error e => simp [status]
    | ok v => cases v with
      | none => simp [status, absence]
      | some o => by_cases h : o.bytes.length = size <;> simp [status, h]

/-- A wrong-size object is damage: never Available, and never absence. -/
theorem wrong_size_is_damage (r : Reference) (size : Nat) (o : Object)
    (h : o.bytes.length ≠ size) :
    status r size (.ok (some o)) none = .error .integrity := by
  simp [status, h]

theorem read_error_not_missing (r : Reference) (size : Nat) (e : ReadFailure) :
    status r size (.error e) none = .error (.failed e) := rfl

/-- §17.4: no device waits for another's upload. A confirmed absence at a
named path is missing, lost by storage, whoever uploaded it. -/
theorem absence_is_lost (r : Reference) (size : Nat) :
    status r size (.ok none) none = .ok (.missing r.uploader .lost) := rfl

/-- Only this device's own pending attachment is ever uploading. -/
theorem uploading_only_here (r : Reference) (size : Nat)
    (remote : Except ReadFailure (Option Object)) (here : Local)
    (h : status r size remote here = .ok .uploading) : here = some none := by
  cases here with
  | some report => cases report <;> simp_all [status]
  | none =>
    cases remote with
    | error e => simp [status] at h
    | ok v => cases v with
      | none => simp [status, absence] at h
      | some o => by_cases hs : o.bytes.length = size <;> simp [status, hs] at h

theorem absence_not_available (r : Reference) : absence r ≠ .available := by
  simp [absence]

/-- A provider's reply to one ranged read of `[start, stop)`. A reply with
bytes also reports the complete object's size (§20.5: a ranged read still
describes the complete object). -/
inductive RangeReply where
  | bytes (b : Bytes) (total : Nat)
  | absent
  | pastEnd
  | failed (e : ReadFailure)
  deriving DecidableEq, Repr

/-- §20.5, and the provider conformance check in coven-storage: a ranged read
returns exactly the requested bytes with the object's complete size, or
confirms absence, or refuses a range reaching past the object's end. -/
def rangeReply (remote : Except ReadFailure (Option Object)) (start stop : Nat) : RangeReply :=
  match remote with
  | .error e => .failed e
  | .ok none => .absent
  | .ok (some o) =>
    if stop ≤ o.bytes.length then .bytes ((o.bytes.drop start).take (stop - start)) o.bytes.length
    else .pastEnd

/-- A read's failure: no answer from storage, or a confirmed absence carrying
the same verdict status gives (§17.4: the typed error distinguishes uploading
from missing and names the uploader). -/
inductive ReadError where
  | file (e : FileError)
  | unavailable (verdict : Status)
  deriving DecidableEq, Repr

/-- §17.5: one ranged fetch of remote bytes, against the file's complete
encrypted `size`. An object of any other size is damage, as status says; a
range past the object's end means it is too short, the same damage. Chunk
authentication and the content hash follow and are outside this function. -/
def fetch (r : Reference) (size start stop : Nat) :
    RangeReply → Except ReadError Bytes
  | .bytes b total =>
    if total = size ∧ b.length = stop - start then .ok b else .error (.file .integrity)
  | .absent => .error (.unavailable (absence r))
  | .pastEnd => .error (.file .integrity)
  | .failed e => .error (.file (.failed e))

/-- A present object is never read as absent. -/
theorem present_never_absent (r : Reference) (size start stop : Nat)
    (o : Object) (v : Status) :
    fetch r size start stop (rangeReply (.ok (some o)) start stop) ≠
      .error (.unavailable v) := by
  simp only [rangeReply]
  split
  · simp only [fetch]; split <;> simp
  · simp [fetch]

/-- An object of any size but the file's is damage to every read, as to
status, whatever the reports say. -/
theorem wrong_size_read_is_damage (r : Reference) (size start stop : Nat)
    (o : Object) (h : o.bytes.length ≠ size) :
    fetch r size start stop (rangeReply (.ok (some o)) start stop) =
      .error (.file .integrity) := by
  simp only [rangeReply]
  split <;> simp [fetch, h]

/-- A range that reaches past the object's end is damage. -/
theorem past_end_is_damage (r : Reference) (size start stop : Nat)
    (o : Object) (h : o.bytes.length < stop) :
    fetch r size start stop (rangeReply (.ok (some o)) start stop) =
      .error (.file .integrity) := by
  simp [rangeReply, fetch, Nat.not_le.mpr h]

/-- A complete object yields exactly the requested bytes for every range
within the file's size: the damage rule never refuses a genuine file. -/
theorem complete_object_reads (r : Reference) (size start stop : Nat)
    (o : Object) (hsize : o.bytes.length = size) (hstop : stop ≤ size) :
    fetch r size start stop (rangeReply (.ok (some o)) start stop) =
      .ok ((o.bytes.drop start).take (stop - start)) := by
  have hle : stop ≤ o.bytes.length := hsize ▸ hstop
  have hlen : ((o.bytes.drop start).take (stop - start)).length = stop - start := by
    simp only [List.length_take, List.length_drop]; omega
  simp only [rangeReply, if_pos hle]
  simp [fetch, hlen, hsize]

/-- A read that meets a confirmed absence fails with exactly the verdict a
status call on the same observation and evidence returns. -/
theorem read_absence_matches_status (r : Reference) (size start stop : Nat) :
    status r size (.ok none) none = .ok (absence r) ∧
    fetch r size start stop (rangeReply (.ok none) start stop) =
      .error (.unavailable (absence r)) := ⟨rfl, rfl⟩

/-- A provider failure fails the read with its category; it is never absence
or damage. -/
theorem read_failure_kept (r : Reference) (size start stop : Nat)
    (e : ReadFailure) :
    fetch r size start stop (rangeReply (.error e) start stop) =
      .error (.file (.failed e)) := rfl

/-- Status and a read of any range within the file agree on every storage
observation: a read returns bytes exactly when status says Available, and
fails as damage exactly when status does. -/
theorem read_agrees_with_status (r : Reference) (size start stop : Nat)
    (remote : Except ReadFailure (Option Object)) (hstop : stop ≤ size) :
    ((∃ b, fetch r size start stop (rangeReply remote start stop) = .ok b) ↔
      status r size remote none = .ok .available) ∧
    (fetch r size start stop (rangeReply remote start stop) = .error (.file .integrity) ↔
      status r size remote none = .error .integrity) := by
  cases remote with
  | error e => simp [rangeReply, fetch, status]
  | ok v => cases v with
    | none =>
      simp [rangeReply, fetch, status, absence]
    | some o =>
      by_cases h : o.bytes.length = size
      · rw [complete_object_reads r size start stop o h hstop]
        simp [status, h]
      · rw [wrong_size_read_is_damage r size start stop o h]
        simp [status, h]

structure RowFile where
  reference : Reference
  size : Nat
  hash : Nat
  deriving DecidableEq, Repr

/-- Hash tokens denote checked content, not an implementation of SHA-256.
The captured chunks below are ghost values for stating hash soundness. -/
structure QueuedFile where
  file : RowFile
  chunks : List Bytes
  deriving DecidableEq, Repr

/-- A row's four file cells in the merged state, with the step that last set
them. Every write that changes the cells, and every delete and re-add of the
row, gives them a fresh `setter`; an unchanged row keeps its setter (§17.2). -/
structure Cells where
  file : Option RowFile
  setter : Nat
  deriving DecidableEq, Repr

/-- §17.2: a pending attachment, on the attaching device only: the row, its
queued upload, and the row's setter when the attaching write committed. -/
structure Pending where
  row : Nat
  upload : Reference
  baseline : Nat
  deriving DecidableEq, Repr

/-- §17.2: why an attachment was abandoned. -/
inductive Abandoned where
  | replaced | cleared | removed
  deriving DecidableEq, Repr

/-- One device's view: merged rows, its upload queue and pending attachments,
its abandonment records, what storage holds, the paths awaiting a deletion
decision (§17.8), the files with a pending record, and how many writes it
made. `tick` makes setters fresh. -/
structure State where
  rows : Nat → Option Cells
  queue : List QueuedFile
  pending : List Pending
  abandoned : List (Reference × Abandoned)
  stored : List Reference
  candidates : List Reference
  records : List Reference
  writes : Nat
  tick : Nat

def State.setRow (s : State) (row : Nat) (c : Option Cells) : State :=
  { s with rows := fun r => if r = row then c else s.rows r }

/-- The file a row names, if any. -/
def State.named (s : State) (row : Nat) : Option Reference :=
  (s.rows row).bind (·.file) |>.map (·.reference)

/-- The pending attachment is current: its row is there and its cells are
still set by the write that set them when it was attached (§17.2). -/
def current (s : State) (p : Pending) : Bool :=
  match s.rows p.row with
  | some c => c.setter == p.baseline
  | none => false

/-- §17.2: why the row no longer takes the attachment. -/
def reason (s : State) (row : Nat) : Abandoned :=
  match s.rows row with
  | none => .removed
  | some c => if c.file.isSome then .replaced else .cleared

/-- Drop the row's pending attachment, recording why. -/
def abandon (s : State) (row : Nat) (why : Abandoned) : State :=
  { s with pending := s.pending.filter (·.row ≠ row),
           abandoned := (s.pending.filter (·.row = row)).map (fun p => (p.upload, why)) ++
             s.abandoned }

/-- §17.2: a write that changes a row's file cells: it sets another stored
file, clears the file (`some none`), or deletes the row (`none`). It is this
device's own write when `own`, otherwise one from another device applied
here. Its transaction abandons the row's pending attachment, and the file the
row named loses that protection (§17.8). -/
def change (s : State) (row : Nat) (cells : Option (Option RowFile)) (own : Bool) : State :=
  let t := s.tick + 1
  let changed : State :=
    { s.setRow row (cells.map fun f => ⟨f, t⟩) with
      tick := t
      writes := if own then s.writes + 1 else s.writes
      candidates := (s.named row).toList ++ s.candidates }
  abandon changed row (reason changed row)

/-- The app's insert of a row with no file. -/
def insert (s : State) (row : Nat) : State :=
  match s.rows row with
  | some _ => s
  | none => change s row (some none) true

/-- §17.2: the attaching write queues the upload and records the pending
attachment with the row's current setter. It changes no file cell, and
replaces the row's earlier pending attachment. A row that isn't there takes
no attachment. -/
def attach (s : State) (row : Nat) (q : QueuedFile) : State :=
  match s.rows row with
  | none => s
  | some c =>
    let s := abandon s row .replaced
    { s with queue := q :: s.queue
             pending := ⟨row, q.file.reference, c.setter⟩ :: s.pending }

theorem attach_changes_no_row (s : State) (row : Nat) (q : QueuedFile) :
    (attach s row q).rows = s.rows ∧ (attach s row q).writes = s.writes := by
  unfold attach
  cases s.rows row <;> exact ⟨rfl, rfl⟩

/-- The queue entry and pending record go; storage holds the bytes. -/
def dequeue (s : State) (r : Reference) : State :=
  { s with queue := s.queue.filter (·.file.reference != r)
           records := s.records.filter (· != r)
           stored := r :: s.stored }

/-- The completing transaction's row decision, after `dequeue`. -/
def settle (s : State) (r : Reference) (q : QueuedFile) : State :=
  match s.pending.find? (·.upload == r) with
  | some p =>
    if current s p then
      { s.setRow p.row (some ⟨some q.file, s.tick + 1⟩) with
        tick := s.tick + 1
        writes := s.writes + 1
        pending := s.pending.filter (·.upload != r)
        candidates := (s.named p.row).toList ++ s.candidates }
    else { abandon s p.row (reason s p.row) with candidates := r :: s.candidates }
  | none => { s with candidates := r :: s.candidates }

/-- §17.7, §17.2: called only after storage confirmed the complete bytes. One
transaction retires the queue entry and its pending record. If the row's
pending attachment is still current, it writes the file into the row as a
new write of this device; otherwise it abandons it, and the stored bytes,
which no row names, become a deletion candidate (§17.8). An upload no longer
queued has been completed already: nothing happens. -/
def complete (s : State) (r : Reference) : State :=
  match s.queue.find? (·.file.reference == r) with
  | none => s
  | some q => settle (dequeue s r) r q

def finish (s : State) (r : Reference) (expected : Bytes)
    (remote : Except ReadFailure (Option Object)) : State × Settlement :=
  let result := compareOccupied expected remote
  let state := match result with
    | .stored => complete s r
    | _ => s
  (state, result)

/-- §17.7: each run of the upload worker first retires the uploads whose
attachment was abandoned, except those `inFlight`, whose attempts retire them
as they end. Their paths become deletion candidates. -/
def retire (s : State) (inFlight : List Reference) : State :=
  let gone := fun (q : QueuedFile) =>
    !(s.pending.any (·.upload == q.file.reference)) && !(inFlight.contains q.file.reference)
  { s with queue := s.queue.filter (fun q => !gone q)
           records := s.records.filter (fun r => !(s.queue.any (fun q => gone q && q.file.reference == r)))
           candidates := (s.queue.filter gone).map (·.file.reference) ++ s.candidates }

/-- The initiator receives errors and collisions; a mismatch refuses this file attempt. -/
theorem finish_reports_result (s : State) (r : Reference) (expected : Bytes)
    (remote : Except ReadFailure (Option Object)) :
    (finish s r expected remote).2 = compareOccupied expected remote := rfl

theorem unequal_bytes_keep_queue (s : State) (r : Reference) (expected : Bytes)
    (o : Object) (h : o.bytes ≠ expected) :
    finish s r expected (.ok (some o)) = (s, .occupied) := by
  simp [finish, compareOccupied, h]

/-- An attempt that didn't confirm the bytes changes nothing: a crash or a
failure before the completing transaction leaves the upload queued. -/
theorem unconfirmed_changes_nothing (s : State) (r : Reference) (expected : Bytes)
    (remote : Except ReadFailure (Option Object))
    (h : compareOccupied expected remote ≠ .stored) :
    (finish s r expected remote).1 = s := by
  unfold finish
  cases hr : compareOccupied expected remote <;> simp_all

theorem abandon_rows (s : State) (row : Nat) (why : Abandoned) :
    (abandon s row why).rows = s.rows ∧ (abandon s row why).queue = s.queue ∧
    (abandon s row why).stored = s.stored ∧ (abandon s row why).candidates = s.candidates ∧
    (abandon s row why).writes = s.writes := ⟨rfl, rfl, rfl, rfl, rfl⟩

theorem abandon_pending (s : State) (row : Nat) (why : Abandoned) (p : Pending)
    (h : p ∈ (abandon s row why).pending) : p ∈ s.pending ∧ p.row ≠ row := by
  simpa [abandon] using h

theorem settle_written (t : State) (r : Reference) (q : QueuedFile) (p : Pending)
    (hp : t.pending.find? (·.upload == r) = some p) (hc : current t p = true) :
    settle t r q =
      { t.setRow p.row (some ⟨some q.file, t.tick + 1⟩) with
        tick := t.tick + 1
        writes := t.writes + 1
        pending := t.pending.filter (·.upload != r)
        candidates := (t.named p.row).toList ++ t.candidates } := by
  simp [settle, hp, hc]

theorem settle_abandoned (t : State) (r : Reference) (q : QueuedFile) (p : Pending)
    (hp : t.pending.find? (·.upload == r) = some p) (hc : current t p = false) :
    settle t r q = { abandon t p.row (reason t p.row) with candidates := r :: t.candidates } := by
  simp [settle, hp, hc]

theorem settle_unlinked (t : State) (r : Reference) (q : QueuedFile)
    (hp : t.pending.find? (·.upload == r) = none) :
    settle t r q = { t with candidates := r :: t.candidates } := by
  simp [settle, hp]

theorem settle_keeps (t : State) (r : Reference) (q : QueuedFile) :
    (settle t r q).queue = t.queue ∧ (settle t r q).stored = t.stored := by
  cases hp : t.pending.find? (·.upload == r) with
  | none => rw [settle_unlinked t r q hp]; exact ⟨rfl, rfl⟩
  | some p =>
    cases hc : current t p
    · rw [settle_abandoned t r q p hp hc]; exact ⟨rfl, rfl⟩
    · rw [settle_written t r q p hp hc]; exact ⟨rfl, rfl⟩

theorem complete_queued (s : State) (r : Reference) (q : QueuedFile)
    (hq : s.queue.find? (·.file.reference == r) = some q) :
    complete s r = settle (dequeue s r) r q := by
  simp [complete, hq]

theorem complete_unqueued (s : State) (r : Reference)
    (hq : s.queue.find? (·.file.reference == r) = none) : complete s r = s := by
  simp [complete, hq]

theorem dequeue_unqueued (s : State) (r : Reference) :
    ∀ q ∈ (dequeue s r).queue, q.file.reference ≠ r := by
  intro q hq; simp [dequeue] at hq; exact hq.2

/-- §17.7: a completed upload is no longer queued, and completing it again
changes nothing. With the row's write in the same transaction, a retried
completion names the file exactly once. -/
theorem completion_idempotent (s : State) (r : Reference) :
    complete (complete s r) r = complete s r := by
  cases hq : s.queue.find? (·.file.reference == r) with
  | none => rw [complete_unqueued s r hq, complete_unqueued s r hq]
  | some q =>
    rw [complete_queued s r q hq]
    apply complete_unqueued
    rw [(settle_keeps _ r q).1]
    apply List.find?_eq_none.mpr
    intro x hx; simpa using dequeue_unqueued s r x hx

theorem completion_retires_queue (s : State) (r : Reference) :
    ∀ q ∈ (complete s r).queue, q.file.reference ≠ r := by
  cases hq : s.queue.find? (·.file.reference == r) with
  | none =>
    rw [complete_unqueued s r hq]
    intro q hqm heq
    have := List.find?_eq_none.mp hq q hqm
    simp [heq] at this
  | some q =>
    rw [complete_queued s r q hq, (settle_keeps _ r q).1]
    exact dequeue_unqueued s r

/-- §17.2: an abandoned or already-completed attachment's completion changes
no row and makes no write. -/
theorem stale_completion_writes_nothing (s : State) (r : Reference)
    (h : ∀ p, s.pending.find? (·.upload == r) = some p → current s p = false) :
    (complete s r).rows = s.rows ∧ (complete s r).writes = s.writes := by
  cases hq : s.queue.find? (·.file.reference == r) with
  | none => rw [complete_unqueued s r hq]; exact ⟨rfl, rfl⟩
  | some q =>
    rw [complete_queued s r q hq]
    have hpend : (dequeue s r).pending = s.pending := rfl
    cases hp : s.pending.find? (·.upload == r) with
    | none =>
      rw [settle_unlinked _ r q (by rw [hpend]; exact hp)]; exact ⟨rfl, rfl⟩
    | some p =>
      have hc : current (dequeue s r) p = false := h p hp
      rw [settle_abandoned _ r q p (by rw [hpend]; exact hp) hc]; exact ⟨rfl, rfl⟩

/-- §17.2: completion writes the row for a current pending attachment, and
then exactly the queued file, which storage holds. -/
theorem completion_writes_current (s : State) (r : Reference) (q : QueuedFile)
    (hq : s.queue.find? (·.file.reference == r) = some q) (p : Pending)
    (hp : s.pending.find? (·.upload == r) = some p) (hcur : current s p = true) :
    ((complete s r).rows p.row).bind (·.file) = some q.file ∧
      q.file.reference = r ∧ r ∈ (complete s r).stored ∧
      (complete s r).writes = s.writes + 1 := by
  have hr : q.file.reference = r := by simpa using List.find?_some hq
  rw [complete_queued s r q hq, settle_written (dequeue s r) r q p hp hcur]
  simp [State.setRow, dequeue, hr]

/-- What a device may do while an upload is in flight: insert rows, attach
files, make or apply writes that change a row's file, retire abandoned
uploads, and complete other uploads. -/
inductive Step where
  | insert (row : Nat)
  | attach (row : Nat) (q : QueuedFile)
  | change (row : Nat) (cells : Option (Option RowFile)) (own : Bool)
  | retire (inFlight : List Reference)
  | complete (r : Reference)

/-- A change that names a file is a write of the completing device (§17.2):
storage already holds the file. A step that would name a file storage
doesn't hold is not a write any device makes, so it changes nothing. -/
def Step.apply (s : State) : Step → State
  | .insert row => Files.insert s row
  | .attach row q => Files.attach s row q
  | .change row cells loc =>
    match cells with
    | some (some f) => if f.reference ∈ s.stored then Files.change s row cells loc else s
    | _ => Files.change s row cells loc
  | .retire inFlight => Files.retire s inFlight
  | .complete r => Files.complete s r

def runSteps (s : State) (steps : List Step) : State := steps.foldl Step.apply s

/-- The step attaches the upload `r`. -/
def Step.attaches (r : Reference) : Step → Bool
  | .attach _ q => q.file.reference == r
  | _ => false

theorem change_parts (s : State) (row : Nat) (cells : Option (Option RowFile)) (own : Bool) :
    (change s row cells own).queue = s.queue ∧ (change s row cells own).stored = s.stored ∧
    (change s row cells own).candidates = (s.named row).toList ++ s.candidates ∧
    (change s row cells own).rows =
      fun x => if x = row then cells.map (fun f => ⟨f, s.tick + 1⟩) else s.rows x :=
  ⟨rfl, rfl, rfl, rfl⟩

theorem change_pending (s : State) (row : Nat) (cells : Option (Option RowFile)) (own : Bool)
    (p : Pending) (h : p ∈ (change s row cells own).pending) : p ∈ s.pending ∧ p.row ≠ row := by
  have := abandon_pending _ row _ p h
  exact ⟨this.1, this.2⟩

theorem named_of_rows (s t : State) (x : Nat) (h : t.rows x = s.rows x) : t.named x = s.named x := by
  simp [State.named, h]

theorem change_named (s : State) (row : Nat) (cells : Option (Option RowFile)) (own : Bool)
    (x : Nat) :
    (change s row cells own).named x =
      if x = row then (cells.bind id).map (·.reference) else s.named x := by
  by_cases e : x = row
  · subst e; cases cells with
    | none => simp [State.named, change_parts]
    | some y => cases y <;> simp [State.named, change_parts]
  · simp [State.named, change_parts, e]

theorem setRow_named (s : State) (row x : Nat) (c : Option Cells) :
    (s.setRow row c).named x = if x = row then (c.bind (·.file)).map (·.reference) else s.named x := by
  by_cases e : x = row <;> simp [State.named, State.setRow, e]

theorem apply_change (s : State) (row : Nat) (cells : Option (Option RowFile)) (own : Bool)
    (h : ∀ f, cells = some (some f) → f.reference ∈ s.stored) :
    Step.apply s (.change row cells own) = change s row cells own := by
  cases cells with
  | none => rfl
  | some y => cases y with
    | none => rfl
    | some f => simp [Step.apply, h f rfl]

theorem apply_change_or_same (s : State) (row : Nat) (cells : Option (Option RowFile)) (own : Bool) :
    Step.apply s (.change row cells own) = change s row cells own ∨
      (Step.apply s (.change row cells own) = s ∧
        ∃ f, cells = some (some f) ∧ f.reference ∉ s.stored) := by
  cases cells with
  | none => left; rfl
  | some y => cases y with
    | none => left; rfl
    | some f =>
      by_cases hs : f.reference ∈ s.stored
      · left; simp [Step.apply, hs]
      · right; exact ⟨by simp [Step.apply, hs], f, rfl, hs⟩

theorem named_rows (t : State) (x : Nat) :
    t.named x = ((t.rows x).bind (·.file)).map (·.reference) := rfl

/-- The completing transaction, by cases: nothing queued; a write for a current
attachment; or no write, the path becoming a deletion candidate. -/
theorem complete_cases (s : State) (r : Reference) :
    complete s r = s ∨
    (∃ (q : QueuedFile) (p : Pending), q.file.reference = r ∧
      s.pending.find? (·.upload == r) = some p ∧ current s p = true ∧
      (complete s r).rows =
        (fun x => if x = p.row then some ⟨some q.file, s.tick + 1⟩ else s.rows x) ∧
      (complete s r).queue = (dequeue s r).queue ∧ (complete s r).stored = r :: s.stored ∧
      (complete s r).candidates = (s.named p.row).toList ++ s.candidates ∧
      (complete s r).writes = s.writes + 1 ∧
      (∀ x ∈ (complete s r).pending, x ∈ s.pending ∧ x.upload ≠ r)) ∨
    ((complete s r).rows = s.rows ∧ (complete s r).queue = (dequeue s r).queue ∧
      (complete s r).stored = r :: s.stored ∧ (complete s r).candidates = r :: s.candidates ∧
      (complete s r).writes = s.writes ∧ (∀ x ∈ (complete s r).pending, x ∈ s.pending)) := by
  cases hq : s.queue.find? (·.file.reference == r) with
  | none => left; exact complete_unqueued s r hq
  | some q =>
    have hr : q.file.reference = r := by simpa using List.find?_some hq
    right
    rw [complete_queued s r q hq]
    cases hp : s.pending.find? (·.upload == r) with
    | none =>
      right
      rw [settle_unlinked (dequeue s r) r q hp]
      exact ⟨rfl, rfl, rfl, rfl, rfl, fun x hx => hx⟩
    | some p =>
      cases hc : current s p
      · right
        rw [settle_abandoned (dequeue s r) r q p hp hc]
        exact ⟨rfl, rfl, rfl, rfl, rfl,
          fun x hx => (abandon_pending (dequeue s r) p.row (reason (dequeue s r) p.row) x hx).1⟩
      · left
        rw [settle_written (dequeue s r) r q p hp hc]
        refine ⟨q, p, hr, rfl, hc, rfl, rfl, rfl, rfl, rfl, ?_⟩
        intro x hx
        simp only [List.mem_filter] at hx
        exact ⟨hx.1, by simpa using hx.2⟩

/-- No step but attaching `r` gives `r` a pending attachment. -/
theorem step_keeps_unlinked (s : State) (r : Reference) (st : Step)
    (hfresh : st.attaches r = false)
    (h : ∀ p ∈ s.pending, p.upload ≠ r) :
    ∀ p ∈ (Step.apply s st).pending, p.upload ≠ r := by
  have hchange : ∀ row cells own, ∀ p ∈ (change s row cells own).pending, p.upload ≠ r :=
    fun row cells own p hp => h p (change_pending s row cells own p hp).1
  cases st with
  | insert row =>
    simp only [Step.apply, Files.insert]
    cases s.rows row
    · exact hchange _ _ _
    · exact h
  | attach row q =>
    simp only [Step.attaches] at hfresh
    simp only [Step.apply, Files.attach]
    cases s.rows row with
    | none => exact h
    | some c =>
      intro p hp
      simp only [List.mem_cons] at hp
      rcases hp with hp | hp
      · subst hp; simpa using hfresh
      · exact h p (abandon_pending _ _ _ p hp).1
  | change row cells own =>
    rcases apply_change_or_same s row cells own with e | ⟨e, _⟩
    · rw [e]; exact hchange _ _ _
    · rw [e]; exact h
  | retire inFlight => exact h
  | complete r' =>
    show ∀ p ∈ (complete s r').pending, p.upload ≠ r
    rcases complete_cases s r' with e | ⟨_, _, _, _, _, _, _, _, _, _, hp⟩ | ⟨_, _, _, _, _, hp⟩
    · rw [e]; exact h
    · intro x hx; exact h x (hp x hx).1
    · intro x hx; exact h x (hp x hx)

theorem steps_keep_unlinked (s : State) (r : Reference) (steps : List Step)
    (hfresh : ∀ st ∈ steps, st.attaches r = false)
    (h : ∀ p ∈ s.pending, p.upload ≠ r) :
    ∀ p ∈ (runSteps s steps).pending, p.upload ≠ r := by
  induction steps generalizing s with
  | nil => exact h
  | cons st rest ih =>
    exact ih (Step.apply s st) (fun x hx => hfresh x (List.mem_cons_of_mem _ hx))
      (step_keeps_unlinked s r st (hfresh st (List.mem_cons_self ..)) h)

/-- §17.2, a newer change wins. A row has a pending attachment of `q`. This
device then sees a newer change to that row's file: its own write, or another
device's applied here, sets another stored file, clears the file or deletes
the row. Whatever happens next, short of attaching `q` again, the completion
of `q`'s upload changes no row and makes no write. -/
theorem newer_change_wins (s : State) (row : Nat) (q : QueuedFile)
    (cells : Option (Option RowFile)) (own : Bool) (after : List Step)
    (hstored : ∀ f, cells = some (some f) → f.reference ∈ s.stored)
    (hfresh : ∀ st ∈ after, st.attaches q.file.reference = false)
    (hunique : ∀ p ∈ s.pending, p.upload = q.file.reference → p.row = row) :
    let mid := runSteps (Step.apply s (.change row cells own)) after
    (complete mid q.file.reference).rows = mid.rows ∧
      (complete mid q.file.reference).writes = mid.writes := by
  intro mid
  have unlinked : ∀ p ∈ (Step.apply s (.change row cells own)).pending,
      p.upload ≠ q.file.reference := by
    rw [apply_change s row cells own hstored]
    intro p hp heq
    have := change_pending s row cells own p hp
    exact this.2 (hunique p this.1 heq)
  have none := steps_keep_unlinked _ q.file.reference after hfresh unlinked
  apply stale_completion_writes_nothing
  intro p hp
  have hmem := List.mem_of_find?_eq_some hp
  have heq : p.upload = q.file.reference := by simpa using List.find?_some hp
  exact absurd heq (none p hmem)

/-- §17.2: attaching another file to the row abandons the earlier pending
attachment there and then: its completion changes no row. -/
theorem reattach_wins (s : State) (row : Nat) (c : Cells) (q q' : QueuedFile)
    (after : List Step) (hrow : s.rows row = some c)
    (hdiff : q'.file.reference ≠ q.file.reference)
    (hfresh : ∀ st ∈ after, st.attaches q.file.reference = false)
    (hunique : ∀ p ∈ s.pending, p.upload = q.file.reference → p.row = row) :
    let mid := runSteps (attach s row q') after
    (complete mid q.file.reference).rows = mid.rows ∧
      (complete mid q.file.reference).writes = mid.writes := by
  intro mid
  have unlinked : ∀ p ∈ (attach s row q').pending, p.upload ≠ q.file.reference := by
    intro p hp heq
    simp only [attach, hrow, List.mem_cons] at hp
    rcases hp with hp | hp
    · subst hp; exact hdiff heq
    · have := abandon_pending s row _ p hp
      exact this.2 (hunique p this.1 heq)
  have none := steps_keep_unlinked _ q.file.reference after hfresh unlinked
  apply stale_completion_writes_nothing
  intro p hp
  exact absurd (by simpa using List.find?_some hp) (none p (List.mem_of_find?_eq_some hp))

/-- Every file a row names is in storage: rows name only files storage
confirmed (§17.2). -/
def NamedStored (s : State) : Prop :=
  ∀ row r, s.named row = some r → r ∈ s.stored

/-- Nothing in storage is beyond deletion's reach: every stored path is named
by a row, held by the upload queue, or a deletion candidate (§17.8). -/
def Reachable (s : State) : Prop :=
  ∀ r ∈ s.stored, (∃ row, s.named row = some r) ∨ (∃ q ∈ s.queue, q.file.reference = r) ∨
    r ∈ s.candidates

/-- §17.2, §17.8: every step keeps every named file in storage. Deleting a
stored file is outside these steps; §17.8 allows it only for an unnamed one. -/
theorem step_named_stored (s : State) (st : Step) (h : NamedStored s) :
    NamedStored (Step.apply s st) := by
  have hchange : ∀ row cells own, (∀ f, cells = some (some f) → f.reference ∈ s.stored) →
      NamedStored (change s row cells own) := by
    intro row cells own hc x r hn
    rw [(change_parts s row cells own).2.1]
    rw [change_named] at hn
    split at hn
    · cases cells with
      | none => simp at hn
      | some y => cases y with
        | none => simp at hn
        | some f => simp at hn; subst hn; exact hc f rfl
    · exact h x r hn
  cases st with
  | insert row =>
    simp only [Step.apply, Files.insert]
    cases s.rows row
    · exact hchange _ _ _ (by simp)
    · exact h
  | attach row q =>
    simp only [Step.apply, Files.attach]
    cases s.rows row with
    | none => exact h
    | some c => intro x r hn; exact h x r hn
  | change row cells own =>
    rcases apply_change_or_same s row cells own with e | ⟨e, _⟩
    · rw [e]
      cases cells with
      | none => exact hchange _ _ _ (by simp)
      | some y => cases y with
        | none => exact hchange _ _ _ (by simp)
        | some f =>
          by_cases hs : f.reference ∈ s.stored
          · exact hchange _ _ _ (by intro g hg; simp at hg; subst hg; exact hs)
          · simp [Step.apply, hs] at e
            rw [← e]; exact h
    · rw [e]; exact h
  | retire inFlight => intro x r hn; exact h x r hn
  | complete r =>
    show NamedStored (complete s r)
    rcases complete_cases s r with e | ⟨q, p, hr, _, _, hrows, _, hst, _, _, _⟩ |
        ⟨hrows, _, hst, _, _, _⟩
    · rw [e]; exact h
    · intro x y hn
      rw [named_rows, hrows] at hn
      rw [hst]
      by_cases e : x = p.row
      · simp [e] at hn; subst hn; simp [hr]
      · simp only [e, ↓reduceIte] at hn
        exact List.mem_cons_of_mem _ (h x y hn)
    · intro x y hn
      rw [named_rows, hrows] at hn
      rw [hst]
      exact List.mem_cons_of_mem _ (h x y hn)

theorem steps_named_stored (s : State) (steps : List Step) (h : NamedStored s) :
    NamedStored (runSteps s steps) := by
  induction steps generalizing s with
  | nil => exact h
  | cons st rest ih => exact ih _ (step_named_stored s st h)

/-- §17.8: every step keeps every stored path within deletion's reach: an
abandoned or retired upload's path becomes a candidate in the transaction
that ends the queue's protection, and a row's previous file does when a
write replaces it. -/
theorem step_reachable (s : State) (st : Step) (h : Reachable s) :
    Reachable (Step.apply s st) := by
  have hchange : ∀ row cells own, Reachable (change s row cells own) := by
    intro row cells own x hx
    rw [(change_parts s row cells own).2.1] at hx
    rw [(change_parts s row cells own).1, (change_parts s row cells own).2.2.1]
    rcases h x hx with ⟨rw, hn⟩ | hq | hc
    · by_cases e : rw = row
      · subst e; right; right; simp [hn]
      · left; exact ⟨rw, by rw [change_named]; simp [e, hn]⟩
    · right; left; exact hq
    · right; right; simp [hc]
  cases st with
  | insert row =>
    simp only [Step.apply, Files.insert]
    cases s.rows row
    · exact hchange _ _ _
    · exact h
  | attach row q =>
    simp only [Step.apply, Files.attach]
    cases s.rows row with
    | none => exact h
    | some c =>
      intro x hx
      rcases h x hx with ⟨rw, hn⟩ | ⟨q', hq', he⟩ | hc
      · left; exact ⟨rw, hn⟩
      · right; left; exact ⟨q', by simp [abandon, hq'], he⟩
      · right; right; exact hc
  | change row cells own =>
    rcases apply_change_or_same s row cells own with e | ⟨e, _⟩
    · rw [e]; exact hchange _ _ _
    · rw [e]; exact h
  | retire inFlight =>
    show Reachable (retire s inFlight)
    intro x hx
    rcases h x hx with ⟨rw, hn⟩ | ⟨q, hq, he⟩ | hc
    · left; exact ⟨rw, hn⟩
    · by_cases hg : (!(s.pending.any (·.upload == q.file.reference)) &&
          !(inFlight.contains q.file.reference)) = true
      · right; right
        simp only [retire, List.mem_append, List.mem_map, List.mem_filter]
        left; exact ⟨q, ⟨hq, hg⟩, he⟩
      · right; left
        refine ⟨q, ?_, he⟩
        have hg' := Bool.eq_false_iff.mpr hg
        exact List.mem_filter.mpr ⟨hq, by simp only [hg', Bool.not_false]⟩
    · right; right; simp [retire, hc]
  | complete r =>
    show Reachable (complete s r)
    rcases complete_cases s r with e | ⟨q, p, hr, _, _, hrows, hq, hst, hc, _, _⟩ |
        ⟨hrows, hq, hst, hc, _, _⟩
    · rw [e]; exact h
    · intro x hx
      rw [hst] at hx
      by_cases ex : x = r
      · left; refine ⟨p.row, ?_⟩; rw [named_rows, hrows]; simp [ex, hr]
      · have hx' : x ∈ s.stored := (List.mem_cons.mp hx).resolve_left ex
        rcases h x hx' with ⟨y, hy⟩ | ⟨q', hq', he⟩ | hcand
        · by_cases ey : y = p.row
          · right; right; rw [hc]; subst ey; simp [hy]
          · left; refine ⟨y, ?_⟩; rw [named_rows, hrows]; simp only [ey, ↓reduceIte]; exact hy
        · right; left; refine ⟨q', ?_, he⟩
          rw [hq]; simp only [dequeue, List.mem_filter]
          exact ⟨hq', by simpa [he] using ex⟩
        · right; right; rw [hc]; simp [hcand]
    · intro x hx
      rw [hst] at hx
      by_cases ex : x = r
      · right; right; rw [hc]; simp [ex]
      · have hx' : x ∈ s.stored := (List.mem_cons.mp hx).resolve_left ex
        rcases h x hx' with ⟨y, hy⟩ | ⟨q', hq', he⟩ | hcand
        · left; refine ⟨y, ?_⟩; rw [named_rows, hrows]; exact hy
        · right; left; refine ⟨q', ?_, he⟩
          rw [hq]; simp only [dequeue, List.mem_filter]
          exact ⟨hq', by simpa [he] using ex⟩
        · right; right; rw [hc]; simp [hcand]

theorem steps_reachable (s : State) (steps : List Step) (h : Reachable s) :
    Reachable (runSteps s steps) := by
  induction steps generalizing s with
  | nil => exact h
  | cons st rest ih => exact ih _ (step_reachable s st h)

/-- D12: a file chunk's nonce is its index. The fixed key and reference,
size header and verified plaintext determine every encrypted byte. -/
structure EncryptedChunk where
  reference : Reference
  size : Nat
  index : Nat
  plaintext : Bytes
  deriving DecidableEq, Repr

/-- §17.7 models a collision-free checked chunk hash by equality with its
ghost captured bytes. No changed chunk reaches encryption. -/
def checkChunk (q : QueuedFile) (index : Nat) (source : Bytes) :
    Except SourceFailure EncryptedChunk :=
  match q.chunks[index]? with
  | none => .error .integrity
  | some original =>
    if source = original then .ok ⟨q.file.reference, q.file.size, index, source⟩
    else .error .changed

theorem encrypted_chunk_is_captured (q : QueuedFile) (i : Nat) (source : Bytes)
    (c : EncryptedChunk) (h : checkChunk q i source = .ok c) :
    q.chunks[i]? = some c.plaintext ∧ c.reference = q.file.reference ∧
    c.size = q.file.size ∧ c.index = i := by
  unfold checkChunk at h
  split at h
  · contradiction
  · split at h
    · cases h; simp_all
    · contradiction

theorem file_retries_fixed (q : QueuedFile) (i : Nat) (a b : Bytes)
    (ca cb : EncryptedChunk) (ha : checkChunk q i a = .ok ca)
    (hb : checkChunk q i b = .ok cb) : ca = cb := by
  obtain ⟨pa, ra, sa, ia⟩ := encrypted_chunk_is_captured q i a ca ha
  obtain ⟨pb, rb, sb, ib⟩ := encrypted_chunk_is_captured q i b cb hb
  have hp : ca.plaintext = cb.plaintext := Option.some.inj (pa.symm.trans pb)
  cases ca; cases cb; simp_all

end CovenStorage.Files
