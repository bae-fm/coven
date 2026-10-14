import CovenStorage.Identity

namespace CovenStorage.Files

/-- §16.1, D12: this whole reference is fixed by the attaching transaction. -/
structure Reference where
  uploader : Nat
  id : Nat
  key : Nat
  deriving DecidableEq, Repr

def Reference.path (r : Reference) : Path := .file r.uploader r.id

inductive SourceFailure where
  | missing | changed | integrity
  deriving DecidableEq, Repr

inductive DeviceState where
  | active | removed | replaced
  deriving DecidableEq, Repr

inductive MissingReason where
  | deviceRemoved | deviceReplaced | source (failure : SourceFailure)
  deriving DecidableEq, Repr

inductive Status where
  | available
  | uploading (device : Nat)
  | missing (device : Nat) (reason : MissingReason)
  deriving DecidableEq, Repr

/-- D8: only authenticated reports by the uploader enter this input. -/
structure Reports where
  device : Nat → DeviceState
  unavailable : Nat → Nat → Option SourceFailure

/-- Why a status call or read gives no answer. A provider failure keeps its
category. `integrity` is damage: an object at the fixed path that cannot be
this file's complete encrypted bytes. -/
inductive FileError where
  | failed (e : ReadFailure)
  | integrity
  deriving DecidableEq, Repr

/-- §16.1: the verdict for a confirmed absence. When both a device state and
a source report explain it, device state selects the reason. -/
def absence (r : Reference) (reports : Reports) : Status :=
  match reports.device r.uploader with
  | .removed => .missing r.uploader .deviceRemoved
  | .replaced => .missing r.uploader .deviceReplaced
  | .active => match reports.unavailable r.uploader r.id with
    | some e => .missing r.uploader (.source e)
    | none => .uploading r.uploader

/-- §16.1, E8. `size` is the file's complete encrypted size, which the row's
plaintext size determines (§16.2, D12). An object of that size wins over every
report; an object of any other size is damage, not presence or absence; a
failure is never absence. Row/reference validation precedes this function. -/
def status (r : Reference) (size : Nat) (remote : Except ReadFailure (Option Object))
    (reports : Reports) : Except FileError Status :=
  match remote with
  | .error e => .error (.failed e)
  | .ok (some o) => if o.bytes.length = size then .ok .available else .error .integrity
  | .ok none => .ok (absence r reports)

/-- Only facts about this uploader and this file matter. Different local
queues, clocks, caches, pause states and unrelated reports cannot change it. -/
theorem status_agreement (r : Reference) (size : Nat)
    (a b : Except ReadFailure (Option Object)) (ra rb : Reports)
    (hs : a = b) (hd : ra.device r.uploader = rb.device r.uploader)
    (hr : ra.unavailable r.uploader r.id = rb.unavailable r.uploader r.id) :
    status r size a ra = status r size b rb := by
  subst b
  cases a with
  | error e => rfl
  | ok v => cases v with
    | some o => rfl
    | none => simp only [status, absence, hd, hr]

theorem presence_wins (r : Reference) (size : Nat) (o : Object) (reports : Reports)
    (h : o.bytes.length = size) :
    status r size (.ok (some o)) reports = .ok .available := by
  simp [status, h]

/-- Status reports Available exactly when storage holds an object of the
file's complete encrypted size at its fixed path. -/
theorem available_iff (r : Reference) (size : Nat)
    (remote : Except ReadFailure (Option Object)) (reports : Reports) :
    status r size remote reports = .ok .available ↔
      ∃ o, remote = .ok (some o) ∧ o.bytes.length = size := by
  cases remote with
  | error e => simp [status]
  | ok v => cases v with
    | none =>
      simp only [status, absence]
      constructor
      · intro h; split at h <;> (try split at h) <;> simp at h
      · intro ⟨_, h, _⟩; simp at h
    | some o => by_cases h : o.bytes.length = size <;> simp [status, h]

/-- A wrong-size object is damage whatever the reports say: never Available,
and never absence (Uploading or Missing). -/
theorem wrong_size_is_damage (r : Reference) (size : Nat) (o : Object) (reports : Reports)
    (h : o.bytes.length ≠ size) :
    status r size (.ok (some o)) reports = .error .integrity := by
  simp [status, h]

theorem read_error_not_missing (r : Reference) (size : Nat) (e : ReadFailure)
    (reports : Reports) :
    status r size (.error e) reports = .error (.failed e) := rfl

theorem absent_active_uploading (r : Reference) (size : Nat) (reports : Reports)
    (hd : reports.device r.uploader = .active)
    (hr : reports.unavailable r.uploader r.id = none) :
    status r size (.ok none) reports = .ok (.uploading r.uploader) := by
  simp [status, absence, hd, hr]

theorem absence_not_available (r : Reference) (reports : Reports) :
    absence r reports ≠ .available := by
  unfold absence
  split
  · simp
  · simp
  · split <;> simp

/-- A provider's reply to one ranged read of `[start, stop)`. -/
inductive RangeReply where
  | bytes (b : Bytes)
  | absent
  | pastEnd
  | failed (e : ReadFailure)
  deriving DecidableEq, Repr

/-- §4, and the provider conformance check in coven-storage: a ranged read
returns exactly the requested bytes, or confirms absence, or refuses a range
reaching past the object's end. -/
def rangeReply (remote : Except ReadFailure (Option Object)) (start stop : Nat) : RangeReply :=
  match remote with
  | .error e => .failed e
  | .ok none => .absent
  | .ok (some o) =>
    if stop ≤ o.bytes.length then .bytes ((o.bytes.drop start).take (stop - start))
    else .pastEnd

/-- A read's failure: no answer from storage, or a confirmed absence carrying
the same verdict status gives (§16.1: the typed error distinguishes uploading
from missing and names the uploader). -/
inductive ReadError where
  | file (e : FileError)
  | unavailable (verdict : Status)
  deriving DecidableEq, Repr

/-- §16.3: one ranged fetch of remote bytes. Every range a read requests lies
within the file's complete encrypted size, because the header is checked
against the row's size first, so a range past the object's end means the
object is too short: damage, not absence. Chunk authentication and the content
hash follow and are outside this function. -/
def fetch (r : Reference) (reports : Reports) (start stop : Nat) : RangeReply → Except ReadError Bytes
  | .bytes b => if b.length = stop - start then .ok b else .error (.file .integrity)
  | .absent => .error (.unavailable (absence r reports))
  | .pastEnd => .error (.file .integrity)
  | .failed e => .error (.file (.failed e))

/-- A present object is never read as absent. -/
theorem present_never_absent (r : Reference) (reports : Reports) (start stop : Nat)
    (o : Object) (v : Status) :
    fetch r reports start stop (rangeReply (.ok (some o)) start stop) ≠
      .error (.unavailable v) := by
  simp only [rangeReply]
  split
  · simp only [fetch]; split <;> simp
  · simp [fetch]

/-- A range within the file's size that reaches past the object's end is
damage, whatever the reports say. -/
theorem past_end_is_damage (r : Reference) (reports : Reports) (start stop : Nat)
    (o : Object) (h : o.bytes.length < stop) :
    fetch r reports start stop (rangeReply (.ok (some o)) start stop) =
      .error (.file .integrity) := by
  simp [rangeReply, fetch, Nat.not_le.mpr h]

/-- A complete object yields exactly the requested bytes for every range
within the file's size: the damage rule never refuses a genuine file. -/
theorem complete_object_reads (r : Reference) (reports : Reports) (size start stop : Nat)
    (o : Object) (hsize : o.bytes.length = size) (hstop : stop ≤ size) :
    fetch r reports start stop (rangeReply (.ok (some o)) start stop) =
      .ok ((o.bytes.drop start).take (stop - start)) := by
  have hle : stop ≤ o.bytes.length := hsize ▸ hstop
  have hlen : ((o.bytes.drop start).take (stop - start)).length = stop - start := by
    simp only [List.length_take, List.length_drop]; omega
  simp [rangeReply, fetch, hle, hlen]

/-- A read that meets a confirmed absence fails with exactly the verdict a
status call on the same observation and evidence returns. -/
theorem read_absence_matches_status (r : Reference) (reports : Reports)
    (size start stop : Nat) :
    status r size (.ok none) reports = .ok (absence r reports) ∧
    fetch r reports start stop (rangeReply (.ok none) start stop) =
      .error (.unavailable (absence r reports)) := ⟨rfl, rfl⟩

/-- A provider failure fails the read with its category; it is never absence
or damage. -/
theorem read_failure_kept (r : Reference) (reports : Reports) (start stop : Nat)
    (e : ReadFailure) :
    fetch r reports start stop (rangeReply (.error e) start stop) =
      .error (.file (.failed e)) := rfl

structure RowFile where
  reference : Reference
  size : Nat
  hash : Nat
  version : Nat
  deriving DecidableEq, Repr

/-- Hash tokens denote checked content, not an implementation of SHA-256.
The captured chunks below are ghost values for stating hash soundness. -/
structure QueuedFile where
  row : RowFile
  chunks : List Bytes
  deriving DecidableEq, Repr

structure State where
  rows : Nat → Option RowFile
  queue : List QueuedFile
  pending : List Reference
  writes : Nat

/-- §16.1: a single transaction publishes all four columns and the queue. -/
def attach (s : State) (rowId : Nat) (q : QueuedFile) : State :=
  { s with rows := fun r => if r = rowId then some q.row else s.rows r
           queue := q :: s.queue
           writes := s.writes + 1 }

theorem attachment_atomic (s : State) (rowId : Nat) (q : QueuedFile) :
    (attach s rowId q).rows rowId = some q.row ∧
    q ∈ (attach s rowId q).queue := by
  simp [attach]

/-- §16.5: called only after complete equal bytes have been confirmed.
Both local queue and pending record disappear in one transaction. -/
def complete (s : State) (r : Reference) : State :=
  { s with queue := s.queue.filter (fun q => q.row.reference != r)
           pending := s.pending.filter (· != r) }

def finish (s : State) (r : Reference) (expected : Bytes)
    (remote : Except ReadFailure (Option Object)) : State × Settlement :=
  let result := compareOccupied expected remote
  let state := match result with
    | .stored => complete s r
    | _ => s
  (state, result)

/-- The initiator receives errors and collisions, including the reset request. -/
theorem finish_reports_result (s : State) (r : Reference) (expected : Bytes)
    (remote : Except ReadFailure (Option Object)) :
    (finish s r expected remote).2 = compareOccupied expected remote := rfl

theorem completion_changes_no_row (s : State) (r : Reference) (expected : Bytes)
    (remote : Except ReadFailure (Option Object)) :
    (finish s r expected remote).1.rows = s.rows ∧
    (finish s r expected remote).1.writes = s.writes := by
  simp only [finish]
  split <;> exact ⟨rfl, rfl⟩

theorem completion_retires_both (s : State) (r : Reference) :
    (∀ q ∈ (complete s r).queue, q.row.reference ≠ r) ∧
    r ∉ (complete s r).pending := by
  simp [complete]

theorem completion_idempotent (s : State) (r : Reference) :
    complete (complete s r) r = complete s r := by
  simp [complete, List.filter_filter]

theorem unequal_bytes_keep_queue (s : State) (r : Reference) (expected : Bytes)
    (o : Object) (h : o.bytes ≠ expected) :
    finish s r expected (.ok (some o)) = (s, .reset) := by
  simp [finish, compareOccupied, h]

/-- An app write deleting a row: an ordinary recorded write. The upload queue
keeps its own captured reference and source facts (§16.5), so nothing here
touches the queue or pending records. -/
def deleteRow (s : State) (rowId : Nat) : State :=
  { s with rows := fun r => if r = rowId then none else s.rows r
           writes := s.writes + 1 }

theorem delete_keeps_uploads (s : State) (rowId : Nat) :
    (deleteRow s rowId).queue = s.queue ∧ (deleteRow s rowId).pending = s.pending := ⟨rfl, rfl⟩

theorem delete_only_that_row (s : State) (rowId other : Nat) (h : other ≠ rowId) :
    (deleteRow s rowId).rows rowId = none ∧ (deleteRow s rowId).rows other = s.rows other := by
  simp [deleteRow, h]

/-- What the app may do while an upload is in flight: attach files to rows,
including the uploading file's own row, and delete rows. -/
inductive Step where
  | attach (rowId : Nat) (q : QueuedFile)
  | delete (rowId : Nat)

def Step.apply (s : State) : Step → State
  | .attach rowId q => Files.attach s rowId q
  | .delete rowId => deleteRow s rowId

def runSteps (s : State) (steps : List Step) : State := steps.foldl Step.apply s

/-- App writes never drop a queued upload or its pending record. -/
theorem steps_keep_upload (s : State) (steps : List Step) (q : QueuedFile)
    (hq : q ∈ s.queue) :
    q ∈ (runSteps s steps).queue ∧ (runSteps s steps).pending = s.pending := by
  induction steps generalizing s with
  | nil => exact ⟨hq, rfl⟩
  | cons step rest ih =>
    have hstep : q ∈ (Step.apply s step).queue ∧ (Step.apply s step).pending = s.pending := by
      cases step with
      | attach rowId q' => exact ⟨List.mem_cons_of_mem _ hq, rfl⟩
      | delete rowId => exact ⟨hq, rfl⟩
    obtain ⟨hq', hp'⟩ := ih (Step.apply s step) hstep.1
    exact ⟨hq', hp'.trans hstep.2⟩

/-- An upload taken from the queue, then any attachments and row deletions,
then its settlement. The settlement writes no row, so a deleted row stays
deleted and a retargeted row keeps its new file. Only a stored result retires
anything, and then exactly the entries with this upload's reference; any other
result keeps the upload queued. -/
theorem inflight_upload_settles (s : State) (q : QueuedFile) (hq : q ∈ s.queue)
    (steps : List Step) (expected : Bytes) (remote : Except ReadFailure (Option Object)) :
    let mid := runSteps s steps
    let settled := finish mid q.row.reference expected remote
    settled.1.rows = mid.rows ∧ settled.1.writes = mid.writes ∧
    (settled.2 = .stored →
      ∀ q' ∈ mid.queue, (q' ∈ settled.1.queue ↔ q'.row.reference ≠ q.row.reference)) ∧
    (settled.2 ≠ .stored → settled.1.queue = mid.queue ∧ q ∈ settled.1.queue) := by
  intro mid settled
  have hmid := (steps_keep_upload s steps q hq).1
  refine ⟨(completion_changes_no_row mid _ expected remote).1,
    (completion_changes_no_row mid _ expected remote).2, ?_, ?_⟩
  · intro hs q' hq'
    have : settled.1 = complete mid q.row.reference := by
      simp only [settled, finish]; rw [show compareOccupied expected remote = .stored from hs]
    rw [this]
    simp [complete, hq']
  · intro hs
    have : settled.1 = mid := by
      simp only [settled, finish]
      split
      · next h => exact absurd h hs
      · rfl
    rw [this]
    exact ⟨rfl, hmid⟩

/-- D12: a file chunk's nonce is its index. The fixed key and reference,
size header and verified plaintext determine every encrypted byte. -/
structure EncryptedChunk where
  reference : Reference
  size : Nat
  index : Nat
  plaintext : Bytes
  deriving DecidableEq, Repr

/-- §16.5 models a collision-free checked chunk hash by equality with its
ghost captured bytes. No changed chunk reaches encryption. -/
def checkChunk (q : QueuedFile) (index : Nat) (source : Bytes) :
    Except SourceFailure EncryptedChunk :=
  match q.chunks[index]? with
  | none => .error .integrity
  | some original =>
    if source = original then .ok ⟨q.row.reference, q.row.size, index, source⟩
    else .error .changed

theorem encrypted_chunk_is_captured (q : QueuedFile) (i : Nat) (source : Bytes)
    (c : EncryptedChunk) (h : checkChunk q i source = .ok c) :
    q.chunks[i]? = some c.plaintext ∧ c.reference = q.row.reference ∧
    c.size = q.row.size ∧ c.index = i := by
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
