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

/-- §16.1, E8. Presence wins; failure is never interpreted as absence.
When both a device state and a source report explain absence, device state
selects the reason. Row/reference validation precedes this function. -/
def status (r : Reference) (remote : Except ReadFailure (Option Object))
    (reports : Reports) : Except ReadFailure Status :=
  match remote with
  | .error e => .error e
  | .ok (some _) => .ok .available
  | .ok none => match reports.device r.uploader with
    | .removed => .ok (.missing r.uploader .deviceRemoved)
    | .replaced => .ok (.missing r.uploader .deviceReplaced)
    | .active => match reports.unavailable r.uploader r.id with
      | some e => .ok (.missing r.uploader (.source e))
      | none => .ok (.uploading r.uploader)

/-- Only facts about this uploader and this file matter. Different local
queues, clocks, caches, pause states and unrelated reports cannot change it. -/
theorem status_agreement (r : Reference)
    (a b : Except ReadFailure (Option Object)) (ra rb : Reports)
    (hs : a = b) (hd : ra.device r.uploader = rb.device r.uploader)
    (hr : ra.unavailable r.uploader r.id = rb.unavailable r.uploader r.id) :
    status r a ra = status r b rb := by
  subst b
  cases a with
  | error e => rfl
  | ok v => cases v with
    | some o => rfl
    | none => simp only [status, hd, hr]

theorem presence_wins (r : Reference) (o : Object) (reports : Reports) :
    status r (.ok (some o)) reports = .ok .available := rfl

theorem read_error_not_missing (r : Reference) (e : ReadFailure) (reports : Reports) :
    status r (.error e) reports = .error e := rfl

theorem absent_active_uploading (r : Reference) (reports : Reports)
    (hd : reports.device r.uploader = .active)
    (hr : reports.unavailable r.uploader r.id = none) :
    status r (.ok none) reports = .ok (.uploading r.uploader) := by
  simp [status, hd, hr]

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
