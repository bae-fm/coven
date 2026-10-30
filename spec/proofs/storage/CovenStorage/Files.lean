import CovenStorage.Objects

namespace CovenStorage.Files

/-- D12: a stored file's location, fixed by the write that staged it: the
uploader, the stored object's id and the file's key. -/
structure Reference where
  uploader : Nat
  id : Nat
  key : Nat
  deriving DecidableEq, Repr

def Reference.path (r : Reference) : Path := .file r.uploader r.id

/-- §17.2, D12: where a file is, as its record's coordinates say: stored at
its location, or on one device only. -/
inductive Coordinates where
  | cloud (r : Reference)
  | device (d : Nat)
  deriving DecidableEq, Repr

/-- The stored location coordinates name, if any. -/
def Coordinates.stored : Coordinates → Option Reference
  | .cloud r => some r
  | .device _ => none

inductive SourceFailure where
  | missing | changed | integrity
  deriving DecidableEq, Repr

/-- §17.5: why a file can't be read. `lost`: storage holds nothing at the
path a record's coordinates name. `source`: a file this device uploads, on it
only until then, whose source failed. -/
inductive MissingReason where
  | lost | source (failure : SourceFailure)
  deriving DecidableEq, Repr

inductive Status where
  | available
  | uploading
  | deviceOnly (device : Nat)
  | missing (device : Nat) (reason : MissingReason)
  deriving DecidableEq, Repr

/-- Why a status call or read gives no answer. A provider failure keeps its
category. `integrity` is damage: an object at the fixed path that cannot be
this file's complete encrypted bytes. -/
inductive FileError where
  | failed (e : ReadFailure)
  | integrity
  deriving DecidableEq, Repr

/-- §17.5: a confirmed absence at the path a record's coordinates name.
Coordinates say cloud only once storage confirmed the bytes, and stored bytes
go only once no record names them (§17.9), so nothing on any device explains
the absence: storage lost it. -/
def absence (r : Reference) : Status := .missing r.uploader .lost

/-- §17.5, E8: a stored file's status, from one observation at its path.
`size` is the file's complete encrypted size, which its record's plaintext
size determines (D12). An object of that size is available; an object of any
other size is damage, not presence or absence; a failure is never absence. -/
def status (r : Reference) (size : Nat) (remote : Except ReadFailure (Option Object)) :
    Except FileError Status :=
  match remote with
  | .error e => .error (.failed e)
  | .ok (some o) => if o.bytes.length = size then .ok .available else .error .integrity
  | .ok none => .ok (absence r)

/-- What this device knows locally about a file on one device only: `some
report` while this device uploads it, with its source failure if one is
recorded (§17.5, §17.8), and `none` for any other file. -/
abbrev Local := Option (Option SourceFailure)

/-- §17.5: a file on one device only is judged with no storage request: on
this device it is uploading, or missing with its source failure; on another
it is that device's only. -/
def deviceStatus (device : Nat) (here : Local) : Status :=
  match here with
  | some none => .uploading
  | some (some failure) => .missing device (.source failure)
  | none => .deviceOnly device

/-- §17.5, E8: a file's status from its record's coordinates. -/
def judge (c : Coordinates) (size : Nat) (remote : Except ReadFailure (Option Object))
    (here : Local) : Except FileError Status :=
  match c with
  | .cloud r => status r size remote
  | .device d => .ok (deviceStatus d here)

/-- A stored file's status depends only on what storage says at its path:
other devices' states and local reports cannot change it. -/
theorem status_agreement (r : Reference) (size : Nat)
    (a b : Except ReadFailure (Option Object)) (here here' : Local) (hs : a = b) :
    judge (.cloud r) size a here = judge (.cloud r) size b here' := by
  subst b; rfl

/-- §17.5: a file on one device only needs no storage: whatever storage
says, or fails to say, its status is the same. -/
theorem device_needs_no_storage (d size : Nat)
    (a b : Except ReadFailure (Option Object)) (here : Local) :
    judge (.device d) size a here = judge (.device d) size b here := rfl

/-- On any device but the one that has it, a file on one device only is that
device's only. -/
theorem device_only_elsewhere (d size : Nat) (remote : Except ReadFailure (Option Object)) :
    judge (.device d) size remote none = .ok (.deviceOnly d) := rfl

theorem presence_wins (r : Reference) (size : Nat) (o : Object)
    (h : o.bytes.length = size) :
    status r size (.ok (some o)) = .ok .available := by
  simp [status, h]

/-- Status reports Available exactly when the file's coordinates say cloud
and storage holds an object of its complete encrypted size there. -/
theorem available_iff (c : Coordinates) (size : Nat)
    (remote : Except ReadFailure (Option Object)) (here : Local) :
    judge c size remote here = .ok .available ↔
      ∃ r, c = .cloud r ∧ ∃ o, remote = .ok (some o) ∧ o.bytes.length = size := by
  cases c with
  | device d =>
    cases here with
    | none => simp [judge, deviceStatus]
    | some report => cases report <;> simp [judge, deviceStatus]
  | cloud r =>
    cases remote with
    | error e => simp [judge, status]
    | ok v => cases v with
      | none => simp [judge, status, absence]
      | some o => by_cases h : o.bytes.length = size <;> simp [judge, status, h]

/-- A wrong-size object is damage: never Available, and never absence. -/
theorem wrong_size_is_damage (r : Reference) (size : Nat) (o : Object)
    (h : o.bytes.length ≠ size) :
    status r size (.ok (some o)) = .error .integrity := by
  simp [status, h]

theorem read_error_not_missing (r : Reference) (size : Nat) (e : ReadFailure) :
    status r size (.error e) = .error (.failed e) := rfl

/-- §17.5: no device waits for another's upload. A confirmed absence at a
stored file's path is missing, lost by storage, whoever uploaded it. -/
theorem absence_is_lost (r : Reference) (size : Nat) :
    status r size (.ok none) = .ok (.missing r.uploader .lost) := rfl

/-- Only a file this device uploads, on it only until then, is ever
uploading. -/
theorem uploading_only_here (c : Coordinates) (size : Nat)
    (remote : Except ReadFailure (Option Object)) (here : Local)
    (h : judge c size remote here = .ok .uploading) : ∃ d, c = .device d ∧ here = some none := by
  cases c with
  | device d =>
    refine ⟨d, rfl, ?_⟩
    cases here with
    | none => simp [judge, deviceStatus] at h
    | some report => cases report <;> simp_all [judge, deviceStatus]
  | cloud r =>
    cases remote with
    | error e => simp [judge, status] at h
    | ok v => cases v with
      | none => simp [judge, status, absence] at h
      | some o => by_cases hs : o.bytes.length = size <;> simp [judge, status, hs] at h

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
the same verdict status gives (§17.5: the typed error names the uploader). -/
inductive ReadError where
  | file (e : FileError)
  | unavailable (verdict : Status)
  deriving DecidableEq, Repr

/-- §17.6: one ranged fetch of a stored file's bytes, against its complete
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
  simp only [rangeReply, hle, ↓reduceIte]
  simp [fetch, hlen, hsize]

/-- A read that meets a confirmed absence fails with exactly the verdict a
status call on the same observation returns. -/
theorem read_absence_matches_status (r : Reference) (size start stop : Nat) :
    status r size (.ok none) = .ok (absence r) ∧
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
      status r size remote = .ok .available) ∧
    (fetch r size start stop (rangeReply remote start stop) = .error (.file .integrity) ↔
      status r size remote = .error .integrity) := by
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

/-- §17.2: a file's record on this device: its id, its size, its content
hash once stored, and its coordinates. Hash tokens denote checked content,
not an implementation of SHA-256. -/
structure Record where
  id : Nat
  size : Nat
  hash : Option Nat
  coordinates : Coordinates
  deriving DecidableEq, Repr

/-- §17.8: a queued upload: the location its staging write fixed, the
checked bytes' size and hash, the captured chunks (ghost values for stating
hash soundness), and the files its completion stores, by id: the one its
write staged, until that file's record is deleted. -/
structure Upload where
  location : Reference
  size : Nat
  hash : Nat
  chunks : List Bytes
  files : List Nat
  deriving DecidableEq, Repr

/-- One device's view: `self`, the rows' references as (row, file) pairs
with at most one per row, its records, its upload queue, what storage holds,
the stored paths named by records its own unpublished writes deleted, the
paths awaiting a deletion decision (§17.9), and how many writes it made. -/
structure State where
  self : Nat
  refs : List (Nat × Nat)
  records : List Record
  queue : List Upload
  stored : List Reference
  unpublished : List Reference
  candidates : List Reference
  writes : Nat

/-- The file a row references, if any. -/
def State.file (s : State) (row : Nat) : Option Nat :=
  (s.refs.find? (·.1 == row)).map (·.2)

/-- Whether any row references `id`. -/
def State.referenced (s : State) (id : Nat) : Bool := s.refs.any (·.2 == id)

/-- Whether this device has a record of `id`. -/
def State.known (s : State) (id : Nat) : Bool := s.records.any (·.id == id)

/-- Point `row` at `file`, or at nothing. -/
def State.point (s : State) (row : Nat) (file : Option Nat) : State :=
  { s with refs := (file.map fun f => (row, f)).toList ++ s.refs.filter (·.1 != row) }

/-- §17.2: delete a record, in this device's own write. The write keeps the
stored path its coordinates name until it is published; the upload linked to
the record no longer stores it. -/
def drop (s : State) (id : Nat) : State :=
  { s with records := s.records.filter (·.id != id)
           queue := s.queue.map fun u => { u with files := u.files.filter (· != id) }
           unpublished :=
             (s.records.filter (·.id == id)).filterMap (·.coordinates.stored) ++ s.unpublished }

/-- §17.2: this device's own write pointing `row` at `file`, or at nothing,
or deleting the row. A write naming a file this device has no record of is
refused (`FileUnknown`). The file the row referenced loses that reference;
if nothing references it now, the same write deletes its record. -/
def change (s : State) (row : Nat) (file : Option Nat) : State :=
  if file.all s.known then
    let t := { s.point row file with writes := s.writes + 1 }
    match s.file row with
    | some old => if t.referenced old then t else drop t old
    | none => t
  else s

/-- §17.2, §17.8: the staging write: a new record on this device only, its
upload queued, and `row` pointed at it, in one transaction. -/
def stage (s : State) (row : Nat) (u : Upload) (id : Nat) : State :=
  change { s with records := ⟨id, u.size, none, .device s.self⟩ :: s.records
                  queue := { u with files := [id] } :: s.queue } row (some id)

/-- §17.3: staging a file on this device only: a new record on this device
only, with no upload, and `row` pointed at it, in one transaction. This
device keeps the file's bytes; nothing is stored. -/
def stageLocal (s : State) (row : Nat) (size : Nat) (id : Nat) : State :=
  change { s with records := ⟨id, size, none, .device s.self⟩ :: s.records } row (some id)

/-- §17.8: switch a record on this device only to its upload's stored
bytes. -/
def storeRecord (s : State) (u : Upload) (rec : Record) : Record :=
  if u.files.contains rec.id && rec.coordinates == .device s.self then
    { rec with coordinates := .cloud u.location, hash := some u.hash }
  else rec

/-- The files an upload still stores: those whose record is on this device
only. -/
def State.waiting (s : State) (u : Upload) : List Nat :=
  u.files.filter fun id => s.records.any fun rec => rec.id == id && rec.coordinates == .device s.self

/-- §17.8: called only after storage confirmed the complete bytes. One
transaction retires the upload. If any file still waits for it, the same
transaction is a write of this device switching each to the stored bytes;
otherwise the stored bytes, which no record names, become a deletion
candidate (§17.9). An upload no longer queued has been completed already:
nothing happens. -/
def complete (s : State) (r : Reference) : State :=
  match s.queue.find? (·.location == r) with
  | none => s
  | some u =>
    let t := { s with queue := s.queue.filter (·.location != r), stored := r :: s.stored }
    if (s.waiting u).isEmpty then { t with candidates := r :: s.candidates }
    else { t with records := s.records.map (storeRecord s u), writes := s.writes + 1 }

def finish (s : State) (r : Reference) (expected : Bytes)
    (remote : Except ReadFailure (Option Object)) : State × Settlement :=
  let result := compareOccupied expected remote
  let state := match result with
    | .stored => complete s r
    | _ => s
  (state, result)

/-- §17.8: each run of the upload worker first retires the uploads no file
waits for, except those `inFlight`, whose attempts retire them as they end.
Their paths become deletion candidates. -/
def retire (s : State) (inFlight : List Reference) : State :=
  let gone := fun (u : Upload) => u.files.isEmpty && !(inFlight.contains u.location)
  { s with queue := s.queue.filter (fun u => !gone u)
           candidates := (s.queue.filter gone).map (·.location) ++ s.candidates }

/-- Publishing this device's writes ends the protection their deleted
records' coordinates gave: those paths become deletion candidates. -/
def publish (s : State) : State :=
  { s with unpublished := [], candidates := s.unpublished ++ s.candidates }

/-- The records nothing references. -/
def State.unreferenced (s : State) : List Nat :=
  (s.records.filter fun rec => !s.referenced rec.id).map (·.id)

/-- §17.2: this device's own write deleting every record nothing references,
which other devices' writes left so: two of them each removed one of a file's
references, neither seeing the other. -/
def settle (s : State) : State :=
  if (s.unreferenced).isEmpty then s
  else { s.unreferenced.foldl drop s with writes := s.writes + 1 }

/-- §17.3: switch a record named in `ids` to this device only, off its stored
bytes. -/
def localRecord (self : Nat) (ids : List Nat) (rec : Record) : Record :=
  if ids.contains rec.id then { rec with coordinates := .device self, hash := none } else rec

/-- Whether this device has a record of `id` whose coordinates say cloud. -/
def State.inCloud (s : State) (id : Nat) : Bool :=
  s.records.any fun rec => rec.id == id && rec.coordinates.stored.isSome

/-- §17.3: this device's one write making the cloud files `ids` device-only
here: every one switches, or, if any isn't a cloud file this device has a
record of, none does and nothing is written. The write keeps their stored
paths until it is published, as a delete does (§17.9). -/
def makeLocal (s : State) (ids : List Nat) : State :=
  if ids.all s.inCloud then
    { s with records := s.records.map (localRecord s.self ids)
             unpublished :=
               (s.records.filter fun rec => ids.contains rec.id).filterMap (·.coordinates.stored)
                 ++ s.unpublished
             writes := s.writes + 1 }
  else s

/-- Another device's switch of record `id` to device `d` only. -/
def leaveAt (id d : Nat) (rec : Record) : Record :=
  if rec.id == id then { rec with coordinates := .device d, hash := none } else rec

/-- What a device may do while uploads are in flight: stage files, make
writes, apply other devices' writes, retire uploads, complete uploads,
publish, and settle what other devices left. Another device's write
switching a record to the cloud, or recording a new file in the cloud,
names bytes storage already holds, since that device's completion confirmed
them; a step naming bytes storage doesn't hold is not a write any device
makes, so it changes nothing. -/
inductive Step where
  | stage (row : Nat) (u : Upload) (id : Nat)
  | stageLocal (row : Nat) (size : Nat) (id : Nat)
  | change (row : Nat) (file : Option Nat)
  /-- Another device's write pointing a row at a file, or at nothing. A
  reference to a file this device has no record of is taken out, as a foreign
  key's child is when its parent is gone (§9.1). -/
  | point (row : Nat) (file : Option Nat)
  /-- Another device's new record. -/
  | record (rec : Record)
  /-- Another device's write switching a file on that device only to stored
  bytes. -/
  | store (id : Nat) (r : Reference) (hash : Nat)
  /-- Another device's published delete of a record. -/
  | delete (id : Nat)
  /-- This device's write making cloud files device-only here (§17.3). -/
  | makeLocal (ids : List Nat)
  /-- Another device's published write making a file device-only on device
  `d` (§17.3). -/
  | leave (id : Nat) (d : Nat)
  | retire (inFlight : List Reference)
  | complete (r : Reference)
  | publish
  | settle

/-- Another device's switch of its file `id`, on that device only, to the
stored bytes at `r`. -/
def storeAt (id : Nat) (r : Reference) (hash : Nat) (rec : Record) : Record :=
  if rec.id == id && rec.coordinates.stored.isNone then
    { rec with coordinates := .cloud r, hash := some hash }
  else rec

def Step.apply (s : State) : Step → State
  | .stage row u id => if s.known id then s else Files.stage s row u id
  | .stageLocal row size id => if s.known id then s else Files.stageLocal s row size id
  | .change row file => Files.change s row file
  | .point row file => s.point row (file.filter s.known)
  | .record rec =>
    if s.known rec.id || rec.coordinates.stored.any (fun r => !s.stored.contains r) then s
    else { s with records := rec :: s.records }
  | .store id r hash =>
    if s.stored.contains r then { s with records := s.records.map (storeAt id r hash) } else s
  | .delete id =>
    { s with records := s.records.filter (·.id != id)
             queue := s.queue.map fun u => { u with files := u.files.filter (· != id) }
             candidates :=
               (s.records.filter (·.id == id)).filterMap (·.coordinates.stored) ++ s.candidates }
  | .makeLocal ids => Files.makeLocal s ids
  | .leave id d =>
    { s with records := s.records.map (leaveAt id d)
             candidates :=
               (s.records.filter (·.id == id)).filterMap (·.coordinates.stored) ++ s.candidates }
  | .retire inFlight => Files.retire s inFlight
  | .complete r => Files.complete s r
  | .publish => Files.publish s
  | .settle => Files.settle s

def runSteps (s : State) (steps : List Step) : State := steps.foldl Step.apply s

/-- Every file whose coordinates say cloud is in storage: coordinates say
cloud only once storage confirmed the bytes (§17.2, §17.8). -/
def CloudStored (s : State) : Prop :=
  ∀ rec ∈ s.records, ∀ r, rec.coordinates = .cloud r → r ∈ s.stored

/-- Nothing in storage is beyond deletion's reach: every stored path is
named by a record's coordinates, held by the upload queue, kept by an
unpublished write of this device, or a deletion candidate (§17.9). -/
def Reachable (s : State) : Prop :=
  ∀ r ∈ s.stored, (∃ rec ∈ s.records, rec.coordinates = .cloud r) ∨
    (∃ u ∈ s.queue, u.location = r) ∨ r ∈ s.unpublished ∨ r ∈ s.candidates

theorem drop_parts (s : State) (id : Nat) :
    (drop s id).stored = s.stored ∧ (drop s id).candidates = s.candidates ∧
    (drop s id).writes = s.writes ∧ (drop s id).refs = s.refs ∧
    (drop s id).self = s.self := ⟨rfl, rfl, rfl, rfl, rfl⟩

theorem drop_records (s : State) (id : Nat) (rec : Record) :
    rec ∈ (drop s id).records ↔ rec ∈ s.records ∧ rec.id ≠ id := by
  simp [drop]

theorem drop_cloud_stored (s : State) (id : Nat) (h : CloudStored s) :
    CloudStored (drop s id) := by
  intro rec hrec r hc
  rw [(drop_parts s id).1]
  exact h rec ((drop_records s id rec).mp hrec).1 r hc

/-- A deleted record's stored path moves to the unpublished write's keeping. -/
theorem drop_reachable (s : State) (id : Nat) (h : Reachable s) : Reachable (drop s id) := by
  intro x hx
  rw [(drop_parts s id).1] at hx
  rcases h x hx with ⟨rec, hrec, hc⟩ | ⟨u, hu, he⟩ | hp | hc
  · by_cases e : rec.id = id
    · right; right; left
      simp only [drop, List.mem_append, List.mem_filterMap, List.mem_filter]
      left; exact ⟨rec, ⟨hrec, by simp [e]⟩, by simp [hc, Coordinates.stored]⟩
    · left; exact ⟨rec, (drop_records s id rec).mpr ⟨hrec, e⟩, hc⟩
  · right; left
    exact ⟨{ u with files := u.files.filter (· != id) },
      List.mem_map_of_mem (f := fun u : Upload => { u with files := u.files.filter (· != id) }) hu, he⟩
  · right; right; left; simp [drop, hp]
  · right; right; right; simpa [drop] using hc

theorem point_parts (s : State) (row : Nat) (file : Option Nat) :
    (s.point row file).records = s.records ∧ (s.point row file).queue = s.queue ∧
    (s.point row file).stored = s.stored ∧ (s.point row file).unpublished = s.unpublished ∧
    (s.point row file).candidates = s.candidates ∧ (s.point row file).self = s.self :=
  ⟨rfl, rfl, rfl, rfl, rfl, rfl⟩

/-- §17.2: a write changes records only by deleting those it left with no
reference: `change` is a pointing write, then possibly one `drop`. -/
theorem change_cases (s : State) (row : Nat) (file : Option Nat) :
    change s row file = s ∨ change s row file = { s.point row file with writes := s.writes + 1 } ∨
      ∃ old, change s row file = drop { s.point row file with writes := s.writes + 1 } old := by
  unfold change
  split
  · cases s.file row with
    | none => right; left; rfl
    | some old =>
      simp only
      split
      · right; left; rfl
      · right; right; exact ⟨old, rfl⟩
  · left; rfl

theorem change_cloud_stored (s : State) (row : Nat) (file : Option Nat) (h : CloudStored s) :
    CloudStored (change s row file) := by
  have hp : CloudStored { s.point row file with writes := s.writes + 1 } := h
  rcases change_cases s row file with e | e | ⟨old, e⟩ <;> rw [e]
  · exact h
  · exact hp
  · exact drop_cloud_stored _ old hp

theorem change_reachable (s : State) (row : Nat) (file : Option Nat) (h : Reachable s) :
    Reachable (change s row file) := by
  have hp : Reachable { s.point row file with writes := s.writes + 1 } := h
  rcases change_cases s row file with e | e | ⟨old, e⟩ <;> rw [e]
  · exact h
  · exact hp
  · exact drop_reachable _ old hp

/-- §17.9: no write of this device makes a path a deletion candidate: the
bytes of a record it deletes stay until that write is published. -/
theorem change_keeps_candidates (s : State) (row : Nat) (file : Option Nat) :
    (change s row file).candidates = s.candidates ∧ (change s row file).stored = s.stored := by
  rcases change_cases s row file with e | e | ⟨old, e⟩ <;> rw [e] <;> exact ⟨rfl, rfl⟩

/-- Publishing ends exactly the protection the published writes gave. -/
theorem publish_releases (s : State) :
    (publish s).candidates = s.unpublished ++ s.candidates ∧ (publish s).unpublished = [] ∧
      (publish s).records = s.records := ⟨rfl, rfl, rfl⟩

theorem find_skip (l : List (Nat × Nat)) (row x : Nat) (e : x ≠ row) :
    (l.filter (·.1 != row)).find? (·.1 == x) = l.find? (·.1 == x) := by
  rw [List.find?_filter]
  congr 1; funext a
  by_cases h : a.1 = x
  · simp [h, e]
  · simp [h]

theorem point_file (s : State) (row x : Nat) (file : Option Nat) :
    (s.point row file).file x = if x = row then file else s.file x := by
  by_cases e : x = row
  · subst e
    cases file with
    | none => simp [State.point, State.file]
    | some f => simp [State.point, State.file]
  · cases file with
    | none => simp [State.point, State.file, e, find_skip _ _ _ e]
    | some f => simp [State.point, State.file, e, Ne.symm e, find_skip _ _ _ e]

theorem point_referenced (s : State) (row id : Nat) (file : Option Nat)
    (h : ∃ x, x ≠ row ∧ (x, id) ∈ s.refs) : (s.point row file).referenced id = true := by
  obtain ⟨x, hx, hm⟩ := h
  simp only [State.point, State.referenced, List.any_append, List.any_eq_true, List.mem_filter,
    Bool.or_eq_true]
  right; exact ⟨(x, id), ⟨hm, by simp [hx]⟩, by simp⟩

/-- §17.2: two rows share a file, and it stays until both go: a write
removing one row's reference keeps the record while another row's remains. -/
theorem shared_file_stays (s : State) (row other id : Nat) (file : Option Nat)
    (hother : other ≠ row) (href : (other, id) ∈ s.refs) (rec : Record)
    (hrec : rec ∈ s.records) (hid : rec.id = id) : rec ∈ (change s row file).records := by
  unfold change
  split
  · cases hf : s.file row with
    | none => exact hrec
    | some old =>
      simp only
      split
      · exact hrec
      · rename_i hnot
        have : old ≠ id := by
          intro e; subst e
          exact hnot (point_referenced s row old file ⟨other, hother, href⟩)
        exact (drop_records _ old rec).mpr ⟨hrec, by rw [hid]; exact Ne.symm this⟩
  · exact hrec

/-- §17.2: the write removing a file's last reference deletes its record in
the same transaction. -/
theorem last_reference_deletes (s : State) (row id : Nat) (file : Option Nat)
    (hfile : file.all s.known = true) (hold : s.file row = some id)
    (hlast : (s.point row file).referenced id = false) :
    (change s row file).known id = false := by
  simp only [change, hfile, ↓reduceIte, hold]
  have : ({ s.point row file with writes := s.writes + 1 } : State).referenced id = false := hlast
  simp only [this, Bool.false_eq_true, ↓reduceIte]
  simp [drop, State.known]

/-- §17.2, §9.1: another device's late write naming a file whose record this
device deleted references nothing: the row is taken out like a foreign key's
child. -/
theorem late_reference_taken_out (s : State) (row id : Nat) (hgone : s.known id = false) :
    (Step.apply s (.point row (some id))).file row = none := by
  simp only [Step.apply, Option.filter, hgone]
  rw [point_file]; simp

/-- §17.2: staging writes no cloud coordinates: every record with cloud
coordinates after staging was there before with them. -/
theorem stage_writes_no_cloud (s : State) (row : Nat) (u : Upload) (id : Nat) :
    ∀ rec ∈ (stage s row u id).records, ∀ r, rec.coordinates = .cloud r → rec ∈ s.records := by
  intro rec hrec r hc
  let t : State := { s with records := ⟨id, u.size, none, .device s.self⟩ :: s.records
                            queue := { u with files := [id] } :: s.queue }
  have hin : rec ∈ t.records := by
    rcases change_cases t row (some id) with e | e | ⟨old, e⟩
    · rw [stage, e] at hrec; exact hrec
    · rw [stage, e] at hrec; exact hrec
    · rw [stage, e] at hrec; exact ((drop_records _ old rec).mp hrec).1
  simp only [t, List.mem_cons] at hin
  rcases hin with e | e
  · subst e; simp at hc
  · exact e

theorem stage_cloud_stored (s : State) (row : Nat) (u : Upload) (id : Nat) (h : CloudStored s) :
    CloudStored (stage s row u id) := by
  apply change_cloud_stored
  intro rec hrec r hc
  simp only [List.mem_cons] at hrec
  rcases hrec with e | e
  · subst e; simp at hc
  · exact h rec e r hc

theorem stage_reachable (s : State) (row : Nat) (u : Upload) (id : Nat) (h : Reachable s) :
    Reachable (stage s row u id) := by
  apply change_reachable
  intro x hx
  rcases h x hx with ⟨rec, hrec, hc⟩ | ⟨v, hv, he⟩ | hp | hc
  · left; exact ⟨rec, List.mem_cons_of_mem _ hrec, hc⟩
  · right; left; exact ⟨v, List.mem_cons_of_mem _ hv, he⟩
  · right; right; left; exact hp
  · right; right; right; exact hc

/-- §17.3: staging on this device only writes no cloud coordinates either. -/
theorem stage_local_writes_no_cloud (s : State) (row size id : Nat) :
    ∀ rec ∈ (stageLocal s row size id).records, ∀ r, rec.coordinates = .cloud r →
      rec ∈ s.records := by
  intro rec hrec r hc
  let t : State := { s with records := ⟨id, size, none, .device s.self⟩ :: s.records }
  have hin : rec ∈ t.records := by
    rcases change_cases t row (some id) with e | e | ⟨old, e⟩
    · rw [stageLocal, e] at hrec; exact hrec
    · rw [stageLocal, e] at hrec; exact hrec
    · rw [stageLocal, e] at hrec; exact ((drop_records _ old rec).mp hrec).1
  simp only [t, List.mem_cons] at hin
  rcases hin with e | e
  · subst e; simp at hc
  · exact e

theorem stage_local_cloud_stored (s : State) (row size id : Nat) (h : CloudStored s) :
    CloudStored (stageLocal s row size id) := by
  apply change_cloud_stored
  intro rec hrec r hc
  simp only [List.mem_cons] at hrec
  rcases hrec with e | e
  · subst e; simp at hc
  · exact h rec e r hc

theorem stage_local_reachable (s : State) (row size id : Nat) (h : Reachable s) :
    Reachable (stageLocal s row size id) := by
  apply change_reachable
  intro x hx
  rcases h x hx with ⟨rec, hrec, hc⟩ | hq | hp | hc
  · left; exact ⟨rec, List.mem_cons_of_mem _ hrec, hc⟩
  · right; left; exact hq
  · right; right; left; exact hp
  · right; right; right; exact hc

theorem complete_unqueued (s : State) (r : Reference)
    (hq : s.queue.find? (·.location == r) = none) : complete s r = s := by
  simp [complete, hq]

/-- The completing transaction, by cases: nothing queued; a write switching
the waiting files; or no write, the path becoming a deletion candidate. -/
theorem complete_cases (s : State) (r : Reference) :
    complete s r = s ∨
    ∃ u, s.queue.find? (·.location == r) = some u ∧ u.location = r ∧
      (complete s r).queue = s.queue.filter (·.location != r) ∧
      (complete s r).stored = r :: s.stored ∧
      (complete s r).unpublished = s.unpublished ∧ (complete s r).refs = s.refs ∧
      (complete s r).self = s.self ∧
      (((s.waiting u).isEmpty = true ∧ (complete s r).records = s.records ∧
          (complete s r).candidates = r :: s.candidates ∧ (complete s r).writes = s.writes) ∨
        ((s.waiting u).isEmpty = false ∧
          (complete s r).records = s.records.map (storeRecord s u) ∧
          (complete s r).candidates = s.candidates ∧ (complete s r).writes = s.writes + 1)) := by
  cases hq : s.queue.find? (·.location == r) with
  | none => left; exact complete_unqueued s r hq
  | some u =>
    right
    have hr : u.location = r := by simpa using List.find?_some hq
    refine ⟨u, rfl, hr, ?_⟩
    by_cases hw : (s.waiting u).isEmpty = true
    · simp [complete, hq, hw]
    · have hw' : (s.waiting u).isEmpty = false := by simpa using hw
      simp [complete, hq, hw']

/-- §17.8: a completed upload is no longer queued, and completing it again
changes nothing. With the records' switch in the same transaction, a retried
completion stores each file exactly once. -/
theorem completion_idempotent (s : State) (r : Reference) :
    complete (complete s r) r = complete s r := by
  rcases complete_cases s r with e | ⟨u, _, _, hq, _⟩
  · rw [e, e]
  · apply complete_unqueued
    rw [hq]
    apply List.find?_eq_none.mpr
    intro x hx; simp at hx; simp [hx.2]

theorem completion_retires_queue (s : State) (r : Reference) :
    ∀ u ∈ (complete s r).queue, u.location ≠ r := by
  cases hq : s.queue.find? (·.location == r) with
  | none =>
    rw [complete_unqueued s r hq]
    intro u hu he
    have := List.find?_eq_none.mp hq u hu
    simp [he] at this
  | some u =>
    intro x hx
    simp only [complete, hq] at hx
    split at hx <;> (simp at hx; exact hx.2)

/-- §17.2: an upload no file waits for any more, because each was deleted or
already stored, completes without a write and changes no record. -/
theorem stale_completion_writes_nothing (s : State) (r : Reference)
    (h : ∀ u, s.queue.find? (·.location == r) = some u → (s.waiting u).isEmpty = true) :
    (complete s r).records = s.records ∧ (complete s r).writes = s.writes := by
  rcases complete_cases s r with e | ⟨u, hu, _, _, _, _, _, _, hcase⟩
  · rw [e]; exact ⟨rfl, rfl⟩
  · rcases hcase with ⟨_, hrec, _, hw⟩ | ⟨hne, _⟩
    · exact ⟨hrec, hw⟩
    · rw [h u hu] at hne; contradiction

/-- §17.8: completion switches each file still waiting for the upload to
the stored bytes, which storage holds, in one write of this device. -/
theorem completion_stores_waiting (s : State) (r : Reference) (u : Upload)
    (hq : s.queue.find? (·.location == r) = some u) (rec : Record) (hrec : rec ∈ s.records)
    (hlinked : rec.id ∈ u.files) (hdevice : rec.coordinates = .device s.self) :
    { rec with coordinates := .cloud r, hash := some u.hash } ∈ (complete s r).records ∧
      r ∈ (complete s r).stored ∧ (complete s r).writes = s.writes + 1 := by
  have hr : u.location = r := by simpa using List.find?_some hq
  have hwait : rec.id ∈ s.waiting u := by
    simp only [State.waiting, List.mem_filter, List.any_eq_true, Bool.and_eq_true, beq_iff_eq]
    exact ⟨hlinked, rec, hrec, rfl, hdevice⟩
  have hne : (s.waiting u).isEmpty = false := by
    cases hw : s.waiting u with
    | nil => rw [hw] at hwait; simp at hwait
    | cons _ _ => rfl
  rcases complete_cases s r with e | ⟨v, hv, _, _, hst, _, _, _, hcase⟩
  · simp [complete, hq, hne] at e
    have := congrArg State.writes e
    simp at this
  · have hvu : v = u := by rw [hq] at hv; exact (Option.some.inj hv).symm
    subst hvu
    rcases hcase with ⟨hempty, _⟩ | ⟨_, hrec', _, hw⟩
    · rw [hne] at hempty; contradiction
    · refine ⟨?_, by rw [hst]; exact List.mem_cons_self .., hw⟩
      rw [hrec']
      exact List.mem_map.mpr ⟨rec, hrec, by simp [storeRecord, hlinked, hdevice, hr]⟩

/-- An attempt that didn't confirm the bytes changes nothing: a crash or a
failure before the completing transaction leaves the upload queued. -/
theorem unconfirmed_changes_nothing (s : State) (r : Reference) (expected : Bytes)
    (remote : Except ReadFailure (Option Object))
    (h : compareOccupied expected remote ≠ .stored) :
    (finish s r expected remote).1 = s := by
  unfold finish
  cases hr : compareOccupied expected remote <;> simp_all

/-- The initiator receives errors and collisions; a mismatch refuses this
file attempt. -/
theorem finish_reports_result (s : State) (r : Reference) (expected : Bytes)
    (remote : Except ReadFailure (Option Object)) :
    (finish s r expected remote).2 = compareOccupied expected remote := rfl

theorem unequal_bytes_keep_queue (s : State) (r : Reference) (expected : Bytes)
    (o : Object) (h : o.bytes ≠ expected) :
    finish s r expected (.ok (some o)) = (s, .occupied) := by
  simp [finish, compareOccupied, h]

/-- §17.2: once a write of this device leaves a staged file with no
reference, that write deletes it, and its upload's completion then stores
nothing and makes no write. -/
theorem replaced_upload_writes_nothing (s : State) (row id : Nat) (file : Option Nat)
    (r : Reference) (hfile : file.all s.known = true) (hold : s.file row = some id)
    (hlast : (s.point row file).referenced id = false)
    (honly : ∀ u ∈ s.queue, u.location = r → u.files = [id]) :
    let t := change s row file
    (complete t r).records = t.records ∧ (complete t r).writes = t.writes := by
  intro t
  apply stale_completion_writes_nothing
  intro u hu
  have hmem := List.mem_of_find?_eq_some hu
  have hr : u.location = r := by simpa using List.find?_some hu
  -- `t` is the drop of `id`, which took it out of every upload
  have ht : t = drop { s.point row file with writes := s.writes + 1 } id := by
    have : ({ s.point row file with writes := s.writes + 1 } : State).referenced id = false := hlast
    simp only [t, change, hfile, ↓reduceIte, hold, this, Bool.false_eq_true]
  rw [ht] at hmem
  simp only [drop, List.mem_map] at hmem
  obtain ⟨v, hv, rfl⟩ := hmem
  have hv' : v ∈ s.queue := hv
  rw [honly v hv' (by simpa using hr)]
  simp [State.waiting]

theorem retire_parts (s : State) (inFlight : List Reference) :
    (retire s inFlight).records = s.records ∧ (retire s inFlight).stored = s.stored ∧
    (retire s inFlight).unpublished = s.unpublished ∧ (retire s inFlight).self = s.self :=
  ⟨rfl, rfl, rfl, rfl⟩

theorem settle_parts (s : State) :
    ((settle s).stored = s.stored ∧ (settle s).candidates = s.candidates) ∧
      (CloudStored s → CloudStored (settle s)) ∧ (Reachable s → Reachable (settle s)) := by
  unfold settle
  split
  · exact ⟨⟨rfl, rfl⟩, id, id⟩
  · generalize s.unreferenced = ids
    suffices ∀ (t : State), ((ids.foldl drop t).stored = t.stored ∧
        (ids.foldl drop t).candidates = t.candidates) ∧
        (CloudStored t → CloudStored (ids.foldl drop t)) ∧
        (Reachable t → Reachable (ids.foldl drop t)) from this s
    induction ids with
    | nil => intro t; exact ⟨⟨rfl, rfl⟩, id, id⟩
    | cons id rest ih =>
      intro t
      obtain ⟨⟨hs, hc⟩, hcs, hr⟩ := ih (drop t id)
      exact ⟨⟨hs.trans (drop_parts t id).1, hc.trans (drop_parts t id).2.1⟩,
        fun h => hcs (drop_cloud_stored t id h), fun h => hr (drop_reachable t id h)⟩

/-- §17.9: settling what other devices left makes no path a candidate: the
records it deletes keep their bytes until its write is published. -/
theorem settle_keeps_candidates (s : State) : (settle s).candidates = s.candidates :=
  (settle_parts s).1.2

/-- §17.3: making files device-only is one write: either every file it
names is on this device only afterwards, or nothing changed. -/
theorem make_local_whole (s : State) (ids : List Nat) :
    makeLocal s ids = s ∨
      ∀ rec ∈ (makeLocal s ids).records, ids.contains rec.id → rec.coordinates = .device s.self := by
  unfold makeLocal
  split
  · right
    intro rec hrec hid
    simp only [List.mem_map] at hrec
    obtain ⟨y, _, rfl⟩ := hrec
    by_cases hy : y.id ∈ ids
    · simp [localRecord, hy]
    · have same : localRecord s.self ids y = y := by simp [localRecord, hy]
      rw [same] at hid
      simp [hy] at hid
  · left; rfl

/-- §17.9: making files device-only makes no path a deletion candidate: the
bytes it takes the files off stay until its write is published, which
releases them. -/
theorem make_local_keeps_candidates (s : State) (ids : List Nat) :
    (makeLocal s ids).candidates = s.candidates ∧ (makeLocal s ids).stored = s.stored ∧
      ∀ rec ∈ s.records, ids.all s.inCloud = true → ids.contains rec.id = true →
        ∀ r, rec.coordinates = .cloud r → r ∈ (makeLocal s ids).unpublished := by
  refine ⟨?_, ?_, ?_⟩
  · unfold makeLocal; split <;> rfl
  · unfold makeLocal; split <;> rfl
  · intro rec hrec hall hid r hc
    simp only [makeLocal, hall, ↓reduceIte, List.mem_append, List.mem_filterMap, List.mem_filter]
    left; exact ⟨rec, ⟨hrec, hid⟩, by simp [hc, Coordinates.stored]⟩

theorem make_local_cloud_stored (s : State) (ids : List Nat) (h : CloudStored s) :
    CloudStored (makeLocal s ids) := by
  unfold makeLocal
  split
  · intro x hx r hc
    simp only [List.mem_map] at hx
    obtain ⟨y, hy, rfl⟩ := hx
    unfold localRecord at hc
    split at hc
    · simp at hc
    · exact h y hy r hc
  · exact h

theorem make_local_reachable (s : State) (ids : List Nat) (h : Reachable s) :
    Reachable (makeLocal s ids) := by
  unfold makeLocal
  split
  · intro x hx
    rcases h x hx with ⟨y, hy, hc⟩ | hq | hp | hcand
    · by_cases e : ids.contains y.id = true
      · right; right; left
        simp only [List.mem_append, List.mem_filterMap, List.mem_filter]
        left; exact ⟨y, ⟨hy, e⟩, by simp [hc, Coordinates.stored]⟩
      · left
        have e' : y.id ∉ ids := by simpa using e
        refine ⟨localRecord s.self ids y, List.mem_map_of_mem hy, ?_⟩
        simp [localRecord, e', hc]
    · right; left; exact hq
    · right; right; left; simp [hp]
    · right; right; right; exact hcand
  · exact h

/-- §17.2, §17.8: every step keeps every file whose coordinates say cloud in
storage. Deleting stored bytes is outside these steps; §17.9 allows it only
for bytes no record names. -/
theorem step_cloud_stored (s : State) (st : Step) (h : CloudStored s) :
    CloudStored (Step.apply s st) := by
  cases st with
  | stage row u id =>
    simp only [Step.apply]; split
    · exact h
    · exact stage_cloud_stored s row u id h
  | stageLocal row size id =>
    simp only [Step.apply]; split
    · exact h
    · exact stage_local_cloud_stored s row size id h
  | change row file => exact change_cloud_stored s row file h
  | point row file => exact h
  | record rec =>
    simp only [Step.apply]; split
    · exact h
    · rename_i hnew
      intro x hx r hc
      simp only [List.mem_cons] at hx
      rcases hx with e | e
      · subst e
        simp only [Bool.or_eq_true, not_or] at hnew
        have : ¬ (x.coordinates.stored.any fun r => !s.stored.contains r) = true := hnew.2
        rw [hc] at this
        simpa [Coordinates.stored] using this
      · exact h x e r hc
  | store id r hash =>
    simp only [Step.apply]; split
    · rename_i hs
      intro x hx r' hc
      simp only [List.mem_map] at hx
      obtain ⟨y, hy, rfl⟩ := hx
      unfold storeAt at hc
      split at hc
      · simp at hc; subst hc; simpa using hs
      · exact h y hy r' hc
    · exact h
  | delete id =>
    intro x hx r hc
    simp only [Step.apply, List.mem_filter] at hx
    exact h x hx.1 r hc
  | makeLocal ids => exact make_local_cloud_stored s ids h
  | leave id d =>
    intro x hx r hc
    simp only [Step.apply, List.mem_map] at hx
    obtain ⟨y, hy, rfl⟩ := hx
    unfold leaveAt at hc
    split at hc
    · simp at hc
    · exact h y hy r hc
  | retire inFlight => exact h
  | complete r =>
    show CloudStored (complete s r)
    rcases complete_cases s r with e | ⟨u, _, hr, _, hst, _, _, hself, hcase⟩
    · rw [e]; exact h
    · rcases hcase with ⟨_, hrec, _, _⟩ | ⟨_, hrec, _, _⟩
      · intro x hx r' hc; rw [hst]; rw [hrec] at hx
        exact List.mem_cons_of_mem _ (h x hx r' hc)
      · intro x hx r' hc; rw [hst]; rw [hrec] at hx
        simp only [List.mem_map] at hx
        obtain ⟨y, hy, rfl⟩ := hx
        unfold storeRecord at hc
        split at hc
        · simp at hc; subst hc; simp [hr]
        · exact List.mem_cons_of_mem _ (h y hy r' hc)
  | publish => exact h
  | settle => exact (settle_parts s).2.1 h

theorem steps_cloud_stored (s : State) (steps : List Step) (h : CloudStored s) :
    CloudStored (runSteps s steps) := by
  induction steps generalizing s with
  | nil => exact h
  | cons st rest ih => exact ih _ (step_cloud_stored s st h)

/-- §17.9: every step keeps every stored path within deletion's reach: a
retired upload's path becomes a candidate in the transaction that ends the
queue's protection, a record this device deletes keeps its path until that
write is published, and another device's published delete releases it. -/
theorem step_reachable (s : State) (st : Step) (h : Reachable s) :
    Reachable (Step.apply s st) := by
  cases st with
  | stage row u id =>
    simp only [Step.apply]; split
    · exact h
    · exact stage_reachable s row u id h
  | stageLocal row size id =>
    simp only [Step.apply]; split
    · exact h
    · exact stage_local_reachable s row size id h
  | change row file => exact change_reachable s row file h
  | point row file => exact h
  | record rec =>
    simp only [Step.apply]; split
    · exact h
    · intro x hx
      rcases h x hx with ⟨y, hy, hc⟩ | hq | hp | hc
      · left; exact ⟨y, List.mem_cons_of_mem _ hy, hc⟩
      · right; left; exact hq
      · right; right; left; exact hp
      · right; right; right; exact hc
  | store id r hash =>
    simp only [Step.apply]; split
    · intro x hx
      rcases h x hx with ⟨y, hy, hc⟩ | hq | hp | hc
      · left
        refine ⟨storeAt id r hash y, List.mem_map_of_mem hy, ?_⟩
        unfold storeAt
        split
        · rename_i hdev
          simp only [Bool.and_eq_true] at hdev
          rw [hc] at hdev; simp [Coordinates.stored] at hdev
        · exact hc
      · right; left; exact hq
      · right; right; left; exact hp
      · right; right; right; exact hc
    · exact h
  | delete id =>
    intro x hx
    rcases h x hx with ⟨y, hy, hc⟩ | ⟨u, hu, he⟩ | hp | hc
    · by_cases e : y.id = id
      · right; right; right
        simp only [Step.apply, List.mem_append, List.mem_filterMap, List.mem_filter]
        left; exact ⟨y, ⟨hy, by simp [e]⟩, by simp [hc, Coordinates.stored]⟩
      · left; exact ⟨y, by simp [Step.apply, hy, e], hc⟩
    · right; left
      exact ⟨{ u with files := u.files.filter (· != id) },
        List.mem_map_of_mem (f := fun u : Upload => { u with files := u.files.filter (· != id) }) hu, he⟩
    · right; right; left; exact hp
    · right; right; right; simp [Step.apply, hc]
  | makeLocal ids => exact make_local_reachable s ids h
  | leave id d =>
    intro x hx
    rcases h x hx with ⟨y, hy, hc⟩ | hq | hp | hcand
    · by_cases e : y.id = id
      · right; right; right
        simp only [Step.apply, List.mem_append, List.mem_filterMap, List.mem_filter]
        left; exact ⟨y, ⟨hy, by simp [e]⟩, by simp [hc, Coordinates.stored]⟩
      · left
        refine ⟨leaveAt id d y, by simp only [Step.apply]; exact List.mem_map_of_mem hy, ?_⟩
        simp [leaveAt, e, hc]
    · right; left; exact hq
    · right; right; left; exact hp
    · right; right; right; simp [Step.apply, hcand]
  | retire inFlight =>
    show Reachable (retire s inFlight)
    intro x hx
    rcases h x hx with ⟨y, hy, hc⟩ | ⟨u, hu, he⟩ | hp | hc
    · left; exact ⟨y, hy, hc⟩
    · by_cases hg : (u.files.isEmpty && !(inFlight.contains u.location)) = true
      · right; right; right
        simp only [retire, List.mem_append, List.mem_map, List.mem_filter]
        left; exact ⟨u, ⟨hu, hg⟩, he⟩
      · right; left
        refine ⟨u, ?_, he⟩
        have hg' := Bool.eq_false_iff.mpr hg
        exact List.mem_filter.mpr ⟨hu, by simp only [hg', Bool.not_false]⟩
    · right; right; left; exact hp
    · right; right; right; simp [retire, hc]
  | complete r =>
    show Reachable (complete s r)
    rcases complete_cases s r with e | ⟨u, hu, hr, hq, hst, hun, _, hself, hcase⟩
    · rw [e]; exact h
    · intro x hx
      rw [hst] at hx
      have queued : ∀ v ∈ s.queue, v.location = x → x ≠ r →
          ∃ v ∈ (complete s r).queue, v.location = x := by
        intro v hv he ex
        refine ⟨v, ?_, he⟩
        rw [hq]; simp only [List.mem_filter]
        exact ⟨hv, by simpa [he] using ex⟩
      rcases hcase with ⟨_, hrec, hc, _⟩ | ⟨hne, hrec, hc, _⟩
      · by_cases ex : x = r
        · right; right; right; rw [hc]; simp [ex]
        · have hx' : x ∈ s.stored := (List.mem_cons.mp hx).resolve_left ex
          rcases h x hx' with ⟨y, hy, hcy⟩ | ⟨v, hv, he⟩ | hp | hcand
          · left; exact ⟨y, by rw [hrec]; exact hy, hcy⟩
          · right; left; exact queued v hv he ex
          · right; right; left; rw [hun]; exact hp
          · right; right; right; rw [hc]; simp [hcand]
      · by_cases ex : x = r
        · -- a waiting file is now stored at `r`
          left
          have : ∃ id, id ∈ s.waiting u := by
            cases hw : s.waiting u with
            | nil => rw [hw] at hne; simp at hne
            | cons id _ => exact ⟨id, by simp⟩
          obtain ⟨id, hid⟩ := this
          simp only [State.waiting, List.mem_filter, List.any_eq_true, Bool.and_eq_true,
            beq_iff_eq] at hid
          obtain ⟨hlinked, y, hy, hyid, hdev⟩ := hid
          refine ⟨storeRecord s u y, by rw [hrec]; exact List.mem_map_of_mem hy, ?_⟩
          simp [storeRecord, hyid, hlinked, hdev, hr, ex]
        · have hx' : x ∈ s.stored := (List.mem_cons.mp hx).resolve_left ex
          rcases h x hx' with ⟨y, hy, hcy⟩ | ⟨v, hv, he⟩ | hp | hcand
          · left
            refine ⟨storeRecord s u y, by rw [hrec]; exact List.mem_map_of_mem hy, ?_⟩
            unfold storeRecord
            split
            · rename_i hdev
              simp only [Bool.and_eq_true] at hdev
              rw [hcy] at hdev; simp at hdev
            · exact hcy
          · right; left; exact queued v hv he ex
          · right; right; left; rw [hun]; exact hp
          · right; right; right; rw [hc]; exact hcand
  | publish =>
    intro x hx
    rcases h x hx with hr | hq | hp | hc
    · left; exact hr
    · right; left; exact hq
    · right; right; right; simp [Step.apply, publish, hp]
    · right; right; right; simp [Step.apply, publish, hc]
  | settle => exact (settle_parts s).2.2 h

theorem steps_reachable (s : State) (steps : List Step) (h : Reachable s) :
    Reachable (runSteps s steps) := by
  induction steps generalizing s with
  | nil => exact h
  | cons st rest ih => exact ih _ (step_reachable s st h)

/-- D12: a file chunk's nonce is its index. The fixed key and location,
size header and verified plaintext determine every encrypted byte. -/
structure EncryptedChunk where
  reference : Reference
  size : Nat
  index : Nat
  plaintext : Bytes
  deriving DecidableEq, Repr

/-- §17.8 models a collision-free checked chunk hash by equality with its
ghost captured bytes. No changed chunk reaches encryption. -/
def checkChunk (u : Upload) (index : Nat) (source : Bytes) :
    Except SourceFailure EncryptedChunk :=
  match u.chunks[index]? with
  | none => .error .integrity
  | some original =>
    if source = original then .ok ⟨u.location, u.size, index, source⟩
    else .error .changed

theorem encrypted_chunk_is_captured (u : Upload) (i : Nat) (source : Bytes)
    (c : EncryptedChunk) (h : checkChunk u i source = .ok c) :
    u.chunks[i]? = some c.plaintext ∧ c.reference = u.location ∧
    c.size = u.size ∧ c.index = i := by
  unfold checkChunk at h
  split at h
  · contradiction
  · split at h
    · cases h; simp_all
    · contradiction

theorem file_retries_fixed (u : Upload) (i : Nat) (a b : Bytes)
    (ca cb : EncryptedChunk) (ha : checkChunk u i a = .ok ca)
    (hb : checkChunk u i b = .ok cb) : ca = cb := by
  obtain ⟨pa, ra, sa, ia⟩ := encrypted_chunk_is_captured u i a ca ha
  obtain ⟨pb, rb, sb, ib⟩ := encrypted_chunk_is_captured u i b cb hb
  have hp : ca.plaintext = cb.plaintext := Option.some.inj (pa.symm.trans pb)
  cases ca; cases cb; simp_all

end CovenStorage.Files
