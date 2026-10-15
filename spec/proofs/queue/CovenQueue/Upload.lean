import CovenQueue.Identity
import CovenQueue.Keys
import CovenQueue.Verdicts

/-! The durable queue and an asynchronous create-only provider (§6, §10,
§17.1, §18.1). A lost reply does not cancel a request. Storage can land any
previous send, even after a later pass found the path absent. -/

namespace CovenQueue

structure Attempt where
  record : Record
  format : Nat
  /-- Header key followed by each part's key (D9). -/
  keys : List Nat
  deriving DecidableEq, Repr

inductive Pending where
  | untried (record : Record)
  | tried (attempt : Attempt)
  deriving DecidableEq, Repr

def Pending.record : Pending → Record
  | .untried r => r
  | .tried a => a.record

/-- §17.1's upload gate (Rust `device_log_sync::uploadable`). `publication`
is the version of this device's latest local breaking migration, whose
captured raise covers every own record through its marker; zero means none.
`through` is the newest own write any current capture covers (zero for
none): the marker, or a later write once a capture is taken again after a
catch-up (§15). A migration marker always sends: it has no row changes, its
effects travel only in the raise snapshot, and it must publish before that
snapshot, whose positions include it. A record older than that version, or
numbered within a capture, belongs to the captured prefix and sends while
the store is older. A later record at that version waits for a kept raise
to it. -/
def Uploadable (keptVersion publication through : Nat) (r : Record) : Prop :=
  r.disposition = .migration ∨ publication = 0 ∨ r.version < publication ∨
    publication ≤ keptVersion ∨ r.identity.id.number ≤ through

instance (keptVersion publication through : Nat) (r : Record) :
    Decidable (Uploadable keptVersion publication through r) :=
  inferInstanceAs (Decidable (_ ∨ _ ∨ _ ∨ _ ∨ _))

/-- A record no newer than the kept store version always sends. -/
theorem old_uploadable (keptVersion publication through : Nat) (r : Record)
    (old : r.version ≤ keptVersion) : Uploadable keptVersion publication through r := by
  unfold Uploadable
  omega

structure Queue where
  device : Nat
  settled : Nat
  waiting : List Pending
  deriving DecidableEq, Repr

def Queue.reserved (q : Queue) : Nat := q.settled + q.waiting.length

def Queue.commit (q : Queue) (r : Record) : Queue :=
  let r := { r with identity := { r.identity with id := ⟨q.device, q.reserved + 1⟩ } }
  { q with waiting := q.waiting ++ [.untried r] }

def Queue.prepare (q : Queue) (format : Nat) (keys : List Nat) : Queue :=
  match q.waiting with
  | .untried r :: rest => { q with waiting := .tried ⟨r, format, keys⟩ :: rest }
  | _ => q

def Queue.head (q : Queue) : Option Attempt :=
  match q.waiting with
  | .tried a :: _ => some a
  | _ => none

def Queue.finish (q : Queue) : Queue :=
  match q.waiting with
  | .tried _ :: rest => { q with settled := q.settled + 1, waiting := rest }
  | _ => q

/-- Reservation and commit order: each position in the queue has its exact
next number. Settled rows have left the local table. -/
inductive Numbered (device : Nat) : Nat → List Pending → Prop where
  | nil (next) : Numbered device next []
  | cons {next p rest} : p.record.identity.id = ⟨device, next⟩ →
      Numbered device (next + 1) rest → Numbered device next (p :: rest)

def Queue.Valid (q : Queue) : Prop := Numbered q.device (q.settled + 1) q.waiting

theorem numbered_append (d n : Nat) (ps : List Pending) (p : Pending)
    (h : Numbered d n ps) (hp : p.record.identity.id = ⟨d, n + ps.length⟩) :
    Numbered d n (ps ++ [p]) := by
  induction h with
  | nil => exact .cons (by simpa using hp) (.nil _)
  | cons hx ht ih =>
    apply Numbered.cons hx
    apply ih
    simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hp

theorem commit_valid (q : Queue) (h : q.Valid) (r : Record) :
    (q.commit r).Valid := by
  apply numbered_append _ _ _ _ h
  simp [Pending.record, Queue.reserved]
  omega

theorem prepare_valid (q : Queue) (h : q.Valid) (f : Nat) (ks : List Nat) :
    (q.prepare f ks).Valid := by
  cases q with | mk d n ps =>
    cases ps with
    | nil => exact h
    | cons p rest =>
      cases p with
      | tried a => exact h
      | untried r =>
        cases h with
        | cons hi ht => exact .cons hi ht

theorem finish_valid (q : Queue) (h : q.Valid) : (q.finish).Valid := by
  cases q with | mk d n ps =>
    cases ps with
    | nil => exact h
    | cons p rest =>
      cases p with
      | untried _ => exact h
      | tried _ => cases h with | cons _ ht => exact ht

theorem head_next (q : Queue) (h : q.Valid) (a : Attempt) (ha : q.head = some a) :
    a.record.identity.id = ⟨q.device, q.settled + 1⟩ := by
  cases q with | mk d n ps =>
    cases ps with
    | nil => simp [Queue.head] at ha
    | cons p rest =>
      cases p with
      | untried _ => simp [Queue.head] at ha
      | tried a' =>
        simp [Queue.head] at ha
        subst a'
        cases h with | cons hi _ => exact hi

abbrev Bytes := List Nat

/-- The deterministic encoder/sealer is a parameter. Equality is proved for
any such function; encryption and actual format bytes are outside this model. -/
abbrev Seal := Attempt → Bytes

/-- Neither an uncommitted app write nor a checked request reply survives
process death. The committed attempt belongs to `Queue`, not this memory. -/
structure Memory where
  transaction : Option Record
  reply : Option Attempt
  deriving DecidableEq, Repr

def Memory.empty : Memory := ⟨none, none⟩

structure World where
  queue : Queue
  stored : Nat → Option Bytes
  /-- Requests handed to the provider, not a local in-memory request list.
  A crash cannot recall them; any may still land, or may never land. -/
  sent : List Attempt
  memory : Memory := .empty

def World.beginWrite (s : World) (r : Record) : World :=
  { s with memory := { s.memory with transaction := some r } }

def World.commitWrite (s : World) (r : Record) : World :=
  { s with
    queue := s.queue.commit r
    memory := { s.memory with transaction := none } }

def World.crash (s : World) : World := { s with memory := .empty }

theorem crash_keeps_committed (s : World) :
    s.crash.queue = s.queue ∧ s.crash.stored = s.stored ∧
    s.crash.sent = s.sent ∧ s.crash.memory = .empty := ⟨rfl, rfl, rfl, rfl⟩

def World.send (s : World) : World :=
  match s.queue.head with
  | some a => { s with sent := a :: s.sent }
  | none => s

def World.land (encode : Seal) (s : World) (a : Attempt) : World :=
  let n := a.record.identity.id.number
  match s.stored n with
  | none => { s with stored := fun k => if k = n then some (encode a) else s.stored k }
  | some _ => s

inductive Confirmation where
  | failedRead
  | pending
  | staleCopy
  | stored
  deriving DecidableEq, Repr

/-- Occupation alone is not success. A failed comparison read leaves the
queue intact; unequal complete bytes require device replacement (§10). -/
def confirm (encode : Seal) (a : Attempt) : Option (Option Bytes) → Confirmation
  | none => .failedRead
  | some none => .pending
  | some (some bytes) => if bytes = encode a then .stored else .staleCopy

theorem confirmation_requires_equal (encode : Seal) (a : Attempt) (read : Option (Option Bytes))
    (h : confirm encode a read = .stored) : read = some (some (encode a)) := by
  cases read with
  | none => simp [confirm] at h
  | some read =>
    cases read with
    | none => simp [confirm] at h
    | some bytes =>
      simp only [confirm] at h
      split at h
      · rename_i he; simp [he]
      · contradiction

theorem occupied_never_replaced (encode : Seal) (s : World) (a : Attempt) (bytes : Bytes)
    (h : s.stored a.record.identity.id.number = some bytes) :
    (s.land encode a).stored = s.stored := by simp [World.land, h]

/-- Local database transitions are transactions: a crash before one leaves
its input state, and a crash after one keeps its whole result. An app write
can be staged without committing. Reply receipt and its acknowledgement
transaction are separate, so a crash between them requires a new read.
Provider acceptance may occur before or after any crash, independently of
reply receipt. The supplied store version is the version of a kept raise, not a local
migration or an entry merely stored; the supplied publication is the version
of the latest local breaking migration and the supplied through is its
newest captured own write (`RaiseCaptures.Local` tracks both). A
send requires a fresh supplied §10 identity check and rechecks that version
gate for its original attempt. -/
inductive Step (encode : Seal) : World → World → Prop where
  | beginWrite (s r) : Step encode s (s.beginWrite r)
  | commit (s r) (audiences : List AudienceState) :
      s.memory.transaction = some r →
      AudiencesReady audiences →
      Step encode s (s.commitWrite r)
  | prepare (s f) (requests : List KeyRequest) (keys : List Key)
      (appVersion keptVersion publication through : Nat) (r : Record) (rest : List Pending) :
      s.queue.waiting = .untried r :: rest →
      keptVersion ≤ appVersion → Uploadable keptVersion publication through r →
      chooseKeys requests = some keys →
      (∃ header parts, requests = header :: parts ∧
        header.audience = 0 ∧ header.currentMember = true) →
      Step encode s { s with queue := s.queue.prepare f (keys.map Key.id) }
  | send (s) (reserved : Counters) (e : Option Evidence)
      (appVersion keptVersion publication through : Nat) :
      reserved.writes = s.queue.reserved →
      checkIdentity s.queue.device reserved e = .ready →
      keptVersion ≤ appVersion →
      (∀ a, s.queue.head = some a → Uploadable keptVersion publication through a.record) →
      Step encode s s.send
  | land (s a) : a ∈ s.sent → Step encode s (s.land encode a)
  | receive (s a) : s.queue.head = some a →
      s.stored a.record.identity.id.number = some (encode a) →
      Step encode s { s with memory := { s.memory with reply := some a } }
  | acknowledge (s a) : s.queue.head = some a →
      s.memory.reply = some a →
      s.stored a.record.identity.id.number = some (encode a) →
      Step encode s { s with
        queue := s.queue.finish
        memory := { s.memory with reply := none } }
  | crash (s) : Step encode s s.crash
  | wait (s) : Step encode s s

theorem step_queue_valid {encode : Seal} {s t : World}
    (h : Step encode s t) (hs : s.queue.Valid) : t.queue.Valid := by
  cases h with
  | beginWrite | receive | crash => exact hs
  | commit => exact commit_valid _ hs _
  | prepare => exact prepare_valid _ hs _ _
  | send => unfold World.send; split <;> exact hs
  | land a _ => simp only [World.land]; split <;> exact hs
  | acknowledge a _ _ _ => exact finish_valid _ hs
  | wait => exact hs

theorem step_queue_device {encode : Seal} {s t : World} (h : Step encode s t) :
    t.queue.device = s.queue.device := by
  cases h with
  | beginWrite | commit | receive | crash | wait => rfl
  | prepare => unfold Queue.prepare; split <;> rfl
  | acknowledge => unfold Queue.finish; split <;> rfl
  | send => unfold World.send; split <;> rfl
  | land => simp only [World.land]; split <;> rfl

end CovenQueue
