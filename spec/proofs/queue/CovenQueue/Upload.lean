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

structure Conversion where
  version : Nat
  /-- An absent callback marks the record lost. Present callbacks model
  successful, validated results; a failing migration never commits its reload. -/
  convert : Option (List Nat → List Nat)

/-- No conversion is permitted after the first attempt, including one whose
reply failed. Coverage is deliberately not a settlement argument. -/
def convertPending (c : Conversion) (inputsExcluded : Bool) : Pending → Pending
  | .tried a => .tried a
  | .untried r =>
    if r.version < c.version then
      let converted := if inputsExcluded then none else
        match r.disposition with
        | .lost _ => none
        | _ => c.convert.map (fun f => f r.body)
      .untried (match converted with
        | some body => { r with version := c.version, body := body }
        | none => { r with disposition := .lost c.version })
    else .untried r

theorem conversion_preserves_identity (c : Conversion) (bad : Bool) (p : Pending) :
    (convertPending c bad p).record.identity = p.record.identity := by
  cases p with
  | tried a => rfl
  | untried r =>
    by_cases hv : r.version < c.version
    · simp only [convertPending, ite_eq_left hv]
      generalize he : (if bad then none else
        match r.disposition with | .lost _ => none | _ => c.convert.map (fun f => f r.body)) = result
      cases result <;> rfl
    · simp [convertPending, hv, Pending.record]

theorem attempted_never_converted (c : Conversion) (bad : Bool) (a : Attempt) :
    convertPending c bad (.tried a) = .tried a := rfl

theorem excluded_input_not_converted (c : Conversion) (r : Record)
    (h : r.version < c.version) :
    convertPending c true (.untried r) =
      .untried { r with disposition := .lost c.version } := by
  simp [convertPending, h]

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

def Queue.convert (q : Queue) (c : Conversion) (bad : Record → Bool) : Queue :=
  { q with waiting := q.waiting.map fun p => convertPending c (bad p.record) p }

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

theorem numbered_convert (c : Conversion) (bad : Record → Bool)
    {d n : Nat} {ps : List Pending} (h : Numbered d n ps) :
    Numbered d n (ps.map fun p => convertPending c (bad p.record) p) := by
  induction h with
  | nil => exact .nil _
  | cons hi ht ih =>
    apply Numbered.cons
    · rw [conversion_preserves_identity]; exact hi
    · exact ih

theorem convert_valid (q : Queue) (h : q.Valid) (c : Conversion) (bad : Record → Bool) :
    (q.convert c bad).Valid := numbered_convert c bad h

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

structure World where
  queue : Queue
  stored : Nat → Option Bytes
  sent : List Attempt

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

theorem retries_identical (encode : Seal) (c : Conversion) (bad : Bool) (a : Attempt) :
    (match convertPending c bad (.tried a) with
      | .tried retried => encode retried
      | .untried _ => []) = encode a := rfl

/-- Events that change this queue. Network failure and coverage are genuine
stutters. A send requires the completed §10 check; proof of that check is
supplied by `Identity.lean`. An acknowledgement is possible only for equal
complete bytes. Storage landing is independent of acknowledgement. -/
inductive Step (encode : Seal) : World → World → Prop where
  | commit (s r) (audiences : List AudienceState) :
      AudiencesReady audiences →
      Step encode s { s with queue := s.queue.commit r }
  | prepare (s f) (requests : List KeyRequest) (keys : List Key)
      (appVersion storeVersion : Nat) (r : Record) (rest : List Pending) :
      s.queue.waiting = .untried r :: rest →
      storeVersion ≤ appVersion → r.version ≤ storeVersion →
      chooseKeys requests = some keys →
      (∃ header parts, requests = header :: parts ∧
        header.audience = 0 ∧ header.currentMember = true) →
      Step encode s { s with queue := s.queue.prepare f (keys.map Key.id) }
  | convert (s c bad) : Step encode s { s with queue := s.queue.convert c bad }
  | send (s) (reserved : Counters) (e : Option Evidence) (appVersion storeVersion : Nat) :
      reserved.writes = s.queue.reserved →
      checkIdentity s.queue.device reserved e = .ready →
      storeVersion ≤ appVersion → Step encode s s.send
  | land (s a) : a ∈ s.sent → Step encode s (s.land encode a)
  | acknowledge (s a) : s.queue.head = some a →
      s.stored a.record.identity.id.number = some (encode a) →
      Step encode s { s with queue := s.queue.finish }
  | wait (s) : Step encode s s

theorem step_queue_valid {encode : Seal} {s t : World}
    (h : Step encode s t) (hs : s.queue.Valid) : t.queue.Valid := by
  cases h with
  | commit => exact commit_valid _ hs _
  | prepare => exact prepare_valid _ hs _ _
  | convert c bad => exact convert_valid _ hs _ _
  | send => unfold World.send; split <;> exact hs
  | land a _ => simp only [World.land]; split <;> exact hs
  | acknowledge a _ _ => exact finish_valid _ hs
  | wait => exact hs

theorem head_attempt_preserved (q : Queue) (c : Conversion) (bad : Record → Bool)
    (a : Attempt) (h : q.head = some a) : (q.convert c bad).head = some a := by
  cases q with | mk d n ps =>
    cases ps with
    | nil => simp [Queue.head] at h
    | cons p rest =>
      cases p with
      | untried _ => simp [Queue.head] at h
      | tried a' => simpa [Queue.head, Queue.convert, convertPending] using h

end CovenQueue
