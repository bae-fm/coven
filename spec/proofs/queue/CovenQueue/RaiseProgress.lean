import CovenQueue.RaiseUpload

namespace CovenQueue.RaiseUpload

/-- A finite schedule of the actual queue events, with no raise publication. -/
inductive Uploads (encode : Seal) : World → World → Prop
  | refl (s) : Uploads encode s s
  | next {s t u} : CovenQueue.Step encode s t → Uploads encode t u → Uploads encode s u

theorem Uploads.trans {encode : Seal} {s t u : World}
    (first : Uploads encode s t) (second : Uploads encode t u) : Uploads encode s u := by
  induction first with
  | refl => exact second
  | next step rest ih => exact .next step (ih second)

def fixedAttempt (p : Pending) (format : Nat) (keys : List Nat) : Attempt :=
  match p with
  | .untried r => ⟨r, format, keys⟩
  | .tried a => a

theorem fixed_record (p : Pending) (format : Nat) (keys : List Nat) :
    (fixedAttempt p format keys).record = p.record := by cases p <;> rfl

/-- Occupied paths must contain the same original attempt, as §10 requires.
A conflicting stale installation cannot claim this progress premise. -/
def Compatible (encode : Seal) (format : Nat) (keys : List Nat) (s : World) : Prop :=
  ∀ p ∈ s.queue.waiting, s.stored p.record.identity.id.number = none ∨
    s.stored p.record.identity.id.number = some (encode (fixedAttempt p format keys))

theorem numbered_lower {d n : Nat} {ps : List Pending} (h : Numbered d n ps)
    (p : Pending) (member : p ∈ ps) : n ≤ p.record.identity.id.number := by
  induction h with
  | nil => simp at member
  | cons eq rest ih =>
      rcases List.mem_cons.mp member with rfl | tail
      · rw [eq]; exact Nat.le_refl _
      · exact Nat.le_trans (by omega) (ih tail)

/-- Each send obtains current custody and counter evidence. The existence
proof cannot manufacture a successful storage observation. -/
def IdentityAvailable : Prop :=
  ∀ q : Queue, q.Valid → ∃ reserved evidence,
    reserved.writes = q.reserved ∧ checkIdentity q.device reserved evidence = .ready

/-- With available keys and a successful identity observation, the provider
can accept the original bytes and confirm one head without any raise. -/
theorem head_uploads (encode : Seal) (s : World) (app store publication format : Nat)
    (requests : List KeyRequest) (keys : List Key) (p : Pending) (rest : List Pending)
    (valid : s.queue.Valid) (head : s.queue.waiting = p :: rest)
    (updated : store ≤ app) (enabled : Uploadable store publication p.record)
    (chosen : chooseKeys requests = some keys)
    (header : ∃ h ps, requests = h :: ps ∧ h.audience = 0 ∧ h.currentMember = true)
    (identity : IdentityAvailable)
    (slots : Compatible encode format (keys.map Key.id) s) :
    ∃ t, Uploads encode s t ∧
      t.queue = { s.queue with settled := s.queue.settled + 1, waiting := rest } ∧
      Compatible encode format (keys.map Key.id) t ∧ t.queue.Valid := by
  let attempt := fixedAttempt p format (keys.map Key.id)
  let prepared : World := { s with queue := s.queue.prepare format (keys.map Key.id) }
  have ready : Uploads encode s prepared := by
    cases p with
    | untried r => exact .next (.prepare s format requests keys app store publication r rest head
        updated enabled chosen header) (.refl _)
    | tried a =>
        have eq : prepared = s := by simp [prepared, Queue.prepare, head]
        rw [eq]; exact .refl _
  have preparedQueue : prepared.queue.waiting = .tried attempt :: rest := by
    cases p <;> simp [prepared, Queue.prepare, head, attempt, fixedAttempt]
  have preparedHead : prepared.queue.head = some attempt := by simp [Queue.head, preparedQueue]
  have preparedSettled : prepared.queue.settled = s.queue.settled := (prepare_counters _ _ _).1
  have preparedDevice : prepared.queue.device = s.queue.device := by
    cases p <;> simp [prepared, Queue.prepare, head]
  have preparedValid : prepared.queue.Valid := prepare_valid _ valid _ _
  have nextId := head_next _ preparedValid attempt preparedHead
  have nextNumber : attempt.record.identity.id.number = s.queue.settled + 1 := by
    simpa [preparedSettled] using congrArg WriteId.number nextId
  let sent := prepared.send
  obtain ⟨reserved, evidence, counters, checked⟩ := identity prepared.queue preparedValid
  have sendStep : CovenQueue.Step encode prepared sent :=
    .send prepared reserved evidence app store publication counters checked updated (by
        intro a found
        have eq : a = attempt := Option.some.inj (found.symm.trans preparedHead)
        rw [eq, fixed_record]; exact enabled)
  have sentEq : sent = { prepared with sent := attempt :: prepared.sent } := by
    simp [sent, World.send, preparedHead]
  let landed := sent.land encode attempt
  have sentMember : attempt ∈ sent.sent := by simp [sentEq]
  have stored : landed.stored attempt.record.identity.id.number = some (encode attempt) := by
    have compatible := slots p (by simp [head])
    rw [← fixed_record p format (keys.map Key.id)] at compatible
    rcases compatible with absent | present
    · simp [landed, World.land, sentEq, prepared, attempt] at absent ⊢
      rw [absent]; simp
    · simp [landed, World.land, sentEq, prepared, attempt, present]
  have landedQueue : landed.queue = prepared.queue := by
    simp only [landed, World.land]; split <;> simp [sentEq]
  have landedHead : landed.queue.head = some attempt := by rw [landedQueue]; exact preparedHead
  let replied : World := { landed with memory := { landed.memory with reply := some attempt } }
  let finished : World := { replied with
    queue := replied.queue.finish
    memory := { replied.memory with reply := none } }
  have finishStep : CovenQueue.Step encode replied finished := .acknowledge replied attempt landedHead rfl stored
  have queueEq : finished.queue = { s.queue with settled := s.queue.settled + 1, waiting := rest } := by
    simp only [finished, replied, landedQueue, Queue.finish, preparedQueue,
      preparedSettled, preparedDevice]
  refine ⟨finished, ready.trans (.next sendStep (.next (.land _ _ sentMember)
    (.next (.receive _ _ landedHead stored) (.next finishStep (.refl _))))), queueEq, ?_, ?_⟩
  · intro later member
    rw [queueEq] at member
    have numbered : Numbered s.queue.device (s.queue.settled + 1) (p :: rest) := by
      simpa [Queue.Valid, head] using valid
    have lower : s.queue.settled + 2 ≤ later.record.identity.id.number := by
      cases numbered with | cons _ tail => exact numbered_lower tail later member
    have distinct : later.record.identity.id.number ≠ attempt.record.identity.id.number := by omega
    have original := slots later (by simp [head, member])
    have preserved : finished.stored later.record.identity.id.number = s.stored later.record.identity.id.number := by
      change landed.stored _ = _
      simp only [landed, World.land]
      split <;> simp [distinct, sentEq, prepared]
    rw [preserved]; exact original
  · exact step_queue_valid finishStep (by rw [landedQueue]; exact preparedValid)

/-- Every finite eligible front drains before the raise, even with an
arbitrary new-schema tail already committed behind the captured boundary.
This constructs a legal successful schedule, rather than assuming readiness
of the entire queue or publication of the raise it is meant to enable. -/
theorem prefix_uploads (encode : Seal) (s : World) (app store publication format : Nat)
    (requests : List KeyRequest) (keys : List Key) (front tail : List Pending)
    (valid : s.queue.Valid) (waiting : s.queue.waiting = front ++ tail)
    (updated : store ≤ app) (enabled : ∀ p ∈ front, Uploadable store publication p.record)
    (chosen : chooseKeys requests = some keys)
    (header : ∃ h ps, requests = h :: ps ∧ h.audience = 0 ∧ h.currentMember = true)
    (identity : IdentityAvailable)
    (slots : Compatible encode format (keys.map Key.id) s) :
    ∃ t, Uploads encode s t ∧ t.queue.settled = s.queue.settled + front.length ∧
      t.queue.waiting = tail := by
  induction front generalizing s with
  | nil => exact ⟨s, .refl _, by simp, by simpa using waiting⟩
  | cons p ps ih =>
      obtain ⟨next, run, queueEq, compatible, validNext⟩ := head_uploads encode s app store publication format
        requests keys p (ps ++ tail) valid waiting updated (enabled p (by simp)) chosen header identity slots
      obtain ⟨last, rest, settled, tailEq⟩ := ih next validNext (by rw [queueEq])
        (fun p hp => enabled p (by simp [hp])) compatible
      refine ⟨last, run.trans rest, ?_, tailEq⟩
      rw [settled, queueEq]
      simp [Nat.add_comm, Nat.add_left_comm]

theorem Uploads.device {encode : Seal} {s t : World} (run : Uploads encode s t) :
    t.queue.device = s.queue.device := by
  induction run with
  | refl => rfl
  | next step rest ih => exact ih.trans (step_queue_device step)

theorem Uploads.lift {encode : Seal} {published : WriteId → Prop} {p : Plan}
    {start s t : World} (run : Uploads encode s t)
    (before : Run encode published p start ⟨s, .captured⟩) :
    Run encode published p start ⟨t, .captured⟩ := by
  induction run with
  | refl => exact before
  | next step rest ih => exact ih (.step before (.queue step))

/-- This produces both the drained prefix and the remotely published raise.
Keys, a valid identity observation and successful provider events are supplied;
no prior kept raise or empty app-write tail is required. -/
theorem no_raise_deadlock (encode : Seal) (published : WriteId → Prop) (plan : Plan)
    (s : World) (app store publication format : Nat) (requests : List KeyRequest) (keys : List Key)
    (front tail : List Pending) (valid : s.queue.Valid)
    (waiting : s.queue.waiting = front ++ tail) (updated : store ≤ app)
    (enabled : ∀ p ∈ front, Uploadable store publication p.record)
    (chosen : chooseKeys requests = some keys)
    (header : ∃ h ps, requests = h :: ps ∧ h.audience = 0 ∧ h.currentMember = true)
    (identity : IdentityAvailable)
    (slots : Compatible encode format (keys.map Key.id) s)
    (cut : plan.through = s.queue.settled + front.length)
    (coverage : ∀ id ∈ plan.covers, if id.device = s.queue.device then
      0 < id.number ∧ id.number ≤ plan.through else published id) :
    ∃ t, Run encode published plan s ⟨t, .entryStored⟩ ∧
      t.queue.settled = plan.through ∧ t.queue.waiting = tail := by
  obtain ⟨t, uploads, settled, waiting⟩ := prefix_uploads encode s app store publication format requests keys
    front tail valid waiting updated enabled chosen header identity slots
  have confirmed : Confirmed published plan t := by
    refine ⟨by simp [settled, cut], ?_⟩
    intro id member
    rw [uploads.device]
    exact coverage id member
  have drained := uploads.lift (published := published) (p := plan) Run.initial
  exact ⟨t, .step (.step (.step (.step drained
    (.requestSnapshot _ rfl confirmed)) (.landSnapshot _ rfl)) (.requestEntry _ rfl))
      (.landEntry _ rfl), settled.trans cut.symm, waiting⟩

/-- Before publication the gate admits the captured prefix: records older
than the migration's version and its empty marker. App writes behind the
marker, at that version, remain blocked. -/
theorem old_prefix_and_marker_enabled (q : Queue) (store version : Nat) (id : Identity)
    (old : ∀ p ∈ q.waiting, p.record.version < version) :
    ∀ p ∈ (migrate q version id).waiting, Uploadable store version p.record := by
  intro p member
  simp only [migrate, Queue.commit, List.mem_append, List.mem_singleton] at member
  rcases member with earlier | rfl
  · exact Or.inr (Or.inr (Or.inl (old p earlier)))
  · exact migration_marker_enabled _ _ _ _

theorem new_schema_send_requires_kept (s : World) (a : Attempt) (kept publication : Nat)
    (head : s.queue.head = some a) (ordinary : a.record.disposition = .ordinary)
    (current : a.record.version = publication) (migrated : 0 < publication)
    (gate : ∀ attempt, s.queue.head = some attempt → Uploadable kept publication attempt.record) :
    a.record.version ≤ kept :=
  new_schema_requires_kept _ _ _ ordinary current migrated (gate a head)

end CovenQueue.RaiseUpload
