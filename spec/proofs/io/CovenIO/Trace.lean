import CovenIO.Discovery
import CovenStorage.Identity

namespace CovenIO

def probes (next : Kind → Nat → Nat) (writer : Nat) : List Request :=
  [.get (.log .entry writer (next .entry writer)),
   .get (.log .copy writer (next .copy writer)),
   .get (.log .write writer (next .write writer))]

def idle (next : Kind → Nat → Nat) (writers : List Nat) : List Request :=
  [.list .deviceEntries, .list .positions] ++ writers.flatMap (probes next)

theorem idle_requests (next : Kind → Nat → Nat) (writers : List Nat) :
    (idle next writers).length = 2 + 3 * writers.length := by
  induction writers with
  | nil => simp [idle]
  | cons w ws ih => simp [idle, probes, List.flatMap_cons] at *; omega

def listing : Request → Bool | .list _ => true | _ => false
def getting : Request → Bool | .get _ => true | _ => false

theorem idle_two_listings (next : Kind → Nat → Nat) (writers : List Nat) :
    ((idle next writers).filter listing).length = 2 := by
  have none : ∀ ws : List Nat, ((ws.flatMap (probes next)).filter listing) = [] := by
    intro ws; induction ws with
    | nil => rfl
    | cons w ws ih => simp [List.flatMap_cons, probes, listing, ih]
  simp [idle, List.filter_cons, listing, none]

theorem idle_gets (next : Kind → Nat → Nat) (writers : List Nat) :
    ((idle next writers).filter getting).length = 3 * writers.length := by
  induction writers with
  | nil => simp [idle, getting]
  | cons w ws ih => simp [idle, probes, List.filter_cons, getting, List.flatMap_cons] at *; omega

theorem scan_cost {world : World} {kind : Kind} {writer start first finish last : Nat}
    {trace : List ReadEvent} (h : Scan world kind writer start first finish last trace) :
    trace.length + first = last + 1 := by
  induction h with
  | miss => simp; omega
  | hit _ _ _ _ _ ih => simp only [List.length_cons]; omega

/-- After a hit, a bounded transfer batch may leave requests beyond the first
miss in flight. Capacity is shared; each actual outstanding request is charged.
This bound is per scanned log, not one allowance for the entire pass. -/
theorem fetch_ahead_cost {world : World} {kind : Kind} {writer start first finish last : Nat}
    {trace : List ReadEvent} (h : Scan world kind writer start first finish last trace)
    (inFlight : List ReadEvent) (limit : Nat) (bounded : inFlight.length < limit) :
    (trace ++ inFlight).length ≤ (last - first) + 1 + (limit - 1) := by
  have := scan_cost h
  have := (scan_terminal h).2.1
  simp only [List.length_append]
  omega

/-- Durable byte identity includes the object revision. Overlapping ranges
share the same addresses. Cache events describe individual byte reservations;
a provider range request batches these events without changing their guards. -/
structure ByteAddress where
  path : Path
  version : Metadata
  offset : Nat
  deriving DecidableEq, Repr

inductive CacheEvent where
  | fetch (version : ByteAddress)
  | commit (version : ByteAddress)
  | failed (version : ByteAddress)
  | evict (version : ByteAddress)
  | reopen
  deriving DecidableEq, Repr

structure Cache where
  retained : List ByteAddress
  inFlight : List ByteAddress
  deriving DecidableEq, Repr

def cacheStep (s : Cache) : CacheEvent → Option Cache
  | .fetch v => if v ∈ s.retained ∨ v ∈ s.inFlight then none
      else some { s with inFlight := v :: s.inFlight }
  | .commit v => if v ∈ s.inFlight then
      some ⟨v :: s.retained, s.inFlight.filter (· != v)⟩ else none
  | .failed v => some { s with inFlight := s.inFlight.filter (· != v) }
  | .evict v => some { s with retained := s.retained.filter (· != v) }
  | .reopen => some s

theorem retained_never_fetched (s : Cache) (v : ByteAddress) (retained : v ∈ s.retained) :
    cacheStep s (.fetch v) = none := by simp [cacheStep, retained]

theorem shared_in_flight (s : Cache) (v : ByteAddress) (busy : v ∈ s.inFlight) :
    cacheStep s (.fetch v) = none := by simp [cacheStep, busy]

theorem reopen_retains_bytes (s : Cache) : cacheStep s .reopen = some s := rfl

theorem cache_keeps_unless_evicted (a b : Cache) (e : CacheEvent) (v : ByteAddress)
    (step : cacheStep a e = some b) (retained : v ∈ a.retained) (noEviction : e ≠ .evict v) :
    v ∈ b.retained := by
  cases e <;> simp only [cacheStep] at step
  · split at step
    · cases step
    · cases step; exact retained
  · split at step
    · cases step; exact List.mem_cons_of_mem _ retained
    · cases step
  · cases step; exact retained
  · cases step; simp_all [Ne.symm]
  · cases step; exact retained

inductive CacheRun : Cache → List CacheEvent → Cache → Prop
  | nil (s : Cache) : CacheRun s [] s
  | cons {a b c : Cache} {e : CacheEvent} {es : List CacheEvent} :
      cacheStep a e = some b → CacheRun b es c → CacheRun a (e :: es) c

/-- No second download while bytes remain, across arbitrary passes/reopens. -/
theorem no_duplicate_download {a b : Cache} {events : List CacheEvent} (run : CacheRun a events b)
    (v : ByteAddress) (retained : v ∈ a.retained) (noEviction : .evict v ∉ events) :
    .fetch v ∉ events ∧ v ∈ b.retained := by
  induction run with
  | nil => exact ⟨by simp, retained⟩
  | @cons a b c e es step rest ih =>
      have ne : e ≠ .evict v := by intro eq; subst e; simp at noEviction
      have kept := cache_keeps_unless_evicted a b e v step retained ne
      have tail := ih kept (fun hm => noEviction (List.mem_cons_of_mem _ hm))
      refine ⟨?_, tail.2⟩
      intro hm
      rcases List.mem_cons.mp hm with eq | hm
      · subst e; rw [retained_never_fetched a v retained] at step; cases step
      · exact tail.1 hm

/-- Every actor, including each SDK retry, goes through the same send rule. -/
inductive Actor where
  | pass | upload | outside
  deriving DecidableEq, Repr

structure Catchup where
  started : Nat
  finished : Nat
  identity : CovenStorage.Gate
  retired : Bool
  deriving DecidableEq, Repr

def SendAllowed (catchup : Catchup) (time : Nat) : Prop :=
  catchup.identity = .send ∧ catchup.retired = false ∧
  catchup.started ≤ catchup.finished ∧ catchup.finished ≤ time ∧ time < catchup.started + freshFor

instance (catchup : Catchup) (time : Nat) : Decidable (SendAllowed catchup time) :=
  inferInstanceAs (Decidable (_ ∧ _ ∧ _ ∧ _ ∧ _))

def sends : Request → Bool
  | .create _ _ | .replace _ _ | .beginUpload _ _ | .uploadPart _ _ _ | .finishUpload _ _ => true
  | _ => false

def fileUpload : Request → Bool
  | .create (.file _ _) _ | .replace (.file _ _) _ |
    .beginUpload _ _ | .uploadPart _ _ _ | .finishUpload _ _ => true
  | _ => false

structure Sender where
  starting : Option Nat
  completed : Option Catchup
  deriving DecidableEq, Repr

inductive IOEvent where
  | beginPass (time : Nat)
  | checked (time : Nat) (gate : CovenStorage.Gate) (retired : Bool)
  | request (actor : Actor) (time : Nat) (value : Request)
  | invalidate
  deriving DecidableEq, Repr

def permitted (completed : Option Catchup) (time : Nat) : Bool :=
  match completed with | none => false | some c => decide (SendAllowed c time)

def gateStep (s : Sender) : IOEvent → Option Sender
  | .beginPass time => some { s with starting := some time }
  | .invalidate => some ⟨none, none⟩
  | .checked time gate retired => do
      let start ← s.starting
      if start ≤ time then some ⟨none, some ⟨start, time, gate, retired⟩⟩ else none
  | .request actor time r =>
      if actor = .pass ∧ fileUpload r then none
      else if (sends r || decide (actor = .upload)) && !permitted s.completed time then none
      else some s

inductive IOTrace : Sender → List IOEvent → Sender → Prop
  | nil (s : Sender) : IOTrace s [] s
  | cons {a b c : Sender} {e : IOEvent} {es : List IOEvent} :
      gateStep a e = some b → IOTrace b es c → IOTrace a (e :: es) c

theorem send_requires_check (before after : Sender) (actor : Actor) (time : Nat) (r : Request)
    (sending : sends r = true ∨ actor = .upload)
    (step : gateStep before (.request actor time r) = some after) :
    ∃ c, before.completed = some c ∧ SendAllowed c time := by
  simp only [gateStep] at step
  split at step
  · cases step
  · split at step
    · cases step
    · rename_i allowed
      have checked : permitted before.completed time = true := by
        rcases sending with sending | rfl <;> simp_all
      cases hc : before.completed with
      | none => simp [permitted, hc] at checked
      | some c => exact ⟨c, rfl, by simpa [permitted, hc] using checked⟩

/-- Request evidence is extracted from the shared trace, not supplied as an
independent worker assumption. The immediately preceding state holds it. -/
theorem check_precedes_send {events : List IOEvent} {before after : Sender}
    (run : IOTrace before events after) (actor : Actor) (time : Nat) (r : Request)
    (member : .request actor time r ∈ events) (sending : sends r = true ∨ actor = .upload) :
    ∃ c, SendAllowed c time := by
  induction run with
  | nil => cases member
  | @cons a b c e es step rest ih =>
      rcases List.mem_cons.mp member with eq | hm
      · subst e
        obtain ⟨catchup, _, allowed⟩ := send_requires_check a b actor time r sending step
        exact ⟨catchup, allowed⟩
      · exact ih hm

def passRequests (events : List IOEvent) : List Request :=
  events.filterMap fun event => match event with
    | .request .pass _ r => some r
    | _ => none

theorem pass_no_file_uploads {events : List IOEvent} {before after : Sender}
    (run : IOTrace before events after) : ∀ r ∈ passRequests events, fileUpload r = false := by
  induction run with
  | nil => simp [passRequests]
  | @cons a b c e es step rest ih =>
      cases e with
      | request actor time value =>
          cases actor with
          | pass =>
              have noUpload : fileUpload value = false := by
                cases h : fileUpload value <;> simp_all [gateStep]
              simpa [passRequests, noUpload] using
                (show ∀ r, r = value ∨ r ∈ passRequests es → fileUpload r = false from
                  fun r hr => hr.elim (fun eq => eq ▸ noUpload) (ih r))
          | upload | outside => simpa [passRequests] using ih
      | beginPass | checked | invalidate => simpa [passRequests] using ih

theorem upload_worker_gated {events : List IOEvent} {before after : Sender}
    (run : IOTrace before events after) (time : Nat) (r : Request)
    (member : .request .upload time r ∈ events) : ∃ c, SendAllowed c time :=
  check_precedes_send run .upload time r member (Or.inr rfl)

theorem expired_sends_nothing (c : Catchup) (time : Nat) (expired : c.started + freshFor ≤ time) :
    permitted (some c) time = false := by
  simp only [permitted, decide_eq_false_iff_not]
  intro allowed
  exact Nat.not_lt_of_ge expired allowed.2.2.2.2

theorem invalidation_clears (s : Sender) :
    gateStep s .invalidate = some ⟨none, none⟩ := rfl

end CovenIO
