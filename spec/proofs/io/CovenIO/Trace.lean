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

inductive PassEvent where
  | beginPass
  | checked (gate : CovenStorage.Gate)
  | request (value : Request)
  | invalidate
  deriving DecidableEq, Repr

def sends : Request → Bool
  | .create _ _ | .replace _ _ => true
  | _ => false

def gateStep (checked : Bool) : PassEvent → Option Bool
  | .beginPass | .invalidate => some false
  | .checked gate => some (decide (gate = .send))
  | .request r => if sends r && !checked then none else some checked

inductive PassTrace : Bool → List PassEvent → Bool → Prop
  | nil (s : Bool) : PassTrace s [] s
  | cons {a b c : Bool} {e : PassEvent} {es : List PassEvent} :
      gateStep a e = some b → PassTrace b es c → PassTrace a (e :: es) c

theorem send_requires_check (checked after : Bool) (r : Request)
    (sending : sends r = true) (step : gateStep checked (.request r) = some after) : checked = true := by
  cases checked <;> simp_all [gateStep]

/-- Without a successful completed check, an unchecked pass cannot gain
send authority through a read, a failed check, a reset or another pass. -/
theorem check_precedes_send {events : List PassEvent} {before after : Bool}
    (run : PassTrace before events after) (unchecked : before = false)
    (noCheck : .checked .send ∉ events) :
    after = false ∧ ∀ r, .request r ∈ events → sends r = false := by
  induction run with
  | nil => simp_all
  | @cons a b c e es step rest ih =>
      subst a
      have head : e ≠ .checked .send := by intro eq; subst e; simp at noCheck
      have next : b = false := by
        cases e with
        | beginPass | invalidate => simpa [gateStep] using step.symm
        | checked g => cases g <;> simp_all [gateStep]
        | request r => cases hs : sends r <;> simp_all [gateStep]
      subst b
      have tail := ih rfl (fun hm => noCheck (List.mem_cons_of_mem _ hm))
      refine ⟨tail.1, ?_⟩
      intro r hr
      rcases List.mem_cons.mp hr with eq | hm
      · subst e; cases hs : sends r <;> simp_all [gateStep]
      · exact tail.2 r hm

end CovenIO
