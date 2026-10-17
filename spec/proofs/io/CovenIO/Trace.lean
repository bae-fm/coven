import CovenIO.Storage

namespace CovenIO

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

end CovenIO
