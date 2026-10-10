import CovenIO.Trace
import CovenIO.Refinement

namespace CovenIO.Retired

inductive Life where
  | active | removed | replaced | drained
  deriving DecidableEq, Repr

def polling (life : Nat → Life) (known : List Nat) : List Nat :=
  known.filter (fun w => decide (life w ≠ .drained))

def retire (life : Nat → Life) (writer : Nat) : Nat → Life :=
  fun w => if w = writer then .drained else life w

theorem drained_not_polled (life : Nat → Life) (known : List Nat) (writer : Nat) :
    writer ∉ polling (retire life writer) known := by simp [polling, retire]

theorem drained_not_named (life : Nat → Life) (known : List Nat) (writer : Nat)
    (next : Kind → Nat → Nat) (request : Request)
    (requested : request ∈ idle next (polling (retire life writer) known)) :
    request.names writer = false := by
  simp only [idle, List.mem_append, List.mem_cons, List.not_mem_nil, or_false] at requested
  rcases requested with (rfl | rfl) | requested
  · rfl
  · rfl
  · obtain ⟨w, member, read⟩ := List.mem_flatMap.mp requested
    have different : w ≠ writer := by
      intro eq; subst w; exact drained_not_polled life known writer member
    simp only [probes, List.mem_cons, List.not_mem_nil, or_false] at read
    rcases read with rfl | rfl | rfl <;> simp [Request.names, Path.writer, different]

/-- Confirmed cut-off includes existing upload sessions; no newly published
object may appear after this frontier. Deletion remains possible. -/
def ClosedAfter (world : World) (writer cutoff : Nat) : Prop :=
  ∀ time, cutoff ≤ time → ∀ kind n o,
    world time (.log kind writer n) = some o → world cutoff (.log kind writer n) = some o

/-- Kept device removals, member removals and replacements are projected
to the device ids they retire. Recorded read membership is immutable. -/
structure Retirement where
  entry : Nat
  storedAt : Nat
  devices : List Nat
  deriving DecidableEq, Repr

def writeAllowed (kept : List Retirement) (read : Nat → Bool) (w : Write) : Bool :=
  kept.all fun r => !(r.devices.contains w.writer) ||
    (!read r.entry && decide (w.storedAt ≤ r.storedAt + month))

theorem write_verdict_agrees (a b : List Retirement) (readA readB : Nat → Bool) (w : Write)
    (sameKept : ∀ r, r ∈ a ↔ r ∈ b) (sameRead : ∀ e, readA e = readB e) :
    writeAllowed a readA w = writeAllowed b readB w := by
  apply Bool.eq_iff_iff.mpr
  simp only [writeAllowed, List.all_eq_true]
  constructor
  · intro allowed r hr
    simpa only [sameRead] using allowed r ((sameKept r).mpr hr)
  · intro allowed r hr
    simpa only [sameRead] using allowed r ((sameKept r).mp hr)

theorem dropped_retirement_restores (remaining : List Retirement) (read : Nat → Bool) (w : Write)
    (otherRetirementsAllow : ∀ r ∈ remaining, w.writer ∈ r.devices →
      read r.entry = false ∧ w.storedAt ≤ r.storedAt + month) :
    writeAllowed remaining read w = true := by
  apply List.all_eq_true.mpr
  intro r hr
  by_cases target : w.writer ∈ r.devices
  · obtain ⟨unread, inTime⟩ := otherRetirementsAllow r hr target
    simp [target, unread, inTime]
  · simp [target]

theorem kept_retirement_rejects (kept : List Retirement) (read : Nat → Bool) (w : Write)
    (r : Retirement) (member : r ∈ kept) (target : w.writer ∈ r.devices)
    (excluded : read r.entry = true ∨ r.storedAt + month < w.storedAt) :
    writeAllowed kept read w = false := by
  apply Bool.eq_false_iff.mpr
  intro allowed
  have rule := List.all_eq_true.mp allowed r member
  rcases excluded with read | late <;> simp_all [writeAllowed] <;> omega

/-- C10 supplies the stable kept verdict used by a completed catch-up.
The receive set contains the old prefix certified by clock discovery. -/
theorem final_retirement_seen (H : CovenStorelog.Finality.History) (window bound T entry : Nat)
    (atFinality caughtUp : CovenStorelog.EntrySet)
    (valid : CovenStorelog.Valid H.log bound)
    (quiet : CovenStorelog.Horizon.quiet H window bound T = true)
    (finalComplete : CovenStorelog.Horizon.CompleteOld H window bound T atFinality)
    (catchupComplete : CovenStorelog.Horizon.CompleteOld H window bound T caughtUp)
    (inHistory : entry < bound) (old : CovenStorelog.Horizon.old H window T entry = true)
    (kept : entry ∈ (CovenStorelog.ReplayPolicy.resolve H window bound atFinality).kept) :
    entry ∈ (CovenStorelog.ReplayPolicy.resolve H window bound caughtUp).kept :=
  ((CovenStorelog.Horizon.current_stability H window bound T atFinality caughtUp
    valid quiet finalComplete catchupComplete inHistory old).1).mp kept

/-- Times share a duration scale. The implementation must relate monotonic
elapsed time to provider durations. The bound includes remote publication
after a lost reply, not merely a local timeout. -/
structure Publication (finalAt : Nat) where
  catchup : Catchup
  sent : Nat
  landed : Nat
  gate : SendAllowed catchup sent
  afterSend : sent ≤ landed
  withinDay : landed ≤ sent + day
  seesFinal : finalAt ≤ catchup.started → catchup.retired = true

theorem publication_before_deadline (finalAt : Nat) (p : Publication finalAt) :
    p.landed < finalAt + freshFor + day := by
  have before : p.catchup.started < finalAt := by
    by_cases h : finalAt ≤ p.catchup.started
    · have seen := p.seesFinal h
      have active := p.gate.2.1
      simp_all
    · omega
  have fresh := p.gate.2.2.2.2
  have duration := p.withinDay
  omega

/-- Every surviving object has its send/publication witness. Entries,
writes and copies use the same bound; exposure is not undone by a verdict. -/
theorem retirement_closed (states : Nat → Provider) (writer cutoff finalAt : Nat)
    (waited : finalAt + freshFor + day < (states cutoff).time)
    (evolves : ∀ t, cutoff ≤ t → ProviderRun (states cutoff) (states t))
    (publications : ∀ t kind n o, (states t).objects (.log kind writer n) = some o →
      ∃ p : Publication finalAt, p.landed = o.value.storedAt) :
    ClosedAfter (fun t => (states t).objects) writer cutoff := by
  intro t ht kind n o found
  obtain ⟨p, time⟩ := publications t kind n o found
  have bound := publication_before_deadline finalAt p
  exact old_immutable_present (evolves t ht) _ o rfl found (by omega)

/-- The gate and request duration derive ClosedAfter. Availability remains
necessary for writes deleted before the drain. -/
theorem drain_complete (states : Nat → Provider) (writer cutoff finalAt start first finish last target : Nat)
    (kind : Kind) (trace : List ReadEvent)
    (scan : Scan (fun t => (states t).objects) kind writer start first finish last trace)
    (afterWait : cutoff ≤ start)
    (available : Available (fun t => (states t).objects) kind writer start finish first target)
    (waited : finalAt + freshFor + day < (states cutoff).time)
    (evolves : ∀ t, cutoff ≤ t → ProviderRun (states cutoff) (states t))
    (publications : ∀ t kind n o, (states t).objects (.log kind writer n) = some o →
      ∃ p : Publication finalAt, p.landed = o.value.storedAt)
    (endAtCutoff : ∀ n o, (states cutoff).objects (.log kind writer n) = some o → n ≤ target)
    (time : Nat) (afterDrain : finish ≤ time) (n : Nat) (o : Object)
    (found : (states time).objects (.log kind writer n) = some o) : n ≤ last := by
  have order := (scan_terminal scan).1
  have closed := retirement_closed states writer cutoff finalAt waited evolves publications
  exact Nat.le_trans (endAtCutoff n o (closed time (by omega) kind n o found))
    (discovery_complete scan target available)

/-- A complete one-time orphan observation is durable; failed pages cannot
record completion. The scan restarts only after a new access epoch. -/
inductive FileScan where
  | pending
  | complete (paths : List Path)
  deriving DecidableEq, Repr

def observeFiles (old : FileScan) (result : Except Failure (List Path)) : FileScan :=
  match old, result with
  | .complete paths, _ => .complete paths
  | .pending, .error _ => .pending
  | .pending, .ok paths => .complete paths

theorem failed_scan_not_complete (e : Failure) : observeFiles .pending (.error e) = .pending := rfl
theorem completed_scan_retained (paths : List Path) (result : Except Failure (List Path)) :
    observeFiles (.complete paths) result = .complete paths := rfl

end CovenIO.Retired
