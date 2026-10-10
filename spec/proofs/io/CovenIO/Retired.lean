import CovenIO.Trace

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

/-- The cutoff prefix has no publication gaps. Needed write coverage is
supplied by the same recent/selected-snapshot condition as ordinary discovery. -/
theorem drain_complete (world : World) (writer cutoff start first finish last target : Nat)
    (kind : Kind) (trace : List ReadEvent) (scan : Scan world kind writer start first finish last trace)
    (available : Available world kind writer start finish first target)
    (closed : ClosedAfter world writer cutoff)
    (endAtCutoff : ∀ n o, world cutoff (.log kind writer n) = some o → n ≤ target)
    (time : Nat) (afterCutoff : cutoff ≤ time) (n : Nat) (o : Object)
    (found : world time (.log kind writer n) = some o) : n ≤ last := by
  exact Nat.le_trans (endAtCutoff n o (closed time afterCutoff kind n o found))
    (discovery_complete scan target available)

/-- The explicit replacement hypothesis talks about sends AND landings.
Reading replacement stops all sends, including attempted retries. Every send
before that read has landed or become unable to land by the drain frontier. -/
structure ReplacementWindow (issued lands : Nat → Nat) (read drain : Nat) : Prop where
  stops : ∀ request, issued request < read
  bounded : ∀ request, issued request < read → lands request ≤ drain

theorem replacement_no_later_landing (issued lands : Nat → Nat) (read drain : Nat)
    (h : ReplacementWindow issued lands read drain) : ∀ request, lands request ≤ drain :=
  fun request => h.bounded request (h.stops request)

/-- A final replacement alone gives no network delivery deadline. Convert the
explicit landing bound and the provider's exact publication witness to closure. -/
theorem replacement_closed (world : World) (writer drain : Nat)
    (issued lands : Nat → Nat) (read : Nat)
    (window : ReplacementWindow issued lands read drain)
    (publication : ∀ time kind n o, world time (.log kind writer n) = some o →
      ∃ request, lands request ≤ drain → world drain (.log kind writer n) = some o) :
    ClosedAfter world writer drain := by
  intro time _ kind n o found
  obtain ⟨request, published⟩ := publication time kind n o found
  exact published (replacement_no_later_landing issued lands read drain window request)

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
