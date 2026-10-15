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
to the device ids they retire. -/
structure Retirement where
  entry : Nat
  storedAt : Nat
  devices : List Nat
  deriving DecidableEq, Repr

/-- §10, per object. `retiredInView` says whether the replay of the write's
own `store_log_read` retires its device (storage's `acceptsView`, §7.1 for
removal); such a write never counts. Every kept retirement of its device,
read or not, then bounds it by storage time: thirty days after the
retirement's storage time, inclusive. -/
def writeAllowed (kept : List Retirement) (retiredInView : Bool) (w : Write) : Bool :=
  !retiredInView && kept.all fun r => !(r.devices.contains w.writer) ||
    decide (w.storedAt ≤ r.storedAt + month)

theorem writeAllowed_iff (kept : List Retirement) (retiredInView : Bool) (w : Write) :
    writeAllowed kept retiredInView w = true ↔ retiredInView = false ∧
      ∀ r ∈ kept, w.writer ∈ r.devices → w.storedAt ≤ r.storedAt + month := by
  simp only [writeAllowed, Bool.and_eq_true, Bool.not_eq_true', List.all_eq_true,
    Bool.or_eq_true, Bool.not_eq_true', List.contains_eq_mem, decide_eq_false_iff_not,
    decide_eq_true_eq]
  constructor
  · rintro ⟨hv, h⟩
    exact ⟨hv, fun r hr hd => (h r hr).resolve_left (by simpa using hd)⟩
  · rintro ⟨hv, h⟩
    refine ⟨hv, fun r hr => ?_⟩
    by_cases hd : w.writer ∈ r.devices
    · exact Or.inr (h r hr hd)
    · exact Or.inl (by simpa using hd)

theorem write_verdict_agrees (a b : List Retirement) (viewA viewB : Bool) (w : Write)
    (sameKept : ∀ r, r ∈ a ↔ r ∈ b) (sameView : viewA = viewB) :
    writeAllowed a viewA w = writeAllowed b viewB w := by
  apply Bool.eq_iff_iff.mpr
  simp only [writeAllowed_iff, sameKept, sameView]

theorem dropped_retirement_restores (remaining : List Retirement) (w : Write)
    (otherRetirementsAllow : ∀ r ∈ remaining, w.writer ∈ r.devices →
      w.storedAt ≤ r.storedAt + month) :
    writeAllowed remaining false w = true :=
  (writeAllowed_iff remaining false w).mpr ⟨rfl, otherRetirementsAllow⟩

theorem kept_retirement_rejects (kept : List Retirement) (retiredInView : Bool) (w : Write)
    (r : Retirement) (member : r ∈ kept) (target : w.writer ∈ r.devices)
    (late : r.storedAt + month < w.storedAt) :
    writeAllowed kept retiredInView w = false := by
  apply Bool.eq_false_iff.mpr
  intro allowed
  have := ((writeAllowed_iff kept retiredInView w).mp allowed).2 r member target
  omega

theorem view_retirement_rejects (kept : List Retirement) (w : Write) :
    writeAllowed kept true w = false := by simp [writeAllowed]

/-- Reading a kept retirement excludes nothing by itself: a write whose own
view does not retire its device is judged by storage time alone. -/
theorem read_retirement_judged_by_time (kept : List Retirement) (w : Write)
    (timely : ∀ r ∈ kept, w.writer ∈ r.devices → w.storedAt ≤ r.storedAt + month) :
    writeAllowed kept false w = true :=
  dropped_retirement_restores kept w timely

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

/-! ### Restored access forgets the scan (§16.5)

A removed uploader can publish into its file folder only while its access is
not confirmed taken away. Each retention lists the folder only after that
cut-off, and the device's one owner (§20.2) runs a retention whole, so no
transaction recording access lands between a listing and its record. A
transaction that records any of the access restored forgets the scan
(`forgetOnRestore`); the rule it replaced forgot it only when a retention
next saw the access restored. -/

/-- One removed uploader's folder as this device knows it: whether its
access is confirmed taken away, the files it published, and the scan. -/
structure Folder where
  cut : Bool
  files : List Nat
  scan : FileScan
  deriving DecidableEq, Repr

def Folder.initial : Folder := ⟨false, [], .pending⟩

inductive FolderEvent where
  /-- The uploader publishes a file; a confirmed cut-off prevents it (§13). -/
  | upload (file : Nat)
  /-- A transaction records some of the access restored. -/
  | restore
  /-- A transaction records all of it confirmed taken away again. -/
  | revoke
  /-- One retention: after the cut-off, a listing that completes with every
  file in the folder or fails. -/
  | retain (completes : Bool)
  deriving DecidableEq, Repr

/-- What one retention's listing returns after the cut-off. -/
def Folder.listing (writer : Nat) (f : Folder) (completes : Bool) : Except Failure (List Path) :=
  if completes then .ok (f.files.map (Path.file writer)) else .error .network

def Folder.step (writer : Nat) (forgetOnRestore : Bool) (f : Folder) : FolderEvent → Folder
  | .upload file => if f.cut then f else { f with files := file :: f.files }
  | .restore => { f with cut := false, scan := if forgetOnRestore then .pending else f.scan }
  | .revoke => { f with cut := true }
  | .retain completes =>
      if f.cut then { f with scan := observeFiles f.scan (f.listing writer completes) }
      else if forgetOnRestore then f
      else { f with scan := .pending }

def Folder.run (writer : Nat) (forgetOnRestore : Bool) (events : List FolderEvent) : Folder :=
  events.foldl (Folder.step writer forgetOnRestore) Folder.initial

/-- A completed scan stands for a cut-off still in force and names every
file the folder holds. -/
def Folder.Sound (writer : Nat) (f : Folder) : Prop :=
  ∀ paths, f.scan = .complete paths → f.cut = true ∧ ∀ file ∈ f.files, Path.file writer file ∈ paths

theorem initial_sound (writer : Nat) : Folder.initial.Sound writer := by
  intro paths h; cases h

theorem step_sound (writer : Nat) (f : Folder) (e : FolderEvent) (sound : f.Sound writer) :
    (f.step writer true e).Sound writer := by
  obtain ⟨cut, files, scan⟩ := f
  intro paths h
  cases e with
  | upload file =>
      cases cut
      · have := (sound paths (by simpa [Folder.step] using h)).1
        cases this
      · exact sound paths (by simpa [Folder.step] using h)
  | restore => simp [Folder.step] at h
  | revoke =>
      exact ⟨rfl, (sound paths (by simpa [Folder.step] using h)).2⟩
  | retain completes =>
      cases cut
      · exact sound paths (by simpa [Folder.step] using h)
      · refine ⟨rfl, ?_⟩
        cases scan with
        | complete old =>
            have : old = paths := by simpa [Folder.step, observeFiles] using h
            subst this
            exact (sound old rfl).2
        | pending =>
            cases completes
            · simp [Folder.step, Folder.listing, observeFiles] at h
            · have : files.map (Path.file writer) = paths := by
                simpa [Folder.step, Folder.listing, observeFiles] using h
              subst this
              intro file hf
              exact List.mem_map.mpr ⟨file, hf, rfl⟩

/-- **A completed scan names every file the folder holds**, however restores
and revocations interleave with retentions, because the transaction that
records a restore forgets it (§16.5). -/
theorem scan_names_every_file (writer : Nat) (events : List FolderEvent) :
    (Folder.run writer true events).Sound writer := by
  unfold Folder.run
  suffices h : ∀ (f : Folder), f.Sound writer → (events.foldl (Folder.step writer true) f).Sound writer from
    h _ (initial_sound writer)
  induction events with
  | nil => exact fun _ h => h
  | cons e es ih => exact fun f h => ih _ (step_sound writer f e h)

/-- After a revocation, one retention whose listing completes records a scan
naming every file the folder holds. -/
theorem revoked_listing_completes (writer : Nat) (events : List FolderEvent) :
    ∃ paths, (Folder.run writer true (events ++ [.revoke, .retain true])).scan = .complete paths := by
  unfold Folder.run
  rw [List.foldl_append]
  generalize events.foldl (Folder.step writer true) Folder.initial = f
  obtain ⟨cut, files, scan⟩ := f
  cases scan with
  | pending => exact ⟨files.map (Path.file writer), by simp [Folder.step, Folder.listing, observeFiles]⟩
  | complete paths => exact ⟨paths, by simp [Folder.step, observeFiles]⟩

/-- The rule T210 replaced: Ana lists Carol's folder after her cut-off and
finds nothing. Carol's access is restored and taken away again between two
retentions, and she uploads photo 1 meanwhile. Forgetting the scan only when
a retention sees the access restored keeps the empty scan, so photo 1 is
never listed. -/
theorem retention_forgetting_counterexample :
    let f := Folder.run 3 false [.revoke, .retain true, .restore, .upload 1, .revoke, .retain true]
    f.scan = .complete [] ∧ 1 ∈ f.files ∧ ¬ f.Sound 3 := by
  refine ⟨by decide, by decide, fun sound => ?_⟩
  have := (sound [] (by decide)).2 1 (by decide)
  simp at this

end CovenIO.Retired
