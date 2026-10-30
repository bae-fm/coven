import CovenIO.Storage
import CovenStorage.Files

namespace CovenIO.Downloads

/-- How a provider failure clears (E5). -/
inductive Clears where
  | byItself | byPerson | never
  deriving DecidableEq, Repr

/-- Provider failures use the shared three classes (§17.8; Rust `storage_clears`). -/
inductive Reason where
  | noStorage | network | disk | database
  | storage (clears : Clears)
  | range (path : Path) (index : Nat)
  | unavailable (reason : CovenStorage.Files.MissingReason)
  | key (parent : Path)
  | authentication | contentHash
  deriving DecidableEq, Repr

/-- What lets a failed download run again (§17.6, E5). Only the app's reads
and pins download, and their calls return the failure, so nothing retries a
download on its own: the app's next read or pin tries again. Only a pin's
failure is recorded. Refused bytes wait for an applicable update, and a
provider's outright refusal never clears (Rust `PendingRecord::retry` for
`PendingSubject::Download`). -/
inductive Retry where
  | nextCall | afterUpdate | never
  deriving DecidableEq, Repr

def retry : Reason → Retry
  | .authentication | .contentHash => .afterUpdate
  | .storage .never => .never
  | .storage .byItself | .storage .byPerson | .noStorage | .network | .disk | .database
  | .range _ _ | .unavailable _ | .key _ => .nextCall

/-- Before decrypting the row there is a pending write/snapshot subject, no
fabricated file reference. Acquiring its key opens the retained row bytes. -/
inductive Subject where
  | encryptedRow (parent : Path)
  | file (reference : CovenStorage.Files.Reference)
  deriving DecidableEq, Repr

def first : List (Except Reason Unit) → Option Reason
  | [] => none
  | .error r :: _ => some r
  | .ok _ :: rest => first rest

theorem first_unmet (passed : List Unit) (reason : Reason) (rest : List (Except Reason Unit)) :
    first (passed.map Except.ok ++ .error reason :: rest) = some reason := by
  induction passed with
  | nil => rfl
  | cons u us ih => simpa [first] using ih

structure Range where
  index : Nat
  bytes : Bytes
  deriving DecidableEq, Repr

/-- `idle` holds verified ranges with nothing pending: what eviction leaves,
and a file no pin has failed for. -/
inductive State where
  | unfinished (verified : List Range) (reason : Reason)
  | complete (verified : List Range) (pinned : Bool)
  | idle (verified : List Range)
  deriving DecidableEq, Repr

def ranges : State → List Range
  | .unfinished rs _ | .complete rs _ | .idle rs => rs

def progress (s : State) : Nat := ((ranges s).map (fun r => r.bytes.length)).sum

def pending : State → Option Reason
  | .unfinished _ r => some r
  | .complete _ _ | .idle _ => none

def pinned : State → Bool | .complete _ pin => pin | _ => false
def completed : State → Bool | .complete _ _ => true | _ => false

/-- A local map has exactly one current record per fixed subject (§19.1, E5).
No reader keeps a parallel progress or pending state. -/
abbrev Database := Subject → State
def pendingAt (db : Database) (s : Subject) : Option Reason := pending (db s)

theorem one_pending (rs : List Range) (reason : Reason) :
    ∃ r, pending (.unfinished rs reason) = some r ∧
      ∀ other, pending (.unfinished rs reason) = some other → other = r := by
  exact ⟨reason, rfl, fun r eq => (Option.some.inj eq).symm⟩

def publish (db : Database) (subject : Subject) (state : State) : Database :=
  fun s => if s = subject then state else db s

theorem pending_replaced (db : Database) (subject : Subject) (state : State) :
    pendingAt (publish db subject state) subject = pending state := by
  simp [pendingAt, publish]

/-- These outcomes come from the real provider, cryptographic reader and
local transaction. The model does not implement those boundaries. Checks
are in processing order; only a fully successful range reaches commit. A
failed pin records its first unmet condition; a failed read returns it to
its caller and leaves the state as it was. -/
structure Attempt where
  path : Path
  fetched : Except Reason Unit
  authenticated : Bool
  hashMatches : Bool
  diskCommitted : Bool
  databaseCommitted : Bool
  range : Range
  chunks : Nat
  last : Bool
  pin : Bool
  deriving Repr

def Attempt.checks (a : Attempt) : List (Except Reason Unit) :=
  [a.fetched,
   if a.authenticated then .ok () else .error .authentication,
   if !a.last || a.hashMatches then .ok () else .error .contentHash,
   if a.diskCommitted then .ok () else .error .disk,
   if a.databaseCommitted then .ok () else .error .database]

def apply (s : State) (attempt : Attempt) : State :=
  match first attempt.checks with
  | some reason => if attempt.pin then .unfinished (ranges s) reason else s
  | none =>
      let retained := if attempt.range.index ∈ (ranges s).map Range.index then ranges s
        else ranges s ++ [attempt.range]
      if attempt.last && (List.range attempt.chunks).all (fun index => index ∈ retained.map Range.index)
        then .complete retained attempt.pin
      else .unfinished retained (.range attempt.path (attempt.range.index + 1))

theorem failure_atomic (s : State) (attempt : Attempt) (reason : Reason)
    (failed : first attempt.checks = some reason) (pin : attempt.pin = true) :
    ranges (apply s attempt) = ranges s ∧ progress (apply s attempt) = progress s ∧
    pending (apply s attempt) = some reason ∧ pinned (apply s attempt) = false ∧
    completed (apply s attempt) = false := by
  simp [apply, failed, pin, ranges, progress, pending, pinned, completed]

/-- §17.6: a read's failure goes to its caller only. Nothing is pending after
it that was not pending before: no record names work nothing will finish. -/
theorem failed_read_records_nothing (s : State) (attempt : Attempt) (reason : Reason)
    (failed : first attempt.checks = some reason) (read : attempt.pin = false) :
    apply s attempt = s := by
  simp [apply, failed, read]

theorem progress_never_decreases (s : State) (attempt : Attempt) :
    progress s ≤ progress (apply s attempt) := by
  cases hf : first attempt.checks with
  | some reason => cases hp : attempt.pin <;> simp [apply, hf, hp, progress, ranges]
  | none =>
      by_cases present : attempt.range.index ∈ (ranges s).map Range.index
      all_goals
        simp only [apply, hf, present, ↓reduceIte]
        split <;> simp [progress, ranges, List.map_append, List.sum_append]

theorem committed_ranges_unique (s : State) (attempt : Attempt)
    (unique : ((ranges s).map Range.index).Nodup) :
    ((ranges (apply s attempt)).map Range.index).Nodup := by
  cases hf : first attempt.checks with
  | some reason => cases hp : attempt.pin <;> simpa [apply, hf, hp, ranges] using unique
  | none =>
      by_cases present : attempt.range.index ∈ (ranges s).map Range.index
      · simp only [apply, hf, present, ↓reduceIte]
        split <;> exact unique
      · simp only [apply, hf, present, ↓reduceIte]
        split <;> simpa [ranges, List.map_append, List.nodup_append] using And.intro unique present

/-- A completion an attempt makes holds every range. A failed read leaves a
state as it was, so only an attempt on an incomplete state is asked about. -/
theorem completion_has_all_ranges (s : State) (attempt : Attempt)
    (incomplete : completed s = false) (done : completed (apply s attempt) = true) :
    ∀ index, index < attempt.chunks → index ∈ (ranges (apply s attempt)).map Range.index := by
  cases hf : first attempt.checks with
  | some reason =>
      cases hp : attempt.pin
      · simp [apply, hf, hp, incomplete] at done
      · simp [apply, hf, hp, completed] at done
  | none =>
      by_cases present : attempt.range.index ∈ (ranges s).map Range.index
      all_goals
        simp only [apply, hf, present, ↓reduceIte] at done ⊢
        split at done
        · rename_i covered
          have all := (Bool.and_eq_true_iff.mp covered).2
          rw [ite_eq_left covered]
          change ∀ index, index < attempt.chunks → _
          intro index bound
          exact of_decide_eq_true (List.all_eq_true.mp all index (List.mem_range.mpr bound))
        · simp [completed] at done

theorem committed_range_checked (a : Attempt) (success : first a.checks = none) :
    a.authenticated = true ∧ (a.last = true → a.hashMatches = true) ∧
      a.diskCommitted = true ∧ a.databaseCommitted = true := by
  cases hf : a.fetched <;> cases ha : a.authenticated <;>
    cases hl : a.last <;> cases hh : a.hashMatches <;>
    cases hd : a.diskCommitted <;> cases hb : a.databaseCommitted <;>
    simp_all [Attempt.checks, first]

def evict (s : State) (index : Nat) : State :=
  let retained := (ranges s).filter (fun r => r.index != index)
  match s with
  | .unfinished _ reason => .unfinished retained reason
  | .complete _ true => s
  | .complete _ false | .idle _ => .idle retained

/-- Whether a download runs again: only when the app `called` again, and only
for a failure that call can clear. -/
def canRetry (s : State) (called : Bool) : Bool :=
  match pending s with
  | none => false
  | some reason => called && retry reason == .nextCall

theorem never_on_its_own (s : State) : canRetry s false = false := by
  cases s <;> simp [canRetry, pending]

theorem next_call_retries (s : State) (reason : Reason)
    (has_pending : pending s = some reason) (clears : retry reason = .nextCall) :
    canRetry s true = true := by simp [canRetry, has_pending, clears]

theorem refusal_never_retries (s : State) (reason : Reason)
    (has_pending : pending s = some reason) (refused : retry reason ≠ .nextCall)
    (called : Bool) : canRetry s called = false := by
  simp [canRetry, has_pending, refused]

theorem permanent_storage_never_retries (s : State)
    (has_pending : pending s = some (.storage .never)) (called : Bool) :
    canRetry s called = false :=
  refusal_never_retries s _ has_pending (by decide) called

theorem person_waits_for_the_call (s : State)
    (has_pending : pending s = some (.storage .byPerson)) (called : Bool) :
    canRetry s called = called := by
  simp [canRetry, has_pending, retry]

/-- §17.5, §17.6: a confirmed absence at the path a record's coordinates
name. Coordinates say cloud only once storage confirmed the bytes, so nothing
will bring them back on their own: the download waits for the app, never for
an upload. A file this device uploads has its source failure read locally,
and the upload retries it. -/
def absence : Reason := .unavailable .lost

theorem absence_waits_for_the_app : retry absence = .nextCall := rfl

theorem eviction_preserves_refusal (rs : List Range) (reason : Reason) (index : Nat)
    (refused : retry reason ≠ .nextCall) (called : Bool) :
    pending (evict (.unfinished rs reason) index) = some reason ∧
    canRetry (evict (.unfinished rs reason) index) called = false := by
  simp [evict, pending, canRetry, refused]

theorem completed_eviction_does_not_refill (rs : List Range) (index : Nat) (called : Bool) :
    canRetry (evict (.complete rs false) index) called = false := rfl

def rowWait (parent : Path) : Subject × State :=
  (.encryptedRow parent, .unfinished [] (.key parent))

theorem key_wait_names_parent (parent : Path) :
    (rowWait parent).1 = .encryptedRow parent ∧ pending (rowWait parent).2 = some (.key parent) :=
  ⟨rfl, rfl⟩

end CovenIO.Downloads
