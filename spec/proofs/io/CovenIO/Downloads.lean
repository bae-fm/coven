import CovenIO.Waiting
import CovenStorage.Files

namespace CovenIO.Downloads

inductive Reason where
  | noStorage | network | storage | disk | database
  | object (path : Path)
  | range (path : Path) (index : Nat)
  | unavailable (reason : CovenStorage.Files.MissingReason)
  | key (parent : Path)
  | authentication | contentHash
  deriving DecidableEq, Repr

def automatic : Reason → Bool
  | .noStorage | .network | .storage | .object _ | .range _ _ | .key _ | .disk | .database => true
  | .unavailable _ | .authentication | .contentHash => false

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

inductive State where
  | unfinished (verified : List Range) (reason : Reason)
  | complete (verified : List Range) (pinned : Bool)
  | evicted (verified : List Range)
  deriving DecidableEq, Repr

def ranges : State → List Range
  | .unfinished rs _ | .complete rs _ | .evicted rs => rs

def progress (s : State) : Nat := ((ranges s).map (fun r => r.bytes.length)).sum

def pending : State → Option Reason
  | .unfinished _ r => some r
  | .complete _ _ | .evicted _ => none

def pinned : State → Bool | .complete _ pin => pin | _ => false
def completed : State → Bool | .complete _ _ => true | _ => false

/-- A map has exactly one current record per fixed subject and reporter.
There is no parallel progress or pending state in the eager worker. -/
abbrev Database := Subject → Nat → State
def pendingAt (db : Database) (s : Subject) (reporter : Nat) : Option Reason := pending (db s reporter)

theorem one_pending (rs : List Range) (reason : Reason) :
    ∃ r, pending (.unfinished rs reason) = some r ∧
      ∀ other, pending (.unfinished rs reason) = some other → other = r := by
  exact ⟨reason, rfl, fun r eq => (Option.some.inj eq).symm⟩

def publish (db : Database) (subject : Subject) (reporter : Nat) (state : State) : Database :=
  fun s r => if s = subject ∧ r = reporter then state else db s r

theorem pending_replaced (db : Database) (subject : Subject) (reporter : Nat) (state : State) :
    pendingAt (publish db subject reporter state) subject reporter = pending state := by
  simp [pendingAt, publish]

/-- These outcomes come from the real provider, cryptographic reader and
local transaction. The model does not implement those boundaries. Checks
are in processing order; only a fully successful range reaches commit. -/
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
  | some reason => .unfinished (ranges s) reason
  | none =>
      let retained := if attempt.range.index ∈ (ranges s).map Range.index then ranges s
        else ranges s ++ [attempt.range]
      if attempt.last && (List.range attempt.chunks).all (fun index => index ∈ retained.map Range.index)
        then .complete retained attempt.pin
      else .unfinished retained (.range attempt.path (attempt.range.index + 1))

theorem failure_atomic (s : State) (attempt : Attempt) (reason : Reason)
    (failed : first attempt.checks = some reason) :
    ranges (apply s attempt) = ranges s ∧ progress (apply s attempt) = progress s ∧
    pending (apply s attempt) = some reason ∧ pinned (apply s attempt) = false ∧
    completed (apply s attempt) = false := by
  simp [apply, failed, ranges, progress, pending, pinned, completed]

theorem progress_never_decreases (s : State) (attempt : Attempt) :
    progress s ≤ progress (apply s attempt) := by
  cases hf : first attempt.checks with
  | some reason => simp [apply, hf, progress, ranges]
  | none =>
      by_cases present : attempt.range.index ∈ (ranges s).map Range.index
      all_goals
        simp only [apply, hf, present, ↓reduceIte]
        split <;> simp [progress, ranges, List.map_append, List.sum_append]

theorem committed_ranges_unique (s : State) (attempt : Attempt)
    (unique : ((ranges s).map Range.index).Nodup) :
    ((ranges (apply s attempt)).map Range.index).Nodup := by
  cases hf : first attempt.checks with
  | some reason => simpa [apply, hf, ranges] using unique
  | none =>
      by_cases present : attempt.range.index ∈ (ranges s).map Range.index
      · simp only [apply, hf, present, ↓reduceIte]
        split <;> exact unique
      · simp only [apply, hf, present, ↓reduceIte]
        split <;> simpa [ranges, List.map_append, List.nodup_append] using And.intro unique present

theorem completion_has_all_ranges (s : State) (attempt : Attempt)
    (done : completed (apply s attempt) = true) :
    ∀ index, index < attempt.chunks → index ∈ (ranges (apply s attempt)).map Range.index := by
  cases hf : first attempt.checks with
  | some reason => simp [apply, hf, completed] at done
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
  | .complete _ false | .evicted _ => .evicted retained

def canRetry (s : State) (timer : Waiting.Timer) (now : Nat) : Bool :=
  match pending s with | none => false | some reason => automatic reason && Waiting.due timer now

theorem permanent_never_retries (s : State) (timer : Waiting.Timer) (now : Nat) (reason : Reason)
    (blocked : pending s = some reason) (permanent : automatic reason = false) :
    canRetry s timer now = false := by simp [canRetry, blocked, permanent]

theorem temporary_uses_shared_delay (s : State) (timer : Waiting.Timer) (now : Nat)
    (reason : Reason) (blocked : pending s = some reason) (temporary : automatic reason = true) :
    canRetry s timer now = Waiting.due timer now := by simp [canRetry, blocked, temporary]

def absence (path : Path) (uploader : CovenStorage.Files.DeviceState)
    (source : Option CovenStorage.Files.SourceFailure) : Reason :=
  match uploader with
  | .removed => .unavailable .deviceRemoved
  | .replaced => .unavailable .deviceReplaced
  | .active => match source with | none => .object path | some f => .unavailable (.source f)

theorem removed_not_waiting (p : Path) (source : Option CovenStorage.Files.SourceFailure) :
    automatic (absence p .removed source) = false := rfl

theorem replaced_not_waiting (p : Path) (source : Option CovenStorage.Files.SourceFailure) :
    automatic (absence p .replaced source) = false := rfl

theorem active_absence_retries (p : Path) : automatic (absence p .active none) = true := rfl

theorem eviction_preserves_refusal (rs : List Range) (reason : Reason) (index : Nat)
    (permanent : automatic reason = false) (timer : Waiting.Timer) (now : Nat) :
    pending (evict (.unfinished rs reason) index) = some reason ∧
    canRetry (evict (.unfinished rs reason) index) timer now = false := by
  simp [evict, pending, canRetry, permanent]

theorem completed_eviction_does_not_refill (rs : List Range) (index : Nat)
    (timer : Waiting.Timer) (now : Nat) :
    canRetry (evict (.complete rs false) index) timer now = false := rfl

def rowWait (parent : Path) : Subject × State :=
  (.encryptedRow parent, .unfinished [] (.key parent))

theorem key_wait_names_parent (parent : Path) :
    (rowWait parent).1 = .encryptedRow parent ∧ pending (rowWait parent).2 = some (.key parent) :=
  ⟨rfl, rfl⟩

end CovenIO.Downloads
