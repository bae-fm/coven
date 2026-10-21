import CovenIO.Waiting
import CovenStorage.Files

namespace CovenIO.Downloads

/-- How a provider failure clears (E5). -/
inductive Clears where
  | byItself | byPerson | never
  deriving DecidableEq, Repr

/-- Provider failures use the shared three classes (§17.7; Rust `storage_clears`). -/
inductive Reason where
  | noStorage | network | disk | database
  | storage (clears : Clears)
  | object (path : Path)
  | range (path : Path) (index : Nat)
  | unavailable (reason : CovenStorage.Files.MissingReason)
  | key (parent : Path)
  | authentication | contentHash
  deriving DecidableEq, Repr

def automatic : Reason → Bool
  | .storage .byItself | .network | .object _ | .range _ _ | .key _ | .disk | .database => true
  | .storage _ | .noStorage | .unavailable _ | .authentication | .contentHash => false

def connectionRetry : Reason → Bool
  | .storage .byPerson | .noStorage => true
  | _ => false

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

/-- A local map has exactly one current record per fixed subject (§19.1, E5).
There is no parallel progress or pending state in the pin worker. -/
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

def canRetry (s : State) (timer : Waiting.Timer) (now : Nat) (connectionChanged : Bool) : Bool :=
  match pending s with
  | none => false
  | some reason => if automatic reason then Waiting.due timer now
    else connectionRetry reason && connectionChanged && decide (timer.provider ≤ now)

theorem permanent_never_retries (s : State) (timer : Waiting.Timer) (now : Nat) (reason : Reason)
    (has_pending : pending s = some reason) (permanent : automatic reason = false)
    (no_connection : connectionRetry reason = false) (changed : Bool) :
    canRetry s timer now changed = false := by simp [canRetry, has_pending, permanent, no_connection]

theorem temporary_uses_shared_delay (s : State) (timer : Waiting.Timer) (now : Nat)
    (reason : Reason) (has_pending : pending s = some reason) (temporary : automatic reason = true)
    (changed : Bool) :
    canRetry s timer now changed = Waiting.due timer now := by simp [canRetry, has_pending, temporary]

theorem permanent_storage_never_retries (s : State) (timer : Waiting.Timer) (now : Nat)
    (has_pending : pending s = some (.storage .never)) (changed : Bool) :
    canRetry s timer now changed = false :=
  permanent_never_retries s timer now _ has_pending rfl rfl changed

theorem person_waits_for_connection (s : State) (timer : Waiting.Timer) (now : Nat)
    (has_pending : pending s = some (.storage .byPerson)) (changed : Bool) :
    canRetry s timer now changed = (changed && decide (timer.provider ≤ now)) := by
  simp [canRetry, has_pending, automatic, connectionRetry]

theorem connection_retry_keeps_cooldown (s : State) (timer : Waiting.Timer) (now : Nat)
    (has_pending : pending s = some (.storage .byPerson))
    (ready : canRetry s timer now true = true) : timer.provider ≤ now := by
  simpa [person_waits_for_connection s timer now has_pending] using ready

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
    (permanent : automatic reason = false) (no_connection : connectionRetry reason = false)
    (timer : Waiting.Timer) (now : Nat) (changed : Bool) :
    pending (evict (.unfinished rs reason) index) = some reason ∧
    canRetry (evict (.unfinished rs reason) index) timer now changed = false := by
  simp [evict, pending, canRetry, permanent, no_connection]

theorem completed_eviction_does_not_refill (rs : List Range) (index : Nat)
    (timer : Waiting.Timer) (now : Nat) (changed : Bool) :
    canRetry (evict (.complete rs false) index) timer now changed = false := rfl

def rowWait (parent : Path) : Subject × State :=
  (.encryptedRow parent, .unfinished [] (.key parent))

theorem key_wait_names_parent (parent : Path) :
    (rowWait parent).1 = .encryptedRow parent ∧ pending (rowWait parent).2 = some (.key parent) :=
  ⟨rfl, rfl⟩

end CovenIO.Downloads
