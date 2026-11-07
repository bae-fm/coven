/-!
# A pending record's lifetime (§19.1)

`_coven_pending` keeps one record per subject, the work that cannot proceed. A
record names the first unmet condition and stays until that condition changes
or the work settles; time passing never clears it. Rust writes it with
`save` (an upsert that changes the row only when its value, build or cause
differs), and ends it with `clear` (the work settled), `clear_slot` and
`clear_covered_slots` (a slot's progress), `clear_move`, `clear_operation`,
`end_captured_raise_waits`, and `reset`, which keeps an operation's provider
access. Opening clears every record's cause and nothing else.

The model has the records, keyed by subject, each with the condition it names,
the error behind it and what ends it.

- `time_keeps`: time passing changes no record.
- `repeat_keeps`: an attempt that meets the same condition again, with the same
  cause, changes no record.
- `reopen_keeps_condition`: opening keeps every record's condition.
- `ended_only_by_its_end`: a record is gone after an event only if the event is
  the settling of its work or the progress its record names (or a reset).
- `condition_changes_only_by_save`: a record's condition changes only when an
  attempt records another.
-/
namespace CovenIO.PendingRecords

/-- What ends a record besides its work settling. -/
inductive End where
  /-- Only the work settling. -/
  | work
  /-- This slot's progress. -/
  | slot (n : Nat)
  /-- Coverage by a loaded snapshot that passes this slot, for a write that waits. -/
  | covered (n : Nat)
  /-- This move's cancellation. -/
  | move (m : Nat)
  /-- This operation advancing, finishing or being retried; `provider` when the
  operation still has provider access to settle. -/
  | operation (o : Nat) (provider : Bool)
  /-- The raise capturing this own write. -/
  | raise (w : Nat)
  deriving DecidableEq

/-- A pending record: the condition it names (its reason), the error behind it
if one was met in this run, and what ends it. -/
structure Record where
  reason : Nat
  cause : Option Nat
  ends : End
  deriving DecidableEq

/-- The records, by subject. -/
abbrev Records := Nat → Option Record

inductive Event where
  /-- An attempt records its reason, as `save` does. -/
  | save (subject : Nat) (r : Record)
  /-- The work settled. -/
  | settle (subject : Nat)
  /-- Time passes. -/
  | tick
  /-- The store opens. -/
  | reopen
  /-- Slot `n`'s progress, which also ends a write's wait covered at `n`. -/
  | slot (n : Nat)
  /-- A loaded snapshot covers up to slot `n`. -/
  | coverage (n : Nat)
  | cancelMove (m : Nat)
  | operationMoves (o : Nat)
  /-- A raise captures the own writes through `w`. -/
  | captured (w : Nat)
  /-- An applied reset. -/
  | reset
  deriving DecidableEq

/-- Whether `e` ends a record that `x` describes, by its own end. -/
def endsRecord : Event → Record → Bool
  | .slot n, x => decide (x.ends = .slot n ∨ x.ends = .covered n)
  | .coverage n, x => match x.ends with | .covered k => decide (k ≤ n) | _ => false
  | .cancelMove m, x => decide (x.ends = .move m)
  | .operationMoves o, x => match x.ends with | .operation k _ => decide (k = o) | _ => false
  | .captured w, x => match x.ends with | .raise k => decide (k ≤ w) | _ => false
  | .reset, x => match x.ends with | .operation _ true => false | _ => true
  | _, _ => false

def step (w : Records) : Event → Records
  | .save s r => fun t => if t = s then some r else w t
  | .settle s => fun t => if t = s then none else w t
  | .tick => w
  | .reopen => fun t => (w t).map fun x => { x with cause := none }
  | e => fun t => match w t with
      | some x => if endsRecord e x then none else some x
      | none => none

/-- **Time passing changes no record.** -/
theorem time_keeps (w : Records) : step w .tick = w := rfl

/-- **An attempt that meets the same condition again changes nothing.** -/
theorem repeat_keeps (w : Records) (s : Nat) (r : Record) (there : w s = some r) :
    step w (.save s r) = w := by
  funext t
  by_cases e : t = s
  · subst e; simp [step, there]
  · simp [step, e]

/-- **Opening keeps every record's condition and what ends it**, and drops only
the cause. -/
theorem reopen_keeps_condition (w : Records) (s : Nat) :
    (step w .reopen s).map (fun x => (x.reason, x.ends)) = (w s).map (fun x => (x.reason, x.ends)) := by
  cases h : w s <;> simp [step, h]

/-- **A record is gone after an event only if the event settles its work, or is
the progress its record names, or is a reset.** -/
theorem ended_only_by_its_end (w : Records) (e : Event) (s : Nat) (x : Record)
    (there : w s = some x) (gone : step w e s = none) :
    e = .settle s ∨ endsRecord e x = true := by
  cases e with
  | save s' r =>
    by_cases h : s = s'
    · subst h; simp [step] at gone
    · simp [step, h, there] at gone
  | settle s' =>
    by_cases h : s = s'
    · exact Or.inl (by rw [h])
    · simp [step, h, there] at gone
  | tick => simp [step, there] at gone
  | reopen => simp [step, there] at gone
  | slot n => simp only [step, there] at gone; split at gone <;> simp_all
  | coverage n => simp only [step, there] at gone; split at gone <;> simp_all
  | cancelMove m => simp only [step, there] at gone; split at gone <;> simp_all
  | operationMoves o => simp only [step, there] at gone; split at gone <;> simp_all
  | captured w' => simp only [step, there] at gone; split at gone <;> simp_all
  | reset => simp only [step, there] at gone; split at gone <;> simp_all

/-- **A record's condition changes only when an attempt records another.** -/
theorem condition_changes_only_by_save (w : Records) (e : Event) (s : Nat) (x y : Record)
    (before : w s = some x) (after : step w e s = some y) (changed : y.reason ≠ x.reason) :
    ∃ r, e = .save s r ∧ r.reason = y.reason := by
  cases e with
  | save s' r =>
    by_cases h : s = s'
    · subst h
      simp only [step, before, ite_true] at after
      exact ⟨r, rfl, by rw [← Option.some.inj after]⟩
    · simp [step, h, before] at after; subst after; exact absurd rfl changed
  | settle s' => by_cases h : s = s' <;> simp_all [step]
  | tick => simp_all [step]
  | reopen => simp [step, before] at after; subst after; exact absurd rfl changed
  | slot n => simp only [step, before] at after; split at after <;> simp_all
  | coverage n => simp only [step, before] at after; split at after <;> simp_all
  | cancelMove m => simp only [step, before] at after; split at after <;> simp_all
  | operationMoves o => simp only [step, before] at after; split at after <;> simp_all
  | captured w' => simp only [step, before] at after; split at after <;> simp_all
  | reset => simp only [step, before] at after; split at after <;> simp_all

end CovenIO.PendingRecords
