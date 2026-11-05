/-! Rechecking a refusal (§19.1, "Refusals and failures"), as
`PendingRecord::can_retry` and `_coven_pending.build` run it.

A refusal is a verdict on bytes that never change: an immutable object, or a
complete authenticated file chunk. What the check reads besides the bytes
never changes either: a refused slot stops the device (§5.4), so the history
before it never moves, and a file's record names the content hash its bytes
must match. So the verdict is a function of the input and of the code that
judged it (`judge`), and only another build of coven can judge it differently.

The record names the build that last refused it (`Record.build`). A build
reads a record another build wrote as due for a recheck (`due`). Rechecking
runs the running build's own check: a second refusal records the running
build, and an acceptance ends the record (`recheck`).

Proved here: a build that would judge differently always rechecks
(`differing_build_rechecks`), as does any other build
(`other_build_rechecks`). A build never rechecks a refusal it recorded itself
(`own_refusal_waits`), and waiting loses nothing, since it would refuse again
(`waiting_loses_nothing`). A recheck that refuses again leaves a record its
build doesn't recheck (`recheck_once`): each build rechecks at most once, and
never loops. Keyed instead to a number several builds share, such as a
package version that development never moves, a refusal one build recorded
is never rechecked by another build with that number, even one that would
accept the input (`shared_key_strands`). -/

namespace CovenIO.Recheck

/-- A refusal's record: the build of coven that last refused the input. -/
structure Record (Build : Type) where
  build : Build
  deriving DecidableEq

section Builds

variable {Build Input : Type} [DecidableEq Build]

/-- Whether the running build rechecks the refusal `r` records: only a build
other than the one that recorded it. -/
def due (r : Record Build) (running : Build) : Bool :=
  decide (r.build ≠ running)

/-- The running build judges `x` again; `judge b x` is whether build `b`
refuses it. Refused again, the record names the running build; accepted, the
record ends. -/
def recheck (judge : Build → Input → Bool) (x : Input) (running : Build) :
    Option (Record Build) :=
  if judge running x then some ⟨running⟩ else none

theorem own_refusal_waits (b : Build) : due ⟨b⟩ b = false := by
  simp [due]

theorem other_build_rechecks (r : Record Build) (running : Build)
    (other : r.build ≠ running) : due r running = true := by
  simp [due, other]

/-- Every build whose verdict on the input differs from the recording build's
rechecks it. -/
theorem differing_build_rechecks (judge : Build → Input → Bool) (x : Input)
    (r : Record Build) (running : Build) (differs : judge running x ≠ judge r.build x) :
    due r running = true :=
  other_build_rechecks r running (fun same => differs (by rw [same]))

/-- A build that doesn't recheck would only refuse again. -/
theorem waiting_loses_nothing (judge : Build → Input → Bool) (x : Input)
    (r : Record Build) (running : Build) (refused : judge r.build x = true)
    (waits : due r running = false) : recheck judge x running = some r := by
  cases r with
  | mk b =>
    simp only [due, decide_eq_false_iff_not, Classical.not_not] at waits
    subst waits
    simp [recheck, refused]

/-- A recheck that refuses again records its own build, which never rechecks
it again. -/
theorem recheck_once (judge : Build → Input → Bool) (x : Input) (running : Build)
    (r : Record Build) (again : recheck judge x running = some r) :
    due r running = false := by
  unfold recheck at again
  split at again
  · cases again
    exact own_refusal_waits running
  · cases again

end Builds

section Keyed

variable {Build Input : Type}

/-- Whether the running build rechecks `r` when records are keyed to
`version` of the build instead of the build. -/
def dueBy {V : Type} [DecidableEq V] (version : Build → V) (r : Record Build)
    (running : Build) : Bool :=
  decide (version r.build ≠ version running)

/-- Keyed to a number two builds share, a refusal the first recorded is never
rechecked by the second, though the second would accept the input. -/
theorem shared_key_strands {V : Type} [DecidableEq V] (version : Build → V)
    (judge : Build → Input → Bool) (x : Input) (old new : Build)
    (same : version old = version new) (refused : judge old x = true)
    (accepts : judge new x = false) :
    judge new x ≠ judge old x ∧ dueBy version ⟨old⟩ new = false := by
  simp [dueBy, same, refused, accepts]

end Keyed

end CovenIO.Recheck
