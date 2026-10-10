import CovenQueue.Model

namespace CovenQueue

structure Counters where
  writes : Nat
  entries : Nat
  snapshots : Nat
  deriving DecidableEq, Repr

structure Evidence where
  custody : Option Nat
  replaced : Bool
  /-- Complete authenticated listing, snapshot prefixes and posted positions.
  A covered, deleted log object still contributes to this maximum (§10). -/
  observed : Counters
  deriving DecidableEq, Repr

inductive Preflight where
  | ready
  | pending
  | identityMismatch
  | storageAhead
  | replaced
  deriving DecidableEq, Repr

/-- No read failure is interpreted as empty storage (§10, E5). -/
def checkIdentity (device : Nat) (reserved : Counters) : Option Evidence → Preflight
  | none => .pending
  | some e =>
    if e.custody ≠ some device then .identityMismatch
    else if e.replaced then .replaced
    else if reserved.writes < e.observed.writes ||
      reserved.entries < e.observed.entries || reserved.snapshots < e.observed.snapshots
    then .storageAhead
    else .ready

theorem ready_owns_counters (d : Nat) (r : Counters) (e : Option Evidence)
    (h : checkIdentity d r e = .ready) :
    ∃ v, e = some v ∧ v.custody = some d ∧ v.replaced = false ∧
      v.observed.writes ≤ r.writes ∧ v.observed.entries ≤ r.entries ∧
      v.observed.snapshots ≤ r.snapshots := by
  cases e with
  | none => simp [checkIdentity] at h
  | some e =>
    simp only [checkIdentity] at h
    split at h
    · contradiction
    · rename_i hc
      split at h
      · contradiction
      · rename_i hr
        split at h
        · contradiction
        · rename_i hn
          refine ⟨e, rfl, by simpa using hc, by simpa using hr, ?_⟩
          simp only [Bool.or_eq_true, decide_eq_true_eq, not_or] at hn
          omega

theorem failed_check_sends_nothing (d : Nat) (r : Counters) :
    checkIdentity d r none ≠ .ready := by simp [checkIdentity]

end CovenQueue
