import CovenSequence.Model

namespace CovenSequence
variable {S : Sig} (R : Rules S)

/-- Every earlier permission in the ledger is the policy at that applied prefix. -/
theorem write_history_prefix (seq : Nat → Slot S) (k readThrough : Nat)
    (earlier : readThrough < k) :
    (ledgerAt R seq k).writeHistory readThrough =
      some (R.mayWrite (ledgerAt R seq readThrough)) := by
  induction k with
  | zero => omega
  | succ k ih =>
    by_cases same : readThrough = k
    · subst readThrough
      simp [ledgerAt_succ, ledgerStep, noteSlot]
    · have before : readThrough < k := by omega
      simpa [ledgerAt_succ, ledgerStep, noteSlot, same] using ih before

/-- Historical authorization agrees with evaluating the author's actual state. -/
theorem authored_permission_eq (seq : Nat → Slot S) (current readThrough : Nat)
    (read : readThrough ≤ current) (d : S.Dev) (A : S.Aud) :
    authoredPermission R (ledgerAt R seq current) current readThrough d A =
      R.mayWrite (ledgerAt R seq readThrough) d A := by
  by_cases same : readThrough = current
  · simp [authoredPermission, same]
  · simp [authoredPermission, same, write_history_prefix R seq current readThrough (by omega)]

/-- Circle permission is checked when authored; store/device/key cut-offs are
still checked immediately before landing. -/
theorem write_permission_at_authored_prefix (seq : Nat → Slot S) (k : Nat)
    (d : S.Dev) (generation : Nat) (parts : S.Aud → Option (WPart S))
    (A : S.Aud) (part : WPart S) (found : parts A = some part)
    (read : part.readThrough ≤ k) :
    audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A (.writes d generation parts) =
      (counts (ledgerAt R seq k) d generation &&
       R.mayWrite (ledgerAt R seq part.readThrough) d A &&
       decide (part.version ≤ (audAt R seq A k).version)) := by
  simp [audValid, found, authored_permission_eq R seq k part.readThrough read,
    show part.readThrough < k + 1 by omega]

end CovenSequence
