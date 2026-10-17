/-! A rejected self-replacement cannot release writes under an unheld binding
(§12.3). Keys are equality-preserving public-pair tokens. The child claim has
already been undone. Installing a held candidate still owes the restore-code
commit, so that branch retains the pause too. -/
namespace CovenSequence.MemberKeyReplacement

inductive Resolution where
  | blocked | activate | undo
  deriving DecidableEq

def resolve (active candidate : Nat) (current : Option Nat) : Resolution :=
  if current = some candidate then .activate
  else if current = some active then .undo else .blocked

theorem unheld_keeps_pause (active candidate : Nat) (current : Option Nat)
    (old : current ≠ some active) (new : current ≠ some candidate) :
    resolve active candidate current = .blocked := by
  simp [resolve, old, new]

theorem candidate_installs (active candidate : Nat) :
    resolve active candidate (some candidate) = .activate := by
  simp [resolve]

theorem undo_requires_active (active candidate : Nat) (current : Option Nat)
    (ended : resolve active candidate current = .undo) :
    current = some active ∧ current ≠ some candidate := by
  unfold resolve at ended
  split at ended
  · contradiction
  · split at ended
    · exact ⟨‹current = some active›, ‹current ≠ some candidate›⟩
    · contradiction

end CovenSequence.MemberKeyReplacement
