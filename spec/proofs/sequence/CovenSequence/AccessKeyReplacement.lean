/-!
# Replacing a member's S3 key on their own device

Dan replaces his S3 key A with B, which he made in the provider's console
(§14.3). Two things change: the key this device's custody connects with, a
keychain entry, and the key the store's history records as Dan's, an admin
entry the device journals and claims (`SetAccess`). The keychain and the
journal share no transaction, so a crash can come between any two steps.

History is what the app reads to tell Ana which key to delete when she
removes Dan. Custody must never connect with a key history doesn't record:
the person would delete the recorded one, and the other would keep
reaching the store.

`Rules.correct` keeps B apart in custody first (`keep`), journals it, and
connects with B only once history records it (`swap`); a kept key with no
journal row and no record goes (`drop`). `Rules.custodyFirst`, as before,
commits B to custody, then journals it: a crash between the two leaves
custody on B, history on A, and nothing journaled, for good.
-/
namespace CovenSequence.AccessKeyReplacement

/-- `true` is B, `false` is A. -/
structure World where
  /-- The key custody connects with. -/
  custody : Bool
  /-- Custody keeps B apart, not yet connecting with it. -/
  kept : Bool
  /-- The journal runs `SetAccess` for B. -/
  row : Bool
  /-- History records B as the member's key. -/
  history : Bool
  deriving DecidableEq, Repr

def start : World := ⟨false, false, false, false⟩

structure Rules where
  custodyFirst : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨false⟩
def Rules.before : Rules := ⟨true⟩

inductive Act where
  /-- The call keeps B apart in custody. -/
  | keep
  /-- The call commits B as the key custody connects with. -/
  | commit
  /-- The call journals `SetAccess` for B. -/
  | journal
  /-- The claim lands: history records B, and the operation ends. -/
  | land
  /-- The operation ends refused, recording nothing. -/
  | refuse
  /-- Settling connects with the kept key history records. -/
  | swap
  /-- Settling lets go of a kept key nothing journals or records. -/
  | drop
  deriving DecidableEq, Repr

def enabled (R : Rules) (w : World) : Act → Bool
  | .keep => !R.custodyFirst && !w.custody && !w.kept && !w.row && !w.history
  | .commit => R.custodyFirst && !w.custody && !w.row && !w.history
  | .journal =>
    !w.row && !w.history && (if R.custodyFirst then w.custody else w.kept)
  | .land | .refuse => w.row
  | .swap => !R.custodyFirst && w.kept && w.history
  | .drop => !R.custodyFirst && w.kept && !w.row && !w.history

def act (w : World) : Act → World
  | .keep => { w with kept := true }
  | .commit => { w with custody := true }
  | .journal => { w with row := true }
  | .land => { w with row := false, history := true }
  | .refuse => { w with row := false }
  | .swap => { w with custody := true, kept := false }
  | .drop => { w with kept := false }

/-- What coven does on its own once the call is gone: its passes run the
journal, and settling looks at custody beside history. -/
def coven : Act → Bool
  | .land | .refuse | .swap | .drop => true
  | _ => false

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' : World} (a : Act) : enabled R w a = true → Steps R (act w a) w' → Steps R w w'

def Reachable (R : Rules) (w : World) : Prop := Steps R start w

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act w a) rest else none

theorem key_trace_steps (R : Rules) :
    ∀ (acts : List Act) (w w' : World), trace R w acts = some w' → Steps R w w'
  | [], w, w', h => by
    simp only [trace, Option.some.injEq] at h
    subst h
    exact .refl w
  | a :: rest, w, w', h => by
    simp only [trace] at h
    split at h
    · exact .step a ‹_› (key_trace_steps R rest _ _ h)
    · cases h

/-- What holds of every world the correct order reaches. -/
def Inv (w : World) : Prop :=
  (w.custody = true → w.history = true) ∧ (w.row = true → w.kept = true) ∧
    (w.history = true → w.custody = true ∨ w.kept = true) ∧
    (w.custody = true → w.kept = false) ∧ (w.row = true → w.history = false)

theorem key_inv_step (w : World) (a : Act) (inv : Inv w) (e : enabled Rules.correct w a = true) :
    Inv (act w a) := by
  obtain ⟨c, k, r, h⟩ := w
  cases a <;> cases c <;> cases k <;> cases r <;> cases h <;>
    simp_all [Inv, act, enabled, Rules.correct]

theorem key_reachable_inv {w w' : World} (h : Steps Rules.correct w w') (inv : Inv w) : Inv w' := by
  induction h with
  | refl => exact inv
  | step a e _ ih => exact ih (key_inv_step _ a inv e)

/-- Custody connects with B only once history records it: in every world
the correct order reaches, crashes between any two steps included. -/
theorem custody_never_ahead {w : World} (h : Reachable Rules.correct w) (b : w.custody = true) :
    w.history = true :=
  (key_reachable_inv h (by simp [Inv, start])).1 b

/-- The steps settling takes from `w`: the journaled claim, then the kept
key, swapped in once recorded or let go. -/
def settling (w : World) : List Act :=
  (if w.row then [.land] else []) ++
    (if w.kept && (w.row || w.history) then [.swap] else if w.kept then [.drop] else [])

/-- From every reachable world, coven's own steps end with no operation
journaled, no key kept apart, and custody on the key history records. -/
theorem replacement_settles {w : World} (h : Reachable Rules.correct w) :
    (settling w).all coven = true ∧
      ∃ w', trace Rules.correct w (settling w) = some w' ∧
        w'.row = false ∧ w'.kept = false ∧ w'.custody = w'.history := by
  have inv := key_reachable_inv h (by simp [Inv, start])
  obtain ⟨c, k, r, hi⟩ := w
  cases c <;> cases k <;> cases r <;> cases hi <;>
    simp_all [Inv, settling, trace, enabled, act, coven, Rules.correct]

/-- *Before.* Committing B to custody first: a crash right after leaves
custody on B, history on A and nothing journaled, and no step of coven's
own changes that. -/
theorem custody_first_strands :
    ∃ w, trace Rules.before start [.commit] = some w ∧ w.custody = true ∧
      w.history = false ∧ ∀ a, coven a = true → enabled Rules.before w a = false := by
  refine ⟨_, rfl, rfl, rfl, ?_⟩
  intro a
  cases a <;> simp [coven, enabled, act, Rules.before, start]

end CovenSequence.AccessKeyReplacement
