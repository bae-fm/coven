/-!
# Using a new S3 key on a member's own device

Dan's S3 key A stops serving him, or he replaces it, and he gives his device a
key B he made in the provider's console (§14.3). Two things change: the key
this device's custody connects with, a keychain entry, and the key the store's
history records as Dan's, an admin entry the device journals and claims
(`SetAccess`). The keychain and the journal share no transaction, so the
device can be killed between any two steps.

History is what the app reads to tell Ana which key to delete when she removes
Dan. Whether a key reaches storage is the provider's business: B is live from
the moment Dan makes it, whether or not a device uses it, and A may be deleted
in the console at any time.

`Rules.correct` journals the entry that records B first, then takes B into
custody, and claims the entry with B. An entry that runs while custody still
holds A is an install that never happened: it ends without claiming
(`rollback`). So no reachable state has custody on B without history on B or
an entry journaled for B, and an entry left by a kill ends in coven's own
passes either way.
`Rules.keptApart` holds B back until history records it and claims with A: once
A is deleted in the console, no claim can land and B is never used.

A claim history refuses is left out: it happens only once Dan's removal has
applied, when no access record is read.
-/
namespace CovenSequence.AccessKeyReplacement

/-- `true` is B, `false` is A. -/
structure World where
  /-- The key custody connects with. -/
  custody : Bool
  /-- The journal runs `SetAccess` for B. -/
  row : Bool
  /-- History records B as the member's key. -/
  history : Bool
  /-- The provider still accepts A. -/
  oldLive : Bool
  deriving DecidableEq, Repr

def start : World := ⟨false, false, false, true⟩

structure Rules where
  /-- The entry is journaled first, B is taken at once, and the entry is
  claimed with B. -/
  atOnce : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true⟩
def Rules.keptApart : Rules := ⟨false⟩

inductive Act where
  /-- The call journals `SetAccess` for B. -/
  | journal
  /-- Custody connects with B. -/
  | install
  /-- The claim lands: history records B, and the operation ends. -/
  | land
  /-- The entry runs while custody still holds A: it ends without claiming. -/
  | rollback
  /-- The person deletes A in the provider's console. -/
  | consoleDelete
  deriving DecidableEq, Repr

def enabled (R : Rules) (w : World) : Act → Bool
  | .journal => !w.row && !w.history && (!R.atOnce || !w.custody)
  | .install => if R.atOnce then w.row && !w.custody else w.history && !w.custody
  | .land => w.row && (if R.atOnce then w.custody else w.oldLive)
  | .rollback => R.atOnce && w.row && !w.custody
  | .consoleDelete => w.oldLive

def act (w : World) : Act → World
  | .journal => { w with row := true }
  | .install => { w with custody := true }
  | .land => { w with row := false, history := true }
  | .rollback => { w with row := false }
  | .consoleDelete => { w with oldLive := false }

/-- What coven does on its own once the call is gone: its passes run the
journal. Nothing of coven's looks at custody beside history. -/
def coven : Act → Bool
  | .land | .rollback => true
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
  (w.custody = true → w.history = true ∨ w.row = true) ∧ (w.history = true → w.custody = true) ∧
    (w.row = true → w.history = false)

theorem key_inv_step (w : World) (a : Act) (inv : Inv w) (e : enabled Rules.correct w a = true) :
    Inv (act w a) := by
  obtain ⟨c, r, h, o⟩ := w
  cases a <;> cases c <;> cases r <;> cases h <;> cases o <;>
    simp_all [Inv, act, enabled, Rules.correct]

theorem key_reachable_inv {w w' : World} (h : Steps Rules.correct w w') (inv : Inv w) : Inv w' := by
  induction h with
  | refl => exact inv
  | step a e _ ih => exact ih (key_inv_step _ a inv e)

/-- In every world the correct order reaches, kills between any two steps
included, custody holds B only where history records B or an entry is
journaled for B. -/
theorem custody_has_record_or_entry {w : World} (h : Reachable Rules.correct w)
    (b : w.custody = true) : w.history = true ∨ w.row = true :=
  (key_reachable_inv h (by simp [Inv, start])).1 b

/-- History records B only where custody connects with B. -/
theorem record_follows_custody {w : World} (h : Reachable Rules.correct w)
    (b : w.history = true) : w.custody = true :=
  (key_reachable_inv h (by simp [Inv, start])).2.1 b

/-- The steps coven's own passes take from `w`: a journaled entry claims
when custody holds B, and ends without claiming when it holds A. -/
def settling (w : World) : List Act :=
  if w.row then (if w.custody then [.land] else [.rollback]) else []

/-- From every reachable world, coven's own steps end with no entry journaled
and custody on the key history records. -/
theorem coven_settles {w : World} (h : Reachable Rules.correct w) :
    (settling w).all coven = true ∧
      ∃ w', trace Rules.correct w (settling w) = some w' ∧
        w'.row = false ∧ w'.custody = w'.history := by
  have inv := key_reachable_inv h (by simp [Inv, start])
  obtain ⟨c, r, hi, o⟩ := w
  cases c <;> cases r <;> cases hi <;> cases o <;>
    simp_all [Inv, settling, trace, enabled, act, coven, Rules.correct]

/-- The steps the call takes on a retry from `w`: it journals the entry when
none is journaled or recorded, takes B, and the claim lands. -/
def retrying (w : World) : List Act :=
  (if w.row || w.history then [] else [.journal]) ++ (if w.custody then [] else [.install]) ++
    (if w.history then [] else [.land])

/-- From every reachable world, a call with B journals the entry unless one is
journaled or recorded, takes B, and the claim, coven's own step, lands:
history records B, custody holds B, and nothing stays journaled. This assumes
the claim lands, as the other models do. -/
theorem retry_finishes {w : World} (h : Reachable Rules.correct w) :
    ∃ w', trace Rules.correct w (retrying w) = some w' ∧
      w'.row = false ∧ w'.history = true ∧ w'.custody = true := by
  have inv := key_reachable_inv h (by simp [Inv, start])
  obtain ⟨cu, r, hi, o⟩ := w
  cases cu <;> cases r <;> cases hi <;> cases o <;>
    simp_all [Inv, retrying, trace, enabled, act, Rules.correct]

/-- *Before.* Keeping B apart until history records it, and claiming with A:
if the person deletes A in the console before the claim lands, nothing is
enabled, so history never records B and custody never takes it. -/
theorem kept_apart_strands :
    ∃ w, trace Rules.keptApart start [.journal, .consoleDelete] = some w ∧
      w.history = false ∧ w.custody = false ∧ ∀ a, enabled Rules.keptApart w a = false := by
  refine ⟨_, rfl, rfl, rfl, ?_⟩
  intro a
  cases a <;> simp [enabled, act, Rules.keptApart, start]

end CovenSequence.AccessKeyReplacement
