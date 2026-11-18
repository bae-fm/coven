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

/-!
## A later key after a refused one

Dan's first new key B passed its check and was taken, and its entry's claim
was refused; then Dan deleted B in the provider's console. He gives the device
a later key C, which storage accepts (§14.3). The call checks C, journals C's
entry, takes C, and only then runs B's entry through C's connection: B's
entry ends without claiming, since custody no longer holds B, and C's entry is
claimed. `Rules.takeFirst` is that order. `Rules.settleFirst` runs B's entry
through C's connection before C is taken, while custody still holds B.

A key is A, B or C. Only C is journaled and taken here, B having been taken
already; A is the key history records at the start.
-/
namespace Later

inductive Key where
  | a
  | b
  | c
  deriving DecidableEq, Repr

structure World where
  /-- The key custody connects with. -/
  custody : Key
  /-- B's entry is journaled. -/
  rowB : Bool
  /-- C's entry is journaled. -/
  rowC : Bool
  /-- The key history records as the member's. -/
  history : Key
  /-- The provider still accepts B. -/
  bLive : Bool
  deriving DecidableEq, Repr

/-- The device after B was taken and its claim refused: custody holds B,
B's entry is journaled, and history records A. -/
def stranded : World := ⟨.b, true, false, .a, true⟩

structure Rules where
  /-- The call runs B's entry through C's connection before it takes C. -/
  earlierFirst : Bool
  deriving DecidableEq, Repr

def Rules.takeFirst : Rules := ⟨false⟩
def Rules.settleFirst : Rules := ⟨true⟩

inductive Act where
  /-- The call journals C's entry, once C passed its check. -/
  | journalC
  /-- Custody connects with C. -/
  | installC
  /-- B's claim lands: history records B. -/
  | landB
  /-- B's entry runs on a connection that doesn't sign with B: it ends
  without claiming. -/
  | rollbackB
  /-- C's claim lands, once B's entry no longer holds it back. -/
  | landC
  /-- C's entry runs while custody holds another key: it ends without
  claiming. -/
  | rollbackC
  /-- The person deletes B in the provider's console. -/
  | consoleDeleteB
  deriving DecidableEq, Repr

def enabled (R : Rules) (w : World) : Act → Bool
  | .journalC => !w.rowC && w.history != .c && w.custody != .c
  | .installC => w.rowC && w.custody != .c
  | .landB => w.rowB && w.custody == .b && w.bLive
  | .rollbackB => w.rowB && (R.earlierFirst || w.custody != .b)
  | .landC => w.rowC && w.custody == .c && !w.rowB
  | .rollbackC => w.rowC && w.custody != .c
  | .consoleDeleteB => w.bLive

def act (w : World) : Act → World
  | .journalC => { w with rowC := true }
  | .installC => { w with custody := .c }
  | .landB => { w with rowB := false, history := .b }
  | .rollbackB => { w with rowB := false }
  | .landC => { w with rowC := false, history := .c }
  | .rollbackC => { w with rowC := false }
  | .consoleDeleteB => { w with bLive := false }

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' : World} (a : Act) : enabled R w a = true → Steps R (act w a) w' → Steps R w w'

def Reachable (R : Rules) (w : World) : Prop := Steps R stranded w

def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act w a) rest else none

/-- Custody's key is the one history records or one whose entry is
journaled, and history records C only where custody holds C. -/
def Inv (w : World) : Prop :=
  (w.custody = w.history ∨ (w.custody = .b ∧ w.rowB = true) ∨
      (w.custody = .c ∧ w.rowC = true)) ∧
    (w.history = .c → w.custody = .c)

theorem later_inv_step (w : World) (a : Act) (inv : Inv w)
    (e : enabled Rules.takeFirst w a = true) : Inv (act w a) := by
  obtain ⟨cu, rb, rc, h, l⟩ := w
  cases a <;> cases cu <;> cases rb <;> cases rc <;> cases h <;> cases l <;>
    simp_all [Inv, act, enabled, Rules.takeFirst]

theorem later_reachable_inv {w w' : World} (h : Steps Rules.takeFirst w w') (inv : Inv w) :
    Inv w' := by
  induction h with
  | refl => exact inv
  | step a e _ ih => exact ih (later_inv_step _ a inv e)

/-- In every world the take-first order reaches from the stranded device,
kills between any two steps included, custody holds a key that history
records or whose entry is journaled. -/
theorem later_custody_has_record_or_entry {w : World} (h : Reachable Rules.takeFirst w) :
    w.custody = w.history ∨ (w.custody = .b ∧ w.rowB = true) ∨
      (w.custody = .c ∧ w.rowC = true) :=
  (later_reachable_inv h (by simp [Inv, stranded])).1

/-- The call's steps with C from `w`: it journals C's entry unless one is
journaled or C is recorded, takes C, ends B's entry, and C's claim lands. -/
def calling (w : World) : List Act :=
  (if w.rowC || w.history == .c then [] else [.journalC]) ++
    (if w.custody == .c then [] else [.installC]) ++
    (if w.rowB then [.rollbackB] else []) ++
    (if w.rowC || w.history != .c then [.landC] else [])

/-- From every world the take-first order reaches, B refused or not, the
call with C finishes: history records C, custody holds C, and no entry stays
journaled. B's entry never blocks C's. -/
theorem later_key_finishes {w : World} (h : Reachable Rules.takeFirst w) :
    ∃ w', trace Rules.takeFirst w (calling w) = some w' ∧
      w'.rowB = false ∧ w'.rowC = false ∧ w'.history = .c ∧ w'.custody = .c := by
  have inv := later_reachable_inv h (by simp [Inv, stranded])
  obtain ⟨cu, rb, rc, hi, l⟩ := w
  cases cu <;> cases rb <;> cases rc <;> cases hi <;> cases l <;>
    simp_all [Inv, calling, trace, enabled, act, Rules.takeFirst]

/-- *Before.* Running B's entry through C's connection before C is taken:
once B is deleted, B's entry ends while custody still holds B, and a kill
before C is journaled leaves custody on a key that history doesn't record
and no entry names. -/
theorem settling_first_misrecords :
    ∃ w, trace Rules.settleFirst stranded [.consoleDeleteB, .rollbackB] = some w ∧
      w.custody = .b ∧ w.history = .a ∧ w.rowB = false ∧ w.rowC = false :=
  ⟨_, rfl, rfl, rfl, rfl, rfl⟩

end Later

end CovenSequence.AccessKeyReplacement
