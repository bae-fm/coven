/-!
# Deleting a store from the device, through crashes

`discard_store` deletes a store from this device only (api.md E1): this
device's keychain entries for it, its directory, and the lock files beside
the directory, in steps no transaction holds together. A crash between two
of them must leave neither a store the listing shows that can't open, nor
remains that keep the store's id taken.

Two rules make that so. `unpublishFirst`: the deletion's first step renames
the store's directory to its deletion name. That hides the store from every
listing and open, and records the deletion: from then on the directory under
its deletion name, or the store's lock files without a directory, say that a
deletion of the store is under way. Its entries go next, then the directory,
then the lock files. `finish`: listing the stores, and a restore or join of
the store, finish a deletion they find recorded, when no call holds it. An
open never reaches one: the store's directory is gone from its place.

Without the first rule, a crash after the entries go leaves the store listed
with no device id or keys: it can't open, though the person deleted it.
Without the second, a crash after the rename leaves remains nothing ever
removes, and creating the store's id again is refused for good: a restore of
it on this device fails. Each is a checked counterexample.
-/
namespace CovenSequence.StoreDiscard

/-- The store's directory. -/
inductive Dir where
  /-- Under the store's id: listed, and opened by its entries. -/
  | published
  /-- Under its deletion name: hidden, a deletion recorded. -/
  | deleting
  | none
  deriving DecidableEq, Repr

structure World where
  dir : Dir
  /-- This device's keychain entries for the store. -/
  entries : Bool
  /-- The store's lock files beside the directory. -/
  locks : Bool
  /-- A deletion call holds the store's locks. -/
  call : Bool
  deriving DecidableEq, Repr

/-- A deletion call has just locked a whole store. -/
def start : World := ⟨.published, true, true, true⟩

structure Rules where
  /-- The deletion renames the directory before anything else. -/
  unpublishFirst : Bool
  /-- Listing and restoring finish a recorded deletion. -/
  finish : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true, true⟩

inductive Act where
  /-- The call renames the directory to its deletion name. -/
  | unpublish
  /-- The call deletes this device's entries for the store. -/
  | deleteEntries
  /-- The call removes the directory. -/
  | removeDir
  /-- The call removes the lock files, and ends. -/
  | removeLocks
  /-- The call fails, and ends. -/
  | fail
  /-- The device crashes, or the app drops the call. -/
  | crash
  /-- The app discards the listed store again. -/
  | retry
  /-- A listing or a restore finds a recorded deletion no call holds, and
  takes its locks to finish it. -/
  | pickUp
  deriving DecidableEq, Repr

/-- A deletion is recorded: the directory under its deletion name, or lock
files with no directory. -/
def recorded (w : World) : Bool :=
  w.dir == .deleting || (w.dir == .none && w.locks)

def enabled (R : Rules) (w : World) : Act → Bool
  | .unpublish =>
    w.call && w.dir == .published && (R.unpublishFirst || !w.entries)
  | .deleteEntries =>
    w.call && w.entries && (!R.unpublishFirst || w.dir != .published)
  | .removeDir => w.call && w.dir == .deleting && !w.entries
  | .removeLocks => w.call && w.dir == .none && !w.entries && w.locks
  | .fail | .crash => w.call
  | .retry => !w.call && w.dir == .published
  | .pickUp => R.finish && !w.call && recorded w

def act (w : World) : Act → World
  | .unpublish => { w with dir := .deleting }
  | .deleteEntries => { w with entries := false }
  | .removeDir => { w with dir := .none }
  | .removeLocks => { w with locks := false, call := false }
  | .fail | .crash => { w with call := false }
  | .retry | .pickUp => { w with call := true }

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' : World} (a : Act) : enabled R w a = true → Steps R (act w a) w' → Steps R w w'

def Reachable (R : Rules) (w : World) : Prop := Steps R start w

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act w a) rest else none

/-- The listing shows the store, and its entries are gone: it can't open. -/
def Broken (w : World) : Prop := w.dir = .published ∧ w.entries = false

/-- Nothing left: the store whole, with no call on it, or gone with its
entries and locks. Creating its id again is refused only while a directory
or lock file is named for it. -/
def quiet (w : World) : Bool :=
  !w.call && ((w.dir == .published && w.entries) || (w.dir == .none && !w.entries && !w.locks))

/-! ## Counterexamples -/

/-- Ana discards Household on her laptop, which deletes its entries, then
crashes. Without the rename first, the listing shows Household, and it
can't open: its device id and keys are gone. -/
theorem entries_first_breaks_store :
    ∃ w, trace ⟨false, true⟩ start [.deleteEntries, .crash] = some w ∧ Broken w := by
  exact ⟨_, rfl, by simp [Broken, act, start]⟩

/-- What only coven's own steps and the app's retry of a listed store do. -/
inductive OwnSteps (R : Rules) : World → World → Prop
  | refl (w : World) : OwnSteps R w w
  | step {w w' : World} (a : Act) (own : a ≠ .fail ∧ a ≠ .crash) (e : enabled R w a = true)
      (rest : OwnSteps R (act w a) w') : OwnSteps R w w'

/-- The laptop crashes right after renaming Household's directory. Nothing
finishes a recorded deletion, so on every path the remains stay, and a
restore of Household on the laptop is refused for good. -/
theorem unfinished_deletion_blocks_store :
    ∃ w, trace ⟨true, false⟩ start [.unpublish, .crash] = some w ∧
      ∀ w', OwnSteps ⟨true, false⟩ w w' → quiet w' = false := by
  refine ⟨_, rfl, fun w' h => ?_⟩
  have stays : ∀ {a b}, OwnSteps ⟨true, false⟩ a b → a = ⟨.deleting, true, true, false⟩ → b = a := by
    intro a b h
    induction h with
    | refl => exact fun _ => rfl
    | step x _ e _ _ =>
      intro ha
      subst ha
      cases x <;> simp [enabled, recorded] at e
  rw [stays h rfl]
  rfl

/-! ## Under the rules -/

structure Inv (w : World) : Prop where
  /-- A published directory keeps its entries. -/
  published : w.dir = .published → w.entries = true
  /-- Locks go last. -/
  locks : w.locks = false → w.dir = .none ∧ w.entries = false
  /-- A call holds the store's locks. -/
  call : w.call = true → w.locks = true

theorem inv_start : Inv start := by
  constructor <;> simp [start]

theorem inv_step (w : World) (a : Act) (inv : Inv w) (e : enabled Rules.correct w a = true) :
    Inv (act w a) := by
  obtain ⟨published, locks, call⟩ := inv
  rcases hd : w.dir <;> rcases he : w.entries <;> rcases hl : w.locks <;> cases a <;>
    constructor <;> simp_all [act, enabled, Rules.correct, recorded]

theorem steps_keep_inv : ∀ {w w'}, Steps Rules.correct w w' → Inv w → Inv w' := by
  intro w w' h
  induction h with
  | refl => exact id
  | step a e _ ih => exact fun inv => ih (inv_step _ a inv e)

theorem reachable_inv {w : World} (h : Reachable Rules.correct w) : Inv w :=
  steps_keep_inv h inv_start

/-- The listing never shows a store whose entries are gone. -/
theorem never_broken {w : World} (h : Reachable Rules.correct w) : ¬Broken w := by
  rintro ⟨d, e⟩
  simp [(reachable_inv h).published d] at e

/-! ### Every deletion settles -/

/-- Coven's own steps: the call's, and finishing what is recorded. -/
def coven : Act → Bool
  | .unpublish | .deleteEntries | .removeDir | .removeLocks | .pickUp => true
  | _ => false

inductive Moves : World → World → Prop
  | refl (w : World) : Moves w w
  | step {w w' : World} (a : Act) (own : coven a = true) (e : enabled Rules.correct w a = true)
      (rest : Moves (act w a) w') : Moves w w'

theorem Moves.steps {w w' : World} (h : Moves w w') : Steps Rules.correct w w' := by
  induction h with
  | refl => exact .refl _
  | step a _ e _ ih => exact .step a e ih

theorem Moves.trans {a b d : World} (h : Moves a b) (k : Moves b d) : Moves a d := by
  induction h with
  | refl => exact k
  | step x own e _ ih => exact .step x own e (ih k)

theorem Steps.trans {R : Rules} {a b d : World} :
    Steps R a b → Steps R b d → Steps R a d := by
  intro h k
  induction h with
  | refl => exact k
  | step x e _ ih => exact .step x e (ih k)

def measure (w : World) : Nat :=
  if quiet w then 0
  else (if w.call then 0 else 10) +
    (match w.dir with | .published => 3 | .deleting => 2 | .none => 1) +
    (if w.entries then 1 else 0) + (if w.locks then 1 else 0)

theorem progress (w : World) (inv : Inv w) (busy : quiet w = false) :
    ∃ w', Moves w w' ∧ measure w' < measure w := by
  have go : ∀ a, coven a = true → enabled Rules.correct w a = true →
      measure (act w a) < measure w → ∃ w', Moves w w' ∧ measure w' < measure w :=
    fun a own e less => ⟨_, .step a own e (.refl _), less⟩
  rcases hc : w.call <;> rcases hd : w.dir <;> rcases he : w.entries <;> rcases hl : w.locks
  all_goals
    first
    | (simp [quiet, hc, hd, he, hl] at busy; done)
    | (have := inv.published hd; simp [he] at this; done)
    | (have := inv.locks hl; simp [hd, he] at this; done)
    | (have := inv.call hc; simp [hl] at this; done)
    | (refine go .pickUp rfl (by simp [enabled, Rules.correct, recorded, hc, hd, hl]) ?_
       simp [measure, quiet, act, hc, hd, he, hl]; done)
    | (refine go .unpublish rfl (by simp [enabled, Rules.correct, hc, hd]) ?_
       simp [measure, quiet, act, hc, hd, he, hl]; done)
    | (refine go .deleteEntries rfl (by simp [enabled, Rules.correct, hc, hd, he]) ?_
       simp [measure, quiet, act, hc, hd, he, hl]; done)
    | (refine go .removeDir rfl (by simp [enabled, hc, hd, he]) ?_
       simp [measure, quiet, act, hc, hd, he, hl]; done)
    | (refine go .removeLocks rfl (by simp [enabled, hc, hd, he, hl]) ?_
       simp [measure, quiet, act, hc, hd, he, hl]; done)

/-- Every deletion settles, on every path: from any reachable state, coven's
own steps, the call's and the listing's, reach a state with nothing left: the
store whole, the deletion having failed before its first step, or gone with
its entries and lock files. -/
theorem settles {w : World} (h : Reachable Rules.correct w) :
    ∃ w', Moves w w' ∧ quiet w' = true := by
  generalize hm : measure w = m
  induction m using Nat.strongRecOn generalizing w with
  | ind m ih =>
    by_cases q : quiet w = true
    · exact ⟨w, .refl w, q⟩
    · obtain ⟨w', moves, less⟩ := progress w (reachable_inv h) (by simpa using q)
      obtain ⟨w'', more, quiet⟩ := ih (measure w') (hm ▸ less) (Steps.trans h moves.steps) rfl
      exact ⟨w'', moves.trans more, quiet⟩

end CovenSequence.StoreDiscard
