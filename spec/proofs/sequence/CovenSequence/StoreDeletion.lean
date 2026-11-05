/-!
# Deleting a store from a device that leaves the store, through crashes

`delete_store` deletes a store from this device and leaves it (api.md E1):
it journals this device's removal, an operation whose claim, once it lands,
makes history name the device as removed (§14.1), and then deletes the
store's data and keys on the device. The journaled removal lives in the
store's own database, so the data can go only once the removal has applied:
deleted sooner, a removal not yet sent never lands, and the device stays
registered with nothing on it.

The removal's journal is the deletion's point of no return (§11.6). Before
it nothing in storage changed; from it on the device may be removed at any
moment, and only a finished deletion stands for that. Without a record of
the deletion, a crash after the removal lands leaves the store on the device
as it was, which opens as removed with its data and keys; and a call that
fails after journaling leaves the store open for use while its removal is
sent behind it.

`Rules` names the two rules the deletion needs. `record`: before it
journals the removal, the deletion records itself in the store's directory.
The record refuses opens, the listing shows the store as being deleted, and
a resume finishes it; a failure before the journal removes the record. From
the journal on it only goes forward: it waits for the removal to apply, then
deletes the store. `signIn`: a store whose location is recorded without
credentials can't send the removal, so the deletion is refused, naming the
sign-in, before anything changes. Without it the deletion deletes the
store and sends no removal, and the device stays registered for good.

The model follows one deletion. The call records it (`record`), journals the
removal (`journal`), the removal lands and applies (`land`), and the call
deletes the store and its record (`delete`). The call may fail or the device
crash at any point; the app opens the store when no call holds it and
nothing refuses it, and a store open with its removal journaled sends it;
the app resumes a recorded deletion. The person may sign out. Every step is
one transaction.
-/
namespace CovenSequence.StoreDeletion

/-- This device's removal. -/
inductive Removal where
  | none
  /-- Journaled in the store's database: it may land whenever the store
  connects. -/
  | journaled
  /-- Landed and applied: history names the device as removed. -/
  | applied
  deriving DecidableEq, Repr

structure World where
  /-- The store's data and keys are on the device. -/
  here : Bool
  /-- The deletion's record in the store's directory. -/
  record : Bool
  removal : Removal
  /-- A deletion call runs. -/
  call : Bool
  /-- The location is recorded, without credentials. -/
  signedOut : Bool
  deriving DecidableEq, Repr

/-- A store set up on this device, with nothing deleting it. -/
def start : World := ⟨true, false, .none, false, false⟩

structure Rules where
  /-- The deletion records itself before it journals the removal. -/
  record : Bool
  /-- A store signed out refuses the deletion. -/
  signIn : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true, true⟩

inductive Act where
  /-- The app deletes the store. -/
  | begin
  /-- The call records the deletion. -/
  | record
  /-- The call journals the removal. -/
  | journal
  /-- A call, or the open store's passes, sends the removal and it applies. -/
  | land
  /-- The call deletes the store, its record with it, and ends. -/
  | delete
  /-- Signed out without the rule, the call deletes the store at once. -/
  | deleteSignedOut
  /-- Signed out before the journal, the call is refused and ends, changing
  nothing: a record a resumed deletion finds goes, as after a failure. -/
  | refuse
  /-- The call fails, and ends. Before the journal it removes the record. -/
  | fail
  /-- The device crashes, or the app drops its future: the call's own task
  goes on, so only a crash stops it. -/
  | crash
  /-- The app resumes a recorded deletion. -/
  | resume
  /-- The app opens the store, and its passes send a journaled removal. -/
  | openSends
  /-- The person signs out, or in again. -/
  | toggleSignIn
  deriving DecidableEq, Repr

/-- The app can open the store: it is there, no call holds it, and no record
refuses it. -/
def opens (R : Rules) (w : World) : Bool := w.here && !w.call && !(R.record && w.record)

def enabled (R : Rules) (w : World) : Act → Bool
  | .begin => w.here && !w.call && !w.record
  | .record => R.record && w.call && !w.record && w.removal == .none && !w.signedOut
  | .journal => w.call && w.removal == .none && (w.record || !R.record) && !w.signedOut
  | .land => w.removal == .journaled && w.call && !w.signedOut
  | .delete => w.call && w.removal == .applied
  | .deleteSignedOut => !R.signIn && w.call && w.signedOut && w.removal == .none
  | .refuse => R.signIn && w.call && w.signedOut && w.removal == .none
  | .fail | .crash => w.call
  | .resume => w.here && !w.call && w.record
  | .openSends => opens R w && w.removal == .journaled && !w.signedOut
  | .toggleSignIn => !w.call

def act (R : Rules) (w : World) : Act → World
  | .begin => { w with call := true }
  | .record => { w with record := true }
  | .journal => { w with removal := .journaled }
  | .land | .openSends => { w with removal := .applied }
  | .delete | .deleteSignedOut => { w with here := false, record := false, call := false }
  | .refuse => { w with record := false, call := false }
  | .fail =>
    if R.record && w.removal == .none then { w with record := false, call := false }
    else { w with call := false }
  | .crash => { w with call := false }
  | .resume => { w with call := true }
  | .toggleSignIn => { w with signedOut := !w.signedOut }

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' : World} (a : Act) : enabled R w a = true → Steps R (act R w a) w' → Steps R w w'

def Reachable (R : Rules) (w : World) : Prop := Steps R start w

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act R w a) rest else none

/-- The device is registered for good with nothing of the store on it:
its data deleted, and no removal journaled that could still land. -/
def Stranded (w : World) : Prop := w.here = false ∧ w.removal = .none

/-- The store is on the device, open to the app, with a removal journaled
or applied: it opens as removed, or is removed while in use. -/
def Unlisted (R : Rules) (w : World) : Prop := opens R w = true ∧ w.removal ≠ .none

/-! ## Counterexamples -/

/-- Ana is signed out of Household's Dropbox on her phone and deletes it.
Without the rule the deletion skips the removal and deletes the store: the
phone stays one of Household's devices for good. -/
theorem signed_out_strands_device :
    ∃ w, trace ⟨true, false⟩ start [.toggleSignIn, .begin, .deleteSignedOut] = some w ∧
      Stranded w := by
  exact ⟨_, rfl, by simp [Stranded, act, start]⟩

/-- Ana deletes Household from her phone. The removal lands, and the phone
crashes before deleting anything. Without the record, Household is still
there and opens, as removed, with its data and keys. -/
theorem crashed_deletion_reopens_removed :
    ∃ w, trace ⟨false, true⟩ start [.begin, .journal, .land, .crash] = some w ∧
      Unlisted ⟨false, true⟩ w := by
  exact ⟨_, rfl, by simp [Unlisted, opens, act, start]⟩

/-- The removal is journaled and the call fails before it lands. Without
the record the store opens as before, and its passes send the removal while
Ana uses it. -/
theorem failed_deletion_removes_store_in_use :
    ∃ w, trace ⟨false, true⟩ start [.begin, .journal, .fail, .openSends] = some w ∧
      Unlisted ⟨false, true⟩ w := by
  exact ⟨_, rfl, by simp [Unlisted, opens, act, start]⟩

/-! ## Under the rules -/

structure Inv (w : World) : Prop where
  /-- A removal under way or applied keeps the record while the store is
  on the device. -/
  recorded : w.removal ≠ .none → w.here = true → w.record = true
  /-- The store leaves the device only with its removal applied. -/
  gone : w.here = false → w.removal = .applied ∧ w.record = false ∧ w.call = false

theorem inv_start : Inv start := by
  constructor <;> simp [start]

theorem inv_step (w : World) (a : Act) (inv : Inv w) (e : enabled Rules.correct w a = true) :
    Inv (act Rules.correct w a) := by
  obtain ⟨recorded, gone⟩ := inv
  rcases hl : w.here <;> rcases hr : w.record <;> rcases hm : w.removal <;>
    rcases hc : w.call <;> cases a <;> constructor <;>
    simp_all [act, enabled, Rules.correct, opens]

theorem steps_keep_inv : ∀ {w w'}, Steps Rules.correct w w' → Inv w → Inv w' := by
  intro w w' h
  induction h with
  | refl => exact id
  | step a e _ ih => exact fun inv => ih (inv_step _ a inv e)

theorem reachable_inv {w : World} (h : Reachable Rules.correct w) : Inv w :=
  steps_keep_inv h inv_start

/-- The device never stays registered with the store deleted. -/
theorem never_stranded {w : World} (h : Reachable Rules.correct w) : ¬Stranded w := by
  rintro ⟨l, r⟩
  simp [(reachable_inv h).gone l |>.1] at r

/-- The app never opens a store whose removal is under way or applied: the
record refuses it, and the listing shows the deletion to finish. -/
theorem never_unlisted {w : World} (h : Reachable Rules.correct w) :
    ¬Unlisted Rules.correct w := by
  rintro ⟨o, r⟩
  have l : w.here = true := by simp [opens] at o; exact o.1.1
  have := (reachable_inv h).recorded r l
  simp [opens, Rules.correct, this] at o

/-! ### Every deletion settles -/

/-- What coven does on its own, a call failing included, and the app
resuming a recorded deletion and signing in when its connection asks. -/
def coven : Act → Bool
  | .record | .journal | .land | .delete | .refuse | .fail | .resume | .toggleSignIn => true
  | _ => false

inductive Moves : World → World → Prop
  | refl (w : World) : Moves w w
  | step {w w' : World} (a : Act) (own : coven a = true) (e : enabled Rules.correct w a = true)
      (rest : Moves (act Rules.correct w a) w') : Moves w w'

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

/-- Nothing left: the store deleted, or untouched with no deletion under
way. -/
def quiet (w : World) : Bool :=
  !w.here || (!w.call && !w.record && w.removal == .none)

/-- What is left. A removal journaled while signed out waits for the
sign-in: the call fails, and the person signs in. -/
def measure (w : World) : Nat :=
  if quiet w then 0
  else (if w.signedOut && w.removal == .journaled then (if w.call then 70 else 40) else 0) +
    (if w.call then 0 else 10) + (if w.record then 0 else 4) +
    (match w.removal with | .none => 3 | .journaled => 2 | .applied => 1)


theorem progress (w : World) (inv : Inv w) (busy : quiet w = false) :
    ∃ w', Moves w w' ∧ measure w' < measure w := by
  have go : ∀ a, coven a = true → enabled Rules.correct w a = true →
      measure (act Rules.correct w a) < measure w →
      ∃ w', Moves w w' ∧ measure w' < measure w :=
    fun a own e less => ⟨_, .step a own e (.refl _), less⟩
  rcases hl : w.here <;> rcases hc : w.call <;> rcases hr : w.record <;>
    rcases hm : w.removal <;> rcases hs : w.signedOut
  all_goals
    first
    | (simp [quiet, hl, hc, hr, hm] at busy; done)
    | (have := inv.recorded; simp [hm, hl, hr] at this; done)
    | (have e : enabled Rules.correct w .toggleSignIn = true := by simp [enabled, hc]
       have less : measure (act Rules.correct w .toggleSignIn) < measure w := by simp [measure, quiet, act, hl, hc, hr, hm, hs]
       exact go .toggleSignIn rfl e less)
    | (have e : enabled Rules.correct w .resume = true := by simp [enabled, hl, hc, hr]
       have less : measure (act Rules.correct w .resume) < measure w := by simp [measure, quiet, act, hl, hc, hr, hm, hs]
       exact go .resume rfl e less)
    | (have e : enabled Rules.correct w .delete = true := by simp [enabled, hc, hm]
       have less : measure (act Rules.correct w .delete) < measure w := by simp [measure, quiet, act, hl, hc, hr, hm, hs]
       exact go .delete rfl e less)
    | (have e : enabled Rules.correct w .land = true := by simp [enabled, hc, hm, hs]
       have less : measure (act Rules.correct w .land) < measure w := by simp [measure, quiet, act, hl, hc, hr, hm, hs]
       exact go .land rfl e less)
    | (have e : enabled Rules.correct w .journal = true := by simp [enabled, Rules.correct, hc, hr, hm, hs]
       have less : measure (act Rules.correct w .journal) < measure w := by simp [measure, quiet, act, hl, hc, hr, hm, hs]
       exact go .journal rfl e less)
    | (have e : enabled Rules.correct w .record = true := by simp [enabled, Rules.correct, hc, hr, hm, hs]
       have less : measure (act Rules.correct w .record) < measure w := by simp [measure, quiet, act, hl, hc, hr, hm, hs]
       exact go .record rfl e less)
    | (have e : enabled Rules.correct w .refuse = true := by simp [enabled, Rules.correct, hc, hm, hs]
       have less : measure (act Rules.correct w .refuse) < measure w := by simp [measure, quiet, act, hl, hc, hr, hm, hs]
       exact go .refuse rfl e less)
    | (have e : enabled Rules.correct w .fail = true := by simp [enabled, hc]
       have less : measure (act Rules.correct w .fail) < measure w := by simp [measure, quiet, act, Rules.correct, hl, hc, hr, hm, hs]
       exact go .fail rfl e less)

/-- Every deletion settles, on every path: from any reachable state, coven's
own steps, the app resuming the deletion the listing shows and signing in
when it is signed out, reach a state with nothing left: the store deleted
with its removal applied, or untouched with nothing journaled. -/
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

/-- Where a deletion settles, the store is gone and its removal applied, or
the store is untouched and no removal of it is journaled. -/
theorem settled_outcome {w : World} (h : Reachable Rules.correct w) (q : quiet w = true) :
    (w.here = false ∧ w.removal = .applied) ∨ (w.here = true ∧ w.removal = .none) := by
  rcases hl : w.here
  · exact .inl ⟨rfl, ((reachable_inv h).gone hl).1⟩
  · right
    refine ⟨rfl, ?_⟩
    rcases hm : w.removal <;> simp_all [quiet]

end CovenSequence.StoreDeletion
