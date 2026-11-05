/-!
# A restore's registration, through crashes, dropped calls and cancels

A restore loads the store on a new device in an unpublished installation,
then registers the device (§13.1 step 4): an operation of its own that claims
an `AddDevice` entry through the device's one claim journal (§5.1). Loading
only reads storage; that claim is the restore's first write to it, and once
it lands the store's history names the device as active.

The registration is the restore's point of no return (§11.6). Before it, a
cancel removes the installation and nothing in storage changed. From it on,
the restore only goes forward: a crash, a dropped call or a cancel leaves it
listed, and the app resumes it to the end, as it resumes a join that may
have sent its request (§13.2). The one way back is the facts': a
registration refused, or a device removed since, leaves nothing in storage
naming the device as active, so the restore ends without its store.

The model follows one restore's call and its registration. The call records
the restore (`record`), journals the registration (`register`), and once
that has ended publishes the store (`conclude`). The registration's own
task claims a slot and settles the claim (`pass`); storage stores an
attempt (`land`) or another device takes its slot (`lose`). The app may
cancel the call, drop it or crash at any point; the call may fail; another
device may remove the member, after which a registration is refused and a
registered device is no longer active. Every step is one transaction, so a
crash between any two is a path of steps.

Before the record, nothing in storage names the device, and the restore's
whole work is the installation it loaded into: a cancel or a failure removes
it (§11.6). A crash before the record leaves the installation with nothing
recorded beside it: no call lists it or runs on it, and nothing in storage
refers to it. That is space, not state; coven reclaims it where it lists
unfinished work (`reclaim`), never while a call holds it.

`Rules` names the three rules the restore needs. `record`: before it
registers, the restore records itself on the device with what finishing
needs (its code and its sign-in), and the app's list of unfinished restores
shows each recorded one. `wait`: a cancel waits for the registration's task
to end, and a call that ends without its store, cancelled, its registration
refused or its device removed, removes the installation only when the facts
say nothing in storage names the device as active and nothing can land;
otherwise the call ends and the restore stays listed, as it does after any
other failure. Without either rule a crash or a cancel leaves a device
registered that no installation stands for, a checked counterexample
each. `undo`: a call that fails before the record removes the
installation. Without it the failed call leaves an installation nothing
lists, for a later listing to reclaim.
-/
namespace CovenSequence.RestoreRegistration

/-- The installation the restore loads into. -/
inductive Inst where
  /-- Removed, or never made. -/
  | none
  /-- Unpublished: hidden from the store listing. -/
  | hidden
  | published
  deriving DecidableEq, Repr

/-- The registration operation in the installation's journal. -/
inductive Op where
  | none
  | running
  /-- Its claim landed and the device applied its slot. -/
  | applied
  /-- Checked again after losing its slot, it was refused: no entry. -/
  | rejected
  deriving DecidableEq, Repr

/-- What storage did with the open claim's last attempt. -/
inductive Attempt where
  /-- Sent, its reply not yet read. -/
  | sent
  /-- Its bytes are in the slot. -/
  | landed
  /-- Another device's entry took the slot first. -/
  | lost
  deriving DecidableEq, Repr

structure World where
  inst : Inst
  /-- The restore's record: its code and sign-in kept on the device. -/
  recorded : Bool
  /-- A restore call is running on the installation. -/
  call : Bool
  /-- The app asked the running call to cancel. -/
  cancelled : Bool
  op : Op
  /-- The registration's task is running: it outlives the call that
  started it, and ends at a crash. -/
  task : Bool
  /-- The device's one open claim, and its last attempt. -/
  claim : Option Attempt
  /-- The store's history holds the device's registration. -/
  landed : Bool
  /-- Another device removed the member. -/
  removed : Bool
  deriving DecidableEq, Repr

/-- A restore call has just made its installation and loaded the store. -/
def start : World := ⟨.hidden, false, true, false, .none, false, none, false, false⟩

structure Rules where
  /-- The restore records itself before it registers, and is listed. -/
  record : Bool
  /-- A cancel waits for the registration's task, and a call ending without
  its store removes the installation only when the facts allow it. -/
  wait : Bool
  /-- A call that fails before the record removes the installation. -/
  undo : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true, true, true⟩

/-- The app lists the restore, to resume it. -/
def listed (w : World) : Bool := w.inst == .hidden && w.recorded

/-- A hidden installation nothing refers to: no call runs on it and no
record lists it. Only a crash before the record leaves one. -/
def unreferenced (w : World) : Bool := w.inst == .hidden && !w.recorded && !w.call

/-- The device is active in the store's history. -/
def active (w : World) : Bool := w.landed && !w.removed

/-- Nothing in storage names the device as active, and nothing can land:
the registration never started or was refused, or it landed and the device
was removed since. -/
def undoable (w : World) : Bool :=
  !w.recorded || w.op == .none || w.op == .rejected || (w.landed && w.removed)

/-- Remove the installation, its record and its journal with it. An attempt
already sent is on its way whatever the device deletes. -/
def undo (w : World) : World :=
  { w with inst := .none, recorded := false, call := false, cancelled := false, op := .none,
           task := false, claim := if w.claim == some .sent then w.claim else none }

/-- The call's steps run while it isn't cancelled. -/
def running (w : World) : Bool := w.call && !w.cancelled

inductive Act where
  /-- The call records the restore. -/
  | record
  /-- The call journals the registration, or runs the journaled one again,
  in a task of its own. -/
  | register
  /-- The registration's task advances it. -/
  | pass
  /-- The provider stores the open claim's attempt. -/
  | land
  /-- Another device's entry takes the attempt's slot first. -/
  | lose
  /-- The registration has ended, or landed for a device removed since: the
  call publishes the store, or, when it was refused or the device removed,
  ends without it. -/
  | conclude
  /-- The app cancels the running call. -/
  | cancel
  /-- The cancelled call ends. -/
  | stop
  /-- The call fails, and ends: a network error, say. Before the record it
  removes the installation; after, the restore stays as it is, and its
  registration's task, if one runs, goes on. -/
  | fail
  /-- The registration's task fails, and ends; the operation stays
  journaled. -/
  | taskFails
  /-- The app resumes a listed restore. -/
  | resume
  /-- The device crashes, or the app drops the call. A crash also ends the
  registration's task; a dropped call doesn't, which `taskFails` covers. -/
  | crash
  /-- Another device removes the member. -/
  | remove
  /-- Listing unfinished work removes an installation nothing refers to. -/
  | reclaim
  deriving DecidableEq, Repr

def enabled (R : Rules) (w : World) : Act → Bool
  | .record => R.record && running w && !w.recorded
  | .register => running w && (w.recorded || !R.record) && !w.task &&
      (w.op == .none || w.op == .running)
  | .pass => w.op == .running && w.task && w.claim != some .sent
  | .land | .lose => w.claim == some .sent
  | .conclude => running w && (w.op == .applied || w.op == .rejected || (w.landed && w.removed))
  | .cancel => running w
  | .stop => w.call && w.cancelled && (!R.wait || !w.task)
  | .fail => running w
  | .taskFails => w.task
  | .resume => !w.call && listed w
  | .crash => true
  | .remove => true
  | .reclaim => unreferenced w

/-- The registration's task: claim when no claim is open, refused once the
member is removed; a landed claim applies and ends the operation; a lost
one checks again and claims the next slot. -/
def pass (w : World) : World :=
  match w.claim with
  | none | some .lost =>
    if w.removed then { w with op := .rejected, task := false, claim := none }
    else { w with claim := some .sent }
  | some .landed => { w with op := .applied, task := false, claim := none }
  | some .sent => w

def act (R : Rules) (w : World) : Act → World
  | .record => { w with recorded := true }
  | .register => { w with op := .running, task := true }
  | .pass => pass w
  | .land => { w with claim := some .landed, landed := true }
  | .lose => { w with claim := some .lost }
  | .conclude =>
    if w.op == .applied && !w.removed then
      { w with inst := .published, recorded := false, call := false, op := .none }
    else if R.wait then undo w
    -- Without the rule, a refused registration fails the call and keeps
    -- the installation.
    else { w with call := false }
  | .cancel => { w with cancelled := true }
  | .stop =>
    if !R.wait || undoable w then undo w else { w with call := false, cancelled := false }
  | .fail => if R.undo && !w.recorded then undo w else { w with call := false }
  | .taskFails => { w with task := false }
  | .resume => { w with call := true }
  | .crash => { w with call := false, cancelled := false, task := false }
  | .remove => { w with removed := true }
  | .reclaim => { w with inst := .none }

inductive Step (R : Rules) : World → World → Prop
  | step (w : World) (a : Act) (h : enabled R w a = true) : Step R w (act R w a)

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' w'' : World} : Step R w w' → Steps R w' w'' → Steps R w w''

def Reachable (R : Rules) (w : World) : Prop := Steps R start w

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act R w a) rest else none

theorem Steps.trans {R : Rules} {a b d : World} :
    Steps R a b → Steps R b d → Steps R a d := by
  intro h k
  induction h with
  | refl => exact k
  | step s _ ih => exact .step s (ih k)

/-- A property every step keeps holds from then on. -/
theorem stays {R : Rules} (P : World → Prop)
    (keep : ∀ w a, P w → enabled R w a = true → P (act R w a)) :
    ∀ {w w'}, Steps R w w' → P w → P w' := by
  intro w w' h
  induction h with
  | refl => exact id
  | step s _ ih =>
    cases s with
    | step a e => exact fun p => ih (keep _ a p e)

/-! ## Counterexamples: a device registered that no installation stands for -/

/-- The store's history holds the device's registration, and no
installation on the device is published or listed for the app to finish:
only another device removing it ends it. -/
def Ghost (w : World) : Prop :=
  w.landed = true ∧ w.inst ≠ .published ∧ w.recorded = false ∧ w.call = false ∧
    w.task = false ∧ w.claim ≠ some .sent

theorem ghost_stays (R : Rules) (w : World) (a : Act) (h : Ghost w)
    (e : enabled R w a = true) : Ghost (act R w a) := by
  obtain ⟨landed, inst, recorded, call, task, claim⟩ := h
  cases a <;> simp_all [Ghost, act, enabled, running, listed, unreferenced]

/-- Ana restores Household on her laptop. The registration lands, and the
laptop crashes before anything else. Without the record nothing lists the
installation, so nothing resumes it: on every path after, whether or not a
cancel waits, Household's history names the laptop as Ana's device and no
installation is published or listed for it. Only another device removing
it makes it inactive. -/
theorem crashed_registration_ghost (wait : Bool) :
    ∃ w, trace ⟨false, wait, true⟩ start [.register, .pass, .land, .crash] = some w ∧
      ∀ w', Steps ⟨false, wait, true⟩ w w' → Ghost w' := by
  refine ⟨_, rfl, fun w' h => stays _ (ghost_stays _) h ?_⟩
  simp [Ghost, act, pass, start]

/-- Ana cancels the restore while its registration's attempt is on its way.
A cancel that doesn't wait removes the installation, and the attempt lands
after: on every path after, whether or not the restore was recorded,
Household's history names the laptop as Ana's device and the laptop has
nothing of Household. -/
theorem cancelled_registration_ghost (record : Bool) :
    ∃ w, trace ⟨record, false, true⟩ start
        ((if record then [.record] else []) ++ [.register, .pass, .cancel, .stop, .land]) =
          some w ∧
      ∀ w', Steps ⟨record, false, true⟩ w w' → Ghost w' := by
  cases record
  all_goals
    refine ⟨_, rfl, fun w' h => stays _ (ghost_stays _) h ?_⟩
    simp [Ghost, act, pass, undo, start]

/-! ## The properties, under the rules -/

/-- What every world reachable under `correct` satisfies. -/
structure Inv (w : World) : Prop where
  call : w.call = true → w.inst = .hidden
  cancelled : w.cancelled = true → w.call = true
  /-- The registration runs only in a recorded restore. -/
  op : w.op ≠ .none → w.recorded = true ∧ w.inst = .hidden
  recorded : w.recorded = true → w.inst = .hidden
  task : w.task = true → w.op = .running
  claim : w.claim.isSome → w.op = .running
  /-- The registration landed only where its claim landed, its operation
  applied, its store published, or its installation was removed. -/
  landed : w.landed = true →
    w.claim = some .landed ∨ w.op = .applied ∨ w.inst = .published ∨ w.inst = .none
  /-- A removed installation's device is no longer active. -/
  gone : w.inst = .none → w.landed = true → w.removed = true
  claimLanded : w.claim = some .landed → w.landed = true
  applied : w.op = .applied → w.landed = true
  /-- A refused registration has no entry. -/
  rejected : w.op = .rejected → w.landed = false
  published : w.inst = .published → w.landed = true

theorem inv_start : Inv start := by
  constructor <;> simp [start]

set_option maxHeartbeats 1000000 in
theorem inv_step (w : World) (a : Act) (inv : Inv w) (e : enabled Rules.correct w a = true) :
    Inv (act Rules.correct w a) := by
  obtain ⟨call, cancelled, op, recorded, task, claim, landed, gone, claimLanded, applied, rejected,
    published⟩ := inv
  cases a
  case record | cancel | taskFails | resume | crash | remove | land | lose =>
    rcases hop : w.op <;> rcases hc : w.claim with _ | (_ | _ | _) <;> constructor <;>
      simp_all [act, enabled, Rules.correct, running, listed]
  case fail =>
    rcases hop : w.op <;> rcases hrec : w.recorded <;> rcases hl : w.landed <;>
      rcases hc : w.claim with _ | (_ | _ | _) <;> constructor <;>
      simp_all [act, enabled, Rules.correct, running, undo]
  case reclaim =>
    rcases hop : w.op <;> rcases hl : w.landed <;> rcases hc : w.claim with _ | (_ | _ | _) <;>
      constructor <;> simp_all [act, enabled, unreferenced]
  case register =>
    rcases hop : w.op <;> rcases hc : w.claim with _ | (_ | _ | _) <;> constructor <;>
      simp_all [act, enabled, Rules.correct, running]
  case pass =>
    rcases hc : w.claim with _ | (_ | _ | _) <;> rcases hrem : w.removed <;> constructor <;>
      simp_all [act, enabled, pass]
  case conclude =>
    rcases hop : w.op <;> rcases hrem : w.removed <;> rcases hl : w.landed <;>
      rcases hc : w.claim with _ | (_ | _ | _) <;> constructor <;>
      simp_all [act, enabled, Rules.correct, running, undo]
  case stop =>
    rcases hop : w.op <;> rcases hrec : w.recorded <;> rcases hrem : w.removed <;>
      rcases hl : w.landed <;> rcases hc : w.claim with _ | (_ | _ | _) <;>
      constructor <;> simp_all [act, enabled, Rules.correct, undo, undoable]

theorem reachable_inv {w : World} (h : Reachable Rules.correct w) : Inv w :=
  stays Inv (fun w a inv e => inv_step w a inv e) h inv_start

/-- A call that ends, but not by a crash, leaves no hidden installation
unlisted: it published the store, removed the installation, or left the
restore listed. Only a crash leaves one for a listing to reclaim. -/
theorem ended_call_leaves_nothing_unlisted {w : World} (h : Reachable Rules.correct w) (a : Act)
    (e : enabled Rules.correct w a = true) (crash : a ≠ .crash) (call : w.call = true)
    (ended : (act Rules.correct w a).call = false) :
    unreferenced (act Rules.correct w a) = false := by
  have inv := reachable_inv h
  cases a
  case fail =>
    rcases hrec : w.recorded <;> simp_all [act, Rules.correct, undo, unreferenced]
  case stop =>
    by_cases hu : undoable w = true
    · simp [act, Rules.correct, hu, undo, unreferenced]
    · have kept : w.recorded = true := by
        rcases hr : w.recorded <;> simp_all [undoable]
      simp [act, Rules.correct, hu, unreferenced, kept]
  case conclude =>
    simp only [act, Rules.correct]
    split <;> simp_all [undo, unreferenced]
  case crash => contradiction
  case pass =>
    rcases hc : w.claim with _ | (_ | _ | _) <;> rcases hrem : w.removed <;> simp_all [act, pass]
  all_goals simp_all [act]

/-- A failed restore's call that keeps its installation leaves it unlisted:
nothing is recorded beside it and no call runs on it, so only a later
listing would reclaim it. -/
theorem failure_keeps_installation :
    ∃ w, trace ⟨true, true, false⟩ start [.fail] = some w ∧ unreferenced w = true := by
  exact ⟨_, rfl, by simp [unreferenced, act, start]⟩

/-- No ghost: while the store's history names the device as active, or a
claim of its registration is open, the restore is published or listed for
the app to finish. -/
theorem registered_listed {w : World} (h : Reachable Rules.correct w)
    (registered : active w = true ∨ w.claim.isSome) :
    w.inst = .published ∨ listed w = true := by
  have inv := reachable_inv h
  by_cases pub : w.inst = .published
  · exact .inl pub
  right
  -- The registration's operation is journaled, or it applied.
  have op : w.op ≠ .none := by
    rcases registered with r | c
    · have r : w.landed = true ∧ w.removed = false := by simpa [active] using r
      rcases inv.landed r.1 with l | ap | p | n
      · simp [inv.claim (by simp [l])]
      · simp [ap]
      · exact absurd p pub
      · simp [inv.gone n r.1] at r
    · simp [inv.claim c]
  have := inv.op op
  simp [listed, this.1, this.2]

/-! ### Every restore ends published, or with nothing registered -/

/-- What coven does on its own, and the app resuming the restores coven
lists: the call's steps, the registration's task, its requests reaching
storage, and a cancelled call ending. -/
def coven : Act → Bool
  | .record | .register | .pass | .land | .conclude | .stop | .resume | .reclaim => true
  | _ => false

inductive Moves : World → World → Prop
  | refl (w : World) : Moves w w
  | step {w w' : World} (a : Act) (own : coven a = true) (e : enabled Rules.correct w a = true)
      (rest : Moves (act Rules.correct w a) w') : Moves w w'

theorem Moves.steps {w w' : World} (h : Moves w w') : Steps Rules.correct w w' := by
  induction h with
  | refl => exact .refl _
  | step a _ e _ ih => exact .step (.step _ a e) ih

theorem Moves.trans {a b d : World} (h : Moves a b) (k : Moves b d) : Moves a d := by
  induction h with
  | refl => exact k
  | step x own e _ ih => exact .step x own e (ih k)

theorem one {w : World} (a : Act) (own : coven a = true)
    (e : enabled Rules.correct w a = true) : Moves w (act Rules.correct w a) :=
  .step a own e (.refl _)

/-- Nothing left: the store is published, or no call runs, nothing is
recorded, so nothing is listed, and no installation is left. -/
def quiet (w : World) : Bool :=
  w.inst == .published || (!w.call && !w.recorded && w.inst == .none)

/-- What is left of the registration. -/
def opWork (w : World) : Nat :=
  match w.op with
  | .none => 20
  | .running =>
    (if w.task then 0 else 10) +
      match w.claim with
      | none => 8
      | some .lost => 7
      | some .sent => 6
      | some .landed => 3
  | .applied | .rejected => 1

/-- What is left to do. A refused cancel ends the call, which the app then
resumes, so a cancelled call outweighs one not running, and that outweighs
a running one. -/
def measure (w : World) : Nat :=
  if quiet w then 0
  else if !w.recorded then 4000 + opWork w
  else if w.cancelled then 3000 + opWork w
  else if !w.call then 2000 + opWork w
  else 1000 + opWork w

theorem opWork_le (w : World) : opWork w ≤ 30 := by
  simp only [opWork]
  split <;> (try split) <;> (try split) <;> omega

/-- A step that leaves the call, the record and the installation alone and
lowers what is left of the registration lowers the measure. -/
theorem measure_lt {w w' : World} (busy : quiet w = false) (inst : w'.inst = w.inst)
    (recorded : w'.recorded = w.recorded) (call : w'.call = w.call)
    (cancelled : w'.cancelled = w.cancelled) (less : opWork w' < opWork w) :
    measure w' < measure w := by
  have busy' : quiet w' = false := by simpa [quiet, inst, recorded, call] using busy
  unfold measure
  rw [busy, busy', recorded, call, cancelled]
  cases w.recorded <;> cases w.cancelled <;> cases w.call <;> simp <;> omega

/-- The registration's task, while it runs, takes a step of coven's own that
lowers what is left of it. -/
theorem task_step (w : World) (inv : Inv w) (ht : w.task = true) :
    ∃ a, coven a = true ∧ enabled Rules.correct w a = true ∧
      (act Rules.correct w a).inst = w.inst ∧ (act Rules.correct w a).recorded = w.recorded ∧
      (act Rules.correct w a).call = w.call ∧
      (act Rules.correct w a).cancelled = w.cancelled ∧
      opWork (act Rules.correct w a) < opWork w := by
  have hop := inv.task ht
  rcases hc : w.claim with _ | (_ | _ | _) <;> rcases hrem : w.removed
  all_goals first
    | (refine ⟨.land, rfl, ?_⟩; simp [enabled, act, opWork, hc, hop, ht]; done)
    | (refine ⟨.pass, rfl, ?_⟩; simp [enabled, act, pass, opWork, hc, hop, ht, hrem]; done)

/-- From every reachable state that isn't quiet, coven and the app's resume
make progress. -/
theorem progress (w : World) (inv : Inv w) (busy : quiet w = false) :
    ∃ w', Moves w w' ∧ measure w' < measure w := by
  have go : ∀ a, coven a = true → enabled Rules.correct w a = true →
      measure (act Rules.correct w a) < measure w →
      ∃ w', Moves w w' ∧ measure w' < measure w :=
    fun a own e less => ⟨_, one a own e, less⟩
  have pub : w.inst ≠ .published := by intro h; simp [quiet, h] at busy
  by_cases hrec : w.recorded = true
  · have hidden := inv.recorded hrec
    by_cases hcall : w.call = true
    · by_cases ht : w.task = true
      · obtain ⟨a, own, e, i, r, c, k, less⟩ := task_step w inv ht
        exact go a own e (measure_lt busy i r c k less)
      · have ht : w.task = false := by simpa using ht
        rcases hcan : w.cancelled
        · rcases hop : w.op
          · exact go .register rfl (by simp [enabled, Rules.correct, running, hcall, hcan, hrec, ht, hop])
              (measure_lt busy rfl rfl rfl rfl
                (by rcases hc : w.claim with _ | (_ | _ | _) <;> simp [act, opWork, hop, hc]))
          · exact go .register rfl (by simp [enabled, Rules.correct, running, hcall, hcan, hrec, ht, hop])
              (measure_lt busy rfl rfl rfl rfl (by simp [act, opWork, hop, ht]))
          all_goals
            refine go .conclude rfl (by simp [enabled, running, hcall, hcan, hop]) ?_
            rcases hrem : w.removed <;>
              simp [act, Rules.correct, measure, quiet, undo, hop, hrem, hrec, hcall, hcan, pub] <;>
              omega
        · refine go .stop rfl (by simp [enabled, Rules.correct, hcall, hcan, ht]) ?_
          by_cases hu : undoable w = true
          · simp [act, Rules.correct, hu, measure, quiet, undo, hrec, hcall, hcan, pub]
            omega
          · simp [act, Rules.correct, hu, measure, quiet, opWork, hrec, hcall, hcan, pub]
    · have hcall : w.call = false := by simpa using hcall
      have hcan : w.cancelled = false := by
        rcases h : w.cancelled
        · rfl
        · simp [inv.cancelled h] at hcall
      refine go .resume rfl (by simp [enabled, listed, hcall, hidden, hrec]) ?_
      simp [act, measure, quiet, opWork, hcall, hcan, hrec, pub]
  · have hrec : w.recorded = false := by simpa using hrec
    -- A crash before the record left the installation: a listing reclaims it.
    by_cases hun : unreferenced w = true
    · refine go .reclaim rfl hun ?_
      have hc : w.call = false := by simp [unreferenced] at hun; exact hun.2
      have before : 0 < measure w := by simp [measure, busy, hrec]; omega
      have after : measure (act Rules.correct w .reclaim) = 0 := by
        simp [act, measure, quiet, hc, hrec]
      omega
    have hcall : w.call = true := by
      rcases h : w.call
      · rcases hi : w.inst <;> simp_all [quiet, unreferenced]
      · rfl
    have hop : w.op = .none := by
      rcases h : w.op
      · rfl
      all_goals (have := (inv.op (by simp [h])).1; simp [hrec] at this)
    have ht : w.task = false := by
      rcases h : w.task
      · rfl
      · simp [inv.task h] at hop
    rcases hcan : w.cancelled
    · exact go .record rfl (by simp [enabled, Rules.correct, running, hcall, hcan, hrec])
        (by simp [act, measure, quiet, hcall, hcan, hrec, pub, opWork, hop])
    · refine go .stop rfl (by simp [enabled, Rules.correct, hcall, hcan, ht]) ?_
      simp [act, Rules.correct, undoable, undo, measure, quiet, hrec, hcall, pub]
      omega

/-- Every restore settles, on every path: from any reachable state, coven's
own steps and the app resuming what coven lists reach a state where nothing
is left to do. Crashes, dropped calls, failures, cancels, lost slots and
the member's removal are all among the paths that reach those states. -/
theorem settles {w : World} (h : Reachable Rules.correct w) :
    ∃ w', Moves w w' ∧ quiet w' = true := by
  generalize hm : measure w = m
  induction m using Nat.strongRecOn generalizing w with
  | ind m ih =>
    by_cases q : quiet w = true
    · exact ⟨w, .refl w, q⟩
    · obtain ⟨w', moves, less⟩ := progress w (reachable_inv h) (by simpa using q)
      obtain ⟨w'', more, quiet⟩ := ih (measure w') (hm ▸ less) (h.trans moves.steps) rfl
      exact ⟨w'', moves.trans more, quiet⟩

/-- Where the restore settles, the device is registered and its store
published, or the device is not active in the store's history and no claim
of it is open. -/
theorem settled_outcome {w : World} (h : Reachable Rules.correct w) (q : quiet w = true) :
    (w.inst = .published ∧ w.landed = true) ∨ (active w = false ∧ w.claim = none) := by
  have inv := reachable_inv h
  by_cases pub : w.inst = .published
  · exact .inl ⟨pub, inv.published pub⟩
  right
  have nrec : w.recorded = false := by
    rcases hr : w.recorded <;> simp_all [quiet]
  have nop : w.op = .none := by
    rcases ho : w.op
    · rfl
    all_goals (have := (inv.op (by simp [ho])).1; simp [nrec] at this)
  have nclaim : w.claim = none := by
    rcases hc : w.claim with _ | a
    · rfl
    · simp [inv.claim (by simp [hc])] at nop
  refine ⟨?_, nclaim⟩
  rcases hl : w.landed
  · simp [active, hl]
  · rcases inv.landed hl with l | ap | p | n
    · simp [nclaim] at l
    · simp [nop] at ap
    · exact absurd p pub
    · simp [active, inv.gone n hl]

end CovenSequence.RestoreRegistration
