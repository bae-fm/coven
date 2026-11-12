/-!
# A pairing, from the showing device's journal and the request's one path

One pairing on the device that shows its code (§13.2, "Pairing: one of the
person's devices adds another"), the object at the request's one path, and the
new device that asks. Every writer that acts on them is a step:

- the app's write steps: approving the request it was shown, declining it,
  and ending the pairing (`end_pairing`, which closing Add device calls).
  Each reads and decides in its own transaction;
- the expiry, which reads and decides in one transaction too;
- a sync pass, which reads the pairing's row, does storage work with no
  transaction open, and only then writes. Between its read and its write any
  other writer may commit. Every pass looks at every pairing: a pairing has no
  doubling wait;
- a crash, which loses the pass's read and nothing durable;
- opening the store, and a reset of the showing device, which carry every
  pairing a closed handle or an old database left to its ending;
- a stop for good, which finishes the pairing's row in its own transaction and
  does no storage work, and the admin's device that sweeps the stopped
  device's requests and handovers;
- whoever saw the code writing a request of their own at the one path;
- the new device: asking, finding the handover, cancelling, recording its
  restore and registering itself.

The journal row carries its step, which every change of the row increases. A
pass's write is conditional on the step it read; so is the pending record it
saves for what it found (`recordAt`).

The one path holds a request, the handover that takes its place, or the
marker. `Rules` names each choice the properties depend on. The properties are
proved under `correct`, which has all of them; leaving any one out is a
checked counterexample below.
-/
namespace CovenSequence.PairingLifecycle

/-- The pairing's state, as its journal row encodes it (`PairingWork`). -/
inductive Stage where
  /-- The code is shown; `request` is the key of the checked request waiting
  for the app. -/
  | waiting (request : Option Nat)
  /-- The app approved the request with this key: the handover sealed to it is
  journaled. -/
  | approving (key : Nat)
  /-- The handover is in the request's place. -/
  | handedOver (key : Nat)
  /-- The marker is being put in the request's or handover's place; `forget`
  once the app has closed the screen. -/
  | ending (forget : Bool)
  /-- The marker is in place; the app reads how it ended until it closes the
  screen. -/
  | ended
  deriving DecidableEq, Repr

/-- The journal row: its state and the last step it completed. -/
structure Row where
  stage : Stage
  step : Nat
  deriving DecidableEq, Repr

/-- What the request's one path holds. -/
inductive Path where
  | empty
  /-- A request, with the key its pair carries; `ok` when it passes the
  pairing's checks: this member, an own-device answer, fresh keys, a fresh
  device id. -/
  | request (key : Nat) (ok : Bool)
  /-- The handover, sealed to the sealing key of the request with this key. -/
  | handover (key : Nat)
  | marker
  deriving DecidableEq, Repr

/-- Where the new device's call is. -/
inductive Phase where
  | none
  /-- Its request is sent, or is being sent again. -/
  | asking
  /-- It opened the handover; it holds the member's keys in memory. -/
  | opened
  /-- Its restore recorded itself: from here the call is a restore. -/
  | recorded
  deriving DecidableEq, Repr

/-- The key of the new device's own request. Whoever saw the code writes
requests with other keys, or replays this one. -/
def mine : Nat := 0

structure World where
  /-- The pairing's journal row; `none` once finished. -/
  row : Option Row
  /-- Its pending record, with the step of the read whose finding it holds. -/
  pending : Option Nat
  /-- What the request's one path holds. -/
  path : Path
  /-- The key of the checked request the pairing offered the app. -/
  checked : Option Nat
  /-- The key of the request the app approved: the only key a handover is
  ever sealed to. -/
  approved : Option Nat
  /-- The clock has reached the pairing's expiry. -/
  expired : Bool
  /-- This device stopped for good. -/
  stopped : Bool
  /-- The row a pass read and is acting on; memory only. -/
  read : Option Row
  /-- An admin's device swept the stopped device's requests and handovers and
  wrote its gone marker. -/
  gone : Bool
  /-- The new device's call. -/
  phase : Phase
  /-- History holds the new device's add-device entry. -/
  registered : Bool
  /-- The pairing ended because that entry applied. -/
  added : Bool
  /-- Slots this pairing claimed, and provider access it recorded. -/
  claims : Nat
  access : Nat
  deriving DecidableEq, Repr

/-- The pairing just made by the app's write (`start_pairing`). -/
def made : World where
  row := some ⟨.waiting none, 0⟩
  pending := none
  path := .empty
  checked := none
  approved := none
  expired := false
  stopped := false
  read := none
  gone := false
  phase := .none
  registered := false
  added := false
  claims := 0
  access := 0

structure Rules where
  /-- A pass saves its finding only while the row is at the step it read. -/
  guardRecord : Bool
  /-- Approving compares the request the app was shown with the one the
  pairing offers, and seals to that request's key. -/
  compares : Bool
  /-- An ending replaces whatever is at the path, the request or the handover,
  with the marker, and the pass finishes the pairing only once it is there. -/
  replacesAll : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true, true, true⟩

/-- Stages that end at the pairing's expiry. -/
def expires : Stage → Bool
  | .waiting _ | .approving _ | .handedOver _ => true
  | _ => false

def isEnding : Stage → Bool
  | .ending _ => true
  | _ => false

def isApproving : Stage → Bool
  | .approving _ => true
  | _ => false

def Path.isRequest : Path → Bool
  | .request _ _ => true
  | _ => false

/-- A write moving the row to `s`: its step increases and its pending record
ends. -/
def advance (w : World) (s : Stage) : World :=
  match w.row with
  | some r => { w with row := some ⟨s, r.step + 1⟩, pending := none }
  | none => w

/-- The row is still at the step `r` the pass read. -/
def atRead (w : World) (r : Row) : Bool := w.row.map (·.step) == some r.step

/-- A pass's write, conditional on the step it read. -/
def guarded (w : World) (r : Row) (f : World → World) : World :=
  if atRead w r then f w else w

/-- A pass saves what it found at `r` as the pending record. -/
def recordAt (R : Rules) (w : World) (r : Row) : World :=
  if (R.guardRecord && atRead w r) || (!R.guardRecord && w.row.isSome) then
    { w with pending := some r.step }
  else w

/-- A pass that found nothing to change clears the pending record of the step
it read: the step succeeded. -/
def clearAt (w : World) (r : Row) : World := guarded w r fun w => { w with pending := none }

/-- The marker goes where the request or the handover is. -/
def markPath (R : Rules) : Path → Path
  | .empty => .marker
  | .request _ _ => .marker
  | .handover k => if R.replacesAll then .marker else .handover k
  | .marker => .marker

/-- An admin's device deletes the stopped device's requests and handovers. -/
def sweepPath : Path → Path
  | .request _ _ | .handover _ => .empty
  | p => p

/-- The pass's write for the row it read. -/
def commitOf (R : Rules) (w : World) (r : Row) : World :=
  match r.stage with
  | .waiting none =>
    match w.path with
    | .request k true => guarded w r fun w => { advance w (.waiting (some k)) with checked := some k }
    -- A request that fails its checks ends the pairing as a decline does:
    -- it is created once at the one path and never changes.
    | .request _ false => guarded w r (advance · (.ending false))
    | _ => clearAt w r
  | .waiting (some _) => clearAt w r
  | .approving k =>
    if w.path = .handover k then guarded w r (advance · (.handedOver k)) else clearAt w r
  | .handedOver _ =>
    if w.registered then guarded w r fun w => { advance w (.ending false) with added := true }
    else clearAt w r
  | .ending forget =>
    if !R.replacesAll || w.path = .marker then
      guarded w r fun w =>
        if forget then { w with row := none, pending := none } else advance w .ended
    else clearAt w r
  | .ended => w

/-- What the new device's check of the request's path finds. -/
inductive Check where
  | waiting
  | handover (key : Nat)
  | ended
  deriving DecidableEq, Repr

/-- The handover, if it is there; the marker or the gone marker, which end the
call; a request, its own or another, which it waits for; and nothing, which it
answers by sending its request again while the pairing is open. -/
def check (w : World) : Check :=
  if w.gone then .ended
  else match w.path with
    | .marker => .ended
    | .handover k => .handover k
    | .request _ _ => .waiting
    | .empty => if w.expired then .ended else .waiting

inductive Act where
  /-- A pass reads the pairing's row. -/
  | read
  /-- A crash loses the pass's read. -/
  | crash
  /-- The pass's write for what it read and did. -/
  | commit
  /-- The pass puts the marker at the path. -/
  | mark
  /-- The pass deletes the request it is replacing. -/
  | delete
  /-- The pass creates the handover where the path is empty. -/
  | create
  /-- An attempt fails with a storage error, saved as its pending record. -/
  | fail
  /-- The clock reaches the pairing's expiry. -/
  | tick
  /-- The expiry's write. -/
  | expiry
  /-- The app approves the request it was shown, by its key. -/
  | approve (key : Nat)
  | decline
  /-- The app ends the pairing (`end_pairing`), or acknowledges that it ended. -/
  | close
  /-- Whoever saw the code writes a request. -/
  | send (key : Nat) (ok : Bool)
  /-- The device stops for good. -/
  | stop
  /-- An admin's device applies the stop: it sweeps the requests and handovers
  and writes the gone marker. -/
  | sweep
  /-- The store is opened again, ending every pairing a closed handle left. -/
  | open
  /-- A reset of the showing device carries the pairing into the new
  database, ended. -/
  | reset
  /-- The new device sends its request, or sends it again. -/
  | ask
  /-- The new device finds the handover and opens it. -/
  | take
  /-- The new device's call is cancelled, dropped or fails. -/
  | cancel
  /-- The new device's restore records itself. -/
  | record
  /-- The new device's add-device entry reaches history. -/
  | register
  /-- The new device's check finds the pairing ended: the call ends. -/
  | endCall
  deriving DecidableEq, Repr

/-- The app's step accepts this action now. -/
def accepted (R : Rules) (w : World) : Act → Bool
  | .approve k => !w.expired &&
      (if R.compares then w.row.map (·.stage) == some (.waiting (some k))
       else (w.row.map (·.stage)).any fun | .waiting (some _) => true | _ => false)
  | .decline => !w.expired && (w.row.map (·.stage)).any fun | .waiting (some _) => true | _ => false
  | .close => w.row.isSome
  | _ => false

def enabled (R : Rules) (w : World) : Act → Bool
  | .read => !w.stopped && w.read.isNone && (w.row.any fun r => r.stage != .ended)
  | .crash => true
  | .commit => !w.stopped && w.read.isSome
  | .mark => !w.stopped && w.read.any (isEnding ·.stage)
  | .delete => !w.stopped && w.read.any (isApproving ·.stage) && w.path.isRequest
  | .create => !w.stopped && w.read.any (isApproving ·.stage) && w.path == .empty
  | .fail => w.read.isSome
  | .tick => !w.expired
  | .expiry => w.expired && w.row.any (expires ·.stage)
  | .send _ _ => w.path == .empty && !w.gone
  | .stop => !w.stopped
  | .sweep => w.stopped && !w.gone
  | .open | .reset => !w.stopped && w.row.isSome
  | .ask => (w.phase == .none || w.phase == .asking) && w.path == .empty && !w.gone && !w.expired
  | .take => w.phase == .asking && check w == .handover mine
  | .cancel => w.phase == .asking || w.phase == .opened
  | .record => w.phase == .opened
  | .register => w.phase == .recorded && !w.registered
  | .endCall => w.phase == .asking && check w == .ended
  | a => accepted R w a

/-- Opening the store: a pairing a closed handle left ends, forgotten; one
that already ended is forgotten. -/
def opened (w : World) : World :=
  match w.row with
  | some ⟨.ended, _⟩ => { w with row := none, pending := none, read := none }
  | some ⟨.ending true, _⟩ => { w with read := none }
  | some _ => { advance w (.ending true) with read := none }
  | none => w

/-- The app's end: forgetting an ended pairing, or ending a pairing the app
closes. -/
def closed (w : World) : World :=
  match w.row with
  | some ⟨.ended, _⟩ => { w with row := none, pending := none }
  | some ⟨.ending true, _⟩ => w
  | some _ => advance w (.ending true)
  | none => w

/-- The key an approval seals the handover to: the key of the request the
pairing checked and offered. An approval that compares has the same key as
the request the app was shown; one that doesn't seals to the stored request
whichever the app was shown. -/
def sealKey (R : Rules) (w : World) (shown : Nat) : Nat :=
  if R.compares then shown
  else match w.row with
    | some ⟨.waiting (some k), _⟩ => k
    | _ => shown

def act (R : Rules) (w : World) : Act → World
  | .read => { w with read := w.row }
  | .crash => { w with read := none }
  | .commit => match w.read with
    | some r => { commitOf R w r with read := none }
    | none => w
  | .mark => { w with path := markPath R w.path }
  | .delete => { w with path := .empty }
  | .create => match w.read with
    | some ⟨.approving k, _⟩ => { w with path := .handover k }
    | _ => w
  | .fail => match w.read with
    | some r => { recordAt R w r with read := none }
    | none => w
  | .tick => { w with expired := true }
  | .expiry => advance w (.ending false)
  | .approve k => { advance w (.approving (sealKey R w k)) with approved := some k }
  | .decline => advance w (.ending false)
  | .close => closed w
  | .send k ok => { w with path := .request k ok }
  | .stop => { w with stopped := true, row := none, pending := none, read := none }
  | .sweep => { w with gone := true, path := sweepPath w.path }
  | .open => opened w
  | .reset => { advance w (.ending true) with read := none }
  | .ask => { w with path := .request mine true, phase := .asking }
  | .take => { w with phase := .opened }
  | .cancel => { w with phase := .none }
  | .record => { w with phase := .recorded }
  | .register => { w with registered := true }
  | .endCall => { w with phase := .none }

inductive Step (R : Rules) : World → World → Prop
  | step (w : World) (a : Act) (h : enabled R w a = true) : Step R w (act R w a)

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' w'' : World} : Step R w w' → Steps R w' w'' → Steps R w w''

def Reachable (R : Rules) (w : World) : Prop := Steps R made w

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act R w a) rest else none

theorem trace_steps (R : Rules) :
    ∀ (acts : List Act) (w w' : World), trace R w acts = some w' → Steps R w w'
  | [], w, w', h => by
    simp only [trace, Option.some.injEq] at h
    subst h
    exact .refl w
  | a :: rest, w, w', h => by
    simp only [trace] at h
    split at h
    · exact .step (.step w a ‹_›) (trace_steps R rest _ _ h)
    · cases h

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


/-! ## What every reachable world satisfies -/

/-- The key of the request a stage offers or holds. -/
def offeredKey : Stage → Option Nat
  | .waiting (some k) | .approving k | .handedOver k => some k
  | _ => none

/-- The key of the request a stage has approved. -/
def approvedKey : Stage → Option Nat
  | .approving k | .handedOver k => some k
  | _ => none

structure Inv (w : World) : Prop where
  /-- A pending record holds what a pass found at the row's current step. -/
  fresh : ∀ p, w.pending = some p → w.row.map (·.step) = some p
  /-- The row a pass read is the row now, or a later write changed the step. -/
  readRow : ∀ r, w.read = some r → ∃ r', w.row = some r' ∧ (r.step < r'.step ∨ r' = r)
  /-- A finished pairing holds no read and no pending record, and left the
  marker at its path, unless its device stopped for good. -/
  noRow : w.row = none → w.read = none ∧ w.pending = none ∧ (w.path = .marker ∨ w.stopped = true)
  /-- An ended pairing is read by no pass, and its marker is in place. -/
  endedClean : ∀ r, w.row = some r → r.stage = .ended → w.read = none ∧ w.path = .marker
  /-- A request the pairing offers or approved was checked and offered. -/
  offered : ∀ r k, w.row = some r → offeredKey r.stage = some k → w.checked = some k
  /-- A handover the row journals is sealed to the approved request's key. -/
  journaled : ∀ r k, w.row = some r → approvedKey r.stage = some k → w.approved = some k
  /-- A pairing waiting for a request has approved none. -/
  waiting : ∀ r q, w.row = some r → r.stage = .waiting q → w.approved = none
  /-- A pass in flight read an approval of the approved key. -/
  readKey : ∀ r k, w.read = some r → r.stage = .approving k → w.approved = some k
  /-- The approved request is the one the pairing checked and offered. -/
  approvedChecked : ∀ k, w.approved = some k → w.checked = some k
  /-- Only the approved request's key has a handover in storage. -/
  handover : ∀ k, w.path = .handover k → w.approved = some k
  /-- A handover in storage belongs to a pairing not yet ended, or to a device
  stopped for good, which an admin's device sweeps. -/
  owned : ∀ k, w.path = .handover k →
    (∃ r, w.row = some r ∧ r.stage ≠ .ended) ∨ w.stopped = true
  /-- The sweep leaves no request or handover. -/
  gone : w.gone = true → w.stopped = true ∧ w.path.isRequest = false ∧ ∀ k, w.path ≠ .handover k
  /-- A stopped device finished its pairing. -/
  stopped : w.stopped = true → w.row = none
  /-- No slot claimed, no access recorded. -/
  claims : w.claims = 0 ∧ w.access = 0
  /-- The new device opens only a handover sealed to its own key. -/
  phase : (w.phase = .opened ∨ w.phase = .recorded) → w.approved = some mine
  /-- It registers only once its restore recorded itself. -/
  registered : w.registered = true → w.phase = .recorded
  added : w.added = true → w.registered = true

theorem inv_made : Inv made where
  fresh := by simp [made]
  readRow := by simp [made]
  noRow := by simp [made]
  endedClean := by
    rintro r h
    simp only [made, Option.some.injEq] at h
    subst h
    simp
  offered := by
    rintro r k h
    simp only [made, Option.some.injEq] at h
    subst h
    simp [offeredKey]
  journaled := by
    rintro r k h
    simp only [made, Option.some.injEq] at h
    subst h
    simp [approvedKey]
  waiting := by simp [made]
  readKey := by simp [made]
  approvedChecked := by simp [made]
  handover := by simp [made]
  owned := by simp [made]
  gone := by simp [made]
  stopped := by simp [made]
  claims := by simp [made]
  phase := by simp [made]
  registered := by simp [made]
  added := by simp [made]

@[simp] theorem advance_row_none (w : World) (s : Stage) (h : w.row = none) : advance w s = w := by
  simp [advance, h]

theorem advance_some {w : World} {r : Row} (hrow : w.row = some r) (s : Stage) :
    advance w s = { w with row := some ⟨s, r.step + 1⟩, pending := none } := by
  simp [advance, hrow]

@[simp] theorem markPath_correct (p : Path) : markPath Rules.correct p = .marker := by
  cases p <;> simp [markPath, Rules.correct]

@[simp] theorem sweepPath_ne (p : Path) (k : Nat) : (sweepPath p = .handover k) = False := by
  cases p <;> simp [sweepPath]

@[simp] theorem sweepPath_isRequest (p : Path) : (sweepPath p).isRequest = false := by
  cases p <;> simp [sweepPath, Path.isRequest]

theorem read_row {w : World} (inv : Inv w) {r : Row} (h : w.read = some r) :
    ∃ r', w.row = some r' ∧ (r.step < r'.step ∨ r' = r) := inv.readRow r h

theorem check_handover {w : World} {k : Nat} (h : check w = .handover k) :
    w.path = .handover k := by
  unfold check at h
  split at h
  · cases h
  · rcases hp : w.path <;> simp_all
    split at h <;> cases h

theorem check_ended {w : World} (h : check w = .ended) :
    w.gone = true ∨ w.path = .marker ∨ (w.path = .empty ∧ w.expired = true) := by
  unfold check at h
  split at h
  · exact Or.inl ‹_›
  · rcases hp : w.path <;> simp_all


/-- Every change of the row to an ending keeps the invariant: the ending
offers and approves nothing, and a read that survives is of an earlier step. -/
theorem inv_ending {w : World} (inv : Inv w) {r : Row} (hrow : w.row = some r) (b : Bool)
    (rd : Option Row) (hrd : rd = w.read ∨ rd = none) :
    Inv { w with row := some ⟨.ending b, r.step + 1⟩, pending := none, read := rd } where
  fresh := by intro p h; simp at h
  readRow := by
    intro q hq
    have hq' : rd = some q := hq
    refine ⟨_, rfl, Or.inl ?_⟩
    rcases hrd with h | h
    · rw [h] at hq'
      obtain ⟨r', h1, h2⟩ := inv.readRow q hq'
      rw [hrow] at h1
      cases h1
      show q.step < r.step + 1
      rcases h2 with h2 | h2
      · omega
      · subst h2; omega
    · rw [h] at hq'; cases hq'
  noRow := by intro h; simp at h
  endedClean := by intro q hq hs; simp at hq; subst hq; simp at hs
  offered := by intro q k hq hk; simp at hq; subst hq; simp [offeredKey] at hk
  journaled := by intro q k hq hk; simp at hq; subst hq; simp [approvedKey] at hk
  waiting := by intro q s hq hs; simp at hq; subst hq; simp at hs
  readKey := by
    intro q k hq hs
    have hq' : rd = some q := hq
    rcases hrd with h | h
    · rw [h] at hq'; exact inv.readKey q k hq' hs
    · rw [h] at hq'; cases hq'
  approvedChecked := inv.approvedChecked
  handover := inv.handover
  owned := by intro k _; exact Or.inl ⟨_, rfl, by simp⟩
  gone := inv.gone
  stopped := by intro h; have := inv.stopped h; rw [hrow] at this; cases this
  claims := inv.claims
  phase := inv.phase
  registered := inv.registered
  added := inv.added

theorem inv_drop {w : World} (inv : Inv w) : Inv { w with read := none } where
  fresh := inv.fresh
  readRow := by intro q h; cases h
  noRow := fun h => ⟨rfl, (inv.noRow h).2⟩
  endedClean := fun q hq hs => ⟨rfl, (inv.endedClean q hq hs).2⟩
  offered := inv.offered
  journaled := inv.journaled
  waiting := inv.waiting
  readKey := by intro q k h; cases h
  approvedChecked := inv.approvedChecked
  handover := inv.handover
  owned := inv.owned
  gone := inv.gone
  stopped := inv.stopped
  claims := inv.claims
  phase := inv.phase
  registered := inv.registered
  added := inv.added

theorem inv_clear {w : World} (inv : Inv w) : Inv { w with pending := none, read := none } where
  fresh := by intro p h; cases h
  readRow := by intro q h; cases h
  noRow := fun h => ⟨rfl, rfl, (inv.noRow h).2.2⟩
  endedClean := fun q hq hs => ⟨rfl, (inv.endedClean q hq hs).2⟩
  offered := inv.offered
  journaled := inv.journaled
  waiting := inv.waiting
  readKey := by intro q k h; cases h
  approvedChecked := inv.approvedChecked
  handover := inv.handover
  owned := inv.owned
  gone := inv.gone
  stopped := inv.stopped
  claims := inv.claims
  phase := inv.phase
  registered := inv.registered
  added := inv.added

/-- A pass whose read is still the row's step read exactly this row. -/
theorem row_of_atRead {w : World} (inv : Inv w) {r : Row} (hr : w.read = some r)
    (hat : atRead w r = true) : w.row = some r := by
  obtain ⟨r', h1, h2⟩ := inv.readRow r hr
  have h3 : r'.step = r.step := by
    simp only [atRead, h1, Option.map_some, beq_iff_eq, Option.some.injEq] at hat
    exact hat
  rcases h2 with h2 | h2
  · omega
  · rw [h1, h2]

theorem inv_guarded_drop {w : World} (inv : Inv w) (r : Row) (f : World → World)
    (hf : atRead w r = true → Inv { f w with read := none }) :
    Inv { guarded w r f with read := none } := by
  by_cases hat : atRead w r = true
  · simp only [guarded, hat, ite_true]; exact hf hat
  · simp only [guarded, hat]; simp at hat; simp; exact inv_drop inv

theorem inv_clearAt {w : World} (inv : Inv w) (r : Row) :
    Inv { clearAt w r with read := none } := by
  unfold clearAt
  apply inv_guarded_drop inv
  intro _
  exact inv_clear (w := w) inv

theorem inv_offer {w : World} (inv : Inv w) {n k : Nat} (hrow : w.row = some ⟨.waiting none, n⟩)
    (hp : w.path = .request k true) :
    Inv { w with row := some ⟨.waiting (some k), n + 1⟩, pending := none, checked := some k, read := none } where
  fresh := by intro p h; cases h
  readRow := by intro q h; cases h
  noRow := by intro h; cases h
  endedClean := by
    intro q hq hs
    cases hq
    cases hs
  offered := by
    intro q k' hq hk
    cases hq
    simp [offeredKey] at hk
    subst hk
    rfl
  journaled := by
    intro q k' hq hk
    cases hq
    simp [approvedKey] at hk
  waiting := by
    intro q s hq hs
    exact inv.waiting _ _ hrow rfl
  readKey := by intro q k h; cases h
  approvedChecked := by
    intro k' h
    have := inv.waiting _ _ hrow rfl
    rw [this] at h; cases h
  handover := by
    intro k' h
    have : w.path = .handover k' := h
    rw [hp] at this; cases this
  owned := by
    intro k' h
    have : w.path = .handover k' := h
    rw [hp] at this; cases this
  gone := inv.gone
  stopped := by
    intro h
    have := inv.stopped h
    rw [hrow] at this; cases this
  claims := inv.claims
  phase := by
    intro h
    have := inv.waiting _ _ hrow rfl
    have h2 := inv.phase h
    rw [this] at h2; cases h2
  registered := inv.registered
  added := inv.added

theorem inv_handedOver {w : World} (inv : Inv w) {n k : Nat}
    (hrow : w.row = some ⟨.approving k, n⟩) :
    Inv { w with row := some ⟨.handedOver k, n + 1⟩, pending := none, read := none } where
  fresh := by intro p h; cases h
  readRow := by intro q h; cases h
  noRow := by intro h; cases h
  endedClean := by
    intro q hq hs
    cases hq
    cases hs
  offered := by
    intro q k' hq hk
    cases hq
    simp [offeredKey] at hk
    subst hk
    exact inv.offered _ _ hrow rfl
  journaled := by
    intro q k' hq hk
    cases hq
    simp [approvedKey] at hk
    subst hk
    exact inv.journaled _ _ hrow rfl
  waiting := by
    intro q s hq hs
    cases hq
    cases hs
  readKey := by intro q k h; cases h
  approvedChecked := inv.approvedChecked
  handover := inv.handover
  owned := by
    intro k' h
    exact Or.inl ⟨_, rfl, by simp⟩
  gone := inv.gone
  stopped := by
    intro h
    have := inv.stopped h
    rw [hrow] at this; cases this
  claims := inv.claims
  phase := inv.phase
  registered := inv.registered
  added := inv.added

theorem inv_forget {w : World} (inv : Inv w) (hp : w.path = .marker) :
    Inv { w with row := none, pending := none, read := none } where
  fresh := by intro p h; cases h
  readRow := by intro q h; cases h
  noRow := fun _ => ⟨rfl, rfl, Or.inl hp⟩
  endedClean := by intro q h; cases h
  offered := by intro q k h; cases h
  journaled := by intro q k h; cases h
  waiting := by intro q s h; cases h
  readKey := by intro q k h; cases h
  approvedChecked := inv.approvedChecked
  handover := inv.handover
  owned := by
    intro k h
    have : w.path = .handover k := h
    rw [hp] at this; cases this
  gone := inv.gone
  stopped := fun _ => rfl
  claims := inv.claims
  phase := inv.phase
  registered := inv.registered
  added := inv.added

theorem inv_ended {w : World} (inv : Inv w) {n : Nat} (hrow : w.row = some ⟨.ending false, n⟩)
    (hp : w.path = .marker) : Inv { w with row := some ⟨.ended, n + 1⟩, pending := none, read := none } where
  fresh := by intro p h; cases h
  readRow := by intro q h; cases h
  noRow := by intro h; cases h
  endedClean := fun q hq hs => ⟨rfl, hp⟩
  offered := by
    intro q k' hq hk
    cases hq
    simp [offeredKey] at hk
  journaled := by
    intro q k' hq hk
    cases hq
    simp [approvedKey] at hk
  waiting := by
    intro q s hq hs
    cases hq
    cases hs
  readKey := by intro q k h; cases h
  approvedChecked := inv.approvedChecked
  handover := inv.handover
  owned := by
    intro k' h
    have : w.path = .handover k' := h
    rw [hp] at this; cases this
  gone := inv.gone
  stopped := by
    intro h
    have := inv.stopped h
    rw [hrow] at this; cases this
  claims := inv.claims
  phase := inv.phase
  registered := inv.registered
  added := inv.added

theorem inv_commit {w : World} (inv : Inv w) {r : Row} (hr : w.read = some r) :
    Inv { commitOf Rules.correct w r with read := none } := by
  obtain ⟨stage, n⟩ := r
  cases stage with
  | waiting q =>
    cases q with
    | none =>
      cases hp : w.path with
      | request k ok =>
        cases ok
        · simp only [commitOf, hp]
          apply inv_guarded_drop inv
          intro hat
          have hrow := row_of_atRead inv hr hat
          rw [advance_some hrow]
          exact inv_ending inv hrow false none (Or.inr rfl)
        · simp only [commitOf, hp]
          apply inv_guarded_drop inv
          intro hat
          have hrow := row_of_atRead inv hr hat
          rw [advance_some hrow]
          exact inv_offer inv hrow hp
      | empty => simp only [commitOf, hp]; exact inv_clearAt inv _
      | handover k => simp only [commitOf, hp]; exact inv_clearAt inv _
      | marker => simp only [commitOf, hp]; exact inv_clearAt inv _
    | some k => simp only [commitOf]; exact inv_clearAt inv _
  | approving k =>
    simp only [commitOf]
    split
    · apply inv_guarded_drop inv
      intro hat
      have hrow := row_of_atRead inv hr hat
      rw [advance_some hrow]
      exact inv_handedOver inv hrow
    · exact inv_clearAt inv _
  | handedOver k =>
    simp only [commitOf]
    split
    · next hreg =>
      apply inv_guarded_drop inv
      intro hat
      have hrow := row_of_atRead inv hr hat
      have h := inv_ending inv hrow false none (Or.inr rfl)
      rw [advance_some hrow]
      exact { h with added := fun _ => by simpa using hreg }
    · exact inv_clearAt inv _
  | ending forget =>
    by_cases hp : w.path = .marker
    · simp only [commitOf, Rules.correct, hp, Bool.not_true, Bool.false_or, decide_true, ite_true]
      apply inv_guarded_drop inv
      intro hat
      have hrow := row_of_atRead inv hr hat
      cases forget
      · simp only [Bool.false_eq_true, ite_false]
        rw [advance_some hrow]
        exact inv_ended inv hrow hp
      · simp only [ite_true]
        exact inv_forget inv hp
    · simp only [commitOf, Rules.correct, hp, Bool.not_true, Bool.false_or, decide_false]
      exact inv_clearAt inv _
  | ended => simp only [commitOf]; exact inv_drop inv

theorem inv_read {w : World} (inv : Inv w) (hne : ∀ q, w.row = some q → q.stage ≠ .ended) :
    Inv { w with read := w.row } where
  fresh := inv.fresh
  readRow := fun q h => ⟨q, h, Or.inr rfl⟩
  noRow := fun h => ⟨h, (inv.noRow h).2⟩
  endedClean := fun q hq hs => absurd hs (hne q hq)
  offered := inv.offered
  journaled := inv.journaled
  waiting := inv.waiting
  readKey := fun q k h hs => inv.journaled q k h (by simp [approvedKey, hs])
  approvedChecked := inv.approvedChecked
  handover := inv.handover
  owned := inv.owned
  gone := inv.gone
  stopped := inv.stopped
  claims := inv.claims
  phase := inv.phase
  registered := inv.registered
  added := inv.added

theorem inv_approve {w : World} (inv : Inv w) {n k : Nat}
    (hrow : w.row = some ⟨.waiting (some k), n⟩) :
    Inv { w with row := some ⟨.approving k, n + 1⟩, pending := none, approved := some k } where
  fresh := by intro p h; cases h
  readRow := by
    intro q h
    obtain ⟨r', h1, h2⟩ := inv.readRow q h
    rw [hrow] at h1
    cases h1
    refine ⟨_, rfl, Or.inl ?_⟩
    show q.step < n + 1
    rcases h2 with h2 | h2
    · simp at h2; omega
    · subst h2; simp
  noRow := by intro h; cases h
  endedClean := by intro q hq hs; cases hq; cases hs
  offered := by
    intro q k' hq hk
    cases hq
    simp [offeredKey] at hk
    subst hk
    exact inv.offered _ _ hrow rfl
  journaled := by
    intro q k' hq hk
    cases hq
    simp [approvedKey] at hk
    subst hk
    rfl
  waiting := by intro q s hq hs; cases hq; cases hs
  readKey := by
    intro q k' h hs
    have := inv.readKey q k' h hs
    rw [inv.waiting _ _ hrow rfl] at this
    cases this
  approvedChecked := by
    intro k' h
    have h' : some k = some k' := h
    cases h'
    exact inv.offered _ _ hrow rfl
  handover := by
    intro k' h
    have := inv.handover k' h
    rw [inv.waiting _ _ hrow rfl] at this
    cases this
  owned := by
    intro k' _
    exact Or.inl ⟨_, rfl, by simp⟩
  gone := inv.gone
  stopped := by
    intro h
    have := inv.stopped h
    rw [hrow] at this; cases this
  claims := inv.claims
  phase := by
    intro h
    have := inv.phase h
    rw [inv.waiting _ _ hrow rfl] at this
    cases this
  registered := inv.registered
  added := inv.added

theorem inv_create {w : World} (inv : Inv w) {n k : Nat}
    (hr : w.read = some ⟨.approving k, n⟩) (hp : w.path = .empty) (hs : w.stopped = false) :
    Inv { w with path := .handover k } where
  fresh := inv.fresh
  readRow := inv.readRow
  noRow := by
    intro h
    have := (inv.noRow h).1
    rw [hr] at this; cases this
  endedClean := by
    intro q hq hq2
    obtain ⟨h1, h2⟩ := inv.endedClean q hq hq2
    rw [hp] at h2; cases h2
  offered := inv.offered
  journaled := inv.journaled
  waiting := inv.waiting
  readKey := inv.readKey
  approvedChecked := inv.approvedChecked
  handover := by
    intro k' h
    have h' : Path.handover k = Path.handover k' := h
    cases h'
    exact inv.readKey _ _ hr rfl
  owned := by
    intro k' h
    obtain ⟨r', h1, _⟩ := inv.readRow _ hr
    refine Or.inl ⟨r', h1, ?_⟩
    intro hs2
    obtain ⟨_, h2⟩ := inv.endedClean r' h1 hs2
    rw [hp] at h2; cases h2
  gone := by
    intro g
    have := (inv.gone g).1
    rw [hs] at this; cases this
  stopped := inv.stopped
  claims := inv.claims
  phase := inv.phase
  registered := inv.registered
  added := inv.added

theorem inv_record {w : World} (inv : Inv w) {r : Row} (hat : atRead w r = true) :
    Inv { w with pending := some r.step, read := none } := by
  have hr : w.row.map (·.step) = some r.step := by
    simpa [atRead] using hat
  exact
  { fresh := by intro p h; cases h; exact hr
    readRow := by intro q h; cases h
    noRow := by
      intro h
      rw [h] at hr; cases hr
    endedClean := fun q hq hs => ⟨rfl, (inv.endedClean q hq hs).2⟩
    offered := inv.offered
    journaled := inv.journaled
    waiting := inv.waiting
    readKey := by intro q k h; cases h
    approvedChecked := inv.approvedChecked
    handover := inv.handover
    owned := inv.owned
    gone := inv.gone
    stopped := inv.stopped
    claims := inv.claims
    phase := inv.phase
    registered := inv.registered
    added := inv.added }

theorem inv_stop {w : World} (inv : Inv w) :
    Inv { w with stopped := true, row := none, pending := none, read := none } where
  fresh := by intro p h; cases h
  readRow := by intro q h; cases h
  noRow := fun _ => ⟨rfl, rfl, Or.inr rfl⟩
  endedClean := by intro q h; cases h
  offered := by intro q k h; cases h
  journaled := by intro q k h; cases h
  waiting := by intro q s h; cases h
  readKey := by intro q k h; cases h
  approvedChecked := inv.approvedChecked
  handover := inv.handover
  owned := fun _ _ => Or.inr rfl
  gone := fun g => ⟨rfl, (inv.gone g).2⟩
  stopped := fun _ => rfl
  claims := inv.claims
  phase := inv.phase
  registered := inv.registered
  added := inv.added

/-- Ending a pairing that has a row, by a writer that leaves a pass's read. -/
theorem inv_end_keep {w : World} (inv : Inv w) {r : Row} (hrow : w.row = some r) (b : Bool) :
    Inv (advance w (.ending b)) := by
  rw [advance_some hrow]
  exact inv_ending inv hrow b w.read (Or.inl rfl)

/-- Ending a pairing that has a row, by a writer that drops the pass's read. -/
theorem inv_end_drop {w : World} (inv : Inv w) {r : Row} (hrow : w.row = some r) (b : Bool) :
    Inv { advance w (.ending b) with read := none } := by
  rw [advance_some hrow]
  exact inv_ending inv hrow b none (Or.inr rfl)
set_option maxHeartbeats 4000000 in
theorem inv_step (w : World) (a : Act) (inv : Inv w)
    (e : enabled Rules.correct w a = true) : Inv (act Rules.correct w a) := by
  cases a
  case crash => exact inv_drop inv
  case read =>
    refine inv_read inv ?_
    intro q hq hs
    simp [enabled, hq, hs] at e
  case commit =>
    cases hr : w.read with
    | none => simp [enabled, hr] at e
    | some r => simp only [act, hr]; exact inv_commit inv hr
  case fail =>
    cases hr : w.read with
    | none => simp [enabled, hr] at e
    | some r =>
      simp only [act, hr]
      by_cases hat : atRead w r = true
      · simp only [recordAt, Rules.correct, hat, Bool.true_and, Bool.not_true, Bool.false_and,
          Bool.or_false, ite_true]
        exact inv_record inv hat
      · simp only [recordAt, Rules.correct, hat, Bool.true_and, Bool.not_true, Bool.false_and,
          Bool.or_false]
        simp
        exact inv_drop inv
  case expiry =>
    cases hrow : w.row with
    | none => simp [enabled, hrow] at e
    | some r => exact inv_end_keep inv hrow false
  case decline =>
    cases hrow : w.row with
    | none => simp [enabled, accepted, hrow] at e
    | some r => exact inv_end_keep inv hrow false
  case approve k =>
    cases hrow : w.row with
    | none => simp [enabled, accepted, hrow] at e
    | some r =>
      obtain ⟨st, n⟩ := r
      have hst : st = .waiting (some k) := by
        simp [enabled, accepted, hrow] at e
        exact e.2
      subst hst
      show Inv { advance w (.approving (sealKey Rules.correct w k)) with approved := some k }
      have hseal : sealKey Rules.correct w k = k := by simp [sealKey, Rules.correct]
      rw [hseal]
      rw [advance_some hrow]
      exact inv_approve inv hrow
  case close =>
    cases hrow : w.row with
    | none => simp [enabled, accepted, hrow] at e
    | some r =>
      obtain ⟨st, n⟩ := r
      cases st with
      | ended =>
        obtain ⟨hrd, hp⟩ := inv.endedClean _ hrow rfl
        simp only [act, closed, hrow]
        rw [hrd]
        exact inv_forget inv hp
      | ending b =>
        cases b
        · simp only [act, closed, hrow]; exact inv_end_keep inv hrow true
        · simp only [act, closed, hrow]; exact inv
      | waiting q => simp only [act, closed, hrow]; exact inv_end_keep inv hrow true
      | approving k => simp only [act, closed, hrow]; exact inv_end_keep inv hrow true
      | handedOver k => simp only [act, closed, hrow]; exact inv_end_keep inv hrow true
  case stop => exact inv_stop inv
  case «open» =>
    cases hrow : w.row with
    | none => simp [enabled, hrow] at e
    | some r =>
      obtain ⟨st, n⟩ := r
      cases st with
      | ended =>
        obtain ⟨hrd, hp⟩ := inv.endedClean _ hrow rfl
        simp only [act, opened, hrow]
        exact inv_forget inv hp
      | ending b =>
        cases b
        · simp only [act, opened, hrow]; exact inv_end_drop inv hrow true
        · simp only [act, opened, hrow]
          have h := inv_drop inv
          rw [hrow] at h
          exact h
      | waiting q => simp only [act, opened, hrow]; exact inv_end_drop inv hrow true
      | approving k => simp only [act, opened, hrow]; exact inv_end_drop inv hrow true
      | handedOver k => simp only [act, opened, hrow]; exact inv_end_drop inv hrow true
  case reset =>
    cases hrow : w.row with
    | none => simp [enabled, hrow] at e
    | some r => exact inv_end_drop inv hrow true
  case create =>
    cases hr : w.read with
    | none => simp [enabled, hr] at e
    | some r =>
      obtain ⟨st, n⟩ := r
      cases st with
      | approving k =>
        have e' := e
        simp [enabled, hr, isApproving] at e'
        obtain ⟨hs, hp⟩ := e'
        simp only [act, hr]
        have h := inv_create inv hr hp hs
        rw [hr] at h
        exact h
      | _ => simp [enabled, hr, isApproving] at e
  all_goals
    obtain ⟨fresh, readRow, noRow, endedClean, offered, journaled, waiting, readKey,
      approvedChecked, handover, owned, gone, stopped, claims, phase, registered, added⟩ := inv
  case tick => constructor <;> simp_all [act]
  case mark => constructor <;> simp_all [act, enabled]
  case delete =>
    obtain ⟨r, hr⟩ : ∃ r, w.read = some r := by
      cases h : w.read <;> simp_all [enabled]
    obtain ⟨r', hrow, _⟩ := readRow r hr
    constructor <;> simp_all [act, enabled]
  case send => constructor <;> simp_all [act, enabled]
  case sweep => constructor <;> simp_all [act, enabled]
  case ask =>
    obtain ⟨⟨⟨h, _⟩, _⟩, _⟩ : (((w.phase = .none ∨ w.phase = .asking) ∧ w.path = .empty) ∧ w.gone = false) ∧ w.expired = false := by simpa [enabled] using e
    rcases h with h | h <;>
      constructor <;> simp_all [act, enabled]
  case take =>
    have hp := check_handover (w := w) (k := mine) (by simp_all [enabled])
    constructor <;> simp_all [act, enabled]
  case cancel =>
    rcases (by simpa [enabled] using e : w.phase = .asking ∨ w.phase = .opened) with h | h <;>
      constructor <;> simp_all [act, enabled]
  case record => constructor <;> simp_all [act, enabled]
  case register => constructor <;> simp_all [act, enabled]
  case endCall => constructor <;> simp_all [act, enabled]

theorem reachable_inv {w : World} (h : Reachable Rules.correct w) : Inv w :=
  stays Inv inv_step h inv_made

/-! ### No write from a stale read -/

theorem commitOf_stale (R : Rules) (w : World) (r : Row) (stale : atRead w r = false) :
    commitOf R w r = w := by
  unfold commitOf
  split <;> simp_all [guarded, clearAt] <;> (repeat' split) <;> simp_all

/-- A pending record always holds what a pass found at the row's current
step: a finding from a step the row has left is never saved. -/
theorem finding_current {w : World} (h : Reachable Rules.correct w) {p : Nat}
    (hp : w.pending = some p) : w.row.map (·.step) = some p :=
  (reachable_inv h).fresh p hp

/-- A pass's write changes the row only when the row is the one it read. -/
theorem commit_acts_on_read {w : World} {r : Row} (h : Reachable Rules.correct w)
    (hr : w.read = some r) (changed : (act Rules.correct w .commit).row ≠ w.row) :
    w.row = some r := by
  obtain ⟨r', hrow, hs⟩ := (reachable_inv h).readRow r hr
  by_cases cur : atRead w r = true
  · rcases hs with hs | hs
    · simp [atRead, hrow] at cur; omega
    · rw [hrow, hs]
  · simp [act, hr, commitOf_stale Rules.correct w r (by simpa using cur)] at changed

/-! ### What the app sees never goes back -/

/-- What the app is shown of a pairing (`coven_pairing`'s status). -/
inductive Shown where
  | waiting
  /-- A checked request is waiting for the app, with its key. -/
  | request (key : Nat)
  | approved
  | ended
  deriving DecidableEq, Repr

def shown (w : World) : Option Shown :=
  match w.row with
  | none => none
  | some ⟨.waiting none, _⟩ => some .waiting
  | some ⟨.waiting (some k), _⟩ => some (.request k)
  | some ⟨.approving _, _⟩ | some ⟨.handedOver _, _⟩ => some .approved
  | some ⟨.ending _, _⟩ | some ⟨.ended, _⟩ => some .ended

/-- How far the pairing has gone, as the app is shown it. -/
def stageRank : Option Row → Nat
  | some ⟨.waiting none, _⟩ => 0
  | some ⟨.waiting (some _), _⟩ => 1
  | some ⟨.approving _, _⟩ | some ⟨.handedOver _, _⟩ => 2
  | some ⟨.ending _, _⟩ | some ⟨.ended, _⟩ => 3
  | none => 4

def Shown.rank : Option Shown → Nat
  | some .waiting => 0
  | some (.request _) => 1
  | some .approved => 2
  | some .ended => 3
  | none => 4

theorem shown_rank (w : World) : Shown.rank (shown w) = stageRank w.row := by
  unfold shown
  rcases w.row with _ | ⟨st, n⟩
  · rfl
  · cases st with
    | waiting q => cases q <;> rfl
    | _ => rfl

theorem commit_forward {w : World} (inv : Inv w) {r : Row} (hr : w.read = some r) :
    stageRank w.row ≤ stageRank (commitOf Rules.correct w r).row := by
  by_cases hat : atRead w r = true
  · have hrow := row_of_atRead inv hr hat
    obtain ⟨st, n⟩ := r
    cases st with
    | waiting q =>
      cases q with
      | none =>
        cases hp : w.path with
        | request k ok =>
          cases ok <;> simp [commitOf, hp, guarded, hat, advance, hrow, stageRank]
        | _ => simp [commitOf, hp, guarded, hat, clearAt, hrow, stageRank]
      | some k => simp [commitOf, guarded, hat, clearAt, hrow, stageRank]
    | approving k =>
      by_cases hp : w.path = .handover k <;>
        simp [commitOf, hp, guarded, hat, clearAt, advance, hrow, stageRank]
    | handedOver k =>
      by_cases hp : w.registered = true <;>
        simp [commitOf, hp, guarded, hat, clearAt, advance, hrow, stageRank]
    | ending b =>
      by_cases hp : w.path = .marker <;> cases b <;>
        simp [commitOf, Rules.correct, hp, guarded, hat, clearAt, advance, hrow, stageRank]
    | ended => simp [commitOf, hrow, stageRank]
  · rw [commitOf_stale Rules.correct w r (by simpa using hat)]
    exact Nat.le_refl _

/-- The writers that never change the row. -/
def keepsRow : Act → Bool
  | .commit | .expiry | .approve _ | .decline | .close | .stop | .open | .reset => false
  | _ => true

theorem act_row_keeps (R : Rules) (w : World) (a : Act) (h : keepsRow a = true) :
    (act R w a).row = w.row := by
  cases a <;> simp_all [keepsRow, act]
  · split <;> simp
  · split <;> simp [recordAt] <;> split <;> simp

theorem forward (w : World) (a : Act) (inv : Inv w)
    (e : enabled Rules.correct w a = true) :
    stageRank w.row ≤ stageRank (act Rules.correct w a).row := by
  by_cases k : keepsRow a = true
  · rw [act_row_keeps _ _ _ k]; exact Nat.le_refl _
  cases a <;> simp [keepsRow] at k
  case commit =>
    cases hr : w.read with
    | none => simp [enabled, hr] at e
    | some r => simpa [act, hr] using commit_forward inv hr
  all_goals
    rcases hrow : w.row with _ | ⟨(_ | _) | _ | _ | (_ | _) | _, n⟩ <;>
    simp_all [act, enabled, stageRank, advance, opened, closed, accepted, sealKey, Rules.correct,
      expires]

/-- What the app is shown: `coven_pairing`'s status and whether the clock
reached the expiry. -/
structure View where
  shown : Option Shown
  expired : Bool

def view (w : World) : View := ⟨shown w, w.expired⟩

/-- What the app offers for a pairing: approving the request it shows,
declining it, and closing the screen. -/
def offered (v : View) : Act → Bool
  | .approve k => !v.expired && v.shown == some (.request k)
  | .decline => !v.expired && v.shown.any fun | .request _ => true | _ => false
  | .close => v.shown.isSome
  | _ => false

/-- The app offers approving, declining and closing exactly when the write
steps accept them. -/
theorem offered_accepted (w : World) :
    (∀ k, offered (view w) (.approve k) = accepted Rules.correct w (.approve k)) ∧
    offered (view w) .decline = accepted Rules.correct w .decline ∧
    offered (view w) .close = accepted Rules.correct w .close := by
  refine ⟨fun k => ?_, ?_, ?_⟩ <;>
  rcases hrow : w.row with _ | ⟨(_ | _) | _ | _ | (_ | _) | _, n⟩ <;> cases hx : w.expired <;>
    simp [offered, accepted, view, shown, hrow, hx, Rules.correct] <;> first | rfl | (simp [BEq.beq])

/-- The app is shown the pairing as approved exactly while the row journals
the approval or the handover is in the request's place. -/
theorem approved_iff {w : World} (h : Reachable Rules.correct w) :
    shown w = some .approved ↔
      ∃ r k, w.row = some r ∧ approvedKey r.stage = some k ∧ w.approved = some k := by
  have inv := reachable_inv h
  constructor
  · intro s
    rcases hrow : w.row with _ | ⟨(_ | _) | _ | _ | (_ | _) | _, n⟩ <;>
      simp_all [shown, approvedKey]
    · exact inv.journaled _ _ hrow rfl
    · exact inv.journaled _ _ hrow rfl
  · rintro ⟨⟨st, n⟩, k, hrow, hk, _⟩
    cases st <;> simp_all [shown, approvedKey]

/-! ### The request's key and the handover's -/

/-- The request a pairing offers or approved is the one that passed its
checks. -/
theorem offered_checked {w : World} (h : Reachable Rules.correct w) {r : Row} {k : Nat}
    (hrow : w.row = some r) (hk : offeredKey r.stage = some k) : w.checked = some k :=
  (reachable_inv h).offered r k hrow hk

/-- A handover is sealed only to the key of the request the app approved, and
that request is the one the pairing checked and offered. -/
theorem handover_only_approved {w : World} (h : Reachable Rules.correct w) {k : Nat}
    (hp : w.path = .handover k) : w.approved = some k ∧ w.checked = some k := by
  have inv := reachable_inv h
  have a := inv.handover k hp
  exact ⟨a, inv.approvedChecked k a⟩

/-- The handover the row journals is sealed to the approved key. -/
theorem journaled_approved {w : World} (h : Reachable Rules.correct w) {r : Row} {k : Nat}
    (hrow : w.row = some r) (hk : approvedKey r.stage = some k) :
    w.approved = some k ∧ w.checked = some k := by
  have inv := reachable_inv h
  have a := inv.journaled r k hrow hk
  exact ⟨a, inv.approvedChecked k a⟩

/-- Before the app approves, nothing at the path carries the member's keys and
the new device holds none. -/
theorem nothing_before_approval {w : World} (h : Reachable Rules.correct w)
    (none : w.approved = none) :
    (∀ k, w.path ≠ .handover k) ∧ w.phase ≠ .opened ∧ w.phase ≠ .recorded := by
  have inv := reachable_inv h
  refine ⟨fun k hp => ?_, fun hp => ?_, fun hp => ?_⟩
  · have := inv.handover k hp; rw [none] at this; cases this
  · have := inv.phase (Or.inl hp); rw [none] at this; cases this
  · have := inv.phase (Or.inr hp); rw [none] at this; cases this

/-- A handover at the path belongs to a pairing that journals or has stored
it, or is ending, or to a device stopped for good, which an admin's device
sweeps. -/
theorem handover_owned {w : World} (h : Reachable Rules.correct w) {k : Nat}
    (hp : w.path = .handover k) :
    w.stopped = true ∨ ∃ r, w.row = some r ∧
      (approvedKey r.stage = some k ∨ isEnding r.stage = true) := by
  have inv := reachable_inv h
  rcases inv.owned k hp with ⟨⟨st, n⟩, hrow, hne⟩ | hs
  · right
    refine ⟨_, hrow, ?_⟩
    have ha := inv.handover k hp
    cases st with
    | waiting q => have := inv.waiting _ _ hrow rfl; rw [this] at ha; cases ha
    | approving k' =>
      left
      have := inv.journaled _ _ hrow rfl
      rw [this] at ha; cases ha; rfl
    | handedOver k' =>
      left
      have := inv.journaled _ _ hrow rfl
      rw [this] at ha; cases ha; rfl
    | ending b => right; rfl
    | ended => exact absurd rfl hne
  · exact Or.inl hs

/-! ### A pairing records nothing in the claim journal -/

theorem claims_zero {w : World} (h : Reachable Rules.correct w) :
    w.claims = 0 ∧ w.access = 0 := (reachable_inv h).claims

/-! ### The new device -/

/-- A check that finds the handover never reports the pairing ended. -/
theorem check_handover_not_ended {w : World} (h : Reachable Rules.correct w) {k : Nat}
    (hp : w.path = .handover k) : check w = .handover k := by
  have inv := reachable_inv h
  have g : w.gone = false := by
    cases hg : w.gone
    · rfl
    · exact absurd hp ((inv.gone hg).2.2 k)
  simp [check, g, hp]

/-- Cancelling the new device, dropped or failed, at any point before its
restore records itself leaves no add-device entry in history. -/
theorem cancel_binds_nothing {w : World} (h : Reachable Rules.correct w)
    (e : enabled Rules.correct w .cancel = true) :
    w.registered = false ∧ w.phase ≠ .recorded ∧ w.added = false := by
  have inv := reachable_inv h
  have ph : w.phase = .asking ∨ w.phase = .opened := by simpa [enabled] using e
  have nr : w.phase ≠ .recorded := by rcases ph with p | p <;> simp [p]
  have reg : w.registered = false := by
    cases hr : w.registered
    · rfl
    · exact absurd (inv.registered hr) nr
  refine ⟨reg, nr, ?_⟩
  cases ha : w.added
  · rfl
  · have := inv.added ha; rw [reg] at this; cases this

/-! ### A request that fails a check ends the pairing -/

@[simp] theorem markPath_all (p : Path) : markPath ⟨true, true, true⟩ p = .marker :=
  markPath_correct p

theorem refused_ends {w : World} (h : Reachable Rules.correct w) {n k : Nat}
    (hrow : w.row = some ⟨.waiting none, n⟩) (hp : w.path = .request k false) :
    ∃ w', trace Rules.correct w [.crash, .read, .commit, .read, .mark, .commit] = some w' ∧
      w'.row.map (·.stage) = some .ended ∧ w'.path = .marker ∧ w'.pending = none ∧
      w'.checked = w.checked := by
  have inv := reachable_inv h
  have st : w.stopped = false := by
    cases hs : w.stopped
    · rfl
    · have := inv.stopped hs; rw [hrow] at this; cases this
  simp [trace, enabled, act, commitOf, guarded, atRead, advance, hrow, st, hp, Rules.correct,
      isEnding]

/-! ### The request-and-handover loop converges -/

/-- While the pairing is approved and the handover not yet in place, the
pass that replaces a request with the handover can finish: it deletes the
request at the one path, creates the handover there, and its write moves the
row on. -/
theorem approved_converges {w : World} (h : Reachable Rules.correct w) {n k : Nat}
    (hrow : w.row = some ⟨.approving k, n⟩) (hp : w.path = .empty ∨ w.path.isRequest = true) :
    ∃ acts w', trace Rules.correct w acts = some w' ∧
      w'.row.map (·.stage) = some (.handedOver k) ∧ w'.path = .handover k := by
  have inv := reachable_inv h
  have st : w.stopped = false := by
    cases hs : w.stopped
    · rfl
    · have := inv.stopped hs; rw [hrow] at this; cases this
  cases hpath : w.path with
  | empty =>
    refine ⟨[.crash, .read, .create, .commit], ?_⟩
    simp [trace, enabled, act, commitOf, guarded, atRead, advance, hrow, st, hpath, isApproving]
  | request j ok =>
    refine ⟨[.crash, .read, .delete, .create, .commit], ?_⟩
    simp [trace, enabled, act, commitOf, guarded, atRead, advance, hrow, st, hpath, isApproving,
      Path.isRequest]
  | handover j => simp [hpath, Path.isRequest] at hp
  | marker => simp [hpath, Path.isRequest] at hp

/-- The new device's request lands in the window between the pass's delete
and its create: the request is at the path again, the create waits, the pass
deletes it once more and creates the handover, and the new device, whose
check finds it, opens it. -/
theorem request_in_window {w : World} (h : Reachable Rules.correct w) {n : Nat}
    (hread : w.read = some ⟨.approving mine, n⟩) (hrow : w.row = some ⟨.approving mine, n⟩)
    (hp : w.path.isRequest = true) (ph : w.phase = .none ∨ w.phase = .asking)
    (live : w.expired = false) (g : w.gone = false) :
    ∃ w', trace Rules.correct w [.delete, .ask, .delete, .create, .commit, .take] = some w' ∧
      w'.phase = .opened ∧ w'.row.map (·.stage) = some (.handedOver mine) := by
  have inv := reachable_inv h
  have st : w.stopped = false := by
    cases hs : w.stopped
    · rfl
    · have := inv.stopped hs; rw [hrow] at this; cases this
  cases hpath : w.path with
  | request j ok =>
    rcases ph with ph | ph <;>
    simp [trace, enabled, act, commitOf, guarded, atRead, advance, hrow, hread, st, hpath, live, g,
      ph, isApproving, Path.isRequest, check, mine]
  | empty => simp [hpath, Path.isRequest] at hp
  | handover j => simp [hpath, Path.isRequest] at hp
  | marker => simp [hpath, Path.isRequest] at hp

/-! ### Counterexamples: each rule is needed -/

/-- A pass that saves its finding whatever the row's step now is. -/
def unguardedFinding : Rules := { Rules.correct with guardRecord := false }

/-- Without the pass saving its finding only at the step it read, a finding
from before the app's close is saved over the closed row. -/
theorem unguarded_finding_is_stale :
    ∃ w, trace unguardedFinding made [.read, .close, .fail] = some w ∧
      w.pending = some 0 ∧ w.row.map (·.step) = some 1 := ⟨_, rfl, rfl, rfl⟩

/-- An approval that doesn't compare the request the app was shown with the
stored one. -/
def unsealed : Rules := { Rules.correct with compares := false }

/-- Without the approval comparing, the app that approves a request it was
shown that isn't the stored one is accepted, and the handover is journaled
for a key the app never approved. -/
theorem uncompared_approval_seals_wrong :
    ∃ w, trace unsealed made [.send 3 true, .read, .commit] = some w ∧
      offered (view w) (.approve 7) = false ∧ accepted unsealed w (.approve 7) = true ∧
      ∃ w', trace unsealed w [.approve 7] = some w' ∧
        w'.row.map (·.stage) = some (.approving 3) ∧ w'.approved = some 7 :=
  ⟨_, rfl, rfl, rfl, _, rfl, rfl, rfl⟩

/-- An ending that replaces a request but leaves a handover, and finishes
the pairing without checking the marker is in place. -/
def partialEnding : Rules := { Rules.correct with replacesAll := false }

/-- Without the ending replacing the handover too, a pairing the app closes
after the handover was stored is forgotten with the handover still at the
path: the new device that finds it opens a pairing that ended. -/
theorem ending_leaves_handover :
    ∃ w, trace partialEnding made
        [.send 3 true, .read, .commit, .approve 3, .read, .delete, .create, .commit, .close,
          .read, .mark, .commit] = some w ∧
      w.row = none ∧ w.path = .handover 3 ∧ w.stopped = false := ⟨_, rfl, rfl, rfl, rfl⟩

/-! ### Every pairing ends -/

/-- Nothing left to do: no row, no pending record, no pass in flight, and at
the path the marker, or, on a device stopped for good, whatever the sweep left. -/
def quiet (w : World) : Bool :=
  w.row.isNone && w.pending.isNone && w.read.isNone &&
    (w.path == .marker || (w.stopped && w.gone && w.path == .empty))

/-- What coven does on its own, and the app's closing of the screen. -/
def coven : Act → Bool
  | .read | .crash | .commit | .mark | .delete | .create | .tick | .expiry | .sweep | .close => true
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

def stageMeasure : Option Row → Nat
  | none => 0
  | some ⟨.ended, _⟩ => 1
  | some ⟨.ending _, _⟩ => 2
  | some _ => 3

/-- What is left to do. -/
def measure (w : World) : Nat :=
  4 * stageMeasure w.row + (!w.expired).toNat + w.read.isSome.toNat + w.pending.isSome.toNat +
    (w.stopped && !w.gone).toNat

theorem one {w : World} (a : Act) (own : coven a = true)
    (e : enabled Rules.correct w a = true) : Moves w (act Rules.correct w a) :=
  .step a own e (.refl _)

theorem progress (w : World) (inv : Inv w) (busy : quiet w = false) :
    ∃ w', Moves w w' ∧ measure w' < measure w := by
  by_cases rd : w.read.isSome = true
  · exact ⟨_, one .crash rfl rfl, by simp [act, measure, rd]⟩
  have hread : w.read = none := by simpa using rd
  rcases hrow : w.row with _ | ⟨st, n⟩
  · obtain ⟨_, hpend, hpath⟩ := inv.noRow hrow
    have notMarker : w.path ≠ .marker := by
      intro hm; simp [quiet, hrow, hpend, hread, hm] at busy
    have stopped : w.stopped = true := by
      rcases hpath with hm | hs
      · exact absurd hm notMarker
      · exact hs
    have notGone : w.gone = false := by
      cases hg : w.gone
      · rfl
      · obtain ⟨_, noReq, noHand⟩ := inv.gone hg
        have : w.path = .empty := by
          cases hp : w.path with
          | empty => rfl
          | request j ok => simp [hp, Path.isRequest] at noReq
          | handover j => exact absurd hp (noHand j)
          | marker => exact absurd hp notMarker
        simp [quiet, hrow, hpend, hread, this, stopped, hg] at busy
    refine ⟨_, one .sweep rfl (by simp [enabled, stopped, notGone]), ?_⟩
    simp [act, measure, hrow, hpend, hread, stopped, notGone]
  have st' : w.stopped = false := by
    cases hs : w.stopped
    · rfl
    · have := inv.stopped hs; rw [hrow] at this; cases this
  -- Live stages end at the pairing's expiry: the clock reaches it, then the expiry's write.
  have expiring : expires st = true → ∃ w', Moves w w' ∧ measure w' < measure w := by
    intro ex
    by_cases tick : w.expired = true
    · refine ⟨_, one .expiry rfl (by simp [enabled, tick, hrow, ex]), ?_⟩
      cases st <;> simp [act, advance, measure, hrow, hread, tick, stageMeasure, expires] at ex ⊢ <;>
        omega
    · have tick' : w.expired = false := by simpa using tick
      exact ⟨_, one .tick rfl (by simp [enabled, tick']), by simp [act, measure, tick']⟩
  cases st with
  | ended =>
    refine ⟨_, one .close rfl (by simp [enabled, accepted, hrow]), ?_⟩
    simp [act, closed, measure, hrow, hread, stageMeasure]
    omega
  | ending forget =>
    have e1 : enabled Rules.correct w .read = true := by simp [enabled, st', hread, hrow]
    have e2 : enabled Rules.correct (act Rules.correct w .read) .mark = true := by
      simp [enabled, act, st', hrow, isEnding]
    have e3 : enabled Rules.correct (act Rules.correct (act Rules.correct w .read) .mark) .commit
        = true := by
      simp [enabled, act, st', hrow]
    refine ⟨_, .step .read rfl e1 (.step .mark rfl e2 (one .commit rfl e3)), ?_⟩
    cases forget <;>
      simp [act, commitOf, guarded, atRead, advance, measure, hrow, hread, st', stageMeasure,
        Rules.correct] <;> omega
  | waiting q => exact expiring rfl
  | approving k => exact expiring rfl
  | handedOver k => exact expiring rfl

/-- Every pairing ends, on every path: from any reachable state, coven alone
and the app closing the screen reach a state with no row, no pending record
and no pass in flight, and at the path the marker (or the sweep's result on
a device stopped for good). That includes crashes anywhere, and a pairing
the app never approved or declined. -/
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

/-- A settled pairing leaves no handover at the path. -/
theorem settled_no_handover {w : World} (q : quiet w = true) (k : Nat) :
    w.path ≠ .handover k := by
  simp only [quiet, Bool.and_eq_true, Bool.or_eq_true, beq_iff_eq] at q
  rcases q.2 with p | ⟨_, p⟩ <;> simp [p]

end CovenSequence.PairingLifecycle
