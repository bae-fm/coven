/-!
# An invite and the app's operations, from the inviting device's journal

One invite on the device that made it (§13.2), the operation that binds its
request's pair, and one other operation the app's write journaled (§11.4).
Every writer that acts on them is a step:

- the app's write steps: approving, declining and cancelling the invite,
  retrying its pending record, acknowledging an outcome, journaling an admin
  change. Each reads and decides in its own transaction;
- the expiry, which reads and decides in one transaction too;
- a sync pass, which reads the invite's row, does storage work with no
  transaction open, and only then writes. Between its read and its write any
  other writer may commit;
- a crash, which loses the pass's read and nothing durable;
- the joiner writing its request, whose keys may be ones history has already
  bound to a member, current or past, rather than fresh ones;
- what a pass learns: the approver's demotion, and a stop for good (removal,
  replacement or member keys replaced, §14.1, or history showing another copy
  of this device, §19.3);
- another admin's device applying that stop first: it writes this device's
  gone marker and deletes the joiner's request, and the marker turns away
  any request sent later (§13.2);
- the invite's share: the account the invite shares the store with, which
  history records before the grant, and which the owner's devices take back
  once the invite ended or, the share recorded, its device stopped for good
  (§13.2, §14.2).

The journal row carries its step, which every change of the row increases.
A pass's write is conditional on the step it read; so is the pending record
it saves for what it found (`recordAt`).

`Rules` names each choice the properties depend on. The properties are
proved under `correct`, which has all of them; leaving any one out is a
checked counterexample below. `rust` names the rules the Rust follows, which
the differential test compares.
-/
namespace CovenSequence.InviteLifecycle

inductive Delivery where
  | inPerson
  | sent
  deriving DecidableEq, Repr

/-- The invite's state, as its journal row encodes it (`InviteState`). -/
inductive Stage where
  | requested
  | grant
  /-- The code is shown; `request` once a checked request waits for the app. -/
  | waiting (request : Bool)
  | approving
  /-- A kept S3 replacement records its member's new key. -/
  | recording
  | settled
  deriving DecidableEq, Repr

/-- The journal row: its state and the last step it completed. -/
structure Row where
  stage : Stage
  step : Nat
  deriving DecidableEq, Repr

/-- How a pending record lets its work go on: by itself after its wait, only
when the app retries it, or never. -/
inductive Retry where
  | auto
  | app
  | never
  deriving DecidableEq, Repr

/-- How an operation ended, as `coven_operations.outcome` lists it. -/
inductive Outcome where
  | landed
  | rejected
  /-- The device stopped for good before it ended: nothing refused it. -/
  | stopped
  deriving DecidableEq, Repr

@[simp] theorem rejected_not_landed : (Outcome.rejected == Outcome.landed) = false := by decide

/-- What the invite is: how it reaches the person, and whether approving it
also records a new S3 key for an existing member. -/
structure Config where
  delivery : Delivery
  records : Bool
  deriving DecidableEq, Repr

structure World where
  /-- The invite's journal row; `none` once finished. -/
  row : Option Row
  /-- Its pending record, which every change of the row ends. -/
  pending : Option Retry
  /-- The step of the read whose finding the pending record holds. -/
  pendingAt : Nat
  /-- The binding operation's journal row exists. -/
  binding : Bool
  /-- Its `coven_operations` entry, for a sent invite's: `some none` while it
  runs, `some (some o)` once ended, `none` when not listed. -/
  outcome : Option (Option Outcome)
  /-- How many binding operations were journaled. -/
  bindings : Nat
  /-- The applied history binds the request's pair. -/
  bound : Bool
  /-- An admin change the app journaled, as `coven_operations` lists it. -/
  op : Option (Option Outcome)
  /-- The joiner's request object is in storage. -/
  request : Bool
  /-- The request's keys are not fresh: history has bound one of them to a
  member already. -/
  used : Bool
  /-- This device's gone marker is in storage: another admin's device
  applied its stop for good (§13.2). -/
  gone : Bool
  /-- The clock has reached the invite's expiry. -/
  expired : Bool
  /-- The applied state lets this device approve: it is an admin, registered,
  with its member's current pair. -/
  approvable : Bool
  /-- This device stopped for good. -/
  stopped : Bool
  /-- The row a pass read and is acting on; memory only. -/
  read : Option Row
  /-- History records the invite's share (`AddInviteShare`). -/
  recorded : Bool
  /-- The provider share the invite granted stands. -/
  shared : Bool
  /-- This device ended the invite's share: it recorded the end, or, with no
  record, took the share back itself. -/
  ended : Bool
  deriving DecidableEq, Repr

/-- The invite just made by the app's write (`create_invite`). -/
def made : World :=
  ⟨some ⟨.requested, 0⟩, none, 0, false, none, 0, false, none, false, false, false, false, true,
    false, none, false, false, false⟩

structure Rules where
  /-- A pass saves its finding only while the row is at the step it read. -/
  guardRecord : Bool
  /-- A stop ends every operation the app's writes journaled. -/
  endOpsOnStop : Bool
  /-- A stop ends this device's invites, and their bindings, too. -/
  endInvitesOnStop : Bool
  /-- An approval whose binding ended unbound ends the invite; it never
  journals a second binding. -/
  noReclaim : Bool
  /-- The binding ends in the transaction that moves its invite on. -/
  bindingWithInvite : Bool
  /-- A refused access record ends the invite. -/
  settleRefusedRecord : Bool
  /-- `coven_invites` shows an approval under way as its own stage. -/
  showApproving : Bool
  /-- The approve step refuses what the applied state refuses. -/
  approveChecksApplied : Bool
  /-- The pass records the invite's share in history before granting it. -/
  recordShare : Bool
  /-- The pass checks that the request's keys are fresh when it opens the
  request, not only when its binding applies. -/
  checkKeys : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true, true, true, true, true, true, true, false, true, true⟩

/-- The rules the Rust follows, as the differential test drives it. -/
def Rules.rust : Rules := ⟨true, true, true, true, true, true, true, false, true, false⟩

/-- Stages that end at the invite's expiry; approval, once begun, finishes. -/
def expires : Stage → Bool
  | .requested | .grant | .waiting _ => true
  | _ => false

/-- Whether the pass may run the invite: no pending record, or one whose
wait ends by itself. -/
def ready : Option Retry → Bool
  | none | some .auto => true
  | _ => false

/-- A write moving the row to `s`: its step increases and its pending record
ends. -/
def advance (w : World) (s : Stage) : World :=
  match w.row with
  | some r => { w with row := some ⟨s, r.step + 1⟩, pending := none, pendingAt := r.step + 1 }
  | none => w

/-- The row is still at the step `r` the pass read. -/
def atRead (w : World) (r : Row) : Bool := w.row.map (·.step) == some r.step

/-- A pass's write, conditional on the step it read. -/
def guarded (w : World) (r : Row) (f : World → World) : World :=
  if atRead w r then f w else w

/-- A pass saves what it found at `r` as the pending record. -/
def recordAt (R : Rules) (w : World) (r : Row) (p : Retry) : World :=
  if (R.guardRecord && atRead w r) || (!R.guardRecord && w.row.isSome) then
    { w with pending := some p, pendingAt := r.step }
  else w

/-- What the binding's operation ends with, once a pass runs it: the caught-up
history refuses it when this device may no longer approve, or when it has
bound one of the request's keys already. -/
def bindingOutcome (w : World) : Outcome := if w.approvable && !w.used then .landed else .rejected

/-- End the binding's operation with its outcome; a sent invite's is listed. -/
def endBinding (c : Config) (w : World) (o : Outcome) : World :=
  { w with
    binding := false
    bound := w.bound || o == .landed
    outcome := if c.delivery = .sent then some (some o) else w.outcome }

/-- The stage after approval: recording a replacement's key, or ended. -/
def afterBound (c : Config) : Stage := if c.records then .recording else .settled

/-- The approval ended unbound in an earlier attempt: end the invite, or (the
fault) journal a second binding. -/
def unbound (R : Rules) (c : Config) (w : World) : World :=
  if R.noReclaim then advance w .settled
  else { w with
    binding := true
    bindings := w.bindings + 1
    outcome := if c.delivery = .sent then some none else w.outcome }

/-- What the transaction applying a stop changes. -/
def stopEffect (R : Rules) (w : World) : World :=
  let w := { w with stopped := true, approvable := false }
  let w := if R.endOpsOnStop then
      { w with
        op := if w.op = some none then some (some .stopped) else w.op
        outcome := if w.outcome = some none then some (some .stopped) else w.outcome
        binding := if w.outcome = some none then false else w.binding }
    else w
  -- The invite's journal row goes, with its binding's and its pending
  -- record; a pass's attempt on it has nothing left to write.
  if R.endInvitesOnStop then { w with binding := false, row := none, pending := none, read := none }
  else w

inductive Act where
  /-- A pass reads the invite's row. -/
  | read
  /-- A crash loses the pass's read. -/
  | crash
  /-- The pass's write for what it read and did. -/
  | commit
  /-- The pass ends an invite it read past its expiry. -/
  | lapse
  /-- The pass ends the binding's operation, in a transaction of its own. -/
  | bind
  /-- An attempt fails with a storage error, saved as its pending record. -/
  | fail (p : Retry)
  /-- A pass runs the app's admin change. -/
  | runOp
  /-- A pass applies the approver's demotion. -/
  | demote
  /-- A pass applies a stop for good. -/
  | stop
  /-- The clock reaches the invite's expiry. -/
  | tick
  /-- The expiry's write. -/
  | expiry
  | approve
  | decline
  | cancel
  | retry
  /-- The app acknowledges the binding's outcome. -/
  | ack
  /-- The app acknowledges its admin change's outcome. -/
  | ackOp
  /-- The app journals an admin change. -/
  | administer
  /-- The joiner writes its request, `used` when its keys are not fresh. -/
  | send (used : Bool)
  /-- Another admin's device applies this device's stop for good: it writes
  the gone marker, then deletes the request. -/
  | gone
  /-- An owner's device takes the invite's share back: this device once it
  ended the share, or any of them once the share is recorded and this device
  stopped for good, unless the account is a member's by then. -/
  | revoke
  deriving DecidableEq, Repr

/-- The pass's write for the row it read. -/
def commitOf (R : Rules) (c : Config) (w : World) (r : Row) : World :=
  match r.stage with
  | .requested =>
    if w.approvable then guarded w r fun w => advance { w with recorded := R.recordShare } .grant
    else recordAt R w r .never
  | .grant => guarded w r fun w => advance { w with shared := true } (.waiting false)
  | .waiting false =>
    -- A request that fails its checks is not offered or approved: the
    -- invite goes on waiting, with the failure as its pending record.
    if w.request && R.checkKeys && w.used then recordAt R w r .app
    else if w.request then
      match c.delivery with
      | .sent => guarded w r (advance · (.waiting true))
      | .inPerson => guarded w r fun w =>
          { advance w .approving with binding := true, bindings := w.bindings + 1 }
    else recordAt R w r .auto
  | .waiting true => recordAt R w r .app
  | .approving =>
    if w.binding then
      if R.bindingWithInvite then guarded w r fun w =>
        let o := bindingOutcome w
        advance (endBinding c w o) (if o = .landed then afterBound c else .settled)
      else w
    else guarded w r fun w => if w.bound then advance w (afterBound c) else unbound R c w
  | .recording =>
    if w.approvable || R.settleRefusedRecord then guarded w r (advance · .settled)
    else recordAt R w r .never
  | .settled => guarded w r fun w =>
    { w with row := none, request := false, pending := none, ended := true }

/-- The app's step accepts this action now. -/
def accepted (R : Rules) (w : World) : Act → Bool
  | .approve => !w.expired && w.row.map (·.stage) == some (.waiting true) &&
      (!R.approveChecksApplied || w.approvable)
  | .decline => !w.expired && w.row.map (·.stage) == some (.waiting true)
  | .cancel => !w.expired && (w.row.map (expires ·.stage)).getD false
  | .retry => w.row.isSome && w.pending != some .never
  | .ack => w.outcome.any Option.isSome
  | .ackOp => w.op.any Option.isSome
  | .administer => w.op.isNone && !w.stopped && w.approvable
  | _ => false

/-- A pass applies slots before its invites, and while it runs an approval's
binding. -/
def applying (w : World) : Bool :=
  !w.stopped && (w.read.isNone || w.read.map (·.stage) == some .approving)

def enabled (R : Rules) (_c : Config) (w : World) : Act → Bool
  | .read => !w.stopped && w.read.isNone &&
      (w.row.any fun r => (w.expired && expires r.stage) || ready w.pending)
  | .crash => true
  | .commit => !w.stopped && w.read.isSome
  | .lapse => !w.stopped && w.expired && w.read.any (expires ·.stage)
  | .bind => !w.stopped && !R.bindingWithInvite && w.binding &&
      w.read.map (·.stage) == some .approving
  | .fail p => w.read.isSome && p != .never
  | .runOp => !w.stopped && w.op == some none
  | .demote => applying w && w.approvable
  | .stop => applying w
  | .tick => !w.expired
  | .expiry => w.expired && w.row.any (expires ·.stage)
  | .send _ => !w.request && !w.gone
  | .gone => !w.gone
  | .revoke => w.shared && !w.bound && (w.ended || (w.recorded && (w.stopped || w.gone)))
  | a => accepted R w a

def act (R : Rules) (c : Config) (w : World) : Act → World
  | .read => { w with read := w.row }
  | .crash => { w with read := none }
  | .commit => match w.read with
    | some r => { commitOf R c w r with read := none }
    | none => w
  | .lapse => match w.read with
    | some r => { guarded w r (advance · .settled) with read := none }
    | none => w
  | .bind => endBinding c w (bindingOutcome w)
  | .fail p => match w.read with
    | some r => { recordAt R w r p with read := none }
    | none => w
  | .runOp => { w with op := some (some (if w.approvable then .landed else .rejected)) }
  | .demote => { w with approvable := false }
  | .stop => stopEffect R w
  | .tick => { w with expired := true }
  | .expiry => advance w .settled
  | .approve => { advance w .approving with
      binding := true
      bindings := w.bindings + 1
      outcome := some none }
  | .decline | .cancel => advance w .settled
  | .retry => { w with pending := none }
  | .ack => { w with outcome := none }
  | .ackOp => { w with op := none }
  | .administer => { w with op := some none }
  | .send used => { w with request := true, used }
  | .gone => { w with gone := true, request := false }
  | .revoke => { w with shared := false }

inductive Step (R : Rules) (c : Config) : World → World → Prop
  | step (w : World) (a : Act) (h : enabled R c w a = true) : Step R c w (act R c w a)

inductive Steps (R : Rules) (c : Config) : World → World → Prop
  | refl (w : World) : Steps R c w w
  | step {w w' w'' : World} : Step R c w w' → Steps R c w' w'' → Steps R c w w''

def Reachable (R : Rules) (c : Config) (w : World) : Prop := Steps R c made w

/-! ## What the app sees -/

/-- An invite as `coven_invites` lists it. -/
inductive Listed where
  | granting
  | waiting (request : Bool)
  | approving
  deriving DecidableEq, Repr

def listed (R : Rules) (w : World) : Option Listed :=
  match w.row with
  | some ⟨.requested, _⟩ | some ⟨.grant, _⟩ => some .granting
  | some ⟨.waiting b, _⟩ => some (.waiting b)
  | some ⟨.approving, _⟩ => if R.showApproving then some .approving else some (.waiting true)
  | _ => none

/-- Everything `coven_invites`, `coven_operations` and `coven_pending` show,
with whether the clock has reached the expiry the listed invite names. -/
structure View where
  invite : Option Listed
  expired : Bool
  pending : Option Retry
  outcome : Option (Option Outcome)
  op : Option (Option Outcome)
  deriving DecidableEq, Repr

def view (R : Rules) (w : World) : View := ⟨listed R w, w.expired, w.pending, w.outcome, w.op⟩

/-- What the app offers from the view: approving or declining a waiting
request, cancelling an invite not yet being approved, before the expiry;
retrying a pending record that can be retried; acknowledging an outcome. -/
def offered (v : View) : Act → Bool
  | .approve | .decline => !v.expired && v.invite == some (.waiting true)
  | .cancel => !v.expired && (v.invite == some .granting || v.invite.any fun
      | .waiting _ => true
      | _ => false)
  | .retry => v.pending == some .auto || v.pending == some .app
  | .ack => v.outcome.any Option.isSome
  | .ackOp => v.op.any Option.isSome
  | _ => false

/-- Nothing left for the app to follow: no invite listed, no operation
running, no pending record, and no share standing for an account no member
uses. -/
def quiet (R : Rules) (w : World) : Bool :=
  (listed R w).isNone && w.outcome != some none && w.op != some none && w.pending.isNone &&
    !(w.shared && !w.bound)

/-- How far the listing has gone. It only ever moves forward. -/
def Listed.rank : Option Listed → Nat
  | some .granting => 0
  | some (.waiting false) => 1
  | some (.waiting true) => 2
  | some .approving => 3
  | none => 4

/-- How far an operation's entry has gone: running, ended, acknowledged. -/
def entryRank : Option (Option Outcome) → Nat
  | some none => 0
  | some (some _) => 1
  | none => 2

/-! ## Traces -/

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) (c : Config) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R c w a then trace R c (act R c w a) rest else none

theorem trace_steps (R : Rules) (c : Config) :
    ∀ (acts : List Act) (w w' : World), trace R c w acts = some w' → Steps R c w w'
  | [], w, w', h => by
    simp only [trace, Option.some.injEq] at h
    subst h
    exact .refl w
  | a :: rest, w, w', h => by
    simp only [trace] at h
    split at h
    · exact .step (.step w a ‹_›) (trace_steps R c rest _ _ h)
    · cases h

theorem Steps.trans {R : Rules} {c : Config} {a b d : World} :
    Steps R c a b → Steps R c b d → Steps R c a d := by
  intro h k
  induction h with
  | refl => exact k
  | step s _ ih => exact .step s (ih k)

/-- The invite as far as a request waiting for the app's decision, from the
invite just made: the pass checks it and grants, shows the code, the joiner
sends its request, and the next pass reads it. -/
def toWaiting : List Act := [.read, .commit, .read, .commit, .send false, .read, .commit]

def sent : Config := ⟨.sent, false⟩

/-- A property every step keeps holds from then on. -/
theorem stays {R : Rules} {c : Config} (P : World → Prop)
    (keep : ∀ w a, P w → enabled R c w a = true → P (act R c w a)) :
    ∀ {w w'}, Steps R c w w' → P w → P w' := by
  intro w w' h
  induction h with
  | refl => exact id
  | step s _ ih =>
    cases s with
    | step a e => exact fun p => ih (keep _ a p e)

@[simp] theorem recordAt_row (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).row = w.row := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_stopped (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).stopped = w.stopped := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_binding (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).binding = w.binding := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_outcome (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).outcome = w.outcome := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_bindings (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).bindings = w.bindings := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_bound (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).bound = w.bound := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_op (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).op = w.op := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_request (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).request = w.request := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_used (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).used = w.used := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_gone (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).gone = w.gone := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_expired (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).expired = w.expired := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_approvable (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).approvable = w.approvable := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_read (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).read = w.read := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_recorded (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).recorded = w.recorded := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_shared (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).shared = w.shared := by
  unfold recordAt; split <;> rfl

@[simp] theorem recordAt_ended (R : Rules) (w : World) (r : Row) (p : Retry) :
    (recordAt R w r p).ended = w.ended := by
  unfold recordAt; split <;> rfl

@[simp] theorem advance_stopped (w : World) (s : Stage) : (advance w s).stopped = w.stopped := by
  unfold advance; split <;> rfl

@[simp] theorem advance_binding (w : World) (s : Stage) : (advance w s).binding = w.binding := by
  unfold advance; split <;> rfl

@[simp] theorem advance_outcome (w : World) (s : Stage) : (advance w s).outcome = w.outcome := by
  unfold advance; split <;> rfl

@[simp] theorem advance_bindings (w : World) (s : Stage) : (advance w s).bindings = w.bindings := by
  unfold advance; split <;> rfl

@[simp] theorem advance_bound (w : World) (s : Stage) : (advance w s).bound = w.bound := by
  unfold advance; split <;> rfl

@[simp] theorem advance_op (w : World) (s : Stage) : (advance w s).op = w.op := by
  unfold advance; split <;> rfl

@[simp] theorem advance_request (w : World) (s : Stage) : (advance w s).request = w.request := by
  unfold advance; split <;> rfl

@[simp] theorem advance_used (w : World) (s : Stage) : (advance w s).used = w.used := by
  unfold advance; split <;> rfl

@[simp] theorem advance_gone (w : World) (s : Stage) : (advance w s).gone = w.gone := by
  unfold advance; split <;> rfl

@[simp] theorem advance_expired (w : World) (s : Stage) : (advance w s).expired = w.expired := by
  unfold advance; split <;> rfl

@[simp] theorem advance_approvable (w : World) (s : Stage) : (advance w s).approvable = w.approvable := by
  unfold advance; split <;> rfl

@[simp] theorem advance_read (w : World) (s : Stage) : (advance w s).read = w.read := by
  unfold advance; split <;> rfl

@[simp] theorem advance_recorded (w : World) (s : Stage) :
    (advance w s).recorded = w.recorded := by
  unfold advance; split <;> rfl

@[simp] theorem advance_shared (w : World) (s : Stage) : (advance w s).shared = w.shared := by
  unfold advance; split <;> rfl

@[simp] theorem advance_ended (w : World) (s : Stage) : (advance w s).ended = w.ended := by
  unfold advance; split <;> rfl

/-! ## Counterexamples

Each leaves one rule out of `correct`. -/

def noInviteStop : Rules := { Rules.correct with endInvitesOnStop := false }

/-- A stopped device whose approval was under way when it stopped. -/
def StrandedApproval (w : World) : Prop :=
  w.stopped = true ∧ ∃ n, w.row = some ⟨.approving, n⟩

theorem stranded_approval_stays (c : Config) (w : World) (a : Act) (h : StrandedApproval w)
    (e : enabled noInviteStop c w a = true) : StrandedApproval (act noInviteStop c w a) := by
  obtain ⟨stopped, n, row⟩ := h
  cases a <;> simp_all [StrandedApproval, enabled, act, accepted, applying, expires]
  case fail => split <;> simp_all

/-- Without a stop ending the invites, an approval under way stays listed
forever on the stopped device: it never expires, and no pass runs again. -/
theorem stop_strands_approval :
    ∃ w, trace noInviteStop sent made (toWaiting ++ [.approve, .stop]) = some w ∧
      ∀ w', Steps noInviteStop sent w w' → listed noInviteStop w' = some .approving := by
  refine ⟨_, rfl, fun w' h => ?_⟩
  obtain ⟨_, n, row⟩ := stays _ (stranded_approval_stays sent) h ⟨rfl, _, rfl⟩
  simp [listed, row, noInviteStop, Rules.correct]

def recordStays : Rules := { Rules.correct with settleRefusedRecord := false }

/-- A kept replacement whose access record was refused, on a device that is
still running. -/
def StrandedRecord (w : World) : Prop :=
  w.stopped = true ∨ (w.read = none ∧ w.pending = some .never ∧ ∃ n, w.row = some ⟨.recording, n⟩)

theorem stranded_record_stays (c : Config) (w : World) (a : Act) (h : StrandedRecord w)
    (e : enabled recordStays c w a = true) : StrandedRecord (act recordStays c w a) := by
  rcases h with stopped | ⟨read, pending, n, row⟩
  · left
    cases a <;> simp_all [enabled, act, accepted, applying]
    all_goals (repeat' split) <;> simp_all
  · cases a <;> simp_all [StrandedRecord, enabled, act, accepted, applying, expires, ready]
    case stop => left; simp [stopEffect]; (repeat' split) <;> simp_all

def replacing : Config := ⟨.sent, true⟩

/-- Without a refused access record ending its invite, the invite stays in
`recording` with a pending record that never retries: not listed, not
cancellable, never cleared. Only a stop for good ends it. -/
theorem refused_record_strands :
    ∃ w, trace recordStays replacing made
        (toWaiting ++ [.approve, .read, .commit, .demote, .read, .commit]) = some w ∧
      ∀ w', Steps recordStays replacing w w' → quiet recordStays w' = true → w'.stopped = true := by
  refine ⟨_, rfl, fun w' h q => ?_⟩
  rcases stays _ (stranded_record_stays replacing) h (.inr ⟨rfl, rfl, _, rfl⟩) with
    stopped | ⟨_, pending, _⟩
  · exact stopped
  · simp [quiet, pending] at q

def splitBinding : Rules := { Rules.correct with bindingWithInvite := false }

/-- When the binding ends in a transaction of its own, a crash before the
invite's next leaves the binding's outcome listed beside an approval still
under way. -/
theorem split_binding_disagrees :
    ∃ w, trace splitBinding sent made (toWaiting ++ [.approve, .read, .bind, .crash]) = some w ∧
      listed splitBinding w = some .approving ∧ w.outcome = some (some .landed) :=
  ⟨_, rfl, rfl, rfl⟩

def appliedApproval : Rules := { Rules.correct with approveChecksApplied := true }

/-- An approve step that checks the applied state refuses a request the app
was shown as waiting for its decision. -/
theorem demoted_approval_offered :
    ∃ w, trace appliedApproval sent made (toWaiting ++ [.demote]) = some w ∧
      offered (view appliedApproval w) .approve = true ∧ accepted appliedApproval w .approve = false :=
  ⟨_, rfl, rfl, rfl⟩

/-- Without the pass saving its finding only at the step it read, a request
missing when the pass looked is saved over the app's cancel. -/
theorem unguarded_finding_is_stale :
    ∃ w, trace { Rules.correct with guardRecord := false } sent made
        [.read, .commit, .read, .commit, .read, .cancel, .commit] = some w ∧
      w.pending = some .auto ∧ w.row.map (·.step) ≠ some w.pendingAt := ⟨_, rfl, rfl, by decide⟩

/-- An operation the app journaled that runs on a stopped device. -/
def StrandedOp (w : World) : Prop := w.stopped = true ∧ w.op = some none

def forgetfulStop : Rules := { Rules.correct with endOpsOnStop := false }

theorem stranded_op_stays (c : Config) (w : World) (a : Act) (h : StrandedOp w)
    (e : enabled forgetfulStop c w a = true) : StrandedOp (act forgetfulStop c w a) := by
  obtain ⟨stopped, op⟩ := h
  cases a <;> simp_all [StrandedOp, enabled, act, accepted, applying, forgetfulStop, Rules.correct]
  all_goals split <;> simp_all

/-- Without a stop ending the app's operations, one journaled before the stop
runs forever. -/
theorem forgetful_stop_runs_forever :
    ∃ w, trace forgetfulStop sent made [.administer, .stop] = some w ∧
      ∀ w', Steps forgetfulStop sent w w' → w'.op = some none := by
  refine ⟨_, rfl, fun w' h => (stays _ (stranded_op_stays sent) h ⟨rfl, rfl⟩).2⟩

/-- Without the invite recording its binding, an approval whose binding was
rejected in an attempt that then stopped journals a second binding. -/
theorem reclaim_binds_twice :
    ∃ w, trace { splitBinding with noReclaim := false } sent made
        (toWaiting ++ [.approve, .demote, .read, .bind, .crash, .read, .commit]) = some w ∧
      w.bindings = 2 := ⟨_, rfl, rfl⟩

/-- Without `coven_invites` showing an approval under way, the app is offered
approving again, which fails. -/
theorem approval_shown_as_waiting :
    ∃ w, trace { Rules.correct with showApproving := false } sent made
        (toWaiting ++ [.approve]) = some w ∧
      offered (view { Rules.correct with showApproving := false } w) .approve = true ∧
      accepted { Rules.correct with showApproving := false } w .approve = false :=
  ⟨_, rfl, rfl, rfl⟩

def unrecorded : Rules := { Rules.correct with recordShare := false }

/-- A share granted with no record in history, on a device stopped for good
before it ended the share. -/
def StrandedShare (w : World) : Prop :=
  w.shared = true ∧ w.recorded = false ∧ w.ended = false ∧ w.stopped = true ∧ w.bound = false

theorem stranded_share_stays (c : Config) (w : World) (a : Act) (h : StrandedShare w)
    (e : enabled unrecorded c w a = true) : StrandedShare (act unrecorded c w a) := by
  obtain ⟨shared, recorded, ended, stopped, bound⟩ := h
  cases a <;> simp_all [StrandedShare, enabled, act, accepted, applying, unrecorded, Rules.correct]
  all_goals (repeat' split) <;> simp_all

/-- Without the share recorded before the grant, a device that stops for
good with its invite's account shared leaves the share standing forever: no
other device knows of it, and none compares the provider's member list with
history, which would take away shares made outside coven. -/
theorem unrecorded_share_strands :
    ∃ w, trace unrecorded sent made [.read, .commit, .read, .commit, .stop] = some w ∧
      ∀ w', Steps unrecorded sent w w' → w'.shared = true := by
  refine ⟨_, rfl, fun w' h => ?_⟩
  exact (stays _ (stranded_share_stays sent) h ⟨rfl, rfl, rfl, rfl, rfl⟩).1

def unchecked : Rules := { Rules.correct with checkKeys := false }

def inPerson : Config := ⟨.inPerson, false⟩

/-- The invite as far as its request, whose keys a member already holds,
being read. -/
def toUsedRequest : List Act := [.read, .commit, .read, .commit, .send true, .read, .commit]

/-- Without the pass checking the request's keys when it opens the request, a
sent invite offers the app a request whose approval history can only refuse,
and an in-person invite's pass approves one and journals its binding; the
binding is refused only once it applies. -/
theorem unchecked_keys_approved :
    (∃ w, trace unchecked sent made toUsedRequest = some w ∧
      offered (view unchecked w) .approve = true ∧ w.used = true) ∧
    (∃ w, trace unchecked inPerson made (toUsedRequest ++ [.read, .commit]) = some w ∧
      w.bindings = 1 ∧ w.bound = false) :=
  ⟨⟨_, rfl, rfl, rfl⟩, ⟨_, rfl, rfl, rfl⟩⟩

/-! ## The properties, under the rules -/

/-- What every world reachable under `correct` satisfies. -/
structure Inv (c : Config) (w : World) : Prop where
  /-- A pending record holds what a pass found at the row's current step. -/
  fresh : w.pending.isSome → w.row.map (·.step) = some w.pendingAt
  /-- The row a pass read is the row now, or a later write changed the step. -/
  readRow : ∀ r, w.read = some r → ∃ r', w.row = some r' ∧ (r.step < r'.step ∨ r' = r)
  /-- The binding's operation runs exactly while its invite is approving. -/
  binding : w.binding = true ↔ w.row.map (·.stage) = some .approving
  /-- A sent invite's binding is listed as running exactly while it runs. -/
  running : w.outcome = some none ↔ (w.binding = true ∧ c.delivery = .sent)
  /-- An invite that can still end at its expiry has journaled no binding. -/
  early : (w.row.map (expires ·.stage)).getD false → w.bindings = 0
  /-- No binding journaled, none listed. -/
  zero : w.bindings = 0 → w.outcome = none ∧ w.binding = false
  once : w.bindings ≤ 1
  /-- Only a sent invite's request waits for the app. -/
  waitingSent : w.row.map (·.stage) = some (.waiting true) → c.delivery = .sent
  /-- A record that never retries is a refused check, which expires. -/
  never : w.pending = some .never → w.row.map (·.stage) = some .requested
  /-- A stopped device runs nothing of the app's and lists no invite; a pass
  it was running read an approval the stop ended. -/
  stopped : w.stopped = true → w.op ≠ some none ∧ w.outcome ≠ some none ∧
    w.binding = false ∧ (w.row.map (·.stage)).all (· == .settled) ∧
    ∀ r, w.read = some r → r.stage = .approving

theorem inv_made (c : Config) : Inv c made := by
  constructor <;> simp [made, expires]

theorem commitOf_stale (c : Config) (w : World) (r : Row) (stale : atRead w r = false) :
    commitOf Rules.correct c w r = w := by
  unfold commitOf
  split <;> simp_all [guarded, recordAt, Rules.correct] <;> (repeat' split) <;> simp_all

/-- A read made before a write that moves the row on is stale after it. -/
theorem read_advance {w : World} {n : Nat} {st : Stage}
    (hrow : w.row = some ⟨st, n⟩)
    (readRow : ∀ r, w.read = some r → ∃ r', w.row = some r' ∧ (r.step < r'.step ∨ r' = r)) :
    ∀ r, w.read = some r → r.step < n + 1 := by
  intro r hr
  obtain ⟨r', h, hs⟩ := readRow r hr
  rw [hrow] at h
  cases h
  rcases hs with hs | hs
  · simp at hs; omega
  · subst hs; simp

theorem advance_some {w : World} {r : Row} (hrow : w.row = some r) (s : Stage) :
    advance w s = { w with row := some ⟨s, r.step + 1⟩, pending := none, pendingAt := r.step + 1 } := by
  simp [advance, hrow]

theorem inv_lapse (c : Config) (w : World) (inv : Inv c w)
    (e : enabled Rules.correct c w .lapse = true) : Inv c (act Rules.correct c w .lapse) := by
  obtain ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never, stopped⟩ := inv
  rcases hr : w.read with _ | r
  · simp [enabled, hr] at e
  obtain ⟨r', hrow, hs⟩ := readRow r hr
  simp only [act, hr, guarded, atRead, hrow]
  split
  · rename_i cur
    have same : r' = r := by
      rcases hs with hs | hs
      · simp at cur; omega
      · exact hs
    subst same
    obtain ⟨stage, n⟩ := r'
    rw [advance_some hrow]
    constructor <;> simp_all [enabled, expires] <;> cases stage <;> simp_all [expires]
  · constructor <;> simp_all

set_option maxHeartbeats 2000000 in
theorem inv_commit (c : Config) (w : World) (inv : Inv c w)
    (e : enabled Rules.correct c w .commit = true) : Inv c (act Rules.correct c w .commit) := by
  obtain ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never, stopped⟩ := inv
  rcases hr : w.read with _ | r
  · simp [enabled, hr] at e
  obtain ⟨r', hrow, hs⟩ := readRow r hr
  by_cases cur : atRead w r = true
  · have same : r' = r := by
      rcases hs with hs | hs
      · simp [atRead, hrow] at cur; omega
      · exact hs
    subst same
    obtain ⟨stage, n⟩ := r'
    cases stage
    all_goals
      simp only [act, hr, commitOf, guarded, recordAt, atRead, hrow, Rules.correct, endBinding,
        bindingOutcome, afterBound, advance]
      (repeat' split) <;> constructor <;> simp_all [expires]
  · simp only [act, hr, commitOf_stale c w r (by simpa using cur)]
    constructor <;> simp_all


/-- The stop's transaction under `correct`: it ends the app's operations and
the invite. -/
def stopped' (w : World) : World :=
  { w with
    stopped := true
    approvable := false
    op := if w.op = some none then some (some .stopped) else w.op
    outcome := if w.outcome = some none then some (some .stopped) else w.outcome
    binding := false
    row := none
    pending := none
    read := none }

@[simp] theorem stopped'_row (w : World) : (stopped' w).row = none := rfl
@[simp] theorem stopped'_bindings (w : World) : (stopped' w).bindings = w.bindings := rfl
@[simp] theorem stopped'_outcome (w : World) : (stopped' w).outcome =
    if w.outcome = some none then some (some .stopped) else w.outcome := rfl

theorem stopEffect_correct (w : World) : stopEffect Rules.correct w = stopped' w := by
  simp [stopEffect, Rules.correct, stopped']

set_option maxHeartbeats 2000000 in
theorem inv_stop (c : Config) (w : World) (inv : Inv c w)
    (e : enabled Rules.correct c w .stop = true) : Inv c (act Rules.correct c w .stop) := by
  obtain ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never, stopped⟩ := inv
  obtain ⟨notStopped, reading⟩ : w.stopped = false ∧
      (w.read = none ∨ ∃ a, w.read = some a ∧ a.stage = .approving) := by
    simpa [enabled, applying] using e
  simp only [act, stopEffect_correct]
  constructor <;> simp_all [stopped'] <;> (repeat' split) <;> simp_all

set_option maxHeartbeats 2000000 in
theorem inv_step (c : Config) (w : World) (a : Act) (inv : Inv c w)
    (e : enabled Rules.correct c w a = true) : Inv c (act Rules.correct c w a) := by
  obtain ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never, stopped⟩ := inv
  cases a
  case crash => constructor <;> simp_all [act]
  case tick => constructor <;> simp_all [act]
  case send => constructor <;> simp_all [act]
  case gone => constructor <;> simp_all [act]
  case revoke => constructor <;> simp_all [act]
  case retry => constructor <;> simp_all [act]
  case ack =>
    constructor <;> simp_all [act, enabled, accepted]
    intro x hx hs hsent
    have := running.mpr ⟨⟨x, hx, hs⟩, hsent⟩
    simp_all
  case ackOp => exact ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never,
      fun h => by simp_all [act]⟩
  case administer => exact ⟨fresh, readRow, binding, running, early, zero, once, waitingSent,
      never, fun h => by simp_all [act, enabled, accepted]⟩
  case runOp => exact ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never,
      fun h => by simp_all [act, enabled]⟩
  case demote => exact ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never,
      stopped⟩
  case expiry =>
    obtain ⟨⟨stage, n⟩, hrow⟩ : ∃ r, w.row = some r := by
      cases h : w.row <;> simp_all [enabled]
    have := read_advance hrow readRow
    simp only [act, advance_some hrow]
    constructor <;> simp_all [enabled, expires] <;> cases stage <;> simp_all [expires]
  case decline =>
    obtain ⟨⟨stage, n⟩, hrow⟩ : ∃ r, w.row = some r := by
      cases h : w.row <;> simp_all [enabled, accepted]
    have := read_advance hrow readRow
    simp only [act, advance_some hrow]
    constructor <;> simp_all [enabled, accepted, expires]
  case cancel =>
    obtain ⟨⟨stage, n⟩, hrow⟩ : ∃ r, w.row = some r := by
      cases h : w.row <;> simp_all [enabled, accepted]
    have := read_advance hrow readRow
    simp only [act, advance_some hrow]
    constructor <;> simp_all [enabled, accepted, expires] <;> cases stage <;> simp_all [expires]
  case approve =>
    obtain ⟨⟨stage, n⟩, hrow⟩ : ∃ r, w.row = some r := by
      cases h : w.row <;> simp_all [enabled, accepted]
    have := read_advance hrow readRow
    simp only [act, advance_some hrow]
    constructor <;> simp_all [enabled, accepted, expires]
  case bind => simp [enabled, Rules.correct] at e
  case read =>
    constructor <;> simp_all [act, enabled]
  case fail p =>
    rcases hr : w.read with _ | r
    · simp [enabled, hr] at e
    obtain ⟨r', hrow, hs⟩ := readRow r hr
    simp only [act, hr, recordAt, Rules.correct, atRead, hrow]
    have : p ≠ .never := by simp_all [enabled]
    split
    · constructor <;> simp_all
    · constructor <;> simp_all
  case lapse => exact inv_lapse c w ⟨fresh, readRow, binding, running, early, zero, once,
      waitingSent, never, stopped⟩ e
  case commit => exact inv_commit c w ⟨fresh, readRow, binding, running, early, zero, once,
      waitingSent, never, stopped⟩ e
  case stop => exact inv_stop c w ⟨fresh, readRow, binding, running, early, zero, once,
      waitingSent, never, stopped⟩ e

theorem reachable_inv (c : Config) {w : World} (h : Reachable Rules.correct c w) : Inv c w :=
  stays (Inv c) (inv_step c) h (inv_made c)

/-! ### No share history doesn't record -/

/-- What every reachable world satisfies about the invite's share. -/
structure ShareInv (w : World) : Prop where
  /-- An invite that is granting recorded its share first. -/
  granting : w.row.map (·.stage) = some .grant → w.recorded = true
  /-- A standing share is recorded, so the owner's other devices know of it. -/
  recorded : w.shared = true → w.recorded = true
  /-- An invite gone from the journal ended its share, or its device stopped
  for good. -/
  over : w.row = none → w.ended = true ∨ w.stopped = true

set_option maxHeartbeats 2000000 in
theorem share_step (c : Config) (w : World) (a : Act) (inv : Inv c w) (sh : ShareInv w)
    (e : enabled Rules.correct c w a = true) : ShareInv (act Rules.correct c w a) := by
  obtain ⟨granting, recorded, over⟩ := sh
  obtain ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never, stopped⟩ := inv
  cases a
  case commit =>
    rcases hr : w.read with _ | r
    · simp [enabled, hr] at e
    obtain ⟨r', hrow, hs⟩ := readRow r hr
    by_cases cur : atRead w r = true
    · have same : r' = r := by
        rcases hs with hs | hs
        · simp [atRead, hrow] at cur; omega
        · exact hs
      subst same
      obtain ⟨stage, n⟩ := r'
      cases stage
      all_goals
        simp only [act, hr, commitOf, guarded, recordAt, atRead, hrow, Rules.correct, endBinding,
          bindingOutcome, afterBound, advance]
        (repeat' split) <;> constructor <;> simp_all
    · simp only [act, hr, commitOf_stale c w r (by simpa using cur)]
      exact ⟨granting, recorded, over⟩
  case lapse =>
    rcases hr : w.read with _ | r
    · simp [enabled, hr] at e
    simp only [act, hr, guarded]
    split
    · rcases hrow : w.row with _ | ⟨stage, n⟩
      · constructor <;> simp_all
      · constructor <;> simp_all [advance]
    · constructor <;> simp_all
  case stop =>
    simp only [act, stopEffect_correct]
    constructor <;> simp_all [stopped']
  case fail p =>
    rcases hr : w.read with _ | r <;> constructor <;> simp_all [act]
  case bind => simp [enabled, Rules.correct] at e
  case expiry | approve | decline | cancel =>
    rcases hrow : w.row with _ | ⟨stage, n⟩
    all_goals constructor <;> simp_all [act, enabled, accepted, advance]
  all_goals
    rcases hrow : w.row with _ | ⟨stage, n⟩
    all_goals constructor <;> simp_all [act, enabled, accepted]

theorem reachable_share (c : Config) {w : World} (h : Reachable Rules.correct c w) :
    Inv c w ∧ ShareInv w :=
  stays (fun w => Inv c w ∧ ShareInv w)
    (fun w a ⟨inv, sh⟩ e => ⟨inv_step c w a inv e, share_step c w a inv sh e⟩) h
    ⟨inv_made c, by constructor <;> simp [made]⟩

/-- A share standing at the provider is always one history records: the
pass records it before the grant, so the owner's other devices can take it
back once its device is gone for good (§13.2). -/
theorem share_recorded (c : Config) {w : World} (h : Reachable Rules.correct c w)
    (shared : w.shared = true) : w.recorded = true :=
  (reachable_share c h).2.recorded shared

/-! ### Only a checked request is offered or approved -/

/-- What every reachable world satisfies about the request's keys. -/
structure KeysInv (w : World) : Prop where
  /-- A request waiting for the app, or being approved, has fresh keys, and
  is the request in storage, or another admin's device has ended this
  device's requests. -/
  checked : (w.row.map (·.stage) = some (.waiting true) ∨ w.row.map (·.stage) = some .approving) →
    w.used = false ∧ (w.request = true ∨ w.gone = true)

set_option maxHeartbeats 2000000 in
theorem keys_step (c : Config) (w : World) (a : Act) (inv : Inv c w) (k : KeysInv w)
    (e : enabled Rules.correct c w a = true) : KeysInv (act Rules.correct c w a) := by
  obtain ⟨checked⟩ := k
  obtain ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never, stopped⟩ := inv
  cases a
  case commit =>
    rcases hr : w.read with _ | r
    · simp [enabled, hr] at e
    obtain ⟨r', hrow, hs⟩ := readRow r hr
    by_cases cur : atRead w r = true
    · have same : r' = r := by
        rcases hs with hs | hs
        · simp [atRead, hrow] at cur; omega
        · exact hs
      subst same
      obtain ⟨stage, n⟩ := r'
      cases stage
      all_goals
        simp only [act, hr, commitOf, guarded, recordAt, atRead, hrow, Rules.correct, endBinding,
          bindingOutcome, afterBound, advance]
        (repeat' split) <;> constructor <;> simp_all
    · simp only [act, hr, commitOf_stale c w r (by simpa using cur)]
      exact ⟨checked⟩
  case lapse =>
    rcases hr : w.read with _ | r
    · simp [enabled, hr] at e
    simp only [act, hr, guarded]
    split
    · rcases hrow : w.row with _ | ⟨stage, n⟩
      · constructor <;> simp_all
      · constructor <;> simp_all [advance]
    · constructor <;> simp_all
  case stop =>
    simp only [act, stopEffect_correct]
    constructor <;> simp_all [stopped']
  case fail p =>
    rcases hr : w.read with _ | r <;> constructor <;> simp_all [act]
  case bind => simp [enabled, Rules.correct] at e
  case send used =>
    constructor
    intro h
    have := checked (by simpa [act] using h)
    simp_all [enabled]
  case expiry | approve | decline | cancel =>
    rcases hrow : w.row with _ | ⟨stage, n⟩
    all_goals constructor <;> simp_all [act, enabled, accepted, advance]
  all_goals
    rcases hrow : w.row with _ | ⟨stage, n⟩
    all_goals constructor <;> simp_all [act, enabled, accepted]

theorem reachable_keys (c : Config) {w : World} (h : Reachable Rules.correct c w) :
    Inv c w ∧ KeysInv w :=
  stays (fun w => Inv c w ∧ KeysInv w)
    (fun w a ⟨inv, k⟩ e => ⟨inv_step c w a inv e, keys_step c w a inv k e⟩) h
    ⟨inv_made c, by constructor; simp [made]⟩

/-- The app is offered approving only a request whose keys are fresh: the
pass checked them when it opened the request (§13.2). -/
theorem offered_fresh (c : Config) {w : World} (h : Reachable Rules.correct c w)
    (o : offered (view Rules.correct w) .approve = true) : w.used = false := by
  refine ((reachable_keys c h).2.checked (.inl ?_)).1
  rcases hrow : w.row with _ | ⟨_ | _ | (_ | _) | _ | _ | _, n⟩ <;>
    simp_all [offered, view, listed, Rules.correct]

/-- An approval under way binds a request whose keys are fresh, so the
caught-up history refuses its binding only when this device may no longer
approve, never for the request's keys. -/
theorem binding_fresh (c : Config) {w : World} (h : Reachable Rules.correct c w)
    (b : w.binding = true) : w.used = false ∧ (bindingOutcome w = .rejected → w.approvable = false) := by
  obtain ⟨inv, k⟩ := reachable_keys c h
  have used := (k.checked (.inr (inv.binding.mp b))).1
  refine ⟨used, fun r => ?_⟩
  cases ha : w.approvable <;> simp_all [bindingOutcome]

/-! ### No write from a stale read -/

/-- A pending record always holds what a pass found at the row's current
step: a finding from a step the row has left is never saved. -/
theorem finding_current (c : Config) {w : World} (h : Reachable Rules.correct c w)
    (p : w.pending.isSome) : w.row.map (·.step) = some w.pendingAt :=
  (reachable_inv c h).fresh p

/-- A pass's write changes the row only when the row is the one it read. -/
theorem commit_acts_on_read (c : Config) {w : World} {r : Row} (h : Reachable Rules.correct c w)
    (hr : w.read = some r) (changed : (act Rules.correct c w .commit).row ≠ w.row) :
    w.row = some r := by
  obtain ⟨r', hrow, hs⟩ := (reachable_inv c h).readRow r hr
  by_cases cur : atRead w r = true
  · rcases hs with hs | hs
    · simp [atRead, hrow] at cur; omega
    · rw [hrow, hs]
  · simp [act, hr, commitOf_stale c w r (by simpa using cur)] at changed

/-! ### Every invite and operation ends -/

/-- What coven does on its own, and the app's retry of a record it is shown. -/
def coven : Act → Bool
  | .read | .crash | .commit | .lapse | .runOp | .tick | .expiry | .retry | .revoke => true
  | _ => false

inductive Moves (c : Config) : World → World → Prop
  | refl (w : World) : Moves c w w
  | step {w w' : World} (a : Act) (own : coven a = true) (e : enabled Rules.correct c w a = true)
      (rest : Moves c (act Rules.correct c w a) w') : Moves c w w'

theorem Moves.steps {c : Config} {w w' : World} (h : Moves c w w') : Steps Rules.correct c w w' := by
  induction h with
  | refl => exact .refl _
  | step a _ e _ ih => exact .step (.step _ a e) ih

theorem Moves.trans {c : Config} {a b d : World} (h : Moves c a b) (k : Moves c b d) :
    Moves c a d := by
  induction h with
  | refl => exact k
  | step x own e _ ih => exact .step x own e (ih k)

def stageMeasure : Option Row → Nat
  | none => 0
  | some ⟨.settled, _⟩ => 1
  | some ⟨.recording, _⟩ => 2
  | some ⟨.approving, _⟩ => 3
  | some _ => 4

/-- What is left to do. -/
def measure (w : World) : Nat :=
  4 * stageMeasure w.row + (!w.expired).toNat + w.read.isSome.toNat + (w.op == some none).toNat +
    w.pending.isSome.toNat + (w.shared && !w.bound).toNat

theorem one {c : Config} {w : World} (a : Act) (own : coven a = true)
    (e : enabled Rules.correct c w a = true) : Moves c w (act Rules.correct c w a) :=
  .step a own e (.refl _)

set_option maxHeartbeats 2000000 in
/-- From every reachable state that isn't quiet, coven alone, or the app
retrying a record it is shown, makes progress. -/
theorem progress (c : Config) (w : World) (inv : Inv c w) (sh : ShareInv w)
    (busy : quiet Rules.correct w = false) :
    ∃ w', Moves c w w' ∧ measure w' < measure w := by
  obtain ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never, stopped⟩ := inv
  obtain ⟨_, recorded, over⟩ := sh
  -- A share owed its revoke is taken back.
  by_cases owed : w.shared = true ∧ w.bound = false ∧
      (w.ended = true ∨ (w.recorded = true ∧ (w.stopped = true ∨ w.gone = true)))
  · obtain ⟨hs, hb, _⟩ := owed
    refine ⟨_, one .revoke rfl (by simp_all [enabled]), ?_⟩
    simp [act, measure, hs, hb]
  -- A read in flight is lost.
  rcases hread : w.read with _ | r
  case some =>
    exact ⟨_, one .crash rfl rfl, by simp [act, measure, hread]⟩
  -- The app's operation runs.
  by_cases opRunning : w.op = some none
  · by_cases st : w.stopped = true
    · exact absurd opRunning (stopped st).1
    · exact ⟨_, one .runOp rfl (by simp_all [enabled]), by
        simp [act, measure, opRunning]⟩
  -- A record that can be retried is retried.
  by_cases retryable : w.pending = some .auto ∨ w.pending = some .app
  · obtain ⟨r, hrow⟩ : ∃ r, w.row = some r := by
      cases h : w.row <;> rcases retryable with p | p <;> simp_all
    exact ⟨_, one .retry rfl (by rcases retryable with p | p <;> simp_all [enabled, accepted]),
      by rcases retryable with p | p <;> simp [act, measure, p]⟩
  have settledOrNever : w.pending = none ∨ w.pending = some .never := by
    rcases h : w.pending with _ | (_ | _ | _) <;> simp_all
  rcases hrow : w.row with _ | ⟨stage, n⟩
  · rcases settledOrNever with p | p <;> simp_all [quiet, listed]
  by_cases st : w.stopped = true
  · obtain ⟨op, out, _, settled, _⟩ := stopped st
    rcases settledOrNever with p | p
    · cases stage <;> simp_all [quiet, listed]
    · cases stage <;> simp_all
  have st : w.stopped = false := by simpa using st
  -- Stages that expire: the clock reaches the expiry, and the pass ends them.
  have expiring : expires stage = true → ∃ w', Moves c w w' ∧ measure w' < measure w := by
    intro ex
    by_cases tick : w.expired = true
    · let w₁ := act Rules.correct c w .read
      have e₁ : enabled Rules.correct c w .read = true := by simp_all [enabled]
      have e₂ : enabled Rules.correct c w₁ .lapse = true := by simp_all [w₁, enabled, act]
      refine ⟨_, .step .read rfl e₁ (one .lapse rfl e₂), ?_⟩
      cases stage <;> simp_all [w₁, act, measure, guarded, atRead, advance, stageMeasure, expires]
        <;> omega
    · exact ⟨_, one .tick rfl (by simp_all [enabled]), by simp_all [act, measure]⟩
  -- Stages a pass moves on: read, then write.
  have passing : (stage = .approving ∨ stage = .recording ∨ stage = .settled) →
      ∃ w', Moves c w w' ∧ measure w' < measure w := by
    intro staged
    rcases settledOrNever with p | p
    · let w₁ := act Rules.correct c w .read
      have e₁ : enabled Rules.correct c w .read = true := by simp_all [enabled, ready]
      have e₂ : enabled Rules.correct c w₁ .commit = true := by simp_all [w₁, enabled, act]
      refine ⟨_, .step .read rfl e₁ (one .commit rfl e₂), ?_⟩
      rcases staged with h | h | h <;> subst h <;> cases ha : w.approvable <;>
        cases hu : w.used <;> cases hc : c.records <;>
        simp_all [w₁, act, measure, commitOf, guarded, atRead, advance, stageMeasure, Rules.correct,
          endBinding, bindingOutcome, afterBound, beq_iff_eq] <;> omega
    · rcases staged with h | h | h <;> simp_all
  cases stage
  case requested => exact expiring rfl
  case grant => exact expiring rfl
  case waiting => exact expiring rfl
  case approving => exact passing (.inl rfl)
  case recording => exact passing (.inr (.inl rfl))
  case settled => exact passing (.inr (.inr rfl))

/-- Every invite and every operation the app's writes journaled ends, on
every path: from any reachable state, coven alone, with the app retrying
only records it is shown, reaches a state with nothing listed, running or
pending. That includes a device stopped for good, and crashes anywhere. -/
theorem settles (c : Config) {w : World} (h : Reachable Rules.correct c w) :
    ∃ w', Moves c w w' ∧ quiet Rules.correct w' = true := by
  generalize hm : measure w = m
  induction m using Nat.strongRecOn generalizing w with
  | ind m ih =>
    by_cases q : quiet Rules.correct w = true
    · exact ⟨w, .refl w, q⟩
    · obtain ⟨inv, sh⟩ := reachable_share c h
      obtain ⟨w', moves, less⟩ := progress c w inv sh (by simpa using q)
      obtain ⟨w'', more, quiet⟩ := ih (measure w') (hm ▸ less) (h.trans moves.steps) rfl
      exact ⟨w'', moves.trans more, quiet⟩

/-! ### What the app sees never goes back -/

/-- How far the invite has gone, as its listing shows it. -/
def stageRank : Option Row → Nat
  | some ⟨.requested, _⟩ | some ⟨.grant, _⟩ => 0
  | some ⟨.waiting false, _⟩ => 1
  | some ⟨.waiting true, _⟩ => 2
  | some ⟨.approving, _⟩ => 3
  | _ => 4

theorem listed_rank (w : World) : Listed.rank (listed Rules.correct w) = stageRank w.row := by
  rcases h : w.row with _ | ⟨_ | _ | (_ | _) | _ | _ | _, n⟩ <;>
    simp [listed, h, Listed.rank, stageRank, Rules.correct]

/-- How far the binding's operation has gone: not journaled, running, ended,
acknowledged. -/
def bindingRank (w : World) : Nat := if w.bindings = 0 then 0 else 1 + entryRank w.outcome

set_option maxHeartbeats 2000000 in
theorem commit_forward (c : Config) (w : World) (inv : Inv c w)
    (e : enabled Rules.correct c w .commit = true) :
    stageRank w.row ≤ stageRank (act Rules.correct c w .commit).row ∧
      bindingRank w ≤ bindingRank (act Rules.correct c w .commit) := by
  obtain ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never, stopped⟩ := inv
  rcases hr : w.read with _ | r
  · simp [enabled, hr] at e
  obtain ⟨r', hrow, hs⟩ := readRow r hr
  by_cases cur : atRead w r = true
  · have same : r' = r := by
      rcases hs with hs | hs
      · simp [atRead, hrow] at cur; omega
      · exact hs
    subst same
    obtain ⟨stage, n⟩ := r'
    rcases stage with _ | _ | (_ | _) | _ | _ | _
    all_goals
      simp only [act, hr, commitOf, guarded, recordAt, atRead, hrow, Rules.correct, endBinding,
        bindingOutcome, afterBound, advance]
      (repeat' split) <;> simp_all [stageRank, bindingRank, entryRank]
    all_goals (first | omega | (have := early (by simp [expires]); simp_all))
  · simp [act, hr, commitOf_stale c w r (by simpa using cur), bindingRank]

set_option maxHeartbeats 2000000 in
theorem lapse_forward (c : Config) (w : World)
    (e : enabled Rules.correct c w .lapse = true) :
    stageRank w.row ≤ stageRank (act Rules.correct c w .lapse).row ∧
      bindingRank w ≤ bindingRank (act Rules.correct c w .lapse) := by
  rcases hr : w.read with _ | r
  · simp [enabled, hr] at e
  simp only [act, hr, guarded]
  split
  · rcases hrow : w.row with _ | ⟨stage, n⟩
    · simp [advance, hrow, bindingRank]
    · rcases stage with _ | _ | (_ | _) | _ | _ | _ <;> simp [advance, hrow, stageRank, bindingRank] <;> (repeat' split) <;> simp_all
  · simp [bindingRank]

set_option maxHeartbeats 2000000 in
theorem stop_forward (c : Config) (w : World) :
    stageRank w.row ≤ stageRank (act Rules.correct c w .stop).row ∧
      bindingRank w ≤ bindingRank (act Rules.correct c w .stop) := by
  simp only [act, stopEffect_correct]
  rcases hrow : w.row with _ | ⟨_ | _ | (_ | _) | _ | _ | _, n⟩ <;>
    simp [stopped', stageRank, bindingRank, entryRank] <;> (repeat' split) <;> simp_all

set_option maxHeartbeats 2000000 in
/-- Every step moves the listed invite and its binding's operation forward,
never back: granting, waiting for a request, waiting for the app, approving,
gone; and running, ended, acknowledged. -/
theorem forward (c : Config) (w : World) (a : Act) (inv : Inv c w)
    (e : enabled Rules.correct c w a = true) :
    Listed.rank (listed Rules.correct w) ≤ Listed.rank (listed Rules.correct (act Rules.correct c w a)) ∧
      bindingRank w ≤ bindingRank (act Rules.correct c w a) := by
  rw [listed_rank, listed_rank]
  cases a
  case commit => exact commit_forward c w inv e
  case lapse => exact lapse_forward c w e
  case stop => exact stop_forward c w
  case bind => simp [enabled, Rules.correct] at e
  case fail p =>
    rcases hr : w.read with _ | r <;> simp [act, hr, bindingRank]
  all_goals
    obtain ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never, stopped⟩ := inv
    rcases hrow : w.row with _ | ⟨_ | _ | (_ | _) | _ | _ | _, n⟩
    all_goals simp_all [act, enabled, accepted, advance, stageRank, bindingRank, entryRank, expires]
    all_goals (repeat' split) <;> simp_all <;> omega

/-! ### The app is offered exactly what the steps accept -/

/-- From what `coven_invites`, `coven_operations` and `coven_pending` show,
the app's offer of approving, declining, cancelling and acknowledging is
exactly what the write steps accept, and every record it is offered to retry
can be retried. -/
theorem offered_accepted (c : Config) (w : World) (inv : Inv c w) :
    (∀ a, a ∈ [Act.approve, .decline, .cancel, .ack, .ackOp] →
      offered (view Rules.correct w) a = accepted Rules.correct w a) ∧
    (offered (view Rules.correct w) .retry = true → accepted Rules.correct w .retry = true) := by
  obtain ⟨fresh, readRow, binding, running, early, zero, once, waitingSent, never, stopped⟩ := inv
  constructor
  · intro a ha
    simp only [List.mem_cons, List.not_mem_nil, or_false] at ha
    rcases hrow : w.row with _ | ⟨_ | _ | (_ | _) | _ | _ | _, n⟩
    all_goals rcases ha with rfl | rfl | rfl | rfl | rfl <;> rcases hx : w.expired <;>
      simp [offered, accepted, view, listed, hrow, hx, Rules.correct, expires] <;> decide
  · intro h
    obtain ⟨r, hrow⟩ : ∃ r, w.row = some r := by
      cases hr : w.row <;> simp_all [offered, view]
    rcases h' : w.pending with _ | (_ | _ | _) <;> simp_all [offered, accepted, view]

/-- A sent invite listed as being approved is exactly one whose binding is
listed as running: no crash between transactions shows one without the other. -/
theorem approval_runs_binding (c : Config) (w : World) (inv : Inv c w) (sent : c.delivery = .sent) :
    listed Rules.correct w = some .approving ↔ w.outcome = some none := by
  rw [inv.running, inv.binding]
  rcases hrow : w.row with _ | ⟨_ | _ | (_ | _) | _ | _ | _, n⟩
  all_goals simp [listed, hrow, Rules.correct, sent]

end CovenSequence.InviteLifecycle
