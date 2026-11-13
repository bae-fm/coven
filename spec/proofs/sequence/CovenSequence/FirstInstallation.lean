/-!
# A join, a restore and a pairing of one store on one device

A device keeps one unfinished first installation of a store, at the store's
path, whichever call made it (§13.4): a join (§13.2), a restore (§13.1) or a
pairing (§13.2, `join_with_pairing`), which is a restore from its record on.
Each call keeps what lists it to resume. A join keeps its invite code from
before its request may be sent; a restore keeps its record from before its
registration may be sent. Each call removes the installation, and everything
kept beside it, when it ends without its store: a join cancelled before it
may have sent its request, or declined; a restore cancelled or failed before
its record.

A call begins on whatever installation is there, so without a rule a restore
or a pairing may begin on a waiting join's installation, or a join on a
recorded restore's. Then one call's removal takes the other's listing with it:
storage may hold the other's request or registration, and nothing on the
device lists it to resume. `Rules.exclusive`: a restore refuses an
installation holding an invite code, a pairing one holding an invite code or a
restore's record, and a join one holding a restore's record, changing nothing.
Without it, each removal is a checked counterexample.

A pairing keeps nothing on the device until its restore records itself: the
request it sends binds nothing, since approval writes nothing in history. It
can be cancelled at any point before the record, and it removes its
installation as a restore's cancel does. From the record on it is a restore.

A join accepts the provider's share before it sends its request, and on
Dropbox the accepted folder sits in the joining account. A join cancelled
before sending takes back the acceptance it records, then removes its
installation. `Rules.recordsAcceptance`: the join records that it is
accepting before it accepts. Without it a cancel removes the join and
leaves the acceptance, with nothing on the device to take it back
(`cancel_strands_acceptance`). A declined join's acceptance ends with the
invite, which the inviting device takes back; an approved one's becomes the
store's access.

A join's approval is one admission, which also registers its device (§13.2).
Once the join finds it, the join has met its outcome: there is nothing left
for it to claim. `Rules.publishesAtAdmission`: the join publishes the store
in the step that finds its admission, and the store loads after. Then a join
whose admission is found is a published store, never a listed join
(`found_published`), and a store whose storage can't be reached shows it as
any store does. Publishing only once the store has loaded, a call that stops
while it loads, its storage access lost say, leaves a join listed that has
nothing left to do (`loaded_first_lists_admitted`). The custody commit between
finding and publishing only goes forward (`BootstrapCommit`); this model takes
it as part of the step.

The join finds its admission by its copies, before any history it applied
could check that admission; the store's first load applies it. A refused one,
which only a faulty or hostile writer lands, leaves the store's device never a
member. `Rules.stopsRefused`: such a load stops the store for good, which the
app shows and removes. Then every loaded store from a join is a member
(`refused_stops`); without it the store syncs on unregistered, with no step
of coven's own or the app's left that would end it (`refused_syncs_forever`).
Past its admission, the other facts, a removal or a replacement of its
member's keys, stop the store as any store, outside this model.

A join reads the account it is signed in as before it keeps a sign-in or
accepts the share, and compares it with the account the invite was shared with
(`Act.checkAccount`). A different account keeps nothing and accepts nothing:
the join stays listed, unsent and cancellable (`wrong_account_keeps_join`).
Until the account is proven, a refusal of the first acceptance is one
observation for two causes, an ended invite and a wrong account, so a join
that ends on it ends some wrong account's join, and one that keeps it keeps
some ended invite's (`unchecked_refusal_misjudged`). Once it is proven, the
location's refusal can only mean the access is gone, and the join ends as a
declined one does (`Act.lostAccess`, `lost_access_after_proof`).

The join's request carries the new person's name and circle, which the
admission records in the member's state in history (`JoiningWrite`). The app
registers a joining write, which runs in the step that first applies the
device's registration: the load that applies the admission, or the install of
a snapshot that covers it. `Rules.writesAtRegistration`: the joining write
commits with the registration. Then every world whose loaded history registers
the device has the profile committed (`profile_with_registration`). Without it,
the app records the profile in a write of its own after the load, and a
registered member can exist without it for as long as the app has not yet
written, or never does (`acknowledged_profile_lags`).
-/
namespace CovenSequence.FirstInstallation

inductive Inst where
  /-- Removed, or never made. -/
  | none
  /-- Unpublished: hidden from the store listing. -/
  | hidden
  | published
  deriving DecidableEq, Repr

/-- The call running on the installation. -/
inductive Call where
  | none
  | join
  | restore
  /-- A pairing on the new device, before its restore's record. -/
  | pair
  deriving DecidableEq, Repr

structure World where
  inst : Inst
  call : Call
  /-- A join's invite code, kept on the device: it lists the join. -/
  code : Bool
  /-- The join's request may be in storage, and its outcome is not met. -/
  sent : Bool
  /-- A restore's record, kept on the device: it lists the restore. -/
  record : Bool
  /-- The restore's registration may be in storage, and its outcome is not
  met. -/
  registered : Bool
  /-- The provider's acceptance of the join's share may be in the joining
  account. -/
  accepted : Bool
  /-- The join's saved request records that it is accepting the share. -/
  acceptKept : Bool
  /-- The join found its admission: its copies opened. -/
  found : Bool
  /-- The store's first load reached the end of the sequence, its device a
  member or not. -/
  loaded : Bool
  /-- The history the store loaded counts its admission. -/
  member : Bool
  /-- The store stopped for good. -/
  stopped : Bool
  /-- The person's profile is committed in the store. -/
  profiled : Bool
  /-- The join has proven the account it is signed in as is the one the
  invite was shared with. -/
  proven : Bool
  deriving DecidableEq, Repr

def start : World :=
  ⟨.none, .none, false, false, false, false, false, false, false, false, false, false, false,
    false⟩

structure Rules where
  /-- A restore refuses an installation holding an invite code, a pairing one
  holding an invite code or a restore's record, and a join one holding a
  restore's record. -/
  exclusive : Bool
  /-- A join records that it is accepting the share before it accepts. -/
  recordsAcceptance : Bool
  /-- A join publishes the store in the step that finds its admission. -/
  publishesAtAdmission : Bool
  /-- A store whose first load leaves its device never a member stops for
  good. -/
  stopsRefused : Bool
  /-- The joining write commits in the step that first applies the device's
  registration. -/
  writesAtRegistration : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true, true, true, true, true⟩

/-- Remove the installation and everything kept beside it, as either call
does when it ends without its store. What may be in storage stays. -/
def undo (w : World) : World :=
  { w with inst := .none, call := .none, code := false, record := false }

inductive Act where
  /-- A join begins on the store's installation, making one if none is
  there, and keeps its code. -/
  | beginJoin
  /-- The join records, in its saved request, that it is accepting. -/
  | keepAcceptance
  /-- The provider accepts the join's share for the joining account. -/
  | accept
  /-- The join sends its request. -/
  | send
  /-- The join is cancelled before it may have sent its request: it takes
  back the acceptance it records, then removes the installation. -/
  | joinCancel
  /-- The join reads the account it is signed in as and compares it with the
  invite's. The right account is proven; a wrong one changes nothing. -/
  | checkAccount (right : Bool)
  /-- The join's request is declined: it ends without its store. -/
  | declined
  /-- The shared location refuses the join whose account is proven: its access
  is gone, and it ends without its store. -/
  | lostAccess
  /-- The join finds its admission. -/
  | found
  /-- The join, its admission found, publishes the store once it has loaded
  it, where it doesn't publish as it finds the admission. -/
  | approved
  /-- The published store's first load reaches the end of the sequence, its
  history counting the admission or not. -/
  | load (counts : Bool)
  /-- A restore begins on the store's installation, making one if none is
  there. -/
  | beginRestore
  /-- A pairing begins on the store's installation, making one if none is
  there. -/
  | beginPair
  /-- The pairing's restore records itself: from here the call is a restore. -/
  | pairRecord
  /-- The pairing is cancelled, or fails or ends, before its record. -/
  | pairUndo
  /-- The restore records itself. -/
  | record
  /-- The restore sends its registration. -/
  | register
  /-- The restore is cancelled, or fails, before its record. -/
  | restoreUndo
  /-- The restore's registration ended: it publishes the store. -/
  | restored
  /-- The app records the person's profile in a write of its own, after
  the store was published. -/
  | appWrites
  /-- The app drops the running call, or the device crashes. -/
  | drop
  deriving DecidableEq, Repr

def enabled (R : Rules) (w : World) : Act → Bool
  | .beginJoin => w.call == .none && w.inst != .published && (!R.exclusive || !w.record)
  | .checkAccount _ => w.call == .join
  | .keepAcceptance => w.call == .join && w.proven && !w.sent
  | .accept => w.call == .join && w.proven && !w.sent && (!R.recordsAcceptance || w.acceptKept)
  | .send => w.call == .join && w.proven
  | .lostAccess => w.call == .join && w.proven
  | .joinCancel => w.call == .join && !w.sent
  | .declined | .found => w.call == .join && w.sent && !w.found
  | .approved => w.call == .join && w.found && !R.publishesAtAdmission
  | .load _ => w.inst == .published && w.found && !w.loaded && !w.stopped
  | .beginRestore => w.call == .none && w.inst != .published && (!R.exclusive || !w.code)
  | .beginPair =>
    w.call == .none && w.inst != .published && (!R.exclusive || (!w.code && !w.record))
  | .pairRecord | .pairUndo => w.call == .pair
  | .record => w.call == .restore
  | .register => w.call == .restore && w.record
  | .restoreUndo => w.call == .restore && !w.record
  | .restored => w.call == .restore && w.registered
  | .appWrites => w.inst == .published && w.found
  | .drop => w.call != .none

/-- Publish a join's store: its code, request and acceptance become the
store's, and the store keeps the person's name the request carried where the
rules say. -/
def publish (R : Rules) (w : World) : World :=
  { w with inst := .published, call := .none, code := false, sent := false, accepted := false,
           acceptKept := false, proven := false }

def act (R : Rules) (w : World) : Act → World
  | .beginJoin => { w with inst := .hidden, call := .join, code := true }
  | .keepAcceptance => { w with acceptKept := true }
  | .accept => { w with accepted := true }
  | .send => { w with sent := true }
  | .checkAccount right => { w with proven := w.proven || right }
  | .joinCancel =>
    { undo w with accepted := w.accepted && !w.acceptKept, acceptKept := false, proven := false }
  | .declined | .lostAccess =>
    { undo w with sent := false, accepted := false, acceptKept := false, proven := false }
  | .found =>
    if R.publishesAtAdmission then publish R { w with found := true } else { w with found := true }
  | .approved => publish R w
  | .load counts =>
    if counts then { w with loaded := true, member := true, profiled := R.writesAtRegistration }
    else if R.stopsRefused then { w with stopped := true }
    else { w with loaded := true }
  | .beginRestore => { w with inst := .hidden, call := .restore }
  | .beginPair => { w with inst := .hidden, call := .pair }
  | .pairRecord => { w with call := .restore, record := true }
  | .pairUndo => undo w
  | .record => { w with record := true }
  | .register => { w with registered := true }
  | .restoreUndo => undo w
  | .restored =>
    { w with inst := .published, call := .none, record := false, registered := false }
  | .appWrites => { w with profiled := true }
  | .drop => { w with call := .none }

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' : World} (a : Act) : enabled R w a = true → Steps R (act R w a) w' → Steps R w w'

def Reachable (R : Rules) (w : World) : Prop := Steps R start w

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act R w a) rest else none

/-- Storage may hold a call's request or registration whose outcome this
device hasn't met, or the joining account a share's acceptance, and nothing
on the device lists the call to resume or take it back: only typing its code
again would. -/
def Stranded (w : World) : Prop :=
  (w.sent = true ∧ w.code = false) ∨ (w.registered = true ∧ w.record = false) ∨
    (w.accepted = true ∧ w.code = false)

/-! ## Counterexamples: one call's removal takes the other's listing -/

/-- Dan's phone sends its join request for Household, and the app drops the
join while it waits. Dan then scans a pairing code for Household on the
phone, which begins on the join's installation, and cancels before the
pairing's restore records itself. The cancel removes the installation and the
join's code with it: Ben may approve Dan's request, and no join on the phone
is listed to finish it. -/
theorem pairing_strands_join :
    ∃ w, trace ⟨false, true, true, true, true⟩ start
        [.beginJoin, .checkAccount true, .send, .drop, .beginPair, .pairUndo] = some w ∧ Stranded w := by
  exact ⟨_, rfl, by simp [Stranded, act, undo, start]⟩


/-- Dan's phone sends its join request for Household, and the app drops the
join while it waits. Someone then restores Household on the phone from a
restore code, which begins on the join's installation, and cancels before
the restore records itself. The cancel removes the installation and the
join's code with it: Ben may approve Dan's request, and no join on the
phone is listed to finish it. -/
theorem restore_strands_join :
    ∃ w, trace ⟨false, true, true, true, true⟩ start [.beginJoin, .checkAccount true, .send, .drop, .beginRestore, .restoreUndo] = some w ∧
      Stranded w := by
  exact ⟨_, rfl, by simp [Stranded, act, undo, start]⟩

/-- Ana's laptop records its restore of Household and sends its
registration, and the app drops the call. A pairing for Household then begins
on the restore's installation and is cancelled before its own record. The
cancel removes the installation and the restore's record with it: the
registration may land, and no restore on the laptop is listed to finish
it. -/
theorem pairing_strands_restore :
    ∃ w, trace ⟨false, true, true, true, true⟩ start
        [.beginRestore, .record, .register, .drop, .beginPair, .pairUndo] = some w ∧
      Stranded w := by
  exact ⟨_, rfl, by simp [Stranded, act, undo, start]⟩

/-- Ana's laptop records its restore of Household and sends its
registration, and the app drops the call. A join with an invite to
Household then begins on the restore's installation and is cancelled
before it sends its request. The cancel removes the installation and the
restore's record with it: the registration may land, and no restore on the
laptop is listed to finish it. -/
theorem join_strands_restore :
    ∃ w, trace ⟨false, true, true, true, true⟩ start [.beginRestore, .record, .register, .drop, .beginJoin, .joinCancel] =
        some w ∧ Stranded w := by
  exact ⟨_, rfl, by simp [Stranded, act, undo, start]⟩

/-- Dan's phone mounts Household in his Dropbox, recording nothing, and the
app closes before sending his request. Resumed, the join is cancelled: it
removes the installation and the code, and Household stays mounted in Dan's
Dropbox with no join listed to take it out. -/
theorem cancel_strands_acceptance :
    ∃ w, trace ⟨true, false, true, true, true⟩ start [.beginJoin, .checkAccount true, .accept, .drop, .beginJoin, .joinCancel] =
        some w ∧ Stranded w := by
  exact ⟨_, rfl, by simp [Stranded, act, undo, start]⟩

/-! ## Under the rules -/

/-- Dan's phone finds its admission and records it, loading before it
publishes, and the call stops while it loads: Dan's storage access was taken
away. The join stays listed, though nothing is left for it to claim. -/
theorem loaded_first_lists_admitted :
    ∃ w, trace ⟨true, true, false, true, true⟩ start [.beginJoin, .checkAccount true, .send, .found, .drop] = some w ∧
      w.found = true ∧ w.code = true := by
  exact ⟨_, rfl, rfl, rfl⟩

/-- Dan's phone publishes Household as it finds its admission, but the
history its first load applies refused that admission, and the store doesn't
stop. Its device never a member, it sends nothing, and no step is left that
would end it: coven has none, and the app's only one records Dan's profile. -/
theorem refused_syncs_forever :
    ∃ w, trace ⟨true, true, true, false, true⟩ start [.beginJoin, .checkAccount true, .send, .found, .load false] =
        some w ∧ w.loaded = true ∧ w.member = false ∧ w.stopped = false ∧
      ∀ a, a ≠ .appWrites → enabled ⟨true, true, true, false, true⟩ w a = false := by
  refine ⟨_, rfl, rfl, rfl, rfl, ?_⟩
  intro a h
  cases a <;> first | rfl | exact absurd rfl h

/-- Dan's phone finds its admission and publishes Household, and its first
load applies the admission and registers his device. The app writes Dan's
profile in a write of its own afterwards, so between the load and that write,
and for good if the app quits first, a registered member has no profile. -/
theorem acknowledged_profile_lags :
    ∃ w, trace ⟨true, true, true, true, false⟩ start
        [.beginJoin, .checkAccount true, .send, .found, .load true] = some w ∧
      w.member = true ∧ w.profiled = false := by
  exact ⟨_, rfl, rfl, rfl⟩

/-! ## Counterexample: one observation for two causes -/

/-- Why the shared location refused a join's first acceptance. -/
inductive Cause where
  | endedInvite
  | wrongAccount
  deriving DecidableEq, Repr

/-- What a join does with the refusal. -/
inductive Reply where
  | ends
  | keeps
  deriving DecidableEq, Repr

/-- The right reply to each cause: an ended invite ends the join; a wrong
account keeps it, to be tried again with the right sign-in. -/
def right : Cause → Reply
  | .endedInvite => .ends
  | .wrongAccount => .keeps

/-- **Without the account check, a refusal is misjudged.** A join that has not
compared accounts sees the same refusal for both causes, so whichever reply
its one rule gives is wrong for one of them: ending strands a wrong account's
join as ended, keeping leaves an ended invite's join waiting for ever. -/
theorem unchecked_refusal_misjudged (rule : Reply) : ∃ cause, rule ≠ right cause := by
  cases rule
  · exact ⟨.wrongAccount, by decide⟩
  · exact ⟨.endedInvite, by decide⟩

/-! ## Under the rules -/

structure Inv (w : World) : Prop where
  sent : w.sent = true → w.code = true
  registered : w.registered = true → w.record = true
  /-- At most one call's listing is kept. -/
  one : ¬(w.code = true ∧ w.record = true)
  join : w.call = .join → w.code = true ∧ w.record = false
  restore : w.call = .restore → w.code = false
  /-- A pairing keeps nothing on the device, so nothing lists it. -/
  pair : w.call = .pair → w.code = false ∧ w.record = false
  /-- An acceptance is kept by a listed join that records it, or has sent. -/
  accepted : w.accepted = true → w.code = true ∧ (w.acceptKept = true ∨ w.sent = true)
  /-- A join that found its admission is a published store, with no call on
  it. -/
  found : w.found = true → w.inst = .published ∧ w.code = false ∧ w.call = .none
  /-- A store whose first load reached the end is a member. -/
  loaded : w.loaded = true → w.member = true
  /-- A join that sent, accepted or kept that it is accepting has proven its
  account. -/
  proven : (w.sent = true ∨ w.accepted = true ∨ w.acceptKept = true) → w.proven = true
  /-- A store whose first load registered its device has the person's profile
  committed. -/
  profile : w.member = true → w.profiled = true

theorem inv_start : Inv start := by
  constructor <;> simp [start]

set_option maxHeartbeats 1600000 in
theorem inv_step (w : World) (a : Act) (inv : Inv w) (e : enabled Rules.correct w a = true) :
    Inv (act Rules.correct w a) := by
  obtain ⟨sent, registered, one, join, restore, pair, accepted, found, loaded, proven, profile⟩ := inv
  cases a <;> rcases hc : w.call <;> constructor <;>
    (try simp_all [act, enabled, Rules.correct, undo, publish]) <;>
    (try split) <;> (try simp_all)

theorem steps_keep_inv : ∀ {w w'}, Steps Rules.correct w w' → Inv w → Inv w' := by
  intro w w' h
  induction h with
  | refl => exact id
  | step a e _ ih => exact fun inv => ih (inv_step _ a inv e)

/-- **Past its admission, a join is a store.** Whatever the app and the device
do, a join that found its admission is a published store: nothing lists it as
a join, and no call runs on it as one. -/
theorem found_published {w : World} (h : Reachable Rules.correct w) (found : w.found = true) :
    w.inst = .published ∧ w.code = false ∧ w.call = .none :=
  (steps_keep_inv h inv_start).found found

/-- **A refused admission stops the store.** Every store a join published
whose first load reached the end of the sequence counts its device a member:
one whose history refused the admission stopped instead. -/
theorem refused_stops {w : World} (h : Reachable Rules.correct w) (loaded : w.loaded = true) :
    w.member = true :=
  (steps_keep_inv h inv_start).loaded loaded

/-- **A wrong account keeps the join.** A join that has not proven its account
has sent nothing and accepted nothing, and is still listed and cancellable:
signing in as another account changes none of that. -/
theorem wrong_account_keeps_join {w : World} (h : Reachable Rules.correct w)
    (c : w.call = .join) (wrong : w.proven = false) :
    w.sent = false ∧ w.accepted = false ∧ w.acceptKept = false ∧ w.code = true ∧
      enabled Rules.correct w .joinCancel = true := by
  have inv := steps_keep_inv h inv_start
  have none : ¬(w.sent = true ∨ w.accepted = true ∨ w.acceptKept = true) :=
    fun held => by simp [inv.proven held] at wrong
  simp only [not_or, Bool.not_eq_true] at none
  obtain ⟨s, a, k⟩ := none
  refine ⟨s, a, k, (inv.join c).1, ?_⟩
  simp [enabled, c, s]

/-- The wrong account is checked, not accepted: the join signs in as another
account, and the account it signed in as before still lists it, unsent and
unaccepted. -/
theorem wrong_account_trace :
    ∃ w, trace Rules.correct start [.beginJoin, .checkAccount false] = some w ∧
      w.code = true ∧ w.accepted = false ∧
      enabled Rules.correct w .accept = false ∧ enabled Rules.correct w .send = false ∧
      enabled Rules.correct w .joinCancel = true := by
  exact ⟨_, rfl, rfl, rfl, rfl, rfl, rfl⟩

/-- A refusal ends a join only on a proven account: the location's refusal
is read as lost access only after the account matched. -/
theorem lost_access_after_proof (R : Rules) (w : World)
    (e : enabled R w .lostAccess = true) : w.proven = true := by
  simp_all [enabled]

/-- **Lost access ends the join.** A join whose account is proven that loses
its access ends as a declined one: no installation, code, request or
acceptance is left, so nothing is stranded. -/
theorem lost_access_leaves_nothing {w : World} (h : Reachable Rules.correct w)
    (e : enabled Rules.correct w .lostAccess = true) :
    (act Rules.correct w .lostAccess).inst = .none ∧
      (act Rules.correct w .lostAccess).code = false ∧
      ¬Stranded (act Rules.correct w .lostAccess) := by
  have inv := steps_keep_inv h inv_start
  have c : w.call = .join := by simp_all [enabled]
  have noRegistration : w.registered = false := by
    cases hr : w.registered
    · rfl
    · have j := (inv.join c).2
      simp [inv.registered hr] at j
  refine ⟨rfl, rfl, ?_⟩
  rintro (⟨s, _⟩ | ⟨r, _⟩ | ⟨a', _⟩)
  · simp [act, undo] at s
  · simp [act, undo, noRegistration] at r
  · simp [act, undo] at a'

/-- **A member's registration comes with their profile.** In every
reachable world whose loaded history registers the device, the joining write
has committed: the step that applied the registration committed it. -/
theorem profile_with_registration {w : World} (h : Reachable Rules.correct w)
    (member : w.member = true) : w.profiled = true :=
  (steps_keep_inv h inv_start).profile member

/-- **A pairing keeps nothing until its record.** Nothing on the device lists
a pairing, and so nothing of a join's or a restore's is under it: its cancel,
at any point before its restore records itself, removes the installation and
strands nothing. -/
theorem pairing_unlisted {w : World} (h : Reachable Rules.correct w) (c : w.call = .pair) :
    w.code = false ∧ w.record = false ∧ w.registered = false ∧ w.sent = false :=
  by
    have inv := steps_keep_inv h inv_start
    obtain ⟨a, b⟩ := inv.pair c
    refine ⟨a, b, ?_, ?_⟩
    · cases hr : w.registered
      · rfl
      · simp [inv.registered hr] at b
    · cases hs : w.sent
      · rfl
      · simp [inv.sent hs] at a

/-- A pairing is refused an installation that holds an invite code or a
restore's record, changing nothing. -/
theorem pairing_refused {w : World} (held : w.code = true ∨ w.record = true) :
    enabled Rules.correct w .beginPair = false := by
  rcases held with c | r
  · simp [enabled, Rules.correct, c]
  · simp [enabled, Rules.correct, r]

/-- A pairing cancelled, failed or ended before its record leaves no
installation and nothing kept beside it. -/
theorem pairing_cancel_leaves_nothing {w : World} (h : Reachable Rules.correct w)
    (e : enabled Rules.correct w .pairUndo = true) :
    (act Rules.correct w .pairUndo).inst = .none ∧ (act Rules.correct w .pairUndo).code = false ∧
      (act Rules.correct w .pairUndo).record = false ∧ ¬Stranded (act Rules.correct w .pairUndo) := by
  have inv := steps_keep_inv h inv_start
  have c : w.call = .pair := by simpa [enabled] using e
  obtain ⟨hc, hr, hreg, hsent⟩ := pairing_unlisted h c
  have acc : w.accepted = false := by
    cases ha : w.accepted
    · rfl
    · simp [(inv.accepted ha).1] at hc
  refine ⟨rfl, rfl, rfl, ?_⟩
  rintro (⟨s, _⟩ | ⟨r, _⟩ | ⟨a', _⟩) <;> simp_all [act, undo]

/-- Under the rules no call's removal takes another's listing, and no cancel
leaves an acceptance behind: whatever storage may hold of a join's request or
a restore's registration, and whatever acceptance the joining account holds,
the call that made it stays listed until it meets its outcome. -/
theorem never_stranded {w : World} (h : Reachable Rules.correct w) : ¬Stranded w := by
  have inv := steps_keep_inv h inv_start
  rintro (⟨s, c⟩ | ⟨r, c⟩ | ⟨a, c⟩)
  · simp [inv.sent s] at c
  · simp [inv.registered r] at c
  · simp [(inv.accepted a).1] at c

end CovenSequence.FirstInstallation
