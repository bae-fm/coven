/-!
# A join and a restore of one store on one device

A device keeps one unfinished first installation of a store, at the store's
path, whichever call made it (§13.4): a join (§13.2) or a restore (§13.1).
Each call keeps what lists it to resume. A join keeps its invite code from
before its request may be sent; a restore keeps its record from before its
registration may be sent. Each call removes the installation, and everything
kept beside it, when it ends without its store: a join cancelled before it
may have sent its request, or declined; a restore cancelled or failed before
its record.

A call begins on whatever installation is there, so without a rule a restore
may begin on a waiting join's installation, or a join on a recorded
restore's. Then one call's removal takes the other's listing with it: storage
may hold the other's request or registration, and nothing on the device
lists it to resume. `Rules.exclusive`: a restore refuses an installation
holding an invite code, and a join one holding a restore's record, changing
nothing. Without it, each removal is a checked counterexample.

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
Past it, the facts end the join as they end a restore past its registration:
a removal of its device or member, or a replacement of its member's keys,
stops its device for good, and the join ends without its store, its outcome
met (`stopped`).
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
  deriving DecidableEq, Repr

def start : World := ⟨.none, .none, false, false, false, false, false, false⟩

structure Rules where
  /-- A restore refuses an installation holding an invite code, and a join
  one holding a restore's record. -/
  exclusive : Bool
  /-- A join records that it is accepting the share before it accepts. -/
  recordsAcceptance : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true, true⟩

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
  /-- The join's request is declined: it ends without its store. -/
  | declined
  /-- The join's request is approved: it publishes the store. -/
  | approved
  /-- The join's admission applied, and a later slot stopped its device for
  good: it ends without its store. -/
  | stopped
  /-- A restore begins on the store's installation, making one if none is
  there. -/
  | beginRestore
  /-- The restore records itself. -/
  | record
  /-- The restore sends its registration. -/
  | register
  /-- The restore is cancelled, or fails, before its record. -/
  | restoreUndo
  /-- The restore's registration ended: it publishes the store. -/
  | restored
  /-- The app drops the running call, or the device crashes. -/
  | drop
  deriving DecidableEq, Repr

def enabled (R : Rules) (w : World) : Act → Bool
  | .beginJoin => w.call == .none && w.inst != .published && (!R.exclusive || !w.record)
  | .keepAcceptance => w.call == .join && !w.sent
  | .accept => w.call == .join && !w.sent && (!R.recordsAcceptance || w.acceptKept)
  | .send => w.call == .join
  | .joinCancel => w.call == .join && !w.sent
  | .declined | .approved | .stopped => w.call == .join && w.sent
  | .beginRestore => w.call == .none && w.inst != .published && (!R.exclusive || !w.code)
  | .record => w.call == .restore
  | .register => w.call == .restore && w.record
  | .restoreUndo => w.call == .restore && !w.record
  | .restored => w.call == .restore && w.registered
  | .drop => w.call != .none

def act (w : World) : Act → World
  | .beginJoin => { w with inst := .hidden, call := .join, code := true }
  | .keepAcceptance => { w with acceptKept := true }
  | .accept => { w with accepted := true }
  | .send => { w with sent := true }
  | .joinCancel => { undo w with accepted := w.accepted && !w.acceptKept, acceptKept := false }
  | .declined | .stopped => { undo w with sent := false, accepted := false, acceptKept := false }
  | .approved =>
    { w with inst := .published, call := .none, code := false, sent := false, accepted := false,
             acceptKept := false }
  | .beginRestore => { w with inst := .hidden, call := .restore }
  | .record => { w with record := true }
  | .register => { w with registered := true }
  | .restoreUndo => undo w
  | .restored =>
    { w with inst := .published, call := .none, record := false, registered := false }
  | .drop => { w with call := .none }

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' : World} (a : Act) : enabled R w a = true → Steps R (act w a) w' → Steps R w w'

def Reachable (R : Rules) (w : World) : Prop := Steps R start w

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act w a) rest else none

/-- Storage may hold a call's request or registration whose outcome this
device hasn't met, or the joining account a share's acceptance, and nothing
on the device lists the call to resume or take it back: only typing its code
again would. -/
def Stranded (w : World) : Prop :=
  (w.sent = true ∧ w.code = false) ∨ (w.registered = true ∧ w.record = false) ∨
    (w.accepted = true ∧ w.code = false)

/-! ## Counterexamples: one call's removal takes the other's listing -/

/-- Dan's phone sends its join request for Household, and the app drops the
join while it waits. Someone then restores Household on the phone from a
restore code, which begins on the join's installation, and cancels before
the restore records itself. The cancel removes the installation and the
join's code with it: Ben may approve Dan's request, and no join on the
phone is listed to finish it. -/
theorem restore_strands_join :
    ∃ w, trace ⟨false, true⟩ start [.beginJoin, .send, .drop, .beginRestore, .restoreUndo] = some w ∧
      Stranded w := by
  exact ⟨_, rfl, by simp [Stranded, act, undo, start]⟩

/-- Ana's laptop records its restore of Household and sends its
registration, and the app drops the call. A join with an invite to
Household then begins on the restore's installation and is cancelled
before it sends its request. The cancel removes the installation and the
restore's record with it: the registration may land, and no restore on the
laptop is listed to finish it. -/
theorem join_strands_restore :
    ∃ w, trace ⟨false, true⟩ start [.beginRestore, .record, .register, .drop, .beginJoin, .joinCancel] =
        some w ∧ Stranded w := by
  exact ⟨_, rfl, by simp [Stranded, act, undo, start]⟩

/-- Dan's phone mounts Household in his Dropbox, recording nothing, and the
app closes before sending his request. Resumed, the join is cancelled: it
removes the installation and the code, and Household stays mounted in Dan's
Dropbox with no join listed to take it out. -/
theorem cancel_strands_acceptance :
    ∃ w, trace ⟨true, false⟩ start [.beginJoin, .accept, .drop, .beginJoin, .joinCancel] =
        some w ∧ Stranded w := by
  exact ⟨_, rfl, by simp [Stranded, act, undo, start]⟩

/-! ## Under the rules -/

structure Inv (w : World) : Prop where
  sent : w.sent = true → w.code = true
  registered : w.registered = true → w.record = true
  /-- At most one call's listing is kept. -/
  one : ¬(w.code = true ∧ w.record = true)
  join : w.call = .join → w.code = true ∧ w.record = false
  restore : w.call = .restore → w.code = false
  /-- An acceptance is kept by a listed join that records it, or has sent. -/
  accepted : w.accepted = true → w.code = true ∧ (w.acceptKept = true ∨ w.sent = true)

theorem inv_start : Inv start := by
  constructor <;> simp [start]

theorem inv_step (w : World) (a : Act) (inv : Inv w) (e : enabled Rules.correct w a = true) :
    Inv (act w a) := by
  obtain ⟨sent, registered, one, join, restore, accepted⟩ := inv
  cases a <;> rcases hc : w.call <;> constructor <;>
    simp_all [act, enabled, Rules.correct, undo]

theorem steps_keep_inv : ∀ {w w'}, Steps Rules.correct w w' → Inv w → Inv w' := by
  intro w w' h
  induction h with
  | refl => exact id
  | step a e _ ih => exact fun inv => ih (inv_step _ a inv e)

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
