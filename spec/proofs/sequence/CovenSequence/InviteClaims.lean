/-!
# The entries an invite claims, and the one claim a device has open

An invite on the device that made it claims entries of its own beyond its
approval's binding (§13.2): the record of its share before the grant
(`AddInviteShare`), the end of that share once the invite ends
(`EndInviteShare`), and a kept S3 replacement's record of its member's new
key (`SetAccess`). Each is an operation of its own, which claims its slot
through the device's one claim journal (§5.1): while one claim is open, no
other claim starts, so no write is sent and no admin operation lands.

`InviteLifecycle` follows the invite's stages, its binding, the app's steps
and a stop for good, and takes each of these entries as landing in the step
that needs it. This model follows what that leaves out: each entry's
operation and its claim, through the attempt being sent, landing or losing
its slot to another device, and this device reading its own landed slot
before it settles the claim, as a pass that restarts after a crash between
the claim landing and its end does. Approval and its binding are one step
here (`approve`); a stop for good ends every operation an invite started in
the transaction applying it, which `InviteLifecycle` covers.

Every step is one transaction, so a crash between any two is a path of
steps. `Rules` names whether the invite names each operation it starts and
ends it with itself (`settleChildren`). Without it, as before, an ended
invite looked only at the share in history: it ended without its share's
end when its own slot showed the share already ended, and never looked at
the share's record it was claiming when the app cancelled it. Both leave a
claim open for good, a checked counterexample each.
-/
namespace CovenSequence.InviteClaims

/-- The invite's state, as its journal row encodes it (`InviteState`). -/
inductive Stage where
  | requested
  /-- Its share's record is being claimed (`Sharing`). -/
  | sharing
  /-- The grant, the code shown, the request and the app's decision
  (`Grant`, `Waiting`, `Approving`), which `InviteLifecycle` follows. -/
  | granted
  /-- A kept S3 replacement records its member's new key. -/
  | recording
  | settled
  deriving DecidableEq, Repr

/-- The operations an invite starts, by the entry each claims. -/
inductive Kind where
  | share
  | ending
  | record
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
  /-- The invite's journal row; `none` once it finished. -/
  row : Option Stage
  /-- The operation recording the share: running (`false`) or at its
  applied step (`true`); `none` when not journaled. -/
  share : Option Bool
  /-- The operation ending the share. -/
  ending : Option Bool
  /-- The operation recording the replacement's key. -/
  record : Option Bool
  /-- The device's one open claim, the operation whose it is, and its last
  attempt. -/
  claim : Option (Kind × Attempt)
  /-- History this device applied records the invite's share. -/
  recorded : Bool
  /-- ... and its end. -/
  ended : Bool
  /-- ... and the replacement's key. -/
  accessed : Bool
  deriving DecidableEq, Repr

/-- The invite just made by the app's write. -/
def made : World := ⟨some .requested, none, none, none, none, false, false, false⟩

/-- Whether approval goes on to record a kept replacement's S3 key. -/
structure Config where
  records : Bool
  deriving DecidableEq, Repr

structure Rules where
  /-- The invite's state names each operation it starts, and its settled
  step settles the claim of every one still journaled, claiming nothing for
  it again, and ends them all in the transaction that ends the invite. -/
  settleChildren : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true⟩

/-- The invite before its settled step named its operations. -/
def Rules.unnamed : Rules := ⟨false⟩

def child (w : World) : Kind → Option Bool
  | .share => w.share
  | .ending => w.ending
  | .record => w.record

def setChild (w : World) : Kind → Option Bool → World
  | .share, v => { w with share := v }
  | .ending, v => { w with ending := v }
  | .record, v => { w with record := v }

/-- Applying the entry `k` claims. -/
def applyEntry (w : World) : Kind → World
  | .share => { w with recorded := true }
  | .ending => { w with ended := true }
  | .record => { w with accessed := true }

/-- History records the share and not its end. -/
def shareOpen (w : World) : Bool := w.recorded && !w.ended

/-- A pass runs the operation `k`: it claims once no claim is open, and a
claim of another operation's refuses it (`ClaimPending`). A landed claim
applies its entry and ends, moving the operation to its applied step; one
that lost its slot claims the next. A sent attempt waits for its reply. -/
def run (w : World) (k : Kind) : World :=
  match w.claim with
  | none => if child w k = some false then { w with claim := some (k, .sent) } else w
  | some (k', a) =>
    if k' = k then
      match a with
      | .sent => w
      | .landed => { setChild (applyEntry w k) k (some true) with claim := none }
      | .lost => { w with claim := some (k, .sent) }
    else w

/-- The ended invite settles the claim of its share's record without
claiming again: a landed one applies and ends, one that lost its slot ends
with no entry, and a sent one waits for its reply. -/
def settleShare (w : World) : World :=
  match w.claim with
  | some (.share, .landed) => { w with recorded := true, share := some true, claim := none }
  | some (.share, .lost) => { w with claim := none }
  | _ => w

/-- The settled step: end the share, and the invite. -/
def settledStep (R : Rules) (w : World) : World :=
  if R.settleChildren then
    match w.claim with
    | some (.share, _) => settleShare w
    | some (.ending, _) => run w .ending
    | some (.record, _) => w
    | none =>
      if shareOpen w then
        match w.ending with
        | none => { w with ending := some false }
        | some _ => run w .ending
      else { w with row := none, share := none, ending := none, record := none }
  else
    -- The share's end, once run, ends in a transaction of its own; the
    -- invite looks at history alone.
    if w.ending = some true then { w with ending := none }
    else if shareOpen w then
      match w.ending with
      | none => { w with ending := some false }
      | some _ => run w .ending
    else { w with row := none }

/-- A pass's step for the invite. The granted stages wait for the joiner
and the app. -/
def pass (R : Rules) (w : World) : World :=
  match w.row with
  | none => w
  | some .requested => { w with row := some .sharing, share := some false }
  | some .sharing =>
    if w.share = some true then { w with row := some .granted, share := none } else run w .share
  | some .granted => w
  | some .recording =>
    match w.record with
    | none => { w with record := some false }
    | some true => { w with row := some .settled, record := none }
    | some false => run w .record
  | some .settled => settledStep R w

inductive Act where
  /-- A pass advances the invite. -/
  | pass
  /-- The provider stores the open claim's attempt: the first request, or
  the same bytes sent again. -/
  | land
  /-- Another device's entry takes the attempt's slot first. -/
  | lose
  /-- A pass reads this device's own landed slot and applies it, before it
  settles the claim: after a restart, it reads forward first. -/
  | apply
  /-- The app approves, and the binding lands. -/
  | approve
  /-- The app cancels or declines. -/
  | cancel
  /-- The invite's expiry, written when the clock reaches it. -/
  | expire
  deriving DecidableEq, Repr

/-- Stages that end at the expiry, and that the app can end. -/
def ends : Option Stage → Bool
  | some .requested | some .sharing | some .granted => true
  | _ => false

def attemptIs (w : World) (a : Attempt) : Bool :=
  match w.claim with
  | some (_, a') => a' == a
  | none => false

def enabled (w : World) : Act → Bool
  | .pass => w.row.isSome
  | .land | .lose => attemptIs w .sent
  | .apply => attemptIs w .landed
  | .approve => w.row == some .granted
  | .cancel | .expire => ends w.row

def act (R : Rules) (c : Config) (w : World) : Act → World
  | .pass => pass R w
  | .land => match w.claim with
    | some (k, _) => { w with claim := some (k, .landed) }
    | none => w
  | .lose => match w.claim with
    | some (k, _) => { w with claim := some (k, .lost) }
    | none => w
  | .apply => match w.claim with
    | some (k, _) => applyEntry w k
    | none => w
  | .approve => { w with row := some (if c.records then .recording else .settled) }
  | .cancel | .expire => { w with row := some .settled }

inductive Step (R : Rules) (c : Config) : World → World → Prop
  | step (w : World) (a : Act) (h : enabled w a = true) : Step R c w (act R c w a)

inductive Steps (R : Rules) (c : Config) : World → World → Prop
  | refl (w : World) : Steps R c w w
  | step {w w' w'' : World} : Step R c w w' → Steps R c w' w'' → Steps R c w w''

def Reachable (R : Rules) (c : Config) (w : World) : Prop := Steps R c made w

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) (c : Config) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled w a then trace R c (act R c w a) rest else none

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

/-- A property every step keeps holds from then on. -/
theorem stays {R : Rules} {c : Config} (P : World → Prop)
    (keep : ∀ w a, P w → enabled w a = true → P (act R c w a)) :
    ∀ {w w'}, Steps R c w w' → P w → P w' := by
  intro w w' h
  induction h with
  | refl => exact id
  | step s _ ih =>
    cases s with
    | step a e => exact fun p => ih (keep _ a p e)

def plain : Config := ⟨false⟩

/-! ## Counterexamples: the invite before it named its operations -/

/-- An invite gone from the journal whose share's end still holds the
claim. -/
def StrandedEnd (w : World) : Prop :=
  w.row = none ∧ ∃ a, w.claim = some (.ending, a)

theorem stranded_end_stays (c : Config) (w : World) (a : Act) (h : StrandedEnd w)
    (_ : enabled w a = true) : StrandedEnd (act Rules.unnamed c w a) := by
  obtain ⟨row, x, claim⟩ := h
  cases a <;> simp_all [StrandedEnd, act, enabled, ends, applyEntry]

/-- Ana cancels the invite, and its pass claims the share's end, which
lands; the device crashes before it ends the claim. On restart the pass
reads its own slot first: the share has ended, so the settled step ends the
invite alone. The end's claim stays open on every path after, and the
device claims no slot again. -/
theorem ended_share_strands_claim :
    ∃ w, trace Rules.unnamed plain made
        [.pass, .pass, .land, .pass, .pass, .cancel, .pass, .pass, .land, .apply, .pass] = some w ∧
      ∀ w', Steps Rules.unnamed plain w w' → w'.claim.isSome := by
  refine ⟨_, rfl, fun w' h => ?_⟩
  obtain ⟨_, x, claim⟩ := stays _ (stranded_end_stays plain) h ⟨rfl, _, rfl⟩
  simp [claim]

/-- A cancelled invite whose share's record holds the claim, its share open
in history, which nothing settles. -/
def StrandedShare (w : World) : Prop :=
  w.row = some .settled ∧ w.claim = some (.share, .landed) ∧ w.recorded = true ∧
    w.ended = false ∧ w.ending ≠ some true

theorem stranded_share_stays (c : Config) (w : World) (a : Act) (h : StrandedShare w)
    (_ : enabled w a = true) : StrandedShare (act Rules.unnamed c w a) := by
  obtain ⟨row, claim, recorded, ended, ending⟩ := h
  cases a <;> simp_all [StrandedShare, act, pass, settledStep, Rules.unnamed, enabled, ends,
    applyEntry, attemptIs, shareOpen]
  case pass => split <;> simp_all [run]

/-- Ana cancels the invite while its pass is claiming the share's record,
and the device crashes after the attempt lands. On restart the ended
invite reads the share open in history and claims its end, which the open
claim refuses. The share's claim stays open on every path after, and the
invite never ends. -/
theorem cancelled_share_strands_claim :
    ∃ w, trace Rules.unnamed plain made [.pass, .pass, .cancel, .land, .apply, .pass, .pass] =
        some w ∧
      ∀ w', Steps Rules.unnamed plain w w' → w'.claim.isSome ∧ w'.row.isSome := by
  refine ⟨_, rfl, fun w' h => ?_⟩
  obtain ⟨row, claim, _⟩ :=
    stays _ (stranded_share_stays plain) h ⟨rfl, rfl, rfl, rfl, by decide⟩
  simp [claim, row]

/-! ## The properties, under the rules -/

/-- What every world reachable under `correct` satisfies. -/
structure Inv (w : World) : Prop where
  /-- A finished invite left no operation it started, and no claim. -/
  gone : w.row = none → w.share = none ∧ w.ending = none ∧ w.record = none ∧ w.claim = none
  /-- Each operation is journaled only in the stages that name it. -/
  shareAt : w.share.isSome → w.row = some .sharing ∨ w.row = some .settled
  sharing : w.row = some .sharing → w.share.isSome
  endingAt : w.ending.isSome → w.row = some .settled
  recordAt : w.record.isSome → w.row = some .recording
  /-- The open claim is a running operation's. -/
  claimShare : ∀ a, w.claim = some (.share, a) → w.share = some false
  claimEnding : ∀ a, w.claim = some (.ending, a) → w.ending = some false
  claimRecord : ∀ a, w.claim = some (.record, a) → w.record = some false
  /-- The share's end at its applied step has applied. -/
  endingDone : w.ending = some true → w.ended = true

theorem inv_made : Inv made := by
  constructor <;> simp [made]

set_option maxHeartbeats 2000000 in
theorem inv_pass (w : World) (inv : Inv w) : Inv (pass Rules.correct w) := by
  obtain ⟨gone, shareAt, sharing, endingAt, recordAt, claimShare, claimEnding, claimRecord,
    endingDone⟩ := inv
  rcases hrow : w.row with _ | (_ | _ | _ | _ | _)
  · constructor <;> simp_all [pass]
  · rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> constructor <;> simp_all [pass]
  · rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> rcases hs : w.share with _ | (_ | _) <;> constructor <;>
      simp_all [pass, run, child, setChild, applyEntry]
  · constructor <;> simp_all [pass]
  · rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> rcases hr : w.record with _ | (_ | _) <;> constructor <;>
      simp_all [pass, run, child, setChild, applyEntry]
  · rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> rcases he : w.ending with _ | (_ | _) <;> rcases hrec : w.recorded <;>
      rcases hend : w.ended <;> constructor <;>
      simp_all [pass, settledStep, Rules.correct, settleShare, run, child, setChild, applyEntry,
        shareOpen]

set_option maxHeartbeats 2000000 in
theorem inv_step (c : Config) (w : World) (a : Act) (inv : Inv w) (e : enabled w a = true) :
    Inv (act Rules.correct c w a) := by
  cases a
  case pass => exact inv_pass w inv
  all_goals
    obtain ⟨gone, shareAt, sharing, endingAt, recordAt, claimShare, claimEnding, claimRecord,
      endingDone⟩ := inv
  case land | lose | apply => rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> constructor <;> simp_all [act, applyEntry]
  all_goals
    rcases hrow : w.row with _ | (_ | _ | _ | _ | _) <;> (try rcases hrec : c.records) <;>
      rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> constructor <;> simp_all [act, enabled, ends]

theorem reachable_inv (c : Config) {w : World} (h : Reachable Rules.correct c w) : Inv w :=
  stays Inv (inv_step c) h inv_made

/-- An invite ends together with every operation it started: once its row
is gone, none of theirs is journaled and no claim is open. -/
theorem children_end_with_invite (c : Config) {w : World} (h : Reachable Rules.correct c w)
    (finished : w.row = none) :
    w.share = none ∧ w.ending = none ∧ w.record = none ∧ w.claim = none :=
  (reachable_inv c h).gone finished

/-! ### Every invite ends, and its claims with it -/

/-- What coven does on its own: its passes, its requests reaching storage,
and the invite's expiry. -/
def coven : Act → Bool
  | .pass | .land | .apply | .expire => true
  | _ => false

inductive Moves (c : Config) : World → World → Prop
  | refl (w : World) : Moves c w w
  | step {w w' : World} (a : Act) (own : coven a = true) (e : enabled w a = true)
      (rest : Moves c (act Rules.correct c w a) w') : Moves c w w'

theorem Moves.steps {c : Config} {w w' : World} (h : Moves c w w') :
    Steps Rules.correct c w w' := by
  induction h with
  | refl => exact .refl _
  | step a _ e _ ih => exact .step (.step _ a e) ih

theorem Moves.trans {c : Config} {a b d : World} (h : Moves c a b) (k : Moves c b d) :
    Moves c a d := by
  induction h with
  | refl => exact k
  | step x own e _ ih => exact .step x own e (ih k)

theorem one {c : Config} {w : World} (a : Act) (own : coven a = true)
    (e : enabled w a = true) : Moves c w (act Rules.correct c w a) :=
  .step a own e (.refl _)

def attemptWork : Attempt → Nat
  | .landed => 1
  | .sent => 2
  | .lost => 3

/-- What is left of the running operation `k` in its stage. -/
def runWork (w : World) (k : Kind) (unstarted : Nat) : Nat :=
  match w.claim with
  | some (k', a) => if k' = k then attemptWork a else unstarted
  | none => if child w k = some true then 0 else unstarted

/-- What is left of the settled step: the share's claim to settle, then the
share's end. -/
def settledWork (w : World) : Nat :=
  let share := match w.claim with
    | some (.share, .sent) => 20
    | some (.share, _) => 10
    | _ => 0
  let ending := match w.claim with
    | some (.ending, a) => attemptWork a + 1
    | _ => if shareOpen w then (if w.ending = none then 6 else 5) else 0
  share + ending

/-- What is left to do. -/
def measure (w : World) : Nat :=
  match w.row with
  | none => 0
  | some .settled => 100 + settledWork w
  | some .recording => 200 + (if w.record = none then 5 else runWork w .record 4)
  | some .granted => 300
  | some .sharing => 400 + runWork w .share 4
  | some .requested => 500

/-- Nothing left: the invite has ended, and no claim is open, so the
device's writes go out. -/
def quiet (w : World) : Bool := w.row.isNone && w.claim.isNone

/-- The settled step's work is bounded, so every earlier stage outweighs it. -/
theorem settledWork_le (w : World) : settledWork w ≤ 30 := by
  simp only [settledWork]
  rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> simp [attemptWork] <;>
    split <;> (try split) <;> omega

set_option maxHeartbeats 4000000 in
/-- From every reachable state that isn't quiet, coven alone makes
progress: it sends a claim's attempt again until it lands, or the pass
takes its next step, or the invite expires. -/
theorem progress (c : Config) (w : World) (inv : Inv w) (busy : quiet w = false) :
    ∃ w', Moves c w w' ∧ measure w' < measure w := by
  obtain ⟨gone, shareAt, sharing, endingAt, recordAt, claimShare, claimEnding, claimRecord,
    endingDone⟩ := inv
  have passes : w.row.isSome → measure (act Rules.correct c w .pass) < measure w →
      ∃ w', Moves c w w' ∧ measure w' < measure w :=
    fun some less => ⟨_, one .pass rfl (by simpa [enabled] using some), less⟩
  have lands : attemptIs w .sent = true → measure (act Rules.correct c w .land) < measure w →
      ∃ w', Moves c w w' ∧ measure w' < measure w :=
    fun sent less => ⟨_, one .land rfl (by simpa [enabled] using sent), less⟩
  rcases hrow : w.row with _ | (_ | _ | _ | _ | _)
  · have := gone hrow
    simp_all [quiet]
  · apply passes (by simp [hrow])
    rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> simp_all [act, pass, measure, runWork, child]
  · rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> rcases hs : w.share with _ | (_ | _)
    all_goals first
      | (refine lands ?_ ?_
         · simp [attemptIs, hc]
         · simp_all [act, measure, runWork, attemptWork])
      | (apply passes (by simp [hrow])
         simp_all [act, pass, run, measure, runWork, child, setChild, applyEntry, attemptWork])
  · refine ⟨_, one .expire rfl (by simp [enabled, ends, hrow]), ?_⟩
    have := settledWork_le { w with row := some .settled }
    simp only [act, measure, hrow] at this ⊢
    omega
  · rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> rcases hr : w.record with _ | (_ | _)
    all_goals first
      | (refine lands ?_ ?_
         · simp [attemptIs, hc]
         · simp_all [act, measure, runWork, attemptWork])
      | (apply passes (by simp [hrow])
         have := settledWork_le { w with row := some .settled, record := none }
         simp_all [act, pass, run, measure, runWork, child, setChild, applyEntry, attemptWork]
         omega)
      | (apply passes (by simp [hrow])
         simp_all [act, pass, run, measure, runWork, child, setChild, applyEntry, attemptWork])
  · rcases hc : w.claim with _ | ⟨(_ | _ | _), (_ | _ | _)⟩ <;> rcases he : w.ending with _ | (_ | _) <;> rcases hrec : w.recorded <;>
      rcases hend : w.ended
    all_goals first
      | (refine lands ?_ ?_
         · simp [attemptIs, hc]
         · simp_all [act, measure, settledWork, attemptWork, shareOpen])
      | (apply passes (by simp [hrow])
         simp_all [act, pass, settledStep, Rules.correct, settleShare, run, measure, settledWork,
           child, setChild, applyEntry, attemptWork, shareOpen])

/-- Every invite ends, with every operation it started, and leaves no claim
open, on every path: from any reachable state, coven alone reaches a state
where the invite has ended and the device's writes go out. That includes a
crash between any two of its transactions, its own slots read before their
claims settle, slots lost to other devices, and the app cancelling the
invite while its share's record is being claimed. -/
theorem settles (c : Config) {w : World} (h : Reachable Rules.correct c w) :
    ∃ w', Moves c w w' ∧ quiet w' = true := by
  generalize hm : measure w = m
  induction m using Nat.strongRecOn generalizing w with
  | ind m ih =>
    by_cases q : quiet w = true
    · exact ⟨w, .refl w, q⟩
    · obtain ⟨w', moves, less⟩ := progress c w (reachable_inv c h) (by simpa using q)
      obtain ⟨w'', more, quiet⟩ := ih (measure w') (hm ▸ less) (h.trans moves.steps) rfl
      exact ⟨w'', moves.trans more, quiet⟩

end CovenSequence.InviteClaims
