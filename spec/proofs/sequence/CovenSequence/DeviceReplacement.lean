/-!
# Replacing a device's installation, through crashes, dropped calls and cancels

`reset_device` and `restore_device` load a new installation of a store beside
the device's old one, under a fresh device id, then register the fresh id as
replacing the old (§19.3, §13.1): an operation that claims an
`AddDevice { replaces }` entry through the device's one claim journal (§5.1).
Once that claim lands, the store's history names the old id as replaced: the
old installation, opened again, applies its replacement and stops for good,
with its unsent writes never sent. Then the call swaps the new installation
into the store's place and publishes it.

The registration is the replacement's point of no return (§11.6). Before it,
the old installation is the store, and a cancel removes the new one. From it
on, the replacement only goes forward: the app's opens are refused, the
store is listed as an unfinished replacement, and an explicit resume takes it
to the end. The one way back is the facts': a registration refused, or the
fresh device gone for good since, leaves nothing in storage naming it as the
old one's active replacement, so the call removes the new installation and
the old one is the store again.

The model follows one replacement's call and its registration, as
`RestoreRegistration` does, with the old installation beside it. The call
records the replacement (`record`), journals the registration (`register`),
swaps the new installation in once the registration applied (`swap`) and
publishes it (`publish`); a registration refused, or a device gone for good
since, ends it without the new installation (`conclude`). The registration's
task claims and settles (`pass`); storage stores an attempt (`land`) or
another device takes its slot (`lose`). The app may cancel the call before
the swap, drop it or crash at any point, and open the store when no call
holds it; the call may fail; another device may remove the member or replace
its keys, after which a registration is refused and a registered device is
gone for good.

`Rules` names the two rules the replacement needs. `record`: before it
registers, the replacement records itself, with what finishing needs, and
the record refuses the app's opens of the old installation and lists the
replacement to resume. `forward`: from the record on, a cancel waits for the
registration and removes the new installation only when the facts allow
it, and a failure after the swap leaves the swap for the resume instead of
moving the old installation back. Without either rule the old installation
opens after its replacement landed, or the new one is removed while its
registration names it, a checked counterexample each.
-/
namespace CovenSequence.DeviceReplacement

/-- The new installation. -/
inductive New where
  /-- Removed, or never made. -/
  | none
  /-- Loaded beside the old installation, in its hidden directory. -/
  | loading
  /-- Swapped into the store's place, not yet published. -/
  | swapped
  | published
  deriving DecidableEq, Repr

/-- The registration operation in the new installation's journal. -/
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
  | sent
  | landed
  | lost
  deriving DecidableEq, Repr

structure World where
  new : New
  /-- The old installation is in the store's place. -/
  old : Bool
  /-- The replacement's record, with what finishing needs. -/
  recorded : Bool
  /-- A call runs on the replacement. -/
  call : Bool
  /-- The app asked the running call to cancel. -/
  cancelled : Bool
  op : Op
  /-- The registration's task is running. -/
  task : Bool
  claim : Option Attempt
  /-- The store's history holds the fresh device's registration, replacing
  the old one. -/
  landed : Bool
  /-- Another device removed the member or replaced its keys: a
  registration is refused, and a registered device is gone for good. -/
  gone : Bool
  /-- The old installation opened after its replacement landed: it stopped
  for good, its unsent writes never sent. -/
  stale : Bool
  deriving DecidableEq, Repr

/-- A replacement call has just loaded its new installation beside the old. -/
def start : World := ⟨.loading, true, false, true, false, .none, false, none, false, false, false⟩

structure Rules where
  /-- The replacement records itself before it registers; the record refuses
  opens and lists the replacement. -/
  record : Bool
  /-- From the record on, a cancel waits and undoes only when the facts allow
  it, and a failure after the swap keeps the swap. -/
  forward : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true, true⟩

/-- Nothing in storage names the fresh device as the old one's active
replacement, and nothing can land. -/
def undoable (w : World) : Bool :=
  !w.recorded || w.op == .none || w.op == .rejected || (w.landed && w.gone)

/-- Remove the new installation and its record: the old one is the store
again. An attempt already sent is on its way whatever the device deletes. -/
def undo (w : World) : World :=
  { w with new := .none, recorded := false, call := false, cancelled := false, op := .none,
           task := false, claim := if w.claim == some .sent then w.claim else none }

def running (w : World) : Bool := w.call && !w.cancelled

inductive Act where
  | record
  | register
  | pass
  | land
  | lose
  /-- The registration applied: the new installation takes the store's place. -/
  | swap
  /-- The swapped installation is published, and the call ends. -/
  | publish
  /-- The registration was refused, or the fresh device is gone for good
  since: the call ends without the new installation. -/
  | conclude
  /-- The app cancels the running call, before its swap. -/
  | cancel
  /-- The cancelled call ends. -/
  | stop
  /-- The call fails and ends. -/
  | fail
  | taskFails
  /-- The app resumes a listed replacement. -/
  | resume
  /-- The device crashes, or the app drops the call. -/
  | crash
  /-- Another device removes the member or replaces its keys. -/
  | remove
  /-- The app opens the store while no call holds it. -/
  | openOld
  deriving DecidableEq, Repr

def enabled (R : Rules) (w : World) : Act → Bool
  | .record => R.record && running w && !w.recorded
  | .register => running w && (w.recorded || !R.record) && !w.task && w.new == .loading &&
      (w.op == .none || w.op == .running)
  | .pass => w.op == .running && w.task && w.claim != some .sent
  | .land | .lose => w.claim == some .sent
  | .swap => running w && w.new == .loading && w.op == .applied && !w.gone
  | .publish => running w && w.new == .swapped
  | .conclude => running w && w.new == .loading &&
      (w.op == .rejected || (w.op == .applied && w.gone) || (w.landed && w.gone))
  | .cancel => running w && w.new == .loading
  | .stop => w.call && w.cancelled && (!R.forward || !w.task)
  | .fail => running w
  | .taskFails => w.task
  | .resume => !w.call && w.recorded
  | .crash => true
  | .remove => true
  -- A running call holds the store's locks; the record refuses opens.
  | .openOld => !w.call && w.old && !(R.record && w.recorded)

/-- The registration's task: claim when no claim is open, refused once the
device is gone; a landed claim applies; a lost one checks again. -/
def pass (w : World) : World :=
  match w.claim with
  | none | some .lost =>
    if w.gone then { w with op := .rejected, task := false, claim := none }
    else { w with claim := some .sent }
  | some .landed => { w with op := .applied, task := false, claim := none }
  | some .sent => w

def act (R : Rules) (w : World) : Act → World
  | .record => { w with recorded := true }
  | .register => { w with op := .running, task := true }
  | .pass => pass w
  | .land => { w with claim := some .landed, landed := true }
  | .lose => { w with claim := some .lost }
  | .swap => { w with new := .swapped, old := false }
  | .publish =>
    { w with new := .published, recorded := false, call := false, op := .none }
  | .conclude => undo w
  | .cancel => { w with cancelled := true }
  | .stop =>
    if !R.forward || undoable w then undo w else { w with call := false, cancelled := false }
  | .fail =>
    -- Without `forward`, a failure after the swap moves the old
    -- installation back and removes the record.
    if !R.forward && w.new == .swapped then
      { w with new := .loading, old := true, recorded := false, call := false }
    else { w with call := false }
  | .taskFails => { w with task := false }
  | .resume => { w with call := true }
  | .crash => { w with call := false, cancelled := false, task := false }
  | .remove => { w with gone := true }
  | .openOld => { w with stale := w.stale || (w.landed && !w.gone) }

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' : World} (a : Act) : enabled R w a = true → Steps R (act R w a) w' → Steps R w w'

def Reachable (R : Rules) (w : World) : Prop := Steps R start w

def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act R w a) rest else none

/-- The fresh device's registration names a replacement nothing stands for:
it is active in history, and its installation is gone. -/
def Missing (w : World) : Prop := w.landed = true ∧ w.gone = false ∧ w.new = .none

/-! ## Counterexamples -/

/-- Carol resets her laptop. The fresh device's registration lands, and the
laptop crashes before the swap. Without the record, opening the store opens
the old installation, which applies its replacement and stops for good. -/
theorem crashed_replacement_opens_old :
    ∃ w, trace ⟨false, true⟩ start [.register, .pass, .land, .crash, .openOld] = some w ∧
      w.stale = true := by
  exact ⟨_, rfl, rfl⟩

/-- The registration applies and the new installation swaps in; then
committing its keys fails. Moving the old installation back removes the
record, and opening the store opens the old installation after its
replacement landed. -/
theorem unswapped_replacement_opens_old :
    ∃ w, trace ⟨true, false⟩ start
        [.record, .register, .pass, .land, .pass, .swap, .fail, .openOld] = some w ∧
      w.stale = true := by
  exact ⟨_, rfl, rfl⟩

/-- Carol cancels while the registration's attempt is on its way. A cancel
that doesn't wait removes the new installation, and the attempt lands
after: history names a replacement that isn't there, and the old
installation is replaced. -/
theorem cancelled_replacement_missing :
    ∃ w, trace ⟨true, false⟩ start [.record, .register, .pass, .cancel, .stop, .land] = some w ∧
      Missing w := by
  exact ⟨_, rfl, by simp [Missing, act, pass, undo, start]⟩

/-! ## Under the rules -/

structure Inv (w : World) : Prop where
  call : w.call = true → w.new = .loading ∨ w.new = .swapped
  cancelled : w.cancelled = true → w.call = true ∧ w.new = .loading
  op : w.op ≠ .none → w.recorded = true
  recorded : w.recorded = true → w.new = .loading ∨ w.new = .swapped
  task : w.task = true → w.op = .running ∧ w.new = .loading
  claim : w.claim.isSome → w.op = .running
  landed : w.landed = true →
    w.claim = some .landed ∨ w.op = .applied ∨ w.new = .published ∨ w.new = .none
  missing : w.new = .none → w.landed = true → w.gone = true
  claimLanded : w.claim = some .landed → w.landed = true
  applied : w.op = .applied → w.landed = true
  rejected : w.op = .rejected → w.landed = false
  swapped : w.new = .swapped → w.old = false ∧ w.recorded = true ∧ w.op = .applied
  old : w.old = false → w.new = .swapped ∨ w.new = .published
  published : w.new = .published → w.landed = true ∧ w.old = false
  stale : w.stale = false

theorem inv_start : Inv start := by
  constructor <;> simp [start]

set_option maxHeartbeats 4000000 in
theorem inv_step (w : World) (a : Act) (inv : Inv w) (e : enabled Rules.correct w a = true) :
    Inv (act Rules.correct w a) := by
  obtain ⟨call, cancelled, op, recorded, task, claim, landed, missing, claimLanded, applied,
    rejected, swapped, old, published, stale⟩ := inv
  cases a
  all_goals simp only [enabled, Rules.correct, running, Bool.and_eq_true, Bool.not_eq_true',
    beq_iff_eq, bne_iff_ne, ne_eq, Bool.or_eq_true, Bool.true_and, Bool.not_true,
    Bool.false_or] at e
  case record | cancel | taskFails | resume | crash | remove =>
    constructor <;> simp_all [act]
  case land | lose =>
    rcases hc : w.claim with _ | (_ | _ | _) <;> constructor <;> simp_all [act]
  case register =>
    rcases hop : w.op <;> constructor <;> simp_all [act]
  case pass =>
    rcases hc : w.claim with _ | (_ | _ | _) <;> rcases hg : w.gone <;>
      constructor <;> simp_all [act, pass]
  case swap | publish =>
    constructor <;> simp_all [act]
  case fail =>
    constructor <;> simp_all [act, Rules.correct]
  case conclude =>
    rcases hc : w.claim with _ | (_ | _ | _) <;> rcases hop : w.op <;>
      constructor <;> simp_all [act, undo]
  case stop =>
    by_cases hu : undoable w = true
    · rcases hc : w.claim with _ | (_ | _ | _) <;> rcases hop : w.op <;>
        rcases hrec : w.recorded <;> constructor <;> simp_all [act, Rules.correct, undo, undoable]
    · constructor <;> simp_all [act, Rules.correct]
  case openOld =>
    rcases hl : w.landed <;> rcases hg : w.gone <;> constructor <;> simp_all [act]
    all_goals
      rcases landed with h | h | h | h <;> simp_all

theorem replacement_steps_inv : ∀ {w w'}, Steps Rules.correct w w' → Inv w → Inv w' := by
  intro w w' h
  induction h with
  | refl => exact id
  | step a e _ ih => exact fun inv => ih (inv_step _ a inv e)

theorem replacement_reachable_inv {w : World} (h : Reachable Rules.correct w) : Inv w :=
  replacement_steps_inv h inv_start

/-- The old installation never opens once a registration replacing it may
have landed: no copy stops for good with its writes unsent. -/
theorem old_never_stale {w : World} (h : Reachable Rules.correct w) : w.stale = false :=
  (replacement_reachable_inv h).stale

/-- While history names the fresh device as the old one's active
replacement, its installation is there: loading, swapped or published. -/
theorem never_missing {w : World} (h : Reachable Rules.correct w) : ¬Missing w := by
  rintro ⟨l, g, n⟩
  simp [(replacement_reachable_inv h).missing n l] at g

/-! ### Every replacement settles -/

/-- What coven does on its own, and the app resuming the replacements the
store listing shows: the call's steps, the registration's task, its
requests reaching storage, and a cancelled call ending. -/
def coven : Act → Bool
  | .record | .register | .pass | .land | .swap | .publish | .conclude | .stop | .resume => true
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

/-- Nothing left: the new installation is published, or no call runs and
nothing is recorded, so the old installation is the store. -/
def quiet (w : World) : Bool := w.new == .published || (!w.call && !w.recorded)

/-- What is left of the registration and the swap. -/
def work (w : World) : Nat :=
  if w.new == .swapped then 0
  else 2 + match w.op with
    | .none => 20
    | .running =>
      (if w.task then 0 else 10) +
        match w.claim with
        | none => 8
        | some .lost => 7
        | some .sent => 6
        | some .landed => 3
    | .applied | .rejected => 1

def measure (w : World) : Nat :=
  if quiet w then 0
  else if !w.recorded then 4000 + work w
  else if w.cancelled then 3000 + work w
  else if !w.call then 2000 + work w
  else 1000 + work w

theorem work_le (w : World) : work w ≤ 40 := by
  simp only [work]
  split
  · omega
  · split <;> (try split) <;> (try split) <;> omega

theorem measure_lt {w w' : World} (busy : quiet w = false) (new : w'.new = w.new)
    (recorded : w'.recorded = w.recorded) (call : w'.call = w.call)
    (cancelled : w'.cancelled = w.cancelled) (less : work w' < work w) :
    measure w' < measure w := by
  have busy' : quiet w' = false := by simpa [quiet, new, recorded, call] using busy
  unfold measure
  rw [busy, busy', recorded, call, cancelled]
  cases w.recorded <;> cases w.cancelled <;> cases w.call <;> simp <;> omega

theorem task_step (w : World) (inv : Inv w) (ht : w.task = true) :
    ∃ a, coven a = true ∧ enabled Rules.correct w a = true ∧
      (act Rules.correct w a).new = w.new ∧ (act Rules.correct w a).recorded = w.recorded ∧
      (act Rules.correct w a).call = w.call ∧
      (act Rules.correct w a).cancelled = w.cancelled ∧
      work (act Rules.correct w a) < work w := by
  have ⟨hop, hn⟩ := inv.task ht
  rcases hc : w.claim with _ | (_ | _ | _) <;> rcases hg : w.gone
  all_goals first
    | (refine ⟨.land, rfl, ?_⟩; simp [enabled, act, work, hc, hop, ht, hn]; done)
    | (refine ⟨.pass, rfl, ?_⟩; simp [enabled, act, pass, work, hc, hop, ht, hn, hg]; done)

set_option maxHeartbeats 1000000 in
theorem progress (w : World) (inv : Inv w) (busy : quiet w = false) :
    ∃ w', Moves w w' ∧ measure w' < measure w := by
  have go : ∀ a, coven a = true → enabled Rules.correct w a = true →
      measure (act Rules.correct w a) < measure w →
      ∃ w', Moves w w' ∧ measure w' < measure w :=
    fun a own e less => ⟨_, .step a own e (.refl _), less⟩
  have pub : w.new ≠ .published := by intro h; simp [quiet, h] at busy
  by_cases hrec : w.recorded = true
  · by_cases hcall : w.call = true
    · by_cases ht : w.task = true
      · obtain ⟨a, own, e, n, r, c, k, less⟩ := task_step w inv ht
        exact go a own e (measure_lt busy n r c k less)
      · have ht : w.task = false := by simpa using ht
        rcases hcan : w.cancelled
        · rcases inv.recorded hrec with hn | hn
          · rcases hop : w.op
            · exact go .register rfl
                (by simp [enabled, Rules.correct, running, hcall, hcan, hrec, ht, hop, hn])
                (measure_lt busy rfl rfl rfl rfl
                  (by rcases hc : w.claim with _ | (_ | _ | _) <;> simp [act, work, hop, hc, hn]))
            · exact go .register rfl
                (by simp [enabled, Rules.correct, running, hcall, hcan, hrec, ht, hop, hn])
                (measure_lt busy rfl rfl rfl rfl (by simp [act, work, hop, ht, hn]))
            · rcases hg : w.gone
              · refine go .swap rfl (by simp [enabled, running, hcall, hcan, hop, hn, hg]) ?_
                simp [act, measure, quiet, work, hrec, hcall, hcan, hop, hn]
              · refine go .conclude rfl (by simp [enabled, running, hcall, hcan, hop, hn, hg]) ?_
                simp [act, undo, measure, quiet, hrec, hcall, hcan, pub]
                omega
            · refine go .conclude rfl (by simp [enabled, running, hcall, hcan, hop, hn]) ?_
              simp [act, undo, measure, quiet, hrec, hcall, hcan, pub]
              omega
          · refine go .publish rfl (by simp [enabled, running, hcall, hcan, hn]) ?_
            simp [act, measure, quiet, hrec, hcall, hcan, pub]
            omega
        · have hn := (inv.cancelled hcan).2
          refine go .stop rfl (by simp [enabled, Rules.correct, hcall, hcan, ht]) ?_
          by_cases hu : undoable w = true
          · simp [act, Rules.correct, hu, undo, measure, quiet, hrec, hcall, hcan, pub]
            omega
          · simp [act, Rules.correct, hu, measure, quiet, work, hrec, hcall, hcan, hn]
    · have hcall : w.call = false := by simpa using hcall
      have hcan : w.cancelled = false := by
        rcases h : w.cancelled
        · rfl
        · simp [(inv.cancelled h).1] at hcall
      refine go .resume rfl (by simp [enabled, hcall, hrec]) ?_
      simp [act, measure, quiet, hcall, hcan, hrec, pub]
      exact (by omega : ∀ x, 1000 + x < 2000 + x) (work w)
  · have hrec : w.recorded = false := by simpa using hrec
    have hcall : w.call = true := by
      rcases h : w.call
      · simp [quiet, h, hrec] at busy
      · rfl
    have hop : w.op = .none := by
      rcases h : w.op
      · rfl
      all_goals (have := inv.op (by simp [h]); simp [hrec] at this)
    have ht : w.task = false := by
      rcases h : w.task
      · rfl
      · simp [(inv.task h).1] at hop
    rcases hcan : w.cancelled
    · exact go .record rfl (by simp [enabled, Rules.correct, running, hcall, hcan, hrec])
        (by simp [act, measure, quiet, hcall, hcan, hrec, pub, work, hop])
    · refine go .stop rfl (by simp [enabled, Rules.correct, hcall, hcan, ht]) ?_
      simp [act, Rules.correct, undoable, undo, measure, quiet, hrec, hcall, pub]
      omega

/-- Every replacement settles: from any reachable state, coven's own steps
and the app resuming what the store listing shows reach a state where the
new installation is published, or no call runs and nothing is recorded. -/
theorem settles {w : World} (h : Reachable Rules.correct w) :
    ∃ w', Moves w w' ∧ quiet w' = true := by
  generalize hm : measure w = m
  induction m using Nat.strongRecOn generalizing w with
  | ind m ih =>
    by_cases q : quiet w = true
    · exact ⟨w, .refl w, q⟩
    · obtain ⟨w', moves, less⟩ := progress w (replacement_reachable_inv h) (by simpa using q)
      obtain ⟨w'', more, quiet⟩ := ih (measure w') (hm ▸ less) (Steps.trans h moves.steps) rfl
      exact ⟨w'', moves.trans more, quiet⟩

/-- Where a replacement settles, the new installation is published with its
registration landed, or no registration of it can still land and history
doesn't name it as active. -/
theorem settled_outcome {w : World} (h : Reachable Rules.correct w) (q : quiet w = true) :
    (w.new = .published ∧ w.landed = true) ∨
      (w.claim = none ∧ (w.landed = false ∨ w.gone = true)) := by
  have inv := replacement_reachable_inv h
  by_cases pub : w.new = .published
  · exact .inl ⟨pub, (inv.published pub).1⟩
  right
  have nrec : w.recorded = false := by
    rcases hr : w.recorded <;> simp_all [quiet]
  have nop : w.op = .none := by
    rcases ho : w.op
    · rfl
    all_goals (have := inv.op (by simp [ho]); simp [nrec] at this)
  have nclaim : w.claim = none := by
    rcases hc : w.claim with _ | a
    · rfl
    · simp [inv.claim (by simp [hc])] at nop
  refine ⟨nclaim, ?_⟩
  rcases hl : w.landed
  · exact .inl rfl
  · rcases inv.landed hl with l | ap | p | n
    · simp [nclaim] at l
    · simp [nop] at ap
    · exact absurd p pub
    · exact .inr (inv.missing n hl)

end CovenSequence.DeviceReplacement
