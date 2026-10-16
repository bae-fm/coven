/-! SP waiting and §17: the operation worker's order between a schema raise
holding the writer slot and a pending reload (`operations.rs` `drive`).

A queued entry's publication waits while a reload is pending, so a raise whose
own queued entry has not published waits on that reload (its recorded
condition names the reload operation). A raise snapshot also waits for its own
captured prefix, such as its migration marker, to be stored: a device-log
write (its recorded condition names the write's object). In a pass it is
prepared only after step 6's write catch-up (§15): before it, its condition
names a catch-up. Only step 6 sends or receives writes and catches up on them,
and step 6 runs only once no reload is pending, so both waits are waits on the
reload too (`step_six_waits_on_reload`).

The worker used to skip every reload while a raise held the writer slot: then
the raise waited on the reload and the reload waited behind the raise
(`old_cycle`). The next rule yielded only to a condition naming the reload, so
a raise waiting on its write still kept the reload back while step 6 waited
for it (`named_only_cycle`); the one after it also yielded to a write, but not
to a raise preparing after the catch-up (`write_only_cycle`). Now a raise
whose wait a pending reload holds back yields: the reload runs (`no_cycle`),
and a raise that waits on nothing else keeps its place ahead of reloads
(`raise_keeps_its_place`).

In the code the raise's wait comes from its recorded pending condition
(`operation_pending.rs` `waits_on_reload`), read when the drive reaches the
reload: `Waits(Reload(_))` is `.reload`, `Waits(Object(path))` with a
device-log write path is `.write`, and `Waits(CatchUp)` is `.catchUp`. -/

namespace CovenIO.DriveOrder

/-- What the raise holding the writer slot, if any, waits on. -/
inductive RaiseWait
  | ready
  /-- Its recorded condition names the reload. -/
  | reload
  /-- Its recorded condition names a device-log write object. -/
  | write
  /-- Its recorded condition names a catch-up its preparation follows. -/
  | catchUp
  | other
  deriving DecidableEq, Repr

structure Drive where
  /-- A raise holds the writer slot. -/
  raiseHolds : Bool
  raiseWaits : RaiseWait
  reloadPending : Bool
  deriving DecidableEq, Repr

/-- Step 6, the only step that sends or receives device-log writes and catches
up on them, runs only once no reload is pending (SP). -/
def stepSixRuns (d : Drive) : Bool := !d.reloadPending

/-- A wait only step 6 can end. -/
def needsStepSix : RaiseWait → Bool
  | .write => true
  | .catchUp => true
  | _ => false

/-- A wait that a pending reload holds back: the reload itself, or one only
step 6 can end. -/
def heldByReload (w : RaiseWait) : Bool := w == .reload || needsStepSix w

/-- A wait only step 6 can end cannot move while a reload is pending: it is a
wait on the reload. -/
theorem step_six_waits_on_reload (d : Drive) (pending : d.reloadPending = true)
    (w : RaiseWait) (needs : needsStepSix w = true) :
    stepSixRuns d = false ∧ heldByReload w = true := by
  simp [stepSixRuns, heldByReload, pending, needs]

/-- The raise goes before reloads only while it holds the slot and a pending
reload does not hold its wait back. -/
def raisingFirst (d : Drive) : Bool := d.raiseHolds && !heldByReload d.raiseWaits

/-- A pending reload runs unless a raise goes first. -/
def reloadRuns (d : Drive) : Bool := d.reloadPending && !raisingFirst d

/-- No mutual wait: whenever a reload is pending and it holds the raise's wait
back, whether that wait names the reload, a write or a catch-up, the reload
runs. -/
theorem no_cycle (d : Drive) (pending : d.reloadPending = true)
    (waits : heldByReload d.raiseWaits = true) : reloadRuns d = true := by
  simp [reloadRuns, raisingFirst, pending, waits]

/-- A raise waiting on nothing, or on something a pending reload does not hold
back, still goes before reloads. -/
theorem raise_keeps_its_place (d : Drive) (holds : d.raiseHolds = true)
    (waits : heldByReload d.raiseWaits = false) : reloadRuns d = false := by
  simp [reloadRuns, raisingFirst, holds, waits]

/-- The first replaced rule skipped every reload while a raise held the slot:
the raise waiting on the reload and the reload skipped behind it. -/
def oldReloadRuns (d : Drive) : Bool := d.reloadPending && !d.raiseHolds

theorem old_cycle :
    oldReloadRuns ⟨true, .reload, true⟩ = false ∧ reloadRuns ⟨true, .reload, true⟩ = true := by
  decide

/-- The second replaced rule yielded only to a condition naming the reload. -/
def namedOnlyReloadRuns (d : Drive) : Bool :=
  d.reloadPending && !(d.raiseHolds && d.raiseWaits != .reload)

/-- Under it a raise waiting on its write kept the reload back while step 6
waited for the reload, so neither moved; now the reload runs. -/
theorem named_only_cycle :
    namedOnlyReloadRuns ⟨true, .write, true⟩ = false ∧
      stepSixRuns ⟨true, .write, true⟩ = false ∧
      reloadRuns ⟨true, .write, true⟩ = true := by
  decide

/-- The third replaced rule yielded to the reload or a write. -/
def writeOnlyReloadRuns (d : Drive) : Bool :=
  d.reloadPending && !(d.raiseHolds && d.raiseWaits != .reload && d.raiseWaits != .write)

/-- Under it a raise preparing after step 6's catch-up kept the reload back
while step 6 waited for the reload; now the reload runs. -/
theorem write_only_cycle :
    writeOnlyReloadRuns ⟨true, .catchUp, true⟩ = false ∧
      stepSixRuns ⟨true, .catchUp, true⟩ = false ∧
      reloadRuns ⟨true, .catchUp, true⟩ = true := by
  decide

end CovenIO.DriveOrder
