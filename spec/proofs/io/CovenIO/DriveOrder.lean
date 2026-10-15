/-! SP waiting and §17: the operation worker's order between a schema raise
holding the writer slot and a pending reload (`operations.rs` `drive`).

A queued entry's publication waits while a reload is pending, so a raise whose
own queued entry has not published waits on that reload (its recorded
condition names the reload operation). A raise snapshot also waits for its own
captured prefix, such as its migration marker, to be stored: a device-log
write (its recorded condition names the write's object). Only step 6 sends or
receives writes, and step 6 runs only once no reload is pending, so that wait
is a wait on the reload too (`write_waits_on_reload`).

The worker used to skip every reload while a raise held the writer slot: then
the raise waited on the reload and the reload waited behind the raise
(`old_cycle`). The next rule yielded only to a condition naming the reload, so
a raise waiting on its write still kept the reload back while step 6 waited
for it (`named_only_cycle`). Now a raise whose wait a pending reload holds
back yields: the reload runs (`no_cycle`), and a raise that waits on nothing
else keeps its place ahead of reloads (`raise_keeps_its_place`).

In the code the raise's wait comes from its recorded pending condition
(`operation_pending.rs` `waits_on_reload`): `Waits(Reload(_))` is `.reload`,
`Waits(Object(path))` with a device-log write path is `.write`. -/

namespace CovenIO.DriveOrder

/-- What the raise holding the writer slot, if any, waits on. -/
inductive RaiseWait
  | ready
  /-- Its recorded condition names the reload. -/
  | reload
  /-- Its recorded condition names a device-log write object. -/
  | write
  | other
  deriving DecidableEq, Repr

structure Drive where
  /-- A raise holds the writer slot. -/
  raiseHolds : Bool
  raiseWaits : RaiseWait
  reloadPending : Bool
  deriving DecidableEq, Repr

/-- Step 6, the only step that sends or receives device-log writes, runs only
once no reload is pending (SP). -/
def stepSixRuns (d : Drive) : Bool := !d.reloadPending

/-- A wait that a pending reload holds back: the reload itself, or a write
only step 6 can move. -/
def heldByReload : RaiseWait → Bool
  | .reload => true
  | .write => true
  | _ => false

/-- A write wait can move only through step 6, which a pending reload holds
back: it is a wait on the reload. -/
theorem write_waits_on_reload (d : Drive) (pending : d.reloadPending = true) :
    stepSixRuns d = false ∧ heldByReload .write = true := by
  simp [stepSixRuns, heldByReload, pending]

/-- The raise goes before reloads only while it holds the slot and a pending
reload does not hold its wait back. -/
def raisingFirst (d : Drive) : Bool := d.raiseHolds && !heldByReload d.raiseWaits

/-- A pending reload runs unless a raise goes first. -/
def reloadRuns (d : Drive) : Bool := d.reloadPending && !raisingFirst d

/-- No mutual wait: whenever a reload is pending and it holds the raise's wait
back, whether that wait names the reload or a write, the reload runs. -/
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

end CovenIO.DriveOrder
