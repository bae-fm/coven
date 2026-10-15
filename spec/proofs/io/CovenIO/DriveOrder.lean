/-! SP waiting and §17: the operation worker's order between a schema raise
holding the writer slot and a pending reload (`operations.rs` `drive`).

A queued entry's publication waits while a reload is pending, so a raise whose
own queued entry has not published waits on that reload (its recorded
condition names the reload operation). The worker used to skip every reload
while a raise held the writer slot: then the raise waited on the reload and the
reload waited behind the raise (`old_cycle`). Now a raise that waits on a
reload yields: the reload runs (`no_cycle`), and a raise that waits on nothing
else keeps its place ahead of reloads (`raise_keeps_its_place`). -/

namespace CovenIO.DriveOrder

/-- What the raise holding the writer slot, if any, waits on. -/
inductive RaiseWait
  | ready
  | reload
  | other
  deriving DecidableEq, Repr

structure Drive where
  /-- A raise holds the writer slot. -/
  raiseHolds : Bool
  raiseWaits : RaiseWait
  reloadPending : Bool
  deriving DecidableEq, Repr

/-- The raise goes before reloads only while it holds the slot and waits on no
reload. -/
def raisingFirst (d : Drive) : Bool := d.raiseHolds && d.raiseWaits != .reload

/-- A pending reload runs unless a raise goes first. -/
def reloadRuns (d : Drive) : Bool := d.reloadPending && !raisingFirst d

/-- No mutual wait: whenever a reload is pending and the raise waits on it,
the reload runs. -/
theorem no_cycle (d : Drive) (pending : d.reloadPending = true)
    (waits : d.raiseWaits = .reload) : reloadRuns d = true := by
  simp [reloadRuns, raisingFirst, pending, waits]

/-- A raise waiting on nothing else, or on something other than a reload,
still goes before reloads. -/
theorem raise_keeps_its_place (d : Drive) (holds : d.raiseHolds = true)
    (waits : d.raiseWaits ≠ .reload) : reloadRuns d = false := by
  cases d with
  | mk h w p => cases w <;> simp_all [reloadRuns, raisingFirst]

/-- The replaced rule skipped every reload while a raise held the slot: the
raise waiting on the reload and the reload skipped behind it. -/
def oldReloadRuns (d : Drive) : Bool := d.reloadPending && !d.raiseHolds

theorem old_cycle :
    oldReloadRuns ⟨true, .reload, true⟩ = false ∧ reloadRuns ⟨true, .reload, true⟩ = true := by
  decide

end CovenIO.DriveOrder
