import CovenStorelogData.Keys

namespace CovenStorelogData

/-- The restoration test in store_log_sync.rs::apply. Appendix C represents
a deleted circle by absence, so `none` corresponds to either no circle or a
deleted tombstone. Both cases fail Rust's `!previous.deleted` test. -/
def membershipReload (before after : CovenStorelog.State) (member : Nat) : Bool :=
  after.circles.any fun (circle, current) => member ∈ current.members &&
    (CovenStorelog.lookup before.circles circle).any (fun previous => member ∉ previous.members)

structure Reader (W Col : Type) where
  state : State W Col
  consumed : List W
  pendingReload : Bool

/-- Applying entries schedules a reload for schema/reset changes and the
membership transitions Rust recognizes. Rule recomputation itself is `observe`.
A skipped part has no merge state to restore merely by clearing its rule. -/
def receiveEntry {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (log : CovenStorelog.Log) (bound member : Nat)
    (reader : Reader W Col) (entry : Nat) : Reader W Col :=
  let next := step writes log bound reader.state (.entry entry)
  let before := reader.state.log.result.state
  let after := next.log.result.state
  { reader with state := next, pendingReload := reader.pendingReload ||
      before.versions != after.versions || before.resets != after.resets ||
      membershipReload before after member }

/-- The download boundary for an authenticated causal write whose entry past
is present. The header needs its store key. Any required unreadable part holds
the whole transaction back; otherwise opened parts merge and skipped parts
advance the same write position. Consumed positions are never applied again.
This models write_object.rs::parts and download.rs::apply/accepted together. -/
def receiveWrite {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (log : CovenStorelog.Log)
    (member device : Nat) (copies : Copies) (headerKey : Key) (keys : List Key)
    (reader : Reader W Col) (w : W) : Reader W Col :=
  if w ∈ reader.consumed || reader.pendingReload ||
      !running reader.state.log.result.state member device || (headerKey, member) ∉ copies ||
      keys.any (fun k => decide (part log reader.state.log.result copies k member = .wait))
  then reader else
    let readable := fun audience => keys.any fun k => k.audience == audience &&
      decide (part log reader.state.log.result copies k member = .apply)
    let projected := CovenMerge.project writes Row.audience readable
    { reader with
      consumed := reader.consumed ++ [w]
      state := { reader.state with data := CovenMerge.step projected reader.state.data w } }

theorem consumed_write_stays_skipped {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (log : CovenStorelog.Log)
    (member device : Nat) (copies : Copies) (headerKey : Key) (keys : List Key)
    (reader : Reader W Col) (w : W) (consumed : w ∈ reader.consumed) :
    receiveWrite writes log member device copies headerKey keys reader w = reader := by
  simp [receiveWrite, consumed]

end CovenStorelogData
