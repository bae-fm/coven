import CovenQueue.Verdicts

/-! Original queued records and their immutable preparation metadata.
Sequence claims and publication are modeled by CovenSequence. -/

namespace CovenQueue

structure Attempt where
  record : Record
  format : Nat
  /-- Header key followed by each part's key (D9). -/
  keys : List Nat
  deriving DecidableEq, Repr

inductive Pending where
  | untried (record : Record)
  | tried (attempt : Attempt)
  deriving DecidableEq, Repr

def Pending.record : Pending → Record
  | .untried r => r
  | .tried a => a.record

end CovenQueue
