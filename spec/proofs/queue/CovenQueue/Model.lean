import CovenMerge.Boundary
import Std

/-! Queue identities and audience verdicts: coven §6, §7, §17.1, §19.3;
format D5. Natural numbers abstract bounded identifiers and timestamps.
The history index is timestamp order, separate from a device's write number. -/

namespace CovenQueue

structure WriteId where
  device : Nat
  number : Nat
  deriving DecidableEq, Repr

/-- Metadata conversion is forbidden to change (§17.1, E13). -/
structure Identity where
  id : WriteId
  timestamp : Nat
  /-- Expanded causal past, derived from the slot prefix and own predecessors. -/
  read : List WriteId
  /-- Applied admin slots in that prefix; not a separate stored frontier. -/
  storeRead : List Nat
  deriving DecidableEq, Repr

inductive Disposition where
  | ordinary
  | lost (version : Nat)
  | migration
  deriving DecidableEq, Repr

/-- Row identity, generation and data sources retain their authored meaning
through conversion. Values are abstract plaintext scalars in the old shape
when frozen. A lost delete may have no scalar at all. -/
inductive ChangeContent where
  | values (written : List Nat)
  | frozen (cause : CovenMerge.LossCause Nat) (written : List Nat)
  deriving DecidableEq, Repr

structure RowChange where
  row : Nat
  generation : Nat
  operation : CovenMerge.Kind
  provenance : CovenMerge.DataDependency.Provenance Nat Nat
  content : ChangeContent
  deriving DecidableEq, Repr

def RowChange.values (ch : RowChange) : List Nat :=
  match ch.content with
  | .values xs | .frozen _ xs => xs

def RowChange.frozen (ch : RowChange) : Bool :=
  match ch.content with
  | .values _ => false
  | .frozen _ _ => true

structure Record where
  identity : Identity
  version : Nat
  disposition : Disposition
  /-- A row includes its table, key and audience. Each appears at most once. -/
  body : List RowChange
  deriving DecidableEq, Repr

/-- Own earlier numbers are read in addition to the expanded slot prefix. -/
def reads (w a : Identity) : Bool :=
  a.id ∈ w.read || (w.id.device == a.id.device && a.id.number < w.id.number)

theorem own_earlier_read (w a : Identity)
    (hd : w.id.device = a.id.device) (hn : a.id.number < w.id.number) :
    reads w a = true := by simp [reads, hd, hn]

structure Boundary where
  entry : Nat
  /-- Writes in the chosen snapshot's covered slot prefix (§15). -/
  covers : List WriteId
  deriving DecidableEq, Repr

structure Raise extends Boundary where
  version : Nat
  deriving DecidableEq, Repr

/-- One audience's boundaries selected by its applied sequence prefix.
The sequence model proves selection; this model interprets the supplied view. -/
structure View where
  raise : Option Raise
  reset : Option Boundary
  /-- Per-change verdicts retained with the selected snapshot, separate from
  dismissible losses. Keys use the history's fixed write enumeration. -/
  drops : List (Nat × Nat × CovenMerge.LossCause Nat)
  deriving DecidableEq, Repr

def snapshotLoss (v : View) (w r : Nat) : Option (CovenMerge.LossCause Nat) :=
  ((v.drops.find? fun x => x.1 == w && x.2.1 == r).map fun x => x.2.2)

inductive Refusal where
  | notAuthorized
  | invalid
  deriving DecidableEq, Repr

inductive Verdict where
  | applied
  | excluded (entry version : Nat)
  | ignored (entry : Nat)
  | refused (reason : Refusal)
  deriving DecidableEq, Repr

structure CheckedWrite where
  record : Record
  /-- Complete immutable-object checks and author-view authority (§7.1, §19.1).
  Unknown registrations and unreadable bytes are waits, not values here. -/
  refusal : Option Refusal
  deriving DecidableEq, Repr

def beforeReset (b : Boundary) (w : Record) : Bool :=
  !(w.identity.id ∈ b.covers) && !(b.entry ∈ w.identity.storeRead)

def outsideRaise (b : Raise) (w : Record) : Bool :=
  !(w.identity.id ∈ b.covers) && (w.version < b.version ||
    match w.disposition with | .lost _ => true | _ => false)

/-- Whole-write admission examines only this write's own authority, selected
reset and schema version/coverage. Data loss is decided per row change by
`DataDependency`, after this decision. Recorded write reads never exclude it. -/
def classify (view : View) (w : CheckedWrite) : Verdict :=
  match w.refusal with
  | some r => .refused r
  | none =>
    match view.reset with
    | some b => if beforeReset b w.record then .ignored b.entry else schema
    | none => schema
where
  schema :=
    match view.raise with
    | some b => if outsideRaise b w.record then .excluded b.entry b.version else .applied
    | none => .applied

theorem recorded_reads_do_not_exclude (v : View) (w : CheckedWrite) (frontier : List WriteId) :
    classify v { w with record := { w.record with
      identity := { w.record.identity with read := frontier } } } = classify v w := rfl

end CovenQueue
