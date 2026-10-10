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
  read : List WriteId
  storeRead : List Nat
  deriving DecidableEq, Repr

inductive Disposition where
  | ordinary
  | lost (version : Nat)
  | migration
  deriving DecidableEq, Repr

structure Record where
  identity : Identity
  version : Nat
  disposition : Disposition
  /-- Abstract canonical plaintext row records, including their audiences. -/
  body : List Nat
  deriving DecidableEq, Repr

/-- Own earlier numbers are read even though D5 omits them from `had_read`. -/
def reads (w a : Identity) : Bool :=
  a.id ∈ w.read || (w.id.device == a.id.device && a.id.number < w.id.number)

theorem own_earlier_read (w a : Identity)
    (hd : w.id.device = a.id.device) (hn : a.id.number < w.id.number) :
    reads w a = true := by simp [reads, hd, hn]

structure Boundary where
  entry : Nat
  /-- Authenticated positions of the chosen boundary snapshot (§15), not storage. -/
  covers : List WriteId
  deriving DecidableEq, Repr

structure Raise extends Boundary where
  version : Nat
  deriving DecidableEq, Repr

/-- Effects discarded when this boundary was adopted. These are retained
verdicts reconstructed from the author's store-log past and boundary inputs,
not extra fields in D5 and not dismissible loss values (§17.1). -/
structure Adoption where
  entry : Nat
  discarded : List WriteId
  deriving DecidableEq, Repr

/-- One audience's selected boundaries. Selection and store-log finality are
inputs; replay may replace this view and rebuild from retained records (§9). -/
structure View where
  raise : Option Raise
  reset : Option Boundary
  adoptions : List Adoption
  deriving DecidableEq, Repr

inductive Refusal where
  | notAuthorized
  | invalid
  deriving DecidableEq, Repr

inductive Verdict where
  | applied
  | excluded (entry : Nat)
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

/-- A passed discarded position is not a state input after adoption (§7.1,
§17.1, §19.3). This is per audience: other parts have their own view. -/
def usesDiscarded (w : Record) : Verdict → Bool
  | .excluded e | .ignored e => !(e ∈ w.identity.storeRead)
  | _ => false

def discardedBefore (view : View) (w a : Identity) : Bool :=
  view.adoptions.any fun b => b.entry ∈ w.storeRead && a.id ∈ b.discarded

def excludedInput (w : Record) (causes : List Verdict) : Option Nat :=
  causes.findSome? fun v => match v with
    | .excluded e => if e ∈ w.identity.storeRead then none else some e
    | _ => none

/-- Complete causal inputs are supplied by the receiver. A refused cause
stops the log before this function is called (§19.1). Resets take precedence
over schema loss in their audience (§17.1). -/
def classify (view : View) (w : CheckedWrite) (causes : List Verdict) : Verdict :=
  match w.refusal with
  | some r => .refused r
  | none =>
    match view.reset with
    | some b => if beforeReset b w.record then .ignored b.entry else schema
    | none => schema
where
  schema :=
    match view.raise with
    | some b => if outsideRaise b w.record then .excluded b.entry else inherited
    | none => inherited
  inherited :=
    match excludedInput w.record causes with
    | some e => .excluded e
    | none => .applied

end CovenQueue
