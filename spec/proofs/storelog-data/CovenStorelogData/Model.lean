import CovenMerge.Audience
import CovenStorelog.ExamplesSupport

/-! The boundary between replay and data. Entries never undo ordinary writes.
The citations and abstraction boundaries are in ../../storelog-data.md. -/

namespace CovenStorelogData

open CovenStorelog (Audience)

/-- A row's identity includes its audience (§14.2). -/
structure Row where
  table : Nat
  key : Nat
  audience : Audience
  deriving DecidableEq, Repr

/-- A signature's member and its device, with the write's immutable log past. -/
structure Author where
  member : Nat
  device : Nat
  entries : List Nat
  deriving DecidableEq, Repr

/-- §7.1 and write_object.rs::authority: judge the recorded past, not the
receiver's current membership or the current disposition of an addition. -/
def writeAuthority (log : CovenStorelog.Log) (bound : Nat) (a : Author) : Bool :=
  let view := (CovenStorelog.resolve log bound (CovenStorelog.entrySet a.entries)).state
  CovenStorelog.member view a.member &&
    CovenStorelog.lookup view.devices a.device == some a.member

/-- Appendix C erases deleted circles; Rust retains their tombstones.
A kept creation and an absent active circle recover that distinction.
A dropped creation alone is not a deleted-circle tombstone. -/
def deletedCircle (log : CovenStorelog.Log) (result : CovenStorelog.Result)
    (circle : Nat) : Bool :=
  result.kept.any (fun e => match (log e).action with
    | .makeCircle c _ => c == circle
    | _ => false) && (CovenStorelog.lookup result.state.circles circle).isNone

/-- Schema-derived rules keep Appendix B's inputs. This development supplies
the replay-dependent rule and the other-audience claims itself. -/
structure Schema (W Col K : Type) where
  rows : List Row
  refs : CovenMerge.St W Row Col → Row → List (CovenMerge.Ref Row)
  checkFails : CovenMerge.St W Row Col → Row → Bool
  claims : CovenMerge.St W Row Col → Row → List (CovenMerge.Claim K)

/-- Unique claims and primary keys in different audiences are different
namespaces; neither can accidentally compete with the other. -/
inductive ClaimKey (K : Type) where
  | unique (table : Nat) (audience : Audience) (key : K)
  | row (table key : Nat)
  deriving DecidableEq

def inputs {W Col K : Type} (schema : Schema W Col K)
    (writes : CovenMerge.Writes W Row Col) (log : CovenStorelog.Log)
    (result : CovenStorelog.Result) (st : CovenMerge.St W Row Col) :
    CovenMerge.Inputs Row (ClaimKey K) where
  rows := schema.rows
  present r := decide (st.gen r % 2 = 1)
  refs := schema.refs st
  checkFails := schema.checkFails st
  inDeletedCircle r := match r.audience with
    | .store => false
    | .circle c => deletedCircle log result c
  claims r :=
    let unique := (schema.claims st r).map fun c =>
      { c with key := ClaimKey.unique r.table r.audience c.key, other := false }
    let other := match st.genWrite r (st.gen r) with
      | none => []
      | some w =>
        let stamp := match r.audience with
          | .store => 0
          | .circle _ => writes.ts w + 1
        [⟨⟨[], none⟩, ClaimKey.row r.table r.key, stamp, true⟩]
    unique ++ other
  rank r := r.key

/-- Both existing states are reused, without a second merge or replay. -/
structure State (W Col : Type) where
  log : CovenStorelog.Device
  data : CovenMerge.St W Row Col

def initial {W Col : Type} (log : CovenStorelog.Log) (bound : Nat) : State W Col :=
  ⟨CovenStorelog.initial log bound, CovenMerge.St.init⟩

inductive Event (W : Type) where
  | entry (id : Nat)
  | write (id : W)
  deriving DecidableEq, Repr

/-- Entry application recomputes the entire log; data writes remain ordinary
merge steps. The derived app view below is recomputed from both atomically. -/
def step {W Col : Type} [DecidableEq W] (writes : CovenMerge.Writes W Row Col)
    (log : CovenStorelog.Log) (bound : Nat) (s : State W Col) : Event W → State W Col
  | .entry e => { s with log := CovenStorelog.step log bound s.log e }
  | .write w => { s with data := CovenMerge.step writes s.data w }

def observe {W Col K : Type} [DecidableEq W] [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (log : CovenStorelog.Log) (s : State W Col) : CovenMerge.Device W Row Col :=
  let v := CovenMerge.view (inputs schema writes log s.log.result s.data)
  ⟨s.data, v, CovenMerge.lossRecord s.data v⟩

def entries {W : Type} (events : List (Event W)) : List Nat :=
  events.filterMap fun e => match e with | .entry n => some n | .write _ => none

def applied {W : Type} (events : List (Event W)) : List W :=
  events.filterMap fun e => match e with | .write w => some w | .entry _ => none

/-- Readiness also requires the whole recorded log past, including dropped
entries. Key readiness is modeled separately in Keys.lean. -/
def Ready {W Col : Type} [DecidableEq W] (writes : CovenMerge.Writes W Row Col)
    (log : CovenStorelog.Log) (bound : Nat) (authors : W → Author)
    (events : List (Event W)) : Event W → Prop
  | .entry e => e < bound ∧ e ∉ entries events ∧
      ∀ a, CovenStorelog.hadRead log e a = true → a ∈ entries events
  | .write w => w ∉ applied events ∧
      (∀ a, writes.past w a = true → a ∈ applied events) ∧
      (∀ a ∈ (authors w).entries, a ∈ entries events) ∧
      CovenStorelog.Closed log (CovenStorelog.entrySet (authors w).entries) ∧
      writeAuthority log bound (authors w) = true

inductive History {W Col : Type} [DecidableEq W] (writes : CovenMerge.Writes W Row Col)
    (log : CovenStorelog.Log) (bound : Nat) (authors : W → Author) : List (Event W) → Prop
  | nil : History writes log bound authors []
  | snoc {events : List (Event W)} {e : Event W} : History writes log bound authors events →
      Ready writes log bound authors events e → History writes log bound authors (events ++ [e])

theorem history_orders {W Col : Type} [DecidableEq W]
    {writes : CovenMerge.Writes W Row Col} {log : CovenStorelog.Log} {bound : Nat}
    {authors : W → Author} {events : List (Event W)}
    (h : History writes log bound authors events) :
    CovenStorelog.CausalOrder log (entries events) ∧
      CovenMerge.CausalOrder writes (applied events) := by
  induction h with
  | nil => exact ⟨.nil, .nil⟩
  | @snoc es e _ ready ih =>
    cases e with
    | entry n =>
      obtain ⟨_, fresh, past⟩ := ready
      simpa [entries, applied, List.filterMap_append] using
        And.intro (CovenStorelog.CausalOrder.snoc ih.1 fresh past) ih.2
    | write w =>
      obtain ⟨fresh, past, _⟩ := ready
      simpa [entries, applied, List.filterMap_append] using
        And.intro ih.1 (CovenMerge.CausalOrder.snoc ih.2 fresh past)

end CovenStorelogData
