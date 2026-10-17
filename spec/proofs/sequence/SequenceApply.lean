import CovenSequence.Authority
import CovenSequence.LiveClaims
import Lean

/-! Concrete inputs at the model's abstract merge boundary. Row updates are
merged presence, value and input setters, before uniqueness removes losers. -/
namespace SequenceApply
open CovenSequence Lean

structure Row where
  id : Nat
  live : Bool
  value : Option Nat
  inputs : List LiveClaims.Position

structure Batch where
  writes : List Nat
  rows : List Row

/-- The two permanent claim namespaces cannot collide. -/
abbrev Key := Sum Nat Nat

abbrev sig (members : Nat) : Sig where
  Dev := Nat
  Mem := Fin members
  Key := Key
  Path := Unit
  Batch := Batch
  Rows := List Row
  Op := Empty
  Aux := Unit
  Aud := Nat

structure Audience where
  id : Nat
  version : Nat
  writers : List Nat

structure Config (n : Nat) where
  members : List (Fin n × Role × Nat)
  devices : List (Nat × Fin n)
  audiences : List Audience

/-- The abstract merge receives complete replacements for affected merged
rows. Losing rows remain present so LiveClaims can select them after release. -/
def merge (rows : List Row) (batch : Batch) : List Row :=
  batch.rows.foldl (fun rs r => rs.filter (fun old => old.id != r.id) ++ [r]) rows

def initialLedger (c : Config n) : Ledger (sig n) where
  member := fun m => c.members.any (fun x => x.1 == m)
  role := fun m => match c.members.find? (fun x => x.1 == m) with
    | some x => x.2.1
    | none => .member
  deviceOf := fun d => (c.devices.find? (fun x => x.1 == d)).map (·.2)
  removed := fun _ => false
  keyGen := fun m => match c.members.find? (fun x => x.1 == m) with
    | some x => x.2.2
    | none => 0
  aux := ()
  copies := []
  refused := fun _ => false
  claims := fun _ => 0

def initialAudience (version : Nat) : AudLedger (sig n) :=
  ⟨fun _ => none, [], 0, 0, 0, [], version, fun _ => false⟩

def initialAudiences (c : Config n) (a : Nat) : AudLedger (sig n) :=
  initialAudience (match c.audiences.find? (fun x => x.id == a) with
    | some x => x.version
    | none => 0)

def baseRules (c : Config n) (numbers : Nat → Nat) : Rules (sig n) where
  authorized := fun L d _ => Authority.isAdmin L d
  applyOp := fun _ op => nomatch op
  copyOk := fun _ _ _ _ => false
  mayWrite := fun _ d a => c.audiences.any (fun x => x.id == a && decide (d ∈ x.writers))
  merge := merge
  convert := fun _ _ b => b
  migrate := fun _ r => r
  resetRows := fun _ => []
  boundaryAllowed := fun _ _ _ _ => false
  sinceReset := fun _ b => b
  number := fun k _ => numbers k
  ledger0 := initialLedger c
  aud0 := initialAudiences c
  rows0 := fun _ => []

def hasAdmin (L : Ledger (sig n)) : Decidable (Authority.HasAdmin L) :=
  decidable_of_iff ((List.finRange n).any (fun m => L.member m && decide (L.role m = .admin)) = true)
    (by simp [Authority.HasAdmin, List.any_eq_true])

def rules (c : Config n) (numbers : Nat → Nat) : Rules (sig n) :=
  Authority.withRoles (baseRules c numbers) hasAdmin

def sorted (xs : List Nat) : List Nat := xs.eraseDups.mergeSort (fun a b => a ≤ b)

def roleJson : Role → Json
  | .admin => toJson "admin"
  | .member => toJson "member"

def ledgerJson (L : Ledger (sig n)) (devices : List Nat) (through : Nat) : Json :=
  Json.mkObj [
    ("members", toJson ((List.finRange n).filterMap fun m =>
      if L.member m then some (Json.arr #[toJson m.val, roleJson (L.role m)]) else none)),
    ("devices", toJson (devices.filterMap fun d =>
      (L.deviceOf d).map fun m => toJson [d, m.val])),
    ("removed", toJson (devices.filter L.removed)),
    ("key_generations", toJson ((List.finRange n).map fun m => [m.val, L.keyGen m])),
    ("last_claims", toJson (devices.filterMap fun d =>
      if L.claims d = 0 then none else some [d, L.claims d])),
    ("refused", toJson ((List.range through).map (· + 1) |>.filter L.refused))]

def rowJson (r : Row) : Json := Json.mkObj [
  ("row", toJson r.id), ("live", toJson r.live), ("value", toJson r.value),
  ("inputs", toJson (r.inputs.map fun (s, w) => [s, w]))]

def audienceJson (entries : List (Nat × Slot (sig n))) (a through : Nat)
    (P : AudLedger (sig n)) (rows : List Row) : Json := Id.run do
  let slots := (List.range through).map (· + 1)
  let parts := (entries.take through).filterMap fun (_, s) => match s with
    | .writes _ _ parts => parts a
    | _ => none
  let keys := parts.flatMap (·.claims)
  let uniques := sorted (keys.filterMap fun k => match k with | .inl v => some v | _ => none)
  let owners := sorted (keys.filterMap fun k => match k with | .inr r => some r | _ => none)
  let rows := rows.mergeSort (fun a b => a.id ≤ b.id)
  let inputs : LiveClaims.Inputs Row Nat := {
    rows, live := Row.live, value := Row.value, inputs := Row.inputs, keyRank := Row.id
    counted := fun (k, w) =>
      if 0 < k && k ≤ through && !P.refused k then
        match entries[k - 1]? with
        | some (_, .writes _ _ ps) => match ps a with
          | some p => decide (w ∈ p.batch.writes)
          | none => false
        | _ => false
      else false }
  let values := sorted (rows.filterMap (·.value))
  return Json.mkObj [
    ("audience", toJson a), ("version", toJson P.version),
    ("unique_claims", toJson (uniques.filterMap fun v => (P.owner (.inl v)).map fun k => [v, k])),
    ("owner_claims", toJson (owners.filterMap fun r => (P.owner (.inr r)).map fun k => [r, k])),
    ("live_winners", toJson (values.map fun v => Json.arr #[toJson v,
      toJson ((LiveClaims.winner inputs v).map (·.id))])),
    ("rows", toJson (rows.map rowJson)),
    ("refused", toJson (slots.filter P.refused))]

/-- Each prefix uses the Model definitions, including refusal-only effects and
claim recording for refused slots. The extension outside 1..length is never
read; it reuses an actual slot instead of inventing a sentinel operation. -/
def run (c : Config n) (entries : List (Nat × Slot (sig n))) : Json := Id.run do
  let devices := sorted (c.devices.map (·.1) ++ entries.flatMap fun (_, s) =>
    author s :: match s with
      | .admin _ (.addDevice d _) | .admin _ (.removeDevice d) => [d]
      | _ => [])
  let audiences := c.audiences.mergeSort (fun a b => a.id ≤ b.id)
  let state : Nat → Ledger (sig n) × (Nat → AudLedger (sig n)) × (Nat → List Row) :=
    match entries with
    | [] => fun _ => (initialLedger c, initialAudiences c, fun _ => [])
    | first :: _ =>
      let seq := fun k => ((entries[k - 1]?).getD first).2
      let R := rules c (fun k => ((entries[k - 1]?).getD first).1)
      fun k => (ledgerAt R seq k, fun a => audAt R seq a k, fun a => rowsAt R seq a k)
  let fields := fun k =>
    let (L, audiencesAt, rowsAt) := state k
    [("ledger", ledgerJson L devices k),
     ("audiences", toJson (audiences.map fun a =>
       audienceJson entries a.id k (audiencesAt a.id) (rowsAt a.id)))]
  return Json.mkObj (fields entries.length ++ [
    ("slots", toJson ((List.range entries.length).map fun k =>
      Json.mkObj (("slot", toJson (k + 1)) :: fields (k + 1))))])

end SequenceApply
