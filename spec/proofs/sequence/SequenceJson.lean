import SequenceApply
import SequenceInput
import SequenceKeys
import CovenSequence.Settle
import CovenSequence.Causality
import CovenSequence.Reload

/-! One case per process; malformed inputs fail on stderr with nonzero exit. -/
namespace SequenceJson
open CovenSequence Lean
open SequenceApply (sig Config Audience Row)
open SequenceInput

def readRole (j : Json) : Except String Role := do
  match ← j.getObjValAs? String "role" with
  | "admin" => return .admin
  | "member" => return .member
  | _ => throw "unknown role"

def member (n : Nat) (j : Json) : Except String (Fin n) := do
  let m ← nat j "member"
  if h : m < n then return ⟨m, h⟩ else throw "member outside member_count"

def readConfig (n : Nat) (j : Json) : Except String (Config n) := do
  let initial ← j.getObjVal? "initial"
  let members ← (← initial.getObjValAs? (List Json) "members").mapM fun m => do
    return (← member n m, ← readRole m, ← nat m "key_gen")
  distinct (members.map (·.1)) "initial member"
  if !members.any (fun m => m.2.1 == .admin) then throw "initial membership must contain an admin"
  let devices ← (← initial.getObjValAs? (List Json) "devices").mapM fun d => do
    let m ← member n d
    if !members.any (fun x => x.1 == m) then throw "initial device has no active member"
    return (← nat d "device", m)
  distinct (devices.map (·.1)) "initial device"
  let audiences ← (← j.getObjValAs? (List Json) "audiences").mapM fun a => do
    let writers ← a.getObjValAs? (List Nat) "writers"
    distinct writers "audience writer"
    return (⟨← nat a "audience", ← nat a "version", writers⟩ : Audience)
  distinct (audiences.map (·.id)) "audience"
  return ⟨members, devices, audiences⟩

def readEntry (n : Nat) (j : Json) : Except String (Entry (sig n)) := do
  match ← j.getObjValAs? String "kind" with
  | "addMember" => return .addMember (← member n j) (← readRole j)
  | "removeMember" => return .removeMember (← member n j)
  | "changeRole" => return .changeRole (← member n j) (← readRole j)
  | "addDevice" => return .addDevice (← nat j "device") (← member n j)
  | "removeDevice" => return .removeDevice (← nat j "device")
  | "replaceKey" => return .replaceKey (← member n j)
  | _ => throw "unknown admin entry"

def readRow (k : Nat) (j : Json) : Except String Row := do
  let inputs ← (← j.getObjValAs? (List Json) "inputs").mapM pair
  for (s, w) in inputs do
    if s = 0 || s > k || w = 0 then throw "row setter must name a positive write at or before its slot"
  distinct inputs "row input setter"
  return ⟨← nat j "row", ← j.getObjValAs? Bool "live",
    ← j.getObjValAs? (Option Nat) "value", inputs⟩

def readPart (c : Config n) (k : Nat) (j : Json) : Except String (Nat × WPart (sig n)) := do
  let a ← nat j "audience"
  if !c.audiences.any (fun x => x.id == a) then throw "part has undeclared audience"
  let uniques ← j.getObjValAs? (List Nat) "unique_claims"
  let owners ← (← j.getObjValAs? (List Json) "owner_creations").mapM pair
  distinct uniques "unique claim"
  distinct owners "owner creation"
  let writes ← j.getObjValAs? (List Nat) "writes"
  distinct writes "write number"
  if writes.any (· == 0) then throw "write numbers must be positive"
  for (_, w) in owners do
    if w ∉ writes then throw "owner creation names no write in its part"
  let rows ← (← j.getObjValAs? (List Json) "rows").mapM (readRow k)
  distinct (rows.map (·.id)) "row update"
  return (a, ⟨← nat j "version", uniques.map Sum.inl ++ (owners.map (fun p => Sum.inr p.1)).eraseDups, ⟨writes, rows, owners⟩⟩)

def readSlot (c : Config n) (k : Nat) (j : Json) : Except String (ClaimRecord String × Slot (sig n)) := do
  let claim ← positive j "claim"
  let d ← nat j "device"
  let slot ← match ← j.getObjValAs? String "kind" with
    | "admin" => pure (.admin d (← readEntry n (← j.getObjVal? "entry")))
    | "writes" => do
      let parts ← (← j.getObjValAs? (List Json) "parts").mapM (readPart c k)
      distinct (parts.map (·.1)) "part audience"
      pure (.writes d (← nat j "key_gen") (fun a => (parts.find? (fun p => p.1 == a)).map (·.2)))
    | _ => throw "unknown slot kind"
  return (⟨claim, ← j.getObjValAs? String "digest"⟩, slot)

def readApply (j : Json) : Except String Json := do
  let n ← nat j "member_count"
  let c ← readConfig n j
  let raw ← j.getObjValAs? (List Json) "slots"
  let entries ← raw.zipIdx.mapM fun (s, k) => readSlot c (k + 1) s
  for (_, x) in entries do
    match x with
    | .writes _ _ parts =>
      for a in c.audiences do
        if let some p := parts a.id then
          for row in p.batch.rows do
            for (s, w) in row.inputs do
              let some (_, source) := entries[s - 1]? | throw "row setter outside history"
              match source with
              | .writes _ _ ps =>
                let some sourcePart := ps a.id | throw "row setter has no part for its audience"
                if w ∉ sourcePart.batch.writes then throw "row setter names no write in its part"
              | _ => throw "row setter does not name a writes slot"
    | _ => pure ()
  return SequenceApply.run c entries

/-- Metadata is authenticated outside this runner; digest strings are SHA-256
tokens supplied by the caller, not hashes of JSON serialization. -/
def readClaim (j : Json) : Except String (Nat × Nat × String) := do
  return (← nat j "device", ← positive j "number", ← j.getObjValAs? String "bytes")

def readClaimRecord (j : Json) : Except String (Nat × ClaimRecord String) := do
  match (← j.getArr?).toList with
  | [d, n, h] =>
    let number ← fromJson? n
    if number = 0 then throw "snapshot claim number must be positive; omit devices with no claim"
    return (← fromJson? d, ⟨number, ← fromJson? h⟩)
  | _ => throw "expected [device, claim number, digest]"

def readSettle (j : Json) : Except String Json := do
  let claim ← j.getObjVal? "recorded"
  let own ← readClaim claim
  let slot ← positive claim "slot"
  let digest ← claim.getObjValAs? String "digest"
  let current ← match ← j.getObjVal? "current" with
    | .null => pure none
    | value => some <$> readClaim value
  if let some x := current then
    if x.2.2 = own.2.2 && (x.1 != own.1 || x.2.1 != own.2.1) then
      throw "equal bytes must have equal authenticated metadata"
  let floor ← j.getObjVal? "floor"
  let start ← nat floor "first_kept"
  let claims ← (← floor.getObjValAs? (List Json) "last_claims").mapM readClaimRecord
  distinct (claims.map (·.1)) "snapshot device"
  let covered := (claims.find? (fun c => c.1 == own.1)).map (·.2)
  let another := fun (x : Nat × Nat × String) => decide (x.1 = own.1 ∧ own.2.1 ≤ x.2.1)
  return toJson (match Settle.settle start covered slot own own.2.1 digest another current with
    | .landed => "landed" | .notLanded => "notLanded" | .retry => "retry" | .copy => "copy")

def readRead (j : Json) : Except String Json := do
  let applied ← nat j "applied"
  let fetched ← j.getObjValAs? (List Nat) "fetched"
  distinct fetched "fetched slot"
  if fetched.any (· == 0) then throw "fetched slots must be positive"
  let start ← nat j "first_kept"
  match Reader.checkedSlots applied start fetched with
  | none => return Json.mkObj [("reload", toJson true), ("apply", toJson ([] : List Nat))]
  | some slots => return Json.mkObj [("reload", toJson false), ("apply", toJson slots)]

def readSelection (j : Json) : Except String (Reload.Selection Nat) := do
  let slot ← positive j "slot"
  let covers ← nat j "covers"
  if covers ≥ slot then throw "snapshot coverage must precede its announcement"
  return ⟨slot, ← nat j "audience", covers⟩

/-- The same reselection decision used by the monotonicity theorems.
Authentication and the reason for retrying are supplied by the Rust fixture. -/
def readReselect (j : Json) : Except String Json := do
  let missing ← match ← j.getObjVal? "missing" with
    | .null => pure none
    | value => some <$> readSelection value
  let floor ← (← j.getObjValAs? (List Json) "floor").mapM readSelection
  distinct (floor.map (·.audience)) "floor audience"
  return toJson (Reload.reselect (← nat j "old_target") (← nat j "floor_at")
    (← j.getObjValAs? (List Nat) "covers") missing floor)

/-- Bytes carry the containing slot in this projection. Timestamp and schema
are units/zero because Past does not inspect either. -/
abbrev Original := Causality.Original Nat Unit Nat

def readOriginal (j : Json) : Except String Original := do
  return ⟨← nat j "author", ← positive j "number", (), 0,
    ← nat j "read_through", ← positive j "slot"⟩

def readPast (j : Json) : Except String Json := do
  let writes ← (← j.getObjValAs? (List Json) "writes").mapM readOriginal
  distinct (writes.map fun w => (w.device, w.number)) "authored write"
  for w in writes do
    if w.bytes ≤ w.readThrough then throw "published write must follow its authored read"
    for a in writes do
      if a.bytes == w.bytes && a.device != w.device then throw "one slot must have one author"
      if a.device == w.device && a.number < w.number then
        if w.bytes < a.bytes || w.readThrough < a.readThrough then
          throw "publication violates own-number order or monotone authored reads"
  return toJson (writes.map fun w => writes.zipIdx.filterMap fun (a, i) =>
    if Causality.Past (·.bytes) w a then some i else none)

def run (j : Json) : Except String Json := do
  match ← j.getObjValAs? String "kind" with
  | "settle" => readSettle j
  | "read" => readRead j
  | "reselect" => readReselect j
  | "apply" => readApply j
  | "past" => readPast j
  | "keys" => SequenceKeys.readKeys j
  | "rotation" => SequenceKeys.readRotation j
  | _ => throw "unknown case kind"

end SequenceJson
