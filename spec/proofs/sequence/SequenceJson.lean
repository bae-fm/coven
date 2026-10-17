import SequenceApply
import CovenSequence.Settle
import CovenSequence.Causality

/-! One case per process; malformed inputs fail on stderr with nonzero exit. -/
namespace SequenceJson
open CovenSequence Lean
open SequenceApply (sig Config Audience Row)

def nat (j : Json) (key : String) : Except String Nat := j.getObjValAs? Nat key

def positive (j : Json) (key : String) : Except String Nat := do
  let n ← nat j key
  if n = 0 then throw s!"{key} must be positive"
  return n

def distinct [DecidableEq α] (xs : List α) (name : String) : Except String Unit := do
  if xs.eraseDups.length != xs.length then throw s!"duplicate {name}"

def pair (j : Json) : Except String (Nat × Nat) := do
  let xs ← j.getArr?
  match xs.toList with
  | [a, b] => return (← fromJson? a, ← fromJson? b)
  | _ => throw "expected a pair of natural numbers"

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
  let owners ← j.getObjValAs? (List Nat) "owner_claims"
  distinct uniques "unique claim"
  distinct owners "owner claim"
  let writes ← j.getObjValAs? (List Nat) "writes"
  distinct writes "write number"
  if writes.any (· == 0) then throw "write numbers must be positive"
  let rows ← (← j.getObjValAs? (List Json) "rows").mapM (readRow k)
  distinct (rows.map (·.id)) "row update"
  return (a, ⟨← nat j "version", uniques.map Sum.inl ++ owners.map Sum.inr, ⟨writes, rows⟩⟩)

def readSlot (c : Config n) (k : Nat) (j : Json) : Except String (Nat × Slot (sig n)) := do
  let claim ← positive j "claim"
  let d ← nat j "device"
  let slot ← match ← j.getObjValAs? String "kind" with
    | "admin" => pure (.admin d (← readEntry n (← j.getObjVal? "entry")))
    | "writes" => do
      let parts ← (← j.getObjValAs? (List Json) "parts").mapM (readPart c k)
      distinct (parts.map (·.1)) "part audience"
      pure (.writes d (← nat j "key_gen") (fun a => (parts.find? (fun p => p.1 == a)).map (·.2)))
    | _ => throw "unknown slot kind"
  return (claim, slot)

def readApply (j : Json) : Except String Json := do
  let n ← nat j "member_count"
  let c ← readConfig n j
  let raw ← j.getObjValAs? (List Json) "slots"
  let entries ← raw.zipIdx.mapM fun (s, k) => readSlot c (k + 1) s
  for (entry, index) in entries.zipIdx do
    let (claim, x) := entry
    for (old, y) in entries.take index do
      if author x == author y && claim ≤ old then throw "device claim numbers must increase"
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

def readSettle (j : Json) : Except String Json := do
  let claim ← j.getObjVal? "recorded"
  let d ← nat claim "device"
  let number ← positive claim "number"
  let slot ← positive claim "slot"
  let bytes ← claim.getObjValAs? String "bytes"
  let current ← j.getObjValAs? (Option String) "current"
  let floor ← j.getObjVal? "floor"
  let start ← nat floor "first_kept"
  let claims ← (← floor.getObjValAs? (List Json) "last_claims").mapM pair
  distinct (claims.map (·.1)) "snapshot device"
  let covered := match claims.find? (fun c => c.1 == d) with
    | some c => c.2
    | none => 0
  return toJson (match Settle.settle start covered slot bytes number current with
    | .landed => "landed" | .notLanded => "notLanded" | .retry => "retry")

def readRead (j : Json) : Except String Json := do
  let applied ← nat j "applied"
  let fetched ← j.getObjValAs? (List Nat) "fetched"
  distinct fetched "fetched slot"
  if fetched.any (· == 0) then throw "fetched slots must be positive"
  let start ← nat j "first_kept"
  match Reader.checkedSlots applied start fetched with
  | none => return Json.mkObj [("reload", toJson true), ("apply", toJson ([] : List Nat))]
  | some slots => return Json.mkObj [("reload", toJson false), ("apply", toJson slots)]

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
  | "apply" => readApply j
  | "past" => readPast j
  | _ => throw "unknown case kind"

end SequenceJson
