import SequenceInput
import CovenSequence.Keys
import CovenSequence.KeyCustody

/-! Authenticated key-slot projections and the current membership/custody at
one applied prefix. Hash and exact credential numbers are equality tokens;
Rust supplies the corresponding SHA-256 and public-key equalities. -/
namespace SequenceKeys
open Lean CovenSequence.Keys SequenceInput

def key (j : Json) : Except String (Key Nat) := do
  return ⟨← nat j "audience", ← nat j "key"⟩

def event (j : Json) : Except String (Event Nat Nat) := do
  match ← j.getObjValAs? String "kind" with
  | "introduce" => return .introduce (← key j) (← nat j "hash")
  | "copy" => return .copy ⟨← key j, ← nat j "member", ← nat j "credential"⟩
  | "other" => return .other
  | _ => throw "unknown key event"

def eventJson : Event Nat Nat → Json
  | .introduce k hash => Json.mkObj [("kind", toJson "introduce"), ("audience", toJson k.audience),
      ("key", toJson k.id), ("hash", toJson hash)]
  | .copy r => Json.mkObj [("kind", toJson "copy"), ("audience", toJson r.key.audience),
      ("key", toJson r.key.id), ("member", toJson r.member), ("credential", toJson r.credential)]
  | .other => Json.mkObj [("kind", toJson "other")]

structure Input where
  slots : List (Event Nat Nat)
  held : Custody Nat
  bindings : List ((Nat × Nat) × Nat)
  writer : Nat
  store : Nat
  audiences : List Nat
  keys : List (Key Nat)

def Input.current (input : Input) : Current Nat Nat :=
  fun a m => (input.bindings.find? (·.1 == (a, m))).map (·.2)

def readInput (j : Json) : Except String Input := do
  let slots ← (← j.getObjValAs? (List Json) "slots").mapM event
  let held ← (← j.getObjValAs? (List Json) "held").mapM fun x => do
    return (← key x, ← nat x "hash")
  distinct (held.map (·.1)) "held key"
  let current ← (← j.getObjValAs? (List Json) "current").mapM fun x => do
    return ((← nat x "audience", ← nat x "member"), ← nat x "credential")
  distinct (current.map (·.1)) "current member credential"
  let audiences ← j.getObjValAs? (List Nat) "audiences"
  distinct audiences "audience"
  let keys ← (← j.getObjValAs? (List Json) "keys").mapM key
  distinct keys "queried key"
  return ⟨slots, fun k => (held.find? (·.1 == k)).map (·.2), current,
    ← nat j "writer", ← nat j "store", audiences, keys⟩

def selectionJson (input : Input) (h : History Nat Nat) (audience : Nat) : Json :=
  match sendingKey h input.held input.current input.writer input.store audience with
  | some i => Json.mkObj [("key", toJson i.key.id)]
  | none =>
    if audience = input.store && (input.current audience input.writer).isNone then
      Json.mkObj [("error", toJson "removed")]
    else match newest (h.introductions.filter (·.key.audience == audience)) with
      | some i => Json.mkObj [("unavailable", toJson i.key.id)]
      | none => Json.mkObj [("error", toJson "notAllowed")]

def result (input : Input) (h : History Nat Nat) : Json := Json.mkObj [
  ("keys", toJson (input.keys.map fun k => Json.mkObj [
    ("audience", toJson k.audience), ("key", toJson k.id),
    ("accepted", toJson (accepted h input.held k)), ("exposed", toJson (exposed h input.current k))])),
  ("selection", toJson (input.audiences.map fun a => Json.mkObj [
    ("audience", toJson a),
    ("first", toJson ((firstKey h input.held input.current input.writer a).map (·.key.id))),
    ("sending", selectionJson input h a)])),
  ("metadata", toJson ((metadataKey h input.held input.store).map (·.key.id)))]

def readKeys (j : Json) : Except String Json := do
  let input ← readInput j
  return result input (publish ⟨[], []⟩ 0 input.slots)

def readCustody (j : Json) : Except String Json := do
  let held ← j.getObjValAs? (Option Nat) "held"
  let fresh ← nat j "fresh"
  let k ← key j
  let saved := CovenSequence.KeyCustody.save (fun _ => held) k fresh
  match CovenSequence.KeyCustody.ready id saved k with
  | .generating _ => throw "saving must precede a prepared commitment"
  | .prepared key hash =>
    return Json.mkObj [("audience", toJson key.audience), ("key", toJson key.id),
      ("hash", toJson hash)]

def readRotation (j : Json) : Except String Json := do
  let input ← readInput j
  let h := publish (⟨[], []⟩ : History Nat Nat) 0 input.slots
  let rotation ← j.getObjVal? "rotation"
  let fresh ← key rotation
  let hash ← nat rotation "hash"
  let members ← rotation.getObjValAs? (List Nat) "members"
  distinct members "rotation member"
  for ((audience, member), _) in input.bindings do
    if audience == fresh.audience && member ∉ members then throw "rotation omits a current audience member"
  if h.introductions.any (·.key == fresh) || h.copies.any (·.key == fresh) then
    throw "rotation key must be fresh, including tentative copies"
  if input.held fresh != some hash then throw "generated rotation material must be held"
  if (input.current fresh.audience input.writer).isNone then throw "rotation writer must belong to audience"
  if (metadataKey h input.held input.store).isNone then throw "rotation requires a held accepted store key"
  let slots := rotationSlots input.current members fresh hash
  return Json.mkObj [("slots", toJson (slots.map eventJson)),
    ("through", toJson (input.slots.length + slots.length)),
    ("after", result input (publish h input.slots.length slots))]

end SequenceKeys
