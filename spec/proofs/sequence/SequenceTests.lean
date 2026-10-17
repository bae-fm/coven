import SequenceJson

/-! Exercise the JSON entry point as well as the model decisions. An evaluation
failure fails the package build; these checks introduce no theorem axioms. -/
open Lean CovenSequence Lean.Json

namespace SequenceTests

def run (input : Json) : IO Json :=
  match SequenceJson.run input with
  | .ok value => pure value
  | .error e => throw (IO.userError e)

def equal (actual expected : Json) : IO Unit := do
  unless actual == expected do
    throw (IO.userError s!"expected {expected.compress}, got {actual.compress}")

def check (input expected : Json) : IO Unit := do equal (← run input) expected

def field (value : Json) (path : List String) : Except String Json := do
  match path with
  | [] => return value
  | key :: rest =>
    let child ← match value with
      | .arr xs => do
        let some i := key.toNat? | throw "array index must be numeric"
        let some x := xs[i]? | throw "array index out of range"
        pure x
      | _ => value.getObjVal? key
    field child rest

def checkField (value : Json) (path : List String) (expected : Json) : IO Unit := do
  match field value path with
  | .ok actual => equal actual expected
  | .error e => throw (IO.userError e)

def rejects (input : Json) : IO Unit := do
  match SequenceJson.run input with
  | .error _ => pure ()
  | .ok value => throw (IO.userError s!"unexpected acceptance: {value.compress}")

def settlement (slot start covered : Nat) (current : Option String) : Json :=
  json% {"kind":"settle", "recorded":{"device":0,"number":2,"slot":$(slot),"bytes":"ours"},
    "current":$(current),"floor":{"first_kept":$(start),"last_claims":[[0,$(covered)],[1,99]]}}

-- Byte comparison at the floor; the snapshot takes precedence below it.
#eval do
  check (settlement 2 0 0 (some "ours")) (toJson "landed")
  check (settlement 2 2 0 (some "ours")) (toJson "landed")
  check (settlement 2 2 9 (some "other")) (toJson "notLanded")
  check (settlement 2 2 9 none) (toJson "retry")
  check (settlement 2 3 2 none) (toJson "landed")
  check (settlement 2 3 3 (some "other")) (toJson "landed")
  -- stale_create_wins_deleted_slot: matching stale bytes cannot settle it.
  check (settlement 2 3 1 (some "ours")) (toJson "notLanded")
  check (settlement 2 3 1 none) (toJson "notLanded")
  check (json% {"kind":"settle","recorded":{"device":2,"number":1,"slot":1,"bytes":"a"},
    "current":null,"floor":{"first_kept":2,"last_claims":[[0,9]]}}) (toJson "notLanded")

-- checked_genuine and strict order: fetched arrival order does not skip gaps.
#eval do
  check (json% {"kind":"read","applied":0,"fetched":[1,2],"first_kept":0})
    (json% {"reload":false,"apply":[1,2]})
  check (json% {"kind":"read","applied":0,"fetched":[1,2],"first_kept":2})
    (json% {"reload":true,"apply":[]})
  check (json% {"kind":"read","applied":1,"fetched":[3,1,2,5],"first_kept":2})
    (json% {"reload":false,"apply":[2,3]})
  check (json% {"kind":"read","applied":1,"fetched":[3,4],"first_kept":1})
    (json% {"reload":false,"apply":[]})
  check (json% {"kind":"read","applied":4,"fetched":[2,3,4],"first_kept":5})
    (json% {"reload":false,"apply":[]})
  check (json% {"kind":"read","applied":0,"fetched":[],"first_kept":1})
    (json% {"reload":false,"apply":[]})
  check (json% {"kind":"read","applied":0,"fetched":[],"first_kept":2})
    (json% {"reload":true,"apply":[]})

-- own_predecessor_is_read includes own writes outside the authored prefix;
-- intervening_write_not_read excludes later-published remote writes.
#eval do
  check (json% {"kind":"past","writes":[
    {"author":0,"number":1,"slot":1,"read_through":0},
    {"author":1,"number":1,"slot":2,"read_through":1},
    {"author":0,"number":2,"slot":3,"read_through":0},
    {"author":0,"number":3,"slot":3,"read_through":0},
    {"author":1,"number":2,"slot":4,"read_through":3}]})
    (json% [[],[0],[0],[0,2],[0,1,2,3]])
  -- Input order supplies output indices, not causal order.
  check (json% {"kind":"past","writes":[
    {"author":0,"number":2,"slot":4,"read_through":1},
    {"author":1,"number":1,"slot":2,"read_through":0},
    {"author":0,"number":1,"slot":1,"read_through":0}]}) (json% [[2],[],[]])
  check (json% {"kind":"past","writes":[]}) (json% [])
  check (json% {"kind":"past","writes":[
    {"author":3,"number":1,"slot":8,"read_through":0}]}) (json% [[]])

def applyCase (slots : List Json) : Json := json% {
  "kind":"apply", "member_count":3,
  "initial":{"members":[{"member":0,"role":"admin","key_gen":0},
    {"member":1,"role":"member","key_gen":0}],
    "devices":[{"device":0,"member":0},{"device":1,"member":1}]},
  "audiences":[{"audience":0,"version":1,"writers":[0,1,2]},
    {"audience":1,"version":0,"writers":[0]}],"slots":$(slots)}

def admin (device claim : Nat) (entry : Json) : Json := json% {
  "kind":"admin","device":$(device),"claim":$(claim),"entry":$(entry)}

def writes (device claim generation : Nat) (parts : List Json) : Json := json% {
  "kind":"writes","device":$(device),"claim":$(claim),"key_gen":$(generation),"parts":$(parts)}

def part (audience version : Nat) (unique owners numbers : List Nat) (rows : List Json) : Json := json% {
  "audience":$(audience),"version":$(version),"unique_claims":$(unique),"owner_claims":$(owners),
  "writes":$(numbers),"rows":$(rows)}

def row (id : Nat) (live : Bool) (value : Option Nat) (inputs : List (List Nat)) : Json := json% {
  "row":$(id),"live":$(live),"value":$(value),"inputs":$(inputs)}

-- first_slot_wins and owner_stays use distinct namespaces, per audience.
-- older_loser_returns: deleting row 1 reveals row 2 ahead of new row 3.
#eval do
  let result ← run (applyCase [
    writes 0 1 0 [part 0 1 [7] [7] [1] [row 1 true (some 7) [[1,1]]]],
    writes 1 1 0 [part 0 0 [7] [7] [1] [row 2 true (some 7) [[2,1]]]],
    writes 0 2 0 [part 0 1 [7] [7] [2,3]
      [row 1 false (some 7) [[1,1]], row 3 true (some 7) [[3,3]]]],
    writes 0 3 0 [part 1 0 [7] [7] [4] [row 4 true (some 7) [[4,4]]]]])
  checkField result ["slots","0","audiences","0","live_winners"] (json% [[7,1]])
  checkField result ["slots","1","audiences","0","live_winners"] (json% [[7,1]])
  checkField result ["slots","2","audiences","0","live_winners"] (json% [[7,2]])
  checkField result ["audiences","0","unique_claims"] (json% [[7,1]])
  checkField result ["audiences","0","owner_claims"] (json% [[7,1]])
  checkField result ["audiences","1","owner_claims"] (json% [[7,4]])
  checkField result ["ledger","last_claims"] (json% [[0,3],[1,1]])

-- first_after_release without a surviving loser, and disjoint permanent
-- namespaces even when their numeric keys coincide.
#eval do
  let result ← run (applyCase [
    writes 0 1 0 [part 0 1 [7] [] [1] [row 1 true (some 7) [[1,1]]]],
    writes 0 2 0 [part 0 1 [] [7] [2] [row 1 false (some 7) [[1,1]]]],
    writes 1 1 0 [part 0 1 [7] [7] [1] [row 2 true (some 7) [[3,1]]],
      part 1 0 [7] [7] [1] []]])
  checkField result ["slots","1","audiences","0","live_winners"] (json% [[7,null]])
  checkField result ["audiences","0","live_winners"] (json% [[7,2]])
  checkField result ["audiences","0","unique_claims"] (json% [[7,1]])
  checkField result ["audiences","0","owner_claims"] (json% [[7,2]])
  checkField result ["audiences","0","refused"] (json% [])
  checkField result ["audiences","1","refused"] (json% [3])

-- All three cut-offs consume refused parts without taking claims or rows.
#eval do
  for entry in [json% {"kind":"removeDevice","device":1},
                json% {"kind":"removeMember","member":1},
                json% {"kind":"replaceKey","member":1}] do
    let result ← run (applyCase [admin 0 1 entry,
      writes 1 1 0 [part 0 1 [8] [9] [1] [row 1 true (some 8) [[2,1]]]]])
    checkField result ["audiences","0","refused"] (json% [2])
    checkField result ["audiences","0","unique_claims"] (json% [])
    checkField result ["audiences","0","owner_claims"] (json% [])
    checkField result ["audiences","0","rows"] (json% [])
    checkField result ["ledger","last_claims"] (json% [[0,1],[1,1]])
    checkField result ["ledger","refused"] (json% [])
  let result ← run (applyCase [admin 0 1 (json% {"kind":"replaceKey","member":1}),
    writes 1 1 1 [part 0 1 [8] [] [1] [row 1 true (some 8) [[2,1]]]]])
  checkField result ["audiences","0","live_winners"] (json% [[8,1]])
  checkField result ["ledger","key_generations"] (json% [[0,0],[1,1],[2,0]])

-- Re-adding membership never revives a removed device; a new device counts.
#eval do
  let result ← run (applyCase [admin 0 1 (json% {"kind":"removeMember","member":1}),
    admin 0 2 (json% {"kind":"addMember","member":1,"role":"member"}),
    admin 0 3 (json% {"kind":"addDevice","device":2,"member":1}),
    writes 1 1 0 [part 0 1 [7] [] [] []],
    writes 2 1 0 [part 0 1 [7] [] [] []]])
  checkField result ["ledger","removed"] (json% [1])
  checkField result ["audiences","0","refused"] (json% [4])
  checkField result ["audiences","0","unique_claims"] (json% [[7,5]])

-- last_admin_refused, applied_membership_authorized, and repeated identical
-- refused slots with distinct claim numbers.
#eval do
  let result ← run (applyCase [admin 0 1 (json% {"kind":"removeMember","member":0}),
    admin 0 2 (json% {"kind":"removeMember","member":0}),
    admin 0 3 (json% {"kind":"changeRole","member":0,"role":"member"}),
    admin 1 1 (json% {"kind":"addMember","member":2,"role":"admin"}),
    admin 0 4 (json% {"kind":"changeRole","member":1,"role":"admin"}),
    admin 0 5 (json% {"kind":"removeMember","member":0})])
  checkField result ["ledger","refused"] (json% [1,2,3,4])
  checkField result ["ledger","members"] (json% [[1,"admin"]])
  checkField result ["ledger","last_claims"] (json% [[0,5],[1,1]])
  checkField result ["slots","1","ledger","last_claims"] (json% [[0,2]])

-- admin_state_cannot_settle: the added member is absent after removal, but
-- the ledger still records which device's claim landed.
#eval do
  let result ← run (applyCase [admin 0 1 (json% {"kind":"addMember","member":2,"role":"member"}),
    admin 0 2 (json% {"kind":"removeMember","member":2})])
  checkField result ["ledger","members"] (json% [[0,"admin"],[1,"member"]])
  checkField result ["ledger","last_claims"] (json% [[0,2]])
  check (settlement 1 3 2 none) (toJson "landed")

-- Version and write permission are checked separately for each audience.
#eval do
  let result ← run (applyCase [writes 1 1 0 [part 0 2 [7] [] [] [], part 1 0 [8] [] [] []],
    writes 0 1 0 [part 0 0 [9] [] [] []],
    writes 9 1 0 [part 0 0 [10] [] [] []]])
  checkField result ["audiences","0","refused"] (json% [1,3])
  checkField result ["audiences","1","refused"] (json% [1])
  checkField result ["audiences","0","unique_claims"] (json% [[9,2]])
  checkField result ["ledger","last_claims"] (json% [[0,1],[1,1],[9,1]])
  let empty ← run (applyCase [])
  checkField empty ["slots"] (json% [])
  checkField empty ["audiences","0","version"] (toJson (1 : Nat))

-- latest input setter, within-slot number order, then primary-key rank;
-- empty, deleted and uncounted-input candidates cannot win.
#eval do
  let result ← run (applyCase [
    writes 0 1 0 [part 0 1 [] [] [1,2] [row 8 true (some 7) [[1,1]]]],
    writes 1 1 0 [part 1 0 [] [] [1] []],
    writes 0 2 0 [part 0 1 [] [] [3,4] [
      row 8 true (some 7) [[1,1],[3,4]], row 9 true (some 7) [[3,3]],
      row 2 true (some 7) [[3,3]], row 1 true (some 7) [],
      row 0 false (some 7) [[1,1]]],
      part 1 0 [] [] [3] [row 4 true (some 8) [[2,1]]]],
    writes 0 3 0 [part 0 1 [] [] [5] [row 2 false (some 7) [[3,3]],
      row 9 false (some 7) [[3,3]],row 8 false (some 7) [[1,1],[3,4]]]]])
  checkField result ["slots","2","audiences","0","live_winners"] (json% [[7,2]])
  checkField result ["slots","2","audiences","1","live_winners"] (json% [[8,null]])
  checkField result ["audiences","0","live_winners"] (json% [[7,null]])

#eval do
  rejects (json% {"kind":"read","applied":0,"fetched":[1,1],"first_kept":1})
  rejects (json% {"kind":"read","applied":0,"fetched":[0],"first_kept":1})
  rejects (applyCase [admin 0 0 (json% {"kind":"removeMember","member":1})])
  rejects (applyCase [admin 0 1 (json% {"kind":"removeMember","member":3})])
  rejects (applyCase [writes 0 1 0 [], writes 0 1 0 []])
  rejects (applyCase [writes 0 1 0 [part 4 0 [] [] [] []]])
  rejects (applyCase [writes 0 1 0 [part 0 1 [] [] [1] [row 1 true (some 7) [[1,2]]]]])
  rejects (json% {"kind":"past","writes":[
    {"author":0,"number":1,"slot":1,"read_through":1}]})
  rejects (json% {"kind":"past","writes":[
    {"author":0,"number":1,"slot":2,"read_through":1},
    {"author":0,"number":2,"slot":3,"read_through":0}]})
  rejects (json% {"kind":"past","writes":[
    {"author":0,"number":1,"slot":3,"read_through":0},
    {"author":0,"number":2,"slot":2,"read_through":0}]})

end SequenceTests
