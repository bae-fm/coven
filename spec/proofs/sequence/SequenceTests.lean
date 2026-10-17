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

def current (device number : Nat) (bytes : String) : Json :=
  json% {"device":$(device),"number":$(number),"bytes":$(bytes)}

def settlement (slot start : Nat) (covered : Option (Nat × String)) (read : Option Json) : Json :=
  let claims := covered.toList.map fun (n, h) => json% [0,$(n),$(h)]
  json% {"kind":"settle",
    "recorded":{"device":0,"number":2,"slot":$(slot),"bytes":"ours","digest":"ours-hash"},
    "current":$(read),"floor":{"first_kept":$(start),"last_claims":$(claims ++ [json% [1,99,"remote-hash"]])}}

-- Exact bytes at the floor; authenticated different same-id claims detect
-- copies. Below the floor only the snapshot record may settle the claim.
#eval do
  let ours := some (current 0 2 "ours")
  check (settlement 2 0 none ours) (toJson "landed")
  check (settlement 2 2 none ours) (toJson "landed")
  check (settlement 2 2 (some (3,"other-hash")) ours) (toJson "landed")
  check (settlement 2 2 (some (9,"remote")) (some (current 1 2 "other"))) (toJson "notLanded")
  check (settlement 2 2 none (some (current 0 2 "other"))) (toJson "copy")
  check (settlement 2 2 none (some (current 0 3 "other"))) (toJson "copy")
  check (settlement 2 2 none (some (current 0 1 "earlier"))) (toJson "copy")
  check (settlement 2 2 (some (9,"remote")) none) (toJson "retry")
  check (settlement 2 3 (some (2,"ours-hash")) none) (toJson "landed")
  check (settlement 2 3 (some (2,"other-hash")) none) (toJson "copy")
  for digest in ["ours-hash", "other-hash"] do
    check (settlement 2 3 (some (3,digest)) ours) (toJson "copy")
    check (settlement 2 3 (some (1,digest)) ours) (toJson "notLanded")
  check (settlement 2 3 none ours) (toJson "notLanded")
  check (settlement 2 3 (some (1,"earlier-hash")) none) (toJson "notLanded")
  check (settlement 2 3 (some (2,"other-hash")) ours) (toJson "copy")
  -- The other device's record cannot confirm ours.
  check (settlement 2 3 none none) (toJson "notLanded")

-- Another copy wins slot 41 with claim 1; deletion must not confirm the loser.
#eval check (json% {"kind":"settle",
  "recorded":{"device":0,"number":1,"slot":41,"bytes":"copy-a","digest":"hash-a"},
  "current":null,"floor":{"first_kept":42,"last_claims":[[0,1,"hash-b"]]}}) (toJson "copy")

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

def reselection (old floorAt : Nat) (covers : List Nat) (missing : Option Json)
    (floor : List Json) : Json := json% {"kind":"reselect", "old_target":$(old),
      "floor_at":$(floorAt),"covers":$(covers),"missing":$(missing),"floor":$(floor)}

def selection (slot audience covers : Nat) : Json :=
  json% {"slot":$(slot),"audience":$(audience),"covers":$(covers)}

#eval do
  let old := some (selection 4 0 3)
  -- A later announcement, including equal coverage, permits reselection.
  check (reselection 4 6 [5] old [selection 6 0 5]) (toJson (6 : Nat))
  check (reselection 4 6 [3] old [selection 6 0 3]) (toJson (6 : Nat))
  -- The old target remains fixed when already past the floor's prefix.
  check (reselection 9 6 [5] old [selection 6 0 5]) (toJson (9 : Nat))
  -- A retained current audience also contributes to the common target.
  check (reselection 4 6 [5,10] old [selection 6 0 5]) (toJson (10 : Nat))
  -- The old audience may have departed: its replacement remains in the floor.
  check (reselection 4 8 [7] old [selection 6 0 5, selection 8 1 7]) (toJson (8 : Nat))
  -- Neither age alone, coverage alone, nor another audience is sufficient.
  for floor in [[selection 2 0 1], [selection 4 0 3], [selection 6 0 2],
                [selection 6 1 5], []] do
    check (reselection 4 6 [5] old floor) Json.null
  -- A deleted suffix is a separate trigger with no missing-body check.
  check (reselection 4 6 [5] none [selection 6 0 5]) (toJson (6 : Nat))
  rejects (reselection 4 6 [5] old [selection 6 0 5, selection 7 0 5])
  rejects (reselection 4 6 [5] old [selection 5 0 5])

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
  "kind":"admin","device":$(device),"claim":$(claim),"digest":$(s!"{device}:{claim}"),"entry":$(entry)}

def writes (device claim generation : Nat) (parts : List Json) : Json := json% {
  "kind":"writes","device":$(device),"claim":$(claim),"digest":$(s!"{device}:{claim}"),
  "key_gen":$(generation),"parts":$(parts)}

def part (audience version : Nat) (unique owners numbers : List Nat) (rows : List Json) : Json := json% {
  "audience":$(audience),"version":$(version),"unique_claims":$(unique),
  "owner_creations":$(owners.flatMap fun r => numbers.map fun w => [r,w]),
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
  checkField result ["audiences","0","owner_creations"] (json% [[7,1,1,0,0]])
  checkField result ["audiences","1","owner_creations"] (json% [[7,4,4,0,0]])
  checkField result ["ledger","last_claims"] (json% [[0,3,"0:3"],[1,1,"1:1"]])

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

-- Ownership selects the lower original number even when candidates arrive reversed.
#eval do
  let result ← run (applyCase [
    writes 0 1 0 [part 0 1 [] [7] [2,1] []],
    writes 1 1 0 [part 0 1 [] [7] [1] []]])
  checkField result ["slots","0","audiences","0","owner_creations"] (json% [[7,1,1,0,0]])
  checkField result ["audiences","0","owner_creations"] (json% [[7,1,1,0,0]])

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
    checkField result ["ledger","last_claims"] (json% [[0,1,"0:1"],[1,1,"1:1"]])
    checkField result ["ledger","refused"] (json% [])
  let result ← run (applyCase [admin 0 1 (json% {"kind":"replaceKey","member":1}),
    writes 1 1 1 [part 0 1 [8] [] [1] [row 1 true (some 8) [[2,1]]]]])
  checkField result ["audiences","0","live_winners"] (json% [[8,1]])
  checkField result ["ledger","key_generations"] (json% [[0,0],[1,1],[2,0]])

-- A removed member id cannot return, even through a fresh device.
#eval do
  let result ← run (applyCase [admin 0 1 (json% {"kind":"removeMember","member":1}),
    admin 0 2 (json% {"kind":"addMember","member":1,"role":"member"}),
    admin 2 1 (json% {"kind":"addDevice","device":2,"member":1}),
    writes 1 1 0 [part 0 1 [7] [] [] []],
    writes 2 2 0 [part 0 1 [7] [] [] []]])
  checkField result ["ledger","removed"] (json% [1])
  checkField result ["ledger","retired"] (json% [1])
  checkField result ["ledger","refused"] (json% [2,3])
  checkField result ["audiences","0","refused"] (json% [4,5])
  checkField result ["audiences","0","unique_claims"] (json% [])

-- Re-inviting a removed person uses a member id with no prior membership.
#eval do
  let result ← run (applyCase [
    admin 0 1 (json% {"kind":"removeMember","member":1}),
    admin 0 2 (json% {"kind":"addMember","member":2,"role":"member"}),
    admin 2 1 (json% {"kind":"addDevice","device":2,"member":2}),
    writes 2 2 0 [part 0 1 [7] [] [] []]])
  checkField result ["ledger","members"] (json% [[0,"admin"],[2,"member"]])
  checkField result ["ledger","retired"] (json% [1])
  checkField result ["ledger","devices"] (json% [[0,0],[1,1],[2,2]])
  checkField result ["ledger","refused"] (json% [])
  checkField result ["audiences","0","unique_claims"] (json% [[7,4]])

-- A member whose last device was removed registers from the fresh device.
#eval do
  let result ← run (applyCase [
    admin 1 1 (json% {"kind":"removeDevice","device":1}),
    admin 2 1 (json% {"kind":"addDevice","device":2,"member":1}),
    writes 2 2 0 [part 0 1 [7] [] [] []]])
  checkField result ["slots","0","ledger","removed"] (json% [1])
  checkField result ["ledger","devices"] (json% [[0,0],[1,1],[2,1]])
  checkField result ["ledger","refused"] (json% [])
  checkField result ["ledger","last_claims"] (json% [[1,1,"1:1"],[2,2,"2:2"]])
  checkField result ["audiences","0","unique_claims"] (json% [[7,3]])

-- The admin role survives removal of all devices. Either member can recover
-- with its current key; an inactive former device cannot authorize entries.
#eval do
  let result ← run (applyCase [
    admin 0 1 (json% {"kind":"removeDevice","device":0}),
    admin 1 1 (json% {"kind":"removeDevice","device":1}),
    admin 0 2 (json% {"kind":"addMember","member":2,"role":"member"}),
    admin 2 1 (json% {"kind":"addDevice","device":2,"member":1}),
    admin 3 1 (json% {"kind":"addDevice","device":3,"member":0}),
    admin 3 2 (json% {"kind":"addMember","member":2,"role":"member"}),
    admin 4 1 (json% {"kind":"addDevice","device":4,"member":2})])
  checkField result ["ledger","devices"] (json% [[0,0],[1,1],[2,1],[3,0],[4,2]])
  checkField result ["ledger","removed"] (json% [0,1])
  checkField result ["ledger","refused"] (json% [3])

-- Neither an admin nor a member may register another installation's id,
-- even for their own member. A refused attempt leaves that id available.
#eval do
  for (device, member) in ([(0,0), (0,1), (1,1)] : List (Nat × Nat)) do
    let result ← run (applyCase [
      admin device 1 (json% {"kind":"addDevice","device":2,"member":$(member)}),
      admin 2 1 (json% {"kind":"addDevice","device":2,"member":$(member)})])
    checkField result ["slots","0","ledger","devices"] (json% [[0,0],[1,1]])
    checkField result ["ledger","refused"] (json% [1])
    checkField result ["ledger","devices"] (json% [[0,0],[1,1],[2,$(member)]])

-- Registered ids cannot register again, including initial ids, removed ids,
-- and attempts to bind the id to another member.
#eval do
  for member in [0,1] do
    let result ← run (applyCase [
      admin 1 1 (json% {"kind":"addDevice","device":1,"member":$(member)}),
      admin 2 1 (json% {"kind":"addDevice","device":2,"member":1}),
      admin 2 2 (json% {"kind":"addDevice","device":2,"member":$(member)}),
      admin 0 1 (json% {"kind":"removeDevice","device":2}),
      admin 2 3 (json% {"kind":"addDevice","device":2,"member":$(member)})])
    checkField result ["ledger","devices"] (json% [[0,0],[1,1],[2,1]])
    checkField result ["ledger","removed"] (json% [2])
    checkField result ["ledger","refused"] (json% [1,3,5])

-- Registration needs current membership; a removed member cannot return by
-- registering a new device, and other unregistered admin authors are refused.
#eval do
  let result ← run (applyCase [
    admin 2 1 (json% {"kind":"addDevice","device":2,"member":2}),
    admin 0 1 (json% {"kind":"removeMember","member":1}),
    admin 2 2 (json% {"kind":"addDevice","device":2,"member":1}),
    admin 2 3 (json% {"kind":"replaceKey","member":0}),
    admin 2 4 (json% {"kind":"removeDevice","device":0}),
    admin 2 5 (json% {"kind":"addMember","member":2,"role":"admin"})])
  checkField result ["ledger","devices"] (json% [[0,0],[1,1]])
  checkField result ["ledger","removed"] (json% [1])
  checkField result ["ledger","refused"] (json% [1,3,4,5,6])
  checkField result ["ledger","key_generations"] (json% [[0,0],[1,0],[2,0]])

-- Members replace their own keys and remove their own devices; an active
-- admin can do both for another member. Replaced-key writes remain refused.
#eval do
  for device in [0,1] do
    let result ← run (applyCase [
      admin device 1 (json% {"kind":"replaceKey","member":1}),
      writes 1 2 0 [part 0 1 [7] [] [] []],
      writes 1 3 1 [part 0 1 [8] [] [] []],
      admin device 4 (json% {"kind":"removeDevice","device":1})])
    checkField result ["ledger","key_generations"] (json% [[0,0],[1,1],[2,0]])
    checkField result ["ledger","removed"] (json% [1])
    checkField result ["ledger","refused"] (json% [])
    checkField result ["audiences","0","refused"] (json% [2])
    checkField result ["audiences","0","unique_claims"] (json% [[8,3]])
  let result ← run (applyCase [
    admin 2 1 (json% {"kind":"addDevice","device":2,"member":1}),
    admin 1 1 (json% {"kind":"removeDevice","device":2})])
  checkField result ["ledger","removed"] (json% [2])
  checkField result ["ledger","refused"] (json% [])

-- Other members' devices and keys, membership and roles require an admin.
#eval do
  let result ← run (applyCase [
    admin 1 1 (json% {"kind":"removeDevice","device":0}),
    admin 1 2 (json% {"kind":"replaceKey","member":0}),
    admin 1 3 (json% {"kind":"addMember","member":2,"role":"member"}),
    admin 1 4 (json% {"kind":"removeMember","member":1}),
    admin 1 5 (json% {"kind":"changeRole","member":1,"role":"admin"}),
    admin 0 1 (json% {"kind":"removeDevice","device":1}),
    admin 1 6 (json% {"kind":"replaceKey","member":1})])
  checkField result ["ledger","members"] (json% [[0,"admin"],[1,"member"]])
  checkField result ["ledger","key_generations"] (json% [[0,0],[1,0],[2,0]])
  checkField result ["ledger","removed"] (json% [1])
  checkField result ["ledger","refused"] (json% [1,2,3,4,5,7])

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
  checkField result ["ledger","last_claims"] (json% [[0,5,"0:5"],[1,1,"1:1"]])
  checkField result ["slots","1","ledger","last_claims"] (json% [[0,2,"0:2"]])

-- admin_state_cannot_settle: the added member is absent after removal, but
-- the ledger still records which device's claim landed.
#eval do
  let result ← run (applyCase [admin 0 1 (json% {"kind":"addMember","member":2,"role":"member"}),
    admin 0 2 (json% {"kind":"removeMember","member":2})])
  checkField result ["ledger","members"] (json% [[0,"admin"],[1,"member"]])
  checkField result ["ledger","last_claims"] (json% [[0,2,"0:2"]])
  check (settlement 1 3 (some (2,"ours-hash")) none) (toJson "landed")

-- Copies may repeat or decrease numbers. The ledger retains the greatest
-- number and its digest, takes the later digest on a tie, and includes refusals.
#eval do
  let claim := fun (number : Nat) (digest : String) => json% {
    "kind":"admin","device":0,"claim":$(number),"digest":$(digest),
    "entry":{"kind":"removeMember","member":0}}
  let result ← run (applyCase [claim 2 "a", claim 1 "lower", claim 2 "b", claim 3 "higher"])
  checkField result ["slots","1","ledger","last_claims"] (json% [[0,2,"a"]])
  checkField result ["slots","2","ledger","last_claims"] (json% [[0,2,"b"]])
  checkField result ["ledger","last_claims"] (json% [[0,3,"higher"]])
  checkField result ["ledger","refused"] (json% [1,2,3,4])
  -- Feed actual ledger output back into settlement, including on an equal-number tie.
  let .ok claims := field result ["slots","2","ledger","last_claims"]
    | throw (IO.userError "missing ledger claims")
  for (digest, verdict) in [("a", "copy"), ("b", "landed")] do
    check (json% {"kind":"settle",
      "recorded":{"device":0,"number":2,"slot":3,"bytes":"recorded","digest":$(digest)},
      "current":null,"floor":{"first_kept":4,"last_claims":$(claims)}}) (toJson verdict)

-- Version and write permission are checked separately for each audience.
#eval do
  let result ← run (applyCase [writes 1 1 0 [part 0 2 [7] [] [] [], part 1 0 [8] [] [] []],
    writes 0 1 0 [part 0 0 [9] [] [] []],
    writes 9 1 0 [part 0 0 [10] [] [] []]])
  checkField result ["audiences","0","refused"] (json% [1,3])
  checkField result ["audiences","1","refused"] (json% [1])
  checkField result ["audiences","0","unique_claims"] (json% [[9,2]])
  checkField result ["ledger","last_claims"] (json% [[0,1,"0:1"],[1,1,"1:1"],[9,1,"9:1"]])
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
  rejects (json% {"kind":"settle",
    "recorded":{"device":0,"number":1,"slot":1,"bytes":"a","digest":"h"},
    "current":null,"floor":{"first_kept":2,"last_claims":[[0,1]]}})
  rejects (json% {"kind":"settle",
    "recorded":{"device":0,"number":1,"slot":1,"bytes":"a","digest":"h"},
    "current":null,"floor":{"first_kept":2,"last_claims":[[0,0,"h"]]}})
  rejects (json% {"kind":"settle",
    "recorded":{"device":0,"number":1,"slot":1,"bytes":"a","digest":"h"},
    "current":null,"floor":{"first_kept":2,"last_claims":[[0,1,"a"],[0,1,"b"]]}})
  rejects (json% {"kind":"settle",
    "recorded":{"device":0,"number":1,"slot":1,"bytes":"a","digest":"h"},
    "current":{"device":1,"number":1,"bytes":"a"},"floor":{"first_kept":1,"last_claims":[]}})
  rejects (json% {"kind":"read","applied":0,"fetched":[1,1],"first_kept":1})
  rejects (json% {"kind":"read","applied":0,"fetched":[0],"first_kept":1})
  rejects (applyCase [admin 0 0 (json% {"kind":"removeMember","member":1})])
  rejects (applyCase [admin 0 1 (json% {"kind":"removeMember","member":3})])
  rejects (applyCase [writes 0 1 0 [part 4 0 [] [] [] []]])
  rejects (applyCase [writes 0 1 0 [json% {"audience":0,"version":1,
    "unique_claims":[],"owner_creations":[[7,2]],"writes":[1],"rows":[]}]])
  rejects (applyCase [writes 0 1 0 [part 0 1 [] [] [1] [row 1 true (some 7) [[1,2]]]]])
  rejects (json% {"kind":"past","writes":[
    {"author":0,"number":1,"slot":1,"read_through":1}]})
  rejects (json% {"kind":"past","writes":[
    {"author":0,"number":1,"slot":2,"read_through":1},
    {"author":0,"number":2,"slot":3,"read_through":0}]})
  rejects (json% {"kind":"past","writes":[
    {"author":0,"number":1,"slot":3,"read_through":0},
    {"author":0,"number":2,"slot":2,"read_through":0}]})

def keyCase (slots held current : List Json) : Json := json% {
  "kind":"keys", "slots":$(slots), "held":$(held), "current":$(current),
  "writer":1,"store":0,"audiences":[0,1],
  "keys":[{"audience":0,"key":9},{"audience":0,"key":1},{"audience":1,"key":7}]}

#eval do
  let old := json% {"kind":"introduce","audience":0,"key":9,"hash":10}
  let copied := json% {"kind":"copy","audience":0,"key":1,"member":1,"credential":2}
  let introduced := json% {"kind":"introduce","audience":0,"key":1,"hash":20}
  let exposed := json% {"kind":"copy","audience":0,"key":9,"member":2,"credential":1}
  let held := [json% {"audience":0,"key":9,"hash":10}, json% {"audience":0,"key":1,"hash":20}]
  let current := [json% {"audience":0,"member":1,"credential":2}]
  let before ← run (keyCase [old, exposed, copied] held current)
  checkField before ["keys","1","accepted"] (toJson false)
  checkField before ["selection","0","sending"] (json% {"unavailable":9})
  checkField before ["metadata"] (toJson (9 : Nat))
  let after ← run (keyCase [old, exposed, copied, introduced] held current)
  checkField after ["selection","0","first"] (toJson (1 : Nat))
  checkField after ["metadata"] (toJson (1 : Nat))
  let late := json% {"kind":"copy","audience":0,"key":1,"member":2,"credential":1}
  let disclosed ← run (keyCase [old, exposed, copied, introduced, late] held current)
  checkField disclosed ["selection","0","sending"] (json% {"unavailable":1})
  checkField disclosed ["metadata"] (toJson (1 : Nat))
  let mismatch ← run (keyCase [old, introduced] [json% {"audience":0,"key":1,"hash":99}] current)
  checkField mismatch ["keys","1","accepted"] (toJson false)
  let conflict ← run (keyCase [old, introduced,
    json% {"kind":"introduce","audience":0,"key":1,"hash":99}] held current)
  checkField conflict ["keys","1","accepted"] (toJson false)
  checkField conflict ["metadata"] (toJson (9 : Nat))
  let departed ← run (keyCase [old, json% {"kind":"introduce","audience":1,"key":7,"hash":30},
    json% {"kind":"copy","audience":1,"key":7,"member":1,"credential":2}]
    (held ++ [json% {"audience":1,"key":7,"hash":30}]) current)
  checkField departed ["selection","1","first"] Json.null
  checkField departed ["selection","1","sending"] (json% {"key":7})
  let removed ← run (keyCase [old] held [])
  checkField removed ["selection","0","sending"] (json% {"error":"removed"})
  let rotate := json% {"kind":"rotation", "slots":$([old, exposed]), "held":$(held),
    "current":$(current),"writer":1,"store":0,"audiences":[0],
    "keys":[{"audience":0,"key":9},{"audience":0,"key":1}],
    "rotation":{"audience":0,"key":1,"hash":20,"members":[1,2]}}
  let rotated ← run rotate
  checkField rotated ["slots"] (toJson [copied, introduced])
  checkField rotated ["through"] (toJson (4 : Nat))
  checkField rotated ["after","selection","0","first"] (toJson (1 : Nat))
  checkField rotated ["after","keys","0","accepted"] (toJson true)
  rejects (json% {"kind":"keys","slots":[],"held":$(held ++ held),"current":[],
    "writer":1,"store":0,"audiences":[],"keys":[]})
  rejects (json% {"kind":"rotation","slots":$([old]),"held":$(held),"current":$(current),
    "writer":1,"store":0,"audiences":[],"keys":[],
    "rotation":{"audience":0,"key":1,"hash":20,"members":[]}})
  rejects (json% {"kind":"rotation","slots":$([old,copied]),"held":$(held),"current":$(current),
    "writer":1,"store":0,"audiences":[],"keys":[],
    "rotation":{"audience":0,"key":1,"hash":20,"members":[1]}})
-- Input-driven projections: changing a queue, row key or authored prefix
-- changes the verdict, even when the scenario name stays the same.
#eval do
  for (through, expected) in ([(2, 0), (3, 2)] : List (Nat × Nat)) do
    check (json% {"kind":"queueReload","scenario":"reset", "applied":2,
      "target":3,"reset":3,"version":1,"loaded_rows":[],
      "queue":[{"number":1,"schema":1,"read_through":$(through),"key":"a"},
               {"number":2,"schema":1,"read_through":$(through),"key":"b"}],
      "after":{"number":3,"schema":1,"read_through":3,"key":"c"}})
      (json% {"retained":2,"replayed":$(expected),"after_replayed":$(expected + 1)})
  for (key, allowed) in [("derived", false), ("other", true)] do
    check (json% {"kind":"queueReload","scenario":"breakingRaise","candidate":$(key),
      "before_rows":["source"],"loaded_rows":["source","derived"],
      "queued_versions":[2],"version":2})
      (json% {"before_allowed":true,"after_allowed":$(allowed),"new_schema":true})
  for member in [false, true] do
    check (json% {"kind":"queueReload","scenario":"newlyReadable", "candidate":"8","existing":"7",
      "before_member":false,"after_member":$(member),"before_rows":[],"loaded_rows":["7"]})
      (json% {"before_allowed":false,"after_allowed":$(member),
        "empty_key_allowed":$(member),"loaded_key_allowed":false})
  for (through, sends) in ([(3, false), (4, true)] : List (Nat × Bool)) do
    check (json% {"kind":"queueReload","scenario":"selfReplacement","replacements":[4,7],
      "read_through":2,"after_read_through":$(through),"after_number":2,"after_schema":1,
      "current":6,"later":8,"old_generation":0,"new_generation":1})
      (json% {"old_authenticates":true,"new_authenticates":false,
        "old_counts":false,"new_counts":true,"after_send":$(sends),"later_counts":false})
  rejects (json% {"kind":"queueReload","scenario":"reset"})
  rejects (json% {"kind":"queueReload","scenario":"unknown"})

#eval do
  check (json% {"kind":"floor","coverage":[10,null]}) Json.null
  check (json% {"kind":"floor","coverage":[10,8]}) (toJson (9 : Nat))
  check (json% {"kind":"floor","coverage":[]}) Json.null

end SequenceTests
