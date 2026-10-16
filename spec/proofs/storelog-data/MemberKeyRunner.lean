import StorelogJson
import CovenStorelogData.MemberKeyPhases
import CovenStorelog.OldKeyObjects

/-! Compare decoded store-log entries and key-copy routing with the keyed
model. Each entry carries its stable author, the signing key that actually
signed it and, for creation, addition and replacement, its public pair;
key-copy recipients carry their member and exact sealing key. Optional
`received` names the received entry set (all entries by default), and
optional `writes` are old-key write objects judged by `OldKeyObjects`, with
`horizon` the stored finality horizon their window verdicts are final by.
Optional `shares` are `[member, introduction]` historical-sharing requests
judged by `MemberKeys.shareInReplay` against the given copies; the runner
evaluates its body, `share` on the one replay it resolves. -/
open Lean CovenStorelog CovenStorelogData

private def readKeys (j : Json) : Except String CurrentReplay.Keyed.MemberKeys := do
  let keys ← j.getObjVal? "keys"
  pure ⟨← StorelogJson.nat keys "signing", ← StorelogJson.nat keys "sealing"⟩

/-- Creation and addition carry their initial pair; no other existing action
carries a member-key payload (`Keyed.InitialKeys`). -/
private def readAction (j : Json) : Except String CurrentReplay.Keyed.Action := do
  if (← StorelogJson.nat j "kind") == 16 then
    return .replaceMemberKeys (← StorelogJson.nat j "member") (← readKeys j)
  match ← StorelogJson.readAction j with
  | .create access key => pure (.entry (.create access key) (← readKeys j))
  | .addMember m r access => pure (.entry (.addMember m r access) (← readKeys j))
  | .setAccess m access => pure (.entry (.setAccess m access) ())
  | .removeMember m => pure (.entry (.removeMember m) ())
  | .changeRole m r => pure (.entry (.changeRole m r) ())
  | .addDevice m => pure (.entry (.addDevice m) ())
  | .removeDevice m d => pure (.entry (.removeDevice m d) ())
  | .makeCircle c name key => pure (.entry (.makeCircle c name key) ())
  | .renameCircle c name => pure (.entry (.renameCircle c name) ())
  | .deleteCircle c => pure (.entry (.deleteCircle c) ())
  | .addToCircle c m => pure (.entry (.addToCircle c m) ())
  | .removeFromCircle c m => pure (.entry (.removeFromCircle c m) ())
  | .raiseSchema v s => pure (.entry (.raiseSchema v s) ())
  | .reset s => pure (.entry (.reset s) ())
  | .rotateKey a key => pure (.entry (.rotateKey a key) ())

private def readEntry (j : Json) : Except String CurrentReplay.Keyed.Entry := do
  pure ⟨← StorelogJson.nat j "author", ← StorelogJson.nat j "signer",
    ← StorelogJson.nat j "device", ← j.getObjValAs? (List Nat) "past",
    ← readAction (← j.getObjVal? "action")⟩

private def readWrite (j : Json) : Except String CurrentReplay.Keyed.OldKeyObjects.Write := do
  pure ⟨← StorelogJson.nat j "author", ← StorelogJson.nat j "signer",
    ← StorelogJson.nat j "device", ← j.getObjValAs? (List Nat) "read",
    ← StorelogJson.nat j "stored"⟩

private def field? (j : Json) (name : String) : Option Json :=
  match j.getObjVal? name with
  | .ok value => some value
  | .error _ => none

private def sortedPairs (xs : List (List Nat)) : List (List Nat) :=
  xs.mergeSort fun a b => decide (a.headD 0 ≤ b.headD 0)

private def run (j : Json) : Except String Json := do
  let raw ← j.getObjValAs? (Array Json) "entries"
  let entries ← raw.mapM readEntry
  let some root := entries[0]? | throw "history must contain creation"
  let stored ← raw.mapM (fun e => StorelogJson.nat e "stored")
  let attempted ← raw.mapM (fun e => StorelogJson.nat e "attempted")
  let W ← StorelogJson.nat j "window"
  let n := entries.size
  let H : CurrentReplay.Keyed.History := ⟨fun e => entries[e]?.getD root,
    fun e => stored[e]?.getD stored[0]!, fun e => attempted[e]?.getD attempted[0]!⟩
  if !CurrentReplay.Keyed.metadataValid H n then throw "history violates Keyed.metadataValid"
  let receivedList ← match field? j "received" with
    | some list => fromJson? (α := List Nat) list
    | none => pure (List.range n)
  let received := entrySet receivedList
  let views := CurrentReplay.Keyed.authorViews H W n n
  let r := CurrentReplay.Keyed.resolve H W n received
  -- Authority in each entry's recorded view, and every conflicting pair
  -- with its preference both ways (`Keyed.authorized`, `conflict`, `prefer`).
  let authorized := (List.range n).map fun e =>
    CurrentReplay.Keyed.authorized (views e) (H.log e)
  let conflicts := ((List.range n).flatMap fun a => (List.range n).filterMap fun b =>
    if a < b && CurrentReplay.Keyed.conflict H.log views a b then
      some [a, b, if CurrentReplay.Keyed.prefer H.log views a b then 1 else 0,
        if CurrentReplay.Keyed.prefer H.log views b a then 1 else 0]
    else none)
  let rawWrites ← match field? j "writes" with
    | some list => fromJson? (α := Array Json) list
    | none => pure #[]
  let writeList ← rawWrites.mapM readWrite
  let writes : CurrentReplay.Keyed.OldKeyObjects.Writes := fun w =>
    writeList[w]?.getD ⟨0, 0, 0, [], 0⟩
  let horizon ← match field? j "horizon" with
    | some h => some <$> fromJson? (α := Nat) h
    | none => pure none
  let judged := (List.range writeList.size).map fun w =>
    let obj := writes w
    let timely := CurrentReplay.Keyed.OldKeyObjects.timely H W r obj
    -- `Final.horizon_finalizes_window` and `horizon_finalizes_replacement`.
    -- Without a horizon nothing is final.
    let fixed := match horizon with
      | none => false
      | some horizon => decide (obj.stored ≤ horizon + W) ||
          (!timely && r.kept.any fun e => CurrentReplay.Keyed.OldKeyObjects.retires H obj e &&
            decide (H.stored e < horizon) && !decide (obj.stored ≤ H.stored e + W))
    Json.mkObj [
      ("authorized", toJson (CurrentReplay.Keyed.OldKeyObjects.authorized H W n
        (fun _ => true) writes w)),
      ("timely", toJson timely),
      ("counted", toJson (CurrentReplay.Keyed.OldKeyObjects.countInReplay H W n received
        (fun _ => true) writes (fun _ => true) w)),
      ("current", toJson (CurrentReplay.Keyed.OldKeyObjects.writerCurrent r.state obj)),
      ("fixed", toJson fixed)]
  -- The key each entry's recorded view selects to verify it.
  let signatures := (List.range n).map fun e =>
    Json.mkObj [("current", toJson (CurrentReplay.Keyed.signatureCurrent (views e) (H.log e)))]
  let keys := sortedPairs (r.state.keys.map fun (m, k) => [m, k.signing, k.sealing])
  -- Every pair kept entries bound in this replay, retained after removal.
  let used := (r.state.usedKeys.map fun k => [k.signing, k.sealing]).eraseDups.mergeSort
    fun a b => decide (a.headD 0 ≤ b.headD 0)
  let members := sortedPairs (r.state.membership.members.map fun (m, role) =>
    [m, StorelogJson.roleNumber role])
  let introduction (e : Nat) : Except String KeySelection.Key := do
    if e ≥ n then throw "key outside history"
    let some (a, k) := KeySelection.MemberKeys.introduction (H.log e)
      | throw "not an introduction"
    pure ⟨e, a, k⟩
  let held ← j.getObjValAs? (List Nat) "held"
  let custody ← held.mapM introduction
  let rawCopies ← j.getObjValAs? (List (List Nat)) "copies"
  let copies ← rawCopies.mapM fun copy => do
    let [entry, member, sealing] := copy | throw "copy needs introduction, member and sealing key"
    pure (← introduction entry, (⟨member, sealing⟩ : KeySelection.MemberKeys.Recipient))
  let pass : KeySelection.MemberKeys.Pass := ⟨custody, copies⟩
  let who ← StorelogJson.nat j "self"
  let audiences ← j.getObjValAs? (List Nat) "audiences"
  let audienceOf (a : Nat) : Audience := match a with | 0 => .store | c + 1 => .circle c
  let selected := audiences.map fun a =>
    toJson ((KeySelection.MemberKeys.selectInReplay H W n received pass who (audienceOf a)).map
      (·.entry))
  let retired := KeySelection.MemberKeys.retired H W n r
  let recipient (copy : KeySelection.Key × KeySelection.MemberKeys.Recipient) : List Nat :=
    [copy.2.member, copy.2.sealing]
  -- `MemberKeys.route` for every member with a binding.
  let routes := sortedPairs (r.state.keys.filterMap fun (m, _) =>
    (KeySelection.MemberKeys.route r.state retired m).map fun to => [m, to.sealing])
  -- Whether each copy alone exposes its key: an excluded recipient
  -- (`KeySelection.exposed`) or a retired or noncurrent credential
  -- (`MemberKeys.exposed`).
  let exposes := copies.map fun (key, to) =>
    KeySelection.exposed (audienceMembers r.state.membership) ⟨custody, [(key, to.member)]⟩ key ||
      KeySelection.MemberKeys.exposed r.state retired ⟨custody, [(key, to)]⟩ key
  -- `KeyPhases.MemberKeys.rotationCopies` for a fresh key of each audience.
  let rotations := audiences.map fun a =>
    let key : KeySelection.Key := ⟨n, audienceOf a, ⟨0, Vector.replicate 32 0⟩⟩
    toJson ((KeyPhases.MemberKeys.rotationCopies r.state retired key).map
      fun copies => sortedPairs (copies.map recipient))
  let rawShares ← match field? j "shares" with
    | some list => fromJson? (α := List (List Nat)) list
    | none => pure []
  let shared ← rawShares.mapM fun request => do
    let [member, entry] := request | throw "share needs member and introduction"
    let key ← introduction entry
    -- `shareInReplay` is `share` on this replay's state and retired keys.
    pure (toJson ((KeySelection.MemberKeys.share r.state retired pass member key).map
      fun after => (after.drop copies.length).map recipient))
  pure (Json.mkObj [
    ("signatures", toJson signatures), ("keys", toJson keys), ("used", toJson used),
    ("members", toJson members),
    ("kept", toJson (StorelogJson.sorted r.kept)),
    ("dropped", toJson (StorelogJson.sorted r.dropped)),
    ("current", toJson (copies.map fun (_, recipient) =>
      KeySelection.MemberKeys.currentCredential r.state recipient)),
    ("selected", toJson selected),
    ("retired", toJson (retired.eraseDups.mergeSort (· ≤ ·))),
    ("routes", toJson routes), ("exposes", toJson exposes),
    ("rotations", toJson rotations), ("shared", toJson shared),
    ("authorized", toJson authorized), ("conflicts", toJson conflicts),
    ("writes", toJson judged)])

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  let result := do
    let cases ← (← Json.parse input).getArr?
    toJson <$> cases.mapM run
  match result with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
