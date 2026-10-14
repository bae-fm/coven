import StorelogJson
import CovenStorelogData.MemberKeySelection

/-! Compare decoded store-log entries and key-copy routing with the keyed
model. Each entry carries its stable author, the signing key that actually
signed it and, for creation and addition, its initial public pair; key-copy
recipients carry their member and exact sealing key. -/
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
  | .addDevice m d => pure (.entry (.addDevice m d) ())
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
  let received := entrySet (List.range n)
  let views := CurrentReplay.Keyed.authorViews H W n n
  let r := CurrentReplay.Keyed.resolve H W n received
  -- The key each entry's recorded view selects to verify it.
  let signatures := (List.range n).map fun e =>
    Json.mkObj [("current", toJson (CurrentReplay.Keyed.signatureCurrent (views e) (H.log e))),
      ("known", toJson (lookup (views e).keys (H.log e).author).isSome)]
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
  let selected := audiences.map fun a =>
    let audience : Audience := match a with | 0 => .store | c + 1 => .circle c
    toJson ((KeySelection.MemberKeys.selectInReplay H W n received pass who audience).map
      (·.entry))
  pure (Json.mkObj [
    ("signatures", toJson signatures), ("keys", toJson keys), ("used", toJson used),
    ("members", toJson members),
    ("kept", toJson (StorelogJson.sorted r.kept)),
    ("dropped", toJson (StorelogJson.sorted r.dropped)),
    ("current", toJson (copies.map fun (_, recipient) =>
      KeySelection.MemberKeys.currentCredential r.state recipient)),
    ("selected", toJson selected)])

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  let result := do
    let cases ← (← Json.parse input).getArr?
    toJson <$> cases.mapM run
  match result with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
