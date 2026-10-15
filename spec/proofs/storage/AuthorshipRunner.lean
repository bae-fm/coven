import CovenStorage.CellProof
import Lean

/-! JSON adapter comparing the Rust authorship multiproofs with `prune` and
`neededLeaves` (D7, D11). The Rust tree splits a part's leaves as RFC 6962
does; this adapter builds that shape and supplies it to the model, whose
hashing is the injective `freeHash`. A step leaf, which the model's cell
format has no kind for, is a deletion leaf under a key no snapshot needs. -/

open Lean CovenStorage.RowAuthorship

/-- RFC 6962's split: the left subtree holds the largest power of two
smaller than the number of leaves. `fuel` bounds the recursion depth. -/
private def rfcTree {α : Type} [Inhabited α] : Nat → Array α → Tree α
  | 0, leaves => .leaf (leaves[0]?.getD default)
  | fuel + 1, leaves =>
    if leaves.size ≤ 1 then .leaf (leaves[0]?.getD default)
    else
      let left := Id.run do
        let mut k := 1
        for _ in [0:leaves.size] do
          if k * 2 < leaves.size then k := k * 2
        return k
      .fork (rfcTree fuel (leaves.extract 0 left)) (rfcTree fuel (leaves.extract left leaves.size))

private abbrev Leaf := Change Nat (Nat × Nat)

private instance : Inhabited Leaf := ⟨.deletion 0 0⟩

private def readLeaf (j : Json) : Except String Leaf := do
  let key ← j.getObjValAs? Nat "key"
  let generation ← j.getObjValAs? Nat "generation"
  match ← j.getObjValAs? String "kind" with
  | "creation" => pure (.creation key generation)
  | "deletion" => pure (.deletion key generation)
  | "insert" => pure (.cell key generation .insert (← j.getObjValAs? Nat "column", ← j.getObjValAs? Nat "value"))
  | "update" => pure (.cell key generation .update (← j.getObjValAs? Nat "column", ← j.getObjValAs? Nat "value"))
  | _ => throw "unknown leaf kind"


private def run (j : Json) : Except String Json := do
  let leaves ← (← j.getObjValAs? (Array Json) "leaves").mapM readLeaf
  -- Leaves are numbered by position, so the opened positions are compared.
  let numbered := (leaves.mapIdx fun i leaf => (i, leaf))
  let creations ← j.getObjValAs? (Array (Nat × Nat)) "creations"
  let deletions ← j.getObjValAs? (Array (Nat × Nat)) "deletions"
  let cells ← (← j.getObjValAs? (Array Json) "cells").mapM fun c => do
    pure (← c.getObjValAs? Nat "key", ← c.getObjValAs? Nat "generation",
      ← c.getObjValAs? String "kind", ← c.getObjValAs? Nat "column", ← c.getObjValAs? Nat "value")
  let kindName : CellKind → String
    | .insert => "insert"
    | .update => "update"
  let needed := neededLeaves (fun k g => creations.contains (k, g)) (fun k g => deletions.contains (k, g))
    (fun k g kind c v => cells.contains (k, g, kindName kind, c, v))
  let tree := rfcTree numbered.size numbered
  let proof := prune Examples.freeHash (fun (_, leaf) => needed leaf) tree
  if leaves.isEmpty then
    return Json.mkObj [("opened", toJson ([] : List Nat)), ("hashes", toJson (1 : Nat))]
  pure (Json.mkObj [("opened", toJson ((revealed proof).map (·.1))), ("hashes", toJson (hashes proof))])

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error err => (← IO.getStderr).putStrLn err; pure 1
