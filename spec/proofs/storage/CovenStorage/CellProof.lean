import CovenStorage.RowAuthorship

/-! The per-cell part format and the authentication material a snapshot
retains. Serialization, admitted history and cryptography remain boundaries. -/
namespace CovenStorage.RowAuthorship

variable {Key Column Value Digest : Type}

/-- Each insert has a creation marker even when it sets no non-key cells. -/
def insertLeaves (key : Key) (generation : Nat) (cells : List (Column × Value)) :
    List (Change Key (Column × Value)) :=
  .creation key generation :: cells.map (.cell key generation .insert)

def updateLeaves (key : Key) (generation : Nat) (cells : List (Column × Value)) :
    List (Change Key (Column × Value)) := cells.map (.cell key generation .update)

def deleteLeaves (key : Key) (generation : Nat) : List (Change Key (Column × Value)) :=
  [.deletion key generation]

theorem leaves_per_change (key : Key) (generation : Nat) (cells : List (Column × Value)) :
    (insertLeaves key generation cells).length = cells.length + 1 ∧
    (updateLeaves key generation cells).length = cells.length ∧
    (deleteLeaves (Column := Column) (Value := Value) key generation).length = 1 := by
  simp [insertLeaves, updateLeaves, deleteLeaves]

/-- Separate predicates allow permanent creation evidence without retaining
any of the creation's overwritten scalar values. Delete evidence is likewise
independent of which cells remain current. -/
def neededLeaves (creation deletion : Key → Nat → Bool)
    (cell : Key → Nat → CellKind → Column → Value → Bool) : Change Key (Column × Value) → Bool
  | .creation k g => creation k g
  | .deletion k g => deletion k g
  | .cell k g kind (c, v) => cell k g kind c v

theorem cell_complete (h : Hashing (Change Key (Column × Value)) Digest)
    (creation deletion : Key → Nat → Bool) (cell : Key → Nat → CellKind → Column → Value → Bool)
    (original : Tree (Change Key (Column × Value))) (k : Key) (g : Nat)
    (kind : CellKind) (c : Column) (v : Value) :
    Change.cell k g kind (c, v) ∈ revealed (prune h (neededLeaves creation deletion cell) original) ↔
      Change.cell k g kind (c, v) ∈ leaves original ∧ cell k g kind c v = true :=
  multiproof_complete h (neededLeaves creation deletion cell) original _

theorem creation_complete (h : Hashing (Change Key (Column × Value)) Digest)
    (creation deletion : Key → Nat → Bool) (cell : Key → Nat → CellKind → Column → Value → Bool)
    (original : Tree (Change Key (Column × Value))) (k : Key) (g : Nat) :
    Change.creation k g ∈ revealed (prune h (neededLeaves creation deletion cell) original) ↔
      Change.creation k g ∈ leaves original ∧ creation k g = true :=
  multiproof_complete h (neededLeaves creation deletion cell) original _

theorem overwritten_not_opened (h : Hashing (Change Key (Column × Value)) Digest)
    (creation deletion : Key → Nat → Bool) (cell : Key → Nat → CellKind → Column → Value → Bool)
    (original : Tree (Change Key (Column × Value))) (k : Key) (g : Nat)
    (kind : CellKind) (c : Column) (v : Value) (unneeded : cell k g kind c v = false) :
    Change.cell k g kind (c, v) ∉ revealed (prune h (neededLeaves creation deletion cell) original) := by
  rw [cell_complete]
  simp [unneeded]

/-- Directions authenticate positions without retaining sibling values. -/
inductive Direction where
  | left | right
  deriving DecidableEq, Repr

variable {Leaf : Type}

def height : Tree Leaf → Nat
  | .leaf _ => 0
  | .fork l r => max (height l) (height r) + 1

/-- Open exactly one position, using one sibling hash per branch. A malformed
position is rejected rather than interpreted as another leaf. -/
def openAt (h : Hashing Leaf Digest) : Tree Leaf → List Direction → Option (Leaf × Proof Leaf Digest)
  | .leaf a, [] => some (a, .reveal a)
  | .fork l r, .left :: rest =>
    (openAt h l rest).map fun (a, p) => (a, .fork p (.hidden (root h r)))
  | .fork l r, .right :: rest =>
    (openAt h r rest).map fun (a, p) => (a, .fork (.hidden (root h l)) p)
  | _, _ => none

theorem openAt_spec (h : Hashing Leaf Digest) (t : Tree Leaf) (path : List Direction)
    (a : Leaf) (p : Proof Leaf Digest) (opened : openAt h t path = some (a, p)) :
    proofRoot h p = root h t ∧ revealed p = [a] ∧ hashes p = path.length ∧ path.length ≤ height t := by
  induction t generalizing path p with
  | leaf b =>
    cases path with
    | nil => simp only [openAt, Option.some.injEq, Prod.mk.injEq] at opened
             obtain ⟨rfl, rfl⟩ := opened
             simp [proofRoot, root, revealed, hashes, height]
    | cons d ds => simp [openAt] at opened
  | fork l r il ir =>
    cases path with
    | nil => simp [openAt] at opened
    | cons d ds =>
      cases d <;> simp only [openAt, Option.map_eq_some_iff] at opened
      · obtain ⟨⟨b, q⟩, hq, eq⟩ := opened
        cases eq
        obtain ⟨hr, hv, hc, hh⟩ := il ds q hq
        refine ⟨by simp [proofRoot, root, hr], by simp [revealed, hv], by simp [hashes, hc], ?_⟩
        simp only [List.length_cons, height]
        omega
      · obtain ⟨⟨b, q⟩, hq, eq⟩ := opened
        cases eq
        obtain ⟨hr, hv, hc, hh⟩ := ir ds q hq
        refine ⟨by simp [proofRoot, root, hr], by simp [revealed, hv], by simp [hashes, hc, Nat.add_comm], ?_⟩
        simp only [List.length_cons, height]
        omega

theorem every_leaf_openable (h : Hashing Leaf Digest) (t : Tree Leaf) (a : Leaf)
    (mem : a ∈ leaves t) : ∃ path p, openAt h t path = some (a, p) := by
  induction t with
  | leaf b => simp only [leaves, List.mem_singleton] at mem
              subst b
              exact ⟨[], .reveal a, rfl⟩
  | fork l r il ir =>
    rcases List.mem_append.mp mem with hl | hr
    · obtain ⟨path, p, hp⟩ := il hl
      exact ⟨.left :: path, .fork p (.hidden (root h r)), by simp [openAt, hp]⟩
    · obtain ⟨path, p, hp⟩ := ir hr
      exact ⟨.right :: path, .fork (.hidden (root h l)) p, by simp [openAt, hp]⟩

/-- Count only signatures and digests. Row/column/generation/kind identities,
positions, the current value and signed header framing are separate. -/
def authenticationBytes (digestBytes signatureBytes : Nat) (p : Proof Leaf Digest) : Nat :=
  signatureBytes + digestBytes * hashes p

theorem per_cell_overhead (h : Hashing Leaf Digest) (t : Tree Leaf) (a : Leaf)
    (mem : a ∈ leaves t) : ∃ path p, openAt h t path = some (a, p) ∧
      revealed p = [a] ∧ proofRoot h p = root h t ∧
      authenticationBytes 32 64 p = 64 + 32 * path.length ∧
      authenticationBytes 32 64 p ≤ 64 + 32 * height t := by
  obtain ⟨path, p, opened⟩ := every_leaf_openable h t a mem
  obtain ⟨hr, hv, hc, hh⟩ := openAt_spec h t path a p opened
  exact ⟨path, p, opened, hv, hr, by simp [authenticationBytes, hc], by
    simp only [authenticationBytes, hc]
    exact Nat.add_le_add_left (Nat.mul_le_mul_left 32 hh) 64⟩

namespace Examples

abbrev CellLeaf := Change Nat (Nat × Nat)

def creationTree : Tree CellLeaf :=
  .fork (.leaf (.creation 42 0))
    (.fork (.leaf (.cell 42 0 .insert (0, 7))) (.leaf (.cell 42 0 .insert (1, 8))))

def needed : CellLeaf → Bool := neededLeaves (fun _ _ => true) (fun _ _ => false)
  (fun _ _ _ column _ => column == 0)

def snapshotProof : Proof CellLeaf (Tree CellLeaf) := prune freeHash needed creationTree

/-- The old second value is replaced by one digest; the retained first cell
and permanent creation marker reconstruct the original root. -/
theorem overwritten_value_needs_no_opening :
    revealed snapshotProof = [.creation 42 0, .cell 42 0 .insert (0, 7)] ∧
    proofRoot freeHash snapshotProof = root freeHash creationTree ∧ hashes snapshotProof = 1 := by decide

example : Change.cell 42 0 .insert (1, 8) ∉ revealed snapshotProof := by decide

/-- Even after all initial cells have changed, ownership needs no old value. -/
theorem creation_without_initial_values :
    revealed (prune freeHash (neededLeaves (fun _ _ => true) (fun _ _ => false)
      (fun _ _ _ _ _ => false)) creationTree) = [.creation 42 0] ∧
    hashes (prune freeHash (neededLeaves (fun _ _ => true) (fun _ _ => false)
      (fun _ _ _ _ _ => false)) creationTree) = 1 := by decide

/-- Changing only the operation tag still changes the authenticated root. -/
theorem kind_bound :
    root freeHash (.leaf (Change.cell 42 1 .insert (0, 7))) ≠
      root freeHash (.leaf (Change.cell 42 1 .update (0, 7))) := by decide

end Examples
end CovenStorage.RowAuthorship
