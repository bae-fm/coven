import Std

/-! One audience part's original row changes under its authenticated root.
The write signs a header containing that root; verification below selects
this part's entry. Hashing is supplied for a fixed audience-key context.
Hash and signature security are hypotheses, not new axioms. A leaf is an
original change, never a merged SQL row. -/
namespace CovenStorage.RowAuthorship

inductive Tree (Row : Type) where
  | leaf (row : Row)
  | fork (left right : Tree Row)
  deriving DecidableEq, Repr

inductive HashInput (Row Digest : Type) where
  | leaf (row : Row)
  | fork (left right : Digest)
  deriving DecidableEq, Repr

structure Hashing (Row Digest : Type) where
  hash : HashInput Row Digest → Digest
  collisionFree : ∀ a b, hash a = hash b → a = b

variable {Row Digest : Type}

def root (h : Hashing Row Digest) : Tree Row → Digest
  | .leaf r => h.hash (.leaf r)
  | .fork l r => h.hash (.fork (root h l) (root h r))

def leaves : Tree Row → List Row
  | .leaf r => [r]
  | .fork l r => leaves l ++ leaves r

inductive Proof (Row Digest : Type) where
  | reveal (row : Row)
  | hidden (hash : Digest)
  | fork (left right : Proof Row Digest)
  deriving DecidableEq, Repr

def proofRoot (h : Hashing Row Digest) : Proof Row Digest → Digest
  | .reveal r => h.hash (.leaf r)
  | .hidden d => d
  | .fork l r => h.hash (.fork (proofRoot h l) (proofRoot h r))

def revealed : Proof Row Digest → List Row
  | .reveal r => [r]
  | .hidden _ => []
  | .fork l r => revealed l ++ revealed r

theorem revealed_authentic (h : Hashing Row Digest) (p : Proof Row Digest)
    (t : Tree Row) (accepted : proofRoot h p = root h t) :
    ∀ r ∈ revealed p, r ∈ leaves t := by
  induction p generalizing t with
  | hidden d => simp [revealed]
  | reveal r =>
    cases t with
    | leaf x =>
      have eq := h.collisionFree _ _ accepted
      cases eq
      simp [revealed, leaves]
    | fork l r => have eq := h.collisionFree _ _ accepted; cases eq
  | fork l r il ir =>
    cases t with
    | leaf x => have eq := h.collisionFree _ _ accepted; cases eq
    | fork a b =>
      have eq := h.collisionFree _ _ accepted
      have parts := HashInput.fork.inj eq
      intro x hx
      simp only [revealed, List.mem_append] at hx
      exact hx.elim (fun hl => List.mem_append_left _ (il a parts.1 x hl))
        (fun hr => List.mem_append_right _ (ir b parts.2 x hr))

/-- WriteId includes store and device identity. In a fixed part context,
verification checks the signed header's root entry, binding that id and the
claimed member using admitted keys. `signed` is the actual
signing history, not a publisher's claim. The author may sign several bodies
for one identity; unforgeability does not forbid that behavior. -/
structure Signatures (Member WriteId Row Digest Signature : Type)
    (h : Hashing Row Digest) where
  author : WriteId → Member
  signed : WriteId → Tree Row → Prop
  verifies : Member → WriteId → Digest → Signature → Prop
  unforgeable : ∀ m w d s, verifies m w d s →
    m = author w ∧ ∃ original, signed w original ∧ d = root h original

theorem snapshot_authorship {Member WriteId Signature : Type}
    (h : Hashing Row Digest) (s : Signatures Member WriteId Row Digest Signature h)
    (m : Member) (w : WriteId) (signature : Signature) (p : Proof Row Digest)
    (accepted : s.verifies m w (proofRoot h p) signature) :
    m = s.author w ∧ ∃ original, s.signed w original ∧
      ∀ r ∈ revealed p, r ∈ leaves original := by
  obtain ⟨author, original, signed, digest⟩ := s.unforgeable m w _ signature accepted
  exact ⟨author, original, signed, revealed_authentic h p _ digest⟩

def visible (keep : Row → Bool) : Tree Row → Bool
  | .leaf r => keep r
  | .fork l r => visible keep l || visible keep r

def prune (h : Hashing Row Digest) (keep : Row → Bool) (t : Tree Row) : Proof Row Digest :=
  if visible keep t then
    match t with
    | .leaf r => .reveal r
    | .fork l r => .fork (prune h keep l) (prune h keep r)
  else .hidden (root h t)

theorem prune_root (h : Hashing Row Digest) (keep : Row → Bool) (t : Tree Row) :
    proofRoot h (prune h keep t) = root h t := by
  induction t with
  | leaf r => simp [prune]; split <;> rfl
  | fork l r il ir =>
    rw [prune.eq_def]
    split
    · simp only [proofRoot, root, il, ir]
    · rfl

theorem visible_iff (keep : Row → Bool) (t : Tree Row) :
    visible keep t = true ↔ ∃ r ∈ leaves t, keep r = true := by
  induction t with
  | leaf r => simp [visible, leaves]
  | fork l r il ir => simp [visible, leaves, il, ir, or_and_right, exists_or]

theorem multiproof_complete (h : Hashing Row Digest) (keep : Row → Bool) (t : Tree Row) :
    ∀ r, r ∈ revealed (prune h keep t) ↔ r ∈ leaves t ∧ keep r = true := by
  induction t with
  | leaf x =>
    intro r
    rw [prune.eq_def]
    cases hk : keep x <;> simp [visible, hk, revealed, leaves] <;>
      intro eq <;> subst r <;> assumption
  | fork l r il ir =>
    intro x
    rw [prune.eq_def]
    split
    · simp [revealed, leaves, il, ir, or_and_right]
    · rename_i empty
      have no : ∀ x ∈ leaves (.fork l r), keep x ≠ true := by
        intro x hx hk
        exact empty ((visible_iff keep _).mpr ⟨x, hx, hk⟩)
      simp only [revealed, List.not_mem_nil, false_iff, not_and]
      exact no x

def hashes : Proof Row Digest → Nat
  | .reveal _ => 0
  | .hidden _ => 1
  | .fork l r => hashes l + hashes r

def absent (keep : Row → Bool) : Tree Row → Nat
  | .leaf r => if keep r then 0 else 1
  | .fork l r => absent keep l + absent keep r

theorem invisible_positive (keep : Row → Bool) (t : Tree Row)
    (empty : visible keep t = false) : 0 < absent keep t := by
  induction t with
  | leaf r => simp [visible] at empty; simp [absent, empty]
  | fork l r il ir =>
    simp only [visible, Bool.or_eq_false_iff] at empty
    have := il empty.1
    simp only [absent]
    omega

/-- Fixed tree and positions. An ordinary proof reveals exactly the requested
leaves or replaces an entirely absent subtree by its root. Extra originals
and external dictionaries are outside this proof format. -/
inductive Covers (h : Hashing Row Digest) (keep : Row → Bool) : Tree Row → Proof Row Digest → Prop where
  | reveal {r} : keep r = true → Covers h keep (.leaf r) (.reveal r)
  | hidden {t} : visible keep t = false → Covers h keep t (.hidden (root h t))
  | fork {l r pl pr} : Covers h keep l pl → Covers h keep r pr →
      Covers h keep (.fork l r) (.fork pl pr)

theorem prune_covers (h : Hashing Row Digest) (keep : Row → Bool) (t : Tree Row) :
    Covers h keep t (prune h keep t) := by
  induction t with
  | leaf r =>
    rw [prune.eq_def]
    split
    · exact .reveal (by assumption)
    · exact .hidden (by simpa using ‹¬ visible keep (.leaf r) = true›)
  | fork l r il ir =>
    rw [prune.eq_def]
    split
    · exact .fork il ir
    · exact .hidden (by simpa using ‹¬ visible keep (.fork l r) = true›)

theorem covers_invisible_positive (h : Hashing Row Digest) (keep : Row → Bool)
    {t : Tree Row} {p : Proof Row Digest} (cover : Covers h keep t p)
    (empty : visible keep t = false) : 0 < hashes p := by
  induction cover with
  | reveal hk => simp [visible, hk] at empty
  | hidden => exact Nat.zero_lt_succ _
  | @fork l r pl pr hl hr il ir =>
    simp only [visible, Bool.or_eq_false_iff] at empty
    have := il empty.1
    simp only [hashes]
    omega

theorem multiproof_minimal (h : Hashing Row Digest) (keep : Row → Bool)
    {t : Tree Row} {p : Proof Row Digest} (cover : Covers h keep t p) :
    hashes (prune h keep t) ≤ hashes p := by
  induction cover with
  | reveal hk => simp [prune, visible, hk, hashes]
  | hidden empty => rw [prune.eq_def]; simp [empty, hashes]
  | @fork l r pl pr hl hr il ir =>
    rw [prune.eq_def]
    split
    · simp only [hashes]; omega
    · rename_i empty
      have pos := covers_invisible_positive h keep (.fork hl hr) (by simpa using empty)
      simp only [hashes] at pos ⊢
      omega

theorem hashes_le_absent (h : Hashing Row Digest) (keep : Row → Bool) (t : Tree Row) :
    hashes (prune h keep t) ≤ absent keep t := by
  induction t with
  | leaf r => cases hk : keep r <;> simp [prune, visible, hk, absent, hashes]
  | fork l r il ir =>
    rw [prune.eq_def]
    split
    · simp only [hashes, absent]; omega
    · rename_i empty
      have pos := invisible_positive keep (.fork l r) (by simpa using empty)
      simp only [hashes] at pos ⊢
      omega

theorem untouched_no_hashes (h : Hashing Row Digest) (t : Tree Row) :
    hashes (prune h (fun _ => true) t) = 0 := by
  have zero : absent (fun _ => true) t = 0 := by
    induction t <;> simp_all [absent]
  have bound := hashes_le_absent h (fun _ => true) t
  omega

theorem absent_subtree_one_hash (h : Hashing Row Digest) (keep : Row → Bool)
    (t : Tree Row) (empty : visible keep t = false) :
    hashes (prune h keep t) = 1 := by rw [prune.eq_def]; simp [empty, hashes]

/-- An original row change has its row identity and creation operation inside
its signed leaf. `Payload` also includes generation, provenance and old/new
columns in a concrete encoding. -/
structure Change (Key Payload : Type) where
  key : Key
  creates : Bool
  payload : Payload
  deriving DecidableEq, Repr

/-- The caller cannot authenticate a creator using an opaque digest. It must
open that write's actual creation leaf for this very row. -/
def AcceptsCreator {Key Payload Member WriteId Signature : Type}
    (h : Hashing (Change Key Payload) Digest)
    (s : Signatures Member WriteId (Change Key Payload) Digest Signature h)
    (key : Key) (member : Member) (write : WriteId) (signature : Signature)
    (proof : Proof (Change Key Payload) Digest) : Prop :=
  s.verifies member write (proofRoot h proof) signature ∧
    ∃ change ∈ revealed proof, change.key = key ∧ change.creates = true

theorem owned_row_true_author {Key Payload Member WriteId Signature : Type}
    (h : Hashing (Change Key Payload) Digest)
    (s : Signatures Member WriteId (Change Key Payload) Digest Signature h)
    (key : Key) (member : Member) (write : WriteId) (signature : Signature)
    (proof : Proof (Change Key Payload) Digest)
    (accepted : AcceptsCreator h s key member write signature proof) :
    member = s.author write ∧ ∃ original, s.signed write original ∧
      ∃ change ∈ leaves original, change.key = key ∧ change.creates = true := by
  obtain ⟨signed, change, present, keyEq, creates⟩ := accepted
  obtain ⟨author, original, authored, authentic⟩ := snapshot_authorship h s member write signature proof signed
  exact ⟨author, original, authored, change, authentic change present, keyEq, creates⟩

/-- The exact cell payload also comes from the claimed writer's original
change. Projection to a column/value may be any deterministic read function. -/
theorem cell_true_author {Member WriteId Signature Cell Value : Type}
    (h : Hashing Row Digest) (s : Signatures Member WriteId Row Digest Signature h)
    (member : Member) (write : WriteId) (signature : Signature) (proof : Proof Row Digest)
    (read : Row → Cell → Option Value) (row : Row) (cell : Cell) (value : Value)
    (signed : s.verifies member write (proofRoot h proof) signature)
    (present : row ∈ revealed proof) (valueEq : read row cell = some value) :
    member = s.author write ∧ ∃ original, s.signed write original ∧
      ∃ change ∈ leaves original, read change cell = some value := by
  obtain ⟨author, original, authored, authentic⟩ := snapshot_authorship h s member write signature proof signed
  exact ⟨author, original, authored, row, authentic row present, valueEq⟩

/-- Every requested original leaf can be checked with the same multiproof
and signature. No separate authentication path is needed for each row. -/
theorem all_present_checkable {Member WriteId Signature : Type}
    (h : Hashing Row Digest) (s : Signatures Member WriteId Row Digest Signature h)
    (member : Member) (write : WriteId) (signature : Signature) (keep : Row → Bool)
    (original : Tree Row) (signed : s.verifies member write (root h original) signature) :
    s.verifies member write (proofRoot h (prune h keep original)) signature ∧
    ∀ row ∈ leaves original, keep row = true → row ∈ revealed (prune h keep original) := by
  refine ⟨by simpa [prune_root] using signed, ?_⟩
  intro row mem kept
  exact (multiproof_complete h keep _ row).mpr ⟨mem, kept⟩

namespace Examples

def freeHash {α : Type} : Hashing α (Tree α) where
  hash
    | .leaf r => .leaf r
    | .fork l r => .fork l r
  collisionFree a b eq := by cases a <;> cases b <;> simp_all

/-- Both bodies were signed by the same author for the same identity. The
signature-security contract permits this; it still rejects unsigned roots. -/
def twoSignedBodies : Signatures Bool Unit Nat (Tree Nat) Nat freeHash where
  author _ := false
  signed _ original := original = .leaf 0 ∨ original = .leaf 1
  verifies member _ digest signature := member = false ∧
    (signature = 0 ∨ signature = 1) ∧ digest = .leaf signature
  unforgeable member write digest signature accepted := by
    refine ⟨accepted.1, .leaf signature, ?_, accepted.2.2⟩
    rcases accepted.2.1 with rfl | rfl <;> simp

theorem signature_security_allows_two_bodies :
    twoSignedBodies.verifies false () (.leaf 0) 0 ∧
    twoSignedBodies.verifies false () (.leaf 1) 1 ∧
    (Tree.leaf 0 : Tree Nat) ≠ .leaf 1 := by simp [twoSignedBodies]

/-- Adjacent absent leaves straddling the root cannot share a hash. -/
example : hashes (prune (freeHash (α := Nat)) (fun r => r == 0 || r == 3)
    (.fork (.fork (.leaf 0) (.leaf 1)) (.fork (.leaf 2) (.leaf 3)))) = 2 := by decide

example : hashes (prune (freeHash (α := Nat)) (fun r => r == 2)
    (.fork (.fork (.leaf 0) (.leaf 1)) (.leaf 2))) = 1 := by decide

/-- A changed merged row cannot open the original leaf, even if that write
changed only this row and therefore has no absent-row hashes. -/
example : root (freeHash (α := Nat)) (.leaf 12) ≠ proofRoot (freeHash (α := Nat)) (.reveal 13) := by decide

/-- A hidden creation authenticates no creator-to-row association. -/
example : revealed (Proof.hidden (root (freeHash (α := Nat)) (.leaf 12)) : Proof Nat (Tree Nat)) = [] := rfl

/-- Creation writes both cells; an edit replaces only the second. The old
first cell survives, but the final row is not the original signed leaf. -/
theorem merged_cells_need_original :
    root (freeHash (α := Nat × Nat)) (.leaf (7, 8)) ≠
      proofRoot (freeHash (α := Nat × Nat)) (.reveal (7, 9)) ∧
    hashes (Proof.reveal (7, 9) : Proof (Nat × Nat) (Tree (Nat × Nat))) = 0 := by decide

example : root (freeHash (α := Nat × Nat)) (.leaf (7, 8)) ≠
    proofRoot (freeHash (α := Nat × Nat)) (.reveal (7, 9)) := by decide

theorem one_row_proof_required (original final : Nat × Nat) (changed : original ≠ final) :
    root (freeHash (α := Nat × Nat)) (.leaf original) ≠
      proofRoot (freeHash (α := Nat × Nat)) (.reveal final) := by
  intro eq
  exact changed (Tree.leaf.inj eq)

end Examples
end CovenStorage.RowAuthorship
