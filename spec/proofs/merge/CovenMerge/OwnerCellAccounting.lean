import CovenMerge.Ownership
import CovenStorage.CellProof

/-! Per-cell openings preserve the original setter identities used by the
merge. Merkle granularity does not split the per-row owner/dependency verdict. -/
namespace CovenMerge.Ownership

variable {W Row Col Table Member K Value Digest Signature : Type}
  [DecidableEq W] [DecidableEq Row] [DecidableEq Col] [DecidableEq Member] [DecidableEq K]

/-- The signed leaf is built from the original change, never from a merged
row or a reference value after null substitution. -/
def originalCell (M : Writes W Row Col) (scalar : W → Row → Col → Value)
    (w : W) (r : Row) (c : Col) : Option (CovenStorage.RowAuthorship.Change Row (Col × Value)) :=
  (M.chg w r).bind fun ch =>
    if ch.sets c then
      match ch.kind with
      | .ins => some (.cell r ch.gen .insert (c, scalar w r c))
      | .upd => some (.cell r ch.gen .update (c, scalar w r c))
      | .del => none
    else none

theorem setter_has_cell_leaf (M : Writes W Row Col) (scalar : W → Row → Col → Value)
    (S : W → Prop) (w : W) (r : Row) (c : Col) (k : Nat)
    (setter : Setter M S r c w k) :
    ∃ g kind, originalCell M scalar w r c = some (.cell r g kind (c, scalar w r c)) := by
  obtain ⟨_, ch, change, kind, sets, _⟩ := setter
  cases hk : ch.kind with
  | ins => exact ⟨ch.gen, .insert, by simp [originalCell, change, sets, hk]⟩
  | upd => exact ⟨ch.gen, .update, by simp [originalCell, change, sets, hk]⟩
  | del => exact (kind hk).elim

/-- Authentication and accounting compose on the exact original cell. A
snapshot can open this leaf without any of that row's overwritten siblings. -/
theorem authenticated_value_accounted (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M)
    (guard : W → Row → Option (Cause W Member Row)) (values : W → Row → Col → Option W)
    (scalar : W → Row → Col → Value) (received : List W) (causal : CausalOrder M received)
    (I : EntryInputs Row K)
    (present : ∀ r, I.present r = decide ((merged M valid p guard received).gen r % 2 = 1))
    (w : W) (r : Row) (c : Col) (k : Nat)
    (setter : Setter M (fun a => a ∈ received) r c w k) (written : values w r c = some w)
    (h : CovenStorage.RowAuthorship.Hashing (CovenStorage.RowAuthorship.Change Row (Col × Value)) Digest)
    (s : CovenStorage.RowAuthorship.Signatures Member W
      (CovenStorage.RowAuthorship.Change Row (Col × Value)) Digest Signature h)
    (member : Member) (signature : Signature)
    (proof : CovenStorage.RowAuthorship.Proof (CovenStorage.RowAuthorship.Change Row (Col × Value)) Digest)
    (signed : s.verifies member w (CovenStorage.RowAuthorship.proofRoot h proof) signature)
    (opened : ∀ leaf, originalCell M scalar w r c = some leaf → leaf ∈ CovenStorage.RowAuthorship.revealed proof) :
    member = s.author w ∧
    (∃ original, s.signed w original ∧ ∃ g kind,
      CovenStorage.RowAuthorship.Change.cell r g kind (c, scalar w r c) ∈ CovenStorage.RowAuthorship.leaves original) ∧
    (CovenMerge.ValueAccounted (DataDependency.effective M valid p (base guard)) (fun a => a ∈ received)
      (merged M valid p guard received) I r c w k ∨
      ∃ record, loss M valid p guard values received w r = some record ∧ record.values c = some w) := by
  obtain ⟨g, kind, leaf⟩ := setter_has_cell_leaf M scalar _ w r c k setter
  obtain ⟨author, original, authored, authentic⟩ :=
    CovenStorage.RowAuthorship.cell_true_author h s member w signature proof r g kind c (scalar w r c)
      signed (opened _ leaf)
  exact ⟨author, ⟨original, authored, g, kind, authentic⟩,
    value_accounted M valid p guard values received causal I present w r c k setter written⟩

end CovenMerge.Ownership
