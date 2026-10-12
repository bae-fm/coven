import CovenMerge.OwnerEvaluation

/-! Delete protection in the deleting member's merged context. Only other
members' unread candidate deletes are withheld; the test reads retained references
before ordinary removal rules. Nodes identify row incarnations, so a child of
a reinserted row does not refer to the incarnation an old delete removed. -/
namespace CovenMerge.Ownership

structure ReferenceGraph (Node : Type) where
  rows : List Node
  present : Node → Bool
  parents : Node → List Node

variable {Node Member Op : Type} [DecidableEq Node] [DecidableEq Member]

def referenceFires (g : ReferenceGraph Node) (removed : Node → Bool) (r : Node) : Bool :=
  (g.parents r).any removed

/-- Direct references, cascade and audience-parent links use the same
transitive closure. A target can be absent from the current row list. -/
def references (g : ReferenceGraph Node) (targets : Node → Bool) : Node → Bool :=
  close g.rows (referenceFires g) targets

theorem reference_seed (g : ReferenceGraph Node) (targets : Node → Bool) (r : Node)
    (target : targets r = true) : references g targets r = true :=
  star_grow (close_star g.rows (referenceFires g) targets) r target

theorem reference_parent (g : ReferenceGraph Node) (targets : Node → Bool) (r parent : Node)
    (mem : r ∈ g.rows) (link : parent ∈ g.parents r)
    (affected : references g targets parent = true) : references g targets r = true :=
  normal_closed (close_normal g.rows (referenceFires g) targets) r mem
    (List.any_eq_true.mpr ⟨parent, link, affected⟩)

private theorem reference_monotone (g : ReferenceGraph Node) :
    MonotoneRules (FiresP (referenceFires g)) := by
  intro a b r sub fired
  obtain ⟨p, mem, hit⟩ := List.any_eq_true.mp fired
  exact List.any_eq_true.mpr ⟨p, mem, sub p hit⟩

/-- This is exactly the least transitive reference closure, not a depth cutoff. -/
theorem references_least (g : ReferenceGraph Node) (targets : Node → Bool) :
    ClosedUnder g.rows (FiresP (referenceFires g)) (references g targets) ∧
    (∀ r, targets r = true → references g targets r = true) ∧
    ∀ closed, ClosedUnder g.rows (FiresP (referenceFires g)) closed →
      (∀ r, targets r = true → closed r = true) →
      ∀ r, references g targets r = true → closed r = true :=
  normal_least (reference_monotone g) (close_star _ _ _) (close_normal _ _ _)

/-- Enumeration does not affect reachability, including through cycles. -/
theorem references_order_independent (g : ReferenceGraph Node) (rows : List Node)
    (same : ∀ r, r ∈ g.rows ↔ r ∈ rows) (targets : Node → Bool) :
    references g targets = references { g with rows := rows } targets := by
  have left := references_least g targets
  have right := references_least { g with rows := rows } targets
  have forward := left.2.2 _ (fun r mem hit => right.1 r ((same r).mp mem) hit) right.2.1
  have backward := right.2.2 _ (fun r mem hit => left.1 r ((same r).mpr mem) hit) left.2.1
  funext r
  exact Bool.eq_iff_iff.mpr ⟨forward r, backward r⟩

/-- The deleting member keeps its own candidate deletes. All non-deletes
remain. A foreign delete already read by the deleting write also remains,
even when it is still a candidate for another operation. -/
def memberContext (actor : Op → Member) (isDelete read : Op → Bool)
    (active : List Op) (member : Member) : List Op :=
  active.filter fun op => !isDelete op || decide (actor op = member) || read op

theorem member_context_exact (actor : Op → Member) (isDelete read : Op → Bool)
    (active : List Op) (member : Member) (op : Op) :
    op ∈ memberContext actor isDelete read active member ↔
      op ∈ active ∧ ((isDelete op = false ∨ actor op = member) ∨ read op = true) := by
  simp [memberContext]

theorem read_delete_kept (actor : Op → Member) (isDelete read : Op → Bool)
    (active : List Op) (member : Member) (op : Op)
    (mem : op ∈ active) (seen : read op = true) :
    op ∈ memberContext actor isDelete read active member := by
  simp [member_context_exact, mem, seen]

def referenceBlocker (g : ReferenceGraph Node) (owner : Node → Option Member)
    (actor : Member) (targets : Node → Bool) : Option (Node × Member) :=
  let affected := references g targets
  (g.rows.filterMap fun r => (owner r).map (r, ·)).find? fun (r, member) =>
    g.present r && affected r && decide (member ≠ actor)

def referenceReason {W Row : Type} (g : ReferenceGraph Node) (owner : Node → Option Member)
    (row : Node → Row) (actor : Member) (targets : Node → Bool) : Option (Cause W Member Row) :=
  (referenceBlocker g owner actor targets).map fun (r, member) => .delete (row r) member

theorem reference_blocker_exact (g : ReferenceGraph Node) (owner : Node → Option Member)
    (actor : Member) (targets : Node → Bool) :
    (referenceBlocker g owner actor targets).isSome = true ↔
      ∃ r ∈ g.rows, ∃ member, owner r = some member ∧
        g.present r = true ∧ references g targets r = true ∧ member ≠ actor := by
  simp only [referenceBlocker, List.find?_isSome, List.mem_filterMap, Option.map_eq_some_iff,
    Bool.and_eq_true, decide_eq_true_eq]
  constructor
  · rintro ⟨⟨r, member⟩, ⟨r', mem, m', owned, eq⟩, ⟨present, affected⟩, other⟩
    cases eq
    exact ⟨r, mem, member, owned, present, affected, other⟩
  · rintro ⟨r, mem, member, owned, present, affected, other⟩
    exact ⟨(r, member), ⟨r, mem, member, owned, rfl⟩, ⟨present, affected⟩, other⟩

/-- The veto is independent of graph enumeration. Selecting which blocker
to name still uses the supplied canonical row order. -/
theorem reference_veto_order_independent (g : ReferenceGraph Node) (rows : List Node)
    (same : ∀ r, r ∈ g.rows ↔ r ∈ rows) (owner : Node → Option Member)
    (actor : Member) (targets : Node → Bool) :
    (referenceBlocker g owner actor targets).isSome =
      (referenceBlocker { g with rows := rows } owner actor targets).isSome := by
  apply Bool.eq_iff_iff.mpr
  simp only [reference_blocker_exact, same, references_order_independent g rows same targets]

theorem passing_has_no_foreign_reference (g : ReferenceGraph Node) (owner : Node → Option Member)
    (actor : Member) (targets : Node → Bool) (safe : referenceBlocker g owner actor targets = none)
    (r : Node) (member : Member) (mem : r ∈ g.rows) (owned : owner r = some member)
    (present : g.present r = true) (other : member ≠ actor) : references g targets r = false := by
  cases affected : references g targets r
  · rfl
  · have bad := (reference_blocker_exact g owner actor targets).mpr
      ⟨r, mem, member, owned, present, affected, other⟩
    simp [safe] at bad

variable {W Row Col Table : Type} [DecidableEq W] [DecidableEq Row]

/-- One rebuilt context per delete, using its recorded reads and incarnation.
Ownership comes from the authenticated policy, not a graph publisher. -/
def deleteTrial (M : Writes W Row Col) (P : Policy W Row Table Member)
    (graph : St W Row Col → ReferenceGraph (Row × Nat))
    (build : List (W × Row) → St W Row Col) (active : List (W × Row))
    (w : W) (r : Row) : Option (Cause W Member Row) :=
  (M.chg w r).bind fun ch =>
    if ch.kind = .del then
      let context := memberContext (fun op => P.author op.1)
        (fun op => (M.chg op.1 op.2).any (fun change => decide (change.kind = .del)))
        (fun op => M.past w op.1) active (P.author w)
      let g := graph (build context)
      referenceReason g
        (fun node => if P.owned (P.table node.1) then some (owner P node.1) else none)
        Prod.fst (P.author w) (fun node => decide (node = (r, ch.gen)))
    else none

/-- The graph test does not itself promise protection through uniqueness;
its final view is still the actual three-pass result. -/
theorem delete_result_converges {K : Type} [DecidableEq Col] [DecidableEq K]
    (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List (W × Row))
    (graph : St W Row Col → ReferenceGraph (Row × Nat))
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    {a b : List W} (ca : CausalOrder M a) (cb : CausalOrder M b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    protectedResult M valid p P candidates (deleteTrial M P graph) values inputs a =
      protectedResult M valid p P candidates (deleteTrial M P graph) values inputs b :=
  protected_result_converges M valid p P candidates _ values inputs ca cb same

end CovenMerge.Ownership
