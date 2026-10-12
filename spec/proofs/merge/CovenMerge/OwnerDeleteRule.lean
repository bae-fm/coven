import CovenMerge.OwnerEvaluation

/-! Delete protection in the deleting member's merged context. Only other
members' unread candidate deletes are withheld; the test reads retained references
before ordinary removal rules, then compares the complete views. A superseded
delete remains generation evidence for later incarnations: withholding its removal must not exclude its reinsertion.
Nodes identify row incarnations, so a child of
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

/-- Every path through retained references is traversed, including through
an unowned intermediate row or any number of audience-parent links. -/
theorem reference_path (g : ReferenceGraph Node) (targets : Node → Bool) (target r : Node)
    (seed : targets target = true)
    (path : Star (fun parent child => child ∈ g.rows ∧ parent ∈ g.parents child) target r) :
    references g targets r = true := by
  have walk (affected : references g targets target = true) : references g targets r = true := by
    clear seed
    induction path with
    | refl => exact affected
    | head link _ ih => exact ih (reference_parent g targets _ _ link.1 link.2 affected)
  exact walk (reference_seed g targets target seed)

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

/-- Only a delete which can still remove the current incarnation is withheld.
Earlier deletes remain as generation evidence, without removing the later row.
This distinction belongs to trials; a rejected change still loses its dependents
in the actual merge. The generation comes from the full retained round. -/
def incarnationContext (M : Writes W Row Col) (P : Policy W Row Table Member)
    (st : St W Row Col) (active : List (W × Row)) (w : W) : List (W × Row) :=
  memberContext (fun op => P.author op.1)
    (fun op => (M.chg op.1 op.2).any fun ch =>
      decide (ch.kind = .del) && decide (st.gen op.2 ≤ ch.gen + 1))
    (fun op => M.past w op.1) active (P.author w)

theorem incarnation_context_exact (M : Writes W Row Col) (P : Policy W Row Table Member)
    (st : St W Row Col) (active : List (W × Row)) (w a : W) (r : Row) (ch : Change Col)
    (change : M.chg a r = some ch) :
    (a, r) ∈ incarnationContext M P st active w ↔
      (a, r) ∈ active ∧ ((ch.kind ≠ .del ∨ ch.gen + 1 < st.gen r) ∨
        P.author a = P.author w ∨ M.past w a = true) := by
  by_cases deleted : ch.kind = .del <;>
    simp [incarnationContext, memberContext, change, deleted, or_assoc]

/-- One rebuilt context per delete, using its recorded reads and incarnation.
Ownership comes from the authenticated policy, not a graph publisher. -/
def referenceDeleteTrial (M : Writes W Row Col) (P : Policy W Row Table Member)
    (graph : St W Row Col → ReferenceGraph (Row × Nat))
    (build : List (W × Row) → St W Row Col) (active : List (W × Row))
    (w : W) (r : Row) : Option (Cause W Member Row) :=
  (M.chg w r).bind fun ch =>
    if ch.kind = .del then
      let context := incarnationContext M P (build active) active w
      let g := graph (build context)
      referenceReason g
        (fun node => if P.owned (P.table node.1) then some (owner P node.1) else none)
        Prod.fst (P.author w) (fun node => decide (node = (r, ch.gen)))
    else none

/-- Compare completed views, including CHECK, reference, unique and audience
rules. The row named by a loss is selected in the supplied canonical order. -/
def effectsReason {K : Type} [DecidableEq K]
    (P : Policy W Row Table Member) (actor : Member)
    (before after : EntryInputs Row K) : Option (Cause W Member Row) :=
  (before.rows.find? fun r => P.owned (P.table r) && decide (owner P r ≠ actor) &&
    (entryView before).shown r && !(entryView after).shown r).map fun r => .delete r (owner P r)

theorem effects_reason_exact {K : Type} [DecidableEq K]
    (P : Policy W Row Table Member) (actor : Member) (before after : EntryInputs Row K) :
    (effectsReason P actor before after).isSome = true ↔
      ∃ r ∈ before.rows, P.owned (P.table r) = true ∧ owner P r ≠ actor ∧
        (entryView before).shown r = true ∧ (entryView after).shown r = false := by
  simp only [effectsReason, Option.isSome_map, List.find?_isSome, Bool.and_eq_true,
    decide_eq_true_eq, Bool.not_eq_true']
  constructor
  · rintro ⟨r, mem, ⟨⟨owned, other⟩, shown⟩, hidden⟩
    exact ⟨r, mem, owned, other, shown, hidden⟩
  · rintro ⟨r, mem, owned, other, shown, hidden⟩
    exact ⟨r, mem, ⟨⟨owned, other⟩, shown⟩, hidden⟩

/-- This is universal over all supplied removal inputs, without assumptions
about which rule changed or how many rows or claims are involved. -/
theorem effects_safe {K : Type} [DecidableEq K]
    (P : Policy W Row Table Member) (actor : Member) (before after : EntryInputs Row K)
    (safe : effectsReason P actor before after = none)
    (r : Row) (mem : r ∈ before.rows) (owned : P.owned (P.table r) = true)
    (other : owner P r ≠ actor) (shown : (entryView before).shown r = true) :
    (entryView after).shown r = true := by
  cases hidden : (entryView after).shown r
  · have bad := (effects_reason_exact P actor before after).mpr ⟨r, mem, owned, other, shown, hidden⟩
    simp [safe] at bad
  · rfl

/-- Withhold this candidate's current removal for the before view. A delete
superseded by a later incarnation remains evidence in both views. Other
candidates stay as selected by the deleting member's incarnation context. -/
def beforeDelete (st : St W Row Col) (context : List (W × Row))
    (w : W) (r : Row) (ch : Change Col) : List (W × Row) :=
  if ch.gen + 1 < st.gen r then context else context.filter fun op => decide (op ≠ (w, r))

/-- Preserve the retained-reference veto and additionally evaluate all three
removal passes with and without this candidate's current removal. Both sides
rebuild from original writes; neither consumes a partially removed view. -/
def deleteTrial {K : Type} [DecidableEq K]
    (M : Writes W Row Col) (P : Policy W Row Table Member)
    (graph : St W Row Col → ReferenceGraph (Row × Nat))
    (inputs : St W Row Col → EntryInputs Row K)
    (build : List (W × Row) → St W Row Col) (active : List (W × Row))
    (w : W) (r : Row) : Option (Cause W Member Row) :=
  (M.chg w r).bind fun ch =>
    if ch.kind = .del then
      match referenceDeleteTrial M P graph build active w r with
      | some reason => some reason
      | none =>
        let st := build active
        let context := incarnationContext M P st active w
        effectsReason P (P.author w)
          (inputs (build (beforeDelete st context w r ch))) (inputs (build context))
    else none

/-- Adding the full-effects comparison never permits a reference veto. -/
theorem delete_trial_reference_safe {K : Type} [DecidableEq K]
    (M : Writes W Row Col) (P : Policy W Row Table Member)
    (graph : St W Row Col → ReferenceGraph (Row × Nat))
    (inputs : St W Row Col → EntryInputs Row K)
    (build : List (W × Row) → St W Row Col) (active : List (W × Row))
    (w : W) (r : Row) (safe : deleteTrial M P graph inputs build active w r = none) :
    referenceDeleteTrial M P graph build active w r = none := by
  cases change : M.chg w r with
  | none => simp [referenceDeleteTrial, change]
  | some ch =>
    by_cases deletes : ch.kind = .del
    · simp only [deleteTrial, change, Option.bind_some, deletes, ↓reduceIte] at safe
      cases ref : referenceDeleteTrial M P graph build active w r with
      | none => rfl
      | some reason => simp [ref] at safe
    · simp [referenceDeleteTrial, change, deletes]

/-- A passing candidate preserves every foreign row shown in its tested
before view. This local statement does not identify that view with a baseline
in which several other deletions have also been withheld. -/
theorem delete_trial_effects_safe {K : Type} [DecidableEq K]
    (M : Writes W Row Col) (P : Policy W Row Table Member)
    (graph : St W Row Col → ReferenceGraph (Row × Nat))
    (inputs : St W Row Col → EntryInputs Row K)
    (build : List (W × Row) → St W Row Col) (active : List (W × Row))
    (w : W) (r : Row) (ch : Change Col) (change : M.chg w r = some ch)
    (deletes : ch.kind = .del) (safe : deleteTrial M P graph inputs build active w r = none)
    (target : Row)
    (mem : target ∈ (inputs (build (beforeDelete (build active)
      (incarnationContext M P (build active) active w) w r ch))).rows)
    (owned : P.owned (P.table target) = true) (other : owner P target ≠ P.author w)
    (shown : (entryView (inputs (build (beforeDelete (build active)
      (incarnationContext M P (build active) active w) w r ch)))).shown target = true) :
    (entryView (inputs (build (incarnationContext M P (build active) active w)))).shown target = true := by
  simp only [deleteTrial, change, Option.bind_some, deletes, ↓reduceIte] at safe
  cases ref : referenceDeleteTrial M P graph build active w r with
  | some reason => simp [ref] at safe
  | none =>
    simp only [ref] at safe
    exact effects_safe P (P.author w) _ _ safe target mem owned other shown

/-- Agreement includes the full-effects verdict and every recorded loss. -/
theorem delete_result_converges {K : Type} [DecidableEq Col] [DecidableEq K]
    (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List (W × Row))
    (graph : St W Row Col → ReferenceGraph (Row × Nat))
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    {a b : List W} (ca : CausalOrder M a) (cb : CausalOrder M b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    protectedResult M valid p P candidates (deleteTrial M P graph inputs) values inputs a =
      protectedResult M valid p P candidates (deleteTrial M P graph inputs) values inputs b :=
  protected_result_converges M valid p P candidates _ values inputs ca cb same

end CovenMerge.Ownership
