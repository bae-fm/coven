import CovenMerge.Converge

/-!
# Foreign keys on generated columns (§9.1)

A generated column's value is computed by SQL from the stored columns it
reads. Coven merges stored cells one by one, so when a foreign key reads a
generated column, concurrent writes to different inputs can combine into a
reference that no write named.

* `unwritten_reference`: a single-column key on `ref AS (a || b)`. Ana sets
  `a`, Ben concurrently sets `b`. Both arrival orders show a reference that
  neither write, nor the insert, recorded, so no parent check any author ran
  covers it: the foreign key fails on every device.
* `grouped_cells_share_winner`: when every change sets either all of a group of
  columns or none of them, every cell of that group holds the same write.
* `grouped_reference_written`: every key groups its columns and generated
  inputs (§9.1), including a single generated column. Every reference a device
  shows is then the tuple one write set.

Computing generated inputs from SQL and capturing a group's components are
outside the model: `Grouped` states the capture rule as a hypothesis, as the
composite-reference differential does for Rust.
-/

namespace CovenMerge
namespace GeneratedReference

section General
variable {W Row Col V : Type} (M : Writes W Row Col)

/-- Every change sets either all of `G` or none of it: the capture rule for a
reference group, “changing any component records every component” (§9.1). -/
def Grouped (G : Col → Prop) : Prop :=
  ∀ w r ch c c', M.chg w r = some ch → G c → G c' → ch.sets c = ch.sets c'

variable {M}

theorem setter_grouped {G : Col → Prop} (hg : Grouped M G) {S : W → Prop} {r : Row}
    {c c' : Col} (hc : G c) (hc' : G c') (x : W) (k : Nat) :
    Setter M S r c x k ↔ Setter M S r c' x k := by
  constructor
  · intro ⟨hS, ch, hch, hk, hs, hi⟩
    exact ⟨hS, ch, hch, hk, (hg x r ch c c' hch hc hc') ▸ hs, hi⟩
  · intro ⟨hS, ch, hch, hk, hs, hi⟩
    exact ⟨hS, ch, hch, hk, (hg x r ch c c' hch hc hc').symm ▸ hs, hi⟩

/-- **A captured group has one winner.** Every cell of a group that changes
only together holds the same write, in any state that is the merge result. -/
theorem grouped_cells_share_winner {G : Col → Prop} (hg : Grouped M G) {S : W → Prop}
    {st : St W Row Col} (h : IsSpec M S st) (r : Row) {c c' : Col} (hc : G c) (hc' : G c') :
    st.cell r c = st.cell r c' :=
  option_eq_of_iff fun x => by
    rw [h.cell_some, h.cell_some]
    unfold MaxSetter
    constructor
    · intro ⟨hx, hmax⟩
      exact ⟨(setter_grouped hg hc hc' x _).1 hx,
        fun y hy => hmax y ((setter_grouped hg hc hc' y _).2 hy)⟩
    · intro ⟨hx, hmax⟩
      exact ⟨(setter_grouped hg hc hc' x _).2 hx,
        fun y hy => hmax y ((setter_grouped hg hc hc' y _).1 hy)⟩

/-! ## References as SQL computes them -/

/-- A table's columns: which are generated, the stored columns each generated
column reads (transitively), and its expression over stored values. -/
structure Columns (Col V : Type) where
  generated : Col → Bool
  inputs : Col → List Col
  expr : Col → (Col → Option V) → Option V
  reads_inputs : ∀ c σ σ', (∀ i ∈ inputs c, σ i = σ' i) → expr c σ = expr c σ'

/-- What one column reads as, from the stored values `σ`. -/
def Columns.value (T : Columns Col V) (σ : Col → Option V) (c : Col) : Option V :=
  if T.generated c then T.expr c σ else σ c

/-- A stored cell shows the value its winning write set. -/
def shown (val : W → Col → V) (st : St W Row Col) (r : Row) (c : Col) : Option V :=
  (st.cell r c).map (val · c)

/-- The reference a device shows for key columns `k` of row `r`. -/
def reference (T : Columns Col V) (val : W → Col → V) (st : St W Row Col) (r : Row)
    (k : List Col) : List (Option V) :=
  k.map (T.value (shown val st r))

/-- The tuple one write `x` set in all of a group's cells, or all NULL for none. -/
def setBy (T : Columns Col V) (val : W → Col → V) (x : Option W) (k : List Col) :
    List (Option V) :=
  k.map (T.value fun c => x.map (val · c))

/-- Every key groups its columns and its generated columns' stored inputs.
For a single stored column this is a singleton, requiring nothing extra. -/
def captureGroup (T : Columns Col V) (k : List Col) (c : Col) : Prop :=
  c ∈ k ∨ ∃ g ∈ k, T.generated g = true ∧ c ∈ T.inputs g

/-- **Grouping preserves written references.** A key's shown reference is
one write's tuple, including single generated columns and constant expressions. -/
theorem grouped_reference_written (T : Columns Col V) (val : W → Col → V)
    (k : List Col) (hg : Grouped M (captureGroup T k)) {S : W → Prop}
    {st : St W Row Col} (h : IsSpec M S st) (r : Row) :
    ∃ x, reference T val st r k = setBy T val x k := by
  cases k with
  | nil => exact ⟨none, rfl⟩
  | cons c₀ cs =>
    have hG₀ : captureGroup T (c₀ :: cs) c₀ := Or.inl List.mem_cons_self
    have same : ∀ c, captureGroup T (c₀ :: cs) c → shown val st r c =
        (st.cell r c₀).map (val · c) := fun c hc => by
      unfold shown
      rw [grouped_cells_share_winner hg h r hc hG₀]
    refine ⟨st.cell r c₀, ?_⟩
    unfold reference setBy
    apply List.map_congr_left
    intro c hc
    unfold Columns.value
    split
    · next hgen =>
      apply T.reads_inputs
      intro i hi
      exact same i (Or.inr ⟨c, hc, hgen, hi⟩)
    · exact same c (Or.inl hc)

variable [DecidableEq W]

/-- Every causal order of valid writes shows a written reference for every
key whose columns and generated inputs are captured together. -/
theorem device_reference_written (hV : Valid M) (T : Columns Col V) (val : W → Col → V)
    (k : List Col) (hg : Grouped M (captureGroup T k))
    {L : List W} (hL : CausalOrder M L) (r : Row) :
    ∃ x, reference T val (L.foldl (step M) St.init) r k = setBy T val x k :=
  grouped_reference_written T val k hg (run_isSpec hV hL) r

end General

/-! ## A generated reference cannot always follow an assignment

Grouping does not make a generated column writable. For `ref AS ('p')`, no
assignment to stored inputs can produce the NULL required by SET NULL, or the
new key required by ON UPDATE CASCADE. Here 7 is 'p' and 8 is 'q'. -/

namespace ConstantAction

def columns : Columns Unit Nat where
  generated _ := true
  inputs _ := []
  expr _ _ := some 7
  reads_inputs := by intros; rfl

/-- Even allowing every stored input to change cannot realize either action. -/
theorem generated_constant_cannot_follow_actions (σ : Unit → Option Nat) :
    columns.value σ () ≠ none ∧ columns.value σ () ≠ some 8 := by
  simp [Columns.value, columns]

end ConstantAction

/-! ## A single-column key without grouped inputs

Row 0 of `c(a, b, ref AS (a || b) REFERENCES p(id))`. Columns: 0 is `a`, 1 is
`b`, 2 is `ref`. Write 0 inserts `a = 1, b = 2`. Ana's write 1 sets `a = 3`;
Ben's write 2, concurrent with it, sets `b = 4`. A pair stands for the
concatenated text. -/

namespace Unwritten

def M : Writes Nat Nat Nat where
  ts w := w + 1
  past w a := (w = 1 || w = 2) && a = 0
  chg w r :=
    if r ≠ 0 then none
    else if w = 0 then some ⟨.ins, 0, fun c => c = 0 || c = 1⟩
    else if w = 1 then some ⟨.upd, 1, fun c => c = 0⟩
    else if w = 2 then some ⟨.upd, 1, fun c => c = 1⟩
    else none

theorem valid : Valid M where
  ts_inj a b h := by simp [M] at h; omega
  past_ts w a h := by
    simp only [M, Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq] at h
    simp only [M]; omega
  gen_seen w r ch h := by
    simp only [M] at h
    by_cases hr : r = 0
    · subst hr
      by_cases h0 : w = 0
      · subst h0; simp at h; subst h; exact Or.inl rfl
      by_cases h1 : w = 1
      · subst h1; simp at h; subst h
        exact Or.inr ⟨0, ⟨.ins, 0, fun c => c = 0 || c = 1⟩, by simp [M], by simp [M], by simp, rfl⟩
      by_cases h2 : w = 2
      · subst h2; simp at h; subst h
        exact Or.inr ⟨0, ⟨.ins, 0, fun c => c = 0 || c = 1⟩, by simp [M], by simp [M], by simp, rfl⟩
      simp [h0, h1, h2] at h
    · simp [hr] at h
  parity w r ch h := by
    simp only [M] at h
    by_cases hr : r = 0
    · subst hr
      by_cases h0 : w = 0
      · subst h0; simp at h; subst h; simp
      by_cases h1 : w = 1
      · subst h1; simp at h; subst h; simp
      by_cases h2 : w = 2
      · subst h2; simp at h; subst h; simp
      simp [h0, h1, h2] at h
    · simp [hr] at h

/-- The values each write set: `a` and `b` as numbers. -/
def val (w c : Nat) : Nat × Nat :=
  if w = 0 then (if c = 0 then (1, 0) else (0, 2))
  else if w = 1 then (3, 0) else (0, 4)

/-- The shown reference: `a`'s and `b`'s values, as `a || b` concatenates them. -/
def ref (st : St Nat Nat Nat) : Option (Nat × Nat) :=
  match shown val st 0 0, shown val st 0 1 with
  | some a, some b => some (a.1, b.2)
  | _, _ => none

/-- The reference each write recorded: what its author's device showed after
applying everything it had read and then the write itself. -/
def recorded (w : Nat) : Option (Nat × Nat) :=
  ref (([0, w].eraseDups).foldl (step M) St.init)

/-- **Ungrouped inputs are unsafe.** Whether Ana's or Ben's write
arrives first, the device shows `ref = 'qz'` (3, 4), which no write recorded.
Every author checked its own reference against an existing parent, so with
exactly those parents the foreign key fails on every device. -/
theorem unwritten_reference :
    let first := [0, 1, 2].foldl (step M) St.init
    let second := [0, 2, 1].foldl (step M) St.init
    ref first = some (3, 4) ∧ ref second = some (3, 4) ∧
    recorded 0 = some (1, 2) ∧ recorded 1 = some (3, 2) ∧ recorded 2 = some (1, 4) := by
  decide

/-- Both orders are causal, so this is the merge result every device reaches. -/
theorem orders_causal : CausalOrder M [0, 1, 2] ∧ CausalOrder M [0, 2, 1] := by
  refine ⟨?_, ?_⟩ <;>
  · refine .snoc (L := [0, _]) (.snoc (L := [0]) (.snoc (L := []) .nil (by simp) ?_) (by simp) ?_)
      (by simp) ?_ <;> intro a ha <;> simp [M] at ha ⊢ <;> omega

/-- With §9.1's capture (`a` and `b` recorded together), the same edits show
Ben's whole tuple, which he recorded. -/
def Mgroup : Writes Nat Nat Nat :=
  { M with chg := fun w r =>
      if r ≠ 0 then none
      else if w = 0 then some ⟨.ins, 0, fun c => c = 0 || c = 1⟩
      else if w = 1 || w = 2 then some ⟨.upd, 1, fun c => c = 0 || c = 1⟩
      else none }

/-- Grouped values: each write sets both `a` and `b`. -/
def valGroup (w c : Nat) : Nat × Nat :=
  if w = 0 then (if c = 0 then (1, 0) else (0, 2))
  else if w = 1 then (if c = 0 then (3, 0) else (0, 2))
  else (if c = 0 then (1, 0) else (0, 4))

def refGroup (st : St Nat Nat Nat) : Option (Nat × Nat) :=
  match shown valGroup st 0 0, shown valGroup st 0 1 with
  | some a, some b => some (a.1, b.2)
  | _, _ => none

theorem grouped_reference_recorded :
    refGroup ([0, 1, 2].foldl (step Mgroup) St.init) = some (1, 4) ∧
    refGroup ([0, 2, 1].foldl (step Mgroup) St.init) = some (1, 4) ∧
    refGroup ([0, 2].foldl (step Mgroup) St.init) = some (1, 4) := by
  decide

end Unwritten

end GeneratedReference
end CovenMerge
