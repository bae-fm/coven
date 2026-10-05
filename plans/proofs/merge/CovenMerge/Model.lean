/-!
# The merge model: writes, the declarative result, and the incremental step

This file defines

* `Writes`: a finite-or-infinite universe of writes with their metadata
  (timestamp, had-read, and one row change per touched row);
* `Valid`: what the stamping rule and the authoring device guarantee about
  that metadata (§7.2, §8.3);
* the *declarative* result: predicates that say, for a set `S` of writes,
  which generation each row has, which write set each cell, and which values
  are lost. They mention only the set `S` and the writes' metadata, never an
  order of application;
* `St`: what a device stores (the app's cells, `coven_rows` generations with
  the write per generation change, `coven_lost`);
* `step`: how a device applies one arriving write to its state. It reads only
  the state, the timestamps of writes (`coven_writes`), and the arriving
  write's own record (its row changes and its had-read set).

`Core.lean` proves that `step`, applied in any causal order, lands in the
state the declarative predicates describe.
-/

namespace CovenMerge

/-- The kind of one row change (§5). -/
inductive Kind where
  | ins
  | upd
  | del
  deriving DecidableEq, Repr

/-- One row change: its kind, the generation the row had on the writing
device (§8.3), and which columns it sets. A delete sets no column. -/
structure Change (Col : Type) where
  kind : Kind
  gen : Nat
  sets : Col → Bool

/-- The incarnation of the row a change belongs to. An insert at generation
`g` creates incarnation `g + 1`; an update or delete at generation `g` acts on
incarnation `g`. -/
def Change.inc {Col : Type} (ch : Change Col) : Nat :=
  if ch.kind = .ins then ch.gen + 1 else ch.gen

/-- The writes and their metadata. A write is identified by `W`.
* `ts w`: its timestamp, as a number (milliseconds, counter, device id).
* `past w a`: write `w` had read write `a` (§7.1).
* `chg w r`: the change `w` makes to row `r`, if any. A write makes at most one
  change per row, as SQLite's session extension records it. -/
structure Writes (W Row Col : Type) where
  ts : W → Nat
  past : W → W → Bool
  chg : W → Row → Option (Change Col)

section Defs
variable {W Row Col : Type} (M : Writes W Row Col)

/-- What the stamping rule (§7.2) and the authoring device (§8.3) guarantee.
* timestamps are unique;
* a write is stamped later than every write it had read;
* a change's generation was reached on the authoring device: it is `0`, or
  some write the author had read moved the row to it;
* an insert is made at an even generation (row absent), an update or delete
  at an odd one (row present). -/
structure Valid : Prop where
  ts_inj : ∀ a b, M.ts a = M.ts b → a = b
  past_ts : ∀ w a, M.past w a = true → M.ts a < M.ts w
  gen_seen : ∀ w r ch, M.chg w r = some ch →
    ch.gen = 0 ∨ ∃ x ch', M.past w x = true ∧ M.chg x r = some ch' ∧
      ch'.kind ≠ .upd ∧ ch'.gen + 1 = ch.gen
  parity : ∀ w r ch, M.chg w r = some ch → (ch.kind = .ins ↔ ch.gen % 2 = 0)

/-- A set of writes is causally closed when it holds everything its writes
had read. Every device's applied set is, by §7.1. -/
def Closed (S : W → Prop) : Prop :=
  ∀ x, S x → ∀ a, M.past x a = true → S a

/-! ## The declarative result, as predicates on a set `S` of writes -/

/-- `x ∈ S` moves row `r` to generation `n` (an insert or delete at `n - 1`). -/
def GenChange (S : W → Prop) (r : Row) (x : W) (n : Nat) : Prop :=
  S x ∧ ∃ ch, M.chg x r = some ch ∧ ch.kind ≠ .upd ∧ ch.gen + 1 = n

/-- `x ∈ S` sets cell `(r, c)` in incarnation `k` of row `r`. -/
def Setter (S : W → Prop) (r : Row) (c : Col) (x : W) (k : Nat) : Prop :=
  S x ∧ ∃ ch, M.chg x r = some ch ∧ ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = k

/-- `x ∈ S` deletes incarnation `k` of row `r`. -/
def Del (S : W → Prop) (r : Row) (x : W) (k : Nat) : Prop :=
  S x ∧ ∃ ch, M.chg x r = some ch ∧ ch.kind = .del ∧ ch.gen = k

/-- `y` replaces the value `a` set in cell `(r, c)` of incarnation `k`: it sets
the same cell of the same incarnation with a larger timestamp, or it deletes
that incarnation. -/
def Replacer (S : W → Prop) (r : Row) (c : Col) (a : W) (k : Nat) (y : W) : Prop :=
  (Setter M S r c y k ∧ M.ts a < M.ts y) ∨ Del M S r y k

/-- `G` is row `r`'s generation in `S`: the largest generation any write in
`S` moved it to, and every generation below it was reached by some write. -/
def IsGen (S : W → Prop) (r : Row) (G : Nat) : Prop :=
  (∀ x m, GenChange M S r x m → m ≤ G) ∧
  (∀ n, 0 < n → n ≤ G → ∃ x, GenChange M S r x n)

/-- The write recorded for generation `n` of row `r`: of the writes that
moved the row to `n`, the one with the smallest timestamp. -/
def MinGen (S : W → Prop) (r : Row) (n : Nat) (x : W) : Prop :=
  GenChange M S r x n ∧ ∀ y, GenChange M S r y n → M.ts x ≤ M.ts y

/-- The setter of cell `(r, c)` in incarnation `k` with the largest timestamp. -/
def MaxSetter (S : W → Prop) (r : Row) (c : Col) (k : Nat) (x : W) : Prop :=
  Setter M S r c x k ∧ ∀ y, Setter M S r c y k → M.ts y ≤ M.ts x

/-- The delete of incarnation `k` with the smallest timestamp. -/
def MinDel (S : W → Prop) (r : Row) (k : Nat) (x : W) : Prop :=
  Del M S r x k ∧ ∀ y, Del M S r y k → M.ts x ≤ M.ts y

/-- The write a lost value of incarnation `k` names as "replaced by": the
earliest delete of that incarnation if it was deleted, otherwise the cell's
current winner. -/
def Canon (S : W → Prop) (r : Row) (c : Col) (k : Nat) (x : W) : Prop :=
  ((∃ d, Del M S r d k) ∧ MinDel M S r k x) ∨
  ((¬ ∃ d, Del M S r d k) ∧ MaxSetter M S r c k x)

/-- The value `a` set in cell `(r, c)` of incarnation `k` is lost, recorded
as replaced by `x`: some write replaced it, and no write that replaced it had
read it. -/
def Lost (S : W → Prop) (r : Row) (c : Col) (a : W) (k : Nat) (x : W) : Prop :=
  Setter M S r c a k ∧ (∃ y, Replacer M S r c a k y) ∧
  (∀ y, Replacer M S r c a k y → M.past y a = false) ∧ Canon M S r c k x

end Defs

/-! ## What a device stores -/

/-- A device's merge state.
* `gen r`: row `r`'s generation (`coven_rows`).
* `genWrite r n`: the write recorded for generation `n` of row `r`. The spec's
  `coven_rows` keeps this only for the current generation; the model keeps it
  per generation, which §8.3's example needs (see the appendix).
* `cell r c`: the write whose value cell `(r, c)` holds (`coven_cells`), or
  `none` while the row is absent or the cell was never set. The app's value
  is that write's value for the cell.
* `lost r c a`: `coven_lost`'s row for the value write `a` set in cell `(r, c)`:
  the incarnation it was set in and the write recorded as replacing it. -/
structure St (W Row Col : Type) where
  gen : Row → Nat
  genWrite : Row → Nat → Option W
  cell : Row → Col → Option W
  lost : Row → Col → W → Option (Nat × W)

/-- The empty database. -/
def St.init {W Row Col : Type} : St W Row Col where
  gen _ := 0
  genWrite _ _ := none
  cell _ _ := none
  lost _ _ _ := none

section Step
variable {W Row Col : Type} [DecidableEq W] (M : Writes W Row Col)

/-- The earlier, by `ts`, of an optional write and `w`. -/
def minBy (ts : W → Nat) : Option W → W → W
  | none, w => w
  | some y, w => if ts w < ts y then w else y

/-- The later, by `ts`, of an optional write and `w`. -/
def maxBy (ts : W → Nat) : Option W → W → W
  | none, w => w
  | some y, w => if ts y < ts w then w else y

/-- New generation of a row with generation `G` after the arriving change. -/
def genStep (G : Nat) : Option (Change Col) → Nat
  | none => G
  | some ch => if ch.kind ≠ .upd ∧ G < ch.gen + 1 then ch.gen + 1 else G

/-- New write recorded for generation `n`. -/
def genWriteStep (old : Option W) (n : Nat) (w : W) : Option (Change Col) → Option W
  | none => old
  | some ch => if ch.kind ≠ .upd ∧ n = ch.gen + 1 then some (minBy M.ts old w) else old

/-- New setter of cell `(r, c)`, given the row's generation `G` and the cell's
current setter `old`. -/
def cellStep (G : Nat) (old : Option W) (c : Col) (w : W) : Option (Change Col) → Option W
  | none => old
  | some ch =>
    if ch.kind ≠ .upd ∧ G < ch.gen + 1 then
      -- the change moves the row to a new generation: a delete removes the
      -- cells, an insert starts the new incarnation with its own values
      (if ch.kind = .ins ∧ ch.sets c = true then some w else none)
    else if ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = G then
      -- a change to the current incarnation: the larger timestamp wins
      some (maxBy M.ts old w)
    else old

/-- New `coven_lost` row for the value write `a` set in cell `(r, c)`.
Arguments: the row's generation `G`, the generation records `gw`, the cell's
current setter `cur`, the current lost row `old`, the arriving write `w` and
its change `ch` to row `r`. -/
def lostStep (G : Nat) (gw : Nat → Option W) (cur : Option W) (old : Option (Nat × W))
    (c : Col) (a w : W) (ch : Change Col) : Option (Nat × W) :=
  if a = w then
    -- the arriving write's own value
    if ch.kind ≠ .del ∧ ch.sets c = true then
      if ch.inc < G then
        -- made at an incarnation already deleted: lost to that delete
        (gw (ch.inc + 1)).map (fun d => (ch.inc, d))
      else if ch.inc = G then
        match cur with
        | some s => if M.ts w < M.ts s then some (ch.inc, s) else none
        | none => none
      else none
    else none
  else
    match old with
    | some (ka, x) =>
      -- `a` is already lost; it stays lost unless `w` replaces it having read it
      if (ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = ka ∧ M.ts a < M.ts w) ∨
          (ch.kind = .del ∧ ch.gen = ka) then
        if M.past w a = true then none
        else some (ka,
          if ch.kind = .del then (if ka < G then minBy M.ts (some x) w else w)
          else (if ka < G then x else maxBy M.ts (some x) w))
      else some (ka, x)
    | none =>
      -- `a` is not lost; it becomes lost only if it is the current value and
      -- `w` replaces it without having read it
      match cur with
      | some s =>
        if s = a ∧ ((ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = G ∧ M.ts a < M.ts w) ∨
              (ch.kind = .del ∧ ch.gen = G)) ∧ M.past w a = false then some (G, w)
        else none
      | none => none

/-- Apply one arriving write `w` to state `st`. -/
def step (st : St W Row Col) (w : W) : St W Row Col where
  gen r := genStep (st.gen r) (M.chg w r)
  genWrite r n := genWriteStep M (st.genWrite r n) n w (M.chg w r)
  cell r c := cellStep M (st.gen r) (st.cell r c) c w (M.chg w r)
  lost r c a :=
    match M.chg w r with
    | none => st.lost r c a
    | some ch => lostStep M (st.gen r) (st.genWrite r) (st.cell r c) (st.lost r c a) c a w ch

end Step

/-! ## The declarative result as a property of a state -/

/-- State `st` is the result for the set `S`: every field is the one the
declarative predicates give. Nothing here depends on an order of writes. -/
structure IsSpec {W Row Col : Type} (M : Writes W Row Col) (S : W → Prop)
    (st : St W Row Col) : Prop where
  gen : ∀ r, IsGen M S r (st.gen r)
  gw_some : ∀ r n x, st.genWrite r n = some x ↔ MinGen M S r n x
  gw_none : ∀ r n, st.genWrite r n = none → ∀ x, ¬ GenChange M S r x n
  cell_some : ∀ r c x, st.cell r c = some x ↔ MaxSetter M S r c (st.gen r) x
  cell_none : ∀ r c, st.cell r c = none → ∀ x, ¬ Setter M S r c x (st.gen r)
  lost : ∀ r c a k x, st.lost r c a = some (k, x) ↔ Lost M S r c a k x

/-- A causal order of application, oldest first: each write appears once,
after every write it had read. -/
inductive CausalOrder {W Row Col : Type} (M : Writes W Row Col) : List W → Prop where
  | nil : CausalOrder M []
  | snoc {L : List W} {w : W} : CausalOrder M L → w ∉ L →
      (∀ a, M.past w a = true → a ∈ L) → CausalOrder M (L ++ [w])

/-- A causal order of the writes applied after a snapshot covering the set
`C` (§15): covered writes count as applied. -/
inductive CausalFrom {W Row Col : Type} (M : Writes W Row Col) (C : W → Prop) :
    List W → Prop where
  | nil : CausalFrom M C []
  | snoc {L : List W} {w : W} : CausalFrom M C L → ¬ C w → w ∉ L →
      (∀ a, M.past w a = true → C a ∨ a ∈ L) → CausalFrom M C (L ++ [w])

end CovenMerge
