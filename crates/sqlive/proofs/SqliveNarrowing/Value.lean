/-! Values, rows and SQL's three-valued truth.

The model's tables hold NULL and integers only, in columns SQLite compares
without converting a value, so a comparison here is SQLite's, and one under a
collation compares the values' keys (`Cond.collated`). -/
namespace SqliveNarrowing

/-- NULL (`none`) or an integer. -/
abbrev Value := Option Int

/-- SQL's truth: `none` is NULL, which a WHERE treats as false. -/
abbrev Truth := Option Bool

/-- A row's values, by column position. -/
abbrev Row := List Value

/-- The value of column `c`; NULL past the row's end. -/
def Row.cell (r : Row) (c : Nat) : Value := (r[c]?).join

/-- Column `c` of an item's row in a binding: NULL when the item has no row,
as a LEFT JOIN gives a left row no right row joins. -/
def cellOf : Option Row → Nat → Value
  | some r, c => r.cell c
  | none, _ => none

/-- A comparison operator. `is` and `isNot` compare NULL as a value. -/
inductive Op where
  | eq | ne | lt | le | gt | ge | is | isNot
  deriving DecidableEq, Repr, Inhabited

/-- How `op` orders two integers. -/
def Op.compare : Op → Int → Int → Bool
  | .eq, x, y => x == y
  | .ne, x, y => x != y
  | .lt, x, y => decide (x < y)
  | .le, x, y => decide (x ≤ y)
  | .gt, x, y => decide (y < x)
  | .ge, x, y => decide (y ≤ x)
  | .is, x, y => x == y
  | .isNot, x, y => x != y

/-- `a op b` in SQL: NULL on either side gives NULL, except for `IS`. -/
def Op.apply : Op → Value → Value → Truth
  | .is, a, b => some (a == b)
  | .isNot, a, b => some (a != b)
  | op, some x, some y => some (op.compare x y)
  | _, _, _ => none

/-- SQL's AND: false wins, then NULL. -/
def tand : Truth → Truth → Truth
  | some false, _ => some false
  | _, some false => some false
  | some true, some true => some true
  | _, _ => none

/-- SQL's OR: true wins, then NULL. -/
def tor : Truth → Truth → Truth
  | some true, _ => some true
  | _, some true => some true
  | some false, some false => some false
  | _, _ => none

/-- SQL's NOT: NULL stays NULL. -/
def tnot : Truth → Truth
  | some t => some (!t)
  | none => none

/-- Whether a condition keeps a row: only a true one does. -/
def isTrue (t : Truth) : Bool := t == some true

/-- `v IN vs` in SQL. -/
def memberTruth (v : Value) (vs : List Value) : Truth :=
  match v with
  | none => if vs.isEmpty then some false else none
  | some x =>
    if vs.contains (some x) then some true
    else if vs.contains none then none
    else some false

/-- A row value ordered against another, from its first pair's values `a` and
`b`, compared by their keys, and `rest`, the truth of the other pairs'
comparison: NULL where either value is, the first pair's order where its keys
differ, and `rest` where they tie. -/
def rowOrderTruth (key : Int → Int) (op : Op) : Value → Value → Truth → Truth
  | some x, some y, rest => if key x = key y then rest else some (op.compare (key x) (key y))
  | _, _, _ => none

/-- `(a1, ..., an) = (b1, ..., bn)` in SQL: each pair equal, joined by AND. -/
def rowEqTruth : List Value → List Value → Truth
  | a :: as, b :: bs => tand (Op.apply .eq a b) (rowEqTruth as bs)
  | _, _ => some true

/-- `(a1, ..., an) IN rows` in SQL: true when some row equals it, NULL when
none does but the equality of some row is NULL, false otherwise. -/
def rowMemberTruth (vs : List Value) (rows : List (List Value)) : Truth :=
  rows.foldr (fun r acc => tor (rowEqTruth vs r) acc) (some false)

/-- How SQLite orders two values in ascending order: NULL first. -/
def valueCompare : Value → Value → Ordering
  | none, none => .eq
  | none, some _ => .lt
  | some _, none => .gt
  | some x, some y => if x < y then .lt else if x = y then .eq else .gt

/-- How an ORDER BY term orders values: descending when `desc`, and by a
collation. A collation is a total preorder on values, and the model gives it
as a key into the integers' order: two values compare as their keys do, and
tie when their keys are equal, as `'a'` and `'A'` do in NOCASE. BINARY is the
identity key. A term is `exact` when its collation ties no two different
values, as a column of unique values needs of it to tell rows apart. -/
structure Dir where
  desc : Bool
  key : Int → Int
  exact : Bool
  exact_inj : exact = true → Function.Injective key

instance : Repr (Int → Int) := ⟨fun _ _ => "<key>"⟩
instance : Repr Dir := ⟨fun d _ => repr d.desc⟩
instance : Inhabited Dir := ⟨⟨false, id, true, fun _ => Function.injective_id⟩⟩

/-- A term in BINARY collation, descending when `desc`. -/
def Dir.plain (desc : Bool) : Dir := ⟨desc, id, true, fun _ => Function.injective_id⟩

/-- How a term orders two values in ascending order: by their keys, NULL
first. -/
def Dir.compare (d : Dir) (a b : Value) : Ordering := valueCompare (a.map d.key) (b.map d.key)

/-- How an ORDER BY orders two keys, term by term, each as its `Dir` says. -/
def keyCompare : List Dir → List Value → List Value → Ordering
  | d :: ds, a :: as, b :: bs =>
    (if d.desc then (d.compare a b).swap else d.compare a b).then (keyCompare ds as bs)
  | _, _, _ => .eq

/-- Whether `a` sorts at or before `b`. -/
def keyLe (directions : List Dir) (a b : List Value) : Bool :=
  keyCompare directions a b != .gt

/-- Insert `x` before the first element it sorts at or before: stable. -/
def insertSorted {α : Type} (le : α → α → Bool) (x : α) : List α → List α
  | [] => [x]
  | y :: ys => if le x y then x :: y :: ys else y :: insertSorted le x ys

/-- A stable insertion sort. -/
def sortBy {α : Type} (le : α → α → Bool) : List α → List α
  | [] => []
  | x :: xs => insertSorted le x (sortBy le xs)

end SqliveNarrowing
