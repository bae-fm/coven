import CovenMerge.Converge

/-!
# Counterexamples to the spec's rules as written

`Literal` is an executable reading of `coven-from-scratch.md` §8 as written:
each arriving write is applied with the per-arrival rules the spec states,
and a lost row is deleted for good (its generation moves on, so a later
change made at the old generation loses, §8.3). Each `theorem` below runs
two causal orders of the same writes and checks, by evaluation, that the
results differ. The kernel checks them with `decide`; no axioms.

At the end, the §8.1 example is run through the proven `step` of
`Model.lean`, which gives the same `coven_lost` in both orders.

One example table, one column per rule:
* column 0: `title`, a plain column;
* column 1: `parent`, a foreign key with `ON DELETE CASCADE` (0 is null);
* column 2: `link`, a foreign key with `ON DELETE SET NULL` (0 is null);
* columns 3 and 4: `start` and `end`, with `CHECK (start <= end)`;
* column 6: `name`, with a unique constraint (0 is null).
-/

namespace CovenMerge.Literal

structure Cell where
  val : Nat
  /-- the parent generation a reference carries (§8.4) -/
  pg : Nat
  setBy : Nat
  ts : Nat
  deriving DecidableEq, Repr

structure RowSt where
  gen : Nat
  /-- `coven_rows.write`: the write that last changed the generation -/
  genBy : Nat
  cells : Nat → Option Cell

structure LostRow where
  row : Nat
  col : Nat
  val : Nat
  setBy : Nat
  replacedBy : Nat
  deriving DecidableEq, Repr

structure DB where
  row : Nat → RowSt
  lost : List LostRow

/-- `retract`: an ancestor's delete, sent when its last shared child leaves
the store; receivers ignore it while any present row references the
ancestor (current coven's rule). -/
inductive Kind where
  | ins
  | upd
  | del
  | retract
  deriving DecidableEq

structure RC where
  row : Nat
  kind : Kind
  gen : Nat
  /-- (column, value, parent generation) -/
  sets : List (Nat × Nat × Nat)

structure Wr where
  id : Nat
  ts : Nat
  past : List Nat
  changes : List RC

def cols : List Nat := [0, 1, 2, 3, 4, 6]

def init : DB := ⟨fun _ => ⟨0, 0, fun _ => none⟩, []⟩

def present (db : DB) (r : Nat) : Bool := (db.row r).gen % 2 == 1

def val (db : DB) (r c : Nat) : Option Nat :=
  if present db r then ((db.row r).cells c).map (·.val) else none

def setRow (db : DB) (r : Nat) (rs : RowSt) : DB :=
  { db with row := fun x => if x = r then rs else db.row x }

def setCell (rs : RowSt) (c : Nat) (v : Option Cell) : RowSt :=
  { rs with cells := fun x => if x = c then v else rs.cells x }

def findW (all : List Wr) (id : Nat) : Option Wr := all.find? (·.id == id)

/-- `reader` had read write `setter`. -/
def readBy (all : List Wr) (reader setter : Nat) : Bool :=
  match findW all reader with
  | some w => w.past.contains setter
  | none => false

def tsOf (all : List Wr) (id : Nat) : Nat :=
  match findW all id with
  | some w => w.ts
  | none => 0

/-- §8.1, §8.2: the larger timestamp wins; the losing value is recorded if
the winner had not read the write that set it. -/
def lww (all : List Wr) (w : Wr) (r c v pg : Nat) (db : DB) : DB :=
  let rs := db.row r
  let new : Cell := ⟨v, pg, w.id, w.ts⟩
  match rs.cells c with
  | none => setRow db r (setCell rs c (some new))
  | some cur =>
    if cur.ts < w.ts then
      let db' := setRow db r (setCell rs c (some new))
      if w.past.contains cur.setBy then db'
      else { db' with lost := db'.lost ++ [⟨r, c, cur.val, cur.setBy, w.id⟩] }
    else if readBy all cur.setBy w.id then db
    else { db with lost := db.lost ++ [⟨r, c, v, w.id, cur.setBy⟩] }

/-- §8.3: delete a present row by write `by_`, recording the cells set by
writes it had not read. -/
def delRow (all : List Wr) (by_ r : Nat) (db : DB) : DB :=
  let rs := db.row r
  let lost := cols.filterMap fun c =>
    match rs.cells c with
    | some cur => if readBy all by_ cur.setBy then none else some ⟨r, c, cur.val, cur.setBy, by_⟩
    | none => none
  { setRow db r ⟨rs.gen + 1, by_, fun _ => none⟩ with lost := db.lost ++ lost }

/-- §8.3: a change made at an older generation loses; its cells are recorded,
replaced by the write `coven_rows` names. -/
def loseAll (w : Wr) (rc : RC) (db : DB) : DB :=
  let rs := db.row rc.row
  { db with lost := db.lost ++ rc.sets.map fun s => ⟨rc.row, s.1, s.2.1, w.id, rs.genBy⟩ }

/-- §8.3: a second delete of the same generation removes the lost rows of
values it had read. -/
def concurrentDel (w : Wr) (r : Nat) (db : DB) : DB :=
  { db with lost := db.lost.filter fun l => !(l.row == r && w.past.contains l.setBy) }

def referenced (ids : List Nat) (db : DB) (r : Nat) : Bool :=
  ids.any fun x => present db x && ((db.row x).cells 1).any (·.val == r)

def applyRC (all : List Wr) (ids : List Nat) (w : Wr) (db : DB) (rc : RC) : DB :=
  let rs := db.row rc.row
  match rc.kind with
  | .ins =>
    if rc.gen = rs.gen then
      setRow db rc.row ⟨rs.gen + 1, w.id, fun c =>
        (rc.sets.find? (·.1 == c)).map fun s => ⟨s.2.1, s.2.2, w.id, w.ts⟩⟩
    else if rc.gen + 1 = rs.gen then
      rc.sets.foldl (fun d s => lww all w rc.row s.1 s.2.1 s.2.2 d) db
    else loseAll w rc db
  | .upd =>
    if rc.gen = rs.gen then rc.sets.foldl (fun d s => lww all w rc.row s.1 s.2.1 s.2.2 d) db
    else loseAll w rc db
  | .del =>
    if rc.gen = rs.gen then delRow all w.id rc.row db else concurrentDel w rc.row db
  | .retract =>
    if referenced ids db rc.row then db
    else if rc.gen = rs.gen then delRow all w.id rc.row db else concurrentDel w rc.row db

/-- The parent row `x`'s reference in column `c` names, if that reference
dangles: the parent is absent or at a different generation (§8.4). -/
def dangling (db : DB) (x c : Nat) : Option Nat :=
  match (db.row x).cells c with
  | some cell => if cell.val ≠ 0 ∧ (db.row cell.val).gen ≠ cell.pg then some cell.val else none
  | none => none

def cascadeAt (all : List Wr) (db : DB) (x : Nat) : Option (DB → DB) :=
  (dangling db x 1).map fun p d => delRow all (d.row p).genBy x d

def setNullAt (all : List Wr) (db : DB) (x : Nat) : Option (DB → DB) :=
  (dangling db x 2).map fun p d =>
    let g := (d.row p).genBy
    let rs := d.row x
    let old := rs.cells 2
    { setRow d x (setCell rs 2 (some ⟨0, 0, g, tsOf all g⟩)) with
      lost := d.lost ++ (old.map fun o => (⟨x, 2, o.val, o.setBy, g⟩ : LostRow)).toList }

/-- §8.5: of two present rows claiming one value, the one whose write has the
smaller timestamp keeps it; the other is lost. -/
def uniqueAt (all : List Wr) (ids : List Nat) (db : DB) (x : Nat) : Option (DB → DB) :=
  match (db.row x).cells 6 with
  | some t =>
    if t.val = 0 then none
    else
      (ids.find? fun y => y != x && present db y &&
        ((db.row y).cells 6).any fun u => u.val == t.val && decide (u.ts < t.ts)).map
        fun y d => delRow all (((d.row y).cells 6).map (·.setBy) |>.getD 0) x d
  | none => none

/-- §8.6: a row failing its CHECK after a merge is lost. -/
def checkAt (all : List Wr) (db : DB) (x : Nat) : Option (DB → DB) :=
  match (db.row x).cells 3, (db.row x).cells 4 with
  | some s, some e => if e.val < s.val then some fun d => delRow all 0 x d else none
  | _, _ => none

def ruleAt (all : List Wr) (ids : List Nat) (db : DB) (x : Nat) : Option (DB → DB) :=
  if present db x then
    (cascadeAt all db x <|> setNullAt all db x) <|> (uniqueAt all ids db x <|> checkAt all db x)
  else none

/-- §8: apply the rules until no row breaks any. -/
def post (all : List Wr) (ids : List Nat) : Nat → DB → DB
  | 0, db => db
  | n + 1, db =>
    match ids.findSome? (ruleAt all ids db) with
    | some f => post all ids n (f db)
    | none => db

def applyWrite (all : List Wr) (ids : List Nat) (db : DB) (w : Wr) : DB :=
  post all ids 20 (w.changes.foldl (applyRC all ids w) db)

def run (ids : List Nat) (order : List Wr) : DB := order.foldl (applyWrite order ids) init

/-! ## 1. `coven_lost` depends on arrival order (§8.1, §8.2)

The spec's own example. Titles: 100 "Grocery list", 101 "Groceries",
102 "Weekly groceries", 103 "Shopping". -/

def a1 : Wr := ⟨1, 1, [], [⟨42, .ins, 0, [(0, 100, 0)]⟩]⟩
def ana4 : Wr := ⟨4, 13010, [1], [⟨42, .upd, 1, [(0, 101, 0)]⟩]⟩
def ben9 : Wr := ⟨9, 13011, [1, 4], [⟨42, .upd, 1, [(0, 102, 0)]⟩]⟩
def carol2 : Wr := ⟨2, 14000, [1], [⟨42, .upd, 1, [(0, 103, 0)]⟩]⟩

/-- Ben's phone: Ana's write 4, its own write 9, then Carol's write 2. -/
def bensPhone : DB := run [42] [a1, ana4, ben9, carol2]
/-- Carol's tablet: its own write 2, then Ana's write 4 and Ben's write 9. -/
def carolsTablet : DB := run [42] [a1, carol2, ana4, ben9]

theorem ce1_title_agrees : val bensPhone 42 0 = some 103 ∧ val carolsTablet 42 0 = some 103 := by
  decide

theorem ce1_bens_phone : bensPhone.lost = [⟨42, 0, 102, 9, 2⟩] := by decide

theorem ce1_carols_tablet :
    carolsTablet.lost = [⟨42, 0, 101, 4, 2⟩, ⟨42, 0, 102, 9, 2⟩] := by decide

/-! ## 2. `coven_rows.write` depends on arrival order (§8.3)

Two concurrent deletes of one generation: whichever arrives first is
recorded. -/

def mk43 : Wr := ⟨1, 1, [], [⟨43, .ins, 0, [(0, 200, 0)]⟩]⟩
def delA : Wr := ⟨5, 50, [1], [⟨43, .del, 1, []⟩]⟩
def delB : Wr := ⟨6, 60, [1], [⟨43, .del, 1, []⟩]⟩

theorem ce2_gen_write :
    ((run [43] [mk43, delA, delB]).row 43).genBy = 5 ∧
    ((run [43] [mk43, delB, delA]).row 43).genBy = 6 := by decide

/-! ## 3. "Replaced by" after a re-add depends on arrival order (§8.3)

The spec's example: Ana's write 5 creates note 43, her write 7 deletes it,
her write 8 re-adds it; Ben's edit at generation 1 is stamped after 17:00.
The spec says every device records Ben's value as replaced by Ana's write 7
(id 18). A device that gets Ben's edit after the re-add only has the re-add
(id 19) in `coven_rows`. -/

def ana5 : Wr := ⟨16, 1445, [], [⟨43, .ins, 0, [(0, 200, 0)]⟩]⟩
def ana7 : Wr := ⟨18, 1600, [16], [⟨43, .del, 1, []⟩]⟩
def ana8 : Wr := ⟨19, 1700, [16, 18], [⟨43, .ins, 2, [(0, 201, 0)]⟩]⟩
def ben10 : Wr := ⟨10, 1800, [16], [⟨43, .upd, 1, [(0, 202, 0)]⟩]⟩

theorem ce3_replaced_by :
    (run [43] [ana5, ben10, ana7, ana8]).lost = [⟨43, 0, 202, 10, 18⟩] ∧
    (run [43] [ana5, ana7, ana8, ben10]).lost = [⟨43, 0, 202, 10, 19⟩] := by decide

/-! ## 4. Cascade for a child the deleter never had (§8.4)

Ana makes notes 43 and 44, then tag 9 on note 43. Ben, having both, moves
tag 9 to note 44. Carol, having only the notes, deletes note 43. -/

def notes : Wr := ⟨1, 1, [], [⟨43, .ins, 0, [(0, 1, 0)]⟩, ⟨44, .ins, 0, [(0, 2, 0)]⟩]⟩
def tag9 : Wr := ⟨2, 2, [1], [⟨9, .ins, 0, [(1, 43, 1)]⟩]⟩
def moveTag : Wr := ⟨3, 3, [1, 2], [⟨9, .upd, 1, [(1, 44, 1)]⟩]⟩
def del43 : Wr := ⟨4, 4, [1], [⟨43, .del, 1, []⟩]⟩

theorem ce4_cascade :
    (present (run [43, 44, 9] [notes, tag9, moveTag, del43]) 9 = true ∧
      val (run [43, 44, 9] [notes, tag9, moveTag, del43]) 9 1 = some 44) ∧
    present (run [43, 44, 9] [notes, del43, tag9, moveTag]) 9 = false := by decide

/-! ## 5. Set null for a child the deleter never had (§8.4)

The same, with link 6 through a set-null key. The null coven records is set
by Carol's delete, stamped after Ben's move, so Ben's move loses to it in
one order and never meets it in the other. -/

def link6 : Wr := ⟨2, 2, [1], [⟨6, .ins, 0, [(2, 43, 1)]⟩]⟩
def moveLink : Wr := ⟨3, 3, [1, 2], [⟨6, .upd, 1, [(2, 44, 1)]⟩]⟩

theorem ce5_set_null :
    val (run [43, 44, 6] [notes, link6, moveLink, del43]) 6 2 = some 44 ∧
    val (run [43, 44, 6] [notes, del43, link6, moveLink]) 6 2 = some 0 := by decide

/-! ## 6. A unique loser is deleted for good (§8.5)

Ana's note 45 and Ben's note 46 both claim name 7; Ana's write is earlier.
Ben, before seeing Ana's, renames his note to 8. -/

def ana45 : Wr := ⟨10, 10, [], [⟨45, .ins, 0, [(6, 7, 0)]⟩]⟩
def ben46 : Wr := ⟨11, 11, [], [⟨46, .ins, 0, [(6, 7, 0)]⟩]⟩
def rename46 : Wr := ⟨12, 12, [11], [⟨46, .upd, 1, [(6, 8, 0)]⟩]⟩

theorem ce6_unique :
    present (run [45, 46] [ana45, ben46, rename46]) 46 = false ∧
    present (run [45, 46] [ben46, rename46, ana45]) 46 = true := by decide

/-! ## 7. A CHECK loser is deleted for good (§8.6)

The spec's example (start 5, end 12; Ana sets start 10, Ben sets end 8),
plus Ben's later fix, end 20, made before he saw Ana's write. -/

def row50 : Wr := ⟨1, 1, [], [⟨50, .ins, 0, [(3, 5, 0), (4, 12, 0)]⟩]⟩
def setStart : Wr := ⟨10, 10, [1], [⟨50, .upd, 1, [(3, 10, 0)]⟩]⟩
def setEnd : Wr := ⟨11, 11, [1], [⟨50, .upd, 1, [(4, 8, 0)]⟩]⟩
def fixEnd : Wr := ⟨12, 12, [1, 11], [⟨50, .upd, 1, [(4, 20, 0)]⟩]⟩

theorem ce7_check :
    present (run [50] [row50, setStart, setEnd, fixEnd]) 50 = false ∧
    present (run [50] [row50, setEnd, fixEnd, setStart]) 50 = true := by decide

/-! ## 8. Unique and cascade chained (§8, "applies them until no row breaks any")

Folder 70; Ana's note 45 in it, named 7; Ben's note 46, named 7, later
stamp; Carol deletes the folder. -/

def folder : Wr := ⟨1, 1, [], [⟨70, .ins, 0, [(0, 1, 0)]⟩]⟩
def ana45f : Wr := ⟨10, 10, [1], [⟨45, .ins, 0, [(6, 7, 0), (1, 70, 1)]⟩]⟩
def ben46f : Wr := ⟨11, 11, [1], [⟨46, .ins, 0, [(6, 7, 0)]⟩]⟩
def delFolder : Wr := ⟨12, 12, [1], [⟨70, .del, 1, []⟩]⟩

theorem ce8_unique_cascade :
    present (run [70, 45, 46] [folder, ana45f, ben46f, delFolder]) 46 = false ∧
    present (run [70, 45, 46] [folder, delFolder, ana45f, ben46f]) 46 = true := by decide

/-! ## 9. Ancestor retraction (current coven's place rule)

Ancestor 80 with shared child 81. Ana moves 81 out of the store, so her
write deletes 81 and retracts 80. Ben, concurrently, adds shared child 82 of
80, carrying 80 in his write. Receivers ignore 80's retraction while a
present row references it. -/

def anc : Wr := ⟨1, 1, [], [⟨80, .ins, 0, [(0, 5, 0)]⟩, ⟨81, .ins, 0, [(1, 80, 1)]⟩]⟩
def unshare : Wr := ⟨10, 10, [1], [⟨81, .del, 1, []⟩, ⟨80, .retract, 1, []⟩]⟩
def newChild : Wr := ⟨11, 11, [1], [⟨82, .ins, 0, [(1, 80, 1)]⟩, ⟨80, .upd, 1, [(0, 5, 0)]⟩]⟩

theorem ce9_ancestor :
    (present (run [80, 81, 82] [anc, unshare, newChild]) 80 = false ∧
      present (run [80, 81, 82] [anc, unshare, newChild]) 82 = false) ∧
    (present (run [80, 81, 82] [anc, newChild, unshare]) 80 = true ∧
      present (run [80, 81, 82] [anc, newChild, unshare]) 82 = true) := by decide

end CovenMerge.Literal

/-! ## The §8.1 example through the proven `step`

Writes are named by their ids: 1 Ana's write 1, 4 Ana's write 4, 9 Ben's
write 9, 2 Carol's write 2. Row 42, column 0 is the title. -/

namespace CovenMerge.Example

def M : Writes Nat Nat Nat where
  ts w := if w = 1 then 1 else if w = 4 then 13010 else if w = 9 then 13011 else 14000
  past w a := (w = 4 && a = 1) || (w = 9 && (a = 1 || a = 4)) || (w = 2 && a = 1)
  chg w r :=
    if r = 42 then
      if w = 1 then some ⟨.ins, 0, fun c => c = 0⟩
      else if w ∈ [4, 9, 2] then some ⟨.upd, 1, fun c => c = 0⟩ else none
    else none

def bensPhone : St Nat Nat Nat := [1, 4, 9, 2].foldl (step M) St.init
def carolsTablet : St Nat Nat Nat := [1, 2, 4, 9].foldl (step M) St.init

/-- Both orders: the title holds Carol's value; Ben's "Weekly groceries" is
lost, replaced by Carol's write; Ana's "Groceries" is not lost, since Ben's
write, which replaced it, had read it. -/
theorem example_8_1 :
    bensPhone.cell 42 0 = some 2 ∧ carolsTablet.cell 42 0 = some 2 ∧
    bensPhone.lost 42 0 9 = some (1, 2) ∧ carolsTablet.lost 42 0 9 = some (1, 2) ∧
    bensPhone.lost 42 0 4 = none ∧ carolsTablet.lost 42 0 4 = none := by decide

end CovenMerge.Example
