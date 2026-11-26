import SqliveNarrowing.Syntax

/-! What a statement returns from a database.

A table's rows sit in rowid slots, so a write is two databases that differ in
some slots, and a row's place in a scan is its slot. A core joins its items
left to right, keeps the bindings its conditions hold for, and outputs a row
for each, or one for each group. A FROM item's rows are read under the
binding around its core; a subquery's under the current binding. Evaluation
takes fuel, and every theorem holds for every fuel. -/
namespace SqliveNarrowing

/-- Each table's rows, by rowid slot. -/
abbrev Db := Nat → List (Option Row)

/-- Table `t`'s rows, in slot order. -/
def Db.rows (db : Db) (t : Nat) : List Row := (db t).filterMap id

/-- Each item's row in one evaluation: `none` for a LEFT JOIN's missing row,
or an item not bound. -/
abbrev Bind := Nat → Option Row

def Bind.set (b : Bind) (i : Nat) (r : Option Row) : Bind :=
  fun j => if j = i then r else b j

/-- The binding with no item bound. -/
def Bind.empty : Bind := fun _ => none

/-- The rows each subquery returns under a binding. -/
abbrev Subs := Nat → List Row

def Expr.eval (b : Bind) (sub : Subs) : Expr → Value
  | .col i c => cellOf (b i) c
  | .lit v => v
  | .add l r =>
    match l.eval b sub, r.eval b sub with
    | some x, some y => some (x + y)
    | _, _ => none
  | .greatest l r =>
    match l.eval b sub, r.eval b sub with
    | some x, some y => some (max x y)
    | _, _ => none
  | .sub q => ((sub q).head?.bind List.head?).join
  | .coalesce l r =>
    match l.eval b sub with
    | some x => some x
    | none => r.eval b sub
  | .iif t y n =>
    match t.eval b sub with
    | some x => if x ≠ 0 then y.eval b sub else n.eval b sub
    | none => n.eval b sub
  | .isNull e => some (if (e.eval b sub).isNone then 1 else 0)

def Cond.eval (b : Bind) (sub : Subs) : Cond → Truth
  | .cmp op l r => op.apply (l.eval b sub) (r.eval b sub)
  | .null e n => some ((e.eval b sub).isNone != n)
  | .inList e vs => memberTruth (e.eval b sub) (vs.map (Expr.eval b sub))
  | .inQuery e q n =>
    let t := memberTruth (e.eval b sub) ((sub q).map (Row.cell · 0))
    if n then tnot t else t
  | .exists q n => some ((sub q).isEmpty == n)
  | .rowSub vs q isIn n =>
    let values := vs.map (Expr.eval b sub)
    let t := if isIn then rowMemberTruth values (sub q)
      else match (sub q).head? with
        | some row => rowEqTruth values row
        | none => none
    if n then tnot t else t
  | .rowEq ls rs => rowEqTruth (ls.map (Expr.eval b sub)) (rs.map (Expr.eval b sub))
  | .and l r => tand (l.eval b sub) (r.eval b sub)
  | .or l r => tor (l.eval b sub) (r.eval b sub)
  | .not c => tnot (c.eval b sub)

def combine : Combine → List Row → List Row → List Row
  | .unionAll, a, b => a ++ b
  | .union, a, b => (a ++ b).eraseDups
  | .except, a, b => (a.filter fun r => !b.contains r).eraseDups
  | .intersect, a, b => (a.filter fun r => b.contains r).eraseDups

/-- The arms' rows combined left to right. -/
def combineAll (ops : List Combine) : List (List Row) → List Row
  | [] => []
  | first :: rest => (ops.zip rest).foldl (fun acc (op, rows) => combine op acc rows) first

/-- A query's ORDER BY, OFFSET and LIMIT over its rows. -/
def Query.post (q : Query) (rows : List Row) : List Row :=
  let directions := q.orderBy.map (·.2)
  let key (r : Row) := q.orderBy.map fun (p, _) => r.cell p
  let sorted := if q.orderBy.isEmpty then rows else sortBy (fun a b => keyLe directions (key a) (key b)) rows
  let kept := sorted.drop (q.offset.getD 0)
  match q.limit with
  | some n => kept.take n
  | none => kept

/-- The ON conditions of the LEFT JOIN adding item `i`. -/
def Core.onOf (c : Core) (i : Nat) : List Cond :=
  c.conds.filterMap fun (cd, p) => if p == .leftOn i then some cd else none

/-- The conditions of the WHERE and of inner joins' ONs. -/
def Core.innerConds (c : Core) : List Cond :=
  c.conds.filterMap fun (cd, p) => if p == .inner then some cd else none

/-- Join `items` onto each binding, left to right: an item's rows that its
LEFT JOIN's ON holds for, or `none` when no row does. -/
def joinItems (s : Statement) (c : Core) (rowsOf : Nat → List Row)
    (subs : Bind → Subs) : List Nat → List Bind → List Bind
  | [], bs => bs
  | i :: is, bs =>
    joinItems s c rowsOf subs is <| bs.flatMap fun b =>
      let joined := (rowsOf i).filterMap fun r =>
        let b' := b.set i (some r)
        if (c.onOf i).all (fun cd => isTrue (cd.eval b' (subs b'))) then some b' else none
      if (s.item i).left && joined.isEmpty then [b.set i none] else joined

/-- The groups of `bs` by `key`, in order of each key's first binding. -/
def groupsOf (key : Bind → List Value) (bs : List Bind) : List (List Bind) :=
  ((bs.map key).eraseDups).map fun k => bs.filter fun b => key b == k

/-- The largest non-NULL value, or NULL. -/
def maxValue (vs : List Value) : Value :=
  vs.foldl (fun acc v => match acc, v with
    | some x, some y => some (max x y)
    | none, v => v
    | acc, none => acc) none

/-- SQLite's CAST of text to INTEGER: the longest prefix that reads as an
integer, or 0 when none does, held between the least and the greatest 64-bit
integers. -/
def castInteger (text : String) : Int :=
  let (negative, rest) := match text.toList with
    | '-' :: rest => (true, rest)
    | '+' :: rest => (false, rest)
    | rest => (false, rest)
  let digits := rest.takeWhile Char.isDigit
  let n : Int := digits.foldl (fun acc d => acc * 10 + (d.toNat - '0'.toNat)) 0
  max (-2 ^ 63) (min (2 ^ 63 - 1) (if negative then -n else n))

/-- `CAST(group_concat(v, '') AS INTEGER)` over the values `vs` in order: the
decimal text of each value that isn't NULL, joined and read as an integer;
NULL when every value is NULL. -/
def concatValue (vs : List Value) : Value :=
  match vs.filterMap id with
  | [] => none
  | xs => some (castInteger (String.join (xs.map toString)))

/-- A group's bindings in the order a call's ORDER BY sorts them, each term
descending when `true`, keeping the order SQLite reads them in among
bindings that sort the same; in that order alone with no ORDER BY. -/
def sortBindings (subs : Bind → Subs) (orderBy : List (Expr × Dir)) (g : List Bind) :
    List Bind :=
  let key (b : Bind) := orderBy.map fun (e, _) => e.eval b (subs b)
  if orderBy.isEmpty then g else sortBy (fun a b => keyLe (orderBy.map (·.2)) (key a) (key b)) g

/-- The value of a result column of a core that aggregates, over the group
`g` of its bindings. A plain expression takes its value from one binding of
the group, which the order SQLite reads them in decides; the model takes the
last. -/
def OutCol.aggregate (subs : Bind → Subs) (g : List Bind) : OutCol → Value
  | .expr e => match g.getLast? with
    | some b => e.eval b (subs b)
    | none => none
  | .count => some (Int.ofNat g.length)
  | .max e => maxValue (g.map fun b => e.eval b (subs b))
  | .concat e orderBy => concatValue ((sortBindings subs orderBy g).map fun b => e.eval b (subs b))
  | .rowNumber .. => none

/-- The values of `exprs` in the binding `b`. -/
def windowKey (subs : Bind → Subs) (exprs : List Expr) (b : Bind) : List Value :=
  exprs.map fun e => e.eval b (subs b)

/-- Whether `b'` sits in `b`'s partition of the window `PARTITION BY
partition ORDER BY orderBy`, and how its ORDER BY values compare with `b`'s. -/
def windowPeer (subs : Bind → Subs) (partition : List Expr) (orderBy : List (Expr × Dir))
    (b b' : Bind) (o : Ordering) : Bool :=
  windowKey subs partition b' == windowKey subs partition b &&
    keyCompare (orderBy.map (·.2)) (windowKey subs (orderBy.map (·.1)) b')
      (windowKey subs (orderBy.map (·.1)) b) == o

/-- `row_number()` of the binding `b` at position `j` of the bindings `kept`
of a core that doesn't aggregate: one more than the bindings of its
partition that sort before it, and than those that sort the same and come
before it in the order SQLite reads them in. -/
def windowRowNumber (subs : Bind → Subs) (partition : List Expr) (orderBy : List (Expr × Dir))
    (kept : List Bind) (b : Bind) (j : Nat) : Value :=
  some (Int.ofNat (1 + kept.countP (windowPeer subs partition orderBy b · .lt) +
    (kept.take j).countP (windowPeer subs partition orderBy b · .eq)))

/-- The value of a result column of a core that doesn't aggregate, in the
binding `b` at position `j` of the bindings `kept`. -/
def OutCol.plain (subs : Bind → Subs) (kept : List Bind) (b : Bind) (j : Nat) : OutCol → Value
  | .expr e => e.eval b (subs b)
  | .rowNumber partition orderBy => windowRowNumber subs partition orderBy kept b j
  | _ => none

/-- A core's row of a binding, as a core that calls no window function makes
it. -/
def Core.outRow (c : Core) (E : Bind → Nat → List Row) (b : Bind) : Row :=
  c.output.map fun o => match o with
    | .expr e => e.eval b (E b)
    | _ => none

/-- A core's rows from the bindings it keeps. -/
def Core.rows (c : Core) (subs : Bind → Subs) (kept : List Bind) : List Row :=
  if c.aggregated then
    let groups := if c.groupBy.isEmpty then [kept]
      else groupsOf (fun b => c.groupBy.map (Expr.eval b (subs b))) kept
    groups.map fun g => c.output.map (OutCol.aggregate subs g)
  else
    kept.zipIdx.map fun (b, j) => c.output.map (OutCol.plain subs kept b j)

/-- A core's rows as its query combines them: with DISTINCT, the first of
each set of equal rows, in the order of its rows. -/
def Core.result (c : Core) (subs : Bind → Subs) (kept : List Bind) : List Row :=
  if c.distinct then (c.rows subs kept).eraseDups else c.rows subs kept

/-- The bindings core `k` keeps under the binding `outer` around it, with
`eval` giving a query's rows under a binding. A core in no other, as a
statement's own, a view's or a FROM subquery's of such a core, sees no row
around it. -/
def coreBindings (s : Statement) (db : Db) (eval : Bind → Nat → List Row)
    (outer : Bind) (k : Nat) : List Bind :=
  let c := s.core k
  let b := if c.parent.isNone then Bind.empty else outer
  let rowsOf (i : Nat) : List Row :=
    match (s.item i).kind with
    | .table t => db.rows t
    | .derived q _ => eval b q
  let subs (b' : Bind) : Subs := eval b'
  (joinItems s c rowsOf subs c.items [b]).filter fun b' =>
    c.innerConds.all fun cd => isTrue (cd.eval b' (subs b'))

/-- Query `q`'s rows under the binding `b`, with `eval` for its subqueries. -/
def queryRows (s : Statement) (db : Db) (eval : Bind → Nat → List Row)
    (b : Bind) (q : Nat) : List Row :=
  let query := s.query q
  query.post <| combineAll query.ops <| query.arms.map fun k =>
    (s.core k).result eval (coreBindings s db eval b k)

/-- Query `q`'s rows under `b`, with `fuel` for its nesting. -/
def evalQuery (s : Statement) (db : Db) : Nat → Bind → Nat → List Row
  | 0, _, _ => []
  | fuel + 1, b, q => queryRows s db (evalQuery s db fuel) b q

/-- What the statement returns, with fuel for each level of its nesting. -/
def Statement.result (s : Statement) (db : Db) : List Row :=
  evalQuery s db (s.queries.length + 1) Bind.empty s.root

end SqliveNarrowing
