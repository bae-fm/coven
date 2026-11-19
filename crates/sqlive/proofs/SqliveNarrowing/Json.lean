import SqliveNarrowing.Judge
import Lean.Data.Json

/-! The runner's input: a statement as a JSON tree that names its sources by
alias, numbered as sqlive's planner reads it (`read_select`, `read_core`,
`read_item`): a core before its items, a derived item after its own cores'
items, and a core's subqueries after its items, in the order sqlive finds
them (result columns, each ON in FROM order, the WHERE, GROUP BY). A join's
USING columns read as the equalities they stand for, in its ON's place. -/
namespace SqliveNarrowing.Json
open Lean

structure Schema where
  /-- Each table's name and its columns' names and affinities. -/
  tables : List (String × List (String × Option Affinity))
  views : List (String × Json)

/-- What resolves a name to an item: its alias and its columns' names. -/
structure Named where
  alias : String
  columns : List (Option String)
  deriving Inhabited

structure Built where
  items : Array Item := #[]
  named : Array Named := #[]
  cores : Array Core := #[]
  /-- Each core's result columns' names. -/
  outNames : Array (List (Option String)) := #[]
  queries : Array Query := #[]
  whole : List WholeShape := []

abbrev M := StateT Built (Except String)

def field (j : Json) (name : String) : Option Json := (j.getObjVal? name).toOption

def list (j : Json) (name : String) : List Json :=
  match field j name with
  | some (.arr a) => a.toList
  | _ => []

def str (j : Json) (name : String) : Except String String := j.getObjValAs? String name

def affinity : Json → Except String (Option Affinity)
  | .str "numeric" => pure (some .numeric)
  | .str "blob" => pure (some .blob)
  | .null => pure none
  | _ => throw "unknown affinity"

def value : Json → Except String Value
  | .null => pure none
  | j => do pure (some (← j.getInt?))

def op : String → Except String Op
  | "=" => pure .eq | "<>" => pure .ne | "<" => pure .lt | "<=" => pure .le
  | ">" => pure .gt | ">=" => pure .ge | "is" => pure .is | "is not" => pure .isNot
  | other => throw s!"unknown operator {other}"

def combineOp : String → Except String Combine
  | "union all" => pure .unionAll | "union" => pure .union
  | "except" => pure .except | "intersect" => pure .intersect
  | other => throw s!"unknown compound operator {other}"

def wholeShape : String → Except String WholeShape
  | "natural" => pure .natural | "right" => pure .right
  | "recursive" => pure .recursive | "in_table" => pure .inTable | "virtual" => pure .virtual
  | other => throw s!"unknown whole shape {other}"

def lift {α : Type} (e : Except String α) : M α := StateT.lift e

/-- The item an alias names in core `k` or a core it sits in, and the
column's position, as SQLite resolves a qualified name. -/
def resolve (k : Nat) (alias column : String) : M (Nat × Nat) := do
  let b ← get
  let rec go : Nat → Nat → Except String (Nat × Nat)
    | 0, _ => throw s!"{alias}.{column} names no item"
    | f + 1, k =>
      let core := b.cores.getD k default
      match core.items.find? fun i => (b.named.getD i default).alias == alias with
      | some i =>
        match (b.named.getD i default).columns.findIdx? (· == some column) with
        | some c => pure (i, c)
        | none => throw s!"{alias} has no column {column}"
      | none =>
        match core.parent with
        | some p => go f p
        | none => throw s!"{alias}.{column} names no item"
  lift (go (b.cores.size + 1) k)

/-- The equality `JOIN` item `i` `USING (column)` stands for, after the items
`before` in its FROM: `l.column = i.column`, with `l` the leftmost of them
that has the column, which SQLite compares with `i`'s column, by `l`'s
collation. -/
def usingCond (before : List Nat) (i : Nat) (column : String) : M Cond := do
  let b ← get
  let position (j : Nat) := (b.named.getD j default).columns.findIdx? (· == some column)
  let some (l, lc) := before.findSome? fun j => (position j).map (j, ·)
    | lift (throw s!"no item before USING has column {column}")
  let some rc := position i | lift (throw s!"a USING item has no column {column}")
  pure (.cmp .eq (.col l lc) (.col i rc))

/-- Whether query `q`, read as the queries from `first` to `q`, names
nothing outside them (`View.closed`). -/
def closedSince (first q : Nat) : M Bool := do
  let b ← get
  let s : Statement :=
    { items := b.items.toList, cores := b.cores.toList, queries := b.queries.toList
      root := q, whole := [] }
  pure (View.closed s ((List.range (q + 1 - first)).map (first + ·)))

/-- The conjuncts of a condition joined by AND (`conjuncts`). -/
partial def conjuncts (j : Json) : List Json :=
  match field j "and" with
  | some (.arr #[l, r]) => conjuncts l ++ conjuncts r
  | _ => [j]

mutual

/-- Read a query's tree sitting in `parent`, where its FROM items can name
`ctes` and those of its own WITH: its arms' cores, then the query. -/
partial def readSelect (schema : Schema) (j : Json) (parent : Option Nat)
    (ctes : CteNames.Visible Json) : M Nat := do
  let mut declared := []
  for cte in list j "with" do
    declared := declared ++ [((← lift (str cte "name")), (← lift (cte.getObjVal? "query")))]
  let ctes := ctes ++ [declared]
  let mut arms := []
  for arm in list j "arms" do
    arms := arms ++ [← readCore schema arm parent ctes]
  let ops ← lift ((list j "ops").mapM fun o => do combineOp (← o.getStr?))
  let orderBy ← lift ((list j "order").mapM fun o => do
    pure ((← o.getObjValAs? Nat "position"), (← o.getObjValAs? Bool "desc")))
  let orderNamed := (field j "order_named").bind (·.getBool?.toOption) |>.getD false
  let limit := (field j "limit").bind (·.getNat?.toOption)
  let offset := (field j "offset").bind (·.getNat?.toOption)
  let b ← get
  set { b with queries := b.queries.push { arms, ops, orderBy, orderNamed, limit, offset } }
  pure b.queries.size

/-- Read one core's tree: its items, then its result columns, conditions,
GROUP BY and DISTINCT, reading each subquery where it appears. -/
partial def readCore (schema : Schema) (j : Json) (parent : Option Nat)
    (ctes : CteNames.Visible Json) : M Nat := do
  let k := (← get).cores.size
  modify fun b => { b with
    cores := b.cores.push { parent, items := [], conds := [], output := [], groupBy := [] }
    outNames := b.outNames.push [] }
  let (items, ons, groups) ← readFrom schema (list j "from") k parent ctes [] false
  modify fun b => { b with cores := b.cores.modify k fun c => { c with items } }
  let mut output := []
  let mut names := []
  for column in list j "select" do
    let (o, name) ← readOut schema column k ctes items
    output := output ++ [o]
    names := names ++ [name]
  let mut conds := []
  for (i, c, left) in ons do
    let c ← match c with
      | .inl c => readCond schema c k ctes
      | .inr c => pure c
    conds := conds ++ [(c, if left then Place.leftOn i else .inner)]
  for c in (list j "where").flatMap conjuncts do
    conds := conds ++ [(← readCond schema c k ctes, Place.inner)]
  let mut groupBy := []
  for e in list j "group" do
    groupBy := groupBy ++ [← readExpr schema e k ctes]
  let distinct := (field j "distinct").bind (·.getBool?.toOption) |>.getD false
  let core : Core := { parent, items, conds, output, groupBy, distinct }
  if core.aggregated && core.windowed then
    lift (throw "the model numbers a window's rows only in a core that doesn't aggregate")
  let b ← get
  let s : Statement :=
    { items := b.items.toList, cores := b.cores.toList, queries := b.queries.toList
      root := 0, whole := [] }
  for (first, rest, left, start, count) in groups do
    let later := (items.dropWhile (· != first)).drop (1 + rest.length)
    let on := ((conds.drop start).take count).map (·.1)
    let flattens := if left then Parenthesized.leftFlattens s core first rest
      else Parenthesized.innerFlattens s core first later on
    if !flattens then
      lift (throw "the model reads a parenthesized join only where its tables joined one after another give SQLite's rows")
  modify fun b => { b with
    cores := b.cores.modify k fun c => { c with conds, output, groupBy, distinct }
    outNames := b.outNames.modify k fun _ => names }
  pure k

/-- Read the FROM entries `entries` of core `k`, after its items `before`, as
items of the core, the tables of a parenthesized join among them
(`read_from`), the first a LEFT JOIN's when `firstLeft`. Gives the items;
each ON condition, as JSON, or USING equality, with its item and whether a
LEFT JOIN adds that item; and each parenthesized join, as its first item, its
other items, whether a LEFT JOIN adds it, and where its ON's conditions start
among the conditions and how many there are. -/
partial def readFrom (schema : Schema) (entries : List Json) (k : Nat) (parent : Option Nat)
    (ctes : CteNames.Visible Json) (before : List Nat) (firstLeft : Bool) :
    M (List Nat × List (Nat × (Json ⊕ Cond) × Bool) × List (Nat × List Nat × Bool × Nat × Nat)) := do
  let mut items := []
  let mut ons : List (Nat × (Json ⊕ Cond) × Bool) := []
  let mut groups : List (Nat × List Nat × Bool × Nat × Nat) := []
  let mut position := 0
  for entry in entries do
    let left := if position == 0 then firstLeft
      else (field entry "left").bind (·.getBool?.toOption) |>.getD false
    position := position + 1
    if let some (.arr group) := field entry "group" then
      if !(list entry "using").isEmpty then
        lift (throw "the model reads no USING join of a parenthesized join")
      if (group[0]?.bind (field · "group")).isSome then
        lift (throw "the model reads no parenthesized join that starts with another")
      let (inner, innerOns, innerGroups) ←
        readFrom schema group.toList k parent ctes (before ++ items) left
      let some first := inner.head? | lift (throw "a parenthesized join with no item")
      groups := groups ++ innerGroups.map fun (f, r, l, start, count) => (f, r, l, ons.length + start, count)
      ons := ons ++ innerOns
      let on := (list entry "on").flatMap conjuncts
      groups := groups ++ [(first, inner.tail, left, ons.length, on.length)]
      ons := ons ++ on.map ((first, .inl ·, left))
      items := items ++ inner
      -- SQLite reads a join in parentheses of two tables or more, after the
      -- first item of its FROM, as a `SELECT *` of its own, which reads every
      -- column of its tables; one of one table, or first, it reads in place.
      if position > 1 && inner.length > 1 then
        modify fun b => { b with
          items := inner.foldl (fun all i => all.modify i fun item => { item with grouped := true }) b.items }
    else
      let i ← readItem schema entry k parent ctes
      if left then
        modify fun b => { b with items := b.items.modify i fun item => { item with left := true } }
      for column in list entry "using" do
        ons := ons ++ [(i, .inr (← usingCond (before ++ items) i (← lift column.getStr?)), left)]
      items := items ++ [i]
      for shape in list entry "whole" do
        let shape ← lift (wholeShape (← lift shape.getStr?))
        modify fun b => { b with whole := b.whole ++ [shape] }
      ons := ons ++ (list entry "on").flatMap fun c => (conjuncts c).map ((i, .inl ·, left))
  pure (items, ons, groups)

/-- Read a FROM item of core `k`, which sits in `parent`. -/
partial def readItem (schema : Schema) (j : Json) (k : Nat) (parent : Option Nat)
    (ctes : CteNames.Visible Json) : M Nat := do
  let alias ← lift (str j "alias")
  let left := (field j "left").bind (·.getBool?.toOption) |>.getD false
  let (kind, columns, names, readable) ← match field j "table", field j "view", field j "cte", field j "query" with
    | some (.str name), _, _, _ =>
      match schema.tables.findIdx? (·.1 == name) with
      | some t =>
        let cols := (schema.tables.getD t default).2
        pure (ItemKind.table t, cols.map (·.2), cols.map (some ·.1), true)
      | none => lift (throw s!"no table {name}")
    | _, some (.str name), _, _ =>
      match schema.views.find? (·.1 == name) with
      | some (_, query) => do
        let q ← readSelect schema query none []
        let (names, columns) ← derivedColumns q
        pure (ItemKind.derived q true, columns, names, View.readable true parent false)
      | none => lift (throw s!"no view {name}")
    | _, _, some (.str name), _ =>
      match CteNames.resolve ctes name with
      | some (query, scope) => do
        let first := (← get).queries.size
        let q ← readSelect schema query parent scope
        let (names, columns) ← derivedColumns q
        pure (ItemKind.derived q false, columns, names, View.readable false parent (← closedSince first q))
      | none => lift (throw s!"no CTE {name}")
    | _, _, _, some query => do
      let first := (← get).queries.size
      let q ← readSelect schema query parent ctes
      let (names, columns) ← derivedColumns q
      pure (ItemKind.derived q false, columns, names, View.readable false parent (← closedSince first q))
    | _, _, _, _ => lift (throw "a FROM item names no source")
  let b ← get
  set { b with
    items := b.items.push { core := k, kind, left, columns, readable }
    named := b.named.push { alias, columns := names } }
  pure b.items.size

/-- A derived item's columns: the names of its first arm's result columns,
a repeated name left unnamed, and each column's affinity when every arm
passes on columns of one affinity (`read_derived`). -/
partial def derivedColumns (q : Nat) : M (List (Option String) × List (Option Affinity)) := do
  let b ← get
  let query := b.queries.getD q default
  let first := query.arms.headD 0
  let names := (b.outNames.getD first []).foldl (fun acc name =>
    acc ++ [name.filter fun n => !acc.contains (some n)]) []
  let affinities := (List.range names.length).map fun position =>
    let found := query.arms.map fun k =>
      match (b.cores.getD k default).output[position]? with
      | some (.expr (.col i c)) =>
        if (b.items.getD i default).core == k then ((b.items.getD i default).columns[c]?).join
        else none
      | _ => none
    match found with
    | some a :: rest => if rest.all (· == some a) then some a else none
    | _ => none
  pure (names, affinities)

partial def readOut (schema : Schema) (j : Json) (k : Nat) (ctes : CteNames.Visible Json)
    (items : List Nat) : M (OutCol × Option String) := do
  let alias := (field j "as").bind (·.getStr?.toOption)
  if let some e := field j "expr" then
    let e ← readExpr schema e k ctes
    let passed : Option String ← match e with
      | .col i c =>
        if items.contains i then
          pure (((← get).named.getD i default).columns[c]?).join
        else pure none
      | _ => pure none
    pure (.expr e, alias.orElse fun _ => passed)
  else if let some e := field j "max" then
    pure (.max (← readExpr schema e k ctes), alias)
  else if let some e := field j "concat" then
    let e ← readExpr schema e k ctes
    let mut orderBy := []
    for term in list j "order" do
      let some sorted := field term "expr" | lift (throw "an ORDER BY term sorts by nothing")
      let desc := (field term "desc").bind (·.getBool?.toOption) |>.getD false
      orderBy := orderBy ++ [(← readExpr schema sorted k ctes, desc)]
    pure (.concat e orderBy, alias)
  else if let some window := field j "row_number" then
    let mut partition := []
    for e in list window "partition" do
      partition := partition ++ [← readExpr schema e k ctes]
    let mut orderBy := []
    for term in list window "order" do
      let some sorted := field term "expr" | lift (throw "an ORDER BY term sorts by nothing")
      let desc := (field term "desc").bind (·.getBool?.toOption) |>.getD false
      orderBy := orderBy ++ [(← readExpr schema sorted k ctes, desc)]
    pure (.rowNumber partition orderBy, alias)
  else if (field j "count").isSome then
    pure (.count, alias)
  else lift (throw "unknown result column")

partial def readExpr (schema : Schema) (j : Json) (k : Nat) (ctes : CteNames.Visible Json) :
    M Expr := do
  if let some (.arr #[.str alias, .str column]) := field j "col" then
    let (i, c) ← resolve k alias column
    pure (.col i c)
  else if let some v := field j "lit" then
    pure (.lit (← lift (value v)))
  else if let some (.arr #[l, r]) := field j "add" then
    let l ← readExpr schema l k ctes
    pure (.add l (← readExpr schema r k ctes))
  else if let some (.arr #[l, r]) := field j "greatest" then
    let l ← readExpr schema l k ctes
    pure (.greatest l (← readExpr schema r k ctes))
  else if let some q := field j "query" then
    pure (.sub (← readSelect schema q (some k) ctes))
  else if let some (.arr #[l, r]) := field j "coalesce" then
    let l ← readExpr schema l k ctes
    pure (.coalesce l (← readExpr schema r k ctes))
  else if let some (.arr #[t, y, n]) := field j "iif" then
    let t ← readExpr schema t k ctes
    let y ← readExpr schema y k ctes
    pure (.iif t y (← readExpr schema n k ctes))
  else if let some e := field j "is_null" then
    pure (.isNull (← readExpr schema e k ctes))
  else lift (throw s!"unknown expression {j.compress}")

partial def readCond (schema : Schema) (j : Json) (k : Nat) (ctes : CteNames.Visible Json) :
    M Cond := do
  let not := (field j "not").bind (·.getBool?.toOption) |>.getD false
  if let some (.arr #[.str o, l, r]) := field j "cmp" then
    let o ← lift (op o)
    let l ← readExpr schema l k ctes
    pure (.cmp o l (← readExpr schema r k ctes))
  else if let some e := field j "null" then
    pure (.null (← readExpr schema e k ctes) not)
  else if let some e := field j "in" then
    let e ← readExpr schema e k ctes
    if let some q := field j "query" then
      pure (.inQuery e (← readSelect schema q (some k) ctes) not)
    else
      let mut values := []
      for v in list j "list" do
        values := values ++ [← readExpr schema v k ctes]
      pure (.inList e values)
  else if let some (.arr values) := field j "row" then
    let mut read := []
    for v in values.toList do
      read := read ++ [← readExpr schema v k ctes]
    let some q := field j "query" | lift (throw "a row value compares with no query")
    let isIn := (field j "member").bind (·.getBool?.toOption) |>.getD false
    pure (.rowSub read (← readSelect schema q (some k) ctes) isIn not)
  else if let some (.arr #[.arr ls, .arr rs]) := field j "rows" then
    let mut left := []
    for v in ls.toList do
      left := left ++ [← readExpr schema v k ctes]
    let mut right := []
    for v in rs.toList do
      right := right ++ [← readExpr schema v k ctes]
    pure (.rowEq left right)
  else if let some q := field j "exists" then
    pure (.exists (← readSelect schema q (some k) ctes) not)
  else if let some (.arr #[l, r]) := field j "and" then
    let l ← readCond schema l k ctes
    pure (.and l (← readCond schema r k ctes))
  else if let some (.arr #[l, r]) := field j "or" then
    let l ← readCond schema l k ctes
    pure (.or l (← readCond schema r k ctes))
  else if let some c := field j "negate" then
    pure (.not (← readCond schema c k ctes))
  else lift (throw s!"unknown condition {j.compress}")

end

def readSchema (j : Json) : Except String Schema := do
  let tables ← (list j "tables").mapM fun t => do
    let columns ← (list t "columns").mapM fun c => do
      pure ((← str c "name"), (← affinity ((c.getObjVal? "affinity").toOption.getD .null)))
    pure ((← str t "name"), columns)
  let views ← (list j "views").mapM fun v => do pure ((← str v "name"), (← v.getObjVal? "query"))
  pure { tables, views }

/-- The statement the tree `j` holds. -/
def readStatement (schema : Schema) (j : Json) : Except String Statement := do
  let (root, b) ← (readSelect schema j none []).run {}
  pure { items := b.items.toList, cores := b.cores.toList, queries := b.queries.toList,
         root, whole := b.whole }

/-- A database: each table's rows, keyed by the rowid in their first column. -/
def readDb (schema : Schema) (j : Json) : Except String Db := do
  let tables ← schema.tables.mapM fun (name, _) => do
    let rows ← match (j.getObjVal? name).toOption with
      | some (.arr rows) => rows.toList.mapM fun row => do
        let values ← (← row.getArr?).toList.mapM value
        match values.head? with
        | some (some id) =>
          if id < 0 then throw "a rowid below zero" else pure (id.toNat, values)
        | _ => throw "a row whose first column is not its rowid"
      | _ => pure []
    let size := (rows.map (·.1 + 1)).foldl max 0
    pure ((List.range size).map fun slot => (rows.find? (·.1 == slot)).map (·.2))
  pure fun t => tables.getD t []

/-- A write's row changes, in order. -/
def readChanges (schema : Schema) (j : Json) : Except String (List Change) :=
  (list j "changes").mapM fun c => do
    let name ← str c "table"
    let some t := schema.tables.findIdx? (·.1 == name) | throw s!"no table {name}"
    let row (key : String) : Except String (Option Row) :=
      match (c.getObjVal? key).toOption with
      | some (.arr values) => do pure (some (← values.toList.mapM value))
      | _ => pure none
    pure { table := t, old := ← row "old", new := ← row "new" }

end SqliveNarrowing.Json
