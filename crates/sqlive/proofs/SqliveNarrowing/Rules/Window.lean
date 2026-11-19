import SqliveNarrowing.Rules.Edge
import SqliveNarrowing.Rules.Limit

/-! Rule 6 for each binding around a subquery: a subquery that is one SELECT
of one table, ordered by expressions over that table's columns, with a LIMIT
and no OFFSET, returns for each binding of the core around it the first rows
of its window: the table's rows that its own conditions pass and that hold,
in each column an equality compares with a row around the subquery, that
row's value (`scope_window.rs`). A row that sorts after the LIMIT-th row of
its window changes nothing the subquery returns, so the table narrows to the
rows that fewer than LIMIT rows of their window sort before. A write asks
SQLite how many rows of the changed row's window sort before it, in the
database the write leaves, as it asks a lookup; the run keeps no value.

The subquery's every condition is one of the two kinds, so the rows its
conditions keep under a binding around it are exactly that binding's window.
Its ORDER BY names each term's column or expression, and its table is the
source of no subquery's value: a comparison that takes the subquery's value
reads its source's rows that its conditions keep, wherever they sort. -/
namespace SqliveNarrowing.Window

/-- A subquery whose rows for each binding around it are the first rows of a
window (`Window`). -/
structure Window where
  /-- The subquery. -/
  query : Nat
  /-- Its one core. -/
  core : Nat
  /-- The table item the core reads. -/
  item : Nat
  /-- The conditions on the item's row alone. -/
  own : List Cond
  /-- The item's columns that an equality compares with a value of a row
  around the subquery. -/
  partition : List Nat
  /-- The ORDER BY's terms, expressions over the item's columns, each
  descending when `true`. -/
  keys : List (Expr × Bool)
  limit : Nat
  deriving Repr

/-- The scope the window gives its item. -/
def Window.scope (w : Window) : RScope := .window w.item w.own w.partition w.keys w.limit

/-- Whether a condition of a subquery's core names only columns of its item
`i`, each one sqlive narrows by, and holds no subquery. -/
def ownCond (s : Statement) (i : Nat) (cd : Cond) : Bool :=
  cd.queries.isEmpty && !cd.refs.isEmpty &&
    cd.refs.all fun (j, c) => j == i && (s.affinity i c).isSome

/-- The column of item `i` that `member = other`, written in core `k` with
`other` first when `first`, compares with a value of an item around the
core, when the edge reader reads it so. -/
def partitionColumn (s : Statement) (k i : Nat) (member other : Expr) (first : Bool) : Option Nat :=
  match member, Edge.edge s k member other first with
  | .col j c, some e => if j = i ∧ e.member = i ∧ (s.item e.partner).core ≠ k then some c else none
  | _, _ => none

/-- The column of item `i` an equality, a condition of core `k`, compares
with a value of an item around the core. -/
def partitionOf (s : Statement) (k i : Nat) : Cond → Option Nat
  | .cmp .eq l r => (partitionColumn s k i l r false).orElse fun _ => partitionColumn s k i r l true
  | _ => none

/-- Whether a planner state takes the value of a subquery from item `i`'s
core: an edge, or a side of an OR of one of the items `items`, whose
partner, as a subquery's source, is an item of that core. -/
def takesValues (s : Statement) (st : PState) (items : List Nat) (i : Nat) : Bool :=
  let nested (e : Edge) : Bool := e.nested && (s.item e.partner).core == (s.item i).core
  st.edges.any nested || items.any fun j =>
    (st.items j).ors.any fun sides => sides.any fun side => side.edges.any nested

/-- The ORDER BY term at result column `pos` of core `c`, sorting item
`i`'s rows, when it is an expression over `i`'s columns, each one sqlive
narrows by. -/
def keyAt (s : Statement) (c : Core) (i pos : Nat) : Option Expr :=
  match c.output[pos]? with
  | some (.expr e) => (Limit.sortKey s i e).map fun _ => e
  | _ => none

/-- The window of core `k`'s one table item `i`, the arm of the subquery
`query`, when the core neither groups, aggregates, calls a window function
nor is DISTINCT, the query keeps its first rows by an ORDER BY over `i`'s
columns that names each term's expression, and each condition of the core is
one of the two kinds. -/
def readCore (s : Statement) (q k i : Nat) (query : Query) : Option Window :=
  let c := s.core k
  match query.limit with
  | none => none
  | some n =>
    if n = 0 ∨ query.offset.isSome ∨ query.orderBy.isEmpty ∨ !query.orderNamed ∨
      (s.item i).left ∨ c.aggregated ∨ c.windowed ∨ c.distinct then none
    else if !(query.orderBy.all fun (pos, _) => (keyAt s c i pos).isSome) then none
    else if !(c.conds.all fun (cd, place) =>
        place == .inner && (ownCond s i cd || (partitionOf s k i cd).isSome)) then none
    else some
      { query := q, core := k, item := i, limit := n
        keys := query.orderBy.filterMap fun (pos, d) => (keyAt s c i pos).map (·, d)
        own := c.conds.filterMap fun (cd, _) => if ownCond s i cd then some cd else none
        partition := c.conds.filterMap fun (cd, _) =>
          if ownCond s i cd then none else partitionOf s k i cd }

/-- The window of query `q`, when it is a subquery of the core around its
one core, whose rows for each binding around it are the first rows of a
window, and whose table no edge of the planned state `st` takes a subquery's
value from (`read_window`). -/
def read (s : Statement) (st : PState) (q : Nat) : Option Window :=
  match (s.query q).arms with
  | [k] =>
    match (s.core k).parent, (s.core k).items with
    | some p, [i] =>
      if (s.core p).subqueries.contains q && (s.table? i).isSome && !takesValues s st s.itemNumbers i
      then readCore s q k i (s.query q) else none
    | _, _ => none
  | _ => none

/-- The windows of a statement's subqueries. -/
def windows (s : Statement) (st : PState) : List Window :=
  (List.range s.queries.length).filterMap (read s st)

/-! What the reader's checks say of the subquery. -/

/-- `a = b` holds in SQL: neither is NULL and they are equal. -/
def eqHolds (a b : Value) : Bool := isTrue (Op.apply .eq a b)

theorem eqHolds_iff {a b : Value} : eqHolds a b = true ↔ ∃ x, a = some x ∧ b = some x := by
  cases a with
  | none => simp [eqHolds, Op.apply, isTrue]
  | some x =>
    cases b with
    | none => simp [eqHolds, Op.apply, isTrue]
    | some y =>
      simp only [eqHolds, Op.apply, Op.compare, isTrue, beq_iff_eq, Option.some.injEq,
        beq_iff_eq]
      constructor
      · intro h; exact ⟨x, rfl, h.symm⟩
      · rintro ⟨z, rfl, rfl⟩; rfl

theorem eqHolds_comm (a b : Value) : eqHolds a b = eqHolds b a := by
  apply Bool.eq_iff_iff.mpr
  rw [eqHolds_iff, eqHolds_iff]
  constructor <;> rintro ⟨x, h1, h2⟩ <;> exact ⟨x, h2, h1⟩

/-- A condition on the item's own columns holds of a binding as of the
item's row alone. -/
theorem ownCond_eval {s : Statement} {i : Nat} {cd : Cond} (h : ownCond s i cd = true) (b : Bind)
    (sub : Subs) : cd.eval b sub = cd.eval (rowBind (b i)) noSubs := by
  simp only [ownCond, Bool.and_eq_true, List.isEmpty_iff, List.all_eq_true, beq_iff_eq] at h
  refine cd.eval_rowBind (fun j hj => ?_) h.1.1
  simp only [Cond.items, List.mem_map] at hj
  obtain ⟨⟨j', c⟩, hm, rfl⟩ := hj
  exact (h.2 _ hm).1

theorem partitionColumn_facts {s : Statement} {k i : Nat} {member other : Expr} {first : Bool}
    {c : Nat} (h : partitionColumn s k i member other first = some c) :
    member = .col i c ∧ other.queries = [] ∧ ∀ j ∈ other.items, j ≠ i := by
  unfold partitionColumn at h
  split at h
  · rename_i j c' e hedge
    split at h
    · rename_i hc
      cases h
      obtain ⟨hj, hm, _⟩ := hc
      obtain ⟨_, _, _, hq, hitems, _, _, hpm, _, _, _⟩ := Edge.edge_facts hedge
      subst hj
      refine ⟨rfl, hq, fun j hj hji => hpm ?_⟩
      rw [← hitems j hj, hji, hm]
    · cases h
  · cases h

/-- An equality the reader reads as a partition column holds of a binding
exactly when the item's row holds in that column the value, not NULL, that
an expression over other items gives. -/
theorem partitionOf_holds {s : Statement} {k i : Nat} {cd : Cond} {c : Nat}
    (h : partitionOf s k i cd = some c) :
    ∃ other : Expr, other.queries = [] ∧ (∀ j ∈ other.items, j ≠ i) ∧ cd.queries = [] ∧
      ∀ b sub, isTrue (cd.eval b sub) = eqHolds (cellOf (b i) c) (other.eval b noSubs) := by
  unfold partitionOf at h
  split at h
  · rename_i l r
    cases h1 : partitionColumn s k i l r false with
    | some c' =>
      rw [h1] at h
      simp only [Option.orElse_some, Option.some.injEq] at h
      subst h
      obtain ⟨rfl, hq, hitems⟩ := partitionColumn_facts h1
      refine ⟨r, hq, hitems, by simp [Cond.queries, Expr.queries, hq], fun b sub => ?_⟩
      simp only [Cond.eval, Expr.eval, eqHolds]
      rw [r.eval_congr (sub' := noSubs) (fun _ _ _ => rfl) (by simp [hq])]
    | none =>
      rw [h1] at h
      simp only [Option.orElse_none] at h
      obtain ⟨rfl, hq, hitems⟩ := partitionColumn_facts h
      refine ⟨l, hq, hitems, by simp [Cond.queries, Expr.queries, hq], fun b sub => ?_⟩
      simp only [Cond.eval, Expr.eval]
      rw [l.eval_congr (sub' := noSubs) (fun _ _ _ => rfl) (by simp [hq]), eqHolds_comm]
      rfl
  · cases h

/-- What `read` checked of a window's subquery. -/
structure ReadFacts (s : Statement) (st : PState) (w : Window) : Prop where
  arms : (s.query w.query).arms = [w.core]
  parent : ∃ p, (s.core w.core).parent = some p ∧ w.query ∈ (s.core p).subqueries
  items : (s.core w.core).items = [w.item]
  table : (s.table? w.item).isSome = true
  takes : takesValues s st s.itemNumbers w.item = false
  limit : (s.query w.query).limit = some w.limit
  positive : 0 < w.limit
  offset : (s.query w.query).offset = none
  order : (s.query w.query).orderBy ≠ []
  left : (s.item w.item).left = false
  agg : (s.core w.core).aggregated = false
  windowed : (s.core w.core).windowed = false
  distinct : (s.core w.core).distinct = false
  keyed : ∀ p ∈ (s.query w.query).orderBy, (keyAt s (s.core w.core) w.item p.1).isSome = true
  keys : w.keys = (s.query w.query).orderBy.filterMap fun (pos, d) =>
    (keyAt s (s.core w.core) w.item pos).map (·, d)
  conds : ∀ x ∈ (s.core w.core).conds, x.2 = .inner ∧
    (ownCond s w.item x.1 = true ∨ (partitionOf s w.core w.item x.1).isSome = true)
  own : w.own = (s.core w.core).conds.filterMap fun (cd, _) =>
    if ownCond s w.item cd then some cd else none
  partition : w.partition = (s.core w.core).conds.filterMap fun (cd, _) =>
    if ownCond s w.item cd then none else partitionOf s w.core w.item cd

theorem read_facts {s : Statement} {st : PState} {q : Nat} {w : Window} (h : read s st q = some w) :
    w.query = q ∧ ReadFacts s st w := by
  unfold read at h
  split at h
  · rename_i k harms
    split at h
    · rename_i p i hparent hitems
      split at h
      · rename_i hcheck
        simp only [Bool.and_eq_true, List.contains_iff_mem, Bool.not_eq_true'] at hcheck
        obtain ⟨⟨hsub, htable⟩, htakes⟩ := hcheck
        unfold readCore at h
        split at h
        · cases h
        · rename_i n hlim
          dsimp only at h
          split at h
          · cases h
          rename_i hok
          simp only [not_or, Bool.not_eq_true, Option.isSome_eq_false_iff, Option.isNone_iff_eq_none,
            List.isEmpty_eq_false_iff] at hok
          split at h
          · cases h
          rename_i hkeys
          split at h
          · cases h
          rename_i hconds
          cases h
          simp only [Bool.not_eq_eq_eq_not, Bool.not_true, Bool.not_eq_false, List.all_eq_true] at hkeys hconds
          refine ⟨rfl, harms, ⟨p, hparent, hsub⟩, hitems, htable, htakes, hlim, ?_, hok.2.1, ?_,
            hok.2.2.2.2.1, hok.2.2.2.2.2.1, hok.2.2.2.2.2.2.1, hok.2.2.2.2.2.2.2, hkeys, rfl, ?_, rfl, rfl⟩
          · exact Nat.pos_of_ne_zero hok.1
          · intro he; exact hok.2.2.1 (by simp [he])
          · intro x hx
            have := hconds x hx
            simp only [Bool.and_eq_true, beq_iff_eq, Bool.or_eq_true] at this
            exact this
      · cases h
    · cases h
  · cases h

end SqliveNarrowing.Window
