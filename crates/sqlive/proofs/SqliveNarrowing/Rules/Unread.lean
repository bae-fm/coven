import SqliveNarrowing.Congr
import SqliveNarrowing.Sound
import SqliveNarrowing.WellFormed

/-! A column of a subquery, CTE or view that nothing reads changes no row the
statement returns, so sqlive plans the statement without it
(`unread_columns.rs`): the tables of the SELECTs inside it narrow to no row.
`Statement.pruned` writes NULL for each such column, until no more are
unread, as leaving one out can leave another unnamed, and then empties the
cores the statement no longer reads. SQLite reports every column the
statement names, those under an unread column too, but sqlive keeps of them
only the ones a clause it still reads names, so a column only an unread
column names is not read. What no clause names, the statement keeps as it
read it (`Statement.unclaused`). `result_pruned` proves the pruned
statement returns the statement's rows from every database, so a write the
judge of the pruned statement lets pass leaves the statement's rows
unchanged. -/
namespace SqliveNarrowing

namespace Unread

/-- The cores the statement reads: its own query's, and those of each query a
read core reads. -/
def reached (s : Statement) : List Nat := s.queryCores (s.queries.length + 1) s.root

/-- Whether the cores `l` hold the statement's own and every core one of them
reads. -/
def closed (s : Statement) (l : List Nat) : Bool :=
  (s.query s.root).arms.all l.contains &&
    l.all fun k => (s.nestedQueries k).all fun q => (s.query q).arms.all l.contains

/-- Whether the rows of core `k`, an arm of query `q`, are the same without
a column: `q` has no ORDER BY and no compound but UNION ALL, and `k` is
neither DISTINCT nor grouped. -/
def prunable (s : Statement) (q k : Nat) : Bool :=
  (s.query q).orderBy.isEmpty && (s.query q).ops.all (· == .unionAll) &&
    !(s.core k).distinct && (s.core k).groupBy.isEmpty

/-- Whether nothing reads column `p` of core `k`: each query it is an arm of
keeps its rows without it, is neither the statement's own nor a subquery of a
core the statement reads, and no core the statement reads names the column of
an item that reads the query. -/
def unnamed (s : Statement) (k p : Nat) : Bool :=
  (List.range s.queries.length).all fun q => !(s.query q).arms.contains k ||
    (prunable s q k && q != s.root &&
      (reached s).all fun k' => !(s.core k').subqueries.contains q &&
        (s.core k').items.all fun d =>
          match (s.item d).kind with
          | .derived q' _ => q' != q ||
              (reached s).all fun k'' => !(s.core k'').refs.contains (d, p)
          | .table _ => true)

/-- Whether column `p` of core `k` is a column nothing reads: an expression,
which calls no aggregate and no window function, that nothing names. -/
def unread (s : Statement) (k p : Nat) : Bool :=
  closed s (reached s) && unnamed s k p &&
    match (s.core k).output[p]? with
    | some (.expr _) => true
    | _ => false

/-- Column `p` of core `k`, written `o`, as the pruned statement writes it. -/
def prunedColumn (s : Statement) (k p : Nat) (o : OutCol) : OutCol :=
  if unread s k p then OutCol.expr (Expr.lit none) else o

/-- Core `k`, `c`, with NULL written for each of its columns nothing reads. -/
def prunedCore (s : Statement) (k : Nat) (c : Core) : Core :=
  { c with output := c.output.mapIdx (prunedColumn s k) }

/-- `s` with NULL written for each column nothing reads. -/
def pruneOnce (s : Statement) : Statement :=
  { s with cores := s.cores.mapIdx (prunedCore s) }

/-- `s` with each core it doesn't read emptied, and each item no core it
reads lists made one that reads no table. -/
def dropUnreached (s : Statement) : Statement :=
  if closed s (reached s) then
    { s with
      cores := s.cores.mapIdx fun k c =>
        if (reached s).contains k then c
        else { c with items := [], conds := [], output := [], groupBy := [] }
      items := s.items.mapIdx fun i item =>
        if (reached s).any fun k => (s.core k).items.contains i then item
        else { item with kind := .derived s.root false, readable := false } }
  else s

def pruneTimes : Nat → Statement → Statement
  | 0, s => s
  | n + 1, s => pruneTimes n (pruneOnce s)

end Unread

/-- The columns, each of a table, `s` reads whether or not a clause names
them: every column of a table that a query whose rows SQLite picks, a
compound or core whose rows follow the order SQLite reads its rows in, or a
parenthesized join reads, and those `Statement.reported` names. -/
def Statement.unclaused (s : Statement) : List (Nat × Nat) :=
  (((s.itemNumbers.filterMap s.table?).eraseDups.filter fun t =>
      s.pickedTables.contains t || s.groupedTables.contains t).flatMap fun t =>
    (List.range (s.width t)).map (t, ·)) ++ s.reported

/-- The statement sqlive plans: `s` without the columns nothing reads. It
writes NULL for them once for each column the statement has, which is more
often than any of them waits for another. It reads the columns its own
clauses name, and what `s` reads that no clause names. -/
def Statement.pruned (s : Statement) : Statement :=
  { Unread.dropUnreached (Unread.pruneTimes (s.cores.flatMap (·.output)).length s) with
    reported := s.unclaused }

namespace Unread

/-! Lists that hold related elements place by place. -/

def Rel {α : Type} (R : α → α → Prop) (l l' : List α) : Prop :=
  ∃ zs : List (α × α), (∀ z ∈ zs, R z.1 z.2) ∧ l = zs.map Prod.fst ∧ l' = zs.map Prod.snd

theorem Rel.nil {α : Type} {R : α → α → Prop} : Rel R [] [] := ⟨[], by simp, rfl, rfl⟩

theorem Rel.single {α : Type} {R : α → α → Prop} {a b : α} (h : R a b) : Rel R [a] [b] :=
  ⟨[(a, b)], by simpa using h, rfl, rfl⟩

theorem Rel.append {α : Type} {R : α → α → Prop} {a a' b b' : List α} (h : Rel R a a')
    (h' : Rel R b b') : Rel R (a ++ b) (a' ++ b') := by
  obtain ⟨zs, hz, rfl, rfl⟩ := h
  obtain ⟨ws, hw, rfl, rfl⟩ := h'
  refine ⟨zs ++ ws, fun z hm => ?_, by simp, by simp⟩
  rcases List.mem_append.mp hm with hm | hm
  · exact hz z hm
  · exact hw z hm

theorem Rel.refl {α : Type} {R : α → α → Prop} {l : List α} (h : ∀ a ∈ l, R a a) : Rel R l l :=
  ⟨l.map fun a => (a, a), fun z hz => by
    obtain ⟨a, ha, rfl⟩ := List.mem_map.mp hz
    exact h a ha, by simp [Function.comp_def], by simp [Function.comp_def]⟩

theorem Rel.mono {α : Type} {R S : α → α → Prop} {l l' : List α} (h : Rel R l l')
    (hs : ∀ a b, R a b → S a b) : Rel S l l' := by
  obtain ⟨zs, hz, h1, h2⟩ := h
  exact ⟨zs, fun z hm => hs _ _ (hz z hm), h1, h2⟩

theorem Rel.eq {α : Type} {l l' : List α} (h : Rel Eq l l') : l = l' := by
  obtain ⟨zs, hz, rfl, rfl⟩ := h
  exact List.map_congr_left hz

theorem Rel.map {α β : Type} {R : α → α → Prop} {S : β → β → Prop} {l l' : List α}
    {f g : α → β} (h : Rel R l l') (hf : ∀ a b, R a b → S (f a) (g b)) :
    Rel S (l.map f) (l'.map g) := by
  obtain ⟨zs, hz, rfl, rfl⟩ := h
  refine ⟨zs.map fun z => (f z.1, g z.2), fun z hm => ?_, by simp [Function.comp_def],
    by simp [Function.comp_def]⟩
  obtain ⟨w, hw, rfl⟩ := List.mem_map.mp hm
  exact hf _ _ (hz w hw)

theorem Rel.map_eq {α β : Type} {R : α → α → Prop} {l l' : List α} {f g : α → β}
    (h : Rel R l l') (hf : ∀ a b, R a b → f a = g b) : l.map f = l'.map g :=
  (h.map (S := Eq) hf).eq

theorem Rel.length {α : Type} {R : α → α → Prop} {l l' : List α} (h : Rel R l l') :
    l.length = l'.length := by
  obtain ⟨zs, _, rfl, rfl⟩ := h
  simp

theorem Rel.isEmpty {α : Type} {R : α → α → Prop} {l l' : List α} (h : Rel R l l') :
    l.isEmpty = l'.isEmpty := by
  obtain ⟨zs, _, rfl, rfl⟩ := h
  simp

theorem Rel.take {α : Type} {R : α → α → Prop} {l l' : List α} (h : Rel R l l') (n : Nat) :
    Rel R (l.take n) (l'.take n) := by
  obtain ⟨zs, hz, rfl, rfl⟩ := h
  exact ⟨zs.take n, fun z hm => hz z (List.mem_of_mem_take hm), by simp [List.map_take],
    by simp [List.map_take]⟩

theorem Rel.drop {α : Type} {R : α → α → Prop} {l l' : List α} (h : Rel R l l') (n : Nat) :
    Rel R (l.drop n) (l'.drop n) := by
  obtain ⟨zs, hz, rfl, rfl⟩ := h
  exact ⟨zs.drop n, fun z hm => hz z (List.mem_of_mem_drop hm), by simp [List.map_drop],
    by simp [List.map_drop]⟩

theorem Rel.flatMap {α β : Type} {R : α → α → Prop} {S : β → β → Prop} {l l' : List α}
    {f g : α → List β} (h : Rel R l l') (hf : ∀ a b, R a b → Rel S (f a) (g b)) :
    Rel S (l.flatMap f) (l'.flatMap g) := by
  obtain ⟨zs, hz, rfl, rfl⟩ := h
  induction zs with
  | nil => exact .nil
  | cons z zs ih =>
    simp only [List.map_cons, List.flatMap_cons]
    exact (hf _ _ (hz z List.mem_cons_self)).append
      (ih fun w hw => hz w (List.mem_cons_of_mem z hw))

theorem Rel.filter {α : Type} {R : α → α → Prop} {l l' : List α} {f g : α → Bool}
    (h : Rel R l l') (hf : ∀ a b, R a b → f a = g b) : Rel R (l.filter f) (l'.filter g) := by
  obtain ⟨zs, hz, rfl, rfl⟩ := h
  refine ⟨zs.filter fun z => f z.1, fun z hm => hz z (List.mem_filter.mp hm).1, ?_, ?_⟩
  · simp [List.filter_map, Function.comp_def]
  · simp only [List.filter_map, Function.comp_def]
    congr 1
    exact List.filter_congr fun z hm => (hf _ _ (hz z hm)).symm

theorem Rel.filterMap {α β : Type} {R : α → α → Prop} {S : β → β → Prop} {l l' : List α}
    {f g : α → Option β} (h : Rel R l l') (hf : ∀ a b, R a b → Rel S (f a).toList (g b).toList) :
    Rel S (l.filterMap f) (l'.filterMap g) := by
  have e : ∀ (h : α → Option β) (l : List α), l.filterMap h = l.flatMap fun a => (h a).toList := by
    intro h l
    induction l with
    | nil => rfl
    | cons a l ih => cases ha : h a <;> simp [ha, ih]
  rw [e f, e g]
  exact h.flatMap hf

theorem Rel.countP {α : Type} {R : α → α → Prop} {l l' : List α} {f g : α → Bool}
    (h : Rel R l l') (hf : ∀ a b, R a b → f a = g b) : l.countP f = l'.countP g := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  exact (h.filter hf).length

theorem insertSorted_map {α β : Type} (f : α → β) (le : β → β → Bool) (x : α) :
    ∀ l : List α, insertSorted le (f x) (l.map f) =
      (insertSorted (fun a b => le (f a) (f b)) x l).map f
  | [] => rfl
  | y :: ys => by
    simp only [List.map_cons, insertSorted]
    split <;> simp [insertSorted_map f le x ys]

theorem sortBy_map {α β : Type} (f : α → β) (le : β → β → Bool) :
    ∀ l : List α, sortBy le (l.map f) = (sortBy (fun a b => le (f a) (f b)) l).map f
  | [] => rfl
  | x :: xs => by
    simp only [List.map_cons, sortBy]
    rw [sortBy_map f le xs, insertSorted_map]

theorem Rel.sortBy {α : Type} {R : α → α → Prop} {l l' : List α} {le le' : α → α → Bool}
    (h : Rel R l l') (hle : ∀ a a' b b', R a a' → R b b' → le a b = le' a' b') :
    Rel R (sortBy le l) (sortBy le' l') := by
  obtain ⟨zs, hz, rfl, rfl⟩ := h
  rw [sortBy_map, sortBy_map]
  have e : SqliveNarrowing.sortBy (fun a b : α × α => le a.1 b.1) zs =
      SqliveNarrowing.sortBy (fun a b : α × α => le' a.2 b.2) zs :=
    sortBy_congr fun a ha b hb => hle _ _ _ _ (hz a ha) (hz b hb)
  exact ⟨_, fun z hm => hz z (mem_sortBy.mp hm), rfl, by rw [e]⟩

theorem Rel.getLast? {α : Type} {R : α → α → Prop} {l l' : List α} (h : Rel R l l') :
    (l.getLast? = none ∧ l'.getLast? = none) ∨
      ∃ a b, l.getLast? = some a ∧ l'.getLast? = some b ∧ R a b := by
  obtain ⟨zs, hz, rfl, rfl⟩ := h
  rw [List.getLast?_map, List.getLast?_map]
  cases hl : zs.getLast? with
  | none => exact Or.inl ⟨rfl, rfl⟩
  | some z => exact Or.inr ⟨_, _, rfl, rfl, hz z (List.mem_of_getLast? hl)⟩

theorem Rel.zipIdx {α : Type} {R : α → α → Prop} {l l' : List α} (h : Rel R l l') :
    Rel (fun a b : α × Nat => R a.1 b.1 ∧ a.2 = b.2) l.zipIdx l'.zipIdx := by
  obtain ⟨zs, hz, rfl, rfl⟩ := h
  refine ⟨zs.zipIdx.map fun w => ((w.1.1, w.2), (w.1.2, w.2)), fun z hm => ?_, ?_, ?_⟩
  · obtain ⟨w, hw, rfl⟩ := List.mem_map.mp hm
    exact ⟨hz _ (List.fst_mem_of_mem_zipIdx hw), rfl⟩
  · simp [List.zipIdx_map, Function.comp_def, Prod.map]
  · simp [List.zipIdx_map, Function.comp_def, Prod.map]

/-! The relation of a statement and one that writes NULL for some columns. -/

/-- What `t` keeps of `s`: its queries, and in each core of `l`, which holds
every core `s` reads of those it holds, everything but the columns `dead`,
for which `t` writes NULL. Nothing in `l` reads a dead column. -/
structure Prunes (s t : Statement) (l : List Nat) (dead : Nat → Nat → Prop) : Prop where
  queries : t.queries = s.queries
  root : t.root = s.root
  rootArms : ∀ k ∈ (s.query s.root).arms, k ∈ l
  closed : ∀ k ∈ l, ∀ q ∈ s.nestedQueries k, ∀ a ∈ (s.query q).arms, a ∈ l
  items : ∀ k ∈ l, (t.core k).items = (s.core k).items
  conds : ∀ k ∈ l, (t.core k).conds = (s.core k).conds
  groupBy : ∀ k ∈ l, (t.core k).groupBy = (s.core k).groupBy
  distinct : ∀ k ∈ l, (t.core k).distinct = (s.core k).distinct
  parent : ∀ k ∈ l, (t.core k).parent = (s.core k).parent
  item : ∀ k ∈ l, ∀ i ∈ (s.core k).items, t.item i = s.item i
  width : ∀ k ∈ l, (t.core k).output.length = (s.core k).output.length
  output : ∀ k ∈ l, ∀ p, (t.core k).output[p]? = (s.core k).output[p]? ∨
    (dead k p ∧ (t.core k).output[p]? = some (OutCol.expr (Expr.lit none)) ∧
      ∃ e, (s.core k).output[p]? = some (.expr e))
  /-- A query with a dead column keeps its rows without it. -/
  ordered : ∀ q, ∀ k ∈ (s.query q).arms, ∀ p, k ∈ l → dead k p →
    (s.query q).orderBy = [] ∧ ∀ op ∈ (s.query q).ops, op = .unionAll
  plain : ∀ k ∈ l, ∀ p, dead k p → (s.core k).distinct = false
  /-- No subquery reads a dead column, nor does the statement itself. -/
  rootRead : ∀ k ∈ (s.query s.root).arms, ∀ p, ¬dead k p
  subRead : ∀ k' ∈ l, ∀ q ∈ (s.core k').subqueries, ∀ k ∈ (s.query q).arms, ∀ p, ¬dead k p
  /-- No core names a dead column of an item. -/
  named : ∀ k' ∈ l, ∀ d ∈ (s.core k').items, ∀ q v, (s.item d).kind = .derived q v →
    ∀ k ∈ (s.query q).arms, ∀ p, dead k p → ∀ k'' ∈ l, (d, p) ∉ (t.core k'').refs

/-- Query `q` has a dead column at `p`. -/
def QueryDead (s : Statement) (l : List Nat) (dead : Nat → Nat → Prop) (q p : Nat) : Prop :=
  ∃ k ∈ (s.query q).arms, k ∈ l ∧ dead k p

/-- Two rows that differ only at the places `d`. -/
def RowsBut (d : Nat → Prop) (r r' : Row) : Prop :=
  r.length = r'.length ∧ ∀ p, ¬d p → r[p]? = r'[p]?

theorem RowsBut.eq {d : Nat → Prop} {r r' : Row} (h : RowsBut d r r') (hd : ∀ p, ¬d p) : r = r' :=
  List.ext_getElem? fun p => h.2 p (hd p)

/-- Two bindings that bind the same items, and hold the same value in each
column a core of `l` names. -/
def Binds (t : Statement) (l : List Nat) (b b' : Bind) : Prop :=
  ∀ i, (b i).isSome = (b' i).isSome ∧
    ∀ c, (∃ k ∈ l, (i, c) ∈ (t.core k).refs) → cellOf (b i) c = cellOf (b' i) c

theorem Binds.empty (t : Statement) (l : List Nat) : Binds t l Bind.empty Bind.empty :=
  fun _ => ⟨rfl, fun _ _ => rfl⟩

theorem cond_refs_core {c : Core} {cd : Cond} {pl : Place} (h : (cd, pl) ∈ c.conds)
    {x : Nat × Nat} (hx : x ∈ cd.refs) : x ∈ c.refs := by
  unfold Core.refs
  exact List.mem_append_left _ (List.mem_append_left _ (List.mem_flatMap.mpr ⟨(cd, pl), h, hx⟩))

theorem out_refs_core {c : Core} {o : OutCol} (h : o ∈ c.output) {e : Expr} (he : e ∈ o.exprs)
    {x : Nat × Nat} (hx : x ∈ e.refs) : x ∈ c.refs := by
  unfold Core.refs
  exact List.mem_append_left _ (List.mem_append_right _
    (List.mem_flatMap.mpr ⟨o, h, List.mem_flatMap.mpr ⟨e, he, hx⟩⟩))

theorem group_refs_core {c : Core} {e : Expr} (he : e ∈ c.groupBy)
    {x : Nat × Nat} (hx : x ∈ e.refs) : x ∈ c.refs := by
  unfold Core.refs
  exact List.mem_append_right _ (List.mem_flatMap.mpr ⟨e, he, hx⟩)

theorem out_sub_core {c : Core} {o : OutCol} (h : o ∈ c.output) {e : Expr} (he : e ∈ o.exprs)
    {q : Nat} (hq : q ∈ e.queries) : q ∈ c.subqueries :=
  List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_append_left _
    (List.mem_flatMap.mpr ⟨o, h, List.mem_flatMap.mpr ⟨e, he, hq⟩⟩)))

theorem cond_sub_core {c : Core} {cd : Cond} {pl : Place} (h : (cd, pl) ∈ c.conds)
    {q : Nat} (hq : q ∈ cd.queries) : q ∈ c.subqueries :=
  List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_append_right _
    (List.mem_flatMap.mpr ⟨(cd, pl), h, hq⟩)))

theorem group_sub_core {c : Core} {e : Expr} (he : e ∈ c.groupBy)
    {q : Nat} (hq : q ∈ e.queries) : q ∈ c.subqueries :=
  List.mem_eraseDups.mpr (List.mem_append_right _ (List.mem_flatMap.mpr ⟨e, he, hq⟩))

/-- The pruned statement's subqueries are the statement's. -/
theorem Prunes.sub_of {s t : Statement} {l : List Nat} {dead : Nat → Nat → Prop}
    (h : Prunes s t l dead) {k : Nat} (hk : k ∈ l) {q : Nat} (hq : q ∈ (t.core k).subqueries) :
    q ∈ (s.core k).subqueries := by
  unfold Core.subqueries at hq
  rcases List.mem_append.mp (List.mem_eraseDups.mp hq) with hq | hq
  · rcases List.mem_append.mp hq with hq | hq
    · obtain ⟨o, ho, hq⟩ := List.mem_flatMap.mp hq
      obtain ⟨e, he, hq⟩ := List.mem_flatMap.mp hq
      obtain ⟨p, hp⟩ := List.mem_iff_getElem?.mp ho
      rcases h.output k hk p with hsame | ⟨_, hnull, _⟩
      · rw [hp] at hsame
        exact out_sub_core (List.mem_of_getElem? hsame.symm) he hq
      · rw [hp] at hnull
        obtain rfl := Option.some.inj hnull
        simp only [OutCol.exprs, List.mem_singleton] at he
        subst he
        simp [Expr.queries] at hq
    · rw [h.conds k hk] at hq
      obtain ⟨⟨cd, pl⟩, hc, hq⟩ := List.mem_flatMap.mp hq
      exact cond_sub_core hc hq
  · rw [h.groupBy k hk] at hq
    obtain ⟨e, he, hq⟩ := List.mem_flatMap.mp hq
    exact group_sub_core he hq

/-- An expression of core `k` has one value under related bindings. -/
theorem expr_binds {t : Statement} {l : List Nat} {k : Nat} (hk : k ∈ l) {b b' : Bind}
    (hb : Binds t l b b') {E E' : Subs} (e : Expr) (hrefs : ∀ x ∈ e.refs, x ∈ (t.core k).refs)
    (hsub : ∀ q ∈ e.queries, E q = E' q) : e.eval b E = e.eval b' E' :=
  e.eval_congr (fun i c hx => (hb i).2 c ⟨k, hk, hrefs _ hx⟩) hsub

/-- A core's rows from related bindings differ only in its dead columns. -/
theorem rows_pruned {s t : Statement} {l : List Nat} {dead : Nat → Nat → Prop}
    (h : Prunes s t l dead) {k : Nat} (hk : k ∈ l) {E E' : Bind → Nat → List Row}
    (hsub : ∀ b b', Binds t l b b' → ∀ q ∈ (t.core k).subqueries, E b q = E' b' q)
    {kept kept' : List Bind} (hkept : Rel (Binds t l) kept kept') :
    Rel (RowsBut (dead k)) ((s.core k).rows E kept) ((t.core k).rows E' kept') := by
  have hkinds : (t.core k).aggregated = (s.core k).aggregated := by
    have hany : ((t.core k).output.map fun o => match o with
          | .count | .max _ | .concat .. => true
          | .expr _ | .rowNumber .. => false) =
        (s.core k).output.map fun o => match o with
          | .count | .max _ | .concat .. => true
          | .expr _ | .rowNumber .. => false := by
      apply List.ext_getElem?
      intro p
      rw [List.getElem?_map, List.getElem?_map]
      rcases h.output k hk p with hsame | ⟨_, hnull, e, he⟩
      · rw [hsame]
      · rw [hnull, he]
        rfl
    have := congrArg (fun x : List Bool => x.any id) hany
    simp only [List.any_map, Function.id_comp] at this
    unfold Core.aggregated
    rw [h.groupBy k hk]
    exact congrArg _ this
  -- A column that isn't dead has one value from related bindings.
  have houtput : ∀ (f g : OutCol → Value),
      (∀ o ∈ (s.core k).output, o ∈ (t.core k).output → f o = g o) →
      RowsBut (dead k) ((s.core k).output.map f) ((t.core k).output.map g) := by
    intro f g hfg
    refine ⟨by simp [h.width k hk], fun p hp => ?_⟩
    rw [List.getElem?_map, List.getElem?_map]
    rcases h.output k hk p with hsame | ⟨hdead, _⟩
    · rw [hsame]
      cases ho : (s.core k).output[p]? with
      | none => rfl
      | some o =>
        exact congrArg some (hfg o (List.mem_of_getElem? ho)
          (List.mem_of_getElem? (hsame.trans ho)))
    · exact absurd hdead hp
  have hexpr : ∀ b b', Binds t l b b' → ∀ o ∈ (t.core k).output, ∀ e ∈ o.exprs,
      e.eval b (E b) = e.eval b' (E' b') := fun b b' hb o ho e he =>
    expr_binds hk hb e (fun _ hx => out_refs_core ho he hx)
      (fun q hq => hsub b b' hb q (out_sub_core ho he hq))
  have hgroup : ∀ b b', Binds t l b b' →
      (s.core k).groupBy.map (Expr.eval b (E b)) =
        (t.core k).groupBy.map (Expr.eval b' (E' b')) := by
    intro b b' hb
    rw [h.groupBy k hk]
    apply List.map_congr_left
    intro e he
    have het : e ∈ (t.core k).groupBy := by rw [h.groupBy k hk]; exact he
    exact expr_binds hk hb e (fun _ hx => group_refs_core het hx)
      (fun q hq => hsub b b' hb q (group_sub_core het hq))
  have hagg : ∀ g g', Rel (Binds t l) g g' →
      RowsBut (dead k) ((s.core k).output.map (OutCol.aggregate E g))
        ((t.core k).output.map (OutCol.aggregate E' g')) := by
    intro g g' hg
    refine houtput _ _ fun o _ ho => ?_
    cases o with
    | expr e =>
      simp only [OutCol.aggregate]
      rcases hg.getLast? with ⟨h1, h2⟩ | ⟨a, b, h1, h2, hab⟩
      · rw [h1, h2]
      · rw [h1, h2]
        exact hexpr a b hab _ ho e (by simp [OutCol.exprs])
    | count => simp only [OutCol.aggregate, hg.length]
    | max e =>
      simp only [OutCol.aggregate]
      exact congrArg maxValue (hg.map_eq fun a b hab => hexpr a b hab _ ho e (by simp [OutCol.exprs]))
    | concat e orderBy =>
      have hsort : Rel (Binds t l) (sortBindings E orderBy g) (sortBindings E' orderBy g') := by
        unfold sortBindings
        split
        · exact hg
        · refine hg.sortBy fun a a' b b' ha hb => ?_
          have hkey : ∀ x y, Binds t l x y → (orderBy.map fun (e, _) => e.eval x (E x)) =
              orderBy.map fun (e, _) => e.eval y (E' y) := fun x y hxy =>
            List.map_congr_left fun (e', d) hed => hexpr x y hxy _ ho e' (by
              simp only [OutCol.exprs, List.mem_cons, List.mem_map]
              exact Or.inr ⟨(e', d), hed, rfl⟩)
          simp only [hkey a a' ha, hkey b b' hb]
      simp only [OutCol.aggregate]
      exact congrArg concatValue
        (hsort.map_eq fun a b hab => hexpr a b hab _ ho e (by simp [OutCol.exprs]))
    | rowNumber => rfl
  unfold Core.rows
  rw [hkinds]
  split
  · rw [h.groupBy k hk]
    split
    · exact .single (hagg _ _ hkept)
    · -- The groups of related bindings by their keys, which agree.
      have hkeys : (kept.map fun b => (s.core k).groupBy.map (Expr.eval b (E b))) =
          kept'.map fun b => (s.core k).groupBy.map (Expr.eval b (E' b)) :=
        hkept.map_eq fun a b hab => by
          rw [hgroup a b hab, h.groupBy k hk]
      unfold groupsOf
      rw [hkeys, List.map_map, List.map_map]
      refine (Rel.refl (R := Eq) fun _ _ => rfl).map fun key key' hkey => ?_
      subst hkey
      refine hagg _ _ (hkept.filter fun a b hab => ?_)
      have e := hgroup a b hab
      simp only [h.groupBy k hk] at e ⊢
      rw [e]
  · refine hkept.zipIdx.map fun ⟨a, j⟩ ⟨b, j'⟩ ⟨hab, hj⟩ => ?_
    simp only at hab hj
    subst hj
    refine houtput _ _ fun o _ ho => ?_
    cases o with
    | expr e => exact hexpr a b hab _ ho e (by simp [OutCol.exprs])
    | rowNumber partition orderBy =>
      have hkey : ∀ exprs : List Expr, (∀ e ∈ exprs, e ∈ partition ++ orderBy.map (·.1)) →
          ∀ x y, Binds t l x y → windowKey E exprs x = windowKey E' exprs y :=
        fun exprs hin x y hxy => List.map_congr_left fun e he =>
          hexpr x y hxy _ ho e (by simpa [OutCol.exprs] using hin e he)
      have hpeer : ∀ x y, Binds t l x y → ∀ o', windowPeer E partition orderBy a x o' =
          windowPeer E' partition orderBy b y o' := by
        intro x y hxy o'
        simp only [windowPeer, hkey partition (fun e he => List.mem_append_left _ he) x y hxy,
          hkey partition (fun e he => List.mem_append_left _ he) a b hab,
          hkey (orderBy.map (·.1)) (fun e he => List.mem_append_right _ he) x y hxy,
          hkey (orderBy.map (·.1)) (fun e he => List.mem_append_right _ he) a b hab]
      have h1 : kept.countP (windowPeer E partition orderBy a · .lt) =
          kept'.countP (windowPeer E' partition orderBy b · .lt) :=
        hkept.countP fun x y hxy => hpeer x y hxy _
      have h2 : (kept.take j).countP (windowPeer E partition orderBy a · .eq) =
          (kept'.take j).countP (windowPeer E' partition orderBy b · .eq) :=
        (hkept.take j).countP fun x y hxy => hpeer x y hxy _
      simp only [OutCol.plain, windowRowNumber, h1, h2]
    | _ => rfl

/-- Joining a core's items onto related bindings gives related bindings, when
each item's rows are related and differ only where no core names them. -/
theorem join_pruned {s t : Statement} {l : List Nat} {c c' : Core} (hconds : c'.conds = c.conds)
    {rowsOf rowsOf' : Nat → List Row} {subs subs' : Bind → Subs} :
    ∀ (is : List Nat),
      (∀ i ∈ is, t.item i = s.item i) →
      (∀ i ∈ is, ∃ d : Nat → Prop, Rel (RowsBut d) (rowsOf i) (rowsOf' i) ∧
        ∀ p, d p → ∀ k ∈ l, (i, p) ∉ (t.core k).refs) →
      (∀ b b', Binds t l b b' → ∀ i, (c.onOf i).all (fun cd => isTrue (cd.eval b (subs b))) =
        (c.onOf i).all (fun cd => isTrue (cd.eval b' (subs' b')))) →
      ∀ bs bs', Rel (Binds t l) bs bs' →
        Rel (Binds t l) (joinItems s c rowsOf subs is bs) (joinItems t c' rowsOf' subs' is bs')
  | [], _, _, _, _, _, h => h
  | i :: is, hitem, hrows, hon, bs, bs', hbs => by
    have hsame : c'.onOf i = c.onOf i := by unfold Core.onOf; rw [hconds]
    simp only [joinItems]
    refine join_pruned hconds is (fun j hj => hitem j (List.mem_cons_of_mem i hj))
      (fun j hj => hrows j (List.mem_cons_of_mem i hj)) hon _ _ (hbs.flatMap fun b b' hb => ?_)
    obtain ⟨d, hr, hd⟩ := hrows i List.mem_cons_self
    have hset : ∀ r r', RowsBut d r r' → Binds t l (b.set i (some r)) (b'.set i (some r')) := by
      intro r r' hrr j
      simp only [Bind.set]
      split
      · rename_i hji
        subst hji
        refine ⟨rfl, fun col ⟨k, hk, href⟩ => ?_⟩
        have : r[col]? = r'[col]? := hrr.2 col fun hc => hd col hc k hk href
        simp only [cellOf, Row.cell, this]
      · exact hb j
    have hjoined : Rel (Binds t l)
        ((rowsOf i).filterMap fun r =>
          if (c.onOf i).all (fun cd => isTrue (cd.eval (b.set i (some r)) (subs (b.set i (some r)))))
          then some (b.set i (some r)) else none)
        ((rowsOf' i).filterMap fun r =>
          if (c'.onOf i).all
              (fun cd => isTrue (cd.eval (b'.set i (some r)) (subs' (b'.set i (some r)))))
          then some (b'.set i (some r)) else none) := by
      refine hr.filterMap fun r r' hrr => ?_
      rw [hsame, hon _ _ (hset r r' hrr) i]
      split
      · exact .single (hset r r' hrr)
      · exact .nil
    rw [hitem i List.mem_cons_self, hjoined.isEmpty]
    split
    · refine .single fun j => ?_
      simp only [Bind.set]
      split
      · exact ⟨rfl, fun _ _ => rfl⟩
      · exact hb j
    · exact hjoined

/-- The theorem for each query: the pruned statement's rows differ from the
statement's only in the query's dead columns, under related bindings. -/
theorem query_pruned {s t : Statement} {l : List Nat} {dead : Nat → Nat → Prop}
    (h : Prunes s t l dead) (db : Db) :
    ∀ (f q : Nat), (∀ k ∈ (s.query q).arms, k ∈ l) → ∀ b b', Binds t l b b' →
      Rel (RowsBut (QueryDead s l dead q)) (evalQuery s db f b q) (evalQuery t db f b' q)
  | 0, _, _, _, _, _ => .nil
  | f + 1, q, harms, b, b', hb => by
    have ih := query_pruned h db f
    have hquery : t.query q = s.query q := by simp [Statement.query, h.queries]
    -- Each arm's rows differ only in its own dead columns.
    have harm : ∀ k ∈ (s.query q).arms,
        Rel (RowsBut (dead k))
          ((s.core k).result (evalQuery s db f) (coreBindings s db (evalQuery s db f) b k))
          ((t.core k).result (evalQuery t db f) (coreBindings t db (evalQuery t db f) b' k)) := by
      intro k hk
      have hkl := harms k hk
      have hsubq : ∀ x x', Binds t l x x' → ∀ q' ∈ (t.core k).subqueries,
          evalQuery s db f x q' = evalQuery t db f x' q' := by
        intro x x' hx q' hq'
        have hsq := h.sub_of hkl hq'
        have harms' : ∀ a ∈ (s.query q').arms, a ∈ l :=
          h.closed k hkl q' (List.mem_append_right _ hsq)
        refine ((ih q' harms' x x' hx).mono fun r r' hr => ?_).eq
        exact hr.eq fun p ⟨a, ha, _, hd⟩ => h.subRead k hkl q' hsq a ha p hd
      have hkept : Rel (Binds t l) (coreBindings s db (evalQuery s db f) b k)
          (coreBindings t db (evalQuery t db f) b' k) := by
        unfold coreBindings
        simp only [h.items k hkl, h.parent k hkl]
        have hstart : Binds t l (if (s.core k).parent.isNone then Bind.empty else b)
            (if (s.core k).parent.isNone then Bind.empty else b') := by
          split
          · exact Binds.empty t l
          · exact hb
        have hcond : ∀ x x', Binds t l x x' → ∀ (cd : Cond) (pl : Place),
            (cd, pl) ∈ (s.core k).conds →
            cd.eval x (evalQuery s db f x) = cd.eval x' (evalQuery t db f x') := by
          intro x x' hx cd pl hcd
          have hcdt : (cd, pl) ∈ (t.core k).conds := by rw [h.conds k hkl]; exact hcd
          exact cd.eval_congr (fun i c hr => (hx i).2 c ⟨k, hkl, cond_refs_core hcdt hr⟩)
            (fun q' hq' => hsubq x x' hx q' (cond_sub_core hcdt hq'))
        have hall : ∀ x x', Binds t l x x' → ∀ cds : List Cond,
            (∀ cd ∈ cds, ∃ pl, (cd, pl) ∈ (s.core k).conds) →
            (cds.all fun cd => isTrue (cd.eval x (evalQuery s db f x))) =
              cds.all fun cd => isTrue (cd.eval x' (evalQuery t db f x')) := by
          intro x x' hx cds hcds
          apply all_congr_mem
          intro cd hcd
          obtain ⟨pl, hpl⟩ := hcds cd hcd
          rw [hcond x x' hx cd pl hpl]
        have hmem : ∀ (c : Core) (cd : Cond) (want : Place),
            cd ∈ c.conds.filterMap (fun (cd, p) => if p == want then some cd else none) →
            ∃ pl, (cd, pl) ∈ c.conds := by
          intro c cd want hcd
          obtain ⟨⟨cd', pl⟩, hm, hif⟩ := List.mem_filterMap.mp hcd
          simp only at hif
          split at hif
          · obtain rfl := Option.some.inj hif
            exact ⟨pl, hm⟩
          · cases hif
        have hinner : (t.core k).innerConds = (s.core k).innerConds := by
          unfold Core.innerConds
          rw [h.conds k hkl]
        rw [hinner]
        refine Rel.filter ?_ fun x x' hx =>
          hall x x' hx _ fun cd hcd => hmem _ cd .inner hcd
        refine join_pruned (h.conds k hkl) _ (fun i hi => h.item k hkl i hi) (fun i hi => ?_)
          (fun x x' hx i => hall x x' hx _ fun cd hcd => hmem _ cd (.leftOn i) hcd)
          _ _ (.single hstart)
        rw [h.item k hkl i hi]
        cases hkind : (s.item i).kind with
        | table table =>
          exact ⟨fun _ => False, Rel.refl fun _ _ => ⟨rfl, fun _ _ => rfl⟩, fun _ hf => hf.elim⟩
        | derived q' v =>
          have hnest : q' ∈ s.nestedQueries k :=
            List.mem_append_left _ (List.mem_filterMap.mpr ⟨i, hi, by simp [hkind]⟩)
          refine ⟨QueryDead s l dead q', ih q' (h.closed k hkl q' hnest) _ _ hstart, ?_⟩
          intro p ⟨a, ha, _, hd⟩ k'' hk''
          exact h.named k hkl i hi q' v hkind a ha p hd k'' hk''
      have hrows := rows_pruned h hkl hsubq hkept
      unfold Core.result
      rw [h.distinct k hkl]
      split
      · rename_i hdistinct
        have hnone : ∀ p, ¬dead k p := fun p hd => by
          rw [h.plain k hkl p hd] at hdistinct
          cases hdistinct
        have e := (hrows.mono (S := Eq) fun r r' hr => hr.eq hnone).eq
        rw [e]
        exact Rel.refl fun _ _ => ⟨rfl, fun _ _ => rfl⟩
      · exact hrows
    have hwiden : ∀ k ∈ (s.query q).arms, ∀ r r' : Row, RowsBut (dead k) r r' →
        RowsBut (QueryDead s l dead q) r r' := fun k hk r r' hr =>
      ⟨hr.1, fun p hp => hr.2 p fun hd => hp ⟨k, hk, harms k hk, hd⟩⟩
    simp only [evalQuery, queryRows, hquery]
    by_cases hany : ∃ p, QueryDead s l dead q p
    · obtain ⟨p, k, hk, hkl, hd⟩ := hany
      obtain ⟨horder, hops⟩ := h.ordered q k hk p hkl hd
      have hcombine : Rel (RowsBut (QueryDead s l dead q))
          (combineAll (s.query q).ops ((s.query q).arms.map fun k =>
            (s.core k).result (evalQuery s db f) (coreBindings s db (evalQuery s db f) b k)))
          (combineAll (s.query q).ops ((s.query q).arms.map fun k =>
            (t.core k).result (evalQuery t db f) (coreBindings t db (evalQuery t db f) b' k))) := by
        have hfold : ∀ (ops : List Combine), (∀ op ∈ ops, op = .unionAll) →
            ∀ (ks : List Nat), (∀ k ∈ ks, k ∈ (s.query q).arms) →
            ∀ acc acc', Rel (RowsBut (QueryDead s l dead q)) acc acc' →
            Rel (RowsBut (QueryDead s l dead q))
              ((ops.zip (ks.map fun k => (s.core k).result (evalQuery s db f)
                  (coreBindings s db (evalQuery s db f) b k))).foldl
                (fun acc (op, rows) => combine op acc rows) acc)
              ((ops.zip (ks.map fun k => (t.core k).result (evalQuery t db f)
                  (coreBindings t db (evalQuery t db f) b' k))).foldl
                (fun acc (op, rows) => combine op acc rows) acc') := by
          intro ops
          induction ops with
          | nil => intro _ ks _ acc acc' hacc; simpa using hacc
          | cons op ops ihops =>
            intro hall ks hks acc acc' hacc
            cases ks with
            | nil => simpa using hacc
            | cons a ks =>
              simp only [List.map_cons, List.zip_cons_cons, List.foldl_cons]
              refine ihops (fun o ho => hall o (List.mem_cons_of_mem op ho)) ks
                (fun k hk => hks k (List.mem_cons_of_mem a hk)) _ _ ?_
              rw [hall op List.mem_cons_self]
              exact hacc.append ((harm a (hks a List.mem_cons_self)).mono
                (hwiden a (hks a List.mem_cons_self)))
        cases harmsq : (s.query q).arms with
        | nil => exact .nil
        | cons a ks =>
          simp only [List.map_cons, combineAll]
          have ha : a ∈ (s.query q).arms := by rw [harmsq]; exact List.mem_cons_self
          exact hfold _ hops ks (fun k hk => by rw [harmsq]; exact List.mem_cons_of_mem a hk) _ _
            ((harm a ha).mono (hwiden a ha))
      unfold Query.post
      simp only [horder, List.isEmpty_nil, ↓reduceIte]
      split
      · exact (hcombine.drop _).take _
      · exact hcombine.drop _
    · have hnone : ∀ k ∈ (s.query q).arms, ∀ p, ¬dead k p := fun k hk p hd =>
        hany ⟨p, k, hk, harms k hk, hd⟩
      have e : ((s.query q).arms.map fun k => (s.core k).result (evalQuery s db f)
            (coreBindings s db (evalQuery s db f) b k)) =
          (s.query q).arms.map fun k => (t.core k).result (evalQuery t db f)
            (coreBindings t db (evalQuery t db f) b' k) :=
        List.map_congr_left fun k hk =>
          ((harm k hk).mono (S := Eq) fun r r' hr => hr.eq (hnone k hk)).eq
      rw [e]
      exact Rel.refl fun _ _ => ⟨rfl, fun _ _ => rfl⟩

/-- A statement and one that prunes it return the same rows. -/
theorem Prunes.result {s t : Statement} {l : List Nat} {dead : Nat → Nat → Prop}
    (h : Prunes s t l dead) (db : Db) : t.result db = s.result db := by
  unfold Statement.result
  rw [h.queries, h.root]
  refine ((query_pruned h db _ s.root h.rootArms _ _ (Binds.empty t l)).mono
    (S := Eq) fun r r' hr => ?_).eq.symm
  exact hr.eq fun p ⟨k, hk, _, hd⟩ => h.rootRead k hk p hd

/-! Each step of `Statement.pruned` prunes. -/

theorem closed_facts {s : Statement} {l : List Nat} (h : closed s l = true) :
    (∀ k ∈ (s.query s.root).arms, k ∈ l) ∧
      ∀ k ∈ l, ∀ q ∈ s.nestedQueries k, ∀ a ∈ (s.query q).arms, a ∈ l := by
  simp only [closed, Bool.and_eq_true, List.all_eq_true, List.contains_iff_mem] at h
  exact h

theorem mapIdx_getD {α : Type} [Inhabited α] (f : Nat → α → α) (l : List α) (i : Nat)
    (hout : l.length ≤ i → f i default = default) :
    (l.mapIdx f).getD i default = f i (l.getD i default) := by
  simp only [List.getD_eq_getElem?_getD, List.getElem?_mapIdx]
  cases h : l[i]? with
  | some a => rfl
  | none =>
    rw [List.getElem?_eq_none_iff] at h
    exact (hout h).symm

theorem pruneOnce_core (s : Statement) (k : Nat) :
    (pruneOnce s).core k = prunedCore s k (s.core k) :=
  mapIdx_getD (prunedCore s) s.cores k fun _ => rfl

/-- Each core a query reads is an arm of some query. -/
theorem mem_queryCores_arm {s : Statement} {k : Nat} :
    ∀ (f q : Nat), k ∈ s.queryCores f q → ∃ q', k ∈ (s.query q').arms
  | 0, _, h => by cases h
  | f + 1, q, h => by
    simp only [Statement.queryCores, List.mem_flatMap, List.mem_cons] at h
    obtain ⟨a, ha, rfl | ⟨q', _, h⟩⟩ := h
    · exact ⟨q, ha⟩
    · exact mem_queryCores_arm f q' h

/-- What an unread column says of each query it sits in. -/
theorem unread_facts {s : Statement} {k p : Nat} (h : unread s k p = true) :
    closed s (reached s) = true ∧ (∃ e, (s.core k).output[p]? = some (.expr e)) ∧
      ∀ q, k ∈ (s.query q).arms → prunable s q k = true ∧ q ≠ s.root ∧
        ∀ k' ∈ reached s, q ∉ (s.core k').subqueries ∧
          ∀ d ∈ (s.core k').items, ∀ v, (s.item d).kind = .derived q v →
            ∀ k'' ∈ reached s, (d, p) ∉ (s.core k'').refs := by
  simp only [unread, Bool.and_eq_true] at h
  obtain ⟨⟨hc, hn⟩, ho⟩ := h
  refine ⟨hc, ?_, fun q hq => ?_⟩
  · split at ho
    · rename_i e he
      exact ⟨e, he⟩
    · cases ho
  have hlt : q < s.queries.length := by
    refine Decidable.byContradiction fun hge => ?_
    have : s.query q = default := by
      simp only [Statement.query, List.getD_eq_getElem?_getD]
      rw [List.getElem?_eq_none (Nat.le_of_not_lt hge)]
      rfl
    rw [this] at hq
    cases hq
  have := List.all_eq_true.mp hn q (List.mem_range.mpr hlt)
  simp only [Bool.or_eq_true, Bool.not_eq_true', Bool.and_eq_true, bne_iff_ne, ne_eq,
    List.all_eq_true] at this
  rcases this with hno | ⟨⟨hpr, hroot⟩, hall⟩
  · rw [List.contains_iff_mem.mpr hq] at hno
    cases hno
  refine ⟨hpr, hroot, fun k' hk' => ⟨?_, fun d hd v hkind k'' hk'' => ?_⟩⟩
  · have := (hall k' hk').1
    simpa using this
  · have := (hall k' hk').2 d hd
    rw [hkind] at this
    simp only [Bool.or_eq_true, bne_iff_ne, ne_eq, not_true_eq_false, false_or,
      List.all_eq_true, Bool.not_eq_true'] at this
    have := this k'' hk''
    simpa using this

theorem result_pruneOnce (s : Statement) (db : Db) : (pruneOnce s).result db = s.result db := by
  by_cases hc : closed s (reached s) = true
  case neg =>
    have hsame : pruneOnce s = s := by
      have hno : ∀ k p, unread s k p = false := fun k p => by
        simp [unread, Bool.eq_false_iff.mpr hc]
      have hcol : ∀ k, prunedColumn s k = fun _ o => o := fun k => by
        funext p o
        simp [prunedColumn, hno]
      have hcore : ∀ k (c : Core), prunedCore s k c = c := fun k c => by
        have : c.output.mapIdx (prunedColumn s k) = c.output := by
          apply List.ext_getElem?
          intro p
          simp [hcol]
        simp [prunedCore, this]
      have : s.cores.mapIdx (prunedCore s) = s.cores := by
        apply List.ext_getElem?
        intro k
        rw [List.getElem?_mapIdx]
        cases s.cores[k]? with
        | none => rfl
        | some c => exact congrArg some (hcore k c)
      unfold pruneOnce
      rw [this]
    rw [hsame]
  have hf := closed_facts hc
  refine Prunes.result (l := reached s) (dead := fun k p => unread s k p = true) ?_ db
  refine
    { queries := rfl, root := rfl, rootArms := hf.1, closed := hf.2
      items := fun k _ => by rw [pruneOnce_core]; rfl
      conds := fun k _ => by rw [pruneOnce_core]; rfl
      groupBy := fun k _ => by rw [pruneOnce_core]; rfl
      distinct := fun k _ => by rw [pruneOnce_core]; rfl
      parent := fun k _ => by rw [pruneOnce_core]; rfl
      item := fun _ _ _ _ => rfl
      width := fun k _ => by rw [pruneOnce_core]; simp [prunedCore]
      output := fun k _ p => ?_
      ordered := fun q k hk p _ hd => ?_
      plain := fun k hk p hd => ?_
      rootRead := fun k hk p hd => ((unread_facts hd).2.2 _ hk).2.1 rfl
      subRead := fun k' hk' q hq k hk p hd => (((unread_facts hd).2.2 q hk).2.2 k' hk').1 hq
      named := fun k' hk' d hd' q v hkind k hk p hd k'' hk'' href => ?_ }
  · rw [pruneOnce_core]
    simp only [prunedCore, List.getElem?_mapIdx]
    unfold prunedColumn
    by_cases hd : unread s k p = true
    · obtain ⟨_, ⟨e, he⟩, _⟩ := unread_facts hd
      exact Or.inr ⟨hd, by simp [he, hd], e, he⟩
    · refine Or.inl ?_
      cases (s.core k).output[p]? <;> simp [hd]
  · have := ((unread_facts hd).2.2 q hk).1
    simp only [prunable, Bool.and_eq_true, List.isEmpty_iff, List.all_eq_true, beq_iff_eq] at this
    exact ⟨this.1.1.1, this.1.1.2⟩
  · obtain ⟨q, hq⟩ := mem_queryCores_arm _ _ (show k ∈ s.queryCores _ _ by assumption)
    have := ((unread_facts hd).2.2 q hq).1
    simp only [prunable, Bool.and_eq_true, Bool.not_eq_true'] at this
    exact this.1.2
  · have hsrc : (d, p) ∈ (s.core k'').refs := by
      rw [pruneOnce_core] at href
      unfold Core.refs prunedCore at href
      unfold Core.refs
      simp only [List.mem_append, List.mem_flatMap] at href ⊢
      rcases href with (href | ⟨o, ho, href⟩) | href
      · exact Or.inl (Or.inl href)
      · obtain ⟨j, hj⟩ := List.mem_iff_getElem?.mp ho
        rw [List.getElem?_mapIdx] at hj
        obtain ⟨o0, ho0, hif⟩ := Option.map_eq_some_iff.mp hj
        unfold prunedColumn at hif
        split at hif
        · subst hif
          simp [OutCol.refs, OutCol.exprs, Expr.refs] at href
        · subst hif
          exact Or.inl (Or.inr ⟨o0, List.mem_of_getElem? ho0, href⟩)
      · exact Or.inr href
    exact (((unread_facts hd).2.2 q hk).2.2 k' hk').2 d hd' v hkind k'' hk'' hsrc

theorem result_dropUnreached (s : Statement) (db : Db) :
    (dropUnreached s).result db = s.result db := by
  unfold dropUnreached
  split
  case isFalse => rfl
  rename_i hc
  have hf := closed_facts hc
  have hcore : ∀ k ∈ reached s, ∀ (t : Statement),
      t.cores = (s.cores.mapIdx fun k c => if (reached s).contains k then c
        else { c with items := [], conds := [], output := [], groupBy := [] }) →
      t.core k = s.core k := by
    intro k hk t ht
    unfold Statement.core
    simp only [ht, List.getD_eq_getElem?_getD, List.getElem?_mapIdx]
    cases s.cores[k]? with
    | none => rfl
    | some c => simp only [Option.map_some, List.contains_iff_mem.mpr hk, ↓reduceIte]
  refine Prunes.result (l := reached s) (dead := fun _ _ => False) ?_ db
  refine
    { queries := rfl, root := rfl, rootArms := hf.1, closed := hf.2
      items := fun k hk => by rw [hcore k hk _ rfl]
      conds := fun k hk => by rw [hcore k hk _ rfl]
      groupBy := fun k hk => by rw [hcore k hk _ rfl]
      distinct := fun k hk => by rw [hcore k hk _ rfl]
      parent := fun k hk => by rw [hcore k hk _ rfl]
      item := fun k hk i hi => ?_
      width := fun k hk => by rw [hcore k hk _ rfl]
      output := fun k hk p => Or.inl (by rw [hcore k hk _ rfl])
      ordered := fun _ _ _ _ _ hd => hd.elim
      plain := fun _ _ _ hd => hd.elim
      rootRead := fun _ _ _ hd => hd.elim
      subRead := fun _ _ _ _ _ _ _ hd => hd.elim
      named := fun _ _ _ _ _ _ _ _ _ _ hd => hd.elim }
  have hlisted : (reached s).any (fun k => (s.core k).items.contains i) = true :=
    List.any_eq_true.mpr ⟨k, hk, List.contains_iff_mem.mpr hi⟩
  simp only [Statement.item, List.getD_eq_getElem?_getD, List.getElem?_mapIdx]
  cases s.items[i]? with
  | none => rfl
  | some item => simp only [Option.map_some, hlisted, ↓reduceIte]

theorem result_pruneTimes (db : Db) : ∀ (n : Nat) (s : Statement),
    (pruneTimes n s).result db = s.result db
  | 0, _ => rfl
  | n + 1, s => by
    rw [pruneTimes, result_pruneTimes db n, result_pruneOnce]

theorem joinItems_items {s t : Statement} (h : t.items = s.items) (c : Core)
    (rowsOf : Nat → List Row) (subs : Bind → Subs) :
    ∀ (is : List Nat) (bs : List Bind),
      joinItems t c rowsOf subs is bs = joinItems s c rowsOf subs is bs
  | [], _ => rfl
  | i :: is, bs => by
    have hi : t.item i = s.item i := by simp [Statement.item, h]
    simp only [joinItems, hi, joinItems_items h c rowsOf subs is]

/-- The columns a statement is said to read change no row it returns. -/
theorem result_reported (s : Statement) (reported : List (Nat × Nat)) (db : Db) :
    ({ s with reported } : Statement).result db = s.result db := by
  have heval : ∀ f, evalQuery { s with reported } db f = evalQuery s db f := by
    intro f
    induction f with
    | zero => rfl
    | succ f ih =>
      funext b q
      have hcore : ∀ k, coreBindings { s with reported } db (evalQuery s db f) b k =
          coreBindings s db (evalQuery s db f) b k := fun k => by
        unfold coreBindings
        simp only [joinItems_items (s := s) (t := { s with reported }) rfl]
        rfl
      simp only [evalQuery, ih, queryRows, hcore]
      rfl
  unfold Statement.result
  rw [heval]

end Unread

/-- The theorem: the statement sqlive plans returns the statement's rows from
every database. -/
theorem result_pruned (s : Statement) (db : Db) : s.pruned.result db = s.result db := by
  unfold Statement.pruned
  rw [Unread.result_reported, Unread.result_dropUnreached, Unread.result_pruneTimes]

/-- Narrowing's soundness for the statement sqlive plans: a write the judge
of the pruned statement lets pass, stopping questions at its step limit or
not, leaves the statement's rows unchanged. -/
theorem pruned_narrowing_sound {s : Statement} (hwf : s.pruned.wellFormed = true) {db db' : Db}
    {changes : List Change} {stopped : Bool} (hw : Writes db changes db')
    (hr : s.pruned.rerunsLimited db changes db' stopped = false) : s.result db' = s.result db := by
  rw [← result_pruned s db, ← result_pruned s db']
  exact narrowing_sound_limited hwf hw hr

end SqliveNarrowing
