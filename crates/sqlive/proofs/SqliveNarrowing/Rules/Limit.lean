import SqliveNarrowing.Reader

/-! Rule 6: a statement that is one SELECT ordered by its result columns,
each a column or an expression over the columns of one of its tables, with a
LIMIT and no OFFSET, depends only on its rows that sort at or before its last
row when a run returns as many rows as its LIMIT (`scope_order.rs`). The
ORDER BY's terms fall into runs, each of consecutive terms over one table.
The table of the first run is bounded by its own terms. A later run's table
is bounded by its own terms too, or by a row it joins of each earlier run's
table that sorts before the last row's values of that run. No table is a
LEFT JOIN's right side, and the SELECT neither groups, aggregates nor calls a
window function, whose value in a row the rows after the last can change. -/
namespace SqliveNarrowing.Limit

/-- What an ORDER BY term sorts its item's rows by (`SortKey`): one of
its columns, or an expression over its columns with no subquery. A write asks
SQLite for its value in each row it changes, and how it compares with the
last row's by the term's collation (`RScope.ordered`). -/
inductive SortKey where
  | column (c : Nat)
  | expr (e : Expr)
  deriving Repr

/-- The key's value in its item's row `r`. -/
def SortKey.value (r : Row) : SortKey → Value
  | .column c => r.cell c
  | .expr e => e.eval (rowBind (some r)) noSubs

/-- Consecutive ORDER BY terms over one item (`SortRun`). -/
structure SortRun where
  item : Nat
  /-- Each term: its key, its direction and collation, and the result column
  that returns it. -/
  terms : List (SortKey × Dir × Nat)
  deriving Repr

/-- The run's keys, each with its direction and collation. -/
def SortRun.keys (run : SortRun) : List (SortKey × Dir) := run.terms.map fun (key, d, _) => (key, d)

/-- The ORDER BY's terms, each with its item, as runs of consecutive terms
over one item. -/
def SortRun.group : List (Nat × SortKey × Dir × Nat) → List SortRun
  | [] => []
  | (i, term) :: rest =>
    match group rest with
    | run :: runs =>
      if run.item = i then { run with terms := term :: run.terms } :: runs
      else ⟨i, [term]⟩ :: run :: runs
    | [] => [⟨i, [term]⟩]

structure SortedLimit where
  /-- The ORDER BY's runs, in order. -/
  runs : List SortRun
  limit : Nat
  deriving Repr

/-- The key the result column `e` sorts item `i`'s rows by: a column, or an
expression over its columns and no subquery, where each column it names is
one sqlive narrows by (`sort_key`). -/
def sortKey (s : Statement) (i : Nat) (e : Expr) : Option SortKey :=
  if e.queries.isEmpty && !e.refs.isEmpty &&
      e.refs.all (fun (j, c) => j == i && (s.affinity i c).isSome) then
    some (match e with
      | .col _ c => .column c
      | e => .expr e)
  else none

/-- The order and LIMIT of the root query, when a full run's last row bounds
the scopes of the items it sorts (`sorted_limit`); `st` is the state after
the statement's conditions are read. -/
def sortedLimit (s : Statement) (st : PState) : Option SortedLimit := do
  let query := s.query s.root
  let [k] := query.arms | none
  let n ← query.limit
  if query.orderBy.isEmpty ∨ !query.orderNamed ∨ n = 0 ∨ query.offset.isSome then none
  let c := s.core k
  if c.aggregated || c.windowed then none
  let terms ← query.orderBy.mapM fun (p, desc) => do
    let .expr e ← c.output[p]? | none
    let (i, _) ← e.refs.head?
    if (s.item i).core ≠ k ∨ (s.table? i).isNone ∨ (st.items i).leftOn.isSome then none
    let position ← c.output.findIdx? fun
      | .expr e' => e' == e
      | _ => false
    pure (i, (← sortKey s i e), desc, position)
  pure { runs := SortRun.group terms, limit := n }

/-- The rows of item `i` whose key is NULL, or isn't when `not`. -/
def SortKey.nullScope (i : Nat) (not : Bool) : SortKey → RScope
  | .column c => .null c not
  | .expr e => .row i (.null e not)

/-- The rows of item `i` whose key compares with `x` by `op` in the
collation whose key is `k`. -/
def SortKey.compareScope (i : Nat) (k : Int → Int) (op : Op) (x : Int) : SortKey → RScope
  | .column c => .ordered k (.col i c) op x
  | .expr e => .ordered k e op x

/-- The rows of item `i` whose key sorts strictly before `v` as `d` orders:
NULL first ascending and last descending; `none` when no value does. -/
def SortKey.beforeScope (i : Nat) (key : SortKey) (d : Dir) : Value → Option RScope
  | none => if d.desc then some (key.nullScope i true) else none
  | some x =>
    if d.desc then some (key.compareScope i d.key .gt x)
    else some ((key.nullScope i false).either (key.compareScope i d.key .lt x))

/-- The rows of item `i` whose key ties with `v` as `d` orders, NULL
included. -/
def SortKey.equalScope (i : Nat) (key : SortKey) (d : Dir) : Value → RScope
  | none => key.nullScope i false
  | some x => key.compareScope i d.key .eq x

/-- The rows either scope passes; `none` passes no row. -/
def orTied : Option RScope → Option RScope → Option RScope
  | some before, some tied => some (before.either tied)
  | before, none => before
  | none, tied => tied

/-- The rows of item `i` that sort before `last`, the ORDER BY's values of a
run's last row, or tie with it and pass `tied`: term by term, NULL first
ascending and last descending; `none` when no row does
(`sorted_before`). -/
def sortedBefore (i : Nat) (terms : List (SortKey × Dir)) (last : List Value)
    (tied : Option RScope) : Option RScope :=
  (terms.zip last).foldr (fun ((key, d), v) scope =>
    orTied (key.beforeScope i d v) (scope.map (key.equalScope i d v).both)) tied

/-- The rows of item `i` that sort at or before `last`: a row equal to `last`
passes. -/
def atOrBefore (i : Nat) (terms : List (SortKey × Dir)) (last : List Value) : RScope :=
  (sortedBefore i terms last (some .all)).getD .all

/-- Item `i`'s bound from the ORDER BY's runs, each paired with the last
row's values of its terms, when an item of its runs (`SortedLimit::bound`):
the first run over `i` at or before its values, or, for each earlier run, a
row `i` joins of that run's item that sorts before them. `lookup j` gives
the scope of `i`'s rows that join a row of `j` in `j`'s scope that passes a
given scope too, or `none` when `i` takes no value from `j`, and then `i` has
no bound. -/
def runBound (i : Nat) (lookup : Nat → Option (RScope → RScope)) :
    List (SortRun × List Value) → Option RScope
  | [] => none
  | (run, last) :: rest =>
    if run.item = i then some (atOrBefore i run.keys last)
    else do
      let later ← runBound i lookup rest
      let look ← lookup run.item
      pure (match sortedBefore run.item run.keys last none with
        | some before => (look before).either later
        | none => later)

/-- The last row's ORDER BY values, run by run, when the run returned as
many rows as the LIMIT. -/
def lastValues (sorted : SortedLimit) (rows : List Row) : Option (List (List Value)) :=
  if rows.length = sorted.limit then
    rows.getLast?.map fun r => sorted.runs.map fun run => run.terms.map fun (_, _, p) => r.cell p
  else none

/-! The proof. An ORDER BY orders rows by a total preorder, and the model's
sort is stable, so sorting the rows that pass a test gives the sorted rows
that pass it. When a run returned LIMIT rows, every row it returned sorts at
or before its last row, so the rows that sort after the last row change
nothing in the first LIMIT rows. -/

/-- How an ORDER BY term compares two values, descending when `d` says so,
and by its collation. -/
def dcmp (d : Dir) (a b : Value) : Ordering :=
  if d.desc then (d.compare a b).swap else d.compare a b

theorem valueCompare_some (x y : Int) :
    valueCompare (some x) (some y) = if x < y then .lt else if x = y then .eq else .gt := rfl

theorem valueCompare_swap (a b : Value) : valueCompare b a = (valueCompare a b).swap := by
  cases a with
  | none => cases b <;> rfl
  | some x =>
    cases b with
    | none => rfl
    | some y =>
      rw [valueCompare_some, valueCompare_some]
      by_cases h1 : y < x
      · have h2 : ¬x < y := by omega
        have h3 : ¬x = y := by omega
        simp [h1, h2, h3, Ordering.swap]
      · by_cases h2 : x < y
        · have h3 : ¬y = x := by omega
          simp [h1, h2, h3, Ordering.swap]
        · have h3 : y = x := by omega
          subst h3
          simp [Ordering.swap]

theorem valueCompare_eq {a b : Value} : valueCompare a b = .eq ↔ a = b := by
  cases a with
  | none => cases b <;> simp [valueCompare]
  | some x =>
    cases b with
    | none => simp [valueCompare]
    | some y =>
      rw [valueCompare_some]
      split
      · simp; omega
      · split
        · simp_all
        · simp; omega

theorem valueCompare_lt {a b : Value} : valueCompare a b = .lt ↔
    (a = none ∧ b ≠ none) ∨ ∃ x y, a = some x ∧ b = some y ∧ x < y := by
  cases a with
  | none => cases b <;> simp [valueCompare]
  | some x =>
    cases b with
    | none => simp [valueCompare]
    | some y =>
      rw [valueCompare_some]
      by_cases h1 : x < y <;> by_cases h2 : x = y <;> simp [h1, h2]

theorem valueCompare_lt_trans {a b c : Value} (h1 : valueCompare a b = .lt)
    (h2 : valueCompare b c = .lt) : valueCompare a c = .lt := by
  rw [valueCompare_lt] at h1 h2 ⊢
  rcases h1 with ⟨rfl, _⟩ | ⟨x, y, rfl, rfl, hxy⟩
  · rcases h2 with ⟨h, _⟩ | ⟨y, z, rfl, rfl, _⟩
    · simp_all
    · exact Or.inl ⟨rfl, by simp⟩
  · rcases h2 with ⟨h, _⟩ | ⟨y', z, h', rfl, hyz⟩
    · simp at h
    · cases h'
      exact Or.inr ⟨x, z, rfl, rfl, by omega⟩

theorem Dir.compare_swap (d : Dir) (a b : Value) : d.compare b a = (d.compare a b).swap :=
  valueCompare_swap _ _

theorem dcmp_swap (d : Dir) (a b : Value) : dcmp d b a = (dcmp d a b).swap := by
  unfold dcmp
  cases d.desc <;> simp [Dir.compare_swap d a b, Ordering.swap_swap]

/-- Two values tie when their keys are equal, which the model's collations
let two different values do (`'a'` and `'A'` in NOCASE). -/
theorem dcmp_eq {d : Dir} {a b : Value} : dcmp d a b = .eq ↔ a.map d.key = b.map d.key := by
  unfold dcmp Dir.compare
  cases d.desc <;> simp [valueCompare_eq, Ordering.swap_eq_eq]

/-- A value that ties with another sorts the same against any third. -/
theorem dcmp_congr_right {d : Dir} {a b c : Value} (h : b.map d.key = c.map d.key) :
    dcmp d a b = dcmp d a c := by
  unfold dcmp Dir.compare; rw [h]

theorem dcmp_congr_left {d : Dir} {a b c : Value} (h : a.map d.key = b.map d.key) :
    dcmp d a c = dcmp d b c := by
  unfold dcmp Dir.compare; rw [h]

theorem dcmp_lt_trans {d : Dir} {a b c : Value} (h1 : dcmp d a b = .lt) (h2 : dcmp d b c = .lt) :
    dcmp d a c = .lt := by
  unfold dcmp Dir.compare at *
  cases hd : d.desc
  · rw [hd] at h1 h2
    exact valueCompare_lt_trans h1 h2
  · rw [hd] at h1 h2
    simp only [↓reduceIte, Ordering.swap_eq_lt, Bool.true_eq_false] at h1 h2 ⊢
    have h1' : valueCompare (b.map d.key) (a.map d.key) = .lt := by rw [valueCompare_swap, h1]; rfl
    have h2' : valueCompare (c.map d.key) (b.map d.key) = .lt := by rw [valueCompare_swap, h2]; rfl
    have := valueCompare_lt_trans h2' h1'
    rw [valueCompare_swap, this]; rfl

theorem keyCompare_cons (d : Dir) (ds : List Dir) (a b : Value) (as bs : List Value) :
    keyCompare (d :: ds) (a :: as) (b :: bs) = (dcmp d a b).then (keyCompare ds as bs) := by
  simp [keyCompare, dcmp]

theorem then_ne_gt {x y : Ordering} : x.then y ≠ .gt ↔ x = .lt ∨ (x = .eq ∧ y ≠ .gt) := by
  cases x <;> simp [Ordering.then]

theorem keyCompare_swap (ds : List Dir) (as bs : List Value) :
    keyCompare ds bs as = (keyCompare ds as bs).swap := by
  induction ds generalizing as bs with
  | nil => simp [keyCompare, Ordering.swap]
  | cons d ds ih =>
    cases as with
    | nil => cases bs <;> simp [keyCompare, Ordering.swap]
    | cons a as =>
      cases bs with
      | nil => simp [keyCompare, Ordering.swap]
      | cons b bs => rw [keyCompare_cons, keyCompare_cons, ih, dcmp_swap, Ordering.swap_then]

theorem keyLe_cons (d : Dir) (ds : List Dir) (a v : Value) (as vs : List Value) :
    keyLe (d :: ds) (a :: as) (v :: vs) =
      (dcmp d a v == .lt || (dcmp d a v == .eq && keyLe ds as vs)) := by
  simp only [keyLe, keyCompare_cons]
  cases dcmp d a v <;> cases keyCompare ds as vs <;> rfl

theorem keyLe_total (ds : List Dir) (as bs : List Value) : keyLe ds as bs || keyLe ds bs as := by
  simp only [keyLe, keyCompare_swap ds as bs, Bool.or_eq_true, bne_iff_ne, ne_eq]
  cases keyCompare ds as bs <;> simp [Ordering.swap]

theorem keyLe_trans {ds : List Dir} {as bs cs : List Value} (ha : as.length = ds.length)
    (hb : bs.length = ds.length) (hc : cs.length = ds.length)
    (h1 : keyLe ds as bs) (h2 : keyLe ds bs cs) : keyLe ds as cs := by
  simp only [keyLe, bne_iff_ne, ne_eq] at *
  induction ds generalizing as bs cs with
  | nil => simp [keyCompare]
  | cons d ds ih =>
    cases as <;> cases bs <;> cases cs <;> simp at ha hb hc
    rename_i a as b bs c cs
    rw [keyCompare_cons] at h1 h2 ⊢
    rw [← ne_eq, then_ne_gt] at h1 h2 ⊢
    rcases h1 with h1 | ⟨h1, r1⟩ <;> rcases h2 with h2 | ⟨h2, r2⟩
    · exact Or.inl (dcmp_lt_trans h1 h2)
    · rw [dcmp_congr_right (dcmp_eq.mp h2)] at h1; exact Or.inl h1
    · rw [dcmp_congr_left (dcmp_eq.mp h1)]; exact Or.inl h2
    · refine Or.inr ⟨?_, ih ha hb hc r1 r2⟩
      rw [dcmp_congr_left (dcmp_eq.mp h1)]; exact h2

/-! A stable sort and a test. -/

section Sorting

variable {α : Type} {le : α → α → Bool}
  (total : ∀ a b, le a b || le b a) (trans : ∀ a b c, le a b → le b c → le a c)

include total trans in
theorem pairwise_insertSorted {x : α} {l : List α} (h : l.Pairwise (le · ·)) :
    (insertSorted le x l).Pairwise (le · ·) := by
  induction l with
  | nil => simp [insertSorted]
  | cons y ys ih =>
    rw [List.pairwise_cons] at h
    unfold insertSorted
    split
    · rename_i hxy
      refine List.pairwise_cons.mpr ⟨?_, List.pairwise_cons.mpr h⟩
      intro z hz
      simp only [List.mem_cons] at hz
      rcases hz with rfl | hz
      · exact hxy
      · exact trans _ _ _ hxy (h.1 z hz)
    · rename_i hxy
      have hyx : le y x := by have := total x y; simp_all
      refine List.pairwise_cons.mpr ⟨?_, ih h.2⟩
      intro z hz
      rcases mem_insertSorted.mp hz with rfl | hz
      · exact hyx
      · exact h.1 z hz

include total trans in
theorem pairwise_sortBy (l : List α) : (sortBy le l).Pairwise (le · ·) := by
  induction l with
  | nil => simp [sortBy]
  | cons x xs ih => exact pairwise_insertSorted total trans ih

theorem insertSorted_le {x y : α} {ys : List α} (h : le x y) :
    insertSorted le x (y :: ys) = x :: y :: ys := by
  simp [insertSorted, h]

theorem insertSorted_not_le {x y : α} {ys : List α} (h : le x y = false) :
    insertSorted le x (y :: ys) = y :: insertSorted le x ys := by
  simp [insertSorted, h]

theorem insertSorted_least {x : α} {l : List α} (h : ∀ y ∈ l, le x y) :
    insertSorted le x l = x :: l := by
  cases l with
  | nil => rfl
  | cons y ys => exact insertSorted_le (h y (by simp))

include trans in
theorem filter_insertSorted (P : α → Bool) {x : α} {l : List α} (h : l.Pairwise (le · ·)) :
    (insertSorted le x l).filter P =
      if P x then insertSorted le x (l.filter P) else l.filter P := by
  induction l with
  | nil => cases hp : P x <;> simp [insertSorted, hp]
  | cons y ys ih =>
    rw [List.pairwise_cons] at h
    cases hxy : le x y
    · rw [insertSorted_not_le hxy, List.filter_cons, ih h.2]
      cases hp : P x <;> cases hpy : P y <;> simp [hpy, insertSorted_not_le hxy]
    · rw [insertSorted_le hxy]
      cases hp : P x
      · simp [hp]
      · have hall : ∀ z ∈ (y :: ys).filter P, le x z := by
          intro z hz
          have hz' := (List.mem_filter.mp hz).1
          simp only [List.mem_cons] at hz'
          rcases hz' with rfl | hz'
          · exact hxy
          · exact trans _ _ _ hxy (h.1 z hz')
        simp only [↓reduceIte, insertSorted_least hall, List.filter_cons_of_pos hp]

include total trans in
/-- Sorting the rows that pass a test gives the sorted rows that pass it. -/
theorem sortBy_filter (P : α → Bool) (l : List α) :
    sortBy le (l.filter P) = (sortBy le l).filter P := by
  induction l with
  | nil => simp [sortBy]
  | cons x xs ih =>
    simp only [sortBy, List.filter_cons]
    rw [filter_insertSorted trans P (pairwise_sortBy total trans xs), ← ih]
    cases hp : P x <;> simp [sortBy]

/-- A sorted list's rows that pass a test closed downward come first. -/
theorem split_sorted {P : α → Bool} (down : ∀ a b, le a b → P b → P a) {l : List α}
    (h : l.Pairwise (le · ·)) : l = l.filter P ++ l.filter (fun a => !P a) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    rw [List.pairwise_cons] at h
    cases hp : P x
    · have hnone : ∀ y ∈ xs, P y = false := by
        intro y hy
        cases hpy : P y
        · rfl
        · have := down x y (h.1 y hy) hpy
          rw [hp] at this
          cases this
      have h1 : xs.filter P = [] := List.filter_eq_nil_iff.mpr (fun y hy => by simp [hnone y hy])
      have h2 : xs.filter (fun a => !P a) = xs :=
        List.filter_eq_self.mpr (fun y hy => by simp [hnone y hy])
      rw [List.filter_cons, List.filter_cons, hp, h1, h2]
      rfl
    · rw [List.filter_cons, List.filter_cons, hp]
      simp only [↓reduceIte, Bool.not_true, Bool.false_eq_true, List.cons_append]
      exact congrArg _ (ih h.2)

theorem take_filter {P : α → Bool} {n : Nat} {l : List α} (h : ∀ x ∈ l.take n, P x) :
    l.take n = (l.filter P).take n := by
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
    cases n with
    | zero => simp
    | succ n =>
      have hx : P x := h x (by simp)
      simp only [List.take_succ_cons, List.filter_cons, hx, ↓reduceIte]
      exact congrArg _ (ih fun y hy => h y (by simp [hy]))

theorem le_last {R : α → α → Prop} {l : List α} {y x : α} (h : l.Pairwise R)
    (hy : l.getLast? = some y) (hx : x ∈ l) : x = y ∨ R x y := by
  obtain ⟨ys, rfl⟩ := List.getLast?_eq_some_iff.mp hy
  rw [List.pairwise_append] at h
  simp only [List.mem_append, List.mem_singleton] at hx
  rcases hx with hx | hx
  · exact Or.inr (h.2.2 x hx y (by simp))
  · exact Or.inl hx

include total trans in
/-- When the first `n` sorted rows are `n` rows, rows that sort after the
last of them change nothing in them. -/
theorem take_sortBy {P : α → Bool} (down : ∀ a b, le a b → P b → P a) {n : Nat} {l l' : List α}
    (hlast : ∀ y, (sortBy le l |>.take n).getLast? = some y → P y)
    (hlen : (sortBy le l |>.take n).length = n) (heq : l.filter P = l'.filter P) :
    (sortBy le l').take n = (sortBy le l).take n := by
  have hs := pairwise_sortBy total trans l
  have hs' := pairwise_sortBy total trans l'
  have hpass : ∀ x ∈ (sortBy le l).take n, P x := by
    intro x hx
    cases hy : ((sortBy le l).take n).getLast? with
    | none =>
      rw [List.getLast?_eq_none_iff] at hy
      rw [hy] at hx
      cases hx
    | some y =>
      rcases le_last (hs.sublist (List.take_sublist n _)) hy hx with rfl | hxy
      · exact hlast _ hy
      · exact down x y hxy (hlast y hy)
  have h1 := take_filter hpass
  have hf : (sortBy le l).filter P = (sortBy le l').filter P := by
    rw [← sortBy_filter total trans, ← sortBy_filter total trans, heq]
  rw [hf] at h1
  have hn : n ≤ ((sortBy le l').filter P).length := by
    have := congrArg List.length h1
    rw [hlen, List.length_take] at this
    omega
  have hsplit := congrArg (List.take n) (split_sorted down hs')
  rw [List.take_append_of_le_length hn] at hsplit
  rw [h1]
  exact hsplit

theorem length_insertSorted (x : α) (l : List α) : (insertSorted le x l).length = l.length + 1 := by
  induction l with
  | nil => rfl
  | cons y ys ih =>
    unfold insertSorted
    split
    · rfl
    · simp [ih]

theorem length_sortBy (l : List α) : (sortBy le l).length = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [sortBy, length_insertSorted, ih]

include total trans in
/-- The first `n` sorted rows of a list holding at least `n` rows that pass a
test closed downward are the first `n` sorted rows that pass it. -/
theorem take_sortBy_passing {P : α → Bool} (down : ∀ a b, le a b → P b → P a) {n : Nat}
    {l : List α} (hn : n ≤ (l.filter P).length) :
    (sortBy le l).take n = (sortBy le (l.filter P)).take n := by
  have hs := pairwise_sortBy total trans l
  have hsplit := congrArg (List.take n) (split_sorted down hs)
  rw [← sortBy_filter total trans] at hsplit
  rw [List.take_append_of_le_length (by rw [length_sortBy]; exact hn)] at hsplit
  exact hsplit

include total trans in
/-- Two lists that hold the same rows that pass a test closed downward, at
least `n` of them, have the same first `n` sorted rows: a window's LIMIT
keeps rows that pass. -/
theorem take_sortBy_window {P : α → Bool} (down : ∀ a b, le a b → P b → P a) {n : Nat}
    {l l' : List α} (heq : l.filter P = l'.filter P) (hn : n ≤ (l'.filter P).length) :
    (sortBy le l).take n = (sortBy le l').take n := by
  rw [take_sortBy_passing total trans down (heq ▸ hn), take_sortBy_passing total trans down hn, heq]

end Sorting

/-- An ORDER BY's sort key of a row. -/
def orderKey (q : Query) (r : Row) : List Value := q.orderBy.map fun (p, _) => r.cell p

/-- Rule 6's theorem: a query with an ORDER BY, a LIMIT and no OFFSET whose
run on `rows` returned as many rows as its LIMIT, the last with ORDER BY values
`last`, returns the same rows from any `rows'` that hold the same rows
sorting at or before `last`, in the same order. -/
theorem limit_sound {q : Query} {n : Nat} (hlimit : q.limit = some n) (hoffset : q.offset = none)
    (horder : q.orderBy ≠ [])
    {rows rows' : List Row} {last : List Value}
    (hlen : (q.post rows).length = n)
    (hlast : ((q.post rows).getLast?.map (orderKey q)) = some last)
    (heq : rows.filter (fun r => keyLe (q.orderBy.map (·.2)) (orderKey q r) last) =
      rows'.filter (fun r => keyLe (q.orderBy.map (·.2)) (orderKey q r) last)) :
    q.post rows' = q.post rows := by
  have hne : q.orderBy.isEmpty = false := by simpa using horder
  have hlen_key : ∀ r, (orderKey q r).length = (q.orderBy.map (·.2)).length := by
    intro r; simp [orderKey]
  simp only [Query.post, hne, Bool.false_eq_true, ↓reduceIte, hlimit, hoffset, Option.getD_none,
    List.drop_zero] at hlen hlast ⊢
  obtain ⟨y, hy, hky⟩ := Option.map_eq_some_iff.mp hlast
  have hlast_len : last.length = (q.orderBy.map (·.2)).length := by rw [← hky]; exact hlen_key y
  apply take_sortBy (P := fun r => keyLe (q.orderBy.map (·.2)) (orderKey q r) last)
  · intro a b; exact keyLe_total _ _ _
  · intro a b c h1 h2; exact keyLe_trans (hlen_key a) (hlen_key b) (hlen_key c) h1 h2
  · intro a b hab hb
    exact keyLe_trans (hlen_key a) (hlen_key b) hlast_len hab hb
  · intro y' hy'
    rw [hy] at hy'
    cases hy'
    rw [← hky]
    simp only [keyLe, bne_iff_ne, ne_eq]
    have : keyCompare (q.orderBy.map (·.2)) (orderKey q y) (orderKey q y) = .eq := by
      have := keyCompare_swap (q.orderBy.map (·.2)) (orderKey q y) (orderKey q y)
      revert this
      cases keyCompare (q.orderBy.map (·.2)) (orderKey q y) (orderKey q y) <;> simp [Ordering.swap]
    simp [this]
  · exact hlen
  · exact heq

/-! The scope the bound gives: a row passes when it sorts at or before the
last row. -/

theorem passes_both (rowsOf : Nat → List Row) (r : Row) (a b : RScope) :
    (a.both b).passes rowsOf r = (a.passes rowsOf r && b.passes rowsOf r) := by
  unfold RScope.both
  split <;> simp_all [RScope.passes]

theorem passes_either (rowsOf : Nat → List Row) (r : Row) (a b : RScope) :
    (a.either b).passes rowsOf r = (a.passes rowsOf r || b.passes rowsOf r) := by
  unfold RScope.either
  split <;> simp_all [RScope.passes]

theorem null_passes (rowsOf : Nat → List Row) (r : Row) (i : Nat) (n : Bool) (key : SortKey) :
    (key.nullScope i n).passes rowsOf r = ((key.value r).isNone != n) := by
  cases key <;> simp [SortKey.nullScope, SortKey.value, RScope.passes, Cond.eval, isTrue]

/-- A key's comparison by `<`, `>` or `=` in a collation passes a row whose key
holds a value that compares so by the collation's key, and no row whose key
is NULL. -/
theorem compare_passes (rowsOf : Nat → List Row) (r : Row) (i : Nat) (k : Int → Int)
    (op : Op) (x : Int) (key : SortKey) :
    (key.compareScope i k op x).passes rowsOf r =
      match key.value r with
      | some y => op.compare (k y) (k x)
      | none => false := by
  cases key with
  | column c => rfl
  | expr e => rfl

/-- Whether a scope that may pass no row passes `r`. -/
def passesSome (rowsOf : Nat → List Row) (r : Row) (scope : Option RScope) : Bool :=
  (scope.map (·.passes rowsOf r)).getD false

theorem orTied_passes (rowsOf : Nat → List Row) (r : Row) (before tied : Option RScope) :
    passesSome rowsOf r (orTied before tied) =
      (passesSome rowsOf r before || passesSome rowsOf r tied) := by
  cases before <;> cases tied <;> simp [orTied, passesSome, passes_either]

theorem map_both_passes (rowsOf : Nat → List Row) (r : Row) (a : RScope) (scope : Option RScope) :
    passesSome rowsOf r (scope.map a.both) = (a.passes rowsOf r && passesSome rowsOf r scope) := by
  cases scope <;> simp [passesSome, passes_both]

theorem before_passes (rowsOf : Nat → List Row) (r : Row) (i : Nat) (key : SortKey) (d : Dir)
    (v : Value) :
    ((key.beforeScope i d v).map (·.passes rowsOf r)).getD false =
      (dcmp d (key.value r) v == .lt) := by
  obtain ⟨desc, k⟩ := d
  cases desc <;> cases v with
  | none =>
    cases hx : key.value r <;>
      simp [SortKey.beforeScope, null_passes, hx, dcmp, Dir.compare, valueCompare, Ordering.swap]
  | some y =>
    cases hx : key.value r with
    | none =>
      simp [SortKey.beforeScope, null_passes, compare_passes, passes_either, hx, dcmp,
        Dir.compare, valueCompare, Ordering.swap]
    | some x =>
      simp only [SortKey.beforeScope, dcmp, Dir.compare, Bool.false_eq_true, ↓reduceIte,
        Option.map_some, valueCompare_some, passes_either, null_passes, compare_passes, hx,
        Op.compare, Option.isNone_some, bne_self_eq_false, Bool.false_or, Option.getD_some]
      by_cases h1 : k x < k y <;> by_cases h2 : k x = k y <;> simp [h1, h2, Ordering.swap]
      all_goals omega
theorem equal_passes (rowsOf : Nat → List Row) (r : Row) (i : Nat) (key : SortKey) (d : Dir)
    (v : Value) :
    (key.equalScope i d v).passes rowsOf r = (dcmp d (key.value r) v == .eq) := by
  obtain ⟨desc, k⟩ := d
  cases desc <;> cases v with
  | none => cases hx : key.value r <;> simp [SortKey.equalScope, null_passes, hx, dcmp,
      Dir.compare, valueCompare, Ordering.swap]
  | some y =>
    cases hx : key.value r with
    | none => simp [SortKey.equalScope, compare_passes, hx, dcmp, Dir.compare, valueCompare,
        Ordering.swap]
    | some x =>
      simp only [SortKey.equalScope, compare_passes, hx, dcmp, Dir.compare, Bool.false_eq_true,
        ↓reduceIte, Option.map_some, valueCompare_some, Op.compare]
      by_cases h : k x = k y <;> by_cases h3 : k x < k y <;> simp [h, h3, Ordering.swap]

/-- The scope `sortedBefore` gives passes a row of item `i` when the row's
values of the keys sort before `last`, or equal it and pass `tied`. -/
theorem sortedBefore_passes (rowsOf : Nat → List Row) (r : Row) (i : Nat)
    (terms : List (SortKey × Dir)) (last : List Value) (tied : Option RScope)
    (hlen : last.length = terms.length) :
    passesSome rowsOf r (sortedBefore i terms last tied) =
      match keyCompare (terms.map (·.2)) (terms.map (·.1.value r)) last with
      | .lt => true
      | .eq => passesSome rowsOf r tied
      | .gt => false := by
  induction terms generalizing last with
  | nil => simp [sortedBefore, keyCompare]
  | cons term terms ih =>
    cases last with
    | nil => simp at hlen
    | cons v vs =>
      simp only [List.length_cons, Nat.add_right_cancel_iff] at hlen
      obtain ⟨key, d⟩ := term
      have hrest := ih vs hlen
      simp only [sortedBefore, List.zip_cons_cons, List.foldr_cons] at hrest ⊢
      rw [orTied_passes, map_both_passes, hrest, List.map_cons, List.map_cons, keyCompare_cons]
      have hb := before_passes rowsOf r i key d v
      have he := equal_passes rowsOf r i key d v
      simp only [passesSome] at hb ⊢
      rw [hb, he]
      cases dcmp d (key.value r) v <;>
        cases keyCompare (terms.map (·.2)) (terms.map (·.1.value r)) vs <;>
        simp [Ordering.then]

theorem sortedBefore_isSome (i : Nat) (terms : List (SortKey × Dir)) (last : List Value)
    {tied : Option RScope} (h : tied.isSome) : (sortedBefore i terms last tied).isSome := by
  induction terms generalizing last with
  | nil => simpa [sortedBefore] using h
  | cons term terms ih =>
    cases last with
    | nil => simpa [sortedBefore] using h
    | cons v vs =>
      obtain ⟨key, d⟩ := term
      have hrest := ih vs
      simp only [sortedBefore, List.zip_cons_cons, List.foldr_cons] at hrest ⊢
      obtain ⟨rest, hr⟩ := Option.isSome_iff_exists.mp hrest
      rw [hr]
      cases key.beforeScope i d v <;> simp [orTied]

/-- The bound's scope passes a row of the sorted item when the row's values
of the ORDER BY's keys sort at or before `last`. -/
theorem atOrBefore_passes (rowsOf : Nat → List Row) (r : Row) (i : Nat)
    (terms : List (SortKey × Dir)) (last : List Value) (hlen : last.length = terms.length) :
    (atOrBefore i terms last).passes rowsOf r =
      keyLe (terms.map (·.2)) (terms.map (·.1.value r)) last := by
  have hsome := sortedBefore_isSome i terms last (tied := some .all) rfl
  have h := sortedBefore_passes rowsOf r i terms last (some .all) hlen
  obtain ⟨scope, hs⟩ := Option.isSome_iff_exists.mp hsome
  simp only [atOrBefore, hs, Option.getD_some]
  simp only [passesSome, hs, Option.map_some, Option.getD_some, RScope.passes] at h
  rw [h, keyLe]
  cases keyCompare (terms.map (·.2)) (terms.map (·.1.value r)) last <;> rfl

/-- A key `sortKey` reads sorts the core's bindings as its result column
does: in a binding where the sorted item has the row `r`, the result column's
expression gives the key's value in `r`. -/
theorem sortKey_value {s : Statement} {i : Nat} {e : Expr} {key : SortKey}
    (h : sortKey s i e = some key) {b : Bind} {sub : Subs} {r : Row} (hb : b i = some r) :
    e.eval b sub = key.value r := by
  unfold sortKey at h
  split at h
  · rename_i hcheck
    simp only [Bool.and_eq_true, List.isEmpty_iff, Bool.not_eq_true', List.isEmpty_eq_false_iff,
      List.all_eq_true] at hcheck
    obtain ⟨⟨hq, _⟩, hrefs⟩ := hcheck
    have hitems : ∀ j ∈ e.items, j = i := by
      intro j hj
      simp only [Expr.items, List.mem_map] at hj
      obtain ⟨⟨j', c⟩, hmem, rfl⟩ := hj
      have := hrefs (j', c) hmem
      simp only [beq_iff_eq] at this
      exact this.1
    rw [e.eval_rowBind hitems hq, hb]
    cases h
    cases e <;> simp [SortKey.value, Expr.eval, cellOf, rowBind]
  · cases h

/-! The bound of each item a run sorts. A key sorts at or before the last
row's when its first run's values sort before that run's, or equal them and
the rest sort at or before. So an item whose first run comes first sorts at
or before that run's values, and an item after it does too, unless some
earlier run's values sort before. -/

theorem keyCompare_append (ds ds' : List Dir) (as as' bs bs' : List Value)
    (ha : as.length = ds.length) (hb : bs.length = ds.length) :
    keyCompare (ds ++ ds') (as ++ as') (bs ++ bs') =
      (keyCompare ds as bs).then (keyCompare ds' as' bs') := by
  induction ds generalizing as bs with
  | nil =>
    cases as <;> cases bs <;> simp_all [keyCompare, Ordering.then]
  | cons d ds ih =>
    cases as with
    | nil => simp at ha
    | cons a as =>
      cases bs with
      | nil => simp at hb
      | cons b bs =>
        simp only [List.length_cons, Nat.add_right_cancel_iff] at ha hb
        rw [List.cons_append, List.cons_append, List.cons_append, keyCompare_cons,
          keyCompare_cons, ih as bs ha hb, Ordering.then_assoc]

/-- The runs' directions, in order. -/
def runDirections (runs : List SortRun) : List Dir := runs.flatMap fun run => run.keys.map (·.2)

/-- The ORDER BY's values of a binding whose items hold the rows `row`: each
run's keys' values in its item's row. -/
def runKey (row : Nat → Row) (runs : List SortRun) : List Value :=
  runs.flatMap fun run => run.keys.map (·.1.value (row run.item))

theorem runDirections_cons (run : SortRun) (runs : List SortRun) :
    runDirections (run :: runs) = run.keys.map (·.2) ++ runDirections runs := by
  simp [runDirections]

theorem runKey_cons (row : Nat → Row) (run : SortRun) (runs : List SortRun) :
    runKey row (run :: runs) = run.keys.map (·.1.value (row run.item)) ++ runKey row runs := by
  simp [runKey]

/-- An item's bound passes its row in a binding whose ORDER BY values sort at
or before the last row's, when each lookup the bound makes of a run's item
passes the item's row wherever the partner's row passes the lookup's scope
(`joined_passes` gives that of a membership). -/
theorem runBound_passes (rowsOf : Nat → List Row) (row : Nat → Row) (i : Nat)
    (lookup : Nat → Option (RScope → RScope))
    (runs : List (SortRun × List Value))
    (hlookup : ∀ p ∈ runs, ∀ look, lookup p.1.item = some look → ∀ scope : RScope,
      scope.passes rowsOf (row p.1.item) → (look scope).passes rowsOf (row i))
    (hlen : ∀ p ∈ runs, p.2.length = p.1.terms.length)
    (hle : keyLe (runDirections (runs.map (·.1))) (runKey row (runs.map (·.1)))
      (runs.flatMap (·.2)))
    {bound : RScope} (h : runBound i lookup runs = some bound) : bound.passes rowsOf (row i) := by
  induction runs generalizing bound with
  | nil => simp [runBound] at h
  | cons p rest ih =>
    obtain ⟨run, last⟩ := p
    have hl : last.length = run.keys.length := by
      simpa [SortRun.keys] using hlen (run, last) (by simp)
    have hrest : ∀ p ∈ rest, p.2.length = p.1.terms.length := fun p hp => hlen p (by simp [hp])
    rw [List.map_cons, runDirections_cons, runKey_cons, List.flatMap_cons] at hle
    unfold keyLe at hle
    rw [bne_iff_ne, keyCompare_append _ _ _ _ _ _ (by simp) (by simp [hl]), ne_eq,
      ← ne_eq, then_ne_gt] at hle
    by_cases hi : run.item = i
    · subst hi
      simp only [runBound, ↓reduceIte, Option.some.injEq] at h
      subst h
      rw [atOrBefore_passes _ _ _ _ _ hl, keyLe, bne_iff_ne]
      rcases hle with h1 | ⟨h1, _⟩ <;> rw [h1] <;> simp
    · simp only [runBound, hi, ↓reduceIte] at h
      cases hlater : runBound i lookup rest with
      | none => simp [hlater] at h
      | some later =>
        cases hlook : lookup run.item with
        | none => simp [hlater, hlook] at h
        | some look =>
          simp only [hlater, hlook, Option.bind_eq_bind, Option.bind_some, Option.pure_def,
            Option.some.injEq] at h
          rcases hle with hlt | ⟨_, hge⟩
          · have hb := sortedBefore_passes rowsOf (row run.item) run.item run.keys last none hl
            rw [hlt] at hb
            cases hs : sortedBefore run.item run.keys last none with
            | none => rw [hs] at hb; simp [passesSome] at hb
            | some before =>
              rw [hs] at hb h
              simp only at h
              subst h
              simp only [passesSome, Option.map_some, Option.getD_some] at hb
              rw [passes_either, hlookup (run, last) (by simp) look hlook before hb, Bool.true_or]
          · have hpass := ih (fun p hp => hlookup p (by simp [hp])) hrest
              (by unfold keyLe; exact bne_iff_ne.mpr hge) hlater
            cases hs : sortedBefore run.item run.keys last none <;> rw [hs] at h <;>
              simp only at h <;> subst h <;> simp [passes_either, hpass]

/-- A membership's lookup passes the member's row in a binding where its edge
holds, when the partner's row in the binding is among `rowsOf` the partner's
rows and passes the lookup's scope. -/
theorem joined_passes {s : Statement} {db : Db} {f : Nat} {b : Bind} {e : Edge}
    (hnested : e.nested = false) (hholds : e.HoldsIn s db f b) {r r' : Row}
    (hm : b e.member = some r) (hp : b e.partner = some r') (rowsOf : Nat → List Row)
    (hr' : r' ∈ rowsOf e.partner) {scope : RScope} (hs : scope.passes rowsOf r') :
    (RScope.joined e.memberValue e.partner e.value scope e.nulls).passes rowsOf r := by
  simp only [RScope.passes, Bool.and_eq_true, Bool.or_eq_true, List.any_eq_true]
  rcases hholds with ⟨x, hx, hrow⟩ | ⟨hn, _, _, hx, hnone⟩ | ⟨_, _, _, hnone⟩
  · rw [hnested] at hrow
    simp only [Bool.false_eq_true, ↓reduceIte] at hrow
    obtain ⟨r'', hr'', hv⟩ := hrow
    rw [hp] at hr''
    cases hr''
    rw [hm, PValue.on_some] at hx
    refine ⟨Or.inl (by simp [hx]), r', hr', ?_, hs⟩
    simp [hv, hx]
  · rw [hm, PValue.on_some] at hx
    refine ⟨Or.inr hn, r', hr', ?_, hs⟩
    simp [hnone r' hp, hx]
  · rw [hp] at hnone
    cases hnone

end SqliveNarrowing.Limit
