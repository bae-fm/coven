import SqliveNarrowing.Reader

/-! Rule 6: a statement that is one SELECT ordered by columns of one of its
tables, each a result column, with a LIMIT and no OFFSET, depends only on that
table's rows up to its last row when a run returns as many rows as its LIMIT
(`scope_order.rs`). The table isn't a LEFT JOIN's right side, and the SELECT
neither groups, aggregates nor calls a window function, whose value in a row
the rows after the last can change. -/
namespace SqliveNarrowing.Limit

structure SortedLimit where
  /-- The item whose rows the order sorts. -/
  item : Nat
  /-- Each ORDER BY term: the column, whether descending, and the result
  column that returns it. -/
  terms : List (Nat × Bool × Nat)
  limit : Nat
  deriving Repr

/-- The order and LIMIT of the root query, when a full run's last row bounds
its sorted item's scope (`sorted_limit`); `st` is the state after the
statement's conditions are read. -/
def sortedLimit (s : Statement) (st : PState) : Option SortedLimit := do
  let query := s.query s.root
  let [k] := query.arms | none
  let n ← query.limit
  if query.orderBy.isEmpty ∨ !query.orderNamed ∨ n = 0 ∨ query.offset.isSome then none
  let c := s.core k
  if c.aggregated || c.windowed then none
  let terms ← query.orderBy.mapM fun (p, desc) => do
    let .expr (.col i col) ← c.output[p]? | none
    let position ← c.output.findIdx? fun
      | .expr (.col j d) => j == i && d == col
      | _ => false
    pure (i, col, desc, position)
  let (i, _, _, _) ← terms.head?
  if !terms.all (·.1 = i) then none
  if (s.item i).core ≠ k ∨ (s.table? i).isNone ∨ (st.items i).leftOn.isSome then none
  if !terms.all (fun (_, col, _, _) => (s.affinity i col).isSome) then none
  pure { item := i, terms := terms.map fun (_, col, desc, position) => (col, desc, position), limit := n }

/-- The rows whose column `c` sorts strictly before `v`: NULL first
ascending and last descending. -/
def beforeScope (c : Nat) (desc : Bool) : Value → RScope
  | none => if desc then .null c true else .inVals c []
  | some x => if desc then .compare c .gt x else (RScope.null c false).either (.compare c .lt x)

/-- The rows whose column `c` holds `v`, NULL included. -/
def equalScope (c : Nat) : Value → RScope
  | none => .null c false
  | some x => .compare c .eq x

/-- The rows that sort at or before `last`, the ORDER BY's values of a run's
last row: term by term, NULL first ascending and last descending; a row equal
to `last` passes (`sorted_at_or_before`). -/
def atOrBefore (terms : List (Nat × Bool)) (last : List Value) : RScope :=
  (terms.zip last).foldr (fun ((c, desc), v) scope =>
    (beforeScope c desc v).either ((equalScope c v).both scope)) .all

/-- The last row's ORDER BY values, when the run returned as many rows as the
LIMIT. -/
def lastValues (sorted : SortedLimit) (rows : List Row) : Option (List Value) :=
  if rows.length = sorted.limit then
    rows.getLast?.map fun r => sorted.terms.map fun (_, _, p) => r.cell p
  else none

/-! The proof. An ORDER BY orders rows by a total preorder, and the model's
sort is stable, so sorting the rows that pass a test gives the sorted rows
that pass it. When a run returned LIMIT rows, every row it returned sorts at
or before its last row, so the rows that sort after the last row change
nothing in the first LIMIT rows. -/

/-- How an ORDER BY term compares two values, descending when `d`. -/
def dcmp (d : Bool) (a b : Value) : Ordering :=
  if d then (valueCompare a b).swap else valueCompare a b

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

theorem dcmp_swap (d : Bool) (a b : Value) : dcmp d b a = (dcmp d a b).swap := by
  cases d <;> simp [dcmp, valueCompare_swap a b, Ordering.swap_swap]

theorem dcmp_eq {d : Bool} {a b : Value} : dcmp d a b = .eq ↔ a = b := by
  cases d <;> simp [dcmp, valueCompare_eq, Ordering.swap_eq_eq]

theorem dcmp_lt_trans {d : Bool} {a b c : Value} (h1 : dcmp d a b = .lt) (h2 : dcmp d b c = .lt) :
    dcmp d a c = .lt := by
  cases d
  · exact valueCompare_lt_trans h1 h2
  · simp only [dcmp, ↓reduceIte, Ordering.swap_eq_lt] at h1 h2 ⊢
    have h1' : valueCompare b a = .lt := by rw [valueCompare_swap, h1]; rfl
    have h2' : valueCompare c b = .lt := by rw [valueCompare_swap, h2]; rfl
    have := valueCompare_lt_trans h2' h1'
    rw [valueCompare_swap, this]; rfl

theorem keyCompare_cons (d : Bool) (ds : List Bool) (a b : Value) (as bs : List Value) :
    keyCompare (d :: ds) (a :: as) (b :: bs) = (dcmp d a b).then (keyCompare ds as bs) := by
  simp [keyCompare, dcmp]

theorem then_ne_gt {x y : Ordering} : x.then y ≠ .gt ↔ x = .lt ∨ (x = .eq ∧ y ≠ .gt) := by
  cases x <;> simp [Ordering.then]

theorem keyCompare_swap (ds : List Bool) (as bs : List Value) :
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

theorem keyLe_cons (d : Bool) (ds : List Bool) (a v : Value) (as vs : List Value) :
    keyLe (d :: ds) (a :: as) (v :: vs) =
      (dcmp d a v == .lt || (dcmp d a v == .eq && keyLe ds as vs)) := by
  simp only [keyLe, keyCompare_cons]
  cases dcmp d a v <;> cases keyCompare ds as vs <;> rfl

theorem keyLe_total (ds : List Bool) (as bs : List Value) : keyLe ds as bs || keyLe ds bs as := by
  simp only [keyLe, keyCompare_swap ds as bs, Bool.or_eq_true, bne_iff_ne, ne_eq]
  cases keyCompare ds as bs <;> simp [Ordering.swap]

theorem keyLe_trans {ds : List Bool} {as bs cs : List Value} (ha : as.length = ds.length)
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
    · rw [dcmp_eq.mp h2] at h1; exact Or.inl h1
    · rw [dcmp_eq.mp h1]; exact Or.inl h2
    · rw [dcmp_eq.mp h1]
      exact Or.inr ⟨h2, ih ha hb hc r1 r2⟩

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

theorem before_passes (rowsOf : Nat → List Row) (r : Row) (c : Nat) (d : Bool) (v : Value) :
    (beforeScope c d v).passes rowsOf r = (dcmp d (r.cell c) v == .lt) := by
  cases d <;> cases v with
  | none => cases hx : r.cell c <;> simp [beforeScope, RScope.passes, hx, dcmp, valueCompare,
      Ordering.swap]
  | some y =>
    cases hx : r.cell c with
    | none => simp [beforeScope, RScope.passes, hx, dcmp, valueCompare, Ordering.swap, passes_either]
    | some x =>
      simp only [beforeScope, dcmp, Bool.false_eq_true, ↓reduceIte, valueCompare_some, passes_either,
        RScope.passes, hx, Op.compare, Option.isNone_some, bne_self_eq_false, Bool.false_or]
      by_cases h1 : x < y <;> by_cases h2 : x = y <;> simp [h1, h2, Ordering.swap] <;> omega

theorem equal_passes (rowsOf : Nat → List Row) (r : Row) (c : Nat) (d : Bool) (v : Value) :
    (equalScope c v).passes rowsOf r = (dcmp d (r.cell c) v == .eq) := by
  have key : (dcmp d (r.cell c) v == .eq) = decide (r.cell c = v) := by
    by_cases h : r.cell c = v
    · rw [h, dcmp_eq.mpr rfl]; simp
    · have : dcmp d (r.cell c) v ≠ .eq := fun h' => h (dcmp_eq.mp h')
      simp only [h, decide_false]
      cases hd : dcmp d (r.cell c) v <;> simp_all
  rw [key]
  cases v with
  | none => cases hx : r.cell c <;> simp [equalScope, RScope.passes, hx]
  | some y =>
    cases hx : r.cell c <;> simp [equalScope, RScope.passes, hx, Op.compare]
    exact Bool.eq_iff_iff.mpr (by simp)

/-- The bound's scope passes a row when the row's values in the ORDER BY's
columns sort at or before `last`. -/
theorem atOrBefore_passes (rowsOf : Nat → List Row) (r : Row) (terms : List (Nat × Bool))
    (last : List Value) (hlen : last.length = terms.length) :
    (atOrBefore terms last).passes rowsOf r =
      keyLe (terms.map (·.2)) (terms.map fun (c, _) => r.cell c) last := by
  induction terms generalizing last with
  | nil => simp [atOrBefore, RScope.passes, keyLe, keyCompare]
  | cons term terms ih =>
    cases last with
    | nil => simp at hlen
    | cons v vs =>
      simp only [List.length_cons, Nat.add_right_cancel_iff] at hlen
      obtain ⟨c, d⟩ := term
      have hrest := ih vs hlen
      simp only [atOrBefore, List.zip_cons_cons, List.foldr_cons] at hrest ⊢
      rw [passes_either, passes_both, hrest, List.map_cons, List.map_cons, keyLe_cons,
        before_passes, equal_passes (d := d)]

end SqliveNarrowing.Limit
