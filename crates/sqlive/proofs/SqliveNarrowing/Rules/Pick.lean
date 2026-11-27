import SqliveNarrowing.Rules.Limit
import SqliveNarrowing.Rules.TellsApart

/-! Rule 7: the rows SQLite picks. A query that keeps some of its rows, by a
LIMIT or an OFFSET or as a subquery whose first row a clause takes, returns
the rows it reads first, unless its ORDER BY tells every two of them apart.
SQLite reads rows in the order of the plan it picks, which can follow an
index on a column no clause names, so any of the rows can come first
(`post_picks_any`). Such a query depends on every column of each table it
reads (`Statement.picks`, `Statement.reads`, `picked_column_reruns`); its
conditions still narrow which rows it depends on. A query whose ORDER BY,
with the equalities of its conditions, tells its bindings apart
(`Statement.tellsApart`, `tellsApart_separates`), or that aggregates with no
GROUP BY, returns the same rows in every order SQLite reads them in
(`fullyOrders_sound`), so it depends only on the columns its clauses name
(`unreported_columns.rs`).

A core that aggregates picks rows too: a result column outside every GROUP BY
term takes its value from one binding of its group, which the order SQLite
reads them in decides (`bare_picks_any`), and `group_concat` joins its values in the order SQLite
reads them (`concat_follows_order`), unless the call's own ORDER BY tells the
core's bindings apart, and `max` keeps the value it reads
first among values that compare equal (`max_keeps_first`), unless its
argument is a column whose equal values are the same, and a GROUP BY term
keeps the value it reads first among values that compare equal
(`distinct_keeps_first`), so a result column that shows one follows that
order, unless the term is such a column. DISTINCT, UNION, INTERSECT and
EXCEPT keep one of each set of rows that compare equal in the same way, so a
DISTINCT core that returns anything but such columns follows the order SQLite
reads its rows in (`Statement.distinctFollowsOrder`), and so does a compound
whose arms do (`Statement.compoundFollowsOrder`). A window numbers the
bindings its ORDER BY doesn't tell apart in that order too
(`rowNumber_follows_order`), unless the ORDER BY tells the core's bindings
apart. Such a core depends on every column of each table it
reads (`Statement.followsOrder`); any other core gives the same rows in every
order (`orderFree_rows_perm`). -/
namespace SqliveNarrowing.Pick
open Limit

/-! Without an ORDER BY that tells the rows apart, any row can come first. -/

/-- A query with no ORDER BY that skips `m` rows and keeps at least one
returns, for some order of its rows, any row it holds, when it holds more
than `m`. -/
theorem post_picks_any {q : Query} (horder : q.orderBy = [])
    (hlimit : ∀ n, q.limit = some n → 0 < n) {rows : List Row} {x : Row} (hx : x ∈ rows)
    (hoffset : q.offset.getD 0 < rows.length) :
    ∃ rows', rows'.Perm rows ∧ x ∈ q.post rows' := by
  let m := q.offset.getD 0
  let l := rows.erase x
  have hl : l.length = rows.length - 1 := List.length_erase_of_mem hx
  have htake : (l.take m).length = m := by
    rw [List.length_take]; omega
  refine ⟨l.take m ++ x :: l.drop m, ?_, ?_⟩
  · refine List.perm_middle.trans ?_
    rw [List.take_append_drop]
    exact (List.perm_cons_erase hx).symm
  · have hdrop : (l.take m ++ x :: l.drop m).drop m = x :: l.drop m := by
      rw [List.drop_append_of_le_length (by omega), List.drop_eq_nil_of_le (by omega)]
      rfl
    unfold Query.post
    simp only [horder, List.isEmpty_nil, ↓reduceIte]
    change x ∈ (match q.limit with
      | some n => ((l.take m ++ x :: l.drop m).drop m).take n
      | none => (l.take m ++ x :: l.drop m).drop m)
    rw [hdrop]
    cases h : q.limit with
    | none => simp
    | some n =>
      obtain ⟨n', rfl⟩ : ∃ n', n = n' + 1 := ⟨n - 1, by have := hlimit n h; omega⟩
      simp

/-! An ORDER BY that tells every two rows apart sorts them the same in every
order. -/

section Sorting

variable {α : Type} {le : α → α → Bool}

theorem perm_insertSorted (x : α) (l : List α) : (insertSorted le x l).Perm (x :: l) := by
  induction l with
  | nil => exact List.Perm.refl _
  | cons y ys ih =>
    unfold insertSorted
    split
    · exact List.Perm.refl _
    · exact (ih.cons y).trans (List.Perm.swap x y ys)

theorem perm_sortBy (l : List α) : (sortBy le l).Perm l := by
  induction l with
  | nil => exact List.Perm.refl _
  | cons x xs ih => exact (perm_insertSorted x _).trans (ih.cons x)

/-- Two sorted lists of the same elements are equal when no two different
elements sort at or before each other. -/
theorem eq_of_perm_of_sorted {l₁ l₂ : List α} (hperm : l₁.Perm l₂)
    (h₁ : l₁.Pairwise (le · ·)) (h₂ : l₂.Pairwise (le · ·))
    (anti : ∀ a ∈ l₁, ∀ b ∈ l₁, le a b → le b a → a = b) : l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil => exact (List.nil_perm.mp hperm).symm
  | cons x xs ih =>
    cases l₂ with
    | nil => exact absurd hperm (fun h => by simpa using h.length_eq)
    | cons y ys =>
      rw [List.pairwise_cons] at h₁ h₂
      have hxy : x = y := by
        have hx : x ∈ y :: ys := hperm.mem_iff.mp (by simp)
        have hy : y ∈ x :: xs := hperm.mem_iff.mpr (by simp)
        simp only [List.mem_cons] at hx hy
        rcases hx with hx | hx
        · exact hx
        · rcases hy with hy | hy
          · exact hy.symm
          · exact anti x (by simp) y (by simp [hy]) (h₁.1 y hy) (h₂.1 x hx)
      subst hxy
      have htail := ih ((List.perm_cons x).mp hperm) h₁.2 h₂.2
        (fun a ha b hb => anti a (by simp [ha]) b (by simp [hb]))
      rw [htail]

end Sorting

/-- Two keys that tie hold the same value in each term whose collation ties no
two different values. -/
theorem keyCompare_eq_exact {α : Type} (os : List (α × Dir)) (f g : α → Value)
    (h : keyCompare (os.map (·.2)) (os.map fun o => f o.1) (os.map fun o => g o.1) = .eq) :
    ∀ o ∈ os, o.2.exact = true → f o.1 = g o.1 := by
  induction os with
  | nil => simp
  | cons o os ih =>
    simp only [List.map_cons, keyCompare_cons, Ordering.then_eq_eq] at h
    intro o' ho' hex
    rcases List.mem_cons.mp ho' with rfl | ho'
    · have h1 := dcmp_eq.mp h.1
      exact Option.map_injective (o'.2.exact_inj hex) h1
    · exact ih h.2 o' ho' hex

theorem keyCompare_self (ds : List Dir) (as : List Value) : keyCompare ds as as = .eq := by
  induction ds generalizing as with
  | nil => cases as <;> rfl
  | cons d ds ih =>
    cases as with
    | nil => rfl
    | cons a as => rw [keyCompare_cons, dcmp_eq.mpr rfl, ih]; rfl

/-- Two keys that each sort at or before the other tie. -/
theorem keyLe_antisymm {ds : List Dir} {as bs : List Value}
    (h1 : keyLe ds as bs) (h2 : keyLe ds bs as) : keyCompare ds as bs = .eq := by
  simp only [keyLe, bne_iff_ne, ne_eq] at h1 h2
  rw [keyCompare_swap] at h2
  cases h : keyCompare ds as bs <;> simp_all [Ordering.swap]

/-- A query whose ORDER BY tells every two of its rows apart returns the same
rows whatever order it reads them in. -/
theorem post_perm {q : Query} {rows rows' : List Row} (hperm : rows'.Perm rows)
    (hsep : ∀ a ∈ rows, ∀ b ∈ rows,
      keyCompare (q.orderBy.map (·.2)) (orderKey q a) (orderKey q b) = .eq → a = b) :
    q.post rows' = q.post rows := by
  have hlen_key : ∀ r, (orderKey q r).length = (q.orderBy.map (·.2)).length := by
    intro r; simp [orderKey]
  have hsorted : (if q.orderBy.isEmpty then rows' else
        sortBy (fun a b => keyLe (q.orderBy.map (·.2)) (orderKey q a) (orderKey q b)) rows') =
      (if q.orderBy.isEmpty then rows else
        sortBy (fun a b => keyLe (q.orderBy.map (·.2)) (orderKey q a) (orderKey q b)) rows) := by
    split
    · rename_i hempty
      have hnil : q.orderBy = [] := List.isEmpty_iff.mp hempty
      have hkey : ∀ r, orderKey q r = [] := by intro r; simp [orderKey, hnil]
      apply eq_of_perm_of_sorted (le := fun _ _ => true) hperm
      · exact List.pairwise_of_forall (by simp)
      · exact List.pairwise_of_forall (by simp)
      · intro a ha b hb _ _
        exact hsep a (hperm.mem_iff.mp ha) b (hperm.mem_iff.mp hb) (by rw [hnil]; rfl)
    · let le := fun a b => keyLe (q.orderBy.map (·.2)) (orderKey q a) (orderKey q b)
      have total : ∀ a b, le a b || le b a := fun a b => keyLe_total _ _ _
      have trans : ∀ a b c, le a b → le b c → le a c := fun a b c h1 h2 =>
        keyLe_trans (hlen_key a) (hlen_key b) (hlen_key c) h1 h2
      apply eq_of_perm_of_sorted (le := le)
      · exact ((perm_sortBy rows').trans hperm).trans (perm_sortBy rows).symm
      · exact pairwise_sortBy total trans rows'
      · exact pairwise_sortBy total trans rows
      · intro a ha b hb h1 h2
        have ha' := hperm.mem_iff.mp ((perm_sortBy rows').mem_iff.mp ha)
        have hb' := hperm.mem_iff.mp ((perm_sortBy rows').mem_iff.mp hb)
        exact hsep a ha' b hb' (keyLe_antisymm h1 h2)
  unfold Query.post
  simp only
  rw [show (fun a b => keyLe (q.orderBy.map (·.2)) (q.orderBy.map fun (p, _) => a.cell p)
      (q.orderBy.map fun (p, _) => b.cell p)) =
      (fun a b => keyLe (q.orderBy.map (·.2)) (orderKey q a) (orderKey q b)) from rfl, hsorted]

/-- Two rows of a query that returns one row, or whose ORDER BY tells its
bindings apart, on a database that keeps each row's rowid in its first column
and holds its declared constraints, differ in their sort key. -/
theorem fullyOrders_separates {s : Statement} (hwf : s.wellFormed = true) {db : Db}
    (hdb : db.Keyed) (hcons : db.Constrained s) {q k g : Nat}
    (hfull : s.fullyOrders q = true) (harms : (s.query q).arms = [k]) {outer : Bind} :
    ∀ a ∈ (s.core k).rows (evalQuery s db g) (coreBindings s db (evalQuery s db g) outer k),
    ∀ b ∈ (s.core k).rows (evalQuery s db g) (coreBindings s db (evalQuery s db g) outer k),
      keyCompare ((s.query q).orderBy.map (·.2)) (orderKey (s.query q) a)
        (orderKey (s.query q) b) = .eq → a = b := by
  have hk : k ∈ (s.query q).arms := by rw [harms]; exact List.mem_singleton_self _
  unfold Statement.fullyOrders at hfull
  rw [harms] at hfull
  simp only [Bool.or_eq_true, Bool.and_eq_true, Bool.not_eq_true'] at hfull
  rcases hfull with ⟨hagg, hgroup⟩ | ⟨⟨⟨hagg, hwin⟩, _⟩, htells⟩
  · intro a ha b hb _
    exact (one_row hagg hgroup ha).trans (one_row hagg hgroup hb).symm
  intro a ha b hb hkey
  obtain ⟨ba, hba, ja, rfl⟩ := mem_rows_plain hagg ha
  obtain ⟨bb, hbb, jb, rfl⟩ := mem_rows_plain hagg hb
  let eval := evalQuery s db g
  have hex := keyCompare_eq_exact (s.query q).orderBy
    (fun p => Row.cell ((s.core k).output.map
      (OutCol.plain eval (coreBindings s db eval outer k) ba ja)) p)
    (fun p => Row.cell ((s.core k).output.map
      (OutCol.plain eval (coreBindings s db eval outer k) bb jb)) p) hkey
  have hsame : ba = bb := by
    apply tellsApart_separates hwf hdb hcons hk htells hba hbb (sa := eval ba) (sb := eval bb)
    intro e he
    simp only [Statement.orderExprs, List.mem_filterMap] at he
    obtain ⟨⟨p, d⟩, hpd, hp⟩ := he
    have hcell := hex (p, d) hpd
    cases hd : d.exact with
    | false => simp [hd] at hp
    | true =>
      cases hout : (s.core k).output[p]? with
      | none => simp [hd, hout] at hp
      | some o =>
        cases o <;> simp [hd, hout] at hp
        subst hp
        have hc := hcell hd
        simpa [Row.cell, List.getElem?_map, hout, OutCol.plain] using hc
  rw [hsame]
  exact plain_any_place hwin _ _ _ _ _

/-- Rule 7's theorem for a query that tells its rows apart: a one-arm query
that aggregates with no GROUP BY, or whose ORDER BY tells its bindings apart,
returns the same rows from any order of its core's rows, as SQLite's plan
reads them, on a database that keeps each row's rowid in its first column and
holds its declared constraints. -/
theorem fullyOrders_sound {s : Statement} (hwf : s.wellFormed = true) {db : Db}
    (hdb : db.Keyed) (hcons : db.Constrained s) {q k g : Nat}
    (hfull : s.fullyOrders q = true) (harms : (s.query q).arms = [k])
    {outer : Bind} {rows' : List Row}
    (hperm : rows'.Perm ((s.core k).result (evalQuery s db g)
      (coreBindings s db (evalQuery s db g) outer k))) :
    (s.query q).post rows' =
      (s.query q).post ((s.core k).result (evalQuery s db g)
        (coreBindings s db (evalQuery s db g) outer k)) :=
  post_perm hperm fun a ha b hb =>
    fullyOrders_separates hwf hdb hcons hfull harms a (mem_result ha) b (mem_result hb)

/-! A core that aggregates takes a plain result column's value from one
binding of each group, which the order SQLite reads them in decides, and joins `group_concat`'s values in the
order SQLite reads them, unless the call's own ORDER BY tells the bindings
apart. That order follows the plan SQLite picks, so such a core depends on
every column of each table it reads (`Statement.followsOrder`). A core whose
rows don't follow that order gives the same rows from every order of its
bindings (`orderFree_rows_perm`). -/

/-- For any binding a core that aggregates keeps, some order of its bindings
gives a row that holds a plain result column's value in that binding. A
column outside every GROUP BY term (`Expr.bare`) can hold a different value
in each binding of a group, so the order SQLite reads them in decides which
the row shows. -/
theorem bare_picks_any {c : Core} (hagg : c.aggregated = true) {subs : Bind → Subs}
    {kept : List Bind} {b : Bind} (hb : b ∈ kept) {p : Nat} {e : Expr}
    (hp : c.output[p]? = some (.expr e)) :
    ∃ kept', kept'.Perm kept ∧ ∃ r ∈ c.rows subs kept', r[p]? = some (e.eval b (subs b)) := by
  obtain ⟨l₁, l₂, rfl⟩ := List.append_of_mem hb
  refine ⟨l₁ ++ l₂ ++ [b], (List.perm_append_singleton b _).trans List.perm_middle.symm, ?_⟩
  let key := fun b' => c.groupBy.map (Expr.eval b' (subs b'))
  have hcell : ∀ g : List Bind, g.getLast? = some b →
      (c.output.map (OutCol.aggregate subs g))[p]? = some (e.eval b (subs b)) := by
    intro g hlast
    simp [List.getElem?_map, hp, OutCol.aggregate, hlast]
  unfold Core.rows
  simp only [hagg, ↓reduceIte]
  split
  · exact ⟨_, List.mem_singleton_self _, hcell _ List.getLast?_concat⟩
  · refine ⟨_, List.mem_map.mpr ⟨(l₁ ++ l₂ ++ [b]).filter fun b' => key b' == key b, ?_, rfl⟩,
      hcell _ (by simp [List.filter_append])⟩
    simp only [groupsOf, List.mem_map]
    exact ⟨key b, List.mem_eraseDups.mpr (List.mem_map.mpr ⟨b, by simp, rfl⟩), rfl⟩

/-- A `group_concat` with no ORDER BY of its own joins its values in the
order SQLite reads the group's bindings in: two bindings whose values are 1
and 2 show 12 read one way and 21 the other. -/
theorem concat_follows_order {c : Core} (hagg : c.aggregated = true) (hgroup : c.groupBy = [])
    {p : Nat} {e : Expr} (hp : c.output[p]? = some (.concat e [])) {subs : Bind → Subs}
    {b₁ b₂ : Bind} (h₁ : e.eval b₁ (subs b₁) = some 1) (h₂ : e.eval b₂ (subs b₂) = some 2) :
    (c.rows subs [b₁, b₂]).map (·[p]?) = [some (some 12)] ∧
    (c.rows subs [b₂, b₁]).map (·[p]?) = [some (some 21)] := by
  have h12 : concatValue [some 1, some 2] = some 12 := by decide
  have h21 : concatValue [some 2, some 1] = some 21 := by decide
  simp [Core.rows, hagg, hgroup, List.getElem?_map, hp, OutCol.aggregate, sortBindings, h₁, h₂,
    h12, h21]

/-- SQLite's `max` over values that `cmp` orders: the first value it reads
among the greatest, as it replaces the value it keeps only by a greater one
(`minmaxStep`). -/
def keepMax {α : Type} (cmp : α → α → Ordering) (vs : List α) : Option α :=
  vs.foldl (fun acc v => match acc with
    | none => some v
    | some best => if cmp best v = .lt then some v else some best) none

/-- Among values that compare equal, `max` keeps the one it reads first: two
values that compare equal and differ, as `'Abc'` and `'ABC'` under NOCASE or
`1` and `1.0`, give `max` a different value read one way than the other. The
model's values are integers, which compare equal only when they are the same
(`maxValue_perm`), so a core whose `max` reads a value that can compare equal
to another and differ follows the order SQLite reads its bindings in
(`Statement.followsOrder`). -/
theorem max_keeps_first {α : Type} {cmp : α → α → Ordering} {a b : α} (hab : cmp a b = .eq)
    (hba : cmp b a = .eq) : keepMax cmp [a, b] = some a ∧ keepMax cmp [b, a] = some b := by
  simp [keepMax, hab, hba]

/-- SQLite's GROUP BY and DISTINCT over values that `cmp` orders: one value of
each set of values that compare equal, the one it reads first, in the order it
reads them. UNION, INTERSECT and EXCEPT keep one row of each set of rows that
compare equal too: the first they read, or the last, which is the first in
the reverse order, as their plan decides. -/
def keepDistinct {α : Type} (cmp : α → α → Ordering) (vs : List α) : List α :=
  vs.foldl (fun kept v => if kept.any (cmp · v == .eq) then kept else kept ++ [v]) []

/-- Among values that compare equal, a GROUP BY term keeps the one it reads
first: two values that compare equal and differ, as `'Abc'` and `'ABC'` under
NOCASE or `1` and `1.0`, give the group a different value read one way than
the other. The model's values are integers, which compare equal only when
they are the same, so a core that shows a GROUP BY term that can hold values
that compare equal and differ follows the order SQLite reads its bindings in
(`Statement.followsOrder`), and so do a DISTINCT core that returns such
values (`Statement.distinctFollowsOrder`) and a compound that combines them
(`Statement.compoundFollowsOrder`). -/
theorem distinct_keeps_first {α : Type} {cmp : α → α → Ordering} {a b : α}
    (hab : cmp a b = .eq) (hba : cmp b a = .eq) :
    keepDistinct cmp [a, b] = [a] ∧ keepDistinct cmp [b, a] = [b] := by
  simp [keepDistinct, hab, hba]

/-- An expression that names no column or subquery outside its core's GROUP
BY terms takes the same value in two bindings that agree on those terms. -/
theorem Expr.eval_of_not_bare {groupBy : List Expr} {b b' : Bind} {sub sub' : Subs}
    (hkey : groupBy.map (Expr.eval b sub) = groupBy.map (Expr.eval b' sub')) (e : Expr)
    (h : e.bare groupBy = false) : e.eval b sub = e.eval b' sub' := by
  by_cases hg : e ∈ groupBy
  · exact List.map_inj_left.mp hkey e hg
  have hc : groupBy.contains e = false := by simpa using hg
  induction e with
  | col i c => simp [Expr.bare] at h; exact absurd h hg
  | sub q => simp [Expr.bare] at h; exact absurd h hg
  | lit _ => rfl
  | add l r ihl ihr | greatest l r ihl ihr | coalesce l r ihl ihr =>
    simp only [Expr.bare, hc, Bool.not_false, Bool.true_and, Bool.or_eq_false_iff] at h
    have hl : l.eval b sub = l.eval b' sub' := by
      by_cases hgl : l ∈ groupBy
      · exact List.map_inj_left.mp hkey l hgl
      · exact ihl h.1 hgl (by simpa using hgl)
    have hr : r.eval b sub = r.eval b' sub' := by
      by_cases hgr : r ∈ groupBy
      · exact List.map_inj_left.mp hkey r hgr
      · exact ihr h.2 hgr (by simpa using hgr)
    simp only [Expr.eval, hl, hr]
  | iif t y n iht ihy ihn =>
    simp only [Expr.bare, hc, Bool.not_false, Bool.true_and, Bool.or_eq_false_iff] at h
    have ht : t.eval b sub = t.eval b' sub' := by
      by_cases hgt : t ∈ groupBy
      · exact List.map_inj_left.mp hkey t hgt
      · exact iht h.1.1 hgt (by simpa using hgt)
    have hy : y.eval b sub = y.eval b' sub' := by
      by_cases hgy : y ∈ groupBy
      · exact List.map_inj_left.mp hkey y hgy
      · exact ihy h.1.2 hgy (by simpa using hgy)
    have hn : n.eval b sub = n.eval b' sub' := by
      by_cases hgn : n ∈ groupBy
      · exact List.map_inj_left.mp hkey n hgn
      · exact ihn h.2 hgn (by simpa using hgn)
    simp only [Expr.eval, ht, hy, hn]
  | isNull e ih =>
    simp only [Expr.bare, hc, Bool.not_false, Bool.true_and] at h
    have he : e.eval b sub = e.eval b' sub' := by
      by_cases hge : e ∈ groupBy
      · exact List.map_inj_left.mp hkey e hge
      · exact ih h hge (by simpa using hge)
    simp only [Expr.eval, he]

/-- The largest value of a list is the same in every order. -/
theorem maxValue_perm {vs vs' : List Value} (h : vs'.Perm vs) : maxValue vs' = maxValue vs := by
  unfold maxValue
  apply h.foldl_eq'
  intro x _ y _ z
  cases x <;> cases y <;> cases z <;> simp only [Option.some.injEq] <;> omega

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ (l : List α), l.eraseDups.Nodup
  | [] => List.nodup_nil
  | a :: as => by
    rw [List.eraseDups_cons]
    have := nodup_eraseDups (as.filter fun b => !b == a)
    refine List.nodup_cons.mpr ⟨fun hm => ?_, this⟩
    have := (List.mem_filter.mp (List.mem_eraseDups.mp hm)).2
    simp at this
termination_by l => l.length
decreasing_by exact Nat.lt_succ_of_le (List.length_filter_le _ _)

/-- The groups of a permutation of bindings, each made into a row by a map
that gives the same row for groups that are permutations of each other, are
a permutation of the groups' rows. -/
theorem groupsOf_perm {key : Bind → List Value} {bs bs' : List Bind} (hperm : bs'.Perm bs)
    {row : List Bind → Row}
    (hrow : ∀ v, row (bs'.filter fun b => key b == v) = row (bs.filter fun b => key b == v)) :
    ((groupsOf key bs').map row).Perm ((groupsOf key bs).map row) := by
  unfold groupsOf
  simp only [List.map_map, Function.comp_def, hrow]
  apply List.Perm.map
  apply (List.perm_ext_iff_of_nodup (nodup_eraseDups _) (nodup_eraseDups _)).mpr
  intro v
  simp only [List.mem_eraseDups, List.mem_map]
  exact ⟨fun ⟨b, hb, hv⟩ => ⟨b, hperm.mem_iff.mp hb, hv⟩,
    fun ⟨b, hb, hv⟩ => ⟨b, hperm.mem_iff.mpr hb, hv⟩⟩

/-- A window whose ORDER BY doesn't tell the bindings apart numbers them in
the order SQLite reads them in: of two bindings whose values of `e` are 1
and 2, the one read first is numbered 1, so the rows pair each value with
another number read one way than the other. -/
theorem rowNumber_follows_order {c : Core} (hagg : c.aggregated = false) {p q : Nat} {e : Expr}
    (hp : c.output[p]? = some (.rowNumber [] [])) (hq : c.output[q]? = some (.expr e))
    {subs : Bind → Subs} {b₁ b₂ : Bind} (h₁ : e.eval b₁ (subs b₁) = some 1)
    (h₂ : e.eval b₂ (subs b₂) = some 2) :
    (c.rows subs [b₁, b₂]).map (fun r => (r[q]?, r[p]?)) =
      [(some (some 1), some (some 1)), (some (some 2), some (some 2))] ∧
    (c.rows subs [b₂, b₁]).map (fun r => (r[q]?, r[p]?)) =
      [(some (some 2), some (some 1)), (some (some 1), some (some 2))] := by
  simp [Core.rows, hagg, List.getElem?_map, hp, hq, OutCol.plain, windowRowNumber, windowPeer,
    windowKey, keyCompare, h₁, h₂]

section Occurrences

open Classical

variable {α : Type}

/-- Each element of `l` with how many elements equal to it come before it. -/
noncomputable def occurrences (l : List α) : List (α × Nat) :=
  l.zipIdx.map fun (b, j) => (b, (l.take j).countP fun b' => decide (b' = b))

theorem occurrences_cons (x : α) (l : List α) :
    occurrences (x :: l) =
      (x, 0) :: (occurrences l).map fun (y, n) => (y, n + if x = y then 1 else 0) := by
  unfold occurrences
  rw [List.zipIdx_cons, List.zipIdx_succ]
  simp [List.map_map, List.countP_cons]

/-- Two lists of the same elements, in any order, give the same elements
with their occurrence counts, in some order. -/
theorem occurrences_perm {l₁ l₂ : List α} (h : l₁.Perm l₂) :
    (occurrences l₁).Perm (occurrences l₂) := by
  induction h with
  | nil => exact List.Perm.refl _
  | cons x _ ih =>
    rw [occurrences_cons, occurrences_cons]
    exact (ih.map _).cons _
  | swap x y l =>
    rw [occurrences_cons, occurrences_cons, occurrences_cons, occurrences_cons]
    simp only [List.map_cons, List.map_map]
    by_cases hxy : x = y
    · subst hxy; exact List.Perm.refl _
    · have hyx : ¬y = x := fun h => hxy h.symm
      have hcomm : ((fun (p : α × Nat) => (p.1, p.2 + if y = p.1 then 1 else 0)) ∘
            fun (p : α × Nat) => (p.1, p.2 + if x = p.1 then 1 else 0)) =
          ((fun (p : α × Nat) => (p.1, p.2 + if x = p.1 then 1 else 0)) ∘
            fun (p : α × Nat) => (p.1, p.2 + if y = p.1 then 1 else 0)) := by
        funext ⟨z, n⟩
        simp only [Function.comp_apply, Prod.mk.injEq, true_and]
        omega
      simp only [hxy, hyx, ↓reduceIte, Nat.add_zero]
      rw [hcomm]
      exact List.Perm.swap _ _ _
  | trans _ _ ih₁ ih₂ => exact ih₁.trans ih₂

end Occurrences

/-- Two lists of the same elements, in any order, keep the same elements,
in some order, when each keeps the first of each set of equal ones. -/
theorem perm_eraseDups {α : Type} [BEq α] [LawfulBEq α] {l l' : List α} (h : l'.Perm l) :
    l'.eraseDups.Perm l.eraseDups :=
  (List.perm_ext_iff_of_nodup (nodup_eraseDups _) (nodup_eraseDups _)).mpr fun x => by
    simp only [List.mem_eraseDups, h.mem_iff]

/-- The rows of a core whose rows don't follow the order SQLite reads its
bindings in, from any order of the bindings it keeps, are its rows from the
order the model reads them in, in some order. -/
theorem rows_perm_of_orderFree {s : Statement} (hwf : s.wellFormed = true) {db : Db}
    (hdb : db.Keyed) (hcons : db.Constrained s) {q k g : Nat} (hk : k ∈ (s.query q).arms)
    (hfree : s.followsOrder k = false) {outer : Bind} {kept' : List Bind}
    (hperm : kept'.Perm (coreBindings s db (evalQuery s db g) outer k)) :
    ((s.core k).rows (evalQuery s db g) kept').Perm
      ((s.core k).rows (evalQuery s db g) (coreBindings s db (evalQuery s db g) outer k)) := by
  let eval := evalQuery s db g
  show ((s.core k).rows eval kept').Perm ((s.core k).rows eval (coreBindings s db eval outer k))
  change kept'.Perm (coreBindings s db eval outer k) at hperm
  let c := s.core k
  let kept := coreBindings s db eval outer k
  let key := fun b => c.groupBy.map (Expr.eval b (eval b))
  show (c.rows eval kept').Perm (c.rows eval kept)
  cases hagg : c.aggregated with
  | false =>
    -- Each window sorts by the rowid of each of the core's tables.
    have hsorts : ∀ partition orderBy, OutCol.rowNumber partition orderBy ∈ c.output →
        s.tellsApart k (exactTerms orderBy) = true := by
      intro partition orderBy ho
      have hagg' : (s.core k).aggregated = false := hagg
      unfold Statement.followsOrder at hfree
      rw [List.any_eq_false] at hfree
      simpa [hagg'] using hfree _ ho
    -- So a binding the core keeps ties in a window only with itself.
    have htie : ∀ partition orderBy, OutCol.rowNumber partition orderBy ∈ c.output →
        ∀ b ∈ kept, ∀ b' ∈ kept, windowPeer eval partition orderBy b b' .eq = true ↔ b' = b := by
      intro partition orderBy ho b hb b' hb'
      constructor
      · intro h
        simp only [windowPeer, Bool.and_eq_true, beq_iff_eq] at h
        have h2 : keyCompare (orderBy.map (·.2))
            (orderBy.map fun o => o.1.eval b' (eval b')) (orderBy.map fun o => o.1.eval b (eval b))
            = .eq := by
          simpa [windowKey, List.map_map, Function.comp_def] using h.2
        have hex := keyCompare_eq_exact orderBy (fun e => e.eval b' (eval b'))
          (fun e => e.eval b (eval b)) h2
        exact tellsApart_separates hwf hdb hcons hk (hsorts _ _ ho) hb' hb (sa := eval b')
          (sb := eval b)
          (fun e he => by
            obtain ⟨⟨e', d⟩, hmem, rfl⟩ := List.mem_map.mp he
            obtain ⟨hmem, hd⟩ := List.mem_filter.mp hmem
            exact hex (e', d) hmem hd)
      · rintro rfl
        simp [windowPeer, keyCompare_self]
    -- A row is its binding's values, each window's number counting the
    -- bindings that sort before it in the window and the copies of the
    -- binding read before it.
    let G : Bind × Nat → Row := fun (b, n) => c.output.map fun
      | .expr e => e.eval b (eval b)
      | .rowNumber partition orderBy =>
        some (Int.ofNat (1 + kept.countP (windowPeer eval partition orderBy b · .lt) + n))
      | _ => none
    have hrows : ∀ L : List Bind, L.Perm kept → c.rows eval L = (occurrences L).map G := by
      intro L hL
      unfold Core.rows occurrences
      simp only [hagg, Bool.false_eq_true, ↓reduceIte, List.map_map]
      apply List.map_congr_left
      intro ⟨b, j⟩ hbj
      have hb : b ∈ kept := hL.mem_iff.mp (List.fst_mem_of_mem_zipIdx hbj)
      simp only [Function.comp_apply, G]
      apply List.map_congr_left
      intro o ho
      cases o with
      | expr e => rfl
      | rowNumber partition orderBy =>
        simp only [OutCol.plain, windowRowNumber, hL.countP_eq]
        congr 3
        apply List.countP_congr
        intro b' hb'
        have hb'' : b' ∈ kept := hL.mem_iff.mp (List.mem_of_mem_take hb')
        simp [htie partition orderBy ho b hb b' hb'']
      | _ => rfl
    show (c.rows eval kept').Perm (c.rows eval kept)
    rw [hrows kept' hperm, hrows kept (List.Perm.refl _)]
    exact (occurrences_perm hperm).map G
  | true =>
  have hfree' : ∀ o ∈ c.output, (∀ e, o = .expr e → e.bare c.groupBy = false) ∧
      (∀ e orderBy, o = .concat e orderBy → s.tellsApart k (exactTerms orderBy) = true) := by
    intro o ho
    have hagg' : (s.core k).aggregated = true := hagg
    unfold Statement.followsOrder at hfree
    simp only [hagg', Bool.true_and, List.any_eq_false] at hfree
    have := hfree o ho
    constructor
    · rintro e rfl; simp at this; exact this.1
    · rintro e orderBy rfl; simpa using this
  -- Two permuted groups of the kept bindings, whose bindings agree on the
  -- GROUP BY terms, give the same row.
  have hgroup : ∀ g g' : List Bind, g'.Perm g → (∀ b ∈ g, b ∈ kept) →
      (∀ b ∈ g, ∀ b' ∈ g, key b = key b') →
      c.output.map (OutCol.aggregate eval g') = c.output.map (OutCol.aggregate eval g) := by
    intro g g' hg hin hsame
    apply List.map_congr_left
    intro o ho
    obtain ⟨hbare, hsorts⟩ := hfree' o ho
    cases o with
    | expr e =>
      simp only [OutCol.aggregate]
      cases hl' : g'.getLast? with
      | none =>
        have : g' = [] := List.getLast?_eq_none_iff.mp hl'
        subst this
        rw [List.nil_perm.mp hg]; rfl
      | some x' =>
        cases hl : g.getLast? with
        | none =>
          have : g = [] := List.getLast?_eq_none_iff.mp hl
          subst this
          rw [List.perm_nil.mp hg] at hl'; cases hl'
        | some x =>
          have hx' : x' ∈ g := hg.mem_iff.mp (List.mem_of_getLast? hl')
          have hx : x ∈ g := List.mem_of_getLast? hl
          exact Expr.eval_of_not_bare (hsame x' hx' x hx) e (hbare e rfl)
    | count => simp [OutCol.aggregate, hg.length_eq]
    | max e => simp only [OutCol.aggregate]; exact maxValue_perm (hg.map _)
    | concat e orderBy =>
      have hsep : ∀ a ∈ g', ∀ b ∈ g', keyCompare (orderBy.map (·.2))
          (orderBy.map fun (e, _) => e.eval a (eval a))
          (orderBy.map fun (e, _) => e.eval b (eval b)) = .eq → a = b := by
        intro a ha b hb hkey
        have hex := keyCompare_eq_exact orderBy (fun e => e.eval a (eval a))
          (fun e => e.eval b (eval b)) hkey
        apply tellsApart_separates hwf hdb hcons hk (hsorts e orderBy rfl)
          (hin a (hg.mem_iff.mp ha)) (hin b (hg.mem_iff.mp hb)) (sa := eval a) (sb := eval b)
        intro e' he'
        obtain ⟨⟨e'', d⟩, hed, rfl⟩ := List.mem_map.mp he'
        obtain ⟨hmem, hd⟩ := List.mem_filter.mp hed
        exact hex (e'', d) hmem hd
      have hsort : sortBindings eval orderBy g' = sortBindings eval orderBy g := by
        unfold sortBindings
        split
        · rename_i hempty
          have hnil : orderBy = [] := List.isEmpty_iff.mp hempty
          apply eq_of_perm_of_sorted (le := fun _ _ => true) hg
          · exact List.pairwise_of_forall (by simp)
          · exact List.pairwise_of_forall (by simp)
          · intro a ha b hb _ _
            exact hsep a ha b hb (by simp [hnil, keyCompare])
        · let ds := orderBy.map (·.2)
          let okey := fun (b : Bind) => orderBy.map fun (e, _) => e.eval b (eval b)
          have hlen : ∀ b, (okey b).length = ds.length := by intro b; simp [okey, ds]
          let le := fun a b => keyLe ds (okey a) (okey b)
          have total : ∀ a b, le a b || le b a := fun a b => keyLe_total _ _ _
          have trans : ∀ a b c, le a b → le b c → le a c := fun a b c h1 h2 =>
            keyLe_trans (hlen a) (hlen b) (hlen c) h1 h2
          apply eq_of_perm_of_sorted (le := le)
          · exact ((perm_sortBy g').trans hg).trans (perm_sortBy g).symm
          · exact pairwise_sortBy total trans g'
          · exact pairwise_sortBy total trans g
          · intro a ha b hb h1 h2
            have ha' := (perm_sortBy g').mem_iff.mp ha
            have hb' := (perm_sortBy g').mem_iff.mp hb
            exact hsep a ha' b hb' (keyLe_antisymm h1 h2)
      simp only [OutCol.aggregate, hsort]
    | rowNumber => rfl
  unfold Core.rows
  simp only [hagg, ↓reduceIte]
  split
  · rename_i hempty
    have hnil : c.groupBy = [] := List.isEmpty_iff.mp hempty
    simp only [List.map_cons, List.map_nil]
    rw [hgroup kept kept' hperm (fun b hb => hb) (fun b _ b' _ => by simp [key, hnil])]
  · apply groupsOf_perm hperm
    intro v
    apply hgroup _ _ (hperm.filter _)
    · intro b hb; exact (List.mem_filter.mp hb).1
    · intro b hb b' hb'
      have h1 := (List.mem_filter.mp hb).2
      have h2 := (List.mem_filter.mp hb').2
      simp only [beq_iff_eq] at h1 h2
      exact h1.trans h2.symm

/-- A core whose rows don't follow the order SQLite reads its bindings in
gives, from any order of the bindings it keeps, the rows it gives from the
order the model reads them in, in some order, on a database that keeps each
row's rowid in its first column and holds its declared constraints. With
DISTINCT it keeps the first of each set of equal rows, and the model's equal
rows are the same row. -/
theorem orderFree_rows_perm {s : Statement} (hwf : s.wellFormed = true) {db : Db}
    (hdb : db.Keyed) (hcons : db.Constrained s) {q k g : Nat} (hk : k ∈ (s.query q).arms)
    (hfree : s.followsOrder k = false) {outer : Bind} {kept' : List Bind}
    (hperm : kept'.Perm (coreBindings s db (evalQuery s db g) outer k)) :
    ((s.core k).result (evalQuery s db g) kept').Perm
      ((s.core k).result (evalQuery s db g) (coreBindings s db (evalQuery s db g) outer k)) := by
  have h := rows_perm_of_orderFree hwf hdb hcons hk hfree hperm
  unfold Core.result
  split
  · exact perm_eraseDups h
  · exact h

/-! Every column of a table that a picked query, or a core whose rows follow
the order SQLite reads them in, reads counts as read. -/

/-- Rule 7's theorem for a query whose rows SQLite picks, or a core whose rows
follow the order SQLite reads its bindings in: a change to any column of a
table that such a query or core reads reruns the statement, when the row
before or after passes the table's scope. -/
theorem picked_column_reruns {s : Statement} {scopeOf : Nat → Option RScope}
    {changes : List Change} {db' : Db} {ch : Change} (hch : ch ∈ changes)
    (hread : ch.table ∈ s.itemNumbers.filterMap s.table?) (hpicked : ch.table ∈ s.pickedTables)
    {old new : Row} (hold : ch.old = some old) (hnew : ch.new = some new) {c : Nat}
    (hc : c < s.width ch.table) (hchanged : old.cell c ≠ new.cell c)
    (hpasses : ch.old.any (scopePasses s scopeOf s.reads db' ch.table) ||
      ch.new.any (scopePasses s scopeOf s.reads db' ch.table)) :
    reruns s scopeOf s.reads changes db' = true := by
  unfold reruns
  simp only [List.any_eq_true]
  refine ⟨ch, hch, ?_⟩
  rw [s.find?_reads hread]
  simp only [Bool.and_eq_true]
  refine ⟨?_, hpasses⟩
  rw [hold, hnew]
  simp only [changedIn, List.any_eq_true, bne_iff_ne, ne_eq]
  refine ⟨c, ?_, hchanged⟩
  rw [List.mem_eraseDups, List.mem_append]
  right
  simp [hpicked, hc]

end SqliveNarrowing.Pick
