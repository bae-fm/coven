import SqliveNarrowing.Reader

/-! Rule 4: a condition on a derived item's rows (a FROM subquery, a CTE or
a view) passes into each of its arms that calls no window function, when
its query has no LIMIT and no OFFSET: an output column that is a bare column of the arm's
own item holds that item's value, so a term or an edge on the column holds of
the item's row (`derived.rs`, `push_down`). In an arm that groups its rows or
calls an aggregate, only a column that its GROUP BY lists as a bare column
passes on: every binding of a group holds the group's value there, so a term
true of the group's row is true of the item's row in each of them. The target
is then an inner
join's, as a WHERE condition in the arm would make it, except that a term a
row of NULLs can pass, and an `IS` edge, narrow nothing on a LEFT JOIN's
right side. -/
namespace SqliveNarrowing.PushDown

/-- For a derived item, each arm a condition on its rows passes into: the
item and column each output column passes on unchanged, which in an arm that
aggregates is only a column its GROUP BY lists. `none` for a table; no arm
when the query has a LIMIT or an OFFSET. -/
def arms (s : Statement) (i : Nat) : Option (List (List (Option (Nat × Nat)))) :=
  match (s.item i).kind with
  | .table _ => none
  | .derived q _ =>
    let query := s.query q
    if query.limit.isSome || query.offset.isSome then some []
    else some <| (query.arms.filter fun k => !(s.core k).windowed).map fun k =>
      (s.core k).output.map fun
        | .expr (.col j c) =>
          if (s.item j).core = k && (!(s.core k).aggregated || (s.core k).groupBy.contains (.col j c))
          then some (j, c) else none
        | _ => none

/-- The column of `target` that arm `arm` of a derived item's query passes on
as its result column `c`, if it does. -/
def renameInto (arm : List (Option (Nat × Nat))) (target c : Nat) : Option Nat :=
  match (arm[c]?).join with
  | some (t, c') => if t = target then some c' else none
  | none => none

/-- `e`, an edge of a derived item, passed into `target`, whose column
`rename c` holds the derived item's column `c`. -/
def edgeInto (target : Nat) (rename : Nat → Option Nat) (e : Edge) : Option Edge :=
  (e.memberValue.passed e.member target rename).map fun value =>
    { e with member := target, memberValue := value, pushed := true }

/-- The edges among `edges` passed into `target`, a LEFT JOIN's right side
that stays one when `left`: an `IS` edge passes into no such side, and an
edge whose member value can be other than NULL on a row of NULLs passes into
no item a LEFT JOIN adds. -/
def passedInto (s : Statement) (left : Bool) (target : Nat) (rename : Nat → Option Nat)
    (edges : List Edge) : List Edge :=
  (edges.filter fun e => !(left && e.nulls) &&
    (s.alwaysBound target || e.memberValue.strict)).filterMap (edgeInto target rename)

/-- Pass `term` and `edges`, which item `item`'s own core puts on its rows,
into the arms of `item` and, through them, further down. -/
def pushDown (s : Statement) (narrowFuel : Nat) :
    Nat → PState → Nat → Option STerm → List Edge → PState
  | 0, st, _, _, _ => st
  | fuel + 1, st, item, term, edges =>
    match arms s item with
    | none => st
    | some found =>
      if term.isNone && edges.isEmpty then st
      else found.foldl (fun st arm =>
        let through (c : Nat) : Option (Nat × Nat) := (arm[c]?).join
        let tested := (term.map STerm.columns).getD []
        let reached := tested ++ edges.flatMap fun e => e.memberValue.columns
        let targets := (reached.filterMap fun c => (through c).map (·.1)).eraseDups
        targets.foldl (fun st target =>
          let rename := renameInto arm target
          let left := (st.items target).leftOn.isSome
          let renamed := (term.bind (·.renamed rename (STerm.judgedOn target rename))).filter
            fun r => !(left && r.mayHoldOnNulls)
          let passed := passedInto s left target rename edges
          let st := match renamed with
            | some r => st.addTerm target r
            | none => st
          let st := { st with edges := st.edges ++ passed, pushed := st.pushed ++ passed }
          let st := if renamed.isSome || !passed.isEmpty then st.joinInner narrowFuel target else st
          pushDown s narrowFuel fuel st target renamed passed) st) st

/-! The proof: a row of a derived item's query is a row of one of its arms,
and a row of an arm that doesn't aggregate holds, in each column the arm
passes on, the value of the item it passes on. So a term or an edge true of
the derived row is true of that item's row. -/

/-- An expression over item `target` evaluates as the expression does where
`target`'s row holds, in each column `rename` gives, the value of the column it
stands for. -/
theorem eval_over (target : Nat) (rename : Nat → Nat) {b b' : Bind} {sub : Subs} (e : Expr)
    (h : ∀ j c, (j, c) ∈ e.refs → cellOf (b' target) (rename c) = cellOf (b j) c) :
    (e.over target rename).eval b' sub = e.eval b sub := by
  induction e with
  | col j c => exact h j c (by simp [Expr.refs])
  | lit v => rfl
  | sub q => rfl
  | add l r ihl ihr | greatest l r ihl ihr | coalesce l r ihl ihr =>
    simp only [Expr.over, Expr.eval]
    rw [ihl fun j c hc => h j c (by simp [Expr.refs, hc]),
      ihr fun j c hc => h j c (by simp [Expr.refs, hc])]
  | iif t y n iht ihy ihn =>
    simp only [Expr.over, Expr.eval]
    rw [iht fun j c hc => h j c (by simp [Expr.refs, hc]),
      ihy fun j c hc => h j c (by simp [Expr.refs, hc]),
      ihn fun j c hc => h j c (by simp [Expr.refs, hc])]
  | isNull e ih =>
    simp only [Expr.over, Expr.eval]
    rw [ih fun j c hc => h j c (by simp [Expr.refs, hc])]

/-- Expressions over `target` evaluate as the expressions do (`eval_over`). -/
theorem evals_over (target : Nat) (rename : Nat → Nat) {b b' : Bind} {sub : Subs} (es : List Expr)
    (h : ∀ j c, (j, c) ∈ es.flatMap Expr.refs →
      cellOf (b' target) (rename c) = cellOf (b j) c) :
    (es.map (Expr.over target rename)).map (Expr.eval b' sub) = es.map (Expr.eval b sub) := by
  rw [List.map_map]
  apply List.map_congr_left
  intro e he
  exact eval_over target rename e fun j c hc => h j c (List.mem_flatMap.mpr ⟨e, he, hc⟩)

/-- `eval_over` for a condition. -/
theorem cond_eval_over (target : Nat) (rename : Nat → Nat) {b b' : Bind} {sub : Subs} (cd : Cond)
    (h : ∀ j c, (j, c) ∈ cd.refs → cellOf (b' target) (rename c) = cellOf (b j) c) :
    (cd.over target rename).eval b' sub = cd.eval b sub := by
  induction cd with
  | cmp op l r | collated key op l r =>
    simp only [Cond.over, Cond.eval]
    rw [eval_over target rename l fun j c hc => h j c (by simp [Cond.refs, hc]),
      eval_over target rename r fun j c hc => h j c (by simp [Cond.refs, hc])]
  | null e n =>
    simp only [Cond.over, Cond.eval]
    rw [eval_over target rename e fun j c hc => h j c (by simpa [Cond.refs] using hc)]
  | inList e vs =>
    simp only [Cond.over, Cond.eval]
    rw [eval_over target rename e fun j c hc => h j c (by simp [Cond.refs, hc]),
      evals_over target rename vs fun j c hc => h j c (by
        simp only [Cond.refs, List.mem_append]; exact Or.inr hc)]
  | inQuery e q n =>
    simp only [Cond.over, Cond.eval]
    rw [eval_over target rename e fun j c hc => h j c (by simpa [Cond.refs] using hc)]
  | «exists» q n => rfl
  | rowSub vs q isIn n =>
    simp only [Cond.over, Cond.eval]
    rw [evals_over target rename vs fun j c hc => h j c (by simpa [Cond.refs] using hc)]
  | rowEq ls rs =>
    simp only [Cond.over, Cond.eval]
    rw [evals_over target rename ls fun j c hc => h j c (by
        simp only [Cond.refs, List.mem_append]; exact Or.inl hc),
      evals_over target rename rs fun j c hc => h j c (by
        simp only [Cond.refs, List.mem_append]; exact Or.inr hc)]
  | and l r ihl ihr | or l r ihl ihr =>
    simp only [Cond.over, Cond.eval]
    rw [ihl fun j c hc => h j c (by simp [Cond.refs, hc]),
      ihr fun j c hc => h j c (by simp [Cond.refs, hc])]
  | not c ih =>
    simp only [Cond.over, Cond.eval]
    rw [ih fun j c' hc => h j c' (by simpa [Cond.refs] using hc)]

/-- An expression over another item holds the subqueries the expression
does, is strict where it is, and names as many columns. -/
theorem over_shape (target : Nat) (rename : Nat → Nat) (e : Expr) :
    (e.over target rename).queries = e.queries ∧ (e.over target rename).strict = e.strict ∧
      (e.over target rename).refs = e.refs.map fun x => (target, rename x.2) := by
  induction e with
  | col j c => simp [Expr.over, Expr.queries, Expr.strict, Expr.refs]
  | lit v => simp [Expr.over, Expr.queries, Expr.strict, Expr.refs]
  | sub q => simp [Expr.over, Expr.queries, Expr.strict, Expr.refs]
  | add l r ihl ihr | greatest l r ihl ihr | coalesce l r ihl ihr =>
    simp [Expr.over, Expr.queries, Expr.strict, Expr.refs, ihl, ihr]
  | iif t y n iht ihy ihn => simp [Expr.over, Expr.queries, Expr.strict, Expr.refs, iht, ihy, ihn]
  | isNull e ih => simp [Expr.over, Expr.queries, Expr.strict, Expr.refs, ih]

/-- Where `rename` gives a column for each of item `i`'s that `refs` names,
and `row` holds in each the value `r` holds in the one it stands for, `row`
holds in the renamed column what any row of a binding where every item holds
`r` holds in the named one. -/
theorem renames_cells {i : Nat} {rename : Nat → Option Nat} {refs : List (Nat × Nat)}
    (h : renames i rename refs = true) {r : Row} {row : Option Row}
    (hcols : ∀ c c', rename c = some c' → r.cell c = cellOf row c') :
    ∀ j c, (j, c) ∈ refs → cellOf row ((rename c).getD c) = cellOf (rowBind (some r) j) c := by
  intro j c hc
  have hone := List.all_eq_true.mp h (j, c) hc
  simp only [Bool.and_eq_true] at hone
  obtain ⟨c', hc'⟩ := Option.isSome_iff_exists.mp hone.2
  simp only [hc', Option.getD_some, rowBind, cellOf]
  exact (hcols c c' hc').symm

/-- A condition true of a row holds, passed to another item, of a row that
holds its columns' values (`Cond.passed`). -/
theorem passed_sound {i target : Nat} {rename : Nat → Option Nat} {cd cd' : Cond}
    (h : cd.passed i target rename = some cd') {r : Row} {row : Option Row}
    (hcols : ∀ c c', rename c = some c' → r.cell c = cellOf row c') :
    cd'.eval (rowBind row) noSubs = cd.eval (rowBind (some r)) noSubs := by
  unfold Cond.passed at h
  split at h
  · rename_i hc
    simp only [Bool.and_eq_true] at hc
    cases h
    exact cond_eval_over target _ cd (renames_cells hc.2 hcols)
  · cases h

/-- A value of a row, passed to another item, is the value a row that holds
its columns' values gives (`PValue.passed`). -/
theorem value_passed_sound {i target : Nat} {rename : Nat → Option Nat} {v v' : PValue}
    (h : v.passed i target rename = some v') {r : Row} {row : Option Row}
    (hcols : ∀ c c', rename c = some c' → r.cell c = cellOf row c') :
    v'.on row = v.on (some r) := by
  cases v with
  | column c =>
    simp only [PValue.passed, Option.map_eq_some_iff] at h
    obtain ⟨c', hc', rfl⟩ := h
    exact (hcols c c' hc').symm
  | expr e =>
    simp only [PValue.passed] at h
    split at h
    · rename_i hc
      simp only [Bool.and_eq_true] at hc
      cases h
      exact eval_over target _ e (renames_cells hc.2 hcols)
    · cases h

/-- A strict value passed to an item is NULL where the item has no row. -/
theorem value_passed_none {i target : Nat} {rename : Nat → Option Nat} {v v' : PValue}
    (h : v.passed i target rename = some v') (hs : v.strict = true) : v'.on none = none := by
  cases v with
  | column c =>
    simp only [PValue.passed, Option.map_eq_some_iff] at h
    obtain ⟨c', _, rfl⟩ := h
    rfl
  | expr e =>
    simp only [PValue.passed] at h
    split at h
    · rename_i hc
      simp only [Bool.and_eq_true, Bool.not_eq_true', List.isEmpty_eq_false_iff,
        List.isEmpty_iff] at hc
      cases h
      obtain ⟨hq, hst, hr⟩ := over_shape target (fun c => (rename c).getD c) e
      refine Expr.eval_null _ (by simp [hr, hc.1.1]) (by rw [hq]; exact hc.1.2)
        (by rw [hst]; exact hs) fun _ _ _ => rfl
    · cases h

/-- A term over a row whose columns `rename` passes on to another row holds
of that row as renamed, where what `judged` gives for a condition SQLite
judges holds of it too. -/
theorem renamed_sound {t t' : STerm} {rename : Nat → Option Nat}
    {judged : Nat → Cond → Option STerm} (h : t.renamed rename judged = some t')
    {r : Row} {row : Option Row} (hcols : ∀ c c', rename c = some c' → r.cell c = cellOf row c')
    (hjudged : ∀ i cd t', judged i cd = some t' → STerm.holds (some r) (.row i cd) = true →
      t'.holds row = true)
    (ht : t.holds (some r)) : t'.holds row := by
  induction t generalizing t' with
  | compare c op v =>
    simp only [STerm.renamed, Option.map_eq_some_iff] at h
    obtain ⟨c', hc', rfl⟩ := h
    simpa [STerm.holds, cellOf, hcols c c' hc'] using ht
  | inList c vs =>
    simp only [STerm.renamed, Option.map_eq_some_iff] at h
    obtain ⟨c', hc', rfl⟩ := h
    simpa [STerm.holds, cellOf, hcols c c' hc'] using ht
  | null c n =>
    simp only [STerm.renamed, Option.map_eq_some_iff] at h
    obtain ⟨c', hc', rfl⟩ := h
    simpa [STerm.holds, cellOf, hcols c c' hc'] using ht
  | row i cd =>
    simp only [STerm.renamed] at h
    exact hjudged i cd t' h ht
  | and l r ihl ihr =>
    simp only [STerm.holds, Bool.and_eq_true] at ht
    simp only [STerm.renamed] at h
    split at h
    · rename_i lt rt hl hr
      cases h
      simp [STerm.holds, ihl hl ht.1, ihr hr ht.2]
    · rename_i t'' hl _
      cases h
      exact ihl hl ht.1
    · rename_i t'' _ hr
      cases h
      exact ihr hr ht.2
    · cases h
  | or l r ihl ihr =>
    simp only [STerm.renamed] at h
    split at h
    · rename_i lt rt hl hr
      cases h
      simp only [STerm.holds, Bool.or_eq_true] at ht ⊢
      rcases ht with ht | ht
      · exact Or.inl (ihl hl ht)
      · exact Or.inr (ihr hr ht)
    · cases h

/-- A condition SQLite judges, passed to an item (`STerm.judgedOn`), holds of
a row that holds its columns' values. -/
theorem judgedOn_sound {target : Nat} {rename : Nat → Option Nat} {r : Row} {row : Option Row}
    (hcols : ∀ c c', rename c = some c' → r.cell c = cellOf row c') :
    ∀ i cd t', STerm.judgedOn target rename i cd = some t' →
      STerm.holds (some r) (.row i cd) = true → t'.holds row = true := by
  intro i cd t' h ht
  simp only [STerm.judgedOn, Option.map_eq_some_iff] at h
  obtain ⟨cd', hcd, rfl⟩ := h
  simp only [STerm.holds] at ht ⊢
  rw [passed_sound hcd hcols]
  exact ht

/-- Rule 4's theorem: each row of a derived item's query, when the query has
no LIMIT, is a row of one of its arms, and a row of an arm that neither
groups nor aggregates holds, in each result column that is a bare column of
one of its items, that column's value in a binding the arm keeps. -/
theorem push_down_sound {s : Statement} {q : Nat} {db : Db} {f : Nat} {b : Bind} {r : Row}
    (hr : r ∈ evalQuery s db (f + 1) b q) :
    ∃ k ∈ (s.query q).arms, (s.core k).aggregated = true ∨
      ∃ b' ∈ coreBindings s db (evalQuery s db f) b k,
        ∀ c j c', (s.core k).output[c]? = some (.expr (.col j c')) → r.cell c = cellOf (b' j) c' := by
  simp only [evalQuery, queryRows] at hr
  obtain ⟨rows, hrows, hr⟩ := mem_combineAll (mem_post hr)
  simp only [List.mem_map] at hrows
  obtain ⟨k, hk, rfl⟩ := hrows
  refine ⟨k, hk, ?_⟩
  cases hagg : (s.core k).aggregated with
  | true => exact Or.inl rfl
  | false =>
    right
    obtain ⟨b', hb', j, rfl⟩ := mem_rows_plain hagg (mem_result hr)
    refine ⟨b', hb', fun c j c' hc => ?_⟩
    simp [Row.cell, List.getElem?_map, hc, Expr.eval, OutCol.plain]

/-- Rule 4's theorem for an arm that groups its rows or calls an aggregate:
each row of a derived item's query, when the query has no LIMIT, is a row of
one of its arms, and a row of such an arm is the row of a group of the
bindings the arm keeps, every one of which holds, in each result column that
is a bare column the GROUP BY lists, the row's value. So a term true of the
row is true of the column's item's row in every binding the row is made
from. -/
theorem group_key_sound {s : Statement} {q : Nat} {db : Db} {f : Nat} {b : Bind} {r : Row}
    (hr : r ∈ evalQuery s db (f + 1) b q) :
    ∃ k ∈ (s.query q).arms, (s.core k).aggregated = false ∨
      ∃ g : List Bind, (∀ b' ∈ g, b' ∈ coreBindings s db (evalQuery s db f) b k) ∧
        ∀ b' ∈ g, ∀ c j c', (s.core k).output[c]? = some (.expr (.col j c')) →
          Expr.col j c' ∈ (s.core k).groupBy → r.cell c = cellOf (b' j) c' := by
  simp only [evalQuery, queryRows] at hr
  obtain ⟨rows, hrows, hr⟩ := mem_combineAll (mem_post hr)
  simp only [List.mem_map] at hrows
  obtain ⟨k, hk, rfl⟩ := hrows
  refine ⟨k, hk, ?_⟩
  cases hagg : (s.core k).aggregated with
  | false => exact Or.inl rfl
  | true =>
    right
    have hr := mem_result hr
    unfold Core.rows at hr
    simp only [hagg, ↓reduceIte, List.mem_map] at hr
    obtain ⟨g, hg, rfl⟩ := hr
    refine ⟨g, fun b' hb' => ?_, fun b' hb' c j c' hc hkey => ?_⟩
    · split at hg
      · simp only [List.mem_singleton] at hg
        subst hg
        exact hb'
      · exact mem_groupsOf hg hb'
    · have hne : (s.core k).groupBy.isEmpty = false := by
        cases h : (s.core k).groupBy with
        | nil => rw [h] at hkey; cases hkey
        | cons _ _ => rfl
      simp only [hne, Bool.false_eq_true, ↓reduceIte, groupsOf, List.mem_map] at hg
      obtain ⟨key, _, rfl⟩ := hg
      cases hlast : (List.filter _ _).getLast? with
      | none =>
        rw [List.getLast?_eq_none_iff] at hlast
        rw [hlast] at hb'
        cases hb'
      | some bl =>
        have hbl := List.mem_of_getLast? hlast
        simp only [Row.cell, List.getElem?_map, hc, Option.map_some, Option.join_some,
          OutCol.aggregate, hlast, Expr.eval]
        simp only [List.mem_filter, beq_iff_eq] at hb' hbl
        have heq := hb'.2.trans hbl.2.symm
        have := (List.map_inj_left.mp heq) _ hkey
        simpa [Expr.eval] using this.symm

end SqliveNarrowing.PushDown
