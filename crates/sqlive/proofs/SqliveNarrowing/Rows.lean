import SqliveNarrowing.Semantics

/-! Facts about where a query's rows come from: sorting, OFFSET and LIMIT keep only
rows they were given, and a core's row is made from one binding it keeps. -/
namespace SqliveNarrowing

theorem mem_insertSorted {α : Type} {le : α → α → Bool} {x y : α} {l : List α} :
    x ∈ insertSorted le y l ↔ x = y ∨ x ∈ l := by
  induction l with
  | nil => simp [insertSorted]
  | cons z zs ih =>
    unfold insertSorted
    split
    · simp
    · simp only [List.mem_cons, ih]
      constructor
      · rintro (h | h | h) <;> simp_all
      · rintro (h | h | h) <;> simp_all

theorem mem_sortBy {α : Type} {le : α → α → Bool} {x : α} {l : List α} :
    x ∈ sortBy le l ↔ x ∈ l := by
  induction l with
  | nil => simp [sortBy]
  | cons y ys ih => simp [sortBy, mem_insertSorted, ih]

/-- Sorting the images of a list under a map that keeps how two elements
sort sorts the list, then maps it. -/
theorem sortBy_map {α β : Type} {le : β → β → Bool} {le' : α → α → Bool} {R : α → β}
    (h : ∀ a b, le (R a) (R b) = le' a b) {l : List α} :
    sortBy le (l.map R) = (sortBy le' l).map R := by
  have hins : ∀ (x : α) (l : List α),
      insertSorted le (R x) (l.map R) = (insertSorted le' x l).map R := by
    intro x l
    induction l with
    | nil => rfl
    | cons y ys ih =>
      simp only [List.map_cons, insertSorted, h]
      split <;> simp [ih]
  induction l with
  | nil => rfl
  | cons x xs ih => simp only [List.map_cons, sortBy, ih, hins]

theorem mem_post {q : Query} {rows : List Row} {r : Row} (h : r ∈ q.post rows) : r ∈ rows := by
  unfold Query.post at h
  simp only at h
  have hs : ∀ {l : List Row}, r ∈ l → r ∈ (if q.orderBy.isEmpty then rows else
      sortBy (fun a b => keyLe (q.orderBy.map (·.2)) (q.orderBy.map fun (p, _) => a.cell p)
        (q.orderBy.map fun (p, _) => b.cell p)) rows) → r ∈ rows := by
    intro _ _ h
    split at h
    · exact h
    · exact mem_sortBy.mp h
  split at h
  · exact hs h (List.mem_of_mem_drop (List.mem_of_mem_take h))
  · exact hs h (List.mem_of_mem_drop h)

/-- A row of a core's result is one of its rows. -/
theorem mem_result {c : Core} {subs : Bind → Subs} {kept : List Bind} {r : Row}
    (h : r ∈ c.result subs kept) : r ∈ c.rows subs kept := by
  unfold Core.result at h
  split at h
  · exact List.mem_eraseDups.mp h
  · exact h

theorem mem_groupsOf {key : Bind → List Value} {bs g : List Bind} {b : Bind}
    (hg : g ∈ groupsOf key bs) (hb : b ∈ g) : b ∈ bs := by
  simp only [groupsOf, List.mem_map] at hg
  obtain ⟨_, _, rfl⟩ := hg
  exact (List.mem_filter.mp hb).1

/-- A row of a core whose one result column is `e` holds `e`'s value in a
binding the core keeps, or NULL for an aggregate over no binding. -/
theorem mem_rows_single {c : Core} {e : Expr} (ho : c.output = [.expr e])
    {subs : Bind → Subs} {kept : List Bind} {r : Row} (hr : r ∈ c.rows subs kept) :
    r = [none] ∨ ∃ b ∈ kept, r = [e.eval b (subs b)] := by
  unfold Core.rows at hr
  rw [ho] at hr
  split at hr
  · simp only [List.mem_map, List.map_cons, List.map_nil] at hr
    obtain ⟨g, hg, rfl⟩ := hr
    simp only [OutCol.aggregate]
    split
    · rename_i b hlast
      right
      refine ⟨b, ?_, rfl⟩
      have hb : b ∈ g := List.mem_of_getLast? hlast
      split at hg
      · simp only [List.mem_singleton] at hg
        subst hg
        exact hb
      · exact mem_groupsOf hg hb
    · left; rfl
  · simp only [List.mem_map, List.map_cons, List.map_nil] at hr
    obtain ⟨⟨b, j⟩, hb, rfl⟩ := hr
    exact Or.inr ⟨b, List.fst_mem_of_mem_zipIdx hb, rfl⟩

theorem combineAll_single (ops : List Combine) (rows : List Row) :
    combineAll ops [rows] = rows := by
  simp [combineAll]

/-- A row of a one-arm query whose one result column is `e` holds `e`'s
value in a binding the arm keeps, or NULL. -/
theorem mem_query_single {s : Statement} {db : Db} {f : Nat} {b : Bind} {q k : Nat} {e : Expr}
    (ha : (s.query q).arms = [k]) (ho : (s.core k).output = [.expr e]) {r : Row}
    (hr : r ∈ evalQuery s db (f + 1) b q) :
    r = [none] ∨ ∃ b' ∈ coreBindings s db (evalQuery s db f) b k,
      r = [e.eval b' (evalQuery s db f b')] := by
  simp only [evalQuery, queryRows, ha, List.map_cons, List.map_nil, combineAll_single] at hr
  exact mem_rows_single ho (mem_result (mem_post hr))

theorem mem_combine {op : Combine} {a b : List Row} {r : Row} (h : r ∈ combine op a b) :
    r ∈ a ∨ r ∈ b := by
  cases op <;> simp only [combine] at h
  · exact List.mem_append.mp h
  · exact List.mem_append.mp (List.mem_eraseDups.mp h)
  · exact Or.inl (List.mem_filter.mp (List.mem_eraseDups.mp h)).1
  · exact Or.inl (List.mem_filter.mp (List.mem_eraseDups.mp h)).1

/-- A compound's row is a row of one of its arms. -/
theorem mem_combineAll {ops : List Combine} {ls : List (List Row)} {r : Row}
    (h : r ∈ combineAll ops ls) : ∃ l ∈ ls, r ∈ l := by
  cases ls with
  | nil => simp [combineAll] at h
  | cons first rest =>
    simp only [combineAll] at h
    have key : ∀ (pairs : List (Combine × List Row)) (acc : List Row),
        r ∈ pairs.foldl (fun acc (op, rows) => combine op acc rows) acc →
        r ∈ acc ∨ ∃ p ∈ pairs, r ∈ p.2 := by
      intro pairs
      induction pairs with
      | nil => intro acc h; exact Or.inl h
      | cons p ps ih =>
        intro acc h
        rcases ih _ h with h | ⟨p', hp', hr⟩
        · rcases mem_combine h with h | h
          · exact Or.inl h
          · exact Or.inr ⟨p, by simp, h⟩
        · exact Or.inr ⟨p', by simp [hp'], hr⟩
    rcases key _ _ h with h | ⟨p, hp, hr⟩
    · exact ⟨first, by simp, h⟩
    · exact ⟨p.2, by simp [(List.of_mem_zip hp).2], hr⟩

/-- A row of a core that doesn't aggregate holds its result columns' values
in a binding it keeps, at some position of the bindings. -/
theorem mem_rows_plain {c : Core} (hc : c.aggregated = false) {subs : Bind → Subs}
    {kept : List Bind} {r : Row} (hr : r ∈ c.rows subs kept) :
    ∃ b ∈ kept, ∃ j, r = c.output.map (OutCol.plain subs kept b j) := by
  unfold Core.rows at hr
  simp only [hc, Bool.false_eq_true, ↓reduceIte, List.mem_map] at hr
  obtain ⟨⟨b, j⟩, hb, rfl⟩ := hr
  exact ⟨b, List.fst_mem_of_mem_zipIdx hb, j, rfl⟩

/-- A core that neither aggregates nor calls a window function makes each
binding's row as `Core.outRow` does. -/
theorem Core.plain_outRow {c : Core} (hw : c.windowed = false) (subs : Bind → Subs)
    (kept : List Bind) (b : Bind) (j : Nat) :
    c.output.map (OutCol.plain subs kept b j) = c.outRow subs b := by
  unfold Core.outRow
  apply List.map_congr_left
  intro o ho
  cases o with
  | expr e => rfl
  | rowNumber p ob =>
    have : c.windowed = true := by
      unfold Core.windowed
      exact List.any_eq_true.mpr ⟨_, ho, rfl⟩
    rw [hw] at this
    cases this
  | _ => rfl

/-- A row of a one-arm query holds, at a position whose result column is
`e`, NULL or `e`'s value in a binding the arm keeps; when the arm neither
aggregates nor calls a window function, the row is that binding's. -/
theorem query_row_at {s : Statement} {db : Db} {f : Nat} {b : Bind} {q k i : Nat} {e : Expr}
    (ha : (s.query q).arms = [k]) (ho : (s.core k).output[i]? = some (.expr e)) {r : Row}
    (hr : r ∈ evalQuery s db (f + 1) b q) :
    r[i]? = some none ∨ ∃ b' ∈ coreBindings s db (evalQuery s db f) b k,
      r[i]? = some (e.eval b' (evalQuery s db f b')) ∧
      ((s.core k).aggregated = false → (s.core k).windowed = false →
        r = (s.core k).outRow (evalQuery s db f) b') := by
  simp only [evalQuery, queryRows, ha, List.map_cons, List.map_nil, combineAll_single] at hr
  have hr := mem_result (mem_post hr)
  unfold Core.rows at hr
  split at hr
  · rename_i hagg
    simp only [List.mem_map] at hr
    obtain ⟨g, hg, rfl⟩ := hr
    simp only [List.getElem?_map, ho, Option.map_some, OutCol.aggregate]
    split
    · rename_i b' hlast
      right
      refine ⟨b', ?_, rfl, fun h => by rw [h] at hagg; cases hagg⟩
      have hb : b' ∈ g := List.mem_of_getLast? hlast
      split at hg
      · simp only [List.mem_singleton] at hg
        subst hg
        exact hb
      · exact mem_groupsOf hg hb
    · left; rfl
  · simp only [List.mem_map] at hr
    obtain ⟨⟨b', j⟩, hb', rfl⟩ := hr
    right
    exact ⟨b', List.fst_mem_of_mem_zipIdx hb', by simp [List.getElem?_map, ho, OutCol.plain],
      fun _ hw => Core.plain_outRow hw _ _ _ _⟩

end SqliveNarrowing
