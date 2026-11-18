import SqliveNarrowing.Semantics

/-! Facts about where a query's rows come from: sorting and LIMIT keep only
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
  · exact hs h (List.mem_of_mem_take h)
  · exact hs h h

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
    obtain ⟨b, hb, rfl⟩ := hr
    exact Or.inr ⟨b, hb, rfl⟩

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
  exact mem_rows_single ho (mem_post hr)

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
in a binding it keeps. -/
theorem mem_rows_plain {c : Core} (hc : c.aggregated = false) {subs : Bind → Subs}
    {kept : List Bind} {r : Row} (hr : r ∈ c.rows subs kept) :
    ∃ b ∈ kept, r = c.output.map fun
      | .expr e => e.eval b (subs b)
      | _ => none := by
  unfold Core.rows at hr
  simp only [hc, Bool.false_eq_true, ↓reduceIte, List.mem_map] at hr
  obtain ⟨b, hb, rfl⟩ := hr
  exact ⟨b, hb, rfl⟩

end SqliveNarrowing
