import SqliveNarrowing.Reader

/-! An OR of conditions that each narrow one item narrows that item to the
rows either side allows (`read_or`): `a.x = t.y OR a.id = t.z + 1` narrows
`a` to the rows whose `x` some row of `t` holds in `y`, or whose `id` some
row of `t` gives as `z + 1`. A row the OR keeps passes one of its sides,
and each side's narrowing holds of every row that side keeps, so the OR of
the narrowings holds of every row the OR keeps.

Each side is read as its conjuncts, each by the other rules. An item gets a
disjunction when every side narrows it, by a term on its row or an edge from
it; a side that says nothing of the item leaves it unnarrowed. The planner
keeps a disjunction for an item when the partners of every side's edges are
placed before the item, and binds it then: the edges of a side can't be
ordered as memberships of their own. -/
namespace SqliveNarrowing.Or

/-- The sides of an OR, its nested ORs flattened. -/
def sides : Cond → List Cond
  | .or l r => sides l ++ sides r
  | c => [c]

/-- The conjuncts of a condition joined by AND. -/
def conjuncts : Cond → List Cond
  | .and l r => conjuncts l ++ conjuncts r
  | c => [c]

/-- What `read` reads from each conjunct of `side`, written in core `k`. -/
def sideNarrowing (read : ReadCondition) (s : Statement) (k : Nat) (side : Cond) : Narrowing :=
  { terms := (conjuncts side).flatMap fun c => ((read s k c).map (·.terms)).getD []
    edges := (conjuncts side).flatMap fun c => ((read s k c).map (·.edges)).getD [] }

/-- What of `n` narrows item `i`. -/
def sideFor (n : Narrowing) (i : Nat) : Side :=
  { terms := (n.terms.filter (·.1 = i)).map (·.2), edges := n.edges.filter (·.member = i) }

/-- The items the first side narrows. -/
def items : List Narrowing → List Nat
  | first :: _ =>
    (first.terms.map (fun (p : Nat × STerm) => p.1) ++ first.edges.map (fun (e : Edge) => e.member)).eraseDups
  | [] => []

/-- The reader, with `read` reading each side's conjuncts: an OR gives a
disjunction for each item its first side narrows and every other side
narrows too. -/
def readCondition (read : ReadCondition) : ReadCondition := fun s k cd =>
  match cd with
  | .or l r =>
    let found : List Narrowing := (sides (.or l r)).map (sideNarrowing read s k)
    some { ors := (items found).filterMap fun i =>
      let each := found.map (sideFor · i)
      if each.all Side.narrows then some ⟨i, each⟩ else none }
  | _ => none

/-! The proof. -/

theorem sides_true {cd : Cond} {b : Bind} {sub : Subs} (h : isTrue (cd.eval b sub)) :
    ∃ c ∈ sides cd, isTrue (c.eval b sub) := by
  induction cd with
  | or l r ihl ihr =>
    simp only [Cond.eval, isTrue] at h
    have : isTrue (l.eval b sub) ∨ isTrue (r.eval b sub) := by
      revert h
      cases l.eval b sub <;> cases r.eval b sub <;> simp [tor, isTrue] <;>
        (try rename_i x; cases x) <;> (try rename_i y; cases y) <;> simp_all
    rcases this with hl | hr
    · obtain ⟨c, hc, ht⟩ := ihl hl
      exact ⟨c, by simp [sides, hc], ht⟩
    · obtain ⟨c, hc, ht⟩ := ihr hr
      exact ⟨c, by simp [sides, hc], ht⟩
  | _ => exact ⟨_, by simp [sides], h⟩

theorem conjuncts_true {cd : Cond} {b : Bind} {sub : Subs} (h : isTrue (cd.eval b sub)) :
    ∀ c ∈ conjuncts cd, isTrue (c.eval b sub) := by
  induction cd with
  | and l r ihl ihr =>
    simp only [Cond.eval, isTrue] at h
    have hl : isTrue (l.eval b sub) ∧ isTrue (r.eval b sub) := by
      revert h
      cases l.eval b sub <;> cases r.eval b sub <;> simp [tand, isTrue] <;>
        (try rename_i x; cases x) <;> (try rename_i y; cases y) <;> simp_all
    intro c hc
    simp only [conjuncts, List.mem_append] at hc
    rcases hc with hc | hc
    · exact ihl hl.1 c hc
    · exact ihr hl.2 c hc
  | _ =>
    intro c hc
    simp only [conjuncts, List.mem_singleton] at hc
    subst hc
    exact h

/-- The items a side of an OR names are items the OR names. -/
theorem sides_items {cd c : Cond} (h : c ∈ sides cd) : ∀ j ∈ c.items, j ∈ cd.items := by
  induction cd with
  | or l r ihl ihr =>
    intro j hj
    simp only [sides, List.mem_append] at h
    simp only [Cond.items, Cond.refs, List.map_append, List.mem_append]
    rcases h with h | h
    · exact Or.inl (ihl h j hj)
    · exact Or.inr (ihr h j hj)
  | _ =>
    simp only [sides, List.mem_singleton] at h
    subst h
    exact fun j hj => hj

/-- The items a conjunct names are items its condition names. -/
theorem conjuncts_items {cd c : Cond} (h : c ∈ conjuncts cd) : ∀ j ∈ c.items, j ∈ cd.items := by
  induction cd with
  | and l r ihl ihr =>
    intro j hj
    simp only [conjuncts, List.mem_append] at h
    simp only [Cond.items, Cond.refs, List.map_append, List.mem_append]
    rcases h with h | h
    · exact Or.inl (ihl h j hj)
    · exact Or.inr (ihr h j hj)
  | _ =>
    simp only [conjuncts, List.mem_singleton] at h
    subst h
    exact fun j hj => hj

/-- The subqueries a side of an OR holds are subqueries the OR holds. -/
theorem sides_queries {cd c : Cond} (h : c ∈ sides cd) : ∀ q ∈ c.queries, q ∈ cd.queries := by
  induction cd with
  | or l r ihl ihr =>
    intro q hq
    simp only [sides, List.mem_append] at h
    simp only [Cond.queries, List.mem_append]
    rcases h with h | h
    · exact Or.inl (ihl h q hq)
    · exact Or.inr (ihr h q hq)
  | _ =>
    simp only [sides, List.mem_singleton] at h
    subst h
    exact fun q hq => hq

/-- The subqueries a conjunct holds are subqueries its condition holds. -/
theorem conjuncts_queries {cd c : Cond} (h : c ∈ conjuncts cd) : ∀ q ∈ c.queries, q ∈ cd.queries := by
  induction cd with
  | and l r ihl ihr =>
    intro q hq
    simp only [conjuncts, List.mem_append] at h
    simp only [Cond.queries, List.mem_append]
    rcases h with h | h
    · exact Or.inl (ihl h q hq)
    · exact Or.inr (ihr h q hq)
  | _ =>
    simp only [conjuncts, List.mem_singleton] at h
    subst h
    exact fun q hq => hq

theorem mem_side_terms {read : ReadCondition} {s : Statement} {k : Nat} {side : Cond}
    {i : Nat} {t : STerm} (h : (i, t) ∈ (sideNarrowing read s k side).terms) :
    ∃ c ∈ conjuncts side, ∃ n, read s k c = some n ∧ (i, t) ∈ n.terms := by
  simp only [sideNarrowing, List.mem_flatMap] at h
  obtain ⟨c, hc, hm⟩ := h
  cases hr : read s k c with
  | none => simp [hr] at hm
  | some n => exact ⟨c, hc, n, hr, by simpa [hr] using hm⟩

theorem mem_side_edges {read : ReadCondition} {s : Statement} {k : Nat} {side : Cond}
    {e : Edge} (h : e ∈ (sideNarrowing read s k side).edges) :
    ∃ c ∈ conjuncts side, ∃ n, read s k c = some n ∧ e ∈ n.edges := by
  simp only [sideNarrowing, List.mem_flatMap] at h
  obtain ⟨c, hc, hm⟩ := h
  cases hr : read s k c with
  | none => simp [hr] at hm
  | some n => exact ⟨c, hc, n, hr, by simpa [hr] using hm⟩

/-- The theorem: with `read` keeping the reader's promise, an OR true of a
binding makes one side of each disjunction it reads hold: each term of the
side true of the item's row, and each edge of it holding. -/
theorem or_sound {read : ReadCondition} (h : read.Sound) : (readCondition read).Disjoins := by
  intro s k cd n hn
  cases cd
  case or l r =>
    simp only [readCondition, Option.some.injEq] at hn
    subst hn
    refine ⟨rfl, rfl, fun d hd => ?_⟩
    simp only [List.mem_filterMap] at hd
    obtain ⟨i, hi, hd⟩ := hd
    split at hd
    · rename_i hnarrows
      cases hd
      refine ⟨?_, fun side hside => List.all_eq_true.mp hnarrows side hside, ?_, ?_⟩
      · -- The item is one the first side's conjuncts narrow, an item of core `k`.
        generalize hfound : (sides (.or l r)).map (sideNarrowing read s k) = found at hi
        cases found with
        | nil => simp [items] at hi
        | cons first rest =>
          simp only [items, List.mem_eraseDups, List.mem_append, List.mem_map] at hi
          have hfirst : first ∈ (sides (.or l r)).map (sideNarrowing read s k) := by
            rw [hfound]; simp
          obtain ⟨side, _, rfl⟩ := List.mem_map.mp hfirst
          rcases hi with ⟨⟨_, t⟩, ht, rfl⟩ | ⟨e, he, rfl⟩
          · obtain ⟨c, _, n', hr, ht'⟩ := mem_side_terms ht
            exact (h.terms hr _ t ht').1
          · obtain ⟨c, _, n', hr, he'⟩ := mem_side_edges he
            exact (h.edges hr e he').1
      · intro side hside e he
        obtain ⟨found, hfound, rfl⟩ := List.mem_map.mp hside
        simp only [sideFor, List.mem_filter, decide_eq_true_eq] at he
        obtain ⟨he, hmem⟩ := he
        obtain ⟨side', hside', rfl⟩ := List.mem_map.mp hfound
        obtain ⟨c, hc, n', hr, he'⟩ := mem_side_edges he
        obtain ⟨_, hne, hnamed, hread, hstrict, hnested, _⟩ := h.edges hr e he'
        exact ⟨hmem, hne, fun hn => sides_items hside' _ (conjuncts_items hc _ (hnamed hn)), hread,
          hstrict, hnested.mono fun q hq => sides_queries hside' _ (conjuncts_queries hc _ hq)⟩
      · intro db f b hc
        obtain ⟨side, hside, htrue⟩ := sides_true hc
        refine ⟨sideFor (sideNarrowing read s k side) i,
          List.mem_map_of_mem (List.mem_map_of_mem hside), ?_, ?_⟩
        · intro t ht
          simp only [sideFor, List.mem_map, List.mem_filter, decide_eq_true_eq] at ht
          obtain ⟨⟨j, t'⟩, ⟨ht', hj⟩, rfl⟩ := ht
          simp only at hj
          subst hj
          obtain ⟨c, hc', n', hr, ht''⟩ := mem_side_terms ht'
          exact (h.terms hr j t' ht'').2 db f b (conjuncts_true htrue c hc')
        · intro e he
          simp only [sideFor, List.mem_filter, decide_eq_true_eq] at he
          obtain ⟨he, _⟩ := he
          obtain ⟨c, hc', n', hr, he'⟩ := mem_side_edges he
          exact (h.edges hr e he').2.2.2.2.2.2 db f b (conjuncts_true htrue c hc')
    · cases hd
  all_goals simp [readCondition] at hn

end SqliveNarrowing.Or
