import SqliveNarrowing.Fuel
import SqliveNarrowing.Unjudged

/-! A query's rows that pass a test: DISTINCT, UNION, EXCEPT, INTERSECT and
an ORDER BY with no LIMIT keep the rows a test keeps of the rows they read
(`filter_eraseDups`, `combineAll_filter`, `post_filter`), and a core's rows
read its subqueries only under the bindings it keeps (`Core.rows_congr`,
`Core.result_congr`). The restriction lemma (`Restrict.lean`) takes a
query's rows that matter through each. -/
namespace SqliveNarrowing

/-! Lists. -/

theorem filterMap_congr_mem {α β : Type} {f g : α → Option β} :
    ∀ {l : List α}, (∀ a ∈ l, f a = g a) → l.filterMap f = l.filterMap g
  | [], _ => rfl
  | a :: l, h => by
    simp only [List.filterMap_cons, h a (by simp),
      filterMap_congr_mem (fun x hx => h x (List.mem_cons_of_mem a hx))]

theorem flatMap_congr_mem {α β : Type} {f g : α → List β} :
    ∀ {l : List α}, (∀ a ∈ l, f a = g a) → l.flatMap f = l.flatMap g
  | [], _ => rfl
  | a :: l, h => by
    simp only [List.flatMap_cons, h a (by simp),
      flatMap_congr_mem (fun x hx => h x (List.mem_cons_of_mem a hx))]

/-- A flatMap that gives nothing for the elements `p` drops reads only the
elements `p` keeps. -/
theorem flatMap_filter_of_nil {α β : Type} {f : α → List β} {p : α → Bool} :
    ∀ {l : List α}, (∀ a ∈ l, p a = false → f a = []) → l.flatMap f = (l.filter p).flatMap f
  | [], _ => rfl
  | a :: l, h => by
    have ih := flatMap_filter_of_nil (fun x hx => h x (List.mem_cons_of_mem a hx))
    cases hp : p a
    · simp [hp, h a (by simp) hp, ih]
    · simp [hp, ih]

/-- Filtering by a test that a stronger test implies changes nothing of what
the stronger test keeps. -/
theorem filter_filter_of_imp {α : Type} {p w : α → Bool} {l : List α}
    (h : ∀ a ∈ l, p a = true → w a = true) : (l.filter w).filter p = l.filter p := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro a ha
  cases hp : p a
  · rfl
  · simp [h a ha hp]

theorem filter_eraseDups {α : Type} [BEq α] [LawfulBEq α] (p : α → Bool) (l : List α) :
    l.eraseDups.filter p = (l.filter p).eraseDups := by
  induction h : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
    cases l with
    | nil => rfl
    | cons a as =>
      have hlt : (as.filter fun b => !b == a).length < n := by
        rw [← h, List.length_cons]
        exact Nat.lt_succ_of_le (List.length_filter_le _ _)
      rw [List.eraseDups_cons]
      cases hp : p a
      · rw [List.filter_cons_of_neg (by simp [hp]), List.filter_cons_of_neg (by simp [hp]),
          ih _ hlt _ rfl, List.filter_filter]
        congr 1
        apply List.filter_congr
        intro x _
        cases hx : p x
        · rfl
        · have : (x == a) = false := by
            rw [beq_eq_false_iff_ne]
            intro e
            rw [e, hp] at hx
            cases hx
          simp [this]
      · rw [List.filter_cons_of_pos hp, List.filter_cons_of_pos hp, List.eraseDups_cons,
          ih _ hlt _ rfl, List.filter_filter, List.filter_filter]
        congr 2
        apply List.filter_congr
        intro x _
        rw [Bool.and_comm]

/-- Whether a row is among rows that pass `P` is, for a row that passes it,
whether it is among the rows. -/
theorem contains_filter {P : Row → Bool} {b : List Row} {r : Row} (hr : P r = true) :
    (b.filter P).contains r = b.contains r := by
  rw [Bool.eq_iff_iff, List.contains_iff_mem, List.contains_iff_mem, List.mem_filter]
  simp [hr]

theorem combine_filter (P : Row → Bool) (op : Combine) (a b : List Row) :
    (combine op a b).filter P = combine op (a.filter P) (b.filter P) := by
  cases op <;> simp only [combine]
  · exact List.filter_append a b
  · rw [filter_eraseDups, List.filter_append]
  · rw [filter_eraseDups, List.filter_filter, List.filter_filter]
    congr 1
    apply List.filter_congr
    intro r _
    cases hr : P r
    · simp
    · rw [contains_filter hr, Bool.true_and, Bool.and_true]
  · rw [filter_eraseDups, List.filter_filter, List.filter_filter]
    congr 1
    apply List.filter_congr
    intro r _
    cases hr : P r
    · simp
    · rw [contains_filter hr, Bool.true_and, Bool.and_true]

theorem combineAll_filter (P : Row → Bool) (ops : List Combine) (ls : List (List Row)) :
    (combineAll ops ls).filter P = combineAll ops (ls.map (·.filter P)) := by
  cases ls with
  | nil => rfl
  | cons first rest =>
    simp only [combineAll, List.map_cons]
    have key : ∀ (pairs : List (Combine × List Row)) (acc : List Row),
        (pairs.foldl (fun acc (op, rows) => combine op acc rows) acc).filter P =
          (pairs.map fun p => (p.1, p.2.filter P)).foldl
            (fun acc (op, rows) => combine op acc rows) (acc.filter P) := by
      intro pairs
      induction pairs with
      | nil => intro acc; rfl
      | cons p ps ih =>
        intro acc
        obtain ⟨op, rows⟩ := p
        simp only [List.foldl_cons, List.map_cons]
        rw [ih, combine_filter]
    rw [key, List.zip_map_right]
    rfl

/-- The ORDER BY's sort keeps the rows a test keeps: with no LIMIT and no
OFFSET, a query's rows that pass a test are its rows of those that do. -/
theorem post_filter {q : Query} (hl : q.limit = none) (ho : q.offset = none) (P : Row → Bool)
    (rows : List Row) : (q.post rows).filter P = q.post (rows.filter P) := by
  unfold Query.post
  simp only [hl, ho, Option.getD_none, List.drop_zero]
  split
  · rfl
  · have hlen : ∀ r : Row, (q.orderBy.map fun (p, _) => r.cell p).length =
        (q.orderBy.map (·.2)).length := by intro r; simp
    rw [Limit.sortBy_filter (fun a b => Limit.keyLe_total _ _ _)
      (fun a b c h1 h2 => Limit.keyLe_trans (hlen a) (hlen b) (hlen c) h1 h2)]

theorem insertSorted_congr {α : Type} {le le' : α → α → Bool} {x : α} :
    ∀ {l : List α}, (∀ y ∈ l, le x y = le' x y) → insertSorted le x l = insertSorted le' x l
  | [], _ => rfl
  | y :: ys, h => by
    simp only [insertSorted, h y (by simp),
      insertSorted_congr (fun z hz => h z (List.mem_cons_of_mem y hz))]

theorem sortBy_congr {α : Type} {le le' : α → α → Bool} :
    ∀ {l : List α}, (∀ a ∈ l, ∀ b ∈ l, le a b = le' a b) → sortBy le l = sortBy le' l
  | [], _ => rfl
  | x :: xs, h => by
    simp only [sortBy]
    rw [sortBy_congr (fun a ha b hb => h a (List.mem_cons_of_mem x ha) b (List.mem_cons_of_mem x hb))]
    exact insertSorted_congr fun y hy =>
      h x (by simp) y (List.mem_cons_of_mem x (mem_sortBy.mp hy))

/-! A core's rows read their subqueries only under the bindings it keeps. -/

theorem groupsOf_congr {key key' : Bind → List Value} {bs : List Bind}
    (h : ∀ b ∈ bs, key b = key' b) : groupsOf key bs = groupsOf key' bs := by
  unfold groupsOf
  rw [List.map_congr_left h]
  apply List.map_congr_left
  intro k _
  apply List.filter_congr
  intro b hb
  rw [h b hb]

theorem Core.rows_congr {c : Core} {E E' : Bind → Nat → List Row} {kept : List Bind}
    (h : ∀ b ∈ kept, ∀ e ∈ View.outExprs c, e.eval b (E b) = e.eval b (E' b)) :
    c.rows E kept = c.rows E' kept := by
  have hout : ∀ b ∈ kept, ∀ o ∈ c.output, ∀ e ∈ o.exprs, e.eval b (E b) = e.eval b (E' b) :=
    fun b hb o ho e he => h b hb e (by
      simp only [View.outExprs, List.mem_append, List.mem_flatMap]; exact Or.inl ⟨o, ho, he⟩)
  have hgroup : ∀ b ∈ kept, ∀ e ∈ c.groupBy, e.eval b (E b) = e.eval b (E' b) :=
    fun b hb e he => h b hb e (by simp [View.outExprs, he])
  have hg : ∀ g : List Bind, (∀ b ∈ g, b ∈ kept) →
      c.output.map (OutCol.aggregate E g) = c.output.map (OutCol.aggregate E' g) := by
    intro g hgk
    apply List.map_congr_left
    intro o ho
    cases o with
    | expr e =>
      simp only [OutCol.aggregate]
      cases hl : g.getLast? with
      | none => rfl
      | some b => exact hout b (hgk b (List.mem_of_getLast? hl)) _ ho e (by simp [OutCol.exprs])
    | count => rfl
    | max e =>
      simp only [OutCol.aggregate]
      congr 1
      exact List.map_congr_left fun b hb => hout b (hgk b hb) _ ho e (by simp [OutCol.exprs])
    | concat e orderBy =>
      have hsort : sortBindings E orderBy g = sortBindings E' orderBy g := by
        unfold sortBindings
        split
        · rfl
        · apply sortBy_congr
          intro a ha b hb
          have hkey : ∀ x ∈ g, (orderBy.map fun (e, _) => e.eval x (E x)) =
              orderBy.map fun (e, _) => e.eval x (E' x) := fun x hx =>
            List.map_congr_left fun (e', d) hed =>
              hout x (hgk x hx) _ ho e' (by
                simp only [OutCol.exprs, List.mem_cons, List.mem_map]
                exact Or.inr ⟨(e', d), hed, rfl⟩)
          simp only [hkey a ha, hkey b hb]
      have hmem : ∀ b ∈ sortBindings E' orderBy g, b ∈ g := by
        intro b hb
        unfold sortBindings at hb
        split at hb
        · exact hb
        · exact mem_sortBy.mp hb
      simp only [OutCol.aggregate, hsort]
      congr 1
      exact List.map_congr_left fun b hb =>
        hout b (hgk b (hmem b hb)) _ ho e (by simp [OutCol.exprs])
    | rowNumber => rfl
  unfold Core.rows
  split
  · split
    · exact congrArg (· :: []) (hg kept fun b hb => hb)
    · rw [groupsOf_congr (key' := fun b => c.groupBy.map (Expr.eval b (E' b)))
        (fun b hb => List.map_congr_left fun e he => hgroup b hb e he)]
      apply List.map_congr_left
      intro g hgm
      exact hg g fun b hb => mem_groupsOf hgm hb
  · apply List.map_congr_left
    intro ⟨b, j⟩ hbj
    have hb : b ∈ kept := List.fst_mem_of_mem_zipIdx hbj
    apply List.map_congr_left
    intro o ho
    cases o with
    | expr e => exact hout b hb _ ho e (by simp [OutCol.exprs])
    | rowNumber partition orderBy =>
      have hkey : ∀ exprs : List Expr, (∀ e ∈ exprs, e ∈ partition ++ orderBy.map (·.1)) →
          ∀ x ∈ kept, windowKey E exprs x = windowKey E' exprs x := fun exprs hin x hx =>
        List.map_congr_left fun e he => hout x hx _ ho e (by simpa [OutCol.exprs] using hin e he)
      have hpeer : ∀ x ∈ kept, ∀ o', windowPeer E partition orderBy b x o' =
          windowPeer E' partition orderBy b x o' := by
        intro x hx o'
        simp only [windowPeer, hkey partition (fun e he => List.mem_append_left _ he) x hx,
          hkey partition (fun e he => List.mem_append_left _ he) b hb,
          hkey (orderBy.map (·.1)) (fun e he => List.mem_append_right _ he) x hx,
          hkey (orderBy.map (·.1)) (fun e he => List.mem_append_right _ he) b hb]
      have h1 : kept.countP (windowPeer E partition orderBy b · .lt) =
          kept.countP (windowPeer E' partition orderBy b · .lt) :=
        List.countP_congr (fun x hx => by rw [hpeer x hx])
      have h2 : (kept.take j).countP (windowPeer E partition orderBy b · .eq) =
          (kept.take j).countP (windowPeer E' partition orderBy b · .eq) :=
        List.countP_congr (fun x hx => by rw [hpeer x (List.mem_of_mem_take hx)])
      simp only [OutCol.plain, windowRowNumber, h1, h2]
    | _ => rfl

theorem Core.result_congr {c : Core} {E E' : Bind → Nat → List Row} {kept : List Bind}
    (h : ∀ b ∈ kept, ∀ q ∈ c.subqueries, E b q = E' b q) : c.result E kept = c.result E' kept := by
  have hrows : c.rows E kept = c.rows E' kept := by
    apply Core.rows_congr
    intro b hb e he
    apply e.eval_congr
    · intro _ _ _; rfl
    · intro q hq
      apply h b hb q
      unfold Core.subqueries
      simp only [View.outExprs, List.mem_append, List.mem_flatMap] at he
      refine List.mem_eraseDups.mpr ?_
      rcases he with ⟨o, ho, he⟩ | he
      · exact List.mem_append_left _ (List.mem_append_left _
          (List.mem_flatMap.mpr ⟨o, ho, List.mem_flatMap.mpr ⟨e, he, hq⟩⟩))
      · exact List.mem_append_right _ (List.mem_flatMap.mpr ⟨e, he, hq⟩)
  unfold Core.result
  rw [hrows]

end SqliveNarrowing
