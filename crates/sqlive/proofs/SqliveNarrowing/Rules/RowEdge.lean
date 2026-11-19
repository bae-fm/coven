import SqliveNarrowing.Rules.Edge
import SqliveNarrowing.Rules.RowSubqueryEdge

/-! A row value equality between items, `(a.x1, ..., a.xn) = (b.y1, ...,
b.yn)`, narrows as the equalities of its pairs: it has the truth of the AND
of `a.xi = b.yi`, NULLs included, so each pair gives the edges an equality
between two items gives (`read_condition`, `Edge.edge`). A pair naming one
item only, or whose sides compare by another affinity or collation, gives
none. -/
namespace SqliveNarrowing.RowEdge

/-- The reader: `(l1, ..., ln) = (r1, ..., rn)`, with no subquery, naming
more than one item, each pair's edges both ways. -/
def readCondition : ReadCondition := fun s k cd =>
  match cd with
  | .rowEq ls rs =>
    if !cd.queries.isEmpty ∨ cd.items.eraseDups.length = 1 then none
    else some { edges := (ls.zip rs).flatMap fun (l, r) =>
      (Edge.edge s k l r false).toList ++ (Edge.edge s k r l true).toList }
  | _ => none

/-- A row value equal to another: each pair's sides hold one value. -/
theorem rowEqTruth_zip {f : Expr → Value} :
    ∀ {ls rs : List Expr}, rowEqTruth (ls.map f) (rs.map f) = some true →
      ∀ {l r}, (l, r) ∈ ls.zip rs → ∃ x, f l = some x ∧ f r = some x
  | l' :: ls, r' :: rs, h, l, r, hm => by
    simp only [List.map_cons, rowEqTruth] at h
    obtain ⟨h1, h2⟩ := RowSubqueryEdge.tand_true h
    simp only [List.zip_cons_cons, List.mem_cons, Prod.mk.injEq] at hm
    rcases hm with ⟨rfl, rfl⟩ | hm
    · revert h1
      cases f l <;> cases f r <;> simp [Op.apply, Op.compare]
      omega
    · exact rowEqTruth_zip h2 hm
  | [], _, _, _, _, hm => by simp at hm
  | _ :: _, [], _, _, _, hm => by simp at hm

/-- The theorem: a row value equality true of a binding makes each edge
sqlive reads from its pairs hold. -/
theorem row_edge_sound : ReadCondition.Sound readCondition where
  terms := by
    intro s k cd n h i t ht
    unfold readCondition at h
    split at h
    · split at h
      · cases h
      · cases h; simp at ht
    · cases h
  edges := by
    intro s k cd n h e he
    unfold readCondition at h
    split at h
    · rename_i ls rs
      split at h
      · cases h
      cases h
      simp only [List.mem_flatMap, List.mem_append, Option.mem_toList] at he
      obtain ⟨⟨l, r⟩, hpair, he⟩ := he
      have hside : ∀ {db f b}, isTrue ((Cond.rowEq ls rs).eval b (evalQuery s db f b)) →
          ∃ x, l.eval b (evalQuery s db f b) = some x ∧ r.eval b (evalQuery s db f b) = some x := by
        intro db f b hc
        simp only [Cond.eval, isTrue, beq_iff_eq] at hc
        exact rowEqTruth_zip hc hpair
      rcases he with he | he
      · obtain ⟨_, hk, _, _, _, _, _, hne, hread, _⟩ := Edge.edge_facts he
        refine ⟨hk, fun _ => hne, hread, fun db f b hc => ?_⟩
        obtain ⟨x, hl, hr⟩ := hside hc
        exact Edge.edge_holds he hl hr
      · obtain ⟨_, hk, _, _, _, _, _, hne, hread, _⟩ := Edge.edge_facts he
        refine ⟨hk, fun _ => hne, hread, fun db f b hc => ?_⟩
        obtain ⟨x, hl, hr⟩ := hside hc
        exact Edge.edge_holds he hr hl
    · cases h

end SqliveNarrowing.RowEdge
