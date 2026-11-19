import SqliveNarrowing.Rules.Edge

/-! `IS` between items, `a.x IS b.y`, narrows as the equality does, except
that a NULL matches a NULL: the member's column holds a value a row of the
partner gives, NULL included (`Planner::edge`, `nulls`). The edge's lookup
asks for a NULL the same way. The partner is no item a LEFT JOIN adds: a
left row no right row joins gives NULL in each of the right item's columns
without a row of it, which no lookup finds. An `IS` edge can hold of a row
of NULLs in its member's place, so it never makes a LEFT JOIN an inner
join's (`PState.narrow`). -/
namespace SqliveNarrowing.IsEdge

/-- The edge `member IS other` gives, written in core `k`, with `other`
first when `first`: the equality's, comparing by `IS`, when the partner is
no item a LEFT JOIN adds. -/
def edge (s : Statement) (k : Nat) (member other : Expr) (first : Bool) : Option Edge := do
  let e ← Edge.edge s k member other first
  if !s.alwaysBound e.partner then none
  pure { e with nulls := true }

/-- The reader: `a IS b`, with no subquery, naming more than one item. -/
def readCondition : ReadCondition := fun s k cd =>
  match cd with
  | .cmp .is l r =>
    if !cd.queries.isEmpty ∨ cd.items.eraseDups.length = 1 then none
    else some { edges := (edge s k l r false).toList ++ (edge s k r l true).toList }
  | _ => none

/-! The proof: `IS` true of a binding makes its sides one value. A value
that isn't NULL gives the equality's edge; a NULL member has a partner whose
value is NULL, in its row or for want of one. -/

theorem edge_facts {s : Statement} {k : Nat} {member other : Expr} {first : Bool} {e : Edge}
    (h : edge s k member other first = some e) :
    ∃ e', Edge.edge s k member other first = some e' ∧ e = { e' with nulls := true } ∧
      s.alwaysBound e'.partner = true := by
  unfold edge at h
  simp only [bind, Option.bind] at h
  cases he : Edge.edge s k member other first with
  | none => simp [he] at h
  | some e' =>
    simp only [he] at h
    split at h
    · cases h
    · rename_i hleft
      cases h
      exact ⟨e', rfl, rfl, by simpa using hleft⟩

/-- An `IS` edge holds of a binding where its sides hold one value. -/
theorem edge_holds {s : Statement} {k : Nat} {member other : Expr} {first : Bool} {e : Edge}
    (h : edge s k member other first = some e) {db : Db} {f : Nat} {b : Bind} {sub : Subs}
    {v : Value} (hm : member.eval b sub = v) (ho : other.eval b sub = v) :
    e.HoldsIn s db f b := by
  obtain ⟨e', he', rfl, hleft⟩ := edge_facts h
  obtain ⟨hmem, _, hn, hq, hitems, hrefs, hst, _, _, hval, hvs⟩ := Edge.edge_facts he'
  have hmv : cellOf (b e'.member) e'.column = v := by
    rw [hmem] at hm
    simpa [Expr.eval] using hm
  have hov : ∀ r, b e'.partner = some r → e'.value.eval r = v := by
    intro r hb
    rw [hval, ← hb, ← other.eval_rowBind hitems hq]
    exact ho
  cases v with
  | some x =>
    cases hb : b e'.partner with
    | none =>
      cases hs : other.strict with
      | true =>
        rw [other.eval_rowBind hitems hq, hb,
          other.eval_null hrefs hq hs (fun _ _ _ => by simp [rowBind, cellOf])] at ho
        cases ho
      | false => exact Or.inr (Or.inr ⟨by simp [hvs, hs], hn, hleft, hb⟩)
    | some r =>
      refine Or.inl ⟨x, hmv, ?_⟩
      simp only [hn, Bool.false_eq_true, ↓reduceIte]
      exact ⟨r, hb, hov r hb⟩
  | none => exact Or.inr (Or.inl ⟨rfl, hn, hleft, hmv, hov⟩)

/-- The theorem: `IS` true of a binding makes each edge sqlive reads from it
hold, a NULL member matching a partner's NULL. -/
theorem is_edge_sound : ReadCondition.Sound readCondition where
  terms := by
    intro s k cd n h i t ht
    unfold readCondition at h
    split at h
    · split at h
      · cases h
      · cases h
        simp at ht
    · cases h
  edges := by
    intro s k cd n h e he
    unfold readCondition at h
    split at h
    · rename_i l r
      split at h
      · cases h
      cases h
      have hside : ∀ {db f b}, isTrue ((Cond.cmp .is l r).eval b (evalQuery s db f b)) →
          l.eval b (evalQuery s db f b) = r.eval b (evalQuery s db f b) := by
        intro db f b hc
        simpa [Cond.eval, isTrue, Op.apply] using hc
      simp only [List.mem_append, Option.mem_toList] at he
      rcases he with he | he
      · obtain ⟨e', he', rfl, _⟩ := edge_facts he
        obtain ⟨_, hk, _, _, _, _, _, hne, hread, _⟩ := Edge.edge_facts he'
        refine ⟨hk, fun _ => hne, hread, fun db f b hc => ?_⟩
        exact edge_holds he rfl (hside hc).symm
      · obtain ⟨e', he', rfl, _⟩ := edge_facts he
        obtain ⟨_, hk, _, _, _, _, _, hne, hread, _⟩ := Edge.edge_facts he'
        refine ⟨hk, fun _ => hne, hread, fun db f b hc => ?_⟩
        exact edge_holds he rfl (hside hc)
    · cases h

end SqliveNarrowing.IsEdge
