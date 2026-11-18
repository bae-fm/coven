import SqliveNarrowing.Reader

/-! Rule 4: a condition on a derived item's rows (a FROM subquery, a CTE or
a view) passes into each of its arms that neither groups, aggregates nor
calls a window function, when
its query has no LIMIT and no OFFSET: an output column that is a bare column of the arm's
own item holds that item's value, so a term or an edge on the column holds of
the item's row (`derived.rs`, `push_down`). The target is then an inner
join's, as a WHERE condition in the arm would make it, except that a term a
row of NULLs can pass narrows nothing on a LEFT JOIN's right side. -/
namespace SqliveNarrowing.PushDown

/-- For a derived item, each arm a condition on its rows passes into: the
item and column each output column passes on unchanged. `none` for a table;
no arm when the query has a LIMIT or an OFFSET. -/
def arms (s : Statement) (i : Nat) : Option (List (List (Option (Nat × Nat)))) :=
  match (s.item i).kind with
  | .table _ => none
  | .derived q _ =>
    let query := s.query q
    if query.limit.isSome || query.offset.isSome then some []
    else some <| (query.arms.filter fun k => !(s.core k).aggregated && !(s.core k).windowed).map fun k =>
      (s.core k).output.map fun
        | .expr (.col j c) => if (s.item j).core = k then some (j, c) else none
        | _ => none

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
        let reached := tested ++ edges.map (·.column)
        let targets := (reached.filterMap fun c => (through c).map (·.1)).eraseDups
        targets.foldl (fun st target =>
          let rename (c : Nat) : Option Nat :=
            match through c with
            | some (t, c') => if t = target then some c' else none
            | none => none
          let left := (st.items target).leftOn.isSome
          let renamed := (term.bind (·.renamed rename)).filter
            fun r => !(left && r.mayHoldOnNulls)
          let passed := edges.filterMap fun e =>
            (rename e.column).map fun c' => { e with member := target, column := c' }
          let st := match renamed with
            | some r => st.addTerm target r
            | none => st
          let st := { st with edges := st.edges ++ passed }
          let st := if renamed.isSome || !passed.isEmpty then st.joinInner narrowFuel target else st
          pushDown s narrowFuel fuel st target renamed passed) st) st

/-! The proof: a row of a derived item's query is a row of one of its arms,
and a row of an arm that doesn't aggregate holds, in each column the arm
passes on, the value of the item it passes on. So a term or an edge true of
the derived row is true of that item's row. -/

/-- A term over a row whose columns `rename` passes on to another row holds
of that row as renamed. -/
theorem renamed_sound {t t' : STerm} {rename : Nat → Option Nat} (h : t.renamed rename = some t')
    {r : Row} {row : Option Row} (hcols : ∀ c c', rename c = some c' → r.cell c = cellOf row c')
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
  | row i cd => simp [STerm.renamed] at h
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
    obtain ⟨b', hb', j, rfl⟩ := mem_rows_plain hagg hr
    refine ⟨b', hb', fun c j c' hc => ?_⟩
    simp [Row.cell, List.getElem?_map, hc, Expr.eval, OutCol.plain]

end SqliveNarrowing.PushDown
