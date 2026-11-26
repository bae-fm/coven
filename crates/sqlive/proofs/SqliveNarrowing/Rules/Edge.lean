import SqliveNarrowing.Rules.Passed

/-! Rule 2: an equality between a value of one of a core's items and a value
of one other item narrows the first to the rows whose value is one some row
of the other gives, in its own scope (`Planner::edge`).

The member is a column sqlive narrows by, or an expression over one table
item's own columns (`RowMember`); the partner is readable alone, since a
write's lookup reads it alone; a bare partner column has a member column's
affinity, or none (BLOB), whose values SQLite gives the member's affinity, and
one written first has an affinity at all, so the equality compares by
BINARY. SQLite gives an expression member's value the partner
column's affinity, so a partner column of any affinity gives it values. An expression partner is strict, NULL where
the partner's columns are, so a missing partner row gives no value, or the
partner is no item a LEFT JOIN adds, so every binding a core keeps holds its
row: a COALESCE, a CASE or `IS NULL` over a LEFT JOIN's item gives no edge
(`OneOf` reads a column equal to a COALESCE). Both directions of `a.x = b.y`
are read. A column of a derived item a lookup can't read alone gives the
edge to the source its arm passes the column on from, through the item, when
the item sits in the member's core (`Passed`, `orPassed`). -/
namespace SqliveNarrowing.Edge

/-- The edge `member = other` gives, written in core `k`, with `other` first
when `first`. -/
def edge (s : Statement) (k : Nat) (member other : Expr) (first : Bool) : Option Edge := do
  let (m, memberValue, affinity) ← RowMember.read s k member
  if !other.queries.isEmpty then none
  let p ← other.items.head?
  if !other.items.all (· = p) ∨ p = m ∨ !(s.item p).readable ∨
    !(other.strict || s.alwaysBound p) then none
  let value ← match other with
    | .col _ pc =>
      match s.affinity p pc, memberValue with
      | some pa, .column _ =>
        -- A partner column of no affinity (BLOB) takes the member's
        -- (`columnValue`).
        if pa = affinity then some (PValue.column pc)
        else if pa = .blob then some (PValue.expr other) else none
      -- SQLite gives an expression's value the column's affinity.
      | some _, .expr _ => some (PValue.column pc)
      | none, _ => if first then none else some (PValue.expr other)
    | _ => some (PValue.expr other)
  pure { member := m, memberValue, affinity, partner := p, value, nested := false }

/-- The edge `member = d.pc` gives, written in core `k` with the column
first when `first`, when `d` is an item of the core a lookup can't read
alone: through `d`, to the source its arm passes the column on from
(`Planner::column_edge`). A column written first has an affinity, so the
equality compares by BINARY. -/
def throughEdge (s : Statement) (k : Nat) (member other : Expr) (first : Bool) : Option Edge :=
  match other with
  | .col d pc =>
    if (s.item d).readable ∨ (s.item d).core ≠ k ∨ (first ∧ s.affinity d pc = none) then none
    else Passed.edge s k member d pc
  | _ => none

/-- The edge `member = other` gives: `edge`'s, or else `throughEdge`'s. -/
def orPassed (s : Statement) (k : Nat) (member other : Expr) (first : Bool) : Option Edge :=
  (edge s k member other first).orElse fun _ => throughEdge s k member other first

/-- The reader: `a = b`, with no subquery, naming more than one item. -/
def readCondition : ReadCondition := fun s k cd =>
  match cd with
  | .cmp .eq l r =>
    if !cd.queries.isEmpty ∨ cd.items.eraseDups.length = 1 then none
    else some { edges := (orPassed s k l r false).toList ++ (orPassed s k r l true).toList }
  | _ => none

/-! The proof: the equality true of a binding makes the member's column equal
to the value its partner's row in the binding gives, neither NULL. -/

/-- What an edge `edge` reads says of the condition's sides. -/
theorem edge_facts {s : Statement} {k : Nat} {member other : Expr} {first : Bool} {e : Edge}
    (h : edge s k member other first = some e) :
    (∀ b sub, e.memberValue.on (b e.member) = member.eval b sub) ∧ (s.item e.member).core = k ∧
    e.nested = false ∧ e.via = none ∧
    other.queries = [] ∧ (∀ j ∈ other.items, j = e.partner) ∧ other.refs ≠ [] ∧
    (other.strict = true ∨ s.alwaysBound e.partner = true) ∧ e.partner ≠ e.member ∧
    (s.item e.partner).readable ∧ (∀ r, e.value.eval r = other.eval (rowBind (some r)) noSubs) ∧
    e.value.strict = other.strict := by
  unfold edge at h
  simp only [bind, Option.bind] at h
  cases hmo : RowMember.read s k member with
  | none => simp [hmo] at h
  | some found =>
    obtain ⟨m, mv, aff⟩ := found
    obtain ⟨hk', hmv, _⟩ := RowMember.row_member_sound hmo
    simp only [hmo] at h
    split at h
    · cases h
    rename_i hq
    replace hq : other.queries = [] := by simpa using hq
    cases hp : other.items.head? with
    | none => simp [hp] at h
    | some p =>
      simp only [hp] at h
      split at h
      · cases h
      rename_i hbad
      simp only [not_or, Bool.not_eq_true] at hbad
      have hitems : ∀ j ∈ other.items, j = p := by
        have h1 := hbad.1
        simpa using h1
      have hrefs : other.refs ≠ [] := by
        intro hnil
        simp [Expr.items, hnil] at hp
      have hread : (s.item p).readable = true := by simpa using hbad.2.2.1
      have hstrict : other.strict = true ∨ s.alwaysBound p = true := by
        cases hs : other.strict
        · exact Or.inr (by simpa [hs] using hbad.2.2.2)
        · exact Or.inl rfl
      have hcol : ∀ j pc, other = .col j pc →
          ∀ r, (PValue.column pc).eval r = other.eval (rowBind (some r)) noSubs := by
        intro j pc ho r
        subst ho
        have : j = p := hitems j (by simp [Expr.items, Expr.refs])
        subst this
        simp [PValue.eval, Expr.eval, rowBind, cellOf]
      split at h
      · rename_i j pc
        split at h
        · split at h
          · cases h
            exact ⟨hmv, hk', rfl, rfl, hq, hitems, hrefs, hstrict, hbad.2.1, hread, hcol j pc rfl, rfl⟩
          · split at h
            · cases h
              exact ⟨hmv, hk', rfl, rfl, hq, hitems, hrefs, hstrict, hbad.2.1, hread, fun r => rfl, rfl⟩
            · cases h
        · cases h
          exact ⟨hmv, hk', rfl, rfl, hq, hitems, hrefs, hstrict, hbad.2.1, hread, hcol j pc rfl, rfl⟩
        · split at h
          · cases h
          · cases h
            exact ⟨hmv, hk', rfl, rfl, hq, hitems, hrefs, hstrict, hbad.2.1, hread, fun r => rfl, rfl⟩
      · cases h
        exact ⟨hmv, hk', rfl, rfl, hq, hitems, hrefs, hstrict, hbad.2.1, hread, fun r => rfl, rfl⟩

/-- An edge's member value is NULL for a missing row, unless every binding
holds the member's row. -/
theorem edge_memberStrict {s : Statement} {k : Nat} {member other : Expr} {first : Bool}
    {e : Edge} (h : edge s k member other first = some e) : e.memberStrict s := by
  unfold edge at h
  simp only [bind, Option.bind] at h
  cases hmo : RowMember.read s k member with
  | none => simp [hmo] at h
  | some found =>
    obtain ⟨m, mv, aff⟩ := found
    obtain ⟨_, _, hnone⟩ := RowMember.row_member_sound hmo
    simp only [hmo] at h
    split at h
    · cases h
    cases hp : other.items.head? with
    | none => simp [hp] at h
    | some p =>
      simp only [hp] at h
      split at h
      · cases h
      repeat' (first | (split at h) | (cases h; exact hnone) | (cases h))

/-- An edge's partner is an item its other side names. -/
theorem edge_partner_mem {s : Statement} {k : Nat} {member other : Expr} {first : Bool} {e : Edge}
    (h : edge s k member other first = some e) : e.partner ∈ other.items := by
  obtain ⟨_, _, _, _, _, hitems, hrefs, _⟩ := edge_facts h
  cases hr : other.refs with
  | nil => exact absurd hr hrefs
  | cons p ps =>
    have hp : p.1 ∈ other.items := by simp [Expr.items, hr]
    rw [← hitems p.1 hp]
    exact hp

/-- The items each side of a comparison names are items it names. -/
theorem items_left {op : Op} {l r : Expr} : ∀ j ∈ l.items, j ∈ (Cond.cmp op l r).items := by
  intro j hj
  simp only [Expr.items, List.mem_map] at hj
  obtain ⟨p, hp, rfl⟩ := hj
  simp only [Cond.items, Cond.refs, List.mem_map, List.mem_append]
  exact ⟨p, Or.inl hp, rfl⟩

theorem items_right {op : Op} {l r : Expr} : ∀ j ∈ r.items, j ∈ (Cond.cmp op l r).items := by
  intro j hj
  simp only [Expr.items, List.mem_map] at hj
  obtain ⟨p, hp, rfl⟩ := hj
  simp only [Cond.items, Cond.refs, List.mem_map, List.mem_append]
  exact ⟨p, Or.inr hp, rfl⟩

/-- An edge holds of a binding where its member equals its partner's value. -/
theorem edge_holds {s : Statement} {k : Nat} {member other : Expr} {first : Bool} {e : Edge}
    (h : edge s k member other first = some e) {db : Db} {f : Nat} {b : Bind} {sub : Subs}
    {x : Int} (hm : member.eval b sub = some x) (ho : other.eval b sub = some x) :
    e.HoldsIn s db f b := by
  obtain ⟨hmem, _, hn, _, hq, hitems, hrefs, hst, _, _, hval, hvs⟩ := edge_facts h
  rw [other.eval_rowBind hitems hq] at ho
  cases hb : b e.partner with
  | none =>
    -- A strict value is NULL where the partner has no row; any other value
    -- has a partner that every binding a core keeps holds a row of.
    cases hs : other.strict with
    | true =>
      rw [hb, other.eval_null hrefs hq hs (fun _ _ _ => by simp [rowBind, cellOf])] at ho
      cases ho
    | false =>
      have hab : s.alwaysBound e.partner = true := by simpa [hs] using hst
      exact Or.inr (Or.inr (Or.inl ⟨by rw [hvs, hs], hn, hab, hb⟩))
  | some r =>
    refine Or.inl ⟨x, by rw [hmem b sub]; exact hm, ?_⟩
    · simp only [hn, Bool.false_eq_true, ↓reduceIte]
      exact ⟨r, hb, by rw [hval, ← hb, ho]⟩

/-- Why `edge` reads an expression that isn't strict only over an item no LEFT
JOIN adds: COALESCE, a CASE and `IS NULL` each give a value where the item
they name has no row, as a LEFT JOIN's missing row, so an equality with one
holds of a binding where the partner has no row for a lookup to find. -/
theorem nonstrict_holds_without_partner {m c p pc : Nat} {r : Row} (hr : r.cell c = some 1)
    (hpm : p ≠ m) (sub : Subs) :
    ∀ other ∈ [Expr.coalesce (.col p pc) (.lit (some 1)),
      .iif (.col p pc) (.lit (some 2)) (.lit (some 1)), .isNull (.col p pc)],
      other.strict = false ∧ (Bind.empty.set m (some r)) p = none ∧
      isTrue ((Cond.cmp .eq (.col m c) other).eval (Bind.empty.set m (some r)) sub) := by
  intro other ho
  simp only [List.mem_cons, List.not_mem_nil, or_false] at ho
  rcases ho with rfl | rfl | rfl <;>
    simp [Expr.strict, Cond.eval, Expr.eval, Bind.set, Bind.empty, cellOf, hr, hpm, Op.apply,
      Op.compare, isTrue]

/-- An edge `throughEdge` reads is `Passed.edge`'s, through an item of the
member's core that the other side is a column of. -/
theorem throughEdge_facts {s : Statement} {k : Nat} {member other : Expr} {first : Bool} {e : Edge}
    (h : throughEdge s k member other first = some e) :
    ∃ d pc, other = .col d pc ∧ (s.item d).core = k ∧ Passed.edge s k member d pc = some e := by
  unfold throughEdge at h
  split at h
  · rename_i d pc
    split at h
    · cases h
    · rename_i hok
      simp only [not_or, not_and, Bool.not_eq_true, ne_eq, Decidable.not_not] at hok
      exact ⟨d, pc, rfl, hok.2.1, h⟩
  · cases h

/-- What an edge `orPassed` reads says of it, where the condition names the
items the other side names: its member is in the core, its partner is
readable alone and, outside a subquery, another item the condition names;
an edge through a derived item reads through one of the member's core; and
the edge holds of each binding where the member equals the other side. -/
theorem orPassed_sound {s : Statement} {k : Nat} {member other : Expr} {first : Bool} {e : Edge}
    (h : orPassed s k member other first = some e) {cd : Cond}
    (hnamed : ∀ j ∈ other.items, j ∈ cd.items) :
    (s.item e.member).core = k ∧ (e.nested = false → e.partner ≠ e.member) ∧
    (e.nested = false → e.partner ∈ cd.items) ∧ (s.item e.partner).readable ∧ e.memberStrict s ∧
    e.NestedIn s cd.queries ∧
    ∀ db f b sub x, member.eval b sub = some x → other.eval b sub = some x → e.HoldsIn s db f b := by
  unfold orPassed at h
  cases he : edge s k member other first with
  | some e' =>
    rw [he] at h
    cases h
    obtain ⟨_, hk, hnest, hvia, _, _, _, _, hne, hread, _⟩ := edge_facts he
    exact ⟨hk, fun _ => hne, fun _ => hnamed _ (edge_partner_mem he), hread, edge_memberStrict he,
      Edge.nestedIn_of hvia fun h => by simp [hnest] at h,
      fun db f b sub x hm ho => edge_holds he hm ho⟩
  | none =>
    rw [he] at h
    obtain ⟨d, pc, rfl, hd, hp⟩ := throughEdge_facts h
    obtain ⟨_, hk, hvia, hpassed, hread, hstrict, _⟩ := Passed.edge_facts hp
    refine ⟨hk, fun hn => by simp [hpassed.1] at hn, fun hn => by simp [hpassed.1] at hn, hread,
      hstrict, ⟨fun _ hv => (by rw [hvia] at hv; cases hv), fun d' pc' hv => ?_⟩,
      fun db f b sub x hm ho => ?_⟩
    · rw [hvia] at hv
      obtain ⟨rfl, rfl⟩ := Prod.mk.inj (Option.some.inj hv)
      exact ⟨hpassed, Or.inl (by rw [hd, hk])⟩
    · simp only [Expr.eval, cellOf] at ho
      cases hbd : b d with
      | none => rw [hbd] at ho; cases ho
      | some rd =>
        rw [hbd] at ho
        exact Passed.edge_holds hp hm (Or.inl ⟨hd, rd, hbd, ho⟩)

/-- Rule 2's theorem: an equality true of a binding makes each edge sqlive
reads from it hold. -/
theorem edge_sound : ReadCondition.Sound readCondition where
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
      have hside : ∀ {db f b}, isTrue ((Cond.cmp .eq l r).eval b (evalQuery s db f b)) →
          ∃ x, l.eval b (evalQuery s db f b) = some x ∧ r.eval b (evalQuery s db f b) = some x := by
        intro db f b hc
        simp only [Cond.eval, isTrue, beq_iff_eq] at hc
        cases hl : l.eval b (evalQuery s db f b) <;> cases hr : r.eval b (evalQuery s db f b) <;>
          simp_all [Op.apply, Op.compare]
      simp only [List.mem_append, Option.mem_toList] at he
      rcases he with he | he
      · obtain ⟨hk, hne, hin, hread, hstrict, hnest, hh⟩ := orPassed_sound he (items_right (op := .eq))
        refine ⟨hk, hne, hin, hread, hstrict, hnest, fun db f b hc => ?_⟩
        obtain ⟨x, hl, hr⟩ := hside hc
        exact hh db f b _ x hl hr
      · obtain ⟨hk, hne, hin, hread, hstrict, hnest, hh⟩ := orPassed_sound he (items_left (op := .eq))
        refine ⟨hk, hne, hin, hread, hstrict, hnest, fun db f b hc => ?_⟩
        obtain ⟨x, hl, hr⟩ := hside hc
        exact hh db f b _ x hr hl
    · cases h

end SqliveNarrowing.Edge
