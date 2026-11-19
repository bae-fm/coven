import SqliveNarrowing.Reader

/-! Rule 2: an equality between a column of one of a core's items and a value
of one other item narrows the first to the rows whose column holds a value
some row of the other holds, in its own scope (`Planner::edge_between`).

The member is a column sqlive narrows by; the partner is readable alone,
since a write's lookup reads it alone; a bare partner column has the
member's affinity, and one written first has an affinity at all, so the
equality compares by BINARY. Both directions of `a.x = b.y` are read. -/
namespace SqliveNarrowing.Edge

/-- The edge `member = other` gives, written in core `k`, with `other` first
when `first`. -/
def edge (s : Statement) (k : Nat) (member other : Expr) (first : Bool) : Option Edge := do
  let .col m c := member | none
  if (s.item m).core ≠ k then none
  let affinity ← s.affinity m c
  if !other.queries.isEmpty then none
  let p ← other.items.head?
  if !other.items.all (· = p) ∨ p = m ∨ !(s.item p).readable then none
  let value ← match other with
    | .col _ pc =>
      match s.affinity p pc with
      | some pa => if pa = affinity then some (PValue.column pc) else none
      | none => if first then none else some (PValue.expr other)
    | _ => some (PValue.expr other)
  pure { member := m, column := c, affinity, partner := p, value, nested := false }

/-- The reader: `a = b`, with no subquery, naming more than one item. -/
def readCondition : ReadCondition := fun s k cd =>
  match cd with
  | .cmp .eq l r =>
    if !cd.queries.isEmpty ∨ cd.items.eraseDups.length = 1 then none
    else some { edges := (edge s k l r false).toList ++ (edge s k r l true).toList }
  | _ => none

/-! The proof: the equality true of a binding makes the member's column equal
to the value its partner's row in the binding gives, neither NULL. -/

/-- What an edge `edge` reads says of the condition's sides. -/
theorem edge_facts {s : Statement} {k : Nat} {member other : Expr} {first : Bool} {e : Edge}
    (h : edge s k member other first = some e) :
    member = .col e.member e.column ∧ (s.item e.member).core = k ∧ e.nested = false ∧
    other.queries = [] ∧ (∀ j ∈ other.items, j = e.partner) ∧ other.refs ≠ [] ∧
    e.partner ≠ e.member ∧ (s.item e.partner).readable ∧
    ∀ r, e.value.eval r = other.eval (rowBind (some r)) noSubs := by
  unfold edge at h
  cases member
  case lit | add | greatest | sub => simp at h
  case col m c =>
    simp only [bind, Option.bind] at h
    split at h
    · cases h
    rename_i hk
    have hk' : (s.item m).core = k := by simpa using hk
    cases haff : s.affinity m c with
    | none => simp [haff] at h
    | some aff =>
      simp only [haff] at h
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
        have hread : (s.item p).readable = true := by simpa using hbad.2.2
        split at h
        · rename_i j pc
          split at h
          · split at h
            · cases h
              refine ⟨rfl, hk', rfl, hq, hitems, hrefs, hbad.2.1, hread, ?_⟩
              intro r
              have : j = p := hitems j (by simp [Expr.items, Expr.refs])
              subst this
              simp [PValue.eval, Expr.eval, rowBind, cellOf]
            · cases h
          · split at h
            · cases h
            · cases h
              exact ⟨rfl, hk', rfl, hq, hitems, hrefs, hbad.2.1, hread, fun r => rfl⟩
        · cases h
          exact ⟨rfl, hk', rfl, hq, hitems, hrefs, hbad.2.1, hread, fun r => rfl⟩

/-- An edge holds of a binding where its member equals its partner's value. -/
theorem edge_holds {s : Statement} {k : Nat} {member other : Expr} {first : Bool} {e : Edge}
    (h : edge s k member other first = some e) {db : Db} {f : Nat} {b : Bind} {sub : Subs}
    {x : Int} (hm : member.eval b sub = some x) (ho : other.eval b sub = some x) :
    e.HoldsIn s db f b := by
  obtain ⟨hmem, _, hn, hq, hitems, hrefs, _, _, hval⟩ := edge_facts h
  refine Or.inl ⟨x, ?_, ?_⟩
  · rw [hmem] at hm
    simpa [Expr.eval] using hm
  · simp only [hn, Bool.false_eq_true, ↓reduceIte]
    rw [other.eval_rowBind hitems hq] at ho
    cases hb : b e.partner with
    | none =>
      rw [hb, other.eval_null hrefs hq (fun _ _ _ => by simp [rowBind, cellOf])] at ho
      cases ho
    | some r => exact ⟨r, rfl, by rw [hval, ← hb, ho]⟩

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
      · obtain ⟨_, hk, _, _, _, _, hne, hread, _⟩ := edge_facts he
        refine ⟨hk, fun _ => hne, hread, fun db f b hc => ?_⟩
        obtain ⟨x, hl, hr⟩ := hside hc
        exact edge_holds he hl hr
      · obtain ⟨_, hk, _, _, _, _, hne, hread, _⟩ := edge_facts he
        refine ⟨hk, fun _ => hne, hread, fun db f b hc => ?_⟩
        obtain ⟨x, hl, hr⟩ := hside hc
        exact edge_holds he hr hl
    · cases h

end SqliveNarrowing.Edge
