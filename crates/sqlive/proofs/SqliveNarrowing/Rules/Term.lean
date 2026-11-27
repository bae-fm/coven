import SqliveNarrowing.Reader

/-! Rule 1: a condition on one item's own columns narrows that item to the
rows it holds for (`scope_term.rs`, `scope_row.rs`).

sqlive reads a comparison of a column with a constant, an IN list of
constants, a NULL test, and AND and OR of these as its own term; any other
condition on a table's columns, each a column it narrows by, as a condition
SQLite judges row by row. A condition naming one item of an outer core
narrows nothing. -/
namespace SqliveNarrowing.Term

/-- The column `e` names and its affinity, when it is a column of item `i`
that a condition can narrow by. -/
def column (s : Statement) (i : Nat) : Expr → Option Nat
  | .col j c => if j = i ∧ (s.affinity i c).isSome then some c else none
  | _ => none

/-- A constant spelled in the SQL. -/
def operand : Expr → Option Int
  | .lit (some v) => some v
  | _ => none

/-- `e IS NULL` or `e IS NOT NULL` written with `IS` and a NULL constant, as
a term on the column `e` names (`null_test`). -/
def nullOf (s : Statement) (i : Nat) : Op → Expr → Expr → Option STerm
  | .is, e, .lit none => (column s i e).map (STerm.null · false)
  | .is, .lit none, e => (column s i e).map (STerm.null · false)
  | .isNot, e, .lit none => (column s i e).map (STerm.null · true)
  | .isNot, .lit none, e => (column s i e).map (STerm.null · true)
  | _, _, _ => none

/-- The ordering a comparison operator reads as, reversed when the column is
on the right; `none` for an operator sqlive doesn't read. -/
def ordering (op : Op) (reverse : Bool) : Option Op :=
  match op, reverse with
  | .eq, _ | .is, _ => some .eq
  | .lt, false | .gt, true => some .lt
  | .le, false | .ge, true => some .le
  | .gt, false | .lt, true => some .gt
  | .ge, false | .le, true => some .ge
  | _, _ => none

/-- A column compared with a constant, the column on either side. -/
def compareOf (s : Statement) (i : Nat) (op : Op) (l r : Expr) : Option STerm :=
  let found := match column s i l with
    | some c => some (c, r, false)
    | none => (column s i r).map fun c => (c, l, true)
  match found with
  | some (c, other, reverse) =>
    match ordering op reverse, operand other with
    | some o, some v => some (.compare c o v)
    | _, _ => none
  | none => none

/-- Whether `l op r` orders a column of item `i` that declares a collation
against a constant, on either side. -/
def collatedOrder (s : Statement) (i : Nat) (op : Op) : Expr → Expr → Bool
  | .col j c, .lit (some _) | .lit (some _), .col j c =>
    (op == .lt || op == .le || op == .gt || op == .ge) && j == i &&
      ((s.item i).collated[c]?).getD false
  | _, _ => false

/-- The term an ordering of such a column against a constant gives item `i`,
which SQLite judges (`column_order.rs`). -/
def orderTerm (s : Statement) (i : Nat) (op : Op) (l r : Expr) (cd : Cond) : Option STerm :=
  if collatedOrder s i op l r then some (.row i cd) else none

/-- The term `cd` gives item `i`, if sqlive reads it as one (`term`): an
ordering of a column in its own collation against a constant is one SQLite
judges. -/
def read (s : Statement) (i : Nat) : Cond → Option STerm
  | .null e n => (column s i e).map (STerm.null · n)
  | .collated key op l r => orderTerm s i op l r (.collated key op l r)
  | .cmp op l r =>
    ((nullOf s i op l r).orElse fun _ => compareOf s i op l r).orElse fun _ =>
      orderTerm s i op l r (.cmp op l r)
  | .inList e vs =>
    match column s i e, vs.mapM operand with
    | some c, some values => some (.inList c values)
    | _, _ => none
  | .and l r =>
    match read s i l, read s i r with
    | some l, some r => some (.and l r)
    | some t, none | none, some t => some t
    | none, none => none
  | .or l r =>
    match read s i l, read s i r with
    | some l, some r => some (.or l r)
    | _, _ => none
  | _ => none

/-- The condition SQLite judges row by row, when each column `cd` names, of
item `i`, is one sqlive narrows by (`row_term`). On a derived item it passes
into the items its columns come from (`Cond.passed`). -/
def rowTerm (s : Statement) (i : Nat) (cd : Cond) : Option STerm :=
  if cd.refs.all (fun (_, c) => (s.affinity i c).isSome) then
    some (.row i cd)
  else none

/-- The reader: a condition with no subquery that names one item's columns
only. It narrows that item when the item is one of core `k`'s, and nothing
when it is an outer core's. -/
def readCondition : ReadCondition := fun s k cd =>
  match cd.items.eraseDups with
  | [i] =>
    if !cd.queries.isEmpty then none
    else if (s.item i).core ≠ k then some {}
    else match read s i cd with
      | some t => some { terms := [(i, t)] }
      | none => some { terms := (rowTerm s i cd).toList.map (i, ·) }
  | _ => none

/-! The proof: a condition true of a binding makes the term sqlive reads
true of the item's row. -/

theorem column_eq {s : Statement} {i c : Nat} {e : Expr} (h : column s i e = some c) :
    e = .col i c := by
  unfold column at h
  split at h
  · rename_i j c'
    split at h
    · rename_i hj
      cases h
      rw [hj.1]
    · cases h
  · cases h

theorem nullOf_sound {s : Statement} {i : Nat} {op : Op} {l r : Expr} {t : STerm}
    (h : nullOf s i op l r = some t) {b : Bind} {sub : Subs}
    (hc : isTrue ((Cond.cmp op l r).eval b sub)) : t.holds (b i) := by
  unfold nullOf at h
  split at h
  all_goals first
    | (cases h; done)
    | (obtain ⟨c, hcol, rfl⟩ := Option.map_eq_some_iff.mp h
       have he := column_eq hcol
       subst he
       simp_all [Cond.eval, Expr.eval, Op.apply, isTrue, STerm.holds, Option.isSome_iff_ne_none]
       try exact fun h => hc h.symm)

/-- A comparison true of a column's value and a constant, read as `o`. -/
theorem ordering_apply {op o : Op} {rev : Bool} (h : ordering op rev = some o) {a : Value}
    {v : Int} (hc : (if rev then op.apply (some v) a else op.apply a (some v)) = some true) :
    ∃ x, a = some x ∧ o.compare x v = true := by
  cases op <;> cases rev <;> simp only [ordering, reduceCtorEq] at h <;> cases h <;>
    cases a <;> simp_all [Op.apply, Op.compare] <;> omega

theorem compareOf_sound {s : Statement} {i : Nat} {op : Op} {l r : Expr} {t : STerm}
    (h : compareOf s i op l r = some t) {b : Bind} {sub : Subs}
    (hc : isTrue ((Cond.cmp op l r).eval b sub)) : t.holds (b i) := by
  unfold compareOf at h
  simp only at h
  split at h
  · rename_i c other reverse hfound
    split at h
    · rename_i o v hord hop
      cases h
      have hv : other = .lit (some v) := by
        unfold operand at hop; split at hop <;> simp_all
      subst hv
      simp only [Cond.eval, isTrue, beq_iff_eq] at hc
      have hside : (if reverse then op.apply (some v) (cellOf (b i) c)
          else op.apply (cellOf (b i) c) (some v)) = some true := by
        split at hfound
        · rename_i c' hcol
          cases hfound
          have he := column_eq hcol
          subst he
          simpa [Expr.eval] using hc
        · obtain ⟨c', hcol', hfound⟩ := Option.map_eq_some_iff.mp hfound
          cases hfound
          have he := column_eq hcol'
          subst he
          simpa [Expr.eval] using hc
      obtain ⟨x, hx, hcmp⟩ := ordering_apply hord hside
      simp [STerm.holds, hx, hcmp]
    · cases h
  · cases h

theorem operands_eq {vs : List Expr} {values : List Int} (h : vs.mapM operand = some values) :
    vs = values.map fun v => .lit (some v) := by
  induction vs generalizing values with
  | nil => simp at h; subst h; rfl
  | cons e es ih =>
    simp only [List.mapM_cons, Option.bind_eq_bind, Option.bind_eq_some_iff, Option.pure_def,
      Option.some.injEq] at h
    obtain ⟨v, hv, rest, hrest, rfl⟩ := h
    have : e = .lit (some v) := by unfold operand at hv; split at hv <;> simp_all
    subst this
    simp [ih hrest]

/-- A judged ordering holds of the item's row wherever the condition it was
read from, which names the columns of `l` and `r` and no subquery where both
are a column and a constant, holds. -/
theorem orderTerm_sound {s : Statement} {i : Nat} {op : Op} {l r : Expr} {cd : Cond} {t : STerm}
    (h : orderTerm s i op l r cd = some t)
    (hitems : cd.items = (l.refs ++ r.refs).map (·.1))
    (hqueries : cd.queries = l.queries ++ r.queries) {b : Bind} {sub : Subs}
    (hc : isTrue (cd.eval b sub)) : t.holds (b i) := by
  unfold orderTerm at h
  split at h
  · rename_i horder
    cases h
    have hnames : (∀ j ∈ cd.items, j = i) ∧ cd.queries = [] := by
      rw [hitems, hqueries]
      unfold collatedOrder at horder
      split at horder
      all_goals first
        | (cases horder; done)
        | (simp only [Bool.and_eq_true, beq_iff_eq] at horder
           obtain ⟨⟨_, hj⟩, _⟩ := horder
           refine ⟨fun x hx => ?_, rfl⟩
           simp only [Expr.refs, List.append_nil, List.nil_append, List.map_cons, List.map_nil,
             List.mem_singleton] at hx
           rw [hx]
           exact hj)
    have heq := cd.eval_rowBind (b := b) (sub := sub) hnames.1 hnames.2
    rw [heq] at hc
    exact hc
  · cases h

theorem read_sound {s : Statement} {i : Nat} {cd : Cond} {t : STerm}
    (h : read s i cd = some t) {b : Bind} {sub : Subs}
    (hc : isTrue (cd.eval b sub)) : t.holds (b i) := by
  induction cd generalizing t with
  | null e n =>
    simp only [read, Option.map_eq_some_iff] at h
    obtain ⟨c, hcol, rfl⟩ := h
    have he := column_eq hcol
    subst he
    simpa [Cond.eval, Expr.eval, isTrue, STerm.holds] using hc
  | cmp op l r =>
    simp only [read] at h
    cases hn : nullOf s i op l r with
    | some t' =>
      rw [hn] at h
      cases h
      exact nullOf_sound hn hc
    | none =>
      rw [hn] at h
      cases hcompare : compareOf s i op l r with
      | some t' =>
        rw [Option.orElse_none, hcompare] at h
        cases h
        exact compareOf_sound hcompare hc
      | none =>
        rw [Option.orElse_none, hcompare, Option.orElse_none] at h
        exact orderTerm_sound h rfl rfl hc
  | inList e vs =>
    simp only [read] at h
    split at h
    · rename_i c values hcol hvals
      cases h
      have he := column_eq hcol
      subst he
      have hv := operands_eq hvals
      subst hv
      simp only [Cond.eval, Expr.eval, List.map_map, isTrue] at hc
      cases hb : cellOf (b i) c with
      | none =>
        rw [hb] at hc
        simp only [memberTruth] at hc
        split at hc <;> simp_all
      | some x =>
        rw [hb] at hc
        simp only [STerm.holds, hb]
        simp only [memberTruth] at hc
        split at hc
        · rename_i hx
          simp only [List.contains_iff_exists_mem_beq, List.mem_map] at hx
          obtain ⟨_, ⟨v, hv, rfl⟩, hxv⟩ := hx
          simp only [Function.comp, Expr.eval, beq_iff_eq, Option.some.injEq] at hxv
          subst hxv
          simp [hv]
        · split at hc <;> simp_all
    · cases h
  | collated key op l r =>
    simp only [read] at h
    exact orderTerm_sound h rfl rfl hc
  | inQuery e q n => simp [read] at h
  | «exists» q n => simp [read] at h
  | rowSub vs q isIn n => simp [read] at h
  | rowEq ls rs => simp [read] at h
  | rowOrder key op l r rest => simp [read] at h
  | and l r ihl ihr =>
    simp only [Cond.eval, isTrue] at hc
    have hl : isTrue (l.eval b sub) ∧ isTrue (r.eval b sub) := by
      revert hc
      cases l.eval b sub <;> cases r.eval b sub <;> simp [tand, isTrue] <;>
        (try rename_i x; cases x) <;> (try rename_i y; cases y) <;> simp_all
    simp only [read] at h
    split at h
    · rename_i lt rt hlt hrt
      cases h
      simp [STerm.holds, ihl hlt hl.1, ihr hrt hl.2]
    · rename_i t' hlt _
      cases h
      exact ihl hlt hl.1
    · rename_i t' _ hrt
      cases h
      exact ihr hrt hl.2
    · cases h
  | or l r ihl ihr =>
    simp only [read] at h
    split at h
    · rename_i lt rt hlt hrt
      cases h
      simp only [Cond.eval, isTrue] at hc
      have : isTrue (l.eval b sub) ∨ isTrue (r.eval b sub) := by
        revert hc
        cases l.eval b sub <;> cases r.eval b sub <;> simp [tor, isTrue] <;>
          (try rename_i x; cases x) <;> (try rename_i y; cases y) <;> simp_all
      rcases this with hl | hr
      · simp [STerm.holds, ihl hlt hl]
      · simp [STerm.holds, ihr hrt hr]
    · cases h
  | not c _ => simp [read] at h

/-- Rule 1's theorem: a condition true of a binding makes each term sqlive
reads from it true of its item's row, whatever its subqueries return. -/
theorem term_sound : ReadCondition.Sound readCondition where
  terms := by
    intro s k cd n h i t ht
    unfold readCondition at h
    split at h
    · rename_i j hj
      have hall := mem_of_eraseDups_singleton hj
      split at h
      · cases h
      · split at h
        · cases h; simp at ht
        · rename_i hq hk
          simp only [Bool.not_eq_true, ne_eq, Decidable.not_not] at hq hk
          split at h
          · rename_i t' hread
            cases h
            simp only [List.mem_singleton, Prod.mk.injEq] at ht
            obtain ⟨rfl, rfl⟩ := ht
            exact ⟨hk, fun _ _ b hc => read_sound hread hc⟩
          · cases h
            unfold rowTerm at ht
            split at ht
            · simp only [Option.toList_some, List.map_cons, List.map_nil, List.mem_singleton,
                Prod.mk.injEq] at ht
              obtain ⟨rfl, rfl⟩ := ht
              refine ⟨hk, fun db f b hc => ?_⟩
              simp only [STerm.holds]
              rwa [← Cond.eval_rowBind cd hall (by simpa using hq)]
            · simp at ht
    · cases h
  edges := by
    intro s k cd n h e he
    unfold readCondition at h
    split at h
    · split at h
      · cases h
      · split at h
        · cases h; simp at he
        · split at h
          · cases h; simp at he
          · cases h
            unfold rowTerm at he
            split at he <;> simp at he
    · cases h

end SqliveNarrowing.Term
