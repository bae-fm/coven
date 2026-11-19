import SqliveNarrowing.WellFormed

/-! The composed proof's first step: a statement's rows depend only on the
columns it reads (`Statement.reads`). sqlive keeps of a changed row only the
values its scopes test, and the model's judge tests a row masked to the
columns the statement reads (`maskRow`), with lookups in the database masked
the same way (`Db.mask`). Running the statement on a masked database gives
its rows unchanged (`result_mask`), so the judge's lookups read the rows the
statement reads, and two databases whose masks agree give the statement the
same rows. -/
namespace SqliveNarrowing

theorem le_foldr_max {l : List Nat} {x : Nat} (hx : x ∈ l) : x ≤ l.foldr max 0 := by
  induction l with
  | nil => cases hx
  | cons y ys ih =>
    simp only [List.foldr_cons]
    rcases List.mem_cons.mp hx with rfl | hx
    · exact Nat.le_max_left _ _
    · exact Nat.le_trans (ih hx) (Nat.le_max_right _ _)

theorem cell_maskRow (columns : List Nat) (r : Row) (c : Nat) :
    (maskRow columns r).cell c = if c ∈ columns then r.cell c else none := by
  unfold maskRow Row.cell
  rw [List.getElem?_map]
  by_cases hc : c < columns.foldr max 0 + 1
  · simp [List.getElem?_range hc]
  · have hn : c ∉ columns := fun h => hc (Nat.lt_succ_of_le (le_foldr_max h))
    rw [List.getElem?_eq_none (by simp only [List.length_range]; omega)]
    simp [hn]

/-- Two rows mask to the same row exactly when they agree in each column. -/
theorem maskRow_eq_iff {columns : List Nat} {a b : Row} :
    maskRow columns a = maskRow columns b ↔ ∀ c ∈ columns, a.cell c = b.cell c := by
  constructor
  · intro h c hc
    have := congrArg (Row.cell · c) h
    simpa [cell_maskRow, hc] using this
  · intro h
    unfold maskRow
    apply List.map_congr_left
    intro c _
    split
    · rename_i hc
      exact h c (by simpa using hc)
    · rfl

/-- A change touches what the statement reads of its row exactly when the
row before and the row after mask to different rows. -/
theorem changedIn_iff {columns : List Nat} {x y : Option Row} :
    changedIn columns x y = true ↔ x.map (maskRow columns) ≠ y.map (maskRow columns) := by
  cases x <;> cases y <;> simp [changedIn, maskRow_eq_iff]

/-- What sqlive keeps of item `i`'s row: a row of a table the statement
reads, masked to the columns it reads of the table; any other row whole. -/
def Statement.maskItem (s : Statement) (i : Nat) (r : Row) : Row :=
  match s.table? i with
  | some t =>
    match s.reads.find? (·.1 == t) with
    | some (_, columns) => maskRow columns r
    | none => r
  | none => r

/-- A binding with each item's row masked as sqlive keeps it. -/
def Statement.maskBind (s : Statement) (b : Bind) : Bind := fun i => (b i).map (s.maskItem i)

theorem Statement.maskBind_set (s : Statement) (b : Bind) (i : Nat) (r : Option Row) :
    s.maskBind (b.set i r) = (s.maskBind b).set i (r.map (s.maskItem i)) := by
  funext j
  by_cases hj : j = i
  · subst hj; simp [Statement.maskBind, Bind.set]
  · simp [Statement.maskBind, Bind.set, hj]

theorem Statement.maskBind_empty (s : Statement) : s.maskBind Bind.empty = Bind.empty := by
  funext i; rfl

/-- A column a core's clause names. -/
theorem Statement.mem_refs {s : Statement} {k : Nat} {p : Nat × Nat} (h : p ∈ (s.core k).refs) :
    p ∈ s.cores.flatMap Core.refs := by
  by_cases hk : k < s.cores.length
  · have : s.core k = s.cores[k] := by
      simp [Statement.core, List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hk]
    rw [this] at h
    exact List.mem_flatMap.mpr ⟨_, List.getElem_mem hk, h⟩
  · have : s.core k = default := by
      simp only [Statement.core, List.getD_eq_getElem?_getD]
      rw [List.getElem?_eq_none (Nat.le_of_not_lt hk)]
      rfl
    rw [this, show (default : Core).refs = [] from rfl] at h
    cases h

/-- The columns `Statement.reads` names of a table: those the clauses name of
it, among others. -/
theorem Statement.reads_find? {s : Statement} {t : Nat} {read : Nat × List Nat}
    (h : s.reads.find? (·.1 == t) = some read) :
    read.1 = t ∧ ∀ i c, (i, c) ∈ s.cores.flatMap Core.refs → s.table? i = some t → c ∈ read.2 := by
  have hp := List.find?_some h
  have hm := List.mem_of_find?_eq_some h
  simp only [beq_iff_eq] at hp
  refine ⟨hp, fun i c hic ht => ?_⟩
  unfold Statement.reads at hm
  simp only [List.mem_map] at hm
  obtain ⟨t', _, rfl⟩ := hm
  simp only at hp
  subst hp
  simp only [List.mem_eraseDups, List.mem_append, List.mem_filterMap]
  exact Or.inl ⟨(i, c), hic, by simp [ht]⟩

/-- Masking keeps every cell a clause names. -/
theorem Statement.cellOf_maskBind (s : Statement) (b : Bind) {i c : Nat}
    (h : (i, c) ∈ s.cores.flatMap Core.refs) : cellOf (s.maskBind b i) c = cellOf (b i) c := by
  unfold Statement.maskBind Statement.maskItem
  cases hb : b i with
  | none => rfl
  | some r =>
    simp only [Option.map_some]
    split
    · rename_i t ht
      split
      · rename_i read columns hread
        have := (Statement.reads_find? hread).2 i c h ht
        simp [cellOf, cell_maskRow, this]
      · rfl
    · rfl

/-- A table an item in range reads is one `Statement.reads` names, and its
masked rows are the rows of that item masked. -/
theorem Statement.mask_rows {s : Statement} {i t : Nat} (hi : i < s.items.length)
    (ht : s.table? i = some t) (db : Db) :
    (db.mask s.reads).rows t = (db.rows t).map (s.maskItem i) := by
  have hfind := s.find?_reads
    (List.mem_filterMap.mpr ⟨i, by simpa [Statement.itemNumbers] using hi, ht⟩)
  unfold Db.rows Db.mask Statement.maskItem
  rw [hfind, ht]
  simp only
  rw [hfind]
  simp [List.filterMap_map, List.map_filterMap]

/-- Joining items whose rows are another's mapped row by row gives the
other's bindings mapped. -/
theorem joinItems_map (s : Statement) (c : Core) {M : Bind → Bind} {mr : Nat → Row → Row}
    (hset : ∀ x i r, M (x.set i r) = (M x).set i (r.map (mr i)))
    {rowsOf rowsOf' : Nat → List Row} {subs subs' : Bind → Subs} :
    ∀ (is : List Nat), (∀ i ∈ is, rowsOf' i = (rowsOf i).map (mr i)) →
    (∀ i ∈ is, ∀ x, ((c.onOf i).all fun cd => isTrue (cd.eval (M x) (subs' (M x)))) =
      ((c.onOf i).all fun cd => isTrue (cd.eval x (subs x)))) →
    ∀ (bs : List Bind),
      (joinItems s c rowsOf subs is bs).map M = joinItems s c rowsOf' subs' is (bs.map M)
  | [], _, _, bs => by simp [joinItems]
  | i :: is, hrows, hon, bs => by
    rw [View.joinItems_cons, View.joinItems_cons,
      joinItems_map s c hset is (fun j hj => hrows j (by simp [hj]))
        (fun j hj => hon j (by simp [hj]))]
    congr 1
    rw [List.flatMap_map, List.map_flatMap]
    congr 1
    funext x
    have hon' := hon i (by simp)
    have hj : ((rowsOf' i).filterMap fun r =>
          if (c.onOf i).all (fun cd =>
            isTrue (cd.eval ((M x).set i (some r)) (subs' ((M x).set i (some r)))))
          then some ((M x).set i (some r)) else none) =
        ((rowsOf i).filterMap fun r =>
          if (c.onOf i).all (fun cd =>
            isTrue (cd.eval (x.set i (some r)) (subs (x.set i (some r)))))
          then some (x.set i (some r)) else none).map M := by
      rw [hrows i (by simp), List.filterMap_map, List.map_filterMap]
      congr 1
      funext r
      have hm : (M x).set i (some (mr i r)) = M (x.set i (some r)) := by
        rw [hset]; rfl
      simp only [Function.comp, hm, hon']
      split <;> rfl
    unfold View.joinStep
    simp only
    rw [hj]
    have he : ∀ l : List Bind, (l.map M).isEmpty = l.isEmpty := by intro l; cases l <;> rfl
    rw [he]
    split
    · simp [hset x i none]
    · rfl

/-- One core of a statement keeps, in the masked database, its bindings
masked, and makes the same rows of them, when its subqueries return the same
rows under masked bindings. -/
theorem result_mask_core {s : Statement}
    (hitems : ∀ k, ∀ i ∈ (s.core k).items, i < s.items.length) (db : Db)
    {E E' : Bind → Nat → List Row} (hsub : ∀ x, E' (s.maskBind x) = E x) (b : Bind) (k : Nat) :
    (s.core k).result E' (coreBindings s (db.mask s.reads) E' (s.maskBind b) k) =
      (s.core k).result E (coreBindings s db E b k) := by
  let M := s.maskBind
  let c := s.core k
  have hcell : ∀ x i col, (i, col) ∈ c.refs → cellOf (M x i) col = cellOf (x i) col :=
    fun x i col h => s.cellOf_maskBind x (Statement.mem_refs h)
  have hcond : ∀ cd ∈ c.conds.map (·.1), ∀ x, cd.eval (M x) (E' (M x)) = cd.eval x (E x) := by
    intro cd hcd x
    obtain ⟨⟨cd', p⟩, hmem, rfl⟩ := List.mem_map.mp hcd
    rw [hsub]
    apply Cond.eval_congr
    · intro i col h
      exact hcell x i col (by
        simp only [Core.refs, List.mem_append, List.mem_flatMap]
        exact Or.inl (Or.inl ⟨(cd', p), hmem, h⟩))
    · intro _ _; rfl
  have hall : ∀ (cds : List Cond), (∀ cd ∈ cds, cd ∈ c.conds.map (·.1)) → ∀ x,
      (cds.all fun cd => isTrue (cd.eval (M x) (E' (M x)))) =
        (cds.all fun cd => isTrue (cd.eval x (E x))) := by
    intro cds hcds x
    induction cds with
    | nil => rfl
    | cons cd rest ihc =>
      simp only [List.all_cons]
      rw [hcond cd (hcds cd (by simp)) x, ihc (fun c' h' => hcds c' (by simp [h']))]
  have honOf : ∀ i cd, cd ∈ c.onOf i → cd ∈ c.conds.map (·.1) := by
    intro i cd hcd
    simp only [Core.onOf, List.mem_filterMap] at hcd
    obtain ⟨⟨cd', p⟩, hmem, hp⟩ := hcd
    split at hp
    · cases hp; exact List.mem_map.mpr ⟨_, hmem, rfl⟩
    · cases hp
  have hinner : ∀ cd, cd ∈ c.innerConds → cd ∈ c.conds.map (·.1) := by
    intro cd hcd
    simp only [Core.innerConds, List.mem_filterMap] at hcd
    obtain ⟨⟨cd', p⟩, hmem, hp⟩ := hcd
    split at hp
    · cases hp; exact List.mem_map.mpr ⟨_, hmem, rfl⟩
    · cases hp
  let b0 := if c.parent.isNone then Bind.empty else b
  have hb0 : (if c.parent.isNone then Bind.empty else M b) = M b0 := by
    simp only [b0]
    split
    · exact (s.maskBind_empty).symm
    · rfl
  let rowsOf (i : Nat) : List Row :=
    match (s.item i).kind with
    | .table t => db.rows t
    | .derived q _ => E b0 q
  let rowsOf' (i : Nat) : List Row :=
    match (s.item i).kind with
    | .table t => (db.mask s.reads).rows t
    | .derived q _ => E' (M b0) q
  have hrowsOf : ∀ i ∈ c.items, rowsOf' i = (rowsOf i).map (s.maskItem i) := by
    intro i hi
    simp only [rowsOf, rowsOf']
    cases hkind : (s.item i).kind with
    | table t =>
      have ht : s.table? i = some t := by simp [Statement.table?, hkind]
      exact Statement.mask_rows (hitems k i hi) ht db
    | derived q' v =>
      have ht : s.table? i = none := by simp [Statement.table?, hkind]
      have hid : s.maskItem i = id := by
        funext r; simp [Statement.maskItem, ht]
      simp only [hid, List.map_id]
      rw [hsub]
  -- The masked database's bindings are the bindings masked.
  have hj := joinItems_map s c (s.maskBind_set) c.items hrowsOf
    (fun i _ x => hall _ (fun cd hcd => honOf i cd hcd) x) [b0] (subs := E) (subs' := E')
  rw [List.map_singleton] at hj
  have hbind : coreBindings s (db.mask s.reads) E' (M b) k = (coreBindings s db E b k).map M := by
    unfold coreBindings
    simp only
    rw [hb0]
    change (joinItems s c rowsOf' E' c.items [M b0]).filter _ =
      ((joinItems s c rowsOf E c.items [b0]).filter _).map M
    rw [← hj, List.filter_map]
    congr 2
    funext x
    simp only [Function.comp]
    exact hall _ (fun cd hcd => hinner cd hcd) x
  have hrows : ∀ L : List Bind, c.rows E' (L.map M) = c.rows E L := by
    apply View.rows_restrict
    intro e he x
    rw [hsub]
    apply e.eval_congr
    · intro i col h
      apply hcell x i col
      simp only [View.outExprs, List.mem_append, List.mem_flatMap] at he
      simp only [Core.refs, List.mem_append, List.mem_flatMap]
      rcases he with ⟨o, ho, he⟩ | he
      · exact Or.inl (Or.inr ⟨o, ho, by
          simp only [OutCol.refs, List.mem_flatMap]; exact ⟨e, he, h⟩⟩)
      · exact Or.inr ⟨e, he, h⟩
    · intro _ _; rfl
  change c.result E' (coreBindings s (db.mask s.reads) E' (M b) k) =
    c.result E (coreBindings s db E b k)
  rw [hbind]
  unfold Core.result
  rw [hrows]

/-- The statement's queries return the same rows from the masked database,
under a binding masked the same way. -/
theorem evalQuery_mask {s : Statement}
    (hitems : ∀ k, ∀ i ∈ (s.core k).items, i < s.items.length) (db : Db) (f : Nat) :
    ∀ b q, evalQuery s (db.mask s.reads) f (s.maskBind b) q = evalQuery s db f b q := by
  induction f with
  | zero => intro _ _; rfl
  | succ f ih =>
    intro b q
    simp only [evalQuery, queryRows]
    congr 2
    apply List.map_congr_left
    intro k _
    exact result_mask_core hitems db (fun x => funext (ih x)) b k

/-- Masking's theorem: a statement whose cores list items in range returns
the same rows from what sqlive keeps of a database as from the database. -/
theorem result_mask {s : Statement} (hitems : ∀ k, ∀ i ∈ (s.core k).items, i < s.items.length)
    (db : Db) : s.result (db.mask s.reads) = s.result db := by
  unfold Statement.result
  rw [← s.maskBind_empty]
  exact evalQuery_mask hitems db _ _ _

end SqliveNarrowing
