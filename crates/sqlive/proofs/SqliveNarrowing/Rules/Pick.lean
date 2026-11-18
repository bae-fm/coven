import SqliveNarrowing.Rules.Limit

/-! Rule 7: the rows SQLite picks. A query that keeps some of its rows, by a
LIMIT or an OFFSET or as a subquery whose first row a clause takes, returns
the rows it reads first, unless its ORDER BY tells every two of them apart.
SQLite reads rows in the order of the plan it picks, which can follow an
index on a column no clause names, so any of the rows can come first
(`post_picks_any`). Such a query depends on every column of each table it
reads (`Statement.picks`, `Statement.reads`, `picked_column_reruns`); its
conditions still narrow which rows it depends on. A query whose ORDER BY sorts
by the rowid of each of its tables, or that aggregates with no GROUP BY,
returns the same rows in every order SQLite reads them in
(`fullyOrders_sound`), so it depends only on the columns its clauses name
(`unreported_columns.rs`). -/
namespace SqliveNarrowing.Pick
open Limit

/-! Without an ORDER BY that tells the rows apart, any row can come first. -/

/-- A query with no ORDER BY that skips `m` rows and keeps at least one
returns, for some order of its rows, any row it holds, when it holds more
than `m`. -/
theorem post_picks_any {q : Query} (horder : q.orderBy = [])
    (hlimit : ∀ n, q.limit = some n → 0 < n) {rows : List Row} {x : Row} (hx : x ∈ rows)
    (hoffset : q.offset.getD 0 < rows.length) :
    ∃ rows', rows'.Perm rows ∧ x ∈ q.post rows' := by
  let m := q.offset.getD 0
  let l := rows.erase x
  have hl : l.length = rows.length - 1 := List.length_erase_of_mem hx
  have htake : (l.take m).length = m := by
    rw [List.length_take]; omega
  refine ⟨l.take m ++ x :: l.drop m, ?_, ?_⟩
  · refine List.perm_middle.trans ?_
    rw [List.take_append_drop]
    exact (List.perm_cons_erase hx).symm
  · have hdrop : (l.take m ++ x :: l.drop m).drop m = x :: l.drop m := by
      rw [List.drop_append_of_le_length (by omega), List.drop_eq_nil_of_le (by omega)]
      rfl
    unfold Query.post
    simp only [horder, List.isEmpty_nil, ↓reduceIte]
    change x ∈ (match q.limit with
      | some n => ((l.take m ++ x :: l.drop m).drop m).take n
      | none => (l.take m ++ x :: l.drop m).drop m)
    rw [hdrop]
    cases h : q.limit with
    | none => simp
    | some n =>
      obtain ⟨n', rfl⟩ : ∃ n', n = n' + 1 := ⟨n - 1, by have := hlimit n h; omega⟩
      simp

/-! An ORDER BY that tells every two rows apart sorts them the same in every
order. -/

section Sorting

variable {α : Type} {le : α → α → Bool}

theorem perm_insertSorted (x : α) (l : List α) : (insertSorted le x l).Perm (x :: l) := by
  induction l with
  | nil => exact List.Perm.refl _
  | cons y ys ih =>
    unfold insertSorted
    split
    · exact List.Perm.refl _
    · exact (ih.cons y).trans (List.Perm.swap x y ys)

theorem perm_sortBy (l : List α) : (sortBy le l).Perm l := by
  induction l with
  | nil => exact List.Perm.refl _
  | cons x xs ih => exact (perm_insertSorted x _).trans (ih.cons x)

/-- Two sorted lists of the same elements are equal when no two different
elements sort at or before each other. -/
theorem eq_of_perm_of_sorted {l₁ l₂ : List α} (hperm : l₁.Perm l₂)
    (h₁ : l₁.Pairwise (le · ·)) (h₂ : l₂.Pairwise (le · ·))
    (anti : ∀ a ∈ l₁, ∀ b ∈ l₁, le a b → le b a → a = b) : l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil => exact (List.nil_perm.mp hperm).symm
  | cons x xs ih =>
    cases l₂ with
    | nil => exact absurd hperm (fun h => by simpa using h.length_eq)
    | cons y ys =>
      rw [List.pairwise_cons] at h₁ h₂
      have hxy : x = y := by
        have hx : x ∈ y :: ys := hperm.mem_iff.mp (by simp)
        have hy : y ∈ x :: xs := hperm.mem_iff.mpr (by simp)
        simp only [List.mem_cons] at hx hy
        rcases hx with hx | hx
        · exact hx
        · rcases hy with hy | hy
          · exact hy.symm
          · exact anti x (by simp) y (by simp [hy]) (h₁.1 y hy) (h₂.1 x hx)
      subst hxy
      have htail := ih ((List.perm_cons x).mp hperm) h₁.2 h₂.2
        (fun a ha b hb => anti a (by simp [ha]) b (by simp [hb]))
      rw [htail]

end Sorting

theorem keyCompare_eq {ds : List Bool} {as bs : List Value} (ha : as.length = ds.length)
    (hb : bs.length = ds.length) (h : keyCompare ds as bs = .eq) : as = bs := by
  induction ds generalizing as bs with
  | nil =>
    cases as <;> cases bs <;> simp_all
  | cons d ds ih =>
    cases as <;> cases bs <;> simp at ha hb
    rename_i a as b bs
    rw [keyCompare_cons, Ordering.then_eq_eq] at h
    rw [dcmp_eq.mp h.1, ih ha hb h.2]

/-- Two keys that each sort at or before the other are equal. -/
theorem keyLe_antisymm {ds : List Bool} {as bs : List Value} (ha : as.length = ds.length)
    (hb : bs.length = ds.length) (h1 : keyLe ds as bs) (h2 : keyLe ds bs as) : as = bs := by
  simp only [keyLe, bne_iff_ne, ne_eq] at h1 h2
  rw [keyCompare_swap] at h2
  apply keyCompare_eq ha hb
  cases h : keyCompare ds as bs <;> simp_all [Ordering.swap]

/-- A query whose ORDER BY tells every two of its rows apart returns the same
rows whatever order it reads them in. -/
theorem post_perm {q : Query} {rows rows' : List Row} (hperm : rows'.Perm rows)
    (hsep : ∀ a ∈ rows, ∀ b ∈ rows, orderKey q a = orderKey q b → a = b) :
    q.post rows' = q.post rows := by
  have hlen_key : ∀ r, (orderKey q r).length = (q.orderBy.map (·.2)).length := by
    intro r; simp [orderKey]
  have hsorted : (if q.orderBy.isEmpty then rows' else
        sortBy (fun a b => keyLe (q.orderBy.map (·.2)) (orderKey q a) (orderKey q b)) rows') =
      (if q.orderBy.isEmpty then rows else
        sortBy (fun a b => keyLe (q.orderBy.map (·.2)) (orderKey q a) (orderKey q b)) rows) := by
    split
    · rename_i hempty
      have hnil : q.orderBy = [] := List.isEmpty_iff.mp hempty
      have hkey : ∀ r, orderKey q r = [] := by intro r; simp [orderKey, hnil]
      apply eq_of_perm_of_sorted (le := fun _ _ => true) hperm
      · exact List.pairwise_of_forall (by simp)
      · exact List.pairwise_of_forall (by simp)
      · intro a ha b hb _ _
        exact hsep a (hperm.mem_iff.mp ha) b (hperm.mem_iff.mp hb) (by rw [hkey, hkey])
    · let le := fun a b => keyLe (q.orderBy.map (·.2)) (orderKey q a) (orderKey q b)
      have total : ∀ a b, le a b || le b a := fun a b => keyLe_total _ _ _
      have trans : ∀ a b c, le a b → le b c → le a c := fun a b c h1 h2 =>
        keyLe_trans (hlen_key a) (hlen_key b) (hlen_key c) h1 h2
      apply eq_of_perm_of_sorted (le := le)
      · exact ((perm_sortBy rows').trans hperm).trans (perm_sortBy rows).symm
      · exact pairwise_sortBy total trans rows'
      · exact pairwise_sortBy total trans rows
      · intro a ha b hb h1 h2
        have ha' := hperm.mem_iff.mp ((perm_sortBy rows').mem_iff.mp ha)
        have hb' := hperm.mem_iff.mp ((perm_sortBy rows').mem_iff.mp hb)
        exact hsep a ha' b hb' (keyLe_antisymm (hlen_key a) (hlen_key b) h1 h2)
  unfold Query.post
  simp only
  rw [show (fun a b => keyLe (q.orderBy.map (·.2)) (q.orderBy.map fun (p, _) => a.cell p)
      (q.orderBy.map fun (p, _) => b.cell p)) =
      (fun a b => keyLe (q.orderBy.map (·.2)) (orderKey q a) (orderKey q b)) from rfl, hsorted]

/-! The rows of a query whose ORDER BY sorts by each table's rowid differ in
their sort key. -/

/-- A database whose tables hold each row's rowid in its first column, never
NULL and different in each row, as each slot holds the row of its rowid
(`Json.readDb`). -/
def _root_.SqliveNarrowing.Db.Keyed (db : Db) : Prop :=
  ∀ t, ∀ r ∈ db.rows t, r.cell 0 ≠ none ∧ ∀ r' ∈ db.rows t, r.cell 0 = r'.cell 0 → r = r'

/-- A binding that joining `is` onto `bs` gives is a binding of `bs` with
each item of `is` bound to one of its rows, or to none. -/
theorem mem_joinItems {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} :
    ∀ (is : List Nat) (bs : List Bind) (b' : Bind), b' ∈ joinItems s c rowsOf subs is bs →
      ∃ b ∈ bs, (∀ j, j ∉ is → b' j = b j) ∧
        ∀ i ∈ is, b' i = none ∨ ∃ r ∈ rowsOf i, b' i = some r
  | [], bs, b', h => ⟨b', h, fun _ _ => rfl, fun _ hi => by simp at hi⟩
  | i :: is, bs, b', h => by
    simp only [joinItems] at h
    obtain ⟨b'', hb'', hout, hin⟩ := mem_joinItems is _ b' h
    obtain ⟨b, hb, hset⟩ := List.mem_flatMap.mp hb''
    have hrow : b'' = b.set i none ∨ ∃ r ∈ rowsOf i, b'' = b.set i (some r) := by
      split at hset
      · simp only [List.mem_singleton] at hset
        exact Or.inl hset
      · obtain ⟨r, hr, hsome⟩ := List.mem_filterMap.mp hset
        split at hsome
        · exact Or.inr ⟨r, hr, (Option.some.inj hsome).symm⟩
        · cases hsome
    refine ⟨b, hb, ?_, ?_⟩
    · intro j hj
      simp only [List.mem_cons, not_or] at hj
      rw [hout j hj.2]
      rcases hrow with rfl | ⟨r, _, rfl⟩ <;> simp [Bind.set, hj.1]
    · intro i' hi'
      by_cases hmem : i' ∈ is
      · exact hin i' hmem
      · have hi : i' = i := by simpa [hmem] using hi'
        subst hi
        rw [hout i' hmem]
        rcases hrow with rfl | ⟨r, hr, rfl⟩
        · simp [Bind.set]
        · exact Or.inr ⟨r, hr, by simp [Bind.set]⟩

/-- A core that aggregates with no GROUP BY makes one row of all its
bindings. -/
theorem one_row {c : Core} (hagg : c.aggregated = true) (hgroup : c.groupBy.isEmpty = true)
    {subs : Bind → Subs} {kept : List Bind} {r : Row} (hr : r ∈ c.rows subs kept) :
    r = c.output.map fun
      | .expr e => match kept.getLast? with
        | some b => e.eval b (subs b)
        | none => none
      | .count => some (Int.ofNat kept.length)
      | .max e => maxValue (kept.map fun b => e.eval b (subs b)) := by
  unfold Core.rows at hr
  simp only [hagg, hgroup, ↓reduceIte, List.map_cons, List.map_nil, List.mem_singleton] at hr
  exact hr

/-- Two rows of a query that returns one row, or whose ORDER BY sorts by the
rowid of each of its tables on a database that keeps each row's rowid in its
first column, differ in their sort key. -/
theorem fullyOrders_separates {s : Statement} {db : Db} (hdb : db.Keyed) {q k : Nat}
    (hfull : s.fullyOrders q = true) (harms : (s.query q).arms = [k])
    {eval : Bind → Nat → List Row} {outer : Bind} :
    ∀ a ∈ (s.core k).rows eval (coreBindings s db eval outer k),
    ∀ b ∈ (s.core k).rows eval (coreBindings s db eval outer k),
      orderKey (s.query q) a = orderKey (s.query q) b → a = b := by
  unfold Statement.fullyOrders at hfull
  rw [harms] at hfull
  simp only [Bool.or_eq_true, Bool.and_eq_true, Bool.not_eq_true', List.all_eq_true] at hfull
  rcases hfull with ⟨hagg, hgroup⟩ | ⟨hagg, hitems⟩
  · intro a ha b hb _
    exact (one_row hagg hgroup ha).trans (one_row hagg hgroup hb).symm
  intro a ha b hb hkey
  obtain ⟨ba, hba, rfl⟩ := mem_rows_plain hagg ha
  obtain ⟨bb, hbb, rfl⟩ := mem_rows_plain hagg hb
  simp only [coreBindings, List.mem_filter] at hba hbb
  obtain ⟨a0, ha0, haout, hain⟩ := mem_joinItems _ _ _ hba.1
  obtain ⟨b0, hb0, hbout, hbin⟩ := mem_joinItems _ _ _ hbb.1
  simp only [List.mem_singleton] at ha0 hb0
  subst ha0 hb0
  have hsame : ba = bb := by
    funext j
    by_cases hj : j ∈ (s.core k).items
    · obtain ⟨htable, horder⟩ := hitems j hj
      obtain ⟨t, ht⟩ := Option.isSome_iff_exists.mp htable
      have hkind : (s.item j).kind = .table t := by
        unfold Statement.table? at ht
        split at ht
        · rename_i t' hk; cases ht; exact hk
        · cases ht
      obtain ⟨⟨p, d⟩, hpd, hout⟩ := List.any_eq_true.mp horder
      have hp : (s.core k).output[p]? = some (.expr (.col j 0)) := by
        split at hout
        · rename_i j' hp'
          simp only [beq_iff_eq] at hout
          subst hout
          exact hp'
        · cases hout
      have hcell := (List.map_inj_left.mp hkey) (p, d) hpd
      simp only [Row.cell, List.getElem?_map, hp, Option.map_some, Option.join_some,
        Expr.eval] at hcell
      have hrows : ∀ (bnd : Bind) (r : Row), bnd j = some r →
          (bnd j = none ∨ ∃ r' ∈ (match (s.item j).kind with
            | .table t => db.rows t
            | .derived q _ => eval (if (s.core k).parent.isNone then Bind.empty else outer) q),
            bnd j = some r') → r ∈ db.rows t := by
        intro bnd r hr hin
        rw [hkind] at hin
        rcases hin with h | ⟨r', hr', h⟩
        · rw [hr] at h; cases h
        · rw [hr] at h; cases h; exact hr'
      cases hja : ba j with
      | none =>
        cases hjb : bb j with
        | none => rfl
        | some rb =>
          have hrb := hrows bb rb hjb (hbin j hj)
          rw [hja, hjb] at hcell
          exact absurd hcell.symm (hdb t rb hrb).1
      | some ra =>
        have hra := hrows ba ra hja (hain j hj)
        cases hjb : bb j with
        | none =>
          rw [hja, hjb] at hcell
          exact absurd hcell (hdb t ra hra).1
        | some rb =>
          have hrb := hrows bb rb hjb (hbin j hj)
          rw [hja, hjb] at hcell
          rw [(hdb t ra hra).2 rb hrb hcell]
    · rw [haout j hj, hbout j hj]
  rw [hsame]

/-- Rule 7's theorem for a query that tells its rows apart: a one-arm query
that aggregates with no GROUP BY, or whose ORDER BY sorts by the rowid of each
of its tables, returns the same rows from any order of its core's rows, as
SQLite's plan reads them, on a database that keeps each row's rowid in its
first column. -/
theorem fullyOrders_sound {s : Statement} {db : Db} (hdb : db.Keyed) {q k : Nat}
    (hfull : s.fullyOrders q = true) (harms : (s.query q).arms = [k])
    {eval : Bind → Nat → List Row} {outer : Bind} {rows' : List Row}
    (hperm : rows'.Perm ((s.core k).rows eval (coreBindings s db eval outer k))) :
    (s.query q).post rows' =
      (s.query q).post ((s.core k).rows eval (coreBindings s db eval outer k)) :=
  post_perm hperm (fullyOrders_separates hdb hfull harms)

/-! Every column of a table a picked query reads counts as read. -/

theorem find?_pairs {f : Nat → List Nat} {l : List Nat} {t : Nat} (ht : t ∈ l) :
    (l.map fun t => (t, f t)).find? (·.1 == t) = some (t, f t) := by
  induction l with
  | nil => cases ht
  | cons u us ih =>
    by_cases hu : u = t
    · subst hu; simp
    · have ht' : t ∈ us := by
        rcases List.mem_cons.mp ht with h | h
        · exact absurd h.symm hu
        · exact h
      simp [hu, ih ht']

/-- Rule 7's theorem for a query whose rows SQLite picks: a change to any
column of a table that such a query reads reruns the statement, when the row
before or after passes the table's scope. -/
theorem picked_column_reruns {s : Statement} {scopeOf : Nat → Option RScope}
    {changes : List Change} {db' : Db} {ch : Change} (hch : ch ∈ changes)
    (hread : ch.table ∈ s.itemNumbers.filterMap s.table?) (hpicked : ch.table ∈ s.pickedTables)
    {old new : Row} (hold : ch.old = some old) (hnew : ch.new = some new) {c : Nat}
    (hc : c < s.width ch.table) (hchanged : old.cell c ≠ new.cell c)
    (hpasses : let passes (r : Row) := ((scopeOf ch.table).getD .all).passes (lookupRows s db') r
      ch.old.any passes || ch.new.any passes) :
    reruns s scopeOf s.reads changes db' = true := by
  unfold reruns
  simp only [List.any_eq_true]
  refine ⟨ch, hch, ?_⟩
  unfold Statement.reads
  simp only
  rw [find?_pairs (List.mem_eraseDups.mpr hread)]
  simp only [Bool.and_eq_true]
  refine ⟨?_, hpasses⟩
  rw [hold, hnew]
  simp only [changedIn, List.any_eq_true, bne_iff_ne, ne_eq]
  refine ⟨c, ?_, hchanged⟩
  rw [List.mem_eraseDups, List.mem_append]
  right
  simp [hpicked, hc]

end SqliveNarrowing.Pick
