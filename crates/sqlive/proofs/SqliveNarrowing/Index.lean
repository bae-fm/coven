import SqliveNarrowing.Sound

/-! Which live queries a write is judged against (`read_index.rs`). A handle
files each table read of each waiting live query under its scope's key: a
column whose value every row in the scope holds one of a few constants in,
and those constants (`RowScope::key`), or under the table alone when the
scope has none. A write's change is judged only against the live queries
filed under its table and under a constant its row holds in the key's
column, before or after the change. A row the scope passes holds one of the
key's constants (`RScope.key_passes`), so a change the index files no query
under reruns none of them, and the judge through the index reruns each
live query on exactly the writes the judge of every change reruns it on
(`index_judges_alike`, `indexed_narrowing_sound`). -/
namespace SqliveNarrowing

/-- The columns a scope's equalities and IN lists test, in order: those
whose values `RScope.equalValues` can give (`RowScope::equality_columns`). -/
def RScope.keyColumns : RScope → List Nat
  | .compare c .eq _ | .inVals c _ => [c]
  | .and l r | .or l r => l.keyColumns ++ r.keyColumns
  | _ => []

/-- The column the index files a scope's read under, with the values every
row in the scope holds there: the first of its equalities' columns for
which the scope says (`RowScope::key`). -/
def RScope.key (sc : RScope) : Option (Nat × List Int) :=
  sc.keyColumns.findSome? fun c => (sc.equalValues c).map (c, ·)

/-- A scope's key names values the scope says every row it passes holds in
the key's column. -/
theorem RScope.key_equalValues {sc : RScope} {c : Nat} {vs : List Int}
    (h : sc.key = some (c, vs)) : sc.equalValues c = some vs := by
  unfold RScope.key at h
  obtain ⟨_, c', _, _, hf, _⟩ := List.findSome?_eq_some_iff.mp h
  cases hv : sc.equalValues c' with
  | none => rw [hv] at hf; cases hf
  | some vs' =>
    rw [hv] at hf
    simp only [Option.map_some, Option.some.injEq, Prod.mk.injEq] at hf
    obtain ⟨rfl, rfl⟩ := hf
    exact hv

/-- A row a scope passes holds one of its key's values in the key's
column. -/
theorem RScope.key_passes {sc : RScope} {rowsOf : Nat → List Row} {r : Row} {c : Nat}
    {vs : List Int} (h : sc.key = some (c, vs)) (hp : sc.passes rowsOf r = true) :
    ∃ x, r.cell c = some x ∧ x ∈ vs :=
  RScope.equalValues_passes (RScope.key_equalValues h) hp

/-- Whether a key admits row `r`: every row when there is no key, else a
row that holds one of the key's values in its column. -/
def keyAdmits : Option (Nat × List Int) → Row → Bool
  | none, _ => true
  | some (c, vs), r =>
    match r.cell c with
    | some x => vs.contains x
    | none => false

/-- The key a statement's read of table `t` is filed under, with the scopes
the run that read `db` gives: none for a table it reads whole. -/
def Statement.key (s : Statement) (db : Db) (t : Nat) : Option (Nat × List Int) :=
  (((s.tableScopes db).getD fun _ => none) t).bind RScope.key

/-- One entry of the index: the number of a live query, a table it reads,
and the key its read of that table is filed under. -/
structure Entry where
  query : Nat
  table : Nat
  key : Option (Nat × List Int)

/-- The index of live queries `qs`, each a number and a statement whose last
run read `db`: one entry for each table each statement reads. -/
def index (qs : List (Nat × Statement)) (db : Db) : List Entry :=
  qs.flatMap fun (i, s) => s.reads.map fun (t, _) => ⟨i, t, s.key db t⟩

/-- Whether entry `e` finds change `ch`: the change is to the entry's table,
and its row before or after holds a value the entry's key admits. -/
def Entry.finds (e : Entry) (ch : Change) : Bool :=
  e.table == ch.table && (ch.old.any (keyAdmits e.key) || ch.new.any (keyAdmits e.key))

/-- The numbers of the live queries the index finds for change `ch`. -/
def lookup (ix : List Entry) (ch : Change) : List Nat :=
  (ix.filter (·.finds ch)).map (·.query)

/-- Whether the judge, through the index of `qs`, reruns live query `i`,
statement `s`: it judges only the changes the index finds the query for. -/
def rerunsIndexed (qs : List (Nat × Statement)) (db : Db) (changes : List Change) (db' : Db)
    (i : Nat) (s : Statement) : Bool :=
  s.reruns db (changes.filter fun ch => (lookup (index qs db) ch).contains i) db'

/-- A change that reruns a statement is one the statement's read of the
changed table admits by its key, before or after. -/
theorem key_admits_of_reruns {s : Statement} {db db' : Db} {ch : Change}
    (h : SqliveNarrowing.reruns s ((s.tableScopes db).getD fun _ => none) s.reads [ch] db' = true) :
    (∃ columns, (ch.table, columns) ∈ s.reads) ∧
      (ch.old.any (keyAdmits (s.key db ch.table)) ||
        ch.new.any (keyAdmits (s.key db ch.table))) = true := by
  unfold SqliveNarrowing.reruns at h
  simp only [List.any_cons, List.any_nil, Bool.or_false] at h
  cases hf : s.reads.find? (·.1 == ch.table) with
  | none => rw [hf] at h; cases h
  | some read =>
    obtain ⟨t, columns⟩ := read
    have ht : t = ch.table := by simpa using List.find?_some hf
    subst ht
    refine ⟨⟨columns, List.mem_of_find?_eq_some hf⟩, ?_⟩
    rw [hf] at h
    simp only [Bool.and_eq_true, Bool.or_eq_true] at h
    obtain ⟨_, hpass⟩ := h
    -- A row that passes the table's scope is one its key admits.
    have hadmit : ∀ r : Row, scopePasses s ((s.tableScopes db).getD fun _ => none) s.reads db'
        ch.table r = true → keyAdmits (s.key db ch.table) r = true := by
      intro r hr
      unfold scopePasses at hr
      unfold Statement.key
      cases hsc : ((s.tableScopes db).getD fun _ => none) ch.table with
      | none => rfl
      | some sc =>
        rw [hsc] at hr
        simp only [Option.getD_some] at hr
        simp only [Option.bind_some]
        cases hk : sc.key with
        | none => rfl
        | some key =>
          obtain ⟨c, vs⟩ := key
          obtain ⟨x, hx, hxs⟩ := RScope.key_passes hk hr
          rw [cell_maskRow] at hx
          split at hx
          · simp only [keyAdmits, hx]
            exact List.contains_iff_mem.mpr hxs
          · cases hx
    have hany : ∀ o : Option Row, o.any (scopePasses s ((s.tableScopes db).getD fun _ => none)
        s.reads db' ch.table) = true → o.any (keyAdmits (s.key db ch.table)) = true := by
      intro o ho
      cases o with
      | none => cases ho
      | some r => exact hadmit r ho
    rcases hpass with hold | hnew
    · simp [hany _ hold]
    · simp [hany _ hnew]

/-- The index finds a live query for every change that reruns it. -/
theorem lookup_complete {qs : List (Nat × Statement)} {db db' : Db} {i : Nat} {s : Statement}
    (hq : (i, s) ∈ qs) {ch : Change}
    (h : SqliveNarrowing.reruns s ((s.tableScopes db).getD fun _ => none) s.reads [ch] db' = true) :
    (lookup (index qs db) ch).contains i = true := by
  obtain ⟨⟨columns, hread⟩, hadmit⟩ := key_admits_of_reruns h
  refine List.contains_iff_mem.mpr (List.mem_map.mpr ⟨⟨i, ch.table, s.key db ch.table⟩, ?_, rfl⟩)
  refine List.mem_filter.mpr ⟨?_, by simp [Entry.finds, hadmit]⟩
  exact List.mem_flatMap.mpr ⟨(i, s), hq, List.mem_map.mpr ⟨(ch.table, columns), hread, rfl⟩⟩

/-- A write reruns a statement when one of its changes alone does. -/
theorem reruns_any (s : Statement) (scopeOf : Nat → Option RScope)
    (reads : List (Nat × List Nat)) (changes : List Change) (db' : Db) :
    SqliveNarrowing.reruns s scopeOf reads changes db' =
      changes.any fun ch => SqliveNarrowing.reruns s scopeOf reads [ch] db' := by
  unfold SqliveNarrowing.reruns
  simp

/-- The judge through the index reruns each live query on exactly the
writes the judge of every change reruns it on: a change the index doesn't
find the query for reruns it on no write. -/
theorem index_judges_alike {qs : List (Nat × Statement)} {db db' : Db} {changes : List Change}
    {i : Nat} {s : Statement} (hq : (i, s) ∈ qs) :
    rerunsIndexed qs db changes db' i s = s.reruns db changes db' := by
  unfold rerunsIndexed Statement.reruns
  rw [reruns_any, reruns_any (changes := changes), List.any_filter]
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true, Bool.and_eq_true]
  constructor
  · rintro ⟨ch, hch, -, h⟩
    exact ⟨ch, hch, h⟩
  · rintro ⟨ch, hch, h⟩
    exact ⟨ch, hch, lookup_complete hq h, h⟩

/-- Narrowing's soundness through the index: for a well-formed statement
among the live queries, a write the judge through the index lets pass
leaves the statement's rows unchanged. -/
theorem indexed_narrowing_sound {qs : List (Nat × Statement)} {i : Nat} {s : Statement}
    (hq : (i, s) ∈ qs) (hwf : s.wellFormed = true) {db db' : Db} {changes : List Change}
    (hw : Writes db changes db') (hr : rerunsIndexed qs db changes db' i s = false) :
    s.result db' = s.result db :=
  narrowing_sound hwf hw (by rw [← index_judges_alike hq]; exact hr)

end SqliveNarrowing
