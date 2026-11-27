import SqliveNarrowing.Net

/-! sqlive records one read for each reference to a table, each FROM item that
names it, with the scope of that item and the columns the statement reads
through it (`read_capture.rs`). A write reruns a live query when some one
reference both reads a column the write changes and passes the changed row.

The model states that judge as the judge of another statement,
`Statement.split`, in which each item reads a table of its own, numbered as
the item: there each table's read is one reference's, with its own columns
and its own scope. `Db.split` gives each of those tables the rows of the table
the item names, and a write changes each of them
(`Statement.splitChanges`). The split statement returns the statement's rows
(`split_result`), so narrowing's soundness for it is the soundness of the
judge of references (`references_sound`). -/
namespace SqliveNarrowing

/-- Item `i` reading a table of its own, number `i`, where it reads a
table. -/
def Item.reading (i : Nat) (item : Item) : Item :=
  { item with kind := match item.kind with
      | .table _ => .table i
      | derived => derived }

/-- The statement with each item that reads a table reading its own, and no
column reported beyond its clauses'. -/
def Statement.referenced (s : Statement) : Statement :=
  { s with items := s.items.mapIdx fun i item => item.reading i, reported := [] }

/-- The items that read table `t`: its references. -/
def Statement.references (s : Statement) (t : Nat) : List Nat :=
  s.itemNumbers.filter fun i => s.table? i = some t

/-- The statement sqlive's judge of references judges: each item reading its
own table. A column SQLite reports of a table that no reference's read
holds, one only a column nothing reads names, goes to every reference of the
table. -/
def Statement.split (s : Statement) : Statement :=
  { s.referenced with reported := s.reported.flatMap fun (t, c) =>
      if (s.references t).any (fun i => (readColumns s.referenced.reads i).contains c) then []
      else (s.references t).map (·, c) }

/-- Each reference's table, holding the rows of the table it names. -/
def Db.split (s : Statement) (db : Db) : Db := fun i =>
  match s.table? i with
  | some t => if i < s.items.length then db t else []
  | none => []

/-- A change of a table's row, as a change of the row in each reference's
table. -/
def Statement.splitChanges (s : Statement) (changes : List Change) : List Change :=
  changes.flatMap fun ch => (s.references ch.table).map fun i => { ch with table := i }

/-- Whether sqlive reruns the statement for a write, whose question stopped
at the step limit or not: whether the split statement's judge reruns it for
the write's changes of each reference's table. -/
def Statement.rerunsReferences (s : Statement) (db : Db) (changes : List Change) (db' : Db)
    (stopped : Bool) : Bool :=
  s.split.rerunsLimited (db.split s) (s.splitChanges changes) (db'.split s) stopped

/-! The split statement is the statement but for the tables its items name. -/

theorem Statement.split_item_of_lt (s : Statement) {i : Nat} (hi : i < s.items.length) :
    s.split.item i = (s.item i).reading i := by
  simp [Statement.item, Statement.split, Statement.referenced, List.getD_eq_getElem?_getD,
    List.getElem?_mapIdx, List.getElem?_eq_getElem hi]

theorem Statement.split_item_of_le (s : Statement) {i : Nat} (hi : s.items.length ≤ i) :
    s.split.item i = s.item i := by
  simp [Statement.item, Statement.split, Statement.referenced, List.getD_eq_getElem?_getD,
    List.getElem?_mapIdx, List.getElem?_eq_none hi]

/-- Each item is as it was but for the table it names. -/
theorem Statement.split_item (s : Statement) (i : Nat) :
    s.split.item i = { s.item i with kind := (s.split.item i).kind } := by
  by_cases hi : i < s.items.length
  · rw [s.split_item_of_lt hi]; rfl
  · rw [s.split_item_of_le (Nat.le_of_not_lt hi)]

/-- Each item reads what it read, or a table where it read a table. -/
theorem Statement.split_kind (s : Statement) (i : Nat) :
    (s.split.item i).kind = (s.item i).kind ∨
      ∃ a b, (s.split.item i).kind = .table a ∧ (s.item i).kind = .table b := by
  by_cases hi : i < s.items.length
  · rw [s.split_item_of_lt hi]
    cases h : (s.item i).kind with
    | table t => exact Or.inr ⟨i, t, by simp [Item.reading, h], rfl⟩
    | derived q v => exact Or.inl (by simp [Item.reading, h])
  · exact Or.inl (by rw [s.split_item_of_le (Nat.le_of_not_lt hi)])

theorem Statement.split_left (s : Statement) (i : Nat) :
    (s.split.item i).left = (s.item i).left := by rw [s.split_item]

theorem Statement.split_core_of_item (s : Statement) (i : Nat) :
    (s.split.item i).core = (s.item i).core := by rw [s.split_item]

theorem Statement.split_readable (s : Statement) (i : Nat) :
    (s.split.item i).readable = (s.item i).readable := by rw [s.split_item]

theorem Statement.split_length (s : Statement) : s.split.items.length = s.items.length := by
  simp [Statement.split, Statement.referenced]

theorem Statement.split_nestedQueries (s : Statement) :
    s.split.nestedQueries = s.nestedQueries := by
  funext k
  unfold Statement.nestedQueries
  congr 1
  refine congrArg (fun f => List.filterMap f (s.core k).items) (funext fun i => ?_)
  rcases s.split_kind i with h | ⟨a, b, ha, hb⟩
  · rw [h]
  · rw [ha, hb]

theorem Statement.split_queryCores (s : Statement) : s.split.queryCores = s.queryCores := by
  funext f
  induction f with
  | zero => rfl
  | succ f ih =>
    funext q
    simp only [Statement.queryCores, ih, s.split_nestedQueries]
    rfl

theorem Statement.split_outerNames (s : Statement) : s.split.outerNames = s.outerNames := by
  funext q
  unfold Statement.outerNames
  simp only [s.split_queryCores, s.split_core_of_item]
  rfl

theorem Statement.split_closed (s : Statement) (l : List Nat) :
    View.closed s.split l = View.closed s l := by
  unfold View.closed
  have harms : View.armsOf s.split l = View.armsOf s l := rfl
  have hinside : View.inside s.split l = View.inside s l := by
    funext i
    unfold View.inside
    rw [s.split_core_of_item]
    rfl
  rw [harms, hinside]
  refine congrArg (fun f => (View.armsOf s l).all f) (funext fun k => ?_)
  have hc : s.split.core k = s.core k := rfl
  simp only [hc]
  congr 2
  rw [Bool.eq_iff_iff, List.all_eq_true, List.all_eq_true]
  refine forall_congr' fun i => imp_congr_right fun _ => ?_
  rcases s.split_kind i with h | ⟨a, b, ha, hb⟩
  · rw [h]
  · rw [ha, hb]

theorem Statement.split_scopeChain (s : Statement) : s.split.scopeChain = s.scopeChain := by
  funext k
  unfold Statement.scopeChain
  have : ∀ f k, Statement.scopeChain.go s.split f k = Statement.scopeChain.go s f k := by
    intro f
    induction f with
    | zero => intro k; rfl
    | succ f ih =>
      intro k
      simp only [Statement.scopeChain.go, ih]
      rfl
  exact this _ _

theorem Statement.split_wellFormed {s : Statement} (h : s.wellFormed = true) :
    s.split.wellFormed = true := by
  unfold Statement.wellFormed at h ⊢
  simp only [Bool.and_eq_true] at h ⊢
  obtain ⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨hroot, harms⟩, hitems⟩, hkinds⟩, hons⟩, hrefs⟩, hnodup⟩, hread⟩, harmCores⟩,
    houtside⟩, hafter⟩, hmatch⟩, hsubs⟩ := h
  refine ⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨hroot, ?_⟩, ?_⟩, ?_⟩, ?_⟩, ?_⟩, hnodup⟩, ?_⟩, harmCores⟩,
    houtside⟩, hafter⟩, ?_⟩, ?_⟩
  · rw [s.split_nestedQueries]
    exact harms
  · rw [s.split_length]
    simp only [s.split_core_of_item]
    exact hitems
  · have hclosed : s.split.readsAlone = s.readsAlone := by
      funext q
      unfold Statement.readsAlone
      simp only [s.split_closed]
      rfl
    rw [s.split_length, List.all_eq_true]
    rw [List.all_eq_true] at hkinds
    intro i hi
    have := hkinds i hi
    rw [hclosed, s.split_readable]
    rcases s.split_kind i with h | ⟨a, b, ha, hb⟩
    · rw [h]; exact this
    · rw [ha]
  · have : s.split.onsNameEarlier = s.onsNameEarlier := by
      funext k
      unfold Statement.onsNameEarlier
      simp only [s.split_left]
      rfl
    rw [this]
    exact hons
  · rw [s.split_scopeChain]
    exact hrefs
  · have : s.split.readQueries = s.readQueries := by
      unfold Statement.readQueries
      rw [s.split_nestedQueries]
      rfl
    rw [this]
    exact hread
  · unfold Statement.parentsMatch at hmatch ⊢
    rw [List.all_eq_true] at hmatch ⊢
    intro k hk
    have := hmatch k hk
    rw [Bool.and_eq_true] at this ⊢
    refine ⟨this.1, ?_⟩
    have hkitems := this.2
    rw [List.all_eq_true] at hkitems ⊢
    intro i hi
    have := hkitems i hi
    rcases s.split_kind i with h | ⟨a, b, ha, hb⟩
    · rw [h]; exact this
    · rw [ha]
  · unfold Statement.onSubqueriesNameEarlier at hsubs ⊢
    simp only [s.split_outerNames, s.split_core_of_item]
    exact hsubs

/-! The split statement returns the statement's rows. -/

theorem Db.split_rows (s : Statement) (db : Db) {i t : Nat} (hi : i < s.items.length)
    (ht : s.table? i = some t) : (db.split s).rows i = db.rows t := by
  simp [Db.rows, Db.split, ht, hi]

theorem joinItems_congr_left {s s' : Statement} (hleft : ∀ i, (s'.item i).left = (s.item i).left)
    (c : Core) (rowsOf rowsOf' : Nat → List Row) (subs : Bind → Subs) :
    ∀ (is : List Nat), (∀ i ∈ is, rowsOf' i = rowsOf i) → ∀ bs,
      joinItems s' c rowsOf' subs is bs = joinItems s c rowsOf subs is bs
  | [], _, _ => by simp [joinItems]
  | i :: is, hrows, bs => by
    rw [View.joinItems_cons, View.joinItems_cons,
      joinItems_congr_left hleft c rowsOf rowsOf' subs is (fun j hj => hrows j (by simp [hj]))]
    congr 2
    funext b
    unfold View.joinStep
    rw [hrows i (by simp), hleft]

theorem split_evalQuery {s : Statement}
    (hitems : ∀ k, ∀ i ∈ (s.core k).items, i < s.items.length) (db : Db) :
    ∀ f b q, evalQuery s.split (db.split s) f b q = evalQuery s db f b q
  | 0, _, _ => rfl
  | f + 1, b, q => by
    have ih : evalQuery s.split (db.split s) f = evalQuery s db f :=
      funext fun b => funext fun q => split_evalQuery hitems db f b q
    have hcore : ∀ k, coreBindings s.split (db.split s) (evalQuery s db f) b k =
        coreBindings s db (evalQuery s db f) b k := by
      intro k
      unfold coreBindings
      have hc : s.split.core k = s.core k := rfl
      simp only [hc]
      congr 1
      refine joinItems_congr_left s.split_left _ _ _ _ _ (fun i hi => ?_) _
      have hi' := hitems k i hi
      rcases s.split_kind i with h | ⟨a, t, ha, ht⟩
      · rw [h]
        cases hk : (s.item i).kind with
        | derived q v => rfl
        | table t =>
          rw [s.split_item_of_lt hi'] at h
          simp only [Item.reading, hk, ItemKind.table.injEq] at h
          subst h
          exact Db.split_rows s db hi' (by simp [Statement.table?, hk])
      · rw [ha, ht]
        have : a = i := by
          rw [s.split_item_of_lt hi'] at ha
          simpa [Item.reading, ht] using ha.symm
        subst this
        exact Db.split_rows s db hi' (by simp [Statement.table?, ht])
    simp only [evalQuery, queryRows, ih, hcore]
    rfl

theorem split_result {s : Statement} (hwf : s.wellFormed = true) (db : Db) :
    s.split.result (db.split s) = s.result db :=
  split_evalQuery (fun k i hi => s.wellFormed_items hwf (k := k) (i := i) hi) db _ _ _

/-! A write changes the references' tables. -/

theorem Db.put_slot_congr {a b : Db} (h : ∀ t n, a.slot t n = b.slot t n) (t n : Nat)
    (r : Option Row) : ∀ t' m, (a.put t n r).slot t' m = (b.put t n r).slot t' m := by
  intro t' m
  rw [Db.slot_put, Db.slot_put, h]

theorem Writes.of_slots {a b c : Db} {changes : List Change}
    (h : ∀ t n, a.slot t n = b.slot t n) (hw : Writes b changes c) : Writes a changes c := by
  induction hw generalizing a with
  | done hs => exact .done fun t n => (h t n).trans (hs t n)
  | step n hold _ ih =>
    exact .step n ((h _ _).trans hold) (ih (Db.put_slot_congr h _ _ _))

theorem Writes.append {a b c : Db} {first rest : List Change} (hfirst : Writes a first b)
    (hrest : Writes b rest c) : Writes a (first ++ rest) c := by
  induction hfirst with
  | done h => exact Writes.of_slots h hrest
  | step n hold _ ih => exact .step n hold (ih hrest)

/-- One change made to slot `n` of each table `tables` names, each holding
the row before. -/
theorem Writes.each {ch : Change} {n : Nat} :
    ∀ (tables : List Nat), tables.Nodup → ∀ (db : Db),
      (∀ i ∈ tables, db.slot i n = ch.old) →
      ∃ db', Writes db (tables.map fun i => { ch with table := i }) db' ∧
        ∀ t m, db'.slot t m = if t ∈ tables ∧ m = n then ch.new else db.slot t m
  | [], _, db, _ => ⟨db, .done fun _ _ => rfl, by simp⟩
  | i :: tables, hnd, db, hold => by
    obtain ⟨hni, hnd'⟩ := List.nodup_cons.mp hnd
    obtain ⟨db', hw, hs⟩ := Writes.each (ch := ch) (n := n) tables hnd' (db.put i n ch.new)
      (fun j hj => by
        have hji : j ≠ i := fun e => hni (e ▸ hj)
        rw [Db.slot_put]
        simp only [hji, false_and, ↓reduceIte]
        exact hold j (by simp [hj]))
    refine ⟨db', .step n (hold i (by simp)) hw, fun t m => ?_⟩
    rw [hs, Db.slot_put]
    by_cases hm : m = n
    · by_cases ht : t = i
      · simp [hm, ht]
      · simp [hm, ht]
    · simp [hm]

theorem Statement.mem_references {s : Statement} {i t : Nat} :
    i ∈ s.references t ↔ i < s.items.length ∧ s.table? i = some t := by
  simp [Statement.references, Statement.itemNumbers]

theorem Db.split_slot (s : Statement) (db : Db) (i n : Nat) :
    (db.split s).slot i n =
      match s.table? i with
      | some t => if i < s.items.length then db.slot t n else none
      | none => none := by
  unfold Db.slot Db.split
  cases s.table? i with
  | none => simp
  | some t =>
    by_cases hi : i < s.items.length <;> simp [hi]

theorem split_writes {s : Statement} {db db' : Db} {changes : List Change}
    (hw : Writes db changes db') :
    Writes (db.split s) (s.splitChanges changes) (db'.split s) := by
  induction hw with
  | done h =>
    refine .done fun i n => ?_
    simp only [Db.split_slot, h]
  | @step db db' ch changes n hold _ ih =>
    unfold Statement.splitChanges
    rw [List.flatMap_cons]
    have hnodup : (s.references ch.table).Nodup := List.nodup_range.filter _
    obtain ⟨next, hfirst, hslots⟩ := Writes.each (ch := ch) (n := n) _ hnodup (db.split s)
      (fun i hi => by
        obtain ⟨hlt, ht⟩ := Statement.mem_references.mp hi
        rw [Db.split_slot, ht]
        simpa [hlt] using hold)
    refine hfirst.append (Writes.of_slots (fun i m => ?_) ih)
    rw [hslots, Db.split_slot, Db.split_slot]
    simp only [Statement.mem_references, Db.slot_put]
    cases ht : s.table? i with
    | none => simp
    | some t =>
      by_cases hi : i < s.items.length
      · by_cases htc : t = ch.table
        · simp [hi, htc]
        · simp [hi, htc]
      · simp [hi]

/-! Soundness. -/

/-- A write sqlive's judge of references lets pass leaves the statement's
rows unchanged. -/
theorem references_sound {s : Statement} (hwf : s.wellFormed = true) {db db' : Db}
    {changes : List Change} {stopped : Bool} (hw : Writes db changes db')
    (hr : s.rerunsReferences db changes db' stopped = false) : s.result db' = s.result db := by
  rw [← split_result hwf db, ← split_result hwf db']
  exact narrowing_sound_limited (Statement.split_wellFormed hwf) (split_writes hw) hr

/-- The same for a write's net change of each row, which sqlive judges. -/
theorem references_net_sound {s : Statement} (hwf : s.wellFormed = true) {db db' : Db}
    {cs : List SlotChange} {stopped : Bool} (hw : SlotWrites db cs db')
    (hr : s.rerunsReferences db (net cs) db' stopped = false) : s.result db' = s.result db :=
  references_sound hwf (net_writes hw) hr

/-- The same through the index of waiting live queries, which files each
reference of each live query under its own scope's key (`read_index.rs`). The
index finds a change of a reference's table by the entry filed for that
reference, whatever else it holds, so the model states one live query's
entries. -/
theorem references_indexed_sound {s : Statement} (hwf : s.wellFormed = true) {db db' : Db}
    {changes : List Change} {i : Nat} (hw : Writes db changes db')
    (hr : rerunsIndexed [(i, s.split)] (db.split s) (s.splitChanges changes) (db'.split s) i
      s.split = false) : s.result db' = s.result db := by
  rw [← split_result hwf db, ← split_result hwf db']
  exact indexed_narrowing_sound (List.mem_singleton.mpr rfl) (Statement.split_wellFormed hwf)
    (split_writes hw) hr

end SqliveNarrowing
