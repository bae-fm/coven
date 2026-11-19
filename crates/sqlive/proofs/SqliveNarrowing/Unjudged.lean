import SqliveNarrowing.Mask
import SqliveNarrowing.Judge

/-! A write the judge lets pass leaves
unchanged, in what sqlive keeps of each table (`Db.mask`), every row that
passes the table's scope. A write's row changes take the database before it
to the database after it one slot at a time (`Writes`), and each slot whose
masked row the write changes has a judged change for its row before and for
its row after, so neither passes the scope (`Writes.unjudged`). The rows
that pass the scopes are then the same before and after
(`unjudged_rows`). A statement read whole has no scope, so it returns the
same rows (`whole_narrowing_sound`). -/
namespace SqliveNarrowing

/-- Slot `n` of table `t`: `none` past the table's last slot. -/
def Db.slot (db : Db) (t n : Nat) : Option Row := ((db t)[n]?).join

/-- The database with slot `n` of table `t` holding `r`. -/
def Db.put (db : Db) (t n : Nat) (r : Option Row) : Db := fun t' =>
  if t' = t then
    (List.range (max (db t).length (n + 1))).map fun m => if m = n then r else db.slot t m
  else db t'

theorem Db.slot_put (db : Db) (t n : Nat) (r : Option Row) (t' m : Nat) :
    (db.put t n r).slot t' m = if t' = t ∧ m = n then r else db.slot t' m := by
  unfold Db.put Db.slot
  by_cases ht : t' = t
  · subst ht
    simp only [↓reduceIte, true_and, List.getElem?_map]
    by_cases hm : m < max (db t').length (n + 1)
    · rw [List.getElem?_range hm]
      by_cases hmn : m = n <;> simp [hmn]
    · rw [List.getElem?_eq_none (by simpa using hm)]
      have hmn : m ≠ n := by omega
      have hl : (db t').length ≤ m := by omega
      simp [hmn, List.getElem?_eq_none hl]
  · simp [ht]

/-- A write's row changes, applied in order, take `db` to `db'`: each changes
one slot of its table from its row before to its row after, as sqlive's
pre-update hook reports them and the differential's mirror records them. -/
inductive Writes : Db → List Change → Db → Prop
  | done {db db' : Db} : (∀ t n, db.slot t n = db'.slot t n) → Writes db [] db'
  | step {db db' : Db} {ch : Change} {chs : List Change} (n : Nat) :
      db.slot ch.table n = ch.old → Writes (db.put ch.table n ch.new) chs db' →
      Writes db (ch :: chs) db'

/-- When every change that changes what `m` keeps of its row has a row before
and a row after that fail `P`, and `P` reads only what `m` keeps, every slot
whose kept row the write changes holds rows before and after that fail
`P`. -/
theorem Writes.unjudged {db db' : Db} {changes : List Change} (hw : Writes db changes db')
    {m : Nat → Option Row → Option Row} {P : Nat → Option Row → Bool}
    (hP : ∀ t x y, m t x = m t y → P t x = P t y)
    (hj : ∀ ch ∈ changes, m ch.table ch.old ≠ m ch.table ch.new →
      P ch.table ch.old = false ∧ P ch.table ch.new = false) :
    ∀ t n, m t (db.slot t n) ≠ m t (db'.slot t n) →
      P t (db.slot t n) = false ∧ P t (db'.slot t n) = false := by
  induction hw with
  | done h => intro t n hne; exact absurd (by rw [h t n]) hne
  | @step db db' ch chs n0 hold hrest ih =>
    intro t n hne
    have ih := ih (fun ch' hch' => hj ch' (List.mem_cons_of_mem _ hch'))
    by_cases hslot : t = ch.table ∧ n = n0
    · obtain ⟨ht, hn⟩ := hslot
      have hnew : (db.put ch.table n0 ch.new).slot t n = ch.new := by
        simp [Db.slot_put, ht, hn]
      have hold' : db.slot t n = ch.old := by rw [ht, hn]; exact hold
      have hjc := hj ch (by simp)
      rw [← ht] at hjc
      rw [hold'] at hne ⊢
      have ih' : m t ch.new ≠ m t (db'.slot t n) →
          P t ch.new = false ∧ P t (db'.slot t n) = false := by
        intro h
        have := ih t n (by rwa [hnew])
        rwa [hnew] at this
      by_cases hxy : m t ch.old = m t ch.new
      · rw [hxy] at hne
        obtain ⟨h1, h2⟩ := ih' hne
        exact ⟨by rw [hP t _ _ hxy, h1], h2⟩
      · obtain ⟨h1, h2⟩ := hjc hxy
        refine ⟨h1, ?_⟩
        by_cases hyz : m t ch.new = m t (db'.slot t n)
        · rw [← hP t _ _ hyz, h2]
        · exact (ih' hyz).2
    · have hsame : (db.put ch.table n0 ch.new).slot t n = db.slot t n := by
        rw [Db.slot_put]; simp [hslot]
      have := ih t n (by rwa [hsame])
      rwa [hsame] at this

theorem filter_cons_rows {Q : Row → Bool} (x : Option Row) (xs : List (Option Row)) :
    ((x :: xs).filterMap id).filter Q = (x.filter Q).toList ++ (xs.filterMap id).filter Q := by
  cases x with
  | none => simp
  | some r => cases h : Q r <;> simp [Option.filter, h]

/-- Slots that hold no row passing `Q` hold no such row. -/
theorem filter_rows_of_none {Q : Row → Bool} :
    ∀ {l : List (Option Row)}, (∀ n : Nat, ((l[n]?).join).filter Q = none) →
      (l.filterMap id).filter Q = []
  | [], _ => rfl
  | x :: xs, h => by
    have h0 : x.filter Q = none := by simpa using h 0
    rw [filter_cons_rows, h0, filter_rows_of_none fun n => by simpa using h (n + 1)]
    rfl

/-- Lists of slots that agree, slot by slot, in the rows that pass `Q` hold
the same rows that pass `Q`, in order. -/
theorem filter_rows_of_slots {Q : Row → Bool} :
    ∀ {l l' : List (Option Row)}, (∀ n : Nat, ((l[n]?).join).filter Q = ((l'[n]?).join).filter Q) →
      (l.filterMap id).filter Q = (l'.filterMap id).filter Q
  | [], l', h => (filter_rows_of_none fun n => by simpa using (h n).symm).symm
  | l, [], h => filter_rows_of_none fun n => by simpa using h n
  | x :: xs, y :: ys, h => by
    have h0 : x.filter Q = y.filter Q := by simpa using h 0
    have hrest := filter_rows_of_slots (l := xs) (l' := ys) fun n => by simpa using h (n + 1)
    rw [filter_cons_rows, filter_cons_rows, h0, hrest]

/-- What sqlive keeps of slot `n` of table `t`. -/
theorem Db.mask_slot (reads : List (Nat × List Nat)) (db : Db) (t n : Nat) :
    (db.mask reads).slot t n =
      match reads.find? (·.1 == t) with
      | some (_, columns) => (db.slot t n).map (maskRow columns)
      | none => none := by
  cases hf : reads.find? (·.1 == t) with
  | none => simp [Db.mask, Db.slot, hf]
  | some p =>
    obtain ⟨_, columns⟩ := p
    simp only [Db.mask, Db.slot, hf, List.getElem?_map]
    cases (db t)[n]? <;> rfl

/-- What sqlive keeps of row `x` of a slot of table `t` for statement `s`:
the row masked to the columns `s` reads of the table, and nothing of a table
`s` doesn't read. -/
def Statement.keptSlot (s : Statement) (t : Nat) (x : Option Row) : Option Row :=
  match s.reads.find? (·.1 == t) with
  | some (_, columns) => x.map (maskRow columns)
  | none => none

/-- Whether row `x` of a slot of table `t` passes the table's scope as the
judge tests it, for the run that read `db`, with lookups in what sqlive keeps
of `db'`: never for a table `s` doesn't read. -/
def Statement.slotPasses (s : Statement) (db db' : Db) (t : Nat) (x : Option Row) : Bool :=
  match s.reads.find? (·.1 == t) with
  | some _ => x.any (scopePasses s ((s.tableScopes db).getD fun _ => none) s.reads db' t)
  | none => false

/-- When the judge lets a write pass, each slot whose kept row the write
changes holds rows that fail its table's scope, before the write and after
it. -/
theorem unjudged_slots {s : Statement} {db db' : Db} {changes : List Change}
    (hw : Writes db changes db') (hr : s.reruns db changes db' = false) :
    ∀ t n, s.keptSlot t (db.slot t n) ≠ s.keptSlot t (db'.slot t n) →
      s.slotPasses db db' t (db.slot t n) = false ∧
        s.slotPasses db db' t (db'.slot t n) = false := by
  have hcols : ∀ t columns, s.reads.find? (·.1 == t) = some columns →
      readColumns s.reads t = columns.2 := by
    intro t columns h; simp [readColumns, h]
  have hP : ∀ t x y, s.keptSlot t x = s.keptSlot t y →
      s.slotPasses db db' t x = s.slotPasses db db' t y := by
    intro t x y h
    unfold Statement.keptSlot at h
    unfold Statement.slotPasses
    cases hf : s.reads.find? (·.1 == t) with
    | none => rfl
    | some read =>
      rw [hf] at h
      obtain ⟨_, columns⟩ := read
      simp only at h ⊢
      unfold scopePasses
      rw [hcols t _ hf]
      cases x <;> cases y <;> simp_all
  have hj : ∀ ch ∈ changes, s.keptSlot ch.table ch.old ≠ s.keptSlot ch.table ch.new →
      s.slotPasses db db' ch.table ch.old = false ∧
        s.slotPasses db db' ch.table ch.new = false := by
    intro ch hch hne
    unfold Statement.keptSlot at hne
    unfold Statement.slotPasses
    cases hf : s.reads.find? (·.1 == ch.table) with
    | none => rw [hf] at hne; exact absurd rfl hne
    | some read =>
      rw [hf] at hne
      obtain ⟨_, columns⟩ := read
      simp only at hne ⊢
      have hchanged : changedIn columns ch.old ch.new = true := changedIn_iff.mpr hne
      unfold Statement.reruns reruns at hr
      have := List.any_eq_false.mp hr ch hch
      rw [hf] at this
      simp only [hchanged, Bool.true_and, Bool.or_eq_true, not_or, Bool.not_eq_true] at this
      exact this
  exact hw.unjudged hP hj

/-- When the judge lets a write pass, what sqlive keeps of each table holds,
before and after the write, the same rows that pass the table's scope, with
lookups in what it keeps of the database after the write. -/
theorem unjudged_rows {s : Statement} {db db' : Db} {changes : List Change}
    (hw : Writes db changes db') (hr : s.reruns db changes db' = false) (t : Nat) :
    let scopeOf := (s.tableScopes db).getD fun _ => none
    let passes (r : Row) := ((scopeOf t).getD .all).passes (lookupRows s (db'.mask s.reads)) r
    ((db.mask s.reads).rows t).filter passes = ((db'.mask s.reads).rows t).filter passes := by
  intro scopeOf passes
  let m (t : Nat) (x : Option Row) : Option Row :=
    match s.reads.find? (·.1 == t) with
    | some (_, columns) => x.map (maskRow columns)
    | none => none
  let P (t : Nat) (x : Option Row) : Bool :=
    match s.reads.find? (·.1 == t) with
    | some _ => x.any (scopePasses s scopeOf s.reads db' t)
    | none => false
  have hcols : ∀ t columns, s.reads.find? (·.1 == t) = some columns →
      readColumns s.reads t = columns.2 := by
    intro t columns h; simp [readColumns, h]
  have hP : ∀ t x y, m t x = m t y → P t x = P t y := by
    intro t x y h
    simp only [m, P] at h ⊢
    cases hf : s.reads.find? (·.1 == t) with
    | none => rfl
    | some read =>
      rw [hf] at h
      obtain ⟨_, columns⟩ := read
      simp only at h ⊢
      unfold scopePasses
      rw [hcols t _ hf]
      cases x <;> cases y <;> simp_all
  have hslots : ∀ t n, m t (db.slot t n) ≠ m t (db'.slot t n) →
      P t (db.slot t n) = false ∧ P t (db'.slot t n) = false :=
    unjudged_slots hw hr
  have hmask : ∀ (d : Db) n, ((d.mask s.reads t)[n]?).join = m t (d.slot t n) := by
    intro d n
    exact Db.mask_slot s.reads d t n
  apply filter_rows_of_slots
  intro n
  rw [hmask db n, hmask db' n]
  have hfilter : ∀ x : Option Row, (m t x).filter passes = cond (P t x) (m t x) none := by
    intro x
    simp only [m, P]
    cases hf : s.reads.find? (·.1 == t) with
    | none => simp
    | some read =>
      obtain ⟨_, columns⟩ := read
      cases x with
      | none => simp
      | some r =>
        simp only [Option.map_some, Option.filter, Option.any_some]
        unfold scopePasses
        rw [hcols t _ hf]
        simp only [passes, scopeOf]
        split <;> simp_all
  rw [hfilter, hfilter]
  rcases Classical.em (m t (db.slot t n) = m t (db'.slot t n)) with heq | heq
  · rw [heq, hP t _ _ heq]
  · obtain ⟨h1, h2⟩ := hslots t n heq
    rw [h1, h2]
    rfl

/-- A statement returns the same rows from two databases whose tables hold
the same rows. -/
theorem evalQuery_rows_congr {s : Statement} {db db' : Db} (h : ∀ t, db.rows t = db'.rows t) :
    ∀ f b q, evalQuery s db f b q = evalQuery s db' f b q
  | 0, _, _ => rfl
  | f + 1, b, q => by
    have ih : evalQuery s db f = evalQuery s db' f :=
      funext fun b => funext fun q => evalQuery_rows_congr h f b q
    simp only [evalQuery, queryRows, coreBindings, h, ih]

/-- For a statement sqlive reads whole, a write the judge lets pass leaves
its rows unchanged. -/
theorem whole_narrowing_sound {s : Statement} (hwf : s.wellFormed = true)
    (hwhole : Whole.readsWhole s = true) {db db' : Db} {changes : List Change}
    (hw : Writes db changes db') (hr : s.reruns db changes db' = false) :
    s.result db' = s.result db := by
  have hitems := fun k i hi => s.wellFormed_items hwf (k := k) (i := i) hi
  rw [← result_mask hitems db, ← result_mask hitems db']
  have hnone : s.tableScopes db = none := by simp [Statement.tableScopes, hwhole]
  have hrows : ∀ t, (db'.mask s.reads).rows t = (db.mask s.reads).rows t := by
    intro t
    have := unjudged_rows hw hr t
    simp only [hnone, Option.getD_none, RScope.passes] at this
    rw [List.filter_eq_self.mpr (by simp), List.filter_eq_self.mpr (by simp)] at this
    exact this.symm
  unfold Statement.result
  exact evalQuery_rows_congr hrows _ _ _

end SqliveNarrowing
