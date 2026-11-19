import SqliveNarrowing.Index

/-! A transaction's net change to each row (`net_changes.rs`). A write can
change one row more than once: a statement inserts a row and the next deletes
it, or an update sets a column and a later one sets it back. sqlive judges the
write by each row's net change: the row as it was before the write's first
change of it, and as it is after the write's last change of it, and no change
at all for a row that ends as it began.

The model names the slot each change changes (`SlotChange`), as sqlive names
a row by its key, and folds the changes of each slot into one (`netSlots`),
both in `Changes.lean`, where the runner reads them; the changes, applied in
order, take the database before the write to the database after it
(`SlotWrites`). The net changes take the database before the write to the
database after it (`net_writes`), so the composed theorem holds of them
(`net_sound`, `indexed_net_sound`). They are exactly the slots whose rows
differ before and after the write (`mem_net`), so the judge's verdict on them
depends only on those two databases (`net_judges_states`), a write whose every
slot ends as it began reruns no statement (`net_unchanged`), and the judge of
the net changes reruns a statement only where the judge of every change does
(`net_reruns_imp`). -/
namespace SqliveNarrowing

attribute [local instance] Classical.propDecidable

/-- A write's row changes, applied in order, take `db` to `db'`: each changes
its own slot from its row before to its row after. -/
inductive SlotWrites : Db → List SlotChange → Db → Prop
  | done {db db' : Db} : (∀ t n, db.slot t n = db'.slot t n) → SlotWrites db [] db'
  | step {db db' : Db} {c : SlotChange} {cs : List SlotChange} :
      db.slot c.table c.slot = c.old → SlotWrites (db.put c.table c.slot c.new) cs db' →
      SlotWrites db (c :: cs) db'

/-- Changes that name their slots are changes of slots. -/
theorem SlotWrites.writes {db db' : Db} {cs : List SlotChange} (h : SlotWrites db cs db') :
    Writes db (cs.map SlotChange.change) db' := by
  induction h with
  | done h => exact Writes.done h
  | @step db db' c cs hold _ ih => exact Writes.step c.slot hold ih

theorem mem_distinct {q : Nat × Nat} : ∀ {l : List (Nat × Nat)}, q ∈ distinct l ↔ q ∈ l
  | [] => Iff.rfl
  | p :: ps => by
    simp only [distinct, List.mem_cons, List.mem_filter, mem_distinct, bne_iff_ne, ne_eq]
    by_cases h : q = p <;> simp [h]

theorem nodup_distinct : ∀ {l : List (Nat × Nat)}, (distinct l).Nodup
  | [] => List.nodup_nil
  | p :: ps => List.nodup_cons.mpr
      ⟨by simp [List.mem_filter], nodup_distinct.filter _⟩

/-- A slot the write never changes holds the same row after it. -/
theorem SlotWrites.untouched {db db' : Db} {cs : List SlotChange} (h : SlotWrites db cs db')
    {p : Nat × Nat} (hp : p ∉ cs.map SlotChange.place) : db.slot p.1 p.2 = db'.slot p.1 p.2 := by
  induction h with
  | done h => exact h p.1 p.2
  | @step db db' c cs _ _ ih =>
    have hne : p ≠ c.place := fun he => hp (by simp [he])
    have hrest : p ∉ cs.map SlotChange.place := fun hm => hp (List.mem_cons_of_mem _ hm)
    rw [← ih hrest, Db.slot_put]
    have : ¬(p.1 = c.table ∧ p.2 = c.slot) := fun ⟨h1, h2⟩ =>
      hne (by simp [SlotChange.place, ← h1, ← h2])
    simp [this]

/-- A slot's row before the write's first change of it is the slot's row
before the write. -/
theorem SlotWrites.firstOld_eq {db db' : Db} {cs : List SlotChange} (h : SlotWrites db cs db')
    {p : Nat × Nat} (hp : p ∈ cs.map SlotChange.place) : firstOld cs p = db.slot p.1 p.2 := by
  induction h with
  | done _ => cases hp
  | @step db db' c cs hold _ ih =>
    unfold firstOld
    by_cases hc : c.place = p
    · subst hc
      simp [hold, SlotChange.place]
    · have hrest : p ∈ cs.map SlotChange.place := by
        rcases List.mem_cons.mp hp with he | hm
        · exact absurd he.symm hc
        · exact hm
      have hbeq : (c.place == p) = false := by simpa using hc
      simp only [List.find?_cons, hbeq]
      have := ih hrest
      unfold firstOld at this
      rw [this, Db.slot_put]
      have : ¬(p.1 = c.table ∧ p.2 = c.slot) := fun ⟨h1, h2⟩ =>
        hc (by simp [SlotChange.place, ← h1, ← h2])
      simp [this]

/-- A slot's row after the write's last change of it is the slot's row
after the write. -/
theorem SlotWrites.lastNew_eq {db db' : Db} {cs : List SlotChange} (h : SlotWrites db cs db')
    {p : Nat × Nat} (hp : p ∈ cs.map SlotChange.place) : lastNew cs p = db'.slot p.1 p.2 := by
  induction h with
  | done _ => cases hp
  | @step db db' c cs _ hrest ih =>
    unfold lastNew
    simp only [List.reverse_cons, List.find?_append]
    by_cases hm : p ∈ cs.map SlotChange.place
    · have := ih hm
      unfold lastNew at this
      cases hf : cs.reverse.find? (·.place == p) with
      | none =>
        rw [hf] at this
        obtain ⟨c', hc', hpc⟩ := List.mem_map.mp hm
        have := List.find?_eq_none.mp hf c' (List.mem_reverse.mpr hc')
        simp [hpc] at this
      | some c' =>
        rw [hf] at this
        simpa using this
    · have hc : c.place = p := by
        rcases List.mem_cons.mp hp with he | hm'
        · exact he.symm
        · exact absurd hm' hm
      have hf : cs.reverse.find? (·.place == p) = none := by
        apply List.find?_eq_none.mpr
        intro c' hc' hpc
        exact hm (List.mem_map.mpr ⟨c', List.mem_reverse.mp hc', by simpa using hpc⟩)
      rw [hf, ← hrest.untouched hm, Db.slot_put]
      subst hc
      simp [SlotChange.place]

/-- Two functions that agree on a list's elements map it alike. -/
theorem filterMap_congr {α β : Type} {f g : α → Option β} :
    ∀ {l : List α}, (∀ x ∈ l, f x = g x) → l.filterMap f = l.filterMap g
  | [], _ => rfl
  | x :: xs, h => by
    rw [List.filterMap_cons, List.filterMap_cons, h x (by simp),
      filterMap_congr fun y hy => h y (List.mem_cons_of_mem _ hy)]

/-- The changes whose slots differ before and after a write, one for each
slot of `L` that covers every such slot, take the database before to the
database after. -/
theorem diff_writes {db' : Db} :
    ∀ {L : List (Nat × Nat)} {db : Db}, L.Nodup →
      (∀ t n, db.slot t n ≠ db'.slot t n → (t, n) ∈ L) →
      Writes db (L.filterMap fun p => if db.slot p.1 p.2 = db'.slot p.1 p.2 then none
        else some (⟨p.1, db.slot p.1 p.2, db'.slot p.1 p.2⟩ : Change)) db'
  | [], db, _, hcover => Writes.done fun t n =>
      Classical.byContradiction fun hne => by simpa using hcover t n hne
  | p :: L, db, hnd, hcover => by
    have hp : p ∉ L := (List.nodup_cons.mp hnd).1
    have hL : L.Nodup := (List.nodup_cons.mp hnd).2
    by_cases heq : db.slot p.1 p.2 = db'.slot p.1 p.2
    · rw [List.filterMap_cons, ite_eq_left_of_eq_true _ _ (eq_true heq)]
      refine diff_writes hL fun t n hne => ?_
      rcases List.mem_cons.mp (hcover t n hne) with he | hm
      · subst he; exact absurd heq hne
      · exact hm
    · rw [List.filterMap_cons, ite_eq_right_of_eq_false _ _ (eq_false heq)]
      let db1 := db.put p.1 p.2 (db'.slot p.1 p.2)
      have hslot : ∀ q ∈ L, db1.slot q.1 q.2 = db.slot q.1 q.2 := by
        intro q hq
        rw [Db.slot_put]
        have : ¬(q.1 = p.1 ∧ q.2 = p.2) := fun ⟨h1, h2⟩ => hp (by
          have : q = p := Prod.ext h1 h2
          rwa [← this])
        simp [this]
      refine Writes.step p.2 rfl ?_
      have hrest := diff_writes (db' := db') (db := db1) hL fun t n hne => by
        by_cases htn : (t, n) = p
        · subst htn
          simp [db1, Db.slot_put] at hne
        · have : db1.slot t n = db.slot t n := by
            rw [Db.slot_put]
            have : ¬(t = p.1 ∧ n = p.2) := fun ⟨h1, h2⟩ => htn (by simp [h1, h2])
            simp [this]
          rw [this] at hne
          rcases List.mem_cons.mp (hcover t n hne) with he | hm
          · exact absurd he htn
          · exact hm
      have hmap : (L.filterMap fun q => if db1.slot q.1 q.2 = db'.slot q.1 q.2 then none
          else some (⟨q.1, db1.slot q.1 q.2, db'.slot q.1 q.2⟩ : Change)) =
          L.filterMap fun q => if db.slot q.1 q.2 = db'.slot q.1 q.2 then none
            else some (⟨q.1, db.slot q.1 q.2, db'.slot q.1 q.2⟩ : Change) :=
        filterMap_congr fun q hq => by rw [hslot q hq]
      rw [← hmap]
      exact hrest

/-- The net changes are the changes of the slots whose rows differ before and
after the write, each from its row before to its row after. -/
theorem SlotWrites.net_eq {db db' : Db} {cs : List SlotChange} (h : SlotWrites db cs db') :
    net cs = (touched cs).filterMap fun p => if db.slot p.1 p.2 = db'.slot p.1 p.2 then none
      else some (⟨p.1, db.slot p.1 p.2, db'.slot p.1 p.2⟩ : Change) := by
  unfold net netSlots
  rw [List.map_filterMap]
  apply filterMap_congr
  intro p hp
  have hm : p ∈ cs.map SlotChange.place := mem_distinct.mp hp
  rw [h.firstOld_eq hm, h.lastNew_eq hm]
  split <;> rfl

/-- The net changes take the database before the write to the database after
it. -/
theorem net_writes {db db' : Db} {cs : List SlotChange} (h : SlotWrites db cs db') :
    Writes db (net cs) db' := by
  rw [h.net_eq]
  refine diff_writes (nodup_distinct) fun t n hne => ?_
  apply mem_distinct.mpr
  exact Classical.byContradiction fun hm => hne (h.untouched hm)

/-- A change is a net change exactly when it changes a slot whose row
differs before and after the write, from its row before to its row after. -/
theorem mem_net {db db' : Db} {cs : List SlotChange} (h : SlotWrites db cs db') (ch : Change) :
    ch ∈ net cs ↔ ∃ t n, db.slot t n ≠ db'.slot t n ∧ ch = ⟨t, db.slot t n, db'.slot t n⟩ := by
  rw [h.net_eq, List.mem_filterMap]
  constructor
  · rintro ⟨p, _, hp⟩
    by_cases heq : db.slot p.1 p.2 = db'.slot p.1 p.2
    · simp [heq] at hp
    · simp only [heq, ↓reduceIte, Option.some.injEq] at hp
      exact ⟨p.1, p.2, heq, hp.symm⟩
  · rintro ⟨t, n, hne, rfl⟩
    refine ⟨(t, n), mem_distinct.mpr ?_, by simp [hne]⟩
    exact Classical.byContradiction fun hm => hne (h.untouched hm)

/-- Narrowing's soundness for the net changes: for a well-formed statement,
a write whose net changes the judge lets pass leaves the statement's rows
unchanged. -/
theorem net_sound {s : Statement} (hwf : s.wellFormed = true) {db db' : Db}
    {cs : List SlotChange} (hw : SlotWrites db cs db') (hr : s.reruns db (net cs) db' = false) :
    s.result db' = s.result db :=
  narrowing_sound hwf (net_writes hw) hr

/-- The same through the index of live queries. -/
theorem indexed_net_sound {qs : List (Nat × Statement)} {i : Nat} {s : Statement}
    (hq : (i, s) ∈ qs) (hwf : s.wellFormed = true) {db db' : Db} {cs : List SlotChange}
    (hw : SlotWrites db cs db') (hr : rerunsIndexed qs db (net cs) db' i s = false) :
    s.result db' = s.result db :=
  indexed_narrowing_sound hq hwf (net_writes hw) hr

/-- The judge's verdict on two lists of changes that hold the same changes is
the same. -/
theorem reruns_congr_mem {s : Statement} {db db' : Db} {l l' : List Change}
    (h : ∀ ch, ch ∈ l ↔ ch ∈ l') : s.reruns db l db' = s.reruns db l' db' := by
  unfold Statement.reruns reruns
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true]
  constructor
  · rintro ⟨ch, hch, hr⟩; exact ⟨ch, (h ch).mp hch, hr⟩
  · rintro ⟨ch, hch, hr⟩; exact ⟨ch, (h ch).mpr hch, hr⟩

/-- The judge's verdict on a write's net changes depends only on the
database before the write and the database after it, not on the changes
that took one to the other. -/
theorem net_judges_states {s : Statement} {db db' : Db} {cs cs' : List SlotChange}
    (hw : SlotWrites db cs db') (hw' : SlotWrites db cs' db') :
    s.reruns db (net cs) db' = s.reruns db (net cs') db' :=
  reruns_congr_mem fun ch => by rw [mem_net hw, mem_net hw']

/-- A write whose every slot ends holding the row it began with, as an
insert and a delete of one row or an update and an update back, has no net
change and reruns no statement. -/
theorem net_unchanged {s : Statement} {db db' : Db} {cs : List SlotChange}
    (hw : SlotWrites db cs db') (hsame : ∀ t n, db.slot t n = db'.slot t n) :
    net cs = [] ∧ s.reruns db (net cs) db' = false := by
  have hnil : net cs = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro ch hch
    obtain ⟨t, n, hne, _⟩ := (mem_net hw ch).mp hch
    exact hne (hsame t n)
  refine ⟨hnil, ?_⟩
  rw [hnil]
  simp [Statement.reruns, reruns]

/-- The judge of a write's net changes reruns a statement only where the
judge of each of its changes does: a slot whose net change reruns the
statement changes its kept row through some change whose row before or after
passes the scope. -/
theorem net_reruns_imp {s : Statement} {db db' : Db} {cs : List SlotChange}
    (hw : SlotWrites db cs db') (h : s.reruns db (net cs) db' = true) :
    s.reruns db (cs.map SlotChange.change) db' = true := by
  apply Classical.byContradiction
  intro hne
  have hr : s.reruns db (cs.map SlotChange.change) db' = false := by simpa using hne
  have hslots := unjudged_slots hw.writes hr
  have hfalse : s.reruns db (net cs) db' = false := by
    unfold Statement.reruns reruns
    apply List.any_eq_false.mpr
    intro ch hch
    obtain ⟨t, n, _, rfl⟩ := (mem_net hw ch).mp hch
    simp only
    cases hf : s.reads.find? (·.1 == t) with
    | none => simp
    | some read =>
      obtain ⟨_, columns⟩ := read
      simp only [Bool.and_eq_true, Bool.or_eq_true, not_and, not_or]
      intro hchanged
      have hkept : s.keptSlot t (db.slot t n) ≠ s.keptSlot t (db'.slot t n) := by
        unfold Statement.keptSlot
        rw [hf]
        exact changedIn_iff.mp hchanged
      obtain ⟨h1, h2⟩ := hslots t n hkept
      unfold Statement.slotPasses at h1 h2
      rw [hf] at h1 h2
      exact ⟨by simpa using h1, by simpa using h2⟩
  rw [hfalse] at h
  cases h

end SqliveNarrowing
