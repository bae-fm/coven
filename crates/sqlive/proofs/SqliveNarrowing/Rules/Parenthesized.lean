import SqliveNarrowing.Reader

/-! A parenthesized join (`scope_plan.rs`, `read_from`): `m LEFT JOIN (p LEFT
JOIN a ON X) ON Y`, or an inner join of such a group. SQLite joins the
group's items among themselves, keeps the group's rows where `Y` holds, and
for a LEFT JOIN gives a left row that no row of the group joins NULLs in
every item of the group. sqlive reads the group's items as items of the core,
joined one after another, the first by `Y`: the runner reads a group that
way (`Json.readFrom`) only where `groupJoin`, the group as SQLite reads it,
gives the rows that reading gives. For an inner join that is always, `Y`
being one more condition on the joined rows. For a LEFT JOIN it is where `Y`
names no item of the group but its first, and each later item of the group
is a LEFT JOIN's whose ON can't hold where an item before it in the group has
no row: then a left row that no row of the group joins gets NULLs in every
item of the group, one after another, as SQLite gives it. -/
namespace SqliveNarrowing.Parenthesized

/-- Whether `e` is a column of item `i`. -/
def colOf (i : Nat) : Expr → Bool
  | .col j _ => j == i
  | _ => false

/-- Whether `cd` can't hold where item `i` has no row: a comparison other than
`IS` and `IS NOT` with a column of `i` on one side. -/
def rejects (i : Nat) : Cond → Bool
  | .cmp op l r => op != .is && op != .isNot && (colOf i l || colOf i r)
  | _ => false

/-- `b` with no row of any of `items`. -/
def unbind (b : Bind) (items : List Nat) : Bind := items.foldl (fun b i => b.set i none) b

/-- The bindings `JOIN (first rest...) ON on`, or a LEFT JOIN when `left`,
gives the binding `b`, as SQLite reads a parenthesized join: the group's
items joined among themselves from `first`, each by its own join, kept where
`on` holds, or, for a LEFT JOIN that keeps none, `b` with no row of any item
of the group. -/
def groupJoin (s : Statement) (c : Core) (rowsOf : Nat → List Row) (subs : Bind → Subs)
    (first : Nat) (rest : List Nat) (on : List Cond) (left : Bool) (b : Bind) : List Bind :=
  let kept := (joinItems s c rowsOf subs rest ((rowsOf first).map fun r => b.set first (some r))).filter
    fun b' => on.all fun cd => isTrue (cd.eval b' (subs b'))
  if left && kept.isEmpty then [unbind b (first :: rest)] else kept

/-- Whether each item of `items` is a LEFT JOIN's whose ON has a condition
that can't hold where an item of `done`, or an item of `items` before it,
has no row. -/
def rejectsEach (c : Core) : List Nat → List Nat → Bool
  | _, [] => true
  | done, i :: rest => (c.onOf i).any (fun cd => done.any (rejects · cd)) && rejectsEach c (done ++ [i]) rest

/-- Whether a LEFT JOIN of the group `first :: rest` gives the rows its items
give joined one after another, the first by its ON: the items differ, each
is a LEFT JOIN's, each after the first has an ON condition that can't hold
where an item before it in the group has no row, and the first's ON holds no
subquery and names no other item of the group. -/
def leftFlattens (s : Statement) (c : Core) (first : Nat) (rest : List Nat) : Bool :=
  decide (first :: rest).Nodup && (s.item first).left && rest.all (fun i => (s.item i).left) &&
    rejectsEach c [first] rest &&
    (c.onOf first).all fun cd => cd.queries.isEmpty && cd.items.all fun x => !rest.contains x

/-- Whether an inner join of the group `first :: rest` by `on` gives the rows
its items give joined one after another, `on` read with the WHERE: its first
item is no LEFT JOIN's and has no ON of its own, and `on` holds no subquery
and names none of the items `later` that follow the group. -/
def innerFlattens (s : Statement) (c : Core) (first : Nat) (later : List Nat) (on : List Cond) : Bool :=
  !(s.item first).left && (c.onOf first).isEmpty &&
    on.all fun cd => cd.queries.isEmpty && cd.items.all fun x => !later.contains x

/-! Joining items onto bindings. -/

theorem joinItems_append {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} (is : List Nat) (xs ys : List Bind) :
    joinItems s c rowsOf subs is (xs ++ ys) = joinItems s c rowsOf subs is xs ++ joinItems s c rowsOf subs is ys := by
  induction is generalizing xs ys with
  | nil => rfl
  | cons i is ih => simp only [joinItems, List.flatMap_append, ih]

theorem joinItems_nil {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} (is : List Nat) : joinItems s c rowsOf subs is [] = [] := by
  induction is with
  | nil => rfl
  | cons i is ih => simpa [joinItems] using ih

/-- Joining items onto bindings joins them onto each binding. -/
theorem joinItems_flatMap {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} (is : List Nat) (bs : List Bind) :
    joinItems s c rowsOf subs is bs = bs.flatMap fun b => joinItems s c rowsOf subs is [b] := by
  induction bs with
  | nil => induction is <;> simp_all [joinItems]
  | cons b bs ih =>
    rw [show b :: bs = [b] ++ bs from rfl, joinItems_append, ih]
    simp

theorem joinItems_items_append {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} (is js : List Nat) (bs : List Bind) :
    joinItems s c rowsOf subs (is ++ js) bs = joinItems s c rowsOf subs js (joinItems s c rowsOf subs is bs) := by
  induction is generalizing bs with
  | nil => rfl
  | cons i is ih => simp only [List.cons_append, joinItems, ih]

/-- A binding joining items gives holds the row of every other item that the
binding it was given holds. -/
theorem joinItems_keeps {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} (is : List Nat) {b b' : Bind}
    (h : b' ∈ joinItems s c rowsOf subs is [b]) : ∀ j, j ∉ is → b' j = b j := by
  induction is generalizing b with
  | nil =>
    simp only [joinItems, List.mem_singleton] at h
    subst h; intro _ _; rfl
  | cons i is ih =>
    intro j hj
    simp only [List.mem_cons, not_or] at hj
    simp only [joinItems] at h
    rw [joinItems_flatMap] at h
    simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil, List.mem_flatMap] at h
    obtain ⟨x, hx, hb'⟩ := h
    rw [ih hb' j hj.2]
    have hxj : x j = b j := by
      split at hx
      · simp only [List.mem_singleton] at hx
        subst hx
        simp [Bind.set, hj.1]
      · simp only [List.mem_filterMap] at hx
        obtain ⟨r, _, hr⟩ := hx
        split at hr
        · cases hr; simp [Bind.set, hj.1]
        · cases hr
    exact hxj

/-- Joining LEFT JOINs' items onto a binding gives a binding at least. -/
theorem joinItems_ne_nil {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} (is : List Nat) (hleft : ∀ i ∈ is, (s.item i).left = true) (b : Bind) :
    joinItems s c rowsOf subs is [b] ≠ [] := by
  induction is generalizing b with
  | nil => simp [joinItems]
  | cons i is ih =>
    have hi := hleft i (by simp)
    have hrest : ∀ i ∈ is, (s.item i).left = true := fun i h => hleft i (by simp [h])
    have hall : ∀ L : List Bind, L ≠ [] → joinItems s c rowsOf subs is L ≠ [] := by
      intro L hL
      obtain ⟨x, xs, rfl⟩ := List.exists_cons_of_ne_nil hL
      rw [show x :: xs = [x] ++ xs from rfl, joinItems_append]
      intro h
      exact ih hrest x (List.append_eq_nil_iff.mp h).1
    simp only [joinItems, List.flatMap_cons, List.flatMap_nil, List.append_nil]
    apply hall
    split
    · simp
    · rename_i hne
      intro hF
      exact hne (by rw [hF]; simp [hi])

/-- A comparison that rejects item `i` isn't true where `i` has no row. -/
theorem rejects_false {i : Nat} {cd : Cond} (h : rejects i cd = true) {b : Bind}
    (hb : b i = none) (sub : Subs) : isTrue (cd.eval b sub) = false := by
  cases cd with
  | cmp op l r =>
    simp only [rejects, Bool.and_eq_true, bne_iff_ne, ne_eq, Bool.or_eq_true] at h
    obtain ⟨⟨his, hisnot⟩, hcol⟩ := h
    have hnull : l.eval b sub = none ∨ r.eval b sub = none := by
      rcases hcol with h | h
      · left
        cases l <;> simp_all [colOf, Expr.eval, cellOf]
      · right
        cases r <;> simp_all [colOf, Expr.eval, cellOf]
    simp only [Cond.eval]
    rcases hnull with h | h <;> rw [h] <;> cases op <;> simp_all [Op.apply, isTrue] <;>
      split <;> simp_all
  | _ => simp [rejects] at h

/-- LEFT JOINs whose ONs each reject an item with no row, or an item joined
before them, join no row onto the binding: each gives its NULLs. -/
theorem joinItems_unbound {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} (is done : List Nat) (hleft : ∀ i ∈ is, (s.item i).left = true)
    (hnodup : (done ++ is).Nodup) (b : Bind) (hdone : ∀ j ∈ done, b j = none)
    (hrej : rejectsEach c done is = true) :
    joinItems s c rowsOf subs is [b] = [unbind b is] := by
  induction is generalizing done b with
  | nil => rfl
  | cons i is ih =>
    simp only [rejectsEach, Bool.and_eq_true, List.any_eq_true] at hrej
    obtain ⟨⟨cd, hcd, j, hj, hrj⟩, hrest⟩ := hrej
    have hji : j ≠ i := by
      intro h
      subst h
      exact (List.nodup_append.mp hnodup).2.2 j hj j (by simp) rfl
    have hjoined : ((rowsOf i).filterMap fun r =>
        let b' := b.set i (some r)
        if (c.onOf i).all (fun cd => isTrue (cd.eval b' (subs b'))) then some b' else none) = [] := by
      simp only [List.filterMap_eq_nil_iff]
      intro r _
      have hfalse : isTrue (cd.eval (b.set i (some r)) (subs (b.set i (some r)))) = false :=
        rejects_false hrj (by simp [Bind.set, hji, hdone j hj]) _
      have : (c.onOf i).all (fun cd => isTrue (cd.eval (b.set i (some r)) (subs (b.set i (some r))))) = false := by
        rw [List.all_eq_false]
        exact ⟨cd, hcd, by simp [hfalse]⟩
      simp [this]
    simp only [joinItems, List.flatMap_cons, List.flatMap_nil, List.append_nil, hjoined,
      hleft i (by simp), List.isEmpty_nil, Bool.and_self, ↓reduceIte]
    have := ih (done ++ [i]) (fun i' h => hleft i' (by simp [h])) (by simpa using hnodup) (b.set i none)
      (by
        intro j' hj'
        simp only [List.mem_append, List.mem_singleton] at hj'
        rcases hj' with hj' | rfl
        · have hne : j' ≠ i := by
            intro h; subst h
            exact (List.nodup_append.mp hnodup).2.2 j' hj' j' (by simp) rfl
          simp [Bind.set, hne, hdone j' hj']
        · simp [Bind.set]) hrest
    rw [this]
    rfl

/-- A condition that holds no subquery and names none of `is` is as true of
every binding that joining `is` gives a binding as of that binding. -/
theorem eval_joinItems {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} {is : List Nat} {cd : Cond} (hq : cd.queries = [])
    (hn : ∀ x ∈ cd.items, x ∉ is) {b b' : Bind} (h : b' ∈ joinItems s c rowsOf subs is [b]) :
    cd.eval b' (subs b') = cd.eval b (subs b) := by
  apply Cond.eval_congr
  · intro i col hic
    have hi : i ∉ is := hn i (by simp only [Cond.items, List.mem_map]; exact ⟨(i, col), hic, rfl⟩)
    rw [joinItems_keeps is h i hi]
  · intro q hq'
    rw [hq] at hq'
    cases hq'

theorem all_congr_mem {α : Type} {l : List α} {f g : α → Bool} (h : ∀ a ∈ l, f a = g a) :
    l.all f = l.all g := by
  induction l <;> simp_all

/-- Keeping the bindings a condition is true of, before or after joining items
it doesn't name, keeps the same bindings. -/
theorem filter_joinItems {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} {is : List Nat} {on : List Cond} (hq : ∀ cd ∈ on, cd.queries = [])
    (hn : ∀ cd ∈ on, ∀ x ∈ cd.items, x ∉ is) (bs : List Bind) :
    (joinItems s c rowsOf subs is bs).filter (fun b' => on.all fun cd => isTrue (cd.eval b' (subs b'))) =
      joinItems s c rowsOf subs is (bs.filter fun b' => on.all fun cd => isTrue (cd.eval b' (subs b'))) := by
  rw [joinItems_flatMap, joinItems_flatMap, List.filter_flatMap]
  induction bs with
  | nil => rfl
  | cons b bs ih =>
    simp only [List.flatMap_cons, List.filter_cons]
    have hsame : (joinItems s c rowsOf subs is [b]).filter
        (fun b' => on.all fun cd => isTrue (cd.eval b' (subs b'))) =
        if on.all (fun cd => isTrue (cd.eval b (subs b))) then joinItems s c rowsOf subs is [b] else [] := by
      split
      · rename_i htrue
        apply List.filter_eq_self.mpr
        intro b' hb'
        rw [← htrue]
        exact all_congr_mem fun cd hcd => by rw [eval_joinItems (hq cd hcd) (hn cd hcd) hb']
      · rename_i hfalse
        apply List.filter_eq_nil_iff.mpr
        intro b' hb'
        simp only [Bool.not_eq_true] at hfalse ⊢
        rw [← hfalse]
        exact all_congr_mem fun cd hcd => by rw [eval_joinItems (hq cd hcd) (hn cd hcd) hb']
    rw [hsame, ih]
    split <;> simp

theorem filterMap_set (rows : List Row) (b : Bind) (i : Nat) (P : Bind → Bool) :
    (rows.filterMap fun r => if P (b.set i (some r)) = true then some (b.set i (some r)) else none) =
      (rows.map fun r => b.set i (some r)).filter P := by
  induction rows with
  | nil => rfl
  | cons r rows ih =>
    by_cases hP : P (b.set i (some r)) = true <;> simp [hP, ih]

/-- A LEFT JOIN of a group that `leftFlattens` gives the bindings its items
give joined one after another, the first by the group's ON. -/
theorem left_group_flat {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} {first : Nat} {rest : List Nat} (h : leftFlattens s c first rest = true)
    (b : Bind) :
    groupJoin s c rowsOf subs first rest (c.onOf first) true b =
      joinItems s c rowsOf subs (first :: rest) [b] := by
  simp only [leftFlattens, Bool.and_eq_true, decide_eq_true_eq, List.all_eq_true,
    List.isEmpty_iff, Bool.not_eq_true', List.contains_eq_mem, decide_eq_false_iff_not] at h
  obtain ⟨⟨⟨⟨hnodup, hfirst⟩, hrestLeft⟩, hrej⟩, hon⟩ := h
  have hq : ∀ cd ∈ c.onOf first, cd.queries = [] := fun cd hcd => (hon cd hcd).1
  have hn : ∀ cd ∈ c.onOf first, ∀ x ∈ cd.items, x ∉ rest := fun cd hcd x hx => (hon cd hcd).2 x hx
  unfold groupJoin
  rw [filter_joinItems hq hn]
  simp only [joinItems, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  rw [filterMap_set (rowsOf first) b first
    (fun b' => (c.onOf first).all fun cd => isTrue (cd.eval b' (subs b')))]
  simp only [hfirst, Bool.true_and]
  generalize hkept : ((rowsOf first).map fun r => b.set first (some r)).filter
    (fun b' => (c.onOf first).all fun cd => isTrue (cd.eval b' (subs b'))) = kept
  cases kept with
  | nil =>
    simp only [List.isEmpty_nil, ↓reduceIte]
    rw [joinItems_nil]
    simp only [List.isEmpty_nil, ↓reduceIte]
    rw [joinItems_unbound rest [first] hrestLeft (by simpa using hnodup) (b.set first none)
      (by simp [Bind.set]) hrej]
    rfl
  | cons x xs =>
    have hne : joinItems s c rowsOf subs rest (x :: xs) ≠ [] := by
      rw [show x :: xs = [x] ++ xs from rfl, joinItems_append]
      intro h
      exact joinItems_ne_nil rest hrestLeft x (List.append_eq_nil_iff.mp h).1
    simp [List.isEmpty_iff, hne]

/-- An inner join of a group that `innerFlattens` gives, followed by the items
`later`, the bindings its items and `later` give joined one after another
that `on` is true of. -/
theorem inner_group_flat {s : Statement} {c : Core} {rowsOf : Nat → List Row}
    {subs : Bind → Subs} {first : Nat} {rest later : List Nat} {on : List Cond}
    (h : innerFlattens s c first later on = true) (b : Bind) :
    joinItems s c rowsOf subs later (groupJoin s c rowsOf subs first rest on false b) =
      (joinItems s c rowsOf subs (first :: rest ++ later) [b]).filter
        fun b' => on.all fun cd => isTrue (cd.eval b' (subs b')) := by
  simp only [innerFlattens, Bool.and_eq_true, Bool.not_eq_true', List.isEmpty_iff,
    List.all_eq_true, List.contains_eq_mem, decide_eq_false_iff_not] at h
  obtain ⟨⟨hfirst, hnoOn⟩, hon⟩ := h
  have hq : ∀ cd ∈ on, cd.queries = [] := fun cd hcd => (hon cd hcd).1
  have hn : ∀ cd ∈ on, ∀ x ∈ cd.items, x ∉ later := fun cd hcd x hx => (hon cd hcd).2 x hx
  rw [joinItems_items_append, filter_joinItems hq hn]
  unfold groupJoin
  simp only [Bool.false_and, Bool.false_eq_true, ↓reduceIte]
  congr 1
  simp only [joinItems, List.flatMap_cons, List.flatMap_nil, List.append_nil, hnoOn, hfirst]
  simp

end SqliveNarrowing.Parenthesized
