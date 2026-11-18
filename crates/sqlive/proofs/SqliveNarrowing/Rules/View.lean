import SqliveNarrowing.Reader

/-! Rule 5: one of the app's views is read as its query's cores, like a FROM
subquery, and a lookup reads it alone by its name wherever the statement
reads it. A FROM subquery or CTE is readable alone outside any expression
subquery, where nothing around it can name an outer row, and inside one when
its SQL names no row outside it (`read_derived_item`). -/
namespace SqliveNarrowing.View

/-- Whether a derived item is readable alone: a view always, and a FROM
subquery or a CTE when the core holding it sits in no other, or when its
query names nothing outside it (`closed`). -/
def readable (view : Bool) (parent : Option Nat) (closed : Bool) : Bool :=
  view || parent.isNone || closed

/-- Rule 5's theorem: a query whose arms sit in no other core, as a view's
or a FROM subquery's or CTE's outside any expression subquery, returns the
same rows under every binding around it, so a lookup that reads it alone
reads the rows the statement reads. -/
theorem view_sound {s : Statement} {q : Nat} (h : ∀ k ∈ (s.query q).arms, (s.core k).parent = none)
    (db : Db) (f : Nat) (b : Bind) : evalQuery s db f b q = evalQuery s db f Bind.empty q := by
  cases f with
  | zero => rfl
  | succ f =>
    simp only [evalQuery, queryRows]
    congr 2
    apply List.map_congr_left
    intro k hk
    simp [coreBindings, h k hk]

/-- The cores of the arms of queries `qs`. -/
def armsOf (s : Statement) (qs : List Nat) : List Nat := qs.flatMap fun q => (s.query q).arms

/-- Whether item `i` is an item of an arm of one of queries `qs`. -/
def inside (s : Statement) (qs : List Nat) (i : Nat) : Bool := (armsOf s qs).contains (s.item i).core

/-- The queries a core's clauses hold: its result columns', conditions' and
GROUP BY's. -/
def clauseQueries (c : Core) : List Nat :=
  c.output.flatMap (fun
    | .expr e | .max e => e.queries
    | .count => []) ++ c.conds.flatMap (·.1.queries) ++ c.groupBy.flatMap Expr.queries

/-- Whether queries `qs` name nothing outside them: each arm's subqueries
and derived items' queries are among them, and each column an arm names is
of an item of one of their arms. -/
def closed (s : Statement) (qs : List Nat) : Bool :=
  (armsOf s qs).all fun k =>
    let c := s.core k
    (c.items.all fun i => match (s.item i).kind with
      | .derived q _ => qs.contains q
      | .table _ => true) &&
    (clauseQueries c).all qs.contains &&
    c.refs.all fun (i, _) => inside s qs i

/-- A binding's rows of the items inside queries `qs`, and no other. -/
def restrict (s : Statement) (qs : List Nat) (b : Bind) : Bind :=
  fun i => if inside s qs i then b i else none

theorem restrict_restrict (s : Statement) (qs : List Nat) (b : Bind) :
    restrict s qs (restrict s qs b) = restrict s qs b := by
  funext i; unfold restrict; split <;> simp_all

theorem restrict_set (s : Statement) (qs : List Nat) (b : Bind) (i : Nat) (r : Option Row) :
    restrict s qs (b.set i r) = restrict s qs ((restrict s qs b).set i r) := by
  funext j
  by_cases hj : j = i <;> by_cases hin : inside s qs j <;> simp [restrict, Bind.set, hj, hin]

/-- Filtering lists that agree once restricted. -/
theorem filter_restrict {R : Bind → Bind} {p : Bind → Bool} (hp : ∀ x, p (R x) = p x)
    {l l' : List Bind} (h : l.map R = l'.map R) : (l.filter p).map R = (l'.filter p).map R := by
  have key : ∀ l : List Bind, (l.filter p).map R = (l.map R).filter p := by
    intro l
    induction l with
    | nil => rfl
    | cons x xs ih =>
      simp only [List.filter_cons, List.map_cons, hp]
      split <;> simp [ih]
  rw [key, key, h]

/-- A flatMap over lists that agree once restricted, by a function that
gives restricted lists that agree for bindings that do. -/
theorem flatMap_restrict {R : Bind → Bind} {g g' : Bind → List Bind}
    (hg : ∀ x, (g x).map R = (g' (R x)).map R) (hg' : ∀ x, (g' x).map R = (g' (R x)).map R)
    {l l' : List Bind} (h : l.map R = l'.map R) :
    (l.flatMap g).map R = (l'.flatMap g').map R := by
  have k : ∀ (g : Bind → List Bind), (∀ x, (g x).map R = (g' (R x)).map R) →
      ∀ l : List Bind, (l.flatMap g).map R = (l.map R).flatMap fun x => (g' x).map R := by
    intro g hg l
    induction l with
    | nil => rfl
    | cons x xs ih => simp [List.flatMap_cons, List.map_append, ih, hg x]
  rw [k g hg, k g' hg', h]


/-- One item's step of `joinItems`. -/
def joinStep (s : Statement) (c : Core) (rowsOf : Nat → List Row) (subs : Bind → Subs) (i : Nat)
    (b : Bind) : List Bind :=
  let joined := (rowsOf i).filterMap fun r =>
    let b' := b.set i (some r)
    if (c.onOf i).all (fun cd => isTrue (cd.eval b' (subs b'))) then some b' else none
  if (s.item i).left && joined.isEmpty then [b.set i none] else joined

theorem joinItems_cons (s : Statement) (c : Core) (rowsOf : Nat → List Row) (subs : Bind → Subs)
    (i : Nat) (is : List Nat) (bs : List Bind) :
    joinItems s c rowsOf subs (i :: is) bs =
      joinItems s c rowsOf subs is (bs.flatMap (joinStep s c rowsOf subs i)) := rfl

theorem joinStep_restrict {s : Statement} {c : Core} {rows : Nat → List Row} {subs : Bind → Subs}
    {i : Nat} {R : Bind → Bind} (hset : ∀ x i r, R (x.set i r) = R ((R x).set i r))
    (hon : ∀ x, ((c.onOf i).all fun cd => isTrue (cd.eval (R x) (subs (R x)))) =
      ((c.onOf i).all fun cd => isTrue (cd.eval x (subs x)))) (x : Bind) :
    (joinStep s c rows subs i x).map R = (joinStep s c rows subs i (R x)).map R := by
  have hj : ((rows i).filterMap fun r =>
        if (c.onOf i).all (fun cd => isTrue (cd.eval (x.set i (some r)) (subs (x.set i (some r)))))
        then some (x.set i (some r)) else none).map R =
      ((rows i).filterMap fun r =>
        if (c.onOf i).all (fun cd =>
          isTrue (cd.eval ((R x).set i (some r)) (subs ((R x).set i (some r)))))
        then some ((R x).set i (some r)) else none).map R := by
    simp only [List.map_filterMap]
    congr 1
    funext r
    have hp : ((c.onOf i).all fun cd =>
          isTrue (cd.eval (x.set i (some r)) (subs (x.set i (some r))))) =
        ((c.onOf i).all fun cd =>
          isTrue (cd.eval ((R x).set i (some r)) (subs ((R x).set i (some r))))) := by
      rw [← hon (x.set i (some r)), ← hon ((R x).set i (some r)), hset x i (some r)]
    rw [hp]
    split <;> simp [hset x i (some r)]
  unfold joinStep
  simp only
  have hlen := congrArg List.length hj
  simp only [List.length_map] at hlen
  have he : ∀ {l l' : List Bind}, l.length = l'.length → l.isEmpty = l'.isEmpty := by
    intro l l' h
    cases l <;> cases l' <;> simp_all
  rw [he hlen]
  split
  · simp [hset x i none]
  · exact hj

theorem joinItems_restrict (s : Statement) (c : Core) {R : Bind → Bind}
    (hset : ∀ x i r, R (x.set i r) = R ((R x).set i r)) {rowsOf rowsOf' : Nat → List Row}
    {subs : Bind → Subs} :
    ∀ (is : List Nat), (∀ i ∈ is, rowsOf i = rowsOf' i) →
    (∀ i ∈ is, ∀ x, ((c.onOf i).all fun cd => isTrue (cd.eval (R x) (subs (R x)))) =
      ((c.onOf i).all fun cd => isTrue (cd.eval x (subs x)))) →
    ∀ (bs bs' : List Bind), bs.map R = bs'.map R →
    (joinItems s c rowsOf subs is bs).map R = (joinItems s c rowsOf' subs is bs').map R
  | [], _, _, bs, bs', h => by simpa [joinItems] using h
  | i :: is, hrows, hon, bs, bs', h => by
    rw [joinItems_cons, joinItems_cons]
    apply joinItems_restrict s c hset is (fun j hj => hrows j (by simp [hj]))
      (fun j hj => hon j (by simp [hj]))
    have hstep : joinStep s c rowsOf subs i = joinStep s c rowsOf' subs i := by
      funext x; simp [joinStep, hrows i (by simp)]
    rw [hstep]
    have hon' := hon i (by simp)
    exact flatMap_restrict (fun x => joinStep_restrict hset hon' x)
      (fun x => joinStep_restrict hset hon' x) h

theorem groupsOf_map {key : Bind → List Value} {R : Bind → Bind} (hkey : ∀ x, key (R x) = key x)
    (bs : List Bind) : groupsOf key (bs.map R) = (groupsOf key bs).map (·.map R) := by
  unfold groupsOf
  have hk : (bs.map R).map key = bs.map key := by
    simp only [List.map_map]; exact List.map_congr_left fun x _ => hkey x
  rw [hk, List.map_map]
  apply List.map_congr_left
  intro k _
  simp only [Function.comp]
  have key2 : ∀ l : List Bind, (l.map R).filter (fun b => key b == k) =
      (l.filter fun b => key b == k).map R := by
    intro l
    induction l with
    | nil => rfl
    | cons x xs ih =>
      simp only [List.map_cons, List.filter_cons, hkey]
      split <;> simp [ih]
  exact key2 bs

/-- The result columns' and GROUP BY's expressions of a core. -/
def outExprs (c : Core) : List Expr :=
  c.output.flatMap (fun
    | .expr e | .max e => [e]
    | .count => []) ++ c.groupBy

theorem rows_restrict {c : Core} {subs : Bind → Subs} {R : Bind → Bind}
    (hinv : ∀ e ∈ outExprs c, ∀ x, e.eval (R x) (subs (R x)) = e.eval x (subs x))
    (L : List Bind) : c.rows subs (L.map R) = c.rows subs L := by
  have hout : ∀ e, OutCol.expr e ∈ c.output → ∀ x, e.eval (R x) (subs (R x)) = e.eval x (subs x) :=
    fun e he => hinv e (by simp only [outExprs, List.mem_append, List.mem_flatMap]; exact Or.inl ⟨_, he, by simp⟩)
  have hmax : ∀ e, OutCol.max e ∈ c.output → ∀ x, e.eval (R x) (subs (R x)) = e.eval x (subs x) :=
    fun e he => hinv e (by simp only [outExprs, List.mem_append, List.mem_flatMap]; exact Or.inl ⟨_, he, by simp⟩)
  have hgroup : ∀ e ∈ c.groupBy, ∀ x, e.eval (R x) (subs (R x)) = e.eval x (subs x) :=
    fun e he => hinv e (by simp [outExprs, he])
  have hg : ∀ g : List Bind, (c.output.map fun
      | .expr e => match (g.map R).getLast? with
        | some b => e.eval b (subs b)
        | none => none
      | .count => some (Int.ofNat (g.map R).length)
      | .max e => maxValue ((g.map R).map fun b => e.eval b (subs b))) =
      (c.output.map fun
      | .expr e => match g.getLast? with
        | some b => e.eval b (subs b)
        | none => none
      | .count => some (Int.ofNat g.length)
      | .max e => maxValue (g.map fun b => e.eval b (subs b))) := by
    intro g
    apply List.map_congr_left
    intro o ho
    cases o with
    | expr e =>
      simp only [List.getLast?_map]
      cases g.getLast? with
      | none => rfl
      | some b => exact hout e ho b
    | count => simp
    | max e =>
      simp only [List.map_map]
      congr 1
      exact List.map_congr_left fun b _ => hmax e ho b
  unfold Core.rows
  split
  · split
    · exact congrArg (· :: []) (hg L)
    · rw [groupsOf_map (fun x => List.map_congr_left fun e he => hgroup e he x) L]
      simp only [List.map_map]
      exact List.map_congr_left fun g _ => hg g
  · simp only [List.map_map]
    apply List.map_congr_left
    intro b _
    apply List.map_congr_left
    intro o ho
    cases o with
    | expr e => exact hout e ho b
    | _ => rfl

/-- What `closed` says of each arm `k` of queries `qs`. -/
theorem closed_core {s : Statement} {qs : List Nat} (h : closed s qs = true) {k : Nat}
    (hk : k ∈ armsOf s qs) :
    (∀ i ∈ (s.core k).items, ∀ q v, (s.item i).kind = .derived q v → q ∈ qs) ∧
    (∀ q ∈ clauseQueries (s.core k), q ∈ qs) ∧
    (∀ p ∈ (s.core k).refs, inside s qs p.1 = true) := by
  unfold closed at h
  have hc := List.all_eq_true.mp h k hk
  simp only [Bool.and_eq_true, List.all_eq_true] at hc
  obtain ⟨⟨hitems, hqs⟩, hrefs⟩ := hc
  refine ⟨fun i hi q v hkind => ?_, fun q hq => by simpa using hqs q hq,
    fun p hp => hrefs p hp⟩
  have := hitems i hi
  rw [hkind] at this
  simpa using this

/-- Rule 5's theorem for a FROM subquery or CTE inside an expression
subquery: queries that name nothing outside them return, under a binding,
the rows they return under the binding's rows of their own items alone. -/
theorem closed_eval {s : Statement} {qs : List Nat} (h : closed s qs = true) (db : Db) :
    ∀ f q, q ∈ qs → ∀ b, evalQuery s db f b q = evalQuery s db f (restrict s qs b) q
  | 0, _, _, _ => rfl
  | f + 1, q, hq, b => by
    have ih := closed_eval h db f
    let R := restrict s qs
    let E := evalQuery s db f
    have hR : ∀ x, R (R x) = R x := restrict_restrict s qs
    have hset : ∀ x i r, R (x.set i r) = R ((R x).set i r) := restrict_set s qs
    simp only [evalQuery, queryRows]
    congr 2
    apply List.map_congr_left
    intro k hk
    have hk' : k ∈ armsOf s qs := List.mem_flatMap.mpr ⟨q, hq, hk⟩
    obtain ⟨hderived, hqueries, hrefs⟩ := closed_core h hk'
    let c := s.core k
    have hexpr : ∀ e : Expr, (∀ p ∈ e.refs, inside s qs p.1 = true) → (∀ q ∈ e.queries, q ∈ qs) →
        ∀ x, e.eval (R x) (E (R x)) = e.eval x (E x) := by
      intro e her heq x
      apply e.eval_congr
      · intro i col hic
        simp [R, restrict, her (i, col) hic]
      · intro q' hq'
        exact (ih q' (heq q' hq') x).symm
    have hcond : ∀ cd ∈ c.conds.map (·.1), ∀ x, cd.eval (R x) (E (R x)) = cd.eval x (E x) := by
      intro cd hcd x
      obtain ⟨⟨cd', p⟩, hmem, rfl⟩ := List.mem_map.mp hcd
      apply Cond.eval_congr
      · intro i col hic
        have : inside s qs i = true := hrefs (i, col) (by
          simp only [Core.refs, List.mem_append, List.mem_flatMap]
          exact Or.inl (Or.inl ⟨(cd', p), hmem, hic⟩))
        simp [R, restrict, this]
      · intro q' hq'
        exact (ih q' (hqueries q' (by
          simp only [clauseQueries, List.mem_append, List.mem_flatMap]
          exact Or.inl (Or.inr ⟨(cd', p), hmem, hq'⟩))) x).symm
    have hall : ∀ (cds : List Cond), (∀ cd ∈ cds, cd ∈ c.conds.map (·.1)) → ∀ x,
        (cds.all fun cd => isTrue (cd.eval (R x) (E (R x)))) =
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
    -- The bindings the core keeps agree once restricted.
    have hbind : (coreBindings s db E b k).map R = (coreBindings s db E (R b) k).map R := by
      unfold coreBindings
      apply filter_restrict (fun x => hall _ (fun cd hcd => hinner cd hcd) x)
      apply joinItems_restrict s c hset
      · intro i hi
        cases hkind : (s.item i).kind with
        | table t => simp
        | derived q' v =>
          cases hp : (s.core k).parent with
          | none => simp
          | some _ => simpa using ih q' (hderived i hi q' v hkind) b
      · intro i _ x
        exact hall _ (fun cd hcd => honOf i cd hcd) x
      · cases hp : (s.core k).parent with
        | none => simp
        | some _ => simp [hR b]
    have hrows : ∀ L : List Bind, c.rows E (L.map R) = c.rows E L := by
      apply rows_restrict
      intro e he x
      apply hexpr e
      · intro p hp
        apply hrefs p
        simp only [outExprs, List.mem_append, List.mem_flatMap] at he
        simp only [Core.refs, List.mem_append, List.mem_flatMap]
        rcases he with ⟨o, ho, he⟩ | he
        · cases o with
          | expr e' =>
            simp at he; subst he
            exact Or.inl (Or.inr ⟨_, ho, by simpa [OutCol.refs] using hp⟩)
          | max e' =>
            simp at he; subst he
            exact Or.inl (Or.inr ⟨_, ho, by simpa [OutCol.refs] using hp⟩)
          | count => simp at he
        · exact Or.inr ⟨e, he, hp⟩
      · intro q' hq'
        apply hqueries
        simp only [outExprs, List.mem_append, List.mem_flatMap] at he
        simp only [clauseQueries, List.mem_append, List.mem_flatMap]
        rcases he with ⟨o, ho, he⟩ | he
        · cases o with
          | expr e' =>
            simp at he; subst he
            exact Or.inl (Or.inl ⟨_, ho, hq'⟩)
          | max e' =>
            simp at he; subst he
            exact Or.inl (Or.inl ⟨_, ho, hq'⟩)
          | count => simp at he
        · exact Or.inr ⟨e, he, hq'⟩
    show c.rows E (coreBindings s db E b k) = c.rows E (coreBindings s db E (R b) k)
    rw [← hrows (coreBindings s db E b k), hbind, hrows]

/-- A query among queries that name nothing outside them returns the same
rows under every binding of none of their items, as the empty binding a
lookup reads it under. -/
theorem closed_sound {s : Statement} {qs : List Nat} (h : closed s qs = true) {q : Nat}
    (hq : q ∈ qs) (db : Db) (f : Nat) {b : Bind} (hb : ∀ i, inside s qs i = true → b i = none) :
    evalQuery s db f b q = evalQuery s db f Bind.empty q := by
  rw [closed_eval h db f q hq b, closed_eval h db f q hq Bind.empty]
  congr 1
  funext i
  unfold restrict
  split
  · rename_i hi; simp [hb i hi, Bind.empty]
  · rfl

end SqliveNarrowing.View
