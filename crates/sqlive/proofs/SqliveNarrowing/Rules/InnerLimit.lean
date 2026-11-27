import SqliveNarrowing.Rules.Limit
import SqliveNarrowing.Rules.PushDown

/-! Rule 6 inside a derived item: where a run's item is a derived item whose
one arm passes the columns that the run's first terms read on from one of its
own items, that item's rows take the run's bound over those terms, and inside
it again where it is derived too (`inner_runs`). A row of the inner item is in
a binding of the arm only if the arm's row holds its values in those columns,
and the bound of the derived item passes the arm's row (`inner_step`). -/
namespace SqliveNarrowing.Limit

/-! What `passedTo` says. -/

/-- The derived item `i` passes its column `c` on from column `d` of item `j`
of its query's one arm. -/
structure Passes (s : Statement) (st : PState) (i c j d : Nat) : Prop where
  shape : ∃ q v a, (s.item i).kind = .derived q v ∧ (s.query q).limit = none ∧
    (s.query q).offset = none ∧ (s.query q).arms = [a] ∧ (s.core a).windowed = false ∧
    (s.core a).output[c]? = some (.expr (.col j d)) ∧ j ∈ (s.core a).items ∧
    ((s.core a).aggregated = false ∨ (s.core a).groupBy.contains (.col j d) = true)
  inner : (st.items j).leftOn = none

theorem passedTo_passes {s : Statement} {st : PState} {i c j d : Nat}
    (h : passedTo s st i c = some (j, d)) : Passes s st i c j d := by
  unfold passedTo at h
  split at h
  · rename_i q v hkind
    split at h
    · cases h
    rename_i hcut
    simp only [Bool.or_eq_true, not_or, Bool.not_eq_true, Option.isSome_eq_false_iff,
      Option.isNone_iff_eq_none] at hcut
    split at h
    · rename_i a harms
      split at h
      · cases h
      rename_i hwin
      split at h
      · rename_i j' d' hout
        split at h
        · rename_i hok
          obtain ⟨rfl, rfl⟩ := Prod.mk.inj (Option.some.inj h)
          simp only [Bool.and_eq_true, Bool.or_eq_true, Bool.not_eq_true', List.contains_iff_mem,
            Option.isNone_iff_eq_none] at hok
          obtain ⟨⟨hj, hagg⟩, hleft⟩ := hok
          exact ⟨⟨q, v, a, hkind, hcut.1, hcut.2, harms, by simpa using hwin, hout, hj,
            hagg.imp id fun h => List.contains_iff_mem.mpr h.1⟩, hleft⟩
        · cases h
      · cases h
    · cases h
  · cases h

/-! A run's terms over an item inside. -/

/-- A term read over an item whose columns hold the row's values in the
columns `rename` gives has the term's direction and result column, and its
value. -/
theorem passTerm_sound {s : Statement} {i j : Nat} {rename : Nat → Option Nat}
    {term found : SortKey × Dir × Nat} (h : passTerm s i j rename term = some found) :
    found.2 = term.2 ∧ ∀ rp rc : Row, (∀ c d, rename c = some d → rp.cell c = rc.cell d) →
      found.1.value rc = term.1.value rp := by
  obtain ⟨key, d, p⟩ := term
  simp only [passTerm, Option.bind_eq_some_iff, Option.map_eq_some_iff] at h
  obtain ⟨v, hv, key', hkey, rfl⟩ := h
  refine ⟨rfl, fun rp rc hcells => ?_⟩
  have h1 : (valueExpr j v).eval (rowBind (some rc)) noSubs = key'.value rc :=
    termKey_value hkey rfl
  have h2 : (valueExpr j v).eval (rowBind (some rc)) noSubs = v.on (some rc) := by
    cases v <;> rfl
  have h3 : v.on (some rc) = key.pvalue.on (some rp) :=
    PushDown.value_passed_sound hv fun c c' hc => hcells c c' hc
  have h4 : key.pvalue.on (some rp) = key.value rp := by cases key <;> rfl
  rw [← h4, ← h3, ← h2, h1]

/-- The terms that pass are the run's first, each in its direction and with
its value. -/
theorem passTerms_sound {s : Statement} {i j : Nat} {rename : Nat → Option Nat}
    (terms : List (SortKey × Dir × Nat)) :
    (passTerms s i j rename terms).length ≤ terms.length ∧
      (passTerms s i j rename terms).map (·.2) =
        (terms.take (passTerms s i j rename terms).length).map (·.2) ∧
      ∀ rp rc : Row, (∀ c d, rename c = some d → rp.cell c = rc.cell d) →
        (passTerms s i j rename terms).map (·.1.value rc) =
          (terms.take (passTerms s i j rename terms).length).map (·.1.value rp) := by
  induction terms with
  | nil => simp [passTerms]
  | cons term rest ih =>
    simp only [passTerms]
    cases h : passTerm s i j rename term with
    | none => simp
    | some found =>
      obtain ⟨hdir, hval⟩ := passTerm_sound h
      obtain ⟨hlen, hdirs, hvals⟩ := ih
      refine ⟨by simpa using hlen, ?_, fun rp rc hcells => ?_⟩
      · simp only [List.map_cons, List.length_cons, List.take_succ_cons, hdir, hdirs]
      · simp only [List.map_cons, List.length_cons, List.take_succ_cons, hval rp rc hcells,
          hvals rp rc hcells]

/-- What each inner item is to the one around it, `i`, whose terms are
`terms`. -/
structure Link (s : Statement) (st : PState) (i : Nat) (terms : List (SortKey × Dir × Nat))
    (level : Level) : Prop where
  passes : ∀ c d, level.rename c = some d → Passes s st i c level.item d
  found : ∃ c d, Passes s st i c level.item d
  length : level.terms.length ≤ terms.length
  dirs : level.terms.map (·.2) = (terms.take level.terms.length).map (·.2)
  values : ∀ rp rc : Row, (∀ c d, level.rename c = some d → rp.cell c = rc.cell d) →
    level.terms.map (·.1.value rc) = (terms.take level.terms.length).map (·.1.value rp)

/-- Inner items, each linked to the one around it. -/
def Chain (s : Statement) (st : PState) : Nat → List (SortKey × Dir × Nat) → List Level → Prop
  | _, _, [] => True
  | i, terms, level :: rest => Link s st i terms level ∧ Chain s st level.item level.terms rest

theorem innerLevels_chain (s : Statement) (st : PState) :
    ∀ f i terms, Chain s st i terms (innerLevels s st f i terms)
  | 0, _, _ => by simp [innerLevels, Chain]
  | _ + 1, _, [] => by simp [innerLevels, Chain]
  | f + 1, i, (key, d, p) :: rest => by
    simp only [innerLevels]
    split
    · rename_i j dj hfirst
      split
      · simp [Chain]
      · rename_i term terms hpass
        refine ⟨?_, innerLevels_chain s st f j _⟩
        have hsound := passTerms_sound (s := s) (i := i) (j := j)
          (rename := passedColumn s st i j) ((key, d, p) :: rest)
        rw [hpass] at hsound
        refine ⟨fun c d' hc => ?_, ?_, hsound.1, hsound.2.1, hsound.2.2⟩
        · simp only [passedColumn, Option.bind_eq_some_iff] at hc
          obtain ⟨⟨j', d''⟩, hp, hc⟩ := hc
          split at hc
          · rename_i hj
            subst hj
            cases hc
            exact passedTo_passes hp
          · cases hc
        · obtain ⟨c, _, hc⟩ := Option.bind_eq_some_iff.mp hfirst
          exact ⟨c, dj, passedTo_passes hc⟩
    · simp [Chain]

/-- The item around the inner item after `pre`, and its terms. -/
def around (i : Nat) (terms : List (SortKey × Dir × Nat)) (pre : List Level) :
    Nat × List (SortKey × Dir × Nat) :=
  pre.foldl (fun _ level => (level.item, level.terms)) (i, terms)

theorem Chain.link {s : Statement} {st : PState} :
    ∀ {i : Nat} {terms : List (SortKey × Dir × Nat)} (pre : List Level) {level : Level}
      {post : List Level}, Chain s st i terms (pre ++ level :: post) →
      Link s st (around i terms pre).1 (around i terms pre).2 level
  | _, _, [], _, _, h => h.1
  | _, _, _ :: pre, _, _, h => Chain.link pre h.2

theorem around_append (i : Nat) (terms : List (SortKey × Dir × Nat)) (pre : List Level)
    (level : Level) : around i terms (pre ++ [level]) = (level.item, level.terms) := by
  simp [around]

/-! The runs `attach` gives. -/

theorem mem_attach {s : Statement} {st : PState} {run : SortRun} :
    ∀ {seen : List Nat} {l : List SortRun}, run ∈ attach s st seen l →
      ∃ r ∈ l, r.item = run.item ∧ r.terms = run.terms
  | _, [], h => by simp [attach] at h
  | _, r :: l, h => by
    simp only [attach, List.mem_cons] at h
    rcases h with rfl | h
    · exact ⟨r, by simp, rfl, rfl⟩
    · obtain ⟨r', hr', he⟩ := mem_attach h
      exact ⟨r', by simp [hr'], he⟩

theorem attach_has {s : Statement} {st : PState} {r : SortRun} :
    ∀ {seen : List Nat} {l : List SortRun}, r ∈ l →
      ∃ run ∈ attach s st seen l, run.item = r.item ∧ run.terms = r.terms
  | _, [], h => by cases h
  | seen, x :: l, h => by
    simp only [List.mem_cons] at h
    rcases h with rfl | h
    · exact ⟨_, List.mem_cons_self, rfl, rfl⟩
    · obtain ⟨run, hrun, he⟩ := attach_has (s := s) (st := st) (seen := x.item :: seen) h
      exact ⟨run, List.mem_cons_of_mem _ hrun, he⟩

/-- What reads only each run's item and terms reads the same of the runs
`attach` gives. -/
theorem attach_flatMap {β : Type} (s : Statement) (st : PState)
    (g : Nat → List (SortKey × Dir × Nat) → List β) :
    ∀ (seen : List Nat) (l : List SortRun),
      (attach s st seen l).flatMap (fun run => g run.item run.terms) =
        l.flatMap fun run => g run.item run.terms
  | _, [] => rfl
  | seen, x :: l => by
    simp only [attach, List.flatMap_cons, attach_flatMap s st g (x.item :: seen) l]

/-- A run with inner items is the first over its item, and its inner items
are those its terms pass down to. -/
theorem attach_split {s : Statement} {st : PState} {run : SortRun} {more : List SortRun} :
    ∀ {before : List SortRun} {seen : List Nat} {l : List SortRun},
      attach s st seen l = before ++ run :: more → run.inner ≠ [] →
        run.item ∉ seen ∧ (∀ b ∈ before, b.item ≠ run.item) ∧
          run.inner = innerLevels s st s.items.length run.item run.terms
  | [], seen, [], h, _ => by simp [attach] at h
  | _ :: _, _, [], h, _ => by simp [attach] at h
  | [], seen, x :: l, h, hne => by
    simp only [attach, List.nil_append, List.cons.injEq] at h
    obtain ⟨rfl, _⟩ := h
    simp only at hne ⊢
    split at hne
    · exact absurd rfl hne
    · rename_i hseen
      refine ⟨by simpa using hseen, by simp, ?_⟩
      simp only [hseen, Bool.false_eq_true, ↓reduceIte]
  | b :: before, seen, x :: l, h, hne => by
    simp only [attach, List.cons_append, List.cons.injEq] at h
    obtain ⟨rfl, h⟩ := h
    obtain ⟨hseen, hbefore, hinner⟩ := attach_split h hne
    simp only [List.mem_cons, not_or] at hseen
    refine ⟨hseen.2, fun b' hb' => ?_, hinner⟩
    simp only [List.mem_cons] at hb'
    rcases hb' with rfl | hb'
    · exact Ne.symm hseen.1
    · exact hbefore b' hb'

/-! Where `innerRuns` finds an item. -/

theorem mem_levelRuns {i root : Nat} {before : List (SortRun × List Value)} {last : List Value}
    {x : Nat × List Level × List (SortRun × List Value)} :
    ∀ {ls outer : List Level}, x ∈ levelRuns i root before last outer ls ↔
      ∃ pre level post, ls = pre ++ level :: post ∧ level.item = i ∧
        x = (root, outer ++ pre ++ [level],
          before ++ [({ item := i, terms := level.terms }, last)])
  | [], outer => by simp [levelRuns]
  | l :: ls, outer => by
    have ih := mem_levelRuns (i := i) (root := root) (before := before) (last := last) (x := x)
      (ls := ls) (outer := outer ++ [l])
    have split : (∃ pre level post, l :: ls = pre ++ level :: post ∧ level.item = i ∧
          x = (root, outer ++ pre ++ [level],
            before ++ [({ item := i, terms := level.terms }, last)])) ↔
        (l.item = i ∧ x = (root, outer ++ [l],
            before ++ [({ item := i, terms := l.terms }, last)])) ∨
          x ∈ levelRuns i root before last (outer ++ [l]) ls := by
      rw [ih]
      constructor
      · rintro ⟨pre, level, post, he, hi, hx⟩
        cases pre with
        | nil =>
          simp only [List.nil_append, List.cons.injEq] at he
          obtain ⟨rfl, _⟩ := he
          exact Or.inl ⟨hi, by simpa using hx⟩
        | cons a pre =>
          simp only [List.cons_append, List.cons.injEq] at he
          obtain ⟨rfl, he⟩ := he
          exact Or.inr ⟨pre, level, post, he, hi, by simpa using hx⟩
      · rintro (⟨hi, hx⟩ | ⟨pre, level, post, he, hi, hx⟩)
        · exact ⟨[], l, ls, rfl, hi, by simpa using hx⟩
        · exact ⟨l :: pre, level, post, by simp [he], hi, by simpa using hx⟩
    rw [split]
    simp only [levelRuns]
    split
    · rename_i hi
      simp only [List.mem_cons, hi, true_and]
    · rename_i hi
      simp only [hi, false_and, false_or]

theorem mem_innerRuns {i : Nat} {x : Nat × List Level × List (SortRun × List Value)} :
    ∀ {runs before : List (SortRun × List Value)}, x ∈ innerRuns i before runs ↔
      ∃ pre run last post, runs = pre ++ (run, last) :: post ∧
        x ∈ levelRuns i run.item (before ++ pre) last [] run.inner
  | [], before => by simp [innerRuns]
  | (r, v) :: runs, before => by
    simp only [innerRuns, List.mem_append,
      mem_innerRuns (runs := runs) (before := before ++ [(r, v)])]
    constructor
    · rintro (h | ⟨pre, run, last, post, rfl, h⟩)
      · exact ⟨[], r, v, runs, rfl, by simpa using h⟩
      · exact ⟨(r, v) :: pre, run, last, post, rfl, by simpa using h⟩
    · rintro ⟨pre, run, last, post, he, h⟩
      cases pre with
      | nil =>
        simp only [List.nil_append, List.cons.injEq, Prod.mk.injEq] at he
        obtain ⟨⟨rfl, rfl⟩, _⟩ := he
        exact Or.inl (by simpa using h)
      | cons a pre =>
        simp only [List.cons_append, List.cons.injEq] at he
        obtain ⟨rfl, he⟩ := he
        exact Or.inr ⟨pre, run, last, post, he, by simpa using h⟩

/-- A scope that joins each of a list's with AND passes a row each passes. -/
theorem foldl_both_passes {α : Type} (rowsOf : Nat → List Row) (r : Row) (g : α → RScope) :
    ∀ (l : List α) (scope : RScope),
      (l.foldl (fun scope x => scope.both (g x)) scope).passes rowsOf r =
        (scope.passes rowsOf r && l.all fun x => (g x).passes rowsOf r)
  | [], scope => by simp
  | x :: l, scope => by
    simp only [List.foldl_cons, List.all_cons, foldl_both_passes rowsOf r g l, passes_both,
      Bool.and_assoc]

/-- A bound reads the runs only up to the first over its item. -/
theorem runBound_first {i : Nat} {lookup : Nat → Option (RScope → RScope)} {run : SortRun}
    {last : List Value} {post : List (SortRun × List Value)} (hi : run.item = i) :
    ∀ {pre : List (SortRun × List Value)}, (∀ p ∈ pre, p.1.item ≠ i) →
      runBound i lookup (pre ++ (run, last) :: post) =
        runBound i lookup (pre ++ [({ item := i, terms := run.terms }, last)])
  | [], _ => by simp [runBound, hi, SortRun.keys]
  | (r, v) :: pre, h => by
    have hr : r.item ≠ i := h (r, v) (by simp)
    simp only [List.cons_append, runBound, hr, ↓reduceIte,
      runBound_first (post := post) hi (pre := pre) fun p hp => h p (by simp [hp])]

/-! The bound inside. -/

theorem keyLe_take {ds : List Dir} {as vs : List Value} (n : Nat) (ha : as.length = ds.length)
    (hv : vs.length = ds.length) (h : keyLe ds as vs) :
    keyLe (ds.take n) (as.take n) (vs.take n) := by
  unfold keyLe at h ⊢
  rw [bne_iff_ne] at h ⊢
  rw [← List.take_append_drop n ds, ← List.take_append_drop n as, ← List.take_append_drop n vs,
    keyCompare_append _ _ _ _ _ _ (by simp [ha]) (by simp [hv]), then_ne_gt] at h
  rcases h with h | ⟨h, _⟩ <;> rw [h] <;> simp

theorem zip_take {α β : Type} :
    ∀ (a : List α) (b : List β), (a.take b.length).zip (b.take a.length) = a.zip b
  | [], _ => by simp
  | _ :: _, [] => by simp
  | x :: a, y :: b => by
    simp only [List.length_cons, List.take_succ_cons, List.zip_cons_cons, zip_take a b]

/-- A bound reads as many terms and values as both lists hold. -/
theorem atOrBefore_take (i : Nat) (terms : List (SortKey × Dir)) (last : List Value) :
    atOrBefore i terms last =
      atOrBefore i (terms.take (min terms.length last.length))
        (last.take (min terms.length last.length)) := by
  have h1 : terms.take (min terms.length last.length) = terms.take last.length := by
    rw [Nat.min_comm, ← List.take_take, List.take_length]
  have h2 : last.take (min terms.length last.length) = last.take terms.length := by
    rw [← List.take_take, List.take_length]
  unfold atOrBefore sortedBefore
  rw [h1, h2, zip_take]

/-- A row whose keys' values are those of the first keys of another row's
passes the bound over those keys when the other row passes the bound over all
of its. -/
theorem atOrBefore_prefix (rowsOf : Nat → List Row) (rp rc : Row) (i j : Nat)
    (kp kc : List (SortKey × Dir)) (last : List Value) (hlen : kc.length ≤ kp.length)
    (hdirs : kc.map (·.2) = (kp.take kc.length).map (·.2))
    (hvals : kc.map (·.1.value rc) = (kp.take kc.length).map (·.1.value rp))
    (h : (atOrBefore i kp last).passes rowsOf rp) :
    (atOrBefore j kc last).passes rowsOf rc := by
  have hm : min kc.length last.length ≤ min kp.length last.length := by omega
  rw [atOrBefore_take, atOrBefore_passes _ _ _ _ _ (by simp <;> omega)] at h
  rw [atOrBefore_take, atOrBefore_passes _ _ _ _ _ (by simp <;> omega)]
  have htake := keyLe_take (min kc.length last.length) (by simp)
    (by simp only [List.length_take, List.length_map]; omega) h
  have cut : ∀ {α : Type} (f : SortKey × Dir → α) (g : SortKey × Dir → α),
      kc.map f = (kp.take kc.length).map g →
      (kc.take (min kc.length last.length)).map f =
        ((kp.take (min kp.length last.length)).map g).take (min kc.length last.length) := by
    intro α f g he
    rw [List.map_take, he, ← List.map_take, ← List.map_take, List.take_take, List.take_take,
      Nat.min_eq_left hm, Nat.min_eq_left (Nat.min_le_left _ _)]
  have hlast : last.take (min kc.length last.length) =
      (last.take (min kp.length last.length)).take (min kc.length last.length) := by
    rw [List.take_take, Nat.min_eq_left hm]
  rw [cut _ _ hdirs, cut _ _ hvals, hlast]
  exact htake

/-- The bound of an item inside a derived item is one where the derived
item's is, and passes the inner item's row where the derived item's passes
its row: the inner item's terms are the derived item's first, with its
values, and each of its lookups finds only where the derived item's does. -/
theorem inner_step (rowsOf : Nat → List Row) (rp rc : Row) (p c : Nat)
    (lookP lookC : Nat → Option (RScope → RScope)) (tp tc : List (SortKey × Dir × Nat))
    (last : List Value) (hlen : tc.length ≤ tp.length)
    (hdirs : tc.map (·.2) = (tp.take tc.length).map (·.2))
    (hvals : tc.map (·.1.value rc) = (tp.take tc.length).map (·.1.value rp)) :
    ∀ (before : List (SortRun × List Value)),
      (∀ x ∈ before, x.1.item ≠ p ∧ x.1.item ≠ c) →
      (∀ j lc, lookC j = some lc → ∃ lp, lookP j = some lp ∧
        ∀ scope, (lp scope).passes rowsOf rp → (lc scope).passes rowsOf rc) →
      ∀ bc, runBound c lookC (before ++ [({ item := c, terms := tc }, last)]) = some bc →
        ∃ bp, runBound p lookP (before ++ [({ item := p, terms := tp }, last)]) = some bp ∧
          (bp.passes rowsOf rp → bc.passes rowsOf rc)
  | [], _, _, bc, h => by
    simp only [List.nil_append, runBound, ↓reduceIte, Option.some.injEq] at h
    subst h
    refine ⟨_, by simp only [List.nil_append, runBound, ↓reduceIte]; rfl, fun hp => ?_⟩
    refine atOrBefore_prefix rowsOf rp rc p c _ _ last (by simpa [SortRun.keys] using hlen) ?_ ?_ hp
    · have := congrArg (List.map Prod.fst) hdirs
      simpa [SortRun.keys, List.map_take, Function.comp_def] using this
    · simpa [SortRun.keys, List.map_take, Function.comp_def] using hvals
  | (run, v) :: before, hitems, hlook, bc, h => by
    have hne := hitems (run, v) (by simp)
    simp only [List.cons_append, runBound, hne.2, ↓reduceIte] at h
    cases hlater : runBound c lookC (before ++ [({ item := c, terms := tc }, last)]) with
    | none => simp [hlater] at h
    | some later =>
      cases hlc : lookC run.item with
      | none => simp [hlater, hlc] at h
      | some lc =>
        obtain ⟨lp, hlp, hpass⟩ := hlook _ _ hlc
        obtain ⟨laterP, hlaterP, hlaterPass⟩ := inner_step rowsOf rp rc p c lookP lookC tp tc last
          hlen hdirs hvals before (fun x hx => hitems x (by simp [hx])) hlook later hlater
        simp only [hlater, hlc, Option.bind_eq_bind, Option.bind_some, Option.pure_def,
          Option.some.injEq] at h
        subst h
        simp only [List.cons_append, runBound, hne.1, ↓reduceIte, hlaterP, hlp,
          Option.bind_eq_bind, Option.bind_some, Option.pure_def]
        refine ⟨_, rfl, fun hp => ?_⟩
        cases hs : sortedBefore run.item run.keys v none with
        | none =>
          rw [hs] at hp
          exact hlaterPass hp
        | some found =>
          rw [hs] at hp
          simp only [passes_either, Bool.or_eq_true] at hp ⊢
          exact hp.imp (hpass found) hlaterPass

end SqliveNarrowing.Limit
