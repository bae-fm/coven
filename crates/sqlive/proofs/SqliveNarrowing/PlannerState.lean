import SqliveNarrowing.Planner

/-! What the planner's state keeps however it reads the conditions: it holds
an ON narrowing back only for an item a LEFT JOIN adds, and once it releases
one, as it makes the item an inner join's, it never holds one back again. So
an item the planned state still holds an ON narrowing back for is one a LEFT
JOIN adds (`plan_leftOn`), which the restriction's `hLeft` takes. Each edge
the planner passes into a derived item's arms comes from an edge the derived
item holds, through the result column the arm passes the target's column on
to, and makes its target an inner join's (`plan_pushed`): so a row of the
derived item that passes the derived item's edge makes the target's row in
the arm's binding pass the pushed edge. Each step keeps the memberships and
pushed edges it found (`PState.Keeps`). The planner places each item once,
and every item of every core (`plan_order`), which `Planned.scopes_eq`
takes. -/
namespace SqliveNarrowing

/-- Where a pushed edge comes from, in the state `st`: its member is an
inner join's, and an arm of a derived item's query, which keeps every row
and makes a row of each binding, passes the member's column on unchanged as
a result column, of which the derived item holds an edge, as a membership or
as an edge pushed into it, with the same partner, partner value and NULL
matching. -/
def PState.PushedFrom (s : Statement) (st : PState) (e : Edge) : Prop :=
  (st.items e.member).leftOn = none ∧
  ∃ d q v k c c', (s.item d).kind = .derived q v ∧ (s.query q).limit = none ∧
    (s.query q).offset = none ∧ k ∈ (s.query q).arms ∧ (s.core k).aggregated = false ∧
    (s.core k).windowed = false ∧ (s.item e.member).core = k ∧
    (s.core k).output[c]? = some (.expr (.col e.member c')) ∧ e.memberValue = .column c' ∧
    ∃ e0, (e0 ∈ (st.items d).memberships ∨ (e0 ∈ st.pushed ∧ e0.member = d)) ∧
      e0.memberValue = .column c ∧ e0.partner = e.partner ∧ e0.value = e.value ∧ e0.nulls = e.nulls

/-- `st'` holds an ON narrowing back only for items `st` holds one back for,
and keeps each membership and pushed edge `st` holds. -/
def PState.Grows (st st' : PState) : Prop :=
  (∀ i, (st'.items i).leftOn.isSome → (st.items i).leftOn.isSome) ∧
  (∀ i, ∀ m ∈ (st.items i).memberships, m ∈ (st'.items i).memberships) ∧
  (∀ e ∈ st.pushed, e ∈ st'.pushed)

theorem PState.Grows.trans {a b c : PState} (hab : a.Grows b) (hbc : b.Grows c) : a.Grows c :=
  ⟨fun i h => hab.1 i (hbc.1 i h), fun i m h => hbc.2.1 i m (hab.2.1 i m h),
    fun e h => hbc.2.2 e (hab.2.2 e h)⟩

/-- `st'` grows from `st`, and each edge it pushed since comes from where
`PushedFrom` says. -/
def PState.Keeps (s : Statement) (st st' : PState) : Prop :=
  st.Grows st' ∧ ∀ e ∈ st'.pushed, e ∈ st.pushed ∨ PState.PushedFrom s st' e

/-- A state that grows from `st` keeps where its pushed edges come from. -/
theorem PState.PushedFrom.mono {s : Statement} {st st' : PState} (h : st.Grows st')
    {e : Edge} (he : PState.PushedFrom s st e) : PState.PushedFrom s st' e := by
  obtain ⟨hleft, d, q, v, k, c, c', hd, hl, ho, hk, hagg, hwin, hcore, hout, hmv, e0, he0, hrest⟩ := he
  refine ⟨?_, d, q, v, k, c, c', hd, hl, ho, hk, hagg, hwin, hcore, hout, hmv, e0, ?_, hrest⟩
  · cases h' : (st'.items e.member).leftOn with
    | none => rfl
    | some _ =>
      have := h.1 e.member (by simp [h'])
      rw [hleft] at this
      cases this
  · rcases he0 with he0 | ⟨he0, hm⟩
    · exact Or.inl (h.2.1 d e0 he0)
    · exact Or.inr ⟨h.2.2 e0 he0, hm⟩

theorem PState.Keeps.refl (s : Statement) (st : PState) : PState.Keeps s st st :=
  ⟨⟨fun _ h => h, fun _ _ h => h, fun _ h => h⟩, fun _ h => Or.inl h⟩

theorem PState.Keeps.trans {s : Statement} {a b c : PState} (hab : PState.Keeps s a b)
    (hbc : PState.Keeps s b c) : PState.Keeps s a c := by
  refine ⟨hab.1.trans hbc.1, fun e h => ?_⟩
  rcases hbc.2 e h with h | h
  · rcases hab.2 e h with h | h
    · exact Or.inl h
    · exact Or.inr (h.mono hbc.1)
  · exact Or.inr h

/-- A fold each of whose steps keeps, from a state that keeps what `base`
holds, keeps it. -/
theorem PState.Keeps.foldl_from {s : Statement} {α : Type} {g : PState → α → PState} {base : PState}
    {l : List α} (hg : ∀ st x, x ∈ l → PState.Keeps s base st → PState.Keeps s st (g st x)) :
    ∀ st, PState.Keeps s base st → PState.Keeps s base (l.foldl g st) := by
  induction l with
  | nil => exact fun st h => h
  | cons x l ih =>
    intro st h
    exact ih (fun st' y hy hb => hg st' y (List.mem_cons_of_mem x hy) hb) _
      (h.trans (hg st x List.mem_cons_self h))

/-- A fold whose every step keeps keeps. -/
theorem PState.Keeps.foldl {s : Statement} {α : Type} {g : PState → α → PState}
    (hg : ∀ (st : PState) (x : α), PState.Keeps s st (g st x)) :
    ∀ (l : List α) (st : PState), PState.Keeps s st (l.foldl g st)
  | [], st => PState.Keeps.refl s st
  | x :: l, st => (hg st x).trans (PState.Keeps.foldl hg l (g st x))

/-- An update that keeps or drops an item's held narrowing and keeps its
memberships keeps. -/
theorem PState.update_keeps {s : Statement} {st : PState} {i : Nat} {f : PItem → PItem}
    (hf : (f (st.items i)).leftOn.isSome → (st.items i).leftOn.isSome)
    (hm : ∀ m ∈ (st.items i).memberships, m ∈ (f (st.items i)).memberships) :
    PState.Keeps s st (st.update i f) := by
  refine ⟨⟨fun j h => ?_, fun j m h => ?_, fun _ h => h⟩, fun _ h => Or.inl h⟩
  · simp only [PState.update] at h
    split at h
    · rename_i hji
      subst hji
      exact hf h
    · exact h
  · simp only [PState.update]
    split
    · rename_i hji
      subst hji
      exact hm m h
    · exact h

/-- A state with other edges keeps. -/
theorem PState.edges_keeps (s : Statement) (st : PState) (edges : List Edge) :
    PState.Keeps s st { st with edges } :=
  ⟨⟨fun _ h => h, fun _ _ h => h, fun _ h => h⟩, fun _ h => Or.inl h⟩

theorem PState.addTerm_keeps (s : Statement) (st : PState) (i : Nat) (t : STerm) :
    PState.Keeps s st (st.addTerm i t) := by
  unfold PState.addTerm
  exact PState.update_keeps (fun h => h) (fun _ h => h)

theorem PState.add_keeps (s : Statement) (st : PState) (n : Narrowing) : PState.Keeps s st (st.add n).1 := by
  simp only [PState.add]
  have hors := PState.Keeps.foldl (s := s) (g := fun st (d : Disjunction) =>
    st.update d.item fun p => { p with ors := p.ors ++ [d.sides] })
    (fun st d => PState.update_keeps (fun h => h) (fun _ h => h)) n.ors
    (n.terms.foldl (fun st (i, t) => st.addTerm i t) st)
  have hterms := PState.Keeps.foldl (s := s) (g := fun st (x : Nat × STerm) => st.addTerm x.1 x.2)
    (fun st x => PState.addTerm_keeps s st x.1 x.2) n.terms st
  exact (hterms.trans hors).trans (PState.edges_keeps s _ _)

theorem PState.narrow_keeps (s : Statement) : ∀ (fuel : Nat) (st : PState) (n : Narrowing),
    PState.Keeps s st (PState.narrow fuel st n)
  | 0, st, _ => PState.Keeps.refl s st
  | fuel + 1, st, n => by
    simp only [PState.narrow]
    refine (PState.add_keeps s st _).trans (PState.Keeps.foldl (fun st' i => ?_) _ _)
    split
    · exact (PState.update_keeps (by simp) (fun _ h => h)).trans (PState.narrow_keeps s fuel _ _)
    · exact PState.Keeps.refl s _

theorem PState.joinInner_keeps (s : Statement) (fuel : Nat) (st : PState) (i : Nat) :
    PState.Keeps s st (st.joinInner fuel i) := by
  unfold PState.joinInner
  split
  · exact (PState.update_keeps (by simp) (fun _ h => h)).trans (PState.narrow_keeps s fuel _ _)
  · exact PState.Keeps.refl s _

/-- Making an item an inner join's leaves it holding no ON narrowing back. -/
theorem PState.joinInner_leftOn (fuel : Nat) (st : PState) (i : Nat) :
    ((st.joinInner fuel i).items i).leftOn = none := by
  unfold PState.joinInner
  cases hi : (st.items i).leftOn with
  | none => simp [hi]
  | some held =>
    simp only
    cases h : ((PState.narrow fuel (st.update i fun p => { p with leftOn := none }) held).items i).leftOn with
    | none => rfl
    | some _ =>
      have := (PState.narrow_keeps default fuel (st.update i fun p => { p with leftOn := none })
        held).1.1 i (by simp [h])
      simp [PState.update] at this

theorem PState.narrowOn_keeps (s : Statement) (fuel : Nat) (st : PState) (i : Nat) (n : Narrowing) :
    PState.Keeps s st (st.narrowOn fuel i n) := by
  unfold PState.narrowOn
  generalize n.split i = x
  obtain ⟨own, others⟩ := x
  simp only
  refine (PState.add_keeps s st own).trans ?_
  split
  · rename_i held hheld
    exact PState.update_keeps (by simp [hheld]) (fun _ h => h)
  · exact PState.narrow_keeps s fuel _ _

theorem Transfer.carry_keeps (s : Statement) (st : PState) (k : Nat) :
    PState.Keeps s st (Transfer.carry s st k) :=
  PState.Keeps.foldl (fun st (x : Nat × STerm) => by
    unfold Transfer.addCarried
    exact PState.update_keeps (fun h => h) (fun _ h => h)) _ st

/-- The edges `pushDown` passes into `item`'s arms: memberships of `item`, or
edges pushed into it. -/
def PState.Passes (st : PState) (item : Nat) (edges : List Edge) : Prop :=
  ∀ e ∈ edges, e ∈ (st.items item).memberships ∨ (e ∈ st.pushed ∧ e.member = item)

theorem PState.Passes.mono {st st' : PState} (h : st.Grows st')
    {item : Nat} {edges : List Edge} (hp : st.Passes item edges) : st'.Passes item edges := by
  intro e he
  rcases hp e he with hm | ⟨hm, hi⟩
  · exact Or.inl (h.2.1 item e hm)
  · exact Or.inr ⟨h.2.2 e hm, hi⟩

/-- An arm `pushDown` passes conditions into: an arm of a derived item's
query with no LIMIT and no OFFSET that neither aggregates nor calls a window
function, passing on each result column that is a bare column of its own
item. -/
theorem PushDown.mem_arms {s : Statement} {item : Nat} {found : List (List (Option (Nat × Nat)))}
    (h : PushDown.arms s item = some found) {arm : List (Option (Nat × Nat))} (harm : arm ∈ found) :
    ∃ q v k, (s.item item).kind = .derived q v ∧ (s.query q).limit = none ∧
      (s.query q).offset = none ∧ k ∈ (s.query q).arms ∧ (s.core k).aggregated = false ∧
      (s.core k).windowed = false ∧ ∀ (c t c' : Nat), (arm[c]?).join = some (t, c') →
        (s.item t).core = k ∧ (s.core k).output[c]? = some (OutCol.expr (Expr.col t c')) := by
  unfold PushDown.arms at h
  dsimp only at h
  split at h
  · cases h
  · rename_i q v hkind
    split at h
    · cases h
      cases harm
    · rename_i hcut
      cases h
      obtain ⟨k, hk, rfl⟩ := List.mem_map.mp harm
      simp only [List.mem_filter, Bool.and_eq_true, Bool.not_eq_true'] at hk
      simp only [Bool.or_eq_true, Option.isSome_iff_ne_none, ne_eq, not_or, Decidable.not_not] at hcut
      refine ⟨q, v, k, hkind, hcut.1, hcut.2, hk.1, hk.2.1, hk.2.2, fun c t c' hc => ?_⟩
      simp only [List.getElem?_map] at hc
      cases ho : (s.core k).output[c]? with
      | none => simp [ho] at hc
      | some o =>
        simp only [ho, Option.map_some, Option.join_some] at hc
        split at hc
        · rename_i j c'' 
          split at hc
          · rename_i hj
            cases hc
            exact ⟨hj, rfl⟩
          · cases hc
        · cases hc

/-- An edge `pushDown` passes to a target comes from one of the edges it
was given, through the column the arm passes on to the target. -/
theorem PushDown.mem_passed {edges : List Edge} {keep : Edge → Bool}
    {through : Nat → Option (Nat × Nat)} {target : Nat} {e : Edge}
    (he : e ∈ (edges.filter keep).filterMap fun e =>
      match e.memberValue with
      | .column c => (match through c with
          | some (t, c') => if t = target then some c' else none
          | none => none).map fun c' => { e with member := target, memberValue := .column c' }
      | .expr _ => none) :
    ∃ e0 ∈ edges, ∃ c c', e0.memberValue = .column c ∧ through c = some (target, c') ∧
      e = { e0 with member := target, memberValue := .column c' } := by
  obtain ⟨e0, he0, hmap⟩ := List.mem_filterMap.mp he
  refine ⟨e0, (List.mem_filter.mp he0).1, ?_⟩
  split at hmap
  · rename_i c hc
    obtain ⟨c', hc', rfl⟩ := Option.map_eq_some_iff.mp hmap
    refine ⟨c, c', hc, ?_, rfl⟩
    split at hc'
    · rename_i t c'' ht
      split at hc'
      · rename_i htt
        subst htt
        cases hc'
        exact ht
      · cases hc'
    · cases hc'
  · cases hmap

/-- One target's step of `pushDown` keeps, when each edge it passes comes
from where `PushedFrom` says once its target is an inner join's. -/
theorem PushDown.step_keeps {s : Statement} {narrowFuel fuel : Nat} {st1 : PState} {target : Nat}
    {renamed : Option STerm} {passed : List Edge}
    (hpassed : ∀ e ∈ passed, e.member = target ∧
      ∀ st', st1.Grows st' → (st'.items target).leftOn = none → PState.PushedFrom s st' e)
    (ih : ∀ st', st'.Passes target passed →
      PState.Keeps s st' (PushDown.pushDown s narrowFuel fuel st' target renamed passed)) :
    PState.Keeps s st1 (PushDown.pushDown s narrowFuel fuel
      (if renamed.isSome || !passed.isEmpty then
        PState.joinInner narrowFuel
          { (match renamed with
              | some r => st1.addTerm target r
              | none => st1) with
            edges := (match renamed with
              | some r => st1.addTerm target r
              | none => st1).edges ++ passed,
            pushed := (match renamed with
              | some r => st1.addTerm target r
              | none => st1).pushed ++ passed } target
      else
        { (match renamed with
            | some r => st1.addTerm target r
            | none => st1) with
          edges := (match renamed with
            | some r => st1.addTerm target r
            | none => st1).edges ++ passed,
          pushed := (match renamed with
            | some r => st1.addTerm target r
            | none => st1).pushed ++ passed })
      target renamed passed) := by
  generalize hst2 : (match renamed with
    | some r => st1.addTerm target r
    | none => st1) = st2
  have h12 : PState.Keeps s st1 st2 := by
    subst hst2
    cases renamed
    · exact PState.Keeps.refl s st1
    · exact PState.addTerm_keeps s st1 target _
  generalize hst3 : ({ st2 with edges := st2.edges ++ passed, pushed := st2.pushed ++ passed } :
    PState) = st3
  have h23 : st2.Grows st3 := by
    subst hst3
    exact ⟨fun _ h => h, fun _ _ h => h, fun _ h => List.mem_append_left _ h⟩
  have hpushed3 : ∀ e ∈ st3.pushed, e ∈ st2.pushed ∨ e ∈ passed := by
    subst hst3
    intro e he
    exact List.mem_append.mp he
  generalize hst4 : (if renamed.isSome || !passed.isEmpty then st3.joinInner narrowFuel target else st3) = st4
  have h34 : PState.Keeps s st3 st4 := by
    subst hst4
    split
    · exact PState.joinInner_keeps s narrowFuel st3 target
    · exact PState.Keeps.refl s st3
  have hgrow : st1.Grows st4 := (h12.1.trans h23).trans h34.1
  have h14 : PState.Keeps s st1 st4 := by
    refine ⟨hgrow, fun e he => ?_⟩
    rcases h34.2 e he with he3 | hpf
    · rcases hpushed3 e he3 with he2 | hpass
      · rcases h12.2 e he2 with he1 | hpf
        · exact Or.inl he1
        · exact Or.inr (hpf.mono (h23.trans h34.1))
      · have hne : passed.isEmpty = false := by
          cases passed with
          | nil => cases hpass
          | cons _ _ => rfl
        have hjoin : st4 = st3.joinInner narrowFuel target := by
          rw [← hst4, hne]
          simp
        obtain ⟨hmem, hfrom⟩ := hpassed e hpass
        refine Or.inr (hfrom st4 hgrow ?_)
        rw [hjoin]
        exact PState.joinInner_leftOn narrowFuel st3 target
    · exact Or.inr hpf
  refine h14.trans (ih st4 fun e he => Or.inr ⟨h34.1.2.2 e ?_, (hpassed e he).1⟩)
  rw [← hst3]
  exact List.mem_append_right _ he

theorem PushDown.pushDown_keeps (s : Statement) (narrowFuel : Nat) :
    ∀ (fuel : Nat) (st : PState) (item : Nat) (term : Option STerm) (edges : List Edge),
      st.Passes item edges → PState.Keeps s st (PushDown.pushDown s narrowFuel fuel st item term edges)
  | 0, st, _, _, _, _ => PState.Keeps.refl s st
  | fuel + 1, st, item, term, edges, hpass => by
    simp only [PushDown.pushDown]
    split
    · exact PState.Keeps.refl s st
    · rename_i found hfound
      split
      · exact PState.Keeps.refl s st
      · refine PState.Keeps.foldl_from (base := st) (fun st0 arm harm hst0 => ?_) st
          (PState.Keeps.refl s st)
        obtain ⟨q, v, k, hkind, hlim, hoff, hk, hagg, hwin, hthrough⟩ := PushDown.mem_arms hfound harm
        have hpass0 := hpass.mono hst0.1
        refine PState.Keeps.foldl_from (base := st0) (fun st1 target _ hst1 => ?_) st0
          (PState.Keeps.refl s st0)
        have hpass1 := hpass0.mono hst1.1
        refine PushDown.step_keeps (fun e he => ?_)
          (fun st' h => PushDown.pushDown_keeps s narrowFuel fuel st' target _ _ h)
        obtain ⟨e0, he0, c, c', hc, hthr, rfl⟩ := PushDown.mem_passed (through := fun c => arm[c]?.join) he
        obtain ⟨hcore, hout⟩ := hthrough c target c' hthr
        refine ⟨rfl, fun st' hgrow hleft => ⟨hleft, item, q, v, k, c, c', hkind, hlim, hoff, hk, hagg,
          hwin, hcore, hout, rfl, e0, ?_, hc, rfl, rfl, rfl⟩⟩
        rcases hpass1 e0 he0 with hm | ⟨hm, hi⟩
        · exact Or.inl (hgrow.2.1 item e0 hm)
        · exact Or.inr ⟨hgrow.2.2 e0 hm, hi⟩
theorem readCore_keeps (s : Statement) (st : PState) (k : Nat) :
    PState.Keeps s st (readCore s st k) := by
  unfold readCore
  simp only
  refine PState.Keeps.trans ?_ (PState.Keeps.foldl
    (fun st i => PushDown.pushDown_keeps s _ _ st i _ _ (fun _ h => by cases h)) _ _)
  refine PState.Keeps.trans ?_ (Transfer.carry_keeps s _ k)
  refine PState.Keeps.foldl (fun st x => ?_) _ st
  obtain ⟨n, p⟩ := x
  cases p
  · exact PState.narrow_keeps s _ _ _
  · exact PState.narrowOn_keeps s _ _ _ _

theorem ordered_keeps (s : Statement) :
    ∀ (fuel : Nat) (all : List Nat) (st : PState) (order waited : List Nat),
      PState.Keeps s st (ordered.go s fuel all st order waited).1
  | 0, _, st, _, _ => PState.Keeps.refl s st
  | fuel + 1, all, st, order, waited => by
    simp only [ordered.go]
    split
    · exact PState.Keeps.refl s st
    · refine PState.Keeps.trans ?_ (ordered_keeps s fuel _ _ _ _)
      refine PState.Keeps.trans (PState.update_keeps (fun h => h)
        (fun _ h => List.mem_append_left _ h)) (PushDown.pushDown_keeps s _ _ _ _ _ _ ?_)
      intro e he
      left
      simp only [PState.update, ite_true]
      exact List.mem_append_right _ he

/-- The planner's state from the initial one to the planned one keeps. -/
theorem plan_keeps (s : Statement) : PState.Keeps s (PState.initial s) (plan s).state := by
  unfold plan ordered
  simp only
  exact (PState.Keeps.foldl (fun st k => readCore_keeps s st k) s.readOrder (PState.initial s)).trans
    (ordered_keeps s _ _ _ [] [])

/-- The planned state holds an ON narrowing back only for an item a LEFT JOIN
adds. -/
theorem plan_leftOn (s : Statement) {i : Nat} (h : ((plan s).state.items i).leftOn.isSome) :
    s.alwaysBound i = false := by
  have h0 := (plan_keeps s).1.1 i h
  simp only [PState.initial] at h0
  simp only [Statement.alwaysBound]
  revert h0
  cases (s.item i).left <;> simp

/-- Each edge the planner pushes into a derived item's arms comes from an
edge the derived item holds, through the result column an arm passes the
edge's member's column on to, and its member is an inner join's. -/
theorem plan_pushed (s : Statement) {e : Edge} (he : e ∈ (plan s).state.pushed) :
    PState.PushedFrom s (plan s).state e := by
  rcases (plan_keeps s).2 e he with h | h
  · simp [PState.initial] at h
  · exact h

/-- The first of three searches of `l` that finds an item, or `first`, is in
any list holding `l` and `first`. -/
theorem getD_find_mem {l : List Nat} {p1 p2 p3 : Nat → Bool} {first : Nat} {S : List Nat}
    (hl : ∀ x ∈ l, x ∈ S) (hfirst : first ∈ S) :
    ((((l.find? p1).orElse fun _ => l.find? p2).orElse fun _ => l.find? p3).getD first) ∈ S := by
  cases h1 : l.find? p1 with
  | some x => simpa [h1] using hl x (List.mem_of_find?_eq_some h1)
  | none =>
    cases h2 : l.find? p2 with
    | some x => simpa [h1, h2] using hl x (List.mem_of_find?_eq_some h2)
    | none =>
      cases h3 : l.find? p3 with
      | some x => simpa [h1, h2, h3] using hl x (List.mem_of_find?_eq_some h3)
      | none => simpa [h1, h2, h3] using hfirst

/-- A step of placing items finds none left, or places one of those left. -/
theorem ordered_go_step (s : Statement) (fuel : Nat) (all : List Nat) (st : PState) (order waited : List Nat) :
    (all.filter (fun i => !order.contains i) = [] ∧ ordered.go s (fuel + 1) all st order waited = (st, order)) ∨
    ∃ next st' waited', next ∈ all.filter (fun i => !order.contains i) ∧
      ordered.go s (fuel + 1) all st order waited = ordered.go s fuel all st' (order ++ [next]) waited' := by
  simp only [ordered.go]
  generalize hR : List.filter (fun i => !order.contains i) all = R
  cases R with
  | nil => exact Or.inl ⟨rfl, rfl⟩
  | cons r tail =>
    refine Or.inr ⟨_, _, _, ?_, rfl⟩
    split
    · rename_i i _ hfind
      exact List.mem_of_find?_eq_some hfind
    · rename_i first tail' _ hfree
      have hsub : ∀ x ∈ List.filter (fun i => !(waiting st order (r :: tail)).contains i) (r :: tail),
          x ∈ r :: tail := fun x hx => (List.mem_filter.mp hx).1
      have hfirst : first ∈ r :: tail := hsub first (by rw [hfree]; simp)
      exact getD_find_mem (fun x hx => hsub x (List.mem_filter.mp hx).1) hfirst
    · exact List.mem_cons_self

/-- Placing an item leaves fewer to place. -/
theorem remaining_shrinks {all order : List Nat} {next : Nat}
    (h : next ∈ all.filter (fun i => !order.contains i)) :
    (all.filter (fun i => !(order ++ [next]).contains i)).length <
      (all.filter (fun i => !order.contains i)).length := by
  have heq : all.filter (fun i => !(order ++ [next]).contains i) =
      (all.filter (fun i => !order.contains i)).filter (fun i => i != next) := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro i _
    simp only [List.contains_append, List.contains_cons, List.contains_nil, Bool.or_false,
      Bool.not_or]
    cases order.contains i <;> cases hi : (i == next) <;> simp_all [bne]
  rw [heq, List.length_filter_lt_length_iff_exists]
  exact ⟨next, h, by simp⟩

/-- Placing items from `all` after `order` adds each item once, and with fuel
for every item left, places them all. -/
theorem ordered_go_order (s : Statement) : ∀ (fuel : Nat) (all : List Nat) (st : PState)
    (order waited : List Nat), order.Nodup →
    ((ordered.go s fuel all st order waited).2).Nodup ∧
      ((all.filter (fun i => !order.contains i)).length ≤ fuel →
        ∀ i ∈ all, i ∈ (ordered.go s fuel all st order waited).2)
  | 0, all, st, order, waited, hnd => by
    refine ⟨hnd, fun hlen i hi => ?_⟩
    have hnil : all.filter (fun i => !order.contains i) = [] := List.eq_nil_of_length_eq_zero (by omega)
    have := List.filter_eq_nil_iff.mp hnil i hi
    simpa [ordered.go] using this
  | fuel + 1, all, st, order, waited, hnd => by
    rcases ordered_go_step s fuel all st order waited with ⟨hnil, heq⟩ | ⟨next, st', waited', hnext, heq⟩
    · rw [heq]
      refine ⟨hnd, fun _ i hi => ?_⟩
      have := List.filter_eq_nil_iff.mp hnil i hi
      simpa using this
    · rw [heq]
      have hnin : next ∉ order := by
        have := (List.mem_filter.mp hnext).2
        simpa using this
      have hnd' : (order ++ [next]).Nodup :=
        List.nodup_append.mpr ⟨hnd, (by simp), fun a ha b hb => by
          simp only [List.mem_singleton] at hb
          subst hb
          exact fun e => hnin (e ▸ ha)⟩
      obtain ⟨h1, h2⟩ := ordered_go_order s fuel all st' (order ++ [next]) waited' hnd'
      exact ⟨h1, fun hlen => h2 (by have := remaining_shrinks hnext; omega)⟩

/-- The planner places each item once, and every item of every core. -/
theorem plan_order (s : Statement) : (plan s).order.Nodup ∧
    ∀ k, k < s.cores.length → ∀ i ∈ (s.core k).items, i ∈ (plan s).order := by
  unfold plan ordered
  simp only
  obtain ⟨hnd, hall⟩ := ordered_go_order s _ ((List.range s.cores.length).flatMap fun k =>
    (s.core k).items) (s.readOrder.foldl (readCore s) (PState.initial s)) [] [] List.nodup_nil
  refine ⟨hnd, fun k hk i hi => hall (List.length_filter_le _ _) i ?_⟩
  exact List.mem_flatMap.mpr ⟨k, List.mem_range.mpr hk, hi⟩

end SqliveNarrowing
