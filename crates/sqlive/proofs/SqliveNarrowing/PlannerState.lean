import SqliveNarrowing.Planner
import SqliveNarrowing.WellFormed

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
takes. An edge to a subquery's source has its partner in an arm of some
core's subquery (`plan_nestedOK`), and each item's memberships and kept ORs
take values from items placed before it (`plan_placedBefore`). -/
namespace SqliveNarrowing

/-- Where a pushed edge comes from, in the state `st`: its member is an
inner join's, and an arm of a derived item's query, which keeps every row
and makes a row of each binding, or groups its rows by the member's column,
passes the member's column on unchanged as a result column, of which the
derived item holds an edge, as a membership or as an edge pushed into it,
with the same partner, partner value and NULL matching. -/
def PState.PushedFrom (s : Statement) (st : PState) (e : Edge) : Prop :=
  (st.items e.member).leftOn = none ∧ e.pushed = true ∧
  ∃ d q v k rename, (s.item d).kind = .derived q v ∧ (s.query q).limit = none ∧
    (s.query q).offset = none ∧ k ∈ (s.query q).arms ∧
    (s.core k).windowed = false ∧ (s.item e.member).core = k ∧
    (∀ c c', rename c = some c' →
      ((s.core k).aggregated = false ∨ (s.core k).groupBy.contains (.col e.member c') = true) ∧
      (s.core k).output[c]? = some (.expr (.col e.member c'))) ∧
    ∃ e0, (e0 ∈ (st.items d).memberships ∨ (e0 ∈ st.pushed ∧ e0.member = d)) ∧
      e0.memberValue.passed e0.member e.member rename = some e.memberValue ∧
      e0.partner = e.partner ∧ e0.value = e.value ∧ e0.nulls = e.nulls

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
  obtain ⟨hleft, hpushed, d, q, v, k, rename, hd, hl, ho, hk, hwin, hcore, hout, e0, he0, hrest⟩ := he
  refine ⟨?_, hpushed, d, q, v, k, rename, hd, hl, ho, hk, hwin, hcore, hout, e0, ?_, hrest⟩
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
query with no LIMIT and no OFFSET that calls no window function, passing on
each result column that is a bare column of its own item and, when the arm
groups its rows, one its GROUP BY lists. -/
theorem PushDown.mem_arms {s : Statement} {item : Nat} {found : List (List (Option (Nat × Nat)))}
    (h : PushDown.arms s item = some found) {arm : List (Option (Nat × Nat))} (harm : arm ∈ found) :
    ∃ q v k, (s.item item).kind = .derived q v ∧ (s.query q).limit = none ∧
      (s.query q).offset = none ∧ k ∈ (s.query q).arms ∧
      (s.core k).windowed = false ∧ ∀ (c t c' : Nat), (arm[c]?).join = some (t, c') →
        (s.item t).core = k ∧
        ((s.core k).aggregated = false ∨ (s.core k).groupBy.contains (.col t c') = true) ∧
        (s.core k).output[c]? = some (OutCol.expr (Expr.col t c')) := by
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
      simp only [List.mem_filter, Bool.not_eq_true'] at hk
      simp only [Bool.or_eq_true, Option.isSome_iff_ne_none, ne_eq, not_or, Decidable.not_not] at hcut
      refine ⟨q, v, k, hkind, hcut.1, hcut.2, hk.1, hk.2, fun c t c' hc => ?_⟩
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
            simp only [Bool.and_eq_true, decide_eq_true_eq, Bool.or_eq_true, Bool.not_eq_true'] at hj
            exact ⟨hj.1, hj.2, rfl⟩
          · cases hc
        · cases hc

/-- An edge `pushDown` passes to a target comes from one of the edges it
was given, its member value passed to the target. -/
theorem PushDown.mem_passed {s : Statement} {left : Bool} {target : Nat}
    {rename : Nat → Option Nat} {edges : List Edge} {e : Edge}
    (he : e ∈ PushDown.passedInto s left target rename edges) :
    ∃ e0 ∈ edges, ∃ value, e0.memberValue.passed e0.member target rename = some value ∧
      e = { e0 with member := target, memberValue := value, pushed := true } := by
  obtain ⟨e0, he0, hmap⟩ := List.mem_filterMap.mp he
  obtain ⟨value, hvalue, rfl⟩ := Option.map_eq_some_iff.mp hmap
  exact ⟨e0, (List.mem_filter.mp he0).1, value, hvalue, rfl⟩

/-- An edge `pushDown` passes comes from an edge that is no `IS` edge where
the target stays a LEFT JOIN's right side, and whose member value is strict
where a LEFT JOIN adds the target. -/
theorem PushDown.mem_passed_kept {s : Statement} {left : Bool} {target : Nat}
    {rename : Nat → Option Nat} {edges : List Edge} {e : Edge}
    (he : e ∈ PushDown.passedInto s left target rename edges) :
    ∃ e0 ∈ edges, ∃ value, e0.memberValue.passed e0.member target rename = some value ∧
      e = { e0 with member := target, memberValue := value, pushed := true } ∧
      (left = true → e0.nulls = false) ∧
      (s.alwaysBound target = true ∨ e0.memberValue.strict = true) := by
  obtain ⟨e0, he0, hmap⟩ := List.mem_filterMap.mp he
  obtain ⟨value, hvalue, rfl⟩ := Option.map_eq_some_iff.mp hmap
  obtain ⟨he0, hkeep⟩ := List.mem_filter.mp he0
  simp only [Bool.and_eq_true, Bool.or_eq_true, Bool.not_eq_true'] at hkeep
  refine ⟨e0, he0, value, hvalue, rfl, fun hl => ?_, hkeep.2⟩
  simpa [hl] using hkeep.1

/-- The column `renameInto` gives is the one the arm passes on to the
target. -/
theorem PushDown.renameInto_some {arm : List (Option (Nat × Nat))} {target c c' : Nat}
    (h : PushDown.renameInto arm target c = some c') : (arm[c]?).join = some (target, c') := by
  unfold PushDown.renameInto at h
  split at h
  · rename_i t c'' ht
    split at h
    · rename_i htt
      subst htt
      cases h
      exact ht
    · cases h
  · cases h

/-- A target `pushDown` passes into is an item the arm passes a column of. -/
theorem PushDown.mem_targets {arm : List (Option (Nat × Nat))} {reached : List Nat} {target : Nat}
    (h : target ∈ (reached.filterMap fun c => ((arm[c]?).join).map (·.1)).eraseDups) :
    ∃ (c c' : Nat), (arm[c]?).join = some (target, c') := by
  obtain ⟨c, _, hc⟩ := List.mem_filterMap.mp (List.mem_eraseDups.mp h)
  obtain ⟨⟨t, c'⟩, hc', ht⟩ := Option.map_eq_some_iff.mp hc
  simp only at ht
  subst ht
  exact ⟨c, c', hc'⟩

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
        obtain ⟨q, v, k, hkind, hlim, hoff, hk, hwin, hthrough⟩ := PushDown.mem_arms hfound harm
        have hpass0 := hpass.mono hst0.1
        refine PState.Keeps.foldl_from (base := st0) (fun st1 target htarget hst1 => ?_) st0
          (PState.Keeps.refl s st0)
        have hpass1 := hpass0.mono hst1.1
        obtain ⟨ct, ct', hct⟩ := PushDown.mem_targets htarget
        refine PushDown.step_keeps (fun e he => ?_)
          (fun st' h => PushDown.pushDown_keeps s narrowFuel fuel st' target _ _ h)
        obtain ⟨e0, he0, value, hvalue, rfl⟩ := PushDown.mem_passed he
        have hcore : (s.item target).core = k := (hthrough ct target ct' hct).1
        refine ⟨rfl, fun st' hgrow hleft => ⟨hleft, rfl, item, q, v, k,
          PushDown.renameInto arm target, hkind, hlim, hoff, hk, hwin, hcore,
          fun c c' hc =>
            ⟨(hthrough c target c' (PushDown.renameInto_some hc)).2.1,
              (hthrough c target c' (PushDown.renameInto_some hc)).2.2⟩,
          e0, ?_, hvalue, rfl, rfl, rfl⟩⟩
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

/-- The state after `ordered` places `next` with the items `order` placed
before it. -/
def ordered.placed (s : Statement) (st : PState) (order : List Nat) (next : Nat) : PState :=
  let taken := st.edges.filter fun e => e.member == next && e.placedIn order
  let either := (st.items next).ors.filter fun sides =>
    sides.all fun side => side.edges.all fun e => e.placedIn order
  let st := st.update next fun p => { p with memberships := p.memberships ++ taken, either }
  PushDown.pushDown s s.fuel s.fuel st next none taken

/-- A step of placing items finds none left, or places one of those left. -/
theorem ordered_go_place (s : Statement) (fuel : Nat) (all : List Nat) (st : PState) (order waited : List Nat) :
    (all.filter (fun i => !order.contains i) = [] ∧ ordered.go s (fuel + 1) all st order waited = (st, order)) ∨
    ∃ next waited', next ∈ all.filter (fun i => !order.contains i) ∧
      ordered.go s (fuel + 1) all st order waited =
        ordered.go s fuel all (ordered.placed s st order next) (order ++ [next]) waited' := by
  simp only [ordered.go]
  generalize hR : List.filter (fun i => !order.contains i) all = R
  cases R with
  | nil => exact Or.inl ⟨rfl, rfl⟩
  | cons r tail =>
    refine Or.inr ⟨_, _, ?_, rfl⟩
    split
    · rename_i i _ hfind
      exact List.mem_of_find?_eq_some hfind
    · rename_i first tail' _ hfree
      have hsub : ∀ x ∈ List.filter (fun i => !(waiting s st order (r :: tail)).contains i) (r :: tail),
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

/-- With a step for each item not yet found, a closure takes every item it
takes given what it found. -/
theorem closure_closed (items : List Nat) (adds : List Nat → Nat → Bool) :
    ∀ (fuel : Nat) (found : List Nat),
      (items.filter fun i => !found.contains i).length ≤ fuel →
      ∀ i ∈ items, adds (closure items adds fuel found) i = true →
        i ∈ closure items adds fuel found
  | 0, found, hlen, i, hi, _ => by
    have hnil : items.filter (fun i => !found.contains i) = [] :=
      List.eq_nil_of_length_eq_zero (by omega)
    have := List.filter_eq_nil_iff.mp hnil i hi
    simpa [closure] using this
  | fuel + 1, found, hlen, i, hi, hadds => by
    unfold closure at hadds ⊢
    cases hfind : items.find? (fun i => !found.contains i && adds found i) with
    | none =>
      simp only [hfind] at hadds ⊢
      have := List.find?_eq_none.mp hfind i hi
      cases hc : found.contains i with
      | true => simpa using hc
      | false =>
        rw [hc, hadds] at this
        exact absurd rfl this
    | some j =>
      simp only [hfind] at hadds ⊢
      have hj := List.mem_of_find?_eq_some hfind
      have hnew : found.contains j = false := by
        have := List.find?_some hfind
        simp only [Bool.and_eq_true, Bool.not_eq_true'] at this
        exact this.1
      have hshrink := remaining_shrinks (all := items) (order := found) (next := j)
        (List.mem_filter.mpr ⟨hj, by rw [hnew]; rfl⟩)
      exact closure_closed items adds fuel _ (by omega) i hi hadds

/-- An item that a waiting derived item could pass edges into waits too, so
the planner places it after that item, which then passes them into it. -/
theorem waiting_receivers (s : Statement) (st : PState) (order remaining : List Nat) {d i : Nat}
    (hd : d ∈ waiting s st order remaining) (hi : i ∈ receivers s st d) (hr : i ∈ remaining) :
    i ∈ waiting s st order remaining := by
  unfold waiting at hd ⊢
  refine closure_closed _ _ _ _ (List.length_filter_le _ _) i hr ?_
  simp only [received, Bool.or_eq_true, List.any_eq_true]
  exact Or.inr ⟨d, hd, by simpa using hi⟩

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
    rcases ordered_go_place s fuel all st order waited with ⟨hnil, heq⟩ | ⟨next, waited', hnext, heq⟩
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
      obtain ⟨h1, h2⟩ := ordered_go_order s fuel all _ (order ++ [next]) waited' hnd'
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


/-! Edges to a subquery's source. A condition's reader promises that such an
edge's partner sits in an arm of one of the condition's subqueries
(`Edge.NestedIn`), and the planner moves an edge's member only, so every
edge of the planned state to a subquery's source has a partner in an arm of
some core's subquery (`plan_nestedOK`): never an item of the statement's own
core, which sits in no core. -/

/-- Whether an edge to a subquery's source, or to the source a derived
item passes a column on from, has its partner in an arm of a query some core
reads. -/
def Edge.NestedOK (s : Statement) (e : Edge) : Prop :=
  e.nested = true → ∃ k, ∃ q ∈ s.nestedQueries k, (s.item e.partner).core ∈ (s.query q).arms

/-- Whether every edge of the narrowing keeps `Edge.NestedOK`. -/
def Narrowing.NestedOK (s : Statement) (n : Narrowing) : Prop := ∀ e ∈ n.edges, e.NestedOK s

/-- Whether every edge, membership and held edge of the state keeps
`Edge.NestedOK`, and each item's memberships are edges from it. -/
def PState.NestedOK (s : Statement) (st : PState) : Prop :=
  (∀ e ∈ st.edges, e.NestedOK s) ∧ (∀ i, ∀ m ∈ (st.items i).memberships, m.member = i ∧ m.NestedOK s) ∧
    ∀ i n, (st.items i).leftOn = some n → n.NestedOK s

/-- A fold each of whose steps keeps a property keeps it. -/
theorem foldl_invariant {α β : Type} {P : β → Prop} {g : β → α → β} :
    ∀ (l : List α), (∀ b x, x ∈ l → P b → P (g b x)) → ∀ b, P b → P (l.foldl g b)
  | [], _, _, h => h
  | x :: l, hg, b, h =>
    foldl_invariant l (fun b' y hy => hg b' y (List.mem_cons_of_mem x hy)) _ (hg b x (by simp) h)

theorem PState.NestedOK.update {s : Statement} {st : PState} (h : st.NestedOK s) {i : Nat}
    {f : PItem → PItem} (hm : ∀ m ∈ (f (st.items i)).memberships, m.member = i ∧ m.NestedOK s)
    (hl : ∀ n, (f (st.items i)).leftOn = some n → n.NestedOK s) : (st.update i f).NestedOK s := by
  refine ⟨h.1, fun j m hmj => ?_, fun j n hn => ?_⟩
  · simp only [PState.update] at hmj
    split at hmj
    · rename_i hji; subst hji; exact hm m hmj
    · exact h.2.1 j m hmj
  · simp only [PState.update] at hn
    split at hn
    · rename_i hji; subst hji; exact hl n hn
    · exact h.2.2 j n hn

/-- An update that keeps an item's memberships and keeps or drops its held
narrowing keeps the edges. -/
theorem PState.NestedOK.update_same {s : Statement} {st : PState} (h : st.NestedOK s) {i : Nat}
    {f : PItem → PItem} (hm : (f (st.items i)).memberships = (st.items i).memberships)
    (hl : ∀ n, (f (st.items i)).leftOn = some n → (st.items i).leftOn = some n) :
    (st.update i f).NestedOK s :=
  h.update (fun m hmem => h.2.1 i m (hm ▸ hmem)) (fun n hn => h.2.2 i n (hl n hn))

theorem PState.NestedOK.addTerm {s : Statement} {st : PState} (h : st.NestedOK s) (i : Nat) (t : STerm) :
    (st.addTerm i t).NestedOK s :=
  h.update_same rfl (fun _ hn => hn)

theorem PState.NestedOK.add {s : Statement} {st : PState} (h : st.NestedOK s) {n : Narrowing}
    (hn : n.NestedOK s) : (st.add n).1.NestedOK s := by
  simp only [PState.add]
  have hterms := foldl_invariant (P := PState.NestedOK s) (g := fun st (x : Nat × STerm) => st.addTerm x.1 x.2)
    n.terms (fun st x _ hst => hst.addTerm x.1 x.2) st h
  have hors := foldl_invariant (P := PState.NestedOK s) (g := fun st (d : Disjunction) =>
    st.update d.item fun p => { p with ors := p.ors ++ [d.sides] })
    n.ors (fun st d _ hst => hst.update_same rfl (fun _ hl => hl)) _ hterms
  refine ⟨fun e he => ?_, hors.2.1, hors.2.2⟩
  rcases List.mem_append.mp he with he | he
  · exact hors.1 e he
  · exact hn e he

theorem PState.NestedOK.narrow {s : Statement} :
    ∀ (fuel : Nat) {st : PState} {n : Narrowing}, st.NestedOK s → n.NestedOK s →
      (PState.narrow fuel st n).NestedOK s
  | 0, _, _, h, _ => h
  | fuel + 1, st, n, h, hn => by
    simp only [PState.narrow]
    refine foldl_invariant _ (fun st' i _ hst' => ?_) _ (h.add (fun e he => hn e (List.mem_filter.mp he).1))
    split
    · rename_i held hheld
      exact PState.NestedOK.narrow fuel (hst'.update_same rfl (by simp))
        (hst'.2.2 i held hheld)
    · exact hst'

theorem PState.NestedOK.joinInner {s : Statement} {st : PState} (h : st.NestedOK s) (fuel i : Nat) :
    (st.joinInner fuel i).NestedOK s := by
  unfold PState.joinInner
  split
  · rename_i held hheld
    exact PState.NestedOK.narrow fuel (h.update_same rfl (by simp)) (h.2.2 i held hheld)
  · exact h

theorem PState.NestedOK.narrowOn {s : Statement} {st : PState} (h : st.NestedOK s) (fuel i : Nat)
    {n : Narrowing} (hn : n.NestedOK s) : (st.narrowOn fuel i n).NestedOK s := by
  unfold PState.narrowOn
  simp only [Narrowing.split]
  have hown := h.add (n := ⟨n.terms.filter (·.1 == i), n.edges.filter (Edge.ownOn i),
    n.ors.filter (Disjunction.ownOn i)⟩) (fun e he => hn e (List.mem_filter.mp he).1)
  have hothers : Narrowing.NestedOK s ⟨n.terms.filter (·.1 != i), n.edges.filter (!Edge.ownOn i ·),
      n.ors.filter (!Disjunction.ownOn i ·)⟩ := fun e he => hn e (List.mem_filter.mp he).1
  split
  · rename_i held hheld
    refine hown.update (fun m hm => hown.2.1 i m hm) ?_
    intro n' hn'
    simp only [Option.some.injEq] at hn'
    subst hn'
    intro e he
    rcases List.mem_append.mp he with he | he
    · exact hown.2.2 i held hheld e he
    · exact hothers e he
  · exact PState.NestedOK.narrow fuel hown hothers

theorem PState.NestedOK.carry {s : Statement} {st : PState} (h : st.NestedOK s) (k : Nat) :
    (Transfer.carry s st k).NestedOK s :=
  foldl_invariant _ (fun st x _ hst => by
    unfold Transfer.addCarried
    exact hst.update_same rfl (fun _ hl => hl)) _ h

/-- An edge `pushDown` passes into an arm keeps its partner, partner value,
NULL matching and whether the partner is a subquery's source. -/
theorem Edge.NestedOK.moved {s : Statement} {e : Edge} (h : e.NestedOK s) (m : Nat)
    (value : PValue) : Edge.NestedOK s { e with member := m, memberValue := value, pushed := true } :=
  h

theorem PState.NestedOK.pushDown {s : Statement} (narrowFuel : Nat) :
    ∀ (fuel : Nat) {st : PState} (item : Nat) (term : Option STerm) {edges : List Edge},
      st.NestedOK s → (∀ e ∈ edges, e.NestedOK s) →
      (PushDown.pushDown s narrowFuel fuel st item term edges).NestedOK s
  | 0, _, _, _, _, h, _ => h
  | fuel + 1, st, item, term, edges, h, hedges => by
    simp only [PushDown.pushDown]
    split
    · exact h
    · split
      · exact h
      · refine foldl_invariant _ (fun st0 arm _ hst0 => ?_) _ h
        refine foldl_invariant _ (fun st1 target _ hst1 => ?_) _ hst0
        apply PState.NestedOK.pushDown narrowFuel fuel
        · have hpassed : ∀ e ∈ PushDown.passedInto s (st1.items target).leftOn.isSome target
              (PushDown.renameInto arm target) edges, e.NestedOK s := by
            intro e he
            obtain ⟨e0, he0, value, _, rfl⟩ := PushDown.mem_passed he
            exact (hedges e0 he0).moved target value
          have hst2 : ∀ (renamed : Option STerm), (match renamed with
              | some r => st1.addTerm target r
              | none => st1).NestedOK s := by
            intro renamed
            cases renamed
            · exact hst1
            · exact hst1.addTerm target _
          split
          · apply PState.NestedOK.joinInner
            exact ⟨fun e he => (List.mem_append.mp he).elim ((hst2 _).1 e) (hpassed e),
              (hst2 _).2.1, (hst2 _).2.2⟩
          · exact ⟨fun e he => (List.mem_append.mp he).elim ((hst2 _).1 e) (hpassed e),
              (hst2 _).2.1, (hst2 _).2.2⟩
        · intro e he
          obtain ⟨e0, he0, value, _, rfl⟩ := PushDown.mem_passed he
          exact (hedges e0 he0).moved target value

/-- The subqueries of a core's conditions are the core's subqueries. -/
theorem Core.cond_queries {c : Core} {cd : Cond} {p : Place} (hcd : (cd, p) ∈ c.conds) {q : Nat}
    (hq : q ∈ cd.queries) : q ∈ c.subqueries := by
  unfold Core.subqueries
  exact List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_append_right _
    (List.mem_flatMap.mpr ⟨(cd, p), hcd, hq⟩)))

theorem PState.NestedOK.readCore {s : Statement} {st : PState} (h : st.NestedOK s) (k : Nat) :
    (SqliveNarrowing.readCore s st k).NestedOK s := by
  unfold SqliveNarrowing.readCore
  simp only
  refine foldl_invariant _ (fun st i _ hst => PState.NestedOK.pushDown _ _ i _ hst (by simp)) _ ?_
  refine PState.NestedOK.carry ?_ k
  refine foldl_invariant _ (fun st x hx hst => ?_) _ h
  obtain ⟨n, p⟩ := x
  have hread : ∀ x ∈ (s.core k).conds.map fun (cd, p) => (readCondition s k cd, p),
      x.1.NestedOK s := by
    intro x hx e he
    obtain ⟨⟨cd, p'⟩, hcd, rfl⟩ := List.mem_map.mp hx
    intro hnest
    have hin := (readCondition_sound s k cd).2.1 e he |>.2.2.2.2.2.1
    cases hv : e.via with
    | none =>
      obtain ⟨q, hq, harm⟩ := hin.1 hnest hv
      exact ⟨k, q, List.mem_append_right _ (Core.cond_queries hcd hq), harm⟩
    | some dp =>
      obtain ⟨⟨_, xc, hps, _⟩, _⟩ := hin.2 dp.1 dp.2 hv
      obtain ⟨qd, v, a, hkind, harms, _, _, _, hd, _, hxa, _⟩ := Passed.passedSource_facts hps
      exact ⟨(s.item dp.1).core, qd,
        List.mem_append_left _ (List.mem_filterMap.mpr ⟨dp.1, hd, by simp [hkind]⟩),
        by rw [hxa, harms]; simp⟩
  have hn : n.NestedOK s := by
    rcases List.mem_append.mp hx with hx | hx
    · exact hread _ (List.mem_filter.mp hx).1
    · exact hread _ (List.mem_filter.mp hx).1
  cases p
  · exact PState.NestedOK.narrow _ hst hn
  · exact hst.narrowOn _ _ hn

theorem PState.NestedOK.ordered {s : Statement} :
    ∀ (fuel : Nat) (all : List Nat) {st : PState} (order waited : List Nat), st.NestedOK s →
      (SqliveNarrowing.ordered.go s fuel all st order waited).1.NestedOK s
  | 0, _, _, _, _, h => h
  | fuel + 1, all, st, order, waited, h => by
    simp only [SqliveNarrowing.ordered.go]
    split
    · exact h
    · apply PState.NestedOK.ordered fuel
      refine PState.NestedOK.pushDown _ _ _ _ (h.update ?_ ?_) ?_
      · intro m hm
        rcases List.mem_append.mp hm with hm | hm
        · exact h.2.1 _ m hm
        · have := List.mem_filter.mp hm
          simp only [Bool.and_eq_true, beq_iff_eq] at this
          exact ⟨this.2.1, h.1 m this.1⟩
      · exact h.2.2 _
      · exact fun e he => h.1 e (List.mem_filter.mp he).1

/-- Every edge, membership and held edge of the planned state to a
subquery's source has its partner in an arm of a subquery of some core, and
each item's memberships are edges from it. -/
theorem plan_nestedOK (s : Statement) : (plan s).state.NestedOK s := by
  unfold plan ordered
  simp only
  refine PState.NestedOK.ordered _ _ _ _ ?_
  refine foldl_invariant _ (fun st k _ hst => hst.readCore k) _ ?_
  refine ⟨fun e he => by simp [PState.initial] at he, fun i m hm => by simp [PState.initial] at hm,
    fun i n hn => ?_⟩
  simp only [PState.initial] at hn
  split at hn
  · cases hn; intro e he; cases he
  · cases hn


/-! What each item takes values from. The planner sets an item's
memberships and the ORs it keeps only as it places the item, from edges and
sides whose partners it placed before (`plan_placedBefore`); no other step
changes them. -/

/-- `st'` holds the memberships and kept ORs `st` holds. -/
def PState.SameLinks (st st' : PState) : Prop :=
  ∀ i, (st'.items i).memberships = (st.items i).memberships ∧ (st'.items i).either = (st.items i).either

theorem PState.SameLinks.refl (st : PState) : st.SameLinks st := fun _ => ⟨rfl, rfl⟩

theorem PState.SameLinks.trans {a b c : PState} (hab : a.SameLinks b) (hbc : b.SameLinks c) :
    a.SameLinks c := fun i => ⟨(hbc i).1.trans (hab i).1, (hbc i).2.trans (hab i).2⟩

theorem PState.SameLinks.foldl {α : Type} {g : PState → α → PState}
    (hg : ∀ (st : PState) (x : α), st.SameLinks (g st x)) :
    ∀ (l : List α) (st : PState), st.SameLinks (l.foldl g st)
  | [], st => PState.SameLinks.refl st
  | x :: l, st => (hg st x).trans (PState.SameLinks.foldl hg l (g st x))

theorem PState.sameLinks_update {st : PState} {i : Nat} {f : PItem → PItem}
    (hm : (f (st.items i)).memberships = (st.items i).memberships)
    (he : (f (st.items i)).either = (st.items i).either) : st.SameLinks (st.update i f) := by
  intro j
  simp only [PState.update]
  split
  · rename_i hji; subst hji; exact ⟨hm, he⟩
  · exact ⟨rfl, rfl⟩

theorem PState.sameLinks_addTerm (st : PState) (i : Nat) (t : STerm) : st.SameLinks (st.addTerm i t) :=
  PState.sameLinks_update rfl rfl

theorem PState.sameLinks_add (st : PState) (n : Narrowing) : st.SameLinks (st.add n).1 := by
  simp only [PState.add]
  exact (PState.SameLinks.foldl (fun st (x : Nat × STerm) => PState.sameLinks_addTerm st x.1 x.2) _ _).trans
    ((PState.SameLinks.foldl (g := fun st (d : Disjunction) =>
      st.update d.item fun p => { p with ors := p.ors ++ [d.sides] })
      (fun st d => PState.sameLinks_update rfl rfl) _ _).trans (fun _ => ⟨rfl, rfl⟩))

theorem PState.sameLinks_narrow : ∀ (fuel : Nat) (st : PState) (n : Narrowing),
    st.SameLinks (PState.narrow fuel st n)
  | 0, st, _ => PState.SameLinks.refl st
  | fuel + 1, st, n => by
    simp only [PState.narrow]
    refine (PState.sameLinks_add st _).trans (PState.SameLinks.foldl (fun st' i => ?_) _ _)
    split
    · exact (PState.sameLinks_update rfl rfl).trans (PState.sameLinks_narrow fuel _ _)
    · exact PState.SameLinks.refl _

theorem PState.sameLinks_joinInner (fuel : Nat) (st : PState) (i : Nat) :
    st.SameLinks (st.joinInner fuel i) := by
  unfold PState.joinInner
  split
  · exact (PState.sameLinks_update rfl rfl).trans (PState.sameLinks_narrow fuel _ _)
  · exact PState.SameLinks.refl _

theorem PState.sameLinks_narrowOn (fuel : Nat) (st : PState) (i : Nat) (n : Narrowing) :
    st.SameLinks (st.narrowOn fuel i n) := by
  unfold PState.narrowOn
  generalize n.split i = x
  obtain ⟨own, others⟩ := x
  simp only
  refine (PState.sameLinks_add st own).trans ?_
  split
  · exact PState.sameLinks_update rfl rfl
  · exact PState.sameLinks_narrow fuel _ _

theorem Transfer.sameLinks_carry (s : Statement) (st : PState) (k : Nat) :
    st.SameLinks (Transfer.carry s st k) :=
  PState.SameLinks.foldl (fun st (x : Nat × STerm) => by
    unfold Transfer.addCarried
    exact PState.sameLinks_update rfl rfl) _ st

theorem PushDown.sameLinks_pushDown (s : Statement) (narrowFuel : Nat) :
    ∀ (fuel : Nat) (st : PState) (item : Nat) (term : Option STerm) (edges : List Edge),
      st.SameLinks (PushDown.pushDown s narrowFuel fuel st item term edges)
  | 0, st, _, _, _ => PState.SameLinks.refl st
  | fuel + 1, st, item, term, edges => by
    simp only [PushDown.pushDown]
    split
    · exact PState.SameLinks.refl st
    · split
      · exact PState.SameLinks.refl st
      · refine PState.SameLinks.foldl (fun st0 arm => ?_) _ st
        refine PState.SameLinks.foldl (fun st1 target => ?_) _ st0
        refine PState.SameLinks.trans ?_ (PushDown.sameLinks_pushDown s narrowFuel fuel _ _ _ _)
        have h2 : ∀ (renamed : Option STerm), st1.SameLinks (match renamed with
            | some r => st1.addTerm target r
            | none => st1) := by
          intro renamed
          cases renamed
          · exact PState.SameLinks.refl st1
          · exact PState.sameLinks_addTerm st1 target _
        have hrec : ∀ (st2 : PState) (passed : List Edge),
            st2.SameLinks { st2 with edges := st2.edges ++ passed, pushed := st2.pushed ++ passed } :=
          fun _ _ _ => ⟨rfl, rfl⟩
        split
        · exact (h2 _).trans ((hrec _ _).trans (PState.sameLinks_joinInner _ _ _))
        · exact (h2 _).trans (hrec _ _)

theorem sameLinks_readCore (s : Statement) (st : PState) (k : Nat) :
    st.SameLinks (readCore s st k) := by
  unfold readCore
  simp only
  refine PState.SameLinks.trans ?_ (PState.SameLinks.foldl
    (fun st i => PushDown.sameLinks_pushDown s _ _ st i _ _) _ _)
  refine PState.SameLinks.trans ?_ (Transfer.sameLinks_carry s _ k)
  refine PState.SameLinks.foldl (fun st x => ?_) _ st
  obtain ⟨n, p⟩ := x
  cases p
  · exact PState.sameLinks_narrow _ _ _
  · exact PState.sameLinks_narrowOn _ _ _ _

/-- Whether `pre` holds an edge's partner and the derived item it reads
through, if any. -/
def Edge.Before (pre : List Nat) (e : Edge) : Prop :=
  e.partner ∈ pre ∧ ∀ d pc, e.via = some (d, pc) → d ∈ pre

theorem Edge.before_of_placedIn {order : List Nat} {e : Edge} (h : e.placedIn order = true) :
    e.Before order := by
  unfold Edge.placedIn at h
  simp only [Bool.and_eq_true, List.contains_iff_mem] at h
  refine ⟨h.1, fun d pc hv => ?_⟩
  have := h.2
  rw [hv] at this
  simpa using this

/-- Whether each item placed in `order` takes its memberships' values and
its kept ORs' sides' values from items placed before it, through derived
items placed before it, and each item not placed has no membership and keeps
no OR. -/
def PState.PlacedLinks (st : PState) (order : List Nat) : Prop :=
  ∀ i, (i ∉ order → (st.items i).memberships = [] ∧ (st.items i).either = []) ∧
    (∀ m ∈ (st.items i).memberships, ∃ pre post, order = pre ++ i :: post ∧ m.Before pre) ∧
    ∀ sides ∈ (st.items i).either, ∀ side ∈ sides, ∀ e ∈ side.edges,
      ∃ pre post, order = pre ++ i :: post ∧ e.Before pre

theorem PState.PlacedLinks.placed {s : Statement} {st : PState} {order : List Nat}
    (h : st.PlacedLinks order) {next : Nat} (hnext : next ∉ order) :
    (ordered.placed s st order next).PlacedLinks (order ++ [next]) := by
  unfold ordered.placed
  simp only
  generalize hst' : st.update next _ = st'
  have hsame := PushDown.sameLinks_pushDown s s.fuel s.fuel st' next none
    (st.edges.filter fun e => e.member == next && e.placedIn order)
  generalize PushDown.pushDown s s.fuel s.fuel st' next none _ = st'' at hsame
  have hext : ∀ {i : Nat} {x : Edge}, (∃ pre post, order = pre ++ i :: post ∧ x.Before pre) →
      ∃ pre post, order ++ [next] = pre ++ i :: post ∧ x.Before pre := by
    rintro i x ⟨pre, post, heq, hx⟩
    exact ⟨pre, post ++ [next], by simp [heq], hx⟩
  intro i
  rw [(hsame i).1, (hsame i).2]
  subst hst'
  simp only [PState.update]
  split
  · rename_i hi
    subst hi
    obtain ⟨hm0, he0⟩ := (h i).1 hnext
    refine ⟨fun hn => absurd (List.mem_append_right _ (List.mem_singleton_self i)) hn, ?_, ?_⟩
    · intro m hm
      simp only [hm0, List.nil_append, List.mem_filter, Bool.and_eq_true, beq_iff_eq] at hm
      exact ⟨order, [], rfl, Edge.before_of_placedIn hm.2.2⟩
    · intro sides hsides side hside e he
      simp only [List.mem_filter, List.all_eq_true] at hsides
      exact ⟨order, [], rfl, Edge.before_of_placedIn (hsides.2 side hside e he)⟩
  · rename_i hi
    refine ⟨fun hn => (h i).1 (fun ho => hn (List.mem_append_left _ ho)),
      fun m hm => hext ((h i).2.1 m hm), fun sides hs side hside e he => hext ((h i).2.2 sides hs side hside e he)⟩

theorem ordered_go_placedLinks (s : Statement) : ∀ (fuel : Nat) (all : List Nat) (st : PState)
    (order waited : List Nat), st.PlacedLinks order →
    (ordered.go s fuel all st order waited).1.PlacedLinks (ordered.go s fuel all st order waited).2
  | 0, _, _, _, _, h => h
  | fuel + 1, all, st, order, waited, h => by
    rcases ordered_go_place s fuel all st order waited with ⟨_, heq⟩ | ⟨next, waited', hnext, heq⟩
    · rw [heq]; exact h
    · rw [heq]
      have hnin : next ∉ order := by
        have := (List.mem_filter.mp hnext).2
        simpa using this
      exact ordered_go_placedLinks s fuel all _ _ _ (h.placed hnin)

/-- Each item's memberships, and the sides of each OR it keeps, take values
from items the planner placed before it, through derived items it placed
before it. -/
theorem plan_before (s : Statement) (i : Nat) :
    (∀ m ∈ ((plan s).state.items i).memberships,
      ∃ pre post, (plan s).order = pre ++ i :: post ∧ m.Before pre) ∧
    ∀ sides ∈ ((plan s).state.items i).either, ∀ side ∈ sides, ∀ e ∈ side.edges,
      ∃ pre post, (plan s).order = pre ++ i :: post ∧ e.Before pre := by
  unfold plan ordered
  simp only
  refine (ordered_go_placedLinks s _ _ _ [] [] (fun j => ?_) i).2
  have hsame := PState.SameLinks.foldl (fun st k => sameLinks_readCore s st k) s.readOrder (PState.initial s) j
  rw [hsame.1, hsame.2]
  simp [PState.initial]

/-- Each item's memberships, and the sides of each OR it keeps, take values
from items the planner placed before it. -/
theorem plan_placedBefore (s : Statement) (i : Nat) :
    (∀ m ∈ ((plan s).state.items i).memberships,
      ∃ pre post, (plan s).order = pre ++ i :: post ∧ m.partner ∈ pre) ∧
    ∀ sides ∈ ((plan s).state.items i).either, ∀ side ∈ sides, ∀ e ∈ side.edges,
      ∃ pre post, (plan s).order = pre ++ i :: post ∧ e.partner ∈ pre :=
  ⟨fun m hm => by
    obtain ⟨pre, post, h, hb⟩ := (plan_before s i).1 m hm
    exact ⟨pre, post, h, hb.1⟩,
  fun sides hs side hside e he => by
    obtain ⟨pre, post, h, hb⟩ := (plan_before s i).2 sides hs side hside e he
    exact ⟨pre, post, h, hb.1⟩⟩

end SqliveNarrowing
