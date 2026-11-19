import SqliveNarrowing.Tests

/-! The order the planner reads cores in (`Statement.readOrder`). A core sits
below core `a` when it is an arm of a query `a` reads, a derived item's or a
subquery's, at any depth (`Statement.Below`). A well-formed statement is a
tree: each core is an arm of one query, and one core reads each query, so a
core sits directly below one core at most, and the cores a core sits below
lie on one chain. `readOrder_postorder` proves that the planner reads each
core once, and never reads a core after a core it sits below: the planner
reads the cores below a core before the core itself. -/
namespace SqliveNarrowing

/-- Core `k` is an arm of a query core `a` reads. -/
def Statement.ArmOf (s : Statement) (k a : Nat) : Prop :=
  ∃ q ∈ s.nestedQueries a, k ∈ (s.query q).arms

/-- Core `k` sits below core `a`: in an arm of a query `a` reads, at any
depth. -/
inductive Statement.Below (s : Statement) : Nat → Nat → Prop
  | arm {k a : Nat} : s.ArmOf k a → s.Below k a
  | step {k m a : Nat} : s.ArmOf k m → s.Below m a → s.Below k a

theorem Statement.Below.trans {s : Statement} {x y z : Nat} (hxy : s.Below x y) (hyz : s.Below y z) :
    s.Below x z := by
  induction hxy with
  | arm h => exact .step h hyz
  | step h _ ih => exact .step h (ih hyz)

/-- A core sits directly below one core at most. -/
theorem Statement.armOf_unique {s : Statement} (hwf : s.wellFormed = true) {k a a' : Nat}
    (h : s.ArmOf k a) (h' : s.ArmOf k a') : a = a' := by
  obtain ⟨q, hq, hk⟩ := h
  obtain ⟨q', hq', hk'⟩ := h'
  obtain rfl := s.wellFormed_armOf hwf hk hk'
  exact s.wellFormed_reader hwf hq hq'

/-- A core below another is an arm of a query numbered below the other's. -/
theorem Statement.Below.query_lt {s : Statement} (hwf : s.wellFormed = true) {k a : Nat}
    (h : s.Below k a) : ∀ {qk qa : Nat}, k ∈ (s.query qk).arms → a ∈ (s.query qa).arms → qk < qa := by
  induction h with
  | @arm k a h =>
    intro qk qa hk ha
    obtain ⟨q, hq, hkq⟩ := h
    obtain rfl := s.wellFormed_armOf hwf hkq hk
    exact s.wellFormed_nested hwf (s.query_lt ha) ha hq
  | @step k m a h hm ih =>
    intro qk qa hk ha
    obtain ⟨q, hq, hkq⟩ := h
    obtain rfl := s.wellFormed_armOf hwf hkq hk
    have hm' : ∃ qm, m ∈ (s.query qm).arms := by
      cases hm with
      | arm h => obtain ⟨q', _, h'⟩ := h; exact ⟨q', h'⟩
      | step h _ => obtain ⟨q', _, h'⟩ := h; exact ⟨q', h'⟩
    obtain ⟨qm, hmq⟩ := hm'
    exact Nat.lt_trans (s.wellFormed_nested hwf (s.query_lt hmq) hmq hq) (ih hmq ha)

/-- No core sits below itself. -/
theorem Statement.Below.irrefl {s : Statement} (hwf : s.wellFormed = true) {a : Nat}
    (h : s.Below a a) : False := by
  have ha : ∃ q, a ∈ (s.query q).arms := by
    cases h with
    | arm h => obtain ⟨q, _, h'⟩ := h; exact ⟨q, h'⟩
    | step h _ => obtain ⟨q, _, h'⟩ := h; exact ⟨q, h'⟩
  obtain ⟨q, hq⟩ := ha
  exact Nat.lt_irrefl q (h.query_lt hwf hq hq)

/-- The cores a core sits below lie on the chain through the core it sits
directly below. -/
theorem Statement.Below.chain {s : Statement} (hwf : s.wellFormed = true) {k a p : Nat}
    (h : s.Below k a) (hp : s.ArmOf k p) : p = a ∨ s.Below p a := by
  cases h with
  | arm h => exact Or.inl (s.armOf_unique hwf hp h)
  | step h hm =>
    obtain rfl := s.armOf_unique hwf hp h
    exact Or.inr hm

/-- Two cores a core sits below are one, or one sits below the other. -/
theorem Statement.Below.comparable {s : Statement} (hwf : s.wellFormed = true) {k a : Nat}
    (h : s.Below k a) : ∀ {b : Nat}, s.Below k b → a = b ∨ s.Below a b ∨ s.Below b a := by
  induction h with
  | @arm k a h =>
    intro b hb
    rcases hb.chain hwf h with e | hab
    · exact Or.inl e
    · exact Or.inr (Or.inl hab)
  | @step k m a h _ ih =>
    intro b hb
    rcases hb.chain hwf h with rfl | hmb
    · exact Or.inr (Or.inr ‹s.Below m a›)
    · exact ih hmb

/-- Two cores that sit directly below one core have no core in common at or
below them. -/
theorem Statement.siblings_apart {s : Statement} (hwf : s.wellFormed = true) {a c1 c2 x : Nat}
    (h1 : s.ArmOf c1 a) (h2 : s.ArmOf c2 a) (hne : c1 ≠ c2)
    (hx1 : x = c1 ∨ s.Below x c1) (hx2 : x = c2 ∨ s.Below x c2) : False := by
  -- One of the two sits below the other, and so below the core both sit below.
  have hup : ∀ {c c' : Nat}, s.ArmOf c a → s.ArmOf c' a → s.Below c c' → False := by
    intro c c' hc hc' hb
    rcases hb.chain hwf hc with rfl | hab
    · exact (Statement.Below.arm hc').irrefl hwf
    · exact (hab.trans (.arm hc')).irrefl hwf
  rcases hx1 with rfl | hx1 <;> rcases hx2 with rfl | hx2
  · exact hne rfl
  · exact hup h1 h2 hx2
  · exact hup h2 h1 hx1
  · rcases hx1.comparable hwf hx2 with e | hb | hb
    · exact hne e
    · exact hup h1 h2 hb
    · exact hup h2 h1 hb

/-- Two arms of the statement's own query have no core in common at or below
them: no core reads the statement's own query. -/
theorem Statement.rootArms_apart {s : Statement} (hwf : s.wellFormed = true) {c1 c2 x : Nat}
    (h1 : c1 ∈ (s.query s.root).arms) (h2 : c2 ∈ (s.query s.root).arms) (hne : c1 ≠ c2)
    (hx1 : x = c1 ∨ s.Below x c1) (hx2 : x = c2 ∨ s.Below x c2) : False := by
  have hup : ∀ {c c' : Nat}, c ∈ (s.query s.root).arms → s.Below c c' → False := by
    intro c c' hc hb
    have hp : ∃ p, s.ArmOf c p := by
      cases hb with
      | arm h => exact ⟨_, h⟩
      | step h _ => exact ⟨_, h⟩
    obtain ⟨p, q, hq, hcq⟩ := hp
    obtain rfl := s.wellFormed_armOf hwf hcq hc
    exact s.wellFormed_root hwf p hq
  rcases hx1 with rfl | hx1 <;> rcases hx2 with rfl | hx2
  · exact hne rfl
  · exact hup h1 hx2
  · exact hup h2 hx1
  · rcases hx1.comparable hwf hx2 with e | hb | hb
    · exact hne e
    · exact hup h1 hb
    · exact hup h2 hb

/-- The cores directly below core `k`, in the order the planner reads them:
its derived items' queries' arms, then its subqueries'. -/
theorem Statement.readOrder_go_succ (s : Statement) (f k : Nat) :
    Statement.readOrder.go s (f + 1) k =
      ((s.nestedQueries k).flatMap fun q => (s.query q).arms).flatMap (Statement.readOrder.go s f) ++ [k] := by
  simp only [Statement.readOrder.go, Statement.nestedQueries, List.flatMap_append]
  congr 3
  induction (s.core k).items with
  | nil => rfl
  | cons i l ih => cases hk : (s.item i).kind <;> simp [hk, ih]

/-- The cores directly below a core are each in the list once. -/
theorem Statement.children_nodup {s : Statement} (hwf : s.wellFormed = true) (k : Nat) :
    ((s.nestedQueries k).flatMap fun q => (s.query q).arms).Nodup := by
  by_cases hk : k < s.cores.length
  · have hnd := s.nestedQueries_nodup hwf hk
    unfold List.Nodup
    rw [List.pairwise_flatMap]
    refine ⟨fun q hq => ?_, hnd.imp_of_mem fun {q q'} _ _ hne => ?_⟩
    · by_cases hq' : q < s.queries.length
      · exact nodup_of_flatMap (f := fun q => (s.query q).arms) (s.wellFormed_parts hwf).armCores (List.mem_range.mpr hq')
      · rw [s.query_out hq']; exact List.nodup_nil
    · intro x hx y hy e
      subst e
      exact hne (s.wellFormed_armOf hwf hx hy)
  · have : s.nestedQueries k = [] := by
      unfold Statement.nestedQueries
      rw [s.core_out hk]
      rfl
    rw [this]
    exact List.nodup_nil

/-- What reading below a core lists: the core and cores below it, each
once, and no core after a core it sits below. -/
theorem Statement.readOrder_go {s : Statement} (hwf : s.wellFormed = true) :
    ∀ (f a : Nat), (∀ x ∈ Statement.readOrder.go s f a, x = a ∨ s.Below x a) ∧
      (Statement.readOrder.go s f a).Pairwise fun y x => y ≠ x ∧ ¬s.Below x y
  | 0, a => by simp [Statement.readOrder.go]
  | f + 1, a => by
    rw [s.readOrder_go_succ]
    have ih := fun c => Statement.readOrder_go hwf f c
    have hmem : ∀ c, s.ArmOf c a → ∀ x ∈ Statement.readOrder.go s f c, s.Below x a := by
      intro c hc x hx
      rcases (ih c).1 x hx with rfl | hb
      · exact .arm hc
      · exact hb.trans (.arm hc)
    have harm : ∀ c ∈ (s.nestedQueries a).flatMap (fun q => (s.query q).arms), s.ArmOf c a := by
      intro c hc
      obtain ⟨q, hq, hcq⟩ := List.mem_flatMap.mp hc
      exact ⟨q, hq, hcq⟩
    refine ⟨fun x hx => ?_, ?_⟩
    · rcases List.mem_append.mp hx with hx | hx
      · obtain ⟨c, hc, hxc⟩ := List.mem_flatMap.mp hx
        exact Or.inr (hmem c (harm c hc) x hxc)
      · simp only [List.mem_singleton] at hx
        exact Or.inl hx
    · rw [List.pairwise_append, List.pairwise_flatMap]
      refine ⟨⟨fun c _ => (ih c).2, (s.children_nodup hwf a).imp_of_mem fun {c1 c2} h1 h2 hne => ?_⟩,
        List.pairwise_singleton _ _, fun y hy x hx => ?_⟩
      · intro y hy x hx
        have hy1 := (ih c1).1 y hy
        have hx2 := (ih c2).1 x hx
        refine ⟨fun e => ?_, fun hb => ?_⟩
        · subst e
          exact s.siblings_apart hwf (harm c1 h1) (harm c2 h2) hne hy1 hx2
        · have hx1 : x = c1 ∨ s.Below x c1 := by
            rcases hy1 with rfl | hy1
            · exact Or.inr hb
            · exact Or.inr (hb.trans hy1)
          exact s.siblings_apart hwf (harm c1 h1) (harm c2 h2) hne hx1 hx2
      · simp only [List.mem_singleton] at hx
        subst hx
        obtain ⟨c, hc, hyc⟩ := List.mem_flatMap.mp hy
        have hya := hmem c (harm c hc) y hyc
        exact ⟨fun e => (e ▸ hya).irrefl hwf, fun hb => (hb.trans hya).irrefl hwf⟩

/-- The planner reads each core once, and never reads a core after a core
it sits below. -/
theorem readOrder_postorder {s : Statement} (hwf : s.wellFormed = true) :
    s.readOrder.Pairwise fun y x => y ≠ x ∧ ¬s.Below x y := by
  unfold Statement.readOrder
  rw [List.pairwise_flatMap]
  have hnd : (s.query s.root).arms.Nodup :=
    nodup_of_flatMap (f := fun q => (s.query q).arms) (s.wellFormed_parts hwf).armCores (List.mem_range.mpr (s.wellFormed_parts hwf).root)
  refine ⟨fun c _ => (s.readOrder_go hwf _ c).2, hnd.imp_of_mem fun {c1 c2} h1 h2 hne => ?_⟩
  intro y hy x hx
  have hy1 := (s.readOrder_go hwf _ c1).1 y hy
  have hx2 := (s.readOrder_go hwf _ c2).1 x hx
  refine ⟨fun e => ?_, fun hb => ?_⟩
  · subst e
    exact s.rootArms_apart hwf h1 h2 hne hy1 hx2
  · have hx1 : x = c1 ∨ s.Below x c1 := by
      rcases hy1 with rfl | hy1
      · exact Or.inr hb
      · exact Or.inr (hb.trans hy1)
    exact s.rootArms_apart hwf h1 h2 hne hx1 hx2

end SqliveNarrowing
