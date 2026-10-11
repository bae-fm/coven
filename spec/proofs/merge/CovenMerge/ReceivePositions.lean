import CovenMerge.Receive

/-! Numbered frontiers implement the finite-past readiness check. Each
position represents the complete prefix of its device's log (§6, §7.1).
The differential runner uses this proved specialization. -/

namespace CovenMerge.Receive

structure Position where
  device : Nat
  number : Nat
  deriving DecidableEq, Repr

def covers (positions : List Position) (w : Position) : Bool :=
  positions.any fun p => decide (p.device = w.device ∧ w.number ≤ p.number)

def frontier (w : Position) (others : List Position) : List Position :=
  others ++ if 1 < w.number then [⟨w.device, w.number - 1⟩] else []

def expand (positions : List Position) : List Position :=
  positions.flatMap fun p => (List.range p.number).map fun n => ⟨p.device, n + 1⟩

theorem mem_expand (positions : List Position) (a : Position) :
    a ∈ expand positions ↔ ∃ p ∈ positions,
      a.device = p.device ∧ 0 < a.number ∧ a.number ≤ p.number := by
  simp only [expand, List.mem_flatMap, List.mem_map, List.mem_range]
  constructor
  · rintro ⟨p, hp, n, hn, rfl⟩
    exact ⟨p, hp, rfl, by dsimp; omega, by dsimp; omega⟩
  · rintro ⟨p, hp, hd, hn, hle⟩
    refine ⟨p, hp, a.number - 1, by omega, ?_⟩
    cases a
    simp_all
    omega

theorem covers_prefix (positions : List Position) (p a : Position)
    (covered : covers positions p = true) (device : a.device = p.device)
    (number : a.number ≤ p.number) : covers positions a = true := by
  simp only [covers, List.any_eq_true, decide_eq_true_eq] at covered ⊢
  obtain ⟨q, hq, hd, hn⟩ := covered
  exact ⟨q, hq, by omega, by omega⟩

theorem ready_frontier (required positions : List Position)
    (positive : ∀ p ∈ required, 0 < p.number) :
    ready required (covers positions) = true ↔ ready (expand required) (covers positions) = true := by
  rw [ready_iff, ready_iff]
  constructor
  · intro h a ha
    obtain ⟨p, hp, hd, _, hn⟩ := (mem_expand required a).mp ha
    exact covers_prefix positions p a (h p hp) hd hn
  · intro h p hp
    exact h p ((mem_expand required p).mpr ⟨p, hp, rfl, positive p hp, Nat.le_refl _⟩)

def positionsReady (w : Position) (others positions : List Position) : Bool :=
  ready (frontier w others) (covers positions)

theorem positionsReady_correct (w : Position) (others positions : List Position)
    (positive : ∀ p ∈ others, 0 < p.number) :
    positionsReady w others positions = true ↔
      ready (expand (frontier w others)) (covers positions) = true := by
  apply ready_frontier
  intro p hp
  rcases List.mem_append.mp hp with other | own
  · exact positive p other
  · split at own
    · simp only [List.mem_singleton] at own
      subst p
      simp only
      omega
    · simp at own

end CovenMerge.Receive
