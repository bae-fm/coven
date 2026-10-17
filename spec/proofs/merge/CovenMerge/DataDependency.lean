import CovenMerge.Model
import Std

/-! Data provenance survives exclusion, conversion, snapshots and loss dismissal.
Recorded read positions are deliberately absent from the dependency decision.
The row is part of each change's identity; losing one change does not drop its
write's other changes. All sources below belong to that same row. -/

namespace CovenMerge.DataDependency

structure Provenance (W Col : Type) where
  generation : Option W
  setters : List (Col × W)
  deriving DecidableEq, Repr

def Provenance.sources {W Col : Type} (p : Provenance W Col) : List W :=
  p.generation.toList ++ p.setters.map Prod.snd

/-- Retained merge evidence, independent of displayed rows and dismissible
losses. `base` is the change's own admission, before following data sources.
Validation rejects missing, false or non-earlier provenance before consumption. -/
structure State (W Row Col : Type) where
  rank : W → Nat
  present : W → Row → Bool
  base : W → Row → Bool
  provenance : W → Row → Provenance W Col
  earlier : ∀ w r a, a ∈ (provenance w r).sources → rank a < rank w

variable {W Row Col : Type}

/-- Recursion follows the authenticated source's timestamp, with no fuel or
search through frontiers. A lost source change cannot support another change. -/
def retained (s : State W Row Col) (w : W) (r : Row) : Bool :=
  s.base w r && s.present w r &&
    (s.provenance w r).sources.attach.all (fun a => retained s a.val r)
termination_by s.rank w
decreasing_by exact s.earlier w r a.val a.property

theorem retained_eq (s : State W Row Col) (w : W) (r : Row) :
    retained s w r = (s.base w r && s.present w r &&
      (s.provenance w r).sources.all (fun a => retained s a r)) := by
  rw [retained]
  simp

/-- The shared definition: this change used a generation or replaced a cell
setter whose own change does not survive. Cut-off, schema and other direct
exclusion policies supply `base`; none changes this dependency relation. -/
def dependsOnDropped (s : State W Row Col) (w : W) (r : Row) : Bool :=
  (s.provenance w r).sources.any (fun a => !retained s a r)

def droppedSources (s : State W Row Col) (w : W) (r : Row) : List W :=
  (s.provenance w r).sources.filter (fun a => !retained s a r)

theorem depends_iff (s : State W Row Col) (w : W) (r : Row) :
    dependsOnDropped s w r = true ↔
      ∃ a ∈ (s.provenance w r).sources, retained s a r = false := by
  simp [dependsOnDropped]

theorem retained_iff (s : State W Row Col) (w : W) (r : Row) :
    retained s w r = true ↔ s.base w r = true ∧ s.present w r = true ∧
      ∀ a ∈ (s.provenance w r).sources, retained s a r = true := by
  rw [retained_eq]
  simp only [Bool.and_eq_true, List.all_eq_true]
  exact and_assoc

theorem retained_not_dependent (s : State W Row Col) (w : W) (r : Row)
    (h : retained s w r = true) : dependsOnDropped s w r = false := by
  have hs := (retained_iff s w r).mp h
  simpa [dependsOnDropped, List.any_eq_false] using hs.2.2

theorem dropped_stays_dropped (s : State W Row Col) (w : W) (r : Row)
    (h : s.base w r = false) : retained s w r = false := by
  rw [retained_eq]; simp [h]

theorem dependent_not_retained (s : State W Row Col) (w : W) (r : Row)
    (h : dependsOnDropped s w r = true) : retained s w r = false := by
  cases hk : retained s w r
  · rfl
  · have := retained_not_dependent s w r hk
    simp [h] at this

theorem independent_retained (s : State W Row Col) (w : W) (r : Row)
    (base : s.base w r = true) (present : s.present w r = true)
    (independent : dependsOnDropped s w r = false) : retained s w r = true := by
  apply (retained_iff s w r).mpr
  refine ⟨base, present, ?_⟩
  simpa [dependsOnDropped, List.any_eq_false] using independent

/-- An executable decision from retained merge evidence alone. In particular
there is no oracle for applied reads, device identity, or future arrivals. -/
def dependencyDecision (s : State W Row Col) (w : W) (r : Row) :
    Decidable (∃ a ∈ (s.provenance w r).sources, retained s a r = false) :=
  decidable_of_iff (dependsOnDropped s w r = true) (depends_iff s w r)

/-- Equal stored facts suffice even if devices use different valid rankings.
Neither changes elsewhere nor a device's recorded-read frontier participates. -/
theorem retained_congr (a b : State W Row Col)
    (present : a.present = b.present) (base : a.base = b.base)
    (sources : ∀ w r, (a.provenance w r).sources = (b.provenance w r).sources)
    (w : W) (r : Row) : retained a w r = retained b w r := by
  generalize hn : a.rank w = n
  induction n using Nat.strongRecOn generalizing w with
  | ind n ih =>
    have children : (a.provenance w r).sources.all (fun x => retained a x r) =
        (b.provenance w r).sources.all (fun x => retained b x r) := by
      apply Bool.eq_iff_iff.mpr
      rw [← sources w r]
      simp only [List.all_eq_true]
      have eq : ∀ x ∈ (a.provenance w r).sources, retained a x r = retained b x r := by
        intro x hx
        exact ih (a.rank x) (by have := a.earlier w r x hx; omega) x rfl
      constructor
      · intro h x hx; rw [← eq x hx]; exact h x hx
      · intro h x hx; rw [eq x hx]; exact h x hx
    rw [retained_eq, retained_eq, present, base, children]

theorem state_alone (a b : State W Row Col)
    (present : a.present = b.present) (base : a.base = b.base)
    (sources : ∀ w r, (a.provenance w r).sources = (b.provenance w r).sources)
    (w : W) (r : Row) : dependsOnDropped a w r = dependsOnDropped b w r := by
  simp only [dependsOnDropped, sources, retained_congr a b present base sources]

/-- Recording already-decided losses in the retained state cannot change any
decision. This covers conversion and snapshot preservation of frozen changes. -/
theorem retained_after_recording_losses (a b : State W Row Col)
    (present : a.present = b.present)
    (sources : ∀ w r, (a.provenance w r).sources = (b.provenance w r).sources)
    (onlyDrop : ∀ w r, b.base w r = true → a.base w r = true)
    (keep : ∀ w r, retained a w r = true → b.base w r = true)
    (w : W) (r : Row) : retained a w r = retained b w r := by
  generalize hn : a.rank w = n
  induction n using Nat.strongRecOn generalizing w with
  | ind n ih =>
    have eq : ∀ x ∈ (a.provenance w r).sources, retained a x r = retained b x r := by
      intro x hx
      exact ih (a.rank x) (by have := a.earlier w r x hx; omega) x rfl
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro h
      have parts := (retained_iff a w r).mp h
      apply (retained_iff b w r).mpr
      refine ⟨keep w r h, by rw [← present]; exact parts.2.1, ?_⟩
      intro x hx
      rw [← sources w r] at hx
      rw [← eq x hx]
      exact parts.2.2 x hx
    · intro h
      have parts := (retained_iff b w r).mp h
      apply (retained_iff a w r).mpr
      refine ⟨onlyDrop w r parts.1, by rw [present]; exact parts.2.1, ?_⟩
      intro x hx
      rw [eq x hx]
      exact parts.2.2 x (by rw [← sources w r]; exact hx)

/-- Evidence about the data actually used when authoring. A numeric generation
or a timestamp-plausible frontier member is not a substitute for this source.
The setter list contains exactly the old cells the row change replaces. -/
structure Authored (M : Writes W Row Col) where
  provenance : W → Row → Provenance W Col
  read_source : ∀ w r a, a ∈ (provenance w r).sources → M.past w a = true
  generation_source : ∀ w r ch, M.chg w r = some ch →
    (ch.gen = 0 ∧ (provenance w r).generation = none) ∨
    ∃ a ca, (provenance w r).generation = some a ∧ M.chg a r = some ca ∧
      ca.kind ≠ .upd ∧ ca.gen + 1 = ch.gen
  setter_source : ∀ w r c a, (c, a) ∈ (provenance w r).setters →
    ∃ ch ca, M.chg w r = some ch ∧ M.chg a r = some ca ∧
      ca.kind ≠ .del ∧ ca.sets c = true ∧ ca.inc = ch.inc

def authoredState (M : Writes W Row Col) (valid : Valid M) (p : Authored M)
    (base : W → Row → Bool) : State W Row Col where
  rank := M.ts
  present w r := (M.chg w r).isSome
  base := base
  provenance := p.provenance
  earlier w r a h := valid.past_ts w a (p.read_source w r a h)

/-- Consumed writes keep their causal metadata even if all their changes
were dropped. Only actual data changes enter the row merge. -/
def effective (M : Writes W Row Col) (valid : Valid M) (p : Authored M)
    (base : W → Row → Bool) : Writes W Row Col :=
  { M with chg := fun w r =>
      if retained (authoredState M valid p base) w r then M.chg w r else none }

theorem effective_valid (M : Writes W Row Col) (valid : Valid M) (p : Authored M)
    (base : W → Row → Bool) : Valid (effective M valid p base) where
  ts_inj := valid.ts_inj
  past_ts := valid.past_ts
  gen_seen w r ch h := by
    simp only [effective] at h
    split at h
    · rename_i kept
      rcases p.generation_source w r ch h with ⟨zero, _⟩ | ⟨a, ca, source, change, kind, gen⟩
      · exact Or.inl zero
      · have mem : a ∈ (p.provenance w r).sources := by simp [Provenance.sources, source]
        have ka := ((retained_iff _ w r).mp kept).2.2 a mem
        exact Or.inr ⟨a, ca, p.read_source w r a mem,
          by simp [effective, ka, change], kind, gen⟩
    · contradiction
  parity w r ch h := by
    simp only [effective] at h
    split at h
    · exact valid.parity w r ch h
    · contradiction

theorem causal_from (M : Writes W Row Col) (valid : Valid M) (p : Authored M)
    (base : W → Row → Bool) {C : W → Prop} {xs : List W}
    (h : CausalFrom M C xs) : CausalFrom (effective M valid p base) C xs := by
  induction h with
  | nil => exact .nil
  | snoc _ hc fresh past ih => exact .snoc ih hc fresh past

end CovenMerge.DataDependency
