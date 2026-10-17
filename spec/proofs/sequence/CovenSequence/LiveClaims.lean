import CovenSequence.Reader

/-! Unique values are selected from merged live rows, after the first pass
of other removal rules. A constraint's complete value uses the latest of its
current input setters. Primary-key rank breaks a tie within one write, as in
the merge package's `RemovalInputs.rank`. -/
namespace CovenSequence.LiveClaims

abbrev Position := Nat × Nat
abbrev Rank := Nat × Nat × Nat

def le (a b : Rank) : Prop :=
  a.1 < b.1 ∨ a.1 = b.1 ∧ (a.2.1 < b.2.1 ∨ a.2.1 = b.2.1 ∧ a.2.2 ≤ b.2.2)

instance (a b : Rank) : Decidable (le a b) := inferInstanceAs (Decidable (_ ∨ _))

theorem le_refl (a : Rank) : le a a := by unfold le; omega
theorem le_total (a b : Rank) : le a b ∨ le b a := by unfold le; omega
theorem le_trans {a b c : Rank} (ab : le a b) (bc : le b c) : le a c := by unfold le at *; omega
theorem le_antisymm {a b : Rank} (ab : le a b) (ba : le b a) : a = b := by
  obtain ⟨a, an, ak⟩ := a
  obtain ⟨b, bn, bk⟩ := b
  simp only [le] at ab ba
  congr <;> omega

variable {Row Value : Type}

def select (rank : Row → Rank) : List Row → Option Row
  | [] => none
  | r :: rs => match select rank rs with
    | none => some r
    | some s => if le (rank r) (rank s) then some r else some s

theorem select_none (rank : Row → Rank) (rs : List Row) : select rank rs = none ↔ rs = [] := by
  cases rs with
  | nil => simp [select]
  | cons r rs => simp only [select]; split <;> simp_all; split <;> simp

theorem select_min (rank : Row → Rank) (rs : List Row) (r : Row)
    (chosen : select rank rs = some r) : r ∈ rs ∧ ∀ s ∈ rs, le (rank r) (rank s) := by
  induction rs generalizing r with
  | nil => cases chosen
  | cons x xs ih =>
    simp only [select] at chosen
    cases old : select rank xs with
    | none =>
      simp only [old, Option.some.injEq] at chosen
      subst r
      have empty := (select_none rank xs).1 old
      subst xs
      exact ⟨by simp, by intro s hs; simp only [List.mem_singleton] at hs; subst s; exact le_refl _⟩
    | some y =>
      have tail := ih y old
      simp only [old] at chosen
      split at chosen
      · rename_i xy
        cases chosen
        refine ⟨by simp, fun s hs => ?_⟩
        rcases List.mem_cons.mp hs with rfl | hs
        · exact le_refl _
        · exact le_trans xy (tail.2 s hs)
      · rename_i xy
        cases chosen
        refine ⟨List.mem_cons_of_mem _ tail.1, fun s hs => ?_⟩
        rcases List.mem_cons.mp hs with rfl | hs
        · exact (le_total _ _).resolve_left xy
        · exact tail.2 s hs

theorem select_first (rank : Row → Rank) (rs : List Row) (r : Row)
    (unique : ∀ a b, rank a = rank b → a = b)
    (member : r ∈ rs) (first : ∀ s ∈ rs, le (rank r) (rank s)) : select rank rs = some r := by
  cases chosen : select rank rs with
  | none => have := (select_none rank rs).1 chosen; subst rs; cases member
  | some s =>
    have hs := select_min rank rs s chosen
    have eq := unique r s (le_antisymm (first s hs.1) (hs.2 r member))
    subst s; rfl

def latest : List Position → Position
  | [] => (0, 0)
  | p :: ps => if le (p.1, p.2, 0) ((latest ps).1, (latest ps).2, 0) then latest ps else p

theorem latest_mem (ps : List Position) (nonempty : ps ≠ []) : latest ps ∈ ps := by
  induction ps with
  | nil => exact (nonempty rfl).elim
  | cons p ps ih =>
    cases ps with
    | nil =>
      rw [latest]
      split
      · rename_i h
        have eq : p = (0, 0) := by
          change le (p.1, p.2, 0) (0, 0, 0) at h
          simp only [le] at h
          apply Prod.ext <;> simp only <;> omega
        simp [eq, latest]
      · exact List.mem_cons_self
    | cons q qs =>
      rw [latest]
      split
      · exact List.mem_cons_of_mem _ (ih (by simp))
      · exact List.mem_cons_self

theorem latest_ge (ps : List Position) (p : Position) (hp : p ∈ ps) :
    le (p.1, p.2, 0) ((latest ps).1, (latest ps).2, 0) := by
  induction ps with
  | nil => cases hp
  | cons q qs ih =>
    simp only [latest]
    split
    · rename_i h
      rcases List.mem_cons.mp hp with rfl | hp
      · exact h
      · exact ih hp
    · rename_i h
      rcases List.mem_cons.mp hp with rfl | hp
      · exact le_refl _
      · exact le_trans (ih hp) ((le_total _ _).resolve_left h)

/-- `live` is merged presence after the other rules' first pass, not the
displayed table after uniqueness. `inputs` lists setters of all constraint
inputs, including expression and partial-index inputs. -/
structure Inputs (Row Value : Type) where
  rows : List Row
  live : Row → Bool
  value : Row → Option Value
  inputs : Row → List Position
  counted : Position → Bool
  keyRank : Row → Nat

def rank (I : Inputs Row Value) (r : Row) : Rank :=
  ((latest (I.inputs r)).1, (latest (I.inputs r)).2, I.keyRank r)

def candidates [DecidableEq Value] (I : Inputs Row Value) (v : Value) : List Row :=
  I.rows.filter fun r => I.live r && decide (I.value r = some v) &&
    !(I.inputs r).isEmpty && (I.inputs r).all I.counted

def winner [DecidableEq Value] (I : Inputs Row Value) (v : Value) : Option Row :=
  select (rank I) (candidates I v)

theorem winner_live [DecidableEq Value] (I : Inputs Row Value) (v : Value) (r : Row)
    (chosen : winner I v = some r) :
    I.live r = true ∧ I.value r = some v ∧ ∀ p ∈ I.inputs r, I.counted p = true := by
  have h := List.mem_filter.mp (select_min (rank I) _ r chosen).1
  simp only [Bool.and_eq_true, decide_eq_true_eq, List.all_eq_true] at h
  exact ⟨h.2.1.1.1, h.2.1.1.2, h.2.2⟩

theorem winner_counted_setter [DecidableEq Value] (I : Inputs Row Value) (v : Value) (r : Row)
    (chosen : winner I v = some r) :
    latest (I.inputs r) ∈ I.inputs r ∧ I.counted (latest (I.inputs r)) = true := by
  have h := List.mem_filter.mp (select_min (rank I) _ r chosen).1
  have nonempty : I.inputs r ≠ [] := by
    intro empty
    simp [empty] at h
  have member := latest_mem (I.inputs r) nonempty
  exact ⟨member, (winner_live I v r chosen).2.2 _ member⟩

/-- Deleting or removing a row before uniqueness releases its claim. -/
theorem deleted_releases [DecidableEq Value] (I : Inputs Row Value) (v : Value) (r : Row)
    (gone : I.live r = false) : winner I v ≠ some r := by
  intro chosen
  have := (winner_live I v r chosen).1
  simp [gone] at this

/-- The first counted live claim wins, including claims formed from different
merged setters. No already deleted creation can reserve a value. -/
theorem first_live_claim [DecidableEq Value] (I : Inputs Row Value) (v : Value) (r : Row)
    (keys : ∀ a b, I.keyRank a = I.keyRank b → a = b)
    (eligible : r ∈ candidates I v) (first : ∀ s ∈ candidates I v, le (rank I r) (rank I s)) :
    winner I v = some r := by
  exact select_first (rank I) _ r (fun a b eq => keys a b (congrArg (fun x => x.2.2) eq)) eligible first

/-- At a shared slot the lower write number precedes any primary-key tie. -/
theorem within_slot (slot n m ka kb : Nat) (earlier : n < m) :
    le (slot, n, ka) (slot, m, kb) ∧ ¬ le (slot, m, kb) (slot, n, ka) := by
  simp only [le, Nat.lt_irrefl, false_or, true_and]
  constructor
  · exact Or.inl earlier
  · rintro (h | ⟨h, _⟩) <;> omega

/-- With no surviving claim from before release, the first counted claim
after release wins. Without that premise an older live loser can return. -/
theorem first_after_release [DecidableEq Value] (I : Inputs Row Value) (v : Value) (r : Row)
    (keys : ∀ a b, I.keyRank a = I.keyRank b → a = b) (release : Rank)
    (eligible : r ∈ candidates I v)
    (released : ∀ s ∈ candidates I v, le release (rank I s))
    (first : ∀ s ∈ candidates I v, le release (rank I s) → le (rank I r) (rank I s)) :
    winner I v = some r := first_live_claim I v r keys eligible (fun s hs => first s hs (released s hs))

/-- Compose with actual sequence readers: their merged rows, not an
independent claim log, supply the selection inputs. -/
theorem readers_agree {S : Sig} (R : Rules S) [DecidableEq Value]
    (inputs : S.Rows → Inputs Row Value) {st₁ st₂ : Store S} {seq : Nat → Slot S}
    {A : S.Aud} {a b : Reader S} (ha : ReaderOk R st₁ seq A a) (hb : ReaderOk R st₂ seq A b)
    (same : a.pos = b.pos) (v : Value) : winner (inputs a.rows) v = winner (inputs b.rows) v := by
  rw [(CovenSequence.readers_agree R ha hb same).2.2]

def releaseExample (deleted : Bool) : Inputs Nat Unit where
  rows := [1, 2, 3]
  live := fun r => !(deleted && r == 1)
  value := fun _ => some ()
  inputs := fun r => [(r, 1)]
  counted := fun _ => true
  keyRank := id

/-- Claim 1 wins; claim 2 loses but remains live. Deleting 1 lets 2 return,
ahead of the new claim 3. Requiring a new post-release claim would lose 2. -/
theorem older_loser_returns :
    winner (releaseExample false) () = some 1 ∧
    winner (releaseExample true) () = some 2 ∧
    winner (releaseExample true) () ≠ some 3 := by decide

end CovenSequence.LiveClaims
