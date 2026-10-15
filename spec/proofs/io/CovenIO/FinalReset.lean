/-! ## A final reset left out of the hold never becomes the boundary again

§15's hold checks every boundary replay could still select for an audience:
the current one, the one the final entries alone select, and every non-final
raise or reset. A final reset is left out unless it is one of the first two
(Rust `held_coverage`). `RaiseHistory.finalize` assumes that leaving it out
is safe: replay never selects it again. This file proves it from §9's
finality and replay's boundary rules.

Replay applies one audience's kept raise and reset entries in timestamp order
(Rust `Boundary::select`): a raise below the current version, or repeating the
current raise, and a reset repeating the current reset are already in place
and change nothing; every other one is effective, and the last effective one
is the audience's boundary. A dropped final reset is never kept again. For a
kept final reset `F`, every kept entry ordered before it that is not final is
concurrent with it: `F` read only entries stored before it, which are final,
and an entry that read `F` is ordered after it. Concurrent kept entries never
conflict, and §9 makes any two raise or reset entries of one audience conflict
unless they repeat one raise or name one reset snapshot. So those entries are
resets naming `F`'s snapshot (`before_final_reset_repeats_it`), and the state
after `F` is the same as replaying the final entries alone. If an entry after
`F` was effective there, one is effective in every later replay too
(`final_reset_never_returns`). -/
namespace CovenIO.FinalReset

/-- One audience's raise (to a version, naming a snapshot) or reset. -/
inductive Boundary where
  | raise (version snapshot : Nat)
  | reset (snapshot : Nat)
  deriving DecidableEq, Repr

structure Entry where
  id : Nat
  boundary : Boundary
  deriving DecidableEq, Repr

/-- The audience's replayed version and reset (Rust `StoreLogState.schema`,
`resets`). -/
structure Placed where
  schema : Option (Nat × Nat)
  reset : Option Nat
  deriving DecidableEq, Repr

def Placed.empty : Placed := ⟨none, none⟩

/-- Rust `Boundary::already_in_place`. -/
def inPlace (s : Placed) : Boundary → Bool
  | .raise v snap => match s.schema with
      | some (cv, cs) => decide (v < cv) || (v == cv && snap == cs)
      | none => false
  | .reset snap => s.reset == some snap

/-- Rust `Boundary::apply`. -/
def apply (s : Placed) : Boundary → Placed
  | .raise v snap => { s with schema := some (v, snap) }
  | .reset snap => { s with reset := some snap }

def next (s : Placed) (e : Entry) : Placed :=
  if inPlace s e.boundary then s else apply s e.boundary

def run (s : Placed) (es : List Entry) : Placed := es.foldl next s

/-- The effective entries in replay order (Rust `Boundary::select`). -/
def select : Placed → List Entry → List Entry
  | _, [] => []
  | s, e :: es => if inPlace s e.boundary then select s es else e :: select (apply s e.boundary) es

/-- The audience's boundary: its last effective entry. -/
def current (es : List Entry) : Option Entry := (select .empty es).getLast?

/-- §9's conflict rule for two raise or reset entries of one audience. -/
def Boundary.conflicts : Boundary → Boundary → Bool
  | .raise v s, .raise w t => v == w && s != t
  | .reset s, .reset t => s != t
  | _, _ => true

theorem before_final_reset_repeats_it (b : Boundary) (snapshot : Nat)
    (concurrent : b.conflicts (.reset snapshot) = false) : b = .reset snapshot := by
  cases b with
  | raise v s => simp [Boundary.conflicts] at concurrent
  | reset s => simp [Boundary.conflicts] at concurrent; simp [concurrent]

theorem select_append (s : Placed) (l₁ l₂ : List Entry) :
    select s (l₁ ++ l₂) = select s l₁ ++ select (run s l₁) l₂ := by
  induction l₁ generalizing s with
  | nil => rfl
  | cons e es ih =>
      simp only [List.cons_append, select, run, List.foldl_cons, next]
      split
      · exact ih s
      · rw [ih]; rfl

theorem select_nil (s : Placed) (l : List Entry) (none : select s l = []) :
    ∀ e ∈ l, inPlace s e.boundary = true := by
  induction l with
  | nil => intro e h; cases h
  | cons x xs ih =>
      simp only [select] at none
      split at none
      · rename_i h
        intro e he
        rcases List.mem_cons.mp he with rfl | he
        · exact h
        · exact ih none e he
      · cases none

theorem select_of_inPlace (s : Placed) (l : List Entry)
    (all : ∀ e ∈ l, inPlace s e.boundary = true) : select s l = [] := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [select, all x List.mem_cons_self, ite_true]
      exact ih fun e he => all e (List.mem_cons_of_mem _ he)

theorem select_sub (s : Placed) (l : List Entry) : ∀ e ∈ select s l, e ∈ l := by
  induction l generalizing s with
  | nil => intro e h; cases h
  | cons x xs ih =>
      intro e he
      simp only [select] at he
      split at he
      · exact List.mem_cons_of_mem _ (ih s e he)
      · rcases List.mem_cons.mp he with rfl | he
        · exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ (ih _ e he)

theorem inPlace_raise_schema (s t : Placed) (v snap : Nat) (same : s.schema = t.schema) :
    inPlace s (.raise v snap) = inPlace t (.raise v snap) := by
  simp [inPlace, same]

/-- Replaying the kept entries and replaying only the final ones agree on the
version, and on the reset except where an extra entry repeated `snapshot`. -/
theorem run_with_repeats (final : Entry → Bool) (snapshot : Nat) :
    ∀ (l : List Entry) (s t : Placed),
      (∀ x ∈ l, final x = false → x.boundary = .reset snapshot) →
      s.schema = t.schema → (s.reset = t.reset ∨ s.reset = some snapshot) →
      (run s l).schema = (run t (l.filter final)).schema ∧
        ((run s l).reset = (run t (l.filter final)).reset ∨ (run s l).reset = some snapshot)
  | [], _, _, _, schema, reset => ⟨schema, reset⟩
  | x :: xs, s, t, extras, schema, reset => by
      have rest : ∀ y ∈ xs, final y = false → y.boundary = .reset snapshot :=
        fun y hy => extras y (List.mem_cons_of_mem _ hy)
      cases hx : final x
      · have hb := extras x List.mem_cons_self hx
        simp only [run, List.foldl_cons, List.filter_cons, hx, Bool.false_eq_true, ite_false]
        apply run_with_repeats final snapshot xs _ t rest
        · simp only [next, hb]; split <;> simp [apply, schema]
        · right; simp only [next, hb]; split
          · rename_i h; simpa [inPlace] using h
          · simp [apply]
      · simp only [run, List.foldl_cons, List.filter_cons, hx, ite_true]
        apply run_with_repeats final snapshot xs _ _ rest
        · cases hb : x.boundary with
          | raise v snap =>
              simp only [next, hb, inPlace_raise_schema s t v snap schema]
              split <;> simp [apply, schema]
          | reset snap =>
              simp only [next, hb]
              split <;> split <;> simp [apply, schema]
        · cases hb : x.boundary with
          | raise v snap =>
              simp only [next, hb, inPlace_raise_schema s t v snap schema]
              split <;> simp [apply, reset]
          | reset snap =>
              left
              simp only [next, hb]
              split <;> split <;> rename_i h₁ h₂ <;> simp_all [inPlace, apply]

/-- **A final reset left out of the hold never becomes the boundary again.**
Replaying the kept entries puts the final ones in their order, with `F`
between `pre` and `post`. If replaying the final entries alone does not make
`F` the boundary, neither does any later replay whose kept entries ordered
before `F` and not final repeat `F` (`before_final_reset_repeats_it`). -/
theorem final_reset_never_returns (final : Entry → Bool) (pre post : List Entry) (F : Entry)
    (snapshot : Nat) (reset : F.boundary = .reset snapshot) (isFinal : final F = true)
    (extras : ∀ x ∈ pre, final x = false → x.boundary = .reset snapshot)
    (notPre : F ∉ pre) (notPost : F ∉ post)
    (finals : current ((pre ++ F :: post).filter final) ≠ some F) :
    current (pre ++ F :: post) ≠ some F := by
  intro kept
  have filtered : (pre ++ F :: post).filter final = pre.filter final ++ F :: post.filter final := by
    simp [List.filter_append, isFinal]
  rw [filtered] at finals
  have ⟨schema, resetRel⟩ := run_with_repeats final snapshot pre .empty .empty extras rfl (.inl rfl)
  unfold current at kept finals
  rw [select_append] at kept finals
  simp only [select, reset] at kept finals
  split at kept
  · obtain h := List.mem_of_getLast? kept
    rcases List.mem_append.mp h with h | h
    · exact notPre (select_sub _ _ _ h)
    · exact notPost (select_sub _ _ _ h)
  · rename_i effective
    have none : select (apply (run .empty pre) (.reset snapshot)) post = [] := by
      cases hy : select (apply (run .empty pre) (.reset snapshot)) post with
      | nil => rfl
      | cons y ys =>
          exfalso
          rw [hy, List.getLast?_append] at kept
          simp at kept
          have kept := List.mem_of_getLast? kept
          rw [← hy] at kept
          exact notPost (select_sub _ _ _ kept)
    have sameReset : (run .empty pre).reset = (run .empty (pre.filter final)).reset := by
      rcases resetRel with h | h
      · exact h
      · exfalso; apply effective; simp [inPlace, h]
    have stateEq : apply (run .empty pre) (.reset snapshot) =
        apply (run .empty (pre.filter final)) (.reset snapshot) := by
      simp [apply, schema]
    have effectiveFinal : inPlace (run .empty (pre.filter final)) (.reset snapshot) = false := by
      have : inPlace (run .empty pre) (.reset snapshot) = false := by simpa using effective
      simpa [inPlace, sameReset] using this
    rw [effectiveFinal] at finals
    simp only [Bool.false_eq_true, ite_false] at finals
    have quiet : select (apply (run .empty (pre.filter final)) (.reset snapshot))
        (post.filter final) = [] := by
      rw [← stateEq]
      apply select_of_inPlace
      intro e he
      exact select_nil _ post none e (List.mem_filter.mp he).1
    rw [quiet] at finals
    exact finals (by simp)

end CovenIO.FinalReset
