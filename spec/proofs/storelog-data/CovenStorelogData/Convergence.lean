import CovenStorelogData.Model

namespace CovenStorelogData

variable {W Col : Type} [DecidableEq W]

theorem fold_components (writes : CovenMerge.Writes W Row Col)
    (log : CovenStorelog.Log) (bound : Nat) (events : List (Event W))
    (s : State W Col) :
    events.foldl (step writes log bound) s =
      ⟨(entries events).foldl (CovenStorelog.step log bound) s.log,
       (applied events).foldl (CovenMerge.step writes) s.data⟩ := by
  induction events generalizing s with
  | nil => rfl
  | cons e es ih =>
    cases e <;> simp [List.foldl_cons, step, entries, applied, ih]

/-- Once both devices have applied the same readable writes, all entry
interleavings (including kept/dropped/re-kept outcomes) give the same rows and
loss records. This does not assert that every encrypted part can be read. -/
theorem converges {K : Type} [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (hv : CovenMerge.Valid writes) (log : CovenStorelog.Log) (bound : Nat)
    (a b : List (Event W))
    (ea : CovenStorelog.CausalOrder log (entries a))
    (eb : CovenStorelog.CausalOrder log (entries b))
    (wa : CovenMerge.CausalOrder writes (applied a))
    (wb : CovenMerge.CausalOrder writes (applied b))
    (sameEntries : ∀ e, e ∈ entries a ↔ e ∈ entries b)
    (sameWrites : ∀ w, w ∈ applied a ↔ w ∈ applied b) :
    observe schema writes log (a.foldl (step writes log bound) (initial log bound)) =
      observe schema writes log (b.foldl (step writes log bound) (initial log bound)) := by
  rw [fold_components, fold_components]
  have he := CovenStorelog.storelog_converges log bound ea eb sameEntries
  have hw := CovenMerge.merge_converges hv wa wb sameWrites
  simp only [initial] at *
  rw [he, hw]

/-- Changing only replay never destroys the underlying merged rows, cells,
generations or write-loss records, even when a deletion changes disposition. -/
theorem entry_preserves_data (writes : CovenMerge.Writes W Row Col)
    (log : CovenStorelog.Log) (bound : Nat) (s : State W Col) (e : Nat) :
    (step writes log bound s (.entry e)).data = s.data := rfl

theorem histories_converge {K : Type} [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (hv : CovenMerge.Valid writes) (log : CovenStorelog.Log) (bound : Nat)
    (authors : W → Author) {a b : List (Event W)}
    (ha : History writes log bound authors a) (hb : History writes log bound authors b)
    (sameEntries : ∀ e, e ∈ entries a ↔ e ∈ entries b)
    (sameWrites : ∀ w, w ∈ applied a ↔ w ∈ applied b) :
    observe schema writes log (a.foldl (step writes log bound) (initial log bound)) =
      observe schema writes log (b.foldl (step writes log bound) (initial log bound)) :=
  converges schema writes hv log bound a b (history_orders ha).1 (history_orders hb).1
    (history_orders ha).2 (history_orders hb).2 sameEntries sameWrites

end CovenStorelogData
