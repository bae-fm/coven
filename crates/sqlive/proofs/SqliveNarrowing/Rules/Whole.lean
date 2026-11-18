import SqliveNarrowing.Reader

/-! The fallback: a statement holding a shape sqlive can't read narrows
nothing, and every table it reads depends on every row; so does a table it
reads but not as one of its items (`Sources::read`, `read_capture.rs`). -/
namespace SqliveNarrowing.Whole

/-- The shapes sqlive reads whole today. -/
def shapes : List WholeShape :=
  [.using, .natural, .right, .recursive, .inTable, .virtual, .parenthesized, .repeatedCte]

/-- Whether sqlive reads the statement whole. -/
def readsWhole (s : Statement) : Bool := s.whole.any (shapes.contains ·)

/-- The fallback's theorem: with no scope, every row change that inserts or
deletes a row of a table the statement reads, or changes a column it reads,
reruns the statement. -/
theorem whole_sound {s : Statement} {reads : List (Nat × List Nat)} {changes : List Change}
    {db' : Db} {ch : Change} (hch : ch ∈ changes) {read : Nat × List Nat}
    (hread : reads.find? (·.1 == ch.table) = some read) (hchanged : changedIn read.2 ch.old ch.new) :
    reruns s (fun _ => none) reads changes db' = true := by
  unfold reruns
  simp only [List.any_eq_true]
  refine ⟨ch, hch, ?_⟩
  rw [hread]
  simp only [hchanged, Option.getD_none, Bool.true_and]
  cases hold : ch.old <;> cases hnew : ch.new <;> simp_all [changedIn, RScope.passes]

end SqliveNarrowing.Whole
