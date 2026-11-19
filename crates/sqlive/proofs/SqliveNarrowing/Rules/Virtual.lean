import SqliveNarrowing.Changes

/-! A contentful FTS5 table is a table item of the model, every
column of it of no affinity (BLOB), compared by BINARY (`fts5_columns`). Its
content table, an ordinary table, holds its rows: the rowid in `id`, then
each column in its place, so sqlive records each of the content table's row
changes as the same change of the FTS5 table (`Shadow::Rows`). FTS5
rewrites a row an UPDATE names: it deletes the content table's row and
inserts it again. sqlive folds the two into the row's net change, one update
of the columns that changed (`Net.lean`). `rewrite_reruns` shows the two
changes rerun a statement whenever the update as one change would, so a
judge of each change would be sound too. A statement that names a hidden column,
`rank` or the one that bears the table's name, as MATCH and `bm25` do, and
one that reads any other virtual table, reads it whole (`WholeShape.virtual`).
-/
namespace SqliveNarrowing.Virtual

/-- The theorem: a row's update, reported as its delete and its insert,
reruns a statement wherever the update would. -/
theorem rewrite_reruns {s : Statement} {scopeOf : Nat → Option RScope}
    {reads : List (Nat × List Nat)} {db' : Db} {t : Nat} {a b : Row}
    (h : reruns s scopeOf reads [⟨t, some a, some b⟩] db' = true) :
    reruns s scopeOf reads [⟨t, some a, none⟩, ⟨t, none, some b⟩] db' = true := by
  unfold reruns at h ⊢
  simp only [List.any_cons, List.any_nil, Bool.or_false] at h ⊢
  split at h
  · cases h
  · rename_i read _
    simp only [changedIn, Option.any_some, Option.any_none, Bool.true_and, Bool.or_false,
      Bool.false_or, Bool.and_eq_true, Bool.or_eq_true] at h ⊢
    rcases h.2 with h | h
    · exact Or.inl h
    · exact Or.inr h

end SqliveNarrowing.Virtual
