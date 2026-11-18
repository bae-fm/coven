import SqliveNarrowing.Rules.Term
import SqliveNarrowing.Rules.Edge
import SqliveNarrowing.Rules.SubqueryEdge
import SqliveNarrowing.Rules.RowSubqueryEdge
import SqliveNarrowing.Rules.PushDown
import SqliveNarrowing.Rules.View
import SqliveNarrowing.Rules.Limit
import SqliveNarrowing.Rules.Whole

/-! The rules that read a condition, in the order sqlive tries them
(`read_condition`): the first that reads a condition gives its narrowing, and
a condition none reads narrows nothing. A rule that reads a new condition
shape adds itself here, with the theorem that it keeps a reader's promise. -/
namespace SqliveNarrowing

def conditionRules : List { read : ReadCondition // read.Sound } :=
  [⟨SubqueryEdge.readCondition, SubqueryEdge.subquery_edge_sound⟩,
   ⟨RowSubqueryEdge.readCondition, RowSubqueryEdge.row_subquery_edge_sound⟩,
   ⟨Term.readCondition, Term.term_sound⟩,
   ⟨Edge.readCondition, Edge.edge_sound⟩]

/-- The narrowing condition `cd` of core `k` gives. -/
def readCondition (s : Statement) (k : Nat) (cd : Cond) : Narrowing :=
  (conditionRules.findSome? fun read => read.val s k cd).getD {}

/-- The narrowing a condition gives keeps every rule's promise. -/
theorem readCondition_sound (s : Statement) (k : Nat) (cd : Cond) :
    (∀ i t, (i, t) ∈ (readCondition s k cd).terms → (s.item i).core = k ∧
      ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → t.holds (b i)) ∧
    (∀ e ∈ (readCondition s k cd).edges, (s.item e.member).core = k ∧
      (e.nested = false → e.partner ≠ e.member) ∧ (s.item e.partner).readable ∧
      ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → e.HoldsIn s db f b) := by
  unfold readCondition
  cases h : conditionRules.findSome? (fun read => read.val s k cd) with
  | none => simp
  | some n =>
    obtain ⟨read, _, hread⟩ := List.exists_of_findSome?_eq_some h
    exact ⟨fun i t ht => read.property.terms hread i t ht,
      fun e he => read.property.edges hread e he⟩

end SqliveNarrowing
