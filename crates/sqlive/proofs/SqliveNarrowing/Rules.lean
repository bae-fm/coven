import SqliveNarrowing.Rules.Term
import SqliveNarrowing.Rules.CteNames
import SqliveNarrowing.Rules.RowMember
import SqliveNarrowing.Rules.Edge
import SqliveNarrowing.Rules.RowEdge
import SqliveNarrowing.Rules.RowOrder
import SqliveNarrowing.Rules.IsEdge
import SqliveNarrowing.Rules.Or
import SqliveNarrowing.Rules.OneOf
import SqliveNarrowing.Rules.SubqueryEdge
import SqliveNarrowing.Rules.RowSubqueryEdge
import SqliveNarrowing.Rules.PushDown
import SqliveNarrowing.Rules.Transfer
import SqliveNarrowing.Rules.View
import SqliveNarrowing.Rules.Limit
import SqliveNarrowing.Rules.InnerLimit
import SqliveNarrowing.Rules.Window
import SqliveNarrowing.Rules.Whole
import SqliveNarrowing.Rules.Parenthesized
import SqliveNarrowing.Rules.Virtual

/-! The rules that read a condition, in the order sqlive tries them
(`read_condition`): the first that reads a condition gives its narrowing, and
a condition none reads narrows nothing. A rule that reads a new condition
shape adds itself here, with the theorem that it keeps a reader's promise.
An OR that none of them reads is read last, its sides by these rules
(`Or.readCondition`). -/
namespace SqliveNarrowing

def conditionRules : List { read : ReadCondition // read.Sound } :=
  [⟨SubqueryEdge.readCondition, SubqueryEdge.subquery_edge_sound⟩,
   ⟨RowSubqueryEdge.readCondition, RowSubqueryEdge.row_subquery_edge_sound⟩,
   ⟨Term.readCondition, Term.term_sound⟩,
   ⟨Edge.readCondition, Edge.edge_sound⟩,
   ⟨RowEdge.readCondition, RowEdge.row_edge_sound⟩,
   ⟨IsEdge.readCondition, IsEdge.is_edge_sound⟩]

/-- The narrowing the first of `conditionRules` that reads `cd` gives. -/
def readRules : ReadCondition := fun s k cd =>
  conditionRules.findSome? fun read => read.val s k cd

/-- The rules together keep a reader's promise. -/
theorem readRules_sound : ReadCondition.Sound readRules where
  terms := by
    intro s k cd n h i t ht
    obtain ⟨read, _, hread⟩ := List.exists_of_findSome?_eq_some h
    exact read.property.terms hread i t ht
  edges := by
    intro s k cd n h e he
    obtain ⟨read, _, hread⟩ := List.exists_of_findSome?_eq_some h
    exact read.property.edges hread e he

/-- The narrowing condition `cd` of core `k` gives as a whole: a column equal
to one of several values (`OneOf`) first, then the rules, then an OR none of
them reads. -/
def readWhole (s : Statement) (k : Nat) (cd : Cond) : Narrowing :=
  match OneOf.readCondition s k cd with
  | some n => n
  | none =>
    match readRules s k cd with
    | some n => { n with ors := [] }
    | none => (Or.readCondition readRules s k cd).getD {}

/-- The narrowing condition `cd` of core `k` gives: what it gives as a whole,
and the terms a row value ordering's first pair gives (`RowOrder`). -/
def readCondition (s : Statement) (k : Nat) (cd : Cond) : Narrowing :=
  let whole := readWhole s k cd
  { whole with terms := whole.terms ++ RowOrder.terms s k cd }

/-- The narrowing a condition gives as a whole keeps every rule's promise. -/
theorem readWhole_sound (s : Statement) (k : Nat) (cd : Cond) :
    (∀ i t, (i, t) ∈ (readWhole s k cd).terms → (s.item i).core = k ∧
      ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → t.holds (b i)) ∧
    (∀ e ∈ (readWhole s k cd).edges, (s.item e.member).core = k ∧
      (e.nested = false → e.partner ≠ e.member) ∧ (e.nested = false → e.partner ∈ cd.items) ∧
      (s.item e.partner).readable ∧ e.memberStrict s ∧ e.NestedIn s cd.queries ∧
      ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → e.HoldsIn s db f b) ∧
    (∀ d ∈ (readWhole s k cd).ors, (s.item d.item).core = k ∧
      (∀ side ∈ d.sides, side.narrows = true) ∧
      (∀ side ∈ d.sides, ∀ e ∈ side.edges, e.member = d.item ∧
        (e.nested = false → e.partner ≠ e.member) ∧ (e.nested = false → e.partner ∈ cd.items) ∧
        (s.item e.partner).readable ∧ e.memberStrict s ∧ e.NestedIn s cd.queries) ∧
      ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → d.HoldsIn s db f b) := by
  have disjoins : ∀ {read : ReadCondition}, read.Disjoins → ∀ {n}, read s k cd = some n →
      (∀ i t, (i, t) ∈ n.terms → (s.item i).core = k ∧
        ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → t.holds (b i)) ∧
      (∀ e ∈ n.edges, (s.item e.member).core = k ∧
        (e.nested = false → e.partner ≠ e.member) ∧ (e.nested = false → e.partner ∈ cd.items) ∧
        (s.item e.partner).readable ∧ e.memberStrict s ∧ e.NestedIn s cd.queries ∧
        ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → e.HoldsIn s db f b) ∧
      (∀ d ∈ n.ors, (s.item d.item).core = k ∧
        (∀ side ∈ d.sides, side.narrows = true) ∧
        (∀ side ∈ d.sides, ∀ e ∈ side.edges, e.member = d.item ∧
          (e.nested = false → e.partner ≠ e.member) ∧ (e.nested = false → e.partner ∈ cd.items) ∧
          (s.item e.partner).readable ∧ e.memberStrict s ∧ e.NestedIn s cd.queries) ∧
        ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → d.HoldsIn s db f b) := by
    intro read hread n hn
    obtain ⟨hterms, hedges, hors⟩ := hread hn
    refine ⟨fun i t ht => ?_, fun e he => ?_, hors⟩
    · rw [hterms] at ht; cases ht
    · rw [hedges] at he; cases he
  unfold readWhole
  cases h1 : OneOf.readCondition s k cd with
  | some n => exact disjoins OneOf.one_of_sound h1
  | none =>
    cases h : readRules s k cd with
    | some n =>
      exact ⟨fun i t ht => readRules_sound.terms h i t ht,
        fun e he => readRules_sound.edges h e he, fun d hd => by simp at hd⟩
    | none =>
      cases ho : Or.readCondition readRules s k cd with
      | none => simp
      | some n => exact disjoins (Or.or_sound readRules_sound) ho

/-- The narrowing a condition gives keeps every rule's promise. -/
theorem readCondition_sound (s : Statement) (k : Nat) (cd : Cond) :
    (∀ i t, (i, t) ∈ (readCondition s k cd).terms → (s.item i).core = k ∧
      ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → t.holds (b i)) ∧
    (∀ e ∈ (readCondition s k cd).edges, (s.item e.member).core = k ∧
      (e.nested = false → e.partner ≠ e.member) ∧ (e.nested = false → e.partner ∈ cd.items) ∧
      (s.item e.partner).readable ∧ e.memberStrict s ∧ e.NestedIn s cd.queries ∧
      ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → e.HoldsIn s db f b) ∧
    (∀ d ∈ (readCondition s k cd).ors, (s.item d.item).core = k ∧
      (∀ side ∈ d.sides, side.narrows = true) ∧
      (∀ side ∈ d.sides, ∀ e ∈ side.edges, e.member = d.item ∧
        (e.nested = false → e.partner ≠ e.member) ∧ (e.nested = false → e.partner ∈ cd.items) ∧
        (s.item e.partner).readable ∧ e.memberStrict s ∧ e.NestedIn s cd.queries) ∧
      ∀ db f b, isTrue (cd.eval b (evalQuery s db f b)) → d.HoldsIn s db f b) := by
  obtain ⟨hterms, hedges, hors⟩ := readWhole_sound s k cd
  refine ⟨fun i t ht => ?_, hedges, hors⟩
  rcases List.mem_append.mp ht with ht | ht
  · exact hterms i t ht
  · exact RowOrder.terms_sound s k cd i t ht

end SqliveNarrowing
