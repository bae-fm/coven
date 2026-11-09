import CovenMerge.ReferenceSetters
import CovenMerge.DataDependency

/-!
# Forgetting a kept record no write that can still arrive names

A device keeps each accepted change's record so that a later write naming it
as a source (§9.5) can be judged. A change is *current* while it supplies its
row's generation or sets one of the row's present cells; only a current
change can be named by a genuine write, since a write names the generation
source and the setters of the cells it replaces in its author's state.

This file proves the rule that lets a record go:

* `stopped_stays`: a change that stopped being current never becomes
  current again, whatever writes are applied after.
* `genuine_names_no_stopped`: a genuine write never names a change that
  stopped being current within the slots it read, so the stopped-source rule
  changes no genuine verdict (`stopped_rule_spares_genuine`).
* `later_reads_past`: with the checked rule that a device's `read_through`
  never goes below its own earlier landed write's, every write a device
  lands after one that read past slot `s` read past `s` too.
* `forget_preserves_judgement`: once every device that can still write has
  landed a write past the slot where a change stopped being current, every
  write that can still arrive is judged the same whether the record is kept
  or forgotten.

Judging uses two source rules (§9.5): a source that stopped being current at
a slot the write read counts as dropped, and a landed source whose record
the device doesn't hold counts as dropped.
-/

namespace CovenMerge.ForgetEdits

open CovenMerge
open CovenMerge.DataDependency (Provenance)

section Current
variable {W Row Col : Type} [DecidableEq W] (M : Writes W Row Col)

/-- A change is current while it sets a present cell or supplies its row's
generation. -/
def Current (st : St W Row Col) (x : W) : Prop :=
  (∃ r c, st.cell r c = some x) ∨ ∃ r, st.genWrite r (st.gen r) = some x

/-- No generation above a row's current one has a write recorded. -/
def GenBound (st : St W Row Col) : Prop :=
  ∀ r n, st.gen r < n → st.genWrite r n = none

theorem init_genBound : GenBound (St.init : St W Row Col) := fun _ _ _ => rfl

theorem genStep_ge (G : Nat) (o : Option (Change Col)) : G ≤ genStep G o := by
  cases o with
  | none => simp [genStep]
  | some ch =>
    simp only [genStep]
    split <;> omega

theorem minBy_cases (ts : W → Nat) (o : Option W) (w : W) :
    minBy ts o w = w ∨ ∃ y, o = some y ∧ minBy ts o w = y := by
  cases o with
  | none => exact Or.inl rfl
  | some y =>
    simp only [minBy]
    split
    · exact Or.inl rfl
    · exact Or.inr ⟨y, rfl, rfl⟩

theorem step_genBound {st : St W Row Col} (gb : GenBound st) (w : W) :
    GenBound (step M st w) := by
  intro r n above
  simp only [step] at above ⊢
  cases hch : M.chg w r with
  | none =>
    rw [hch] at above
    simp only [genStep] at above
    simp only [genWriteStep]
    exact gb r n above
  | some ch =>
    rw [hch] at above
    simp only [genStep] at above
    simp only [genWriteStep]
    split
    · rename_i moves
      exfalso
      split at above
      · omega
      · rename_i stays
        exact stays ⟨moves.1, by omega⟩
    · apply gb r n
      split at above <;> omega

/-- **A change that stopped being current never becomes current again** when
another write is applied. -/
theorem stopped_stays {st : St W Row Col} (gb : GenBound st) {x : W}
    (stopped : ¬ Current st x) (w : W) (other : w ≠ x) :
    ¬ Current (step M st w) x := by
  rintro (⟨r, c, h⟩ | ⟨r, h⟩)
  · rcases ReferenceSetters.cell_after_step M st w h with old | ⟨rfl, _⟩
    · exact stopped (Or.inl ⟨r, c, old⟩)
    · exact other rfl
  · simp only [step] at h
    have back : ∀ n, st.gen r ≤ n → st.genWrite r n = some x → False := by
      intro n le held
      rcases Nat.lt_or_eq_of_le le with lt | eq
      · rw [gb r n lt] at held
        cases held
      · subst eq
        exact stopped (Or.inr ⟨r, held⟩)
    have ge := genStep_ge (st.gen r) (M.chg w r)
    cases hch : M.chg w r with
    | none =>
      rw [hch] at h ge
      exact back _ ge h
    | some ch =>
      rw [hch] at h ge
      simp only [genWriteStep] at h
      split at h
      · rcases minBy_cases M.ts (st.genWrite r (genStep (st.gen r) (some ch))) w with
          picked | ⟨y, held, picked⟩
        · rw [picked] at h
          exact other (Option.some.inj h)
        · rw [picked] at h
          cases h
          exact back _ ge held
      · exact back _ ge h

/-- Applying writes in order. -/
def run (st : St W Row Col) (ws : List W) : St W Row Col := ws.foldl (step M) st

theorem run_genBound {st : St W Row Col} (gb : GenBound st) (ws : List W) :
    GenBound (run M st ws) := by
  induction ws generalizing st with
  | nil => exact gb
  | cons w ws ih => exact ih (step_genBound M gb w)

theorem stopped_stays_run {st : St W Row Col} (gb : GenBound st) {x : W}
    (stopped : ¬ Current st x) (ws : List W) (absent : x ∉ ws) :
    ¬ Current (run M st ws) x := by
  induction ws generalizing st with
  | nil => exact stopped
  | cons w ws ih =>
    have ne : w ≠ x := fun h => absent (h ▸ List.mem_cons_self)
    exact ih (step_genBound M gb w) (stopped_stays M gb stopped w ne)
      (fun h => absent (List.mem_cons_of_mem w h))

end Current

section Authoring
variable {W Row Col : Type} [DecidableEq W] (M : Writes W Row Col)

/-- A write authored in state `st` names, for row `r`, the row's generation
source and the setters of the cells it replaces there (§9.5). -/
def NamesCurrent (st : St W Row Col) (r : Row) (p : Provenance W Col) : Prop :=
  (∀ a, p.generation = some a → st.genWrite r (st.gen r) = some a) ∧
    ∀ c a, (c, a) ∈ p.setters → st.cell r c = some a

theorem named_current {st : St W Row Col} {r : Row} {p : Provenance W Col}
    (h : NamesCurrent st r p) : ∀ a ∈ p.sources, Current st a := by
  intro a ha
  simp only [Provenance.sources, List.mem_append, List.mem_map] at ha
  rcases ha with gen | ⟨⟨c, b⟩, mem, rfl⟩
  · exact Or.inr ⟨r, h.1 a (Option.mem_toList.mp gen)⟩
  · exact Or.inl ⟨r, c, h.2 c b mem⟩

/-- `x` landed among the first `s` writes of the canonical order `log` and
had stopped being current once they were applied. -/
def StoppedBy (log : List W) (x : W) (s : Nat) : Prop :=
  x ∈ log.take s ∧ ¬ Current (run M St.init (log.take s)) x

/-- **A genuine write never names a change that stopped being current within
the slots it read.** Its author's state is the canonical one through its
`read_through`, `rt`, with its own writes not yet applied there, `queued`,
applied on top. -/
theorem genuine_names_no_stopped {log : List W} (unique : log.Nodup) {x : W} {s rt : Nat}
    (stop : StoppedBy M log x s) (read : s ≤ rt) {queued : List W}
    (own : ∀ a ∈ queued, a ∉ log.take rt) {r : Row} {p : Provenance W Col}
    (authored : NamesCurrent (run M (run M St.init (log.take rt)) queued) r p) :
    x ∉ p.sources := by
  intro named
  have split : log.take rt = log.take s ++ (log.drop s).take (rt - s) := by
    have := List.take_add (l := log) (i := s) (j := rt - s)
    rwa [Nat.add_sub_cancel' read] at this
  have disjoint : x ∉ (log.drop s).take (rt - s) := by
    intro later
    have nd : (log.take s ++ log.drop s).Nodup := by rwa [List.take_append_drop]
    exact (List.nodup_append.mp nd).2.2 x stop.1 x (List.mem_of_mem_take later) rfl
  have at_rt : ¬ Current (run M St.init (log.take rt)) x := by
    rw [split]
    simp only [run, List.foldl_append]
    exact stopped_stays_run M (run_genBound M init_genBound _) stop.2 _ disjoint
  have landed : x ∈ log.take rt := by
    rw [split]
    exact List.mem_append_left _ stop.1
  have after : ¬ Current (run M (run M St.init (log.take rt)) queued) x :=
    stopped_stays_run M (run_genBound M init_genBound _) at_rt queued
      (fun q => own x q landed)
  exact after (named_current authored x named)

end Authoring

section Judging
variable {W Col : Type}

theorem all_congr_mem {α : Type} {l : List α} {f g : α → Bool}
    (h : ∀ a ∈ l, f a = g a) : l.all f = l.all g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.all_cons, h x List.mem_cons_self,
      ih (fun a ha => h a (List.mem_cons_of_mem x ha))]

/-- What a device keeps of a change: whether its verdict lets it support
others, and the slot it stopped being current at, if it did. -/
structure Kept where
  retained : Bool
  stop : Option Nat

/-- Whether a source counts for a write that read through `rt`. A landed
source with no kept record was forgotten and counts as dropped; one that
stopped being current at a slot the write read counts as dropped (§9.5). -/
def counts (view : W → Option Kept) (rt : Nat) (a : W) : Bool :=
  match view a with
  | none => false
  | some kept => kept.retained && kept.stop.all (fun s => rt < s)

/-- The source rule before stopped sources counted as dropped. -/
def countsBefore (view : W → Option Kept) (a : W) : Bool :=
  match view a with
  | none => false
  | some kept => kept.retained

/-- A change stands when it is admitted and every source it names counts:
the shape of `DataDependency.retained_iff`. -/
def decision (view : W → Option Kept) (rt : Nat) (admitted : Bool)
    (p : Provenance W Col) : Bool :=
  admitted && p.sources.all (counts view rt)

def decisionBefore (view : W → Option Kept) (admitted : Bool)
    (p : Provenance W Col) : Bool :=
  admitted && p.sources.all (countsBefore view)

/-- The view after forgetting the records `F` names. -/
def forget (view : W → Option Kept) (F : W → Bool) : W → Option Kept :=
  fun a => if F a then none else view a

theorem counts_forget (view : W → Option Kept) (F : W → Bool) (rt : Nat)
    (past : ∀ a, F a = true → ∃ kept s, view a = some kept ∧ kept.stop = some s ∧ s ≤ rt)
    (a : W) : counts (forget view F) rt a = counts view rt a := by
  simp only [counts, forget]
  cases hf : F a with
  | false => rfl
  | true =>
    obtain ⟨kept, s, held, stop, le⟩ := past a hf
    simp only [held, stop, Option.all_some]
    have : ¬ rt < s := by omega
    simp [this]

theorem decision_forget (view : W → Option Kept) (F : W → Bool) (rt : Nat)
    (past : ∀ a, F a = true → ∃ kept s, view a = some kept ∧ kept.stop = some s ∧ s ≤ rt)
    (admitted : Bool) (p : Provenance W Col) :
    decision (forget view F) rt admitted p = decision view rt admitted p := by
  simp only [decision]
  rw [List.all_congr rfl (counts_forget view F rt past)]

end Judging

section Sequence
variable {D : Type}

/-- The writes of a slot order, each as its device and `read_through`, pass
the check every device makes on receipt: no write's `read_through` is below
an earlier landed write's of its own device. -/
def ReadsChecked (log : List (D × Nat)) : Prop :=
  log.Pairwise (fun e e' => e.1 = e'.1 → e.2 ≤ e'.2)

/-- Receiving a write that passes the check keeps the order checked. -/
theorem receive_checked {log : List (D × Nat)} (checked : ReadsChecked log) (x : D × Nat)
    (check : ∀ e ∈ log, e.1 = x.1 → e.2 ≤ x.2) : ReadsChecked (log ++ [x]) := by
  unfold ReadsChecked
  rw [List.pairwise_append]
  refine ⟨checked, List.pairwise_singleton _ _, ?_⟩
  intro a ha b hb
  rw [List.mem_singleton] at hb
  subst hb
  exact check a ha

/-- **A write a device lands after one that read past `s` read past `s`.** -/
theorem later_reads_past {before after : List (D × Nat)}
    (checked : ReadsChecked (before ++ after)) {e x : D × Nat}
    (earlier : e ∈ before) (later : x ∈ after) (same : e.1 = x.1) {s : Nat}
    (past : s ≤ e.2) : s ≤ x.2 := by
  unfold ReadsChecked at checked
  rw [List.pairwise_append] at checked
  exact Nat.le_trans past (checked.2.2 e earlier x later same)

/-- Device `d` has landed, among `before`, a write that read through `s`. -/
def Passed (before : List (D × Nat)) (d : D) (s : Nat) : Prop :=
  ∃ e ∈ before, e.1 = d ∧ s ≤ e.2

end Sequence

section Forget
variable {W Col D : Type}

/-- **Every write that can still arrive is judged the same with the
forgotten records gone.** The device forgot each record in `F` once the
change had stopped being current at its slot `s` and the writing device of
`x` had passed `s` among the writes landed by then (`before`), or `x` read
past `s` on its own: a device added after `s` writes with `read_through` at
or past its registration, which authorizes it (§7.1). A gone device's later
writes are cut off and never judged (§14.1). The order of landed writes
passes the receipt check (`ReadsChecked`). -/
theorem forget_preserves_judgement (view : W → Option Kept) (F : W → Bool)
    {before after : List (D × Nat)} (checked : ReadsChecked (before ++ after))
    {x : D × Nat} (arrives : x ∈ after)
    (forgotten : ∀ a, F a = true → ∃ kept s, view a = some kept ∧ kept.stop = some s ∧
      (Passed before x.1 s ∨ s ≤ x.2))
    (admitted : Bool) (p : Provenance W Col) :
    decision (forget view F) x.2 admitted p = decision view x.2 admitted p := by
  apply decision_forget
  intro a fa
  obtain ⟨kept, s, held, stop, passed⟩ := forgotten a fa
  refine ⟨kept, s, held, stop, ?_⟩
  rcases passed with ⟨e, earlier, same, past⟩ | own
  · exact later_reads_past checked earlier arrives same past
  · exact own

end Forget

section Genuine
variable {W Row Col : Type} [DecidableEq W] (M : Writes W Row Col)

/-- A view keeps stop slots faithfully: a record that says it stopped at
`s` names a change that landed among the first `s` writes and was no longer
current once they were applied. -/
def Faithful (log : List W) (view : W → Option Kept) : Prop :=
  ∀ a kept s, view a = some kept → kept.stop = some s → StoppedBy M log a s

/-- **The stopped-source rule changes no genuine verdict.** A genuine write
names only current changes, so no source it names stopped within the slots
it read. -/
theorem stopped_rule_spares_genuine {log : List W} (unique : log.Nodup)
    {view : W → Option Kept} (faithful : Faithful M log view) {rt : Nat}
    {queued : List W} (own : ∀ a ∈ queued, a ∉ log.take rt) {r : Row}
    {p : Provenance W Col}
    (authored : NamesCurrent (run M (run M St.init (log.take rt)) queued) r p)
    (admitted : Bool) :
    decision view rt admitted p = decisionBefore view admitted p := by
  simp only [decision, decisionBefore]
  congr 1
  apply all_congr_mem
  intro a named
  simp only [counts, countsBefore]
  cases held : view a with
  | none => rfl
  | some kept =>
    cases stop : kept.stop with
    | none => simp [stop]
    | some s =>
      by_cases read : s ≤ rt
      · exact absurd named
          (genuine_names_no_stopped M unique (faithful a kept s held stop) read own authored)
      · have : rt < s := by omega
        simp [stop, this]

end Genuine

end CovenMerge.ForgetEdits
