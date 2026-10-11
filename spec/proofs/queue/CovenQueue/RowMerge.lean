import CovenQueue.Verdicts

/-! The queue's row decisions feed the existing row merge. Original write
validity and authenticated author provenance are inputs; retained generation
witnesses are derived, never required of the filtered history. -/
namespace CovenQueue

open CovenMerge

def rowMetadata (ch : RowChange) : Change Nat :=
  ⟨ch.operation, ch.generation, fun c => ch.operation != .del && c < ch.values.length⟩

def rowWrites (h : History) : Writes Nat Nat Nat where
  ts w := (h w).record.identity.timestamp
  past w a := reads (h w).record.identity (h a).record.identity
  chg w r := (rowChange h w r).map rowMetadata

/-- Only applied changes enter the row merge. Other changes retain their
metadata in the checked history. -/
def mergeInput (h : History) (valid : DataValid h) (v : View) : Writes Nat Nat Nat :=
  { rowWrites h with chg := fun w r => (rowChange h w r).bind fun ch =>
      if classifyChange h valid v w ch = .applied then some (rowMetadata ch) else none }

theorem change_applied_iff (h : History) (valid : DataValid h) (v : View)
    (w r : Nat) (ch : RowChange) (found : rowChange h w r = some ch) :
    classifyChange h valid v w ch = .applied ↔
      DataDependency.retained (dataState h valid v) w r = true := by
  have row : ch.row = r := by simpa using List.find?_some found
  cases answer : evaluate h v w with
  | ignored e | refused reason | excluded e version =>
    have dropped := DataDependency.dropped_stays_dropped (dataState h valid v) w r
      (by simp [dataState, answer])
    cases content : ch.content <;>
      simp [classifyChange, freshLoss, preserveFrozen, answer, content, dropped]
  | applied =>
    cases content : ch.content with
    | frozen cause values =>
      have dropped := DataDependency.dropped_stays_dropped (dataState h valid v) w r
        (by simp [dataState, found, RowChange.frozen, content])
      simp [classifyChange, preserveFrozen, answer, content, dropped]
    | values values =>
      cases saved : snapshotLoss v w r with
      | none =>
        simp only [classifyChange, answer, preserveFrozen, content, freshLoss, row, saved]
        cases kept : DataDependency.retained (dataState h valid v) w r <;> simp
      | some cause =>
        have dropped := DataDependency.dropped_stays_dropped (dataState h valid v) w r
          (by simp [dataState, saved])
        simp [classifyChange, freshLoss, preserveFrozen, answer, content, row, saved, dropped]

theorem merge_retention (h : History) (valid : DataValid h) (v : View)
    (original : Valid (rowWrites h)) (p : DataDependency.Authored (rowWrites h))
    (sources : p.provenance = provenance h) (w r : Nat) :
    DataDependency.retained (DataDependency.authoredState (rowWrites h) original p
      (dataState h valid v).base) w r = DataDependency.retained (dataState h valid v) w r := by
  apply DataDependency.retained_congr
  · funext w r
    simp [DataDependency.authoredState, dataState, rowWrites]
  · rfl
  · intro w r
    simp [DataDependency.authoredState, dataState, sources]

theorem mergeInput_effective (h : History) (valid : DataValid h) (v : View)
    (original : Valid (rowWrites h)) (p : DataDependency.Authored (rowWrites h))
    (sources : p.provenance = provenance h) :
    mergeInput h valid v = DataDependency.effective (rowWrites h) original p (dataState h valid v).base := by
  have changes : (mergeInput h valid v).chg =
      (DataDependency.effective (rowWrites h) original p (dataState h valid v).base).chg := by
    funext w r
    change ((rowChange h w r).bind fun ch =>
      if classifyChange h valid v w ch = .applied then some (rowMetadata ch) else none) =
      (if DataDependency.retained (DataDependency.authoredState (rowWrites h) original p
        (dataState h valid v).base) w r then (rowChange h w r).map rowMetadata else none)
    rw [merge_retention h valid v original p sources]
    cases found : rowChange h w r with
    | none => split <;> rfl
    | some ch =>
      simp only [Option.bind_some, Option.map_some, change_applied_iff h valid v w r ch found]
  exact congrArg (fun chg => { rowWrites h with chg := chg }) changes

theorem mergeInput_valid (h : History) (valid : DataValid h) (v : View)
    (original : Valid (rowWrites h)) (p : DataDependency.Authored (rowWrites h))
    (sources : p.provenance = provenance h) : Valid (mergeInput h valid v) := by
  rw [mergeInput_effective h valid v original p sources]
  exact DataDependency.effective_valid _ original p _

def mergeRows (h : History) (valid : DataValid h) (v : View)
    (snapshot : St Nat Nat Nat) (received : List Nat) : St Nat Nat Nat :=
  received.foldl (step (mergeInput h valid v)) snapshot

theorem rows_and_losses_converge (h : History) (valid : DataValid h) (v : View)
    (original : Valid (rowWrites h)) (p : DataDependency.Authored (rowWrites h))
    (sources : p.provenance = provenance h) (C : Nat → Prop)
    (closed : Closed (rowWrites h) C) (snapshot : St Nat Nat Nat)
    (correct : IsSpec (mergeInput h valid v) C snapshot)
    {a b : List Nat} (ca : CausalFrom (rowWrites h) C a) (cb : CausalFrom (rowWrites h) C b)
    (same : ∀ w, w ∈ a ↔ w ∈ b) :
    (mergeRows h valid v snapshot a, materializeChanges h valid v a) =
      (mergeRows h valid v snapshot b, materializeChanges h valid v b) := by
  have eq := mergeInput_effective h valid v original p sources
  have rows : mergeRows h valid v snapshot a = mergeRows h valid v snapshot b := by
    simp only [mergeRows, eq]
    exact Data.converges _ original p _ C closed snapshot (by rwa [← eq]) ca cb same
  exact Prod.ext rows (changes_converge h valid v a b same)

/-- Every original setter either has an ordinary merge outcome or its entire
original row change is frozen, with its values and provenance unchanged. -/
theorem row_value_accounted {K : Type} [DecidableEq K]
    (h : History) (valid : DataValid h) (v : View)
    (original : Valid (rowWrites h)) (p : DataDependency.Authored (rowWrites h))
    (sources : p.provenance = provenance h) (C : Nat → Prop)
    (closed : Closed (rowWrites h) C) (snapshot : St Nat Nat Nat)
    (correct : IsSpec (mergeInput h valid v) C snapshot)
    (received : List Nat) (causal : CausalFrom (rowWrites h) C received)
    (I : EntryInputs Nat K)
    (present : ∀ r, I.present r = decide ((mergeRows h valid v snapshot received).gen r % 2 = 1))
    (w r c k : Nat) (mem : w ∈ received)
    (setter : Setter (rowWrites h) (fun a => C a ∨ a ∈ received) r c w k)
    (ordinary : evaluate h v w = .applied ∨ ∃ e version, evaluate h v w = .excluded e version) :
    ValueAccounted (mergeInput h valid v) (fun a => C a ∨ a ∈ received)
      (mergeRows h valid v snapshot received) I r c w k ∨
    ∃ ch cause, rowChange h w r = some ch ∧
      (ch, .frozen cause) ∈ materializeChanges h valid v received w := by
  obtain ⟨hw, metadata, change, kind, sets, inc⟩ := setter
  obtain ⟨ch, found, rfl⟩ := Option.map_eq_some_iff.mp change
  have inBody := List.mem_of_find?_eq_some found
  rcases change_value_accounted h valid v w ch inBody ordinary with applied | ⟨cause, frozen⟩
  · have answer : classifyChange h valid v w ch = .applied := by
      obtain ⟨other, _, pair⟩ := List.mem_map.mp applied
      have eq : other = ch := congrArg Prod.fst pair
      subst other
      exact congrArg Prod.snd pair
    have input := mergeInput_valid h valid v original p sources
    have eq := mergeInput_effective h valid v original p sources
    have causal' : CausalFrom (mergeInput h valid v) C received := by
      rw [eq]; exact DataDependency.causal_from _ original p _ causal
    have spec := foldl_isSpec input closed correct causal'
    apply Or.inl
    apply CovenMerge.value_accounted input spec.2 spec.1 I present
    exact ⟨hw, rowMetadata ch, by simp [mergeInput, found, answer], kind, sets, inc⟩
  · exact Or.inr ⟨ch, cause, found, by simpa [materializeChanges, mem] using frozen⟩

end CovenQueue
