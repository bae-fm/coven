import CovenQueue.Conversion

/-! §17.1: a migrated device judges received writes under its own captures
before any raise is kept (Rust `write_boundary::WriteBoundary::judging`,
`raise_capture::provisional`). It treats each audience's capture as the raise
its peers will keep: the capture's coverage and version, with no entry yet.

Two facts make that judgment agree with every peer once a raise naming the
published capture is kept:

- `provisional_change_agrees`, `provisional_record_agrees`: a write's
  interpretation and per-change verdicts depend on the raise's coverage and
  version, never on the entry that names it. Only the whole-write verdict
  carries the entry, as a label (`provisional_verdict_agrees`).
- `judged_under_published`: the device captures again only from its current
  state, which holds every write it has judged, so a write judged under an
  earlier capture is covered by every later one. A write the published raise
  snapshot does not cover was therefore judged under that same capture.

Covered writes reach peers inside the raise snapshot, which carries the
device's own interpretation of them (its fingerprint check, §15). Uncovered
ones are judged by peers under the kept raise, exactly as the device judged
them. If another raise wins instead, the device rebuilds from its unchanged
records through the shared receive path (`reload_matches_receiver`). -/

namespace CovenQueue.Provisional

/-- A view with the capture's raise, named by entry `e`: the kept raise once
an entry naming the published capture is kept, or a provisional label. -/
def withEntry (v : View) (b : Raise) (e : Nat) : View :=
  { v with raise := some { b with entry := e } }

/-- A verdict with its raise entry label removed. -/
def Verdict.unlabeled : Verdict → Verdict
  | .excluded _ version => .excluded 0 version
  | verdict => verdict

theorem schema_unlabeled (v : View) (b : Raise) (e e' : Nat) (w : CheckedWrite) :
    Verdict.unlabeled (classify.schema (withEntry v b e) w) =
      Verdict.unlabeled (classify.schema (withEntry v b e') w) := by
  unfold classify.schema withEntry
  dsimp only
  have same : outsideRaise { b with entry := e } w.record =
      outsideRaise { b with entry := e' } w.record := rfl
  rw [same]
  by_cases outside : outsideRaise { b with entry := e' } w.record = true <;>
    simp [outside, Verdict.unlabeled]

theorem provisional_verdict_agrees (v : View) (b : Raise) (e e' : Nat) (w : CheckedWrite) :
    Verdict.unlabeled (classify (withEntry v b e) w) =
      Verdict.unlabeled (classify (withEntry v b e') w) := by
  have reset : ∀ x, (withEntry v b x).reset = v.reset := fun _ => rfl
  unfold classify
  cases w.refusal with
  | some r => rfl
  | none =>
    simp only [reset]
    cases v.reset with
    | none => exact schema_unlabeled v b e e' w
    | some boundary =>
      dsimp only
      split
      · rfl
      · exact schema_unlabeled v b e e' w

theorem applied_agrees (v : View) (b : Raise) (e e' : Nat) (w : CheckedWrite) :
    classify (withEntry v b e) w = .applied ↔ classify (withEntry v b e') w = .applied := by
  have same := provisional_verdict_agrees v b e e' w
  constructor <;> intro applied
  · rw [applied] at same
    cases h : classify (withEntry v b e') w <;> simp_all [Verdict.unlabeled]
  · rw [applied] at same
    cases h : classify (withEntry v b e) w <;> simp_all [Verdict.unlabeled]

theorem dataState_agrees (h : History) (valid : DataValid h) (v : View) (b : Raise)
    (e e' : Nat) : dataState h valid (withEntry v b e) = dataState h valid (withEntry v b e') := by
  have base : ∀ w, decide (evaluate h (withEntry v b e) w = .applied) =
      decide (evaluate h (withEntry v b e') w = .applied) := by
    intro w
    exact decide_eq_decide.mpr (applied_agrees v b e e' (h w))
  unfold dataState
  congr 1
  funext w r
  rw [base w]
  rfl

theorem freshLoss_agrees (h : History) (valid : DataValid h) (v : View) (b : Raise)
    (e e' w r : Nat) :
    freshLoss h valid (withEntry v b e) w r = freshLoss h valid (withEntry v b e') w r := by
  have same := provisional_verdict_agrees v b e e' (h w)
  have state := dataState_agrees h valid v b e e'
  have drops : snapshotLoss (withEntry v b e) w r = snapshotLoss (withEntry v b e') w r := rfl
  unfold freshLoss evaluate
  rw [state, drops]
  cases x : classify (withEntry v b e) (h w) <;>
    cases y : classify (withEntry v b e') (h w) <;>
    simp_all [Verdict.unlabeled]

/-- Every per-change verdict, losses included, is the one peers compute
under the kept raise, whatever entry names it. -/
theorem provisional_change_agrees (h : History) (valid : DataValid h) (v : View) (b : Raise)
    (e e' w : Nat) (ch : RowChange) :
    classifyChange h valid (withEntry v b e) w ch =
      classifyChange h valid (withEntry v b e') w ch := by
  have same := provisional_verdict_agrees v b e e' (h w)
  unfold classifyChange evaluate
  rw [freshLoss_agrees h valid v b e e' w ch.row]
  cases x : classify (withEntry v b e) (h w) <;>
    cases y : classify (withEntry v b e') (h w) <;>
    simp_all [Verdict.unlabeled]

theorem plan_agrees (c : Conversion) (v : View) (b : Raise) (e e' : Nat) (w : CheckedWrite) :
    interpretationPlan c (withEntry v b e) w = interpretationPlan c (withEntry v b e') w := by
  unfold interpretationPlan resetIgnores withEntry
  rfl

theorem preparedHistory_agrees (c : Conversion) (v : View) (b : Raise) (e e' : Nat) (h : History) :
    preparedHistory c (withEntry v b e) h = preparedHistory c (withEntry v b e') h := by
  funext w
  simp only [preparedHistory, prepareInterpretation, plan_agrees c v b e e']

/-- The interpreted record, converted or frozen, is the one peers make under
the kept raise. -/
theorem provisional_record_agrees (c : Conversion) (v : View) (b : Raise) (e e' : Nat)
    (h : History) (valid : DataValid h) (w : Nat) :
    (receive c (withEntry v b e) h valid w).1 = (receive c (withEntry v b e') h valid w).1 := by
  have history : ∀ {k l : History} (same : k = l) (p : DataValid k) (q : DataValid l)
      (view : View) (r : Nat), freshLoss k p view w r = freshLoss l q view w r := by
    intro k l same p q view r
    subst same
    rfl
  have losses : conversionLosses c (withEntry v b e) h valid w =
      conversionLosses c (withEntry v b e') h valid w := by
    funext r
    unfold conversionLosses
    rw [history (preparedHistory_agrees c v b e e' h) _
      (preparedValid c (withEntry v b e') h valid)]
    exact freshLoss_agrees _ _ v b e e' w r
  simp only [receive, interpret, plan_agrees c v b e e', losses]

/-! ## Captures taken again cover what was judged -/

/-- One audience on a migrated device: its current capture and each received
write with the capture it was judged under. -/
structure Judging where
  capture : Raise
  judged : List (Nat × Raise)

/-- A received write is judged under the current capture. A capture taken
again (after a catch-up, or by a later migration) is taken from the current
state, so its coverage includes every write already judged. -/
inductive Step (h : History) : Judging → Judging → Prop
  | receive (s : Judging) (w : Nat) :
      Step h s { s with judged := (w, s.capture) :: s.judged }
  | recapture (s : Judging) (next : Raise)
      (current : ∀ p ∈ s.judged, (h p.1).record.identity.id ∈ next.covers) :
      Step h s { s with capture := next }

inductive Reach (h : History) (first : Raise) : Judging → Prop
  | start : Reach h first ⟨first, []⟩
  | step {s t} : Reach h first s → Step h s t → Reach h first t

/-- Every judged write is covered by the current capture or was judged
under it. -/
theorem judged_under_published {h : History} {first : Raise} {s : Judging}
    (reach : Reach h first s) :
    ∀ p ∈ s.judged, (h p.1).record.identity.id ∈ s.capture.covers ∨ p.2 = s.capture := by
  induction reach with
  | start => simp
  | step _ step ih =>
    cases step with
    | receive w =>
      intro p member
      rcases List.mem_cons.mp member with rfl | old
      · exact .inr rfl
      · exact ih p old
    | recapture next current =>
      intro p member
      exact .inl (current p member)

/-- So once a raise naming the published capture is kept by entry `e`, every
judged write that snapshot leaves out has, change for change, the verdicts
peers compute under the kept raise. -/
theorem provisional_matches_kept {h : History} (valid : DataValid h) (v : View)
    {first : Raise} {s : Judging} (reach : Reach h first s) (e : Nat)
    (w : Nat) (judge : Raise) (member : (w, judge) ∈ s.judged)
    (uncovered : (h w).record.identity.id ∉ s.capture.covers) (ch : RowChange) :
    classifyChange h valid (withEntry v judge judge.entry) w ch =
      classifyChange h valid (withEntry v s.capture e) w ch := by
  rcases judged_under_published reach (w, judge) member with covered | same
  · exact absurd covered uncovered
  · dsimp only at same
    subst same
    exact provisional_change_agrees h valid v _ _ e w ch

end CovenQueue.Provisional
