import CovenSequence.Retention
import CovenSequence.ReaderExamples

/-! Preparation announcements affect the coverage a later operation must
capture, but supply no ordinary snapshot and do not apply rows. A boundary
entry must match an earlier counted preparation before it can load its body. -/
namespace CovenSequence.Preparations

variable {S : Sig} (R : Rules S)

/-- Eligibility for coverage comes from an ordinary announcement or an applied
boundary, never a preparation announcement by itself. -/
def Usable (seq : Nat → Slot S) (A : S.Aud) (head c : Nat) (p : S.Path) : Prop :=
  (∃ a d, 1 ≤ a ∧ a ≤ head ∧ seq a = .snapshot d A c p ∧ AppliedTo R seq A a) ∨
  (∃ r d kind, 1 ≤ r ∧ r ≤ head ∧ seq r = .boundary d A kind c p ∧ AppliedTo R seq A r)

theorem activated_is_usable (seq : Nat → Slot S) (A : S.Aud) (head r : Nat)
    (d : S.Dev) (kind : Boundary) (c : Nat) (p : S.Path) (positive : 1 ≤ r) (le : r ≤ head)
    (slot : seq r = .boundary d A kind c p) (applied : AppliedTo R seq A r) :
    Usable R seq A head c p := Or.inr ⟨r, d, kind, positive, le, slot, applied⟩

/-- A successful announcement step has its prepared body already in storage. -/
theorem announcement_uploaded {seq : Nat → Slot S} {st st' : Store S}
    (step : StoreStep R seq st st') (advances : st.head < st'.head)
    (A : S.Aud) (d : S.Dev) (kind : Boundary) (c : Nat) (p : S.Path)
    (slot : seq (st.head + 1) = .prepare d A kind c p)
    (applied : AppliedTo R seq A (st.head + 1)) :
    ∃ L P, st.body p = some (L, P, preparedRows R kind p (rowsAt R seq A c)) := by
  cases step with
  | claim prepared _ => exact prepared A d kind c p slot applied
  | _ => simp at advances

theorem ordinary_covers_boundary (seq : Nat → Slot S) (A : S.Aud)
    {r a : Nat} {d e : S.Dev} {kind : Boundary} {c f : Nat} {p q : S.Path}
    (positive : 1 ≤ r) (before : r < a)
    (boundary : seq r = .boundary d A kind c p) (applied : AppliedTo R seq A r)
    (snapshot : seq a = .snapshot e A f q) (announced : AppliedTo R seq A a) : r ≤ f := by
  have last := boundary_lastBoundary R seq A positive boundary applied
  have mono := lastBoundary_mono R seq A r (a - 1) (by omega)
  have valid := announced.2
  rw [snapshot] at valid
  simp only [audValid, Bool.and_eq_true, decide_eq_true_eq] at valid
  omega

theorem step_keeps_schema_reset (L : Ledger S) (P : AudLedger S) (k : Nat)
    (A : S.Aud) (x : Slot S) (before : (audStep R L P k A x).lastBoundary < k) :
    (audStep R L P k A x).version = P.version ∧
    (audStep R L P k A x).lastReset = P.lastReset := by
  unfold audStep at before ⊢
  split
  · split
    · cases x with
      | writes d g parts => simp only [audEffect]; split <;> exact ⟨rfl, rfl⟩
      | boundary d a kind c p => simp_all [audEffect]
      | _ => exact ⟨rfl, rfl⟩
    · exact ⟨rfl, rfl⟩
  · exact ⟨rfl, rfl⟩

theorem schema_reset_at_coverage (seq : Nat → Slot S) (A : S.Aud) (c : Nat) :
    ∀ k, c ≤ k → (audAt R seq A k).lastBoundary ≤ c →
      (audAt R seq A k).version = (audAt R seq A c).version ∧
      (audAt R seq A k).lastReset = (audAt R seq A c).lastReset
  | 0, le, _ => by
    have : c = 0 := by omega
    subst c
    exact ⟨rfl, rfl⟩
  | k + 1, le, boundary => by
    by_cases same : c = k + 1
    · subst c; exact ⟨rfl, rfl⟩
    · have old := lastBoundary_mono R seq A k (k + 1) (by omega)
      have ih := schema_reset_at_coverage seq A c k (by omega) (by omega)
      have step := step_keeps_schema_reset R (ledgerAt R seq k) (audAt R seq A k)
        (k + 1) A (seq (k + 1)) (by change (audAt R seq A (k + 1)).lastBoundary < k + 1; omega)
      exact ⟨step.1.trans ih.1, step.2.trans ih.2⟩

/-- A valid ordinary snapshot's schema and reset are those at announcement,
even when the capture and announcement have different sequence positions. -/
theorem ordinary_matches_schema_reset (seq : Nat → Slot S) (A : S.Aud)
    (a c : Nat) (d : S.Dev) (p : S.Path) (slot : seq a = .snapshot d A c p)
    (applied : AppliedTo R seq A a) :
    (audAt R seq A (a - 1)).version = (audAt R seq A c).version ∧
    (audAt R seq A (a - 1)).lastReset = (audAt R seq A c).lastReset := by
  have valid := applied.2
  rw [slot] at valid
  simp only [audValid, Bool.and_eq_true, decide_eq_true_eq] at valid
  exact schema_reset_at_coverage R seq A c (a - 1) (by omega) valid.2

/-- Provenance of every preparation in the audience ledger. -/
theorem preparation_announced (seq : Nat → Slot S) (A : S.Aud)
    (initial : (R.aud0 A).preparations = []) (kind : Boundary) (c : Nat) (p : S.Path) :
    ∀ k, (kind, c, p) ∈ (audAt R seq A k).preparations →
      ∃ a d, 1 ≤ a ∧ a ≤ k ∧ seq a = .prepare d A kind c p ∧ AppliedTo R seq A a
  | 0, h => by simp [audAt, initial] at h
  | k + 1, h => by
    rw [audAt_succ] at h
    unfold audStep at h
    split at h
    · rename_i touches
      split at h
      · rename_i valid
        cases slot : seq (k + 1) with
        | prepare d a v f q =>
          rw [slot] at h touches
          have same : a = A := by simpa [CovenSequence.touches] using touches
          subst a
          simp only [audEffect, List.mem_cons, Prod.mk.injEq] at h
          rcases h with ⟨rfl, rfl, rfl⟩ | old
          · exact ⟨k + 1, d, by omega, Nat.le_refl _, slot,
              by simpa [AppliedTo, slot, CovenSequence.touches] using valid⟩
          · obtain ⟨i, d, hi, le, hs, ha⟩ := preparation_announced seq A initial kind c p k old
            exact ⟨i, d, hi, by omega, hs, ha⟩
        | writes d g parts =>
          rw [slot] at h
          simp only [audEffect] at h
          split at h <;>
            obtain ⟨i, d, hi, le, hs, ha⟩ := preparation_announced seq A initial kind c p k h <;>
            exact ⟨i, d, hi, by omega, hs, ha⟩
        | _ =>
          rw [slot] at h
          obtain ⟨i, d, hi, le, hs, ha⟩ := preparation_announced seq A initial kind c p k h
          exact ⟨i, d, hi, by omega, hs, ha⟩
      · obtain ⟨i, d, hi, le, hs, ha⟩ := preparation_announced seq A initial kind c p k h
        exact ⟨i, d, hi, by omega, hs, ha⟩
    · obtain ⟨i, d, hi, le, hs, ha⟩ := preparation_announced seq A initial kind c p k h
      exact ⟨i, d, hi, by omega, hs, ha⟩

/-- An applied raise or reset has an earlier matching announcement. Upload
precedes that announcement by StoreStep.claim's preparation-body premise. -/
theorem activation_requires_preparation (seq : Nat → Slot S) (A : S.Aud)
    (initial : (R.aud0 A).preparations = []) {r : Nat} {d : S.Dev}
    {kind : Boundary} {c : Nat} {p : S.Path}
    (slot : seq r = .boundary d A kind c p) (applied : AppliedTo R seq A r) :
    ∃ a e, 1 ≤ a ∧ a < r ∧ seq a = .prepare e A kind c p ∧ AppliedTo R seq A a := by
  have valid := applied.2
  rw [slot] at valid
  simp only [audValid, Bool.and_eq_true, decide_eq_true_eq] at valid
  obtain ⟨a, e, positive, le, hs, ha⟩ := preparation_announced R seq A initial kind c p (r - 1) valid.1.2
  exact ⟨a, e, positive, by omega, hs, ha⟩

/-- Announcement alone does not change rows, schema, reset or usable ordinary
snapshots. Its coverage still constrains later preparations and entries. -/
theorem preparation_no_effect (L : Ledger S) (P : AudLedger S) (k : Nat) (A : S.Aud)
    (d : S.Dev) (kind : Boundary) (c : Nat) (p : S.Path) (rows : S.Rows) :
    (audStep R L P k A (.prepare d A kind c p)).snapshots = P.snapshots ∧
    (audStep R L P k A (.prepare d A kind c p)).version = P.version ∧
    (audStep R L P k A (.prepare d A kind c p)).lastReset = P.lastReset ∧
    rowsStep R A L P rows k (.prepare d A kind c p) = rows := by
  by_cases h : audValid R L P k A (.prepare d A kind c p) = true
  all_goals simp [audStep, rowsStep, touches, h, audEffect, refuseAud, applyWrite]

/-- A reset replaces rows with its chosen body and replays none of the suffix
that could not yet have read its entry. -/
theorem reset_replaces (seq : Nat → Slot S) (A : S.Aud) (k : Nat) (d : S.Dev) (c : Nat) (p : S.Path)
    (slot : seq (k + 1) = .boundary d A .reset c p)
    (applied : AppliedTo R seq A (k + 1)) : rowsAt R seq A (k + 1) = R.resetRows p := by
  rw [rowsAt_boundary R seq A k slot]
  have hv := applied.2
  simp only [Nat.add_sub_cancel, slot] at hv
  simp [hv, finishBoundary, preparedRows]

open ReaderExamples in
def abandoned : Nat → Slot sig
  | 1 => .prepare () () .reset 0 (7 : Nat)
  | _ => .writes () 0 (fun _ => none)

/-- A separately announced reset preparation that never takes effect is not
an ordinary snapshot, has no row effects and never becomes a reset. -/
theorem abandoned_supplies_no_coverage :
    AppliedTo ReaderExamples.rules abandoned () 1 ∧
    ∀ k, (audAt ReaderExamples.rules abandoned () k).snapshots = [] ∧
      (audAt ReaderExamples.rules abandoned () k).lastReset = 0 ∧
      rowsAt ReaderExamples.rules abandoned () k = [] := by
  refine ⟨⟨rfl, rfl⟩, ?_⟩
  intro k
  induction k with
  | zero => exact ⟨rfl, rfl, by rw [rowsAt_zero]; rfl⟩
  | succ k ih =>
    cases k with
    | zero =>
      refine ⟨rfl, rfl, ?_⟩
      rw [rowsAt_other ReaderExamples.rules abandoned () 0 (by intros; simp [abandoned])]
      simp [abandoned, touches, audValid, applyWrite, rowsAt_zero, ReaderExamples.rules]
    | succ k =>
      rw [audAt_succ]
      have nr : ∀ d a v c p, abandoned (k + 1 + 1) ≠ .boundary d a v c p := by
        intro d a v c p; simp [abandoned]
      rw [rowsAt_other ReaderExamples.rules abandoned () (k + 1) nr]
      simpa [abandoned, audStep, touches] using ih

theorem abandoned_not_usable (head c : Nat) (p : Nat) :
    ¬ Usable ReaderExamples.rules abandoned () head c p := by
  intro usable
  rcases usable with ⟨a, d, _, _, slot, _⟩ | ⟨a, d, kind, _, _, slot, _⟩
  all_goals cases a with
  | zero => simp [abandoned] at slot
  | succ a => cases a <;> simp [abandoned] at slot

end CovenSequence.Preparations
