import CovenStorelog.CurrentReplay
import CovenStorelogData.RetentionSafety

/-! §9: strict old prefix, inclusive recent window, complete storage listing.
Appendix C10 supplies the replay-prefix theorem and timestamp partition. -/
namespace CovenStorelogData.StorageFinality
open CovenStorelog
open CurrentReplay

def old (H : Finality.History) (W T : Nat) : EntrySet :=
  fun e => H.stored e + W < T

def quiet (H : Finality.History) (W n T : Nat) : Bool :=
  (List.range n).all fun e =>
    !(decide (T ≤ H.stored e + W) && decide (H.stored e ≤ T)) ||
      !Finality.late H n (fun _ => true) e

def CompleteOld (H : Finality.History) (W n T : Nat) (S : EntrySet) : Prop :=
  ∀ e, e < n → old H W T e = true → S e = true

def completeThrough (H : Finality.History) (n T : Nat) (received : EntrySet) : Bool :=
  (List.range n).all fun e => !decide (H.stored e ≤ T) || received e

def certified (H : Finality.History) (W n T : Nat) (received : EntrySet) : EntrySet :=
  fun e => decide (e < n) && old H W T e && quiet H W n T && completeThrough H n T received

theorem complete_old (H : Finality.History) (W n T : Nat) (S : EntrySet)
    (complete : completeThrough H n T S = true) : CompleteOld H W n T S := by
  intro e he ho
  have age : H.stored e + W < T := by simpa [old] using ho
  have h := List.all_eq_true.mp complete e (List.mem_range.mpr he)
  simpa [show H.stored e ≤ T by omega] using h

theorem survivor_reads_old (H : Finality.History) (W n T : Nat)
    (hq : quiet H W n T = true) {a b : Nat} (ha : a < n) (hb : b < n)
    (ho : old H W T a = true) (hn : old H W T b = false)
    (survives : Finality.tooLate H W n (fun _ => true) b = false) :
    hadRead H.log b a = true := by
  have hao : H.stored a + W < T := by simpa [old] using ho
  have hbn : T ≤ H.stored b + W := by simpa [old] using hn
  by_cases landed : H.stored b ≤ T
  · have h := List.all_eq_true.mp hq b (List.mem_range.mpr hb)
    have notLate : Finality.late H n (fun _ => true) b = false := by
      simpa [hbn, landed] using h
    have h := List.any_eq_false.mp notLate a (List.mem_range.mpr ha)
    simpa [show H.stored a < H.stored b by omega] using h
  · cases hr : hadRead H.log b a
    · have bad := (Finality.tooLate_iff H W n (fun _ => true) b).mpr
        ⟨a, ha, rfl, by omega, hr⟩
      simp [survives] at bad
    · rfl

theorem kept_prefix (H : Finality.History) (W n T : Nat) (S : EntrySet)
    (causal : CovenStorelog.Valid H.log n) (hq : quiet H W n T = true) :
    let views := CurrentReplay.authorViews H W n n
    let live := admitted H W n views S
    let P := (List.range n).filter (fun e => live e && old H W T e)
    ReplayPrefix.Agree P (CurrentReplay.resolve H W n S).kept
      (settle H.log views P (conflict H.log views) (prefer H.log views) (realize views)).kept := by
  dsimp only
  let views := CurrentReplay.authorViews H W n n
  let live := admitted H W n views S
  let P := (List.range n).filter (fun e => live e && old H W T e)
  let suffix := (List.range n).filter (fun e => live e && !old H W T e)
  have read (b a : Nat) (hb : b < n) (ha : a < n)
      (hl : live b = true) (hn : old H W T b = false) (ho : old H W T a = true) :
      hadRead H.log b a = true := by
    apply survivor_reads_old H W n T hq ha hb ho hn
    have parsed : (S b = true ∧ Finality.tooLate H W n (fun _ => true) b = false) ∧
        keysMatch (views b) (H.log b) = true := by
      simpa only [live, admitted, Bool.and_eq_true, Bool.not_eq_true'] using hl
    exact parsed.1.2
  have splitList : (List.range n).filter live = P ++ suffix := by
    apply Finality.range_split
    intro a b ha hb _ hl ho hn
    exact causal.past_lt b hb a (read b a hb ha hl hn ho)
  have sep : ∀ b ∈ suffix, b ∉ P ∧ ∀ a ∈ P, conflict H.log views b a = false := by
    intro b hb
    obtain ⟨hb, hl, hn⟩ := by
      simpa only [suffix, List.mem_filter, List.mem_range, Bool.and_eq_true,
        Bool.not_eq_true'] using hb
    constructor
    · simp [P, hn]
    · intro a ha
      obtain ⟨ha, _, ho⟩ := by
        simpa only [P, List.mem_filter, List.mem_range, Bool.and_eq_true] using ha
      exact CurrentReplay.read_no_conflict H.log views b a (read b a hb ha hl hn ho)
  have result := ReplayPrefix.settle_prefix H.log views (conflict H.log views)
    (prefer H.log views) P suffix ((List.nodup_range (n := n)).filter _) sep (realize views)
  rw [← splitList] at result
  exact result.1

theorem stability (H : Finality.History) (W n T : Nat) (S U : EntrySet)
    (causal : CovenStorelog.Valid H.log n)
    (hs : CompleteOld H W n T S) (hu : CompleteOld H W n T U)
    (hq : quiet H W n T = true) {e : Nat} (he : e < n)
    (ho : old H W T e = true) :
    e ∈ (CurrentReplay.resolve H W n S).kept ↔
      e ∈ (CurrentReplay.resolve H W n U).kept := by
  let views := CurrentReplay.authorViews H W n n
  have same : (List.range n).filter (fun a => admitted H W n views S a && old H W T a) =
      (List.range n).filter (fun a => admitted H W n views U a && old H W T a) := by
    apply List.filter_congr
    intro a ha
    cases h : old H W T a
    · simp
    · simp [admitted, hs a (List.mem_range.mp ha) h, hu a (List.mem_range.mp ha) h]
  cases gate : !Finality.tooLate H W n (fun _ => true) e && keysMatch (views e) (H.log e)
  · have absent (V : EntrySet) : e ∉ (CurrentReplay.resolve H W n V).kept := by
      have h := settleN_accounting H.log views
        ((List.range n).filter (admitted H W n views V))
        ((List.nodup_range (n := n)).filter _) (by simp)
        (settle_eq_some H.log views _ (conflict H.log views) (prefer H.log views) (realize views))
      intro kept
      have mem := (h.covered e).mpr (Or.inr (Or.inl kept))
      have no : admitted H W n views V e = false := by
        simp only [admitted, Bool.and_assoc, gate, Bool.and_false]
      simp [no] at mem
    simp [absent S, absent U]
  · have present : e ∈ (List.range n).filter
        (fun a => admitted H W n views U a && old H W T a) := by
      simp [admitted, hu e he ho, he, ho, Bool.and_assoc, gate]
    have ps := kept_prefix H W n T S causal hq
    have pu := kept_prefix H W n T U causal hq
    dsimp only at ps pu
    change ReplayPrefix.Agree _ _ _ at ps pu
    rw [same] at ps
    exact (ps e present).trans (pu e present).symm

theorem retention_finality (H : Finality.History) (W n T : Nat) (S U : EntrySet)
    (causal : CovenStorelog.Valid H.log n)
    (received : ∀ e, S e = true → U e = true) :
    RetentionSafety.Stable
      (fun e => decide (e ∈ (CurrentReplay.resolve H W n S).kept))
      (fun e => decide (e ∈ (CurrentReplay.resolve H W n U).kept))
      (certified H W n T S) := by
  intro e final
  have h : ((e < n ∧ old H W T e = true) ∧ quiet H W n T = true) ∧
      completeThrough H n T S = true := by
    simpa only [certified, Bool.and_eq_true, decide_eq_true_eq] using final
  have hs := complete_old H W n T S h.2
  have hu : CompleteOld H W n T U := fun e he ho => received e (hs e he ho)
  simp only [stability H W n T S U causal hs hu h.1.2 h.1.1.1 h.1.1.2]

theorem retention_preserves_replay (H : Finality.History) (W n T : Nat) (S U : EntrySet)
    (causal : CovenStorelog.Valid H.log n)
    (received : ∀ e, S e = true → U e = true)
    (inputs : List RetentionSafety.Input) (input : RetentionSafety.Input)
    (present : input ∈ inputs)
    (needed : input.needed.eval (fun e => decide (e ∈ (CurrentReplay.resolve H W n U).kept)) = true) :
    input ∈ RetentionSafety.retain
      (fun e => decide (e ∈ (CurrentReplay.resolve H W n S).kept))
      (certified H W n T S) inputs :=
  RetentionSafety.retention_preserves_possible_replay _ _ _
    (retention_finality H W n T S U causal received) inputs input present needed

/-- More than a window without another late landing certifies the old prefix.
Equal stored times share the same boundary; no acknowledgement occurs here. -/
theorem progress (H : Finality.History) (W n last T : Nat)
    (elapsed : last + W < T)
    (noLate : ∀ e, e < n → last < H.stored e → H.stored e ≤ T →
      Finality.late H n (fun _ => true) e = false) :
    quiet H W n T = true := by
  apply List.all_eq_true.mpr
  intro e he
  by_cases window : T ≤ H.stored e + W ∧ H.stored e ≤ T
  · have h := noLate e (List.mem_range.mp he) (by omega) window.2
    simp [h]
  · have outside : H.stored e + W < T ∨ T < H.stored e := by omega
    rcases outside with h | h <;> simp [Nat.not_le.mpr h]

theorem bounded_storage (H : Finality.History) (W n T : Nat) (S : EntrySet)
    (hq : quiet H W n T = true) (complete : completeThrough H n T S = true)
    (inputs : List RetentionSafety.Input)
    (dependencies : ∀ input ∈ inputs, ∀ e ∈ input.needed.entries,
      e < n ∧ H.stored e + W < T)
    (otherChecks : ∀ input ∈ inputs, input.checks.all (·.met) = true) :
    RetentionSafety.retain
      (fun e => decide (e ∈ (CurrentReplay.resolve H W n S).kept))
      (certified H W n T S) inputs =
    inputs.filter (fun input => input.needed.eval
      (fun e => decide (e ∈ (CurrentReplay.resolve H W n S).kept))) := by
  apply RetentionSafety.bounded_after_finality
  intro input hi
  simp only [RetentionSafety.conditions, List.all_append, Bool.and_eq_true]
  refine ⟨?_, otherChecks input hi⟩
  apply List.all_eq_true.mpr
  intro condition hc
  obtain ⟨e, he, rfl⟩ := List.mem_map.mp hc
  have h := dependencies input hi e he
  simp [certified, old, h.1, h.2, hq, complete]

structure Observation where
  time : Nat
  received : EntrySet

/-- Saved windows keep finality established before another race began. -/
def established (H : Finality.History) (W n : Nat) (observations : List Observation) : EntrySet :=
  fun e => observations.any fun observation => certified H W n observation.time observation.received e

theorem finality_persists (H : Finality.History) (W n : Nat)
    (before later : List Observation) (e : Nat) (final : established H W n before e = true) :
    established H W n (before ++ later) e = true := by
  unfold established at final ⊢
  rw [List.any_append, final]
  rfl

theorem incomplete_listing_blocks_finality (H : Finality.History) (W n T : Nat)
    (S : EntrySet) (incomplete : completeThrough H n T S = false) (e : Nat) :
    certified H W n T S e = false := by simp [certified, incomplete]

theorem established_stable (H : Finality.History) (W n : Nat)
    (observations : List Observation) (S U : EntrySet)
    (causal : CovenStorelog.Valid H.log n)
    (hs : ∀ observation ∈ observations, ∀ e, observation.received e = true → S e = true)
    (hu : ∀ observation ∈ observations, ∀ e, observation.received e = true → U e = true) :
    RetentionSafety.Stable
      (fun e => decide (e ∈ (CurrentReplay.resolve H W n S).kept))
      (fun e => decide (e ∈ (CurrentReplay.resolve H W n U).kept))
      (established H W n observations) := by
  intro e final
  obtain ⟨observation, present, certified⟩ := List.any_eq_true.mp final
  have a := retention_finality H W n observation.time observation.received S causal
    (hs observation present) e certified
  have b := retention_finality H W n observation.time observation.received U causal
    (hu observation present) e certified
  exact a.symm.trans b

example (H : Finality.History) (W T e : Nat) (boundary : H.stored e + W = T) :
    old H W T e = false := by simp [old, boundary]

end CovenStorelogData.StorageFinality
