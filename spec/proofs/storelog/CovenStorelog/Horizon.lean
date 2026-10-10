import CovenStorelog.CurrentReplay

/-! §9 finality with the strict old boundary and both recent endpoints.
Storage times are duration values; equal times are never ordered by path. -/
namespace CovenStorelog.Horizon
open Finality (History)

/-- A complete check through T. Both endpoints belong to the recent window;
late entries count even when their changes were permanently dropped. -/
def quiet (H : History) (W n T : Nat) : Bool :=
  (List.range n).all fun e =>
    !(decide (T ≤ H.stored e + W) && decide (H.stored e ≤ T)) ||
      !Finality.late H n (fun _ => true) e

def old (H : History) (W T : Nat) : EntrySet :=
  fun e => decide (H.stored e + W < T)

/-- A successful check contributes the prefix strictly before T-W.
Zero is the empty prefix because storage times are nonnegative. -/
def advance (H : History) (W n horizon T : Nat) : Nat :=
  if quiet H W n T then max horizon (T - W) else horizon

def horizon (H : History) (W n : Nat) (checks : List Nat) : Nat :=
  checks.foldl (advance H W n) 0

/-- The independently specified set: certified by any completed quiet check,
including an earlier one (§9). This does not refer to the stored horizon. -/
def finalSet (H : History) (W n : Nat) (checks : List Nat) : EntrySet :=
  fun e => decide (e < n) && checks.any (fun T => old H W T e && quiet H W n T)

theorem advance_iff (H : History) (W n h T time : Nat) :
    time < advance H W n h T ↔ time < h ∨ quiet H W n T = true ∧ time + W < T := by
  unfold advance
  cases hq : quiet H W n T <;> simp only [Bool.false_eq_true,
    ↓reduceIte, false_and, true_and, or_false] <;> omega

theorem accumulated_iff (H : History) (W n : Nat) (checks : List Nat) (h time : Nat) :
    time < checks.foldl (advance H W n) h ↔
      time < h ∨ ∃ T ∈ checks, quiet H W n T = true ∧ time + W < T := by
  induction checks generalizing h with
  | nil => simp
  | cons T Ts ih =>
    rw [List.foldl_cons, ih, advance_iff]
    simp only [List.mem_cons]
    grind

/-- One number represents exactly all the prefixes certified so far. No
entry at the horizon is final; equal storage times always stay together. -/
theorem final_iff_before_horizon (H : History) (W n : Nat) (checks : List Nat) (e : Nat) :
    finalSet H W n checks e = true ↔ e < n ∧ H.stored e < horizon H W n checks := by
  simp only [finalSet, Bool.and_eq_true, decide_eq_true_eq, List.any_eq_true,
    old, horizon, accumulated_iff, Nat.not_lt_zero, false_or]
  constructor
  · rintro ⟨he, T, ht, ho, hq⟩; exact ⟨he, T, ht, hq, ho⟩
  · rintro ⟨he, T, ht, hq, ho⟩; exact ⟨he, T, ht, ho, hq⟩

theorem advance_never_back (H : History) (W n h T : Nat) : h ≤ advance H W n h T := by
  unfold advance
  split
  · exact Nat.le_max_left _ _
  · exact Nat.le_refl _

theorem horizon_never_back (H : History) (W n : Nat) (checks later : List Nat) :
    horizon H W n checks ≤ horizon H W n (checks ++ later) := by
  have grows (xs : List Nat) (h : Nat) : h ≤ xs.foldl (advance H W n) h := by
    induction xs generalizing h with
    | nil => exact Nat.le_refl _
    | cons T Ts ih => exact Nat.le_trans (advance_never_back H W n h T) (ih _)
  simp only [horizon, List.foldl_append]
  exact grows later _

theorem horizon_mono_checks (H : History) (W n : Nat) (a b : List Nat)
    (includes : ∀ T ∈ a, T ∈ b) : horizon H W n a ≤ horizon H W n b := by
  apply Nat.le_of_not_gt
  intro lt
  obtain ⟨T, ht, hq, ho⟩ := (by
    simpa only [horizon, accumulated_iff, Nat.not_lt_zero, false_or] using lt :
      ∃ T ∈ a, quiet H W n T = true ∧ horizon H W n b + W < T)
  have impossible : horizon H W n b < horizon H W n b := by
    simp only [horizon, accumulated_iff, Nat.not_lt_zero, false_or]
    exact ⟨T, includes T ht, hq, ho⟩
  omega

/-- Mathematical description of all earlier windows discoverable from a
complete listing through T, not a requirement to iterate clock ticks. -/
def recovered (H : History) (W n T : Nat) : Nat :=
  horizon H W n (List.range (T + 1))

theorem recovered_iff (H : History) (W n T time : Nat) :
    time < recovered H W n T ↔ ∃ t, t ≤ T ∧ quiet H W n t = true ∧ time + W < t := by
  simp only [recovered, horizon, accumulated_iff, Nat.not_lt_zero, false_or, List.mem_range]
  constructor <;> rintro ⟨t, ht, hq, ho⟩ <;> exact ⟨t, by omega, hq, ho⟩

theorem recovered_never_back (H : History) (W n T U : Nat) (later : T ≤ U) :
    recovered H W n T ≤ recovered H W n U := by
  apply horizon_mono_checks
  intro t ht
  have := List.mem_range.mp ht
  exact List.mem_range.mpr (by omega)

theorem final_stays_final (H : History) (W n : Nat) (checks later : List Nat) (e : Nat)
    (hf : finalSet H W n checks e = true) : finalSet H W n (checks ++ later) e = true := by
  obtain ⟨he, ht⟩ := (final_iff_before_horizon H W n checks e).mp hf
  exact (final_iff_before_horizon H W n (checks ++ later) e).mpr
    ⟨he, Nat.lt_of_lt_of_le ht (horizon_never_back H W n checks later)⟩

theorem ties_together (H : History) (W n : Nat) (checks : List Nat) (a b : Nat)
    (ha : a < n) (hb : b < n) (same : H.stored a = H.stored b) :
    finalSet H W n checks a = finalSet H W n checks b := by
  apply Bool.eq_iff_iff.mpr
  simp only [final_iff_before_horizon, ha, hb, same]

theorem boundary_not_final (H : History) (W n : Nat) (checks : List Nat) (e : Nat)
    (boundary : H.stored e = horizon H W n checks) : finalSet H W n checks e = false := by
  cases hf : finalSet H W n checks e
  · rfl
  · have := ((final_iff_before_horizon H W n checks e).mp hf).2
    omega

/-- Failed, incomplete and interrupted listings contribute no completed
check. The horizon is unchanged, even if a newer provider time was observed. -/
def checkResult (H : History) (W n h : Nat) : Option Nat → Nat
  | none => h
  | some T => advance H W n h T

theorem failed_check_preserves (H : History) (W n h : Nat) :
    checkResult H W n h none = h := rfl

theorem quiet_reads (H : History) (W n T : Nat) (hq : quiet H W n T = true)
    {b a : Nat} (hb : b < n) (ha : a < n)
    (recent : T ≤ H.stored b + W) (landed : H.stored b ≤ T)
    (earlier : H.stored a < H.stored b) : hadRead H.log b a = true := by
  have h := List.all_eq_true.mp hq b (List.mem_range.mpr hb)
  have hl : Finality.late H n (fun _ => true) b = false := by
    simpa only [recent, landed, decide_true, Bool.and_self, Bool.not_true,
      Bool.false_or, Bool.not_eq_true'] using h
  have hh := List.any_eq_false.mp hl a (List.mem_range.mpr ha)
  simpa [earlier] using hh

/-- Every admitted entry outside the strict prefix read that entire prefix.
No unique-times or online-attempt assumption is needed for this implication. -/
theorem survivor_reads_old (H : History) (W n T : Nat) (hq : quiet H W n T = true)
    {b a : Nat} (hb : b < n) (ha : a < n)
    (new : old H W T b = false) (prior : old H W T a = true)
    (survives : Finality.tooLate H W n (fun _ => true) b = false) :
    hadRead H.log b a = true := by
  have hn : T ≤ H.stored b + W := by simpa [old] using new
  have ho : H.stored a + W < T := by simpa [old] using prior
  by_cases landed : H.stored b ≤ T
  · exact quiet_reads H W n T hq hb ha hn landed (by omega)
  · cases read : hadRead H.log b a
    · have late := (Finality.tooLate_iff H W n (fun _ => true) b).mpr
        ⟨a, ha, rfl, by omega, read⟩
      simp [survives] at late
    · rfl

/-- The current §9 replay has the same settled prefix in every larger
received set. This includes its actual conflict rules and circle effects. -/
theorem current_prefix (H : History) (W n T : Nat) (S : EntrySet)
    (hv : CovenStorelog.Valid H.log n) (hq : quiet H W n T = true) :
    let views := ReplayPolicy.authorViews H W n n
    let live := ReplayPolicy.admitted H W n S
    let P := (List.range n).filter (fun e => live e && old H W T e)
    let r := settle H.log views P (ReplayPolicy.conflict H.log views)
      (ReplayPolicy.prefer H.log views) (ReplayPolicy.realize views)
    ReplayPrefix.Agree P (ReplayPolicy.resolve H W n S).kept r.kept ∧
    ReplayPrefix.Agree P (ReplayPolicy.resolve H W n S).dropped r.dropped := by
  dsimp only
  let views := ReplayPolicy.authorViews H W n n
  let live := ReplayPolicy.admitted H W n S
  let P := (List.range n).filter (fun e => live e && old H W T e)
  let suffix := (List.range n).filter (fun e => live e && !old H W T e)
  have read (b a : Nat) (hb : b < n) (ha : a < n)
      (hl : live b = true) (hn : old H W T b = false) (ho : old H W T a = true) :
      hadRead H.log b a = true := by
    have survives : Finality.tooLate H W n (fun _ => true) b = false := by
      simp only [live, ReplayPolicy.admitted, Bool.and_eq_true, Bool.not_eq_true'] at hl
      exact hl.2
    exact survivor_reads_old H W n T hq hb ha hn ho survives
  have splitList : (List.range n).filter live = P ++ suffix := by
    apply Finality.range_split
    intro a b ha hb _ hl ho hn
    exact hv.past_lt b hb a (read b a hb ha hl hn ho)
  have sep : ∀ b ∈ suffix, b ∉ P ∧ ∀ a ∈ P, ReplayPolicy.conflict H.log views b a = false := by
    intro b hb
    obtain ⟨hb, hl, hn⟩ := by
      simpa only [suffix, List.mem_filter, List.mem_range, Bool.and_eq_true,
        Bool.not_eq_true'] using hb
    constructor
    · simp [P, hn]
    · intro a ha
      obtain ⟨ha, _, ho⟩ := by
        simpa only [P, List.mem_filter, List.mem_range, Bool.and_eq_true] using ha
      exact ReplayPolicy.read_no_conflict H.log views b a (read b a hb ha hl hn ho)
  have hp := ReplayPrefix.settle_prefix H.log views (ReplayPolicy.conflict H.log views)
    (ReplayPolicy.prefer H.log views) P suffix ((List.nodup_range (n := n)).filter _) sep
    (ReplayPolicy.realize views)
  rw [← splitList] at hp
  refine ⟨hp.1, ?_⟩
  intro a ha
  have halive : live a = true := (Bool.and_eq_true_iff.mp (List.mem_filter.mp ha).2).1
  have halive' : ReplayPolicy.admitted H W n S none a = true := halive
  simpa only [ReplayPolicy.resolve, ReplayPolicy.materialize, ReplayPolicy.finish,
    ReplayPolicy.replay, Option.map_none, List.mem_append, List.mem_filter, List.mem_range,
    Bool.and_eq_true, halive',
    Bool.not_true, Bool.false_eq_true, and_false, or_false] using hp.2 a ha

theorem excluded_disposition (H : History) (W n e : Nat) (S : EntrySet)
    (he : e < n) (hs : S e = true)
    (excluded : ReplayPolicy.admitted H W n S none e = false) :
    e ∉ (ReplayPolicy.resolve H W n S).kept ∧
    e ∈ (ReplayPolicy.resolve H W n S).dropped := by
  let views := ReplayPolicy.authorViews H W n n
  let candidates := (List.range n).filter (ReplayPolicy.admitted H W n S)
  have acc := settleN_accounting H.log views candidates ((List.nodup_range (n := n)).filter _)
    (by simp) (settle_eq_some H.log views candidates (ReplayPolicy.conflict H.log views)
      (ReplayPolicy.prefer H.log views) (ReplayPolicy.realize views))
  constructor
  · intro kept
    have hm := (acc.covered e).mpr (Or.inr (Or.inl kept))
    simp only [candidates, List.mem_filter, excluded, Bool.false_eq_true,
      and_false] at hm
  · simp [ReplayPolicy.resolve, ReplayPolicy.materialize, ReplayPolicy.finish,
      ReplayPolicy.replay, he, hs, excluded]

def CompleteOld (H : History) (W n T : Nat) (S : EntrySet) : Prop :=
  ∀ e, e < n → old H W T e = true → S e = true

/-- Read entries were already stored. Equal provider timestamps are allowed;
no assumption that a read must have a strictly smaller storage time is needed. -/
theorem old_closed (H : History) (W n T : Nat) (hv : CovenStorelog.Valid H.log n)
    (readStored : ∀ e, e < n → ∀ a, hadRead H.log e a = true → H.stored a ≤ H.stored e) :
    Closed H.log (fun e => decide (e < n) && old H W T e) := by
  intro e he a read
  obtain ⟨he, ho⟩ : e < n ∧ H.stored e + W < T := by
    simpa only [old, Bool.and_eq_true, decide_eq_true_eq] using he
  have smaller := hv.past_lt e he a read
  have stored := readStored e he a read
  simp only [old, Bool.and_eq_true, decide_eq_true_eq]
  exact ⟨by omega, by omega⟩

/-- Final dispositions agree across arbitrary later receipts. The universe
H,n may contain any finite continuation, with arbitrary timestamp order and
tied storage times. Each device must have the certified old prefix. -/
theorem current_stability (H : History) (W n T : Nat) (S U : EntrySet)
    (hv : CovenStorelog.Valid H.log n) (hq : quiet H W n T = true)
    (hs : CompleteOld H W n T S) (hu : CompleteOld H W n T U)
    {e : Nat} (he : e < n) (ho : old H W T e = true) :
    (e ∈ (ReplayPolicy.resolve H W n S).kept ↔ e ∈ (ReplayPolicy.resolve H W n U).kept) ∧
    (e ∈ (ReplayPolicy.resolve H W n S).dropped ↔ e ∈ (ReplayPolicy.resolve H W n U).dropped) := by
  have live_eq : ReplayPolicy.admitted H W n S none e = ReplayPolicy.admitted H W n U none e := by
    simp [ReplayPolicy.admitted, hs e he ho, hu e he ho]
  cases hl : ReplayPolicy.admitted H W n S none e with
  | false =>
    have ds := excluded_disposition H W n e S he (hs e he ho) hl
    have du := excluded_disposition H W n e U he (hu e he ho) (live_eq ▸ hl)
    exact ⟨by simp [ds.1, du.1], by simp [ds.2, du.2]⟩
  | true =>
    have eqP : (List.range n).filter (fun a => ReplayPolicy.admitted H W n S none a && old H W T a) =
        (List.range n).filter (fun a => ReplayPolicy.admitted H W n U none a && old H W T a) := by
      apply List.filter_congr
      intro a ha
      have ha' := List.mem_range.mp ha
      cases h : old H W T a
      · simp
      · simp [ReplayPolicy.admitted, hs a ha' h, hu a ha' h]
    have ps := current_prefix H W n T S hv hq
    have pu := current_prefix H W n T U hv hq
    dsimp only at ps pu
    have hes : e ∈ (List.range n).filter
        (fun a => ReplayPolicy.admitted H W n S none a && old H W T a) := by simp [he, hl, ho]
    have heu := eqP ▸ hes
    rw [eqP] at ps
    exact ⟨(ps.1 e heu).trans (pu.1 e heu).symm, (ps.2 e heu).trans (pu.2 e heu).symm⟩

/-- The number itself suffices as the receipt cutoff for every earlier
certificate that contributed to it. The evidence establishing those quiet
windows was obtained by completed checks through their observation times. -/
theorem horizon_stability (H : History) (W n : Nat) (checks : List Nat) (S U : EntrySet)
    (hv : CovenStorelog.Valid H.log n)
    (hs : ∀ e, e < n → H.stored e < horizon H W n checks → S e = true)
    (hu : ∀ e, e < n → H.stored e < horizon H W n checks → U e = true)
    {e : Nat} (he : e < n) (hf : H.stored e < horizon H W n checks) :
    (e ∈ (ReplayPolicy.resolve H W n S).kept ↔ e ∈ (ReplayPolicy.resolve H W n U).kept) ∧
    (e ∈ (ReplayPolicy.resolve H W n S).dropped ↔ e ∈ (ReplayPolicy.resolve H W n U).dropped) := by
  have member := (final_iff_before_horizon H W n checks e).mpr ⟨he, hf⟩
  obtain ⟨T, ht, cert⟩ := List.any_eq_true.mp (Bool.and_eq_true_iff.mp member).2
  obtain ⟨ho, hq⟩ := Bool.and_eq_true_iff.mp cert
  have cut (a : Nat) (ha : a < n) (old : old H W T a = true) : H.stored a < horizon H W n checks := by
    apply ((final_iff_before_horizon H W n checks a).mp ?_).2
    simp only [finalSet, ha, decide_true, Bool.true_and]
    exact List.any_eq_true.mpr ⟨T, ht, by simp [old, hq]⟩
  exact current_stability H W n T S U hv hq
    (fun a ha old => hs a ha (cut a ha old))
    (fun a ha old => hu a ha (cut a ha old)) he ho

/-- The current key-bearing wire model inherits the same horizon theorem
through its membership projection, including key-free removals. -/
theorem key_free_horizon_stability (H : CurrentReplay.History) (W n : Nat)
    (checks : List Nat) (S U : EntrySet) (hv : CurrentReplay.Valid H.log n)
    (hs : ∀ e, e < n → H.stored e < horizon H.membership W n checks → S e = true)
    (hu : ∀ e, e < n → H.stored e < horizon H.membership W n checks → U e = true)
    {e : Nat} (he : e < n) (hf : H.stored e < horizon H.membership W n checks) :
    (e ∈ (CurrentReplay.resolve H W n S).kept ↔ e ∈ (CurrentReplay.resolve H W n U).kept) ∧
    (e ∈ (CurrentReplay.resolve H W n S).dropped ↔ e ∈ (CurrentReplay.resolve H W n U).dropped) :=
  horizon_stability H.membership W n checks S U hv hs hu he hf

end CovenStorelog.Horizon
