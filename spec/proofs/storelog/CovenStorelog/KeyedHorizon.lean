import CovenStorelog.Horizon
import CovenStorelog.OldKeyObjects
import CovenStorelog.ReplayMachinePrefix

/-! §9's finality horizon for the keyed replay. The horizon is computed from
recorded pasts and storage times only, exactly as for unkeyed replay; this
file transfers its stability to `Keyed.resolve` through the generalized
machine's prefix theorem, then supplies `OldKeyObjects`' final-prefix premise
from the horizon. The unkeyed `Horizon.horizon_stability` is not used. -/
namespace CovenStorelog.CurrentReplay.Keyed.Final
open CovenStorelog.ReplayPrefix (Agree)

/-- The timing a keyed history shares with the unkeyed horizon: entries keep
their recorded pasts; their actions do not enter §9's quiet-window test. -/
def timing (H : History) : Finality.History :=
  ⟨fun e => ⟨(H.log e).author, (H.log e).device, (H.log e).past, .create ""⟩,
    H.stored, H.attempted⟩

abbrev old (H : History) (W T : Nat) : EntrySet := CovenStorelog.Horizon.old (timing H) W T
abbrev quiet (H : History) (W n T : Nat) : Bool := CovenStorelog.Horizon.quiet (timing H) W n T
abbrev horizon (H : History) (W n : Nat) (checks : List Nat) : Nat :=
  CovenStorelog.Horizon.horizon (timing H) W n checks

/-- Recorded pasts precede their entry. -/
def PastBefore (H : History) (n : Nat) : Prop :=
  ∀ e, e < n → ∀ a, hadRead H.log e a = true → a < e

theorem metadata_past (H : History) (n : Nat) (hv : metadataValid H n = true) :
    PastBefore H n := by
  intro e he a read
  have h := List.all_eq_true.mp hv e (List.mem_range.mpr he)
  simp only [Bool.and_eq_true] at h
  have inPast : a ∈ (H.log e).past := by simpa [hadRead] using read
  have ordered := List.all_eq_true.mp h.1.1.1.2 a inPast
  simp only [Bool.and_eq_true, decide_eq_true_eq] at ordered
  exact ordered.1

abbrev views (H : History) (W n : Nat) := authorViews H W n n

abbrev run (H : History) (W n : Nat) (entries : List Nat) : Result :=
  ReplayMachine.settle State.empty (fun e => authorized (views H W n e) (H.log e))
    (fun s e => unchanged s (H.log e)) (fun s e => effect (views H W n e) s e (H.log e))
    (conflict H.log (views H W n)) (prefer H.log (views H W n)) entries

theorem read_no_conflict (M : Log) (vs : Nat → State) (b a : Nat)
    (read : hadRead M b a = true) : conflict M vs b a = false := by
  simp [conflict, read]

theorem resolve_kept (H : History) (W n : Nat) (S : EntrySet) :
    (resolve H W n S).kept = (run H W n ((List.range n).filter (admitted H W n S))).kept := rfl

theorem resolve_dropped (H : History) (W n : Nat) (S : EntrySet) :
    (resolve H W n S).dropped = (run H W n ((List.range n).filter (admitted H W n S))).dropped ++
      (List.range n).filter (fun e => S e && !admitted H W n S e) := rfl

theorem kept_lt (H : History) (W n : Nat) (S : EntrySet) (e : Nat)
    (kept : e ∈ (resolve H W n S).kept) : e < n := by
  rw [resolve_kept] at kept
  have h := (ReplayMachine.settle_kept _ _ _ _ _ _ _ e kept).1
  exact List.mem_range.mp (List.mem_filter.mp h).1

/-- The keyed replay has the same settled old prefix in every larger received
set: every admitted entry outside it read the whole prefix. -/
theorem keyed_prefix (H : History) (W n T : Nat) (S : EntrySet)
    (past : PastBefore H n) (hq : quiet H W n T = true) :
    let P := (List.range n).filter (fun e => admitted H W n S e && old H W T e)
    Agree P (resolve H W n S).kept (run H W n P).kept ∧
    Agree P (resolve H W n S).dropped (run H W n P).dropped := by
  dsimp only
  let live := admitted H W n S
  let P := (List.range n).filter (fun e => live e && old H W T e)
  let suffix := (List.range n).filter (fun e => live e && !old H W T e)
  have read (b a : Nat) (hb : b < n) (ha : a < n)
      (hl : live b = true) (hn : old H W T b = false) (ho : old H W T a = true) :
      hadRead H.log b a = true := by
    have survives : Finality.tooLate (timing H) W n (fun _ => true) b = false := by
      simp only [live, admitted, Bool.and_eq_true, Bool.not_eq_true'] at hl
      exact hl.2
    exact CovenStorelog.Horizon.survivor_reads_old (timing H) W n T hq hb ha hn ho survives
  have splitList : (List.range n).filter live = P ++ suffix := by
    apply Finality.range_split
    intro a b ha hb _ hl ho hn
    exact past b hb a (read b a hb ha hl hn ho)
  have sep : ∀ b ∈ suffix, b ∉ P ∧ ∀ a ∈ P, conflict H.log (views H W n) b a = false := by
    intro b hb
    obtain ⟨hb, hl, hn⟩ := by
      simpa only [suffix, List.mem_filter, List.mem_range, Bool.and_eq_true,
        Bool.not_eq_true'] using hb
    constructor
    · simp [P, hn]
    · intro a ha
      obtain ⟨ha, _, ho⟩ := by
        simpa only [P, List.mem_filter, List.mem_range, Bool.and_eq_true] using ha
      exact read_no_conflict H.log (views H W n) b a (read b a hb ha hl hn ho)
  have hp := ReplayMachine.Prefix.settle_prefix State.empty
    (fun e => authorized (views H W n e) (H.log e))
    (fun s e => unchanged s (H.log e)) (fun s e => effect (views H W n e) s e (H.log e))
    (conflict H.log (views H W n)) (prefer H.log (views H W n)) P suffix
    ((List.nodup_range (n := n)).filter _) sep
  rw [← splitList] at hp
  refine ⟨fun x hx => by rw [resolve_kept]; exact hp.1 x hx, ?_⟩
  intro a ha
  have halive : live a = true := (Bool.and_eq_true_iff.mp (List.mem_filter.mp ha).2).1
  rw [resolve_dropped]
  simp only [List.mem_append, List.mem_filter, Bool.and_eq_true, Bool.not_eq_true']
  have hd := hp.2 a ha
  constructor
  · rintro (h | ⟨_, _, h⟩)
    · exact hd.mp h
    · simp [live] at halive; simp [halive] at h
  · exact fun h => Or.inl (hd.mpr h)

/-- An entry not admitted by the landing rule is never kept and is dropped. -/
theorem excluded (H : History) (W n e : Nat) (S : EntrySet)
    (he : e < n) (hs : S e = true) (ex : admitted H W n S e = false) :
    e ∉ (resolve H W n S).kept ∧ e ∈ (resolve H W n S).dropped := by
  constructor
  · intro kept
    rw [resolve_kept] at kept
    have h := (ReplayMachine.settle_kept _ _ _ _ _ _ _ e kept).1
    simp [List.mem_filter, ex] at h
  · rw [resolve_dropped]
    simp [he, hs, ex]

def CompleteOld (H : History) (W n T : Nat) (S : EntrySet) : Prop :=
  ∀ e, e < n → old H W T e = true → S e = true

/-- Final dispositions agree across arbitrary later receipts holding the
certified old prefix, for the keyed replay itself. -/
theorem keyed_stability (H : History) (W n T : Nat) (S U : EntrySet)
    (past : PastBefore H n) (hq : quiet H W n T = true)
    (hs : CompleteOld H W n T S) (hu : CompleteOld H W n T U)
    {e : Nat} (he : e < n) (ho : old H W T e = true) :
    (e ∈ (resolve H W n S).kept ↔ e ∈ (resolve H W n U).kept) ∧
    (e ∈ (resolve H W n S).dropped ↔ e ∈ (resolve H W n U).dropped) := by
  have live_eq : admitted H W n S e = admitted H W n U e := by
    simp [admitted, hs e he ho, hu e he ho]
  cases hl : admitted H W n S e with
  | false =>
    have ds := excluded H W n e S he (hs e he ho) hl
    have du := excluded H W n e U he (hu e he ho) (live_eq ▸ hl)
    exact ⟨by simp [ds.1, du.1], by simp [ds.2, du.2]⟩
  | true =>
    have eqP : (List.range n).filter (fun a => admitted H W n S a && old H W T a) =
        (List.range n).filter (fun a => admitted H W n U a && old H W T a) := by
      apply List.filter_congr
      intro a ha
      have ha' := List.mem_range.mp ha
      cases h : old H W T a
      · simp
      · simp [admitted, hs a ha' h, hu a ha' h]
    have ps := keyed_prefix H W n T S past hq
    have pu := keyed_prefix H W n T U past hq
    dsimp only at ps pu
    have hes : e ∈ (List.range n).filter
        (fun a => admitted H W n S a && old H W T a) := by simp [he, hl, ho]
    have heu := eqP ▸ hes
    rw [eqP] at ps
    exact ⟨(ps.1 e heu).trans (pu.1 e heu).symm, (ps.2 e heu).trans (pu.2 e heu).symm⟩

/-- Every entry strictly before the stored horizon keeps its disposition in
every received set holding all entries before the horizon. -/
theorem keyed_horizon_stability (H : History) (W n : Nat) (checks : List Nat) (S U : EntrySet)
    (past : PastBefore H n)
    (hs : ∀ e, e < n → H.stored e < horizon H W n checks → S e = true)
    (hu : ∀ e, e < n → H.stored e < horizon H W n checks → U e = true)
    {e : Nat} (he : e < n) (hf : H.stored e < horizon H W n checks) :
    (e ∈ (resolve H W n S).kept ↔ e ∈ (resolve H W n U).kept) ∧
    (e ∈ (resolve H W n S).dropped ↔ e ∈ (resolve H W n U).dropped) := by
  have member := (CovenStorelog.Horizon.final_iff_before_horizon (timing H) W n checks e).mpr
    ⟨he, hf⟩
  obtain ⟨T, ht, cert⟩ := List.any_eq_true.mp (Bool.and_eq_true_iff.mp member).2
  obtain ⟨ho, hq⟩ := Bool.and_eq_true_iff.mp cert
  have cut (a : Nat) (ha : a < n) (old : old H W T a = true) :
      H.stored a < horizon H W n checks := by
    apply ((CovenStorelog.Horizon.final_iff_before_horizon (timing H) W n checks a).mp ?_).2
    simp only [CovenStorelog.Horizon.finalSet, ha, decide_true, Bool.true_and]
    exact List.any_eq_true.mpr ⟨T, ht, by simp [Final.old] at old; simp [old, hq]⟩
  exact keyed_stability H W n T S U past hq
    (fun a ha old => hs a ha (cut a ha old))
    (fun a ha old => hu a ha (cut a ha old)) he ho

/-- The final-prefix premise of `OldKeyObjects.replacement_window_final`,
supplied by the horizon for every kept entry stored before it. -/
theorem horizon_final_prefix (H : History) (W n : Nat) (checks : List Nat) (S U : EntrySet)
    (past : PastBefore H n)
    (hs : ∀ e, e < n → H.stored e < horizon H W n checks → S e = true)
    (hu : ∀ e, e < n → H.stored e < horizon H W n checks → U e = true)
    (a : Nat) (final : H.stored a < horizon H W n checks) :
    a ∈ (resolve H W n S).kept ↔ a ∈ (resolve H W n U).kept := by
  by_cases ha : a < n
  · exact (keyed_horizon_stability H W n checks S U past hs hu ha final).1
  · exact ⟨fun h => absurd (kept_lt H W n S a h) ha, fun h => absurd (kept_lt H W n U a h) ha⟩

/-- A kept retiring replacement stored before the horizon fixes the
old-key write's window verdict, whatever the write's storage time. -/
theorem horizon_finalizes_replacement (H : History) (W n : Nat) (checks : List Nat)
    (S U : EntrySet) (past : PastBefore H n)
    (hs : ∀ e, e < n → H.stored e < horizon H W n checks → S e = true)
    (hu : ∀ e, e < n → H.stored e < horizon H W n checks → U e = true)
    (obj : OldKeyObjects.Write) (e : Nat) (kept : e ∈ (resolve H W n S).kept)
    (retired : OldKeyObjects.retires H obj e = true) (final : H.stored e < horizon H W n checks) :
    OldKeyObjects.timely H W (resolve H W n U) obj =
      OldKeyObjects.timely H W (resolve H W n S) obj :=
  OldKeyObjects.replacement_window_final H W e _ _ obj kept retired fun a older =>
    horizon_final_prefix H W n checks S U past hs hu a (Nat.lt_of_le_of_lt older final)

/-- Once the horizon reaches the write's storage time minus the window, every
replacement able to exclude it is final, so its verdict is too: admitted or
late, without assuming any retiring replacement is kept. -/
theorem horizon_finalizes_window (H : History) (W n : Nat) (checks : List Nat)
    (S U : EntrySet) (past : PastBefore H n)
    (hs : ∀ e, e < n → H.stored e < horizon H W n checks → S e = true)
    (hu : ∀ e, e < n → H.stored e < horizon H W n checks → U e = true)
    (obj : OldKeyObjects.Write) (settled : obj.stored ≤ horizon H W n checks + W) :
    OldKeyObjects.timely H W (resolve H W n S) obj =
      OldKeyObjects.timely H W (resolve H W n U) obj := by
  have transfer (A B : EntrySet)
      (same : ∀ a, H.stored a < horizon H W n checks →
        (a ∈ (resolve H W n A).kept ↔ a ∈ (resolve H W n B).kept))
      (h : OldKeyObjects.timely H W (resolve H W n A) obj = true) :
      OldKeyObjects.timely H W (resolve H W n B) obj = true := by
    apply (OldKeyObjects.timely_iff _ _ _ _).mpr
    intro a ha hr
    by_cases within : obj.stored ≤ H.stored a + W
    · exact within
    · have final : H.stored a < horizon H W n checks := by omega
      exact (OldKeyObjects.timely_iff _ _ _ _).mp h a ((same a final).mpr ha) hr
  apply Bool.eq_iff_iff.mpr
  exact ⟨transfer S U (horizon_final_prefix H W n checks S U past hs hu),
    transfer U S (horizon_final_prefix H W n checks U S past hu hs)⟩

/-- The admission decision itself, for any other received writes. -/
theorem horizon_finalizes_count (H : History) (W n : Nat) (checks : List Nat)
    (S U X Y : EntrySet) (past : PastBefore H n)
    (hs : ∀ e, e < n → H.stored e < horizon H W n checks → S e = true)
    (hu : ∀ e, e < n → H.stored e < horizon H W n checks → U e = true)
    (otherChecks : Nat → Bool) (writes : OldKeyObjects.Writes) (w : Nat) (present : X w = Y w)
    (settled : (writes w).stored ≤ horizon H W n checks + W) :
    OldKeyObjects.countInReplay H W n S otherChecks writes X w =
      OldKeyObjects.countInReplay H W n U otherChecks writes Y w := by
  simp only [OldKeyObjects.countInReplay, OldKeyObjects.counts, present,
    horizon_finalizes_window H W n checks S U past hs hu (writes w) settled]

end CovenStorelog.CurrentReplay.Keyed.Final
