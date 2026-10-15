import CovenIO.Refinement
import CovenIO.AudienceRetention
import CovenIO.Retired
import CovenStorelogData.StorageFinality

namespace CovenIO.ListingFinality
open CovenStorelog

/-- Includes late entries that replay drops; nothing newer is required. -/
def CompleteThrough (H : Finality.History) (bound time : Nat) (received : EntrySet) : Prop :=
  ∀ e, e < bound → H.stored e ≤ time → received e = true

/-- Ordered immutable publication supplies entry 1 and every preceding number.
Only entry 1 needs listing visibility; exact reads supply the remaining prefix.
A retained complete prefix, including a drained writer's, needs no new scan. -/
theorem completed_prefix (H : Finality.History) (bound clockTime unit : Nat)
    (positive : 0 < unit) (enough : listingMargin + unit ≤ clockTime)
    (listed future : Provider) (afterClock : clockTime ≤ listed.time)
    (run : ProviderRun listed future) (paths : List Path)
    (conforms : ListingComplete listed .deviceEntries paths)
    (states : Nat → Provider) (writer number start first finish last : Nat → Nat)
    (reads : Nat → List ReadEvent) (received : EntrySet)
    (numbers : ∀ e, e < bound → 0 < number e)
    (publishedPrefix : ∀ e, e < bound → ∀ n, 0 < n → n ≤ number e → ∃ o,
      future.objects (.log .entry (writer e) n) = some o ∧ o.value.storedAt ≤ H.stored e)
    (discovered : ∀ w, .log .entry w 1 ∈ paths →
      (∀ e, e < bound → writer e = w → H.stored e ≤ observationTime clockTime unit →
        number e ≤ first w) ∨
      (Scan (fun t => (states t).objects) .entry w (start w) (first w) (finish w) (last w) (reads w) ∧
        ∀ t, start w ≤ t → ProviderRun listed (states t)))
    (cached : ∀ e, e < bound → number e ≤ first (writer e) → received e = true)
    (consumed : ∀ e, e < bound → ∀ time o,
      ⟨time, .log .entry (writer e) (number e), some o⟩ ∈ reads (writer e) → received e = true) :
    CompleteThrough H bound (observationTime clockTime unit) received := by
  intro e he old
  have cutoff := observation_before_listing clockTime unit positive enough
  have present (n : Nat) (hn : 0 < n) (needed : n ≤ number e) :
      ∃ o, listed.objects (.log .entry (writer e) n) = some o ∧ o.value.storedAt ≤ H.stored e := by
    obtain ⟨o, found, age⟩ := publishedPrefix e he n hn needed
    exact ⟨o, old_immutable_present run _ o rfl found (by omega), age⟩
  obtain ⟨registration, registered, age⟩ := present 1 (by omega) (numbers e he)
  have known := conforms _ registration rfl registered (by omega)
  by_cases retained : number e ≤ first (writer e)
  · exact cached e he retained
  · rcases discovered _ known with complete | ⟨scan, evolves⟩
    · exact False.elim (retained (complete e he rfl old))
    have available : Available (fun t => (states t).objects) .entry (writer e)
        (start (writer e)) (finish (writer e)) (first (writer e)) (number e) := by
      intro t ht _ n hn needed
      obtain ⟨o, found, _⟩ := present n (by omega) needed
      exact ⟨o, permanent_retained (evolves t ht) _ o rfl found⟩
    obtain ⟨time, o, read, _⟩ := delivery_from_pass scan (number e)
      available (number e) (by omega) (Nat.le_refl _)
    exact consumed e he time o read

/-- The actual returned metadata includes a listed old registration. -/
theorem registration_in_response (s : Provider) (paths : List Path) (writer : Nat) (o : Object)
    (conforms : ListingComplete s .deviceEntries paths)
    (present : s.objects (.log .entry writer 1) = some o)
    (old : o.value.storedAt < s.time - listingMargin) :
    (.log .entry writer 1, o.metadata) ∈ paths.filterMap (fun p =>
      if Folder.deviceEntries.contains p then (s.objects p).map (fun v => (p, v.metadata)) else none) := by
  apply List.mem_filterMap.mpr
  exact ⟨_, conforms _ o rfl present old, by simp [Folder.contains, present]⟩

/-- The receiver uses the horizon's inclusive window and recorded-read test. -/
def observedQuiet (H : Finality.History) (window bound time : Nat) (received : EntrySet) : Bool :=
  (List.range bound).all fun e =>
    !(received e && decide (time ≤ H.stored e + window) && decide (H.stored e ≤ time)) ||
      !Finality.late H bound received e

theorem late_agrees (H : Finality.History) (bound time : Nat) (received : EntrySet)
    (complete : CompleteThrough H bound time received) (e : Nat) (old : H.stored e ≤ time) :
    Finality.late H bound received e = Finality.late H bound (fun _ => true) e := by
  unfold Finality.late
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true]
  constructor
  · rintro ⟨a, ha, hit⟩
    refine ⟨a, ha, ?_⟩
    simp only [Bool.and_eq_true, Bool.true_and, decide_eq_true_eq, Bool.not_eq_true'] at hit ⊢
    exact ⟨hit.1.2, hit.2⟩
  · rintro ⟨a, ha, hit⟩
    refine ⟨a, ha, ?_⟩
    have earlier : H.stored a < H.stored e := by simpa only [Bool.true_and,
      Bool.and_eq_true, decide_eq_true_eq, Bool.not_eq_true'] using (Bool.and_eq_true_iff.mp hit).1
    simpa [complete a (List.mem_range.mp ha) (by omega)] using hit

theorem quiet_agrees (H : Finality.History) (window bound time : Nat) (received : EntrySet)
    (complete : CompleteThrough H bound time received) :
    observedQuiet H window bound time received = Horizon.quiet H window bound time := by
  apply Bool.eq_iff_iff.mpr
  simp only [observedQuiet, Horizon.quiet, List.all_eq_true]
  have agrees (e : Nat) (he : e ∈ List.range bound) :
      (!(received e && decide (time ≤ H.stored e + window) && decide (H.stored e ≤ time)) ||
        !Finality.late H bound received e) =
      (!(decide (time ≤ H.stored e + window) && decide (H.stored e ≤ time)) ||
        !Finality.late H bound (fun _ => true) e) := by
    by_cases old : H.stored e ≤ time
    · simp [old, complete e (List.mem_range.mp he) old, late_agrees H bound time received complete e old]
    · simp [old]
  constructor <;> intro h e he
  · rw [← agrees e he]; exact h e he
  · rw [agrees e he]; exact h e he

theorem complete_old (H : Finality.History) (window bound time : Nat) (received : EntrySet)
    (complete : CompleteThrough H bound time received) :
    Horizon.CompleteOld H window bound time received := by
  intro e he old
  have age : H.stored e + window < time := of_decide_eq_true old
  exact complete e he (by omega)

/-- The actual current replay keeps the same dispositions at every later
receipt set containing the certified prefix, under causal validity. -/
theorem finality_safe (H : CurrentReplay.History) (window bound clockTime unit : Nat)
    (received later : EntrySet) (valid : CurrentReplay.Valid H.log bound)
    (complete : CompleteThrough H.membership bound (observationTime clockTime unit) received)
    (retained : ∀ e, received e = true → later e = true)
    (quiet : observedQuiet H.membership window bound (observationTime clockTime unit) received = true)
    (e : Nat) (he : e < bound)
    (old : Horizon.old H.membership window (observationTime clockTime unit) e = true) :
    (e ∈ (CurrentReplay.resolve H window bound received).kept ↔
      e ∈ (CurrentReplay.resolve H window bound later).kept) ∧
    (e ∈ (CurrentReplay.resolve H window bound received).dropped ↔
      e ∈ (CurrentReplay.resolve H window bound later).dropped) := by
  have fullQuiet := (quiet_agrees H.membership window bound _ received complete) ▸ quiet
  have hs := complete_old H.membership window bound _ received complete
  exact Horizon.current_stability H.membership window bound _ received later valid fullQuiet hs
    (fun e he old => retained e (hs e he old)) he old

/-- Qualification holds in the whole history, including omitted future entries. -/
theorem qualifying_window (H : Finality.History) (window bound clockTime unit : Nat)
    (received : EntrySet)
    (complete : CompleteThrough H bound (observationTime clockTime unit) received)
    (quiet : observedQuiet H window bound (observationTime clockTime unit) received = true)
    (e : Nat) (old : H.stored e < observationTime clockTime unit - window) :
    Horizon.quiet H window bound (observationTime clockTime unit) = true ∧
    H.stored e + window + listingMargin + unit < clockTime := by
  refine ⟨(quiet_agrees H window bound _ received complete) ▸ quiet, ?_⟩
  unfold observationTime at old
  omega

theorem at_or_after_horizon_not_final (H : Finality.History) (window bound : Nat)
    (checks : List Nat) (e : Nat) (new : Horizon.horizon H window bound checks ≤ H.stored e) :
    Horizon.finalSet H window bound checks e = false := by
  cases h : Horizon.finalSet H window bound checks e
  · rfl
  · have := ((Horizon.final_iff_before_horizon H window bound checks e).mp h).2
    omega

theorem underflow_no_final (H : Finality.History) (window bound clockTime unit : Nat)
    (underflow : clockTime < listingMargin + unit) (e : Nat) :
    Horizon.finalSet H window bound [observationTime clockTime unit] e = false := by
  have zero : observationTime clockTime unit = 0 := by unfold observationTime; omega
  simp [Horizon.finalSet, Horizon.old, zero]

/-- Age thresholds at the endpoint shift by the margin and provider unit. -/
theorem shifted_age (stored duration clockTime unit : Nat) :
    stored + duration < observationTime clockTime unit ↔
      stored + duration + listingMargin + unit < clockTime := by
  unfold observationTime
  omega

/-- The data-retention certificate uses the same complete inclusive prefix. -/
theorem storage_complete (H : CurrentReplay.History) (bound time : Nat) (received : EntrySet)
    (complete : CompleteThrough H.membership bound time received) :
    CovenStorelogData.StorageFinality.completeThrough H bound time received = true := by
  apply List.all_eq_true.mpr
  intro e he
  by_cases old : H.stored e ≤ time
  · simp [old, complete e (List.mem_range.mp he) old]
  · simp [old]

/-- Once every dependency passes the shifted age and quiet-window tests,
the existing retention bound leaves exactly the inputs still required. -/
theorem retained_inputs_bound (H : CurrentReplay.History) (window bound clockTime unit : Nat)
    (received : EntrySet)
    (complete : CompleteThrough H.membership bound (observationTime clockTime unit) received)
    (quiet : observedQuiet H.membership window bound (observationTime clockTime unit) received = true)
    (inputs : List CovenStorelogData.RetentionSafety.Input)
    (dependencies : ∀ input ∈ inputs, ∀ e ∈ input.needed.entries,
      e < bound ∧ H.stored e + window + listingMargin + unit < clockTime)
    (otherChecks : ∀ input ∈ inputs, input.checks.all (·.met) = true) :
    CovenStorelogData.RetentionSafety.retain
      (fun e => decide (e ∈ (CurrentReplay.resolve H window bound received).kept))
      (CovenStorelogData.StorageFinality.certified H window bound
        (observationTime clockTime unit) received) inputs =
    inputs.filter (fun input => input.needed.eval
      (fun e => decide (e ∈ (CurrentReplay.resolve H window bound received).kept))) := by
  have fullQuiet := (quiet_agrees H.membership window bound _ received complete) ▸ quiet
  apply CovenStorelogData.StorageFinality.bounded_storage H window bound _ received
    fullQuiet
    (storage_complete H bound _ received complete) inputs ?_ otherChecks
  intro input present e needed
  have h := dependencies input present e needed
  exact ⟨h.1, (shifted_age (H.stored e) window clockTime unit).mpr h.2⟩

/-- The same endpoint needs exactly M more storage-time units than the
preceding-provider-unit rule. This is not a promise about pass scheduling. -/
theorem sample_shift (clockTime unit : Nat) :
    observationTime (clockTime + listingMargin) unit = clockTime - unit := by
  unfold observationTime
  omega

/-- Conditional on no further late entry; the margin does not erase concurrency. -/
theorem quiet_after_last_late (H : Finality.History) (window bound lastLate clockTime unit : Nat)
    (last : ∀ e, e < bound → Finality.late H bound (fun _ => true) e = true → H.stored e ≤ lastLate)
    (after : lastLate + window + listingMargin + unit < clockTime) :
    Horizon.quiet H window bound (observationTime clockTime unit) = true := by
  apply List.all_eq_true.mpr
  intro e he
  cases late : Finality.late H bound (fun _ => true) e
  · simp
  · have age := last e (List.mem_range.mp he) late
    have endTime := (shifted_age lastLate window clockTime unit).mpr after
    have outside : ¬ observationTime clockTime unit ≤ H.stored e + window := by omega
    simp [outside]

/-- An hour added to the finality gate adds at most an hour to a witnessed
write's deadline. Witness maturity and service durations are unchanged. -/
theorem witnessed_deadline_shift (gated witness passGap upload latency : Nat) :
    max (gated + listingMargin) (witness + day + 1) + 3 * passGap + upload + latency ≤
      max gated (witness + day + 1) + 3 * passGap + upload + latency + listingMargin := by
  omega

theorem audience_ready_shift (w : Write) (gated online : Nat) :
    AudienceRetention.ready w (gated + listingMargin) online ≤
      AudienceRetention.ready w gated online + listingMargin := by
  unfold AudienceRetention.ready
  omega

theorem audience_deadline_shift (w : Write) (gated online passGap upload : Nat) :
    AudienceRetention.ready w (gated + listingMargin) online +
        4 * passGap + 2 * upload + day + 1 ≤
      AudienceRetention.ready w gated online + 4 * passGap + 2 * upload + day + 1 + listingMargin := by
  have := audience_ready_shift w gated online
  omega

theorem witness_bound_shift (w : Write) (gated passGap upload : Nat) (online : Nat → Nat) :
    AudienceRetention.witnessBound w (gated + listingMargin) online passGap upload ≤
      AudienceRetention.witnessBound w gated online passGap upload + listingMargin := by
  apply CommonReload.maximum_le
  intro a ha
  have bound := CommonReload.maximum_member w.holders
    (fun a => AudienceRetention.ready w gated (online a) + 2 * passGap + upload) a ha
  have ready := audience_ready_shift w gated (online a)
  change AudienceRetention.ready w (gated + listingMargin) (online a) + 2 * passGap + upload ≤ _
  unfold AudienceRetention.witnessBound
  omega

theorem storage_deadline_shift (w : Write) (gated passGap upload latency : Nat) (online : Nat → Nat) :
    max (gated + listingMargin)
        (AudienceRetention.witnessBound w (gated + listingMargin) online passGap upload + day + 1) +
        3 * passGap + upload + latency ≤
      max gated (AudienceRetention.witnessBound w gated online passGap upload + day + 1) +
        3 * passGap + upload + latency + listingMargin := by
  have := witness_bound_shift w gated passGap upload online
  omega

theorem first_witness_deadline_shift (w : Write) (gated online passGap upload : Nat) :
    AudienceRetention.ready w (gated + listingMargin) online + 2 * passGap + upload ≤
      AudienceRetention.ready w gated online + 2 * passGap + upload + listingMargin := by
  have := audience_ready_shift w gated online
  omega

theorem covered_deadline_shift (gated witness passGap latency : Nat) :
    max (gated + listingMargin) (witness + day + 1) + passGap + latency ≤
      max gated (witness + day + 1) + passGap + latency + listingMargin := by omega

/-- A drain's wait still begins when finality is established, using the
existing shared-duration and bounded-publication hypotheses. -/
theorem retired_publication_shift (finalAt : Nat) (p : Retired.Publication (finalAt + listingMargin)) :
    p.landed < finalAt + freshFor + day + listingMargin := by
  have := Retired.publication_before_deadline (finalAt + listingMargin) p
  omega

end CovenIO.ListingFinality
