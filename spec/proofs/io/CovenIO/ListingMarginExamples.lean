import CovenIO.ListingFinality

namespace CovenIO.ListingMarginExamples
open CovenStorelog

set_option maxRecDepth 32768

def margin : Nat := listingMargin

/-- Creation, then two concurrent resets. Both authors read creation before
either reset landed. Storage durations are seconds; entry ids give author order. -/
def history : CurrentReplay.History where
  log := fun e => if e = 0 then
      ⟨0, 0, [], .create "initial" ⟨0, Vector.replicate 32 0⟩⟩
    else ⟨0, e, [0], .reset ⟨.store, e⟩⟩
  stored := fun e => e * day
  attempted := fun e => if e = 0 then 0 else day

/-- The strict horizon first includes the second reset one second after
its thirty-day age, plus the one-hour listing margin and one provider tick. -/
def sample : Nat := 32 * day + margin + 2

def initial : Provider := ⟨empty, 0⟩
def created : Provider := ⟨put empty (.log .entry 0 1) [0] 0, 0⟩
def beforeFirst : Provider := { created with time := day }
def first : Provider :=
  { beforeFirst with objects := put beforeFirst.objects (.log .entry 1 1) [1] day }
def beforeSecond : Provider := { first with time := 2 * day }
def second : Provider :=
  { beforeSecond with objects := put beforeSecond.objects (.log .entry 2 1) [2] (2 * day) }
def sampled : Provider := { second with time := sample }

theorem provider_history : ProviderRun initial sampled := by
  have root : ProviderRun initial created :=
    .step (.refl initial) (.create initial (.log .entry 0 1) [0])
  have beforeA : ProviderRun initial beforeFirst :=
    .step root (.tick created day (by decide))
  have afterA : ProviderRun initial first :=
    .step beforeA (.create beforeFirst (.log .entry 1 1) [1])
  have beforeB : ProviderRun initial beforeSecond :=
    .step afterA (.tick first (2 * day) (by decide))
  have afterB : ProviderRun initial second :=
    .step beforeB (.create beforeSecond (.log .entry 2 1) [2])
  exact .step afterB (.tick second sample (by decide))

theorem entries_represented :
    ∀ e : Fin 3, sampled.objects (.log .entry e 1) =
      some ⟨⟨[e.val], history.stored e⟩, 0⟩ := by decide

theorem causal_history : CurrentReplay.Valid history.log 3 :=
  validCheck_sound _ _ (by decide)

theorem online_attempts :
    (∀ e : Fin 3, history.attempted e ≤ history.stored e) ∧
    (∀ e a : Fin 3, hadRead history.membership.log e a = true ↔
      history.stored a < history.attempted e) := by decide

theorem causal_receipts :
    CausalOrder history.membership.log [0, 2] ∧
    CausalOrder history.membership.log [0, 2, 1] := by
  constructor <;> exact causalCheck_sound _ .nil (by decide)

/-- A successful response omits the first reset's device, while exact GETs
remain correct. Its age violates even the strict-before listing requirement. -/
theorem missing_old_folder :
    observe sampled.objects [.log .entry 0 1, .log .entry 2 1] (.list .deviceEntries) =
      .listing [(.log .entry 0 1, ⟨1, 0, 0⟩), (.log .entry 2 1, ⟨1, 2 * day, 0⟩)] ∧
    history.stored 1 < observationTime sample 1 ∧
    observe sampled.objects [] (.get (.log .entry 1 1)) =
      .object (some ⟨⟨[1], day⟩, 0⟩) ∧
    observe sampled.objects [] (.get (.log .entry 2 2)) = .object none := by decide

/-- The missing entry is in the prefix required by the actual horizon
stability theorem, even though the recent window really is quiet. -/
theorem missing_required_prefix :
    ¬ Horizon.CompleteOld history.membership month 3
      (observationTime sample 1) (entrySet [0, 2]) := by
  intro complete
  have absent := complete 1 (by decide) (by decide)
  contradiction

/-- A listing-contract violation can reverse a final result, not merely
delay the next horizon. Neither reset fails the thirty-day landing rule. -/
theorem listing_violation_reverses_final :
    observationTime sample 1 = 32 * day + 1 ∧
    Horizon.quiet history.membership month 3 (observationTime sample 1) = true ∧
    Horizon.horizon history.membership month 3 [observationTime sample 1] = 2 * day + 1 ∧
    Horizon.finalSet history.membership month 3 [observationTime sample 1] 2 = true ∧
    Finality.tooLate history.membership month 3 (fun _ => true) 1 = false ∧
    Finality.tooLate history.membership month 3 (fun _ => true) 2 = false ∧
    2 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 2])).kept ∧
    2 ∉ (CurrentReplay.resolve history month 3 (entrySet [0, 2, 1])).kept ∧
    2 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 2, 1])).dropped := by decide

example :
    Horizon.finalSet history.membership month 3 [observationTime sample 1] 2 = true ∧
    2 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 2])).kept ∧
    2 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 2, 1])).dropped := by decide

namespace Conforming

/-- The second registration starts after the first has landed. Its folder
listing still omits the first device, so its recorded read contains creation. -/
def history : CurrentReplay.History where
  log := fun e => if e = 0 then ListingMarginExamples.history.log 0
    else ⟨0, e, [0], .addDevice 0 e⟩
  stored := fun e => if e = 0 then 0 else margin + e
  attempted := fun e => if e = 0 then 0 else margin + e

def beforeFirst : Provider := { created with time := margin + 1 }
def first : Provider :=
  { beforeFirst with objects := put beforeFirst.objects (.log .entry 1 1) [1] (margin + 1) }
def author : Provider := { first with time := margin + 2 }
def second : Provider :=
  { author with objects := put author.objects (.log .entry 2 1) [2] (margin + 2) }
def sampled : Provider := { second with time := margin + 3 }
def discovered : Provider := { second with time := 2 * margin + 3 }

def paths : List Path := [.log .entry 0 1, .log .entry 1 1, .log .entry 2 1]

/-- Every listing delays visibility by exactly five seconds. Exact GETs
continue to use the provider's objects directly. -/
def listed (s : Provider) : List Path :=
  paths.filter fun p => (s.objects p).any fun o => decide (o.value.storedAt + 5 ≤ s.time)

def Conforms (s : Provider) : Prop := ListingComplete s .deviceEntries (listed s)

/-- This listing rule meets the one-hour requirement whenever the three
paths enumerate the stored objects, at every provider time. -/
theorem listing_conforms (s : Provider)
    (inventory : ∀ p o, s.objects p = some o → p ∈ paths) : Conforms s := by
  intro p o _ present old
  have visible : o.value.storedAt + 5 ≤ s.time := by
    unfold listingMargin at old
    omega
  simp [listed, inventory p o present, present, visible]

theorem provider_history :
    ProviderRun initial author ∧ ProviderRun author sampled ∧ ProviderRun sampled discovered := by
  have root : ProviderRun initial created :=
    .step (.refl initial) (.create initial (.log .entry 0 1) [0])
  have beforeA : ProviderRun initial beforeFirst :=
    .step root (.tick created (margin + 1) (by decide))
  have afterA : ProviderRun initial first :=
    .step beforeA (.create beforeFirst (.log .entry 1 1) [1])
  have atAuthor : ProviderRun initial author :=
    .step afterA (.tick first (margin + 2) (by decide))
  have afterB : ProviderRun author second :=
    .step (.refl author) (.create author (.log .entry 2 1) [2])
  exact ⟨atAuthor, .step afterB (.tick second (margin + 3) (by decide)),
    .step (.refl sampled) (.tick sampled (2 * margin + 3) (by decide))⟩

theorem conforming_observations : Conforms author ∧ Conforms sampled ∧ Conforms discovered := by
  have inventory (time : Nat) :
      ∀ p o, ({ second with time := time } : Provider).objects p = some o → p ∈ paths := by
    intro p o present
    simp only [second, author, first, beforeFirst, created, put, empty] at present
    split at present
    · simp_all [paths]
    · split at present
      · simp_all [paths]
      · split at present
        · simp_all [paths]
        · simp at present
  refine ⟨listing_conforms author ?_, listing_conforms sampled (inventory _),
    listing_conforms discovered (inventory _)⟩
  intro p o present
  apply inventory author.time p o
  exact create_once _ _ _ _ _ _ present

theorem causal_history : CurrentReplay.Valid history.log 3 :=
  validCheck_sound _ _ (by decide)

theorem entries_represented :
    (∀ e : Fin 3, discovered.objects (.log .entry e 1) =
      some ⟨⟨[e.val], history.stored e⟩, 0⟩) ∧
    (∀ e : Fin 3, history.attempted e ≤ history.stored e) := by decide

/-- The author reads creation and reaches its exact next-number miss.
The earlier registration exists, but its device is absent from the listing. -/
theorem author_catchup :
    observe author.objects (listed author) (.list .deviceEntries) =
      .listing [(.log .entry 0 1, ⟨1, 0, 0⟩)] ∧
    observe author.objects [] (.get (.log .entry 1 1)) =
      .object (some ⟨⟨[1], margin + 1⟩, 0⟩) ∧
    history.stored 1 < history.attempted 2 ∧
    hadRead history.membership.log 2 0 = true ∧
    hadRead history.membership.log 2 1 = false ∧
    Scan (fun _ => author.objects) .entry 0 0 0 1 1
      [⟨0, .log .entry 0 1, some ⟨⟨[0], 0⟩, 0⟩⟩, ⟨1, .log .entry 0 2, none⟩] := by
  refine ⟨by decide, by decide, by decide, by decide, by decide, ?_⟩
  exact .hit 0 0 _ (by decide) (.miss 1 1 (by decide))

/-- Both recent registrations may be absent from a conforming listing.
The reader returns an hour later, when both lie within its new prefix. -/
theorem missing_then_discovered :
    observe sampled.objects (listed sampled) (.list .deviceEntries) =
      .listing [(.log .entry 0 1, ⟨1, 0, 0⟩)] ∧
    observationTime sampled.time 1 < history.stored 2 ∧
    history.stored 2 ≤ observationTime discovered.time 1 ∧
    observe discovered.objects (listed discovered) (.list .deviceEntries) =
      .listing [(.log .entry 0 1, ⟨1, 0, 0⟩),
        (.log .entry 1 1, ⟨1, margin + 1, 0⟩), (.log .entry 2 1, ⟨1, margin + 2, 0⟩)] ∧
    observe discovered.objects [] (.get (.log .entry 2 1)) =
      .object (some ⟨⟨[2], margin + 2⟩, 0⟩) := by decide

/-- Subtracting the margin changes the observed interval, not the recorded
read test. The second registration is late; replay keeps both registrations. -/
theorem recent_omission_can_be_late :
    Finality.late history.membership 3 (entrySet [0, 1, 2]) 1 = false ∧
    Finality.late history.membership 3 (entrySet [0, 1, 2]) 2 = true ∧
    Finality.tooLate history.membership month 3 (entrySet [0, 1, 2]) 2 = false ∧
    1 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 1, 2])).kept ∧
    2 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 1, 2])).kept ∧
    Horizon.quiet history.membership month 3 (observationTime sampled.time 1) = true ∧
    Horizon.quiet history.membership month 3 (observationTime discovered.time 1) = false ∧
    Horizon.quiet history.membership month 3
      (observationTime (history.stored 2 + margin + 1) 1) = false ∧
    Horizon.quiet history.membership month 3
      (observationTime (history.stored 2 + month + margin + 1) 1) = false ∧
    Horizon.quiet history.membership month 3
      (observationTime (history.stored 2 + month + margin + 2) 1) = true := by decide

example : Conforms sampled ∧ Conforms discovered ∧
    .log .entry 2 1 ∉ listed sampled ∧
    observationTime sampled.time 1 < history.stored 2 ∧
    .log .entry 2 1 ∈ listed discovered ∧
    history.stored 2 ≤ observationTime discovered.time 1 ∧
    Finality.late history.membership 3 (entrySet [0, 1, 2]) 2 = true := by
  exact ⟨conforming_observations.2.1, conforming_observations.2.2, by decide,
    by decide, by decide, by decide, by decide⟩

/-- A five-second lag meets the hour bound but violates certification through
T minus one second: both absent registrations lie in that alleged prefix. -/
theorem one_tick_misses_prefix :
    Conforms sampled ∧ .log .entry 2 1 ∉ listed sampled ∧
    history.stored 2 ≤ sampled.time - 1 ∧
    observationTime sampled.time 1 < history.stored 2 := by
  exact ⟨conforming_observations.2.1, by decide, by decide, by decide⟩

end Conforming

namespace Endpoint

/-- Creation at zero, registration A at one, and registration B at the first
possible endpoint certifying A. B's fixed attempt did not read A. -/
def history : CurrentReplay.History where
  log := Conforming.history.log
  stored := fun e => if e = 0 then 0 else if e = 1 then 1 else month + 2
  attempted := fun e => if e = 0 then 0 else 1

def cutoff : Nat := month + 2
def beforeFirst : Provider := { created with time := 1 }
def first : Provider :=
  { beforeFirst with objects := put beforeFirst.objects (.log .entry 1 1) [1] 1 }
def beforeSecond : Provider := { first with time := cutoff }
def second : Provider :=
  { beforeSecond with objects := put beforeSecond.objects (.log .entry 2 1) [2] cutoff }
def sampled : Provider := { second with time := cutoff + margin }

/-- The contract says strictly more than an hour old. At equality the
object may still be omitted, including when the clock and listing share a tick. -/
def listed (s : Provider) : List Path :=
  Conforming.paths.filter fun p =>
    (s.objects p).any fun o => decide (o.value.storedAt + margin < s.time)

theorem listing_conforms (s : Provider)
    (inventory : ∀ p o, s.objects p = some o → p ∈ Conforming.paths) :
    ListingComplete s .deviceEntries (listed s) := by
  intro p o _ present old
  have visible : o.value.storedAt + margin < s.time := by
    unfold margin
    omega
  simp [listed, inventory p o present, present, visible]

theorem provider_history : ProviderRun initial sampled := by
  have root : ProviderRun initial created :=
    .step (.refl initial) (.create initial (.log .entry 0 1) [0])
  have beforeA : ProviderRun initial beforeFirst :=
    .step root (.tick created 1 (by decide))
  have afterA : ProviderRun initial first :=
    .step beforeA (.create beforeFirst (.log .entry 1 1) [1])
  have beforeB : ProviderRun initial beforeSecond :=
    .step afterA (.tick first cutoff (by decide))
  have afterB : ProviderRun initial second :=
    .step beforeB (.create beforeSecond (.log .entry 2 1) [2])
  exact .step afterB (.tick second (cutoff + margin) (by decide))

theorem conforming_observation :
    ListingComplete sampled .deviceEntries (listed sampled) := by
  apply listing_conforms
  intro p o present
  simp only [sampled, second, beforeSecond, first, beforeFirst, created, put, empty] at present
  split at present
  · simp_all [Conforming.paths]
  · split at present
    · simp_all [Conforming.paths]
    · split at present
      · simp_all [Conforming.paths]
      · simp at present

theorem causal_history : CurrentReplay.Valid history.log 3 :=
  Conforming.causal_history

theorem entries_represented :
    (∀ e : Fin 3, sampled.objects (.log .entry e 1) =
      some ⟨⟨[e.val], history.stored e⟩, 0⟩) ∧
    (∀ e : Fin 3, history.attempted e ≤ history.stored e) := by decide

/-- The listing and completed exact scans expose creation and A. B is a
new writer at exactly the uncertified endpoint; its exact GET would succeed. -/
theorem observed_prefix :
    (sampled.time - margin) = cutoff ∧
    observe sampled.objects (listed sampled) (.list .deviceEntries) =
      .listing [(.log .entry 0 1, ⟨1, 0, 0⟩), (.log .entry 1 1, ⟨1, 1, 0⟩)] ∧
    history.stored 2 = cutoff ∧
    observe sampled.objects [] (.get (.log .entry 2 1)) =
      .object (some ⟨⟨[2], cutoff⟩, 0⟩) ∧
    (∀ e : Fin 3, history.stored e < cutoff → entrySet [0, 1] e = true) ∧
    (∀ w : Fin 2, Scan (fun _ => sampled.objects) .entry w 0 0 1 1
      [⟨0, .log .entry w 1, some ⟨⟨[w.val], history.stored w⟩, 0⟩⟩,
        ⟨1, .log .entry w 2, none⟩]) := by
  refine ⟨by decide, by decide, by decide, by decide, by decide, ?_⟩
  intro w
  have hit : sampled.objects (.log .entry w 1) =
      some ⟨⟨[w.val], history.stored w⟩, 0⟩ := by
    have cases : w.val = 0 ∨ w.val = 1 := by omega
    rcases cases with h | h <;> simp [h] <;> decide
  exact .hit 0 0 _ hit (.miss 1 1 (by
    have cases : w.val = 0 ∨ w.val = 1 := by omega
    rcases cases with h | h <;> simp [h] <;> decide))

/-- The received history certifies A, although the inclusive window in the
whole history is not quiet. Dropped late entries still prevent qualification. -/
theorem uncertified_window :
    Horizon.quiet history.membership month 2 cutoff = true ∧
    Horizon.horizon history.membership month 2 [cutoff] = 2 ∧
    Horizon.finalSet history.membership month 2 [cutoff] 1 = true ∧
    Finality.late history.membership 3 (fun _ => true) 2 = true ∧
    Finality.tooLate history.membership month 3 (fun _ => true) 2 = true ∧
    Horizon.quiet history.membership month 3 cutoff = false ∧
    Horizon.finalSet history.membership month 3 [cutoff] 1 = false := by decide

/-- Retrospective window recovery cannot justify the observed certificate:
every earlier endpoint leaves A outside the strict final prefix. -/
theorem no_earlier_qualifying_window (t : Nat) (before : t ≤ cutoff)
    (old : Horizon.old history.membership month t 1 = true) :
    Horizon.quiet history.membership month 3 t = false := by
  have age : 1 + month < t := by
    simpa [Horizon.old, history, CurrentReplay.History.membership] using old
  have same : t = cutoff := by unfold cutoff at *; omega
  subst t
  decide

/-- This endpoint mismatch concerns qualification, not a reversal of A:
B is permanently too late to change A's kept result. -/
theorem replay_unchanged :
    1 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 1])).kept ∧
    1 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 1, 2])).kept ∧
    2 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 1, 2])).dropped := by decide

example :
    ListingComplete sampled .deviceEntries (listed sampled) ∧
    Horizon.finalSet history.membership month 2 [cutoff] 1 = true ∧
    ¬ ∃ t, t ≤ cutoff ∧ Horizon.old history.membership month t 1 = true ∧
      Horizon.quiet history.membership month 3 t = true := by
  refine ⟨conforming_observation, by decide, ?_⟩
  rintro ⟨t, before, old, quiet⟩
  have notQuiet := no_earlier_qualifying_window t before old
  simp [notQuiet] at quiet

/-- Instantiate the listing-to-receipt theorem with the executed endpoint
scans. The unlisted writer has no fabricated read trace. -/
theorem revised_prefix_from_scans :
    ListingFinality.CompleteThrough history.membership 3
      (observationTime sampled.time 1) (entrySet [0, 1]) := by
  let reads : Nat → List ReadEvent := fun w => if w < 2 then
      [⟨0, .log .entry w 1, some ⟨⟨[w], history.stored w⟩, 0⟩⟩,
       ⟨1, .log .entry w 2, none⟩] else []
  apply ListingFinality.completed_prefix history.membership 3 sampled.time 1
    (by decide) (by decide) sampled sampled (Nat.le_refl _) (.refl _)
    (listed sampled) conforming_observation (fun _ => sampled) id (fun _ => 1)
    (fun _ => 0) (fun _ => 0) (fun _ => 1) (fun _ => 1) reads (entrySet [0, 1])
  · intros; decide
  · intro e he n positive needed
    have number : n = 1 := by omega
    subst n
    exact ⟨_, entries_represented.1 ⟨e, he⟩, Nat.le_refl _⟩
  · intro w known
    apply Or.inr
    refine ⟨?_, ?_⟩
    · have writers : listed sampled = [.log .entry 0 1, .log .entry 1 1] := by decide
      have hw : w < 2 := by rw [writers] at known; simp at known; omega
      simpa [reads, hw] using observed_prefix.2.2.2.2.2 ⟨w, hw⟩
    · intros; exact .refl _
  · intro e he impossible; omega
  · intro e he time o read
    have cases : e = 0 ∨ e = 1 ∨ e = 2 := by omega
    rcases cases with rfl | rfl | rfl
    · decide
    · decide
    · simp [reads] at read

/-- The same prefix can come entirely from retained receipts. There are no
new reads, including for writers whose complete drained history is retained. -/
theorem revised_prefix_from_retained :
    ListingFinality.CompleteThrough history.membership 3
      (observationTime sampled.time 1) (entrySet [0, 1]) := by
  let first : Nat → Nat := fun w => if w < 2 then 1 else 0
  apply ListingFinality.completed_prefix history.membership 3 sampled.time 1
    (by decide) (by decide) sampled sampled (Nat.le_refl _) (.refl _)
    (listed sampled) conforming_observation (fun _ => sampled) id (fun _ => 1)
    (fun _ => 0) first (fun _ => 0) first (fun _ => []) (entrySet [0, 1])
  · intros; decide
  · intro e he n positive needed
    have number : n = 1 := by omega
    subst n
    exact ⟨_, entries_represented.1 ⟨e, he⟩, Nat.le_refl _⟩
  · intro w known
    apply Or.inl
    intro _ _ _ _
    have writers : listed sampled = [.log .entry 0 1, .log .entry 1 1] := by decide
    have hw : w < 2 := by rw [writers] at known; simp at known; omega
    simp [first, hw]
  · intro e _ cached
    have oldWriter : e < 2 := by
      by_cases h : e < 2
      · exact h
      · simp [first, h] at cached
    have cases : e = 0 ∨ e = 1 := by omega
    rcases cases with rfl | rfl <;> decide
  · intro _ _ _ _ read
    simp at read

/-- The extra provider tick removes the uncertified endpoint. A is exactly
thirty days old at this observation and therefore remains outside the horizon. -/
theorem revised_endpoint_safe :
    observationTime sampled.time 1 = cutoff - 1 ∧
    ListingFinality.CompleteThrough history.membership 3
      (observationTime sampled.time 1) (entrySet [0, 1]) ∧
    ListingFinality.observedQuiet history.membership month 3
      (observationTime sampled.time 1) (entrySet [0, 1]) = true ∧
    Horizon.quiet history.membership month 3 (observationTime sampled.time 1) = true ∧
    Horizon.finalSet history.membership month 3 [observationTime sampled.time 1] 1 = false := by
  exact ⟨by decide, revised_prefix_from_scans, by decide, by decide, by decide⟩

/-- Even the omitted future late entry cannot change the certified creation. -/
theorem revised_replay_stable :
    (0 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 1])).kept ↔
      0 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 1, 2])).kept) ∧
    (0 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 1])).dropped ↔
      0 ∈ (CurrentReplay.resolve history month 3 (entrySet [0, 1, 2])).dropped) := by
  apply ListingFinality.finality_safe history month 3 sampled.time 1
    (entrySet [0, 1]) (entrySet [0, 1, 2]) causal_history revised_prefix_from_scans
    ?_ (by decide) 0 (by decide) (by decide)
  intro e received
  simp [entrySet] at received ⊢
  rcases received with rfl | rfl <;> simp

/-- On the next tick B must be listed and its late verdict blocks the window. -/
theorem next_tick_sees_late :
    .log .entry 2 1 ∈ listed { sampled with time := sampled.time + 1 } ∧
    observationTime (sampled.time + 1) 1 = cutoff ∧
    Horizon.quiet history.membership month 3
      (observationTime (sampled.time + 1) 1) = false := by decide

end Endpoint

end CovenIO.ListingMarginExamples
