import CovenIO.SnapshotTiming

/-! §15's preparation premises and the abandonment rule (T220).

`SnapshotPublication` has remote catch-up, preparation and landing actions
but no state of the preparing device. This module adds that device: the
newest storage time it observed, the listed snapshots whose prefix it cannot
verify, and its attempts, each active until confirmed counting or abandoned.
Its preparation runs the remote `prepare` action, so every result about the
publication model still applies.

- `known_prepare_is_global`, `unverified_newest_counterexample`: preparation
  waits while a listed snapshot this device has not verified might be its
  audience's newest. With the wait, the device's own check is §15's check
  against the true current newest; without it, a snapshot that dominates
  everything the device can read can still miss the true newest, which
  stays the selection and leaves the refresh useless.
- `stale_sample_never_counts`, `fresh_catchup_may_prepare`: a preparation
  whose sample is already more than a day old by the device's observation
  could never count, so it waits; a catch-up begun now samples a time that
  can.
- `abandoned_never_counts_later`, `abandon_enabled`, `retry_new_path`,
  `retry_counts`: the abandonment rule. An attempt is abandoned once an
  observed storage time is past its `T_c` plus a day, or after its landing
  was confirmed too late. A request still in flight that lands after the
  abandonment never counts; abandonment is always possible for an attempt
  that cannot count; the next attempt takes a path no earlier attempt used;
  and with a catch-up and upload completing within a day of the new sample,
  the retry lands counting and is confirmed. Bounded successful service is
  the explicit hypothesis, as §15 states. -/

namespace CovenIO.SnapshotAttempts

open SnapshotPublication

structure State where
  remote : SnapshotPublication.State
  /-- The newest provider time this device observed; never ahead of storage. -/
  observed : Nat
  /-- Listed snapshots whose prefix this device cannot verify: a newer
  format, an author whose registration has not applied, or a refusal this
  version makes (§15, §19.1). -/
  unverified : List Snapshot
  /-- Prepared attempts, neither confirmed counting nor abandoned. -/
  active : List Snapshot
  confirmed : List Snapshot
  /-- Abandoned attempts with the provider time of their abandonment. -/
  abandoned : List (Snapshot × Nat)

def initial (writers : List Nat) : State :=
  ⟨SnapshotPublication.initial writers, 0, [], [], [], []⟩

/-- The newest counting snapshot of an audience among those this device
verified. -/
def knownCurrent (order : SnapshotOrder) (s : State) : Current :=
  catalogCurrent order (s.remote.landed.filter fun x => !s.unverified.contains x)

/-- The device's own preparation check: it covers the newest counting
snapshot it can read. -/
def canPrepareKnown (order : SnapshotOrder) (s : State) (snap : Snapshot) : Bool :=
  match knownCurrent order s snap.audience with
  | none => true
  | some old => dominates snap old

/-- §15: preparation waits while a listed snapshot of the audience is
unverified, since it might be the newest. -/
def waitsForUnverified (s : State) (audience : Nat) : Bool :=
  s.unverified.any fun u => u.audience == audience

/-- The device can still count an attempt sampled at `caughtAt`: no storage
time it observed is past that day. -/
def mayCount (s : State) (caughtAt : Nat) : Bool := decide (s.observed ≤ caughtAt + day)

inductive Step (order : SnapshotOrder) : State → State → Prop
  /-- Every remote event other than preparation: writes landing, catch-up
  reads, requests landing, deletions, ticks. -/
  | remote (s : State) (time : Nat) (action : Action) (next : SnapshotPublication.State)
      (ok : SnapshotPublication.step order s.remote time action = some next)
      (other : ∀ w, action ≠ .prepare w) :
      Step order s { s with remote := next }
  /-- The device learns a provider time no later than storage's. -/
  | observe (s : State) (t : Nat) (seen : t ≤ s.remote.time) :
      Step order s { s with observed := max s.observed t }
  /-- A listed prefix this version cannot verify, or one an update verifies. -/
  | hide (s : State) (u : Snapshot) (listed : u ∈ s.remote.landed) :
      Step order s { s with unverified := u :: s.unverified }
  | reveal (s : State) (u : Snapshot) :
      Step order s { s with unverified := s.unverified.filter (· != u) }
  /-- Preparation, with the device's own check and both waits. One attempt
  per writer at a time. -/
  | prepare (s : State) (w : Nat) (c : Catchup) (next : SnapshotPublication.State)
      (found : s.remote.catching.find? (fun c => c.snapshot.writer == w) = some c)
      (ok : SnapshotPublication.step order s.remote s.remote.time (.prepare w) = some next)
      (single : ∀ a ∈ s.active, a.writer ≠ w)
      (known : waitsForUnverified s c.snapshot.audience = false)
      (fresh : mayCount s c.snapshot.caughtAt = true) :
      Step order s { s with
        remote := next
        active := { c.snapshot with storedAt := s.remote.time } :: s.active }
  /-- Its landing was observed and counts: publication confirmed. -/
  | confirm (s : State) (a : Snapshot) (active : a ∈ s.active) (l : Snapshot)
      (landed : l ∈ s.remote.landed) (same : samePath l a = true) (counts : l.counts = true) :
      Step order s { s with
        active := s.active.erase a
        confirmed := a :: s.confirmed }
  /-- Once an observed storage time is past its sample plus a day, or its
  landing was confirmed too late, the attempt is abandoned. Its request may
  still land. -/
  | abandon (s : State) (a : Snapshot) (active : a ∈ s.active)
      (passed : a.caughtAt + day < s.observed ∨
        ∃ l ∈ s.remote.landed, samePath l a = true ∧ l.counts = false) :
      Step order s { s with
        active := s.active.erase a
        abandoned := (a, s.remote.time) :: s.abandoned }

inductive Reach (order : SnapshotOrder) (writers : List Nat) : State → Prop
  | initial : Reach order writers (initial writers)
  | step {s t} : Reach order writers s → Step order s t → Reach order writers t

/-! ### The device never observes a time storage has not reached -/

theorem act_time (order : SnapshotOrder) (s t : SnapshotPublication.State) (action : Action)
    (taken : act order s action = some t) : t.time = s.time := by
  cases action <;> simp only [act, Option.bind_eq_bind, Option.bind, pure] at taken
  all_goals
    repeat' first | split at taken | dsimp only at taken
    all_goals cases taken <;> rfl

theorem step_time (order : SnapshotOrder) (s t : SnapshotPublication.State) (time : Nat)
    (action : Action) (taken : SnapshotPublication.step order s time action = some t) :
    s.time ≤ t.time ∧ t.time = time := by
  simp only [SnapshotPublication.step] at taken
  split at taken
  · rename_i later
    have := act_time order _ t action taken
    exact ⟨by simp_all, by simp_all⟩
  · cases taken

theorem observed_before {order : SnapshotOrder} {writers : List Nat} {s : State}
    (reach : Reach order writers s) : s.observed ≤ s.remote.time := by
  induction reach with
  | initial => simp [initial, SnapshotPublication.initial]
  | step _ step ih =>
    cases step with
    | remote time action next ok _ =>
      have := (step_time order _ next time action ok).1
      dsimp only; omega
    | observe t seen => dsimp only; omega
    | hide | reveal | confirm | abandon => exact ih
    | prepare w c next found ok =>
      have := (step_time order _ next _ _ ok).1
      dsimp only; omega

/-! ### Waiting for an unverified listed prefix -/

/-- With no unverified listed snapshot in the audience, the newest snapshot
this device can read is the newest one any version reads, so its own check
is §15's check against the true current newest. -/
theorem known_prepare_is_global (order : SnapshotOrder) (s : State) (snap : Snapshot)
    (known : waitsForUnverified s snap.audience = false) :
    canPrepareKnown order s snap = canPrepare order s.remote snap := by
  have same : knownCurrent order s snap.audience = current order s.remote snap.audience := by
    unfold knownCurrent current catalogCurrent
    congr 1
    rw [List.filter_filter]
    apply List.filter_congr
    intro x _
    by_cases here : x.audience = snap.audience
    · have notListed : s.unverified.contains x = false := by
        cases h : s.unverified.contains x
        · rfl
        · exfalso
          have member : x ∈ s.unverified := by simpa using h
          have : waitsForUnverified s snap.audience = true := by
            simp only [waitsForUnverified, List.any_eq_true, beq_iff_eq]
            exact ⟨x, member, here⟩
          simp_all
      have absent : x ∉ s.unverified := by simpa using notListed
      simp [absent]
    · simp [here]
  unfold canPrepareKnown canPrepare
  rw [same]
  cases current order s.remote snap.audience <;> rfl

/-- Without the wait: Ben's snapshot (three writes) is listed but Ana's
version cannot verify its prefix. Her new snapshot covers everything she can
read (two writes), yet not Ben's. Her own check passes, §15's fails, and once
it lands Ben's snapshot is still the newest any updated reader selects: her
snapshot does not supply the coverage it was prepared for. -/
def bensSnapshot : Snapshot := ⟨0, 2, 1, 10, 10, [(1, 1), (2, 2)]⟩
def anasOld : Snapshot := ⟨0, 1, 1, 5, 5, [(1, 1)]⟩
def anasNew : Snapshot := ⟨0, 1, 2, 20, 20, [(1, 2)]⟩
def hiddenState : State :=
  ⟨⟨20, [1, 2], [], [], [bensSnapshot, anasOld], [], []⟩, 20, [bensSnapshot], [], [], []⟩

theorem unverified_newest_counterexample :
    waitsForUnverified hiddenState 0 = true ∧
      canPrepareKnown .writes hiddenState anasNew = true ∧
      canPrepare .writes hiddenState.remote anasNew = false ∧
      catalogCurrent .writes (anasNew :: hiddenState.remote.landed) 0 = some bensSnapshot := by
  decide

/-! ### A sample already a day old -/

/-- A preparation whose sample the device already sees as more than a day
old could never count: it lands no earlier than storage time, which is no
earlier than what the device observed. So preparation waits for a later
catch-up instead. -/
theorem stale_sample_never_counts (snap : Snapshot) (observed landing : Nat)
    (stale : snap.caughtAt + day < observed) (after : observed ≤ landing) :
    ({ snap with storedAt := landing } : Snapshot).counts = false := by
  simp [Snapshot.counts]; omega

/-- That wait ends with the next catch-up: one begun now samples a time the
device can still count. -/
theorem fresh_catchup_may_prepare {order : SnapshotOrder} {writers : List Nat} {s : State}
    (reach : Reach order writers s) (next : SnapshotPublication.State)
    (audience writer number : Nat)
    (begun : act order s.remote (.beginCatchup audience writer number) = some next) :
    ∃ c, next.catching = c :: s.remote.catching ∧ mayCount s c.snapshot.caughtAt = true := by
  obtain ⟨c, cs, sample, _, _⟩ := begin_samples_time order s.remote next audience writer number begun
  refine ⟨c, cs, ?_⟩
  have := observed_before reach
  simp [mayCount, sample]; omega

/-! ### Abandonment -/

/-- An attempt that can no longer count can always be abandoned, which
frees its writer to prepare again (`Step.prepare`'s single-attempt premise). -/
theorem abandon_enabled (order : SnapshotOrder) (s : State) (a : Snapshot) (active : a ∈ s.active)
    (passed : a.caughtAt + day < s.observed ∨
      ∃ l ∈ s.remote.landed, samePath l a = true ∧ l.counts = false) :
    ∃ t, Step order s t ∧ t.active = s.active.erase a ∧ (a, s.remote.time) ∈ t.abandoned :=
  ⟨_, .abandon s a active passed, rfl, List.mem_cons_self⟩

/-- Stopping locally cannot stop a request already sent from landing, but a
request of an attempt abandoned on time lands no earlier than the
abandonment, so it never counts: readers ignore it (`late_snapshot_ignored`).
The landed object carries the attempt's signed sample. -/
theorem abandoned_never_counts_later {order : SnapshotOrder} {writers : List Nat} {s : State}
    (reach : Reach order writers s) (a : Snapshot) (passed : a.caughtAt + day < s.observed)
    (later next : SnapshotPublication.State) (since : s.remote.time ≤ later.time)
    (time audience writer number : Nat)
    (landing : SnapshotPublication.step order later time (.land audience writer number) = some next) :
    ∃ snap ∈ later.uploading, next.landed = { snap with storedAt := time } :: later.landed ∧
      (snap.caughtAt = a.caughtAt → ({ snap with storedAt := time } : Snapshot).counts = false) := by
  have observed := observed_before reach
  simp only [SnapshotPublication.step] at landing
  split at landing
  · rename_i atLeast
    obtain ⟨snap, member, landed⟩ := land_preserves_sample order _ next audience writer number landing
    refine ⟨snap, member, landed, fun sample => ?_⟩
    apply stale_sample_never_counts snap s.observed time (by omega)
    omega
  · cases landing

/-- An abandoned attempt's path stays uploading or landed, so a new attempt
can never reuse it: the catch-up it begins needs a path no request used. -/
theorem retry_new_path (order : SnapshotOrder) (s : SnapshotPublication.State)
    (audience writer number : Nat) (u : Snapshot) (held : u ∈ s.uploading ∨ u ∈ s.landed)
    (path : u.audience = audience ∧ u.writer = writer ∧ u.number = number) :
    act order s (.beginCatchup audience writer number) = none := by
  obtain ⟨ha, hw, hn⟩ := path
  have hit : ∀ snap : Snapshot, snap.audience = audience → snap.writer = writer →
      snap.number = number →
      (s.landed.any (samePath snap) || s.uploading.any (samePath snap)) = true := by
    intro snap a w n
    simp only [Bool.or_eq_true, List.any_eq_true]
    rcases held with up | down
    · exact .inr ⟨u, up, by simp [samePath, a, w, n, ha, hw, hn]⟩
    · exact .inl ⟨u, down, by simp [samePath, a, w, n, ha, hw, hn]⟩
  simp only [act]
  split
  · split
    · split
      · rfl
      · rename_i fresh; exact absurd (hit _ rfl rfl rfl) fresh
    · split
      · rfl
      · rename_i fresh; exact absurd (hit _ rfl rfl rfl) fresh
  · rfl

/-- With bounded successful service, the retry publishes: an attempt
sampled at a fresh catch-up whose request lands within a day of that sample
counts, and the device confirms it. Nothing here bounds the service. -/
theorem retry_counts (order : SnapshotOrder) (s : State) (a : Snapshot) (active : a ∈ s.active)
    (l : Snapshot) (landed : l ∈ s.remote.landed) (same : samePath l a = true)
    (sample : l.caughtAt = a.caughtAt) (inTime : l.storedAt ≤ a.caughtAt + day) :
    l.counts = true ∧ ∃ t, Step order s t ∧ a ∈ t.confirmed := by
  have counts : l.counts = true := by simp [Snapshot.counts, sample, inTime]
  exact ⟨counts, _, .confirm s a active l landed same counts, List.mem_cons_self⟩

end CovenIO.SnapshotAttempts
