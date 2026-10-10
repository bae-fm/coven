import Std

namespace CovenStorage.Clocks

def counterBase : Nat := 65536
def msLimit : Nat := 281474976710656
def tickLimit : Nat := msLimit * counterBase

/-- §7.2, D2: pack milliseconds and counter into their 64-bit order. -/
structure Timestamp where
  tick : Fin tickLimit
  device : Nat
  deriving DecidableEq, Repr

def Timestamp.ms (s : Timestamp) : Nat := s.tick.val / counterBase
def Timestamp.counter (s : Timestamp) : Nat := s.tick.val % counterBase

def Before (a b : Timestamp) : Prop :=
  a.tick.val < b.tick.val ∨ (a.tick.val = b.tick.val ∧ a.device < b.device)

instance (a b : Timestamp) : Decidable (Before a b) := inferInstanceAs
  (Decidable (a.tick.val < b.tick.val ∨ (a.tick.val = b.tick.val ∧ a.device < b.device)))

inductive StampError where
  | clockOutOfRange
  deriving DecidableEq, Repr

/-- §7.2: pre-epoch wall time contributes zero; increment the packed time
to carry counter overflow into the next millisecond. Never wrap the time. -/
def stamp (wall : Int) (device : Nat) (latest : Timestamp) : Except StampError Timestamp :=
  if wall ≥ (msLimit : Int) then .error .clockOutOfRange
  else
    let next := max (latest.tick.val + 1) (wall.toNat * counterBase)
    if h : next < tickLimit then .ok ⟨⟨next, h⟩, device⟩
    else .error .clockOutOfRange

theorem stamp_after_latest (wall : Int) (device : Nat) (latest result : Timestamp)
    (h : stamp wall device latest = .ok result) : latest.tick.val < result.tick.val := by
  unfold stamp at h
  split at h
  · contradiction
  · dsimp only at h
    split at h
    · cases h
      change latest.tick.val < max (latest.tick.val + 1) (wall.toNat * counterBase)
      have := Nat.le_max_left (latest.tick.val + 1) (wall.toNat * counterBase)
      omega
    · contradiction

theorem stamp_value (wall : Int) (device : Nat) (latest result : Timestamp)
    (h : stamp wall device latest = .ok result) :
    result.tick.val = max (latest.tick.val + 1) (wall.toNat * counterBase) ∧
    result.device = device := by
  unfold stamp at h
  split at h
  · contradiction
  · dsimp only at h
    split at h
    · cases h; exact ⟨rfl, rfl⟩
    · contradiction

theorem representable_stamp_exists (wall : Int) (device : Nat) (latest : Timestamp)
    (hw : wall < (msLimit : Int)) (hl : latest.tick.val + 1 < tickLimit) :
    ∃ result, stamp wall device latest = .ok result := by
  have bound : max (latest.tick.val + 1) (wall.toNat * counterBase) < tickLimit := by
    simp only [counterBase, tickLimit, msLimit] at *
    omega
  simp [stamp, show ¬ wall ≥ (msLimit : Int) by omega, bound]

theorem wall_ahead_uses_zero_counter (wall : Int) (device : Nat) (latest result : Timestamp)
    (ahead : (latest.ms : Int) < wall) (h : stamp wall device latest = .ok result) :
    result.ms = wall.toNat ∧ result.counter = 0 := by
  have value := (stamp_value wall device latest result h).1
  have bound : latest.tick.val + 1 ≤ wall.toNat * counterBase := by
    simp only [Timestamp.ms, counterBase] at ahead ⊢
    omega
  rw [Nat.max_eq_right bound] at value
  simp [Timestamp.ms, Timestamp.counter, value, counterBase]

theorem wall_behind_increments (wall : Int) (device : Nat) (latest result : Timestamp)
    (behind : wall ≤ (latest.ms : Int)) (h : stamp wall device latest = .ok result) :
    result.tick.val = latest.tick.val + 1 := by
  have value := (stamp_value wall device latest result h).1
  have bound : wall.toNat * counterBase ≤ latest.tick.val + 1 := by
    simp only [Timestamp.ms, counterBase] at behind ⊢
    omega
  simpa [Nat.max_eq_left bound] using value

theorem stamp_after_every_read (wall : Int) (device : Nat) (latest result : Timestamp)
    (seen : List Timestamp) (upper : ∀ s ∈ seen, s.tick.val ≤ latest.tick.val)
    (h : stamp wall device latest = .ok result) :
    ∀ s ∈ seen, Before s result := by
  intro s hs
  have := upper s hs
  have := stamp_after_latest wall device latest result h
  exact Or.inl (by omega)

theorem different_devices_distinct (a b : Timestamp) (h : a.device ≠ b.device) : a ≠ b := by
  intro he; exact h (congrArg Timestamp.device he)

/-- Keep the greatest complete timestamp, shared by data writes and entries. -/
def observe (a b : Timestamp) : Timestamp :=
  if a.tick.val < b.tick.val ∨ (a.tick.val = b.tick.val ∧ a.device < b.device) then b else a

theorem observe_upper (a b : Timestamp) :
    a.tick.val ≤ (observe a b).tick.val ∧ b.tick.val ≤ (observe a b).tick.val := by
  unfold observe
  split <;> simp_all <;> omega

def loadSnapshot (latest : Timestamp) (applied : List Timestamp) : Timestamp :=
  applied.foldl observe latest

theorem snapshot_upper (latest : Timestamp) (applied : List Timestamp) :
    latest.tick.val ≤ (loadSnapshot latest applied).tick.val ∧
    ∀ s ∈ applied, s.tick.val ≤ (loadSnapshot latest applied).tick.val := by
  induction applied generalizing latest with
  | nil => simp [loadSnapshot]
  | cons s ss ih =>
    obtain ⟨base, rest⟩ := ih (observe latest s)
    obtain ⟨left, right⟩ := observe_upper latest s
    constructor
    · exact Nat.le_trans left base
    · intro t ht
      rcases List.mem_cons.mp ht with he | hm
      · subst t; exact Nat.le_trans right base
      · exact rest t hm

theorem stamp_after_snapshot (wall : Int) (device : Nat) (latest result : Timestamp)
    (applied : List Timestamp) (h : stamp wall device (loadSnapshot latest applied) = .ok result) :
    ∀ s ∈ applied, Before s result :=
  stamp_after_every_read wall device _ result applied (snapshot_upper latest applied).2 h

structure Receiver where
  wall : Int
  latest : Timestamp
  applied : Nat
  deriving DecidableEq, Repr

/-- §7.2: causes and keys gate application; wall time does not. -/
def receive (r : Receiver) (s : Timestamp) (causes keys : Bool) : Receiver :=
  if causes && keys then { r with latest := observe r.latest s, applied := r.applied + 1 }
  else r

theorem no_clock_hold (wall : Int) (latest incoming : Timestamp) (n : Nat) :
    (receive ⟨wall, latest, n⟩ incoming true true).applied = n + 1 ∧
    incoming.tick.val ≤ (receive ⟨wall, latest, n⟩ incoming true true).latest.tick.val := by
  exact ⟨rfl, (observe_upper latest incoming).2⟩

theorem waiting_is_not_seen (r : Receiver) (s : Timestamp) (causes keys : Bool)
    (h : (causes && keys) = false) : receive r s causes keys = r := by
  simp [receive, h]

/-- A lower bound observed from provider metadata (§4, §9), in milliseconds.
There is deliberately no conversion from a device's Timestamp. -/
structure StorageTime where
  value : Nat
  deriving DecidableEq, Repr

/-- §15: equality with the retention duration suffices. -/
def aged (stored observed : StorageTime) (duration : Nat) : Bool :=
  decide (stored.value + duration ≤ observed.value)

/-- §9: time-based entry rejection is strictly beyond the duration. -/
def landedTooLate (unread landing : StorageTime) (duration : Nat) : Bool :=
  decide (unread.value + duration < landing.value)

/-- §9: older prefix is strict; the recent window includes both ends. -/
def oldForFinality (stored observed : StorageTime) (duration : Nat) : Bool :=
  decide (stored.value + duration < observed.value)

def recentForFinality (stored observed : StorageTime) (duration : Nat) : Bool :=
  decide (observed.value ≤ stored.value + duration ∧ stored.value ≤ observed.value)

theorem age_never_early (stored observed actual : StorageTime) (duration : Nat)
    (lowerBound : observed.value ≤ actual.value) (h : aged stored observed duration = true) :
    stored.value + duration ≤ actual.value := by
  simp only [aged, decide_eq_true_eq] at h
  omega

theorem finality_age_never_early (stored observed actual : StorageTime) (duration : Nat)
    (lowerBound : observed.value ≤ actual.value)
    (h : oldForFinality stored observed duration = true) :
    stored.value + duration < actual.value := by
  simp only [oldForFinality, decide_eq_true_eq] at h
  omega

theorem finality_window_partition (stored observed : StorageTime) (duration : Nat)
    (landed : stored.value ≤ observed.value) :
    oldForFinality stored observed duration = !recentForFinality stored observed duration := by
  apply Bool.eq_iff_iff.mpr
  simp [oldForFinality, recentForFinality, landed]

structure Maintenance where
  wall : Int
  observed : StorageTime
  monotonic : Nat
  deriving DecidableEq, Repr

def retentionAge (s : Maintenance) (stored : StorageTime) (duration : Nat) : Bool :=
  aged stored s.observed duration

/-- §16.5: the live timer, not a persisted wall-clock deadline. -/
def retryReady (s : Maintenance) (started delay : Nat) : Bool :=
  decide (started + delay ≤ s.monotonic)

def retryDelay (failures : Nat) : Nat := min (2 ^ failures) 300

theorem wall_jump_changes_no_age_or_retry (s : Maintenance) (newWall : Int)
    (stored : StorageTime) (duration started delay : Nat) :
    retentionAge { s with wall := newWall } stored duration = retentionAge s stored duration ∧
    retryReady { s with wall := newWall } started delay = retryReady s started delay := ⟨rfl, rfl⟩

theorem retry_delay_bounded (failures : Nat) : retryDelay failures ≤ 300 :=
  Nat.min_le_right _ _

end CovenStorage.Clocks
