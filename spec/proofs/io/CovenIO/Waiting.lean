import CovenIO.Storage
import CovenStorage.Clocks

namespace CovenIO.Waiting

abbrev delay := CovenStorage.Clocks.retryDelay

theorem delay_cap (failures : Nat) : delay failures ≤ 300 :=
  CovenStorage.Clocks.retry_delay_bounded failures

theorem delay_positive (failures : Nat) : 1 ≤ delay failures := by
  have positive : 0 < 2 ^ failures := Nat.two_pow_pos failures
  simp only [delay, CovenStorage.Clocks.retryDelay]
  omega

theorem delay_at_cap (failuresAfterNine : Nat) : delay (failuresAfterNine + 9) = 300 := by
  have positive := Nat.two_pow_pos failuresAfterNine
  simp only [delay, CovenStorage.Clocks.retryDelay, Nat.pow_add]
  change min (2 ^ failuresAfterNine * 512) 300 = 300
  omega

def restart (deadline now : Int) (wait : Nat) : Int :=
  max 0 (min (deadline - now) (wait : Int))

theorem restart_bounded (deadline now : Int) (wait : Nat) :
    0 ≤ restart deadline now wait ∧ restart deadline now wait ≤ (wait : Int) := by
  unfold restart; omega

structure Timer where
  ordinary : Nat
  provider : Nat
  deriving DecidableEq, Repr

def due (t : Timer) (now : Nat) : Bool := decide (max t.ordinary t.provider ≤ now)
def appRetry (t : Timer) (now : Nat) : Timer := { t with ordinary := now }

theorem app_retry_keeps_cooldown (t : Timer) (now : Nat)
    (ready : due (appRetry t now) now = true) : t.provider ≤ now := by
  have ready : max now t.provider ≤ now := of_decide_eq_true ready
  omega

/-- Attempt starts in monotonic time, with at least d between consecutive
starts. A failed or still-absent prerequisite schedules the same wait. -/
inductive Starts (d : Nat) : Nat → List Nat → Prop
  | nil (lower : Nat) : Starts d lower []
  | cons (lower time : Nat) (tail : List Nat) (ready : lower ≤ time)
      (rest : Starts d (time + d) tail) : Starts d lower (time :: tail)

theorem span {d lower : Nat} {times : List Nat} (h : Starts d lower times)
    (upper : Nat) (within : ∀ t ∈ times, t ≤ upper) (nonempty : times ≠ []) :
    lower + (times.length - 1) * d ≤ upper := by
  induction h with
  | nil => exact False.elim (nonempty rfl)
  | cons lo time tail ready rest ih =>
      have ht := within time (by simp)
      cases tail with
      | nil => simpa using Nat.le_trans ready ht
      | cons a as =>
          have bound := ih (fun t hm => within t (List.mem_cons_of_mem _ hm)) (by simp)
          simp only [List.length_cons, Nat.add_sub_cancel] at *
          rw [Nat.succ_mul]
          omega

theorem rate {d lower : Nat} {times : List Nat} (h : Starts d lower times)
    (positive : 0 < d) (duration : Nat) (within : ∀ t ∈ times, t ≤ lower + duration) :
    times.length ≤ 1 + duration / d := by
  by_cases empty : times = []
  · simp [empty]
  · have bound := span h (lower + duration) within empty
    have quotient : times.length - 1 ≤ duration / d :=
      (Nat.le_div_iff_mul_le positive).mpr (by omega)
    omega

theorem waits_rate (schedules : List (List Nat)) (d lower duration : Nat) (positive : 0 < d)
    (spaced : ∀ ts ∈ schedules, Starts d lower ts)
    (within : ∀ ts ∈ schedules, ∀ t ∈ ts, t ≤ lower + duration) :
    (schedules.map List.length).sum ≤ schedules.length * (1 + duration / d) := by
  induction schedules with
  | nil => simp
  | cons ts rest ih =>
      have head := rate (spaced ts (by simp)) positive duration (within ts (by simp))
      have tail := ih (fun s hm => spaced s (List.mem_cons_of_mem _ hm))
        (fun s hm => within s (List.mem_cons_of_mem _ hm))
      simp only [List.map_cons, List.sum_cons, List.length_cons, Nat.succ_mul]
      omega

theorem one_second_bound (schedules : List (List Nat)) (lower duration : Nat)
    (spaced : ∀ ts ∈ schedules, Starts 1 lower ts)
    (within : ∀ ts ∈ schedules, ∀ t ∈ ts, t ≤ lower + duration) :
    (schedules.map List.length).sum ≤ schedules.length * (1 + duration) := by
  simpa using waits_rate schedules 1 lower duration (by decide) spaced within

theorem capped_bound (schedules : List (List Nat)) (lower duration : Nat)
    (spaced : ∀ ts ∈ schedules, Starts 300 lower ts)
    (within : ∀ ts ∈ schedules, ∀ t ∈ ts, t ≤ lower + duration) :
    (schedules.map List.length).sum ≤ schedules.length * (1 + duration / 300) :=
  waits_rate schedules 300 lower duration (by decide) spaced within

end CovenIO.Waiting
