import CovenIO.Waiting

/-! Evidence-triggered attempts (§3.1 Waiting, SP step 5 and "Waiting without
repeated work"), as `retry_waits.rs` runs them: an operation's wait is due when
its monotonic deadline passes, or when committed prerequisite evidence arrived
after the wait began. Due by evidence keeps the backoff: the next failure still
continues from the persisted count.

One session holds one operation's wait. `tick` advances the monotonic clock,
`evidence` is one committed prerequisite change, `start` begins an automatic
attempt (only when due), and `finish` ends it unsuccessfully, arming the next
wait with the evidence count seen so far. A provider cooldown only removes
starts, so the bounds below hold with it too. -/

namespace CovenIO.Waiting

theorem delay_mono {a b : Nat} (h : a ≤ b) : delay a ≤ delay b := by
  have pow : 2 ^ a ≤ 2 ^ b := Nat.pow_le_pow_right (by decide) h
  simp only [delay, CovenStorage.Clocks.retryDelay]
  omega

/-- An armed wait: the persisted failure count, the monotonic deadline and the
evidence count when the wait began. -/
structure Armed where
  failures : Nat
  deadline : Nat
  evidence : Nat
  deriving DecidableEq, Repr

/-- Due by its timer, or by evidence committed after the wait began. -/
def Armed.due (a : Armed) (now evidence : Nat) : Bool :=
  decide (a.deadline ≤ now) || decide (a.evidence < evidence)

/-- The wait after an unsuccessful attempt ending at `now`: the backoff
continues from the previous count whatever made the attempt due. -/
def rearm (previous : Option Armed) (now evidence : Nat) : Armed :=
  let failures := match previous with
    | some a => a.failures
    | none => 0
  ⟨failures + 1, now + delay failures, evidence⟩

structure Session where
  now : Nat
  evidence : Nat
  armed : Option Armed
  running : Bool
  deriving DecidableEq, Repr

inductive Event
  | tick (seconds : Nat)
  | evidence
  | start
  | finish
  deriving DecidableEq, Repr

/-- No wait yet, or the armed wait is due; never while an attempt runs. -/
def Session.ready (s : Session) : Bool :=
  !s.running && match s.armed with
    | none => true
    | some a => a.due s.now s.evidence

def step (s : Session) : Event → Option Session
  | .tick n => some { s with now := s.now + n }
  | .evidence => some { s with evidence := s.evidence + 1 }
  | .start => if s.ready then some { s with running := true } else none
  | .finish => if s.running then
      some { s with running := false, armed := some (rearm s.armed s.now s.evidence) }
    else none

def run (s : Session) : List Event → Option Session
  | [] => some s
  | e :: es => (step s e).bind fun next => run next es

def starts : List Event → Nat
  | [] => 0
  | .start :: es => starts es + 1
  | _ :: es => starts es

def evidences : List Event → Nat
  | [] => 0
  | .evidence :: es => evidences es + 1
  | _ :: es => evidences es

def ticks : List Event → Nat
  | [] => 0
  | .tick n :: es => ticks es + n
  | _ :: es => ticks es

/-- The delay the next failure arms. -/
def Session.floor (s : Session) : Nat :=
  delay (match s.armed with
    | some a => a.failures
    | none => 0)

theorem run_counts {s t : Session} {es : List Event} (h : run s es = some t) :
    t.now = s.now + ticks es ∧ t.evidence = s.evidence + evidences es := by
  induction es generalizing s with
  | nil => simp [run] at h; subst h; simp [ticks, evidences]
  | cons e es ih =>
      simp only [run, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      have := ih hr
      cases e with
      | tick n => simp [step] at hs; subst hs; simp [ticks, evidences] at *; omega
      | evidence => simp [step] at hs; subst hs; simp [ticks, evidences] at *; omega
      | start =>
          simp only [step] at hs
          split at hs <;> simp at hs; subst hs; simpa [ticks, evidences] using this
      | finish =>
          simp only [step] at hs
          split at hs <;> simp at hs; subst hs; simpa [ticks, evidences] using this

/-- The accounting behind the bound, for `k` starts so far from a session whose
clock read `n0` and evidence count `e0`: every start after the first is paid by
`d` seconds or by one evidence commit. -/
def Inv (d n0 e0 : Nat) (s : Session) (k : Nat) : Prop :=
  d ≤ s.floor ∧ n0 ≤ s.now ∧ e0 ≤ s.evidence ∧
  (1 ≤ k → d * (k - 1) + d * e0 + n0 ≤ d * s.evidence + s.now) ∧
  (1 ≤ k → s.running = false → ∃ a, s.armed = some a ∧ a.evidence ≤ s.evidence ∧
    d * (k - 1) + d * e0 + n0 ≤ d * a.evidence + s.now ∧
    d * k + d * e0 + n0 ≤ d * a.evidence + a.deadline)

theorem inv_step {d n0 e0 : Nat} {s t : Session} {k : Nat} {e : Event}
    (inv : Inv d n0 e0 s k) (h : step s e = some t) :
    Inv d n0 e0 t (k + starts [e]) := by
  obtain ⟨fl, hn, he, hA, hB⟩ := inv
  cases e with
  | tick n =>
      simp [step] at h; subst h
      simp only [starts, Nat.add_zero]
      refine ⟨fl, by simp; omega, he, ?_, ?_⟩
      · intro k1; have := hA k1; simp; omega
      · intro k1 r; obtain ⟨a, ha, hae, hc, hb⟩ := hB k1 r
        exact ⟨a, ha, hae, by simp; omega, hb⟩
  | evidence =>
      simp [step] at h; subst h
      simp only [starts, Nat.add_zero]
      refine ⟨fl, hn, by simp; omega, ?_, ?_⟩
      · intro k1; have := hA k1; simp [Nat.mul_add]; omega
      · intro k1 r; obtain ⟨a, ha, hae, hc, hb⟩ := hB k1 r
        exact ⟨a, ha, by simp; omega, hc, hb⟩
  | start =>
      simp only [step] at h
      split at h
      · rename_i ready
        simp at h; subst h
        simp only [starts]
        refine ⟨fl, hn, he, ?_, ?_⟩
        · intro _
          show d * (k + 1 - 1) + d * e0 + n0 ≤ d * s.evidence + s.now
          rw [show k + 1 - 1 = k by omega]
          by_cases k0 : k = 0
          · subst k0
            have : d * e0 ≤ d * s.evidence := Nat.mul_le_mul_left d he
            omega
          · have running : s.running = false := by
              simp [Session.ready] at ready; exact ready.1
            obtain ⟨a, ha, hae, hc, hb⟩ := hB (by omega) running
            simp [Session.ready, ha, Armed.due] at ready
            rcases ready.2 with timer | fresh
            · have : d * a.evidence ≤ d * s.evidence := Nat.mul_le_mul_left d hae
              omega
            · have : d * (a.evidence + 1) ≤ d * s.evidence := Nat.mul_le_mul_left d fresh
              have shift : d * k = d * (k - 1) + d := by
                rw [← Nat.mul_succ]; congr 1; omega
              rw [Nat.mul_succ] at this
              omega
        · intro _ r; simp at r
      · simp at h
  | finish =>
      simp only [step] at h
      split at h
      · rename_i running
        simp at h; subst h
        simp only [starts, Nat.add_zero]
        have fl' : d ≤ delay ((match s.armed with | some a => a.failures | none => 0) + 1) :=
          Nat.le_trans fl (delay_mono (Nat.le_succ _))
        refine ⟨?_, hn, he, hA, ?_⟩
        · simpa [Session.floor, rearm] using fl'
        · intro k1 _
          refine ⟨_, rfl, by simp [rearm], ?_, ?_⟩
          · simpa [rearm] using hA k1
          · have := hA k1
            have floor : d ≤ delay (match s.armed with | some a => a.failures | none => 0) := fl
            have shift : d * k = d * (k - 1) + d := by
              rw [← Nat.mul_succ]; congr 1; omega
            simp only [rearm]
            omega
      · simp at h

theorem inv_run {d n0 e0 : Nat} {s t : Session} {k : Nat} {es : List Event}
    (inv : Inv d n0 e0 s k) (h : run s es = some t) :
    Inv d n0 e0 t (k + starts es) := by
  induction es generalizing s k with
  | nil => simp [run] at h; subst h; simpa [starts] using inv
  | cons e es ih =>
      simp only [run, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      have := ih (inv_step inv hs) hr
      have split : starts (e :: es) = starts [e] + starts es := by
        cases e <;> simp [starts] <;> omega
      rw [split, ← Nat.add_assoc]; exact this

/-- Evidence-triggered attempts: from any session (an attempt may be running),
starts over a run are at most one, plus one per evidence commit, plus the
elapsed time over `d`, for any `d` no longer than the delay the next failure
arms. -/
theorem evidence_rate {d : Nat} {s t : Session} {es : List Event} (positive : 0 < d)
    (floor : d ≤ s.floor) (h : run s es = some t) :
    starts es ≤ 1 + evidences es + ticks es / d := by
  have inv0 : Inv d s.now s.evidence s 0 :=
    ⟨floor, Nat.le_refl _, Nat.le_refl _, by intro h; omega, by intro h; omega⟩
  obtain ⟨_, _, _, hA, _⟩ := inv_run inv0 h
  obtain ⟨tn, te⟩ := run_counts h
  have nonneg := Nat.zero_le (ticks es / d)
  by_cases zero : starts es = 0
  · omega
  · have bound := hA (by omega)
    rw [tn, te, Nat.mul_add, Nat.zero_add] at bound
    have paid : d * (starts es - 1) ≤ d * evidences es + ticks es := by omega
    have scaled : (starts es - 1 - evidences es) * d ≤ ticks es := by
      rw [Nat.sub_mul, Nat.mul_comm (starts es - 1), Nat.mul_comm (evidences es)]
      omega
    have quotient : starts es - 1 - evidences es ≤ ticks es / d :=
      (Nat.le_div_iff_mul_le positive).mpr scaled
    omega

theorem evidence_one_second {s t : Session} {es : List Event} (h : run s es = some t) :
    starts es ≤ 1 + evidences es + ticks es := by
  have := evidence_rate (d := 1) (by decide) (delay_positive _) h
  simpa using this

theorem evidence_capped {s t : Session} {es : List Event} {a : Armed}
    (armed : s.armed = some a) (capped : 9 ≤ a.failures) (h : run s es = some t) :
    starts es ≤ 1 + evidences es + ticks es / 300 := by
  have floor : 300 ≤ s.floor := by
    simp only [Session.floor, armed]
    have := delay_at_cap (a.failures - 9)
    rw [show a.failures - 9 + 9 = a.failures by omega] at this
    omega
  exact evidence_rate (by decide) floor h

/-- `W` waits sharing one run's clock and evidence. -/
theorem evidence_waits_rate (bounds : List Nat) (b : Nat) (each : ∀ n ∈ bounds, n ≤ b) :
    bounds.sum ≤ bounds.length * b := by
  induction bounds with
  | nil => simp
  | cons n rest ih =>
      have head := each n (by simp)
      have tail := ih fun m hm => each m (List.mem_cons_of_mem _ hm)
      simp only [List.sum_cons, List.length_cons, Nat.succ_mul]
      omega

/-- Due once: the wait an attempt arms is not due again at the same instant
and evidence count; new evidence or its delay is needed. -/
theorem evidence_due_once (previous : Option Armed) (now evidence : Nat) :
    (rearm previous now evidence).due now evidence = false := by
  cases previous with
  | none =>
      have := delay_positive 0
      simp [rearm, Armed.due]; omega
  | some a =>
      have := delay_positive a.failures
      simp only [rearm, Armed.due, Nat.lt_irrefl, decide_false, Bool.or_false]
      exact decide_eq_false (by omega)

/-- Evidence never resets the backoff: the next wait continues the count. -/
theorem evidence_keeps_backoff (a : Armed) (now evidence : Nat) :
    (rearm (some a) now evidence).failures = a.failures + 1 ∧
    (rearm (some a) now evidence).deadline = now + delay a.failures := ⟨rfl, rfl⟩

namespace Examples

/-- A capped wait (nine failures) that has just armed 300 seconds. -/
def capped : Session := ⟨0, 0, some ⟨9, 300, 0⟩, false⟩

/-- Three evidence commits at the same instant, for example three other
operations advancing in one drive, each make the capped wait due again. -/
def burst : List Event :=
  [.evidence, .start, .finish, .evidence, .start, .finish, .evidence, .start, .finish]

/-- §3.1's bound for unchanged waits does not hold with evidence-triggered
attempts: three starts in an interval of length zero, where both
`1 + floor(0 / 1)` and `1 + floor(0 / 300)` allow one. -/
theorem evidence_exceeds_unchanged_bound :
    (run capped burst).isSome ∧ starts burst = 3 ∧ ticks burst = 0 ∧
    1 + ticks burst / 300 < starts burst ∧ 1 + ticks burst < starts burst := by
  decide

/-- A commit racing an attempt is absorbed: the prerequisite lands after the
attempt read it and before the attempt failed, and the wait armed afterward
is not due by it, so the operation waits its full delay. -/
theorem racing_commit_waits_full_delay :
    (run capped [.evidence, .start, .evidence, .finish]).map
      (fun s => (s.armed.map (·.due s.now s.evidence), s.armed.map (·.deadline))) =
      some (some false, some 300) := by
  decide

end Examples

end CovenIO.Waiting
