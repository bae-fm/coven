import CovenIO.WaitingEvidence

/-! Reopening the store with operations waiting (§3.1 Waiting, §16.5, SP
"Waiting without repeated work"), as `RetryWaits::open` and `watch_persisted`
run it.

A wait persists its wall-clock deadline `T` and its full wait `W`, the delay
the attempt before it armed. Reopening arms `max 0 (min (T - now) W)` on the
monotonic clock, keeps the failure count and the prerequisite the wait names,
and watches that prerequisite from the reopen on: a commit before it is never
seen, so a wait woken before closing is not woken after. An attempt that was
running when the store closed is lost; its operation's previous wait comes
back. `jump` is how far the wall clock moved beyond the monotonic clock since
the wait was armed: negative when it was set back.

Proved here: a reopened wait is due within its full wait `W`, under any wall
clock, whatever committed before the reopen (`reopen_within_wait`,
`reopened_due`), so a prerequisite satisfied before the reopen stalls it at
most that long. With no forward wall-clock jump, starts across any number of
reopens are at most §3.1's count plus one per reopen (`reopen_rate`,
`reopen_unchanged_rate`); the extra start is the lost attempt starting again
(`reopen_mid_attempt_restarts`), and a reopen with no attempt running adds
none (`idle_reopen_keeps_count`). A forward jump can end a wait early
(`forward_jump_ends_wait`); §3.1 states its restart bound only without one. -/

namespace CovenIO.Waiting

/-- The full wait `W` persisted with a wait: the delay its attempt armed,
`delay (failures - 1)` (`retry_waits.rs` `next`). -/
def Armed.wait (a : Armed) : Nat := delay (a.failures - 1)

/-- The restart arm: `max 0 (min (T - wall) W)`, where `T - wall` is
`deadline - now - jump` on the monotonic clock. -/
def restartArm (a : Armed) (now : Nat) (jump : Int) : Nat :=
  (restart (a.deadline : Int) ((now : Int) + jump) a.wait).toNat

/-- A persisted wait rearmed on reopening at monotonic time `now`. -/
def reopened (a : Armed) (now : Nat) (jump : Int) : Armed :=
  { a with deadline := now + restartArm a now jump, woken := false }

/-- Reopen at the session's monotonic time: each persisted wait rearmed by
the restart rule and watched from now, any running attempt lost. -/
def Session.reopen (s : Session) (jump : Int) : Session :=
  { s with armed := s.armed.map (reopened · s.now jump), running := none }

@[simp] theorem reopen_now (s : Session) (jump : Int) : (s.reopen jump).now = s.now := rfl
@[simp] theorem reopen_paid (s : Session) (jump : Int) : (s.reopen jump).paid = s.paid := rfl
@[simp] theorem reopen_running (s : Session) (jump : Int) :
    (s.reopen jump).running = none := rfl
@[simp] theorem reopened_failures (a : Armed) (now : Nat) (jump : Int) :
    (reopened a now jump).failures = a.failures := rfl
@[simp] theorem reopened_deadline (a : Armed) (now : Nat) (jump : Int) :
    (reopened a now jump).deadline = now + restartArm a now jump := rfl
@[simp] theorem reopened_prereq (a : Armed) (now : Nat) (jump : Int) :
    (reopened a now jump).prereq = a.prereq := rfl
@[simp] theorem reopened_woken (a : Armed) (now : Nat) (jump : Int) :
    (reopened a now jump).woken = false := rfl

theorem reopen_floor (s : Session) (jump : Int) : (s.reopen jump).floor = s.floor := by
  cases ha : s.armed <;> simp [Session.reopen, Session.floor, ha]

theorem reopen_armed {s : Session} {a : Armed} (jump : Int) (armed : s.armed = some a) :
    (s.reopen jump).armed = some (reopened a s.now jump) := by
  simp [Session.reopen, armed]

/-- A session's life: its events, and reopens. -/
inductive Life
  | event (e : Event)
  | reopen (jump : Int)
  deriving DecidableEq, Repr

def lifeStep (s : Session) : Life → Option Session
  | .event e => step s e
  | .reopen jump => some (s.reopen jump)

def live (s : Session) : List Life → Option Session
  | [] => some s
  | l :: ls => (lifeStep s l).bind fun next => live next ls

def lifeStarts : List Life → Nat
  | [] => 0
  | .event .start :: ls => lifeStarts ls + 1
  | _ :: ls => lifeStarts ls

def lifeTicks : List Life → Nat
  | [] => 0
  | .event (.tick n) :: ls => lifeTicks ls + n
  | _ :: ls => lifeTicks ls

def reopens : List Life → Nat
  | [] => 0
  | .reopen _ :: ls => reopens ls + 1
  | _ :: ls => reopens ls

/-- No armed deadline lies further ahead than its full wait: true of every
wait an attempt arms, and kept by every step. -/
def Fresh (s : Session) : Prop := ∀ a, s.armed = some a → a.deadline ≤ s.now + a.wait

theorem fresh_step {s t : Session} {e : Event} (fresh : Fresh s) (h : step s e = some t) :
    Fresh t := by
  cases e with
  | tick n =>
      simp [step] at h; subst h
      intro a ha; have := fresh a ha; simp; omega
  | commit q =>
      simp only [step, Option.some.injEq] at h; subst h
      cases hr : s.running with
      | some l => exact fresh
      | none =>
          dsimp only
          unfold wake
          split
          · rename_i a ha
            split
            · intro b hb; simp at hb; subst hb
              have := fresh a ha; simpa [Armed.wait] using this
            · exact fresh
          · exact fresh
  | start =>
      simp only [step] at h
      split at h
      · simp at h; subst h; exact fresh
      · simp at h
  | finish p =>
      cases hr : s.running with
      | none => simp [step, hr] at h
      | some l =>
          simp [step, hr] at h; subst h
          intro a ha; simp at ha; subst ha
          simp [rearm, Armed.wait]

theorem fresh_reopen {s : Session} (jump : Int) : Fresh (s.reopen jump) := by
  intro b hb
  simp only [Session.reopen, Option.map_eq_some_iff] at hb
  obtain ⟨a, _, rfl⟩ := hb
  have := restart_bounded (a.deadline : Int) ((s.now : Int) + jump) a.wait
  simp only [reopened_deadline, reopen_now, Armed.wait, reopened_failures, restartArm] at *
  omega

/-- With the wall clock not moved forward, a reopened deadline is no earlier
than the one it restores. -/
theorem honest_deadline {s : Session} {a : Armed} {jump : Int} (fresh : Fresh s)
    (armed : s.armed = some a) (back : jump ≤ 0) :
    a.deadline ≤ s.now + restartArm a s.now jump := by
  have := fresh a armed
  simp only [restartArm, restart]
  omega

/-! ## Within the full wait, under any wall clock -/

/-- The full wait of the session's persisted wait, if it has one. -/
def Session.fullWait (s : Session) : Nat :=
  match s.armed with
  | some a => a.wait
  | none => 0

/-- Reopening keeps the backoff and the prerequisite, and arms at most the
full wait, whatever the wall clock reads. -/
theorem reopen_within_wait (s : Session) (jump : Int) (a : Armed) (armed : s.armed = some a) :
    ∃ b, (s.reopen jump).armed = some b ∧ b.failures = a.failures ∧ b.prereq = a.prereq ∧
      b.woken = false ∧ b.deadline ≤ s.now + a.wait := by
  refine ⟨reopened a s.now jump, reopen_armed jump armed, rfl, rfl, rfl, ?_⟩
  have := restart_bounded (a.deadline : Int) ((s.now : Int) + jump) a.wait
  simp only [reopened_deadline, restartArm] at *
  omega

/-- Ticks and commits from an idle session keep its wait's deadline and
advance its clock: commits only wake. -/
theorem quiet_run {s t : Session} {es : List Event} (idle : s.running = none)
    (noStart : Event.start ∉ es) (h : run s es = some t) :
    t.running = none ∧ t.now = s.now + ticks es ∧
      (s.armed.map (·.deadline)) = (t.armed.map (·.deadline)) := by
  induction es generalizing s with
  | nil => simp [run] at h; subst h; exact ⟨idle, by simp [ticks], rfl⟩
  | cons e es ih =>
      simp only [run, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      have rest : Event.start ∉ es := fun m => noStart (List.mem_cons_of_mem _ m)
      have keeps : next.running = none ∧ next.now = s.now + ticks [e] ∧
          (s.armed.map (·.deadline)) = (next.armed.map (·.deadline)) := by
        cases e with
        | tick n => simp [step] at hs; subst hs; simp [idle, ticks]
        | commit q =>
            simp only [step, idle, Option.some.injEq] at hs; subst hs
            unfold wake; split
            · rename_i a ha; split
              · simp [idle, ha, ticks]
              · simp [idle, ticks]
            · simp [idle, ticks]
        | start => exact absurd List.mem_cons_self noStart
        | finish p => simp [step, idle] at hs
      obtain ⟨r, n', d⟩ := ih keeps.1 rest hr
      have split : ticks (e :: es) = ticks [e] + ticks es := by
        cases e <;> simp [ticks] <;> omega
      refine ⟨r, by rw [n', keeps.2.1, split]; omega, ?_⟩
      rw [keeps.2.2, d]

/-- A wait whose prerequisite was satisfied before the reopen, so that no
commit after it wakes it, is due once its full wait has passed: whatever the
wall clock, and whatever ticks and commits follow, with no attempt in
between. -/
theorem reopened_due {s t : Session} {jump : Int} {es : List Event}
    (noStart : Event.start ∉ es) (waited : s.fullWait ≤ ticks es)
    (h : run (s.reopen jump) es = some t) : t.ready = true := by
  obtain ⟨idle, now, deadline⟩ := quiet_run (by simp [Session.reopen]) noStart h
  cases armed : s.armed with
  | none =>
      have : t.armed = none := by
        cases ht : t.armed with
        | none => rfl
        | some b => simp [Session.reopen, armed, ht] at deadline
      simp [Session.ready, idle, this]
  | some a =>
      obtain ⟨b, hb, -, -, -, within⟩ := reopen_within_wait s jump a armed
      cases ht : t.armed with
      | none => rw [hb, ht] at deadline; simp at deadline
      | some c =>
          rw [hb, ht] at deadline; simp at deadline
          have reopenedNow : (s.reopen jump).now = s.now := rfl
          simp only [Session.fullWait, armed] at waited
          simp only [Session.ready, idle, ht, Armed.due, Option.isNone_none, Bool.true_and]
          simp only [Bool.or_eq_true, decide_eq_true_eq]
          left; omega

/-! ## §3.1's count across reopens, without a forward wall-clock jump -/

theorem live_now {s t : Session} {ls : List Life} (h : live s ls = some t) :
    t.now = s.now + lifeTicks ls := by
  induction ls generalizing s with
  | nil => simp [live] at h; subst h; simp [lifeTicks]
  | cons l ls ih =>
      simp only [live, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      have := ih hr
      cases l with
      | reopen j =>
          simp [lifeStep] at hs; subst hs
          simpa [lifeTicks, Session.reopen] using this
      | event e =>
          have one := run_now (s := s) (es := [e]) (by simp [run, lifeStep] at hs ⊢; exact hs)
          cases e <;> simp [lifeTicks, ticks] at one ⊢ <;> omega

/-- The accounting through reopens: `k` starts so far, of which at most `r`
were restarts of an attempt a reopen lost; the remaining `k'` meet `Inv`. -/
def LifeInv (d n0 p0 : Nat) (s : Session) (k r : Nat) : Prop :=
  ∃ k', k ≤ k' + r ∧ Inv d n0 p0 s k' ∧ Fresh s ∧ (s.armed = none → k' ≤ 1)

theorem first_needs_no_wait {d n0 p0 : Nat} {s : Session} {k : Nat}
    (inv : Inv d n0 p0 s k) (idle : s.running = none) (none : s.armed = none) : k = 0 := by
  obtain ⟨_, _, _, _, hB⟩ := inv
  cases k with
  | zero => rfl
  | succ k =>
      obtain ⟨a, ha, _⟩ := hB (by omega) idle
      rw [none] at ha; cases ha

theorem lifeInv_event {d n0 p0 : Nat} {s t : Session} {k r : Nat} {e : Event}
    (inv : LifeInv d n0 p0 s k r) (h : step s e = some t) :
    LifeInv d n0 p0 t (k + starts [e]) r := by
  obtain ⟨k', hk, hinv, fresh, first⟩ := inv
  refine ⟨k' + starts [e], by omega, inv_step hinv h, fresh_step fresh h, ?_⟩
  intro none
  cases e with
  | tick n => simp [step] at h; subst h; simpa [starts] using first none
  | commit q =>
      simp only [step, Option.some.injEq] at h; subst h
      cases hr : s.running with
      | some l => simp [hr] at none; simpa [starts] using first none
      | none =>
          simp only [hr] at none
          have : s.armed = Option.none := by
            unfold wake at none; split at none
            · rename_i a ha; split at none <;> simp_all
            · assumption
          simpa [starts] using first this
  | start =>
      simp only [step] at h
      split at h
      · rename_i ready
        simp at h; subst h
        have idle : s.running = Option.none := by
          simp [Session.ready] at ready; exact ready.1
        have := first_needs_no_wait hinv idle none
        simp [starts]; omega
      · simp at h
  | finish p =>
      cases hr : s.running with
      | none => simp [step, hr] at h
      | some l => simp [step, hr] at h; subst h; simp at none

theorem lifeInv_reopen {d n0 p0 : Nat} {s : Session} {k r : Nat} {jump : Int}
    (inv : LifeInv d n0 p0 s k r) (back : jump ≤ 0) :
    LifeInv d n0 p0 (s.reopen jump) k (r + 1) := by
  obtain ⟨k', hk, ⟨fl, hn, hp, hA, hB⟩, fresh, first⟩ := inv
  have floor := reopen_floor s jump
  cases ha : s.armed with
  | none =>
      have k1 := first ha
      refine ⟨0, by omega, ⟨by rw [floor]; exact fl, hn, hp, fun h => by omega,
        fun h => by omega⟩, fresh_reopen jump, fun _ => by omega⟩
  | some a =>
      have later := honest_deadline fresh ha back
      cases hr : s.running with
      | none =>
          -- No attempt lost: the count carries over unchanged.
          refine ⟨k', by omega, ⟨by rw [floor]; exact fl, hn, hp, hA, ?_⟩,
            fresh_reopen jump, fun h => by simp [Session.reopen, ha] at h⟩
          intro k1 _
          obtain ⟨b, hb, hd, _⟩ := hB k1 hr
          rw [ha] at hb; cases hb
          exact ⟨_, reopen_armed jump ha, by simp [reopened]; omega,
            fun w => by simp [reopened] at w⟩
      | some l =>
          -- The lost attempt's start is refunded: its operation may start again.
          refine ⟨k' - 1, by omega, ⟨by rw [floor]; exact fl, hn, hp, ?_, ?_⟩,
            fresh_reopen jump, fun h => by simp [Session.reopen, ha] at h⟩
          · intro k1; have := hA (by omega)
            have mono : d * (k' - 1 - 1) ≤ d * (k' - 1) := Nat.mul_le_mul_left d (by omega)
            simp [Session.reopen]; omega
          · intro k1 _
            have := hA (by omega)
            have shift : d * (k' - 1) = d * (k' - 1 - 1) + d := by
              rw [← Nat.mul_succ]; congr 1; omega
            exact ⟨_, reopen_armed jump ha, by simp [reopened]; omega,
              fun w => by simp [reopened] at w⟩

theorem lifeInv_live {d n0 p0 : Nat} {s t : Session} {k r : Nat} {ls : List Life}
    (inv : LifeInv d n0 p0 s k r) (noForward : ∀ j, Life.reopen j ∈ ls → j ≤ 0)
    (h : live s ls = some t) :
    LifeInv d n0 p0 t (k + lifeStarts ls) (r + reopens ls) := by
  induction ls generalizing s k r with
  | nil => simp [live] at h; subst h; simpa [lifeStarts, reopens] using inv
  | cons l ls ih =>
      simp only [live, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      have rest : ∀ j, Life.reopen j ∈ ls → j ≤ 0 :=
        fun j m => noForward j (List.mem_cons_of_mem _ m)
      cases l with
      | reopen j =>
          simp [lifeStep] at hs; subst hs
          have := ih (lifeInv_reopen inv (noForward j List.mem_cons_self)) rest hr
          simp only [lifeStarts, reopens] at this ⊢
          rw [show r + (reopens ls + 1) = r + 1 + reopens ls by omega]; exact this
      | event e =>
          have := ih (lifeInv_event inv hs) rest hr
          have split : lifeStarts (.event e :: ls) = starts [e] + lifeStarts ls := by
            cases e <;> simp [lifeStarts, starts] <;> omega
          have opens : reopens (.event e :: ls) = reopens ls := by simp [reopens]
          rw [split, opens, ← Nat.add_assoc]; exact this

/-- Starts across a life with reopens and no forward wall-clock jump: at most
one, plus one per waking commit, plus one per reopen, plus the elapsed time
over `d`. -/
theorem reopen_rate {d : Nat} {s t : Session} {ls : List Life} (positive : 0 < d)
    (floor : d ≤ s.floor) (fresh : Fresh s) (noForward : ∀ j, Life.reopen j ∈ ls → j ≤ 0)
    (h : live s ls = some t) :
    lifeStarts ls ≤ 1 + (t.paid - s.paid) + reopens ls + lifeTicks ls / d := by
  have inv0 : LifeInv d s.now s.paid s 0 0 :=
    ⟨0, by omega, ⟨floor, Nat.le_refl _, Nat.le_refl _, by intro h; omega, by intro h; omega⟩,
      fresh, fun _ => by omega⟩
  obtain ⟨k', hk, ⟨_, _, hp, hA, _⟩, _, _⟩ := lifeInv_live inv0 noForward h
  simp only [Nat.zero_add] at hk
  have tn := live_now h
  have nonneg := Nat.zero_le (lifeTicks ls / d)
  by_cases zero : k' = 0
  · omega
  · have bound := hA (by omega)
    rw [tn] at bound
    have spread : d * (t.paid - s.paid) = d * t.paid - d * s.paid := Nat.mul_sub _ _ _
    have grows : d * s.paid ≤ d * t.paid := Nat.mul_le_mul_left d hp
    have factor : (k' - 1 - (t.paid - s.paid)) * d = d * (k' - 1) - d * (t.paid - s.paid) := by
      rw [Nat.mul_comm, Nat.mul_sub]
    have scaled : (k' - 1 - (t.paid - s.paid)) * d ≤ lifeTicks ls := by
      rw [factor, spread]; omega
    have quotient : k' - 1 - (t.paid - s.paid) ≤ lifeTicks ls / d :=
      (Nat.le_div_iff_mul_le positive).mpr scaled
    omega

/-- `Unchanged` through a life: reopens keep the waits' prerequisites. -/
def LifeUnchanged (unrelated : Nat → Bool) (s : Session) (ls : List Life) : Prop :=
  (∀ a, s.armed = some a → unrelated a.prereq = false) ∧
  (∀ l, s.running = some l → ∀ q ∈ l, unrelated q = true) ∧
  (∀ q, Life.event (.commit q) ∈ ls → unrelated q = true) ∧
  (∀ p, Life.event (.finish p) ∈ ls → unrelated p = false)

theorem lifeUnchanged_paid {u : Nat → Bool} {s t : Session} {ls : List Life}
    (un : LifeUnchanged u s ls) (h : live s ls = some t) : t.paid = s.paid := by
  induction ls generalizing s with
  | nil => simp [live] at h; subst h; rfl
  | cons l ls ih =>
      simp only [live, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      obtain ⟨ha, hl, hc, hf⟩ := un
      have hc' : ∀ q, Life.event (.commit q) ∈ ls → u q = true :=
        fun q m => hc q (List.mem_cons_of_mem _ m)
      have hf' : ∀ p, Life.event (.finish p) ∈ ls → u p = false :=
        fun p m => hf p (List.mem_cons_of_mem _ m)
      cases l with
      | reopen j =>
          simp [lifeStep] at hs; subst hs
          have keeps : LifeUnchanged u (s.reopen j) ls := by
            refine ⟨fun b hb => ?_, fun l r => by simp [Session.reopen] at r, hc', hf'⟩
            simp only [Session.reopen, Option.map_eq_some_iff] at hb
            obtain ⟨a, ha', rfl⟩ := hb
            exact ha a ha'
          rw [ih keeps hr]; rfl
      | event e =>
          have one : Unchanged u s [e] := by
            refine ⟨ha, hl, fun q m => ?_, fun p m => ?_⟩
            · simp at m; subst m; exact hc q List.mem_cons_self
            · simp at m; subst m; exact hf p List.mem_cons_self
          obtain ⟨⟨ha', hl', _, _⟩, paid⟩ := unchanged_step one hs
          rw [ih ⟨ha', hl', hc', hf'⟩ hr, paid]

/-- §3.1's bound for an unchanged wait across reopens: at most one start,
plus one per reopen, plus the elapsed time over `d`. Within one
uninterrupted run (no reopen) it is exactly `unchanged_rate`. -/
theorem reopen_unchanged_rate {d : Nat} {u : Nat → Bool} {s t : Session} {ls : List Life}
    (positive : 0 < d) (floor : d ≤ s.floor) (fresh : Fresh s)
    (noForward : ∀ j, Life.reopen j ∈ ls → j ≤ 0) (un : LifeUnchanged u s ls)
    (h : live s ls = some t) : lifeStarts ls ≤ 1 + reopens ls + lifeTicks ls / d := by
  have := reopen_rate positive floor fresh noForward h
  rw [lifeUnchanged_paid un h] at this
  omega

/-- A reopen with no attempt running adds no start to the count. -/
theorem idle_reopen_keeps_count {d n0 p0 : Nat} {s : Session} {k : Nat} {jump : Int}
    (inv : Inv d n0 p0 s k) (fresh : Fresh s) (idle : s.running = none) (back : jump ≤ 0) :
    Inv d n0 p0 (s.reopen jump) k := by
  obtain ⟨fl, hn, hp, hA, hB⟩ := inv
  have floor := reopen_floor s jump
  refine ⟨by rw [floor]; exact fl, hn, hp, hA, fun k1 _ => ?_⟩
  obtain ⟨a, ha, hd, _⟩ := hB k1 idle
  have later := honest_deadline fresh ha back
  exact ⟨_, reopen_armed jump ha, by simp [reopened]; omega, fun w => by simp [reopened] at w⟩

namespace Examples

/-- A capped wait on prerequisite 1, armed at time 0 for its full 300
seconds after ten failures. -/
def armedCapped : Session := ⟨0, 0, some ⟨10, 300, 1, false⟩, none⟩

/-- Ben's positions change before Ana closes: the wait is due at once. She
reopens: that commit is unseen, and the wait runs the rest of its delay, then
is due. A backward clock change arms no more than the full wait. -/
theorem reopen_forgets_evidence :
    (run armedCapped [.commit 1]).map (·.ready) = some true ∧
    ((run armedCapped [.commit 1]).map fun s => (s.reopen 0).ready) = some false ∧
    ((run armedCapped [.commit 1]).bind fun s => run (s.reopen 0) [.tick 299]).map
      (·.ready) = some false ∧
    ((run armedCapped [.commit 1]).bind fun s => run (s.reopen 0) [.tick 300]).map
      (·.ready) = some true ∧
    ((run armedCapped [.commit 1]).bind fun s => run (s.reopen (-100000)) [.tick 300]).map
      (·.ready) = some true := by
  decide

/-- The attempt that was running when the store closed is lost, and its
operation's previous wait was already due: it starts again at once, with no
time passing and no commit. This is the start per reopen `reopen_rate`
allows. -/
theorem reopen_mid_attempt_restarts :
    (live armedCapped [.event (.tick 300), .event .start, .reopen 0, .event .start]).isSome ∧
    lifeStarts [.event (.tick 300), .event .start, .reopen 0, .event .start] = 2 ∧
    lifeTicks [.event (.tick 300), .event .start, .reopen 0, .event .start] = 300 := by
  decide

/-- A wall clock moved forward makes the persisted deadline look past: the
reopened wait is due at once. -/
theorem forward_jump_ends_wait : (armedCapped.reopen 300).ready = true := by
  decide

end Examples

end CovenIO.Waiting
