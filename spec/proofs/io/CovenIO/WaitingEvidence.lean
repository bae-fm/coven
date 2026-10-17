import CovenIO.Waiting

/-! Evidence-triggered attempts (§6.7, "Waiting and
retrying", §21.2), as `retry_waits.rs` and `operation_evidence.rs` run them: an
operation's wait names the prerequisite its attempt ended on, and is due when
its monotonic deadline passes, or when a commit changing that prerequisite
arrives after the wait began. Due by evidence keeps the backoff: the next
failure still continues from the persisted count.

One session holds one operation's wait. `tick` advances the monotonic clock,
`commit q` is one commit by other work that changes prerequisite `q`, `start`
begins an automatic attempt (only when due), and `finish p` ends it
unsuccessfully, waiting on prerequisite `p`. A wait begins when the attempt
that ends in it started, so a commit changing `p` that lands while the attempt
runs makes it due at once (`racing_evidence_due`, `racing_commit_wakes`). A
commit changing any other prerequisite is no evidence for it
(`unrelated_commit_ignored`). The attempt's own commits are no `commit` event:
Rust attributes them to the attempt and leaves them out. A provider cooldown
only removes starts, so the bounds below hold with it too.

`Session.paid` is a ghost counter of the commits that woke a wait. Every start
after the first is paid for by a full delay or by one of them
(`evidence_rate`). A wait whose prerequisite no commit changes meets §21.2's
bound for such waits (`unchanged_rate`, `unchanged_one_second`,
`unchanged_capped`). -/

namespace CovenIO.Waiting

theorem delay_mono {a b : Nat} (h : a ≤ b) : delay a ≤ delay b := by
  have pow : 2 ^ a ≤ 2 ^ b := Nat.pow_le_pow_right (by decide) h
  simp only [delay, CovenStorage.Clocks.retryDelay]
  omega

/-- An armed wait: the persisted failure count, the monotonic deadline, the
prerequisite it names, and whether a commit changing that prerequisite
arrived since it began. -/
structure Armed where
  failures : Nat
  deadline : Nat
  prereq : Nat
  woken : Bool
  deriving DecidableEq, Repr

/-- Due by its timer, or by a commit changing its prerequisite. -/
def Armed.due (a : Armed) (now : Nat) : Bool :=
  decide (a.deadline ≤ now) || a.woken

/-- The wait after an unsuccessful attempt ending at `now` on prerequisite `p`:
the backoff continues from the previous count whatever made the attempt due,
and it is already woken when other work changed `p` while the attempt ran. -/
def rearm (previous : Option Armed) (now p : Nat) (raced : Bool) : Armed :=
  let failures := match previous with
    | some a => a.failures
    | none => 0
  ⟨failures + 1, now + delay failures, p, raced⟩

structure Session where
  now : Nat
  /-- Ghost: the commits that woke a wait so far. -/
  paid : Nat
  armed : Option Armed
  /-- While an attempt runs, the prerequisites other work changed since it
  started. -/
  running : Option (List Nat)
  deriving DecidableEq, Repr

inductive Event
  | tick (seconds : Nat)
  | commit (prereq : Nat)
  | start
  | finish (prereq : Nat)
  deriving DecidableEq, Repr

/-- No wait yet, or the armed wait is due; never while an attempt runs. -/
def Session.ready (s : Session) : Bool :=
  s.running.isNone && match s.armed with
    | none => true
    | some a => a.due s.now

/-- A commit wakes the armed wait when it changes that wait's prerequisite. -/
def wake (s : Session) (q : Nat) : Session :=
  match s.armed with
  | some a => if a.prereq = q ∧ a.woken = false then
      { s with armed := some { a with woken := true }, paid := s.paid + 1 } else s
  | none => s

def step (s : Session) : Event → Option Session
  | .tick n => some { s with now := s.now + n }
  | .commit q => some (match s.running with
      | some l => { s with running := some (q :: l) }
      | none => wake s q)
  | .start => if s.ready then some { s with running := some [] } else none
  | .finish p => s.running.map fun l =>
      { s with running := none, armed := some (rearm s.armed s.now p (l.contains p)),
               paid := s.paid + if l.contains p then 1 else 0 }

def run (s : Session) : List Event → Option Session
  | [] => some s
  | e :: es => (step s e).bind fun next => run next es

def starts : List Event → Nat
  | [] => 0
  | .start :: es => starts es + 1
  | _ :: es => starts es

def ticks : List Event → Nat
  | [] => 0
  | .tick n :: es => ticks es + n
  | _ :: es => ticks es

/-- The delay the next failure arms. -/
def Session.floor (s : Session) : Nat :=
  delay (match s.armed with
    | some a => a.failures
    | none => 0)

theorem wake_now (s : Session) (q : Nat) : (wake s q).now = s.now := by
  unfold wake; split
  · split <;> rfl
  · rfl

theorem run_now {s t : Session} {es : List Event} (h : run s es = some t) :
    t.now = s.now + ticks es := by
  induction es generalizing s with
  | nil => simp [run] at h; subst h; simp [ticks]
  | cons e es ih =>
      simp only [run, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      have := ih hr
      cases e with
      | tick n => simp [step] at hs; subst hs; simp [ticks] at *; omega
      | commit q =>
          simp only [step, Option.some.injEq] at hs; subst hs
          split at this
          · simpa [ticks] using this
          · rw [wake_now] at this; simpa [ticks] using this
      | start =>
          simp only [step] at hs
          split at hs <;> simp at hs; subst hs; simpa [ticks] using this
      | finish p =>
          cases hr : s.running with
          | none => simp [step, hr] at hs
          | some l => simp [step, hr] at hs; subst hs; simpa [ticks] using this

/-- The accounting behind the bound, for `k` starts so far from a session whose
clock read `n0` and paid count `p0`: every start after the first is paid by
`d` seconds or by one waking commit. -/
def Inv (d n0 p0 : Nat) (s : Session) (k : Nat) : Prop :=
  d ≤ s.floor ∧ n0 ≤ s.now ∧ p0 ≤ s.paid ∧
  (1 ≤ k → d * (k - 1) + d * p0 + n0 ≤ d * s.paid + s.now) ∧
  (1 ≤ k → s.running = none → ∃ a, s.armed = some a ∧
    d * k + d * p0 + n0 ≤ d * s.paid + a.deadline ∧
    (a.woken = true → d * k + d * p0 + n0 ≤ d * s.paid + s.now))

theorem inv_wake {d n0 p0 : Nat} {s : Session} {k : Nat} (q : Nat)
    (inv : Inv d n0 p0 s k) (idle : s.running = none) : Inv d n0 p0 (wake s q) k := by
  obtain ⟨fl, hn, hp, hA, hB⟩ := inv
  unfold wake
  split
  · rename_i a ha
    split
    · refine ⟨by simpa [Session.floor, ha] using fl, hn, by simp; omega, ?_, ?_⟩
      · intro k1; have := hA k1; simp [Nat.mul_add]; omega
      · intro k1 _
        obtain ⟨b, hb, deadline, _⟩ := hB k1 idle
        rw [ha] at hb; cases hb
        refine ⟨_, rfl, by simp [Nat.mul_add]; omega, fun _ => ?_⟩
        have := hA k1
        have shift : d * k = d * (k - 1) + d := by
          rw [← Nat.mul_succ]; congr 1; omega
        simp [Nat.mul_add]; omega
    · exact ⟨fl, hn, hp, hA, hB⟩
  · exact ⟨fl, hn, hp, hA, hB⟩

theorem inv_step {d n0 p0 : Nat} {s t : Session} {k : Nat} {e : Event}
    (inv : Inv d n0 p0 s k) (h : step s e = some t) :
    Inv d n0 p0 t (k + starts [e]) := by
  cases e with
  | tick n =>
      obtain ⟨fl, hn, hp, hA, hB⟩ := inv
      simp [step] at h; subst h
      simp only [starts, Nat.add_zero]
      refine ⟨fl, by simp; omega, hp, ?_, ?_⟩
      · intro k1; have := hA k1; simp; omega
      · intro k1 r; obtain ⟨a, ha, hd, hw⟩ := hB k1 r
        exact ⟨a, ha, hd, fun w => by have := hw w; simp; omega⟩
  | commit q =>
      simp only [step, Option.some.injEq] at h; subst h
      simp only [starts, Nat.add_zero]
      cases hr : s.running with
      | none => simpa using inv_wake q inv hr
      | some l =>
          obtain ⟨fl, hn, hp, hA, hB⟩ := inv
          dsimp only
          exact ⟨fl, hn, hp, hA, fun _ r => by simp at r⟩
  | start =>
      obtain ⟨fl, hn, hp, hA, hB⟩ := inv
      simp only [step] at h
      split at h
      · rename_i ready
        simp at h; subst h
        simp only [starts]
        have running : s.running = none := by
          simp [Session.ready] at ready; exact ready.1
        have paid : d * k + d * p0 + n0 ≤ d * s.paid + s.now := by
          by_cases k0 : k = 0
          · subst k0
            have : d * p0 ≤ d * s.paid := Nat.mul_le_mul_left d hp
            omega
          · obtain ⟨a, ha, hd, hw⟩ := hB (by omega) running
            simp [Session.ready, ha, Armed.due] at ready
            rcases ready.2 with timer | woken
            · omega
            · exact hw woken
        refine ⟨fl, hn, hp, fun _ => ?_, fun _ r => by simp at r⟩
        show d * (k + 1 - 1) + d * p0 + n0 ≤ d * s.paid + s.now
        rw [show k + 1 - 1 = k by omega]; exact paid
      · simp at h
  | finish p =>
      obtain ⟨fl, hn, hp, hA, hB⟩ := inv
      cases hr : s.running with
      | none => simp [step, hr] at h
      | some l =>
          simp [step, hr] at h; subst h
          simp only [starts, Nat.add_zero]
          have fl' : d ≤ delay ((match s.armed with | some a => a.failures | none => 0) + 1) :=
            Nat.le_trans fl (delay_mono (Nat.le_succ _))
          refine ⟨by simpa [Session.floor, rearm] using fl', hn, by simp; omega, ?_, ?_⟩
          · intro k1; have := hA k1; simp only [Nat.mul_add]; split <;> omega
          · intro k1 _
            have := hA k1
            have floor : d ≤ delay (match s.armed with | some a => a.failures | none => 0) := fl
            have shift : d * k = d * (k - 1) + d := by
              rw [← Nat.mul_succ]; congr 1; omega
            refine ⟨_, rfl, ?_, ?_⟩
            · simp only [rearm, Nat.mul_add]; split <;> omega
            · intro woken
              have hm : p ∈ l := by simpa [rearm] using woken
              simp only [hm, ite_true, Nat.mul_add, Nat.mul_one]
              omega

theorem inv_run {d n0 p0 : Nat} {s t : Session} {k : Nat} {es : List Event}
    (inv : Inv d n0 p0 s k) (h : run s es = some t) :
    Inv d n0 p0 t (k + starts es) := by
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
starts over a run are at most one, plus one per commit that woke a wait, plus
the elapsed time over `d`, for any `d` no longer than the delay the next
failure arms. -/
theorem evidence_rate {d : Nat} {s t : Session} {es : List Event} (positive : 0 < d)
    (floor : d ≤ s.floor) (h : run s es = some t) :
    starts es ≤ 1 + (t.paid - s.paid) + ticks es / d := by
  have inv0 : Inv d s.now s.paid s 0 :=
    ⟨floor, Nat.le_refl _, Nat.le_refl _, by intro h; omega, by intro h; omega⟩
  obtain ⟨_, _, hp, hA, _⟩ := inv_run inv0 h
  have tn := run_now h
  have nonneg := Nat.zero_le (ticks es / d)
  by_cases zero : starts es = 0
  · omega
  · have bound := hA (by omega)
    rw [tn, Nat.zero_add] at bound
    have spread : d * (t.paid - s.paid) = d * t.paid - d * s.paid := Nat.mul_sub _ _ _
    have grows : d * s.paid ≤ d * t.paid := Nat.mul_le_mul_left d hp
    have factor : (starts es - 1 - (t.paid - s.paid)) * d =
        d * (starts es - 1) - d * (t.paid - s.paid) := by
      rw [Nat.mul_comm, Nat.mul_sub]
    have scaled : (starts es - 1 - (t.paid - s.paid)) * d ≤ ticks es := by
      rw [factor, spread]
      omega
    have quotient : starts es - 1 - (t.paid - s.paid) ≤ ticks es / d :=
      (Nat.le_div_iff_mul_le positive).mpr scaled
    omega

/-! ## Unchanged waits -/

/-- The run's commits change only prerequisites in `unrelated`, its waits name
none of them, and the session's own wait and running attempt name none and
saw none: no wait in it ever changes (§21.2's wait whose prerequisite nothing
changes). -/
def Unchanged (unrelated : Nat → Bool) (s : Session) (es : List Event) : Prop :=
  (∀ a, s.armed = some a → unrelated a.prereq = false) ∧
  (∀ l, s.running = some l → ∀ q ∈ l, unrelated q = true) ∧
  (∀ q, Event.commit q ∈ es → unrelated q = true) ∧
  (∀ p, Event.finish p ∈ es → unrelated p = false)

theorem unchanged_step {u : Nat → Bool} {s t : Session} {e : Event} {es : List Event}
    (un : Unchanged u s (e :: es)) (h : step s e = some t) :
    Unchanged u t es ∧ t.paid = s.paid := by
  obtain ⟨ha, hl, hc, hf⟩ := un
  have hc' : ∀ q, Event.commit q ∈ es → u q = true :=
    fun q m => hc q (List.mem_cons_of_mem _ m)
  have hf' : ∀ p, Event.finish p ∈ es → u p = false :=
    fun p m => hf p (List.mem_cons_of_mem _ m)
  cases e with
  | tick n => simp [step] at h; subst h; exact ⟨⟨ha, hl, hc', hf'⟩, rfl⟩
  | commit q =>
      have uq := hc q List.mem_cons_self
      simp only [step, Option.some.injEq] at h; subst h
      cases hr : s.running with
      | some l =>
          dsimp only
          refine ⟨⟨ha, fun l' r => ?_, hc', hf'⟩, by simp⟩
          simp at r; subst r
          intro x hx
          rcases List.mem_cons.mp hx with rfl | hx
          · exact uq
          · exact hl l hr x hx
      | none =>
          dsimp only
          unfold wake
          split
          · rename_i a hs
            have named := ha a hs
            split
            · rename_i hit; rw [hit.1] at named; rw [named] at uq; cases uq
            · exact ⟨⟨ha, hl, hc', hf'⟩, rfl⟩
          · exact ⟨⟨ha, hl, hc', hf'⟩, rfl⟩
  | start =>
      simp only [step] at h
      split at h
      · simp at h; subst h
        exact ⟨⟨ha, fun l r => by simp at r; subst r; simp, hc', hf'⟩, rfl⟩
      · simp at h
  | finish p =>
      have up := hf p List.mem_cons_self
      cases hr : s.running with
      | none => simp [step, hr] at h
      | some l =>
          simp [step, hr] at h; subst h
          have absent : l.contains p = false := by
            cases hx : l.contains p
            · rfl
            · have := hl l hr p (by simpa using hx); rw [this] at up; cases up
          have outside : p ∉ l := by simpa using absent
          refine ⟨⟨fun a r => ?_, fun l r => by simp at r, hc', hf'⟩, by simp [outside]⟩
          simp at r; subst r; exact up

theorem unchanged_paid {u : Nat → Bool} {s t : Session} {es : List Event}
    (un : Unchanged u s es) (h : run s es = some t) : t.paid = s.paid := by
  induction es generalizing s with
  | nil => simp [run] at h; subst h; rfl
  | cons e es ih =>
      simp only [run, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      obtain ⟨un', paid⟩ := unchanged_step un hs
      rw [ih un' hr, paid]

/-- §21.2's bound for one wait whose prerequisite nothing changes: with no
commit changing what its waits name, starts are at most one plus the elapsed
time over `d`. -/
theorem unchanged_rate {d : Nat} {u : Nat → Bool} {s t : Session} {es : List Event}
    (positive : 0 < d) (floor : d ≤ s.floor) (un : Unchanged u s es)
    (h : run s es = some t) : starts es ≤ 1 + ticks es / d := by
  have := evidence_rate positive floor h
  rw [unchanged_paid un h] at this
  simpa using this

theorem unchanged_one_second {u : Nat → Bool} {s t : Session} {es : List Event}
    (un : Unchanged u s es) (h : run s es = some t) : starts es ≤ 1 + ticks es := by
  simpa using unchanged_rate (d := 1) (by decide) (delay_positive _) un h

theorem unchanged_capped {u : Nat → Bool} {s t : Session} {es : List Event} {a : Armed}
    (armed : s.armed = some a) (capped : 9 ≤ a.failures) (un : Unchanged u s es)
    (h : run s es = some t) : starts es ≤ 1 + ticks es / 300 := by
  have floor : 300 ≤ s.floor := by
    simp only [Session.floor, armed]
    have := delay_at_cap (a.failures - 9)
    rw [show a.failures - 9 + 9 = a.failures by omega] at this
    omega
  exact unchanged_rate (by decide) floor un h

/-- `n` waits sharing one run's clock: §21.2's `n × (1 + ⌊t / d⌋)`. -/
theorem unchanged_waits_rate (bounds : List Nat) (b : Nat) (each : ∀ n ∈ bounds, n ≤ b) :
    bounds.sum ≤ bounds.length * b := by
  induction bounds with
  | nil => simp
  | cons n rest ih =>
      have head := each n (by simp)
      have tail := ih fun m hm => each m (List.mem_cons_of_mem _ hm)
      simp only [List.sum_cons, List.length_cons, Nat.succ_mul]
      omega

/-! ## Due once, racing commits -/

/-- Due once: the wait an attempt arms, when no other work changed its
prerequisite while it ran, is not due again at the same instant; a commit
changing its prerequisite or its delay is needed. -/
theorem evidence_due_once (previous : Option Armed) (now p : Nat) :
    (rearm previous now p false).due now = false := by
  cases previous with
  | none =>
      have := delay_positive 0
      simp [rearm, Armed.due]; omega
  | some a =>
      have := delay_positive a.failures
      simp only [rearm, Armed.due, Bool.or_false]
      exact decide_eq_false (by omega)

/-- A commit changing the prerequisite while the attempt ran is evidence
against the wait it ends in: that wait began when the attempt started, so it
is due at once. -/
theorem racing_evidence_due (previous : Option Armed) (now p : Nat) :
    (rearm previous now p true).due now = true := by
  simp [rearm, Armed.due]

/-- From any session: an attempt during which other work changes the
prerequisite it ends on ends in a wait that is due at once. -/
theorem racing_commit_wakes {s t : Session} (p : Nat)
    (h : run s [.start, .commit p, .finish p] = some t) : t.ready = true := by
  cases ready : s.ready
  · simp [run, step, ready] at h
  · simp [run, step, ready] at h
    subst h
    simp [Session.ready, Armed.due, rearm]

/-- A commit changing another prerequisite leaves the wait as it was. -/
theorem unrelated_commit_ignored (s : Session) (a : Armed) (q : Nat)
    (armed : s.armed = some a) (idle : s.running = none) (other : a.prereq ≠ q) :
    step s (.commit q) = some s := by
  simp [step, idle, wake, armed, other]

/-- Evidence never resets the backoff: the next wait continues the count. -/
theorem evidence_keeps_backoff (a : Armed) (now p : Nat) (raced : Bool) :
    (rearm (some a) now p raced).failures = a.failures + 1 ∧
    (rearm (some a) now p raced).deadline = now + delay a.failures := ⟨rfl, rfl⟩

namespace Examples

/-- A capped wait (nine failures) on prerequisite 1 that has just armed 300
seconds. -/
def capped : Session := ⟨0, 0, some ⟨9, 300, 1, false⟩, none⟩

/-- Three commits at one instant changing other prerequisites, for example
three other operations advancing in one drive: the capped wait stays waiting,
so no attempt starts. Counting every prerequisite commit for every wait gave
three starts here where §21.2 allows one. -/
theorem unrelated_burst_starts_none :
    (run capped [.commit 2, .commit 3, .commit 4]).map (·.ready) = some false ∧
    run capped [.commit 2, .start] = none := by
  decide

/-- Commits changing the wait's own prerequisite each wake it: the wait is not
unchanged, and each start is paid by one of them. -/
def own : List Event :=
  [.commit 1, .start, .finish 1, .commit 1, .start, .finish 1, .commit 1, .start, .finish 1]

theorem own_commits_each_wake :
    (run capped own).isSome ∧ starts own = 3 ∧ ticks own = 0 ∧
    (run capped own).map (·.paid) = some 3 := by
  decide

/-- A commit racing an attempt: the prerequisite changes after the attempt read
it and before the attempt failed. The wait armed afterward began with the
attempt, so the commit wakes it at once although its delay is 300 seconds. -/
theorem racing_commit_wakes_at_once :
    (run capped [.commit 1, .start, .commit 1, .finish 1]).map
      (fun s => (s.ready, s.armed.map (·.deadline))) = some (true, some 300) := by
  decide

/-- Without a racing commit, or with one changing another prerequisite, the
same attempt waits its full delay. -/
theorem unraced_waits_full_delay :
    (run capped [.commit 1, .start, .finish 1]).map
      (fun s => (s.ready, s.armed.map (·.deadline))) = some (false, some 300) ∧
    (run capped [.commit 1, .start, .commit 2, .finish 1]).map
      (fun s => (s.ready, s.armed.map (·.deadline))) = some (false, some 300) := by
  decide

end Examples

end CovenIO.Waiting
