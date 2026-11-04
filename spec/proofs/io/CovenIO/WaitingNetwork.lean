import CovenIO.WaitingReopen

/-! The network report, which coven makes as it sees the device reach a usable
network (§6.7, "Waiting and retrying"; api.md E5 "The device's network"), and
the person's retry (`retry_now`), as
`RetryWaits::end_reported_waits` runs them for the connection's wait and for
each operation's and upload's wait. A download has no wait: only the app's
next read or pin tries it again (`Downloads.canRetry`).

Every wait names what its failed attempt lacked (`WaitingEvidence`'s
`Armed.prereq`). An attempt that received no provider response lacked a
usable network, so its wait names `network`. An attempt the provider
answered, with a refusal or a server error, names something else: nothing the
device's network does changes that answer. Each wait is its own session; one
report is one `report` event to each of them. The network report names a wait
for the network, and the person's retry names every wait.

An owner acts on reports between attempts: the connection after the pass in
flight, the upload worker as the attempt ends. So a report that arrives while
an attempt runs is a `report` right after that attempt's `finish`. A report
ends a wait it names that its timer hasn't ended yet, and Rust saves the end:
the wait's deadline `T = now` with full wait `W = 0`, and the failure count
kept (`endWait`). A wait the timer already ended needs no end.

Proved here: a report ends a wait for the network at once
(`signal_ends_network_wait`) and keeps its backoff (`signal_keeps_backoff`).
It leaves a wait for an answered failure as it was
(`signal_keeps_answered_wait`), and never ends a provider cooldown
(`signal_keeps_cooldown`). Repeated reports coalesce: a second report to a
wait already ended changes nothing (`signals_coalesce`). A report while an
attempt runs ends the wait that attempt fails into
(`signal_during_attempt_wakes`). The ended wait is saved ended: reopened under
any wall clock, it is due at once (`report_survives_reopen`); keeping its full
wait would bring it back once the wall clock moved back (`kept_wait_returns`).
Every start after the first is paid for by a full delay, a commit or a report,
so in t seconds a wait starts at most 1 + commits + ⌊t / d⌋ times
(`signal_rate`), and n waits sharing the reports at most n times that
(`signal_waits_rate`). -/

namespace CovenIO.Waiting

/-- The network report ends an idle wait for the network: the next attempt
may start. -/
theorem signal_ends_network_wait (s : Session) (a : Armed) (armed : s.armed = some a)
    (idle : s.running = none) (names : a.prereq = network) :
    (step s (.report false)).map (·.ready) = some true := by
  by_cases pending : s.now < a.deadline
  · simp [step, idle, endWait, armed, names, pending, Session.ready, Armed.due]
  · simp [step, idle, endWait, armed, names, pending, Session.ready, Armed.due]
    omega

/-- A wait for an answered failure ignores the network report. -/
theorem signal_keeps_answered_wait (s : Session) (a : Armed) (armed : s.armed = some a)
    (idle : s.running = none) (answered : a.prereq ≠ network) :
    step s (.report false) = some s := by
  simp [step, idle, endWait, armed, answered]

/-- A second report to a wait the first one ended changes nothing. -/
theorem signals_coalesce (s : Session) (every : Bool) :
    run s [.report every, .report every] = run s [.report every] := by
  cases idle : s.running with
  | some l => simp [run, step, idle]
  | none =>
      cases armed : s.armed with
      | none => simp [run, step, idle, endWait, armed]
      | some a =>
          by_cases cond : (every = true ∨ a.prereq = network) ∧ s.now < a.deadline
          · simp [run, step, idle, endWait, armed, cond]
          · simp [run, step, idle, endWait, armed, cond]

/-- A report while an attempt runs, acted on as the attempt ends, ends the
wait the attempt fails into for want of the network. -/
theorem signal_during_attempt_wakes {s t : Session}
    (h : run s [.start, .finish network, .report false] = some t) : t.ready = true := by
  cases ready : s.ready
  · simp [run, step, ready] at h
  · have positive (f : Nat) : 0 < delay f := delay_positive f
    cases armed : s.armed <;>
      simp [run, step, ready, endWait, rearm, armed, positive] at h <;>
      (subst h; simp [Session.ready, Armed.due])

/-- The wait after the next failure continues the count. -/
theorem signal_keeps_backoff (a : Armed) (now : Nat) (raced : Bool) :
    (rearm (some a) now network raced).failures = a.failures + 1 :=
  (evidence_keeps_backoff a now network raced).1

/-- Ending the ordinary wait, as the app's retry does, leaves a provider
cooldown in force: an attempt it lets start is past the cooldown. -/
theorem signal_keeps_cooldown (t : Timer) (now : Nat)
    (ready : due (appRetry t now) now = true) : t.provider ≤ now :=
  app_retry_keeps_cooldown t now ready

/-- A report saves the wait it ends: reopened under any wall clock, the wait
is due at once, with its failure count. -/
theorem report_survives_reopen {s t : Session} {every : Bool} {a : Armed} (jump : Int)
    (armed : s.armed = some a) (names : every = true ∨ a.prereq = network)
    (pending : s.now < a.deadline) (h : step s (.report every) = some t) :
    (t.reopen jump).ready = true ∧ (t.reopen jump).armed.map (·.failures) = some a.failures := by
  have cond : (every = true ∨ a.prereq = network) ∧ s.now < a.deadline := ⟨names, pending⟩
  simp only [step] at h
  split at h
  · simp at h; subst h
    simp [endWait, armed, cond, Session.reopen, reopened, restartArm, restart, Armed.wait,
      Session.ready, Armed.due]
    omega
  · simp at h

/-! ## The person's retry

`retry_now` is the person asking to retry everything now, such as by pulling
to refresh. Rust reports it to every owner as it reports a network change,
and each owner ends every ordinary wait it retries, whatever the wait names:
a `report` with `every`. So it ends every idle wait (`retry_ends_wait`), keeps
its failure count (`retry_keeps_backoff`), ends the wait an attempt running
meanwhile fails into (`retry_during_attempt_wakes`), saves the end as a
network report does (`report_survives_reopen`), and never ends a provider
cooldown (`retry_keeps_cooldown`). It adds at most one start to each wait, as
a network report does (`signal_rate`). -/

/-- The person's retry ends an idle wait, whatever it names. -/
theorem retry_ends_wait (s : Session) (a : Armed) (armed : s.armed = some a)
    (idle : s.running = none) : (step s (.report true)).map (·.ready) = some true := by
  by_cases pending : s.now < a.deadline
  · simp [step, idle, endWait, armed, pending, Session.ready, Armed.due]
  · simp [step, idle, endWait, armed, pending, Session.ready, Armed.due]
    omega

/-- It keeps the failure count, so a retry that fails again waits the next
delay rather than starting the backoff over. -/
theorem retry_keeps_backoff (s : Session) (idle : s.running = none) :
    (step s (.report true)).map (·.armed.map (·.failures)) = some (s.armed.map (·.failures)) := by
  simp [step, idle, endWait_failures]

/-- During an attempt, it ends the wait the attempt fails into. -/
theorem retry_during_attempt_wakes (p : Nat) {s t : Session}
    (h : run s [.start, .finish p, .report true] = some t) : t.ready = true := by
  cases ready : s.ready
  · simp [run, step, ready] at h
  · have positive (f : Nat) : 0 < delay f := delay_positive f
    cases armed : s.armed <;>
      simp [run, step, ready, endWait, rearm, armed, positive] at h <;>
      (subst h; simp [Session.ready, Armed.due])

/-- It never ends a provider cooldown: an attempt it lets start is past it. -/
theorem retry_keeps_cooldown (t : Timer) (now : Nat)
    (ready : due (appRetry t now) now = true) : t.provider ≤ now :=
  app_retry_keeps_cooldown t now ready

/-! ## Each commit pays for at most one start -/

/-- The commits and reports in a run. -/
def commits : List Event → Nat
  | [] => 0
  | .commit _ :: es => commits es + 1
  | .report _ :: es => commits es + 1
  | _ :: es => commits es

/-- Commits a running attempt has seen and not yet paid for. -/
def Session.credit (s : Session) : Nat :=
  match s.running with
  | some l => l.length
  | none => 0

theorem wake_paid (s : Session) (q : Nat) : (wake s q).paid ≤ s.paid + 1 := by
  unfold wake; split
  · split <;> simp
  · simp

theorem wake_running (s : Session) (q : Nat) : (wake s q).running = s.running := by
  unfold wake; split
  · split <;> rfl
  · rfl

theorem paid_step {s t : Session} {e : Event} (h : step s e = some t) :
    t.paid + t.credit ≤ s.paid + s.credit + commits [e] := by
  cases e with
  | tick n => simp [step] at h; subst h; simp [Session.credit, commits]
  | commit q =>
      simp only [step, Option.some.injEq] at h; subst h
      cases hr : s.running with
      | some l => simp [Session.credit, commits, hr]; omega
      | none =>
          have := wake_paid s q
          simp [Session.credit, commits, wake_running, hr]
          omega
  | start =>
      simp only [step] at h
      split at h
      · simp at h; subst h; simp [Session.credit, commits]
      · simp at h
  | finish p =>
      cases hr : s.running with
      | none => simp [step, hr] at h
      | some l =>
          simp [step, hr] at h; subst h
          simp only [Session.credit, hr, commits]
          by_cases hm : p ∈ l
          · have : 0 < l.length := List.length_pos_of_mem hm
            simp [hm]; omega
          · simp [hm]
  | report every =>
      simp only [step] at h
      split at h
      · rename_i idle
        simp at h; subst h
        have running : s.running = none := by simpa using idle
        simp only [Session.credit, endWait_running, running, commits]
        unfold endWait; split
        · split <;> simp
        · simp
      · simp at h

theorem paid_run {s t : Session} {es : List Event} (h : run s es = some t) :
    t.paid + t.credit ≤ s.paid + s.credit + commits es := by
  induction es generalizing s with
  | nil => simp [run] at h; subst h; simp [commits]
  | cons e es ih =>
      simp only [run, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      have first := paid_step hs
      have rest := ih hr
      have split : commits (e :: es) = commits [e] + commits es := by
        cases e <;> simp [commits] <;> omega
      rw [split]; omega

/-- From a session with no attempt running, starts over a run are at most one,
plus one per commit, plus the elapsed time over `d`, for any `d` no longer than
the delay the next failure arms. -/
theorem signal_rate {d : Nat} {s t : Session} {es : List Event} (positive : 0 < d)
    (floor : d ≤ s.floor) (idle : s.running = none) (h : run s es = some t) :
    starts es ≤ 1 + commits es + ticks es / d := by
  have rate := evidence_rate positive floor h
  have paid := paid_run h
  simp [Session.credit, idle] at paid
  omega

/-- One wait's run: its starting session, where it ended, and its events. -/
structure WaitRun where
  start : Session
  finish : Session
  events : List Event

/-- Waits sharing a run's clock and reports: each runs from no attempt, with
no delay below `d`, over at most `t` seconds and `r` commits. Together they
start at most `n × (1 + r + ⌊t / d⌋)` times. -/
theorem signal_waits_rate {d t r : Nat} (positive : 0 < d) (waits : List WaitRun)
    (valid : ∀ w ∈ waits, run w.start w.events = some w.finish)
    (idle : ∀ w ∈ waits, w.start.running = none)
    (floor : ∀ w ∈ waits, d ≤ w.start.floor)
    (time : ∀ w ∈ waits, ticks w.events ≤ t)
    (reports : ∀ w ∈ waits, commits w.events ≤ r) :
    (waits.map (starts ·.events)).sum ≤ waits.length * (1 + r + t / d) := by
  induction waits with
  | nil => simp
  | cons w rest ih =>
      have head := signal_rate positive (floor w (by simp)) (idle w (by simp))
        (valid w (by simp))
      have slower : ticks w.events / d ≤ t / d := Nat.div_le_div_right (time w (by simp))
      have fewer := reports w (by simp)
      have tail := ih (fun x m => valid x (List.mem_cons_of_mem _ m))
        (fun x m => idle x (List.mem_cons_of_mem _ m))
        (fun x m => floor x (List.mem_cons_of_mem _ m))
        (fun x m => time x (List.mem_cons_of_mem _ m))
        (fun x m => reports x (List.mem_cons_of_mem _ m))
      simp only [List.map_cons, List.sum_cons, List.length_cons, Nat.succ_mul]
      omega

/-! ## Ending the wait survives a reopen -/

/-- A wait ended at wall time `T` and saved with full wait zero reopens due,
whatever the wall clock reads at the reopen. -/
theorem ended_reopens_due (ended reopened : Int) : restart ended reopened 0 = 0 := by
  unfold restart; omega

/-- Moving only the deadline is not enough: a wall clock moved back 100
seconds before the reopen brings back 100 seconds of a 300-second wait. -/
theorem kept_wait_returns : restart 1000 900 300 = 100 := by
  unfold restart; omega

namespace Examples

/-- Ana's phone, locked on a train: its connection wait is capped at 300
seconds and names the network. -/
def locked : Session := ⟨0, 0, some ⟨9, 300, network, false, false⟩, none⟩

/-- She unlocks it after 20 seconds and coven reports twice: the first report
ends the wait, the second adds nothing, and one attempt starts at once. -/
theorem unlocked_starts_at_once :
    (run locked [.tick 20, .report false, .report false, .start]).map (·.paid) =
      some 1 := by
  decide

/-- The same signal leaves a wait for a provider's answer, here a server
error, at its 300 seconds. -/
theorem answered_keeps_waiting :
    run ⟨0, 0, some ⟨9, 300, 1, false, false⟩, none⟩
      [.tick 20, .report false, .report false, .start] = none := by
  decide

end Examples

end CovenIO.Waiting
