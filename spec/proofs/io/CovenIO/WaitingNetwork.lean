import CovenIO.WaitingReopen

/-! The app's network report (§6.7, "Waiting and retrying"; api.md E5
`network_changed`), as `RetryWaits::end_offline_waits` runs it for the
connection's wait and for each operation's, upload's and download's wait.

Every wait names what its failed attempt lacked (`WaitingEvidence`'s
`Armed.prereq`). An attempt that received no provider response lacked a
usable network, so its wait names `network`. An attempt the provider
answered, with a refusal or a server error, names something else: nothing the
device's network does changes that answer. The app's report that the network
changed is a commit changing `network`: evidence against a wait that names
it, and none against any other wait. Each wait is its own session; one report
is one commit to each of them. A `commit` below is any evidence its wait
names: a report, or for an operation's wait another change it names.

Proved here: a report ends a wait for the network at once
(`signal_ends_network_wait`) and keeps its backoff (`signal_keeps_backoff`).
It leaves a wait for an answered failure as it was
(`signal_keeps_answered_wait`), and never ends a provider cooldown
(`signal_keeps_cooldown`). Repeated reports coalesce: a second report to a
wait already ended changes nothing (`signals_coalesce`). A report while an
attempt runs ends the wait that attempt fails into
(`signal_during_attempt_wakes`). Every start after the first is paid for by a
full delay or by a commit, so in t seconds a wait starts at most
1 + commits + ⌊t / d⌋ times (`signal_rate`), and n waits sharing the reports
at most n times that (`signal_waits_rate`).

Rust ends a wait durably: it saves the wait with deadline `T = now` and full
wait `W = 0`, keeping the failure count, so a reopen arms no wait under any
wall clock (`ended_reopens_due`). Keeping `W` would not do: once the wall
clock moves back, a reopen arms the old wait again (`kept_wait_returns`). -/

namespace CovenIO.Waiting

/-- What the wait of an attempt that received no provider response names. -/
def network : Nat := 0

/-- A signal ends an idle wait for the network: the next attempt may start. -/
theorem signal_ends_network_wait (s : Session) (a : Armed) (armed : s.armed = some a)
    (idle : s.running = none) (names : a.prereq = network) :
    (step s (.commit network)).map (·.ready) = some true := by
  cases woken : a.woken <;>
    simp [step, idle, wake, armed, names, woken, Session.ready, Armed.due]

/-- A wait for an answered failure ignores the signal. -/
theorem signal_keeps_answered_wait (s : Session) (a : Armed) (armed : s.armed = some a)
    (idle : s.running = none) (answered : a.prereq ≠ network) :
    step s (.commit network) = some s :=
  unrelated_commit_ignored s a network armed idle answered

/-- A second signal to a wait the first one ended changes nothing. -/
theorem signals_coalesce (s : Session) (a : Armed) (armed : s.armed = some a)
    (idle : s.running = none) :
    run s [.commit network, .commit network] = run s [.commit network] := by
  cases woken : a.woken <;> by_cases names : a.prereq = network <;>
    simp [run, step, idle, wake, armed, woken, names]

/-- A signal while an attempt runs ends the wait it fails into for want of
the network. -/
theorem signal_during_attempt_wakes {s t : Session}
    (h : run s [.start, .commit network, .finish network] = some t) : t.ready = true :=
  racing_commit_wakes network h

/-- The wait after the next failure continues the count. -/
theorem signal_keeps_backoff (a : Armed) (now : Nat) (raced : Bool) :
    (rearm (some a) now network raced).failures = a.failures + 1 :=
  (evidence_keeps_backoff a now network raced).1

/-- Ending the ordinary wait, as the app's retry does, leaves a provider
cooldown in force: an attempt it lets start is past the cooldown. -/
theorem signal_keeps_cooldown (t : Timer) (now : Nat)
    (ready : due (appRetry t now) now = true) : t.provider ≤ now :=
  app_retry_keeps_cooldown t now ready

/-! ## Each commit pays for at most one start -/

/-- The commits in a run: reports, and other evidence its wait names. -/
def commits : List Event → Nat
  | [] => 0
  | .commit _ :: es => commits es + 1
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
def locked : Session := ⟨0, 0, some ⟨9, 300, network, false⟩, none⟩

/-- She unlocks it after 20 seconds and the app signals twice: the first signal
ends the wait, the second adds nothing, and one attempt starts at once. -/
theorem unlocked_starts_at_once :
    (run locked [.tick 20, .commit network, .commit network, .start]).map (·.paid) =
      some 1 := by
  decide

/-- The same signal leaves a wait for a provider's answer, here a server
error, at its 300 seconds. -/
theorem answered_keeps_waiting :
    run ⟨0, 0, some ⟨9, 300, 1, false⟩, none⟩
      [.tick 20, .commit network, .commit network, .start] = none := by
  decide

end Examples

end CovenIO.Waiting
