/-! The answer to the person's retry (§6.7, "The person's retry ends every
ordinary wait"; api.md E5 `retry_now`), as `RetryWaits::retry_requested`,
`sync_loop.rs` and the connection's first attempt in `storage_control.rs` run
it.

A retry request is a report (`WaitingNetwork`): it takes the next report
number. The connection's owner acts on reports when it plans, between passes
(`plan`), and remembers how far it acted (`handled`). A pass that begins takes
every waiting request the owner has acted on; a request made after the owner
last planned, such as one made while a pass runs, waits for a later pass. When
the pass ends, its requests are answered with its result: synced or failed.

Before a pass begins, the owner may find it held (`wait`): by a provider
cooldown, or by a failure only new credentials, an update or a reset can
clear. A held pass would leave the person waiting for something a pull cannot
change, so the owner answers the requests it acted on at once: cooling down,
or failed.

Stopping sync answers every waiting request "not syncing", and a request made
while sync is stopped is answered so at once: no pass will follow it. A pass
already running when sync stops still ends, and answers its own requests with
its result. The connection's first attempt is the start of the first pass: if
it fails, it ends as a failed pass; if it connects (`connected`), its requests
wait again for the loop's first pass; if sync stopped meanwhile, they are
answered "not syncing".

Rust keeps the requests and whether a started connection will plan under one
lock, so a request, stopping and a pass taking its requests each happen at
once, as one step here.

Proved here: an answer never changes once given (`answer_final`), so each
request is answered at most once. A request waits only while a started
connection will plan (`waits_only_while_syncing`). A pass answers only
requests the owner acted on before it began (`pass_answers_acted_on`), so a
pass already running when the person asks does not answer them
(`running_pass_skips`); the next one does (`next_pass_answers`). Concurrent
requests share the first pass after them (`concurrent_requests_share`). A
held pass answers at once and runs nothing (`held_answers`,
`held_runs_no_pass`). Asking while stopped, or on a closed handle, answers
"not syncing" at once (`stopped_answers_at_once`); stopping during a pass
leaves that pass's answer to the pass (`stop_lets_pass_answer`). And no
request is left waiting: from every reachable state, the owner's own steps
answer every request (`never_stranded`). What a retry made now would do, as
Rust publishes it, is how a request made then is answered: at once with the
answer it names (`published_answers_at_once`), or by the next pass
(`published_pass_runs`). Only starting, stopping and a change of hold change
it (`effect_steady`), and Rust publishes each. -/

namespace CovenIO.RetryAnswer

/-- What the first pass after a request came to. -/
inductive Outcome
  | synced
  | failed
  | coolingDown
  | notSyncing
  deriving DecidableEq, Repr

/-- What can hold the next pass before it begins. -/
inductive Held
  /-- A provider cooldown: the person's retry never ends it. -/
  | cooldown
  /-- A failure only new credentials, an update or a reset can clear. -/
  | failure
  deriving DecidableEq, Repr

def Held.outcome : Held → Outcome
  | .cooldown => .coolingDown
  | .failure => .failed

/-- How a pass ended: it reached its read end, it failed, or the connection's
first attempt found sync stopped before any pass could run. -/
inductive PassEnd
  | synced
  | failed
  | stopped
  deriving DecidableEq, Repr

def PassEnd.outcome : PassEnd → Outcome
  | .synced => .synced
  | .failed => .failed
  | .stopped => .notSyncing

/-- Where one report number's request stands. Numbers of other reports, such
as `network_changed`, and numbers not reached yet stay `unasked`. -/
inductive Phase
  | unasked
  | waiting
  | inPass
  | answered (o : Outcome)
  deriving DecidableEq, Repr

structure Loop where
  /-- Reports so far; each request is one. -/
  reported : Nat
  /-- Reports the connection's owner has acted on. -/
  handled : Nat
  /-- A started connection will plan. -/
  syncing : Bool
  hold : Option Held
  /-- A pass, or the connection's first attempt, is running. -/
  running : Bool
  request : Nat → Phase

inductive Event
  /-- `retry_now`. -/
  | ask
  /-- Another report: `network_changed`. -/
  | report
  /-- `start_sync`, or setup starting a new connection's loop. -/
  | start
  /-- `stop_sync`, closing the handle, disconnecting, or a failure that stops
  the loop. -/
  | stop
  /-- A cooldown or a held failure begins or ends. -/
  | held (h : Option Held)
  /-- The owner, between passes, acts on the reports so far. -/
  | plan
  /-- The owner finds the pass held and answers the requests it acted on. -/
  | wait
  /-- A pass begins and takes the requests the owner acted on. -/
  | begin
  /-- The connection's first attempt connects; the loop's first pass follows. -/
  | connected
  | finish (e : PassEnd)
  deriving DecidableEq, Repr

/-- Every request in phase `p` that `pick` selects moves to `q`. -/
def Loop.move (s : Loop) (p q : Phase) (pick : Nat → Bool) : Nat → Phase :=
  fun m => if s.request m = p ∧ pick m = true then q else s.request m

def step (s : Loop) : Event → Option Loop
  | .ask =>
      some { s with
        reported := s.reported + 1
        request := fun m =>
          if m = s.reported + 1 then
            (if s.syncing then .waiting else .answered .notSyncing)
          else s.request m }
  | .report => some { s with reported := s.reported + 1 }
  | .start => some { s with syncing := true }
  | .stop =>
      some { s with syncing := false,
                    request := s.move .waiting (.answered .notSyncing) (fun _ => true) }
  | .held h => some { s with hold := h }
  | .plan =>
      if s.syncing = true ∧ s.running = false then some { s with handled := s.reported }
      else none
  | .wait =>
      match s.hold with
      | some h =>
          if s.syncing = true ∧ s.running = false then
            some { s with request := s.move .waiting (.answered h.outcome)
                                       (fun m => decide (m ≤ s.handled)) }
          else none
      | none => none
  | .begin =>
      if s.syncing = true ∧ s.running = false ∧ s.hold = none then
        some { s with running := true,
                      request := s.move .waiting .inPass (fun m => decide (m ≤ s.handled)) }
      else none
  | .connected =>
      if s.running = true then
        some { s with running := false,
                      request := s.move .inPass
                        (if s.syncing then .waiting else .answered .notSyncing) (fun _ => true) }
      else none
  | .finish e =>
      if s.running = true then
        some { s with running := false,
                      request := s.move .inPass (.answered e.outcome) (fun _ => true) }
      else none

def run (s : Loop) : List Event → Option Loop
  | [] => some s
  | e :: es => (step s e).bind fun next => run next es

/-- An open handle before sync starts. -/
def initial : Loop := ⟨0, 0, false, none, false, fun _ => .unasked⟩

/-- What every reachable state keeps. -/
structure Inv (s : Loop) : Prop where
  fresh : ∀ m, s.reported < m → s.request m = .unasked
  owned : s.syncing = false → ∀ m, s.request m ≠ .waiting
  taken : s.running = false → ∀ m, s.request m ≠ .inPass
  acted : ∀ m, s.request m = .inPass → m ≤ s.handled
  within : s.handled ≤ s.reported

theorem initial_inv : Inv initial :=
  ⟨fun _ _ => rfl, fun _ _ h => Phase.noConfusion h, fun _ _ h => Phase.noConfusion h,
    fun _ h => Phase.noConfusion h, Nat.le_refl 0⟩

theorem move_apply (s : Loop) (p q : Phase) (pick : Nat → Bool) (m : Nat) :
    s.move p q pick m = if s.request m = p ∧ pick m = true then q else s.request m := rfl

theorem inv_step {s t : Loop} {e : Event} (inv : Inv s) (h : step s e = some t) : Inv t := by
  cases e with
  | ask =>
      simp only [step, Option.some.injEq] at h; subst h
      refine ⟨?_, ?_, ?_, ?_, ?_⟩
      · intro m hm
        have ne : m ≠ s.reported + 1 := by simp at hm; omega
        simp only [ne, ite_false]
        exact inv.fresh m (by simp at hm; omega)
      · intro hs m
        simp only at hs ⊢
        by_cases hm : m = s.reported + 1
        · simp [hm, hs]
        · simp only [hm, ite_false]; exact inv.owned hs m
      · intro hr m
        simp only at hr ⊢
        by_cases hm : m = s.reported + 1
        · cases s.syncing <;> simp [hm]
        · simp only [hm, ite_false]; exact inv.taken hr m
      · intro m hp
        simp only at hp ⊢
        by_cases hm : m = s.reported + 1
        · cases hs : s.syncing <;> simp [hm, hs] at hp
        · simp only [hm, ite_false] at hp; exact inv.acted m hp
      · have := inv.within; simp only; omega
  | report =>
      simp only [step, Option.some.injEq] at h; subst h
      exact ⟨fun m hm => inv.fresh m (by simp at hm; omega), inv.owned, inv.taken, inv.acted,
        by have := inv.within; simp only; omega⟩
  | start =>
      simp only [step, Option.some.injEq] at h; subst h
      exact ⟨inv.fresh, fun hs => by simp at hs, inv.taken, inv.acted, inv.within⟩
  | stop =>
      simp only [step, Option.some.injEq] at h; subst h
      refine ⟨?_, ?_, ?_, ?_, inv.within⟩
      · intro m hm
        have := inv.fresh m hm
        simp [move_apply, this]
      · intro _ m
        simp only [move_apply]
        split
        · simp
        · rename_i hw; simpa using hw
      · intro hr m
        simp only [move_apply]
        split
        · simp
        · exact inv.taken hr m
      · intro m hp
        simp only [move_apply] at hp
        split at hp
        · simp at hp
        · exact inv.acted m hp
  | held hold =>
      simp only [step, Option.some.injEq] at h; subst h
      exact ⟨inv.fresh, inv.owned, inv.taken, inv.acted, inv.within⟩
  | plan =>
      simp only [step] at h
      split at h
      · rename_i guard
        simp only [Option.some.injEq] at h; subst h
        refine ⟨inv.fresh, inv.owned, inv.taken, ?_, Nat.le_refl _⟩
        intro m hp
        exact absurd hp (inv.taken guard.2 m)
      · simp at h
  | wait =>
      simp only [step] at h
      split at h
      · split at h
        · simp only [Option.some.injEq] at h; subst h
          refine ⟨?_, ?_, ?_, ?_, inv.within⟩
          · intro m hm
            have := inv.fresh m hm
            simp [move_apply, this]
          · intro hs m
            simp only [move_apply]
            split
            · simp
            · exact inv.owned hs m
          · intro hr m
            simp only [move_apply]
            split
            · simp
            · exact inv.taken hr m
          · intro m hp
            simp only [move_apply] at hp
            split at hp
            · simp at hp
            · exact inv.acted m hp
        · simp at h
      · simp at h
  | begin =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h
        refine ⟨?_, ?_, ?_, ?_, inv.within⟩
        · intro m hm
          have := inv.fresh m hm
          simp [move_apply, this]
        · intro hs m
          simp only [move_apply]
          split
          · simp
          · exact inv.owned hs m
        · intro hr; simp at hr
        · intro m hp
          simp only [move_apply] at hp
          split at hp
          · rename_i picked; simpa using picked.2
          · exact inv.acted m hp
      · simp at h
  | connected =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h
        refine ⟨?_, ?_, ?_, ?_, inv.within⟩
        · intro m hm
          have := inv.fresh m hm
          simp [move_apply, this]
        · intro hs m
          simp only at hs
          simp only [move_apply, hs]
          split
          · simp
          · exact inv.owned hs m
        · intro _ m
          simp only [move_apply]
          split
          · cases s.syncing <;> simp
          · rename_i hn; simpa using hn
        · intro m hp
          simp only [move_apply] at hp
          split at hp
          · cases hs : s.syncing <;> simp [hs] at hp
          · exact inv.acted m hp
      · simp at h
  | finish e =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h
        refine ⟨?_, ?_, ?_, ?_, inv.within⟩
        · intro m hm
          have := inv.fresh m hm
          simp [move_apply, this]
        · intro hs m
          simp only [move_apply]
          split
          · simp
          · exact inv.owned hs m
        · intro _ m
          simp only [move_apply]
          split
          · simp
          · rename_i hn; simpa using hn
        · intro m hp
          simp only [move_apply] at hp
          split at hp
          · simp at hp
          · exact inv.acted m hp
      · simp at h

theorem inv_run {s t : Loop} {es : List Event} (inv : Inv s) (h : run s es = some t) :
    Inv t := by
  induction es generalizing s with
  | nil => simp [run] at h; subst h; exact inv
  | cons e es ih =>
      simp only [run, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      exact ih (inv_step inv hs) hr

/-- Every state a run from an open handle reaches keeps the invariant. -/
theorem reachable_inv {es : List Event} {t : Loop} (h : run initial es = some t) : Inv t :=
  inv_run initial_inv h

/-! ## Each request is answered at most once -/

/-- An answer never changes once given. -/
theorem answer_final {s t : Loop} {e : Event} {m : Nat} {o : Outcome} (inv : Inv s)
    (h : step s e = some t) (answered : s.request m = .answered o) :
    t.request m = .answered o := by
  cases e with
  | ask =>
      simp only [step, Option.some.injEq] at h; subst h
      have ne : m ≠ s.reported + 1 := by
        intro eq
        have := inv.fresh m (by omega)
        rw [this] at answered; exact Phase.noConfusion answered
      simp [ne, answered]
  | report | start | held _ =>
      simp only [step, Option.some.injEq] at h; subst h; exact answered
  | stop =>
      simp only [step, Option.some.injEq] at h; subst h
      simp [move_apply, answered]
  | plan =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h; exact answered
      · simp at h
  | wait =>
      simp only [step] at h
      split at h
      · split at h
        · simp only [Option.some.injEq] at h; subst h; simp [move_apply, answered]
        · simp at h
      · simp at h
  | begin =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h; simp [move_apply, answered]
      · simp at h
  | connected =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h; simp [move_apply, answered]
      · simp at h
  | finish _ =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h; simp [move_apply, answered]
      · simp at h

/-- Across a whole run, too: a request answered once keeps that answer. -/
theorem answer_final_run {s t : Loop} {es : List Event} {m : Nat} {o : Outcome} (inv : Inv s)
    (h : run s es = some t) (answered : s.request m = .answered o) :
    t.request m = .answered o := by
  induction es generalizing s with
  | nil => simp [run] at h; subst h; exact answered
  | cons e es ih =>
      simp only [run, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      exact ih (inv_step inv hs) hr (answer_final inv hs answered)

/-! ## Who answers -/

/-- A request waits only while a started connection will plan. -/
theorem waits_only_while_syncing {es : List Event} {t : Loop} (h : run initial es = some t)
    (stopped : t.syncing = false) (m : Nat) : t.request m ≠ .waiting :=
  (reachable_inv h).owned stopped m

/-- A pass answers only requests the owner acted on before the pass began. -/
theorem pass_answers_acted_on {es : List Event} {t : Loop} (h : run initial es = some t)
    (m : Nat) (taken : t.request m = .inPass) : m ≤ t.handled :=
  (reachable_inv h).acted m taken

/-- A request made while a pass runs is not answered by that pass: it is still
waiting when the pass ends. -/
theorem running_pass_skips {s t : Loop} (e : PassEnd) (running : s.running = true)
    (syncing : s.syncing = true) (h : run s [.ask, .finish e] = some t) :
    t.request (s.reported + 1) = .waiting := by
  simp [run, step, running, syncing] at h
  subst h
  simp [move_apply]

/-- The next pass the owner plans answers every waiting request, with its own
result. -/
theorem next_pass_answers {s : Loop} (e : PassEnd) (inv : Inv s) (idle : s.running = false)
    (syncing : s.syncing = true) (free : s.hold = none) {m : Nat}
    (waiting : s.request m = .waiting) :
    ∃ t, run s [.plan, .begin, .finish e] = some t ∧ t.request m = .answered e.outcome := by
  have asked : m ≤ s.reported := by
    apply Classical.byContradiction
    intro over
    have := inv.fresh m (by omega)
    rw [this] at waiting; exact Phase.noConfusion waiting
  cases ran : run s [.plan, .begin, .finish e] with
  | none => simp [run, step, idle, syncing, free] at ran
  | some t =>
      refine ⟨t, rfl, ?_⟩
      simp [run, step, idle, syncing, free] at ran
      subst ran
      simp [move_apply, waiting, asked]

/-- Ana and Ben both pull to refresh before the owner plans: one pass answers
both, with the same result. -/
theorem concurrent_requests_share {s t : Loop} (e : PassEnd) (idle : s.running = false)
    (syncing : s.syncing = true) (free : s.hold = none)
    (h : run s [.ask, .ask, .plan, .begin, .finish e] = some t) :
    t.request (s.reported + 1) = .answered e.outcome ∧
      t.request (s.reported + 2) = .answered e.outcome := by
  simp [run, step, idle, syncing, free] at h
  subst h
  constructor
  · simp [move_apply]
  · simp [move_apply]

/-- A held pass answers the requests the owner acted on at once: cooling down
for a provider cooldown, failed for a failure only the person can clear. -/
theorem held_answers {s : Loop} {hold : Held} (inv : Inv s) (idle : s.running = false)
    (syncing : s.syncing = true) (held : s.hold = some hold) {m : Nat}
    (waiting : s.request m = .waiting) :
    ∃ t, run s [.plan, .wait] = some t ∧ t.request m = .answered hold.outcome := by
  have asked : m ≤ s.reported := by
    apply Classical.byContradiction
    intro over
    have := inv.fresh m (by omega)
    rw [this] at waiting; exact Phase.noConfusion waiting
  cases ran : run s [.plan, .wait] with
  | none => simp [run, step, idle, syncing, held] at ran
  | some t =>
      refine ⟨t, rfl, ?_⟩
      simp [run, step, idle, syncing, held] at ran
      subst ran
      simp [move_apply, waiting, asked]

/-- While a pass is held, none begins. -/
theorem held_runs_no_pass (s : Loop) {hold : Held} (held : s.hold = some hold) :
    step s .begin = none := by
  simp [step, held]

/-- Asking while sync is stopped, or on a closed handle, answers "not
syncing" at once. -/
theorem stopped_answers_at_once (s : Loop) (stopped : s.syncing = false) :
    (step s .ask).map (·.request (s.reported + 1)) = some (.answered .notSyncing) := by
  simp [step, stopped]

/-- Stopping while a pass runs leaves that pass's requests to the pass: they
get its result, not "not syncing". -/
theorem stop_lets_pass_answer {s t : Loop} (e : PassEnd) (running : s.running = true)
    {m : Nat} (taken : s.request m = .inPass) (h : run s [.stop, .finish e] = some t) :
    t.request m = .answered e.outcome := by
  simp [run, step, running] at h
  subst h
  simp [move_apply, taken]

/-! ## No request is left waiting -/

/-- The connection's owner's own steps; no request, report, start, stop or
change of hold. -/
def owner : Event → Bool
  | .plan | .wait | .begin | .finish _ => true
  | _ => false

/-- Every request is answered or not yet asked. -/
def Settled (s : Loop) : Prop := ∀ m, s.request m ≠ .waiting ∧ s.request m ≠ .inPass

theorem run_append {s u : Loop} {xs ys : List Event} (h : run s xs = some u) :
    run s (xs ++ ys) = run u ys := by
  induction xs generalizing s with
  | nil => simp [run] at h; subst h; rfl
  | cons x xs ih =>
      simp only [run, Option.bind_eq_some_iff] at h
      obtain ⟨m, hm, hr⟩ := h
      simp only [List.cons_append, run, hm, Option.bind_some]
      exact ih hr

/-- From a state with no pass running, the owner's own steps answer every
request. -/
theorem idle_settles {s : Loop} (inv : Inv s) (idle : s.running = false) :
    ∃ es t, (∀ e ∈ es, owner e = true) ∧ run s es = some t ∧ Settled t := by
  have asked : ∀ m, s.request m = .waiting → m ≤ s.reported := by
    intro m waiting
    apply Classical.byContradiction
    intro over
    have := inv.fresh m (by omega)
    rw [this] at waiting; exact Phase.noConfusion waiting
  cases syncing : s.syncing with
  | false =>
      refine ⟨[], s, by simp, rfl, fun m => ⟨inv.owned syncing m, inv.taken idle m⟩⟩
  | true =>
      cases held : s.hold with
      | none =>
          cases ran : run s [.plan, .begin, .finish .synced] with
          | none => simp [run, step, idle, syncing, held] at ran
          | some t =>
          refine ⟨[.plan, .begin, .finish .synced], t, by simp [owner], ran, ?_⟩
          simp [run, step, idle, syncing, held] at ran
          subst ran
          · intro m
            simp only [move_apply]
            by_cases waiting : s.request m = .waiting
            · simp [waiting, asked m waiting]
            · have := inv.taken idle m
              simp [waiting, this]
      | some hold =>
          cases ran : run s [.plan, .wait] with
          | none => simp [run, step, idle, syncing, held] at ran
          | some t =>
          refine ⟨[.plan, .wait], t, by simp [owner], ran, ?_⟩
          simp [run, step, idle, syncing, held] at ran
          subst ran
          · intro m
            simp only [move_apply]
            by_cases waiting : s.request m = .waiting
            · simp [waiting, asked m waiting]
            · have := inv.taken idle m
              simp [waiting, this]

/-- No request is left waiting: from every reachable state, the owner's own
steps, finishing any pass that runs and then planning, answer every request,
with no outside event. -/
theorem never_stranded {es : List Event} {s : Loop} (h : run initial es = some s) :
    ∃ es' t, (∀ e ∈ es', owner e = true) ∧ run s es' = some t ∧ Settled t := by
  have inv := reachable_inv h
  cases running : s.running with
  | false => exact idle_settles inv running
  | true =>
      let u : Loop := { s with running := false,
                               request := s.move .inPass (.answered PassEnd.synced.outcome)
                                 (fun _ => true) }
      have finished : step s (.finish .synced) = some u := by simp [step, running, u]
      obtain ⟨rest, t, owned, ran, settled⟩ := idle_settles (inv_step inv finished) rfl
      refine ⟨.finish .synced :: rest, t, ?_, ?_, settled⟩
      · intro e m
        simp only [List.mem_cons] at m
        rcases m with eq | m
        · subst eq; rfl
        · exact owned e m
      · simp only [run, finished, Option.bind_some]; exact ran

/-! ## What a retry would do

Rust publishes what a retry made now would do (`RetryEffect`), from the same
state its answers come from: whether a started connection will plan, and
what holds the next pass. An app offers a retry only while the effect is a
pass. These theorems prove the effect is how a request made then is
answered. -/

/-- What a retry made now would do: run a pass, or be answered at once. -/
inductive Effect
  | pass
  | coolingDown
  | failed
  | notSyncing
  deriving DecidableEq, Repr

/-- The published effect. -/
def Loop.effect (s : Loop) : Effect :=
  if s.syncing then
    match s.hold with
    | some .cooldown => .coolingDown
    | some .failure => .failed
    | none => .pass
  else .notSyncing

/-- The answer the effect says a request gets at once; none when a pass
answers it. -/
def Effect.atOnce : Effect → Option Outcome
  | .pass => none
  | .coolingDown => some .coolingDown
  | .failed => some .failed
  | .notSyncing => some .notSyncing

/-- Events that leave the effect: all but starting, stopping and a change of
hold, each of which Rust publishes. -/
def Event.keepsEffect : Event → Bool
  | .start | .stop | .held _ => false
  | _ => true

/-- A request, a report, the owner's own steps and a connected attempt leave
the effect as it was. -/
theorem effect_steady {s t : Loop} {e : Event} (keeps : e.keepsEffect = true)
    (h : step s e = some t) : t.effect = s.effect := by
  cases e with
  | ask | report =>
      simp only [step, Option.some.injEq] at h; subst h; rfl
  | start | stop | held _ => simp [Event.keepsEffect] at keeps
  | plan | begin | connected | finish _ =>
      simp only [step] at h
      split at h
      · simp only [Option.some.injEq] at h; subst h; rfl
      · simp at h
  | wait =>
      simp only [step] at h
      split at h
      · split at h
        · simp only [Option.some.injEq] at h; subst h; rfl
        · simp at h
      · simp at h

/-- When the effect says a request is answered at once, one made then gets
exactly that answer, from the owner's own steps, and no pass begins for it. -/
theorem published_answers_at_once {s : Loop} {o : Outcome} (idle : s.running = false)
    (once : s.effect.atOnce = some o) :
    ∃ es t, (∀ e ∈ es, owner e = true ∧ e ≠ .begin) ∧ run s (.ask :: es) = some t ∧
      t.request (s.reported + 1) = .answered o := by
  cases syncing : s.syncing with
  | false =>
      simp [Loop.effect, syncing, Effect.atOnce] at once
      subst once
      cases ran : run s [.ask] with
      | none => simp [run, step] at ran
      | some t =>
          refine ⟨[], t, by simp, ran, ?_⟩
          simp [run, step, syncing] at ran
          subst ran
          simp
  | true =>
      cases held : s.hold with
      | none => simp [Loop.effect, syncing, held, Effect.atOnce] at once
      | some hold =>
          have outcome : hold.outcome = o := by
            cases hold <;> simp [Loop.effect, syncing, held, Effect.atOnce, Held.outcome] at once ⊢
              <;> exact once
          subst outcome
          cases ran : run s [.ask, .plan, .wait] with
          | none => simp [run, step, syncing, held, idle] at ran
          | some t =>
              refine ⟨[.plan, .wait], t, by simp [owner], ran, ?_⟩
              simp [run, step, syncing, held, idle] at ran
              subst ran
              simp [move_apply]

/-- When the effect is a pass, a request made then is not answered at once:
nothing holds the pass the owner plans, and the pass that begins next takes
the request and answers it with its own result. -/
theorem published_pass_runs {s : Loop} (e : PassEnd) (idle : s.running = false)
    (pass : s.effect = .pass) :
    run s [.ask, .plan, .wait] = none ∧
      ∃ t, run s [.ask, .plan, .begin, .finish e] = some t ∧
        t.request (s.reported + 1) = .answered e.outcome := by
  have syncing : s.syncing = true := by
    cases h : s.syncing <;> simp_all [Loop.effect]
  have free : s.hold = none := by
    cases h : s.hold with
    | none => rfl
    | some hold => cases hold <;> simp_all [Loop.effect]
  refine ⟨by simp [run, step, syncing, free, idle], ?_⟩
  cases ran : run s [.ask, .plan, .begin, .finish e] with
  | none => simp [run, step, syncing, free, idle] at ran
  | some t =>
      refine ⟨t, rfl, ?_⟩
      simp [run, step, syncing, free, idle] at ran
      subst ran
      simp [move_apply]

namespace Examples

/-- Ben's phone syncs. He pulls to refresh while a pass is already running;
that pass ends synced, but his answer comes from the next pass, which fails
because his network dropped. -/
theorem ben_waits_for_the_next_pass :
    (run ⟨0, 0, true, none, false, fun _ => .unasked⟩
      [.plan, .begin, .ask, .finish .synced, .plan, .begin, .finish .failed]).map
        (·.request 1) = some (.answered .failed) := by
  decide

/-- Ana pulls to refresh during a provider cooldown: she is told so at once,
and no pass runs for her. -/
theorem ana_hears_of_the_cooldown :
    (run ⟨0, 0, true, some .cooldown, false, fun _ => .unasked⟩ [.ask, .plan, .wait]).map
      (·.request 1) = some (.answered .coolingDown) := by
  decide

/-- Carol pulls to refresh, then stops sync before the owner plans: she is
told sync is not running. -/
theorem carol_stops_first :
    (run ⟨0, 0, true, none, false, fun _ => .unasked⟩ [.ask, .stop]).map
      (·.request 1) = some (.answered .notSyncing) := by
  decide

end Examples

end CovenIO.RetryAnswer
