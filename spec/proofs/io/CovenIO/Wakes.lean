/-! The sync loop's wakes (§6.7, "Waiting and retrying", E5
`sync_now`), as `sync_loop.rs` and `Database::sync_changes` run them.

The loop observes commits to the outgoing write queue, counting insertions
only: an app write inserts a row, while the loop's own bookkeeping (an attempt
fixing its keys or session, a confirmed upload retiring its row) updates or
removes one. A pass first takes the retained wake and the observation, then
reads the queue: it serves every write queued by then. Between passes an
observed commit becomes the retained wake. Commands (`sync_now`) and the idle
schedule set the wake directly. The wake is one flag, so any number of writes
and commands racing a pass retain one more pass. -/

namespace CovenIO.Wakes

/-- A commit to the outgoing write queue. -/
inductive Commit
  /-- An app write queues its row. -/
  | insert (write : Nat)
  /-- Own bookkeeping: an attempt fixes its keys or upload session. -/
  | update
  /-- Own bookkeeping: a confirmed upload retires its row. -/
  | remove (write : Nat)
  deriving DecidableEq, Repr

/-- The loop's observation counts inserted rows only. -/
def observes : Commit → Bool
  | .insert _ => true
  | _ => false

def enqueue (queue : List Nat) : Commit → List Nat
  | .insert w => queue ++ [w]
  | .update => queue
  | .remove w => queue.erase w

inductive Phase
  | idle
  /-- The pass took the wake and the observation, and has not read the queue. -/
  | taken
  /-- The pass read the queue. -/
  | running
  deriving DecidableEq, Repr

structure Loop where
  queue : List Nat
  /-- Every write some pass read from the queue. -/
  seen : List Nat
  /-- An observed commit since the observation was last taken. -/
  observed : Bool
  /-- The retained wake: the next pass runs. -/
  pending : Bool
  phase : Phase
  deriving DecidableEq, Repr

inductive Event
  | commit (c : Commit)
  /-- `sync_now`. -/
  | command
  /-- Between passes, an observed commit becomes the wake. -/
  | notice
  /-- The idle schedule. -/
  | idle
  | take
  | capture
  | finish
  deriving DecidableEq, Repr

def step (s : Loop) : Event → Option Loop
  | .commit c => some { s with queue := enqueue s.queue c, observed := s.observed || observes c }
  | .command => some { s with pending := true }
  | .notice => if s.phase = .idle ∧ s.observed = true then
      some { s with pending := true, observed := false } else none
  | .idle => if s.phase = .idle then some { s with pending := true } else none
  | .take => if s.phase = .idle ∧ s.pending = true then
      some { s with pending := false, observed := false, phase := .taken } else none
  | .capture => if s.phase = .taken then
      some { s with seen := s.seen ++ s.queue, phase := .running } else none
  | .finish => if s.phase = .running then some { s with phase := .idle } else none

def run (s : Loop) : List Event → Option Loop
  | [] => some s
  | e :: es => (step s e).bind fun next => run next es

/-- The observation reports its initial state, so writes queued before the
loop starts are served by its first pass. -/
def initial (queue : List Nat) : Loop := ⟨queue, [], true, false, .idle⟩

/-- Every queued write no pass has read keeps a wake, unless a pass took its
wake and is about to read the queue. -/
def Covered (s : Loop) : Prop :=
  ∀ w ∈ s.queue, w ∉ s.seen → s.phase = .taken ∨ s.observed = true ∨ s.pending = true

theorem initial_covered (queue : List Nat) : Covered (initial queue) := by
  intro w _ _; simp [initial]

theorem covered_step {s t : Loop} {e : Event} (covered : Covered s) (h : step s e = some t) :
    Covered t := by
  cases e with
  | commit c =>
      simp [step] at h; subst h
      intro w hw unseen
      cases c with
      | insert v =>
          simp [observes]
      | update =>
          simp [enqueue, observes] at hw ⊢; exact covered w hw unseen
      | remove v =>
          simp [enqueue, observes] at hw ⊢
          exact covered w (List.mem_of_mem_erase hw) unseen
  | command => simp [step] at h; subst h; intro w _ _; simp
  | notice =>
      simp only [step] at h; split at h
      · simp at h; subst h; intro w _ _; simp
      · simp at h
  | idle =>
      simp only [step] at h; split at h
      · simp at h; subst h; intro w _ _; simp
      · simp at h
  | take =>
      simp only [step] at h; split at h
      · simp at h; subst h; intro w _ _; simp
      · simp at h
  | capture =>
      simp only [step] at h; split at h
      · simp at h; subst h; intro w hw unseen; simp at unseen; exact absurd hw unseen.2
      · simp at h
  | finish =>
      simp only [step] at h; split at h
      · rename_i running
        simp at h; subst h; intro w hw unseen
        rcases covered w hw unseen with taken | wake
        · rw [running] at taken; cases taken
        · simpa using wake
      · simp at h

theorem covered_run {s t : Loop} {es : List Event} (covered : Covered s)
    (h : run s es = some t) : Covered t := by
  induction es generalizing s with
  | nil => simp [run] at h; subst h; exact covered
  | cons e es ih =>
      simp only [run, Option.bind_eq_some_iff] at h
      obtain ⟨next, hs, hr⟩ := h
      exact ih (covered_step covered hs) hr

/-- A write is never lost: in every reachable state, a queued write that no
pass has read keeps a wake, whatever raced it. -/
theorem write_never_lost {queue : List Nat} {es : List Event} {t : Loop}
    (h : run (initial queue) es = some t) : Covered t :=
  covered_run (initial_covered queue) h

/-- The pass's own bookkeeping retains nothing: an update or removal leaves
the observation and the wake as they were. -/
theorem bookkeeping_retains_none (s : Loop) (c : Commit) (own : observes c = false) :
    ∃ t, step s (.commit c) = some t ∧ t.observed = s.observed ∧ t.pending = s.pending ∧
      t.phase = s.phase := by
  refine ⟨_, rfl, ?_, rfl, rfl⟩; simp [own]

/-- The idle loop with no outside event: notice an observed commit, then run
one pass that serves what it reads, retiring each row it confirms. -/
def settle (s : Loop) : Loop :=
  if s.observed then { s with pending := true, observed := false } else s

def serve (s : Loop) : Loop :=
  let taken := { s with pending := false, observed := false, phase := .taken }
  let read := { taken with seen := taken.seen ++ taken.queue, phase := .running }
  let confirmed := read.queue.foldl (fun l w => { l with queue := l.queue.erase w }) read
  { confirmed with phase := .idle }

/-- `serve` is a run of the loop's own steps. -/
theorem serve_runs (s : Loop) (idle : s.phase = .idle) (pending : s.pending = true) :
    run s ([.take, .capture] ++ s.queue.map (fun w => .commit (.remove w)) ++ [.finish]) =
      some (serve s) := by
  have folded : ∀ (ws : List Nat) (l : Loop), l.phase = .running →
      run l (ws.map (fun w => .commit (.remove w)) ++ [.finish]) =
        some { ws.foldl (fun l w => { l with queue := l.queue.erase w }) l with phase := .idle } := by
    intro ws
    induction ws with
    | nil => intro l running; simp [run, step, running]
    | cons w ws ih =>
        intro l running
        simp only [List.map_cons, List.cons_append, run, step, Option.bind_some, List.foldl_cons]
        have := ih { l with queue := enqueue l.queue (.remove w), observed := l.observed || observes (.remove w) } running
        simpa [enqueue, observes] using this
  simp only [List.cons_append, run, step, idle, pending, and_self, ite_true, Option.bind_some]
  exact folded s.queue _ rfl

theorem serve_quiet (s : Loop) :
    (serve s).observed = false ∧ (serve s).pending = false ∧ (serve s).phase = .idle := by
  have keep : ∀ (ws : List Nat) (l : Loop),
      (ws.foldl (fun l w => { l with queue := l.queue.erase w }) l).observed = l.observed ∧
      (ws.foldl (fun l w => { l with queue := l.queue.erase w }) l).pending = l.pending := by
    intro ws; induction ws with
    | nil => intro l; simp
    | cons w ws ih => intro l; simp only [List.foldl_cons]; exact ih _
  simp only [serve]
  exact ⟨(keep _ _).1, (keep _ _).2, trivial⟩

/-- Passes the loop runs before it next waits, with no outside event, no idle
timer and every pass served. -/
def quietPasses : Nat → Loop → Nat
  | 0, _ => 0
  | n + 1, s => if (settle s).pending then 1 + quietPasses n (serve (settle s)) else 0

/-- Exactly one wake: from an idle loop, the retained wakes, however many
commits and commands set them, run one pass and then none. -/
theorem one_pass (n : Nat) (s : Loop) :
    quietPasses (n + 2) s = if s.observed || s.pending then 1 else 0 := by
  have quiet := serve_quiet (settle s)
  cases ho : s.observed <;> cases hp : s.pending <;>
    simp [quietPasses, settle, ho, hp] at quiet ⊢ <;>
    cases n <;> simp [quiet.1, quiet.2.1]

/-- Whether an outside event retains a wake. -/
def wakes : Event → Bool
  | .commit c => observes c
  | .command => true
  | _ => false

def outside : Event → Bool
  | .commit _ | .command => true
  | _ => false

theorem outside_run (s : Loop) (racing : List Event) (only : ∀ e ∈ racing, outside e = true) :
    ∃ t, run s racing = some t ∧ t.phase = s.phase ∧
      (t.observed || t.pending) = (s.observed || s.pending || racing.any wakes) := by
  induction racing generalizing s with
  | nil => exact ⟨s, rfl, rfl, by simp⟩
  | cons e es ih =>
      have rest := fun e' (m : e' ∈ es) => only e' (List.mem_cons_of_mem _ m)
      cases e with
      | commit c =>
          obtain ⟨t, ht, phase, wake⟩ := ih { s with queue := enqueue s.queue c, observed := s.observed || observes c } rest
          refine ⟨t, by simpa [run, step] using ht, phase, ?_⟩
          rw [wake]; simp only [wakes, List.any_cons]
          generalize s.observed = a; generalize s.pending = b
          generalize observes c = o; generalize es.any wakes = r
          cases a <;> cases b <;> cases o <;> cases r <;> rfl
      | command =>
          obtain ⟨t, ht, phase, wake⟩ := ih { s with pending := true } rest
          refine ⟨t, by simpa [run, step] using ht, phase, ?_⟩
          rw [wake]; simp [wakes]
      | _ => simp [outside] at only

/-- A write racing a pass: whatever commits and commands arrive while a pass
runs, before or after it reads the queue, the loop runs exactly one more pass
when one of them was an app write or a command, and none when all were the
pass's own bookkeeping. -/
theorem racing_passes (n : Nat) (s : Loop) (idle : s.phase = .idle) (pending : s.pending = true)
    (before after : List Event) (only : ∀ e ∈ before ++ after, outside e = true) :
    ∃ t, run s ([.take] ++ before ++ [.capture] ++ after ++ [.finish]) = some t ∧
      quietPasses (n + 2) t = if (before ++ after).any wakes then 1 else 0 := by
  let taken : Loop := { s with pending := false, observed := false, phase := .taken }
  obtain ⟨u, hu, uphase, uwake⟩ := outside_run taken before
    (fun e m => only e (List.mem_append_left _ m))
  let read : Loop := { u with seen := u.seen ++ u.queue, phase := .running }
  obtain ⟨v, hv, vphase, vwake⟩ := outside_run read after
    (fun e m => only e (List.mem_append_right _ m))
  refine ⟨{ v with phase := .idle }, ?_, ?_⟩
  · have hu' : run taken before = some u := hu
    have stepTake : step s .take = some taken := by simp [step, idle, pending, taken]
    have stepCapture : step u .capture = some read := by
      simp [step, uphase, taken, read]
    have stepFinish : step v .finish = some { v with phase := .idle } := by
      simp [step, vphase, read]
    have chain : ∀ (a : Loop) (xs ys : List Event) (b : Loop), run a xs = some b →
        run a (xs ++ ys) = run b ys := by
      intro a xs; induction xs generalizing a with
      | nil => intro ys b h; simp [run] at h; subst h; rfl
      | cons x xs ih =>
          intro ys b h
          simp only [run, Option.bind_eq_some_iff] at h
          obtain ⟨m, hm, hr⟩ := h
          simp only [List.cons_append, run, hm, Option.bind_some]
          exact ih m ys b hr
    simp only [List.append_assoc]
    rw [show [Event.take] ++ (before ++ ([Event.capture] ++ (after ++ [Event.finish]))) =
      Event.take :: (before ++ ([Event.capture] ++ (after ++ [Event.finish]))) from rfl]
    simp only [run, stepTake, Option.bind_some]
    rw [chain taken before _ u hu']
    simp only [List.singleton_append, run, stepCapture, Option.bind_some]
    rw [chain read after _ v hv]
    simp [run, stepFinish]
  · rw [one_pass]
    have : (v.observed || v.pending) = (before ++ after).any wakes := by
      rw [vwake]
      have : (read.observed || read.pending) = before.any wakes := by
        simp only [read]; rw [uwake]; simp [taken]
      rw [this, List.any_append]
    simp only at this ⊢
    rw [this]

end CovenIO.Wakes
