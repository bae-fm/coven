import CovenIO.Execution
import CovenIO.Trace
import CovenIO.Publication
import CovenStorelogData.KeyPhases

/-! One sync pass in its three phases (§6, SP phases 1–3), with the rules
that let a pass's work reuse its own first phase:

- phase 1 is read-only; a complete observation records its catch-up in the
  shared send gate, an incomplete one leaves the last completed catch-up to
  expire, and a failed read ends the pass;
- work decided from the current view (new key sharing, new entries,
  admission and first attempts) runs only after this pass's own phase 1
  completed;
- after an incomplete phase 1 only fixed attempts are resent (copies and the
  entry reserved earlier, attempted writes), each through the gate;
- an entry published under §10's admission exception is followed by another
  read-only observation before any ordinary send;
- an invite reads its copy number only after the reserved copies are published.

Requests carry no bytes: what is sent is named by `Work`. Every ordinary
request goes through Trace's `gateStep`, the same gate every sender uses. -/
namespace CovenIO.PassPhases

/-- What one request of the pass publishes. -/
inductive Work where
  /-- A key copy reserved before this pass: fixed bytes. -/
  | copy (number : Nat)
  /-- A copy this pass reserves from its own view: historical sharing or an
  operation's prerequisite. -/
  | share (number : Nat)
  /-- The store-log entry reserved before this pass: fixed bytes. -/
  | entry (number : Nat)
  /-- An entry an operation reserves from this pass's view. -/
  | author (number : Nat)
  /-- This device's registration, under §10's admission exception. -/
  | admission (number : Nat)
  /-- A write's first attempt: its keys are chosen from the current view. -/
  | first (number : Nat)
  /-- An attempted write, resent with its fixed bytes. -/
  | retry (number : Nat)
  /-- This device's positions. -/
  | positions
  deriving DecidableEq, Repr

/-- Work decided from the current membership and copy view. Resending fixed
bytes and posting positions decide nothing new. -/
def Work.current : Work → Bool
  | .copy _ | .entry _ | .retry _ | .positions => false
  | _ => true

def Work.admits : Work → Bool
  | .admission _ => true
  | _ => false

def Work.writes : Work → Bool
  | .first _ | .retry _ => true
  | _ => false

def Work.request (writer : Nat) : Work → Request
  | .copy n | .share n => .create (.log .copy writer n) []
  | .entry n | .author n | .admission n => .create (.log .entry writer n) []
  | .first n | .retry n => .create (.log .write writer n) []
  | .positions => .replace (.positions writer) []

/-- One read-only membership and key-copy observation (SP steps 1–4): the
entry and copy logs as `readLog` executed them, whether every copy's clear
prefix decoded (§11), the identity check and the completion time. -/
structure Observation where
  entries : Execution
  copies : Execution
  readable : Bool
  identity : CovenStorage.Gate
  retired : Bool
  finished : Nat
  deriving Repr

/-- A read failed. Copies are read only after the entries reach their miss.
The pass reports the failure and sends nothing. -/
def Observation.failed (o : Observation) : Bool :=
  match o.entries.stop, o.copies.stop with
  | .failed _, _ => true
  | .miss, .failed _ => true
  | _, _ => false

/-- Both logs reached a terminal miss and every copy prefix decoded. A
refused entry, exhausted fuel or an unreadable copy prefix is incomplete. -/
def Observation.complete (o : Observation) : Bool :=
  o.entries.stop == .miss && o.copies.stop == .miss && o.readable

/-- Begin an observation at `time`. A complete one replaces the sender's
catch-up with its own start; an incomplete one keeps the last completed
catch-up, with its original expiry. -/
def observe (sender : Sender) (time : Nat) (o : Observation) : Sender :=
  match gateStep sender (.beginPass time) with
  | none => sender
  | some begun =>
      if o.complete then (gateStep begun (.checked o.finished o.identity o.retired)).getD begun
      else begun

theorem observe_completed (sender : Sender) (time : Nat) (o : Observation) :
    (observe sender time o).completed =
      if o.complete ∧ time ≤ o.finished then some ⟨time, o.finished, o.identity, o.retired⟩
      else sender.completed := by
  by_cases complete : o.complete = true
  · by_cases ordered : time ≤ o.finished
    · simp [observe, gateStep, complete, ordered]
    · simp [observe, gateStep, complete, ordered]
  · simp [observe, gateStep, complete]

/-- Phase 1 as the data package's `KeyPhases.begin` receives it: the
received entries from a complete entry observation, and the copy listing
from a complete observation whose every prefix decoded. -/
def Observation.begin (o : Observation) (custody : List CovenStorelogData.KeySelection.Key)
    (received : List Nat) (copies : CovenStorelogData.KeySelection.Copies)
    (why : CovenStorelogData.Pending.Reason) :
    Except CovenStorelogData.Pending.Reason CovenStorelogData.KeyPhases.CaughtUp :=
  CovenStorelogData.KeyPhases.begin custody
    (if o.entries.stop == .miss then .ok received else .error why)
    (if o.copies.stop == .miss && o.readable then .ok copies else .error why)

/-- `KeyPhases.firstSend` needs a `CaughtUp`; one exists exactly when this
observation is complete. -/
theorem begin_iff_complete (o : Observation) (custody : List CovenStorelogData.KeySelection.Key)
    (received : List Nat) (copies : CovenStorelogData.KeySelection.Copies)
    (why : CovenStorelogData.Pending.Reason) :
    (∃ caught, o.begin custody received copies why = .ok caught) ↔ o.complete = true := by
  unfold Observation.begin Observation.complete
  cases entries : (o.entries.stop == .miss) <;> cases listed : (o.copies.stop == .miss) <;>
    cases readable : o.readable <;>
    simp [CovenStorelogData.KeyPhases.begin, bind, Except.bind, pure, Except.pure]

/-- The admission exception (§10): a membership-complete, fresh catch-up of
a member that is not retired, for a device without ordinary authority. -/
def admissible (c : Catchup) (time : Nat) : Bool :=
  c.identity != .send && !c.retired &&
    decide (c.started ≤ c.finished ∧ c.finished ≤ time ∧ time < c.started + freshFor)

structure Sent where
  work : Work
  time : Nat
  evidence : Catchup
  deriving DecidableEq, Repr

structure State where
  sender : Sender
  next : Nat
  sent : List Sent
  stopped : Bool
  deriving DecidableEq, Repr

inductive Step where
  | send (work : Work)
  /-- Publish this registration, then observe again (SP step 5). -/
  | register (number : Nat) (rescan : Observation)
  deriving Repr

def Step.work : Step → Work
  | .send work => work
  | .register n _ => .admission n

/-- The `k`-th request of the pass happens at `clock k`. A refused request
stops the pass: its work and everything after it waits for the next pass.
Ordinary authority never publishes an admission; only `register` does. -/
def step (writer : Nat) (clock : Nat → Nat) (s : State) : Step → State
  | .send work =>
      if s.stopped then s else
      if work.admits then { s with stopped := true } else
      match gateStep s.sender (.request .pass (clock s.next) (work.request writer)),
          s.sender.completed with
      | some sender, some c => ⟨sender, s.next + 1, s.sent ++ [⟨work, clock s.next, c⟩], false⟩
      | _, _ => { s with stopped := true }
  | .register n rescan =>
      if s.stopped then s else
      match s.sender.completed with
      | some c =>
          if admissible c (clock s.next) then
            ⟨observe s.sender (clock (s.next + 1)) rescan, s.next + 2,
              s.sent ++ [⟨.admission n, clock s.next, c⟩], false⟩
          else { s with stopped := true }
      | none => { s with stopped := true }

def steps (writer : Nat) (clock : Nat → Nat) (s : State) (xs : List Step) : State :=
  xs.foldl (step writer clock) s

/-- An operation's prerequisite copies, then its entry (§18.1). -/
def operation (op : List Nat × Nat) : List Step :=
  op.1.map (fun n => .send (.share n)) ++ [.send (.author op.2)]

/-- One pass's inputs, all numbers in order. `copies` and `entry` were
reserved before the pass; `shares` are historical copies this pass reserves.
`admission` is present when the reserved entry registers this device, with
the observation that follows it. Attempted writes precede untried ones in the
queue. `resumed` operations wait on this pass's own uploads. -/
structure Plan where
  writer : Nat
  start : Nat
  phase : Observation
  copies : List Nat
  shares : List Nat
  entry : Option Nat
  admission : Option Observation
  operations : List (List Nat × Nat)
  attempted : List Nat
  untried : List Nat
  resumed : List (List Nat × Nat)
  clock : Nat → Nat

/-- This pass's own catch-up, if phase 1 completes. -/
def Plan.own (p : Plan) : Catchup :=
  ⟨p.start, p.phase.finished, p.phase.identity, p.phase.retired⟩

def Plan.caught (p : Plan) : Bool := p.phase.complete && decide (p.start ≤ p.phase.finished)

/-- Phase 1 completed with sending authority: operations may use it. -/
def Plan.member (p : Plan) : Bool := p.caught && permitted (some p.own) p.phase.finished

/-- The reserved entry goes first in SP step 5. A registration without
ordinary authority is an admission, followed by another observation; it
needs this pass's own phase 1. -/
def Plan.reserved (p : Plan) (caught member : Bool) : List Step :=
  match p.entry, p.admission with
  | none, _ => []
  | some n, none => [.send (.entry n)]
  | some n, some rescan =>
      if member then [.send (.entry n)] else if caught then [.register n rescan] else []

/-- Phase 2: copies and the entry reserved earlier, this pass's shares,
admission and due operations (SP steps 4–5). Reserved copies resend under
ordinary authority: this pass's, or after an incomplete phase 1 the last
completed one. A pass whose phase 1 completed without that authority (an
admission) holds them. -/
def Plan.keys (p : Plan) (caught member : Bool) : List Step :=
  (if !caught || member then p.copies.map (fun n => .send (.copy n)) else []) ++
  (if member then p.shares.map (fun n => .send (.share n)) else []) ++
  p.reserved caught member ++
  (if member then p.operations.flatMap operation else [])

/-- Phase 3's writes in queue order: attempted, then untried (SP step 6). -/
def Plan.data (p : Plan) (caught : Bool) : List Step :=
  p.attempted.map (fun n => .send (.retry n)) ++
    if caught then p.untried.map (fun n => .send (.first n)) else []

def Plan.before (p : Plan) (caught member : Bool) : List Step :=
  p.keys caught member ++ p.data caught

/-- Operations waiting on own uploads resume once, then positions. -/
def Plan.after (p : Plan) (member wrote : Bool) : List Step :=
  (if member && wrote then p.resumed.flatMap operation else []) ++ [.send .positions]

def Plan.planned (p : Plan) (caught member wrote : Bool) : List Step :=
  p.before caught member ++ p.after member wrote

def wrote (sent : List Sent) : Bool := sent.any (·.work.writes)

/-- The pass with its phase-1 decision supplied: which work it treats as
caught up, and whether operations may run. -/
def runWith (p : Plan) (sender : Sender) (caught member : Bool) : State :=
  let s := steps p.writer p.clock ⟨observe sender p.start p.phase, 0, [], p.phase.failed⟩
    (p.before caught member)
  steps p.writer p.clock s (p.after member (wrote s.sent))

/-- The pass deciding from its own phase 1. -/
def run (p : Plan) (sender : Sender) : State := runWith p sender p.caught p.member

/-! ### Executing steps -/

theorem steps_cons (writer : Nat) (clock : Nat → Nat) (s : State) (x : Step) (xs : List Step) :
    steps writer clock s (x :: xs) = steps writer clock (step writer clock s x) xs := rfl

theorem steps_append (writer : Nat) (clock : Nat → Nat) (s : State) (xs ys : List Step) :
    steps writer clock s (xs ++ ys) = steps writer clock (steps writer clock s xs) ys := by
  simp [steps, List.foldl_append]

theorem stopped_steps (writer : Nat) (clock : Nat → Nat) (s : State) (xs : List Step)
    (stopped : s.stopped = true) : steps writer clock s xs = s := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have same : step writer clock s x = s := by cases x <;> simp [step, stopped]
      rw [steps_cons, same]; exact ih

theorem send_allowed {s : Sender} {time : Nat} {r : Request} {next : Sender} {c : Catchup}
    (completed : s.completed = some c) (sending : sends r = true)
    (step : gateStep s (.request .pass time r) = some next) : SendAllowed c time ∧ next = s := by
  obtain ⟨c', eq, allowed⟩ := send_requires_check s next .pass time r (Or.inl sending) step
  rw [completed] at eq; cases eq
  refine ⟨allowed, ?_⟩
  simp only [gateStep] at step
  split at step
  · cases step
  · split at step
    · cases step
    · exact (Option.some.inj step).symm

theorem work_sends (writer : Nat) (work : Work) : sends (work.request writer) = true := by
  cases work <;> rfl

/-- A step stops the pass, sends one ordinary request through the gate
without changing the sender, or sends an admission and observes again. -/
theorem step_cases (writer : Nat) (clock : Nat → Nat) (s : State) (x : Step) :
    step writer clock s x = { s with stopped := true } ∨
    (∃ work c, x = .send work ∧ work.admits = false ∧ s.sender.completed = some c ∧
      SendAllowed c (clock s.next) ∧
      step writer clock s x = ⟨s.sender, s.next + 1, s.sent ++ [⟨work, clock s.next, c⟩], false⟩) ∨
    (∃ n rescan c, x = .register n rescan ∧ s.sender.completed = some c ∧
      admissible c (clock s.next) = true ∧
      step writer clock s x = ⟨observe s.sender (clock (s.next + 1)) rescan, s.next + 2,
        s.sent ++ [⟨.admission n, clock s.next, c⟩], false⟩) := by
  cases hs : s.stopped with
  | true =>
      left
      have same : { s with stopped := true } = s := by cases s; simp_all
      rw [same]; cases x <;> simp [step, hs]
  | false =>
      cases x with
      | send work =>
          cases hw : work.admits with
          | true => left; simp [step, hs, hw]
          | false =>
          cases hg : gateStep s.sender (.request .pass (clock s.next) (work.request writer)) with
          | none => left; simp [step, hs, hw, hg]
          | some next =>
              cases hc : s.sender.completed with
              | none => left; simp [step, hs, hw, hg, hc]
              | some c =>
                  obtain ⟨allowed, same⟩ := send_allowed hc (work_sends writer work) hg
                  subst same
                  right; left
                  exact ⟨work, c, rfl, hw, rfl, allowed, by simp [step, hs, hw, hg, hc]⟩
      | register n rescan =>
          cases hc : s.sender.completed with
          | none => left; simp [step, hs, hc]
          | some c =>
              cases ha : admissible c (clock s.next) with
              | false => left; simp [step, hs, hc, ha]
              | true => right; right; exact ⟨n, rescan, c, rfl, rfl, ha, by simp [step, hs, hc, ha]⟩

/-- What one step appends: nothing, or exactly its own work. -/
theorem step_sent (writer : Nat) (clock : Nat → Nat) (s : State) (x : Step) :
    ((step writer clock s x).sent = s.sent ∧ (step writer clock s x).stopped = true) ∨
    (∃ y, (step writer clock s x).sent = s.sent ++ [y] ∧ y.work = x.work) := by
  rcases step_cases writer clock s x with eq | ⟨work, c, rfl, -, _, _, eq⟩ |
      ⟨n, rescan, c, rfl, _, _, eq⟩ <;> rw [eq]
  · exact Or.inl ⟨rfl, rfl⟩
  · exact Or.inr ⟨_, rfl, rfl⟩
  · exact Or.inr ⟨_, rfl, rfl⟩

/-- Steps send a prefix of their work, in order; a pass that did not send
all of it has stopped. -/
theorem steps_prefix (writer : Nat) (clock : Nat → Nat) (s : State) (xs : List Step) :
    ∃ sent, (steps writer clock s xs).sent = s.sent ++ sent ∧
      sent.map (·.work) <+: xs.map Step.work ∧
      ((steps writer clock s xs).stopped = true ∨ sent.map (·.work) = xs.map Step.work) := by
  induction xs generalizing s with
  | nil => exact ⟨[], by simp [steps], List.prefix_refl _, Or.inr rfl⟩
  | cons x xs ih =>
      rw [steps_cons]
      rcases step_sent writer clock s x with ⟨same, halted⟩ | ⟨y, appended, work⟩
      · refine ⟨[], ?_, List.nil_prefix, Or.inl ?_⟩
        · rw [stopped_steps _ _ _ xs halted, same]; simp
        · rw [stopped_steps _ _ _ xs halted]; exact halted
      · obtain ⟨rest, eq, pre, done⟩ := ih (step writer clock s x)
        refine ⟨y :: rest, ?_, ?_, ?_⟩
        · rw [eq, appended]; simp
        · simp only [List.map_cons, work]
          exact List.cons_prefix_cons.mpr ⟨rfl, pre⟩
        · rcases done with halted | all
          · exact Or.inl halted
          · exact Or.inr (by simp [work, all])

/-- A property of every sent request that each step establishes for what it
appends, given an invariant of the state it starts from. -/
theorem steps_preserve (writer : Nat) (clock : Nat → Nat) (Inv : State → Prop)
    (Good : Sent → Prop)
    (keeps : ∀ s x, Inv s → (∀ y ∈ s.sent, Good y) →
      Inv (step writer clock s x) ∧ ∀ y ∈ (step writer clock s x).sent, Good y)
    (s : State) (xs : List Step) (inv : Inv s) (good : ∀ y ∈ s.sent, Good y) :
    Inv (steps writer clock s xs) ∧ ∀ y ∈ (steps writer clock s xs).sent, Good y := by
  induction xs generalizing s with
  | nil => exact ⟨inv, good⟩
  | cons x xs ih =>
      obtain ⟨inv', good'⟩ := keeps s x inv good
      rw [steps_cons]; exact ih (step writer clock s x) inv' good'

/-! ### Every ordinary send passes the shared gate -/

def Ordinary (y : Sent) : Prop := y.work.admits = false → SendAllowed y.evidence y.time

theorem step_ordinary (writer : Nat) (clock : Nat → Nat) (s : State) (x : Step)
    (good : ∀ y ∈ s.sent, Ordinary y) : ∀ y ∈ (step writer clock s x).sent, Ordinary y := by
  rcases step_cases writer clock s x with eq | ⟨work, c, -, -, _, allowed, eq⟩ |
      ⟨n, rescan, c, -, _, _, eq⟩ <;> rw [eq] <;> intro y member
  · exact good y member
  · rcases List.mem_append.mp member with old | new
    · exact good y old
    · simp only [List.mem_singleton] at new; subst y; intro _; exact allowed
  · rcases List.mem_append.mp member with old | new
    · exact good y old
    · simp only [List.mem_singleton] at new; subst y; intro admits; simp [Work.admits] at admits

/-- Every request except an admission was accepted by the gate: its catch-up
completed with a sending identity and started less than five minutes before. -/
theorem gated (p : Plan) (sender : Sender) (caught member : Bool) :
    ∀ y ∈ (runWith p sender caught member).sent, Ordinary y := by
  have each := fun (s : State) (xs : List Step) (good : ∀ y ∈ s.sent, Ordinary y) =>
    (steps_preserve p.writer p.clock (fun _ => True) Ordinary
      (fun s x _ good => ⟨trivial, step_ordinary p.writer p.clock s x good⟩) s xs trivial good).2
  exact each _ _ (each _ _ (by simp))

/-! ### Work that reads the current view uses this pass's observation -/

/-- Every completed catch-up in the sender started at or after `start`. -/
def Since (start : Nat) (s : State) : Prop :=
  ∀ c, s.sender.completed = some c → start ≤ c.started

theorem step_since (writer : Nat) (clock : Nat → Nat) (start : Nat)
    (later : ∀ k, start ≤ clock k) (s : State) (x : Step) (since : Since start s)
    (good : ∀ y ∈ s.sent, start ≤ y.evidence.started) :
    Since start (step writer clock s x) ∧
      ∀ y ∈ (step writer clock s x).sent, start ≤ y.evidence.started := by
  rcases step_cases writer clock s x with eq | ⟨work, c, -, -, completed, _, eq⟩ |
      ⟨n, rescan, c, -, completed, _, eq⟩ <;> rw [eq]
  · exact ⟨since, good⟩
  · refine ⟨since, ?_⟩
    intro y member
    rcases List.mem_append.mp member with old | new
    · exact good y old
    · simp only [List.mem_singleton] at new; subst y; exact since c completed
  · refine ⟨?_, ?_⟩
    · intro d eq
      simp only [observe_completed] at eq
      split at eq
      · cases eq; exact later _
      · exact since d eq
    · intro y member
      rcases List.mem_append.mp member with old | new
      · exact good y old
      · simp only [List.mem_singleton] at new; subst y; exact since c completed

/-- With phase 1 complete, every request of the pass uses a catch-up started
within this pass: phase 1 itself, or the observation after an admission. -/
theorem caught_uses_this_pass (p : Plan) (sender : Sender) (later : ∀ k, p.start ≤ p.clock k)
    (caught : p.caught = true) : ∀ y ∈ (run p sender).sent, p.start ≤ y.evidence.started := by
  have start : Since p.start ⟨observe sender p.start p.phase, 0, [], p.phase.failed⟩ := by
    intro c eq
    simp only [Plan.caught, Bool.and_eq_true, decide_eq_true_eq] at caught
    simp only [observe_completed, caught, and_self, ite_true] at eq
    cases eq; exact Nat.le_refl _
  have each := fun (s : State) (xs : List Step) (since : Since p.start s)
      (good : ∀ y ∈ s.sent, p.start ≤ y.evidence.started) =>
    steps_preserve p.writer p.clock (Since p.start) (fun y => p.start ≤ y.evidence.started)
      (fun s x since good => step_since p.writer p.clock p.start later s x since good) s xs since good
  have first := each _ (p.before p.caught p.member) start (by simp)
  exact (each _ _ first.1 first.2).2

/-- A step that resends fixed bytes or posts positions. -/
def Step.plain : Step → Bool
  | .send work => !work.current
  | .register _ _ => false

/-- Without a completed phase 1 the pass makes no first attempt, reserves no
copy or entry, admits nothing and runs no operation. It only resends copies
and the entry reserved earlier and attempted writes, and posts positions,
each under the last completed catch-up from before the pass. -/
theorem incomplete_only_retries (p : Plan) (sender : Sender) (incomplete : p.caught = false) :
    ∀ y ∈ (run p sender).sent, y.work.current = false ∧ some y.evidence = sender.completed := by
  have member : p.member = false := by simp [Plan.member, incomplete]
  have kept : (observe sender p.start p.phase).completed = sender.completed := by
    rw [observe_completed]
    simp only [Plan.caught, Bool.and_eq_false_iff, decide_eq_false_iff_not] at incomplete
    split
    · rename_i h; rcases incomplete with c | c
      · simp [c] at h
      · exact absurd h.2 c
    · rfl
  let Inv : State → Prop := fun s => s.sender.completed = sender.completed
  let Good : Sent → Prop := fun y => y.work.current = false ∧ some y.evidence = sender.completed
  have keeps : ∀ s x, x.plain = true → Inv s → (∀ y ∈ s.sent, Good y) →
      Inv (step p.writer p.clock s x) ∧ ∀ y ∈ (step p.writer p.clock s x).sent, Good y := by
    intro s x plain inv good
    rcases step_cases p.writer p.clock s x with eq | ⟨work, c, hx, -, completed, _, eq⟩ |
        ⟨n, rescan, c, hx, _, _, eq⟩ <;> rw [eq]
    · exact ⟨inv, good⟩
    · subst hx
      refine ⟨inv, ?_⟩
      intro y member
      rcases List.mem_append.mp member with old | new
      · exact good y old
      · simp only [List.mem_singleton] at new; subst y
        exact ⟨by simpa [Step.plain] using plain, by rw [← completed]; exact inv⟩
    · subst hx; simp [Step.plain] at plain
  have plainSteps : ∀ xs : List Step, (∀ x ∈ xs, x.plain = true) → ∀ s, Inv s →
      (∀ y ∈ s.sent, Good y) → Inv (steps p.writer p.clock s xs) ∧
        ∀ y ∈ (steps p.writer p.clock s xs).sent, Good y := by
    intro xs
    induction xs with
    | nil => intro _ s inv good; exact ⟨inv, good⟩
    | cons x xs ih =>
        intro plain s inv good
        rw [steps_cons]
        obtain ⟨inv', good'⟩ := keeps s x (plain x (by simp)) inv good
        exact ih (fun v hv => plain v (by simp [hv])) _ inv' good'
  have before : ∀ x ∈ p.before p.caught p.member, x.plain = true := by
    intro x hx
    simp only [Plan.before, Plan.keys, Plan.data, incomplete, member, List.mem_append] at hx
    rcases hx with (((copies | shares) | reserved) | ops) | (retries | firsts)
    · simp only [Bool.not_false, Bool.true_or, ite_true, List.mem_map] at copies
      obtain ⟨n, _, rfl⟩ := copies; rfl
    · simp at shares
    · simp only [Plan.reserved] at reserved
      split at reserved
      · simp at reserved
      · simp only [List.mem_singleton] at reserved; subst x; rfl
      · simp at reserved
    · simp at ops
    · obtain ⟨n, _, rfl⟩ := List.mem_map.mp retries; rfl
    · simp at firsts
  have after : ∀ wrote, ∀ x ∈ p.after p.member wrote, x.plain = true := by
    intro wrote x hx
    simp only [Plan.after, member, Bool.false_and, Bool.false_eq_true, ite_false,
      List.nil_append, List.mem_singleton] at hx
    subst x; rfl
  have first := plainSteps _ before ⟨observe sender p.start p.phase, 0, [], p.phase.failed⟩ kept
    (by simp)
  simp only [run, runWith]
  exact (plainSteps _ (after _) _ first.1 first.2).2

/-- No stale evidence: work decided from the current membership and copy
view uses a catch-up started within this pass. With `gated`, every such
request but an admission also started less than five minutes before. -/
theorem current_uses_this_pass (p : Plan) (sender : Sender) (later : ∀ k, p.start ≤ p.clock k) :
    ∀ y ∈ (run p sender).sent, y.work.current = true → p.start ≤ y.evidence.started := by
  intro y member current
  cases caught : p.caught
  · have := (incomplete_only_retries p sender caught y member).1
    rw [current] at this; cases this
  · exact caught_uses_this_pass p sender later caught y member

/-! ### Order -/

/-- The pass sends a prefix of its planned order: copies reserved earlier,
then this pass's new shares, in number order; then the reserved entry or
admission; then each operation's copies before its entry; then writes in
queue order (attempted before untried); then resumed operations and
positions. A refused request ends the pass, so nothing after it is sent. -/
theorem sent_prefix (p : Plan) (sender : Sender) (caught member : Bool) :
    ∃ wrote, (runWith p sender caught member).sent.map (·.work) <+:
      (p.planned caught member wrote).map Step.work := by
  refine ⟨wrote (steps p.writer p.clock ⟨observe sender p.start p.phase, 0, [], p.phase.failed⟩
    (p.before caught member)).sent, ?_⟩
  simp only [runWith, Plan.planned]
  rw [← steps_append]
  obtain ⟨sent, eq, pre, _⟩ := steps_prefix p.writer p.clock
    ⟨observe sender p.start p.phase, 0, [], p.phase.failed⟩
    (p.before caught member ++ p.after member (wrote (steps p.writer p.clock
      ⟨observe sender p.start p.phase, 0, [], p.phase.failed⟩ (p.before caught member)).sent))
  simpa [eq] using pre

theorem prefix_covers {α : Type} {l a b : List α} (pre : l <+: a ++ b) (y : α) (here : y ∈ l)
    (outside : y ∉ a) : a <+: l := by
  by_cases longer : a.length ≤ l.length
  · exact List.prefix_of_prefix_length_le (List.prefix_append a b) pre longer
  · have inside : l <+: a := List.prefix_of_prefix_length_le pre (List.prefix_append a b) (by omega)
    exact absurd (inside.subset here) outside

/-- No write goes up before phase 2 finished: every copy and entry planned
before data, a needed rotation's copies and entry included, was sent first. -/
theorem keys_before_data (p : Plan) (sender : Sender) (caught member : Bool) (y : Sent)
    (sent : y ∈ (runWith p sender caught member).sent) (writes : y.work.writes = true) :
    (p.keys caught member).map Step.work <+: (runWith p sender caught member).sent.map (·.work) := by
  obtain ⟨wrote, pre⟩ := sent_prefix p sender caught member
  simp only [Plan.planned, Plan.before, List.map_append, List.append_assoc] at pre
  refine prefix_covers pre y.work (List.mem_map.mpr ⟨y, sent, rfl⟩) ?_
  intro inside
  obtain ⟨x, member, eq⟩ := List.mem_map.mp inside
  simp only [Plan.keys, List.mem_append] at member
  rcases member with (((copies | shared) | reserved) | operations)
  · split at copies
    · obtain ⟨n, _, rfl⟩ := List.mem_map.mp copies; rw [← eq] at writes; cases writes
    · cases copies
  · split at shared
    · obtain ⟨n, _, rfl⟩ := List.mem_map.mp shared; rw [← eq] at writes; cases writes
    · cases shared
  · simp only [Plan.reserved] at reserved
    split at reserved
    · cases reserved
    · simp only [List.mem_singleton] at reserved; subst x; rw [← eq] at writes; cases writes
    · split at reserved
      · simp only [List.mem_singleton] at reserved; subst x; rw [← eq] at writes; cases writes
      · split at reserved
        · simp only [List.mem_singleton] at reserved; subst x; rw [← eq] at writes; cases writes
        · cases reserved
  · split at operations
    · obtain ⟨op, _, inOp⟩ := List.mem_flatMap.mp operations
      simp only [operation, List.mem_append, List.mem_map, List.mem_singleton] at inOp
      rcases inOp with ⟨n, _, rfl⟩ | rfl <;> (rw [← eq] at writes; cases writes)
    · cases operations

/-- A failed read in phase 1 ends the pass: nothing is sent. -/
theorem failed_sends_nothing (p : Plan) (sender : Sender) (caught member : Bool)
    (failed : p.phase.failed = true) : (runWith p sender caught member).sent = [] := by
  have start := stopped_steps p.writer p.clock
    ⟨observe sender p.start p.phase, 0, [], p.phase.failed⟩ (p.before caught member) failed
  simp only [runWith]
  rw [start, stopped_steps _ _ _ _ failed]

/-- An ordinary request after an admission, by `Pairwise`. -/
def AfterAdmission (x y : Sent) : Prop :=
  x.work.admits = true → y.work.admits = false → x.time ≤ y.evidence.started

/-- Admissions so far precede the next request, and any sending catch-up
the sender now holds started after each of them. -/
def Admitted (clock : Nat → Nat) (s : State) : Prop :=
  ∀ x ∈ s.sent, x.work.admits = true → x.time ≤ clock s.next ∧
    ∀ c, s.sender.completed = some c → c.identity = .send → x.time ≤ c.started

theorem step_admitted (writer : Nat) (clock : Nat → Nat)
    (monotone : ∀ i j, i ≤ j → clock i ≤ clock j) (s : State) (x : Step)
    (inv : Admitted clock s) (pairs : s.sent.Pairwise AfterAdmission) :
    Admitted clock (step writer clock s x) ∧ (step writer clock s x).sent.Pairwise AfterAdmission := by
  rcases step_cases writer clock s x with eq | ⟨work, c, -, plain, completed, allowed, eq⟩ |
      ⟨n, rescan, c, -, completed, ok, eq⟩ <;> rw [eq]
  · exact ⟨inv, pairs⟩
  · refine ⟨?_, ?_⟩
    · intro y member admits
      rcases List.mem_append.mp member with old | new
      · obtain ⟨bound, held⟩ := inv y old admits
        exact ⟨Nat.le_trans bound (monotone _ _ (Nat.le_succ _)), held⟩
      · simp only [List.mem_singleton] at new; subst y
        simp [plain] at admits
    · refine List.pairwise_append.mpr ⟨pairs, List.pairwise_singleton _ _, ?_⟩
      intro y old z new admits _
      simp only [List.mem_singleton] at new; subst z
      exact (inv y old admits).2 c completed allowed.1
  · have notSend : c.identity ≠ .send := by
      intro same; simp [admissible, same] at ok
    refine ⟨?_, ?_⟩
    · intro y member admits
      have bound : y.time ≤ clock s.next := by
        rcases List.mem_append.mp member with old | new
        · exact (inv y old admits).1
        · simp only [List.mem_singleton] at new; subst y; exact Nat.le_refl _
      refine ⟨Nat.le_trans bound (monotone _ _ (by show s.next ≤ s.next + 2; omega)), ?_⟩
      intro d eq sending
      simp only [observe_completed] at eq
      split at eq
      · cases eq; exact Nat.le_trans bound (monotone _ _ (Nat.le_succ _))
      · rw [completed] at eq; cases eq; exact absurd sending notSend
    · refine List.pairwise_append.mpr ⟨pairs, List.pairwise_singleton _ _, ?_⟩
      intro y _ z new _ plain
      simp only [List.mem_singleton] at new; subst z
      simp [Work.admits] at plain

/-- §10's admission exception grants no ordinary authority: every ordinary
request after an admission uses a catch-up that started after the admission
was sent, which therefore observes it. -/
theorem admission_rescans (p : Plan) (sender : Sender) (caught member : Bool)
    (monotone : ∀ i j, i ≤ j → p.clock i ≤ p.clock j) :
    (runWith p sender caught member).sent.Pairwise AfterAdmission := by
  let Inv : State → Prop := fun s => Admitted p.clock s ∧ s.sent.Pairwise AfterAdmission
  have each : ∀ s xs, Inv s → Inv (steps p.writer p.clock s xs) := by
    intro s xs inv
    exact (steps_preserve p.writer p.clock Inv (fun _ => True)
      (fun s x inv _ => ⟨step_admitted p.writer p.clock monotone s x inv.1 inv.2, by simp⟩)
      s xs inv (by simp)).1
  have start : Inv ⟨observe sender p.start p.phase, 0, [], p.phase.failed⟩ :=
    ⟨by simp [Admitted], List.Pairwise.nil⟩
  exact (each _ _ (each _ _ start)).2

/-- An admission uses a membership-complete catch-up of a member that is not
retired and does not yet hold ordinary sending identity. -/
theorem admission_identity (p : Plan) (sender : Sender) (caught member : Bool) :
    ∀ y ∈ (runWith p sender caught member).sent, y.work.admits = true →
      y.evidence.identity ≠ .send ∧ y.evidence.retired = false := by
  let Good : Sent → Prop := fun y => y.work.admits = true →
    y.evidence.identity ≠ .send ∧ y.evidence.retired = false
  have keeps : ∀ s x, True → (∀ y ∈ s.sent, Good y) →
      True ∧ ∀ y ∈ (step p.writer p.clock s x).sent, Good y := by
    intro s x _ good
    refine ⟨trivial, ?_⟩
    rcases step_cases p.writer p.clock s x with eq | ⟨work, c, -, plain, _, _, eq⟩ |
        ⟨n, rescan, c, -, _, ok, eq⟩ <;> rw [eq] <;> intro y member
    · exact good y member
    · rcases List.mem_append.mp member with old | new
      · exact good y old
      · simp only [List.mem_singleton] at new; subst y
        intro admits
        simp [plain] at admits
    · rcases List.mem_append.mp member with old | new
      · exact good y old
      · simp only [List.mem_singleton] at new; subst y
        intro _
        simp only [admissible, Bool.and_eq_true, bne_iff_ne, ne_eq, Bool.not_eq_true'] at ok
        exact ⟨ok.1.1, ok.1.2⟩
  have each := fun (s : State) (xs : List Step) (good : ∀ y ∈ s.sent, Good y) =>
    (steps_preserve p.writer p.clock (fun _ => True) Good keeps s xs trivial good).2
  exact each _ _ (each _ _ (by simp))

/-! ### Checked history: deciding phase 1 from its entries alone -/

/-- The previous pass completed at second 0. This pass starts at 10 and
reads every entry and copy to its miss, but one copy's clear prefix does
not decode, so its copy observation is incomplete (§11). -/
def unreadableCopy : Plan where
  writer := 1
  start := 10
  phase := ⟨⟨10, 1, [], [], .miss⟩, ⟨10, 0, [], [], .miss⟩, false, .send, false, 10⟩
  copies := []
  shares := []
  entry := none
  admission := none
  operations := []
  attempted := []
  untried := [1]
  resumed := []
  clock := fun _ => 10

def previousPass : Sender := ⟨none, some ⟨0, 0, .send, false⟩⟩

/-- Treating phase 1 as complete because its entries completed while an
earlier catch-up is still fresh sends write 1's first attempt with the
previous pass's copies, started before this pass. Deciding from the pass's
own observation sends only positions. -/
theorem entries_only_reuses_previous :
    unreadableCopy.caught = false ∧
    (runWith unreadableCopy previousPass true true).sent.map (fun y => (y.work, y.evidence.started)) =
      [(.first 1, 0), (.positions, 0)] ∧
    (run unreadableCopy previousPass).sent.map (·.work) = [.positions] := by
  decide

/-! ### Invites name a copy number after every reserved copy -/

/-- Reflexive-transitive closure of `Publication.Step`. -/
inductive Settles : Publication.Writer → Publication.Writer → Prop
  | refl (s : Publication.Writer) : Settles s s
  | step {a b c : Publication.Writer} : Publication.Step a b → Settles b c → Settles a c

/-- From any reachable copy log, publishing and confirming each reserved
copy in number order confirms all of them. An invite that reads the
confirmed number after this names a number at or after every copy reserved
before it, so the joiner's wait reads only later copies (§12.2, SP step 4). -/
theorem invite_after_reserved (s : Publication.Writer) (valid : Publication.Valid s) :
    Settles s ⟨s.reserved, s.reserved, s.reserved.length⟩ := by
  obtain ⟨lo, hi, rest, eq⟩ := valid
  -- Confirm a landed copy whose reply was lost, then settle the remainder.
  have settle : ∀ (rest : List Bytes) (t : Publication.Writer), t.reserved = t.published ++ rest →
      t.confirmed = t.published.length →
      Settles t ⟨t.reserved, t.reserved, t.reserved.length⟩ := by
    intro rest
    induction rest with
    | nil =>
        intro t eq confirmed
        simp only [List.append_nil] at eq
        have same : t = ⟨t.reserved, t.reserved, t.reserved.length⟩ := by
          cases t; simp_all
        rw [← same]; exact .refl t
    | cons bytes rest ih =>
        intro t eq confirmed
        let landed : Publication.Writer := { t with published := t.published ++ [bytes] }
        let done : Publication.Writer := { landed with confirmed := landed.confirmed + 1 }
        have land : Publication.Step t landed := .land t bytes rest confirmed eq
        have confirm : Publication.Step landed done := .confirm landed (by simp [landed, confirmed])
        have next := ih done (by simp [done, landed, eq]) (by simp [done, landed, confirmed])
        exact .step land (.step confirm (by simpa [done, landed] using next))
  by_cases lost : s.published.length = s.confirmed + 1
  · let done : Publication.Writer := { s with confirmed := s.confirmed + 1 }
    have next := settle rest done (by simpa [done] using eq.symm) (by simp [done, lost])
    exact .step (.confirm s lost) (by simpa [done] using next)
  · exact settle rest s eq.symm (by omega)

/-- Reading the number with a copy still reserved names a number before it. -/
theorem unsettled_cursor_precedes_reserved :
    Publication.Valid ⟨[[1], [2]], [[1]], 1⟩ ∧
      (⟨[[1], [2]], [[1]], 1⟩ : Publication.Writer).confirmed < 2 :=
  ⟨⟨by decide, by decide, [[2]], rfl⟩, by decide⟩

end CovenIO.PassPhases
