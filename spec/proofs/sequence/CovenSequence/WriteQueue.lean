/-!
# Leaving the queue (§6.3)

A device's queued writes go out in a writes slot, one claim. The write record
leaves the queue only when two things have happened: the claim is settled as
landed, and a slot that carries the write has been applied, so the state holds
what the write did (`claim::finish`). The rest of the queue is then applied over
that state, and no write is applied twice.

The model has the queue, the writes the applied slots consumed, and one open
claim carrying a prefix of the queue. The claim's create may land at any time;
its slot applies once it has; settling as landed needs the slot's bytes there;
`finish` needs the settlement and the application.

- `nothing_lost`: every write ever committed is in the queue or in the state.
- `applied_once`: the state counts each write once.
- `leaves_only_landed`: a write not in the queue was carried by a claim that
  landed.
- `visible_exactly_once`: the app's rows, the state then the rest of the queue,
  hold every committed write exactly once.
- `unguarded_finish_loses` is the counterexample: forgetting the writes of a
  settled claim before its slot has applied leaves them nowhere.
-/
namespace CovenSequence.WriteQueue

/-- The open claim: the writes it carries, whether its slot holds them in the
sequence, and whether it has settled as landed. -/
structure Claim where
  carried : List Nat
  landed : Bool
  settled : Bool
  deriving DecidableEq

structure World where
  /-- The next write number. -/
  next : Nat
  /-- The queue, in write-number order. -/
  queue : List Nat
  /-- The writes the applied slots consumed. -/
  applied : List Nat
  /-- The writes carried by any claim that landed. -/
  landedWrites : List Nat
  claim : Option Claim

def World.start : World := ⟨0, [], [], [], none⟩

inductive Step : World → World → Prop
  /-- The app commits a write. -/
  | commit (w : World) :
      Step w { w with next := w.next + 1, queue := w.queue ++ [w.next] }
  /-- A pass opens a claim for the first `k` queued writes. -/
  | begin (w : World) (k : Nat) (none : w.claim = none) :
      Step w { w with claim := some ⟨w.queue.take k, false, false⟩ }
  /-- The claim's create lands: its slot holds the writes. -/
  | land (w : World) (c : Claim) (h : w.claim = some c) (notYet : c.landed = false) :
      Step w { w with claim := some { c with landed := true },
                      landedWrites := w.landedWrites ++ c.carried }
  /-- The slot is applied: its writes are consumed into the state, once. -/
  | apply (w : World) (c : Claim) (h : w.claim = some c) (landed : c.landed = true) :
      Step w { w with applied := w.applied ++ c.carried.filter (· ∉ w.applied) }
  /-- Settling reads the slot and finds the claim's own bytes. -/
  | settle (w : World) (c : Claim) (h : w.claim = some c) (landed : c.landed = true) :
      Step w { w with claim := some { c with settled := true } }
  /-- The landed claim ends and its writes leave the queue, once settled and
  consumed. -/
  | finish (w : World) (c : Claim) (h : w.claim = some c) (settled : c.settled = true)
      (consumed : ∀ x ∈ c.carried, x ∈ w.applied) :
      Step w { w with queue := w.queue.filter (· ∉ c.carried), claim := none }

inductive Steps : World → World → Prop
  | refl (w : World) : Steps w w
  | step {a b c : World} : Step a b → Steps b c → Steps a c

/-- What every reachable world satisfies. -/
structure Inv (w : World) : Prop where
  covered : ∀ x, x < w.next → x ∈ w.queue ∨ x ∈ w.applied
  numbered : ∀ x ∈ w.queue, x < w.next
  appliedNumbered : ∀ x ∈ w.applied, x < w.next
  queueNodup : w.queue.Nodup
  appliedNodup : w.applied.Nodup
  appliedLanded : ∀ x ∈ w.applied, x ∈ w.landedWrites
  carriedQueued : ∀ c, w.claim = some c → ∀ x ∈ c.carried, x ∈ w.queue
  carriedNodup : ∀ c, w.claim = some c → c.carried.Nodup
  claimLanded : ∀ c, w.claim = some c → c.landed = true → ∀ x ∈ c.carried, x ∈ w.landedWrites

theorem inv_start : Inv World.start where
  covered := by intro x h; simp [World.start] at h
  numbered := by simp [World.start]
  appliedNumbered := by simp [World.start]
  queueNodup := by simp [World.start]
  appliedNodup := by simp [World.start]
  appliedLanded := by simp [World.start]
  carriedQueued := by simp [World.start]
  carriedNodup := by simp [World.start]
  claimLanded := by simp [World.start]

theorem queue_step_inv {a b : World} (s : Step a b) (i : Inv a) : Inv b := by
  cases s with
  | commit =>
    refine ⟨?_, ?_, ?_, ?_, i.appliedNodup, i.appliedLanded, ?_, i.carriedNodup, i.claimLanded⟩
    · intro x hx
      simp only [List.mem_append, List.mem_singleton]
      by_cases e : x = a.next
      · exact Or.inl (Or.inr e)
      · rcases i.covered x (by simp only at hx; omega) with h | h
        · exact Or.inl (Or.inl h)
        · exact Or.inr h
    · intro x hx
      simp only [List.mem_append, List.mem_singleton] at hx
      rcases hx with h | rfl
      · have := i.numbered x h; show x < a.next + 1; omega
      · show a.next < a.next + 1; omega
    · intro x hx; have := i.appliedNumbered x hx; show x < a.next + 1; omega
    · rw [List.nodup_append]
      refine ⟨i.queueNodup, by simp, ?_⟩
      intro x hx y hy e
      simp only [List.mem_singleton] at hy
      subst hy; subst e
      have := i.numbered _ hx; omega
    · intro c hc x hx; exact List.mem_append_left _ (i.carriedQueued c hc x hx)
  | begin k none =>
    refine ⟨i.covered, i.numbered, i.appliedNumbered, i.queueNodup, i.appliedNodup,
      i.appliedLanded, ?_, ?_, ?_⟩
    · intro c hc x hx
      simp only [Option.some.injEq] at hc; subst hc
      exact List.mem_of_mem_take hx
    · intro c hc
      simp only [Option.some.injEq] at hc; subst hc
      exact i.queueNodup.sublist (List.take_sublist _ _)
    · intro c hc hl; simp only [Option.some.injEq] at hc; subst hc; simp at hl
  | land c h notYet =>
    refine ⟨i.covered, i.numbered, i.appliedNumbered, i.queueNodup, i.appliedNodup, ?_, ?_, ?_, ?_⟩
    · intro x hx; exact List.mem_append_left _ (i.appliedLanded x hx)
    · intro c' hc' x hx
      simp only [Option.some.injEq] at hc'; subst hc'
      exact i.carriedQueued c h x hx
    · intro c' hc'
      simp only [Option.some.injEq] at hc'; subst hc'
      exact i.carriedNodup c h
    · intro c' hc' _ x hx
      simp only [Option.some.injEq] at hc'; subst hc'
      exact List.mem_append_right _ hx
  | apply c h landed =>
    have cq := i.carriedQueued c h
    have cn := i.carriedNodup c h
    have cl := i.claimLanded c h landed
    refine ⟨?_, i.numbered, ?_, ?_, ?_, ?_, i.carriedQueued, i.carriedNodup, i.claimLanded⟩
    · intro x hx
      rcases i.covered x hx with q | ap
      · by_cases m : x ∈ c.carried
        · by_cases ia : x ∈ a.applied
          · exact Or.inr (List.mem_append_left _ ia)
          · exact Or.inr (List.mem_append_right _ (List.mem_filter.mpr ⟨m, by simpa using ia⟩))
        · exact Or.inl q
      · exact Or.inr (List.mem_append_left _ ap)
    · intro x hx
      rcases List.mem_append.mp hx with h' | h'
      · exact i.appliedNumbered x h'
      · exact i.numbered x (cq x (List.mem_filter.mp h').1)
    · exact i.queueNodup
    · rw [List.nodup_append]
      refine ⟨i.appliedNodup, cn.filter _, ?_⟩
      intro x hx y hy e
      subst e
      have := (List.mem_filter.mp hy).2
      simp at this
      exact this hx
    · intro x hx
      rcases List.mem_append.mp hx with h' | h'
      · exact i.appliedLanded x h'
      · exact cl x (List.mem_filter.mp h').1
  | settle c h landed =>
    refine ⟨i.covered, i.numbered, i.appliedNumbered, i.queueNodup, i.appliedNodup,
      i.appliedLanded, ?_, ?_, ?_⟩
    · intro c' hc' x hx
      simp only [Option.some.injEq] at hc'; subst hc'
      exact i.carriedQueued c h x hx
    · intro c' hc'
      simp only [Option.some.injEq] at hc'; subst hc'
      exact i.carriedNodup c h
    · intro c' hc' hl x hx
      simp only [Option.some.injEq] at hc'; subst hc'
      exact i.claimLanded c h landed x hx
  | finish c h settled consumed =>
    refine ⟨?_, ?_, i.appliedNumbered, i.queueNodup.filter _, i.appliedNodup, i.appliedLanded,
      ?_, ?_, ?_⟩
    · intro x hx
      rcases i.covered x hx with q | ap
      · by_cases m : x ∈ c.carried
        · exact Or.inr (consumed x m)
        · exact Or.inl (List.mem_filter.mpr ⟨q, by simpa using m⟩)
      · exact Or.inr ap
    · intro x hx; exact i.numbered x (List.mem_filter.mp hx).1
    · intro c' hc'; simp at hc'
    · intro c' hc'; simp at hc'
    · intro c' hc'; simp at hc'

theorem queue_steps_inv {a b : World} (run : Steps a b) (i : Inv a) : Inv b := by
  induction run with
  | refl => exact i
  | step s _ ih => exact ih (queue_step_inv s i)

/-- **Nothing is lost.** Every write ever committed is in the queue or in the
state. -/
theorem nothing_lost {w : World} (run : Steps World.start w) {x : Nat} (committed : x < w.next) :
    x ∈ w.queue ∨ x ∈ w.applied :=
  (queue_steps_inv run inv_start).covered x committed

/-- **The state counts each write once.** -/
theorem applied_once {w : World} (run : Steps World.start w) : w.applied.Nodup :=
  (queue_steps_inv run inv_start).appliedNodup

/-- **A write leaves the queue only after its claim landed**, and the state
holds it. -/
theorem leaves_only_landed {w : World} (run : Steps World.start w) {x : Nat}
    (committed : x < w.next) (gone : x ∉ w.queue) : x ∈ w.applied ∧ x ∈ w.landedWrites := by
  have i := queue_steps_inv run inv_start
  rcases i.covered x committed with q | ap
  · exact absurd q gone
  · exact ⟨ap, i.appliedLanded x ap⟩

/-- The app's rows: the state, then the rest of the queue over it, the writes
the state already holds left out. -/
def World.visible (w : World) : List Nat := w.applied ++ w.queue.filter (· ∉ w.applied)

/-- **The rest of the queue applies over the state, and every write once.** -/
theorem visible_exactly_once {w : World} (run : Steps World.start w) :
    w.visible.Nodup ∧ ∀ x, x ∈ w.visible ↔ x < w.next := by
  have i := queue_steps_inv run inv_start
  refine ⟨?_, fun x => ⟨fun hx => ?_, fun hx => ?_⟩⟩
  · unfold World.visible
    rw [List.nodup_append]
    refine ⟨i.appliedNodup, i.queueNodup.filter _, ?_⟩
    intro x hx y hy e
    subst e
    have := (List.mem_filter.mp hy).2
    simp at this
    exact this hx
  · rcases List.mem_append.mp hx with h | h
    · exact i.appliedNumbered x h
    · exact i.numbered x (List.mem_filter.mp h).1
  · unfold World.visible
    by_cases ap : x ∈ w.applied
    · exact List.mem_append_left _ ap
    · rcases i.covered x hx with q | ap'
      · exact List.mem_append_right _ (List.mem_filter.mpr ⟨q, by simpa using ap⟩)
      · exact absurd ap' ap

/-- Forgetting the writes of a settled claim without waiting for their slot to
apply. -/
def finishUnguarded (w : World) (c : Claim) : World :=
  { w with queue := w.queue.filter (· ∉ c.carried), claim := none }

/-- **Counterexample: no wait for the application.** A write whose claim
settled as landed, forgotten before its slot applied, is in neither the queue
nor the state. -/
theorem unguarded_finish_loses :
    ∃ w c, Steps World.start w ∧ w.claim = some c ∧ c.settled = true ∧
      0 ∈ c.carried ∧ 0 ∉ (finishUnguarded w c).queue ∧ 0 ∉ (finishUnguarded w c).applied := by
  have run : Steps World.start
      { next := 1, queue := [0], applied := [], landedWrites := [0],
        claim := some ⟨[0], true, true⟩ } := by
    refine .step (Step.commit _) (.step (Step.begin _ 1 rfl) (.step (Step.land _ _ rfl rfl)
      (.step (Step.settle _ _ rfl rfl) (.refl _))))
  exact ⟨_, ⟨[0], true, true⟩, run, rfl, rfl, by simp, by simp [finishUnguarded], by simp [finishUnguarded]⟩

end CovenSequence.WriteQueue
