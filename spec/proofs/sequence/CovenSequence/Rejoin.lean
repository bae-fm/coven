import CovenSequence.Registration
import CovenSequence.Settle

/-!
# Two copies of one device id (§5.3, §19.3)

A restored backup and the installation it was taken from both sign slots with
one device id. This file models what each copy does from the moment they part,
and proves what the copy that finds the other has and hasn't done, so it can
leave the old id with every unsent write and nothing landed.

The world holds the slots signed with the id (slots of other devices are read
and applied with no effect on any of this) and each copy's state: the slots it
applied, its journal of attempts, its one open claim, the creates it sent, its
queue of unsent writes, and whether it stopped. A copy follows the journal
rules of `ClaimJournal`: it records an attempt before sending it, keeps one
claim open at a time, claims slot `applied + 1`, and reads a slot only with no
claim open. What it adds is the detection of §5.3: reading a slot signed with
its id whose attempt its journal never recorded, or settling a claim and finding
other bytes at its slot, stops the copy. A stopped copy begins, sends and reads
nothing; creates it already sent can still land, and the app's new writes still
queue.

The copies part at `W0`, with every copy settled: no claim open, every create
it sent aimed at a taken slot, every write it carried out of its queue. Backups
taken with a claim open are not in this model.
-/
namespace CovenSequence.Rejoin

/-- One recorded attempt: the slot it claims, its sealed bytes (a token no
other attempt shares), and the queued writes it carries. -/
structure Attempt where
  slot : Nat
  bytes : Nat
  writes : List Nat
  deriving DecidableEq

structure Copy where
  /-- The slots through this one are applied. -/
  applied : Nat
  journal : List Attempt
  /-- The open claim's attempt. -/
  live : Option Attempt
  /-- Creates sent, some possibly still in flight. -/
  sent : List Attempt
  /-- Writes made here, not yet carried by a claim that landed and applied. -/
  queue : List Nat
  stopped : Bool

structure World (Inst : Type) where
  /-- Each stored slot: the copy that created it (not visible to any copy) and
  its attempt. -/
  slots : Nat → Option (Inst × Attempt)
  head : Nat
  copy : Inst → Copy

variable {Inst : Type} [DecidableEq Inst]

def World.mod (w : World Inst) (ι : Inst) (f : Copy → Copy) : World Inst :=
  { w with copy := fun κ => if κ = ι then f (w.copy ι) else w.copy κ }

/-- The writes of the slots through `n`, in order. -/
def histOf (w : World Inst) : Nat → List Nat
  | 0 => []
  | n + 1 => histOf w n ++ ((w.slots (n + 1)).map (·.2.writes)).getD []

/-- A write nobody has queued, carried, sent or applied. -/
def Fresh (w : World Inst) (x : Nat) : Prop :=
  (∀ κ, x ∉ (w.copy κ).queue ∧ ∀ a ∈ (w.copy κ).journal, x ∉ a.writes) ∧
  ∀ j κ a, w.slots j = some (κ, a) → x ∉ a.writes

inductive Step : World Inst → World Inst → Prop
  /-- The app commits a write, stopped or not. -/
  | commit (w : World Inst) (ι : Inst) (x : Nat) (fresh : Fresh w x) :
      Step w (w.mod ι fun c => { c with queue := c.queue ++ [x] })
  /-- Begin a claim: record the attempt for the slot after those applied. -/
  | begin (w : World Inst) (ι : Inst) (a : Attempt) (go : (w.copy ι).stopped = false)
      (idle : (w.copy ι).live = none) (slot : a.slot = (w.copy ι).applied + 1)
      (fresh : ∀ κ b, b ∈ (w.copy κ).journal → b.bytes ≠ a.bytes)
      (carried : ∀ x ∈ a.writes, x ∈ (w.copy ι).queue) :
      Step w (w.mod ι fun c => { c with journal := a :: c.journal, live := some a })
  /-- Send the recorded attempt's create. -/
  | send (w : World Inst) (ι : Inst) (a : Attempt) (go : (w.copy ι).stopped = false)
      (h : (w.copy ι).live = some a) :
      Step w (w.mod ι fun c => { c with sent := a :: c.sent })
  /-- A create in flight lands in the empty slot after the head. -/
  | land (w : World Inst) (ι : Inst) (a : Attempt) (h : a ∈ (w.copy ι).sent)
      (next : a.slot = w.head + 1) :
      Step w { w with slots := fun j => if j = w.head + 1 then some (ι, a) else w.slots j,
                      head := w.head + 1 }
  /-- Reading a slot whose attempt the journal recorded applies it. -/
  | readOwn (w : World Inst) (ι κ : Inst) (a : Attempt) (go : (w.copy ι).stopped = false)
      (idle : (w.copy ι).live = none) (lt : (w.copy ι).applied < w.head)
      (at_ : w.slots ((w.copy ι).applied + 1) = some (κ, a)) (mine : a ∈ (w.copy ι).journal) :
      Step w (w.mod ι fun c => { c with applied := c.applied + 1 })
  /-- Reading a slot signed with this id whose attempt the journal never
  recorded: another copy uses the id. -/
  | detectRead (w : World Inst) (ι κ : Inst) (a : Attempt) (go : (w.copy ι).stopped = false)
      (idle : (w.copy ι).live = none) (lt : (w.copy ι).applied < w.head)
      (at_ : w.slots ((w.copy ι).applied + 1) = some (κ, a))
      (foreign : a ∉ (w.copy ι).journal) :
      Step w (w.mod ι fun c => { c with stopped := true })
  /-- Settling finds the attempt's own bytes: it landed, and its writes leave
  the queue. -/
  | settleLanded (w : World Inst) (ι κ : Inst) (a o : Attempt) (go : (w.copy ι).stopped = false)
      (h : (w.copy ι).live = some o) (filled : o.slot ≤ w.head)
      (at_ : w.slots o.slot = some (κ, a)) (same : a.bytes = o.bytes) :
      Step w (w.mod ι fun c => { c with applied := o.slot, live := none,
                                        queue := c.queue.filter fun x => decide (x ∉ o.writes) })
  /-- Settling finds other bytes at the claim's slot: another copy uses the id. -/
  | detectSettle (w : World Inst) (ι κ : Inst) (a o : Attempt) (go : (w.copy ι).stopped = false)
      (h : (w.copy ι).live = some o) (filled : o.slot ≤ w.head)
      (at_ : w.slots o.slot = some (κ, a)) (diff : a.bytes ≠ o.bytes) :
      Step w (w.mod ι fun c => { c with stopped := true })

inductive Steps : World Inst → World Inst → Prop
  | refl (w : World Inst) : Steps w w
  | step {w w' w'' : World Inst} : Step w w' → Steps w' w'' → Steps w w''

/-- The copies at the moment they part: every one settled. -/
structure Parted (w : World Inst) : Prop where
  slot : ∀ j κ a, w.slots j = some (κ, a) →
    1 ≤ j ∧ j ≤ w.head ∧ a.slot = j ∧ a ∈ (w.copy κ).journal ∧ j ≤ (w.copy κ).applied
  idle : ∀ κ, (w.copy κ).live = none ∧ (w.copy κ).stopped = false ∧
    (w.copy κ).applied ≤ w.head
  sent_taken : ∀ κ a, a ∈ (w.copy κ).sent → a.slot ≤ w.head ∧ a ∈ (w.copy κ).journal
  settled : ∀ κ a, a ∈ (w.copy κ).journal → a.slot ≤ (w.copy κ).applied
  applied_own : ∀ κ j, 1 ≤ j → j ≤ (w.copy κ).applied →
    ∃ κ' a, w.slots j = some (κ', a) ∧ a ∈ (w.copy κ).journal
  carried_out : ∀ κ a, a ∈ (w.copy κ).journal → ∀ x ∈ a.writes, x ∉ (w.copy κ).queue
  bytes_name : ∀ ι κ a a', a ∈ (w.copy ι).journal → a' ∈ (w.copy κ).journal →
    a.bytes = a'.bytes → a = a'
  positive : ∀ κ a, a ∈ (w.copy κ).journal → 1 ≤ a.slot
  full : ∀ j, 1 ≤ j → j ≤ w.head → ∃ x, w.slots j = some x

/-- What every state reached from `W0` keeps true. -/
structure Inv (w0 w : World Inst) : Prop where
  slot : ∀ j κ a, w.slots j = some (κ, a) →
    1 ≤ j ∧ j ≤ w.head ∧ a.slot = j ∧ a ∈ (w.copy κ).journal
  sent_in : ∀ κ a, a ∈ (w.copy κ).sent → a ∈ (w.copy κ).journal
  applied_own : ∀ κ j, 1 ≤ j → j ≤ (w.copy κ).applied →
    ∃ κ' a, w.slots j = some (κ', a) ∧ a ∈ (w.copy κ).journal
  applied_le : ∀ κ, (w.copy κ).applied ≤ w.head
  bytes_name : ∀ ι κ a a', a ∈ (w.copy ι).journal → a' ∈ (w.copy κ).journal →
    a.bytes = a'.bytes → a = a'
  /-- An attempt for a slot after the parting is in one copy's journal only. -/
  one_owner : ∀ ι κ a a', a ∈ (w.copy ι).journal → a' ∈ (w.copy κ).journal →
    a.bytes = a'.bytes → w0.head < a.slot → ι = κ
  carried_out : ∀ κ a, a ∈ (w.copy κ).journal → (w.copy κ).live ≠ some a →
    ∀ x ∈ a.writes, x ∉ (w.copy κ).queue
  /-- An attempt no longer open landed and was applied. -/
  settled : ∀ κ a, a ∈ (w.copy κ).journal → (w.copy κ).live ≠ some a →
    a.slot ≤ (w.copy κ).applied
  /-- A slot a copy created is applied, unless it is the open claim's. -/
  own_applied : ∀ κ j a, w.slots j = some (κ, a) →
    j ≤ (w.copy κ).applied ∨ (w.copy κ).live = some a
  sent_taken : ∀ κ a, a ∈ (w.copy κ).sent → a.slot ≤ w.head ∨ (w.copy κ).live = some a
  stopped_sent : ∀ κ, (w.copy κ).stopped = true → ∀ a ∈ (w.copy κ).sent, a.slot ≤ w.head
  opened : ∀ κ o, (w.copy κ).live = some o →
    o ∈ (w.copy κ).journal ∧ o.slot = (w.copy κ).applied + 1 ∧
    ∀ x ∈ o.writes, x ∈ (w.copy κ).queue
  /-- A stopped copy created no slot after the parting. -/
  stopped_landed_nothing : ∀ κ, (w.copy κ).stopped = true →
    ∀ j a, w.slots j = some (κ, a) → j ≤ w0.head
  /-- One copy created every slot after the parting. -/
  one_keeper : ∀ j j' κ κ' a a', w0.head < j → w0.head < j' →
    w.slots j = some (κ, a) → w.slots j' = some (κ', a') → κ = κ'
  /-- A stopped copy found a slot whose attempt its journal never recorded. -/
  evidence : ∀ κ, (w.copy κ).stopped = true →
    ∃ j κ' a, w.slots j = some (κ', a) ∧ a ∉ (w.copy κ).journal
  grows : w0.head ≤ w.head ∧ ∀ κ, (w0.copy κ).applied ≤ (w.copy κ).applied
  positive : ∀ κ a, a ∈ (w.copy κ).journal → 1 ≤ a.slot
  filled : ∀ j, 1 ≤ j → j ≤ w.head → ∃ x, w.slots j = some x
  /-- The slots at the parting stay as they were. -/
  old_slots : ∀ j, j ≤ w0.head → w.slots j = w0.slots j
  journal_grows : ∀ κ a, a ∈ (w0.copy κ).journal → a ∈ (w.copy κ).journal

@[simp] theorem mod_slots (w : World Inst) (ι : Inst) (f : Copy → Copy) :
    (w.mod ι f).slots = w.slots := rfl

@[simp] theorem mod_head (w : World Inst) (ι : Inst) (f : Copy → Copy) :
    (w.mod ι f).head = w.head := rfl

theorem mod_self (w : World Inst) (ι : Inst) (f : Copy → Copy) :
    (w.mod ι f).copy ι = f (w.copy ι) := by simp [World.mod]

theorem mod_ne (w : World Inst) {ι κ : Inst} (f : Copy → Copy) (h : κ ≠ ι) :
    (w.mod ι f).copy κ = w.copy κ := by simp [World.mod, h]

section ModFields
variable (w : World Inst) {ι : Inst} (κ : Inst) (f : Copy → Copy)

@[simp] theorem mod_journal (hf : ∀ c, (f c).journal = c.journal) :
    ((w.mod ι f).copy κ).journal = (w.copy κ).journal := by
  by_cases h : κ = ι
  · subst h; simp [World.mod, hf]
  · simp [World.mod, h]

@[simp] theorem mod_applied (hf : ∀ c, (f c).applied = c.applied) :
    ((w.mod ι f).copy κ).applied = (w.copy κ).applied := by
  by_cases h : κ = ι
  · subst h; simp [World.mod, hf]
  · simp [World.mod, h]

@[simp] theorem mod_live (hf : ∀ c, (f c).live = c.live) :
    ((w.mod ι f).copy κ).live = (w.copy κ).live := by
  by_cases h : κ = ι
  · subst h; simp [World.mod, hf]
  · simp [World.mod, h]

@[simp] theorem mod_sent (hf : ∀ c, (f c).sent = c.sent) :
    ((w.mod ι f).copy κ).sent = (w.copy κ).sent := by
  by_cases h : κ = ι
  · subst h; simp [World.mod, hf]
  · simp [World.mod, h]

@[simp] theorem mod_queue (hf : ∀ c, (f c).queue = c.queue) :
    ((w.mod ι f).copy κ).queue = (w.copy κ).queue := by
  by_cases h : κ = ι
  · subst h; simp [World.mod, hf]
  · simp [World.mod, h]

@[simp] theorem mod_stopped (hf : ∀ c, (f c).stopped = c.stopped) :
    ((w.mod ι f).copy κ).stopped = (w.copy κ).stopped := by
  by_cases h : κ = ι
  · subst h; simp [World.mod, hf]
  · simp [World.mod, h]

end ModFields

theorem Parted.inv {w : World Inst} (p : Parted w) : Inv w w where
  slot := fun j κ a h => (fun ⟨a1, a2, a3, a4, _⟩ => ⟨a1, a2, a3, a4⟩) (p.slot j κ a h)
  sent_in := fun κ a h => (p.sent_taken κ a h).2
  applied_own := p.applied_own
  applied_le := fun κ => (p.idle κ).2.2
  bytes_name := p.bytes_name
  one_owner := fun ι κ a a' h _ _ lt => by
    have := p.settled ι a h
    have := (p.idle ι).2.2
    omega
  carried_out := fun κ a h _ => p.carried_out κ a h
  settled := fun κ a h _ => p.settled κ a h
  own_applied := fun κ j a h => Or.inl (p.slot j κ a h).2.2.2.2
  sent_taken := fun κ a h => Or.inl (p.sent_taken κ a h).1
  stopped_sent := fun κ h => by simp [(p.idle κ).2.1] at h
  opened := fun κ o h => by simp [(p.idle κ).1] at h
  stopped_landed_nothing := fun κ h => by simp [(p.idle κ).2.1] at h
  one_keeper := fun j j' κ κ' a a' hj hj' h h' => by
    have := (p.slot j κ a h).2.1
    omega
  evidence := fun κ h => by simp [(p.idle κ).2.1] at h
  grows := ⟨Nat.le_refl _, fun _ => Nat.le_refl _⟩
  positive := p.positive
  filled := p.full
  old_slots := fun _ _ => rfl
  journal_grows := fun _ _ h => h

theorem commit_inv {w0 w : World Inst} (h : Inv w0 w) (ι : Inst) (x : Nat) (fresh : Fresh w x) :
    Inv w0 (w.mod ι fun c => { c with queue := c.queue ++ [x] }) := by
  refine
    { slot := ?_, sent_in := ?_, applied_own := ?_, applied_le := ?_, bytes_name := ?_,
      one_owner := ?_, carried_out := ?_, settled := ?_, own_applied := ?_, sent_taken := ?_,
      stopped_sent := ?_, opened := ?_, stopped_landed_nothing := ?_, one_keeper := ?_,
      evidence := ?_, grows := ?_, positive := ?_, filled := ?_, old_slots := ?_,
      journal_grows := ?_ } <;> simp only [mod_journal, mod_applied, mod_live, mod_sent,
        mod_stopped, mod_slots, mod_head, implies_true]
  · exact h.slot
  · exact h.sent_in
  · exact h.applied_own
  · exact h.applied_le
  · exact h.bytes_name
  · exact h.one_owner
  · intro κ a ha hl y hy
    by_cases hk : κ = ι
    · subst hk
      rw [mod_self]
      simp only [List.mem_append, List.mem_singleton, not_or]
      refine ⟨h.carried_out κ a ha hl y hy, ?_⟩
      rintro rfl
      exact (fresh.1 κ).2 a ha hy
    · rw [mod_ne w _ hk]; exact h.carried_out κ a ha hl y hy
  · exact h.settled
  · exact h.own_applied
  · exact h.sent_taken
  · exact h.stopped_sent
  · intro κ o hl
    obtain ⟨a1, a2, a3⟩ := h.opened κ o hl
    refine ⟨a1, a2, fun y hy => ?_⟩
    by_cases hk : κ = ι
    · subst hk; rw [mod_self]; simp [a3 y hy]
    · rw [mod_ne w _ hk]; exact a3 y hy
  · exact h.stopped_landed_nothing
  · exact h.one_keeper
  · exact h.evidence
  · exact h.grows
  · exact h.positive
  · exact h.filled
  · exact h.old_slots
  · exact h.journal_grows

theorem begin_mem (w : World Inst) (ι κ : Inst) (a b : Attempt) :
    b ∈ ((w.mod ι fun c => { c with journal := a :: c.journal, live := some a }).copy κ).journal ↔
      (κ = ι ∧ b = a) ∨ b ∈ (w.copy κ).journal := by
  by_cases hk : κ = ι
  · subst hk; rw [mod_self]; simp
  · rw [mod_ne w _ hk]; simp [hk]

theorem begin_inv {w0 w : World Inst} (h : Inv w0 w) (ι : Inst) (a : Attempt)
    (go : (w.copy ι).stopped = false) (idle : (w.copy ι).live = none)
    (slot : a.slot = (w.copy ι).applied + 1)
    (fresh : ∀ κ b, b ∈ (w.copy κ).journal → b.bytes ≠ a.bytes)
    (carried : ∀ x ∈ a.writes, x ∈ (w.copy ι).queue) :
    Inv w0 (w.mod ι fun c => { c with journal := a :: c.journal, live := some a }) := by
  refine
    { slot := ?_, sent_in := ?_, applied_own := ?_, applied_le := ?_, bytes_name := ?_,
      one_owner := ?_, carried_out := ?_, settled := ?_, own_applied := ?_, sent_taken := ?_,
      stopped_sent := ?_, opened := ?_, stopped_landed_nothing := ?_, one_keeper := ?_,
      evidence := ?_, grows := ?_, positive := ?_, filled := ?_, old_slots := ?_,
      journal_grows := ?_ } <;> try simp only [mod_applied, mod_sent,
        mod_stopped, mod_queue, mod_slots, mod_head, implies_true]
  · intro j κ b hs
    obtain ⟨a1, a2, a3, a4⟩ := h.slot j κ b hs
    exact ⟨a1, a2, a3, (begin_mem w ι κ a b).2 (Or.inr a4)⟩
  · intro κ b hb
    exact (begin_mem w ι κ a b).2 (Or.inr (h.sent_in κ b hb))
  · intro κ j h1 h2
    obtain ⟨κ', b, hs, hb⟩ := h.applied_own κ j h1 h2
    exact ⟨κ', b, hs, (begin_mem w ι κ a b).2 (Or.inr hb)⟩
  · exact h.applied_le
  · intro κ1 κ2 b b' hb hb' he
    rcases (begin_mem w ι κ1 a b).1 hb with ⟨_, hb0⟩ | hb <;>
      rcases (begin_mem w ι κ2 a b').1 hb' with ⟨_, hb0'⟩ | hb'
    · rw [hb0, hb0']
    · exact absurd (he.symm.trans (by rw [hb0])) (fresh κ2 b' hb')
    · exact absurd (he.trans (by rw [hb0'])) (fresh κ1 b hb)
    · exact h.bytes_name κ1 κ2 b b' hb hb' he
  · intro κ1 κ2 b b' hb hb' he lt
    rcases (begin_mem w ι κ1 a b).1 hb with ⟨hk1, hb0⟩ | hb <;>
      rcases (begin_mem w ι κ2 a b').1 hb' with ⟨hk2, hb0'⟩ | hb'
    · rw [hk1, hk2]
    · exact absurd (he.symm.trans (by rw [hb0])) (fresh κ2 b' hb')
    · exact absurd (he.trans (by rw [hb0'])) (fresh κ1 b hb)
    · exact h.one_owner κ1 κ2 b b' hb hb' he lt
  · intro κ b hb hl y hy
    by_cases hk : κ = ι
    · subst hk
      rcases (begin_mem w κ κ a b).1 hb with ⟨_, hb0⟩ | hb
      · exact absurd (by rw [mod_self]; simp [hb0]) hl
      · exact h.carried_out κ b hb (by rw [idle]; simp) y hy
    · rw [mod_ne w _ hk] at hb hl
      exact h.carried_out κ b hb hl y hy
  · intro κ b hb hl
    by_cases hk : κ = ι
    · subst hk
      rcases (begin_mem w κ κ a b).1 hb with ⟨_, hb0⟩ | hb
      · exact absurd (by rw [mod_self]; simp [hb0]) hl
      · exact h.settled κ b hb (by rw [idle]; simp)
    · rw [mod_ne w _ hk] at hb hl
      exact h.settled κ b hb hl
  · intro κ j b hs
    by_cases hk : κ = ι
    · subst hk
      rcases h.own_applied κ j b hs with h1 | h1
      · exact Or.inl h1
      · rw [idle] at h1; simp at h1
    · rw [mod_ne w _ hk]; exact h.own_applied κ j b hs
  · intro κ b hb
    by_cases hk : κ = ι
    · subst hk
      rcases h.sent_taken κ b hb with h1 | h1
      · exact Or.inl h1
      · rw [idle] at h1; simp at h1
    · rw [mod_ne w _ hk]; exact h.sent_taken κ b hb
  · exact h.stopped_sent
  · intro κ o hl
    by_cases hk : κ = ι
    · subst hk
      rw [mod_self] at hl
      simp only [Option.some.injEq] at hl
      subst hl
      exact ⟨(begin_mem w κ κ a a).2 (Or.inl ⟨rfl, rfl⟩), slot, carried⟩
    · rw [mod_ne w _ hk] at hl
      obtain ⟨b1, b2, b3⟩ := h.opened κ o hl
      rw [mod_ne w _ hk]
      exact ⟨b1, b2, b3⟩
  · exact h.stopped_landed_nothing
  · exact h.one_keeper
  · intro κ hs
    have hk : κ ≠ ι := fun e => by subst e; rw [go] at hs; simp at hs
    rw [mod_ne w _ hk]
    exact h.evidence κ hs
  · exact h.grows
  · intro κ b hb
    rcases (begin_mem w ι κ a b).1 hb with ⟨_, hb0⟩ | hb
    · rw [hb0]; omega
    · exact h.positive κ b hb
  · exact h.filled
  · exact h.old_slots
  · intro κ b hb
    exact (begin_mem w ι κ a b).2 (Or.inr (h.journal_grows κ b hb))

theorem send_mem (w : World Inst) (ι κ : Inst) (a b : Attempt) :
    b ∈ ((w.mod ι fun c => { c with sent := a :: c.sent }).copy κ).sent ↔
      (κ = ι ∧ b = a) ∨ b ∈ (w.copy κ).sent := by
  by_cases hk : κ = ι
  · subst hk; rw [mod_self]; simp
  · rw [mod_ne w _ hk]; simp [hk]

theorem send_inv {w0 w : World Inst} (h : Inv w0 w) (ι : Inst) (a : Attempt)
    (go : (w.copy ι).stopped = false) (hl : (w.copy ι).live = some a) :
    Inv w0 (w.mod ι fun c => { c with sent := a :: c.sent }) := by
  refine
    { slot := ?_, sent_in := ?_, applied_own := ?_, applied_le := ?_, bytes_name := ?_,
      one_owner := ?_, carried_out := ?_, settled := ?_, own_applied := ?_, sent_taken := ?_,
      stopped_sent := ?_, opened := ?_, stopped_landed_nothing := ?_, one_keeper := ?_,
      evidence := ?_, grows := ?_, positive := ?_, filled := ?_, old_slots := ?_,
      journal_grows := ?_ } <;> try simp only [mod_journal, mod_applied, mod_live,
        mod_stopped, mod_queue, mod_slots, mod_head, implies_true]
  · exact h.slot
  · intro κ b hb
    rcases (send_mem w ι κ a b).1 hb with ⟨hk, hb0⟩ | hb
    · subst hk; rw [hb0]; exact (h.opened κ a hl).1
    · exact h.sent_in κ b hb
  · exact h.applied_own
  · exact h.applied_le
  · exact h.bytes_name
  · exact h.one_owner
  · exact h.carried_out
  · exact h.settled
  · exact h.own_applied
  · intro κ b hb
    rcases (send_mem w ι κ a b).1 hb with ⟨hk, hb0⟩ | hb
    · subst hk; right; rw [hl, hb0]
    · exact h.sent_taken κ b hb
  · intro κ hs b hb
    have hk : κ ≠ ι := fun e => by subst e; rw [go] at hs; simp at hs
    rw [mod_ne w _ hk] at hb
    exact h.stopped_sent κ hs b hb
  · exact h.opened
  · exact h.stopped_landed_nothing
  · exact h.one_keeper
  · exact h.evidence
  · exact h.grows
  · exact h.positive
  · exact h.filled
  · exact h.old_slots
  · exact h.journal_grows

omit [DecidableEq Inst] in
theorem land_inv {w0 w : World Inst} (h : Inv w0 w) (ι : Inst) (a : Attempt)
    (hs : a ∈ (w.copy ι).sent) (next : a.slot = w.head + 1) :
    Inv w0 { w with slots := fun j => if j = w.head + 1 then some (ι, a) else w.slots j,
                    head := w.head + 1 } := by
  have hl : (w.copy ι).live = some a := by
    rcases h.sent_taken ι a hs with h1 | h1
    · omega
    · exact h1
  obtain ⟨_, o1, _⟩ := h.opened ι a hl
  have hap : (w.copy ι).applied = w.head := by omega
  -- an old slot is unchanged
  have keep : ∀ j, j ≤ w.head → (if j = w.head + 1 then some (ι, a) else w.slots j) = w.slots j := by
    intro j hj
    simp [show j ≠ w.head + 1 by omega]
  refine
    { slot := ?_, sent_in := ?_, applied_own := ?_, applied_le := ?_, bytes_name := ?_,
      one_owner := ?_, carried_out := ?_, settled := ?_, own_applied := ?_, sent_taken := ?_,
      stopped_sent := ?_, opened := ?_, stopped_landed_nothing := ?_, one_keeper := ?_,
      evidence := ?_, grows := ?_, positive := ?_, filled := ?_, old_slots := ?_,
      journal_grows := ?_ } <;> dsimp only
  · intro j κ b hj
    by_cases hjh : j = w.head + 1
    · simp only [hjh, ite_true, Option.some.injEq, Prod.mk.injEq] at hj
      obtain ⟨rfl, rfl⟩ := hj
      exact ⟨by omega, by omega, by omega, h.sent_in ι a hs⟩
    · simp only [hjh, ite_false] at hj
      obtain ⟨a1, a2, a3, a4⟩ := h.slot j κ b hj
      exact ⟨a1, by omega, a3, a4⟩
  · exact h.sent_in
  · intro κ j h1 h2
    have := h.applied_le κ
    rw [keep j (by omega)]
    exact h.applied_own κ j h1 h2
  · intro κ; have := h.applied_le κ; omega
  · exact h.bytes_name
  · exact h.one_owner
  · exact h.carried_out
  · exact h.settled
  · intro κ j b hj
    by_cases hjh : j = w.head + 1
    · simp only [hjh, ite_true, Option.some.injEq, Prod.mk.injEq] at hj
      obtain ⟨rfl, rfl⟩ := hj
      exact Or.inr hl
    · simp only [hjh, ite_false] at hj
      exact h.own_applied κ j b hj
  · intro κ b hb
    rcases h.sent_taken κ b hb with h1 | h1
    · exact Or.inl (by omega)
    · exact Or.inr h1
  · intro κ hst b hb
    have := h.stopped_sent κ hst b hb
    omega
  · exact h.opened
  · intro κ hst j b hj
    by_cases hjh : j = w.head + 1
    · simp only [hjh, ite_true, Option.some.injEq, Prod.mk.injEq] at hj
      obtain ⟨rfl, rfl⟩ := hj
      have := h.stopped_sent ι hst a hs
      omega
    · simp only [hjh, ite_false] at hj
      exact h.stopped_landed_nothing κ hst j b hj
  · -- the copy that creates it has applied every earlier slot, so every earlier
    -- slot after the parting is its own
    have old : ∀ j κ' b', w0.head < j → j ≤ w.head → w.slots j = some (κ', b') → κ' = ι := by
      intro j κ' b' hj hjh hs'
      obtain ⟨a1, a2, a3, a4⟩ := h.slot j κ' b' hs'
      obtain ⟨κ'', b'', hs'', hb''⟩ := h.applied_own ι j a1 (by omega)
      rw [hs'] at hs''
      simp only [Option.some.injEq, Prod.mk.injEq] at hs''
      obtain ⟨rfl, rfl⟩ := hs''
      exact (h.one_owner ι κ' b' b' hb'' a4 rfl (by omega)).symm
    intro j j' κ κ' b b' hj hj' hs1 hs2
    by_cases h1 : j = w.head + 1 <;> by_cases h2 : j' = w.head + 1
    · simp only [h1, ite_true, Option.some.injEq, Prod.mk.injEq] at hs1
      simp only [h2, ite_true, Option.some.injEq, Prod.mk.injEq] at hs2
      rw [← hs1.1, ← hs2.1]
    · simp only [h1, ite_true, Option.some.injEq, Prod.mk.injEq] at hs1
      simp only [h2, ite_false] at hs2
      have := h.slot j' κ' b' hs2
      rw [← hs1.1]
      exact (old j' κ' b' hj' (by omega) hs2).symm
    · simp only [h2, ite_true, Option.some.injEq, Prod.mk.injEq] at hs2
      simp only [h1, ite_false] at hs1
      have := h.slot j κ b hs1
      rw [← hs2.1]
      exact old j κ b hj (by omega) hs1
    · simp only [h1, ite_false] at hs1
      simp only [h2, ite_false] at hs2
      exact h.one_keeper j j' κ κ' b b' hj hj' hs1 hs2
  · intro κ hst
    obtain ⟨j, κ', b, hj, hb⟩ := h.evidence κ hst
    obtain ⟨_, a2, _, _⟩ := h.slot j κ' b hj
    exact ⟨j, κ', b, by rw [keep j a2]; exact hj, hb⟩
  · exact ⟨by have := h.grows.1; omega, h.grows.2⟩
  · exact h.positive
  · intro j h1 h2
    by_cases hjh : j = w.head + 1
    · exact ⟨(ι, a), by simp [hjh]⟩
    · rw [keep j (by omega)]; exact h.filled j h1 (by omega)
  · intro j hj
    have := h.grows.1
    rw [keep j (by omega)]; exact h.old_slots j hj
  · exact h.journal_grows

theorem read_inv {w0 w : World Inst} (h : Inv w0 w) (ι κ : Inst) (a : Attempt)
    (idle : (w.copy ι).live = none) (lt : (w.copy ι).applied < w.head)
    (at_ : w.slots ((w.copy ι).applied + 1) = some (κ, a)) (mine : a ∈ (w.copy ι).journal) :
    Inv w0 (w.mod ι fun c => { c with applied := c.applied + 1 }) := by
  refine
    { slot := ?_, sent_in := ?_, applied_own := ?_, applied_le := ?_, bytes_name := ?_,
      one_owner := ?_, carried_out := ?_, settled := ?_, own_applied := ?_, sent_taken := ?_,
      stopped_sent := ?_, opened := ?_, stopped_landed_nothing := ?_, one_keeper := ?_,
      evidence := ?_, grows := ?_, positive := ?_, filled := ?_, old_slots := ?_,
      journal_grows := ?_ } <;> try simp only [mod_journal, mod_live, mod_sent,
        mod_stopped, mod_queue, mod_slots, mod_head, implies_true]
  · exact h.slot
  · exact h.sent_in
  · intro κ' j h1 h2
    by_cases hk : κ' = ι
    · subst hk
      rw [mod_self] at h2
      by_cases hj : j = (w.copy κ').applied + 1
      · subst hj; exact ⟨κ, a, at_, mine⟩
      · exact h.applied_own κ' j h1 (by simp at h2; omega)
    · rw [mod_ne w _ hk] at h2; exact h.applied_own κ' j h1 h2
  · intro κ'
    by_cases hk : κ' = ι
    · subst hk; rw [mod_self]; simp; omega
    · rw [mod_ne w _ hk]; exact h.applied_le κ'
  · exact h.bytes_name
  · exact h.one_owner
  · exact h.carried_out
  · intro κ' b hb hl
    by_cases hk : κ' = ι
    · subst hk
      have := h.settled κ' b hb hl
      rw [mod_self]; simp; omega
    · rw [mod_ne w _ hk]; exact h.settled κ' b hb hl
  · intro κ' j b hj
    by_cases hk : κ' = ι
    · subst hk
      rw [mod_self]
      rcases h.own_applied κ' j b hj with h1 | h1
      · left; simp; omega
      · right; simpa using h1
    · rw [mod_ne w _ hk]; exact h.own_applied κ' j b hj
  · exact h.sent_taken
  · exact h.stopped_sent
  · intro κ' o hl
    by_cases hk : κ' = ι
    · subst hk
      rw [idle] at hl; simp at hl
    · obtain ⟨b1, b2, b3⟩ := h.opened κ' o hl
      rw [mod_ne w _ hk]
      exact ⟨b1, b2, b3⟩
  · exact h.stopped_landed_nothing
  · exact h.one_keeper
  · exact h.evidence
  · refine ⟨h.grows.1, fun κ' => ?_⟩
    have := h.grows.2 κ'
    by_cases hk : κ' = ι
    · subst hk; rw [mod_self]; simp; omega
    · rw [mod_ne w _ hk]; exact this
  · exact h.positive
  · exact h.filled
  · exact h.old_slots
  · exact h.journal_grows

theorem detect_read_inv {w0 w : World Inst} (h : Inv w0 w) (ι κ : Inst) (a : Attempt)
    (idle : (w.copy ι).live = none) (lt : (w.copy ι).applied < w.head)
    (at_ : w.slots ((w.copy ι).applied + 1) = some (κ, a))
    (foreign : a ∉ (w.copy ι).journal) :
    Inv w0 (w.mod ι fun c => { c with stopped := true }) := by
  refine
    { slot := ?_, sent_in := ?_, applied_own := ?_, applied_le := ?_, bytes_name := ?_,
      one_owner := ?_, carried_out := ?_, settled := ?_, own_applied := ?_, sent_taken := ?_,
      stopped_sent := ?_, opened := ?_, stopped_landed_nothing := ?_, one_keeper := ?_,
      evidence := ?_, grows := ?_, positive := ?_, filled := ?_, old_slots := ?_,
      journal_grows := ?_ } <;> try simp only [mod_journal, mod_applied, mod_live,
        mod_sent, mod_queue, mod_slots, mod_head, implies_true]
  · exact h.slot
  · exact h.sent_in
  · exact h.applied_own
  · exact h.applied_le
  · exact h.bytes_name
  · exact h.one_owner
  · exact h.carried_out
  · exact h.settled
  · exact h.own_applied
  · exact h.sent_taken
  · intro κ' hst b hb
    by_cases hk : κ' = ι
    · subst hk
      rcases h.sent_taken κ' b hb with h1 | h1
      · exact h1
      · rw [idle] at h1; simp at h1
    · rw [mod_ne w _ hk] at hst
      exact h.stopped_sent κ' hst b hb
  · exact h.opened
  · intro κ' hst j b hj
    by_cases hk : κ' = ι
    · subst hk
      by_cases hpost : w0.head < j
      · exfalso
        have hown := h.own_applied κ' j b hj
        have hj1 := (h.slot j κ' b hj).1
        have hle : j ≤ (w.copy κ').applied := by
          rcases hown with h1 | h1
          · exact h1
          · rw [idle] at h1; simp at h1
        have := h.one_keeper j ((w.copy κ').applied + 1) κ' κ b a hpost (by omega) hj at_
        subst this
        exact foreign (h.slot _ κ' a at_).2.2.2
      · omega
    · rw [mod_ne w _ hk] at hst
      exact h.stopped_landed_nothing κ' hst j b hj
  · exact h.one_keeper
  · intro κ' hst
    by_cases hk : κ' = ι
    · subst hk
      exact ⟨_, κ, a, at_, foreign⟩
    · rw [mod_ne w _ hk] at hst
      exact h.evidence κ' hst
  · exact h.grows
  · exact h.positive
  · exact h.filled
  · exact h.old_slots
  · exact h.journal_grows

theorem settle_landed_inv {w0 w : World Inst} (h : Inv w0 w) (ι κ : Inst) (a o : Attempt)
    (hl : (w.copy ι).live = some o) (filled : o.slot ≤ w.head)
    (at_ : w.slots o.slot = some (κ, a)) (same : a.bytes = o.bytes) :
    Inv w0 (w.mod ι fun c => { c with applied := o.slot, live := none,
                                      queue := c.queue.filter fun x => decide (x ∉ o.writes) }) := by
  obtain ⟨ho, hos, hoq⟩ := h.opened ι o hl
  obtain ⟨_, _, _, ha⟩ := h.slot o.slot κ a at_
  have hao : a = o := h.bytes_name κ ι a o ha ho same
  subst hao
  refine
    { slot := ?_, sent_in := ?_, applied_own := ?_, applied_le := ?_, bytes_name := ?_,
      one_owner := ?_, carried_out := ?_, settled := ?_, own_applied := ?_, sent_taken := ?_,
      stopped_sent := ?_, opened := ?_, stopped_landed_nothing := ?_, one_keeper := ?_,
      evidence := ?_, grows := ?_, positive := ?_, filled := ?_, old_slots := ?_,
      journal_grows := ?_ } <;> try simp only [mod_journal, mod_sent, mod_stopped,
        mod_slots, mod_head, implies_true]
  · exact h.slot
  · exact h.sent_in
  · intro κ' j h1 h2
    by_cases hk : κ' = ι
    · subst hk
      rw [mod_self] at h2
      by_cases hj : j = a.slot
      · subst hj; exact ⟨κ, a, at_, ho⟩
      · exact h.applied_own κ' j h1 (by simp at h2; omega)
    · rw [mod_ne w _ hk] at h2; exact h.applied_own κ' j h1 h2
  · intro κ'
    by_cases hk : κ' = ι
    · subst hk; rw [mod_self]; simpa using filled
    · rw [mod_ne w _ hk]; exact h.applied_le κ'
  · exact h.bytes_name
  · exact h.one_owner
  · intro κ' b hb hlb y hy
    by_cases hk : κ' = ι
    · subst hk
      rw [mod_self]
      simp only [List.mem_filter, decide_eq_true_eq, not_and]
      intro hyq hyo
      by_cases hba : b = a
      · subst hba; exact hyo hy
      · exact h.carried_out κ' b hb (fun e => hba (Option.some.inj (hl.symm.trans e)).symm) y hy hyq
    · rw [mod_ne w _ hk] at hlb ⊢
      exact h.carried_out κ' b hb hlb y hy
  · intro κ' b hb hlb
    by_cases hk : κ' = ι
    · subst hk
      rw [mod_self]
      by_cases hba : b = a
      · subst hba; simp
      · have := h.settled κ' b hb (fun e => hba (Option.some.inj (hl.symm.trans e)).symm)
        simp; omega
    · rw [mod_ne w _ hk] at hlb ⊢; exact h.settled κ' b hb hlb
  · intro κ' j b hj
    by_cases hk : κ' = ι
    · subst hk
      rcases h.own_applied κ' j b hj with h1 | h1
      · left; rw [mod_self]; simp; omega
      · have hba : b = a := (Option.some.inj (hl.symm.trans h1)).symm
        subst hba
        left; rw [mod_self]; simp
        have := (h.slot j κ' b hj).2.2.1
        omega
    · rw [mod_ne w _ hk]; exact h.own_applied κ' j b hj
  · intro κ' b hb
    by_cases hk : κ' = ι
    · subst hk
      rcases h.sent_taken κ' b hb with h1 | h1
      · exact Or.inl h1
      · have hba : b = a := (Option.some.inj (hl.symm.trans h1)).symm
        subst hba; exact Or.inl filled
    · rw [mod_ne w _ hk]; exact h.sent_taken κ' b hb
  · exact h.stopped_sent
  · intro κ' o' hlo
    by_cases hk : κ' = ι
    · subst hk; rw [mod_self] at hlo; simp at hlo
    · rw [mod_ne w _ hk] at hlo
      obtain ⟨b1, b2, b3⟩ := h.opened κ' o' hlo
      rw [mod_ne w _ hk]
      exact ⟨b1, b2, b3⟩
  · exact h.stopped_landed_nothing
  · exact h.one_keeper
  · exact h.evidence
  · refine ⟨h.grows.1, fun κ' => ?_⟩
    have := h.grows.2 κ'
    by_cases hk : κ' = ι
    · subst hk; rw [mod_self]; simp; omega
    · rw [mod_ne w _ hk]; exact this
  · exact h.positive
  · exact h.filled
  · exact h.old_slots
  · exact h.journal_grows

theorem detect_settle_inv {w0 w : World Inst} (h : Inv w0 w) (ι κ : Inst) (a o : Attempt)
    (hl : (w.copy ι).live = some o) (filled : o.slot ≤ w.head)
    (at_ : w.slots o.slot = some (κ, a)) (diff : a.bytes ≠ o.bytes) :
    Inv w0 (w.mod ι fun c => { c with stopped := true }) := by
  obtain ⟨ho, hos, hoq⟩ := h.opened ι o hl
  obtain ⟨_, _, hsl, ha⟩ := h.slot o.slot κ a at_
  have foreign : a ∉ (w.copy ι).journal := by
    intro hin
    have hne : a ≠ o := fun e => diff (by rw [e])
    have := h.settled ι a hin (fun e => hne (Option.some.inj (hl.symm.trans e)).symm)
    omega
  refine
    { slot := ?_, sent_in := ?_, applied_own := ?_, applied_le := ?_, bytes_name := ?_,
      one_owner := ?_, carried_out := ?_, settled := ?_, own_applied := ?_, sent_taken := ?_,
      stopped_sent := ?_, opened := ?_, stopped_landed_nothing := ?_, one_keeper := ?_,
      evidence := ?_, grows := ?_, positive := ?_, filled := ?_, old_slots := ?_,
      journal_grows := ?_ } <;> try simp only [mod_journal, mod_applied, mod_live,
        mod_sent, mod_queue, mod_slots, mod_head, implies_true]
  · exact h.slot
  · exact h.sent_in
  · exact h.applied_own
  · exact h.applied_le
  · exact h.bytes_name
  · exact h.one_owner
  · exact h.carried_out
  · exact h.settled
  · exact h.own_applied
  · exact h.sent_taken
  · intro κ' hst b hb
    by_cases hk : κ' = ι
    · subst hk
      rcases h.sent_taken κ' b hb with h1 | h1
      · exact h1
      · have hba : b = o := (Option.some.inj (hl.symm.trans h1)).symm
        subst hba; exact filled
    · rw [mod_ne w _ hk] at hst
      exact h.stopped_sent κ' hst b hb
  · exact h.opened
  · intro κ' hst j b hj
    by_cases hk : κ' = ι
    · subst hk
      by_cases hpost : w0.head < j
      · exfalso
        rcases h.own_applied κ' j b hj with h1 | h1
        · have := h.one_keeper j o.slot κ' κ b a hpost (by omega) hj at_
          subst this
          exact foreign ha
        · have hbo : b = o := (Option.some.inj (hl.symm.trans h1)).symm
          subst hbo
          have hj' : j = b.slot := (h.slot j κ' b hj).2.2.1.symm
          rw [hj'] at hj
          rw [hj] at at_
          simp only [Option.some.injEq, Prod.mk.injEq] at at_
          exact diff (by rw [at_.2])
      · omega
    · rw [mod_ne w _ hk] at hst
      exact h.stopped_landed_nothing κ' hst j b hj
  · exact h.one_keeper
  · intro κ' hst
    by_cases hk : κ' = ι
    · subst hk
      exact ⟨_, κ, a, at_, foreign⟩
    · rw [mod_ne w _ hk] at hst
      exact h.evidence κ' hst
  · exact h.grows
  · exact h.positive
  · exact h.filled
  · exact h.old_slots
  · exact h.journal_grows

theorem step_keeps_inv {w0 w w' : World Inst} (s : Step w w') (h : Inv w0 w) : Inv w0 w' := by
  cases s with
  | commit ι x fresh => exact commit_inv h ι x fresh
  | begin ι a go idle slot fresh carried => exact begin_inv h ι a go idle slot fresh carried
  | send ι a go hl => exact send_inv h ι a go hl
  | land ι a hs next => exact land_inv h ι a hs next
  | readOwn ι κ a _ idle lt at_ mine => exact read_inv h ι κ a idle lt at_ mine
  | detectRead ι κ a _ idle lt at_ foreign => exact detect_read_inv h ι κ a idle lt at_ foreign
  | settleLanded ι κ a o _ hl filled at_ same => exact settle_landed_inv h ι κ a o hl filled at_ same
  | detectSettle ι κ a o _ hl filled at_ diff => exact detect_settle_inv h ι κ a o hl filled at_ diff

theorem steps_keep_inv {w0 w w' : World Inst} (run : Steps w w') (h : Inv w0 w) : Inv w0 w' := by
  induction run with
  | refl => exact h
  | step s _ ih => exact ih (step_keeps_inv s h)

/-- Every state the copies reach from their parting keeps `Inv`. -/
theorem reached_keeps_inv {w0 w : World Inst} (p : Parted w0) (run : Steps w0 w) : Inv w0 w :=
  steps_keep_inv run p.inv

/-! ## What the copy that finds the other has done -/

variable {w0 w : World Inst}

/-- **At most one copy lands slots under the id.** Of the slots created after
the copies part, one copy created every one. -/
theorem one_keeper (p : Parted w0) (run : Steps w0 w) {j j' : Nat} {κ κ' : Inst} {a a' : Attempt}
    (hj : w0.head < j) (hj' : w0.head < j')
    (h : w.slots j = some (κ, a)) (h' : w.slots j' = some (κ', a')) : κ = κ' :=
  (reached_keeps_inv p run).one_keeper j j' κ κ' a a' hj hj' h h'

/-- **Every other copy finds the keeper's slot before it can land one.** A copy
never applies a slot another copy created after the parting: applying needs the
slot's attempt in its own journal, and an attempt after the parting is in one
journal. -/
theorem foreign_slot_never_applied (p : Parted w0) (run : Steps w0 w) {κ κ' : Inst} {j : Nat}
    {a : Attempt} (h : w.slots j = some (κ', a)) (post : w0.head < j) (other : κ' ≠ κ) :
    (w.copy κ).applied < j := by
  have inv := reached_keeps_inv p run
  rcases Nat.lt_or_ge (w.copy κ).applied j with hlt | hle
  · exact hlt
  exfalso
  obtain ⟨κ'', b, hs, hb⟩ := inv.applied_own κ j (by have := inv.slot j κ' a h; omega)
    (by omega)
  rw [h] at hs
  simp only [Option.some.injEq, Prod.mk.injEq] at hs
  obtain ⟨-, rfl⟩ := hs
  obtain ⟨_, _, hsl, ha⟩ := inv.slot j κ' a h
  exact other (inv.one_owner κ' κ a a ha hb rfl (by omega))

/-- **A copy that finds the evidence has landed no slot since the copies parted.**
Every slot it created is from before. No attempt it recorded after the parting
is in any slot, so every write it numbered since is still in its queue. -/
theorem detector_landed_nothing (p : Parted w0) (run : Steps w0 w) {κ : Inst}
    (stopped : (w.copy κ).stopped = true) :
    (∀ j a, w.slots j = some (κ, a) → j ≤ w0.head) ∧
    ∀ a ∈ (w.copy κ).journal, w0.head < a.slot →
      ∀ j κ' a', w.slots j = some (κ', a') → a'.bytes ≠ a.bytes := by
  have inv := reached_keeps_inv p run
  refine ⟨inv.stopped_landed_nothing κ stopped, fun a ha post j κ' a' hs he => ?_⟩
  obtain ⟨_, _, hsl, ha'⟩ := inv.slot j κ' a' hs
  have haa : a' = a := inv.bytes_name κ' κ a' a ha' ha he
  subst haa
  have hκ := inv.one_owner κ' κ a' a' ha' ha rfl post
  subst hκ
  have := inv.stopped_landed_nothing κ' stopped j a' hs
  omega

omit [DecidableEq Inst] in
/-- The writes of the slots through `n` are the writes of those slots. -/
theorem mem_histOf {x n : Nat} (hx : x ∈ histOf w n) :
    ∃ j κ a, 1 ≤ j ∧ j ≤ n ∧ w.slots j = some (κ, a) ∧ x ∈ a.writes := by
  induction n with
  | zero => simp [histOf] at hx
  | succ n ih =>
    simp only [histOf, List.mem_append] at hx
    rcases hx with hx | hx
    · obtain ⟨j, κ, a, h1, h2, h3, h4⟩ := ih hx
      exact ⟨j, κ, a, h1, by omega, h3, h4⟩
    · cases hs : w.slots (n + 1) with
      | none => simp [hs] at hx
      | some y =>
        obtain ⟨κ, a⟩ := y
        simp [hs] at hx
        exact ⟨n + 1, κ, a, by omega, by omega, hs, hx⟩

/-- **When a copy finds the evidence, its applied history holds none of its
queued writes** (true of every copy at every moment). A write leaves the queue
only with a claim that landed and was applied, and an applied slot is one of the
copy's own attempts, whose writes already left. -/
theorem detector_queue_unapplied (p : Parted w0) (run : Steps w0 w) (κ : Inst) :
    ∀ x ∈ (w.copy κ).queue, x ∉ histOf w (w.copy κ).applied := by
  have inv := reached_keeps_inv p run
  intro x hq hx
  obtain ⟨j, κ', a, h1, h2, hs, hw⟩ := mem_histOf hx
  obtain ⟨κ'', b, hs', hb⟩ := inv.applied_own κ j h1 h2
  rw [hs] at hs'
  simp only [Option.some.injEq, Prod.mk.injEq] at hs'
  obtain ⟨-, rfl⟩ := hs'
  have hslot := (inv.slot j κ' a hs).2.2.1
  refine inv.carried_out κ a hb (fun hl => ?_) x hw hq
  have := (inv.opened κ a hl).2.1
  omega

/-- **Every create a copy sent that finds the evidence targets a slot already
taken, so none can still land.** A slot lands only in the empty slot after the
head. -/
theorem detector_sent_all_occupied (p : Parted w0) (run : Steps w0 w) {κ : Inst}
    (stopped : (w.copy κ).stopped = true) :
    ∀ a ∈ (w.copy κ).sent, a.slot ≤ w.head ∧ ∃ x, w.slots a.slot = some x := by
  have inv := reached_keeps_inv p run
  intro a ha
  have hle := inv.stopped_sent κ stopped a ha
  exact ⟨hle, inv.filled a.slot (inv.positive κ a (inv.sent_in κ a ha)) hle⟩

/-- **The keeper never finds another copy once the finder sends nothing more.**
A copy whose journal holds every slot from before the parting, with every other
copy stopped, is never stopped: the stopped copies created no slot after the
parting, and every slot from before is one it recorded. -/
theorem keeper_never_detects (p : Parted w0) (run : Steps w0 w) {κ0 : Inst}
    (knows : ∀ j κ a, w0.slots j = some (κ, a) → a ∈ (w0.copy κ0).journal)
    (others : ∀ κ, κ ≠ κ0 → (w.copy κ).stopped = true) :
    (w.copy κ0).stopped = false := by
  have inv := reached_keeps_inv p run
  cases hst : (w.copy κ0).stopped
  · rfl
  · exfalso
    obtain ⟨j, κ', a, hs, hnot⟩ := inv.evidence κ0 hst
    by_cases old : j ≤ w0.head
    · rw [inv.old_slots j old] at hs
      exact hnot (inv.journal_grows κ0 a (knows j κ' a hs))
    · have hκ : κ' ≠ κ0 := fun e => by
        subst e; exact hnot (inv.slot j κ' a hs).2.2.2
      have := inv.stopped_landed_nothing κ' (others κ' hκ) j a hs
      omega

/-! ## The fresh id's registration -/

section Registration
variable {S : Sig} (R : Rules S)

/-- **The rejoin registers its fresh id.** A copy that takes the fresh id `d`
for its member `m` writes `addDevice d m none` as a self-registration. It is
valid whenever no applied history names `d`, `m` is still a member and `m`'s
current keys, which this copy holds, authorize it; applying it binds `d` to
`m`. A member removed or whose keys were replaced before the copy parted from
the keeper is refused by these same conditions and stops the copy as today. -/
theorem rejoin_registers (L : Ledger S) (k : Nat) (d : S.Dev) (m : S.Mem)
    (fresh : L.deviceOf d = none) (member : L.member m = true)
    (authorized : R.authorized L d (.addDevice d m none) = true) :
    ledgerValid R L (.admin d (.addDevice d m none)) = true ∧
    (ledgerStep R L k (.admin d (.addDevice d m none))).deviceOf d = some m :=
  ⟨self_registration_valid R L d m fresh member authorized,
    self_registration_applied R L k d m fresh member authorized⟩

/-- A registration for a member no longer in the store is refused. -/
theorem rejoin_refused_for_removed_member (L : Ledger S) (d : S.Dev) (m : S.Mem)
    (gone : L.member m = false) :
    ledgerValid R L (.admin d (.addDevice d m none)) = false := by
  simp [ledgerValid, structural, gone]

end Registration

/-! ## A restored backup finds the keeper's slot

The keeper (`true`) applied slot 1 and kept going; the restored copy (`false`)
was taken before it recorded or applied slot 1. -/

section Example

def slot1 : Attempt := ⟨1, 1, []⟩

def parting : World Bool where
  slots := fun j => if j = 1 then some (true, slot1) else none
  head := 1
  copy := fun κ =>
    if κ then ⟨1, [slot1], none, [slot1], [], false⟩ else ⟨0, [], none, [], [], false⟩

theorem parting_parted : Parted parting where
  slot := by
    intro j κ a h
    by_cases hj : j = 1
    · subst hj
      simp only [parting, ite_true, Option.some.injEq, Prod.mk.injEq] at h
      obtain ⟨rfl, rfl⟩ := h
      simp [parting, slot1]
    · simp [parting, hj] at h
  idle := by intro κ; cases κ <;> simp [parting]
  sent_taken := by
    intro κ a h
    cases κ <;> simp [parting, slot1] at h ⊢ <;> simp [h]
  settled := by
    intro κ a h
    cases κ <;> simp [parting, slot1] at h ⊢ <;> simp [h]
  applied_own := by
    intro κ j h1 h2
    cases κ
    · simp [parting] at h2; omega
    · have : j = 1 := by simp [parting] at h2; omega
      subst this; exact ⟨true, slot1, by simp [parting], by simp [parting]⟩
  carried_out := by
    intro κ a h x hx
    cases κ <;> simp [parting, slot1] at h ⊢ <;> simp [h] at hx
  bytes_name := by
    intro ι κ a a' h h' he
    cases ι <;> cases κ <;> simp [parting, slot1] at h h' <;> simp [h, h']
  positive := by
    intro κ a h
    cases κ <;> simp [parting, slot1] at h ⊢ <;> simp [h]
  full := by
    intro j h1 h2
    have hj : j = 1 := by simp [parting] at h2; omega
    exact ⟨(true, slot1), by simp [parting, hj]⟩

/-- The keeper claims slot 2 and lands it; the restored copy reads slot 1, whose
attempt its journal never recorded, and stops having landed nothing. -/
theorem restored_copy_finds_keeper :
    ∃ w, Steps parting w ∧ (w.copy false).stopped = true ∧ (w.copy true).stopped = false ∧
      (w.copy false).queue = [] := by
  refine ⟨_, .step (Step.detectRead parting false true slot1 rfl rfl (by simp [parting])
    (by simp [parting, slot1]) (by simp [parting, slot1])) (.refl _), ?_, ?_, ?_⟩ <;>
    simp [World.mod, parting]

end Example

end CovenSequence.Rejoin
