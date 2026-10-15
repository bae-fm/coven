import CovenStorage.KeyDelivery

/-! Finding stray S3 keys by listing the provider's keys.

T204 as first decided: every key coven makes is named at the provider with its
store and member, so an admin can list the store's keys; an admin's catch-up
compares that listing with the keys kept entries record and makes every
unrecorded one pending `DeleteAccessKey` in its own database. The catch-up
deletes another admin's key in flight (`catch_up_deletes_in_flight_key`).

T237 replaces it: the name also carries the minting device, and the catch-up
takes only unrecorded keys whose minting device is retired with a final
entry and drained. That never takes a recorded key or a live device's key in
flight (`reclaimed_safe`, `reclaimed_never_recorded`), and once the retired
devices' copies have stopped one catch-up and its deletions leave every key
they made recorded (`retired_keys_settle`). With T267 a reset keeps no made
key, so every pending deletion is the listing's and none is of a recorded
key (`pending_never_recorded`).

The model has one store, so every provider key is in the listing. -/

namespace CovenStorage.KeyDelivery.Listing
open CovenStorage.KeyDelivery

/-- The listed keys of the store that no kept entry records. -/
def unrecorded (w : World) (i : Nat) : List Nat :=
  (w.provider.filter fun p => p.id ∉ w.recorded ∧ heldKey (w.copies i) ≠ some p.id).map (·.id)

/-- Admin copy `i`'s catch-up records each unrecorded listed key as pending
deletion, in one transaction. -/
def catchUp (w : World) (i : Nat) : World :=
  { w with pending := fun k => if k = i then unrecorded w i ++ w.pending k else w.pending k }

/-- Copy `i` deletes every key it holds pending deletion (§13). -/
def deletePending (w : World) (i : Nat) : World :=
  { w with provider := w.provider.filter (fun p => p.id ∉ w.pending i)
           pending := fun k => if k = i then [] else w.pending k }

/-- After any surviving admin copy's completed catch-up every live key is
recorded, in that copy's journal, or pending deletion there: also a key whose
minting database was lost, which no reset could carry. -/
theorem catchUp_accounts (w : World) (i : Nat) (hi : i < w.slots) :
    Accounted (catchUp w i) := by
  intro p hp
  by_cases hr : p.id ∈ w.recorded
  · exact .inl hr
  by_cases hh : heldKey (w.copies i) = some p.id
  · exact .inr (.inl ⟨i, hi, hh⟩)
  · refine .inr (.inr ⟨i, hi, ?_⟩)
    simp only [catchUp, ↓reduceIte, unrecorded]
    apply List.mem_append_left
    exact List.mem_map.mpr ⟨p, List.mem_filter.mpr ⟨hp, by simp [hr, hh]⟩, rfl⟩

/-- Ana's desktop (device 7, slot 0) and Carol's laptop (device 9, slot 1),
both admins. Ana makes key 101 for Ben and her database is then lost: no
copy journals 101 and nothing records it. Carol's catch-up lists it and
keeps it pending deletion. -/
theorem lost_database_key_found_by_listing :
    let lost := run .random (start [7, 9] 100) [.begin 0, .mint 0, .drop 0]
    lost.provider = [⟨101, 100, 7⟩] ∧ ¬ Accounted lost ∧
    (catchUp lost 1).pending 1 = [101] ∧ Accounted (catchUp lost 1) := by
  decide

/-- The rule as decided is unsafe while another admin's replacement is in
flight. Ana makes key 101 for Ben and delivers it; before her set-access
entry is kept, Carol's catch-up lists 101 as unrecorded. Ana then records
101 as Ben's access, and Carol deletes what her catch-up made pending:
Ben's recorded current key is gone at the provider, so every device of his
that installs it is locked out (§13). An outstanding invite's key, which no
entry records until the person joins, is deleted the same way. -/
theorem catch_up_deletes_in_flight_key :
    let delivered := run .random (start [7, 9] 100) [.begin 0, .mint 0, .sealKey 0, .deliver 0]
    let after := deletePending (record (catchUp delivered 1) 0) 1
    delivered.provider = [⟨101, 100, 7⟩] ∧
    (catchUp delivered 1).pending 1 = [101] ∧
    after.recorded = [101] ∧ after.provider = [] := by
  decide

/-! ### T237: keys named by their minting device

Every key's provider-side name carries its minting device
(`ProviderKey.device`). `Sys` adds the devices this admin's database knows to
be retired with a final retiring entry and whose logs it has drained
(`retired`), and, as bookkeeping only, every key the listing has made pending
(`reclaimed`).

Once a device is drained no copy of it records anything more: §10 bounds its
publications to its retirement's finality plus one day and five minutes, and
the drain reads its logs after that (§6, §13). So a retired copy's `record`
and `deliver` change nothing here. It resets when it reads its retirement
(`stop`), keeping only the deletions it owed, as §10's reset does. Only
copies of the minting database record a key: a replacement's or an invite's
entry comes from the operation that made it. -/

structure Sys where
  w : World
  retired : List Nat
  reclaimed : List Nat

/-- Copy `i` exists and its device is not retired. -/
def Sys.live (s : Sys) (i : Nat) : Bool :=
  match s.w.copies i with
  | some c => decide (c.device ∉ s.retired)
  | none => false

/-- Recording and delivering are sends; a retired copy's are refused. -/
def Sys.allows (s : Sys) : Step → Bool
  | .record i | .deliver i => s.live i
  | _ => true

def baseStep (sealer : Sealer) (s : Sys) (st : Step) : Sys :=
  if s.allows st then { s with w := step sealer s.w st } else s

/-- The device's retirement is final and this admin drained its logs. -/
def retire (s : Sys) (d : Nat) : Sys :=
  if d < s.w.next then { s with retired := d :: s.retired } else s

/-- A copy that reads its device's retirement resets as a new device (§10),
keeping the deletions it owed but not its made key (T267). -/
def stop (s : Sys) (i : Nat) : Sys :=
  match s.w.copies i with
  | some c =>
    if c.device ∈ s.retired then
      { s with w := { s.w with
          copies := fun x => if x = i then some ⟨s.w.next, none⟩ else s.w.copies x
          resets := c.device :: s.w.resets, next := s.w.next + 1 } }
    else s
  | none => s

/-- The store's listed keys whose minting device is retired and that no
entry records. -/
def stray (s : Sys) : List Nat :=
  (s.w.provider.filter fun p => decide (p.device ∈ s.retired ∧ p.id ∉ s.w.recorded)).map (·.id)

/-- T237's catch-up at admin copy `i`: one transaction makes every stray key
pending `DeleteAccessKey`. A key whose device is live is never listed as
stray, whether recorded or in flight. -/
def reclaim (s : Sys) (i : Nat) : Sys :=
  { w := { s.w with pending := fun k => if k = i then stray s ++ s.w.pending k else s.w.pending k }
    retired := s.retired
    reclaimed := stray s ++ s.reclaimed }

def delete (s : Sys) (i : Nat) : Sys := { s with w := deletePending s.w i }

inductive SStep where
  | base (st : Step)
  | retire (d : Nat)
  | stop (i : Nat)
  | reclaim (i : Nat)
  | delete (i : Nat)
  deriving DecidableEq, Repr

def sstep (sealer : Sealer) (s : Sys) : SStep → Sys
  | .base st => baseStep sealer s st
  | .retire d => retire s d
  | .stop i => stop s i
  | .reclaim i => reclaim s i
  | .delete i => delete s i

def srun (sealer : Sealer) (s : Sys) (steps : List SStep) : Sys :=
  steps.foldl (sstep sealer) s

def sstart (devices : List Nat) (next : Nat) : Sys := ⟨start devices next, [], []⟩

/-! #### What one model step changes -/

/-- After a step a copy holds no key, holds what some copy of the same device
held before, or is the minting copy holding the fresh key. -/
theorem step_held (sealer : Sealer) (w : World) (st : Step) (k : Nat) (c' : Copy)
    (h : (step sealer w st).copies k = some c') :
    heldKey (some c') = none ∨
    (∃ j c, w.copies j = some c ∧ c.device = c'.device ∧ heldKey (some c) = heldKey (some c')) ∨
    (∃ c, w.copies k = some c ∧ c.device = c'.device ∧ heldKey (some c') = some w.next ∧
      (step sealer w st).next = w.next + 1 ∧ st = .mint k) := by
  cases st with
  | begin i =>
    simp only [step] at h ⊢; revert h; unfold begin; split
    · rename_i d hc
      intro h; change (if k = i then _ else _) = some c' at h
      split at h
      · cases h; exact .inl rfl
      · exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
    · intro h; exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
  | mint i =>
    simp only [step] at h ⊢; revert h; unfold mint; split
    · rename_i d a sd v hc
      intro h; change (if k = i then _ else _) = some c' at h
      split at h
      · rename_i hk; subst hk; cases h
        exact .inr (.inr ⟨_, hc, rfl, rfl, rfl, rfl⟩)
      · exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
    · intro h; exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
  | sealKey i =>
    simp only [step] at h ⊢; revert h; unfold sealKey; split
    · rename_i d a key v hc
      intro h; change (if k = i then _ else _) = some c' at h
      split at h
      · rename_i hk; subst hk; cases h
        exact .inr (.inl ⟨k, _, hc, rfl, by simp [heldKey]⟩)
      · exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
    · intro h; exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
  | deliver i =>
    simp only [step] at h ⊢; revert h; unfold deliver; split
    · rename_i d a key b hc
      split
      · intro h; change (if k = i then _ else _) = some c' at h
        split at h
        · rename_i hk; subst hk; cases h
          exact .inr (.inl ⟨k, _, hc, rfl, by simp [heldKey]⟩)
        · exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
      · split
        · intro h; rw [put_copies] at h
          split at h
          · rename_i hk; subst hk; cases h
            exact .inr (.inl ⟨k, _, hc, rfl, by simp [heldKey]⟩)
          · exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
        · intro h; change (if k = i then _ else _) = some c' at h
          split at h
          · cases h; exact .inl rfl
          · exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
    · intro h; exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
  | record i =>
    simp only [step] at h ⊢; revert h; unfold record; split
    · intro h; change (if k = i then _ else _) = some c' at h
      split at h
      · cases h; exact .inl rfl
      · exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
    · intro h; exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
  | clone i =>
    simp only [step] at h ⊢; revert h; unfold clone; split
    · rename_i c hc
      intro h; change (if k = w.slots then _ else _) = some c' at h
      split at h
      · cases h; exact .inr (.inl ⟨i, c', hc, rfl, rfl⟩)
      · exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
    · intro h; exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)
  | drop i =>
    simp only [step, drop] at h ⊢; rw [put_copies] at h
    split at h
    · cases h
    · exact .inr (.inl ⟨k, c', h, rfl, rfl⟩)

/-- A key new at the provider is the fresh key minted by an existing copy,
named with that copy's device. -/
theorem step_provider (sealer : Sealer) (w : World) (st : Step) (p : ProviderKey)
    (hp : p ∈ (step sealer w st).provider) :
    p ∈ w.provider ∨ ∃ i c, w.copies i = some c ∧ p.id = w.next ∧ p.device = c.device ∧
      (step sealer w st).next = w.next + 1 ∧ st = .mint i := by
  cases st with
  | mint i =>
    simp only [step] at hp ⊢; revert hp; unfold mint; split
    · rename_i d a sd v hc
      intro hp
      simp only [List.mem_append, List.mem_filter, List.mem_singleton] at hp
      rcases hp with ⟨hp, -⟩ | rfl
      · exact .inl hp
      · exact .inr ⟨i, _, hc, rfl, rfl, rfl, rfl⟩
    · intro hp; exact .inl hp
  | begin i => simp only [step] at hp; unfold begin at hp; split at hp <;> exact .inl hp
  | sealKey i => simp only [step] at hp; unfold sealKey at hp; split at hp <;> exact .inl hp
  | deliver i =>
    simp only [step] at hp; unfold deliver at hp
    split at hp
    · split at hp
      · exact .inl hp
      · split at hp <;> exact .inl hp
    · exact .inl hp
  | record i => simp only [step] at hp; unfold record at hp; split at hp <;> exact .inl hp
  | clone i => simp only [step] at hp; unfold clone at hp; split at hp <;> exact .inl hp
  | drop i => exact .inl hp

/-- Only `record` adds to the recorded keys, and only the key its copy holds. -/
theorem step_recorded (sealer : Sealer) (w : World) (st : Step) :
    (step sealer w st).recorded = w.recorded ∨
    ∃ i c x, st = .record i ∧ w.copies i = some c ∧ heldKey (some c) = some x ∧
      (step sealer w st).recorded = x :: w.recorded := by
  cases st with
  | record i =>
    simp only [step]; unfold record; split
    · rename_i d a x sd hc
      exact .inr ⟨i, _, x, rfl, hc, rfl, rfl⟩
    · exact .inl rfl
  | begin i => left; simp only [step]; unfold begin; split <;> rfl
  | mint i => left; simp only [step]; unfold mint; split <;> rfl
  | sealKey i => left; simp only [step]; unfold sealKey; split <;> rfl
  | deliver i =>
    left; simp only [step]; unfold deliver; split
    · split
      · rfl
      · split
        · rfl
        · rfl
    · rfl
  | clone i => left; simp only [step]; unfold clone; split <;> rfl
  | drop i => exact .inl rfl

theorem step_next (sealer : Sealer) (w : World) (st : Step) : w.next ≤ (step sealer w st).next := by
  cases st with
  | begin i => simp only [step]; unfold begin; split <;> simp
  | mint i => simp only [step]; unfold mint; split <;> simp
  | sealKey i => simp only [step]; unfold sealKey; split <;> simp
  | deliver i =>
    simp only [step]; unfold deliver; split
    · split
      · exact Nat.le_refl _
      · split
        · exact Nat.le_refl _
        · simp
    · exact Nat.le_refl _
  | record i => simp only [step]; unfold record; split <;> exact Nat.le_refl _
  | clone i => simp only [step]; unfold clone; split <;> exact Nat.le_refl _
  | drop i => exact Nat.le_refl _

/-- No step adds a pending deletion: each is one some copy already held. -/
theorem step_pending (sealer : Sealer) (w : World) (st : Step) (k x : Nat)
    (h : x ∈ (step sealer w st).pending k) : ∃ j, x ∈ w.pending j := by
  cases st with
  | clone i =>
    simp only [step] at h; revert h; unfold clone; split
    · intro h; change x ∈ (if k = w.slots then _ else _) at h
      split at h
      · exact ⟨i, h⟩
      · exact ⟨k, h⟩
    · intro h; exact ⟨k, h⟩
  | drop i =>
    simp only [step, drop] at h; change x ∈ (if k = i then [] else w.pending k) at h
    split at h
    · cases h
    · exact ⟨k, h⟩
  | begin i => simp only [step] at h; unfold begin at h; split at h <;> exact ⟨k, h⟩
  | mint i => simp only [step] at h; unfold mint at h; split at h <;> exact ⟨k, h⟩
  | sealKey i => simp only [step] at h; unfold sealKey at h; split at h <;> exact ⟨k, h⟩
  | deliver i =>
    simp only [step] at h; unfold deliver at h
    split at h
    · split at h
      · exact ⟨k, h⟩
      · split at h <;> exact ⟨k, h⟩
    · exact ⟨k, h⟩
  | record i => simp only [step] at h; unfold record at h; split at h <;> exact ⟨k, h⟩

/-! #### The listing never takes a recorded or in-flight key -/

structure Good (s : Sys) : Prop where
  inv : Inv s.w
  heldFresh : ∀ k c x, s.w.copies k = some c → heldKey (some c) = some x → x < s.w.next
  providerFresh : ∀ p ∈ s.w.provider, p.id < s.w.next
  /-- A key a copy holds is named at the provider with that copy's device. -/
  heldProvider : ∀ k c x p, s.w.copies k = some c → heldKey (some c) = some x →
    p ∈ s.w.provider → p.id = x → p.device = c.device
  retiredFresh : ∀ d ∈ s.retired, d < s.w.next
  reclaimedFresh : ∀ x ∈ s.reclaimed, x < s.w.next
  /-- No entry records a key the listing made pending. -/
  unrecorded : ∀ x ∈ s.reclaimed, x ∉ s.w.recorded
  /-- No live copy holds one in flight. -/
  notInFlight : ∀ x ∈ s.reclaimed, ∀ k c, s.w.copies k = some c → c.device ∉ s.retired →
    heldKey (some c) ≠ some x
  /-- Every pending deletion is one the listing made. -/
  pendingReclaimed : ∀ k x, x ∈ s.w.pending k → x ∈ s.reclaimed

theorem live_of {s : Sys} {i : Nat} (h : s.live i = true) :
    ∃ c, s.w.copies i = some c ∧ c.device ∉ s.retired := by
  unfold Sys.live at h
  split at h
  · rename_i c hc; exact ⟨c, hc, by simpa using h⟩
  · cases h

theorem good_base {s : Sys} (h : Good s) (sealer : Sealer) (st : Step) :
    Good (baseStep sealer s st) := by
  unfold baseStep
  split
  · rename_i allowed
    have hn := step_next sealer s.w st
    refine ⟨step_inv h.inv sealer st, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
    · intro k c' x hk hx
      rcases step_held sealer s.w st k c' hk with none | ⟨j, c, hj, -, hjx⟩ | ⟨c, -, -, hx', hnext, -⟩
      · rw [none] at hx; cases hx
      · exact Nat.lt_of_lt_of_le (h.heldFresh j c x hj (hjx.trans hx)) hn
      · rw [hx'] at hx; cases hx; show s.w.next < (step sealer s.w st).next; omega
    · intro p hp
      rcases step_provider sealer s.w st p hp with old | ⟨i, c, -, hid, -, hnext, -⟩
      · exact Nat.lt_of_lt_of_le (h.providerFresh p old) hn
      · show p.id < (step sealer s.w st).next; omega
    · intro k c' x p hk hx hp hpx
      rcases step_held sealer s.w st k c' hk with none | ⟨j, c, hj, hdev, hjx⟩ | ⟨c, hc, hdev, hx', -, hk'⟩
      · rw [none] at hx; cases hx
      · rcases step_provider sealer s.w st p hp with old | ⟨i, c₀, -, hid, -, -, -⟩
        · rw [← hdev]; exact h.heldProvider j c x p hj (hjx.trans hx) old hpx
        · have := h.heldFresh j c x hj (hjx.trans hx); omega
      · rw [hx'] at hx; cases hx
        rcases step_provider sealer s.w st p hp with old | ⟨i, c₀, hc₀, -, hpd, -, hi⟩
        · have := h.providerFresh p old; omega
        · -- The fresh key is held only by the copy that minted it.
          rw [hk'] at hi; cases hi
          rw [hc] at hc₀; cases hc₀
          rw [hpd, hdev]
    · intro d hd; exact Nat.lt_of_lt_of_le (h.retiredFresh d hd) hn
    · intro x hx; exact Nat.lt_of_lt_of_le (h.reclaimedFresh x hx) hn
    · intro x hx hrec
      rcases step_recorded sealer s.w st with same | ⟨i, c, y, rfl, hc, hy, hrec'⟩
      · exact h.unrecorded x hx (same ▸ hrec)
      · rw [hrec'] at hrec
        rcases List.mem_cons.mp hrec with rfl | old
        · obtain ⟨c₁, hc₁, hlive⟩ := live_of (s := s) (i := i) allowed
          rw [hc] at hc₁; cases hc₁
          exact h.notInFlight x hx i c hc hlive hy
        · exact h.unrecorded x hx old
    · intro x hx k c' hk hlive hheld
      rcases step_held sealer s.w st k c' hk with none | ⟨j, c, hj, hdev, hjx⟩ | ⟨c, -, -, hx', -, -⟩
      · rw [none] at hheld; cases hheld
      · exact h.notInFlight x hx j c hj (hdev ▸ hlive) (hjx.trans hheld)
      · rw [hx'] at hheld; cases hheld
        have := h.reclaimedFresh _ hx; omega
    · intro k x hx
      obtain ⟨j, hj⟩ := step_pending sealer s.w st k x hx
      exact h.pendingReclaimed j x hj
  · exact h

theorem good_retire {s : Sys} (h : Good s) (d : Nat) : Good (retire s d) := by
  unfold retire
  split
  · rename_i hd
    refine ⟨h.inv, h.heldFresh, h.providerFresh, h.heldProvider, ?_, h.reclaimedFresh,
      h.unrecorded, ?_, ?_⟩
    · intro e he
      rcases List.mem_cons.mp he with rfl | old
      · exact hd
      · exact h.retiredFresh e old
    · intro x hx k c hk hlive
      exact h.notInFlight x hx k c hk (fun hr => hlive (List.mem_cons_of_mem _ hr))
    · exact h.pendingReclaimed
  · exact h

theorem good_stop {s : Sys} (h : Good s) (i : Nat) : Good (stop s i) := by
  unfold stop
  split
  · rename_i c hc
    split
    · have inv := inv_congr (put_idle h.inv hc (some ⟨s.w.next, none⟩) (by intro y hy; cases hy; rfl))
        (w' := { s.w with
          copies := fun x => if x = i then some ⟨s.w.next, none⟩ else s.w.copies x
          resets := c.device :: s.w.resets, next := s.w.next + 1 })
        rfl rfl rfl (Nat.le_succ _)
      -- the stopped copy holds nothing; every other copy is unchanged
      have view : ∀ k c', (if k = i then some (⟨s.w.next, none⟩ : Copy) else s.w.copies k) = some c' →
          heldKey (some c') = none ∨ s.w.copies k = some c' := by
        intro k c' hk
        split at hk
        · cases hk; exact .inl rfl
        · exact .inr hk
      refine ⟨inv, ?_, ?_, ?_, ?_, ?_, h.unrecorded, ?_, h.pendingReclaimed⟩
      · intro k c' x hk hx
        rcases view k c' hk with none | old
        · rw [none] at hx; cases hx
        · have := h.heldFresh k c' x old hx; show x < s.w.next + 1; omega
      · intro p hp; have := h.providerFresh p hp; show p.id < s.w.next + 1; omega
      · intro k c' x p hk hx hp hpx
        rcases view k c' hk with none | old
        · rw [none] at hx; cases hx
        · exact h.heldProvider k c' x p old hx hp hpx
      · intro d hd; have := h.retiredFresh d hd; show d < s.w.next + 1; omega
      · intro x hx; have := h.reclaimedFresh x hx; show x < s.w.next + 1; omega
      · intro x hx k c' hk hlive hheld
        rcases view k c' hk with none | old
        · rw [none] at hheld; cases hheld
        · exact h.notInFlight x hx k c' old hlive hheld
    · exact h
  · exact h

theorem mem_stray {s : Sys} {x : Nat} (hx : x ∈ stray s) :
    ∃ p ∈ s.w.provider, p.id = x ∧ p.device ∈ s.retired ∧ p.id ∉ s.w.recorded := by
  unfold stray at hx
  obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hx
  obtain ⟨hp, hd⟩ := List.mem_filter.mp hp
  simp only [decide_eq_true_eq] at hd
  exact ⟨p, hp, rfl, hd.1, hd.2⟩

theorem good_reclaim {s : Sys} (h : Good s) (i : Nat) : Good (reclaim s i) := by
  refine ⟨inv_congr h.inv rfl rfl rfl (Nat.le_refl _), h.heldFresh, h.providerFresh,
    h.heldProvider, h.retiredFresh, ?_, ?_, ?_, ?_⟩
  · intro x hx
    rcases List.mem_append.mp hx with new | old
    · obtain ⟨p, hp, rfl, -, -⟩ := mem_stray new; exact h.providerFresh p hp
    · exact h.reclaimedFresh x old
  · intro x hx
    rcases List.mem_append.mp hx with new | old
    · obtain ⟨p, -, rfl, -, hr⟩ := mem_stray new; exact hr
    · exact h.unrecorded x old
  · intro x hx k c hk hlive hheld
    rcases List.mem_append.mp hx with new | old
    · -- A stray key names a retired device; a live copy's key names its own.
      obtain ⟨p, hp, rfl, hret, -⟩ := mem_stray new
      exact hlive (h.heldProvider k c _ p hk hheld hp rfl ▸ hret)
    · exact h.notInFlight x old k c hk hlive hheld
  · intro k x hx
    change x ∈ (if k = i then stray s ++ s.w.pending k else s.w.pending k) at hx
    split at hx
    · rcases List.mem_append.mp hx with new | old
      · exact List.mem_append_left _ new
      · exact List.mem_append_right _ (h.pendingReclaimed k x old)
    · exact List.mem_append_right _ (h.pendingReclaimed k x hx)

theorem good_delete {s : Sys} (h : Good s) (i : Nat) : Good (delete s i) := by
  refine ⟨inv_congr h.inv rfl rfl rfl (Nat.le_refl _), h.heldFresh, ?_, ?_, h.retiredFresh,
    h.reclaimedFresh, h.unrecorded, h.notInFlight, ?_⟩
  · intro p hp; exact h.providerFresh p (List.mem_filter.mp hp).1
  · intro k c x p hk hx hp hpx; exact h.heldProvider k c x p hk hx (List.mem_filter.mp hp).1 hpx
  · intro k x hx
    change x ∈ (if k = i then [] else s.w.pending k) at hx
    split at hx
    · cases hx
    · exact h.pendingReclaimed k x hx

theorem good_sstep {s : Sys} (h : Good s) (sealer : Sealer) (st : SStep) :
    Good (sstep sealer s st) := by
  cases st with
  | base st => exact good_base h sealer st
  | retire d => exact good_retire h d
  | stop i => exact good_stop h i
  | reclaim i => exact good_reclaim h i
  | delete i => exact good_delete h i

theorem good_srun {s : Sys} (h : Good s) (sealer : Sealer) (steps : List SStep) :
    Good (srun sealer s steps) := by
  induction steps generalizing s with
  | nil => exact h
  | cons st sts ih => exact ih (good_sstep h sealer st)

theorem good_start (devices : List Nat) (next : Nat) : Good (sstart devices next) := by
  refine ⟨start_inv devices next, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro k c x hk hx
    simp only [sstart, start, Option.map_eq_some_iff] at hk
    obtain ⟨_, -, rfl⟩ := hk; simp [heldKey] at hx
  · intro p hp; simp [sstart, start] at hp
  · intro k c x p _ _ hp; simp [sstart, start] at hp
  · intro d hd; simp [sstart] at hd
  · intro x hx; simp [sstart] at hx
  · intro x hx; simp [sstart] at hx
  · intro x hx; simp [sstart] at hx
  · intro k x hx; simp [sstart, start] at hx

/-- T237's rule is safe: in every history of backups, clones, resets,
retirements and catch-ups, a key the listing made pending is in no entry,
and no copy of a live device holds it in flight. -/
theorem reclaimed_safe (devices : List Nat) (next : Nat) (sealer : Sealer) (steps : List SStep) :
    let s := srun sealer (sstart devices next) steps
    ∀ x ∈ s.reclaimed, x ∉ s.w.recorded ∧
      ∀ k c, s.w.copies k = some c → c.device ∉ s.retired → heldKey (some c) ≠ some x := by
  intro s x hx
  have h := good_srun (good_start devices next) sealer steps
  exact ⟨h.unrecorded x hx, h.notInFlight x hx⟩

theorem reclaimed_grows (sealer : Sealer) (s : Sys) (st : SStep) :
    ∀ x ∈ s.reclaimed, x ∈ (sstep sealer s st).reclaimed := by
  intro x hx
  cases st with
  | base st => simp only [sstep, baseStep]; split <;> exact hx
  | retire d => simp only [sstep, retire]; split <;> exact hx
  | stop i => simp only [sstep, stop]; split <;> (try split) <;> exact hx
  | reclaim i => simp only [sstep, reclaim]; exact List.mem_append_right _ hx
  | delete i => exact hx

theorem reclaimed_grows_run (sealer : Sealer) (s : Sys) (steps : List SStep) :
    ∀ x ∈ s.reclaimed, x ∈ (srun sealer s steps).reclaimed := by
  induction steps generalizing s with
  | nil => intro x hx; exact hx
  | cons st sts ih => intro x hx; exact ih _ x (reclaimed_grows sealer s st x hx)

theorem srun_append (sealer : Sealer) (s : Sys) (a b : List SStep) :
    srun sealer s (a ++ b) = srun sealer (srun sealer s a) b := by
  simp [srun, List.foldl_append]

/-- So a key the listing made pending is never recorded later either: no
entry recording it is kept, whatever follows. -/
theorem reclaimed_never_recorded (devices : List Nat) (next : Nat) (sealer : Sealer)
    (steps later : List SStep) :
    ∀ x ∈ (srun sealer (sstart devices next) steps).reclaimed,
      x ∉ (srun sealer (sstart devices next) (steps ++ later)).w.recorded := by
  intro x hx
  rw [srun_append]
  exact (good_srun (good_srun (good_start devices next) sealer steps) sealer later).unrecorded x
    (reclaimed_grows_run sealer _ later x hx)

/-- No copy deletes a recorded key: every deletion a copy holds pending is the
listing's, so no entry records it, now or later. A reset adds none (T267). -/
theorem pending_never_recorded (devices : List Nat) (next : Nat) (sealer : Sealer)
    (steps later : List SStep) (k x : Nat)
    (hx : x ∈ (srun sealer (sstart devices next) steps).w.pending k) :
    x ∉ (srun sealer (sstart devices next) (steps ++ later)).w.recorded :=
  reclaimed_never_recorded devices next sealer steps later x
    ((good_srun (good_start devices next) sealer steps).pendingReclaimed k x hx)

/-! #### Every key a retired device made ends recorded or deleted -/

/-- After admin copy `i`'s catch-up every live key a retired device made is
recorded or pending deletion at `i`. -/
theorem reclaim_covers_retired (s : Sys) (i : Nat) :
    ∀ p ∈ (reclaim s i).w.provider, p.device ∈ (reclaim s i).retired →
      p.id ∈ (reclaim s i).w.recorded ∨ p.id ∈ (reclaim s i).w.pending i := by
  intro p hp hd
  by_cases hr : p.id ∈ s.w.recorded
  · exact .inl hr
  · right
    simp only [reclaim, ↓reduceIte]
    apply List.mem_append_left
    exact List.mem_map.mpr ⟨p, List.mem_filter.mpr ⟨hp, by simp_all [reclaim]⟩, rfl⟩

/-- Deleting what that catch-up made pending leaves every live key of a
retired device recorded. -/
theorem delete_after_reclaim (s : Sys) (i : Nat) :
    let t := delete (reclaim s i) i
    ∀ p ∈ t.w.provider, p.device ∈ t.retired → p.id ∈ t.w.recorded := by
  intro t p hp hd
  have hp' := List.mem_filter.mp hp
  rcases reclaim_covers_retired s i p hp'.1 hd with r | pend
  · exact r
  · simp only [decide_eq_true_eq] at hp'
    exact absurd pend hp'.2

/-- A copy after a step has some earlier copy's device or the fresh id. -/
theorem step_device (sealer : Sealer) (w : World) (st : Step) (k : Nat) (c' : Copy)
    (h : (step sealer w st).copies k = some c') :
    (∃ j c, w.copies j = some c ∧ c.device = c'.device) ∨ c'.device = w.next := by
  rcases step_held sealer w st k c' h with none | ⟨j, c, hj, hd, -⟩ | ⟨c, hc, hd, -, -, -⟩
  · -- a copy holding nothing: unchanged, emptied by `record`, or reset
    cases st with
    | begin i =>
      simp only [step] at h; revert h; unfold begin; split
      · rename_i d hc; intro h; change (if k = i then _ else _) = some c' at h
        split at h
        · cases h; exact .inl ⟨i, _, hc, rfl⟩
        · exact .inl ⟨k, c', h, rfl⟩
      · intro h; exact .inl ⟨k, c', h, rfl⟩
    | mint i =>
      simp only [step] at h; revert h; unfold mint; split
      · rename_i d a sd v hc; intro h; change (if k = i then _ else _) = some c' at h
        split at h
        · cases h; exact .inl ⟨i, _, hc, rfl⟩
        · exact .inl ⟨k, c', h, rfl⟩
      · intro h; exact .inl ⟨k, c', h, rfl⟩
    | sealKey i =>
      simp only [step] at h; revert h; unfold sealKey; split
      · rename_i d a key v hc; intro h; change (if k = i then _ else _) = some c' at h
        split at h
        · cases h; exact .inl ⟨i, _, hc, rfl⟩
        · exact .inl ⟨k, c', h, rfl⟩
      · intro h; exact .inl ⟨k, c', h, rfl⟩
    | deliver i =>
      simp only [step] at h; revert h; unfold deliver; split
      · rename_i d a key b hc
        split
        · intro h; change (if k = i then _ else _) = some c' at h
          split at h
          · cases h; exact .inl ⟨i, _, hc, rfl⟩
          · exact .inl ⟨k, c', h, rfl⟩
        · split
          · intro h; rw [put_copies] at h
            split at h
            · cases h; exact .inl ⟨i, _, hc, rfl⟩
            · exact .inl ⟨k, c', h, rfl⟩
          · intro h; change (if k = i then _ else _) = some c' at h
            split at h
            · cases h; exact .inr rfl
            · exact .inl ⟨k, c', h, rfl⟩
      · intro h; exact .inl ⟨k, c', h, rfl⟩
    | record i =>
      simp only [step] at h; revert h; unfold record; split
      · rename_i d a x sd hc; intro h; change (if k = i then _ else _) = some c' at h
        split at h
        · cases h; exact .inl ⟨i, _, hc, rfl⟩
        · exact .inl ⟨k, c', h, rfl⟩
      · intro h; exact .inl ⟨k, c', h, rfl⟩
    | clone i =>
      simp only [step] at h; revert h; unfold clone; split
      · rename_i c hc; intro h; change (if k = w.slots then _ else _) = some c' at h
        split at h
        · cases h; exact .inl ⟨i, c', hc, rfl⟩
        · exact .inl ⟨k, c', h, rfl⟩
      · intro h; exact .inl ⟨k, c', h, rfl⟩
    | drop i =>
      simp only [step, drop] at h; rw [put_copies] at h
      split at h
      · cases h
      · exact .inl ⟨k, c', h, rfl⟩
  · exact .inl ⟨j, c, hj, hd⟩
  · exact .inl ⟨k, c, hc, hd⟩

/-- No copy of a retired device is left: each has read its retirement and reset. -/
def Sys.allLive (s : Sys) : Prop := ∀ k c, s.w.copies k = some c → c.device ∉ s.retired

/-- Once every copy of a retired device has stopped, no step but a new
retirement makes a key of a retired device, retires another device or
unrecords a key. -/
theorem settled_sstep {s : Sys} (h : Good s) (hl : s.allLive) (sealer : Sealer) (st : SStep)
    (hst : ∀ d, st ≠ .retire d) :
    (sstep sealer s st).allLive ∧ (sstep sealer s st).retired = s.retired ∧
    (∀ x ∈ s.w.recorded, x ∈ (sstep sealer s st).w.recorded) ∧
    ∀ p ∈ (sstep sealer s st).w.provider, p.device ∈ s.retired → p ∈ s.w.provider := by
  cases st with
  | retire d => exact absurd rfl (hst d)
  | base st =>
    simp only [sstep, baseStep]
    split
    · refine ⟨?_, rfl, ?_, ?_⟩
      · intro k c' hk
        rcases step_device sealer s.w st k c' hk with ⟨j, c, hj, hd⟩ | fresh
        · rw [← hd]; exact hl j c hj
        · rw [fresh]; intro hr; have := h.retiredFresh _ hr; omega
      · intro x hx
        rcases step_recorded sealer s.w st with same | ⟨_, _, _, _, _, _, hrec⟩
        · show x ∈ (step sealer s.w st).recorded; rw [same]; exact hx
        · show x ∈ (step sealer s.w st).recorded; rw [hrec]; exact List.mem_cons_of_mem _ hx
      · intro p hp hd
        rcases step_provider sealer s.w st p hp with old | ⟨i, c, hc, -, hpd, -, -⟩
        · exact old
        · exact absurd (hpd ▸ hd) (hl i c hc)
    · exact ⟨hl, rfl, fun x hx => hx, fun p hp _ => hp⟩
  | stop i =>
    have same : stop s i = s := by
      unfold stop; split
      · rename_i c hc; simp [hl i c hc]
      · rfl
    have e : sstep sealer s (.stop i) = s := by simp only [sstep, same]
    rw [e]
    exact ⟨hl, rfl, fun x hx => hx, fun p hp _ => hp⟩
  | reclaim i => exact ⟨hl, rfl, fun x hx => hx, fun p hp _ => hp⟩
  | delete i =>
    exact ⟨hl, rfl, fun x hx => hx, fun p hp _ => (List.mem_filter.mp hp).1⟩

/-- T237 liveness: once the retired devices' copies have stopped, one admin
catch-up and the deletion of what it made pending leave every live key a
retired device made recorded, and no later step except another retirement
changes that. With no admin catch-up the keys stay live, so it is the
catch-up that reclaims them, whether the minting database reset (T203), was
lost or was never copied. -/
theorem retired_keys_settle {s : Sys} (h : Good s) (hl : s.allLive) (sealer : Sealer)
    (i : Nat) (steps : List SStep) (hs : ∀ st ∈ steps, ∀ d, st ≠ .retire d) :
    let t := srun sealer (delete (reclaim s i) i) steps
    ∀ p ∈ t.w.provider, p.device ∈ t.retired → p.id ∈ t.w.recorded := by
  intro t
  have base := delete_after_reclaim s i
  have h₀ : Good (delete (reclaim s i) i) := good_delete (good_reclaim h i) i
  have hl₀ : (delete (reclaim s i) i).allLive := hl
  have go : ∀ (u : Sys) (sts : List SStep), Good u → u.allLive →
      (∀ st ∈ sts, ∀ d, st ≠ .retire d) →
      (∀ p ∈ u.w.provider, p.device ∈ u.retired → p.id ∈ u.w.recorded) →
      ∀ p ∈ (srun sealer u sts).w.provider, p.device ∈ (srun sealer u sts).retired →
        p.id ∈ (srun sealer u sts).w.recorded := by
    intro u sts
    induction sts generalizing u with
    | nil => intro _ _ _ hu; exact hu
    | cons st sts ih =>
      intro hg hal hsts hu
      obtain ⟨hal', hret, hrec, hprov⟩ :=
        settled_sstep hg hal sealer st (hsts st List.mem_cons_self)
      apply ih (sstep sealer u st) (good_sstep hg sealer st) hal'
        (fun x hx => hsts x (List.mem_cons_of_mem _ hx))
      intro p hp hd
      rw [hret] at hd
      exact hrec _ (hu p (hprov p hp hd) hd)
  exact go _ steps h₀ hl₀ hs base

/-! #### Checked histories -/

/-- T204's history under T237: Ana's desktop (device 7, slot 0) makes key 101
for Ben and delivers it; Carol's catch-up (slot 1) runs before Ana records
it. Device 7 is live, so the listing leaves 101 alone; Ana records it and
nothing deletes it. -/
theorem device_rule_spares_in_flight_key :
    let t := srun .random (sstart [7, 9] 100)
      [.base (.begin 0), .base (.mint 0), .base (.sealKey 0), .base (.deliver 0), .reclaim 1,
        .base (.record 0), .delete 1]
    t.w.provider = [⟨101, 100, 7⟩] ∧ t.w.recorded = [101] ∧ t.w.pending 1 = [] ∧
    t.reclaimed = [] := by
  decide

/-- Ana makes key 101 and her database is lost before she records it. While
device 7 is not retired the listing keeps the key; once its retirement is
final and drained, Carol's catch-up makes it pending and deletes it. -/
theorem lost_database_key_reclaimed_once_retired :
    let lost := srun .random (sstart [7, 9] 100) [.base (.begin 0), .base (.mint 0), .base (.drop 0)]
    let t := srun .random lost [.retire 7, .reclaim 1]
    (reclaim lost 1).w.pending 1 = [] ∧ t.w.pending 1 = [101] ∧ t.reclaimed = [101] ∧
    (delete t 1).w.provider = [] := by
  decide

/-- T267: a reset keeps no made key, so a second live copy of one database
cannot delete a recorded key through it. Ana's desktop makes key 101, a clone
of its database runs elsewhere, and both seal. The original delivers first;
the clone finds different bytes and resets. The original then records 101
as Ben's access, as §10 allows an entry made before reading its replacement.
Device 7 is retired, the original copy stops, and Carol's catch-up finds 101
recorded: nothing holds it pending and it stays live. -/
theorem reset_spares_recorded_key :
    let t := srun .random (sstart [7, 9] 100)
      [.base (.begin 0), .base (.mint 0), .base (.clone 0), .base (.sealKey 0),
        .base (.deliver 0), .base (.sealKey 2), .base (.deliver 2), .base (.record 0),
        .retire 7, .stop 0, .reclaim 1, .delete 1, .delete 2]
    t.w.resets = [7, 7] ∧ t.w.recorded = [101] ∧ t.w.provider = [⟨101, 100, 7⟩] := by
  decide

/-- The key a reset leaves (KeyDeliveryExamples `restored_before_seal_orphans_key`)
is reclaimed once its device is retired. Ana's desktop makes key 101 and is
backed up before sealing; the live database delivers, the backup is restored
over it, re-seals, finds different bytes and resets. Once device 7 is
retired, Carol's catch-up makes 101 pending and deletes it. -/
theorem reset_key_reclaimed_once_retired :
    let reset := srun .random (sstart [7, 9] 100)
      [.base (.begin 0), .base (.mint 0), .base (.clone 0), .base (.sealKey 0),
        .base (.deliver 0), .base (.drop 0), .base (.sealKey 2), .base (.deliver 2)]
    reset.w.resets = [7] ∧ reset.w.provider = [⟨101, 100, 7⟩] ∧ reset.w.recorded = [] ∧
    (srun .random reset [.retire 7, .reclaim 1, .delete 1]).w.provider = [] := by
  decide

/-- §4's retry rule deletes a recorded key under the same two live copies
(checked counterexample). The clone was taken after the attempt was
journaled and before its key was made; the original makes, delivers and
records 101; the clone's mint then deletes the keys under the attempt's name
whose secret it does not hold, which is 101, and makes 103. -/
theorem same_name_mint_deletes_recorded_key :
    let after := run .random (start [7] 100)
      [.begin 0, .clone 0, .mint 0, .sealKey 0, .deliver 0, .record 0, .mint 1]
    after.recorded = [101] ∧ after.provider = [⟨103, 100, 7⟩] := by
  decide

end CovenStorage.KeyDelivery.Listing
