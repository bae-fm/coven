/-!
# Claiming a slot

A slot `<store>/seq/<n>` is claimed by a create-once request. The provider
stores the first create of a slot and refuses every later one. Its reply can
be wrong in one direction: a create that stored its bytes may still be
answered "already exists", for example to the retry after a lost reply
(CloudKit did this live). It never answers "created" to a create whose bytes
are not the ones stored. A reply can also be lost.

The claim protocol: "created" means won. Any other reply means read the slot
and compare it with the claimant's own bytes: equal means won, other bytes
mean lost, and an empty slot means send the same bytes again.

The store is modelled as a map from slot to stored bytes. Every provider step
only fills empty slots (`create`), so the store only grows (`Extends`). That
fact, and that every claimant's bytes are unique to its attempt, is all the
protocol's correctness rests on.
-/

namespace CovenSequence.Claim

/-- The provider's slots: the bytes stored at each slot, if any. -/
abbrev Store (B : Type) := Nat → Option B

variable {B : Type}

/-- A create-once request for slot `n` with bytes `b`: it stores `b` only when
the slot is empty. -/
def create (s : Store B) (n : Nat) (b : B) : Store B :=
  fun k => if k = n then (match s k with | none => some b | some x => some x) else s k

/-- `t` holds everything `s` holds. -/
def Extends (s t : Store B) : Prop := ∀ n b, s n = some b → t n = some b

theorem Extends.refl (s : Store B) : Extends s s := fun _ _ h => h

theorem Extends.trans {s t u : Store B} (h₁ : Extends s t) (h₂ : Extends t u) :
    Extends s u := fun n b h => h₂ n b (h₁ n b h)

/-- A create never changes a stored slot. -/
theorem create_extends (s : Store B) (n : Nat) (b : B) : Extends s (create s n b) := by
  intro k x h
  unfold create
  by_cases hk : k = n
  · subst hk; simp [h]
  · simp [hk, h]

/-- A create of an empty slot stores its bytes; of an occupied slot, keeps the
occupant. -/
theorem create_at (s : Store B) (n : Nat) (b : B) :
    create s n b n = (match s n with | none => some b | some x => some x) := by
  simp [create]

@[simp] theorem create_self_none {s : Store B} {n : Nat} (b : B) (h : s n = none) :
    create s n b n = some b := by simp [create, h]

@[simp] theorem create_self_some {s : Store B} {n : Nat} {x : B} (b : B) (h : s n = some x) :
    create s n b n = some x := by simp [create, h]

@[simp] theorem create_other {s : Store B} {n k : Nat} (b : B) (h : k ≠ n) :
    create s n b k = s k := by simp [create, h]

/-- Replies a provider can give. -/
inductive Reply
  | created
  | alreadyExists
  | lost
  deriving DecidableEq

/-- The provider assumption: "created" only when the slot now holds the
claimant's bytes. "Already exists" and a lost reply are always possible, even
to the claimant whose create stored the bytes. -/
def Permitted (t : Store B) (n : Nat) (own : B) : Reply → Prop
  | .created => t n = some own
  | .alreadyExists => True
  | .lost => True

/-- What a claimant concludes. `retry` resends the same bytes. -/
inductive Outcome
  | won
  | lost
  | retry
  deriving DecidableEq

/-- The claim protocol: a reply, then on anything but "created" a read of the
slot compared with the claimant's own bytes. -/
def conclude [DecidableEq B] (own : B) : Reply → Option B → Outcome
  | .created, _ => .won
  | _, none => .retry
  | _, some b => if b = own then .won else .lost

/-- **Every claimant learns correctly whether it won.** The reply is given at
store `t`, the slot is read at a later `u`, and `w` is any store after that.
`won` means the slot holds the claimant's bytes forever; `lost` means it holds
other bytes forever, so the claimant's own create can never land there. -/
theorem conclude_correct [DecidableEq B] {t u w : Store B} {n : Nat} {own : B} {r : Reply}
    (hr : Permitted t n own r) (htu : Extends t u) (huw : Extends u w) :
    (conclude own r (u n) = .won → w n = some own) ∧
    (conclude own r (u n) = .lost → ∃ b, b ≠ own ∧ w n = some b) := by
  cases r with
  | created =>
    simp only [conclude, Permitted] at hr ⊢
    exact ⟨fun _ => huw n own (htu n own hr), fun h => nomatch h⟩
  | alreadyExists | lost =>
    cases hu : u n with
    | none => simp [conclude]
    | some b =>
      by_cases hb : b = own
      · subst hb; simp [conclude, huw n b hu]
      · refine ⟨fun h => ?_, fun _ => ⟨b, hb, huw n b hu⟩⟩
        simp [conclude, hb] at h

/-- A run of creates from the empty store: each element is a claimant
creating a slot with its attempt's bytes `enc a n`. -/
def runStore {A : Type} (enc : A → Nat → B) : List (Nat × A) → Store B
  | [] => fun _ => none
  | (n, a) :: rest => create (runStore enc rest) n (enc a n)

/-- Every stored slot holds the bytes of a claimant that created that slot. -/
theorem occupant_claimed {A : Type} (enc : A → Nat → B) :
    ∀ (evs : List (Nat × A)) (n : Nat) (b : B), runStore enc evs n = some b →
      ∃ a, (n, a) ∈ evs ∧ b = enc a n
  | [], _, _, h => nomatch h
  | (m, a) :: rest, n, b, h => by
    simp only [runStore, create] at h
    by_cases hn : n = m
    · subst hn
      cases hr : runStore enc rest n with
      | none =>
        simp [hr] at h
        exact ⟨a, List.mem_cons_self, h.symm⟩
      | some x =>
        simp [hr] at h
        subst h
        obtain ⟨a', ha', he⟩ := occupant_claimed enc rest n x hr
        exact ⟨a', List.mem_cons_of_mem _ ha', he⟩
    · simp [hn] at h
      obtain ⟨a', ha', he⟩ := occupant_claimed enc rest n b h
      exact ⟨a', List.mem_cons_of_mem _ ha', he⟩

theorem runStore_extends {A : Type} (enc : A → Nat → B) (e : Nat × A) (evs : List (Nat × A)) :
    Extends (runStore enc evs) (runStore enc (e :: evs)) := by
  obtain ⟨n, a⟩ := e
  exact create_extends _ _ _

/-- **Claim exclusion: every slot has exactly one claimant.** Attempts' bytes
are unique (`enc` injective). An occupied slot holds one claimant's bytes; that
claimant concludes `won` whenever it concludes anything; every other claimant
that concludes anything concludes `lost`. Replies and reads may come from any
stores along the run before the final one. -/
theorem exactly_one_claimant [DecidableEq B] {A : Type} (enc : A → Nat → B)
    (hinj : ∀ a a' n, enc a n = enc a' n → a = a')
    (evs : List (Nat × A)) (n : Nat) (b : B) (hocc : runStore enc evs n = some b) :
    ∃ a, (n, a) ∈ evs ∧ b = enc a n ∧
      ∀ (a' : A) (t u : Store B) (r : Reply),
        Permitted t n (enc a' n) r → Extends t u → Extends u (runStore enc evs) →
        conclude (enc a' n) r (u n) ≠ .retry →
        (conclude (enc a' n) r (u n) = .won ↔ a' = a) := by
  obtain ⟨a, ha, hb⟩ := occupant_claimed enc evs n b hocc
  refine ⟨a, ha, hb, ?_⟩
  intro a' t u r hr htu huw hnr
  have hc := conclude_correct hr htu huw
  constructor
  · intro hw
    have := hc.1 hw
    rw [hocc, hb] at this
    exact hinj _ _ _ (Option.some.inj this).symm
  · intro he
    subst he
    cases hd : conclude (enc a' n) r (u n) with
    | won => rfl
    | retry => exact absurd hd hnr
    | lost =>
      obtain ⟨b', hne, hw⟩ := hc.2 hd
      rw [hocc, hb] at hw
      exact absurd (Option.some.inj hw).symm hne

/-! ## Without the compare step

A protocol that takes "already exists" as a lost claim, and compares only
after a lost reply, is wrong. -/

/-- Treat "already exists" as lost. -/
def decideNoCompare [DecidableEq B] (own : B) : Reply → Option B → Outcome
  | .created, _ => .won
  | .alreadyExists, _ => .lost
  | .lost, none => .retry
  | .lost, some b => if b = own then .won else .lost

/-- **Counterexample without the compare.** Claimant 0 creates slot 1; the
provider stores its bytes and answers "already exists", as it may after a lost
reply. The protocol without the compare concludes `lost`, so claimant 0 claims
slot 2 with the same batch, wins it, and its batch occupies two slots. The
compare protocol concludes `won` at slot 1 and never claims slot 2. Bytes are
`(claimant, slot)`, unique per attempt. -/
theorem no_compare_lands_twice :
    let enc : Nat → Nat → Nat × Nat := fun a n => (a, n)
    let s1 := runStore enc [(1, 0)]
    let s2 := runStore enc [(2, 0), (1, 0)]
    Permitted s1 1 (enc 0 1) .alreadyExists ∧
    decideNoCompare (enc 0 1) .alreadyExists (s1 1) = .lost ∧
    Permitted s2 2 (enc 0 2) .created ∧
    s2 1 = some (0, 1) ∧ s2 2 = some (0, 2) ∧
    conclude (enc 0 1) .alreadyExists (s1 1) = .won := by
  simp [Permitted, decideNoCompare, conclude, runStore, create]

/-! ## Deleted slots and the floor record

Covered slots are deleted, and a create-once path that was deleted is free
again: the provider stores a create there. The floor record names the lowest
retained slot. It is raised before any slot below it is deleted, and never
lowered. `first` is the ghost record of the first bytes ever stored at each
slot, which is the slot's genuine content. -/

structure Hist (B : Type) where
  cur : Store B
  first : Store B
  floor : Nat

/-- Provider and retention steps. -/
inductive HStep : Hist B → Hist B → Prop
  | create (h : Hist B) (n : Nat) (b : B) :
      HStep h { h with cur := create h.cur n b, first := create h.first n b }
  | raiseFloor (h : Hist B) (f : Nat) (hf : h.floor ≤ f) :
      HStep h { h with floor := f }
  | delete (h : Hist B) (n : Nat) (hn : n < h.floor) :
      HStep h { h with cur := fun k => if k = n then none else h.cur k }

inductive HSteps : Hist B → Hist B → Prop
  | refl (h : Hist B) : HSteps h h
  | step {h h' h'' : Hist B} : HStep h h' → HSteps h' h'' → HSteps h h''

/-- What retention keeps true: a stored slot at or above the floor holds its
first bytes, a slot that was ever filled and is now empty lies below the floor,
and the first bytes never change. -/
def HInv (h : Hist B) : Prop :=
  (∀ n b, h.cur n = some b → h.floor ≤ n → h.first n = some b) ∧
  (∀ n, h.cur n = none → h.first n ≠ none → n < h.floor) ∧
  (∀ n, h.cur n ≠ none → h.first n ≠ none)

theorem hinv_start : HInv ({ cur := fun _ => none, first := fun _ => none, floor := 0 } : Hist B) := by
  refine ⟨?_, ?_, ?_⟩ <;> intro n <;> simp

theorem hstep_inv {h h' : Hist B} (s : HStep h h') (hi : HInv h) : HInv h' := by
  obtain ⟨i1, i2, i3⟩ := hi
  cases s with
  | create n b =>
    refine ⟨?_, ?_, ?_⟩
    · intro k x hk hf
      simp only at hk hf ⊢
      by_cases hkn : k = n
      · subst hkn
        cases hc : h.cur k with
        | none =>
          rw [create_self_none b hc] at hk
          cases hk
          cases hfk : h.first k with
          | none => exact create_self_none _ hfk
          | some y => exact absurd (i2 k hc (by simp [hfk])) (by omega)
        | some y =>
          rw [create_self_some b hc] at hk
          cases hk
          exact create_self_some b (i1 k _ hc hf)
      · rw [create_other b hkn] at hk ⊢
        exact i1 k x hk hf
    · intro k hk hfk
      simp only at hk hfk ⊢
      by_cases hkn : k = n
      · subst hkn
        cases hc : h.cur k with
        | none => rw [create_self_none b hc] at hk; cases hk
        | some y => rw [create_self_some b hc] at hk; cases hk
      · rw [create_other b hkn] at hk hfk
        exact i2 k hk hfk
    · intro k hk
      simp only at hk ⊢
      by_cases hkn : k = n
      · subst hkn
        cases hfk : h.first k with
        | none => rw [create_self_none b hfk]; simp
        | some y => rw [create_self_some b hfk]; simp
      · rw [create_other b hkn] at hk ⊢
        exact i3 k hk
  | raiseFloor f hf =>
    refine ⟨?_, ?_, i3⟩
    · intro k x hk hfk; exact i1 k x hk (by simp at hfk; omega)
    · intro k hk hfk; have := i2 k hk hfk; simp; omega
  | delete n hn =>
    refine ⟨?_, ?_, ?_⟩
    · intro k x hk hf
      by_cases hkn : k = n
      · simp [hkn] at hk
      · simp [hkn] at hk; exact i1 k x hk hf
    · intro k hk hfk
      by_cases hkn : k = n
      · subst hkn; exact hn
      · simp [hkn] at hk; exact i2 k hk hfk
    · intro k hk
      by_cases hkn : k = n
      · simp [hkn] at hk
      · simp [hkn] at hk; exact i3 k hk

theorem hstep_floor_mono {h h' : Hist B} (s : HStep h h') : h.floor ≤ h'.floor := by
  cases s <;> simp_all

theorem hstep_first_keeps {h h' : Hist B} (s : HStep h h') :
    ∀ n b, h.first n = some b → h'.first n = some b := by
  cases s with
  | create n b => exact create_extends h.first n b
  | raiseFloor => intro _ _ h; exact h
  | delete => intro _ _ h; exact h

theorem hsteps_inv {h h' : Hist B} (s : HSteps h h') (hi : HInv h) : HInv h' := by
  induction s with
  | refl => exact hi
  | step s _ ih => exact ih (hstep_inv s hi)

theorem hsteps_floor_mono {h h' : Hist B} (s : HSteps h h') : h.floor ≤ h'.floor := by
  induction s with
  | refl => exact Nat.le_refl _
  | step s _ ih => exact Nat.le_trans (hstep_floor_mono s) ih

theorem hsteps_first_keeps {h h' : Hist B} (s : HSteps h h') :
    ∀ n b, h.first n = some b → h'.first n = some b := by
  induction s with
  | refl => exact fun _ _ h => h
  | step s _ ih => exact fun n b hb => ih n b (hstep_first_keeps s n b hb)

/-- **The floor check.** A slot read as `b` at history `h` (a claimant seeing
its own bytes, or a reader seeing a slot), followed by a floor read at a later
`h'` that has not passed the slot, read the slot's genuine first bytes, and they
stay its first bytes. A claimant or reader that sees the floor past the slot
discards what it read. -/
theorem floor_check_sound {h h' : Hist B} (hi : HInv h) (run : HSteps h h')
    {n : Nat} {b : B} (hread : h.cur n = some b) (hfloor : h'.floor ≤ n) :
    h'.first n = some b := by
  have hf : h.floor ≤ n := Nat.le_trans (hsteps_floor_mono run) hfloor
  exact hsteps_first_keeps run n b (hi.1 n b hread hf)

/-- **Counterexample without the floor check.** Slot 1 is claimed with `a`,
covered, the floor raised to 2 and slot 1 deleted. A stale create of slot 1 by
another claimant (sent before it saw the claim, or after a long sleep) is then
stored, and the provider truthfully answers "created". The claimant concludes
`won` for a slot no device at the floor reads, and a reader below the floor
reads its bytes where the genuine content was `a`. -/
theorem stale_create_wins_deleted_slot :
    let h0 : Hist Nat := { cur := fun _ => none, first := fun _ => none, floor := 0 }
    let h1 : Hist Nat := { h0 with cur := create h0.cur 1 7, first := create h0.first 1 7 }
    let h2 : Hist Nat := { h1 with floor := 2 }
    let h3 : Hist Nat := { h2 with cur := fun k => if k = 1 then none else h2.cur k }
    let h4 : Hist Nat := { h3 with cur := create h3.cur 1 9, first := create h3.first 1 9 }
    HStep h0 h1 ∧ HStep h1 h2 ∧ HStep h2 h3 ∧ HStep h3 h4 ∧
    Permitted h4.cur 1 9 .created ∧ conclude 9 .created (h4.cur 1) = .won ∧
    h4.cur 1 = some 9 ∧ h4.first 1 = some 7 ∧ h4.floor = 2 := by
  intro h0 h1 h2 h3 h4
  refine ⟨HStep.create h0 1 7, HStep.raiseFloor h1 2 (by decide),
    HStep.delete h2 1 (by decide), HStep.create h3 1 9, ?_⟩
  simp [h4, h3, h2, h1, h0, Permitted, conclude, create]

end CovenSequence.Claim
