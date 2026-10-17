import CovenSequence.Retention

/-!
# Settling a claim after its slot was deleted

Each device numbers every slot it claims with one counter: batches, admin
entries, key copies, announcements. Before each create, the journal records
the slot and the exact bytes. On reopen, the device reads that slot and
compares the bytes. If the floor has passed the slot, the slot is gone and the
device settles from the floor's snapshot instead: each snapshot records, per
device, the last claim number it includes, and a claim landed exactly when
that number covers it. Admin claims settle by the same rule as batches.

The first part abstracts the claimed sequence to what settlement reads: a
claim's device, its number, an attempt nonce (bytes are unique per attempt),
and a value through which an abstract state is computed. `settle_from_floor`
then reads the record from the ledger in `Model`'s snapshot bodies.
-/

namespace CovenSequence.Settle

/-- A slot as settlement sees it. -/
inductive Rec (Dev : Type) where
  | claim (author : Dev) (number : Nat) (attempt : Nat) (value : Nat)
  | other
  deriving DecidableEq

variable {Dev : Type} [DecidableEq Dev]

/-- The per-device record of a snapshot covering `k`: the largest claim number
of each device's slots `1..k`. -/
def lastClaim (seq : Nat → Rec Dev) (d : Dev) : Nat → Nat
  | 0 => 0
  | k + 1 =>
    match seq (k + 1) with
    | .claim a n _ _ => if a = d then max (lastClaim seq d k) n else lastClaim seq d k
    | .other => lastClaim seq d k

/-- A positive number is covered exactly when some claim of `d` at a slot up
to `k` reaches it. -/
theorem lastClaim_ge (seq : Nat → Rec Dev) (d : Dev) {n : Nat} (hn : 1 ≤ n) :
    ∀ k, n ≤ lastClaim seq d k ↔ ∃ j m t v, 1 ≤ j ∧ j ≤ k ∧ seq j = .claim d m t v ∧ n ≤ m
  | 0 => by
    simp only [lastClaim]
    constructor
    · intro h; omega
    · rintro ⟨j, _, _, _, h1, h2, _⟩; omega
  | k + 1 => by
    have ih := lastClaim_ge seq d hn k
    simp only [lastClaim]
    cases hs : seq (k + 1) with
    | other =>
      simp only
      rw [ih]
      constructor
      · rintro ⟨j, m, t, v, h1, h2, h3, h4⟩; exact ⟨j, m, t, v, h1, by omega, h3, h4⟩
      · rintro ⟨j, m, t, v, h1, h2, h3, h4⟩
        by_cases hj : j = k + 1
        · subst hj; rw [hs] at h3; cases h3
        · exact ⟨j, m, t, v, h1, by omega, h3, h4⟩
    | claim a m t v =>
      simp only
      by_cases ha : a = d
      · subst ha
        simp only [ite_true]
        constructor
        · intro h
          by_cases hm : n ≤ m
          · exact ⟨k + 1, m, t, v, by omega, Nat.le_refl _, hs, hm⟩
          · have : n ≤ lastClaim seq a k := by omega
            obtain ⟨j, m', t', v', h1, h2, h3, h4⟩ := ih.1 this
            exact ⟨j, m', t', v', h1, by omega, h3, h4⟩
        · rintro ⟨j, m', t', v', h1, h2, h3, h4⟩
          by_cases hj : j = k + 1
          · subst hj; rw [hs] at h3; cases h3; omega
          · have := ih.2 ⟨j, m', t', v', h1, by omega, h3, h4⟩; omega
      · simp only [ha, ite_false]
        rw [ih]
        constructor
        · rintro ⟨j, m', t', v', h1, h2, h3, h4⟩; exact ⟨j, m', t', v', h1, by omega, h3, h4⟩
        · rintro ⟨j, m', t', v', h1, h2, h3, h4⟩
          by_cases hj : j = k + 1
          · subst hj; rw [hs] at h3; cases h3; exact absurd rfl ha
          · exact ⟨j, m', t', v', h1, by omega, h3, h4⟩

/-- What settlement concludes. `retry` resends the same bytes. -/
inductive Verdict where
  | landed
  | notLanded
  | retry
  deriving DecidableEq

/-- Settle a recorded claim of slot `i` with bytes `own`, from the floor's first
kept slot `start`, the floor snapshot's record for this device `covered`, and
what reading slot `i` returned. -/
def settle (start covered : Nat) (i : Nat) (own : Rec Dev) (n : Nat) (read : Option (Rec Dev)) : Verdict :=
  if start ≤ i then
    match read with
    | some x => if x = own then .landed else .notLanded
    | none => .retry
  else if n ≤ covered then .landed else .notLanded

/-- What the device knows of its own claim numbered `n`: its claims carry
numbers up to `n` (the journal holds its latest claim), and only the recorded
attempt `(i, t)` of claim `n` can be in the sequence, because every earlier
attempt was settled as not landed. -/
structure Own (seq : Nat → Rec Dev) (head : Nat) (d : Dev) (n i t v : Nat) : Prop where
  pos : 1 ≤ n
  slot_pos : 1 ≤ i
  numbers_le : ∀ j m t' v', 1 ≤ j → j ≤ head → seq j = .claim d m t' v' → m ≤ n
  only_here : ∀ j t' v', 1 ≤ j → j ≤ head → seq j = .claim d n t' v' → j = i ∧ t' = t ∧ v' = v

/-- **Settlement is correct.** The floor's snapshot covers `c ≥ start - 1` and
records `lastClaim` as of `c`. A read of a kept slot returns the genuine slot,
or nothing above the head (`Claim.floor_check_sound`). Then settlement says
`landed` exactly when the claim is in the sequence; `notLanded` means it is
nowhere in it, so claiming it again lands it once; `retry` means its slot is
still empty. -/
theorem settle_correct {seq : Nat → Rec Dev} {head start c : Nat} {d : Dev} {n i t v : Nat}
    (own : Own seq head d n i t v) (hsc : start ≤ c + 1) (hch : c ≤ head)
    {read : Option (Rec Dev)} (hread : start ≤ i → read = if i ≤ head then some (seq i) else none) :
    (settle start (lastClaim seq d c) i (.claim d n t v) n read = .landed ↔
      i ≤ head ∧ seq i = .claim d n t v) ∧
    (settle start (lastClaim seq d c) i (.claim d n t v) n read = .notLanded →
      ∀ j t' v', 1 ≤ j → j ≤ head → seq j ≠ .claim d n t' v') ∧
    (settle start (lastClaim seq d c) i (.claim d n t v) n read = .retry → head < i) := by
  have nowhere : ¬ (i ≤ head ∧ seq i = .claim d n t v) → ∀ j t' v', 1 ≤ j → j ≤ head →
      seq j ≠ .claim d n t' v' := by
    intro hnot j t' v' h1 h2 hj
    obtain ⟨rfl, rfl, rfl⟩ := own.only_here j t' v' h1 h2 hj
    exact hnot ⟨h2, hj⟩
  by_cases hk : start ≤ i
  · have hr := hread hk
    by_cases hih : i ≤ head
    · simp only [hih, ite_true] at hr
      subst hr
      by_cases he : seq i = .claim d n t v
      · have hv : settle start (lastClaim seq d c) i (.claim d n t v) n (some (seq i)) = .landed := by
          simp [settle, hk, he]
        rw [hv]
        exact ⟨⟨fun _ => ⟨hih, he⟩, fun _ => rfl⟩, fun h => (by cases h), fun h => (by cases h)⟩
      · have hv : settle start (lastClaim seq d c) i (.claim d n t v) n (some (seq i)) = .notLanded := by
          simp [settle, hk, he]
        rw [hv]
        exact ⟨⟨fun h => (by cases h), fun h => absurd h.2 he⟩, fun _ => nowhere (fun h => he h.2),
          fun h => (by cases h)⟩
    · simp only [hih, ite_false] at hr
      subst hr
      have hv : settle start (lastClaim seq d c) i (.claim d n t v) n none = .retry := by
        simp [settle, hk]
      rw [hv]
      exact ⟨⟨fun h => (by cases h), fun h => absurd h.1 hih⟩, fun h => (by cases h), fun _ => by omega⟩
  · have hic : i ≤ c := by omega
    have hcov := lastClaim_ge seq d own.pos c
    by_cases hn : n ≤ lastClaim seq d c
    · obtain ⟨j, m, t', v', h1, h2, h3, h4⟩ := hcov.1 hn
      have hm := own.numbers_le j m t' v' h1 (by omega) h3
      have : m = n := by omega
      subst this
      obtain ⟨rfl, rfl, rfl⟩ := own.only_here j t' v' h1 (by omega) h3
      have hv : settle start (lastClaim seq d c) j (.claim d m t' v') m read = .landed := by
        simp [settle, hk, hn]
      rw [hv]
      exact ⟨⟨fun _ => ⟨by omega, h3⟩, fun _ => rfl⟩, fun h => (by cases h), fun h => (by cases h)⟩
    · have hv : settle start (lastClaim seq d c) i (.claim d n t v) n read = .notLanded := by
        simp [settle, hk, hn]
      rw [hv]
      have hnot : ¬ (i ≤ head ∧ seq i = .claim d n t v) := fun ⟨_, he⟩ =>
        hn (hcov.2 ⟨i, n, t, v, own.slot_pos, hic, he, Nat.le_refl _⟩)
      exact ⟨⟨fun h => (by cases h), fun h => absurd h hnot⟩, fun _ => nowhere hnot, fun h => (by cases h)⟩

/-- **A claim lands at most once and is never lost.** After settlement, a
device claims again (with a new attempt) exactly when settlement says it did
not land; once that claim lands at the next slot, it is in the sequence at
exactly one slot. For a batch, its writes land once; for an admin entry, its
change lands once. -/
theorem lands_once_never_lost {seq : Nat → Rec Dev} {head start c : Nat} {d : Dev} {n i t v : Nat}
    (own : Own seq head d n i t v) (hsc : start ≤ c + 1) (hch : c ≤ head) (hih : i ≤ head)
    {read : Option (Rec Dev)} (hread : start ≤ i → read = some (seq i)) (t₂ : Nat) :
    let verdict := settle start (lastClaim seq d c) i (.claim d n t v) n read
    let seq' : Nat → Rec Dev := fun j =>
      if j = head + 1 ∧ verdict = .notLanded then .claim d n t₂ v else seq j
    let head' := if verdict = .notLanded then head + 1 else head
    ∃ j, 1 ≤ j ∧ j ≤ head' ∧ (∃ t' v', seq' j = .claim d n t' v') ∧
      ∀ j', 1 ≤ j' → j' ≤ head' → (∃ t' v', seq' j' = .claim d n t' v') → j' = j := by
  intro verdict seq' head'
  have hc := settle_correct own hsc hch (read := read) (fun h => by simp [hih, hread h])
  have hret : verdict ≠ .retry := fun h => by have := hc.2.2 h; omega
  by_cases hv : verdict = .notLanded
  · have hnone := hc.2.1 hv
    refine ⟨head + 1, by omega, by simp [head', hv], ⟨t₂, v, by simp [seq', hv]⟩, ?_⟩
    rintro j' h1 h2 ⟨t', v', he⟩
    simp only [head', hv, ite_true] at h2
    by_cases hj : j' = head + 1
    · exact hj
    · simp only [seq', hj, false_and, ite_false] at he
      exact absurd he (hnone j' t' v' h1 (by omega))
  · have hl : verdict = .landed := by
      cases h : verdict
      · rfl
      · exact absurd h hv
      · exact absurd h hret
    obtain ⟨_, hseq⟩ := hc.1.1 hl
    refine ⟨i, own.slot_pos, by simp [head', hv, hih], ⟨t, v, by simp [seq', hv, hseq]⟩, ?_⟩
    rintro j' h1 h2 ⟨t', v', he⟩
    simp only [head', hv, ite_false] at h2
    simp only [seq', hv, and_false, ite_false] at he
    exact (own.only_here j' t' v' h1 h2 he).1

/-! ## Settling from the state alone -/

/-- An abstract state of the rows: the value of the last claim. A later one
overwrites an earlier one, as a later write to the same cell does. -/
def lastValue (seq : Nat → Rec Dev) : Nat → Nat
  | 0 => 0
  | k + 1 =>
    match seq (k + 1) with
    | .claim _ _ _ v => v
    | .other => lastValue seq k

/-- Device `true` claimed slot 1 and later device `false` overwrote its value. -/
def seqLanded : Nat → Rec Bool
  | 1 => .claim true 1 0 5
  | 2 => .claim false 1 0 7
  | _ => .other

/-- Device `false` took slot 1, then overwrote its own value. -/
def seqLost : Nat → Rec Bool
  | 1 => .claim false 1 0 3
  | 2 => .claim false 2 0 7
  | _ => .other

/-- **Counterexample: settling from the state alone.** Device `true` recorded a
claim of slot 1 with its batch 1. In one history it landed and was overwritten;
in the other another device took slot 1. Once the floor has passed slot 2, the
snapshot's state is the same in both, so any rule that settles from the state
answers wrongly in one of them: it claims again a batch that landed (its writes
land twice) or drops one that did not (its writes are lost). The per-device
record tells them apart. -/
theorem state_alone_cannot_settle :
    lastValue seqLanded 2 = lastValue seqLost 2 ∧
    seqLanded 1 = .claim true 1 0 5 ∧ seqLost 1 ≠ .claim true 1 0 5 ∧
    (∀ f : Nat → Bool, f (lastValue seqLanded 2) ≠ true ∨ f (lastValue seqLost 2) ≠ false) ∧
    lastClaim seqLanded true 2 = 1 ∧ lastClaim seqLost true 2 = 0 := by
  refine ⟨rfl, rfl, by decide, fun f => ?_, rfl, rfl⟩
  show f 7 ≠ true ∨ f 7 ≠ false
  cases f 7 <;> simp

/-! ## Admin claims settled from the state

An admin claim settled by whether its change is present in the snapshot's
state has the same problem: a later entry can undo the change. -/

open CovenSequence in
/-- Two devices of one admin member; member `false` is added then removed. -/
def adminSig : Sig where
  Dev := Bool
  Mem := Bool
  Key := Unit
  Path := Unit
  Batch := Unit
  Rows := Unit
  Op := Unit
  Aux := Unit
  Aud := Unit

open CovenSequence in
def adminRules : Rules adminSig where
  authorized := fun _ _ _ => true
  applyOp := fun a _ => a
  copyOk := fun _ _ _ _ => true
  mayWrite := fun _ _ _ => true
  merge := fun r _ => r
  convert := fun _ _ b => b
  migrate := fun _ r => r
  number := fun
    | .admin true _ => 1
    | .admin false (.addMember _) => 1
    | .admin false _ => 2
    | _ => 0
  ledger0 := ⟨fun m => m, fun _ => some true, fun _ => false, fun _ => 0, (), [], fun _ => false,
    fun _ => 0⟩
  aud0 := fun _ => ⟨fun _ => none, [], 0, 0, 0, fun _ => false⟩
  rows0 := fun _ => ()

open CovenSequence in
/-- Device `true` adds member `false`; device `false` removes it. -/
def adminLanded : Nat → Slot adminSig
  | 1 => .admin true (.addMember false)
  | 2 => .admin false (.removeMember false)
  | _ => .admin false (.other ())

open CovenSequence in
/-- Device `false` adds member `false` and removes it. -/
def adminLost : Nat → Slot adminSig
  | 1 => .admin false (.addMember false)
  | 2 => .admin false (.removeMember false)
  | _ => .admin false (.other ())

open CovenSequence in
/-- **Counterexample: settling an admin claim from the state.** Device `true`
recorded its claim 1, of slot 1, adding member `false`. Whether it landed or
another device's identical change took the slot, the ledger after slot 2 is the
same apart from the claim numbers: the change was undone. Settling by presence
says "not landed" in both, so in the first history the device adds the member
again, against the removal. The claim numbers tell the two apart: device
`true`'s last claim is 1 in the first and none in the second. -/
theorem admin_state_cannot_settle :
    { ledgerAt adminRules adminLanded 2 with claims := fun _ => 0 } =
      { ledgerAt adminRules adminLost 2 with claims := fun _ => 0 } ∧
    (ledgerAt adminRules adminLanded 2).claims true = 1 ∧
    (ledgerAt adminRules adminLost 2).claims true = 0 ∧
    (ledgerAt adminRules adminLanded 2).member false = false ∧
    adminLanded 1 = .admin true (.addMember false) ∧
    adminLost 1 ≠ .admin true (.addMember false) := by
  refine ⟨rfl, rfl, rfl, rfl, rfl, fun h => ?_⟩
  simp only [adminLost] at h
  injection h with h1
  cases h1

/-! ## Settling from `Model`'s floor snapshot -/

section Compose

variable {S : Sig} (R : Rules S)

theorem step_claims (L : Ledger S) (k : Nat) (x : Slot S) (e : S.Dev) :
    (ledgerStep R L k x).claims e = if e = author x then max (L.claims e) (R.number x) else L.claims e := by
  have hin : (if ledgerValid R L x then ledgerEffect R L k x else refuse L k).claims = L.claims := by
    split
    · cases x with
      | admin a e => cases e <;> rfl
      | _ => rfl
    · rfl
  simp only [ledgerStep, noteClaim, hin]

/-- The ledger's record covers a positive number exactly when some slot of the
device up to `k` carries a claim number reaching it. -/
theorem claims_ge (seq : Nat → Slot S) (h0 : ∀ e, R.ledger0.claims e = 0) (d : S.Dev) {n : Nat}
    (hn : 1 ≤ n) :
    ∀ k, n ≤ (ledgerAt R seq k).claims d ↔ ∃ j, 1 ≤ j ∧ j ≤ k ∧ author (seq j) = d ∧ n ≤ R.number (seq j)
  | 0 => by
    simp only [ledgerAt, h0]
    constructor
    · intro h; omega
    · rintro ⟨j, h1, h2, _⟩; omega
  | k + 1 => by
    have ih := claims_ge seq h0 d hn k
    rw [ledgerAt_succ, step_claims]
    by_cases ha : d = author (seq (k + 1))
    · simp only [ha, ite_true]
      rw [← ha]
      constructor
      · intro h
        by_cases hm : n ≤ R.number (seq (k + 1))
        · exact ⟨k + 1, by omega, Nat.le_refl _, ha.symm, hm⟩
        · obtain ⟨j, h1, h2, h3, h4⟩ := ih.1 (by omega)
          exact ⟨j, h1, by omega, h3, h4⟩
      · rintro ⟨j, h1, h2, h3, h4⟩
        by_cases hj : j = k + 1
        · subst hj; omega
        · have := ih.2 ⟨j, h1, by omega, h3, h4⟩; omega
    · simp only [ha, ite_false]
      rw [ih]
      constructor
      · rintro ⟨j, h1, h2, h3, h4⟩; exact ⟨j, h1, by omega, h3, h4⟩
      · rintro ⟨j, h1, h2, h3, h4⟩
        by_cases hj : j = k + 1
        · subst hj; exact absurd h3.symm ha
        · exact ⟨j, h1, by omega, h3, h4⟩

/-- **Settling from the floor snapshot.** A device's journal recorded slot `i`
with bytes `x`, its claim number `n`, and `x` is its latest claim: its slots
carry numbers up to `n`, and only slot `i` can hold claim `n` (every earlier
attempt was settled as not landed). If the floor has passed `i`, the floor's
snapshot body for any audience records `n` for the device exactly when slot `i`
holds `x`. With `claim_floor_check` for a kept slot, every claim settles
correctly, admin entries included. -/
theorem settle_from_floor {seq : Nat → Slot S} {st : Store S} (g : Good R st seq)
    (h0 : ∀ e, R.ledger0.claims e = 0) (A : S.Aud) {d : S.Dev} {n i : Nat} {x : Slot S}
    (hxa : author x = d) (hxn : R.number x = n) (hn : 1 ≤ n) (hi : 1 ≤ i)
    (hord : ∀ j, 1 ≤ j → j ≤ st.head → author (seq j) = d → R.number (seq j) ≤ n)
    (honly : ∀ j, 1 ≤ j → j ≤ st.head → author (seq j) = d → R.number (seq j) = n → j = i ∧ seq j = x)
    (hdel : i < st.floor.start) {L : Ledger S} {P : AudLedger S} {rows : S.Rows}
    (hb : st.body (st.floor.path A) = some (L, P, rows)) :
    n ≤ L.claims d ↔ (i ≤ st.head ∧ seq i = x) := by
  rw [g.floor_body A] at hb
  have hL : L = ledgerAt R seq (st.floor.covers A) := by
    have := Option.some.inj hb
    simp only [stateAt, Prod.mk.injEq] at this
    exact this.1.symm
  subst hL
  have hic : i ≤ st.floor.covers A := by have := g.start_le A; omega
  have hch := g.covers_le A
  rw [claims_ge R seq h0 d hn]
  constructor
  · rintro ⟨j, h1, h2, h3, h4⟩
    have hle := hord j h1 (by omega) h3
    obtain ⟨rfl, hx⟩ := honly j h1 (by omega) h3 (by omega)
    exact ⟨by omega, hx⟩
  · rintro ⟨hih, hx⟩
    exact ⟨i, hi, hic, by rw [hx, hxa], by rw [hx, hxn]; exact Nat.le_refl _⟩

end Compose

/-! ## Another copy using this device id -/

/-- Storage and each running copy's journal of attempted creates. Copies are
instances; `idOf` is the device id each signs with. -/
structure CWorld (Inst : Type) where
  slots : Claim.Store (Dev × Nat)
  journal : Inst → List (Nat × Nat)

variable {Inst : Type} [DecidableEq Inst]

/-- A copy records an attempt before sending it, and creates only what it
recorded, signed with its id. -/
inductive CStep (idOf : Inst → Dev) : CWorld (Dev := Dev) Inst → CWorld (Dev := Dev) Inst → Prop
  | record (w : CWorld (Dev := Dev) Inst) (ι : Inst) (i b : Nat) :
      CStep idOf w { w with journal := fun κ => if κ = ι then (i, b) :: w.journal ι else w.journal κ }
  | create (w : CWorld (Dev := Dev) Inst) (ι : Inst) (i b : Nat) (h : (i, b) ∈ w.journal ι) :
      CStep idOf w { w with slots := Claim.create w.slots i (idOf ι, b) }

inductive CSteps (idOf : Inst → Dev) : CWorld (Dev := Dev) Inst → CWorld (Dev := Dev) Inst → Prop
  | refl (w : CWorld (Dev := Dev) Inst) : CSteps idOf w w
  | step {w w' w'' : CWorld (Dev := Dev) Inst} : CStep idOf w w' → CSteps idOf w' w'' → CSteps idOf w w''

/-- Every stored slot was recorded by a copy using its signing id. -/
def Signed (idOf : Inst → Dev) (w : CWorld (Dev := Dev) Inst) : Prop :=
  ∀ i d b, w.slots i = some (d, b) → ∃ ι, idOf ι = d ∧ (i, b) ∈ w.journal ι

omit [DecidableEq Dev] in
theorem cstep_signed (idOf : Inst → Dev) {w w' : CWorld (Dev := Dev) Inst}
    (s : CStep idOf w w') (h : Signed idOf w) : Signed idOf w' := by
  cases s with
  | record ι i b =>
    intro j d c hj
    obtain ⟨κ, hκ, hm⟩ := h j d c hj
    refine ⟨κ, hκ, ?_⟩
    simp only
    by_cases hk : κ = ι
    · subst hk; simp [hm]
    · simp [hk, hm]
  | create ι i b hb =>
    intro j d c hj
    simp only at hj
    by_cases hji : j = i
    · subst hji
      revert hj
      cases hs : w.slots j with
      | none =>
        intro hj
        rw [Claim.create_self_none _ hs] at hj
        cases hj
        exact ⟨ι, rfl, hb⟩
      | some y =>
        intro hj
        rw [Claim.create_self_some _ hs] at hj
        exact h j d c (hs.trans hj)
    · rw [Claim.create_other _ hji] at hj
      exact h j d c hj

omit [DecidableEq Dev] in
theorem csteps_signed (idOf : Inst → Dev) {w w' : CWorld (Dev := Dev) Inst}
    (run : CSteps idOf w w') (h : Signed idOf w) : Signed idOf w' := by
  induction run with
  | refl => exact h
  | step s _ ih => exact ih (cstep_signed idOf s h)

omit [DecidableEq Dev] in
/-- **Another copy is detected by a slot its journal never attempted.** From
empty storage, a slot signed with copy `ι`'s id whose bytes `ι`'s journal never
attempted was created by another copy using the same id. -/
theorem foreign_slot_means_copy (idOf : Inst → Dev) (j0 : Inst → List (Nat × Nat))
    {w : CWorld (Dev := Dev) Inst} (run : CSteps idOf ⟨fun _ => none, j0⟩ w)
    {ι : Inst} {i b : Nat} (hs : w.slots i = some (idOf ι, b)) (hnot : (i, b) ∉ w.journal ι) :
    ∃ κ, κ ≠ ι ∧ idOf κ = idOf ι := by
  obtain ⟨κ, hκ, hm⟩ := csteps_signed idOf run (fun _ _ _ h => by simp at h) i _ b hs
  refine ⟨κ, fun he => ?_, hκ⟩
  subst he
  exact hnot hm

end CovenSequence.Settle
