import CovenSequence.Retention

/-! Settlement compares complete kept bytes, or the floor's per-device claim
number and SHA-256 digest. Copies may reuse an id and its claim numbers.
`Own` is used only for the single-copy result. Hash matching and path binding
are explicit premises, separate from claim-number uniqueness. -/
namespace CovenSequence.Settle

inductive Rec (Dev : Type) where
  | claim (author : Dev) (number : Nat) (attempt : Nat) (value : Nat)
  | other
  deriving DecidableEq

variable {Dev H : Type} [DecidableEq Dev]

/-- The slot's authenticated number and digest, including refused claims. -/
def record (hash : Nat → Rec Dev → H) (i : Nat) : Rec Dev → Option (Dev × ClaimRecord H)
  | x@(.claim d n _ _) => some (d, ⟨n, hash i x⟩)
  | .other => none

def lastClaim (seq : Nat → Rec Dev) (hash : Nat → Rec Dev → H) (d : Dev) : Nat → Option (ClaimRecord H) :=
  Claims.last (fun j => record hash j (seq j)) d

/-- No uniqueness of claim numbers is needed for either the bound or its witness. -/
theorem lastClaim_ge (seq : Nat → Rec Dev) (hash : Nat → Rec Dev → H) (d : Dev)
    {n : Nat} (hn : 1 ≤ n) (k : Nat) :
    n ≤ ClaimRecord.numberOf (lastClaim seq hash d k) ↔
      ∃ j m t v, 1 ≤ j ∧ j ≤ k ∧ seq j = .claim d m t v ∧ n ≤ m := by
  rw [lastClaim, Claims.ge _ _ hn]
  constructor
  · rintro ⟨j, r, h1, h2, hj, hr⟩
    cases hs : seq j with
    | other => simp [record, hs] at hj
    | claim a m t v =>
      simp [record, hs] at hj
      obtain ⟨rfl, rfl⟩ := hj
      exact ⟨j, m, t, v, h1, h2, hs, hr⟩
  · rintro ⟨j, m, t, v, h1, h2, hj, hr⟩
    exact ⟨j, ⟨m, hash j (seq j)⟩, h1, h2, by simp [record, hj], hr⟩

theorem lastClaim_witness (seq : Nat → Rec Dev) (hash : Nat → Rec Dev → H) (d : Dev)
    (k : Nat) {r : ClaimRecord H} (h : lastClaim seq hash d k = some r) :
    ∃ j t v, 1 ≤ j ∧ j ≤ k ∧ seq j = .claim d r.number t v ∧ hash j (seq j) = r.digest := by
  obtain ⟨j, h1, h2, hj⟩ := Claims.witness _ d k r h
  cases hs : seq j with
  | other => simp [record, hs] at hj
  | claim a m t v =>
    simp [record, hs] at hj
    obtain ⟨rfl, rfl⟩ := hj
    exact ⟨j, t, v, h1, h2, hs, by rw [hs]⟩

variable [DecidableEq H]

inductive Verdict where
  | landed
  | notLanded
  | retry
  | copy
  deriving DecidableEq

/-- The floor record takes precedence over any bytes recreated below it. -/
def settleFloor (covered : Option (ClaimRecord H)) (n : Nat) (digest : H) : Verdict :=
  match covered with
  | none => .notLanded
  | some r =>
    if n < r.number then .copy
    else if r.number = n then (if r.digest = digest then .landed else .copy)
    else .notLanded

/-- `another` checks authenticated device id and claim number in the kept slot.
The complete-byte comparison always comes first. -/
def settle {Bytes : Type} [DecidableEq Bytes] (start : Nat) (covered : Option (ClaimRecord H))
    (i : Nat) (own : Bytes) (n : Nat) (digest : H) (another : Bytes → Bool)
    (read : Option Bytes) : Verdict :=
  if start ≤ i then
    match read with
    | some x => if x = own then .landed else if another x then .copy else .notLanded
    | none => .retry
  else settleFloor covered n digest

/-- A different slot at or above the current claim number cannot come from
this copy, which has settled its earlier claims and has only this one in flight. -/
def anotherClaim (d : Dev) (n : Nat) : Rec Dev → Bool
  | .claim a m _ _ => decide (a = d ∧ n ≤ m)
  | .other => false

@[simp] theorem floor_landed (covered : Option (ClaimRecord H)) (n : Nat) (h : H) :
    settleFloor covered n h = .landed ↔ covered = some ⟨n, h⟩ := by
  cases covered with
  | none => simp [settleFloor]
  | some r =>
    obtain ⟨m, g⟩ := r
    by_cases hn : n < m <;> by_cases he : m = n <;> by_cases hh : g = h <;>
      simp_all [settleFloor] <;> split <;> decide

@[simp] theorem floor_notLanded (covered : Option (ClaimRecord H)) (n : Nat) (h : H)
    (hn : 1 ≤ n) :
    settleFloor covered n h = .notLanded ↔ ClaimRecord.numberOf covered < n := by
  cases covered with
  | none => simp [settleFloor, ClaimRecord.numberOf]; omega
  | some r =>
    by_cases hl : n < r.number <;> by_cases he : r.number = n <;>
      by_cases hh : r.digest = h <;> simp_all [settleFloor, ClaimRecord.numberOf] <;> omega

@[simp] theorem floor_copy (covered : Option (ClaimRecord H)) (n : Nat) (h : H) :
    settleFloor covered n h = .copy ↔
      ∃ r, covered = some r ∧ (n < r.number ∨ (r.number = n ∧ r.digest ≠ h)) := by
  cases covered with
  | none => simp [settleFloor]
  | some r =>
    by_cases hl : n < r.number <;> by_cases he : r.number = n <;>
      by_cases hh : r.digest = h <;> simp_all [settleFloor]

@[simp] theorem floor_not_retry (covered : Option (ClaimRecord H)) (n : Nat) (h : H) :
    settleFloor covered n h ≠ .retry := by
  cases covered <;> simp only [settleFloor]
  · decide
  · split <;> first | decide | (split <;> first | decide | (split <;> decide))

/-- Evidence visible to settlement. Kept slots are checked byte-for-byte;
deleted slots use only the authenticated floor record. -/
def CopyEvidence (seq : Nat → Rec Dev) (hash : Nat → Rec Dev → H)
    (head start c : Nat) (d : Dev) (n i t v : Nat) : Prop :=
  if start ≤ i then
    i ≤ head ∧ seq i ≠ .claim d n t v ∧ anotherClaim d n (seq i) = true
  else ∃ r, lastClaim seq hash d c = some r ∧
    (n < r.number ∨ (r.number = n ∧ r.digest ≠ hash i (.claim d n t v)))

/-- **Settlement with any number of copies.** No claim-number uniqueness is
assumed. Digest equality identifies the recorded complete bytes and their path
(the cryptographic and encoding boundary). `landed` never confirms other bytes;
`notLanded` excludes this attempt; `retry` is exactly an empty slot; `copy` is
exactly the evidence of another claim at or above n visible in the chosen source. -/
theorem settle_correct' {seq : Nat → Rec Dev} {head start c : Nat} {d : Dev} {n i t v : Nat}
    (hash : Nat → Rec Dev → H) (hn : 1 ≤ n) (hi : 1 ≤ i)
    (hsc : start ≤ c + 1) (hch : c ≤ head)
    (hhash : ∀ j m t' v', 1 ≤ j → j ≤ head → seq j = .claim d m t' v' →
      hash j (seq j) = hash i (.claim d n t v) → j = i ∧ seq j = .claim d n t v)
    {read : Option (Rec Dev)} (hread : start ≤ i → read = if i ≤ head then some (seq i) else none) :
    let verdict := settle start (lastClaim seq hash d c) i (.claim d n t v) n
      (hash i (.claim d n t v)) (anotherClaim d n) read
    (verdict = .landed → i ≤ head ∧ seq i = .claim d n t v) ∧
    (verdict = .notLanded → ¬ (i ≤ head ∧ seq i = .claim d n t v)) ∧
    (verdict = .retry ↔ head < i) ∧
    (verdict = .copy ↔ CopyEvidence seq hash head start c d n i t v) := by
  dsimp only
  by_cases hk : start ≤ i
  · simp only [settle, hk, ite_true, hread hk]
    by_cases hih : i ≤ head
    · simp only [hih, ite_true]
      by_cases he : seq i = .claim d n t v
      · simp [he, hih, CopyEvidence, hk]
      · by_cases ha : anotherClaim d n (seq i) = true <;>
          simp [he, ha, hih, CopyEvidence, hk] <;> omega
    · simp [hih, CopyEvidence, hk]
      omega
  · simp only [settle, hk, ite_false]
    have hic : i ≤ c := by omega
    refine ⟨?_, ?_, ?_, ?_⟩
    · intro hl
      have hr := (floor_landed _ _ _).1 hl
      obtain ⟨j, t', v', h1, h2, hj, hh⟩ := lastClaim_witness seq hash d c hr
      obtain ⟨rfl, he⟩ := hhash j n t' v' h1 (by omega) hj hh
      exact ⟨by omega, he⟩
    · intro hl ⟨_, he⟩
      have hlt := (floor_notLanded _ _ _ hn).1 hl
      have hge := (lastClaim_ge seq hash d hn c).2 ⟨i, n, t, v, hi, hic, he, by omega⟩
      omega
    · simp only [floor_not_retry, false_iff]
      omega
    · simp only [CopyEvidence, hk, ite_false, floor_copy]

/-- The additional facts true of a single copy. These are deliberately absent
from `settle_correct'`: other copies may exceed n or reuse it with different bytes. -/
structure Own (seq : Nat → Rec Dev) (head : Nat) (d : Dev) (n i t v : Nat) : Prop where
  pos : 1 ≤ n
  slot_pos : 1 ≤ i
  numbers_le : ∀ j m t' v', 1 ≤ j → j ≤ head → seq j = .claim d m t' v' → m ≤ n
  only_here : ∀ j t' v', 1 ≤ j → j ≤ head → seq j = .claim d n t' v' → j = i ∧ t' = t ∧ v' = v

/-- With one copy, settlement identifies a landed, absent or pending claim; copy is impossible. -/
theorem settle_correct {seq : Nat → Rec Dev} {head start c : Nat} {d : Dev} {n i t v : Nat}
    (hash : Nat → Rec Dev → H) (own : Own seq head d n i t v)
    (hsc : start ≤ c + 1) (hch : c ≤ head)
    {read : Option (Rec Dev)} (hread : start ≤ i → read = if i ≤ head then some (seq i) else none) :
    let verdict := settle start (lastClaim seq hash d c) i (.claim d n t v) n
      (hash i (.claim d n t v)) (anotherClaim d n) read
    (verdict = .landed ↔ i ≤ head ∧ seq i = .claim d n t v) ∧
    (verdict = .notLanded → ∀ j t' v', 1 ≤ j → j ≤ head → seq j ≠ .claim d n t' v') ∧
    (verdict = .retry → head < i) ∧ verdict ≠ .copy := by
  have nowhere : ¬ (i ≤ head ∧ seq i = .claim d n t v) →
      ∀ j t' v', 1 ≤ j → j ≤ head → seq j ≠ .claim d n t' v' := by
    intro hnot j t' v' h1 h2 hj
    obtain ⟨rfl, rfl, rfl⟩ := own.only_here j t' v' h1 h2 hj
    exact hnot ⟨h2, hj⟩
  dsimp only
  by_cases hk : start ≤ i
  · simp only [settle, hk, ite_true, hread hk]
    by_cases hih : i ≤ head
    · simp only [hih, ite_true]
      by_cases he : seq i = .claim d n t v
      · simp [he]
      · have ha : anotherClaim d n (seq i) = false := by
          cases hs : seq i with
          | other => rfl
          | claim a m t' v' =>
            simp only [anotherClaim, decide_eq_false_iff_not]
            rintro ⟨rfl, hm⟩
            have hle := own.numbers_le i m t' v' own.slot_pos hih hs
            have hmn : m = n := by omega
            subst m
            obtain ⟨_, rfl, rfl⟩ := own.only_here i t' v' own.slot_pos hih hs
            exact he hs
        simp only [he, ite_false, ha, Bool.false_eq_true]
        exact ⟨by simp, fun _ => nowhere (fun h => he h.2), by simp, by decide⟩
    · simp [hih]
      omega
  · simp only [settle, hk, ite_false]
    have hic : i ≤ c := by omega
    by_cases hcov : n ≤ ClaimRecord.numberOf (lastClaim seq hash d c)
    · cases hr : lastClaim seq hash d c with
      | none => simp [hr, ClaimRecord.numberOf] at hcov; have := own.pos; omega
      | some r =>
        obtain ⟨j, t', v', h1, h2, hj, hh⟩ := lastClaim_witness seq hash d c hr
        have hle := own.numbers_le j r.number t' v' h1 (by omega) hj
        have hn : r.number = n := by simp [hr, ClaimRecord.numberOf] at hcov; omega
        rw [hn] at hj
        obtain ⟨hji, ht, hv⟩ := own.only_here j t' v' h1 (by omega) hj
        subst j; subst t'; subst v'
        have he : r = ⟨n, hash i (.claim d n t v)⟩ := by
          cases r; simp_all
        rw [he]
        simp [settleFloor, hj, show i ≤ head by omega]
    · have hv : settleFloor (lastClaim seq hash d c) n (hash i (.claim d n t v)) = .notLanded :=
        (floor_notLanded _ _ _ own.pos).2 (by omega)
      rw [hv]
      have hnot : ¬ (i ≤ head ∧ seq i = .claim d n t v) := fun ⟨_, he⟩ =>
        hcov ((lastClaim_ge seq hash d own.pos c).2 ⟨i, n, t, v, own.slot_pos, hic, he, by omega⟩)
      exact ⟨by simp [hnot], fun _ => nowhere hnot, by simp, by decide⟩

/-- **A claim lands at most once and is never lost.** After settlement, a
device claims again (with a new attempt) exactly when settlement says it did
not land; once that claim lands at the next slot, it is in the sequence at
exactly one slot. For a batch, its writes land once; for an admin entry, its
change lands once. -/
theorem lands_once_never_lost {seq : Nat → Rec Dev} {head start c : Nat} {d : Dev} {n i t v : Nat}
    (hash : Nat → Rec Dev → H) (own : Own seq head d n i t v)
    (hsc : start ≤ c + 1) (hch : c ≤ head) (hih : i ≤ head)
    {read : Option (Rec Dev)} (hread : start ≤ i → read = some (seq i)) (t₂ : Nat) :
    let verdict := settle start (lastClaim seq hash d c) i (Rec.claim d n t v) n
      (hash i (.claim d n t v)) (anotherClaim d n) read
    let seq' : Nat → Rec Dev := fun j =>
      if j = head + 1 ∧ verdict = .notLanded then .claim d n t₂ v else seq j
    let head' := if verdict = .notLanded then head + 1 else head
    ∃ j, 1 ≤ j ∧ j ≤ head' ∧ (∃ t' v', seq' j = .claim d n t' v') ∧
      ∀ j', 1 ≤ j' → j' ≤ head' → (∃ t' v', seq' j' = .claim d n t' v') → j' = j := by
  intro verdict seq' head'
  have hc := settle_correct hash own hsc hch (read := read) (fun h => by simp [hih, hread h])
  have hret : verdict ≠ .retry := fun h => by have := hc.2.2.1 h; omega
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
      · exact absurd h hc.2.2.2
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
    lastClaim seqLanded (fun _ x => x) true 2 = some ⟨1, .claim true 1 0 5⟩ ∧
    lastClaim seqLost (fun _ x => x) true 2 = none := by
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
  Digest := Nat
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
  resetRows := fun _ => ()
  boundaryAllowed := fun _ _ _ _ => true
  sinceReset := fun _ b => b
  number := fun _ => fun
    | .admin true _ => 1
    | .admin false (.addMember _ _) => 1
    | .admin false _ => 2
    | _ => 0
  digest := fun k _ => k
  ledger0 := ⟨fun m => m, fun _ => .admin, fun _ => some true, fun _ => false, fun _ => 0, (), [], fun _ => false,
    fun _ => none⟩
  aud0 := fun _ => ⟨fun _ => none, [], 0, 0, 0, [], 0, fun _ => false⟩
  rows0 := fun _ => ()

open CovenSequence in
/-- Device `true` adds member `false`; device `false` removes it. -/
def adminLanded : Nat → Slot adminSig
  | 1 => .admin true (.addMember false .member)
  | 2 => .admin false (.removeMember false)
  | _ => .admin false (.other ())

open CovenSequence in
/-- Device `false` adds member `false` and removes it. -/
def adminLost : Nat → Slot adminSig
  | 1 => .admin false (.addMember false .member)
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
    { ledgerAt adminRules adminLanded 2 with claims := fun _ => none } =
      { ledgerAt adminRules adminLost 2 with claims := fun _ => none } ∧
    (ledgerAt adminRules adminLanded 2).claims true = some ⟨1, (1 : Nat)⟩ ∧
    (ledgerAt adminRules adminLost 2).claims true = none ∧
    (ledgerAt adminRules adminLanded 2).member false = false ∧
    adminLanded 1 = .admin true (.addMember false .member) ∧
    adminLost 1 ≠ .admin true (.addMember false .member) := by
  refine ⟨rfl, rfl, rfl, rfl, rfl, fun h => ?_⟩
  simp only [adminLost] at h
  injection h with h1
  cases h1

/-! ## The number-only counterexample -/

/-- Two copies attempt slot 41 with claim 1 and different complete bytes. -/
def copiesSequence : Nat → Rec Bool := fun k =>
  if k = 41 then .claim true 1 1 7 else .other

/-- The number-only rule confirms the losing copy after deletion. The digest
rule detects the copy, including when the losing bytes were recreated below
floor 42. -/
theorem copies_fool_number_only :
    let a : Rec Bool := .claim true 1 0 5
    let b : Rec Bool := .claim true 1 1 7
    let hash := fun (k : Nat) (x : Rec Bool) => (k, x)
    let covered := lastClaim copiesSequence hash true 41
    Claim.create (Claim.create (fun _ => none) 41 b) 41 a 41 = some b ∧
    copiesSequence 41 ≠ a ∧ ClaimRecord.numberOf covered = 1 ∧
    (if 1 ≤ ClaimRecord.numberOf covered then Verdict.landed else .notLanded) = .landed ∧
    settle 42 covered 41 a 1 (hash 41 a) (anotherClaim true 1) none = .copy ∧
    settle 42 covered 41 a 1 (hash 41 a) (anotherClaim true 1) (some a) = .copy := by
  decide

/-! ## The per-device record in every audience's snapshot ledger -/
section Compose
variable {S : Sig} (R : Rules S)

theorem step_claims (L : Ledger S) (k : Nat) (x : Slot S) (e : S.Dev) :
    (ledgerStep R L k x).claims e = if e = author x then
      ClaimRecord.remember (L.claims e) ⟨R.number k x, R.digest k x⟩ else L.claims e := by
  have hin : (if ledgerValid R L x then ledgerEffect R L k x else refuse L k).claims = L.claims := by
    split
    · cases x with
      | admin a e => cases e <;> rfl
      | _ => rfl
    · rfl
  simp only [ledgerStep, noteClaim, hin]

/-- Ledger replay uses exactly the same record fold as abstract settlement. -/
theorem claims_eq (seq : Nat → Slot S) (h0 : ∀ e, R.ledger0.claims e = none) (d : S.Dev) :
    ∀ k, (ledgerAt R seq k).claims d =
      Claims.last (fun j => some (author (seq j), ⟨R.number j (seq j), R.digest j (seq j)⟩)) d k
  | 0 => h0 d
  | k + 1 => by
    rw [ledgerAt_succ, step_claims, claims_eq seq h0 d k, Claims.last]
    by_cases ha : d = author (seq (k + 1))
    · simp [ha]
    · simp [ha, Ne.symm ha]

/-- The greatest included number still bounds all claims, even with copies. -/
theorem claims_ge (seq : Nat → Slot S) (h0 : ∀ e, R.ledger0.claims e = none) (d : S.Dev)
    {n : Nat} (hn : 1 ≤ n) (k : Nat) :
    n ≤ ClaimRecord.numberOf ((ledgerAt R seq k).claims d) ↔
      ∃ j, 1 ≤ j ∧ j ≤ k ∧ author (seq j) = d ∧ n ≤ R.number j (seq j) := by
  rw [claims_eq R seq h0 d k, Claims.ge _ _ hn]
  constructor
  · rintro ⟨j, r, h1, h2, hj, hr⟩
    cases hj
    exact ⟨j, h1, h2, rfl, hr⟩
  · rintro ⟨j, h1, h2, ha, hn⟩
    exact ⟨j, ⟨R.number j (seq j), R.digest j (seq j)⟩, h1, h2, by rw [ha], hn⟩

/-- A snapshot's number and digest belong to the same included slot. -/
theorem claims_witness (seq : Nat → Slot S) (h0 : ∀ e, R.ledger0.claims e = none)
    (d : S.Dev) (k : Nat) {r : ClaimRecord S.Digest} (hr : (ledgerAt R seq k).claims d = some r) :
    ∃ j, 1 ≤ j ∧ j ≤ k ∧ author (seq j) = d ∧
      R.number j (seq j) = r.number ∧ R.digest j (seq j) = r.digest := by
  rw [claims_eq R seq h0 d k] at hr
  obtain ⟨j, h1, h2, hj⟩ := Claims.witness _ d k r hr
  cases hj
  exact ⟨j, h1, h2, rfl, rfl, rfl⟩

/-- **Settlement from a genuine floor snapshot with any number of copies.**
The digest premise expresses collision resistance and the slot path in the
complete bytes. No ordering or uniqueness of a device id's claim numbers is
assumed. Refused slots contribute the same evidence as accepted slots. -/
theorem settle_from_floor {seq : Nat → Slot S} {st : Store S} (g : Good R st seq)
    (h0 : ∀ e, R.ledger0.claims e = none) (A : S.Aud) {d : S.Dev} {n i : Nat} {x : Slot S}
    (hxa : author x = d) (hxn : R.number i x = n) (hn : 1 ≤ n) (hi : 1 ≤ i)
    (hhash : ∀ j, 1 ≤ j → j ≤ st.head → author (seq j) = d →
      R.digest j (seq j) = R.digest i x → j = i ∧ seq j = x)
    (hdel : i < st.floor.start) {L : Ledger S} {P : AudLedger S} {rows : S.Rows}
    (hb : st.body (st.floor.path A) = some (L, P, rows)) :
    let verdict := settleFloor (L.claims d) n (R.digest i x)
    (verdict = .landed → i ≤ st.head ∧ seq i = x) ∧
    (verdict = .notLanded → ¬ (i ≤ st.head ∧ seq i = x)) ∧
    (verdict = .copy ↔ ∃ r, L.claims d = some r ∧
      (n < r.number ∨ (r.number = n ∧ r.digest ≠ R.digest i x))) := by
  rw [g.floor_body A] at hb
  have hL : L = ledgerAt R seq (st.floor.covers A) := by
    have := Option.some.inj hb
    exact (congrArg Prod.fst this).symm
  subst L
  have hic : i ≤ st.floor.covers A := by have := g.start_le A; omega
  have hch := g.covers_le A
  dsimp only
  refine ⟨?_, ?_, floor_copy _ _ _⟩
  · intro hl
    obtain ⟨j, h1, h2, ha, _, hh⟩ := claims_witness R seq h0 d _ ((floor_landed _ _ _).1 hl)
    obtain ⟨rfl, he⟩ := hhash j h1 (by omega) ha hh
    exact ⟨by omega, he⟩
  · intro hl ⟨_, he⟩
    have hlt := (floor_notLanded _ _ _ hn).1 hl
    have hge := (claims_ge R seq h0 d hn (st.floor.covers A)).2
      ⟨i, hi, hic, by rw [he, hxa], by simp [he, hxn]⟩
    omega

/-- With one copy, the floor confirms exactly the recorded claim. -/
theorem settle_from_floor_single {seq : Nat → Slot S} {st : Store S} (g : Good R st seq)
    (h0 : ∀ e, R.ledger0.claims e = none) (A : S.Aud) {d : S.Dev} {n i : Nat} {x : Slot S}
    (hxa : author x = d) (hxn : R.number i x = n) (hn : 1 ≤ n) (hi : 1 ≤ i)
    (hord : ∀ j, 1 ≤ j → j ≤ st.head → author (seq j) = d → R.number j (seq j) ≤ n)
    (honly : ∀ j, 1 ≤ j → j ≤ st.head → author (seq j) = d →
      R.number j (seq j) = n → j = i ∧ seq j = x)
    (hdel : i < st.floor.start) {L : Ledger S} {P : AudLedger S} {rows : S.Rows}
    (hb : st.body (st.floor.path A) = some (L, P, rows)) :
    settleFloor (L.claims d) n (R.digest i x) = .landed ↔ i ≤ st.head ∧ seq i = x := by
  rw [g.floor_body A] at hb
  have hL : L = ledgerAt R seq (st.floor.covers A) := by
    exact (congrArg Prod.fst (Option.some.inj hb)).symm
  subst L
  have hic : i ≤ st.floor.covers A := by have := g.start_le A; omega
  have hch := g.covers_le A
  rw [floor_landed]
  constructor
  · intro hr
    obtain ⟨j, h1, h2, ha, hm, _⟩ := claims_witness R seq h0 d _ hr
    obtain ⟨rfl, he⟩ := honly j h1 (by omega) ha hm
    exact ⟨by omega, he⟩
  · rintro ⟨_, he⟩
    have hge := (claims_ge R seq h0 d hn (st.floor.covers A)).2
      ⟨i, hi, hic, by rw [he, hxa], by simp [he, hxn]⟩
    cases hr : (ledgerAt R seq (st.floor.covers A)).claims d with
    | none => simp [hr, ClaimRecord.numberOf] at hge; omega
    | some r =>
      obtain ⟨j, h1, h2, ha, hm, hh⟩ := claims_witness R seq h0 d _ hr
      have hle := hord j h1 (by omega) ha
      have hnum : r.number = n := by simp [hr, ClaimRecord.numberOf] at hge; omega
      obtain ⟨hji, hx⟩ := honly j h1 (by omega) ha (hm.trans hnum)
      subst j
      congr 1
      cases r
      simp_all

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
