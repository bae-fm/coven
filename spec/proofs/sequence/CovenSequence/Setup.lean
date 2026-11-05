import CovenSequence.Claim

/-!
# Storage setup: one point of no return

Storage setup checks the provider, then claims what the store needs and
commits the connection on the device. It is an operation with one point of
no return (spec §11.6):

- It journals itself before its first write to storage. While it checks the
  provider it has claimed nothing. A cancel, a failure, or opening the store
  after a crash undoes it: the journal ends, and nothing it claimed or
  committed exists.
- Its point of no return is recording the claim it is about to send. From
  then on a cancel is refused: the claim, once sent, can land at any time,
  even after a crash.
- After a crash past that point, setup settles its claim. Landed, it commits
  the connection and finishes. Lost to another device, it reads on, and is
  refused only where every claim it sent targets a slot another device holds,
  so nothing of it can still land.
- The connection is committed only once its claim has landed.

Test objects and probes setup writes while it checks are space, not state:
nothing refers to them, so they are not modelled.

The claim's bytes for slot `n` are `ours n`: they name the attempt, so a
resend is the same attempt.
-/

namespace CovenSequence.Setup

open CovenSequence.Claim (Store create create_extends)

/-- Setup's claim at slot `n`, or another device's bytes. -/
inductive Bytes where
  | ours (n : Nat)
  | other (x : Nat)
  deriving DecidableEq

inductive Phase where
  /-- Journaled, checking the provider; nothing is claimed. -/
  | checking
  /-- Past the point of no return: the claim of slot `n` is recorded and sent. -/
  | claiming (n : Nat)
  /-- Another device took the claimed slot; read on. -/
  | retry
  /-- Its claim landed at slot `n`. -/
  | landed (n : Nat)
  deriving DecidableEq

inductive Result where
  | landed (n : Nat)
  | undone
  deriving DecidableEq

structure World where
  slots : Store Bytes
  /-- Slots of setup's sent claims that have not landed; each may land at any
  time, even after a crash. -/
  inflight : List Nat
  journal : Option Phase
  /-- The connection is committed on the device. -/
  committed : Bool
  result : Option Result

/-- Faulty variants checked as counterexamples. -/
inductive Fault where
  | none
  /-- A cancel honoured once the claim is sent. -/
  | lateCancel
  deriving DecidableEq

/-- Slots `1..n-1` are filled and slot `n` is empty. -/
def AtHead (s : Store Bytes) (n : Nat) : Prop :=
  (∀ i, 1 ≤ i → i < n → s i ≠ none) ∧ s n = none

/-- Setup's steps and the environment's. `allowed s n` is setup's check of
slot `n` against slots `1..n-1`. A crash takes no step of its own: every step
is conditioned on the journal and on storage, which survive it, and opening
the store takes the same steps. -/
inductive Step (allowed : Store Bytes → Nat → Bool) (fault : Fault) : World → World → Prop
  /-- Before the point of no return a cancel, a failure, or opening the store
  after a crash undoes setup. -/
  | undo (w : World) (hj : w.journal = some .checking) :
      Step allowed fault w { w with journal := none, result := some .undone }
  /-- The faulty cancel, honoured past the point of no return. -/
  | lateCancel (w : World) (n : Nat) (hf : fault = .lateCancel)
      (hj : w.journal = some (.claiming n)) :
      Step allowed fault w { w with journal := none, result := some .undone }
  /-- Pass the point of no return: record the claim, then send it. -/
  | claim (w : World) (n : Nat) (hj : w.journal = some .checking ∨ w.journal = some .retry)
      (hn : 1 ≤ n) (hhead : AtHead w.slots n) (hv : allowed w.slots n = true) :
      Step allowed fault w { w with journal := some (.claiming n), inflight := n :: w.inflight }
  | resend (w : World) (n : Nat) (hj : w.journal = some (.claiming n)) :
      Step allowed fault w { w with inflight := n :: w.inflight }
  /-- A sent claim lands, at any time. -/
  | land (w : World) (n : Nat) (hn : n ∈ w.inflight) :
      Step allowed fault w
        { w with slots := create w.slots n (.ours n), inflight := w.inflight.erase n }
  /-- Another device claims a slot, at any time. -/
  | other (w : World) (n x : Nat) :
      Step allowed fault w { w with slots := create w.slots n (.other x) }
  | won (w : World) (n : Nat) (hj : w.journal = some (.claiming n))
      (hs : w.slots n = some (.ours n)) :
      Step allowed fault w { w with journal := some (.landed n) }
  | lost (w : World) (n x : Nat) (hj : w.journal = some (.claiming n))
      (hs : w.slots n = some (.other x)) :
      Step allowed fault w { w with journal := some .retry }
  /-- Refused once it has read on: the next slot doesn't allow it. -/
  | refuse (w : World) (n : Nat) (hj : w.journal = some .retry) (hhead : AtHead w.slots n)
      (hv : allowed w.slots n = false) :
      Step allowed fault w { w with journal := none, result := some .undone }
  /-- Commit the connection, once the claim has landed. -/
  | commit (w : World) (n : Nat) (hj : w.journal = some (.landed n)) :
      Step allowed fault w { w with committed := true }
  | complete (w : World) (n : Nat) (hj : w.journal = some (.landed n))
      (hc : w.committed = true) :
      Step allowed fault w { w with journal := none, result := some (.landed n) }

inductive Steps (allowed : Store Bytes → Nat → Bool) (fault : Fault) : World → World → Prop
  | refl (w : World) : Steps allowed fault w w
  | step {w w' w'' : World} : Step allowed fault w w' → Steps allowed fault w' w'' →
      Steps allowed fault w w''

theorem Steps.trans {allowed : Store Bytes → Nat → Bool} {fault : Fault} {w w' w'' : World}
    (a : Steps allowed fault w w') (b : Steps allowed fault w' w'') :
    Steps allowed fault w w'' := by
  induction a with
  | refl => exact b
  | step s _ ih => exact Steps.step s (ih b)

/-- A journaled setup over slots holding none of its bytes. -/
def start (s : Store Bytes) : World :=
  { slots := s, inflight := [], journal := some .checking, committed := false, result := none }

def World.phase (w : World) : Option Phase := w.journal

/-- `s` holds setup's bytes at slot `n`. -/
def Own (s : Store Bytes) (n : Nat) : Prop := s n = some (.ours n)

/-- What every reachable world satisfies. -/
structure Inv (w : World) : Prop where
  /-- Setup's bytes appear only at the slot they name. -/
  ours_here : ∀ n m, w.slots n = some (.ours m) → m = n
  /-- A claim in flight is the one being claimed, or targets a filled slot,
  where create-once refuses it. -/
  inflight_ok : ∀ m, m ∈ w.inflight → w.journal = some (.claiming m) ∨ w.slots m ≠ none
  /-- Setup's bytes are at a slot only while claiming it, or once landed there. -/
  own_phase : ∀ n, Own w.slots n →
    w.journal = some (.claiming n) ∨ w.journal = some (.landed n) ∨ w.result = some (.landed n)
  /-- A landed setup's bytes are at its slot. -/
  landed_own : ∀ n, (w.journal = some (.landed n) ∨ w.result = some (.landed n)) → Own w.slots n
  /-- Checking, setup has sent nothing and committed nothing. -/
  checking_clean : w.journal = some .checking → w.inflight = [] ∧ w.committed = false
  /-- The connection is committed only once the claim has landed. -/
  committed_landed : w.committed = true →
    ∃ n, w.journal = some (.landed n) ∨ w.result = some (.landed n)
  /-- A result belongs to an ended journal. -/
  result_done : w.result ≠ none → w.journal = none

theorem inv_start (s : Store Bytes) (h : ∀ n m, s n ≠ some (.ours m)) : Inv (start s) := by
  refine ⟨fun n m hs => absurd hs (h n m), fun m hm => by simp [start] at hm,
    fun n hn => absurd hn (h n n), fun n hn => ?_, fun _ => ⟨rfl, rfl⟩,
    fun hc => by simp [start] at hc, fun hr => by simp [start] at hr⟩
  simp [start] at hn

theorem result_none {w : World} (hi : Inv w) {p : Phase} (hj : w.journal = some p) :
    w.result = none := by
  cases hr : w.result with
  | none => rfl
  | some r => have := hi.result_done (by simp [hr]); simp [hj] at this

/-- A create by anyone keeps setup's bytes where they were, and adds them only
at the slot they name. -/
theorem ours_create {w : World} (hi : Inv w) (n : Nat) (b : Bytes)
    (hb : ∀ m, b = .ours m → m = n) :
    ∀ k m, create w.slots n b k = some (.ours m) → m = k := by
  intro k m hk
  by_cases hkn : k = n
  · subst hkn
    cases hs : w.slots k with
    | none => rw [Claim.create_self_none _ hs] at hk; cases hk; exact hb m rfl
    | some x => rw [Claim.create_self_some _ hs] at hk; exact hi.ours_here k m (hs ▸ hk)
  · rw [Claim.create_other _ hkn] at hk; exact hi.ours_here k m hk

theorem filled_create (s : Store Bytes) (n : Nat) (b : Bytes) (m : Nat) (h : s m ≠ none) :
    create s n b m ≠ none := by
  cases hs : s m with
  | none => exact absurd hs h
  | some x => rw [create_extends s n b m x hs]; simp

theorem own_create_keeps {s : Store Bytes} {n k : Nat} {b : Bytes} (h : Own s k) :
    Own (create s n b) k := by
  unfold Own at h ⊢; exact create_extends s n b k _ h

/-- **Every step keeps the invariant.** -/
theorem step_inv {allowed : Store Bytes → Nat → Bool} {w w' : World}
    (st : Step allowed .none w w') (hi : Inv w) : Inv w' := by
  cases st with
  | undo hj =>
    have hres := result_none hi hj
    have hclean := hi.checking_clean hj
    refine ⟨hi.ours_here, fun m hm => by simp [hclean.1] at hm, fun n hn => ?_,
      fun n hn => by simp at hn, fun h => by simp at h, fun hc => ?_, fun _ => rfl⟩
    · rcases hi.own_phase n hn with h | h | h <;> simp [hj, hres] at h
    · simp [hclean.2] at hc
  | lateCancel n hf => cases hf
  | claim n hj hn hhead hv =>
    have hres : w.result = none := by
      rcases hj with h | h <;> exact result_none hi h
    refine ⟨hi.ours_here, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun h => by simp at h, fun hc => ?_, fun h => by simp [hres] at h⟩
    · simp only [List.mem_cons] at hm
      rcases hm with rfl | hm
      · exact Or.inl rfl
      · rcases hi.inflight_ok m hm with h | h
        · rcases hj with h' | h' <;> rw [h'] at h <;> cases h
        · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · rcases hj with h' | h' <;> rw [h'] at h <;> cases h
      · rcases hj with h' | h' <;> rw [h'] at h <;> cases h
      · simp [hres] at h
    · simp [hres] at hk
    · rcases hi.committed_landed hc with ⟨k, h | h⟩
      · rcases hj with h' | h' <;> rw [h'] at h <;> cases h
      · simp [hres] at h
  | resend n hj =>
    refine ⟨hi.ours_here, fun m hm => ?_, hi.own_phase, hi.landed_own,
      fun h => by simp [hj] at h, hi.committed_landed, hi.result_done⟩
    simp only [List.mem_cons] at hm
    rcases hm with rfl | hm
    · exact Or.inl hj
    · exact hi.inflight_ok m hm
  | land n hn =>
    refine ⟨ours_create hi n _ (fun m h => by cases h; rfl), fun m hm => ?_, fun k hk => ?_,
      fun k hk => own_create_keeps (hi.landed_own k hk), fun h => ?_, hi.committed_landed,
      hi.result_done⟩
    · rcases hi.inflight_ok m (List.mem_of_mem_erase hm) with h | h
      · exact Or.inl h
      · exact Or.inr (filled_create _ _ _ _ h)
    · unfold Own at hk
      simp only at hk ⊢
      by_cases hkn : k = n
      · subst hkn
        cases hs : w.slots k with
        | none =>
          rcases hi.inflight_ok k hn with h | h
          · exact Or.inl h
          · exact absurd hs h
        | some x =>
          rw [Claim.create_self_some _ hs] at hk
          exact hi.own_phase k (by unfold Own; rw [hs]; exact hk)
      · rw [Claim.create_other _ hkn] at hk; exact hi.own_phase k hk
    · have := (hi.checking_clean h).1; simp [this] at hn
  | other n x =>
    refine ⟨ours_create hi n _ (fun m h => by cases h), fun m hm => ?_, fun k hk => ?_,
      fun k hk => own_create_keeps (hi.landed_own k hk), hi.checking_clean, hi.committed_landed,
      hi.result_done⟩
    · rcases hi.inflight_ok m hm with h | h
      · exact Or.inl h
      · exact Or.inr (filled_create _ _ _ _ h)
    · unfold Own at hk
      simp only at hk ⊢
      by_cases hkn : k = n
      · subst hkn
        cases hs : w.slots k with
        | none => rw [Claim.create_self_none _ hs] at hk; cases hk
        | some y =>
          rw [Claim.create_self_some _ hs] at hk
          exact hi.own_phase k (by unfold Own; rw [hs]; exact hk)
      · rw [Claim.create_other _ hkn] at hk; exact hi.own_phase k hk
  | won n hj hs =>
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun h => by simp at h, fun hc => ?_, fun h => by simp [hres] at h⟩
    · rcases hi.inflight_ok m hm with h | h
      · rw [hj] at h; cases h; exact Or.inr (by simp [hs])
      · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · rw [hj] at h; cases h; exact Or.inr (Or.inl rfl)
      · rw [hj] at h; cases h
      · simp [hres] at h
    · rcases hk with h | h
      · cases h; exact hs
      · simp [hres] at h
    · rcases hi.committed_landed hc with ⟨k, h | h⟩
      · rw [hj] at h; cases h
      · simp [hres] at h
  | lost n x hj hs =>
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun h => by simp at h, fun hc => ?_, fun h => by simp [hres] at h⟩
    · rcases hi.inflight_ok m hm with h | h
      · rw [hj] at h; cases h; exact Or.inr (by simp [hs])
      · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · rw [hj] at h; cases h; unfold Own at hk; rw [hs] at hk; cases hk
      · rw [hj] at h; cases h
      · simp [hres] at h
    · rcases hk with h | h
      · simp at h
      · simp [hres] at h
    · rcases hi.committed_landed hc with ⟨k, h | h⟩
      · rw [hj] at h; cases h
      · simp [hres] at h
  | refuse n hj hhead hv =>
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun h => by simp at h, fun hc => ?_, fun _ => rfl⟩
    · rcases hi.inflight_ok m hm with h | h
      · rw [hj] at h; cases h
      · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · rw [hj] at h; cases h
      · rw [hj] at h; cases h
      · simp [hres] at h
    · rcases hk with h | h <;> simp at h
    · rcases hi.committed_landed hc with ⟨k, h | h⟩
      · rw [hj] at h; cases h
      · simp [hres] at h
  | commit n hj =>
    refine ⟨hi.ours_here, hi.inflight_ok, hi.own_phase, hi.landed_own,
      fun h => by simp [hj] at h, fun _ => ⟨n, Or.inl hj⟩, hi.result_done⟩
  | complete n hj hc =>
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun h => by simp at h, fun _ => ⟨n, Or.inr rfl⟩, fun _ => rfl⟩
    · rcases hi.inflight_ok m hm with h | h
      · rw [hj] at h; cases h
      · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · rw [hj] at h; cases h
      · rw [hj] at h; cases h; exact Or.inr (Or.inr rfl)
      · simp [hres] at h
    · rcases hk with h | h
      · simp at h
      · cases h; exact hi.landed_own _ (Or.inl hj)

theorem steps_inv {allowed : Store Bytes → Nat → Bool} {w w' : World}
    (run : Steps allowed .none w w') (hi : Inv w) : Inv w' := by
  induction run with
  | refl => exact hi
  | step s _ ih => exact ih (step_inv s hi)

/-- An ended setup's result never changes. -/
theorem step_keeps_result {allowed : Store Bytes → Nat → Bool} {w w' : World}
    (st : Step allowed .none w w') (hi : Inv w) {r : Result} (hr : w.result = some r) :
    w'.result = some r := by
  have hj := hi.result_done (by simp [hr])
  cases st <;> simp_all

theorem steps_keep_result {allowed : Store Bytes → Nat → Bool} {w w' : World}
    (run : Steps allowed .none w w') (hi : Inv w) {r : Result} (hr : w.result = some r) :
    w'.result = some r := by
  induction run with
  | refl => exact hr
  | step s _ ih => exact ih (step_inv s hi) (step_keeps_result s hi hr)

/-- An undone setup has no bytes in the sequence and no committed connection,
and every claim it ever sent targets a slot another device holds. -/
theorem undone_clean {w : World} (hi : Inv w) (hu : w.result = some .undone) :
    (∀ n, ¬ Own w.slots n) ∧ w.committed = false ∧ (∀ m, m ∈ w.inflight → w.slots m ≠ none) := by
  have hj := hi.result_done (by simp [hu])
  refine ⟨fun n hn => ?_, ?_, fun m hm => ?_⟩
  · rcases hi.own_phase n hn with h | h | h <;> simp [hj, hu] at h
  · cases hc : w.committed with
    | false => rfl
    | true => obtain ⟨n, h | h⟩ := hi.committed_landed hc <;> simp [hj, hu] at h
  · rcases hi.inflight_ok m hm with h | h
    · simp [hj] at h
    · exact h

/-- **Setup's outcome.** From slots holding none of its bytes, through any
interleaving of its steps, crashes, late-landing claims and other devices'
claims:

- its bytes occupy at most one slot;
- the connection is committed only where its claim landed;
- an undone setup has no bytes in the sequence, now or later, and no
  committed connection. -/
theorem setup_outcome {allowed : Store Bytes → Nat → Bool} (s0 : Store Bytes)
    (h0 : ∀ n m, s0 n ≠ some (.ours m)) {w : World} (run : Steps allowed .none (start s0) w) :
    (∀ n m, Own w.slots n → Own w.slots m → n = m) ∧
    (w.committed = true → ∃ n, Own w.slots n) ∧
    (w.result = some .undone → w.committed = false ∧
      ∀ w', Steps allowed .none w w' → ∀ n, ¬ Own w'.slots n) := by
  have hi := steps_inv run (inv_start s0 h0)
  refine ⟨fun n m hn hm => ?_, fun hc => ?_, fun hu => ⟨(undone_clean hi hu).2.1, ?_⟩⟩
  · have hres : ∀ k, w.result = some (.landed k) → w.journal = none :=
      fun k h => hi.result_done (by simp [h])
    rcases hi.own_phase n hn with h | h | h <;> rcases hi.own_phase m hm with h' | h' | h'
    all_goals first
      | (rw [h] at h'; simp at h'; done)
      | (rw [h] at h'; simp at h'; omega)
      | (rw [hres _ h] at h'; simp at h')
      | (rw [hres _ h'] at h; simp at h)
  · obtain ⟨n, h⟩ := hi.committed_landed hc
    exact ⟨n, hi.landed_own n h⟩
  · intro w' run' n
    exact (undone_clean (steps_inv run' hi) (steps_keep_result run' hi hu)).1 n

/-- **A crash before the point of no return undoes setup.** Wherever a run
leaves setup checking the provider, opening the store undoes it: it ends
undone, with nothing claimed or committed, and nothing of it can land
later. -/
theorem crash_before_point_undoes {allowed : Store Bytes → Nat → Bool} (s0 : Store Bytes)
    (h0 : ∀ n m, s0 n ≠ some (.ours m)) {w : World} (run : Steps allowed .none (start s0) w)
    (hj : w.journal = some .checking) :
    ∃ w', Step allowed .none w w' ∧ w'.result = some .undone ∧ w'.committed = false ∧
      w'.inflight = [] ∧ ∀ w'', Steps allowed .none w' w'' → ∀ n, ¬ Own w''.slots n := by
  have hi := steps_inv run (inv_start s0 h0)
  have hclean := hi.checking_clean hj
  refine ⟨_, Step.undo w hj, rfl, hclean.2, hclean.1, ?_⟩
  exact ((setup_outcome s0 h0 (run.trans (Steps.step (Step.undo w hj) (Steps.refl _)))).2.2 rfl).2

/-- **A crash after the point of no return finishes setup.** Wherever a run
leaves setup claiming slot `n`, its own steps settle the claim: where slot
`n` is empty or holds its bytes, it lands, commits the connection and
finishes; where another device took the slot, it reads on. -/
theorem crash_after_point_finishes {allowed : Store Bytes → Nat → Bool} (s0 : Store Bytes)
    (h0 : ∀ n m, s0 n ≠ some (.ours m)) {w : World} (run : Steps allowed .none (start s0) w)
    {n : Nat} (hj : w.journal = some (.claiming n)) :
    ∃ w', Steps allowed .none w w' ∧
      ((w'.result = some (.landed n) ∧ w'.committed = true ∧ Own w'.slots n) ∨
        w'.journal = some .retry) := by
  have hi := steps_inv run (inv_start s0 h0)
  -- Once its bytes are at slot `n`, it wins, commits and completes.
  have finish : ∀ v : World, Inv v → v.journal = some (.claiming n) →
      v.slots n = some (.ours n) →
      ∃ w', Steps allowed .none v w' ∧ w'.result = some (.landed n) ∧ w'.committed = true ∧
        Own w'.slots n := by
    intro v hv hvj hvs
    refine ⟨_, Steps.step (Step.won v n hvj hvs)
      (Steps.step (Step.commit _ n rfl)
      (Steps.step (Step.complete _ n rfl rfl) (Steps.refl _))), rfl, rfl, hvs⟩
  cases hs : w.slots n with
  | none =>
    let v : World := { w with inflight := n :: w.inflight }
    let u : World := { v with slots := create v.slots n (.ours n), inflight := v.inflight.erase n }
    have hu : Steps allowed .none w u :=
      Steps.step (Step.resend w n hj) (Steps.step (Step.land v n (by simp [v])) (Steps.refl _))
    have hiu := steps_inv hu hi
    obtain ⟨w', run', h⟩ :=
      finish u hiu (by simp [u, v, hj]) (by simp [u, v, Claim.create_self_none _ hs])
    exact ⟨w', hu.trans run', Or.inl h⟩
  | some b =>
    cases b with
    | ours m =>
      have := hi.ours_here n m hs; subst this
      obtain ⟨w', run', h⟩ := finish w hi hj hs
      exact ⟨w', run', Or.inl h⟩
    | other x =>
      exact ⟨_, Steps.step (Step.lost w n x hj hs) (Steps.refl _), Or.inr rfl⟩

/-- **Counterexample: a cancel past the point of no return strands a claim.**
Setup claims slot 1 and the app cancels it once the claim is sent. The setup
ends undone, then its claim lands: slot 1 holds a store this device doesn't
know it made. -/
theorem late_cancel_strands_claim :
    ∃ w : World, Steps (fun _ _ => true) .lateCancel (start (fun _ => none)) w ∧
      w.result = some .undone ∧ Own w.slots 1 := by
  refine ⟨_, Steps.step (Step.claim _ 1 (Or.inl rfl) (Nat.le_refl _)
      ⟨fun i h1 h2 => by omega, rfl⟩ rfl)
    (Steps.step (Step.lateCancel _ 1 rfl rfl)
    (Steps.step (Step.land _ 1 (by simp)) (Steps.refl _))), rfl, ?_⟩
  simp [Own, Claim.create, start]

end CovenSequence.Setup
