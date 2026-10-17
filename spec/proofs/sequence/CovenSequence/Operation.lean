import CovenSequence.Claim

/-!
# Admin operations: the claim loop and the journal

Every admin action is an operation, a journal entry. It validates against the
latest slot it read, records each pre-claim artifact in the journal before
creating it (sealed key copies, snapshot bodies: the provider's storage of them
is abstract here), then claims the next slot. The journal records the slot it
is claiming before the request is sent.

- The claim lands (its bytes are at the slot): the operation completes.
- Another device took the slot: read on, validate again against the new
  slots, and claim the next one.
- No longer allowed: undo every recorded artifact, then delete the journal. A
  failed undo is a step not taken; the record stays and the undo retries.

A crash loses nothing the steps use: every step is conditioned on the journal
and on storage, which survive, and a restart takes the same steps. Claim
requests already sent survive the crash and can land at any later time.
Other devices claim slots at any time.

The operation's bytes for slot `n` are `ours n`: they name the operation and
the slot, so they are unique per attempt and a resend is the same attempt.
Artifact creates are modelled as completed provider calls; a create request
that could land after its undo is outside the model.
-/

namespace CovenSequence.Operation

open CovenSequence.Claim (Store create create_extends Extends)

/-- Bytes in the sequence: this operation's claim of slot `n`, or another
device's claim. -/
inductive Bytes where
  | ours (n : Nat)
  | other (x : Nat)
  deriving DecidableEq

inductive Phase where
  /-- Validated; recording and creating pre-claim artifacts. -/
  | prep
  /-- The claim of slot `n` is recorded and sent. -/
  | claiming (n : Nat)
  /-- Another device took the claimed slot; read on and validate again. -/
  | retry
  | landed (n : Nat)
  | undoing
  deriving DecidableEq

structure Journal (A : Type) where
  recorded : List A
  phase : Phase

inductive Result where
  | landed (n : Nat)
  | undone
  deriving DecidableEq

structure World (A : Type) where
  slots : Store Bytes
  /-- Slots of this operation's sent claim requests that have not landed. -/
  inflight : List Nat
  /-- The provider's storage of this operation's artifacts. -/
  artifacts : List A
  journal : Option (Journal A)
  result : Option Result

variable {A : Type}

/-- Slots `1..n-1` are filled and slot `n` is empty: `n` is the next slot. -/
def AtHead (s : Store Bytes) (n : Nat) : Prop :=
  (∀ i, 1 ≤ i → i < n → s i ≠ none) ∧ s n = none

/-- The operation's steps and the environment's. `allowed s n` is the
operation's validation against slots `1..n-1`. `forgetful` enables a faulty
restart that forgets its recorded claim. -/
inductive Step [DecidableEq A] (allowed : Store Bytes → Nat → Bool) (forgetful : Bool) : World A → World A → Prop
  | record (w : World A) (rec : List A) (a : A) (hj : w.journal = some ⟨rec, .prep⟩) :
      Step allowed forgetful w { w with journal := some ⟨a :: rec, .prep⟩ }
  | createArt (w : World A) (rec : List A) (a : A) (hj : w.journal = some ⟨rec, .prep⟩) (ha : a ∈ rec) :
      Step allowed forgetful w { w with artifacts := a :: w.artifacts }
  | claim (w : World A) (rec : List A) (ph : Phase) (n : Nat) (hj : w.journal = some ⟨rec, ph⟩)
      (hph : ph = .prep ∨ ph = .retry) (hn : 1 ≤ n) (hhead : AtHead w.slots n)
      (hv : allowed w.slots n = true) :
      Step allowed forgetful w { w with journal := some ⟨rec, .claiming n⟩, inflight := n :: w.inflight }
  | resend (w : World A) (rec : List A) (n : Nat) (hj : w.journal = some ⟨rec, .claiming n⟩) :
      Step allowed forgetful w { w with inflight := n :: w.inflight }
  | land (w : World A) (n : Nat) (hn : n ∈ w.inflight) :
      Step allowed forgetful w { w with slots := create w.slots n (.ours n), inflight := w.inflight.erase n }
  | other (w : World A) (n x : Nat) :
      Step allowed forgetful w { w with slots := create w.slots n (.other x) }
  | won (w : World A) (rec : List A) (n : Nat) (hj : w.journal = some ⟨rec, .claiming n⟩)
      (hs : w.slots n = some (.ours n)) :
      Step allowed forgetful w { w with journal := some ⟨rec, .landed n⟩ }
  | lost (w : World A) (rec : List A) (n x : Nat) (hj : w.journal = some ⟨rec, .claiming n⟩)
      (hs : w.slots n = some (.other x)) :
      Step allowed forgetful w { w with journal := some ⟨rec, .retry⟩ }
  | giveUp (w : World A) (rec : List A) (ph : Phase) (n : Nat) (hj : w.journal = some ⟨rec, ph⟩)
      (hph : ph = .prep ∨ ph = .retry) (hhead : AtHead w.slots n) (hv : allowed w.slots n = false) :
      Step allowed forgetful w { w with journal := some ⟨rec, .undoing⟩ }
  | undoOne (w : World A) (rec : List A) (a : A) (hj : w.journal = some ⟨rec, .undoing⟩) :
      Step allowed forgetful w
        { w with artifacts := w.artifacts.filter (· ≠ a),
                 journal := some ⟨rec.filter (· ≠ a), .undoing⟩ }
  | finishUndo (w : World A) (hj : w.journal = some ⟨[], .undoing⟩) :
      Step allowed forgetful w { w with journal := none, result := some .undone }
  | complete (w : World A) (rec : List A) (n : Nat) (hj : w.journal = some ⟨rec, .landed n⟩) :
      Step allowed forgetful w { w with journal := none, result := some (.landed n) }
  /-- The faulty restart: the claim in flight is forgotten. -/
  | forget (w : World A) (rec : List A) (n : Nat) (hf : forgetful = true)
      (hj : w.journal = some ⟨rec, .claiming n⟩) :
      Step allowed forgetful w { w with journal := some ⟨rec, .prep⟩ }

inductive Steps [DecidableEq A] (allowed : Store Bytes → Nat → Bool) (forgetful : Bool) : World A → World A → Prop
  | refl (w : World A) : Steps allowed forgetful w w
  | step {w w' w'' : World A} : Step allowed forgetful w w' → Steps allowed forgetful w' w'' →
      Steps allowed forgetful w w''

/-- An operation that has validated, with no artifacts and no requests, over
any slots that hold none of its bytes. -/
def start (s : Store Bytes) : World A :=
  { slots := s, inflight := [], artifacts := [], journal := some ⟨[], .prep⟩, result := none }

/-- The journal's phase, or none once it is deleted. -/
def World.phase (w : World A) : Option Phase := w.journal.map (·.phase)

/-- `s` holds this operation's bytes at slot `n`. -/
def Own (s : Store Bytes) (n : Nat) : Prop := s n = some (.ours n)

/-- Validation reads only slots `1..n-1`. -/
def PrefixOnly (allowed : Store Bytes → Nat → Bool) : Prop :=
  ∀ s t n, (∀ i, 1 ≤ i → i < n → s i = t i) → allowed s n = allowed t n

/-- What every reachable world satisfies. -/
structure Inv (allowed : Store Bytes → Nat → Bool) (w : World A) : Prop where
  /-- Our bytes appear only at the slot they name. -/
  ours_here : ∀ n m, w.slots n = some (.ours m) → m = n
  /-- Every stored artifact is recorded while the journal exists; after the
  journal is deleted, artifacts remain only for a landed operation. -/
  recorded : ∀ a, a ∈ w.artifacts → (∃ j, w.journal = some j ∧ a ∈ j.recorded) ∨
    (∃ n, w.result = some (.landed n))
  /-- A request still in flight is the one being claimed, or targets a filled
  slot, where create-once refuses it. -/
  inflight_ok : ∀ m, m ∈ w.inflight → w.phase = some (.claiming m) ∨ w.slots m ≠ none
  /-- Our bytes are at a slot only while claiming it or after landing it. -/
  own_phase : ∀ n, Own w.slots n →
    w.phase = some (.claiming n) ∨ w.phase = some (.landed n) ∨ w.result = some (.landed n)
  /-- A landed operation's bytes are at its slot. -/
  landed_own : ∀ n, (w.phase = some (.landed n) ∨ w.result = some (.landed n)) → Own w.slots n
  /-- The slot being claimed or landed was the next slot when validated, and
  validation held there. -/
  validated : ∀ n, (w.phase = some (.claiming n) ∨ w.phase = some (.landed n) ∨
      w.result = some (.landed n)) →
    1 ≤ n ∧ (∀ i, 1 ≤ i → i < n → w.slots i ≠ none) ∧ allowed w.slots n = true
  /-- A result belongs to a deleted journal. -/
  result_done : w.result ≠ none → w.journal = none

theorem create_keeps_filled (s : Store Bytes) (n : Nat) (b : Bytes) (i : Nat) (h : s i ≠ none) :
    create s n b i = s i := by
  cases hs : s i with
  | none => exact absurd hs h
  | some x => exact create_extends s n b i x hs

theorem create_filled_ne (s : Store Bytes) (n : Nat) (b : Bytes) (i : Nat) (h : s i ≠ none) :
    create s n b i ≠ none := by
  rw [create_keeps_filled s n b i h]; exact h

/-- Validation of a slot whose predecessors are filled survives any create. -/
theorem allowed_create {allowed : Store Bytes → Nat → Bool} (hp : PrefixOnly allowed)
    (s : Store Bytes) (n m : Nat) (b : Bytes) (hfill : ∀ i, 1 ≤ i → i < n → s i ≠ none) :
    allowed (create s m b) n = allowed s n :=
  hp _ _ n (fun i h1 h2 => create_keeps_filled s m b i (hfill i h1 h2))

theorem inv_start (allowed : Store Bytes → Nat → Bool) (s : Store Bytes)
    (h : ∀ n m, s n ≠ some (.ours m)) : Inv allowed (start (A := A) s) := by
  refine ⟨fun n m hs => absurd hs (h n m), fun a ha => by simp [start] at ha,
    fun m hm => by simp [start] at hm, fun n hn => absurd hn (h n n), ?_, ?_, by simp [start]⟩
  · intro n hn; simp [start, World.phase] at hn
  · intro n hn; simp [start, World.phase] at hn

/-- In a phase other than claiming or landed, with the journal present, the
operation has no bytes in the sequence. -/
theorem no_own {allowed : Store Bytes → Nat → Bool} {w : World A} (hi : Inv allowed w)
    {rec : List A} {ph : Phase} (hj : w.journal = some ⟨rec, ph⟩)
    (hph : ∀ n, ph ≠ .claiming n ∧ ph ≠ .landed n) : ∀ n, ¬ Own w.slots n := by
  intro n hn
  have hres : w.result = none := by
    cases hr : w.result with
    | none => rfl
    | some r => have := hi.result_done (by simp [hr]); simp [hj] at this
  rcases hi.own_phase n hn with h | h | h
  · simp [World.phase, hj] at h; exact (hph n).1 h
  · simp [World.phase, hj] at h; exact (hph n).2 h
  · simp [hres] at h

theorem result_none {allowed : Store Bytes → Nat → Bool} {w : World A} (hi : Inv allowed w)
    {j : Journal A} (hj : w.journal = some j) : w.result = none := by
  cases hr : w.result with
  | none => rfl
  | some r => have := hi.result_done (by simp [hr]); simp [hj] at this

/-- **Every step keeps the invariant.** -/
theorem step_inv [DecidableEq A] {allowed : Store Bytes → Nat → Bool} (hp : PrefixOnly allowed) {w w' : World A}
    (st : Step allowed false w w') (hi : Inv allowed w) : Inv allowed w' := by
  cases st with
  | record rec a hj =>
    have hno := no_own hi hj (fun n => by simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun x hx => ?_, fun m hm => ?_, fun n hn => absurd hn (hno n),
      fun n hn => ?_, fun n hn => ?_, fun _ => by simp [hres] at *⟩
    · rcases hi.recorded x hx with ⟨j, hj', hxj⟩ | h
      · rw [hj] at hj'; cases hj'
        exact Or.inl ⟨_, rfl, List.mem_cons_of_mem _ hxj⟩
      · exact Or.inr h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h
      · exact Or.inr h
    · simp [World.phase, hres] at hn
    · simp [World.phase, hres] at hn
  | createArt rec a hj ha =>
    have hno := no_own hi hj (fun n => by simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun x hx => ?_, hi.inflight_ok, hi.own_phase, hi.landed_own, hi.validated,
      hi.result_done⟩
    simp only [List.mem_cons] at hx
    rcases hx with rfl | hx
    · exact Or.inl ⟨_, hj, ha⟩
    · exact hi.recorded x hx
  | claim rec ph n hj hph hn hhead hv =>
    have hno := no_own hi hj (fun k => by rcases hph with rfl | rfl <;> simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun x hx => ?_, fun m hm => ?_, fun k hk => absurd hk (hno k),
      fun k hk => ?_, fun k hk => ?_, fun _ => by simp [hres] at *⟩
    · rcases hi.recorded x hx with ⟨j, hj', hxj⟩ | h
      · rw [hj] at hj'; cases hj'; exact Or.inl ⟨_, rfl, hxj⟩
      · exact Or.inr h
    · simp only [List.mem_cons] at hm
      rcases hm with rfl | hm
      · exact Or.inl (by simp [World.phase])
      · rcases hi.inflight_ok m hm with h | h
        · simp [World.phase, hj] at h; rcases hph with rfl | rfl <;> simp at h
        · exact Or.inr h
    · simp [World.phase, hres] at hk
    · simp [World.phase, hres] at hk
      subst hk
      exact ⟨hn, hhead.1, hv⟩
  | resend rec n hj =>
    refine ⟨hi.ours_here, hi.recorded, fun m hm => ?_, hi.own_phase, hi.landed_own, hi.validated,
      hi.result_done⟩
    simp only [List.mem_cons] at hm
    rcases hm with rfl | hm
    · exact Or.inl (by simp [World.phase, hj])
    · exact hi.inflight_ok m hm
  | land n hn =>
    refine ⟨fun k m hk => ?_, hi.recorded, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun k hk => ?_, hi.result_done⟩
    · simp only at hk
      by_cases hkn : k = n
      · subst hkn
        cases hs : w.slots k with
        | none => rw [Claim.create_self_none _ hs] at hk; cases hk; rfl
        | some x => rw [Claim.create_self_some _ hs] at hk; exact hi.ours_here k m (hs ▸ hk)
      · rw [Claim.create_other _ hkn] at hk; exact hi.ours_here k m hk
    · rcases hi.inflight_ok m (List.mem_of_mem_erase hm) with h | h
      · exact Or.inl h
      · exact Or.inr (create_filled_ne _ _ _ _ h)
    · unfold Own at hk
      simp only at hk
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
    · have := hi.landed_own k hk
      unfold Own at this ⊢
      simp only
      rw [create_keeps_filled _ _ _ _ (by simp [this]), this]
    · obtain ⟨h1, hfill, hv⟩ := hi.validated k hk
      refine ⟨h1, fun i hi1 hi2 => create_filled_ne _ _ _ _ (hfill i hi1 hi2), ?_⟩
      simp only
      rw [allowed_create hp _ _ _ _ hfill]; exact hv
  | other n x =>
    refine ⟨fun k m hk => ?_, hi.recorded, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun k hk => ?_, hi.result_done⟩
    · simp only at hk
      by_cases hkn : k = n
      · subst hkn
        cases hs : w.slots k with
        | none => rw [Claim.create_self_none _ hs] at hk; cases hk
        | some y => rw [Claim.create_self_some _ hs] at hk; exact hi.ours_here k m (hs ▸ hk)
      · rw [Claim.create_other _ hkn] at hk; exact hi.ours_here k m hk
    · rcases hi.inflight_ok m hm with h | h
      · exact Or.inl h
      · exact Or.inr (create_filled_ne _ _ _ _ h)
    · unfold Own at hk
      simp only at hk
      by_cases hkn : k = n
      · subst hkn
        cases hs : w.slots k with
        | none => rw [Claim.create_self_none _ hs] at hk; cases hk
        | some y =>
          rw [Claim.create_self_some _ hs] at hk
          exact hi.own_phase k (by unfold Own; rw [hs]; exact hk)
      · rw [Claim.create_other _ hkn] at hk; exact hi.own_phase k hk
    · have := hi.landed_own k hk
      unfold Own at this ⊢
      simp only
      rw [create_keeps_filled _ _ _ _ (by simp [this]), this]
    · obtain ⟨h1, hfill, hv⟩ := hi.validated k hk
      refine ⟨h1, fun i hi1 hi2 => create_filled_ne _ _ _ _ (hfill i hi1 hi2), ?_⟩
      simp only
      rw [allowed_create hp _ _ _ _ hfill]; exact hv
  | won rec n hj hs =>
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun x hx => ?_, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun k hk => ?_, fun _ => by simp [hres] at *⟩
    · rcases hi.recorded x hx with ⟨j, hj', hxj⟩ | h
      · rw [hj] at hj'; cases hj'; exact Or.inl ⟨_, rfl, hxj⟩
      · exact Or.inr h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h; subst h; exact Or.inr (by simp [hs])
      · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · simp [World.phase, hj] at h; subst h; exact Or.inr (Or.inl (by simp [World.phase]))
      · simp [World.phase, hj] at h
      · simp [hres] at h
    · simp [World.phase, hres] at hk; subst hk; exact hs
    · simp [World.phase, hres] at hk; subst hk
      exact hi.validated _ (Or.inl (by simp [World.phase, hj]))
  | lost rec n x hj hs =>
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun y hy => ?_, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun k hk => ?_, fun _ => by simp [hres] at *⟩
    · rcases hi.recorded y hy with ⟨j, hj', hyj⟩ | h
      · rw [hj] at hj'; cases hj'; exact Or.inl ⟨_, rfl, hyj⟩
      · exact Or.inr h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h; subst h; exact Or.inr (by simp [hs])
      · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · simp [World.phase, hj] at h; subst h; unfold Own at hk; rw [hs] at hk; cases hk
      · simp [World.phase, hj] at h
      · simp [hres] at h
    · simp [World.phase, hres] at hk
    · simp [World.phase, hres] at hk
  | giveUp rec ph n hj hph hhead hv =>
    have hno := no_own hi hj (fun k => by rcases hph with rfl | rfl <;> simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun y hy => ?_, fun m hm => ?_, fun k hk => absurd hk (hno k),
      fun k hk => ?_, fun k hk => ?_, fun _ => by simp [hres] at *⟩
    · rcases hi.recorded y hy with ⟨j, hj', hyj⟩ | h
      · rw [hj] at hj'; cases hj'; exact Or.inl ⟨_, rfl, hyj⟩
      · exact Or.inr h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h; rcases hph with rfl | rfl <;> simp at h
      · exact Or.inr h
    · simp [World.phase, hres] at hk
    · simp [World.phase, hres] at hk
  | undoOne rec a hj =>
    have hno := no_own hi hj (fun k => by simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun y hy => ?_, fun m hm => ?_, fun k hk => absurd hk (hno k),
      fun k hk => ?_, fun k hk => ?_, fun _ => by simp [hres] at *⟩
    · simp only [List.mem_filter, decide_eq_true_eq] at hy
      rcases hi.recorded y hy.1 with ⟨j, hj', hyj⟩ | h
      · rw [hj] at hj'; cases hj'
        exact Or.inl ⟨_, rfl, by simp [hyj, hy.2]⟩
      · exact Or.inr h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h
      · exact Or.inr h
    · simp [World.phase, hres] at hk
    · simp [World.phase, hres] at hk
  | finishUndo hj =>
    have hno := no_own hi hj (fun k => by simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun y hy => ?_, fun m hm => ?_, fun k hk => absurd hk (hno k),
      fun k hk => ?_, fun k hk => ?_, fun _ => rfl⟩
    · rcases hi.recorded y hy with ⟨j, hj', hyj⟩ | ⟨n, h⟩
      · rw [hj] at hj'; cases hj'; simp at hyj
      · simp [hres] at h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h
      · exact Or.inr h
    · simp [World.phase] at hk
    · simp [World.phase] at hk
  | complete rec n hj =>
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun y _ => Or.inr ⟨n, rfl⟩, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun k hk => ?_, fun _ => rfl⟩
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h
      · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · simp [World.phase, hj] at h
      · simp [World.phase, hj] at h; subst h; exact Or.inr (Or.inr rfl)
      · simp [hres] at h
    · simp [World.phase] at hk; subst hk
      exact hi.landed_own _ (Or.inl (by simp [World.phase, hj]))
    · simp [World.phase] at hk; subst hk
      exact hi.validated _ (Or.inr (Or.inl (by simp [World.phase, hj])))
  | forget rec n hf => cases hf

theorem steps_inv [DecidableEq A] {allowed : Store Bytes → Nat → Bool} (hp : PrefixOnly allowed) {w w' : World A}
    (run : Steps allowed false w w') (hi : Inv allowed w) : Inv allowed w' := by
  induction run with
  | refl => exact hi
  | step s _ ih => exact ih (step_inv hp s hi)

/-- **The claim loop and journal.** From validation over any slots holding
none of its bytes, through any interleaving of its own steps, crashes and
restarts, late-landing requests and other devices' claims:

- its bytes occupy at most one slot (it never lands twice);
- a landed operation's bytes are at its slot, whose predecessors were all
  filled when it validated, and validation held against them;
- an undone operation has no bytes in the sequence, now or later, and none of
  its recorded artifacts remains in storage. -/
theorem operation_outcome [DecidableEq A] {allowed : Store Bytes → Nat → Bool} (hp : PrefixOnly allowed)
    (s0 : Store Bytes) (h0 : ∀ n m, s0 n ≠ some (.ours m)) {w : World A}
    (run : Steps allowed false (start s0) w) :
    (∀ n m, Own w.slots n → Own w.slots m → n = m) ∧
    (∀ n, w.result = some (.landed n) →
      Own w.slots n ∧ 1 ≤ n ∧ (∀ i, 1 ≤ i → i < n → w.slots i ≠ none) ∧ allowed w.slots n = true) ∧
    (w.result = some .undone → (∀ n, ¬ Own w.slots n) ∧ w.artifacts = []) := by
  have hi := steps_inv hp run (inv_start allowed s0 h0)
  refine ⟨fun n m hn hm => ?_, fun n hn => ?_, fun hu => ⟨fun n hn => ?_, ?_⟩⟩
  · rcases hi.own_phase n hn with h | h | h <;> rcases hi.own_phase m hm with h' | h' | h'
    all_goals first
      | (rw [h] at h'; simp at h'; try omega)
      | (have := hi.result_done (by simp [h]); simp [World.phase, this] at h')
      | (have := hi.result_done (by simp [h']); simp [World.phase, this] at h)
  · exact ⟨hi.landed_own n (Or.inr hn), hi.validated n (Or.inr (Or.inr hn))⟩
  · have hj := hi.result_done (by simp [hu])
    rcases hi.own_phase n hn with h | h | h
    · simp [World.phase, hj] at h
    · simp [World.phase, hj] at h
    · rw [hu] at h; cases h
  · have hj := hi.result_done (by simp [hu])
    cases ha : w.artifacts with
    | nil => rfl
    | cons a rest =>
      rcases hi.recorded a (by simp [ha]) with ⟨j, hj', _⟩ | ⟨n, hn⟩
      · simp [hj] at hj'
      · rw [hu] at hn; cases hn

/-- **Counterexample: a restart that forgets its claim lands twice.** The
operation claims slot 1, the request lands, and a restart that does not
resolve its recorded claim validates again, claims slot 2 and lands there too. -/
theorem forgetful_lands_twice :
    ∃ w : World Nat, Steps (fun _ _ => true) true (start (fun _ => none)) w ∧
      Own w.slots 1 ∧ Own w.slots 2 := by
  refine ⟨_, Steps.step (Step.claim _ [] .prep 1 rfl (Or.inl rfl) (Nat.le_refl _)
      ⟨fun i h1 h2 => by omega, rfl⟩ rfl)
    (Steps.step (Step.land _ 1 (by simp))
    (Steps.step (Step.forget _ [] 1 rfl rfl)
    (Steps.step (Step.claim _ [] .prep 2 rfl (Or.inl rfl) (by decide) ⟨fun i h1 h2 => ?_, ?_⟩ rfl)
    (Steps.step (Step.land _ 2 (by simp)) (Steps.refl _))))), ?_, ?_⟩
  · have : i = 1 := by omega
    subst this; simp [Claim.create, start]
  · simp [Claim.create, start]
  · simp [Own, Claim.create, start]
  · simp [Own, Claim.create, start]

end CovenSequence.Operation
