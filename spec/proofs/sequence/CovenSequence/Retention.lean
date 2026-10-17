import CovenSequence.Reader
import CovenSequence.Claim

/-!
# Claims, stale creates and retention

What happens to storage while devices read it:

- a claim stores the next slot (its prepared body, if any, uploaded first);
- a stale create of an earlier slot: create-once refuses it while the slot is
  kept, and stores it if the slot was deleted;
- retention writes the floor record conditionally, never backwards: every
  audience's snapshot and the first kept slot only move forward, each
  audience's snapshot is validly announced with its body stored, and the
  first kept slot is covered by every audience's snapshot;
- retention deletes slots below the first kept slot, and snapshot bodies no
  floor names and no raise or reset above the floor needs;
- a snapshot body is uploaded to a fresh path no applied raise or reset names.

There is no separate step that advertises a floor before it takes effect:
the floor record is written, then slots below it are deleted. A reader's
floor read after its fetch (`ReadStep.apply`) is what makes this safe.
-/

namespace CovenSequence

variable {S : Sig} (R : Rules S)

/-- Storage steps, for the claimed sequence `seq`. -/
inductive StoreStep (seq : Nat → Slot S) : Store S → Store S → Prop
  | claim (st : Store S)
      (hprepared : ∀ A au v c p, seq (st.head + 1) = .prepare au A v c p →
        AppliedTo R seq A (st.head + 1) →
        ∃ L' P', st.body p = some (L', P', preparedRows R v p (rowsAt R seq A c)))
      (hbody : ∀ A au v c p, seq (st.head + 1) = .boundary au A v c p → AppliedTo R seq A (st.head + 1) →
        ∃ L' P', st.body p = some (L', P', preparedRows R v p (rowsAt R seq A c))) :
      StoreStep seq st
        { st with
          slot := fun i => if i = st.head + 1 then some (seq (st.head + 1)) else st.slot i
          first := fun i => if i = st.head + 1 then some (seq (st.head + 1)) else st.first i
          head := st.head + 1 }
  | create (st : Store S) (i : Nat) (x : Slot S) (hi : 1 ≤ i) (hih : i ≤ st.head) :
      StoreStep seq st { st with slot := Claim.create st.slot i x }
  | writeFloor (st : Store S) (F : Floor S) (hstart : st.floor.start ≤ F.start)
      (hcov : ∀ A, st.floor.covers A ≤ F.covers A) (hle : ∀ A, F.start ≤ F.covers A + 1)
      (hhead : ∀ A, F.covers A ≤ st.head)
      (hann : ∀ A, F.covers A = 0 ∨ ∃ a au, 1 ≤ a ∧ a ≤ st.head ∧
        seq a = .snapshot au A (F.covers A) (F.path A) ∧ AppliedTo R seq A a)
      (hbody : ∀ A, st.body (F.path A) = some (stateAt R seq A (F.covers A))) :
      StoreStep seq st { st with floor := F }
  | delete (st : Store S) (i : Nat) (hi : i < st.floor.start) :
      StoreStep seq st { st with slot := fun j => if j = i then none else st.slot j }
  | deleteBody (st : Store S) (p : S.Path) (hp : ∀ A, p ≠ st.floor.path A)
      (hraise : ∀ A r au v c, 1 ≤ r → r ≤ st.head → seq r = .boundary au A v c p → AppliedTo R seq A r →
        r ≤ st.floor.covers A) :
      StoreStep seq st { st with body := fun q => if q = p then none else st.body q }
  | upload (st : Store S) (p : S.Path) (b : Ledger S × AudLedger S × S.Rows) (hp : st.body p = none)
      (hfree : ∀ A r au v c, 1 ≤ r → r ≤ st.head → seq r = .boundary au A v c p → ¬ AppliedTo R seq A r) :
      StoreStep seq st { st with body := fun q => if q = p then some b else st.body q }

theorem store_mono {seq : Nat → Slot S} {st st' : Store S} (s : StoreStep R seq st st') :
    st.floor.start ≤ st'.floor.start ∧ st.head ≤ st'.head := by
  cases s with
  | claim => simp
  | writeFloor F hstart => exact ⟨hstart, Nat.le_refl _⟩
  | _ => simp

/-- **Storage steps keep storage good.** -/
theorem store_good {seq : Nat → Slot S} {st st' : Store S} (s : StoreStep R seq st st')
    (g : Good R st seq) : Good R st' seq := by
  cases s with
  | claim hprepared hbody =>
    refine ⟨fun i h1 h2 => ?_, fun i hi => ?_, fun i hi => ?_, fun i hi => ?_, g.start_le,
      fun A => by simp only; have := g.covers_le A; omega, g.floor_body, fun A => ?_,
      fun A r au v c p h1 h2 h3 h4 => ?_⟩
    · simp only at h2 ⊢
      by_cases hi : i = st.head + 1
      · simp [hi]
      · simp only [hi, ite_false]; exact g.first_genuine i h1 (by omega)
    · simp only at hi ⊢
      have : i ≠ st.head + 1 := by omega
      simp only [this, ite_false]; exact g.first_out i (by omega)
    · simp only at hi ⊢
      have : i ≠ st.head + 1 := by omega
      simp only [this, ite_false]; exact g.slot_out i (by omega)
    · simp only at hi ⊢
      by_cases h : i = st.head + 1
      · simp [h]
      · simp only [h, ite_false]; exact g.kept_genuine i hi
    · rcases g.floor_announced A with h | ⟨a, au, h1, h2, h3, h4⟩
      · exact Or.inl h
      · exact Or.inr ⟨a, au, h1, by simp only; omega, h3, h4⟩
    · simp only at h2 ⊢
      by_cases hr : r = st.head + 1
      · subst hr
        obtain ⟨L', P', hb⟩ := hbody A au v c p h3 h4
        refine ⟨fun _ => ⟨L', P', hb⟩, fun L'' P'' rows hb' => ?_⟩
        rw [hb] at hb'
        have := Option.some.inj hb'
        simp only [Prod.mk.injEq] at this
        exact this.2.2.symm
      · exact g.boundary_body A r au v c p h1 (by omega) h3 h4
  | create i x hi hih =>
    refine ⟨g.first_genuine, g.first_out, fun j hj => ?_, fun j hj => ?_, g.start_le, g.covers_le,
      g.floor_body, g.floor_announced, g.boundary_body⟩
    · simp only at hj ⊢
      by_cases hji : j = i
      · subst hji; omega
      · rw [Claim.create_other _ hji]; exact g.slot_out j hj
    · simp only at hj ⊢
      have hk := g.kept_genuine j hj
      by_cases hji : j = i
      · subst hji
        have hf := g.first_genuine j hi hih
        have hs : st.slot j = some (seq j) := hk.trans hf
        rw [Claim.create_self_some _ hs, hf]
      · rw [Claim.create_other _ hji]; exact hk
  | writeFloor F hstart hcov hle hhead hann hbody =>
    exact ⟨g.first_genuine, g.first_out, g.slot_out, fun j hj => g.kept_genuine j (by simp only at hj; omega),
      hle, hhead, hbody, hann, fun A r au v c p h1 h2 h3 h4 =>
        ⟨fun hlt => (g.boundary_body A r au v c p h1 h2 h3 h4).1 (by have := hcov A; simp only at hlt; omega),
         (g.boundary_body A r au v c p h1 h2 h3 h4).2⟩⟩
  | delete i hi =>
    refine ⟨g.first_genuine, g.first_out, fun j hj => ?_, fun j hj => ?_, g.start_le, g.covers_le,
      g.floor_body, g.floor_announced, g.boundary_body⟩
    · simp only; by_cases h : j = i
      · simp [h]
      · simp only [h, ite_false]; exact g.slot_out j hj
    · simp only at hj ⊢
      have : j ≠ i := by omega
      simp only [this, ite_false]; exact g.kept_genuine j hj
  | deleteBody p hp hraise =>
    refine ⟨g.first_genuine, g.first_out, g.slot_out, g.kept_genuine, g.start_le, g.covers_le,
      fun A => ?_, g.floor_announced, fun A r au v c q h1 h2 h3 h4 => ⟨fun hlt => ?_, fun L' P' rows hb => ?_⟩⟩
    · simp only [Ne.symm (hp A), ite_false]; exact g.floor_body A
    · by_cases hq : q = p
      · subst hq; have := hraise A r au v c h1 h2 h3 h4; simp only at hlt; omega
      · simp only [hq, ite_false]; exact (g.boundary_body A r au v c q h1 h2 h3 h4).1 hlt
    · by_cases hq : q = p
      · subst hq; simp at hb
      · simp only [hq, ite_false] at hb; exact (g.boundary_body A r au v c q h1 h2 h3 h4).2 L' P' rows hb
  | upload p b hp hfree =>
    refine ⟨g.first_genuine, g.first_out, g.slot_out, g.kept_genuine, g.start_le, g.covers_le,
      fun A => ?_, g.floor_announced, fun A r au v c q h1 h2 h3 h4 => ?_⟩
    · have hne : st.floor.path A ≠ p := by
        intro he; have := g.floor_body A; rw [he, hp] at this; cases this
      simp only [hne, ite_false]; exact g.floor_body A
    · have hne : q ≠ p := by intro he; subst he; exact hfree A r au v c h1 h2 h3 h4
      simp only [hne, ite_false]; exact g.boundary_body A r au v c q h1 h2 h3 h4

/-- **Storage steps keep every sound reader sound**: its buffered records
were checked against a floor that only moves forward. -/
theorem store_reader {seq : Nat → Slot S} {st st' : Store S} (s : StoreStep R seq st st')
    {A : S.Aud} {d : Reader S} (hd : ReaderOk R st seq A d) : ReaderOk R st' seq A d := by
  obtain ⟨hs, hh⟩ := store_mono R s
  exact ⟨hd.ledger, hd.aud, hd.rows, by have := hd.le_head; omega, fun i x hb hf => by
    obtain ⟨h1, h2, h3⟩ := hd.buf i x hb (by omega)
    exact ⟨h1, by omega, h3⟩⟩

inductive StoreSteps (seq : Nat → Slot S) : Store S → Store S → Prop
  | refl (st : Store S) : StoreSteps seq st st
  | step {st st' st'' : Store S} : StoreStep R seq st st' → StoreSteps seq st' st'' → StoreSteps seq st st''

theorem stores_good {seq : Nat → Slot S} {st st' : Store S} (run : StoreSteps R seq st st')
    (g : Good R st seq) : Good R st' seq := by
  induction run with
  | refl => exact g
  | step s _ ih => exact ih (store_good R s g)

/-- **The claimant's floor check.** A claimant that saw its bytes `x` at slot
`i`, and then, after any storage steps, reads a floor whose first kept slot is
not past `i`, holds the genuine slot `i`. If the floor has passed `i`, the claim
is void and its batch is claimed again. -/
theorem claim_floor_check {seq : Nat → Slot S} {st st' : Store S} (g : Good R st seq)
    {i : Nat} {x : Slot S} (hx : st.slot i = some x) (run : StoreSteps R seq st st')
    (hf : st'.floor.start ≤ i) : 1 ≤ i ∧ i ≤ st'.head ∧ x = seq i := by
  have h0 : st.floor.start ≤ i → 1 ≤ i ∧ i ≤ st.head ∧ x = seq i := by
    intro hfi
    rw [g.kept_genuine i hfi] at hx
    have h1 : 1 ≤ i ∧ i ≤ st.head := by
      by_cases hz : i = 0 ∨ st.head < i
      · rw [g.first_out i hz] at hx; cases hx
      · omega
    rw [g.first_genuine i h1.1 h1.2] at hx
    exact ⟨h1.1, h1.2, (Option.some.inj hx).symm⟩
  clear hx g
  revert h0
  induction run with
  | refl => intro h0; exact h0 hf
  | step s _ ih =>
    intro h0
    obtain ⟨hs, hh⟩ := store_mono R s
    exact ih hf (fun hfi => by
      obtain ⟨a, b, c⟩ := h0 (by omega)
      exact ⟨a, by omega, c⟩)

end CovenSequence
