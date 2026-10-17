import CovenSequence.Unique

/-!
# Readers, the floor record and deleting covered slots

Storage holds slot records at create-once paths, snapshot bodies, and one
floor record: the snapshot each audience relies on (its coverage and path)
and the first kept slot. Retention writes the floor conditionally and never
backwards, then deletes slots below its first kept slot. A deleted path is
free again, so a stale create can store bytes there; `first` is the ghost
record of each slot's first bytes, its genuine content (`Claim`).

A device reads one audience with one reader. A reader of audience `A`:

- fetches slot records into a buffer;
- applies its next slot from the buffer once a floor read, after the fetch,
  shows the slot at or above the first kept slot. A slot with a part or
  announcement for `A` needs `A`'s key. Other audiences' parts are not read:
  the slot's clear metadata says they are not `A`'s;
- applies a valid raise or reset of `A` by loading its prepared body; a raise
  replays the suffix after coverage, while a reset discards that suffix;
- reloads from the floor's snapshot for `A`, with `A`'s key, when that covers
  more than it applied.

`Good` is what claims, stale creates, retention and body uploads keep true:
at or above the first kept slot every record is genuine; every audience's
snapshot covers the slot before the first kept slot; each audience's floor
snapshot was validly announced and its body is the state it covers; a valid
boundary's body, if present, holds its prepared rows, and it is present
while the floor has not covered the boundary.
-/

namespace CovenSequence

variable {S : Sig} (R : Rules S)

/-- The floor record. -/
structure Floor (S : Sig) where
  /-- The first kept slot. -/
  start : Nat
  covers : S.Aud → Nat
  path : S.Aud → S.Path

/-- Storage, with the ghost first bytes and head. -/
structure Store (S : Sig) where
  slot : Nat → Option (Slot S)
  first : Nat → Option (Slot S)
  head : Nat
  floor : Floor S
  body : S.Path → Option (Ledger S × AudLedger S × S.Rows)

/-- A reader of one audience. -/
structure Reader (S : Sig) where
  pos : Nat
  ledger : Ledger S
  aud : AudLedger S
  rows : S.Rows
  /-- Slot records fetched and not yet checked against the floor. -/
  buf : Nat → Option (Slot S)

/-- The rows after applying `x` at `k`, when it is not a valid raise or reset of `A`. -/
def rowsStep (A : S.Aud) (L : Ledger S) (P : AudLedger S) (rows : S.Rows) (k : Nat) (x : Slot S) :
    S.Rows :=
  if touches A x && audValid R L P k A x then applyWrite R A P.version P.lastReset rows x else rows

/-- The ordinary apply rule's buffer and post-fetch floor check. -/
def Reader.next {α : Type} (pos start : Nat) (buf : Nat → Option α) : Option α :=
  if start ≤ pos + 1 then buf (pos + 1) else none

theorem Reader.next_eq_some {α : Type} (pos start : Nat) (buf : Nat → Option α) (x : α) :
    Reader.next pos start buf = some x ↔ start ≤ pos + 1 ∧ buf (pos + 1) = some x := by
  unfold Reader.next
  split
  · simp_all
  · rename_i h
    simp only [reduceCtorEq, false_iff, not_and]
    intro le
    omega

/-- Consume consecutive fetched slots; a gap ends the prefix. Fuel bounds the
number of fetched records, not their numeric distance from the reader. -/
def Reader.prefix (start : Nat) (fetched : List Nat) : Nat → Nat → List Nat
  | _, 0 => []
  | pos, fuel + 1 =>
    match Reader.next pos start (fun i => if i ∈ fetched then some i else none) with
    | none => []
    | some i => i :: Reader.prefix start fetched i fuel

/-- `none` requires a floor reload; `some []` waits for the next slot. This
projects ordinary application with its audience key, without boundary loads. -/
def Reader.checkedSlots (pos start : Nat) (fetched : List Nat) : Option (List Nat) :=
  if start ≤ pos + 1 then some (Reader.prefix start fetched pos fetched.length) else none

theorem Reader.prefix_sound (start : Nat) (fetched : List Nat) (pos fuel : Nat) :
    ∀ i ∈ Reader.prefix start fetched pos fuel, i ∈ fetched ∧ start ≤ i ∧ pos < i := by
  induction fuel generalizing pos with
  | zero => simp [Reader.prefix]
  | succ fuel ih =>
    simp only [Reader.prefix]
    split
    · simp
    · rename_i next h
      have check := (Reader.next_eq_some pos start _ next).mp h
      have same : next = pos + 1 ∧ next ∈ fetched := by
        split at check <;> simp_all
      obtain ⟨rfl, member⟩ := same
      intro i hi
      rcases List.mem_cons.mp hi with rfl | hi
      · exact ⟨member, check.1, by omega⟩
      · have h := ih (pos + 1) i hi
        exact ⟨h.1, h.2.1, by omega⟩

/-- A reader's steps against fixed storage, holding `A`'s key or not. -/
inductive ReadStep (reloadAllowed : Bool) (A : S.Aud) (key : Bool) (st : Store S) : Reader S → Reader S → Prop
  | fetch (d : Reader S) (i : Nat) :
      ReadStep reloadAllowed A key st d { d with buf := fun j => if j = i then st.slot i else d.buf j }
  | apply (d : Reader S) (x : Slot S) (hb : d.buf (d.pos + 1) = some x)
      (hf : st.floor.start ≤ d.pos + 1)
      (hnr : ∀ au v c p, x = .boundary au A v c p → audValid R d.ledger d.aud (d.pos + 1) A x = false)
      (hk : touches A x = true → key = true) :
      ReadStep reloadAllowed A key st d ⟨d.pos + 1, ledgerStep R d.ledger (d.pos + 1) x,
        audStep R d.ledger d.aud (d.pos + 1) A x, rowsStep R A d.ledger d.aud d.rows (d.pos + 1) x, d.buf⟩
  | boundary (d : Reader S) (au : S.Dev) (v : Boundary) (c : Nat) (p : S.Path)
      (hb : d.buf (d.pos + 1) = some (.boundary au A v c p)) (hf : st.floor.start ≤ d.pos + 1)
      (hv : audValid R d.ledger d.aud (d.pos + 1) A (.boundary au A v c p) = true) (hk : key = true)
      (L' : Ledger S) (P' : AudLedger S) (rows : S.Rows) (hbody : st.body p = some (L', P', rows))
      (get : Nat → Slot S)
      (hget : ∀ i, c < i → i ≤ d.pos → d.buf i = some (get i) ∧ st.floor.start ≤ i) :
      ReadStep reloadAllowed A key st d ⟨d.pos + 1, ledgerStep R d.ledger (d.pos + 1) (.boundary au A v c p),
        audStep R d.ledger d.aud (d.pos + 1) A (.boundary au A v c p),
        finishBoundary R A get d.aud.refused v d.aud.lastReset rows (c + 1) (d.pos - c), d.buf⟩
  | reload (d : Reader S) (L : Ledger S) (P : AudLedger S) (rows : S.Rows)
      (hc : d.pos < st.floor.covers A) (hk : key = true) (hload : reloadAllowed = true)
      (hb : st.body (st.floor.path A) = some (L, P, rows)) :
      ReadStep reloadAllowed A key st d ⟨st.floor.covers A, L, P, rows, d.buf⟩

/-- The executable check supplies exactly the ordinary apply constructor's
buffer and floor premises; key and boundary admission remain explicit. -/
theorem Reader.next_apply {A : S.Aud} {key : Bool} {st : Store S} (d : Reader S) (x : Slot S)
    (next : Reader.next d.pos st.floor.start d.buf = some x)
    (hnr : ∀ au v c p, x = .boundary au A v c p → audValid R d.ledger d.aud (d.pos + 1) A x = false)
    (hk : touches A x = true → key = true) :
    ReadStep R true A key st d ⟨d.pos + 1, ledgerStep R d.ledger (d.pos + 1) x,
      audStep R d.ledger d.aud (d.pos + 1) A x,
      rowsStep R A d.ledger d.aud d.rows (d.pos + 1) x, d.buf⟩ :=
  let checked := (Reader.next_eq_some _ _ _ _).mp next
  ReadStep.apply d x checked.2 checked.1 hnr hk

inductive ReadSteps (A : S.Aud) (key : Bool) (st : Store S) : Reader S → Reader S → Prop
  | refl (d : Reader S) : ReadSteps A key st d d
  | step {d d' d'' : Reader S} : ReadStep R true A key st d d' → ReadSteps A key st d' d'' →
      ReadSteps A key st d d''

theorem ReadSteps.trans {A : S.Aud} {key : Bool} {st : Store S} {d d' d'' : Reader S}
    (h₁ : ReadSteps R A key st d d') (h₂ : ReadSteps R A key st d' d'') : ReadSteps R A key st d d'' := by
  induction h₁ with
  | refl => exact h₂
  | step s _ ih => exact ReadSteps.step s (ih h₂)

/-- Storage as claims, stale creates and retention keep it, for the claimed
sequence `seq`. -/
structure Good (st : Store S) (seq : Nat → Slot S) : Prop where
  first_genuine : ∀ i, 1 ≤ i → i ≤ st.head → st.first i = some (seq i)
  first_out : ∀ i, (i = 0 ∨ st.head < i) → st.first i = none
  slot_out : ∀ i, (i = 0 ∨ st.head < i) → st.slot i = none
  /-- At or above the first kept slot, records are the genuine first bytes. -/
  kept_genuine : ∀ i, st.floor.start ≤ i → st.slot i = st.first i
  start_le : ∀ A, st.floor.start ≤ st.floor.covers A + 1
  covers_le : ∀ A, st.floor.covers A ≤ st.head
  floor_body : ∀ A, st.body (st.floor.path A) = some (stateAt R seq A (st.floor.covers A))
  floor_announced : ∀ A, st.floor.covers A = 0 ∨
    ∃ a au, 1 ≤ a ∧ a ≤ st.head ∧ seq a = .snapshot au A (st.floor.covers A) (st.floor.path A) ∧
      AppliedTo R seq A a
  boundary_body : ∀ A r au v c p, 1 ≤ r → r ≤ st.head → seq r = .boundary au A v c p → AppliedTo R seq A r →
    (st.floor.covers A < r → ∃ L' P', st.body p = some (L', P', preparedRows R v p (rowsAt R seq A c))) ∧
    (∀ L' P' rows, st.body p = some (L', P', rows) → rows = preparedRows R v p (rowsAt R seq A c))

/-- A reader of `A` holds the state of the prefix it applied, and every buffered
record at or above the first kept slot is genuine. -/
structure ReaderOk (st : Store S) (seq : Nat → Slot S) (A : S.Aud) (d : Reader S) : Prop where
  ledger : d.ledger = ledgerAt R seq d.pos
  aud : d.aud = audAt R seq A d.pos
  rows : d.rows = rowsAt R seq A d.pos
  le_head : d.pos ≤ st.head
  buf : ∀ i x, d.buf i = some x → st.floor.start ≤ i → 1 ≤ i ∧ i ≤ st.head ∧ x = seq i

/-! ## Monotone audience facts -/

theorem step_maxCover (L : Ledger S) (P : AudLedger S) (k : Nat) (A : S.Aud) (x : Slot S) :
    P.maxCover ≤ (audStep R L P k A x).maxCover := by
  unfold audStep
  split
  · split
    · cases x with
      | writes a g parts => simp only [audEffect]; split <;> simp
      | snapshot a b c p | prepare a b v c p => simp only [audEffect]; exact Nat.le_max_left _ _
      | _ => simp [audEffect]
    · simp [refuseAud]
  · exact Nat.le_refl _

theorem step_lastBoundary (L : Ledger S) (P : AudLedger S) (k : Nat) (A : S.Aud) (x : Slot S) :
    P.lastBoundary ≤ (audStep R L P k A x).lastBoundary := by
  unfold audStep
  split
  · split
    · rename_i hv
      cases x with
      | writes a g parts => simp only [audEffect]; split <;> simp
      | boundary a b v c p =>
        simp only [audValid, Bool.and_eq_true, decide_eq_true_eq] at hv
        simp only [audEffect]; omega
      | _ => simp [audEffect]
    · simp [refuseAud]
  · exact Nat.le_refl _

theorem maxCover_mono (seq : Nat → Slot S) (A : S.Aud) (j : Nat) :
    ∀ k, j ≤ k → (audAt R seq A j).maxCover ≤ (audAt R seq A k).maxCover
  | k, hk => by
    induction k with
    | zero => have : j = 0 := by omega
              subst this; exact Nat.le_refl _
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; exact Nat.le_refl _
      · exact Nat.le_trans (ih (by omega)) (step_maxCover R _ _ _ _ _)

theorem lastBoundary_mono (seq : Nat → Slot S) (A : S.Aud) (j : Nat) :
    ∀ k, j ≤ k → (audAt R seq A j).lastBoundary ≤ (audAt R seq A k).lastBoundary
  | k, hk => by
    induction k with
    | zero => have : j = 0 := by omega
              subst this; exact Nat.le_refl _
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; exact Nat.le_refl _
      · exact Nat.le_trans (ih (by omega)) (step_lastBoundary R _ _ _ _ _)

theorem applied_aud (seq : Nat → Slot S) (A : S.Aud) {j : Nat} (hj : 1 ≤ j) (h : AppliedTo R seq A j) :
    audAt R seq A j = audEffect (audAt R seq A (j - 1)) j A (seq j) := by
  have hjeq : j = (j - 1) + 1 := by omega
  obtain ⟨ht, hv⟩ := h
  rw [hjeq, audAt_succ, ← hjeq]
  simp [audStep, ht, hv]

theorem announced_maxCover (seq : Nat → Slot S) (A : S.Aud) {a : Nat} {au : S.Dev} {c : Nat}
    {p : S.Path} (ha : 1 ≤ a) (hs : seq a = .snapshot au A c p) (happ : AppliedTo R seq A a) :
    c ≤ (audAt R seq A a).maxCover := by
  rw [applied_aud R seq A ha happ, hs]
  simp only [audEffect]; exact Nat.le_max_right _ _

theorem boundary_lastBoundary (seq : Nat → Slot S) (A : S.Aud) {r : Nat} {au : S.Dev} {v : Boundary} {c : Nat}
    {p : S.Path} (hr : 1 ≤ r) (hs : seq r = .boundary au A v c p) (happ : AppliedTo R seq A r) :
    (audAt R seq A r).lastBoundary = r := by
  rw [applied_aud R seq A hr happ, hs]
  simp [audEffect]

/-! ## Soundness -/

/-- A buffered record that passes the floor check is the genuine slot. -/
theorem checked_genuine {st : Store S} {seq : Nat → Slot S} {A : S.Aud} {d : Reader S}
    (hd : ReaderOk R st seq A d) {i : Nat} {x : Slot S} (hb : d.buf i = some x)
    (hf : st.floor.start ≤ i) : 1 ≤ i ∧ i ≤ st.head ∧ x = seq i :=
  hd.buf i x hb hf

/-- **Reading is sound.** Every reader step from a sound reader on good storage
gives a sound reader, whatever was deleted or recreated below the floor. -/
theorem read_ok {reloadAllowed : Bool} {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) {A : S.Aud} {key : Bool}
    {d d' : Reader S} (hd : ReaderOk R st seq A d) (step : ReadStep R reloadAllowed A key st d d') :
    ReaderOk R st seq A d' := by
  cases step with
  | fetch i =>
    refine ⟨hd.ledger, hd.aud, hd.rows, hd.le_head, fun j x hb hf => ?_⟩
    simp only at hb
    by_cases hj : j = i
    · subst hj
      simp only [ite_true] at hb
      rw [g.kept_genuine j hf] at hb
      have h1 : 1 ≤ j ∧ j ≤ st.head := by
        by_cases hz : j = 0 ∨ st.head < j
        · rw [g.first_out j hz] at hb; cases hb
        · omega
      rw [g.first_genuine j h1.1 h1.2] at hb
      exact ⟨h1.1, h1.2, (Option.some.inj hb).symm⟩
    · simp only [hj, ite_false] at hb
      exact hd.buf j x hb hf
  | apply x hb hf hnr hk =>
    obtain ⟨_, hhead, hxs⟩ := checked_genuine R hd hb hf
    subst hxs
    refine ⟨by simp only; rw [hd.ledger, ledgerAt_succ], by simp only; rw [hd.ledger, hd.aud, audAt_succ],
      ?_, hhead, hd.buf⟩
    simp only [rowsStep]
    rw [hd.ledger, hd.aud, hd.rows]
    cases hs : seq (d.pos + 1) with
    | boundary au a v c p =>
      rw [rowsAt_boundary R seq A d.pos hs]
      by_cases ha : a = A
      · subst ha
        have hf' := hnr au v c p hs
        rw [hd.ledger, hd.aud, hs] at hf'
        simp [hf', touches]
      · have : touches A (Slot.boundary au a v c p) = false := by simp [touches, ha]
        simp [this, ha]
    | _ =>
      have hnr' : ∀ au a v c p, seq (d.pos + 1) ≠ .boundary au a v c p := by intro _ _ _ _ _; simp [hs]
      rw [rowsAt_other R seq A d.pos hnr', hs]
  | boundary au v c p hb hf hv hk L' P' rows hbody get hget =>
    obtain ⟨_, hhead, hxs⟩ := checked_genuine R hd hb hf
    rw [hd.ledger, hd.aud] at hv
    refine ⟨by simp only; rw [hd.ledger, ledgerAt_succ, ← hxs],
      by simp only; rw [hd.ledger, hd.aud, audAt_succ, ← hxs], ?_, hhead, hd.buf⟩
    simp only
    have happ : AppliedTo R seq A (d.pos + 1) := by
      refine ⟨by rw [← hxs]; simp [touches], ?_⟩
      simp only [Nat.add_sub_cancel]; rw [← hxs]; exact hv
    have hrows : rows = preparedRows R v p (rowsAt R seq A c) :=
      (g.boundary_body A (d.pos + 1) au v c p (by omega) hhead hxs.symm happ).2 L' P' rows hbody
    rw [rowsAt_boundary R seq A d.pos hxs.symm]
    simp only [true_and, hv, ite_true]
    rw [hrows, hd.aud]
    have hc : c < d.pos + 1 := by simp [audValid] at hv; omega
    apply finishBoundary_congr
    intro i h1 h2
    obtain ⟨hgi, hfi⟩ := hget i (by omega) (by omega)
    exact (checked_genuine R hd hgi hfi).2.2
  | reload L P rows hc hk hload hb =>
    rw [g.floor_body A] at hb
    have := Option.some.inj hb
    simp only [stateAt, Prod.mk.injEq] at this
    obtain ⟨h1, h2, h3⟩ := this
    exact ⟨h1.symm, h2.symm, h3.symm, g.covers_le A, hd.buf⟩

theorem reads_ok {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) {A : S.Aud} {key : Bool}
    {d d' : Reader S} (hd : ReaderOk R st seq A d) (run : ReadSteps R A key st d d') :
    ReaderOk R st seq A d' := by
  induction run with
  | refl => exact hd
  | step s _ ih => exact ih (read_ok R g hd s)

/-! ## Progress -/

/-- The reader with slots `lo .. lo+n-1` fetched. -/
def fetched (st : Store S) (d : Reader S) (lo n : Nat) : Reader S :=
  { d with buf := fun i => if lo ≤ i ∧ i < lo + n then st.slot i else d.buf i }

@[simp] theorem fetched_pos (st : Store S) (d : Reader S) (lo n : Nat) :
    (fetched st d lo n).pos = d.pos := rfl
@[simp] theorem fetched_ledger (st : Store S) (d : Reader S) (lo n : Nat) :
    (fetched st d lo n).ledger = d.ledger := rfl
@[simp] theorem fetched_aud (st : Store S) (d : Reader S) (lo n : Nat) :
    (fetched st d lo n).aud = d.aud := rfl
@[simp] theorem fetched_rows (st : Store S) (d : Reader S) (lo n : Nat) :
    (fetched st d lo n).rows = d.rows := rfl
theorem fetched_buf_in (st : Store S) (d : Reader S) {lo n i : Nat} (h1 : lo ≤ i) (h2 : i < lo + n) :
    (fetched st d lo n).buf i = st.slot i := by
  simp [fetched, h1, h2]

theorem fetched_zero (st : Store S) (d : Reader S) (lo : Nat) : fetched st d lo 0 = d := by
  simp only [fetched]
  congr
  funext i
  split
  · rename_i h; exact absurd h (by omega)
  · rfl

theorem fetched_succ (st : Store S) (d : Reader S) (lo n : Nat) :
    fetched st d lo (n + 1) =
      { fetched st d lo n with
        buf := fun j => if j = lo + n then st.slot (lo + n) else (fetched st d lo n).buf j } := by
  simp only [fetched]
  congr
  funext i
  by_cases h : i = lo + n
  · subst h; simp
  · by_cases h2 : lo ≤ i ∧ i < lo + n
    · have : lo ≤ i ∧ i < lo + (n + 1) := by omega
      simp [this, h2, h]
    · have : ¬ (lo ≤ i ∧ i < lo + (n + 1)) := by omega
      simp only [h, h2, ite_false]
      split
      · rename_i h3; exact absurd h3 this
      · rfl

theorem fetch_run (A : S.Aud) (key : Bool) (st : Store S) (d : Reader S) (lo : Nat) :
    ∀ n, ReadSteps R A key st d (fetched st d lo n)
  | 0 => by rw [fetched_zero]; exact ReadSteps.refl d
  | n + 1 => by
    rw [fetched_succ]
    exact ReadSteps.trans R (fetch_run A key st d lo n)
      (ReadSteps.step (ReadStep.fetch _ (lo + n)) (ReadSteps.refl _))

/-- **Progress.** With `A`'s key, a sound reader of `A` below the head can fetch
and then step forward. -/
theorem read_progress {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) {A : S.Aud}
    {d : Reader S} (hd : ReaderOk R st seq A d) (hlt : d.pos < st.head) :
    ∃ lo n d', ReadStep R true A true st (fetched st d lo n) d' ∧ d.pos < d'.pos := by
  by_cases hfc : d.pos < st.floor.covers A
  · refine ⟨0, 0, ⟨st.floor.covers A, ledgerAt R seq (st.floor.covers A),
      audAt R seq A (st.floor.covers A), rowsAt R seq A (st.floor.covers A), d.buf⟩, ?_, hfc⟩
    rw [fetched_zero]
    exact ReadStep.reload d _ _ _ hfc rfl rfl (g.floor_body A)
  have hst := g.start_le A
  have hslot : ∀ i, st.floor.covers A < i → i ≤ st.head → st.slot i = some (seq i) := by
    intro i h1 h2
    rw [g.kept_genuine i (by omega), g.first_genuine i (by omega) h2]
  -- The plain case: fetch the next slot and apply it.
  have plain : (∀ au v c p, seq (d.pos + 1) = .boundary au A v c p →
      audValid R d.ledger d.aud (d.pos + 1) A (seq (d.pos + 1)) = false) →
      ∃ lo n d', ReadStep R true A true st (fetched st d lo n) d' ∧ d.pos < d'.pos := by
    intro hnr
    refine ⟨d.pos + 1, 1, _, ReadStep.apply (fetched st d (d.pos + 1) 1) (seq (d.pos + 1)) ?_ ?_ ?_
      (fun _ => rfl), by simp⟩
    · rw [fetched_pos, fetched_buf_in st d (Nat.le_refl _) (by omega)]
      exact hslot _ (by omega) (by omega)
    · rw [fetched_pos]; omega
    · simpa using hnr
  cases hs : seq (d.pos + 1) with
  | boundary au a v c p =>
    by_cases hva : a = A ∧ audValid R d.ledger d.aud (d.pos + 1) A (.boundary au a v c p) = true
    · obtain ⟨rfl, hv⟩ := hva
      have happ : AppliedTo R seq a (d.pos + 1) := by
        refine ⟨by rw [hs]; simp [touches], ?_⟩
        simp only [Nat.add_sub_cancel]; rw [hs, ← hd.ledger, ← hd.aud]; exact hv
      obtain ⟨L', P', hb⟩ := (g.boundary_body a (d.pos + 1) au v c p (by omega) (by omega) hs happ).1
        (by omega)
      -- Either boundary kind covers the floor's snapshot.
      have hcov : st.floor.covers a ≤ c := by
        rcases g.floor_announced a with h0 | ⟨a', au', ha1, hahead, has, haapp⟩
        · omega
        · by_cases hlt' : a' < d.pos + 1
          · have h1 := announced_maxCover R seq a ha1 has haapp
            have h2 := maxCover_mono R seq a a' d.pos (by omega)
            have h3 : (audAt R seq a d.pos).maxCover ≤ c := by
              rw [hd.aud] at hv; simp [audValid] at hv; omega
            omega
          · have hne : a' ≠ d.pos + 1 := by intro he; rw [he, hs] at has; cases has
            have hlr := boundary_lastBoundary R seq a (by omega) hs happ
            have hmono := lastBoundary_mono R seq a (d.pos + 1) (a' - 1) (by omega)
            have hsv : (audAt R seq a (a' - 1)).lastBoundary ≤ st.floor.covers a := by
              have := haapp.2
              rw [has] at this
              simp [audValid] at this
              omega
            omega
      have hc : c < d.pos + 1 := by simp [audValid] at hv; omega
      refine ⟨c + 1, d.pos + 1 - c, _,
        ReadStep.boundary (fetched st d (c + 1) (d.pos + 1 - c)) au v c p ?_ ?_ (by simpa using hv) rfl
          L' P' _ hb seq ?_, by simp⟩
      · rw [fetched_pos, fetched_buf_in st d (by omega) (by omega), hslot _ (by omega) (by omega), hs]
      · rw [fetched_pos]; omega
      · intro i h1 h2
        rw [fetched_pos] at h2
        rw [fetched_buf_in st d (by omega) (by omega)]
        exact ⟨hslot i (by omega) (by omega), by omega⟩
    · apply plain
      intro au' v' c' p' he
      rw [hs] at he
      cases he
      rw [hs]
      cases h : audValid R d.ledger d.aud (d.pos + 1) A (.boundary au A v c p)
      · rfl
      · exact absurd ⟨rfl, h⟩ hva
  | _ =>
    apply plain
    intro _ _ _ _ he; rw [hs] at he; cases he

/-- **A fixed target is reachable.** Later slots do not extend the reload
work, provided the target includes the selected floor's coverage. -/
theorem reaches_target {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) (A : S.Aud)
    (target : Nat) (head : target ≤ st.head) (floor : st.floor.covers A ≤ target) :
    ∀ (d : Reader S), ReaderOk R st seq A d → d.pos ≤ target →
      ∃ d', ReadSteps R A true st d d' ∧ d'.pos = target ∧ ReaderOk R st seq A d' := by
  intro d
  generalize hn : target - d.pos = n
  induction n using Nat.strongRecOn generalizing d with
  | ind n ih =>
    intro hd bound
    by_cases heq : d.pos = target
    · exact ⟨d, ReadSteps.refl d, heq, hd⟩
    · have hlt : d.pos < st.head := by omega
      obtain ⟨lo, k, d', hs, hpos⟩ := read_progress R g hd hlt
      have run1 := fetch_run R A true st d lo k
      have hd1 := reads_ok R g hd run1
      have hd' := read_ok R g hd1 hs
      have hpos' : d.pos < d'.pos := by simpa [fetched] using hpos
      have bound' : d'.pos ≤ target := by
        cases hs <;> simp only [fetched_pos] at * <;> omega
      obtain ⟨d'', run, hp, hok⟩ := ih (target - d'.pos) (by omega) d' rfl hd' bound'
      exact ⟨d'', ReadSteps.trans R run1 (ReadSteps.step hs run), hp, hok⟩

/-- **Every reader with its key reaches the head.** -/
theorem reaches_head {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) (A : S.Aud) :
    ∀ (d : Reader S), ReaderOk R st seq A d →
      ∃ d', ReadSteps R A true st d d' ∧ d'.pos = st.head ∧ ReaderOk R st seq A d' := by
  intro d hd
  exact reaches_target R g A st.head (Nat.le_refl _) (g.covers_le A) d hd hd.le_head

/-- **A joiner reaches every device's state.** A joiner of `A` that loads the
snapshot the floor names for `A` and then follows storage reaches the state of
a device that applied every slot. -/
theorem joiner_reaches_head {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) (A : S.Aud) :
    ∃ d', ReadSteps R A true st
        ⟨st.floor.covers A, ledgerAt R seq (st.floor.covers A), audAt R seq A (st.floor.covers A),
          rowsAt R seq A (st.floor.covers A), fun _ => none⟩ d' ∧
      d'.pos = st.head ∧ (d'.ledger, d'.aud, d'.rows) = stateAt R seq A st.head := by
  obtain ⟨d', run, hpos, hok⟩ := reaches_head R g A
    ⟨st.floor.covers A, ledgerAt R seq (st.floor.covers A), audAt R seq A (st.floor.covers A),
      rowsAt R seq A (st.floor.covers A), fun _ => none⟩
    ⟨rfl, rfl, rfl, g.covers_le A, fun _ _ h => by simp at h⟩
  refine ⟨d', run, hpos, ?_⟩
  rw [hok.ledger, hok.aud, hok.rows, hpos]; rfl

/-- **Devices agree.** Two sound readers of the same audience, on any good
storages (for example before and after a deletion), at the same position hold
the same state. -/
theorem readers_agree {st₁ st₂ : Store S} {seq : Nat → Slot S} {A : S.Aud} {d₁ d₂ : Reader S}
    (h₁ : ReaderOk R st₁ seq A d₁) (h₂ : ReaderOk R st₂ seq A d₂) (hpos : d₁.pos = d₂.pos) :
    d₁.ledger = d₂.ledger ∧ d₁.aud = d₂.aud ∧ d₁.rows = d₂.rows := by
  rw [h₁.ledger, h₁.aud, h₁.rows, h₂.ledger, h₂.aud, h₂.rows, hpos]
  exact ⟨rfl, rfl, rfl⟩

/-! ## Without the key -/

/-- **A reader without the key passes only slots that are not its audience's.**
Every step that moves a reader of `A` without `A`'s key applies exactly the next
slot, and that slot has no part or announcement for `A`. -/
theorem keyless_passes_only_others {reloadAllowed : Bool} {st : Store S} {seq : Nat → Slot S} {A : S.Aud}
    {d d' : Reader S} (hd : ReaderOk R st seq A d) (step : ReadStep R reloadAllowed A false st d d')
    (hmove : d'.pos ≠ d.pos) : d'.pos = d.pos + 1 ∧ touches A (seq (d.pos + 1)) = false := by
  cases step with
  | fetch => exact absurd rfl hmove
  | apply x hb hf hnr hk =>
    obtain ⟨_, _, hx⟩ := checked_genuine R hd hb hf
    refine ⟨rfl, ?_⟩
    rw [← hx]
    cases h : touches A x
    · rfl
    · exact absurd (hk h) (by simp)
  | boundary _ _ _ _ _ _ _ hk => cases hk
  | reload _ _ _ _ hk => cases hk

/-- **A reader stops at a slot it should read but cannot.** A reader of `A`
without `A`'s key whose next slot has a part or announcement for `A` never
moves. -/
theorem keyless_stops {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) {A : S.Aud}
    {d : Reader S} (hd : ReaderOk R st seq A d) (htouch : touches A (seq (d.pos + 1)) = true) :
    ∀ d', ReadSteps R A false st d d' → d'.pos = d.pos := by
  intro d' run
  induction run with
  | refl => rfl
  | @step d₁ d₂ d₃ s _ ih =>
    have hd₂ := read_ok R g hd s
    by_cases hm : d₂.pos = d₁.pos
    · rw [← hm] at htouch ⊢
      exact ih hd₂ htouch
    · have := (keyless_passes_only_others R hd s hm).2
      rw [htouch] at this; cases this

end CovenSequence
