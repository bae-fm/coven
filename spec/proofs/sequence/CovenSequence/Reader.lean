import CovenSequence.Unique

/-!
# Readers, snapshots and deleting covered slots

A reader is a device following storage: its position, ledger and rows. Storage
holds the retained slots, a floor record ("the log starts at slot `n`; the
snapshot at path `p` covers slot `c`"), and snapshot bodies.

A reader steps in three ways:

- apply the next slot, when storage holds it and it is not a valid raise;
- apply a valid raise: load the raise snapshot's rows and replay the slots
  after its coverage, which it reads from storage;
- reload from the floor's snapshot, when that covers more than the reader has
  applied. A joiner starts this way; so does a device that was away past the
  grace period.

`Good` is what retention keeps true of storage: retained slots are the genuine
ones (`Claim.floor_check_sound`), every slot at or above the floor is retained,
the floor's snapshot was announced validly and its body is the state it
covers, and every valid raise's body is the raiser's migrated rows.

Results: every reader step keeps the reader at a prefix state
(`read_sound`); on good storage a reader can always reach the head
(`reaches_head`), including a joiner from the floor's snapshot
(`joiner_reaches_head`); and deleting covered slots and raising the floor keep
storage good (`retain_good`). So deletion never changes any device's reachable
state. The last needs the snapshot rule `versionRule`: without it, C6 alone
strands a reader (`ReaderExamples.c6_alone_strands_reader`).
-/

namespace CovenSequence

variable {S : Sig} (R : Rules S)

/-- What storage holds. -/
structure Storage (S : Sig) where
  slot : Nat → Option (Slot S)
  floorStart : Nat
  floorCovers : Nat
  floorPath : S.Path
  body : S.Path → Option (Ledger S × S.Rows)

/-- A device following storage. -/
structure Reader (S : Sig) where
  pos : Nat
  ledger : Ledger S
  rows : S.Rows

/-- A reader's steps against fixed storage. -/
inductive ReadStep (st : Storage S) : Reader S → Reader S → Prop
  | apply (d : Reader S) (x : Slot S) (hx : st.slot (d.pos + 1) = some x)
      (hnr : ∀ a v c p, x = .raise a v c p → valid R d.ledger (d.pos + 1) x = false) :
      ReadStep st d ⟨d.pos + 1, ledgerStep R d.ledger (d.pos + 1) x,
        if valid R d.ledger (d.pos + 1) x then applyWrite R d.ledger.version d.rows x else d.rows⟩
  | raise (d : Reader S) (a : S.Dev) (v c : Nat) (p : S.Path)
      (hx : st.slot (d.pos + 1) = some (.raise a v c p))
      (hv : valid R d.ledger (d.pos + 1) (.raise a v c p) = true)
      (L' : Ledger S) (rows : S.Rows) (hb : st.body p = some (L', rows))
      (get : Nat → Slot S) (hget : ∀ i, c < i → i ≤ d.pos → st.slot i = some (get i)) :
      ReadStep st d ⟨d.pos + 1, ledgerStep R d.ledger (d.pos + 1) (.raise a v c p),
        replay R get d.ledger.refused v rows (c + 1) (d.pos - c)⟩
  | reload (d : Reader S) (L : Ledger S) (rows : S.Rows) (hc : d.pos < st.floorCovers)
      (hb : st.body st.floorPath = some (L, rows)) :
      ReadStep st d ⟨st.floorCovers, L, rows⟩

inductive ReadSteps (st : Storage S) : Reader S → Reader S → Prop
  | refl (d : Reader S) : ReadSteps st d d
  | step {d d' d'' : Reader S} : ReadStep R st d d' → ReadSteps st d' d'' → ReadSteps st d d''

/-- Storage as retention keeps it, for the claimed sequence `seq` with `head`
slots. -/
structure Good (st : Storage S) (seq : Nat → Slot S) (head : Nat) : Prop where
  genuine : ∀ i x, st.slot i = some x → 1 ≤ i ∧ i ≤ head ∧ x = seq i
  retained : ∀ i, st.floorStart ≤ i → 1 ≤ i → i ≤ head → st.slot i = some (seq i)
  floor_le : st.floorStart ≤ st.floorCovers + 1
  covers_le : st.floorCovers ≤ head
  floor_body : st.body st.floorPath = some (stateAt R seq st.floorCovers)
  floor_announced : st.floorCovers = 0 ∨
    ∃ a d, 1 ≤ a ∧ a ≤ head ∧ seq a = .snapshot d st.floorCovers st.floorPath ∧ Applied R seq a
  /-- A valid raise's body is the raiser's migrated rows if present, and is
  present while the floor has not covered the raise. -/
  raise_body : ∀ r a v c p, 1 ≤ r → r ≤ head → seq r = .raise a v c p → Applied R seq r →
    (st.floorCovers < r → ∃ L', st.body p = some (L', R.migrate v (rowsAt R seq c))) ∧
    (∀ L' rows, st.body p = some (L', rows) → rows = R.migrate v (rowsAt R seq c))

/-- The reader holds the state of the prefix it applied. -/
def Sound (seq : Nat → Slot S) (d : Reader S) : Prop :=
  d.ledger = ledgerAt R seq d.pos ∧ d.rows = rowsAt R seq d.pos

/-! ## Monotone ledger facts -/

theorem step_maxCover (L : Ledger S) (k : Nat) (x : Slot S) :
    L.maxCover ≤ (ledgerStep R L k x).maxCover := by
  unfold ledgerStep
  split
  · cases x with
    | admin a e => cases e <;> simp [effect, applyEntry]
    | snapshot a c p => simp only [effect]; exact Nat.le_max_left _ _
    | _ => simp [effect]
  · simp [refuse]

theorem step_lastRaise (L : Ledger S) (k : Nat) (x : Slot S) :
    L.lastRaise ≤ (ledgerStep R L k x).lastRaise := by
  unfold ledgerStep
  split
  · rename_i hv
    cases x with
    | admin a e => cases e <;> simp [effect, applyEntry]
    | raise a v c p =>
      simp [valid] at hv
      simp [effect]; omega
    | _ => simp [effect]
  · simp [refuse]

theorem maxCover_mono (seq : Nat → Slot S) (j : Nat) :
    ∀ k, j ≤ k → (ledgerAt R seq j).maxCover ≤ (ledgerAt R seq k).maxCover
  | k, hk => by
    induction k with
    | zero => have : j = 0 := by omega
              subst this; exact Nat.le_refl _
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; exact Nat.le_refl _
      · exact Nat.le_trans (ih (by omega)) (step_maxCover R _ _ _)

theorem lastRaise_mono (seq : Nat → Slot S) (j : Nat) :
    ∀ k, j ≤ k → (ledgerAt R seq j).lastRaise ≤ (ledgerAt R seq k).lastRaise
  | k, hk => by
    induction k with
    | zero => have : j = 0 := by omega
              subst this; exact Nat.le_refl _
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; exact Nat.le_refl _
      · exact Nat.le_trans (ih (by omega)) (step_lastRaise R _ _ _)

/-- An announced snapshot's coverage counts in `maxCover` from its slot on. -/
theorem announced_maxCover (seq : Nat → Slot S) {a : Nat} {d : S.Dev} {c : Nat} {p : S.Path}
    (ha : 1 ≤ a) (hs : seq a = .snapshot d c p) (happ : Applied R seq a) :
    c ≤ (ledgerAt R seq a).maxCover := by
  rw [applied_ledger R seq ha happ, hs]
  simp only [effect]; exact Nat.le_max_right _ _

/-- An applied raise's slot is `lastRaise` from its slot on. -/
theorem raise_lastRaise (seq : Nat → Slot S) {r : Nat} {a : S.Dev} {v c : Nat} {p : S.Path}
    (hr : 1 ≤ r) (hs : seq r = .raise a v c p) (happ : Applied R seq r) :
    (ledgerAt R seq r).lastRaise = r := by
  rw [applied_ledger R seq hr happ, hs]
  simp [effect]

/-! ## Soundness -/

/-- **Reading is sound.** Every reader step from a prefix state on good
storage reaches a prefix state, whatever slots were deleted. -/
theorem read_sound {st : Storage S} {seq : Nat → Slot S} {head : Nat} (g : Good R st seq head)
    {d d' : Reader S} (hd : Sound R seq d) (step : ReadStep R st d d') : Sound R seq d' := by
  obtain ⟨hl, hr⟩ := hd
  cases step with
  | apply x hx hnr =>
    obtain ⟨_, _, hxs⟩ := g.genuine _ _ hx
    subst hxs
    refine ⟨by simp only; rw [hl, ledgerAt_succ], ?_⟩
    simp only
    rw [hl, hr]
    cases hs : seq (d.pos + 1) with
    | raise a v c p =>
      have hf := hnr a v c p hs
      rw [hl, hs] at hf
      rw [rowsAt_raise R seq d.pos hs]
      simp [hf]
    | _ =>
      have hnr' : ∀ a v c p, seq (d.pos + 1) ≠ .raise a v c p := by intro _ _ _ _; simp [hs]
      rw [rowsAt_other R seq d.pos hnr', hs]
  | raise a v c p hx hv L' rows hb get hget =>
    obtain ⟨_, hhead, hxs⟩ := g.genuine _ _ hx
    rw [hl] at hv
    refine ⟨by simp only; rw [hl, ledgerAt_succ, ← hxs], ?_⟩
    simp only
    have happ : Applied R seq (d.pos + 1) := by
      unfold Applied; simp only [Nat.add_sub_cancel]; rw [← hxs]; exact hv
    have hrows : rows = R.migrate v (rowsAt R seq c) :=
      (g.raise_body (d.pos + 1) a v c p (by omega) hhead hxs.symm happ).2 L' rows hb
    rw [rowsAt_raise R seq d.pos hxs.symm]
    simp only [hv, ite_true]
    rw [hrows, hl]
    have hc : c < d.pos + 1 := by simp [valid] at hv; omega
    apply replay_congr
    intro i h1 h2
    obtain ⟨_, _, he⟩ := g.genuine i (get i) (hget i (by omega) (by omega))
    exact he
  | reload L rows hc hb =>
    rw [g.floor_body] at hb
    have := Option.some.inj hb
    simp only [stateAt] at this
    obtain ⟨h1, h2⟩ := Prod.mk.inj this
    exact ⟨h1.symm, h2.symm⟩

theorem reads_sound {st : Storage S} {seq : Nat → Slot S} {head : Nat} (g : Good R st seq head)
    {d d' : Reader S} (hd : Sound R seq d) (run : ReadSteps R st d d') : Sound R seq d' := by
  induction run with
  | refl => exact hd
  | step s _ ih => exact ih (read_sound R g hd s)

/-- Readers never pass the head. -/
theorem read_le_head {st : Storage S} {seq : Nat → Slot S} {head : Nat} (g : Good R st seq head)
    {d d' : Reader S} (_hd : d.pos ≤ head) (step : ReadStep R st d d') : d'.pos ≤ head := by
  cases step with
  | apply x hx => exact (g.genuine _ _ hx).2.1
  | raise a v c p hx => exact (g.genuine _ _ hx).2.1
  | reload => exact g.covers_le

/-! ## Progress -/

/-- **Progress.** On good storage under the version rule, a reader at a prefix
state below the head can step forward. -/
theorem read_progress (hrule : R.versionRule = true) {st : Storage S} {seq : Nat → Slot S}
    {head : Nat} (g : Good R st seq head) {d : Reader S} (hd : Sound R seq d) (hlt : d.pos < head) :
    ∃ d', ReadStep R st d d' ∧ d.pos < d'.pos := by
  obtain ⟨hl, hr⟩ := hd
  by_cases hfc : d.pos < st.floorCovers
  · exact ⟨_, ReadStep.reload d _ _ hfc g.floor_body, hfc⟩
  have hnext : st.slot (d.pos + 1) = some (seq (d.pos + 1)) :=
    g.retained _ (by have := g.floor_le; omega) (by omega) (by omega)
  cases hs : seq (d.pos + 1) with
  | raise a v c p =>
    rw [hs] at hnext
    by_cases hv : valid R d.ledger (d.pos + 1) (.raise a v c p) = true
    · have happ : Applied R seq (d.pos + 1) := by
        unfold Applied; simp only [Nat.add_sub_cancel]; rw [hs, ← hl]; exact hv
      obtain ⟨L', hb⟩ := (g.raise_body (d.pos + 1) a v c p (by omega) (by omega) hs happ).1
        (by omega)
      -- The raise covers the floor's snapshot.
      have hcov : st.floorCovers ≤ c := by
        rcases g.floor_announced with h0 | ⟨a', d', ha1, hahead, has, haapp⟩
        · omega
        · by_cases hlt' : a' < d.pos + 1
          · have h1 := announced_maxCover R seq ha1 has haapp
            have h2 := maxCover_mono R seq a' d.pos (by omega)
            have h3 : (ledgerAt R seq d.pos).maxCover ≤ c := by
              rw [hl] at hv; simp [valid] at hv; omega
            omega
          · have hne : a' ≠ d.pos + 1 := by intro he; rw [he, hs] at has; cases has
            have hlr := raise_lastRaise R seq (by omega) hs happ
            have hmono := lastRaise_mono R seq (d.pos + 1) (a' - 1) (by omega)
            have hsv : (ledgerAt R seq (a' - 1)).lastRaise ≤ st.floorCovers := by
              have := haapp
              unfold Applied at this
              rw [has] at this
              simp [valid, hrule] at this
              omega
            omega
      refine ⟨_, ReadStep.raise d a v c p hnext hv L' _ hb seq (fun i h1 h2 => ?_), by simp⟩
      exact g.retained i (by have := g.floor_le; omega) (by omega) (by omega)
    · have hf : valid R d.ledger (d.pos + 1) (.raise a v c p) = false := by simpa using hv
      refine ⟨_, ReadStep.apply d _ hnext (fun a' v' c' p' he => ?_), by simp⟩
      cases he; exact hf
  | _ =>
    rw [hs] at hnext
    exact ⟨_, ReadStep.apply d _ hnext (fun _ _ _ _ he => nomatch he), by simp⟩

/-- **Every reader reaches the head.** From any prefix state, on good storage
under the version rule, some run of reader steps reaches the head state. -/
theorem reaches_head (hrule : R.versionRule = true) {st : Storage S} {seq : Nat → Slot S}
    {head : Nat} (g : Good R st seq head) :
    ∀ (d : Reader S), Sound R seq d → d.pos ≤ head →
      ∃ d', ReadSteps R st d d' ∧ d'.pos = head ∧ Sound R seq d' := by
  intro d
  generalize hn : head - d.pos = n
  induction n using Nat.strongRecOn generalizing d with
  | ind n ih =>
    intro hd hle
    by_cases heq : d.pos = head
    · exact ⟨d, ReadSteps.refl d, heq, hd⟩
    · obtain ⟨d', hs, hlt⟩ := read_progress R hrule g hd (by omega)
      have hle' := read_le_head R g hle hs
      obtain ⟨d'', run, hpos, hsound⟩ :=
        ih (head - d'.pos) (by omega) d' rfl (read_sound R g hd hs) hle'
      exact ⟨d'', ReadSteps.step hs run, hpos, hsound⟩

/-- **A joiner reaches every device's state.** A joiner that loads the
snapshot the floor record names and then follows storage reaches the state of a
device that applied every slot. -/
theorem joiner_reaches_head (hrule : R.versionRule = true) {st : Storage S}
    {seq : Nat → Slot S} {head : Nat} (g : Good R st seq head) :
    ∃ d', ReadSteps R st ⟨st.floorCovers, ledgerAt R seq st.floorCovers, rowsAt R seq st.floorCovers⟩ d' ∧
      d' = ⟨head, ledgerAt R seq head, rowsAt R seq head⟩ := by
  obtain ⟨d', run, hpos, hl, hr⟩ := reaches_head R hrule g
    ⟨st.floorCovers, ledgerAt R seq st.floorCovers, rowsAt R seq st.floorCovers⟩ ⟨rfl, rfl⟩ g.covers_le
  refine ⟨d', run, ?_⟩
  cases d'
  simp only at hpos hl hr
  subst hpos; subst hl; subst hr; rfl

/-! ## Retention -/

/-- Retention steps: delete a slot below the floor; raise the floor to a
validly announced snapshot whose body is the state it covers; or delete a
snapshot body that is not the floor's and names no raise above the floor's
coverage. -/
inductive Retain (seq : Nat → Slot S) (head : Nat) : Storage S → Storage S → Prop
  | delete (st : Storage S) (i : Nat) (hi : i < st.floorStart) :
      Retain seq head st { st with slot := fun j => if j = i then none else st.slot j }
  | deleteBody (st : Storage S) (p : S.Path) (hp : p ≠ st.floorPath)
      (hraise : ∀ r a v c, 1 ≤ r → r ≤ head → seq r = .raise a v c p → Applied R seq r →
        r ≤ st.floorCovers) :
      Retain seq head st { st with body := fun q => if q = p then none else st.body q }
  | raiseFloor (st : Storage S) (n a : Nat) (d : S.Dev) (c : Nat) (p : S.Path)
      (hn : st.floorStart ≤ n) (hcov : st.floorCovers ≤ c) (hnc : n ≤ c + 1) (ha : 1 ≤ a)
      (hahead : a ≤ head)
      (hs : seq a = .snapshot d c p) (happ : Applied R seq a)
      (hb : st.body p = some (stateAt R seq c)) :
      Retain seq head st { st with floorStart := n, floorCovers := c, floorPath := p }

/-- **Retention keeps storage good.** -/
theorem retain_good {seq : Nat → Slot S} {head : Nat} {st st' : Storage S}
    (g : Good R st seq head) (r : Retain R seq head st st') : Good R st' seq head := by
  cases r with
  | delete i hi =>
    refine ⟨fun j x h => ?_, fun j h1 h2 h3 => ?_, g.floor_le, g.covers_le, g.floor_body,
      g.floor_announced, g.raise_body⟩
    · by_cases hj : j = i
      · simp [hj] at h
      · simp only [hj, ite_false] at h; exact g.genuine j x h
    · simp only at h1 ⊢
      have hj : j ≠ i := by omega
      simp only [hj, ite_false]; exact g.retained j h1 h2 h3
  | deleteBody p hp hraise =>
    refine ⟨g.genuine, g.retained, g.floor_le, g.covers_le, ?_, g.floor_announced,
      fun r a v c q h1 h2 h3 h4 => ⟨fun hlt => ?_, fun L' rows hb => ?_⟩⟩
    · simp only [Ne.symm hp, ite_false]; exact g.floor_body
    · by_cases hq : q = p
      · subst hq; have := hraise r a v c h1 h2 h3 h4; simp only at hlt; omega
      · simp only [hq, ite_false]; exact (g.raise_body r a v c q h1 h2 h3 h4).1 hlt
    · by_cases hq : q = p
      · subst hq; simp at hb
      · simp only [hq, ite_false] at hb; exact (g.raise_body r a v c q h1 h2 h3 h4).2 L' rows hb
  | raiseFloor n a d c p hn hcov hnc ha hahead hs happ hb =>
    have hca : c < a := by
      have := happ; unfold Applied at this; rw [hs] at this; simp [valid] at this; omega
    exact ⟨g.genuine, fun j h1 h2 h3 => g.retained j (by simp only at h1; omega) h2 h3, hnc,
      by simp only; omega, hb, Or.inr ⟨a, d, ha, hahead, hs, happ⟩,
      fun r a v c' q h1 h2 h3 h4 =>
        ⟨fun hlt => (g.raise_body r a v c' q h1 h2 h3 h4).1 (by simp only at hlt; omega),
         (g.raise_body r a v c' q h1 h2 h3 h4).2⟩⟩

/-- A claim of the next slot keeps storage good, given that a valid raise's
body was uploaded to its own path before its claim. -/
theorem claim_good {seq : Nat → Slot S} {head : Nat} {st : Storage S} (g : Good R st seq head)
    (hbody : ∀ a v c p, seq (head + 1) = .raise a v c p → Applied R seq (head + 1) →
      ∃ L', st.body p = some (L', R.migrate v (rowsAt R seq c))) :
    Good R { st with slot := fun i => if i = head + 1 then some (seq (head + 1)) else st.slot i }
      seq (head + 1) := by
  refine ⟨fun i x h => ?_, fun i h1 h2 h3 => ?_, g.floor_le, by simp only; have := g.covers_le; omega,
    g.floor_body, ?_, fun r a v c p h1 h2 h3 h4 => ?_⟩
  · by_cases hi : i = head + 1
    · subst hi; simp at h; exact ⟨by omega, Nat.le_refl _, h.symm⟩
    · simp only [hi, ite_false] at h
      obtain ⟨a, b, c⟩ := g.genuine i x h
      exact ⟨a, by omega, c⟩
  · by_cases hi : i = head + 1
    · subst hi; simp
    · simp only [hi, ite_false]; exact g.retained i h1 h2 (by omega)
  · rcases g.floor_announced with h | ⟨a, d, h1, h2, h3, h4⟩
    · exact Or.inl h
    · exact Or.inr ⟨a, d, h1, by omega, h3, h4⟩
  · by_cases hr : r = head + 1
    · subst hr
      obtain ⟨L', hb⟩ := hbody a v c p h3 h4
      refine ⟨fun _ => ⟨L', hb⟩, fun L'' rows hb' => ?_⟩
      rw [hb] at hb'
      exact ((Prod.mk.inj (Option.some.inj hb')).2).symm
    · exact g.raise_body r a v c p h1 (by omega) h3 h4

/-- Storage reachable by retention from good storage. -/
inductive Retains (seq : Nat → Slot S) (head : Nat) : Storage S → Storage S → Prop
  | refl (st : Storage S) : Retains seq head st st
  | step {st st' st'' : Storage S} : Retain R seq head st st' → Retains seq head st' st'' →
      Retains seq head st st''

/-- **Deleting covered slots never changes any device's reachable state.**
After any retention, every state a reader reaches from a prefix state is a
prefix state, and every reader can still reach the head state. -/
theorem deletion_keeps_reachable (hrule : R.versionRule = true) {seq : Nat → Slot S} {head : Nat}
    {st st' : Storage S} (g : Good R st seq head) (ret : Retains R seq head st st') :
    (∀ d d', Sound R seq d → ReadSteps R st' d d' → Sound R seq d') ∧
    (∀ d, Sound R seq d → d.pos ≤ head →
      ∃ d', ReadSteps R st' d d' ∧ d' = ⟨head, ledgerAt R seq head, rowsAt R seq head⟩) := by
  have g' : Good R st' seq head := by
    induction ret with
    | refl => exact g
    | step r _ ih => exact ih (retain_good R g r)
  refine ⟨fun d d' hd run => reads_sound R g' hd run, fun d hd hle => ?_⟩
  obtain ⟨d', run, hpos, hl, hr⟩ := reaches_head R hrule g' d hd hle
  refine ⟨d', run, ?_⟩
  cases d'
  simp only at hpos hl hr
  subst hpos; subst hl; subst hr; rfl

/-- **Devices agree.** Two readers of the same sequence, on any good
storages (for example before and after a deletion), at the same position hold
the same state. -/
theorem readers_agree {seq : Nat → Slot S} {d₁ d₂ : Reader S}
    (h₁ : Sound R seq d₁) (h₂ : Sound R seq d₂) (hpos : d₁.pos = d₂.pos) : d₁ = d₂ := by
  obtain ⟨p₁, l₁, r₁⟩ := d₁
  obtain ⟨p₂, l₂, r₂⟩ := d₂
  obtain ⟨hl₁, hr₁⟩ := h₁
  obtain ⟨hl₂, hr₂⟩ := h₂
  simp only at hpos hl₁ hr₁ hl₂ hr₂
  subst hpos; subst hl₁; subst hr₁; subst hl₂; subst hr₂; rfl

end CovenSequence
