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
  floor names and no raise or reset above the floor needs, and none that a
  rebuild of a circle below the store's version starts from;
- the first kept slot is no later than the slot after the store snapshot such
  a rebuild starts from (`floor_within_caps_spares_rebuild`,
  `Retention.first_spares_rebuild_store`);
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
      (hprepared : ∀ A au v c w p, seq (st.head + 1) = .prepare au A v c w p →
        AppliedTo R seq A (st.head + 1) →
        ∃ L' P', st.body p = some (L', P', preparedRows R v p (raiseView R seq A c) A))
      (hbody : ∀ A au v c p, seq (st.head + 1) = .boundary au A v c p → AppliedTo R seq A (st.head + 1) →
        ∃ L' P', st.body p = some (L', P', preparedRows R v p (raiseView R seq A c) A))
      (hrebuild : RebuildKept R seq (st.head + 1) st.body st.floor.start) :
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
      (hann : ∀ A, F.covers A = 0 ∨ ∃ a au w, 1 ≤ a ∧ a ≤ st.head ∧
        seq a = .snapshot au A (F.covers A) w (F.path A) ∧ AppliedTo R seq A a)
      (hbody : ∀ A, st.body (F.path A) = some (stateAt R seq A (F.covers A)))
      (hspare : ∀ A a cs p, NeedsRebuild R seq st.head A → RebuildSnapshot R seq st.head A a cs p →
        F.start ≤ cs + 1)
      (hnone : ∀ A, NeedsRebuild R seq st.head A →
        (¬ ∃ a cs p, RebuildSnapshot R seq st.head A a cs p) → F.start ≤ 1) :
      StoreStep seq st { st with floor := F }
  | delete (st : Store S) (i : Nat) (hi : i < st.floor.start) :
      StoreStep seq st { st with slot := fun j => if j = i then none else st.slot j }
  | deleteBody (st : Store S) (p : S.Path) (hp : ∀ A, p ≠ st.floor.path A)
      (hraise : ∀ A r au v c, 1 ≤ r → r ≤ st.head → seq r = .boundary au A v c p → AppliedTo R seq A r →
        r ≤ st.floor.covers A)
      (hrebuild : ∀ A a cs q, NeedsRebuild R seq st.head A → RebuildSnapshot R seq st.head A a cs q →
        p ≠ q)
      (hreplay : ∀ A a cs q r au v c q', NeedsRebuild R seq st.head A →
        RebuildSnapshot R seq st.head A a cs q → cs < r →
        r ≤ storeBefore R seq (audAt R seq A st.head).version st.head →
        seq r = .boundary au R.store v c q' → AppliedTo R seq R.store r → p ≠ q') :
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
  | claim hprepared hbody hrebuild =>
    refine ⟨fun i h1 h2 => ?_, fun i hi => ?_, fun i hi => ?_, fun i hi => ?_, g.start_le,
      fun A => by simp only; have := g.covers_le A; omega, g.floor_body, fun A => ?_,
      fun A r au v c p h1 h2 h3 h4 => ?_, hrebuild⟩
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
    · rcases g.floor_announced A with h | ⟨a, au, w, h1, h2, h3, h4⟩
      · exact Or.inl h
      · exact Or.inr ⟨a, au, w, h1, by simp only; omega, h3, h4⟩
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
      g.floor_body, g.floor_announced, g.boundary_body, g.rebuild_kept⟩
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
  | writeFloor F hstart hcov hle hhead hann hbody hspare hnone =>
    refine ⟨g.first_genuine, g.first_out, g.slot_out, fun j hj => g.kept_genuine j (by simp only at hj; omega),
      hle, hhead, hbody, hann, fun A r au v c p h1 h2 h3 h4 =>
        ⟨fun hlt => (g.boundary_body A r au v c p h1 h2 h3 h4).1 (by have := hcov A; simp only at hlt; omega),
         (g.boundary_body A r au v c p h1 h2 h3 h4).2⟩, fun A need => ?_⟩
    exact ⟨fun a cs p snap => ⟨((g.rebuild_kept A need).1 a cs p snap).1, hspare A a cs p need snap,
      ((g.rebuild_kept A need).1 a cs p snap).2.2⟩, hnone A need⟩
  | delete i hi =>
    refine ⟨g.first_genuine, g.first_out, fun j hj => ?_, fun j hj => ?_, g.start_le, g.covers_le,
      g.floor_body, g.floor_announced, g.boundary_body, g.rebuild_kept⟩
    · simp only; by_cases h : j = i
      · simp [h]
      · simp only [h, ite_false]; exact g.slot_out j hj
    · simp only at hj ⊢
      have : j ≠ i := by omega
      simp only [this, ite_false]; exact g.kept_genuine j hj
  | deleteBody p hp hraise hrebuild hreplay =>
    refine ⟨g.first_genuine, g.first_out, g.slot_out, g.kept_genuine, g.start_le, g.covers_le,
      fun A => ?_, g.floor_announced, fun A r au v c q h1 h2 h3 h4 => ⟨fun hlt => ?_, fun L' P' rows hb => ?_⟩,
      fun A need => ?_⟩
    · simp only [Ne.symm (hp A), ite_false]; exact g.floor_body A
    · by_cases hq : q = p
      · subst hq; have := hraise A r au v c h1 h2 h3 h4; simp only at hlt; omega
      · simp only [hq, ite_false]; exact (g.boundary_body A r au v c q h1 h2 h3 h4).1 hlt
    · by_cases hq : q = p
      · subst hq; simp at hb
      · simp only [hq, ite_false] at hb; exact (g.boundary_body A r au v c q h1 h2 h3 h4).2 L' P' rows hb
    · refine ⟨fun a cs q snap => ?_, (g.rebuild_kept A need).2⟩
      have kept := (g.rebuild_kept A need).1 a cs q snap
      have ne : q ≠ p := fun h => hrebuild A a cs q need snap h.symm
      refine ⟨by simp only [ne, ite_false]; exact kept.1, kept.2.1, fun r au v c q' h1 h2 h3 h4 => ?_⟩
      obtain ⟨L', P', hb⟩ := kept.2.2 r au v c q' h1 h2 h3 h4
      have ne' : q' ≠ p := fun h => hreplay A a cs q r au v c q' need snap h1 h2 h3 h4 h.symm
      exact ⟨L', P', by simp only [ne', ite_false]; exact hb⟩
  | upload p b hp hfree =>
    refine ⟨g.first_genuine, g.first_out, g.slot_out, g.kept_genuine, g.start_le, g.covers_le,
      fun A => ?_, g.floor_announced, fun A r au v c q h1 h2 h3 h4 => ?_, fun A need => ?_⟩
    · have hne : st.floor.path A ≠ p := by
        intro he; have := g.floor_body A; rw [he, hp] at this; cases this
      simp only [hne, ite_false]; exact g.floor_body A
    · have hne : q ≠ p := by intro he; subst he; exact hfree A r au v c h1 h2 h3 h4
      simp only [hne, ite_false]; exact g.boundary_body A r au v c q h1 h2 h3 h4
    · refine ⟨fun a cs q snap => ?_, (g.rebuild_kept A need).2⟩
      have kept := (g.rebuild_kept A need).1 a cs q snap
      have hne : q ≠ p := by intro he; subst he; rw [hp] at kept; cases kept.1
      refine ⟨by simp only [hne, ite_false]; exact kept.1, kept.2.1, fun r au v c q' h1 h2 h3 h4 => ?_⟩
      obtain ⟨L', P', hb⟩ := kept.2.2 r au v c q' h1 h2 h3 h4
      have hne' : q' ≠ p := by intro he; subst he; rw [hp] at hb; cases hb
      exact ⟨L', P', by simp only [hne', ite_false]; exact hb⟩

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

/-- What holds the first kept slot back for the circle `A` that needs a
rebuild: the coverage plus one of the store snapshot its rebuild starts from,
or 1 when history holds none. -/
def RebuildCap (seq : Nat → Slot S) (head : Nat) (A : S.Aud) (cap : Nat) : Prop :=
  (∃ a cs p, RebuildSnapshot R seq head A a cs p ∧ cap = cs + 1) ∨
    ((¬ ∃ a cs p, RebuildSnapshot R seq head A a cs p) ∧ cap = 1)

/-- Every circle that needs a rebuild has a cap. -/
theorem rebuild_cap_exists (seq : Nat → Slot S) (head : Nat) (A : S.Aud) :
    ∃ cap, RebuildCap R seq head A cap := by
  by_cases h : ∃ a cs p, RebuildSnapshot R seq head A a cs p
  · obtain ⟨a, cs, p, snap⟩ := h
    exact ⟨cs + 1, Or.inl ⟨a, cs, p, snap, rfl⟩⟩
  · exact ⟨1, Or.inr ⟨h, rfl⟩⟩

/-- **A first kept slot within every cap spares what rebuilds read.** A floor
whose first kept slot is at most the cap of each circle that needs a rebuild
leaves the store snapshot each starts from, and the slots after it, in
storage: it meets the premises `StoreStep.writeFloor` takes about rebuilds.
`Retention.sparedFirst` is a first kept slot of that kind. -/
theorem floor_within_caps_spares_rebuild (seq : Nat → Slot S) (head : Nat) (F : Floor S)
    (spared : ∀ A cap, NeedsRebuild R seq head A → RebuildCap R seq head A cap → F.start ≤ cap) :
    (∀ A a cs p, NeedsRebuild R seq head A → RebuildSnapshot R seq head A a cs p →
      F.start ≤ cs + 1) ∧
    (∀ A, NeedsRebuild R seq head A → (¬ ∃ a cs p, RebuildSnapshot R seq head A a cs p) →
      F.start ≤ 1) :=
  ⟨fun A a cs p need snap => spared A (cs + 1) need (Or.inl ⟨a, cs, p, snap, rfl⟩),
   fun A need none => spared A 1 need (Or.inr ⟨none, rfl⟩)⟩

/-- **A circle that needs a rebuild finds the store snapshot to start from.**
In good storage, the snapshot it starts from is in storage with its state,
read as a sound reader of the store at its coverage, and every slot after the
coverage is still kept. -/
theorem rebuild_store_start {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) {A : S.Aud}
    (need : NeedsRebuild R seq st.head A) {a cs : Nat} {p : S.Path}
    (snap : RebuildSnapshot R seq st.head A a cs p) :
    ∃ d : Reader S, ReaderOk R st seq R.store d ∧ d.pos = cs ∧
      st.body p = some (d.ledger, d.aud, d.rows) ∧ st.floor.start ≤ d.pos + 1 := by
  obtain ⟨hbody, hstart, _⟩ := (g.rebuild_kept A need).1 a cs p snap
  obtain ⟨ha1, hahead, ⟨au, w, hs, _⟩, ⟨_, valid⟩⟩ := snap.1
  rw [hs] at valid
  simp only [audValid, Bool.and_eq_true, decide_eq_true_eq] at valid
  refine ⟨⟨cs, ledgerAt R seq cs, audAt R seq R.store cs, rowsAt R seq R.store cs, fun _ => none⟩,
    ⟨rfl, rfl, rfl, (by show cs ≤ st.head; omega), fun i x hb _ => by simp at hb⟩, rfl, hbody, hstart⟩

/-- **The claimant's floor check.** A claimant that saw its bytes `x` at slot
`i`, and then, after any storage steps, reads a floor whose first kept slot is
not past `i`, holds the genuine slot `i`. If the floor has passed `i`, these bytes
cannot settle the claim: use the floor snapshot's claim number and digest. -/
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

namespace CovenSequence.Retention

/-- The slot orders choices; the provider timestamp upper bound starts grace. -/
structure Snapshot where
  slot : Nat
  covers : Nat
  announcedAt : Nat
  deriving DecidableEq

def eligible (usable : Snapshot → Bool) (grace time : Nat) (s : Snapshot) : Bool :=
  usable s && decide (s.announcedAt + grace ≤ time)

/-- Announcements are newest first. Grace filters candidates before selection. -/
def choose (snapshots : List Snapshot) (usable : Snapshot → Bool) (grace time : Nat) :
    Option Snapshot := snapshots.find? (eligible usable grace time)

/-- A qualifying witness cannot be hidden by more recent announcements. -/
theorem choose_dominates (rank : Snapshot → Nat) (snapshots : List Snapshot)
    (ordered : snapshots.Pairwise (fun newer older => rank older ≤ rank newer))
    (usable : Snapshot → Bool) (grace time : Nat) {w : Snapshot}
    (hw : w ∈ snapshots) (he : eligible usable grace time w = true) :
    ∃ s, choose snapshots usable grace time = some s ∧
      s ∈ snapshots ∧ eligible usable grace time s = true ∧ rank w ≤ rank s := by
  induction snapshots with
  | nil => simp at hw
  | cons head tail ih =>
    rw [List.pairwise_cons] at ordered
    by_cases hh : eligible usable grace time head = true
    · refine ⟨head, by simp [choose, hh], by simp, hh, ?_⟩
      rcases List.mem_cons.mp hw with h | h
      · subst w; exact Nat.le_refl _
      · exact ordered.1 w h
    · have hwt : w ∈ tail := by
        rcases List.mem_cons.mp hw with h | h
        · subst w; exact False.elim (hh he)
        · exact h
      obtain ⟨s, hs, hm, he, hr⟩ := ih ordered.2 hwt
      exact ⟨s, by simpa [choose, hh] using hs, by simp [hm], he, hr⟩

/-- Among eligible announcements, the selected slot is the newest. -/
theorem chosen_newest (snapshots : List Snapshot)
    (ordered : snapshots.Pairwise (fun newer older => older.slot ≤ newer.slot))
    (usable : Snapshot → Bool) (grace time : Nat) {selected witness : Snapshot}
    (chosen : choose snapshots usable grace time = some selected)
    (member : witness ∈ snapshots) (qualifies : eligible usable grace time witness = true) :
    witness.slot ≤ selected.slot := by
  obtain ⟨s, hs, _, _, greatest⟩ :=
    choose_dominates Snapshot.slot snapshots ordered usable grace time member qualifies
  have same : s = selected := Option.some.inj (hs.symm.trans chosen)
  simpa [same] using greatest

/-- The selected snapshot cannot pass the replacement-body deletion gate. -/
theorem chosen_has_no_mature_replacement (snapshots : List Snapshot)
    (ordered : snapshots.Pairwise (fun newer older => older.slot ≤ newer.slot))
    (usable : Snapshot → Bool) (grace time : Nat) {selected : Snapshot}
    (chosen : choose snapshots usable grace time = some selected) :
    ∀ newer ∈ snapshots, selected.slot < newer.slot →
      eligible usable grace time newer = false := by
  intro newer member later
  cases qualifies : eligible usable grace time newer with
  | false => rfl
  | true =>
    have := chosen_newest snapshots ordered usable grace time chosen member qualifies
    omega

/-- The same selection gates both floor use and replacement-body deletion. -/
theorem chosen_mature (snapshots : List Snapshot) (usable : Snapshot → Bool)
    (grace time : Nat) {s : Snapshot}
    (selected : choose snapshots usable grace time = some s) :
    s ∈ snapshots ∧ usable s = true ∧ s.announcedAt + grace ≤ time := by
  have he := List.find?_some selected
  exact ⟨List.mem_of_find?_eq_some selected, by simpa [eligible] using he⟩

/-- A body is kept until some applicable replacement has passed grace. -/
theorem immature_replacements_keep_body (snapshots : List Snapshot)
    (usable : Snapshot → Bool) (grace time : Nat)
    (fresh : ∀ s ∈ snapshots, usable s = true → time < s.announcedAt + grace) :
    choose snapshots usable grace time = none := by
  cases selected : choose snapshots usable grace time with
  | none => rfl
  | some s =>
    obtain ⟨member, usable, mature⟩ := chosen_mature snapshots usable grace time selected
    have := fresh s member usable
    omega

/-- For `first`, choosing before checking grace lets every fresh snapshot
restart the wait. The floor's names use `newest`, which has no wait, so this
starvation is about deletion only. -/
def newestThenGrace (snapshots : List Snapshot) (usable : Snapshot → Bool)
    (grace time : Nat) : Option Snapshot := do
  let s ← snapshots.find? usable
  if s.announcedAt + grace ≤ time then some s else none

theorem fresh_head_starves (s : Snapshot) (tail : List Snapshot)
    (usable : Snapshot → Bool) (grace time : Nat)
    (hu : usable s = true) (ht : s.announcedAt = time) (hg : 0 < grace) :
    newestThenGrace (s :: tail) usable grace time = none := by
  have h : ¬ s.announcedAt + grace ≤ time := by omega
  simp [newestThenGrace, hu, h]

def daily (day : Nat) : Snapshot := ⟨day + 2, day + 1, day⟩
theorem daily_coverage_precedes_announcement (day : Nat) :
    (daily day).covers < (daily day).slot := by simp [daily]

def fortnight (day : Nat) : List Snapshot := ((List.range (day + 1)).map daily).reverse

/-- For `first`, daily announcements would hide already mature snapshots for
all fourteen days. -/
theorem daily_announcements_starve :
    ∀ day ∈ List.range 14,
      newestThenGrace (fortnight day) (fun _ => true) 7 day = none := by
  decide

/-- `first` follows the first snapshot on day seven despite newer announcements. -/
theorem daily_announcements_advance :
    ∀ day ∈ (List.range 7).map (· + 7),
      choose (fortnight day) (fun _ => true) 7 day = some (daily (day - 7)) := by
  decide

/-- One coverage per introduced audience, including the store. Missing any
snapshot prevents a floor; an empty audience universe supplies no floor. -/
def floorStart : List (Option Nat) → Option Nat
  | [] => none
  | [covered] => covered.map (· + 1)
  | covered :: next :: rest => do
    let c ← covered
    let first ← floorStart (next :: rest)
    pure (min (c + 1) first)

theorem floor_requires_every_audience (coverage : List (Option Nat))
    (missing : none ∈ coverage) : floorStart coverage = none := by
  induction coverage with
  | nil => simp at missing
  | cons head tail ih =>
    cases tail with
    | nil => simp only [List.mem_singleton] at missing; subst head; rfl
    | cons next rest =>
      rcases List.mem_cons.mp missing with eq | later
      · subst head; rfl
      · simp [floorStart, ih later]

theorem complete_coverage_advances (coverage : List (Option Nat)) (cut : Nat)
    (nonempty : coverage ≠ [])
    (covered : ∀ c ∈ coverage, ∃ n, c = some n ∧ cut ≤ n) :
    ∃ first, floorStart coverage = some first ∧ cut < first := by
  induction coverage with
  | nil => exact False.elim (nonempty rfl)
  | cons head tail ih =>
    obtain ⟨c, rfl, hc⟩ := covered head (by simp)
    cases tail with
    | nil => exact ⟨c + 1, rfl, by omega⟩
    | cons next rest =>
      obtain ⟨first, hf, hc'⟩ := ih (by simp) (fun x hx => covered x (by simp [hx]))
      exact ⟨min (c + 1) first, by simp [floorStart, hf], by omega⟩

theorem floor_covered (coverage : List (Option Nat)) (first : Nat)
    (selected : floorStart coverage = some first) :
    ∀ c ∈ coverage, ∃ n, c = some n ∧ first ≤ n + 1 := by
  induction coverage generalizing first with
  | nil => simp [floorStart] at selected
  | cons head tail ih =>
    cases head with
    | none => cases tail <;> simp [floorStart] at selected
    | some c =>
      cases tail with
      | nil =>
        simp [floorStart] at selected
        subst first
        intro x hx
        simp only [List.mem_singleton] at hx
        subst x
        exact ⟨c, rfl, Nat.le_refl _⟩
      | cons next rest =>
        cases hs : floorStart (next :: rest) with
        | none => simp [floorStart, hs] at selected
        | some f =>
          simp [floorStart, hs] at selected
          subst first
          intro x hx
          rcases List.mem_cons.mp hx with rfl | ht
          · exact ⟨c, rfl, by omega⟩
          · obtain ⟨n, hn, hf⟩ := ih f hs x ht
            exact ⟨n, hn, by omega⟩

/-- The floor names each audience's newest applicable announcement, mature or
not: a new device starts from it at once. Grace bounds only `first`. -/
def newest (snapshots : List Snapshot) (usable : Snapshot → Bool) : Option Snapshot :=
  snapshots.find? usable

/-- The newest applicable snapshot covers at least as much as the newest one
past grace, since coverage never decreases with announcement order. -/
theorem newest_covers_chosen (snapshots : List Snapshot)
    (ordered : snapshots.Pairwise (fun newer older => older.covers ≤ newer.covers))
    (usable : Snapshot → Bool) (grace time : Nat) {mature : Snapshot}
    (chosen : choose snapshots usable grace time = some mature) :
    ∃ s, newest snapshots usable = some s ∧ mature.covers ≤ s.covers := by
  induction snapshots with
  | nil => simp [choose] at chosen
  | cons head tail ih =>
    rw [List.pairwise_cons] at ordered
    cases hu : usable head with
    | true =>
      refine ⟨head, by simp [newest, hu], ?_⟩
      obtain ⟨member, _, _⟩ := chosen_mature _ usable grace time chosen
      rcases List.mem_cons.mp member with h | h
      · subst h; exact Nat.le_refl _
      · exact ordered.1 mature h
    | false =>
      have he : eligible usable grace time head = false := by simp [eligible, hu]
      have later : choose tail usable grace time = some mature := by
        simpa [choose, he] using chosen
      obtain ⟨s, hs, hc⟩ := ih ordered.2 later
      exact ⟨s, by simpa [newest, hu] using hs, hc⟩

section Floor
variable {Aud : Type}

/-- Coverage choices use every introduced audience, even one without writes. -/
def selectedCoverage (relevant : List Aud) (catalog : Aud → List Snapshot)
    (usable : Aud → Snapshot → Bool) (grace time : Nat) : List (Option Nat) :=
  relevant.map fun a => (choose (catalog a) (usable a) grace time).map Snapshot.covers

/-- `first` follows the snapshots past grace and never moves back. While an
audience has none, it stays at the current floor's. -/
def floorFirst (current : Nat) (mature : List (Option Nat)) : Nat :=
  match floorStart mature with
  | some first => max current first
  | none => current

theorem current_le_floorFirst (current : Nat) (mature : List (Option Nat)) :
    current ≤ floorFirst current mature := by
  unfold floorFirst
  split <;> omega

/-- `first` held back for the circles that need a rebuild: no further than any
circle's cap (`CovenSequence.RebuildCap`), the coverage plus one of the store
snapshot its rebuild starts from. -/
def sparedFirst (current : Nat) (mature : List (Option Nat)) (caps : List Nat) : Nat :=
  caps.foldl min (floorFirst current mature)

theorem foldl_min_le_init (caps : List Nat) : ∀ init, caps.foldl min init ≤ init := by
  induction caps with
  | nil => intro init; exact Nat.le_refl _
  | cons c rest ih =>
    intro init
    exact Nat.le_trans (ih (min init c)) (Nat.min_le_left _ _)

theorem foldl_min_le_mem (caps : List Nat) : ∀ init {c : Nat}, c ∈ caps → caps.foldl min init ≤ c := by
  induction caps with
  | nil => intro init c h; simp at h
  | cons x rest ih =>
    intro init c h
    rcases List.mem_cons.mp h with rfl | h
    · exact Nat.le_trans (foldl_min_le_init rest _) (Nat.min_le_right _ _)
    · exact ih (min init x) h

theorem le_foldl_min (caps : List Nat) : ∀ init m, m ≤ init → (∀ c ∈ caps, m ≤ c) →
    m ≤ caps.foldl min init := by
  induction caps with
  | nil => intro init m h _; exact h
  | cons x rest ih =>
    intro init m h hc
    exact ih (min init x) m (Nat.le_min.mpr ⟨h, hc x List.mem_cons_self⟩)
      (fun c hc' => hc c (List.mem_cons_of_mem _ hc'))

theorem foldl_min_eq_init (caps : List Nat) : ∀ init, (∀ c ∈ caps, init ≤ c) →
    caps.foldl min init = init := by
  intro init h
  exact Nat.le_antisymm (foldl_min_le_init caps init)
    (le_foldl_min caps init init (Nat.le_refl _) h)

/-- **`first` spares the store snapshots rebuilds start from.** Held back by
the caps, `first` is no further than any cap or than the floor's choice
without them, never moves back while the current `first` is within every cap
(the rebuild snapshots stay), and is the floor's choice when no cap binds. -/
theorem first_spares_rebuild_store (current : Nat) (mature : List (Option Nat)) (caps : List Nat)
    (within : ∀ c ∈ caps, current ≤ c) :
    sparedFirst current mature caps ≤ floorFirst current mature ∧
    (∀ c ∈ caps, sparedFirst current mature caps ≤ c) ∧
    current ≤ sparedFirst current mature caps ∧
    ((∀ c ∈ caps, floorFirst current mature ≤ c) →
      sparedFirst current mature caps = floorFirst current mature) :=
  ⟨foldl_min_le_init caps _, fun _ hc => foldl_min_le_mem caps _ hc,
   le_foldl_min caps _ _ (current_le_floorFirst current mature) within,
   fun h => foldl_min_eq_init caps _ h⟩

/-- **The combined floor meets `StoreStep.writeFloor`'s rules.** Name each
audience's newest applicable snapshot and set `first` by `floorFirst` from the
snapshots past grace. If the current floor's `first` is covered by its own
names and no name moves back, then `first` doesn't move back (`hstart`), no
coverage does (`hcov`), and every name covers the new `first - 1` (`hle`). -/
theorem floor_write_rules (relevant : List Aud) (catalog : Aud → List Snapshot)
    (usable : Aud → Snapshot → Bool) (grace time current : Nat) (covers : Aud → Nat)
    (ordered : ∀ a ∈ relevant,
      (catalog a).Pairwise (fun newer older => older.covers ≤ newer.covers))
    (kept : ∀ a ∈ relevant, current ≤ covers a + 1)
    (named : ∀ a ∈ relevant, ∃ s, newest (catalog a) (usable a) = some s ∧ covers a ≤ s.covers) :
    current ≤ floorFirst current (selectedCoverage relevant catalog usable grace time) ∧
    ∀ a ∈ relevant, ∃ s, newest (catalog a) (usable a) = some s ∧ covers a ≤ s.covers ∧
      floorFirst current (selectedCoverage relevant catalog usable grace time) ≤ s.covers + 1 := by
  refine ⟨?_, fun a ha => ?_⟩
  · unfold floorFirst
    split <;> omega
  · obtain ⟨s, hs, hcov⟩ := named a ha
    refine ⟨s, hs, hcov, ?_⟩
    have hcur := kept a ha
    unfold floorFirst
    split
    · next first hf =>
      have hmem : (choose (catalog a) (usable a) grace time).map Snapshot.covers ∈
          selectedCoverage relevant catalog usable grace time :=
        List.mem_map.mpr ⟨a, ha, rfl⟩
      obtain ⟨n, hn, hle⟩ := floor_covered _ first hf _ hmem
      cases hc : choose (catalog a) (usable a) grace time with
      | none => rw [hc] at hn; cases hn
      | some m =>
        rw [hc] at hn
        have hm : m.covers = n := Option.some.inj hn
        obtain ⟨s', hs', hdom⟩ := newest_covers_chosen _ (ordered a ha) (usable a) grace time hc
        have : s' = s := Option.some.inj (hs'.symm.trans hs)
        subst this
        omega
    · omega

/-- **The floor with `first` held back still meets its rules.** Holding `first`
back for rebuilds, within every cap, moves it no further than the floor's
choice, so it never moves back, and every name still covers the new
`first - 1`. -/
theorem floor_write_spared (relevant : List Aud) (catalog : Aud → List Snapshot)
    (usable : Aud → Snapshot → Bool) (grace time current : Nat) (covers : Aud → Nat)
    (caps : List Nat) (within : ∀ c ∈ caps, current ≤ c)
    (ordered : ∀ a ∈ relevant,
      (catalog a).Pairwise (fun newer older => older.covers ≤ newer.covers))
    (kept : ∀ a ∈ relevant, current ≤ covers a + 1)
    (named : ∀ a ∈ relevant, ∃ s, newest (catalog a) (usable a) = some s ∧ covers a ≤ s.covers) :
    current ≤ sparedFirst current (selectedCoverage relevant catalog usable grace time) caps ∧
    ∀ a ∈ relevant, ∃ s, newest (catalog a) (usable a) = some s ∧ covers a ≤ s.covers ∧
      sparedFirst current (selectedCoverage relevant catalog usable grace time) caps ≤ s.covers + 1 := by
  obtain ⟨_, hnames⟩ :=
    floor_write_rules relevant catalog usable grace time current covers ordered kept named
  obtain ⟨hle, _, hcur, _⟩ := first_spares_rebuild_store current
    (selectedCoverage relevant catalog usable grace time) caps within
  refine ⟨hcur, fun a ha => ?_⟩
  obtain ⟨s, hs, hcov, hf⟩ := hnames a ha
  exact ⟨s, hs, hcov, by omega⟩

end Floor

/-- The announcements at or after the first kept slot `current`: their slots
are still in storage. -/
def kept (current : Nat) (snapshots : List Snapshot) : List Snapshot :=
  snapshots.filter (fun s => decide (current ≤ s.slot))

/-- Choosing among the kept announcements picks what choosing among all of
them does, unless that choice was announced before `current`; then it picks
none. -/
theorem choose_kept (usable : Snapshot → Bool) (grace time current : Nat) :
    ∀ (snapshots : List Snapshot),
      snapshots.Pairwise (fun newer older => older.slot ≤ newer.slot) →
      choose (kept current snapshots) usable grace time = choose snapshots usable grace time ∨
      (choose (kept current snapshots) usable grace time = none ∧
        ∃ s, choose snapshots usable grace time = some s ∧ s.slot < current)
  | [], _ => Or.inl rfl
  | head :: tail, ordered => by
    rw [List.pairwise_cons] at ordered
    by_cases he : eligible usable grace time head = true
    · by_cases hk : current ≤ head.slot
      · left
        simp [choose, kept, hk, he]
      · right
        have hnil : kept current tail = [] := by
          rw [kept, List.filter_eq_nil_iff]
          intro s hs
          have := ordered.1 s hs
          simp only [decide_eq_true_eq]
          omega
        refine ⟨?_, head, by simp [choose, he], by omega⟩
        rw [choose, show kept current (head :: tail) = kept current tail by simp [kept, hk], hnil]
        rfl
    · have hrest := choose_kept usable grace time current tail ordered.2
      have hall : choose (head :: tail) usable grace time = choose tail usable grace time := by
        simp [choose, he]
      have hkept : choose (kept current (head :: tail)) usable grace time =
          choose (kept current tail) usable grace time := by
        by_cases hk : current ≤ head.slot
        · simp [choose, kept, hk, he]
        · simp [choose, kept, hk]
      rw [hall, hkept]
      exact hrest

/-- **A kept replacement is the one choosing among all would find.** A
snapshot body is deleted once a replacement has passed grace; finding it among
the kept announcements only, retention deletes no body it wouldn't delete
choosing among all of them, and keeps one whose only replacement past grace
was announced before `current`. -/
theorem kept_replacement (usable : Snapshot → Bool) (grace time current : Nat)
    {snapshots : List Snapshot}
    (ordered : snapshots.Pairwise (fun newer older => older.slot ≤ newer.slot)) {s : Snapshot}
    (h : choose (kept current snapshots) usable grace time = some s) :
    choose snapshots usable grace time = some s := by
  rcases choose_kept usable grace time current snapshots ordered with he | ⟨hnone, _⟩
  · rw [← he, h]
  · rw [hnone] at h; cases h

/-- Two coverage choices that differ only where the first has none and the
second a coverage before `current`. -/
def SameFirst (current : Nat) (x y : Option Nat) : Prop :=
  x = y ∨ (x = none ∧ ∃ n, y = some n ∧ n + 1 ≤ current)

/-- `SameFirst` at each position of two coverage lists. -/
inductive SameFirsts (current : Nat) : List (Option Nat) → List (Option Nat) → Prop
  | nil : SameFirsts current [] []
  | cons {x y : Option Nat} {a b : List (Option Nat)} :
      SameFirst current x y → SameFirsts current a b → SameFirsts current (x :: a) (y :: b)

theorem sameFirst_eq {current : Nat} :
    ∀ {a b : List (Option Nat)}, SameFirsts current a b → none ∉ a → a = b
  | [], [], _, _ => rfl
  | x :: a, y :: b, SameFirsts.cons hxy rest, hn => by
    have hx : x ≠ none := fun h => hn (h ▸ List.mem_cons_self)
    have hy : x = y := by
      rcases hxy with h | ⟨h, _⟩
      · exact h
      · exact absurd h hx
    rw [hy, sameFirst_eq rest (fun h => hn (List.mem_cons_of_mem _ h))]

theorem sameFirst_none {current : Nat} :
    ∀ {a b : List (Option Nat)}, SameFirsts current a b → none ∈ a →
      ∃ y ∈ b, y = none ∨ ∃ n, y = some n ∧ n + 1 ≤ current
  | x :: a, y :: b, SameFirsts.cons hxy rest, hn => by
    rcases List.mem_cons.mp hn with h | h
    · subst h
      rcases hxy with h | ⟨_, n, hn, hle⟩
      · exact ⟨y, List.mem_cons_self, Or.inl h.symm⟩
      · exact ⟨y, List.mem_cons_self, Or.inr ⟨n, hn, hle⟩⟩
    · obtain ⟨z, hz, hp⟩ := sameFirst_none rest h
      exact ⟨z, List.mem_cons_of_mem _ hz, hp⟩

/-- Coverage choices that differ only by announcements before `current` give
the same `first`: such an announcement covers less than `current - 1`, so it
can't move `first` past `current`. -/
theorem floorFirst_sameFirst {current : Nat} {a b : List (Option Nat)}
    (h : SameFirsts current a b) : floorFirst current a = floorFirst current b := by
  by_cases hn : none ∈ a
  · have ha : floorFirst current a = current := by
      simp [floorFirst, floor_requires_every_audience a hn]
    obtain ⟨y, hy, hp⟩ := sameFirst_none h hn
    rw [ha]
    unfold floorFirst
    split
    · next f hf =>
      obtain ⟨n', hn', hle⟩ := floor_covered b f hf y hy
      rcases hp with hnone | ⟨n, hsome, hlt⟩
      · rw [hnone] at hn'; cases hn'
      · rw [hsome] at hn'
        cases hn'
        omega
    · rfl
  · rw [sameFirst_eq h hn]

section Kept
variable {Aud : Type}

/-- **Choosing only kept announcements gives the same `first`.** Each
announcement follows its coverage, and the catalog is newest first. Choosing
among the announcements at or after the first kept slot `current` moves
`first` exactly as choosing among all of them does: retention never needs the
slot of an announcement retention may have deleted. -/
theorem kept_candidates_same_first (relevant : List Aud) (catalog : Aud → List Snapshot)
    (usable : Aud → Snapshot → Bool) (grace time current : Nat)
    (ordered : ∀ a ∈ relevant, (catalog a).Pairwise (fun newer older => older.slot ≤ newer.slot))
    (announced : ∀ a ∈ relevant, ∀ s ∈ catalog a, s.covers < s.slot) :
    floorFirst current (selectedCoverage relevant (fun a => kept current (catalog a)) usable grace time) =
      floorFirst current (selectedCoverage relevant catalog usable grace time) := by
  apply floorFirst_sameFirst
  induction relevant with
  | nil => exact SameFirsts.nil
  | cons a rest ih =>
    refine SameFirsts.cons ?_ (ih (fun b hb => ordered b (List.mem_cons_of_mem _ hb))
      (fun b hb => announced b (List.mem_cons_of_mem _ hb)))
    rcases choose_kept (usable a) grace time current (catalog a) (ordered a List.mem_cons_self) with
      h | ⟨hnone, s, hs, hlt⟩
    · exact Or.inl (by simp only [h])
    · refine Or.inr ⟨by simp only [hnone, Option.map_none], s.covers,
        by simp only [hs, Option.map_some], ?_⟩
      have := announced a List.mem_cons_self s (chosen_mature _ _ grace time hs).1
      omega

end Kept


/-- An idle circle still needs a snapshot: its membership and boundaries are
history even when no data-write part belongs to it. -/
theorem idle_audience_blocks_floor :
    floorStart [some 10, none] = none ∧ floorStart [some 10, some 8] = some 9 := by
  decide

end CovenSequence.Retention
