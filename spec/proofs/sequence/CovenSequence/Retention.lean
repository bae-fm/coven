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

end Floor

/-- An idle circle still needs a snapshot: its membership and boundaries are
history even when no data-write part belongs to it. -/
theorem idle_audience_blocks_floor :
    floorStart [some 10, none] = none ∧ floorStart [some 10, some 8] = some 9 := by
  decide

end CovenSequence.Retention
