import CovenSequence.Retention

/-!
# Devices: the audiences a device reads

A device belongs to some audiences: the store and the circles its member is
in. It runs one reader per audience it follows and holds a key per audience.
A reader may apply a slot only while no other of the device's readers is
behind it, so when one reader stops at a slot it should read but cannot, the
device applies nothing after that slot. Loading a floor snapshot is gated the
same way.

A device outside a circle has no reader for it. Its readers apply the circle's
slots without reading their circle parts, and reach the right state for every
audience it follows (`device_reaches_head`, which needs keys only for those).
-/

namespace CovenSequence

variable {S : Sig} (R : Rules S)

structure Device (S : Sig) where
  follows : List S.Aud
  keys : S.Aud → Bool
  reader : S.Aud → Reader S

def Device.set (dv : Device S) (A : S.Aud) (d : Reader S) : Device S :=
  { dv with reader := fun B => if B = A then d else dv.reader B }

@[simp] theorem Device.set_follows (dv : Device S) (A : S.Aud) (d : Reader S) :
    (dv.set A d).follows = dv.follows := rfl
@[simp] theorem Device.set_keys (dv : Device S) (A : S.Aud) (d : Reader S) :
    (dv.set A d).keys = dv.keys := rfl
@[simp] theorem Device.set_same (dv : Device S) (A : S.Aud) (d : Reader S) :
    (dv.set A d).reader A = d := by simp [Device.set]
theorem Device.set_other (dv : Device S) {A B : S.Aud} (d : Reader S) (h : B ≠ A) :
    (dv.set A d).reader B = dv.reader B := by simp [Device.set, h]
@[simp] theorem Device.set_set (dv : Device S) (A : S.Aud) (d d' : Reader S) :
    (dv.set A d).set A d' = dv.set A d' := by
  simp only [Device.set]; congr; funext B; by_cases h : B = A <;> simp [h]

/-- One reader of the device steps. A step that moves a reader needs every
other reader of the device to be at least as far. -/
inductive DevStep (st : Store S) : Device S → Device S → Prop
  | step (dv : Device S) (A : S.Aud) (hA : A ∈ dv.follows) (d' : Reader S)
      (hs : ReadStep R A (dv.keys A) st (dv.reader A) d')
      (hgate : d'.pos ≠ (dv.reader A).pos → ∀ B ∈ dv.follows, (dv.reader A).pos ≤ (dv.reader B).pos) :
      DevStep st dv (dv.set A d')

inductive DevSteps (st : Store S) : Device S → Device S → Prop
  | refl (dv : Device S) : DevSteps st dv dv
  | step {dv dv' dv'' : Device S} : DevStep R st dv dv' → DevSteps st dv' dv'' → DevSteps st dv dv''

theorem DevSteps.trans {st : Store S} {a b c : Device S} (h₁ : DevSteps R st a b) (h₂ : DevSteps R st b c) :
    DevSteps R st a c := by
  induction h₁ with
  | refl => exact h₂
  | step s _ ih => exact DevSteps.step s (ih h₂)

/-- Every reader of every followed audience is sound. -/
def DevOk (st : Store S) (seq : Nat → Slot S) (dv : Device S) : Prop :=
  ∀ A ∈ dv.follows, ReaderOk R st seq A (dv.reader A)

theorem devstep_frame {st : Store S} {dv dv' : Device S} (s : DevStep R st dv dv') :
    dv'.follows = dv.follows ∧ dv'.keys = dv.keys := by
  cases s; exact ⟨rfl, rfl⟩

theorem dev_ok {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) {dv dv' : Device S}
    (s : DevStep R st dv dv') (h : DevOk R st seq dv) : DevOk R st seq dv' := by
  cases s with
  | step A hA d' hs hgate =>
    intro B hB
    simp only [Device.set_follows] at hB
    by_cases hBA : B = A
    · subst hBA; rw [Device.set_same]; exact read_ok R g (h B hB) hs
    · rw [Device.set_other _ _ hBA]; exact h B hB

theorem devs_ok {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) {dv dv' : Device S}
    (run : DevSteps R st dv dv') (h : DevOk R st seq dv) : DevOk R st seq dv' := by
  induction run with
  | refl => exact h
  | step s _ ih => exact ih (dev_ok R g s h)

theorem devs_frame {st : Store S} {dv dv' : Device S} (run : DevSteps R st dv dv') :
    dv'.follows = dv.follows ∧ dv'.keys = dv.keys := by
  induction run with
  | refl => exact ⟨rfl, rfl⟩
  | step s _ ih => obtain ⟨a, b⟩ := devstep_frame R s; exact ⟨ih.1.trans a, ih.2.trans b⟩

/-- Fetching never moves a reader, so it needs no gate. -/
theorem lift_fetch (st : Store S) (dv : Device S) (A : S.Aud) (hA : A ∈ dv.follows) (lo : Nat) :
    ∀ n, DevSteps R st dv (dv.set A (fetched st (dv.reader A) lo n))
  | 0 => by
    rw [fetched_zero]
    have : dv.set A (dv.reader A) = dv := by
      cases dv; simp only [Device.set]; congr; funext B; by_cases h : B = A <;> simp [h]
    rw [this]; exact DevSteps.refl dv
  | n + 1 => by
    have ih := lift_fetch st dv A hA lo n
    rw [fetched_succ]
    refine DevSteps.trans R ih ?_
    have hs := DevStep.step (R := R) (st := st) (dv.set A (fetched st (dv.reader A) lo n)) A
      (by simpa using hA) _ (by rw [Device.set_same]; exact ReadStep.fetch _ (lo + n))
      (by intro h; simp at h)
    rw [Device.set_set] at hs
    exact DevSteps.step hs (DevSteps.refl _)

theorem exists_min {α : Type} (f : α → Nat) : ∀ (l : List α), l ≠ [] → ∃ a ∈ l, ∀ b ∈ l, f a ≤ f b
  | [], h => absurd rfl h
  | [x], _ => ⟨x, by simp, by simp⟩
  | x :: y :: rest, _ => by
    obtain ⟨m, hm, hmin⟩ := exists_min f (y :: rest) (by simp)
    by_cases hx : f x ≤ f m
    · refine ⟨x, by simp, fun b hb => ?_⟩
      simp only [List.mem_cons] at hb
      rcases hb with rfl | hb
      · exact Nat.le_refl _
      · exact Nat.le_trans hx (hmin b (by simpa using hb))
    · refine ⟨m, List.mem_cons_of_mem _ hm, fun b hb => ?_⟩
      simp only [List.mem_cons] at hb
      rcases hb with rfl | hb
      · omega
      · exact hmin b (by simpa using hb)

theorem sum_le {α : Type} (f g : α → Nat) :
    ∀ (l : List α), (∀ x ∈ l, g x ≤ f x) → (l.map g).sum ≤ (l.map f).sum
  | [], _ => by simp
  | x :: rest, hle => by
    simp only [List.map_cons, List.sum_cons]
    have h1 := hle x (by simp)
    have h2 := sum_le f g rest (fun z hz => hle z (List.mem_cons_of_mem _ hz))
    omega

theorem sum_lt {α : Type} (f g : α → Nat) (a : α) :
    ∀ (l : List α), (∀ x ∈ l, g x ≤ f x) → a ∈ l → g a < f a → (l.map g).sum < (l.map f).sum
  | [], _, ha, _ => absurd ha (by simp)
  | x :: rest, hle, ha, hlt => by
    simp only [List.map_cons, List.sum_cons]
    have hrest := sum_le f g rest (fun z hz => hle z (List.mem_cons_of_mem _ hz))
    have hx := hle x (by simp)
    simp only [List.mem_cons] at ha
    rcases ha with rfl | ha
    · omega
    · have := sum_lt f g a rest (fun z hz => hle z (List.mem_cons_of_mem _ hz)) ha hlt
      omega

/-- **A device with the keys of its audiences reaches the head in every one.**
On good storage, a device whose readers are sound, holding the key of every
audience it follows (and of no other), can run its readers to the head; each
then holds the state of every slot for its audience. -/
theorem device_reaches_head {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) :
    ∀ (dv : Device S), DevOk R st seq dv → (∀ A ∈ dv.follows, dv.keys A = true) →
      ∃ dv', DevSteps R st dv dv' ∧ DevOk R st seq dv' ∧ ∀ A ∈ dv.follows, (dv'.reader A).pos = st.head := by
  intro dv
  generalize hm : (dv.follows.map (fun B => st.head - (dv.reader B).pos)).sum = m
  induction m using Nat.strongRecOn generalizing dv with
  | ind m ih =>
    intro hok hkeys
    by_cases hall : ∀ A ∈ dv.follows, (dv.reader A).pos = st.head
    · exact ⟨dv, DevSteps.refl dv, hok, hall⟩
    have hne : dv.follows ≠ [] := by intro h; apply hall; simp [h]
    obtain ⟨a, ha, hmin⟩ := exists_min (fun B => (dv.reader B).pos) dv.follows hne
    have hlt : (dv.reader a).pos < st.head := by
      have : ∃ A ∈ dv.follows, (dv.reader A).pos ≠ st.head :=
        Classical.byContradiction fun hc =>
          hall (fun A hA => Classical.byContradiction fun h => hc ⟨A, hA, h⟩)
      obtain ⟨A, hA, hAne⟩ := this
      have := hmin A hA
      have := (hok A hA).le_head
      omega
    obtain ⟨lo, n, d', hs, hpos⟩ := read_progress R g (hok a ha) hlt
    have run1 := lift_fetch R st dv a ha lo n
    have hs' := DevStep.step (R := R) (st := st) (dv.set a (fetched st (dv.reader a) lo n)) a
      (by simpa using ha) d' (by rw [Device.set_same, Device.set_keys, hkeys a ha]; exact hs)
      (by
        intro _ B hB
        rw [Device.set_same, fetched_pos]
        by_cases hBa : B = a
        · subst hBa; simp
        · rw [Device.set_other _ _ hBa]; exact hmin B (by simpa using hB))
    rw [Device.set_set] at hs'
    have run2 := DevSteps.trans R run1 (DevSteps.step hs' (DevSteps.refl _))
    have hok2 := devs_ok R g run2 hok
    have hd'le : d'.pos ≤ st.head := by
      have := (hok2 a (by simpa using ha)); rw [Device.set_same] at this; exact this.le_head
    have hdec : ((dv.set a d').follows.map (fun B => st.head - ((dv.set a d').reader B).pos)).sum < m := by
      rw [← hm, Device.set_follows]
      apply sum_lt _ _ a dv.follows
      · intro x hx
        by_cases hxa : x = a
        · subst hxa; rw [Device.set_same]; omega
        · rw [Device.set_other _ _ hxa]; exact Nat.le_refl _
      · exact ha
      · rw [Device.set_same]; omega
    obtain ⟨dv'', run3, hok3, hpos3⟩ := ih _ hdec (dv.set a d') rfl hok2 (by simpa using hkeys)
    exact ⟨dv'', DevSteps.trans R run2 run3, hok3, by simpa using hpos3⟩

theorem read_pos {A : S.Aud} {key : Bool} {st : Store S} {d d' : Reader S}
    (s : ReadStep R A key st d d') : d'.pos = d.pos ∨ d'.pos = d.pos + 1 ∨ d'.pos = st.floor.covers A := by
  cases s <;> simp

/-- **A device stops at a slot it should read but cannot.** A device that
follows `B` without `B`'s key, with `B`'s reader sound and its next slot
having a part or announcement for `B`, never moves that reader. Every other
reader applies at most up to that slot, or loads its own floor snapshot. -/
theorem device_stops {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) {dv : Device S}
    {B : S.Aud} (hB : B ∈ dv.follows) (hkey : dv.keys B = false) (hok : DevOk R st seq dv)
    (htouch : touches B (seq ((dv.reader B).pos + 1)) = true) :
    ∀ dv', DevSteps R st dv dv' →
      (dv'.reader B).pos = (dv.reader B).pos ∧
      ∀ A ∈ dv.follows, (dv'.reader A).pos ≤
        max (dv.reader A).pos (max ((dv.reader B).pos + 1) (st.floor.covers A)) := by
  intro dv' run
  -- Strengthen to an invariant over every intermediate device.
  suffices h : ∀ d₁ d₂, DevSteps R st d₁ d₂ → d₁.follows = dv.follows → d₁.keys = dv.keys →
      DevOk R st seq d₁ → (d₁.reader B).pos = (dv.reader B).pos →
      (∀ A ∈ dv.follows, (d₁.reader A).pos ≤
        max (dv.reader A).pos (max ((dv.reader B).pos + 1) (st.floor.covers A))) →
      (d₂.reader B).pos = (dv.reader B).pos ∧
      ∀ A ∈ dv.follows, (d₂.reader A).pos ≤
        max (dv.reader A).pos (max ((dv.reader B).pos + 1) (st.floor.covers A)) from
    h dv dv' run rfl rfl hok rfl (fun A _ => by omega)
  intro d₁ d₂ run
  induction run with
  | refl => intro _ _ _ hb hall; exact ⟨hb, hall⟩
  | @step x y z s _ ih =>
    intro hf hk hokx hb hall
    have ⟨hfy, hky⟩ := devstep_frame R s
    have hoky := dev_ok R g s hokx
    apply ih (hfy.trans hf) (hky.trans hk) hoky
    · cases s with
      | step A hA d' hs hgate =>
        by_cases hAB : B = A
        · subst hAB
          rw [Device.set_same]
          by_cases hm : d'.pos = (x.reader B).pos
          · rw [hm, hb]
          · rw [hk, hkey] at hs
            have := (keyless_passes_only_others R (hokx B (by rw [hf]; exact hB)) hs hm).2
            rw [hb, htouch] at this; cases this
        · rw [Device.set_other _ _ hAB]; exact hb
    · intro C hC
      cases s with
      | step A hA d' hs hgate =>
        by_cases hCA : C = A
        · subst hCA
          rw [Device.set_same]
          by_cases hm : d'.pos = (x.reader C).pos
          · rw [hm]; exact hall C hC
          · have hg := hgate hm B (by rw [hf]; exact hB)
            rcases read_pos R hs with h | h | h
            · exact absurd h hm
            · rw [h]; rw [hb] at hg; omega
            · rw [h]; omega
        · rw [Device.set_other _ _ hCA]; exact hall C hC

/-! ## Storage and devices together -/

/-- Storage and one device, interleaved. -/
inductive SysStep (seq : Nat → Slot S) : Store S × Device S → Store S × Device S → Prop
  | store (st st' : Store S) (dv : Device S) (h : StoreStep R seq st st') : SysStep seq (st, dv) (st', dv)
  | device (st : Store S) (dv dv' : Device S) (h : DevStep R st dv dv') : SysStep seq (st, dv) (st, dv')

inductive SysSteps (seq : Nat → Slot S) : Store S × Device S → Store S × Device S → Prop
  | refl (x : Store S × Device S) : SysSteps seq x x
  | step {x y z : Store S × Device S} : SysStep R seq x y → SysSteps seq y z → SysSteps seq x z

/-- **Deleting covered slots never changes any device's reachable state.**
Under any interleaving of claims, stale creates, floor writes, deletions,
body uploads and reader steps, storage stays good and every reader of the
device stays at a prefix state of its audience; and from wherever they are,
a device holding the keys of its audiences can still run every reader to the
head of the current storage. -/
theorem deletion_keeps_reachable {seq : Nat → Slot S} {x y : Store S × Device S}
    (run : SysSteps R seq x y) (g : Good R x.1 seq) (hok : DevOk R x.1 seq x.2) :
    Good R y.1 seq ∧ DevOk R y.1 seq y.2 ∧ y.2.follows = x.2.follows ∧ y.2.keys = x.2.keys ∧
      ((∀ A ∈ x.2.follows, x.2.keys A = true) →
        ∃ dv', DevSteps R y.1 y.2 dv' ∧
          ∀ A ∈ x.2.follows, (dv'.reader A).pos = y.1.head ∧
            ((dv'.reader A).ledger, (dv'.reader A).aud, (dv'.reader A).rows) = stateAt R seq A y.1.head) := by
  have inv : Good R y.1 seq ∧ DevOk R y.1 seq y.2 ∧ y.2.follows = x.2.follows ∧ y.2.keys = x.2.keys := by
    induction run with
    | refl => exact ⟨g, hok, rfl, rfl⟩
    | step s _ ih =>
      cases s with
      | store st st' dv h =>
        exact ih (store_good R h g) (fun A hA => store_reader R h (hok A hA))
      | device st dv dv' h =>
        obtain ⟨a, b, c, d⟩ := ih g (dev_ok R g h hok)
        obtain ⟨e, f⟩ := devstep_frame R h
        exact ⟨a, b, c.trans e, d.trans f⟩
  obtain ⟨g', hok', hf, hk⟩ := inv
  refine ⟨g', hok', hf, hk, fun hkeys => ?_⟩
  obtain ⟨dv', run', hok'', hpos⟩ := device_reaches_head R g' y.2 hok'
    (by intro A hA; rw [hk]; exact hkeys A (by rw [← hf]; exact hA))
  refine ⟨dv', run', fun A hA => ?_⟩
  have hA' : A ∈ y.2.follows := by rw [hf]; exact hA
  have hp := hpos A hA'
  have hr := hok'' A (by rw [(devs_frame R run').1]; exact hA')
  refine ⟨hp, ?_⟩
  rw [hr.ledger, hr.aud, hr.rows, hp]; rfl

end CovenSequence
