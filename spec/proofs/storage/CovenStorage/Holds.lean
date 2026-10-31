namespace CovenStorage.Holds

/-! §17.4: bound uploads hold one device's writes. Writes are numbered as they
commit and go out in order, a prefix at a time: one writes slot holds every
waiting write the device may send. A write staging a file for a bound
reference queues a bound upload, which records the first write it holds back:
its own, or, while another bound upload is queued, that one's. The write that
ends an upload, by completing it or by dropping its file's last reference,
takes its hold away. A pass sends the largest prefix that ends before every
queued bound upload's held write and, while a breaking raise is captured, at
or before its marker. The raise is published only once its marker is (§18.5).

Proved here, in every state the steps reach: no write from a held write on is
published while its upload is queued (`held_unpublished`), and a pass that
publishes a held write publishes the write ending its upload in the same
prefix, unless a raise's marker falls between them (`held_with_completion`).
With a marker between them: the raise waits while the held write's upload is
queued (`raise_waits_for_held`), a pass that publishes the held write
publishes through the marker (`held_with_marker`), and the write ending the
upload waits for the raise (`completion_after_raise`). -/

/-- One device's queue. `next` is the next write's number and `published` the
last write published. Each queued bound upload, by id, has the write staging
its file and the first write it holds back. `ended` pairs each upload's
staging write with the write that ended it. `capture` is a breaking raise's
marker while it waits to be published. -/
structure State where
  next : Nat
  published : Nat
  queued : Nat → Option (Nat × Nat)
  ended : List (Nat × Nat)
  capture : Option Nat

/-- A prefix the device may send: written, at or before a raise's marker,
and before every queued bound upload's first held write. -/
def Sendable (s : State) (k : Nat) : Prop :=
  k < s.next ∧ (∀ r, s.capture = some r → k ≤ r) ∧
    (∀ u w h, s.queued u = some (w, h) → k < h)

/-- What a device does. `write` commits any other write, one setting a bound
reference to a file a queued bound upload stores included. `stage` commits a
write staging a file for a bound reference; its upload holds from its own
write when no bound upload is queued, and otherwise from the first write a
queued one holds. `finish` commits the write completing an upload, or
dropping its file's last reference. `publish` sends the largest prefix it may.
`capture` and `raised` begin and end a breaking raise's wait; the raise is
published only once every write through its marker is. -/
inductive Step : State → State → Prop where
  | write (s : State) : Step s { s with next := s.next + 1 }
  | stage (s : State) (u h : Nat) (fresh : s.queued u = none)
      (first : (h = s.next ∧ ∀ v, s.queued v = none) ∨
        ((∃ v w, s.queued v = some (w, h)) ∧
          ∀ v w h', s.queued v = some (w, h') → h ≤ h')) :
      Step s { s with
        next := s.next + 1
        queued := fun x => if x = u then some (s.next, h) else s.queued x }
  | finish (s : State) (u w h : Nat) (waiting : s.queued u = some (w, h)) :
      Step s { s with
        next := s.next + 1
        queued := fun x => if x = u then none else s.queued x
        ended := (w, s.next) :: s.ended }
  | publish (s : State) (k : Nat) (ahead : s.published ≤ k) (sendable : Sendable s k)
      (largest : ∀ k', Sendable s k' → k' ≤ k) :
      Step s { s with published := k }
  | capture (s : State) (r : Nat) (after : s.published ≤ r) (written : r < s.next) :
      Step s { s with capture := some r }
  | raised (s : State) (landed : ∀ m, s.capture = some m → m ≤ s.published) :
      Step s { s with capture := none }

/-- What every reachable state keeps. -/
structure Inv (s : State) : Prop where
  /-- A queued upload's staging write is written. -/
  staged : ∀ u w h, s.queued u = some (w, h) → w < s.next
  /-- Every queued upload holds from at or before every queued staging
  write, its own included. -/
  together : ∀ u v wu hu wv hv,
    s.queued u = some (wu, hu) → s.queued v = some (wv, hv) → hv ≤ wu
  /-- Every ending write is written. -/
  written : ∀ p ∈ s.ended, p.2 < s.next
  /-- A queued upload holds from at or before an ended upload's staging
  write, or from after the write that ended it. -/
  apart : ∀ p ∈ s.ended, ∀ u w h, s.queued u = some (w, h) → h ≤ p.1 ∨ p.2 < h
  /-- Nothing a queued upload holds is published. -/
  unpublished : ∀ u w h, s.queued u = some (w, h) → s.published < h
  /-- Only written writes are published. -/
  behind : s.published < s.next
  /-- Nothing after a captured raise's marker is published while it waits. -/
  captured : ∀ m, s.capture = some m → s.published ≤ m

/-- A device that has written nothing. -/
def initial : State :=
  { next := 1, published := 0, queued := fun _ => none, ended := [], capture := none }

theorem initial_inv : Inv initial := by
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> simp [initial]

theorem step_inv {s t : State} (st : Step s t) (i : Inv s) : Inv t := by
  cases st with
  | write =>
    refine ⟨?_, i.together, ?_, i.apart, i.unpublished, ?_, i.captured⟩
    · intro u w h q; have := i.staged u w h q; simp only; omega
    · intro p hp; have := i.written p hp; simp only; omega
    · have := i.behind; simp only; omega
  | stage u h fresh first =>
    have lt : ∀ v w h', s.queued v = some (w, h') → h' < s.next := fun v w h' q =>
      Nat.lt_of_le_of_lt (i.together v v w h' w h' q q) (i.staged v w h' q)
    have hle : h ≤ s.next := by
      rcases first with ⟨e, _⟩ | ⟨⟨v, w, q⟩, _⟩
      · omega
      · exact Nat.le_of_lt (lt v w h q)
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_, i.captured⟩
    · intro x w h' q
      simp only at q ⊢
      by_cases e : x = u
      · simp [e] at q; omega
      · simp [e] at q; have := i.staged x w h' q; omega
    · intro x y wx hx wy hy qx qy
      simp only at qx qy
      by_cases ex : x = u <;> by_cases ey : y = u
      · simp [ex] at qx; simp [ey] at qy; omega
      · simp [ex] at qx; simp [ey] at qy
        have := lt y wy hy qy; omega
      · simp [ex] at qx; simp [ey] at qy
        obtain ⟨e1, e2⟩ := qy
        subst e1; subst e2
        rcases first with ⟨_, none⟩ | ⟨_, least⟩
        · rw [none x] at qx; simp at qx
        · exact Nat.le_trans (least x wx hx qx) (i.together x x wx hx wx hx qx qx)
      · simp [ex] at qx; simp [ey] at qy
        exact i.together x y wx hx wy hy qx qy
    · intro p hp; have := i.written p hp; simp only; omega
    · intro p hp x w h' q
      simp only at q
      by_cases e : x = u
      · simp [e] at q
        obtain ⟨_, e2⟩ := q
        subst e2
        rcases first with ⟨e, _⟩ | ⟨⟨v, w', q'⟩, _⟩
        · have := i.written p hp; omega
        · exact i.apart p hp v w' h q'
      · simp [e] at q; exact i.apart p hp x w h' q
    · intro x w h' q
      simp only at q ⊢
      by_cases e : x = u
      · simp [e] at q
        obtain ⟨_, e2⟩ := q
        subst e2
        rcases first with ⟨e, _⟩ | ⟨⟨v, w', q'⟩, _⟩
        · have := i.behind; omega
        · exact i.unpublished v w' h q'
      · simp [e] at q; exact i.unpublished x w h' q
    · have := i.behind; simp only; omega
  | finish u w h waiting =>
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_, i.captured⟩
    · intro x w' h' q
      simp only at q ⊢
      by_cases e : x = u
      · simp [e] at q
      · simp [e] at q; have := i.staged x w' h' q; omega
    · intro x y wx hx wy hy qx qy
      simp only at qx qy
      by_cases ex : x = u
      · simp [ex] at qx
      · by_cases ey : y = u
        · simp [ey] at qy
        · simp [ex] at qx; simp [ey] at qy
          exact i.together x y wx hx wy hy qx qy
    · intro p hp
      simp only [List.mem_cons] at hp ⊢
      rcases hp with e | hp
      · subst e; simp
      · have := i.written p hp; omega
    · intro p hp x w' h' q
      simp only [List.mem_cons] at hp q
      by_cases e : x = u
      · simp [e] at q
      · simp [e] at q
        rcases hp with e' | hp
        · subst e'
          left
          exact i.together u x w h w' h' waiting q
        · exact i.apart p hp x w' h' q
    · intro x w' h' q
      simp only at q ⊢
      by_cases e : x = u
      · simp [e] at q
      · simp [e] at q; exact i.unpublished x w' h' q
    · have := i.behind; simp only; omega
  | publish k ahead sendable largest =>
    refine ⟨i.staged, i.together, i.written, i.apart, ?_, sendable.1, ?_⟩
    · intro x w h q
      exact sendable.2.2 x w h q
    · intro m cm
      exact sendable.2.1 m cm
  | capture r after written =>
    refine ⟨i.staged, i.together, i.written, i.apart, i.unpublished, i.behind, ?_⟩
    intro m cm
    simp only [Option.some.injEq] at cm
    subst cm
    exact after
  | raised landed =>
    refine ⟨i.staged, i.together, i.written, i.apart, i.unpublished, i.behind, ?_⟩
    intro m cm
    simp at cm

/-- The states a device reaches from one that has written nothing. -/
inductive Reaches : State → Prop where
  | initial : Reaches initial
  | step {s t : State} : Reaches s → Step s t → Reaches t

theorem reaches_inv {s : State} (r : Reaches s) : Inv s := by
  induction r with
  | initial => exact initial_inv
  | step _ st ih => exact step_inv st ih

/-- §17.4: while a bound upload is queued, no write from the one staging its
file on is published. -/
theorem held_unpublished {s : State} (r : Reaches s) {u w h : Nat}
    (q : s.queued u = some (w, h)) : s.published < w := by
  have i := reaches_inv r
  have := i.unpublished u w h q
  have := i.together u u w h w h q q
  omega

/-- §17.4: a pass that publishes a held write publishes the write ending its
upload in the same prefix, unless a raise's marker falls between them. -/
theorem held_with_completion {s : State} (r : Reaches s) {k : Nat}
    (sendable : Sendable s k) (largest : ∀ k', Sendable s k' → k' ≤ k)
    {p : Nat × Nat} (hp : p ∈ s.ended) (held : p.1 ≤ k) :
    p.2 ≤ k ∨ ∃ m, s.capture = some m ∧ p.1 ≤ m ∧ m < p.2 := by
  have i := reaches_inv r
  by_cases marker : ∃ m, s.capture = some m ∧ m < p.2
  · obtain ⟨m, cm, lt⟩ := marker
    exact Or.inr ⟨m, cm, Nat.le_trans held (sendable.2.1 m cm), lt⟩
  · left
    apply largest
    refine ⟨i.written p hp, ?_, ?_⟩
    · intro m cm
      apply Nat.le_of_not_lt
      intro lt
      exact marker ⟨m, cm, lt⟩
    · intro u w h q
      rcases i.apart p hp u w h q with le | lt
      · have := sendable.2.2 u w h q; omega
      · exact lt

/-- §17.4, §18.5: while a bound upload is queued whose held write is at or
before a captured raise's marker, the raise can't be published: its marker
isn't. -/
theorem raise_waits_for_held {s : State} (r : Reaches s) {u w h m : Nat}
    (q : s.queued u = some (w, h)) (cm : s.capture = some m) (before : w ≤ m) :
    ¬ ∀ m', s.capture = some m' → m' ≤ s.published := by
  intro landed
  have := held_unpublished r q
  have := landed m cm
  omega

/-- §17.4, §18.5: a pass that publishes a held write whose upload ended after
a captured raise's marker publishes every write through the marker: the held
write and the marker go out in one prefix. -/
theorem held_with_marker {s : State} (r : Reaches s) {k : Nat}
    (sendable : Sendable s k) (largest : ∀ k', Sendable s k' → k' ≤ k)
    {p : Nat × Nat} (hp : p ∈ s.ended) (held : p.1 ≤ k) {m : Nat}
    (cm : s.capture = some m) (between : m < p.2) : k = m := by
  have i := reaches_inv r
  have le := sendable.2.1 m cm
  have marked : m < s.next := by have := i.written p hp; omega
  have := largest m ⟨marked, fun m' cm' => by
      rw [cm] at cm'; simp only [Option.some.injEq] at cm'; omega,
    fun u w h q => by
      rcases i.apart p hp u w h q with le' | lt
      · have := sendable.2.2 u w h q; omega
      · omega⟩
  omega

/-- §17.4, §18.5: while a captured raise waits, the write that ended an upload
after its marker isn't published: the completion goes out after the raise. -/
theorem completion_after_raise {s : State} (r : Reaches s) {m : Nat}
    (cm : s.capture = some m) {p : Nat × Nat} (between : m < p.2) :
    s.published < p.2 := by
  have := (reaches_inv r).captured m cm
  omega

end CovenStorage.Holds
