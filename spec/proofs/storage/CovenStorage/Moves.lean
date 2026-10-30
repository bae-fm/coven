namespace CovenStorage.Moves

/-! §17.3: moves of one device's files to the cloud. A move is the files one
write stages for the cloud, or the files the app moves. Each file waits for
at most one move, through its upload. Storing an upload marks it stored; a
move switches every file still waiting for it to the cloud, in one write,
only once every one of their uploads is stored. A move that fails or is
cancelled switches none, and its files stop waiting for it. A file whose last
reference goes drops out of its move.

Proved here: no file of a move is in the cloud unless every file of the move
is (`move_whole`), in every state the steps reach. -/

/-- Where a move stands; `unused` for an id this device never gave a move. -/
inductive Phase where
  | unused | moving | failed | cancelled | finished
  deriving DecidableEq, Repr

/-- One device's moves, by id, and its files, by id: the move each waits
for, whether its upload is stored, and whether its coordinates say cloud. -/
structure State where
  phase : Nat → Phase
  member : Nat → Option Nat
  stored : Nat → Bool
  cloud : Nat → Bool

/-- What a device may do. `record` makes a new move of files waiting for no
move and not in the cloud. `elsewhere` is any switch of a file waiting for no
move, such as another audience's record or another device's write. -/
inductive Step : State → State → Prop where
  | record (s : State) (m : Nat) (files : Nat → Bool)
      (fresh : s.phase m = .unused)
      (free : ∀ f, files f = true → s.member f = none ∧ s.cloud f = false) :
      Step s { s with
        phase := fun x => if x = m then .moving else s.phase x
        member := fun f => if files f then some m else s.member f }
  | store (s : State) (f : Nat) :
      Step s { s with stored := fun x => if x = f then true else s.stored x }
  | drop (s : State) (f : Nat) :
      Step s { s with member := fun x => if x = f then none else s.member x }
  | stop (s : State) (m : Nat) (p : Phase) (moving : s.phase m = .moving)
      (ends : p = .failed ∨ p = .cancelled) :
      Step s { s with
        phase := fun x => if x = m then p else s.phase x
        member := fun f => if s.member f = some m then none else s.member f }
  | switch (s : State) (m : Nat) (moving : s.phase m = .moving)
      (ready : ∀ f, s.member f = some m → s.stored f = true) :
      Step s { s with
        phase := fun x => if x = m then .finished else s.phase x
        cloud := fun f => s.cloud f || decide (s.member f = some m) }
  | elsewhere (s : State) (f : Nat) (free : s.member f = none) (c : Bool) :
      Step s { s with cloud := fun x => if x = f then c else s.cloud x }

/-- A file waiting for a move is in the cloud exactly when its move
finished. -/
def Waiting (s : State) : Prop :=
  ∀ f m, s.member f = some m → (s.cloud f = true ↔ s.phase m = .finished)

/-- A move with no files waiting for it never finishes on its own, so it
names no phase but its own: every id a file waits for is a move. -/
def Known (s : State) : Prop := ∀ f m, s.member f = some m → s.phase m ≠ .unused

theorem step_known {s t : State} (st : Step s t) (h : Known s) : Known t := by
  cases st with
  | record m files fresh free =>
    intro f x hx
    simp only at hx ⊢
    by_cases hf : files f = true
    · simp [hf] at hx; subst hx; simp
    · simp [hf] at hx
      by_cases e : x = m
      · subst e; simp
      · simp [e]; exact h f x hx
  | store f => exact h
  | drop f =>
    intro x y hy
    simp only at hy ⊢
    by_cases e : x = f
    · simp [e] at hy
    · simp [e] at hy; exact h x y hy
  | stop m p moving ends =>
    intro f x hx
    simp only at hx ⊢
    by_cases hm : s.member f = some m
    · simp [hm] at hx
    · simp [hm] at hx
      by_cases e : x = m
      · subst e; rcases ends with e | e <;> simp [e]
      · simp [e]; exact h f x hx
  | switch m moving ready =>
    intro f x hx
    simp only at hx ⊢
    by_cases e : x = m
    · subst e; simp
    · simp [e]; exact h f x hx
  | elsewhere f free c => exact h

theorem step_waiting {s t : State} (st : Step s t) (hk : Known s) (h : Waiting s) :
    Waiting t := by
  cases st with
  | record m files fresh free =>
    intro f x hx
    simp only at hx ⊢
    by_cases hf : files f = true
    · simp [hf] at hx; subst hx
      simp [(free f hf).2]
    · simp [hf] at hx
      have hne : x ≠ m := by
        intro e; subst e; exact hk f x hx fresh
      simp [hne]; exact h f x hx
  | store f => exact h
  | drop f =>
    intro x y hy
    simp only at hy ⊢
    by_cases e : x = f
    · simp [e] at hy
    · simp [e] at hy; exact h x y hy
  | stop m p moving ends =>
    intro f x hx
    simp only at hx ⊢
    by_cases hm : s.member f = some m
    · simp [hm] at hx
    · simp [hm] at hx
      have hne : x ≠ m := by intro e; subst e; exact hm hx
      simp [hne]; exact h f x hx
  | switch m moving ready =>
    intro f x hx
    simp only at hx ⊢
    by_cases e : x = m
    · subst e; simp [hx]
    · have : s.member f ≠ some m := by rw [hx]; simpa using e
      simp [e, this]; exact h f x hx
  | elsewhere f free c =>
    intro x y hy
    simp only at hy ⊢
    by_cases e : x = f
    · subst e; rw [free] at hy; simp at hy
    · simp [e]; exact h x y hy

/-- The states a device reaches from one with no moves. -/
inductive Reaches : State → State → Prop where
  | refl (s : State) : Reaches s s
  | step {s t u : State} : Reaches s t → Step t u → Reaches s u

theorem reaches_invariant {s t : State} (r : Reaches s t) (hk : Known s) (h : Waiting s) :
    Known t ∧ Waiting t := by
  induction r with
  | refl => exact ⟨hk, h⟩
  | step _ st ih => exact ⟨step_known st ih.1, step_waiting st ih.1 ih.2⟩

/-- §17.3: no file of a move is in the cloud unless every file still waiting
for it is: from a state with no moves, in every reachable state, the files of
one move are all in the cloud or none is. -/
theorem move_whole {s t : State} (r : Reaches s t) (none : ∀ f, s.member f = none) (m : Nat) :
    (∀ f, t.member f = some m → t.cloud f = true) ∨
      (∀ f, t.member f = some m → t.cloud f = false) := by
  have hk : Known s := by intro f x hx; rw [none f] at hx; simp at hx
  have hw : Waiting s := by intro f x hx; rw [none f] at hx; simp at hx
  have ⟨_, h⟩ := reaches_invariant r hk hw
  by_cases e : t.phase m = .finished
  · left; intro f hf; exact (h f m hf).2 e
  · right; intro f hf
    cases hc : t.cloud f with
    | false => rfl
    | true => exact absurd ((h f m hf).1 hc) e

/-- A move switches only once every file waiting for it is stored. -/
theorem switch_needs_every_upload (s : State) (m f : Nat)
    (ready : ∀ g, s.member g = some m → s.stored g = true) (hf : s.member f = some m) :
    s.stored f = true := ready f hf

end CovenStorage.Moves
