namespace CovenStorage.Moves

/-! §17.3: moves of one device's files to the cloud. A move is the files one
write stages for the cloud, or the files the app moves. Each file waits for
at most one move, through its upload. Storing an upload marks it stored; a
move switches every file still waiting for it to the cloud, in one write,
only once every one of their uploads is stored. A move that fails or is
cancelled switches none, and its files stop waiting for it. A file whose last
reference goes drops out of its move.

Each move also lists its files, for the app to show what a move holds: the
list is written in the write that records the move, and a file that comes to
wait for a moving move, as a new file over a waiting file's bytes does, joins
it. A failure ends only the waiting, so a failed move still lists the files
the app can cancel it for; a switch or a cancel ends the list with the move.
A file is in at most one move: one a failed move still lists can't be moved
again until the app cancels that move.

Proved here: no file of a move is in the cloud unless every file of the move
is (`move_whole`), in every state the steps reach. In every such state only a
moving or failed move lists files, and every file waiting for a moving move
is listed under it (`list_while_live`); failing a move keeps every listing and
leaves no file waiting for it (`fail_keeps_list`). -/

/-- Where a move stands; `unused` for an id this device never gave a move. -/
inductive Phase where
  | unused | moving | failed | cancelled | finished
  deriving DecidableEq, Repr

/-- One device's moves, by id, and its files, by id: the move each waits
for, whether its upload is stored, whether its coordinates say cloud, and
the move that lists it, at most one. -/
structure State where
  phase : Nat → Phase
  member : Nat → Option Nat
  stored : Nat → Bool
  cloud : Nat → Bool
  listed : Nat → Option Nat

/-- What a device may do. `record` makes a new move of files in no move,
waiting for none and not in the cloud, listing them. `join` has such a file
wait for a moving move and be listed under it.
`fail` stops a moving move and keeps its list; `cancel` stops a moving or
failed move and ends its list, as `switch` does. `elsewhere` is any switch of
a file waiting for no move, such as another audience's record or another
device's write. -/
inductive Step : State → State → Prop where
  | record (s : State) (m : Nat) (files : Nat → Bool)
      (fresh : s.phase m = .unused)
      (free : ∀ f, files f = true →
        s.member f = none ∧ s.cloud f = false ∧ s.listed f = none) :
      Step s { s with
        phase := fun x => if x = m then .moving else s.phase x
        member := fun f => if files f then some m else s.member f
        listed := fun f => if files f then some m else s.listed f }
  | join (s : State) (m f : Nat) (moving : s.phase m = .moving)
      (free : s.member f = none ∧ s.cloud f = false ∧ s.listed f = none) :
      Step s { s with
        member := fun x => if x = f then some m else s.member x
        listed := fun x => if x = f then some m else s.listed x }
  | store (s : State) (f : Nat) :
      Step s { s with stored := fun x => if x = f then true else s.stored x }
  | drop (s : State) (f : Nat) :
      Step s { s with
        member := fun x => if x = f then none else s.member x
        listed := fun x => if x = f then none else s.listed x }
  | fail (s : State) (m : Nat) (moving : s.phase m = .moving) :
      Step s { s with
        phase := fun x => if x = m then .failed else s.phase x
        member := fun f => if s.member f = some m then none else s.member f }
  | cancel (s : State) (m : Nat) (live : s.phase m = .moving ∨ s.phase m = .failed) :
      Step s { s with
        phase := fun x => if x = m then .cancelled else s.phase x
        member := fun f => if s.member f = some m then none else s.member f
        listed := fun f => if s.listed f = some m then none else s.listed f }
  | switch (s : State) (m : Nat) (moving : s.phase m = .moving)
      (ready : ∀ f, s.member f = some m → s.stored f = true) :
      Step s { s with
        phase := fun x => if x = m then .finished else s.phase x
        cloud := fun f => s.cloud f || decide (s.member f = some m)
        listed := fun f => if s.listed f = some m then none else s.listed f }
  | elsewhere (s : State) (f : Nat) (free : s.member f = none) (c : Bool) :
      Step s { s with cloud := fun x => if x = f then c else s.cloud x }

/-- A file waiting for a move is in the cloud exactly when its move
finished. -/
def Waiting (s : State) : Prop :=
  ∀ f m, s.member f = some m → (s.cloud f = true ↔ s.phase m = .finished)

/-- A move with no files waiting for it never finishes on its own, so it
names no phase but its own: every id a file waits for is a move. -/
def Known (s : State) : Prop := ∀ f m, s.member f = some m → s.phase m ≠ .unused

/-- Only a moving or failed move lists files. -/
def Listed (s : State) : Prop :=
  ∀ f m, s.listed f = some m → s.phase m = .moving ∨ s.phase m = .failed

/-- Every file waiting for a moving move is listed under it. -/
def WaitingListed (s : State) : Prop :=
  ∀ f m, s.member f = some m → s.phase m = .moving → s.listed f = some m

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
  | join m g moving free =>
    intro f x hx
    simp only at hx ⊢
    by_cases e : f = g
    · simp [e] at hx; subst hx; rw [moving]; simp
    · simp [e] at hx; exact h f x hx
  | store f => exact h
  | drop f =>
    intro x y hy
    simp only at hy ⊢
    by_cases e : x = f
    · simp [e] at hy
    · simp [e] at hy; exact h x y hy
  | fail m moving =>
    intro f x hx
    simp only at hx ⊢
    by_cases hm : s.member f = some m
    · simp [hm] at hx
    · simp [hm] at hx
      by_cases e : x = m
      · subst e; simp
      · simp [e]; exact h f x hx
  | cancel m live =>
    intro f x hx
    simp only at hx ⊢
    by_cases hm : s.member f = some m
    · simp [hm] at hx
    · simp [hm] at hx
      by_cases e : x = m
      · subst e; simp
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
      simp [(free f hf).2.1]
    · simp [hf] at hx
      have hne : x ≠ m := by
        intro e; subst e; exact hk f x hx fresh
      simp [hne]; exact h f x hx
  | join m g moving free =>
    intro f x hx
    simp only at hx ⊢
    by_cases e : f = g
    · simp [e] at hx; subst hx; subst e
      simp [free.2, moving]
    · simp [e] at hx; exact h f x hx
  | store f => exact h
  | drop f =>
    intro x y hy
    simp only at hy ⊢
    by_cases e : x = f
    · simp [e] at hy
    · simp [e] at hy; exact h x y hy
  | fail m moving =>
    intro f x hx
    simp only at hx ⊢
    by_cases hm : s.member f = some m
    · simp [hm] at hx
    · simp [hm] at hx
      have hne : x ≠ m := by intro e; subst e; exact hm hx
      simp [hne]; exact h f x hx
  | cancel m live =>
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

theorem step_listed {s t : State} (st : Step s t) (h : Listed s) : Listed t := by
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
  | join m g moving free =>
    intro f x hx
    simp only at hx ⊢
    by_cases e : f = g
    · simp [e] at hx; subst hx; simp [moving]
    · simp [e] at hx; exact h f x hx
  | store f => exact h
  | drop g =>
    intro f x hx
    simp only at hx ⊢
    by_cases e : f = g
    · simp [e] at hx
    · simp [e] at hx; exact h f x hx
  | fail m moving =>
    intro f x hx
    simp only at hx ⊢
    by_cases e : x = m
    · subst e; simp
    · simp [e]; exact h f x hx
  | cancel m live =>
    intro f x hx
    simp only at hx ⊢
    by_cases hl : s.listed f = some m
    · simp [hl] at hx
    · simp [hl] at hx
      have hne : x ≠ m := by intro e; subst e; exact hl hx
      simp [hne]; exact h f x hx
  | switch m moving ready =>
    intro f x hx
    simp only at hx ⊢
    by_cases hl : s.listed f = some m
    · simp [hl] at hx
    · simp [hl] at hx
      have hne : x ≠ m := by intro e; subst e; exact hl hx
      simp [hne]; exact h f x hx
  | elsewhere f free c => exact h

theorem step_waiting_listed {s t : State} (st : Step s t) (hk : Known s)
    (h : WaitingListed s) : WaitingListed t := by
  cases st with
  | record m files fresh free =>
    intro f x hx hm
    simp only at hx hm ⊢
    by_cases hf : files f = true
    · simp [hf] at hx; subst hx; simp [hf]
    · simp [hf] at hx ⊢
      have hne : x ≠ m := by
        intro e; subst e; exact hk f x hx fresh
      simp [hne] at hm; exact h f x hx hm
  | join m g moving free =>
    intro f x hx hm
    simp only at hx hm ⊢
    by_cases e : f = g
    · simp [e] at hx ⊢; exact hx
    · simp [e] at hx ⊢; exact h f x hx hm
  | store f => exact h
  | drop g =>
    intro f x hx hm
    simp only at hx hm ⊢
    by_cases e : f = g
    · simp [e] at hx
    · simp [e] at hx ⊢; exact h f x hx hm
  | fail m moving =>
    intro f x hx hm
    simp only at hx hm ⊢
    by_cases hw : s.member f = some m
    · simp [hw] at hx
    · simp [hw] at hx
      have hne : x ≠ m := by intro e; subst e; exact hw hx
      simp [hne] at hm; exact h f x hx hm
  | cancel m live =>
    intro f x hx hm
    simp only at hx hm ⊢
    by_cases hw : s.member f = some m
    · simp [hw] at hx
    · simp [hw] at hx
      have hne : x ≠ m := by intro e; subst e; exact hw hx
      simp [hne] at hm
      have hl := h f x hx hm
      simp [hl, hne]
  | switch m moving ready =>
    intro f x hx hm
    simp only at hx hm ⊢
    by_cases e : x = m
    · simp [e] at hm
    · simp [e] at hm
      have hl := h f x hx hm
      simp [hl, e]
  | elsewhere f free c => exact h

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

theorem reaches_listed {s t : State} (r : Reaches s t) (hk : Known s) (hl : Listed s)
    (hw : WaitingListed s) : Known t ∧ Listed t ∧ WaitingListed t := by
  induction r with
  | refl => exact ⟨hk, hl, hw⟩
  | step _ st ih =>
    exact ⟨step_known st ih.1, step_listed st ih.2.1, step_waiting_listed st ih.1 ih.2.2⟩

/-- §17.3: a move's list lives exactly as long as the move. From a state with
no moves, in every reachable state, only a moving or failed move lists files,
so a switched or cancelled one lists none, and every file waiting for a moving
move is listed under it, from the write that records the move on. A file is
listed under at most one move by the state's shape. -/
theorem list_while_live {s t : State} (r : Reaches s t) (idle : ∀ f, s.member f = none)
    (empty : ∀ f, s.listed f = none) :
    (∀ f m, t.listed f = some m → t.phase m = .moving ∨ t.phase m = .failed) ∧
      (∀ f m, t.member f = some m → t.phase m = .moving → t.listed f = some m) := by
  have hk : Known s := by intro f x hx; rw [idle f] at hx; simp at hx
  have hl : Listed s := by intro f m hm; rw [empty f] at hm; simp at hm
  have hw : WaitingListed s := by intro f x hx; rw [idle f] at hx; simp at hx
  have ⟨_, h, w⟩ := reaches_listed r hk hl hw
  exact ⟨h, w⟩

/-- §17.3: failing a move ends only the waiting. The step that fails moving
move `m` keeps every file's listing as it was, and leaves no file waiting for
`m`, so the failed move lists the files the app can cancel it for. Each stays
in it: neither `record` nor `join` takes a listed file. -/
theorem fail_keeps_list {s t : State} (st : Step s t) (m : Nat) (was : s.phase m = .moving)
    (now : t.phase m = .failed) : t.listed = s.listed ∧ ∀ f, t.member f ≠ some m := by
  cases st with
  | record x files fresh free =>
    simp only at now
    by_cases e : m = x
    · simp [e] at now
    · simp [e, was] at now
  | join x g moving free => simp [was] at now
  | store f => simp [was] at now
  | drop f => simp [was] at now
  | fail x moving =>
    simp only at now
    by_cases e : m = x
    · subst e
      refine ⟨rfl, fun f => ?_⟩
      show (if s.member f = some m then none else s.member f) ≠ some m
      by_cases hw : s.member f = some m <;> simp [hw]
    · simp [e, was] at now
  | cancel x live =>
    simp only at now
    by_cases e : m = x
    · simp [e] at now
    · simp [e, was] at now
  | switch x moving ready =>
    simp only at now
    by_cases e : m = x
    · simp [e] at now
    · simp [e, was] at now
  | elsewhere f free c => simp [was] at now

end CovenStorage.Moves
