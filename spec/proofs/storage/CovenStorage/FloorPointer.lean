import Std

/-! §16.4, §16.6: the floor is one small pointer record, replaced conditionally
in one request, naming the newest floor body. A body is created once at a name
of its writer's own, `floors/<device>/<n>`, and a device never uses a number
twice. The writer journals a floor write before it replaces the pointer; it
deletes a body only once the pointer no longer names it and no journaled write
names it; a device gone for good has its bodies cleared by another device.

A deleted name is empty again, so a delayed create can recreate a body there.
Nothing reads such a body: readers follow the pointer, and the pointer moves
only to a body its writer journaled. -/
namespace CovenStorage.FloorPointer

/-- A body's name: its writer and its number. -/
structure Name where
  owner : Nat
  number : Nat
  deriving DecidableEq, Repr

structure State where
  /-- The bodies in storage. -/
  bodies : List Name
  /-- The body the pointer names. -/
  pointer : Option Name
  /-- The pointer's revision, which every replacement advances. -/
  revision : Nat
  /-- Every name a device has used; none is used twice. -/
  used : List Name
  /-- The bodies a device's journaled floor write names. -/
  journaled : List Name
  /-- Devices gone for good. -/
  gone : List Nat
  deriving DecidableEq, Repr

def initial : State := ⟨[], none, 0, [], [], []⟩

inductive Step where
  /-- A device journals a floor write naming a body with a number it never used,
  then creates the body. -/
  | upload (b : Name)
  /-- A delayed create of a name the device used reaches storage. It lands only
  on an empty name. -/
  | lateCreate (b : Name)
  /-- Replace the pointer with a journaled body, only if the pointer still has
  the revision the writer read. -/
  | replace (b : Name) (observed : Nat)
  /-- The floor write settles: its journal entry ends. -/
  | settle (b : Name)
  /-- The body's own device deletes it. -/
  | deleteOwn (b : Name)
  /-- Another device clears a body of a device gone for good. -/
  | deleteGone (b : Name)
  /-- A device is gone for good. -/
  | markGone (d : Nat)
  deriving DecidableEq, Repr

def Step.apply (s : State) : Step → State
  | .upload b =>
    if b ∈ s.used then s
    else { s with bodies := b :: s.bodies, used := b :: s.used, journaled := b :: s.journaled }
  | .lateCreate b =>
    if b ∈ s.used ∧ b ∉ s.bodies then { s with bodies := b :: s.bodies } else s
  | .replace b observed =>
    if b ∈ s.journaled ∧ b ∈ s.bodies ∧ s.revision = observed then
      { s with pointer := some b, revision := s.revision + 1 }
    else s
  | .settle b => { s with journaled := s.journaled.erase b }
  | .deleteOwn b =>
    if s.pointer ≠ some b ∧ b ∉ s.journaled then { s with bodies := s.bodies.erase b } else s
  | .deleteGone b =>
    if b.owner ∈ s.gone ∧ s.pointer ≠ some b then { s with bodies := s.bodies.erase b } else s
  | .markGone d => { s with gone := d :: s.gone }

def runSteps (s : State) (steps : List Step) : State := steps.foldl Step.apply s

/-- What holds in every reachable state: the pointer names a body in storage,
and every journaled or stored body is a name some device used. -/
structure Good (s : State) : Prop where
  named : ∀ b, s.pointer = some b → b ∈ s.bodies
  journaled : ∀ b, b ∈ s.journaled → b ∈ s.used
  stored : ∀ b, b ∈ s.bodies → b ∈ s.used

theorem initial_good : Good initial := ⟨by simp [initial], by simp [initial], by simp [initial]⟩

theorem apply_good (s : State) (st : Step) (g : Good s) : Good (st.apply s) := by
  obtain ⟨nm, jn, sd⟩ := g
  cases st with
  | upload b =>
    simp only [Step.apply]
    split
    · exact ⟨nm, jn, sd⟩
    · refine ⟨?_, ?_, ?_⟩
      · intro c h; exact List.mem_cons_of_mem _ (nm c h)
      · intro c h
        rcases List.mem_cons.1 h with rfl | h
        · exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ (jn c h)
      · intro c h
        rcases List.mem_cons.1 h with rfl | h
        · exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ (sd c h)
  | lateCreate b =>
    simp only [Step.apply]
    split
    · rename_i h
      refine ⟨?_, jn, ?_⟩
      · intro c hc; exact List.mem_cons_of_mem _ (nm c hc)
      · intro c hc
        rcases List.mem_cons.1 hc with rfl | hc
        · exact h.1
        · exact sd c hc
    · exact ⟨nm, jn, sd⟩
  | replace b observed =>
    simp only [Step.apply]
    split
    · rename_i h
      refine ⟨?_, jn, sd⟩
      intro c hc; simp at hc; subst hc; exact h.2.1
    · exact ⟨nm, jn, sd⟩
  | settle b =>
    simp only [Step.apply]
    exact ⟨nm, fun c h => jn c (List.mem_of_mem_erase h), sd⟩
  | deleteOwn b =>
    simp only [Step.apply]
    split
    · rename_i h
      refine ⟨?_, jn, fun c hc => sd c (List.mem_of_mem_erase hc)⟩
      intro c hc
      have := nm c hc
      exact (List.mem_erase_of_ne (fun e => h.1 (by rw [hc, e]))).2 this
    · exact ⟨nm, jn, sd⟩
  | deleteGone b =>
    simp only [Step.apply]
    split
    · rename_i h
      refine ⟨?_, jn, fun c hc => sd c (List.mem_of_mem_erase hc)⟩
      intro c hc
      have := nm c hc
      exact (List.mem_erase_of_ne (fun e => h.2 (by rw [hc, e]))).2 this
    · exact ⟨nm, jn, sd⟩
  | markGone d =>
    exact ⟨nm, jn, sd⟩

theorem steps_good (s : State) (steps : List Step) (g : Good s) : Good (runSteps s steps) := by
  induction steps generalizing s with
  | nil => exact g
  | cons st rest ih => exact ih _ (apply_good s st g)

def Reachable (s : State) : Prop := ∃ steps, s = runSteps initial steps

theorem reachable_good (s : State) (h : Reachable s) : Good s := by
  obtain ⟨steps, rfl⟩ := h
  exact steps_good _ steps initial_good

/-- §16.4: the pointer names a body that exists, in every reachable state. -/
theorem pointer_names_published (s : State) (h : Reachable s) (b : Name)
    (named : s.pointer = some b) : b ∈ s.bodies :=
  (reachable_good s h).named b named

/-- No step removes the body the pointer names. -/
theorem named_body_kept (s : State) (st : Step) (b : Name) (g : Good s)
    (stays : (st.apply s).pointer = some b) : b ∈ (st.apply s).bodies :=
  (apply_good s st g).named b stays

/-- Deleting a body, its own or a gone device's, never moves the pointer. -/
theorem delete_keeps_pointer (s : State) (b : Name) :
    (Step.apply s (.deleteOwn b)).pointer = s.pointer ∧
      (Step.apply s (.deleteGone b)).pointer = s.pointer := by
  simp only [Step.apply]
  constructor <;> split <;> rfl

/-- A replacement refused because the pointer moved first leaves the pointer
and its revision as they were; its writer then deletes its own new body without
touching anything the pointer names. -/
theorem refused_replace_keeps_pointer (s : State) (b : Name) (observed : Nat)
    (moved : s.revision ≠ observed) :
    Step.apply s (.replace b observed) = s := by
  simp [Step.apply, moved]

/-- Two replacements under one observed revision cannot both move the pointer. -/
theorem one_pointer_winner (s : State) (b c : Name) (observed : Nat)
    (first : Step.apply s (.replace b observed) ≠ s) :
    Step.apply (Step.apply s (.replace b observed)) (.replace c observed) =
      Step.apply s (.replace b observed) := by
  by_cases won : b ∈ s.journaled ∧ b ∈ s.bodies ∧ s.revision = observed
  · have e : Step.apply s (.replace b observed) =
        { s with pointer := some b, revision := s.revision + 1 } := by
      simp [Step.apply, won]
    rw [e]
    have stale : ¬(c ∈ s.journaled ∧ c ∈ s.bodies ∧ s.revision + 1 = observed) := by
      intro h; have := won.2.2; omega
    simp [Step.apply, stale]
  · exact absurd (by simp [Step.apply, won]) first

/-- A delayed create of a name never moves the pointer: readers follow the
pointer, not names. -/
theorem late_create_unread (s : State) (b : Name) :
    (Step.apply s (.lateCreate b)).pointer = s.pointer := by
  simp only [Step.apply]; split <;> rfl

/-- The pointer moves only to a body its writer journaled and that exists, so
a recreated old body is never named: its journal entry ended before the body
was deleted. -/
theorem pointer_moves_to_journaled (s : State) (b : Name) (observed : Nat)
    (moved : (Step.apply s (.replace b observed)).pointer ≠ s.pointer) :
    b ∈ s.journaled ∧ b ∈ s.bodies := by
  by_cases h : b ∈ s.journaled ∧ b ∈ s.bodies ∧ s.revision = observed
  · exact ⟨h.1, h.2.1⟩
  · exact absurd (by simp [Step.apply, h]) moved

/-- A body its own device deleted had no journaled write and was not named. -/
theorem delete_own_unnamed (s : State) (b : Name)
    (deleted : b ∈ s.bodies ∧ b ∉ (Step.apply s (.deleteOwn b)).bodies) :
    s.pointer ≠ some b ∧ b ∉ s.journaled := by
  by_cases h : s.pointer ≠ some b ∧ b ∉ s.journaled
  · exact h
  · simp only [Step.apply, h, ite_false] at deleted
    exact absurd deleted.1 deleted.2

end CovenStorage.FloorPointer
