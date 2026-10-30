namespace CovenStorage.DeviceOnly

/-! §17.3, Appendix C.3: the copies coven writes into a folder the person
picked, to make cloud files device-only. Each copy is recorded before any of
its bytes exist. A call places its copies, then one write switches them all,
keeping each as an original. A call that fails or is cancelled before that
write removes what it made, and a crash leaves it to the next open. A kept
copy whose switch lost to another device's is removed too. A removal takes a
copy only while it is as coven left it.

Proved here: removing an unchanged copy coven placed leaves nothing at its
path (`removal_takes_unchanged`); no step of coven's takes away a file the
person wrote or changed (`person_files_stay`), nor a kept copy
(`kept_stays`); and a
call's switch keeps every one of its copies, all placed before it
(`switch_whole`). -/

/-- Where a copy's record stands. -/
inductive Phase where
  | writing | placed | kept | lost
  deriving DecidableEq, Repr

/-- One device's paths in the person's folders: each copy's record, if one
is recorded; whether a file is at the path; whether coven wrote what is
there; and whether the person changed it since. -/
structure State where
  phase : Nat → Option Phase
  present : Nat → Bool
  written : Nat → Bool
  changed : Nat → Bool

/-- Whether a removal takes a copy in this phase: one a call left
unswitched, or one whose switch lost. Never a kept one. -/
def Phase.removable : Phase → Bool
  | .writing | .placed | .lost => true
  | .kept => false

/-- What coven does at the paths. `record` records a call's copies before
any of their bytes exist; `place` renames one, whole and synced, to its path;
`switch` is the one write keeping every copy of a call; `lose` is another
device's switch winning; `forget` is a kept copy's original record going,
which leaves the file; `remove` is a removal. Between steps the person writes
and changes files as they like, which `written` and `changed` record. -/
inductive Step : State → State → Prop where
  | record (s : State) (paths : Nat → Bool)
      (free : ∀ p, paths p = true → s.phase p = none ∧ s.present p = false) :
      Step s { s with phase := fun p => if paths p then some .writing else s.phase p }
  | place (s : State) (p : Nat) (writing : s.phase p = some .writing)
      (empty : s.present p = false) :
      Step s { s with
        phase := fun x => if x = p then some .placed else s.phase x
        present := fun x => if x = p then true else s.present x
        written := fun x => if x = p then true else s.written x
        changed := fun x => if x = p then false else s.changed x }
  | switch (s : State) (paths : Nat → Bool)
      (placed : ∀ p, paths p = true → s.phase p = some .placed) :
      Step s { s with phase := fun p => if paths p then some .kept else s.phase p }
  | lose (s : State) (p : Nat) (kept : s.phase p = some .kept) :
      Step s { s with phase := fun x => if x = p then some .lost else s.phase x }
  | forget (s : State) (p : Nat) (kept : s.phase p = some .kept) :
      Step s { s with phase := fun x => if x = p then none else s.phase x }
  | remove (s : State) (p : Nat) (phase : Phase) (recorded : s.phase p = some phase)
      (removable : phase.removable = true) :
      Step s { s with
        phase := fun x => if x = p then none else s.phase x
        present := fun x =>
          if x = p then s.present x && !(s.written x && !s.changed x) else s.present x }

/-- Removing a copy coven placed, as it left it, leaves nothing at its path. -/
theorem removal_takes_unchanged (s : State) (p : Nat)
    (written : s.written p = true) (unchanged : s.changed p = false) :
    ({ s with
        phase := fun x => if x = p then none else s.phase x
        present := fun x =>
          if x = p then s.present x && !(s.written x && !s.changed x) else s.present x } :
        State).present p = false := by
  simp [written, unchanged]

/-- No step of coven's takes away a file the person wrote or changed. -/
theorem person_files_stay {s t : State} (st : Step s t) (p : Nat) (there : s.present p = true)
    (theirs : s.written p = false ∨ s.changed p = true) : t.present p = true := by
  cases st with
  | record paths free => exact there
  | place q writing empty =>
    simp only
    by_cases e : p = q
    · simp [e]
    · simp [e, there]
  | switch paths placed => exact there
  | lose q kept => exact there
  | forget q kept => exact there
  | remove q phase recorded removable =>
    simp only
    by_cases e : p = q
    · subst e
      rcases theirs with h | h <;> simp [there, h]
    · simp [e, there]

/-- A kept copy is an original this device records: no removal takes it
while it is kept, whatever coven wrote there. -/
theorem kept_stays {s t : State} (st : Step s t) (p : Nat) (kept : s.phase p = some .kept)
    (there : s.present p = true) : t.present p = true := by
  cases st with
  | record paths free => exact there
  | place q writing empty =>
    simp only
    by_cases e : p = q
    · simp [e]
    · simp [e, there]
  | switch paths placed => exact there
  | lose q kept' => exact there
  | forget q kept' => exact there
  | remove q phase recorded removable =>
    simp only
    by_cases e : p = q
    · subst e
      rw [kept] at recorded
      cases recorded
      simp [Phase.removable] at removable
    · simp [e, there]

/-- A call's switch keeps every one of its copies, each placed before it, in
one step: none is kept before the switch unless it was already. -/
theorem switch_whole (s : State) (paths : Nat → Bool)
    (placed : ∀ p, paths p = true → s.phase p = some .placed) (p : Nat) (h : paths p = true) :
    s.phase p = some .placed ∧
      ({ s with phase := fun q => if paths q then some .kept else s.phase q } : State).phase p
        = some .kept := by
  exact ⟨placed p h, by simp [h]⟩

end CovenStorage.DeviceOnly
