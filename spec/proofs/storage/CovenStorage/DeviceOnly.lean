namespace CovenStorage.DeviceOnly

/-! §17.3, Appendix C.3: the copies coven writes into a folder the person
picked, to make cloud files device-only, and the folders it makes for them.
Each copy and folder is recorded before any of it exists, and a running call
reserves its copies. A call places its copies, then one write switches them
all, keeping each as an original. A call that fails or is cancelled before
that write abandons what it made; once its reservation drops, as when its
future is dropped, the store's next write takes it, and after a crash the
next open. A kept copy whose switch lost to another device's is taken too.
A removal takes a copy only while it is as coven left it, and a folder only
when the call's own mkdir made it and it is empty.

Proved here: removing an unchanged copy coven placed leaves nothing at its
path (`removal_takes_unchanged`); no step of coven's takes away a file the
person wrote or changed (`person_files_stay`, `abandon_person_files_stay`),
nor a kept copy (`kept_stays`, `abandon_kept_stays`); a call's switch keeps
every one of its copies, all placed before it (`switch_whole`). A folder
counts as made only in the step whose mkdir found nothing there
(`made_only_by_mkdir`), and only an empty made folder no recorded copy needs
is ever taken away (`folder_removal_made_empty`), so a folder that was
already there stays (`found_folder_stays`), and so does one a recorded copy
needs (`needed_folder_stays`). While a call reserves a copy, no step but its
own abandon takes the copy's record, its file or its folder links
(`reserved_stays`); once the reservation drops, the next write can take it
(`released_removable`). -/

/-- Where a copy's record stands. -/
inductive Phase where
  | writing | placed | kept | lost
  deriving DecidableEq, Repr

/-- One device's paths in the person's folders. For each copy: its record,
if one is recorded; whether a file is at its path; whether coven wrote what
is there; whether the person changed it since; and whether a running call
reserves it. For each folder: its record, if one is recorded, with whether
the call's own mkdir made it; whether a folder is at its path; and whether
it holds anything. `needs p f` records that copy `p` lies in folder `f`; it
goes with the copy's record. -/
structure State where
  phase : Nat → Option Phase
  present : Nat → Bool
  written : Nat → Bool
  changed : Nat → Bool
  reserved : Nat → Bool
  folder : Nat → Option Bool
  there : Nat → Bool
  holds : Nat → Bool
  needs : Nat → Nat → Bool

/-- Whether a removal takes a copy in this phase: one a call left
unswitched, or one whose switch lost. Never a kept one. -/
def Phase.removable : Phase → Bool
  | .writing | .placed | .lost => true
  | .kept => false

/-- A copy's record going with its folder links, and the file at its path
with them only if coven wrote it and the person didn't change it. -/
def State.take (s : State) (p : Nat) : State :=
  { s with
    phase := fun x => if x = p then none else s.phase x
    present := fun x =>
      if x = p then s.present x && !(s.written x && !s.changed x) else s.present x
    needs := fun x f => if x = p then false else s.needs x f }

/-- What coven does at the paths, but for a call abandoning its own copies.
`record` records a call's copies, the folders it plans to make and which
copy lies in which, before any of them exists, and reserves the copies;
`mkdir` is the call's mkdir finding nothing at a recorded folder's path,
making the folder and marking it made; `place` renames a copy, whole and
synced, to its path; `switch` is the one write keeping every copy of a call;
`release` is a call's reservation dropping, however the call ended, and
`crash` the process ending, which drops them all; `lose` is another
device's switch winning; `forget` is a kept copy's original record going,
which leaves the file; `sweep` is the store's next write, or opening,
taking a copy no running call reserves, or one that lost; `sweepFolder`
takes a folder the call's mkdir made once no recorded copy needs it, if it
is empty. Between steps the person writes, changes and removes files and
folders as they like, which `written`, `changed`, `there` and `holds`
record. -/
inductive Step : State → State → Prop where
  | record (s : State) (paths folders : Nat → Bool) (links : Nat → Nat → Bool)
      (free : ∀ p, paths p = true → s.phase p = none ∧ s.present p = false)
      (linked : ∀ p f, links p f = true → paths p = true ∧ folders f = true) :
      Step s { s with
        phase := fun p => if paths p then some .writing else s.phase p
        reserved := fun p => s.reserved p || paths p
        folder := fun f => if folders f && (s.folder f).isNone then some false else s.folder f
        needs := fun p f => s.needs p f || links p f }
  | mkdir (s : State) (f : Nat) (recorded : s.folder f ≠ none) (absent : s.there f = false) :
      Step s { s with
        folder := fun x => if x = f then some true else s.folder x
        there := fun x => if x = f then true else s.there x
        holds := fun x => if x = f then false else s.holds x }
  | place (s : State) (p : Nat) (writing : s.phase p = some .writing)
      (empty : s.present p = false) :
      Step s { s with
        phase := fun x => if x = p then some .placed else s.phase x
        present := fun x => if x = p then true else s.present x
        written := fun x => if x = p then true else s.written x
        changed := fun x => if x = p then false else s.changed x
        holds := fun f => s.holds f || s.needs p f }
  | switch (s : State) (paths : Nat → Bool)
      (placed : ∀ p, paths p = true → s.phase p = some .placed) :
      Step s { s with phase := fun p => if paths p then some .kept else s.phase p }
  | release (s : State) (paths : Nat → Bool) :
      Step s { s with reserved := fun p => s.reserved p && !paths p }
  | crash (s : State) : Step s { s with reserved := fun _ => false }
  | lose (s : State) (p : Nat) (kept : s.phase p = some .kept) :
      Step s { s with phase := fun x => if x = p then some .lost else s.phase x }
  | forget (s : State) (p : Nat) (kept : s.phase p = some .kept) :
      Step s { s with
        phase := fun x => if x = p then none else s.phase x
        needs := fun x f => if x = p then false else s.needs x f }
  | sweep (s : State) (p : Nat) (phase : Phase) (recorded : s.phase p = some phase)
      (removable : phase.removable = true)
      (unreserved : s.reserved p = false ∨ phase = .lost) :
      Step s (s.take p)
  | sweepFolder (s : State) (f : Nat) (made : s.folder f = some true)
      (unneeded : ∀ p, s.needs p f = false) :
      Step s { s with
        folder := fun x => if x = f then none else s.folder x
        there := fun x => if x = f then s.there x && s.holds x else s.there x }

/-- A call failing or cancelled before its switch abandons a copy it still
reserves: one being written, or placed and waiting for the switch. -/
inductive Abandon : State → State → Prop where
  | abandon (s : State) (p : Nat) (phase : Phase) (recorded : s.phase p = some phase)
      (own : s.reserved p = true) (unswitched : phase = .writing ∨ phase = .placed) :
      Abandon s (s.take p)

/-- Removing a copy coven placed, as it left it, leaves nothing at its path. -/
theorem removal_takes_unchanged (s : State) (p : Nat)
    (written : s.written p = true) (unchanged : s.changed p = false) :
    (s.take p).present p = false := by
  simp [State.take, written, unchanged]

/-- Taking a copy's record never takes away a file at another path, nor
one the person wrote or changed at its own. -/
theorem take_keeps_theirs (s : State) (q p : Nat) (there : s.present p = true)
    (theirs : q ≠ p ∨ s.written p = false ∨ s.changed p = true) :
    (s.take q).present p = true := by
  simp only [State.take]
  by_cases e : p = q
  · subst e
    rcases theirs with h | h | h
    · exact absurd rfl h
    · simp [there, h]
    · simp [there, h]
  · simp [e, there]

/-- No step of coven's takes away a file the person wrote or changed. -/
theorem person_files_stay {s t : State} (st : Step s t) (p : Nat) (there : s.present p = true)
    (theirs : s.written p = false ∨ s.changed p = true) : t.present p = true := by
  cases st with
  | record paths folders links free linked => exact there
  | mkdir f recorded absent => exact there
  | place q writing empty =>
    simp only
    by_cases e : p = q
    · simp [e]
    · simp [e, there]
  | switch paths placed => exact there
  | release paths => exact there
  | crash => exact there
  | lose q kept => exact there
  | forget q kept => exact there
  | sweep q phase recorded removable unreserved =>
    by_cases e : q = p
    · subst e; exact take_keeps_theirs s q q there (Or.inr theirs)
    · exact take_keeps_theirs s q p there (Or.inl e)
  | sweepFolder f made unneeded => exact there

/-- Nor does a call abandoning its own copies. -/
theorem abandon_person_files_stay {s t : State} (st : Abandon s t) (p : Nat)
    (there : s.present p = true) (theirs : s.written p = false ∨ s.changed p = true) :
    t.present p = true := by
  cases st with
  | abandon q phase recorded own unswitched =>
    by_cases e : q = p
    · subst e; exact take_keeps_theirs s q q there (Or.inr theirs)
    · exact take_keeps_theirs s q p there (Or.inl e)

/-- A kept copy is an original this device records: no removal takes it
while it is kept, whatever coven wrote there. -/
theorem kept_stays {s t : State} (st : Step s t) (p : Nat) (kept : s.phase p = some .kept)
    (there : s.present p = true) : t.present p = true := by
  cases st with
  | record paths folders links free linked => exact there
  | mkdir f recorded absent => exact there
  | place q writing empty =>
    simp only
    by_cases e : p = q
    · simp [e]
    · simp [e, there]
  | switch paths placed => exact there
  | release paths => exact there
  | crash => exact there
  | lose q kept' => exact there
  | forget q kept' => exact there
  | sweep q phase recorded removable unreserved =>
    by_cases e : q = p
    · subst e
      rw [kept] at recorded
      cases recorded
      simp [Phase.removable] at removable
    · exact take_keeps_theirs s q p there (Or.inl e)
  | sweepFolder f made unneeded => exact there

/-- Nor does a call's abandon: it takes only copies its switch didn't keep. -/
theorem abandon_kept_stays {s t : State} (st : Abandon s t) (p : Nat)
    (kept : s.phase p = some .kept) (there : s.present p = true) : t.present p = true := by
  cases st with
  | abandon q phase recorded own unswitched =>
    by_cases e : q = p
    · subst e
      rw [kept] at recorded
      cases recorded
      rcases unswitched with h | h <;> cases h
    · exact take_keeps_theirs s q p there (Or.inl e)

/-- A call's switch keeps every one of its copies, each placed before it, in
one step: none is kept before the switch unless it was already. -/
theorem switch_whole (s : State) (paths : Nat → Bool)
    (placed : ∀ p, paths p = true → s.phase p = some .placed) (p : Nat) (h : paths p = true) :
    s.phase p = some .placed ∧
      ({ s with phase := fun q => if paths q then some .kept else s.phase q } : State).phase p
        = some .kept := by
  exact ⟨placed p h, by simp [h]⟩

/-- A folder counts as made only in the step whose mkdir found nothing at
its path and made it: one that was there, or that the person made before
the mkdir, is never marked. -/
theorem made_only_by_mkdir {s t : State} (st : Step s t) (f : Nat)
    (before : s.folder f ≠ some true) (after : t.folder f = some true) :
    s.there f = false ∧ t.there f = true := by
  cases st with
  | record paths folders links free linked =>
    simp only at after
    by_cases h : (folders f && (s.folder f).isNone) = true
    · simp [h] at after
    · simp [h] at after; exact absurd after before
  | mkdir g recorded absent =>
    simp only at after ⊢
    by_cases e : f = g
    · subst e; exact ⟨absent, by simp⟩
    · simp [e] at after; exact absurd after before
  | place q writing empty => exact absurd after before
  | switch paths placed => exact absurd after before
  | release paths => exact absurd after before
  | crash => exact absurd after before
  | lose q kept => exact absurd after before
  | forget q kept => exact absurd after before
  | sweep q phase recorded removable unreserved => exact absurd after before
  | sweepFolder g made unneeded =>
    simp only at after
    by_cases e : f = g
    · simp [e] at after
    · simp [e] at after; exact absurd after before

/-- Only an empty folder the call's mkdir made, which no recorded copy
needs, is ever taken away. -/
theorem folder_removal_made_empty {s t : State} (st : Step s t) (f : Nat)
    (there : s.there f = true) (gone : t.there f = false) :
    s.folder f = some true ∧ s.holds f = false ∧ ∀ p, s.needs p f = false := by
  cases st with
  | record paths folders links free linked => simp [there] at gone
  | mkdir g recorded absent =>
    simp only at gone
    by_cases e : f = g
    · simp [e] at gone
    · simp [e, there] at gone
  | place q writing empty => simp [there] at gone
  | switch paths placed => simp [there] at gone
  | release paths => simp [there] at gone
  | crash => simp [there] at gone
  | lose q kept => simp [there] at gone
  | forget q kept => simp [there] at gone
  | sweep q phase recorded removable unreserved => simp [State.take, there] at gone
  | sweepFolder g made unneeded =>
    simp only at gone
    by_cases e : f = g
    · subst e
      simp [there] at gone
      exact ⟨made, gone, unneeded⟩
    · simp [e, there] at gone

/-- A folder the call's mkdir didn't make stays, whatever coven does. -/
theorem found_folder_stays {s t : State} (st : Step s t) (f : Nat)
    (there : s.there f = true) (found : s.folder f ≠ some true) : t.there f = true := by
  cases h : t.there f
  · exact absurd (folder_removal_made_empty st f there h).1 found
  · rfl

/-- So does a folder a recorded copy lies in. -/
theorem needed_folder_stays {s t : State} (st : Step s t) (p f : Nat)
    (needed : s.needs p f = true) (there : s.there f = true) : t.there f = true := by
  cases h : t.there f
  · have := (folder_removal_made_empty st f there h).2.2 p
    rw [needed] at this
    cases this
  · rfl

/-- While a call reserves a copy it hasn't switched, no step but its own
abandon takes the copy's record, the file at its path, or its folder
links, which keep its folders (`needed_folder_stays`). -/
theorem reserved_stays {s t : State} (st : Step s t) (p : Nat) (own : s.reserved p = true)
    (unswitched : s.phase p = some .writing ∨ s.phase p = some .placed) :
    t.phase p ≠ none ∧ (s.present p = true → t.present p = true) ∧
      (∀ f, s.needs p f = true → t.needs p f = true) := by
  have recorded : s.phase p ≠ none := by
    rcases unswitched with h | h <;> simp [h]
  cases st with
  | record paths folders links free linked =>
    refine ⟨?_, id, fun f h => by simp [h]⟩
    simp only
    by_cases h : paths p = true
    · simp [h]
    · simp [h, recorded]
  | mkdir g r absent => exact ⟨recorded, id, fun _ h => h⟩
  | place q writing empty =>
    refine ⟨?_, ?_, fun _ h => h⟩
    · simp only
      by_cases e : p = q
      · simp [e]
      · simp [e, recorded]
    · intro there
      simp only
      by_cases e : p = q
      · simp [e]
      · simp [e, there]
  | switch paths placed =>
    refine ⟨?_, id, fun _ h => h⟩
    simp only
    by_cases h : paths p = true
    · simp [h]
    · simp [h, recorded]
  | release paths => exact ⟨recorded, id, fun _ h => h⟩
  | crash => exact ⟨recorded, id, fun _ h => h⟩
  | lose q kept =>
    refine ⟨?_, id, fun _ h => h⟩
    simp only
    by_cases e : p = q
    · simp [e]
    · simp [e, recorded]
  | forget q kept =>
    by_cases e : p = q
    · subst e; rcases unswitched with h | h <;> rw [h] at kept <;> cases kept
    · exact ⟨by simp [e, recorded], id, fun f h => by simp [e, h]⟩
  | sweep q phase r removable unreserved =>
    by_cases e : q = p
    · subst e
      rcases unreserved with h | h
      · rw [own] at h; cases h
      · subst h; rcases unswitched with u | u <;> rw [u] at r <;> cases r
    · refine ⟨?_, fun there => take_keeps_theirs s q p there (Or.inl e), ?_⟩
      · simp [State.take, Ne.symm e, recorded]
      · intro f h; simp [State.take, Ne.symm e, h]
  | sweepFolder g made unneeded => exact ⟨recorded, id, fun _ h => h⟩

/-- Once the reservation drops, the store's next write can take a copy its
call left unswitched. -/
theorem released_removable (s : State) (p : Nat) (released : s.reserved p = false)
    (unswitched : s.phase p = some .writing ∨ s.phase p = some .placed) :
    ∃ t, Step s t ∧ t.phase p = none := by
  rcases unswitched with h | h
  · exact ⟨s.take p, Step.sweep s p .writing h rfl (Or.inl released), by simp [State.take]⟩
  · exact ⟨s.take p, Step.sweep s p .placed h rfl (Or.inl released), by simp [State.take]⟩

end CovenStorage.DeviceOnly
