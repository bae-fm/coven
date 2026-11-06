namespace CovenStorage.LocalBytes

/-! Appendix C.3: a device's own file bytes (staged app files, cache files,
pins) and the rows that name them, across crashes. A new file's name is
recorded for cleanup before the file exists. Its bytes are made durable before
a row naming them commits, and that commit clears the cleanup record. A
transaction releasing a row records its bytes for deletion in the same commit;
the bytes go after it, and then the record. A deletion is recorded only for
bytes no row names, and no row names bytes recorded for deletion: the database
refuses either. A crash loses bytes not yet durable, and nothing else.

Proved here: in every reachable state no row names bytes that aren't durable on
disk (`rows_durable`), every file on disk is named by a row or recorded for
cleanup, so some later deletion finds it (`bytes_findable`), and a deletion
never takes bytes a row names (`delete_spares_rows`). Two counterexamples show
why the order matters: a row committed before its bytes are durable names
missing bytes after a crash (`early_commit_strands`), and a file created before
its name is recorded is left where nothing finds it (`unrecorded_file_lost`). -/

/-- What is on disk at a name: nothing, bytes written but not yet durable, or
durable bytes. -/
inductive Disk where
  | absent | written | durable
  deriving DecidableEq, Repr

/-- Committed rows naming bytes, cleanup records, and the disk. -/
structure State where
  row : Nat → Bool
  record : Nat → Bool
  disk : Nat → Disk

/-- Faulty variants checked as counterexamples. -/
inductive Fault where
  | none
  /-- A row committed before its bytes are durable. -/
  | commitEarly
  /-- A file created before its name is recorded. -/
  | createUnrecorded
  deriving DecidableEq

def set {α : Type} (f : Nat → α) (n : Nat) (a : α) : Nat → α :=
  fun x => if x = n then a else f x

inductive Step (fault : Fault) : State → State → Prop where
  /-- Record a new name for cleanup before creating anything there. -/
  | recordName (s : State) (n : Nat) (fresh : s.row n = false ∧ s.disk n = .absent) :
      Step fault s { s with record := set s.record n true }
  /-- Create the file at a recorded name. -/
  | create (s : State) (n : Nat) (recorded : s.record n = true ∨ fault = .createUnrecorded)
      (empty : s.disk n = .absent) :
      Step fault s { s with disk := set s.disk n .written }
  /-- `fsync`, and the batch's flush. -/
  | sync (s : State) (n : Nat) (written : s.disk n = .written) :
      Step fault s { s with disk := set s.disk n .durable }
  /-- Commit a row naming durable bytes, clearing their cleanup record. -/
  | commitRow (s : State) (n : Nat) (ready : s.disk n = .durable ∨ fault = .commitEarly)
      (recorded : s.record n = true) :
      Step fault s { s with row := set s.row n true, record := set s.record n false }
  /-- A transaction releasing a row records its bytes for deletion. -/
  | release (s : State) (n : Nat) (named : s.row n = true) :
      Step fault s { s with row := set s.row n false, record := set s.record n true }
  /-- Delete the bytes a record names, once the writer is free. A failure is
  a step not taken: the record stays. -/
  | delete (s : State) (n : Nat) (recorded : s.record n = true) :
      Step fault s { s with disk := set s.disk n .absent }
  /-- Remove a record once its bytes are gone. -/
  | clearRecord (s : State) (n : Nat) (recorded : s.record n = true) (gone : s.disk n = .absent) :
      Step fault s { s with record := set s.record n false }
  /-- A crash: bytes not yet durable may be lost; committed rows, records and
  durable bytes stay. -/
  | crash (s : State) (lost : Nat → Bool) :
      Step fault s { s with disk := fun x =>
        if (s.disk x == .written && lost x) = true then .absent else s.disk x }

inductive Steps (fault : Fault) : State → State → Prop where
  | refl (s : State) : Steps fault s s
  | step {a b c : State} : Step fault a b → Steps fault b c → Steps fault a c

/-- What every reachable state satisfies. -/
structure Inv (s : State) : Prop where
  rows_durable : ∀ n, s.row n = true → s.disk n = .durable
  findable : ∀ n, s.disk n ≠ .absent → s.row n = true ∨ s.record n = true
  apart : ∀ n, ¬ (s.row n = true ∧ s.record n = true)

def State.empty : State := ⟨fun _ => false, fun _ => false, fun _ => .absent⟩

theorem inv_empty : Inv State.empty :=
  ⟨by simp [State.empty], by simp [State.empty], by simp [State.empty]⟩

theorem step_inv {s t : State} (st : Step .none s t) (hi : Inv s) : Inv t := by
  cases st with
  | recordName n fresh =>
    refine ⟨hi.rows_durable, fun x hx => ?_, fun x ⟨hr, hc⟩ => ?_⟩
    · by_cases hn : x = n
      · subst hn; exact Or.inr (by simp [set])
      · simpa [set, hn] using hi.findable x hx
    · by_cases hn : x = n
      · subst hn; simp [fresh.1] at hr
      · exact hi.apart x ⟨hr, by simpa [set, hn] using hc⟩
  | create n recorded empty =>
    have recorded : s.record n = true := by simpa using recorded
    refine ⟨fun x hx => ?_, fun x hx => ?_, hi.apart⟩
    · by_cases hn : x = n
      · subst hn; have := hi.rows_durable x hx; simp [empty] at this
      · simpa [set, hn] using hi.rows_durable x hx
    · by_cases hn : x = n
      · subst hn; exact Or.inr recorded
      · exact hi.findable x (by simpa [set, hn] using hx)
  | sync n written =>
    refine ⟨fun x hx => ?_, fun x hx => ?_, hi.apart⟩
    · by_cases hn : x = n
      · subst hn; simp [set]
      · simpa [set, hn] using hi.rows_durable x hx
    · by_cases hn : x = n
      · subst hn; exact hi.findable x (by simp [written])
      · exact hi.findable x (by simpa [set, hn] using hx)
  | commitRow n ready recorded =>
    have ready : s.disk n = .durable := by simpa using ready
    refine ⟨fun x hx => ?_, fun x hx => ?_, fun x ⟨hr, hc⟩ => ?_⟩
    · by_cases hn : x = n
      · subst hn; exact ready
      · exact hi.rows_durable x (by simpa [set, hn] using hx)
    · by_cases hn : x = n
      · subst hn; exact Or.inl (by simp [set])
      · simpa [set, hn] using hi.findable x hx
    · by_cases hn : x = n
      · subst hn; simp [set] at hc
      · exact hi.apart x ⟨by simpa [set, hn] using hr, by simpa [set, hn] using hc⟩
  | release n named =>
    refine ⟨fun x hx => ?_, fun x hx => ?_, fun x ⟨hr, hc⟩ => ?_⟩
    · by_cases hn : x = n
      · subst hn; simp [set] at hx
      · exact hi.rows_durable x (by simpa [set, hn] using hx)
    · by_cases hn : x = n
      · subst hn; exact Or.inr (by simp [set])
      · simpa [set, hn] using hi.findable x hx
    · by_cases hn : x = n
      · subst hn; simp [set] at hr
      · exact hi.apart x ⟨by simpa [set, hn] using hr, by simpa [set, hn] using hc⟩
  | delete n recorded =>
    refine ⟨fun x hx => ?_, fun x hx => ?_, hi.apart⟩
    · by_cases hn : x = n
      · subst hn; exact absurd ⟨hx, recorded⟩ (hi.apart x)
      · simpa [set, hn] using hi.rows_durable x hx
    · by_cases hn : x = n
      · subst hn; simp [set] at hx
      · exact hi.findable x (by simpa [set, hn] using hx)
  | clearRecord n recorded gone =>
    refine ⟨hi.rows_durable, fun x hx => ?_, fun x ⟨hr, hc⟩ => ?_⟩
    · by_cases hn : x = n
      · subst hn; exact absurd gone hx
      · simpa [set, hn] using hi.findable x hx
    · by_cases hn : x = n
      · subst hn; simp [set] at hc
      · exact hi.apart x ⟨hr, by simpa [set, hn] using hc⟩
  | crash lost =>
    refine ⟨fun x hx => ?_, fun x hx => ?_, hi.apart⟩
    · have := hi.rows_durable x hx
      simp only at hx ⊢
      simp [this]
    · simp only at hx ⊢
      by_cases w : (s.disk x == .written && lost x) = true
      · simp only [w, ite_true, ne_eq, not_true_eq_false] at hx
      · simp only [w, Bool.false_eq_true, ite_false] at hx; exact hi.findable x hx

theorem steps_inv {s t : State} (run : Steps .none s t) (hi : Inv s) : Inv t := by
  induction run with
  | refl => exact hi
  | step st _ ih => exact ih (step_inv st hi)

/-- **No row names bytes that aren't durable,** after any crash. -/
theorem rows_durable {s : State} (run : Steps .none State.empty s) (n : Nat)
    (named : s.row n = true) : s.disk n = .durable :=
  (steps_inv run inv_empty).rows_durable n named

/-- **No bytes nobody will delete:** every file on disk is named by a row or
recorded for cleanup. -/
theorem bytes_findable {s : State} (run : Steps .none State.empty s) (n : Nat)
    (there : s.disk n ≠ .absent) : s.row n = true ∨ s.record n = true :=
  (steps_inv run inv_empty).findable n there

/-- **A deletion never takes bytes a row names:** a deletion goes by its
record, and no row names recorded bytes, so it needs no check of its own. -/
theorem delete_spares_rows {s : State} (run : Steps .none State.empty s) (n : Nat)
    (recorded : s.record n = true) : s.row n = false := by
  cases h : s.row n
  · rfl
  · exact absurd ⟨h, recorded⟩ ((steps_inv run inv_empty).apart n)

/-- **Counterexample: a row committed before its bytes are durable** names
missing bytes once a crash loses them. -/
theorem early_commit_strands :
    ∃ s, Steps .commitEarly State.empty s ∧ s.row 1 = true ∧ s.disk 1 = .absent := by
  let s1 : State := { State.empty with record := set State.empty.record 1 true }
  let s2 : State := { s1 with disk := set s1.disk 1 .written }
  let s3 : State := { s2 with row := set s2.row 1 true, record := set s2.record 1 false }
  refine ⟨{ s3 with disk := fun x =>
      if (s3.disk x == .written && (x == 1)) = true then .absent else s3.disk x },
    Steps.step (Step.recordName _ 1 (by simp [State.empty]))
      (Steps.step (Step.create _ 1 (Or.inl (by simp [set])) (by simp [State.empty]))
      (Steps.step (Step.commitRow _ 1 (Or.inr rfl) (by simp [set]))
      (Steps.step (Step.crash _ (fun x => x == 1)) (Steps.refl _)))),
    by simp [s3, set], by simp [s3, s2, set]⟩

/-- **Counterexample: a file created before its name is recorded** is on disk
with no row and no record naming it, where no deletion finds it. -/
theorem unrecorded_file_lost :
    ∃ s, Steps .createUnrecorded State.empty s ∧ s.disk 1 ≠ .absent ∧
      s.row 1 = false ∧ s.record 1 = false :=
  ⟨_, Steps.step (Step.create State.empty 1 (Or.inr rfl) rfl) (Steps.refl _),
    by simp [set], rfl, rfl⟩

end CovenStorage.LocalBytes
