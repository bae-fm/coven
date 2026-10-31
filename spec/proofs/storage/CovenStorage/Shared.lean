import CovenStorage.Files

namespace CovenStorage.Shared

open Files

/-! §17.2, §17.10: files over the same bytes. A new file over an existing
file's bytes, as `existing_file` stages one for a row in another audience
and as a moved row gets one, is a record of its own with the same size,
content hash and coordinates; one over a file still uploading here waits for
the same upload. Each record changes and goes on its own, and stored bytes
are deleted only when they are a deletion candidate that no record names, no
upload holds and no unpublished write of this device keeps.

Proved here, in every state the steps reach, with the steps of `Files`
among them: every file whose coordinates say cloud is in storage
(`steps_cloud_stored`), so no stored bytes a record names are ever deleted
(`purge_unnamed`); and every stored path stays within deletion's reach
(`steps_reachable`). -/

/-- §17.2: this device's write making file `id` over the bytes of the file
`source` it has a record of, with `source`'s size, content hash and
coordinates, and pointing `row` at it, in one transaction. Nothing is
uploaded: an upload still storing `source` stores `id` too. A `source` this
device has no record of gives nothing (`FileAbsent`). -/
def copy (s : State) (row source id : Nat) : State :=
  match s.records.find? (·.id == source) with
  | none => s
  | some rec =>
    change { s with
      records := { rec with id := id } :: s.records
      queue := s.queue.map fun u =>
        if u.files.contains source then { u with files := id :: u.files } else u } row (some id)

/-- §17.10: whether stored bytes at `r` may be deleted: they are a deletion
candidate, and no record names them, no upload holds them and no unpublished
write of this device keeps them. -/
def purgeable (s : State) (r : Reference) : Bool :=
  s.candidates.contains r && !(s.records.any (·.coordinates == .cloud r)) &&
    !(s.queue.any (·.location == r)) && !(s.unpublished.contains r)

/-- §17.10: delete the stored bytes at `r` if they may be deleted; otherwise
leave them. -/
def purge (s : State) (r : Reference) : State :=
  if purgeable s r then
    { s with stored := s.stored.filter (· != r), candidates := s.candidates.filter (· != r) }
  else s

/-- What a device may do: any step of `Files`, a write making a new file
over an existing file's bytes, and deleting stored bytes. -/
inductive Step where
  | files (st : Files.Step)
  | copy (row source id : Nat)
  | purge (r : Reference)

def Step.apply (s : State) : Step → State
  | .files st => Files.Step.apply s st
  | .copy row source id => if s.known id then s else Shared.copy s row source id
  | .purge r => Shared.purge s r

def runSteps (s : State) (steps : List Step) : State := steps.foldl Step.apply s

theorem copy_cloud_stored (s : State) (row source id : Nat) (h : CloudStored s) :
    CloudStored (copy s row source id) := by
  unfold copy
  split
  · exact h
  · rename_i rec hfind
    apply change_cloud_stored
    intro x hx r hc
    simp only [List.mem_cons] at hx
    rcases hx with e | e
    · subst e
      exact h rec (List.mem_of_find?_eq_some hfind) r hc
    · exact h x e r hc

theorem copy_reachable (s : State) (row source id : Nat) (h : Reachable s) :
    Reachable (copy s row source id) := by
  unfold copy
  split
  · exact h
  · apply change_reachable
    intro x hx
    rcases h x hx with ⟨y, hy, hc⟩ | ⟨u, hu, he⟩ | hp | hc
    · left; exact ⟨y, List.mem_cons_of_mem _ hy, hc⟩
    · right; left
      refine ⟨if u.files.contains source then { u with files := id :: u.files } else u,
        List.mem_map_of_mem (f := fun u : Files.Upload =>
          if u.files.contains source then { u with files := id :: u.files } else u) hu, ?_⟩
      split <;> exact he
    · right; right; left; exact hp
    · right; right; right; exact hc

/-- Bytes that may be deleted are named by no record. -/
theorem purgeable_unnamed (s : State) (r : Reference) (hp : purgeable s r = true) :
    ∀ rec ∈ s.records, rec.coordinates ≠ .cloud r := by
  intro rec hrec hc
  have named : s.records.any (·.coordinates == .cloud r) = true :=
    List.any_eq_true.mpr ⟨rec, hrec, by simp [hc]⟩
  simp [purgeable, named] at hp

/-- §17.10: stored bytes are deleted only when no record names them. -/
theorem purge_unnamed (s : State) (r : Reference) (was : r ∈ s.stored)
    (gone : r ∉ (purge s r).stored) : ∀ rec ∈ s.records, rec.coordinates ≠ .cloud r := by
  unfold purge at gone
  split at gone
  · rename_i hp
    exact purgeable_unnamed s r hp
  · exact absurd was gone

theorem purge_cloud_stored (s : State) (r : Reference) (h : CloudStored s) :
    CloudStored (purge s r) := by
  unfold purge
  split
  · rename_i hp
    intro x hx r' hc
    have hne : r' ≠ r := by
      intro e; subst e
      exact purgeable_unnamed s r' hp x hx hc
    simp only [List.mem_filter]
    exact ⟨h x hx r' hc, by simpa using hne⟩
  · exact h

theorem purge_reachable (s : State) (r : Reference) (h : Reachable s) :
    Reachable (purge s r) := by
  unfold purge
  split
  · intro x hx
    simp only [List.mem_filter, bne_iff_ne, ne_eq] at hx
    rcases h x hx.1 with hr | hq | hp | hc
    · left; exact hr
    · right; left; exact hq
    · right; right; left; exact hp
    · right; right; right; simp [List.mem_filter, hc, hx.2]
  · exact h

theorem step_cloud_stored (s : State) (st : Step) (h : CloudStored s) :
    CloudStored (Step.apply s st) := by
  cases st with
  | files st => exact Files.step_cloud_stored s st h
  | copy row source id =>
    simp only [Step.apply]; split
    · exact h
    · exact copy_cloud_stored s row source id h
  | purge r => exact purge_cloud_stored s r h

theorem step_reachable (s : State) (st : Step) (h : Reachable s) :
    Reachable (Step.apply s st) := by
  cases st with
  | files st => exact Files.step_reachable s st h
  | copy row source id =>
    simp only [Step.apply]; split
    · exact h
    · exact copy_reachable s row source id h
  | purge r => exact purge_reachable s r h

/-- §17.2, §17.10: in every state the steps reach, every file whose
coordinates say cloud is in storage. -/
theorem steps_cloud_stored (s : State) (steps : List Step) (h : CloudStored s) :
    CloudStored (runSteps s steps) := by
  induction steps generalizing s with
  | nil => exact h
  | cons st rest ih => exact ih _ (step_cloud_stored s st h)

theorem steps_reachable (s : State) (steps : List Step) (h : Reachable s) :
    Reachable (runSteps s steps) := by
  induction steps generalizing s with
  | nil => exact h
  | cons st rest ih => exact ih _ (step_reachable s st h)

/-- Ana's photo A, file 1, is stored for row 7, and she puts it in row 8 as
file 2 over the same bytes, with no upload. Deleting row 7 deletes A's
record, and once that is published its bytes are a candidate, but Gifts'
file 2 still names them, so they stay. Deleting row 8 too, and publishing,
lets them go. -/
theorem bytes_stay_while_either_record_names_them :
    let photo : Reference := ⟨7, 1, 100⟩
    let start : State := ⟨7, [(7, 1)], [⟨1, 2, some 12, .cloud photo⟩], [], [photo], [], [], 1⟩
    let shared := copy start 8 1 2
    let first := purge (Files.publish (change shared 7 none)) photo
    let both := purge (Files.publish (change first 8 none)) photo
    shared.records = [⟨2, 2, some 12, .cloud photo⟩, ⟨1, 2, some 12, .cloud photo⟩] ∧
      shared.queue = [] ∧
      first.records = [⟨2, 2, some 12, .cloud photo⟩] ∧ first.stored = [photo] ∧
      both.records = [] ∧ both.stored = [] := by decide

end CovenStorage.Shared
