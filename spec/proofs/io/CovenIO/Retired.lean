import CovenIO.Storage

namespace CovenIO.Retired

/-! ### Cleaning up the folder of an uploader gone for good (§17.10)

Once the entry that stops a device for good applies, such as its removal,
nothing it writes counts (§14.1). A file it uploads afterwards, while its
provider access still works, can never be referred to, so any listing of its
folder is sound whenever it is taken: each file it finds becomes a candidate,
and retained references alone decide whether it is deleted. A pass lists the
folder once, when that entry applies; the app's cleanup lists it again
whenever it is asked. A failed listing records nothing and is retried. -/

/-- One gone uploader's folder as this device knows it: the files stored
in it, the candidates its completed listings found, and whether a listing
has completed. -/
structure Folder where
  files : List Nat
  candidates : List Nat
  listed : Bool
  deriving DecidableEq, Repr

def Folder.initial : Folder := ⟨[], [], false⟩

inductive FolderEvent where
  /-- The gone device uploads a file its key still lets it store. -/
  | upload (file : Nat)
  /-- A pass: it lists the folder only if no listing has completed yet. -/
  | pass (completes : Bool)
  /-- The app's cleanup: it lists the folder again. -/
  | cleanUp (completes : Bool)
  deriving DecidableEq, Repr

/-- One listing: a completed one adds every file in the folder to the
candidates; a failed one changes nothing. -/
def Folder.list (f : Folder) (completes : Bool) : Folder :=
  if completes then
    { f with candidates := f.candidates ++ f.files.filter (· ∉ f.candidates), listed := true }
  else f

def Folder.step (f : Folder) : FolderEvent → Folder
  | .upload file => { f with files := file :: f.files }
  | .pass completes => if f.listed then f else f.list completes
  | .cleanUp completes => f.list completes

def Folder.run (events : List FolderEvent) : Folder :=
  events.foldl Folder.step Folder.initial

theorem failed_listing_records_nothing (f : Folder) : f.list false = f := by
  simp [Folder.list]

/-- **A completed listing finds every file the folder holds.** -/
theorem listing_finds_every_file (f : Folder) :
    ∀ file ∈ f.files, file ∈ (f.list true).candidates := by
  intro file h
  by_cases hc : file ∈ f.candidates
  · simp [Folder.list, hc]
  · simp [Folder.list, hc, h]

/-- A candidate stays one whatever happens next; only the reference check
of a deletion removes it. -/
theorem candidates_kept (f : Folder) (e : FolderEvent) :
    ∀ file ∈ f.candidates, file ∈ (f.step e).candidates := by
  intro file h
  cases e with
  | upload _ => simpa [Folder.step] using h
  | pass completes =>
      cases completes <;> by_cases hl : f.listed = true <;> simp [Folder.step, Folder.list, hl, h]
  | cleanUp completes =>
      cases completes <;> simp [Folder.step, Folder.list, h]

/-- A pass after a completed listing lists nothing again. -/
theorem pass_lists_once (f : Folder) (completes : Bool) (listed : f.listed = true) :
    f.step (.pass completes) = f := by
  simp [Folder.step, listed]

/-- **The cleanup finds what a removed device uploaded after its removal.**
A listing once, at removal, misses Ben's photo 1, uploaded after it; a later
pass doesn't list again, and the app's cleanup finds it. -/
theorem cleanup_finds_a_later_upload :
    let once := Folder.run [.pass true, .upload 1, .pass true]
    1 ∈ once.files ∧ 1 ∉ once.candidates ∧ 1 ∈ (once.step (.cleanUp true)).candidates := by
  decide

/-! ### Who cleans up, and whose folders (§14.1, §17.10)

A member's removal stops every device of the member for good, and a
replacement of the member's keys every device of the member added before it,
except the device that wrote it, which holds the new pair. A device's own
member's other devices clean up its folder; when that member has no device
left that isn't gone, every active admin's devices do. This mirrors
`History::stops` and `History::deletes_for`. -/

/-- A counted entry that stops devices for good: a device's removal, a
member's, a reset registering another installation in a device's place, or a
member's keys replaced by the device `author`. -/
inductive Stop (Dev Mem : Type) where
  | removeDevice (device : Dev)
  | removeMember (member : Mem)
  | replaced (device : Dev)
  | replaceKeys (member : Mem) (author : Dev)

/-- A registered device: its id, its member and the slot that added it. -/
structure Device (Dev Mem : Type) where
  id : Dev
  member : Mem
  added : Nat

variable {Dev Mem : Type} [DecidableEq Dev] [DecidableEq Mem]

/-- Whether the entry at `slot` stops device `d` for good. -/
def stops (d : Device Dev Mem) : Nat × Stop Dev Mem → Bool
  | (_, .removeDevice x) => decide (x = d.id)
  | (_, .removeMember m) => decide (m = d.member)
  | (_, .replaced x) => decide (x = d.id)
  | (slot, .replaceKeys m author) => decide (m = d.member) && decide (d.added < slot) && decide (author ≠ d.id)

/-- Gone for good: some counted entry stops it. -/
def gone (cutoffs : List (Nat × Stop Dev Mem)) (d : Device Dev Mem) : Bool :=
  cutoffs.any (stops d)

/-- Whether device `own`, whose member is an active admin when `admin`
says so, cleans up `uploader`'s folder. -/
def cleansUp (devices : List (Device Dev Mem)) (cutoffs : List (Nat × Stop Dev Mem))
    (admin : Mem → Bool) (own uploader : Device Dev Mem) : Bool :=
  decide (own.id = uploader.id) ||
    (gone cutoffs uploader &&
      (decide (own.member = uploader.member) ||
        (!devices.any (fun d => decide (d.member = uploader.member) && !gone cutoffs d) &&
          admin own.member)))

/-- **A member's cut-off covers every one of its devices.** A member's removal
stops each of the member's devices, and a replacement of the member's keys
stops each one added before it except its author. -/
theorem member_cutoff_covers (cutoffs : List (Nat × Stop Dev Mem)) (d : Device Dev Mem) :
    ((∃ slot, (slot, Stop.removeMember d.member) ∈ cutoffs) → gone cutoffs d = true) ∧
    (∀ slot author, (slot, Stop.replaceKeys d.member author) ∈ cutoffs → d.added < slot →
      author ≠ d.id → gone cutoffs d = true) := by
  constructor
  · rintro ⟨slot, member⟩
    exact List.any_eq_true.mpr ⟨_, member, by simp [stops]⟩
  · intro slot author member before other
    exact List.any_eq_true.mpr ⟨_, member, by simp [stops, before, other]⟩

/-- **Every gone device's folder has a device to clean it up**, as long as
some active admin has a device that isn't gone: one of its own member's
devices that isn't gone, or else that admin's. -/
theorem every_gone_folder_cleaned (devices : List (Device Dev Mem))
    (cutoffs : List (Nat × Stop Dev Mem)) (admin : Mem → Bool) (g : Device Dev Mem)
    (stopped : gone cutoffs g = true)
    (adminLeft : ∃ a ∈ devices, gone cutoffs a = false ∧ admin a.member = true) :
    ∃ d ∈ devices, gone cutoffs d = false ∧ cleansUp devices cutoffs admin d g = true := by
  by_cases left : devices.any (fun d => decide (d.member = g.member) && !gone cutoffs d) = true
  · obtain ⟨d, member, same⟩ := List.any_eq_true.mp left
    simp only [Bool.and_eq_true, decide_eq_true_eq, Bool.not_eq_true'] at same
    exact ⟨d, member, same.2, by simp [cleansUp, stopped, same.1]⟩
  · obtain ⟨a, member, alive, isAdmin⟩ := adminLeft
    refine ⟨a, member, alive, ?_⟩
    simp only [Bool.not_eq_true] at left
    simp [cleansUp, stopped, left, isAdmin]

end CovenIO.Retired
