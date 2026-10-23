import CovenIO.Storage

namespace CovenIO.Retired

/-! ### Cleaning up a removed uploader's folder (§17.8)

Once a device's removal applies, nothing it writes counts (§14.1). A file it
uploads afterwards, while its provider access still works, can never be
referred to, so any listing of its folder is sound whenever it is taken: each
file it finds becomes a candidate, and retained references alone decide
whether it is deleted. A pass lists the folder once, when the removal
applies; the app's cleanup lists it again whenever it is asked. A failed
listing records nothing and is retried. -/

/-- One removed uploader's folder as this device knows it: the files stored
in it, the candidates its completed listings found, and whether a listing
has completed. -/
structure Folder where
  files : List Nat
  candidates : List Nat
  listed : Bool
  deriving DecidableEq, Repr

def Folder.initial : Folder := ⟨[], [], false⟩

inductive FolderEvent where
  /-- The removed device uploads a file its key still lets it store. -/
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

end CovenIO.Retired
