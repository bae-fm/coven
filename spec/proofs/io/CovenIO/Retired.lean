import CovenIO.Storage

namespace CovenIO.Retired

/-- A complete one-time orphan observation is durable; failed pages cannot
record completion. The scan restarts only after a new access epoch. -/
inductive FileScan where
  | pending
  | complete (paths : List Path)
  deriving DecidableEq, Repr

def observeFiles (old : FileScan) (result : Except Failure (List Path)) : FileScan :=
  match old, result with
  | .complete paths, _ => .complete paths
  | .pending, .error _ => .pending
  | .pending, .ok paths => .complete paths

theorem failed_scan_not_complete (e : Failure) : observeFiles .pending (.error e) = .pending := rfl
theorem completed_scan_retained (paths : List Path) (result : Except Failure (List Path)) :
    observeFiles (.complete paths) result = .complete paths := rfl

/-! ### Restored access forgets the scan (§17.8)

A removed uploader can publish into its file folder only while its access is
not confirmed taken away. Each retention lists the folder only after that
cut-off, and the device's one owner (Appendix F.3) runs a retention whole, so no
transaction recording access lands between a listing and its record. A
transaction that records any of the access restored forgets the scan
(`forgetOnRestore`); the rule it replaced forgot it only when a retention
next saw the access restored. -/

/-- One removed uploader's folder as this device knows it: whether its
access is confirmed taken away, the files it published, and the scan. -/
structure Folder where
  cut : Bool
  files : List Nat
  scan : FileScan
  deriving DecidableEq, Repr

def Folder.initial : Folder := ⟨false, [], .pending⟩

inductive FolderEvent where
  /-- The uploader publishes a file; a confirmed cut-off prevents it (§14.2). -/
  | upload (file : Nat)
  /-- A transaction records some of the access restored. -/
  | restore
  /-- A transaction records all of it confirmed taken away again. -/
  | revoke
  /-- One retention: after the cut-off, a listing that completes with every
  file in the folder or fails. -/
  | retain (completes : Bool)
  deriving DecidableEq, Repr

/-- What one retention's listing returns after the cut-off. -/
def Folder.listing (writer : Nat) (f : Folder) (completes : Bool) : Except Failure (List Path) :=
  if completes then .ok (f.files.map (CovenStorage.Path.file writer)) else .error .network

def Folder.step (writer : Nat) (forgetOnRestore : Bool) (f : Folder) : FolderEvent → Folder
  | .upload file => if f.cut then f else { f with files := file :: f.files }
  | .restore => { f with cut := false, scan := if forgetOnRestore then .pending else f.scan }
  | .revoke => { f with cut := true }
  | .retain completes =>
      if f.cut then { f with scan := observeFiles f.scan (f.listing writer completes) }
      else if forgetOnRestore then f
      else { f with scan := .pending }

def Folder.run (writer : Nat) (forgetOnRestore : Bool) (events : List FolderEvent) : Folder :=
  events.foldl (Folder.step writer forgetOnRestore) Folder.initial

/-- A completed scan stands for a cut-off still in force and names every
file the folder holds. -/
def Folder.Sound (writer : Nat) (f : Folder) : Prop :=
  ∀ paths, f.scan = .complete paths → f.cut = true ∧ ∀ file ∈ f.files, CovenStorage.Path.file writer file ∈ paths

theorem initial_sound (writer : Nat) : Folder.initial.Sound writer := by
  intro paths h; cases h

theorem step_sound (writer : Nat) (f : Folder) (e : FolderEvent) (sound : f.Sound writer) :
    (f.step writer true e).Sound writer := by
  obtain ⟨cut, files, scan⟩ := f
  intro paths h
  cases e with
  | upload file =>
      cases cut
      · have := (sound paths (by simpa [Folder.step] using h)).1
        cases this
      · exact sound paths (by simpa [Folder.step] using h)
  | restore => simp [Folder.step] at h
  | revoke =>
      exact ⟨rfl, (sound paths (by simpa [Folder.step] using h)).2⟩
  | retain completes =>
      cases cut
      · exact sound paths (by simpa [Folder.step] using h)
      · refine ⟨rfl, ?_⟩
        cases scan with
        | complete old =>
            have : old = paths := by simpa [Folder.step, observeFiles] using h
            subst this
            exact (sound old rfl).2
        | pending =>
            cases completes
            · simp [Folder.step, Folder.listing, observeFiles] at h
            · have : files.map (CovenStorage.Path.file writer) = paths := by
                simpa [Folder.step, Folder.listing, observeFiles] using h
              subst this
              intro file hf
              exact List.mem_map.mpr ⟨file, hf, rfl⟩

/-- **A completed scan names every file the folder holds**, however restores
and revocations interleave with retentions, because the transaction that
records a restore forgets it (§17.8). -/
theorem scan_names_every_file (writer : Nat) (events : List FolderEvent) :
    (Folder.run writer true events).Sound writer := by
  unfold Folder.run
  suffices h : ∀ (f : Folder), f.Sound writer → (events.foldl (Folder.step writer true) f).Sound writer from
    h _ (initial_sound writer)
  induction events with
  | nil => exact fun _ h => h
  | cons e es ih => exact fun f h => ih _ (step_sound writer f e h)

/-- After a revocation, one retention whose listing completes records a scan
naming every file the folder holds. -/
theorem revoked_listing_completes (writer : Nat) (events : List FolderEvent) :
    ∃ paths, (Folder.run writer true (events ++ [.revoke, .retain true])).scan = .complete paths := by
  unfold Folder.run
  rw [List.foldl_append]
  generalize events.foldl (Folder.step writer true) Folder.initial = f
  obtain ⟨cut, files, scan⟩ := f
  cases scan with
  | pending => exact ⟨files.map (CovenStorage.Path.file writer), by simp [Folder.step, Folder.listing, observeFiles]⟩
  | complete paths => exact ⟨paths, by simp [Folder.step, observeFiles]⟩

/-- Forgetting only at the next retention is insufficient: Ana lists Carol's folder after her cut-off and
finds nothing. Carol's access is restored and taken away again between two
retentions, and she uploads photo 1 meanwhile. Forgetting the scan only when
a retention sees the access restored keeps the empty scan, so photo 1 is
never listed. -/
theorem retention_forgetting_counterexample :
    let f := Folder.run 3 false [.revoke, .retain true, .restore, .upload 1, .revoke, .retain true]
    f.scan = .complete [] ∧ 1 ∈ f.files ∧ ¬ f.Sound 3 := by
  refine ⟨by decide, by decide, fun sound => ?_⟩
  have := (sound [] (by decide)).2 1 (by decide)
  simp at this

end CovenIO.Retired
