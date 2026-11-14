import CovenIO.Activity
import CovenStorage.Files

/-!
# Which file histories reach a sync state (§17.9, §19.1)

`CovenStorage.Files` steps a device's records and upload queue. This reads that
queue as the sync state does (`queued`), with no attempt running.

- `staged_queued`, `queue_holds_synced`: staging a file queues its upload, and
  a device with an upload queued doesn't show synced, however the rest stands.
- `queued_syncs`: on an otherwise settled device the upload shows syncing,
  uploading a bound file, for its turn.
- `completed_empties`, `completed_synced`: once storage confirmed the last
  queued upload, the queue is empty, and the settled device shows synced.
- `lost_file_fails`: a confirmed absence at a record's path is a lost file, and
  its record keeps the state at failed or what outranks it.

What holds a queued upload other than its turn, the pause, the network or a
backoff, is a setting or a failed attempt, not a step of `Files`: `wait_facts`
says what each one needs.
-/
namespace CovenIO.FileReach

open CovenIO.SyncState
open CovenStorage (Files.State Files.Step Files.complete Files.change Files.drop)

/-- The upload queue as the sync state reads it, with no attempt running:
each staged file's upload holds the write that staged it, so it is bound. -/
def queued (s : Files.State) : List Upload :=
  s.queue.map fun _ => ⟨.queued, true, false, none⟩

theorem bound_queued (s : Files.State) : (queued s).filter (·.bound) = queued s := by
  unfold queued
  apply List.filter_eq_self.mpr
  intro a ha
  simp only [List.mem_map] at ha
  obtain ⟨_, _, rfl⟩ := ha
  rfl

theorem bound_eq (f : Facts) (s : Files.State) (uploads : f.uploads = queued s) :
    f.bound = queued s := by
  simp [Facts.bound, uploads, bound_queued]

theorem change_queue (s : Files.State) (row : Nat) (file : Option Nat) :
    (Files.change s row file).queue.length = s.queue.length := by
  unfold Files.change
  split
  · split
    · dsimp only
      split
      · rfl
      · simp [Files.drop, Files.State.point]
    · rfl
  · rfl

/-- **Staging a file queues its upload.** -/
theorem staged_queued (s : Files.State) (row : Nat) (u : CovenStorage.Files.Upload) (id : Nat)
    (fresh : s.known id = false) : (Files.Step.apply s (.stage row u id)).queue ≠ [] := by
  intro empty
  simp only [Files.Step.apply, fresh, Bool.false_eq_true, ite_false, CovenStorage.Files.stage] at empty
  have := change_queue { s with
    records := ⟨id, u.size, none, .device s.self⟩ :: s.records
    queue := { u with files := [id] } :: s.queue } row (some id)
  rw [empty] at this
  simp at this

/-- **A queued upload holds synced off.** -/
theorem queue_holds_synced (f : Facts) (s : Files.State) (uploads : f.uploads = queued s)
    (held : s.queue ≠ []) : state f ≠ .synced := by
  intro shown
  obtain ⟨u, rest, eq⟩ := List.exists_cons_of_ne_nil held
  have := synced_bound_stored f shown ⟨.queued, true, false, none⟩
    (by simp [bound_eq f s uploads, queued, eq])
  cases this

/-- A device with nothing but its uploads to name a state: connected, its last
pass synced, nothing moving, no write queued, no record, sync running and
nothing holding it. -/
structure Settled (f : Facts) : Prop where
  connected : f.storage = .connected
  last : f.last = .synced
  still : f.moved = none
  noWrites : f.unsent = false
  noRecords : f.records = []
  running : f.running = true
  cooling : f.cooling = false

/-- **A queued upload is next in line.** On a settled device it shows syncing,
uploading a bound file: its turn is no wait. -/
theorem queued_syncs (f : Facts) (s : Files.State) (settled : Settled f)
    (uploads : f.uploads = queued s) (held : s.queue ≠ []) :
    state f = .syncing ∧ f.waits = [] ∧ f.activities.head? = some .uploadingBound := by
  obtain ⟨u, rest, eq⟩ := List.exists_cons_of_ne_nil held
  have recorded : f.storage ≠ .none := by rw [settled.connected]; simp
  have loop : f.loop = .synced := by
    simp [Facts.loop, settled.still, Facts.ended, settled.last]
  have blocked : f.blocked = false := by simp [Facts.blocked, settled.last]
  have free : f.heldAll = none := by
    simp [Facts.heldAll, settled.running, settled.cooling, blocked]
  have mem : (⟨.queued, true, false, none⟩ : Upload) ∈ f.bound := by
    simp [bound_eq f s uploads, queued, eq]
  have cand : (⟨.queued, true, false, none⟩ : Upload).candidate f.held = some .syncing := by
    simp [Upload.candidate, Facts.held, free, State.work]
  refine ⟨?_, ?_, bound_before_write f settled.still _ mem cand⟩
  · refine (state_iff f recorded _).mpr ⟨(candidate_iff f _).mpr (.inr (.inl ⟨_, mem, cand⟩)), ?_⟩
    intro c hc
    rcases (candidate_iff f c).mp hc with e | ⟨x, mx, hx⟩ | ⟨_, w⟩ | ⟨_, h⟩ | ⟨r, hr, _⟩
    · rw [← e, loop]; simp [State.rank]
    · simp only [bound_eq f s uploads, queued, List.mem_map] at mx
      obtain ⟨_, _, rfl⟩ := mx
      simp [Upload.candidate, Facts.held, free, State.work] at hx
      subst hx
      simp [State.rank]
    · rw [settled.noWrites] at w; cases w
    · rw [settled.connected] at h; cases h
    · rw [settled.noRecords] at hr; cases hr
  · simp [Facts.waits, loop, bound_eq f s uploads, queued, eq, Facts.uploadWait, free,
      settled.noWrites, Upload.candidate, Facts.held, State.work]

theorem complete_queue (s : Files.State) (r : CovenStorage.Files.Reference)
    (u : CovenStorage.Files.Upload) (h : u ∈ (Files.complete s r).queue) : u ∈ s.queue := by
  unfold Files.complete at h
  split at h
  · exact h
  · split at h <;> exact (List.mem_filter.mp h).1

/-- **Storage confirming the last upload empties the queue.** -/
theorem completed_empties (s : Files.State) (r : CovenStorage.Files.Reference)
    (only : ∀ u ∈ s.queue, u.location = r) : (Files.complete s r).queue = [] := by
  apply List.eq_nil_iff_forall_not_mem.mpr
  intro u hu
  exact CovenStorage.Files.completion_retires_queue s r u hu (only u (complete_queue s r u hu))

/-- **A settled device whose last upload was confirmed shows synced.** -/
theorem completed_synced (f : Facts) (s : Files.State) (r : CovenStorage.Files.Reference)
    (settled : Settled f) (uploads : f.uploads = queued (Files.complete s r))
    (only : ∀ u ∈ s.queue, u.location = r) : state f = .synced := by
  have empty : f.uploads = [] := by
    rw [uploads, queued, completed_empties s r only]; rfl
  apply (synced_iff f).mpr
  refine ⟨settled.connected, settled.last, settled.still, ?_, settled.noWrites, ?_⟩
  · intro u hu; simp [Facts.bound, empty] at hu
  · intro x hx; rw [settled.noRecords] at hx; cases hx

/-- **A file storage lost keeps the device failed.** Storage confirming nothing
at a record's path says the file is lost, and its record shows failed or what
outranks it. -/
theorem lost_file_fails (f : Facts) (recorded : f.storage ≠ .none) (r : CovenStorage.Files.Reference)
    (size : Nat) (noted : Reason.fileLost ∈ f.records) :
    CovenStorage.Files.status r size (.ok none) = .ok (.missing r.uploader .lost) ∧
      State.failed.rank ≤ (state f).rank :=
  ⟨rfl, bad_file_fails f recorded .fileLost noted rfl⟩

end CovenIO.FileReach
