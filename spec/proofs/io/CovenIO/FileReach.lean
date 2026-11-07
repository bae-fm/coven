import CovenIO.SyncReach
import CovenStorage.Files

/-!
# Which file histories reach a sync state (§17.9, §19.1)

`CovenStorage.Files` steps a device's records and upload queue. This reads that
queue as the sync state does (`queued`), with no attempt running.

- `staged_queued`, `queue_holds_synced`: staging a file queues its upload, and
  a device with an upload queued doesn't show synced, however the rest stands.
- `queued_waits`: on an otherwise settled device the upload shows waiting to
  sync, and it waits its turn.
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

/-- The upload queue as the sync state reads it, with no attempt running. -/
def queued (s : Files.State) : List Upload :=
  s.queue.map fun _ => ⟨.queued, false, false, none⟩

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
  have := synced_files_stored f shown ⟨.queued, false, false, none⟩
    (by simp [uploads, queued, eq])
  cases this

/-- A device with nothing but its uploads to name a state: connected, its last
pass synced, nothing moving, no record, sync running and nothing holding it. -/
structure Settled (f : Facts) : Prop where
  connected : f.storage = .connected
  last : f.last = .synced
  still : f.moving = false
  noDownload : f.downloading = false
  noRecords : f.records = []
  running : f.running = true
  cooling : f.cooling = false
  unpaused : f.paused = false
  anyNetwork : f.unmeteredOnly = false

/-- **A queued upload waits its turn.** On a settled device it shows waiting to
sync, and that is what it waits for. -/
theorem queued_waits (f : Facts) (s : Files.State) (settled : Settled f)
    (uploads : f.uploads = queued s) (held : s.queue ≠ []) :
    state f = .waitingToSync ∧ f.waits.head? = some .queued := by
  obtain ⟨u, rest, eq⟩ := List.exists_cons_of_ne_nil held
  have recorded : f.storage ≠ .none := by rw [settled.connected]; simp
  have loop : f.loop = .synced := by
    simp [Facts.loop, settled.still, Facts.ended, settled.last]
  constructor
  · refine (state_iff f recorded _).mpr ⟨(candidate_iff f _).mpr (.inr (.inr (.inl ?_))), ?_⟩
    · exact ⟨⟨.queued, false, false, none⟩, by simp [uploads, queued, eq], by
        simp [Upload.candidate]⟩
    · intro c hc
      rcases (candidate_iff f c).mp hc with e | ⟨_, d⟩ | ⟨x, mx, hx⟩ | ⟨_, h⟩ | ⟨r, hr, _⟩
      · rw [← e, loop]; simp [State.rank]
      · rw [settled.noDownload] at d; cases d
      · simp only [uploads, queued, List.mem_map] at mx
        obtain ⟨_, _, rfl⟩ := mx
        simp [Upload.candidate] at hx
        subst hx
        simp [State.rank]
      · rw [settled.connected] at h; cases h
      · rw [settled.noRecords] at hr; cases hr
  · have blocked : f.blocked = false := by simp [Facts.blocked, settled.last]
    have free : f.heldAll = none := by
      simp [Facts.heldAll, settled.running, settled.cooling, blocked]
    simp [Facts.waits, loop, uploads, queued, eq, Facts.uploadWait, free, settled.unpaused,
      Facts.meteredHolds, settled.anyNetwork, Upload.candidate]

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
  refine ⟨settled.connected, settled.last, settled.still, settled.noDownload, ?_, ?_⟩
  · intro u hu; rw [empty] at hu; cases hu
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
