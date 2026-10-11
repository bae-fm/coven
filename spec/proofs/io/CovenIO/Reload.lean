import CovenIO.Execution

namespace CovenIO

def readLoadingLog (world : World) (catalog : Nat → List Snapshot)
    (failure : Nat → Option Failure) (refused : Object → Bool)
    (writer fuel time position : Nat) : Execution :=
  let result := readLog world failure refused .write writer fuel time position
  if result.stop = .miss && coveredMiss (catalog result.time) writer result.position then
    { result with stop := .snapshots }
  else result

theorem loading_miss_uncovered (world : World) (catalog : Nat → List Snapshot)
    (failure : Nat → Option Failure) (refused : Object → Bool)
    (writer fuel time position : Nat)
    (accepted : (readLoadingLog world catalog failure refused writer fuel time position).stop = .miss) :
    let result := readLog world failure refused .write writer fuel time position
    result.stop = .miss ∧ coveredMiss (catalog result.time) writer result.position = false := by
  dsimp only [readLoadingLog] at accepted
  dsimp only
  split at accepted
  · cases accepted
  · rename_i no
    exact ⟨accepted, by cases h : coveredMiss _ _ _ <;> simp_all⟩

/-- Published prefix numbers are either still readable or have counting
coverage. This is the deletion ledger obligation, not suffix availability. -/
theorem loading_prefix_complete (world : World) (catalog : Nat → List Snapshot)
    (failure : Nat → Option Failure) (refused : Object → Bool)
    (writer fuel time position target : Nat)
    (accepted : (readLoadingLog world catalog failure refused writer fuel time position).stop = .miss)
    (accounted : ∀ t, time ≤ t → ∀ n, position < n → n ≤ target →
      (∃ o, world t (.log .write writer n) = some o) ∨
        coveredMiss (catalog t) writer (n - 1) = true) :
    target ≤ (readLog world failure refused .write writer fuel time position).position := by
  obtain ⟨miss, uncovered⟩ := loading_miss_uncovered _ _ _ _ _ _ _ _ accepted
  have scan := executed_scan world failure refused .write writer fuel time position miss
  obtain ⟨afterStart, advanced, absent⟩ := scan_terminal scan
  apply Nat.le_of_not_gt
  intro short
  rcases accounted _ afterStart ((readLog world failure refused .write writer fuel time position).position + 1) (by omega) (by omega) with ⟨o, present⟩ | covered
  · rw [absent] at present; cases present
  · simp only [Nat.add_sub_cancel] at covered
    rw [uncovered] at covered
    cases covered

/-- Apply this to every writer's final scan after the last reload. Reload
invalidates earlier terminal misses; the catch-up sample remains unchanged. -/
theorem completed_catchup_covers_old (world : World) (catalog : Nat → List Snapshot)
    (failure : Nat → Option Failure) (refused : Object → Bool)
    (snap : Snapshot) (fuel start initial : Nat → Nat)
    (accepted : ∀ writer, (readLoadingLog world catalog failure refused
      writer (fuel writer) (start writer) (initial writer)).stop = .miss)
    (positions : ∀ writer, snap.position writer =
      (readLog world failure refused .write writer (fuel writer) (start writer) (initial writer)).position)
    (w : Write)
    (accounted : ∀ t, start w.writer ≤ t → ∀ n, initial w.writer < n → n ≤ w.number →
      (∃ o, world t (.log .write w.writer n) = some o) ∨
        coveredMiss (catalog t) w.writer (n - 1) = true) :
    w.number ≤ snap.position w.writer := by
  rw [positions]
  exact loading_prefix_complete _ _ _ _ _ _ _ _ _ (accepted _) accounted

theorem newest_member (order : SnapshotOrder) (ss : List Snapshot) (s : Snapshot)
    (selected : newest order ss = some s) : s ∈ ss := by
  induction ss with
  | nil => cases selected
  | cons head tail ih =>
      simp only [newest] at selected
      cases h : newest order tail with
      | none => simp only [h] at selected; cases selected; simp
      | some other =>
          simp only [h] at selected
          split at selected
          · cases selected; simp
          · cases selected; exact List.mem_cons_of_mem _ (ih h)

theorem current_member (order : SnapshotOrder) (ss : List Snapshot) (a : Nat) (s : Snapshot)
    (selected : catalogCurrent order ss a = some s) :
    s ∈ ss ∧ s.audience = a ∧ s.counts = true := by
  have member := newest_member order _ s selected
  simpa using List.mem_filter.mp member

theorem current_detects_deleted (order : SnapshotOrder) (ss : List Snapshot) (w : Write)
    (covered : Covered (catalogCurrent order ss) w) (a : Nat) (ha : a ∈ w.audiences)
    (position : Nat) (needed : position < w.number) : coveredMiss ss w.writer position = true := by
  have hc := covered a ha
  cases selected : catalogCurrent order ss a with
  | none => simp [selected] at hc
  | some snap =>
      have facts := current_member order ss a snap selected
      have hc : snap.covers w a := by simpa [selected] using hc
      have bound := hc.2
      exact List.any_eq_true.mpr ⟨snap, facts.1, by simp [facts.2.2, show position < snap.position w.writer by omega]⟩

theorem add_preserves_coverage (order : SnapshotOrder) (ss : List Snapshot) (snap : Snapshot)
    (w : Write) (old : Covered (catalogCurrent order ss) w)
    (new : snap.counts = true → snap.audience ∈ w.audiences → snap.covers w snap.audience) :
    Covered (catalogCurrent order (snap :: ss)) w := by
  intro a ha
  have previous := old a ha
  by_cases counted : (snap.audience == a && snap.counts) = true
  · have facts : snap.audience = a ∧ snap.counts = true := by simpa using counted
    have next := new facts.2 (facts.1 ▸ ha)
    rw [facts.1] at next
    simp only [catalogCurrent, List.filter_cons, counted, ↓reduceIte, newest]
    cases h : newest order (ss.filter (fun s => s.audience == a && s.counts)) with
    | none => simp [catalogCurrent, h] at previous
    | some other =>
        cases hn : newer order snap other with
        | true => simpa [hn] using next
        | false => simpa [hn, catalogCurrent, h] using previous
  · simpa [catalogCurrent, List.filter_cons, counted] using previous

namespace SnapshotHistory

/-- Faithful snapshot construction and successful catch-up supply these facts. The
second is derived per writer by `completed_catchup_covers_old`: a completed
scan cannot stop at a deleted prefix hole. Strict time handles equal ticks. -/
structure Sound (published : Write → Prop) (s : Snapshot) : Prop where
  causal : ∀ w, published w → s.audience ∈ w.audiences →
    s.covers w s.audience → w.storedAt ≤ s.storedAt
  caught : ∀ w, published w → s.audience ∈ w.audiences →
    w.storedAt < s.caughtAt → s.covers w s.audience

structure State where
  time : Nat
  landed : List Snapshot
  deleted : List Write

def Valid (published : Write → Prop) (order : SnapshotOrder) (s : State) : Prop :=
  (∀ snap ∈ s.landed, Sound published snap) ∧
  ∀ w ∈ s.deleted, published w ∧ Covered (catalogCurrent order s.landed) w ∧
    MatureCoverage s.landed s.time w

/-- Catch-up and reads happen outside the catalog boundary. An arriving
snapshot need not dominate the selection at arrival. Late uploads may land. -/
inductive Step (published : Write → Prop) (order : SnapshotOrder) : State → State → Prop
  | tick (s : State) (time : Nat) (later : s.time ≤ time) : Step published order s { s with time := time }
  | land (s : State) (snap : Snapshot) (now : snap.storedAt = s.time)
      (sound : Sound published snap) : Step published order s { s with landed := snap :: s.landed }
  | delete (s : State) (w : Write) (publishedWrite : published w)
      (active : List Nat) (posted : Nat → Nat → Nat) (final : Prop)
      (eligible : Eligible order s.landed active posted s.time final w) :
      Step published order s { s with deleted := w :: s.deleted }

theorem step_valid {published : Write → Prop} {order : SnapshotOrder} {s t : State}
    (step : Step published order s t) (valid : Valid published order s) : Valid published order t := by
  obtain ⟨sound, deleted⟩ := valid
  cases step with
  | tick time later =>
      refine ⟨sound, ?_⟩
      intro w hw
      obtain ⟨pub, covered, mature⟩ := deleted w hw
      refine ⟨pub, covered, ?_⟩
      intro a ha
      obtain ⟨snap, member, counted, covers, age⟩ := mature a ha
      exact ⟨snap, member, counted, covers, by dsimp; omega⟩
  | land snap now good =>
      refine ⟨?_, ?_⟩
      · intro other member
        rcases List.mem_cons.mp member with rfl | old
        · exact good
        · exact sound other old
      · intro w hw
        obtain ⟨pub, covered, mature⟩ := deleted w hw
        refine ⟨pub, add_preserves_coverage order s.landed snap w covered ?_,
          maturity_survives_new_snapshots s.landed [snap] s.time w mature⟩
        intro counted ha
        obtain ⟨witness, member, _, covers, age⟩ := mature snap.audience ha
        have causal := (sound witness member).causal w pub (covers.1 ▸ ha) (by simpa [covers.1] using covers)
        have admission : snap.storedAt ≤ snap.caughtAt + day := by simpa [Snapshot.counts] using counted
        exact good.caught w pub ha (by omega)
  | delete w publishedWrite active posted final eligible =>
      refine ⟨sound, ?_⟩
      intro other member
      rcases List.mem_cons.mp member with rfl | old
      · exact ⟨publishedWrite, eligible.1.1, eligible.2⟩
      · exact deleted other old

inductive Run (published : Write → Prop) (order : SnapshotOrder) : State → Prop
  | initial : Run published order ⟨0, [], []⟩
  | step {s t : State} : Run published order s → Step published order s t → Run published order t

theorem run_valid {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (run : Run published order s) : Valid published order s := by
  induction run with
  | initial => simp [Valid]
  | step _ step ih => exact step_valid step ih

/-- Arbitrary finite prefixes of an infinite concurrent history satisfy the
same invariant, for all three orders, including slow and late arrivals. -/
theorem deleted_stays_covered {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (run : Run published order s) (w : Write) (deleted : w ∈ s.deleted) :
    Covered (catalogCurrent order s.landed) w := ((run_valid run).2 w deleted).2.1

theorem deleted_miss_detected {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (run : Run published order s) (w : Write) (deleted : w ∈ s.deleted)
    (a : Nat) (ha : a ∈ w.audiences) (position : Nat) (needed : position < w.number) :
    coveredMiss s.landed w.writer position = true :=
  current_detects_deleted order s.landed w (deleted_stays_covered run w deleted) a ha position needed

/-- A current snapshot that cannot reach a deleted number certifies that
the deleted write has no part in this audience. An absent selection does too. -/
theorem deleted_outside_uncovered_audience {published : Write → Prop}
    {order : SnapshotOrder} {s : State} (run : Run published order s)
    (w : Write) (deleted : w ∈ s.deleted) (a : Nat)
    (uncovered : ∀ snap, catalogCurrent order s.landed a = some snap →
      snap.position w.writer < w.number) : a ∉ w.audiences := by
  intro relevant
  have covered := deleted_stays_covered run w deleted a relevant
  cases selected : catalogCurrent order s.landed a with
  | none => simp [selected] at covered
  | some snap =>
      have covers : snap.covers w a := by simpa [selected] using covered
      have := uncovered snap selected
      have := covers.2
      omega

/-- Even if reloading never finishes, loading counting snapshots with
unbounded publication times eventually includes each fixed old write.
This is receipt/application of each snapshot, not an atomic suffix commit. -/
theorem eventual_snapshot_delivery (published : Write → Prop) (loaded : Nat → Snapshot)
    (sound : ∀ n, Sound published (loaded n))
    (counts : ∀ n, (loaded n).counts = true)
    (progress : ∀ bound, ∃ n, bound < (loaded n).storedAt)
    (w : Write) (pub : published w) (audience : ∀ n, (loaded n).audience ∈ w.audiences) :
    ∃ n, (loaded n).covers w (loaded n).audience := by
  obtain ⟨n, later⟩ := progress (w.storedAt + day)
  have admitted : (loaded n).storedAt ≤ (loaded n).caughtAt + day := by
    simpa [Snapshot.counts] using counts n
  exact ⟨n, (sound n).caught w pub (audience n) (by omega)⟩

end SnapshotHistory

/-- A missed prefix number was deleted after this round's selection: the
loaded snapshot already included every deletion known at that selection. -/
theorem reload_requires_new_deletion (loaded : Snapshot) (atSelection : List Write)
    (position : Nat → Nat) (w : Write)
    (covers : ∀ v ∈ atSelection, v.number ≤ loaded.position v.writer)
    (advances : ∀ writer, loaded.position writer ≤ position writer)
    (missingNext : w.number = position w.writer + 1) : w ∉ atSelection := by
  intro member
  have := covers w member
  have := advances w.writer
  omega

/-- A counter counts deletions relevant to the fixed target. A failed round
requires a new deletion; the next selection includes it. -/
theorem reload_round_bound (selected failed : Nat → Nat) (rounds bound : Nat)
    (miss : ∀ r, r < rounds → selected r < failed r)
    (reload : ∀ r, r < rounds → failed r ≤ selected (r + 1))
    (finite : selected rounds ≤ bound) : selected 0 + rounds ≤ bound := by
  have growth : ∀ n, n ≤ rounds → selected 0 + n ≤ selected n := by
    intro n hn
    induction n with
    | zero => simp
    | succ n ih => have := ih (by omega); have := miss n (by omega); have := reload n (by omega); omega
  exact Nat.le_trans (growth rounds (Nat.le_refl _)) finite

/-- After deletions stop, a fresh selection cannot trigger another reload.
Log scans must still finish; deletion quiescence alone does not stop appends. -/
theorem no_reload_after_quiescence (loaded : Snapshot) (deleted : List Write)
    (position : Nat → Nat) (w : Write)
    (covers : ∀ v ∈ deleted, v.number ≤ loaded.position v.writer)
    (advances : ∀ writer, loaded.position writer ≤ position writer)
    (missingNext : w.number = position w.writer + 1) (absent : w ∈ deleted) : False :=
  reload_requires_new_deletion loaded deleted position w covers advances missingNext absent

/-- Triggering writes cannot recur: every later selection includes the
earlier deletion. Restrict misses to a fixed finite target to bound retries. -/
theorem finite_target_reload_bound (triggers target : List Write)
    (newDeletion : ∀ before w after, triggers = before ++ w :: after → w ∉ before)
    (required : triggers ⊆ target) : triggers.length ≤ target.length := by
  have distinct : triggers.Nodup := by
    induction triggers with
    | nil => simp
    | cons head tail ih =>
        refine List.nodup_cons.mpr ⟨?_, ?_⟩
        · intro member
          obtain ⟨before, after, eq⟩ := List.append_of_mem member
          exact newDeletion (head :: before) head after (by simp [eq]) (by simp)
        · apply ih
          · intro before w after eq
            have := newDeletion (head :: before) w after (by simp [eq])
            intro member
            exact this (List.mem_cons_of_mem _ member)
          · intro w hw; exact required (List.mem_cons_of_mem _ hw)
  exact distinct.length_le_of_subset required

namespace SnapshotPublication

/-- One successful retention pass checks every stored write using the same
eligibility decision as individual DELETE actions. Already removed paths
remain removed. Provider failure does not count as a successful pass. -/
def retainWrites (order : SnapshotOrder) (s : State)
    (posted : List (Nat × Nat × Nat)) (final : Write → Bool) : State :=
  { s with deleted := s.writes.filter (fun w => !s.deleted.contains w && eligible order s w posted (final w))
      ++ s.deleted }

/-- The comparison histories give every candidate the same finality verdict. -/
abbrev retainPass (order : SnapshotOrder) (s : State)
    (posted : List (Nat × Nat × Nat)) (final : Bool) : State :=
  retainWrites order s posted (fun _ => final)

theorem due_write_deleted (order : SnapshotOrder) (s : State)
    (posted : List (Nat × Nat × Nat)) (final : Write → Bool) (w : Write)
    (published : w ∈ s.writes) (due : eligible order s w posted (final w) = true) :
    w ∈ (retainWrites order s posted final).deleted := by
  by_cases removed : w ∈ s.deleted
  · exact List.mem_append_right _ removed
  · apply List.mem_append_left
    exact List.mem_filter.mpr ⟨published, by simp [removed, due]⟩

theorem nonfinal_write_kept (order : SnapshotOrder) (s : State)
    (posted : List (Nat × Nat × Nat)) (final : Write → Bool) (w : Write)
    (present : w ∉ s.deleted) (nonfinal : final w = false) :
    w ∉ (retainWrites order s posted final).deleted := by
  intro removed
  rcases List.mem_append.mp removed with fresh | old
  · have check := (List.mem_filter.mp fresh).2
    simp only [Bool.and_eq_true] at check
    have due : Eligible order s.landed s.writers (postedPosition posted) s.time (final w = true) w := by
      simpa [eligible] using check.2
    have yes := due.1.2.1
    simp [nonfinal] at yes
  · exact present old

theorem due_pass_deletes (order : SnapshotOrder) (s : State)
    (posted : List (Nat × Nat × Nat)) (final : Bool) (w : Write)
    (published : w ∈ s.writes) (due : eligible order s w posted final = true) :
    w ∈ (retainPass order s posted final).deleted :=
  due_write_deleted order s posted (fun _ => final) w published due

theorem uncovered_pass_keeps (order : SnapshotOrder) (s : State)
    (posted : List (Nat × Nat × Nat)) (final : Bool) (w : Write)
    (present : w ∉ s.deleted) (uncovered : ¬ Covered (catalogCurrent order s.landed) w) :
    w ∉ (retainPass order s posted final).deleted := by
  intro removed
  rcases List.mem_append.mp removed with fresh | old
  · have check := (List.mem_filter.mp fresh).2
    simp only [Bool.and_eq_true] at check
    have due : Eligible order s.landed s.writers (postedPosition posted) s.time (final = true) w := by
      simpa [eligible] using check.2
    exact uncovered due.1.1
  · exact present old

/-- The deadline includes coverage stability, finality/reader gates, a bound
on pass spacing, and a bound on successful DELETE completion. New snapshot
publication does not restart a historical covering witness's age. -/
theorem scheduled_deletion_bound (order : SnapshotOrder) (s : State)
    (posted : List (Nat × Nat × Nat)) (w : Write) (witnessBound ready passGap latency : Nat)
    (published : w ∈ s.writes)
    (witnesses : ∀ a ∈ w.audiences, ∃ snap ∈ s.landed,
      snap.counts = true ∧ snap.covers w a ∧ snap.storedAt ≤ witnessBound)
    (current : Covered (catalogCurrent order s.landed) w)
    (gate : (∀ reader ∈ s.writers, w.number ≤ postedPosition posted reader w.writer) ∨
      w.storedAt + month ≤ s.time)
    (matureAtReady : witnessBound + day < ready)
    (scheduled : ready ≤ s.time ∧ s.time ≤ ready + passGap)
    (removed : Nat → Write → Prop)
    (completion : ∀ v ∈ (retainPass order s posted true).deleted, removed (s.time + latency) v) :
    ∃ time, time ≤ ready + passGap + latency ∧ removed time w := by
  have due : eligible order s w posted true = true := by
    simp only [eligible, decide_eq_true_eq, Eligible, Deletable]
    refine ⟨⟨current, trivial, gate⟩, ?_⟩
    intro a ha
    obtain ⟨snap, member, counted, covers, age⟩ := witnesses a ha
    exact ⟨snap, member, counted, covers, by omega⟩
  exact ⟨s.time + latency, by omega,
    completion w (due_pass_deletes order s posted true w published due)⟩

end SnapshotPublication

end CovenIO
