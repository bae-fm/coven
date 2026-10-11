import CovenIO.Storage

namespace CovenIO

def day : Nat := 24 * 60 * 60
def month : Nat := 30 * day
def freshFor : Nat := 5 * 60

structure Write where
  writer : Nat
  number : Nat
  storedAt : Nat
  audiences : List Nat
  deriving DecidableEq, Repr

structure Snapshot where
  audience : Nat
  writer : Nat
  number : Nat
  storedAt : Nat
  caughtAt : Nat
  positions : List (Nat × Nat)
  deriving DecidableEq, Repr

def Snapshot.position (s : Snapshot) (writer : Nat) : Nat :=
  ((s.positions.filter (fun p => p.1 == writer)).map Prod.snd).foldl max 0

/-- Prefix positions have one entry per writer in the executable histories. -/
def Snapshot.writeCount (s : Snapshot) : Nat := (s.positions.map Prod.snd).sum

inductive SnapshotOrder where
  | writes | catchup | storage
  deriving DecidableEq, Repr

def Snapshot.rank (order : SnapshotOrder) (s : Snapshot) : Nat :=
  match order with
  | .writes => s.writeCount
  | .catchup => s.caughtAt
  | .storage => s.storedAt

/-- A smaller path breaks ties for all three proposed orderings. -/
def newer (order : SnapshotOrder) (a b : Snapshot) : Bool :=
  decide (b.rank order < a.rank order ∨ (a.rank order = b.rank order ∧
    (a.writer < b.writer ∨ (a.writer = b.writer ∧ a.number < b.number))))

def newest (order : SnapshotOrder) : List Snapshot → Option Snapshot
  | [] => none
  | s :: rest => match newest order rest with
    | none => some s
    | some other => some (if newer order s other then s else other)

def Snapshot.counts (s : Snapshot) : Bool := decide (s.storedAt ≤ s.caughtAt + day)

def advances (s : Snapshot) (localPosition : Nat → Nat) : Bool :=
  s.positions.any fun p => decide (localPosition p.1 < p.2)

def Snapshot.covers (s : Snapshot) (w : Write) (a : Nat) : Prop :=
  s.audience = a ∧ w.number ≤ s.position w.writer

instance (s : Snapshot) (w : Write) (a : Nat) : Decidable (s.covers w a) :=
  inferInstanceAs (Decidable (s.audience = a ∧ w.number ≤ s.position w.writer))

def Dominates (new old : Snapshot) : Prop :=
  new.audience = old.audience ∧ ∀ w, old.position w ≤ new.position w

abbrev Current := Nat → Option Snapshot

def Covered (current : Current) (w : Write) : Prop :=
  ∀ a ∈ w.audiences, match current a with
    | none => False
    | some snap => snap.covers w a

instance (current : Current) (w : Write) : Decidable (Covered current w) := by
  unfold Covered
  letI (a : Nat) : Decidable (match current a with
      | none => False | some snap => snap.covers w a) := by
    cases current a <;> infer_instance
  infer_instance

/-- Current selection filters late uploads before ranking or loading. -/
def catalogCurrent (order : SnapshotOrder) (snapshots : List Snapshot) : Current :=
  fun audience => newest order (snapshots.filter (fun s => s.audience == audience && s.counts))

/-- This is checked at preparation, never atomically at remote publication. -/
def CanSnapshot (current : Current) (snap : Snapshot) : Prop :=
  ∀ old, current snap.audience = some old → Dominates snap old

/-- The existing §15 prerequisites, also used by the recent-reader proofs.
The additional snapshot age condition is `MatureCoverage`. -/
def Deletable (current : Current) (active : List Nat)
    (posted : Nat → Nat → Nat) (time : Nat) (final : Prop) (w : Write) : Prop :=
  Covered current w ∧ final ∧
    ((∀ reader ∈ active, w.number ≤ posted reader w.writer) ∨ w.storedAt + month ≤ time)

/-- “Once a snapshot ... landed” uses historical counting publications.
Removing or superseding that snapshot does not restart its age. -/
def MatureCoverage (landed : List Snapshot) (time : Nat) (w : Write) : Prop :=
  ∀ a ∈ w.audiences, ∃ snap ∈ landed,
    snap.counts = true ∧ snap.covers w a ∧ snap.storedAt + day < time

def Eligible (order : SnapshotOrder) (landed : List Snapshot) (active : List Nat)
    (posted : Nat → Nat → Nat) (time : Nat) (final : Prop) (w : Write) : Prop :=
  Deletable (catalogCurrent order landed) active posted time final w ∧
    MatureCoverage landed time w

instance (landed : List Snapshot) (time : Nat) (w : Write) :
    Decidable (MatureCoverage landed time w) := by
  unfold MatureCoverage
  infer_instance

instance (current : Current) (active : List Nat) (posted : Nat → Nat → Nat)
    (time : Nat) (final : Prop) [Decidable final] (w : Write) :
    Decidable (Deletable current active posted time final w) := by
  unfold Deletable
  infer_instance

instance (order : SnapshotOrder) (landed : List Snapshot) (active : List Nat)
    (posted : Nat → Nat → Nat) (time : Nat) (final : Prop) [Decidable final] (w : Write) :
    Decidable (Eligible order landed active posted time final w) := by
  unfold Eligible
  infer_instance

theorem late_snapshot_ignored (order : SnapshotOrder) (snap : Snapshot) (rest : List Snapshot)
    (late : snap.caughtAt + day < snap.storedAt) :
    catalogCurrent order (snap :: rest) = catalogCurrent order rest := by
  funext audience
  simp [catalogCurrent, Snapshot.counts, Nat.not_le.mpr late]

theorem old_upload_before_deletion (snap witness : Snapshot) (time : Nat)
    (counts : snap.counts = true) (old : snap.caughtAt ≤ witness.storedAt)
    (mature : witness.storedAt + day < time) : snap.storedAt < time := by
  simp only [Snapshot.counts, decide_eq_true_eq] at counts
  omega

theorem symmetric_in_flight_impossible (a b : Snapshot) (deleteA deleteB : Nat)
    (ca : a.counts = true) (cb : b.counts = true)
    (oldA : a.caughtAt ≤ b.storedAt) (oldB : b.caughtAt ≤ a.storedAt)
    (matureA : a.storedAt + day < deleteA) (matureB : b.storedAt + day < deleteB)
    (late : deleteB ≤ a.storedAt ∨ deleteA ≤ b.storedAt) : False := by
  have ha := old_upload_before_deletion a b deleteB ca oldA matureB
  have hb := old_upload_before_deletion b a deleteA cb oldB matureA
  rcases late with lateA | lateB
  · exact Nat.not_lt_of_ge lateA ha
  · exact Nat.not_lt_of_ge lateB hb

theorem maturity_survives_new_snapshots (old added : List Snapshot) (time : Nat) (w : Write)
    (mature : MatureCoverage old time w) : MatureCoverage (added ++ old) time w := by
  intro a ha
  obtain ⟨snap, member, counted, covered, aged⟩ := mature a ha
  exact ⟨snap, List.mem_append_right _ member, counted, covered, aged⟩

/-- Once the current selection covers the write, new publications cannot
reset the historical witness's deadline. Finality and positions remain required. -/
theorem eligible_after_day (order : SnapshotOrder) (landed : List Snapshot)
    (active : List Nat) (posted : Nat → Nat → Nat) (time bound : Nat) (w : Write)
    (witnesses : ∀ a ∈ w.audiences, ∃ snap ∈ landed,
      snap.counts = true ∧ snap.covers w a ∧ snap.storedAt ≤ bound)
    (current : Covered (catalogCurrent order landed) w)
    (passed : ∀ reader ∈ active, w.number ≤ posted reader w.writer)
    (age : bound + day < time) : Eligible order landed active posted time True w := by
  refine ⟨⟨current, trivial, Or.inl passed⟩, ?_⟩
  intro a ha
  obtain ⟨snap, member, counted, covered, stored⟩ := witnesses a ha
  exact ⟨snap, member, counted, covered, by omega⟩

/-- The reader is active and its posted position never claims unconsumed work. -/
theorem recent_write_protected (current : Current) (active : List Nat)
    (posted : Nat → Nat → Nat) (time checkpoint reader consumed : Nat) (final : Prop) (w : Write)
    (member : reader ∈ active) (honest : posted reader w.writer ≤ consumed)
    (unconsumed : consumed < w.number) (afterCheckpoint : checkpoint ≤ w.storedAt)
    (recent : time < checkpoint + month) :
    ¬ Deletable current active posted time final w := by
  rintro ⟨_, _, readers | aged⟩
  · have := readers reader member; omega
  · omega

def checkpoint (old : Option Nat) (sample : Nat) (history post : Bool) : Option Nat :=
  if history && post then some sample else old

theorem unresolved_preserves_checkpoint (old : Option Nat) (sample : Nat) (post : Bool) :
    checkpoint old sample false post = old := by simp [checkpoint]

/-- §15: evaluate at every miss, with monotonic elapsed time including sleep. -/
def needsSnapshots (saved : Option Nat) (observed elapsed : Nat) : Bool :=
  match saved with | none => true | some s => decide (s + 29 * day ≤ observed + elapsed)

theorem exact_boundary_discovers (s : Nat) : needsSnapshots (some s) (s + 29 * day) 0 = true := by
  simp [needsSnapshots]

/-- The one-day margin accommodates at most a day of sample age. Relating
provider clock advance to elapsed time is explicit, not implied by monotonicity. -/
theorem miss_before_retention (checkpoint observed elapsed storageTime : Nat)
    (recent : needsSnapshots (some checkpoint) observed elapsed = false)
    (clockBound : storageTime ≤ observed + elapsed + day) : storageTime < checkpoint + month := by
  simp only [needsSnapshots, decide_eq_false_iff_not, Nat.not_le] at recent
  unfold month day at *
  omega

namespace SnapshotPublication

/-- Each session contains fully applied reads. Failed or unresolved reads
have no successful transition. The clock sample precedes every read. -/
structure Catchup where
  snapshot : Snapshot
  missed : List Nat
  lastRead : Nat
  deriving DecidableEq, Repr

structure State where
  time : Nat
  writers : List Nat
  writes : List Write
  deleted : List Write
  landed : List Snapshot
  catching : List Catchup
  uploading : List Snapshot
  deriving DecidableEq, Repr

def initial (writers : List Nat) : State := ⟨0, writers, [], [], [], [], []⟩

def live (s : State) (writer number : Nat) : Option Write :=
  s.writes.find? fun w => w.writer == writer && w.number == number && !s.deleted.contains w

def current (order : SnapshotOrder) (s : State) : Current := catalogCurrent order s.landed

def samePath (a b : Snapshot) : Bool :=
  a.audience == b.audience && a.writer == b.writer && a.number == b.number

def dominates (a b : Snapshot) : Bool :=
  a.audience == b.audience && b.positions.all (fun p => decide (p.2 ≤ a.position p.1))

def canPrepare (order : SnapshotOrder) (s : State) (snap : Snapshot) : Bool :=
  match current order s snap.audience with
  | none => true
  | some old => dominates snap old

def postedPosition (posted : List (Nat × Nat × Nat)) (reader writer : Nat) : Nat :=
  ((posted.filter (fun p => p.1 == reader && p.2.1 == writer)).map (fun p => p.2.2)).foldl max 0

def eligible (order : SnapshotOrder) (s : State) (w : Write)
    (posted : List (Nat × Nat × Nat)) (final : Bool) : Bool :=
  decide (Eligible order s.landed s.writers (postedPosition posted) s.time (final = true) w)

inductive Action where
  | write (writer : Nat) (audiences : List Nat)
  | beginCatchup (audience writer number : Nat)
  | read (snapshotWriter logWriter : Nat)
  | prepare (writer : Nat)
  | land (audience writer number : Nat)
  | delete (writer number : Nat) (posted : List (Nat × Nat × Nat)) (final : Bool)
  | tick
  deriving DecidableEq, Repr

/-- Requests are ordered independently of time ticks. Preparation checks the
current snapshot after every writer's terminal miss; landing has no precheck.
`uploading` includes abandoned requests that may still publish remotely. -/
def act (order : SnapshotOrder) (s : State) : Action → Option State
  | .write writer audiences =>
      if s.writers.contains writer then
        let number := (s.writes.filter (fun w => w.writer == writer)).length + 1
        some { s with writes := ⟨writer, number, s.time, audiences⟩ :: s.writes }
      else none
  | .beginCatchup audience writer number =>
      if s.writers.contains writer && !(s.catching.any (fun c => c.snapshot.writer == writer)) then
        let positions := match current order s audience with
          | none => []
          | some snap => snap.positions
        let snap : Snapshot := ⟨audience, writer, number, s.time, s.time, positions⟩
        if s.landed.any (samePath snap) || s.uploading.any (samePath snap) then none
        else some { s with catching := ⟨snap, [], s.time⟩ :: s.catching }
      else none
  | .read writer logWriter => do
      let c ← s.catching.find? (fun c => c.snapshot.writer == writer)
      if !s.writers.contains logWriter then none else
      let position := c.snapshot.position logWriter
      let next := match live s logWriter (position + 1) with
        | none => { c with missed := logWriter :: c.missed, lastRead := s.time }
        | some _ => { c with
            snapshot := { c.snapshot with positions :=
              (logWriter, position + 1) :: c.snapshot.positions.filter (fun p => p.1 != logWriter) }
            missed := c.missed.filter (· != logWriter), lastRead := s.time }
      some { s with catching := next :: s.catching.filter (fun c => c.snapshot.writer != writer) }
  | .prepare writer => do
      let c ← s.catching.find? (fun c => c.snapshot.writer == writer)
      if c.lastRead == s.time && s.writers.all c.missed.contains &&
          canPrepare order s c.snapshot then
        some { s with
          catching := s.catching.filter (fun c => c.snapshot.writer != writer)
          uploading := { c.snapshot with storedAt := s.time } :: s.uploading }
      else none
  | .land audience writer number => do
      let snap ← s.uploading.find? (fun snap =>
        snap.audience == audience && snap.writer == writer && snap.number == number)
      some { s with
        landed := { snap with storedAt := s.time } :: s.landed
        uploading := s.uploading.filter (fun s => !samePath s snap) }
  | .delete writer number posted final => do
      let w ← live s writer number
      if eligible order s w posted final then some { s with deleted := w :: s.deleted } else none
  | .tick => some s

def step (order : SnapshotOrder) (s : State) (time : Nat) (action : Action) : Option State :=
  if s.time ≤ time then act order { s with time := time } action else none

def execute (order : SnapshotOrder) : State → List (Nat × Action) → Option State
  | s, [] => some s
  | s, (time, action) :: rest => (step order s time action).bind (fun next => execute order next rest)

def Run (order : SnapshotOrder) (writers : List Nat) (s : State) : Prop :=
  ∃ events, execute order (initial writers) events = some s

theorem run_append (order : SnapshotOrder) (s : State) (a b : List (Nat × Action)) :
    execute order s (a ++ b) = (execute order s a).bind (fun next => execute order next b) := by
  induction a generalizing s with
  | nil => rfl
  | cons head tail ih =>
      simp only [List.cons_append, execute]
      cases step order s head.1 head.2 <;> simp [ih]

/-- Deletion needs an explicit successful action. The coverage rule does
not supply a bound for scheduling or provider completion. -/
theorem tick_preserves_deleted (order : SnapshotOrder) (s : State) (time : Nat)
    (later : s.time ≤ time) :
    step order s time .tick = some { s with time := time } := by
  simp [step, later, act]

end SnapshotPublication
end CovenIO
