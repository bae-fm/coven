import CovenQueue.Progress

namespace CovenQueue.RaiseUpload

/-- A fixed captured raise. `through` includes the migration's empty marker,
not the app writes committed after the capture. Remote positions were read
from published logs or previously publishable snapshots; IO proves that
induction across snapshots. -/
structure Plan where
  through : Nat
  covers : List WriteId
  snapshot : Bytes
  entry : Bytes
  deriving DecidableEq, Repr

inductive Stage where
  | captured | snapshotRequested | snapshotStored | entryRequested | entryStored
  deriving DecidableEq, Repr

structure State where
  world : World
  stage : Stage

def storedSnapshot (p : Plan) (s : State) : Option Bytes :=
  match s.stage with
  | .snapshotStored | .entryRequested | .entryStored => some p.snapshot
  | _ => none

def storedEntry (p : Plan) (s : State) : Option Bytes :=
  match s.stage with
  | .entryStored => some p.entry
  | _ => none

theorem entry_has_snapshot (p : Plan) (s : State) (bytes : Bytes)
    (stored : storedEntry p s = some bytes) :
    bytes = p.entry ∧ storedSnapshot p s = some p.snapshot := by
  cases stage : s.stage <;> simp [storedEntry, stage] at stored
  exact ⟨stored.symm, by simp [storedSnapshot, stage]⟩

/-- Remote confirmation means retained publication evidence, not continued
physical presence: a covered log object can already have been deleted. -/
def Confirmed (published : WriteId → Prop) (p : Plan) (s : World) : Prop :=
  p.through ≤ s.queue.settled ∧
    ∀ id ∈ p.covers, if id.device = s.queue.device then
      0 < id.number ∧ id.number ≤ p.through else published id

/-- Requests and landings are distinct. Crashes and lost replies can occur
between them; snapshot storage precedes its entry request.
Every queue event uses the upload model, which never rewrites queued records. -/
inductive Step (encode : Seal) (published : WriteId → Prop) (p : Plan) : State → State → Prop
  | queue {s : State} {next : World} (step : CovenQueue.Step encode s.world next) :
      Step encode published p s { s with world := next }
  | requestSnapshot (s : State) (atCapture : s.stage = .captured)
      (confirmed : Confirmed published p s.world) :
      Step encode published p s { s with stage := .snapshotRequested }
  | landSnapshot (s : State) (sent : s.stage = .snapshotRequested) :
      Step encode published p s { s with stage := .snapshotStored }
  | requestEntry (s : State) (snapshot : s.stage = .snapshotStored) :
      Step encode published p s { s with stage := .entryRequested }
  | landEntry (s : State) (sent : s.stage = .entryRequested) :
      Step encode published p s { s with stage := .entryStored }

inductive Run (encode : Seal) (published : WriteId → Prop) (p : Plan) (start : World) : State → Prop
  | initial : Run encode published p start ⟨start, .captured⟩
  | step {s t} : Run encode published p start s → Step encode published p s t →
      Run encode published p start t

/-- The certificate records confirmation before the first snapshot request.
Later app commits, crashes and late landings cannot revoke it. -/
theorem publication_after_confirmation {encode : Seal} {published : WriteId → Prop}
    {p : Plan} {start : World} {s : State} (run : Run encode published p start s)
    (visible : s.stage ≠ .captured) :
    ∃ atRequest, Confirmed published p atRequest ∧
      atRequest.queue.settled ≤ s.world.queue.settled := by
  induction run with
  | initial => exact False.elim (visible rfl)
  | @step a b run step ih =>
    cases step with
    | queue step =>
        obtain ⟨atRequest, confirmed, earlier⟩ := ih visible
        exact ⟨atRequest, confirmed, Nat.le_trans earlier (step_monotone step).1⟩
    | requestSnapshot captured confirmed => exact ⟨_, confirmed, Nat.le_refl _⟩
    | landSnapshot sent => exact ih (by rw [sent]; decide)
    | requestEntry stored => exact ih (by rw [stored]; decide)
    | landEntry sent => exact ih (by rw [sent]; decide)

theorem run_reachable {encode : Seal} {published : WriteId → Prop} {p : Plan}
    {start : World} {s : State} {device : Nat}
    (initial : Reachable encode device start) (run : Run encode published p start s) :
    Reachable encode device s.world := by
  induction run with
  | initial => exact initial
  | step run step ih =>
    cases step with
    | queue step => exact .next ih step
    | requestSnapshot | landSnapshot | requestEntry | landEntry => exact ih

/-- In particular this holds as soon as the entry is remotely visible, before
its author learns whether replay keeps it. No empty-queue precondition appears. -/
theorem published_raise_covers_published {encode : Seal} {published : WriteId → Prop}
    {p : Plan} {start : World} {s : State} {device : Nat}
    (initial : Reachable encode device start) (run : Run encode published p start s)
    (visible : s.stage = .entryStored) :
    ∃ atRequest, Confirmed published p atRequest ∧
      ∀ id ∈ p.covers, if id.device = atRequest.queue.device then
        ∃ bytes, atRequest.stored id.number = some bytes else published id := by
  -- Retain the request's reachability, not only its numeric confirmation.
  have certificate : ∀ {t : State}, Run encode published p start t → t.stage ≠ .captured →
      ∃ q, Confirmed published p q ∧ Reachable encode device q := by
    intro t history ready
    induction history with
    | initial => exact False.elim (ready rfl)
    | step history step ih =>
      cases step with
      | queue => exact ih ready
      | requestSnapshot captured confirmed => exact ⟨_, confirmed, run_reachable initial history⟩
      | landSnapshot sent => exact ih (by rw [sent]; decide)
      | requestEntry stored => exact ih (by rw [stored]; decide)
      | landEntry sent => exact ih (by rw [sent]; decide)
  obtain ⟨q, confirmed, reachable⟩ := certificate run (by rw [visible]; decide)
  refine ⟨q, confirmed, ?_⟩
  intro id member
  have fact := confirmed.2 id member
  split at fact
  · rename_i own
    rw [ite_eq_left own]
    exact (reachable_invariants reachable).2 id.number fact.1 (Nat.le_trans fact.2 confirmed.1)
  · rename_i remote
    rw [ite_eq_right remote]
    exact fact

/-- Local migration appends its empty marker; every existing pending record,
including untried plaintext, is retained byte for byte. -/
def migrate (q : Queue) (version : Nat) (identity : Identity) : Queue :=
  q.commit ⟨identity, version, .migration, []⟩

theorem migration_preserves_waiting (q : Queue) (version : Nat) (identity : Identity) :
    q.waiting <+: (migrate q version identity).waiting := ⟨_, rfl⟩

theorem post_migration_behind_marker (q : Queue) (version : Nat) (identity : Identity) (r : Record) :
    ((migrate q version identity).commit r).waiting = q.waiting ++
      [.untried ⟨{ identity with id := ⟨q.device, q.reserved + 1⟩ }, version, .migration, []⟩,
       .untried { r with identity := { r.identity with id := ⟨q.device, q.reserved + 2⟩ } }] := by
  simp [migrate, Queue.commit, Queue.reserved, List.append_assoc, Nat.add_assoc]

/-- The old-schema head remains first-sendable while the store is old, even
though the app has migrated. Keys/identity and provider success are separate. -/
theorem old_schema_head_enabled (encode : Seal) (s : World) (app store publication f : Nat)
    (requests : List KeyRequest) (keys : List Key) (r : Record) (rest : List Pending)
    (head : s.queue.waiting = .untried r :: rest) (updated : store ≤ app)
    (old : r.version ≤ store) (chosen : chooseKeys requests = some keys)
    (header : ∃ h ps, requests = h :: ps ∧ h.audience = 0 ∧ h.currentMember = true) :
    CovenQueue.Step encode s { s with queue := s.queue.prepare f (keys.map Key.id) } :=
  .prepare s f requests keys app store publication r rest head updated
    (old_uploadable _ _ _ old) chosen header

theorem migration_marker_enabled (store publication version : Nat) (id : Identity) :
    Uploadable store publication ⟨id, version, .migration, []⟩ := Or.inl rfl

/-- An app write at the latest local migration's version sends only once a
raise to that version is kept. -/
theorem new_schema_requires_kept (r : Record) (kept publication : Nat)
    (appWrite : r.disposition = .ordinary) (current : r.version = publication)
    (migrated : 0 < publication) (enabled : Uploadable kept publication r) :
    r.version ≤ kept := by
  rcases enabled with marker | none | older | raised
  · rw [appWrite] at marker; cases marker
  · omega
  · omega
  · omega

end CovenQueue.RaiseUpload
