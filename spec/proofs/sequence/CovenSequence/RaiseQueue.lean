import CovenSequence.QueueReload
import CovenSequence.ReaderExamples

/-! A migration captures originals through its marker. Publication confirms
that prefix before the raise's snapshot is taken, uploaded and announced and
the raise is claimed, as §18.5 requires. Later writes wait without extending
the capture or preventing its confirmation.

The marker is the migration write's record, and it carries no part: its slot
changes no audience, and the migration's changes reach other devices only in
the raise's body (§18.4). An app update that only adds tables or columns
appends no marker; a capture whose snapshot isn't taken yet moves to the
update's version, taken again through the newest write (§18.5). -/
namespace CovenSequence.RaiseQueue

variable {S : Sig} {Stamp : Type}
open QueueReload (Write)

inductive Phase where
  | ordinary
  | captured (count version : Nat)
  | confirmed (version : Nat)
  | taken (version : Nat)
  | uploaded (version : Nat)
  | announced (version : Nat)
  deriving DecidableEq

structure State (S : Sig) (Stamp : Type) where
  queue : List (Write S Stamp)
  published : List (Write S Stamp)
  version : Nat
  phase : Phase

def capture (s : State S Stamp) (marker : Write S Stamp) : State S Stamp :=
  { s with queue := s.queue ++ [marker], phase := .captured (s.queue.length + 1) marker.schema }

def append (s : State S Stamp) (writes : List (Write S Stamp)) : State S Stamp :=
  { s with queue := s.queue ++ writes }

def sendable (s : State S Stamp) : List (Write S Stamp) :=
  match s.phase with
  | .ordinary => s.queue.takeWhile (fun w => decide (w.schema ≤ s.version))
  | .captured count _ => s.queue.take count
  | .confirmed _ | .taken _ | .uploaded _ | .announced _ => []

/-- Only confirmed storage publication removes this prefix from the queue.
Its original records, including attempted bytes, become the published records. -/
def confirm (s : State S Stamp) : State S Stamp :=
  match s.phase with
  | .captured count v =>
    { s with queue := s.queue.drop count, published := s.published ++ s.queue.take count,
             phase := .confirmed v }
  | _ => s

/-- The raise's snapshot taken from the device's rows at the capture's
version, after confirming the captured write slot. -/
def take (s : State S Stamp) : State S Stamp :=
  match s.phase with
  | .confirmed v => { s with phase := .taken v }
  | _ => s

/-- Successful fixed-body upload of the taken snapshot. -/
def upload (s : State S Stamp) : State S Stamp :=
  match s.phase with
  | .taken v => { s with phase := .uploaded v }
  | _ => s

/-- Successful preparation announcement, after its body upload. -/
def announce (s : State S Stamp) : State S Stamp :=
  match s.phase with
  | .uploaded v => { s with phase := .announced v }
  | _ => s

/-- Apply the matching raise after its preparation announcement. Storage
validation of that entry is the boundary rule in Model. -/
def activate (s : State S Stamp) : State S Stamp :=
  match s.phase with
  | .announced v => { s with version := v, phase := .ordinary }
  | _ => s

/-- Later appends cannot change which bytes are sent for the capture. -/
theorem captured_prefix (s : State S Stamp) (marker : Write S Stamp) (later : List (Write S Stamp)) :
    sendable (append (capture s marker) later) = s.queue ++ [marker] := by
  change (s.queue ++ [marker] ++ later).take (s.queue.length + 1) = _
  rw [show s.queue.length + 1 = (s.queue ++ [marker]).length by simp]
  exact List.take_left

/-- Identity/key admission is checked on the captured sendable prefix. An
ineligible later write cannot delay that prefix's publication. -/
theorem later_writes_do_not_delay (s : State S Stamp) (marker : Write S Stamp)
    (later : List (Write S Stamp)) (admitted : Write S Stamp → Prop) :
    (∀ w ∈ sendable (append (capture s marker) later), admitted w) ↔
      (∀ w ∈ s.queue ++ [marker], admitted w) := by rw [captured_prefix]

/-- Confirmation consumes the capture exactly once and preserves the tail,
whether empty or continuously extended by later commits. -/
theorem confirm_captured (s : State S Stamp) (marker : Write S Stamp) (later : List (Write S Stamp)) :
    (confirm (append (capture s marker) later)).published = s.published ++ (s.queue ++ [marker]) ∧
    (confirm (append (capture s marker) later)).queue = later ∧
    (confirm (append (capture s marker) later)).phase = .confirmed marker.schema := by
  change s.published ++ (s.queue ++ [marker] ++ later).take (s.queue.length + 1) = _ ∧
    (s.queue ++ [marker] ++ later).drop (s.queue.length + 1) = later ∧ _
  rw [show s.queue.length + 1 = (s.queue ++ [marker]).length by simp]
  rw [List.take_left, List.drop_left]
  exact ⟨rfl, rfl, rfl⟩

theorem confirmation_not_repeated (s : State S Stamp) (v : Nat) (ready : s.phase = .confirmed v) :
    confirm s = s := by simp [confirm, ready]

/-- Activating the raise never waits for an empty tail. It admits the captured
version and leaves every later original queued for ordinary sending. -/
theorem raise_without_waiting (s : State S Stamp) (marker : Write S Stamp) (later : List (Write S Stamp)) :
    let ready := confirm (append (capture s marker) later)
    (activate (announce (upload (take ready)))).version = marker.schema ∧
    (activate (announce (upload (take ready)))).queue = later ∧
    (activate (announce (upload (take ready)))).published = s.published ++ (s.queue ++ [marker]) := by
  have h := confirm_captured s marker later
  simp [activate, announce, upload, take, h.2.2, h.2.1, h.1]

theorem tail_waits (s : State S Stamp) (marker : Write S Stamp) (later : List (Write S Stamp)) :
    sendable (confirm (append (capture s marker) later)) = [] := by
  simp [sendable, (confirm_captured s marker later).2.2]

/-- Taking and uploading the snapshot cannot start from an unconfirmed
capture. After confirmation they need no condition on the remaining queue. -/
theorem preparation_waits_for_confirmation (s : State S Stamp) (marker : Write S Stamp)
    (later : List (Write S Stamp)) :
    take (append (capture s marker) later) = append (capture s marker) later ∧
    upload (append (capture s marker) later) = append (capture s marker) later ∧
    (upload (take (confirm (append (capture s marker) later)))).phase = .uploaded marker.schema := by
  exact ⟨rfl, rfl, by simp [upload, take, (confirm_captured s marker later).2.2]⟩

theorem activation_waits_for_announcement (s : State S Stamp) (v : Nat)
    (confirmed : s.phase = .confirmed v) :
    activate s = s ∧ activate (upload (take s)) = upload (take s) := by
  simp [activate, upload, take, confirmed]

/-- One sequence of confirmed originals survives every capture/confirmation/
activation, in order. This equality preserves numbers, timestamps, schema,
read-through and complete bytes together. -/
theorem originals_preserved (s : State S Stamp) (marker : Write S Stamp) (later : List (Write S Stamp)) :
    let result := activate (announce (upload (take (confirm (append (capture s marker) later)))))
    result.published ++ result.queue = s.published ++ s.queue ++ [marker] ++ later := by
  have h := raise_without_waiting s marker later
  simp only [h.2.1, h.2.2, List.append_assoc]

/-- A later breaking migration captures intervening originals and both
markers, so an earlier schema wait cannot deadlock the newer raise. -/
theorem recapture_includes_waiting (s : State S Stamp) (first second : Write S Stamp)
    (between later : List (Write S Stamp)) :
    sendable (append (capture (append (capture s first) between) second) later) =
      s.queue ++ [first] ++ between ++ [second] := by
  rw [captured_prefix]
  rfl

/-- Every receiver, including the original author rebuilding after reload,
interprets a late original with its authored schema and the applied version.
Conversion is not an operation on the publication queue. -/
theorem late_original_converted (R : Rules S) (A : S.Aud) (rows : S.Rows) (v : Nat)
    (w : Write S Stamp) (batch : S.Batch) (part : w.bytes A = some batch) :
    QueueReload.applyOriginal R A v rows w = R.merge rows (R.convert w.schema v batch) := by
  simp [QueueReload.applyOriginal, part]

/-- Compose the queue interpretation with the actual next sequence slot.
The authored schema selects conversion even when publication is after a raise. -/
theorem reload_matches_receiver (R : Rules S) (seq : Nat → Slot S) (A : S.Aud)
    (k g : Nat) (w : Write S Stamp) (batch : S.Batch) (claims : List S.Key)
    (parts : S.Aud → Option (WPart S)) (original : w.bytes A = some batch)
    (part : parts A = some ⟨w.schema, claims, batch, w.readThrough⟩)
    (slot : seq (k + 1) = .writes w.device g parts)
    (applied : AppliedTo R seq A (k + 1))
    (resetKeeps : R.sinceReset (audAt R seq A k).lastReset batch = batch) :
    rowsAt R seq A (k + 1) =
      QueueReload.applyOriginal R A (audAt R seq A k).version (rowsAt R seq A k) w := by
  rw [rowsAt_other R seq A k (by intros; simp [slot])]
  have valid := applied.2
  simp only [Nat.add_sub_cancel, slot] at valid
  simp [slot, touches, part, valid, applyWrite, resetKeeps, QueueReload.applyOriginal, original]

/-- An app update that only adds tables or columns, to version `v`, appends no
marker. A capture whose snapshot isn't taken yet moves to `v`, taken again
through the device's newest write (Rust `raise_capture::raise_to`). A
snapshot already taken goes out at the version it was taken at. With no
capture waiting, the update changes nothing here. -/
def addition (s : State S Stamp) (v : Nat) : State S Stamp :=
  match s.phase with
  | .captured _ _ | .confirmed _ => { s with phase := .captured s.queue.length v }
  | _ => s

/-- A capture whose snapshot isn't taken yet: its writes still wait to land,
or they landed and the snapshot isn't taken. -/
def Untaken (s : State S Stamp) : Prop :=
  (∃ count v, s.phase = .captured count v) ∨ ∃ v, s.phase = .confirmed v

/-- **An addition captures through the newest write.** The writes queued when
the update ran, the ones after the marker included, go out before the raise,
and writes after the update don't change what does. -/
theorem addition_sends_newest (s : State S Stamp) (v : Nat) (later : List (Write S Stamp))
    (untaken : Untaken s) :
    sendable (append (addition s v) later) = s.queue := by
  have phase : (addition s v).phase = .captured s.queue.length v := by
    rcases untaken with ⟨count, w, h⟩ | ⟨w, h⟩ <;> simp [addition, h]
  have queue : (addition s v).queue = s.queue := by
    rcases untaken with ⟨count, w, h⟩ | ⟨w, h⟩ <;> simp [addition, h]
  simp only [sendable, append, phase, queue]
  exact List.take_left

/-- **The raise publishes the addition's version.** Confirming, taking,
uploading, announcing and activating an untaken capture after an addition
to `v` raises the audience to `v`: the snapshot is taken at the version its
raise names. Every queued original is published, in order, and later writes
stay queued. -/
theorem addition_raises_its_version (s : State S Stamp) (v : Nat)
    (later : List (Write S Stamp)) (untaken : Untaken s) :
    let result := activate (announce (upload (take (confirm (append (addition s v) later)))))
    result.version = v ∧ result.queue = later ∧ result.published = s.published ++ s.queue := by
  have phase : (addition s v).phase = .captured s.queue.length v := by
    rcases untaken with ⟨count, w, h⟩ | ⟨w, h⟩ <;> simp [addition, h]
  have queue : (addition s v).queue = s.queue := by
    rcases untaken with ⟨count, w, h⟩ | ⟨w, h⟩ <;> simp [addition, h]
  have published : (addition s v).published = s.published := by
    rcases untaken with ⟨count, w, h⟩ | ⟨w, h⟩ <;> simp [addition, h]
  have taken : (s.queue ++ later).take s.queue.length = s.queue := List.take_left
  have rest : (s.queue ++ later).drop s.queue.length = later := List.drop_left
  simp [activate, announce, upload, take, confirm, append, phase, queue, published, taken, rest]

/-- **A taken snapshot keeps its version.** Once the raise's snapshot is
taken, an addition changes nothing here: the raise goes out at the version
its snapshot was taken at. Without a capture an addition raises nothing. -/
theorem addition_keeps_taken (s : State S Stamp) (v : Nat) (untaken : ¬ Untaken s) :
    addition s v = s := by
  cases h : s.phase with
  | captured count w => exact absurd (Or.inl ⟨count, w, h⟩) untaken
  | confirmed w => exact absurd (Or.inr ⟨w, h⟩) untaken
  | _ => simp [addition, h]

/-- **The migration write's slot changes nothing.** Its marker carries no
part, so the slot holding it leaves every audience's ledger and rows as they
were: a device that ran the same migration and loaded the raise's snapshot
changes nothing with its own migration write (§18.4). -/
theorem marker_changes_nothing (R : Rules S) (seq : Nat → Slot S) (A : S.Aud) (k : Nat)
    {d : S.Dev} {g : Nat} (slot : seq (k + 1) = .writes d g (fun _ => none)) :
    audAt R seq A (k + 1) = audAt R seq A k ∧ rowsAt R seq A (k + 1) = rowsAt R seq A k := by
  refine ⟨?_, ?_⟩
  · rw [audAt_succ, slot]
    simp [audStep, touches]
  · rw [rowsAt_other R seq A k (by intros; simp [slot]), slot]
    simp [touches]

/-- Rebuilding from the queue applies the marker as nothing. -/
theorem marker_replays_nothing (R : Rules S) (A : S.Aud) (v : Nat) (rows : S.Rows)
    (marker : Write S Stamp) (empty : ∀ A, marker.bytes A = none) :
    QueueReload.applyOriginal R A v rows marker = rows := by
  simp [QueueReload.applyOriginal, empty]

theorem conversion_agreement (R : Rules S) (A : S.Aud) (v : Nat) (a b : S.Rows)
    (same : a = b) (originals : List (Write S Stamp)) :
    originals.foldl (QueueReload.applyOriginal R A v) a =
      originals.foldl (QueueReload.applyOriginal R A v) b := by rw [same]

open ReaderExamples in
/-- A raise to version 2 prepared with its body at version 3, and the same
body for a raise to 3. -/
def unmoved : Nat → Slot sig
  | 1 => .prepare () () (.raise 2) 0 3 (6 : Nat)
  | _ => .writes () 0 (fun _ => none)

open ReaderExamples in
def moved : Nat → Slot sig
  | 1 => .prepare () () (.raise 3) 0 3 (6 : Nat)
  | _ => .writes () 0 (fun _ => none)

open ReaderExamples in
/-- **Counterexample: a capture left at its old version after an addition.**
The update to 3 changed the device's schema, so a snapshot it takes is at
version 3. Raising to 2 with that body is refused by every device, since a
raise's body may be at most the version it raises to; raising to 3 counts. -/
theorem unmoved_raise_refused :
    ¬ AppliedTo ReaderExamples.rules unmoved () 1 ∧ AppliedTo ReaderExamples.rules moved () 1 := by
  unfold AppliedTo
  decide

end CovenSequence.RaiseQueue
