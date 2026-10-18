import CovenSequence.QueueReload

/-! A migration captures originals through its marker. Publication confirms
that prefix before the preparation and raise, as §18.5 requires. Later writes
wait without extending the capture or preventing its confirmation. -/
namespace CovenSequence.RaiseQueue

variable {S : Sig} {Stamp : Type}
open QueueReload (Write)

inductive Phase where
  | ordinary
  | captured (count version : Nat)
  | confirmed (version : Nat)
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
  | .confirmed _ | .uploaded _ | .announced _ => []

/-- Only confirmed storage publication removes this prefix from the queue.
Its original records, including attempted bytes, become the published records. -/
def confirm (s : State S Stamp) : State S Stamp :=
  match s.phase with
  | .captured count v =>
    { s with queue := s.queue.drop count, published := s.published ++ s.queue.take count,
             phase := .confirmed v }
  | _ => s

/-- Successful fixed-body upload, after confirming the captured write slot. -/
def upload (s : State S Stamp) : State S Stamp :=
  match s.phase with
  | .confirmed v => { s with phase := .uploaded v }
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
    (activate (announce (upload ready))).version = marker.schema ∧
    (activate (announce (upload ready))).queue = later ∧
    (activate (announce (upload ready))).published = s.published ++ (s.queue ++ [marker]) := by
  have h := confirm_captured s marker later
  simp [activate, announce, upload, h.2.2, h.2.1, h.1]

theorem tail_waits (s : State S Stamp) (marker : Write S Stamp) (later : List (Write S Stamp)) :
    sendable (confirm (append (capture s marker) later)) = [] := by
  simp [sendable, (confirm_captured s marker later).2.2]

/-- Upload cannot start from an unconfirmed capture. After confirmation its
completion needs no condition on the remaining queue. -/
theorem preparation_waits_for_confirmation (s : State S Stamp) (marker : Write S Stamp)
    (later : List (Write S Stamp)) :
    upload (append (capture s marker) later) = append (capture s marker) later ∧
    (upload (confirm (append (capture s marker) later))).phase = .uploaded marker.schema := by
  exact ⟨rfl, by simp [upload, (confirm_captured s marker later).2.2]⟩

theorem activation_waits_for_announcement (s : State S Stamp) (v : Nat)
    (confirmed : s.phase = .confirmed v) : activate s = s ∧ activate (upload s) = upload s := by
  simp [activate, upload, confirmed]

/-- One sequence of confirmed originals survives every capture/confirmation/
activation, in order. This equality preserves numbers, timestamps, schema,
read-through and complete bytes together. -/
theorem originals_preserved (s : State S Stamp) (marker : Write S Stamp) (later : List (Write S Stamp)) :
    let result := activate (announce (upload (confirm (append (capture s marker) later))))
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

theorem conversion_agreement (R : Rules S) (A : S.Aud) (v : Nat) (a b : S.Rows)
    (same : a = b) (originals : List (Write S Stamp)) :
    originals.foldl (QueueReload.applyOriginal R A v) a =
      originals.foldl (QueueReload.applyOriginal R A v) b := by rw [same]

end CovenSequence.RaiseQueue
