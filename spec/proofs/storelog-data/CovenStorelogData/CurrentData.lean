import CovenStorelogData.ReplayEffects
import CovenStorelogData.EntryFate
import CovenStorelogData.Pending
import CovenStorelogData.LostValues
import CovenStorelogData.CircleCreation

/-! §9, §10, §14.4–7. Database projections retain their original inputs.
A returning circle reloads from storage without treating passed positions as
evidence that its encrypted parts were read. -/
namespace CovenStorelogData.CurrentData
open CovenStorelog

def observe {W Col K : Type} [DecidableEq W] [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (H : CurrentReplay.History) (Wtime n : Nat) (received : List Nat)
    (original : CovenMerge.St W Row Col) : CovenMerge.Device W Row Col :=
  let result := CurrentReplay.resolve H Wtime n (entrySet received)
  let view := CovenMerge.view (inputs schema writes H.membership.log result original)
  ⟨original, view, CovenMerge.lossRecord original view⟩

/-- Deleted-circle loss metadata uses the same replay as row visibility. -/
def circleLossCause (H : CurrentReplay.History) (Wtime n : Nat) (received : List Nat)
    (row : Row) : Option LostValues.Cause :=
  match row.audience with
  | .store => none
  | .circle c =>
      if deletedCircle H.membership.log (CurrentReplay.resolve H Wtime n (entrySet received)) c then
        (CurrentReplay.circleCause H Wtime n (entrySet received) c).map
          LostValues.Cause.deletedCircle
      else none

theorem entries_preserve_inputs {W Col K : Type}
    [DecidableEq W] [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (H : CurrentReplay.History) (Wtime n : Nat) (received : List Nat)
    (original : CovenMerge.St W Row Col) :
    (observe schema writes H Wtime n received original).merged = original := rfl

theorem convergence {W Col K : Type}
    [DecidableEq W] [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (valid : CovenMerge.Valid writes) (H : CurrentReplay.History) (Wtime n : Nat)
    (A B : List Nat) (sameEntries : ∀ e, e ∈ A ↔ e ∈ B)
    (x y : List W) (hx : CovenMerge.CausalOrder writes x)
    (hy : CovenMerge.CausalOrder writes y) (sameWrites : ∀ w, w ∈ x ↔ w ∈ y) :
    observe schema writes H Wtime n A (x.foldl (CovenMerge.step writes) CovenMerge.St.init) =
      observe schema writes H Wtime n B (y.foldl (CovenMerge.step writes) CovenMerge.St.init) := by
  unfold observe
  rw [CurrentReplay.equal_received H Wtime n A B sameEntries,
    CovenMerge.merge_converges valid hx hy sameWrites]

/-- An earlier applied creation counts even if replay dropped it, unless its
time rejection is permanent. `received` is the device's log before the batch. -/
def createdBefore (H : CurrentReplay.History) (Wtime n : Nat)
    (received : List Nat) (c : Nat) : Bool :=
  received.any fun e => !Finality.tooLate H.membership Wtime n (fun _ => true) e &&
    match (H.membership.log e).action with
    | .makeCircle d _ => d == c
    | _ => false

def needsReload (H : CurrentReplay.History) (Wtime n : Nat) (received : List Nat)
    (before after : CovenStorelog.State) (m c : Nat) : Bool :=
  inCircle after c m && ((!inCircle before c m && createdBefore H Wtime n received c) ||
    before.versions != after.versions || before.resets != after.resets)

theorem deleted_return_reloads (H : CurrentReplay.History) (Wtime n : Nat)
    (received : List Nat) (before after : CovenStorelog.State) (m c : Nat)
    (created : createdBefore H Wtime n received c = true)
    (deleted : lookup before.circles c = none) (returned : inCircle after c m = true) :
    needsReload H Wtime n received before after m c = true := by
  simp [needsReload, inCircle, deleted, created] at *
  exact returned

/-- §14.5's author-in-circle check derives a creation from actual replay.
§7.1 then puts that creation in the receiver's applied entries before the
write can pass, even if the receiver's replay drops it. -/
theorem passed_part_has_creation (H : CurrentReplay.History) (Wtime n : Nat)
    (received : List Nat) (author : Author) (c : Nat)
    (past : ∀ e ∈ author.entries, e ∈ received)
    (allowed : inCircle (CurrentReplay.resolve H Wtime n (entrySet author.entries)).state
      c author.member = true) :
    createdBefore H Wtime n received c = true := by
  obtain ⟨e, kept, name, action⟩ :=
    CircleCreation.readable_has_kept_creation H Wtime n _ c author.member allowed
  have timely := CircleCreation.kept_not_too_late H Wtime n _ e kept
  have read := (CurrentReplay.kept_received H Wtime n _ e kept).2
  apply List.any_eq_true.mpr
  exact ⟨e, past e (by simpa [entrySet] using read), by simp [timely, action]⟩

/-- At a newly readable circle, every previously passed authorized part
forces a reload. This quantifies over any write's recorded author and past. -/
theorem passed_part_return_reloads (H : CurrentReplay.History) (Wtime n : Nat)
    (received : List Nat) (before after : CovenStorelog.State) (m c : Nat)
    (author : Author) (past : ∀ e ∈ author.entries, e ∈ received)
    (allowed : inCircle (CurrentReplay.resolve H Wtime n (entrySet author.entries)).state
      c author.member = true)
    (outside : inCircle before c m = false) (returned : inCircle after c m = true) :
    needsReload H Wtime n received before after m c = true := by
  simp [needsReload, outside, returned,
    passed_part_has_creation H Wtime n received author c past allowed]

/-- If a newly readable circle needs no reload, no authorized circle part
could have passed before the batch, whether opened or skipped. -/
theorem no_reload_no_skipped_part (H : CurrentReplay.History) (Wtime n : Nat)
    (received : List Nat) (before after : CovenStorelog.State) (m c : Nat)
    (outside : inCircle before c m = false) (returned : inCircle after c m = true)
    (noReload : needsReload H Wtime n received before after m c = false)
    (author : Author) (past : ∀ e ∈ author.entries, e ∈ received) :
    inCircle (CurrentReplay.resolve H Wtime n (entrySet author.entries)).state
      c author.member = false := by
  cases allowed : inCircle (CurrentReplay.resolve H Wtime n (entrySet author.entries)).state
      c author.member with
  | false => rfl
  | true =>
    have reload := passed_part_return_reloads H Wtime n received before after m c
      author past allowed outside returned
    simp [noReload] at reload

inductive CircleStatus where
  | available | outside | reloading (reason : Pending.Reason)
  deriving DecidableEq, Repr

structure CircleReader (W Col : Type) where
  data : Reloaded W Col
  positions : List W
  status : CircleStatus

def circleReplay {W Col : Type} (H : CurrentReplay.History) (Wtime n : Nat)
    (received : List Nat) (before after : CovenStorelog.State)
    (m c : Nat) (reader : CircleReader W Col) : CircleReader W Col :=
  if needsReload H Wtime n received before after m c then
    { reader with status := .reloading (.waits (.reload (.circle c))) }
  else if !inCircle after c m then { reader with status := .outside }
  else match reader.status with
    | .outside => { reader with status := .available }
    | _ => reader

def writable {W Col : Type} (reader : CircleReader W Col) : Bool :=
  reader.status == .available

def reloadWork {W Col : Type} (c reporter : Nat) (reader : CircleReader W Col) : Pending.Work :=
  ⟨.audience (.circle c), reporter, match reader.status with
    | .reloading reason => [⟨false, reason⟩]
    | _ => []⟩

/-- Storage has prepared the usable snapshot and remaining readable history at
the common point (§15). Previous passed positions never filter this input. -/
def finishReload {W Col : Type} (reader : CircleReader W Col)
    (prepared : Except Pending.Reason (Reloaded W Col × List W)) : CircleReader W Col :=
  match prepared with
  | .error reason => { reader with status := .reloading reason }
  | .ok (data, positions) => ⟨data, positions, .available⟩

/-- Commit checks the entries captured when loading began. A stale result
cannot replace a newer replay or advance its positions (§9, §14.4). -/
def commitReload {W Col : Type} (c : Nat) (expected current : List Nat)
    (reader : CircleReader W Col)
    (prepared : Except Pending.Reason (Reloaded W Col × List W)) : CircleReader W Col :=
  if expected == current then finishReload reader prepared
  else finishReload reader (.error (.waits (.reload (.circle c))))

def prepareReload {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (headers : W → Header W)
    (H : CurrentReplay.History) (Wtime n : Nat) (received : List Nat) (circle : Nat)
    (snapshots : SnapshotId → Snapshot W Col) (original : Snapshot W Col)
    (history : List W) : Reloaded W Col :=
  EntryFate.rebuild writes headers H.membership.log n (CurrentReplay.resolve H Wtime n (entrySet received))
    (.circle circle) snapshots original history

theorem rejoin_reloads_skipped {W Col : Type} (a b : CircleReader W Col)
    (data : Reloaded W Col) (positions : List W) :
    finishReload a (.ok (data, positions)) = finishReload b (.ok (data, positions)) := rfl

theorem failed_reload_keeps_inputs {W Col : Type} (reader : CircleReader W Col)
    (reason : Pending.Reason) :
    (finishReload reader (.error reason)).data = reader.data ∧
    (finishReload reader (.error reason)).positions = reader.positions ∧
    writable (finishReload reader (.error reason)) = false := ⟨rfl, rfl, rfl⟩

theorem failed_reload_visible {W Col : Type} (c reporter : Nat)
    (reader : CircleReader W Col) (reason : Pending.Reason) :
    ⟨.audience (.circle c), reporter, reason⟩ ∈
      Pending.records [reloadWork c reporter (finishReload reader (.error reason))] := by
  simp [Pending.records, Pending.first, reloadWork, finishReload]

theorem stale_reload_keeps_positions {W Col : Type} (c : Nat)
    (expected current : List Nat) (changed : expected ≠ current)
    (reader : CircleReader W Col) (prepared : Except Pending.Reason (Reloaded W Col × List W)) :
    (commitReload c expected current reader prepared).data = reader.data ∧
    (commitReload c expected current reader prepared).positions = reader.positions ∧
    writable (commitReload c expected current reader prepared) = false := by
  simp [commitReload, changed, failed_reload_keeps_inputs]

/-- An installation's stop gate is permanent; a new installation is another
device, never a reversal of this gate (§10). -/
structure Installation where
  received : List Nat
  stopped : Bool
  deriving DecidableEq, Repr

def receive (H : CurrentReplay.History) (W n m d : Nat) (s : Installation) (e : Nat) : Installation :=
  if s.stopped then s else
    let entries := s.received ++ [e]
    let result := CurrentReplay.resolve H W n (entrySet entries)
    ⟨entries, !running result.state m d⟩

theorem stopped_forever (H : CurrentReplay.History) (W n m d : Nat) (s : Installation)
    (stopped : s.stopped = true) (arrivals : List Nat) :
    arrivals.foldl (receive H W n m d) s = s := by
  induction arrivals with
  | nil => rfl
  | cons e es ih => simpa [receive, stopped] using ih

end CovenStorelogData.CurrentData
