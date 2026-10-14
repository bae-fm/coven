import CovenQueue.RowMerge
import CovenMerge.ReceiveMerge

/-! A write's own retirement window composed with the queue's per-change
classifier (§10, §8), as `Receive.base` composes `withinWindow` with the
boundary verdict. `late` is the receiver's `Receive.withinWindow` verdict
under the kept retirements it currently replays; changing that set starts
another run of these functions, which the receiver installs by rebuilding.
A late write is consumed with no effects and no loss of its own; otherwise
eligible changes using its data become dependency losses. -/
namespace CovenQueue

open CovenMerge

def windowState (h : History) (valid : DataValid h) (v : View) (late : Nat → Bool) :
    DataDependency.State Nat Nat Nat :=
  { dataState h valid v with base := fun w r => !late w && (dataState h valid v).base w r }

/-- `freshLoss` with sources judged under the window. A late source is
dropped with no loss of its own. -/
def windowLoss (h : History) (valid : DataValid h) (v : View) (late : Nat → Bool)
    (w r : Nat) : Option (LossCause Nat) :=
  match evaluate h v w with
  | .excluded _ version => (snapshotLoss v w r).orElse (fun _ => some (.schemaChange version w))
  | .applied => (snapshotLoss v w r).orElse fun _ =>
      if DataDependency.retained (windowState h valid v late) w r then none
      else some (.dependency w (DataDependency.droppedSources (windowState h valid v late) w r))
  | .ignored _ | .refused _ => none

/-- `none` is a change of a write outside its own window: dropped, with no
schema or dependency loss, whatever its boundary verdict would be. -/
def windowChange (h : History) (valid : DataValid h) (v : View) (late : Nat → Bool)
    (w : Nat) (ch : RowChange) : Option ChangeVerdict :=
  if late w then none else
    some (match evaluate h v w with
      | .refused r => .refused r
      | .ignored e => .ignored e
      | .applied | .excluded _ _ =>
          preserveFrozen ch ((windowLoss h valid v late w ch.row).elim .applied .frozen))

def windowResults (h : History) (valid : DataValid h) (v : View) (late : Nat → Bool)
    (w : Nat) : List (RowChange × Option ChangeVerdict) :=
  (h w).record.body.map fun ch => (ch, windowChange h valid v late w ch)

def windowInput (h : History) (valid : DataValid h) (v : View) (late : Nat → Bool) :
    Writes Nat Nat Nat :=
  { rowWrites h with chg := fun w r => (rowChange h w r).bind fun ch =>
      if windowChange h valid v late w ch = some .applied then some (rowMetadata ch) else none }

theorem window_late_no_change (h : History) (valid : DataValid h) (v : View)
    (late : Nat → Bool) (w : Nat) (outside : late w = true) (ch : RowChange) :
    windowChange h valid v late w ch = none := by
  simp [windowChange, outside]

theorem window_applied_iff (h : History) (valid : DataValid h) (v : View) (late : Nat → Bool)
    (w r : Nat) (ch : RowChange) (found : rowChange h w r = some ch) :
    windowChange h valid v late w ch = some .applied ↔
      DataDependency.retained (windowState h valid v late) w r = true := by
  have row : ch.row = r := by simpa using List.find?_some found
  cases outside : late w
  case true =>
    have dropped := DataDependency.dropped_stays_dropped (windowState h valid v late) w r
      (by simp [windowState, outside])
    simp [windowChange, outside, dropped]
  case false =>
  cases answer : evaluate h v w with
  | ignored e | refused reason | excluded e version =>
    have dropped := DataDependency.dropped_stays_dropped (windowState h valid v late) w r
      (by simp [windowState, dataState, answer])
    cases content : ch.content <;>
      simp [windowChange, windowLoss, preserveFrozen, outside, answer, content, dropped]
  | applied =>
    cases content : ch.content with
    | frozen cause values =>
      have dropped := DataDependency.dropped_stays_dropped (windowState h valid v late) w r
        (by simp [windowState, dataState, found, RowChange.frozen, content])
      simp [windowChange, preserveFrozen, outside, answer, content, dropped]
    | values values =>
      cases saved : snapshotLoss v w r with
      | none =>
        simp only [windowChange, outside, answer, preserveFrozen, content, windowLoss, row, saved]
        cases kept : DataDependency.retained (windowState h valid v late) w r <;> simp
      | some cause =>
        have dropped := DataDependency.dropped_stays_dropped (windowState h valid v late) w r
          (by simp [windowState, dataState, saved])
        simp [windowChange, windowLoss, preserveFrozen, outside, answer, content, row, saved,
          dropped]

theorem window_retention (h : History) (valid : DataValid h) (v : View) (late : Nat → Bool)
    (original : Valid (rowWrites h)) (p : DataDependency.Authored (rowWrites h))
    (sources : p.provenance = provenance h) (w r : Nat) :
    DataDependency.retained (DataDependency.authoredState (rowWrites h) original p
      (windowState h valid v late).base) w r =
      DataDependency.retained (windowState h valid v late) w r := by
  apply DataDependency.retained_congr
  · funext w r
    simp [DataDependency.authoredState, windowState, dataState, rowWrites]
  · rfl
  · intro w r
    simp [DataDependency.authoredState, windowState, dataState, sources]

/-- The window's merge input is B17's `effective` history under the composed
admission: no separate retained-witness premise is needed. -/
theorem windowInput_effective (h : History) (valid : DataValid h) (v : View)
    (late : Nat → Bool) (original : Valid (rowWrites h))
    (p : DataDependency.Authored (rowWrites h)) (sources : p.provenance = provenance h) :
    windowInput h valid v late =
      DataDependency.effective (rowWrites h) original p (windowState h valid v late).base := by
  have changes : (windowInput h valid v late).chg =
      (DataDependency.effective (rowWrites h) original p (windowState h valid v late).base).chg := by
    funext w r
    change ((rowChange h w r).bind fun ch =>
      if windowChange h valid v late w ch = some .applied then some (rowMetadata ch) else none) =
      (if DataDependency.retained (DataDependency.authoredState (rowWrites h) original p
        (windowState h valid v late).base) w r then (rowChange h w r).map rowMetadata else none)
    rw [window_retention h valid v late original p sources]
    cases found : rowChange h w r with
    | none => split <;> rfl
    | some ch =>
      simp only [Option.bind_some, Option.map_some,
        window_applied_iff h valid v late w r ch found]
  exact congrArg (fun chg => { rowWrites h with chg := chg }) changes

def windowRows (h : History) (valid : DataValid h) (v : View) (late : Nat → Bool)
    (snapshot : St Nat Nat Nat) (received : List Nat) : St Nat Nat Nat :=
  received.foldl (step (windowInput h valid v late)) snapshot

def windowChanges (h : History) (valid : DataValid h) (v : View) (late : Nat → Bool)
    (received : List Nat) : Nat → List (RowChange × Option ChangeVerdict) :=
  fun w => if w ∈ received then windowResults h valid v late w else []

/-- `rows_and_losses_converge` with the receiver's window verdicts: equal
consumed sets give equal rows, cell losses and per-change verdicts,
including dependency losses on late writes, in every causal receive order. -/
theorem window_rows_and_losses_converge (h : History) (valid : DataValid h) (v : View)
    (late : Nat → Bool)
    (original : Valid (rowWrites h)) (p : DataDependency.Authored (rowWrites h))
    (sources : p.provenance = provenance h) (C : Nat → Prop)
    (closed : Closed (rowWrites h) C) (snapshot : St Nat Nat Nat)
    (correct : IsSpec (windowInput h valid v late) C snapshot)
    {a b : List Nat} (ca : CausalFrom (rowWrites h) C a) (cb : CausalFrom (rowWrites h) C b)
    (same : ∀ w, w ∈ a ↔ w ∈ b) :
    (windowRows h valid v late snapshot a, windowChanges h valid v late a) =
      (windowRows h valid v late snapshot b, windowChanges h valid v late b) := by
  have eq := windowInput_effective h valid v late original p sources
  have rows : windowRows h valid v late snapshot a = windowRows h valid v late snapshot b := by
    simp only [windowRows, eq]
    exact Data.converges _ original p _ C closed snapshot (by rwa [← eq]) ca cb same
  have changes : windowChanges h valid v late a = windowChanges h valid v late b := by
    funext w
    simp only [windowChanges, same w]
  exact Prod.ext rows changes

/-- Without late writes the window changes nothing: the queue's own
classifier and merge input are recovered exactly. -/
theorem window_none_classify (h : History) (valid : DataValid h) (v : View) (w : Nat)
    (ch : RowChange) :
    windowChange h valid v (fun _ => false) w ch = some (classifyChange h valid v w ch) := by
  have same : windowState h valid v (fun _ => false) = dataState h valid v := by
    simp [windowState]
  cases answer : evaluate h v w <;>
    simp [windowChange, windowLoss, classifyChange, freshLoss, answer, same]

end CovenQueue
