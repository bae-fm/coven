import CovenQueue.Model

namespace CovenQueue

abbrev History := Nat → CheckedWrite

/-- Receive readiness includes every recorded read and implicit own number.
Passing a position is not evidence of using its row data. -/
def prerequisites (h : History) (w : Nat) : List Nat :=
  (List.range w).filter fun a => reads (h w).record.identity (h a).record.identity

def Causal (h : History) : Prop :=
  ∀ w a, reads (h w).record.identity (h a).record.identity = true → a < w

theorem prerequisite_earlier {h : History} {w a : Nat} (ha : a ∈ prerequisites h w) : a < w :=
  List.mem_range.mp (List.mem_filter.mp ha).1

theorem own_prerequisite (h : History) (valid : Causal h) (w a : Nat)
    (hd : (h w).record.identity.id.device = (h a).record.identity.id.device)
    (hn : (h a).record.identity.id.number < (h w).record.identity.id.number) :
    a ∈ prerequisites h w := by
  have hr := own_earlier_read _ _ hd hn
  exact List.mem_filter.mpr ⟨List.mem_range.mpr (valid _ _ hr), hr⟩

inductive Judged (h : History) (v : View) : Nat → Verdict → Prop where
  | receive (w : Nat) (answers : Nat → Verdict) :
      (∀ a ∈ prerequisites h w, Judged h v a (answers a)) →
      (∀ a ∈ prerequisites h w, ∀ r, answers a ≠ .refused r) →
      Judged h v w (classify v (h w))

theorem verdict_agreement {h : History} {v : View} {w : Nat} {x y : Verdict}
    (hx : Judged h v w x) (hy : Judged h v w y) : x = y := by
  cases hx; cases hy; rfl

def evaluate (h : History) (v : View) (w : Nat) : Verdict := classify v (h w)

theorem classify_not_refused (v : View) (w : CheckedWrite)
    (hw : w.refusal = none) (r : Refusal) : classify v w ≠ .refused r := by
  simp only [classify, hw, classify.schema]
  split
  · split
    · simp
    · split
      · split <;> simp
      · simp
  · split
    · split <;> simp
    · simp

theorem every_write_judged (h : History) (v : View)
    (good : ∀ w, (h w).refusal = none) (w : Nat) : Judged h v w (evaluate h v w) := by
  induction w using Nat.strongRecOn with
  | ind w ih =>
    apply Judged.receive
    · intro a ha; exact ih a (prerequisite_earlier ha)
    · intro a _ r; exact classify_not_refused v (h a) (good a) r

/-- Looking up the original row change, independently of its current verdict.
Absent rows are not changes; a source naming an absent row fails `DataValid`. -/
def rowChange (h : History) (w r : Nat) : Option RowChange :=
  (h w).record.body.find? (fun ch => ch.row == r)

def provenance (h : History) (w r : Nat) : CovenMerge.DataDependency.Provenance Nat Nat :=
  match rowChange h w r with
  | none => ⟨none, []⟩
  | some ch => ch.provenance

structure DataValid (h : History) : Prop where
  uniqueRows : ∀ w, ((h w).record.body.map RowChange.row).Nodup
  earlier : ∀ w r a, a ∈ (provenance h w r).sources → a < w
  sourcePresent : ∀ w r a, a ∈ (provenance h w r).sources → (rowChange h a r).isSome = true

theorem rowChange_of_member (h : History) (valid : DataValid h) (w : Nat)
    (ch : RowChange) (mem : ch ∈ (h w).record.body) : rowChange h w ch.row = some ch := by
  have find (xs : List RowChange) (unique : (xs.map RowChange.row).Nodup)
      (member : ch ∈ xs) : xs.find? (fun other => other.row == ch.row) = some ch := by
    induction xs with
    | nil => simp at member
    | cons other xs ih =>
      simp only [List.map_cons, List.nodup_cons] at unique
      rcases List.mem_cons.mp member with rfl | member
      · simp
      · have different : other.row ≠ ch.row := by
          intro same
          exact unique.1 (List.mem_map.mpr ⟨ch, member, same.symm⟩)
        have notSame : (other.row == ch.row) = false := beq_eq_false_iff_ne.mpr different
        simp [List.find?, notSame, ih unique.2 member]
  exact find _ (valid.uniqueRows w) mem

/-- All direct admission and preserved frozen-change facts, before computing
new dependent losses. There is no adoption-exclusion marker. -/
def dataState (h : History) (valid : DataValid h) (v : View) :
    CovenMerge.DataDependency.State Nat Nat Nat where
  rank := id
  present w r := (rowChange h w r).isSome
  base w r := decide (evaluate h v w = .applied) &&
    (snapshotLoss v w r).isNone &&
    (rowChange h w r).any (fun ch => !ch.frozen)
  provenance := provenance h
  earlier := valid.earlier

inductive ChangeVerdict where
  | applied
  | frozen (cause : CovenMerge.LossCause Nat)
  | ignored (entry : Nat)
  | refused (reason : Refusal)
  deriving DecidableEq, Repr

def preserveFrozen (ch : RowChange) (otherwise : ChangeVerdict) : ChangeVerdict :=
  match ch.content with
  | .frozen cause _ => .frozen cause
  | .values _ => otherwise

def freshLoss (h : History) (valid : DataValid h) (v : View) (w r : Nat) :
    Option (CovenMerge.LossCause Nat) :=
  match evaluate h v w with
  | .excluded _ version => (snapshotLoss v w r).orElse (fun _ => some (.schemaChange version w))
  | .applied => (snapshotLoss v w r).orElse fun _ =>
      if CovenMerge.DataDependency.retained (dataState h valid v) w r then none
      else some (.dependency w (CovenMerge.DataDependency.droppedSources (dataState h valid v) w r))
  | .ignored _ | .refused _ => none

def classifyChange (h : History) (valid : DataValid h) (v : View)
    (w : Nat) (ch : RowChange) : ChangeVerdict :=
  match evaluate h v w with
  | .refused r => .refused r
  | .ignored e => .ignored e
  | .applied | .excluded _ _ =>
      preserveFrozen ch ((freshLoss h valid v w ch.row).elim .applied .frozen)

def changeResults (h : History) (valid : DataValid h) (v : View) (w : Nat) :
    List (RowChange × ChangeVerdict) :=
  (h w).record.body.map fun ch => (ch, classifyChange h valid v w ch)

/-- Snapshot evidence is derived from actual per-change decisions, never
from the reads of later writes. Dismissing displayed values does not edit it. -/
def captureDrops (h : History) (valid : DataValid h) (v : View) (received : List Nat) :
    List (Nat × Nat × CovenMerge.LossCause Nat) :=
  received.flatMap fun w => (changeResults h valid v w).filterMap fun (ch, answer) =>
    match answer with
    | .frozen cause => some (w, ch.row, cause)
    | _ => none

theorem capture_records_loss (h : History) (valid : DataValid h) (v : View)
    (received : List Nat) (w : Nat) (ch : RowChange) (cause : CovenMerge.LossCause Nat)
    (mem : w ∈ received) (change : ch ∈ (h w).record.body)
    (frozen : classifyChange h valid v w ch = .frozen cause) :
    (w, ch.row, cause) ∈ captureDrops h valid v received := by
  apply List.mem_flatMap.mpr
  refine ⟨w, mem, ?_⟩
  apply List.mem_filterMap.mpr
  exact ⟨(ch, .frozen cause), List.mem_map.mpr ⟨ch, change, by simp [frozen]⟩, rfl⟩

theorem captured_loss_original (h : History) (valid : DataValid h) (v : View)
    (received : List Nat) (w r : Nat) (cause : CovenMerge.LossCause Nat)
    (mem : (w, r, cause) ∈ captureDrops h valid v received) :
    w ∈ received ∧ ∃ ch, rowChange h w r = some ch ∧
      classifyChange h valid v w ch = .frozen cause := by
  obtain ⟨a, receivedA, changes⟩ := List.mem_flatMap.mp mem
  obtain ⟨pair, member, equal⟩ := List.mem_filterMap.mp changes
  obtain ⟨ch, body, rfl⟩ := List.mem_map.mp member
  cases answer : classifyChange h valid v a ch <;> simp [answer] at equal
  rcases equal with ⟨writeEq, row, reasonEq⟩
  subst a
  exact ⟨receivedA, ch, by rw [← row]; exact rowChange_of_member h valid w ch body,
    by simpa only [reasonEq] using answer⟩

theorem capture_loss_lookup (h : History) (valid : DataValid h) (v : View)
    (received : List Nat) (w r : Nat) (ch : RowChange) (cause : CovenMerge.LossCause Nat)
    (mem : w ∈ received) (found : rowChange h w r = some ch)
    (frozen : classifyChange h valid v w ch = .frozen cause) :
    snapshotLoss { v with drops := captureDrops h valid v received } w r = some cause := by
  have row : ch.row = r := by simpa using List.find?_some found
  have captured := capture_records_loss h valid v received w ch cause mem
    (List.mem_of_find?_eq_some found) frozen
  rw [row] at captured
  unfold snapshotLoss
  dsimp only
  cases result : (captureDrops h valid v received).find? (fun x => x.1 == w && x.2.1 == r) with
  | none =>
    have absent := List.find?_eq_none.mp result (w, r, cause) captured
    simp at absent
  | some x =>
    have key := List.find?_some result
    rcases x with ⟨a, rowId, reason⟩
    simp only [Bool.and_eq_true, beq_iff_eq] at key
    rcases key with ⟨writeEq, rowEq⟩
    subst a
    subst rowId
    obtain ⟨_, other, otherFound, otherFrozen⟩ := captured_loss_original h valid v received w r reason
      (List.mem_of_find?_eq_some result)
    rw [found] at otherFound
    cases Option.some.inj otherFound
    rw [frozen] at otherFrozen
    cases ChangeVerdict.frozen.inj otherFrozen
    rfl

/-- Snapshot lists may have different orders or repeated receipt identities;
their per-change lookup results still agree. -/
theorem captured_losses_converge (h : History) (valid : DataValid h) (v : View)
    (a b : List Nat) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    snapshotLoss { v with drops := captureDrops h valid v a } =
      snapshotLoss { v with drops := captureDrops h valid v b } := by
  have transfer (xs ys : List Nat) (subset : ∀ w, w ∈ xs → w ∈ ys)
      (w r : Nat) (cause : CovenMerge.LossCause Nat)
      (lookup : snapshotLoss { v with drops := captureDrops h valid v xs } w r = some cause) :
      snapshotLoss { v with drops := captureDrops h valid v ys } w r = some cause := by
    obtain ⟨x, found, reason⟩ := Option.map_eq_some_iff.mp lookup
    have key := List.find?_some found
    rcases x with ⟨source, rowId, saved⟩
    simp only [Bool.and_eq_true, beq_iff_eq] at key
    rcases key with ⟨writeEq, rowEq⟩
    subst source
    subst rowId
    obtain ⟨member, ch, change, frozen⟩ := captured_loss_original h valid v xs w r saved
      (List.mem_of_find?_eq_some found)
    exact reason ▸ capture_loss_lookup h valid v ys w r ch saved (subset w member) change frozen
  funext w r
  cases left : snapshotLoss { v with drops := captureDrops h valid v a } w r with
  | some cause => exact (transfer a b (fun w => (same w).mp) w r cause left).symm
  | none =>
    cases right : snapshotLoss { v with drops := captureDrops h valid v b } w r with
    | none => rfl
    | some cause =>
      have impossible := transfer b a (fun w => (same w).mpr) w r cause right
      simp [left] at impossible

/-- A covered position does not revive a source whose row change was lost.
This evidence persists even when its displayed loss has been dismissed. -/
theorem snapshot_drop_stays_dropped (h : History) (valid : DataValid h) (v : View)
    (w r : Nat) (cause : CovenMerge.LossCause Nat) (saved : snapshotLoss v w r = some cause) :
    CovenMerge.DataDependency.retained (dataState h valid v) w r = false :=
  CovenMerge.DataDependency.dropped_stays_dropped _ _ _ (by simp [dataState, saved])

theorem snapshot_loss_preserves_cause (h : History) (valid : DataValid h) (v : View)
    (w : Nat) (ch : RowChange) (cause : CovenMerge.LossCause Nat) (values : List Nat)
    (saved : snapshotLoss v w ch.row = some cause) (original : ch.content = .values values)
    (ordinary : evaluate h v w = .applied ∨ ∃ e version, evaluate h v w = .excluded e version) :
    classifyChange h valid v w ch = .frozen cause := by
  rcases ordinary with applied | ⟨e, version, excluded⟩ <;>
    simp [classifyChange, preserveFrozen, freshLoss, *]

/-- The loss retains the original row change, not a converted substitute. -/
theorem change_value_accounted (h : History) (valid : DataValid h) (v : View)
    (w : Nat) (ch : RowChange) (mem : ch ∈ (h w).record.body)
    (ordinary : evaluate h v w = .applied ∨ ∃ e version, evaluate h v w = .excluded e version) :
    (ch, .applied) ∈ changeResults h valid v w ∨
      ∃ cause, (ch, .frozen cause) ∈ changeResults h valid v w := by
  have member : (ch, classifyChange h valid v w ch) ∈ changeResults h valid v w :=
    List.mem_map.mpr ⟨ch, mem, rfl⟩
  have answer : classifyChange h valid v w ch = .applied ∨
      ∃ cause, classifyChange h valid v w ch = .frozen cause := by
    rcases ordinary with applied | ⟨e, version, excluded⟩ <;>
      cases content : ch.content <;>
      cases saved : snapshotLoss v w ch.row <;>
      cases kept : CovenMerge.DataDependency.retained (dataState h valid v) w ch.row <;>
      simp [classifyChange, preserveFrozen, freshLoss, *, Option.orElse]
  rcases answer with applied | ⟨cause, frozen⟩
  · exact Or.inl (by simpa [applied] using member)
  · exact Or.inr ⟨cause, by simpa [frozen] using member⟩

theorem applied_has_no_dropped_source (h : History) (valid : DataValid h) (v : View)
    (w : Nat) (ch : RowChange) (applied : classifyChange h valid v w ch = .applied) :
    CovenMerge.DataDependency.dependsOnDropped (dataState h valid v) w ch.row = false := by
  cases verdict : evaluate h v w <;> cases content : ch.content <;>
    cases saved : snapshotLoss v w ch.row <;>
    simp [classifyChange, freshLoss, preserveFrozen, verdict, content, saved] at applied
  cases kept : CovenMerge.DataDependency.retained (dataState h valid v) w ch.row
  · simp [kept] at applied
  · exact CovenMerge.DataDependency.retained_not_dependent _ _ _ kept

/-- Classification is insensitive to arrival order, including distinct orders
of unrelated writes. The map includes frozen losses with all original values. -/
def materializeChanges (h : History) (valid : DataValid h) (v : View) (received : List Nat) :
    Nat → List (RowChange × ChangeVerdict) :=
  fun w => if w ∈ received then changeResults h valid v w else []

theorem changes_converge (h : History) (valid : DataValid h) (v : View) (a b : List Nat)
    (same : ∀ w, w ∈ a ↔ w ∈ b) : materializeChanges h valid v a = materializeChanges h valid v b := by
  funext w
  simp only [materializeChanges, same w]

/-- A materialized audience after reload. Snapshot identities begin passed;
eligible waiting and arriving writes are consumed by the same function. -/
structure Materialized where
  passed : List Nat
  applied : List Nat
  deriving DecidableEq, Repr

def Materialized.load (included : List Nat) : Materialized := ⟨included, included⟩

def Materialized.consume (s : Materialized) (w : Nat) (v : Verdict) : Materialized :=
  if w ∈ s.passed then s else
    match v with
    | .refused _ => s
    | .applied => ⟨w :: s.passed, w :: s.applied⟩
    | _ => { s with passed := w :: s.passed }

def Materialized.Valid (s : Materialized) : Prop :=
  s.applied.Nodup ∧ ∀ w ∈ s.applied, w ∈ s.passed

theorem load_valid (included : List Nat) (hn : included.Nodup) :
    (Materialized.load included).Valid := ⟨hn, fun _ h => h⟩

theorem consume_valid (s : Materialized) (w : Nat) (v : Verdict) (h : s.Valid) :
    (s.consume w v).Valid := by
  unfold Materialized.consume
  split
  · exact h
  · rename_i hn
    cases v with
    | refused r => exact h
    | applied =>
      constructor
      · exact List.nodup_cons.mpr ⟨fun hw => hn (h.2 w hw), h.1⟩
      · intro a ha
        rcases List.mem_cons.mp ha with rfl | ha
        · exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ (h.2 a ha)
    | excluded e _ | ignored e =>
      exact ⟨h.1, fun a ha => List.mem_cons_of_mem _ (h.2 a ha)⟩

theorem consume_idempotent (s : Materialized) (w : Nat) (v : Verdict) :
    (s.consume w v).consume w v = s.consume w v := by
  unfold Materialized.consume
  split
  · simp_all
  · cases v <;> simp_all

theorem covered_not_applied_again (included : List Nat) (w : Nat) (hw : w ∈ included)
    (v : Verdict) : (Materialized.load included).consume w v = .load included := by
  simp [Materialized.consume, Materialized.load, hw]

theorem nothing_applied_twice (s : Materialized) (h : s.Valid) (arrivals : List (Nat × Verdict)) :
    (arrivals.foldl (fun s w => s.consume w.1 w.2) s).applied.Nodup := by
  have hv : (arrivals.foldl (fun s w => s.consume w.1 w.2) s).Valid := by
    induction arrivals generalizing s with
    | nil => exact h
    | cons x xs ih => exact ih _ (consume_valid _ _ _ h)
  exact hv.1

inductive AudienceState where
  | ready (adopted : View) (data : Materialized)
  | reloading (target : View) (previous : Materialized)
  deriving DecidableEq, Repr

inductive CommitResult where
  | accepted (view : View)
  | audienceReloading
  deriving DecidableEq, Repr

def commitInto : AudienceState → CommitResult
  | .ready v _ => .accepted v
  | .reloading _ _ => .audienceReloading

/-- All audiences touched by the commit are supplied by its row records. -/
def AudiencesReady (audiences : List AudienceState) : Prop :=
  ∀ audience ∈ audiences, ∃ view, commitInto audience = .accepted view

theorem reloading_blocks_commit (audiences : List AudienceState) (v : View)
    (old : Materialized) (h : AudiencesReady audiences) :
    .reloading v old ∉ audiences := by
  intro member
  obtain ⟨view, ready⟩ := h _ member
  cases ready

/-- Download or rebuilding failure retains the previous state. Only successful
atomic replacement changes which boundary a new write may claim (§15, E3). -/
def finishReload (s : AudienceState) (loaded : Option Materialized) : AudienceState :=
  match s, loaded with
  | .reloading v _, some data => .ready v data
  | _, _ => s

theorem reloading_refused (v : View) (old : Materialized) :
    commitInto (.reloading v old) = .audienceReloading := rfl

theorem failed_reload_unchanged (s : AudienceState) : finishReload s none = s := by
  cases s <;> rfl

end CovenQueue
