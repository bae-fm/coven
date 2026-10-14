import CovenStorelog.Model

/-! §18, §19.1, E4–E5. Work and its first unmet condition are committed together.
A failed database commit is a failed read, never a successful empty list. -/
namespace CovenStorelogData.Pending
open CovenStorelog

inductive Subject where
  | write (device number : Nat) | entry (device number : Nat)
  | keyCopy (audience : Audience) (key member : Nat)
  | snapshot (id : SnapshotId) | positions (device : Nat)
  | file (device id : Nat) | operation (id : Nat)
  | retention (path : String) | audience (id : Audience)
  | agreement (audience : Audience) (peer : Nat)
  | joinRequest (id : Nat) | connection
  deriving DecidableEq, Repr

inductive Prerequisite where
  | object (path : String) | deviceRegistration (id : Nat)
  | deviceReplacement (id : Nat) | schemaPublication (audience : Audience) (version : Nat)
  | reload (audience : Audience) | ownUploads | positions (device : Nat)
  | entryFinality (entry : Nat) | circleVisible (circle : Nat)
  deriving DecidableEq, Repr

inductive DropReason where
  | landedTooLate | beatenBy (entry : Nat) | targetGone | noAdminLeft
  | notAllowed
  deriving DecidableEq, Repr

inductive Failure where
  | database | disk | keyCustody | crypto | operation
  deriving DecidableEq, Repr

inductive AccessAction where
  | grant | revoke
  deriving DecidableEq, Repr

inductive Reason where
  | missing (path : String) | waits (prerequisite : Prerequisite)
  | keyUnavailable (audience : Audience) (key : Nat)
  | updateRequired (version : Nat) | noStorage
  | storage (retryable : Bool) | refused | invalidPositions
  | dropped (reason : DropReason)
  | providerPending (action : AccessAction) (account : String)
  | pendingOwner (account : String) | accountInUse (account : String)
  | accessRemains (account : String) | deleteAccessKey (key : String)
  | fileUnavailable | disagrees | failed (cause : Failure) | paused | removed
  deriving DecidableEq, Repr

inductive Retry where
  | automatic | afterUpdate | appAction | never
  deriving DecidableEq, Repr

def retry : Reason → Retry
  | .missing _ | .waits _ | .keyUnavailable _ _ | .invalidPositions |
    .providerPending _ _ | .pendingOwner _ | .accountInUse _ | .storage true => .automatic
  | .updateRequired _ | .refused => .afterUpdate
  | .dropped _ | .removed => .never
  | _ => .appAction

structure Condition where
  met : Bool
  reason : Reason
  deriving DecidableEq, Repr

/-- Conditions are in the subject's processing order, not error priority. -/
def first : List Condition → Option Reason
  | [] => none
  | c :: cs => if c.met then first cs else some c.reason

structure Work where
  subject : Subject
  reporter : Nat
  conditions : List Condition
  deriving DecidableEq, Repr

structure Record where
  subject : Subject
  reporter : Nat
  reason : Reason
  deriving DecidableEq, Repr

def records (work : List Work) : List Record :=
  work.filterMap fun w => (first w.conditions).map (Record.mk w.subject w.reporter)

theorem every_pending_subject (work : List Work) (w : Work) (reason : Reason)
    (present : w ∈ work) (waiting : first w.conditions = some reason) :
    ⟨w.subject, w.reporter, reason⟩ ∈ records work := by
  apply List.mem_filterMap.mpr
  exact ⟨w, present, by simp [waiting]⟩

theorem only_first_reason (work : List Work) (record : Record)
    (present : record ∈ records work) :
    ∃ w ∈ work, w.subject = record.subject ∧ w.reporter = record.reporter ∧
      first w.conditions = some record.reason := by
  obtain ⟨w, hw, h⟩ := List.mem_filterMap.mp present
  cases hf : first w.conditions with
  | none => simp [hf] at h
  | some reason =>
      simp only [hf, Option.map_some, Option.some.injEq] at h
      subst record
      exact ⟨w, hw, rfl, rfl, hf⟩

theorem first_unmet (before after : List Condition) (c : Condition)
    (earlier : ∀ x ∈ before, x.met = true) (unmet : c.met = false) :
    first (before ++ c :: after) = some c.reason := by
  induction before with
  | nil => simp [first, unmet]
  | cons x xs ih =>
      simp [first, earlier x List.mem_cons_self,
        ih (fun y hy => earlier y (List.mem_cons_of_mem _ hy))]

/-- An observation replaces this subject's previous observation in the same
transaction as its progress. A task with no unmet conditions emits no record. -/
def observe (work : List Work) (next : Work) : List Work :=
  next :: work.filter (fun w => !(w.subject == next.subject && w.reporter == next.reporter))

theorem observed_wait_visible (work : List Work) (next : Work) (reason : Reason)
    (h : first next.conditions = some reason) :
    ⟨next.subject, next.reporter, reason⟩ ∈ records (observe work next) :=
  every_pending_subject _ _ _ List.mem_cons_self h

theorem latest_reason_replaces_previous (work : List Work) (next : Work) (record : Record)
    (present : record ∈ records (observe work next))
    (subject : record.subject = next.subject) (reporter : record.reporter = next.reporter) :
    first next.conditions = some record.reason := by
  obtain ⟨w, hw, hs, hr, hf⟩ := only_first_reason _ _ present
  rcases List.mem_cons.mp hw with rfl | hw
  · exact hf
  · have hh := (List.mem_filter.mp hw).2
    simp [hs, hr, subject, reporter] at hh

theorem progress_removes_record (work : List Work) (next : Work)
    (advanced : first next.conditions = none) :
    ∀ record ∈ records (observe work next),
      ¬(record.subject = next.subject ∧ record.reporter = next.reporter) := by
  intro record present same
  have := latest_reason_replaces_previous work next record present same.1 same.2
  simp [advanced] at this

inductive Database where
  | available (work : List Work)
  | failed (cause : Failure)
  deriving DecidableEq, Repr

/-- Database failure prevents calls and live queries until reopening (E5). -/
def commit (db : Database) (next : Work) (saved : Except Failure Unit) : Database :=
  match db, saved with
  | .failed cause, _ => .failed cause
  | _, .error cause => .failed cause
  | .available work, .ok () => .available (observe work next)

def query : Database → Except Failure (List Record)
  | .available work => .ok (records work)
  | .failed cause => .error cause

/-- Both paired E4 reads execute this same query against a committed snapshot. -/
def pending := query
def subscribePending := query

theorem paired_reads (db : Database) : pending db = subscribePending db := rfl

theorem save_failure_visible (work : List Work) (next : Work) (cause : Failure) :
    pending (commit (.available work) next (.error cause)) = .error cause := rfl

/-- A reset suppresses prior observations while effective; the observations
remain inputs until finality. Later observations are separate work (§19.1). -/
structure Observation where
  work : Work
  suppressedBy : List Nat
  deriving DecidableEq, Repr

def visible (kept : Nat → Bool) (observations : List Observation) : List Work :=
  (observations.filter fun o => !o.suppressedBy.any kept).map (·.work)

theorem dropped_reset_restores (observations : List Observation)
    (gone : ∀ o ∈ observations, ∀ e ∈ o.suppressedBy, kept e = false) :
    visible kept observations = observations.map (·.work) := by
  unfold visible
  congr 1
  apply List.filter_eq_self.mpr
  intro o ho
  simp only [Bool.not_eq_true']
  exact List.any_eq_false.mpr (fun e he => by simp [gone o ho e he])


/-- At most one work item, hence one record, per subject and reporting device
(§19.1). Every transition below preserves it. -/
def keys (work : List Work) : List (Subject × Nat) := work.map fun w => (w.subject, w.reporter)

def Unique (work : List Work) : Prop := (keys work).Nodup

theorem unique_same (work : List Work) (u : Unique work) (w₁ w₂ : Work)
    (h₁ : w₁ ∈ work) (h₂ : w₂ ∈ work) (hs : w₁.subject = w₂.subject)
    (hr : w₁.reporter = w₂.reporter) : w₁ = w₂ := by
  induction work with
  | nil => cases h₁
  | cons w ws ih =>
      simp only [Unique, keys, List.map_cons, List.nodup_cons] at u
      rcases List.mem_cons.mp h₁ with e₁ | h₁ <;> rcases List.mem_cons.mp h₂ with e₂ | h₂
      · rw [e₁, e₂]
      · exact absurd (List.mem_map.mpr ⟨w₂, h₂, by rw [e₁] at hs hr; simp [hs, hr]⟩) u.1
      · exact absurd (List.mem_map.mpr ⟨w₁, h₁, by rw [e₂] at hs hr; simp [hs, hr]⟩) u.1
      · exact ih u.2 h₁ h₂

theorem one_record (work : List Work) (u : Unique work) (r₁ r₂ : Record)
    (h₁ : r₁ ∈ records work) (h₂ : r₂ ∈ records work) (hs : r₁.subject = r₂.subject)
    (hr : r₁.reporter = r₂.reporter) : r₁ = r₂ := by
  obtain ⟨w₁, hw₁, hs₁, hr₁, hf₁⟩ := only_first_reason _ _ h₁
  obtain ⟨w₂, hw₂, hs₂, hr₂, hf₂⟩ := only_first_reason _ _ h₂
  have e := unique_same work u w₁ w₂ hw₁ hw₂ (by rw [hs₁, hs₂, hs]) (by rw [hr₁, hr₂, hr])
  subst e
  cases r₁; cases r₂
  simp only [Record.mk.injEq] at hs hr hf₁ hf₂ ⊢
  rw [hf₁] at hf₂
  exact ⟨hs, hr, Option.some.inj hf₂⟩

theorem filter_unique (work : List Work) (p : Work → Bool) (u : Unique work) :
    Unique (work.filter p) :=
  List.Nodup.sublist ((List.filter_sublist).map _) u

theorem observe_unique (work : List Work) (next : Work) (u : Unique work) :
    Unique (observe work next) := by
  unfold observe
  simp only [Unique, keys, List.map_cons, List.nodup_cons]
  refine ⟨fun h => ?_, filter_unique work _ u⟩
  obtain ⟨w, hw, he⟩ := List.mem_map.mp h
  have hk := (List.mem_filter.mp hw).2
  simp only [Prod.mk.injEq] at he
  simp [he.1, he.2] at hk

/-! ## Positions covering a waiting write (§6, §15, §19.1)

A loaded snapshot moves this device's write positions past writes it never
applied. The download fetches only writes its positions do not cover, so a
wait for a covered write can never end by that write applying: the load ends
it in the same transaction. A verdict on the write's bytes (a refusal or a
drop) is not a wait and stays. -/

/-- §6 write positions: the highest number this device has consumed from each
device's write log, `0` for none. -/
abbrev Positions := Nat → Nat

/-- Rust `WritePositions::covers`. -/
def covers (p : Positions) (device number : Nat) : Bool := decide (number ≤ p device)

/-- The download schedules exactly the writes its positions do not cover (§6). -/
def scheduled (p : Positions) (device number : Nat) : Bool := !covers p device number

private def raiseOne (d : Nat) (m : Nat) (x : Nat × Nat) : Nat := if x.1 = d then max m x.2 else m

/-- A snapshot load installs the positions it consumed; none moves back. -/
def raise (p : Positions) (jump : List (Nat × Nat)) : Positions :=
  fun d => jump.foldl (raiseOne d) (p d)

/-- A verdict on a write's bytes, which loading never clears (§19.1).
Rust `PendingValue::ends_when_covered` is its negation. -/
def verdict : Reason → Bool
  | .refused | .dropped _ => true
  | _ => false

/-- A record coverage ends: this device's wait for a write its positions cover. -/
def coveredRecord (device : Nat) (p : Positions) (r : Record) : Bool :=
  match r.subject with
  | .write d n => r.reporter == device && covers p d n && !verdict r.reason
  | _ => false

/-- The same test on the work whose first unmet condition is that record. -/
def coveredWait (device : Nat) (p : Positions) (w : Work) : Bool :=
  match first w.conditions with
  | some reason => coveredRecord device p ⟨w.subject, w.reporter, reason⟩
  | none => false

/-- The snapshot load's transaction ends every covered wait this device
reported. Peer reports, other subjects and verdicts are untouched. -/
def cover (device : Nat) (p : Positions) (work : List Work) : List Work :=
  work.filter fun w => !coveredWait device p w

theorem records_cover (device : Nat) (p : Positions) (work : List Work) :
    records (cover device p work) = (records work).filter (!coveredRecord device p ·) := by
  induction work with
  | nil => rfl
  | cons w ws ih =>
      unfold cover at ih ⊢
      cases hf : first w.conditions with
      | none =>
          simp only [List.filter_cons, coveredWait, hf, Bool.not_false, ite_true, records,
            List.filterMap_cons, Option.map_none] at ih ⊢
          exact ih
      | some reason =>
          by_cases hc : coveredRecord device p ⟨w.subject, w.reporter, reason⟩ = true
          · simp only [List.filter_cons, coveredWait, hf, hc, Bool.not_true, records,
              List.filterMap_cons, Option.map_some] at ih ⊢
            simpa using ih
          · simp only [Bool.not_eq_true] at hc
            simp only [List.filter_cons, coveredWait, hf, hc, Bool.not_false, ite_true, records,
              List.filterMap_cons, Option.map_some] at ih ⊢
            simp [ih]

/-- Coverage removes only waits for writes this device's positions now cover. -/
theorem cover_removes_only_covered_waits (device : Nat) (p : Positions)
    (work : List Work) (r : Record) (before : r ∈ records work)
    (gone : r ∉ records (cover device p work)) :
    ∃ d n, r.subject = .write d n ∧ r.reporter = device ∧ covers p d n = true ∧
      verdict r.reason = false := by
  rw [records_cover] at gone
  have hc : coveredRecord device p r = true := by
    cases h : coveredRecord device p r
    · exact absurd (List.mem_filter.mpr ⟨before, by simp [h]⟩) gone
    · rfl
  unfold coveredRecord at hc
  split at hc
  · next d n hs =>
      simp only [Bool.and_eq_true, beq_iff_eq, Bool.not_eq_true'] at hc
      exact ⟨d, n, hs, hc.1.1, hc.1.2, hc.2⟩
  · simp at hc

/-- A refusal or drop of a write's bytes survives coverage (§19.1: snapshot
loading alone does not clear refusals). -/
theorem cover_keeps_verdicts (device : Nat) (p : Positions) (work : List Work)
    (r : Record) (present : r ∈ records work) (byBytes : verdict r.reason = true) :
    r ∈ records (cover device p work) := by
  rw [records_cover]
  refine List.mem_filter.mpr ⟨present, ?_⟩
  unfold coveredRecord
  split <;> simp [byBytes]

/-- After coverage this device holds no wait for a covered write. -/
theorem covered_wait_gone (device : Nat) (p : Positions) (work : List Work)
    (r : Record) (d n : Nat) (present : r ∈ records (cover device p work))
    (subject : r.subject = .write d n) (reporter : r.reporter = device)
    (covered : covers p d n = true) : verdict r.reason = true := by
  rw [records_cover] at present
  have h := (List.mem_filter.mp present).2
  simp only [coveredRecord, subject, reporter, beq_self_eq_true, covered,
    Bool.true_and, Bool.not_not] at h
  exact h

private theorem fold_raiseOne_le (d : Nat) (xs : List (Nat × Nat)) (s : Nat) :
    s ≤ xs.foldl (raiseOne d) s := by
  induction xs generalizing s with
  | nil => exact Nat.le_refl _
  | cons x xs ih =>
      simp only [List.foldl_cons, raiseOne]
      split
      · exact Nat.le_trans (Nat.le_max_left _ _) (ih _)
      · exact ih _

theorem raise_monotone (p : Positions) (jump : List (Nat × Nat)) (d : Nat) :
    p d ≤ raise p jump d :=
  fold_raiseOne_le d jump (p d)

theorem raise_covers (p : Positions) (jump : List (Nat × Nat)) (d n m : Nat)
    (installed : (d, m) ∈ jump) (le : n ≤ m) : covers (raise p jump) d n = true := by
  have key : ∀ (xs : List (Nat × Nat)) (s : Nat), (d, m) ∈ xs → m ≤ xs.foldl (raiseOne d) s := by
    intro xs
    induction xs with
    | nil => intro _ h; cases h
    | cons x xs ih =>
        intro s h
        simp only [List.foldl_cons]
        rcases List.mem_cons.mp h with e | h
        · subst e
          simp only [raiseOne, ite_true]
          exact Nat.le_trans (Nat.le_max_right _ _) (fold_raiseOne_le d xs _)
        · exact ih _ h
  simp only [covers, raise]
  exact decide_eq_true (Nat.le_trans le (key jump _ installed))

/-- Once covered, a write is never scheduled again, whatever later loads
install: no later progress can apply it, so no condition of its wait remains. -/
theorem covered_never_scheduled (p : Positions) (d n : Nat) (covered : covers p d n = true)
    (later : List (List (Nat × Nat))) :
    scheduled (later.foldl raise p) d n = false := by
  induction later generalizing p with
  | nil => simp [scheduled, covered]
  | cons jump rest ih =>
      apply ih
      simp only [covers, decide_eq_true_eq] at covered ⊢
      exact Nat.le_trans covered (raise_monotone p jump d)

/-- Coverage removes a record only when its wait is over for good: a wait,
not a verdict, for a write this device will never schedule again. -/
theorem cover_safe (device : Nat) (p : Positions) (jump : List (Nat × Nat))
    (work : List Work) (r : Record) (before : r ∈ records work)
    (gone : r ∉ records (cover device (raise p jump) work)) :
    ∃ d n, r.subject = .write d n ∧ r.reporter = device ∧ verdict r.reason = false ∧
      ∀ later : List (List (Nat × Nat)),
        scheduled (later.foldl raise (raise p jump)) d n = false := by
  obtain ⟨d, n, hs, hr, hc, hv⟩ := cover_removes_only_covered_waits _ _ _ _ before gone
  exact ⟨d, n, hs, hr, hv, covered_never_scheduled _ _ _ hc⟩

theorem cover_unique (device : Nat) (p : Positions) (work : List Work) (u : Unique work) :
    Unique (cover device p work) :=
  filter_unique work _ u

/-! ## A completed listing decides its class (§19.1)

Each pass reads every peer's posted positions, and each audience's snapshot
listing reads every snapshot of that audience. One completed listing replaces
this device's records for that class as a whole: a listed subject gets its
first unmet condition, and a subject the listing does not name loses its
record. A failed listing replaces nothing. This device's own positions
record belongs to its publication, which clears it only on confirmation, so
no listing decides it; a failed check of its own listed object is an
ordinary observation of that subject. -/

/-- The class of subjects one completed listing decides. -/
inductive Scan where
  | positions | snapshots (audience : Audience)
  deriving DecidableEq, Repr

/-- Rust: the peer subjects `posted_positions::scan` replaces, and the
audience test of `replace_snapshot_waits`. -/
def Scan.decides (device : Nat) : Scan → Subject → Bool
  | .positions, .positions peer => peer != device
  | .snapshots a, .snapshot id => id.audience == a
  | _, _ => false

/-- Each listed object read, with its conditions in processing order. An
object that disappears between listing and reading is not listed. -/
structure Listing where
  scan : Scan
  observed : List (Subject × List Condition)
  deriving DecidableEq, Repr

/-- Rust refuses a replacement naming a subject twice or outside its class. -/
def Listing.valid (device : Nat) (l : Listing) : Bool :=
  l.observed.all (fun o => l.scan.decides device o.1) && decide (l.observed.map (·.1)).Nodup

def replace (device : Nat) (l : Listing) (work : List Work) : List Work :=
  l.observed.map (fun o => ⟨o.1, device, o.2⟩) ++
    work.filter fun w => !(w.reporter == device && l.scan.decides device w.subject)

/-- The listing names exactly the subjects of its class whose objects storage
holds (`present`), with the conditions their processing currently checks. -/
def Listing.Complete (device : Nat) (l : Listing) (present : Subject → Bool)
    (check : Subject → List Condition) : Prop :=
  ∀ s, l.scan.decides device s = true →
    (present s = true ↔ ∃ cs, (s, cs) ∈ l.observed) ∧ ∀ cs, (s, cs) ∈ l.observed → cs = check s

/-- A subject whose object storage does not hold has nothing to process. -/
def current (present : Subject → Bool) (check : Subject → List Condition) (s : Subject) :
    List Condition :=
  if present s then check s else []

/-- A record outside the listing's class, or reported by a peer, stays. -/
theorem replace_keeps_others (device : Nat) (l : Listing) (work : List Work) (r : Record)
    (outside : ¬(r.reporter = device ∧ l.scan.decides device r.subject = true))
    (before : r ∈ records work) : r ∈ records (replace device l work) := by
  obtain ⟨w, hw, hs, hr, hf⟩ := only_first_reason _ _ before
  have kept : w ∈ replace device l work := by
    refine List.mem_append_right _ (List.mem_filter.mpr ⟨hw, ?_⟩)
    cases hrep : w.reporter == device <;> cases hd : l.scan.decides device w.subject <;> simp
    exact outside ⟨hr ▸ beq_iff_eq.mp hrep, hs ▸ hd⟩
  have h := every_pending_subject _ w r.reason kept hf
  cases r
  simp only at hs hr
  subst hs hr
  exact h

/-- After a complete listing, this device's record for a subject of its class
is exactly that subject's current first unmet condition. -/
theorem listing_decides (device : Nat) (l : Listing) (work : List Work)
    (present : Subject → Bool) (check : Subject → List Condition)
    (complete : l.Complete device present check) (s : Subject) (inClass : l.scan.decides device s = true)
    (reason : Reason) :
    ⟨s, device, reason⟩ ∈ records (replace device l work) ↔
      first (current present check s) = some reason := by
  constructor
  · intro h
    obtain ⟨w, hw, hs, hr, hf⟩ := only_first_reason _ _ h
    rcases List.mem_append.mp hw with hw | hw
    · obtain ⟨o, ho, e⟩ := List.mem_map.mp hw
      subst e
      simp only at hs hf
      subst hs
      have hp := (complete o.1 inClass).1.mpr ⟨o.2, ho⟩
      have hc := (complete o.1 inClass).2 o.2 ho
      simpa [current, hp, ← hc] using hf
    · have hk := (List.mem_filter.mp hw).2
      simp [hs, hr, inClass] at hk
  · intro h
    have hp : present s = true := by
      cases hp : present s
      · simp [current, hp, first] at h
      · rfl
    obtain ⟨cs, hcs⟩ := (complete s inClass).1.mp hp
    have hc := (complete s inClass).2 cs hcs
    apply every_pending_subject _ ⟨s, device, cs⟩ reason
    · exact List.mem_append_left _ (List.mem_map.mpr ⟨(s, cs), hcs, rfl⟩)
    · simpa [current, hp, hc] using h

/-- A subject absent from a complete listing has no unmet condition and no record. -/
theorem absent_subject_cleared (device : Nat) (l : Listing) (work : List Work)
    (present : Subject → Bool) (check : Subject → List Condition)
    (complete : l.Complete device present check) (s : Subject) (inClass : l.scan.decides device s = true)
    (absent : present s = false) (reason : Reason) :
    ⟨s, device, reason⟩ ∉ records (replace device l work) := by
  rw [listing_decides device l work present check complete s inClass]
  simp [current, absent, first]

/-- No record is removed while its condition still holds: a record survives a
complete listing whenever, if the listing decides it, its subject's current
first unmet condition is still that record's reason. -/
theorem listing_safe (device : Nat) (l : Listing) (work : List Work)
    (present : Subject → Bool) (check : Subject → List Condition)
    (complete : l.Complete device present check) (r : Record) (before : r ∈ records work)
    (holds : r.reporter = device → l.scan.decides device r.subject = true →
      first (current present check r.subject) = some r.reason) :
    r ∈ records (replace device l work) := by
  by_cases mine : r.reporter = device ∧ l.scan.decides device r.subject = true
  · obtain ⟨hr, hd⟩ := mine
    have h := (listing_decides device l work present check complete r.subject hd r.reason).mpr
      (holds hr hd)
    cases r
    simp only at hr
    subst hr
    exact h
  · exact replace_keeps_others device l work r mine before

/-- A record a complete listing removes was this device's, in the listing's
class, and its subject's current first unmet condition is no longer it. -/
theorem listing_removes_only_ended (device : Nat) (l : Listing) (work : List Work)
    (present : Subject → Bool) (check : Subject → List Condition)
    (complete : l.Complete device present check) (r : Record) (before : r ∈ records work)
    (gone : r ∉ records (replace device l work)) :
    r.reporter = device ∧ l.scan.decides device r.subject = true ∧
      first (current present check r.subject) ≠ some r.reason := by
  by_cases mine : r.reporter = device ∧ l.scan.decides device r.subject = true
  · refine ⟨mine.1, mine.2, fun h => gone ?_⟩
    exact listing_safe device l work present check complete r before (fun _ _ => h)
  · exact absurd (replace_keeps_others device l work r mine before) gone

/-- No listing removes this device's own positions record; only confirmation
of its publication does. -/
theorem own_positions_kept (device : Nat) (l : Listing) (work : List Work) (r : Record)
    (own : r.subject = .positions device) (before : r ∈ records work) :
    r ∈ records (replace device l work) := by
  apply replace_keeps_others device l work r _ before
  rintro ⟨-, hd⟩
  cases hs : l.scan <;> simp [Scan.decides, hs, own] at hd

/-- A record whose subject a listing names comes from that listing. -/
theorem listed_record (device : Nat) (l : Listing) (work : List Work) (s : Subject)
    (cs : List Condition) (listed : (s, cs) ∈ l.observed) (reason : Reason)
    (waiting : first cs = some reason) : ⟨s, device, reason⟩ ∈ records (replace device l work) :=
  every_pending_subject _ ⟨s, device, cs⟩ reason
    (List.mem_append_left _ (List.mem_map.mpr ⟨(s, cs), listed, rfl⟩)) waiting

theorem replace_unique (device : Nat) (l : Listing) (work : List Work)
    (valid : l.valid device = true) (u : Unique work) : Unique (replace device l work) := by
  simp only [Listing.valid, Bool.and_eq_true, List.all_eq_true, decide_eq_true_eq] at valid
  obtain ⟨inClass, distinct⟩ := valid
  simp only [Unique, keys, replace, List.map_append, List.map_map, List.nodup_append]
  refine ⟨?_, filter_unique work _ u, ?_⟩
  · have : (l.observed.map ((fun w : Work => (w.subject, w.reporter)) ∘
        fun o => (⟨o.1, device, o.2⟩ : Work))) =
        (l.observed.map (·.1)).map (fun s => (s, device)) := by
      simp [List.map_map, Function.comp_def]
    rw [this]
    exact List.Pairwise.map _ (fun a b h e => h (by simpa using e)) distinct
  · intro a ha b hb e
    obtain ⟨o, ho, rfl⟩ := List.mem_map.mp ha
    obtain ⟨w, hw, rfl⟩ := List.mem_map.mp hb
    have hk := (List.mem_filter.mp hw).2
    simp only [Function.comp_apply, Prod.mk.injEq] at e
    simp [← e.1, ← e.2, inClass o ho] at hk

/-! ## A snapshot listing keeps this device's refusals (§19.1)

A refusal of a snapshot's bytes cannot be retried by the version that made
it. Each audience listing therefore keeps it as that listed snapshot's
condition instead of checking the snapshot again; the snapshot is never
offered for loading (`never_loads_refused`, in PendingScans). A snapshot no longer
listed loses its record like any other. -/

/-- This device holds a refusal of `s`. Rust `refused_snapshots`: every
refusal recorded by this installed version is one it cannot retry. -/
def refusedHere (device : Nat) (work : List Work) (s : Subject) : Bool :=
  (records work).any fun r => r.subject == s && r.reporter == device && r.reason == .refused

/-- The listing Rust's snapshot catalog commits: a listed snapshot this device
refused keeps that refusal as its condition. A positions pass is unchanged. -/
def Listing.keepingRefusals (device : Nat) (work : List Work) (l : Listing) : Listing :=
  match l.scan with
  | .snapshots _ => ⟨l.scan, l.observed.map fun o =>
      if refusedHere device work o.1 then (o.1, [⟨false, .refused⟩]) else o⟩
  | .positions => l

theorem keepingRefusals_scan (device : Nat) (work : List Work) (l : Listing) :
    (l.keepingRefusals device work).scan = l.scan := by
  unfold Listing.keepingRefusals; split <;> rfl

theorem keepingRefusals_subjects (device : Nat) (work : List Work) (l : Listing) :
    (l.keepingRefusals device work).observed.map (·.1) = l.observed.map (·.1) := by
  unfold Listing.keepingRefusals
  split
  · simp only [List.map_map]
    congr 1
    funext o
    simp only [Function.comp_apply]
    split <;> rfl
  · rfl

theorem keepingRefusals_valid (device : Nat) (work : List Work) (l : Listing) :
    (l.keepingRefusals device work).valid device = l.valid device := by
  have hs := keepingRefusals_subjects device work l
  have hc := keepingRefusals_scan device work l
  unfold Listing.valid
  rw [hs, hc]
  congr 1
  have h₁ : (l.keepingRefusals device work).observed.all (fun o => l.scan.decides device o.1) =
      ((l.keepingRefusals device work).observed.map (·.1)).all (l.scan.decides device) := by
    simp [List.all_map, Function.comp_def]
  have h₂ : l.observed.all (fun o => l.scan.decides device o.1) =
      (l.observed.map (·.1)).all (l.scan.decides device) := by
    simp [List.all_map, Function.comp_def]
  rw [h₁, h₂, hs]

/-- A refusal of a snapshot the listing still names survives it. -/
theorem listing_keeps_refusal (device : Nat) (l : Listing) (work : List Work) (r : Record)
    (a : Audience) (snapshots : l.scan = .snapshots a) (before : r ∈ records work)
    (own : r.reporter = device) (refused : r.reason = .refused)
    (listed : ∃ cs, (r.subject, cs) ∈ l.observed) :
    r ∈ records (replace device (l.keepingRefusals device work) work) := by
  obtain ⟨cs, hcs⟩ := listed
  have here : refusedHere device work r.subject = true :=
    List.any_eq_true.mpr ⟨r, before, by simp [own, refused]⟩
  have kept : (r.subject, [⟨false, .refused⟩]) ∈ (l.keepingRefusals device work).observed := by
    unfold Listing.keepingRefusals
    rw [snapshots]
    exact List.mem_map.mpr ⟨(r.subject, cs), hcs, by simp [here]⟩
  have h := listed_record device _ work r.subject _ kept .refused (by simp [first])
  cases r
  simp only at own refused
  subst own refused
  exact h

/-! ## A completed store-log scan decides this device's entry waits (§19.1)

Each scan reads the received store log and this device's queued entry. The
waits it finds (an entry's missing key, its newer format, or the first entry
of its recorded past that has not applied) replace every entry wait this
device recorded before. A refusal or a drop of an entry is a verdict on its
bytes, not a wait: the scan's replacement never selects it. A named entry's
new wait replaces whatever record that entry had, as Rust saves it.
`PendingScans` computes the waits from what the scan read. -/

/-- The reasons a store-log scan decides for an entry: Rust `replace_entry_waits`. -/
def entryWait : Reason → Bool
  | .waits _ | .keyUnavailable _ _ | .updateRequired _ => true
  | _ => false

/-- A record one scan decides: this device's wait on an entry. -/
def decidedByScan (device : Nat) (r : Record) : Bool :=
  r.reporter == device && entryWait r.reason &&
    match r.subject with
    | .entry _ _ => true
    | _ => false

/-- The waits one completed scan found, keyed by entry `(device, number)`. -/
structure EntryScan where
  waits : List ((Nat × Nat) × Reason)
  deriving DecidableEq, Repr

/-- Rust refuses a replacement naming an entry twice or with another reason. -/
def EntryScan.valid (scan : EntryScan) : Bool :=
  scan.waits.all (fun w => entryWait w.2) && decide (scan.waits.map (·.1)).Nodup

def EntryScan.names (scan : EntryScan) (s : Subject) : Bool :=
  scan.waits.any fun w => s == .entry w.1.1 w.1.2

def scanRecord (device : Nat) (w : (Nat × Nat) × Reason) : Record :=
  ⟨.entry w.1.1 w.1.2, device, w.2⟩

/-- The work whose record the scan decides. -/
def scannedWork (device : Nat) (w : Work) : Bool :=
  match first w.conditions with
  | some reason => decidedByScan device ⟨w.subject, w.reporter, reason⟩
  | none => false

/-- Rust `replace_local_pending`: decided records the scan does not name are
cleared, and each named entry's record is saved over its previous one. -/
def scanEntries (device : Nat) (scan : EntryScan) (work : List Work) : List Work :=
  scan.waits.map (fun w => ⟨.entry w.1.1 w.1.2, device, [⟨false, w.2⟩]⟩) ++
    work.filter fun w => !(scannedWork device w || (w.reporter == device && scan.names w.subject))

/-- Filtering work filters its records by the same test on each record. -/
theorem records_filter (work : List Work) (p : Work → Bool) (q : Record → Bool)
    (agree : ∀ w reason, first w.conditions = some reason → p w = q ⟨w.subject, w.reporter, reason⟩) :
    records (work.filter p) = (records work).filter q := by
  induction work with
  | nil => rfl
  | cons w ws ih =>
      cases hf : first w.conditions with
      | none =>
          have step : records (w :: ws) = records ws := by simp [records, hf]
          cases hp : p w
          · simpa [hp, step] using ih
          · have h : records (w :: ws.filter p) = records (ws.filter p) := by simp [records, hf]
            simpa [List.filter_cons, hp, h, step] using ih
      | some reason =>
          have hq := agree w reason hf
          have step : records (w :: ws) = ⟨w.subject, w.reporter, reason⟩ :: records ws := by
            simp [records, hf]
          cases hp : p w
          · simpa [hp, step, ← hq] using ih
          · have h : records (w :: ws.filter p) =
                ⟨w.subject, w.reporter, reason⟩ :: records (ws.filter p) := by simp [records, hf]
            simpa [List.filter_cons, hp, h, step, ← hq] using ih

theorem records_scanEntries (device : Nat) (scan : EntryScan) (work : List Work) :
    records (scanEntries device scan work) = scan.waits.map (scanRecord device) ++
      (records work).filter fun r =>
        !(decidedByScan device r || (r.reporter == device && scan.names r.subject)) := by
  unfold scanEntries
  rw [records, List.filterMap_append]
  congr 1
  · simp [List.filterMap_map, Function.comp_def, first, scanRecord]
  · exact records_filter work _ _ fun w reason hf => by simp [scannedWork, hf]

theorem names_entry (scan : EntryScan) (s : Subject) (named : scan.names s = true) :
    ∃ e, s = .entry e.1 e.2 ∧ ∃ r, (e, r) ∈ scan.waits := by
  obtain ⟨w, hw, he⟩ := List.any_eq_true.mp named
  exact ⟨w.1, beq_iff_eq.mp he, w.2, hw⟩

/-- After a scan this device's wait on an entry is exactly the wait the scan
found: none is kept after the scan stops finding it, and each one found is
visible. -/
theorem entry_scan_decides (device : Nat) (scan : EntryScan) (work : List Work)
    (e : Nat × Nat) (reason : Reason) (wait : entryWait reason = true) :
    ⟨.entry e.1 e.2, device, reason⟩ ∈ records (scanEntries device scan work) ↔
      (e, reason) ∈ scan.waits := by
  rw [records_scanEntries]
  constructor
  · intro h
    rcases List.mem_append.mp h with h | h
    · obtain ⟨w, hw, he⟩ := List.mem_map.mp h
      simp only [scanRecord, Record.mk.injEq, Subject.entry.injEq] at he
      obtain ⟨⟨h₁, h₂⟩, -, h₃⟩ := he
      have : w = (e, reason) := by
        obtain ⟨⟨a, b⟩, c⟩ := w
        simp only at h₁ h₂ h₃
        rw [h₁, h₂, h₃]
      exact this ▸ hw
    · have := (List.mem_filter.mp h).2
      simp [decidedByScan, wait] at this
  · intro h
    exact List.mem_append_left _ (List.mem_map.mpr ⟨(e, reason), h, rfl⟩)

/-- A record the scan does not decide or name stays: peers' reports, other
subjects, and this device's refusals and drops of entries it does not name. -/
theorem entry_scan_keeps_others (device : Nat) (scan : EntryScan) (work : List Work)
    (r : Record) (before : r ∈ records work) (undecided : decidedByScan device r = false)
    (unnamed : ¬(r.reporter = device ∧ scan.names r.subject = true)) :
    r ∈ records (scanEntries device scan work) := by
  rw [records_scanEntries]
  refine List.mem_append_right _ (List.mem_filter.mpr ⟨before, ?_⟩)
  cases hn : scan.names r.subject
  · simp [undecided]
  · have : (r.reporter == device) = false := by
      cases hr : r.reporter == device
      · rfl
      · exact absurd ⟨beq_iff_eq.mp hr, hn⟩ unnamed
    simp [undecided, this]

/-- A verdict on an entry's bytes survives every scan that does not observe
that entry anew. -/
theorem entry_scan_keeps_verdicts (device : Nat) (scan : EntryScan) (work : List Work)
    (r : Record) (before : r ∈ records work) (verdict : entryWait r.reason = false)
    (unnamed : scan.names r.subject = false) : r ∈ records (scanEntries device scan work) :=
  entry_scan_keeps_others device scan work r before (by simp [decidedByScan, verdict])
    (by simp [unnamed])

/-- No record is removed while its condition holds: this device's wait that
the scan still finds survives it. -/
theorem entry_scan_safe (device : Nat) (scan : EntryScan) (work : List Work) (r : Record)
    (e : Nat × Nat) (subject : r.subject = .entry e.1 e.2) (own : r.reporter = device)
    (holds : (e, r.reason) ∈ scan.waits) : r ∈ records (scanEntries device scan work) := by
  rw [records_scanEntries]
  refine List.mem_append_left _ (List.mem_map.mpr ⟨(e, r.reason), holds, ?_⟩)
  cases r
  simp only at subject own
  simp [scanRecord, subject, own]

/-- A record a scan removes was this device's record of an entry whose wait
the scan no longer finds: either a wait that ended, or an older record of an
entry the scan now finds waiting. -/
theorem entry_scan_removes_only_ended (device : Nat) (scan : EntryScan) (work : List Work)
    (r : Record) (before : r ∈ records work) (gone : r ∉ records (scanEntries device scan work)) :
    r.reporter = device ∧ ∃ e, r.subject = .entry e.1 e.2 ∧ (e, r.reason) ∉ scan.waits ∧
      (entryWait r.reason = true ∨ scan.names r.subject = true) := by
  by_cases hd : decidedByScan device r = true
  · have hd' := hd
    simp only [decidedByScan, Bool.and_eq_true, beq_iff_eq] at hd'
    obtain ⟨⟨own, wait⟩, hs⟩ := hd'
    refine ⟨own, ?_⟩
    match h : r.subject, hs with
    | .entry d n, _ =>
        exact ⟨(d, n), rfl, fun holds => gone (entry_scan_safe device scan work r (d, n) h own holds),
          .inl wait⟩
  · by_cases named : r.reporter = device ∧ scan.names r.subject = true
    · obtain ⟨own, hn⟩ := named
      obtain ⟨e, hs, -⟩ := names_entry scan r.subject hn
      exact ⟨own, e, hs, fun holds => gone (entry_scan_safe device scan work r e hs own holds), .inr hn⟩
    · exact absurd (entry_scan_keeps_others device scan work r before (by simpa using hd) named) gone

theorem scanEntries_unique (device : Nat) (scan : EntryScan) (work : List Work)
    (valid : scan.valid = true) (u : Unique work) : Unique (scanEntries device scan work) := by
  simp only [EntryScan.valid, Bool.and_eq_true, decide_eq_true_eq] at valid
  obtain ⟨-, distinct⟩ := valid
  simp only [Unique, keys, scanEntries, List.map_append, List.map_map, List.nodup_append]
  refine ⟨?_, filter_unique work _ u, ?_⟩
  · have : (scan.waits.map ((fun w : Work => (w.subject, w.reporter)) ∘
        fun w => (⟨.entry w.1.1 w.1.2, device, [⟨false, w.2⟩]⟩ : Work))) =
        (scan.waits.map (·.1)).map (fun e => ((Subject.entry e.1 e.2), device)) := by
      simp [List.map_map, Function.comp_def]
    rw [this]
    refine List.Pairwise.map _ (fun a b h e => h ?_) distinct
    simp only [Prod.mk.injEq, Subject.entry.injEq] at e
    exact Prod.ext e.1.1 e.1.2
  · intro a ha b hb e
    obtain ⟨w, hw, rfl⟩ := List.mem_map.mp ha
    obtain ⟨v, hv, rfl⟩ := List.mem_map.mp hb
    have hk := (List.mem_filter.mp hv).2
    simp only [Function.comp_apply, Prod.mk.injEq] at e
    have named : scan.names v.subject = true :=
      List.any_eq_true.mpr ⟨w, hw, by rw [← e.1]; exact beq_self_eq_true _⟩
    simp [← e.2, named] at hk

/-! ## One device's pending registry across its steps -/

/-- This installation's registry: its own device, its write positions and the
observed work. Peer reports are work with another reporter. -/
structure Local where
  device : Nat
  positions : Positions
  work : List Work

inductive Step where
  /-- One subject's observation, replacing its previous one (`observe`). -/
  | observe (next : Work)
  /-- A snapshot load installing positions, which ends covered waits. -/
  | cover (jump : List (Nat × Nat))
  /-- A completed listing replacing its class; a snapshot listing keeps this
  device's refusals of the snapshots it names. -/
  | listing (l : Listing)
  /-- A completed store-log scan replacing this device's entry waits. -/
  | entries (scan : EntryScan)

/-- An invalid listing or scan is refused and changes nothing. -/
def Local.step (s : Local) : Step → Local
  | .observe next => { s with work := observe s.work next }
  | .cover jump =>
      { s with positions := raise s.positions jump,
               work := cover s.device (raise s.positions jump) s.work }
  | .listing l =>
      if l.valid s.device then
        { s with work := replace s.device (l.keepingRefusals s.device s.work) s.work }
      else s
  | .entries scan =>
      if scan.valid then { s with work := scanEntries s.device scan s.work } else s

def Local.run (s : Local) (steps : List Step) : Local := steps.foldl Local.step s

theorem step_unique (s : Local) (step : Step) (u : Unique s.work) : Unique (s.step step).work := by
  cases step with
  | observe next => exact observe_unique _ _ u
  | cover jump => exact cover_unique _ _ _ u
  | listing l =>
      cases hv : l.valid s.device
      · simpa [Local.step, hv] using u
      · have hk : (l.keepingRefusals s.device s.work).valid s.device = true := by
          rw [keepingRefusals_valid, hv]
        simpa [Local.step, hv] using replace_unique _ _ _ hk u
  | entries scan =>
      cases hv : scan.valid
      · simpa [Local.step, hv] using u
      · simpa [Local.step, hv] using scanEntries_unique _ _ _ hv u

/-- Every reachable registry has at most one work item per subject and
reporting device. -/
theorem history_unique (steps : List Step) (device : Nat) (p : Positions) :
    Unique (Local.run ⟨device, p, []⟩ steps).work := by
  suffices ∀ s : Local, Unique s.work → Unique (s.run steps).work from this _ List.nodup_nil
  induction steps with
  | nil => intro s u; exact u
  | cons step rest ih => intro s u; exact ih _ (step_unique s step u)

/-- Hence one record per subject and reporting device (§19.1); each record is
its work's first unmet condition by `only_first_reason`. -/
theorem history_one_record (steps : List Step) (device : Nat) (p : Positions)
    (r₁ r₂ : Record) (h₁ : r₁ ∈ records (Local.run ⟨device, p, []⟩ steps).work)
    (h₂ : r₂ ∈ records (Local.run ⟨device, p, []⟩ steps).work)
    (hs : r₁.subject = r₂.subject) (hr : r₁.reporter = r₂.reporter) : r₁ = r₂ :=
  one_record _ (history_unique steps device p) r₁ r₂ h₁ h₂ hs hr

/-- Positions never move back across any history, so a covered write stays unscheduled. -/
theorem history_positions_monotone (steps : List Step) (s : Local) (d : Nat) :
    s.positions d ≤ (s.run steps).positions d := by
  induction steps generalizing s with
  | nil => exact Nat.le_refl _
  | cons step rest ih =>
      refine Nat.le_trans ?_ (ih (s.step step))
      cases step with
      | observe => exact Nat.le_refl _
      | cover jump => exact raise_monotone _ _ _
      | listing l => cases hv : l.valid s.device <;> simp [Local.step, hv]
      | entries scan => cases hv : scan.valid <;> simp [Local.step, hv]


/-! ### Checked histories

Ana's tablet (device 2) observes Ben's (device 7) writes 1 and 3 waiting for
missing objects and write 2 refused. A snapshot load covers Ben's log through
write 2: write 1's wait ends, the refusal stays and write 3 still waits. -/

private def waits (s : Subject) (path : String) : Step :=
  .observe ⟨s, 2, [⟨false, .missing path⟩]⟩

private def coverHistory : List Step :=
  [waits (.write 7 1) "w1", .observe ⟨.write 7 2, 2, [⟨true, .missing "w2"⟩, ⟨false, .refused⟩]⟩,
    waits (.write 7 3) "w3", .cover [(7, 2)]]

example : records (Local.run ⟨2, fun _ => 0, []⟩ coverHistory).work =
    [⟨.write 7 3, 2, .missing "w3"⟩, ⟨.write 7 2, 2, .refused⟩] := by decide

/-- Ana's own positions publication failed. Peer 3's positions wait for its
registration and peer 4's fail their check. The next pass lists only peer 4,
now valid: both peer records end, while the snapshot wait from another
listing and Ana's own publication failure stay. -/
private def listingHistory : List Step :=
  [.observe ⟨.positions 2, 2, [⟨false, .storage true⟩]⟩,
    .listing ⟨.positions, [(.positions 3, [⟨false, .waits (.deviceRegistration 3)⟩]),
      (.positions 4, [⟨false, .invalidPositions⟩])]⟩,
    .listing ⟨.snapshots .store, [(.snapshot ⟨.store, 1⟩, [⟨false, .waits (.deviceRegistration 5)⟩])]⟩,
    .listing ⟨.positions, [(.positions 4, [⟨true, .invalidPositions⟩])]⟩]

example : records (Local.run ⟨2, fun _ => 0, []⟩ listingHistory).work =
    [⟨.snapshot ⟨.store, 1⟩, 2, .waits (.deviceRegistration 5)⟩,
      ⟨.positions 2, 2, .storage true⟩] := by decide

/-- A positions listing that names this device's own subject is refused. -/
example : (Local.run ⟨2, fun _ => 0, []⟩
    [.listing ⟨.positions, [(.positions 2, [⟨false, .invalidPositions⟩])]⟩]).work = [] := by
  decide

/-- A listing naming one subject twice is refused and changes nothing. -/
example : (Local.run ⟨2, fun _ => 0, []⟩
    [.listing ⟨.positions, [(.positions 3, []), (.positions 3, [⟨false, .refused⟩])]⟩]).work = [] := by
  decide

/-- Ana's tablet (device 2) refused Ben's entry (7, 2), and peer 4 reports
entry (5, 3) waiting. Ana's first scan finds (7, 1) waiting for its key and
(5, 3) for entry (5, 2). Her second finds only (5, 3) still waiting: (7, 1)'s
wait ends, while her refusal and the peer's report stay. -/
private def entryHistory : List Step :=
  [.observe ⟨.entry 7 2, 2, [⟨false, .refused⟩]⟩,
    .observe ⟨.entry 5 3, 4, [⟨false, .waits (.object "e5/2")⟩]⟩,
    .entries ⟨[((7, 1), .keyUnavailable .store 5), ((5, 3), .waits (.object "e5/2"))]⟩,
    .entries ⟨[((5, 3), .waits (.object "e5/2"))]⟩]

example : records (Local.run ⟨2, fun _ => 0, []⟩ entryHistory).work =
    [⟨.entry 5 3, 2, .waits (.object "e5/2")⟩, ⟨.entry 5 3, 4, .waits (.object "e5/2")⟩,
      ⟨.entry 7 2, 2, .refused⟩] := by decide

/-- A scan naming an entry twice is refused and changes nothing. -/
example : (Local.run ⟨2, fun _ => 0, []⟩
    [.entries ⟨[((7, 1), .keyUnavailable .store 5), ((7, 1), .updateRequired 3)]⟩]).work = [] := by
  decide

/-- Loading refused the body of Ben's store snapshot 1. The next listing finds
its prefix verified and snapshot 2 in a newer format: the refusal stays and
snapshot 2 waits for the update. Once snapshot 1 is deleted and snapshot 2
reads, the listing clears both. -/
private def refusalHistory : List Step :=
  [.observe ⟨.snapshot ⟨.store, 1⟩, 2, [⟨false, .refused⟩]⟩,
    .listing ⟨.snapshots .store, [(.snapshot ⟨.store, 1⟩, []),
      (.snapshot ⟨.store, 2⟩, [⟨false, .updateRequired 3⟩])]⟩]

example : records (Local.run ⟨2, fun _ => 0, []⟩ refusalHistory).work =
    [⟨.snapshot ⟨.store, 1⟩, 2, .refused⟩, ⟨.snapshot ⟨.store, 2⟩, 2, .updateRequired 3⟩] := by
  decide

example : records (Local.run ⟨2, fun _ => 0, []⟩
    (refusalHistory ++ [.listing ⟨.snapshots .store, [(.snapshot ⟨.store, 2⟩, [])]⟩])).work = [] := by
  decide

end CovenStorelogData.Pending
