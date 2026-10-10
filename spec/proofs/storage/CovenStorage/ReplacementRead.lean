import CovenStorage.Identity

/-! A variant of §10: replacement records only the old id, and admission uses
the object's recorded store-log past, as for removal. The closed-end model in
`Identity` remains a separate model of §10's written rule and D6's format. -/
namespace CovenStorage.ReplacementRead

/-- The add-device entry's identity is its new device and entry number.
Authority, permanent time rejection and replay supply the kept registrations. -/
structure Registration where
  old : Nat
  fresh : Nat
  number : Nat
  deriving DecidableEq, Repr

def Registration.entry (r : Registration) : Path := .entry r.fresh r.number

def allowed (owner author : Nat) (r : Registration) : Bool :=
  owner == author && r.old != r.fresh

theorem registration_authority (owner author : Nat) (r : Registration)
    (h : allowed owner author r = true) : owner = author ∧ r.old ≠ r.fresh := by
  simpa [allowed] using h

/-- D5's store_log_read, or D6's had_read expanded with own earlier entries.
Authentication, parsing and the other admission rules have already succeeded. -/
structure LogObject where
  device : Nat
  number : Nat
  kind : LogKind
  past : List Path
  deriving DecidableEq, Repr

def LogObject.path (o : LogObject) : Path :=
  match o.kind with
  | .writes => .write o.device o.number
  | .entries => .entry o.device o.number

/-- Any kept replacement read by this old id excludes its later object.
Publication time and the replacing device's observed counters play no part. -/
def accepts (kept : List Registration) (o : LogObject) : Bool :=
  kept.all fun r => !(r.old == o.device && r.entry ∈ o.past)

theorem accepts_iff (kept : List Registration) (o : LogObject) :
    accepts kept o = true ↔ ∀ r ∈ kept, r.old = o.device → r.entry ∉ o.past := by
  simp [accepts, List.all_eq_true]
  grind

theorem before_read_counts (kept : List Registration) (o : LogObject)
    (before : ∀ r ∈ kept, r.old = o.device → r.entry ∉ o.past) :
    accepts kept o = true := (accepts_iff kept o).mpr before

theorem after_read_rejected (kept : List Registration) (o : LogObject)
    (r : Registration) (hr : r ∈ kept) (old : r.old = o.device)
    (read : r.entry ∈ o.past) : accepts kept o = false := by
  cases h : accepts kept o
  · rfl
  · exact False.elim ((accepts_iff kept o).mp h r hr old read)

theorem replacement_order_irrelevant (a b : List Registration)
    (same : ∀ r, r ∈ a ↔ r ∈ b) (o : LogObject) : accepts a o = accepts b o := by
  apply Bool.eq_iff_iff.mpr
  simp only [accepts_iff, same]

/-- Rebuild from retained received objects when replay changes the kept
replacements (§9). No rejected input is destroyed by this operation. -/
def load (kept : List Registration) (received : List LogObject) : Applied :=
  receiveAll [] ((received.filter (accepts kept)).map LogObject.path)

theorem loaded_iff (kept : List Registration) (received : List LogObject) (p : Path) :
    p ∈ load kept received ↔
      ∃ o ∈ received, accepts kept o = true ∧ o.path = p := by
  simp [load, receipt_members, List.mem_map, List.mem_filter, and_assoc]

theorem no_duplicate_application (kept : List Registration) (received : List LogObject) :
    (load kept received).Nodup := receipt_no_duplicates _ _ (by simp)

/-- Every delivered stored object made before its copy read any kept
replacement is represented, even if it lands after those replacements. -/
theorem no_stored_write_lost (kept : List Registration) (received : List LogObject)
    (o : LogObject) (delivered : o ∈ received)
    (before : ∀ r ∈ kept, r.old = o.device → r.entry ∉ o.past) :
    o.path ∈ load kept received :=
  (loaded_iff kept received o.path).mpr
    ⟨o, delivered, before_read_counts kept o before, rfl⟩

theorem stored_object_survives (storage : Storage) (requests : List Publication)
    (kept : List Registration) (received : List LogObject) (o : LogObject) (value : Object)
    (stored : storage o.path = some value) (delivered : o ∈ received)
    (before : ∀ r ∈ kept, r.old = o.device → r.entry ∉ o.past) :
    publishAll storage requests o.path = some value ∧ o.path ∈ load kept received :=
  ⟨stored_history_preserved storage requests o.path value stored,
    no_stored_write_lost kept received o delivered before⟩

/-- Agreement is on the set of applied identities, not arrival-order lists.
Storage's create-once rule supplies the same bytes for each such identity. -/
theorem devices_converge (a b : List Registration) (xs ys : List LogObject)
    (replacements : ∀ r, r ∈ a ↔ r ∈ b) (objects : ∀ o, o ∈ xs ↔ o ∈ ys) :
    (fun p => decide (p ∈ load a xs)) = (fun p => decide (p ∈ load b ys)) := by
  funext p
  simp only [loaded_iff, objects, replacement_order_irrelevant a b replacements]

theorem receipt_after_restore (fresh number : Nat) (kept : List Registration)
    (snapshot later : List LogObject) :
    (receiveAll (loadDevice fresh number (load kept snapshot)).applied (load kept later)).Nodup ∧
    ∀ p, p ∈ receiveAll (loadDevice fresh number (load kept snapshot)).applied (load kept later) ↔
      p ∈ load kept (snapshot ++ later) := by
  constructor
  · exact receipt_no_duplicates _ _ (receipt_no_duplicates [] _ (by simp))
  · intro p
    simp only [loadDevice, receipt_members, List.not_mem_nil, false_or, loaded_iff,
      List.mem_append]
    constructor
    · rintro (⟨o, ho, ha, hp⟩ | ⟨o, ho, ha, hp⟩)
      · exact ⟨o, Or.inl ho, ha, hp⟩
      · exact ⟨o, Or.inr ho, ha, hp⟩
    · rintro ⟨o, ho | ho, ha, hp⟩
      · exact Or.inl ⟨o, ho, ha, hp⟩
      · exact Or.inr ⟨o, ho, ha, hp⟩

inductive Device where
  | active (state : Local) (past : List Path)
  | resetting (registration : Registration) (reason : ResetReason)
  deriving DecidableEq, Repr

/-- The observing copy chooses its own fresh id. It does not take over the
other copy's replacement id. Fresh-id generation is supplied by bootstrap. -/
def observe (d : Device) (r : Registration) (fresh : Nat) : Device :=
  match d with
  | .resetting _ _ => d
  | .active l past =>
    if l.device = r.old then .resetting ⟨l.device, fresh, 1⟩ .replaced
    else .active l (applyOnce past r.entry)

def commit (d : Device) (bytes : Bytes) : Device × Option LogObject :=
  match d with
  | .resetting _ _ => (d, none)
  | .active l past =>
    let next := commitWrite l bytes
    (.active next past, some ⟨l.device, next.reserved.logs.writes, .writes, past⟩)

def gate (d : Device) (kind : SendKind) (custody : Option Nat)
    (scan : Except ReadFailure Evidence) : Gate :=
  match d with
  | .active l _ => authorizeSend kind l.device l.reserved custody scan
  | .resetting _ reason => .reset reason

def finish (r : Registration) (loaded : Applied) (past : List Path) : Device :=
  .active (loadDevice r.fresh r.number loaded) past

theorem old_copy_stops (l : Local) (past : List Path) (r : Registration)
    (fresh : Nat) (old : l.device = r.old) (bytes : Bytes)
    (kind : SendKind) (custody : Option Nat) (scan : Except ReadFailure Evidence) :
    observe (.active l past) r fresh = .resetting ⟨l.device, fresh, 1⟩ .replaced ∧
    (commit (observe (.active l past) r fresh) bytes).2 = none ∧
    gate (observe (.active l past) r fresh) kind custody scan = .reset .replaced := by
  simp [observe, old, commit, gate]

theorem observed_replacement_blocks_send (l : Local) (past : List Path)
    (kind : SendKind) (custody : Option Nat) (e : Evidence) (replaced : e.replaced = true) :
    gate (.active l past) kind custody (.ok e) ≠ .send := by
  intro h
  have := (send_not_ahead l.device l.reserved custody e h).2.1
  simp [replaced] at this

theorem missing_custody_blocks_restore (l : Local) (past : List Path)
    (kind : SendKind) (scan : Except ReadFailure Evidence) :
    gate (.active l past) kind none scan = .reset .identityMismatch := by
  simp [gate, authorizeSend, checkIdentity]

theorem reset_uses_new_path (r : Registration) (loaded past : List Path) (bytes : Bytes)
    (distinct : r.old ≠ r.fresh) (oldNumber : Nat) :
    (commit (finish r loaded past) bytes).2 = some ⟨r.fresh, 1, .writes, past⟩ ∧
    (LogObject.mk r.fresh 1 .writes past).path ≠ .write r.old oldNumber := by
  constructor
  · rfl
  · simp only [LogObject.path, ne_eq, Path.write.injEq]
    exact fun h => distinct h.1.symm

end CovenStorage.ReplacementRead
