import CovenMerge.ReceiveMerge
import CovenMerge.ReceivePositions

namespace CovenMerge.Receive.Examples

inductive Write where
  | create | edit | delete
  deriving DecidableEq, Repr

open Write

def history : Writes Write Unit Unit where
  ts | .create => 1 | .edit => 2 | .delete => 3
  past w a := decide (w ≠ .create ∧ a = .create)
  chg w _ := some ⟨match w with | .create => .ins | .edit => .upd | .delete => .del,
    if w = .create then 0 else 1, fun _ => w != .delete⟩

theorem authored_valid : Valid history where
  ts_inj a b := by cases a <;> cases b <;> simp [history]
  past_ts w a := by cases w <;> cases a <;> decide
  gen_seen w r ch h := by
    cases w <;> simp only [history, reduceCtorEq, ↓reduceIte, Option.some.injEq] at h
    · subst ch; exact Or.inl rfl
    · subst ch; exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
    · subst ch; exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
  parity w r ch h := by
    cases w <;> simp only [history, reduceCtorEq, ↓reduceIte, Option.some.injEq] at h <;>
      subst ch <;> decide

def noSnapshot : SnapshotBoundary Write := ⟨fun _ => false, fun _ => false, by simp⟩

def logs : Logs Write where
  device | .create => 0 | .edit => 1 | .delete => 2
  number _ := 1

def policy : Policy Write where
  boundary := noSnapshot
  logs := logs
  reset := none
  -- The creation was authored before retirement. Its readers saw the entry
  -- while replay dropped it; their own devices are not retired by that entry.
  readEntry w e := decide (w ≠ .create ∧ e = 0)
  schema _ := .eligible
  retirements := [⟨0, 0, 0⟩]
  stored | .create => 30 * 86400 + 1 | _ => 30 * 86400 + 2
  viewRetired _ := false

def past : Write → List Write
  | .create => []
  | .edit | .delete => [.create]

theorem exact_past : ∀ w a, a ∈ past w ↔ history.past w a = true := by
  intro w a; cases w <;> cases a <;> decide

def received : State Write := ⟨[.edit, .delete, .create], []⟩
def pass (s : State Write) (w : Write) : State Write :=
  attempt noSnapshot past s (fun _ => true) w

/-- Even with every object downloaded, the effect waits for its cause to be
consumed. Download completion alone does not satisfy a cause. -/
example : pass received .edit = received := by decide

/-- All three downloads are legal in this order; each is its writer's first
number. No missing same-device number is bypassed. -/
example : Step noSnapshot logs past (fun _ => true) (.download .create)
    ⟨[.edit, .delete], []⟩ received := by
  apply Step.download <;> simp [noSnapshot, logs] <;> omega

example : (pass (pass received .create) .edit).passed = [.create, .edit] := by decide

/-- Exact data used by the original edit and delete, independently of the
positions they passed. Both replaced the creation's cell at generation one. -/
def provenance : DataDependency.Authored history where
  provenance w _ := if w = .create then ⟨none, []⟩ else ⟨some .create, [((), .create)]⟩
  read_source w r a h := by
    cases w <;> cases a <;> simp_all [DataDependency.Provenance.sources, history]
  generation_source w r ch h := by
    cases w <;> simp only [history, reduceCtorEq, ↓reduceIte, Option.some.injEq] at h
    · subst ch; exact Or.inl ⟨rfl, rfl⟩
    · subst ch; exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
    · subst ch; exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
  setter_source w r c a h := by
    cases w <;> simp at h
    all_goals subst a; exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

def values (w : Write) (_r _c : Unit) : Option Write :=
  some (if w = .delete then .create else w)
def snapshot : MergeSnapshot Write Unit Unit := ⟨St.init, []⟩
def output (rules : St Write Unit Unit → EntryInputs Unit Unit) (s : State Write) :=
  result history authored_valid provenance policy snapshot values rules s

theorem changes_dropped (w : Write) :
    DataDependency.retained (DataDependency.authoredState history authored_valid provenance (base policy)) w () = false := by
  have first := DataDependency.dropped_stays_dropped
    (DataDependency.authoredState history authored_valid provenance (base policy)) .create () (by decide)
  cases w with
  | create => exact first
  | edit | delete =>
    rw [DataDependency.retained_eq]
    change (true && true && [Write.create, .create].all _) = false
    simp [first]

theorem changes_restored (w : Write) :
    DataDependency.retained (DataDependency.authoredState history authored_valid provenance
      (base { policy with retirements := [] })) w () = true := by
  have first : DataDependency.retained (DataDependency.authoredState history authored_valid provenance
      (base { policy with retirements := [] })) .create () = true := by
    rw [DataDependency.retained_eq]
    rfl
  cases w with
  | create => exact first
  | edit | delete =>
    rw [DataDependency.retained_eq]
    change (true && true && [Write.create, .create].all _) = true
    simp [first]

theorem retirement_generation_accounted :
    withinWindow policy .create = false ∧
    Valid (effective history authored_valid provenance policy) ∧
    (effective history authored_valid provenance policy).chg .edit () = none :=
  ⟨by decide, DataDependency.effective_valid _ _ _ _, by simp [effective, DataDependency.effective, changes_dropped]⟩

def creationAndEdit : State Write := pass (pass ⟨[.create, .edit], []⟩ .create) .edit

def editThenDelete : State Write := pass (pass (pass received .create) .edit) .delete
def deleteThenEdit : State Write := pass (pass (pass received .create) .delete) .edit

/-- Both histories are complete receive runs: downloads in per-log order,
followed by three causally ready attempts, then idle. -/
def arrivalRun (editFirst : Bool) : Run noSnapshot logs past (fun _ _ => true) where
  state
    | 0 => initial
    | 1 => ⟨[.edit], []⟩
    | 2 => ⟨[.edit, .delete], []⟩
    | 3 => received
    | 4 => pass received .create
    | 5 => pass (pass received .create) (if editFirst then .edit else .delete)
    | _ => if editFirst then editThenDelete else deleteThenEdit
  action
    | 0 => .download .edit
    | 1 => .download .delete
    | 2 => .download .create
    | 3 => .attempt .create
    | 4 => .attempt (if editFirst then .edit else .delete)
    | 5 => .attempt (if editFirst then .delete else .edit)
    | _ => .idle
  start := rfl
  next t := by
    cases editFirst <;> match t with
    | 0 => apply Step.download <;> simp [initial, noSnapshot, logs] <;> omega
    | 1 => apply Step.download <;> simp [noSnapshot, logs] <;> omega
    | 2 => apply Step.download <;> simp [noSnapshot, logs] <;> omega
    | 3 => exact .attempt _ _
    | 4 => exact .attempt _ _
    | 5 => exact .attempt _ _
    | _ + 6 => exact .idle _

theorem retirement_value_recorded (rules : St Write Unit Unit → EntryInputs Unit Unit) :
    ((output rules creationAndEdit).excluded .edit ()).map (fun l =>
      (l.record.generation, l.record.values (), l.record.cause)) =
      some (1, some .edit, .dependency .edit [.create, .create]) := by
  simp only [output, result, Data.loss, changes_dropped, Bool.false_eq_true, ↓reduceIte,
    dependentLoss, DataDependency.droppedSources, Bool.not_false]
  simp [creationAndEdit, pass, attempt, enabled,
    ready, consumed, past, noSnapshot, base, policy, snapshotRetains, boundaryEligible,
    resetAllows, withinWindow, retirementAllows, logs, history, freeze, DataDependency.authoredState, provenance,
    DataDependency.Provenance.sources, values]

theorem retirement_value_accounted (rules : St Write Unit Unit → EntryInputs Unit Unit) :
    Data.ValueAccounted (effective history authored_valid provenance policy)
      (inputs policy creationAndEdit) (output rules creationAndEdit).current.merged
      (rules (output rules creationAndEdit).current.merged)
      (output rules creationAndEdit).excluded () () .edit 1 := by
  have recorded := retirement_value_recorded rules
  cases found : (output rules creationAndEdit).excluded .edit () with
  | none => simp [found] at recorded
  | some loss =>
    simp only [found, Option.map_some, Option.some.injEq] at recorded
    exact Or.inr ⟨loss, found, congrArg (fun x => x.2.1) recorded⟩

theorem retirement_loss_agrees (rules : St Write Unit Unit → EntryInputs Unit Unit) :
    ((output rules editThenDelete).excluded .edit ()).map (fun l => l.record.cause) =
      some (.dependency .edit [.create, .create]) ∧
    ((output rules deleteThenEdit).excluded .edit ()).map (fun l => l.record.cause) =
      some (.dependency .edit [.create, .create]) ∧
    ((output rules editThenDelete).excluded .delete ()).map (fun l => l.record.cause) =
      some (.dependency .delete [.create, .create]) ∧
    (output rules editThenDelete).excluded .create () = none := by
  simp only [output, result, Data.loss, changes_dropped, Bool.false_eq_true, ↓reduceIte,
    dependentLoss, DataDependency.droppedSources, Bool.not_false]
  simp [editThenDelete, deleteThenEdit,
    received, pass, attempt, enabled, ready, consumed, past, noSnapshot, base, policy,
    snapshotRetains, boundaryEligible, resetAllows, withinWindow, retirementAllows,
    logs, history, freeze,
    DataDependency.authoredState, provenance, DataDependency.Provenance.sources]

/-- The two legal runs agree in all merge, app-view and loss fields. -/
theorem receive_runs_agree (rules : St Write Unit Unit → EntryInputs Unit Unit) :
    output rules ((arrivalRun true).state 6) = output rules ((arrivalRun false).state 6) := by
  apply receive_result_converges history authored_valid provenance policy past exact_past
    (fun _ _ => true) (fun _ _ => true) (arrivalRun true) (arrivalRun false)
    (by intro w h; cases h) snapshot
    (isSpec_congr (by intro w; simp [policy, noSnapshot]) isSpec_init) values rules
  intro w; cases w <;> decide

example (rules : St Write Unit Unit → EntryInputs Unit Unit) :
    output rules editThenDelete = output rules deleteThenEdit := receive_runs_agree rules

/-- Retirement replay may restore a base by changing its own admission.
Receipt of an active reader never changes that admission. -/
theorem replay_restores_by_own_window :
    withinWindow { policy with retirements := [] } .create = true ∧
    (merged history authored_valid provenance { policy with retirements := [] }
      St.init creationAndEdit).cell () () = some .edit ∧
    (merged history authored_valid provenance policy St.init creationAndEdit).cell () () = none := by
  simp only [merged, Data.merged, DataDependency.effective]
  have dropped : (fun w r => if DataDependency.retained
      (DataDependency.authoredState history authored_valid provenance (base policy)) w r
      then history.chg w r else none) = (fun _ _ => none) := by
    funext w r; cases r; simp [changes_dropped]
  have restored : (fun w r => if DataDependency.retained
      (DataDependency.authoredState history authored_valid provenance (base { policy with retirements := [] })) w r
      then history.chg w r else none) = history.chg := by
    funext w r; cases r; simp [changes_restored]
  rw [dropped, restored]
  decide

/-- Reset has an explicit exception for discarded causes (§19.3). It passes
the old write, applies the new one, and records no loss for the old effect. -/
def resetPolicy : Policy Write :=
  { policy with
    retirements := []
    reset := some 9
    readEntry := fun w e => decide (w = .edit ∧ e = 9) }

/-- The post-reset write authors a new generation-zero insert from the empty
reset snapshot, rather than reusing the discarded creation's generation. -/
def resetHistory : Writes Write Unit Unit :=
  { history with chg := fun w r => if w = .edit then history.chg .create r else history.chg w r }

theorem reset_history_valid : Valid resetHistory where
  ts_inj := authored_valid.ts_inj
  past_ts := authored_valid.past_ts
  gen_seen w r ch h := by
    cases w <;> simp only [resetHistory, history, reduceCtorEq, ↓reduceIte, Option.some.injEq] at h
    · subst ch; exact Or.inl rfl
    · subst ch; exact Or.inl rfl
    · subst ch; exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
  parity w r ch h := by
    cases w <;> simp only [resetHistory, history, reduceCtorEq, ↓reduceIte, Option.some.injEq] at h <;>
      subst ch <;> decide

def resetProvenance : DataDependency.Authored resetHistory where
  provenance w _ := if w = .delete then ⟨some .create, [((), .create)]⟩ else ⟨none, []⟩
  read_source w r a h := by
    cases w <;> cases a <;> simp_all [DataDependency.Provenance.sources, resetHistory, history]
  generation_source w r ch h := by
    cases w <;> simp only [resetHistory, history, reduceCtorEq, ↓reduceIte, Option.some.injEq] at h
    · subst ch; exact Or.inl ⟨rfl, rfl⟩
    · subst ch; exact Or.inl ⟨rfl, rfl⟩
    · subst ch; exact Or.inr ⟨.create, _, rfl, rfl, by decide, rfl⟩
  setter_source w r c a h := by
    cases w <;> simp at h
    subst a
    exact ⟨_, _, rfl, rfl, by decide, rfl, rfl⟩

theorem reset_independent_applies :
    (merged resetHistory reset_history_valid resetProvenance resetPolicy St.init
      (pass (pass received .create) .edit)).cell () () = some .edit ∧
    (merged resetHistory reset_history_valid resetProvenance resetPolicy St.init
      (pass (pass received .create) .edit)).lost () () .create = none := by
  have kept (w : Write) (r : Unit) : DataDependency.retained
      (DataDependency.authoredState resetHistory reset_history_valid resetProvenance
        (base resetPolicy)) w r = decide (w = .edit) := by
    rw [DataDependency.retained_eq]
    cases w <;> rfl
  simp only [merged, Data.merged, DataDependency.effective, kept]
  decide

example : consumed noSnapshot (pass (pass received .create) .edit) .create = true ∧
    applied resetPolicy (pass (pass received .create) .edit) = [.edit] ∧
    disposition noSnapshot resetPolicy.reset resetPolicy.readEntry resetPolicy.schema .create =
      .resetIgnored := by decide
def schemaPolicy : Policy Write :=
  { policy with
    retirements := []
    schema := fun _ => .excluded 2 }

example : consumed noSnapshot (pass received .create) .create = true ∧
    applied schemaPolicy (pass received .create) = [] ∧
    disposition noSnapshot none schemaPolicy.readEntry schemaPolicy.schema .create = .schemaLoss 2 := by
  decide

example : consumed noSnapshot (pass (pass received .create) .edit) .edit = true ∧
    applied schemaPolicy (pass (pass received .create) .edit) = [] := by decide

example : withinWindow { policy with
    stored := fun _ => 30 * 86400 } .create = true ∧
    withinWindow policy .create = false := by decide

/-- Complete data alone cannot imply progress: a scheduler may never attempt
the retained write. Fairness excludes this execution. -/
def starved : Run noSnapshot logs past (fun _ _ => true) where
  state | 0 => initial | _ => ⟨[.create], []⟩
  action | 0 => .download .create | _ => .idle
  start := rfl
  next t := by
    cases t
    · apply Step.download <;> simp [initial, noSnapshot, logs] <;> omega
    · exact .idle _

example : (∃ t, .create ∈ (starved.state t).downloaded) ∧
    ready (past .create) (consumed noSnapshot (starved.state 1)) = true ∧
    (∀ t, .create ∉ (starved.state t).passed) := by
  refine ⟨⟨1, by decide⟩, rfl, ?_⟩
  intro t; cases t <;> simp [starved, initial]

end CovenMerge.Receive.Examples
