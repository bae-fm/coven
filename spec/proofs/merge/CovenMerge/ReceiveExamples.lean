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
  snapshotWithinWindow w h := by simp [noSnapshot] at h

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

/-- Two writes suffice to lose the generation witness. The creation lands
while retirement is dropped, one second after its deadline. An active device
reads it and edits. Once replay keeps retirement again, §10 excludes the
creation but specifies no inherited exclusion for the edit. Store-log replay
supplies the changed verdict; this example checks its data consequences. -/
theorem retirement_removes_generation_witness :
    withinWindow policy .create = false ∧ withinWindow policy .edit = true ∧
    ¬ SnapshotGenerations history noSnapshot (eligible policy) := by
  refine ⟨by decide, by decide, ?_⟩
  intro h
  have witness := h .edit () ⟨.upd, 1, fun _ => true⟩ (by decide) rfl
  rcases witness with impossible | ⟨w, ch, _, kept, change, kind, gen⟩
  · cases impossible
  · cases w <;> simp [history] at change <;> subst ch
    · contradiction
    · exact kind rfl
    · simp at gen

example : ¬ SnapshotGenerations history noSnapshot (eligible policy) :=
  retirement_removes_generation_witness.2.2

/-- Literal causality cannot hold after discarding this cause. The effective
past can be filtered, but that does not repair the missing row generation. -/
example : history.past .edit .create = true ∧
    applied policy (pass (pass received .create) .edit) = [.edit] ∧
    (effective history policy).past .edit .create = false := by decide

def creationAndEdit : State Write := pass (pass ⟨[.create, .edit], []⟩ .create) .edit

/-- With only the creation and edit consumed, the admitted edit has no
displayed value, loss record, or replacement that read it. This fails value
accounting under every choice of the app's removal rules. -/
theorem retirement_value_unaccounted (I : EntryInputs Unit Unit) :
    Setter (effective history policy) (inputs policy creationAndEdit) () () .edit 1 ∧
    ¬ ValueAccounted (effective history policy) (inputs policy creationAndEdit)
      (merged history policy St.init creationAndEdit) I () () .edit 1 := by
  have cell : (merged history policy St.init creationAndEdit).cell () () = none := by decide
  have lost : (merged history policy St.init creationAndEdit).lost () () .edit = none := by decide
  constructor
  · exact ⟨Or.inr (by decide), ⟨⟨.upd, 1, fun _ => true⟩, rfl, by decide, rfl, rfl⟩⟩
  · intro accounted
    rcases accounted with shown | ⟨w, _, read⟩ | recorded | hidden
    · simp [cell] at shown
    · have unread : (effective history policy).past w .edit = false := by cases w <;> decide
      simp [unread] at read
    · simp [lost] at recorded
    · simp [cell] at hidden

example (I : EntryInputs Unit Unit) :
    ¬ ValueAccounted (effective history policy) (inputs policy creationAndEdit)
      (merged history policy St.init creationAndEdit) I () () .edit 1 :=
  (retirement_value_unaccounted I).2

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

/-- Three writes expose actual merge disagreement: creation, then a concurrent
edit and delete on two active devices. Both orders consume all causes. -/
theorem retirement_loss_disagrees :
    editThenDelete.passed = [.create, .edit, .delete] ∧
    deleteThenEdit.passed = [.create, .delete, .edit] ∧
    (merged history policy St.init editThenDelete).lost () () .edit = none ∧
    (merged history policy St.init deleteThenEdit).lost () () .edit = some (1, .delete) := by
  decide

example : (merged history policy St.init editThenDelete).lost () () .edit ≠
    (merged history policy St.init deleteThenEdit).lost () () .edit := by decide

theorem receive_runs_disagree :
    (∀ w, w ∈ ((arrivalRun true).state 6).passed ↔ w ∈ ((arrivalRun false).state 6).passed) ∧
    (merged history policy St.init ((arrivalRun true).state 6)).lost () () .edit ≠
      (merged history policy St.init ((arrivalRun false).state 6)).lost () () .edit := by
  constructor
  · intro w; cases w <;> decide
  · decide

/-- Waiting for applied effects instead of consumed positions holds the edit
forever: even after both objects arrive its only cause cannot be applied. -/
example : ready (past .edit) (fun w => decide (w ∈ applied policy (pass received .create))) = false := by
  decide

/-- Reset has an explicit exception for discarded causes (§19.3). It passes
the old write, applies the new one, and records no loss for the old effect. -/
def resetPolicy : Policy Write :=
  { policy with
    retirements := []
    reset := some 9
    readEntry := fun w e => decide (w = .edit ∧ e = 9)
    snapshotWithinWindow := fun w h => by simp [noSnapshot, policy] at h }

/-- The post-reset write authors a new generation-zero insert from the empty
reset snapshot, rather than reusing the discarded creation's generation. -/
def resetHistory : Writes Write Unit Unit :=
  { history with chg := fun w r => if w = .edit then history.chg .create r else history.chg w r }

example : consumed noSnapshot (pass (pass received .create) .edit) .create = true ∧
    applied resetPolicy (pass (pass received .create) .edit) = [.edit] ∧
    disposition noSnapshot resetPolicy.reset resetPolicy.readEntry resetPolicy.schema .create =
      .resetIgnored := by decide

example : (merged resetHistory resetPolicy St.init (pass (pass received .create) .edit)).cell () () =
    some .edit ∧
    (merged resetHistory resetPolicy St.init (pass (pass received .create) .edit)).lost () () .create =
      none := by decide

def schemaPolicy : Policy Write :=
  { policy with
    retirements := []
    schema := fun _ => .excluded 2
    snapshotWithinWindow := fun w h => by simp [noSnapshot, policy] at h }

example : consumed noSnapshot (pass received .create) .create = true ∧
    applied schemaPolicy (pass received .create) = [] ∧
    disposition noSnapshot none schemaPolicy.readEntry schemaPolicy.schema .create = .schemaLoss 2 := by
  decide

example : consumed noSnapshot (pass (pass received .create) .edit) .edit = true ∧
    applied schemaPolicy (pass (pass received .create) .edit) = [] := by decide

example : withinWindow { policy with
    stored := fun _ => 30 * 86400
    snapshotWithinWindow := fun w h => by simp [noSnapshot, policy] at h } .create = true ∧
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
