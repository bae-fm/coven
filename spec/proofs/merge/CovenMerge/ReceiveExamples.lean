import CovenMerge.ReceiveMerge

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

def policy : Policy Write where
  boundary := noSnapshot
  reset := none
  readEntry _ _ := false
  schema _ := .eligible
  admitted _ := true

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

/-- The input model permits download order independently of causality. -/
example : Step noSnapshot past (fun _ => true) (.download .create)
    ⟨[.edit, .delete], []⟩ received := by
  apply Step.download <;> simp [noSnapshot]

example : (pass (pass received .create) .edit).passed = [.create, .edit] := by decide

/-- Exact data used by the original edit and delete, independently of the
inputs they consumed. Both replaced the creation's cell at generation one. -/
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

def editThenDelete : State Write := pass (pass (pass received .create) .edit) .delete
def deleteThenEdit : State Write := pass (pass (pass received .create) .delete) .edit

/-- Both histories download every input, then make three causally ready
attempts and become idle. -/
def arrivalRun (editFirst : Bool) : Run noSnapshot past (fun _ _ => true) where
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
    | 0 => apply Step.download <;> simp [initial, noSnapshot]
    | 1 => apply Step.download <;> simp [noSnapshot]
    | 2 => apply Step.download <;> simp [noSnapshot]
    | 3 => exact .attempt _ _
    | 4 => exact .attempt _ _
    | 5 => exact .attempt _ _
    | _ + 6 => exact .idle _

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

/-- Reset has an explicit exception for discarded causes (§19.3). It passes
the old write, applies the new one, and records no loss for the old effect. -/
def resetPolicy : Policy Write :=
  { policy with
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
    schema := fun _ => .excluded 2 }

example : consumed noSnapshot (pass received .create) .create = true ∧
    applied schemaPolicy (pass received .create) = [] ∧
    disposition noSnapshot none schemaPolicy.readEntry schemaPolicy.schema .create = .schemaLoss 2 := by
  decide

example : consumed noSnapshot (pass (pass received .create) .edit) .edit = true ∧
    applied schemaPolicy (pass (pass received .create) .edit) = [] := by decide

/-- Complete data alone cannot imply progress: a scheduler may never attempt
the retained write. Fairness excludes this execution. -/
def starved : Run noSnapshot past (fun _ _ => true) where
  state | 0 => initial | _ => ⟨[.create], []⟩
  action | 0 => .download .create | _ => .idle
  start := rfl
  next t := by
    cases t
    · apply Step.download <;> simp [initial, noSnapshot]
    · exact .idle _

example : (∃ t, .create ∈ (starved.state t).downloaded) ∧
    ready (past .create) (consumed noSnapshot (starved.state 1)) = true ∧
    (∀ t, .create ∉ (starved.state t).passed) := by
  refine ⟨⟨1, by decide⟩, rfl, ?_⟩
  intro t; cases t <;> simp [starved, initial]

end CovenMerge.Receive.Examples
