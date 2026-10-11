import CovenQueue.Model

/-! §3 and §6 receiver atomicity. Complete object/signature and causal checks
precede `begin`; their result is the supplied write. Row updates within its
transaction are separate events, so crashes can interrupt every update. -/
namespace CovenQueue.AtomicReceive

abbrev Rows := Nat → Option Nat

structure Change where
  row : Nat
  value : Option Nat
  deriving DecidableEq, Repr

structure Write where
  number : Nat
  changes : List Change
  deriving DecidableEq, Repr

def applyChange (rows : Rows) (c : Change) : Rows :=
  fun row => if row = c.row then c.value else rows row

def applyAll (rows : Rows) (changes : List Change) : Rows := changes.foldl applyChange rows

structure Database where
  rows : Rows
  position : Nat

structure Transaction where
  rows : Rows
  remaining : List Change

structure State where
  visible : Database
  transaction : Option Transaction

def initial (db : Database) : State := ⟨db, none⟩

def result (before : Database) (w : Write) : Database :=
  ⟨applyAll before.rows w.changes, w.number⟩

inductive Step (w : Write) : State → State → Prop where
  | begin (s : State) : s.transaction = none → s.visible.position < w.number →
      Step w s { s with transaction := some ⟨s.visible.rows, w.changes⟩ }
  | change (s : State) (tx : Transaction) (c : Change) (rest : List Change) :
      s.transaction = some tx → tx.remaining = c :: rest →
      Step w s { s with transaction := some ⟨applyChange tx.rows c, rest⟩ }
  | commit (s : State) (tx : Transaction) : s.transaction = some tx → tx.remaining = [] →
      Step w s ⟨⟨tx.rows, w.number⟩, none⟩
  | crash (s : State) : Step w s { s with transaction := none }
  | wait (s : State) : Step w s s

structure Valid (before : Database) (w : Write) (s : State) : Prop where
  visible : s.visible = before ∨ s.visible = result before w
  transaction : ∀ tx, s.transaction = some tx →
    s.visible = before ∧ applyAll tx.rows tx.remaining = (result before w).rows

theorem step_valid {before : Database} {w : Write} {s t : State}
    (step : Step w s t) (valid : Valid before w s) : Valid before w t := by
  cases step with
  | begin absent newer =>
    refine ⟨valid.visible, ?_⟩
    intro tx eq
    cases eq
    rcases valid.visible with old | applied
    · exact ⟨old, by simp [old, result]⟩
    · simp [applied, result] at newer
  | change tx c rest present remaining =>
    refine ⟨valid.visible, ?_⟩
    intro next eq
    cases eq
    have v := valid.transaction tx present
    exact ⟨v.1, by simpa [remaining, applyAll] using v.2⟩
  | commit tx present empty =>
    have v := valid.transaction tx present
    have rows : tx.rows = (result before w).rows := by simpa [applyAll, empty] using v.2
    refine ⟨Or.inr ?_, ?_⟩
    · cases before; simp_all [result]
    · intro tx eq; cases eq
  | crash => exact ⟨valid.visible, by intro tx eq; cases eq⟩
  | wait => exact valid

inductive Reachable (before : Database) (w : Write) : State → Prop where
  | initial : Reachable before w (initial before)
  | step {s t} : Reachable before w s → Step w s t → Reachable before w t

theorem reachable_valid {before : Database} {w : Write} {s : State}
    (h : Reachable before w s) : Valid before w s := by
  induction h with
  | initial => exact ⟨Or.inl rfl, by intro tx eq; cases eq⟩
  | step _ step ih => exact step_valid step ih

/-- Rows and their consumed position become visible together. There is no
state exposing only some changes, including after a crash during application. -/
theorem never_half_visible {before : Database} {w : Write} {s : State}
    (h : Reachable before w s) : s.visible = before ∨ s.visible = result before w :=
  (reachable_valid h).visible

theorem crash_discards_transaction (s : State) :
    ({ s with transaction := none } : State).visible = s.visible ∧
    ({ s with transaction := none } : State).transaction = none := ⟨rfl, rfl⟩

/-- Once committed, repeated receipt cannot start this write again. -/
theorem committed_cannot_begin (before : Database) (w : Write) :
    ¬ (result before w).position < w.number := by simp [result]

namespace Examples

def before : Database := ⟨fun _ => none, 0⟩
def write : Write := ⟨1, [⟨0, some 10⟩, ⟨1, some 20⟩]⟩
def begun : State := ⟨before, some ⟨before.rows, write.changes⟩⟩
def interrupted : State := ⟨before, some ⟨applyChange before.rows ⟨0, some 10⟩, [⟨1, some 20⟩]⟩⟩
def staged : State := ⟨before, some ⟨(result before write).rows, []⟩⟩

theorem interrupted_reachable : Reachable before write interrupted :=
  .step (.step .initial (.begin _ rfl (by decide)))
    (.change begun ⟨before.rows, write.changes⟩ ⟨0, some 10⟩ [⟨1, some 20⟩] rfl rfl)

theorem crash_mid_write : Reachable before write (initial before) ∧
    interrupted.visible.rows 0 = none ∧ interrupted.visible.rows 1 = none ∧
    interrupted.visible.position = 0 :=
  ⟨.step interrupted_reachable (.crash _), rfl, rfl, rfl⟩

theorem committed_reachable : Reachable before write ⟨result before write, none⟩ := by
  have done : Reachable before write staged := .step interrupted_reachable
    (.change interrupted ⟨applyChange before.rows ⟨0, some 10⟩, [⟨1, some 20⟩]⟩
      ⟨1, some 20⟩ [] rfl rfl)
  exact .step done (.commit _ ⟨(result before write).rows, []⟩ rfl rfl)

example : (result before write).rows 0 = some 10 ∧
    (result before write).rows 1 = some 20 ∧ (result before write).position = 1 := by decide

end Examples
end CovenQueue.AtomicReceive
