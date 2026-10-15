import CovenIO.FixedReload

namespace CovenIO.AtomicReload

abbrev View := Nat → Nat → Nat

/-- Every readable audience realizes the past claimed by a waiting write. -/
def WaitingReady (audiences writers : List Nat) (view : View) (past : Nat → Nat) : Prop :=
  ∀ a ∈ audiences, ∀ writer ∈ writers, past writer ≤ view a writer

/-- Readers and app writes see only the committed view during staging. -/
structure State where
  visible : View
  staged : Option View

def prepare (s : State) (next : View) : State := { s with staged := some next }

/-- One transition publishes the whole staged view and clears pending work. -/
def commit (s : State) : Option State :=
  match s.staged with
  | none => none
  | some next => some ⟨next, none⟩

theorem staging_keeps_visible (s : State) (next : View) :
    (prepare s next).visible = s.visible := rfl

theorem atomic_waiting_ready (audiences writers : List Nat) (old staged : View)
    (target past : Nat → Nat)
    (realized : ∀ a ∈ audiences, ∀ writer ∈ writers, target writer ≤ staged a writer)
    (frozenPast : ∀ writer ∈ writers, past writer ≤ target writer) :
    commit ⟨old, some staged⟩ = some ⟨staged, none⟩ ∧
      WaitingReady audiences writers staged past := by
  refine ⟨rfl, ?_⟩
  intro a ha writer hw
  exact Nat.le_trans (frozenPast writer hw) (realized a ha writer hw)

/-- A write authored during staging reads the committed view, so its past is
at most this device's applied positions. A frozen requirement that includes
them bounds every such past, which `CommonReload.committed_safe` assumes. -/
theorem staging_past_bounded (writers : List Nat) (applied required past : Nat → Nat)
    (keeps : ∀ w ∈ writers, applied w ≤ required w)
    (authored : ∀ w ∈ writers, past w ≤ applied w) :
    ∀ w ∈ writers, past w ≤ required w :=
  fun w hw => Nat.le_trans (authored w hw) (keeps w hw)

/-- Without them: applied through 2, no own write, a snapshot through 1. The
target is 1, and a write made during staging read 2. -/
example : ¬ WaitingReady [0] [0] (fun _ _ => max 0 1) (fun _ => 2) := by simp [WaitingReady]

/-- One two-audience write and one waiting write suffice: exposing one part
first lets a waiting write claim a past not realized by the other audience. -/
example : ¬ WaitingReady [0, 1] [0] (fun a _ => if a = 0 then 1 else 0) (fun _ => 1) := by simp [WaitingReady]

/-- Reaching a frozen lower bound is not the spec's common-point condition.
A replacement snapshot may already contain the next two-audience write. -/
def CommonPoint (audiences writers : List Nat) (view : View) : Prop :=
  ∀ a ∈ audiences, ∀ b ∈ audiences, ∀ writer ∈ writers, view a writer = view b writer

/-- A replacement may exceed the required lower bound only as a complete
common cut. Such a bundle needs no further tail scan before the atomic commit. -/
theorem closed_replacement_commits (audiences writers : List Nat) (old staged : View)
    (required cut past : Nat → Nat)
    (bundle : ∀ a ∈ audiences, ∀ writer ∈ writers, staged a writer = cut writer)
    (reaches : ∀ writer ∈ writers, required writer ≤ cut writer)
    (waiting : ∀ writer ∈ writers, past writer ≤ required writer) :
    commit ⟨old, some staged⟩ = some ⟨staged, none⟩ ∧
      CommonPoint audiences writers staged ∧ WaitingReady audiences writers staged past := by
  refine ⟨rfl, ?_, ?_⟩
  · intro a ha b hb writer hw
    exact (bundle a ha writer hw).trans (bundle b hb writer hw).symm
  · exact (atomic_waiting_ready audiences writers old staged required past
      (by
        intro a ha writer hw
        rw [bundle a ha writer hw]
        exact reaches writer hw)
      waiting).2

example : WaitingReady [0, 1] [0] (fun a _ => if a = 0 then 2 else 1) (fun _ => 1) ∧
    ¬ CommonPoint [0, 1] [0] (fun a _ => if a = 0 then 2 else 1) := by
  constructor
  · simp [WaitingReady]
  · intro aligned
    have := aligned 0 (by simp) 1 (by simp) 0 (by simp)
    simp at this

/-- A local write authored on that exposed state can now require number 2.
Atomic visibility alone cannot repair an unaligned staged state. -/
example (old : View) :
    commit ⟨old, some (fun a _ => if a = 0 then 2 else 1)⟩ =
      some ⟨(fun a _ => if a = 0 then 2 else 1), none⟩ ∧
    ¬ WaitingReady [0, 1] [0] (fun a _ => if a = 0 then 2 else 1) (fun _ => 2) := by
  exact ⟨rfl, by simp [WaitingReady]⟩

end CovenIO.AtomicReload
