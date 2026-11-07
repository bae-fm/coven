/-! Applying a slot beside unsent writes folds the queue over the slots' state
again (§6.2). Only the queued writes the slot reaches need folding again.

A state holds a value for each row. An unsent write names the rows it
changes and changes no other. What it leaves in each of its rows depends
only on what its own rows held: the device judges a write as a whole, from
its own records and the owners its rows hold, never from another row's state
(`judge.rs`). A slot of writes changes only the rows it stages; one that
changes membership, cut-offs or boundaries changes how every queued write
applies, and is outside this model.

The rows a slot reaches are those it stages, closed over the queue: a queued
write that changes one of them adds all of its rows. Outside that closure the
queue's fold is the one the state already holds (`untouched_rows_keep_their_fold`).
Inside it, folding only the writes that reach it gives what folding the whole
queue would (`reached_rows_refold_alone`). Together the device puts back and
folds again only the reached rows (`refold_reached`). The staged rows alone
are too few: a write that changes a staged row and another takes the other
row's value from the staged one (`staged_rows_alone_too_small`). -/
namespace CovenSequence.QueueFold

/-- One unsent write: the rows it changes, and what it leaves in each row of
a state. -/
structure Write (Row Val : Type) where
  rows : List Row
  apply : (Row → Val) → Row → Val
  /-- It leaves every other row as it was. -/
  outside : ∀ s r, r ∉ rows → apply s r = s r
  /-- What it leaves in its rows depends only on what they held. -/
  own : ∀ s t, (∀ x ∈ rows, s x = t x) → ∀ r ∈ rows, apply s r = apply t r

variable {Row Val : Type}

/-- The queue folded over `s`, in queue order. -/
def fold (queue : List (Write Row Val)) (s : Row → Val) : Row → Val :=
  queue.foldl (fun s w => w.apply s) s

/-- Whether `w` changes a row of `C`. -/
def reaches (C : Row → Bool) (w : Write Row Val) : Bool :=
  w.rows.any C

/-- `C` is closed over the queue: a queued write that changes one of its rows
changes only rows of `C`. -/
def Closed (queue : List (Write Row Val)) (C : Row → Bool) : Prop :=
  ∀ w ∈ queue, reaches C w = true → ∀ r ∈ w.rows, C r = true

theorem closed_tail {w : Write Row Val} {rest : List (Write Row Val)} {C : Row → Bool}
    (closed : Closed (w :: rest) C) : Closed rest C :=
  fun w' member => closed w' (List.mem_cons_of_mem w member)

/-- Two states that agree outside a closed `C` fold the queue to states that
agree there too: a write that changes a row outside `C` changes only rows
outside it, and reads only them. -/
theorem untouched_rows_keep_their_fold :
    ∀ (queue : List (Write Row Val)) (C : Row → Bool) (s t : Row → Val),
      Closed queue C → (∀ r, C r = false → t r = s r) →
      ∀ r, C r = false → fold queue t r = fold queue s r := by
  intro queue
  induction queue with
  | nil => intro C s t _ agree r outside; exact agree r outside
  | cons w rest ih =>
    intro C s t closed agree r outsideC
    show fold rest (w.apply t) r = fold rest (w.apply s) r
    refine ih C (w.apply s) (w.apply t) (closed_tail closed) ?_ r outsideC
    intro x outsideX
    by_cases member : x ∈ w.rows
    · have none : ∀ y ∈ w.rows, C y = false := by
        intro y memberY
        cases inC : C y with
        | false => rfl
        | true =>
          have reached : reaches C w = true :=
            List.any_eq_true.mpr ⟨y, memberY, inC⟩
          have := closed w (by simp) reached x member
          simp [outsideX] at this
      exact w.own t s (fun y memberY => agree y (none y memberY)) x member
    · rw [w.outside t x member, w.outside s x member]
      exact agree x outsideX

/-- Folding only the writes that reach a closed `C`, over a state that agrees
with `s` on `C`, gives on `C` what folding the whole queue over `s` gives. -/
theorem reached_rows_refold_alone :
    ∀ (queue : List (Write Row Val)) (C : Row → Bool) (s t : Row → Val),
      Closed queue C → (∀ r, C r = true → t r = s r) →
      ∀ r, C r = true → fold (queue.filter (reaches C)) t r = fold queue s r := by
  intro queue
  induction queue with
  | nil => intro C s t _ agree r inside; exact agree r inside
  | cons w rest ih =>
    intro C s t closed agree r insideC
    cases reached : reaches C w with
    | true =>
      simp only [List.filter_cons, reached, ↓reduceIte]
      show fold (rest.filter (reaches C)) (w.apply t) r = fold rest (w.apply s) r
      refine ih C (w.apply s) (w.apply t) (closed_tail closed) ?_ r insideC
      intro x insideX
      by_cases member : x ∈ w.rows
      · have all := closed w (by simp) reached
        exact w.own t s (fun y memberY => agree y (all y memberY)) x member
      · rw [w.outside t x member, w.outside s x member]
        exact agree x insideX
    | false =>
      simp only [List.filter_cons, reached, Bool.false_eq_true, ↓reduceIte]
      show fold (rest.filter (reaches C)) t r = fold rest (w.apply s) r
      refine ih C (w.apply s) t (closed_tail closed) ?_ r insideC
      intro x insideX
      have member : x ∉ w.rows := by
        intro member
        have : reaches C w = true := List.any_eq_true.mpr ⟨x, member, insideX⟩
        simp [reached] at this
      rw [w.outside s x member]
      exact agree x insideX

/-- Applying a slot beside the queue. `counted` is the slots' state before
the slot, `slotted` after it, differing only on rows of `C`, which holds the
rows the slot stages closed over the queue. The device keeps every row outside
`C` as the queue's fold over `counted` left it, puts back the slots' state on
`C`, and folds there only the queued writes that reach `C`, starting from any
state `t` that holds the slots' state on `C`. That is the whole queue folded
over `slotted`. -/
theorem refold_reached (queue : List (Write Row Val)) (C : Row → Bool)
    (closed : Closed queue C) (counted slotted t : Row → Val)
    (staged : ∀ r, C r = false → slotted r = counted r)
    (restored : ∀ r, C r = true → t r = slotted r) :
    (fun r => if C r then fold (queue.filter (reaches C)) t r else fold queue counted r) =
      fold queue slotted := by
  funext r
  cases inC : C r with
  | false =>
    simp only [Bool.false_eq_true, ↓reduceIte]
    exact (untouched_rows_keep_their_fold queue C counted slotted closed staged r inC).symm
  | true =>
    simp only [↓reduceIte]
    exact reached_rows_refold_alone queue C slotted t closed restored r inC

/-- A write that changes both rows and leaves the staged row's value in each. -/
def copyStaged : Write Bool Nat where
  rows := [true, false]
  apply s _ := s true
  outside s r notMember := by cases r <;> simp at notMember
  own _ _ agree _ _ := agree true (by simp)

/-- The rows a slot stages alone are too few to fold again. The slot stages
row `true`, and the queued write changes both rows: row `false` takes the
staged row's new value, though the slot never staged it. -/
theorem staged_rows_alone_too_small :
    let counted : Bool → Nat := fun _ => 0
    let slotted : Bool → Nat := fun r => if r then 1 else 0
    (∀ r, r = false → slotted r = counted r) ∧
      fold [copyStaged] slotted false ≠ fold [copyStaged] counted false := by
  refine ⟨?_, ?_⟩
  · intro r isFalse
    simp [isFalse]
  · simp [fold, copyStaged]

end CovenSequence.QueueFold
