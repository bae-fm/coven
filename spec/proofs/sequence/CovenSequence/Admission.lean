/-!
# Admitting a joining person in one slot

Ana's approval of Dan's request (§13.2) gives Dan a copy of every store key,
makes him a member and registers his phone. This module compares two ways of
putting that in history.

Split, as the sequence once did it: each copy in a slot of its own, then an
entry adding Dan, then Dan's phone registering itself after it loads. Other
slots can land between any two of them. Merged: one admission slot carries
Dan's copies, admits him and registers his phone.

A slot here is what landed. A device claims a slot only after applying every
slot before it, and lands only at the slot after what it read, so a landed
admission was checked against exactly the state before it (§11.1,
`Operation`). `Good` says that of a merged history.

The properties:

- Merged, no copy ever names someone who isn't a member
  (`admission_never_exposes`), so no device counts a store key exposed while
  an approval is under way (§12.2); and every member has a registered device
  (`member_has_device`), so a later removal stops a device the joining phone
  runs (`removal_stops_admitted_device`).
- Split, a copy can stand in history for good naming someone never admitted:
  the approver is demoted between the copies and the entry
  (`split_approval_exposes`); and a member removed between the entry and his
  phone's registration has no device in any extension, so his phone's join can
  never register nor be ended by a stop (`removal_strands_admitted_join`).
-/
namespace CovenSequence.Admission

/-- A slot, as far as admitting a member goes. Members and devices are
numbers. -/
inductive Slot where
  /-- A key copy in a slot of its own, to member `m`. -/
  | copy (m : Nat)
  /-- An entry adding member `m`, after its copies. -/
  | add (m : Nat)
  /-- Device `d` registering itself for member `m`. -/
  | register (m d : Nat)
  /-- An admission: copies to `m`, `m` added and device `d` registered. -/
  | admit (m d : Nat)
  /-- An admin's removal of member `m`. -/
  | remove (m : Nat)
  /-- The approving admin is demoted: nothing it approves counts again. -/
  | demote
  deriving DecidableEq, Repr

structure State where
  /-- Members ever admitted, removed ones included. -/
  members : List Nat
  removed : List Nat
  /-- Registered devices, each with its member. -/
  devices : List (Nat × Nat)
  /-- The approving admin may still admit. -/
  approver : Bool
  /-- Recipients of the copies in history. -/
  copied : List Nat
  deriving DecidableEq, Repr

def start : State := ⟨[], [], [], true, []⟩

def registered (s : State) (d : Nat) : Bool := s.devices.any (·.1 == d)

/-- Whether an admission of `m` with device `d` counts in state `s`. -/
def admits (s : State) (m d : Nat) : Bool :=
  s.approver && !s.members.contains m && !registered s d

/-- Apply one slot. A copy discloses its key whether or not anything admits
its recipient. -/
def step (s : State) : Slot → State
  | .copy m => { s with copied := m :: s.copied }
  | .add m =>
    if s.approver && !s.members.contains m then { s with members := m :: s.members } else s
  | .register m d =>
    if s.members.contains m && !s.removed.contains m && !registered s d then
      { s with devices := (d, m) :: s.devices }
    else s
  | .admit m d =>
    let s' := { s with copied := m :: s.copied }
    if admits s m d then { s' with members := m :: s.members, devices := (d, m) :: s.devices }
    else s'
  | .remove m => if s.members.contains m then { s with removed := m :: s.removed } else s
  | .demote => { s with approver := false }

def run (s : State) (slots : List Slot) : State := slots.foldl step s

/-- A copy names someone who isn't a member: every device counts its key
exposed (§12.2). -/
def Exposed (s : State) : Prop := ∃ m ∈ s.copied, m ∉ s.members

/-- A merged history: only admissions carry copies, members are added only by
admissions and devices register only through them, and each admission was
checked against the state just before it. -/
def Merged : Slot → Bool
  | .admit .. | .remove _ | .demote => true
  | _ => false

def Good : State → List Slot → Prop
  | _, [] => True
  | s, x :: rest =>
    Merged x = true ∧ (∀ m d, x = .admit m d → admits s m d = true) ∧ Good (step s x) rest

structure Inv (s : State) : Prop where
  copied : ∀ m ∈ s.copied, m ∈ s.members
  device : ∀ m ∈ s.members, ∃ d, (d, m) ∈ s.devices

theorem inv_start : Inv start := ⟨by simp [start], by simp [start]⟩

theorem inv_step (s : State) (x : Slot) (inv : Inv s) (merged : Merged x = true)
    (checked : ∀ m d, x = .admit m d → admits s m d = true) : Inv (step s x) := by
  obtain ⟨copied, device⟩ := inv
  cases x with
  | admit m d =>
    have yes := checked m d rfl
    simp only [step, yes, ite_true]
    constructor
    · intro n hn
      simp only [List.mem_cons] at hn ⊢
      rcases hn with rfl | hn
      · exact .inl rfl
      · exact .inr (copied n hn)
    · intro n hn
      simp only [List.mem_cons] at hn
      rcases hn with rfl | hn
      · exact ⟨d, by simp⟩
      · obtain ⟨e, he⟩ := device n hn
        exact ⟨e, by simp [he]⟩
  | remove m =>
    simp only [step]
    split <;> exact ⟨copied, device⟩
  | demote => exact ⟨copied, device⟩
  | _ => simp [Merged] at merged

theorem good_inv : ∀ (s : State) (slots : List Slot), Inv s → Good s slots → Inv (run s slots)
  | _, [], inv, _ => inv
  | s, x :: rest, inv, ⟨merged, checked, good⟩ =>
    good_inv (step s x) rest (inv_step s x inv merged checked) good

/-- **Merged, no key is ever exposed by an approval.** At every point of a
merged history, every copy's recipient is a member, so no device counts a
store key exposed while an approval is under way. -/
theorem admission_never_exposes (slots : List Slot) (good : Good start slots) :
    ¬Exposed (run start slots) := by
  rintro ⟨m, hm, not⟩
  exact not ((good_inv start slots inv_start good).copied m hm)

/-- **Merged, every member has a registered device.** No point of a merged
history holds a member without one. -/
theorem member_has_device (slots : List Slot) (good : Good start slots) :
    ∀ m ∈ (run start slots).members, ∃ d, (d, m) ∈ (run start slots).devices :=
  (good_inv start slots inv_start good).device

theorem devices_stay : ∀ (s : State) (slots : List Slot) (p : Nat × Nat),
    p ∈ s.devices → p ∈ (run s slots).devices
  | _, [], _, h => h
  | s, x :: rest, p, h => devices_stay (step s x) rest p (by
      cases x <;> simp only [step] <;> (try split) <;> simp [h])

/-- **Merged, a removal after the admission stops the phone it admitted.**
The device the admission registered belongs to the removed member, so the
removal makes it gone for good, and the facts end its join (§13.2, §14.1). -/
theorem removal_stops_admitted_device (before after : List Slot) (m d : Nat)
    (good : Good start (before ++ [.admit m d] ++ after))
    (removal : ∃ pre post, after = pre ++ [.remove m] ++ post) :
    (d, m) ∈ (run start (before ++ [.admit m d] ++ after)).devices ∧
      m ∈ (run start (before ++ [.admit m d] ++ after)).removed := by
  have split : ∀ (s : State) (xs ys : List Slot), Good s (xs ++ ys) → Good (run s xs) ys := by
    intro s xs
    induction xs generalizing s with
    | nil => intro ys h; exact h
    | cons x rest ih => intro ys h; exact ih (step s x) ys h.2.2
  have mid := split start before ([.admit m d] ++ after) (by simpa using good)
  have yes : admits (List.foldl step start before) m d = true := mid.2.1 m d rfl
  let s₁ := run start (before ++ [.admit m d])
  have reg : (d, m) ∈ s₁.devices := by simp [s₁, run, List.foldl_append, step, yes]
  have mem : m ∈ s₁.members := by simp [s₁, run, List.foldl_append, step, yes]
  have members_stay : ∀ (s : State) (xs : List Slot), m ∈ s.members → m ∈ (run s xs).members := by
    intro s xs
    induction xs generalizing s with
    | nil => exact id
    | cons x rest ih =>
      intro h; apply ih
      cases x <;> simp only [step] <;> (try split) <;> simp [h]
  have removed_stay : ∀ (s : State) (xs : List Slot), m ∈ s.removed → m ∈ (run s xs).removed := by
    intro s xs
    induction xs generalizing s with
    | nil => exact id
    | cons x rest ih =>
      intro h; apply ih
      cases x <;> simp only [step] <;> (try split) <;> simp [h]
  obtain ⟨pre, post, rfl⟩ := removal
  have run_append : ∀ (s : State) (xs ys : List Slot), run s (xs ++ ys) = run (run s xs) ys := by
    intro s xs ys; simp [run, List.foldl_append]
  rw [show before ++ [.admit m d] ++ (pre ++ [.remove m] ++ post) =
      (before ++ [.admit m d]) ++ (pre ++ ([.remove m] ++ post)) by simp,
    run_append, run_append, run_append]
  have atPre := members_stay s₁ pre mem
  refine ⟨devices_stay _ _ _ (devices_stay _ _ _ (devices_stay s₁ pre _ reg)), ?_⟩
  exact removed_stay _ post (by
    show m ∈ (step (run s₁ pre) (.remove m)).removed
    have c : (run s₁ pre).members.contains m = true := by simpa using atPre
    simp only [step, c, ite_true, List.mem_cons, true_or])

/-! ## The split shape -/

theorem approver_stays_false : ∀ (s : State) (slots : List Slot),
    s.approver = false → (run s slots).approver = false
  | _, [], h => h
  | s, x :: rest, h => approver_stays_false (step s x) rest (by
      cases x <;> simp only [step] <;> (try split) <;> simp_all [admits])

theorem never_added (n : Nat) : ∀ (s : State) (slots : List Slot), s.approver = false →
    n ∉ s.members → n ∉ (run s slots).members
  | _, [], _, h => h
  | s, x :: rest, a, h => by
    apply never_added n (step s x) rest
    · cases x <;> simp only [step] <;> (try split) <;> simp_all [admits]
    · cases x <;> simp only [step] <;> (try split) <;> simp_all [admits]

theorem copied_stays (n : Nat) : ∀ (s : State) (slots : List Slot),
    n ∈ s.copied → n ∈ (run s slots).copied
  | _, [], h => h
  | s, x :: rest, h => copied_stays n (step s x) rest (by
      cases x <;> simp only [step] <;> (try split) <;> simp [h])

/-- **Split, a refused approval exposes the store key for good.** Ana's phone
claims Dan's copy, then a slot demoting her lands before her entry, which the
caught-up history refuses. The copy stays, naming someone who never becomes a
member, whatever lands after: every device counts the key exposed. -/
theorem split_approval_exposes (rest : List Slot) :
    Exposed (run start ([.copy 1, .demote] ++ rest)) := by
  have prefixState : run start [.copy 1, .demote] = ⟨[], [], [], false, [1]⟩ := rfl
  refine ⟨1, ?_, ?_⟩
  · rw [run, List.foldl_append]
    exact copied_stays 1 _ rest (by rw [← run, prefixState]; simp)
  · rw [run, List.foldl_append]
    exact never_added 1 _ rest (by rw [← run, prefixState]) (by rw [← run, prefixState]; simp)

theorem no_device_for (n : Nat) : ∀ (s : State) (slots : List Slot), n ∈ s.members →
    n ∈ s.removed → (∀ d, (d, n) ∉ s.devices) → ∀ d, (d, n) ∉ (run s slots).devices
  | _, [], _, _, h => h
  | s, x :: rest, member, removed, absent => by
    apply no_device_for n (step s x) rest
    · cases x <;> simp only [step] <;> (try split) <;> simp [member]
    · cases x <;> simp only [step] <;> (try split) <;> simp [removed]
    · intro d
      cases x with
      | register m e =>
        simp only [step]
        split
        · rename_i h
          have : m ≠ n := by
            rintro rfl
            simp at h
            exact h.1.2 removed
          simp [absent d, Ne.symm this]
        · exact absent d
      | admit m e =>
        simp only [step]
        split
        · rename_i h
          have : m ≠ n := by
            rintro rfl
            simp [admits] at h
            exact h.1.2 member
          simp [absent d, Ne.symm this]
        · exact absent d
      | _ => simp only [step]; (try split) <;> exact absent d

/-- **Split, a removal before registration strands the join.** Ana's copies
and her entry adding Dan land; Ben removes Dan before Dan's phone registers.
In every extension Dan has no device: his phone's registration is refused,
so no stop for good ever names a device of his, and his join, whose request
was sent, can neither finish nor be ended by the facts (§13.2). -/
theorem removal_strands_admitted_join (rest : List Slot) (d : Nat) :
    (d, 1) ∉ (run start ([.copy 1, .add 1, .remove 1] ++ rest)).devices := by
  have prefixState : run start [.copy 1, .add 1, .remove 1] = ⟨[1], [1], [], true, [1]⟩ := rfl
  rw [run, List.foldl_append]
  exact no_device_for 1 _ rest (by rw [← run, prefixState]; simp)
    (by rw [← run, prefixState]; simp) (by rw [← run, prefixState]; simp) d

end CovenSequence.Admission
