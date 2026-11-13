/-!
# Admitting a joining person in one slot

Ana's approval of Dan's request (§13.2) gives Dan a copy of every store key,
makes him a member, registers his phone and makes his circle, which he is the
only member of and whose key his phone made and sealed to itself. This module
compares two ways of putting that in history.

Split, as the sequence once did it: each copy in a slot of its own, then an
entry adding Dan, then Dan's phone registering itself after it loads. Other
slots can land between any two of them. Merged: one admission slot carries
Dan's copies, admits him, registers his phone and makes his circle with his
copy of its key. The split shape's circle is Dan's own later entry.

A slot here is what landed. A device claims a slot only after applying every
slot before it, and lands only at the slot after what it read, so a landed
admission was checked against exactly the state before it (§11.1,
`Operation`). `Good` says that of a merged history.

The properties:

- Merged, no copy ever names someone who isn't a member
  (`admission_never_exposes`), so no device counts a store key exposed while
  an approval is under way (§12.2); and every member has a registered device
  (`member_has_device`), so a later removal stops a device the joining phone
  runs (`removal_stops_admitted_device`); and from its admission's slot every
  admitted member is in their circle and holds its key (`admitted_has_circle`).
- Split, a copy can stand in history for good naming someone never admitted:
  the approver is demoted between the copies and the entry
  (`split_approval_exposes`); and a member removed between the entry and his
  phone's registration has no device in any extension, so his phone's join can
  never register nor be ended by a stop (`removal_strands_admitted_join`); and
  a circle that is the joiner's own later entry leaves the member without one,
  for good if that entry is refused (`split_circle_leaves_member_without`).
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
  /-- An admission: copies to `m`, `m` added, device `d` registered and
  circle `c` made with `m` its only member, who holds its key. -/
  | admit (m d c : Nat)
  /-- An admission without a circle: copies to `m`, `m` added and device `d`
  registered. -/
  | admitBare (m d : Nat)
  /-- Member `m`'s own entry making circle `c`. -/
  | makeCircle (m c : Nat)
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
  /-- Circles made, each with its only member. -/
  circles : List (Nat × Nat)
  /-- Members holding the key of a circle, as (member, circle). -/
  held : List (Nat × Nat)
  deriving DecidableEq, Repr

def start : State := ⟨[], [], [], true, [], [], []⟩

def registered (s : State) (d : Nat) : Bool := s.devices.any (·.1 == d)

def usedCircle (s : State) (c : Nat) : Bool := s.circles.any (·.1 == c)

/-- Whether an admission of `m` with device `d` counts in state `s`, leaving
aside its circle. -/
def admitsMember (s : State) (m d : Nat) : Bool :=
  s.approver && !s.members.contains m && !registered s d

/-- Whether an admission of `m` with device `d` and circle `c` counts in state
`s`: the circle's id is unused. -/
def admits (s : State) (m d c : Nat) : Bool := admitsMember s m d && !usedCircle s c

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
  | .admit m d c =>
    let s' := { s with copied := m :: s.copied }
    if admits s m d c then
      { s' with members := m :: s.members, devices := (d, m) :: s.devices,
                circles := (c, m) :: s.circles, held := (m, c) :: s.held }
    else s'
  | .admitBare m d =>
    let s' := { s with copied := m :: s.copied }
    if admitsMember s m d then
      { s' with members := m :: s.members, devices := (d, m) :: s.devices }
    else s'
  | .makeCircle m c =>
    if s.members.contains m && !s.removed.contains m && !usedCircle s c then
      { s with circles := (c, m) :: s.circles, held := (m, c) :: s.held }
    else s
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
    Merged x = true ∧ (∀ m d c, x = .admit m d c → admits s m d c = true) ∧ Good (step s x) rest

structure Inv (s : State) : Prop where
  copied : ∀ m ∈ s.copied, m ∈ s.members
  device : ∀ m ∈ s.members, ∃ d, (d, m) ∈ s.devices
  circle : ∀ m ∈ s.members, ∃ c, (c, m) ∈ s.circles ∧ (m, c) ∈ s.held

theorem inv_start : Inv start := ⟨by simp [start], by simp [start], by simp [start]⟩

theorem inv_step (s : State) (x : Slot) (inv : Inv s) (merged : Merged x = true)
    (checked : ∀ m d c, x = .admit m d c → admits s m d c = true) : Inv (step s x) := by
  obtain ⟨copied, device, circle⟩ := inv
  cases x with
  | admit m d c =>
    have yes := checked m d c rfl
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
    · intro n hn
      simp only [List.mem_cons] at hn
      rcases hn with rfl | hn
      · exact ⟨c, by simp⟩
      · obtain ⟨e, he⟩ := circle n hn
        exact ⟨e, by simp [he]⟩
  | remove m =>
    simp only [step]
    split <;> exact ⟨copied, device, circle⟩
  | demote => exact ⟨copied, device, circle⟩
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

/-- **Merged, every admitted member is in their circle and holds its key.**
From the slot that admits them, no point of a merged history holds a member
without a circle they are in and the key of: the admission made both. -/
theorem admitted_has_circle (slots : List Slot) (good : Good start slots) :
    ∀ m ∈ (run start slots).members,
      ∃ c, (c, m) ∈ (run start slots).circles ∧ (m, c) ∈ (run start slots).held :=
  (good_inv start slots inv_start good).circle

theorem devices_stay : ∀ (s : State) (slots : List Slot) (p : Nat × Nat),
    p ∈ s.devices → p ∈ (run s slots).devices
  | _, [], _, h => h
  | s, x :: rest, p, h => devices_stay (step s x) rest p (by
      cases x <;> simp only [step] <;> (try split) <;> simp [h])

/-- **Merged, a removal after the admission stops the phone it admitted.**
The device the admission registered belongs to the removed member, so the
removal makes it gone for good, and the store the joining phone published
stops as any store does (§13.2, §14.1). -/
theorem removal_stops_admitted_device (before after : List Slot) (m d c : Nat)
    (good : Good start (before ++ [.admit m d c] ++ after))
    (removal : ∃ pre post, after = pre ++ [.remove m] ++ post) :
    (d, m) ∈ (run start (before ++ [.admit m d c] ++ after)).devices ∧
      m ∈ (run start (before ++ [.admit m d c] ++ after)).removed := by
  have split : ∀ (s : State) (xs ys : List Slot), Good s (xs ++ ys) → Good (run s xs) ys := by
    intro s xs
    induction xs generalizing s with
    | nil => intro ys h; exact h
    | cons x rest ih => intro ys h; exact ih (step s x) ys h.2.2
  have mid := split start before ([.admit m d c] ++ after) (by simpa using good)
  have yes : admits (List.foldl step start before) m d c = true := mid.2.1 m d c rfl
  let s₁ := run start (before ++ [.admit m d c])
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
  rw [show before ++ [.admit m d c] ++ (pre ++ [.remove m] ++ post) =
      (before ++ [.admit m d c]) ++ (pre ++ ([.remove m] ++ post)) by simp,
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
      cases x <;> simp only [step] <;> (try split) <;> simp_all [admits, admitsMember])

theorem never_added (n : Nat) : ∀ (s : State) (slots : List Slot), s.approver = false →
    n ∉ s.members → n ∉ (run s slots).members
  | _, [], _, h => h
  | s, x :: rest, a, h => by
    apply never_added n (step s x) rest
    · cases x <;> simp only [step] <;> (try split) <;> simp_all [admits, admitsMember]
    · cases x <;> simp only [step] <;> (try split) <;> simp_all [admits, admitsMember]

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
  have prefixState : run start [.copy 1, .demote] = ⟨[], [], [], false, [1], [], []⟩ := rfl
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
      | admit m e c =>
        simp only [step]
        split
        · rename_i h
          have : m ≠ n := by
            rintro rfl
            simp [admits, admitsMember, member] at h
          simp [absent d, Ne.symm this]
        · exact absent d
      | admitBare m e =>
        simp only [step]
        split
        · rename_i h
          have : m ≠ n := by
            rintro rfl
            simp [admitsMember, member] at h
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
  have prefixState : run start [.copy 1, .add 1, .remove 1] = ⟨[1], [1], [], true, [1], [], []⟩ := rfl
  rw [run, List.foldl_append]
  exact no_device_for 1 _ rest (by rw [← run, prefixState]; simp)
    (by rw [← run, prefixState]; simp) (by rw [← run, prefixState]; simp) d

theorem no_circle_for (n : Nat) : ∀ (s : State) (slots : List Slot), n ∈ s.members →
    n ∈ s.removed → (∀ c, (c, n) ∉ s.circles) → ∀ c, (c, n) ∉ (run s slots).circles
  | _, [], _, _, h => h
  | s, x :: rest, member, removed, absent => by
    apply no_circle_for n (step s x) rest
    · cases x <;> simp only [step] <;> (try split) <;> simp [member]
    · cases x <;> simp only [step] <;> (try split) <;> simp [removed]
    · intro c
      cases x with
      | makeCircle m e =>
        simp only [step]
        split
        · rename_i h
          have : m ≠ n := by
            rintro rfl
            simp [removed] at h
          simp [absent c, Ne.symm this]
        · exact absent c
      | admit m e f =>
        simp only [step]
        split
        · rename_i h
          have : m ≠ n := by
            rintro rfl
            simp [admits, admitsMember, member] at h
          simp [absent c, Ne.symm this]
        · exact absent c
      | _ => simp only [step]; (try split) <;> exact absent c

/-- **Split, a circle that is the joiner's own later entry leaves the member
without one.** Ana's entry admits Dan, with his device and copies, and no
circle: Dan is a member who is in none and holds no key of any. If Ben removes
Dan before Dan's phone makes its circle, that entry is refused (§13.2), and
in every extension Dan stays without a circle: nothing writes his profile's
rows anywhere. -/
theorem split_circle_leaves_member_without (rest : List Slot) :
    (1 ∈ (run start [.admitBare 1 0]).members ∧ ∀ c, (c, 1) ∉ (run start [.admitBare 1 0]).circles) ∧
      ∀ c, (c, 1) ∉ (run start ([.admitBare 1 0, .remove 1, .makeCircle 1 5] ++ rest)).circles := by
  refine ⟨⟨by decide, fun c => by simp [run, step, start, admitsMember, registered]⟩, ?_⟩
  have prefixState : run start [.admitBare 1 0, .remove 1, .makeCircle 1 5] =
      ⟨[1], [1], [(0, 1)], true, [1], [], []⟩ := rfl
  rw [run, List.foldl_append]
  exact no_circle_for 1 _ rest (by rw [← run, prefixState]; simp)
    (by rw [← run, prefixState]; simp) (by rw [← run, prefixState]; simp)

end CovenSequence.Admission
