/-! ## One snapshot, one boundary entry

`RaiseHistory.publish` assumes a snapshot already named by an entry is named
again only by one of the same kind, and the model keys a boundary's readers
by its named snapshot where Rust keys them by entry. Both follow from how
coven authors raise and reset entries, given §2's members who are not hostile
to their store.

Every preparation reserves the next number of its device's snapshot
sequence (Rust `reserve_snapshot`), so no two preparations share a path, and
a raise preparation marks its prefix while a reset preparation marks none.
A raise or reset operation publishes one entry, at the position its
operation reserved; retries resend that entry. It names a snapshot its own
operation prepared, of the operation's kind. So every snapshot is named by at
most one entry, of the kind its prefix marks (`one_entry_per_snapshot`). -/
namespace CovenIO.BoundaryNames

/-- A prepared snapshot: its device, its number in that device's sequence,
the operation that prepared it, and whether that operation raises. -/
structure Prepared where
  device : Nat
  number : Nat
  operation : Nat
  raise : Bool
  deriving DecidableEq, Repr

/-- A published raise (`raise`) or reset entry naming a snapshot. -/
structure Named where
  entry : Nat
  raise : Bool
  device : Nat
  number : Nat
  deriving DecidableEq, Repr

structure Authoring where
  /-- The greatest number each device reserved. -/
  reserved : Nat → Nat
  prepared : List Prepared
  named : List Named

def Authoring.initial : Authoring := ⟨fun _ => 0, [], []⟩

/-- `entryOf` is the entry position each operation reserved once. -/
inductive Step (entryOf : Nat → Nat) : Authoring → Authoring → Prop
  | prepare (a : Authoring) (device operation : Nat) (raise : Bool) :
      Step entryOf a
        { a with
          reserved := fun d => if d = device then a.reserved d + 1 else a.reserved d
          prepared := ⟨device, a.reserved device + 1, operation, raise⟩ :: a.prepared }
  | publish (a : Authoring) (p : Prepared) (mem : p ∈ a.prepared) :
      Step entryOf a
        { a with named := ⟨entryOf p.operation, p.raise, p.device, p.number⟩ :: a.named }

inductive Run (entryOf : Nat → Nat) : Authoring → Prop
  | initial : Run entryOf .initial
  | step {a b : Authoring} : Run entryOf a → Step entryOf a b → Run entryOf b

structure Valid (entryOf : Nat → Nat) (a : Authoring) : Prop where
  reserved : ∀ p ∈ a.prepared, p.number ≤ a.reserved p.device
  paths : ∀ p ∈ a.prepared, ∀ q ∈ a.prepared, p.device = q.device → p.number = q.number → p = q
  named : ∀ n ∈ a.named, ∃ p ∈ a.prepared, n.device = p.device ∧ n.number = p.number ∧
    n.raise = p.raise ∧ n.entry = entryOf p.operation

theorem step_valid {entryOf : Nat → Nat} {a b : Authoring} (valid : Valid entryOf a)
    (step : Step entryOf a b) : Valid entryOf b := by
  cases step with
  | prepare device operation raise =>
      constructor
      · intro p hp
        rcases List.mem_cons.mp hp with rfl | hp
        · simp
        · have := valid.reserved p hp
          show p.number ≤ (if p.device = device then a.reserved p.device + 1 else a.reserved p.device)
          split <;> omega
      · intro p hp q hq hd hn
        rcases List.mem_cons.mp hp with rfl | hp <;> rcases List.mem_cons.mp hq with rfl | hq
        · rfl
        · have := valid.reserved q hq; simp at hd hn; subst hd; omega
        · have := valid.reserved p hp; simp at hd hn; subst hd; omega
        · exact valid.paths p hp q hq hd hn
      · intro n hn
        obtain ⟨p, hp, h⟩ := valid.named n hn
        exact ⟨p, List.mem_cons_of_mem _ hp, h⟩
  | publish p mem =>
      refine ⟨valid.reserved, valid.paths, ?_⟩
      intro n hn
      rcases List.mem_cons.mp hn with rfl | hn
      · exact ⟨p, mem, rfl, rfl, rfl, rfl⟩
      · exact valid.named n hn

theorem run_valid {entryOf : Nat → Nat} {a : Authoring} (run : Run entryOf a) :
    Valid entryOf a := by
  induction run with
  | initial => exact ⟨by simp [Authoring.initial], by simp [Authoring.initial],
      by simp [Authoring.initial]⟩
  | step _ step ih => exact step_valid ih step

/-- **Every snapshot is named by at most one entry, of one kind**: two
published entries naming one snapshot are the same entry, both a raise or
both a reset, and that kind is what the snapshot's prefix marks. -/
theorem one_entry_per_snapshot {entryOf : Nat → Nat} {a : Authoring} (run : Run entryOf a)
    (n m : Named) (hn : n ∈ a.named) (hm : m ∈ a.named)
    (device : n.device = m.device) (number : n.number = m.number) :
    n.entry = m.entry ∧ n.raise = m.raise ∧
      ∃ p ∈ a.prepared, p.device = n.device ∧ p.number = n.number ∧ p.raise = n.raise := by
  have valid := run_valid run
  obtain ⟨p, hp, pd, pn, pr, pe⟩ := valid.named n hn
  obtain ⟨q, hq, qd, qn, qr, qe⟩ := valid.named m hm
  have same := valid.paths p hp q hq (by rw [← pd, ← qd, device]) (by rw [← pn, ← qn, number])
  subst same
  exact ⟨by rw [pe, qe], by rw [pr, qr], p, hp, pd.symm, pn.symm, pr.symm⟩

end CovenIO.BoundaryNames
