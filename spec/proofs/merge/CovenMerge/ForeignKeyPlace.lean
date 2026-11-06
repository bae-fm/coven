/-! Where a write's steps leave a parent key (§9.6, §15.4, D5).

A step records the parent each written reference names at that moment: the
row its values key, in the author's view as the write's earlier operations
left it. A receiver rebuilds that row from the steps alone, never from its own
rows.

The author's view holds one parent key in at most one audience at a time
(`View`). Each operation of the write's SQL on the key is a step on its row in
the audience it is in then (`emit`). A move is the delete of the old
audience's row and the insert of the new one's, at one position: an update of
the row's own audience, and the move of an ancestor that carries the row
along, record it alike (`Op.move`). Only a change of no step at all
(`Op.unstepped`), as a migration difference or a forged write has, leaves the
steps silent.

* `place` is the receiver's rule as `StepIndex::place` runs it: with no
  unstepped change in the write, the latest earlier step on any row with the
  key, a move's insert half after its delete half; no step at all leaves the
  key as the authoring view holds it; an unstepped change leaves it unplaced.
* `place_sound`: when `place` names a step, every authoring view the steps
  are consistent with holds the key where that step left it, at its
  incarnation, or nowhere after a delete; when no step touched the key, it is
  where it was before the write.
* `recorded_sound` lifts that to the parent a recorded reference names
  (`named`): present in the child's audience or the store, as the author's
  capture names it (`capture`), given that a reference left as it was names
  the parent its setter recorded while that parent is the authoring view's
  row (§9.1's display rule).
* `net_sound` does the same for a row's net change, which names the parent
  where the steps leave the key at the end of the write, at the incarnation
  the write's net changes leave that row at (`namedNet`).
  `readded_net_differs` and `deferred_net_differs` are why that is not the
  parent an action's step names.
* `untouched_views_differ` and `unstepped_views_differ` are why the receiver
  refuses the rest: two authoring views with the same steps capture different
  parents. -/
namespace CovenMerge.Ownership.Classification.Place

variable {A : Type} [DecidableEq A]

/-- Where one parent key is in the author's view: nowhere, or in one audience
at one incarnation. -/
abbrev View (A : Type) := Option (A × Nat)

/-- One operation of the write, at its own position. An insert or move names
the incarnation it begins. A move is any operation that takes the key's row
to another audience: an update of its own audience, or a move of an ancestor
that carries it (§15.4). -/
inductive Op (A : Type) where
  | insert (a : A) (g : Nat)
  | update
  | delete
  | move (b : A) (g : Nat)
  /-- A change of the key's row the write made at no step of its own. -/
  | unstepped (b : A) (g : Nat)
  /-- An operation on another row. -/
  | other
deriving DecidableEq, Repr

/-- A step on the key's row in audience `aud`: its position, whether it is a
delete and a move's half, and the incarnation it ends or leaves. -/
structure Step (A : Type) where
  seq : Nat
  aud : A
  delete : Bool
  move : Bool
  inc : Nat
deriving DecidableEq, Repr

/-- The view an operation leaves, or none when it can't run in this one. -/
def apply : Op A → View A → Option (View A)
  | .insert a g, none => some (some (a, g))
  | .update, some p => some (some p)
  | .delete, some _ => some none
  | .move b g, some (a, _) => if a = b then none else some (some (b, g))
  | .unstepped b g, some (a, _) => if a = b then none else some (some (b, g))
  | .other, v => some v
  | _, _ => none

/-- The steps an operation at position `i` records on the key's rows. -/
def emit (i : Nat) : Op A → View A → List (Step A)
  | .insert a g, _ => [⟨i, a, false, false, g⟩]
  | .update, some (a, g) => [⟨i, a, false, false, g⟩]
  | .delete, some (a, g) => [⟨i, a, true, false, g⟩]
  | .move b g', some (a, g) => [⟨i, a, true, true, g⟩, ⟨i, b, false, true, g'⟩]
  | _, _ => []

/-- A run of operations from position `i` and view `v`: the view it leaves and
its steps, or none when an operation can't run. -/
def run : List (Op A) → Nat → View A → Option (View A × List (Step A))
  | [], _, v => some (v, [])
  | op :: ops, i, v => do
      let w ← apply op v
      let r ← run ops (i + 1) w
      pure (r.1, emit i op v ++ r.2)

def Op.isUnstepped : Op A → Bool
  | .unstepped _ _ => true
  | _ => false

/-- The view a step leaves. -/
def Step.view (st : Step A) : View A := if st.delete then none else some (st.aud, st.inc)

/-- A step's order among the write's: by position, a move's delete half before
its insert half. -/
def Step.key (st : Step A) : Nat := 2 * st.seq + if st.delete && st.move then 0 else 1

/-- The later of two steps. -/
def later (x y : Step A) : Step A := if x.key ≤ y.key then y else x

def latest : List (Step A) → Option (Step A)
  | [] => none
  | x :: xs => some (xs.foldl later x)

inductive Place (A : Type) where
  | untouched
  | unplaced
  | last (st : Step A)
deriving DecidableEq, Repr

/-- The receiver's rule: where the key is before position `s`, from the write's
steps and whether the write changed the key at no step of its own. -/
def place (unstepped : Bool) (steps : List (Step A)) (s : Nat) : Place A :=
  if unstepped then .unplaced else
    match latest (steps.filter (·.seq < s)) with
    | none => .untouched
    | some st => .last st

/-! The emitted steps are in order. -/

theorem foldl_later_last (x : Step A) (xs : List (Step A))
    (h : List.Pairwise (fun a b => a.key < b.key) (x :: xs)) :
    xs.foldl later x = (x :: xs).getLast (by simp) := by
  induction xs generalizing x with
  | nil => rfl
  | cons y ys ih =>
    have hxy : x.key < y.key := List.rel_of_pairwise_cons h (by simp)
    have : later x y = y := by simp [later, Nat.le_of_lt hxy]
    simp only [List.foldl_cons, this]
    rw [ih y (List.Pairwise.of_cons h)]
    simp

theorem latest_getLast (l : List (Step A)) (h : List.Pairwise (fun a b => a.key < b.key) l) :
    latest l = l.getLast? := by
  cases l with
  | nil => rfl
  | cons x xs =>
    simp only [latest, foldl_later_last x xs h]
    exact (List.getLast?_eq_some_getLast (by simp)).symm

theorem emit_seq (i : Nat) (op : Op A) (v : View A) : ∀ st ∈ emit i op v, st.seq = i := by
  cases op <;> cases v <;> simp [emit] <;> (try split) <;> simp_all

theorem emit_sorted (i : Nat) (op : Op A) (v : View A) :
    List.Pairwise (fun a b => a.key < b.key) (emit i op v) := by
  cases op <;> cases v <;> simp [emit, Step.key] <;> (try split) <;> simp_all [Step.key]

theorem run_seq : ∀ (ops : List (Op A)) (i : Nat) (v u : View A) (ss : List (Step A)),
    run ops i v = some (u, ss) → ∀ st ∈ ss, i ≤ st.seq ∧ st.seq < i + ops.length := by
  intro ops
  induction ops with
  | nil => intro i v u ss h; simp [run] at h; obtain ⟨-, rfl⟩ := h; simp
  | cons op ops ih =>
    intro i v u ss h
    simp only [run, Option.bind_eq_bind] at h
    cases hw : apply op v with
    | none => simp [hw] at h
    | some w =>
      cases hr : run ops (i + 1) w with
      | none => simp [hw, hr] at h
      | some r =>
        simp [hw, hr] at h
        obtain ⟨-, rfl⟩ := h
        intro st hst
        rcases List.mem_append.mp hst with he | hs
        · have := emit_seq i op v st he; simp [List.length_cons]; omega
        · have := ih (i + 1) w r.1 r.2 (by rw [hr]) st hs
          simp [List.length_cons]; omega

theorem run_sorted : ∀ (ops : List (Op A)) (i : Nat) (v u : View A) (ss : List (Step A)),
    run ops i v = some (u, ss) → List.Pairwise (fun a b => a.key < b.key) ss := by
  intro ops
  induction ops with
  | nil => intro i v u ss h; simp [run] at h; obtain ⟨-, rfl⟩ := h; simp
  | cons op ops ih =>
    intro i v u ss h
    simp only [run, Option.bind_eq_bind] at h
    cases hw : apply op v with
    | none => simp [hw] at h
    | some w =>
      cases hr : run ops (i + 1) w with
      | none => simp [hw, hr] at h
      | some r =>
        simp [hw, hr] at h
        obtain ⟨-, rfl⟩ := h
        refine List.pairwise_append.mpr ⟨emit_sorted i op v, ih _ _ _ _ (by rw [hr]), ?_⟩
        intro a ha b hb
        have h1 := emit_seq i op v a ha
        have h2 := (run_seq ops (i + 1) w r.1 r.2 (by rw [hr]) b hb).1
        simp only [Step.key]
        split <;> split <;> omega

/-! A run's last step says where it leaves the key. -/

theorem run_last : ∀ (ops : List (Op A)) (i : Nat) (v u : View A) (ss : List (Step A)),
    run ops i v = some (u, ss) → (∀ op ∈ ops, op.isUnstepped = false) →
      u = match ss.getLast? with
        | none => v
        | some st => st.view := by
  intro ops
  induction ops with
  | nil => intro i v u ss h _; simp [run] at h; obtain ⟨rfl, rfl⟩ := h; rfl
  | cons op ops ih =>
    intro i v u ss h hc
    simp only [run, Option.bind_eq_bind] at h
    cases hw : apply op v with
    | none => simp [hw] at h
    | some w =>
      cases hr : run ops (i + 1) w with
      | none => simp [hw, hr] at h
      | some r =>
        simp [hw, hr] at h
        obtain ⟨rfl, rfl⟩ := h
        have hu := ih (i + 1) w r.1 r.2 (by rw [hr]) (fun o ho => hc o (by simp [ho]))
        cases hss : r.2.getLast? with
        | some st =>
          rw [hss] at hu
          rw [List.getLast?_append, hss]
          exact hu
        | none =>
          rw [hss] at hu
          rw [List.getLast?_append, hss]
          have hop := hc op (by simp)
          subst hu
          cases op <;> cases v <;> simp_all [apply, emit, Op.isUnstepped, Step.view] <;>
            (try split at hw) <;> simp_all [Step.view]

/-! The steps before position `i + k` are the first `k` operations' steps. -/

theorem run_take : ∀ (ops : List (Op A)) (k i : Nat) (v u : View A) (ss : List (Step A)),
    run ops i v = some (u, ss) →
      ∃ u', run (ops.take k) i v = some (u', ss.filter (·.seq < i + k)) := by
  intro ops
  induction ops with
  | nil => intro k i v u ss h; simp [run] at h; obtain ⟨-, rfl⟩ := h; exact ⟨v, by simp [run]⟩
  | cons op ops ih =>
    intro k i v u ss h
    simp only [run, Option.bind_eq_bind] at h
    cases hw : apply op v with
    | none => simp [hw] at h
    | some w =>
      cases hr : run ops (i + 1) w with
      | none => simp [hw, hr] at h
      | some r =>
        simp [hw, hr] at h
        obtain ⟨-, rfl⟩ := h
        cases k with
        | zero =>
          refine ⟨v, ?_⟩
          simp only [List.take_zero, run, Nat.add_zero, Option.some.injEq, Prod.mk.injEq,
            true_and]
          symm
          rw [List.filter_eq_nil_iff]
          intro st hst
          rcases List.mem_append.mp hst with he | hs
          · simp [emit_seq i op v st he]
          · have := (run_seq ops (i + 1) w r.1 r.2 (by rw [hr]) st hs).1
            simp; omega
        | succ k =>
          obtain ⟨u', hu'⟩ := ih k (i + 1) w r.1 r.2 (by rw [hr])
          refine ⟨u', ?_⟩
          simp only [List.take_succ_cons, run, Option.bind_eq_bind, hw, Option.bind_some, hu',
            Option.pure_def, Option.some.injEq, Prod.mk.injEq, true_and]
          rw [List.filter_append]
          congr 1
          · symm
            rw [List.filter_eq_self]
            intro st hst
            have := emit_seq i op v st hst
            simp; omega
          · congr 1
            funext st
            simp only [decide_eq_decide]
            omega

/-- The view the first `s` operations leave. -/
def viewAt (ops : List (Op A)) (v : View A) (s : Nat) : Option (View A) :=
  (run (ops.take s) 0 v).map (·.1)

/-- With no unstepped change in the write, the place the receiver finds is where every
authoring view the steps are consistent with holds the key: where the named
step left it, or where it was before the write when no step touched it. -/
theorem place_sound (ops : List (Op A)) (v u : View A) (ss : List (Step A)) (s : Nat)
    (h : run ops 0 v = some (u, ss)) (hc : ∀ op ∈ ops, op.isUnstepped = false) :
    (place false ss s = .untouched → viewAt ops v s = some v) ∧
    (∀ st, place false ss s = .last st → viewAt ops v s = some st.view) := by
  obtain ⟨u', hu'⟩ := run_take ops s 0 v u ss h
  have hc' : ∀ op ∈ ops.take s, op.isUnstepped = false := fun o ho => hc o (List.mem_of_mem_take ho)
  have hl := run_last (ops.take s) 0 v u' _ hu' hc'
  have hs := run_sorted (ops.take s) 0 v u' _ hu'
  simp only [Nat.zero_add] at hl hs hu'
  have hv : viewAt ops v s = some u' := by simp [viewAt, hu']
  constructor
  · intro hp
    simp only [place, Bool.false_eq_true, ↓reduceIte, latest_getLast _ hs] at hp
    cases hg : (ss.filter (·.seq < s)).getLast? with
    | none => rw [hg] at hl; rw [hv, hl]
    | some st => rw [hg] at hp; cases hp
  · intro st hp
    simp only [place, Bool.false_eq_true, ↓reduceIte, latest_getLast _ hs] at hp
    cases hg : (ss.filter (·.seq < s)).getLast? with
    | none => rw [hg] at hp; cases hp
    | some st' =>
      rw [hg] at hp hl
      cases hp
      rw [hv, hl]

/-! The parent a recorded reference names. -/

/-- The parent the author's capture names for a reference whose values key
the parent: a present row in the child's audience or the store. -/
def capture (child store : A) : View A → Option (A × Nat)
  | some (a, g) => if a = child ∨ a = store then some (a, g) else none
  | none => none

/-- The parent a receiver records for a reference over an action's columns,
or none when it refuses: from the key's place, or, when no step touched the
key and the action leaves the reference as it was, the parent its setter
recorded. -/
def named (child store : A) (unchanged : Bool) (setter : Option (A × Nat)) :
    Place A → Option (Option (A × Nat))
  | .last st => some (capture child store st.view)
  | .untouched => if unchanged then some setter else none
  | .unplaced => none

/-- Whatever parent the receiver records is the one the author's capture
names in every authoring view consistent with the steps, given that a
reference left as it was names the authoring view's parent row (§9.1). -/
theorem recorded_sound (child store : A) (unchanged : Bool) (setter : Option (A × Nat))
    (ops : List (Op A)) (v u : View A) (ss : List (Step A)) (s : Nat)
    (h : run ops 0 v = some (u, ss)) (hc : ∀ op ∈ ops, op.isUnstepped = false)
    (display : capture child store v = setter) (p : Option (A × Nat))
    (hn : named child store unchanged setter (place false ss s) = some p) :
    (viewAt ops v s).map (capture child store) = some p := by
  have ⟨hu, hl⟩ := place_sound ops v u ss s h hc
  cases hp : place false ss s with
  | untouched =>
    rw [hp] at hn
    simp only [named] at hn
    split at hn
    · cases hn; simp [hu hp, display]
    · cases hn
  | unplaced => rw [hp] at hn; cases hn
  | last st =>
    rw [hp] at hn
    simp only [named, Option.some.injEq] at hn
    simp [hl st hp, hn]

/-- A key no step touched: two authoring views, one holding the parent in the
child's audience and one without it, give the same steps and capture
different parents. -/
theorem untouched_views_differ (child store : A) (g s : Nat) :
    let ops := List.replicate s (Op.other : Op A)
    run ops 0 (some (child, g)) = some (some (child, g), []) ∧
    run ops 0 none = some (none, []) ∧
    capture child store (some (child, g)) ≠ capture child store none := by
  refine ⟨?_, ?_, by simp [capture]⟩ <;>
  · generalize 0 = i
    induction s generalizing i with
    | zero => rfl
    | succ s ih => simp [List.replicate_succ, run, apply, emit, ih]

/-- A key the write changed at no step: two such changes taking it to
different readable audiences give the same steps, none, and capture different
parents. -/
theorem unstepped_views_differ (child store a : A) (g h : Nat) (hca : child ≠ a)
    (hsa : store ≠ a) (hcs : child ≠ store) :
    run [Op.unstepped child h] 0 (some (a, g)) = some (some (child, h), []) ∧
    run [Op.unstepped store h] 0 (some (a, g)) = some (some (store, h), []) ∧
    capture child store (some (child, h)) ≠ capture child store (some (store, h)) := by
  refine ⟨?_, ?_, ?_⟩
  · simp [run, apply, emit, Ne.symm hca]
  · simp [run, apply, emit, Ne.symm hsa]
  · simp [capture, hcs]

/-! The parent a net change names.

A row's net change is not its steps combined. A row the write deletes and
adds again keeps its incarnation in the net change, an update or no change at
all, while its steps end that incarnation and begin the next. And a deferred
reference can name a parent the write adds only after the step that set it.
So a net change names, for a reference whose values key the parent, the row
the steps leave the key in at the end of the write, at the incarnation the
write's net changes leave that row at. -/

/-- The key's view in the incarnations of the write's net changes: `ends a`
is the incarnation the write's record of audience `a`'s row leaves it at,
when the write holds one. -/
def netView (ends : A → Option Nat) : View A → View A
  | some (a, g) => some (a, (ends a).getD g)
  | none => none

/-- The parent a receiver expects a net change to name for a reference, or
none when it refuses: from where the steps leave the key at the end of the
write, in the net changes' incarnations, or, when no step touched the key,
the parent its setter recorded. -/
def namedNet (child store : A) (ends : A → Option Nat) (setter : Option (A × Nat)) :
    Place A → Option (Option (A × Nat))
  | .last st => some (capture child store (netView ends st.view))
  | .untouched => some setter
  | .unplaced => none

/-- Whatever parent the receiver expects of a net change is the one the
author's capture of that net change names in every authoring view consistent
with the steps: the key's view at the end of the write, in the net changes'
incarnations. A row of the key no step touched has no record in the write
(`hends`), and a reference left untouched names the authoring view's parent
(§9.1). -/
theorem net_sound (child store : A) (ends : A → Option Nat) (setter : Option (A × Nat))
    (ops : List (Op A)) (v u : View A) (ss : List (Step A))
    (h : run ops 0 v = some (u, ss)) (hc : ∀ op ∈ ops, op.isUnstepped = false)
    (hends : ∀ a, (∀ st ∈ ss, st.aud ≠ a) → ends a = none)
    (display : capture child store v = setter) (p : Option (A × Nat))
    (hn : namedNet child store ends setter (place false ss ops.length) = some p) :
    capture child store (netView ends u) = p := by
  have ⟨hu, hl⟩ := place_sound ops v u ss ops.length h hc
  have hv : viewAt ops v ops.length = some u := by simp [viewAt, h]
  cases hp : place false ss ops.length with
  | untouched =>
    rw [hp] at hn
    simp only [namedNet, Option.some.injEq] at hn
    have hvu : u = v := by
      have := hu hp
      rw [hv] at this
      exact Option.some.inj this
    have hempty : ss = [] := by
      have hs := run_sorted ops 0 v u ss h
      simp only [place, Bool.false_eq_true, ↓reduceIte, latest_getLast _
        (List.Pairwise.sublist List.filter_sublist hs)] at hp
      cases hg : (ss.filter (·.seq < ops.length)).getLast? with
      | some st => rw [hg] at hp; cases hp
      | none =>
        rw [List.getLast?_eq_none_iff, List.filter_eq_nil_iff] at hg
        cases ss with
        | nil => rfl
        | cons st rest =>
          have := (run_seq ops 0 v u _ h st (by simp)).2
          exact absurd (by simpa using this) (hg st (by simp))
    subst hvu hempty
    cases u with
    | none => simp [netView, ← hn, display]
    | some p' =>
      obtain ⟨a, g⟩ := p'
      simp [netView, hends a (by simp), ← hn, display]
  | unplaced => rw [hp] at hn; cases hn
  | last st =>
    rw [hp] at hn
    simp only [namedNet, Option.some.injEq] at hn
    have := hl st hp
    rw [hv] at this
    rw [Option.some.inj this, hn]

/-- Ben deletes the combo and adds it again before the cascade: the action's
step names the incarnation the insert began, and the net change the one the
write's net change on the combo continues. -/
theorem readded_net_differs (child store : A) (g g' : Nat) (hg : g ≠ g') :
    let ops := [Op.delete, Op.insert child g', Op.other]
    let ss : List (Step A) := [⟨0, child, true, false, g⟩, ⟨1, child, false, false, g'⟩]
    run ops 0 (some (child, g)) = some (some (child, g'), ss) ∧
    named child store false none (place false ss 2) = some (some (child, g')) ∧
    namedNet child store (fun _ => some g) none (place false ss 3) = some (some (child, g)) ∧
    some (child, g') ≠ some (child, g) := by
  refine ⟨by simp [run, apply, emit], ?_, ?_, by simp [Ne.symm hg]⟩ <;>
    simp [named, namedNet, place, latest, later, Step.key, Step.view, capture, netView]

/-- Ben deletes the combo, the cascade sets Ana's deferred reference to its
key, and he adds it again after: the action's step names no combo, and the
net change the one he added. -/
theorem deferred_net_differs (child store : A) (g g' : Nat) :
    let ops := [Op.delete, Op.other, Op.insert child g']
    let ss : List (Step A) := [⟨0, child, true, false, g⟩, ⟨2, child, false, false, g'⟩]
    run ops 0 (some (child, g)) = some (some (child, g'), ss) ∧
    named child store false none (place false ss 1) = some none ∧
    namedNet child store (fun _ => some g) none (place false ss 3) = some (some (child, g)) := by
  refine ⟨by simp [run, apply, emit], ?_, ?_⟩ <;>
    simp [named, namedNet, place, latest, later, Step.key, Step.view, capture, netView]

end CovenMerge.Ownership.Classification.Place
