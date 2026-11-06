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
along, record it alike (`Op.move`).

The write can also change a row of the key at no step. A removed row (§8.4)
that a move carries along is one: SQLite's tables never held it, so it is
never the view's row (`Op.aside`). A forged write can claim the same of a
change that did move the key in the view (`Op.unstepped`). Both leave the
row's record changed beyond its steps, and the receiver sees only that
(`marks`).

* `place` is `StepIndex::place`: the latest earlier step on any row
  with the key, a move's insert half after its delete half.
* `run_settles`: after that step, or from the start when there is none, the
  view moves only through rows changed at no step.
* `recorded_sound`: the parent the receiver records (`named`, through
  `resolve`) is the one the author's capture names in every run with the
  same steps and marks, forged ones included: where the step left the key, or
  where the setter's parent is, unless a row changed at no step could hold
  the key where the capture would see it (`seen`).
* `net_sound` does the same for a row's net change, which names the parent
  where the steps leave the key at the end of the write, at the incarnation
  the write's net changes leave that row at (`namedNet`).
  `readded_net_differs` and `deferred_net_differs` are why that is not the
  parent an action's step names.
* `aside_placed`: a removed row carried beside the view's row leaves the
  parent placed. `untouched_views_differ` and `unstepped_views_differ` are
  why the receiver refuses the rest: two authoring views with the same steps
  and marks capture different parents. -/
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
  /-- A change at no step that moves the key in the view to `to`. -/
  | unstepped (to : View A)
  /-- A change at no step of the key's row in audience `b`, which the view
  doesn't hold. -/
  | aside (b : A)
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

/-- The audience a view holds the key in. -/
def View.aud : View A → Option A
  | some (a, _) => some a
  | none => none

/-- The view an operation leaves, or none when it can't run in this one. -/
def apply : Op A → View A → Option (View A)
  | .insert a g, none => some (some (a, g))
  | .update, some p => some (some p)
  | .delete, some _ => some none
  | .move b g, some (a, _) => if a = b then none else some (some (b, g))
  | .unstepped t, v => if t = v then none else some t
  | .aside b, v => if v.aud = some b then none else some v
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

/-- The audiences whose row of the key an operation changes at no step: the
rows a change at no step leaves and enters, or the row aside. -/
def opMarks : Op A → View A → List A
  | .unstepped t, v => v.aud.toList ++ t.aud.toList
  | .aside b, _ => [b]
  | _, _ => []

/-- Every audience whose row of the key the run changes at no step. -/
def marks : List (Op A) → View A → List A
  | [], _ => []
  | op :: ops, v => match apply op v with
    | some w => opMarks op v ++ marks ops w
    | none => []

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
  | last (st : Step A)
deriving DecidableEq, Repr

/-- Where the steps before position `s` leave the key. -/
def place (steps : List (Step A)) (s : Nat) : Place A :=
  match latest (steps.filter (·.seq < s)) with
  | none => .untouched
  | some st => .last st

/-! The emitted steps are in order. -/

omit [DecidableEq A] in
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

omit [DecidableEq A] in
theorem latest_getLast (l : List (Step A)) (h : List.Pairwise (fun a b => a.key < b.key) l) :
    latest l = l.getLast? := by
  cases l with
  | nil => rfl
  | cons x xs =>
    simp only [latest, foldl_later_last x xs h]
    exact (List.getLast?_eq_some_getLast (by simp)).symm

omit [DecidableEq A] in
theorem emit_seq (i : Nat) (op : Op A) (v : View A) : ∀ st ∈ emit i op v, st.seq = i := by
  cases op <;> cases v <;> simp [emit] <;> (try split) <;> simp_all

omit [DecidableEq A] in
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

/-! A run's last step says where it leaves the key, unless a change at no
step moved it since, which marks the row it left and the row it entered. -/

/-- The view a run's steps say it leaves: its last step's, or the one it
began from. -/
def stepped (v : View A) (ss : List (Step A)) : View A :=
  match ss.getLast? with
  | none => v
  | some st => st.view

theorem emit_last (i : Nat) (op : Op A) (v w : View A) (hw : apply op v = some w)
    (st : Step A) (hl : (emit i op v).getLast? = some st) : st.view = w := by
  cases op with
  | insert a g =>
    cases v <;> simp [apply] at hw
    subst hw; simp [emit] at hl; subst hl; simp [Step.view]
  | update =>
    cases v with
    | none => simp [apply] at hw
    | some p =>
      simp [apply] at hw; subst hw; simp [emit] at hl; subst hl; simp [Step.view]
  | delete =>
    cases v with
    | none => simp [apply] at hw
    | some p =>
      simp [apply] at hw; subst hw; simp [emit] at hl; subst hl; simp [Step.view]
  | move b g' =>
    cases v with
    | none => simp [apply] at hw
    | some p =>
      obtain ⟨a, g⟩ := p
      simp [apply] at hw; obtain ⟨_, rfl⟩ := hw
      simp [emit] at hl; subst hl; simp [Step.view]
  | unstepped t => simp [emit] at hl
  | aside b => simp [emit] at hl
  | other => simp [emit] at hl

theorem emit_nil (i : Nat) (op : Op A) (v w : View A) (hw : apply op v = some w)
    (he : emit i op v = []) : w = v ∨ ∃ t, op = .unstepped t ∧ w = t := by
  cases op <;> cases v <;> simp_all [apply, emit] <;> (try split at hw) <;> simp_all

theorem run_settles : ∀ (ops : List (Op A)) (i : Nat) (v u : View A) (ss : List (Step A)),
    run ops i v = some (u, ss) →
      u = stepped v ss ∨
        ((∀ a, (stepped v ss).aud = some a → a ∈ marks ops v) ∧
          (∀ b, u.aud = some b → b ∈ marks ops v)) := by
  intro ops
  induction ops with
  | nil => intro i v u ss h; simp [run] at h; obtain ⟨rfl, rfl⟩ := h; left; rfl
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
        obtain ⟨rfl, rfl⟩ := h
        have hm : ∀ a, a ∈ marks ops w → a ∈ marks (op :: ops) v := by
          intro a ha; simp [marks, hw, ha]
        have hu := ih (i + 1) w r.1 r.2 (by rw [hr])
        cases hss : r.2.getLast? with
        | some st =>
          have hs : stepped v (emit i op v ++ r.2) = stepped w r.2 := by
            simp [stepped, List.getLast?_append, hss]
          rw [hs]
          rcases hu with hu | ⟨h1, h2⟩
          · exact Or.inl hu
          · exact Or.inr ⟨fun a ha => hm a (h1 a ha), fun b hb => hm b (h2 b hb)⟩
        | none =>
          have hsw : stepped w r.2 = w := by simp [stepped, hss]
          rw [hsw] at hu
          cases he : (emit i op v).getLast? with
          | some st =>
            have hs : stepped v (emit i op v ++ r.2) = w := by
              simp [stepped, List.getLast?_append, hss, he, emit_last i op v w hw st he]
            rw [hs]
            rcases hu with hu | ⟨h1, h2⟩
            · exact Or.inl hu
            · exact Or.inr ⟨fun a ha => hm a (h1 a ha), fun b hb => hm b (h2 b hb)⟩
          | none =>
            have hnil : emit i op v = [] := List.getLast?_eq_none_iff.mp he
            have hs : stepped v (emit i op v ++ r.2) = v := by
              simp [stepped, List.getLast?_append, hss, he]
            rw [hs]
            rcases emit_nil i op v w hw hnil with rfl | ⟨t, rfl, rfl⟩
            · rcases hu with hu | ⟨h1, h2⟩
              · exact Or.inl hu
              · exact Or.inr ⟨fun a ha => hm a (h1 a ha), fun b hb => hm b (h2 b hb)⟩
            · right
              refine ⟨fun a ha => ?_, fun b hb => ?_⟩
              · simp [marks, hw, opMarks, ha]
              · rcases hu with rfl | ⟨_, h2⟩
                · simp [marks, hw, opMarks, hb]
                · exact hm b (h2 b hb)

/-- The marks of a run's first operations are among the whole run's. -/
theorem marks_take : ∀ (ops : List (Op A)) (k : Nat) (v : View A) (a : A),
    a ∈ marks (ops.take k) v → a ∈ marks ops v := by
  intro ops
  induction ops with
  | nil => intro k v a h; simp [marks] at h
  | cons op ops ih =>
    intro k v a h
    cases k with
    | zero => simp [marks] at h
    | succ k =>
      simp only [List.take_succ_cons, marks] at h ⊢
      cases hw : apply op v with
      | none => simp [hw] at h
      | some w =>
        simp only [hw, List.mem_append] at h ⊢
        rcases h with h | h
        · exact Or.inl h
        · exact Or.inr (ih k w a h)

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

/-- Where the steps before `s` leave the key, in the view's terms: the
latest step's view, or the view before the write when no step touched it.
The view at `s` is there, unless a change at no step moved it, which marks
the row it left and the row it entered. -/
theorem place_settles (ops : List (Op A)) (v u : View A) (ss : List (Step A)) (s : Nat)
    (h : run ops 0 v = some (u, ss)) :
    ∃ w, viewAt ops v s = some w ∧
      let base := match place ss s with
        | .untouched => v
        | .last st => st.view
      (w = base ∨ ((∀ a, base.aud = some a → a ∈ marks ops v) ∧
        (∀ b, w.aud = some b → b ∈ marks ops v))) := by
  obtain ⟨u', hu'⟩ := run_take ops s 0 v u ss h
  have hs := run_sorted (ops.take s) 0 v u' _ hu'
  simp only [Nat.zero_add] at hu' hs
  refine ⟨u', by simp [viewAt, hu'], ?_⟩
  have hb : (match place ss s with
      | .untouched => v
      | .last st => st.view) = stepped v (ss.filter (·.seq < s)) := by
    simp only [place, latest_getLast _ hs, stepped]
    cases (ss.filter (·.seq < s)).getLast? <;> rfl
  simp only [hb]
  rcases run_settles (ops.take s) 0 v u' _ hu' with h1 | ⟨h1, h2⟩
  · exact Or.inl h1
  · exact Or.inr ⟨fun a ha => marks_take ops s v a (h1 a ha),
      fun b hb => marks_take ops s v b (h2 b hb)⟩

/-- With no change at no step, the place the receiver finds is where every
authoring view the steps are consistent with holds the key: where the named
step left it, or where it was before the write when no step touched it. -/
theorem place_sound (ops : List (Op A)) (v u : View A) (ss : List (Step A)) (s : Nat)
    (h : run ops 0 v = some (u, ss)) (hq : marks ops v = []) :
    (place ss s = .untouched → viewAt ops v s = some v) ∧
    (∀ st, place ss s = .last st → viewAt ops v s = some st.view) := by
  obtain ⟨w, hw, hb⟩ := place_settles ops v u ss s h
  have base : ∀ x : View A, (w = x ∨ ((∀ a, x.aud = some a → a ∈ marks ops v) ∧
      (∀ b, w.aud = some b → b ∈ marks ops v))) → x.aud.isSome → w = x := by
    intro x hx hs
    rcases hx with hx | ⟨h1, _⟩
    · exact hx
    · obtain ⟨a, ha⟩ := Option.isSome_iff_exists.mp hs
      have := h1 a ha; rw [hq] at this; simp at this
  have quiet : ∀ x : View A, (w = x ∨ ((∀ a, x.aud = some a → a ∈ marks ops v) ∧
      (∀ b, w.aud = some b → b ∈ marks ops v))) → w = x := by
    intro x hx
    rcases hx with hx | ⟨h1, h2⟩
    · exact hx
    · cases hx' : x.aud with
      | some a => have := h1 a hx'; rw [hq] at this; simp at this
      | none =>
        cases hwa : w.aud with
        | some b => have := h2 b hwa; rw [hq] at this; simp at this
        | none =>
          cases x <;> cases w <;> simp_all [View.aud]
  constructor
  · intro hp; rw [hp] at hb; rw [hw, quiet v hb]
  · intro st hp; rw [hp] at hb; rw [hw, quiet st.view hb]

/-! The parent a recorded reference names. -/

/-- The audiences a reference of a row in `child` may name a parent in. -/
def seen (child store : A) (a : A) : Bool := decide (a = child ∨ a = store)

/-- The parent the author's capture names for a reference whose values key
the parent: a present row in the child's audience or the store. -/
def capture (child store : A) : View A → Option (A × Nat)
  | some (a, g) => if a = child ∨ a = store then some (a, g) else none
  | none => none

/-- The parent a receiver records, given where the steps leave the key, the
audiences whose row of the key the write changed at no step (`marked`), the
parent the setter recorded, and how a placed view maps to the parent it
names (`f`), or none when it refuses. A step's view stands unless a change at
no step marked its row; a delete's, or a view outside the reference's
reach, stands unless a change at no step could have put the key in a row the
reference reaches. With no step, the setter's parent stands unless its row
is marked, or, when it names none, unless a reachable row is marked. -/
def resolve (child store : A) (marked : List A) (f : View A → View A)
    (setter : Option (A × Nat)) : Place A → Option (Option (A × Nat))
  | .last st =>
    if st.aud ∈ marked ∨ st.delete then
      if (st.view.aud.any (seen child store)) ∨ marked.any (seen child store) then none
      else some none
    else some (capture child store (f st.view))
  | .untouched => match setter with
    | some (a, g) => if a ∈ marked then none else some (some (a, g))
    | none => if marked.any (seen child store) then none else some none

theorem capture_unseen (child store : A) (x : View A)
    (hx : x.aud.any (seen child store) = false) : capture child store x = none := by
  cases x with
  | none => rfl
  | some p =>
    obtain ⟨a, g⟩ := p
    simp_all [View.aud, seen, capture]

/-- Whatever parent the receiver resolves is the one the author's capture
names, through `f`, in every run with the same steps and marks, a forged
change at no step included, given that the setter's parent is the capture of
the view before the write (§9.1's display rule) and `f` keeps a view's
audience. -/
theorem resolve_sound (child store : A) (marked : List A) (f : View A → View A)
    (hf : ∀ x, (f x).aud = x.aud) (setter : Option (A × Nat))
    (ops : List (Op A)) (v u : View A) (ss : List (Step A)) (s : Nat)
    (h : run ops 0 v = some (u, ss)) (hm : ∀ a ∈ marks ops v, a ∈ marked)
    (display : capture child store v = setter)
    (hfv : ∀ a g, v = some (a, g) → a ∉ marked → (∀ st ∈ ss, st.seq < s → st.aud ≠ a) →
      f v = v)
    (p : Option (A × Nat))
    (hn : resolve child store marked f setter (place ss s) = some p) :
    (viewAt ops v s).map (fun w => capture child store (f w)) = some p := by
  obtain ⟨w, hw, hb⟩ := place_settles ops v u ss s h
  rw [hw]
  simp only [Option.map_some, Option.some.injEq]
  have unseen : ∀ x : View A, x.aud.any (seen child store) = false →
      capture child store (f x) = none := fun x hx =>
    capture_unseen child store (f x) (by rw [hf]; exact hx)
  cases hp : place ss s with
  | last st =>
    rw [hp] at hn hb
    simp only [resolve] at hn
    split at hn
    · split at hn
      · cases hn
      · rename_i hmk hseen
        cases hn
        simp only [not_or, Bool.not_eq_true, List.any_eq_false] at hseen
        apply unseen
        rcases hb with rfl | ⟨_, h2⟩
        · exact hseen.1
        · cases hwa : w.aud with
          | none => rfl
          | some b =>
            have := hseen.2 b (hm b (h2 b hwa))
            simp [this]
    · rename_i hmk
      cases hn
      simp only [not_or, Bool.not_eq_true] at hmk
      rcases hb with rfl | ⟨h1, _⟩
      · rfl
      · have := hm st.aud (h1 st.aud (by simp [Step.view, hmk.2, View.aud]))
        exact absurd this hmk.1
  | untouched =>
    rw [hp] at hn hb
    simp only [resolve] at hn
    cases hs : setter with
    | some ag =>
      obtain ⟨a, g⟩ := ag
      rw [hs] at hn
      simp only at hn
      split at hn
      · cases hn
      · rename_i ha
        cases hn
        have hv : v = some (a, g) := by
          rw [hs] at display
          cases v with
          | none => simp [capture] at display
          | some q =>
            obtain ⟨b, k⟩ := q
            simp only [capture] at display
            split at display <;> simp_all
        have hwv : w = v := by
          rcases hb with hb | ⟨h1, _⟩
          · exact hb
          · exact absurd (hm a (h1 a (by simp [hv, View.aud]))) ha
        have hnone : ∀ st ∈ ss, st.seq < s → st.aud ≠ a := by
          intro st hst hlt
          have hs' := run_sorted ops 0 v u ss h
          simp only [place, latest_getLast _ (List.Pairwise.sublist List.filter_sublist hs')]
            at hp
          cases hg : (ss.filter (·.seq < s)).getLast? with
          | some _ => rw [hg] at hp; cases hp
          | none =>
            rw [List.getLast?_eq_none_iff, List.filter_eq_nil_iff] at hg
            exact absurd (by simpa using hlt) (hg st hst)
        rw [hwv, hfv a g hv ha hnone, ← hs, ← display]
    | none =>
      rw [hs] at hn
      simp only at hn
      split at hn
      · cases hn
      · rename_i hseen
        cases hn
        simp only [Bool.not_eq_true, List.any_eq_false] at hseen
        rw [hs] at display
        have hvu : v.aud.any (seen child store) = false := by
          cases v with
          | none => rfl
          | some q =>
            obtain ⟨b, k⟩ := q
            simp only [capture] at display
            split at display
            · cases display
            · rename_i hb; simp [View.aud, seen, hb]
        apply unseen
        rcases hb with rfl | ⟨_, h2⟩
        · exact hvu
        · cases hwa : w.aud with
          | none => rfl
          | some b =>
            have := hseen b (hm b (h2 b hwa))
            simp [this]

/-- The parent a receiver records for a reference over an action's columns,
or none when it refuses: `resolve` at the step, or, when no step touched the
key, only if the action leaves the reference as it was. -/
def named (child store : A) (marked : List A) (unchanged : Bool) (setter : Option (A × Nat))
    (p : Place A) : Option (Option (A × Nat)) :=
  match p with
  | .untouched => if unchanged then resolve child store marked id setter p else none
  | .last _ => resolve child store marked id setter p

/-- Whatever parent the receiver records is the one the author's capture
names in every authoring view consistent with the steps and marks, given
that a reference left as it was names the authoring view's parent (§9.1). -/
theorem recorded_sound (child store : A) (marked : List A) (unchanged : Bool)
    (setter : Option (A × Nat)) (ops : List (Op A)) (v u : View A) (ss : List (Step A))
    (s : Nat) (h : run ops 0 v = some (u, ss)) (hm : ∀ a ∈ marks ops v, a ∈ marked)
    (display : capture child store v = setter) (p : Option (A × Nat))
    (hn : named child store marked unchanged setter (place ss s) = some p) :
    (viewAt ops v s).map (capture child store) = some p := by
  have hr : resolve child store marked id setter (place ss s) = some p := by
    cases hp : place ss s with
    | untouched =>
      rw [hp] at hn; simp only [named] at hn
      split at hn
      · exact hn
      · cases hn
    | last st => rw [hp] at hn; exact hn
  exact resolve_sound child store marked id (fun _ => rfl) setter ops v u ss s h hm display
    (fun _ _ _ _ _ => rfl) p hr

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

/-- A key the write changed at no step: a change that moved the setter's row
to the store, and one that moved it there and back, give the same steps,
none, and mark the same rows, but capture different parents. So the receiver
refuses a setter whose row is marked. -/
theorem unstepped_views_differ (child store : A) (g h : Nat) (hcs : child ≠ store) :
    run [Op.unstepped (some (store, h))] 0 (some (child, g)) =
      some (some (store, h), []) ∧
    run [Op.unstepped (some (store, h)), Op.unstepped (some (child, g))] 0 (some (child, g)) =
      some (some (child, g), []) ∧
    marks [Op.unstepped (some (store, h))] (some (child, g)) = [child, store] ∧
    marks [Op.unstepped (some (store, h)), Op.unstepped (some (child, g))] (some (child, g)) =
      [child, store, store, child] ∧
    capture child store (some (store, h)) ≠ capture child store (some (child, g)) := by
  refine ⟨?_, ?_, ?_, ?_, ?_⟩ <;>
    simp [run, apply, emit, marks, opMarks, View.aud, capture, hcs, Ne.symm hcs]

/-- A removed row carried in another audience beside the view's row: no step,
its row marked, and the receiver records the setter's parent, which the view
still holds. -/
theorem aside_placed (child store b : A) (g : Nat) (hb : b ≠ child) :
    run [Op.aside b] 0 (some (child, g)) = some (some (child, g), []) ∧
    named child store (marks [Op.aside b] (some (child, g))) true (some (child, g))
      (place [] 1) = some (some (child, g)) := by
  refine ⟨?_, ?_⟩
  · simp [run, apply, emit, View.aud, Ne.symm hb]
  · simp [named, resolve, place, latest, marks, apply, opMarks, View.aud, Ne.symm hb]

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
none when it refuses: `resolve` at the end of the write, in the net changes'
incarnations. -/
def namedNet (child store : A) (marked : List A) (ends : A → Option Nat)
    (setter : Option (A × Nat)) (p : Place A) : Option (Option (A × Nat)) :=
  resolve child store marked (netView ends) setter p

/-- Whatever parent the receiver expects of a net change is the one the
author's capture of that net change names in every run consistent with the
steps and marks: the key's view at the end of the write, in the net
changes' incarnations. A row of the key no step touched and the write did
not change has no record in the write (`hends`), and a reference left
untouched names the authoring view's parent (§9.1). -/
theorem net_sound (child store : A) (marked : List A) (ends : A → Option Nat)
    (setter : Option (A × Nat)) (ops : List (Op A)) (v u : View A) (ss : List (Step A))
    (h : run ops 0 v = some (u, ss)) (hm : ∀ a ∈ marks ops v, a ∈ marked)
    (hends : ∀ a, (∀ st ∈ ss, st.aud ≠ a) → a ∉ marked → ends a = none)
    (display : capture child store v = setter) (p : Option (A × Nat))
    (hn : namedNet child store marked ends setter (place ss ops.length) = some p) :
    capture child store (netView ends u) = p := by
  have hv : viewAt ops v ops.length = some u := by simp [viewAt, h]
  have := resolve_sound child store marked (netView ends)
    (fun x => by cases x with
      | none => rfl
      | some q => obtain ⟨a, g⟩ := q; rfl)
    setter ops v u ss ops.length h hm display
    (fun a g hva ha hst => by
      have hlen : ∀ st ∈ ss, st.aud ≠ a := fun st hs =>
        hst st hs (by simpa using (run_seq ops 0 v u ss h st hs).2)
      rw [hva]; simp [netView, hends a hlen ha])
    p hn
  rw [hv] at this
  simpa using this

/-- Ben deletes the combo and adds it again before the cascade: the action's
step names the incarnation the insert began, and the net change the one the
write's net change on the combo continues. -/
theorem readded_net_differs (child store : A) (g g' : Nat) (hg : g ≠ g') :
    let ops := [Op.delete, Op.insert child g', Op.other]
    let ss : List (Step A) := [⟨0, child, true, false, g⟩, ⟨1, child, false, false, g'⟩]
    run ops 0 (some (child, g)) = some (some (child, g'), ss) ∧
    named child store [] false none (place ss 2) = some (some (child, g')) ∧
    namedNet child store [] (fun _ => some g) none (place ss 3) = some (some (child, g)) ∧
    some (child, g') ≠ some (child, g) := by
  refine ⟨by simp [run, apply, emit], ?_, ?_, by simp [Ne.symm hg]⟩ <;>
    simp [named, namedNet, resolve, place, latest, later, Step.key, Step.view, capture, netView]

/-- Ben deletes the combo, the cascade sets Ana's deferred reference to its
key, and he adds it again after: the action's step names no combo, and the
net change the one he added. -/
theorem deferred_net_differs (child store : A) (g g' : Nat) :
    let ops := [Op.delete, Op.other, Op.insert child g']
    let ss : List (Step A) := [⟨0, child, true, false, g⟩, ⟨2, child, false, false, g'⟩]
    run ops 0 (some (child, g)) = some (some (child, g'), ss) ∧
    named child store [] false none (place ss 1) = some none ∧
    namedNet child store [] (fun _ => some g) none (place ss 3) = some (some (child, g)) := by
  refine ⟨by simp [run, apply, emit], ?_, ?_⟩ <;>
    simp [named, namedNet, resolve, place, latest, later, Step.key, Step.view, capture, netView,
      View.aud]

end CovenMerge.Ownership.Classification.Place
