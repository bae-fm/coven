/-! References stay readable (§15.3). A row may point at a row in its own
audience or in the store, and nowhere else: `allowed`, the check every
write, move and received change passes (`validate_audience` in
coven-merge's `input.rs`, which refuses the rest with `ReferenceAudience`).

Who reads an audience is the store's membership: every member reads the
store, and a circle's members read the circle. `allowed_readable` proves
that an allowed reference's parent is readable by every member who reads
the row, under any membership; `readable_allowed` proves the check refuses
exactly the references some membership would leave unreadable, so it
refuses nothing it needn't. `allowed` takes no membership, so a change of
members never breaks a reference: `allowed_readable` holds for every one.
`moved_refused_iff` lifts the check to a write: it is refused exactly when
some reference, with every row's audience as the write leaves it, points
out of what its row's readers read, as when a note moves into a circle
while a store row still points at it. -/

namespace CovenMerge.References

variable {Aud Member : Type} [DecidableEq Aud]

/-- A reference from a row in `row` to a row in `parent` is allowed: the
parent is in the store or in the row's own audience. -/
def allowed (store row parent : Aud) : Bool := decide (parent = store) || decide (parent = row)

/-- Whether member `m` reads audience `a`, given who is in each circle:
everyone reads the store. -/
def reads (store : Aud) (inCircle : Aud → Member → Bool) (m : Member) (a : Aud) : Bool :=
  decide (a = store) || inCircle a m

/-- An allowed reference's parent is readable by every member who reads its
row, whoever is in which circle: every device that reads a row can check
its foreign keys. -/
theorem allowed_readable (store row parent : Aud) (ok : allowed store row parent = true)
    (inCircle : Aud → Member → Bool) (m : Member) (readsRow : reads store inCircle m row = true) :
    reads store inCircle m parent = true := by
  simp only [allowed, Bool.or_eq_true, decide_eq_true_eq] at ok
  rcases ok with s | r
  · simp [reads, s]
  · subst r; exact readsRow

/-- The check refuses only what some membership would leave unreadable: a
reference whose parent every reader of its row reads, under every
membership with a member, is allowed. -/
theorem readable_allowed (store row parent : Aud) (m : Member)
    (always : ∀ inCircle : Aud → Member → Bool,
      reads store inCircle m row = true → reads store inCircle m parent = true) :
    allowed store row parent = true := by
  have h := always (fun a _ => decide (a = row)) (by simp [reads])
  simp only [reads, Bool.or_eq_true, decide_eq_true_eq] at h
  simp only [allowed, Bool.or_eq_true, decide_eq_true_eq]
  exact h

/-- A write's references: each row that points at another, by row. -/
structure Refs (Row : Type) where
  refs : List (Row × Row)

/-- The write is refused when a reference, with each row's audience as the
write leaves it, isn't allowed. -/
def refused {Row : Type} (store : Aud) (aud : Row → Aud) (w : Refs Row) : Bool :=
  w.refs.any fun (child, parent) => !allowed store (aud child) (aud parent)

/-- Refused exactly when some reference leaves its parent unreadable to a
reader of its row under some membership, with audiences as the write
leaves them. -/
theorem moved_refused_iff {Row : Type} (store : Aud) (aud : Row → Aud) (w : Refs Row) (m : Member) :
    refused store aud w = true ↔
      ∃ ref ∈ w.refs, ∃ inCircle : Aud → Member → Bool,
        reads store inCircle m (aud ref.1) = true ∧ reads store inCircle m (aud ref.2) = false := by
  simp only [refused, List.any_eq_true, Bool.not_eq_true']
  constructor
  · rintro ⟨⟨child, parent⟩, mem, bad⟩
    refine ⟨(child, parent), mem, fun a _ => decide (a = aud child), by simp [reads], ?_⟩
    simp only [allowed, Bool.or_eq_false_iff, decide_eq_false_iff_not] at bad
    simp [reads, bad.1, bad.2]
  · rintro ⟨⟨child, parent⟩, mem, inCircle, readsRow, unread⟩
    refine ⟨(child, parent), mem, ?_⟩
    cases ok : allowed store (aud child) (aud parent)
    · rfl
    · have := allowed_readable store _ _ ok inCircle m readsRow
      simp_all

namespace Examples

/-- §15.3's table: the store is 0, Gifts 1, Recipes 2. -/
inductive Row | note42 | pin3 | link10 | pin2 | link5
  deriving DecidableEq

def before : Row → Nat
  | .note42 => 0 | .pin3 => 1 | .link10 => 0 | .pin2 => 2 | .link5 => 0

/-- Gifts' pin 3 may point at store note 42; store link 10 and Recipes' pin
2 may not point at pin 3. -/
example : allowed 0 (before .pin3) (before .note42) = true ∧
    allowed 0 (before .link10) (before .pin3) = false ∧
    allowed 0 (before .pin2) (before .pin3) = false := by decide

/-- Moving note 42 into Gifts while store link 5 points at it is refused. -/
example : refused 0 (fun r => if r = .note42 then 1 else before r)
    ⟨[(.pin3, .note42), (.link5, .note42)]⟩ = true := by decide

/-- Without link 5 the move is allowed: pin 3 is in Gifts too. -/
example : refused 0 (fun r => if r = .note42 then 1 else before r)
    ⟨[(.pin3, .note42)]⟩ = false := by decide

end Examples

end CovenMerge.References
