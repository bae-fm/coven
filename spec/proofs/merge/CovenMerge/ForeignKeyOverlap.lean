import CovenMerge.ForeignKeyRecords
import CovenMerge.ForeignKeyAuthoring

/-! Actions whose columns another reference shares (§9.1, §9.6, D5).

A cascade into a compound key also writes the columns another reference of
the child shares with it, so its recorded step names that reference's parent
too. The receiver knows that parent when the write determines it:

* `Overlap`: the action leaves the other reference's values as they were,
  so the reference keeps the parent it held (`kept_classified`). Without that
  parent the receiver refuses SQLite's own action (`unknown_refuses`).
* `Retargeted`: the action points the other reference at a row an earlier
  step of the write made, whose incarnation that step gives
  (`retargeted_classified`).
* `one_view_misjudged` is why the receiver refuses the rest. Where the
  action points the other reference at a row the write never touched, or
  carries a reference whose parent may have ended before the write, the step
  SQLite records depends on that row's incarnation in the authoring view,
  which no step of the write carries. Two authoring views with the same
  write then make different steps (`Untouched.views_differ`,
  `Stale.views_differ`), and a receiver reading only the write accepts at
  most one of them: it refuses SQLite's action in one view, or accepts in the
  other a step SQLite would not make, which points the other member's row at
  an incarnation that has ended. Its author runs the receivers' check before
  the write leaves and refuses it too (`rebuild_refusal_refuses`).

Rows: 0 artist a, 1 album (a, x), 2 album after its key changes, 3 Ana's
track, 4 another artist. Columns: 10 and 11 the track's artist and album, 20
and 21 the album's artist and id, 30 an artist's id. References: 0 the
track's artist, 1 the track's album over both its columns, 2 the album's
artist. Values: 1 a, 2 x, 3 y, 4 the other artist. -/
namespace CovenMerge.Ownership.Classification.Overlapping

open ForeignKey

abbrev Step := Recorded Nat Nat Nat Nat

/-- The track's reference to its album, cascading on update. -/
def albumRef : Reference Nat Nat Nat :=
  ⟨1, 1, 3, 1, [10, 11], [some 1, some 2], .cascade, .cascade, [20, 21], []⟩

def held (others : Option (List (Nat × Nat × Nat × Nat))) : Held Nat Nat Nat Nat :=
  ⟨albumRef, 1, [], [], others⟩

/-- A receiver accepts at most one of two different steps for the same held
reference and parent step. -/
theorem one_view_misjudged {Row Col Value Name : Type} [DecidableEq Row] [DecidableEq Col]
    [DecidableEq Value] [DecidableEq Name] (h : Held Row Col Value Name)
    (p : Recorded Row Col Value Name) (a b : Recorded Row Col Value Name) (differ : a ≠ b) :
    heldConsequence h p ≠ some a ∨ heldConsequence h p ≠ some b := by
  by_cases ha : heldConsequence h p = some a
  · exact Or.inr fun hb => differ (Option.some.inj (ha.symm.trans hb))
  · exact Or.inl ha

namespace Overlap

/-! Ben changes his album's id from x to y. ON UPDATE CASCADE writes Ana's
track's artist and album; its artist stays a, so its artist reference keeps
naming artist a. -/

def trace (others : Option (List (Nat × Nat × Nat × Nat))) :
    Trace Nat (Held Nat Nat Nat Nat) Step where
  steps := [⟨⟨1, 1, .upd, [(21, some 3)]⟩, [], some (2, 1)⟩,
    ⟨⟨2, 0, .ins, [(20, some 1), (21, some 3)]⟩, [(20, 2, 0, 1)], none⟩,
    ⟨⟨3, 1, .upd, [(10, some 1), (11, some 3)]⟩, [(10, 1, 2, 1), (11, 1, 2, 1), (10, 0, 0, 1)], none⟩]
  refs i r := if i = 2 ∧ r = 3 then [held others] else []

def made : Nat → Made
  | 2 => .action 0
  | _ => .statement

theorem faithful : Faithful heldRule (trace (some [(10, 0, 0, 1)])) made := by
  intro i j h
  rcases i with _ | _ | _ | i
  · cases h
  · cases h
  · cases h
    exact (candidate_iff _ _ 2 0).mp (by decide)
  · cases h

theorem kept_classified : cause heldRule (trace (some [(10, 0, 0, 1)])) 2 = some 0 := by decide

/-- Without the kept parent, SQLite's action is refused as a direct edit. -/
theorem unknown_refuses : made 2 = .action 0 ∧ cause heldRule (trace none) 2 = none := by decide

end Overlap

namespace Retargeted

/-! Ben inserts artist 4 and moves his album to it. The cascade points Ana's
track's artist reference at artist 4, whose incarnation the insert made. -/

def trace : Trace Nat (Held Nat Nat Nat Nat) Step where
  steps := [⟨⟨4, 0, .ins, [(30, some 4)]⟩, [], none⟩,
    ⟨⟨1, 1, .upd, [(20, some 4)]⟩, [], some (2, 1)⟩,
    ⟨⟨2, 0, .ins, [(20, some 4), (21, some 2)]⟩, [(20, 2, 4, 1)], none⟩,
    ⟨⟨3, 1, .upd, [(10, some 4), (11, some 2)]⟩, [(10, 1, 2, 1), (11, 1, 2, 1), (10, 0, 4, 1)], none⟩]
  refs i r := if i = 3 ∧ r = 3 then [held (some [(10, 0, 4, 1)])] else []

def made : Nat → Made
  | 3 => .action 1
  | _ => .statement

theorem faithful : Faithful heldRule trace made := by
  intro i j h
  rcases i with _ | _ | _ | _ | i
  · cases h
  · cases h
  · cases h
  · cases h
    exact (candidate_iff _ _ 3 1).mp (by decide)
  · cases h

theorem retargeted_classified : cause heldRule trace 3 = some 1 := by decide

end Retargeted

namespace Untouched

/-! Ben moves his album to artist 4, which his write never touches. If
artist 4's first incarnation is live, the cascade names it; if another write
the author had read deleted it and inserted it again, the cascade names its
third incarnation instead. -/

def albumMove : Step := ⟨⟨1, 1, .upd, [(20, some 4)]⟩, [], some (2, 1)⟩

def track (generation : Nat) : Step :=
  ⟨⟨3, 1, .upd, [(10, some 4), (11, some 2)]⟩,
    [(10, 1, 2, 1), (11, 1, 2, 1), (10, 0, 4, generation)], none⟩

theorem views_differ : track 1 ≠ track 3 := by decide

/-- Whatever parent a receiver supplies for the untouched artist, it accepts
the cascade in at most one of the two views. -/
theorem misjudged (others : Option (List (Nat × Nat × Nat × Nat))) :
    heldConsequence (held others) albumMove ≠ some (track 1) ∨
    heldConsequence (held others) albumMove ≠ some (track 3) :=
  one_view_misjudged _ _ _ _ views_differ

end Untouched

namespace Stale

/-! The track's album reference shares column 10 with its artist reference.
If artist a's incarnation the track's setter named has ended before Ben's
write, the track shows its artist NULL (ON DELETE SET NULL, §9.1), and the
cascade from Ben's album key change records that NULL with no parent; if it
is live, the cascade records a with its parent. No step of the write says
which. -/

def albumChange : Step := ⟨⟨1, 1, .upd, [(21, some 3)]⟩, [], some (2, 1)⟩

def carrying (artist : Option Nat) (others : List (Nat × Nat × Nat × Nat)) :
    Held Nat Nat Nat Nat :=
  ⟨{ albumRef with columns := [11], oldKey := [some 2], parentColumns := [21] }, 1, [],
    [(10, artist)], some others⟩

def live : Step := ⟨⟨3, 1, .upd, [(11, some 3), (10, some 1)]⟩, [(11, 1, 2, 1), (10, 0, 0, 1)], none⟩
def ended : Step := ⟨⟨3, 1, .upd, [(11, some 3), (10, none)]⟩, [(11, 1, 2, 1)], none⟩

theorem live_exact : heldConsequence (carrying (some 1) [(10, 0, 0, 1)]) albumChange = some live := by
  decide

theorem ended_exact : heldConsequence (carrying none []) albumChange = some ended := by decide

theorem views_differ : live ≠ ended := by decide

end Stale

end CovenMerge.Ownership.Classification.Overlapping
