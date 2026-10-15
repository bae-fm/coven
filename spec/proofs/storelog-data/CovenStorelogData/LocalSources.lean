import CovenStorelogData.RetentionSafety

/-! §14.7, §16.5 and §16.6: when this device forgets the record of a
user-provided original (`_coven_user_files`), as a `localSource` retention
input.

A circle deletion hides its rows without deleting them; they return if the
deletion drops. A row delete is a write, whose admission can still change
(§10) until its window verdict is final. Physical cleanup of a hidden row
waits for the deletion's finality (§14.7). Entries and writes share `kept`
and `final`: an id names either a circle deletion entry or a deleting write.

The stated rule keeps the record while the row is current or could become
current again, and releases it once that can no longer happen. The rule Rust
implements (`FileWrite::retain_rows`) keeps the record while a merge row
names the original, hidden or not, and forgets it as soon as a deleting
generation lands; nothing physically removes a deleted circle's rows. -/

namespace CovenStorelogData.RetentionSafety

/-- Where the row naming an original stands on this device. -/
inductive RowState where
  | live
  /-- A circle deletion entry hides it. -/
  | hidden (deletion : Nat)
  /-- A write deleted it. -/
  | deleted (write : Nat)
  /-- Physically removed after its circle's deletion became final. -/
  | cleaned
  deriving DecidableEq, Repr

/-- The stated rule: the record is needed while its row is, or can again be,
current. -/
def sourceNeed : RowState → Need
  | .live => .always
  | .hidden deletion => .not (.kept deletion)
  | .deleted write => .not (.kept write)
  | .cleaned => .never

/-- The rule Rust implements. -/
def implementedNeed : RowState → Need
  | .live | .hidden _ => .always
  | .deleted _ | .cleaned => .never

def localSource (id : Nat) (need : Need) : Input := ⟨id, .localSource, need, []⟩

/-- §14.7: a hidden row is physically removed only once its deletion is kept
and final. -/
def cleanup (kept final : Nat → Bool) : RowState → RowState
  | .hidden deletion => if kept deletion && final deletion then .cleaned else .hidden deletion
  | row => row

/-- No source is released while its row could come back: whatever a later
replay or rejudging keeps (`Stable` on final ids), a record whose row that
later state needs was retained. -/
theorem source_kept_while_row_returns (before after final : Nat → Bool)
    (stable : Stable before after final) (inputs : List Input) (id : Nat) (row : RowState)
    (present : localSource id (sourceNeed row) ∈ inputs)
    (returns : (sourceNeed row).eval after = true) :
    localSource id (sourceNeed row) ∈ retain before final inputs :=
  retention_preserves_possible_replay before after final stable inputs _ present returns

/-- In particular a hidden row's record stays until its deletion is final, and
a deleted row's until the deleting write's verdict is. -/
theorem hidden_kept_until_final (kept final : Nat → Bool) (id deletion : Nat)
    (open_ : final deletion = false) :
    mayDelete kept final (localSource id (sourceNeed (.hidden deletion))) = false := by
  simp [mayDelete, conditions, localSource, sourceNeed, Need.entries, open_]

theorem deleted_kept_until_final (kept final : Nat → Bool) (id write : Nat)
    (open_ : final write = false) :
    mayDelete kept final (localSource id (sourceNeed (.deleted write))) = false := by
  simp [mayDelete, conditions, localSource, sourceNeed, Need.entries, open_]

/-- None is kept forever: once the deciding entry or write is final and keeps
the row from returning, the record is released, and a hidden row may be
physically removed. -/
theorem hidden_released_when_final (kept final : Nat → Bool) (id deletion : Nat)
    (keptDeletion : kept deletion = true) (finalDeletion : final deletion = true) :
    mayDelete kept final (localSource id (sourceNeed (.hidden deletion))) = true ∧
    cleanup kept final (.hidden deletion) = .cleaned ∧
    mayDelete kept final (localSource id (sourceNeed .cleaned)) = true := by
  simp [mayDelete, conditions, localSource, sourceNeed, Need.entries, Need.eval, cleanup,
    keptDeletion, finalDeletion]

theorem deleted_released_when_final (kept final : Nat → Bool) (id write : Nat)
    (keptWrite : kept write = true) (finalWrite : final write = true) :
    mayDelete kept final (localSource id (sourceNeed (.deleted write))) = true := by
  simp [mayDelete, conditions, localSource, sourceNeed, Need.entries, Need.eval,
    keptWrite, finalWrite]

/-- Bounded: once every dependency is final, exactly the records of rows that
are current remain (`bounded_after_finality`). -/
theorem sources_bounded (kept final : Nat → Bool) (rows : List (Nat × RowState))
    (ready : ∀ p ∈ rows, ∀ e ∈ (sourceNeed p.2).entries, final e = true) :
    retain kept final (rows.map fun p => localSource p.1 (sourceNeed p.2)) =
      (rows.map fun p => localSource p.1 (sourceNeed p.2)).filter
        (fun input => input.needed.eval kept) := by
  apply bounded_after_finality
  intro input member
  obtain ⟨p, hp, rfl⟩ := List.mem_map.mp member
  simp only [conditions, localSource, List.append_nil, List.all_map, List.all_eq_true]
  intro e he
  simp [Function.comp, ready p hp e he]

/-- The implemented rule keeps a hidden row's record forever: whatever the
deletion's fate, kept and final included, it is never released, and no
cleanup ever moves the row out of `hidden`. -/
theorem implemented_keeps_hidden_forever (kept final : Nat → Bool) (id deletion : Nat) :
    mayDelete kept final (localSource id (implementedNeed (.hidden deletion))) = false := by
  simp [mayDelete, localSource, implementedNeed, Need.eval]

/-- The implemented rule releases a deleted row's record while the deleting
write's verdict can still change: here the write is kept but not final, and
a later rejudging that excludes it (a retirement window, §10) brings the row
back, whose record is already gone. -/
theorem implemented_releases_returning_row :
    let before : Nat → Bool := fun _ => true
    let after : Nat → Bool := fun _ => false
    let final : Nat → Bool := fun _ => false
    Stable before after final ∧
    mayDelete before final (localSource 0 (implementedNeed (.deleted 1))) = true ∧
    (sourceNeed (.deleted 1)).eval after = true ∧
    mayDelete before final (localSource 0 (sourceNeed (.deleted 1))) = false := by
  refine ⟨fun _ h => by simp at h, ?_⟩
  decide

end CovenStorelogData.RetentionSafety
