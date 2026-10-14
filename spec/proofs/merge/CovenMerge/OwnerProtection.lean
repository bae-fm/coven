import CovenMerge.Ownership

/-! Audience moves retain their separate destination-readability requirement.
This check reads the merged descendant graph and membership. -/
namespace CovenMerge.Ownership

variable {Row K Member Op : Type} [DecidableEq Row] [DecidableEq K] [DecidableEq Member]

/-- Following the declared audience parent, not arbitrary foreign keys. The
closure includes the root and concurrent descendants in the merged graph. -/
def carried (rows : List Row) (parent : Row → Option Row) (root : Row) : Row → Bool :=
  close rows (fun D r => (parent r).any D) (fun r => decide (r = root))

theorem carried_root (rows : List Row) (parent : Row → Option Row) (root : Row) :
    carried rows parent root root = true :=
  star_grow (close_star rows (fun D r => (parent r).any D) (fun r => decide (r = root))) root (by simp)

theorem carried_child (rows : List Row) (parent : Row → Option Row) (root child p : Row)
    (mem : child ∈ rows) (link : parent child = some p)
    (ancestor : carried rows parent root p = true) : carried rows parent root child = true := by
  exact normal_closed (close_normal rows (fun D r => (parent r).any D) (fun r => decide (r = root)))
    child mem (by simpa [FiresP, link, carried] using ancestor)

private theorem parent_monotone (parent : Row → Option Row) :
    MonotoneRules (FiresP (fun D r => (parent r).any D)) := by
  intro D E r sub fired
  cases hp : parent r with
  | none => simp [FiresP, hp] at fired
  | some p => simpa [FiresP, hp] using sub p (by simpa [FiresP, hp] using fired)

theorem carried_order_independent (a b : List Row) (parent : Row → Option Row) (root : Row)
    (same : ∀ r, r ∈ a ↔ r ∈ b) : carried a parent root = carried b parent root := by
  have least (rows : List Row) := normal_least (parent_monotone parent)
    (close_star rows (fun D r => (parent r).any D) (fun r => decide (r = root)))
    (close_normal rows (fun D r => (parent r).any D) (fun r => decide (r = root)))
  have ab : ∀ r, carried a parent root r = true → carried b parent root r = true :=
    (least a).2.2 _ (fun r hr hf => (least b).1 r ((same r).mp hr) hf) (least b).2.1
  have ba : ∀ r, carried b parent root r = true → carried a parent root r = true :=
    (least b).2.2 _ (fun r hr hf => (least a).1 r ((same r).mpr hr) hf) (least a).2.1
  funext r
  exact Bool.eq_iff_iff.mpr ⟨ab r, ba r⟩

structure MoveState (Row Member Audience : Type) where
  rows : List Row
  present : Row → Bool
  owner : Row → Option Member
  parent : Row → Option Row
  canRead : Member → Audience → Bool

variable {Audience : Type}

def moveBlocker (s : MoveState Row Member Audience) (root : Row) (destination : Audience) : Option Row :=
  s.rows.find? fun r => s.present r && carried s.rows s.parent root r &&
    (s.owner r).any (fun m => !s.canRead m destination)

def moveDenied (s : MoveState Row Member Audience) (root : Row) (destination : Audience) : Bool :=
  (moveBlocker s root destination).isSome

def moveReason {W : Type} (s : MoveState Row Member Audience) (root : Row) (destination : Audience) :
    Option (Cause W Member Row) :=
  (moveBlocker s root destination).bind fun r => (s.owner r).map (fun member => .move r member)

theorem move_reason_complete {W : Type} (s : MoveState Row Member Audience)
    (root : Row) (destination : Audience) (bad : moveDenied s root destination = true) :
    ∃ r member, moveReason (W := W) s root destination = some (.move r member) ∧
      r ∈ s.rows ∧ s.present r = true ∧ carried s.rows s.parent root r = true ∧
      s.owner r = some member ∧ s.canRead member destination = false := by
  cases blocker : moveBlocker s root destination with
  | none => simp [moveDenied, blocker] at bad
  | some r =>
    have found := List.find?_some (show s.rows.find? _ = some r from blocker)
    have mem := List.mem_of_find?_eq_some (show s.rows.find? _ = some r from blocker)
    simp only [Bool.and_eq_true, Option.any_eq_true, Bool.not_eq_true'] at found
    obtain ⟨⟨present, affected⟩, member, owned, unreadable⟩ := found
    exact ⟨r, member, by simp [moveReason, blocker, owned], mem, present, affected, owned, unreadable⟩

theorem move_order_independent (s : MoveState Row Member Audience) (rows : List Row)
    (same : ∀ r, r ∈ s.rows ↔ r ∈ rows) (root : Row) (destination : Audience) :
    moveDenied s root destination = moveDenied { s with rows := rows } root destination := by
  have carry := carried_order_independent s.rows rows s.parent root same
  apply Bool.eq_iff_iff.mpr
  simp only [moveDenied, moveBlocker, List.find?_isSome]
  constructor
  · rintro ⟨r, mem, h⟩
    exact ⟨r, (same r).mp mem, by simpa only [carry] using h⟩
  · rintro ⟨r, mem, h⟩
    exact ⟨r, (same r).mpr mem, by simpa only [carry] using h⟩

theorem unread_owner_blocks_move (s : MoveState Row Member Audience) (root r : Row)
    (destination : Audience) (m : Member) (mem : r ∈ s.rows) (present : s.present r = true)
    (carriedRow : carried s.rows s.parent root r = true) (owned : s.owner r = some m)
    (unreadable : s.canRead m destination = false) : moveDenied s root destination = true := by
  simp only [moveDenied, moveBlocker, List.find?_isSome]
  exact ⟨r, mem, by simp [present, carriedRow, owned, unreadable]⟩

theorem move_safe (s : MoveState Row Member Audience) (root : Row) (destination : Audience)
    (safe : moveDenied s root destination = false) (r : Row) (m : Member)
    (mem : r ∈ s.rows) (present : s.present r = true)
    (carriedRow : carried s.rows s.parent root r = true) (owned : s.owner r = some m) :
    s.canRead m destination = true := by
  cases h : s.canRead m destination
  · have := unread_owner_blocks_move s root r destination m mem present carriedRow owned h
    simp [safe] at this
  · rfl

end CovenMerge.Ownership
