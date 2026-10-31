/-!
# Selecting an explicit reload's snapshots

An explicit reload chooses, for each readable audience, the latest applicable
announcement (§16.2). Its journal fixes that selection and the target; when
retention deletes what it selected, the reload ends with the error and the
next call selects again (§16.5).
-/
namespace CovenSequence.Reload

/-- The announcement metadata an explicit reload selects by. -/
structure Selection (Audience : Type) where
  slot : Nat
  audience : Audience
  covers : Nat
  deriving DecidableEq

/-- Explicit reload chooses the latest applicable announcement for an audience.
The candidates have already passed the schema/reset applicability check. Floor
bootstrap has its own selection rule. -/
def newest : List (Selection A) → Option (Selection A)
  | [] => none
  | candidate :: rest =>
    match newest rest with
    | none => some candidate
    | some selected => some (if selected.slot < candidate.slot then candidate else selected)

theorem newest_mem (candidates : List (Selection A)) (selected : Selection A)
    (h : newest candidates = some selected) : selected ∈ candidates := by
  induction candidates generalizing selected with
  | nil => cases h
  | cons candidate rest ih =>
    simp only [newest] at h
    split at h
    · cases h; exact List.mem_cons_self
    · rename_i previous hp
      split at h
      · cases h; exact List.mem_cons_self
      · cases h; exact List.mem_cons_of_mem _ (ih _ hp)

/-- No applicable announcement is later than the selected one. -/
theorem newest_latest (candidates : List (Selection A)) (selected : Selection A)
    (h : newest candidates = some selected) :
    ∀ candidate ∈ candidates, candidate.slot ≤ selected.slot := by
  induction candidates generalizing selected with
  | nil => cases h
  | cons candidate rest ih =>
    simp only [newest] at h
    split at h
    · rename_i empty
      have hr : rest = [] := by
        cases rest with
        | nil => rfl
        | cons x xs => simp only [newest] at empty; split at empty <;> cases empty
      cases h; simp [hr]
    · rename_i previous hp
      have bound := ih _ hp
      split at h
      · rename_i later
        cases h
        intro x hx
        rcases List.mem_cons.mp hx with rfl | hx
        · exact Nat.le_refl _
        · exact Nat.le_trans (bound x hx) (Nat.le_of_lt later)
      · rename_i later
        cases h
        intro x hx
        rcases List.mem_cons.mp hx with rfl | hx
        · omega
        · exact bound x hx

end CovenSequence.Reload
