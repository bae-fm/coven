import CovenSequence.Retention

/-!
# Reselecting a deleted reload snapshot

The journal fixes the target and the selected announcements. A missing body
can change that selection only to the same audience's later floor announcement
with at least its coverage. The common target includes the old target, the
floor's authentication prefix (`at`), and every reselected start's coverage.
As in `Reader`, floor bodies here are ordinary snapshots.
-/
namespace CovenSequence.Reload

/-- The announcement metadata inspected when a selected body is missing. -/
structure Selection (Audience : Type) where
  slot : Nat
  audience : Audience
  covers : Nat
  deriving DecidableEq

/-- Explicit reload chooses the latest applicable announcement for an audience.
The candidates have already passed the schema/reset applicability check. Floor
bootstrap and deletion-driven reselection have their own selection rules. -/
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

def replaces [DecidableEq A] (old next : Selection A) : Bool :=
  decide (old.slot < next.slot ∧ next.audience = old.audience ∧ old.covers ≤ next.covers)

/-- The already chosen audiences supply their start positions, including any
audience kept at its current committed prefix. -/
def target (old floorAt : Nat) (covers : List Nat) : Nat :=
  covers.foldr max (max floorAt old)

/-- Called only after deletion requires reselection. `none` means a suffix
fell below the floor; `some` identifies the body whose NotFound needs a
replacement. The floor includes audiences no longer readable at the target. -/
def reselect [DecidableEq A] (old floorAt : Nat) (covers : List Nat)
    (missing : Option (Selection A)) (floor : List (Selection A)) : Option Nat :=
  match missing with
  | none => some (target old floorAt covers)
  | some selected =>
    if floor.any (replaces selected) then some (target old floorAt covers) else none

theorem target_bounds (old floorAt : Nat) (covers : List Nat) :
    old ≤ target old floorAt covers ∧ floorAt ≤ target old floorAt covers ∧
      ∀ c ∈ covers, c ≤ target old floorAt covers := by
  induction covers with
  | nil => simp [target]; omega
  | cons c cs ih =>
    simp only [target, List.foldr_cons] at *
    refine ⟨by omega, by omega, ?_⟩
    intro n hn
    rcases List.mem_cons.mp hn with rfl | hn
    · exact Nat.le_max_left _ _
    · exact Nat.le_trans (ih.2.2 n hn) (Nat.le_max_right _ _)

theorem target_le (old floorAt head : Nat) (covers : List Nat)
    (ho : old ≤ head) (ha : floorAt ≤ head) (hc : ∀ c ∈ covers, c ≤ head) :
    target old floorAt covers ≤ head := by
  induction covers with
  | nil => simpa [target] using (show max floorAt old ≤ head by omega)
  | cons c cs ih =>
    have h := hc c (by simp)
    have rest := ih (fun n hn => hc n (by simp [hn]))
    simpa only [target, List.foldr_cons] using (show max c (target old floorAt cs) ≤ head by omega)

theorem reselect_target [DecidableEq A] (old floorAt : Nat) (covers : List Nat)
    (missing : Option (Selection A)) (floor : List (Selection A)) (next : Nat)
    (h : reselect old floorAt covers missing floor = some next) :
    next = target old floorAt covers := by
  cases missing with
  | none => exact (Option.some.inj h).symm
  | some selected =>
    simp only [reselect] at h
    split at h
    · exact (Option.some.inj h).symm
    · cases h

/-- Both deletion triggers preserve the old target and include the floor's
authentication prefix and every selected start. -/
theorem reselect_monotone [DecidableEq A] (old floorAt : Nat) (covers : List Nat)
    (missing : Option (Selection A)) (floor : List (Selection A)) (next : Nat)
    (h : reselect old floorAt covers missing floor = some next) :
    old ≤ next ∧ floorAt ≤ next ∧ ∀ c ∈ covers, c ≤ next := by
  rw [reselect_target old floorAt covers missing floor next h]
  exact target_bounds old floorAt covers

/-- Missing-body reselection supplies a later announcement of the same
audience covering at least as much, including an audience that has departed. -/
theorem reselect_replacement [DecidableEq A] (old floorAt : Nat) (covers : List Nat)
    (selected : Selection A) (floor : List (Selection A)) (next : Nat)
    (h : reselect old floorAt covers (some selected) floor = some next) :
    ∃ replacement ∈ floor, selected.slot < replacement.slot ∧
      replacement.audience = selected.audience ∧ selected.covers ≤ replacement.covers := by
  simp only [reselect] at h
  split at h
  · rename_i accepted
    simpa [List.any_eq_true, replaces] using accepted
  · cases h

/-- For valid ordinary announcements, being later already implies at least
as much coverage. This does not imply that a floor announcement is later
than a reload's selection. -/
theorem later_announcement_covers {S : Sig} (R : Rules S) (seq : Nat → Slot S) (A : S.Aud)
    {a b c d : Nat} {au bu : S.Dev} {p q : S.Path}
    (positive : 1 ≤ a) (later : a < b)
    (old : seq a = .snapshot au A c p) (new : seq b = .snapshot bu A d q)
    (oldApplied : AppliedTo R seq A a) (newApplied : AppliedTo R seq A b) : c ≤ d := by
  have first := announced_maxCover R seq A positive old oldApplied
  have mono := maxCover_mono R seq A a (b - 1) (by omega)
  have valid := newApplied.2
  rw [new] at valid
  simp only [audValid, Bool.and_eq_true, decide_eq_true_eq] at valid
  omega

/-- Loading the actual floor body and applying retained slots to the
reselected common target yields the full-history state at exactly that target.
Later slots through `st.head` do not extend the run. -/
theorem reselect_preserves_prefix {S : Sig} (R : Rules S)
    {st : Store S} {seq : Nat → Slot S} (g : Good R st seq) (A : S.Aud)
    (old floorAt : Nat) (covers : List Nat) (ho : old ≤ st.head) (ha : floorAt ≤ st.head)
    (hc : ∀ c ∈ covers, c ≤ st.head) (selected : st.floor.covers A ∈ covers)
    (missing : Option (Selection S.Aud)) (floor : List (Selection S.Aud)) (next : Nat)
    (accepted : reselect old floorAt covers missing floor = some next)
    (L : Ledger S) (P : AudLedger S) (rows : S.Rows)
    (body : st.body (st.floor.path A) = some (L, P, rows)) :
    ∃ d', ReadSteps R A true st
      ⟨st.floor.covers A, L, P, rows, fun _ => none⟩ d' ∧
      d'.pos = next ∧ (d'.ledger, d'.aud, d'.rows) = stateAt R seq A next := by
  rw [reselect_target old floorAt covers missing floor next accepted]
  rw [g.floor_body A] at body
  have fields := Option.some.inj body
  simp only [stateAt, Prod.mk.injEq] at fields
  obtain ⟨rfl, rfl, rfl⟩ := fields
  have bound := (target_bounds old floorAt covers).2.2 _ selected
  obtain ⟨d', run, pos, ok⟩ := reaches_target R g A (target old floorAt covers)
    (target_le old floorAt st.head covers ho ha hc) bound
    ⟨st.floor.covers A, ledgerAt R seq (st.floor.covers A),
      audAt R seq A (st.floor.covers A), rowsAt R seq A (st.floor.covers A), fun _ => none⟩
    ⟨rfl, rfl, rfl, g.covers_le A, fun _ _ h => by cases h⟩ bound
  refine ⟨d', run, pos, ?_⟩
  rw [ok.ledger, ok.aud, ok.rows, pos]
  rfl

end CovenSequence.Reload
