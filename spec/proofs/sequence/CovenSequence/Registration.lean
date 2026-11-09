import CovenSequence.Authority

/-! Registration is authored by the fresh installation, authenticated outside
the model with its member's current key, except a joining person's first
installation, which the admission admitting or recovering them registers
(coven.md §13.2). Registration persists after removal, so neither removal nor
a later membership change makes the id reusable. -/
namespace CovenSequence

variable {S : Sig} (R : Rules S)

/-- The installation an entry registers: an installation's own registration,
or an admission's named device. -/
def registers : Entry S → Option S.Dev
  | .addDevice d _ _ | .admit _ _ d | .recover _ d => some d
  | _ => none

/-- Removal only marks registered ids. -/
theorem step_unregistered_unremoved (L : Ledger S) (k : Nat) (x : Slot S)
    (prior : ∀ d, L.deviceOf d = none → L.removed d = false) :
    ∀ d, (ledgerStep R L k x).deviceOf d = none →
      (ledgerStep R L k x).removed d = false := by
  intro d fresh
  unfold ledgerStep at fresh ⊢
  simp only [noteSlot_deviceOf, noteSlot_removed] at fresh ⊢
  by_cases valid : ledgerValid R L x = true
  · simp only [valid, ite_true] at fresh ⊢
    cases x with
    | admin a e =>
      cases e with
      | addDevice target m r =>
        simp only [ledgerEffect, applyEntry] at fresh ⊢
        split at fresh
        · contradiction
        · rw [prior d fresh, Bool.false_or, decide_eq_false_iff_not]
          rintro rfl
          simp [ledgerValid, structural, fresh] at valid
      | removeDevice target =>
        change L.deviceOf d = none at fresh
        simp only [ledgerEffect, applyEntry]
        split
        · rename_i same
          subst target
          simp [ledgerValid, structural, fresh] at valid
        · exact prior d fresh
      | removeMember m =>
        change L.deviceOf d = none at fresh
        simp [ledgerEffect, applyEntry, fresh, prior d fresh]
      | admit m r target =>
        simp only [ledgerEffect, applyEntry] at fresh ⊢
        split at fresh
        · contradiction
        · exact prior d fresh
      | recover m target =>
        simp only [ledgerEffect, applyEntry] at fresh ⊢
        split at fresh
        · contradiction
        · exact prior d fresh
      | _ => exact prior d fresh
    | _ => exact prior d fresh
  · simp [valid, refuse] at fresh ⊢
    exact prior d fresh

theorem unregistered_unremoved (seq : Nat → Slot S)
    (initial : ∀ d, R.ledger0.deviceOf d = none → R.ledger0.removed d = false) :
    ∀ k d, (ledgerAt R seq k).deviceOf d = none → (ledgerAt R seq k).removed d = false
  | 0 => initial
  | k + 1 => step_unregistered_unremoved R _ _ _ (unregistered_unremoved seq initial k)

/-- Registration needs no already-active device of the member. -/
theorem self_registration_valid (L : Ledger S) (d : S.Dev) (m : S.Mem)
    (fresh : L.deviceOf d = none) (member : L.member m = true)
    (authorized : R.authorized L d (.addDevice d m none) = true) :
    ledgerValid R L (.admin d (.addDevice d m none)) = true := by
  simp [ledgerValid, adminAuthor, structural, fresh, member, authorized]

theorem self_registration_applied (L : Ledger S) (k : Nat) (d : S.Dev) (m : S.Mem)
    (fresh : L.deviceOf d = none) (member : L.member m = true)
    (authorized : R.authorized L d (.addDevice d m none) = true) :
    (ledgerStep R L k (.admin d (.addDevice d m none))).deviceOf d = some m := by
  simp [ledgerStep, self_registration_valid R L d m fresh member authorized,
    ledgerEffect, applyEntry]

/-- **An applied admission registers its named device for its member.** The
joining person's first installation is registered by the admission that
admits or recovers them, written by an admin's device, not by itself. -/
theorem admission_registers (seq : Nat → Slot S) (i : Nat) (positive : 1 ≤ i)
    (a d : S.Dev) (e : Entry S) (m : S.Mem) (role : Role)
    (admission : e = .admit m role d ∨ e = .recover m d)
    (slot : seq i = .admin a e) (applied : Applied R seq i) :
    (ledgerAt R seq i).deviceOf d = some m ∧ a ≠ d := by
  have valid := applied
  unfold Applied at valid
  rw [slot] at valid
  rcases admission with rfl | rfl
  · refine ⟨by
      rw [applied_ledger R seq positive applied, slot]; simp [ledgerEffect, applyEntry], ?_⟩
    simp only [ledgerValid, structural, Bool.and_eq_true, Bool.not_eq_true',
      decide_eq_false_iff_not] at valid
    exact valid.2.2
  · refine ⟨by
      rw [applied_ledger R seq positive applied, slot]; simp [ledgerEffect, applyEntry], ?_⟩
    simp only [ledgerValid, structural, Bool.and_eq_true, Bool.not_eq_true',
      decide_eq_false_iff_not] at valid
    exact valid.2.2

/-- Every authority policy must respect the installation's own authorship. -/
theorem registration_self_authored (L : Ledger S) (a d : S.Dev) (m : S.Mem) (r : Option S.Dev)
    (valid : ledgerValid R L (.admin a (.addDevice d m r)) = true) : a = d := by
  simp only [ledgerValid, structural, Bool.and_eq_true, decide_eq_true_eq] at valid
  exact valid.2.1.1.1

/-- A binding from creation, an applied registration or an applied admission
cannot be registered again, by an installation itself or by an admission,
whether its device or member is active or removed. -/
theorem registered_device_refused (L : Ledger S) (a d : S.Dev) (e : Entry S) (old : S.Mem)
    (names : registers e = some d) (registered : L.deviceOf d = some old) :
    ledgerValid R L (.admin a e) = false := by
  cases e <;> simp only [registers, Option.some.injEq, reduceCtorEq] at names <;> subst names <;>
    simp [ledgerValid, structural, registered]

theorem initial_registration_never_repeated (seq : Nat → Slot S)
    (a d : S.Dev) (e : Entry S) (old : S.Mem) (names : registers e = some d)
    (registered : R.ledger0.deviceOf d = some old) (k : Nat) :
    ledgerValid R (ledgerAt R seq k) (.admin a e) = false := by
  exact registered_device_refused R _ a d e old names
    (deviceOf_stays R seq 0 registered k (Nat.zero_le _))

/-- What an applied entry registering `d` binds it to. -/
theorem registered_by (seq : Nat → Slot S) (i : Nat) (positive : 1 ≤ i)
    (a d : S.Dev) (e : Entry S) (names : registers e = some d) (slot : seq i = .admin a e)
    (applied : Applied R seq i) : ∃ m, (ledgerAt R seq i).deviceOf d = some m := by
  rw [applied_ledger R seq positive applied, slot]
  cases e <;> simp only [registers, Option.some.injEq, reduceCtorEq] at names <;> subst names <;>
    simp [ledgerEffect, applyEntry]

/-- After an applied registration or admission, every later entry
registering that id is refused, including attempts naming a different
member or author, and admissions. -/
theorem registration_never_repeated (seq : Nat → Slot S) (i : Nat) (positive : 1 ≤ i)
    (a d : S.Dev) (e : Entry S) (names : registers e = some d) (slot : seq i = .admin a e)
    (applied : Applied R seq i) (k : Nat) (later : i ≤ k) (a' : S.Dev) (e' : Entry S)
    (names' : registers e' = some d) :
    ledgerValid R (ledgerAt R seq k) (.admin a' e') = false := by
  obtain ⟨m, registered⟩ := registered_by R seq i positive a d e names slot applied
  exact registered_device_refused R _ a' d e' m names'
    (deviceOf_stays R seq i registered k later)

/-- Two applied entries registering one device id, registrations or
admissions, occupy the same slot. -/
theorem registration_at_most_once (seq : Nat → Slot S) (i j : Nat)
    (ipos : 1 ≤ i) (jpos : 1 ≤ j) (a b d : S.Dev) (e e' : Entry S)
    (inames : registers e = some d) (jnames : registers e' = some d)
    (islot : seq i = .admin a e) (jslot : seq j = .admin b e')
    (iapplied : Applied R seq i) (japplied : Applied R seq j) : i = j := by
  rcases Nat.lt_trichotomy i j with before | same | after
  · have refused := registration_never_repeated R seq i ipos a d e inames islot iapplied
      (j - 1) (by omega) b e' jnames
    have valid := japplied
    unfold Applied at valid
    rw [jslot, refused] at valid
    contradiction
  · exact same
  · have refused := registration_never_repeated R seq j jpos b d e' jnames jslot japplied
      (i - 1) (by omega) a e inames
    have valid := iapplied
    unfold Applied at valid
    rw [islot, refused] at valid
    contradiction

end CovenSequence
