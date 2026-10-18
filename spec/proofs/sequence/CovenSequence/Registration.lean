import CovenSequence.Authority

/-! Registration is authored by the fresh installation, authenticated outside
the model with its member's current key. Registration persists after removal,
so neither removal nor a later membership change makes the id reusable. -/
namespace CovenSequence

variable {S : Sig} (R : Rules S)

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
      | addDevice target m =>
        simp only [ledgerEffect, applyEntry] at fresh ⊢
        split at fresh
        · contradiction
        · exact prior d fresh
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
    (authorized : R.authorized L d (.addDevice d m) = true) :
    ledgerValid R L (.admin d (.addDevice d m)) = true := by
  simp [ledgerValid, adminAuthor, structural, fresh, member, authorized]

theorem self_registration_applied (L : Ledger S) (k : Nat) (d : S.Dev) (m : S.Mem)
    (fresh : L.deviceOf d = none) (member : L.member m = true)
    (authorized : R.authorized L d (.addDevice d m) = true) :
    (ledgerStep R L k (.admin d (.addDevice d m))).deviceOf d = some m := by
  simp [ledgerStep, self_registration_valid R L d m fresh member authorized,
    ledgerEffect, applyEntry]

/-- Every authority policy must respect the installation's own authorship. -/
theorem registration_self_authored (L : Ledger S) (a d : S.Dev) (m : S.Mem)
    (valid : ledgerValid R L (.admin a (.addDevice d m)) = true) : a = d := by
  simp only [ledgerValid, structural, Bool.and_eq_true, decide_eq_true_eq] at valid
  exact valid.2.1.1

/-- A binding from creation or an applied registration cannot be registered
again, whether its device or member is active or removed. -/
theorem registered_device_refused (L : Ledger S) (a d : S.Dev) (m old : S.Mem)
    (registered : L.deviceOf d = some old) :
    ledgerValid R L (.admin a (.addDevice d m)) = false := by
  simp [ledgerValid, structural, registered]

theorem initial_registration_never_repeated (seq : Nat → Slot S)
    (a d : S.Dev) (m old : S.Mem) (registered : R.ledger0.deviceOf d = some old) (k : Nat) :
    ledgerValid R (ledgerAt R seq k) (.admin a (.addDevice d m)) = false := by
  exact registered_device_refused R _ a d m old
    (deviceOf_stays R seq 0 registered k (Nat.zero_le _))

/-- After an applied registration, every later registration of that id is
refused, including attempts naming a different member or author. -/
theorem registration_never_repeated (seq : Nat → Slot S) (i : Nat) (positive : 1 ≤ i)
    (a d : S.Dev) (m : S.Mem) (slot : seq i = .admin a (.addDevice d m))
    (applied : Applied R seq i) (k : Nat) (later : i ≤ k) (a' : S.Dev) (m' : S.Mem) :
    ledgerValid R (ledgerAt R seq k) (.admin a' (.addDevice d m')) = false := by
  have registered : (ledgerAt R seq i).deviceOf d = some m := by
    rw [applied_ledger R seq positive applied, slot]
    simp [ledgerEffect, applyEntry]
  exact registered_device_refused R _ a' d m' m
    (deviceOf_stays R seq i registered k later)

/-- Two applied registrations of one device id occupy the same slot. -/
theorem registration_at_most_once (seq : Nat → Slot S) (i j : Nat)
    (ipos : 1 ≤ i) (jpos : 1 ≤ j) (a b d : S.Dev) (m n : S.Mem)
    (islot : seq i = .admin a (.addDevice d m))
    (jslot : seq j = .admin b (.addDevice d n))
    (iapplied : Applied R seq i) (japplied : Applied R seq j) : i = j := by
  rcases Nat.lt_trichotomy i j with before | same | after
  · have refused := registration_never_repeated R seq i ipos a d m islot iapplied
      (j - 1) (by omega) b n
    have valid := japplied
    unfold Applied at valid
    rw [jslot, refused] at valid
    contradiction
  · exact same
  · have refused := registration_never_repeated R seq j jpos b d n jslot japplied
      (i - 1) (by omega) a m
    have valid := iapplied
    unfold Applied at valid
    rw [islot, refused] at valid
    contradiction

end CovenSequence
