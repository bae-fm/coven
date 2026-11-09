import CovenSequence.Cutoff

/-! Concrete membership, role and store-name admission. Other admin actions keep
their own authority policy; these changes require a current admin.
The candidate state must retain an admin. -/
namespace CovenSequence.Authority

variable {S : Sig}

def HasAdmin (L : Ledger S) : Prop := ∃ m, L.member m = true ∧ L.role m = .admin

def isAdmin (L : Ledger S) (d : S.Dev) : Bool :=
  match L.deviceOf d with
  | none => false
  | some m => !L.removed d && L.member m && decide (L.role m = .admin)

def requiresAdmin : Entry S → Bool
  | .admit .. | .recover .. | .removeMember .. | .changeRole .. | .renameStore .. => true
  | _ => false

def allowed (R : Rules S) (L : Ledger S) (d : S.Dev) (e : Entry S)
    (hasAdmin : Decidable (HasAdmin (applyEntry R L e)) := Classical.propDecidable _) : Bool :=
  letI := hasAdmin
  (if requiresAdmin e then isAdmin L d else R.authorized L d e) &&
    decide (HasAdmin (applyEntry R L e))

def withRoles (R : Rules S)
    (hasAdmin : (L : Ledger S) → Decidable (HasAdmin L) := fun _ => Classical.propDecidable _) : Rules S :=
  { R with authorized := fun L d e => allowed R L d e (hasAdmin (applyEntry R L e)) }

/-- Executable decisions preserve the abstract authority policy. -/
theorem withRoles_decision_eq (R : Rules S)
    (hasAdmin : (L : Ledger S) → Decidable (HasAdmin L)) :
    withRoles R hasAdmin = withRoles R := by
  have same : hasAdmin = (fun _ => Classical.propDecidable _) := funext fun _ => Subsingleton.elim _ _
  rw [same]

theorem same_effect (R : Rules S) (L : Ledger S) (e : Entry S) :
    applyEntry (withRoles R) L e = applyEntry R L e := by cases e <;> rfl

theorem allowed_authorized (R : Rules S) (L : Ledger S) (d : S.Dev) (e : Entry S)
    (changes : requiresAdmin e = true) (yes : allowed R L d e = true) : isAdmin L d = true := by
  simpa [allowed, changes] using (Bool.and_eq_true_iff.mp yes).1

theorem allowed_safe (R : Rules S) (L : Ledger S) (d : S.Dev) (e : Entry S)
    (yes : allowed R L d e = true) : HasAdmin (applyEntry R L e) := by
  simpa [allowed] using (Bool.and_eq_true_iff.mp yes).2

/-- Every actual ledger step preserves an admin, including refused entries,
key copies and data slots. -/
theorem step_has_admin (R : Rules S) (L : Ledger S) (k : Nat) (x : Slot S) (prior : HasAdmin L) :
    HasAdmin (ledgerStep (withRoles R) L k x) := by
  unfold ledgerStep
  change HasAdmin (if ledgerValid (withRoles R) L x then ledgerEffect (withRoles R) L k x else refuse L k)
  split
  · rename_i hv
    cases x with
    | admin d e =>
      have yes : allowed R L d e = true := by
        have h := hv
        simp only [ledgerValid, Bool.and_eq_true] at h
        exact h.1.2
      change HasAdmin (applyEntry (withRoles R) L e)
      rw [same_effect]
      exact allowed_safe R L d e yes
    | _ => exact prior
  · exact prior

/-- The initial ledger is the store's creation binding. Every applied prefix
after that creation retains at least one active member with the admin role. -/
theorem every_prefix_has_admin (R : Rules S) (seq : Nat → Slot S) (initial : HasAdmin R.ledger0) :
    ∀ k, HasAdmin (ledgerAt (withRoles R) seq k)
  | 0 => initial
  | k + 1 => step_has_admin R _ _ _ (every_prefix_has_admin R seq initial k)

/-- Authorization is checked against exactly the preceding sequence state,
not the author's old view and not a later snapshot's membership. -/
theorem applied_membership_authorized (R : Rules S) (seq : Nat → Slot S) (j : Nat)
    (d : S.Dev) (e : Entry S) (slot : seq j = .admin d e)
    (changes : requiresAdmin e = true) (applied : Applied (withRoles R) seq j) :
    isAdmin (ledgerAt (withRoles R) seq (j - 1)) d = true := by
  unfold Applied at applied
  rw [slot] at applied
  have yes := (Bool.and_eq_true_iff.mp (Bool.and_eq_true_iff.mp applied).1).2
  exact allowed_authorized R _ d e changes yes

/-- A rename is authorized by an admin in the state before its slot. -/
theorem applied_rename_authorized (R : Rules S) (seq : Nat → Slot S) (j : Nat)
    (d : S.Dev) (name : String) (slot : seq j = .admin d (.renameStore name))
    (applied : Applied (withRoles R) seq j) :
    isAdmin (ledgerAt (withRoles R) seq (j - 1)) d = true :=
  applied_membership_authorized R seq j d (.renameStore name) slot rfl applied

/-- A non-admin rename records refusal and preserves the current name. -/
theorem non_admin_rename_refused (R : Rules S) (L : Ledger S) (k : Nat)
    (d : S.Dev) (name : String) (no : isAdmin L d = false) :
    ledgerValid (withRoles R) L (.admin d (.renameStore name)) = false ∧
      (ledgerStep (withRoles R) L k (.admin d (.renameStore name))).name = L.name := by
  simp [ledgerValid, withRoles, allowed, requiresAdmin, no, ledgerStep, noteSlot, refuse]

/-- The current name is the creation name updated by the applied renames in slot order. -/
def nameAt (R : Rules S) (seq : Nat → Slot S) : Nat → String
  | 0 => R.ledger0.name
  | k + 1 => match seq (k + 1) with
    | .admin d (.renameStore name) =>
      if ledgerValid R (ledgerAt R seq k) (.admin d (.renameStore name)) then name else nameAt R seq k
    | _ => nameAt R seq k

theorem name_is_latest_applied (R : Rules S) (seq : Nat → Slot S) :
    ∀ k, (ledgerAt R seq k).name = nameAt R seq k
  | 0 => rfl
  | k + 1 => by
    rw [ledgerAt_succ]
    have prior := name_is_latest_applied R seq k
    cases h : seq (k + 1) with
    | admin d e =>
      cases e <;> simp [nameAt, h, ledgerStep, noteSlot, ledgerEffect, applyEntry, refuse, prior] <;>
        split <;> simp_all
    | _ => simp [nameAt, h, ledgerStep, noteSlot, ledgerEffect, refuse, prior] <;>
        split <;> simp_all

/-- Equal applied prefixes give equal names, including refused rename slots. -/
theorem name_prefix (R : Rules S) (seq₁ seq₂ : Nat → Slot S) (k : Nat)
    (same : ∀ i, 1 ≤ i → i ≤ k → seq₁ i = seq₂ i) :
    nameAt R seq₁ k = nameAt R seq₂ k := by
  rw [← name_is_latest_applied, ← name_is_latest_applied, ledger_prefix R seq₁ seq₂ k same]

/-- Removing or demoting the only admin is consumed as a refusal. -/
theorem last_admin_refused (R : Rules S) (L : Ledger S) (d : S.Dev) (e : Entry S)
    (noneLeft : ¬ HasAdmin (applyEntry R L e)) : ledgerValid (withRoles R) L (.admin d e) = false := by
  simp [ledgerValid, withRoles, allowed, noneLeft]

/-- Store resets require an admin. Circle resets retain the audience policy;
schema raises retain their policy because an ordinary member may migrate. -/
noncomputable def withResetRole (R : Rules S) (store : S.Aud) : Rules S :=
  { withRoles R with boundaryAllowed := fun L d A kind =>
      R.boundaryAllowed L d A kind &&
        match kind with
        | .raise _ => true
        | .reset => if A = store then isAdmin L d else true }

theorem store_reset_authorized (R : Rules S) (store : S.Aud) (seq : Nat → Slot S)
    (r : Nat) (d : S.Dev) (c : Nat) (p : S.Path)
    (slot : seq r = .boundary d store .reset c p)
    (applied : AppliedTo (withResetRole R store) seq store r) :
    isAdmin (ledgerAt (withResetRole R store) seq (r - 1)) d = true := by
  have valid := applied.2
  rw [slot] at valid
  simp only [audValid, withResetRole, Bool.and_eq_true, ite_true] at valid
  exact valid.1.1.1.1.1.2.2

/-! ## Circle actions and provider access (§10.1, §10.3)

The admin state `aux` holds circles and each member's provider access. These
rules decide who may make the admin actions that change them, against the state
before the entry's slot, as every other admin entry is judged. -/

/-- What an action other than membership, roles and renames names.
* `circle o`: the circle a circle action names (renaming it, changing its
  members, deleting or rotating it);
* `reads aux c m`: member `m` belongs to circle `c` in admin state `aux`;
* `access o`: the member whose provider access an access change sets, and
  whether the new access is an S3 key;
* `s3`: whether slot 1 recorded an S3 key, which fixes every member's kind of
  access. -/
structure Policy (S : Sig) where
  circle : S.Op → Option S.Aud
  reads : S.Aux → S.Aud → S.Mem → Bool
  access : S.Op → Option (S.Mem × Bool)
  s3 : Bool

/-- A circle action needs its author in the circle; an admin outside it can't.
Provider access is set by its member, or by an admin when it is an S3 key, and
only to the kind of access slot 1 recorded. -/
def policyAllows (P : Policy S) (L : Ledger S) (d : S.Dev) (o : S.Op) : Bool :=
  match L.deviceOf d with
  | none => false
  | some m =>
    (match P.circle o with
      | some c => P.reads L.aux c m
      | none => true) &&
    (match P.access o with
      | some (target, s3) =>
          (decide (m = target) || (isAdmin L d && s3)) && decide (s3 = P.s3)
      | none => true)

def withPolicy (R : Rules S) (P : Policy S) : Rules S :=
  { R with authorized := fun L d e => match e with
      | .other o => policyAllows P L d o && R.authorized L d e
      | _ => R.authorized L d e }

theorem policy_of_valid (R : Rules S) (P : Policy S) (L : Ledger S) (d : S.Dev) (o : S.Op)
    (valid : ledgerValid (withRoles (withPolicy R P)) L (.admin d (.other o)) = true) :
    policyAllows P L d o = true := by
  simp only [ledgerValid, withRoles, allowed, requiresAdmin, withPolicy, Bool.false_eq_true,
    ↓reduceIte, Bool.and_eq_true] at valid
  exact valid.1.2.1.1

theorem policy_of_applied (R : Rules S) (P : Policy S) (seq : Nat → Slot S) (j : Nat)
    (d : S.Dev) (o : S.Op) (slot : seq j = .admin d (.other o))
    (applied : Applied (withRoles (withPolicy R P)) seq j) :
    policyAllows P (ledgerAt (withRoles (withPolicy R P)) seq (j - 1)) d o = true := by
  unfold Applied at applied
  rw [slot] at applied
  exact policy_of_valid R P _ d o applied

/-- **Only a circle's members act on it.** An applied circle action was made by
a device whose member belonged to the circle in the state before its slot. -/
theorem applied_circle_action_by_member (R : Rules S) (P : Policy S) (seq : Nat → Slot S)
    (j : Nat) (d : S.Dev) (o : S.Op) (c : S.Aud) (slot : seq j = .admin d (.other o))
    (names : P.circle o = some c) (applied : Applied (withRoles (withPolicy R P)) seq j) :
    ∃ m, (ledgerAt (withRoles (withPolicy R P)) seq (j - 1)).deviceOf d = some m ∧
      P.reads (ledgerAt (withRoles (withPolicy R P)) seq (j - 1)).aux c m = true := by
  have yes := policy_of_applied R P seq j d o slot applied
  unfold policyAllows at yes
  split at yes
  · cases yes
  · rename_i m author
    simp only [names, Bool.and_eq_true] at yes
    exact ⟨m, author, yes.1⟩

/-- An admin outside the circle is refused like anyone else outside it. -/
theorem outside_circle_refused (R : Rules S) (P : Policy S) (L : Ledger S) (d : S.Dev)
    (o : S.Op) (c : S.Aud) (m : S.Mem) (names : P.circle o = some c) (author : L.deviceOf d = some m)
    (outside : P.reads L.aux c m = false) :
    ledgerValid (withRoles (withPolicy R P)) L (.admin d (.other o)) = false := by
  simp [ledgerValid, withRoles, allowed, requiresAdmin, withPolicy, policyAllows, author, names,
    outside]

/-- **Provider access is set by its own member, or by an admin for an S3
key,** and only to the kind of access slot 1 recorded. -/
theorem applied_access_authorized (R : Rules S) (P : Policy S) (seq : Nat → Slot S) (j : Nat)
    (d : S.Dev) (o : S.Op) (target : S.Mem) (s3 : Bool) (slot : seq j = .admin d (.other o))
    (sets : P.access o = some (target, s3)) (applied : Applied (withRoles (withPolicy R P)) seq j) :
    ((ledgerAt (withRoles (withPolicy R P)) seq (j - 1)).deviceOf d = some target ∨
      (isAdmin (ledgerAt (withRoles (withPolicy R P)) seq (j - 1)) d = true ∧ s3 = true)) ∧
      s3 = P.s3 := by
  have yes := policy_of_applied R P seq j d o slot applied
  unfold policyAllows at yes
  split at yes
  · cases yes
  · rename_i m author
    simp only [sets, Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq] at yes
    obtain ⟨-, own | admin, kind⟩ := yes
    · exact ⟨Or.inl (own ▸ author), kind⟩
    · exact ⟨Or.inr admin, kind⟩

/-- An access change of the other kind than slot 1's is refused, whoever makes it. -/
theorem other_kind_refused (R : Rules S) (P : Policy S) (L : Ledger S) (d : S.Dev)
    (o : S.Op) (target : S.Mem) (sets : P.access o = some (target, !P.s3)) :
    ledgerValid (withRoles (withPolicy R P)) L (.admin d (.other o)) = false := by
  cases author : L.deviceOf d <;> cases h : P.s3 <;>
    simp [ledgerValid, withRoles, allowed, requiresAdmin, withPolicy, policyAllows, author, sets, h]

end CovenSequence.Authority
