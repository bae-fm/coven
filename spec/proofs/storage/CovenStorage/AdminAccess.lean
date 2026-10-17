/-! §20.10's admin access for one S3 key replacement (§14.3): the app supplies it
to the operation, which holds it in memory only. Does a key get made with
admin access the app did not supply again after a failure, and how long
does a replacement hold it? Each mint call names its principal afresh
(KeyListing), so a made key is only counted here, never deleted. -/

namespace CovenStorage.AdminAccess

/-- The journal row: `key` is the made key, `checked` its member-side check. -/
structure Row where
  key : Option Nat
  checked : Bool
  deriving DecidableEq, Repr

/-- `admin` is the operation's admin access, in memory; `made` lists every
key a mint call made, including one whose reply was lost. -/
structure World where
  row : Option Row
  admin : Bool
  made : List Nat
  next : Nat
  deriving DecidableEq, Repr

inductive Step where
  /-- The app supplies admin access: the call that begins it, or a retry. -/
  | supply
  /-- A restart loses what memory held. -/
  | restart
  | mint
  | check
  /-- A mint fails, perhaps after making a key whose reply was lost. -/
  | failMint (made : Bool)
  /-- A check fails and its rollback removes the key, or fails to. -/
  | failCheck (rolledBack : Bool)
  /-- Delivery and the kept entry, which `KeyDelivery` models, in one. -/
  | record
  /-- The old key's deletion settles and the operation ends. -/
  | finish
  /-- The app discards the replacement before its entry. -/
  | discard
  deriving DecidableEq, Repr

/-- T241's rules: a failed step releases admin access; a replacement keeps
it past the check, to delete the old key, until the operation ends. -/
def step (w : World) : Step → World
  | .supply => { w with admin := true }
  | .restart => { w with admin := false }
  | .mint =>
    match w.row, w.admin with
    | some ⟨none, _⟩, true =>
      { w with row := some ⟨some w.next, false⟩, made := w.made ++ [w.next], next := w.next + 1 }
    | _, _ => w
  | .check =>
    match w.row, w.admin with
    | some ⟨some k, false⟩, true => { w with row := some ⟨some k, true⟩ }
    | _, _ => w
  | .failMint made =>
    match w.row, w.admin with
    | some ⟨none, _⟩, true =>
      if made then { w with admin := false, made := w.made ++ [w.next], next := w.next + 1 }
      else { w with admin := false }
    | _, _ => w
  | .failCheck rolledBack =>
    match w.row, w.admin with
    | some ⟨some _, false⟩, true =>
      if rolledBack then { w with admin := false, row := some ⟨none, false⟩ }
      else { w with admin := false }
    | _, _ => w
  | .record =>
    match w.row with
    | some ⟨some _, true⟩ => { w with row := none }
    | _ => w
  | .finish =>
    match w.row with
    | none => { w with admin := false }
    | some _ => w
  | .discard => { w with row := none, admin := false }

def run (w : World) (steps : List Step) : World := steps.foldl step w

/-- Without admin access, no key is made until the app supplies it. -/
theorem released_makes_nothing (w : World) (steps : List Step) (hw : w.admin = false)
    (hs : Step.supply ∉ steps) : (run w steps).made = w.made := by
  induction steps generalizing w with
  | nil => rfl
  | cons s ss ih =>
    have hs' : s ≠ .supply := fun h => hs (h ▸ List.mem_cons_self)
    have rest : Step.supply ∉ ss := fun h => hs (List.mem_cons_of_mem _ h)
    obtain ⟨row, admin, made, next⟩ := w
    simp only at hw; subst hw
    have one : (step ⟨row, false, made, next⟩ s).admin = false ∧
        (step ⟨row, false, made, next⟩ s).made = made := by
      cases s with
      | supply => exact absurd rfl hs'
      | record => rcases row with _ | ⟨_ | k, _ | _⟩ <;> simp [step]
      | finish => rcases row <;> simp [step]
      | _ => rcases row with _ | ⟨_ | k, _ | _⟩ <;> simp [step]
    have h := ih _ one.1 rest
    rw [one.2] at h
    exact h

/-- A failed mint or check releases admin access. -/
theorem failed_mint_releases (w : World) (made c : Bool)
    (hr : w.row = some ⟨none, c⟩) (ha : w.admin = true) :
    (step w (.failMint made)).admin = false := by
  obtain ⟨row, admin, m, next⟩ := w
  simp only at hr ha; subst hr ha
  cases made <;> rfl

theorem failed_check_releases (w : World) (rolledBack : Bool) (k : Nat)
    (hr : w.row = some ⟨some k, false⟩) (ha : w.admin = true) :
    (step w (.failCheck rolledBack)).admin = false := by
  obtain ⟨row, admin, m, next⟩ := w
  simp only at hr ha; subst hr ha
  cases rolledBack <;> rfl

/-- So after any failure, whatever follows makes no key until the app
supplies admin access again. -/
theorem failure_makes_nothing_until_supplied (w : World) (made : Bool) (c : Bool)
    (hr : w.row = some ⟨none, c⟩) (ha : w.admin = true) (steps : List Step)
    (hs : Step.supply ∉ steps) :
    (run (step w (.failMint made)) steps).made = (step w (.failMint made)).made :=
  released_makes_nothing _ steps (failed_mint_releases w made c hr ha) hs

/-- A replacement keeps admin access past the check, to delete the old key
once its entry is kept (§14.3), and releases it when the operation ends. -/
theorem check_keeps_admin (w : World) (k : Nat) (hr : w.row = some ⟨some k, false⟩)
    (ha : w.admin = true) :
    (step w .check).admin = true ∧ (step (step w .check) .record).admin = true ∧
      (step (step (step w .check) .record) .finish).admin = false := by
  obtain ⟨row, admin, m, next⟩ := w
  simp only at hr ha; subst hr ha
  simp [step]

/-! ### Checked counterexample: the rule before T241

A failed replacement step kept the admin access it was given. -/

def oldStep (w : World) : Step → World
  | .failMint made => { step w (.failMint made) with admin := w.admin }
  | .failCheck rolledBack => { step w (.failCheck rolledBack) with admin := w.admin }
  | s => step w s

def oldRun (w : World) (steps : List Step) : World := steps.foldl oldStep w

/-- Ana's desktop begins replacing Ben's key with admin access, and the mint
fails. Under the old rule the automatic retry mints key 0 with admin access
Ana never supplied again; under T241's it waits for her. -/
def ana : World := ⟨some ⟨none, false⟩, false, [], 0⟩

theorem old_rule_mints_without_supply :
    (oldRun ana [.supply, .failMint false, .mint]).made = [0] ∧
    (run ana [.supply, .failMint false, .mint]).made = [] ∧
    (run ana [.supply, .failMint false, .supply, .mint]).made = [0] := by
  decide

end CovenStorage.AdminAccess
