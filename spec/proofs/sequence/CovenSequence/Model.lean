/-!
# The store sequence

One numbered sequence per store. Slot `k` holds one of: an admin entry, a
batch of data writes (one per device per sync pass), a key copy, a snapshot
announcement ("the store as of slot `c` is at path `p`"), or a schema raise,
which announces the raise's snapshot the same way.

Every device applies slots in strict order, so its state is a function of the
prefix it applied. That state has two parts:

- the *ledger*: everything decided by slot order alone. Members, devices and
  their removal, member key generations, other admin state (roles, names,
  access, circles) abstractly as `aux`, the first slot that claimed each unique
  value or owner row, key copies, announced snapshots, the schema version, and
  the slots that were refused;
- the *rows*, through an abstract deterministic merge. A write made at an older
  schema version is converted by the app's abstract converter. A raise replaces
  the rows with the raise snapshot (the raiser's migrated rows as of the raise's
  coverage `c`) and re-applies the counted writes of slots `c+1` to `r-1`,
  converted to the new version.

A slot that is invalid at its position is refused: it is consumed, changes
nothing but the refusal record, and every device refuses it alike.
-/

namespace CovenSequence

/-- The abstract types of a store. -/
structure Sig where
  /-- Device ids. -/
  Dev : Type
  /-- Member ids. -/
  Mem : Type
  /-- Unique values and owner rows: whatever the first slot wins. -/
  Key : Type
  /-- Storage paths of snapshot bodies. -/
  Path : Type
  /-- A device's batch of data writes for one pass. -/
  Batch : Type
  /-- The merged rows. -/
  Rows : Type
  /-- Admin operations other than membership: roles, renames, access, circles. -/
  Op : Type
  /-- The admin state those operations change. -/
  Aux : Type
  [decDev : DecidableEq Dev]
  [decMem : DecidableEq Mem]
  [decKey : DecidableEq Key]
  [decPath : DecidableEq Path]

attribute [instance] Sig.decDev Sig.decMem Sig.decKey Sig.decPath

variable {S : Sig}

/-- Admin entries. -/
inductive Entry (S : Sig) where
  | addMember (m : S.Mem)
  | removeMember (m : S.Mem)
  | addDevice (d : S.Dev) (m : S.Mem)
  | removeDevice (d : S.Dev)
  | replaceKey (m : S.Mem)
  | other (o : S.Op)

/-- What a slot holds. Each kind names the device that claimed it. -/
inductive Slot (S : Sig) where
  | admin (author : S.Dev) (e : Entry S)
  /-- `keyGen` is the member key generation the batch was made under,
  `version` its schema version, `claims` the unique values and owner rows it
  takes. -/
  | writes (author : S.Dev) (keyGen : Nat) (version : Nat) (claims : List S.Key) (batch : S.Batch)
  | keyCopy (author : S.Dev) (recipient : S.Mem) (keyGen : Nat)
  | snapshot (author : S.Dev) (covers : Nat) (path : S.Path)
  | raise (author : S.Dev) (version : Nat) (covers : Nat) (path : S.Path)

/-- The state decided by slot order alone. -/
structure Ledger (S : Sig) where
  member : S.Mem → Bool
  /-- The member each device was registered to. Never changes once set. -/
  deviceOf : S.Dev → Option S.Mem
  removed : S.Dev → Bool
  keyGen : S.Mem → Nat
  aux : S.Aux
  /-- The first slot that claimed each unique value or owner row. -/
  owner : S.Key → Option Nat
  copies : List (Nat × S.Mem × Nat)
  /-- Announced snapshots: slot, coverage, path. -/
  snapshots : List (Nat × Nat × S.Path)
  /-- The largest coverage of an announced snapshot. -/
  maxCover : Nat
  /-- The slot of the last raise. -/
  lastRaise : Nat
  version : Nat
  refused : Nat → Bool

/-- The store's rules: abstract authority, admin effects, merge, conversion
and migration, and the initial state. `versionRule` switches the snapshot
condition this package checks: a snapshot announced at slot `a` must cover the
last raise before `a`, i.e. be at the store's version. -/
structure Rules (S : Sig) where
  authorized : Ledger S → S.Dev → Entry S → Bool
  applyOp : S.Aux → S.Op → S.Aux
  copyOk : Ledger S → S.Dev → S.Mem → Nat → Bool
  merge : S.Rows → S.Batch → S.Rows
  /-- `convert from to batch`. -/
  convert : Nat → Nat → S.Batch → S.Batch
  /-- `migrate version rows`: the database part of the raise's migrations. -/
  migrate : Nat → S.Rows → S.Rows
  ledger0 : Ledger S
  rows0 : S.Rows
  versionRule : Bool

variable (R : Rules S)

/-- A device that is registered, not removed, and whose member is a member. -/
def active (L : Ledger S) (d : S.Dev) : Bool :=
  match L.deviceOf d with
  | some m => !L.removed d && L.member m
  | none => false

/-- **The removal cut-off.** A batch counts only if its device is active and it
was made under its member's current key generation, at a version the store
has reached. -/
def counts (L : Ledger S) (d : S.Dev) (g v : Nat) : Bool :=
  match L.deviceOf d with
  | some m => !L.removed d && L.member m && L.keyGen m == g && decide (v ≤ L.version)
  | none => false

/-- The checks of an entry that do not depend on roles. -/
def structural (L : Ledger S) : Entry S → Bool
  | .addMember m => !L.member m
  | .removeMember m => L.member m
  | .addDevice d m => (L.deviceOf d).isNone && L.member m
  | .removeDevice d => (L.deviceOf d).isSome && !L.removed d
  | .replaceKey m => L.member m
  | .other _ => true

/-- An entry's effect. Removing a member removes every device registered to
it, so re-adding the member does not revive them. -/
def applyEntry (L : Ledger S) : Entry S → Ledger S
  | .addMember m => { L with member := fun x => if x = m then true else L.member x }
  | .removeMember m =>
    { L with
      member := fun x => if x = m then false else L.member x
      removed := fun d => L.removed d || L.deviceOf d == some m }
  | .addDevice d m => { L with deviceOf := fun x => if x = d then some m else L.deviceOf x }
  | .removeDevice d => { L with removed := fun x => if x = d then true else L.removed x }
  | .replaceKey m => { L with keyGen := fun x => if x = m then L.keyGen m + 1 else L.keyGen x }
  | .other o => { L with aux := R.applyOp L.aux o }

/-- First slot wins: a key keeps its owner; an unowned key claimed at `k` goes
to `k`. -/
def claimAll (o : S.Key → Option Nat) (k : Nat) (cl : List S.Key) : S.Key → Option Nat :=
  fun key => match o key with
    | some i => some i
    | none => if key ∈ cl then some k else none

/-- Whether slot `k` holding `x` is valid against the ledger as of slot `k-1`.
A raise must cover every announced snapshot (C6) and the last raise, and raise
the version. A snapshot must cover less than its own slot and, under
`versionRule`, the last raise. -/
def valid (L : Ledger S) (k : Nat) : Slot S → Bool
  | .admin d e => active L d && R.authorized L d e && structural L e
  | .writes d g v _ _ => counts L d g v
  | .keyCopy d m g => active L d && R.copyOk L d m g
  | .snapshot d c _ => active L d && decide (c < k) && (!R.versionRule || decide (L.lastRaise ≤ c))
  | .raise d v c _ =>
    active L d && decide (c < k) && decide (L.maxCover ≤ c) && decide (L.lastRaise ≤ c) &&
      decide (L.version < v)

/-- The effect of a valid slot on the ledger. -/
def effect (L : Ledger S) (k : Nat) : Slot S → Ledger S
  | .admin _ e => applyEntry R L e
  | .writes _ _ _ cl _ => { L with owner := claimAll L.owner k cl }
  | .keyCopy _ m g => { L with copies := (k, m, g) :: L.copies }
  | .snapshot _ c p => { L with snapshots := (k, c, p) :: L.snapshots, maxCover := max L.maxCover c }
  | .raise _ v _ _ => { L with version := v, lastRaise := k }

/-- Record slot `k` as refused. -/
def refuse (L : Ledger S) (k : Nat) : Ledger S :=
  { L with refused := fun i => if i = k then true else L.refused i }

/-- Apply slot `k`: its effect if valid, otherwise only its refusal. -/
def ledgerStep (L : Ledger S) (k : Nat) (x : Slot S) : Ledger S :=
  if valid R L k x then effect R L k x else refuse L k

/-- The ledger after slots `1..k` of `seq`. Slot 0 is unused. -/
def ledgerAt (seq : Nat → Slot S) : Nat → Ledger S
  | 0 => R.ledger0
  | k + 1 => ledgerStep R (ledgerAt seq k) (k + 1) (seq (k + 1))

/-- A batch's effect on the rows, converted to `version`. -/
def applyWrite (version : Nat) (rows : S.Rows) : Slot S → S.Rows
  | .writes _ _ v _ b => R.merge rows (R.convert v version b)
  | _ => rows

/-- Re-apply slots `lo .. lo+n-1` from `get`, skipping refused ones, converted
to `v`. -/
def replay (get : Nat → Slot S) (refused : Nat → Bool) (v : Nat) :
    S.Rows → Nat → Nat → S.Rows
  | rows, _, 0 => rows
  | rows, lo, n + 1 =>
    replay get refused v (if refused lo then rows else applyWrite R v rows (get lo)) (lo + 1) n

/-- The rows after slots `1..k`. A valid raise at `k+1` covering `c` loads the
migrated rows as of `c` and replays slots `c+1..k`. -/
def rowsAt (seq : Nat → Slot S) : Nat → S.Rows
  | 0 => R.rows0
  | k + 1 =>
    match seq (k + 1) with
    | .raise d v c p =>
      if _h : c < k + 1 ∧ valid R (ledgerAt R seq k) (k + 1) (.raise d v c p) = true then
        replay R seq (ledgerAt R seq k).refused v (R.migrate v (rowsAt seq c)) (c + 1) (k - c)
      else rowsAt seq k
    | x =>
      if valid R (ledgerAt R seq k) (k + 1) x then
        applyWrite R (ledgerAt R seq k).version (rowsAt seq k) x
      else rowsAt seq k
termination_by k => k

/-- The device state after slots `1..k`. -/
def stateAt (seq : Nat → Slot S) (k : Nat) : Ledger S × S.Rows :=
  (ledgerAt R seq k, rowsAt R seq k)

theorem ledgerAt_succ (seq : Nat → Slot S) (k : Nat) :
    ledgerAt R seq (k + 1) = ledgerStep R (ledgerAt R seq k) (k + 1) (seq (k + 1)) := rfl

theorem rowsAt_zero (seq : Nat → Slot S) : rowsAt R seq 0 = R.rows0 := by
  rw [rowsAt]

/-- The rows step at a raise slot. -/
theorem rowsAt_raise (seq : Nat → Slot S) (k : Nat) {d v c p}
    (hs : seq (k + 1) = .raise d v c p) :
    rowsAt R seq (k + 1) =
      if valid R (ledgerAt R seq k) (k + 1) (.raise d v c p) = true then
        replay R seq (ledgerAt R seq k).refused v (R.migrate v (rowsAt R seq c)) (c + 1) (k - c)
      else rowsAt R seq k := by
  rw [rowsAt]
  simp only [hs]
  by_cases hv : valid R (ledgerAt R seq k) (k + 1) (.raise d v c p) = true
  · have hc : c < k + 1 := by
      simp [valid] at hv
      omega
    simp [hv, hc]
  · simp [hv]

/-- The rows step at any slot that is not a raise. -/
theorem rowsAt_other (seq : Nat → Slot S) (k : Nat)
    (hs : ∀ d v c p, seq (k + 1) ≠ .raise d v c p) :
    rowsAt R seq (k + 1) =
      if valid R (ledgerAt R seq k) (k + 1) (seq (k + 1)) then
        applyWrite R (ledgerAt R seq k).version (rowsAt R seq k) (seq (k + 1))
      else rowsAt R seq k := by
  rw [rowsAt]
  cases h : seq (k + 1) with
  | raise d v c p => exact absurd h (hs d v c p)
  | _ => simp

/-- A replay reads only the slots it replays. -/
theorem replay_congr (g₁ g₂ : Nat → Slot S) (refused : Nat → Bool) (v : Nat) :
    ∀ (n : Nat) (rows : S.Rows) (lo : Nat), (∀ i, lo ≤ i → i < lo + n → g₁ i = g₂ i) →
      replay R g₁ refused v rows lo n = replay R g₂ refused v rows lo n
  | 0, _, _, _ => rfl
  | n + 1, rows, lo, h => by
    simp only [replay]
    rw [h lo (Nat.le_refl _) (by omega)]
    exact replay_congr g₁ g₂ refused v n _ (lo + 1) (fun i h1 h2 => h i (by omega) (by omega))

/-- A replay reads only the refusals of the slots it replays. -/
theorem replay_congr_refused (g : Nat → Slot S) (f₁ f₂ : Nat → Bool) (v : Nat) :
    ∀ (n : Nat) (rows : S.Rows) (lo : Nat), (∀ i, lo ≤ i → i < lo + n → f₁ i = f₂ i) →
      replay R g f₁ v rows lo n = replay R g f₂ v rows lo n
  | 0, _, _, _ => rfl
  | n + 1, rows, lo, h => by
    simp only [replay]
    rw [h lo (Nat.le_refl _) (by omega)]
    exact replay_congr_refused g f₁ f₂ v n _ (lo + 1) (fun i h1 h2 => h i (by omega) (by omega))

end CovenSequence
