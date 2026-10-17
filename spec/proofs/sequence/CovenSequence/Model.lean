import CovenSequence.Claims

/-!
# The store sequence

One numbered sequence per store. Slot `k` holds one of: an admin entry, one
device's batch of data writes for a sync pass (with one part per audience it
writes to), a key copy, a snapshot announcement for one audience ("audience
`A` as of slot `c` is at path `p`"), a preparation announcement, or a schema raise or reset of one audience
that activates its previously announced body. The audiences are the
store and each circle. A slot's kind, and which audiences its parts or
announcement belong to, and its device and claim number, are readable by every
member device: the kind in the clear, the rest sealed under the store key.

Every device applies slots in strict order, so its state is a function of the
prefix it applied. For each audience `A` the state has three parts:

- the *ledger*: what admin slots decide, the same for every audience. That is
  members and roles, devices and their removal, member key generations, other admin
  state (names, access, circle membership) abstractly as `aux`, key
  copies, refused admin slots, and per device the greatest claim number and
  its slot digest;
- the *audience ledger* of `A`: the first slot that claimed each unique value
  or owner row of `A`, `A`'s announced snapshots, its schema version, its last
  raise or reset, preparations, and its refused parts;
- the *rows* of `A`, through an abstract deterministic merge. A part made at an
  older version is converted by the app's abstract converter. A raise of `A`
  covering `c` replaces the rows with the raiser's migrated rows as of `c` and
  re-applies the counted `A` parts of slots `c+1` to `r-1`, converted.

Only admin slots and key copies change the ledger, besides the claim records
every slot records, and only slots that touch
`A` change `A`'s audience ledger and rows. So a device that reads the admin
slots and the parts of the audiences it belongs to computes exactly their
state, and never needs another audience's parts.

A slot or part that is invalid at its position is refused: it is consumed,
changes nothing but the refusal record, and every device refuses it alike.
-/

namespace CovenSequence

/-- The abstract types of a store. -/
structure Sig where
  /-- Device ids. -/
  Dev : Type
  /-- SHA-256 tokens for complete sealed slot bytes. -/
  Digest : Type
  /-- Member ids. -/
  Mem : Type
  /-- Unique values and owner rows: whatever the first slot wins. -/
  Key : Type
  /-- Storage paths of snapshot bodies. -/
  Path : Type
  /-- A device's writes to one audience in one pass. -/
  Batch : Type
  /-- One audience's merged rows. -/
  Rows : Type
  /-- Admin operations other than membership and roles: renames, access, circles. -/
  Op : Type
  /-- The admin state those operations change. -/
  Aux : Type
  /-- Audiences: the store and each circle. -/
  Aud : Type
  [decDigest : DecidableEq Digest]
  [decDev : DecidableEq Dev]
  [decMem : DecidableEq Mem]
  [decKey : DecidableEq Key]
  [decPath : DecidableEq Path]
  [decAud : DecidableEq Aud]

attribute [instance] Sig.decDigest Sig.decDev Sig.decMem Sig.decKey Sig.decPath Sig.decAud

variable {S : Sig}

inductive Role where
  | member
  | admin
  deriving DecidableEq

/-- Admin entries. -/
inductive Entry (S : Sig) where
  | addMember (m : S.Mem) (role : Role)
  | removeMember (m : S.Mem)
  | changeRole (m : S.Mem) (role : Role)
  | addDevice (d : S.Dev) (m : S.Mem)
  | removeDevice (d : S.Dev)
  | replaceKey (m : S.Mem)
  | other (o : S.Op)

/-- One audience's part of a batch: its schema version, the unique values and
owner rows it takes, and its writes. -/
structure WPart (S : Sig) where
  version : Nat
  claims : List S.Key
  batch : S.Batch

/-- An admin action that activates a prepared audience snapshot. -/
inductive Boundary where
  | raise (version : Nat)
  | reset
  deriving DecidableEq

/-- What a slot holds. Each kind names the device that claimed it. -/
inductive Slot (S : Sig) where
  | admin (author : S.Dev) (e : Entry S)
  /-- `keyGen` is the member key generation the batch was made under; `parts`
  gives its part for each audience it writes to. -/
  | writes (author : S.Dev) (keyGen : Nat) (parts : S.Aud → Option (WPart S))
  | keyCopy (author : S.Dev) (recipient : S.Mem) (keyGen : Nat)
  | snapshot (author : S.Dev) (aud : S.Aud) (covers : Nat) (path : S.Path)
  | prepare (author : S.Dev) (aud : S.Aud) (kind : Boundary) (covers : Nat) (path : S.Path)
  | boundary (author : S.Dev) (aud : S.Aud) (kind : Boundary) (covers : Nat) (path : S.Path)

/-- What admin slots decide. -/
structure Ledger (S : Sig) where
  member : S.Mem → Bool
  /-- Removed member ids remain unavailable forever. -/
  retired : S.Mem → Bool
  role : S.Mem → Role
  /-- The member each device was registered to. Never changes once set. -/
  deviceOf : S.Dev → Option S.Mem
  removed : S.Dev → Bool
  keyGen : S.Mem → Nat
  aux : S.Aux
  copies : List (Nat × S.Mem × Nat)
  refused : Nat → Bool
  /-- Greatest included claim number and its slot digest; later slots win ties. -/
  claims : S.Dev → Option (ClaimRecord S.Digest)

/-- What one audience's slots decide, besides its rows. -/
structure AudLedger (S : Sig) where
  /-- The first slot that claimed each unique value or owner row. -/
  owner : S.Key → Option Nat
  /-- Announced snapshots: slot, coverage, path. -/
  snapshots : List (Nat × Nat × S.Path)
  /-- The largest coverage of an announced snapshot. -/
  maxCover : Nat
  /-- The slot of the last raise or reset. -/
  lastBoundary : Nat
  lastReset : Nat
  preparations : List (Boundary × Nat × S.Path)
  version : Nat
  refused : Nat → Bool

/-- The store's rules: abstract authority, admin effects, who may write to an
audience, merge, conversion and migration, and the initial state. -/
structure Rules (S : Sig) where
  authorized : Ledger S → S.Dev → Entry S → Bool
  applyOp : S.Aux → S.Op → S.Aux
  copyOk : Ledger S → S.Dev → S.Mem → Nat → Bool
  /-- Whether a device may write to an audience, e.g. its member is in the
  circle. -/
  mayWrite : Ledger S → S.Dev → S.Aud → Bool
  merge : S.Rows → S.Batch → S.Rows
  /-- `convert from to batch`. -/
  convert : Nat → Nat → S.Batch → S.Batch
  /-- `migrate version rows`: the database part of the raise's migrations. -/
  migrate : Nat → S.Rows → S.Rows
  resetRows : S.Path → S.Rows
  boundaryAllowed : Ledger S → S.Dev → S.Aud → Boundary → Bool
  /-- Keep only each batch's writes whose authored reads include the reset. -/
  sinceReset : Nat → S.Batch → S.Batch
  /-- The claim number at a sequence position: its device's counter at the claim.
  The position distinguishes repeated slots with identical decoded contents. -/
  number : Nat → Slot S → Nat
  /-- SHA-256 of the complete slot bytes, including the claimed path. -/
  digest : Nat → Slot S → S.Digest
  ledger0 : Ledger S
  aud0 : S.Aud → AudLedger S
  rows0 : S.Aud → S.Rows

variable (R : Rules S)

/-- A device that is registered, not removed, and whose member is a member. -/
def active (L : Ledger S) (d : S.Dev) : Bool :=
  match L.deviceOf d with
  | some m => !L.removed d && L.member m
  | none => false

/-- **The removal cut-off.** A batch counts only if its device is active and it
was made under its member's current key generation. -/
def counts (L : Ledger S) (d : S.Dev) (g : Nat) : Bool :=
  match L.deviceOf d with
  | some m => !L.removed d && L.member m && L.keyGen m == g
  | none => false

/-- An admin author is active, or is the fresh installation registering itself.
The member's current-key signature authenticating that installation is checked
outside the model. -/
def adminAuthor (L : Ledger S) (d : S.Dev) (e : Entry S) : Bool :=
  active L d || match e with
    | .addDevice target _ => decide (d = target) && (L.deviceOf d).isNone
    | _ => false

/-- The checks of an entry that do not depend on roles. Only an installation
itself can register its id, including when the member is an admin. -/
def structural (L : Ledger S) (author : S.Dev) : Entry S → Bool
  | .addMember m _ => !L.member m && !L.retired m
  | .removeMember m => L.member m
  | .changeRole m _ => L.member m
  | .addDevice d m => decide (author = d) && (L.deviceOf d).isNone && L.member m
  | .removeDevice d => (L.deviceOf d).isSome && !L.removed d
  | .replaceKey m => L.member m
  | .other _ => true

/-- An entry's effect. Removal retires the member id and every device bound to it. -/
def applyEntry (L : Ledger S) : Entry S → Ledger S
  | .addMember m role =>
    { L with member := fun x => if x = m then true else L.member x
             role := fun x => if x = m then role else L.role x }
  | .removeMember m =>
    { L with
      member := fun x => if x = m then false else L.member x
      retired := fun x => x == m || L.retired x
      removed := fun d => L.removed d || L.deviceOf d == some m }
  | .changeRole m role => { L with role := fun x => if x = m then role else L.role x }
  | .addDevice d m => { L with deviceOf := fun x => if x = d then some m else L.deviceOf x }
  | .removeDevice d => { L with removed := fun x => if x = d then true else L.removed x }
  | .replaceKey m => { L with keyGen := fun x => if x = m then L.keyGen m + 1 else L.keyGen x }
  | .other o => { L with aux := R.applyOp L.aux o }

/-- Whether an admin slot or key copy is valid; every other kind is left to
its audiences. -/
def ledgerValid (L : Ledger S) : Slot S → Bool
  | .admin d e => adminAuthor L d e && R.authorized L d e && structural L d e
  | .keyCopy d m g => active L d && R.copyOk L d m g
  | _ => true

def ledgerEffect (L : Ledger S) (k : Nat) : Slot S → Ledger S
  | .admin _ e => applyEntry R L e
  | .keyCopy _ m g => { L with copies := (k, m, g) :: L.copies }
  | _ => L

/-- Record slot `k` as refused. -/
def refuse (L : Ledger S) (k : Nat) : Ledger S :=
  { L with refused := fun i => if i = k then true else L.refused i }

/-- The device that claimed a slot. -/
def author : Slot S → S.Dev
  | .admin d _ => d
  | .writes d _ _ => d
  | .keyCopy d _ _ => d
  | .snapshot d _ _ _ => d
  | .prepare d _ _ _ _ => d
  | .boundary d _ _ _ _ => d

/-- Record a claimed slot's number and digest for its device, whether or not the slot is
valid: a refused slot was still claimed. -/
def noteClaim (L : Ledger S) (k : Nat) (x : Slot S) : Ledger S :=
  { L with claims := fun e => if e = author x then
      ClaimRecord.remember (L.claims e) ⟨R.number k x, R.digest k x⟩ else L.claims e }

@[simp] theorem noteClaim_member (L : Ledger S) (k : Nat) (x : Slot S) : (noteClaim R L k x).member = L.member := rfl
@[simp] theorem noteClaim_deviceOf (L : Ledger S) (k : Nat) (x : Slot S) : (noteClaim R L k x).deviceOf = L.deviceOf := rfl
@[simp] theorem noteClaim_removed (L : Ledger S) (k : Nat) (x : Slot S) : (noteClaim R L k x).removed = L.removed := rfl
@[simp] theorem noteClaim_keyGen (L : Ledger S) (k : Nat) (x : Slot S) : (noteClaim R L k x).keyGen = L.keyGen := rfl

/-- Apply slot `k` to the ledger: its effect if it is a valid admin slot or key
copy, its refusal if invalid, and in every case its claim number and digest. -/
def ledgerStep (L : Ledger S) (k : Nat) (x : Slot S) : Ledger S :=
  noteClaim R (if ledgerValid R L x then ledgerEffect R L k x else refuse L k) k x

/-- Whether slot `x` has a part for, or announces for, audience `A`. -/
def touches (A : S.Aud) : Slot S → Bool
  | .writes _ _ parts => (parts A).isSome
  | .snapshot _ a _ _ => decide (a = A)
  | .prepare _ a _ _ _ | .boundary _ a _ _ _ => decide (a = A)
  | _ => false

/-- Whether slot `k`'s part for `A` is valid, against the ledger and `A`'s
ledger as of slot `k-1`.

- A part counts under the removal cut-off, when its device may write to `A`,
  and at a version `A` has reached.
- Every announcement covers less than its own slot, never decreases
  announced coverage and covers the last raise or reset.
- A boundary requires its matching preparation, audience authority and
  coverage of every earlier announcement. A raise increases the version. -/
def audValid (L : Ledger S) (P : AudLedger S) (k : Nat) (A : S.Aud) : Slot S → Bool
  | .writes d g parts =>
    match parts A with
    | some w => counts L d g && R.mayWrite L d A && decide (w.version ≤ P.version)
    | none => true
  | .snapshot d _ c _ =>
    active L d && decide (c < k) && decide (P.maxCover ≤ c) && decide (P.lastBoundary ≤ c)
  | .prepare d _ kind c _ =>
    active L d && decide (c < k) && decide (P.maxCover ≤ c) && decide (P.lastBoundary ≤ c) &&
      (match kind with | .raise v => decide (P.version < v) | .reset => true)
  | .boundary d _ kind c p =>
    active L d && R.boundaryAllowed L d A kind && decide (c < k) && decide (P.maxCover ≤ c) &&
      decide (P.lastBoundary ≤ c) && decide ((kind, c, p) ∈ P.preparations) &&
      (match kind with | .raise v => decide (P.version < v) | .reset => true)
  | _ => true

/-- First slot wins: a key keeps its owner; an unowned key claimed at `k` goes
to `k`. -/
def claimAll (o : S.Key → Option Nat) (k : Nat) (cl : List S.Key) : S.Key → Option Nat :=
  fun key => match o key with
    | some i => some i
    | none => if key ∈ cl then some k else none

def audEffect (P : AudLedger S) (k : Nat) (A : S.Aud) : Slot S → AudLedger S
  | .writes _ _ parts =>
    match parts A with
    | some w => { P with owner := claimAll P.owner k w.claims }
    | none => P
  | .snapshot _ _ c p => { P with snapshots := (k, c, p) :: P.snapshots, maxCover := max P.maxCover c }
  | .prepare _ _ kind c p =>
    { P with preparations := (kind, c, p) :: P.preparations, maxCover := max P.maxCover c }
  | .boundary _ _ kind _ _ =>
    { P with lastBoundary := k
             lastReset := match kind with | .raise _ => P.lastReset | .reset => k
             version := match kind with | .raise v => v | .reset => P.version }
  | _ => P

def refuseAud (P : AudLedger S) (k : Nat) : AudLedger S :=
  { P with refused := fun i => if i = k then true else P.refused i }

/-- Apply slot `k` to `A`'s ledger: nothing unless it touches `A`. -/
def audStep (L : Ledger S) (P : AudLedger S) (k : Nat) (A : S.Aud) (x : Slot S) : AudLedger S :=
  if touches A x then (if audValid R L P k A x then audEffect P k A x else refuseAud P k) else P

/-- The ledger after slots `1..k` of `seq`. Slot 0 is unused. -/
def ledgerAt (seq : Nat → Slot S) : Nat → Ledger S
  | 0 => R.ledger0
  | k + 1 => ledgerStep R (ledgerAt seq k) (k + 1) (seq (k + 1))

/-- Audience `A`'s ledger after slots `1..k`. -/
def audAt (seq : Nat → Slot S) (A : S.Aud) : Nat → AudLedger S
  | 0 => R.aud0 A
  | k + 1 => audStep R (ledgerAt R seq k) (audAt seq A k) (k + 1) A (seq (k + 1))

/-- A slot's part for `A` applied to `A`'s rows, converted to `version`. -/
def applyWrite (A : S.Aud) (version reset : Nat) (rows : S.Rows) : Slot S → S.Rows
  | .writes _ _ parts =>
    match parts A with
    | some w => R.merge rows (R.convert w.version version (R.sinceReset reset w.batch))
    | none => rows
  | _ => rows

/-- Re-apply the `A` parts of slots `lo .. lo+n-1` from `get`, skipping refused
ones, converted to `v`. -/
def replay (A : S.Aud) (get : Nat → Slot S) (refused : Nat → Bool) (v reset : Nat) :
    S.Rows → Nat → Nat → S.Rows
  | rows, _, 0 => rows
  | rows, lo, n + 1 =>
    replay A get refused v reset (if refused lo then rows else applyWrite R A v reset rows (get lo)) (lo + 1) n

/-- Prepared bodies differ by purpose; reset contents are chosen by the
resetting device, while a raise migrates the covered prefix. -/
def preparedRows (kind : Boundary) (path : S.Path) (rows : S.Rows) : S.Rows :=
  match kind with | .raise v => R.migrate v rows | .reset => R.resetRows path

/-- A raise converts the uncovered suffix. A reset discards that suffix:
none of it could have read the reset's not-yet-claimed slot. -/
def finishBoundary (A : S.Aud) (get : Nat → Slot S) (refused : Nat → Bool)
    (kind : Boundary) (reset : Nat) (rows : S.Rows) (lo n : Nat) : S.Rows :=
  match kind with
  | .raise v => replay R A get refused v reset rows lo n
  | .reset => rows

/-- Audience `A`'s rows after slots `1..k`. A valid raise of `A` at `k+1`
covering `c` loads the migrated rows as of `c` and replays slots `c+1..k`. -/
def rowsAt (seq : Nat → Slot S) (A : S.Aud) : Nat → S.Rows
  | 0 => R.rows0 A
  | k + 1 =>
    match seq (k + 1) with
    | .boundary d a v c p =>
      if _h : c < k + 1 ∧ a = A ∧
          audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A (.boundary d a v c p) = true then
        finishBoundary R A seq (audAt R seq A k).refused v (audAt R seq A k).lastReset (preparedRows R v p (rowsAt seq A c)) (c + 1) (k - c)
      else rowsAt seq A k
    | x =>
      if touches A x && audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A x then
        applyWrite R A (audAt R seq A k).version (audAt R seq A k).lastReset (rowsAt seq A k) x
      else rowsAt seq A k
termination_by k => k

/-- Audience `A`'s state after slots `1..k`. -/
def stateAt (seq : Nat → Slot S) (A : S.Aud) (k : Nat) : Ledger S × AudLedger S × S.Rows :=
  (ledgerAt R seq k, audAt R seq A k, rowsAt R seq A k)

/-- Admin slot or key copy `j` was applied. -/
def Applied (seq : Nat → Slot S) (j : Nat) : Prop :=
  ledgerValid R (ledgerAt R seq (j - 1)) (seq j) = true

/-- Slot `j` touches `A` and its part or announcement for `A` was applied. -/
def AppliedTo (seq : Nat → Slot S) (A : S.Aud) (j : Nat) : Prop :=
  touches A (seq j) = true ∧
    audValid R (ledgerAt R seq (j - 1)) (audAt R seq A (j - 1)) j A (seq j) = true

theorem ledgerAt_succ (seq : Nat → Slot S) (k : Nat) :
    ledgerAt R seq (k + 1) = ledgerStep R (ledgerAt R seq k) (k + 1) (seq (k + 1)) := rfl

theorem audAt_succ (seq : Nat → Slot S) (A : S.Aud) (k : Nat) :
    audAt R seq A (k + 1) = audStep R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A (seq (k + 1)) := rfl

theorem rowsAt_zero (seq : Nat → Slot S) (A : S.Aud) : rowsAt R seq A 0 = R.rows0 A := by
  rw [rowsAt]

/-- The rows step at a raise or reset slot. -/
theorem rowsAt_boundary (seq : Nat → Slot S) (A : S.Aud) (k : Nat) {d a v c p}
    (hs : seq (k + 1) = .boundary d a v c p) :
    rowsAt R seq A (k + 1) =
      if a = A ∧ audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A (.boundary d a v c p) = true then
        finishBoundary R A seq (audAt R seq A k).refused v (audAt R seq A k).lastReset (preparedRows R v p (rowsAt R seq A c)) (c + 1) (k - c)
      else rowsAt R seq A k := by
  rw [rowsAt]
  simp only [hs]
  split
  · rename_i h
    split
    · rfl
    · rename_i h'; exact absurd ⟨h.2.1, h.2.2⟩ h'
  · rename_i h
    split
    · rename_i hv
      have hc : c < k + 1 := by
        have := hv.2
        simp [audValid] at this
        omega
      exact absurd ⟨hc, hv.1, hv.2⟩ h
    · rfl

/-- The rows step at any slot that is not a raise or reset. -/
theorem rowsAt_other (seq : Nat → Slot S) (A : S.Aud) (k : Nat)
    (hs : ∀ d a v c p, seq (k + 1) ≠ .boundary d a v c p) :
    rowsAt R seq A (k + 1) =
      if touches A (seq (k + 1)) && audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A (seq (k + 1)) then
        applyWrite R A (audAt R seq A k).version (audAt R seq A k).lastReset (rowsAt R seq A k) (seq (k + 1))
      else rowsAt R seq A k := by
  rw [rowsAt]
  cases h : seq (k + 1) with
  | boundary d a v c p => exact absurd h (hs d a v c p)
  | _ => simp

/-- A replay reads only the slots it replays. -/
theorem replay_congr (A : S.Aud) (g₁ g₂ : Nat → Slot S) (refused : Nat → Bool) (v reset : Nat) :
    ∀ (n : Nat) (rows : S.Rows) (lo : Nat), (∀ i, lo ≤ i → i < lo + n → g₁ i = g₂ i) →
      replay R A g₁ refused v reset rows lo n = replay R A g₂ refused v reset rows lo n
  | 0, _, _, _ => rfl
  | n + 1, rows, lo, h => by
    simp only [replay]
    rw [h lo (Nat.le_refl _) (by omega)]
    exact replay_congr A g₁ g₂ refused v reset n _ (lo + 1) (fun i h1 h2 => h i (by omega) (by omega))

theorem finishBoundary_congr (A : S.Aud) (g₁ g₂ : Nat → Slot S) (refused : Nat → Bool)
    (kind : Boundary) (reset n : Nat) (rows : S.Rows) (lo : Nat)
    (same : ∀ i, lo ≤ i → i < lo + n → g₁ i = g₂ i) :
    finishBoundary R A g₁ refused kind reset rows lo n = finishBoundary R A g₂ refused kind reset rows lo n := by
  cases kind with
  | raise v => exact replay_congr R A g₁ g₂ refused v reset n rows lo same
  | reset => rfl

end CovenSequence
