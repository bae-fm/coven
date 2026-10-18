import CovenSequence.Authorship
import CovenSequence.Keys
import CovenSequence.Registration
import CovenSequence.Owners
import Lean

/-! Concrete inputs at the model's abstract merge boundary. Row updates are
merged presence, value and input setters, before uniqueness removes losers. -/
namespace SequenceApply
open CovenSequence Lean

structure Row where
  id : Nat
  live : Bool
  value : Option Nat
  inputs : List LiveClaims.Position

structure Batch where
  writes : List Nat
  rows : List Row
  creations : List (Nat × Nat)

/-- The two permanent claim namespaces cannot collide. -/
abbrev Key := Sum Nat Nat

structure CircleChange (members : Nat) where
  audience : Nat
  member : Fin members
  allowed : Bool

abbrev sig (members : Nat) : Sig where
  Dev := Nat
  Digest := String
  Mem := Fin members
  Key := Key
  Path := Unit
  Batch := Batch
  Rows := List Row
  Op := CircleChange members
  Aux := List (CircleChange members)
  Aud := Nat

structure Audience where
  id : Nat
  version : Nat
  writers : List Nat

structure Config (n : Nat) where
  name : String
  members : List (Fin n × Role × Nat)
  devices : List (Nat × Fin n)
  audiences : List Audience

/-- The abstract merge receives complete replacements for affected merged
rows. Losing rows remain present so LiveClaims can select them after release. -/
def merge (rows : List Row) (batch : Batch) : List Row :=
  batch.rows.foldl (fun rs r => rs.filter (fun old => old.id != r.id) ++ [r]) rows

def initialLedger (c : Config n) : Ledger (sig n) where
  name := c.name
  member := fun m => c.members.any (fun x => x.1 == m)
  retired := fun _ => false
  role := fun m => match c.members.find? (fun x => x.1 == m) with
    | some x => x.2.1
    | none => .member
  deviceOf := fun d => (c.devices.find? (fun x => x.1 == d)).map (·.2)
  removed := fun _ => false
  keyGen := fun m => match c.members.find? (fun x => x.1 == m) with
    | some x => x.2.2
    | none => 0
  aux := []
  copies := []
  refused := fun _ => false
  claims := fun _ => none
  writeHistory := fun _ => none

def initialAudience (version : Nat) : AudLedger (sig n) :=
  ⟨fun _ => none, [], 0, 0, 0, [], version, fun _ => false⟩

def initialAudiences (c : Config n) (a : Nat) : AudLedger (sig n) :=
  initialAudience (match c.audiences.find? (fun x => x.id == a) with
    | some x => x.version
    | none => 0)

/-- Audience membership is per member, and devices resolve it at the supplied prefix. -/
def mayWrite (c : Config n) (L : Ledger (sig n)) (d a : Nat) : Bool :=
  match L.deviceOf d with
  | none => false
  | some m => match L.aux.find? (fun change => change.audience == a && change.member == m) with
    | some change => change.allowed
    | none => c.audiences.any (fun x => x.id == a && decide (m.val ∈ x.writers))

def baseRules (c : Config n) (records : Nat → ClaimRecord String) : Rules (sig n) where
  authorized := fun L d e => match e with
    | .addDevice _ m => L.member m
    | .removeDevice target => Authority.isAdmin L d ||
        match L.deviceOf d with
        | some m => L.deviceOf target == some m
        | none => false
    | .replaceKey m => Authority.isAdmin L d || L.deviceOf d == some m
    | .other change => mayWrite c L d change.audience && L.member change.member
    | _ => Authority.isAdmin L d
  applyOp := fun changes op => op :: changes
  copyOk := fun _ _ _ _ => false
  mayWrite := mayWrite c
  merge := merge
  convert := fun _ _ b => b
  migrate := fun _ r => r
  resetRows := fun _ => []
  boundaryAllowed := fun _ _ _ _ => false
  sinceReset := fun _ b => b
  number := fun k _ => (records k).number
  digest := fun k _ => (records k).digest
  ledger0 := initialLedger c
  aud0 := initialAudiences c
  rows0 := fun _ => []

def hasAdmin (L : Ledger (sig n)) : Decidable (Authority.HasAdmin L) :=
  decidable_of_iff ((List.finRange n).any (fun m => L.member m && decide (L.role m = .admin)) = true)
    (by simp [Authority.HasAdmin, List.any_eq_true])

def rules (c : Config n) (records : Nat → ClaimRecord String) : Rules (sig n) :=
  Authority.withRoles (baseRules c records) hasAdmin

/-- An existing member can register from a fresh installation even when none
of that member's registered devices is active. Its older devices stay inactive.
The current member-key signature is an external authentication premise. -/
theorem member_without_active_device_can_register (c : Config n) (records : Nat → ClaimRecord String)
    (seq : Nat → Slot (sig n)) (k : Nat) (m : Fin n) (d : Nat)
    (initial : Authority.HasAdmin (initialLedger c))
    (member : (ledgerAt (rules c records) seq k).member m = true)
    (noActive : ∀ old, (ledgerAt (rules c records) seq k).deviceOf old = some m →
      active (ledgerAt (rules c records) seq k) old = false)
    (fresh : (ledgerAt (rules c records) seq k).deviceOf d = none) :
    let R := rules c records
    let L := ledgerAt R seq k
    let next := ledgerStep R L (k + 1) (.admin d (.addDevice d m))
    ledgerValid R L (.admin d (.addDevice d m)) = true ∧
      next.deviceOf d = some m ∧ active next d = true ∧
      ∀ old, L.deviceOf old = some m → active next old = false := by
  let R := rules c records
  let L := ledgerAt R seq k
  change L.member m = true at member
  have admin : Authority.HasAdmin L := by
    have h := Authority.every_prefix_has_admin (baseRules c records) seq initial k
    simpa [L, R, rules, Authority.withRoles_decision_eq] using h
  have authorized : R.authorized L d (.addDevice d m) = true := by
    simp [R, rules, Authority.withRoles, Authority.allowed, Authority.requiresAdmin,
      baseRules, applyEntry, Authority.HasAdmin] at admin ⊢
    exact ⟨member, admin⟩
  have valid := self_registration_valid R L d m fresh member authorized
  have unremoved : L.removed d = false :=
    unregistered_unremoved R seq (fun _ _ => rfl) k d fresh
  have effect : ledgerStep R L (k + 1) (.admin d (.addDevice d m)) =
      noteSlot R L (applyEntry R L (.addDevice d m)) (k + 1) (.admin d (.addDevice d m)) := by
    simp [ledgerStep, valid, ledgerEffect]
  change ledgerValid R L _ = true ∧ _
  refine ⟨valid, self_registration_applied R L _ d m fresh member authorized, ?_, ?_⟩
  · rw [effect]
    simp [active, applyEntry, member, unremoved]
  · intro old owned
    have different : old ≠ d := by
      intro same
      subst old
      simp [fresh] at owned
    rw [effect]
    simpa [active, applyEntry, different] using noActive old owned

def sorted (xs : List Nat) : List Nat := xs.eraseDups.mergeSort (fun a b => a ≤ b)

def roleJson : Role → Json
  | .admin => toJson "admin"
  | .member => toJson "member"

def ledgerJson (L : Ledger (sig n)) (devices : List Nat) (through : Nat) : Json :=
  Json.mkObj [
    ("name", toJson L.name),
    ("members", toJson ((List.finRange n).filterMap fun m =>
      if L.member m then some (Json.arr #[toJson m.val, roleJson (L.role m)]) else none)),
    ("retired", toJson ((List.finRange n).filterMap fun m =>
      if L.retired m then some m.val else none)),
    ("devices", toJson (devices.filterMap fun d =>
      (L.deviceOf d).map fun m => toJson [d, m.val])),
    ("removed", toJson (devices.filter L.removed)),
    ("key_generations", toJson ((List.finRange n).map fun m => [m.val, L.keyGen m])),
    ("last_claims", toJson (devices.filterMap fun d =>
      (L.claims d).map fun r => Json.arr #[toJson d, toJson r.number, toJson r.digest])),
    ("refused", toJson ((List.range through).map (· + 1) |>.filter L.refused))]

def rowJson (r : Row) : Json := Json.mkObj [
  ("row", toJson r.id), ("live", toJson r.live), ("value", toJson r.value),
  ("inputs", toJson (r.inputs.map fun (s, w) => [s, w]))]

def audienceJson (entries : List (ClaimRecord String × Slot (sig n))) (a through : Nat)
    (L : Ledger (sig n)) (P : AudLedger (sig n)) (rows : List Row) : Json := Id.run do
  let slots := (List.range through).map (· + 1)
  let parts := (entries.take through).filterMap fun (_, s) => match s with
    | .writes _ _ parts => parts a
    | _ => none
  let keys := parts.flatMap (·.claims)
  let uniques := sorted (keys.filterMap fun k => match k with | .inl v => some v | _ => none)
  let owners := sorted (keys.filterMap fun k => match k with | .inr r => some r | _ => none)
  let creations : List (Nat × Owners.Creation Nat Unit Nat Nat) :=
    ((entries.take through).zipIdx).flatMap fun ((_, s), i) =>
      match s with
      | .writes d _ ps =>
        if P.refused (i + 1) then [] else
        match ps a, L.deviceOf d with
        | some p, some m => p.batch.creations.map fun (row, number) =>
            (row, ⟨⟨d, number, (), 0, 0, i + 1⟩, i + 1, m.val⟩)
        | _, _ => []
      | _ => []
  let creators := (sorted (creations.map (·.1))).filterMap fun row =>
    (Owners.establish none (creations.filterMap fun (r,c) => if r == row then some c else none)).map fun c =>
      [row, c.slot, c.original.number, c.original.device, c.member]
  let rows := rows.mergeSort (fun a b => a.id ≤ b.id)
  let inputs : LiveClaims.Inputs Row Nat := {
    rows, live := Row.live, value := Row.value, inputs := Row.inputs, keyRank := Row.id
    counted := fun (k, w) =>
      if 0 < k && k ≤ through && !P.refused k then
        match entries[k - 1]? with
        | some (_, .writes _ _ ps) => match ps a with
          | some p => decide (w ∈ p.batch.writes)
          | none => false
        | _ => false
      else false }
  let values := sorted (rows.filterMap (·.value))
  return Json.mkObj [
    ("audience", toJson a), ("version", toJson P.version),
    ("unique_claims", toJson (uniques.filterMap fun v => (P.owner (.inl v)).map fun k => [v, k])),
    ("owner_claims", toJson (owners.filterMap fun r => (P.owner (.inr r)).map fun k => [r, k])),
    ("owner_creations", toJson creators),
    ("live_winners", toJson (values.map fun v => Json.arr #[toJson v,
      toJson ((LiveClaims.winner inputs v).map (·.id))])),
    ("rows", toJson (rows.map rowJson)),
    ("refused", toJson (slots.filter P.refused))]

/-- Each prefix uses the Model definitions, including refusal-only effects and
claim recording for refused slots. The extension outside 1..length is never
read; it reuses an actual slot instead of inventing a sentinel operation. -/
def run (c : Config n) (entries : List (ClaimRecord String × Slot (sig n))) : Json := Id.run do
  let devices := sorted (c.devices.map (·.1) ++ entries.flatMap fun (_, s) =>
    author s :: match s with
      | .admin _ (.addDevice d _) | .admin _ (.removeDevice d) => [d]
      | _ => [])
  let audiences := c.audiences.mergeSort (fun a b => a.id ≤ b.id)
  let state : Nat → Ledger (sig n) × (Nat → AudLedger (sig n)) × (Nat → List Row) :=
    match entries with
    | [] => fun _ => (initialLedger c, initialAudiences c, fun _ => [])
    | first :: _ =>
      let seq := fun k => ((entries[k - 1]?).getD first).2
      let R := rules c (fun k => ((entries[k - 1]?).getD first).1)
      fun k => (ledgerAt R seq k, fun a => audAt R seq a k, fun a => rowsAt R seq a k)
  let fields := fun k =>
    let (L, audiencesAt, rowsAt) := state k
    [("ledger", ledgerJson L devices k),
     ("audiences", toJson (audiences.map fun a =>
       audienceJson entries a.id k L (audiencesAt a.id) (rowsAt a.id)))]
  return Json.mkObj (fields entries.length ++ [
    ("slots", toJson ((List.range entries.length).map fun k =>
      Json.mkObj (("slot", toJson (k + 1)) :: fields (k + 1))))])

namespace AuthorshipExample

def config : Config 3 := ⟨"Household", [(0, .admin, 0), (2, .member, 0)], [(0, 0), (2, 2)],
  [⟨0, 1, [0, 2]⟩, ⟨1, 1, [0, 2]⟩]⟩
def policy : Rules (sig 3) := rules config (fun k => ⟨k, "authenticated"⟩)
def part (readThrough : Nat) : WPart (sig 3) := ⟨1, [], ⟨[1], [], []⟩, readThrough⟩
def sequence : Nat → Slot (sig 3)
  | 1 => .admin 0 (.other ⟨1, 2, false⟩)
  | 2 => .writes 2 0 (fun a => if a = 1 then some (part 0) else none)
  | 3 => .writes 2 0 (fun a => if a = 1 then some (part 1) else none)
  | 4 => .admin 0 (.removeDevice 2)
  | _ => .writes 2 0 (fun a => if a = 1 then some (part 0) else none)

/-- Landing-time circle permission refuses Ben's offline original even though
he authored it before reading his removal from Gifts. -/
theorem landing_permission_counterexample :
    policy.mayWrite (ledgerAt policy sequence 0) 2 1 = true ∧
    policy.mayWrite (ledgerAt policy sequence 1) 2 1 = false := by decide

theorem offline_edit_counts : AppliedTo policy sequence 1 2 := by unfold AppliedTo; decide

theorem removal_read_refused : ¬ AppliedTo policy sequence 1 3 := by unfold AppliedTo; decide

theorem device_cutoff_still_applies : ¬ AppliedTo policy sequence 1 5 := by unfold AppliedTo; decide

/-- Sending the original with the exposed circle key and receiving it agree. -/
theorem sending_and_apply_agree :
    let key : Keys.Key Nat := ⟨1, 7⟩
    let history : Keys.History Nat Nat := ⟨[⟨key, 30, 1⟩], [⟨key, 2, 1⟩]⟩
    let custody : Keys.Custody Nat := fun k => if k = key then some 30 else none
    let current : Keys.Current Nat Nat := fun a m =>
      if m = 0 then some 2 else if a = 0 ∧ m = 2 then some 1 else none
    Keys.sendingKey history custody current 2 0 1 = some ⟨key, 30, 1⟩ ∧
      AppliedTo policy sequence 1 2 := by unfold AppliedTo; decide

end AuthorshipExample
end SequenceApply
