import CovenSequence.QueueReload
import CovenSequence.ReaderExamples

/-! Counterexamples to treating a commit during installation as a commit
after installation. These use the gate-free `queueApp`, not the refusal to
prove its own necessity. Reset and replacement use the sequence admission
rules; key checks project SQLite's primary-key check to a list of keys.
Neither SQL nor cryptography is proved here. -/
namespace CovenSequence.QueueReloadExamples
open QueueReload
open ReaderExamples (sig rules ledger0 aud0 part)

def unclaimed : Write sig Nat → Option Nat := fun _ => none
def admitted : Unit → Write sig Nat → Bool := fun _ _ => true
def bytes (key : Nat) : Unit → Option Nat := fun _ => some key

def appState (through : Nat) (rows : List Nat) : State sig Nat :=
  ⟨⟨through, 1, []⟩, [()], fun _ => rows⟩

def plan (reason : Reason) : Plan sig := ⟨3, [()], fun _ => some reason⟩

def loaded (R : Rules sig) (history : Nat → Slot sig) (through : Nat) : Reader sig :=
  ⟨through, ledgerAt R history through, audAt R history () through,
    rowsAt R history () through, fun _ => none⟩

def resetHistory : Nat → Slot sig
  | 2 => .prepare () () .reset 1 1
  | 3 => .boundary () () .reset 1 1
  | _ => .writes () 0 (fun _ => none)

def resetBefore := appState 2 []
def resetLoaded := loaded rules resetHistory 3
def resetQueued := queueApp resetBefore () 10 0 (bytes 7) (fun _ => [7])
def resetAfter := Causality.author (Causality.catchUp resetBefore.authored 3) () 10 0 (bytes 7)

/-- A valid reset at 3 is unread at commit. Its new write is retained once
but has no row effect; writing the same value after installation applies it.
No membership, key, conversion or contention complication is needed. -/
theorem reset_counterexample :
    AppliedTo rules resetHistory () 3 ∧
    (resetLoaded.aud.lastReset = 3) ∧
    (install rules unclaimed admitted resetQueued (plan .reset) (fun _ => resetLoaded)).authored.queue =
      resetQueued.authored.queue ∧
    resetQueued.authored.queue.length = 1 ∧
    rebuild rules unclaimed admitted () resetLoaded resetQueued.authored.queue = [] ∧
    rebuild rules unclaimed admitted () resetLoaded [resetAfter] = [7] := by
  refine ⟨by unfold AppliedTo; decide, rfl, rfl, rfl, by decide +kernel, by decide +kernel⟩

/-- Only key presence is modeled, as required to refute equality of the
commit verdict. Merge is irrelevant once INSERT would fail locally. -/
def freshKey (rows : List Nat) (key : Nat) : Bool := !rows.contains key

/-- A data-changing migration derives key 7 from source row 42. A device
which migrated before receiving 42 did not create 7; receiving 42 afterward
does not rerun its migration. Both devices now run schema 1. -/
def raiseRules : Rules sig :=
  { rules with migrate := fun _ rows => if 42 ∈ rows then rows ++ [7] else rows }

def raiseHistory : Nat → Slot sig
  | 1 => .writes () 0 (part 42)
  | 2 => .prepare () () (.raise 1) 1 1
  | 3 => .boundary () () (.raise 1) 1 1
  | _ => .writes () 0 (fun _ => none)

def raiseBefore := appState 2 [42]
def raiseLoaded := loaded raiseRules raiseHistory 3
def raiseQueued := queueApp raiseBefore () 10 1 (bytes 7) (fun _ => [42, 7])

/-- Even a new-schema original passes a key check before adoption that fails
against the selected migration's rows. Deferring only merge cannot turn the
successful app transaction into the transaction it would execute afterward. -/
theorem breaking_raise_counterexample :
    AppliedTo raiseRules raiseHistory () 3 ∧
    raiseRules.migrate 1 [] = [] ∧
    raiseRules.merge (raiseRules.migrate 1 []) (raiseRules.convert 0 1 42) = [42] ∧
    raiseLoaded.aud.version = 1 ∧ raiseLoaded.rows = [42, 7] ∧
    (raiseQueued.authored.queue.map (·.schema)) = [1] ∧
    permission raiseRules freshKey (ledgerAt raiseRules raiseHistory 2) (raiseBefore.rows ()) () () 7 = true ∧
    permission raiseRules freshKey raiseLoaded.ledger raiseLoaded.rows () () 7 = false ∧
    (reapplied unclaimed admitted () raiseLoaded raiseQueued.authored.queue).length = 1 := by
  refine ⟨by unfold AppliedTo; decide, rfl, rfl, rfl, by decide +kernel, rfl, rfl, by decide +kernel, by decide +kernel⟩

/-- Two members: false already reads the circle, true is added by `other`.
Rows and keys are projected independently from that membership change. -/
abbrev circleSig : Sig where
  Dev := Bool
  Digest := Nat
  Mem := Bool
  Key := Unit
  Path := Nat
  Batch := Nat
  Rows := List Nat
  Op := Unit
  Aux := Bool
  Aud := Unit

def circleLedger : Ledger circleSig where
  name := "Household"
  member := fun _ => true
  retired := fun _ => false
  role := fun _ => .admin
  deviceOf := some
  removed := fun _ => false
  keyGen := fun _ => 0
  aux := false
  copies := []
  refused := fun _ => false
  claims := fun _ => none
  writeHistory := fun _ => none

def circleAud : AudLedger circleSig :=
  ⟨fun _ => none, [], 0, 0, 0, [], 0, fun _ => false⟩

def circleRules : Rules circleSig where
  authorized := fun _ _ _ => true
  applyOp := fun _ _ => true
  copyOk := fun _ _ _ _ => true
  mayWrite := fun L d _ => !d || L.aux
  merge := rules.merge
  convert := rules.convert
  migrate := rules.migrate
  resetRows := rules.resetRows
  boundaryAllowed := fun _ _ _ _ => true
  sinceReset := rules.sinceReset
  number := fun k _ => k
  digest := fun k _ => k
  ledger0 := circleLedger
  aud0 := fun _ => circleAud
  rows0 := fun _ => []

def circleHistory : Nat → Slot circleSig
  | 1 => .writes false 0 (fun _ => some ⟨0, [], 7, 0⟩)
  | 2 => .admin false (.other ())
  | _ => .writes false 0 (fun _ => none)

def circleBefore : State circleSig Nat := ⟨⟨1, 1, []⟩, [], fun _ => []⟩
def circleLoaded : Reader circleSig :=
  ⟨2, ledgerAt circleRules circleHistory 2, audAt circleRules circleHistory () 2,
    rowsAt circleRules circleHistory () 2, fun _ => none⟩
def circleQueued := queueApp circleBefore true 10 0 (bytes 8) (fun _ => [8])

/-- A fresh key is writable only after the membership entry installs. The
old authored view cannot authorize it. Even projecting the new membership
early cannot make key checks against the unloaded rows equivalent. -/
theorem newly_readable_counterexample :
    Applied circleRules circleHistory 2 ∧ circleLoaded.rows = [7] ∧
    permission circleRules freshKey (ledgerAt circleRules circleHistory 1) [] true () 8 = false ∧
    permission circleRules freshKey circleLoaded.ledger circleLoaded.rows true () 8 = true ∧
    permission circleRules freshKey circleLoaded.ledger [] true () 7 = true ∧
    permission circleRules freshKey circleLoaded.ledger circleLoaded.rows true () 7 = false ∧
    (circleQueued.authored.queue.map (·.readThrough)) = [1] := by
  unfold Applied
  decide +kernel

def replacementHistory : Nat → Slot sig
  | 3 => .admin () (.replaceKey ())
  | _ => .writes () 0 (fun _ => none)

def replacementQueued := queueApp (appState 2 []) () 10 0 (bytes 7) (fun _ => [7])
def replacementOriginal := Causality.author (appState 2 []).authored () 10 0 (bytes 7)
def replacementAfter := Causality.author (appState 3 []).authored () 10 0 (bytes 7)

/-- Before replacement the original authenticates under generation zero.
After replacement that key is cut off, while generation one cannot
authenticate its unchanged authored prefix. A post-binding write passes.
Row permissions remain identical, isolating the credential conflict. -/
theorem self_replacement_counterexample :
    Applied rules replacementHistory 3 ∧
    replacementQueued.authored.queue = [replacementOriginal] ∧
    canSendOriginal rules replacementHistory 2 replacementOriginal 0 = true ∧
    (∀ g, canSendOriginal rules replacementHistory 3 replacementOriginal g = false) ∧
    canSendOriginal rules replacementHistory 3 replacementAfter 1 = true ∧
    permission rules freshKey (ledgerAt rules replacementHistory 2) [] () () 7 =
      permission rules freshKey (ledgerAt rules replacementHistory 3) [] () () 7 := by
  refine ⟨by unfold Applied; decide, rfl, rfl, ?_, rfl, rfl⟩
  intro g
  exact changed_binding_cannot_send rules replacementHistory 3 replacementOriginal () rfl rfl
    (by decide) g

/-- Applying a later replacement cannot authorize a queued original under
the intervening binding either. Draining a fixed old prefix does not solve
the authentication of the new tail. -/
theorem later_replacement_counterexample :
    let history := fun k => if k = 4 then Slot.admin () (.replaceKey ()) else replacementHistory k
    ∀ g, canSendOriginal rules history 4 replacementAfter g = false := by
  intro history g
  exact changed_binding_cannot_send rules history 4 replacementAfter () rfl rfl (by decide) g

end CovenSequence.QueueReloadExamples
