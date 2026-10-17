import CovenSequence.Keys

/-! Counterexamples in the key projection to stronger catch-up and authoring
claims. They do not establish reachability through the full publication protocol. -/
namespace CovenSequence.KeysExamples
open Keys

def old : Key Nat := ⟨0, 9⟩
def fresh : Key Nat := ⟨0, 1⟩
def held : Custody Nat := fun k => if k = old then some 10 else if k = fresh then some 20 else none
def current : Current Nat Nat := fun _ m => if m = 1 then some 2 else none

def slots : Nat → Event Nat Nat
  | 1 => .introduce old 10
  | 2 => .copy ⟨old, 2, 1⟩
  | 3 => .copy ⟨fresh, 1, 2⟩
  | 4 => .introduce fresh 20
  | _ => .other

theorem tentative_not_accepted :
    accepted (historyAt slots 3) held fresh = false ∧
    firstKey (historyAt slots 3) held current 1 0 = none := by decide

theorem introduction_enables_first_attempt :
    firstKey (historyAt slots 4) held current 1 0 = some ⟨fresh, 20, 4⟩ := by decide

theorem exposed_metadata_still_publishable :
    exposed (historyAt slots 2) current old = true ∧
    metadataKey (historyAt slots 2) held 0 = some ⟨old, 10, 1⟩ := by decide

/-- The slot after a completed rotation can carry previously attempted bytes
under the old key. Rotation does not retroactively hide those bytes. -/
theorem retry_readable_after_rotation :
    firstKey (historyAt slots 4) held current 1 0 = some ⟨fresh, 20, 4⟩ ∧
    observe (exposed (historyAt slots 4) current) old 42 = some 42 := by decide

/-- An unobserved in-flight copy can expose the key chosen at the last read.
At the next read this same key is rejected for new first attempts. -/
theorem late_copy_exposes_selected_key :
    let before := historyAt slots 4
    let after := step before 5 (.copy ⟨fresh, 2, 1⟩)
    firstKey before held current 1 0 = some ⟨fresh, 20, 4⟩ ∧
    observe (exposed after current) fresh 42 = some 42 ∧
    firstKey after held current 1 0 = none := by decide

/-- Even a recipient never admitted to the audience counts as exposure. -/
theorem unbound_recipient_exposes_key :
    exposed (step (historyAt slots 4) 5 (.copy ⟨fresh, 3, 1⟩)) current fresh = true := by decide

theorem departed_circle_original_can_use_exposed_key :
    let key : Key Nat := ⟨1, 7⟩
    let h : History Nat Nat := ⟨[⟨key, 30, 1⟩], [⟨key, 2, 1⟩]⟩
    let custody : Custody Nat := fun k => if k = key then some 30 else none
    exposed h current key = true ∧
    firstKey h custody current 2 1 = none ∧
    sendingKey h custody current 2 0 1 = some ⟨key, 30, 1⟩ := by decide

/-- Removing one installation does not retire the member's shared credential.
A stolen installation requires credential replacement to exclude that secret. -/
theorem device_removal_keeps_credentials {S : Sig} (R : Rules S) (L : Ledger S)
    (reads : S.Aud → S.Mem → Bool) (d : S.Dev) :
    credentials (applyEntry R L (.removeDevice d)) reads = credentials L reads := rfl

end CovenSequence.KeysExamples
