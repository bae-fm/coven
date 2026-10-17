import CovenStorage.Objects

/-! §20.10's fresh S3 provider principals. Minting for one database copy must not
delete a key made by another copy, including one already recorded by an
applied set-access entry. This model contains minting, recording and database
copy/loss; delivery, claim settlement, undo and key deletion are outside it. -/

namespace CovenStorage.KeyDelivery

/-- An open operation records its key after minting. -/
structure Journal where
  key : Option Nat
  deriving DecidableEq, Repr

structure Copy where
  device : Nat
  journal : Option Journal
  deriving DecidableEq, Repr

/-- The provider-side name identifies the minting device. -/
structure ProviderKey where
  id : Nat
  device : Nat
  deriving DecidableEq, Repr

/-- Array indices identify database copies. One fresh-id supply stands for
collision-free provider principals; it is not a persisted device counter. `recorded` contains keys named by applied set-access entries. -/
structure World where
  copies : Nat → Option Copy
  slots : Nat
  provider : List ProviderKey
  recorded : List Nat
  next : Nat

def World.put (w : World) (i : Nat) (c : Option Copy) : World :=
  { w with copies := fun k => if k = i then c else w.copies k }

/-- Journal an operation before minting. -/
def begin (w : World) (i : Nat) : World :=
  match w.copies i with
  | some ⟨d, none⟩ =>
    w.put i (some ⟨d, some ⟨none⟩⟩)
  | _ => w

/-- A successful mint creates a fresh principal. The member-side check before
its secret is recorded (§20.10) is not modelled. A failed mint that rolls back
has no step. -/
def mint (w : World) (i : Nat) : World :=
  match w.copies i with
  | some ⟨d, some ⟨none⟩⟩ =>
    { w with copies := fun k => if k = i then some ⟨d, some ⟨some w.next⟩⟩ else w.copies k
             provider := w.provider ++ [⟨w.next, d⟩]
             next := w.next + 1 }
  | _ => w

/-- Observe the accepted set-access entry for this operation's key. The
sequence's authority and claim checks are premises of this observation. -/
def record (w : World) (i : Nat) : World :=
  match w.copies i with
  | some ⟨d, some ⟨some k⟩⟩ =>
    { w.put i (some ⟨d, none⟩) with recorded := k :: w.recorded }
  | _ => w

/-- A backup or second copy retains the operation journal. -/
def clone (w : World) (i : Nat) : World :=
  match w.copies i with
  | some c => { w with copies := fun k => if k = w.slots then some c else w.copies k
                       slots := w.slots + 1 }
  | none => w

/-- Losing a database does not itself delete a provider key. -/
def drop (w : World) (i : Nat) : World := w.put i none

inductive Step where
  | begin (i : Nat)
  | mint (i : Nat)
  | record (i : Nat)
  | clone (i : Nat)
  | drop (i : Nat)
  deriving DecidableEq, Repr

def step (w : World) : Step → World
  | .begin i => begin w i
  | .mint i => mint w i
  | .record i => record w i
  | .clone i => clone w i
  | .drop i => drop w i

def run (w : World) (steps : List Step) : World := steps.foldl step w

def start (devices : List Nat) (next : Nat) : World :=
  ⟨fun k => devices[k]?.map (⟨·, none⟩), devices.length, [], [], next⟩

/-- Fresh-principal minting never deletes a provider key held by another copy. -/
theorem step_keeps_provider (w : World) (s : Step) :
    ∀ p ∈ w.provider, p ∈ (step w s).provider := by
  intro p hp
  cases s with
  | mint i =>
    simp only [step]; unfold mint; split
    · exact List.mem_append_left _ hp
    · exact hp
  | begin i => simp only [step]; unfold begin; split <;> exact hp
  | record i => simp only [step]; unfold record; split <;> exact hp
  | clone i => simp only [step]; unfold clone; split <;> exact hp
  | drop i => exact hp

theorem run_keeps_provider (w : World) (steps : List Step) :
    ∀ p ∈ w.provider, p ∈ (run w steps).provider := by
  induction steps generalizing w with
  | nil => intro p hp; exact hp
  | cons s ss ih => intro p hp; exact ih _ p (step_keeps_provider w s p hp)

end CovenStorage.KeyDelivery
