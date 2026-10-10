import CovenQueue.Model

namespace CovenQueue

structure Key where
  id : Nat
  introduced : Nat
  authorized : Bool
  held : Bool
  /-- Deliveries this sending device has learned, including dropped entries. -/
  knownRecipients : List Nat
  deriving DecidableEq, Repr

structure KeyRequest where
  /-- Zero denotes the store; positive numbers denote circles. -/
  audience : Nat
  currentMember : Bool
  madeBeforeRemoval : Bool
  excludedMembers : List Nat
  retiredKeys : List Nat
  candidates : List Key
  deriving DecidableEq, Repr

def usable (r : KeyRequest) (k : Key) : Bool :=
  k.authorized && k.held && !(k.id ∈ r.retiredKeys) &&
    !(k.knownRecipients.any fun m => m ∈ r.excludedMembers)

def newer (a b : Key) : Bool :=
  a.introduced < b.introduced || (a.introduced == b.introduced && a.id < b.id)

def newest : List Key → Option Key
  | [] => none
  | k :: ks => match newest ks with
    | none => some k
    | some x => some (if newer k x then x else k)

/-- §11 selects an unexposed key for current members. §6 and §14.6 explicitly
allow a departed circle member's untried pre-removal edits under a held key. -/
def chooseKey (r : KeyRequest) : Option Key :=
  if r.currentMember then newest (r.candidates.filter (usable r))
  else if r.audience ≠ 0 ∧ r.madeBeforeRemoval then
    newest (r.candidates.filter fun k => k.authorized && k.held)
  else none

def chooseKeys : List KeyRequest → Option (List Key)
  | [] => some []
  | r :: rs => do
    let k ← chooseKey r
    let ks ← chooseKeys rs
    pure (k :: ks)

theorem newest_member (ks : List Key) (k : Key) (h : newest ks = some k) : k ∈ ks := by
  induction ks with
  | nil => simp [newest] at h
  | cons a rest ih =>
    simp only [newest] at h
    split at h
    · cases h; exact List.mem_cons_self
    · rename_i x hx
      split at h
      · cases h; exact List.mem_cons_of_mem _ (ih hx)
      · cases h; exact List.mem_cons_self

theorem first_key_usable (r : KeyRequest) (k : Key) (hm : r.currentMember = true)
    (hk : chooseKey r = some k) : usable r k = true := by
  simp only [chooseKey, hm, ite_true] at hk
  exact (List.mem_filter.mp (newest_member _ _ hk)).2

theorem first_key_excludes_known_recipient (r : KeyRequest) (k : Key) (m : Nat)
    (hm : r.currentMember = true) (he : m ∈ r.excludedMembers)
    (hk : chooseKey r = some k) : m ∉ k.knownRecipients := by
  have hu := first_key_usable r k hm hk
  simp only [usable, Bool.and_eq_true, Bool.not_eq_true', List.any_eq_false] at hu
  intro hd
  have := hu.2 m hd
  simp [he] at this

/-- The stronger physical-delivery claim needs complete exposure knowledge.
The model does not identify unknown deliveries with known deliveries. -/
theorem first_key_never_delivered (r : KeyRequest) (k : Key) (m : Nat)
    (delivered : Key → Nat → Prop)
    (complete : ∀ key ∈ r.candidates, delivered key m → m ∈ key.knownRecipients)
    (hm : r.currentMember = true) (he : m ∈ r.excludedMembers)
    (hk : chooseKey r = some k) : ¬ delivered k m := by
  have member : k ∈ r.candidates := by
    simp only [chooseKey, hm, ite_true] at hk
    exact (List.mem_filter.mp (newest_member _ _ hk)).1
  intro hd
  exact first_key_excludes_known_recipient r k m hm he hk (complete k member hd)

theorem retired_never_selected (r : KeyRequest) (k : Key)
    (hm : r.currentMember = true) (hk : chooseKey r = some k) :
    k.id ∉ r.retiredKeys := by
  have hu := first_key_usable r k hm hk
  simp only [usable, Bool.and_eq_true, Bool.not_eq_true', decide_eq_false_iff_not] at hu
  exact hu.1.2

/-- Exposure knowledge survives replay reversals (§11). -/
def rememberExposure (r : KeyRequest) (keys : List Nat) : KeyRequest :=
  { r with retiredKeys := r.retiredKeys ++ keys }

theorem exposure_persists (r : KeyRequest) (keys : List Nat) (k : Nat)
    (h : k ∈ r.retiredKeys) : k ∈ (rememberExposure r keys).retiredKeys :=
  List.mem_append_left _ h

end CovenQueue
