import CovenStorelog.CurrentKeys

/-! §11's store-log, positions and clock sealing path. Key choice does not
check exposure. Entry authority and the send gates remain caller obligations.
Opening abstracts correct encryption to possession of the exact key bytes. -/
namespace CovenStorelog.EntryKeys
open CurrentKeys
open CurrentReplay (KeyBytes KeyHash)

/-- Only received, accepted store keys compete; copy exposure is irrelevant. -/
def select (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) : Option Introduction :=
  newest (is.filter fun i => i.audience == .store && holdsAccepted hash is held i)

theorem selected_accepted (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (i : Introduction) (selected : select hash is held = some i) :
    i ∈ is ∧ i.audience = .store ∧ holdsAccepted hash is held i = true := by
  obtain ⟨mem, good⟩ := List.mem_filter.mp (newest_mem _ _ selected)
  have good := Bool.and_eq_true_iff.mp good
  exact ⟨mem, by simpa using good.1, good.2⟩

theorem selected_newest (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (i : Introduction) (selected : select hash is held = some i)
    (other : Introduction) (mem : other ∈ is) (store : other.audience = .store)
    (accepted : holdsAccepted hash is held other = true) :
    other.entry ≤ i.entry ∧ (other.entry = i.entry → other.key.id ≤ i.key.id) :=
  newest_maximal _ _ selected other (by simp [mem, store, accepted])

/-- Any accepted store key in custody suffices, even if every key is exposed. -/
theorem selection_available (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (p : Payload) (mem : p ∈ held) (store : p.key.audience = .store)
    (accepted : accepts hash is p.key p.bytes = true) :
    ∃ i, select hash is held = some i := by
  have committed : hashFor is p.key = some (hash p.bytes) := by
    simpa [accepts] using accepted
  obtain ⟨i, hi, identity, _⟩ := hashFor_source _ _ _ committed
  have audience : i.audience = .store := by
    simpa [Introduction.identity] using (congrArg KeyId.audience identity).trans store
  have good : holdsAccepted hash is held i = true :=
    List.any_eq_true.mpr ⟨p, mem, by simp [acceptedHeld, identity, accepted]⟩
  have candidate : i ∈ is.filter (fun i => i.audience == .store && holdsAccepted hash is held i) :=
    by simp [hi, audience, good]
  cases h : select hash is held with
  | none => simp [(newest_none _).mp h] at candidate
  | some j => exact ⟨j, rfl⟩

/-- The entry case contains the actual action, including removals and rotations.
Positions and clock bodies are abstract: their contents do not affect key choice. -/
inductive Metadata where
  | entry (value : CurrentReplay.Entry)
  | positions
  | clock
  deriving DecidableEq, Repr

/-- A mathematical sealing record, not a wire object containing its secret. -/
structure Sealed where
  object : Metadata
  key : Payload
  deriving DecidableEq, Repr

def sealMetadata (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (object : Metadata) : Option Sealed := do
  let i ← select hash is held
  let p ← held.find? (acceptedHeld hash is i)
  pure ⟨object, p⟩

theorem seal_selected (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (object : Metadata) (i : Introduction)
    (selected : select hash is held = some i) :
    ∃ p ∈ held, p.key = i.identity ∧ accepts hash is p.key p.bytes = true ∧
      sealMetadata hash is held object = some ⟨object, p⟩ := by
  have good := (selected_accepted _ _ _ _ selected).2.2
  have found : (held.find? (acceptedHeld hash is i)).isSome = true := by
    simpa [List.isSome_find?, holdsAccepted] using good
  cases hf : held.find? (acceptedHeld hash is i) with
  | none => simp [hf] at found
  | some p =>
    have checked := List.find?_some hf
    have checks := Bool.and_eq_true_iff.mp checked
    exact ⟨p, List.mem_of_find?_eq_some hf, by simpa using checks.1, checks.2,
      by simp [sealMetadata, selected, hf]⟩

/-- In particular, an authorized remaining member can seal any removal or
rotation with its retained store key. This is availability of sealing, not a
claim that an unauthorized action is kept or that its upload succeeds. -/
theorem metadata_sealing_available (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (p : Payload) (mem : p ∈ held) (store : p.key.audience = .store)
    (accepted : accepts hash is p.key p.bytes = true) (object : Metadata) :
    ∃ sealed, sealMetadata hash is held object = some sealed := by
  obtain ⟨i, selected⟩ := selection_available hash is held p mem store accepted
  obtain ⟨q, _, _, _, sealed⟩ := seal_selected hash is held object i selected
  exact ⟨⟨object, q⟩, sealed⟩

def firstAttempt (H : CurrentReplay.History) (W n : Nat) (S : EntrySet)
    (hash : KeyBytes → KeyHash) (held : List Payload) (object : Metadata) : Option Sealed :=
  sealMetadata hash (receivedIntroductions H W n S) held object

theorem sealing_checked (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (object : Metadata) (sealed : Sealed)
    (result : sealMetadata hash is held object = some sealed) :
    sealed.object = object ∧ sealed.key ∈ held ∧
      ∃ i ∈ is, select hash is held = some i ∧ i.audience = .store ∧
        sealed.key.key = i.identity ∧ accepts hash is sealed.key.key sealed.key.bytes = true := by
  cases hs : select hash is held with
  | none => simp [sealMetadata, hs] at result
  | some i =>
    obtain ⟨p, mem, identity, accepted, sealed⟩ := seal_selected hash is held object i hs
    rw [sealed] at result
    cases result
    obtain ⟨hi, store, _⟩ := selected_accepted _ _ _ _ hs
    exact ⟨rfl, mem, i, hi, rfl, store, identity, accepted⟩

theorem first_attempt_authorized (H : CurrentReplay.History) (W n : Nat) (S : EntrySet)
    (hash : KeyBytes → KeyHash) (held : List Payload) (object : Metadata) (sealed : Sealed)
    (result : firstAttempt H W n S hash held object = some sealed) :
    ∃ i : Introduction, i.entry < n ∧ S i.entry = true ∧
      CurrentReplay.authorized (CurrentReplay.authorViews H W n n i.entry) (H.log i.entry) = true ∧
      (H.log i.entry).action.introduction = some (.store, i.key) ∧
      sealed.key.key = i.identity ∧ accepts hash (receivedIntroductions H W n S)
        sealed.key.key sealed.key.bytes = true := by
  obtain ⟨_, _, i, hi, _, store, identity, accepted⟩ := sealing_checked _ _ _ _ _ result
  obtain ⟨mem, auth, intro⟩ := (introduction_source _ _ _ _).mp hi
  obtain ⟨lt, received⟩ := List.mem_filter.mp mem
  exact ⟨i, List.mem_range.mp lt, received, auth, by simpa [store] using intro, identity, accepted⟩

/-- Ex-members need the bytes, not permission from current replay. Knowledge
includes retained keys and any later disclosure. Key ids alone do not open. -/
def canOpen (known : List KeyBytes) (sealed : Sealed) : Bool :=
  sealed.key.bytes ∈ known

/-- Storage cutoff prevents obtaining a new ciphertext, not opening one already
obtained. The provider's confirmed cutoff supplies `available = false`. -/
def canRead (known : List KeyBytes) (available : Bool) (sealed : Sealed) : Bool :=
  available && canOpen known sealed

theorem readable_iff (known : List KeyBytes) (available : Bool) (sealed : Sealed) :
    canRead known available sealed = true ↔ available = true ∧ sealed.key.bytes ∈ known := by
  simp [canRead, canOpen]

theorem cutoff_prevents_new_read (known : List KeyBytes) (sealed : Sealed) :
    canRead known false sealed = false := rfl

/-- Knowing an accepted copy of the selected key suffices for every metadata
kind. Neither removal nor later sealing changes those bytes. -/
theorem accepted_key_opens (hash : KeyBytes → KeyHash)
    (injective : ∀ a b, hash a = hash b → a = b) (is : List Introduction)
    (held : List Payload) (object : Metadata) (sealed : Sealed)
    (result : sealMetadata hash is held object = some sealed)
    (known : List KeyBytes) (p : Payload) (retained : p.bytes ∈ known)
    (same : p.key = sealed.key.key) (accepted : accepts hash is p.key p.bytes = true) :
    canOpen known sealed = true := by
  obtain ⟨_, _, _, _, _, _, _, checked⟩ := sealing_checked _ _ _ _ _ result
  rw [same] at accepted
  have bytes := accepted_bytes_unique hash injective is _ _ _ accepted checked
  simpa [canOpen, bytes] using retained

theorem unknown_key_unreadable (known : List KeyBytes) (available : Bool) (sealed : Sealed)
    (unknown : sealed.key.bytes ∉ known) : canRead known available sealed = false := by
  simp [canRead, canOpen, unknown]

end CovenStorelog.EntryKeys
