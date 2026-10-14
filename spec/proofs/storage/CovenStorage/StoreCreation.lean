import CovenStorage.Objects

/-! §4, §9, §10: setting up storage for a store that has never had it.

The store log's creation entry is entry 1 of the creating device (D6, D10).
After §10's storage-less reset the original installation and the reset copy
hold the same store under different device ids, so their creation entries
have different paths and create-once never compares them.

- Setup that writes its creation without reading the store folder leaves two
  creation entries when both set up one location:
  `blind_setup_two_creations`.
- Setup that first lists the store's entries and refuses one by another
  device creates the store at most once when the listings are complete
  (`listed_setup_one_creation`), and a retried creation by the same device
  still passes (`retry_passes`).
- Listing is not a lock: two setups whose listings both precede the other's
  creation both pass (`concurrent_setups_two_creations`), the same margin §4
  accepts for two stores sharing a bucket. -/

namespace CovenStorage.StoreCreation
open CovenStorage

/-- What a listing of the store's entries shows: every stored entry path. -/
def entries (s : Storage) (devices : List Nat) : List Path :=
  devices.filterMap fun d => (s (.entry d 1)).map fun _ => .entry d 1

/-- The devices whose entry 1 is stored: each one a creation. -/
def creators (s : Storage) (devices : List Nat) : List Nat :=
  devices.filter fun d => (s (.entry d 1)).isSome

/-- Setup before T163: create this device's creation entry. -/
def blindSetup (s : Storage) (device : Nat) (bytes : Bytes) (time : Nat) : Storage :=
  create s (.entry device 1) bytes time

/-- An entry by another device in the listing means the store exists here. -/
def anotherCreated (listing : List Path) (device : Nat) : Bool :=
  listing.any fun
    | .entry d _ => d != device
    | _ => false

/-- Setup after T163: refuse when the listing taken first names another
device's entry; otherwise create this device's creation entry. -/
def listedSetup (listing : List Path) (s : Storage) (device : Nat) (bytes : Bytes)
    (time : Nat) : Option Storage :=
  if anotherCreated listing device then none else some (create s (.entry device 1) bytes time)

/-- Ana's phone (device 7) and its storage-less reset copy (device 8) both set
up the same location: two creation entries. -/
theorem blind_setup_two_creations :
    creators (blindSetup (blindSetup emptyStorage 7 [1] 10) 8 [2] 11) [7, 8] = [7, 8] := by
  decide

/-- A listing complete through the first creation refuses the second. -/
theorem listed_setup_one_creation (s : Storage) (first second : Nat) (b c : Bytes) (t u : Nat)
    (distinct : first ≠ second) (devices : List Nat) (listed : first ∈ devices) :
    listedSetup (entries (create s (.entry first 1) b t) devices)
      (create s (.entry first 1) b t) second c u = none := by
  have stored : (create s (.entry first 1) b t) (.entry first 1) ≠ none := by
    cases h : s (.entry first 1) <;> simp [create, h]
  have named : anotherCreated (entries (create s (.entry first 1) b t) devices) second = true := by
    simp only [anotherCreated, List.any_eq_true]
    refine ⟨.entry first 1, ?_, by simpa using distinct⟩
    simp only [entries, List.mem_filterMap]
    obtain ⟨o, ho⟩ := Option.ne_none_iff_exists'.mp stored
    exact ⟨first, listed, by simp [ho]⟩
  simp [listedSetup, named]

/-- A retried creation that already wrote this device's entry passes, and
create-once keeps the first bytes (§4: a retried creation passes). -/
theorem retry_passes (s : Storage) (device : Nat) (b : Bytes) (t u : Nat) (devices : List Nat)
    (only : ∀ d ∈ devices, d ≠ device → s (.entry d 1) = none) :
    listedSetup (entries (create s (.entry device 1) b t) devices)
      (create s (.entry device 1) b t) device b u =
      some (create s (.entry device 1) b t) := by
  have none_other : anotherCreated (entries (create s (.entry device 1) b t) devices) device = false := by
    simp only [anotherCreated, List.any_eq_false, entries, List.mem_filterMap]
    rintro p ⟨d, hd, hp⟩
    by_cases same : d = device
    · subst same
      cases h : (create s (.entry d 1) b t) (.entry d 1)
      · simp [h] at hp
      · simp only [h, Option.map_some, Option.some.injEq] at hp
        subst hp
        simp
    · have : (create s (.entry device 1) b t) (.entry d 1) = none := by
        simp [create, same, only d hd same]
      simp [this] at hp
  simp [listedSetup, none_other, retry_preserves_time]

/-- Both setups list before either creates: both pass, two creations. -/
theorem concurrent_setups_two_creations :
    let ana := listedSetup (entries emptyStorage [7, 8]) emptyStorage 7 [1] 10
    let copy := ana.bind fun s => listedSetup (entries emptyStorage [7, 8]) s 8 [2] 11
    copy.map (creators · [7, 8]) = some [7, 8] := by
  decide

end CovenStorage.StoreCreation
