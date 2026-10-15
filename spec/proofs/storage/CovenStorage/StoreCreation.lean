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
  accepts for two stores sharing a bucket.
- So setup claims the create-once marker `created` first (T205, D10). In
  every interleaving of any number of setups at most one creation entry is
  stored (`marked_setups_create_once`); a refused setup stores none
  (`refused_setup_no_creation`) and its claim changes nothing
  (`refused_claim_writes_nothing`); a retried claim of the stored marker
  passes (`own_marker_passes`). T163's interleaving now creates once
  (`concurrent_marked_setups_one_creation`). -/

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

/-! ### The creation marker (T205)

A listing is not a lock, so every setup instead first claims one fixed path,
`created`, with a create-once write holding its store and device, before it
writes anything else. It goes on only when the stored marker is its own: a
setup by another device refuses and writes nothing more. -/

/-- The marker a setup claims: its store and its device. -/
def marker (store device : Nat) : Bytes := [store, device]

/-- Whether the stored marker is exactly `m`. -/
def holds (s : Storage) (m : Bytes) : Bool := (s .created).map Object.bytes == some m

/-- One setup's progress, by device. -/
inductive Phase where
  | start | claimed | refused | wrote
  deriving DecidableEq, Repr

structure Run where
  storage : Storage
  phase : Nat → Phase

/-- A step of one device's setup. Steps of different setups interleave freely. -/
inductive Step where
  | claim (device time : Nat)
  | write (device time : Nat)
  deriving DecidableEq, Repr

/-- Claiming creates the marker once and reads back whose it is; writing the
creation entry happens only after a successful claim. -/
def step (store : Nat) (entry : Nat → Bytes) (r : Run) : Step → Run
  | .claim d t =>
    if r.phase d = .start then
      let s := create r.storage .created (marker store d) t
      ⟨s, fun e => if e = d then (if holds s (marker store d) then .claimed else .refused)
        else r.phase e⟩
    else r
  | .write d t =>
    if r.phase d = .claimed then
      ⟨create r.storage (.entry d 1) (entry d) t, fun e => if e = d then .wrote else r.phase e⟩
    else r

def run (store : Nat) (entry : Nat → Bytes) (r : Run) (steps : List Step) : Run :=
  steps.foldl (step store entry) r

theorem create_other (s : Storage) (p q : Path) (b : Bytes) (t : Nat) (h : q ≠ p) :
    create s p b t q = s q := by
  simp [create, h]

theorem marker_keeps_entries (s : Storage) (b : Bytes) (t e : Nat) :
    create s .created b t (.entry e 1) = s (.entry e 1) :=
  create_other s .created (.entry e 1) b t (by simp)

theorem entry_keeps_marker (s : Storage) (b : Bytes) (t d : Nat) :
    create s (.entry d 1) b t .created = s .created :=
  create_other s (.entry d 1) .created b t (by simp)

theorem entry_keeps_others (s : Storage) (b : Bytes) (t d e : Nat) (h : e ≠ d) :
    create s (.entry d 1) b t (.entry e 1) = s (.entry e 1) :=
  create_other s (.entry d 1) (.entry e 1) b t (by simp [h])

theorem create_occupied (s : Storage) (p : Path) (b : Bytes) (t : Nat) (o : Object)
    (h : s p = some o) : create s p b t = s := by
  funext q
  by_cases hq : q = p
  · subst q; simp [create, h]
  · simp [create, hq]

/-- What every reachable state satisfies: a creation entry exists only for the
device whose marker is stored, a claimed setup's marker is stored, and a
refused setup saw another marker, which stays. -/
def Inv (store : Nat) (r : Run) : Prop :=
  (∀ d, (r.storage (.entry d 1)).isSome = true → holds r.storage (marker store d) = true) ∧
  (∀ d, r.phase d = .claimed → holds r.storage (marker store d) = true) ∧
  (∀ d, r.phase d = .refused →
    (r.storage .created).isSome = true ∧ holds r.storage (marker store d) = false)

theorem step_inv (store : Nat) (entry : Nat → Bytes) (r : Run) (x : Step)
    (h : Inv store r) : Inv store (step store entry r x) := by
  obtain ⟨hA, hB, hC⟩ := h
  cases x with
  | claim d t =>
    by_cases hstart : r.phase d = .start
    · simp only [step, hstart, ↓reduceIte]
      cases hc : r.storage .created with
      | none =>
        have newMarker : create r.storage .created (marker store d) t .created =
            some ⟨marker store d, t⟩ := create_publishes _ _ _ _ hc
        have own : holds (create r.storage .created (marker store d) t) (marker store d) = true := by
          simp [holds, newMarker]
        refine ⟨?_, ?_, ?_⟩
        · intro e he
          dsimp only at he
          rw [marker_keeps_entries] at he
          have := hA e he
          simp [holds, hc] at this
        · intro e he
          by_cases hed : e = d
          · subst hed; exact own
          · simp only [hed, ↓reduceIte] at he
            have := hB e he
            simp [holds, hc] at this
        · intro e he
          by_cases hed : e = d
          · subst hed; simp [own] at he
          · simp only [hed, ↓reduceIte] at he
            have := (hC e he).1
            simp [hc] at this
      | some o =>
        have same := create_occupied r.storage .created (marker store d) t o hc
        simp only [same]
        refine ⟨hA, ?_, ?_⟩
        · intro e he
          by_cases hed : e = d
          · subst hed
            by_cases hh : holds r.storage (marker store e) = true
            · exact hh
            · simp [hh] at he
          · simp only [hed, ↓reduceIte] at he
            exact hB e he
        · intro e he
          by_cases hed : e = d
          · subst hed
            by_cases hh : holds r.storage (marker store e) = true
            · simp [hh] at he
            · exact ⟨by simp [hc], by simpa using hh⟩
          · simp only [hed, ↓reduceIte] at he
            exact hC e he
    · simp only [step, hstart, ↓reduceIte]
      exact ⟨hA, hB, hC⟩
  | write d t =>
    by_cases hclaimed : r.phase d = .claimed
    · simp only [step, hclaimed, ↓reduceIte]
      have keep : ∀ m, holds (create r.storage (.entry d 1) (entry d) t) m = holds r.storage m := by
        intro m
        simp only [holds]
        rw [entry_keeps_marker]
      refine ⟨?_, ?_, ?_⟩
      · intro e he
        dsimp only at he ⊢
        rw [keep]
        by_cases hed : e = d
        · subst hed; exact hB e hclaimed
        · rw [entry_keeps_others _ _ _ _ _ hed] at he
          exact hA e he
      · intro e he
        dsimp only at he ⊢
        rw [keep]
        by_cases hed : e = d
        · subst hed; simp at he
        · simp only [hed, ↓reduceIte] at he
          exact hB e he
      · intro e he
        dsimp only at he ⊢
        rw [keep, entry_keeps_marker]
        by_cases hed : e = d
        · subst hed; simp at he
        · simp only [hed, ↓reduceIte] at he
          exact hC e he
    · simp only [step, hclaimed, ↓reduceIte]
      exact ⟨hA, hB, hC⟩

theorem run_inv (store : Nat) (entry : Nat → Bytes) (r : Run) (steps : List Step)
    (h : Inv store r) : Inv store (run store entry r steps) := by
  induction steps generalizing r with
  | nil => exact h
  | cons x xs ih => exact ih _ (step_inv store entry r x h)

/-- A location with no creation entry; a marker may already be stored. -/
theorem fresh_inv (store : Nat) (s : Storage)
    (noEntry : ∀ d, s (.entry d 1) = none) : Inv store ⟨s, fun _ => .start⟩ := by
  refine ⟨?_, ?_, ?_⟩
  · intro d h; simp [noEntry d] at h
  · intro d h; simp at h
  · intro d h; simp at h

/-- In every interleaving of any number of setups of one store, at most one
device's creation entry is ever stored. -/
theorem marked_setups_create_once (store : Nat) (entry : Nat → Bytes) (s : Storage)
    (noEntry : ∀ d, s (.entry d 1) = none)
    (steps : List Step) (d e : Nat)
    (hd : ((run store entry ⟨s, fun _ => .start⟩ steps).storage (.entry d 1)).isSome = true)
    (he : ((run store entry ⟨s, fun _ => .start⟩ steps).storage (.entry e 1)).isSome = true) :
    d = e := by
  obtain ⟨hA, -, -⟩ := run_inv store entry _ steps (fresh_inv store s noEntry)
  have md := hA d hd
  have me := hA e he
  simp only [holds, beq_iff_eq] at md me
  rw [md] at me
  simp [marker] at me
  exact me

/-- A setup whose claim was refused stored no creation entry, in every
interleaving; its claim left storage unchanged (`refused_claim_writes_nothing`). -/
theorem refused_setup_no_creation (store : Nat) (entry : Nat → Bytes) (s : Storage)
    (noEntry : ∀ d, s (.entry d 1) = none)
    (steps : List Step) (d : Nat)
    (refused : (run store entry ⟨s, fun _ => .start⟩ steps).phase d = .refused) :
    (run store entry ⟨s, fun _ => .start⟩ steps).storage (.entry d 1) = none := by
  obtain ⟨hA, -, hC⟩ := run_inv store entry _ steps (fresh_inv store s noEntry)
  cases h : (run store entry ⟨s, fun _ => .start⟩ steps).storage (.entry d 1) with
  | none => rfl
  | some o =>
    have := hA d (by simp [h])
    simp [(hC d refused).2] at this

/-- A losing claim changes nothing: the marker is another's, and create-once
leaves it and every other object as they were. -/
theorem refused_claim_writes_nothing (s : Storage) (m : Bytes) (t : Nat) (o : Object)
    (other : s .created = some o) : create s .created m t = s :=
  create_occupied s .created m t o other

/-- A retried claim by the device whose marker is stored passes again, so a
setup that lost its reply goes on. -/
theorem own_marker_passes (s : Storage) (m : Bytes) (t : Nat)
    (own : holds s m = true) : holds (create s .created m t) m = true := by
  cases hc : s .created with
  | none => simp [holds, hc] at own
  | some o => rw [create_occupied s .created m t o hc]; exact own

/-- T163's interleaving with the marker: both setups claim before either
writes, and only Ana's phone (the first claim) creates the store. -/
theorem concurrent_marked_setups_one_creation :
    let r := run 1 (fun d => [d]) ⟨emptyStorage, fun _ => .start⟩
      [.claim 7 10, .claim 8 11, .write 7 12, .write 8 13]
    creators r.storage [7, 8] = [7] ∧ r.phase 8 = .refused := by
  decide

end CovenStorage.StoreCreation
