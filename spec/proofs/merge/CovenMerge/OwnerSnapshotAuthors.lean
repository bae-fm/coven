import CovenMerge.Ownership

/-! Which authorship a snapshot must prove (§8.8, §15, D7 section 7). A
snapshot carries another member's write evidence for every owned creation and
present owned cell it attributes to that member. Writes of the snapshot's own
device, and of the signer's other devices once the signer's member is known,
are its signer's and carry none.

* `own_writes_signers`: a write that needs no evidence has the snapshot's
  signer as its author. This holds across key replacement: a member's id, not
  a key, owns rows, and a device belongs to one member for good.
* `owner_true`: the owner a loader computes for an accepted creation is its
  write's true author, and unless that owner is the signer, the creation leaf
  for this very row was opened from the write's signed body. A snapshot cannot
  name another member as an owned row's creator without them. -/
namespace CovenMerge.Ownership.SnapshotAuthors

open CovenStorage.RowAuthorship

/-- Who signed what. Registration fixes each device's member, and a device id
is never registered again (§10). Replacing a member's keys binds a fresh pair
to the same id; every historical binding is kept (§9). -/
structure Identities (WriteId Device Member Key : Type) where
  device : WriteId → Device
  member : Device → Member
  bound : Key → Option Member
  /-- The key a write's signature verified with: its device member's key
  current in the write's recorded view (D9). -/
  signedWith : WriteId → Key
  write_signed : ∀ w, bound (signedWith w) = some (member (device w))

/-- What a loader knows about a snapshot's author: the device in its path,
the key its signature verified with, and that device's member once the
device's registration is applied. -/
structure Signer (Device Member Key : Type) where
  device : Device
  member : Option Member
  key : Key

variable {WriteId Device Member Key : Type} [DecidableEq Device] [DecidableEq Member]

/-- The snapshot's signature verified with its device member's key, and a
known member is that device's. -/
structure Signer.Valid (I : Identities WriteId Device Member Key)
    (a : Signer Device Member Key) : Prop where
  signed : I.bound a.key = some (I.member a.device)
  known : ∀ m, a.member = some m → m = I.member a.device

/-- The loader's replayed device table agrees with registration. -/
def Registry (I : Identities WriteId Device Member Key) (registered : Device → Option Member) :
    Prop :=
  ∀ d m, registered d = some m → m = I.member d

/-- D7 section 7: a write needs evidence unless it is from the snapshot's own
device or, when the signer's member is known, from a device of that member. -/
def other (I : Identities WriteId Device Member Key) (registered : Device → Option Member)
    (a : Signer Device Member Key) (w : WriteId) : Bool :=
  decide (I.device w ≠ a.device) && (a.member.isNone || decide (registered (I.device w) ≠ a.member))

/-- The owner a loader computes for a creation made by `w`: the member of
that write's device. -/
def owner (I : Identities WriteId Device Member Key) (w : WriteId) : Member :=
  I.member (I.device w)

/-- **Own writes are the signer's.** A write that needs no evidence has the
snapshot's signer as its author, and its signing key binds that same member,
also when it differs from the snapshot's key after a replacement. -/
theorem own_writes_signers (I : Identities WriteId Device Member Key)
    (registered : Device → Option Member) (registry : Registry I registered)
    (a : Signer Device Member Key) (valid : a.Valid I) (w : WriteId)
    (exempt : other I registered a w = false) :
    owner I w = I.member a.device ∧ I.bound (I.signedWith w) = I.bound a.key := by
  have same : owner I w = I.member a.device := by
    simp only [other, Bool.and_eq_false_iff, decide_eq_false_iff_not, Decidable.not_not,
      Bool.or_eq_false_iff, Option.isNone_eq_false_iff, Option.isSome_iff_exists] at exempt
    rcases exempt with own | ⟨⟨m, known⟩, mine⟩
    · simp [owner, own]
    · rw [known] at mine
      rw [owner, ← registry _ m mine, valid.known m known]
  exact ⟨same, by rw [I.write_signed, valid.signed, ← same, owner]⟩

/-- What a loader accepts for a creation of `key` attributed to write `w`:
another member's write must come with its signed opening of that creation. -/
def Accepts {K Payload Digest Signature : Type}
    (h : Hashing (CovenStorage.RowAuthorship.Change K Payload) Digest)
    (s : Signatures Member WriteId (CovenStorage.RowAuthorship.Change K Payload) Digest Signature h)
    (I : Identities WriteId Device Member Key) (registered : Device → Option Member)
    (a : Signer Device Member Key) (key : K) (w : WriteId)
    (evidence : Option (Signature × Proof (CovenStorage.RowAuthorship.Change K Payload) Digest)) : Prop :=
  other I registered a w = true → ∃ signature proof, evidence = some (signature, proof) ∧
    AcceptsCreator h s key (owner I w) w signature proof

/-- **A snapshot cannot invent another member's authorship.** The owner a
loader computes is the write's true author. Unless that owner is the
snapshot's signer, the write's signed body contains this row's creation. -/
theorem owner_true {K Payload Digest Signature : Type}
    (h : Hashing (CovenStorage.RowAuthorship.Change K Payload) Digest)
    (s : Signatures Member WriteId (CovenStorage.RowAuthorship.Change K Payload) Digest Signature h)
    (I : Identities WriteId Device Member Key) (authors : ∀ w, s.author w = owner I w)
    (registered : Device → Option Member) (registry : Registry I registered)
    (a : Signer Device Member Key) (valid : a.Valid I) (key : K) (w : WriteId)
    (evidence : Option (Signature × Proof (CovenStorage.RowAuthorship.Change K Payload) Digest))
    (accepted : Accepts h s I registered a key w evidence) :
    owner I w = s.author w ∧
      (owner I w ≠ I.member a.device →
        ∃ original, s.signed w original ∧ ∃ generation, CovenStorage.RowAuthorship.Change.creation key generation ∈ leaves original) := by
  refine ⟨(authors w).symm, fun notSigner => ?_⟩
  have needed : other I registered a w = true := by
    cases hx : other I registered a w
    · exact absurd (own_writes_signers I registered registry a valid w hx).1 notSigner
    · rfl
  obtain ⟨signature, proof, _, opened⟩ := accepted needed
  exact (owned_row_true_author h s key (owner I w) w signature proof opened).2

namespace Replaced

/-! Carol's device 1 wrote write 0 with her first key 10. After she replaced
her keys, device 1's snapshot verifies with key 11. Ana's device 2 wrote
write 1 with key 20. -/

def identities : Identities Nat Nat Nat Nat where
  device w := if w = 1 then 2 else 1
  member d := if d = 2 then 0 else 1
  bound k := if k = 10 ∨ k = 11 then some 1 else if k = 20 then some 0 else none
  signedWith w := if w = 1 then 20 else 10
  write_signed w := by by_cases h : w = 1 <;> simp [h]

def snapshot : Signer Nat Nat Nat := ⟨1, some 1, 11⟩

theorem valid : snapshot.Valid identities :=
  ⟨by decide, fun m h => by cases h; rfl⟩

def registered (d : Nat) : Option Nat := some (identities.member d)

/-- Carol's own write needs no evidence despite the new key; Ana's does. -/
theorem own_write_after_replacement :
    other identities registered snapshot 0 = false ∧
    identities.signedWith 0 ≠ snapshot.key ∧
    owner identities 0 = identities.member snapshot.device ∧
    other identities registered snapshot 1 = true := by decide

end Replaced

end CovenMerge.Ownership.SnapshotAuthors
