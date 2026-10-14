import CovenStorelog.EntryKeys
import CovenStorelog.CurrentKeyExamples

/-! What a key sealing store-log entries, positions and clock objects reveals
(§11, §19.1). Opening one of them yields its contents. Entries name keys only
by `KeyCommitment` (id and hash), never by bytes. Positions also carry
fingerprints: keyed digests of an audience's app data. A reader holding a
fingerprint's key can test guesses of that data; one without it learns
nothing, in this abstraction of a keyed hash. -/
namespace CovenStorelog.PositionFingerprints
open CurrentKeys
open CurrentReplay (KeyBytes KeyHash)

/-- The newest received introduction for an audience whose bytes the device
holds and accepts, ignoring exposure. -/
def readable (hash : KeyBytes → KeyHash) (is : List Introduction) (held : List Payload)
    (audience : Audience) : Option Introduction :=
  newest (is.filter fun i => i.audience == audience && holdsAccepted hash is held i)

/-- The metadata envelope's store key is the store's readable key. -/
theorem envelope_readable (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) : EntryKeys.select hash is held = readable hash is held .store := rfl

/-- A fingerprint keeps the key it was computed with and the state it digests. -/
structure Fingerprint (σ : Type) where
  audience : Audience
  key : Payload
  state : σ

/-- With the key, a reader digests a candidate state and compares it with the
posted value. Without the key, the posted value answers nothing. -/
def test {σ : Type} [DecidableEq σ] (known : List KeyBytes) (f : Fingerprint σ)
    (candidate : σ) : Option Bool :=
  if f.key.bytes ∈ known then some (decide (f.state = candidate)) else none

def fingerprintWith {σ : Type} (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (audience : Audience) (state : σ) (i : Introduction) :
    Option (Fingerprint σ) :=
  (held.find? (acceptedHeld hash is i)).map fun p => ⟨audience, p, state⟩

/-- The reading of §19.1 that §4 of the sync pass names: the audience's usable
key, as C12 selects it for app data. A selection wait omits the fingerprint. -/
def usableFingerprint {σ : Type} (hash : KeyBytes → KeyHash) (is : List Introduction)
    (s : State) (self : Nat) (listing : Listing) (held : List Payload)
    (audience : Audience) (state : σ) : Option (Fingerprint σ) :=
  match select hash is s self listing held audience with
  | .use i => fingerprintWith hash is held audience state i
  | .pending _ => none

/-- The other reading: the newest key this device reads the audience with. -/
def readableFingerprint {σ : Type} (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (audience : Audience) (state : σ) : Option (Fingerprint σ) :=
  (readable hash is held audience).bind (fingerprintWith hash is held audience state)

/-- Key bytes a reader obtains: every listed copy addressed to it that it
opens and accepts. Opening metadata adds none, since entries carry only
commitments. -/
def copyKnowledge (hash : KeyBytes → KeyHash) (is : List Introduction)
    (copies : List Copy) (reader : Nat) : List KeyBytes :=
  copies.filterMap fun c => (acceptedCopy hash is reader c).map (·.bytes)

def coalitionKnowledge (hash : KeyBytes → KeyHash) (is : List Introduction)
    (copies : List Copy) (readers : List Nat) : List KeyBytes :=
  readers.flatMap (copyKnowledge hash is copies)

/-- Fresh keys: different key identities commit to different hashes. -/
def Fresh (is : List Introduction) : Prop :=
  ∀ a ∈ is, ∀ b ∈ is, a.identity ≠ b.identity → a.key.keyHash ≠ b.key.keyHash

/-- No group of members excluded from an audience obtains, from this pass's
listed copies, the bytes of the key C12 selects for it. A copy under another
key id would need equal bytes, which freshness rules out without assuming the
hash is injective. -/
theorem selected_bytes_unknown (hash : KeyBytes → KeyHash) (is : List Introduction)
    (fresh : Fresh is) (s : State) (self : Nat) (copies : List Copy) (held : List Payload)
    (audience : Audience) (i : Introduction)
    (selected : select hash is s self (.complete copies) held audience = .use i)
    (p : Payload) (accepted : acceptedHeld hash is i p = true)
    (readers : List Nat) (excluded : ∀ r ∈ readers, belongs s audience r = false) :
    p.bytes ∉ coalitionKnowledge hash is copies readers := by
  intro known
  obtain ⟨r, hr, known⟩ := List.mem_flatMap.mp known
  obtain ⟨c, listed, opened⟩ := List.mem_filterMap.mp known
  cases hq : acceptedCopy hash is r c with
  | none => simp [hq] at opened
  | some q =>
    simp only [hq, Option.map_some, Option.some.injEq] at opened
    obtain ⟨recipient, _, qkey, qaccepts⟩ := accepted_copy_checked _ _ _ _ _ hq
    obtain ⟨_, _, mem, _, _, _⟩ := selected_usable _ _ _ _ _ _ _ _ selected
    have checks := Bool.and_eq_true_iff.mp accepted
    have pkey : p.key = i.identity := by simpa using checks.1
    have ihash : i.key.keyHash = hash p.bytes :=
      hashFor_agrees is i.identity _ (by simpa [accepts, pkey] using checks.2) i mem rfl
    by_cases same : c.key = i.identity
    · have inside := selected_no_excluded_copy _ _ _ _ _ _ _ _ selected c listed same
      rw [recipient, excluded r hr] at inside
      cases inside
    · obtain ⟨j, hj, jid, jhash⟩ := hashFor_source is c.key _ (by simpa [accepts] using qaccepts)
      exact fresh j hj i mem (fun h => same (jid.symm.trans h)) (by rw [jhash, ihash, opened])

/-- So a fingerprint under the usable key answers no guess for them, whatever
key seals the positions object carrying it. -/
theorem usable_fingerprint_untestable {σ : Type} [DecidableEq σ]
    (hash : KeyBytes → KeyHash) (is : List Introduction) (fresh : Fresh is) (s : State)
    (self : Nat) (copies : List Copy) (held : List Payload) (audience : Audience) (state : σ)
    (f : Fingerprint σ)
    (posted : usableFingerprint hash is s self (.complete copies) held audience state = some f)
    (readers : List Nat) (excluded : ∀ r ∈ readers, belongs s audience r = false)
    (candidate : σ) :
    test (coalitionKnowledge hash is copies readers) f candidate = none := by
  unfold usableFingerprint at posted
  cases hs : select hash is s self (.complete copies) held audience with
  | pending _ => simp [hs] at posted
  | use i =>
    rw [hs] at posted
    cases hf : held.find? (acceptedHeld hash is i) with
    | none => simp [fingerprintWith, hf] at posted
    | some p =>
      simp only [fingerprintWith, hf, Option.map_some, Option.some.injEq] at posted
      subst f
      have unknown := selected_bytes_unknown hash is fresh s self copies held audience i hs p
        (List.find?_some hf) readers excluded
      simp [test, unknown]

/-! ### Examples

Identity is the hash, so a payload's bytes are its commitment. -/

open CurrentExamples (entry key)
open CurrentKeyExamples (payload copy)

set_option maxRecDepth 32768
set_option maxHeartbeats 1600000

/-- Ana removes Ben from the store and rotates to K1. Carol, concurrently and
not having read the removal, rotates to K2 and seals it to Ben too, as §11's
lagging-device example permits. K2 is newer than K1. -/
def storeLog : CurrentReplay.Log
  | 0 => entry 0 0 [] (.create "ana" (key 0))
  | 1 => entry 0 0 [0] (.addMember 1 .member "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol")
  | 3 => entry 2 2 (List.range 3) (.addDevice 2 2)
  | 4 => entry 0 0 (List.range 4) (.removeMember 1)
  | 5 => entry 0 0 (List.range 5) (.rotateKey .store (key 1))
  | _ => entry 2 2 (List.range 4) (.rotateKey .store (key 2))

def storeHistory : CurrentReplay.History :=
  ⟨storeLog, id, fun e => if e = 6 then 4 else e⟩
def storeReplay := CurrentReplay.resolve storeHistory 30 7 (entrySet (List.range 7))
def storeIntros := receivedIntroductions storeHistory 30 7 (entrySet (List.range 7))

def k0 : Introduction := ⟨0, .store, key 0⟩
def k1 : Introduction := ⟨5, .store, key 1⟩
def k2 : Introduction := ⟨6, .store, key 2⟩
def storeCopies : List Copy :=
  [copy k0 0, copy k0 1, copy k0 2, copy k1 0, copy k1 2, copy k2 0, copy k2 1, copy k2 2]
def anaHeld := [payload k0, payload k1, payload k2]
def benKnows := copyKnowledge id storeIntros storeCopies 1

theorem store_history_valid : CurrentReplay.Valid storeLog 7 :=
  validCheck_sound _ _ (by decide)

theorem store_online : Finality.Valid storeHistory.membership 7 := by
  refine ⟨store_history_valid, ?_, ?_, ?_⟩
  · intro a b _ _ same; exact same
  · intro w _
    show (if w = 6 then 4 else w) ≤ w
    split <;> omega
  · intro w a hw ha
    have ws : w = 0 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 4 ∨ w = 5 ∨ w = 6 := by omega
    have as : a = 0 ∨ a = 1 ∨ a = 2 ∨ a = 3 ∨ a = 4 ∨ a = 5 ∨ a = 6 := by omega
    rcases ws with rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
      rcases as with rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> decide

/-- Both rotations are kept, Ben is out, and K2's copy to him is genuine: he
accepts it. App data uses K1; the newest readable key is K2. -/
theorem store_keys :
    4 ∈ storeReplay.kept ∧ 5 ∈ storeReplay.kept ∧ 6 ∈ storeReplay.kept ∧
    member storeReplay.state 1 = false ∧ member storeReplay.state 2 = true ∧
    acceptedCopy id storeIntros 1 (copy k2 1) = some (payload k2) ∧
    CurrentKeys.firstAttempt storeHistory 30 7 (entrySet (List.range 7)) id 0
      (.complete storeCopies) anaHeld .store = .use k1 ∧
    readable id storeIntros anaHeld .store = some k2 ∧
    benKnows = [(payload k0).bytes, (payload k2).bytes] := by decide

/-- Ana's positions are sealed with K2, which Ben opens while provider
revocation is pending. Her store fingerprint covers a write sealed with K1,
which Ben cannot open: its flag is 1 or 0. With the newest readable key the
fingerprint tells Ben which; with the usable key it tells him nothing. -/
theorem readable_store_fingerprint_reveals_write :
    EntryKeys.sealMetadata id storeIntros anaHeld .positions =
      some ⟨.positions, payload k2⟩ ∧
    EntryKeys.canRead benKnows true ⟨.positions, payload k2⟩ = true ∧
    (payload k1).bytes ∉ benKnows ∧
    ((readableFingerprint id storeIntros anaHeld .store (1 : Nat)).bind fun f =>
      (test benKnows f 1).bind fun yes => (test benKnows f 0).map (yes, ·)) =
      some (true, false) ∧
    ((usableFingerprint id storeIntros storeReplay.state 0 (.complete storeCopies) anaHeld
      .store (1 : Nat)).map fun f => (test benKnows f 1, test benKnows f 0)) =
      some (none, none) := by decide

example :
    member storeReplay.state 1 = false ∧
    ((readableFingerprint id storeIntros anaHeld .store (1 : Nat)).bind fun f =>
      (test benKnows f 1).bind fun yes => (test benKnows f 0).map (yes, ·)) =
      some (true, false) := by decide

/-- Ben leaves Gifts but stays in the store, so storage revocation never ends
his access. Ana rotates Gifts to G2; Carol, concurrently and not having read
the removal, rotates to G3 and seals it to Ben too. -/
def circleLog : CurrentReplay.Log
  | 0 => entry 0 0 [] (.create "ana" (key 0))
  | 1 => entry 0 0 [0] (.addMember 1 .member "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol")
  | 3 => entry 2 2 (List.range 3) (.addDevice 2 2)
  | 4 => entry 0 0 (List.range 4) (.makeCircle 0 "Gifts" (key 1))
  | 5 => entry 0 0 (List.range 5) (.addToCircle 0 1)
  | 6 => entry 0 0 (List.range 6) (.addToCircle 0 2)
  | 7 => entry 0 0 (List.range 7) (.removeFromCircle 0 1)
  | 8 => entry 0 0 (List.range 8) (.rotateKey (.circle 0) (key 2))
  | _ => entry 2 2 (List.range 7) (.rotateKey (.circle 0) (key 3))

def circleHistory : CurrentReplay.History :=
  ⟨circleLog, id, fun e => if e = 9 then 7 else e⟩
def circleReplay := CurrentReplay.resolve circleHistory 30 10 (entrySet (List.range 10))
def circleIntros := receivedIntroductions circleHistory 30 10 (entrySet (List.range 10))

def s0 : Introduction := ⟨0, .store, key 0⟩
def g1 : Introduction := ⟨4, .circle 0, key 1⟩
def g2 : Introduction := ⟨8, .circle 0, key 2⟩
def g3 : Introduction := ⟨9, .circle 0, key 3⟩
def circleCopies : List Copy :=
  [copy s0 0, copy s0 1, copy s0 2, copy g1 0, copy g1 1, copy g1 2,
   copy g2 0, copy g2 2, copy g3 0, copy g3 1, copy g3 2]
def anaCircleHeld := [payload s0, payload g1, payload g2, payload g3]
def benCircleKnows := copyKnowledge id circleIntros circleCopies 1

theorem circle_history_valid : CurrentReplay.Valid circleLog 10 :=
  validCheck_sound _ _ (by decide)

theorem circle_online : Finality.Valid circleHistory.membership 10 := by
  refine ⟨circle_history_valid, ?_, ?_, ?_⟩
  · intro a b _ _ same; exact same
  · intro w _
    show (if w = 9 then 7 else w) ≤ w
    split <;> omega
  · intro w a hw ha
    have ws : w = 0 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 4 ∨ w = 5 ∨ w = 6 ∨ w = 7 ∨ w = 8 ∨
        w = 9 := by omega
    have as : a = 0 ∨ a = 1 ∨ a = 2 ∨ a = 3 ∨ a = 4 ∨ a = 5 ∨ a = 6 ∨ a = 7 ∨ a = 8 ∨
        a = 9 := by omega
    rcases ws with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
      rcases as with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> decide

theorem circle_keys :
    7 ∈ circleReplay.kept ∧ 8 ∈ circleReplay.kept ∧ 9 ∈ circleReplay.kept ∧
    member circleReplay.state 1 = true ∧ inCircle circleReplay.state 0 1 = false ∧
    acceptedCopy id circleIntros 1 (copy g3 1) = some (payload g3) ∧
    CurrentKeys.firstAttempt circleHistory 30 10 (entrySet (List.range 10)) id 0
      (.complete circleCopies) anaCircleHeld (.circle 0) = .use g2 ∧
    readable id circleIntros anaCircleHeld (.circle 0) = some g3 := by decide

/-- Ben opens Ana's positions with the store key he holds as a member, with no
cutoff to come. The newest readable Gifts key G3 lets him test a Gifts write
sealed with G2; the usable key G2 does not. -/
theorem readable_circle_fingerprint_reveals_write :
    EntryKeys.sealMetadata id circleIntros anaCircleHeld .positions =
      some ⟨.positions, payload s0⟩ ∧
    EntryKeys.canRead benCircleKnows true ⟨.positions, payload s0⟩ = true ∧
    (payload g2).bytes ∉ benCircleKnows ∧
    ((readableFingerprint id circleIntros anaCircleHeld (.circle 0) (1 : Nat)).bind fun f =>
      (test benCircleKnows f 1).bind fun yes => (test benCircleKnows f 0).map (yes, ·)) =
      some (true, false) ∧
    ((usableFingerprint id circleIntros circleReplay.state 0 (.complete circleCopies)
      anaCircleHeld (.circle 0) (1 : Nat)).map fun f =>
        (test benCircleKnows f 1, test benCircleKnows f 0)) = some (none, none) := by decide

example :
    member circleReplay.state 1 = true ∧ inCircle circleReplay.state 0 1 = false ∧
    ((readableFingerprint id circleIntros anaCircleHeld (.circle 0) (1 : Nat)).bind fun f =>
      (test benCircleKnows f 1).bind fun yes => (test benCircleKnows f 0).map (yes, ·)) =
      some (true, false) := by decide

end CovenStorelog.PositionFingerprints
