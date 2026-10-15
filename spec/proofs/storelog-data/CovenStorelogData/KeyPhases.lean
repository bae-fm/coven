import CovenStorelogData.KeySelection
import CovenStorelogData.Pending

/-! Sync-pass phases 1–3: complete observations, publish rotation copies and
entry, then select for data. Successful receipts abstract storage IO. -/
namespace CovenStorelogData.KeyPhases
open CovenStorelog KeySelection

structure CaughtUp where
  received : List Nat
  observation : KeySelection.Pass
  deriving DecidableEq, Repr

def begin (custody : List KeySelection.Key)
    (membership : Except Pending.Reason (List Nat))
    (listing : Except Pending.Reason KeySelection.Copies) : Except Pending.Reason CaughtUp := do
  let received ← membership
  let copies ← listing
  return ⟨received, ⟨custody, copies⟩⟩

/-- Only a currently running audience member can rotate. The candidate reads
exactly this caught-up view. Its copies precede its entry; neither a failed
copy nor a failed entry makes the key available for ordinary first sends. -/
def rotate (H : CurrentReplay.History) (W n who device : Nat) (p : CaughtUp) (e : Nat)
    (copiesStored entryStored : Except Pending.Reason Unit) : Except Pending.Reason CaughtUp := do
  let entry := H.log e
  let (audience, commitment) ← match entry.action with
    | .rotateKey audience commitment => pure (audience, commitment)
    | _ => .error (.dropped .notAllowed)
  let state := (CurrentReplay.resolve H W n (entrySet p.received)).state
  let members := audienceMembers state audience
  if !(e < n && e ∉ p.received && entry.author == who && entry.device == device &&
      running state who device && who ∈ members &&
      entry.past.all (· ∈ p.received) && p.received.all (· ∈ entry.past)) then
    throw (.dropped .notAllowed)
  let key : KeySelection.Key := ⟨e, audience, commitment⟩
  let received := p.received ++ [e]
  if !(receivedAuthorized H W n (entrySet received) key &&
      e ∈ (CurrentReplay.resolve H W n (entrySet received)).kept) then
    throw (.dropped .notAllowed)
  let _ ← copiesStored
  let _ ← entryStored
  return ⟨received, ⟨key :: p.observation.custody,
    p.observation.copies ++ members.map (key, ·)⟩⟩

def firstSend (H : CurrentReplay.History) (W n who : Nat) (audience : Audience)
    (p : CaughtUp) (bytes : List Nat) : Option Attempt :=
  (selectInReplay H W n (entrySet p.received) p.observation who audience).map
    (fun key => ⟨key, bytes⟩)

theorem incomplete_membership_waits (custody : List KeySelection.Key) (why : Pending.Reason)
    (listing : Except Pending.Reason KeySelection.Copies) :
    begin custody (.error why) listing = .error why := rfl

theorem incomplete_listing_waits (custody : List KeySelection.Key) (received : List Nat)
    (why : Pending.Reason) :
    begin custody (.ok received) (.error why) = .error why := rfl

theorem rotation_requires_publication (H : CurrentReplay.History) (W n who device : Nat)
    (p next : CaughtUp) (e : Nat) (copies entry : Except Pending.Reason Unit)
    (success : rotate H W n who device p e copies entry = .ok next) :
    copies = .ok () ∧ entry = .ok () := by
  unfold rotate at success
  cases ha : (H.log e).action <;> simp only [ha] at success
  all_goals try { contradiction }
  all_goals
    simp only [Except.pure, Except.bind, bind, pure] at success
    split at success <;> try contradiction
    split at success <;> try contradiction
    cases copies <;> cases entry <;> simp_all

theorem rotation_by_remaining_member (H : CurrentReplay.History) (W n who device : Nat)
    (p next : CaughtUp) (e : Nat) (commitment : CurrentReplay.KeyCommitment) (audience : Audience)
    (copies stored : Except Pending.Reason Unit)
    (action : (H.log e).action = .rotateKey audience commitment)
    (success : rotate H W n who device p e copies stored = .ok next) :
    who ∈ audienceMembers (CurrentReplay.resolve H W n (entrySet p.received)).state audience ∧
    running (CurrentReplay.resolve H W n (entrySet p.received)).state who device = true := by
  constructor
  · by_cases present : who ∈ audienceMembers
        (CurrentReplay.resolve H W n (entrySet p.received)).state audience
    · exact present
    · simp [rotate, action, present] at success
      cases success
  · cases active : running (CurrentReplay.resolve H W n (entrySet p.received)).state who device
    · simp [rotate, action, active] at success
      cases success
    · rfl

/-- §3 excludes the departed author's own queued pre-removal writes: this
path admits only a current member's first send. No later copy is the stated
listing window, not an assumption that membership can never reverse. -/
theorem revocation (H : CurrentReplay.History) (W n who removed : Nat) (audience : Audience)
    (p : CaughtUp) (bytes : List Nat) (attempt : Attempt) (later : KeySelection.Copies)
    (sent : firstSend H W n who audience p bytes = some attempt)
    (excluded : removed ∉ audienceMembers
      (CurrentReplay.resolve H W n (entrySet p.received)).state audience)
    (noLaterCopy : (attempt.key, removed) ∉ later) :
    who ≠ removed ∧ (attempt.key, removed) ∉ p.observation.copies ++ later := by
  obtain ⟨key, selected, equal⟩ := Option.map_eq_some_iff.mp sent
  subst attempt
  have safe := first_attempt_safe _ _ _ _ _ _ selected
  refine ⟨?_, revocation_between_listings _ _ _ _ later who audience key
    selected excluded noLaterCopy⟩
  intro same
  exact excluded (same ▸ safe.1)

/-! §11 tentative candidates within one store-log scan. A received entry
sealed with a key this device does not hold yet is opened with a candidate
from a copy addressed to it. Its signature, identity and authority checks run
only once its recorded past has applied, so a scan holds every opened entry
before those checks. The model covers only custody, the device's held key
bytes; the checks themselves are the `verified` outcome. -/
section Tentative
open CurrentKeys (Payload KeyId Introduction accepts holdsAccepted)
open CurrentReplay (KeyBytes KeyHash)

/-- Custody keeps one value per key id and refuses a second
(coven-crypto `StoreKeyring::insert_store_key`). -/
def keep (held : List Payload) (p : Payload) : List Payload :=
  if held.any (·.key == p.key) then held else held ++ [p]

/-- One entry a scan opened with a candidate it did not hold: the key id the
entry names, the candidate bytes that decrypted it, the hash the decrypted
entry itself commits to (`none` for an entry that only uses the key), and
whether its checks in its recorded view passed. -/
structure Opening where
  key : KeyId
  candidate : KeyBytes
  commitment : Option KeyHash
  verified : Bool
  deriving DecidableEq, Repr

def Opening.payload (o : Opening) : Payload := ⟨o.key, o.candidate⟩

/-- §11's rule: the candidate becomes this device's key only once its
introducing entry passed its checks, and then only as an ordinary accepted
copy, matching the agreed commitment of the received authorized
introductions `is` (which contain only checked entries). -/
def confirm (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (o : Opening) : List Payload :=
  if o.verified && accepts hash is o.key o.candidate then keep held o.payload else held

def scan (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (os : List Opening) : List Payload :=
  os.foldl (confirm hash is) held

/-- What the code did before (D-11-1): it kept the candidate once it matched
the commitment of the opened entry itself, before that entry's checks. -/
def confirmUnchecked (hash : KeyBytes → KeyHash) (held : List Payload)
    (o : Opening) : List Payload :=
  if o.commitment == some (hash o.candidate) then keep held o.payload else held

/-- A later entry this scan opened with an introducer's candidate joins the
batch only once that introducer passed its checks and the candidate is
accepted: an unchecked candidate authorizes no other data. -/
def admitsDependent (hash : KeyBytes → KeyHash) (is : List Introduction)
    (introducer : Opening) : Bool :=
  introducer.verified && accepts hash is introducer.key introducer.candidate

theorem unverified_keeps_custody (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (o : Opening) (unchecked : o.verified = false) :
    confirm hash is held o = held := by
  simp [confirm, unchecked]

/-- No unverified entry changes key state: a scan's custody is the custody of
the same scan with every unverified opening left out. -/
theorem scan_ignores_unverified (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (os : List Opening) :
    scan hash is held os = scan hash is held (os.filter (·.verified)) := by
  induction os generalizing held with
  | nil => rfl
  | cons o rest ih =>
    cases hv : o.verified
    · simp only [scan, List.foldl_cons, List.filter_cons, hv] at ih ⊢
      rw [unverified_keeps_custody hash is held o hv]
      exact ih held
    · simp only [scan, List.foldl_cons, List.filter_cons, hv, ite_true] at ih ⊢
      exact ih _

theorem keep_mem (held : List Payload) (p q : Payload) (present : q ∈ held) :
    q ∈ keep held p := by
  unfold keep
  split
  · exact present
  · exact List.mem_append_left _ present

theorem confirm_mem (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (o : Opening) (q : Payload) (present : q ∈ held) :
    q ∈ confirm hash is held o := by
  unfold confirm
  split
  · exact keep_mem _ _ _ present
  · exact present

theorem scan_mem (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (os : List Opening) (q : Payload) (present : q ∈ held) :
    q ∈ scan hash is held os := by
  induction os generalizing held with
  | nil => exact present
  | cons o rest ih => exact ih _ (confirm_mem hash is held o q present)

/-- Every key in custody after a scan matches the agreed commitment of a
received authorized introduction, as before it. -/
theorem scan_accepted (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (os : List Opening)
    (accepted : ∀ p ∈ held, accepts hash is p.key p.bytes = true) :
    ∀ p ∈ scan hash is held os, accepts hash is p.key p.bytes = true := by
  induction os generalizing held with
  | nil => exact accepted
  | cons o rest ih =>
    apply ih
    intro p mem
    unfold confirm at mem
    split at mem
    · rename_i checks
      unfold keep at mem
      split at mem
      · exact accepted p mem
      · rcases List.mem_append.mp mem with old | new
        · exact accepted p old
        · simp only [List.mem_singleton] at new
          subst p
          exact (Bool.and_eq_true_iff.mp checks).2
    · exact accepted p mem

/-- An accepted payload enters custody that holds only accepted payloads:
any value already held for its id is the same bytes. -/
theorem keep_accepted (hash : KeyBytes → KeyHash)
    (injective : ∀ a b, hash a = hash b → a = b) (is : List Introduction)
    (held : List Payload) (p : Payload)
    (accepted : ∀ q ∈ held, accepts hash is q.key q.bytes = true)
    (good : accepts hash is p.key p.bytes = true) : p ∈ keep held p := by
  unfold keep
  split
  · rename_i occupied
    obtain ⟨q, mem, same⟩ := List.any_eq_true.mp occupied
    have same : q.key = p.key := by simpa using same
    have bytes := CurrentKeys.accepted_bytes_unique hash injective is p.key q.bytes p.bytes
      (same ▸ accepted q mem) good
    have : q = p := by cases q; cases p; simp_all
    exact this ▸ mem
  · exact List.mem_append_right _ (List.mem_singleton.mpr rfl)

/-- Forged entries cannot block the genuine key: whatever unverified or
mismatching openings come first, a checked introduction's accepted candidate
is in custody after the scan. -/
theorem genuine_confirmed (hash : KeyBytes → KeyHash)
    (injective : ∀ a b, hash a = hash b → a = b) (is : List Introduction)
    (held : List Payload) (before after : List Opening) (genuine : Opening)
    (accepted : ∀ p ∈ held, accepts hash is p.key p.bytes = true)
    (checked : genuine.verified = true)
    (good : accepts hash is genuine.key genuine.candidate = true) :
    genuine.payload ∈ scan hash is held (before ++ genuine :: after) := by
  have prior := scan_accepted hash is held before accepted
  simp only [scan, List.foldl_append, List.foldl_cons]
  apply scan_mem
  simp only [confirm, checked, good, Bool.and_self, ite_true]
  exact keep_accepted hash injective is _ genuine.payload prior good

/-- A dependent admitted in the batch was opened with the one byte value
custody can accept for that id. -/
theorem dependent_accepted (hash : KeyBytes → KeyHash)
    (injective : ∀ a b, hash a = hash b → a = b) (is : List Introduction)
    (introducer : Opening) (admitted : admitsDependent hash is introducer = true)
    (bytes : KeyBytes) (other : accepts hash is introducer.key bytes = true) :
    introducer.verified = true ∧ introducer.candidate = bytes := by
  have h := Bool.and_eq_true_iff.mp admitted
  exact ⟨h.1, CurrentKeys.accepted_bytes_unique hash injective is _ _ _ h.2 other⟩

/-- Once custody holds a value for an id, no step keeping one value per id
adds another value for it. -/
theorem keep_occupied (held : List Payload) (k : KeyId) (occupied : ∃ q ∈ held, q.key = k)
    (r p : Payload) (mem : p ∈ keep held r) (same : p.key = k) : p ∈ held := by
  unfold keep at mem
  split at mem
  · exact mem
  · rename_i free
    rcases List.mem_append.mp mem with old | new
    · exact old
    · simp only [List.mem_singleton] at new
      subst p
      obtain ⟨q, hq, qk⟩ := occupied
      exact False.elim (free (List.any_eq_true.mpr ⟨q, hq, by simp [qk, same]⟩))

/-- A planted value blocks its id for good: if every value custody holds for
the id fails the agreed commitment, then after any further values are kept
custody still holds no accepted key for that id. -/
theorem planted_blocks (hash : KeyBytes → KeyHash) (is : List Introduction)
    (held : List Payload) (later : List Payload) (i : Introduction)
    (occupied : ∃ q ∈ held, q.key = i.identity)
    (rejected : ∀ q ∈ held, q.key = i.identity → accepts hash is q.key q.bytes = false) :
    holdsAccepted hash is (later.foldl keep held) i = false := by
  induction later generalizing held with
  | nil =>
    simp only [List.foldl_nil]
    cases hh : holdsAccepted hash is held i
    · rfl
    · obtain ⟨q, mem, good⟩ := List.any_eq_true.mp hh
      simp only [CurrentKeys.acceptedHeld, Bool.and_eq_true, beq_iff_eq] at good
      simp [rejected q mem good.1] at good
  | cons r rest ih =>
    simp only [List.foldl_cons]
    apply ih
    · obtain ⟨q, hq, qk⟩ := occupied
      exact ⟨q, keep_mem _ _ _ hq, qk⟩
    · intro q mem same
      exact rejected q (keep_occupied held i.identity occupied r q mem same) same

end Tentative

/-! D-11-1's history. Ana's rotation introduces store key 3 with the genuine
bytes; its copies are stored before its entry. Someone with storage access
plants an entry claiming key 3 with chosen bytes, sealed with them, and a copy
of them for Ben. Its checks fail. The identity stands in for the hash. -/
section ForgedIntroduction
open CurrentKeys (Payload KeyId Introduction holdsAccepted)

def genuineBytes : CurrentReplay.KeyBytes := Vector.replicate 32 3
def chosenBytes : CurrentReplay.KeyBytes := Vector.replicate 32 4
def keyThree : KeyId := ⟨.store, 3⟩
def rotation : Introduction := ⟨5, .store, ⟨3, genuineBytes⟩⟩
def forgedOpening : Opening := ⟨keyThree, chosenBytes, some chosenBytes, false⟩
def genuineOpening : Opening := ⟨keyThree, genuineBytes, some genuineBytes, true⟩

/-- Under the old rule Ben keeps the chosen bytes as key 3, and the genuine
rotation arriving after cannot replace them. -/
theorem unchecked_plants_chosen_bytes :
    [forgedOpening, genuineOpening].foldl (confirmUnchecked id) [] =
      [⟨keyThree, chosenBytes⟩] ∧
    holdsAccepted id [rotation] [⟨keyThree, chosenBytes⟩] rotation = false := by
  decide

/-- And no later value ever makes key 3 accepted in his custody. -/
theorem planted_key_never_accepted (later : List Payload) :
    holdsAccepted id [rotation] (later.foldl keep [⟨keyThree, chosenBytes⟩]) rotation = false :=
  planted_blocks id [rotation] _ later rotation ⟨_, List.mem_singleton.mpr rfl, rfl⟩
    (by intro q mem _; simp only [List.mem_singleton] at mem; subst q; decide)

/-- Under §11's rule the forged entry changes nothing and the genuine key is
accepted. -/
theorem checked_keeps_genuine_bytes :
    scan id [rotation] [] [forgedOpening, genuineOpening] = [⟨keyThree, genuineBytes⟩] ∧
    holdsAccepted id [rotation] [⟨keyThree, genuineBytes⟩] rotation = true := by
  decide

end ForgedIntroduction

end CovenStorelogData.KeyPhases
