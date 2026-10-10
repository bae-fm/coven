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

end CovenStorelogData.KeyPhases
