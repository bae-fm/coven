import CovenStorelogData.MemberKeySelection
import CovenStorelogData.KeyPhases

namespace CovenStorelogData.KeyPhases.MemberKeys
open CovenStorelog CurrentReplay

structure CaughtUp where
  received : List Nat
  observation : KeySelection.MemberKeys.Pass
  deriving DecidableEq, Repr

def begin (custody : List KeySelection.Key) (membership : Except Pending.Reason (List Nat))
    (listing : Except Pending.Reason KeySelection.MemberKeys.Copies) : Except Pending.Reason CaughtUp := do
  let received ← membership
  let copies ← listing
  return ⟨received, ⟨custody, copies⟩⟩

/-- Every rotation copy uses the current binding and excludes retired sealing
keys, including copies for the rotating member. An unrouteable member blocks
publication; no audience member silently loses their copy. -/
def rotationCopies (state : Keyed.State) (retired : List Nat) (key : KeySelection.Key) :
    Option KeySelection.MemberKeys.Copies :=
  (audienceMembers state.membership key.audience).mapM (fun m =>
    (KeySelection.MemberKeys.route state retired m).map (key, ·))

def rotate (H : Keyed.History) (W n who device : Nat) (p : CaughtUp) (e : Nat)
    (copiesStored entryStored : Except Pending.Reason Unit) : Except Pending.Reason CaughtUp := do
  let entry := H.log e
  let (audience, commitment) ← match entry.action with
    | .entry (.rotateKey audience commitment) _ => pure (audience, commitment)
    | _ => .error (.dropped .notAllowed)
  let r := Keyed.resolve H W n (entrySet p.received)
  if !(e < n && e ∉ p.received && entry.author == who && entry.device == device &&
      running r.state.membership who device && who ∈ audienceMembers r.state.membership audience &&
      entry.past.all (· ∈ p.received) && p.received.all (· ∈ entry.past)) then
    throw (.dropped .notAllowed)
  let key : KeySelection.Key := ⟨e, audience, commitment⟩
  let received := p.received ++ [e]
  if !(KeySelection.MemberKeys.receivedAuthorized H W n (entrySet received) key &&
      e ∈ (Keyed.resolve H W n (entrySet received)).kept) then throw (.dropped .notAllowed)
  let copies ← match rotationCopies r.state (KeySelection.MemberKeys.retired H W n r) key with
    | none => .error (.dropped .notAllowed)
    | some copies => pure copies
  let _ ← copiesStored
  let _ ← entryStored
  return ⟨received, ⟨key :: p.observation.custody, p.observation.copies ++ copies⟩⟩

def firstSend (H : Keyed.History) (W n who : Nat) (audience : Audience)
    (p : CaughtUp) (bytes : List Nat) : Option KeySelection.Attempt :=
  (KeySelection.MemberKeys.selectInReplay H W n (entrySet p.received) p.observation who audience).map
    (fun key => ⟨key, bytes⟩)

theorem first_send_after_replacement (H : Keyed.History) (W n who e m : Nat)
    (audience : Audience) (p : CaughtUp) (bytes : List Nat) (attempt : KeySelection.Attempt)
    (old keys : Keyed.MemberKeys)
    (kept : e ∈ (Keyed.resolve H W n (entrySet p.received)).kept)
    (action : (H.log e).action = .replaceMemberKeys m keys)
    (past : lookup (Keyed.authorViews H W n n e).keys m = some old)
    (sent : firstSend H W n who audience p bytes = some attempt)
    (recipient : KeySelection.MemberKeys.Recipient) (oldSeal : recipient.sealing = old.sealing) :
    (attempt.key, recipient) ∉ p.observation.copies := by
  obtain ⟨key, selected, equal⟩ := Option.map_eq_some_iff.mp sent
  subst attempt
  exact KeySelection.MemberKeys.replacement_selection H W n e m _ old keys p.observation who
    audience key kept action past selected recipient oldSeal

theorem incomplete_membership_waits (custody : List KeySelection.Key) (why : Pending.Reason)
    (listing : Except Pending.Reason KeySelection.MemberKeys.Copies) :
    begin custody (.error why) listing = .error why := rfl

theorem incomplete_listing_waits (custody : List KeySelection.Key) (received : List Nat) (why : Pending.Reason) :
    begin custody (.ok received) (.error why) = .error why := rfl

theorem rotation_requires_publication (H : Keyed.History) (W n who device : Nat)
    (p next : CaughtUp) (e : Nat) (copies entry : Except Pending.Reason Unit)
    (success : rotate H W n who device p e copies entry = .ok next) :
    copies = .ok () ∧ entry = .ok () := by
  unfold rotate at success
  cases ha : (H.log e).action with
  | replaceMemberKeys m keys => simp [ha, bind, Except.bind] at success
  | entry a keys =>
      cases a <;> simp only [ha] at success
      all_goals try { contradiction }
      all_goals
        simp only [Except.pure, Except.bind, bind, pure] at success
        split at success <;> try contradiction
        split at success <;> try contradiction
        split at success <;> try contradiction
        cases copies <;> cases entry <;> simp_all

/-- Every successful list consists of copies for non-retired keys. -/
theorem rotation_copies_safe (state : Keyed.State) (retired : List Nat) (key : KeySelection.Key)
    (copies : KeySelection.MemberKeys.Copies) (built : rotationCopies state retired key = some copies) :
    ∀ copy ∈ copies, copy.2.sealing ∉ retired := by
  unfold rotationCopies at built
  generalize audienceMembers state.membership key.audience = members at built
  induction members generalizing copies with
  | nil => cases built; simp
  | cons m ms ih =>
      simp only [List.mapM_cons, bind, Option.bind] at built
      cases hr : KeySelection.MemberKeys.route state retired m with
      | none => simp [hr] at built
      | some recipient =>
          simp only [hr, Option.map_some] at built
          cases ht : ms.mapM (fun m => (KeySelection.MemberKeys.route state retired m).map (key, ·)) with
          | none => simp [ht] at built
          | some tail =>
              simp only [ht, pure, Option.some.injEq] at built
              subst copies
              intro copy present
              rcases List.mem_cons.mp present with rfl | present
              · exact (KeySelection.MemberKeys.route_current state retired m recipient hr).2.1
              · exact ih tail ht copy present

end CovenStorelogData.KeyPhases.MemberKeys
