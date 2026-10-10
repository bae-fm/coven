import CovenStorelogData.EntryFate

namespace CovenStorelogData.EntryFate

open CovenStorelog

/-- A fresh replacement lives at its creator's path and next local number.
There is no shared allocation slot and no global sequence. -/
structure FreshKey where
  device : Nat
  number : Nat
  audience : Audience
  deriving DecidableEq, Repr

/-- Custody is per device even though §11's sealed envelopes address members.
Removing one device must not be mistaken for removing its whole member. -/
structure Recipient where
  member : Nat
  device : Nat
  deriving DecidableEq, Repr

abbrev Deliveries := List (FreshKey × Recipient)

def recipientAllowed (result : Result) (audience : Audience) (recipient : Recipient) : Bool :=
  recipient.member ∈ audienceMembers result.state audience &&
    running result.state recipient.member recipient.device

def safeKey (result : Result) (deliveries : Deliveries) (key : FreshKey) : Bool :=
  deliveries.all fun (k, recipient) => k != key || recipientAllowed result key.audience recipient

def deliver (key : FreshKey) (recipients : List Recipient) (delivered : Deliveries) : Deliveries :=
  delivered ++ recipients.map (key, ·)

/-- Refresh uses the author's observations. Supplying a globally complete
delivery ledger would be an extra assumption, not a storage capability. -/
def refresh (result : Result) (known : Deliveries) (current fresh : FreshKey) : FreshKey :=
  if safeKey result known current then current else fresh

theorem refresh_safe (result : Result) (known : Deliveries) (current fresh : FreshKey)
    (freshSafe : safeKey result known fresh = true) :
    safeKey result known (refresh result known current fresh) = true := by
  unfold refresh
  split
  · assumption
  · exact freshSafe

/-- The exception holds at a checked send only when the check covers every
actual delivery, including other devices' deliveries. Freshness alone does not
establish that knowledge, nor prevent a later disclosure of the same key. -/
theorem checked_send_safe (result : Result) (known actual : Deliveries)
    (current fresh : FreshKey) (complete : ∀ receipt ∈ actual, receipt ∈ known)
    (freshSafe : safeKey result known fresh = true) :
    safeKey result actual (refresh result known current fresh) = true := by
  have checked := refresh_safe result known current fresh freshSafe
  simp only [safeKey, List.all_eq_true] at checked ⊢
  intro receipt present
  exact checked receipt (complete receipt present)

theorem delivery_persists (key : FreshKey) (recipients : List Recipient)
    (delivered : Deliveries) (receipt : FreshKey × Recipient) (present : receipt ∈ delivered) :
    receipt ∈ deliver key recipients delivered := List.mem_append_left _ present

/-- Two histories can require different current keys even with the same final
entry/write sets: the exception depends on past disclosure, not just entry fate.
This is a statement about key identity, without requiring custody to converge. -/
theorem exposure_forces_change (result : Result) (known : Deliveries)
    (current fresh : FreshKey) (exposed : safeKey result known current = false) :
  refresh result known current fresh = fresh := by simp [refresh, exposed]

/-- Member encryption alone cannot distinguish a kept device from a removed
device holding the same private member key. Storage delivery and honest-client
stopping are separate from this cryptographic ability. -/
def canOpenMemberSeal (member : Nat) (recipient : Recipient) : Bool :=
  member == recipient.member

theorem member_seal_cannot_distinguish (member : Nat) (a b : Recipient)
    (sameMember : a.member = b.member) :
    canOpenMemberSeal member a = canOpenMemberSeal member b := by
  simp [canOpenMemberSeal, sameMember]

end CovenStorelogData.EntryFate
