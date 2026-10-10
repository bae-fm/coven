import CovenStorelogData.Model

namespace CovenStorelogData

open CovenStorelog

/-- Fresh ids are abstracted by the introducing entry and audience.
Only creation and rotation introduce keys (§11). -/
structure Key where
  entry : Nat
  audience : Audience
  deriving DecidableEq, Repr

def introduced (M : Log) (e : Nat) : List Key :=
  let audiences : List Audience := match (M e).action with
    | .create _ => [.store]
    | .rotateKey audience _ => [audience]
    | .makeCircle c _ => [.circle c]
    | _ => []
  audiences.map (Key.mk e)

def audienceMembers (s : CovenStorelog.State) : Audience → List Nat
  | .store => s.members.map Prod.fst
  | .circle c => match lookup s.circles c with
    | none => []
    | some circle => circle.members.filter (member s)

/-- Creation seals to its author; rotation seals to its recorded audience. -/
def initialRecipients (M : Log) (k : Key) : List Nat :=
  let s := authorView M k.entry
  match (M k.entry).action with
  | .create _ | .makeCircle _ _ => [(M k.entry).author]
  | .rotateKey audience _ => audienceMembers s audience
  | _ => []

def authorizedKey (M : Log) (k : Key) : Bool :=
  authorized (authorView M k.entry) (M k.entry) && k ∈ introduced M k.entry

abbrev Copies := List (Key × Nat)

/-- Completed key seals precede publication (§18.1). Additions share every
available historical key separately; these are the fresh-key receipts. -/
def introductionCopies (M : Log) (published : List Nat) : Copies :=
  published.flatMap fun e => (introduced M e).flatMap fun k =>
    (initialRecipients M k).map (k, ·)

/-- Additions must hold every needed historical key before sharing. The ring
then seals all keys it holds for that audience (store_log_keys.rs::seal).
`none` is the KeyUnavailable failure, not a partially published addition. -/
def shareAddition (M : Log) (e : Nat) (copies : Copies) : Option Copies := do
  let (audience, recipient) ← match (M e).action with
    | .addMember m _ _ => some (Audience.store, m)
    | .addToCircle c m => some (Audience.circle c, m)
    | _ => none
  let prior := resolve M e (entrySet (M e).past)
  let needed := ((M e).past.filter fun p => p ∈ prior.kept || p ∈ prior.dropped)
    |>.flatMap (introduced M) |>.filter (fun k =>
      authorizedKey M k && k.audience == audience)
  if !needed.all (fun k => (k, (M e).author) ∈ copies) then none else
    some (copies ++ (copies.filterMap fun (k, m) =>
      if m == (M e).author && k.audience == audience then some (k, recipient) else none))

/-- A device which has read its own or its member's removal stops before
acquiring or sharing keys (store_log_sync.rs::step and ::apply). -/
def running (s : CovenStorelog.State) (member device : Nat) : Bool :=
  CovenStorelog.member s member && lookup s.devices device == some member

/-- A full successful redistribution round, even allowing every running
device to fetch all its stored copies. No network failure or unfair schedule
is needed for the counterexample. Copies already stored never disappear. -/
def redistribute (M : Log) (result : Result) (copies : Copies) : Copies :=
  let keys := (result.kept ++ result.dropped).flatMap (introduced M)
  copies ++ keys.flatMap fun k =>
    if authorizedKey M k && result.state.devices.any (fun (d, m) =>
        running result.state m d && (k, m) ∈ copies) then
      (audienceMembers result.state k.audience).filterMap fun m =>
        if (k, m) ∈ copies then none else some (k, m)
    else []

def rounds (M : Log) (result : Result) : Nat → Copies → Copies
  | 0, copies => copies
  | n + 1, copies => rounds M result n (redistribute M result copies)

theorem unavailable_forever (M : Log) (result : Result) (copies : Copies)
    (fixed : redistribute M result copies = copies) (k : Key) (m : Nat)
    (missing : (k, m) ∉ copies) : ∀ n, (k, m) ∉ rounds M result n copies := by
  intro n
  induction n with
  | zero => exact missing
  | succ n ih => simpa [rounds, fixed] using ih

/-- write_object.rs::key_audience_contains uses replay immediately after the
introduction, and later kept additions, as well as the current audience. -/
def mustRead (M : Log) (result : Result) (k : Key) (m : Nat) : Bool :=
  let atIntroduction := (resolve M (k.entry + 1)
    (entrySet ((M k.entry).past ++ [k.entry]))).state
  m ∈ audienceMembers result.state k.audience || m ∈ audienceMembers atIntroduction k.audience ||
    result.kept.any (fun e => hadRead M e k.entry && match (M e).action, k.audience with
      | .addMember who _ _, .store => who == m
      | .addToCircle c who, .circle owner => c == owner && who == m
      | _, _ => false)

inductive Part where
  | apply | skip | wait
  deriving DecidableEq, Repr

def part (M : Log) (result : Result) (copies : Copies) (k : Key) (m : Nat) : Part :=
  if (k, m) ∈ copies then .apply
  else if mustRead M result k m then .wait else .skip

/-- A missing required key prevents application and prevents counting the
part as applied; a skipped part is counted only for an outside reader. -/
theorem required_key_waits (M : Log) (result : Result) (copies : Copies) (k : Key) (m : Nat)
    (missing : (k, m) ∉ copies) (required : mustRead M result k m = true) :
    part M result copies k m = .wait := by simp [part, missing, required]

theorem removals_introduce_no_keys (M : Log) (e m c : Nat) :
    ((M e).action = .removeMember m [] → introduced M e = []) ∧
    ((M e).action = .removeFromCircle c m → introduced M e = []) := by
  constructor <;> intro h <;> simp [introduced, h]

end CovenStorelogData
