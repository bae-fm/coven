import CovenStorelog.CurrentKeys
import CovenStorelogData.Keys

/-! §11: a successful pass lists immutable sealed copies from storage.
The listing is shared exposure evidence; custody remains device-local.
A later share changes storage, never the already captured pass. -/
namespace CovenStorelogData.KeySelection
open CovenStorelog

abbrev Key := CurrentKeys.Introduction

/-- Receipts name the shared, authorized introduction and its recipient.
Fresh key ids identify one introduction; ciphertext is outside this model. -/
abbrev Copies := List (Key × Nat)

structure Pass where
  custody : List Key
  copies : Copies
  deriving DecidableEq, Repr

inductive ListingFailure where
  | network | permission | incomplete
  deriving DecidableEq, Repr

/-- One complete listing per pass. An error cannot reuse a previous listing. -/
def beginPass (custody : List Key) (stored : Copies)
    (read : Except ListingFailure Unit) : Except ListingFailure Pass :=
  read.map fun _ => ⟨custody, stored⟩

def exposed (members : Audience → List Nat) (pass : Pass) (key : Key) : Bool :=
  pass.copies.any fun (k, m) => k == key && m ∉ members key.audience

def usable (members : Audience → List Nat) (authorized : Key → Bool)
    (pass : Pass) (key : Key) : Bool :=
  authorized key && key ∈ pass.custody && !exposed members pass key

abbrev newer := CurrentKeys.newer

/-- Introduction timestamp, then key id. Concurrent rotations coexist. -/
abbrev newest := CurrentKeys.newest

def select (members : Audience → List Nat) (authorized : Key → Bool)
    (pass : Pass) (who : Nat) (audience : Audience) : Option Key :=
  if who ∈ members audience then
    newest (pass.custody.filter fun key => key.audience == audience &&
      usable members authorized pass key)
  else none

/-- Historical-key sharing adds permanent storage evidence, independently of
which other devices saw this replay. The caller must still be running (§10). -/
def share (members : Audience → List Nat) (custody : List Key) (stored : Copies)
    (recipient : Nat) (key : Key) : Option Copies :=
  if key ∈ custody ∧ recipient ∈ members key.audience then
    some (stored ++ [(key, recipient)])
  else none

theorem newest_mem {keys : List Key} {key : Key} (h : newest keys = some key) :
    key ∈ keys := CurrentKeys.newest_mem keys key h

theorem newer_trans {a b c : Key} (ab : newer a b = true) (bc : newer b c = true) :
    newer a c = true := by
  simp only [newer, CurrentKeys.newer, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq,
    beq_iff_eq] at *
  omega

theorem newest_maximal {keys : List Key} {key : Key} (h : newest keys = some key) :
    ∀ other ∈ keys, newer other key = false := by
  intro other present
  have maximal := CurrentKeys.newest_maximal keys key h other present
  simp only [newer, CurrentKeys.newer, Bool.or_eq_false_iff, Bool.and_eq_false_iff,
    decide_eq_false_iff_not, beq_eq_false_iff_ne]
  constructor <;> omega

theorem selection_is_newest (members : Audience → List Nat) (authorized : Key → Bool)
    (pass : Pass) (who : Nat) (audience : Audience) (key : Key)
    (selected : select members authorized pass who audience = some key)
    (other : Key) (held : other ∈ pass.custody) (same : other.audience = audience)
    (allowed : usable members authorized pass other = true) : newer other key = false := by
  unfold select at selected
  split at selected
  · exact newest_maximal selected other (List.mem_filter.mpr ⟨held, by simp [same, allowed]⟩)
  · cases selected

theorem first_attempt_safe (members : Audience → List Nat) (authorized : Key → Bool)
    (pass : Pass) (who : Nat) (audience : Audience) (key : Key)
    (sent : select members authorized pass who audience = some key) :
    who ∈ members audience ∧ key.audience = audience ∧
    authorized key = true ∧ key ∈ pass.custody ∧ exposed members pass key = false := by
  unfold select at sent
  split at sent
  · rename_i hm
    have h := (List.mem_filter.mp (newest_mem sent)).2
    simp only [Bool.and_eq_true, beq_iff_eq, usable, decide_eq_true_eq,
      Bool.not_eq_true'] at h
    exact ⟨hm, h.1, h.2.1.1, h.2.1.2, h.2.2⟩
  · cases sent

theorem listed_excluded_cannot_read (members : Audience → List Nat)
    (authorized : Key → Bool) (pass : Pass) (who : Nat) (audience : Audience)
    (key : Key) (sent : select members authorized pass who audience = some key)
    (excluded : m ∉ members audience) : (key, m) ∉ pass.copies := by
  intro delivered
  have hs := first_attempt_safe members authorized pass who audience key sent
  have hx : exposed members pass key = true := by
    apply List.any_eq_true.mpr
    exact ⟨(key, m), delivered, by simp [hs.2.1, excluded]⟩
  simp [hs.2.2.2.2] at hx

theorem listed_exposure_retires (members : Audience → List Nat) (authorized : Key → Bool)
    (pass : Pass) (key : Key) (m : Nat)
    (listed : (key, m) ∈ pass.copies) (excluded : m ∉ members key.audience) :
    usable members authorized pass key = false := by
  have exposed : exposed members pass key = true :=
    List.any_eq_true.mpr ⟨(key, m), listed, by simp [excluded]⟩
  simp [usable, exposed]

theorem copies_persist (members : Audience → List Nat) (custody : List Key)
    (before after : Copies) (recipient : Nat) (key : Key)
    (stored : share members custody before recipient key = some after) :
    ∀ copy ∈ before, copy ∈ after := by
  unfold share at stored
  split at stored
  · cases stored; exact fun _ h => List.mem_append_left _ h
  · cases stored

/-- Changing the introducing entry's fate cannot erase a stored copy. As long
as its recipient stays excluded, every complete subsequent listing retires it. -/
theorem exposure_persists_while_excluded (members : Audience → List Nat)
    (authorized : Key → Bool) (custody : List Key) (before later : Copies)
    (key : Key) (m : Nat) (copied : (key, m) ∈ before)
    (excluded : m ∉ members key.audience) :
    usable members authorized ⟨custody, before ++ later⟩ key = false :=
  listed_exposure_retires members authorized _ key m (List.mem_append_left _ copied) excluded

/-- The only new secrecy premise after a complete listing: no copy of the
selected key for this excluded member appears after the listing. This may
cover an entire pass or the later period while an old write remains readable. -/
theorem revocation_between_listings (members : Audience → List Nat)
    (authorized : Key → Bool) (custody : List Key) (listed later : Copies)
    (who : Nat) (audience : Audience) (key : Key)
    (sent : select members authorized ⟨custody, listed⟩ who audience = some key)
    (excluded : m ∉ members audience) (noLaterCopy : (key, m) ∉ later) :
    (key, m) ∉ listed ++ later := by
  intro copied
  rcases List.mem_append.mp copied with before | after
  · exact listed_excluded_cannot_read members authorized _ who audience key sent excluded before
  · exact noLaterCopy after

theorem listing_failure_blocks (custody : List Key) (stored : Copies) (failure : ListingFailure) :
    beginPass custody stored (.error failure) = .error failure := rfl

/-- Key introductions use the shared action's creation/rotation projection. -/
def introductions (M : CurrentReplay.Log) (e : Nat) : List Key :=
  ((M e).action.introduction.map fun (audience, key) => ⟨e, audience, key⟩).toList

def receivedAuthorized (H : CurrentReplay.History) (W n : Nat) (received : EntrySet)
    (key : Key) : Bool :=
  key ∈ CurrentKeys.receivedIntroductions H W n received

def selectInReplay (H : CurrentReplay.History) (W n : Nat) (received : EntrySet)
    (pass : Pass) (who : Nat) (audience : Audience) : Option Key :=
  select (audienceMembers (CurrentReplay.resolve H W n received).state)
    (receivedAuthorized H W n received) pass who audience

theorem selected_introduction_authorized (H : CurrentReplay.History) (W n : Nat)
    (received : EntrySet) (pass : Pass) (who : Nat) (audience : Audience) (key : Key)
    (sent : selectInReplay H W n received pass who audience = some key) :
    received key.entry = true ∧
    CurrentReplay.authorized (CurrentReplay.authorViews H W n n key.entry) (H.log key.entry) = true ∧
    key ∈ introductions H.log key.entry := by
  have h := (first_attempt_safe _ _ pass who audience key sent).2.2.1
  have accepted : key ∈ CurrentKeys.receivedIntroductions H W n received := by
    simpa [receivedAuthorized] using h
  obtain ⟨present, authorized, introduction⟩ := (CurrentKeys.introduction_source _ _ _ _).mp accepted
  exact ⟨(List.mem_filter.mp present).2, authorized, by simp [introductions, introduction]⟩

/-- Authorization is an immutable recorded-past check. A changed membership
replay cannot revoke it, even if the introducing entry is no longer kept. -/
theorem authorized_introduction_persists (H : CurrentReplay.History) (W n : Nat)
    (before after : EntrySet) (key : Key)
    (accepted : receivedAuthorized H W n before key = true)
    (retained : after key.entry = true) :
    receivedAuthorized H W n after key = true := by
  have present : key ∈ CurrentKeys.receivedIntroductions H W n before := by
    simpa [receivedAuthorized] using accepted
  obtain ⟨present, authorized, introduction⟩ := (CurrentKeys.introduction_source _ _ _ _).mp present
  change decide (_ ∈ _) = true
  apply decide_eq_true
  apply (CurrentKeys.introduction_source _ _ _ _).mpr
  exact ⟨List.mem_filter.mpr ⟨(List.mem_filter.mp present).1, by simpa using retained⟩,
    authorized, introduction⟩

structure Attempt where
  key : Key
  bytes : List Nat
  deriving DecidableEq, Repr

def retry (attempt : Attempt) : Attempt := attempt

theorem retry_fixed (attempt : Attempt) :
    (retry attempt).key = attempt.key ∧ (retry attempt).bytes = attempt.bytes := ⟨rfl, rfl⟩

end CovenStorelogData.KeySelection
