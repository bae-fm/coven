import CovenStorelog.CurrentReplay
import CovenStorelogData.Keys

/-! §11: a successful pass lists immutable sealed copies from storage.
The listing is shared exposure evidence; custody remains device-local.
A later share changes storage, never the already captured pass. -/
namespace CovenStorelogData.KeySelection
open CovenStorelog

structure Key where
  id : Nat
  entry : Nat
  audience : Audience
  deriving DecidableEq, Repr

/-- Abstract paths keys/store/key/member and keys/circles/circle/key/member.
Fresh key ids identify one introduction; ciphertext is outside the model. -/
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

def newer (a b : Key) : Bool :=
  b.entry < a.entry || (b.entry == a.entry && b.id < a.id)

/-- Introduction timestamp, then key id. Concurrent rotations coexist. -/
def newest : List Key → Option Key
  | [] => none
  | k :: ks => match newest ks with
    | none => some k
    | some other => some (if newer k other then k else other)

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
    key ∈ keys := by
  induction keys with
  | nil => cases h
  | cons k ks ih =>
      simp only [newest] at h
      cases hn : newest ks with
      | none => simp [hn] at h; subst key; simp
      | some other =>
          simp only [hn, Option.some.injEq] at h
          split at h
          · subst key; simp
          · subst key; exact List.mem_cons_of_mem _ (ih hn)

theorem newer_trans {a b c : Key} (ab : newer a b = true) (bc : newer b c = true) :
    newer a c = true := by
  simp only [newer, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq,
    beq_iff_eq] at *
  omega

theorem newest_maximal {keys : List Key} {key : Key} (h : newest keys = some key) :
    ∀ other ∈ keys, newer other key = false := by
  induction keys generalizing key with
  | nil => cases h
  | cons k ks ih =>
      simp only [newest] at h
      cases hn : newest ks with
      | none =>
          have empty : ks = [] := by
            cases ks with
            | nil => rfl
            | cons a rest =>
                simp only [newest] at hn
                cases hr : newest rest <;> simp [hr] at hn
          subst ks
          simp only [hn, Option.some.injEq] at h
          subst key
          simp [newer]
      | some winner =>
          have greatest := ih hn
          simp only [hn, Option.some.injEq] at h
          split at h
          · rename_i wins
            subst key
            intro other present
            rcases List.mem_cons.mp present with rfl | present
            · simp [newer]
            · cases ho : newer other k
              · rfl
              · have ht := newer_trans ho wins
                simp [greatest other present] at ht
          · rename_i loses
            subst key
            intro other present
            rcases List.mem_cons.mp present with rfl | present
            · exact Bool.eq_false_iff.mpr loses
            · exact greatest other present

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

/-- Removal and creation keys use their checked wire ids as an input; tag 15
carries its id directly. Membership/authority are derived by the shared replay. -/
def introductions (M : Log) (ids : Nat → Audience → Nat) (e : Nat) : List Key :=
  (introduced M e).map fun k =>
    ⟨match (M e).action with | .rotateKey _ id => id | _ => ids e k.audience,
      e, k.audience⟩

def receivedAuthorized (H : Finality.History) (W n : Nat) (received : EntrySet)
    (ids : Nat → Audience → Nat) (key : Key) : Bool :=
  key.entry < n && received key.entry &&
    authorized (CurrentReplay.authorViews H W n n key.entry) (H.log key.entry) &&
    CurrentReplay.keysMatch (CurrentReplay.authorViews H W n n key.entry) (H.log key.entry) &&
    key ∈ introductions H.log ids key.entry

def selectInReplay (H : Finality.History) (W n : Nat) (received : EntrySet)
    (ids : Nat → Audience → Nat) (pass : Pass) (who : Nat) (audience : Audience) : Option Key :=
  select (audienceMembers (CurrentReplay.resolve H W n received).state)
    (receivedAuthorized H W n received ids) pass who audience

theorem selected_introduction_authorized (H : Finality.History) (W n : Nat)
    (received : EntrySet) (ids : Nat → Audience → Nat) (pass : Pass)
    (who : Nat) (audience : Audience) (key : Key)
    (sent : selectInReplay H W n received ids pass who audience = some key) :
    received key.entry = true ∧
    authorized (CurrentReplay.authorViews H W n n key.entry) (H.log key.entry) = true ∧
    key ∈ introductions H.log ids key.entry := by
  have h := (first_attempt_safe _ _ pass who audience key sent).2.2.1
  simp only [receivedAuthorized, Bool.and_eq_true, decide_eq_true_eq] at h
  exact ⟨h.1.1.1.2, h.1.1.2, h.2⟩

structure Attempt where
  key : Key
  bytes : List Nat
  deriving DecidableEq, Repr

def retry (attempt : Attempt) : Attempt := attempt

theorem retry_fixed (attempt : Attempt) :
    (retry attempt).key = attempt.key ∧ (retry attempt).bytes = attempt.bytes := ⟨rfl, rfl⟩

end CovenStorelogData.KeySelection
