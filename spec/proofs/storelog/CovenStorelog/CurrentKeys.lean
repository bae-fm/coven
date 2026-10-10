import CovenStorelog.CurrentReplay

/-! §11 key provenance, opened-copy checks and first attempts. Cryptography
supplies a hash parameter; byte uniqueness explicitly assumes injectivity. -/
namespace CovenStorelog.CurrentKeys
open CurrentReplay (KeyBytes KeyHash KeyCommitment)

structure KeyId where
  audience : Audience
  id : Nat
  deriving DecidableEq, Repr

structure Introduction where
  entry : Nat
  audience : Audience
  key : KeyCommitment
  deriving DecidableEq, Repr

def Introduction.identity (i : Introduction) : KeyId := ⟨i.audience, i.key.id⟩

def introductions (M : CurrentReplay.Log) (views : Nat → State) (entries : List Nat) :
    List Introduction :=
  entries.filterMap fun e =>
    if CurrentReplay.authorized (views e) (M e) then
      (M e).action.introduction.map fun (a, key) => ⟨e, a, key⟩
    else none

theorem introduction_source (M : CurrentReplay.Log) (views : Nat → State)
    (entries : List Nat) (i : Introduction) :
    i ∈ introductions M views entries ↔
      i.entry ∈ entries ∧ CurrentReplay.authorized (views i.entry) (M i.entry) = true ∧
      (M i.entry).action.introduction = some (i.audience, i.key) := by
  simp only [introductions, List.mem_filterMap]
  constructor
  · rintro ⟨e, he, hi⟩
    split at hi
    · rename_i auth
      cases hx : (M e).action.introduction with
      | none => simp [hx] at hi
      | some pair =>
        rcases pair with ⟨a, key⟩
        simp only [hx, Option.map_some, Option.some.injEq] at hi
        subst i
        exact ⟨he, auth, hx⟩
    · cases hi
  · rintro ⟨he, auth, hi⟩
    exact ⟨i.entry, he, by simp [auth, hi]⟩

/-- Received authorized introductions survive membership replay reversals.
Kept introductions are the subset used to justify newly published keys. -/
def receivedIntroductions (H : CurrentReplay.History) (W n : Nat) (S : EntrySet) :
    List Introduction :=
  introductions H.log (CurrentReplay.authorViews H W n n) ((List.range n).filter S)

def keptIntroductions (H : CurrentReplay.History) (W n : Nat) (S : EntrySet) :
    List Introduction :=
  introductions H.log (CurrentReplay.authorViews H W n n) (CurrentReplay.resolve H W n S).kept

theorem kept_introduction_received (H : CurrentReplay.History) (W n : Nat) (S : EntrySet)
    (i : Introduction) (hi : i ∈ keptIntroductions H W n S) :
    i ∈ receivedIntroductions H W n S := by
  obtain ⟨kept, auth, action⟩ := (introduction_source _ _ _ _).mp hi
  have received := CurrentReplay.kept_received H W n S i.entry kept
  exact (introduction_source _ _ _ _).mpr
    ⟨by simp [received.1, received.2], auth, action⟩

theorem kept_key_origin (H : CurrentReplay.History) (W n : Nat) (S : EntrySet)
    (i : Introduction) (hi : i ∈ keptIntroductions H W n S) :
    (i.audience = .store ∧ ∃ access, (H.log i.entry).action = .create access i.key) ∨
    (∃ c name, i.audience = .circle c ∧ (H.log i.entry).action = .makeCircle c name i.key) ∨
    (H.log i.entry).action = .rotateKey i.audience i.key :=
  CurrentReplay.introduction_cases _ _ _ ((introduction_source _ _ _ _).mp hi).2.2

/-- Creation puts its author in the new circle. Every other circle-key
introduction requires membership of that circle in the recorded past. -/
theorem circle_key_authority (M : CurrentReplay.Log) (views : Nat → State)
    (entries : List Nat) (i : Introduction) (c : Nat)
    (hi : i ∈ introductions M views entries) (circle : i.audience = .circle c) :
    (∃ name, (M i.entry).action = .makeCircle c name i.key ∧
      member (views i.entry) (M i.entry).author = true) ∨
    ((M i.entry).action = .rotateKey (.circle c) i.key ∧
      inCircle (views i.entry) c (M i.entry).author = true) := by
  obtain ⟨_, auth, intro⟩ := (introduction_source _ _ _ _).mp hi
  rw [circle] at intro
  rcases CurrentReplay.introduction_cases _ _ _ intro with h | h | h
  · cases h.1
  · obtain ⟨d, name, eq, action⟩ := h
    cases eq
    exact Or.inl ⟨name, action, by simpa [CurrentReplay.authorized,
      CurrentReplay.Entry.membership, CurrentReplay.Action.membership, authorized, action] using auth⟩
  · exact Or.inr ⟨h, by simpa [CurrentReplay.authorized, CurrentReplay.Entry.membership,
      CurrentReplay.Action.membership, authorized, h] using auth⟩

/-- Conflicting commitments for the same audience/id refuse acceptance. This
also gives uniqueness without an axiom about random id generation. -/
def hashFor (is : List Introduction) (key : KeyId) : Option KeyHash :=
  let matching := is.filter (fun i => i.identity == key)
  match matching with
  | [] => none
  | first :: rest =>
    if rest.all (fun i => i.key.keyHash == first.key.keyHash) then some first.key.keyHash else none

theorem hashFor_source (is : List Introduction) (key : KeyId) (h : KeyHash)
    (accepted : hashFor is key = some h) :
    ∃ i ∈ is, i.identity = key ∧ i.key.keyHash = h := by
  unfold hashFor at accepted
  cases he : is.filter (fun i => i.identity == key) with
  | nil => simp [he] at accepted
  | cons i rest =>
    rw [he] at accepted
    dsimp only at accepted
    split at accepted
    · cases accepted
      have mem : i ∈ is.filter (fun i => i.identity == key) := by simp [he]
      have hm := List.mem_filter.mp mem
      exact ⟨i, hm.1, by simpa using hm.2, rfl⟩
    · cases accepted

theorem hashFor_agrees (is : List Introduction) (key : KeyId) (h : KeyHash)
    (accepted : hashFor is key = some h) (i : Introduction)
    (mem : i ∈ is) (identity : i.identity = key) : i.key.keyHash = h := by
  have mi : i ∈ is.filter (fun i => i.identity == key) := by simp [mem, identity]
  unfold hashFor at accepted
  cases he : is.filter (fun i => i.identity == key) with
  | nil => simp [he] at mi
  | cons first rest =>
    rw [he] at accepted mi
    dsimp only at accepted
    split at accepted
    · rename_i all
      cases accepted
      rcases List.mem_cons.mp mi with rfl | mem
      · rfl
      · simpa using List.all_eq_true.mp all i mem
    · cases accepted

structure Payload where
  key : KeyId
  bytes : KeyBytes
  deriving DecidableEq, Repr

/-- The payload describes the outcome if the recipient opens the box. Other
members cannot inspect it; their exposure check uses only its listed path. -/
structure Copy where
  key : KeyId
  recipient : Nat
  opened : Option Payload
  deriving DecidableEq, Repr

def accepts (hash : KeyBytes → KeyHash) (is : List Introduction) (key : KeyId)
    (bytes : KeyBytes) : Bool := hashFor is key == some (hash bytes)

def acceptedCopy (hash : KeyBytes → KeyHash) (is : List Introduction) (self : Nat)
    (copy : Copy) : Option Payload := do
  if copy.recipient != self then none else
  let payload ← copy.opened
  if payload.key == copy.key && accepts hash is copy.key payload.bytes then some payload else none

theorem accepted_bytes_unique (hash : KeyBytes → KeyHash)
    (injective : ∀ a b, hash a = hash b → a = b)
    (is : List Introduction) (key : KeyId) (a b : KeyBytes)
    (ha : accepts hash is key a = true) (hb : accepts hash is key b = true) : a = b := by
  have ha : hashFor is key = some (hash a) := by simpa [accepts] using ha
  have hb : hashFor is key = some (hash b) := by simpa [accepts] using hb
  exact injective a b (Option.some.inj (ha.symm.trans hb))

theorem other_bytes_rejected (hash : KeyBytes → KeyHash)
    (injective : ∀ a b, hash a = hash b → a = b)
    (is : List Introduction) (i : Introduction) (original other : KeyBytes)
    (hi : i ∈ is) (committed : i.key.keyHash = hash original) (different : other ≠ original) :
    accepts hash is i.identity other = false := by
  cases ha : accepts hash is i.identity other
  · rfl
  · have hf : hashFor is i.identity = some (hash other) := by simpa [accepts] using ha
    have same := hashFor_agrees is i.identity (hash other) hf i hi rfl
    exact False.elim (different (injective _ _ (same.symm.trans committed)))

def exposure (hash : KeyBytes → KeyHash) (is : List Introduction) (self : Nat)
    (copy : Copy) : Bool :=
  if copy.recipient == self then (acceptedCopy hash is self copy).isSome else true

theorem mismatching_copy_no_key_or_exposure (hash : KeyBytes → KeyHash)
    (is : List Introduction) (self : Nat) (copy : Copy) (payload : Payload)
    (own : copy.recipient = self) (opened : copy.opened = some payload)
    (mismatch : hashFor is copy.key ≠ some (hash payload.bytes)) :
    acceptedCopy hash is self copy = none ∧ exposure hash is self copy = false := by
  have reject : accepts hash is copy.key payload.bytes = false := by simp [accepts, mismatch]
  simp [acceptedCopy, exposure, own, opened, reject]

theorem foreign_copy_counts (hash : KeyBytes → KeyHash) (is : List Introduction)
    (self : Nat) (copy : Copy) (other : copy.recipient ≠ self) :
    acceptedCopy hash is self copy = none ∧ exposure hash is self copy = true := by
  simp [acceptedCopy, exposure, other]

def belongs (s : State) (a : Audience) (m : Nat) : Bool :=
  match a with
  | .store => member s m
  | .circle c => inCircle s c m

def exposed (hash : KeyBytes → KeyHash) (is : List Introduction) (s : State)
    (self : Nat) (copies : List Copy) (key : KeyId) : Bool :=
  copies.any fun copy => copy.key == key && !belongs s key.audience copy.recipient &&
    exposure hash is self copy

/-- Custody is checked against the authorized commitment even when a caller
supplies an arbitrary local candidate. Nothing tentative is usable. -/
def usable (hash : KeyBytes → KeyHash) (is : List Introduction) (s : State)
    (self : Nat) (copies : List Copy) (held : List Payload) (i : Introduction) : Bool :=
  (held.any fun p => p.key == i.identity && accepts hash is p.key p.bytes) &&
    !exposed hash is s self copies i.identity

theorem excluded_copy_unusable (hash : KeyBytes → KeyHash) (is : List Introduction)
    (s : State) (self : Nat) (copies : List Copy) (held : List Payload)
    (i : Introduction) (copy : Copy) (listed : copy ∈ copies)
    (same : copy.key = i.identity) (excluded : belongs s i.audience copy.recipient = false)
    (evidence : exposure hash is self copy = true) :
    usable hash is s self copies held i = false := by
  have exp : exposed hash is s self copies i.identity = true := by
    apply List.any_eq_true.mpr
    exact ⟨copy, listed, by simp [same, Introduction.identity, excluded, evidence]⟩
  simp [usable, exp]

/-- Complete means the pass finished both listings. An interrupted pass
cannot be used to start sending with an older selection. -/
inductive Listing where
  | incomplete
  | complete (copies : List Copy)
  deriving DecidableEq, Repr

inductive Wait where
  | listing
  | membership
  | key
  deriving DecidableEq, Repr

inductive Selection where
  | use (introduction : Introduction)
  | pending (reason : Wait)
  deriving DecidableEq, Repr

/-- Introduction timestamps, then key ids, order usable candidates. -/
def newer (a b : Introduction) : Bool :=
  a.entry > b.entry || (a.entry == b.entry && a.key.id > b.key.id)

def newest : List Introduction → Option Introduction
  | [] => none
  | i :: rest => match newest rest with
    | none => some i
    | some j => some (if newer i j then i else j)

theorem newest_mem (is : List Introduction) (i : Introduction) (h : newest is = some i) :
    i ∈ is := by
  induction is with
  | nil => cases h
  | cons a rest ih =>
    simp only [newest] at h
    cases he : newest rest with
    | none => simp [he] at h; subst i; exact List.mem_cons_self
    | some b =>
      simp only [he] at h
      split at h
      · cases h; exact List.mem_cons_self
      · cases h; exact List.mem_cons_of_mem _ (ih he)

def select (hash : KeyBytes → KeyHash) (is : List Introduction) (s : State)
    (self : Nat) (listing : Listing) (held : List Payload) (audience : Audience) : Selection :=
  match listing with
  | .incomplete => .pending .listing
  | .complete copies =>
    if !belongs s audience self then .pending .membership else
    match newest (is.filter fun i => i.audience == audience && usable hash is s self copies held i) with
    | none => .pending .key
    | some i => .use i

theorem selected_usable (hash : KeyBytes → KeyHash) (is : List Introduction) (s : State)
    (self : Nat) (listing : Listing) (held : List Payload) (audience : Audience)
    (i : Introduction) (selected : select hash is s self listing held audience = .use i) :
    ∃ copies, listing = .complete copies ∧ i ∈ is ∧ i.audience = audience ∧
      belongs s audience self = true ∧ usable hash is s self copies held i = true := by
  cases listing with
  | incomplete => cases selected
  | complete copies =>
    simp only [select] at selected
    split at selected
    · cases selected
    · rename_i member
      cases hn : newest (is.filter fun i => i.audience == audience && usable hash is s self copies held i) with
      | none => simp [hn] at selected
      | some j =>
        simp only [hn, Selection.use.injEq] at selected
        subst i
        obtain ⟨mem, good⟩ := List.mem_filter.mp (newest_mem _ _ hn)
        have good := Bool.and_eq_true_iff.mp good
        exact ⟨copies, rfl, mem, by simpa using good.1, by simpa using member, good.2⟩

theorem usable_or_pending (hash : KeyBytes → KeyHash) (is : List Introduction) (s : State)
    (self : Nat) (listing : Listing) (held : List Payload) (audience : Audience) :
    (∃ i copies, select hash is s self listing held audience = .use i ∧
      listing = .complete copies ∧ usable hash is s self copies held i = true) ∨
    ∃ reason, select hash is s self listing held audience = .pending reason := by
  cases hs : select hash is s self listing held audience with
  | use i =>
    obtain ⟨copies, hl, _, _, _, hu⟩ := selected_usable _ _ _ _ _ _ _ _ hs
    exact Or.inl ⟨i, copies, rfl, hl, hu⟩
  | pending reason => exact Or.inr ⟨reason, rfl⟩

/-- Safety is relative to this pass's complete listing. A valid opened box
for self counts; every foreign box counts without decrypting it. -/
theorem selected_no_excluded_holder (hash : KeyBytes → KeyHash) (is : List Introduction)
    (s : State) (self : Nat) (copies : List Copy) (held : List Payload) (audience : Audience)
    (i : Introduction) (selected : select hash is s self (.complete copies) held audience = .use i)
    (copy : Copy) (listed : copy ∈ copies) (same : copy.key = i.identity)
    (evidence : exposure hash is self copy = true) :
    belongs s audience copy.recipient = true := by
  obtain ⟨cs, eq, _, aud, _, good⟩ := selected_usable _ _ _ _ _ _ _ _ selected
  cases eq
  cases hm : belongs s audience copy.recipient
  · have bad := excluded_copy_unusable hash is s self copies held i copy listed same
      (by simpa [aud] using hm) evidence
    simp [bad] at good
  · rfl

/-- In particular the safety argument needs no plaintext from other boxes. -/
theorem selected_no_excluded_foreign_copy (hash : KeyBytes → KeyHash) (is : List Introduction)
    (s : State) (self : Nat) (copies : List Copy) (held : List Payload) (audience : Audience)
    (i : Introduction) (selected : select hash is s self (.complete copies) held audience = .use i)
    (copy : Copy) (listed : copy ∈ copies) (same : copy.key = i.identity)
    (other : copy.recipient ≠ self) : belongs s audience copy.recipient = true :=
  selected_no_excluded_holder _ _ _ _ _ _ _ _ selected copy listed same
    (foreign_copy_counts _ _ _ _ other).2

theorem accepted_copy_checked (hash : KeyBytes → KeyHash) (is : List Introduction)
    (self : Nat) (copy : Copy) (p : Payload)
    (accepted : acceptedCopy hash is self copy = some p) :
    copy.recipient = self ∧ copy.opened = some p ∧ p.key = copy.key ∧
      accepts hash is copy.key p.bytes = true := by
  simp only [acceptedCopy] at accepted
  split at accepted
  · cases accepted
  · rename_i own
    have own : copy.recipient = self := by simpa using own
    cases ho : copy.opened with
    | none => simp [ho] at accepted
    | some payload =>
      simp only [ho, bind, Option.bind] at accepted
      split at accepted
      · rename_i checks
        cases accepted
        have checks := Bool.and_eq_true_iff.mp checks
        exact ⟨own, rfl, by simpa using checks.1, checks.2⟩
      · cases accepted

theorem accepted_copy_authorized (H : CurrentReplay.History) (W n : Nat) (S : EntrySet)
    (hash : KeyBytes → KeyHash) (self : Nat) (copy : Copy) (p : Payload)
    (accepted : acceptedCopy hash (receivedIntroductions H W n S) self copy = some p) :
    ∃ i : Introduction, i.entry < n ∧ S i.entry = true ∧
      CurrentReplay.authorized (CurrentReplay.authorViews H W n n i.entry) (H.log i.entry) = true ∧
      (H.log i.entry).action.introduction = some (i.audience, i.key) ∧
      i.identity = p.key ∧ i.key.keyHash = hash p.bytes := by
  obtain ⟨_, _, same, ha⟩ := accepted_copy_checked _ _ _ _ _ accepted
  have hf : hashFor (receivedIntroductions H W n S) copy.key = some (hash p.bytes) := by
    simpa [accepts] using ha
  obtain ⟨i, hi, identity, committed⟩ := hashFor_source _ _ _ hf
  obtain ⟨mem, auth, intro⟩ := (introduction_source _ _ _ _).mp hi
  obtain ⟨lt, received⟩ := List.mem_filter.mp mem
  exact ⟨i, List.mem_range.mp lt, received, auth, intro, identity.trans same.symm, committed⟩

theorem accepted_copies_unique (hash : KeyBytes → KeyHash)
    (injective : ∀ a b, hash a = hash b → a = b) (is : List Introduction)
    (self other : Nat) (a b : Copy) (pa pb : Payload)
    (ha : acceptedCopy hash is self a = some pa)
    (hb : acceptedCopy hash is other b = some pb) (same : a.key = b.key) : pa.bytes = pb.bytes := by
  have ac := (accepted_copy_checked _ _ _ _ _ ha).2.2.2
  have bc := (accepted_copy_checked _ _ _ _ _ hb).2.2.2
  rw [← same] at bc
  exact accepted_bytes_unique _ injective _ _ _ _ ac bc

/-- More received introductions cannot replace a previously accepted value
with different bytes. Conflicting hashes block acceptance instead. -/
theorem accepted_bytes_stable (hash : KeyBytes → KeyHash)
    (injective : ∀ a b, hash a = hash b → a = b)
    (before after : List Introduction) (includes : ∀ i ∈ before, i ∈ after)
    (key : KeyId) (a b : KeyBytes)
    (ha : accepts hash before key a = true) (hb : accepts hash after key b = true) : a = b := by
  have ha : hashFor before key = some (hash a) := by simpa [accepts] using ha
  have hb : hashFor after key = some (hash b) := by simpa [accepts] using hb
  obtain ⟨i, hi, identity, committed⟩ := hashFor_source _ _ _ ha
  exact injective _ _ (committed.symm.trans (hashFor_agrees _ _ _ hb i (includes i hi) identity))

theorem conflicting_hashes_rejected (is : List Introduction) (a b : Introduction)
    (ha : a ∈ is) (hb : b ∈ is) (same : a.identity = b.identity)
    (different : a.key.keyHash ≠ b.key.keyHash) : hashFor is a.identity = none := by
  cases hf : hashFor is a.identity with
  | none => rfl
  | some h =>
    have eqA := hashFor_agrees _ _ _ hf a ha rfl
    have eqB := hashFor_agrees _ _ _ hf b hb same.symm
    exact False.elim (different (eqA.trans eqB.symm))

theorem selected_no_excluded_copy (hash : KeyBytes → KeyHash) (is : List Introduction)
    (s : State) (self : Nat) (copies : List Copy) (held : List Payload) (audience : Audience)
    (i : Introduction) (selected : select hash is s self (.complete copies) held audience = .use i)
    (copy : Copy) (listed : copy ∈ copies) (same : copy.key = i.identity) :
    belongs s audience copy.recipient = true := by
  by_cases own : copy.recipient = self
  · obtain ⟨_, _, _, _, member, _⟩ := selected_usable _ _ _ _ _ _ _ _ selected
    simpa [own] using member
  · exact selected_no_excluded_foreign_copy _ _ _ _ _ _ _ _ selected copy listed same own

theorem newest_none (is : List Introduction) : newest is = none ↔ is = [] := by
  cases is with
  | nil => simp [newest]
  | cons a rest => cases h : newest rest <;> simp [newest, h]

theorem replacement_avoids_key_wait (hash : KeyBytes → KeyHash) (is : List Introduction)
    (s : State) (self : Nat) (copies : List Copy) (held : List Payload) (audience : Audience)
    (member : belongs s audience self = true) (i : Introduction) (hi : i ∈ is)
    (aud : i.audience = audience) (good : usable hash is s self copies held i = true) :
    ∃ selected, select hash is s self (.complete copies) held audience = .use selected := by
  have mem : i ∈ is.filter (fun i => i.audience == audience && usable hash is s self copies held i) :=
    by simp [hi, aud, good]
  cases hn : newest (is.filter (fun i => i.audience == audience && usable hash is s self copies held i)) with
  | none => simp [(newest_none _).mp hn] at mem
  | some j => exact ⟨j, by simp [select, member, hn]⟩

theorem newest_maximal (is : List Introduction) (i : Introduction)
    (selected : newest is = some i) (other : Introduction) (mem : other ∈ is) :
    other.entry ≤ i.entry ∧ (other.entry = i.entry → other.key.id ≤ i.key.id) := by
  induction is generalizing i other with
  | nil => cases selected
  | cons first rest ih =>
    simp only [newest] at selected
    cases hn : newest rest with
    | none =>
      have empty := (newest_none _).mp hn
      subst rest
      simp only [List.mem_cons, List.not_mem_nil, or_false] at mem
      subst other
      simp [newest] at selected
      subst i
      exact ⟨Nat.le_refl _, fun _ => Nat.le_refl _⟩
    | some next =>
      simp only [hn] at selected
      have leFirst : ∀ a ∈ rest, a.entry ≤ next.entry ∧
          (a.entry = next.entry → a.key.id ≤ next.key.id) := fun a ha => ih next hn a ha
      split at selected
      · rename_i newer
        have eq : first = i := Option.some.inj selected
        subst i
        simp only [CurrentKeys.newer, Bool.or_eq_true, Bool.and_eq_true,
          decide_eq_true_eq, beq_iff_eq] at newer
        rcases List.mem_cons.mp mem with rfl | mem
        · exact ⟨Nat.le_refl _, fun _ => Nat.le_refl _⟩
        · have h := leFirst other mem
          constructor <;> omega
      · rename_i newer
        have eq : next = i := Option.some.inj selected
        subst i
        have ord : first.entry ≤ next.entry ∧
            (first.entry = next.entry → first.key.id ≤ next.key.id) := by
          simp only [CurrentKeys.newer, Bool.or_eq_true, Bool.and_eq_true,
            decide_eq_true_eq, beq_iff_eq] at newer
          constructor <;> omega
        rcases List.mem_cons.mp mem with rfl | mem
        · exact ord
        · exact leFirst other mem

/-- First attempts use the pass's actual replay and authorized received
introductions, not a caller-selected membership or commitment list. -/
def firstAttempt (H : CurrentReplay.History) (W n : Nat) (S : EntrySet)
    (hash : KeyBytes → KeyHash) (self : Nat) (listing : Listing)
    (held : List Payload) (audience : Audience) : Selection :=
  select hash (receivedIntroductions H W n S) (CurrentReplay.resolve H W n S).state
    self listing held audience

theorem first_attempt_safe (H : CurrentReplay.History) (W n : Nat) (S : EntrySet)
    (hash : KeyBytes → KeyHash) (self : Nat) (copies : List Copy)
    (held : List Payload) (audience : Audience) (i : Introduction)
    (selected : firstAttempt H W n S hash self (.complete copies) held audience = .use i)
    (copy : Copy) (listed : copy ∈ copies) (same : copy.key = i.identity) :
    belongs (CurrentReplay.resolve H W n S).state audience copy.recipient = true :=
  selected_no_excluded_copy _ _ _ _ _ _ _ _ selected copy listed same

theorem first_attempt_authorized (H : CurrentReplay.History) (W n : Nat) (S : EntrySet)
    (hash : KeyBytes → KeyHash) (self : Nat) (listing : Listing)
    (held : List Payload) (audience : Audience) (i : Introduction)
    (selected : firstAttempt H W n S hash self listing held audience = .use i) :
    i.entry < n ∧ S i.entry = true ∧
      CurrentReplay.authorized (CurrentReplay.authorViews H W n n i.entry) (H.log i.entry) = true ∧
      (H.log i.entry).action.introduction = some (audience, i.key) := by
  obtain ⟨_, _, hi, aud, _, _⟩ := selected_usable _ _ _ _ _ _ _ _ selected
  obtain ⟨mem, auth, intro⟩ := (introduction_source _ _ _ _).mp hi
  obtain ⟨lt, received⟩ := List.mem_filter.mp mem
  exact ⟨List.mem_range.mp lt, received, auth, by simpa [aud] using intro⟩

theorem selected_newest_usable (hash : KeyBytes → KeyHash) (is : List Introduction) (s : State)
    (self : Nat) (copies : List Copy) (held : List Payload) (audience : Audience)
    (i : Introduction) (selected : select hash is s self (.complete copies) held audience = .use i)
    (other : Introduction) (mem : other ∈ is) (aud : other.audience = audience)
    (good : usable hash is s self copies held other = true) :
    other.entry ≤ i.entry ∧ (other.entry = i.entry → other.key.id ≤ i.key.id) := by
  simp only [select] at selected
  split at selected
  · cases selected
  · cases hn : newest (is.filter fun i => i.audience == audience && usable hash is s self copies held i) with
    | none => simp [hn] at selected
    | some j =>
      simp only [hn, Selection.use.injEq] at selected
      subst i
      exact newest_maximal _ _ hn other (by simp [mem, aud, good])

end CovenStorelog.CurrentKeys
