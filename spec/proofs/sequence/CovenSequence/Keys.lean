import CovenSequence.Cutoff

/-!
# Audience keys in the sequence (§11)

This is the key projection of authenticated slots, not a second log. An
introduction is projected only from an applied creation/rotation; every signed
copy is projected, including copies whose recipient never becomes authorized.
Membership is the preceding sequence ledger (`credentials`). Custody is local:
holding bytes does not establish an introduction or authority.

Natural numbers denote random key ids, exact sealing credentials and hash
tokens. Hash equality checks the material committed under a named key.
Distinct key identities stand for independently generated, non-aliasing
secrets (§2's random-generation assumption). Encryption is idealized below by
observation with the matching secret, not implemented or proved
cryptographically. No theorem promises delivery, custody availability,
fairness, or secrecy against an unobserved in-flight copy.
-/
namespace CovenSequence.Keys

structure Key (Aud : Type) where
  audience : Aud
  id : Nat
  deriving DecidableEq

structure Introduction (Aud : Type) where
  key : Key Aud
  hash : Nat
  slot : Nat
  deriving DecidableEq

structure Route (Aud Mem : Type) where
  key : Key Aud
  member : Mem
  credential : Nat
  deriving DecidableEq

inductive Event (Aud Mem : Type) where
  | introduce (key : Key Aud) (hash : Nat)
  | copy (route : Route Aud Mem)
  | other
  deriving DecidableEq

structure History (Aud Mem : Type) where
  introductions : List (Introduction Aud)
  copies : List (Route Aud Mem)
  deriving DecidableEq

variable {Aud Mem : Type} [DecidableEq Aud]

abbrev Custody (Aud : Type) := Key Aud → Option Nat
abbrev Current (Aud Mem : Type) := Aud → Mem → Option Nat

/-- Newest introductions first; repeated copies remain independent slots. -/
def step (h : History Aud Mem) (slot : Nat) : Event Aud Mem → History Aud Mem
  | .introduce key hash => { h with introductions := ⟨key, hash, slot⟩ :: h.introductions }
  | .copy route => { h with copies := route :: h.copies }
  | .other => h

def historyAt (seq : Nat → Event Aud Mem) : Nat → History Aud Mem
  | 0 => ⟨[], []⟩
  | n + 1 => step (historyAt seq n) (n + 1) (seq (n + 1))

omit [DecidableEq Aud] in
/-- Exact provenance, including the introduction's actual sequence position. -/
theorem introduction_slot (seq : Nat → Event Aud Mem) (i : Introduction Aud) :
    ∀ n, i ∈ (historyAt seq n).introductions ↔
      0 < i.slot ∧ i.slot ≤ n ∧ seq i.slot = .introduce i.key i.hash := by
  intro n
  induction n with
  | zero => simp [historyAt]; omega
  | succ n ih =>
    rw [historyAt]
    cases hs : seq (n + 1) with
    | introduce key hash =>
      simp only [step, List.mem_cons, ih]
      constructor
      · rintro (rfl | ⟨hp, hn, he⟩)
        · exact ⟨Nat.zero_lt_succ n, Nat.le_refl _, hs⟩
        · exact ⟨hp, by omega, he⟩
      · rintro ⟨hp, hn, he⟩
        by_cases same : i.slot = n + 1
        · rw [same, hs] at he
          cases i
          simp_all
        · exact Or.inr ⟨hp, by omega, he⟩
    | copy route | other =>
      simp only [step, ih]
      constructor
      · rintro ⟨hp, hn, he⟩; exact ⟨hp, by omega, he⟩
      · rintro ⟨hp, hn, he⟩
        have ne : i.slot ≠ n + 1 := by intro eq; rw [eq, hs] at he; cases he
        exact ⟨hp, by omega, he⟩

omit [DecidableEq Aud] in
theorem copy_slot (seq : Nat → Event Aud Mem) (r : Route Aud Mem) :
    ∀ n, r ∈ (historyAt seq n).copies ↔ ∃ s, 0 < s ∧ s ≤ n ∧ seq s = .copy r := by
  intro n
  induction n with
  | zero => simp [historyAt]; omega
  | succ n ih =>
    rw [historyAt]
    cases hs : seq (n + 1) with
    | copy route =>
      simp only [step, List.mem_cons, ih]
      constructor
      · rintro (rfl | ⟨s, hp, hn, he⟩)
        · exact ⟨n + 1, by omega, Nat.le_refl _, hs⟩
        · exact ⟨s, hp, by omega, he⟩
      · rintro ⟨s, hp, hn, he⟩
        by_cases same : s = n + 1
        · rw [same, hs] at he; cases he; exact Or.inl rfl
        · exact Or.inr ⟨s, hp, by omega, he⟩
    | introduce key hash | other =>
      simp only [step, ih]
      constructor
      · rintro ⟨s, hp, hn, he⟩; exact ⟨s, hp, by omega, he⟩
      · rintro ⟨s, hp, hn, he⟩
        have ne : s ≠ n + 1 := by intro eq; rw [eq, hs] at he; cases he
        exact ⟨s, hp, by omega, he⟩

/-- Conflicting commitments cannot authorize either material under one id. -/
def accepted (h : History Aud Mem) (held : Custody Aud) (key : Key Aud) : Bool :=
  h.introductions.any (fun i => decide (i.key = key)) &&
    h.introductions.all (fun i => decide (i.key = key → held key = some i.hash))

theorem accepted_iff (h : History Aud Mem) (held : Custody Aud) (key : Key Aud) :
    accepted h held key = true ↔
      (∃ i ∈ h.introductions, i.key = key) ∧
      ∀ i ∈ h.introductions, i.key = key → held key = some i.hash := by
  simp only [accepted, Bool.and_eq_true, List.any_eq_true, List.all_eq_true, decide_eq_true_eq]

/-- A tentative copy cannot authorize use before its introduction slot. -/
theorem accepted_from_introduction (seq : Nat → Event Aud Mem) (held : Custody Aud)
    (key : Key Aud) (n : Nat) (yes : accepted (historyAt seq n) held key = true) :
    ∃ s hash, 0 < s ∧ s ≤ n ∧ seq s = .introduce key hash ∧ held key = some hash := by
  obtain ⟨⟨i, hi, hk⟩, hashes⟩ := (accepted_iff _ _ _).mp yes
  have origin := (introduction_slot seq i n).mp hi
  exact ⟨i.slot, i.hash, origin.1, origin.2.1, hk ▸ origin.2.2, hashes i hi hk⟩

theorem introduction_accepts (h : History Aud Mem) (held : Custody Aud) (key : Key Aud)
    (hash slot : Nat) (fresh : ∀ i ∈ h.introductions, i.key ≠ key)
    (material : held key = some hash) :
    accepted (step h slot (.introduce key hash)) held key = true := by
  apply (accepted_iff _ _ _).mpr
  constructor
  · exact ⟨⟨key, hash, slot⟩, by simp [step], rfl⟩
  · intro i hi hk
    rcases List.mem_cons.mp hi with rfl | hi
    · exact material
    · exact False.elim (fresh i hi hk)

/-- Routing is exposure even if this device did not open the box. -/
def exposed (h : History Aud Mem) (current : Current Aud Mem) (key : Key Aud) : Bool :=
  h.copies.any fun r => decide (r.key = key ∧ current key.audience r.member ≠ some r.credential)

theorem exposed_iff (h : History Aud Mem) (current : Current Aud Mem) (key : Key Aud) :
    exposed h current key = true ↔ ∃ r ∈ h.copies,
      r.key = key ∧ current key.audience r.member ≠ some r.credential := by
  simp [exposed, List.any_eq_true]

/-- Keys this member's device could open with any of its retained credentials.
This conservatively includes unopened boxes; actual deletion from custody is
not evidence that a removed or stolen device forgot a secret. -/
def RoutedTo (h : History Aud Mem) (member : Mem) (credentials : List Nat) (key : Key Aud) : Prop :=
  ∃ r ∈ h.copies, r.key = key ∧ r.member = member ∧ r.credential ∈ credentials

theorem routed_exposure (h : History Aud Mem) (current : Current Aud Mem)
    (member : Mem) (credentials : List Nat) (key : Key Aud)
    (received : RoutedTo h member credentials key)
    (excluded : ∀ c ∈ credentials, current key.audience member ≠ some c) :
    exposed h current key = true := by
  obtain ⟨r, hr, hk, hm, hc⟩ := received
  exact (exposed_iff _ _ _).mpr ⟨r, hr, hk, hm ▸ excluded r.credential hc⟩

/-- The key projection uses the actual membership and credential generation
of the sequence ledger. `reads` supplies circle membership, not store roles. -/
def credentials {S : Sig} (L : Ledger S) (reads : S.Aud → S.Mem → Bool) : Current S.Aud S.Mem :=
  fun a m => if L.member m && reads a m then some (L.keyGen m) else none

theorem removed_exposure {S : Sig} (L : Ledger S) (reads : S.Aud → S.Mem → Bool)
    (h : History S.Aud S.Mem) (m : S.Mem) (cs : List Nat) (key : Key S.Aud)
    (removed : L.member m = false) (received : RoutedTo h m cs key) :
    exposed h (credentials L reads) key = true := by
  apply routed_exposure h _ m cs key received
  intro c _
  simp [credentials, removed]

theorem retired_credential_exposure {S : Sig} (L : Ledger S) (reads : S.Aud → S.Mem → Bool)
    (h : History S.Aud S.Mem) (m : S.Mem) (cs : List Nat) (key : Key S.Aud)
    (retired : ∀ c ∈ cs, c < L.keyGen m) (received : RoutedTo h m cs key) :
    exposed h (credentials L reads) key = true := by
  apply routed_exposure h _ m cs key received
  intro c hc
  have := retired c hc
  simp only [credentials]
  split
  · intro eq; have := Option.some.inj eq; omega
  · simp

/-- Select by introduction slot, independently of key-id order or copy order. -/
def newest : List (Introduction Aud) → Option (Introduction Aud)
  | [] => none
  | i :: rest => match newest rest with
    | none => some i
    | some j => if j.slot ≤ i.slot then some i else some j

omit [DecidableEq Aud] in
theorem newest_none (xs : List (Introduction Aud)) : newest xs = none ↔ xs = [] := by
  cases xs with
  | nil => simp [newest]
  | cons i rest => simp only [newest]; split <;> simp_all; split <;> simp

omit [DecidableEq Aud] in
theorem newest_max (xs : List (Introduction Aud)) (i : Introduction Aud)
    (selected : newest xs = some i) : i ∈ xs ∧ ∀ j ∈ xs, j.slot ≤ i.slot := by
  induction xs generalizing i with
  | nil => cases selected
  | cons x xs ih =>
    simp only [newest] at selected
    cases tail : newest xs with
    | none =>
      simp only [tail, Option.some.injEq] at selected
      subst i
      have empty := (newest_none xs).mp tail
      subst xs
      simp
    | some y =>
      have hy := ih y tail
      simp only [tail] at selected
      split at selected
      · cases selected
        refine ⟨by simp, ?_⟩
        intro j hj
        rcases List.mem_cons.mp hj with rfl | hj
        · omega
        · have := hy.2 j hj; omega
      · cases selected
        refine ⟨List.mem_cons_of_mem _ hy.1, ?_⟩
        intro j hj
        rcases List.mem_cons.mp hj with rfl | hj
        · omega
        · exact hy.2 j hj

omit [DecidableEq Aud] in
theorem newest_head (i : Introduction Aud) (xs : List (Introduction Aud))
    (last : ∀ j ∈ xs, j.slot ≤ i.slot) : newest (i :: xs) = some i := by
  cases tail : newest xs with
  | none => simp [newest, tail]
  | some j => simp [newest, tail, last j (newest_max xs j tail).1]

def usable (h : History Aud Mem) (held : Custody Aud) (current : Current Aud Mem)
    (writer : Mem) (key : Key Aud) : Bool :=
  (current key.audience writer).isSome && accepted h held key && !exposed h current key

def firstKey (h : History Aud Mem) (held : Custody Aud) (current : Current Aud Mem)
    (writer : Mem) (audience : Aud) : Option (Introduction Aud) :=
  newest (h.introductions.filter fun i => decide (i.key.audience = audience) && usable h held current writer i.key)

def metadataKey (h : History Aud Mem) (held : Custody Aud) (store : Aud) : Option (Introduction Aud) :=
  newest (h.introductions.filter fun i => decide (i.key.audience = store) && accepted h held i.key)

/-- §14.6 permits a departed circle member to first send eligible originals
authored before departure. Their authored membership is checked separately;
this selection must not authorize newly authored content. -/
def sendingKey (h : History Aud Mem) (held : Custody Aud) (current : Current Aud Mem)
    (writer : Mem) (store audience : Aud) : Option (Introduction Aud) :=
  if (current audience writer).isSome then firstKey h held current writer audience
  else if audience = store then none else metadataKey h held audience

theorem sending_current (h : History Aud Mem) (held : Custody Aud) (current : Current Aud Mem)
    (writer : Mem) (store audience : Aud) (authorized : (current audience writer).isSome = true) :
    sendingKey h held current writer store audience = firstKey h held current writer audience := by
  simp [sendingKey, authorized]

theorem first_newest_usable (h : History Aud Mem) (held : Custody Aud) (current : Current Aud Mem)
    (writer : Mem) (audience : Aud) (i : Introduction Aud)
    (selected : firstKey h held current writer audience = some i) :
    i ∈ h.introductions ∧ i.key.audience = audience ∧ usable h held current writer i.key = true ∧
      ∀ j ∈ h.introductions, j.key.audience = audience → usable h held current writer j.key = true →
        j.slot ≤ i.slot := by
  have result := newest_max _ i selected
  have member := List.mem_filter.mp result.1
  simp only [Bool.and_eq_true, decide_eq_true_eq] at member
  exact ⟨member.1, member.2.1, member.2.2, fun j hj ha hu =>
    result.2 j (List.mem_filter.mpr ⟨hj, by simp [ha, hu]⟩)⟩

theorem first_not_exposed (h : History Aud Mem) (held : Custody Aud) (current : Current Aud Mem)
    (writer : Mem) (audience : Aud) (i : Introduction Aud)
    (selected : firstKey h held current writer audience = some i) : exposed h current i.key = false := by
  have := (first_newest_usable h held current writer audience i selected).2.2.1
  simpa [usable] using (Bool.and_eq_true_iff.mp this).2

theorem metadata_newest_held (h : History Aud Mem) (held : Custody Aud) (store : Aud)
    (i : Introduction Aud) (selected : metadataKey h held store = some i) :
    i ∈ h.introductions ∧ i.key.audience = store ∧ accepted h held i.key = true ∧
      ∀ j ∈ h.introductions, j.key.audience = store → accepted h held j.key = true → j.slot ≤ i.slot := by
  have result := newest_max _ i selected
  have member := List.mem_filter.mp result.1
  simp only [Bool.and_eq_true, decide_eq_true_eq] at member
  exact ⟨member.1, member.2.1, member.2.2, fun j hj ha hh =>
    result.2 j (List.mem_filter.mpr ⟨hj, by simp [ha, hh]⟩)⟩

/-- Exposure changes no metadata-key requirement. An authorized removal has
no audience-key copies and can be sealed with any selected metadata key. -/
theorem removal_publishable (h : History Aud Mem) (held : Custody Aud) (store : Aud)
    (key : Key Aud) (audience : key.audience = store) (acceptedKey : accepted h held key = true) :
    ∃ i, metadataKey h held store = some i ∧ ∃ hash, held i.key = some hash := by
  have witness := (accepted_iff h held key).mp acceptedKey
  obtain ⟨j, hj, hk⟩ := witness.1
  have member : j ∈ h.introductions.filter (fun i => decide (i.key.audience = store) && accepted h held i.key) := by
    apply List.mem_filter.mpr
    exact ⟨hj, by simp [hk, audience, acceptedKey]⟩
  cases chosen : metadataKey h held store with
  | none =>
    have empty := (newest_none _).mp chosen
    rw [empty] at member
    cases member
  | some i =>
    have yes := metadata_newest_held h held store i chosen
    have material := ((accepted_iff h held i.key).mp yes.2.2.1).2 i yes.1 rfl
    exact ⟨i, rfl, i.hash, material⟩

/-- Recipient public credentials come from the current audience membership.
The writer never needs a recipient's private key or an old circle key. -/
def routes (current : Current Aud Mem) (members : List Mem) (key : Key Aud) : List (Route Aud Mem) :=
  members.filterMap fun m => (current key.audience m).map fun c => ⟨key, m, c⟩

omit [DecidableEq Aud] in
theorem routes_current (current : Current Aud Mem) (members : List Mem) (key : Key Aud)
    (r : Route Aud Mem) (member : r ∈ routes current members key) :
    r.key = key ∧ current key.audience r.member = some r.credential := by
  obtain ⟨m, _, hm⟩ := List.mem_filterMap.mp member
  cases hc : current key.audience m with
  | none => simp [hc] at hm
  | some c => simp [hc] at hm; subst r; exact ⟨rfl, hc⟩

omit [DecidableEq Aud] in
theorem routes_complete (current : Current Aud Mem) (members : List Mem) (key : Key Aud)
    (m : Mem) (c : Nat) (member : m ∈ members) (bound : current key.audience m = some c) :
    (⟨key, m, c⟩ : Route Aud Mem) ∈ routes current members key := by
  exact List.mem_filterMap.mpr ⟨m, member, by simp [bound]⟩

/-- The concrete slot plan: copies first, then the introduction. All outer
envelopes may use the old metadata key, even when it is exposed. -/
def rotationSlots (current : Current Aud Mem) (members : List Mem) (key : Key Aud) (hash : Nat) :
    List (Event Aud Mem) := (routes current members key).map Event.copy ++ [.introduce key hash]

def publish (h : History Aud Mem) (through : Nat) (slots : List (Event Aud Mem)) : History Aud Mem :=
  (slots.zipIdx through).foldl (fun h (e, n) => step h (n + 1) e) h

omit [DecidableEq Aud] in
theorem publish_append (h : History Aud Mem) (through : Nat) (a b : List (Event Aud Mem)) :
    publish h through (a ++ b) = publish (publish h through a) (through + a.length) b := by
  simp [publish, List.zipIdx_append, List.foldl_append]

omit [DecidableEq Aud] in
/-- The finite runner and the unbounded sequence have exactly the same prefix. -/
theorem publish_prefix (seq : Nat → Event Aud Mem) (n : Nat) :
    publish ⟨[], []⟩ 0 ((List.range n).map fun k => seq (k + 1)) = historyAt seq n := by
  induction n with
  | zero => rfl
  | succ n ih =>
    rw [List.range_succ, List.map_append, publish_append, ih]
    simp [publish, historyAt]

omit [DecidableEq Aud] in
theorem publish_copies (h : History Aud Mem) (through : Nat) (rs : List (Route Aud Mem)) :
    publish h through (rs.map Event.copy) = ⟨h.introductions, rs.reverse ++ h.copies⟩ := by
  induction rs generalizing h through with
  | nil => simp [publish]
  | cons r rs ih =>
    simp only [publish, List.map_cons, List.zipIdx_cons, List.foldl_cons]
    change publish (step h (through + 1) (.copy r)) (through + 1) (rs.map Event.copy) = _
    rw [ih]
    simp [step, List.reverse_cons, List.append_assoc]

omit [DecidableEq Aud] in
theorem rotation_effect (h : History Aud Mem) (through : Nat) (current : Current Aud Mem)
    (members : List Mem) (key : Key Aud) (hash : Nat) :
    publish h through (rotationSlots current members key hash) =
      ⟨⟨key, hash, through + (routes current members key).length + 1⟩ :: h.introductions,
        (routes current members key).reverse ++ h.copies⟩ := by
  simp only [rotationSlots, publish, List.zipIdx_append, List.foldl_append,
    List.length_map, List.zipIdx_cons, List.zipIdx_nil, List.foldl_cons, List.foldl_nil]
  change step (publish h through ((routes current members key).map Event.copy)) _ _ = _
  rw [publish_copies]
  rfl

/-- Every copy and the introduction can use the same held metadata key:
none of the preceding copy slots changes metadata-key acceptance. -/
theorem rotation_prefix_metadata (h : History Aud Mem) (through : Nat) (held : Custody Aud)
    (store : Aud) (current : Current Aud Mem) (members : List Mem) (key : Key Aud) (hash n : Nat)
    (beforeIntroduction : n ≤ (routes current members key).length) :
    metadataKey (publish h through ((rotationSlots current members key hash).take n)) held store =
      metadataKey h held store := by
  unfold rotationSlots
  rw [List.take_append_of_le_length (by simpa using beforeIntroduction), ← List.map_take]
  rw [publish_copies]
  rfl

theorem rotation_publishable (h : History Aud Mem) (held : Custody Aud) (store : Aud)
    (old key : Key Aud) (hash : Nat) (current : Current Aud Mem) (members : List Mem)
    (oldStore : old.audience = store) (oldHeld : accepted h held old = true)
    (generated : held key = some hash) :
    ∃ outer, metadataKey h held store = some outer ∧
      (∃ material, held outer.key = some material) ∧ held key = some hash ∧
      (∀ r ∈ routes current members key, current key.audience r.member = some r.credential) ∧
      ∀ through n, n ≤ (routes current members key).length →
        metadataKey (publish h through ((rotationSlots current members key hash).take n)) held store = some outer := by
  obtain ⟨outer, selected, available⟩ := removal_publishable h held store old oldStore oldHeld
  refine ⟨outer, selected, available, generated, fun r hr => (routes_current current members key r hr).2, ?_⟩
  intro through n beforeIntroduction
  rw [rotation_prefix_metadata h through held store current members key hash n beforeIntroduction]
  exact selected

theorem rotation_unexposed (h : History Aud Mem) (through : Nat) (current : Current Aud Mem)
    (members : List Mem) (key : Key Aud) (hash : Nat)
    (fresh : ∀ r ∈ h.copies, r.key ≠ key) :
    exposed (publish h through (rotationSlots current members key hash)) current key = false := by
  rw [rotation_effect]
  apply Bool.eq_false_iff.mpr
  intro yes
  obtain ⟨r, hr, hk, bad⟩ := (exposed_iff _ _ _).mp yes
  rcases List.mem_append.mp hr with new | old
  · have := (routes_current current members key r (List.mem_reverse.mp new)).2
    exact bad this
  · exact fresh r old hk

/-- Fresh material becomes usable at the introduction, after all required
copies. Exposure of every preceding key is irrelevant to this construction. -/
theorem rotation_usable (h : History Aud Mem) (through : Nat) (current : Current Aud Mem)
    (members : List Mem) (key : Key Aud) (hash : Nat) (held : Custody Aud) (writer : Mem)
    (freshIntro : ∀ i ∈ h.introductions, i.key ≠ key)
    (freshCopy : ∀ r ∈ h.copies, r.key ≠ key) (generated : held key = some hash)
    (authorized : (current key.audience writer).isSome = true) :
    usable (publish h through (rotationSlots current members key hash)) held current writer key = true := by
  have noExposure := rotation_unexposed h through current members key hash freshCopy
  simp only [usable, authorized, Bool.true_and, noExposure, Bool.not_false, Bool.and_true]
  rw [rotation_effect]
  exact introduction_accepts ⟨h.introductions, (routes current members key).reverse ++ h.copies⟩
    held key hash _ freshIntro generated

/-- The completed rotation is the next first-attempt choice, even if all
preceding keys were exposed or no preceding circle material was held. -/
theorem rotation_selected (h : History Aud Mem) (through : Nat) (current : Current Aud Mem)
    (members : List Mem) (key : Key Aud) (hash : Nat) (held : Custody Aud) (writer : Mem)
    (bounded : ∀ i ∈ h.introductions, i.slot ≤ through)
    (freshIntro : ∀ i ∈ h.introductions, i.key ≠ key)
    (freshCopy : ∀ r ∈ h.copies, r.key ≠ key) (generated : held key = some hash)
    (authorized : (current key.audience writer).isSome = true) :
    firstKey (publish h through (rotationSlots current members key hash)) held current writer key.audience =
      some ⟨key, hash, through + (routes current members key).length + 1⟩ := by
  have yes := rotation_usable h through current members key hash held writer freshIntro freshCopy generated authorized
  unfold firstKey
  rw [rotation_effect] at yes ⊢
  simp only [List.filter_cons, decide_true, Bool.true_and, yes, ↓reduceIte]
  apply newest_head
  intro j hj
  have := bounded j (List.mem_filter.mp hj).1
  dsimp
  omega

/-- Adding a different introduction never revokes historical decryption. -/
theorem rotation_preserves_old (h : History Aud Mem) (through : Nat) (current : Current Aud Mem)
    (members : List Mem) (key old : Key Aud) (hash : Nat) (held : Custody Aud) (different : key ≠ old) :
    accepted (publish h through (rotationSlots current members key hash)) held old = accepted h held old := by
  rw [rotation_effect]
  simp [accepted, different]

/-- An observer's secret set is independent of what a reader chooses to open.
This is the ideal encryption boundary: only equality of the exact audience key
reveals a protected value. Public ciphertext lengths are outside this value. -/
def observe (known : Key Aud → Bool) (key : Key Aud) (value : α) : Option α :=
  if known key then some value else none

/-- Opening the store envelope yields its full metadata and the still-sealed
protected value. The public routing and ciphertext lengths are fixed inputs. -/
def slotView (known : Key Aud → Bool) (outer inner : Key Aud) (metadata : Header) (value : Content) :=
  observe known outer (metadata, observe known inner value)

omit [DecidableEq Aud] in
/-- Headers and admin frames may be read under an exposed store key. Varying
any separately sealed content unknown to the observer does not change its view.
The metadata argument includes *all* store-sealed headers/operation contents. -/
theorem metadata_reveals_no_protected_content (known : Key Aud → Bool) (outer inner : Key Aud)
    (metadata : Header) (a b : Content) (secret : known inner = false) :
    slotView known outer inner metadata a = slotView known outer inner metadata b := by
  simp [slotView, observe, secret]

omit [DecidableEq Aud] in
theorem exposed_header_view (known : Key Aud → Bool) (outer inner : Key Aud)
    (metadata : Header) (value : Content) (opened : known outer = true) (secret : known inner = false) :
    slotView known outer inner metadata value = some (metadata, none) := by
  simp [slotView, observe, opened, secret]

/-- Knowledge of the outer store key does not supply a box's private credential. -/
def observeBox (privateKeys : Mem → Nat → Bool) (r : Route Aud Mem) (material : Nat) : Option Nat :=
  if privateKeys r.member r.credential then some material else none

omit [DecidableEq Aud] in
theorem store_key_does_not_open_box (privateKeys : Mem → Nat → Bool) (r : Route Aud Mem)
    (a b : Nat) (missing : privateKeys r.member r.credential = false) :
    observeBox privateKeys r a = observeBox privateKeys r b := by simp [observeBox, missing]

/-- No first attempt selected against this prefix is readable with an exposed
key. After rotation this applies to new protected data, not admin metadata or
already attempted inner bytes. It must be re-established at each catch-up. -/
theorem first_attempt_confidential (h : History Aud Mem) (held : Custody Aud) (current : Current Aud Mem)
    (writer : Mem) (audience : Aud) (i : Introduction Aud) (a b : Content)
    (selected : firstKey h held current writer audience = some i) :
    observe (exposed h current) i.key a = none ∧
    observe (exposed h current) i.key a = observe (exposed h current) i.key b := by
  simp [observe, first_not_exposed h held current writer audience i selected]

/-- §3's confidentiality side of revocation: after catch-up excludes all
credentials retained by the removed/stolen device, none of its routed keys
can be the key of a new first attempt. -/
theorem revocation (h : History Aud Mem) (held : Custody Aud) (current : Current Aud Mem)
    (writer removed : Mem) (cs : List Nat) (audience : Aud) (i : Introduction Aud)
    (selected : firstKey h held current writer audience = some i)
    (excluded : ∀ c ∈ cs, current audience removed ≠ some c) :
    ¬ RoutedTo h removed cs i.key := by
  intro received
  have same := (first_newest_usable h held current writer audience i selected).2.1
  have exposure := routed_exposure h current removed cs i.key received (by simpa [same] using excluded)
  rw [first_not_exposed h held current writer audience i selected] at exposure
  cases exposure

/-- Completed rotation provides a selectable key that none of the excluded
device's routed credentials can open. Freshness includes tentative copies. -/
theorem rotation_revokes (h : History Aud Mem) (through : Nat) (current : Current Aud Mem)
    (members : List Mem) (key : Key Aud) (hash : Nat) (held : Custody Aud) (writer removed : Mem)
    (cs : List Nat) (bounded : ∀ i ∈ h.introductions, i.slot ≤ through)
    (freshIntro : ∀ i ∈ h.introductions, i.key ≠ key)
    (freshCopy : ∀ r ∈ h.copies, r.key ≠ key) (generated : held key = some hash)
    (authorized : (current key.audience writer).isSome = true)
    (excluded : ∀ c ∈ cs, current key.audience removed ≠ some c) :
    ¬ RoutedTo (publish h through (rotationSlots current members key hash)) removed cs key := by
  exact revocation _ held current writer removed cs key.audience _
    (rotation_selected h through current members key hash held writer bounded freshIntro freshCopy generated authorized)
    excluded

/-- Compose revocation's authorization side with the full sequence model.
Regardless of the encryption key or retry, a cut-off batch only records refusal
and claim evidence; it contributes no rows, unique values, or owner claims. -/
theorem revoked_write_no_effect {S : Sig} (R : Rules S) (seq : Nat → CovenSequence.Slot S)
    {j k : Nat} (later : j ≤ k) {d : S.Dev} {g : Nat} {parts : S.Aud → Option (WPart S)}
    (slot : seq (k + 1) = .writes d g parts) (removed : Cut R seq j d g)
    (A : S.Aud) {w : WPart S} (part : parts A = some w) :
    audAt R seq A (k + 1) = refuseAud (audAt R seq A k) (k + 1) ∧
    rowsAt R seq A (k + 1) = rowsAt R seq A k :=
  (cutoff R seq later slot removed A part).2.2

end CovenSequence.Keys
