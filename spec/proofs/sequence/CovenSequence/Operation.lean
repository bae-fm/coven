import CovenSequence.Claim

/-!
# Admin operations: the claim loop and the journal

Every admin action is an operation, a journal entry. It validates against the
latest slot it read, records each pre-claim artifact in the journal before
creating it (snapshot bodies and other deletable preparation outside the
sequence), then claims the entry slot. Already claimed key copies and snapshot
announcements are permanent sequence history; their publication is outside
this entry-claim model. The journal records the slot it
is claiming before the request is sent.

- The claim lands (its bytes are at the slot): the operation completes.
- Another device took the slot: read on, validate again against the new
  slots, and claim the next one.
- No longer allowed: undo every recorded deletable artifact, then delete the journal. A
  failed undo is a step not taken; the record stays and the undo retries.

A crash loses nothing the steps use: every step is conditioned on the journal
and on storage, which survive, and a restart takes the same steps. Claim
requests already sent survive the crash and can land at any later time.
Other devices claim slots at any time.

The operation's entry bytes for slot `n` are `ours n`: they name the operation and
the slot, so they are unique per attempt and a resend is the same attempt.

An artifact create is a request that, once sent, can land at any time until it
ends: it lands, fails, or is cancelled. The undo waits for, or cancels, every
create it sent before deleting by recorded name, and confirms absence before
deleting the journal. A request that cannot be cancelled (an S3 key mint) is
covered outside this model, by the reclaim of keys tagged with their device.
-/

namespace CovenSequence.Operation

open CovenSequence.Claim (Store create create_extends Extends)

/-- Published preparation remains sequence history after its operation ends. -/
inductive Preparation where
  | keyCopy
  | announcement
  deriving DecidableEq

/-- This operation's entry claim, already published preparation, or another
device's claim. Preparations are never deletable artifacts. -/
inductive Bytes where
  | ours (n : Nat)
  | preparation (kind : Preparation) (value : Nat)
  | other (x : Nat)
  deriving DecidableEq

inductive Phase where
  /-- Validated; recording and creating pre-claim artifacts. -/
  | prep
  /-- The claim of slot `n` is recorded and sent. -/
  | claiming (n : Nat)
  /-- Another device took the claimed slot; read on and validate again. -/
  | retry
  | landed (n : Nat)
  | undoing
  deriving DecidableEq

structure Journal (A : Type) where
  recorded : List A
  phase : Phase

inductive Result where
  | landed (n : Nat)
  | undone
  deriving DecidableEq

structure World (A : Type) where
  slots : Store Bytes
  /-- Slots of this operation's sent claim requests that have not landed. -/
  inflight : List Nat
  /-- Deletable preparation outside the sequence; never key-copy or announcement slots. -/
  artifacts : List A
  /-- Artifact create requests sent and not yet ended: each may land at any
  time until it ends (it lands, fails, or is cancelled). -/
  artReq : List A
  journal : Option (Journal A)
  result : Option Result

variable {A : Type}

/-- Slots `1..n-1` are filled and slot `n` is empty: `n` is the next slot. -/
def AtHead (s : Store Bytes) (n : Nat) : Prop :=
  (∀ i, 1 ≤ i → i < n → s i ≠ none) ∧ s n = none

/-- Faulty variants checked as counterexamples. -/
inductive Fault where
  | none
  /-- A restart that forgets its recorded claim. -/
  | forget
  /-- An undo that deletes an artifact while its create request may still land. -/
  | eagerUndo
  deriving DecidableEq

/-- The operation's steps and the environment's. `allowed s n` is the
operation's validation against slots `1..n-1`. `fault` enables one faulty
variant. -/
inductive Step [DecidableEq A] (allowed : Store Bytes → Nat → Bool) (fault : Fault) : World A → World A → Prop
  | record (w : World A) (rec : List A) (a : A) (hj : w.journal = some ⟨rec, .prep⟩) :
      Step allowed fault w { w with journal := some ⟨a :: rec, .prep⟩ }
  /-- Send the create of a recorded artifact. -/
  | sendArt (w : World A) (rec : List A) (a : A) (hj : w.journal = some ⟨rec, .prep⟩) (ha : a ∈ rec) :
      Step allowed fault w { w with artReq := a :: w.artReq }
  /-- A sent create lands, at any time, even after a crash or the journal's end. -/
  | landArt (w : World A) (a : A) (ha : a ∈ w.artReq) :
      Step allowed fault w { w with artifacts := a :: w.artifacts, artReq := w.artReq.erase a }
  /-- A sent create ends without landing: awaited to failure, or cancelled. -/
  | endArt (w : World A) (a : A) (ha : a ∈ w.artReq) :
      Step allowed fault w { w with artReq := w.artReq.erase a }
  | claim (w : World A) (rec : List A) (ph : Phase) (n : Nat) (hj : w.journal = some ⟨rec, ph⟩)
      (hph : ph = .prep ∨ ph = .retry) (hn : 1 ≤ n) (hhead : AtHead w.slots n)
      (hv : allowed w.slots n = true) :
      Step allowed fault w { w with journal := some ⟨rec, .claiming n⟩, inflight := n :: w.inflight }
  | resend (w : World A) (rec : List A) (n : Nat) (hj : w.journal = some ⟨rec, .claiming n⟩) :
      Step allowed fault w { w with inflight := n :: w.inflight }
  | land (w : World A) (n : Nat) (hn : n ∈ w.inflight) :
      Step allowed fault w { w with slots := create w.slots n (.ours n), inflight := w.inflight.erase n }
  | other (w : World A) (n x : Nat) :
      Step allowed fault w { w with slots := create w.slots n (.other x) }
  | won (w : World A) (rec : List A) (n : Nat) (hj : w.journal = some ⟨rec, .claiming n⟩)
      (hs : w.slots n = some (.ours n)) :
      Step allowed fault w { w with journal := some ⟨rec, .landed n⟩ }
  | lost (w : World A) (rec : List A) (n x : Nat) (hj : w.journal = some ⟨rec, .claiming n⟩)
      (hs : w.slots n = some (.other x)) :
      Step allowed fault w { w with journal := some ⟨rec, .retry⟩ }
  | giveUp (w : World A) (rec : List A) (ph : Phase) (n : Nat) (hj : w.journal = some ⟨rec, ph⟩)
      (hph : ph = .prep ∨ ph = .retry) (hhead : AtHead w.slots n) (hv : allowed w.slots n = false) :
      Step allowed fault w { w with journal := some ⟨rec, .undoing⟩ }
  /-- Delete an artifact by its recorded name, once no create of it can still
  land. -/
  | undoOne (w : World A) (rec : List A) (a : A) (hj : w.journal = some ⟨rec, .undoing⟩)
      (hreq : a ∉ w.artReq ∨ fault = .eagerUndo) :
      Step allowed fault w
        { w with artifacts := w.artifacts.filter (· ≠ a),
                 journal := some ⟨rec.filter (· ≠ a), .undoing⟩ }
  /-- Confirm the artifacts are absent, then delete the journal. -/
  | finishUndo (w : World A) (hj : w.journal = some ⟨[], .undoing⟩) (hgone : w.artifacts = []) :
      Step allowed fault w { w with journal := none, result := some .undone }
  | complete (w : World A) (rec : List A) (n : Nat) (hj : w.journal = some ⟨rec, .landed n⟩) :
      Step allowed fault w { w with journal := none, result := some (.landed n) }
  /-- The faulty restart: the claim in flight is forgotten. -/
  | forget (w : World A) (rec : List A) (n : Nat) (hf : fault = .forget)
      (hj : w.journal = some ⟨rec, .claiming n⟩) :
      Step allowed fault w { w with journal := some ⟨rec, .prep⟩ }

inductive Steps [DecidableEq A] (allowed : Store Bytes → Nat → Bool) (fault : Fault) : World A → World A → Prop
  | refl (w : World A) : Steps allowed fault w w
  | step {w w' w'' : World A} : Step allowed fault w w' → Steps allowed fault w' w'' →
      Steps allowed fault w w''

/-- An operation that has validated, with no artifacts and no requests, over
any slots that hold none of its bytes. -/
def start (s : Store Bytes) : World A :=
  { slots := s, inflight := [], artifacts := [], artReq := [], journal := some ⟨[], .prep⟩,
    result := none }

/-- The journal's phase, or none once it is deleted. -/
def World.phase (w : World A) : Option Phase := w.journal.map (·.phase)

/-- `s` holds this operation's bytes at slot `n`. -/
def Own (s : Store Bytes) (n : Nat) : Prop := s n = some (.ours n)

/-- Validation reads only slots `1..n-1`. -/
def PrefixOnly (allowed : Store Bytes → Nat → Bool) : Prop :=
  ∀ s t n, (∀ i, 1 ≤ i → i < n → s i = t i) → allowed s n = allowed t n

/-- What every reachable world satisfies. -/
structure Inv (allowed : Store Bytes → Nat → Bool) (w : World A) : Prop where
  /-- Our bytes appear only at the slot they name. -/
  ours_here : ∀ n m, w.slots n = some (.ours m) → m = n
  /-- Every stored artifact is recorded while the journal exists; after the
  journal is deleted, artifacts remain only for a landed operation. -/
  recorded : ∀ a, a ∈ w.artifacts → (∃ j, w.journal = some j ∧ a ∈ j.recorded) ∨
    (∃ n, w.result = some (.landed n))
  /-- A request still in flight is the one being claimed, or targets a filled
  slot, where create-once refuses it. -/
  inflight_ok : ∀ m, m ∈ w.inflight → w.phase = some (.claiming m) ∨ w.slots m ≠ none
  /-- Our bytes are at a slot only while claiming it or after landing it. -/
  own_phase : ∀ n, Own w.slots n →
    w.phase = some (.claiming n) ∨ w.phase = some (.landed n) ∨ w.result = some (.landed n)
  /-- A landed operation's bytes are at its slot. -/
  landed_own : ∀ n, (w.phase = some (.landed n) ∨ w.result = some (.landed n)) → Own w.slots n
  /-- The slot being claimed or landed was the next slot when validated, and
  validation held there. -/
  validated : ∀ n, (w.phase = some (.claiming n) ∨ w.phase = some (.landed n) ∨
      w.result = some (.landed n)) →
    1 ≤ n ∧ (∀ i, 1 ≤ i → i < n → w.slots i ≠ none) ∧ allowed w.slots n = true
  /-- A result belongs to a deleted journal. -/
  result_done : w.result ≠ none → w.journal = none

theorem create_keeps_filled (s : Store Bytes) (n : Nat) (b : Bytes) (i : Nat) (h : s i ≠ none) :
    create s n b i = s i := by
  cases hs : s i with
  | none => exact absurd hs h
  | some x => exact create_extends s n b i x hs

theorem create_filled_ne (s : Store Bytes) (n : Nat) (b : Bytes) (i : Nat) (h : s i ≠ none) :
    create s n b i ≠ none := by
  rw [create_keeps_filled s n b i h]; exact h

/-- Validation of a slot whose predecessors are filled survives any create. -/
theorem allowed_create {allowed : Store Bytes → Nat → Bool} (hp : PrefixOnly allowed)
    (s : Store Bytes) (n m : Nat) (b : Bytes) (hfill : ∀ i, 1 ≤ i → i < n → s i ≠ none) :
    allowed (create s m b) n = allowed s n :=
  hp _ _ n (fun i h1 h2 => create_keeps_filled s m b i (hfill i h1 h2))

theorem inv_start (allowed : Store Bytes → Nat → Bool) (s : Store Bytes)
    (h : ∀ n m, s n ≠ some (.ours m)) : Inv allowed (start (A := A) s) := by
  refine ⟨fun n m hs => absurd hs (h n m), fun a ha => by simp [start] at ha,
    fun m hm => by simp [start] at hm, fun n hn => absurd hn (h n n), ?_, ?_, by simp [start]⟩
  · intro n hn; simp [start, World.phase] at hn
  · intro n hn; simp [start, World.phase] at hn

/-- In a phase other than claiming or landed, with the journal present, the
operation has no bytes in the sequence. -/
theorem no_own {allowed : Store Bytes → Nat → Bool} {w : World A} (hi : Inv allowed w)
    {rec : List A} {ph : Phase} (hj : w.journal = some ⟨rec, ph⟩)
    (hph : ∀ n, ph ≠ .claiming n ∧ ph ≠ .landed n) : ∀ n, ¬ Own w.slots n := by
  intro n hn
  have hres : w.result = none := by
    cases hr : w.result with
    | none => rfl
    | some r => have := hi.result_done (by simp [hr]); simp [hj] at this
  rcases hi.own_phase n hn with h | h | h
  · simp [World.phase, hj] at h; exact (hph n).1 h
  · simp [World.phase, hj] at h; exact (hph n).2 h
  · simp [hres] at h

theorem result_none {allowed : Store Bytes → Nat → Bool} {w : World A} (hi : Inv allowed w)
    {j : Journal A} (hj : w.journal = some j) : w.result = none := by
  cases hr : w.result with
  | none => rfl
  | some r => have := hi.result_done (by simp [hr]); simp [hj] at this

/-- Every artifact create still in flight is recorded while the journal exists,
and after it only for a landed operation. -/
def ReqInv (w : World A) : Prop :=
  ∀ a, a ∈ w.artReq → (∃ j, w.journal = some j ∧ a ∈ j.recorded) ∨ (∃ n, w.result = some (.landed n))

theorem req_keep {w w' : World A} (hq : ReqInv w) (hreq : ∀ a, a ∈ w'.artReq → a ∈ w.artReq)
    (hrec : ∀ j, w.journal = some j → ∃ j', w'.journal = some j' ∧ ∀ a, a ∈ j.recorded → a ∈ j'.recorded)
    (hres : ∀ n, w.result = some (.landed n) → w'.result = some (.landed n)) : ReqInv w' := by
  intro a ha
  rcases hq a (hreq a ha) with ⟨j, hj, haj⟩ | ⟨n, hn⟩
  · obtain ⟨j', hj', hsub⟩ := hrec j hj
    exact Or.inl ⟨j', hj', hsub a haj⟩
  · exact Or.inr ⟨n, hres n hn⟩

/-- **Every step keeps the request invariant.** An undo deletes an artifact only
once no create of it is in flight, so nothing can land after the journal ends
unless the operation landed. -/
theorem step_req [DecidableEq A] {allowed : Store Bytes → Nat → Bool} {w w' : World A}
    (st : Step allowed .none w w') (hi : Inv allowed w) (hq : ReqInv w) : ReqInv w' := by
  cases st with
  | record rec a hj =>
    refine req_keep hq (fun _ h => h) (fun j hj' => ?_) (fun _ h => h)
    rw [hj] at hj'; cases hj'
    exact ⟨_, rfl, fun b hb => List.mem_cons_of_mem _ hb⟩
  | sendArt rec a hj ha =>
    intro b hb
    simp only [List.mem_cons] at hb
    rcases hb with rfl | hb
    · exact Or.inl ⟨_, hj, ha⟩
    · exact hq b hb
  | landArt a ha =>
    exact req_keep hq (fun _ h => List.mem_of_mem_erase h) (fun j hj => ⟨j, hj, fun _ h => h⟩) (fun _ h => h)
  | endArt a ha =>
    exact req_keep hq (fun _ h => List.mem_of_mem_erase h) (fun j hj => ⟨j, hj, fun _ h => h⟩) (fun _ h => h)
  | claim rec ph n hj =>
    refine req_keep hq (fun _ h => h) (fun j hj' => ?_) (fun _ h => h)
    rw [hj] at hj'; cases hj'; exact ⟨_, rfl, fun _ h => h⟩
  | resend => exact req_keep hq (fun _ h => h) (fun j hj => ⟨j, hj, fun _ h => h⟩) (fun _ h => h)
  | land => exact req_keep hq (fun _ h => h) (fun j hj => ⟨j, hj, fun _ h => h⟩) (fun _ h => h)
  | other => exact req_keep hq (fun _ h => h) (fun j hj => ⟨j, hj, fun _ h => h⟩) (fun _ h => h)
  | won rec n hj =>
    refine req_keep hq (fun _ h => h) (fun j hj' => ?_) (fun _ h => h)
    rw [hj] at hj'; cases hj'; exact ⟨_, rfl, fun _ h => h⟩
  | lost rec n x hj =>
    refine req_keep hq (fun _ h => h) (fun j hj' => ?_) (fun _ h => h)
    rw [hj] at hj'; cases hj'; exact ⟨_, rfl, fun _ h => h⟩
  | giveUp rec ph n hj =>
    refine req_keep hq (fun _ h => h) (fun j hj' => ?_) (fun _ h => h)
    rw [hj] at hj'; cases hj'; exact ⟨_, rfl, fun _ h => h⟩
  | undoOne rec a hj hreq =>
    have ha : a ∉ w.artReq := by rcases hreq with h | h; exact h; cases h
    intro b hb
    rcases hq b hb with ⟨j, hj', hbj⟩ | h
    · rw [hj] at hj'; cases hj'
      have hne : b ≠ a := by intro he; subst he; exact ha hb
      exact Or.inl ⟨_, rfl, by simp [hbj, hne]⟩
    · exact Or.inr h
  | finishUndo hj hgone =>
    intro b hb
    rcases hq b hb with ⟨j, hj', hbj⟩ | ⟨n, hn⟩
    · rw [hj] at hj'; cases hj'; simp at hbj
    · simp [result_none hi hj] at hn
  | complete rec n hj => intro b _; exact Or.inr ⟨n, rfl⟩
  | forget rec n hf => cases hf

/-- **Every step keeps the invariant.** -/
theorem step_inv [DecidableEq A] {allowed : Store Bytes → Nat → Bool} (hp : PrefixOnly allowed) {w w' : World A}
    (st : Step allowed .none w w') (hi : Inv allowed w) (hq : ReqInv w) : Inv allowed w' := by
  cases st with
  | record rec a hj =>
    have hno := no_own hi hj (fun n => by simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun x hx => ?_, fun m hm => ?_, fun n hn => absurd hn (hno n),
      fun n hn => ?_, fun n hn => ?_, fun _ => by simp [hres] at *⟩
    · rcases hi.recorded x hx with ⟨j, hj', hxj⟩ | h
      · rw [hj] at hj'; cases hj'
        exact Or.inl ⟨_, rfl, List.mem_cons_of_mem _ hxj⟩
      · exact Or.inr h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h
      · exact Or.inr h
    · simp [World.phase, hres] at hn
    · simp [World.phase, hres] at hn
  | sendArt rec a hj ha =>
    exact ⟨hi.ours_here, hi.recorded, hi.inflight_ok, hi.own_phase, hi.landed_own, hi.validated,
      hi.result_done⟩
  | endArt a ha =>
    exact ⟨hi.ours_here, hi.recorded, hi.inflight_ok, hi.own_phase, hi.landed_own, hi.validated,
      hi.result_done⟩
  | landArt a ha =>
    refine ⟨hi.ours_here, fun x hx => ?_, hi.inflight_ok, hi.own_phase, hi.landed_own, hi.validated,
      hi.result_done⟩
    simp only [List.mem_cons] at hx
    rcases hx with rfl | hx
    · exact hq x ha
    · exact hi.recorded x hx
  | claim rec ph n hj hph hn hhead hv =>
    have hno := no_own hi hj (fun k => by rcases hph with rfl | rfl <;> simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun x hx => ?_, fun m hm => ?_, fun k hk => absurd hk (hno k),
      fun k hk => ?_, fun k hk => ?_, fun _ => by simp [hres] at *⟩
    · rcases hi.recorded x hx with ⟨j, hj', hxj⟩ | h
      · rw [hj] at hj'; cases hj'; exact Or.inl ⟨_, rfl, hxj⟩
      · exact Or.inr h
    · simp only [List.mem_cons] at hm
      rcases hm with rfl | hm
      · exact Or.inl (by simp [World.phase])
      · rcases hi.inflight_ok m hm with h | h
        · simp [World.phase, hj] at h; rcases hph with rfl | rfl <;> simp at h
        · exact Or.inr h
    · simp [World.phase, hres] at hk
    · simp [World.phase, hres] at hk
      subst hk
      exact ⟨hn, hhead.1, hv⟩
  | resend rec n hj =>
    refine ⟨hi.ours_here, hi.recorded, fun m hm => ?_, hi.own_phase, hi.landed_own, hi.validated,
      hi.result_done⟩
    simp only [List.mem_cons] at hm
    rcases hm with rfl | hm
    · exact Or.inl (by simp [World.phase, hj])
    · exact hi.inflight_ok m hm
  | land n hn =>
    refine ⟨fun k m hk => ?_, hi.recorded, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun k hk => ?_, hi.result_done⟩
    · simp only at hk
      by_cases hkn : k = n
      · subst hkn
        cases hs : w.slots k with
        | none => rw [Claim.create_self_none _ hs] at hk; cases hk; rfl
        | some x => rw [Claim.create_self_some _ hs] at hk; exact hi.ours_here k m (hs ▸ hk)
      · rw [Claim.create_other _ hkn] at hk; exact hi.ours_here k m hk
    · rcases hi.inflight_ok m (List.mem_of_mem_erase hm) with h | h
      · exact Or.inl h
      · exact Or.inr (create_filled_ne _ _ _ _ h)
    · unfold Own at hk
      simp only at hk
      by_cases hkn : k = n
      · subst hkn
        cases hs : w.slots k with
        | none =>
          rcases hi.inflight_ok k hn with h | h
          · exact Or.inl h
          · exact absurd hs h
        | some x =>
          rw [Claim.create_self_some _ hs] at hk
          exact hi.own_phase k (by unfold Own; rw [hs]; exact hk)
      · rw [Claim.create_other _ hkn] at hk; exact hi.own_phase k hk
    · have := hi.landed_own k hk
      unfold Own at this ⊢
      simp only
      rw [create_keeps_filled _ _ _ _ (by simp [this]), this]
    · obtain ⟨h1, hfill, hv⟩ := hi.validated k hk
      refine ⟨h1, fun i hi1 hi2 => create_filled_ne _ _ _ _ (hfill i hi1 hi2), ?_⟩
      simp only
      rw [allowed_create hp _ _ _ _ hfill]; exact hv
  | other n x =>
    refine ⟨fun k m hk => ?_, hi.recorded, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun k hk => ?_, hi.result_done⟩
    · simp only at hk
      by_cases hkn : k = n
      · subst hkn
        cases hs : w.slots k with
        | none => rw [Claim.create_self_none _ hs] at hk; cases hk
        | some y => rw [Claim.create_self_some _ hs] at hk; exact hi.ours_here k m (hs ▸ hk)
      · rw [Claim.create_other _ hkn] at hk; exact hi.ours_here k m hk
    · rcases hi.inflight_ok m hm with h | h
      · exact Or.inl h
      · exact Or.inr (create_filled_ne _ _ _ _ h)
    · unfold Own at hk
      simp only at hk
      by_cases hkn : k = n
      · subst hkn
        cases hs : w.slots k with
        | none => rw [Claim.create_self_none _ hs] at hk; cases hk
        | some y =>
          rw [Claim.create_self_some _ hs] at hk
          exact hi.own_phase k (by unfold Own; rw [hs]; exact hk)
      · rw [Claim.create_other _ hkn] at hk; exact hi.own_phase k hk
    · have := hi.landed_own k hk
      unfold Own at this ⊢
      simp only
      rw [create_keeps_filled _ _ _ _ (by simp [this]), this]
    · obtain ⟨h1, hfill, hv⟩ := hi.validated k hk
      refine ⟨h1, fun i hi1 hi2 => create_filled_ne _ _ _ _ (hfill i hi1 hi2), ?_⟩
      simp only
      rw [allowed_create hp _ _ _ _ hfill]; exact hv
  | won rec n hj hs =>
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun x hx => ?_, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun k hk => ?_, fun _ => by simp [hres] at *⟩
    · rcases hi.recorded x hx with ⟨j, hj', hxj⟩ | h
      · rw [hj] at hj'; cases hj'; exact Or.inl ⟨_, rfl, hxj⟩
      · exact Or.inr h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h; subst h; exact Or.inr (by simp [hs])
      · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · simp [World.phase, hj] at h; subst h; exact Or.inr (Or.inl (by simp [World.phase]))
      · simp [World.phase, hj] at h
      · simp [hres] at h
    · simp [World.phase, hres] at hk; subst hk; exact hs
    · simp [World.phase, hres] at hk; subst hk
      exact hi.validated _ (Or.inl (by simp [World.phase, hj]))
  | lost rec n x hj hs =>
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun y hy => ?_, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun k hk => ?_, fun _ => by simp [hres] at *⟩
    · rcases hi.recorded y hy with ⟨j, hj', hyj⟩ | h
      · rw [hj] at hj'; cases hj'; exact Or.inl ⟨_, rfl, hyj⟩
      · exact Or.inr h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h; subst h; exact Or.inr (by simp [hs])
      · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · simp [World.phase, hj] at h; subst h; unfold Own at hk; rw [hs] at hk; cases hk
      · simp [World.phase, hj] at h
      · simp [hres] at h
    · simp [World.phase, hres] at hk
    · simp [World.phase, hres] at hk
  | giveUp rec ph n hj hph hhead hv =>
    have hno := no_own hi hj (fun k => by rcases hph with rfl | rfl <;> simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun y hy => ?_, fun m hm => ?_, fun k hk => absurd hk (hno k),
      fun k hk => ?_, fun k hk => ?_, fun _ => by simp [hres] at *⟩
    · rcases hi.recorded y hy with ⟨j, hj', hyj⟩ | h
      · rw [hj] at hj'; cases hj'; exact Or.inl ⟨_, rfl, hyj⟩
      · exact Or.inr h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h; rcases hph with rfl | rfl <;> simp at h
      · exact Or.inr h
    · simp [World.phase, hres] at hk
    · simp [World.phase, hres] at hk
  | undoOne rec a hj hreq =>
    have hno := no_own hi hj (fun k => by simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun y hy => ?_, fun m hm => ?_, fun k hk => absurd hk (hno k),
      fun k hk => ?_, fun k hk => ?_, fun _ => by simp [hres] at *⟩
    · simp only [List.mem_filter, decide_eq_true_eq] at hy
      rcases hi.recorded y hy.1 with ⟨j, hj', hyj⟩ | h
      · rw [hj] at hj'; cases hj'
        exact Or.inl ⟨_, rfl, by simp [hyj, hy.2]⟩
      · exact Or.inr h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h
      · exact Or.inr h
    · simp [World.phase, hres] at hk
    · simp [World.phase, hres] at hk
  | finishUndo hj hgone =>
    have hno := no_own hi hj (fun k => by simp)
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun y hy => ?_, fun m hm => ?_, fun k hk => absurd hk (hno k),
      fun k hk => ?_, fun k hk => ?_, fun _ => rfl⟩
    · rcases hi.recorded y hy with ⟨j, hj', hyj⟩ | ⟨n, h⟩
      · rw [hj] at hj'; cases hj'; simp at hyj
      · simp [hres] at h
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h
      · exact Or.inr h
    · simp [World.phase] at hk
    · simp [World.phase] at hk
  | complete rec n hj =>
    have hres := result_none hi hj
    refine ⟨hi.ours_here, fun y _ => Or.inr ⟨n, rfl⟩, fun m hm => ?_, fun k hk => ?_, fun k hk => ?_,
      fun k hk => ?_, fun _ => rfl⟩
    · rcases hi.inflight_ok m hm with h | h
      · simp [World.phase, hj] at h
      · exact Or.inr h
    · rcases hi.own_phase k hk with h | h | h
      · simp [World.phase, hj] at h
      · simp [World.phase, hj] at h; subst h; exact Or.inr (Or.inr rfl)
      · simp [hres] at h
    · simp [World.phase] at hk; subst hk
      exact hi.landed_own _ (Or.inl (by simp [World.phase, hj]))
    · simp [World.phase] at hk; subst hk
      exact hi.validated _ (Or.inr (Or.inl (by simp [World.phase, hj])))
  | forget rec n hf => cases hf

theorem steps_inv [DecidableEq A] {allowed : Store Bytes → Nat → Bool} (hp : PrefixOnly allowed) {w w' : World A}
    (run : Steps allowed .none w w') (hi : Inv allowed w) (hq : ReqInv w) : Inv allowed w' ∧ ReqInv w' := by
  induction run with
  | refl => exact ⟨hi, hq⟩
  | step s _ ih => exact ih (step_inv hp s hi hq) (step_req s hi hq)

/-- **The claim loop and journal.** From validation over any slots holding
none of its entry bytes, through any interleaving of its own steps, crashes and
restarts, late-landing requests and other devices' claims:

- its entry bytes occupy at most one slot (the entry never lands twice);
- a landed operation's entry bytes are at its slot, whose predecessors were all
  filled when it validated, and validation held against them;
- an undone operation has no entry bytes in the sequence, now or later, and
  none of its recorded deletable artifacts remains in storage, now or later: its undo ran only
  after every create it sent had ended. -/
theorem operation_outcome [DecidableEq A] {allowed : Store Bytes → Nat → Bool} (hp : PrefixOnly allowed)
    (s0 : Store Bytes) (h0 : ∀ n m, s0 n ≠ some (.ours m)) {w : World A}
    (run : Steps allowed .none (start s0) w) :
    (∀ n m, Own w.slots n → Own w.slots m → n = m) ∧
    (∀ n, w.result = some (.landed n) →
      Own w.slots n ∧ 1 ≤ n ∧ (∀ i, 1 ≤ i → i < n → w.slots i ≠ none) ∧ allowed w.slots n = true) ∧
    (w.result = some .undone → (∀ n, ¬ Own w.slots n) ∧ w.artifacts = []) := by
  have hi := (steps_inv hp run (inv_start allowed s0 h0) (fun a ha => by simp [start] at ha)).1
  refine ⟨fun n m hn hm => ?_, fun n hn => ?_, fun hu => ⟨fun n hn => ?_, ?_⟩⟩
  · rcases hi.own_phase n hn with h | h | h <;> rcases hi.own_phase m hm with h' | h' | h'
    all_goals first
      | (rw [h] at h'; simp at h'; try omega)
      | (have := hi.result_done (by simp [h]); simp [World.phase, this] at h')
      | (have := hi.result_done (by simp [h']); simp [World.phase, this] at h)
  · exact ⟨hi.landed_own n (Or.inr hn), hi.validated n (Or.inr (Or.inr hn))⟩
  · have hj := hi.result_done (by simp [hu])
    rcases hi.own_phase n hn with h | h | h
    · simp [World.phase, hj] at h
    · simp [World.phase, hj] at h
    · rw [hu] at h; cases h
  · have hj := hi.result_done (by simp [hu])
    cases ha : w.artifacts with
    | nil => rfl
    | cons a rest =>
      rcases hi.recorded a (by simp [ha]) with ⟨j, hj', _⟩ | ⟨n, hn⟩
      · simp [hj] at hj'
      · rw [hu] at hn; cases hn

/-- Operation steps never erase or replace an occupied history slot. -/
theorem step_preserves_history [DecidableEq A] {allowed : Store Bytes → Nat → Bool}
    {fault : Fault} {w w' : World A} (step : Step allowed fault w w') :
    Extends w.slots w'.slots := by
  cases step with
  | land => exact create_extends _ _ _
  | other => exact create_extends _ _ _
  | _ => intro _ _ h; exact h

theorem steps_preserve_history [DecidableEq A] {allowed : Store Bytes → Nat → Bool}
    {fault : Fault} {w w' : World A} (run : Steps allowed fault w w') :
    Extends w.slots w'.slots := by
  induction run with
  | refl => intro _ _ h; exact h
  | step s _ ih => intro i b h; exact ih i b (step_preserves_history s i b h)

/-- In particular, confirmed undo keeps every already claimed key copy and
announcement. Their multi-slot publication is outside this entry-claim model. -/
theorem undo_keeps_preparation [DecidableEq A] {allowed : Store Bytes → Nat → Bool}
    {w w' : World A} (run : Steps allowed .none w w')
    {i : Nat} {kind : Preparation} {value : Nat}
    (h : w.slots i = some (.preparation kind value)) :
    w'.slots i = some (.preparation kind value) :=
  steps_preserve_history run i _ h

/-- Undo can remove its temporary snapshot body while its announcement and
key copy remain in history; absence of every byte would be a false claim. -/
theorem undone_retains_history :
    let initial : Store Bytes := fun i =>
      if i = 1 then some (.preparation .keyCopy 7)
      else if i = 2 then some (.preparation .announcement 8) else none
    ∃ w : World Nat, Steps (fun _ _ => false) .none (start initial) w ∧
      w.result = some .undone ∧ w.artifacts = [] ∧
      w.slots 1 = some (.preparation .keyCopy 7) ∧
      w.slots 2 = some (.preparation .announcement 8) := by
  intro initial
  refine ⟨_, Steps.step (Step.record _ [] 8 rfl)
    (Steps.step (Step.sendArt _ [8] 8 rfl (by simp))
    (Steps.step (Step.landArt _ 8 (by simp))
    (Steps.step (Step.giveUp _ [8] .prep 3 rfl (Or.inl rfl) ⟨?_, rfl⟩ rfl)
    (Steps.step (Step.undoOne _ [8] 8 rfl (Or.inl (by simp [start])))
    (Steps.step (Step.finishUndo _ (by simp) (by simp [start])) (Steps.refl _)))))),
    rfl, by simp [start], rfl, rfl⟩
  intro i hi hlt
  have h : i = 1 ∨ i = 2 := by omega
  rcases h with rfl | rfl <;> simp [start, initial]

/-- **Counterexample: a restart that forgets its claim lands twice.** The
operation claims slot 1, the request lands, and a restart that does not
resolve its recorded claim validates again, claims slot 2 and lands there too. -/
theorem forgetful_lands_twice :
    ∃ w : World Nat, Steps (fun _ _ => true) .forget (start (fun _ => none)) w ∧
      Own w.slots 1 ∧ Own w.slots 2 := by
  refine ⟨_, Steps.step (Step.claim _ [] .prep 1 rfl (Or.inl rfl) (Nat.le_refl _)
      ⟨fun i h1 h2 => by omega, rfl⟩ rfl)
    (Steps.step (Step.land _ 1 (by simp))
    (Steps.step (Step.forget _ [] 1 rfl rfl)
    (Steps.step (Step.claim _ [] .prep 2 rfl (Or.inl rfl) (by decide) ⟨fun i h1 h2 => ?_, ?_⟩ rfl)
    (Steps.step (Step.land _ 2 (by simp)) (Steps.refl _))))), ?_, ?_⟩
  · have : i = 1 := by omega
    subst this; simp [Claim.create, start]
  · simp [Claim.create, start]
  · simp [Own, Claim.create, start]
  · simp [Own, Claim.create, start]

/-- **Counterexample: an undo that does not wait for its requests leaves an
artifact.** The operation records and sends an artifact's create, finds it is no
longer allowed, deletes the artifact while the create is in flight, confirms
absence and ends. The create then lands: the operation is undone and its
artifact is in storage. -/
theorem eager_undo_leaks :
    ∃ w : World Nat, Steps (fun _ _ => false) .eagerUndo (start (fun _ => none)) w ∧
      w.result = some .undone ∧ w.artifacts = [7] := by
  refine ⟨_, Steps.step (Step.record _ [] 7 rfl)
    (Steps.step (Step.sendArt _ [7] 7 rfl (by simp))
    (Steps.step (Step.giveUp _ [7] .prep 1 rfl (Or.inl rfl) ⟨fun i h1 h2 => by omega, rfl⟩ rfl)
    (Steps.step (Step.undoOne _ [7] 7 rfl (Or.inr rfl))
    (Steps.step (Step.finishUndo _ (by simp) rfl)
    (Steps.step (Step.landArt _ 7 (by simp)) (Steps.refl _)))))), ?_, ?_⟩
  · rfl
  · simp [start]

end CovenSequence.Operation
