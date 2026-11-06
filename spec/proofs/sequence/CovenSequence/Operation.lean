import CovenSequence.Claim

/-!
# Admin operations: the claim loop and the journal

Every admin action is an operation, a journal entry, which may be journaled
before it validates: an app's write journals an admin change, and a pass
validates it. It validates against the latest slot it read before each
claim, records each pre-claim artifact in the journal before
creating it (snapshot bodies and other deletable preparation outside the
sequence), then claims the entry slot. Key copies and snapshot announcements
claim their own slots while it prepares, and stay permanent sequence history.
The journal records the slot it is claiming before the request is sent.

- The claim lands (its bytes are at the slot): the operation completes.
- Another device took the slot: read on, validate again against the new
  slots, and claim the next one.
- Already satisfied: the action needs no entry. Undo every recorded deletable
  artifact, then end satisfied, with no entry bytes.
- No longer check: undo every recorded deletable artifact, then delete the journal. A
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

/-- Validation of the operation against slots `1..n-1`: it may claim slot `n`,
it is already satisfied, or it is no longer check. -/
inductive Verdict where
  | allow
  | satisfied
  | refuse
  deriving DecidableEq

inductive Phase where
  /-- Journaled; recording and creating pre-claim artifacts. -/
  | prep
  /-- The claim of slot `n` is recorded and sent. -/
  | claiming (n : Nat)
  /-- Another device took the claimed slot; read on and validate again. -/
  | retry
  | landed (n : Nat)
  /-- Undoing its deletable preparation, because the action is already
  satisfied or is no longer check. -/
  | undoing (satisfied : Bool)
  deriving DecidableEq

structure Journal (A : Type) where
  recorded : List A
  phase : Phase

inductive Result where
  | landed (n : Nat)
  /-- Rejected, with its preparation undone. -/
  | undone
  /-- Already satisfied, with its preparation undone and no entry. -/
  | satisfied
  deriving DecidableEq

/-- How an undo ends. -/
def Result.ofUndo (satisfied : Bool) : Result := if satisfied then .satisfied else .undone

structure World (A : Type) where
  slots : Store Bytes
  /-- Slots of this operation's sent claim requests that have not landed. -/
  inflight : List Nat
  /-- Deletable preparation outside the sequence; never key-copy or announcement slots. -/
  artifacts : List A
  /-- Slots where this operation's key copies and announcements landed. -/
  prepared : List Nat
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

/-- The operation's steps and the environment's. `check s n` is the
operation's validation against slots `1..n-1`. `fault` enables one faulty
variant. -/
inductive Step [DecidableEq A] (check : Store Bytes → Nat → Verdict) (fault : Fault) : World A → World A → Prop
  | record (w : World A) (rec : List A) (a : A) (hj : w.journal = some ⟨rec, .prep⟩) :
      Step check fault w { w with journal := some ⟨a :: rec, .prep⟩ }
  /-- Send the create of a recorded artifact. -/
  | sendArt (w : World A) (rec : List A) (a : A) (hj : w.journal = some ⟨rec, .prep⟩) (ha : a ∈ rec) :
      Step check fault w { w with artReq := a :: w.artReq }
  /-- A sent create lands, at any time, even after a crash or the journal's end. -/
  | landArt (w : World A) (a : A) (ha : a ∈ w.artReq) :
      Step check fault w { w with artifacts := a :: w.artifacts, artReq := w.artReq.erase a }
  /-- A sent create ends without landing: awaited to failure, or cancelled. -/
  | endArt (w : World A) (a : A) (ha : a ∈ w.artReq) :
      Step check fault w { w with artReq := w.artReq.erase a }
  | claim (w : World A) (rec : List A) (ph : Phase) (n : Nat) (hj : w.journal = some ⟨rec, ph⟩)
      (hph : ph = .prep ∨ ph = .retry) (hn : 1 ≤ n) (hhead : AtHead w.slots n)
      (hv : check w.slots n = .allow) :
      Step check fault w { w with journal := some ⟨rec, .claiming n⟩, inflight := n :: w.inflight }
  /-- Claim the next slot for a key copy or announcement, which lands there. A
  slot another device took first is the `other` step's. -/
  | prepare (w : World A) (rec : List A) (ph : Phase) (n : Nat) (kind : Preparation) (value : Nat)
      (hj : w.journal = some ⟨rec, ph⟩) (hph : ph = .prep ∨ ph = .retry) (hn : 1 ≤ n)
      (hhead : AtHead w.slots n) :
      Step check fault w
        { w with slots := create w.slots n (.preparation kind value), prepared := n :: w.prepared }
  | resend (w : World A) (rec : List A) (n : Nat) (hj : w.journal = some ⟨rec, .claiming n⟩) :
      Step check fault w { w with inflight := n :: w.inflight }
  | land (w : World A) (n : Nat) (hn : n ∈ w.inflight) :
      Step check fault w { w with slots := create w.slots n (.ours n), inflight := w.inflight.erase n }
  | other (w : World A) (n x : Nat) :
      Step check fault w { w with slots := create w.slots n (.other x) }
  | won (w : World A) (rec : List A) (n : Nat) (hj : w.journal = some ⟨rec, .claiming n⟩)
      (hs : w.slots n = some (.ours n)) :
      Step check fault w { w with journal := some ⟨rec, .landed n⟩ }
  | lost (w : World A) (rec : List A) (n x : Nat) (hj : w.journal = some ⟨rec, .claiming n⟩)
      (hs : w.slots n = some (.other x)) :
      Step check fault w { w with journal := some ⟨rec, .retry⟩ }
  | giveUp (w : World A) (rec : List A) (ph : Phase) (n : Nat) (hj : w.journal = some ⟨rec, ph⟩)
      (hph : ph = .prep ∨ ph = .retry) (hhead : AtHead w.slots n) (hv : check w.slots n = .refuse) :
      Step check fault w { w with journal := some ⟨rec, .undoing false⟩ }
  | satisfy (w : World A) (rec : List A) (ph : Phase) (n : Nat) (hj : w.journal = some ⟨rec, ph⟩)
      (hph : ph = .prep ∨ ph = .retry) (hhead : AtHead w.slots n)
      (hv : check w.slots n = .satisfied) :
      Step check fault w { w with journal := some ⟨rec, .undoing true⟩ }
  /-- Delete an artifact by its recorded name, once no create of it can still
  land. -/
  | undoOne (w : World A) (rec : List A) (a : A) (b : Bool) (hj : w.journal = some ⟨rec, .undoing b⟩)
      (hreq : a ∉ w.artReq ∨ fault = .eagerUndo) :
      Step check fault w
        { w with artifacts := w.artifacts.filter (· ≠ a),
                 journal := some ⟨rec.filter (· ≠ a), .undoing b⟩ }
  /-- Confirm the artifacts are absent, then delete the journal. -/
  | finishUndo (w : World A) (b : Bool) (hj : w.journal = some ⟨[], .undoing b⟩)
      (hgone : w.artifacts = []) :
      Step check fault w { w with journal := none, result := some (Result.ofUndo b) }
  | complete (w : World A) (rec : List A) (n : Nat) (hj : w.journal = some ⟨rec, .landed n⟩) :
      Step check fault w { w with journal := none, result := some (.landed n) }
  /-- The faulty restart: the claim in flight is forgotten. -/
  | forget (w : World A) (rec : List A) (n : Nat) (hf : fault = .forget)
      (hj : w.journal = some ⟨rec, .claiming n⟩) :
      Step check fault w { w with journal := some ⟨rec, .prep⟩ }

inductive Steps [DecidableEq A] (check : Store Bytes → Nat → Verdict) (fault : Fault) : World A → World A → Prop
  | refl (w : World A) : Steps check fault w w
  | step {w w' w'' : World A} : Step check fault w w' → Steps check fault w' w'' →
      Steps check fault w w''

/-- A journaled operation, with no artifacts and no requests, over any slots
that hold none of its bytes. Nothing here assumes it has validated: each claim
step checks `check` at the slot it claims. -/
def start (s : Store Bytes) : World A :=
  { slots := s, inflight := [], artifacts := [], prepared := [], artReq := [],
    journal := some ⟨[], .prep⟩, result := none }

/-- The journal's phase, or none once it is deleted. -/
def World.phase (w : World A) : Option Phase := w.journal.map (·.phase)

/-- `s` holds this operation's bytes at slot `n`. -/
def Own (s : Store Bytes) (n : Nat) : Prop := s n = some (.ours n)

/-- Validation reads only slots `1..n-1`. -/
def PrefixOnly {α : Type} (check : Store Bytes → Nat → α) : Prop :=
  ∀ s t n, (∀ i, 1 ≤ i → i < n → s i = t i) → check s n = check t n

/-- What every reachable world satisfies. -/
structure Inv (check : Store Bytes → Nat → Verdict) (w : World A) : Prop where
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
    1 ≤ n ∧ (∀ i, 1 ≤ i → i < n → w.slots i ≠ none) ∧ check w.slots n = .allow
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
theorem check_create {α : Type} {check : Store Bytes → Nat → α} (hp : PrefixOnly check)
    (s : Store Bytes) (n m : Nat) (b : Bytes) (hfill : ∀ i, 1 ≤ i → i < n → s i ≠ none) :
    check (create s m b) n = check s n :=
  hp _ _ n (fun i h1 h2 => create_keeps_filled s m b i (hfill i h1 h2))

theorem inv_start (check : Store Bytes → Nat → Verdict) (s : Store Bytes)
    (h : ∀ n m, s n ≠ some (.ours m)) : Inv check (start (A := A) s) := by
  refine ⟨fun n m hs => absurd hs (h n m), fun a ha => by simp [start] at ha,
    fun m hm => by simp [start] at hm, fun n hn => absurd hn (h n n), ?_, ?_, by simp [start]⟩
  · intro n hn; simp [start, World.phase] at hn
  · intro n hn; simp [start, World.phase] at hn

/-- In a phase other than claiming or landed, with the journal present, the
operation has no bytes in the sequence. -/
theorem no_own {check : Store Bytes → Nat → Verdict} {w : World A} (hi : Inv check w)
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

theorem result_none {check : Store Bytes → Nat → Verdict} {w : World A} (hi : Inv check w)
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
theorem step_req [DecidableEq A] {check : Store Bytes → Nat → Verdict} {w w' : World A}
    (st : Step check .none w w') (hi : Inv check w) (hq : ReqInv w) : ReqInv w' := by
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
  | satisfy rec ph n hj =>
    refine req_keep hq (fun _ h => h) (fun j hj' => ?_) (fun _ h => h)
    rw [hj] at hj'; cases hj'; exact ⟨_, rfl, fun _ h => h⟩
  | prepare => exact req_keep hq (fun _ h => h) (fun j hj => ⟨j, hj, fun _ h => h⟩) (fun _ h => h)
  | undoOne rec a sat hj hreq =>
    have ha : a ∉ w.artReq := by rcases hreq with h | h; exact h; cases h
    intro b hb
    rcases hq b hb with ⟨j, hj', hbj⟩ | h
    · rw [hj] at hj'; cases hj'
      have hne : b ≠ a := by intro he; subst he; exact ha hb
      exact Or.inl ⟨_, rfl, by simp [hbj, hne]⟩
    · exact Or.inr h
  | finishUndo sat hj hgone =>
    intro b hb
    rcases hq b hb with ⟨j, hj', hbj⟩ | ⟨n, hn⟩
    · rw [hj] at hj'; cases hj'; simp at hbj
    · simp [result_none hi hj] at hn
  | complete rec n hj => intro b _; exact Or.inr ⟨n, rfl⟩
  | forget rec n hf => cases hf

/-- **Every step keeps the invariant.** -/
theorem step_inv [DecidableEq A] {check : Store Bytes → Nat → Verdict} (hp : PrefixOnly check) {w w' : World A}
    (st : Step check .none w w') (hi : Inv check w) (hq : ReqInv w) : Inv check w' := by
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
      rw [check_create hp _ _ _ _ hfill]; exact hv
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
      rw [check_create hp _ _ _ _ hfill]; exact hv
  | prepare rec ph n kind value hj hph hn hhead =>
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
      rw [check_create hp _ _ _ _ hfill]; exact hv
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
  | satisfy rec ph n hj hph hhead hv =>
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
  | undoOne rec a sat hj hreq =>
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
  | finishUndo sat hj hgone =>
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
    · cases sat <;> simp [World.phase, Result.ofUndo] at hk
    · cases sat <;> simp [World.phase, Result.ofUndo] at hk
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

theorem steps_inv [DecidableEq A] {check : Store Bytes → Nat → Verdict} (hp : PrefixOnly check) {w w' : World A}
    (run : Steps check .none w w') (hi : Inv check w) (hq : ReqInv w) : Inv check w' ∧ ReqInv w' := by
  induction run with
  | refl => exact ⟨hi, hq⟩
  | step s _ ih => exact ih (step_inv hp s hi hq) (step_req s hi hq)

/-- **The claim loop and journal.** From validation over any slots holding
none of its entry bytes, through any interleaving of its own steps, crashes and
restarts, late-landing requests and other devices' claims:

- its entry bytes occupy at most one slot (the entry never lands twice);
- a landed operation's entry bytes are at its slot, whose predecessors were all
  filled when it validated, and validation held against them;
- an undone or already satisfied operation has no entry bytes in the sequence,
  now or later, and none of its recorded deletable artifacts remains in
  storage, now or later: its undo ran only after every create it sent had
  ended. -/
theorem operation_outcome [DecidableEq A] {check : Store Bytes → Nat → Verdict} (hp : PrefixOnly check)
    (s0 : Store Bytes) (h0 : ∀ n m, s0 n ≠ some (.ours m)) {w : World A}
    (run : Steps check .none (start s0) w) :
    (∀ n m, Own w.slots n → Own w.slots m → n = m) ∧
    (∀ n, w.result = some (.landed n) →
      Own w.slots n ∧ 1 ≤ n ∧ (∀ i, 1 ≤ i → i < n → w.slots i ≠ none) ∧ check w.slots n = .allow) ∧
    (w.result = some .undone ∨ w.result = some .satisfied →
      (∀ n, ¬ Own w.slots n) ∧ w.artifacts = []) := by
  have hi := (steps_inv hp run (inv_start check s0 h0) (fun a ha => by simp [start] at ha)).1
  refine ⟨fun n m hn hm => ?_, fun n hn => ?_, fun hu => ⟨fun n hn => ?_, ?_⟩⟩
  · rcases hi.own_phase n hn with h | h | h <;> rcases hi.own_phase m hm with h' | h' | h'
    all_goals first
      | (rw [h] at h'; simp at h'; try omega)
      | (have := hi.result_done (by simp [h]); simp [World.phase, this] at h')
      | (have := hi.result_done (by simp [h']); simp [World.phase, this] at h)
  · exact ⟨hi.landed_own n (Or.inr hn), hi.validated n (Or.inr (Or.inr hn))⟩
  · have hj := hi.result_done (by rcases hu with hu | hu <;> simp [hu])
    rcases hi.own_phase n hn with h | h | h
    · simp [World.phase, hj] at h
    · simp [World.phase, hj] at h
    · rcases hu with hu | hu <;> rw [hu] at h <;> cases h
  · have hj := hi.result_done (by rcases hu with hu | hu <;> simp [hu])
    cases ha : w.artifacts with
    | nil => rfl
    | cons a rest =>
      rcases hi.recorded a (by simp [ha]) with ⟨j, hj', _⟩ | ⟨n, hn⟩
      · simp [hj] at hj'
      · rcases hu with hu | hu <;> rw [hu] at hn <;> cases hn

/-- Operation steps never erase or replace an occupied history slot. -/
theorem step_preserves_history [DecidableEq A] {check : Store Bytes → Nat → Verdict}
    {fault : Fault} {w w' : World A} (step : Step check fault w w') :
    Extends w.slots w'.slots := by
  cases step with
  | land => exact create_extends _ _ _
  | other => exact create_extends _ _ _
  | prepare => exact create_extends _ _ _
  | _ => intro _ _ h; exact h

theorem steps_preserve_history [DecidableEq A] {check : Store Bytes → Nat → Verdict}
    {fault : Fault} {w w' : World A} (run : Steps check fault w w') :
    Extends w.slots w'.slots := by
  induction run with
  | refl => intro _ _ h; exact h
  | step s _ ih => intro i b h; exact ih i b (step_preserves_history s i b h)

/-- In particular, confirmed undo keeps every already claimed key copy and
announcement. -/
theorem undo_keeps_preparation [DecidableEq A] {check : Store Bytes → Nat → Verdict}
    {w w' : World A} (run : Steps check .none w w')
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
    ∃ w : World Nat, Steps (fun _ _ => .refuse) .none (start initial) w ∧
      w.result = some .undone ∧ w.artifacts = [] ∧
      w.slots 1 = some (.preparation .keyCopy 7) ∧
      w.slots 2 = some (.preparation .announcement 8) := by
  intro initial
  refine ⟨_, Steps.step (Step.record _ [] 8 rfl)
    (Steps.step (Step.sendArt _ [8] 8 rfl (by simp))
    (Steps.step (Step.landArt _ 8 (by simp))
    (Steps.step (Step.giveUp _ [8] .prep 3 rfl (Or.inl rfl) ⟨?_, rfl⟩ rfl)
    (Steps.step (Step.undoOne _ [8] 8 false rfl (Or.inl (by simp [start])))
    (Steps.step (Step.finishUndo _ false (by simp) (by simp [start])) (Steps.refl _)))))),
    rfl, by simp [start], rfl, rfl⟩
  intro i hi hlt
  have h : i = 1 ∨ i = 2 := by omega
  rcases h with rfl | rfl <;> simp [start, initial]

/-! ## Preparation in the sequence (§11.1, §11.5)

Key copies and snapshot announcements claim their own slots while the operation
prepares. -/

/-- The operation is claiming slot `n`, has landed there, or has ended landed
there. -/
def Claimed (w : World A) (n : Nat) : Prop :=
  w.phase = some (.claiming n) ∨ w.phase = some (.landed n) ∨ w.result = some (.landed n)

/-- Each slot the operation's preparation landed in holds it, every slot
before it is filled, and it comes before any slot the entry claims. -/
def PrepInv (w : World A) : Prop :=
  ∀ i ∈ w.prepared, (∃ kind value, w.slots i = some (.preparation kind value)) ∧
    (∀ j, 1 ≤ j → j < i → w.slots j ≠ none) ∧ ∀ n, Claimed w n → i < n

theorem prep_start (s : Store Bytes) : PrepInv (start (A := A) s) := by
  intro i hi; simp [start] at hi

/-- A slot the entry claims at the head comes after every prepared slot. -/
theorem prepared_before_head {w : World A} (hp : PrepInv w) {i n : Nat} (hi : i ∈ w.prepared)
    (hn : 1 ≤ n) (head : AtHead w.slots n) : i < n := by
  obtain ⟨⟨kind, value, held⟩, below, _⟩ := hp i hi
  by_cases lt : i < n
  · exact lt
  · have ne : i ≠ n := by intro eq; subst eq; rw [head.2] at held; cases held
    exact absurd head.2 (below n hn (by omega))

theorem prep_entry {w w' : World A} (hp : PrepInv w) {i : Nat} (hi : i ∈ w.prepared)
    (grows : Extends w.slots w'.slots)
    (claimed : ∀ n, Claimed w' n → Claimed w n ∨ (1 ≤ n ∧ AtHead w.slots n)) :
    (∃ kind value, w'.slots i = some (.preparation kind value)) ∧
      (∀ j, 1 ≤ j → j < i → w'.slots j ≠ none) ∧ ∀ n, Claimed w' n → i < n := by
  obtain ⟨⟨kind, value, held⟩, below, before⟩ := hp i hi
  refine ⟨⟨kind, value, grows i _ held⟩, fun j h1 h2 => ?_, fun n hn => ?_⟩
  · cases hj : w.slots j with
    | none => exact absurd hj (below j h1 h2)
    | some b => rw [grows j b hj]; simp
  · rcases claimed n hn with old | head
    · exact before n old
    · exact prepared_before_head hp hi head.1 head.2

theorem prep_keep {w w' : World A} (hp : PrepInv w) (same : w'.prepared = w.prepared)
    (grows : Extends w.slots w'.slots)
    (claimed : ∀ n, Claimed w' n → Claimed w n ∨ (1 ≤ n ∧ AtHead w.slots n)) : PrepInv w' :=
  fun _ hi => prep_entry hp (same ▸ hi) grows claimed

theorem step_prep [DecidableEq A] {check : Store Bytes → Nat → Verdict} {fault : Fault}
    {w w' : World A} (st : Step check fault w w') (hi : Inv check w) (hp : PrepInv w) :
    PrepInv w' := by
  have grows := step_preserves_history st
  have hres : ∀ {j : Journal A}, w.journal = some j → w.result = none := fun hj =>
    result_none hi hj
  cases st with
  | prepare rec ph n kind value hj hph hn hhead =>
    intro i hi'
    simp only [List.mem_cons] at hi'
    rcases hi' with rfl | old
    · refine ⟨⟨kind, value, Claim.create_self_none _ hhead.2⟩, fun j h1 h2 => ?_, fun m hm => ?_⟩
      · exact create_filled_ne _ _ _ _ (hhead.1 j h1 h2)
      · rcases hm with h | h | h
        · simp [World.phase, hj] at h; rcases hph with rfl | rfl <;> simp at h
        · simp [World.phase, hj] at h; rcases hph with rfl | rfl <;> simp at h
        · simp [hres hj] at h
    · exact prep_entry hp old grows (fun m hm => Or.inl hm)
  | claim rec ph n hj hph hn hhead hv =>
    refine prep_keep hp rfl grows fun m hm => ?_
    rcases hm with h | h | h
    · simp [World.phase] at h; subst h; exact Or.inr ⟨hn, hhead⟩
    · simp [World.phase] at h
    · simp [hres hj] at h
  | won rec n hj hs =>
    refine prep_keep hp rfl grows fun m hm => ?_
    rcases hm with h | h | h
    · simp [World.phase] at h
    · simp [World.phase] at h; subst h; exact Or.inl (Or.inl (by simp [World.phase, hj]))
    · simp [hres hj] at h
  | complete rec n hj =>
    refine prep_keep hp rfl grows fun m hm => ?_
    rcases hm with h | h | h
    · simp [World.phase] at h
    · simp [World.phase] at h
    · simp at h; subst h; exact Or.inl (Or.inr (Or.inl (by simp [World.phase, hj])))
  | finishUndo sat hj hgone =>
    refine prep_keep hp rfl grows fun m hm => ?_
    rcases hm with h | h | h
    · simp [World.phase] at h
    · simp [World.phase] at h
    · cases sat <;> simp [Result.ofUndo] at h
  | record rec a hj | lost rec n x hj | giveUp rec ph n hj | satisfy rec ph n hj
  | undoOne rec a sat hj | forget rec n hf hj =>
    refine prep_keep hp rfl grows fun m hm => ?_
    rcases hm with h | h | h <;> simp_all [World.phase]
  | sendArt | landArt | endArt | resend | land | other =>
    exact prep_keep hp rfl grows fun m hm => Or.inl hm

/-- **Preparation comes before the entry, and stays.** Over any interleaving,
each slot the operation's key copies and announcements landed in holds them,
and a landed entry's slot comes after every one of them. -/
theorem preparation_precedes_entry [DecidableEq A] {check : Store Bytes → Nat → Verdict}
    (pre : PrefixOnly check) (s0 : Store Bytes) (h0 : ∀ n m, s0 n ≠ some (.ours m)) {w : World A}
    (run : Steps check .none (start s0) w) {i n : Nat} (prepared : i ∈ w.prepared)
    (landed : w.result = some (.landed n)) :
    i < n ∧ ∃ kind value, w.slots i = some (.preparation kind value) := by
  have both : ∀ {u v : World A}, Steps check .none u v → Inv check u → ReqInv u → PrepInv u →
      PrepInv v := by
    intro u v r
    induction r with
    | refl => exact fun _ _ hp => hp
    | step s _ ih =>
      intro hi hq hp
      exact ih (step_inv pre s hi hq) (step_req s hi hq) (step_prep s hi hp)
  have hp := both run (inv_start check s0 h0) (fun a ha => by simp [start] at ha) (prep_start s0)
  obtain ⟨held, _, before⟩ := hp i prepared
  exact ⟨before n (Or.inr (Or.inr landed)), held⟩

/-- **An undo is final.** Once the operation is undoing, because the action is
already satisfied or no longer allowed, every later state is still undoing, or
has ended with that outcome. It never claims again: a failed undo is a step not
taken, so the record stays and the undo retries. -/
theorem undo_final [DecidableEq A] {check : Store Bytes → Nat → Verdict} {fault : Fault}
    {w w' : World A} (run : Steps check fault w w') (sat : Bool)
    (undoing : w.phase = some (.undoing sat) ∨ (w.journal = none ∧ w.result = some (Result.ofUndo sat))) :
    w'.phase = some (.undoing sat) ∨ (w'.journal = none ∧ w'.result = some (Result.ofUndo sat)) := by
  induction run with
  | refl => exact undoing
  | step st _ ih =>
    apply ih
    cases st with
    | undoOne rec a b hj =>
      rcases undoing with h | ⟨h, _⟩
      · simp [World.phase, hj] at h; subst h; exact Or.inl (by simp [World.phase])
      · simp [hj] at h
    | finishUndo b hj =>
      rcases undoing with h | ⟨h, _⟩
      · simp [World.phase, hj] at h; subst h; exact Or.inr ⟨rfl, rfl⟩
      · simp [hj] at h
    | sendArt | landArt | endArt | land | other | prepare => simpa [World.phase] using undoing
    | record rec a hj | claim rec ph n hj | resend rec n hj | won rec n hj | lost rec n x hj
    | giveUp rec ph n hj | satisfy rec ph n hj | complete rec n hj | forget rec n hf hj =>
      rcases undoing with h | ⟨h, _⟩ <;> simp_all [World.phase]

/-- **Counterexample: a restart that forgets its claim lands twice.** The
operation claims slot 1, the request lands, and a restart that does not
resolve its recorded claim validates again, claims slot 2 and lands there too. -/
theorem forgetful_lands_twice :
    ∃ w : World Nat, Steps (fun _ _ => .allow) .forget (start (fun _ => none)) w ∧
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
longer check, deletes the artifact while the create is in flight, confirms
absence and ends. The create then lands: the operation is undone and its
artifact is in storage. -/
theorem eager_undo_leaks :
    ∃ w : World Nat, Steps (fun _ _ => .refuse) .eagerUndo (start (fun _ => none)) w ∧
      w.result = some .undone ∧ w.artifacts = [7] := by
  refine ⟨_, Steps.step (Step.record _ [] 7 rfl)
    (Steps.step (Step.sendArt _ [7] 7 rfl (by simp))
    (Steps.step (Step.giveUp _ [7] .prep 1 rfl (Or.inl rfl) ⟨fun i h1 h2 => by omega, rfl⟩ rfl)
    (Steps.step (Step.undoOne _ [7] 7 false rfl (Or.inr rfl))
    (Steps.step (Step.finishUndo _ false (by simp) rfl)
    (Steps.step (Step.landArt _ 7 (by simp)) (Steps.refl _)))))), ?_, ?_⟩
  · rfl
  · simp [start]

end CovenSequence.Operation
