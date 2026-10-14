import CovenStorelog.PositionFingerprints

/-! Posting positions (sync pass step 9, §19.1) with fingerprints under usable
keys. Two readings of where those keys come from:

* `fresh`: only a send-fresh copy observation, less than five minutes after
  its catch-up started (§10). Without one, current positions wait and the
  confirmed post stands.
* `pass`: this pass's completed observation, whatever its age. Sending is
  still gated by freshness.

Positions' other fields (write and store-log positions, schema, pending
reports) are abstracted to `past` and `pending`; key selection never reads
them. Time is the monotonic elapsed time counting sleep. -/
namespace CovenStorelog.PositionPublication
open CurrentKeys
open CurrentReplay (KeyBytes KeyHash)

/-- How long a completed catch-up permits sending (§10), in seconds. -/
def window : Nat := 300

inductive Binding where
  | device
  | member
  | absent
  | retired
  deriving DecidableEq, Repr

/-- The last completed membership and key-copy observation (sync pass §4). -/
structure Completed where
  started : Nat
  binding : Binding
  copies : List Copy
  deriving DecidableEq, Repr

def fresh (c : Completed) (now : Nat) : Bool := c.started ≤ now && now < c.started + window

/-- The publication gate: this device's binding and a fresh catch-up. -/
def canSend (c : Option Completed) (now : Nat) : Bool :=
  match c with
  | some c => c.binding == .device && fresh c now
  | none => false

def bound (c : Completed) : Bool := c.binding == .device || c.binding == .member

/-- T164's observation: send-fresh only. -/
def freshCopies (c : Option Completed) (now : Nat) : Option (List Copy) :=
  c.bind fun c => if bound c && fresh c now then some c.copies else none

/-- The alternative: this pass's completed observation, whatever its age. -/
def passCopies (c : Option Completed) : Option (List Copy) :=
  c.bind fun c => if bound c then some c.copies else none

structure Post where
  past : Nat
  pending : Nat
  fingerprints : List (Audience × KeyId)
  deriving DecidableEq, Repr

/-- What the device must agree on, given everything but key choice. -/
structure Input where
  hash : KeyBytes → KeyHash
  is : List Introduction
  state : State
  self : Nat
  held : List Payload
  audiences : List Audience
  past : Nat
  pending : Nat

/-- Each audience's usable key, or no fingerprint for it. -/
def fingerprints (x : Input) (copies : List Copy) : List (Audience × KeyId) :=
  x.audiences.filterMap fun a =>
    match select x.hash x.is x.state x.self (.complete copies) x.held a with
    | .use i => some (a, i.identity)
    | .pending _ => none

def current (x : Input) (copies : Option (List Copy)) : Option Post :=
  copies.map fun copies => ⟨x.past, x.pending, fingerprints x copies⟩

structure Outcome where
  /-- The post in storage afterwards. -/
  stored : Option Post
  sent : Bool
  /-- Current positions are confirmed: step 9's condition for the return
  checkpoint. -/
  confirmed : Bool
  deriving DecidableEq, Repr

/-- Without current positions, the confirmed post travels with fresh pending
reports. -/
def candidate (x : Input) (current confirmed : Option Post) : Option Post :=
  match current with
  | some post => some post
  | none => confirmed.map fun post => { post with pending := x.pending }

/-- Step 9. Only a changed candidate is sent, and only through the gate. -/
def publish (c : Option Completed) (now : Nat) (x : Input) (current : Option Post)
    (confirmed remote : Option Post) : Outcome :=
  if candidate x current confirmed = remote then ⟨remote, false, current.isSome⟩
  else if canSend c now then ⟨candidate x current confirmed, true, current.isSome⟩
  else ⟨remote, false, false⟩

def publishFresh (c : Option Completed) (now : Nat) (x : Input) (confirmed remote : Option Post) :=
  publish c now x (current x (freshCopies c now)) confirmed remote

def publishPass (c : Option Completed) (now : Nat) (x : Input) (confirmed remote : Option Post) :=
  publish c now x (current x (passCopies c)) confirmed remote

theorem send_has_fresh_observation (c : Option Completed) (now : Nat)
    (send : canSend c now = true) : ∃ copies, freshCopies c now = some copies := by
  cases c with
  | none => cases send
  | some c =>
    have h := Bool.and_eq_true_iff.mp send
    have device : c.binding = .device := by simpa using h.1
    exact ⟨c.copies, by simp [freshCopies, bound, device, h.2]⟩

/-- Anything sent goes through the gate, under either reading. -/
theorem sent_through_gate (c : Option Completed) (now : Nat) (x : Input)
    (current confirmed remote : Option Post)
    (sent : (publish c now x current confirmed remote).sent = true) : canSend c now = true := by
  unfold publish at sent
  by_cases h : candidate x current confirmed = remote
  · simp [h] at sent
  · by_cases g : canSend c now = true
    · exact g
    · simp [h, g] at sent

/-- Holding the post never leaks: with no fresh observation nothing is sent and
storage keeps exactly the post it had. -/
theorem hold_sends_nothing (c : Option Completed) (now : Nat) (x : Input)
    (confirmed remote : Option Post) (waiting : freshCopies c now = none) :
    (publishFresh c now x confirmed remote).sent = false ∧
      (publishFresh c now x confirmed remote).stored = remote := by
  have closed : canSend c now = false := by
    cases h : canSend c now
    · rfl
    · obtain ⟨_, some⟩ := send_has_fresh_observation c now h
      rw [waiting] at some
      cases some
  have nothing : current x (freshCopies c now) = none := by simp [current, waiting]
  unfold publishFresh publish
  rw [nothing]
  by_cases h : candidate x none confirmed = remote
  · simp [h]
  · simp [h, closed]

/-- Every posted fingerprint names a key `select` returns for that audience,
so `usable_fingerprint_untestable` applies to each. -/
theorem posted_fingerprints_usable (x : Input) (copies : List Copy) (a : Audience) (k : KeyId)
    (posted : (a, k) ∈ fingerprints x copies) :
    ∃ i, select x.hash x.is x.state x.self (.complete copies) x.held a = .use i ∧
      i.identity = k := by
  obtain ⟨b, _, hb⟩ := List.mem_filterMap.mp posted
  cases hs : select x.hash x.is x.state x.self (.complete copies) x.held b with
  | pending _ => simp [hs] at hb
  | use i =>
    simp only [hs, Option.some.injEq, Prod.mk.injEq] at hb
    obtain ⟨rfl, rfl⟩ := hb
    exact ⟨i, hs, rfl⟩

/-- Omitting a fingerprint changes nothing else: positions and pending reports,
which retention and finality read, never depend on key choice. -/
theorem omission_keeps_positions (x : Input) (a b : List Copy) :
    ((current x (some a)).map fun p => (p.past, p.pending)) =
      ((current x (some b)).map fun p => (p.past, p.pending)) := rfl

/-- The readings differ only after the send window: within it they coincide. -/
theorem readings_agree_when_fresh (c : Option Completed) (now : Nat)
    (live : ∀ g, c = some g → fresh g now = true) : freshCopies c now = passCopies c := by
  cases c with
  | none => rfl
  | some g => simp [freshCopies, passCopies, live g rfl]

/-- Without the gate nothing is sent and storage keeps its post. -/
theorem closed_keeps_storage (c : Option Completed) (now : Nat) (x : Input)
    (current confirmed remote : Option Post) (closed : canSend c now = false) :
    (publish c now x current confirmed remote).stored = remote ∧
      (publish c now x current confirmed remote).sent = false := by
  unfold publish
  by_cases h : candidate x current confirmed = remote
  · simp [h]
  · simp [h, closed]

/-- The two readings store and send exactly the same, so nothing that reads
posted positions (peers' retention, finality, agreement) can tell them apart.
They differ only in this device's own confirmation. -/
theorem readings_store_alike (c : Option Completed) (now : Nat) (x : Input)
    (confirmed remote : Option Post) :
    (publishFresh c now x confirmed remote).stored = (publishPass c now x confirmed remote).stored ∧
      (publishFresh c now x confirmed remote).sent = (publishPass c now x confirmed remote).sent := by
  cases hs : canSend c now
  · have a := closed_keeps_storage c now x (current x (freshCopies c now)) confirmed remote hs
    have b := closed_keeps_storage c now x (current x (passCopies c)) confirmed remote hs
    simp only [publishFresh, publishPass] at *
    exact ⟨a.1.trans b.1.symm, a.2.trans b.2.symm⟩
  · have same : freshCopies c now = passCopies c := by
      apply readings_agree_when_fresh
      intro g hg
      subst hg
      exact (Bool.and_eq_true_iff.mp hs).2
    simp [publishFresh, publishPass, same]

/-- The pass reading confirms unchanged positions with no send at any age. -/
theorem pass_confirms_unchanged (c : Completed) (now : Nat) (x : Input)
    (confirmed remote : Option Post) (b : bound c = true)
    (unchanged : current x (passCopies (some c)) = remote) (post : remote.isSome = true) :
    publishPass (some c) now x confirmed remote = ⟨remote, false, true⟩ := by
  have h : passCopies (some c) = some c.copies := by simp [passCopies, b]
  cases remote with
  | none => cases post
  | some r =>
    rw [h] at unchanged
    simp [publishPass, publish, h, unchanged, candidate]

/-- Waiting positions are never confirmed, whatever storage holds. -/
theorem waiting_unconfirmed (c : Option Completed) (now : Nat) (x : Input)
    (confirmed remote : Option Post) : (publish c now x none confirmed remote).confirmed = false := by
  unfold publish
  split
  · rfl
  · split <;> rfl

/-! ### Counterexample: a late step 9 never confirms

Ana's store from `PositionFingerprints`: her pass completes catch-up at 0
and reaches step 9 five minutes later, after long file or snapshot work. Her
positions are unchanged since her last post. -/

open PositionFingerprints

def ana : Completed := ⟨0, .device, storeCopies⟩

def anaInput : Input :=
  ⟨id, storeIntros, storeReplay.state, 0, anaHeld, [.store], 7, 0⟩

def lastPost : Post := ⟨7, 0, [(.store, k1.identity)]⟩

/-- The pass's observation yields exactly the stored post; the fresh reading
cannot compute it at 300 seconds, so the pass sends nothing and does not
confirm. The return checkpoint (§15) does not move. -/
theorem late_step_nine_unconfirmed :
    current anaInput (passCopies (some ana)) = some lastPost ∧
    publishPass (some ana) 300 anaInput (some lastPost) (some lastPost) =
      ⟨some lastPost, false, true⟩ ∧
    freshCopies (some ana) 300 = none ∧
    publishFresh (some ana) 300 anaInput (some lastPost) (some lastPost) =
      ⟨some lastPost, false, false⟩ ∧
    freshCopies (some ana) 299 = some storeCopies ∧
    (publishFresh (some ana) 299 anaInput (some lastPost) (some lastPost)).confirmed = true := by
  decide

example :
    (publishFresh (some ana) 300 anaInput (some lastPost) (some lastPost)).confirmed = false ∧
    (publishPass (some ana) 300 anaInput (some lastPost) (some lastPost)).confirmed = true := by
  decide

theorem ana_current : current anaInput (some storeCopies) = some lastPost := by decide

/-- The return checkpoint after a run of passes: each confirmed pass replaces
it with that pass's catch-up start. -/
def checkpoint (rule : Option Completed → Nat → Input → Option Post → Option Post → Outcome)
    (x : Input) (post : Post) (initial : Nat) : List (Nat × Nat) → Nat
  | [] => initial
  | (started, now) :: rest =>
    let next := if (rule (some ⟨started, .device, storeCopies⟩) now x (some post) (some post)).confirmed
      then started else initial
    checkpoint rule x post next rest

/-- If every pass reaches step 9 after its send window, the fresh reading never
moves the return checkpoint, for any number of passes and however recently
each caught up. The pass reading moves it every time. -/
theorem late_passes_stall (passes : List (Nat × Nat))
    (late : ∀ p ∈ passes, p.1 + window ≤ p.2) (initial : Nat) :
    checkpoint publishFresh anaInput lastPost initial passes = initial := by
  induction passes generalizing initial with
  | nil => rfl
  | cons p rest ih =>
    obtain ⟨started, now⟩ := p
    have stale : freshCopies (some ⟨started, .device, storeCopies⟩) now = none := by
      have := late (started, now) List.mem_cons_self
      simp [freshCopies, fresh, bound]
      omega
    have unconfirmed :
        (publishFresh (some ⟨started, .device, storeCopies⟩) now anaInput (some lastPost)
          (some lastPost)).confirmed = false := by
      simp only [publishFresh, stale]
      exact waiting_unconfirmed _ _ _ _ _
    simp only [checkpoint, unconfirmed]
    exact ih (fun q hq => late q (List.mem_cons_of_mem _ hq)) initial

theorem pass_reading_advances (started now : Nat) (rest : List (Nat × Nat)) (initial : Nat) :
    checkpoint publishPass anaInput lastPost initial ((started, now) :: rest) =
      checkpoint publishPass anaInput lastPost started rest := by
  have confirmed :
      (publishPass (some ⟨started, .device, storeCopies⟩) now anaInput (some lastPost)
        (some lastPost)).confirmed = true := by
    simp [publishPass, passCopies, bound, ana_current, publish, candidate]
  simp only [checkpoint, confirmed]
  rfl

end CovenStorelog.PositionPublication
