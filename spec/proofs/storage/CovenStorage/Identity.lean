import CovenStorage.Objects

namespace CovenStorage

/-- §10: writes and entries close independently; zero is an empty log. -/
structure Ends where
  writes : Nat
  entries : Nat
  deriving DecidableEq, Repr

def Ends.combine (a b : Ends) : Ends :=
  ⟨max a.writes b.writes, max a.entries b.entries⟩

theorem ends_comm (a b : Ends) : a.combine b = b.combine a := by
  simp [Ends.combine, Nat.max_comm]

theorem ends_assoc (a b c : Ends) : (a.combine b).combine c = a.combine (b.combine c) := by
  simp [Ends.combine, Nat.max_assoc]

theorem ends_idempotent (a : Ends) : a.combine a = a := by
  cases a; simp [Ends.combine]

theorem ends_keep_both (a b : Ends) :
    a.writes ≤ (a.combine b).writes ∧ b.writes ≤ (a.combine b).writes ∧
    a.entries ≤ (a.combine b).entries ∧ b.entries ≤ (a.combine b).entries := by
  exact ⟨Nat.le_max_left _ _, Nat.le_max_right _ _, Nat.le_max_left _ _, Nat.le_max_right _ _⟩

structure Counters where
  logs : Ends
  snapshot : Nat
  deriving DecidableEq, Repr

def Counters.ahead (observed reserved : Counters) : Bool :=
  observed.logs.writes > reserved.logs.writes ||
  observed.logs.entries > reserved.logs.entries || observed.snapshot > reserved.snapshot

/-- Complete listings and authenticated coverage/positions, §10. Numbers
from a failed or partial scan cannot construct a successful observation. -/
structure Evidence where
  listed : Counters
  covered : Ends
  posted : Ends
  replaced : Bool
  removed : Bool
  deriving DecidableEq, Repr

def Evidence.observed (e : Evidence) : Counters :=
  ⟨(e.listed.logs.combine e.covered).combine e.posted, e.listed.snapshot⟩

inductive ResetReason where
  | identityMismatch | storageAhead | slotMismatch | replaced | damage
  deriving DecidableEq, Repr

inductive Gate where
  | send
  | blocked (failure : ReadFailure)
  | reset (reason : ResetReason)
  | stopped
  deriving DecidableEq, Repr

/-- This is the detection algorithm in §10, not an oracle saying whether
the database has ever been rolled back. Equal reservations are allowed. -/
def checkIdentity (device : Nat) (reserved : Counters) (custody : Option Nat)
    (scan : Except ReadFailure Evidence) : Gate :=
  if custody ≠ some device then .reset .identityMismatch
  else match scan with
  | .error e => .blocked e
  | .ok e =>
    if e.removed then .stopped
    else if e.replaced then .reset .replaced
    else if e.observed.ahead reserved then .reset .storageAhead
    else .send

inductive SendKind where
  | write | entry | snapshot | file | keyCopy | positions
  deriving DecidableEq, Repr

/-- All outgoing paths, including ones outside the periodic sync, use §10. -/
def authorizeSend (_kind : SendKind) (device : Nat) (reserved : Counters)
    (custody : Option Nat) (scan : Except ReadFailure Evidence) : Gate :=
  checkIdentity device reserved custody scan

theorem sends_require_check (kind : SendKind) (d : Nat) (r : Counters)
    (c : Option Nat) (s : Except ReadFailure Evidence) :
    authorizeSend kind d r c s = .send ↔ checkIdentity d r c s = .send := Iff.rfl

theorem failed_scan_sends_nothing (kind : SendKind) (d : Nat) (r : Counters)
    (c : Option Nat) (e : ReadFailure) :
    authorizeSend kind d r c (.error e) ≠ .send := by
  simp [authorizeSend, checkIdentity]; split <;> simp

theorem send_has_custody (d : Nat) (r : Counters) (c : Option Nat)
    (s : Except ReadFailure Evidence) (h : checkIdentity d r c s = .send) :
    c = some d := by
  by_cases he : c = some d
  · exact he
  · simp [checkIdentity, he] at h

theorem send_not_ahead (d : Nat) (r : Counters) (c : Option Nat)
    (e : Evidence) (h : checkIdentity d r c (.ok e) = .send) :
    e.observed.ahead r = false ∧ e.replaced = false ∧ e.removed = false := by
  simp only [checkIdentity] at h
  split at h <;> simp_all
  split at h <;> simp_all
  split at h <;> simp_all

theorem send_covers_all_evidence (d : Nat) (r : Counters) (c : Option Nat)
    (e : Evidence) (h : checkIdentity d r c (.ok e) = .send) :
    e.listed.snapshot ≤ r.snapshot ∧
    ∀ evidence ∈ [e.listed.logs, e.covered, e.posted],
      evidence.writes ≤ r.logs.writes ∧ evidence.entries ≤ r.logs.entries := by
  have safe := (send_not_ahead d r c e h).1
  simp only [Counters.ahead, Bool.or_eq_false_iff, decide_eq_false_iff_not,
    Nat.not_lt] at safe
  constructor
  · exact safe.2
  · intro evidence member
    simp only [List.mem_cons, List.not_mem_nil, or_false] at member
    rcases member with rfl | rfl | rfl <;>
      simp only [Evidence.observed, Ends.combine] at safe <;> omega

/-- A replacement is accepted only for its member's old, distinct id (D6).
Store-log authority and finality are supplied by replay, not redone here. -/
structure Replacement where
  old : Nat
  fresh : Nat
  ends : Ends
  registrationNumber : Nat
  reason : ResetReason
  deriving DecidableEq, Repr

def replacementAllowed (owner author : Nat) (r : Replacement) : Bool :=
  owner == author && r.old != r.fresh

theorem replacement_has_owner (owner author : Nat) (r : Replacement)
    (h : replacementAllowed owner author r = true) :
    owner = author ∧ r.old ≠ r.fresh := by
  simpa [replacementAllowed] using h

/-- Applied identities stand for atomic effects. A snapshot supplies these
identities together with their effects; loading replaces, never adds them. -/
abbrev Applied := List Path

def applyOnce (applied : Applied) (p : Path) : Applied :=
  if p ∈ applied then applied else p :: applied

theorem applyOnce_nodup (applied : Applied) (p : Path) (h : applied.Nodup) :
    (applyOnce applied p).Nodup := by
  unfold applyOnce
  split <;> simp_all

theorem applyOnce_members (applied : Applied) (p q : Path) :
    q ∈ applyOnce applied p ↔ q = p ∨ q ∈ applied := by
  unfold applyOnce
  split <;> simp_all

theorem applyOnce_idempotent (applied : Applied) (p : Path) :
    applyOnce (applyOnce applied p) p = applyOnce applied p := by
  have hm := (applyOnce_members applied p p).mpr (Or.inl rfl)
  change (if p ∈ applyOnce applied p then applyOnce applied p else p :: applyOnce applied p) = _
  simp only [hm, ↓reduceIte]

def receiveAll (applied : Applied) (delivery : List Path) : Applied :=
  delivery.foldl applyOnce applied

theorem receipt_no_duplicates (applied delivery : List Path) (h : applied.Nodup) :
    (receiveAll applied delivery).Nodup := by
  induction delivery generalizing applied with
  | nil => exact h
  | cons p ps ih => exact ih _ (applyOnce_nodup _ _ h)

theorem receipt_members (applied delivery : List Path) (p : Path) :
    p ∈ receiveAll applied delivery ↔ p ∈ applied ∨ p ∈ delivery := by
  induction delivery generalizing applied with
  | nil => simp [receiveAll]
  | cons q qs ih =>
    simp only [receiveAll, List.foldl_cons] at *
    rw [ih, applyOnce_members]
    simp only [List.mem_cons]
    grind

structure Local where
  device : Nat
  reserved : Counters
  queue : List Upload
  applied : Applied
  deriving DecidableEq, Repr

inductive Installation where
  | working (state : Local)
  | resetting (replacement : Replacement)
  deriving DecidableEq, Repr

/-- §10: discards the old database/queue and retains one bootstrap identity. -/
def beginReset (_local : Local) (replacement : Replacement) : Installation :=
  .resetting replacement

/-- A failed bootstrap step retains its id and entry; it cannot reopen old data. -/
def retryReset (i : Installation) : Installation := i

def writable : Installation → Bool
  | .working _ => true
  | .resetting _ => false

theorem reset_unavailable (l : Local) (r : Replacement) :
    writable (retryReset (beginReset l r)) = false := rfl

/-- Checked stored history has loaded, and a fresh registration is kept.
Earlier registration attempts may have landed too late (§9, §10).
Data writes and snapshots start from zero. -/
def loadDevice (fresh registrationNumber : Nat) (loaded : Applied) : Local :=
  ⟨fresh, ⟨⟨0, registrationNumber⟩, 0⟩, [], receiveAll [] loaded⟩

def finishReset (r : Replacement) (loaded : Applied) : Local :=
  loadDevice r.fresh r.registrationNumber loaded

/-- §10: once an entry is consumed as too late, catching up online permits
a new entry, under the same installation id, with newly observed ends. -/
def registerAgain (r : Replacement) (observed : Ends) : Replacement :=
  { r with registrationNumber := r.registrationNumber + 1, ends := observed }

theorem registration_retry_number (r : Replacement) (observed : Ends) :
    (registerAgain r observed).fresh = r.fresh ∧
    (registerAgain r observed).registrationNumber = r.registrationNumber + 1 ∧
    (registerAgain r observed).ends = observed := ⟨rfl, rfl, rfl⟩

theorem reset_discards_queue (r : Replacement) (loaded : Applied) :
    (finishReset r loaded).queue = [] := rfl

theorem reset_uses_fresh_id (r : Replacement) (loaded : Applied) :
    (finishReset r loaded).device = r.fresh := rfl

theorem reset_loads_once (r : Replacement) (loaded later : Applied) :
    (receiveAll (finishReset r loaded).applied later).Nodup ∧
    (∀ p, p ∈ receiveAll (finishReset r loaded).applied later ↔
      p ∈ loaded ∨ p ∈ later) := by
  constructor
  · exact receipt_no_duplicates _ _ (receipt_no_duplicates [] loaded (by simp))
  · intro p; simp [finishReset, loadDevice, receipt_members]

/-- §5, §6: commit reserves a fresh contiguous write number together with
the effect and queue record. The next attempt may still be untried. -/
def commitWrite (l : Local) (bytes : Bytes) : Local :=
  let number := l.reserved.logs.writes + 1
  let path := Path.write l.device number
  { l with reserved := { l.reserved with logs := { l.reserved.logs with writes := number } }
           queue := l.queue ++ [.untried ⟨path, bytes⟩]
           applied := applyOnce l.applied path }

theorem commit_reserves_next (l : Local) (bytes : Bytes) :
    (commitWrite l bytes).reserved.logs.writes = l.reserved.logs.writes + 1 ∧
    Path.write l.device (l.reserved.logs.writes + 1) ∈ (commitWrite l bytes).applied := by
  simp [commitWrite, applyOnce_members]

theorem reset_first_write_new_path (r : Replacement) (loaded : Applied) (bytes : Bytes)
    (owner author : Nat) (h : replacementAllowed owner author r = true) (oldNumber : Nat) :
    (commitWrite (finishReset r loaded) bytes).reserved.logs.writes = 1 ∧
    Path.write (finishReset r loaded).device 1 ≠ .write r.old oldNumber := by
  have fresh := (replacement_has_owner owner author r h).2
  constructor
  · rfl
  · change Path.write r.fresh 1 ≠ .write r.old oldNumber
    intro he; exact fresh (Path.write.inj he).1.symm

/-- An attempt that fails to land still commits its fixed attempt record.
Network delivery is a separate event; the caller passes §10's checked gate. -/
def attemptHead (l : Local) (key format : Nat) (gate : Gate) : Local × Option Attempt :=
  if gate = .send then
    match l.queue with
    | [] => (l, none)
    | q :: qs =>
      let a := prepare key format q
      ({ l with queue := .tried a :: qs }, some a)
  else (l, none)

theorem blocked_gate_emits_nothing (l : Local) (key format : Nat) (gate : Gate)
    (h : gate ≠ .send) : attemptHead l key format gate = (l, none) := by
  simp [attemptHead, h]

/-- Out-of-band restore of the database alone. Device custody is not copied
from the backup, nor necessarily erased on the surviving installation. -/
structure BackupRun where
  live : Local
  backup : Local
  custody : Option Nat
  encrypted : List Attempt
  deriving DecidableEq, Repr

def backupAttempt (s : BackupRun) (key format : Nat) (scan : Except ReadFailure Evidence) :
    BackupRun :=
  let gate := checkIdentity s.live.device s.live.reserved s.custody scan
  let (live, emitted) := attemptHead s.live key format gate
  { s with live := live, encrypted := s.encrypted ++ emitted.toList }

/-- One installation stops, restores its database, and opens it again.
There is no second simultaneously live writer in this state space. -/
def restoreDatabase (s : BackupRun) : BackupRun := { s with live := s.backup }

inductive LogKind where
  | writes | entries
  deriving DecidableEq, Repr

def Ends.at (e : Ends) : LogKind → Nat
  | .writes => e.writes
  | .entries => e.entries

inductive LogDecision where
  | consume | awaitsReplacement
  deriving DecidableEq, Repr

/-- §10, D8: an object past the end remains blocked, with its input retained. -/
def closedLog (ends : Ends) (kind : LogKind) (number : Nat) : LogDecision :=
  if number ≤ ends.at kind then .consume else .awaitsReplacement

theorem combined_end_keeps_completed (a b : Ends) (kind : LogKind) (n : Nat)
    (h : n ≤ a.at kind ∨ n ≤ b.at kind) :
    closedLog (a.combine b) kind n = .consume := by
  have bound : n ≤ (a.combine b).at kind := by
    cases kind <;> simp only [Ends.at, Ends.combine] at * <;> omega
  simp [closedLog, bound]

theorem beyond_end_waits (e : Ends) (kind : LogKind) (n : Nat)
    (h : e.at kind < n) : closedLog e kind n = .awaitsReplacement := by
  simp [closedLog, show ¬ n ≤ e.at kind by omega]

end CovenStorage
