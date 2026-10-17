import CovenSequence.Causality
import CovenSequence.Device

/-! Reload stages published state, then installs it with the current local
queue in one transaction. Originals are never the working copy of the rows.
An audience becoming readable must supply its own loaded prefix. -/
namespace CovenSequence.QueueReload

variable {S : Sig} {Stamp : Type}
abbrev Write (S : Sig) (Stamp : Type) := Causality.Original S.Dev Stamp (S.Aud → Option S.Batch)

inductive Reason where
  | ordinary
  | reset
  | breakingRaise
  | newlyReadable
  deriving DecidableEq

def writable : Option Reason → Bool
  | none | some .ordinary => true
  | some .reset | some .breakingRaise | some .newlyReadable => false

structure Plan (S : Sig) where
  target : Nat
  follows : List S.Aud
  reason : S.Aud → Option Reason

structure State (S : Sig) (Stamp : Type) where
  authored : Causality.Local S.Dev Stamp (S.Aud → Option S.Batch)
  follows : List S.Aud
  rows : S.Aud → S.Rows

/-- The local counter is beyond every queued original. -/
def Numbered (s : Causality.Local S.Dev Stamp (S.Aud → Option S.Batch)) : Prop :=
  (s.queue.map (·.number)).Nodup ∧ ∀ w ∈ s.queue, w.number < s.nextNumber

theorem commit_numbered (s : Causality.Local S.Dev Stamp (S.Aud → Option S.Batch))
    (numbered : Numbered s) (d : S.Dev) (t : Stamp) (schema : Nat) (bytes : S.Aud → Option S.Batch) :
    Numbered (Causality.commit s d t schema bytes) := by
  have fresh : s.nextNumber ∉ s.queue.map (·.number) := by
    intro member
    obtain ⟨w, queued, eq⟩ := List.mem_map.mp member
    have := numbered.2 w queued
    omega
  constructor
  · simpa [Causality.commit, Causality.author, List.nodup_append, numbered.1] using fresh
  · intro w queued
    simp only [Causality.commit, List.mem_append, List.mem_singleton] at queued
    rcases queued with old | rfl
    · have := numbered.2 w old
      change w.number < s.nextNumber + 1
      omega
    · exact Nat.lt_succ_self _

def canCommit (s : State S Stamp) (p : Plan S) (bytes : S.Aud → Option S.Batch) : Prop :=
  ∀ A, (bytes A).isSome = true → A ∈ s.follows ∧ writable (p.reason A) = true

def consumed (slot : Write S Stamp → Option Nat) (target : Nat) (w : Write S Stamp) : Bool :=
  match slot w with | none => false | some n => decide (n ≤ target)

/-- Base admission covers authenticated membership, retirement and change
eligibility. Reset admission is explicit and precedes conversion. -/
def eligible (slot : Write S Stamp → Option Nat) (admitted : S.Aud → Write S Stamp → Bool)
    (A : S.Aud) (loaded : Reader S) (w : Write S Stamp) : Bool :=
  !consumed slot loaded.pos w && admitted A w && decide (loaded.aud.lastReset ≤ w.readThrough) &&
    (w.bytes A).isSome

def reapplied (slot : Write S Stamp → Option Nat) (admitted : S.Aud → Write S Stamp → Bool)
    (A : S.Aud) (loaded : Reader S) (queue : List (Write S Stamp)) : List (Write S Stamp) :=
  queue.filter (eligible slot admitted A loaded)

def applyOriginal (R : Rules S) (A : S.Aud) (version : Nat) (rows : S.Rows) (w : Write S Stamp) : S.Rows :=
  match w.bytes A with
  | none => rows
  | some batch => R.merge rows (R.convert w.schema version batch)

def rebuild (R : Rules S) (slot : Write S Stamp → Option Nat)
    (admitted : S.Aud → Write S Stamp → Bool) (A : S.Aud) (loaded : Reader S)
    (queue : List (Write S Stamp)) : S.Rows :=
  (reapplied slot admitted A loaded queue).foldl (applyOriginal R A loaded.aud.version) loaded.rows

/-- Installation reads the queue at commit, including app writes made while
loading. Previously displayed local effects are replaced, never replayed over. -/
def install (R : Rules S) (slot : Write S Stamp → Option Nat)
    (admitted : S.Aud → Write S Stamp → Bool) (s : State S Stamp) (p : Plan S)
    (loaded : S.Aud → Reader S) : State S Stamp :=
  { authored := Causality.catchUp s.authored p.target
    follows := p.follows
    rows := fun A => if A ∈ p.follows then rebuild R slot admitted A (loaded A) s.authored.queue else s.rows A }

/-- Each followed audience has actually loaded its own prefix, including a
circle whose old parts the device consumed without reading. -/
def Ready (R : Rules S) (st : Store S) (seq : Nat → Slot S) (p : Plan S)
    (loaded : S.Aud → Reader S) : Prop :=
  ∀ A ∈ p.follows, (loaded A).pos = p.target ∧ ReaderOk R st seq A (loaded A)

theorem queued_originals_preserved (R : Rules S) (slot : Write S Stamp → Option Nat)
    (admitted : S.Aud → Write S Stamp → Bool) (s : State S Stamp) (p : Plan S)
    (loaded : S.Aud → Reader S) :
    (install R slot admitted s p loaded).authored.queue = s.authored.queue ∧
    (install R slot admitted s p loaded).authored.nextNumber = s.authored.nextNumber := ⟨rfl, rfl⟩

/-- Retrying installation replaces the same published state and rebuilds from
the same originals; it cannot apply the queue over its previous local effects. -/
theorem install_twice (R : Rules S) (slot : Write S Stamp → Option Nat)
    (admitted : S.Aud → Write S Stamp → Bool) (s : State S Stamp) (p : Plan S)
    (loaded : S.Aud → Reader S) :
    install R slot admitted (install R slot admitted s p loaded) p loaded =
      install R slot admitted s p loaded := by
  simp only [install, Causality.catchUp]
  congr 1
  funext A
  split <;> rfl

/-- Each eligible original is an actual input to the rebuilding fold, exactly
once by its original write number. No eligibility decision deletes an upload. -/
theorem queued_exactly_once (slot : Write S Stamp → Option Nat)
    (admitted : S.Aud → Write S Stamp → Bool) (A : S.Aud) (loaded : Reader S)
    (queue : List (Write S Stamp)) (unique : (queue.map (·.number)).Nodup)
    (w : Write S Stamp) (queued : w ∈ queue) (counts : eligible slot admitted A loaded w = true) :
    w ∈ reapplied slot admitted A loaded queue ∧
    ((reapplied slot admitted A loaded queue).map (·.number)).count w.number = 1 := by
  have member : w ∈ reapplied slot admitted A loaded queue := List.mem_filter.mpr ⟨queued, counts⟩
  have nodup : ((reapplied slot admitted A loaded queue).map (·.number)).Nodup :=
    unique.sublist (List.filter_sublist.map (fun w : Write S Stamp => w.number))
  exact ⟨member, nodup.count_of_mem (List.mem_map.mpr ⟨w, member, rfl⟩)⟩

theorem no_queued_duplicate (slot : Write S Stamp → Option Nat)
    (admitted : S.Aud → Write S Stamp → Bool) (A : S.Aud) (loaded : Reader S)
    (queue : List (Write S Stamp)) (unique : (queue.map (·.number)).Nodup) (n : Nat) :
    ((reapplied slot admitted A loaded queue).map (·.number)).count n ≤ 1 :=
  (List.nodup_iff_count.mp (unique.sublist (List.filter_sublist.map (fun w : Write S Stamp => w.number)))) n

theorem covered_not_reapplied (slot : Write S Stamp → Option Nat)
    (admitted : S.Aud → Write S Stamp → Bool) (A : S.Aud) (loaded : Reader S)
    (queue : List (Write S Stamp)) (w : Write S Stamp) (covered : consumed slot loaded.pos w = true) :
    w ∉ reapplied slot admitted A loaded queue := by
  simp [reapplied, eligible, covered]

theorem new_circle_keeps_queue (slot : Write S Stamp → Option Nat)
    (admitted : S.Aud → Write S Stamp → Bool) (A : S.Aud) (loaded : Reader S)
    (queue : List (Write S Stamp)) (w : Write S Stamp) (queued : w ∈ queue)
    (unclaimed : slot w = none) (counts : admitted A w = true)
    (afterReset : loaded.aud.lastReset ≤ w.readThrough) (part : (w.bytes A).isSome = true) :
    w ∈ reapplied slot admitted A loaded queue := by
  simp [reapplied, eligible, consumed, queued, unclaimed, counts, afterReset, part]

/-- A successful installation uses sound published state in every newly
readable audience and applies each audience's eligible originals over it. -/
theorem reload_sound (R : Rules S) (slot : Write S Stamp → Option Nat)
    (admitted : S.Aud → Write S Stamp → Bool) (s : State S Stamp) (p : Plan S)
    (loaded : S.Aud → Reader S) {st : Store S} {seq : Nat → Slot S}
    (ready : Ready R st seq p loaded) :
    (install R slot admitted s p loaded).authored.applied = p.target ∧
    ∀ A ∈ p.follows, (loaded A).rows = rowsAt R seq A p.target ∧
      (install R slot admitted s p loaded).rows A = rebuild R slot admitted A (loaded A) s.authored.queue := by
  refine ⟨rfl, fun A ha => ?_⟩
  have h := ready A ha
  exact ⟨by rw [h.2.rows, h.1], by simp [install, ha]⟩

/-- All followed audiences, including newly readable ones, independently
load the floor and run to one fixed target. Later slots do not extend it. -/
theorem audiences_can_load (R : Rules S) {st : Store S} {seq : Nat → Slot S}
    (good : Good R st seq) (p : Plan S) (target : p.target ≤ st.head)
    (coverage : ∀ A ∈ p.follows, st.floor.covers A ≤ p.target) :
    ∃ loaded, Ready R st seq p loaded := by
  classical
  let initial := fun A => Reader.mk (st.floor.covers A) (ledgerAt R seq (st.floor.covers A))
    (audAt R seq A (st.floor.covers A)) (rowsAt R seq A (st.floor.covers A)) (fun _ => none)
  have sound : ∀ A, ReaderOk R st seq A (initial A) := fun A =>
    ⟨rfl, rfl, rfl, good.covers_le A, fun _ _ h => by simp [initial] at h⟩
  have each := fun A (ha : A ∈ p.follows) =>
    reaches_target R good A p.target target (coverage A ha) (initial A) (sound A) (coverage A ha)
  let loaded := fun A => if ha : A ∈ p.follows then Classical.choose (each A ha) else initial A
  refine ⟨loaded, fun A ha => ?_⟩
  simp only [loaded, dite_eq_left ha]
  exact (Classical.choose_spec (each A ha)).2

theorem ordinary_allows_writes : writable (some .ordinary) = true := rfl
theorem required_reload_blocks : writable (some .reset) = false ∧
    writable (some .breakingRaise) = false ∧ writable (some .newlyReadable) = false := ⟨rfl, rfl, rfl⟩

theorem affected_commit_refused (s : State S Stamp) (p : Plan S) (bytes : S.Aud → Option S.Batch)
    (A : S.Aud) (part : (bytes A).isSome = true) (blocked : writable (p.reason A) = false) :
    ¬ canCommit s p bytes := by
  intro allowed
  have := (allowed A part).2
  rw [blocked] at this
  cases this

theorem ordinary_commit_allowed (s : State S Stamp) (p : Plan S) (bytes : S.Aud → Option S.Batch)
    (members : ∀ A, (bytes A).isSome = true → A ∈ s.follows)
    (ordinary : ∀ A, (bytes A).isSome = true → p.reason A = none ∨ p.reason A = some .ordinary) :
    canCommit s p bytes := by
  intro A ha
  refine ⟨members A ha, ?_⟩
  rcases ordinary A ha with h | h <;> simp [h, writable]

/-- During a reload only admitted app commits append originals. Staging IO
does not change this state; failed reloads have no installation transition. -/
inductive During (p : Plan S) : State S Stamp → State S Stamp → Prop
  | app (s : State S Stamp) (d : S.Dev) (t : Stamp) (schema : Nat) (bytes : S.Aud → Option S.Batch)
      (allowed : canCommit s p bytes) (rows : S.Aud → S.Rows) :
      During p s { s with authored := Causality.commit s.authored d t schema bytes, rows }

theorem during_preserves_queue {p : Plan S} {s s' : State S Stamp} (step : During p s s') :
    s.authored.queue <+: s'.authored.queue ∧ s'.authored.applied = s.authored.applied := by
  cases step
  exact ⟨List.prefix_append _ _, rfl⟩

theorem during_numbered {p : Plan S} {s s' : State S Stamp} (step : During p s s')
    (numbered : Numbered s.authored) : Numbered s'.authored := by
  cases step
  exact commit_numbered _ numbered _ _ _ _

/-- A local commit during loading uses the still-committed applied slot. The
later install sees the appended original; it cannot use an earlier queue copy. -/
theorem write_during_reload (R : Rules S) (slot : Write S Stamp → Option Nat)
    (admitted : S.Aud → Write S Stamp → Bool) (s : State S Stamp) (p : Plan S)
    (loaded : S.Aud → Reader S) (d : S.Dev) (t : Stamp) (schema : Nat) (bytes : S.Aud → Option S.Batch) :
    let committed := { s with authored := Causality.commit s.authored d t schema bytes }
    (install R slot admitted committed p loaded).authored.queue =
      s.authored.queue ++ [Causality.author s.authored d t schema bytes] ∧
    (Causality.author s.authored d t schema bytes).readThrough = s.authored.applied := ⟨rfl, rfl⟩

end CovenSequence.QueueReload
