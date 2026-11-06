import Std

/-! §17.9's upload sessions: one queued upload going up in parts through a
provider session that its queue row records, with the upload worker's runs,
crashes, an expiring provider, the retire of an upload no file waits for, and
the retry of each failure by its class.

A part is its index. The provider's session holds parts in the order they
arrive, and publishing hands them over whole. Beginning a session and
recording it are one step here: a session the provider began whose record
never committed, because its reply was lost or the device stopped first, is
outside this model. -/
namespace CovenStorage.UploadSession

/-- api.md E5: how a storage failure clears, which says when an upload retries
it (§17.9). -/
inductive Class where
  | automatic
  | appAction
  | never
  deriving DecidableEq, Repr

/-- What automatic work waits for before it tries the upload again: nothing,
the persisted retry delay (§6.7), new credentials or a new connection, or
nothing ever. -/
inductive Wait where
  | ready
  | backoff
  | connection
  | forever
  deriving DecidableEq, Repr

/-- The table of §17.9: `Automatic` retries under backoff, `AppAction` when
credentials or the connection change, `Never` not at all. -/
def Class.wait : Class → Wait
  | .automatic => .backoff
  | .appAction => .connection
  | .never => .forever

/-- Whether a run may try the upload: automatic work once its wait is over,
the app's early retry (`forced`) whenever it can ever retry. -/
def Wait.due (forced : Bool) : Wait → Bool
  | .ready => true
  | .forever => false
  | .backoff | .connection => forced

/-- What the staging write fixed and the queue keeps: the location, its key
and the chunk hashes (§17.9). -/
structure Fixed where
  location : Nat
  key : Nat
  chunks : List Nat
  deriving DecidableEq, Repr

/-- The upload's queue row: whether a file still waits for it, the confirmed
parts of the session it records, if any, its pending failure and its wait. -/
structure Row where
  waited : Bool
  recorded : Option Nat
  failure : Option Class
  wait : Wait
  deriving DecidableEq, Repr

/-- The attempt this process is running, in memory only: none, one started
that holds no session yet, one holding a session with this many confirmed
parts, or one whose bytes the provider published. -/
inductive Attempt where
  | idle
  | started
  | holding (confirmed : Nat)
  | finished
  deriving DecidableEq, Repr

/-- The provider's live session, the parts it holds, and the published
object's parts; the queue row, gone once the upload completes or retires;
whether completion switched the files to the stored location; and the
attempt. -/
structure State where
  total : Nat
  fixed : Fixed
  session : Option (List Nat)
  published : Option (List Nat)
  row : Option Row
  stored : Bool
  attempt : Attempt
  deriving DecidableEq, Repr

/-- A file staged for the cloud: its upload is queued, with no session. -/
def initial (total : Nat) (fixed : Fixed) : State where
  total := total
  fixed := fixed
  session := none
  published := none
  row := some ⟨true, none, none, .ready⟩
  stored := false
  attempt := .idle

/-- The provider's answer to one part: it holds the part and says so, holds
it but the reply is lost, or never holds it. -/
inductive Reply where
  | confirmed
  | lost
  | dropped
  deriving DecidableEq, Repr

inductive Step where
  /-- A run starts an attempt, if the upload is due. -/
  | start (forced : Bool)
  /-- The attempt's first request: resume the recorded session, start over
  with a new one if the provider expired it, or begin and record one. -/
  | open_
  /-- Record the session the attempt holds, with its confirmed parts. -/
  | record
  /-- Send the next part, once the attempt has recorded what it holds. -/
  | part (reply : Reply)
  /-- Ask the provider to publish the whole object; the reply may be lost. -/
  | finish (replied : Bool)
  /-- The completion transaction switches the files and removes the row. -/
  | complete
  /-- The attempt's request fails with a failure of this class. -/
  | fail (c : Class)
  /-- The process stops, and the store opens again: memory goes, and a
  reopened store connects anew. -/
  | crash
  /-- The provider expires its session. -/
  | expire
  /-- The last file waiting for the upload goes (§17.3). -/
  | abandon
  /-- Abort the recorded session of an upload no file waits for, then forget
  it; `some c` fails with class `c`. -/
  | abort (result : Option Class)
  /-- Retire an upload no file waits for and with no session recorded. -/
  | retire
  /-- The retry delay passes. -/
  | elapse
  /-- Credentials or the connection change. -/
  | connect
  deriving DecidableEq, Repr

/-- A failed request ends the attempt and becomes the upload's failure, with
the wait its class gives it. -/
def failWith (s : State) (c : Class) : State :=
  { s with attempt := .idle,
           row := s.row.map fun r => { r with failure := some c, wait := c.wait } }

/-- A wait the step ends: `w` turns ready, any other stays. -/
def Row.ending (r : Row) (w : Wait) : Row :=
  if r.wait = w then { r with wait := .ready } else r

def Step.apply (s : State) : Step → State
  | .start forced =>
    match s.row, s.attempt with
    | some r, .idle =>
      if r.waited = true ∧ r.wait.due forced = true then { s with attempt := .started } else s
    | _, _ => s
  | .open_ =>
    match s.row, s.attempt with
    | some r, .started =>
      match r.recorded, s.session, s.published with
      | some _, some held, _ => { s with attempt := .holding held.length }
      | some _, none, some _ => { s with attempt := .holding s.total }
      | _, _, _ =>
        { s with session := some [], row := some { r with recorded := some 0 },
                 attempt := .holding 0 }
    | _, _ => s
  | .record =>
    match s.row, s.attempt with
    | some r, .holding n => { s with row := some { r with recorded := some n } }
    | _, _ => s
  | .part reply =>
    match s.row, s.attempt, s.session with
    | some r, .holding n, some held =>
      if n < s.total ∧ r.recorded = some n then
        match reply with
        | .confirmed => { s with session := some (held ++ [n]), attempt := .holding (n + 1) }
        | .lost => failWith { s with session := some (held ++ [n]) } .automatic
        | .dropped => failWith s .automatic
      else s
    | some r, .holding n, none =>
      if n < s.total ∧ r.recorded = some n then failWith s .automatic else s
    | _, _, _ => s
  | .finish replied =>
    match s.row, s.attempt with
    | some r, .holding n =>
      if n = s.total ∧ r.recorded = some n then
        match s.session, s.published with
        | some held, _ =>
          if replied then { s with session := none, published := some held, attempt := .finished }
          else failWith { s with session := none, published := some held } .automatic
        | none, some _ => { s with attempt := .finished }
        | none, none => failWith s .automatic
      else s
    | _, _ => s
  | .complete =>
    match s.row, s.attempt with
    | some r, .finished => { s with row := none, stored := s.stored || r.waited, attempt := .idle }
    | _, _ => s
  | .fail c =>
    match s.attempt with
    | .started | .holding _ => failWith s c
    | _ => s
  | .crash => { s with attempt := .idle, row := s.row.map (·.ending .connection) }
  | .expire => { s with session := none }
  | .abandon => { s with row := s.row.map fun r => { r with waited := false } }
  | .abort result =>
    match s.row, s.attempt with
    | some r, .idle =>
      if r.waited = false ∧ r.recorded.isSome = true ∧ r.wait = .ready then
        match result with
        | none => { s with session := none, row := some { r with recorded := none } }
        | some c => failWith s c
      else s
    | _, _ => s
  | .retire =>
    match s.row, s.attempt with
    | some r, .idle => if r.waited = false ∧ r.recorded = none then { s with row := none } else s
    | _, _ => s
  | .elapse => { s with row := s.row.map (·.ending .backoff) }
  | .connect => { s with row := s.row.map (·.ending .connection) }

def runSteps (s : State) (steps : List Step) : State := steps.foldl Step.apply s

/-! ## The invariant -/

/-- What holds in every reachable state: the provider's session holds parts
`0 … n-1` in order, never more than the file has; the published object is
every part in order; an attempt holding a session holds exactly what the
provider confirmed; the recorded parts are ones the provider holds; a live
provider session is one the row records; and a finished attempt holds none. -/
structure Good (s : State) : Prop where
  in_order : ∀ held, s.session = some held → held = List.range held.length
  bounded : ∀ held, s.session = some held → held.length ≤ s.total
  whole : ∀ ps, s.published = some ps → ps = List.range s.total
  holding : ∀ n held, s.attempt = .holding n → s.session = some held → held.length = n
  recorded : ∀ r n held, s.row = some r → r.recorded = some n → s.session = some held →
    n ≤ held.length
  accounted : ∀ held, s.session = some held → ∃ r, s.row = some r ∧ r.recorded.isSome
  finished : s.attempt = .finished → s.session = none

theorem initial_good (total : Nat) (fixed : Fixed) : Good (initial total fixed) := by
  constructor <;> simp [initial]

theorem range_snoc (n : Nat) : List.range n ++ [n] = List.range (n + 1) :=
  (List.range_succ).symm

theorem failWith_session (s : State) (c : Class) : (failWith s c).session = s.session := rfl
theorem failWith_published (s : State) (c : Class) : (failWith s c).published = s.published := rfl
theorem failWith_attempt (s : State) (c : Class) : (failWith s c).attempt = .idle := rfl

theorem failWith_row (s : State) (c : Class) (r : Row) :
    (failWith s c).row = some r ↔
      ∃ r₀, s.row = some r₀ ∧ r = { r₀ with failure := some c, wait := c.wait } := by
  cases h : s.row <;> simp [failWith, h, eq_comm]

/-- A failure keeps everything but the attempt, the failure and the wait, so
the invariant needs only what it says of the provider and the row. -/
theorem failWith_good_of (s : State) (c : Class)
    (o : ∀ held, s.session = some held → held = List.range held.length)
    (b : ∀ held, s.session = some held → held.length ≤ s.total)
    (w : ∀ ps, s.published = some ps → ps = List.range s.total)
    (rc : ∀ r n held, s.row = some r → r.recorded = some n → s.session = some held →
      n ≤ held.length)
    (a : ∀ held, s.session = some held → ∃ r, s.row = some r ∧ r.recorded.isSome) :
    Good (failWith s c) := by
  refine ⟨o, b, w, ?_, ?_, ?_, ?_⟩
  · intro n held h; simp [failWith] at h
  · intro r n held hr hn hs
    obtain ⟨r₀, h₀, rfl⟩ := (failWith_row s c r).1 hr
    exact rc r₀ n held h₀ hn hs
  · intro held hs
    obtain ⟨r, hr, hrec⟩ := a held hs
    exact ⟨_, (failWith_row s c _).2 ⟨r, hr, rfl⟩, hrec⟩
  · intro h; simp [failWith] at h

theorem failWith_good (s : State) (c : Class) (g : Good s) : Good (failWith s c) :=
  failWith_good_of s c g.in_order g.bounded g.whole g.recorded g.accounted

theorem ending_recorded (r : Row) (w : Wait) : (r.ending w).recorded = r.recorded := by
  unfold Row.ending; split <;> rfl

theorem ending_waited (r : Row) (w : Wait) : (r.ending w).waited = r.waited := by
  unfold Row.ending; split <;> rfl

theorem ending_failure (r : Row) (w : Wait) : (r.ending w).failure = r.failure := by
  unfold Row.ending; split <;> rfl

/-- A step that only changes waits, or whether a file waits, keeps the
invariant: none of it reads them. -/
theorem rows_good (s : State) (f : Row → Row) (g : Good s)
    (keep : ∀ r, (f r).recorded = r.recorded) (attempt : Attempt)
    (idle : attempt = s.attempt ∨ attempt = .idle) :
    Good { s with row := s.row.map f, attempt := attempt } := by
  obtain ⟨o, b, w, hh, rc, a, fin⟩ := g
  refine ⟨o, b, w, ?_, ?_, ?_, ?_⟩
  · intro n held ha hs
    rcases idle with rfl | rfl
    · exact hh n held ha hs
    · cases ha
  · intro r n held hr hn hs
    cases h : s.row with
    | none => simp [h] at hr
    | some r₀ =>
      simp [h] at hr
      subst hr
      rw [keep] at hn
      exact rc r₀ n held h hn hs
  · intro held hs
    obtain ⟨r, hr, hrec⟩ := a held hs
    exact ⟨f r, by simp [hr], by rw [keep]; exact hrec⟩
  · intro ha
    rcases idle with rfl | rfl
    · exact fin ha
    · cases ha

theorem apply_good (s : State) (st : Step) (g : Good s) : Good (st.apply s) := by
  obtain ⟨o, b, w, hh, rc, a, fin⟩ := g
  have g : Good s := ⟨o, b, w, hh, rc, a, fin⟩
  cases st with
  | start forced =>
    simp only [Step.apply]
    split
    · split
      · exact ⟨o, b, w, by simp, rc, a, by simp⟩
      · exact g
    · exact g
  | open_ =>
    simp only [Step.apply]
    split
    · rename_i r hr hat
      split
      · rename_i held hs
        refine ⟨o, b, w, ?_, rc, a, by simp⟩
        intro n held' ha hs'
        simp at ha; rw [hs] at hs'; cases hs'; exact ha
      · rename_i hs _
        refine ⟨o, b, w, ?_, rc, a, by simp⟩
        intro n held' _ hs'; rw [hs] at hs'; cases hs'
      · refine ⟨?_, ?_, w, ?_, ?_, ?_, by simp⟩
        · intro held h; simp at h; subst h; rfl
        · intro held h; simp at h; subst h; simp
        · intro n held ha h; simp at ha h; subst h; simp [ha]
        · intro r' n held hr' hn h; simp at hr' h; subst hr' h; simp at hn; omega
        · intro held _; exact ⟨_, rfl, by simp⟩
    · exact g
  | record =>
    simp only [Step.apply]
    split
    · rename_i r n hr hat
      refine ⟨o, b, w, hh, ?_, ?_, fin⟩
      · intro r' m held hr' hm hs
        simp at hr'; subst hr'; simp at hm; subst hm
        exact Nat.le_of_eq (hh n held hat hs).symm
      · intro held hs; exact ⟨_, rfl, by simp⟩
    · exact g
  | part reply =>
    simp only [Step.apply]
    split
    · rename_i r n held hr hat hs
      split
      · rename_i guard
        obtain ⟨lt, hrec⟩ := guard
        have len : held.length = n := hh n held hat hs
        have ord := o held hs
        cases reply with
        | confirmed =>
          refine ⟨?_, ?_, w, ?_, ?_, ?_, by simp⟩
          · intro held' h; simp at h; subst h
            rw [ord, ← len, List.length_append, List.length_range, range_snoc]; simp
          · intro held' h; simp at h; subst h; simp; omega
          · intro m held' ha h; simp at ha h; subst h; simp; omega
          · intro r' m held' hr' hm h; simp at h; subst h
            rw [hr] at hr'; cases hr'; rw [hrec] at hm; cases hm; simp; omega
          · intro held' _; exact ⟨r, hr, by simp [hrec]⟩
        | lost =>
          apply failWith_good_of
          · intro held' h; simp at h; subst h
            rw [ord, ← len, List.length_append, List.length_range, range_snoc]; simp
          · intro held' h; simp at h; subst h; simp; omega
          · exact w
          · intro r' m held' hr' hm h; simp at h; subst h
            rw [hr] at hr'; cases hr'; rw [hrec] at hm; cases hm; simp; omega
          · intro held' _; exact ⟨r, hr, by simp [hrec]⟩
        | dropped => exact failWith_good s _ g
      · exact g
    · rename_i r n hr hat hs
      split
      · exact failWith_good s _ g
      · exact g
    · exact g
  | finish replied =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | holding n =>
        simp only [Step.apply, hr, hat]
        split
        · rename_i guard
          obtain ⟨eq, hrec⟩ := guard
          cases hs : s.session with
          | some held =>
            have len : held.length = n := hh n held hat hs
            have whole : held = List.range s.total := by rw [o held hs, len, eq]
            simp only
            split
            · refine ⟨by simp, by simp, ?_, by simp, by simp, by simp, by simp⟩
              intro ps h; simp at h; subst h; exact whole
            · apply failWith_good_of <;> try simp
              exact whole
          | none =>
            cases hp : s.published with
            | some ps =>
              simp only
              refine ⟨by simp, by simp, ?_, by simp, by simp, by simp, by simp⟩
              intro ps' h; simp at h; subst h; exact w ps hp
            | none =>
              simp only
              exact failWith_good s _ g
        · exact g
      | _ => simpa [Step.apply, hr, hat] using g
  | complete =>
    simp only [Step.apply]
    split
    · rename_i r hr hat
      have none := fin hat
      exact ⟨by simp [none], by simp [none], w, by simp, by simp, by simp [none], by simp⟩
    · exact g
  | fail c =>
    simp only [Step.apply]
    split
    · exact failWith_good s c g
    · exact failWith_good s c g
    · exact g
  | crash =>
    simp only [Step.apply]
    exact rows_good s _ g (fun r => ending_recorded r _) .idle (Or.inr rfl)
  | expire =>
    simp only [Step.apply]
    exact ⟨by simp, by simp, w, by simp, by simp, by simp, by simp⟩
  | abandon =>
    simp only [Step.apply]
    exact rows_good s (fun r => { r with waited := false }) g (fun _ => rfl) s.attempt
      (Or.inl rfl)
  | abort result =>
    simp only [Step.apply]
    split
    · rename_i r hr hat
      split
      · cases result with
        | none =>
          refine ⟨by simp, by simp, w, by simp, by simp, by simp, by simp⟩
        | some c => exact failWith_good s c g
      · exact g
    · exact g
  | retire =>
    simp only [Step.apply]
    split
    · rename_i r hr hat
      split
      · rename_i guard
        have none : s.session = none := by
          cases hs : s.session with
          | none => rfl
          | some held =>
            obtain ⟨r', hr', hrec⟩ := a held hs
            rw [hr] at hr'; cases hr'; simp [guard.2] at hrec
        exact ⟨by simp [none], by simp [none], w, by simp [none], by simp, by simp [none],
          by simp [none]⟩
      · exact g
    · exact g
  | elapse =>
    simp only [Step.apply]
    exact rows_good s _ g (fun r => ending_recorded r _) s.attempt (Or.inl rfl)
  | connect =>
    simp only [Step.apply]
    exact rows_good s _ g (fun r => ending_recorded r _) s.attempt (Or.inl rfl)

theorem steps_good (s : State) (steps : List Step) (g : Good s) : Good (runSteps s steps) := by
  induction steps generalizing s with
  | nil => exact g
  | cons st rest ih => exact ih _ (apply_good s st g)

def Reachable (s : State) : Prop :=
  ∃ total fixed steps, s = runSteps (initial total fixed) steps

theorem reachable_good (s : State) (h : Reachable s) : Good s := by
  obtain ⟨total, fixed, steps, rfl⟩ := h
  exact steps_good _ steps (initial_good total fixed)

/-! ## Progress, resuming and expiry -/

/-- The confirmed progress the app is shown: `uploads()` and `coven_uploads`
read the recorded session's columns (§17.9). -/
def State.progress (s : State) : Option Nat := s.row.bind (·.recorded)

/-- §17.9: progress is never ahead of the parts the provider's session holds:
a part is recorded only once the provider confirmed it. -/
theorem progress_never_ahead (s : State) (g : Good s) (n : Nat) (held : List Nat)
    (shown : s.progress = some n) (live : s.session = some held) : n ≤ held.length := by
  unfold State.progress at shown
  cases hr : s.row with
  | none => simp [hr] at shown
  | some r =>
    simp [hr] at shown
    exact g.recorded r n held hr shown live

/-- The part an attempt sends next is the first the provider lacks, and it
holds every earlier one: no part goes twice and none is skipped, after any
crashes and failed attempts. -/
theorem next_part_new (s : State) (g : Good s) (n : Nat) (held : List Nat)
    (hat : s.attempt = .holding n) (live : s.session = some held) :
    held = List.range n ∧ n ∉ held := by
  have len := g.holding n held hat live
  have ord := g.in_order held live
  rw [len] at ord
  exact ⟨ord, by rw [ord]; simp⟩

/-- The provider's session holds no part twice. -/
theorem held_nodup (s : State) (g : Good s) (held : List Nat) (live : s.session = some held) :
    held.Nodup := by
  rw [g.in_order held live]; exact List.nodup_range

/-- The published object is every part, each once, in order. -/
theorem published_whole (s : State) (g : Good s) (ps : List Nat)
    (h : s.published = some ps) : ps = List.range s.total :=
  g.whole ps h

/-- A crash forgets only memory: the provider's session, the published object
and the recorded parts stay. -/
theorem crash_keeps (s : State) :
    (Step.crash.apply s).session = s.session ∧ (Step.crash.apply s).published = s.published ∧
      (Step.crash.apply s).progress = s.progress := by
  refine ⟨rfl, rfl, ?_⟩
  cases hr : s.row <;> simp [Step.apply, State.progress, hr, ending_recorded]

/-- §17.9: after a crash, a session the provider still has continues from
the parts it holds, which are at least the recorded ones. -/
theorem resume_continues (s : State) (r : Row) (m : Nat) (held : List Nat)
    (hr : s.row = some r) (hat : s.attempt = .started) (hrec : r.recorded = some m)
    (live : s.session = some held) :
    Step.open_.apply s = { s with attempt := .holding held.length } := by
  simp [Step.apply, hr, hat, hrec, live]

/-- §17.9: a session the provider has expired starts over from its first
part, recorded at once; the location, key and chunk hashes stay. -/
theorem expired_starts_over (s : State) (r : Row) (m : Nat)
    (hr : s.row = some r) (hat : s.attempt = .started) (hrec : r.recorded = some m)
    (gone : s.session = none) (unpublished : s.published = none) :
    Step.open_.apply s =
      { s with session := some [], row := some { r with recorded := some 0 },
               attempt := .holding 0 } := by
  simp [Step.apply, hr, hat, hrec, gone, unpublished]

theorem apply_fixed (s : State) (st : Step) :
    (st.apply s).fixed = s.fixed ∧ (st.apply s).total = s.total := by
  cases st <;> simp only [Step.apply] <;> (repeat' split) <;> simp [failWith]

/-- No step changes what the staging write fixed. -/
theorem steps_keep_fixed (s : State) (steps : List Step) :
    (runSteps s steps).fixed = s.fixed ∧ (runSteps s steps).total = s.total := by
  induction steps generalizing s with
  | nil => exact ⟨rfl, rfl⟩
  | cons st rest ih =>
    obtain ⟨f, t⟩ := ih (st.apply s)
    obtain ⟨f', t'⟩ := apply_fixed s st
    exact ⟨f.trans f', t.trans t'⟩

/-! ## Retiring an upload no file waits for -/

/-- §17.9: an upload retires with no session left at the provider. -/
theorem retired_no_session (s : State) (g : Good s) (gone : s.row = none) : s.session = none := by
  cases hs : s.session with
  | none => rfl
  | some held =>
    obtain ⟨r, hr, _⟩ := g.accounted held hs
    rw [gone] at hr; cases hr

/-- Only a retire removes the row of an upload no file waits for, and only
once it records no session. -/
theorem retire_needs_no_session (s : State) (r : Row) (hr : s.row = some r)
    (gone : (Step.retire.apply s).row = none) : r.waited = false ∧ r.recorded = none := by
  obtain ⟨total, fixed, session, published, row, stored, attempt⟩ := s
  simp only at hr; subst hr
  cases attempt <;> simp [Step.apply] at gone
  split at gone
  · assumption
  · simp at gone

/-- §17.9: a recorded session is forgotten only by aborting it, which ends
the provider's session first. -/
theorem forgotten_by_abort (s : State) (st : Step) (r r' : Row) (m : Nat)
    (hr : s.row = some r) (hrec : r.recorded = some m)
    (after : (st.apply s).row = some r') (forgot : r'.recorded = none) :
    st = .abort none ∧ (st.apply s).session = none := by
  obtain ⟨total, fixed, session, published, row, stored, attempt⟩ := s
  simp only at hr; subst hr
  cases st <;> cases attempt <;> cases session <;> cases published <;>
    simp [Step.apply, failWith, Row.ending, hrec] at after ⊢ <;>
    (repeat' split at after) <;> (repeat' split) <;> (try cases ‹Option Class›) <;> (try simp only [Option.some.injEq] at after) <;>
    (try subst after) <;> simp_all

/-- §17.9: a failed abort is the upload's failure: the upload stays queued
with its session recorded, and waits as its class says. -/
theorem failed_abort_queued (s : State) (r : Row) (c : Class) (hr : s.row = some r)
    (idle : s.attempt = .idle) (abandoned : r.waited = false) (held : r.recorded.isSome = true)
    (ready : r.wait = .ready) :
    (Step.abort (some c)).apply s =
      { s with attempt := .idle, row := some { r with failure := some c, wait := c.wait } } := by
  simp [Step.apply, hr, idle, abandoned, held, ready, failWith]

/-! ## Retries by class -/

/-- An attempt starts only when the upload is due: for automatic work, once its
wait is over; for the app's early retry, unless it never retries. -/
theorem start_needs_due (s : State) (forced : Bool) (moved : (Step.start forced).apply s ≠ s) :
    ∃ r, s.row = some r ∧ r.wait.due forced = true := by
  simp only [Step.apply] at moved
  split at moved
  · rename_i r hr _
    split at moved
    · rename_i h; exact ⟨r, hr, h.2⟩
    · exact absurd rfl moved
  · exact absurd rfl moved

/-- An abort runs only once the upload's wait is over: the app's early retry
covers queued files, and no file waits for this upload. -/
theorem abort_needs_ready (s : State) (result : Option Class)
    (moved : (Step.abort result).apply s ≠ s) : ∃ r, s.row = some r ∧ r.wait = .ready := by
  simp only [Step.apply] at moved
  split at moved
  · rename_i r hr _
    split at moved
    · rename_i h; exact ⟨r, hr, h.2.2⟩
    · exact absurd rfl moved
  · exact absurd rfl moved

/-- A failure's wait comes from its class (§17.9's table). -/
theorem failure_waits_by_class (s : State) (c : Class) (r : Row)
    (h : (failWith s c).row = some r) : r.failure = some c ∧ r.wait = c.wait := by
  obtain ⟨r₀, _, rfl⟩ := (failWith_row s c r).1 h
  exact ⟨rfl, rfl⟩

/-- A wait ends only as its class says: backoff when the delay passes, a
connection wait when credentials or the connection change (a reopened store
connects anew), and a `Never` failure's never. -/
theorem wait_ends (s : State) (st : Step) (r r' : Row) (hr : s.row = some r)
    (waiting : r.wait ≠ .ready) (after : (st.apply s).row = some r') (ended : r'.wait = .ready) :
    (r.wait = .backoff ∧ st = .elapse) ∨
      (r.wait = .connection ∧ (st = .connect ∨ st = .crash)) := by
  obtain ⟨total, fixed, session, published, row, stored, attempt⟩ := s
  simp only at hr; subst hr
  cases hw : r.wait <;> cases st <;> cases attempt <;> cases session <;> cases published <;>
    simp [Step.apply, failWith, Row.ending, hw] at after waiting ⊢ <;>
    (repeat' split at after) <;> (try cases ‹Option Class›) <;>
    (try cases ‹Class›) <;> (try simp only [Option.some.injEq] at after) <;>
    (try subst after) <;> simp_all [Class.wait]

/-- §17.9: a `Never` failure is not retried. From an idle upload whose
failure never retries, no step sends anything or changes its failure or its
recorded session; at most its row retires, when it records no session. -/
theorem never_stays (s : State) (st : Step) (r : Row) (hr : s.row = some r)
    (forever : r.wait = .forever) (idle : s.attempt = .idle) :
    (st.apply s).attempt = .idle ∧ (st.apply s).published = s.published ∧
      ((st.apply s).row = none ∨ ∃ r', (st.apply s).row = some r' ∧ r'.wait = .forever ∧
        r'.recorded = r.recorded ∧ r'.failure = r.failure) := by
  obtain ⟨total, fixed, session, published, row, stored, attempt⟩ := s
  simp only at hr idle; subst hr idle
  cases st <;> simp [Step.apply, Row.ending, forever, Wait.due] <;> (repeat' split) <;> simp_all

/-- No step brings back the row of an upload that completed or retired, or
publishes anything for it. -/
theorem gone_stays (s : State) (st : Step) (gone : s.row = none) :
    (st.apply s).row = none ∧ (st.apply s).published = s.published := by
  obtain ⟨total, fixed, session, published, row, stored, attempt⟩ := s
  simp only at gone; subst gone
  cases st <;> cases attempt <;> simp [Step.apply, failWith]

theorem gone_steps (s : State) (steps : List Step) (gone : s.row = none) :
    (runSteps s steps).row = none ∧ (runSteps s steps).published = s.published := by
  induction steps generalizing s with
  | nil => exact ⟨gone, rfl⟩
  | cons st rest ih =>
    obtain ⟨g', p'⟩ := gone_stays s st gone
    obtain ⟨g'', p''⟩ := ih (st.apply s) g'
    exact ⟨g'', p''.trans p'⟩

theorem never_retried (s : State) (steps : List Step) (r : Row) (hr : s.row = some r)
    (forever : r.wait = .forever) (idle : s.attempt = .idle) :
    (runSteps s steps).published = s.published ∧
      ((runSteps s steps).row = none ∨ ∃ r', (runSteps s steps).row = some r' ∧
        r'.wait = .forever ∧ r'.recorded = r.recorded ∧ r'.failure = r.failure) := by
  induction steps generalizing s r with
  | nil => exact ⟨rfl, Or.inr ⟨r, hr, forever, rfl, rfl⟩⟩
  | cons st rest ih =>
    obtain ⟨idle', pub, row⟩ := never_stays s st r hr forever idle
    show (runSteps (st.apply s) rest).published = s.published ∧ _
    rcases row with gone | ⟨r', hr', fv, rec, fl⟩
    · obtain ⟨g, p⟩ := gone_steps (st.apply s) rest gone
      exact ⟨p.trans pub, Or.inl g⟩
    · obtain ⟨p, rest'⟩ := ih (st.apply s) r' hr' fv idle'
      refine ⟨p.trans pub, ?_⟩
      rcases rest' with g | ⟨r'', h'', f'', rc'', fl''⟩
      · exact Or.inl g
      · exact Or.inr ⟨r'', h'', f'', rc''.trans rec, fl''.trans fl⟩

/-! ## The worker's runs

A run of the upload worker (§17.9) first aborts the session of an upload no
file waits for, once due, and retires one that records no session; then it
attempts an upload a file waits for, if due. An attempt resumes or begins its
session, records it, sends each part and records it, publishes, and completes.
What the provider does to the run is its `Fault`. -/

/-- What happens to the part after the run's `k` confirmed ones: its reply is
lost, it is dropped, or the process stops while sending it. -/
inductive PartFault where
  | lost
  | dropped
  | crash
  deriving DecidableEq, Repr

/-- What the provider does to one run: nothing; expire the session before the
run's first request; fail that request with a class; act on the part after
`k` confirmed ones; or lose the completion reply. -/
inductive Fault where
  | none
  | expire
  | fail (c : Class)
  | afterParts (k : Nat) (then_ : PartFault)
  | loseCompletion
  deriving DecidableEq, Repr

/-- `k` parts, each recorded once confirmed. -/
def partSteps : Nat → List Step
  | 0 => []
  | k + 1 => .part .confirmed :: .record :: partSteps k

/-- The steps of an attempt that has just started. -/
def attemptPlan (s : State) (fault : Fault) : List Step :=
  match fault with
  | .fail c => [.fail c]
  | _ =>
    let opened := (if fault = .expire then [Step.expire] else []) ++ [.open_, .record]
    match (runSteps s opened).attempt with
    | .holding n =>
      let remaining := s.total - n
      let finishing := partSteps remaining ++ [.finish (fault ≠ .loseCompletion), .complete]
      match fault with
      | .afterParts k pf =>
        if k < remaining then
          opened ++ partSteps k ++
            (match pf with
             | .lost => [.part .lost]
             | .dropped => [.part .dropped]
             | .crash => [])
        else opened ++ finishing
      | _ => opened ++ finishing
    | _ => opened

/-- The steps of one run; `forced` is the app's early retry. A run that meets
a crash ends there, and the store opens again after it. -/
def plan (s : State) (forced : Bool) (fault : Fault) : List Step :=
  let steps :=
    match s.row, s.attempt with
    | some r, .idle =>
      if r.waited then
        if r.wait.due forced then
          .start forced :: attemptPlan ((Step.start forced).apply s) fault
        else []
      else if r.recorded.isSome ∧ r.wait = .ready then
        (if fault = .expire then [.expire] else []) ++
          [.abort (match fault with | .fail c => some c | _ => none), .retire]
      else [.retire]
    | _, _ => []
  steps ++ (match fault with | .afterParts _ .crash => [.crash] | _ => [])

def run (s : State) (forced : Bool) (fault : Fault) : State := runSteps s (plan s forced fault)

/-- Every run keeps the invariant: it is a sequence of steps. -/
theorem run_good (s : State) (forced : Bool) (fault : Fault) (g : Good s) :
    Good (run s forced fault) :=
  steps_good s _ g

end CovenStorage.UploadSession
