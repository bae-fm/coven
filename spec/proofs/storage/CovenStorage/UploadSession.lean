import Std

/-! §17.9's upload sessions: one queued upload going up in parts through a
provider session that its queue row records, with the upload worker's runs,
crashes, an expiring provider, the retire of an upload no file waits for, and
the retry of each failure by its class.

A part is its index. The provider's session holds parts in the order they
arrive, and publishing hands them over whole. Before the provider begins a
session, the row records a plan that names where the session will be. A
session the provider began whose name the row never recorded, because the
reply was lost or the process stopped first, is a stray there, and the
upload's next attempt, or the abort of an upload no file waits for, ends every
stray at that place before anything else. -/
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

/-- What the queue row records of its provider session: nothing; a plan,
which names where the next session will be before the provider begins it; or
the session itself, with its confirmed parts. -/
inductive Record where
  | none
  | plan
  | session (confirmed : Nat)
  deriving DecidableEq, Repr

@[simp] def Record.isSession : Record → Bool
  | .session _ => true
  | _ => false

/-- Whether the part after `n` confirmed ones goes to a session the row can
find again: the one it names, holding those parts, or, for the first part, the
one at its plan's place. -/
@[simp] def Record.finds : Record → Nat → Bool
  | .session m, n => m == n
  | .plan, n => n == 0
  | .none, _ => false

/-- The confirmed parts a record shows: none yet for a plan. -/
@[simp] def Record.confirmed : Record → Option Nat
  | .session n => some n
  | .plan => some 0
  | .none => Option.none

/-- The upload's queue row: whether a file still waits for it, what it records
of its session, its pending failure and its wait. -/
structure Row where
  waited : Bool
  recorded : Record
  failure : Option Class
  wait : Wait
  deriving DecidableEq, Repr

/-- The attempt this process is running, in memory only: none, one started
that holds no session yet, one whose row records the plan of the session it
is about to begin, one holding a session with this many confirmed parts, or
one whose bytes the provider published. -/
inductive Attempt where
  | idle
  | started
  | planned
  | holding (confirmed : Nat)
  | finished
  deriving DecidableEq, Repr

/-- Whether the attempt's first request is behind it. -/
@[simp] def Attempt.opened : Attempt → Bool
  | .planned | .holding _ | .finished => true
  | .idle | .started => false

/-- The provider's live session, the parts it holds, and the sessions it began
at the plan's place that the row never named; the published object's parts;
the queue row, gone once the upload completes or retires; whether completion
switched the files to the stored location; and the attempt. -/
structure State where
  total : Nat
  fixed : Fixed
  session : Option (List Nat)
  stray : Nat
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
  stray := 0
  published := none
  row := some ⟨true, .none, none, .ready⟩
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
  /-- The attempt's first request: resume the session the row records; or,
  with none to resume (none recorded, an expired one, or only a plan), end
  every session at the plan's place and record the plan of a new one. -/
  | open_
  /-- The provider begins the planned session; the reply may be lost, and
  the session it began is then a stray. -/
  | begin (replied : Bool)
  /-- Record the session the attempt holds, with its confirmed parts. -/
  | record
  /-- Send the next part, to a session the row can find again
  (`Record.finds`). -/
  | part (reply : Reply)
  /-- Ask the provider to publish the whole object; the reply may be lost. -/
  | finish (replied : Bool)
  /-- The completion transaction switches the files and removes the row. -/
  | complete
  /-- The attempt's request fails with a failure of this class, recording
  the session the attempt holds, if any. -/
  | fail (c : Class)
  /-- The process stops, and the store opens again: memory goes, a session
  the attempt began that the row doesn't name yet is a stray, and a reopened
  store connects anew. -/
  | crash
  /-- The provider expires its sessions, strays included. -/
  | expire
  /-- The last file waiting for the upload goes (§17.3). -/
  | abandon
  /-- End what an upload no file waits for left at the provider, its recorded
  session or every session at its plan's place, then forget it; `some c`
  fails with class `c`. -/
  | abort (result : Option Class)
  /-- Retire an upload no file waits for and with nothing recorded. -/
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

/-- With no session to resume: end every session at the plan's place and
record the plan of the next. -/
def replan (s : State) (r : Row) : State :=
  { s with stray := 0, row := some { r with recorded := .plan }, attempt := .planned }

/-- What a stopped attempt leaves at the provider: a session it began that its
row doesn't name yet is a stray at the plan's place. -/
def strand (s : State) : State :=
  match s.row, s.session with
  | some r, some _ =>
    if r.recorded = .plan then { s with session := none, stray := s.stray + 1 } else s
  | _, _ => s

def Step.apply (s : State) : Step → State
  | .start forced =>
    match s.row, s.attempt with
    | some r, .idle =>
      if r.waited = true ∧ r.wait.due forced = true then { s with attempt := .started } else s
    | _, _ => s
  | .open_ =>
    match s.row, s.attempt with
    | some r, .started =>
      match r.recorded with
      | .session _ =>
        match s.session, s.published with
        | some held, _ => { s with attempt := .holding held.length }
        | none, some _ => { s with attempt := .holding s.total }
        | none, none => replan s r
      | _ => replan s r
    | _, _ => s
  | .begin replied =>
    match s.row, s.attempt with
    | some _, .planned =>
      if replied then { s with session := some [], attempt := .holding 0 }
      else failWith { s with stray := s.stray + 1 } .automatic
    | _, _ => s
  | .record =>
    match s.row, s.attempt with
    | some r, .holding n => { s with row := some { r with recorded := .session n } }
    | _, _ => s
  | .part reply =>
    match s.row, s.attempt, s.session with
    | some r, .holding n, some held =>
      if n < s.total ∧ r.recorded.finds n = true then
        match reply with
        | .confirmed => { s with session := some (held ++ [n]), attempt := .holding (n + 1) }
        | .lost =>
          failWith { s with session := some (held ++ [n]),
                            row := some { r with recorded := .session n } } .automatic
        | .dropped => failWith { s with row := some { r with recorded := .session n } } .automatic
      else s
    | some r, .holding n, none =>
      if n < s.total ∧ r.recorded.finds n = true then
        failWith { s with row := some { r with recorded := .session n } } .automatic
      else s
    | _, _, _ => s
  | .finish replied =>
    match s.row, s.attempt with
    | some r, .holding n =>
      if n = s.total ∧ r.recorded = .session n then
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
    | .started | .planned => failWith s c
    | .holding n => failWith { s with row := s.row.map fun r => { r with recorded := .session n } } c
    | _ => s
  | .crash => { strand s with attempt := .idle, row := s.row.map (·.ending .connection) }
  | .expire => { s with session := none, stray := 0 }
  | .abandon => { s with row := s.row.map fun r => { r with waited := false } }
  | .abort result =>
    match s.row, s.attempt with
    | some r, .idle =>
      if r.waited = false ∧ r.recorded ≠ Record.none ∧ r.wait = .ready then
        match result with
        | none => { s with session := none, stray := 0, row := some { r with recorded := .none } }
        | some c => failWith s c
      else s
    | _, _ => s
  | .retire =>
    match s.row, s.attempt with
    | some r, .idle =>
      if r.waited = false ∧ r.recorded = Record.none then { s with row := none } else s
    | _, _ => s
  | .elapse => { s with row := s.row.map (·.ending .backoff) }
  | .connect => { s with row := s.row.map (·.ending .connection) }

def runSteps (s : State) (steps : List Step) : State := steps.foldl Step.apply s

/-! ## The invariant -/

/-- What holds in every reachable state: the provider's session holds parts
`0 … n-1` in order, never more than the file has; the published object is
every part in order; an attempt holding a session holds exactly what the
provider confirmed; the recorded parts are ones the provider holds; a live
provider session is one the row names, or one the attempt holds that the row
names by its plan's place; every stray sits at the place of the plan the row
records; a planned attempt holds no session yet; no stray outlasts an
attempt's first request; and a finished attempt holds no session. -/
structure Good (s : State) : Prop where
  in_order : ∀ held, s.session = some held → held = List.range held.length
  bounded : ∀ held, s.session = some held → held.length ≤ s.total
  whole : ∀ ps, s.published = some ps → ps = List.range s.total
  holding : ∀ n held, s.attempt = .holding n → s.session = some held → held.length = n
  recorded : ∀ r n held, s.row = some r → r.recorded = .session n → s.session = some held →
    n ≤ held.length
  accounted : ∀ held, s.session = some held → ∃ r, s.row = some r ∧
    (r.recorded.isSession = true ∨ (r.recorded = .plan ∧ ∃ n, s.attempt = .holding n))
  named : 0 < s.stray → ∃ r, s.row = some r ∧ r.recorded = .plan
  planned : s.attempt = .planned → s.session = none ∧ ∃ r, s.row = some r ∧ r.recorded = .plan
  swept : s.attempt.opened = true → s.stray = 0
  finished : s.attempt = .finished → s.session = none

theorem initial_good (total : Nat) (fixed : Fixed) : Good (initial total fixed) := by
  constructor <;> simp [initial]

theorem range_snoc (n : Nat) : List.range n ++ [n] = List.range (n + 1) :=
  (List.range_succ).symm

theorem failWith_session (s : State) (c : Class) : (failWith s c).session = s.session := rfl
theorem failWith_published (s : State) (c : Class) : (failWith s c).published = s.published := rfl
theorem failWith_attempt (s : State) (c : Class) : (failWith s c).attempt = .idle := rfl
theorem failWith_stray (s : State) (c : Class) : (failWith s c).stray = s.stray := rfl

theorem failWith_row (s : State) (c : Class) (r : Row) :
    (failWith s c).row = some r ↔
      ∃ r₀, s.row = some r₀ ∧ r = { r₀ with failure := some c, wait := c.wait } := by
  cases h : s.row <;> simp [failWith, h, eq_comm]

/-- A failure keeps everything but the attempt, the failure and the wait, so
the invariant needs only what it says of the provider and the row, with every
live session one the row names: the attempt that held one is gone. -/
theorem failWith_good_of (s : State) (c : Class)
    (o : ∀ held, s.session = some held → held = List.range held.length)
    (b : ∀ held, s.session = some held → held.length ≤ s.total)
    (w : ∀ ps, s.published = some ps → ps = List.range s.total)
    (rc : ∀ r n held, s.row = some r → r.recorded = .session n → s.session = some held →
      n ≤ held.length)
    (a : ∀ held, s.session = some held → ∃ r, s.row = some r ∧ r.recorded.isSession = true)
    (nm : 0 < s.stray → ∃ r, s.row = some r ∧ r.recorded = .plan) :
    Good (failWith s c) := by
  refine ⟨o, b, w, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro n held h; simp [failWith] at h
  · intro r n held hr hn hs
    obtain ⟨r₀, h₀, rfl⟩ := (failWith_row s c r).1 hr
    exact rc r₀ n held h₀ hn hs
  · intro held hs
    obtain ⟨r, hr, hrec⟩ := a held hs
    exact ⟨_, (failWith_row s c _).2 ⟨r, hr, rfl⟩, Or.inl hrec⟩
  · intro h
    obtain ⟨r, hr, hp⟩ := nm h
    exact ⟨_, (failWith_row s c _).2 ⟨r, hr, rfl⟩, hp⟩
  · intro h; simp [failWith] at h
  · intro h; simp [failWith] at h
  · intro h; simp [failWith] at h

/-- With no attempt holding a session, every live session is one the row
names. -/
theorem named_session (s : State) (g : Good s) (free : ∀ n, s.attempt ≠ .holding n) :
    ∀ held, s.session = some held → ∃ r, s.row = some r ∧ r.recorded.isSession = true := by
  intro held hs
  obtain ⟨r, hr, h⟩ := g.accounted held hs
  rcases h with h | ⟨_, n, hn⟩
  · exact ⟨r, hr, h⟩
  · exact absurd hn (free n)

theorem failWith_good (s : State) (c : Class) (g : Good s)
    (a : ∀ held, s.session = some held → ∃ r, s.row = some r ∧ r.recorded.isSession = true) :
    Good (failWith s c) :=
  failWith_good_of s c g.in_order g.bounded g.whole g.recorded a g.named

@[simp] theorem ending_recorded (r : Row) (w : Wait) : (r.ending w).recorded = r.recorded := by
  unfold Row.ending; split <;> rfl

theorem ending_waited (r : Row) (w : Wait) : (r.ending w).waited = r.waited := by
  unfold Row.ending; split <;> rfl

theorem ending_failure (r : Row) (w : Wait) : (r.ending w).failure = r.failure := by
  unfold Row.ending; split <;> rfl

/-- A step that only changes waits, or whether a file waits, keeps the
invariant: none of it reads them. -/
theorem rows_good (s : State) (f : Row → Row) (g : Good s)
    (keep : ∀ r, (f r).recorded = r.recorded) : Good { s with row := s.row.map f } := by
  obtain ⟨o, b, w, hh, rc, a, nm, pl, sw, fin⟩ := g
  refine ⟨o, b, w, hh, ?_, ?_, ?_, ?_, sw, fin⟩
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
  · intro h
    obtain ⟨r, hr, hp⟩ := nm h
    exact ⟨f r, by simp [hr], by rw [keep]; exact hp⟩
  · intro h
    obtain ⟨gone, r, hr, hp⟩ := pl h
    exact ⟨gone, f r, by simp [hr], by rw [keep]; exact hp⟩

/-- Planning a new session keeps the invariant once no session is live. -/
theorem replan_good (s : State) (r : Row) (g : Good s) (gone : s.session = none) :
    Good (replan s r) := by
  refine ⟨?_, ?_, g.whole, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> simp [replan, gone]

theorem apply_good (s : State) (st : Step) (g : Good s) : Good (st.apply s) := by
  obtain ⟨o, b, w, hh, rc, a, nm, pl, sw, fin⟩ := g
  have g : Good s := ⟨o, b, w, hh, rc, a, nm, pl, sw, fin⟩
  cases st with
  | start forced =>
    simp only [Step.apply]
    split
    · rename_i r hr hat
      split
      · refine ⟨o, b, w, by simp, rc, ?_, nm, by simp, by simp, by simp⟩
        intro held hs
        obtain ⟨r', hr', h⟩ := a held hs
        refine ⟨r', hr', ?_⟩
        rcases h with h | ⟨_, n, hn⟩
        · exact Or.inl h
        · rw [hat] at hn; cases hn
      · exact g
    · exact g
  | open_ =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | started =>
        have plain := named_session s g (by simp [hat])
        have clean : ∀ m, r.recorded = .session m → s.stray = 0 := by
          intro m hm
          rcases Nat.eq_zero_or_pos s.stray with h | h
          · exact h
          · obtain ⟨r', hr', hp⟩ := nm h
            rw [hr] at hr'; cases hr'; rw [hm] at hp; cases hp
        have gone : r.recorded.isSession = false → s.session = none := by
          intro hn
          cases hs : s.session with
          | none => rfl
          | some held =>
            obtain ⟨r', hr', h⟩ := plain held hs
            rw [hr] at hr'; cases hr'; rw [hn] at h; cases h
        cases hrec : r.recorded with
        | session m =>
          cases hs : s.session with
          | some held =>
            have e : Step.open_.apply s = { s with attempt := .holding held.length } := by
              simp [Step.apply, hr, hat, hrec, hs]
            rw [e]
            refine ⟨o, b, w, ?_, rc, ?_, nm, by simp, by simp [clean m hrec], by simp⟩
            · intro n held' ha hs'
              simp at ha; rw [hs] at hs'; cases hs'; exact ha
            · intro held' _; exact ⟨r, hr, Or.inl (by simp [hrec])⟩
          | none =>
            cases hp : s.published with
            | some ps =>
              have e : Step.open_.apply s = { s with attempt := .holding s.total } := by
                simp [Step.apply, hr, hat, hrec, hs, hp]
              rw [e]
              refine ⟨o, b, w, ?_, rc, ?_, nm, by simp, by simp [clean m hrec], by simp⟩
              · intro n held' _ hs'; rw [hs] at hs'; cases hs'
              · intro held' hs'; rw [hs] at hs'; cases hs'
            | none =>
              simp only [Step.apply, hr, hat, hrec, hs, hp]
              exact replan_good s r g hs
        | plan =>
          simp only [Step.apply, hr, hat, hrec]
          exact replan_good s r g (gone (by simp [hrec]))
        | none =>
          simp only [Step.apply, hr, hat, hrec]
          exact replan_good s r g (gone (by simp [hrec]))
      | _ => simpa [Step.apply, hr, hat] using g
  | begin replied =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | planned =>
        obtain ⟨gone, r', hr', hplan⟩ := pl hat
        rw [hr] at hr'; cases hr'
        have clean : s.stray = 0 := sw (by simp [hat])
        cases replied with
        | true =>
          simp only [Step.apply, hr, hat, ite_true]
          refine ⟨?_, ?_, w, ?_, ?_, ?_, ?_, by simp, by simp [clean], by simp⟩
          · intro held h; simp at h; subst h; rfl
          · intro held h; simp at h; subst h; simp
          · intro n held ha h; simp at ha h; subst h; simp [ha]
          · intro r'' n held hr'' hn _
            simp at hr''; subst hr''; rw [hplan] at hn; cases hn
          · intro held _; exact ⟨r, by simp , Or.inr ⟨hplan, 0, rfl⟩⟩
          · intro _; exact ⟨r, by simp , hplan⟩
        | false =>
          simp only [Step.apply, hr, hat]
          apply failWith_good_of
          · intro held h; simp [gone] at h
          · intro held h; simp [gone] at h
          · exact w
          · intro r'' n held _ _ h; simp [gone] at h
          · intro held h; simp [gone] at h
          · intro _; exact ⟨r, by simp , hplan⟩
      | _ => simpa [Step.apply, hr, hat] using g
  | record =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | holding n =>
        have clean : s.stray = 0 := sw (by simp [hat])
        have e : Step.record.apply s = { s with row := some { r with recorded := .session n } } := by
          simp [Step.apply, hr, hat]
        rw [e]
        refine ⟨o, b, w, hh, ?_, ?_, ?_, ?_, sw, fin⟩
        · intro r' m held hr' hm hs
          simp at hr'; subst hr'; simp at hm; subst hm
          exact Nat.le_of_eq (hh n held hat hs).symm
        · intro held _; exact ⟨_, rfl, Or.inl (by simp)⟩
        · intro h; simp [clean] at h
        · intro h; simp [hat] at h
      | _ => simpa [Step.apply, hr, hat] using g
  | part reply =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | holding n =>
        have clean : s.stray = 0 := sw (by simp [hat])
        cases hs : s.session with
        | some held =>
          simp only [Step.apply, hr, hat, hs]
          split
          · rename_i guard
            obtain ⟨lt, hf⟩ := guard
            have len : held.length = n := hh n held hat hs
            have ord := o held hs
            have snoc : held ++ [n] = List.range (held ++ [n]).length := by
              rw [ord, ← len, List.length_append, List.length_range, range_snoc]; simp
            cases reply with
            | confirmed =>
              refine ⟨?_, ?_, w, ?_, ?_, ?_, ?_, by simp, by simp [clean], by simp⟩
              · intro held' h; simp at h; subst h; exact snoc
              · intro held' h; simp at h; subst h; simp; omega
              · intro m held' ha h; simp at ha h; subst h; simp; omega
              · intro r' m held' hr' hm h
                simp at h hr'; subst h hr'
                rw [hm] at hf; simp at hf; simp; omega
              · intro held' _
                obtain ⟨r', hr', h⟩ := a held hs
                rw [hr] at hr'; cases hr'
                refine ⟨r, by simp , ?_⟩
                rcases h with h | ⟨hp, _⟩
                · exact Or.inl h
                · exact Or.inr ⟨hp, n + 1, rfl⟩
              · intro h; simp [clean] at h
            | lost =>
              apply failWith_good_of
              · intro held' h; simp at h; subst h; exact snoc
              · intro held' h; simp at h; subst h; simp; omega
              · exact w
              · intro r' m held' hr' hm h
                simp at hr' h; subst hr' h; simp at hm; subst hm; simp; omega
              · intro held' _; exact ⟨_, rfl, by simp⟩
              · intro h; simp [clean] at h
            | dropped =>
              apply failWith_good_of
              · intro held' e; simp at e; subst e; exact o _ hs
              · intro held' e; simp at e; subst e; exact b _ hs
              · exact w
              · intro r' m held' hr' hm h
                simp at hr'; subst hr'; simp at hm; subst hm
                simp at h; subst h; omega
              · intro held' _; exact ⟨_, rfl, by simp⟩
              · intro h; simp [clean] at h
          · exact g
        | none =>
          simp only [Step.apply, hr, hat, hs]
          split
          · apply failWith_good_of
            · intro held h; simp at h
            · intro held h; simp at h
            · exact w
            · intro r' m held _ _ h; simp at h
            · intro held h; simp at h
            · intro h; simp [clean] at h
          · exact g
      | _ => simpa [Step.apply, hr, hat] using g
  | finish replied =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | holding n =>
        have clean : s.stray = 0 := sw (by simp [hat])
        simp only [Step.apply, hr, hat]
        split
        · rename_i guard
          obtain ⟨eq, hrec⟩ := guard
          have plain : ∀ held, s.session = some held →
              ∃ r, s.row = some r ∧ r.recorded.isSession = true :=
            fun _ _ => ⟨r, hr, by simp [hrec]⟩
          cases hs : s.session with
          | some held =>
            have len : held.length = n := hh n held hat hs
            have whole : held = List.range s.total := by rw [o held hs, len, eq]
            simp only
            split
            · refine ⟨by simp, by simp, ?_, by simp, by simp, by simp, ?_, by simp, ?_, by simp⟩
              · intro ps h; simp at h; subst h; exact whole
              · intro h; simp [clean] at h
              · intro _; simpa using clean
            · apply failWith_good_of <;> try simp
              · exact whole
              · intro h; simp [clean] at h
          | none =>
            cases hp : s.published with
            | some ps =>
              simp only
              refine ⟨by simp , by simp , ?_, by simp, by simp , by simp , ?_,
                by simp, ?_, by simp ⟩
              · intro ps' h; simp at h; subst h; exact w ps hp
              · intro h; simp [clean] at h
              · intro _; simpa using clean
            | none =>
              simp only
              exact failWith_good s _ g plain
        · exact g
      | _ => simpa [Step.apply, hr, hat] using g
  | complete =>
    simp only [Step.apply]
    split
    · rename_i r hr hat
      have none := fin hat
      have clean : s.stray = 0 := sw (by simp [hat])
      exact ⟨by simp [none], by simp [none], w, by simp, by simp, by simp [none], by simp [clean],
        by simp, by simp, by simp⟩
    · exact g
  | fail c =>
    cases hat : s.attempt with
    | started =>
      simp only [Step.apply, hat]
      exact failWith_good s c g (named_session s g (by simp [hat]))
    | planned =>
      simp only [Step.apply, hat]
      exact failWith_good s c g (named_session s g (by simp [hat]))
    | holding n =>
      have clean : s.stray = 0 := sw (by simp [hat])
      simp only [Step.apply, hat]
      apply failWith_good_of
      · exact o
      · exact b
      · exact w
      · intro r' m held hr' hm hs
        cases hr₀ : s.row with
        | none => simp [hr₀] at hr'
        | some r₀ =>
          simp [hr₀] at hr'; subst hr'; simp at hm; subst hm
          exact Nat.le_of_eq (hh _ held hat hs).symm
      · intro held hs
        obtain ⟨r₀, hr₀, _⟩ := a held hs
        exact ⟨{ r₀ with recorded := .session n }, by simp [hr₀], by simp⟩
      · intro h; simp [clean] at h
    | _ => simpa [Step.apply, hat] using g
  | crash =>
    simp only [Step.apply]
    unfold strand
    cases hr : s.row with
    | none =>
      have hs : s.session = none := by
        cases h : s.session with
        | none => rfl
        | some held => obtain ⟨r, hr', _⟩ := a held h; rw [hr] at hr'; cases hr'
      have hst : s.stray = 0 := by
        rcases Nat.eq_zero_or_pos s.stray with h | h
        · exact h
        · obtain ⟨r, hr', _⟩ := nm h; rw [hr] at hr'; cases hr'
      simp only 
      exact ⟨by simp [hs], by simp [hs], w, by simp, by simp [hs], by simp [hs], by simp [hst],
        by simp, by simp, by simp⟩
    | some r =>
      cases hs : s.session with
      | none =>
        simp only [hs]
        refine ⟨by simp , by simp , w, by simp, by simp , by simp , ?_, by simp,
          by simp, by simp⟩
        intro h
        obtain ⟨r', hr', hp⟩ := nm h
        rw [hr] at hr'; cases hr'
        exact ⟨_, rfl, by simp [hp]⟩
      | some held =>
        simp only 
        split
        · rename_i hp
          refine ⟨by simp, by simp, w, by simp, by simp, by simp, ?_, by simp, by simp, by simp⟩
          intro _; exact ⟨_, rfl, by simp [hp]⟩
        · rename_i hp
          refine ⟨?_, ?_, w, by simp, ?_, ?_, ?_, by simp, by simp, by simp⟩
          · intro held' h; simp [hs] at h; subst h; exact o _ hs
          · intro held' h; simp [hs] at h; subst h; exact b _ hs
          · intro r' m held' hr' hm h
            simp at hr' h; subst hr'; simp at hm
            exact rc r m held' hr hm h
          · intro held' _
            obtain ⟨r₀, hr₀, h₀⟩ := a held hs
            rw [hr] at hr₀; cases hr₀
            refine ⟨_, rfl, Or.inl ?_⟩
            rcases h₀ with h₁ | ⟨h₂, _⟩
            · simpa using h₁
            · exact absurd h₂ hp
          · intro h
            obtain ⟨r', hr', hp'⟩ := nm h
            rw [hr] at hr'; cases hr'
            exact absurd hp' hp
  | expire =>
    simp only [Step.apply]
    exact ⟨by simp, by simp, w, by simp, by simp, by simp, by simp,
      fun h => ⟨rfl, (pl h).2⟩, by simp, by simp⟩
  | abandon =>
    simp only [Step.apply]
    exact rows_good s (fun r => { r with waited := false }) g (fun _ => rfl)
  | abort result =>
    simp only [Step.apply]
    split
    · rename_i r hr hat
      split
      · cases result with
        | none =>
          exact ⟨by simp, by simp, w, by simp, by simp, by simp, by simp, by simp [hat], by simp,
            by simp [hat]⟩
        | some c => exact failWith_good s c g (named_session s g (by simp [hat]))
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
            obtain ⟨r', hr', h⟩ := a held hs
            rw [hr] at hr'; cases hr'
            rcases h with h | ⟨h, _⟩
            · simp [guard.2] at h
            · rw [guard.2] at h; cases h
        have clean : s.stray = 0 := by
          rcases Nat.eq_zero_or_pos s.stray with h | h
          · exact h
          · obtain ⟨r', hr', hp⟩ := nm h
            rw [hr] at hr'; cases hr'; rw [guard.2] at hp; cases hp
        exact ⟨by simp [none], by simp [none], w, by simp [none], by simp, by simp [none],
          by simp [clean], by simp [hat], by simp [clean], by simp [none]⟩
      · exact g
    · exact g
  | elapse =>
    simp only [Step.apply]
    exact rows_good s _ g (fun r => ending_recorded r _)
  | connect =>
    simp only [Step.apply]
    exact rows_good s _ g (fun r => ending_recorded r _)

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
read the recorded session's columns, a plan's showing none yet (§17.9). -/
def State.progress (s : State) : Option Nat := s.row.bind (·.recorded.confirmed)

/-- §17.9: progress is never ahead of the parts the provider's session holds:
a part is recorded only once the provider confirmed it. -/
theorem progress_never_ahead (s : State) (g : Good s) (n : Nat) (held : List Nat)
    (shown : s.progress = some n) (live : s.session = some held) : n ≤ held.length := by
  unfold State.progress at shown
  cases hr : s.row with
  | none => simp [hr] at shown
  | some r =>
    simp [hr] at shown
    cases hrec : r.recorded with
    | session m =>
      simp [hrec] at shown; subst shown
      exact g.recorded r m held hr hrec live
    | plan => simp [hrec] at shown; omega
    | none => simp [hrec] at shown

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

/-- The sessions the provider holds for the upload: the live one, if any, and
the strays. -/
def State.live (s : State) : Nat := s.stray + (if s.session.isSome then 1 else 0)

/-- A crash forgets only memory: the published object and the recorded parts
stay, and so does every session the provider holds, one the row doesn't name
yet as a stray. -/
theorem crash_keeps (s : State) :
    (Step.crash.apply s).published = s.published ∧ (Step.crash.apply s).progress = s.progress ∧
      (Step.crash.apply s).live = s.live := by
  obtain ⟨total, fixed, session, stray, published, row, stored, attempt⟩ := s
  cases row <;> cases session <;> simp [Step.apply, strand, State.progress, State.live] <;>
    (try split) <;> simp_all <;> omega

/-- §17.9: after a crash, a session the provider still has continues from
the parts it holds, which are at least the recorded ones. -/
theorem resume_continues (s : State) (r : Row) (m : Nat) (held : List Nat)
    (hr : s.row = some r) (hat : s.attempt = .started) (hrec : r.recorded = .session m)
    (live : s.session = some held) :
    Step.open_.apply s = { s with attempt := .holding held.length } := by
  simp [Step.apply, hr, hat, hrec, live]

/-- §17.9: a session the provider has expired starts over: the attempt plans
a new one, recorded before the provider begins it; the location, key and
chunk hashes stay. -/
theorem expired_starts_over (s : State) (r : Row) (m : Nat)
    (hr : s.row = some r) (hat : s.attempt = .started) (hrec : r.recorded = .session m)
    (gone : s.session = none) (unpublished : s.published = none) :
    Step.open_.apply s = replan s r := by
  simp [Step.apply, hr, hat, hrec, gone, unpublished]

theorem apply_fixed (s : State) (st : Step) :
    (st.apply s).fixed = s.fixed ∧ (st.apply s).total = s.total := by
  cases st <;> simp only [Step.apply, strand] <;> (repeat' split) <;> simp [failWith, replan]

/-- No step changes what the staging write fixed. -/
theorem steps_keep_fixed (s : State) (steps : List Step) :
    (runSteps s steps).fixed = s.fixed ∧ (runSteps s steps).total = s.total := by
  induction steps generalizing s with
  | nil => exact ⟨rfl, rfl⟩
  | cons st rest ih =>
    obtain ⟨f, t⟩ := ih (st.apply s)
    obtain ⟨f', t'⟩ := apply_fixed s st
    exact ⟨f.trans f', t.trans t'⟩

/-! ## Sessions begun but never named -/

/-- §17.9: the provider begins a session only once the row records its plan,
and with no session live. -/
theorem begin_needs_plan (s : State) (g : Good s) (replied : Bool)
    (moved : (Step.begin replied).apply s ≠ s) :
    s.session = none ∧ ∃ r, s.row = some r ∧ r.recorded = .plan := by
  simp only [Step.apply] at moved
  split at moved
  · rename_i r hr hat; exact g.planned hat
  · exact absurd rfl moved

/-- Every stray sits at the place of the plan the row records, so the row
finds it again. -/
theorem stray_named (s : State) (g : Good s) (h : 0 < s.stray) :
    ∃ r, s.row = some r ∧ r.recorded = .plan :=
  g.named h

/-- The steps that can leave a stray: a lost begin reply, and a crash or a
failure, which strand a session the attempt began that its row doesn't name
yet. -/
def Step.strands : Step → Bool
  | .begin replied => !replied
  | .crash | .fail _ => true
  | _ => false

theorem apply_stray_le (s : State) (st : Step) (h : st.strands = false) :
    (st.apply s).stray ≤ s.stray := by
  cases st <;> simp only [Step.strands] at h <;> simp only [Step.apply] <;> (repeat' split) <;>
    simp_all [failWith, replan]

theorem steps_stray_le (s : State) (steps : List Step) (h : ∀ st ∈ steps, st.strands = false) :
    (runSteps s steps).stray ≤ s.stray := by
  induction steps generalizing s with
  | nil => exact Nat.le_refl _
  | cons st rest ih =>
    have here := apply_stray_le s st (h st (by simp))
    have later := ih (st.apply s) (fun st' m => h st' (by simp [m]))
    exact Nat.le_trans later here

/-- The attempt's first request ends every stray: a resumed row names its
session, so it has none, and planning ends them all. -/
theorem open_sweeps (s : State) (g : Good s) (r : Row) (hr : s.row = some r)
    (hat : s.attempt = .started) : (Step.open_.apply s).stray = 0 := by
  cases hrec : r.recorded with
  | session m =>
    have clean : s.stray = 0 := by
      rcases Nat.eq_zero_or_pos s.stray with h | h
      · exact h
      · obtain ⟨r', hr', hp⟩ := g.named h
        rw [hr] at hr'; cases hr'; rw [hrec] at hp; cases hp
    cases hs : s.session <;> cases hp : s.published <;>
      simp [Step.apply, hr, hat, hrec, hs, hp, replan, clean]
  | plan => simp [Step.apply, hr, hat, hrec, replan]
  | none => simp [Step.apply, hr, hat, hrec, replan]

/-- The abort of an upload no file waits for ends its session and every
stray before forgetting it. -/
theorem abort_sweeps (s : State) (moved : (Step.abort none).apply s ≠ s) :
    ((Step.abort none).apply s).session = none ∧ ((Step.abort none).apply s).stray = 0 := by
  simp only [Step.apply] at moved ⊢
  split at moved
  · rename_i r hr hat
    split at moved
    · rename_i guard
      simp [hat, guard]
    · exact absurd rfl moved
  · exact absurd rfl moved

/-! ## Retiring an upload no file waits for -/

/-- §17.9: an upload retires with no session left at the provider, named or
stray. -/
theorem retired_no_session (s : State) (g : Good s) (gone : s.row = none) :
    s.session = none ∧ s.stray = 0 := by
  constructor
  · cases hs : s.session with
    | none => rfl
    | some held =>
      obtain ⟨r, hr, _⟩ := g.accounted held hs
      rw [gone] at hr; cases hr
  · rcases Nat.eq_zero_or_pos s.stray with h | h
    · exact h
    · obtain ⟨r, hr, _⟩ := g.named h
      rw [gone] at hr; cases hr

/-- Only a retire removes the row of an upload no file waits for, and only
once it records nothing. -/
theorem retire_needs_no_session (s : State) (r : Row) (hr : s.row = some r)
    (gone : (Step.retire.apply s).row = none) : r.waited = false ∧ r.recorded = .none := by
  obtain ⟨total, fixed, session, stray, published, row, stored, attempt⟩ := s
  simp only at hr; subst hr
  cases attempt <;> simp [Step.apply] at gone
  split at gone
  · assumption
  · simp at gone

/-- §17.9: what a row records is forgotten only by aborting it, which ends the
provider's session and every stray first. -/
theorem forgotten_by_abort (s : State) (st : Step) (r r' : Row)
    (hr : s.row = some r) (hrec : r.recorded ≠ .none)
    (after : (st.apply s).row = some r') (forgot : r'.recorded = .none) :
    st = .abort none ∧ (st.apply s).session = none ∧ (st.apply s).stray = 0 := by
  obtain ⟨total, fixed, session, stray, published, row, stored, attempt⟩ := s
  simp only at hr; subst hr
  cases hrc : r.recorded
  · exact absurd hrc hrec
  all_goals
  cases st <;> cases attempt <;> cases session <;> cases published <;>
    simp [Step.apply, failWith, Row.ending, replan, strand, hrc] at after ⊢ <;>
    (repeat' split at after) <;> (repeat' split) <;> (try cases ‹Option Class›) <;>
    (try simp only [Option.some.injEq] at after) <;> (try subst after) <;> simp_all

/-- §17.9: a failed abort is the upload's failure: the upload stays queued
with what it records, and waits as its class says. -/
theorem failed_abort_queued (s : State) (r : Row) (c : Class) (hr : s.row = some r)
    (idle : s.attempt = .idle) (abandoned : r.waited = false) (held : r.recorded ≠ .none)
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
  obtain ⟨total, fixed, session, stray, published, row, stored, attempt⟩ := s
  simp only at hr; subst hr
  cases hw : r.wait <;> cases st <;> cases attempt <;> cases session <;> cases published <;>
    simp [Step.apply, failWith, Row.ending, replan, strand, hw] at after waiting ⊢ <;>
    (repeat' split at after) <;> (try cases ‹Option Class›) <;>
    (try cases ‹Class›) <;> (try simp only [Option.some.injEq] at after) <;>
    (try subst after) <;> simp_all [Class.wait]

/-- §17.9: a `Never` failure is not retried. From an idle upload whose
failure never retries, no step sends anything or changes its failure or what
it records; at most its row retires, when it records nothing. -/
theorem never_stays (s : State) (st : Step) (r : Row) (hr : s.row = some r)
    (forever : r.wait = .forever) (idle : s.attempt = .idle) :
    (st.apply s).attempt = .idle ∧ (st.apply s).published = s.published ∧
      ((st.apply s).row = none ∨ ∃ r', (st.apply s).row = some r' ∧ r'.wait = .forever ∧
        r'.recorded = r.recorded ∧ r'.failure = r.failure) := by
  obtain ⟨total, fixed, session, stray, published, row, stored, attempt⟩ := s
  simp only at hr idle; subst hr idle
  cases st <;> simp [Step.apply, Row.ending, forever, Wait.due, strand] <;> (repeat' split) <;>
    simp_all

/-- No step brings back the row of an upload that completed or retired, or
publishes anything for it. -/
theorem gone_stays (s : State) (st : Step) (gone : s.row = none) :
    (st.apply s).row = none ∧ (st.apply s).published = s.published := by
  obtain ⟨total, fixed, session, stray, published, row, stored, attempt⟩ := s
  simp only at gone; subst gone
  cases st <;> cases attempt <;> simp [Step.apply, failWith, strand]

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

A run of the upload worker (§17.9) first aborts what an upload no file waits
for left, once due, and retires one that records nothing; then it attempts an
upload a file waits for, if due. An attempt resumes its recorded session and
records it, or records a plan and begins the session; it sends each part and
records it, publishes, and completes. What the provider does to the run is
its `Fault`. -/

/-- What happens to the part after the run's `k` confirmed ones: its reply is
lost, it is dropped, or the process stops while sending it. -/
inductive PartFault where
  | lost
  | dropped
  | crash
  deriving DecidableEq, Repr

/-- What the provider does to one run: nothing; expire its sessions before the
run's first request; fail that request with a class; lose the reply to the
begin of a planned session; act on the part after `k` confirmed ones; or lose
the completion reply. -/
inductive Fault where
  | none
  | expire
  | fail (c : Class)
  | loseBegin
  | afterParts (k : Nat) (then_ : PartFault)
  | loseCompletion
  deriving DecidableEq, Repr

/-- `k` parts, each recorded once confirmed. -/
def partSteps : Nat → List Step
  | 0 => []
  | k + 1 => .part .confirmed :: .record :: partSteps k

/-- The parts and publication of an attempt holding a session. -/
def partsPlan (s : State) (fault : Fault) : List Step :=
  match s.attempt with
  | .holding n =>
    let remaining := s.total - n
    let finishing := partSteps remaining ++ [.finish (fault ≠ .loseCompletion), .complete]
    match fault with
    | .afterParts k pf =>
      if k < remaining then
        partSteps k ++
          (match pf with
           | .lost => [.part .lost]
           | .dropped => [.part .dropped]
           | .crash => [])
      else finishing
    | _ => finishing
  | _ => []

/-- What follows the attempt's first request: a resumed session is recorded
as the provider holds it; a planned one is begun. -/
def openedPlan (s : State) (fault : Fault) : List Step :=
  match s.attempt with
  | .planned =>
    if fault = .loseBegin then [.begin false]
    else .begin true :: partsPlan ((Step.begin true).apply s) fault
  | .holding _ => .record :: partsPlan (Step.record.apply s) fault
  | _ => []

/-- The steps of an attempt that has just started. -/
def attemptPlan (s : State) (fault : Fault) : List Step :=
  match fault with
  | .fail c => [.fail c]
  | _ =>
    let opening := (if fault = .expire then [Step.expire] else []) ++ [.open_]
    opening ++ openedPlan (runSteps s opening) fault

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
      else if r.recorded ≠ Record.none ∧ r.wait = .ready then
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

theorem partSteps_keep (k : Nat) : ∀ st ∈ partSteps k, st.strands = false := by
  induction k with
  | zero => simp [partSteps]
  | succ k ih =>
    intro st h
    simp only [partSteps, List.mem_cons] at h
    rcases h with rfl | rfl | h
    · rfl
    · rfl
    · exact ih st h

theorem partsPlan_keep (s : State) : ∀ st ∈ partsPlan s .none, st.strands = false := by
  intro st h
  unfold partsPlan at h
  split at h
  · simp only [List.mem_append, List.mem_cons, List.not_mem_nil] at h
    rcases h with h | rfl | rfl | h
    · exact partSteps_keep _ st h
    · rfl
    · rfl
    · exact h.elim
  · simp at h

theorem openedPlan_keep (s : State) : ∀ st ∈ openedPlan s .none, st.strands = false := by
  intro st h
  unfold openedPlan at h
  split at h
  · simp only [reduceCtorEq, ite_false, List.mem_cons] at h
    rcases h with rfl | h
    · rfl
    · exact partsPlan_keep _ st h
  · simp only [List.mem_cons] at h
    rcases h with rfl | h
    · rfl
    · exact partsPlan_keep _ st h
  · simp at h

theorem attemptPlan_keep (s : State) : ∀ st ∈ attemptPlan s .none, st.strands = false := by
  intro st h
  simp only [attemptPlan, reduceCtorEq, ite_false, List.nil_append, List.singleton_append,
    List.mem_cons] at h
  rcases h with rfl | h
  · rfl
  · exact openedPlan_keep _ st h

theorem plan_none_keep (s : State) (forced : Bool) : ∀ st ∈ plan s forced .none, st.strands = false := by
  intro st h
  unfold plan at h
  simp only [List.append_nil] at h
  split at h
  · split at h
    · split at h
      · simp only [List.mem_cons] at h
        rcases h with rfl | h
        · rfl
        · exact attemptPlan_keep _ st h
      · simp at h
    · split at h
      · simp at h
        rcases h with rfl | rfl <;> rfl
      · simp at h; subst h; rfl
  · simp at h

/-- §17.9: whatever a cut begin left, the upload's next run on a provider
that fails nothing ends: with a file waiting, its first request ends every
stray; with none, its abort does. -/
theorem next_run_sweeps (s : State) (g : Good s) (forced : Bool) (r : Row)
    (hr : s.row = some r) (idle : s.attempt = .idle) (ready : r.wait = .ready) :
    (run s forced .none).stray = 0 := by
  rcases Nat.eq_zero_or_pos s.stray with h0 | hpos
  · have := steps_stray_le s (plan s forced .none) (plan_none_keep s forced)
    unfold run; omega
  · obtain ⟨r', hr', hplan⟩ := g.named hpos
    rw [hr] at hr'; cases hr'
    cases hw : r.waited with
    | false =>
      simp [run, plan, hr, idle, hw, ready, hplan, runSteps, Step.apply]
    | true =>
      have opened : plan s forced .none = Step.start forced :: Step.open_ ::
          openedPlan (Step.open_.apply ((Step.start forced).apply s)) .none := by
        simp [plan, hr, idle, hw, ready, Wait.due, attemptPlan, runSteps]
      have swept : (Step.open_.apply ((Step.start forced).apply s)).stray = 0 :=
        open_sweeps _ (apply_good s _ g) r (by simp [Step.apply, hr, idle, hw, ready, Wait.due])
          (by simp [Step.apply, hr, idle, hw, ready, Wait.due])
      have keep := steps_stray_le (Step.open_.apply ((Step.start forced).apply s))
        (openedPlan (Step.open_.apply ((Step.start forced).apply s)) .none) (openedPlan_keep _)
      unfold run
      rw [opened]
      show (runSteps (Step.open_.apply ((Step.start forced).apply s)) _).stray = 0
      omega

end CovenStorage.UploadSession
