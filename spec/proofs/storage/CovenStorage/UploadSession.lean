import Std

/-! §17.9's upload sessions: one queued upload going up in parts through a
provider session that its queue row records, with the upload worker's runs,
crashes, an expiring provider, the retire of an upload no file waits for, and
the retry of each failure by its class.

A part is its index. The provider's session holds each part it was sent once,
in whatever order they arrived: a part sent again takes its own place, as S3
keeps one part per number. An attempt may send any part it hasn't confirmed,
so parts go together and their replies come back in any order, some lost, as
an attempt sending several at once meets them. Publishing hands the parts
over in index order, so it needs every one confirmed and recorded.

Before the provider begins a session, the row records a plan that names where
the session will be, and before the request that may begin it, the row marks
the plan. A session the provider began whose name the row never recorded,
because the reply was lost or the process stopped first, is a stray there,
behind a marked plan. Where the place is the device's own, the upload's next
attempt, or the abort of an upload no file waits for, ends every stray there
before anything else. Where it is shared, nothing tells a stray from another
device's session, so a marked plan there aborts nothing: it reads its
destination instead. Something published there means the name is taken, by
this upload's own bytes or another's; an empty destination starts a fresh
session at the same name, without waiting on the stray its lost start may
have begun. A stray can never publish, because no record names it, so no
device sends its completion, and once the name is taken create-once refuses
every other completion; whoever applies the name ends it with the other dead
sessions. An unmarked plan has nothing at its place, and resumes or is
forgotten anywhere. A begin refused before its request goes out, as when the
publication gate closes after the part's admission, began nothing, and its
failure leaves the plan unmarked. -/
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
and the chunk hashes (§17.9), and whether the upload's place is the device's
own, where no other upload goes, so that every session there is this
upload's (Appendix E.10). -/
structure Fixed where
  location : Nat
  key : Nat
  chunks : List Nat
  owned : Bool
  deriving DecidableEq, Repr

/-- What the queue row records of its provider session: nothing; a plan,
which names where the next session will be before the provider begins it,
marked (`started`) before the request that may begin it; or the session
itself, with its confirmed parts. -/
inductive Record where
  | none
  | plan (started : Bool)
  | session (confirmed : List Nat)
  deriving DecidableEq, Repr

@[simp] def Record.isSession : Record → Bool
  | .session _ => true
  | _ => false

@[simp] def Record.isPlan : Record → Bool
  | .plan _ => true
  | _ => false

/-- Whether a part sent with `c` confirmed goes to a session the row can find
again: the one it names, or, for the first part, alone, the one at its plan's
place. -/
def Record.finds : Record → List Nat → Bool
  | .session _, _ => true
  | .plan _, c => c.isEmpty
  | .none, _ => false

/-- The confirmed parts a record shows: none yet for a plan. -/
@[simp] def Record.confirmed : Record → Option (List Nat)
  | .session c => some c
  | .plan _ => some []
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
is about to begin, one holding a session with these parts confirmed, or one
whose bytes the provider published. -/
inductive Attempt where
  | idle
  | started
  | planned
  | holding (confirmed : List Nat)
  | finished
  deriving DecidableEq, Repr

/-- Whether the attempt's first request is behind it. -/
@[simp] def Attempt.opened : Attempt → Bool
  | .planned | .holding _ | .finished => true
  | .idle | .started => false

/-- The provider's live session, the parts it holds, and the sessions it began
at the plan's place that the row never named; the published object's parts;
the queue row, gone once the upload completes or retires; whether completion
switched the files to the stored location; and the attempt. `taken` says
something is published at the upload's place, this upload's own bytes or
another's: only a shared place has another's. -/
structure State where
  total : Nat
  fixed : Fixed
  session : Option (List Nat)
  stray : Nat
  published : Option (List Nat)
  taken : Bool
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
  taken := false
  row := some ⟨true, .none, none, .ready⟩
  stored := false
  attempt := .idle

/-- The provider's answer to one part: it holds the part and says so, or holds
it but the reply is lost. A part it never holds changes nothing; the attempt
then fails (`fail`). -/
inductive Reply where
  | confirmed
  | lost
  deriving DecidableEq, Repr

/-- The parts a provider session holds once part `i` arrives: a part sent
again takes its own place, so each is held once. -/
def receive (held : List Nat) (i : Nat) : List Nat := if i ∈ held then held else held ++ [i]

/-- Whether `c` holds every part of an object of `total` parts. -/
def covers (total : Nat) (c : List Nat) : Bool := (List.range total).all fun i => decide (i ∈ c)

/-- What publishing hands over from the parts a session holds: each part of
the object, in index order, that the session has. -/
def assemble (total : Nat) (held : List Nat) : List Nat :=
  (List.range total).filter fun i => decide (i ∈ held)

inductive Step where
  /-- A run starts an attempt, if the upload is due. -/
  | start (forced : Bool)
  /-- The attempt's first request: resume the session the row records; or,
  with none to resume (none recorded, an expired one, or only a plan), record
  the plan of a new one, unmarked. A marked plan may have begun a session:
  where the place is the device's own, every session there is ended first;
  where it is shared, nothing tells this upload's from another's, so the
  request reads the destination instead. Nothing published there records a
  fresh plan beside whatever the lost start began; this upload's own bytes
  published there complete it; another's make the name occupied, which fails
  the attempt for the caller to settle. -/
  | open_
  /-- Mark the plan, before the request that may begin its session. -/
  | mark
  /-- The provider begins the session of a marked plan; the reply may be
  lost, and the session it began is then a stray. -/
  | begin (replied : Bool)
  /-- The begin of a marked plan's session is refused before its request
  goes out, as when the publication gate closes after the part's admission:
  nothing began, so the attempt fails with class `c` and the row's plan is
  unmarked again. -/
  | refuse (c : Class)
  /-- The attempt stops before its first part, as a pause stops it; the row
  keeps its plan. -/
  | pause
  /-- Record the session the attempt holds, with its confirmed parts. -/
  | record
  /-- Send part `i`, one the attempt hasn't confirmed, to a session the row
  can find again (`Record.finds`). Parts sent together are these steps in
  the order their replies come back. -/
  | part (i : Nat) (reply : Reply)
  /-- Ask the provider to publish the whole object, once the row records every
  part confirmed; the reply may be lost. -/
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
  fails with class `c`. An unmarked plan has nothing at its place, so its
  abort sends nothing and only forgets it; a marked one at a shared place
  ends nothing, as nothing tells a stray from another device's session, and
  forgets the plan. -/
  | abort (result : Option Class)
  /-- Retire an upload no file waits for and with nothing recorded. -/
  | retire
  /-- Another device publishes at a shared place: the name is taken. -/
  | rival
  /-- A device that applied the taken place ends every session there, as
  Appendix E.10 says: the strays, and this upload's own session if its bytes
  are not the ones published. -/
  | endDead
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

/-- With no session to resume: record the plan of the next, unmarked. -/
def replan (s : State) (r : Row) : State :=
  { s with row := some { r with recorded := .plan false }, attempt := .planned }

/-- End every session at a marked plan's place, its strays: only at a place
the device owns, where each is this upload's. -/
def sweep (s : State) : State := { s with stray := 0 }

/-- Forget what the row records, having ended its session and, where the
place is the device's own, every stray. -/
def forget (s : State) (r : Row) : State :=
  { s with session := none, stray := if s.fixed.owned then 0 else s.stray,
           row := some { r with recorded := .none } }

/-- What a stopped attempt leaves at the provider: a session it began that its
row doesn't name yet is a stray at the plan's place. -/
def strand (s : State) : State :=
  match s.row, s.session with
  | some r, some _ =>
    if r.recorded = .plan true then { s with session := none, stray := s.stray + 1 } else s
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
        | some held, _ => { s with attempt := .holding held }
        | none, some _ => { s with attempt := .holding (List.range s.total) }
        | none, none => if s.taken then failWith s .never else replan s r
      | .plan true =>
        if s.fixed.owned then replan (sweep s) r
        else if s.taken then
          match s.published with
          | some _ => { s with attempt := .holding (List.range s.total) }
          | none => failWith s .never
        else replan s r
      | _ => replan s r
    | _, _ => s
  | .mark =>
    match s.row, s.attempt with
    | some r, .planned =>
      match r.recorded with
      | .plan _ => { s with row := some { r with recorded := .plan true } }
      | _ => s
    | _, _ => s
  | .begin replied =>
    match s.row, s.attempt with
    | some r, .planned =>
      if r.recorded = .plan true then
        if replied then { s with session := some [], attempt := .holding [] }
        else failWith { s with stray := s.stray + 1 } .automatic
      else s
    | _, _ => s
  | .refuse c =>
    match s.row, s.attempt with
    | some r, .planned =>
      if r.recorded = .plan true then
        failWith { s with row := some { r with recorded := .plan false } } c
      else s
    | _, _ => s
  | .pause =>
    match s.attempt with
    | .planned => { s with attempt := .idle }
    | _ => s
  | .record =>
    match s.row, s.attempt with
    | some r, .holding c => { s with row := some { r with recorded := .session c } }
    | _, _ => s
  | .part i reply =>
    match s.row, s.attempt, s.session with
    | some r, .holding c, some held =>
      if i < s.total ∧ i ∉ c ∧ r.recorded.finds c = true then
        match reply with
        | .confirmed => { s with session := some (receive held i), attempt := .holding (c ++ [i]) }
        | .lost => { s with session := some (receive held i) }
      else s
    | some r, .holding c, none =>
      if i < s.total ∧ i ∉ c ∧ r.recorded.finds c = true then
        failWith { s with row := some { r with recorded := .session c } } .automatic
      else s
    | _, _, _ => s
  | .finish replied =>
    match s.row, s.attempt with
    | some r, .holding _ =>
      match r.recorded with
      | .session m =>
        if covers s.total m then
          if s.taken = true ∧ s.published = none then failWith s .never
          else
            match s.session, s.published with
            | some held, none =>
              if replied then
                { s with session := none, published := some (assemble s.total held),
                         taken := true, attempt := .finished }
              else failWith { s with session := none, published := some (assemble s.total held),
                                     taken := true }
                .automatic
            | _, some _ => { s with session := none, attempt := .finished }
            | none, none => failWith s .automatic
        else s
      | _ => s
    | _, _ => s
  | .complete =>
    match s.row, s.attempt with
    | some r, .finished => { s with row := none, stored := s.stored || r.waited, attempt := .idle }
    | _, _ => s
  | .fail c =>
    match s.attempt with
    | .started | .planned => failWith s c
    | .holding k => failWith { s with row := s.row.map fun r => { r with recorded := .session k } } c
    | _ => s
  | .crash => { strand s with attempt := .idle, row := s.row.map (·.ending .connection) }
  | .expire => { s with session := none, stray := 0 }
  | .abandon => { s with row := s.row.map fun r => { r with waited := false } }
  | .abort result =>
    match s.row, s.attempt with
    | some r, .idle =>
      if r.waited = false ∧ r.recorded ≠ Record.none ∧ r.wait = .ready then
        if r.recorded = .plan false ∨ (r.recorded = .plan true ∧ s.fixed.owned = false) then
          forget s r
        else
          match result with
          | none => forget s r
          | some c => failWith s c
      else s
    | _, _ => s
  | .retire =>
    match s.row, s.attempt with
    | some r, .idle =>
      if r.waited = false ∧ r.recorded = Record.none then { s with row := none } else s
    | _, _ => s
  | .rival => if s.fixed.owned = false then { s with taken := true } else s
  | .endDead => if s.taken then { s with stray := 0, session := none } else s
  | .elapse => { s with row := s.row.map (·.ending .backoff) }
  | .connect => { s with row := s.row.map (·.ending .connection) }

def runSteps (s : State) (steps : List Step) : State := steps.foldl Step.apply s

/-! ## The invariant -/

/-- What holds in every reachable state: the provider's session holds each
part once, never one the file lacks; the published object is every part in
order; the parts an attempt holding a session counts confirmed are ones the
provider holds, each once; the recorded parts are ones the provider holds,
each once; a live provider session is one the row names, or one the attempt
holds that the row names by its marked plan's place; where the place is the
device's own, every stray sits at the place of the marked plan the row records
(at a shared place a stray needs no plan behind it: it is beside whatever else
is there); a planned attempt holds no session yet, and its row records a plan;
where the place is the device's own, no stray outlasts an attempt's first
request; a finished attempt holds no session; and a published object makes
the name taken. -/
structure Good (s : State) : Prop where
  nodup : ∀ held, s.session = some held → held.Nodup
  bounded : ∀ held, s.session = some held → ∀ i ∈ held, i < s.total
  whole : ∀ ps, s.published = some ps → ps = List.range s.total
  holding : ∀ c held, s.attempt = .holding c → s.session = some held → ∀ i ∈ c, i ∈ held
  distinct : ∀ c, s.attempt = .holding c → c.Nodup
  recorded : ∀ r m held, s.row = some r → r.recorded = .session m → s.session = some held →
    ∀ i ∈ m, i ∈ held
  listed : ∀ r m, s.row = some r → r.recorded = .session m → m.Nodup
  accounted : ∀ held, s.session = some held → ∃ r, s.row = some r ∧
    (r.recorded.isSession = true ∨ (r.recorded = .plan true ∧ ∃ c, s.attempt = .holding c))
  named : s.fixed.owned = true → 0 < s.stray → ∃ r, s.row = some r ∧ r.recorded = .plan true
  planned : s.attempt = .planned → s.session = none ∧ ∃ r, s.row = some r ∧ r.recorded.isPlan = true
  swept : s.attempt.opened = true → s.fixed.owned = true → s.stray = 0
  finished : s.attempt = .finished → s.session = none
  taken : s.published.isSome = true → s.taken = true

theorem initial_good (total : Nat) (fixed : Fixed) : Good (initial total fixed) := by
  constructor <;> simp [initial]
  rintro r m rfl h; simp at h

theorem mem_receive (held : List Nat) (i j : Nat) : j ∈ receive held i ↔ j ∈ held ∨ j = i := by
  unfold receive
  by_cases h : i ∈ held
  · simp only [h, ite_true]
    constructor
    · exact Or.inl
    · rintro (m | rfl)
      · exact m
      · exact h
  · simp [h]

theorem receive_nodup (held : List Nat) (i : Nat) (d : held.Nodup) : (receive held i).Nodup := by
  unfold receive
  by_cases h : i ∈ held
  · simpa [h] using d
  · simp only [h, ite_false]
    exact List.nodup_append.2 ⟨d, (List.nodup_cons.2 ⟨List.not_mem_nil, List.nodup_nil⟩), by
      intro a ha b hb; simp at hb; subst hb; intro e; subst e; exact h ha⟩

theorem snoc_nodup (c : List Nat) (i : Nat) (d : c.Nodup) (h : i ∉ c) : (c ++ [i]).Nodup :=
  List.nodup_append.2 ⟨d, (List.nodup_cons.2 ⟨List.not_mem_nil, List.nodup_nil⟩), by
    intro a ha b hb; simp at hb; subst hb; intro e; subst e; exact h ha⟩

theorem covers_mem (total : Nat) (c : List Nat) (h : covers total c = true) :
    ∀ i, i < total → i ∈ c := by
  intro i hi
  unfold covers at h
  have := List.all_eq_true.1 h i (List.mem_range.2 hi)
  simpa using this

/-- Publishing a session that holds every part hands over each part once, in
order. -/
theorem assemble_whole (total : Nat) (held : List Nat) (all : ∀ i, i < total → i ∈ held) :
    assemble total held = List.range total := by
  unfold assemble
  apply List.filter_eq_self.2
  intro i hi
  simpa using all i (List.mem_range.1 hi)

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
    (nd : ∀ held, s.session = some held → held.Nodup)
    (b : ∀ held, s.session = some held → ∀ i ∈ held, i < s.total)
    (w : ∀ ps, s.published = some ps → ps = List.range s.total)
    (rc : ∀ r m held, s.row = some r → r.recorded = .session m → s.session = some held →
      ∀ i ∈ m, i ∈ held)
    (ls : ∀ r m, s.row = some r → r.recorded = .session m → m.Nodup)
    (a : ∀ held, s.session = some held → ∃ r, s.row = some r ∧ r.recorded.isSession = true)
    (nm : s.fixed.owned = true → 0 < s.stray → ∃ r, s.row = some r ∧ r.recorded = .plan true)
    (tk : s.published.isSome = true → s.taken = true) :
    Good (failWith s c) := by
  refine ⟨nd, b, w, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, tk⟩
  · intro k held h; simp [failWith] at h
  · intro k h; simp [failWith] at h
  · intro r m held hr hm hs
    obtain ⟨r₀, h₀, rfl⟩ := (failWith_row s c r).1 hr
    exact rc r₀ m held h₀ hm hs
  · intro r m hr hm
    obtain ⟨r₀, h₀, rfl⟩ := (failWith_row s c r).1 hr
    exact ls r₀ m h₀ hm
  · intro held hs
    obtain ⟨r, hr, hrec⟩ := a held hs
    exact ⟨_, (failWith_row s c _).2 ⟨r, hr, rfl⟩, Or.inl hrec⟩
  · intro ho h
    obtain ⟨r, hr, hp⟩ := nm ho h
    exact ⟨_, (failWith_row s c _).2 ⟨r, hr, rfl⟩, hp⟩
  · intro h; simp [failWith] at h
  · intro h; simp [failWith] at h
  · intro h; simp [failWith] at h

/-- With no attempt holding a session, every live session is one the row
names. -/
theorem named_session (s : State) (g : Good s) (free : ∀ c, s.attempt ≠ .holding c) :
    ∀ held, s.session = some held → ∃ r, s.row = some r ∧ r.recorded.isSession = true := by
  intro held hs
  obtain ⟨r, hr, h⟩ := g.accounted held hs
  rcases h with h | ⟨_, c, hc⟩
  · exact ⟨r, hr, h⟩
  · exact absurd hc (free c)

theorem failWith_good (s : State) (c : Class) (g : Good s)
    (a : ∀ held, s.session = some held → ∃ r, s.row = some r ∧ r.recorded.isSession = true) :
    Good (failWith s c) :=
  failWith_good_of s c g.nodup g.bounded g.whole g.recorded g.listed a g.named g.taken

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
  obtain ⟨nd, b, w, hh, dc, rc, ls, a, nm, pl, sw, fin, tk⟩ := g
  refine ⟨nd, b, w, hh, dc, ?_, ?_, ?_, ?_, ?_, sw, fin, tk⟩
  · intro r m held hr hm hs
    cases h : s.row with
    | none => simp [h] at hr
    | some r₀ =>
      simp [h] at hr
      subst hr
      rw [keep] at hm
      exact rc r₀ m held h hm hs
  · intro r m hr hm
    cases h : s.row with
    | none => simp [h] at hr
    | some r₀ =>
      simp [h] at hr
      subst hr
      rw [keep] at hm
      exact ls r₀ m h hm
  · intro held hs
    obtain ⟨r, hr, hrec⟩ := a held hs
    exact ⟨f r, by simp [hr], by rw [keep]; exact hrec⟩
  · intro ho h
    obtain ⟨r, hr, hp⟩ := nm ho h
    exact ⟨f r, by simp [hr], by rw [keep]; exact hp⟩
  · intro h
    obtain ⟨gone, r, hr, hp⟩ := pl h
    exact ⟨gone, f r, by simp [hr], by rw [keep]; exact hp⟩

/-- Planning a new session keeps the invariant once no session or stray is
live. -/
theorem replan_good (s : State) (r : Row)
    (w : ∀ ps, s.published = some ps → ps = List.range s.total) (gone : s.session = none)
    (clean : s.fixed.owned = true → s.stray = 0) (tk : s.published.isSome = true → s.taken = true) :
    Good (replan s r) := by
  refine ⟨?_, ?_, w, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, tk⟩ <;> simp [replan, gone]
  · rintro r' m rfl h; simp at h
  all_goals first | exact clean ‹_› | (intro ho; exact clean ho) | skip

/-- Only a marked plan has strays at its place. -/
theorem clean_of (s : State) (g : Good s) (r : Row) (hr : s.row = some r)
    (h : r.recorded ≠ .plan true) : s.fixed.owned = true → s.stray = 0 := by
  intro ho
  rcases Nat.eq_zero_or_pos s.stray with z | p
  · exact z
  · obtain ⟨r', hr', hp⟩ := g.named ho p
    rw [hr] at hr'; cases hr'; exact absurd hp h

/-- Forgetting what the row records keeps the invariant with no attempt. -/
theorem forget_good (s : State) (r : Row) (g : Good s) (idle : s.attempt = .idle) :
    Good (forget s r) :=
  ⟨by simp [forget], by simp [forget], g.whole, by simp [forget], by simp [idle, forget],
    by simp [forget], by simp [forget]; rintro r' m rfl h; simp at h, by simp [forget],
    fun ho h => by
      have ho' : s.fixed.owned = true := ho
      simp [forget, ho'] at h,
    by simp [forget, idle], fun hop _ => by simp [forget, idle] at hop, by simp [forget, idle],
    g.taken⟩

/-- Recording the parts an attempt holds keeps what the invariant says of the
row: they are held, each once. -/
theorem record_good (s : State) (g : Good s) (r : Row) (c : List Nat)
    (hat : s.attempt = .holding c) :
    Good { s with row := some { r with recorded := .session c } } := by
  obtain ⟨nd, b, w, hh, dc, rc, ls, a, nm, pl, sw, fin, tk⟩ := g
  refine ⟨nd, b, w, hh, dc, ?_, ?_, ?_, ?_, ?_, sw, fin, tk⟩
  · intro r' m held hr' hm hs
    simp at hr'; subst hr'; simp at hm; subst hm
    exact hh _ held hat hs
  · intro r' m hr' hm
    simp at hr'; subst hr'; simp at hm; subst hm
    exact dc _ hat
  · intro held _; exact ⟨_, rfl, Or.inl (by simp)⟩
  · intro ho h; simp [sw (by simp [hat]) ho] at h
  · intro h; simp [hat] at h

/-- Holding every part of an object the name already has, with no session
live, keeps the invariant. -/
theorem holding_range_good (s : State) (g : Good s) (gone : s.session = none)
    (clean : s.fixed.owned = true → s.stray = 0) :
    Good { s with attempt := .holding (List.range s.total) } := by
  obtain ⟨nd, b, w, hh, dc, rc, ls, a, nm, pl, sw, fin, tk⟩ := g
  refine ⟨nd, b, w, ?_, ?_, rc, ls, ?_, nm, by simp, fun _ ho => clean ho, by simp, tk⟩
  · intro c held' _ hs'; rw [gone] at hs'; cases hs'
  · intro c ha; simp at ha; subst ha; exact List.nodup_range
  · intro held' hs'; rw [gone] at hs'; cases hs'

/-- A completion that finds its bytes already published ends the attempt with
no session. -/
theorem finish_published_good (s : State) (g : Good s) (hp : s.published.isSome = true)
    (clean : s.fixed.owned = true → s.stray = 0) :
    Good { s with session := none, attempt := .finished } := by
  obtain ⟨nd, b, w, hh, dc, rc, ls, a, nm, pl, sw, fin, tk⟩ := g
  refine ⟨by simp, by simp, w, by simp, by simp, by simp, ls, by simp, nm, by simp,
    fun _ ho => clean ho, by simp, tk⟩

theorem apply_good (s : State) (st : Step) (g : Good s) : Good (st.apply s) := by
  obtain ⟨nd, b, w, hh, dc, rc, ls, a, nm, pl, sw, fin, tk⟩ := g
  have g : Good s := ⟨nd, b, w, hh, dc, rc, ls, a, nm, pl, sw, fin, tk⟩
  cases st with
  | start forced =>
    simp only [Step.apply]
    split
    · rename_i r hr hat
      split
      · refine ⟨nd, b, w, by simp, by simp, rc, ls, ?_, nm, by simp, by simp, by simp, tk⟩
        intro held hs
        obtain ⟨r', hr', h⟩ := a held hs
        refine ⟨r', hr', ?_⟩
        rcases h with h | ⟨_, c, hc⟩
        · exact Or.inl h
        · rw [hat] at hc; cases hc
      · exact g
    · exact g
  | open_ =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | started =>
        have plain := named_session s g (by simp [hat])
        have gone : r.recorded.isSession = false → s.session = none := by
          intro hn
          cases hs : s.session with
          | none => rfl
          | some held =>
            obtain ⟨r', hr', h⟩ := plain held hs
            rw [hr] at hr'; cases hr'; rw [hn] at h; cases h
        cases hrec : r.recorded with
        | session m =>
          have clean := clean_of s g r hr (by simp [hrec])
          cases hs : s.session with
          | some held =>
            have e : Step.open_.apply s = { s with attempt := .holding held } := by
              simp [Step.apply, hr, hat, hrec, hs]
            rw [e]
            refine ⟨nd, b, w, ?_, ?_, rc, ls, ?_, nm, by simp, fun _ ho => clean ho, by simp, tk⟩
            · intro c held' ha hs'
              simp at ha; rw [hs] at hs'; cases hs'; subst ha; intro i hi; exact hi
            · intro c ha; simp at ha; subst ha; exact nd held hs
            · intro held' _; exact ⟨r, hr, Or.inl (by simp [hrec])⟩
          | none =>
            cases hp : s.published with
            | some ps =>
              have e : Step.open_.apply s = { s with attempt := .holding (List.range s.total) } := by
                simp [Step.apply, hr, hat, hrec, hs, hp]
              rw [e]
              exact holding_range_good s g hs clean
            | none =>
              by_cases ht : s.taken = true
              · simp only [Step.apply, hr, hat, hrec, hs, hp, ht, ite_true]
                exact failWith_good s _ g plain
              · simp only [Step.apply, hr, hat, hrec, hs, hp, ht, ite_false]
                exact replan_good s r w hs clean tk
        | plan started =>
          cases started with
          | true =>
            cases ho : s.fixed.owned with
            | true =>
              simp only [Step.apply, hr, hat, hrec, ho, ite_true]
              exact replan_good (sweep s) r w (gone (by simp [hrec])) (fun _ => rfl) tk
            | false =>
              by_cases ht : s.taken = true
              · cases hp : s.published with
                | some ps =>
                  have e : Step.open_.apply s =
                      { s with attempt := .holding (List.range s.total) } := by
                    simp [Step.apply, hr, hat, hrec, ho, ht, hp]
                  rw [e]
                  exact holding_range_good s g (gone (by simp [hrec]))
                    (fun h => by rw [ho] at h; cases h)
                | none =>
                  simp only [Step.apply, hr, hat, hrec, ho, ht, hp, ite_true, Bool.false_eq_true,
                    ite_false]
                  exact failWith_good s _ g plain
              · simp only [Step.apply, hr, hat, hrec, ho, ht, Bool.false_eq_true, ite_false]
                exact replan_good s r w (gone (by simp [hrec]))
                  (fun h => by rw [ho] at h; cases h) tk
          | false =>
            simp only [Step.apply, hr, hat, hrec]
            exact replan_good s r w (gone (by simp [hrec])) (clean_of s g r hr (by simp [hrec])) tk
        | none =>
          simp only [Step.apply, hr, hat, hrec]
          exact replan_good s r w (gone (by simp [hrec])) (clean_of s g r hr (by simp [hrec])) tk
      | _ => simpa [Step.apply, hr, hat] using g
  | mark =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | planned =>
        obtain ⟨gone, r', hr', hplan⟩ := pl hat
        rw [hr] at hr'; cases hr'
        have clean : s.fixed.owned = true → s.stray = 0 := sw (by simp [hat])
        cases hrec : r.recorded with
        | plan started =>
          have e : Step.mark.apply s = { s with row := some { r with recorded := .plan true } } := by
            simp [Step.apply, hr, hat, hrec]
          rw [e]
          refine ⟨nd, b, w, hh, dc, ?_, ?_, ?_, ?_, ?_, sw, fin, tk⟩
          · intro r'' m held hr'' hm _
            simp at hr''; subst hr''; simp at hm
          · intro r'' m hr'' hm
            simp at hr''; subst hr''; simp at hm
          · intro held hs; rw [gone] at hs; cases hs
          · intro ho h; simp [clean ho] at h
          · intro _; exact ⟨gone, _, rfl, by simp⟩
        | none => simp [hrec] at hplan
        | session _ => simp [hrec] at hplan
      | _ => simpa [Step.apply, hr, hat] using g
  | begin replied =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | planned =>
        obtain ⟨gone, r', hr', _⟩ := pl hat
        rw [hr] at hr'; cases hr'
        have clean : s.fixed.owned = true → s.stray = 0 := sw (by simp [hat])
        by_cases hplan : r.recorded = .plan true
        · cases replied with
          | true =>
            simp only [Step.apply, hr, hat, hplan, ite_true]
            refine ⟨?_, ?_, w, ?_, ?_, ?_, ?_, ?_, ?_, by simp, fun _ ho => clean ho, by simp, tk⟩
            · intro held h; simp at h; subst h; exact List.nodup_nil
            · intro held h; simp at h; subst h; simp
            · intro c held ha h; simp at ha; subst ha; simp
            · intro c ha; simp at ha; subst ha; exact List.nodup_nil
            · intro r'' m held hr'' hm _
              simp at hr''; subst hr''; rw [hplan] at hm; cases hm
            · intro r'' m hr'' hm
              simp at hr''; subst hr''; rw [hplan] at hm; cases hm
            · intro held _; exact ⟨r, by simp, Or.inr ⟨hplan, [], rfl⟩⟩
            · intro _ _; exact ⟨r, by simp, hplan⟩
          | false =>
            simp only [Step.apply, hr, hat, hplan]
            apply failWith_good_of
            · intro held h; simp [gone] at h
            · intro held h; simp [gone] at h
            · exact w
            · intro r'' m held _ _ h; simp [gone] at h
            · intro r'' m hr'' hm
              simp at hr''; subst hr''; rw [hplan] at hm; cases hm
            · intro held h; simp [gone] at h
            · intro _ _; exact ⟨r, by simp, hplan⟩
            · exact tk
        · simp only [Step.apply, hr, hat, hplan, ite_false]
          exact g
      | _ => simpa [Step.apply, hr, hat] using g
  | refuse c =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | planned =>
        obtain ⟨gone, _⟩ := pl hat
        have clean : s.fixed.owned = true → s.stray = 0 := sw (by simp [hat])
        by_cases hplan : r.recorded = .plan true
        · simp only [Step.apply, hr, hat, hplan, ite_true]
          apply failWith_good_of
          · intro held h; simp [gone] at h
          · intro held h; simp [gone] at h
          · exact w
          · intro r'' m held _ _ h; simp [gone] at h
          · intro r'' m hr'' hm; simp at hr''; subst hr''; simp at hm
          · intro held h; simp [gone] at h
          · intro ho h; simp [clean ho] at h
          · exact tk
        · simp only [Step.apply, hr, hat, hplan, ite_false]
          exact g
      | _ => simpa [Step.apply, hr, hat] using g
  | pause =>
    cases hat : s.attempt with
    | planned =>
      obtain ⟨gone, _⟩ := pl hat
      have clean : s.fixed.owned = true → s.stray = 0 := sw (by simp [hat])
      simp only [Step.apply, hat]
      refine ⟨nd, b, w, by simp, by simp, rc, ls, ?_, fun ho h => by simp [clean ho] at h,
        by simp, by simp, by simp, tk⟩
      intro held hs; rw [gone] at hs; cases hs
    | _ => simpa [Step.apply, hat] using g
  | record =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | holding c =>
        have e : Step.record.apply s = { s with row := some { r with recorded := .session c } } := by
          simp [Step.apply, hr, hat]
        rw [e]
        exact record_good s g r c hat
      | _ => simpa [Step.apply, hr, hat] using g
  | part i reply =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | holding c =>
        have clean : s.fixed.owned = true → s.stray = 0 := sw (by simp [hat])
        cases hs : s.session with
        | some held =>
          simp only [Step.apply, hr, hat, hs]
          split
          · rename_i guard
            obtain ⟨lt, fresh, hf⟩ := guard
            have held_nd := nd held hs
            have kept : ∀ j, j ∈ held → j ∈ receive held i := fun j m => (mem_receive held i j).2 (Or.inl m)
            have bound : ∀ j ∈ receive held i, j < s.total := by
              intro j m
              rcases (mem_receive held i j).1 m with m | rfl
              · exact b held hs j m
              · exact lt
            have acc : ∃ r', some r = some r' ∧ (r'.recorded.isSession = true ∨
                (r'.recorded = .plan true ∧ ∃ c', Attempt.holding c = .holding c')) := by
              obtain ⟨r', hr', h⟩ := a held hs
              rw [hr] at hr'; cases hr'
              refine ⟨r, rfl, ?_⟩
              rcases h with h | ⟨hp, _⟩
              · exact Or.inl h
              · exact Or.inr ⟨hp, c, rfl⟩
            cases reply with
            | confirmed =>
              dsimp only
              refine ⟨?_, ?_, w, ?_, ?_, ?_, ?_, ?_, ?_, by simp, fun _ ho => clean ho, by simp, tk⟩
              · intro held' h; simp at h; subst h; exact receive_nodup held i held_nd
              · intro held' h; simp at h; subst h; exact bound
              · intro c' held' ha h
                simp at ha h; subst ha h
                intro j m
                simp at m
                rcases m with m | e
                · exact kept j (hh c held hat hs j m)
                · exact (mem_receive held i j).2 (Or.inr e)
              · intro c' ha; simp at ha; subst ha; exact snoc_nodup c i (dc c hat) fresh
              · intro r' m held' hr' hm h
                simp at h hr'; subst h hr'
                intro j mj; exact kept j (rc r m held hr hm hs j mj)
              · intro r' m hr' hm; simp at hr'; subst hr'; exact ls r m hr hm
              · intro held' _
                obtain ⟨r', hr', h⟩ := acc
                cases hr'
                refine ⟨r, by simp, ?_⟩
                rcases h with h | ⟨hp, _⟩
                · exact Or.inl h
                · exact Or.inr ⟨hp, c ++ [i], rfl⟩
              · intro ho h; simp [clean ho] at h
            | lost =>
              dsimp only
              refine ⟨?_, ?_, w, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, tk⟩
              · intro held' h; simp at h; subst h; exact receive_nodup held i held_nd
              · intro held' h; simp at h; subst h; exact bound
              · intro c' held' ha h
                simp at ha h; subst ha h
                intro j m; exact kept j (hh c held hat hs j m)
              · intro c' ha; simp at ha; subst ha; exact dc c hat
              · intro r' m held' hr' hm h
                simp at hr' h; subst hr' h
                intro j mj; exact kept j (rc r m held hr hm hs j mj)
              · intro r' m hr' hm; simp at hr'; subst hr'; exact ls r m hr hm
              · intro held' _
                obtain ⟨r', hr', h⟩ := acc
                cases hr'
                exact ⟨r, rfl, h⟩
              · intro ho h; simp [clean ho] at h
              · intro h; simp at h
              · intro _ ho; exact clean ho
              · intro h; simp at h
          · exact g
        | none =>
          simp only [Step.apply, hr, hat, hs]
          split
          · apply failWith_good_of
            · intro held h; simp at h
            · intro held h; simp at h
            · exact w
            · intro r' m held _ _ h; simp at h
            · intro r' m hr' hm
              simp at hr'; subst hr'; simp at hm; subst hm; exact dc _ hat
            · intro held h; simp at h
            · intro ho h; simp [clean ho] at h
            · exact tk
          · exact g
      | _ => simpa [Step.apply, hr, hat] using g
  | finish replied =>
    cases hr : s.row with
    | none => simpa [Step.apply, hr] using g
    | some r =>
      cases hat : s.attempt with
      | holding c =>
        have clean : s.fixed.owned = true → s.stray = 0 := sw (by simp [hat])
        cases hrec : r.recorded with
        | session m =>
          simp only [Step.apply, hr, hat, hrec]
          split
          · rename_i all
            have plain : ∀ held, s.session = some held →
                ∃ r, s.row = some r ∧ r.recorded.isSession = true :=
              fun _ _ => ⟨r, hr, by simp [hrec]⟩
            split
            · exact failWith_good s _ g plain
            · rename_i ht
              cases hp : s.published with
              | some ps =>
                cases hs : s.session with
                | some held =>
                  simp only []
                  rw [← hp, ← hr]
                  exact finish_published_good s g (by simp [hp]) clean
                | none =>
                  simp only []
                  rw [← hp, ← hr]
                  exact finish_published_good s g (by simp [hp]) clean
              | none =>
                cases hs : s.session with
                | some held =>
                  have whole : assemble s.total held = List.range s.total :=
                    assemble_whole s.total held fun i hi =>
                      rc r m held hr hrec hs i (covers_mem s.total m all i hi)
                  simp only [hs, hp]
                  split
                  · refine ⟨by simp, by simp, ?_, by simp, by simp, by simp, fun r' m hr' hm => ls r' m (by rw [hr]; simpa using hr') hm, by simp, ?_, by simp,
                      ?_, by simp, by simp⟩
                    · intro ps h; simp at h; subst h; exact whole
                    · intro ho h; simp [clean ho] at h
                    · intro _ ho; simpa using clean ho
                  · apply failWith_good_of <;> try simp
                    · exact whole
                    · exact fun r' m hr' hm => ls r' m (by rw [hr]; simpa using hr') hm
                    · intro ho h; simp [clean ho] at h
                | none =>
                  simp only [hs, hp]
                  exact failWith_good s _ g plain
          · exact g
        | _ => simpa [Step.apply, hr, hat, hrec] using g
      | _ => simpa [Step.apply, hr, hat] using g
  | complete =>
    simp only [Step.apply]
    split
    · rename_i r hr hat
      have none := fin hat
      have clean : s.fixed.owned = true → s.stray = 0 := sw (by simp [hat])
      exact ⟨by simp [none], by simp [none], w, by simp, by simp, by simp, by simp, by simp [none],
        fun ho h => by simp [clean ho] at h, by simp, by simp, by simp, tk⟩
    · exact g
  | fail c =>
    cases hat : s.attempt with
    | started =>
      simp only [Step.apply, hat]
      exact failWith_good s c g (named_session s g (by simp [hat]))
    | planned =>
      simp only [Step.apply, hat]
      exact failWith_good s c g (named_session s g (by simp [hat]))
    | holding k =>
      simp only [Step.apply, hat]
      have clean : s.fixed.owned = true → s.stray = 0 := sw (by simp [hat])
      apply failWith_good_of
      · exact nd
      · exact b
      · exact w
      · intro r' m held hr' hm hs
        cases hr₀ : s.row with
        | none => simp [hr₀] at hr'
        | some r₀ =>
          simp [hr₀] at hr'; subst hr'; simp at hm; subst hm
          exact hh _ held hat hs
      · intro r' m hr' hm
        cases hr₀ : s.row with
        | none => simp [hr₀] at hr'
        | some r₀ =>
          simp [hr₀] at hr'; subst hr'; simp at hm; subst hm
          exact dc _ hat
      · intro held hs
        obtain ⟨r₀, hr₀, _⟩ := a held hs
        exact ⟨{ r₀ with recorded := .session k }, by simp [hr₀], by simp⟩
      · intro ho h; simp [clean ho] at h
      · exact tk
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
      have hst : s.fixed.owned = true → s.stray = 0 := by
        intro ho
        rcases Nat.eq_zero_or_pos s.stray with h | h
        · exact h
        · obtain ⟨r, hr', _⟩ := nm ho h; rw [hr] at hr'; cases hr'
      simp only
      exact ⟨by simp [hs], by simp [hs], w, by simp, by simp, by simp [hs], by simp, by simp [hs],
        fun ho h => by simp [hst ho] at h, by simp, by simp, by simp, tk⟩
    | some r =>
      cases hs : s.session with
      | none =>
        simp only [hs]
        refine ⟨by simp, by simp, w, by simp, by simp, by simp, ?_, by simp, ?_, by simp,
          by simp, by simp, tk⟩
        · intro r' m hr' hm
          simp at hr'; subst hr'; simp at hm
          exact ls r m hr hm
        · intro ho h
          obtain ⟨r', hr', hp⟩ := nm ho h
          rw [hr] at hr'; cases hr'
          exact ⟨_, rfl, by simp [hp]⟩
      | some held =>
        simp only
        split
        · rename_i hp
          refine ⟨by simp, by simp, w, by simp, by simp, by simp, ?_, by simp, ?_, by simp,
            by simp, by simp, tk⟩
          · intro r' m hr' hm
            simp at hr'; subst hr'; simp [hp] at hm
          · intro _ _; exact ⟨_, rfl, by simp [hp]⟩
        · rename_i hp
          refine ⟨?_, ?_, w, by simp, by simp, ?_, ?_, ?_, ?_, by simp, by simp, by simp, tk⟩
          · intro held' h; simp [hs] at h; subst h; exact nd _ hs
          · intro held' h; simp [hs] at h; subst h; exact b _ hs
          · intro r' m held' hr' hm h
            simp at hr' h; subst hr'; simp at hm
            exact rc r m held' hr hm h
          · intro r' m hr' hm
            simp at hr'; subst hr'; simp at hm
            exact ls r m hr hm
          · intro held' _
            obtain ⟨r₀, hr₀, h₀⟩ := a held hs
            rw [hr] at hr₀; cases hr₀
            refine ⟨_, rfl, Or.inl ?_⟩
            rcases h₀ with h₁ | ⟨h₂, _⟩
            · simpa using h₁
            · exact absurd h₂ hp
          · intro ho h
            obtain ⟨r', hr', hp'⟩ := nm ho h
            rw [hr] at hr'; cases hr'
            exact absurd hp' hp
  | expire =>
    simp only [Step.apply]
    exact ⟨by simp, by simp, w, by simp, dc, by simp, ls, by simp, by simp,
      fun h => ⟨rfl, (pl h).2⟩, by simp, by simp, tk⟩
  | abandon =>
    simp only [Step.apply]
    exact rows_good s (fun r => { r with waited := false }) g (fun _ => rfl)
  | abort result =>
    simp only [Step.apply]
    split
    · rename_i r hr hat
      have plain := named_session s g (by simp [hat])
      split
      · split
        · exact forget_good s r g hat
        · cases result with
          | none => exact forget_good s r g hat
          | some c => exact failWith_good s c g plain
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
        have clean : s.fixed.owned = true → s.stray = 0 := by
          intro ho
          rcases Nat.eq_zero_or_pos s.stray with h | h
          · exact h
          · obtain ⟨r', hr', hp⟩ := nm ho h
            rw [hr] at hr'; cases hr'; rw [guard.2] at hp; cases hp
        exact ⟨by simp [none], by simp [none], w, by simp [none], by simp [hat], by simp, by simp,
          by simp [none], fun ho h => by simp [clean ho] at h, by simp [hat], fun _ ho => clean ho,
          by simp [none], tk⟩
      · exact g
    · exact g
  | rival =>
    simp only [Step.apply]
    split
    · exact ⟨nd, b, w, hh, dc, rc, ls, a, nm, pl, sw, fin, fun _ => rfl⟩
    · exact g
  | endDead =>
    simp only [Step.apply]
    split
    · exact ⟨by simp, by simp, w, by simp, dc, by simp, ls, by simp, by simp,
        fun h => ⟨rfl, (pl h).2⟩, by simp, by simp, tk⟩
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

/-- The confirmed parts the app is shown: `uploads()` and `coven_uploads`
read the recorded session's columns, a plan's showing none yet (§17.9). -/
def State.progress (s : State) : Option (List Nat) := s.row.bind (·.recorded.confirmed)

/-- §17.9: progress is never ahead of the parts the provider's session holds:
a part is recorded only once the provider confirmed it, each once, so the
count shown is never more than the parts held. -/
theorem progress_never_ahead (s : State) (g : Good s) (m : List Nat) (held : List Nat)
    (shown : s.progress = some m) (live : s.session = some held) :
    (∀ i ∈ m, i ∈ held) ∧ m.length ≤ held.length := by
  unfold State.progress at shown
  cases hr : s.row with
  | none => simp [hr] at shown
  | some r =>
    simp [hr] at shown
    cases hrec : r.recorded with
    | session k =>
      simp [hrec] at shown; subst shown
      have sub := g.recorded r k held hr hrec live
      exact ⟨sub, List.Nodup.length_le_of_subset (g.listed r k hr hrec) sub⟩
    | plan => simp [hrec] at shown; subst shown; simp
    | none => simp [hrec] at shown

/-- The parts an attempt counts confirmed are ones the provider holds, each
once, after any crashes, failed attempts and lost replies, so the parts it
sends are only ones it hasn't: a part the provider holds whose reply was lost
goes again and takes its own place. -/
theorem confirmed_held (s : State) (g : Good s) (c : List Nat) (held : List Nat)
    (hat : s.attempt = .holding c) (live : s.session = some held) :
    (∀ i ∈ c, i ∈ held) ∧ c.Nodup :=
  ⟨g.holding c held hat live, g.distinct c hat⟩

/-- The provider's session holds no part twice, and none the file lacks. -/
theorem held_nodup (s : State) (g : Good s) (held : List Nat) (live : s.session = some held) :
    held.Nodup ∧ ∀ i ∈ held, i < s.total :=
  ⟨g.nodup held live, g.bounded held live⟩

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
  obtain ⟨total, fixed, session, stray, published, taken, row, stored, attempt⟩ := s
  cases row <;> cases session <;> simp [Step.apply, strand, State.progress, State.live] <;>
    (try split) <;> simp_all <;> omega

/-- §17.9: after a crash, a session the provider still has continues from
the parts it holds, which are at least the recorded ones, in whatever order
they arrived. -/
theorem resume_continues (s : State) (r : Row) (m : List Nat) (held : List Nat)
    (hr : s.row = some r) (hat : s.attempt = .started) (hrec : r.recorded = .session m)
    (live : s.session = some held) :
    Step.open_.apply s = { s with attempt := .holding held } := by
  simp [Step.apply, hr, hat, hrec, live]

/-- §17.9: a session the provider has expired starts over: the attempt plans
a new one, recorded before the provider begins it; the location, key and
chunk hashes stay. -/
theorem expired_starts_over (s : State) (r : Row) (m : List Nat)
    (hr : s.row = some r) (hat : s.attempt = .started) (hrec : r.recorded = .session m)
    (gone : s.session = none) (unpublished : s.published = none) (open_name : s.taken = false) :
    Step.open_.apply s = replan s r := by
  simp [Step.apply, hr, hat, hrec, gone, unpublished, open_name]

theorem apply_fixed (s : State) (st : Step) :
    (st.apply s).fixed = s.fixed ∧ (st.apply s).total = s.total := by
  cases st <;> simp only [Step.apply, strand] <;> (repeat' split) <;>
    simp [failWith, replan, sweep, forget]

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

/-- §17.9, E.10: the provider begins a session only once the row records its
plan, marked, and with no session live. -/
theorem begin_needs_plan (s : State) (g : Good s) (replied : Bool)
    (moved : (Step.begin replied).apply s ≠ s) :
    s.session = none ∧ ∃ r, s.row = some r ∧ r.recorded = .plan true := by
  simp only [Step.apply] at moved
  split at moved
  · rename_i r hr hat
    split at moved
    · rename_i marked; exact ⟨(g.planned hat).1, r, hr, marked⟩
    · exact absurd rfl moved
  · exact absurd rfl moved

/-- E.10: only the mark marks a plan, from the attempt that recorded it. A
failure, a refused request, a pause or a crash before it leaves the plan
unmarked. -/
theorem only_mark_marks (s : State) (st : Step) (r r' : Row) (hr : s.row = some r)
    (unmarked : r.recorded ≠ .plan true) (after : (st.apply s).row = some r')
    (marked : r'.recorded = .plan true) : st = .mark ∧ s.attempt = .planned := by
  obtain ⟨total, fixed, session, stray, published, taken, row, stored, attempt⟩ := s
  simp only at hr; subst hr
  have unmarked' : r.recorded = .none ∨ r.recorded = .plan false ∨ ∃ m, r.recorded = .session m := by
    cases hrc : r.recorded with
    | none => exact Or.inl rfl
    | plan started => cases started <;> simp_all
    | session m => exact Or.inr (Or.inr ⟨m, rfl⟩)
  rcases unmarked' with hrc | hrc | ⟨m, hrc⟩ <;>
    cases st <;> cases attempt <;> cases session <;> cases published <;>
      simp [Step.apply, failWith, Row.ending, replan, forget, strand, hrc] at after ⊢ <;>
      (repeat' split at after) <;> (try cases ‹Option Class›) <;>
      (try simp only [Option.some.injEq] at after) <;> (try subst after) <;> simp_all

/-- E.10: a begin refused before its request goes out, as when the publication
gate closes after the part's admission, leaves the plan unmarked and the
upload failed by the refusal's class: nothing began, so the row never says a
session may have. With `unmarked_resumes` and `unmarked_aborts`, the upload
then goes on, or is forgotten, at any place, the device's own or shared. -/
theorem refusal_unmarks (s : State) (r : Row) (c : Class) (hr : s.row = some r)
    (hat : s.attempt = .planned) (marked : r.recorded = .plan true) :
    (Step.refuse c).apply s =
      { s with attempt := .idle,
               row := some { r with recorded := .plan false, failure := some c,
                                    wait := c.wait } } := by
  simp [Step.apply, hr, hat, marked, failWith]

/-- Where the place is the device's own, every stray sits at the place of the
marked plan the row records, so the row finds it again. -/
theorem stray_named (s : State) (g : Good s) (owned : s.fixed.owned = true) (h : 0 < s.stray) :
    ∃ r, s.row = some r ∧ r.recorded = .plan true :=
  g.named owned h

/-- The steps that can leave a stray: a lost begin reply, and a crash, which
strands a session the attempt began that its row doesn't name yet. A failure
records the session the attempt holds instead. -/
def Step.strands : Step → Bool
  | .begin replied => !replied
  | .crash => true
  | _ => false

theorem apply_stray_le (s : State) (st : Step) (h : st.strands = false) :
    (st.apply s).stray ≤ s.stray := by
  cases st <;> simp only [Step.strands] at h <;> simp only [Step.apply] <;> (repeat' split) <;>
    simp_all [failWith, replan, sweep, forget] <;> (try (split <;> omega))

theorem steps_stray_le (s : State) (steps : List Step) (h : ∀ st ∈ steps, st.strands = false) :
    (runSteps s steps).stray ≤ s.stray := by
  induction steps generalizing s with
  | nil => exact Nat.le_refl _
  | cons st rest ih =>
    have here := apply_stray_le s st (h st (by simp))
    have later := ih (st.apply s) (fun st' m => h st' (by simp [m]))
    exact Nat.le_trans later here

/-- Where the place is the device's own, the attempt's first request ends
every stray: a resumed row names its session, so it has none, an unmarked plan
has none, and a marked plan's are ended before the new plan is recorded. -/
theorem open_sweeps (s : State) (g : Good s) (r : Row) (hr : s.row = some r)
    (hat : s.attempt = .started) (owned : s.fixed.owned = true) :
    (Step.open_.apply s).stray = 0 := by
  cases hrec : r.recorded with
  | plan started =>
    cases started with
    | true => simp [Step.apply, hr, hat, hrec, owned, replan, sweep]
    | false =>
      have clean := clean_of s g r hr (by simp [hrec]) owned
      simp [Step.apply, hr, hat, hrec, replan, clean]
  | session m =>
    have clean := clean_of s g r hr (by simp [hrec]) owned
    cases hs : s.session <;> cases hp : s.published <;> cases ht : s.taken <;>
      simp [Step.apply, hr, hat, hrec, hs, hp, ht, replan, clean, failWith]
  | none =>
    have clean := clean_of s g r hr (by simp [hrec]) owned
    simp [Step.apply, hr, hat, hrec, replan, clean]

/-- §17.9, E.10: a plan never marked has nothing at its place, so its attempt
records it again and goes on, at any place, the device's own or shared. -/
theorem unmarked_resumes (s : State) (r : Row) (hr : s.row = some r)
    (hat : s.attempt = .started) (unmarked : r.recorded = .plan false) :
    Step.open_.apply s = replan s r := by
  simp [Step.apply, hr, hat, unmarked]

/-- E.10: at a shared place, a marked plan may have begun a session nothing
tells from another device's, so its attempt reads the destination instead of
aborting anything. Finding it empty, it goes on with a fresh unmarked plan at
the same name, never failing for good, and the strays stay. -/
theorem marked_shared_restarts (s : State) (r : Row) (hr : s.row = some r)
    (hat : s.attempt = .started) (marked : r.recorded = .plan true)
    (shared : s.fixed.owned = false) (empty : s.taken = false) :
    Step.open_.apply s = replan s r ∧ (Step.open_.apply s).stray = s.stray ∧
      (Step.open_.apply s).row = some { r with recorded := .plan false } ∧
      (Step.open_.apply s).attempt = .planned := by
  simp [Step.apply, hr, hat, marked, shared, empty, replan]

/-- E.10: finding its own bytes published at the shared name, a marked plan's
attempt completes. -/
theorem marked_shared_taken_own (s : State) (r : Row) (hr : s.row = some r)
    (hat : s.attempt = .started) (marked : r.recorded = .plan true)
    (shared : s.fixed.owned = false) (taken : s.taken = true) (own : s.published.isSome = true) :
    (Step.open_.apply s).attempt = .holding (List.range s.total) := by
  cases hp : s.published with
  | none => simp [hp] at own
  | some _ => simp [Step.apply, hr, hat, marked, shared, taken, hp]

/-- E.10: finding another object at the shared name, a marked plan's attempt
fails as occupied, for the caller to settle. -/
theorem marked_shared_occupied (s : State) (r : Row) (hr : s.row = some r)
    (hat : s.attempt = .started) (marked : r.recorded = .plan true)
    (shared : s.fixed.owned = false) (taken : s.taken = true) (rival : s.published = none) :
    Step.open_.apply s = failWith s .never := by
  simp [Step.apply, hr, hat, marked, shared, taken, rival]

/-- An abort that moves ends the upload's session, and, where the place is the
device's own, every stray; at a shared place the strays stay, as nothing tells
them from another device's sessions. -/
theorem abort_sweeps (s : State) (moved : (Step.abort none).apply s ≠ s) :
    ((Step.abort none).apply s).session = none ∧
      (s.fixed.owned = true → ((Step.abort none).apply s).stray = 0) := by
  cases hr : s.row with
  | none => simp [Step.apply, hr] at moved
  | some r =>
    cases hat : s.attempt with
    | idle =>
      by_cases guard : r.waited = false ∧ r.recorded ≠ Record.none ∧ r.wait = .ready
      · by_cases cond : r.recorded = .plan false ∨ (r.recorded = .plan true ∧ s.fixed.owned = false)
        · simp [Step.apply, hr, hat, guard, cond, forget]
          intro ho hf; rw [ho] at hf; cases hf
        · simp [Step.apply, hr, hat, guard, cond, forget]
          intro ho hf; rw [ho] at hf; cases hf
      · simp [Step.apply, hr, hat, guard] at moved
    | _ => simp [Step.apply, hr, hat] at moved

/-- §17.9, E.10: an upload no file waits for whose plan was never marked is
forgotten at any place, whatever the provider would answer: nothing is at its
place, so its abort sends nothing. -/
theorem unmarked_aborts (s : State) (r : Row) (result : Option Class) (hr : s.row = some r)
    (idle : s.attempt = .idle) (abandoned : r.waited = false) (ready : r.wait = .ready)
    (unmarked : r.recorded = .plan false) : (Step.abort result).apply s = forget s r := by
  simp [Step.apply, hr, idle, abandoned, ready, unmarked]

/-! ## Retiring an upload no file waits for -/

/-- §17.9: an upload retires with no session left at the provider, named or
stray. -/
theorem retired_no_session (s : State) (g : Good s) (gone : s.row = none) :
    s.session = none ∧ (s.fixed.owned = true → s.stray = 0) := by
  constructor
  · cases hs : s.session with
    | none => rfl
    | some held =>
      obtain ⟨r, hr, _⟩ := g.accounted held hs
      rw [gone] at hr; cases hr
  · intro ho
    rcases Nat.eq_zero_or_pos s.stray with h | h
    · exact h
    · obtain ⟨r, hr, _⟩ := g.named ho h
      rw [gone] at hr; cases hr

/-- Only a retire removes the row of an upload no file waits for, and only
once it records nothing. -/
theorem retire_needs_no_session (s : State) (r : Row) (hr : s.row = some r)
    (gone : (Step.retire.apply s).row = none) : r.waited = false ∧ r.recorded = .none := by
  obtain ⟨total, fixed, session, stray, published, taken, row, stored, attempt⟩ := s
  simp only at hr; subst hr
  cases attempt <;> simp [Step.apply] at gone
  split at gone
  · assumption
  · simp at gone

/-- §17.9: what a row records is forgotten only by aborting it, which ends the
provider's session and, where the place is the device's own, every stray
first. -/
theorem forgotten_by_abort (s : State) (st : Step) (r r' : Row)
    (hr : s.row = some r) (hrec : r.recorded ≠ .none)
    (after : (st.apply s).row = some r') (forgot : r'.recorded = .none) :
    (∃ result, st = .abort result) ∧ (st.apply s).session = none ∧
      (s.fixed.owned = true → (st.apply s).stray = 0) := by
  obtain ⟨total, fixed, session, stray, published, taken, row, stored, attempt⟩ := s
  simp only at hr; subst hr
  cases hrc : r.recorded
  · exact absurd hrc hrec
  all_goals
  cases st <;> cases attempt <;> cases session <;> cases published <;>
    simp [Step.apply, failWith, Row.ending, replan, sweep, forget, strand, hrc] at after ⊢ <;>
    (repeat' split at after) <;> (repeat' split) <;> (try cases ‹Option Class›) <;>
    (try simp only [Option.some.injEq] at after) <;> (try subst after) <;> simp_all

/-- §17.9: a failed abort is the upload's failure: the upload stays queued
with what it records, and waits as its class says. Only an abort that sends a
request fails so: one of a recorded session, or of a marked plan at the
device's own place. -/
theorem failed_abort_queued (s : State) (r : Row) (c : Class) (hr : s.row = some r)
    (idle : s.attempt = .idle) (abandoned : r.waited = false) (held : r.recorded ≠ .none)
    (ready : r.wait = .ready) (marked : r.recorded ≠ .plan false)
    (owned : r.recorded = .plan true → s.fixed.owned = true) :
    (Step.abort (some c)).apply s =
      { s with attempt := .idle, row := some { r with failure := some c, wait := c.wait } } := by
  have sends : ¬(r.recorded = .plan true ∧ s.fixed.owned = false) := by
    rintro ⟨h, shared⟩; rw [owned h] at shared; cases shared
  simp [Step.apply, hr, idle, abandoned, held, ready, marked, sends, failWith]

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
  obtain ⟨total, fixed, session, stray, published, taken, row, stored, attempt⟩ := s
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
  obtain ⟨total, fixed, session, stray, published, taken, row, stored, attempt⟩ := s
  simp only at hr idle; subst hr idle
  cases st <;> simp [Step.apply, Row.ending, forever, Wait.due, strand] <;> (repeat' split) <;>
    simp_all

/-- No step brings back the row of an upload that completed or retired, or
publishes anything for it. -/
theorem gone_stays (s : State) (st : Step) (gone : s.row = none) :
    (st.apply s).row = none ∧ (st.apply s).published = s.published := by
  obtain ⟨total, fixed, session, stray, published, taken, row, stored, attempt⟩ := s
  simp only at gone; subst gone
  cases st <;> cases attempt <;> simp [Step.apply, failWith, strand] <;> (try split) <;> simp

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
records it, or records a plan, marks it and begins the session; it sends each
part and records it, publishes, and completes. What the provider does to the
run, or the app's pause, is its `Fault`. -/

/-- What happens to the part after the run's `k` confirmed ones: its reply is
lost, it is dropped, or the process stops while sending it. A lost or dropped
part fails the attempt. -/
inductive PartFault where
  | lost
  | dropped
  | crash
  deriving DecidableEq, Repr

/-- What the provider does to one run: nothing; expire its sessions before the
run's first request; fail that request with a class; refuse the begin of a
planned session with a class before its request goes out, as a publication
gate that closed after the part's admission does; lose the reply to that
begin; act on the part after `k` confirmed ones; or lose the completion
reply. Or the app pauses uploads as the attempt records its plan, so it stops
before its first part. -/
inductive Fault where
  | none
  | expire
  | fail (c : Class)
  | refuse (c : Class)
  | loseBegin
  | afterParts (k : Nat) (then_ : PartFault)
  | loseCompletion
  | pause
  deriving DecidableEq, Repr

/-- These parts, each recorded once confirmed. -/
def partSteps : List Nat → List Step
  | [] => []
  | i :: rest => .part i .confirmed :: .record :: partSteps rest

/-- The parts and publication of an attempt holding a session: the parts it
hasn't confirmed, in index order. -/
def partsPlan (s : State) (fault : Fault) : List Step :=
  match s.attempt with
  | .holding c =>
    let remaining := (List.range s.total).filter fun i => decide (i ∉ c)
    let finishing := partSteps remaining ++ [.finish (fault ≠ .loseCompletion), .complete]
    match fault with
    | .afterParts k pf =>
      match remaining.drop k with
      | i :: _ =>
        partSteps (remaining.take k) ++
          (match pf with
           | .lost => [.part i .lost, .fail .automatic]
           | .dropped => [.fail .automatic]
           | .crash => [])
      | [] => finishing
    | _ => finishing
  | _ => []

/-- What follows the attempt's first request: a resumed session is recorded
as the provider holds it; a planned one is marked and begun, unless a pause
stops the attempt first. The begin is the first request of an attempt whose
open sent none, so a failure meets it there, the plan already marked; one
refused before its request goes out unmarks the plan again. -/
def openedPlan (s : State) (fault : Fault) : List Step :=
  match s.attempt with
  | .planned =>
    match fault with
    | .pause => [.pause]
    | .refuse c => [.mark, .refuse c]
    | .loseBegin => [.mark, .begin false]
    | .fail c => [.mark, .fail c]
    | _ => .mark :: .begin true :: partsPlan (runSteps s [.mark, .begin true]) fault
  | .holding _ => .record :: partsPlan (Step.record.apply s) fault
  | _ => []

/-- Whether opening what the row records sends a request: not for an unmarked
plan, with nothing at its place; a marked plan reads its destination, at a
shared place as at the device's own. -/
def Record.requested : Record → Bool
  | .plan false => false
  | _ => true

/-- Whether aborting what the row records sends a request: not for an unmarked
plan, with nothing at its place, nor for a marked one at a shared place, which
ends nothing and forgets the plan. -/
def Record.aborts (owned : Bool) : Record → Bool
  | .plan false => false
  | .plan true => owned
  | _ => true

/-- The steps of an attempt that has just started. -/
def attemptPlan (s : State) (fault : Fault) : List Step :=
  if s.row.all (·.recorded.requested) then
    match fault with
    | .fail c => [.fail c]
    | _ =>
      let opening := (if fault = .expire then [Step.expire] else []) ++ [.open_]
      opening ++ openedPlan (runSteps s opening) fault
  else .open_ :: openedPlan (Step.open_.apply s) fault

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
        (if r.recorded.aborts s.fixed.owned then
          (if fault = .expire then [.expire] else []) ++
            [.abort (match fault with | .fail c => some c | _ => none)]
        else [.abort none]) ++ [.retire]
      else [.retire]
    | _, _ => []
  steps ++ (match fault with | .afterParts _ .crash => [.crash] | _ => [])

def run (s : State) (forced : Bool) (fault : Fault) : State := runSteps s (plan s forced fault)

/-- Every run keeps the invariant: it is a sequence of steps. -/
theorem run_good (s : State) (forced : Bool) (fault : Fault) (g : Good s) :
    Good (run s forced fault) :=
  steps_good s _ g

theorem partSteps_keep (k : List Nat) : ∀ st ∈ partSteps k, st.strands = false := by
  induction k with
  | nil => simp [partSteps]
  | cons i k ih =>
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
  · simp only [List.mem_cons] at h
    rcases h with rfl | rfl | h
    · rfl
    · rfl
    · exact partsPlan_keep _ st h
  · simp only [List.mem_cons] at h
    rcases h with rfl | h
    · rfl
    · exact partsPlan_keep _ st h
  · simp at h

theorem attemptPlan_keep (s : State) : ∀ st ∈ attemptPlan s .none, st.strands = false := by
  intro st h
  unfold attemptPlan at h
  split at h
  · simp only [reduceCtorEq, ite_false, List.nil_append, List.singleton_append,
      List.mem_cons] at h
    rcases h with rfl | h
    · rfl
    · exact openedPlan_keep _ st h
  · simp only [List.mem_cons] at h
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
      · split at h <;> simp at h <;> rcases h with rfl | rfl <;> rfl
      · simp at h; subst h; rfl
  · simp at h

/-- §17.9: whatever a cut begin left, where the place is the device's own,
or the plan unmarked, the upload's next run on a provider that fails nothing
ends it: with a file waiting, its first request ends every stray; with none,
its abort does. At a shared place the strays stay: they are another device's
to tell apart from its own, and each is ended once its name is taken
(`endDead`, §17.10). -/
theorem next_run_sweeps (s : State) (g : Good s) (forced : Bool) (r : Row)
    (hr : s.row = some r) (idle : s.attempt = .idle) (ready : r.wait = .ready)
    (owned : s.fixed.owned = true) :
    (run s forced .none).stray = 0 := by
  rcases Nat.eq_zero_or_pos s.stray with h0 | hpos
  · have := steps_stray_le s (plan s forced .none) (plan_none_keep s forced)
    unfold run; omega
  · obtain ⟨r', hr', hplan⟩ := g.named owned hpos
    rw [hr] at hr'; cases hr'
    cases hw : r.waited with
    | false =>
      simp [run, plan, hr, idle, hw, ready, hplan, owned, Record.requested, runSteps, Step.apply,
        forget]
    | true =>
      have opened : plan s forced .none = Step.start forced :: Step.open_ ::
          openedPlan (Step.open_.apply ((Step.start forced).apply s)) .none := by
        simp [plan, hr, idle, hw, ready, Wait.due, attemptPlan, Step.apply, hplan, owned,
          Record.requested, Record.aborts, runSteps]
      have swept : (Step.open_.apply ((Step.start forced).apply s)).stray = 0 :=
        open_sweeps _ (apply_good s _ g) r (by simp [Step.apply, hr, idle, hw, ready, Wait.due])
          (by simp [Step.apply, hr, idle, hw, ready, Wait.due])
          (by simp [Step.apply, hr, idle, hw, ready, Wait.due, owned])
      have keep := steps_stray_le (Step.open_.apply ((Step.start forced).apply s))
        (openedPlan (Step.open_.apply ((Step.start forced).apply s)) .none) (openedPlan_keep _)
      unfold run
      rw [opened]
      show (runSteps (Step.open_.apply ((Step.start forced).apply s)) _).stray = 0
      omega

/-- E.10: once the name is taken, a device that applied it ends every session
there, the strays and this upload's own if its bytes are not the ones
published; what is published does not change. -/
theorem end_dead_harmless (s : State) :
    (Step.endDead.apply s).published = s.published ∧
      (s.taken = true → (Step.endDead.apply s).stray = 0) := by
  simp only [Step.apply]
  split <;> simp_all

/-- E.10: after the name is taken, no strays remain once the next run is
done: a device that applied the name ends them, and the run adds none. -/
theorem ended_dead_run_sweeps (s : State) (forced : Bool) (taken : s.taken = true) :
    (run (Step.endDead.apply s) forced .none).stray = 0 := by
  have z := (end_dead_harmless s).2 taken
  have := steps_stray_le (Step.endDead.apply s) (plan (Step.endDead.apply s) forced .none)
    (plan_none_keep _ forced)
  unfold run; omega

/-- E.10: once something is published at a place, no step changes what is
published there: a completion there is refused. -/
theorem taken_refuses_others (s : State) (st : Step) (taken : s.taken = true) :
    (st.apply s).published = s.published := by
  cases st <;> simp only [Step.apply, strand] <;> (repeat' split) <;>
    simp_all [failWith, replan, forget, sweep]

theorem taken_stays (s : State) (st : Step) (taken : s.taken = true) :
    (st.apply s).taken = true := by
  cases st <;> simp only [Step.apply, strand] <;> (repeat' split) <;>
    simp_all [failWith, replan, forget, sweep]

/-- E.10, the decision's theorem: the published object is the assembly of the
parts of the session the row names, and no step publishes anything else. A
stray's parts are never published: no step publishes from one. -/
theorem stray_never_published (s : State) (st : Step)
    (changed : (st.apply s).published ≠ s.published) :
    ∃ r m held, s.row = some r ∧ r.recorded = .session m ∧ s.session = some held ∧
      covers s.total m = true ∧ s.published = none ∧
      (st.apply s).published = some (assemble s.total held) := by
  cases st <;> simp only [Step.apply, strand] at changed ⊢ <;> (repeat' split at changed) <;>
    simp_all [failWith, replan, forget, sweep]

end CovenStorage.UploadSession
