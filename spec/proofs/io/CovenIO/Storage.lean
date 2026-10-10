import CovenStorage.Objects

/-! §4: exact paths, create-once objects, replacement revisions, scoped reads.
Numbers and storage times are naturals; one time unit is one second. -/
namespace CovenIO

abbrev Bytes := CovenStorage.Bytes

inductive Kind where
  | entry | write | copy
  deriving DecidableEq, Repr

inductive Path where
  | log (kind : Kind) (writer number : Nat)
  | snapshot (audience writer number : Nat)
  | positions (writer : Nat)
  | clock (writer : Nat)
  | file (writer id : Nat)
  deriving DecidableEq, Repr

def Path.writer : Path → Nat
  | .log _ w _ | .snapshot _ w _ | .positions w | .clock w | .file w _ => w

structure Object where
  value : CovenStorage.Object
  revision : Nat
  deriving DecidableEq, Repr

/-- Exact paths stand for provider object ids. Metadata never carries the
body; equal size and time still require comparing the revision. -/
structure Metadata where
  size : Nat
  storedAt : Nat
  revision : Nat
  deriving DecidableEq, Repr

def Object.metadata (o : Object) : Metadata :=
  ⟨o.value.bytes.length, o.value.storedAt, o.revision⟩

abbrev Store := Path → Option Object

def empty : Store := fun _ => none

def put (s : Store) (p : Path) (b : Bytes) (time : Nat) : Store :=
  fun q => if q = p then match s p with
    | none => some ⟨⟨b, time⟩, 0⟩
    | some o => some o
  else s q

def replace (s : Store) (p : Path) (b : Bytes) (time : Nat) : Store :=
  fun q => if q = p then
    some ⟨⟨b, time⟩, match s p with | none => 0 | some o => o.revision + 1⟩
  else s q

def erase (s : Store) (p : Path) : Store := fun q => if q = p then none else s q

theorem create_once (s : Store) (p q : Path) (b : Bytes) (t : Nat)
    (o : Object) (h : s q = some o) : put s p b t q = some o := by
  by_cases eq : q = p
  · subst q; simp [put, h]
  · simp [put, eq, h]

theorem create_visible (s : Store) (p : Path) (b : Bytes) (t : Nat)
    (h : s p = none) : put s p b t p = some ⟨⟨b, t⟩, 0⟩ := by simp [put, h]

theorem replacement_revision (s : Store) (p : Path) (b : Bytes) (t : Nat)
    (o : Object) (h : s p = some o) :
    replace s p b t p = some ⟨⟨b, t⟩, o.revision + 1⟩ := by simp [replace, h]

inductive Folder where
  | deviceEntries | positions | snapshots (audience : Nat) | files (writer : Nat)
  deriving DecidableEq, Repr

def Folder.contains : Folder → Path → Bool
  | .deviceEntries, .log .entry _ 1 => true
  | .positions, .positions _ => true
  | .snapshots a, .snapshot b _ _ => a == b
  | .files a, .file b _ => a == b
  | _, _ => false

inductive Request where
  | list (folder : Folder)
  | get (path : Path)
  | status (path : Path)
  | range (path : Path) (offset length : Nat)
  | create (path : Path) (bytes : Bytes)
  | replace (path : Path) (bytes : Bytes)
  | delete (path : Path)
  | beginUpload (writer file : Nat)
  | uploadPart (writer file : Nat) (bytes : Bytes)
  | finishUpload (writer file : Nat)
  deriving DecidableEq, Repr

def Request.names (writer : Nat) : Request → Bool
  | .list (.files w) => w == writer
  | .list _ => false
  | .beginUpload w _ | .uploadPart w _ _ | .finishUpload w _ => w == writer
  | .get p | .status p | .range p _ _ | .create p _ | .replace p _ | .delete p =>
      p.writer == writer

inductive Failure where
  | network | storage | disk | database | protocol
  deriving DecidableEq, Repr

inductive Response where
  | object (value : Option Object)
  | status (value : Option Metadata)
  | bytes (value : Option Bytes)
  | listing (objects : List (Path × Metadata))
  | published (value : Metadata)
  | occupied (value : Metadata)
  | deleted
  | failure (reason : Failure)
  deriving DecidableEq, Repr

/-- `paths` is the provider's finite namespace at this instant. Pagination
must finish before this result can be used as a complete observation. -/
def observe (s : Store) (paths : List Path) : Request → Response
  | .get p => .object (s p)
  | .status p => .status ((s p).map Object.metadata)
  | .range p offset length => .bytes ((s p).map fun o => (o.value.bytes.drop offset).take length)
  | .list folder => .listing (paths.filterMap fun p =>
      if folder.contains p then (s p).map (fun o => (p, o.metadata)) else none)
  | _ => .failure .protocol

theorem scoped_listing (s : Store) (paths : List Path) (f : Folder) (p : Path) (o : Metadata)
    (h : (p, o) ∈ paths.filterMap (fun q =>
      if f.contains q then (s q).map (fun value => (q, value.metadata)) else none)) : f.contains p = true := by
  obtain ⟨q, _, h⟩ := List.mem_filterMap.mp h
  split at h
  · rename_i yes
    cases hs : s q <;> simp_all
  · simp at h

theorem status_is_exact (s : Store) (paths : List Path) (p : Path) :
    observe s paths (.status p) = .status ((s p).map Object.metadata) := rfl

def permanent : Path → Bool
  | .log .entry _ _ | .log .copy _ _ => true
  | _ => false

def replaceable : Path → Bool
  | .positions _ | .clock _ => true
  | _ => false

structure Provider where
  objects : Store
  time : Nat

/-- Legal provider mutations. Retention eligibility is proved separately;
even this more permissive deletion relation cannot delete permanent logs. -/
inductive ProviderStep : Provider → Provider → Prop
  | tick (s : Provider) (time : Nat) (later : s.time ≤ time) :
      ProviderStep s { s with time := time }
  | create (s : Provider) (p : Path) (bytes : Bytes) :
      ProviderStep s { s with objects := put s.objects p bytes s.time }
  | replace (s : Provider) (p : Path) (bytes : Bytes) (allowed : replaceable p = true) :
      ProviderStep s { s with objects := replace s.objects p bytes s.time }
  | delete (s : Provider) (p : Path) (allowed : permanent p = false) :
      ProviderStep s { s with objects := erase s.objects p }

/-- Serve one request at the provider's current storage time. Failures before
arrival are separate execution outcomes; a lost reply does not undo arrival. -/
def serve (s : Provider) (paths : List Path) (request : Request) : Provider × Response :=
  match request with
  | .create p bytes => match s.objects p with
      | some o => (s, .occupied o.metadata)
      | none => ({ s with objects := put s.objects p bytes s.time },
          .published ⟨bytes.length, s.time, 0⟩)
  | .replace p bytes =>
      if replaceable p then
        ({ s with objects := replace s.objects p bytes s.time },
          .published ⟨bytes.length, s.time,
            match s.objects p with | none => 0 | some o => o.revision + 1⟩)
      else (s, .failure .protocol)
  | .delete p =>
      if permanent p then (s, .failure .protocol)
      else ({ s with objects := erase s.objects p }, .deleted)
  | _ => (s, observe s.objects paths request)

theorem serve_get (s : Provider) (paths : List Path) (p : Path) :
    serve s paths (.get p) = (s, .object (s.objects p)) := rfl

theorem serve_occupied (s : Provider) (paths : List Path) (p : Path) (bytes : Bytes)
    (object : Object) (present : s.objects p = some object) :
    serve s paths (.create p bytes) = (s, .occupied object.metadata) := by
  simp [serve, present]

theorem served_transition (s : Provider) (paths : List Path) (request : Request) :
    (serve s paths request).1 = s ∨ ProviderStep s (serve s paths request).1 := by
  cases request with
  | create p bytes =>
      cases found : s.objects p with
      | none => exact Or.inr (by simpa [serve, found] using ProviderStep.create s p bytes)
      | some object => exact Or.inl (by simp [serve, found])
  | replace p bytes =>
      cases allowed : replaceable p with
      | false => exact Or.inl (by simp [serve, allowed])
      | true => exact Or.inr (by simpa [serve, allowed] using ProviderStep.replace s p bytes allowed)
  | delete p =>
      cases allowed : permanent p with
      | true => exact Or.inl (by simp [serve, allowed])
      | false => exact Or.inr (by simpa [serve, allowed] using ProviderStep.delete s p allowed)
  | list | get | status | range | beginUpload | uploadPart | finishUpload => exact Or.inl rfl

theorem permanent_step {a b : Provider} (step : ProviderStep a b) (p : Path) (o : Object)
    (fixed : permanent p = true) (found : a.objects p = some o) : b.objects p = some o := by
  cases step with
  | tick => exact found
  | create q bytes => exact create_once _ _ _ _ _ _ found
  | replace q bytes allowed =>
      have different : p ≠ q := by
        intro eq; subst q; cases p <;> simp_all [permanent, replaceable]
      simp [replace, different, found]
  | delete q allowed =>
      have different : p ≠ q := by intro eq; subst q; simp_all
      simp [erase, different, found]

theorem storage_time_monotone {a b : Provider} (step : ProviderStep a b) : a.time ≤ b.time := by
  cases step with
  | tick _ later => exact later
  | create | replace | delete => exact Nat.le_refl _

inductive ProviderRun : Provider → Provider → Prop
  | refl (s : Provider) : ProviderRun s s
  | step {a b c : Provider} : ProviderRun a b → ProviderStep b c → ProviderRun a c

theorem permanent_retained {a b : Provider} (run : ProviderRun a b) (p : Path) (o : Object)
    (fixed : permanent p = true) (found : a.objects p = some o) : b.objects p = some o := by
  induction run with
  | refl => exact found
  | step _ step ih => exact permanent_step step p o fixed ih

/-- Immutable objects with earlier timestamps cannot first appear in a
later provider transition, even if many transitions share a timestamp. -/
theorem old_immutable_step {a b : Provider} (step : ProviderStep a b) (p : Path) (o : Object)
    (fixed : replaceable p = false) (found : b.objects p = some o)
    (older : o.value.storedAt < a.time) : a.objects p = some o := by
  cases step with
  | tick => exact found
  | create q bytes =>
      by_cases same : p = q
      · subst q
        cases previous : a.objects p with
        | some old => simpa [put, previous] using found
        | none =>
            have eq : o = ⟨⟨bytes, a.time⟩, 0⟩ := by simpa [put, previous] using found.symm
            subst o; simp at older
      · simpa [put, same] using found
  | replace q bytes allowed =>
      have different : p ≠ q := by
        intro eq; subst q; simp_all
      simpa [replace, different] using found
  | delete q allowed =>
      by_cases same : p = q
      · subst q; simp [erase] at found
      · simpa [erase, same] using found

theorem provider_time_grows {a b : Provider} (run : ProviderRun a b) : a.time ≤ b.time := by
  induction run with
  | refl => exact Nat.le_refl _
  | step _ step ih => exact Nat.le_trans ih (storage_time_monotone step)

theorem old_immutable_present {a b : Provider} (run : ProviderRun a b) (p : Path) (o : Object)
    (fixed : replaceable p = false) (found : b.objects p = some o)
    (older : o.value.storedAt < a.time) : a.objects p = some o := by
  induction run with
  | refl => exact found
  | step previous step ih =>
      exact ih (old_immutable_step step p o fixed found
        (Nat.lt_of_lt_of_le older (provider_time_grows previous)))

structure CopyPrefix where
  audience : Nat
  key : Nat
  recipient : Nat
  deriving DecidableEq, Repr

/-- §11: exposure uses the clear routing facts, independently of opening. -/
def exposed (copies : List CopyPrefix) (audience key : Nat) (eligible : Nat → Bool) : Bool :=
  copies.any fun c => c.audience == audience && c.key == key && !eligible c.recipient

theorem exposure_survives (a b : List CopyPrefix) (audience key : Nat) (eligible : Nat → Bool)
    (h : exposed a audience key eligible = true) : exposed (a ++ b) audience key eligible = true := by
  change (a ++ b).any _ = true
  rw [List.any_append]
  change a.any _ = true at h
  rw [h]
  rfl

end CovenIO
