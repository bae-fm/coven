import Std

namespace CovenStorage

/-- D10, within one store. Snapshot audiences have independent paths. -/
inductive Path where
  | write (device number : Nat)
  | entry (device number : Nat)
  | snapshot (audience device number : Nat)
  | file (device id : Nat)
  deriving DecidableEq, Repr

/-- Exact symbolic bytes; encryption and encoding are outside this model. -/
abbrev Bytes := List Nat

structure Object where
  bytes : Bytes
  storedAt : Nat
  deriving DecidableEq, Repr

abbrev Storage := Path → Option Object

def emptyStorage : Storage := fun _ => none

/-- §4: publication is atomic; even a different retry cannot overwrite. -/
def create (s : Storage) (p : Path) (bytes : Bytes) (time : Nat) : Storage :=
  fun q => if q = p then
    match s p with
    | none => some ⟨bytes, time⟩
    | some o => some o
  else s q

theorem create_preserves (s : Storage) (p q : Path) (b : Bytes) (t : Nat)
    (o : Object) (h : s q = some o) : create s p b t q = some o := by
  by_cases he : q = p
  · subst q; simp [create, h]
  · simp [create, he, h]

theorem create_publishes (s : Storage) (p : Path) (b : Bytes) (t : Nat)
    (h : s p = none) : create s p b t p = some ⟨b, t⟩ := by
  simp [create, h]

theorem retry_preserves_time (s : Storage) (p : Path) (b : Bytes) (t u : Nat) :
    create (create s p b t) p b u = create s p b t := by
  funext q
  by_cases h : q = p
  · subst q; cases hs : s p <;> simp [create, hs]
  · simp [create, h]

structure Publication where
  path : Path
  bytes : Bytes
  time : Nat
  deriving DecidableEq, Repr

def publishAll (s : Storage) (requests : List Publication) : Storage :=
  requests.foldl (fun s r => create s r.path r.bytes r.time) s

theorem stored_history_preserved (s : Storage) (requests : List Publication)
    (p : Path) (o : Object) (h : s p = some o) :
    publishAll s requests p = some o := by
  induction requests generalizing s with
  | nil => exact h
  | cons r rs ih => exact ih _ (create_preserves s r.path p r.bytes r.time o h)

inductive ReadFailure where
  | network | permission | noStorage
  deriving DecidableEq, Repr

inductive Settlement where
  | stored
  | pending (failure : ReadFailure)
  | absent
  | reset
  deriving DecidableEq, Repr

/-- §6, §10: an occupied path is not an acknowledgement. -/
def compareOccupied (expected : Bytes) : Except ReadFailure (Option Object) → Settlement
  | .error e => .pending e
  | .ok none => .absent
  | .ok (some o) => if o.bytes = expected then .stored else .reset

theorem settled_iff_equal (b : Bytes) (r : Except ReadFailure (Option Object)) :
    compareOccupied b r = .stored ↔ ∃ o, r = .ok (some o) ∧ o.bytes = b := by
  cases r with
  | error e => simp [compareOccupied]
  | ok r => cases r with
    | none => simp [compareOccupied]
    | some o => simp [compareOccupied]

/-- §6, D11: a chunk's nonce inputs. This is not an HMAC implementation. -/
structure NonceContext where
  key : Nat
  path : Path
  sectionIndex : Nat
  index : Nat
  deriving DecidableEq, Repr

structure Draft where
  path : Path
  plaintext : Bytes
  deriving DecidableEq, Repr

/-- A fixed first attempt, including the format/prefix covered by authentication. -/
structure Attempt where
  draft : Draft
  key : Nat
  format : Nat
  deriving DecidableEq, Repr

def Attempt.nonce (a : Attempt) (sectionIndex index : Nat) : NonceContext :=
  ⟨a.key, a.draft.path, sectionIndex, index⟩

inductive Upload where
  | untried (draft : Draft)
  | tried (attempt : Attempt)
  deriving DecidableEq, Repr

/-- §17.1 permits changing only untried plaintext, preserving its identity. -/
def convert (f : Bytes → Bytes) : Upload → Upload
  | .untried d => .untried { d with plaintext := f d.plaintext }
  | .tried a => .tried a

/-- §6, §17.2: the first attempt fixes the key and format; a retry ignores
new choices. The returned attempted state commits before encryption. -/
def prepare (key format : Nat) : Upload → Attempt
  | .untried d => ⟨d, key, format⟩
  | .tried a => a

theorem retry_fixed (a : Attempt) (key format : Nat) (f : Bytes → Bytes) :
    prepare key format (convert f (.tried a)) = a := rfl

theorem fresh_path_separates_nonces (a b : Attempt) (s i t j : Nat)
    (h : a.draft.path ≠ b.draft.path) : a.nonce s i ≠ b.nonce t j := by
  intro he
  exact h (congrArg NonceContext.path he)

/-- §6: one writer's first attempts assign each path only once. Rollback of
the durable attempt record is deliberately not assumed to preserve this. -/
def OneAssignment (attempts : List Attempt) : Prop :=
  ∀ a ∈ attempts, ∀ b ∈ attempts, a.draft.path = b.draft.path → a = b

theorem one_writer_nonce_safe (attempts : List Attempt) (h : OneAssignment attempts)
    (a b : Attempt) (ha : a ∈ attempts) (hb : b ∈ attempts) (s i t j : Nat)
    (he : a.nonce s i = b.nonce t j) : a = b :=
  h a ha b hb (congrArg NonceContext.path he)

theorem fresh_assignment_preserved (attempts : List Attempt) (a : Attempt)
    (h : OneAssignment attempts)
    (fresh : ∀ b ∈ attempts, a.draft.path ≠ b.draft.path) :
    OneAssignment (a :: attempts) := by
  intro x hx y hy he
  simp only [List.mem_cons] at hx hy
  rcases hx with hx | hx
  · subst x
    rcases hy with hy | hy
    · exact hy.symm
    · exact False.elim (fresh y hy he)
  · rcases hy with hy | hy
    · subst y; exact False.elim (fresh x hx he.symm)
    · exact h x hx y hy he

theorem retry_assignment_preserved (attempts : List Attempt) (a : Attempt)
    (h : OneAssignment attempts) (ha : a ∈ attempts) :
    OneAssignment (a :: attempts) := by
  intro x hx y hy he
  have hx' : x ∈ attempts := by rcases List.mem_cons.mp hx with rfl | hx; exact ha; exact hx
  have hy' : y ∈ attempts := by rcases List.mem_cons.mp hy with rfl | hy; exact ha; exact hy
  exact h x hx' y hy' he

/-- Every first assignment has a fresh path, and retries use a retained
attempt. This history excludes database rollback, but allows any interleaving. -/
inductive DurableAttempts : List Attempt → Prop where
  | empty : DurableAttempts []
  | first {history : List Attempt} (a : Attempt) : DurableAttempts history →
      (∀ b ∈ history, a.draft.path ≠ b.draft.path) → DurableAttempts (a :: history)
  | retry {history : List Attempt} (a : Attempt) : DurableAttempts history →
      a ∈ history → DurableAttempts (a :: history)

theorem durable_attempts_nonce_safe (history : List Attempt) (h : DurableAttempts history) :
    OneAssignment history := by
  induction h with
  | empty => intro a ha; simp at ha
  | first a _ fresh ih => exact fresh_assignment_preserved _ a ih fresh
  | retry a _ member ih => exact retry_assignment_preserved _ a ih member

end CovenStorage
