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

/-- Key and chunk position within D11's content-bound nonce inputs. -/
structure NoncePosition where
  key : Nat
  path : Path
  sectionIndex : Nat
  index : Nat
  deriving DecidableEq, Repr

/-- D11's nonce inputs for one chunk. Plaintext stands for its
SHA-256 digest in the collision-free abstraction; prefix includes the format
and every cleartext routing byte. Actual hashing and HMAC are not implemented. -/
structure NonceContext where
  position : NoncePosition
  cleartext : Bytes
  plaintext : Bytes
  deriving DecidableEq, Repr

/-- No premise about paths, durable counters, rollback or live copies. -/
theorem nonce_exclusivity (a b : NonceContext) (same : a = b) :
    a.plaintext = b.plaintext ∧ a.cleartext = b.cleartext :=
  ⟨congrArg NonceContext.plaintext same, congrArg NonceContext.cleartext same⟩

theorem different_plaintext_separates_nonces (a b : NonceContext)
    (different : a.plaintext ≠ b.plaintext) : a ≠ b :=
  fun same => different (nonce_exclusivity a b same).1

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

/-- Restore histories represent one affected chunk. Its abstract cleartext
prefix is the format; arbitrary complete prefixes are covered above. -/
def Attempt.nonce (a : Attempt) (sectionIndex index : Nat) : NonceContext :=
  ⟨⟨a.key, a.draft.path, sectionIndex, index⟩, [a.format], a.draft.plaintext⟩

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
  exact h (congrArg (fun n => n.position.path) he)

/-- Any two chunk attempts, including attempts absent from every surviving
database, have equal plaintext when their content-bound nonce contexts agree. -/
theorem attempts_nonce_exclusive (a b : Attempt) (s i t j : Nat)
    (same : a.nonce s i = b.nonce t j) : a.draft.plaintext = b.draft.plaintext :=
  (nonce_exclusivity _ _ same).1

theorem retries_identical (encrypt : NonceContext → Bytes) (a : Attempt)
    (key format sectionIndex index : Nat) (convertBody : Bytes → Bytes) :
    encrypt ((prepare key format (convert convertBody (.tried a))).nonce sectionIndex index) =
      encrypt (a.nonce sectionIndex index) := rfl

end CovenStorage
