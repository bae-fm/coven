import CovenSequence.Keys

/-! Audience-key preparation records an identity before saving material in
custody (§11.1, §12.4). A restart reuses saved material. The database records
only the identity and, once prepared, its public commitment. -/
namespace CovenSequence.KeyCustody
open Keys

variable {Aud Secret : Type} [DecidableEq Aud]

/-- The database record has no field for secret material. -/
inductive Journal (Aud : Type) where
  | generating (key : Key Aud)
  | prepared (key : Key Aud) (commitment : Nat)
  deriving DecidableEq

/-- Custody is the only destination of newly generated material. -/
def save (held : Key Aud → Option Secret) (key : Key Aud) (fresh : Secret) :
    Key Aud → Option Secret :=
  fun k => if k = key then some ((held key).getD fresh) else held k

def ready (hash : Secret → Nat) (held : Key Aud → Option Secret) (key : Key Aud) :
    Journal Aud :=
  match held key with
  | none => .generating key
  | some material => .prepared key (hash material)

omit [DecidableEq Aud] in
theorem saved_before_prepared (hash : Secret → Nat) (held : Key Aud → Option Secret)
    (key : Key Aud) (commitment : Nat) (h : ready hash held key = .prepared key commitment) :
    ∃ material, held key = some material ∧ hash material = commitment := by
  unfold ready at h
  split at h
  · cases h
  · cases h
    exact ⟨_, ‹held key = some _›, rfl⟩

theorem restart_reuses_saved (held : Key Aud → Option Secret) (key : Key Aud)
    (material fresh : Secret) (h : held key = some material) :
    save held key fresh = held := by
  funext k
  by_cases same : k = key
  · subst k; simp [save, h]
  · simp [save, same]

theorem save_keeps_other (held : Key Aud → Option Secret) (key other : Key Aud)
    (fresh : Secret) (different : other ≠ key) : save held key fresh other = held other := by
  simp [save, different]

omit [DecidableEq Aud] in
/-- Even during preparation, changing secrets with equal public commitments
does not change the database record. SQLite's copies of that record have the
same observation; their physical encoding is checked in Rust. -/
theorem journal_depends_only_on_commitment (hash : Secret → Nat)
    (a b : Key Aud → Option Secret) (key : Key Aud)
    (same : (a key).map hash = (b key).map hash) :
    ready hash a key = ready hash b key := by
  unfold ready
  cases ha : a key <;> cases hb : b key <;> simp_all

end CovenSequence.KeyCustody
