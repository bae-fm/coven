/-! Key preparation records an identity before saving material in custody
(§11.1, §12.3, §12.4). A restart reuses saved material. The database records
only the identity and, once prepared, its public commitment. `Id` is an
audience key's `Id`, or a member's candidate pair in a replacement of its
own keys. -/
namespace CovenSequence.KeyCustody

variable {Id Secret : Type} [DecidableEq Id]

/-- The database record has no field for secret material. -/
inductive Journal (Id : Type) where
  | generating (key : Id)
  | prepared (key : Id) (commitment : Nat)
  deriving DecidableEq

/-- Custody is the only destination of newly generated material. -/
def save (held : Id → Option Secret) (key : Id) (fresh : Secret) :
    Id → Option Secret :=
  fun k => if k = key then some ((held key).getD fresh) else held k

def ready (hash : Secret → Nat) (held : Id → Option Secret) (key : Id) :
    Journal Id :=
  match held key with
  | none => .generating key
  | some material => .prepared key (hash material)

omit [DecidableEq Id] in
theorem saved_before_prepared (hash : Secret → Nat) (held : Id → Option Secret)
    (key : Id) (commitment : Nat) (h : ready hash held key = .prepared key commitment) :
    ∃ material, held key = some material ∧ hash material = commitment := by
  unfold ready at h
  split at h
  · cases h
  · cases h
    exact ⟨_, ‹held key = some _›, rfl⟩

theorem restart_reuses_saved (held : Id → Option Secret) (key : Id)
    (material fresh : Secret) (h : held key = some material) :
    save held key fresh = held := by
  funext k
  by_cases same : k = key
  · subst k; simp [save, h]
  · simp [save, same]

theorem save_keeps_other (held : Id → Option Secret) (key other : Id)
    (fresh : Secret) (different : other ≠ key) : save held key fresh other = held other := by
  simp [save, different]

omit [DecidableEq Id] in
/-- Even during preparation, changing secrets with equal public commitments
does not change the database record. SQLite's copies of that record have the
same observation; their physical encoding is checked in Rust. -/
theorem journal_depends_only_on_commitment (hash : Secret → Nat)
    (a b : Id → Option Secret) (key : Id)
    (same : (a key).map hash = (b key).map hash) :
    ready hash a key = ready hash b key := by
  unfold ready
  cases ha : a key <;> cases hb : b key <;> simp_all

end CovenSequence.KeyCustody
