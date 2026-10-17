import CovenStorage.Objects

namespace CovenStorage.BucketClaim

/-! The bucket marker uses the same atomic create as slots. Store ids are
their canonical, injective byte encodings. Listings can omit every recent
object: they are an additional refusal gate, never evidence of ownership.
No transition replaces or deletes the marker, including local store deletion. -/

def names (s : Storage) (store : Bytes) : Bool :=
  match s .bucket with
  | none => false
  | some o => o.bytes == store

def setup (s : Storage) (store : Bytes) (time : Nat) : Storage × Bool :=
  (create s .bucket store time, match s .bucket with
    | none => true
    | some o => o.bytes == store)

theorem names_iff (s : Storage) (store : Bytes) :
    names s store = true ↔ ∃ o, s .bucket = some o ∧ o.bytes = store := by
  cases h : s .bucket <;> simp [names, h]

theorem setup_passes (s : Storage) (store : Bytes) (time : Nat)
    (h : (setup s store time).2 = true) : names (setup s store time).1 store = true := by
  cases hs : s .bucket <;> simp_all [setup, names, create]

theorem names_preserved (s : Storage) (store : Bytes) (requests : List Publication)
    (h : names s store = true) : names (publishAll s requests) store = true := by
  obtain ⟨o, ho, hb⟩ := (names_iff _ _).1 h
  exact (names_iff _ _).2 ⟨o, stored_history_preserved s requests .bucket o ho, hb⟩

/-- Two successful setups, separated by any number of creates and retries,
must name the same store. A delayed read after a refused create sees that
same immutable marker. Atomic provider requests give any concurrent history
an arrival order covered by this theorem. -/
theorem at_most_one_store (s : Storage) (a b : Bytes) (ta tb : Nat)
    (between : List Publication)
    (ha : (setup s a ta).2 = true)
    (hb : (setup (publishAll (setup s a ta).1 between) b tb).2 = true) : a = b := by
  have kept := names_preserved _ a between (setup_passes s a ta ha)
  obtain ⟨o, ho, hbytes⟩ := (names_iff _ _).1 kept
  simp only [setup] at ho hb
  simp [ho] at hb
  exact hbytes.symm.trans hb

theorem retry_passes (s : Storage) (store : Bytes) (time : Nat)
    (h : names s store = true) : (setup s store time).2 = true := by
  obtain ⟨o, ho, hb⟩ := (names_iff _ _).1 h
  simp [setup, ho, hb]

theorem reconnect_refuses_other (s : Storage) (a b : Bytes)
    (h : names s a = true) (different : a ≠ b) : names s b = false := by
  obtain ⟨o, ho, hb⟩ := (names_iff _ _).1 h
  simp [names, ho, hb, different]

end CovenStorage.BucketClaim
