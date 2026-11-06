import CovenStorage.Objects

namespace CovenStorage.LocationClaim

/-! The location marker uses the same atomic create as slots. A location is a
Dropbox shared folder, a OneDrive folder, or an S3 bucket; each needs only
that create and exact reads, so one model covers them. Store ids are their canonical, injective byte encodings. Listings can
omit every recent object: they are an additional refusal gate, never evidence
of ownership.
No transition replaces or deletes the marker, including local store deletion. -/

def names (s : Storage) (store : Bytes) : Bool :=
  match s .locationMarker with
  | none => false
  | some o => o.bytes == store

def setup (s : Storage) (store : Bytes) (time : Nat) : Storage × Bool :=
  (create s .locationMarker store time, match s .locationMarker with
    | none => true
    | some o => o.bytes == store)

theorem names_iff (s : Storage) (store : Bytes) :
    names s store = true ↔ ∃ o, s .locationMarker = some o ∧ o.bytes = store := by
  cases h : s .locationMarker <;> simp [names, h]

theorem setup_passes (s : Storage) (store : Bytes) (time : Nat)
    (h : (setup s store time).2 = true) : names (setup s store time).1 store = true := by
  cases hs : s .locationMarker <;> simp_all [setup, names, create]

theorem names_preserved (s : Storage) (store : Bytes) (requests : List Publication)
    (h : names s store = true) : names (publishAll s requests) store = true := by
  obtain ⟨o, ho, hb⟩ := (names_iff _ _).1 h
  exact (names_iff _ _).2 ⟨o, stored_history_preserved s requests .locationMarker o ho, hb⟩

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

end CovenStorage.LocationClaim
