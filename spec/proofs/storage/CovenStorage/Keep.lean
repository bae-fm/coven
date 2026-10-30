namespace CovenStorage.Keep

/-! §17.7: files kept here. A file staged or moved for the cloud can be kept
here: the write that switches it to the cloud records a pin request, and
coven makes the pin itself. The request ends when the pin is made, when the
file's record goes from this device, or when a switch takes the file off its
stored bytes, making it device-only (§17.3). A failed attempt leaves it in
place, waiting to retry.

Proved here: a request appears only in the write switching its file to the
cloud, for a file kept here (`requested_by_switch`); and it ends only once
the pin is made, the file's record is gone or the file has left the cloud
(`request_ends_made_or_gone`). -/

/-- One device's files, by id: whether this device has a record of each,
whether its coordinates say cloud, whether its upload link says keep it
here, whether a pin request waits for it, and whether it is pinned. -/
structure State where
  recorded : Nat → Bool
  cloud : Nat → Bool
  keep : Nat → Bool
  requested : Nat → Bool
  pinned : Nat → Bool

/-- What a device may do. `switch` is the write switching a move's files,
on this device only until then, to the cloud; it requests the pin of each one
kept here. `make` makes a requested pin and ends its request; `fail` is a
failed attempt, which changes nothing it records here; `forget` is the
record going; `leave` is a switch, this device's or another's, taking the
file off its stored bytes. The app pins and unpins as it likes. -/
inductive Step : State → State → Prop where
  | switch (s : State) (files : Nat → Bool)
      (here : ∀ f, files f = true → s.recorded f = true ∧ s.cloud f = false) :
      Step s { s with
        cloud := fun f => s.cloud f || files f
        requested := fun f => s.requested f || (files f && s.keep f) }
  | make (s : State) (f : Nat) (asked : s.requested f = true) :
      Step s { s with
        pinned := fun x => if x = f then true else s.pinned x
        requested := fun x => if x = f then false else s.requested x }
  | fail (s : State) : Step s s
  | forget (s : State) (f : Nat) :
      Step s { s with
        recorded := fun x => if x = f then false else s.recorded x
        requested := fun x => if x = f then false else s.requested x }
  | leave (s : State) (f : Nat) :
      Step s { s with
        cloud := fun x => if x = f then false else s.cloud x
        requested := fun x => if x = f then false else s.requested x }
  | pin (s : State) (f : Nat) (p : Bool) :
      Step s { s with pinned := fun x => if x = f then p else s.pinned x }
  | mark (s : State) (f : Nat) (k : Bool) (here : s.cloud f = false) :
      Step s { s with keep := fun x => if x = f then k else s.keep x }

/-- A pin request appears only in the write that switches its file to the
cloud, and only for a file kept here. -/
theorem requested_by_switch {s t : State} (st : Step s t) (f : Nat)
    (before : s.requested f = false) (after : t.requested f = true) :
    s.cloud f = false ∧ t.cloud f = true ∧ s.keep f = true := by
  cases st with
  | switch files here =>
    simp only [before, Bool.false_or, Bool.and_eq_true] at after
    have ⟨_, hl⟩ := here f after.1
    exact ⟨hl, by simp [after.1], after.2⟩
  | make g asked =>
    simp only at after
    by_cases e : f = g
    · simp [e] at after
    · simp [e, before] at after
  | fail => rw [before] at after; simp at after
  | forget g =>
    simp only at after
    by_cases e : f = g
    · simp [e] at after
    · simp [e, before] at after
  | leave g =>
    simp only at after
    by_cases e : f = g
    · simp [e] at after
    · simp [e, before] at after
  | pin g p => simp only at after; rw [before] at after; simp at after
  | mark g k here => simp only at after; rw [before] at after; simp at after

/-- A pin request ends only once its pin is made, its file's record is gone
or the file has left the cloud: a failed attempt, the app's pins and unpins,
and other files' steps leave it waiting. -/
theorem request_ends_made_or_gone {s t : State} (st : Step s t) (f : Nat)
    (before : s.requested f = true) (after : t.requested f = false) :
    t.pinned f = true ∨ t.recorded f = false ∨ t.cloud f = false := by
  cases st with
  | switch files here => simp [before] at after
  | make g asked =>
    simp only at after ⊢
    by_cases e : f = g
    · left; simp [e]
    · simp [e, before] at after
  | fail => rw [before] at after; simp at after
  | forget g =>
    simp only at after ⊢
    by_cases e : f = g
    · right; left; simp [e]
    · simp [e, before] at after
  | leave g =>
    simp only at after ⊢
    by_cases e : f = g
    · right; right; simp [e]
    · simp [e, before] at after
  | pin g p => simp only at after; rw [before] at after; simp at after
  | mark g k here => simp only at after; rw [before] at after; simp at after

/-- A file not kept here is never requested by its switch. -/
theorem unkept_unrequested (s : State) (files : Nat → Bool) (f : Nat)
    (plain : s.keep f = false) (before : s.requested f = false) :
    ({ s with
        cloud := fun g => s.cloud g || files g
        requested := fun g => s.requested g || (files g && s.keep g) } : State).requested f
      = false := by
  simp [before, plain]

end CovenStorage.Keep
