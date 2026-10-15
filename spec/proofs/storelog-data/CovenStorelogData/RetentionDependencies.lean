import CovenStorelogData.RetentionSafety

/-! §9, §15, §16.5. Which store-log entries one physical deletion depends on.

`RetentionSafety` takes a deletion's dependencies as given. Here they are
derived from the received entries, as coven derives them: a device's state
(absent, active, replaced or removed) comes from the kept entries that
register, replace or remove it, or remove the member that registered it. A
covered log is deleted only once

* the entries deciding its uploader's state are final, when this device
  deletes another device's objects, and
* short of thirty storage days, the entries deciding the state of every
  device that is not an active reader now and has not posted past it are
  final.

Another uploader's files wait for the entries deciding its state too.

`deletion_survives_later_replay` shows that no later replay, with later
arrivals and with every non-final entry free to change its fate, restores a
reader that needs a deleted log or makes its uploader anything but removed.
`waits_only_on_finality_and_posts` and `eligible_eventually_deleted` show
the decision waits on nothing else, so an eligible deletion happens once its
dependencies are final and storage age passes thirty days.
`creation_device_counterexample` shows why store creation counts as a
registration: a rule that learns devices only from additions deletes a log
the creation device still needs. -/
namespace CovenStorelogData.RetentionSafety

open CovenStorelogData.Pending (Condition first)

/-- The changes that decide a device's state (§9, §10, §13). Creation
registers the device that wrote it, for its author, and is the first entry,
so it never resets a registered device. Every other change is `other`. -/
inductive Change where
  | create (device member : Nat)
  | addDevice (device member : Nat) (replaces : Option Nat)
  | removeDevice (device : Nat)
  | removeMember (member : Nat)
  | other
  deriving DecidableEq, Repr

/-- A received entry in replay order. Ids order entries as coven's entry
ids do, so the smallest non-final id is the first one a record names. -/
structure Entry where
  id : Nat
  change : Change
  deriving DecidableEq, Repr

inductive Status where
  | active | replaced | removed
  deriving DecidableEq, Repr

def Status.rank : Status → Nat
  | .active => 0 | .replaced => 1 | .removed => 2

/-- One device's state: absent, or registered for a member with a status. -/
abbrev DeviceState := Option (Nat × Status)

/-- The member an entry registers `d` for, if it registers `d`. -/
def Change.registers (d : Nat) : Change → Option Nat
  | .create device member | .addDevice device member _ =>
      if device = d then some member else none
  | _ => none

/-- One kept entry's effect on device `d`, as coven's replay applies it: a
registration makes it active for its author; a replacement marks an active
device replaced; its removal or its member's marks it removed. -/
def step (d : Nat) (s : DeviceState) (e : Entry) : DeviceState :=
  match e.change with
  | .create device member =>
      if device = d then some (member, .active) else s
  | .addDevice device member replaces =>
      if device = d then some (member, .active)
      else if replaces = some d then
        match s with
        | some (m, .active) => some (m, .replaced)
        | other => other
      else s
  | .removeDevice device =>
      if device = d then s.map fun (m, _) => (m, .removed) else s
  | .removeMember member =>
      match s with
      | some (m, status) => if m = member then some (m, .removed) else some (m, status)
      | none => none
  | .other => s

def track (d : Nat) (kept : Nat → Bool) : List Entry → DeviceState → DeviceState
  | [], s => s
  | e :: es, s => track d kept es (if kept e.id then step d s e else s)

/-- Device `d`'s state after replaying the kept entries in order. -/
def deviceState (entries : List Entry) (kept : Nat → Bool) (d : Nat) : DeviceState :=
  track d kept entries none

def active (entries : List Entry) (kept : Nat → Bool) (d : Nat) : Bool :=
  match deviceState entries kept d with
  | some (_, .active) => true
  | _ => false

def removed (entries : List Entry) (kept : Nat → Bool) (d : Nat) : Bool :=
  match deviceState entries kept d with
  | some (_, .removed) => true
  | _ => false

/-- Whether entry `e` decides device `d`'s state: registering, replacing or
removing it, or removing a member some received entry registered it for.
Dropped entries count: they can return. -/
def concerns (entries : List Entry) (d : Nat) (e : Entry) : Bool :=
  match e.change with
  | .create device _ => device = d
  | .addDevice device _ replaces => device = d || replaces = some d
  | .removeDevice device => device = d
  | .removeMember m => entries.any fun r => r.change.registers d = some m
  | .other => false

/-- The ids of the entries deciding `d`'s state (coven `Dependencies.device`). -/
def deviceEntries (entries : List Entry) (d : Nat) : List Nat :=
  (entries.filter (concerns entries d)).map (·.id)

/-- Insertion into an ascending list. The model sorts with it rather than
`List.mergeSort` so that kernel evaluation checks its examples. -/
def insertAscending (a : Nat) : List Nat → List Nat
  | [] => [a]
  | b :: bs => if a ≤ b then a :: b :: bs else b :: insertAscending a bs

def ascending (l : List Nat) : List Nat := l.foldr insertAscending []

theorem mem_insertAscending (a x : Nat) :
    ∀ l : List Nat, x ∈ insertAscending a l ↔ x = a ∨ x ∈ l
  | [] => by simp [insertAscending]
  | b :: bs => by
      unfold insertAscending
      split
      · simp
      · simp only [List.mem_cons, mem_insertAscending a x bs]
        exact or_left_comm

theorem mem_ascending (x : Nat) : ∀ l : List Nat, x ∈ ascending l ↔ x ∈ l
  | [] => by simp [ascending]
  | a :: l => by
      simp only [ascending, List.foldr_cons, mem_insertAscending, List.mem_cons]
      rw [← ascending, mem_ascending x l]

/-- Every device some received entry names (coven `Dependencies.known_devices`),
ascending. -/
def known (entries : List Entry) : List Nat :=
  (entries.flatMap fun e => match e.change with
    | .create device _ | .removeDevice device => [device]
    | .addDevice device _ none => [device]
    | .addDevice device _ (some old) => [device, old]
    | _ => []) |> ascending

/-- One covered log's deletion as this device judges it. `entitled` is its
authority over another member's objects (its own member's device, or a
removed member's storage it owns, §13); it decides who deletes, not whether
anyone needs the object, and is outside this model. -/
structure Deletion where
  entries : List Entry
  kept : Nat → Bool
  final : Nat → Bool
  own : Nat
  uploader : Nat
  posted : Nat → Bool
  aged : Bool
  covered : Bool
  entitled : Bool

/-- This device deletes its own objects, and a removed device's it is
entitled to (coven `can_delete_device`). -/
def Deletion.owned (x : Deletion) : Bool :=
  x.uploader = x.own || (removed x.entries x.kept x.uploader && x.entitled)

/-- Active readers ascending by device id. -/
def Deletion.readers (x : Deletion) : List Nat :=
  (known x.entries).filter (active x.entries x.kept)

/-- The entries a decision depends on, ascending, for a rule naming
`named` devices and the entries `deps` deciding each one's state: the
uploader's when this device deletes another's objects and, short of thirty
storage days, those of every named device that is not an active reader and
has not posted past the object. -/
def Deletion.dependenciesBy (x : Deletion) (named : List Nat) (deps : Nat → List Nat) :
    List Nat :=
  ((if x.uploader = x.own then [] else deps x.uploader) ++
    (if x.aged then [] else named.flatMap fun d =>
      if !active x.entries x.kept d && !x.posted d then deps d else [])) |> ascending

/-- coven's rule: `Dependencies::deleting` and `Facts::for_write`. -/
def Deletion.dependencies (x : Deletion) : List Nat :=
  x.dependenciesBy (known x.entries) (deviceEntries x.entries)

/-- Every dependency final, in order; then, short of thirty storage days,
every active reader's post (coven `Facts::first_unmet`). -/
def Deletion.conditionsBy (x : Deletion) (dependencies : List Nat) : List Condition :=
  dependencies.map (fun e => ⟨x.final e, .waits (.entryFinality e)⟩) ++
    (if x.aged then [] else x.readers.map fun d => ⟨x.posted d, .waits (.positions d)⟩)

def Deletion.conditions (x : Deletion) : List Condition :=
  x.conditionsBy x.dependencies

/-- A covered, owned object's retention record names its first unmet
condition; it has none otherwise. -/
def Deletion.record (x : Deletion) : Option CovenStorelogData.Pending.Reason :=
  if x.covered && x.owned then first x.conditions else none

def Deletion.mayDeleteBy (x : Deletion) (dependencies : List Nat) : Bool :=
  x.covered && x.owned && (x.conditionsBy dependencies).all (·.met)

def Deletion.mayDelete (x : Deletion) : Bool :=
  x.mayDeleteBy x.dependencies

/-- Another uploader's files go only once it is owned and the entries
deciding its state are final (coven `retain_uploaded_files`); reference
checks are `fileDeletable`'s. -/
def Deletion.filesReleased (x : Deletion) : Bool :=
  x.owned && (x.uploader = x.own || (deviceEntries x.entries x.uploader).all x.final)

/-! ### A device's state depends only on the entries that concern it -/

/-- The state names a member some received entry registered `d` for. -/
def Registered (entries : List Entry) (d : Nat) (s : DeviceState) : Prop :=
  ∀ m status, s = some (m, status) → ∃ r ∈ entries, r.change.registers d = some m

theorem registers_eq {d device member : Nat} {c : Change}
    (h : c = .create device member ∨ ∃ r, c = .addDevice device member r)
    (hd : device = d) : c.registers d = some member := by
  rcases h with rfl | ⟨r, rfl⟩ <;> simp [Change.registers, hd]

theorem step_registered (entries : List Entry) (d : Nat) (s : DeviceState) (e : Entry)
    (he : e ∈ entries) (hs : Registered entries d s) :
    Registered entries d (step d s e) := by
  intro m status h
  unfold step at h
  split at h
  · rename_i device member hc
    split at h
    · cases h
      exact ⟨e, he, registers_eq (.inl hc) (by assumption)⟩
    · exact hs m status h
  · rename_i device member replaces hc
    split at h
    · cases h
      exact ⟨e, he, registers_eq (.inr ⟨replaces, hc⟩) (by assumption)⟩
    · split at h
      · split at h
        · cases h; exact hs m .active rfl
        · exact hs m status h
      · exact hs m status h
  · split at h
    · cases s with
      | none => cases h
      | some p => cases h; exact hs p.1 p.2 rfl
    · exact hs m status h
  · split at h
    · split at h
      · cases h; exact hs _ _ rfl
      · cases h; exact hs _ _ rfl
    · cases h
  · exact hs m status h

/-- An entry that does not concern `d` leaves a registered state alone. -/
theorem step_unconcerned (entries : List Entry) (d : Nat) (s : DeviceState) (e : Entry)
    (hs : Registered entries d s) (unconcerned : concerns entries d e = false) :
    step d s e = s := by
  unfold step
  unfold concerns at unconcerned
  split
  · rename_i device member hc
    rw [hc] at unconcerned
    simp only [decide_eq_false_iff_not] at unconcerned
    simp [unconcerned]
  · rename_i device member replaces hc
    rw [hc] at unconcerned
    simp only [Bool.or_eq_false_iff, decide_eq_false_iff_not] at unconcerned
    simp [unconcerned.1, unconcerned.2]
  · rename_i device hc
    rw [hc] at unconcerned
    simp only [decide_eq_false_iff_not] at unconcerned
    simp [unconcerned]
  · rename_i member hc
    rw [hc] at unconcerned
    cases s with
    | none => rfl
    | some p =>
        obtain ⟨m, status⟩ := p
        obtain ⟨r, hr, reg⟩ := hs m status rfl
        have : m ≠ member := by
          intro eq
          subst eq
          have := List.any_eq_false.mp unconcerned r hr
          simp [reg] at this
        simp [this]
  · rfl

theorem track_determined (entries : List Entry) (d : Nat) (kept kept' : Nat → Bool) :
    ∀ (es : List Entry) (s : DeviceState), (∀ e ∈ es, e ∈ entries) →
      Registered entries d s →
      (∀ e ∈ es, concerns entries d e = true → kept e.id = kept' e.id) →
      track d kept es s = track d kept' es s
  | [], _, _, _, _ => rfl
  | e :: es, s, sub, hs, same => by
      have he := sub e List.mem_cons_self
      have next : (if kept e.id then step d s e else s) =
          (if kept' e.id then step d s e else s) := by
        cases hc : concerns entries d e
        · rw [step_unconcerned entries d s e hs hc]; simp
        · rw [same e List.mem_cons_self hc]
      simp only [track]
      rw [next]
      apply track_determined entries d kept kept' es _
        (fun x hx => sub x (List.mem_cons_of_mem _ hx))
      · split
        · exact step_registered entries d s e he hs
        · exact hs
      · exact fun x hx => same x (List.mem_cons_of_mem _ hx)

/-- Two replays that agree on the entries concerning `d` give `d` one state. -/
theorem state_determined (entries : List Entry) (d : Nat) (kept kept' : Nat → Bool)
    (same : ∀ e ∈ entries, concerns entries d e = true → kept e.id = kept' e.id) :
    deviceState entries kept d = deviceState entries kept' d :=
  track_determined entries d kept kept' entries none (fun _ h => h)
    (fun _ _ h => nomatch h) same

/-! ### Later arrivals only retire a device

A later replay sees more entries, interleaved by timestamp with the received
ones. None registers a device the received entries already name: a device
registers itself once (§10, §12.1). -/

/-- `after` is `before` with later arrivals interleaved, none registering `d`. -/
inductive Later (d : Nat) : List Entry → List Entry → Prop
  | nil : Later d [] []
  | old (e : Entry) {before after : List Entry} : Later d before after →
      Later d (e :: before) (e :: after)
  | new (e : Entry) {before after : List Entry} : e.change.registers d = none →
      Later d before after → Later d before (e :: after)

/-- `s` is at most as retired as `t`, for the same member. -/
def Retired : DeviceState → DeviceState → Prop
  | none, none => True
  | some (m, a), some (n, b) => m = n ∧ a.rank ≤ b.rank
  | _, _ => False

theorem retired_refl : ∀ s : DeviceState, Retired s s
  | none => trivial
  | some (_, _) => ⟨rfl, Nat.le_refl _⟩

theorem retired_trans : ∀ {s t u : DeviceState}, Retired s t → Retired t u → Retired s u
  | none, none, none, _, _ => trivial
  | some (_, _), some (_, _), some (_, _), ⟨h1, r1⟩, ⟨h2, r2⟩ => ⟨h1.trans h2, Nat.le_trans r1 r2⟩

theorem step_monotone (d : Nat) (e : Entry) :
    ∀ {s t : DeviceState}, Retired s t → Retired (step d s e) (step d t e)
  | none, none, _ => by
      obtain ⟨id, c⟩ := e
      cases c with
      | create device member =>
          by_cases h : device = d <;> simp [step, h, Retired, Status.rank]
      | addDevice device member replaces =>
          by_cases h : device = d <;> by_cases h' : replaces = some d <;>
            simp [step, h, h', Retired, Status.rank]
      | removeDevice device => by_cases h : device = d <;> simp [step, h, Retired]
      | removeMember member => simp [step, Retired]
      | other => simp [step, Retired]
  | some (m, a), some (n, b), ⟨hm, hr⟩ => by
      subst hm
      obtain ⟨id, c⟩ := e
      cases c with
      | create device member =>
          by_cases h : device = d <;> cases a <;> cases b <;>
            simp_all [step, Retired, Status.rank]
      | addDevice device member replaces =>
          by_cases h : device = d <;> by_cases h' : replaces = some d <;>
            cases a <;> cases b <;> simp_all [step, Retired, Status.rank]
      | removeDevice device =>
          by_cases h : device = d <;> cases a <;> cases b <;>
            simp_all [step, Retired, Status.rank]
      | removeMember member =>
          by_cases h : m = member <;> cases a <;> cases b <;>
            simp_all [step, Retired, Status.rank]
      | other => simp [step, Retired, hr]

/-- A change that registers nothing for `d` can only retire it further. -/
theorem step_retires (d : Nat) (e : Entry) (fresh : e.change.registers d = none) :
    ∀ s : DeviceState, Retired s (step d s e)
  | none => by
      obtain ⟨id, c⟩ := e
      cases c with
      | create device member =>
          by_cases h : device = d <;> simp_all [step, Retired, Change.registers]
      | addDevice device member replaces =>
          by_cases h : device = d <;> by_cases h' : replaces = some d <;>
            simp_all [step, Retired, Change.registers]
      | removeDevice device => by_cases h : device = d <;> simp [step, h, Retired]
      | removeMember member => simp [step, Retired]
      | other => simp [step, Retired]
  | some (m, a) => by
      obtain ⟨id, c⟩ := e
      cases c with
      | create device member =>
          by_cases h : device = d <;> simp_all [step, Retired, Change.registers]
      | addDevice device member replaces =>
          by_cases h : device = d <;> by_cases h' : replaces = some d <;>
            cases a <;> simp_all [step, Retired, Change.registers, Status.rank]
      | removeDevice device =>
          by_cases h : device = d <;> cases a <;> simp [step, h, Retired, Status.rank]
      | removeMember member =>
          by_cases h : m = member <;> cases a <;> simp [step, h, Retired, Status.rank]
      | other => simp [step, Retired, Status.rank]

theorem registered_retired {entries : List Entry} {d : Nat} :
    ∀ {s t : DeviceState}, Registered entries d s → Retired s t → Registered entries d t
  | none, none, _, _ => fun _ _ h => nomatch h
  | some (_, _), some (_, _), hs, ⟨hm, _⟩ => fun m' st' h => by
      cases h
      subst hm
      exact hs _ _ rfl

theorem track_later (entries : List Entry) (d : Nat) (kept kept' : Nat → Bool) :
    ∀ {before after : List Entry}, Later d before after →
      ∀ (s t : DeviceState), (∀ e ∈ before, e ∈ entries) →
        Registered entries d s → Retired s t →
        (∀ e ∈ before, concerns entries d e = true → kept e.id = kept' e.id) →
        Retired (track d kept before s) (track d kept' after t)
  | _, _, .nil, _, _, _, _, r, _ => r
  | _, _, .old e rest, s, t, sub, hs, r, same => by
      simp only [track]
      apply track_later entries d kept kept' rest
      · exact fun x hx => sub x (List.mem_cons_of_mem _ hx)
      · split
        · exact step_registered entries d s e (sub e List.mem_cons_self) hs
        · exact hs
      · cases hc : concerns entries d e
        · rw [step_unconcerned entries d s e hs hc,
            step_unconcerned entries d t e (registered_retired hs r) hc]
          split <;> split <;> exact r
        · rw [same e List.mem_cons_self hc]
          split
          · exact step_monotone d e r
          · exact r
      · exact fun x hx => same x (List.mem_cons_of_mem _ hx)
  | _, _, .new e fresh rest, s, t, sub, hs, r, same => by
      simp only [track]
      apply track_later entries d kept kept' rest s _ sub hs _ same
      split
      · exact retired_trans r (step_retires d e fresh t)
      · exact r

/-- A later replay leaves `d` at most as active as the received replay
did, when they agree on the received entries concerning `d`. -/
theorem later_state (entries after : List Entry) (d : Nat) (kept kept' : Nat → Bool)
    (later : Later d entries after)
    (same : ∀ e ∈ entries, concerns entries d e = true → kept e.id = kept' e.id) :
    Retired (deviceState entries kept d) (deviceState after kept' d) :=
  track_later entries d kept kept' later none none (fun _ h => h)
    (fun _ _ h => nomatch h) trivial same

theorem later_active (entries after : List Entry) (d : Nat) (kept kept' : Nat → Bool)
    (later : Later d entries after)
    (same : ∀ e ∈ entries, concerns entries d e = true → kept e.id = kept' e.id)
    (now : active after kept' d = true) : active entries kept d = true := by
  have r := later_state entries after d kept kept' later same
  unfold active at now ⊢
  revert r now
  cases deviceState entries kept d with
  | none => cases deviceState after kept' d <;> simp [Retired]
  | some p =>
      obtain ⟨m, a⟩ := p
      cases deviceState after kept' d with
      | none => simp [Retired]
      | some q =>
          obtain ⟨n, b⟩ := q
          cases a <;> cases b <;> simp [Retired, Status.rank]

theorem later_removed (entries after : List Entry) (d : Nat) (kept kept' : Nat → Bool)
    (later : Later d entries after)
    (same : ∀ e ∈ entries, concerns entries d e = true → kept e.id = kept' e.id)
    (before : removed entries kept d = true) : removed after kept' d = true := by
  have r := later_state entries after d kept kept' later same
  unfold removed at before ⊢
  revert r before
  cases deviceState entries kept d with
  | none => simp
  | some p =>
      obtain ⟨m, a⟩ := p
      cases deviceState after kept' d with
      | none => simp [Retired]
      | some q =>
          obtain ⟨n, b⟩ := q
          cases a <;> cases b <;> simp [Retired, Status.rank]

theorem track_registered (entries : List Entry) (d : Nat) (kept : Nat → Bool) :
    ∀ (es : List Entry) (s : DeviceState), (∀ e ∈ es, e ∈ entries) →
      Registered entries d s → Registered entries d (track d kept es s)
  | [], _, _, hs => hs
  | e :: es, s, sub, hs => by
      simp only [track]
      apply track_registered entries d kept es _ (fun x hx => sub x (List.mem_cons_of_mem _ hx))
      split
      · exact step_registered entries d s e (sub e List.mem_cons_self) hs
      · exact hs

/-- A device with a state was registered by a received entry, so it is known. -/
theorem state_known (entries : List Entry) (kept : Nat → Bool) (d : Nat) (p : Nat × Status)
    (h : deviceState entries kept d = some p) : d ∈ known entries := by
  obtain ⟨r, hr, reg⟩ := track_registered entries d kept entries none (fun _ h => h)
    (fun _ _ h => nomatch h) p.1 p.2 h
  unfold known
  rw [mem_ascending, List.mem_flatMap]
  refine ⟨r, hr, ?_⟩
  obtain ⟨id, c⟩ := r
  cases c with
  | create device member =>
      by_cases hd : device = d <;> simp_all [Change.registers]
  | addDevice device member replaces =>
      by_cases hd : device = d <;> cases replaces <;> simp_all [Change.registers]
  | removeDevice _ | removeMember _ | other => simp [Change.registers] at reg

theorem active_known (entries : List Entry) (kept : Nat → Bool) (d : Nat)
    (h : active entries kept d = true) : d ∈ known entries := by
  unfold active at h
  split at h
  · rename_i heq; exact state_known entries kept d _ heq
  · cases h

theorem removed_known (entries : List Entry) (kept : Nat → Bool) (d : Nat)
    (h : removed entries kept d = true) : d ∈ known entries := by
  unfold removed at h
  split at h
  · rename_i heq; exact state_known entries kept d _ heq
  · cases h

theorem concerned_dependency (entries : List Entry) (d : Nat) (e : Entry)
    (he : e ∈ entries) (hc : concerns entries d e = true) : e.id ∈ deviceEntries entries d :=
  List.mem_map.mpr ⟨e, List.mem_filter.mpr ⟨he, hc⟩, rfl⟩

/-- What a deletion establishes: coverage, ownership, every dependency
final, and, short of thirty storage days, every active reader's post. -/
theorem mayDelete_spec (x : Deletion) (delete : x.mayDelete = true) :
    x.covered = true ∧ x.owned = true ∧ (∀ e ∈ x.dependencies, x.final e = true) ∧
      (x.aged = false → ∀ d ∈ x.readers, x.posted d = true) := by
  simp only [Deletion.mayDelete, Deletion.mayDeleteBy, Deletion.conditionsBy, Bool.and_eq_true, List.all_append, List.all_map,
    List.all_eq_true, Function.comp_def] at delete
  obtain ⟨⟨covered, owned⟩, finals, posts⟩ := delete
  refine ⟨covered, owned, finals, fun young d hd => ?_⟩
  simp only [young] at posts
  exact posts _ (List.mem_map.mpr ⟨d, hd, rfl⟩)

theorem uploader_dependency (x : Deletion) (other : x.uploader ≠ x.own) (e : Nat)
    (he : e ∈ deviceEntries x.entries x.uploader) : e ∈ x.dependencies := by
  unfold Deletion.dependencies Deletion.dependenciesBy
  rw [mem_ascending]
  exact List.mem_append_left _ (by simp [other, he])

theorem reader_dependency (x : Deletion) (young : x.aged = false) (d : Nat)
    (hd : d ∈ known x.entries) (inactive : active x.entries x.kept d = false)
    (silent : x.posted d = false) (e : Nat) (he : e ∈ deviceEntries x.entries d) :
    e ∈ x.dependencies := by
  unfold Deletion.dependencies Deletion.dependenciesBy
  rw [mem_ascending]
  apply List.mem_append_right
  simp only [young, Bool.false_eq_true, ite_false, List.mem_flatMap]
  exact ⟨d, hd, by simp [inactive, silent, he]⟩

/-- **No deletion depends on an entry that can still change.** Take any later
replay: more entries arrived, interleaved by timestamp, none registering a
device the received entries name, and every entry that was not final may now
be kept or dropped. The deleted log's uploader is still this device or
removed, and, short of thirty storage days, every active reader the received
entries name has posted past it (§9, §15). A device registered only by a
later arrival starts from a snapshot, which the coverage check covers
(§12.1, §15); it is outside this statement. -/
theorem deletion_survives_later_replay (x : Deletion) (after : List Entry)
    (kept' : Nat → Bool)
    (later : ∀ d ∈ known x.entries, Later d x.entries after)
    (stable : ∀ e ∈ x.entries, x.final e.id = true → kept' e.id = x.kept e.id)
    (delete : x.mayDelete = true) :
    (x.uploader = x.own ∨ removed after kept' x.uploader = true) ∧
    (x.aged = true ∨ ∀ d ∈ known x.entries, active after kept' d = true →
      x.posted d = true) := by
  obtain ⟨_, owned, finals, posts⟩ := mayDelete_spec x delete
  constructor
  · by_cases own : x.uploader = x.own
    · exact .inl own
    · right
      have gone : removed x.entries x.kept x.uploader = true := by
        simp only [Deletion.owned, own, decide_false, Bool.false_or, Bool.and_eq_true] at owned
        exact owned.1
      apply later_removed x.entries after x.uploader x.kept kept'
        (later _ (removed_known _ _ _ gone)) _ gone
      intro e he hc
      exact (stable e he (finals _ (uploader_dependency x own _
        (concerned_dependency _ _ e he hc)))).symm
  · cases young : x.aged
    · right
      intro d hd now
      cases silent : x.posted d
      · exfalso
        cases inactive : active x.entries x.kept d
        · have := later_active x.entries after d x.kept kept' (later d hd)
            (fun e he hc => (stable e he (finals _ (reader_dependency x young d hd
              inactive silent _ (concerned_dependency _ _ e he hc)))).symm) now
          rw [inactive] at this
          cases this
        · have := posts young d (List.mem_filter.mpr ⟨hd, inactive⟩)
          rw [silent] at this
          cases this
      · rfl
    · exact .inl rfl

/-- Another uploader's files: once released, no later replay makes their
uploader anything but removed, so it never names them again (§9, §16.5). -/
theorem files_survive_later_replay (x : Deletion) (after : List Entry) (kept' : Nat → Bool)
    (later : ∀ d ∈ known x.entries, Later d x.entries after)
    (stable : ∀ e ∈ x.entries, x.final e.id = true → kept' e.id = x.kept e.id)
    (released : x.filesReleased = true) :
    x.uploader = x.own ∨ removed after kept' x.uploader = true := by
  by_cases own : x.uploader = x.own
  · exact .inl own
  · right
    simp only [Deletion.filesReleased, Deletion.owned, own, decide_false, Bool.false_or,
      Bool.and_eq_true, List.all_eq_true] at released
    obtain ⟨⟨gone, _⟩, finals⟩ := released
    apply later_removed x.entries after x.uploader x.kept kept'
      (later _ (removed_known _ _ _ gone)) _ gone
    intro e he hc
    exact (stable e he (finals _ (concerned_dependency _ _ e he hc))).symm

/-- **The decision waits on nothing else.** A covered, owned log whose
dependencies are final goes once every active reader posted past it or
storage held it for thirty days (§15). -/
theorem waits_only_on_finality_and_posts (x : Deletion) (covered : x.covered = true)
    (owned : x.owned = true) (finals : ∀ e ∈ x.dependencies, x.final e = true)
    (posts : x.aged = true ∨ ∀ d ∈ x.readers, x.posted d = true) :
    x.mayDelete = true := by
  simp only [Deletion.mayDelete, Deletion.mayDeleteBy, Deletion.conditionsBy,
    Bool.and_eq_true, List.all_append, List.all_map, List.all_eq_true, Function.comp_def]
  refine ⟨⟨covered, owned⟩, finals, ?_⟩
  rcases posts with aged | posts
  · simp [aged]
  · split
    · simp
    · intro c hc
      obtain ⟨d, hd, rfl⟩ := List.mem_map.mp hc
      exact posts d hd

theorem dependency_received (x : Deletion) (e : Nat) (he : e ∈ x.dependencies) :
    ∃ r ∈ x.entries, r.id = e := by
  unfold Deletion.dependencies Deletion.dependenciesBy at he
  rw [mem_ascending] at he
  have entry : ∀ d, e ∈ deviceEntries x.entries d → ∃ r ∈ x.entries, r.id = e := by
    intro d h
    obtain ⟨r, hr, rfl⟩ := List.mem_map.mp h
    exact ⟨r, (List.mem_filter.mp hr).1, rfl⟩
  rcases List.mem_append.mp he with h | h
  · split at h
    · cases h
    · exact entry _ h
  · split at h
    · cases h
    · obtain ⟨d, _, hd⟩ := List.mem_flatMap.mp h
      split at hd
      · exact entry _ hd
      · cases hd

/-- One device's view over time with no further arrivals: its replay,
finality and storage age move; everything else stays. -/
def Deletion.at (x : Deletion) (kept final : Nat → Nat → Bool) (aged : Nat → Bool)
    (t : Nat) : Deletion :=
  { x with kept := kept t, final := final t, aged := aged t }

/-- **Every eligible deletion eventually happens.** Suppose finality only
grows and fixes the fate of what it covers, storage age eventually passes
thirty days, and once every received entry is final the log is covered and
owned. Then some evaluation deletes it, whether or not every reader ever
posts (§9, §15). -/
theorem eligible_eventually_deleted (x : Deletion) (kept final : Nat → Nat → Bool)
    (aged : Nat → Bool)
    (grows : ∀ t t' e, t ≤ t' → final t e = true → final t' e = true)
    (fixed : ∀ t t' e, t ≤ t' → final t e = true → kept t' e = kept t e)
    (ages : ∃ t, aged t = true) (stays : ∀ t t', t ≤ t' → aged t = true → aged t' = true)
    (eligible : ∃ T, (∀ e ∈ x.entries, final T e.id = true) ∧
      (x.at kept final aged T).covered = true ∧ (x.at kept final aged T).owned = true) :
    ∃ t, (x.at kept final aged t).mayDelete = true := by
  obtain ⟨A, old⟩ := ages
  obtain ⟨T, finals, covered, owned⟩ := eligible
  refine ⟨max T A, waits_only_on_finality_and_posts _ covered ?_ ?_
    (.inl (stays A _ (Nat.le_max_right _ _) old))⟩
  · have same : deviceState x.entries (kept (max T A)) x.uploader =
        deviceState x.entries (kept T) x.uploader :=
      state_determined _ _ _ _ fun e he _ =>
        fixed T _ _ (Nat.le_max_left _ _) (finals e he)
    have gone : removed x.entries (kept (max T A)) x.uploader =
        removed x.entries (kept T) x.uploader := by
      unfold removed; rw [same]
    simp only [Deletion.owned, Deletion.at] at owned ⊢
    rw [gone]
    exact owned
  · intro e he
    obtain ⟨r, hr, rfl⟩ := dependency_received _ e he
    exact grows T _ _ (Nat.le_max_left _ _) (finals r hr)

/-! ### The creation device

Store creation registers the device that wrote it. A rule that learns
devices and their members only from additions, replacements and removals
never names that device's member removal, so its decision can delete a log
the creation device still needs. -/

/-- Whether `e` decides `d`'s state, judged from additions alone. -/
def concernsByAddition (entries : List Entry) (d : Nat) (e : Entry) : Bool :=
  match e.change with
  | .create _ _ => false
  | .addDevice device _ replaces => device = d || replaces = some d
  | .removeDevice device => device = d
  | .removeMember m => entries.any fun r => match r.change with
      | .addDevice device member _ => device = d && member = m
      | _ => false
  | .other => false

def knownByAddition (entries : List Entry) : List Nat :=
  (entries.flatMap fun e => match e.change with
    | .removeDevice device => [device]
    | .addDevice device _ none => [device]
    | .addDevice device _ (some old) => [device, old]
    | _ => []) |> ascending

def Deletion.byAddition (x : Deletion) : List Nat :=
  x.dependenciesBy (knownByAddition x.entries) fun d =>
    (x.entries.filter (concernsByAddition x.entries d)).map (·.id)

namespace CreationExample

/-- Ana's phone 0 created the store; Ben's laptop 1 registered itself; Ben
removed Ana, which is kept but not final. Ben's covered log is a day old,
his laptop posted past it, Ana's phone never did. -/
def entries : List Entry :=
  [⟨0, .create 0 0⟩, ⟨1, .addDevice 1 1 none⟩, ⟨2, .removeMember 0⟩]

def deletion : Deletion :=
  { entries, kept := fun _ => true, final := fun e => decide (e < 2), own := 1,
    uploader := 1, posted := fun d => d == 1, aged := false, covered := true,
    entitled := true }

/-- Ana's concurrent removal of Ben, stamped earlier, arrives and wins: Ben's
removal of Ana drops. -/
def after : List Entry :=
  [⟨0, .create 0 0⟩, ⟨1, .addDevice 1 1 none⟩, ⟨3, .removeMember 1⟩, ⟨2, .removeMember 0⟩]

def kept' (e : Nat) : Bool := e != 2

/-- Judged from additions alone, Ben deletes the log; the later replay keeps
Ana's phone active, and it never posted past it. Judged with creation, the
log waits for Ben's removal of Ana to be final. -/
theorem creation_device_counterexample :
    deletion.mayDeleteBy deletion.byAddition = true ∧
    (∀ d ∈ known deletion.entries, Later d deletion.entries after) ∧
    (∀ e ∈ deletion.entries, deletion.final e.id = true →
      kept' e.id = deletion.kept e.id) ∧
    active after kept' 0 = true ∧ deletion.posted 0 = false ∧
    deletion.mayDelete = false ∧
    deletion.record = some (.waits (.entryFinality 2)) := by
  refine ⟨by decide +kernel, fun d _ => ?_, by decide, by decide +kernel, by decide,
    by decide +kernel, by decide +kernel⟩
  exact .old _ (.old _ (.new _ (by simp [Change.registers]) (.old _ .nil)))

end CreationExample

end CovenStorelogData.RetentionSafety
