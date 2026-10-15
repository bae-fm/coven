import CovenStorelogData.Pending

/-! §15, §19.1: what a store-log scan and a snapshot listing find, before
`Pending` commits it. A scan's waits are each read entry's first unmet
condition; a snapshot this device refused is never offered for loading; and
a listed snapshot in a newer format makes the reload wait for the update.
Retention that passes over that snapshot instead can delete a write the
waiting reload needs: `newer_snapshot_retention_counterexample`. -/
namespace CovenStorelogData.Pending
open CovenStorelog

/-! ## The waits one store-log scan finds -/

/-- A store-log entry: its author device and its number in that log. -/
abbrev EntryId := Nat × Nat

/-- Rust `object::first_missing`'s order: the entry's own previous entry,
then each entry it recorded reading. -/
def readinessOrder (e : EntryId) (read : List EntryId) : List EntryId :=
  (if 1 < e.2 then [(e.1, e.2 - 1)] else []) ++ read

/-- Rust `object::first_missing`. -/
def firstMissing (e : EntryId) (read : List EntryId) (applied : EntryId → Bool) :
    Option EntryId :=
  (readinessOrder e read).find? fun p => !applied p

/-- The entry's readiness conditions in that order: each entry it follows
has applied, or the entry waits for that entry's object. -/
def readiness (path : EntryId → String) (e : EntryId) (read : List EntryId)
    (applied : EntryId → Bool) : List Condition :=
  (readinessOrder e read).map fun p => ⟨applied p, .waits (.object (path p))⟩

private theorem first_map_find (path : EntryId → String) (applied : EntryId → Bool)
    (xs : List EntryId) :
    first (xs.map fun p => ⟨applied p, .waits (.object (path p))⟩) =
      (xs.find? fun p => !applied p).map fun p => .waits (.object (path p)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      cases h : applied x <;> simp [first, h, ih]

/-- Rust's search is §19.1's rule: the wait it records is the entry's first
unmet readiness condition. -/
theorem first_missing_first_unmet (path : EntryId → String) (e : EntryId)
    (read : List EntryId) (applied : EntryId → Bool) :
    first (readiness path e read applied) =
      (firstMissing e read applied).map fun p => .waits (.object (path p)) :=
  first_map_find path applied _

/-- What one scan found reading a received entry. -/
inductive Reading where
  /-- Sealed under a store key this device does not hold. -/
  | keyMissing (key : Nat)
  /-- In a format this version cannot read. -/
  | newerFormat (version : Nat)
  /-- A failed check of its complete bytes: a verdict, recorded on its own. -/
  | refused
  /-- Opened; its recorded reads. -/
  | opened (read : List EntryId)
  deriving DecidableEq, Repr

/-- Rust `pending_entries`: a reading that stops its log at that entry. -/
def Reading.stops : Reading → Bool
  | .refused | .newerFormat _ => true
  | _ => false

/-- One scan: each received entry it read, this device's queued entry with
its reads, and which entries have applied once its batch commits. -/
structure ScanInput where
  received : List (EntryId × Reading)
  own : Option (EntryId × List EntryId)
  applied : EntryId → Bool

/-- An entry behind a stopped entry of its own log is not read again. -/
def ScanInput.behind (input : ScanInput) (e : EntryId) : Bool :=
  input.received.any fun s => s.2.stops && s.1.1 == e.1 && s.1.2 < e.2

def readingWait (path : EntryId → String) (applied : EntryId → Bool) (e : EntryId) :
    Reading → Option Reason
  | .keyMissing key => some (.keyUnavailable .store key)
  | .newerFormat version => some (.updateRequired version)
  | .refused => none
  | .opened read => (firstMissing e read applied).map fun p => .waits (.object (path p))

/-- This device's queued entry waits for the first entry of its recorded past
that has not applied. -/
def ownWait (path : EntryId → String) (input : ScanInput) : List (EntryId × Reason) :=
  match input.own with
  | some (e, read) =>
      if input.applied e then []
      else ((firstMissing e read input.applied).map fun p => (e, .waits (.object (path p)))).toList
  | none => []

def receivedWaits (path : EntryId → String) (input : ScanInput) : List (EntryId × Reason) :=
  input.received.filterMap fun s =>
    if input.applied s.1 || input.behind s.1 then none
    else (readingWait path input.applied s.1 s.2).map (s.1, ·)

/-- Rust `scan_store_log_once` and `replace_entry_waits`: every read entry
that has not applied and is not behind a stop gets its first unmet condition;
the queued entry's wait replaces a received one for the same entry. -/
def scanWaits (path : EntryId → String) (input : ScanInput) : EntryScan :=
  let own := ownWait path input
  ⟨own ++ (receivedWaits path input).filter fun w => !own.any (·.1 == w.1)⟩

private theorem readingWait_wait (path : EntryId → String) (applied : EntryId → Bool)
    (e : EntryId) (r : Reading) (reason : Reason) (h : readingWait path applied e r = some reason) :
    entryWait reason = true := by
  cases r with
  | keyMissing => simp [readingWait] at h; subst h; rfl
  | newerFormat => simp [readingWait] at h; subst h; rfl
  | refused => simp [readingWait] at h
  | opened read =>
      simp only [readingWait, Option.map_eq_some_iff] at h
      obtain ⟨_, -, rfl⟩ := h
      rfl

private theorem filterMap_fst_sublist {α β γ : Type} (xs : List (α × β))
    (f : α × β → Option (α × γ)) (keeps : ∀ x y, f x = some y → y.1 = x.1) :
    ((xs.filterMap f).map (·.1)).Sublist (xs.map (·.1)) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      cases h : f x with
      | none =>
          rw [List.filterMap_cons_none h, List.map_cons]
          exact ih.cons _
      | some y =>
          rw [List.filterMap_cons_some h, List.map_cons, List.map_cons, keeps x y h]
          exact ih.cons_cons _

private theorem nodup_fst_unique {α β : Type} (xs : List (α × β))
    (distinct : (xs.map (·.1)).Nodup) (a : α) (b c : β) (hb : (a, b) ∈ xs) (hc : (a, c) ∈ xs) :
    b = c := by
  induction xs with
  | nil => cases hb
  | cons x xs ih =>
      simp only [List.map_cons, List.nodup_cons] at distinct
      rcases List.mem_cons.mp hb with hb | hb <;> rcases List.mem_cons.mp hc with hc | hc
      · rw [← hc] at hb; exact (Prod.mk.inj hb).2
      · exact absurd (List.mem_map.mpr ⟨(a, c), hc, by rw [← hb]⟩) distinct.1
      · exact absurd (List.mem_map.mpr ⟨(a, b), hb, by rw [← hc]⟩) distinct.1
      · exact ih distinct.2 hb hc

private theorem receivedWaits_ids (path : EntryId → String) (input : ScanInput) :
    ((receivedWaits path input).map (·.1)).Sublist (input.received.map (·.1)) := by
  refine filterMap_fst_sublist _ _ fun x y h => ?_
  split at h
  · simp at h
  · obtain ⟨r, -, rfl⟩ := Option.map_eq_some_iff.mp h
    rfl

private theorem ownWait_ids (path : EntryId → String) (input : ScanInput) :
    ((ownWait path input).map (·.1)).Nodup := by
  unfold ownWait
  split
  · split
    · simp
    · cases firstMissing _ _ _ <;> simp
  · simp

/-- A scan's waits are a valid replacement: Rust's commit never refuses them. -/
theorem scanWaits_valid (path : EntryId → String) (input : ScanInput)
    (distinct : (input.received.map (·.1)).Nodup) : (scanWaits path input).valid = true := by
  simp only [EntryScan.valid, Bool.and_eq_true, List.all_eq_true]
  refine ⟨fun w hw => ?_, decide_eq_true ?_⟩
  · simp only [scanWaits] at hw
    rcases List.mem_append.mp hw with hw | hw
    · unfold ownWait at hw
      split at hw
      · next e' read' _ =>
          split at hw
          · simp at hw
          · cases hf : firstMissing e' read' input.applied
            · simp [hf] at hw
            · rw [hf] at hw
              simp only [Option.map_some, Option.toList_some, List.mem_singleton] at hw
              subst hw; rfl
      · simp at hw
    · obtain ⟨s, -, h⟩ := List.mem_filterMap.mp (List.mem_filter.mp hw).1
      split at h
      · simp at h
      · obtain ⟨reason, hr, rfl⟩ := Option.map_eq_some_iff.mp h
        exact readingWait_wait path _ _ _ _ hr
  · simp only [scanWaits]
    rw [List.map_append, List.nodup_append]
    refine ⟨ownWait_ids path input, ?_, ?_⟩
    · exact (distinct.sublist (receivedWaits_ids path input)).sublist
        ((List.filter_sublist).map _)
    · intro a ha b hb e
      obtain ⟨w, hw, rfl⟩ := List.mem_map.mp ha
      obtain ⟨v, hv, rfl⟩ := List.mem_map.mp hb
      have hk := (List.mem_filter.mp hv).2
      simp only [Bool.not_eq_true', List.any_eq_false, beq_iff_eq] at hk
      exact hk w hw e

/-- The queued entry whose recorded past has not applied waits for the first
missing entry in readiness order. -/
theorem own_entry_waits (path : EntryId → String) (input : ScanInput) (e : EntryId)
    (read : List EntryId) (own : input.own = some (e, read)) (pending : input.applied e = false)
    (p : EntryId) (missing : firstMissing e read input.applied = some p) :
    (e, .waits (.object (path p))) ∈ (scanWaits path input).waits := by
  refine List.mem_append_left _ ?_
  simp [ownWait, own, pending, missing]

/-- A read entry that has not applied, is not behind a stop and is not the
queued entry gets exactly its reading's first unmet condition. -/
theorem read_entry_waits (path : EntryId → String) (input : ScanInput) (e : EntryId)
    (r : Reading) (read : (e, r) ∈ input.received) (pending : input.applied e = false)
    (ahead : input.behind e = false) (notOwn : ∀ x, input.own ≠ some (e, x))
    (reason : Reason) (waits : readingWait path input.applied e r = some reason) :
    (e, reason) ∈ (scanWaits path input).waits := by
  refine List.mem_append_right _ (List.mem_filter.mpr ⟨?_, ?_⟩)
  · exact List.mem_filterMap.mpr ⟨(e, r), read, by simp [pending, ahead, waits]⟩
  · simp only [Bool.not_eq_true', List.any_eq_false, beq_iff_eq]
    intro w hw he
    unfold ownWait at hw
    split at hw
    · next e' read' h =>
        split at hw
        · simp at hw
        · cases hf : firstMissing e' read' input.applied <;> simp [hf] at hw
          subst hw
          exact notOwn read' (by simp only at he; rw [h, he])
    · simp at hw

/-- Any entry the scan names: the queued entry, or a read entry with a wait. -/
theorem scan_names_only (path : EntryId → String) (input : ScanInput) (e : EntryId)
    (reason : Reason) (named : (e, reason) ∈ (scanWaits path input).waits) :
    input.applied e = false ∧
      ((∃ read, input.own = some (e, read)) ∨
        ∃ r, (e, r) ∈ input.received ∧ input.behind e = false ∧
          readingWait path input.applied e r = some reason) := by
  rcases List.mem_append.mp named with h | h
  · unfold ownWait at h
    split at h
    · next e' read' ho =>
        split at h
        · simp at h
        · next pending =>
            cases hf : firstMissing e' read' input.applied <;> simp [hf] at h
            obtain ⟨rfl, -⟩ := h
            exact ⟨by simpa using pending, .inl ⟨read', ho⟩⟩
    · simp at h
  · obtain ⟨s, hs, hm⟩ := List.mem_filterMap.mp (List.mem_filter.mp h).1
    split at hm
    · simp at hm
    · next ok =>
        obtain ⟨reason', hr, he⟩ := Option.map_eq_some_iff.mp hm
        simp only [Prod.mk.injEq] at he
        obtain ⟨rfl, rfl⟩ := he
        simp only [Bool.or_eq_true, not_or, Bool.not_eq_true] at ok
        exact ⟨ok.1, .inr ⟨s.2, hs, ok.2, hr⟩⟩

/-- An entry that applied keeps no wait after the scan: progress ends it. -/
theorem applied_entry_never_waits (path : EntryId → String) (input : ScanInput) (e : EntryId)
    (done : input.applied e = true) (reason : Reason) :
    (e, reason) ∉ (scanWaits path input).waits := fun h => by
  simp [(scan_names_only path input e reason h).1] at done

/-- The scan never names an entry it read as refused, so the refusal stays
(`entry_scan_keeps_verdicts`). An entry behind a stop is not read again and
gets no record of its own. -/
theorem refused_entry_never_named (path : EntryId → String) (input : ScanInput) (e : EntryId)
    (distinct : (input.received.map (·.1)).Nodup) (refused : (e, .refused) ∈ input.received)
    (notOwn : ∀ x, input.own ≠ some (e, x)) (reason : Reason) :
    (e, reason) ∉ (scanWaits path input).waits := fun h => by
  obtain ⟨-, own | ⟨r, hr, -, hw⟩⟩ := scan_names_only path input e reason h
  · obtain ⟨x, hx⟩ := own
    exact notOwn x hx
  · have : r = .refused := nodup_fst_unique _ distinct e r .refused hr refused
    subst this
    simp [readingWait] at hw

theorem behind_stop_never_named (path : EntryId → String) (input : ScanInput) (e : EntryId)
    (stopped : input.behind e = true) (notOwn : ∀ x, input.own ≠ some (e, x)) (reason : Reason) :
    (e, reason) ∉ (scanWaits path input).waits := fun h => by
  obtain ⟨-, own | ⟨r, -, ahead, -⟩⟩ := scan_names_only path input e reason h
  · obtain ⟨x, hx⟩ := own
    exact notOwn x hx
  · simp [stopped] at ahead

/-- The scan's commit: after it, this device's wait on any entry is the
first unmet condition the scan found for it, and nothing else. -/
theorem scan_decides (device : Nat) (path : EntryId → String) (input : ScanInput)
    (work : List Work) (e : EntryId) (reason : Reason) (wait : entryWait reason = true) :
    ⟨.entry e.1 e.2, device, reason⟩ ∈ records (scanEntries device (scanWaits path input) work) ↔
      (e, reason) ∈ (scanWaits path input).waits :=
  entry_scan_decides device _ work e reason wait

/-! ## A refused snapshot is never offered for loading -/

/-- What checking a listed snapshot's signed prefix found. A verified prefix
carries its positions, whether it counts (landed within a day of its signed
catch-up time and allowed by the current boundary), and whether this device
holds an authorized key for its rows. -/
inductive PrefixCheck where
  | verified (positions : List (Nat × Nat)) (counts keyKnown : Bool)
  | unmet (reason : Reason)
  deriving DecidableEq, Repr

def PrefixCheck.conditions : PrefixCheck → List Condition
  | .verified _ _ _ => []
  | .unmet reason => [⟨false, reason⟩]

/-- One audience's listing, the history its verified prefixes require, and
the snapshots offered for loading, in the listing's order. -/
structure Catalog where
  listing : Listing
  required : List (Nat × Nat)
  candidates : List SnapshotId

/-- Rust `snapshot_candidates`. A refused snapshot's verified, counting
prefix still requires its history; it is never a candidate. -/
def catalog (device : Nat) (audience : Audience) (work : List Work)
    (listed : List (SnapshotId × PrefixCheck)) : Catalog where
  listing := Listing.keepingRefusals device work
    ⟨.snapshots audience, listed.map fun s => (.snapshot s.1, s.2.conditions)⟩
  required := listed.flatMap fun s => match s.2 with
    | .verified positions true _ => positions
    | _ => []
  candidates := listed.filterMap fun s => match s.2 with
    | .verified _ true true => if refusedHere device work (.snapshot s.1) then none else some s.1
    | _ => none

/-- Loading: try the offered candidates in order. The first whose complete
body checks loads; each one before it whose body fails is refused. Rust's
reload offers only an audience's current newest snapshot and waits on it
when its body fails (`ReloadWait.choose`), so its list has at most one
element. -/
def load (body : SnapshotId → Bool) : List SnapshotId → Option SnapshotId × List SnapshotId
  | [] => (none, [])
  | id :: rest =>
      if body id then (some id, []) else ((load body rest).1, id :: (load body rest).2)

theorem refused_never_offered (device : Nat) (audience : Audience) (work : List Work)
    (listed : List (SnapshotId × PrefixCheck)) (id : SnapshotId)
    (offered : id ∈ (catalog device audience work listed).candidates) :
    refusedHere device work (.snapshot id) = false := by
  obtain ⟨s, -, h⟩ := List.mem_filterMap.mp offered
  split at h
  · split at h
    · simp at h
    · next here => simp only [Option.some.injEq] at h; subst h; simpa using here
  · simp at h

theorem loaded_checked (body : SnapshotId → Bool) (candidates : List SnapshotId)
    (id : SnapshotId) (loaded : (load body candidates).1 = some id) :
    id ∈ candidates ∧ body id = true := by
  induction candidates with
  | nil => simp [load] at loaded
  | cons c cs ih =>
      unfold load at loaded
      split at loaded
      · next ok => simp only [Option.some.injEq] at loaded; subst loaded; exact ⟨List.mem_cons_self, ok⟩
      · obtain ⟨h, ok⟩ := ih loaded
        exact ⟨List.mem_cons_of_mem _ h, ok⟩

/-- Loading never applies refused bytes: the loaded snapshot's complete body
checked, and it is not one this device had refused. -/
theorem never_loads_refused (device : Nat) (audience : Audience) (work : List Work)
    (listed : List (SnapshotId × PrefixCheck)) (body : SnapshotId → Bool) (id : SnapshotId)
    (loaded : (load body (catalog device audience work listed).candidates).1 = some id) :
    body id = true ∧ refusedHere device work (.snapshot id) = false :=
  let ⟨offered, ok⟩ := loaded_checked body _ id loaded
  ⟨ok, refused_never_offered device audience work listed id offered⟩

/-- Every snapshot passed over during loading had a body that failed. -/
theorem load_refuses_failed (body : SnapshotId → Bool) (candidates : List SnapshotId)
    (id : SnapshotId) (refused : id ∈ (load body candidates).2) : body id = false := by
  induction candidates with
  | nil => simp [load] at refused
  | cons c cs ih =>
      unfold load at refused
      split at refused
      · simp at refused
      · next failed =>
          rcases List.mem_cons.mp refused with rfl | h
          · simpa using failed
          · exact ih h

/-- A refused snapshot's verified, counting prefix still requires its history. -/
theorem refused_prefix_requires_history (device : Nat) (audience : Audience) (work : List Work)
    (listed : List (SnapshotId × PrefixCheck)) (id : SnapshotId) (positions : List (Nat × Nat))
    (keyKnown : Bool) (present : (id, .verified positions true keyKnown) ∈ listed)
    (p : Nat × Nat) (hp : p ∈ positions) : p ∈ (catalog device audience work listed).required :=
  List.mem_flatMap.mpr ⟨_, present, hp⟩

/-- Once loading refuses a body, the refusal is this device's record of that
snapshot, and every later listing that still names it keeps it. -/
theorem refusal_survives_listing (s : Local) (a : Audience)
    (observed : List (Subject × List Condition)) (id : SnapshotId)
    (here : refusedHere s.device s.work (.snapshot id) = true)
    (listed : ∃ cs, (.snapshot id, cs) ∈ observed) :
    refusedHere s.device (s.step (.listing ⟨.snapshots a, observed⟩)).work (.snapshot id) = true := by
  obtain ⟨r, hr, hm⟩ := List.any_eq_true.mp here
  simp only [Bool.and_eq_true, beq_iff_eq] at hm
  obtain ⟨⟨hs, ho⟩, hreason⟩ := hm
  have kept : r ∈ records (s.step (.listing ⟨.snapshots a, observed⟩)).work := by
    simp only [Local.step]
    split
    · exact listing_keeps_refusal s.device ⟨.snapshots a, observed⟩ s.work r a rfl hr ho hreason
        (by rw [hs]; exact listed)
    · exact hr
  exact List.any_eq_true.mpr ⟨r, kept, by simp [hs, ho, hreason]⟩

theorem step_device (s : Local) (step : Step) : (s.step step).device = s.device := by
  cases step <;> simp only [Local.step] <;> (try split) <;> rfl

/-- A refusal survives every later listing that still names its snapshot. -/
theorem refusal_survives_listings (a : Audience) (id : SnapshotId)
    (later : List (List (Subject × List Condition)))
    (listed : ∀ observed ∈ later, ∃ cs, (.snapshot id, cs) ∈ observed) (t : Local)
    (here : refusedHere t.device t.work (.snapshot id) = true) :
    refusedHere t.device (t.run (later.map fun o => .listing ⟨.snapshots a, o⟩)).work
      (.snapshot id) = true := by
  induction later generalizing t with
  | nil => exact here
  | cons o rest ih =>
      have next := refusal_survives_listing t a o id here (listed o List.mem_cons_self)
      have after := ih (fun o' h' => listed o' (List.mem_cons_of_mem _ h'))
        (t.step (.listing ⟨.snapshots a, o⟩)) (by rw [step_device]; exact next)
      rw [step_device] at after
      exact after

/-- The refusal a failed load records keeps that snapshot out of every later
catalog for as long as listings name it: its bytes are never offered again. -/
theorem refused_stays_unoffered (s : Local) (a : Audience) (id : SnapshotId)
    (later : List (List (Subject × List Condition)))
    (listed : ∀ observed ∈ later, ∃ cs, (.snapshot id, cs) ∈ observed)
    (listedNow : List (SnapshotId × PrefixCheck)) :
    id ∉ (catalog s.device a (s.run (.observe ⟨.snapshot id, s.device, [⟨false, .refused⟩]⟩ ::
      later.map fun o => .listing ⟨.snapshots a, o⟩)).work listedNow).candidates := by
  intro offered
  let refusal : Step := .observe ⟨.snapshot id, s.device, [⟨false, .refused⟩]⟩
  have start : refusedHere (s.step refusal).device (s.step refusal).work (.snapshot id) = true :=
    List.any_eq_true.mpr ⟨⟨.snapshot id, s.device, .refused⟩,
      observed_wait_visible _ _ _ (by simp [first]), by simp [step_device]⟩
  have kept : refusedHere s.device (s.run (refusal :: later.map fun o =>
      .listing ⟨.snapshots a, o⟩)).work (.snapshot id) = true :=
    refusal_survives_listings a id later listed (s.step refusal) start
  exact Bool.false_ne_true ((refused_never_offered s.device a _ listedNow id offered).symm.trans kept)

/-! ## A newer-format snapshot: the reload waits, retention must too

A listed snapshot whose prefix this version cannot read could be the
audience's newest (§15). The reload and the return check wait for the update
rather than choose without it. Rust's retention waits too; judging deletion
against the newest readable snapshot instead loses a write. -/

/-- A listed snapshot: its path `(device, number)`, its signed positions
`(device, highest number)` and the format of its prefix. -/
structure Listed where
  path : Nat × Nat
  writes : List (Nat × Nat)
  format : Nat
  deriving DecidableEq, Repr

def Listed.readable (supported : Nat) (s : Listed) : Bool := decide (s.format ≤ supported)

/-- Writes covered, counted over every log. -/
def Listed.count (s : Listed) : Nat := (s.writes.map (·.2)).sum

def Listed.covers (s : Listed) (w : Nat × Nat) : Bool :=
  s.writes.any fun p => p.1 == w.1 && decide (w.2 ≤ p.2)

/-- §15's ordering: more writes covered, then the smaller path. -/
def Listed.before (a b : Listed) : Bool :=
  decide (b.count < a.count) ||
    (a.count == b.count && (decide (a.path.1 < b.path.1) ||
      (a.path.1 == b.path.1 && decide (a.path.2 < b.path.2))))

/-- The latest snapshot of a listing. -/
def newest (listed : List Listed) : Option Listed :=
  listed.foldl (fun best s => match best with
    | none => some s
    | some b => if s.before b then some s else some b) none

/-- The reload and the return check: wait for the update while any listed
prefix is newer than this version reads; otherwise select the newest. -/
def select (supported : Nat) (listed : List Listed) : Except Reason (Option Listed) :=
  match listed.find? fun s => !s.readable supported with
  | some s => .error (.updateRequired s.format)
  | none => .ok (newest listed)

/-- Retention's coverage test before it waited for such a prefix: the newest
snapshot among those whose prefixes this version reads covers it. The other
§15 gates (historical coverage a day old, posted positions or 30 storage
days, finality) are separate conjuncts, taken to pass. -/
def retentionCovers (supported : Nat) (listed : List Listed) (w : Nat × Nat) : Bool :=
  match newest (listed.filter (·.readable supported)) with
  | some s => s.covers w
  | none => false

/-- What a reload needs from the logs: every write up to its fixed target
that its selected snapshot does not cover (§15). -/
def needs (selected : Listed) (target : Nat → Nat) (w : Nat × Nat) : Bool :=
  decide (w.2 ≤ target w.1) && !selected.covers w

/-- The reload never selects while a newer-format snapshot is listed. -/
theorem reload_waits_for_newer (supported : Nat) (listed : List Listed) (s : Listed)
    (present : s ∈ listed) (newer : s.readable supported = false) :
    ∃ v, select supported listed = .error (.updateRequired v) := by
  unfold select
  cases h : listed.find? fun s => !s.readable supported with
  | some t => exact ⟨t.format, rfl⟩
  | none =>
      have := List.find?_eq_none.mp h s present
      simp [newer] at this

/-- Ana (device 1) and Ben (device 2) snapshot the store concurrently. Ana's
snapshot R covers her writes through 10 and Ben's through 2; Ben's newer
version writes N in format 2, covering Ana through 3 and himself through 20.
N covers more writes, so it is the store's newest (§15). On Ana's version:
- her reload waits for the update, since N's prefix is unreadable;
- her retention judges coverage by R, which covers her write 4, and deletes it;
- after the update the reload selects N, which does not cover write 4, while
  Ana's own uploaded writes through 10 are in its fixed target.
Rule 3 lets retention delete what the waiting reload needs. -/
def anaSnapshot : Listed := ⟨(1, 1), [(1, 10), (2, 2)], 1⟩
def benSnapshot : Listed := ⟨(2, 1), [(1, 3), (2, 20)], 2⟩

theorem newer_snapshot_retention_counterexample :
    select 1 [anaSnapshot, benSnapshot] = .error (.updateRequired 2) ∧
      retentionCovers 1 [anaSnapshot, benSnapshot] (1, 4) = true ∧
      select 2 [anaSnapshot, benSnapshot] = .ok (some benSnapshot) ∧
      needs benSnapshot (fun d => if d = 1 then 10 else 20) (1, 4) = true :=
  ⟨rfl, by decide, rfl, by decide⟩

/-- The safe rule: retention that waits, like the reload, while any listed
prefix is unreadable judges against the snapshot the reload will select once
it proceeds, at this version or any later one, so it never deletes a write
that reload needs. -/
def retentionCoversAll (supported : Nat) (listed : List Listed) (w : Nat × Nat) : Bool :=
  listed.all (·.readable supported) && retentionCovers supported listed w

theorem waiting_retention_safe (supported later : Nat) (listed : List Listed) (w : Nat × Nat)
    (deletes : retentionCoversAll supported listed w = true) (updated : supported ≤ later)
    (target : Nat → Nat) :
    ∃ s, select later listed = .ok (some s) ∧ needs s target w = false := by
  simp only [retentionCoversAll, Bool.and_eq_true, List.all_eq_true] at deletes
  obtain ⟨readable, covers⟩ := deletes
  have all : listed.filter (·.readable supported) = listed := List.filter_eq_self.mpr readable
  unfold retentionCovers at covers
  rw [all] at covers
  have none : listed.find? (fun s => !s.readable later) = none := by
    apply List.find?_eq_none.mpr
    intro s hs
    have := readable s hs
    simp only [Listed.readable, decide_eq_true_eq] at this ⊢
    simp only [Bool.not_eq_true', decide_eq_false_iff_not, Nat.not_le]
    omega
  cases h : newest listed with
  | none => simp [h] at covers
  | some s =>
      refine ⟨s, by simp [select, none, h], ?_⟩
      simp only [h] at covers
      simp [needs, covers]

end CovenStorelogData.Pending
