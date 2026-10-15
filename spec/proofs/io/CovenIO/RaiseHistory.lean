import CovenIO.RaiseSelection

namespace CovenIO.RaiseCoverage

/-! ## Recapture and the hold: no reload skips a deleted write

The rules decided for T200 and completed by T216. A raise snapshot is
prepared like any other: immediately after a completed catch-up whose storage
sample is its `T_c`, so it is `SnapshotHistory.Sound`. A migration's capture
is sent only when that catch-up applied nothing past it
(`unchanged_capture_prepares`); otherwise the device captures again, and its
later local writes join the captured prefix. Its signed prefix marks it as a
raise snapshot, so retention sees it while no entry names it yet.

Deleting a write's parts then needs coverage under every boundary replay
could still select for each part audience (`Held`): the current one, the
one final entries fix, every raise or reset entry not ruled out, and each
listed counting raise snapshot that a raise could still name. Under each, the
newest usable counting snapshot must cover the write.

A raise entry may name any snapshot: one not listed yet, one that never
counts, or one whose prefix does not mark the raise. Replay can keep such an
entry; the reload then refuses its boundary and waits (Rust
`snapshot_boundaries`), so only a boundary whose named snapshot landed,
counts and marks the raise is `Usable`.

A raise entry counts only if it is stored at most one day after its
snapshot's `T_c`, which it carries (§9; replay drops it as landed too late
otherwise). So once storage time passes that day and no entry names a listed
raise snapshot, no raise entry ever will: it stops being a possible boundary
(`expire`). Its author being removed, or simply gone, holds nothing longer
than that day (`expired_stays_out`, `listed_raise_hold_ends`).

A reset entry (§19.3) is a boundary too, and the hold checks it like a
raise entry while replay could still select it: a reset that is dropped now
but not final can be kept later. Under a kept reset only its snapshot and
later writes that read its entry count; a write outside the snapshot that
did not read the entry is ignored, so skipping it is the reset's rule, not a
loss. A write reads an entry only after it is stored, so a write stored
before the entry never read it (`Applies`). The reset's snapshot marks no
raise, so retention cannot see it before its entry, and needs no hold on it:
a write deleted by then never read the entry, so it counts under the reset
only if the reset's snapshot covers it (`publish_valid`).
`dropped_reset_skips` shows a hold without the non-final resets losing a
write. -/
namespace RaiseHistory

open SnapshotHistory

structure State where
  time : Nat
  landed : List Snapshot
  /-- Landed snapshots whose signed prefix marks a raise. -/
  marked : List Snapshot
  deleted : List Write
  reads : List (Snapshot × Snapshot)
  /-- Snapshots named by a published entry: a raise stored within its
  window, or a reset. -/
  entries : List Snapshot
  /-- The boundary replay currently keeps for each audience. -/
  kept : Nat → Option Snapshot
  /-- The boundaries replay could still select for each audience. -/
  possible : Nat → List (Option Snapshot)
  /-- Raise snapshots finality ruled out: replay can no longer select one. -/
  ruledOut : List Snapshot
  /-- Listed raise snapshots no entry named by a day after their `T_c`. -/
  expired : List Snapshot
  /-- The snapshots named by published reset entries, each with the storage
  time its entry was stored at; these are among `entries`. -/
  resets : List (Snapshot × Nat)

def initial : State := ⟨0, [], [], [], [], [], fun _ => none, fun _ => [none], [], [], []⟩

/-- Whether a reset entry names this snapshot; otherwise its entries raise. -/
def isReset (s : State) (r : Snapshot) : Bool := decide (r ∈ s.resets.map Prod.fst)

/-- A boundary whose named snapshot did not land, does not count, or does
not mark the raise is refused by the reload, which waits; a reset's named
snapshot must mark none. -/
def marking (s : State) (r : Snapshot) : Bool :=
  if isReset s r then !decide (r ∈ s.marked) else decide (r ∈ s.marked)

def usable (s : State) : Option Snapshot → Bool
  | none => true
  | some r => decide (r ∈ s.landed) && r.counts && marking s r

theorem usable_some (s : State) (r : Snapshot) :
    usable s (some r) = (decide (r ∈ s.landed) && r.counts && marking s r) := rfl

/-- Usability depends only on the named snapshot's landing, mark and kind. -/
theorem usable_of {s t : State} {r : Snapshot} (use : usable t (some r) = true)
    (landed : decide (r ∈ t.landed) = decide (r ∈ s.landed))
    (marked : decide (r ∈ t.marked) = decide (r ∈ s.marked))
    (reset : isReset t r = isReset s r) : usable s (some r) = true := by
  rw [usable_some, landed] at use
  unfold marking at use
  rw [reset, marked] at use
  exact use

theorem marked_reset_unusable (s : State) (r : Snapshot) (reset : isReset s r = true)
    (marked : r ∈ s.marked) : usable s (some r) = false := by
  simp [usable_some, marking, reset, marked]

theorem unmarked_raise_unusable (s : State) (r : Snapshot) (raise : isReset s r = false)
    (unmarked : r ∉ s.marked) : usable s (some r) = false := by
  simp [usable_some, marking, raise, unmarked]

/-- Whether write `w`'s part in audience `a` counts under a boundary. Under a
reset only the snapshot's contents and writes that read its entry count
(§19.3); a write that read the entry was stored after it. -/
def Applies (s : State) (w : Write) (a : Nat) : Option Snapshot → Prop
  | none => True
  | some r => isReset s r = true →
      (∃ t, (r, t) ∈ s.resets ∧ t ≤ w.storedAt) ∨ r.covers w a

def excluded (s : State) (r : Snapshot) : Bool :=
  decide (r ∈ s.ruledOut) || decide (r ∈ s.expired)

/-- The hold: coverage under every possible boundary of every part audience. -/
def Held (order : SnapshotOrder) (s : State) (w : Write) : Prop :=
  ∀ a ∈ w.holders, ∀ b ∈ s.possible a, CoversIn (selected order s.landed s.reads a b) w a

instance (order : SnapshotOrder) (s : State) (w : Write) : Decidable (Held order s w) := by
  unfold Held; infer_instance

def addPossible (possible : Nat → List (Option Snapshot)) (snap : Snapshot) :
    Nat → List (Option Snapshot) :=
  fun a => if a = snap.audience then some snap :: possible a else possible a

/-- The snapshots a finalization removes from audience `a`'s boundaries. -/
def removed (possible : List (Option Snapshot)) (remaining : List (Option Snapshot)) :
    List Snapshot :=
  (possible.filterMap id).filter fun r => !decide (some r ∈ remaining)

/-- The state once an entry naming `r` is published: a raise, or a reset
(`reset`) stored now. -/
def State.withEntry (s : State) (r : Snapshot) (reset : Bool) : State :=
  { s with
    entries := r :: s.entries
    possible := if excluded s r then s.possible else addPossible s.possible r
    resets := if reset then (r, s.time) :: s.resets else s.resets }

/-- Catch-up, reads and finality decisions happen outside this boundary.
Replay keeps only an entry's boundary among the possible ones, and finality
drops only boundaries replay can no longer select; the Rust replay
obligations behind those premises are listed in io.md. -/
inductive Step (published : Write → Prop) (order : SnapshotOrder) : State → State → Prop
  | tick (s : State) (time : Nat) (later : s.time ≤ time) :
      Step published order s { s with time := time }
  /-- A sound snapshot lands with the published raise entries its signed
  view read; an entry names a path, so a snapshot landing at a named path is
  the one named. A counting raise snapshot becomes a possible boundary unless
  finality or expiry already ruled it out. -/
  | land (s : State) (snap : Snapshot) (read : List Snapshot) (raise : Bool)
      (now : snap.storedAt = s.time) (sound : Sound published snap)
      (fresh : ∀ x ∈ s.landed, samePath x snap = false)
      (named : ∀ n ∈ read, n ∈ s.entries)
      (path : ∀ e ∈ s.entries, samePath e snap = true → e = snap)
      (counting : raise = true → snap.counts = true) :
      Step published order s { s with
        landed := snap :: s.landed
        marked := if raise then snap :: s.marked else s.marked
        reads := read.map (snap, ·) ++ s.reads
        possible := if raise && !excluded s snap then addPossible s.possible snap else s.possible }
  /-- A raise entry is stored within a day of its snapshot's `T_c`; it may
  name a snapshot not yet listed, or one that never counts. A reset entry
  (`reset`) has no such window; replay can keep it while it is not final.
  An entry names its author's snapshot of its own kind: a snapshot already
  named by an entry is named again only by one of the same kind. -/
  | publish (s : State) (r : Snapshot) (reset : Bool)
      (onTime : reset = false → s.time ≤ r.caughtAt + day)
      (path : ∀ x ∈ s.landed, samePath x r = true → x = r)
      (kind : r ∈ s.entries → isReset s r = reset) :
      Step published order s (s.withEntry r reset)
  | replay (s : State) (a : Nat) (b : Option Snapshot) (possible : b ∈ s.possible a)
      (named : ∀ r, b = some r → r ∈ s.entries) :
      Step published order s { s with kept := fun x => if x = a then b else s.kept x }
  | finalize (s : State) (a : Nat) (remaining : List (Option Snapshot))
      (narrower : ∀ b ∈ remaining, b ∈ s.possible a) (keeps : s.kept a ∈ remaining) :
      Step published order s { s with
        possible := fun x => if x = a then remaining else s.possible x
        ruledOut := removed (s.possible a) remaining ++ s.ruledOut }
  /-- A day after its `T_c` with no entry naming it, a listed raise snapshot
  stops being a possible boundary: no entry stored later can name it. -/
  | expire (s : State) (x : Snapshot) (late : x.caughtAt + day < s.time)
      (unnamed : x ∉ s.entries) :
      Step published order s { s with
        possible := fun a => if a = x.audience then (s.possible a).filter (· != some x)
          else s.possible a
        expired := x :: s.expired }
  /-- §15's deletion gates with the hold; finality and reader positions are
  further gates that this invariant does not need. -/
  | delete (s : State) (w : Write) (publishedWrite : published w)
      (mature : MatureCoverage s.landed s.time w) (held : Held order s w) :
      Step published order s { s with deleted := w :: s.deleted }

inductive Run (published : Write → Prop) (order : SnapshotOrder) : State → Prop
  | initial : Run published order initial
  | step {s t : State} : Run published order s → Step published order s t → Run published order t

/-- What a deleted write keeps under one possible boundary where it counts:
coverage once the boundary is usable, and, while its named snapshot has not
landed, coverage by whatever its readers select. -/
def Good (order : SnapshotOrder) (s : State) (w : Write) (a : Nat) (b : Option Snapshot) : Prop :=
  Applies s w a b → ((∀ r, b = some r → r ∉ s.landed →
      selected order s.landed s.reads a b = none ∨ CoversIn (selected order s.landed s.reads a b) w a) ∧
    (usable s b = true → CoversIn (selected order s.landed s.reads a b) w a))

structure Valid (published : Write → Prop) (order : SnapshotOrder) (s : State) : Prop where
  sound : ∀ snap ∈ s.landed, Sound published snap
  reads : ∀ p ∈ s.reads, p.2 ∈ s.entries
  keptPossible : ∀ a, s.kept a ∈ s.possible a
  keptNamed : ∀ a r, s.kept a = some r → r ∈ s.entries
  markedLanded : ∀ m ∈ s.marked, m ∈ s.landed ∧ m.counts = true
  markedPossible : ∀ m ∈ s.marked, excluded s m = false → some m ∈ s.possible m.audience
  entriesPossible : ∀ r ∈ s.entries, excluded s r = false → some r ∈ s.possible r.audience
  audience : ∀ a r, some r ∈ s.possible a → r.audience = a
  paths : ∀ x ∈ s.landed, ∀ r ∈ s.entries, samePath x r = true → x = r
  expiredOut : ∀ x ∈ s.expired, some x ∉ s.possible x.audience
  expiredUnnamed : ∀ x ∈ s.expired, (x ∈ s.entries → isReset s x = true) ∧ x.caughtAt + day < s.time
  deleted : ∀ w ∈ s.deleted, published w ∧ MatureCoverage s.landed s.time w ∧
    ∀ a ∈ w.holders, ∀ b ∈ s.possible a, Good order s w a b
  resetsNamed : ∀ p ∈ s.resets, p.1 ∈ s.entries
  possibleSource : ∀ a r, some r ∈ s.possible a → r ∈ s.entries ∨ r ∈ s.marked

/-- A counting sound snapshot landing after a deletion covers the deleted
write: its catch-up began after the write's mature witness landed. -/
theorem late_landing_covers {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (valid : Valid published order s) (w : Write) (deleted : w ∈ s.deleted)
    (snap : Snapshot) (now : snap.storedAt = s.time) (sound : Sound published snap)
    (counts : snap.counts = true) (relevant : snap.audience ∈ w.holders) :
    snap.covers w snap.audience := by
  obtain ⟨pub, mature, _⟩ := valid.deleted w deleted
  obtain ⟨witness, member, _, covers, age⟩ := mature snap.audience relevant
  have causal := (valid.sound witness member).causal w pub (covers.1 ▸ relevant)
    (by simpa [covers.1] using covers)
  have admission : snap.storedAt ≤ snap.caughtAt + day := by simpa [Snapshot.counts] using counts
  exact sound.caught w pub relevant (by omega)


theorem excluded_mono {s t : State} (r : Snapshot)
    (ruled : ∀ x, x ∈ s.ruledOut → x ∈ t.ruledOut) (gone : ∀ x, x ∈ s.expired → x ∈ t.expired)
    (was : excluded s r = true) : excluded t r = true := by
  simp only [excluded, Bool.or_eq_true, decide_eq_true_eq] at was ⊢
  rcases was with h | h
  · exact .inl (ruled r h)
  · exact .inr (gone r h)

/-- The landing step preserves every part of the invariant. -/
theorem land_valid {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (valid : Valid published order s) (snap : Snapshot) (read : List Snapshot) (raise : Bool)
    (now : snap.storedAt = s.time) (good : Sound published snap)
    (fresh : ∀ x ∈ s.landed, samePath x snap = false) (named : ∀ n ∈ read, n ∈ s.entries)
    (path : ∀ e ∈ s.entries, samePath e snap = true → e = snap)
    (counting : raise = true → snap.counts = true) :
    Valid published order { s with
        landed := snap :: s.landed
        marked := if raise then snap :: s.marked else s.marked
        reads := read.map (snap, ·) ++ s.reads
        possible := if raise && !excluded s snap then addPossible s.possible snap else s.possible } := by
  have notLanded : snap ∉ s.landed := fun member => by
    have := fresh snap member; simp [samePath_self] at this
  have sameExcluded : ∀ r, excluded { s with
      landed := snap :: s.landed
      marked := if raise then snap :: s.marked else s.marked
      reads := read.map (snap, ·) ++ s.reads
      possible := if raise && !excluded s snap then addPossible s.possible snap else s.possible } r =
      excluded s r := fun _ => rfl
  -- The new possible lists contain the old ones, plus `some snap` when it is added.
  have grows : ∀ a b, b ∈ s.possible a →
      b ∈ (if raise && !excluded s snap then addPossible s.possible snap else s.possible) a := by
    intro a b hb
    split
    · simp only [addPossible]; split <;> simp [hb]
    · exact hb
  have shape : ∀ a b, b ∈ (if raise && !excluded s snap then addPossible s.possible snap
      else s.possible) a → b ∈ s.possible a ∨ (b = some snap ∧ a = snap.audience ∧ raise = true ∧
        excluded s snap = false) := by
    intro a b hb
    split at hb
    · rename_i cond
      simp only [Bool.and_eq_true, Bool.not_eq_true'] at cond
      simp only [addPossible] at hb
      split at hb
      · rcases List.mem_cons.mp hb with rfl | old
        · exact .inr ⟨rfl, by assumption, cond.1, cond.2⟩
        · exact .inl old
      · exact .inl hb
    · exact .inl hb
  constructor
  · intro other member
    rcases List.mem_cons.mp member with rfl | old
    · exact good
    · exact valid.sound other old
  · intro p hp
    rcases List.mem_append.mp hp with fromRead | old
    · obtain ⟨n, hn, rfl⟩ := List.mem_map.mp fromRead
      exact named n hn
    · exact valid.reads p old
  · intro a; exact grows a _ (valid.keptPossible a)
  · exact valid.keptNamed
  · intro m hm
    split at hm
    · rcases List.mem_cons.mp hm with rfl | old
      · rename_i isRaise
        exact ⟨List.mem_cons_self, counting isRaise⟩
      · obtain ⟨l, c⟩ := valid.markedLanded m old
        exact ⟨List.mem_cons_of_mem _ l, c⟩
    · obtain ⟨l, c⟩ := valid.markedLanded m hm
      exact ⟨List.mem_cons_of_mem _ l, c⟩
  · intro m hm notEx
    rw [sameExcluded] at notEx
    split at hm
    · rename_i isRaise
      rcases List.mem_cons.mp hm with rfl | old
      · simp [isRaise, notEx, addPossible]
      · exact grows _ _ (valid.markedPossible m old notEx)
    · exact grows _ _ (valid.markedPossible m hm notEx)
  · intro r hr notEx
    exact grows _ _ (valid.entriesPossible r hr notEx)
  · intro a r hr
    rcases shape a _ hr with old | ⟨eq, audience, _, _⟩
    · exact valid.audience a r old
    · cases eq; exact audience.symm
  · intro x hx r hr same
    rcases List.mem_cons.mp hx with rfl | old
    · exact (path r hr (by rw [samePath_symm]; exact same)).symm
    · exact valid.paths x old r hr same
  · intro x hx
    intro member
    rcases shape _ _ member with old | ⟨eq, _, _, notEx⟩
    · exact valid.expiredOut x hx old
    · cases eq
      simp [excluded, hx] at notEx
  · exact valid.expiredUnnamed
  · intro w hw
    obtain ⟨pub, mature, before⟩ := valid.deleted w hw
    refine ⟨pub, maturity_survives_new_snapshots s.landed [snap] s.time w mature, ?_⟩
    intro a ha b hb app
    have covers : snap.audience = a → snap.counts = true → snap.covers w a := by
      intro same counted
      subst same
      exact late_landing_covers valid w hw snap now good counted ha
    have step := landed_selection order s.landed s.reads read snap a b fresh w covers
    -- Old candidates under `some snap` exist only if an entry names it, and
    -- then `some snap` was already possible.
    have own : b = some snap →
        (selected order s.landed s.reads a (some snap) = none ∨
          CoversIn (selected order s.landed s.reads a (some snap)) w a) := by
      intro isSnap
      subst isSnap
      rcases shape a (some snap) hb with old | ⟨_, audience, _, notEx⟩
      · exact (before a ha (some snap) old app).1 snap rfl notLanded
      · cases sel : selected order s.landed s.reads a (some snap) with
        | none => exact .inl rfl
        | some x =>
          have member := newest_member order _ x sel
          simp only [List.mem_filter, Bool.and_eq_true, beq_iff_eq] at member
          obtain ⟨landedX, _, admitted⟩ := member
          simp only [admits, Bool.or_eq_true, decide_eq_true_eq] at admitted
          have entry : snap ∈ s.entries := by
            rcases admitted with samePathX | readX
            · have := fresh x landedX; simp_all
            · exact valid.reads _ readX
          have possibleOld := valid.entriesPossible snap entry notEx
          rw [← audience] at possibleOld
          have g := (before a ha (some snap) possibleOld app).1 snap rfl notLanded
          rw [sel] at g
          exact g
    constructor
    · intro r hr notNow
      have notOld : r ∉ s.landed := fun m => notNow (List.mem_cons_of_mem _ m)
      have prior : selected order s.landed s.reads a b = none ∨
          CoversIn (selected order s.landed s.reads a b) w a := by
        rcases shape a b hb with old | ⟨eq, _, _, _⟩
        · exact (before a ha b old app).1 r hr notOld
        · subst hr; cases eq; exact absurd List.mem_cons_self notNow
      exact (step prior).1
    · intro use
      rcases shape a b hb with old | ⟨eq, audience, isRaise, notEx⟩
      · cases b with
        | none =>
          exact (step (.inr ((before a ha none old app).2 rfl))).2 ((before a ha none old app).2 rfl)
        | some r =>
          have whole := use
          simp only [usable, Bool.and_eq_true, decide_eq_true_eq] at use
          obtain ⟨⟨landedR, countsR⟩, _⟩ := use
          by_cases isSnap : r = snap
          · subst isSnap
            have prior := own rfl
            rcases (step prior).1 with none | covered
            · exfalso
              unfold selected at none
              rw [newest_none] at none
              have mem : r ∈ (r :: s.landed).filter fun x => x.audience == a && x.counts &&
                  admits (read.map (r, ·) ++ s.reads) (some r) x := by
                simp [List.mem_filter, admits, samePath_self, countsR, valid.audience a r old]
              rw [none] at mem
              simp at mem
            · exact covered
          · have oldLanded : r ∈ s.landed := by
              rcases List.mem_cons.mp landedR with eq | old
              · exact absurd eq isSnap
              · exact old
            have markedSame : decide (r ∈ (if raise then snap :: s.marked else s.marked)) =
                decide (r ∈ s.marked) := by
              split <;> simp [isSnap]
            have landedSame : decide (r ∈ snap :: s.landed) = decide (r ∈ s.landed) := by
              simp [isSnap]
            have oldUse : usable s (some r) = true :=
              usable_of whole landedSame markedSame rfl
            exact (step (.inr ((before a ha (some r) old app).2 oldUse))).2
              ((before a ha (some r) old app).2 oldUse)
      · cases eq
        have prior := own rfl
        rcases (step prior).1 with none | covered
        · exfalso
          unfold selected at none
          rw [newest_none] at none
          have counted := counting isRaise
          have mem : snap ∈ (snap :: s.landed).filter fun x => x.audience == a && x.counts &&
              admits (read.map (snap, ·) ++ s.reads) (some snap) x := by
            simp [List.mem_filter, admits, samePath_self, counted, audience]
          rw [none] at mem
          simp at mem
        · exact covered
  · exact valid.resetsNamed
  · intro a r hr
    rcases shape a _ hr with old | ⟨eq, _, isRaise, _⟩
    · rcases valid.possibleSource a r old with entry | marked
      · exact .inl entry
      · right; split
        · exact List.mem_cons_of_mem _ marked
        · exact marked
    · cases eq; right; simp [isRaise]

/-- A deleted write was stored before now: its mature witness covers it. -/
theorem deleted_before {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (valid : Valid published order s) (w : Write) (deleted : w ∈ s.deleted) (a : Nat)
    (relevant : a ∈ w.holders) : w.storedAt < s.time := by
  obtain ⟨pub, mature, _⟩ := valid.deleted w deleted
  obtain ⟨witness, member, _, covers, age⟩ := mature a relevant
  have causal := (valid.sound witness member).causal w pub (covers.1 ▸ relevant)
    (by simpa [covers.1] using covers)
  omega

/-- Publishing a raise or reset entry preserves every part of the invariant.
A reset that becomes a possible boundary only now holds nothing back: every
write deleted before it was stored before its entry, so under it that write
counts only if its snapshot covers it, and then its snapshot is its only
usable selection. -/
theorem publish_valid {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (valid : Valid published order s) (r : Snapshot) (reset : Bool)
    (onTime : reset = false → s.time ≤ r.caughtAt + day)
    (path : ∀ x ∈ s.landed, samePath x r = true → x = r)
    (kind : r ∈ s.entries → isReset s r = reset) :
    Valid published order (s.withEntry r reset) := by
  have sameExcluded : ∀ x, excluded (s.withEntry r reset) x =
      excluded s x := fun _ => rfl
  have grows : ∀ a b, b ∈ s.possible a →
      b ∈ (if excluded s r then s.possible else addPossible s.possible r) a := by
    intro a b hb
    split
    · exact hb
    · simp only [addPossible]; split <;> simp [hb]
  have shape : ∀ a b, b ∈ (if excluded s r then s.possible else addPossible s.possible r) a →
      b ∈ s.possible a ∨ (b = some r ∧ a = r.audience ∧ excluded s r = false) := by
    intro a b hb
    split at hb
    · exact .inl hb
    · rename_i notEx
      simp only [addPossible] at hb
      split at hb
      · rcases List.mem_cons.mp hb with rfl | old
        · exact .inr ⟨rfl, by assumption, by simpa using notEx⟩
        · exact .inl old
      · exact .inl hb
  -- Which snapshots reset entries name after the publication.
  have resetOther : ∀ x, x ≠ r → isReset (s.withEntry r reset) x = isReset s x := by
    intro x ne
    cases reset with
    | false => rfl
    | true => exact decide_eq_decide.mpr (by simp [State.withEntry, ne])
  have resetGrows : ∀ x, isReset s x = true → isReset (s.withEntry r reset) x = true := by
    intro x was
    cases reset with
    | false => exact was
    | true =>
      simp only [isReset, decide_eq_true_eq] at was ⊢
      exact List.mem_cons_of_mem _ was
  have resetNamed : reset = true → isReset (s.withEntry r reset) r = true := by
    intro isR; subst isR
    simp only [isReset, decide_eq_true_eq]
    exact List.mem_cons_self
  have resetSame : reset = false → ∀ x, isReset (s.withEntry r reset) x = isReset s x := by
    intro notR x; subst notR; rfl
  -- A pair naming `r` that the publication adds is stored now, after every
  -- deleted write.
  have resetPairs : ∀ x t, (x, t) ∈ (if reset then (r, s.time) :: s.resets else s.resets) →
      (x, t) ∈ s.resets ∨ (x = r ∧ t = s.time) := by
    intro x t mem
    split at mem
    · rcases List.mem_cons.mp mem with eq | old
      · cases eq; exact .inr ⟨rfl, rfl⟩
      · exact .inl old
    · exact .inl mem
  refine ⟨valid.sound, fun p hp => List.mem_cons_of_mem _ (valid.reads p hp),
    fun a => grows a _ (valid.keptPossible a),
    fun a x hx => List.mem_cons_of_mem _ (valid.keptNamed a x hx), valid.markedLanded,
    fun m hm notEx => grows _ _ (valid.markedPossible m hm (by rwa [sameExcluded] at notEx)),
    ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro x hx notEx
    rw [sameExcluded] at notEx
    rcases List.mem_cons.mp hx with rfl | old
    · simp [State.withEntry, notEx, addPossible]
    · exact grows _ _ (valid.entriesPossible x old notEx)
  · intro a x hx
    rcases shape a _ hx with old | ⟨eq, audience, _⟩
    · exact valid.audience a x old
    · cases eq; exact audience.symm
  · intro x hx e he same
    rcases List.mem_cons.mp he with rfl | old
    · exact path x hx same
    · exact valid.paths x hx e old same
  · intro x hx member
    rcases shape _ _ member with old | ⟨eq, _, notEx⟩
    · exact valid.expiredOut x hx old
    · cases eq
      simp only [excluded, Bool.or_eq_false_iff, decide_eq_false_iff_not] at notEx
      exact notEx.2 hx
  · intro x hx
    obtain ⟨unnamed, late⟩ := valid.expiredUnnamed x hx
    refine ⟨?_, late⟩
    intro member
    rcases List.mem_cons.mp member with rfl | old
    · cases reset with
      | true => exact resetNamed rfl
      | false => have := onTime rfl; omega
    · exact resetGrows x (unnamed old)
  · intro w hw
    obtain ⟨pub, mature, before⟩ := valid.deleted w hw
    refine ⟨pub, mature, ?_⟩
    intro a ha b hb app
    have early := deleted_before valid w hw a ha
    by_cases known : b ∈ s.possible a
    · cases b with
      | none => exact before a ha none known trivial
      | some x =>
        by_cases same : isReset (s.withEntry r reset) x = isReset s x
        · have appOld : Applies s w a (some x) := by
            intro isR
            rcases app (same ▸ isR) with ⟨t, mem, le⟩ | covers
            · rcases resetPairs x t mem with old | ⟨_, now⟩
              · exact .inl ⟨t, old, le⟩
              · omega
            · exact .inr covers
          have good := before a ha (some x) known appOld
          exact ⟨good.1, fun use => good.2 (usable_of use rfl rfl same)⟩
        · -- A snapshot first named by a reset now, already possible as a
          -- listed raise snapshot: marked, so never usable as a reset.
          have isR : x = r := by
            by_cases ne : x = r
            · exact ne
            · exact absurd (resetOther x ne) same
          subst isR
          have notEntry : x ∉ s.entries := by
            intro entry
            apply same
            cases reset with
            | true => rw [resetNamed rfl, kind entry]
            | false => exact resetSame rfl x
          have markedX : x ∈ s.marked := (valid.possibleSource a x known).resolve_left notEntry
          have landedX := (valid.markedLanded x markedX).1
          have nowReset : reset = true := by
            cases reset with
            | true => rfl
            | false => exact absurd (resetSame rfl x) same
          refine ⟨fun y hy notLanded => ?_, fun use => ?_⟩
          · cases hy; exact absurd landedX notLanded
          · rw [marked_reset_unusable _ x (resetNamed nowReset) markedX] at use
            exact absurd use Bool.false_ne_true
    · rcases shape a b hb with old | ⟨eq, audience, notEx⟩
      · exact absurd old known
      subst eq
      -- Not yet possible: no entry named `r` before, so no snapshot read
      -- one, and a landed `r` does not mark a raise.
      have unnamed : r ∉ s.entries := fun named =>
        known (audience ▸ valid.entriesPossible r named notEx)
      have unmarked : r ∉ s.marked := fun marked =>
        known (audience ▸ valid.markedPossible r marked notEx)
      have only : ∀ x ∈ s.landed, admits s.reads (some r) x = true → x = r := by
        intro x hx admitted
        simp only [admits, Bool.or_eq_true, decide_eq_true_eq] at admitted
        rcases admitted with same | readR
        · exact path x hx same
        · exact absurd (valid.reads _ readR) unnamed
      have none : selected order s.landed s.reads a (some r) = none ∨ r ∈ s.landed := by
        by_cases landedR : r ∈ s.landed
        · exact .inr landedR
        · left
          unfold selected
          rw [newest_none, List.filter_eq_nil_iff]
          intro x hx
          simp only [Bool.and_eq_true, not_and]
          intro _ admitted
          exact landedR ((only x hx admitted) ▸ hx)
      constructor
      · intro x hx notLanded
        cases hx
        rcases none with none | landedR
        · exact .inl none
        · exact absurd landedR notLanded
      · intro use
        have whole := use
        simp only [usable, Bool.and_eq_true, decide_eq_true_eq] at use
        obtain ⟨⟨landedR, countsR⟩, _⟩ := use
        by_cases isR : isReset (s.withEntry r reset) r = true
        · -- A reset's snapshot covers every deleted write that counts under it.
          have covers : r.covers w a := by
            rcases app isR with ⟨t, mem, le⟩ | covers
            · rcases resetPairs r t mem with old | ⟨_, now⟩
              · exact absurd (valid.resetsNamed _ old) unnamed
              · omega
            · exact covers
          have landedOld : r ∈ s.landed := landedR
          show CoversIn (selected order s.landed s.reads a (some r)) w a
          cases sel : selected order s.landed s.reads a (some r) with
          | none =>
            exfalso
            unfold selected at sel
            rw [newest_none] at sel
            have mem : r ∈ s.landed.filter fun x => x.audience == a && x.counts &&
                admits s.reads (some r) x := by
              simp [List.mem_filter, admits, samePath_self, countsR, landedOld, audience]
            rw [sel] at mem
            simp at mem
          | some x =>
            have member := newest_member order _ x sel
            simp only [List.mem_filter, Bool.and_eq_true] at member
            obtain ⟨landedX, _, admitted⟩ := member
            have := only x landedX admitted
            subst this
            exact covers
        · simp only [Bool.not_eq_true] at isR
          rw [unmarked_raise_unusable _ r isR unmarked] at whole
          exact absurd whole Bool.false_ne_true
  · intro p hp
    rcases resetPairs p.1 p.2 hp with old | ⟨named, _⟩
    · exact List.mem_cons_of_mem _ (valid.resetsNamed p old)
    · rw [named]; exact List.mem_cons_self
  · intro a x hx
    rcases shape a _ hx with old | ⟨eq, _, _⟩
    · exact (valid.possibleSource a x old).imp (List.mem_cons_of_mem _) id
    · cases eq; exact .inl List.mem_cons_self

theorem step_valid {published : Write → Prop} {order : SnapshotOrder} {s t : State}
    (step : Step published order s t) (valid : Valid published order s) :
    Valid published order t := by
  cases step with
  | tick time later =>
      refine ⟨valid.sound, valid.reads, valid.keptPossible, valid.keptNamed, valid.markedLanded,
        valid.markedPossible, valid.entriesPossible, valid.audience, valid.paths, valid.expiredOut,
        ?_, ?_, valid.resetsNamed, valid.possibleSource⟩
      · intro x hx
        obtain ⟨unnamed, late⟩ := valid.expiredUnnamed x hx
        exact ⟨unnamed, by dsimp only; omega⟩
      · intro w hw
        obtain ⟨pub, mature, held⟩ := valid.deleted w hw
        refine ⟨pub, ?_, held⟩
        intro a ha
        obtain ⟨snap, member, counted, covers, age⟩ := mature a ha
        exact ⟨snap, member, counted, covers, by dsimp only; omega⟩
  | land snap read raise now good fresh named path counting =>
      exact land_valid valid snap read raise now good fresh named path counting
  | publish r reset onTime path kind =>
      exact publish_valid valid r reset onTime path kind
  | replay a b possible named =>
      refine ⟨valid.sound, valid.reads, ?_, ?_, valid.markedLanded, valid.markedPossible,
        valid.entriesPossible, valid.audience, valid.paths, valid.expiredOut,
        valid.expiredUnnamed, valid.deleted, valid.resetsNamed, valid.possibleSource⟩
      · intro x
        by_cases same : x = a
        · subst same; simpa using possible
        · simpa [same] using valid.keptPossible x
      · intro x r hr
        by_cases same : x = a
        · subst same; simp at hr; exact named r hr
        · simp [same] at hr; exact valid.keptNamed x r hr
  | finalize a remaining narrower keeps =>
      have shrink : ∀ x b, b ∈ (fun y => if y = a then remaining else s.possible y) x →
          b ∈ s.possible x := by
        intro x b hb
        by_cases same : x = a
        · subst same; simp only [ite_true] at hb; exact narrower b hb
        · simpa [same] using hb
      have stays : ∀ r, some r ∈ s.possible r.audience →
          r ∈ removed (s.possible a) remaining ∨
            some r ∈ (fun y => if y = a then remaining else s.possible y) r.audience := by
        intro r hr
        by_cases same : r.audience = a
        · by_cases kept : some r ∈ remaining
          · right; simp [same, kept]
          · left
            simp only [removed, List.mem_filter, List.mem_filterMap, id, Bool.not_eq_true',
              decide_eq_false_iff_not]
            exact ⟨⟨some r, same ▸ hr, rfl⟩, kept⟩
        · right; simp [same, hr]
      have notExcluded : ∀ r, excluded { s with
          possible := fun x => if x = a then remaining else s.possible x
          ruledOut := removed (s.possible a) remaining ++ s.ruledOut } r = false →
          excluded s r = false ∧ r ∉ removed (s.possible a) remaining := by
        intro r h
        simp only [excluded, Bool.or_eq_false_iff, decide_eq_false_iff_not, List.mem_append,
          not_or] at h ⊢
        exact ⟨⟨h.1.2, h.2⟩, h.1.1⟩
      refine ⟨valid.sound, valid.reads, ?_, valid.keptNamed, valid.markedLanded, ?_, ?_,
        fun x r hr => valid.audience x r (shrink x _ hr), valid.paths, ?_, valid.expiredUnnamed, ?_,
        valid.resetsNamed, fun x r hr => valid.possibleSource x r (shrink x _ hr)⟩
      · intro x
        by_cases same : x = a
        · subst same; simpa using keeps
        · simpa [same] using valid.keptPossible x
      · intro m hm notEx
        obtain ⟨old, notRemoved⟩ := notExcluded m notEx
        rcases stays m (valid.markedPossible m hm old) with gone | here
        · exact absurd gone notRemoved
        · exact here
      · intro r hr notEx
        obtain ⟨old, notRemoved⟩ := notExcluded r notEx
        rcases stays r (valid.entriesPossible r hr old) with gone | here
        · exact absurd gone notRemoved
        · exact here
      · intro x hx member
        exact valid.expiredOut x hx (shrink _ _ member)
      · intro w hw
        obtain ⟨pub, mature, before⟩ := valid.deleted w hw
        exact ⟨pub, mature, fun x hx b hb => before x hx b (shrink x b hb)⟩
  | expire x late unnamed =>
      have shrink : ∀ y b, b ∈ (fun a => if a = x.audience then (s.possible a).filter (· != some x)
          else s.possible a) y → b ∈ s.possible y ∧ b ≠ some x ∨ (b ∈ s.possible y ∧ y ≠ x.audience) := by
        intro y b hb
        by_cases same : y = x.audience
        · subst same
          simp only [ite_true, List.mem_filter, bne_iff_ne, ne_eq] at hb
          exact .inl hb
        · simp only [same, ite_false] at hb
          exact .inr ⟨hb, same⟩
      have inOld : ∀ y b, b ∈ (fun a => if a = x.audience then (s.possible a).filter (· != some x)
          else s.possible a) y → b ∈ s.possible y := by
        intro y b hb
        rcases shrink y b hb with ⟨h, _⟩ | ⟨h, _⟩ <;> exact h
      have keep : ∀ y b, b ∈ s.possible y → b ≠ some x →
          b ∈ (fun a => if a = x.audience then (s.possible a).filter (· != some x)
            else s.possible a) y := by
        intro y b hb ne
        by_cases same : y = x.audience
        · subst same; simp [hb, ne]
        · simp [same, hb]
      have notExcluded : ∀ r, excluded { s with
          possible := fun a => if a = x.audience then (s.possible a).filter (· != some x)
            else s.possible a
          expired := x :: s.expired } r = false → excluded s r = false ∧ r ≠ x := by
        intro r h
        simp only [excluded, Bool.or_eq_false_iff, decide_eq_false_iff_not, List.mem_cons,
          not_or] at h ⊢
        exact ⟨⟨h.1, h.2.2⟩, h.2.1⟩
      refine ⟨valid.sound, valid.reads, ?_, valid.keptNamed, valid.markedLanded, ?_, ?_,
        fun y r hr => valid.audience y r (inOld y _ hr), valid.paths, ?_, ?_, ?_,
        valid.resetsNamed, fun y r hr => valid.possibleSource y r (inOld y _ hr)⟩
      · intro y
        apply keep y _ (valid.keptPossible y)
        intro isX
        exact unnamed (valid.keptNamed y x isX)
      · intro m hm notEx
        obtain ⟨old, ne⟩ := notExcluded m notEx
        exact keep _ _ (valid.markedPossible m hm old) (by simpa using ne)
      · intro r hr notEx
        obtain ⟨old, ne⟩ := notExcluded r notEx
        exact keep _ _ (valid.entriesPossible r hr old) (by simpa using ne)
      · intro y hy member
        rcases List.mem_cons.mp hy with rfl | old
        · rcases shrink _ _ member with ⟨_, ne⟩ | ⟨_, ne⟩
          · exact ne rfl
          · exact ne rfl
        · exact valid.expiredOut y old (inOld _ _ member)
      · intro y hy
        rcases List.mem_cons.mp hy with rfl | old
        · exact ⟨fun named => absurd named unnamed, late⟩
        · exact valid.expiredUnnamed y old
      · intro w hw
        obtain ⟨pub, mature, before⟩ := valid.deleted w hw
        exact ⟨pub, mature, fun y hy b hb => before y hy b (inOld y b hb)⟩
  | delete w publishedWrite mature held =>
      refine ⟨valid.sound, valid.reads, valid.keptPossible, valid.keptNamed, valid.markedLanded,
        valid.markedPossible, valid.entriesPossible, valid.audience, valid.paths, valid.expiredOut,
        valid.expiredUnnamed, ?_, valid.resetsNamed, valid.possibleSource⟩
      intro other member
      rcases List.mem_cons.mp member with rfl | old
      · exact ⟨publishedWrite, mature, fun a ha b hb _ =>
          ⟨fun _ _ _ => .inr (held a ha b hb), fun _ => held a ha b hb⟩⟩
      · exact valid.deleted other old

theorem initial_valid (published : Write → Prop) (order : SnapshotOrder) :
    Valid published order initial := by
  constructor <;> simp [initial, excluded]

theorem run_valid {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (run : Run published order s) : Valid published order s := by
  induction run with
  | initial => exact initial_valid published order
  | step _ step ih => exact step_valid step ih

/-- Every deleted write stays covered by its audiences' current selection
under whatever usable boundary replay keeps, for all three orders, wherever
it counts under that boundary: always under a raise or none, and under a
reset when its snapshot covers the write or the write was stored after its
entry, as every write that read the entry was. -/
theorem deleted_stays_covered {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (run : Run published order s) (w : Write) (deleted : w ∈ s.deleted) (a : Nat)
    (relevant : a ∈ w.holders) (counts : Applies s w a (s.kept a))
    (use : usable s (s.kept a) = true) :
    CoversIn (selected order s.landed s.reads a (s.kept a)) w a := by
  have valid := run_valid run
  exact ((valid.deleted w deleted).2.2 a relevant (s.kept a) (valid.keptPossible a) counts).2 use

/-- Under no boundary or a raise, every write counts. -/
theorem applies_unless_reset (s : State) (w : Write) (a : Nat) (b : Option Snapshot)
    (raise : ∀ r, b = some r → isReset s r = false) : Applies s w a b := by
  cases b with
  | none => trivial
  | some r => intro isR; simp [raise r rfl] at isR

/-- §15's reload under the kept boundary: it waits while that boundary's
named snapshot is not listed, does not count or does not mark its raise
(Rust `snapshot_boundaries`), and otherwise follows FixedReload. -/
def reload (order : SnapshotOrder) (s : State) (a : Nat) (w : Write) : Missing :=
  if usable s (s.kept a) then
    missing s.landed (fun x => selected order s.landed s.reads x (s.kept x)) a w
  else .uncertified

/-- So a reload that finds a deleted write missing loads a snapshot holding
it in every part audience where it counts, or waits; it never skips it as
having no part there. Under a kept reset it skips only a write that the
reset ignores (§19.3): one outside its snapshot that never read its entry. -/
theorem reload_never_skips {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (run : Run published order s) (w : Write) (deleted : w ∈ s.deleted) (a : Nat)
    (relevant : a ∈ w.holders) (counts : Applies s w a (s.kept a)) :
    reload order s a w ≠ .skip := by
  unfold reload
  split
  · rename_i use
    have covered := deleted_stays_covered run w deleted a relevant counts use
    unfold missing
    split
    · cases h : selected order s.landed s.reads a (s.kept a) with
      | none => simp [h, CoversIn] at covered
      | some snap =>
          have bound : w.number ≤ snap.position w.writer := by
            rw [h] at covered
            exact covered.2
          simp [h, bound]
    · simp
  · simp

/-- Without a pending raise the hold is §15's existing current-coverage gate. -/
theorem held_without_raises (order : SnapshotOrder) (s : State) (w : Write)
    (none_pending : ∀ a, s.possible a = [none]) :
    Held order s w ↔ Covered (catalogCurrent order s.landed) w := by
  have same : ∀ a, selected order s.landed s.reads a none = catalogCurrent order s.landed a := by
    intro a; simp [selected, catalogCurrent, admits]
  constructor
  · intro held a ha
    have := held a ha none (by simp [none_pending a])
    rw [same a] at this
    cases h : catalogCurrent order s.landed a <;> simp_all [CoversIn]
  · intro covered a ha b hb
    simp only [none_pending a, List.mem_singleton] at hb
    subst hb
    rw [same a]
    have := covered a ha
    cases h : catalogCurrent order s.landed a <;> simp_all [CoversIn]

/-! ### The hold on a listed raise snapshot ends -/

/-- Once storage time is past a day after a listed raise snapshot's `T_c`
and no entry names it, expiry is a step of every history. -/
theorem listed_raise_hold_ends (published : Write → Prop) (order : SnapshotOrder) (s : State)
    (x : Snapshot) (late : x.caughtAt + day < s.time) (unnamed : x ∉ s.entries) :
    ∃ t, Step published order s t ∧ some x ∉ t.possible x.audience ∧
      (∀ a b, b ∈ t.possible a ↔ b ∈ s.possible a ∧ (a = x.audience → b ≠ some x)) := by
  refine ⟨_, .expire s x late unnamed, by simp, ?_⟩
  intro a b
  by_cases same : a = x.audience
  · subst same; simp
  · simp [same]

/-- After expiry the snapshot never becomes a possible boundary again, no
raise entry names it (none stored a day after its `T_c` counts), and replay
never keeps it, even under a reset entry naming it. -/
theorem expired_stays_out {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (run : Run published order s) (x : Snapshot) (gone : x ∈ s.expired) :
    some x ∉ s.possible x.audience ∧ (x ∈ s.entries → isReset s x = true) ∧
      ∀ a, s.kept a ≠ some x := by
  have valid := run_valid run
  obtain ⟨unnamed, _⟩ := valid.expiredUnnamed x gone
  refine ⟨valid.expiredOut x gone, unnamed, fun a kept => ?_⟩
  have possible := valid.keptPossible a
  rw [kept] at possible
  have audience := valid.audience a x possible
  subst audience
  exact valid.expiredOut x gone possible

/-- A migration's capture can be sent when the completed catch-up that
sampled its `T_c` applied nothing past it: every counting snapshot of its
audience lists only positions that catch-up read (`snapshot_covers_published`),
so the capture meets §15's preparation rule. -/
theorem unchanged_capture_prepares (order : SnapshotOrder) (landed : List Snapshot)
    (capture : Snapshot) (caught : Nat → Nat)
    (unchanged : ∀ writer, caught writer ≤ capture.position writer)
    (read : ∀ s ∈ landed, s.audience = capture.audience → s.counts = true →
      ∀ writer, s.position writer ≤ caught writer) :
    CanSnapshot (catalogCurrent order landed) capture := by
  intro old selected
  obtain ⟨member, audience, counted⟩ := current_member order landed capture.audience old selected
  exact ⟨audience.symm, fun writer => Nat.le_trans (read old member audience counted writer)
    (unchanged writer)⟩

end RaiseHistory

end CovenIO.RaiseCoverage
