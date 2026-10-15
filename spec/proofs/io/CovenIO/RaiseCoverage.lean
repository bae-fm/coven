import CovenIO.Reload

/-! §15, §17.1: judging a raise snapshot only by what it covers (T200,
option A) loses writes. Checked counterexamples.

A breaking migration captures one raise snapshot per audience its device
reads, in the migration transaction (§17.1). Option A exempts that snapshot
from §15's preparation rule (cover the audience's current newest counting
snapshot), lets it count only for the writes it covers, and relies on the
logs keeping every write it does not cover, for peers to convert on receipt.

Once replay keeps the raise, the audience's usable snapshots are the named
snapshot and those whose prefix read the raise entry (Rust
`snapshot_boundaries::allows`); older-schema snapshots no longer count as its
current newest (`boundaryCurrent`). Retention, however, judged deletion
before the raise existed remotely, against an older snapshot that did cover
the write. Nothing in §15's deletion gates refers to a raise that has not
landed, so:

- `option_a_loses_write`: Ana migrates before Ben's old-schema write 1 lands
  and captures store and Gifts raise snapshots; Carol, a member of Work,
  which Ana cannot read, captures Work's. Ben's snapshot of each audience
  covers his write 1; every device posts past it; a day later Ben deletes it
  under every §15 gate. The raise snapshots then land and the raises are
  kept: in all three audiences the reload's current newest is the raise
  snapshot, which stops before Ben's write 1, so a fresh catalog certifies
  the deletion and the reload skips the write as having no part there. Its
  parts are lost in every audience, including the circle Ana cannot read.
- `capture_unsound`, `capture_stamp_never_counts`: the counterexample
  needs no exotic timing. A raise snapshot stamped with the catch-up time of
  its sending (Rust samples `T_c` when sealing) violates the soundness that
  `SnapshotHistory.deleted_stays_covered` rests on: it omits a write stored
  before that time. Stamped with its capture time instead, it lands more
  than a day later and never counts, so the raise can never become usable.
- `kept_late_raise_loses_write`: the same loss happens to a raise snapshot
  that satisfies every current rule (prepared after a completed catch-up,
  covering the then-current newest, sound and counting) when its raise entry
  is kept more than a day after the snapshot landed: Ben's later old-schema
  write is covered by his newer snapshot, deleted, and then excluded by the
  boundary. Here the raise snapshot was already listed when Ben deleted, so
  a retention hold on listed raise snapshots would have kept the write; in
  `option_a_loses_write` it was not listed yet. -/
namespace CovenIO.RaiseCoverage

/-- The snapshot each audience's kept raise or reset entry names, if any. -/
abbrev Boundary := Nat → Option Snapshot

def samePath (a b : Snapshot) : Bool :=
  a.audience == b.audience && a.writer == b.writer && a.number == b.number

/-- Rust `snapshot_boundaries::allows`: with a kept boundary, its named
snapshot and snapshots whose signed store-log view includes the boundary
entry are usable. -/
def admitted (boundary : Boundary) (readBoundary : Snapshot → Bool) (s : Snapshot) : Bool :=
  match boundary s.audience with
  | none => true
  | some named => samePath s named || readBoundary s

/-- §15's current newest counting snapshot among usable snapshots (Rust
`Catalog::take_current`, retention's `follows_boundary`). -/
def boundaryCurrent (order : SnapshotOrder) (landed : List Snapshot) (boundary : Boundary)
    (readBoundary : Snapshot → Bool) : Current :=
  fun a => newest order (landed.filter fun s =>
    s.audience == a && s.counts && admitted boundary readBoundary s)

/-- Without a kept boundary this is the IO model's `catalogCurrent`. -/
theorem no_boundary_current (order : SnapshotOrder) (landed : List Snapshot)
    (readBoundary : Snapshot → Bool) :
    boundaryCurrent order landed (fun _ => none) readBoundary = catalogCurrent order landed := by
  funext a
  simp [boundaryCurrent, catalogCurrent, admitted]

inductive Missing where
  | load (s : Snapshot)
  | skip
  | uncertified
  deriving DecidableEq, Repr

/-- §15 after a successful missing GET of write `w` in audience `a`: a fresh
catalog with counting coverage through the number in any audience certifies
the deletion; the reload loads the audience's current newest if it reaches
the number and otherwise skips it as having no part there (FixedReload
`scan`). -/
def missing (landed : List Snapshot) (current : Current) (a : Nat) (w : Write) : Missing :=
  if coveredMiss landed w.writer (w.number - 1) then
    match current a with
    | some s => if w.number ≤ s.position w.writer then .load s else .skip
    | none => .skip
  else .uncertified

/-- Any kept boundary whose usable snapshots stop before a certified deleted
write makes the reload skip that write's part. -/
theorem boundary_skips (order : SnapshotOrder) (landed : List Snapshot) (boundary : Boundary)
    (readBoundary : Snapshot → Bool) (a : Nat) (w : Write) (s : Snapshot)
    (certified : coveredMiss landed w.writer (w.number - 1) = true)
    (selected : boundaryCurrent order landed boundary readBoundary a = some s)
    (short : s.position w.writer < w.number) :
    missing landed (boundaryCurrent order landed boundary readBoundary) a w = .skip := by
  simp [missing, certified, selected, Nat.not_le.mpr short]

/-! ## Option A: raise snapshots captured at migration

Writers: Ana 0, Ben 1, Carol 2. Audiences: the store 0, Gifts 1 (Ana and
Ben), Work 2 (Ben and Carol; Ana cannot read it, so Carol, its first member
to update, raises it). -/

/-- Ben's old-schema write 1, with a part in every audience, and his write 2. -/
def benOne : Write := ⟨1, 1, 100, [0, 1, 2]⟩
def benTwo : Write := ⟨1, 2, 150, [0, 1, 2]⟩

/-- Ben's snapshot of each audience after a completed catch-up at 200. -/
def bensSnapshot (audience number : Nat) : Snapshot := ⟨audience, 1, number, 200, 200, [(1, 2)]⟩
def bens : List Snapshot := [bensSnapshot 0 1, bensSnapshot 1 2, bensSnapshot 2 3]

/-- Every device has posted past Ben's writes. -/
def postedAll : Nat → Nat → Nat := fun _ _ => 2
def devices : List Nat := [0, 1, 2]
def deletedAt : Nat := 200 + day + 1

/-- Captured at time 0 through the migrating device's marker 1, before Ben's
writes; sent after a completed catch-up at `sentAt`. -/
def sentAt : Nat := 2 * day
def raiseSnapshot (audience writer number caughtAt : Nat) : Snapshot :=
  ⟨audience, writer, number, sentAt, caughtAt, [(writer, 1)]⟩
def anaStore : Snapshot := raiseSnapshot 0 0 1 sentAt
def anaGifts : Snapshot := raiseSnapshot 1 0 2 sentAt
def carolWork : Snapshot := raiseSnapshot 2 2 1 sentAt
def raises : List Snapshot := [anaStore, anaGifts, carolWork]

def kept : Boundary := fun a =>
  if a = 0 then some anaStore else if a = 1 then some anaGifts else if a = 2 then some carolWork
  else none

def afterRaise : List Snapshot := raises ++ bens
def noLaterSnapshot : Snapshot → Bool := fun _ => false

/-- The loss. Ben deletes write 1 under every §15 gate (current coverage of
every part, a counting witness landed more than a day earlier, every device
posted past it, final). The raise snapshots fail the preparation rule
option A exempts them from, count, and become their audiences' boundaries.
Each audience's reload then skips write 1 as having no part there, though
Ben's snapshots still list its coverage. -/
theorem option_a_loses_write :
    Eligible .writes bens devices postedAll deletedAt True benOne ∧
      (∀ s ∈ raises, s.counts = true) ∧
      SnapshotPublication.dominates anaStore (bensSnapshot 0 1) = false ∧
      SnapshotPublication.dominates anaGifts (bensSnapshot 1 2) = false ∧
      SnapshotPublication.dominates carolWork (bensSnapshot 2 3) = false ∧
      missing afterRaise (boundaryCurrent .writes afterRaise kept noLaterSnapshot) 0 benOne = .skip ∧
      missing afterRaise (boundaryCurrent .writes afterRaise kept noLaterSnapshot) 1 benOne = .skip ∧
      missing afterRaise (boundaryCurrent .writes afterRaise kept noLaterSnapshot) 2 benOne = .skip ∧
      missing afterRaise (catalogCurrent .writes afterRaise) 0 benOne = .load (bensSnapshot 0 1) ∧
      missing afterRaise (catalogCurrent .writes afterRaise) 2 benOne = .load (bensSnapshot 2 3) := by
  refine ⟨by decide, by decide, by decide, by decide, by decide, by decide, by decide, by decide,
    by decide, by decide⟩

/-- When Ben deleted write 1, no raise snapshot was listed: a retention hold
on listed raise snapshots could not have seen them. -/
theorem raises_absent_at_deletion :
    (∀ s ∈ bens, s.storedAt < deletedAt) ∧ ∀ s ∈ raises, deletedAt < s.storedAt := by
  decide

/-- A raise snapshot stamped with its sending catch-up time omits a write
stored before that time, so it lies outside `SnapshotHistory.Sound`, the
premise of `deleted_stays_covered`. -/
theorem capture_unsound : ¬ SnapshotHistory.Sound (· ∈ [benOne, benTwo]) anaStore := by
  intro sound
  have covered := sound.caught benOne (by simp) (by decide) (by decide)
  exact absurd covered (by decide)

/-- Stamped with its capture time it is sound, but landing two days later it
never counts: readers ignore it, and a raise naming it never becomes usable. -/
theorem capture_stamp_never_counts :
    (raiseSnapshot 0 0 1 0).counts = false ∧ (raiseSnapshot 2 2 1 0).counts = false := by decide

/-! ## A raise kept late, under the current rules -/

/-- Ana's store raise snapshot, prepared after a completed catch-up at 50
with nothing landed (so it covers the current newest), lands at 60. -/
def earlyRaise : Snapshot := ⟨0, 0, 1, 60, 50, [(0, 1)]⟩
def anaMarker : Write := ⟨0, 1, 40, []⟩
/-- Ben's old-schema store write after it, and his snapshot covering it. -/
def benLate : Write := ⟨1, 1, 100, [0]⟩
def bensLater : Snapshot := ⟨0, 1, 1, 200, 200, [(0, 1), (1, 1)]⟩
def lateListing : List Snapshot := [bensLater, earlyRaise]
def lateKept : Boundary := fun a => if a = 0 then some earlyRaise else none

/-- The raise snapshot meets every current rule, Ben deletes his later
write under every §15 gate while it is listed, and Ana's raise entry, kept
after that, makes the store's reload skip the write. -/
theorem kept_late_raise_loses_write :
    catalogCurrent .writes [] 0 = none ∧
      SnapshotHistory.Sound (· ∈ [anaMarker, benLate]) earlyRaise ∧
      earlyRaise.counts = true ∧
      Eligible .writes lateListing devices postedAll deletedAt True benLate ∧
      earlyRaise ∈ lateListing ∧ ¬ earlyRaise.covers benLate 0 ∧
      missing lateListing (boundaryCurrent .writes lateListing lateKept noLaterSnapshot) 0 benLate =
        .skip := by
  refine ⟨by decide, ⟨?_, ?_⟩, by decide, by decide, by decide, by decide, by decide⟩
  · intro w published
    simp only [List.mem_cons, List.not_mem_nil, or_false] at published
    rcases published with rfl | rfl <;> decide
  · intro w published
    simp only [List.mem_cons, List.not_mem_nil, or_false] at published
    rcases published with rfl | rfl <;> decide


/-! ## Recapture and the hold: no reload skips a deleted write

The rules decided for T200. A raise snapshot is prepared like any other:
immediately after a completed catch-up whose storage sample is its `T_c`, so
it is `SnapshotHistory.Sound`. A migration's capture is sent only when that
catch-up applied nothing past it (`unchanged_capture_prepares`); otherwise
the device captures again, and its later local writes join the captured
prefix. Its signed prefix marks it as a raise snapshot, so retention sees it
while no entry names it yet.

Deleting a write's parts then needs coverage under every boundary replay
could still select for each part audience (`Held`): the current one, the
one final entries fix, and each listed counting raise snapshot that a raise
could still name. Under each, the newest usable counting snapshot must
cover the write. Replay keeps only possible boundaries, finality rules some
out, and a landing raise snapshot becomes one. -/
namespace RaiseHistory

open SnapshotHistory

/-- Usable under a possible boundary: everything without one; otherwise the
named snapshot and every snapshot whose signed store-log view read an entry
naming it (`reads` pairs a reader with the named snapshot). -/
def admits (reads : List (Snapshot × Snapshot)) : Option Snapshot → Snapshot → Bool
  | none, _ => true
  | some named, s => samePath s named || decide ((s, named) ∈ reads)

/-- §15's newest counting snapshot of audience `a` under boundary `b`. -/
def selected (order : SnapshotOrder) (landed : List Snapshot)
    (reads : List (Snapshot × Snapshot)) (a : Nat) (b : Option Snapshot) : Option Snapshot :=
  newest order (landed.filter fun s => s.audience == a && s.counts && admits reads b s)

def CoversIn (selection : Option Snapshot) (w : Write) (a : Nat) : Prop :=
  match selection with
  | none => False
  | some s => s.covers w a

instance (selection : Option Snapshot) (w : Write) (a : Nat) : Decidable (CoversIn selection w a) := by
  cases selection <;> unfold CoversIn <;> infer_instance

structure State where
  time : Nat
  landed : List Snapshot
  deleted : List Write
  reads : List (Snapshot × Snapshot)
  /-- Raise snapshots named by a published raise entry. -/
  entries : List Snapshot
  /-- The boundary replay currently keeps for each audience. -/
  kept : Nat → Option Snapshot
  /-- The boundaries replay could still select for each audience. -/
  possible : Nat → List (Option Snapshot)

def initial : State := ⟨0, [], [], [], [], fun _ => none, fun _ => [none]⟩

/-- The hold: coverage under every possible boundary of every part audience. -/
def Held (order : SnapshotOrder) (s : State) (w : Write) : Prop :=
  ∀ a ∈ w.audiences, ∀ b ∈ s.possible a, CoversIn (selected order s.landed s.reads a b) w a

instance (order : SnapshotOrder) (s : State) (w : Write) : Decidable (Held order s w) := by
  unfold Held; infer_instance

def addPossible (possible : Nat → List (Option Snapshot)) (snap : Snapshot) :
    Nat → List (Option Snapshot) :=
  fun a => if a = snap.audience then some snap :: possible a else possible a

/-- Catch-up, reads and finality decisions happen outside this boundary.
Replay changes `kept` only among possible boundaries, and finality drops
only boundaries replay can no longer select; the Rust replay obligations
behind those two premises are listed in io.md. -/
inductive Step (published : Write → Prop) (order : SnapshotOrder) : State → State → Prop
  | tick (s : State) (time : Nat) (later : s.time ≤ time) :
      Step published order s { s with time := time }
  /-- A sound snapshot lands with the published raise entries its signed
  view read. A counting raise snapshot becomes a possible boundary. -/
  | land (s : State) (snap : Snapshot) (read : List Snapshot) (raise : Bool)
      (now : snap.storedAt = s.time) (sound : Sound published snap)
      (fresh : ∀ x ∈ s.landed, samePath x snap = false)
      (named : ∀ n ∈ read, n ∈ s.entries) (counting : raise = true → snap.counts = true) :
      Step published order s { s with
        landed := snap :: s.landed
        reads := read.map (snap, ·) ++ s.reads
        possible := if raise then addPossible s.possible snap else s.possible }
  /-- A raise entry is published once its snapshot has landed and counts. -/
  | publish (s : State) (r : Snapshot) (landed : r ∈ s.landed) (counts : r.counts = true) :
      Step published order s { s with entries := r :: s.entries }
  | replay (s : State) (a : Nat) (b : Option Snapshot) (possible : b ∈ s.possible a) :
      Step published order s { s with kept := fun x => if x = a then b else s.kept x }
  | finalize (s : State) (a : Nat) (remaining : List (Option Snapshot))
      (narrower : ∀ b ∈ remaining, b ∈ s.possible a) (keeps : s.kept a ∈ remaining) :
      Step published order s { s with possible := fun x => if x = a then remaining else s.possible x }
  /-- §15's deletion gates with the hold; finality and reader positions are
  further gates that this invariant does not need. -/
  | delete (s : State) (w : Write) (publishedWrite : published w)
      (mature : MatureCoverage s.landed s.time w) (held : Held order s w) :
      Step published order s { s with deleted := w :: s.deleted }

inductive Run (published : Write → Prop) (order : SnapshotOrder) : State → Prop
  | initial : Run published order initial
  | step {s t : State} : Run published order s → Step published order s t → Run published order t

def Valid (published : Write → Prop) (order : SnapshotOrder) (s : State) : Prop :=
  (∀ snap ∈ s.landed, Sound published snap) ∧
  (∀ e ∈ s.entries, e ∈ s.landed) ∧
  (∀ p ∈ s.reads, p.2 ∈ s.entries) ∧
  (∀ a, s.kept a ∈ s.possible a) ∧
  ∀ w ∈ s.deleted, published w ∧ MatureCoverage s.landed s.time w ∧ Held order s w

theorem samePath_self (s : Snapshot) : samePath s s = true := by simp [samePath]

/-- The newest of a list with one more element covering the write still covers it. -/
theorem newest_cons_covers (order : SnapshotOrder) (snap : Snapshot) (rest : List Snapshot)
    (w : Write) (a : Nat) (new : snap.covers w a)
    (old : rest = [] ∨ CoversIn (newest order rest) w a) :
    CoversIn (newest order (snap :: rest)) w a := by
  simp only [newest]
  cases h : newest order rest with
  | none => simpa [CoversIn] using new
  | some other =>
      have previous : other.covers w a := by
        rcases old with empty | covered
        · subst empty; simp [newest] at h
        · simpa [h, CoversIn] using covered
      by_cases pick : newer order snap other = true
      · simpa [pick, CoversIn] using new
      · simpa [pick, CoversIn] using previous

/-- A counting sound snapshot landing after a deletion covers the deleted
write: its catch-up began after the write's mature witness landed. -/
theorem late_landing_covers {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (valid : Valid published order s) (w : Write) (deleted : w ∈ s.deleted)
    (snap : Snapshot) (now : snap.storedAt = s.time) (sound : Sound published snap)
    (counts : snap.counts = true) (relevant : snap.audience ∈ w.audiences) :
    snap.covers w snap.audience := by
  obtain ⟨landedSound, _, _, _, gone⟩ := valid
  obtain ⟨pub, mature, _⟩ := gone w deleted
  obtain ⟨witness, member, _, covers, age⟩ := mature snap.audience relevant
  have causal := (landedSound witness member).causal w pub (covers.1 ▸ relevant)
    (by simpa [covers.1] using covers)
  have admission : snap.storedAt ≤ snap.caughtAt + day := by simpa [Snapshot.counts] using counts
  exact sound.caught w pub relevant (by omega)

theorem admits_old (reads : List (Snapshot × Snapshot)) (read : List Snapshot)
    (snap x : Snapshot) (b : Option Snapshot) (other : samePath x snap = false) :
    admits (read.map (snap, ·) ++ reads) b x = admits reads b x := by
  cases b with
  | none => rfl
  | some named =>
      have different : x ≠ snap := by
        intro same; subst same; simp [samePath_self] at other
      simp only [admits, List.mem_append, List.mem_map]
      congr 1
      apply decide_eq_decide.mpr
      constructor
      · rintro (⟨n, _, eq⟩ | old)
        · exact absurd (congrArg Prod.fst eq).symm different
        · exact old
      · exact Or.inr

/-- Landing changes a possible boundary's selection only by the new snapshot. -/
theorem selected_after_land (landed : List Snapshot)
    (reads : List (Snapshot × Snapshot)) (read : List Snapshot) (snap : Snapshot)
    (a : Nat) (b : Option Snapshot) (fresh : ∀ x ∈ landed, samePath x snap = false) :
    (landed.filter fun s => s.audience == a && s.counts &&
        admits (read.map (snap, ·) ++ reads) b s) =
      landed.filter fun s => s.audience == a && s.counts && admits reads b s := by
  apply List.filter_congr
  intro x member
  rw [admits_old reads read snap x b (fresh x member)]

theorem step_valid {published : Write → Prop} {order : SnapshotOrder} {s t : State}
    (step : Step published order s t) (valid : Valid published order s) :
    Valid published order t := by
  have whole := valid
  obtain ⟨sound, entries, reads, keptPossible, deleted⟩ := valid
  cases step with
  | tick time later =>
      refine ⟨sound, entries, reads, keptPossible, ?_⟩
      intro w hw
      obtain ⟨pub, mature, held⟩ := deleted w hw
      refine ⟨pub, ?_, held⟩
      intro a ha
      obtain ⟨snap, member, counted, covers, age⟩ := mature a ha
      exact ⟨snap, member, counted, covers, by dsimp; omega⟩
  | land snap read raise now good fresh named counting =>
      refine ⟨?_, ?_, ?_, ?_, ?_⟩
      · intro other member
        rcases List.mem_cons.mp member with rfl | old
        · exact good
        · exact sound other old
      · intro e he; exact List.mem_cons_of_mem _ (entries e he)
      · intro p hp
        rcases List.mem_append.mp hp with fromRead | old
        · obtain ⟨n, hn, rfl⟩ := List.mem_map.mp fromRead
          exact named n hn
        · exact reads p old
      · intro a
        cases raise with
        | false => exact keptPossible a
        | true =>
            simp only [ite_true, addPossible]
            split
            · exact List.mem_cons_of_mem _ (keptPossible a)
            · exact keptPossible a
      · intro w hw
        obtain ⟨pub, mature, held⟩ := deleted w hw
        refine ⟨pub, maturity_survives_new_snapshots s.landed [snap] s.time w mature, ?_⟩
        intro a ha b hb
        have covers : snap.audience = a → snap.counts = true → snap.covers w a := by
          intro same counted
          subst same
          exact late_landing_covers whole w hw snap now good counted ha
        -- The new raise snapshot's own boundary selects only itself.
        have own : b = some snap → raise = true → snap.audience = a →
            CoversIn (selected order (snap :: s.landed) (read.map (snap, ·) ++ s.reads) a b) w a := by
          intro hb' isRaise same
          subst hb'
          have counted := counting isRaise
          have none_old : (s.landed.filter fun x => x.audience == a && x.counts &&
              admits (read.map (snap, ·) ++ s.reads) (some snap) x) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro x member
            rw [admits_old s.reads read snap x (some snap) (fresh x member)]
            simp only [admits, fresh x member, Bool.false_or, Bool.and_eq_true,
              decide_eq_true_eq, not_and]
            intro _ readsSnap
            have hn := entries snap (reads _ readsSnap)
            have := fresh snap hn
            simp [samePath_self] at this
          have self : (snap.audience == a && snap.counts &&
              admits (read.map (snap, ·) ++ s.reads) (some snap) snap) = true := by
            simp [admits, samePath_self, same, counted]
          unfold selected
          simp only [List.filter_cons, self, ite_true]
          rw [none_old]
          exact newest_cons_covers order snap [] w a (covers same counted) (Or.inl rfl)
        have previous : b ∈ s.possible a →
            CoversIn (selected order (snap :: s.landed) (read.map (snap, ·) ++ s.reads) a b) w a := by
          intro old
          have before := held a ha b old
          unfold selected
          rw [List.filter_cons, selected_after_land s.landed s.reads read snap a b fresh]
          split
          · rename_i admitted
            simp only [Bool.and_eq_true, beq_iff_eq] at admitted
            apply newest_cons_covers order snap _ w a (covers admitted.1.1 admitted.1.2)
            exact Or.inr before
          · exact before
        cases raise with
        | false => exact previous hb
        | true =>
            simp only [ite_true, addPossible] at hb
            split at hb
            · rename_i same
              rcases List.mem_cons.mp hb with rfl | old
              · exact own rfl rfl same.symm
              · exact previous old
            · exact previous hb
  | publish r landed counts =>
      refine ⟨sound, ?_, ?_, keptPossible, deleted⟩
      · intro e he
        rcases List.mem_cons.mp he with rfl | old
        · exact landed
        · exact entries e old
      · intro p hp; exact List.mem_cons_of_mem _ (reads p hp)
  | replay a b possible =>
      refine ⟨sound, entries, reads, ?_, deleted⟩
      intro x
      by_cases same : x = a
      · subst same; simpa using possible
      · simpa [same] using keptPossible x
  | finalize a remaining narrower keeps =>
      refine ⟨sound, entries, reads, ?_, ?_⟩
      · intro x
        by_cases same : x = a
        · subst same; simpa using keeps
        · simpa [same] using keptPossible x
      · intro w hw
        obtain ⟨pub, mature, held⟩ := deleted w hw
        refine ⟨pub, mature, ?_⟩
        intro x hx b hb
        by_cases same : x = a
        · subst same
          simp only [ite_true] at hb
          exact held x hx b (narrower b hb)
        · simp only [same, ite_false] at hb
          exact held x hx b hb
  | delete w publishedWrite mature held =>
      refine ⟨sound, entries, reads, keptPossible, ?_⟩
      intro other member
      rcases List.mem_cons.mp member with rfl | old
      · exact ⟨publishedWrite, mature, held⟩
      · exact deleted other old

theorem run_valid {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (run : Run published order s) : Valid published order s := by
  induction run with
  | initial =>
      refine ⟨by simp [initial], by simp [initial], by simp [initial], by simp [initial],
        by simp [initial]⟩
  | step _ step ih => exact step_valid step ih

/-- Every deleted write stays covered by its audiences' current selection
under whatever boundary replay keeps, for all three orders. -/
theorem deleted_stays_covered {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (run : Run published order s) (w : Write) (deleted : w ∈ s.deleted) (a : Nat)
    (relevant : a ∈ w.audiences) :
    CoversIn (selected order s.landed s.reads a (s.kept a)) w a := by
  obtain ⟨_, _, _, keptPossible, gone⟩ := run_valid run
  exact (gone w deleted).2.2 a relevant (s.kept a) (keptPossible a)

/-- So a reload that finds a deleted write missing loads a snapshot holding
it in every part audience; it never skips it as having no part there. -/
theorem reload_never_skips {published : Write → Prop} {order : SnapshotOrder} {s : State}
    (run : Run published order s) (w : Write) (deleted : w ∈ s.deleted) (a : Nat)
    (relevant : a ∈ w.audiences) :
    missing s.landed (fun x => selected order s.landed s.reads x (s.kept x)) a w ≠ .skip := by
  have covered := deleted_stays_covered run w deleted a relevant
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

/-! ### The two failing histories under these rules -/

/-- `option_a_loses_write`: the migration-time raise snapshots are unsound
(`capture_unsound`), so none can land. Ana's recapture after a completed
catch-up at `sentAt` covers Ben's writes, and with it as the kept boundary
every audience's reload loads write 1. -/
def recaptured (audience writer number : Nat) : Snapshot :=
  ⟨audience, writer, number, sentAt, sentAt, [(writer, 1), (1, 2)]⟩
def recaptures : List Snapshot := [recaptured 0 0 1, recaptured 1 0 2, recaptured 2 2 1]
def recapturedKept : Boundary := fun a =>
  if a = 0 then some (recaptured 0 0 1) else if a = 1 then some (recaptured 1 0 2)
  else if a = 2 then some (recaptured 2 2 1) else none

theorem recapture_keeps_write :
    SnapshotHistory.Sound (· ∈ [benOne, benTwo]) (recaptured 0 0 1) ∧
      missing (recaptures ++ bens)
        (boundaryCurrent .writes (recaptures ++ bens) recapturedKept noLaterSnapshot) 0 benOne =
          .load (recaptured 0 0 1) ∧
      missing (recaptures ++ bens)
        (boundaryCurrent .writes (recaptures ++ bens) recapturedKept noLaterSnapshot) 1 benOne =
          .load (recaptured 1 0 2) ∧
      missing (recaptures ++ bens)
        (boundaryCurrent .writes (recaptures ++ bens) recapturedKept noLaterSnapshot) 2 benOne =
          .load (recaptured 2 2 1) := by
  refine ⟨⟨?_, ?_⟩, by decide, by decide, by decide⟩
  · intro w published
    simp only [List.mem_cons, List.not_mem_nil, or_false] at published
    rcases published with rfl | rfl <;> decide
  · intro w published
    simp only [List.mem_cons, List.not_mem_nil, or_false] at published
    rcases published with rfl | rfl <;> decide

/-- `kept_late_raise_loses_write`: while Ana's raise snapshot is listed and
no raise entry names it, it is a possible boundary of the store, and under
it the store's selection is that snapshot, which omits Ben's write: the
hold refuses Ben's deletion. -/
def lateState : State :=
  ⟨deletedAt, lateListing, [], [], [], fun _ => none,
    fun a => if a = 0 then [some earlyRaise, none] else [none]⟩

theorem kept_late_held :
    selected .writes lateState.landed lateState.reads 0 (some earlyRaise) = some earlyRaise ∧
      Covered (catalogCurrent .writes lateListing) benLate ∧ ¬ Held .writes lateState benLate := by
  refine ⟨by decide, by decide, ?_⟩
  intro held
  have := held 0 (by decide) (some earlyRaise) (by decide)
  revert this
  decide

end RaiseHistory

end CovenIO.RaiseCoverage
