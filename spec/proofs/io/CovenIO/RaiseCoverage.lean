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

end CovenIO.RaiseCoverage
