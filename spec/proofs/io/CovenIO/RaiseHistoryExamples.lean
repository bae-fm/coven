import CovenIO.RaiseHistory

namespace CovenIO.RaiseCoverage

/-! # Checked histories for the recapture rule and the retention hold

The histories that failed under the old rules, replayed under
[`RaiseHistory`](RaiseHistory.lean)'s rules, and the dropped-reset history
that shows why the hold checks every reset replay could still keep (T225). -/
namespace RaiseHistory

open SnapshotHistory

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
  ⟨deletedAt, lateListing, [earlyRaise], [], [], [], fun _ => none,
    fun a => if a = 0 then [some earlyRaise, none] else [none], [], [], []⟩

theorem kept_late_held :
    selected .writes lateState.landed lateState.reads 0 (some earlyRaise) = some earlyRaise ∧
      Covered (catalogCurrent .writes lateListing) benLate ∧ ¬ Held .writes lateState benLate := by
  refine ⟨by decide, by decide, ?_⟩
  intro held
  have := held 0 (by decide) (some earlyRaise) (by decide)
  revert this
  decide

/-- T216: Ana's store raise snapshot from `kept_late_raise_loses_write` is
listed, but her device is removed before it publishes the raise entry. A day
after its `T_c` (50) no entry can name it; at Ben's deletion time it has
expired, and the hold is §15's current coverage again, which his snapshot
gives. -/
def releasedState : State :=
  { lateState with
    possible := fun a => if a = 0 then [none] else [none]
    expired := [earlyRaise] }

theorem abandoned_raise_released :
    earlyRaise.caughtAt + day < deletedAt ∧ earlyRaise ∉ lateState.entries ∧
      ¬ Held .writes lateState benLate ∧ Held .writes releasedState benLate := by
  refine ⟨by decide, by decide, ?_, by decide⟩
  intro held
  have := held 0 (by decide) (some earlyRaise) (by decide)
  revert this
  decide

theorem abandoned_raise_expires :
    ∃ t, Step (· ∈ [anaMarker, benLate]) .writes lateState t ∧ Held .writes t benLate := by
  exact ⟨_, .expire lateState earlyRaise (by decide) (by decide), by decide⟩

/-! ### A reset dropped now but not final (T225)

Writers: Ana 0, Ben 1, Carl 2; the store is audience 0. Carl's earlier reset
`F` is final and kept. Ana prepares her reset snapshot at 90, before `F`'s
entry is stored (94), so her snapshot did not read it; her own writes 41 to 50,
stored by 100, are in it. Carl's reset `C` (100) and Ana's `R` (102) are
concurrent; `C` has the smaller timestamp and is kept, `R` is dropped, and
neither is final. Ben's catch-up sampled storage at 100; its listing already
saw both resets, and he wrote write 5 at 104 after reading `R`, then captured
it in his snapshot, which covers the newest snapshot `C` admitted when he
prepared it (Carl's) but not Ana's, which `C` leaves out. -/

def finalSnapshot : Snapshot := ⟨0, 2, 1, 93, 92, [(0, 40), (2, 1)]⟩
def anaReset : Snapshot := ⟨0, 0, 1, 101, 90, [(0, 50)]⟩
def carlReset : Snapshot := ⟨0, 2, 2, 99, 95, [(0, 40), (2, 2)]⟩
def benSnapshot : Snapshot := ⟨0, 1, 1, 105, 100, [(0, 40), (1, 5), (2, 2)]⟩
def anaForty : Write := ⟨0, 40, 80, [0]⟩
def anaFifty : Write := ⟨0, 50, 100, [0]⟩
def benFive : Write := ⟨1, 5, 104, [0]⟩
def resetWrites : List Write := [anaForty, anaFifty, benFive]

/-- A day after Ben's snapshot landed, with Ben's write 5 already deleted by
a hold that checks only the kept reset `C` and the final one `F`. -/
def droppedResetState : State :=
  { time := 105 + day + 1
    landed := [benSnapshot, anaReset, carlReset, finalSnapshot]
    marked := []
    deleted := [benFive]
    reads := [(carlReset, finalSnapshot), (benSnapshot, finalSnapshot),
      (benSnapshot, carlReset), (benSnapshot, anaReset)]
    entries := [anaReset, carlReset, finalSnapshot]
    kept := fun a => if a = 0 then some carlReset else none
    possible := fun a => if a = 0 then [some carlReset, some finalSnapshot, some anaReset]
      else [none]
    ruledOut := []
    expired := []
    resets := [(anaReset, 102), (carlReset, 100), (finalSnapshot, 94)] }

/-- Every snapshot is sound for the published writes, and Ben's write has a
mature witness. Under `C` and under `F` the newest snapshot each admits is
Ben's, which covers his write 5; under `R` it is Ana's reset snapshot, which
has more writes but not his. So the hold refuses the deletion: `R` can still
be kept. -/
theorem dropped_reset_held :
    (∀ snap ∈ droppedResetState.landed, Sound (· ∈ resetWrites) snap) ∧
      MatureCoverage droppedResetState.landed droppedResetState.time benFive ∧
      selected .writes droppedResetState.landed droppedResetState.reads 0 (some carlReset) =
        some benSnapshot ∧
      selected .writes droppedResetState.landed droppedResetState.reads 0 (some finalSnapshot) =
        some benSnapshot ∧
      CoversIn (some benSnapshot) benFive 0 ∧
      selected .writes droppedResetState.landed droppedResetState.reads 0 (some anaReset) =
        some anaReset ∧
      ¬ Held .writes droppedResetState benFive := by
  refine ⟨?_, by decide, by decide, by decide, by decide, by decide, ?_⟩
  · intro snap landed
    simp only [droppedResetState, List.mem_cons, List.not_mem_nil, or_false] at landed
    refine ⟨?_, ?_⟩ <;> intro w published <;>
      simp only [resetWrites, List.mem_cons, List.not_mem_nil, or_false] at published <;>
      rcases landed with rfl | rfl | rfl | rfl <;> rcases published with rfl | rfl | rfl <;> decide
  · intro held
    have := held 0 (by decide) (some anaReset) (by decide)
    revert this
    decide

/-- Had the write been deleted under that narrower hold, Carl's reset could
still drop (say, his removal arrives, made concurrently) and replay keep
Ana's. Ben's write counts under her reset, since he read its entry, yet the
reload certifies the deletion and skips it: his write is lost. -/
theorem dropped_reset_skips :
    ∃ t, Step (· ∈ resetWrites) .writes droppedResetState t ∧
      t.kept 0 = some anaReset ∧ usable t (t.kept 0) = true ∧
      Applies t benFive 0 (t.kept 0) ∧ reload .writes t 0 benFive = .skip := by
  refine ⟨_, .replay droppedResetState 0 (some anaReset) (by decide) ?_, by decide, by decide,
    ?_, by decide⟩
  · intro r named
    cases named
    decide
  · intro _
    exact .inl ⟨102, by decide, by decide⟩

end RaiseHistory

end CovenIO.RaiseCoverage
