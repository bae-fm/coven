import CovenMerge.ForgetEdits

/-!
# A genuine write finds every source it names

Judging a write checks each source it names against the writes this device has
landed (§9.5). A source whose write hasn't landed yet *waits*; that is the one
verdict that depends on what else arrives. This file proves that wait is never
the verdict on a genuine write that is judged in its place in the slot order:

* `sources_present`: every change a genuine write names as a source is the
  change of a write that landed before it. The writes an author's state holds
  are the slots it read, `read_through`, which come before the write's own slot,
  and its own queued writes, which its device numbered below the write and so
  lands before it (format D9). A device judging the write has landed every
  write before it, since it applies slots in order.

So a source that has not landed when its write is judged was named by a write
that is not genuine: the slot is damaged, and no later slot supplies it.
-/

namespace CovenMerge.Sources

open CovenMerge
open CovenMerge.ForgetEdits
open CovenMerge.DataDependency (Provenance)

variable {W Row Col : Type} [DecidableEq W] (M : Writes W Row Col)

/-- A change current in the state after `ws` applied to `st` came from `st` or
from one of `ws`. -/
theorem current_run {st : St W Row Col} {ws : List W} (gb : GenBound st) {x : W}
    (current : Current (run M st ws) x) : Current st x ∨ x ∈ ws := by
  by_cases absent : x ∈ ws
  · exact .inr absent
  · exact .inl (Classical.byContradiction fun stopped =>
      stopped_stays_run M gb stopped ws absent current)

/-- Nothing is current in the empty database. -/
theorem init_not_current (x : W) : ¬ Current (St.init : St W Row Col) x := by
  rintro (⟨r, c, h⟩ | ⟨r, h⟩) <;> simp [St.init] at h

/-- **A genuine write's sources landed before it.** `log` is the canonical order
of landed writes and the write is judged at position `i`. Its author's state is
the canonical one through its `read_through`, `rt`, which is no later than `i`,
with its own queued writes applied on top, each of which the author's device
numbered below it and so lands before it. Every source it names is in the
writes before position `i`. -/
theorem sources_present {log : List W} {i rt : Nat} (before : rt ≤ i) {queued : List W}
    (own : ∀ a ∈ queued, a ∈ log.take i) {r : Row} {p : Provenance W Col}
    (authored : NamesCurrent (run M (run M St.init (log.take rt)) queued) r p) :
    ∀ x ∈ p.sources, x ∈ log.take i := by
  intro x named
  have current := named_current authored x named
  have gb : GenBound (run M St.init (log.take rt)) := run_genBound M init_genBound _
  rcases current_run M gb current with read | mine
  · rcases current_run M init_genBound read with init | landed
    · exact absurd init (init_not_current x)
    · have : log.take rt = (log.take i).take rt := by
        rw [List.take_take]
        congr 1
        omega
      rw [this] at landed
      exact List.mem_of_mem_take landed
  · exact own x mine

end CovenMerge.Sources
