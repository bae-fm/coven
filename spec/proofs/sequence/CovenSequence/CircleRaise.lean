import CovenSequence.Preparations

/-!
# Which device can raise a circle below its version

A device raises the audiences whose rows it held when its app migrated: their
raise bodies are those rows, migrated (`preparedRows`). An audience it starts
reading later sits below the device's breaking version, so the version rule
refuses every snapshot the device takes of it until something raises it
(`below_refused_until_raised`).

Whether this device can make that raise depends on what it held:

- A circle its own member's creation entry made holds no rows yet. Its honest
  raise body at a coverage before any slot touches it is the empty initial
  rows, which the device holds, as long as migrating leaves an empty audience
  empty (`fresh_raise_body`).
- A circle with history it never held at the circle's version has an honest
  body it can't produce. Once another device's snapshot covers a slot of the
  circle, a raise must cover it too, and its body must be those rows migrated;
  a device that started reading after migrating holds only the parts it
  converted on reading (`joiner_cannot_raise`).
-/

namespace CovenSequence.CircleRaise

open CovenSequence

section General
variable {S : Sig} (R : Rules S)

/-- An audience no slot through `c` touches still holds its initial rows. -/
theorem untouched_rows (seq : Nat → Slot S) (A : S.Aud) :
    ∀ c, (∀ j, 1 ≤ j → j ≤ c → touches A (seq j) = false) → rowsAt R seq A c = R.rows0 A
  | 0, _ => by rw [rowsAt]
  | c + 1, untouched => by
    have earlier := untouched_rows seq A c (fun j lo hi => untouched j lo (by omega))
    have this := untouched (c + 1) (by omega) (Nat.le_refl _)
    cases slot : seq (c + 1) with
    | boundary d a kind covers p =>
      rw [slot] at this
      have other : ¬ a = A := by simpa [touches] using this
      rw [rowsAt, slot]
      simp [other, earlier]
    | _ =>
      rw [rowsAt_other R seq A c (by intros; simp [slot])]
      rw [slot] at this ⊢
      simp [this, earlier]

/-- **A new circle's maker holds its honest raise body.** At a coverage before
any slot touches the circle, the raise body is its initial rows migrated, which
are those rows when migrating leaves an empty audience empty. -/
theorem fresh_raise_body (seq : Nat → Slot S) (A : S.Aud) (v c : Nat) (p : S.Path)
    (fresh : ∀ j, 1 ≤ j → j ≤ c → touches A (seq j) = false)
    (empty : R.migrate v (R.rows0 A) = R.rows0 A) :
    preparedRows R (.raise v) p (rowsAt R seq A c) = R.rows0 A := by
  simp [preparedRows, untouched_rows R seq A c fresh, empty]

end General

open ReaderExamples

/-- A circle that starts at version 1: its creation recorded base 1. -/
def baseOne : Rules sig := { rules with aud0 := fun _ => { aud0 with version := 1 } }

/-- Slot 1 announces a snapshot at version 2 of the circle at version 1. In
`raised`, slot 1 prepares a raise to 2, slot 2 applies it and slot 3
announces the same snapshot. -/
def unraised : Nat → Slot sig
  | 1 => .snapshot () () 0 2 (5 : Nat)
  | _ => .writes () 0 (fun _ => none)

def raised : Nat → Slot sig
  | 1 => .prepare () () (.raise 2) 0 2 (6 : Nat)
  | 2 => .boundary () () (.raise 2) 0 (6 : Nat)
  | 3 => .snapshot () () 2 2 (5 : Nat)
  | _ => .writes () 0 (fun _ => none)

/-- **The stuck state.** A device whose breaking version is 2 reads a circle at
version 1. Every snapshot it takes names version 2, and the version rule
refuses it while the circle has no raise; once a raise to 2 applies, the same
snapshot counts. A device that never raises the circle never gets one counted. -/
theorem below_refused_until_raised :
    ¬ AppliedTo baseOne unraised () 1 ∧ AppliedTo baseOne raised () 3 := by
  unfold AppliedTo
  decide

/-- Slot 1 writes a part at version 1 into the circle; slot 2 announces a
snapshot of it covering slot 1, from a member on the old app. -/
def history : Nat → Slot sig
  | 1 => .writes () 0 (fun _ => some ⟨1, [], 10, 0⟩)
  | 2 => .snapshot () () 1 1 (7 : Nat)
  | _ => .writes () 0 (fun _ => none)

/-- **Counterexample: a device that started reading after migrating can't make
the raise.** After slot 2's snapshot, a raise must cover slot 1: one covering
less is refused. Its body must then be slot 1's rows migrated to version 2,
`[10, 1002]`. A device that never held the circle at version 1 holds only the
part it converted on reading, `[10]`, which isn't that body. -/
theorem joiner_cannot_raise :
    AppliedTo baseOne history () 2 ∧
    audValid baseOne (ledgerAt baseOne history 2) (audAt baseOne history () 2) 3 ()
      (.prepare () () (.raise 2) 0 2 (6 : Nat)) = false ∧
    preparedRows baseOne (.raise 2) (6 : Nat) (rowsAt baseOne history () 1) = [10, 1002] ∧
    baseOne.merge (baseOne.rows0 ()) (baseOne.convert 1 2 10) ≠ [10, 1002] := by
  refine ⟨by unfold AppliedTo; decide, by decide, ?_, by decide⟩
  have one : rowsAt baseOne history () 1 = [10] := by
    rw [rowsAt_other baseOne history () 0 (by intros; simp [history])]
    rw [rowsAt]
    decide
  rw [one]
  rfl

end CovenSequence.CircleRaise
