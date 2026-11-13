import CovenSequence.Preparations

/-!
# Which device can raise a circle below its version

A device raises the audiences whose rows it held when its app migrated: their
raise bodies are those rows, migrated (`preparedRows`). An audience it starts
reading later sits below the device's breaking version, so the version rule
refuses every snapshot the device takes of it until something raises it
(`below_refused_until_raised`).

A raise body is an audience's rows migrated, and a migration may read the
store's rows besides the audience's own, never another circle's
(`Rules.migrate_reads`, coven.md §18.1). Whether this device can make the
raise depends on what it held:

- A circle its own member's creation entry made holds no rows yet. Its honest
  raise body at a coverage before any slot touches it is the empty initial
  rows, which the device holds, as long as migrating leaves an empty audience
  empty (`fresh_raise_body`).
- A circle with history it never held at the circle's version has an honest
  body the device can't get by converting: once another device's snapshot
  covers a slot of the circle, a raise must cover it too, and its body must be
  those rows migrated, while a device that started reading after migrating
  holds only the parts it converted on reading (`joiner_cannot_raise`).
- It can *rebuild* the circle instead: read the circle's rows and the store's
  at the circle's version from storage, as a member on the old app holds them,
  and migrate those. The rule that a migration reads only those two makes the
  result the honest body (`rebuilt_body_honest`), and the raise built on it
  counts (`rebuilt_raise_counted`). A migration that read a second circle
  would give the rebuilder another body than a member who held both
  (`cross_read_rebuild_differs`).
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
are those rows when migrating leaves an empty audience empty, whatever the
store holds. -/
theorem fresh_raise_body (seq : Nat → Slot S) (A : S.Aud) (v c : Nat) (p : S.Path)
    (fresh : ∀ j, 1 ≤ j → j ≤ c → touches A (seq j) = false)
    (empty : ∀ f : S.Aud → S.Rows, f A = R.rows0 A → R.migrate v f A = R.rows0 A) :
    preparedRows R (.raise v) p (raiseView R seq A c) A = R.rows0 A := by
  apply empty
  simp [raiseView, inputSlot, untouched_rows R seq A c fresh]

/-- The view of a rebuild: the circle's rows and the store's as the rebuilder
read them from storage, and the initial rows of every other audience. -/
def rebuildView (A : S.Aud) (circle store : S.Rows) : S.Aud → S.Rows :=
  fun B => if B = A then circle else if B = R.store then store else R.rows0 B

/-- **A rebuilt body is the honest body.** A device that reads the circle `A`
and the store from storage, each from a sound starting state, up to the circle's
coverage `c` and the store's input slot (`c`, or the slot before the store's
raise past the circle's version), and migrates a view holding just those two
audiences' rows, gets the raise body prepared at `c`. Every other audience's
rows are the initial ones and don't matter, since a migration reads only the
circle's rows and the store's (`migrate_reads`). -/
theorem rebuilt_body_honest {st : Store S} {seq : Nat → Slot S} (g : Good R st seq)
    (A : S.Aud) (v c : Nat) (p : S.Path) (head : c ≤ st.head)
    (floorA : st.floor.covers A ≤ c)
    (floorS : st.floor.covers R.store ≤ inputSlot R seq A c R.store)
    (dA dS : Reader S) (okA : ReaderOk R st seq A dA) (okS : ReaderOk R st seq R.store dS)
    (startA : dA.pos ≤ c) (startS : dS.pos ≤ inputSlot R seq A c R.store) :
    ∃ rA rS : Reader S,
      ReadSteps R A true st dA rA ∧ rA.pos = c ∧
      ReadSteps R R.store true st dS rS ∧ rS.pos = inputSlot R seq A c R.store ∧
      R.migrate v (rebuildView R A rA.rows rS.rows) A =
        preparedRows R (.raise v) p (raiseView R seq A c) A := by
  obtain ⟨rA, runA, posA, okA'⟩ := reaches_target R g A c head floorA dA okA startA
  obtain ⟨rS, runS, posS, okS'⟩ := reaches_target R g R.store (inputSlot R seq A c R.store)
    (Nat.le_trans (inputSlot_le R seq A c R.store) head) floorS dS okS startS
  refine ⟨rA, rS, runA, posA, runS, posS, ?_⟩
  apply R.migrate_reads
  · have : inputSlot R seq A c A = c := by simp [inputSlot]
    simp [rebuildView, raiseView, this, okA'.rows, posA]
  · by_cases same : R.store = A
    · have hA : inputSlot R seq A c A = c := by simp [inputSlot]
      simp [rebuildView, raiseView, same, hA, okA'.rows, posA]
    · simp [rebuildView, raiseView, same, okS'.rows, posS]

/-- **A rebuild from the kept store snapshot gets the honest body.** The store
side starts from `rebuild_store_start`'s reader, at the coverage `cs` of the
store snapshot retention keeps, below the floor's name for the store if need be,
and replays the store's raises and resets after it from the bodies retention
keeps (`RebuildKept`), up to the store's input slot, which is no later than the
slot before the store's raise past the circle's version. The circle side starts
from any sound reader up to the rebuild slot `c`. Migrating a view of just those
two audiences' rows gives the raise body prepared at `c`. -/
theorem rebuilt_body_from_kept {st : Store S} {seq : Nat → Slot S} (g : Good R st seq)
    {A : S.Aud} (need : NeedsRebuild R seq st.head A) {a cs : Nat} {q : S.Path}
    (snap : RebuildSnapshot R seq st.head A a cs q) (v c : Nat) (p : S.Path)
    (head : c ≤ st.head) (floorA : st.floor.covers A ≤ c)
    (dA : Reader S) (okA : ReaderOk R st seq A dA) (startA : dA.pos ≤ c)
    (after : cs ≤ inputSlot R seq A c R.store)
    (before : inputSlot R seq A c R.store ≤ storeBefore R seq (audAt R seq A st.head).version st.head) :
    ∃ rA rS dS : Reader S, ReaderOk R st seq R.store dS ∧ dS.pos = cs ∧
      ReadSteps R A true st dA rA ∧ rA.pos = c ∧
      ReadSteps R R.store true st dS rS ∧ rS.pos = inputSlot R seq A c R.store ∧
      R.migrate v (rebuildView R A rA.rows rS.rows) A =
        preparedRows R (.raise v) p (raiseView R seq A c) A := by
  obtain ⟨dS, okS, posS0, _, _⟩ := rebuild_store_start R g need snap
  obtain ⟨_, hstart, bodies⟩ := (g.rebuild_kept A need).1 a cs q snap
  obtain ⟨ha1, _, ⟨au, w, hs, _⟩, happ⟩ := snap.1
  have inputLe := inputSlot_le R seq A c R.store
  obtain ⟨rA, runA, posA, okA'⟩ := reaches_target R g A c head floorA dA okA startA
  obtain ⟨rS, runS, posS, okS'⟩ := reaches_target_kept R g R.store (target := inputSlot R seq A c R.store)
    (cs := cs) (by omega) hstart ⟨a, au, w, q, ha1, hs, happ⟩
    (fun r au v c p h1 h2 => bodies r au v c p h1 (Nat.le_trans h2 before)) dS okS
    (by omega) (by omega)
  refine ⟨rA, rS, dS, okS, posS0, runA, posA, runS, posS, ?_⟩
  apply R.migrate_reads
  · have : inputSlot R seq A c A = c := by simp [inputSlot]
    simp [rebuildView, raiseView, this, okA'.rows, posA]
  · by_cases same : R.store = A
    · have hA : inputSlot R seq A c A = c := by simp [inputSlot]
      simp [rebuildView, raiseView, same, hA, okA'.rows, posA]
    · simp [rebuildView, raiseView, same, okS'.rows, posS]

/-- The view of the store's own rebuild: the store's rows as the rebuilder
read them from storage, and the initial rows of every other audience. -/
def storeView (store : S.Rows) : S.Aud → S.Rows :=
  fun B => if B = R.store then store else R.rows0 B

/-- **The store's rebuilt body is the honest body.** A device that loads the
store as any joiner does, from the floor's snapshot, and follows storage to the
head, migrates a view holding just the store's rows to the store's raise body
prepared at the head. A migration of the store reads only the store's own rows
(`migrate_reads`), so no other audience is read and there is no input slot
below the head. -/
theorem rebuilt_store_body {st : Store S} {seq : Nat → Slot S} (g : Good R st seq)
    (v : Nat) (p : S.Path) :
    ∃ d', ReadSteps R R.store true st
        ⟨st.floor.covers R.store, ledgerAt R seq (st.floor.covers R.store),
          audAt R seq R.store (st.floor.covers R.store),
          rowsAt R seq R.store (st.floor.covers R.store), fun _ => none⟩ d' ∧
      d'.pos = st.head ∧
      R.migrate v (storeView R d'.rows) R.store =
        preparedRows R (.raise v) p (raiseView R seq R.store st.head) R.store := by
  obtain ⟨d', run, hpos, hstate⟩ := joiner_reaches_head R g R.store
  refine ⟨d', run, hpos, ?_⟩
  have rows : d'.rows = rowsAt R seq R.store st.head := by
    have := congrArg (fun x => x.2.2) hstate
    simpa [stateAt] using this
  simp only [preparedRows]
  apply R.migrate_reads
  · simp [storeView, raiseView, inputSlot, rows]
  · simp [storeView, raiseView, inputSlot, rows]

/-- **A rebuilt raise counts.** A raise of `A` to `v`, prepared at coverage `c`
and landing at slot `k + 1` with `A` having had no other raise or reset since
`c` and no announcement covering more than `c`, by an active device allowed to
raise, is applied. Its rows are the rebuilt body, with the parts after `c`
converted. -/
theorem rebuilt_raise_counted (seq : Nat → Slot S) (A : S.Aud) (k : Nat) (d : S.Dev)
    (v c : Nat) (p : S.Path) (view : S.Aud → S.Rows)
    (circle : view A = rowsAt R seq A c)
    (store : view R.store = rowsAt R seq R.store (inputSlot R seq A c R.store))
    (slot : seq (k + 1) = .boundary d A (.raise v) c p)
    (active : active (ledgerAt R seq k) d = true)
    (allowed : R.boundaryAllowed (ledgerAt R seq k) d A (.raise v) = true)
    (before : c < k + 1) (covered : (audAt R seq A k).maxCover ≤ c)
    (noBoundary : (audAt R seq A k).lastBoundary ≤ c)
    (prepared : (Boundary.raise v, c, p) ∈ (audAt R seq A k).preparations)
    (above : (audAt R seq A k).version < v) :
    AppliedTo R seq A (k + 1) ∧
    rowsAt R seq A (k + 1) =
      finishBoundary R A seq (audAt R seq A k).refused (.raise v) (audAt R seq A k).lastReset
        (R.migrate v view A) (c + 1) (k - c) := by
  have valid : audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A
      (.boundary d A (.raise v) c p) = true := by
    simp [audValid, active, allowed, before, covered, noBoundary, prepared, above]
  refine ⟨⟨by simp [slot, touches], by simpa [slot] using valid⟩, ?_⟩
  rw [rowsAt_boundary R seq A k slot]
  have body : R.migrate v view A = preparedRows R (.raise v) p (raiseView R seq A c) A := by
    apply R.migrate_reads
    · have : inputSlot R seq A c A = c := by simp [inputSlot]
      simp [raiseView, this, circle]
    · by_cases same : R.store = A
      · have hA : inputSlot R seq A c A = c := by simp [inputSlot]
        simp [raiseView, hA, same, circle]
      · simpa [raiseView, same] using store
  rw [body]
  simp [valid, preparedRows]

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
the raise from what it converted.** After slot 2's snapshot, a raise must cover
slot 1: one covering less is refused. Its body must then be slot 1's rows
migrated to version 2, `[10, 1002]`. A device that never held the circle at
version 1 holds only the part it converted on reading, `[10]`, which isn't that
body. It rebuilds the circle instead (`rebuilt_body_honest`). -/
theorem joiner_cannot_raise :
    AppliedTo baseOne history () 2 ∧
    audValid baseOne (ledgerAt baseOne history 2) (audAt baseOne history () 2) 3 ()
      (.prepare () () (.raise 2) 0 2 (6 : Nat)) = false ∧
    preparedRows baseOne (.raise 2) (6 : Nat) (raiseView baseOne history () 1) () = [10, 1002] ∧
    baseOne.merge (baseOne.rows0 ()) (baseOne.convert 1 2 10) ≠ [10, 1002] := by
  refine ⟨by unfold AppliedTo; decide, by decide, ?_, by decide⟩
  have one : rowsAt baseOne history () 1 = [10] := by
    rw [rowsAt_other baseOne history () 0 (by intros; simp [history])]
    rw [rowsAt]
    decide
  have view : raiseView baseOne history () 1 = fun _ => [10] := by
    funext B
    simp [raiseView, inputSlot, one]
  rw [view]
  rfl

/-- Three audiences: the store, and the circles Gifts and Garden. -/
abbrev threeSig : Sig where
  Dev := Unit
  Digest := Nat
  Mem := Unit
  Key := Unit
  Path := Nat
  Batch := Nat
  Rows := List Nat
  Op := Unit
  Aux := Unit
  Aud := Fin 3

/-- A migration that breaks `migrate_reads`: it fills each Gifts row (audience
1) with the number of Garden rows (audience 2) beside it. -/
def crossRead (view : Fin 3 → List Nat) (A : Fin 3) : List Nat :=
  if A = 1 then view 1 ++ [(view 2).length] else view A

/-- **Counterexample: a migration that reads a second circle gives a rebuilder
another body than a holder.** A member of Gifts and Garden migrates a view
holding both circles' rows, and Gifts' body counts Garden's two rows. A
rebuilder of Gifts reads only Gifts and the store, so Garden's rows are the
initial, empty ones in its view, and it computes another body at the same
coverage. This is why a migration may read only its audience's rows and the
store's (coven.md §18.1): `migrate_reads` fails for it. -/
theorem cross_read_rebuild_differs :
    let held : Fin 3 → List Nat := fun B => [B.val * 10 + 1, B.val * 10 + 2]
    let rebuilt : Fin 3 → List Nat := fun B => if B = 2 then [] else held B
    held 1 = rebuilt 1 ∧ held 0 = rebuilt 0 ∧
    crossRead held 1 ≠ crossRead rebuilt 1 ∧
    ¬ ∀ (f g : Fin 3 → List Nat) (A : Fin 3), f A = g A → f 0 = g 0 → crossRead f A = crossRead g A := by
  intro held rebuilt
  refine ⟨by decide, by decide, by decide, ?_⟩
  intro all
  exact absurd (all held rebuilt 1 (by decide) (by decide)) (by decide)

end CovenSequence.CircleRaise
