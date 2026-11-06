import CovenSequence.FloorCheck

/-!
# Settling a floor write whose reply was lost (§16.6)

Retention writes the floor with a request that succeeds only if the floor is
still the revision it read (`previous`). When the reply is lost, the device
can't tell whether the write landed. It reads the floor storage holds now and
checks it from the history it holds (`FloorCheck`): from its own applied slots,
or, once that floor's first kept slot has passed the slot after them, from the
floor's store snapshot. Then it decides (`confirm`):

- the floor is still `previous`: the write didn't land, and the device writes
  it again (`write`);
- the floor changed, and its first kept slot reaches the write's own: the
  device deletes the slots below that floor's first kept slot (`confirmed`);
- the floor changed, and its first kept slot is below the write's own: the
  write didn't land, and nothing is deleted (`lost`).

Storage keeps the floor's revisions as a log (`Log`): every write makes a new
revision, and none moves the first kept slot back (`StoreStep.writeFloor`). A
write landed when it directly follows the revision it was conditional on
(`Landed`).

A landed write is always confirmed, at the first kept slot of the floor the
device reads, which can be further than its own (`landed_confirms`). So a write
not confirmed didn't land (`unconfirmed_not_landed`). A confirmed write may not
have landed: another device's floor can reach its first kept slot. Deleting
below the floor the device read is a storage step in either case, then and
after any later storage steps (`confirmed_deletes`).
-/
namespace CovenSequence.RetentionConfirm

/-- One revision of the floor: its identity and its first kept slot. -/
structure Rev where
  id : Nat
  first : Nat
  deriving DecidableEq

/-- What the device does after reading the floor. -/
inductive Verdict where
  /-- The floor is still the revision the write was conditional on: write again. -/
  | write
  /-- Delete the slots below `first`, the first kept slot of the floor read. -/
  | confirmed (first : Nat)
  /-- Another floor replaced the one the write was conditional on: delete nothing. -/
  | lost
  deriving DecidableEq

/-- The verdict for a write of first kept slot `own`, conditional on `previous`,
once the device has read and checked `current`. -/
def confirm (previous : Option Rev) (own : Nat) (current : Option Rev) : Verdict :=
  if current = previous then .write
  else match current with
    | some c => if own ≤ c.first then .confirmed c.first else .lost
    | none => .lost

/-- Storage's floor revisions, oldest first: each write a new revision, and no
write moves the first kept slot back. -/
structure Log (log : List Rev) : Prop where
  fresh : (log.map Rev.id).Nodup
  forward : log.Pairwise (fun a b => a.first ≤ b.first)

/-- The write `w`, conditional on `previous`, landed: it directly follows
`previous` in the log, or opens it when there was no floor. -/
def Landed (log : List Rev) (previous : Option Rev) (w : Rev) : Prop :=
  ∃ pre post, log = pre ++ w :: post ∧ pre.getLast? = previous

/-- **A landed write is confirmed.** Whatever floors other devices wrote after
it, the floor the device reads is a new revision whose first kept slot reaches
the write's own, and the device deletes below that floor's first kept slot. -/
theorem landed_confirms {log : List Rev} (h : Log log) {previous : Option Rev} {w : Rev}
    (landed : Landed log previous w) :
    ∃ c, log.getLast? = some c ∧ w.first ≤ c.first ∧
      confirm previous w.first log.getLast? = .confirmed c.first := by
  obtain ⟨pre, post, rfl, hprev⟩ := landed
  have hlast : (pre ++ w :: post).getLast? = some ((w :: post).getLast (by simp)) := by
    rw [List.getLast?_append, List.getLast?_eq_some_getLast (by simp)]
    rfl
  have hreach : w.first ≤ ((w :: post).getLast (by simp)).first := by
    rcases List.mem_cons.mp (List.getLast_mem (l := w :: post) (by simp)) with he | hp
    · rw [he]
      exact Nat.le_refl _
    · exact (List.pairwise_cons.mp (List.pairwise_append.mp h.forward).2.1).1 _ hp
  have hne : some ((w :: post).getLast (by simp)) ≠ previous := by
    intro he
    rw [← hprev] at he
    have hp := List.mem_of_getLast? he.symm
    have hc := List.getLast_mem (l := w :: post) (by simp)
    have hd := (List.nodup_append.mp (List.map_append (f := Rev.id) ▸ h.fresh)).2.2
    exact hd _ (List.mem_map_of_mem hp) _ (List.mem_map_of_mem hc) rfl
  refine ⟨_, hlast, hreach, ?_⟩
  simp only [confirm, hlast, hne, ite_false, hreach, ite_true]

/-- **A write not confirmed didn't land.** When the device finds the revision
it was conditional on, or a floor below its own first kept slot, its write is
not in the log. -/
theorem unconfirmed_not_landed {log : List Rev} (h : Log log) {previous : Option Rev} {w : Rev}
    (unconfirmed : ∀ n, confirm previous w.first log.getLast? ≠ .confirmed n) :
    ¬ Landed log previous w := by
  intro landed
  obtain ⟨c, _, _, hc⟩ := landed_confirms h landed
  exact unconfirmed _ hc

section Store
variable {S : Sig} (R : Rules S)

theorem stores_mono {seq : Nat → Slot S} {st st' : Store S} (run : StoreSteps R seq st st') :
    st.floor.start ≤ st'.floor.start := by
  induction run with
  | refl => exact Nat.le_refl _
  | step s _ ih => exact Nat.le_trans (store_mono R s).1 ih

/-- **A confirmed deletion is a storage step.** The device read the floor of
`st`, whose first kept slot is `current`'s. A confirmed verdict names that slot,
which reaches the write's own, and deleting any slot below it is a storage step
(`StoreStep.delete`), in `st` or after any later storage steps, since the first
kept slot never moves back. -/
theorem confirmed_deletes {seq : Nat → Slot S} {st st' : Store S} {previous : Option Rev}
    {own n : Nat} {c : Rev} (hc : c.first = st.floor.start)
    (verdict : confirm previous own (some c) = .confirmed n) (run : StoreSteps R seq st st') :
    n = st.floor.start ∧ own ≤ n ∧ ∀ i, i < n →
      StoreStep R seq st' { st' with slot := fun j => if j = i then none else st'.slot j } := by
  have hn : n = c.first ∧ own ≤ c.first := by
    unfold confirm at verdict
    split at verdict
    · cases verdict
    · simp only at verdict
      split at verdict
      · cases verdict; exact ⟨rfl, by assumption⟩
      · cases verdict
  obtain ⟨rfl, hown⟩ := hn
  refine ⟨hc, hown, fun i hi => StoreStep.delete st' i ?_⟩
  have := stores_mono R run
  omega

end Store

/-- Ana's write moves the first kept slot from 3 to 5, conditional on revision
1. It lands as revision 2, and Ben's revision 3 then moves it to 9. Reading
revision 3, Ana confirms at 9. Had Ben's revision 3 replaced revision 1 first,
Ana's write could not land; she still confirms at 9, which reaches hers. A floor
of first kept slot 4 replacing revision 1 is `lost`, and revision 1 itself means
writing again. -/
theorem moved_floor_confirms :
    let r1 : Rev := ⟨1, 3⟩
    let ana : Rev := ⟨2, 5⟩
    let ben : Rev := ⟨3, 9⟩
    Log [r1, ana, ben] ∧ Landed [r1, ana, ben] (some r1) ana ∧
    confirm (some r1) 5 (some ben) = .confirmed 9 ∧
    ¬ Landed [r1, ben] (some r1) ana ∧
    confirm (some r1) 5 (some ⟨3, 4⟩) = .lost ∧ confirm (some r1) 5 (some r1) = .write := by
  refine ⟨⟨by decide, by decide⟩, ⟨[⟨1, 3⟩], [⟨3, 9⟩], rfl, rfl⟩, by decide, ?_, by decide, by decide⟩
  rintro ⟨pre, post, he, _⟩
  match pre, he with
  | [], he => simp at he
  | [_], he => simp at he
  | [_, _], he => simp at he
  | _ :: _ :: _ :: _, he => simp at he

end CovenSequence.RetentionConfirm
